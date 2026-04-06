"""Textual TUI application for magsync."""

from __future__ import annotations

import asyncio

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    ProgressBar,
    Static,
    TabbedContent,
    TabPane,
    Tree,
)

from magsync.config import load_config
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus
from magsync.core.organizer import normalize_title, parse_date, organize_path
from magsync.core.scraper import search_with_details


class MagSyncApp(App):
    """magsync - Magazine Sync Tool."""

    CSS = """
    #search-input {
        dock: top;
        margin: 1 2;
    }
    #status-bar {
        dock: bottom;
        height: 1;
        background: $accent;
        color: $text;
        padding: 0 2;
    }
    #results-table {
        height: 1fr;
        margin: 0 2;
    }
    #download-log {
        height: 1fr;
        margin: 0 2;
        overflow-y: auto;
    }
    #library-tree {
        height: 1fr;
        margin: 0 2;
    }
    .progress-label {
        margin: 0 2;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("s", "focus_search", "Search", show=True),
        Binding("a", "select_all", "Select All", show=True),
        Binding("d", "download_selected", "Download", show=True),
    ]

    TITLE = "magsync"

    def __init__(self):
        super().__init__()
        self.cfg = load_config()
        self.selected_issues: set[int] = set()
        self.search_results: list[dict] = []
        self.idx: MagazineIndex | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent():
            with TabPane("Search", id="search-tab"):
                yield Input(placeholder="Search for a magazine...", id="search-input")
                yield DataTable(id="results-table")
            with TabPane("Downloads", id="downloads-tab"):
                yield Label("Downloads will appear here.", id="download-status", classes="progress-label")
                yield Static(id="download-log")
            with TabPane("Library", id="library-tab"):
                yield Tree("Magazines", id="library-tree")
        yield Label("Ready. Press 's' to search.", id="status-bar")
        yield Footer()

    def on_mount(self) -> None:
        self.idx = MagazineIndex()
        table = self.query_one("#results-table", DataTable)
        table.add_columns("✓", "Title", "Year", "Month", "Size", "Status")
        table.cursor_type = "row"
        self._refresh_library()

    def on_unmount(self) -> None:
        if self.idx:
            self.idx.close()

    def action_focus_search(self) -> None:
        self.query_one("#search-input", Input).focus()

    @on(Input.Submitted, "#search-input")
    def on_search_submit(self, event: Input.Submitted) -> None:
        query = event.value.strip()
        if query:
            self._do_search(query)

    @work(thread=True)
    def _do_search(self, query: str) -> None:
        self._update_status(f"Searching for '{query}'...")
        results = asyncio.run(
            search_with_details(query, scrape_delay=self.cfg.download.scrape_delay)
        )

        if not results:
            self._update_status(f"No results for '{query}'")
            return

        # Index results
        norm = normalize_title(results[0].title) if results[0].title else query
        mag_id = self.idx.get_or_create_magazine(query, norm)
        issues_data = []
        for r in results:
            parsed = parse_date(r.title, r.page_url)
            issues_data.append({
                "title": r.title,
                "page_url": r.page_url,
                "limewire_url": r.limewire_url,
                "year": parsed.year,
                "month": parsed.month,
                "date_raw": r.title,
                "genre": r.genre,
                "file_size": r.file_size,
                "cover_image_url": r.cover_image_url,
            })
        new_count = self.idx.add_issues(mag_id, issues_data)

        # Get indexed issues
        all_issues = self.idx.get_issues(magazine_title=norm)
        self.search_results = all_issues
        self.selected_issues.clear()

        self.app.call_from_thread(self._populate_table, all_issues, new_count)

    def _populate_table(self, issues: list[dict], new_count: int) -> None:
        table = self.query_one("#results-table", DataTable)
        table.clear()
        for issue in issues:
            status = issue.get("download_status", "pending")
            check = "☐"
            table.add_row(
                check,
                (issue.get("title") or "")[:60],
                str(issue.get("year") or "?"),
                str(issue.get("month") or "?"),
                issue.get("file_size") or "?",
                status,
                key=str(issue["id"]),
            )
        self._update_status(f"Found {len(issues)} issues ({new_count} new)")

    @on(DataTable.RowSelected, "#results-table")
    def on_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.row_key is None:
            return
        issue_id = int(event.row_key.value)
        table = self.query_one("#results-table", DataTable)

        if issue_id in self.selected_issues:
            self.selected_issues.discard(issue_id)
            table.update_cell_at((event.cursor_row, 0), "☐")
        else:
            self.selected_issues.add(issue_id)
            table.update_cell_at((event.cursor_row, 0), "☑")

        self._update_status(f"{len(self.selected_issues)} issues selected")

    def action_select_all(self) -> None:
        table = self.query_one("#results-table", DataTable)
        if len(self.selected_issues) == len(self.search_results):
            # Deselect all
            self.selected_issues.clear()
            for i in range(table.row_count):
                table.update_cell_at((i, 0), "☐")
        else:
            # Select all
            for issue in self.search_results:
                self.selected_issues.add(issue["id"])
            for i in range(table.row_count):
                table.update_cell_at((i, 0), "☑")
        self._update_status(f"{len(self.selected_issues)} issues selected")

    def action_download_selected(self) -> None:
        if not self.selected_issues:
            self._update_status("No issues selected. Select with Enter, or press 'a' for all.")
            return
        self._do_download()

    @work(thread=True)
    def _do_download(self) -> None:
        from magsync.core.downloader import download_and_decrypt

        issues = [
            i for i in self.search_results
            if i["id"] in self.selected_issues
            and i.get("download_status") != "complete"
            and i.get("limewire_url")
        ]

        if not issues:
            self._update_status("No downloadable issues selected.")
            return

        self._update_status(f"Downloading {len(issues)} issues...")
        log_lines = []

        for n, issue in enumerate(issues, 1):
            title = issue["title"][:50]
            self._update_status(f"[{n}/{len(issues)}] Downloading: {title}...")

            self.idx.update_download_status(issue["id"], DownloadStatus.DOWNLOADING)

            dest = organize_path(
                issue["title"], issue["page_url"], self.cfg.output_dir
            )
            result = asyncio.run(
                download_and_decrypt(
                    issue["limewire_url"], dest, constants=self.cfg.limewire
                )
            )

            if result.success:
                self.idx.update_download_status(
                    issue["id"], DownloadStatus.COMPLETE,
                    str(result.file_path), result.file_size_bytes,
                )
                log_lines.append(f"✓ {issue['title']}")
            else:
                self.idx.update_download_status(issue["id"], DownloadStatus.FAILED)
                log_lines.append(f"✗ {issue['title']}: {result.error}")

            self.app.call_from_thread(
                self._update_download_log, "\n".join(log_lines)
            )

        self._update_status(f"Done! {len(issues)} issues processed.")
        self.app.call_from_thread(self._refresh_library)

    def _update_download_log(self, text: str) -> None:
        log = self.query_one("#download-log", Static)
        log.update(text)

    def _refresh_library(self) -> None:
        tree = self.query_one("#library-tree", Tree)
        tree.clear()

        if not self.idx:
            return

        magazines = self.idx.get_tracked_magazines()
        for mag in magazines:
            mag_node = tree.root.add(
                f"{mag['title']} ({mag['downloaded_count']}/{mag['issue_count']})"
            )
            issues = self.idx.get_issues(magazine_title=mag["normalized_title"])
            years: dict[int, list] = {}
            for issue in issues:
                y = issue.get("year") or 0
                years.setdefault(y, []).append(issue)

            for year in sorted(years.keys(), reverse=True):
                year_label = str(year) if year else "Unknown"
                year_node = mag_node.add(year_label)
                for issue in years[year]:
                    status = "✓" if issue.get("download_status") == "complete" else "○"
                    year_node.add_leaf(f"{status} {issue['title'][:60]}")

    def _update_status(self, text: str) -> None:
        try:
            self.app.call_from_thread(
                lambda: self.query_one("#status-bar", Label).update(text)
            )
        except Exception:
            # Might be called from main thread
            try:
                self.query_one("#status-bar", Label).update(text)
            except Exception:
                pass
