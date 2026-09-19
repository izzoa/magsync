"""Textual TUI application for magsync."""

from __future__ import annotations

import asyncio

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Static,
    TabbedContent,
    TabPane,
    Tree,
)

from magsync.config import load_config
from magsync.core.diagnostics import sanitize_external_error
from magsync.core.index import MagazineIndex
from magsync.companion.local import CoordinatorBusy, submit_local
from magsync.companion.protocol import ProtocolError
from magsync.core.models import (
    DownloadFailureKind,
    SourceFailure,
    SourceFailureKind,
)
from magsync.core.policy import get_download_failure_policy


def _is_queueable(issue: dict, selected: set[int]) -> bool:
    """True when a selected issue may enter the download queue.

    "unsupported" is terminal (non-PDF payload) — bulk selection must not
    re-queue it; it only re-probes when the share link rotates.
    """
    return (
        issue["id"] in selected
        and issue.get("download_status") not in ("complete", "unsupported")
        and bool(issue.get("limewire_url"))
    )


def _source_failure_status(failure: SourceFailure) -> str:
    """Return concise, secret-safe TUI guidance for a typed source failure."""
    prefixes = {
        SourceFailureKind.ACCESS_BLOCKED: "Source access is blocked; retry later",
        SourceFailureKind.TRANSIENT: "Source is temporarily unavailable; retry later",
        SourceFailureKind.PROTOCOL: "Source response format was not recognized",
    }
    prefix = prefixes[failure.kind]
    detail = sanitize_external_error(failure.message).strip()
    if detail and detail.casefold() not in prefix.casefold():
        return f"{prefix}: {detail}"
    return prefix


# Marker per terminal label; ``_download_outcome_label`` supplies the label.
_OUTCOME_MARKERS = {"downloaded": "✓", "unavailable": "○", "unsupported": "⊘", "failed": "✗"}
# Physical status -> outcome label, for results observed in the shared store.
_STATUS_LABELS = {
    "complete": "downloaded",
    "unavailable": "unavailable",
    "unsupported": "unsupported",
    "failed": "failed",
}


def _search_failure_status(result: dict) -> str:
    """Typed, sanitized guidance for a search operation that did not validate."""
    failure = result.get("failure") or {}
    kind = failure.get("kind") or result.get("failure_kind")
    if kind is None and result.get("outcome") == "blocked":
        kind = SourceFailureKind.ACCESS_BLOCKED.value
    try:
        return _source_failure_status(SourceFailure(SourceFailureKind(kind), failure.get("message") or ""))
    except (TypeError, ValueError):
        code = result.get("code")
        return f"Search failed ({code}); retry later" if code else "Search failed; retry later"


def _outcome_line(title: str, status: str, failure_kind: str | None) -> str | None:
    """``✓ Title``-style line for a terminal physical status, else None."""
    label = _STATUS_LABELS.get(status)
    if label is None:
        return None
    detail = f" ({failure_kind})" if failure_kind and label != "downloaded" else ""
    return f"{_OUTCOME_MARKERS[label]} {sanitize_external_error((title or 'Unknown issue')[:60])}{detail}"


def _download_outcome_label(
    success: bool,
    failure_kind: DownloadFailureKind | str | None,
) -> str:
    """Map one structured terminal result to its TUI label."""
    if success:
        return "downloaded"
    if failure_kind is None:
        return "failed"
    return get_download_failure_policy(failure_kind).summary_bucket.value


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
        try:
            operation = asyncio.run(submit_local(
                'search', {'query': query}, self.cfg, self.idx.db_path, status=self._update_status))
        except (ProtocolError, CoordinatorBusy) as exc:
            self._update_status(getattr(exc, 'message', None) or str(exc))
            return
        result = operation.get('result') or {}
        if result.get('outcome') in ('blocked', 'failed') or operation['state'] in ('failed', 'suspended', 'blocked'):
            # A failure is not an empty result: keep the prior table and selection.
            self._update_status(_search_failure_status(result) + '. Previous results kept.')
            return
        if result.get('outcome') == 'empty':
            self.search_results = []
            self.selected_issues.clear()
            self.app.call_from_thread(self._populate_empty_results, query)
            return
        # Each worker owns its SQLite connection; Textual's UI connection stays
        # on its creating thread. The command result bounds the displayed set.
        index = MagazineIndex(self.idx.db_path)
        try:
            from magsync.companion.store import Store
            store = Store(index)
            ids = [store.internal_issue(item['id']) for item in result.get('items', [])]
            rows = index.get_issues_by_ids(ids)
        finally:
            index.close()
        self.search_results = rows
        self.selected_issues.clear()
        self.app.call_from_thread(self._populate_table, rows, result.get('added') or 0,
                                  result.get('detail_failures') or 0)

    def _populate_empty_results(self, query: str) -> None:
        self.query_one("#results-table", DataTable).clear()
        self._update_status(f"No results for '{query}'")

    def _populate_table(
        self,
        issues: list[dict],
        new_count: int,
        omitted_details: int = 0,
    ) -> None:
        table = self.query_one("#results-table", DataTable)
        table.clear()
        for issue in issues:
            status = issue.get("download_status", "pending")
            # Never-requested rows are catalog entries, not queued work; they
            # must not present as "pending". Selecting one for download marks
            # it manual, at which point it renders normally.
            if status not in ("complete", "downloading") and issue.get(
                "requested_by"
            ) not in ("manual", "subscription"):
                status = "cataloged"
            elif status == "unsupported":
                status = "⊘ non-PDF"
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
        status = f"Found {len(issues)} issues ({new_count} new)"
        if omitted_details:
            status += f"; {omitted_details} detail page(s) omitted"
        self._update_status(status)

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
        issues = [i for i in self.search_results if _is_queueable(i, self.selected_issues)]
        if not issues:
            self._update_status("No downloadable issues selected.")
            return
        total = len(issues)
        titles = {issue['id']: issue.get('title') or 'Unknown issue' for issue in issues}
        first_seen: dict[int, tuple] = {}
        lines: dict[int, str] = {}

        def show() -> None:
            self.app.call_from_thread(self._update_download_log, "\n".join(lines.values()))
            self._update_status(f"Processed {len(lines)}/{total} selected issues...")

        def progress(update: dict) -> None:
            issue_id = update['issue_id']
            observed = (update['status'], update.get('failure_kind'))
            if issue_id not in first_seen:
                first_seen[issue_id] = observed  # Its state when the command started.
                return
            line = _outcome_line(titles.get(issue_id, update.get('title')), *observed)
            if line and observed != first_seen[issue_id]:
                lines[issue_id] = line
                show()

        self._update_status(f"Queued {total} selected issues...")
        try:
            operation = asyncio.run(submit_local(
                'download', {'issue_ids': [i['id'] for i in issues]}, self.cfg, self.idx.db_path,
                progress=progress, status=self._update_status))
        except (ProtocolError, CoordinatorBusy) as exc:
            self._update_status(getattr(exc, 'message', None) or str(exc))
            return
        result = operation.get('result') or {}
        # The final state of every selected issue, read back from the store.
        index = MagazineIndex(self.idx.db_path)
        try:
            final = {row['id']: row for row in index.get_issues_by_ids(list(titles))}
        finally:
            index.close()
        for issue_id, title in titles.items():
            row = final.get(issue_id) or {}
            line = _outcome_line(title, row.get('download_status'), row.get('last_error_kind'))
            lines[issue_id] = line or f"· {sanitize_external_error(title[:60])}: not downloaded"
        self.app.call_from_thread(self._update_download_log, "\n".join(lines.values()))
        attempts = result.get('physical_attempts', 0)
        message = f"Done: {total} selected issues processed; {attempts} physical transfer{'s' if attempts != 1 else ''} started."
        if operation['state'] == 'failed':
            message = f"Download did not complete ({result.get('code', 'failed')})."
        self._update_status(message)
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
                    ds = issue.get("download_status")
                    status = "✓" if ds == "complete" else ("⊘" if ds == "unsupported" else "○")
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
