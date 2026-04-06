"""CLI interface for magsync using Typer."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import sys
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from magsync.config import load_config, save_config, set_config_value
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus, Subscription
from magsync.core.organizer import normalize_title, parse_date, organize_path, strip_accents
from magsync.core.scraper import search_with_details

app = typer.Typer(
    name="magsync",
    help="Index and download magazines from freemagazines.top",
    no_args_is_help=False,
    invoke_without_command=True,
)
console = Console()


@app.callback()
def main(ctx: typer.Context):
    """magsync - magazine sync tool. Run without arguments for TUI."""
    if ctx.invoked_subcommand is None:
        from magsync.tui.app import MagSyncApp
        tui_app = MagSyncApp()
        tui_app.run()


def _filter_results(results, query: str, exact: bool):
    """Filter scraped results by exact title match if requested."""
    if not exact:
        return results
    query_norm = strip_accents(query).lower()
    return [r for r in results if strip_accents(normalize_title(r.title)).lower() == query_norm]


@app.command()
def search(
    query: str = typer.Argument(..., help="Magazine title to search for"),
):
    """Search for magazines and display results."""
    cfg = load_config()

    with console.status(f"Searching for '{query}'..."):
        results = asyncio.run(
            search_with_details(query, scrape_delay=cfg.download.scrape_delay)
        )

    if not results:
        console.print(f"[yellow]No results found for '{query}'[/yellow]")
        raise typer.Exit()

    # Index the results
    idx = MagazineIndex()
    try:
        norm = normalize_title(results[0].title) if results[0].title else query
        mag_id = idx.get_or_create_magazine(query, norm)
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
        new_count = idx.add_issues(mag_id, issues_data)

        # Display results
        table = Table(title=f"Results for '{query}' ({len(results)} issues, {new_count} new)")
        table.add_column("#", style="dim", width=4)
        table.add_column("Title", style="cyan", max_width=60)
        table.add_column("Year", width=6)
        table.add_column("Month", width=6)
        table.add_column("Size", width=8)
        table.add_column("Status", width=10)

        all_issues = idx.get_issues(magazine_title=norm)
        for i, issue in enumerate(all_issues, 1):
            status = issue.get("download_status", "pending")
            status_style = {
                "complete": "[green]done[/green]",
                "pending": "[dim]pending[/dim]",
                "failed": "[red]failed[/red]",
                "downloading": "[yellow]downloading[/yellow]",
            }.get(status, status)

            table.add_row(
                str(i),
                issue["title"][:60],
                str(issue.get("year") or "?"),
                str(issue.get("month") or "?"),
                issue.get("file_size") or "?",
                status_style,
            )

        console.print(table)
    finally:
        idx.close()


@app.command()
def fetch(
    query: str = typer.Argument(..., help="Magazine title to fetch"),
    since: str = typer.Option(None, "--since", help="Fetch issues from this date (YYYY-MM)"),
    output: str = typer.Option(None, "--output", "-o", help="Output directory override"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be downloaded without downloading"),
):
    """Search, index, and download magazines."""
    cfg = load_config()
    output_dir = output or cfg.output_dir

    # Parse --since
    since_year = since_month = None
    if since:
        parts = since.split("-")
        since_year = int(parts[0])
        since_month = int(parts[1]) if len(parts) > 1 else None

    # Search and index
    with console.status(f"Searching for '{query}'..."):
        results = asyncio.run(
            search_with_details(query, scrape_delay=cfg.download.scrape_delay)
        )

    if not results:
        console.print(f"[yellow]No results found for '{query}'[/yellow]")
        raise typer.Exit()

    idx = MagazineIndex()
    try:
        norm = normalize_title(results[0].title) if results[0].title else query
        mag_id = idx.get_or_create_magazine(query, norm)
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
        idx.add_issues(mag_id, issues_data)

        # Get pending issues matching date filter
        pending = idx.get_issues(
            magazine_title=norm,
            since_year=since_year,
            since_month=since_month,
            status=DownloadStatus.PENDING,
        )

        if not pending:
            console.print("[green]All matching issues already downloaded![/green]")
            raise typer.Exit()

        if dry_run:
            table = Table(title=f"Would download {len(pending)} issues")
            table.add_column("#", style="dim", width=4)
            table.add_column("Title", style="cyan", max_width=55)
            table.add_column("Year", width=6)
            table.add_column("Month", width=6)
            table.add_column("Size", width=8)
            total_size = 0
            for i, issue in enumerate(pending, 1):
                table.add_row(
                    str(i),
                    issue["title"][:55],
                    str(issue.get("year") or "?"),
                    str(issue.get("month") or "?"),
                    issue.get("file_size") or "?",
                )
                size_str = issue.get("file_size") or ""
                if "MB" in size_str:
                    try:
                        total_size += int("".join(c for c in size_str if c.isdigit()))
                    except ValueError:
                        pass
            console.print(table)
            if total_size:
                console.print(f"\n[dim]Estimated total: ~{total_size} MB[/dim]")
            console.print("\n[yellow]Dry run — no files downloaded.[/yellow]")
            raise typer.Exit()

        console.print(f"[cyan]Downloading {len(pending)} issues (max {cfg.download.max_concurrent} concurrent)...[/cyan]")

        from magsync.core.batch import download_batch

        completed = [0]
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            overall = progress.add_task("Overall", total=len(pending))

            def on_start(issue):
                title = issue["title"][:50]
                progress.add_task(f"  {title}...", total=None)

            def on_complete(issue, success, error):
                completed[0] += 1
                progress.update(overall, completed=completed[0])
                title = issue["title"][:50]
                if success:
                    console.print(f"  [green]✓[/green] {title}")
                else:
                    console.print(f"  [red]✗[/red] {title}: {error}")

            asyncio.run(download_batch(pending, cfg, idx, on_start, on_complete))

        stats = idx.get_download_stats()
        console.print(
            f"\n[green]Done![/green] {stats['downloaded']} downloaded, "
            f"{stats['pending']} pending, {stats['failed']} failed"
        )
    finally:
        idx.close()


@app.command()
def update():
    """Re-scrape all tracked magazines and update the index."""
    cfg = load_config()
    idx = MagazineIndex()

    try:
        magazines = idx.get_tracked_magazines()
        if not magazines:
            console.print("[yellow]No tracked magazines. Run 'magsync search' first.[/yellow]")
            raise typer.Exit()

        total_new = 0
        for mag in magazines:
            with console.status(f"Updating '{mag['title']}'..."):
                results = asyncio.run(
                    search_with_details(
                        mag["title"], scrape_delay=cfg.download.scrape_delay
                    )
                )
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
                new = idx.add_issues(mag["id"], issues_data)
                total_new += new
                if new:
                    console.print(f"  [cyan]{mag['title']}[/cyan]: {new} new issues")
                else:
                    console.print(f"  [dim]{mag['title']}: up to date[/dim]")

        console.print(f"\n[green]Update complete.[/green] {total_new} new issues found.")
    finally:
        idx.close()


@app.command()
def config(
    key: str = typer.Argument(None, help="Config key to view or set (e.g., 'output_dir')"),
    value: str = typer.Argument(None, help="Value to set"),
):
    """View or modify magsync configuration."""
    if key and value:
        cfg = set_config_value(key, value)
        console.print(f"[green]Set {key} = {value}[/green]")
    else:
        cfg = load_config()
        table = Table(title="magsync configuration")
        table.add_column("Key", style="cyan")
        table.add_column("Value")

        table.add_row("output_dir", cfg.output_dir)
        table.add_row("download.max_concurrent", str(cfg.download.max_concurrent))
        table.add_row("download.retry_attempts", str(cfg.download.retry_attempts))
        table.add_row("download.scrape_delay", str(cfg.download.scrape_delay))
        lw_status = "[green]configured[/green]" if cfg.limewire.file_iv_b64 else "[dim]auto-extract on first download[/dim]"
        table.add_row("limewire.constants", lw_status)
        table.add_row("notifications.enabled", str(cfg.notifications.enabled))
        table.add_row("notifications.apprise_urls", ", ".join(cfg.notifications.apprise_urls) or "(none)")

        console.print(table)

        if cfg.subscriptions:
            sub_table = Table(title="Subscriptions")
            sub_table.add_column("Query", style="cyan")
            sub_table.add_column("Since")
            for sub in cfg.subscriptions:
                sub_table.add_row(sub.query, sub.since or "(all time)")
            console.print(sub_table)


@app.command()
def subscribe(
    query: str = typer.Argument(None, help="Magazine title to subscribe to"),
    since: str = typer.Option(None, "--since", help="Only fetch issues from this date (YYYY-MM)"),
    exact: bool = typer.Option(False, "--exact", help="Only download issues whose title matches exactly"),
):
    """Add a magazine subscription, or list current subscriptions."""
    cfg = load_config()

    if query is None:
        if not cfg.subscriptions:
            console.print("[yellow]No subscriptions configured.[/yellow]")
            console.print('Add one with: magsync subscribe "Magazine Name" --since 2025-01')
            raise typer.Exit()
        table = Table(title="Subscriptions")
        table.add_column("Query", style="cyan")
        table.add_column("Since")
        table.add_column("Match")
        for sub in cfg.subscriptions:
            table.add_row(
                sub.query,
                sub.since or "(all time)",
                "exact" if sub.exact else "partial",
            )
        console.print(table)
        raise typer.Exit()

    # Check for duplicate (accent-insensitive)
    for sub in cfg.subscriptions:
        if strip_accents(sub.query).lower() == strip_accents(query).lower():
            console.print(f"[yellow]Already subscribed to '{query}'[/yellow]")
            raise typer.Exit()

    cfg.subscriptions.append(Subscription(query=query, since=since, exact=exact))
    save_config(cfg)
    since_str = f" since {since}" if since else ""
    exact_str = " (exact match)" if exact else ""
    console.print(f"[green]Subscribed to '{query}'{since_str}{exact_str}[/green]")


@app.command()
def unsubscribe(
    query: str = typer.Argument(..., help="Magazine title to unsubscribe from"),
):
    """Remove a magazine subscription."""
    cfg = load_config()
    original_count = len(cfg.subscriptions)
    cfg.subscriptions = [s for s in cfg.subscriptions if strip_accents(s.query).lower() != strip_accents(query).lower()]

    if len(cfg.subscriptions) == original_count:
        console.print(f"[yellow]No subscription found for '{query}'[/yellow]")
        raise typer.Exit()

    save_config(cfg)
    console.print(f"[green]Unsubscribed from '{query}'[/green]")


HEALTH_CHECK_PATH = Path("/tmp/magsync-healthy")


@app.command()
def retry(
    query: str = typer.Argument(None, help="Only retry failed downloads for this magazine"),
):
    """Re-attempt all failed downloads."""
    cfg = load_config()
    idx = MagazineIndex()

    try:
        reset_count = idx.reset_failed_downloads(magazine_title=query)
        if reset_count == 0:
            console.print("[green]No failed downloads to retry.[/green]")
            raise typer.Exit()

        console.print(f"[cyan]Retrying {reset_count} failed download{'s' if reset_count != 1 else ''}...[/cyan]")

        pending = idx.get_issues(
            magazine_title=query,
            status=DownloadStatus.PENDING,
        )
        # Only include issues that have download links
        pending = [p for p in pending if p.get("limewire_url")]

        from magsync.core.batch import download_batch

        completed = [0]
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            overall = progress.add_task("Overall", total=len(pending))

            def on_start(issue):
                pass

            def on_complete(issue, success, error):
                completed[0] += 1
                progress.update(overall, completed=completed[0])
                title = issue["title"][:50]
                if success:
                    console.print(f"  [green]✓[/green] {title}")
                else:
                    console.print(f"  [red]✗[/red] {title}: {error}")

            asyncio.run(download_batch(pending, cfg, idx, on_start, on_complete))

        stats = idx.get_download_stats()
        console.print(
            f"\n[green]Done![/green] {stats['downloaded']} downloaded, "
            f"{stats['pending']} pending, {stats['failed']} failed"
        )
    finally:
        idx.close()


def _parse_interval(interval: str) -> int:
    """Parse interval string like '30m', '6h', '1d' to seconds."""
    m = re.fullmatch(r"(\d+)\s*(s|m|h|d)", interval.strip().lower())
    if not m:
        raise ValueError(f"Invalid interval format: '{interval}'. Use e.g. 30m, 6h, 1d")
    value, unit = int(m.group(1)), m.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return value * multipliers[unit]


@app.command()
def daemon(
    interval: str = typer.Option(
        None, "--interval", "-i",
        help="Time between cycles (e.g. 30m, 6h, 1d). Default: 6h",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Run one cycle, show what would be downloaded, then exit"),
):
    """Run magsync as a daemon, periodically fetching subscribed magazines."""
    from magsync import __version__
    from magsync.core.downloader import download_and_decrypt
    from magsync.core.notify import send_download_summary

    # Resolve interval: CLI arg > env var > default
    interval_str = interval or os.environ.get("MAGSYNC_INTERVAL", "6h")
    interval_secs = _parse_interval(interval_str)

    cfg = load_config()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logger = logging.getLogger("magsync")

    # Startup banner + initial health check touch
    HEALTH_CHECK_PATH.touch()
    logger.info(f"magsync v{__version__} daemon starting")
    logger.info(f"  Output directory: {cfg.output_dir}")
    logger.info(f"  Subscriptions: {len(cfg.subscriptions)}")
    logger.info(f"  Interval: {interval_str} ({interval_secs}s)")
    logger.info(f"  Notifications: {'enabled' if cfg.notifications.enabled else 'disabled'}")
    for sub in cfg.subscriptions:
        since_str = f" (since {sub.since})" if sub.since else ""
        logger.info(f"    - {sub.query}{since_str}")

    if not cfg.subscriptions:
        logger.warning("No subscriptions configured. Add with 'magsync subscribe' or MAGSYNC_SUBSCRIPTIONS env var.")

    # SIGTERM handler
    shutdown = False

    def handle_sigterm(signum, frame):
        nonlocal shutdown
        logger.info("Received shutdown signal, finishing current work...")
        shutdown = True

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    # Daemon loop
    while not shutdown:
        cycle_start = time.time()
        logger.info("Starting cycle...")

        idx = MagazineIndex()
        downloaded_issues: list[dict] = []
        total_new_indexed = 0

        try:
            # Phase 1: Update index for each subscription
            for sub in cfg.subscriptions:
                if shutdown:
                    break
                logger.info(f"Searching: {sub.query}")
                try:
                    results = asyncio.run(
                        search_with_details(sub.query, scrape_delay=cfg.download.scrape_delay)
                    )
                except Exception as e:
                    logger.error(f"Search failed for '{sub.query}': {e}")
                    continue

                if not results:
                    continue

                results = _filter_results(results, sub.query, sub.exact)
                if not results:
                    logger.info(f"  {sub.query}: no exact matches found")
                    continue

                norm = normalize_title(results[0].title) if results[0].title else sub.query
                mag_id = idx.get_or_create_magazine(sub.query, norm)
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
                new = idx.add_issues(mag_id, issues_data)
                total_new_indexed += new
                if new:
                    logger.info(f"  {sub.query}: {new} new issues indexed")

            # Phase 2: Collect all pending issues across subscriptions, then download concurrently
            all_pending: list[dict] = []
            for sub in cfg.subscriptions:
                if shutdown:
                    break

                since_year = since_month = None
                if sub.since:
                    parts = sub.since.split("-")
                    since_year = int(parts[0])
                    since_month = int(parts[1]) if len(parts) > 1 else None

                norm = normalize_title(sub.query) if sub.query else sub.query
                pending = idx.get_issues(
                    magazine_title=norm,
                    since_year=since_year,
                    since_month=since_month,
                    status=DownloadStatus.PENDING,
                )
                all_pending.extend(p for p in pending if p.get("limewire_url"))

            if all_pending and not shutdown:
                if dry_run:
                    logger.info(f"Dry run — would download {len(all_pending)} issues:")
                    for issue in all_pending:
                        logger.info(f"  - {issue['title']} ({issue.get('file_size') or '?'})")
                else:
                    from magsync.core.batch import download_batch

                    logger.info(f"Downloading {len(all_pending)} issues (max {cfg.download.max_concurrent} concurrent)...")

                    def on_start(issue):
                        logger.info(f"  Downloading: {issue['title'][:60]}")

                    def on_complete(issue, success, error):
                        if success:
                            downloaded_issues.append(issue)
                            logger.info(f"  Done: {issue['title'][:60]}")
                        else:
                            logger.error(f"  Failed: {issue['title'][:60]}: {error}")

                    asyncio.run(download_batch(all_pending, cfg, idx, on_start, on_complete))

            # Phase 3: Notify
            if downloaded_issues:
                send_download_summary(downloaded_issues, cfg.notifications)

            # Phase 4: Health check
            HEALTH_CHECK_PATH.touch()

            # Phase 5: Summary
            elapsed = int(time.time() - cycle_start)
            logger.info(
                f"Cycle complete in {elapsed}s: "
                f"{total_new_indexed} new indexed, "
                f"{len(downloaded_issues)} downloaded"
            )

        except Exception as e:
            logger.error(f"Cycle error: {e}")
        finally:
            idx.close()

        if shutdown or dry_run:
            break

        # Sleep with interrupt support
        logger.info(f"Sleeping {interval_str} until next cycle...")
        sleep_end = time.time() + interval_secs
        while time.time() < sleep_end and not shutdown:
            time.sleep(min(5, sleep_end - time.time()))

    logger.info("magsync daemon stopped.")
