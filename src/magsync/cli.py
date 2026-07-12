"""CLI interface for magsync using Typer."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import typer
from rich.console import Console
from rich.table import Table

from magsync.config import load_config, save_config, set_config_value
from magsync.core.diagnostics import sanitize_external_error
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadStatus,
    SourceError,
    SourceFailure,
    SourceFailureKind,
    Subscription,
)
from magsync.core.organizer import normalize_title, parse_date, strip_accents
from magsync.core.scraper import (
    FreemagazinesClient,
    scrape_detail_page,
    search_with_details_result,
)
from magsync.output import BatchOutput, resolve_mode

app = typer.Typer(
    name="magsync",
    help="Index and download magazines from freemagazines.top",
    no_args_is_help=False,
    invoke_without_command=True,
)
console = Console()


def _reject_conflicting_flags(verbose: bool, quiet: bool) -> None:
    """Fail fast (before any work) if mutually exclusive flags are combined."""
    if verbose and quiet:
        console.print("[red]--verbose and --quiet are mutually exclusive[/red]")
        raise typer.Exit(2)


def _batch_output(total: int, title: str, verbose: bool, quiet: bool, no_progress: bool) -> BatchOutput:
    """Build a coordinated progress/logging surface for a bulk command."""
    use_live_bar, log_level = resolve_mode(verbose, quiet, no_progress)
    return BatchOutput(
        console, total, title=title, use_live_bar=use_live_bar, log_level=log_level, verbose=verbose
    )


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


def _index_results(results, idx: MagazineIndex, cfg, subscription=None) -> int:
    """Index scraped results, grouping by normalized title.

    Each unique normalized title gets its own magazine entry.
    Returns total new issues added.

    ``subscription`` is the subscription whose search produced these results,
    when there is one: matching rows record subscription provenance (and
    null-provenance re-encounters are promoted); fuzzy strangers are cataloged
    without provenance and are never claimable work.
    """
    from collections import defaultdict

    # Group results by normalized title
    by_magazine: dict[str, list] = defaultdict(list)
    for r in results:
        norm = normalize_title(r.title) if r.title else "Unknown"
        by_magazine[norm].append(r)

    total_new = 0
    for norm_title, issues in by_magazine.items():
        display_title = norm_title
        mag_id = idx.get_or_create_magazine(display_title, strip_accents(norm_title).lower())
        issues_data = []
        for r in issues:
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
        total_new += idx.add_issues(mag_id, issues_data, subscription=subscription)

    return total_new


def _cli_source_failure_message(failure: SourceFailure) -> str:
    """Render concise, secret-safe CLI guidance from typed source state."""
    prefix = {
        SourceFailureKind.ACCESS_BLOCKED: "Source access is blocked; retry later",
        SourceFailureKind.TRANSIENT: "Source is temporarily unavailable; retry later",
        SourceFailureKind.PROTOCOL: "Source response could not be validated",
    }[failure.kind]
    context: list[str] = []
    if failure.message and failure.message.casefold() not in prefix.casefold():
        context.append(failure.message)
    if failure.status_code is not None:
        context.append(f"status={failure.status_code}")
    if failure.host:
        context.append(f"host={failure.host}")
    if failure.cf_ray:
        context.append(f"cf_ray={failure.cf_ray}")
    detail = sanitize_external_error("; ".join(context))
    return f"{prefix}: {detail}" if detail else prefix


def _print_source_failure(failure: SourceFailure) -> None:
    console.print(
        _cli_source_failure_message(failure),
        style="red",
        markup=False,
        highlight=False,
    )


def _print_partial_details(count: int) -> None:
    if count:
        console.print(
            f"Source results are incomplete: {count} detail page(s) were omitted.",
            style="yellow",
            markup=False,
            highlight=False,
        )


@app.command()
def search(
    query: str = typer.Argument(..., help="Magazine title to search for"),
):
    """Search for magazines and display results."""
    cfg = load_config()

    async def _search():
        async with FreemagazinesClient(
            scrape_delay=cfg.download.scrape_delay
        ) as source_client:
            return await search_with_details_result(query, client=source_client)

    with console.status(f"Searching for '{query}'..."):
        source_result = asyncio.run(_search())

    if source_result.failure is not None:
        _print_source_failure(source_result.failure)
        raise typer.Exit(1)
    if source_result.validated_empty:
        console.print(f"[yellow]No results found for '{query}'[/yellow]")
        raise typer.Exit()
    if not source_result.items:
        console.print(
            "Source response could not be validated: no issues were returned "
            "without a recognized no-results marker.",
            style="red",
            markup=False,
        )
        raise typer.Exit(1)

    results = source_result.items
    detail_failures = len(source_result.failures)
    _print_partial_details(detail_failures)

    # Index the results (grouped by normalized title)
    idx = MagazineIndex()
    try:
        new_count = _index_results(results, idx, cfg)

        # Display results
        norm = strip_accents(query).lower()
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
            # Never-requested rows are catalog entries, not queued work — a
            # parked side-effect row must not present itself as "pending".
            if status not in ("complete", "downloading") and issue.get(
                "requested_by"
            ) not in ("manual", "subscription"):
                status = "cataloged"
            status_style = {
                "complete": "[green]done[/green]",
                "pending": "[dim]pending[/dim]",
                "cataloged": "[dim italic]cataloged[/dim italic]",
                "failed": "[red]failed[/red]",
                "downloading": "[yellow]downloading[/yellow]",
                "unavailable": "[red dim]unavailable[/red dim]",
                "unsupported": "[magenta]unsupported[/magenta]",
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
    if detail_failures:
        raise typer.Exit(1)


@app.command()
def fetch(
    query: str = typer.Argument(..., help="Magazine title to fetch"),
    since: str = typer.Option(None, "--since", help="Fetch issues from this date (YYYY-MM)"),
    output: str = typer.Option(None, "--output", "-o", help="Output directory override"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be downloaded without downloading"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show per-issue detail (dead-link logs, ✓/✗ lines)"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Only show the final summary (errors still surface)"),
    no_progress: bool = typer.Option(False, "--no-progress", help="Disable the live progress bar"),
):
    """Search, index, and download magazines."""
    _reject_conflicting_flags(verbose, quiet)
    cfg = load_config()
    if output:
        cfg.output_dir = output

    # Parse --since
    since_year = since_month = None
    if since:
        parts = since.split("-")
        since_year = int(parts[0])
        since_month = int(parts[1]) if len(parts) > 1 else None

    idx = MagazineIndex()
    try:
        async def _run_fetch() -> int:
            from magsync.core.batch import download_batch

            async with FreemagazinesClient(
                scrape_delay=cfg.download.scrape_delay
            ) as source_client:
                with console.status(f"Searching for '{query}'..."):
                    source_result = await search_with_details_result(
                        query,
                        client=source_client,
                    )

                if source_result.failure is not None:
                    _print_source_failure(source_result.failure)
                    return 1
                if source_result.validated_empty:
                    console.print(f"[yellow]No results found for '{query}'[/yellow]")
                    return 0
                if not source_result.items:
                    console.print(
                        "Source response could not be validated: no issues were "
                        "returned without a recognized no-results marker.",
                        style="red",
                        markup=False,
                    )
                    return 1

                detail_failures = len(source_result.failures)
                _print_partial_details(detail_failures)
                _index_results(source_result.items, idx, cfg)

                # Record explicit intent for everything in this fetch's query
                # scope — every non-complete status, not just pending — so a
                # parked (never-requested) failure becomes recoverable via
                # `magsync retry`. Provenance backfill first, then manual
                # marking; the download set below stays pending-only. Dry runs
                # mutate nothing.
                norm = strip_accents(query).lower()
                if not dry_run:
                    idx.promote_subscribed(cfg.subscriptions)
                    scope = idx.get_issues(
                        magazine_title=norm,
                        since_year=since_year,
                        since_month=since_month,
                    )
                    non_complete = [
                        i for i in scope
                        if i.get("download_status") != "complete"
                    ]
                    if non_complete:
                        idx.mark_manual([i["id"] for i in non_complete])
                    recoverable = sum(
                        1 for i in non_complete
                        if i.get("download_status") in ("failed", "unavailable")
                    )
                    if recoverable:
                        console.print(
                            f"[yellow]{recoverable} previously failed/unavailable "
                            f"issue{'s' if recoverable != 1 else ''} marked as "
                            "requested — run 'magsync retry' to attempt "
                            "them.[/yellow]"
                        )

                pending = idx.get_issues(
                    magazine_title=norm,
                    since_year=since_year,
                    since_month=since_month,
                    status=DownloadStatus.PENDING,
                )

                if not pending:
                    console.print(
                        "[green]All matching issues already downloaded![/green]"
                    )
                    return 1 if detail_failures else 0

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
                                total_size += int(
                                    "".join(c for c in size_str if c.isdigit())
                                )
                            except ValueError:
                                pass
                    console.print(table)
                    if total_size:
                        console.print(
                            f"\n[dim]Estimated total: ~{total_size} MB[/dim]"
                        )
                    console.print("\n[yellow]Dry run — no files downloaded.[/yellow]")
                    return 1 if detail_failures else 0

                console.print(
                    f"[cyan]Downloading {len(pending)} issues "
                    f"(max {cfg.download.max_concurrent} concurrent)...[/cyan]"
                )
                with _batch_output(
                    len(pending), "Downloading", verbose, quiet, no_progress
                ) as out:
                    batch_results = await download_batch(
                        pending,
                        cfg,
                        idx,
                        out.on_start,
                        out.on_complete,
                        source_client=source_client,
                    )
                out.summarize(batch_results)
                return 1 if detail_failures else 0

        exit_code = asyncio.run(_run_fetch())
    finally:
        idx.close()
    if exit_code:
        raise typer.Exit(exit_code)


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

        async def _run_update() -> int:
            total_new = 0
            incomplete = 0
            skipped = 0
            async with FreemagazinesClient(
                scrape_delay=cfg.download.scrape_delay
            ) as source_client:
                for position, mag in enumerate(magazines):
                    with console.status(f"Updating '{mag['title']}'..."):
                        source_result = await search_with_details_result(
                            mag["title"],
                            client=source_client,
                        )

                    if source_result.failure is not None:
                        incomplete += 1
                        console.print(
                            f"Update for '{mag['title']}' failed:",
                            style="red",
                            markup=False,
                        )
                        _print_source_failure(source_result.failure)
                        if (
                            source_result.failure.kind
                            is SourceFailureKind.ACCESS_BLOCKED
                        ):
                            skipped = len(magazines) - position - 1
                            break
                        continue

                    detail_failures = len(source_result.failures)
                    new = _index_results(source_result.items, idx, cfg)
                    total_new += new
                    if detail_failures:
                        incomplete += 1
                        console.print(
                            f"  {mag['title']}: {new} new issues; "
                            f"{detail_failures} detail page(s) omitted",
                            style="yellow",
                            markup=False,
                        )
                    elif new:
                        console.print(
                            f"  [cyan]{mag['title']}[/cyan]: {new} new issues"
                        )
                    else:
                        console.print(f"  [dim]{mag['title']}: up to date[/dim]")

            if incomplete or skipped:
                console.print(
                    f"\nUpdate incomplete: {total_new} new issues; "
                    f"{incomplete} source operation(s) incomplete; "
                    f"{skipped} skipped after source blocking.",
                    style="yellow",
                    markup=False,
                )
                return 1
            console.print(
                f"\n[green]Update complete.[/green] {total_new} new issues found."
            )
            return 0

        exit_code = asyncio.run(_run_update())
    finally:
        idx.close()
    if exit_code:
        raise typer.Exit(exit_code)


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


def _start_heartbeat(interval: int = 30) -> Callable:
    """Start a daemon thread that touches the health check file every `interval` seconds.

    Returns a stop function.
    """
    import threading

    stop_event = threading.Event()

    def _beat():
        while not stop_event.is_set():
            try:
                HEALTH_CHECK_PATH.touch()
            except OSError:
                pass
            stop_event.wait(interval)

    t = threading.Thread(target=_beat, daemon=True)
    t.start()
    return stop_event.set


@app.command()
def retry(
    query: str = typer.Argument(None, help="Only retry failed downloads for this magazine"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show per-issue detail (dead-link logs, ✓/✗ lines)"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Only show the final summary (errors still surface)"),
    no_progress: bool = typer.Option(False, "--no-progress", help="Disable the live progress bar"),
):
    """Re-attempt all failed downloads."""
    _reject_conflicting_flags(verbose, quiet)
    cfg = load_config()
    idx = MagazineIndex()

    try:
        # Provenance backfill first: legacy rows matching a current
        # subscription become wanted before the snapshot is taken.
        idx.promote_subscribed(cfg.subscriptions)

        # Atomically claim exactly the wanted failed/unavailable invocation
        # snapshot, bypassing persisted schedules without touching the pending
        # backlog. Never-requested (null-provenance) rows are excluded and
        # reported with their recovery path.
        claimed, skipped, excluded = idx.claim_manual_retry_downloads(
            magazine_title=query
        )
        excluded_msg = (
            f"[yellow]{excluded} failed/unavailable row"
            f"{'s' if excluded != 1 else ''} excluded: never requested "
            f"(subscribe, or 'magsync fetch \"<title>\"' first, then retry).[/yellow]"
            if excluded else None
        )
        if not claimed and not skipped:
            if excluded_msg:
                console.print(excluded_msg)
            else:
                console.print("[green]No failed downloads to retry.[/green]")
            raise typer.Exit()

        skipped_msg = (
            f"[yellow]{skipped} failed download{'s' if skipped != 1 else ''} "
            f"skipped: no download link (run 'magsync backfill-urls' to repair).[/yellow]"
            if skipped else None
        )
        if not claimed:
            console.print(skipped_msg)
            if excluded_msg:
                console.print(excluded_msg)
            raise typer.Exit()

        console.print(
            f"[cyan]Retrying {len(claimed)} failed download"
            f"{'s' if len(claimed) != 1 else ''}...[/cyan]"
        )

        from magsync.core.batch import download_batch

        with _batch_output(
            len(claimed), "Retrying", verbose, quiet, no_progress
        ) as out:
            results = asyncio.run(
                download_batch(
                    claimed,
                    cfg,
                    idx,
                    out.on_start,
                    out.on_complete,
                )
            )
        out.summarize(results)
        if skipped_msg:
            console.print(skipped_msg)
        if excluded_msg:
            console.print(excluded_msg)
    finally:
        idx.close()


@app.command(name="backfill-urls")
def backfill_urls(
    query: str = typer.Argument(None, help="Only backfill issues for this magazine"),
    include_all: bool = typer.Option(
        False, "--all",
        help="Repair the full catalog, including never-requested rows",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show per-issue detail"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Only show the final summary (errors still surface)"),
    no_progress: bool = typer.Option(False, "--no-progress", help="Disable the live progress bar"),
):
    """Re-scrape issues missing a download URL and repair them.

    Useful after a site template change leaves indexed issues without a LimeWire
    URL. `magsync update` repairs these automatically too; this command targets
    only the broken rows and also reaches de-tracked magazines. By default only
    wanted (requested) rows are repaired; --all covers the whole catalog.
    """
    _reject_conflicting_flags(verbose, quiet)
    cfg = load_config()
    idx = MagazineIndex()

    try:
        idx.promote_subscribed(cfg.subscriptions)
        missing = idx.get_issues_missing_url(
            magazine_title=query, wanted_only=not include_all
        )
        parked_skipped = 0
        if not include_all:
            parked_skipped = len(
                idx.get_issues_missing_url(magazine_title=query)
            ) - len(missing)
        if parked_skipped:
            console.print(
                f"[dim]{parked_skipped} never-requested issue"
                f"{'s' if parked_skipped != 1 else ''} skipped "
                "(use --all to include them).[/dim]"
            )
        if not missing:
            console.print("[green]No issues missing a download URL.[/green]")
            raise typer.Exit()

        console.print(
            f"[cyan]Re-scraping {len(missing)} issue"
            f"{'s' if len(missing) != 1 else ''} missing a download URL...[/cyan]"
        )

        async def _backfill(out: BatchOutput) -> int:
            failures = 0
            async with FreemagazinesClient(
                scrape_delay=cfg.download.scrape_delay
            ) as source_client:
                for row in missing:
                    title = sanitize_external_error(
                        (row["title"] or row["page_url"])[:50]
                    )
                    if source_client.circuit_open:
                        failures += 1
                        if out.verbose:
                            console.print(
                                f"  – {title}: skipped after source blocking",
                                style="yellow",
                                markup=False,
                                highlight=False,
                            )
                        out.record("skipped")
                        continue
                    try:
                        detail = await scrape_detail_page(
                            row["page_url"], client=source_client
                        )
                        if detail.limewire_url:
                            idx.set_limewire_url(row["id"], detail.limewire_url)
                            if out.verbose:
                                console.print(f"  [green]✓[/green] {title}")
                            out.record("repaired")
                        else:
                            if out.verbose:
                                console.print(
                                    f"  [dim]–[/dim] {title}: still no URL"
                                )
                            out.record("missing")
                    except asyncio.CancelledError:
                        raise
                    except SourceError as exc:
                        failures += 1
                        label = {
                            SourceFailureKind.ACCESS_BLOCKED: "blocked",
                            SourceFailureKind.TRANSIENT: "transient",
                            SourceFailureKind.PROTOCOL: "protocol",
                        }[exc.kind]
                        if out.verbose:
                            console.print(
                                f"  ✗ {title}: "
                                f"{_cli_source_failure_message(exc.failure)}",
                                style="red",
                                markup=False,
                                highlight=False,
                            )
                        out.record(label)
                    except Exception:
                        failures += 1
                        if out.verbose:
                            console.print(
                                f"  ✗ {title}: unable to process source detail",
                                style="red",
                                markup=False,
                                highlight=False,
                            )
                        out.record("error")
            return failures

        with _batch_output(len(missing), "Backfilling", verbose, quiet, no_progress) as out:
            failure_count = asyncio.run(_backfill(out))
        repaired = out.counts.get("repaired", 0)
        checked_missing = out.counts.get("missing", 0)
        blocked = out.counts.get("blocked", 0)
        skipped = out.counts.get("skipped", 0)
        if failure_count:
            console.print(
                f"\nBackfill incomplete. {repaired} repaired, "
                f"{checked_missing} checked with no URL, {blocked} blocked, "
                f"{skipped} skipped, {failure_count - blocked - skipped} failed.",
                style="yellow",
                markup=False,
            )
        else:
            console.print(
                f"\n[green]Backfill complete.[/green] {repaired} repaired, "
                f"{checked_missing} still missing a URL."
            )
    finally:
        idx.close()
    if failure_count:
        raise typer.Exit(1)


def _parse_interval(interval: str) -> int:
    """Parse interval string like '30m', '6h', '1d' to seconds."""
    m = re.fullmatch(r"(\d+)\s*(s|m|h|d)", interval.strip().lower())
    if not m:
        raise ValueError(f"Invalid interval format: '{interval}'. Use e.g. 30m, 6h, 1d")
    value, unit = int(m.group(1)), m.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return value * multipliers[unit]


class _DaemonRedactionFilter(logging.Filter):
    """Sanitize a fully-rendered daemon log record before it reaches a sink."""

    def filter(self, record: logging.LogRecord) -> bool:
        from magsync.core.diagnostics import sanitize_external_error

        try:
            rendered = record.getMessage()
        except Exception:
            rendered = "Unable to render log message safely"
        record.msg = sanitize_external_error(rendered, max_length=2_000)
        record.args = ()
        # Exception repr/traceback text can contain a presigned URL or key. All
        # daemon error paths log the sanitized operation context explicitly.
        record.exc_info = None
        record.exc_text = None
        return True


def _configure_daemon_external_logging() -> None:
    """Keep third-party request URLs and raw exception text out of daemon logs."""

    for name in ("httpx", "httpcore"):
        external_logger = logging.getLogger(name)
        external_logger.setLevel(logging.CRITICAL + 1)
        external_logger.propagate = False

    root = logging.getLogger()
    for handler in root.handlers:
        if not any(isinstance(item, _DaemonRedactionFilter) for item in handler.filters):
            handler.addFilter(_DaemonRedactionFilter())


def _source_failure_reason(failure: Any) -> str:
    """Render only the safe fields carried by a structured source failure."""

    from magsync.core.diagnostics import sanitize_external_error

    parts = [getattr(getattr(failure, "kind", None), "value", "source_failure")]
    message = getattr(failure, "message", None)
    if message:
        parts.append(str(message))
    status_code = getattr(failure, "status_code", None)
    if status_code is not None:
        parts.append(f"status={status_code}")
    host = getattr(failure, "host", None)
    if host:
        parts.append(f"host={host}")
    cf_ray = getattr(failure, "cf_ray", None)
    if cf_ray:
        parts.append(f"cf_ray={cf_ray}")
    return sanitize_external_error("; ".join(parts))


def _batch_failure_kind(result: dict):
    """Read a batch result's typed kind without consulting display text."""

    from magsync.core.models import DownloadFailureKind

    value = result.get("failure_kind")
    if value is None:
        nested = result.get("result")
        value = getattr(nested, "failure_kind", None)
    try:
        return DownloadFailureKind(value) if value is not None else DownloadFailureKind.INTERNAL
    except (TypeError, ValueError):
        return DownloadFailureKind.INTERNAL


def _reconcile_download_results(
    report,
    results: list[dict],
    logger: logging.Logger,
) -> list[dict]:
    """Update a cycle report solely from returned typed batch results."""

    from magsync.core.diagnostics import sanitize_external_error
    from magsync.core.models import DownloadSummaryBucket
    from magsync.core.policy import get_download_failure_policy

    downloaded: list[dict] = []
    for result in results:
        issue = result.get("issue") or {}
        title = sanitize_external_error(issue.get("title") or "Unknown issue", 120)
        if result.get("success"):
            report.downloads_complete += 1
            downloaded.append(issue)
            logger.info("  Done: %s", title)
            continue

        kind = _batch_failure_kind(result)
        policy = get_download_failure_policy(kind)
        if policy.summary_bucket is DownloadSummaryBucket.UNAVAILABLE:
            report.downloads_unavailable += 1
            label = "Unavailable"
        elif policy.summary_bucket is DownloadSummaryBucket.UNSUPPORTED:
            report.downloads_unsupported += 1
            label = "Skipped (unsupported)"
        else:
            report.downloads_failed += 1
            label = "Failed"

        detail = sanitize_external_error(result.get("error") or kind.value)
        logger.log(policy.log_level, "  %s: %s: %s", label, title, detail)
    return downloaded


def _log_cycle_report(report, logger: logging.Logger) -> None:
    """Emit one reconciled, secret-safe phase summary."""

    from magsync.core.models import PipelineStatus

    level = {
        PipelineStatus.HEALTHY: logging.INFO,
        PipelineStatus.DEGRADED: logging.WARNING,
        PipelineStatus.FAILED: logging.ERROR,
    }[report.status]
    reason = f"; reason={report.reason}" if report.reason else ""
    logger.log(
        level,
        (
            "Cycle %s in %.1fs: source %d/%d completed "
            "(%d attempted, %d empty, %d failed, %d skipped, %d detail failures); "
            "downloads %d queued/%d unique "
            "(%d complete, %d unavailable, %d unsupported, %d failed); "
            "%d refreshes pending%s"
        ),
        report.status.value,
        report.elapsed_seconds,
        report.source_completed,
        report.source_total,
        report.source_attempted,
        report.source_empty,
        report.source_failed,
        report.source_skipped,
        report.detail_failures,
        report.downloads_queued,
        report.downloads_unique,
        report.downloads_complete,
        report.downloads_unavailable,
        report.downloads_unsupported,
        report.downloads_failed,
        report.pending_refreshes,
        reason,
    )


async def _run_daemon_cycle(
    cfg,
    idx: MagazineIndex,
    *,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
    clock: Callable[[], float] = time.monotonic,
    source_client_factory: Callable[..., Any] | None = None,
    subscriptions: list[Any] | None = None,
    config_failure_reason: str | None = None,
) -> Any:
    """Run one complete daemon cycle in one event loop and source session.

    Subscription indexing, due source-only refreshes, and cached downloads all
    share the same ``FreemagazinesClient`` and therefore the same cookies,
    request pacing, and challenge circuit. Returned batch results, never
    callbacks, are the source of download counters.

    ``subscriptions`` is the cycle's subscription snapshot (defaults to
    ``cfg.subscriptions``); the daemon loop passes a freshly re-read snapshot
    each cycle so config-file edits take effect without restart. Every phase —
    indexing, refresh claiming, download claiming — uses this exact snapshot.
    ``config_failure_reason`` marks the cycle degraded when the loop had to
    fall back to a stale snapshot.
    """

    from magsync.core.batch import download_batch, refresh_due_links
    from magsync.core.diagnostics import sanitize_external_error
    from magsync.core.models import CycleReport, PipelineStatus, SourceFailureKind
    from magsync.core.notify import send_download_summary
    from magsync.core.scraper import FreemagazinesClient
    from magsync.core.urls import URLValidationError, normalize_limewire_share_url

    daemon_logger = logger or logging.getLogger("magsync")
    if subscriptions is None:
        subscriptions = cfg.subscriptions
    report = CycleReport(source_total=len(subscriptions))
    started = clock()
    cycle_at = now or datetime.now(timezone.utc)
    source_expected = bool(subscriptions)
    source_failed = False
    source_reason: str | None = None
    fatal_reason: str | None = None
    downloaded_issues: list[dict] = []

    factory = source_client_factory or FreemagazinesClient
    try:
        # Provenance backfill with this cycle's snapshot: promotes legacy or
        # newly subscribed titles so the scoped claims below can see them.
        # Idempotent, title-only (matching.py). Runs for dry runs too so the
        # preview matches what a real cycle would claim.
        promoted = idx.promote_subscribed(subscriptions)
        if promoted:
            daemon_logger.info(
                "Promoted %d cataloged row(s) to subscription provenance", promoted
            )

        async with factory(scrape_delay=cfg.download.scrape_delay) as source_client:
            # Phase 1: subscription indexing. A challenge result opens the
            # client's circuit; no later subscription is even invoked.
            for position, sub in enumerate(subscriptions):
                if source_client.circuit_open:
                    report.source_skipped += len(subscriptions) - position
                    source_failed = True
                    failure = source_client.circuit_failure
                    if source_reason is None and failure is not None:
                        source_reason = _source_failure_reason(failure)
                    break

                daemon_logger.info("Searching: %s", sub.query)
                report.source_attempted += 1
                source_result = await source_client.search_with_details(sub.query)
                report.detail_failures += len(source_result.failures)
                if (
                    source_result.failure is not None
                    and source_result.failure.operation == "detail"
                ):
                    # When every advertised detail fails, the scraper promotes
                    # one detail failure to the operation-level failure and
                    # retains the remaining siblings in ``failures``.
                    report.detail_failures += 1
                blocked_result = False

                if source_result.failure is not None:
                    report.source_failed += 1
                    source_failed = True
                    blocked_result = (
                        source_result.failure.kind
                        is SourceFailureKind.ACCESS_BLOCKED
                    )
                    if source_reason is None:
                        source_reason = _source_failure_reason(source_result.failure)
                    daemon_logger.warning(
                        "Search failed for %s: %s",
                        sanitize_external_error(sub.query, 120),
                        _source_failure_reason(source_result.failure),
                    )
                else:
                    if source_result.validated_empty:
                        report.source_empty += 1
                    else:
                        report.source_succeeded += 1

                    filtered = _filter_results(
                        source_result.items, sub.query, sub.exact
                    )
                    new = (
                        _index_results(filtered, idx, cfg, subscription=sub)
                        if filtered
                        else 0
                    )
                    if new:
                        daemon_logger.info("  %s: %d new issues indexed", sub.query, new)

                    if source_result.failures:
                        source_failed = True
                        if source_reason is None:
                            source_reason = (
                                f"{len(source_result.failures)} source detail "
                                "request(s) failed"
                            )

                if blocked_result:
                    report.source_skipped += len(subscriptions) - position - 1
                    break

                # A detail request can open the circuit while still preserving
                # valid siblings, so check again after the structured result.
                if source_client.circuit_open:
                    remaining = len(subscriptions) - position - 1
                    report.source_skipped += remaining
                    source_failed = True
                    failure = source_client.circuit_failure
                    if source_reason is None and failure is not None:
                        source_reason = _source_failure_reason(failure)
                    break

            # Phase 2: claim source-only refresh actions before downloads. This
            # path never invokes the known-dead stored LimeWire URL. A circuit
            # opened above is reused and short-circuits these source calls.
            if not dry_run:
                due_refreshes = idx.claim_due_link_refreshes(
                    subscriptions, now=cycle_at
                )
                if due_refreshes:
                    source_expected = True
                    refresh_results = await refresh_due_links(
                        due_refreshes, idx, source_client
                    )
                    for refresh_result in refresh_results:
                        outcome = refresh_result.get("outcome")
                        failure = getattr(outcome, "failure", None)
                        if failure is not None:
                            source_failed = True
                            if source_reason is None:
                                source_reason = _source_failure_reason(failure)
                        if refresh_result.get("failure_kind") is not None:
                            source_failed = True
                            if source_reason is None:
                                source_reason = "Unable to persist source refresh result"

            # Phase 3: wanted pending and due transient downloads are claimed
            # every cycle, even if source indexing was blocked. The dry-run
            # preview shares the claim's exact predicates so it can never show
            # a set the real claim would not take (or hide one it would).
            dry_run_due_refreshes = 0
            if dry_run:
                claimed, dry_run_due_refreshes = idx.preview_claimable_downloads(
                    subscriptions, now=cycle_at
                )
            else:
                claimed = idx.claim_pending_and_due_downloads(
                    subscriptions, now=cycle_at
                )

            report.downloads_queued = len(claimed)
            identities: set[str] = set()
            for issue in claimed:
                try:
                    identities.add(
                        normalize_limewire_share_url(issue.get("limewire_url") or "")
                    )
                except URLValidationError:
                    identities.add(f"invalid-issue:{issue.get('id')}")
            report.downloads_unique = len(identities)

            if dry_run:
                if claimed:
                    daemon_logger.info("Dry run - would download %d issues", len(claimed))
                if dry_run_due_refreshes:
                    daemon_logger.info(
                        "Dry run - %d due link refresh(es) would be attempted",
                        dry_run_due_refreshes,
                    )
            elif claimed:
                daemon_logger.info(
                    "Downloading %d issues (%d unique URLs; max %d concurrent)...",
                    len(claimed),
                    report.downloads_unique,
                    cfg.download.max_concurrent,
                )

                def on_start(issue: dict) -> None:
                    title = sanitize_external_error(
                        issue.get("title") or "Unknown issue", 120
                    )
                    daemon_logger.info("  Downloading: %s", title)

                results = await download_batch(
                    claimed,
                    cfg,
                    idx,
                    on_start=on_start,
                    source_client=source_client,
                )
                downloaded_issues = _reconcile_download_results(
                    report, results, daemon_logger
                )
                missing_results = max(0, len(claimed) - len(results))
                if missing_results:
                    report.downloads_failed += missing_results
                    source_reason = source_reason or (
                        f"Batch omitted {missing_results} claimed result(s)"
                    )

            # An immediate dead-link refresh inside the batch may be the first
            # operation to encounter a host-wide source challenge.
            if source_client.circuit_open:
                source_expected = True
                source_failed = True
                failure = source_client.circuit_failure
                if source_reason is None and failure is not None:
                    source_reason = _source_failure_reason(failure)

        if downloaded_issues:
            send_download_summary(downloaded_issues, cfg.notifications)
        report.pending_refreshes = idx.count_pending_link_refreshes()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        fatal_reason = sanitize_external_error(exc)

    if fatal_reason is not None:
        report.status = PipelineStatus.FAILED
        report.reason = fatal_reason or "Local daemon cycle failure"
    elif (
        source_failed
        or report.detail_failures
        or report.downloads_failed
        or config_failure_reason is not None
    ):
        report.status = PipelineStatus.DEGRADED
        report.reason = source_reason
        if report.reason is None and report.detail_failures:
            report.reason = f"{report.detail_failures} detail request(s) failed"
        if report.reason is None and report.downloads_failed:
            report.reason = f"{report.downloads_failed} download(s) failed"
        if report.reason is None and config_failure_reason is not None:
            report.reason = config_failure_reason
    else:
        report.status = PipelineStatus.HEALTHY

    report.reason = sanitize_external_error(report.reason) if report.reason else None
    report.elapsed_seconds = max(0.0, clock() - started)

    source_validated: bool | None
    if not source_expected:
        source_validated = None
    else:
        source_validated = not source_failed and report.detail_failures == 0

    try:
        idx.update_pipeline_state(
            report.status,
            cycle_at=cycle_at,
            source_validated=source_validated,
            source_check_at=cycle_at,
            degraded_reason=report.reason,
        )
    except Exception as exc:
        report.status = PipelineStatus.FAILED
        report.reason = sanitize_external_error(exc) or "Unable to persist pipeline state"
        daemon_logger.error("Unable to persist pipeline state: %s", report.reason)

    _log_cycle_report(report, daemon_logger)
    return report


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
    _configure_daemon_external_logging()
    logger = logging.getLogger("magsync")

    # Start background heartbeat for Docker health check
    stop_heartbeat = _start_heartbeat(interval=30)

    # Recover interrupted work only. Typed failure schedules and their UTC due
    # times survive restarts and are claimed by ordinary cycles when eligible.
    # Then run the idempotent provenance backfill: legacy rows matching a
    # subscription become wanted; never-subscribed rows park as cataloged
    # (requested_by NULL) and are permanently invisible to automatic claims.
    startup_idx = MagazineIndex()
    stuck = startup_idx.reset_stuck_downloads()
    promoted = startup_idx.promote_subscribed(cfg.subscriptions)
    startup_idx.close()
    if stuck:
        logger.info("Reset %d interrupted download(s) to pending", stuck)
    if promoted:
        logger.info(
            "Promoted %d cataloged row(s) to subscription provenance", promoted
        )

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

    # Daemon loop. Each cycle gets exactly one asyncio.run(), so the source
    # session, its circuit, due refreshes, and downloads share one event loop.
    # Subscriptions are re-read every cycle so config-file edits (unsubscribe,
    # since changes) take effect without a restart; a read failure falls back
    # to the previous snapshot and degrades that cycle — never unscoped work.
    subs_snapshot = cfg.subscriptions
    try:
        while not shutdown:
            logger.info("Starting cycle...")
            config_failure_reason = None
            try:
                subs_snapshot = load_config().subscriptions
            except Exception as exc:
                config_failure_reason = (
                    "Subscription config reload failed; previous snapshot in use"
                )
                logger.warning(
                    "%s: %s", config_failure_reason, sanitize_external_error(exc)
                )
            idx = MagazineIndex()
            try:
                asyncio.run(
                    _run_daemon_cycle(
                        cfg,
                        idx,
                        dry_run=dry_run,
                        logger=logger,
                        subscriptions=subs_snapshot,
                        config_failure_reason=config_failure_reason,
                    )
                )
            finally:
                idx.close()

            if shutdown or dry_run:
                break

            # Sleep with interrupt support. The heartbeat's daemon thread keeps
            # process liveness current regardless of pipeline health.
            logger.info("Sleeping %s until next cycle...", interval_str)
            sleep_end = time.time() + interval_secs
            while time.time() < sleep_end and not shutdown:
                time.sleep(min(5, sleep_end - time.time()))
    finally:
        stop_heartbeat()
        logger.info("magsync daemon stopped.")
