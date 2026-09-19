"""CLI interface for magsync using Typer."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import sys

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from magsync.config import (
    ConfigurationConflict,
    add_subscription,
    get_db_path,
    load_config,
    remove_subscription,
    set_config_value,
)
from magsync.core.diagnostics import sanitize_external_error
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadStatus,
    LinkResolutionKind,
    SourceError,
    SourceFailure,
    SourceFailureKind,
)
from magsync.core.organizer import strip_accents
from magsync.core.scraper import (
    FreemagazinesClient,
    scrape_detail_page,
    search_with_details_result,
)
from magsync.output import BatchOutput, resolve_mode
from magsync.companion.local import coordinated, read_only_snapshot
from magsync.companion.protocol import ProtocolError

app = typer.Typer(
    name="magsync",
    help="Index and download magazines from freemagazines.top",
    no_args_is_help=False,
    invoke_without_command=True,
)
console = Console()

# The operator surface stays importable without FastAPI/Uvicorn installed.
from magsync.companion.cli import companion, clients, serve
app.add_typer(companion, name="companion")
app.add_typer(clients, name="clients")
app.command()(serve)


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


from magsync.core.orchestration import _index_results
# One daemon cycle through the production runtime (tests and diagnostics).
from magsync.core.orchestration import _run_daemon_cycle  # noqa: F401


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


def _parse_since_option(since: str | None) -> tuple[int | None, int | None]:
    """Parse ``--since YYYY[-MM]``; reject anything else before doing work."""
    if not since:
        return None, None
    parts = since.split("-")
    try:
        year = int(parts[0])
        month = int(parts[1]) if len(parts) > 1 and parts[1] else None
    except ValueError:
        raise typer.BadParameter("--since must look like YYYY-MM (for example 2025-06)") from None
    if month is not None and not 1 <= month <= 12:
        raise typer.BadParameter("--since month must be between 01 and 12")
    return year, month


def _print_search_table(query: str, result_count: int, new_count: int, idx: MagazineIndex) -> None:
    """The search result table, shared by standalone and daemon-executed searches."""
    norm = strip_accents(query).lower()
    table = Table(title=f"Results for '{query}' ({result_count} issues, {new_count} new)")
    table.add_column("#", style="dim", width=4)
    table.add_column("Title", style="cyan", max_width=60)
    table.add_column("Year", width=6)
    table.add_column("Month", width=6)
    table.add_column("Size", width=8)
    table.add_column("Status", width=10)

    for i, issue in enumerate(idx.get_issues(magazine_title=norm), 1):
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
            escape(issue["title"][:60]),
            str(issue.get("year") or "?"),
            str(issue.get("month") or "?"),
            escape(issue.get("file_size") or "?"),
            status_style,
        )

    console.print(table)


def _print_dry_run_table(issues: list[dict], title: str) -> None:
    """Cached issues a dry run would download, with an estimated total size."""
    table = Table(title=title)
    table.add_column("#", style="dim", width=4)
    table.add_column("Title", style="cyan", max_width=55)
    table.add_column("Year", width=6)
    table.add_column("Month", width=6)
    table.add_column("Size", width=8)
    total_size = 0
    for i, issue in enumerate(issues, 1):
        table.add_row(
            str(i),
            escape((issue.get("title") or "")[:55]),
            str(issue.get("year") or "?"),
            str(issue.get("month") or "?"),
            escape(issue.get("file_size") or "?"),
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


_DRY_RUN_NOTE = "Dry run — cached catalog only; no source requests and no files downloaded."

_OPERATION_ERRORS = {
    "internal_error": "The magsync daemon could not complete this command; see its log for details.",
    "runtime_unavailable": "The magsync daemon stopped before finishing this command.",
    "interrupted": "The command was interrupted before it finished.",
    "abandoned": "The command was abandoned before it started.",
    "withdrawn": "The command was cancelled before it started.",
    "capacity_exhausted": "Not enough free disk space to download right now; free some space and try again.",
    "configuration_managed": "Configuration is managed outside magsync; the change was not saved.",
    "invalid_request": "The command was not valid.",
    "scope_disabled": "The local library is disabled.",
}

_OUTCOME_LABELS = {
    "complete": ("✓", "downloaded", "green"),
    "unavailable": ("○", "unavailable", "yellow"),
    "unsupported": ("⊘", "unsupported", "magenta"),
    "failed": ("✗", "failed", "red"),
    "pending": ("·", "not downloaded", "dim"),
    "downloading": ("…", "downloading", "yellow"),
}


def _render_operation_error(operation: dict) -> int | None:
    """Print a sentence for an operation that failed as a whole; return its exit code."""
    result = operation.get("result") or {}
    if operation["state"] != "failed" or result.get("failure"):
        return None
    code = result.get("code")
    message = result.get("message") or _OPERATION_ERRORS.get(code) or f"The command failed ({code or 'unknown'})."
    console.print(message, style="red", markup=False, highlight=False)
    return 1


def _render_source_failure(result: dict) -> None:
    failure = result.get("failure") or {}
    try:
        _print_source_failure(SourceFailure(
            SourceFailureKind(failure["kind"]), failure.get("message") or "",
            status_code=failure.get("status_code"), host=failure.get("host"), cf_ray=failure.get("cf_ray"),
        ))
    except (KeyError, ValueError, TypeError):
        console.print("Source search failed; retry later.", style="red", markup=False, highlight=False)


def _print_outcomes(store, outcomes: list[dict]) -> dict[str, int]:
    """One line per issue (title, marker, typed outcome); returns counts by label."""
    internal = {}
    for outcome in outcomes:
        try:
            internal[outcome["issue_id"]] = store.internal_issue(outcome["issue_id"])
        except ProtocolError:
            continue
    rows = {row["id"]: row for row in store.index.get_issues_by_ids(list(internal.values()))}
    counts: dict[str, int] = {}
    for outcome in outcomes:
        row = rows.get(internal.get(outcome["issue_id"])) or {}
        marker, label, style = _OUTCOME_LABELS.get(outcome.get("status") or "failed", _OUTCOME_LABELS["failed"])
        counts[label] = counts.get(label, 0) + 1
        kind = outcome.get("failure_kind")
        detail = f" ({kind})" if kind and label != "downloaded" else ""
        title = sanitize_external_error((row.get("title") or "Unknown issue")[:60])
        console.print(f"  {marker} {title}: {label}{detail}", style=style, markup=False, highlight=False)
    return counts


def _render_search(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    result = operation["result"]
    if result.get("outcome") in ("blocked", "failed"):
        _render_source_failure(result)
        return 1
    if result.get("outcome") == "empty":
        console.print(f"[yellow]No results found for '{escape(body['query'])}'[/yellow]")
        return 0
    detail_failures = result.get("detail_failures") or 0
    _print_partial_details(detail_failures)
    _print_search_table(body["query"], len(result.get("items") or []), result.get("added") or 0, store.index)
    return 1 if detail_failures else 0


def _render_fetch(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    result = operation["result"]
    if result.get("outcome") in ("blocked", "failed"):
        _render_source_failure(result)
        return 1
    if result.get("outcome") == "empty":
        console.print(f"[yellow]No results found for '{escape(body['query'])}'[/yellow]")
        return 0
    detail_failures = result.get("detail_failures") or 0
    _print_partial_details(detail_failures)
    recoverable = result.get("recoverable") or 0
    if recoverable:
        console.print(
            f"[yellow]{recoverable} previously failed/unavailable "
            f"issue{'s' if recoverable != 1 else ''} marked as requested — run "
            "'magsync retry' to attempt them.[/yellow]"
        )
    pending = result.get("pending") or 0
    if not pending:
        console.print("[green]All matching issues already downloaded![/green]")
        return 1 if detail_failures else 0
    counts = _print_outcomes(store, result.get("outcomes") or [])
    summary = ", ".join(f"{count} {label}" for label, count in counts.items()) or "nothing processed"
    console.print(f"\nFetched {pending} issue{'s' if pending != 1 else ''}: {summary}.", markup=False, highlight=False)
    if result.get("code") == "capacity_exhausted":
        console.print(_OPERATION_ERRORS["capacity_exhausted"], style="yellow", markup=False)
        return 1
    return 1 if detail_failures else 0


def _render_update(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    result = operation["result"]
    if not result.get("tracked"):
        console.print("[yellow]No tracked magazines. Run 'magsync search' first.[/yellow]")
        return 0
    for line in result.get("magazines") or []:
        title = line["title"]
        if line["outcome"] in ("blocked", "failed"):
            console.print(f"Update for '{title}' failed:", style="red", markup=False)
            _render_source_failure(line)
        elif line.get("detail_failures"):
            console.print(f"  {title}: {line.get('added') or 0} new issues; {line['detail_failures']} detail page(s) omitted",
                          style="yellow", markup=False)
        elif line.get("added"):
            console.print(f"  [cyan]{escape(title)}[/cyan]: {line['added']} new issues")
        else:
            console.print(f"  [dim]{escape(title)}: up to date[/dim]")
    if result.get("incomplete") or result.get("skipped"):
        console.print(
            f"\nUpdate incomplete: {result.get('new') or 0} new issues; {result.get('incomplete') or 0} source "
            f"operation(s) incomplete; {result.get('skipped') or 0} skipped after source blocking.",
            style="yellow", markup=False,
        )
        return 1
    console.print(f"\n[green]Update complete.[/green] {result.get('new') or 0} new issues found.")
    return 0


def _render_backfill(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    result = operation["result"]
    parked = result.get("parked_skipped") or 0
    if parked:
        console.print(f"[dim]{parked} never-requested issue{'s' if parked != 1 else ''} skipped (use --all to include them).[/dim]")
    if not result.get("total"):
        console.print("[green]No issues missing a download URL.[/green]")
        return 0
    repaired, missing = result.get("repaired") or 0, result.get("missing") or 0
    failed, skipped = result.get("failed") or 0, result.get("skipped") or 0
    if failed or skipped or result.get("outcome") == "blocked":
        console.print(f"\nBackfill incomplete. {repaired} repaired, {missing} checked with no URL, "
                      f"{skipped} skipped, {failed} failed.", style="yellow", markup=False)
        return 1
    console.print(f"\n[green]Backfill complete.[/green] {repaired} repaired, {missing} still missing a URL.")
    return 0


def _render_config(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    console.print(f"Set {body['key']} = {body['value']}", style="green", markup=False, highlight=False)
    return 0


def _subscribed_message(query: str, since: str | None, exact: bool) -> str:
    since_str = f" since {since}" if since else ""
    exact_str = " (exact match)" if exact else ""
    return f"Subscribed to '{query}'{since_str}{exact_str}"


def _render_subscribe(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    if operation["result"].get("outcome") == "unchanged":
        console.print(f"Already subscribed to '{body['query']}'", style="yellow", markup=False, highlight=False)
        return 0
    console.print(_subscribed_message(body["query"], body.get("since"), bool(body.get("exact"))),
                  style="green", markup=False, highlight=False)
    return 0


def _render_unsubscribe(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    if operation["result"].get("outcome") == "unchanged":
        console.print(f"No subscription found for '{body['query']}'", style="yellow", markup=False, highlight=False)
        return 0
    console.print(f"Unsubscribed from '{body['query']}'", style="green", markup=False, highlight=False)
    return 0


def _render_retry(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    result = operation["result"]
    outcomes = result.get("outcomes") or []
    skipped, excluded = result.get("skipped") or 0, result.get("excluded") or 0
    if not outcomes and not skipped and not excluded:
        console.print("[green]No failed downloads to retry.[/green]")
        return 0
    console.print(f"Retried {result.get('physical_attempts', 0)} shared download(s).", markup=False, highlight=False)
    _print_outcomes(store, outcomes)
    if skipped:
        console.print(f"{skipped} skipped: no download link (run 'magsync backfill-urls' to repair).",
                      style="yellow", markup=False, highlight=False)
    if excluded:
        console.print(f"{excluded} excluded: no current local request (subscribe or fetch first, then retry).",
                      style="yellow", markup=False, highlight=False)
    return 0


def _print_repair(report: dict, *, dry_run: bool) -> None:
    """Repair output, shared by standalone and daemon-executed repair-titles."""
    if not report["candidates"]:
        console.print("[green]No titles need repair.[/green]")
    else:
        suffix = " [dim](dry run — nothing will be written)[/dim]" if dry_run else ""
        console.print(f"Repairing {report['candidates']} issue(s){suffix}")
    for kind, label, detail in report["lines"]:
        label = escape(label)
        if kind == "renamed":
            console.print(f"  [dim]·[/dim] {label} → {escape(detail)}")
        elif kind == "correct":
            console.print(f"  [dim]·[/dim] {label} (file already correct)")
        elif kind == "missing":
            console.print(f"  [yellow]?[/yellow] {label}: recorded file is missing, left alone")
        elif kind == "collision":
            console.print(f"  [yellow]![/yellow] {label}: destination already exists, both files left in place")
        elif kind == "moved":
            console.print(f"  [green]→[/green] {label}: moved into {escape(detail)}")
    pruned = report.get("pruned") or []
    console.print(
        f"\n[bold]{report['repaired']} title(s) repaired[/bold], {report['moved']} file(s) moved"
        + (f", {report['collisions']} collision(s)" if report["collisions"] else "")
        + (f", {report['missing']} missing file(s)" if report["missing"] else "")
        + (f", {len(pruned)} empty magazine record(s) pruned" if pruned else "")
        + (f", {report['removed_dirs']} empty folder(s) removed" if report.get("removed_dirs") else "")
    )
    if dry_run:
        console.print("[dim]Dry run: no titles, paths, or files were changed.[/dim]")


def _render_repair(kind: str, operation: dict, body: dict, store) -> int:
    code = _render_operation_error(operation)
    if code is not None:
        return code
    _print_repair(operation["result"], dry_run=False)
    return 0


def _preview_fetch(arguments: dict) -> None:
    """``fetch --dry-run``: cached pending issues, read from a private snapshot."""
    query = arguments["query"]
    since_year, since_month = _parse_since_option(arguments.get("since"))
    with read_only_snapshot() as preview:
        pending = preview.get_issues(
            magazine_title=strip_accents(query).lower(),
            since_year=since_year,
            since_month=since_month,
            status=DownloadStatus.PENDING,
        )
    if not pending:
        console.print(
            f"No cached issues match '{query}'. Run magsync search \"{query}\" "
            "(or fetch without --dry-run) to refresh the catalog first.",
            style="yellow", markup=False, highlight=False,
        )
        return
    _print_dry_run_table(pending, f"Would download {len(pending)} cached issues")
    console.print(f"\n[yellow]{_DRY_RUN_NOTE}[/yellow]")


@app.command()
@coordinated("search", render=_render_search)
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
        new_count = _index_results(results, idx, cfg).added
        _print_search_table(query, len(results), new_count, idx)
    finally:
        idx.close()
    if detail_failures:
        raise typer.Exit(1)


@app.command()
@coordinated("fetch", render=_render_fetch, preview=_preview_fetch)
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

    since_year, since_month = _parse_since_option(since)

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
                    _print_dry_run_table(pending, f"Would download {len(pending)} issues")
                    console.print(f"\n[yellow]{_DRY_RUN_NOTE}[/yellow]")
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
@coordinated("update", render=_render_update)
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
                    new = _index_results(source_result.items, idx, cfg).added
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


def _print_config_error(exc: BaseException) -> None:
    """One sentence for a rejected configuration change; never a traceback."""
    console.print(str(exc), style="red", markup=False, highlight=False)


@app.command()
@coordinated("config", read_only=lambda arguments: not (arguments.get("key") and arguments.get("value")),
             render=_render_config)
def config(
    key: str = typer.Argument(None, help="Config key to view or set (e.g., 'output_dir')"),
    value: str = typer.Argument(None, help="Value to set"),
):
    """View or modify magsync configuration."""
    if key and value:
        try:
            cfg = set_config_value(key, value)
        except (ValueError, ConfigurationConflict) as exc:
            _print_config_error(exc)
            raise typer.Exit(1)
        console.print(f"Set {key} = {value}", style="green", markup=False, highlight=False)
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
@coordinated("subscribe", read_only=lambda arguments: arguments.get("query") is None, render=_render_subscribe)
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

    try:
        added = add_subscription(query, since=since, exact=exact)
    except ConfigurationConflict as exc:
        _print_config_error(exc)
        raise typer.Exit(1)
    if not added:
        console.print(f"Already subscribed to '{query}'", style="yellow", markup=False, highlight=False)
        raise typer.Exit()
    console.print(_subscribed_message(query, since, exact), style="green", markup=False, highlight=False)


@app.command()
@coordinated("unsubscribe", render=_render_unsubscribe)
def unsubscribe(
    query: str = typer.Argument(..., help="Magazine title to unsubscribe from"),
):
    """Remove a magazine subscription."""
    try:
        removed = remove_subscription(query)
    except ConfigurationConflict as exc:
        _print_config_error(exc)
        raise typer.Exit(1)
    if not removed:
        console.print(f"No subscription found for '{query}'", style="yellow", markup=False, highlight=False)
        raise typer.Exit()
    console.print(f"Unsubscribed from '{query}'", style="green", markup=False, highlight=False)


@app.command()
@coordinated("retry", render=_render_retry)
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


@app.command("repair-titles")
@coordinated("repair", render=_render_repair)
def repair_titles(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Report what would change without writing anything"
    ),
):
    """Repair stored titles carrying a legacy source format tag (e.g. "[PDF] ").

    The source used to prepend a format label to every listing. Issues indexed
    back then still carry it, and because indexing deliberately never
    backfills a title, they never healed. That splits the library into
    duplicate directories and — since claim eligibility compares the *stored*
    title — makes an `exact` subscription unable to claim its own back
    catalogue at all.

    This strips the tag, re-derives the fields the title determines,
    re-associates each issue with its correctly-named magazine, moves any
    already-downloaded file to its corrected path, and updates the recorded
    path so content deduplication keeps resolving. Safe to re-run.
    """
    from magsync.core.repair import repair_titles as run_repair

    cfg = load_config()
    idx = MagazineIndex()
    try:
        report = run_repair(idx, cfg.output_dir, dry_run=dry_run)
    finally:
        idx.close()
    _print_repair(report.as_dict(), dry_run=dry_run)


@app.command(name="backfill-urls")
@coordinated("backfill", render=_render_backfill)
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
                        if not detail.limewire_url and detail.download_key:
                            # Explicit repair: always resolve the masked key.
                            resolution = await source_client.resolve_masked_download(
                                row["page_url"], detail.download_key
                            )
                            if resolution.kind is LinkResolutionKind.SUPPORTED:
                                detail.limewire_url = resolution.url
                            elif (
                                resolution.kind
                                is LinkResolutionKind.UNSUPPORTED_HOST
                            ):
                                if out.verbose:
                                    console.print(
                                        f"  [dim]–[/dim] {title}: unsupported "
                                        f"host {resolution.host or 'unknown'}",
                                        markup=True,
                                        highlight=False,
                                    )
                                out.record("missing")
                                continue
                            else:
                                if out.verbose:
                                    console.print(
                                        f"  [dim]–[/dim] {title}: no available "
                                        "download link"
                                    )
                                out.record("missing")
                                continue
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


@app.command()
def daemon(
    interval: str = typer.Option(
        None, "--interval", "-i",
        help="Time between cycles (e.g. 30m, 6h, 1d). Default: 6h",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Preview the cached downloads and due link refreshes the next cycle would claim, then exit (no source requests)",
    ),
):
    """Run magsync as a daemon: scheduled discovery and downloads, no HTTP listener."""
    from magsync.companion.cli import export_root, log_startup_banner, service_limits
    from magsync.companion.runtime import Runtime, preview_claimable

    interval_str = interval or os.environ.get("MAGSYNC_INTERVAL", "6h")
    try:
        interval_secs = _parse_interval(interval_str)
    except ValueError as exc:
        console.print(str(exc), style="red", markup=False, highlight=False)
        raise typer.Exit(2)
    cfg = load_config()
    if dry_run:
        issues, refreshes = preview_claimable(get_db_path(), cfg.subscriptions)
        if issues:
            _print_dry_run_table(issues, f"Next cycle would download {len(issues)} cached issues")
        else:
            console.print("No cached issues are ready to download.")
        console.print(
            f"{refreshes} due link refresh{'es' if refreshes != 1 else ''} would be attempted.",
            markup=False, highlight=False,
        )
        console.print(f"\n[yellow]{_DRY_RUN_NOTE}[/yellow]")
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    _configure_daemon_external_logging()
    logger = logging.getLogger("magsync")
    log_startup_banner(logger, cfg, mode="daemon", interval_label=f"{interval_str} ({interval_secs}s)")

    async def run() -> int:
        idx = MagazineIndex()
        runtime = Runtime(
            idx, cfg, limits=service_limits(), exports=export_root(idx),
            scan_seconds=interval_secs, require_initialized=False, notify=True, logger=logger,
        )
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, runtime.request_stop)
        try:
            try:
                await runtime.start_when_available(background=True)
            except ProtocolError as exc:
                if runtime.stopping:
                    return 0
                if exc.code == "runtime_unavailable":
                    logger.error("Another magsync daemon or service already owns this library; not starting.")
                else:
                    logger.error("%s", exc.message)
                return 1
            await asyncio.gather(runtime.loop_task, return_exceptions=True)
            return 1 if runtime.fatal else 0
        finally:
            if runtime.owner.generation is not None:
                await runtime.stop()
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(signum)
            idx.close()
            logger.info("magsync daemon stopped.")

    try:
        code = asyncio.run(run())
    except ProtocolError as exc:
        console.print(exc.message, style="red", markup=False, highlight=False)
        raise typer.Exit(1) from None
    if code:
        raise typer.Exit(code)
