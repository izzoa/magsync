"""Shared source indexing and daemon-cycle orchestration, independent of terminal UI."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable

from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus, IndexOutcome
from magsync.core.organizer import normalize_title, parse_date, strip_accents
from magsync.core.scraper import resolve_masked_links

def _filter_results(results, query: str, exact: bool):
    """Filter scraped results by exact title match if requested."""
    if not exact:
        return results
    query_norm = strip_accents(query).lower()
    return [r for r in results if strip_accents(normalize_title(r.title)).lower() == query_norm]


_SAME = object()


def _index_results(results, idx: MagazineIndex, cfg, subscription=None, provenance=_SAME) -> IndexOutcome:
    """Index scraped results, grouping by normalized title.

    Each unique normalized title gets its own magazine entry.
    Returns total new issues added.

    ``subscription`` is the subscription whose search produced these results,
    when there is one: matching rows record subscription provenance (and
    null-provenance re-encounters are promoted); fuzzy strangers are cataloged
    without provenance and are never claimable work.

    ``provenance`` (defaulting to ``subscription``) is the matcher whose
    title matches record subscription provenance; ``None`` records none. The
    runtime passes a separate matcher so only local intent becomes provenance
    while remote subscriptions still gate link resolution and link-less counts.

    Returns an :class:`IndexOutcome`: new issues added, plus the number stored
    or left without a usable download URL.
    """
    from collections import defaultdict

    # Group results by normalized title
    by_magazine: dict[str, list] = defaultdict(list)
    for r in results:
        norm = normalize_title(r.title) if r.title else "Unknown"
        by_magazine[norm].append(r)

    total_new = 0
    total_linkless = 0
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
        if provenance is _SAME:
            outcome = idx.add_issues(mag_id, issues_data, subscription=subscription)
        else:
            outcome = idx.add_issues(mag_id, issues_data, subscription=subscription, provenance=provenance)
        total_new += outcome.added
        total_linkless += outcome.linkless

    return IndexOutcome(added=total_new, linkless=total_linkless)


async def _resolve_links_for_indexing(
    items, idx: MagazineIndex, source_client, *, subscription=None
):
    """Resolve masked download links only for the issues that still need one.

    Two gates, both about not spending source requests pointlessly:

    * Issues already carrying a usable stored URL, or already parked with a
      pending re-probe, are skipped - so in steady state only genuinely new
      issues are resolved.
    * When the search was driven by a subscription, issues whose title does
      not match it are skipped too. A fuzzy-search stranger is cataloged with
      no provenance and can never be claimed, so resolving its link is wasted
      traffic - the same reason ``backfill-urls`` repairs only wanted rows.
      Nothing is lost permanently: subscribing later promotes the row, and
      ``backfill-urls`` then repairs its URL.
    """
    from magsync.core.matching import title_match

    needed = idx.page_urls_missing_link([item.page_url for item in items])

    def needs_link(issue) -> bool:
        if issue.page_url not in needed:
            return False
        if subscription is not None and not title_match(
            issue.title or "", subscription
        ):
            return False
        return True

    return await resolve_masked_links(items, source_client, needs_link=needs_link)


def _park_link_dispositions(batch, idx: MagazineIndex) -> tuple[int, int]:
    """Park issues whose link resolved but is not usable.

    Returns ``(unsupported_host, dead_link)`` counts. Both are parked with a
    scheduled re-probe rather than retried every cycle or abandoned: the
    source may rehost or rotate the link later. The pending action is also
    what keeps them out of the indexing resolution gate.
    """
    from magsync.core.policy import dead_link_reprobe_at, unsupported_host_reprobe_at

    disposed = [issue for issue, _host in batch.unsupported_host] + list(
        batch.dead_link
    )
    if not disposed:
        return (0, 0)

    ids = idx.issue_ids_for_page_urls([issue.page_url for issue in disposed])
    unsupported = 0
    for issue, host in batch.unsupported_host:
        issue_id = ids.get(issue.page_url)
        if issue_id is None:
            continue
        if idx.park_link_outcome(
            issue_id,
            DownloadStatus.UNSUPPORTED,
            unsupported_host_reprobe_at(),
            error=f"download link resolved to unsupported host {host}",
        ):
            unsupported += 1

    dead = 0
    for issue in batch.dead_link:
        issue_id = ids.get(issue.page_url)
        if issue_id is None:
            continue
        if idx.park_link_outcome(
            issue_id,
            DownloadStatus.UNAVAILABLE,
            dead_link_reprobe_at(),
            error="source reported no available download link",
        ):
            dead += 1

    return (unsupported, dead)


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
            "(%d attempted, %d empty, %d failed, %d skipped, %d detail failures, "
            "%d link failures, %d link-less, %d unsupported host, %d dead link); "
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
        report.link_resolution_failures,
        report.issues_linkless,
        report.issues_unsupported_host,
        report.issues_dead_link,
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
    limits: Any = None,
) -> Any:
    """Run exactly one daemon discovery cycle through a temporary runtime.

    Production daemons and services run :meth:`Runtime.discover` from their
    work loop; this wrapper drives that same code for one cycle so the
    established cycle tests (and diagnostics) exercise the production path.
    ``subscriptions`` is the cycle's local subscription snapshot (defaults to
    ``cfg.subscriptions``).

    ``dry_run`` previews the cached work the next cycle would claim, computed
    on a private copy of the store: no source requests and no mutation.
    """
    from magsync.companion.cli import service_limits
    from magsync.companion.runtime import Runtime, preview_claimable
    from magsync.core.models import CycleReport
    from magsync.core.scraper import FreemagazinesClient

    daemon_logger = logger or logging.getLogger("magsync")
    snapshot = list(cfg.subscriptions if subscriptions is None else subscriptions)
    if dry_run:
        issues, due_refreshes = preview_claimable(idx.db_path, snapshot, now=now)
        report = CycleReport(downloads_queued=len(issues))
        if issues:
            daemon_logger.info("Dry run - would download %d issues", len(issues))
        if due_refreshes:
            daemon_logger.info("Dry run - %d due link refresh(es) would be attempted", due_refreshes)
        return report

    runtime = Runtime(
        idx,
        cfg,
        limits=limits or service_limits(),
        exports=idx.db_path.parent / "exports",
        source_factory=source_client_factory or FreemagazinesClient,
        require_initialized=False,
        accept_commands=False,
        notify=True,
        logger=daemon_logger,
        clock=clock,
        utcnow=(lambda: now) if now is not None else None,
        subscription_loader=lambda: snapshot,
    )
    await runtime.start(background=False)
    try:
        return await runtime.discover(
            now=now, subscriptions=snapshot, config_failure_reason=config_failure_reason
        )
    finally:
        await runtime.stop()
