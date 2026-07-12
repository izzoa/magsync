"""Concurrent, typed batch download orchestration."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from magsync.config import Config
from magsync.core.diagnostics import sanitize_external_error
from magsync.core.downloader import RateLimitGate, download_and_decrypt
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadFailureKind,
    DownloadResult,
    DownloadStatus,
    RefreshOutcome,
    RefreshOutcomeKind,
    RetryAction,
    SourceError,
    SourceFailure,
    SourceFailureKind,
)
from magsync.core.organizer import organize_path
from magsync.core.policy import get_download_failure_policy
from magsync.core.scraper import FreemagazinesClient, scrape_detail_page
from magsync.core.urls import URLValidationError, normalize_limewire_share_url

logger = logging.getLogger("magsync")

# Immediate retrying is owned by the downloader. Once that budget is exhausted,
# retain work for a later daemon cycle without making the very next cycle hammer
# the same external service again.
_SCHEDULE_DELAY = timedelta(minutes=5)


def _next_retry_at() -> datetime:
    return datetime.now(timezone.utc) + _SCHEDULE_DELAY


def _safe_title(issue: dict) -> str:
    title = str(issue.get("title") or "Untitled issue").replace("\r", " ").replace(
        "\n", " "
    )
    return sanitize_external_error(title)[:80]


def _source_failure(
    kind: SourceFailureKind,
    message: str,
    *,
    operation: str = "detail",
) -> SourceFailure:
    return SourceFailure(kind=kind, message=message, operation=operation)


async def _refresh_link_from_page(
    issue: dict,
    attempted_url: str,
    source_client: FreemagazinesClient,
) -> RefreshOutcome:
    """Return the structured result of one page-specific link refresh.

    This helper performs no index mutation. The caller applies the outcome
    atomically through :class:`MagazineIndex`, which keeps source-only due work
    separate from known-dead LimeWire download work.
    """
    title = _safe_title(issue)
    page_url = issue.get("page_url")
    if not page_url:
        failure = _source_failure(
            SourceFailureKind.PROTOCOL,
            "Issue has no source page URL for link refresh",
        )
        logger.error("Link refresh failed for %s: missing source page", title)
        return RefreshOutcome(RefreshOutcomeKind.SCRAPE_ERROR, failure=failure)

    try:
        detail = await scrape_detail_page(page_url, client=source_client)
    except asyncio.CancelledError:
        raise
    except SourceError as exc:
        if exc.kind is SourceFailureKind.ACCESS_BLOCKED:
            logger.info("Link refresh blocked by source challenge for %s", title)
            return RefreshOutcome(
                RefreshOutcomeKind.SOURCE_BLOCKED,
                failure=exc.failure,
            )
        logger.log(
            logging.WARNING
            if exc.kind is SourceFailureKind.TRANSIENT
            else logging.ERROR,
            "Link refresh scrape failed for %s: %s",
            title,
            sanitize_external_error(exc.failure.message),
        )
        return RefreshOutcome(
            RefreshOutcomeKind.SCRAPE_ERROR,
            failure=exc.failure,
        )
    except Exception:
        # Parser and adapter exceptions are deliberately converted without
        # reflecting raw exception text, which may contain a URL or response.
        failure = _source_failure(
            SourceFailureKind.PROTOCOL,
            "Unable to parse source detail page during link refresh",
        )
        logger.error("Unexpected link-refresh failure for %s", title)
        return RefreshOutcome(RefreshOutcomeKind.SCRAPE_ERROR, failure=failure)

    if not detail.limewire_url:
        logger.info("Link refresh found no download link for %s", title)
        return RefreshOutcome(RefreshOutcomeKind.NO_LINK)

    try:
        old_identity = normalize_limewire_share_url(attempted_url)
        new_identity = normalize_limewire_share_url(detail.limewire_url)
    except (TypeError, URLValidationError):
        failure = _source_failure(
            SourceFailureKind.PROTOCOL,
            "Source detail page contained an invalid download link",
        )
        logger.error("Link refresh found an invalid download link for %s", title)
        return RefreshOutcome(RefreshOutcomeKind.SCRAPE_ERROR, failure=failure)

    if new_identity == old_identity:
        logger.info("Link refresh found the unchanged share for %s", title)
        return RefreshOutcome(RefreshOutcomeKind.UNCHANGED)

    # Never include either full URL in the log: the fragment is key material.
    logger.info("Link refresh found a rotated share for %s", title)
    return RefreshOutcome(RefreshOutcomeKind.ROTATED, url=new_identity)


def _refresh_needs_reschedule(outcome: RefreshOutcome) -> bool:
    if outcome.kind is RefreshOutcomeKind.SOURCE_BLOCKED:
        return True
    return (
        outcome.kind is RefreshOutcomeKind.SCRAPE_ERROR
        and outcome.failure is not None
        and outcome.failure.kind is SourceFailureKind.TRANSIENT
    )


def _apply_refresh_outcome(
    issue: dict,
    idx: MagazineIndex,
    outcome: RefreshOutcome,
) -> bool:
    """Persist one structured refresh result.

    Protocol/parser errors stay parked without an automatic action. Only a
    source challenge or typed transient source failure receives a future
    ``REFRESH_LINK`` action.
    """
    issue_id = issue.get("id")
    if issue_id is None:
        raise ValueError("issue has no id")
    if _refresh_needs_reschedule(outcome):
        return idx.resolve_link_refresh(
            issue_id,
            outcome,
            retry_at=_next_retry_at(),
        )
    if outcome.kind is RefreshOutcomeKind.SCRAPE_ERROR:
        return idx.clear_link_refresh(issue_id)
    return idx.resolve_link_refresh(issue_id, outcome)


async def refresh_due_links(
    issues: list[dict],
    idx: MagazineIndex,
    source_client: FreemagazinesClient,
) -> list[dict]:
    """Resolve claimed source-only refresh work without touching dead shares.

    Each returned mapping contains ``issue``, ``outcome``, ``success``, and an
    authoritative ``failure_kind``. Ordinary per-row failures are isolated;
    task cancellation continues to propagate.
    """

    async def refresh_one(issue: dict) -> dict:
        attempted_url = str(issue.get("limewire_url") or "")
        try:
            outcome = await _refresh_link_from_page(
                issue,
                attempted_url,
                source_client,
            )
            _apply_refresh_outcome(issue, idx, outcome)
            return {
                "issue": issue,
                "outcome": outcome,
                "success": True,
                "error": outcome.failure.message if outcome.failure else None,
                "failure_kind": None,
            }
        except asyncio.CancelledError:
            raise
        except Exception:
            error = "Unable to resolve source-only link refresh"
            logger.error("Source-only link refresh failed for %s", _safe_title(issue))
            return {
                "issue": issue,
                "outcome": RefreshOutcome(
                    RefreshOutcomeKind.SCRAPE_ERROR,
                    failure=_source_failure(
                        SourceFailureKind.PROTOCOL,
                        "Unable to resolve source-only link refresh",
                    ),
                ),
                "success": False,
                "error": error,
                "failure_kind": DownloadFailureKind.INTERNAL,
            }

    workers = [asyncio.create_task(refresh_one(issue)) for issue in issues]
    try:
        return await asyncio.gather(*workers)
    except BaseException:
        for worker in workers:
            if not worker.done():
                worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise


class _SingleFlightRegistry:
    """Share one physical download result per normalized full URL."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task[DownloadResult]] = {}
        self._lock = asyncio.Lock()

    async def run(
        self,
        identity: str,
        factory: Callable[[], Any],
    ) -> DownloadResult:
        async with self._lock:
            task = self._tasks.get(identity)
            if task is None:
                task = asyncio.create_task(factory())
                self._tasks[identity] = task
        # One cancelled alias must not cancel the shared leader out from under
        # its siblings. ``download_batch`` explicitly cancels these tasks when
        # the whole operation is cancelled.
        return await asyncio.shield(task)

    async def cancel(self) -> None:
        async with self._lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def _call_maybe_async(callback: Callable[..., Any], *args: Any) -> None:
    returned = callback(*args)
    if inspect.isawaitable(returned):
        await returned


def _callback_accepts_failure_kind(callback: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return False
    try:
        signature.bind({}, False, None, DownloadFailureKind.INTERNAL)
    except TypeError:
        return False
    return True


async def _emit_complete(
    callback: Callable[..., Any] | None,
    issue: dict,
    outcome: dict,
) -> None:
    if callback is None:
        return
    args: tuple[Any, ...] = (
        issue,
        outcome["success"],
        outcome.get("error"),
    )
    if _callback_accepts_failure_kind(callback):
        args += (outcome.get("failure_kind"),)
    try:
        await _call_maybe_async(callback, *args)
    except asyncio.CancelledError:
        raise
    except Exception:
        # Presentation callbacks cannot be allowed to cancel siblings or undo
        # an already-committed terminal database result.
        logger.error("Completion callback failed for %s", _safe_title(issue))


def _outcome_dict(
    issue: dict,
    result: DownloadResult,
    *,
    refresh_outcome: RefreshOutcome | None = None,
) -> dict:
    outcome = {
        "issue": issue,
        "success": result.success,
        "error": result.error,
        "failure_kind": result.failure_kind,
        "attempt_count": result.attempt_count,
    }
    if result.success:
        outcome["path"] = result.file_path
    if result.failure_kind is DownloadFailureKind.UNSUPPORTED:
        outcome["unsupported"] = True
    if refresh_outcome is not None:
        outcome["refresh_outcome"] = refresh_outcome
    return outcome


def _internal_result(error: str, *, attempts: int = 0) -> DownloadResult:
    return DownloadResult(
        success=False,
        failure_kind=DownloadFailureKind.INTERNAL,
        error=sanitize_external_error(error),
        attempt_count=attempts,
    )


def _typed_result(result: Any) -> DownloadResult:
    if not isinstance(result, DownloadResult):
        return _internal_result("Downloader returned an invalid result", attempts=1)
    if not result.success and result.failure_kind is None:
        result.failure_kind = DownloadFailureKind.INTERNAL
    if not result.success and result.error is not None:
        result.error = sanitize_external_error(result.error)
    return result


async def _perform_url_download(
    url: str,
    dest: Path,
    cfg: Config,
    semaphore: asyncio.Semaphore,
    rate_gate: RateLimitGate,
    dest_locks: dict[str, asyncio.Lock],
    *,
    retry_attempts: int,
) -> DownloadResult:
    """Perform the physical operation for one single-flight leader."""
    dest_lock = dest_locks[str(dest.absolute())]
    async with dest_lock:
        if dest.exists():
            logger.info(
                "Already on disk, skipping: %s",
                sanitize_external_error(dest.name),
            )
            return DownloadResult(
                success=True,
                file_path=dest,
                file_size_bytes=dest.stat().st_size,
            )

        async with semaphore:
            await rate_gate.wait()
            # Preserve the existing polite staggering between distinct
            # LimeWire operations. Exact aliases never reach this point twice.
            await asyncio.sleep(1)
            try:
                result = await download_and_decrypt(
                    url,
                    dest,
                    constants=cfg.limewire,
                    rate_gate=rate_gate,
                    retry_attempts=retry_attempts,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                return _internal_result(
                    "Unexpected downloader failure",
                    attempts=1,
                )

    result = _typed_result(result)
    if result.attempt_count <= 0:
        result.attempt_count = 1
    if result.success:
        if result.file_path is None or not Path(result.file_path).is_file():
            return _internal_result(
                "Downloader reported success without an existing file",
                attempts=result.attempt_count,
            )
        result.file_path = Path(result.file_path)
    return result


async def _persist_and_emit(
    issue: dict,
    idx: MagazineIndex,
    result: DownloadResult,
    on_complete: Callable[..., Any] | None,
    *,
    refresh_outcome: RefreshOutcome | None = None,
) -> dict:
    issue_id = issue.get("id")
    if issue_id is None:
        raise ValueError("issue has no id")

    next_retry_at = None
    next_action = None
    if not result.success and result.failure_kind is not None:
        policy = get_download_failure_policy(result.failure_kind)
        if policy.automatic_retry:
            next_retry_at = _next_retry_at()
            next_action = RetryAction.DOWNLOAD

    idx.record_download_result(
        issue_id,
        result,
        physical_attempts=result.attempt_count,
        next_retry_at=next_retry_at,
        next_action=next_action,
    )
    outcome = _outcome_dict(issue, result, refresh_outcome=refresh_outcome)
    await _emit_complete(on_complete, issue, outcome)
    return outcome


async def _download_one_impl(
    issue: dict,
    cfg: Config,
    idx: MagazineIndex,
    semaphore: asyncio.Semaphore,
    rate_gate: RateLimitGate,
    on_start: Callable[[dict], Any] | None,
    on_complete: Callable[..., Any] | None,
    dest_locks: dict[str, asyncio.Lock],
    source_client: FreemagazinesClient,
    singleflight: _SingleFlightRegistry,
) -> dict:
    raw_url = issue.get("limewire_url")
    if not raw_url:
        return await _persist_and_emit(
            issue,
            idx,
            DownloadResult(
                success=False,
                failure_kind=DownloadFailureKind.CONFIGURATION,
                error="No download link",
            ),
            on_complete,
        )

    try:
        identity = normalize_limewire_share_url(raw_url)
    except (TypeError, URLValidationError):
        return await _persist_and_emit(
            issue,
            idx,
            DownloadResult(
                success=False,
                failure_kind=DownloadFailureKind.METADATA_INVALID,
                error="Stored download link is invalid",
            ),
            on_complete,
        )

    dest = organize_path(issue["title"], issue["page_url"], cfg.output_dir)

    # Per-issue lifecycle callbacks and state remain independent even when the
    # external operation below is shared with exact-URL aliases.
    if not dest.exists() and on_start is not None:
        await _call_maybe_async(on_start, issue)
    idx.update_download_status(issue["id"], DownloadStatus.DOWNLOADING)

    result = await singleflight.run(
        identity,
        lambda: _perform_url_download(
            identity,
            dest,
            cfg,
            semaphore,
            rate_gate,
            dest_locks,
            retry_attempts=cfg.download.retry_attempts,
        ),
    )
    result = _typed_result(result)

    if result.success:
        return await _persist_and_emit(issue, idx, result, on_complete)

    policy = get_download_failure_policy(
        result.failure_kind or DownloadFailureKind.INTERNAL
    )
    if not policy.refresh_link:
        return await _persist_and_emit(issue, idx, result, on_complete)

    # Unavailable aliases share the dead-link operation but refresh their own
    # source pages exactly once. The refresh helper has no persistence side
    # effects, allowing the rotated path to be classified from scratch.
    refresh_outcome = await _refresh_link_from_page(
        issue,
        identity,
        source_client,
    )
    if refresh_outcome.kind is RefreshOutcomeKind.ROTATED:
        assert refresh_outcome.url is not None
        _apply_refresh_outcome(issue, idx, refresh_outcome)
        fresh_identity = refresh_outcome.url
        fresh_result = await singleflight.run(
            fresh_identity,
            lambda: _perform_url_download(
                fresh_identity,
                dest,
                cfg,
                semaphore,
                rate_gate,
                dest_locks,
                retry_attempts=0,
            ),
        )
        return await _persist_and_emit(
            issue,
            idx,
            _typed_result(fresh_result),
            on_complete,
            refresh_outcome=refresh_outcome,
        )

    # ``schedule_link_refresh`` requires the row to be parked unavailable, so
    # first persist the authoritative dead-share result, then resolve the
    # page-specific source outcome, and only then emit the terminal callback.
    idx.record_download_result(
        issue["id"],
        result,
        physical_attempts=result.attempt_count,
    )
    _apply_refresh_outcome(issue, idx, refresh_outcome)
    outcome = _outcome_dict(issue, result, refresh_outcome=refresh_outcome)
    await _emit_complete(on_complete, issue, outcome)
    return outcome


async def _download_one(
    issue: dict,
    cfg: Config,
    idx: MagazineIndex,
    semaphore: asyncio.Semaphore,
    rate_gate: RateLimitGate,
    on_start: Callable[[dict], Any] | None = None,
    on_complete: Callable[..., Any] | None = None,
    dest_locks: dict[str, asyncio.Lock] | None = None,
    *,
    source_client: FreemagazinesClient | None = None,
    singleflight: _SingleFlightRegistry | None = None,
) -> dict:
    """Download one issue while isolating ordinary worker failures."""
    locks = dest_locks if dest_locks is not None else defaultdict(asyncio.Lock)
    owns_flights = singleflight is None
    flights = singleflight or _SingleFlightRegistry()

    async def run(client: FreemagazinesClient) -> dict:
        try:
            return await _download_one_impl(
                issue,
                cfg,
                idx,
                semaphore,
                rate_gate,
                on_start,
                on_complete,
                locks,
                client,
                flights,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error("Issue worker failed for %s", _safe_title(issue))
            result = _internal_result("Unexpected issue worker failure")
            issue_id = issue.get("id")
            if issue_id is not None:
                try:
                    idx.record_download_result(
                        issue_id,
                        result,
                        physical_attempts=0,
                    )
                except Exception:
                    logger.error(
                        "Unable to persist isolated worker failure for %s",
                        _safe_title(issue),
                    )
            outcome = _outcome_dict(issue, result)
            await _emit_complete(on_complete, issue, outcome)
            return outcome

    try:
        if source_client is not None:
            return await run(source_client)
        async with FreemagazinesClient(
            scrape_delay=cfg.download.scrape_delay,
        ) as owned_source:
            return await run(owned_source)
    except BaseException:
        if owns_flights:
            await flights.cancel()
        raise


async def _batch_failure_results(
    issues: list[dict],
    idx: MagazineIndex,
    on_complete: Callable[..., Any] | None,
    result: DownloadResult,
) -> list[dict]:
    outcomes: list[dict] = []
    for issue in issues:
        per_issue = DownloadResult(
            success=False,
            failure_kind=result.failure_kind,
            error=result.error,
            attempt_count=result.attempt_count,
        )
        try:
            outcome = await _persist_and_emit(issue, idx, per_issue, on_complete)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error("Unable to persist batch failure for %s", _safe_title(issue))
            outcome = _outcome_dict(issue, per_issue)
            await _emit_complete(on_complete, issue, outcome)
        outcomes.append(outcome)
    return outcomes


async def download_batch(
    issues: list[dict],
    cfg: Config,
    idx: MagazineIndex,
    on_start: Callable[[dict], Any] | None = None,
    on_complete: Callable[..., Any] | None = None,
    *,
    source_client: FreemagazinesClient | None = None,
) -> list[dict]:
    """Download issues with typed isolation and exact-URL single-flight.

    A caller may provide its cycle-scoped ``FreemagazinesClient`` so indexing,
    detail work, and dead-link refreshes share one cookie jar, pacing gate, and
    circuit. Otherwise this operation owns one client for the whole batch.
    """
    if not issues:
        return []

    # Overlapping subscriptions may enqueue one database row more than once.
    # Distinct ids are retained even when they share an exact full URL.
    seen_ids: set[Any] = set()
    deduped: list[dict] = []
    for issue in issues:
        issue_id = issue.get("id")
        if issue_id is not None:
            if issue_id in seen_ids:
                continue
            seen_ids.add(issue_id)
        deduped.append(issue)
    issues = deduped

    if not cfg.limewire.file_iv_b64 or not cfg.limewire.sharing_salt_b64:
        from magsync.config import save_config
        from magsync.core.downloader import auto_extract_constants

        logger.info("No encryption constants - extracting before batch download...")
        try:
            extracted = await auto_extract_constants()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error("Encryption constant extraction failed")
            extracted = None
        if extracted:
            cfg.limewire.sharing_salt_b64 = extracted.sharing_salt_b64
            cfg.limewire.sharing_iv_b64 = extracted.sharing_iv_b64
            cfg.limewire.file_iv_b64 = extracted.file_iv_b64
            cfg.limewire.file_name_iv_b64 = extracted.file_name_iv_b64
            cfg.limewire.file_sha1_iv_b64 = extracted.file_sha1_iv_b64
            cfg.limewire.preview_iv_b64 = extracted.preview_iv_b64
            cfg.limewire.pbkdf2_iterations = extracted.pbkdf2_iterations
            try:
                save_config(cfg)
                logger.info("Encryption constants saved to config")
            except OSError:
                logger.info("Config is read-only - constants in memory only")
        else:
            logger.error("Auto-extraction failed - aborting batch. See UPDATE_KEYS.md.")
            return await _batch_failure_results(
                issues,
                idx,
                on_complete,
                DownloadResult(
                    success=False,
                    failure_kind=DownloadFailureKind.CONFIGURATION,
                    error="Encryption constants unavailable",
                ),
            )

    semaphore = asyncio.Semaphore(cfg.download.max_concurrent)
    rate_gate = RateLimitGate()
    dest_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
    singleflight = _SingleFlightRegistry()

    async def run(client: FreemagazinesClient) -> list[dict]:
        workers = [
            asyncio.create_task(
                _download_one(
                    issue,
                    cfg,
                    idx,
                    semaphore,
                    rate_gate,
                    on_start,
                    on_complete,
                    dest_locks,
                    source_client=client,
                    singleflight=singleflight,
                )
            )
            for issue in issues
        ]
        try:
            return await asyncio.gather(*workers)
        except BaseException:
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            await singleflight.cancel()
            raise

    if source_client is not None:
        return await run(source_client)
    async with FreemagazinesClient(
        scrape_delay=cfg.download.scrape_delay,
    ) as owned_source:
        return await run(owned_source)
