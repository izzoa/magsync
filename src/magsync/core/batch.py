"""Concurrent batch download manager."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from contextlib import nullcontext
from pathlib import Path
from typing import Callable

from magsync.config import Config
from magsync.core.downloader import download_and_decrypt, RateLimitGate, _is_permanent_error
from magsync.core.index import MagazineIndex, _plausible_limewire_url
from magsync.core.models import DownloadResult, DownloadStatus
from magsync.core.organizer import organize_path
from magsync.core.scraper import scrape_detail_page

logger = logging.getLogger("magsync")


async def _refresh_link_from_page(
    issue: dict,
    idx: MagazineIndex,
    attempted_url: str,
) -> str | None:
    """Re-scrape the issue's page looking for a rotated LimeWire link.

    The site swaps share links on existing posts after takedowns, so a
    permanent dead-link failure may just mean the stored URL is stale. Returns
    the fresh URL (persisted to the index) when the page now carries a
    validated link different from the one just attempted; otherwise None.
    """
    try:
        detail = await scrape_detail_page(issue["page_url"])
    except Exception as e:
        logger.debug(f"Link-refresh scrape failed for {issue.get('page_url')}: {e}")
        return None
    new_url = detail.limewire_url
    if not new_url or new_url == attempted_url or not _plausible_limewire_url(new_url):
        return None
    idx.set_limewire_url(issue["id"], new_url)
    return new_url


async def _download_one(
    issue: dict,
    cfg: Config,
    idx: MagazineIndex,
    semaphore: asyncio.Semaphore,
    rate_gate: RateLimitGate,
    on_start: Callable[[dict], None] | None = None,
    on_complete: Callable[[dict, bool, str | None], None] | None = None,
    dest_locks: dict | None = None,
) -> dict:
    """Download a single issue, respecting the concurrency semaphore and rate limit gate.

    Returns a result dict with issue info and success/error status.
    """
    lw_url = issue.get("limewire_url")
    if not lw_url:
        # No URL to try (e.g. removed upstream, or awaiting backfill). Report
        # it so callers that don't pre-filter (e.g. `magsync fetch`) account
        # for it instead of silently dropping it. Status stays pending.
        if on_complete:
            on_complete(issue, False, "No download link")
        return {"issue": issue, "success": False, "error": "No download link"}

    dest = organize_path(issue["title"], issue["page_url"], cfg.output_dir)

    # Per-destination lock: serialize issues that resolve to the same output file
    # (e.g. hyphen vs en-dash title variants of one issue) so the on-disk dedup
    # below short-circuits the duplicate instead of a concurrent double-fetch.
    # Acquired OUTSIDE the semaphore so a waiting duplicate doesn't hold a slot.
    dest_lock = dest_locks[str(dest)] if dest_locks is not None else nullcontext()

    async with dest_lock:
        async with semaphore:
            # Wait if a 429/throttle pause is active before starting
            await rate_gate.wait()

            # Stagger concurrent requests to avoid hitting LimeWire too fast
            await asyncio.sleep(1)

            # Skip if file already exists on disk (index wiped but files remain,
            # or a same-destination sibling already downloaded it this batch)
            if dest.exists():
                logger.info(f"Already on disk, skipping: {dest.name}")
                idx.update_download_status(
                    issue["id"], DownloadStatus.COMPLETE, str(dest), dest.stat().st_size,
                )
                if on_complete:
                    on_complete(issue, True, None)
                return {"issue": issue, "success": True, "error": None, "path": dest}

            if on_start:
                on_start(issue)

            idx.update_download_status(issue["id"], DownloadStatus.DOWNLOADING)

            async def _attempt(url: str) -> tuple[DownloadResult | None, str | None]:
                """One download attempt → (result, error); error None on success."""
                try:
                    r = await download_and_decrypt(
                        url, dest, constants=cfg.limewire, rate_gate=rate_gate,
                        retry_attempts=cfg.download.retry_attempts,
                    )
                except Exception as e:
                    return None, str(e)
                if r.success:
                    return r, None
                return r, r.error or ""

            result, error = await _attempt(lw_url)

            # A permanent dead link may just be stale: the site rotates share
            # links on existing posts. Re-scrape the page once; a different
            # validated link gets exactly one retry. The terminal status and
            # on_complete reflect only the final outcome.
            if error is not None and _is_permanent_error(error):
                fresh_url = await _refresh_link_from_page(issue, idx, lw_url)
                if fresh_url:
                    logger.info(
                        f"Link rotated on page for {issue['title'][:50]} — retrying with fresh link"
                    )
                    result, error = await _attempt(fresh_url)
                else:
                    # INFO, not WARNING: an unrecovered dead link is an expected
                    # outcome; interactive commands convey it via the summary's
                    # `unavailable` count. The daemon (INFO) still logs it.
                    logger.info(
                        f"Page still shows the dead link for {issue['title'][:50]} — marking unavailable"
                    )

            if error is None and result is not None:
                idx.update_download_status(
                    issue["id"],
                    DownloadStatus.COMPLETE,
                    str(result.file_path),
                    result.file_size_bytes,
                    result.sha256,
                )
                if on_complete:
                    on_complete(issue, True, None)
                return {"issue": issue, "success": True, "error": None, "path": result.file_path}

            status = DownloadStatus.UNAVAILABLE if _is_permanent_error(error) else DownloadStatus.FAILED
            idx.update_download_status(issue["id"], status)
            if on_complete:
                on_complete(issue, False, error)
            return {"issue": issue, "success": False, "error": error}


async def download_batch(
    issues: list[dict],
    cfg: Config,
    idx: MagazineIndex,
    on_start: Callable[[dict], None] | None = None,
    on_complete: Callable[[dict, bool, str | None], None] | None = None,
) -> list[dict]:
    """Download multiple issues concurrently, bounded by max_concurrent.

    All downloads share a single RateLimitGate — if any download
    receives a 429, all downloads pause until the rate limit expires.

    Returns a list of result dicts with success/error status for each issue.
    """
    if not issues:
        return []

    # De-duplicate by issue id: overlapping subscriptions can enqueue the same
    # issue twice, which would double-download it and defeat the one-refresh
    # bound on dead-link re-scrapes. First occurrence wins, order preserved.
    seen_ids: set = set()
    deduped: list[dict] = []
    for issue in issues:
        issue_id = issue.get("id")
        if issue_id is not None:
            if issue_id in seen_ids:
                continue
            seen_ids.add(issue_id)
        deduped.append(issue)
    issues = deduped

    # Auto-extract encryption constants once before starting any downloads
    if not cfg.limewire.file_iv_b64 or not cfg.limewire.sharing_salt_b64:
        from magsync.core.downloader import auto_extract_constants
        from magsync.config import save_config

        logger.info("No encryption constants — extracting before batch download...")
        extracted = await auto_extract_constants()
        if extracted:
            # Mutate the shared config object so all downloads see the constants
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
                logger.info("Config is read-only — constants in memory only")
        else:
            logger.error("Auto-extraction failed — aborting batch. See UPDATE_KEYS.md.")
            return [
                {"issue": issue, "success": False, "error": "Encryption constants unavailable"}
                for issue in issues
            ]

    semaphore = asyncio.Semaphore(cfg.download.max_concurrent)
    rate_gate = RateLimitGate()
    # Shared per-destination locks so issues resolving to the same output file
    # are downloaded once (the on-disk dedup handles the rest), not concurrently.
    dest_locks: dict = defaultdict(asyncio.Lock)

    tasks = [
        _download_one(issue, cfg, idx, semaphore, rate_gate, on_start, on_complete, dest_locks)
        for issue in issues
    ]

    return await asyncio.gather(*tasks)
