"""Concurrent batch download manager."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Callable

from magsync.config import Config
from magsync.core.downloader import download_and_decrypt, RateLimitGate, _is_permanent_error
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus
from magsync.core.organizer import organize_path

logger = logging.getLogger("magsync")


async def _download_one(
    issue: dict,
    cfg: Config,
    idx: MagazineIndex,
    semaphore: asyncio.Semaphore,
    rate_gate: RateLimitGate,
    on_start: Callable[[dict], None] | None = None,
    on_complete: Callable[[dict, bool, str | None], None] | None = None,
) -> dict:
    """Download a single issue, respecting the concurrency semaphore and rate limit gate.

    Returns a result dict with issue info and success/error status.
    """
    async with semaphore:
        # Wait if a 429 pause is active before starting
        await rate_gate.wait()

        # Stagger concurrent requests to avoid hitting LimeWire too fast
        await asyncio.sleep(1)

        lw_url = issue.get("limewire_url")
        if not lw_url:
            return {"issue": issue, "success": False, "error": "No download link"}

        dest = organize_path(issue["title"], issue["page_url"], cfg.output_dir)

        # Skip if file already exists on disk (e.g., index was wiped but files remain)
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

        try:
            result = await download_and_decrypt(
                lw_url, dest, constants=cfg.limewire, rate_gate=rate_gate,
                retry_attempts=cfg.download.retry_attempts,
            )
        except Exception as e:
            error_msg = str(e)
            status = DownloadStatus.UNAVAILABLE if _is_permanent_error(error_msg) else DownloadStatus.FAILED
            idx.update_download_status(issue["id"], status)
            if on_complete:
                on_complete(issue, False, error_msg)
            return {"issue": issue, "success": False, "error": error_msg}

        if result.success:
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
        else:
            error = result.error or ""
            status = DownloadStatus.UNAVAILABLE if _is_permanent_error(error) else DownloadStatus.FAILED
            idx.update_download_status(issue["id"], status)
            if on_complete:
                on_complete(issue, False, result.error)
            return {"issue": issue, "success": False, "error": result.error}


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

    tasks = [
        _download_one(issue, cfg, idx, semaphore, rate_gate, on_start, on_complete)
        for issue in issues
    ]

    return await asyncio.gather(*tasks)
