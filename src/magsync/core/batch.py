"""Concurrent batch download manager."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Callable

from magsync.config import Config
from magsync.core.downloader import download_and_decrypt, RateLimitGate
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

        lw_url = issue.get("limewire_url")
        if not lw_url:
            return {"issue": issue, "success": False, "error": "No download link"}

        if on_start:
            on_start(issue)

        idx.update_download_status(issue["id"], DownloadStatus.DOWNLOADING)
        dest = organize_path(issue["title"], issue["page_url"], cfg.output_dir)

        try:
            result = await download_and_decrypt(
                lw_url, dest, constants=cfg.limewire, rate_gate=rate_gate,
            )
        except Exception as e:
            idx.update_download_status(issue["id"], DownloadStatus.FAILED)
            error_msg = str(e)
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
            idx.update_download_status(issue["id"], DownloadStatus.FAILED)
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

    semaphore = asyncio.Semaphore(cfg.download.max_concurrent)
    rate_gate = RateLimitGate()

    tasks = [
        _download_one(issue, cfg, idx, semaphore, rate_gate, on_start, on_complete)
        for issue in issues
    ]

    return await asyncio.gather(*tasks)
