"""Tests for batch download edge cases."""

from __future__ import annotations

import asyncio

from magsync.config import load_config
from magsync.core.batch import _download_one
from magsync.core.downloader import RateLimitGate
from magsync.core.index import MagazineIndex


async def test_no_download_link_reports_via_callback(tmp_path):
    cfg = load_config()
    idx = MagazineIndex(db_path=tmp_path / "index.db")
    calls: list[tuple[bool, str | None]] = []

    def on_complete(issue, success, error):
        calls.append((success, error))

    issue = {"id": 1, "title": "T", "page_url": "p", "limewire_url": ""}
    result = await _download_one(
        issue,
        cfg,
        idx,
        asyncio.Semaphore(1),
        RateLimitGate(),
        on_complete=on_complete,
    )
    idx.close()

    assert result["success"] is False
    assert result["error"] == "No download link"
    # The skipped issue is reported instead of being silently dropped.
    assert calls == [(False, "No download link")]
