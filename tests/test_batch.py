"""Tests for batch download edge cases."""

from __future__ import annotations

import asyncio

import magsync.core.batch as batch_mod
from magsync.config import Config, load_config
from magsync.core.batch import _download_one, download_batch
from magsync.core.downloader import RateLimitGate
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadResult, DownloadStatus


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


async def test_batch_dedupes_same_destination(tmp_path, monkeypatch):
    # Two issues (hyphen vs en-dash) resolve to the same output file and share a
    # LimeWire id. Content must be fetched once; the duplicate completes via the
    # on-disk dedup and is reported as success, not failure.
    cfg = Config()
    cfg.output_dir = str(tmp_path / "out")
    cfg.download.max_concurrent = 3
    # Non-empty constants so download_batch skips auto-extraction (download is mocked).
    cfg.limewire.file_iv_b64 = "x"
    cfg.limewire.sharing_salt_b64 = "x"

    idx = MagazineIndex(db_path=tmp_path / "index.db")
    mag = idx.get_or_create_magazine("Travel + Leisure USA", "travel + leisure usa")
    idx.add_issues(mag, [
        {"title": "Travel + Leisure USA - October 2025", "page_url": "pa",
         "limewire_url": "https://limewire.com/d/Ma9ue#k", "year": 2025, "month": 10},
        {"title": "Travel + Leisure USA – October 2025", "page_url": "pb",
         "limewire_url": "https://limewire.com/d/Ma9ue#k", "year": 2025, "month": 10},
    ])
    pending = idx.get_issues(status=DownloadStatus.PENDING)
    assert len(pending) == 2

    downloads = []

    async def fake_download_and_decrypt(lw_url, dest, **kwargs):
        downloads.append(str(dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"%PDF-1.4 fake")
        return DownloadResult(success=True, file_path=dest, file_size_bytes=12, sha256="abc")

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download_and_decrypt)

    results = []

    def on_complete(issue, success, error):
        results.append((issue["title"], success, error))

    await download_batch(pending, cfg, idx, on_complete=on_complete)
    idx.close()

    assert len(downloads) == 1                            # content fetched once, not twice
    assert len(results) == 2                              # both issues reported
    assert all(success for _t, success, _e in results)    # neither reported as a failure
