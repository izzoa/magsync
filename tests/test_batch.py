"""Tests for batch download edge cases."""

from __future__ import annotations

import asyncio

import magsync.core.batch as batch_mod
from magsync.config import Config, load_config
from magsync.core.batch import _download_one, download_batch
from magsync.core.downloader import RateLimitGate
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadResult, DownloadStatus
from magsync.core.scraper import ScrapedIssue

OLD = "https://limewire.com/d/OldId#oldkey"
FRESH = "https://limewire.com/d/NewId#newkey"
PERM_ERR = "LimeWire share link is unavailable (removed or expired)"


def _setup(tmp_path):
    """Config + index with one pending issue stored under the OLD link."""
    cfg = Config()
    cfg.output_dir = str(tmp_path / "out")
    cfg.limewire.file_iv_b64 = "x"
    cfg.limewire.sharing_salt_b64 = "x"
    idx = MagazineIndex(db_path=tmp_path / "index.db")
    mag = idx.get_or_create_magazine("M", "m")
    idx.add_issues(mag, [{
        "title": "T - January 2025",
        "page_url": "https://freemagazines.top/t-january-2025/",
        "limewire_url": OLD, "year": 2025, "month": 1,
    }])
    return cfg, idx, idx.get_issues()[0]


def _fake_scrape(limewire_url, calls=None):
    async def fake(page_url, **kwargs):
        if calls is not None:
            calls.append(page_url)
        return ScrapedIssue(title="T", page_url=page_url, limewire_url=limewire_url)
    return fake


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


async def test_permanent_error_maps_to_unavailable(tmp_path, monkeypatch):
    # A permanent "share link is unavailable" error → DownloadStatus.UNAVAILABLE
    # (not FAILED). Page still shows the same dead link → no retry.
    cfg, idx, issue = _setup(tmp_path)

    async def fake_dl(lw_url, dest, **kwargs):
        raise RuntimeError(PERM_ERR)

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_dl)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", _fake_scrape(OLD))

    await download_batch([issue], cfg, idx, on_complete=lambda *a: None)
    rows = idx.get_issues()
    idx.close()
    assert rows[0]["download_status"] == "unavailable"


# --- dead-link re-scrape (link rotation recovery) ---

async def test_dead_link_refresh_retries_and_completes(tmp_path, monkeypatch):
    cfg, idx, issue = _setup(tmp_path)
    attempts, scrapes, calls = [], [], []

    async def fake_dl(lw_url, dest, **kwargs):
        attempts.append(lw_url)
        if lw_url == OLD:
            return DownloadResult(success=False, error=PERM_ERR)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"%PDF fake")
        return DownloadResult(success=True, file_path=dest, file_size_bytes=9, sha256="h")

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_dl)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", _fake_scrape(FRESH, scrapes))

    result = await _download_one(
        issue, cfg, idx, asyncio.Semaphore(1), RateLimitGate(),
        on_complete=lambda i, s, e: calls.append((s, e)),
    )
    rows = idx.get_issues()
    idx.close()

    assert result["success"] is True
    assert attempts == [OLD, FRESH]                  # exactly one retry, fresh link
    assert scrapes == [issue["page_url"]]            # exactly one re-scrape
    assert rows[0]["download_status"] == "complete"
    assert rows[0]["limewire_url"] == FRESH          # fresh link persisted
    assert calls == [(True, None)]                   # single callback, final outcome only


async def test_dead_link_same_url_parks_without_retry(tmp_path, monkeypatch):
    cfg, idx, issue = _setup(tmp_path)
    attempts, calls = [], []

    async def fake_dl(lw_url, dest, **kwargs):
        attempts.append(lw_url)
        return DownloadResult(success=False, error=PERM_ERR)

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_dl)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", _fake_scrape(OLD))

    await _download_one(issue, cfg, idx, asyncio.Semaphore(1), RateLimitGate(),
                        on_complete=lambda i, s, e: calls.append((s, e)))
    rows = idx.get_issues()
    idx.close()

    assert attempts == [OLD]                         # no retry with the same dead link
    assert rows[0]["download_status"] == "unavailable"
    assert rows[0]["limewire_url"] == OLD
    assert calls == [(False, PERM_ERR)]


async def test_dead_link_scrape_error_parks_unavailable(tmp_path, monkeypatch):
    cfg, idx, issue = _setup(tmp_path)
    attempts = []

    async def fake_dl(lw_url, dest, **kwargs):
        attempts.append(lw_url)
        return DownloadResult(success=False, error=PERM_ERR)

    async def broken_scrape(page_url, **kwargs):
        raise RuntimeError("404 post deleted")

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_dl)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", broken_scrape)

    await _download_one(issue, cfg, idx, asyncio.Semaphore(1), RateLimitGate())
    rows = idx.get_issues()
    idx.close()

    assert attempts == [OLD]
    assert rows[0]["download_status"] == "unavailable"


async def test_refresh_retry_permanent_parks_without_second_scrape(tmp_path, monkeypatch):
    cfg, idx, issue = _setup(tmp_path)
    attempts, scrapes = [], []

    async def fake_dl(lw_url, dest, **kwargs):
        attempts.append(lw_url)
        return DownloadResult(success=False, error=PERM_ERR)

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_dl)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", _fake_scrape(FRESH, scrapes))

    await _download_one(issue, cfg, idx, asyncio.Semaphore(1), RateLimitGate())
    rows = idx.get_issues()
    idx.close()

    assert attempts == [OLD, FRESH]                  # bounded: one refresh retry
    assert scrapes == [issue["page_url"]]            # exactly one re-scrape
    assert rows[0]["download_status"] == "unavailable"


async def test_refresh_retry_transient_marks_failed(tmp_path, monkeypatch):
    cfg, idx, issue = _setup(tmp_path)

    async def fake_dl(lw_url, dest, **kwargs):
        if lw_url == OLD:
            return DownloadResult(success=False, error=PERM_ERR)
        return DownloadResult(success=False, error="HTTP 500: transient blip")

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_dl)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", _fake_scrape(FRESH))

    await _download_one(issue, cfg, idx, asyncio.Semaphore(1), RateLimitGate())
    rows = idx.get_issues()
    idx.close()

    assert rows[0]["download_status"] == "failed"    # transient → failed, not unavailable
    assert rows[0]["limewire_url"] == FRESH          # fresh link persisted for next pass


async def test_transient_failure_never_rescrapes(tmp_path, monkeypatch):
    cfg, idx, issue = _setup(tmp_path)
    scrapes = []

    async def fake_dl(lw_url, dest, **kwargs):
        return DownloadResult(success=False, error="429 rate limited (retry-after: 30s)")

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_dl)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", _fake_scrape(FRESH, scrapes))

    await _download_one(issue, cfg, idx, asyncio.Semaphore(1), RateLimitGate())
    rows = idx.get_issues()
    idx.close()

    assert scrapes == []                             # transient errors never re-scrape
    assert rows[0]["download_status"] == "failed"


async def test_batch_dedupes_duplicate_issue_ids(tmp_path, monkeypatch):
    # Overlapping subscriptions can enqueue the same issue twice; only one task runs.
    cfg, idx, issue = _setup(tmp_path)
    downloads, calls = [], []

    async def fake_dl(lw_url, dest, **kwargs):
        downloads.append(lw_url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"%PDF fake")
        return DownloadResult(success=True, file_path=dest, file_size_bytes=9, sha256="h")

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_dl)

    await download_batch([issue, dict(issue)], cfg, idx,
                         on_complete=lambda i, s, e: calls.append(s))
    idx.close()

    assert len(downloads) == 1                       # duplicate row dropped
    assert calls == [True]                           # one callback total


async def test_unsupported_result_marks_status_and_skips_refresh(tmp_path, monkeypatch):
    # A live share with a non-PDF payload: terminal 'unsupported' status, no
    # dead-link page re-scrape (the link works), structured flag in the result.
    cfg, idx, issue = _setup(tmp_path)
    scrape_calls: list[str] = []
    monkeypatch.setattr(batch_mod, "scrape_detail_page", _fake_scrape(FRESH, scrape_calls))

    async def fake_download(url, dest, **kwargs):
        return DownloadResult(success=False, unsupported=True,
                              error="Unsupported payload: x.zip")

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download)

    outcomes: list[tuple[bool, str | None]] = []
    result = await _download_one(
        issue, cfg, idx, asyncio.Semaphore(1), RateLimitGate(),
        on_complete=lambda i, s, e: outcomes.append((s, e)),
    )
    status = idx.conn.execute(
        "SELECT status FROM downloads WHERE issue_id = ?", (issue["id"],)
    ).fetchone()[0]
    idx.close()

    assert result["unsupported"] is True and result["success"] is False
    assert status == DownloadStatus.UNSUPPORTED.value
    assert scrape_calls == []  # dead-link refresh never invoked for a live share
    assert outcomes == [(False, "Unsupported payload: x.zip")]
