"""Typed single-flight, refresh, and worker-isolation batch tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest

import magsync.core.batch as batch_mod
from magsync.config import Config
from magsync.core.batch import download_batch, refresh_due_links
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadFailureKind,
    DownloadResult,
    DownloadStatus,
    RefreshOutcomeKind,
    RetryAction,
    SourceError,
    SourceFailureKind,
)
from magsync.core.scraper import FreemagazinesClient, ScrapedIssue


SHARED = "https://limewire.com/d/SameShare#same-fragment"
FRESH_A = "https://limewire.com/d/FreshA#fresh-a"
FRESH_B = "https://limewire.com/d/FreshB#fresh-b"


@pytest.fixture(autouse=True)
def no_batch_stagger(monkeypatch):
    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(batch_mod.asyncio, "sleep", no_sleep)


def _setup(tmp_path: Path, rows: list[dict]):
    cfg = Config()
    cfg.output_dir = str(tmp_path / "out")
    cfg.download.max_concurrent = 4
    cfg.limewire.file_iv_b64 = "configured"
    cfg.limewire.sharing_salt_b64 = "configured"

    idx = MagazineIndex(db_path=tmp_path / "index.db")
    magazine_id = idx.get_or_create_magazine("Batch Tests", "batch tests")
    idx.add_issues(magazine_id, rows)
    issues_by_page = {row["page_url"]: row for row in idx.get_issues()}
    # Wanted (manual) provenance: batch mechanics are provenance-independent,
    # and intent scoping has its own dedicated tests.
    idx.mark_manual([row["id"] for row in issues_by_page.values()])
    return cfg, idx, issues_by_page


def _row(title: str, page: str, url: str) -> dict:
    return {
        "title": title,
        "page_url": f"https://freemagazines.top/{page}/",
        "limewire_url": url,
        "year": 2025,
        "month": 1,
    }


def _unavailable() -> DownloadResult:
    return DownloadResult(
        success=False,
        failure_kind=DownloadFailureKind.SHARE_UNAVAILABLE,
        error="Share unavailable",
        attempt_count=1,
    )


async def test_exact_normalized_url_single_flights_and_fans_canonical_path(
    tmp_path, monkeypatch
):
    cfg, idx, by_page = _setup(
        tmp_path,
        [
            _row("Alias A - January 2025", "alias-a-2025", SHARED),
            _row(
                "Alias B - January 2025",
                "alias-b-2025",
                "https://www.limewire.com:443/d/SameShare#same-fragment",
            ),
        ],
    )
    issues = list(by_page.values())
    downloads: list[str] = []
    callbacks: list[tuple[int, bool, DownloadFailureKind | None]] = []

    async def fake_download(url, dest, **kwargs):
        downloads.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"%PDF shared")
        return DownloadResult(
            success=True,
            file_path=dest,
            file_size_bytes=11,
            sha256="shared-hash",
            attempt_count=1,
        )

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download)

    def on_complete(issue, success, error, failure_kind):
        callbacks.append((issue["id"], success, failure_kind))

    results = await download_batch(issues, cfg, idx, on_complete=on_complete)
    stored = idx.get_issues()
    idx.close()

    assert downloads == [SHARED]
    assert len(results) == 2
    assert all(result["success"] for result in results)
    assert all(result["failure_kind"] is None for result in results)
    canonical_paths = {str(result["path"]) for result in results}
    assert len(canonical_paths) == 1
    assert Path(canonical_paths.pop()).is_file()
    assert {row["file_path"] for row in stored} == {
        str(results[0]["path"]),
    }
    assert sorted(callbacks) == sorted(
        (issue["id"], True, None) for issue in issues
    )


async def test_same_share_id_with_different_fragments_is_not_coalesced(
    tmp_path, monkeypatch
):
    first = "https://limewire.com/d/SameId#fragment-one"
    second = "https://limewire.com/d/SameId#fragment-two"
    cfg, idx, by_page = _setup(
        tmp_path,
        [
            _row("Fragment A - January 2025", "fragment-a-2025", first),
            _row("Fragment B - January 2025", "fragment-b-2025", second),
        ],
    )
    downloads: list[str] = []

    async def fake_download(url, dest, **kwargs):
        downloads.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"%PDF fragment")
        return DownloadResult(success=True, file_path=dest, attempt_count=1)

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download)
    results = await download_batch(list(by_page.values()), cfg, idx)
    idx.close()

    assert set(downloads) == {first, second}
    assert all(result["success"] for result in results)


async def test_unavailable_aliases_refresh_pages_independently_and_reenter_flights(
    tmp_path, monkeypatch
):
    cfg, idx, by_page = _setup(
        tmp_path,
        [
            _row("Rotate A - January 2025", "rotate-a-2025", SHARED),
            _row("Rotate B - January 2025", "rotate-b-2025", SHARED),
        ],
    )
    issues = list(by_page.values())
    downloads: list[tuple[str, int]] = []
    scrapes: list[str] = []
    callbacks: list[int] = []

    async def fake_download(url, dest, **kwargs):
        downloads.append((url, kwargs["retry_attempts"]))
        if url == SHARED:
            return _unavailable()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"%PDF rotated")
        return DownloadResult(success=True, file_path=dest, attempt_count=1)

    async def fake_scrape(page_url, **kwargs):
        scrapes.append(page_url)
        url = FRESH_A if "rotate-a" in page_url else FRESH_B
        return ScrapedIssue("Rotated", page_url, limewire_url=url)

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", fake_scrape)

    results = await download_batch(
        issues,
        cfg,
        idx,
        on_complete=lambda issue, *_args: callbacks.append(issue["id"]),
    )
    stored = idx.get_issues()
    idx.close()

    assert downloads.count((SHARED, cfg.download.retry_attempts)) == 1
    assert set(downloads) == {
        (SHARED, cfg.download.retry_attempts),
        (FRESH_A, 0),
        (FRESH_B, 0),
    }
    assert set(scrapes) == {issue["page_url"] for issue in issues}
    assert all(result["success"] for result in results)
    assert {row["limewire_url"] for row in stored} == {FRESH_A, FRESH_B}
    assert len(callbacks) == len(set(callbacks)) == 2


async def test_source_blocked_refresh_parks_source_only_action(
    tmp_path, monkeypatch
):
    cfg, idx, by_page = _setup(
        tmp_path,
        [_row("Blocked - January 2025", "blocked-2025", SHARED)],
    )
    issue = next(iter(by_page.values()))
    downloads: list[str] = []
    callbacks: list[DownloadFailureKind | None] = []

    async def fake_download(url, dest, **kwargs):
        downloads.append(url)
        return _unavailable()

    async def blocked_scrape(page_url, **kwargs):
        raise SourceError(
            SourceFailureKind.ACCESS_BLOCKED,
            "Source access blocked",
            operation="detail",
        )

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", blocked_scrape)
    before = datetime.now(timezone.utc)

    results = await download_batch(
        [issue],
        cfg,
        idx,
        on_complete=lambda _i, _s, _e, kind: callbacks.append(kind),
    )
    stored = idx.get_issues()[0]
    idx.close()

    assert downloads == [SHARED]
    assert results[0]["failure_kind"] is DownloadFailureKind.SHARE_UNAVAILABLE
    assert results[0]["refresh_outcome"].kind is RefreshOutcomeKind.SOURCE_BLOCKED
    assert stored["download_status"] == DownloadStatus.UNAVAILABLE.value
    assert stored["next_action"] == RetryAction.REFRESH_LINK.value
    assert datetime.fromisoformat(stored["next_retry_at"]) > before
    assert callbacks == [DownloadFailureKind.SHARE_UNAVAILABLE]


async def test_rotated_unsupported_is_reclassified_and_called_back_once(
    tmp_path, monkeypatch, caplog
):
    caplog.set_level("INFO", logger="magsync")
    cfg, idx, by_page = _setup(
        tmp_path,
        [_row("Rotated Zip - January 2025", "rotated-zip-2025", SHARED)],
    )
    issue = next(iter(by_page.values()))
    attempts: list[tuple[str, int]] = []
    callbacks: list[tuple[bool, DownloadFailureKind | None]] = []

    async def fake_download(url, dest, **kwargs):
        attempts.append((url, kwargs["retry_attempts"]))
        if url == SHARED:
            return _unavailable()
        return DownloadResult(
            success=False,
            failure_kind=DownloadFailureKind.UNSUPPORTED,
            error="Unsupported payload: audio.zip",
            attempt_count=1,
        )

    async def fake_scrape(page_url, **kwargs):
        return ScrapedIssue("Zip", page_url, limewire_url=FRESH_A)

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download)
    monkeypatch.setattr(batch_mod, "scrape_detail_page", fake_scrape)
    results = await download_batch(
        [issue],
        cfg,
        idx,
        on_complete=lambda _i, success, _e, kind: callbacks.append((success, kind)),
    )
    stored = idx.get_issues()[0]
    idx.close()

    assert attempts == [(SHARED, cfg.download.retry_attempts), (FRESH_A, 0)]
    assert results[0]["failure_kind"] is DownloadFailureKind.UNSUPPORTED
    assert results[0]["unsupported"] is True
    assert stored["limewire_url"] == FRESH_A
    assert stored["download_status"] == DownloadStatus.UNSUPPORTED.value
    assert stored["next_action"] is None
    assert callbacks == [(False, DownloadFailureKind.UNSUPPORTED)]
    assert "rotated share" in caplog.text
    assert "same-fragment" not in caplog.text
    assert "fresh-a" not in caplog.text


async def test_different_urls_keep_destination_locking(tmp_path, monkeypatch):
    first = "https://limewire.com/d/First#first"
    second = "https://limewire.com/d/Second#second"
    cfg, idx, by_page = _setup(
        tmp_path,
        [
            _row("Same Name - January 2025", "same-name-a-2025", first),
            _row("Same Name – January 2025", "same-name-b-2025", second),
        ],
    )
    downloads: list[str] = []

    async def fake_download(url, dest, **kwargs):
        downloads.append(url)
        # Yield while holding the destination lock so an unlocked
        # implementation would enter the second physical operation.
        await asyncio.sleep(0)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"%PDF destination")
        return DownloadResult(success=True, file_path=dest, attempt_count=1)

    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download)
    results = await download_batch(list(by_page.values()), cfg, idx)
    idx.close()

    assert len(downloads) == 1
    assert all(result["success"] for result in results)
    assert {result["path"] for result in results} == {results[0]["path"]}


async def test_worker_exception_is_typed_and_does_not_cancel_sibling(
    tmp_path, monkeypatch
):
    good_url = "https://limewire.com/d/Good#good"
    bad_url = "https://limewire.com/d/Bad#bad"
    cfg, idx, by_page = _setup(
        tmp_path,
        [
            _row("Bad Worker - January 2025", "bad-worker-2025", bad_url),
            _row("Good Worker - January 2025", "good-worker-2025", good_url),
        ],
    )
    real_organize = batch_mod.organize_path

    def flaky_organize(title, page_url, output_dir):
        if title.startswith("Bad Worker"):
            raise RuntimeError("organizer exploded")
        return real_organize(title, page_url, output_dir)

    async def fake_download(url, dest, **kwargs):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"%PDF survivor")
        return DownloadResult(success=True, file_path=dest, attempt_count=1)

    monkeypatch.setattr(batch_mod, "organize_path", flaky_organize)
    monkeypatch.setattr(batch_mod, "download_and_decrypt", fake_download)
    results = await download_batch(list(by_page.values()), cfg, idx)
    stored = {row["title"]: row for row in idx.get_issues()}
    idx.close()

    by_title = {result["issue"]["title"]: result for result in results}
    assert by_title["Bad Worker - January 2025"]["failure_kind"] is (
        DownloadFailureKind.INTERNAL
    )
    assert by_title["Good Worker - January 2025"]["success"] is True
    assert stored["Bad Worker - January 2025"]["download_status"] == "failed"
    assert stored["Good Worker - January 2025"]["download_status"] == "complete"


async def test_cancellation_propagates_from_worker(tmp_path, monkeypatch):
    cfg, idx, by_page = _setup(
        tmp_path,
        [_row("Cancel - January 2025", "cancel-2025", SHARED)],
    )

    async def cancelled_download(url, dest, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(batch_mod, "download_and_decrypt", cancelled_download)
    with pytest.raises(asyncio.CancelledError):
        await download_batch(list(by_page.values()), cfg, idx)
    idx.close()


async def test_due_refresh_rotates_without_requesting_dead_share(tmp_path, monkeypatch):
    cfg, idx, by_page = _setup(
        tmp_path,
        [_row("Due Refresh - January 2025", "due-refresh-2025", SHARED)],
    )
    issue = next(iter(by_page.values()))
    idx.record_download_failure(
        issue["id"],
        DownloadFailureKind.SHARE_UNAVAILABLE,
        "dead share",
    )
    idx.schedule_link_refresh(issue["id"], datetime.now(timezone.utc))
    claimed = idx.claim_due_link_refreshes([], now=datetime.now(timezone.utc))
    assert len(claimed) == 1

    async def fake_scrape(page_url, **kwargs):
        return ScrapedIssue("Due", page_url, limewire_url=FRESH_A)

    async def forbidden_download(*args, **kwargs):
        pytest.fail("source-only refresh requested the known-dead LimeWire share")

    monkeypatch.setattr(batch_mod, "scrape_detail_page", fake_scrape)
    monkeypatch.setattr(batch_mod, "download_and_decrypt", forbidden_download)
    async with FreemagazinesClient(scrape_delay=0) as source_client:
        outcomes = await refresh_due_links(claimed, idx, source_client)

    stored = idx.get_issues()[0]
    idx.close()

    assert outcomes[0]["success"] is True
    assert outcomes[0]["outcome"].kind is RefreshOutcomeKind.ROTATED
    assert stored["limewire_url"] == FRESH_A
    assert stored["download_status"] == DownloadStatus.PENDING.value
    assert stored["next_action"] is None
