"""Regression for the combined source-block/audio/orphan daemon cycle."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from magsync import cli
from magsync.config import Config
from magsync.core import batch as batch_mod
from magsync.core import downloader as downloader_mod
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadFailureKind,
    DownloadResult,
    PipelineStatus,
    Subscription,
)
from magsync.core.scraper import FreemagazinesClient


AUDIO_URL = "https://limewire.com/d/AudioAlias#AUDIO_FRAGMENT_SECRET"
GETAWAY_URL = "https://limewire.com/d/xTsja#GETAWAY_FRAGMENT_SECRET"


def _issue(title: str, slug: str, limewire_url: str) -> dict:
    return {
        "title": title,
        "page_url": f"https://freemagazines.top/{slug}/",
        "limewire_url": limewire_url,
        "year": 2026,
        "month": 7,
    }


@pytest.mark.asyncio
async def test_combined_blocked_audio_and_orphan_cycle_is_degraded_and_secret_safe(
    tmp_path, monkeypatch, caplog
):
    cfg = Config(output_dir=str(tmp_path / "magazines"))
    cfg.subscriptions = [
        Subscription(query="First"),
        Subscription(query="Second"),
        Subscription(query="Third"),
    ]
    cfg.download.max_concurrent = 4
    cfg.download.scrape_delay = 0
    cfg.download.retry_attempts = 2
    cfg.limewire.file_iv_b64 = "eA=="
    cfg.limewire.sharing_salt_b64 = "eA=="

    idx = MagazineIndex(tmp_path / "index.db")
    magazine_id = idx.get_or_create_magazine("Cached", "cached")
    idx.add_issues(
        magazine_id,
        [
            _issue("The Economist Audio - Alias A", "audio-a-2026", AUDIO_URL),
            _issue("The Economist Audio - Alias B", "audio-b-2026", AUDIO_URL),
            _issue("Getaway - April/May 2026", "getaway-a-2026", GETAWAY_URL),
            _issue("Getaway – April/May 2026", "getaway-b-2026", GETAWAY_URL),
        ],
    )

    source_requests: list[str] = []

    def source_handler(request: httpx.Request) -> httpx.Response:
        source_requests.append(str(request.url))
        return httpx.Response(
            403,
            request=request,
            headers={
                "content-type": "text/html; charset=UTF-8",
                "cf-mitigated": "challenge",
                "cf-ray": "safe-cycle-ray",
            },
            text=(
                "<html><title>Just a moment...</title>"
                "<p>enable javascript and cookies to continue</p>"
                "<p>SOURCE_BODY_SECRET</p></html>"
            ),
        )

    raw_source_client = httpx.AsyncClient(
        transport=httpx.MockTransport(source_handler),
        follow_redirects=True,
    )
    source_client = FreemagazinesClient(
        scrape_delay=0,
        http_client=raw_source_client,
    )

    fixture = (
        Path(__file__).parent / "fixtures" / "limewire_share_orphaned.html"
    ).read_text()
    orphan_state = downloader_mod._extract_share_metadata_state(
        downloader_mod._decode_react_stream(fixture),
        sharing_id="xTsja",
    )
    assert orphan_state.state is downloader_mod.ShareMetadataState.ORPHAN_CANDIDATE

    share_page_gets: list[str] = []

    async def orphan_share_page(_client, sharing_id):
        share_page_gets.append(sharing_id)
        return fixture, orphan_state

    monkeypatch.setattr(downloader_mod, "_fetch_share_page", orphan_share_page)

    physical_downloads: list[str] = []

    async def classified_download(url, dest, **kwargs):
        physical_downloads.append(url)
        if url == AUDIO_URL:
            return DownloadResult(
                success=False,
                failure_kind=DownloadFailureKind.UNSUPPORTED,
                error=(
                    "Unsupported storage payload at "
                    "https://bucket.example/audio.zip?"
                    "X-Amz-Credential=PRESIGNED_SECRET#STORAGE_FRAGMENT_SECRET"
                ),
                unsupported=True,
                attempt_count=1,
            )
        assert url == GETAWAY_URL
        return await downloader_mod.download_and_decrypt(url, dest, **kwargs)

    monkeypatch.setattr(batch_mod, "download_and_decrypt", classified_download)

    async def no_stagger(_delay):
        return None

    monkeypatch.setattr(batch_mod.asyncio, "sleep", no_stagger)
    caplog.set_level(logging.INFO)

    try:
        report = await cli._run_daemon_cycle(
            cfg,
            idx,
            now=datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc),
            logger=logging.getLogger("magsync.combined-cycle"),
            source_client_factory=lambda **_kwargs: source_client,
        )
    finally:
        await raw_source_client.aclose()

    stored = idx.get_issues()
    pipeline = idx.get_pipeline_state()
    idx.close()

    assert len(source_requests) == 1
    assert report.source_attempted == 1
    assert report.source_failed == 1
    assert report.source_skipped == 2

    assert physical_downloads.count(AUDIO_URL) == 1
    assert physical_downloads.count(GETAWAY_URL) == 1
    assert share_page_gets == ["xTsja", "xTsja"]

    assert report.downloads_queued == 4
    assert report.downloads_unique == 2
    assert report.downloads_unsupported == 2
    assert report.downloads_unavailable == 2
    assert report.downloads_failed == 0
    assert report.pending_refreshes == 2
    assert report.status is PipelineStatus.DEGRADED
    assert pipeline["last_cycle_status"] == PipelineStatus.DEGRADED.value

    statuses = {row["title"]: row["download_status"] for row in stored}
    assert statuses["The Economist Audio - Alias A"] == "unsupported"
    assert statuses["The Economist Audio - Alias B"] == "unsupported"
    assert statuses["Getaway - April/May 2026"] == "unavailable"
    assert statuses["Getaway – April/May 2026"] == "unavailable"

    diagnostic_text = caplog.text + "\n".join(
        str(row.get("last_error") or "") for row in stored
    )
    for secret in (
        "AUDIO_FRAGMENT_SECRET",
        "GETAWAY_FRAGMENT_SECRET",
        "PRESIGNED_SECRET",
        "STORAGE_FRAGMENT_SECRET",
        "SOURCE_BODY_SECRET",
    ):
        assert secret not in diagnostic_text
