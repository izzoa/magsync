"""Focused daemon-cycle scheduling, reporting, and liveness tests."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import pytest

from magsync import cli
from magsync.config import Config
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadFailureKind,
    DownloadStatus,
    PipelineStatus,
    RefreshOutcome,
    RefreshOutcomeKind,
    RetryAction,
    SourceFailure,
    SourceFailureKind,
    SourceResult,
    Subscription,
)
from magsync.core.scraper import ScrapedIssue


OLD_URL = "https://limewire.com/d/OldShare#old-secret"
FRESH_URL = "https://limewire.com/d/FreshShare#fresh-secret"


class ScriptedSource:
    """Minimal cycle-scoped source client with challenge-circuit behavior."""

    def __init__(self, results: list[SourceResult] | None = None):
        self.results = list(results or [])
        self.searches: list[str] = []
        self._circuit_failure: SourceFailure | None = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        return None

    @property
    def circuit_open(self) -> bool:
        return self._circuit_failure is not None

    @property
    def circuit_failure(self) -> SourceFailure | None:
        return self._circuit_failure

    async def search_with_details(self, query: str) -> SourceResult:
        self.searches.append(query)
        result = self.results.pop(0)
        if (
            result.failure is not None
            and result.failure.kind is SourceFailureKind.ACCESS_BLOCKED
        ):
            self._circuit_failure = result.failure
        return result


def _source_factory(source: ScriptedSource):
    return lambda **_kwargs: source


def _config(tmp_path, *queries: str) -> Config:
    cfg = Config(output_dir=str(tmp_path / "magazines"))
    cfg.subscriptions = [Subscription(query=query) for query in queries]
    return cfg


def _add_issue(
    idx: MagazineIndex,
    *,
    title: str = "Issue July 2026",
    page_url: str = "https://freemagazines.top/issue-july-2026/",
    limewire_url: str = OLD_URL,
) -> int:
    magazine_id = idx.get_or_create_magazine("Magazine", "magazine")
    idx.add_issues(
        magazine_id,
        [
            {
                "title": title,
                "page_url": page_url,
                "limewire_url": limewire_url,
                "year": 2026,
                "month": 7,
            }
        ],
    )
    return next(
        row["id"] for row in idx.get_issues() if row["page_url"] == page_url
    )


def _blocked(message: str = "Source challenge blocked access") -> SourceResult:
    return SourceResult(
        failure=SourceFailure(
            SourceFailureKind.ACCESS_BLOCKED,
            message,
            operation="search",
            status_code=403,
            host="freemagazines.top",
            cf_ray="safe-ray",
        )
    )


@pytest.mark.asyncio
async def test_challenge_stops_source_but_cached_result_drives_report(
    tmp_path, monkeypatch, caplog
):
    """One blocked probe skips siblings while cached work and notification continue."""

    idx = MagazineIndex(tmp_path / "index.db")
    issue_id = _add_issue(idx)
    cfg = _config(tmp_path, "One", "Two", "Three")
    source = ScriptedSource([_blocked()])
    batch_sources = []
    notified = []

    async def fake_batch(issues, _cfg, _idx, **kwargs):
        batch_sources.append(kwargs["source_client"])
        # Deliberately do not invoke callbacks: daemon counters must use these
        # returned records, including batch-level/alias paths.
        return [
            {
                "issue": issues[0],
                "success": True,
                "error": None,
                "failure_kind": None,
                "path": tmp_path / "issue.pdf",
            }
        ]

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_batch)
    monkeypatch.setattr(
        "magsync.core.notify.send_download_summary",
        lambda issues, _settings: notified.extend(issues),
    )
    caplog.set_level(logging.INFO, logger="magsync.daemon-test")

    report = await cli._run_daemon_cycle(
        cfg,
        idx,
        logger=logging.getLogger("magsync.daemon-test"),
        source_client_factory=_source_factory(source),
    )

    assert source.searches == ["One"]
    assert batch_sources == [source]
    assert [row["id"] for row in notified] == [issue_id]
    assert report.source_attempted == 1
    assert report.source_failed == 1
    assert report.source_skipped == 2
    assert report.source_completed == 0
    assert report.downloads_queued == report.downloads_complete == 1
    assert report.status is PipelineStatus.DEGRADED
    assert "source 0/3 completed" in caplog.text
    state = idx.get_pipeline_state()
    assert state["last_cycle_status"] == PipelineStatus.DEGRADED.value
    assert state["consecutive_source_failure_cycles"] == 1
    idx.close()


@pytest.mark.asyncio
async def test_due_transient_downloads_run_without_restart_but_not_before_due(
    tmp_path, monkeypatch
):
    idx = MagazineIndex(tmp_path / "index.db")
    first = _add_issue(idx, title="First", page_url="https://freemagazines.top/first/")
    second = _add_issue(
        idx,
        title="Second",
        page_url="https://freemagazines.top/second/",
        limewire_url="https://limewire.com/d/Second#second-secret",
    )
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    idx.record_download_failure(
        first,
        DownloadFailureKind.TRANSIENT,
        "temporary",
        next_action=RetryAction.DOWNLOAD,
        next_retry_at=now,
    )
    idx.record_download_failure(
        second,
        DownloadFailureKind.TRANSIENT,
        "temporary",
        next_action=RetryAction.DOWNLOAD,
        next_retry_at=now + timedelta(hours=1),
    )
    attempted: list[int] = []

    async def fake_batch(issues, _cfg, real_idx, **_kwargs):
        returned = []
        for issue in issues:
            attempted.append(issue["id"])
            real_idx.update_download_status(issue["id"], DownloadStatus.COMPLETE)
            returned.append(
                {
                    "issue": issue,
                    "success": True,
                    "error": None,
                    "failure_kind": None,
                }
            )
        return returned

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_batch)
    cfg = _config(tmp_path)

    first_report = await cli._run_daemon_cycle(
        cfg,
        idx,
        now=now,
        source_client_factory=_source_factory(ScriptedSource()),
    )
    # Reopen the database before the future timestamp: persisted UTC timing,
    # not process-local state, must keep the second row parked.
    idx.close()
    idx = MagazineIndex(tmp_path / "index.db")
    early_report = await cli._run_daemon_cycle(
        cfg,
        idx,
        now=now + timedelta(minutes=30),
        source_client_factory=_source_factory(ScriptedSource()),
    )
    due_report = await cli._run_daemon_cycle(
        cfg,
        idx,
        now=now + timedelta(hours=2),
        source_client_factory=_source_factory(ScriptedSource()),
    )

    assert attempted == [first, second]
    assert first_report.downloads_queued == 1
    assert early_report.downloads_queued == 0
    assert due_report.downloads_queued == 1
    idx.close()


@pytest.mark.asyncio
async def test_due_refresh_is_source_only_then_rotated_url_is_downloaded(
    tmp_path, monkeypatch
):
    idx = MagazineIndex(tmp_path / "index.db")
    issue_id = _add_issue(idx)
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    idx.record_download_failure(
        issue_id,
        DownloadFailureKind.SHARE_UNAVAILABLE,
        "orphaned",
    )
    idx.schedule_link_refresh(issue_id, now - timedelta(seconds=1))
    cfg = _config(tmp_path)
    source = ScriptedSource()
    refreshed = []
    downloaded_urls = []

    async def fake_refresh(issues, real_idx, source_client):
        assert source_client is source
        refreshed.extend(issues)
        outcome = RefreshOutcome(RefreshOutcomeKind.ROTATED, url=FRESH_URL)
        assert real_idx.resolve_link_refresh(issues[0]["id"], outcome)
        return [
            {
                "issue": issues[0],
                "outcome": outcome,
                "success": True,
                "failure_kind": None,
            }
        ]

    async def fake_batch(issues, _cfg, real_idx, **_kwargs):
        downloaded_urls.extend(issue["limewire_url"] for issue in issues)
        real_idx.update_download_status(issues[0]["id"], DownloadStatus.UNSUPPORTED)
        return [
            {
                "issue": issues[0],
                "success": False,
                "error": "audio payload",
                "failure_kind": DownloadFailureKind.UNSUPPORTED,
            }
        ]

    monkeypatch.setattr("magsync.core.batch.refresh_due_links", fake_refresh)
    monkeypatch.setattr("magsync.core.batch.download_batch", fake_batch)

    report = await cli._run_daemon_cycle(
        cfg,
        idx,
        now=now,
        source_client_factory=_source_factory(source),
    )

    assert [row["id"] for row in refreshed] == [issue_id]
    assert downloaded_urls == [FRESH_URL]
    assert OLD_URL not in downloaded_urls
    assert report.downloads_unsupported == 1
    assert report.downloads_failed == 0
    assert report.pending_refreshes == 0
    assert report.status is PipelineStatus.HEALTHY
    idx.close()


@pytest.mark.asyncio
async def test_valid_empty_and_expected_terminal_downloads_are_healthy(
    tmp_path, monkeypatch
):
    idx = MagazineIndex(tmp_path / "index.db")
    _add_issue(idx)
    _add_issue(
        idx,
        title="Audio",
        page_url="https://freemagazines.top/audio/",
        limewire_url="https://limewire.com/d/Audio#secret",
    )
    source = ScriptedSource([SourceResult(validated_empty=True)])

    async def fake_batch(issues, _cfg, _idx, **_kwargs):
        return [
            {
                "issue": issues[0],
                "success": False,
                "error": "wording may change",
                "failure_kind": DownloadFailureKind.SHARE_UNAVAILABLE,
            },
            {
                "issue": issues[1],
                "success": False,
                "error": "also arbitrary",
                "failure_kind": DownloadFailureKind.UNSUPPORTED,
            },
        ]

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_batch)
    report = await cli._run_daemon_cycle(
        _config(tmp_path, "Nothing"),
        idx,
        source_client_factory=_source_factory(source),
    )

    assert report.source_empty == 1
    assert report.downloads_unavailable == 1
    assert report.downloads_unsupported == 1
    assert report.downloads_failed == 0
    assert report.status is PipelineStatus.HEALTHY
    idx.close()


@pytest.mark.asyncio
async def test_partial_details_degrade_without_discarding_valid_issue(
    tmp_path, monkeypatch
):
    idx = MagazineIndex(tmp_path / "index.db")
    valid = ScrapedIssue(
        title="Magazine - July 2026",
        page_url="https://freemagazines.top/magazine-july-2026/",
        limewire_url=OLD_URL,
    )
    partial = SourceResult(
        items=[valid],
        failures=[
            SourceFailure(
                SourceFailureKind.PROTOCOL,
                "One detail page was malformed",
                operation="detail",
            )
        ],
    )
    source = ScriptedSource([partial])

    async def fake_batch(issues, _cfg, _idx, **_kwargs):
        return [
            {
                "issue": issues[0],
                "success": True,
                "error": None,
                "failure_kind": None,
            }
        ]

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_batch)
    report = await cli._run_daemon_cycle(
        _config(tmp_path, "Magazine"),
        idx,
        source_client_factory=_source_factory(source),
    )

    assert report.source_succeeded == 1
    assert report.detail_failures == 1
    assert report.downloads_complete == 1
    assert report.status is PipelineStatus.DEGRADED
    assert idx.get_issues()[0]["title"] == valid.title
    idx.close()


@pytest.mark.asyncio
async def test_local_cycle_failure_is_failed_and_reason_is_sanitized(
    tmp_path, monkeypatch
):
    idx = MagazineIndex(tmp_path / "index.db")

    def broken_claim(**_kwargs):
        raise RuntimeError(
            "database failed for https://storage.example/file?X-Amz-Signature=secret"
            "#fragment-secret"
        )

    monkeypatch.setattr(idx, "claim_pending_and_due_downloads", broken_claim)
    report = await cli._run_daemon_cycle(
        _config(tmp_path),
        idx,
        source_client_factory=_source_factory(ScriptedSource()),
    )

    assert report.status is PipelineStatus.FAILED
    assert "secret" not in (report.reason or "")
    assert "https://storage.example/file" in (report.reason or "")
    assert idx.get_pipeline_state()["last_cycle_status"] == PipelineStatus.FAILED.value
    idx.close()


@pytest.mark.asyncio
async def test_pipeline_state_recovers_on_fresh_cycle_client(tmp_path, monkeypatch):
    idx = MagazineIndex(tmp_path / "index.db")
    cfg = _config(tmp_path, "Magazine")

    async def no_downloads(*_args, **_kwargs):
        return []

    monkeypatch.setattr("magsync.core.batch.download_batch", no_downloads)
    first_at = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    blocked_source = ScriptedSource([_blocked()])
    healthy_source = ScriptedSource([SourceResult(validated_empty=True)])

    await cli._run_daemon_cycle(
        cfg,
        idx,
        now=first_at,
        source_client_factory=_source_factory(blocked_source),
    )
    degraded = idx.get_pipeline_state()
    recovered_report = await cli._run_daemon_cycle(
        cfg,
        idx,
        now=first_at + timedelta(hours=1),
        source_client_factory=_source_factory(healthy_source),
    )
    recovered = idx.get_pipeline_state()

    assert degraded["consecutive_source_failure_cycles"] == 1
    assert recovered_report.status is PipelineStatus.HEALTHY
    assert recovered["consecutive_source_failure_cycles"] == 0
    assert recovered["last_successful_source_check_at"] == (
        first_at + timedelta(hours=1)
    ).isoformat()
    assert blocked_source is not healthy_source
    idx.close()


@pytest.mark.asyncio
async def test_heartbeat_remains_independent_during_degraded_cycle(
    tmp_path, monkeypatch
):
    heartbeat = tmp_path / "magsync-healthy"
    monkeypatch.setattr(cli, "HEALTH_CHECK_PATH", heartbeat)
    stop = cli._start_heartbeat(interval=0.01)
    idx = MagazineIndex(tmp_path / "index.db")
    try:
        report = await cli._run_daemon_cycle(
            _config(tmp_path, "Magazine"),
            idx,
            source_client_factory=_source_factory(ScriptedSource([_blocked()])),
        )
        assert report.status is PipelineStatus.DEGRADED
        deadline = time.monotonic() + 0.5
        while not heartbeat.exists() and time.monotonic() < deadline:
            time.sleep(0.005)
        assert heartbeat.exists()
        first_mtime = heartbeat.stat().st_mtime_ns
        time.sleep(0.03)
        assert heartbeat.stat().st_mtime_ns >= first_mtime
        stop()
        time.sleep(0.03)
        stopped_mtime = heartbeat.stat().st_mtime_ns
        time.sleep(0.03)
        assert heartbeat.stat().st_mtime_ns == stopped_mtime
    finally:
        stop()
        idx.close()


@pytest.mark.asyncio
async def test_daemon_logs_and_third_party_request_logging_are_secret_safe(
    tmp_path, monkeypatch, caplog
):
    idx = MagazineIndex(tmp_path / "index.db")
    _add_issue(idx)
    secret_fragment = "fragment-do-not-log"
    secret_signature = "signature-do-not-log"
    secret_token = "token-do-not-log"
    error = (
        "GET https://storage.example/file.pdf?X-Amz-Signature="
        f"{secret_signature}#{secret_fragment} Authorization: Bearer {secret_token}"
    )

    async def fake_batch(issues, _cfg, _idx, **_kwargs):
        return [
            {
                "issue": issues[0],
                "success": False,
                "error": error,
                "failure_kind": DownloadFailureKind.TRANSIENT,
            }
        ]

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_batch)
    logger = logging.getLogger("magsync.daemon-secret-test")
    caplog.set_level(logging.INFO, logger=logger.name)
    await cli._run_daemon_cycle(
        _config(tmp_path),
        idx,
        logger=logger,
        source_client_factory=_source_factory(ScriptedSource()),
    )

    assert secret_fragment not in caplog.text
    assert secret_signature not in caplog.text
    assert secret_token not in caplog.text
    assert "https://storage.example/file.pdf" in caplog.text

    record = logging.LogRecord(
        "magsync",
        logging.ERROR,
        __file__,
        1,
        error,
        (),
        None,
    )
    assert cli._DaemonRedactionFilter().filter(record)
    assert secret_fragment not in record.getMessage()
    assert secret_signature not in record.getMessage()
    assert secret_token not in record.getMessage()

    old = {
        name: (logging.getLogger(name).level, logging.getLogger(name).propagate)
        for name in ("httpx", "httpcore")
    }
    root_filters = {
        handler: list(handler.filters) for handler in logging.getLogger().handlers
    }
    try:
        cli._configure_daemon_external_logging()
        for name in ("httpx", "httpcore"):
            external = logging.getLogger(name)
            assert external.level > logging.CRITICAL
            assert external.propagate is False
    finally:
        for name, (level, propagate) in old.items():
            external = logging.getLogger(name)
            external.setLevel(level)
            external.propagate = propagate
        for handler, filters in root_filters.items():
            handler.filters[:] = filters
    idx.close()
