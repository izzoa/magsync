"""CLI coverage for structured source failures and partial results."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from typer.testing import CliRunner

import magsync.cli as cli
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadFailureKind,
    RetryAction,
    SourceError,
    SourceFailure,
    SourceFailureKind,
    SourceResult,
)
from magsync.core.scraper import ScrapedIssue


runner = CliRunner()
LIMEWIRE_URL = "https://limewire.com/d/Example#safe-test-key"


class ScriptedSource:
    def __init__(self, *results: SourceResult):
        self.results = list(results)
        self._circuit_failure: SourceFailure | None = None
        self.searches: list[str] = []

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

    async def search_with_details(self, query: str, **_kwargs) -> SourceResult:
        self.searches.append(query)
        result = self.results.pop(0)
        if (
            result.failure is not None
            and result.failure.kind is SourceFailureKind.ACCESS_BLOCKED
        ):
            self._circuit_failure = result.failure
        return result


def _configure_paths(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MAGSYNC_DB_PATH", str(tmp_path / "index.db"))
    monkeypatch.setenv("MAGSYNC_NO_PROGRESS", "1")


def _install_source(monkeypatch, source: ScriptedSource) -> None:
    monkeypatch.setattr(cli, "FreemagazinesClient", lambda **_kwargs: source)


def _failure(kind: SourceFailureKind) -> SourceResult:
    return SourceResult(
        failure=SourceFailure(
            kind,
            "failure at https://freemagazines.top/?token=DO_NOT_PRINT#SECRET_FRAGMENT",
            operation="search",
            status_code=403 if kind is SourceFailureKind.ACCESS_BLOCKED else 200,
            host="freemagazines.top",
            cf_ray="safe-ray",
        )
    )


@pytest.mark.parametrize(
    ("kind", "expected"),
    (
        (SourceFailureKind.ACCESS_BLOCKED, "blocked"),
        (SourceFailureKind.PROTOCOL, "could not be validated"),
        (SourceFailureKind.TRANSIENT, "temporarily unavailable"),
    ),
)
def test_search_source_failures_exit_nonzero_without_empty_success(
    tmp_path, monkeypatch, kind, expected
):
    _configure_paths(tmp_path, monkeypatch)
    _install_source(monkeypatch, ScriptedSource(_failure(kind)))

    result = runner.invoke(cli.app, ["search", "Example"])

    assert result.exit_code == 1
    assert expected in result.output.casefold()
    assert "no results" not in result.output.casefold()
    assert "DO_NOT_PRINT" not in result.output
    assert "SECRET_FRAGMENT" not in result.output


def test_search_validated_empty_remains_successful(tmp_path, monkeypatch):
    _configure_paths(tmp_path, monkeypatch)
    _install_source(
        monkeypatch,
        ScriptedSource(SourceResult(items=[], validated_empty=True)),
    )

    result = runner.invoke(cli.app, ["search", "Absent"])

    assert result.exit_code == 0
    assert "No results found" in result.output


def test_search_partial_details_indexes_results_but_exits_nonzero(
    tmp_path, monkeypatch
):
    _configure_paths(tmp_path, monkeypatch)
    partial = SourceResult(
        items=[
            ScrapedIssue(
                title="Example - July 2026",
                page_url="https://freemagazines.top/example-july-2026/",
                limewire_url=LIMEWIRE_URL,
            )
        ],
        failures=[
            SourceFailure(
                SourceFailureKind.PROTOCOL,
                "One detail page was invalid",
                operation="detail",
            )
        ],
    )
    _install_source(monkeypatch, ScriptedSource(partial))

    result = runner.invoke(cli.app, ["search", "Example"])

    assert result.exit_code == 1
    assert "1 detail page(s) were omitted" in result.output
    assert "Example - July 2026" in result.output
    idx = MagazineIndex(tmp_path / "index.db")
    try:
        assert len(idx.get_issues()) == 1
    finally:
        idx.close()


def test_fetch_blocked_exits_nonzero_without_downloading(tmp_path, monkeypatch):
    _configure_paths(tmp_path, monkeypatch)
    _install_source(
        monkeypatch,
        ScriptedSource(_failure(SourceFailureKind.ACCESS_BLOCKED)),
    )

    result = runner.invoke(cli.app, ["fetch", "Example"])

    assert result.exit_code == 1
    assert "blocked" in result.output.casefold()
    assert "downloading" not in result.output.casefold()


def test_update_blocked_is_incomplete_and_never_up_to_date(tmp_path, monkeypatch):
    _configure_paths(tmp_path, monkeypatch)
    idx = MagazineIndex(tmp_path / "index.db")
    idx.get_or_create_magazine("Example", "example")
    idx.close()
    _install_source(
        monkeypatch,
        ScriptedSource(_failure(SourceFailureKind.ACCESS_BLOCKED)),
    )

    result = runner.invoke(cli.app, ["update"])

    assert result.exit_code == 1
    assert "Update incomplete" in result.output
    assert "up to date" not in result.output


def test_backfill_reports_blocked_then_skipped_and_exits_nonzero(
    tmp_path, monkeypatch
):
    _configure_paths(tmp_path, monkeypatch)
    idx = MagazineIndex(tmp_path / "index.db")
    magazine_id = idx.get_or_create_magazine("Example", "example")
    idx.add_issues(
        magazine_id,
        [
            {
                "title": "Example One",
                "page_url": "https://freemagazines.top/example-one-2026/",
                "limewire_url": None,
            },
            {
                "title": "Example Two",
                "page_url": "https://freemagazines.top/example-two-2026/",
                "limewire_url": None,
            },
        ],
    )
    idx.close()
    source = ScriptedSource()
    _install_source(monkeypatch, source)
    blocked = SourceFailure(
        SourceFailureKind.ACCESS_BLOCKED,
        "Source access is blocked",
        operation="detail",
        status_code=403,
        host="freemagazines.top",
    )

    async def blocked_detail(_page_url, *, client):
        client._circuit_failure = blocked
        raise SourceError(
            blocked.kind,
            blocked.message,
            operation=blocked.operation,
            status_code=blocked.status_code,
            host=blocked.host,
        )

    monkeypatch.setattr(cli, "scrape_detail_page", blocked_detail)

    result = runner.invoke(cli.app, ["backfill-urls", "--no-progress"])

    assert result.exit_code == 1
    assert "1 blocked" in result.output
    assert "1 skipped" in result.output
    assert "Backfill incomplete" in result.output


def test_retry_claim_bypasses_future_schedule(tmp_path, monkeypatch):
    _configure_paths(tmp_path, monkeypatch)
    idx = MagazineIndex(tmp_path / "index.db")
    magazine_id = idx.get_or_create_magazine("Example", "example")
    idx.add_issues(
        magazine_id,
        [
            {
                "title": "Example - July 2026",
                "page_url": "https://freemagazines.top/example-july-2026/",
                "limewire_url": LIMEWIRE_URL,
            }
        ],
    )
    issue_id = idx.get_issues()[0]["id"]
    idx.record_download_failure(
        issue_id,
        DownloadFailureKind.TRANSIENT,
        "try later",
        next_action=RetryAction.DOWNLOAD,
        next_retry_at=datetime.now(timezone.utc) + timedelta(days=1),
    )
    idx.close()

    captured: list[dict] = []

    async def fake_batch(issues, _cfg, _idx, *args, **kwargs):
        captured.extend(issues)
        return [
            {
                "issue": issue,
                "success": False,
                "error": "still transient",
                "failure_kind": DownloadFailureKind.TRANSIENT,
            }
            for issue in issues
        ]

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_batch)

    result = runner.invoke(cli.app, ["retry", "Example", "--no-progress"])

    assert result.exit_code == 0, result.output
    assert len(captured) == 1
    assert captured[0]["download_status"] == "downloading"
    assert captured[0]["next_action"] is None
    assert captured[0]["next_retry_at"] is None
