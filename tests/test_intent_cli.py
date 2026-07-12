"""CLI surfaces of download provenance: fetch marking, retry exclusion
reporting, backfill-urls scoping, and cataloged rendering in search."""

from __future__ import annotations

from typer.testing import CliRunner

import magsync.cli as cli
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadFailureKind,
    SourceResult,
)
from magsync.core.scraper import ScrapedIssue

runner = CliRunner()
LW = "https://limewire.com/d/{}#key"


class _StubClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    circuit_open = False
    circuit_failure = None


def _configure_paths(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MAGSYNC_DB_PATH", str(tmp_path / "index.db"))
    monkeypatch.setenv("MAGSYNC_NO_PROGRESS", "1")
    monkeypatch.setattr(cli, "FreemagazinesClient", lambda **_kw: _StubClient())


def _install_search(monkeypatch, items) -> None:
    async def fake_search(query, client=None):
        return SourceResult(items=items)

    monkeypatch.setattr(cli, "search_with_details_result", fake_search)


def _install_batch(monkeypatch, attempted: list[int], success: bool = True):
    async def fake_download_batch(issues, cfg, idx, on_start=None,
                                  on_complete=None, **_kw):
        results = []
        for issue in issues:
            attempted.append(issue["id"])
            if on_complete:
                on_complete(issue, success, None)
            results.append({"issue": issue, "success": success, "error": None})
        return results

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_download_batch)


def _provenance(db_path, issue_id):
    idx = MagazineIndex(db_path=db_path)
    try:
        return idx.conn.execute(
            "SELECT requested_by FROM downloads WHERE issue_id = ?", (issue_id,)
        ).fetchone()[0]
    finally:
        idx.close()


def test_retry_reports_never_requested_exclusions(tmp_path, monkeypatch):
    _configure_paths(tmp_path, monkeypatch)
    idx = MagazineIndex(tmp_path / "index.db")
    mag = idx.get_or_create_magazine("Stranger", "stranger")
    idx.add_issues(mag, [{"title": "Stranger - June 2026", "page_url": "p1",
                          "limewire_url": LW.format("s"), "year": 2026, "month": 6}])
    issue_id = idx.get_issues()[0]["id"]
    idx.record_download_failure(
        issue_id, DownloadFailureKind.SHARE_UNAVAILABLE, "dead"
    )
    idx.close()

    attempted: list[int] = []
    _install_batch(monkeypatch, attempted)

    result = runner.invoke(cli.app, ["retry"])
    assert result.exit_code == 0, result.output
    assert attempted == []
    assert "excluded: never requested" in result.output
    assert "magsync fetch" in result.output  # recovery path is named


def test_fetch_promotes_scope_to_manual_and_hints_retry(tmp_path, monkeypatch):
    _configure_paths(tmp_path, monkeypatch)
    db = tmp_path / "index.db"
    idx = MagazineIndex(db)
    mag = idx.get_or_create_magazine("Alpha", "alpha")
    idx.add_issues(mag, [
        {"title": "Alpha - May 2026", "page_url": "p-old",
         "limewire_url": LW.format("a"), "year": 2026, "month": 5},
    ])
    parked_failed = idx.get_issues()[0]["id"]
    idx.record_download_failure(
        parked_failed, DownloadFailureKind.SHARE_UNAVAILABLE, "dead"
    )
    idx.close()

    _install_search(monkeypatch, [
        ScrapedIssue(
            title="Alpha - June 2026",
            page_url="https://freemagazines.top/alpha-june-2026/",
            limewire_url=LW.format("b"),
        ),
    ])
    attempted: list[int] = []
    _install_batch(monkeypatch, attempted)

    result = runner.invoke(cli.app, ["fetch", "Alpha"])
    assert result.exit_code == 0, result.output

    # The parked failure was promoted to manual and the recovery hint printed.
    assert _provenance(db, parked_failed) == "manual"
    assert "marked as requested" in result.output
    assert "magsync retry" in result.output
    # The download pass stayed pending-only: the failed row was not attempted.
    assert parked_failed not in attempted
    assert len(attempted) == 1  # the newly indexed pending issue

    # And the promised recovery path works end-to-end: retry now takes it.
    attempted.clear()
    result = runner.invoke(cli.app, ["retry"])
    assert result.exit_code == 0, result.output
    assert attempted == [parked_failed]


def test_backfill_urls_default_skips_parked_and_all_includes(tmp_path, monkeypatch):
    _configure_paths(tmp_path, monkeypatch)
    db = tmp_path / "index.db"
    idx = MagazineIndex(db)
    mag = idx.get_or_create_magazine("Stranger", "stranger")
    idx.add_issues(mag, [{
        "title": "Stranger - June 2026",
        "page_url": "https://freemagazines.top/stranger-june-2026/",
        "limewire_url": None, "year": 2026, "month": 6,
    }])
    idx.close()

    async def fake_scrape(page_url, client=None):
        return ScrapedIssue(
            title="Stranger - June 2026", page_url=page_url,
            limewire_url=LW.format("z"),
        )

    monkeypatch.setattr(cli, "scrape_detail_page", fake_scrape)

    result = runner.invoke(cli.app, ["backfill-urls"])
    assert result.exit_code == 0, result.output
    assert "never-requested" in result.output
    assert "use --all" in result.output

    idx = MagazineIndex(db)
    assert idx.get_issues()[0]["limewire_url"] is None  # untouched by default
    idx.close()

    result = runner.invoke(cli.app, ["backfill-urls", "--all"])
    assert result.exit_code == 0, result.output
    idx = MagazineIndex(db)
    assert idx.get_issues()[0]["limewire_url"] == LW.format("z")
    idx.close()


def test_search_renders_parked_rows_as_cataloged(tmp_path, monkeypatch):
    _configure_paths(tmp_path, monkeypatch)
    _install_search(monkeypatch, [
        ScrapedIssue(
            title="Alpha - June 2026",
            page_url="https://freemagazines.top/alpha-june-2026/",
            limewire_url=LW.format("a"),
        ),
    ])

    result = runner.invoke(cli.app, ["search", "Alpha"])
    assert result.exit_code == 0, result.output
    # Indexed via plain search (no subscription): a catalog entry, not work.
    assert "cataloged" in result.output
    assert "pending" not in result.output
