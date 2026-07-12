"""Tests for the `magsync backfill-urls` command."""

from __future__ import annotations

from typer.testing import CliRunner

import magsync.cli as cli_mod
import magsync.core.index as index_mod
from magsync.cli import app
from magsync.core.index import MagazineIndex
from magsync.core.scraper import ScrapedIssue

runner = CliRunner()
LW = "https://limewire.com/d/zzzz#key"


def _point_db_at(tmp_path, monkeypatch):
    db = tmp_path / "index.db"
    monkeypatch.setattr(index_mod, "get_db_path", lambda: db)
    return db


def test_backfill_urls_populates_missing(tmp_path, monkeypatch):
    db = _point_db_at(tmp_path, monkeypatch)
    idx = MagazineIndex(db_path=db)
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(
        mag,
        [{"title": "T", "page_url": "https://freemagazines.top/t-2026/", "limewire_url": None}],
    )
    # Wanted row: default backfill-urls repairs requested rows only.
    idx.mark_manual([issue["id"] for issue in idx.get_issues()])
    idx.close()

    async def fake_scrape(page_url, client=None):
        return ScrapedIssue(title="T", page_url=page_url, limewire_url=LW)

    monkeypatch.setattr(cli_mod, "scrape_detail_page", fake_scrape)

    result = runner.invoke(app, ["backfill-urls"])
    assert result.exit_code == 0, result.output
    assert "1 repaired" in result.output

    idx2 = MagazineIndex(db_path=db)
    assert idx2.get_issues()[0]["limewire_url"] == LW
    assert idx2.get_issues_missing_url() == []
    idx2.close()


def test_backfill_urls_nothing_to_do(tmp_path, monkeypatch):
    db = _point_db_at(tmp_path, monkeypatch)
    MagazineIndex(db_path=db).close()  # empty index

    result = runner.invoke(app, ["backfill-urls"])
    assert result.exit_code == 0, result.output
    assert "No issues missing" in result.output


def test_backfill_urls_filter_scopes_to_magazine(tmp_path, monkeypatch):
    db = _point_db_at(tmp_path, monkeypatch)
    idx = MagazineIndex(db_path=db)
    a = idx.get_or_create_magazine("Alpha", "alpha")
    b = idx.get_or_create_magazine("Beta", "beta")
    idx.add_issues(a, [{"title": "A", "page_url": "pa", "limewire_url": None}])
    idx.add_issues(b, [{"title": "B", "page_url": "pb", "limewire_url": None}])
    idx.mark_manual([issue["id"] for issue in idx.get_issues()])
    idx.close()

    scraped: list[str] = []

    async def fake_scrape(page_url, client=None):
        scraped.append(page_url)
        return ScrapedIssue(title="x", page_url=page_url, limewire_url=LW)

    monkeypatch.setattr(cli_mod, "scrape_detail_page", fake_scrape)

    result = runner.invoke(app, ["backfill-urls", "alpha"])
    assert result.exit_code == 0, result.output
    assert scraped == ["pa"]  # only Alpha's missing-URL issue was re-scraped
