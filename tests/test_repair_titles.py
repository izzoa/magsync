"""Tests for `magsync repair-titles` (legacy source format tags)."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

import magsync.cli as cli_mod
import magsync.core.index as index_mod
from magsync.config import Config, Subscription
from magsync.cli import app
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus

runner = CliRunner()
LW = "https://limewire.com/d/zzzz#key"
TAGGED = "[PDF] Airliner World - January 2024"


def _setup(tmp_path, monkeypatch, *, downloaded: bool = True, manual: bool = True):
    db = tmp_path / "index.db"
    out = tmp_path / "magazines"
    monkeypatch.setattr(index_mod, "get_db_path", lambda: db)
    monkeypatch.setattr(
        cli_mod, "load_config", lambda: Config(output_dir=str(out))
    )

    idx = MagazineIndex(db_path=db)
    mag = idx.get_or_create_magazine("[PDF] Airliner World", "[pdf] airliner world")
    idx.add_issues(
        mag,
        [{"title": TAGGED, "page_url": "https://freemagazines.top/aw-jan-2024/",
          "limewire_url": LW, "year": 2024, "month": 1}],
    )
    issue_id = idx.get_issues()[0]["id"]
    if manual:
        idx.mark_manual([issue_id])

    old_file = None
    if downloaded:
        old_file = out / "[PDF] Airliner World" / "[PDF] Airliner World - 2024-01.pdf"
        old_file.parent.mkdir(parents=True)
        old_file.write_bytes(b"%PDF-1.6 payload")
        idx.update_download_status(
            issue_id, DownloadStatus.COMPLETE, file_path=str(old_file)
        )
    idx.close()
    return db, out, issue_id, old_file


def test_repair_fixes_title_dates_magazine_and_relocates_file(tmp_path, monkeypatch):
    db, out, issue_id, old_file = _setup(tmp_path, monkeypatch)

    result = runner.invoke(app, ["repair-titles"])
    assert result.exit_code == 0, result.output

    idx = MagazineIndex(db_path=db)
    row = idx.get_issues()[0]
    assert row["title"] == "Airliner World - January 2024"
    assert (row["year"], row["month"]) == (2024, 1)
    assert row["magazine_title"] == "Airliner World"

    new_file = Path(row["file_path"])
    assert new_file.exists()
    assert not old_file.exists()
    assert new_file.parent.name == "Airliner World"
    assert "[PDF]" not in new_file.name
    # Dedup resolves a hash to the recorded path, so it must name a real file.
    assert Path(idx.find_by_hash(row["sha256"]) or new_file).exists()
    idx.close()


def test_tagged_stored_title_is_claimable_by_an_exact_subscription(
    tmp_path, monkeypatch
):
    # Regression test for the actual bug. Claim eligibility compares the
    # *stored* title, so while the tag counted as part of the name an `exact`
    # subscription could own a row and still never claim it — its whole legacy
    # back catalogue was silently skipped. Normalizing the tag away fixes this
    # on its own; the repair command only tidies names already on disk.
    db, _out, _issue_id, _old = _setup(
        tmp_path, monkeypatch, downloaded=False, manual=False
    )
    exact_sub = Subscription(query="Airliner World", exact=True)

    idx = MagazineIndex(db_path=db)
    mag = idx.get_or_create_magazine("[PDF] Airliner World", "[pdf] airliner world")
    idx.add_issues(
        mag,
        [{"title": "Airliner World - January 2024",
          "page_url": "https://freemagazines.top/aw-jan-2024/",
          "limewire_url": LW}],
        subscription=exact_sub,
    )
    row = idx.get_issues()[0]
    assert row["requested_by"] == "subscription"
    assert row["title"] == TAGGED  # a title is never backfilled by indexing

    claimed = idx.claim_pending_and_due_downloads([exact_sub])
    assert [c["limewire_url"] for c in claimed] == [LW]
    idx.close()


def test_dry_run_changes_nothing(tmp_path, monkeypatch):
    db, _out, _issue_id, old_file = _setup(tmp_path, monkeypatch)

    result = runner.invoke(app, ["repair-titles", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "dry run" in result.output.lower()

    idx = MagazineIndex(db_path=db)
    row = idx.get_issues()[0]
    assert row["title"] == TAGGED          # untouched
    assert row["file_path"] == str(old_file)
    idx.close()
    assert old_file.exists()


def test_existing_destination_is_never_overwritten(tmp_path, monkeypatch):
    db, out, _issue_id, old_file = _setup(tmp_path, monkeypatch)
    # Pre-occupy the corrected path with different content.
    target = out / "Airliner World" / "Airliner World - 2024-01 - January.pdf"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"%PDF-1.6 different")

    result = runner.invoke(app, ["repair-titles"])
    assert result.exit_code == 0, result.output
    assert "collision" in result.output.lower()

    # Both files survive, and neither is clobbered.
    assert old_file.read_bytes() == b"%PDF-1.6 payload"
    assert target.read_bytes() == b"%PDF-1.6 different"


def test_repair_is_idempotent(tmp_path, monkeypatch):
    db, _out, _issue_id, _old = _setup(tmp_path, monkeypatch)
    assert runner.invoke(app, ["repair-titles"]).exit_code == 0

    second = runner.invoke(app, ["repair-titles"])
    assert second.exit_code == 0, second.output
    assert "No titles need repair" in second.output


def test_empty_tagged_magazine_records_are_pruned(tmp_path, monkeypatch):
    db, _out, _issue_id, _old = _setup(tmp_path, monkeypatch, downloaded=False)
    idx = MagazineIndex(db_path=db)
    # An orphaned tagged record with no issues, as accumulated historically.
    idx.get_or_create_magazine("[PDF] Orphaned Title", "[pdf] orphaned title")
    idx.close()

    assert runner.invoke(app, ["repair-titles"]).exit_code == 0

    idx = MagazineIndex(db_path=db)
    titles = [m["title"] for m in idx.get_tracked_magazines()]
    assert not [t for t in titles if t.startswith("[PDF]")]
    assert "Airliner World" in titles
    idx.close()


def test_untagged_library_is_left_alone(tmp_path, monkeypatch):
    db = tmp_path / "index.db"
    monkeypatch.setattr(index_mod, "get_db_path", lambda: db)
    monkeypatch.setattr(
        cli_mod, "load_config", lambda: Config(output_dir=str(tmp_path / "m"))
    )
    idx = MagazineIndex(db_path=db)
    mag = idx.get_or_create_magazine("Airliner World", "airliner world")
    idx.add_issues(mag, [{"title": "Airliner World - August 2026",
                          "page_url": "p9", "limewire_url": LW}])
    idx.close()

    result = runner.invoke(app, ["repair-titles"])
    assert "No titles need repair" in result.output
