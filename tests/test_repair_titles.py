"""Tests for `magsync repair-titles` (legacy tags and site suffixes)."""

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


SUFFIXED = "GQ USA Fall 2026 | Magazine PDF"


def test_site_suffixed_title_is_repaired_and_its_file_relocated(tmp_path, monkeypatch):
    # Issues indexed while the scraper missed the source's current og:title
    # suffix: on a title with no other separator the suffix became part of the
    # magazine name, so its file landed in a folder of its own.
    db = tmp_path / "index.db"
    out = tmp_path / "magazines"
    monkeypatch.setattr(index_mod, "get_db_path", lambda: db)
    monkeypatch.setattr(cli_mod, "load_config", lambda: Config(output_dir=str(out)))
    idx = MagazineIndex(db_path=db)
    mag = idx.get_or_create_magazine(SUFFIXED, SUFFIXED.lower())
    idx.add_issues(
        mag,
        [{"title": SUFFIXED, "page_url": "https://freemagazines.top/gq-usa-fall-2026/",
          "limewire_url": LW, "year": 2026}],
    )
    issue_id = idx.get_issues()[0]["id"]
    idx.mark_manual([issue_id])
    old_file = out / "GQ USA Fall 2026 Magazine PDF" / "GQ USA Fall 2026 Magazine PDF.pdf"
    old_file.parent.mkdir(parents=True)
    old_file.write_bytes(b"%PDF-1.6 payload")
    idx.update_download_status(issue_id, DownloadStatus.COMPLETE, file_path=str(old_file))
    idx.close()

    result = runner.invoke(app, ["repair-titles"])
    assert result.exit_code == 0, result.output

    idx = MagazineIndex(db_path=db)
    row = idx.get_issues()[0]
    assert row["title"] == "GQ USA Fall 2026"
    assert row["magazine_title"] == "GQ USA Fall"
    new_file = Path(row["file_path"])
    assert new_file.exists() and not old_file.exists()
    assert new_file.parent.name == "GQ USA Fall"
    assert "Magazine PDF" not in new_file.name
    titles = [m["title"] for m in idx.get_tracked_magazines()]
    assert SUFFIXED not in titles  # the emptied suffixed record is pruned
    idx.close()


SHARED = "FlyPast – July 2026 | Magazine [PDF]"


def _shared_file_library(tmp_path, titles):
    """Several rows pointing at one file, as content deduplication leaves them."""
    from magsync.core.index import MagazineIndex as Index

    idx = Index(db_path=tmp_path / "index.db")
    out = tmp_path / "magazines"
    shared = out / "FlyPast" / "FlyPast - 2026-07 - July 2026 Magazine [PDF].pdf"
    shared.parent.mkdir(parents=True)
    shared.write_bytes(b"%PDF-1.6 payload")
    ids = []
    for n, title in enumerate(titles):
        mag = idx.get_or_create_magazine(title, title.lower())
        idx.add_issues(mag, [{"title": title, "page_url": f"https://freemagazines.top/flypast-{n}/",
                              "limewire_url": LW, "year": 2026, "month": 7}])
        issue_id = max(r["id"] for r in idx.get_issues())
        idx.update_download_status(issue_id, DownloadStatus.COMPLETE, file_path=str(shared))
        ids.append(issue_id)
    return idx, out, shared, ids


def _paths(idx, ids):
    rows = {r["id"]: r for r in idx.get_issues()}
    return [Path(rows[i]["file_path"]) for i in ids]


def test_every_row_sharing_a_moved_file_follows_it(tmp_path):
    from magsync.core.repair import repair_titles

    idx, out, shared, ids = _shared_file_library(tmp_path, [SHARED, SHARED])
    report = repair_titles(idx, out)

    assert (report.moved, report.missing) == (1, 0)
    first, second = _paths(idx, ids)
    assert first == second and first.exists() and not shared.exists()
    assert "[PDF]" not in first.name
    assert [line[0] for line in report.lines] == ["moved", "shared"]
    idx.close()


def test_a_clean_row_sharing_the_file_is_repointed_too(tmp_path):
    # Only the decorated row is a candidate; the clean row must not be left
    # naming the file's old location.
    from magsync.core.repair import repair_titles

    idx, out, shared, ids = _shared_file_library(tmp_path, ["FlyPast – July 2026", SHARED])
    repair_titles(idx, out)

    clean, decorated = _paths(idx, ids)
    assert clean == decorated and clean.exists() and not shared.exists()
    idx.close()


def test_dry_run_counts_a_shared_file_once(tmp_path):
    from magsync.core.repair import repair_titles

    idx, out, shared, ids = _shared_file_library(tmp_path, [SHARED, SHARED])
    report = repair_titles(idx, out, dry_run=True)

    assert (report.moved, report.missing) == (1, 0)
    assert shared.exists() and _paths(idx, ids) == [shared, shared]
    idx.close()


def test_every_decorated_suffix_spelling_is_pruned_when_emptied(tmp_path):
    from magsync.core.repair import repair_titles

    idx = MagazineIndex(db_path=tmp_path / "index.db")
    for title in ("Edge | Magazine [PDF]", "Total Film | Magazine (PDF)", "BBC Science Focus | PDF",
                  "GQ USA Fall 2026 - Magazine PDF", "[PDF] Orphan", "Wired [UK]", "Mac|Life"):
        idx.get_or_create_magazine(title, title.lower())
    report = repair_titles(idx, tmp_path / "out")

    assert sorted(report.pruned) == sorted([
        "Edge | Magazine [PDF]", "Total Film | Magazine (PDF)", "BBC Science Focus | PDF",
        "GQ USA Fall 2026 - Magazine PDF", "[PDF] Orphan"])
    titles = {m["title"] for m in idx.get_tracked_magazines()}
    assert {"Wired [UK]", "Mac|Life"} <= titles  # undecorated empty records stay
    idx.close()


def test_every_suffix_spelling_is_a_repair_candidate(tmp_path):
    # The candidate query once prefiltered on "|...PDF" and silently skipped
    # spellings the stripper already handled.
    from magsync.core.repair import repair_titles

    idx = MagazineIndex(db_path=tmp_path / "index.db")
    titles = ["Astronomy - July 2026 - Magazine PDF", "Edge 2026 - | Free Download",
              "Sky at Night - March 2026 - Free Download", "Wired - May 2026 – PDF"]
    for n, title in enumerate(titles):
        mag = idx.get_or_create_magazine(title, title.lower())
        idx.add_issues(mag, [{"title": title, "page_url": f"https://freemagazines.top/s{n}/",
                              "limewire_url": LW}])
    report = repair_titles(idx, tmp_path / "out")

    assert report.candidates == len(titles)
    assert sorted(r["title"] for r in idx.get_issues()) == sorted([
        "Astronomy - July 2026", "Edge 2026", "Sky at Night - March 2026", "Wired - May 2026"])
    idx.close()
