"""Tests for add_issues backfill/refresh behavior and missing-URL queries."""

from __future__ import annotations

import logging

from magsync.core.index import MagazineIndex, _plausible_limewire_url
from magsync.core.models import DownloadStatus

LW_A = "https://limewire.com/d/aaaa#k1"
LW_B = "https://limewire.com/d/bbbb#k2"


def _index(tmp_path) -> MagazineIndex:
    return MagazineIndex(db_path=tmp_path / "index.db")


def _add_one(idx: MagazineIndex, url: str | None, **extra) -> int:
    """Add/refresh the canonical test issue; returns its issue id."""
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(mag, [{"title": "T", "page_url": "p1", "limewire_url": url, **extra}])
    return idx.get_issues()[0]["id"]


def test_backfill_fills_null_limewire_url(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    added = idx.add_issues(mag, [{"title": "T", "page_url": "p1", "limewire_url": None}])
    assert added == 1

    # Re-scrape now yields a URL for the same page_url → backfill, not a new row.
    added2 = idx.add_issues(mag, [{"title": "T", "page_url": "p1", "limewire_url": LW_A}])
    assert added2 == 0

    rows = idx.get_issues()
    assert rows[0]["limewire_url"] == LW_A
    idx.close()


def test_rotated_link_refreshes_stored_url(tmp_path, caplog):
    # The site rotates share links on existing posts; a validated different
    # incoming URL replaces the stored one (was: never overwritten).
    idx = _index(tmp_path)
    _add_one(idx, LW_A)
    with caplog.at_level(logging.INFO, logger="magsync"):
        _add_one(idx, LW_B)

    rows = idx.get_issues()
    assert rows[0]["limewire_url"] == LW_B
    swap_logs = [r.getMessage() for r in caplog.records if "Refreshed LimeWire link" in r.getMessage()]
    assert len(swap_logs) == 1
    assert "aaaa" in swap_logs[0] and "bbbb" in swap_logs[0]  # sharing IDs logged
    # Fragments are decryption keys — they must never reach logs.
    assert all("k1" not in r.getMessage() and "k2" not in r.getMessage() for r in caplog.records)
    idx.close()


def test_other_columns_remain_backfill_only(tmp_path):
    # Only limewire_url gets refresh semantics; populated metadata is never overwritten.
    idx = _index(tmp_path)
    _add_one(idx, LW_A, genre="Cooking", file_size="6 MB")
    _add_one(idx, LW_B, genre="Changed", file_size="9 MB", cover_image_url="http://c/1.jpg")

    row = idx.get_issues()[0]
    assert row["limewire_url"] == LW_B          # refreshed
    assert row["genre"] == "Cooking"            # populated → preserved
    assert row["file_size"] == "6 MB"           # populated → preserved
    assert row["cover_image_url"] == "http://c/1.jpg"  # NULL → backfilled
    idx.close()


def test_identical_link_is_noop(tmp_path, caplog):
    idx = _index(tmp_path)
    _add_one(idx, LW_A)
    mag = idx.get_or_create_magazine("Mag", "mag")
    with caplog.at_level(logging.INFO, logger="magsync"):
        added = idx.add_issues(mag, [{"title": "T", "page_url": "p1", "limewire_url": LW_A}])

    assert added == 0  # not counted as new
    assert idx.get_issues()[0]["limewire_url"] == LW_A
    assert idx.get_issues()[0]["download_status"] == "pending"
    assert not any("Refreshed LimeWire link" in r.getMessage() for r in caplog.records)
    idx.close()


def test_empty_incoming_never_clears_stored_url(tmp_path):
    idx = _index(tmp_path)
    _add_one(idx, LW_A)
    _add_one(idx, None)
    _add_one(idx, "")
    assert idx.get_issues()[0]["limewire_url"] == LW_A
    idx.close()


def test_implausible_and_lookalike_candidates_rejected(tmp_path):
    idx = _index(tmp_path)
    _add_one(idx, LW_A)
    for bad in (
        "https://limewire.com/nope#k",            # not a /d/ share path
        "https://limewire.com/d/cccc",            # no fragment (no key)
        "https://notlimewire.com/d/cccc#k3",      # lookalike host
        "https://evil.com/https://limewire.com/d/cccc#k3",  # host is evil.com
    ):
        _add_one(idx, bad)
        assert idx.get_issues()[0]["limewire_url"] == LW_A, bad
    idx.close()


def test_plausible_limewire_url_guard():
    assert _plausible_limewire_url(LW_A)
    assert _plausible_limewire_url("https://www.limewire.com/d/Xy9#frag")
    assert not _plausible_limewire_url("https://notlimewire.com/d/x#k")
    assert not _plausible_limewire_url("https://limewire.com/d/#k")
    assert not _plausible_limewire_url("https://limewire.com/d/x/y#k")
    assert not _plausible_limewire_url("")


def test_url_change_resets_failed_and_unavailable_to_pending(tmp_path):
    idx = _index(tmp_path)
    for parked in (DownloadStatus.FAILED, DownloadStatus.UNAVAILABLE):
        idx.conn.execute("DELETE FROM downloads"); idx.conn.execute("DELETE FROM issues"); idx.conn.commit()
        issue_id = _add_one(idx, LW_A)
        idx.update_download_status(issue_id, parked, sha256="keepme")
        _add_one(idx, LW_B)
        row = idx.get_issues()[0]
        assert row["download_status"] == "pending", parked
        assert row["limewire_url"] == LW_B
        sha = idx.conn.execute("SELECT sha256 FROM downloads WHERE issue_id = ?", (issue_id,)).fetchone()[0]
        assert sha == "keepme"  # reset clears path/timestamp but keeps the hash
    idx.close()


def test_url_change_leaves_complete_and_downloading_untouched(tmp_path):
    idx = _index(tmp_path)
    # complete: keeps status, file path, and hash — but the URL still refreshes
    issue_id = _add_one(idx, LW_A)
    idx.update_download_status(issue_id, DownloadStatus.COMPLETE, "/x/f.pdf", 10, "hash1")
    _add_one(idx, LW_B)
    row = idx.get_issues()[0]
    assert row["download_status"] == "complete"
    assert row["file_path"] == "/x/f.pdf"
    assert row["limewire_url"] == LW_B

    # downloading: in-flight work is never interfered with
    idx.conn.execute("DELETE FROM downloads"); idx.conn.execute("DELETE FROM issues"); idx.conn.commit()
    issue_id = _add_one(idx, LW_A)
    idx.update_download_status(issue_id, DownloadStatus.DOWNLOADING)
    _add_one(idx, LW_B)
    row = idx.get_issues()[0]
    assert row["download_status"] == "downloading"
    assert row["limewire_url"] == LW_B
    idx.close()


def test_title_is_not_backfilled(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(mag, [{"title": "Original Title", "page_url": "p1", "limewire_url": None}])
    idx.add_issues(mag, [{"title": "Changed Title", "page_url": "p1", "limewire_url": LW_A}])

    rows = idx.get_issues()
    assert rows[0]["title"] == "Original Title"  # title untouched (drives derived fields)
    assert rows[0]["limewire_url"] == LW_A  # leaf field still backfilled
    idx.close()


def test_genuinely_new_page_url_is_counted(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    assert idx.add_issues(mag, [{"title": "A", "page_url": "pa", "limewire_url": LW_A}]) == 1
    assert idx.add_issues(mag, [{"title": "B", "page_url": "pb", "limewire_url": LW_B}]) == 1
    idx.close()


def test_get_issues_missing_url_and_set(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(
        mag,
        [
            {"title": "A", "page_url": "pa", "limewire_url": None},
            {"title": "B", "page_url": "pb", "limewire_url": LW_B},
        ],
    )
    missing = idx.get_issues_missing_url()
    assert [m["page_url"] for m in missing] == ["pa"]

    idx.set_limewire_url(missing[0]["id"], LW_A)
    assert idx.get_issues_missing_url() == []
    idx.close()


def test_get_issues_missing_url_filtered_by_magazine(tmp_path):
    idx = _index(tmp_path)
    a = idx.get_or_create_magazine("Alpha", "alpha")
    b = idx.get_or_create_magazine("Beta", "beta")
    idx.add_issues(a, [{"title": "A", "page_url": "pa", "limewire_url": None}])
    idx.add_issues(b, [{"title": "B", "page_url": "pb", "limewire_url": None}])

    missing = idx.get_issues_missing_url(magazine_title="alpha")
    assert [m["page_url"] for m in missing] == ["pa"]
    idx.close()


# --- reset_failed_downloads / reset_stuck_downloads / get_issues_by_ids ---


def _add_with_status(idx, mag, key, url, status=None):
    """Add one issue; optionally move its download to `status`. Returns issue id."""
    idx.add_issues(mag, [{"title": key, "page_url": key, "limewire_url": url}])
    issue_id = idx.conn.execute(
        "SELECT id FROM issues WHERE page_url = ?", (key,)
    ).fetchone()[0]
    if status is not None:
        idx.update_download_status(issue_id, status)
    return issue_id


def _status_of(idx, issue_id):
    return idx.conn.execute(
        "SELECT status FROM downloads WHERE issue_id = ?", (issue_id,)
    ).fetchone()[0]


def test_reset_failed_returns_flipped_ids_and_skipped_count(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    failed = _add_with_status(idx, mag, "pf", LW_A, DownloadStatus.FAILED)
    unavailable = _add_with_status(idx, mag, "pu", LW_B, DownloadStatus.UNAVAILABLE)
    linkless = _add_with_status(idx, mag, "pn", None, DownloadStatus.FAILED)
    backlog = _add_with_status(idx, mag, "pp", LW_A)  # pending, never attempted

    reset_ids, skipped = idx.reset_failed_downloads()

    assert sorted(reset_ids) == sorted([failed, unavailable])
    assert skipped == 1
    assert _status_of(idx, failed) == "pending"
    assert _status_of(idx, unavailable) == "pending"
    assert _status_of(idx, linkless) == "failed"      # preserved, not stranded
    assert backlog not in reset_ids                    # backlog untouched
    idx.close()


def test_reset_failed_scoped_to_magazine(tmp_path):
    idx = _index(tmp_path)
    a = idx.get_or_create_magazine("Alpha", "alpha")
    b = idx.get_or_create_magazine("Beta", "beta")
    in_scope = _add_with_status(idx, a, "pa", LW_A, DownloadStatus.FAILED)
    out_of_scope = _add_with_status(idx, b, "pb", LW_B, DownloadStatus.FAILED)

    reset_ids, skipped = idx.reset_failed_downloads(magazine_title="alpha")

    assert reset_ids == [in_scope]
    assert skipped == 0
    assert _status_of(idx, out_of_scope) == "failed"
    idx.close()


def test_reset_failed_leaves_other_statuses_alone(tmp_path):
    # The UPDATE re-asserts the status guard: rows that are complete (or any
    # non-failed status) are neither reset nor returned.
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    done = _add_with_status(idx, mag, "pc", LW_A, DownloadStatus.COMPLETE)

    reset_ids, skipped = idx.reset_failed_downloads()

    assert reset_ids == [] and skipped == 0
    assert _status_of(idx, done) == "complete"
    idx.close()


def test_reset_stuck_downloads_keeps_linkless_failures(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    linkless_failed = _add_with_status(idx, mag, "pn", None, DownloadStatus.FAILED)
    linked_failed = _add_with_status(idx, mag, "pf", LW_A, DownloadStatus.FAILED)
    stuck = _add_with_status(idx, mag, "pd", LW_B, DownloadStatus.DOWNLOADING)

    count = idx.reset_stuck_downloads()

    assert count == 2
    assert _status_of(idx, linkless_failed) == "failed"
    assert _status_of(idx, linked_failed) == "pending"
    assert _status_of(idx, stuck) == "pending"
    idx.close()


def test_get_issues_by_ids_round_trips_large_list(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(
        mag,
        [{"title": f"I{n:04d}", "page_url": f"p{n}", "limewire_url": LW_A} for n in range(600)],
    )
    ids = [r[0] for r in idx.conn.execute("SELECT id FROM issues").fetchall()]
    assert len(ids) > 500  # forces chunking

    rows = idx.get_issues_by_ids(ids)

    assert len(rows) == len(ids)
    assert sorted(r["id"] for r in rows) == sorted(ids)
    sample = rows[0]
    for key in ("magazine_title", "normalized_title", "download_status", "file_path", "limewire_url"):
        assert key in sample
    idx.close()
