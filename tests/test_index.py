"""Tests for add_issues backfill/refresh behavior and missing-URL queries."""

from __future__ import annotations

import logging
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from magsync.core.index import MagazineIndex, _plausible_limewire_url
from magsync.core.models import (
    DownloadFailureKind,
    DownloadStatus,
    PipelineStatus,
    RefreshOutcome,
    RefreshOutcomeKind,
    RetryAction,
)

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
        idx.conn.execute("DELETE FROM downloads")
        idx.conn.execute("DELETE FROM issues")
        idx.conn.commit()
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
    idx.conn.execute("DELETE FROM downloads")
    idx.conn.execute("DELETE FROM issues")
    idx.conn.commit()
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


def test_reset_stuck_downloads_recovers_only_downloading(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    linkless_failed = _add_with_status(idx, mag, "pn", None, DownloadStatus.FAILED)
    linked_failed = _add_with_status(idx, mag, "pf", LW_A, DownloadStatus.FAILED)
    stuck = _add_with_status(idx, mag, "pd", LW_B, DownloadStatus.DOWNLOADING)

    count = idx.reset_stuck_downloads()

    assert count == 1
    assert _status_of(idx, linkless_failed) == "failed"
    assert _status_of(idx, linked_failed) == "failed"
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


# --- 'unsupported' terminal-status semantics ---

def test_resets_leave_unsupported_untouched(tmp_path):
    # Both reset paths must skip 'unsupported' even when a link exists —
    # a non-PDF payload is terminal until the share link rotates.
    idx = _index(tmp_path)
    issue_id = _add_one(idx, LW_A)
    idx.update_download_status(issue_id, DownloadStatus.UNSUPPORTED)

    assert idx.reset_stuck_downloads() == 0
    reset_ids, skipped = idx.reset_failed_downloads()
    assert reset_ids == [] and skipped == 0

    status = idx.conn.execute(
        "SELECT status FROM downloads WHERE issue_id = ?", (issue_id,)
    ).fetchone()[0]
    assert status == "unsupported"
    idx.close()


def test_link_rotation_requeues_unsupported(tmp_path):
    # A rotated blob may carry a different payload type → one cheap re-probe.
    idx = _index(tmp_path)
    issue_id = _add_one(idx, LW_A)
    idx.update_download_status(issue_id, DownloadStatus.UNSUPPORTED)

    _add_one(idx, LW_B)  # same page_url, different validated link → rotation

    row = idx.conn.execute(
        "SELECT status, file_path FROM downloads WHERE issue_id = ?", (issue_id,)
    ).fetchone()
    assert row["status"] == "pending"
    assert row["file_path"] is None
    idx.close()


def test_download_stats_count_unsupported(tmp_path):
    idx = _index(tmp_path)
    issue_id = _add_one(idx, LW_A)
    idx.update_download_status(issue_id, DownloadStatus.UNSUPPORTED)

    stats = idx.get_download_stats()
    assert stats["unsupported"] == 1
    assert stats["failed"] == 0
    idx.close()


# --- typed retry metadata / atomic work claims / pipeline state ---


def _download_row(idx: MagazineIndex, issue_id: int):
    return idx.conn.execute(
        "SELECT * FROM downloads WHERE issue_id = ?", (issue_id,)
    ).fetchone()


def test_additive_migration_preserves_data_and_schedules_legacy_linked_failure(tmp_path):
    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE magazines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            normalized_title TEXT NOT NULL UNIQUE,
            first_seen TEXT NOT NULL DEFAULT (datetime('now')),
            last_updated TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            magazine_id INTEGER NOT NULL REFERENCES magazines(id),
            title TEXT NOT NULL,
            page_url TEXT NOT NULL UNIQUE,
            limewire_url TEXT,
            year INTEGER, month INTEGER, date_raw TEXT, genre TEXT,
            file_size TEXT, cover_image_url TEXT,
            discovered_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_id INTEGER NOT NULL UNIQUE REFERENCES issues(id),
            status TEXT NOT NULL DEFAULT 'pending',
            file_path TEXT, downloaded_at TEXT, file_size_bytes INTEGER,
            sha256 TEXT
        );
        INSERT INTO magazines (id, title, normalized_title)
            VALUES (7, 'Legacy', 'legacy');
        INSERT INTO issues
            (id, magazine_id, title, page_url, limewire_url, year, month, date_raw)
            VALUES
            (11, 7, 'Done', 'done', 'https://limewire.com/d/done#key', 2025, 1, 'Jan 2025'),
            (12, 7, 'Linked failure', 'linked', 'https://limewire.com/d/dead#key', 2025, 2, 'Feb 2025'),
            (13, 7, 'Linkless failure', 'linkless', NULL, 2025, 3, 'Mar 2025');
        INSERT INTO downloads
            (issue_id, status, file_path, downloaded_at, file_size_bytes, sha256)
            VALUES
            (11, 'complete', '/library/done.pdf', '2025-01-02T03:04:05+00:00', 1234, 'abc'),
            (12, 'failed', NULL, NULL, NULL, NULL),
            (13, 'failed', NULL, NULL, NULL, NULL);
        """
    )
    conn.commit()
    conn.close()

    idx = MagazineIndex(db_path=db)
    columns = {row[1] for row in idx.conn.execute("PRAGMA table_info(downloads)")}
    assert {
        "last_error_kind", "last_error", "attempt_count", "last_attempt_at",
        "next_action", "next_retry_at",
    } <= columns

    done = _download_row(idx, 11)
    assert (done["status"], done["file_path"], done["downloaded_at"]) == (
        "complete", "/library/done.pdf", "2025-01-02T03:04:05+00:00",
    )
    assert (done["file_size_bytes"], done["sha256"]) == (1234, "abc")

    linked = _download_row(idx, 12)
    linkless = _download_row(idx, 13)
    assert linked["last_error_kind"] is None
    assert linked["next_action"] == RetryAction.DOWNLOAD.value
    assert linked["next_retry_at"] is not None
    assert linkless["next_action"] is None

    claimed = idx.claim_pending_and_due_downloads(
        now=datetime.now(timezone.utc) + timedelta(minutes=1)
    )
    assert [row["id"] for row in claimed] == [12]
    assert _status_of(idx, 12) == "downloading"
    assert _status_of(idx, 13) == "failed"
    assert idx.get_pipeline_state()["consecutive_source_failure_cycles"] == 0
    idx.close()


def test_typed_failure_persistence_is_policy_derived_and_success_clears_it(tmp_path):
    idx = _index(tmp_path)
    issue_id = _add_one(idx, LW_A)
    due = datetime(2026, 7, 11, 16, 0, tzinfo=timezone.utc)
    secret = "fragment-secret"

    idx.record_download_failure(
        issue_id,
        DownloadFailureKind.TRANSIENT,
        f"GET https://limewire.com/d/aaaa?X-Amz-Credential=cred#{secret} token=abc",
        physical_attempts=3,
        attempted_at=due - timedelta(minutes=1),
        next_retry_at=due,
    )
    row = _download_row(idx, issue_id)
    assert row["status"] == "failed"
    assert row["last_error_kind"] == DownloadFailureKind.TRANSIENT.value
    assert secret not in row["last_error"] and "X-Amz-Credential" not in row["last_error"]
    assert row["attempt_count"] == 3
    assert row["next_action"] == RetryAction.DOWNLOAD.value
    assert row["next_retry_at"] == due.isoformat()

    # A non-automatic kind ignores a supplied future time rather than being
    # accidentally scheduled by caller display/error details.
    idx.record_download_failure(
        issue_id,
        DownloadFailureKind.METADATA_INVALID,
        "missing field",
        next_retry_at=due + timedelta(hours=1),
    )
    row = _download_row(idx, issue_id)
    assert row["status"] == "failed"
    assert row["next_action"] is None and row["next_retry_at"] is None

    idx.update_download_status(
        issue_id, DownloadStatus.COMPLETE, "/library/file.pdf", 42, "hash"
    )
    row = _download_row(idx, issue_id)
    assert row["status"] == "complete" and row["file_path"] == "/library/file.pdf"
    assert row["attempt_count"] == 0
    for field in (
        "last_error_kind", "last_error", "last_attempt_at", "next_action", "next_retry_at"
    ):
        assert row[field] is None
    idx.close()


def test_claim_selects_pending_and_due_transient_but_not_future_or_parked(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    now = datetime(2026, 7, 11, 17, 0, tzinfo=timezone.utc)
    pending = _add_with_status(idx, mag, "pending", LW_A)
    due = _add_with_status(idx, mag, "due", LW_B)
    future = _add_with_status(idx, mag, "future", LW_A)
    parked = _add_with_status(idx, mag, "parked", LW_B)
    unavailable = _add_with_status(idx, mag, "unavailable", LW_A)
    unsupported = _add_with_status(idx, mag, "unsupported", LW_B)

    idx.record_download_failure(
        due, DownloadFailureKind.TRANSIENT, "temporary",
        next_retry_at=now - timedelta(seconds=1),
    )
    idx.record_download_failure(
        future, DownloadFailureKind.TRANSIENT, "temporary",
        next_retry_at=now + timedelta(seconds=1),
    )
    idx.record_download_failure(
        parked, DownloadFailureKind.DECRYPTION_FAILED, "deterministic",
        next_retry_at=now - timedelta(seconds=1),
    )
    idx.record_download_failure(
        unavailable, DownloadFailureKind.SHARE_UNAVAILABLE, "removed"
    )
    idx.record_download_failure(
        unsupported, DownloadFailureKind.UNSUPPORTED, "zip"
    )

    claimed = idx.claim_pending_and_due_downloads(now=now)
    assert {row["id"] for row in claimed} == {pending, due}
    assert all(row["download_status"] == "downloading" for row in claimed)
    assert _status_of(idx, future) == "failed"
    assert _status_of(idx, parked) == "failed"
    assert _status_of(idx, unavailable) == "unavailable"
    assert _status_of(idx, unsupported) == "unsupported"
    idx.close()


def test_claim_race_has_one_winner(tmp_path):
    db = tmp_path / "index.db"
    setup = MagazineIndex(db_path=db)
    issue_id = _add_one(setup, LW_A)
    setup.close()
    barrier = threading.Barrier(2)

    def claim():
        idx = MagazineIndex(db_path=db)
        try:
            barrier.wait()
            return [row["id"] for row in idx.claim_pending_and_due_downloads()]
        finally:
            idx.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: claim(), range(2)))

    assert sorted(results, key=len) == [[], [issue_id]]


def test_source_only_refresh_claim_reschedule_and_rotation(tmp_path):
    db = tmp_path / "index.db"
    idx = MagazineIndex(db_path=db)
    issue_id = _add_one(idx, LW_A)
    now = datetime(2026, 7, 11, 18, 0, tzinfo=timezone.utc)
    idx.record_download_failure(
        issue_id, DownloadFailureKind.SHARE_UNAVAILABLE, "orphaned"
    )
    assert idx.schedule_link_refresh(issue_id, now - timedelta(seconds=1))

    # Download selection never turns the known-dead URL into work.
    assert idx.claim_pending_and_due_downloads(now=now) == []
    refreshes = idx.claim_due_link_refreshes(now=now)
    assert [row["id"] for row in refreshes] == [issue_id]
    row = _download_row(idx, issue_id)
    assert row["status"] == "unavailable"
    assert row["next_action"] is None and row["next_retry_at"] is None

    future = now + timedelta(hours=1)
    blocked = RefreshOutcome(kind=RefreshOutcomeKind.SOURCE_BLOCKED)
    assert idx.resolve_link_refresh(issue_id, blocked, retry_at=future)
    assert idx.count_pending_link_refreshes() == 1
    idx.close()

    # A restart before the due time preserves source-only timing.
    idx = MagazineIndex(db_path=db)
    assert idx.reset_stuck_downloads() == 0
    assert idx.claim_due_link_refreshes(now=now) == []
    assert idx.count_pending_link_refreshes() == 1
    row = _download_row(idx, issue_id)
    assert row["next_action"] == RetryAction.REFRESH_LINK.value
    assert row["next_retry_at"] == future.isoformat()

    refreshes = idx.claim_due_link_refreshes(now=future + timedelta(seconds=1))
    assert [row["id"] for row in refreshes] == [issue_id]
    assert idx.count_pending_link_refreshes() == 0
    rotated = RefreshOutcome(kind=RefreshOutcomeKind.ROTATED, url=LW_B)
    assert idx.resolve_link_refresh(issue_id, rotated)
    row = _download_row(idx, issue_id)
    assert row["status"] == "pending"
    assert row["last_error_kind"] is None and row["attempt_count"] == 0
    assert row["next_action"] is None and row["next_retry_at"] is None
    assert idx.get_issues_by_ids([issue_id])[0]["limewire_url"] == LW_B
    idx.close()


def test_manual_retry_claim_bypasses_schedules_and_preserves_scope(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    future = datetime(2026, 7, 12, tzinfo=timezone.utc)
    failed = _add_with_status(idx, mag, "failed", LW_A)
    unavailable = _add_with_status(idx, mag, "unavailable", LW_B)
    linkless = _add_with_status(idx, mag, "linkless", None)
    unsupported = _add_with_status(idx, mag, "unsupported", LW_A)
    backlog = _add_with_status(idx, mag, "backlog", LW_B)

    idx.record_download_failure(
        failed, DownloadFailureKind.TRANSIENT, "later", physical_attempts=2,
        next_retry_at=future,
    )
    idx.record_download_failure(
        unavailable, DownloadFailureKind.SHARE_UNAVAILABLE, "dead"
    )
    idx.schedule_link_refresh(unavailable, future)
    idx.record_download_failure(
        linkless, DownloadFailureKind.INTERNAL, "no link"
    )
    idx.record_download_failure(
        unsupported, DownloadFailureKind.UNSUPPORTED, "zip"
    )

    claimed, skipped = idx.claim_manual_retry_downloads()
    assert {row["id"] for row in claimed} == {failed, unavailable}
    assert skipped == 1
    for issue_id in (failed, unavailable):
        row = _download_row(idx, issue_id)
        assert row["status"] == "downloading"
        assert row["attempt_count"] == 0
        assert row["last_error_kind"] is None
        assert row["next_action"] is None and row["next_retry_at"] is None
    assert _status_of(idx, linkless) == "failed"
    assert _status_of(idx, unsupported) == "unsupported"
    assert _status_of(idx, backlog) == "pending"
    idx.close()


def test_link_rotation_clears_typed_metadata_but_preserves_hash(tmp_path):
    idx = _index(tmp_path)
    issue_id = _add_one(idx, LW_A)
    due = datetime(2026, 7, 12, tzinfo=timezone.utc)
    idx.conn.execute(
        "UPDATE downloads SET sha256 = 'keep-hash' WHERE issue_id = ?", (issue_id,)
    )
    idx.conn.commit()
    idx.record_download_failure(
        issue_id, DownloadFailureKind.TRANSIENT, "old link",
        physical_attempts=4, next_retry_at=due,
    )

    assert idx.rotate_limewire_url(issue_id, LW_B)
    row = _download_row(idx, issue_id)
    assert row["status"] == "pending" and row["sha256"] == "keep-hash"
    assert row["attempt_count"] == 0
    for field in (
        "last_error_kind", "last_error", "last_attempt_at", "next_action", "next_retry_at"
    ):
        assert row[field] is None
    idx.close()


def test_pipeline_state_survives_restart_and_recovers_after_valid_empty(tmp_path):
    db = tmp_path / "index.db"
    idx = MagazineIndex(db_path=db)
    first = datetime(2026, 7, 11, 20, 0, tzinfo=timezone.utc)
    secret = "do-not-store"
    idx.update_pipeline_state(
        PipelineStatus.DEGRADED,
        cycle_at=first,
        source_validated=False,
        degraded_reason=f"blocked https://freemagazines.top/?token={secret}#{secret}",
    )
    idx.update_pipeline_state(
        PipelineStatus.DEGRADED,
        cycle_at=first + timedelta(hours=1),
        source_validated=False,
        degraded_reason="blocked again",
    )
    idx.close()

    idx = MagazineIndex(db_path=db)
    state = idx.get_pipeline_state()
    assert state["last_cycle_status"] == PipelineStatus.DEGRADED.value
    assert state["consecutive_source_failure_cycles"] == 2
    assert secret not in (state["degraded_reason"] or "")

    recovered = first + timedelta(hours=2)
    state = idx.update_pipeline_state(
        PipelineStatus.HEALTHY,
        cycle_at=recovered,
        source_validated=True,  # includes a validated explicit-empty result
        source_check_at=recovered,
    )
    assert state["last_cycle_status"] == PipelineStatus.HEALTHY.value
    assert state["consecutive_source_failure_cycles"] == 0
    assert state["last_successful_source_check_at"] == recovered.isoformat()
    assert state["degraded_reason"] is None
    idx.close()
