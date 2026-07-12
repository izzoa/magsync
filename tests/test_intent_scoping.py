"""Download provenance and intent-scoped claiming (scope-downloads-to-intent).

The v0.6.0 incident: an unscoped claim downloaded never-subscribed magazines
because indexing implied enqueueing. These tests pin the two-layer fix —
provenance recorded at write time (title-only promotion), eligibility derived
at claim time (title + since against the caller's snapshot) — plus the parked
visibility, retry exclusion, and recovery-path behavior around it.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from magsync import cli
from magsync.config import Config
from magsync.core.index import MagazineIndex
from magsync.core.models import (
    DownloadFailureKind,
    DownloadStatus,
    PipelineStatus,
    RetryAction,
    SourceResult,
    Subscription,
)

LW = "https://limewire.com/d/{}#key{}"
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)


def _index(tmp_path) -> MagazineIndex:
    return MagazineIndex(db_path=tmp_path / "index.db")


def _seed(idx, title, page, url=None, *, year=2026, month=6, subscription=None):
    mag = idx.get_or_create_magazine("Seeded", "seeded")
    idx.add_issues(
        mag,
        [{
            "title": title,
            "page_url": page,
            "limewire_url": url or LW.format(page, page),
            "year": year,
            "month": month,
        }],
        subscription=subscription,
    )
    return idx.conn.execute(
        "SELECT id FROM issues WHERE page_url = ?", (page,)
    ).fetchone()[0]


def _provenance(idx, issue_id):
    return idx.conn.execute(
        "SELECT requested_by FROM downloads WHERE issue_id = ?", (issue_id,)
    ).fetchone()[0]


def _status(idx, issue_id):
    return idx.conn.execute(
        "SELECT status FROM downloads WHERE issue_id = ?", (issue_id,)
    ).fetchone()[0]


# --- provenance writes -------------------------------------------------------


def test_indexing_without_subscription_catalogs_null(tmp_path):
    idx = _index(tmp_path)
    issue = _seed(idx, "Some Magazine - June 2026", "p1")
    assert _provenance(idx, issue) is None
    idx.close()


def test_indexing_under_subscription_records_matching_and_parks_strangers(tmp_path):
    idx = _index(tmp_path)
    sub = Subscription(query="Getaway")
    wanted = _seed(idx, "Getaway - June 2026", "pw", subscription=sub)
    stranger = _seed(
        idx, "Women's Golf Americas - Spring 2026", "ps", subscription=sub
    )
    assert _provenance(idx, wanted) == "subscription"
    assert _provenance(idx, stranger) is None
    idx.close()


def test_reencounter_promotes_null_to_subscription(tmp_path):
    idx = _index(tmp_path)
    issue = _seed(idx, "Getaway - June 2026", "p1")  # cataloged before subscribing
    assert _provenance(idx, issue) is None
    _seed(idx, "Getaway - June 2026", "p1", subscription=Subscription(query="Getaway"))
    assert _provenance(idx, issue) == "subscription"
    idx.close()


def test_promote_subscribed_is_title_only_and_idempotent(tmp_path):
    idx = _index(tmp_path)
    old = _seed(idx, "Getaway - June 2024", "pold", year=2024)
    stranger = _seed(idx, "The New Yorker - June 2026", "pny")
    manual = _seed(idx, "Getaway - July 2026", "pman", month=7)
    idx.mark_manual([manual])

    subs = [Subscription(query="Getaway", since="2026-01")]
    # Title-only: the 2024 back-issue is promoted despite the since floor.
    assert idx.promote_subscribed(subs) == 1
    assert _provenance(idx, old) == "subscription"
    assert _provenance(idx, stranger) is None
    assert _provenance(idx, manual) == "manual"
    # Idempotent: nothing left to promote.
    assert idx.promote_subscribed(subs) == 0
    idx.close()


def test_mark_manual_ladder_strengthens_and_never_demotes(tmp_path):
    idx = _index(tmp_path)
    from_null = _seed(idx, "A - June 2026", "pa")
    from_sub = _seed(idx, "B - June 2026", "pb", subscription=Subscription(query="B"))
    assert _provenance(idx, from_sub) == "subscription"

    idx.mark_manual([from_null, from_sub])
    assert _provenance(idx, from_null) == "manual"
    assert _provenance(idx, from_sub) == "manual"  # explicit request strengthens

    # Manual survives promotion passes and stays manual on repeat marking.
    idx.promote_subscribed([Subscription(query="B")])
    idx.mark_manual([from_sub])
    assert _provenance(idx, from_sub) == "manual"
    idx.close()


# --- claim scoping -----------------------------------------------------------


def test_null_pending_row_is_never_claimed(tmp_path):
    idx = _index(tmp_path)
    issue = _seed(idx, "Stranger - June 2026", "p1")
    assert idx.claim_pending_and_due_downloads([], now=NOW) == []
    assert (
        idx.claim_pending_and_due_downloads(
            [Subscription(query="Stranger")], now=NOW
        )
        == []
    )  # even a matching snapshot cannot claim an unpromoted row
    assert _status(idx, issue) == "pending"
    idx.close()


def test_unknown_provenance_fails_closed_and_logs(tmp_path, caplog):
    idx = _index(tmp_path)
    issue = _seed(idx, "Odd - June 2026", "p1")
    idx.conn.execute(
        "UPDATE downloads SET requested_by = 'gremlin' WHERE issue_id = ?", (issue,)
    )
    idx.conn.commit()
    with caplog.at_level(logging.WARNING, logger="magsync"):
        claimed = idx.claim_pending_and_due_downloads([], now=NOW)
    assert claimed == []
    assert _status(idx, issue) == "pending"
    assert any("unrecognized requested_by" in r.getMessage() for r in caplog.records)
    idx.close()


def test_unsubscribed_provenance_stops_claiming_manual_does_not(tmp_path):
    idx = _index(tmp_path)
    sub_row = _seed(
        idx, "Getaway - June 2026", "pg", subscription=Subscription(query="Getaway")
    )
    manual_row = _seed(idx, "One Off - June 2026", "pm")
    idx.mark_manual([manual_row])

    claimed = idx.claim_pending_and_due_downloads([], now=NOW)
    assert [row["id"] for row in claimed] == [manual_row]
    assert _status(idx, sub_row) == "pending"  # no matching sub in snapshot

    idx.update_download_status(manual_row, DownloadStatus.COMPLETE)
    claimed = idx.claim_pending_and_due_downloads(
        [Subscription(query="Getaway")], now=NOW
    )
    assert [row["id"] for row in claimed] == [sub_row]
    idx.close()


def test_since_is_evaluated_at_claim_time_without_row_mutation(tmp_path):
    idx = _index(tmp_path)
    issue = _seed(
        idx, "Getaway - June 2024", "p1", year=2024,
        subscription=Subscription(query="Getaway"),
    )
    tight = [Subscription(query="Getaway", since="2026-01")]
    loose = [Subscription(query="Getaway", since="2024-01")]

    assert idx.claim_pending_and_due_downloads(tight, now=NOW) == []
    row_before = dict(
        idx.conn.execute(
            "SELECT * FROM downloads WHERE issue_id = ?", (issue,)
        ).fetchone()
    )
    # Loosening the floor in config alone makes the row claimable.
    claimed = idx.claim_pending_and_due_downloads(loose, now=NOW)
    assert [row["id"] for row in claimed] == [issue]
    row_before.pop("status")
    for field, value in row_before.items():
        assert value == dict(
            idx.conn.execute(
                "SELECT * FROM downloads WHERE issue_id = ?", (issue,)
            ).fetchone()
        )[field]
    idx.close()


def test_due_refresh_on_null_row_is_never_claimed_and_action_survives(tmp_path):
    idx = _index(tmp_path)
    stranger = _seed(idx, "Stranger - June 2026", "ps")
    wanted = _seed(idx, "Mine - June 2026", "pw")
    idx.mark_manual([wanted])
    for issue in (stranger, wanted):
        idx.record_download_failure(
            issue, DownloadFailureKind.SHARE_UNAVAILABLE, "orphaned"
        )
        idx.schedule_link_refresh(issue, NOW - timedelta(seconds=1))

    claimed = idx.claim_due_link_refreshes([], now=NOW)
    assert [row["id"] for row in claimed] == [wanted]
    row = idx.conn.execute(
        "SELECT next_action, next_retry_at FROM downloads WHERE issue_id = ?",
        (stranger,),
    ).fetchone()
    assert row["next_action"] == RetryAction.REFRESH_LINK.value
    assert row["next_retry_at"] is not None  # parked action persists, unclaimable
    idx.close()


def test_rotation_cannot_resurrect_side_effect_work(tmp_path):
    idx = _index(tmp_path)
    stranger = _seed(idx, "Stranger - June 2026", "p1", url=LW.format("old", 1))
    idx.record_download_failure(
        stranger, DownloadFailureKind.SHARE_UNAVAILABLE, "dead"
    )
    assert _status(idx, stranger) == "unavailable"

    # The site rotates the stranger's link; catalog updates, status resets…
    _seed(idx, "Stranger - June 2026", "p1", url=LW.format("new", 2))
    assert _status(idx, stranger) == "pending"
    # …but no snapshot makes it claimable work, and retry ignores it too.
    assert idx.claim_pending_and_due_downloads(
        [Subscription(query="Stranger")], now=NOW
    ) == []
    claimed, skipped, excluded = idx.claim_manual_retry_downloads()
    assert claimed == [] and skipped == 0 and excluded == 0  # pending ≠ retryable
    idx.close()


# --- preview parity ----------------------------------------------------------


def test_preview_matches_claim_and_mutates_nothing(tmp_path):
    idx = _index(tmp_path)
    wanted_pending = _seed(idx, "A - June 2026", "pa")
    wanted_due = _seed(idx, "B - June 2026", "pb")
    wanted_future = _seed(idx, "C - June 2026", "pc")
    stranger = _seed(idx, "D - June 2026", "pd")
    idx.mark_manual([wanted_pending, wanted_due, wanted_future])
    idx.record_download_failure(
        wanted_due, DownloadFailureKind.TRANSIENT, "x",
        next_retry_at=NOW - timedelta(seconds=1),
    )
    idx.record_download_failure(
        wanted_future, DownloadFailureKind.TRANSIENT, "x",
        next_retry_at=NOW + timedelta(hours=1),
    )
    refresh_target = _seed(idx, "E - June 2026", "pe")
    idx.mark_manual([refresh_target])
    idx.record_download_failure(
        refresh_target, DownloadFailureKind.SHARE_UNAVAILABLE, "dead"
    )
    idx.schedule_link_refresh(refresh_target, NOW - timedelta(seconds=1))

    previewed, due_refreshes = idx.preview_claimable_downloads([], now=NOW)
    assert {row["id"] for row in previewed} == {wanted_pending, wanted_due}
    assert due_refreshes == 1
    # Nothing mutated by the preview.
    assert _status(idx, wanted_pending) == "pending"
    assert _status(idx, wanted_due) == "failed"
    assert _status(idx, stranger) == "pending"

    claimed = idx.claim_pending_and_due_downloads([], now=NOW)
    assert {row["id"] for row in claimed} == {row["id"] for row in previewed}
    idx.close()


# --- retry provenance --------------------------------------------------------


def test_retry_is_provenance_only_and_reports_exclusions(tmp_path):
    idx = _index(tmp_path)
    lapsed = _seed(
        idx, "Getaway - June 2026", "pl", subscription=Subscription(query="Getaway")
    )
    stranger = _seed(idx, "Stranger - June 2026", "ps")
    for issue in (lapsed, stranger):
        idx.record_download_failure(
            issue, DownloadFailureKind.SHARE_UNAVAILABLE, "dead"
        )

    # No subscriptions exist anymore; the lapsed subscription row is still an
    # explicit retry target (provenance-only), the stranger never is.
    claimed, skipped, excluded = idx.claim_manual_retry_downloads()
    assert [row["id"] for row in claimed] == [lapsed]
    assert skipped == 0
    assert excluded == 1
    assert _status(idx, stranger) == "unavailable"
    idx.close()


# --- parked visibility -------------------------------------------------------


def test_stats_split_actionable_pending_from_cataloged(tmp_path):
    idx = _index(tmp_path)
    wanted = [_seed(idx, f"W{n} - June 2026", f"pw{n}") for n in range(3)]
    idx.mark_manual(wanted)
    for n in range(5):
        _seed(idx, f"S{n} - June 2026", f"ps{n}")

    stats = idx.get_download_stats()
    assert stats["pending"] == 3
    assert stats["cataloged"] == 5
    idx.close()


def test_pending_refresh_count_excludes_parked_actions(tmp_path):
    idx = _index(tmp_path)
    wanted = _seed(idx, "Mine - June 2026", "pw")
    idx.mark_manual([wanted])
    stranger = _seed(idx, "Stranger - June 2026", "ps")
    for issue in (wanted, stranger):
        idx.record_download_failure(
            issue, DownloadFailureKind.SHARE_UNAVAILABLE, "dead"
        )
        idx.schedule_link_refresh(issue, NOW + timedelta(hours=1))

    assert idx.count_pending_link_refreshes() == 1
    idx.close()


# --- upgrade path ------------------------------------------------------------


def test_upgrade_parks_zombies_promotes_subscribed_preserves_schedules(tmp_path):
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
            magazine_id INTEGER NOT NULL REFERENCES issues(id),
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
        INSERT INTO magazines (id, title, normalized_title) VALUES (1, 'Mixed', 'mixed');
        INSERT INTO issues (id, magazine_id, title, page_url, limewire_url, year, month)
            VALUES
            (21, 1, 'Getaway - June 2026', 'sub-pending', 'https://limewire.com/d/a#k', 2026, 6),
            (22, 1, 'Womens Golf Americas - Spring 2026', 'zombie-pending', 'https://limewire.com/d/b#k', 2026, 4),
            (23, 1, 'The New Yorker - June 1 2026', 'zombie-failed', 'https://limewire.com/d/c#k', 2026, 6);
        INSERT INTO downloads (issue_id, status, sha256) VALUES
            (21, 'pending', NULL),
            (22, 'pending', NULL),
            (23, 'failed', 'keep-hash');
        """
    )
    conn.commit()
    conn.close()

    idx = MagazineIndex(db_path=db)
    subs = [Subscription(query="Getaway")]
    assert idx.promote_subscribed(subs) == 1

    assert _provenance(idx, 21) == "subscription"
    assert _provenance(idx, 22) is None
    assert _provenance(idx, 23) is None

    # The migration scheduled the legacy linked failure due-now; parked, that
    # action is inert while the row's data is untouched.
    row = idx.conn.execute(
        "SELECT sha256, next_action FROM downloads WHERE issue_id = 23"
    ).fetchone()
    assert row["sha256"] == "keep-hash"
    assert row["next_action"] == RetryAction.DOWNLOAD.value

    later = datetime.now(timezone.utc) + timedelta(minutes=1)
    claimed = idx.claim_pending_and_due_downloads(subs, now=later)
    assert [row["id"] for row in claimed] == [21]
    assert _status(idx, 22) == "pending"
    assert _status(idx, 23) == "failed"
    idx.close()


# --- lifecycle sequences -----------------------------------------------------


def test_manual_intent_survives_unsubscribe_through_failure_and_retry(tmp_path):
    idx = _index(tmp_path)
    sub = Subscription(query="Getaway")
    issue = _seed(idx, "Getaway - June 2026", "p1", subscription=sub)
    idx.mark_manual([issue])  # explicit fetch/TUI selection while subscribed

    # Unsubscribed: the explicit request still drives automatic retries.
    idx.record_download_failure(
        issue, DownloadFailureKind.TRANSIENT, "flaky",
        next_retry_at=NOW - timedelta(seconds=1),
    )
    claimed = idx.claim_pending_and_due_downloads([], now=NOW)
    assert [row["id"] for row in claimed] == [issue]
    idx.close()


# --- daemon cycle ------------------------------------------------------------


class _BlockedSource:
    """Source whose circuit is open before the first search (challenge)."""

    def __init__(self):
        from magsync.core.models import SourceFailure, SourceFailureKind

        self.failure = SourceFailure(
            SourceFailureKind.ACCESS_BLOCKED,
            "challenge",
            operation="search",
            status_code=403,
            host="freemagazines.top",
        )
        self.searches: list[str] = []
        self._blocked = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    @property
    def circuit_open(self):
        return self._blocked

    @property
    def circuit_failure(self):
        return self.failure if self._blocked else None

    async def search_with_details(self, query):
        self.searches.append(query)
        self._blocked = True
        return SourceResult(failure=self.failure)


@pytest.mark.asyncio
async def test_blocked_cycle_claims_only_wanted_rows(tmp_path, monkeypatch):
    """The 2026-07-12 incident, replayed: legacy zombies stay parked."""
    idx = _index(tmp_path)
    wanted = _seed(idx, "Getaway - June 2026", "pw")
    idx.mark_manual([wanted])
    stranger = _seed(idx, "Women's Golf Americas - Spring 2026", "ps")
    # Parked stranger refresh action must not surface in the summary either.
    orphan = _seed(idx, "The New Yorker - June 1, 2026", "po")
    idx.record_download_failure(
        orphan, DownloadFailureKind.SHARE_UNAVAILABLE, "orphaned"
    )
    idx.schedule_link_refresh(orphan, NOW + timedelta(hours=1))

    attempted: list[int] = []

    async def fake_download_batch(issues, cfg, index, **kwargs):
        results = []
        for issue in issues:
            attempted.append(issue["id"])
            index.update_download_status(issue["id"], DownloadStatus.COMPLETE)
            results.append({"issue": issue, "success": True, "error": None})
        return results

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_download_batch)

    cfg = Config(output_dir=str(tmp_path / "out"))
    cfg.subscriptions = [Subscription(query="Getaway")]
    report = await cli._run_daemon_cycle(
        cfg,
        idx,
        source_client_factory=lambda **_kw: _BlockedSource(),
        subscriptions=cfg.subscriptions,
    )

    assert attempted == [wanted]
    assert report.downloads_queued == 1
    assert report.pending_refreshes == 0  # parked stranger action invisible
    assert report.status is PipelineStatus.DEGRADED
    assert _status(idx, stranger) == "pending"  # untouched catalog entry
    idx.close()


class _HealthySource:
    """One healthy search returning a matching issue and a fuzzy stranger."""

    def __init__(self, items):
        self.items = items

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    circuit_open = False
    circuit_failure = None

    async def search_with_details(self, query):
        return SourceResult(items=self.items)


@pytest.mark.asyncio
async def test_healthy_cycle_catalogs_stranger_without_downloading_it(
    tmp_path, monkeypatch
):
    from magsync.core.scraper import ScrapedIssue

    idx = _index(tmp_path)
    items = [
        ScrapedIssue(
            title="Getaway - June 2026",
            page_url="https://freemagazines.top/getaway-june-2026/",
            limewire_url=LW.format("g", 1),
        ),
        ScrapedIssue(
            title="Military Aviation World War II Air Combat - Issue 2",
            page_url="https://freemagazines.top/military-aviation-2/",
            limewire_url=LW.format("m", 2),
        ),
    ]

    attempted: list[str] = []

    async def fake_download_batch(issues, cfg, index, **kwargs):
        results = []
        for issue in issues:
            attempted.append(issue["title"])
            index.update_download_status(issue["id"], DownloadStatus.COMPLETE)
            results.append({"issue": issue, "success": True, "error": None})
        return results

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_download_batch)

    cfg = Config(output_dir=str(tmp_path / "out"))
    cfg.subscriptions = [Subscription(query="Getaway")]
    report = await cli._run_daemon_cycle(
        cfg,
        idx,
        source_client_factory=lambda **_kw: _HealthySource(items),
        subscriptions=cfg.subscriptions,
    )

    assert attempted == ["Getaway - June 2026"]
    rows = {row["title"]: row for row in idx.get_issues()}
    assert rows["Getaway - June 2026"]["requested_by"] == "subscription"
    stranger = rows["Military Aviation World War II Air Combat - Issue 2"]
    assert stranger["requested_by"] is None
    assert stranger["download_status"] == "pending"  # cataloged, never claimed
    assert report.status is PipelineStatus.HEALTHY
    idx.close()


@pytest.mark.asyncio
async def test_dry_run_previews_claim_set_without_mutating(tmp_path, monkeypatch):
    idx = _index(tmp_path)
    wanted_pending = _seed(idx, "A - June 2026", "pa")
    wanted_due = _seed(idx, "B - June 2026", "pb")
    idx.mark_manual([wanted_pending, wanted_due])
    idx.record_download_failure(
        wanted_due, DownloadFailureKind.TRANSIENT, "x",
        next_retry_at=NOW - timedelta(days=1),
    )
    stranger = _seed(idx, "C - June 2026", "pc")

    async def explode(*_a, **_kw):  # dry run must never download
        raise AssertionError("download_batch called during dry run")

    monkeypatch.setattr("magsync.core.batch.download_batch", explode)

    cfg = Config(output_dir=str(tmp_path / "out"))
    report = await cli._run_daemon_cycle(
        cfg,
        idx,
        dry_run=True,
        source_client_factory=lambda **_kw: _HealthySource([]),
        subscriptions=[],
    )

    assert report.downloads_queued == 2  # pending + due failure, stranger absent
    assert _status(idx, wanted_pending) == "pending"
    assert _status(idx, wanted_due) == "failed"  # preview claimed nothing
    assert _status(idx, stranger) == "pending"
    idx.close()
