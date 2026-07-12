"""SQLite magazine index for magsync."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from magsync.config import get_db_path
from magsync.core.matching import eligible_for_any, title_match
from magsync.core.models import DownloadStatus, RequestedBy
from magsync.core.urls import (
    is_valid_limewire_share_url,
    normalize_limewire_share_url,
)

logger = logging.getLogger("magsync")

# The only provenance values that make a row wanted. Anything else non-null is
# unrecognized (corruption/external writes) and MUST fail closed at claim time.
_WANTED_PROVENANCE = (RequestedBy.MANUAL.value, RequestedBy.SUBSCRIPTION.value)


def _enum_value(value: Any) -> Any:
    """Return an enum's persisted value while accepting plain strings."""
    return getattr(value, "value", value)


def _utc_timestamp(value: datetime | str | None = None) -> str:
    """Normalize a timestamp for UTC persistence and SQLite comparison."""
    if value is None:
        dt = datetime.now(timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        dt = datetime.fromisoformat(text)
    else:
        dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _plausible_limewire_url(url: str) -> bool:
    """Compatibility wrapper around the shared strict validator."""
    return is_valid_limewire_share_url(url)


def _sharing_id(url: str) -> str:
    """Best-effort sharing-ID extraction for log lines (never the fragment)."""
    path = urlparse(url).path
    return path.rstrip("/").rsplit("/", 1)[-1] or "?"


# Keep IN-clause parameter counts under SQLite's host-parameter limit
# (999 on older builds).
_SQL_IN_CHUNK = 500

SCHEMA = """
CREATE TABLE IF NOT EXISTS magazines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL UNIQUE,
    first_seen TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    magazine_id INTEGER NOT NULL REFERENCES magazines(id),
    title TEXT NOT NULL,
    page_url TEXT NOT NULL UNIQUE,
    limewire_url TEXT,
    year INTEGER,
    month INTEGER,
    date_raw TEXT,
    genre TEXT,
    file_size TEXT,
    cover_image_url TEXT,
    discovered_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS downloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id INTEGER NOT NULL UNIQUE REFERENCES issues(id),
    status TEXT NOT NULL DEFAULT 'pending',
    file_path TEXT,
    downloaded_at TEXT,
    file_size_bytes INTEGER,
    sha256 TEXT,
    last_error_kind TEXT,
    last_error TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    next_action TEXT,
    next_retry_at TEXT,
    requested_by TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_cycle_at TEXT,
    last_cycle_status TEXT,
    last_successful_source_check_at TEXT,
    consecutive_source_failure_cycles INTEGER NOT NULL DEFAULT 0,
    degraded_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_issues_magazine ON issues(magazine_id);
CREATE INDEX IF NOT EXISTS idx_issues_year_month ON issues(year, month);
CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
"""


class MagazineIndex:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or get_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self):
        """Apply schema migrations for existing databases."""
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(downloads)")}
        additions = {
            "sha256": "TEXT",
            "last_error_kind": "TEXT",
            "last_error": "TEXT",
            "attempt_count": "INTEGER NOT NULL DEFAULT 0",
            "last_attempt_at": "TEXT",
            "next_action": "TEXT",
            "next_retry_at": "TEXT",
            # Provenance ladder NULL < 'subscription' < 'manual'; NULL means
            # side-effect catalog entry, which is never claimable work.
            "requested_by": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                self.conn.execute(
                    f"ALTER TABLE downloads ADD COLUMN {name} {declaration}"
                )

        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS pipeline_state (
                   id INTEGER PRIMARY KEY CHECK (id = 1),
                   last_cycle_at TEXT,
                   last_cycle_status TEXT,
                   last_successful_source_check_at TEXT,
                   consecutive_source_failure_cycles INTEGER NOT NULL DEFAULT 0,
                   degraded_reason TEXT
               )"""
        )
        self.conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_downloads_due_action
               ON downloads(next_action, next_retry_at, status)"""
        )
        self.conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_downloads_requested_by
               ON downloads(requested_by, status)"""
        )
        self.conn.execute(
            """INSERT OR IGNORE INTO pipeline_state
               (id, consecutive_source_failure_cycles) VALUES (1, 0)"""
        )

        # Existing linked failures predate typed policy. Schedule one bounded
        # attempt to classify them; do not guess that they are transient, and
        # leave link-less rows parked for URL backfill.
        self.conn.execute(
            """UPDATE downloads
               SET next_action = 'DOWNLOAD', next_retry_at = ?
               WHERE status = 'failed'
                 AND last_error_kind IS NULL
                 AND next_action IS NULL
                 AND issue_id IN (
                     SELECT id FROM issues
                     WHERE limewire_url IS NOT NULL AND limewire_url != ''
                 )""",
            (_utc_timestamp(),),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()

    def get_or_create_magazine(self, title: str, normalized_title: str) -> int:
        """Get or create a magazine record. Returns the magazine ID."""
        row = self.conn.execute(
            "SELECT id FROM magazines WHERE normalized_title = ?",
            (normalized_title,),
        ).fetchone()
        if row:
            self.conn.execute(
                "UPDATE magazines SET last_updated = datetime('now') WHERE id = ?",
                (row["id"],),
            )
            self.conn.commit()
            return row["id"]

        cursor = self.conn.execute(
            "INSERT INTO magazines (title, normalized_title) VALUES (?, ?)",
            (title, normalized_title),
        )
        self.conn.commit()
        return cursor.lastrowid

    def add_issues(
        self, magazine_id: int, issues: list[dict], *, subscription: Any = None
    ) -> int:
        """Upsert issues for a magazine. Returns count of *new* issues added.

        For a ``page_url`` that already exists, backfill any leaf metadata column
        that is currently NULL/empty from the incoming scrape. ``limewire_url``
        is additionally *refreshed*: the site rotates share links on existing
        posts after takedowns, so a validated incoming URL that differs from the
        stored one replaces it, and a ``failed``/``unavailable`` download is
        reset to ``pending`` so the fresh link gets retried. Other populated
        columns are never overwritten, and existing rows are not counted as new.
        ``title`` is intentionally excluded — it drives derived
        year/month/date_raw and magazine association and cannot be repaired in
        isolation.

        When ``subscription`` is given (indexing under a subscription search),
        rows whose issue title matches it — title only, no ``since`` — record
        ``requested_by='subscription'``: new rows at insert, existing
        null-provenance rows by promotion on re-encounter. Non-matching
        (stranger) results stay null-provenance catalog entries, which no
        automatic claim ever downloads.
        """
        added = 0
        backfill_columns = ("genre", "file_size", "cover_image_url")
        for issue in issues:
            sub_wants = subscription is not None and title_match(
                issue.get("title") or "", subscription
            )
            existing = self.conn.execute(
                "SELECT id, limewire_url, genre, file_size, cover_image_url "
                "FROM issues WHERE page_url = ?",
                (issue["page_url"],),
            ).fetchone()
            if existing:
                updates = {
                    col: issue.get(col)
                    for col in backfill_columns
                    if issue.get(col) and not existing[col]
                }
                incoming_raw = issue.get("limewire_url")
                incoming_url = (
                    normalize_limewire_share_url(incoming_raw)
                    if _plausible_limewire_url(incoming_raw)
                    else None
                )
                url_changed = bool(
                    incoming_url
                    and incoming_url != existing["limewire_url"]
                    and _plausible_limewire_url(incoming_url)
                )
                if incoming_url and not existing["limewire_url"]:
                    updates["limewire_url"] = incoming_url
                if url_changed:
                    updates["limewire_url"] = incoming_url
                if updates:
                    set_clause = ", ".join(f"{c} = ?" for c in updates)
                    self.conn.execute(
                        f"UPDATE issues SET {set_clause} WHERE id = ?",
                        (*updates.values(), existing["id"]),
                    )
                if url_changed:
                    # A different validated URL is a new attempt identity.
                    # Parked rows get one fresh probe; complete/in-flight rows
                    # retain their lifecycle data. sha256 is deliberately kept
                    # for content de-duplication in every case.
                    self.conn.execute(
                        """UPDATE downloads
                           SET status = CASE
                                   WHEN status IN ('failed', 'unavailable', 'unsupported')
                                       THEN 'pending'
                                   ELSE status
                               END,
                               file_path = CASE
                                   WHEN status IN ('failed', 'unavailable', 'unsupported')
                                       THEN NULL
                                   ELSE file_path
                               END,
                               downloaded_at = CASE
                                   WHEN status IN ('failed', 'unavailable', 'unsupported')
                                       THEN NULL
                                   ELSE downloaded_at
                               END,
                               file_size_bytes = CASE
                                   WHEN status IN ('failed', 'unavailable', 'unsupported')
                                       THEN NULL
                                   ELSE file_size_bytes
                               END,
                               last_error_kind = NULL,
                               last_error = NULL,
                               attempt_count = 0,
                               last_attempt_at = NULL,
                               next_action = NULL,
                               next_retry_at = NULL
                           WHERE issue_id = ?""",
                        (existing["id"],),
                    )
                    logger.info(
                        f"Refreshed LimeWire link for {issue['page_url']}: "
                        f"{_sharing_id(existing['limewire_url'] or '')} → {_sharing_id(incoming_url)}"
                    )
                if sub_wants:
                    # Promotion on re-encounter: heals rows cataloged before
                    # this subscription existed. Guarded to NULL so manual (or
                    # any recorded) intent is never overwritten here.
                    self.conn.execute(
                        """UPDATE downloads SET requested_by = 'subscription'
                           WHERE issue_id = ? AND requested_by IS NULL""",
                        (existing["id"],),
                    )
                continue

            incoming_raw = issue.get("limewire_url")
            incoming_url = (
                normalize_limewire_share_url(incoming_raw)
                if _plausible_limewire_url(incoming_raw)
                else None
            )
            cursor = self.conn.execute(
                """INSERT INTO issues
                   (magazine_id, title, page_url, limewire_url, year, month,
                    date_raw, genre, file_size, cover_image_url)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    magazine_id,
                    issue.get("title", ""),
                    issue["page_url"],
                    incoming_url,
                    issue.get("year"),
                    issue.get("month"),
                    issue.get("date_raw", ""),
                    issue.get("genre"),
                    issue.get("file_size"),
                    issue.get("cover_image_url"),
                ),
            )
            # Create initial download record. Provenance is recorded only when
            # the triggering subscription actually wants this title; stranger
            # results are cataloged (requested_by NULL), not enqueued.
            self.conn.execute(
                "INSERT INTO downloads (issue_id, status, requested_by) VALUES (?, ?, ?)",
                (
                    cursor.lastrowid,
                    DownloadStatus.PENDING.value,
                    RequestedBy.SUBSCRIPTION.value if sub_wants else None,
                ),
            )
            added += 1

        self.conn.commit()
        return added

    def update_download_status(
        self,
        issue_id: int,
        status: DownloadStatus,
        file_path: str | None = None,
        file_size_bytes: int | None = None,
        sha256: str | None = None,
    ):
        """Update a lifecycle status while preserving the legacy API.

        Successful completion also clears any stale typed failure/schedule
        metadata. Typed failures should use :meth:`record_download_failure` so
        their policy metadata is written atomically with the lifecycle status.
        """
        downloaded_at = _utc_timestamp() if status == DownloadStatus.COMPLETE else None
        if status == DownloadStatus.COMPLETE:
            self.conn.execute(
                """UPDATE downloads
                   SET status = ?, file_path = ?, downloaded_at = ?,
                       file_size_bytes = ?, sha256 = ?,
                       last_error_kind = NULL, last_error = NULL,
                       attempt_count = 0, last_attempt_at = NULL,
                       next_action = NULL, next_retry_at = NULL
                   WHERE issue_id = ?""",
                (
                    status.value,
                    file_path,
                    downloaded_at,
                    file_size_bytes,
                    sha256,
                    issue_id,
                ),
            )
        else:
            self.conn.execute(
                """UPDATE downloads
                   SET status = ?, file_path = ?, downloaded_at = ?,
                       file_size_bytes = ?, sha256 = ?
                   WHERE issue_id = ?""",
                (status.value, file_path, downloaded_at, file_size_bytes, sha256, issue_id),
            )
        self.conn.commit()

    def record_download_failure(
        self,
        issue_id: int,
        failure_kind: Any,
        error: str | BaseException | None,
        *,
        physical_attempts: int = 1,
        attempted_at: datetime | str | None = None,
        next_retry_at: datetime | str | None = None,
        next_action: Any | None = None,
    ) -> None:
        """Atomically persist a typed failure and its policy-derived schedule.

        ``next_retry_at`` is intentionally supplied by the retry owner: the
        index persists timing but does not invent backoff. A transient failure
        receives ``DOWNLOAD`` only when a due time is supplied. An unavailable
        share may receive ``REFRESH_LINK`` only after its immediate source
        refresh was blocked/transient. Other kinds stay parked by policy.
        """
        from magsync.core.diagnostics import sanitize_external_error
        from magsync.core.models import RetryAction
        from magsync.core.policy import get_download_failure_policy

        kind_value = _enum_value(failure_kind)
        policy = get_download_failure_policy(failure_kind)
        requested_action = _enum_value(next_action) if next_action is not None else None

        action: str | None = None
        retry_at: str | None = None
        if next_retry_at is not None:
            if policy.automatic_retry and requested_action in (
                None,
                RetryAction.DOWNLOAD.value,
            ):
                action = RetryAction.DOWNLOAD.value
                retry_at = _utc_timestamp(next_retry_at)
            elif policy.refresh_link and requested_action == RetryAction.REFRESH_LINK.value:
                action = RetryAction.REFRESH_LINK.value
                retry_at = _utc_timestamp(next_retry_at)

        sanitized = sanitize_external_error(error) if error is not None else None
        attempts = max(int(physical_attempts), 0)
        self.conn.execute(
            """UPDATE downloads
               SET status = ?, file_path = NULL, downloaded_at = NULL,
                   file_size_bytes = NULL,
                   last_error_kind = ?, last_error = ?,
                   attempt_count = attempt_count + ?, last_attempt_at = ?,
                   next_action = ?, next_retry_at = ?
               WHERE issue_id = ?""",
            (
                _enum_value(policy.final_status),
                kind_value,
                sanitized,
                attempts,
                _utc_timestamp(attempted_at),
                action,
                retry_at,
                issue_id,
            ),
        )
        self.conn.commit()

    def record_download_result(
        self,
        issue_id: int,
        result: Any,
        *,
        physical_attempts: int | None = None,
        attempted_at: datetime | str | None = None,
        next_retry_at: datetime | str | None = None,
        next_action: Any | None = None,
    ) -> None:
        """Persist a ``DownloadResult`` without routing on display text."""
        if result.success:
            path = str(result.file_path) if result.file_path is not None else None
            self.update_download_status(
                issue_id,
                DownloadStatus.COMPLETE,
                path,
                result.file_size_bytes,
                result.sha256,
            )
            return
        if result.failure_kind is None:
            raise ValueError("failed DownloadResult requires failure_kind")
        self.record_download_failure(
            issue_id,
            result.failure_kind,
            result.error,
            physical_attempts=(
                physical_attempts
                if physical_attempts is not None
                else getattr(result, "attempt_count", 1)
            ),
            attempted_at=attempted_at,
            next_retry_at=next_retry_at,
            next_action=next_action,
        )

    def find_by_hash(self, sha256: str) -> str | None:
        """Find an existing download with the same SHA-256 hash. Returns file_path or None."""
        row = self.conn.execute(
            "SELECT file_path FROM downloads WHERE sha256 = ? AND status = 'complete' LIMIT 1",
            (sha256,),
        ).fetchone()
        return row["file_path"] if row else None

    def get_issues(
        self,
        magazine_title: str | None = None,
        since_year: int | None = None,
        since_month: int | None = None,
        status: DownloadStatus | None = None,
    ) -> list[dict]:
        """Query issues with optional filters."""
        query = """
            SELECT i.*, d.status as download_status, d.file_path,
                   d.file_size_bytes AS downloaded_file_size_bytes, d.sha256,
                   d.last_error_kind, d.last_error, d.attempt_count,
                   d.last_attempt_at, d.next_action, d.next_retry_at,
                   d.requested_by,
                   m.title as magazine_title, m.normalized_title
            FROM issues i
            JOIN magazines m ON i.magazine_id = m.id
            LEFT JOIN downloads d ON d.issue_id = i.id
            WHERE 1=1
        """
        params: list = []

        if magazine_title:
            query += " AND m.normalized_title LIKE ?"
            params.append(f"%{magazine_title}%")

        if since_year:
            if since_month:
                query += " AND (i.year > ? OR (i.year = ? AND i.month IS NOT NULL AND i.month >= ?))"
                params.extend([since_year, since_year, since_month])
            else:
                query += " AND i.year >= ?"
                params.append(since_year)

        if status:
            query += " AND d.status = ?"
            params.append(status.value)

        query += " ORDER BY i.year DESC, i.month DESC, i.title"

        rows = self.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_issues_missing_url(
        self,
        magazine_title: str | None = None,
        *,
        wanted_only: bool = False,
    ) -> list[dict]:
        """Return issues whose limewire_url is NULL/empty, optionally filtered by magazine.

        With ``wanted_only``, restrict to rows someone actually requested
        (``manual``/``subscription`` provenance) — URL repair for
        never-requested catalog entries is wasted source traffic.
        """
        query = """
            SELECT i.id, i.page_url, i.title,
                   m.title as magazine_title, m.normalized_title
            FROM issues i
            JOIN magazines m ON i.magazine_id = m.id
            LEFT JOIN downloads d ON d.issue_id = i.id
            WHERE (i.limewire_url IS NULL OR i.limewire_url = '')
        """
        params: list = []
        if wanted_only:
            query += " AND d.requested_by IN ('manual', 'subscription')"
        if magazine_title:
            query += " AND m.normalized_title LIKE ?"
            params.append(f"%{magazine_title}%")
        query += " ORDER BY m.title, i.title"
        rows = self.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def set_limewire_url(self, issue_id: int, limewire_url: str):
        """Store a validated URL and apply new-link attempt semantics."""
        if not _plausible_limewire_url(limewire_url):
            raise ValueError("invalid LimeWire share URL")
        self.rotate_limewire_url(issue_id, normalize_limewire_share_url(limewire_url))

    def get_tracked_magazines(self) -> list[dict]:
        """Get all tracked magazines with issue counts."""
        rows = self.conn.execute(
            """SELECT m.*, COUNT(i.id) as issue_count,
                      SUM(CASE WHEN d.status = 'complete' THEN 1 ELSE 0 END) as downloaded_count
               FROM magazines m
               LEFT JOIN issues i ON i.magazine_id = m.id
               LEFT JOIN downloads d ON d.issue_id = i.id
               GROUP BY m.id
               ORDER BY m.title"""
        ).fetchall()
        return [dict(row) for row in rows]

    def promote_subscribed(self, subscriptions: list[Any]) -> int:
        """Idempotently promote null-provenance rows matching a subscription.

        Title-only by design: eligibility applies each subscription's ``since``
        floor at claim time; promotion must not, or loosening a floor later
        could never revive rows left NULL under the tighter one. Safe to run
        every startup and every cycle — repeat runs change nothing.
        """
        if not subscriptions:
            return 0
        rows = self.conn.execute(
            """SELECT d.issue_id, i.title
               FROM downloads d
               JOIN issues i ON i.id = d.issue_id
               WHERE d.requested_by IS NULL"""
        ).fetchall()
        promote_ids = [
            row["issue_id"]
            for row in rows
            if any(title_match(row["title"] or "", sub) for sub in subscriptions)
        ]
        if not promote_ids:
            return 0
        self.conn.executemany(
            """UPDATE downloads SET requested_by = 'subscription'
               WHERE issue_id = ? AND requested_by IS NULL""",
            [(issue_id,) for issue_id in promote_ids],
        )
        self.conn.commit()
        return len(promote_ids)

    def mark_manual(self, issue_ids: list[int]) -> int:
        """Record explicit user intent: promote rows to ``manual``.

        Strengthens ``subscription`` provenance too — an explicit request must
        survive a later unsubscribe. Top of the ladder; never demoted.
        """
        if not issue_ids:
            return 0
        marked = 0
        for start in range(0, len(issue_ids), _SQL_IN_CHUNK):
            chunk = issue_ids[start : start + _SQL_IN_CHUNK]
            placeholders = ",".join("?" * len(chunk))
            cursor = self.conn.execute(
                f"""UPDATE downloads SET requested_by = 'manual'
                    WHERE issue_id IN ({placeholders})
                      AND (requested_by IS NULL OR requested_by != 'manual')""",
                chunk,
            )
            marked += cursor.rowcount
        self.conn.commit()
        return marked

    def _eligible_download_candidates(
        self, subscriptions: list[Any], now_text: str
    ) -> list[sqlite3.Row]:
        """Shared candidate selection for the download claim and its preview.

        SQL narrows to wanted provenance and policy-eligible status/schedule;
        Python applies claim-time subscription eligibility (title honoring
        ``exact``, plus ``since``) for ``subscription`` rows. ``manual`` rows
        are eligible unconditionally. Unknown non-null provenance fails closed
        and is logged.
        """
        candidates = self.conn.execute(
            """SELECT d.issue_id, d.status, d.last_error_kind, d.requested_by,
                      d.next_action, d.next_retry_at, i.limewire_url,
                      i.title, i.year, i.month
               FROM downloads d
               JOIN issues i ON i.id = d.issue_id
               WHERE i.limewire_url IS NOT NULL
                 AND i.limewire_url != ''
                 AND d.requested_by IS NOT NULL
                 AND (
                     (d.status = 'pending' AND d.next_action IS NULL)
                     OR
                     (d.status = 'failed'
                      AND d.next_action = 'DOWNLOAD'
                      AND d.next_retry_at IS NOT NULL
                      AND datetime(d.next_retry_at) <= datetime(?)
                      AND (d.last_error_kind = 'transient'
                           OR d.last_error_kind IS NULL))
                 )
               ORDER BY CASE WHEN d.status = 'pending' THEN 0 ELSE 1 END,
                        datetime(d.next_retry_at), d.issue_id""",
            (now_text,),
        ).fetchall()

        eligible: list[sqlite3.Row] = []
        unknown = 0
        for row in candidates:
            provenance = row["requested_by"]
            if provenance not in _WANTED_PROVENANCE:
                unknown += 1
                continue
            if provenance == RequestedBy.SUBSCRIPTION.value and not eligible_for_any(
                {"title": row["title"], "year": row["year"], "month": row["month"]},
                subscriptions,
            ):
                continue
            if not _plausible_limewire_url(row["limewire_url"]):
                continue
            eligible.append(row)
        if unknown:
            logger.warning(
                "Ignoring %d download row(s) with unrecognized requested_by "
                "values; they fail closed and are never claimed",
                unknown,
            )
        return eligible

    def claim_pending_and_due_downloads(
        self,
        subscriptions: list[Any],
        *,
        now: datetime | str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Atomically claim wanted pending and due policy-eligible downloads.

        ``subscriptions`` is required (fail-closed): only ``manual`` rows and
        ``subscription`` rows matching the caller's snapshot (title honoring
        ``exact``, plus ``since``, evaluated now) are claimable. Null and
        unrecognized provenance are never work. A due legacy row (null kind
        plus the one migration-created DOWNLOAD action) is the only exception
        to typed transient selection. Claimed rows move to ``downloading`` and
        their consumed schedule is cleared.
        """
        now_text = _utc_timestamp(now)
        max_rows = None if limit is None else max(int(limit), 0)
        if max_rows == 0:
            return []

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            eligible = self._eligible_download_candidates(subscriptions, now_text)

            claimed_ids: list[int] = []
            for row in eligible:
                if max_rows is not None and len(claimed_ids) >= max_rows:
                    break
                cursor = self.conn.execute(
                    """UPDATE downloads
                       SET status = 'downloading',
                           next_action = NULL, next_retry_at = NULL
                       WHERE issue_id = ?
                         AND requested_by IN ('manual', 'subscription')
                         AND EXISTS (
                             SELECT 1 FROM issues i
                             WHERE i.id = downloads.issue_id
                               AND i.limewire_url = ?
                               AND i.limewire_url IS NOT NULL
                               AND i.limewire_url != ''
                         )
                         AND (
                             (status = 'pending' AND next_action IS NULL)
                             OR
                             (status = 'failed'
                              AND next_action = 'DOWNLOAD'
                              AND next_retry_at IS NOT NULL
                              AND datetime(next_retry_at) <= datetime(?)
                              AND (last_error_kind = 'transient'
                                   OR last_error_kind IS NULL))
                         )""",
                    (row["issue_id"], row["limewire_url"], now_text),
                )
                if cursor.rowcount == 1:
                    claimed_ids.append(row["issue_id"])

            claimed = self.get_issues_by_ids(claimed_ids)
            self.conn.commit()
            return claimed
        except BaseException:
            self.conn.rollback()
            raise

    # Concise alias for daemon/caller code.
    claim_download_work = claim_pending_and_due_downloads

    def preview_claimable_downloads(
        self,
        subscriptions: list[Any],
        *,
        now: datetime | str | None = None,
    ) -> tuple[list[dict], int]:
        """Non-mutating preview sharing the claim's exact predicates.

        Returns the issues the download claim would take right now, plus the
        count of due wanted link-refresh actions, without claiming or changing
        any row. Divergence from the claim under identical inputs is a defect.
        """
        now_text = _utc_timestamp(now)
        eligible = self._eligible_download_candidates(subscriptions, now_text)
        issues = self.get_issues_by_ids([row["issue_id"] for row in eligible])
        due_refreshes = len(self._eligible_refresh_candidates(subscriptions, now_text))
        return issues, due_refreshes

    def _eligible_refresh_candidates(
        self, subscriptions: list[Any], now_text: str
    ) -> list[sqlite3.Row]:
        """Shared candidate selection for the refresh claim and its preview.

        Same provenance allowlist and claim-time eligibility as download
        claiming: a refresh for a row nobody wants is wasted source traffic.
        """
        candidates = self.conn.execute(
            """SELECT d.issue_id, d.next_retry_at, d.requested_by, i.page_url,
                      COALESCE(i.limewire_url, '') AS limewire_url,
                      i.title, i.year, i.month
               FROM downloads d
               JOIN issues i ON i.id = d.issue_id
               WHERE d.status = 'unavailable'
                 AND d.next_action = 'REFRESH_LINK'
                 AND d.next_retry_at IS NOT NULL
                 AND datetime(d.next_retry_at) <= datetime(?)
                 AND d.requested_by IS NOT NULL
                 AND i.page_url IS NOT NULL AND i.page_url != ''
               ORDER BY datetime(d.next_retry_at), d.issue_id""",
            (now_text,),
        ).fetchall()

        eligible: list[sqlite3.Row] = []
        unknown = 0
        for row in candidates:
            provenance = row["requested_by"]
            if provenance not in _WANTED_PROVENANCE:
                unknown += 1
                continue
            if provenance == RequestedBy.SUBSCRIPTION.value and not eligible_for_any(
                {"title": row["title"], "year": row["year"], "month": row["month"]},
                subscriptions,
            ):
                continue
            eligible.append(row)
        if unknown:
            logger.warning(
                "Ignoring %d refresh action(s) with unrecognized requested_by "
                "values; they fail closed and are never claimed",
                unknown,
            )
        return eligible

    def claim_due_link_refreshes(
        self,
        subscriptions: list[Any],
        *,
        now: datetime | str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Reserve due wanted source-only refreshes without queueing dead shares.

        ``subscriptions`` is required (fail-closed): null-provenance rows'
        refresh actions stay persisted but are never claimed. Reservation
        consumes the action/time while leaving status unavailable. The caller
        must resolve it as clear, rescheduled, or rotated; ordinary exceptions
        should reschedule through :meth:`schedule_link_refresh`.
        """
        now_text = _utc_timestamp(now)
        max_rows = None if limit is None else max(int(limit), 0)
        if max_rows == 0:
            return []

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            candidates = self._eligible_refresh_candidates(subscriptions, now_text)

            claimed_ids: list[int] = []
            for row in candidates:
                if max_rows is not None and len(claimed_ids) >= max_rows:
                    break
                cursor = self.conn.execute(
                    """UPDATE downloads
                       SET next_action = NULL, next_retry_at = NULL
                       WHERE issue_id = ?
                         AND status = 'unavailable'
                         AND next_action = 'REFRESH_LINK'
                         AND next_retry_at IS NOT NULL
                         AND datetime(next_retry_at) <= datetime(?)
                         AND requested_by IN ('manual', 'subscription')
                         AND EXISTS (
                             SELECT 1 FROM issues i
                             WHERE i.id = downloads.issue_id
                               AND i.page_url = ?
                               AND COALESCE(i.limewire_url, '') = ?
                         )""",
                    (
                        row["issue_id"],
                        now_text,
                        row["page_url"],
                        row["limewire_url"],
                    ),
                )
                if cursor.rowcount == 1:
                    claimed_ids.append(row["issue_id"])

            claimed = self.get_issues_by_ids(claimed_ids)
            self.conn.commit()
            return claimed
        except BaseException:
            self.conn.rollback()
            raise

    claim_due_refreshes = claim_due_link_refreshes

    def count_pending_link_refreshes(self) -> int:
        """Return the number of persisted *wanted* source-only refresh actions.

        This is an observability count, so it includes both due and future
        ``REFRESH_LINK`` work without claiming or otherwise mutating rows.
        Parked (null/unrecognized provenance) actions are excluded: the cycle
        summary must not advertise refreshes that will never run.
        """
        row = self.conn.execute(
            """SELECT COUNT(*) AS count
               FROM downloads
               WHERE next_action = 'REFRESH_LINK'
                 AND requested_by IN ('manual', 'subscription')"""
        ).fetchone()
        return int(row["count"] if row is not None else 0)

    def claim_manual_retry_downloads(
        self, magazine_title: str | None = None
    ) -> tuple[list[dict], int, int]:
        """Atomically claim the invocation snapshot for explicit retry.

        This bypasses future DOWNLOAD/REFRESH_LINK timing, excludes pending and
        unsupported rows, and returns only rows moved to ``downloading``.

        Eligibility is provenance-only: wanted rows are retried regardless of
        current subscription membership or ``since`` (an explicit retry must
        not skip rows whose subscription lapsed). Null/unrecognized provenance
        is excluded and reported via the third return element so the caller
        can name the recovery path.
        """
        query = """
            SELECT i.id, i.limewire_url, d.requested_by
            FROM downloads d
            JOIN issues i ON d.issue_id = i.id
            JOIN magazines m ON i.magazine_id = m.id
            WHERE d.status IN ('failed', 'unavailable')
        """
        params: list[Any] = []
        if magazine_title:
            query += " AND m.normalized_title LIKE ?"
            params.append(f"%{magazine_title}%")

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self.conn.execute(query, params).fetchall()
            wanted = [row for row in rows if row["requested_by"] in _WANTED_PROVENANCE]
            excluded = len(rows) - len(wanted)
            linked = [row for row in wanted if row["limewire_url"]]
            skipped = len(wanted) - len(linked)
            claimed_ids: list[int] = []
            for row in linked:
                if not _plausible_limewire_url(row["limewire_url"]):
                    skipped += 1
                    continue
                cursor = self.conn.execute(
                    """UPDATE downloads
                       SET status = 'downloading', file_path = NULL,
                           downloaded_at = NULL, file_size_bytes = NULL,
                           last_error_kind = NULL, last_error = NULL,
                           attempt_count = 0, last_attempt_at = NULL,
                           next_action = NULL, next_retry_at = NULL
                       WHERE issue_id = ?
                         AND status IN ('failed', 'unavailable')
                         AND requested_by IN ('manual', 'subscription')
                         AND EXISTS (
                             SELECT 1 FROM issues i
                             WHERE i.id = downloads.issue_id
                               AND i.limewire_url = ?
                               AND i.limewire_url IS NOT NULL
                               AND i.limewire_url != ''
                         )""",
                    (row["id"], row["limewire_url"]),
                )
                if cursor.rowcount == 1:
                    claimed_ids.append(row["id"])
            claimed = self.get_issues_by_ids(claimed_ids)
            self.conn.commit()
            return claimed, skipped, excluded
        except BaseException:
            self.conn.rollback()
            raise

    def schedule_link_refresh(
        self,
        issue_id: int,
        retry_at: datetime | str,
        *,
        error: str | BaseException | None = None,
    ) -> bool:
        """Park an unavailable share with a future source-only refresh."""
        from magsync.core.diagnostics import sanitize_external_error

        sanitized = sanitize_external_error(error) if error is not None else None
        cursor = self.conn.execute(
            """UPDATE downloads
               SET status = 'unavailable', next_action = 'REFRESH_LINK',
                   next_retry_at = ?,
                   last_error = COALESCE(?, last_error)
               WHERE issue_id = ? AND status = 'unavailable'""",
            (_utc_timestamp(retry_at), sanitized, issue_id),
        )
        self.conn.commit()
        return cursor.rowcount == 1

    reschedule_link_refresh = schedule_link_refresh

    def clear_link_refresh(self, issue_id: int) -> bool:
        """Clear a pending/claimed source refresh while leaving it parked."""
        cursor = self.conn.execute(
            """UPDATE downloads
               SET next_action = NULL, next_retry_at = NULL
               WHERE issue_id = ? AND status = 'unavailable'""",
            (issue_id,),
        )
        self.conn.commit()
        return cursor.rowcount == 1

    def rotate_limewire_url(self, issue_id: int, limewire_url: str) -> bool:
        """Atomically store a validated different URL and clear old identity."""
        if not _plausible_limewire_url(limewire_url):
            raise ValueError("invalid LimeWire share URL")
        limewire_url = normalize_limewire_share_url(limewire_url)

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            existing = self.conn.execute(
                "SELECT limewire_url FROM issues WHERE id = ?", (issue_id,)
            ).fetchone()
            if existing is None:
                self.conn.rollback()
                return False
            if existing["limewire_url"] == limewire_url:
                self.conn.commit()
                return False

            self.conn.execute(
                "UPDATE issues SET limewire_url = ? WHERE id = ?",
                (limewire_url, issue_id),
            )
            self.conn.execute(
                """UPDATE downloads
                   SET status = CASE
                           WHEN status IN ('failed', 'unavailable', 'unsupported')
                               THEN 'pending'
                           ELSE status
                       END,
                       file_path = CASE
                           WHEN status IN ('failed', 'unavailable', 'unsupported')
                               THEN NULL
                           ELSE file_path
                       END,
                       downloaded_at = CASE
                           WHEN status IN ('failed', 'unavailable', 'unsupported')
                               THEN NULL
                           ELSE downloaded_at
                       END,
                       file_size_bytes = CASE
                           WHEN status IN ('failed', 'unavailable', 'unsupported')
                               THEN NULL
                           ELSE file_size_bytes
                       END,
                       last_error_kind = NULL, last_error = NULL,
                       attempt_count = 0, last_attempt_at = NULL,
                       next_action = NULL, next_retry_at = NULL
                   WHERE issue_id = ?""",
                (issue_id,),
            )
            self.conn.commit()
            return True
        except BaseException:
            self.conn.rollback()
            raise

    def resolve_link_refresh(
        self,
        issue_id: int,
        outcome: Any,
        *,
        retry_at: datetime | str | None = None,
    ) -> bool:
        """Apply a structured refresh outcome to the parked issue."""
        from magsync.core.models import RefreshOutcomeKind

        kind = _enum_value(outcome.kind)
        if kind == RefreshOutcomeKind.ROTATED.value:
            if not outcome.url:
                raise ValueError("rotated refresh outcome requires a URL")
            return self.rotate_limewire_url(issue_id, outcome.url)
        if kind in (
            RefreshOutcomeKind.UNCHANGED.value,
            RefreshOutcomeKind.NO_LINK.value,
        ):
            return self.clear_link_refresh(issue_id)
        if kind in (
            RefreshOutcomeKind.SOURCE_BLOCKED.value,
            RefreshOutcomeKind.SCRAPE_ERROR.value,
        ):
            if retry_at is None:
                raise ValueError("blocked/failed refresh outcome requires retry_at")
            failure = getattr(outcome, "failure", None)
            message = getattr(failure, "message", None) or getattr(
                failure, "error", None
            )
            return self.schedule_link_refresh(issue_id, retry_at, error=message)
        raise ValueError(f"unknown refresh outcome: {kind!r}")

    def reset_failed_downloads(
        self, magazine_title: str | None = None
    ) -> tuple[list[int], int]:
        """Reset failed and unavailable downloads back to pending.

        Only rows whose issue has a download link are reset — link-less rows
        keep their status so they stay visible as failures and reachable by
        ``backfill-urls`` instead of being stranded as permanently-pending.
        'unsupported' rows are deliberately excluded: a non-PDF payload is
        terminal until the share link rotates (see add_issues). Null-provenance
        rows are likewise excluded — never-requested catalog entries must not
        be flipped into (now claim-invisible) pending work by a manual reset.

        Runs as a single write transaction (``BEGIN IMMEDIATE``) and the
        UPDATE re-asserts the status guard, so a concurrent writer (the
        daemon shares this DB) can never have an in-flight or completed row
        clobbered back to pending.

        Returns ``(reset_issue_ids, skipped_missing_link_count)``.
        """
        query = """
            SELECT i.id, i.limewire_url
            FROM downloads d
            JOIN issues i ON d.issue_id = i.id
            JOIN magazines m ON i.magazine_id = m.id
            WHERE d.status IN ('failed', 'unavailable')
              AND d.requested_by IN ('manual', 'subscription')
        """
        params: list = []
        if magazine_title:
            query += " AND m.normalized_title LIKE ?"
            params.append(f"%{magazine_title}%")

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self.conn.execute(query, params).fetchall()
            linked = [row["id"] for row in rows if row["limewire_url"]]
            skipped = len(rows) - len(linked)

            reset_ids: list[int] = []
            for start in range(0, len(linked), _SQL_IN_CHUNK):
                chunk = linked[start : start + _SQL_IN_CHUNK]
                placeholders = ",".join("?" * len(chunk))
                cursor = self.conn.execute(
                    f"""UPDATE downloads
                        SET status = 'pending', file_path = NULL, downloaded_at = NULL,
                            file_size_bytes = NULL,
                            last_error_kind = NULL, last_error = NULL,
                            attempt_count = 0, last_attempt_at = NULL,
                            next_action = NULL, next_retry_at = NULL
                        WHERE issue_id IN ({placeholders})
                          AND status IN ('failed', 'unavailable')
                          AND requested_by IN ('manual', 'subscription')""",
                    chunk,
                )
                if cursor.rowcount == len(chunk):
                    reset_ids.extend(chunk)
                else:
                    # Belt-and-braces: the write lock should make this
                    # unreachable, but never report a row we didn't flip.
                    flipped = self.conn.execute(
                        f"""SELECT issue_id FROM downloads
                            WHERE issue_id IN ({placeholders}) AND status = 'pending'""",
                        chunk,
                    ).fetchall()
                    reset_ids.extend(r["issue_id"] for r in flipped)
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return reset_ids, skipped

    def reset_stuck_downloads(self) -> int:
        """Daemon-startup reset for interrupted downloads only.

        Persisted failed/unavailable timing survives restarts. Future daemon
        cycles claim those rows only when their typed action becomes due.
        """
        count = self.conn.execute(
            """UPDATE downloads
               SET status = 'pending', next_action = NULL, next_retry_at = NULL
               WHERE status = 'downloading'"""
        ).rowcount
        self.conn.commit()
        return count

    def get_issues_by_ids(self, issue_ids: list[int]) -> list[dict]:
        """Fetch issues by explicit IDs, in the same joined shape as
        ``get_issues()``. Chunks the IN clause to stay under SQLite's
        host-parameter limit."""
        results: list[dict] = []
        for start in range(0, len(issue_ids), _SQL_IN_CHUNK):
            chunk = issue_ids[start : start + _SQL_IN_CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"""SELECT i.*, d.status as download_status, d.file_path,
                           d.file_size_bytes AS downloaded_file_size_bytes, d.sha256,
                           d.last_error_kind, d.last_error, d.attempt_count,
                           d.last_attempt_at, d.next_action, d.next_retry_at,
                           d.requested_by,
                           m.title as magazine_title, m.normalized_title
                    FROM issues i
                    JOIN magazines m ON i.magazine_id = m.id
                    LEFT JOIN downloads d ON d.issue_id = i.id
                    WHERE i.id IN ({placeholders})""",
                chunk,
            ).fetchall()
            results.extend(dict(row) for row in rows)
        results.sort(
            key=lambda r: (
                r["year"] is None, -(r["year"] or 0),
                r["month"] is None, -(r["month"] or 0),
                r["title"],
            )
        )
        return results

    def get_download_stats(self) -> dict:
        """Get overall download statistics.

        ``pending`` counts actionable (wanted) queued work only; ``cataloged``
        counts parked null/unrecognized-provenance pending rows, which no
        automatic claim will ever download.
        """
        row = self.conn.execute(
            """SELECT
                 COUNT(*) as total_issues,
                 SUM(CASE WHEN d.status = 'complete' THEN 1 ELSE 0 END) as downloaded,
                 SUM(CASE WHEN d.status = 'pending'
                          AND d.requested_by IN ('manual', 'subscription')
                     THEN 1 ELSE 0 END) as pending,
                 SUM(CASE WHEN d.status = 'pending'
                          AND (d.requested_by IS NULL
                               OR d.requested_by NOT IN ('manual', 'subscription'))
                     THEN 1 ELSE 0 END) as cataloged,
                 SUM(CASE WHEN d.status = 'failed' THEN 1 ELSE 0 END) as failed,
                 SUM(CASE WHEN d.status = 'unavailable' THEN 1 ELSE 0 END) as unavailable,
                 SUM(CASE WHEN d.status = 'unsupported' THEN 1 ELSE 0 END) as unsupported
               FROM issues i
               LEFT JOIN downloads d ON d.issue_id = i.id"""
        ).fetchone()
        return dict(row)

    def get_pipeline_state(self) -> dict:
        """Return the durable singleton pipeline state."""
        row = self.conn.execute(
            """SELECT last_cycle_at, last_cycle_status,
                      last_successful_source_check_at,
                      consecutive_source_failure_cycles, degraded_reason
               FROM pipeline_state WHERE id = 1"""
        ).fetchone()
        if row is None:  # Defensive for externally modified databases.
            self.conn.execute(
                """INSERT INTO pipeline_state
                   (id, consecutive_source_failure_cycles) VALUES (1, 0)"""
            )
            self.conn.commit()
            return {
                "last_cycle_at": None,
                "last_cycle_status": None,
                "last_successful_source_check_at": None,
                "consecutive_source_failure_cycles": 0,
                "degraded_reason": None,
            }
        return dict(row)

    def update_pipeline_state(
        self,
        status: Any,
        *,
        cycle_at: datetime | str | None = None,
        source_validated: bool | None = None,
        source_check_at: datetime | str | None = None,
        degraded_reason: str | BaseException | None = None,
    ) -> dict:
        """Persist one cycle's pipeline health independently of liveness.

        ``source_validated=True`` includes a validated explicit-empty result;
        it refreshes the success timestamp and clears consecutive source
        failures. ``False`` increments once for the cycle. ``None`` means the
        cycle performed no authoritative source check and preserves both.
        """
        from magsync.core.diagnostics import sanitize_external_error

        cycle_timestamp = _utc_timestamp(cycle_at)
        reason = (
            sanitize_external_error(degraded_reason)
            if degraded_reason is not None
            else None
        )
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            current = self.conn.execute(
                "SELECT * FROM pipeline_state WHERE id = 1"
            ).fetchone()
            previous_success = (
                current["last_successful_source_check_at"] if current else None
            )
            previous_failures = (
                current["consecutive_source_failure_cycles"] if current else 0
            )
            if source_validated is True:
                successful_at = _utc_timestamp(source_check_at or cycle_timestamp)
                consecutive_failures = 0
            elif source_validated is False:
                successful_at = previous_success
                consecutive_failures = previous_failures + 1
            else:
                successful_at = previous_success
                consecutive_failures = previous_failures

            self.conn.execute(
                """INSERT INTO pipeline_state
                   (id, last_cycle_at, last_cycle_status,
                    last_successful_source_check_at,
                    consecutive_source_failure_cycles, degraded_reason)
                   VALUES (1, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       last_cycle_at = excluded.last_cycle_at,
                       last_cycle_status = excluded.last_cycle_status,
                       last_successful_source_check_at =
                           excluded.last_successful_source_check_at,
                       consecutive_source_failure_cycles =
                           excluded.consecutive_source_failure_cycles,
                       degraded_reason = excluded.degraded_reason""",
                (
                    cycle_timestamp,
                    _enum_value(status),
                    successful_at,
                    consecutive_failures,
                    reason,
                ),
            )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return self.get_pipeline_state()

    record_pipeline_cycle = update_pipeline_state
