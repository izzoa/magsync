"""SQLite magazine index for magsync."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from magsync.config import get_db_path
from magsync.core.models import DownloadStatus, Issue, Magazine

logger = logging.getLogger("magsync")


def _plausible_limewire_url(url: str) -> bool:
    """Strict guard for overwriting a stored LimeWire URL.

    Deliberately stricter than the scraper's extraction-time validation (which
    is substring-based): requires the exact LimeWire host, a ``/d/<sharing_id>``
    path, and a non-empty ``#fragment`` (the decryption key). Protects the
    destructive path of replacing a known-good stored value.
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.hostname not in ("limewire.com", "www.limewire.com"):
        return False
    segments = [s for s in parsed.path.split("/") if s]
    if len(segments) != 2 or segments[0] != "d" or not segments[1]:
        return False
    return bool(parsed.fragment)


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
    sha256 TEXT
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
        if "sha256" not in columns:
            self.conn.execute("ALTER TABLE downloads ADD COLUMN sha256 TEXT")
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

    def add_issues(self, magazine_id: int, issues: list[dict]) -> int:
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
        """
        added = 0
        backfill_columns = ("limewire_url", "genre", "file_size", "cover_image_url")
        for issue in issues:
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
                incoming_url = issue.get("limewire_url")
                url_changed = bool(
                    incoming_url
                    and existing["limewire_url"]
                    and incoming_url != existing["limewire_url"]
                    and _plausible_limewire_url(incoming_url)
                )
                if url_changed:
                    updates["limewire_url"] = incoming_url
                if updates:
                    set_clause = ", ".join(f"{c} = ?" for c in updates)
                    self.conn.execute(
                        f"UPDATE issues SET {set_clause} WHERE id = ?",
                        (*updates.values(), existing["id"]),
                    )
                if url_changed:
                    # The old link is dead weight now — give parked downloads
                    # another chance with the fresh one (sha256 kept, complete
                    # and in-flight rows untouched). 'unsupported' is included:
                    # a rotated blob may carry a different payload type, so it
                    # gets exactly one cheap re-probe.
                    self.conn.execute(
                        """UPDATE downloads
                           SET status = 'pending', file_path = NULL, downloaded_at = NULL
                           WHERE issue_id = ? AND status IN ('failed', 'unavailable', 'unsupported')""",
                        (existing["id"],),
                    )
                    logger.info(
                        f"Refreshed LimeWire link for {issue['page_url']}: "
                        f"{_sharing_id(existing['limewire_url'])} → {_sharing_id(incoming_url)}"
                    )
                continue

            cursor = self.conn.execute(
                """INSERT INTO issues
                   (magazine_id, title, page_url, limewire_url, year, month,
                    date_raw, genre, file_size, cover_image_url)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    magazine_id,
                    issue.get("title", ""),
                    issue["page_url"],
                    issue.get("limewire_url"),
                    issue.get("year"),
                    issue.get("month"),
                    issue.get("date_raw", ""),
                    issue.get("genre"),
                    issue.get("file_size"),
                    issue.get("cover_image_url"),
                ),
            )
            # Create initial download record
            self.conn.execute(
                "INSERT INTO downloads (issue_id, status) VALUES (?, ?)",
                (cursor.lastrowid, DownloadStatus.PENDING.value),
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
        """Update download status for an issue."""
        downloaded_at = datetime.now().isoformat() if status == DownloadStatus.COMPLETE else None
        self.conn.execute(
            """UPDATE downloads
               SET status = ?, file_path = ?, downloaded_at = ?, file_size_bytes = ?, sha256 = ?
               WHERE issue_id = ?""",
            (status.value, file_path, downloaded_at, file_size_bytes, sha256, issue_id),
        )
        self.conn.commit()

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

    def get_issues_missing_url(self, magazine_title: str | None = None) -> list[dict]:
        """Return issues whose limewire_url is NULL/empty, optionally filtered by magazine."""
        query = """
            SELECT i.id, i.page_url, i.title,
                   m.title as magazine_title, m.normalized_title
            FROM issues i
            JOIN magazines m ON i.magazine_id = m.id
            WHERE (i.limewire_url IS NULL OR i.limewire_url = '')
        """
        params: list = []
        if magazine_title:
            query += " AND m.normalized_title LIKE ?"
            params.append(f"%{magazine_title}%")
        query += " ORDER BY m.title, i.title"
        rows = self.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def set_limewire_url(self, issue_id: int, limewire_url: str):
        """Set the limewire_url for an issue (used by backfill)."""
        self.conn.execute(
            "UPDATE issues SET limewire_url = ? WHERE id = ?",
            (limewire_url, issue_id),
        )
        self.conn.commit()

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

    def reset_failed_downloads(
        self, magazine_title: str | None = None
    ) -> tuple[list[int], int]:
        """Reset failed and unavailable downloads back to pending.

        Only rows whose issue has a download link are reset — link-less rows
        keep their status so they stay visible as failures and reachable by
        ``backfill-urls`` instead of being stranded as permanently-pending.
        'unsupported' rows are deliberately excluded: a non-PDF payload is
        terminal until the share link rotates (see add_issues).

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
                        SET status = 'pending', file_path = NULL, downloaded_at = NULL
                        WHERE issue_id IN ({placeholders})
                          AND status IN ('failed', 'unavailable')""",
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
        """Daemon-startup reset: re-queue interrupted and failed downloads.

        Interrupted (``downloading``) rows are reset unconditionally — they
        were in-flight when the process died. ``failed`` rows are reset only
        when the issue still has a download link, mirroring
        ``reset_failed_downloads``, so link-less failures survive restarts.
        'unavailable' and 'unsupported' rows are deliberately untouched — both
        are terminal until the share link rotates. Returns count reset.
        """
        count = self.conn.execute(
            "UPDATE downloads SET status = 'pending' WHERE status = 'downloading'"
        ).rowcount
        count += self.conn.execute(
            """UPDATE downloads SET status = 'pending'
               WHERE status = 'failed' AND issue_id IN (
                   SELECT id FROM issues
                   WHERE limewire_url IS NOT NULL AND limewire_url != ''
               )"""
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
        """Get overall download statistics."""
        row = self.conn.execute(
            """SELECT
                 COUNT(*) as total_issues,
                 SUM(CASE WHEN d.status = 'complete' THEN 1 ELSE 0 END) as downloaded,
                 SUM(CASE WHEN d.status = 'pending' THEN 1 ELSE 0 END) as pending,
                 SUM(CASE WHEN d.status = 'failed' THEN 1 ELSE 0 END) as failed,
                 SUM(CASE WHEN d.status = 'unavailable' THEN 1 ELSE 0 END) as unavailable,
                 SUM(CASE WHEN d.status = 'unsupported' THEN 1 ELSE 0 END) as unsupported
               FROM issues i
               LEFT JOIN downloads d ON d.issue_id = i.id"""
        ).fetchone()
        return dict(row)
