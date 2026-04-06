"""SQLite magazine index for magsync."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from magsync.config import get_db_path
from magsync.core.models import DownloadStatus, Issue, Magazine

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
    file_size_bytes INTEGER
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
        """Upsert issues for a magazine. Returns count of new issues added."""
        added = 0
        for issue in issues:
            existing = self.conn.execute(
                "SELECT id FROM issues WHERE page_url = ?",
                (issue["page_url"],),
            ).fetchone()
            if existing:
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
    ):
        """Update download status for an issue."""
        downloaded_at = datetime.now().isoformat() if status == DownloadStatus.COMPLETE else None
        self.conn.execute(
            """UPDATE downloads
               SET status = ?, file_path = ?, downloaded_at = ?, file_size_bytes = ?
               WHERE issue_id = ?""",
            (status.value, file_path, downloaded_at, file_size_bytes, issue_id),
        )
        self.conn.commit()

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

    def reset_failed_downloads(self, magazine_title: str | None = None) -> int:
        """Reset failed downloads back to pending. Returns count reset."""
        if magazine_title:
            cursor = self.conn.execute(
                """UPDATE downloads SET status = 'pending', file_path = NULL, downloaded_at = NULL
                   WHERE status = 'failed' AND issue_id IN (
                       SELECT i.id FROM issues i
                       JOIN magazines m ON i.magazine_id = m.id
                       WHERE m.normalized_title LIKE ?
                   )""",
                (f"%{magazine_title}%",),
            )
        else:
            cursor = self.conn.execute(
                "UPDATE downloads SET status = 'pending', file_path = NULL, downloaded_at = NULL WHERE status = 'failed'"
            )
        self.conn.commit()
        return cursor.rowcount

    def get_download_stats(self) -> dict:
        """Get overall download statistics."""
        row = self.conn.execute(
            """SELECT
                 COUNT(*) as total_issues,
                 SUM(CASE WHEN d.status = 'complete' THEN 1 ELSE 0 END) as downloaded,
                 SUM(CASE WHEN d.status = 'pending' THEN 1 ELSE 0 END) as pending,
                 SUM(CASE WHEN d.status = 'failed' THEN 1 ELSE 0 END) as failed
               FROM issues i
               LEFT JOIN downloads d ON d.issue_id = i.id"""
        ).fetchone()
        return dict(row)
