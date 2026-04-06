"""Data models for magsync."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path


class DownloadStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class Magazine:
    id: int | None = None
    title: str = ""
    normalized_title: str = ""
    first_seen: datetime | None = None
    last_updated: datetime | None = None


@dataclass
class Issue:
    id: int | None = None
    magazine_id: int | None = None
    title: str = ""
    page_url: str = ""
    limewire_url: str = ""
    year: int | None = None
    month: int | None = None
    date_raw: str = ""
    genre: str | None = None
    file_size: str | None = None
    cover_image_url: str | None = None
    discovered_at: datetime | None = None


@dataclass
class DownloadRecord:
    id: int | None = None
    issue_id: int | None = None
    status: DownloadStatus = DownloadStatus.PENDING
    file_path: str | None = None
    downloaded_at: datetime | None = None
    file_size_bytes: int | None = None


@dataclass
class DownloadResult:
    success: bool
    file_path: Path | None = None
    error: str | None = None
    file_size_bytes: int = 0
    sha256: str | None = None


@dataclass
class LimeWireSession:
    jwt_token: str = ""
    csrf_token: str = ""
    bucket_id: str = ""
    content_item_id: str = ""
    passphrase_wrapped_pk: str = ""
    ephemeral_public_key: str = ""
    file_name: str = ""
    file_size: int = 0


@dataclass
class Subscription:
    query: str = ""
    since: str | None = None  # YYYY-MM format, e.g. "2025-01"
    exact: bool = False  # Only index issues whose normalized title matches the query exactly


@dataclass
class EncryptionConstants:
    sharing_salt_b64: str = ""
    file_iv_b64: str = ""
    pbkdf2_iterations: int = 100_000
