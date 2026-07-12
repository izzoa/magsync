"""Data models for magsync.

The enums in this module are deliberately suitable for persistence: their
values form the stable boundary between downloader, index, CLI, and daemon
code.  Human-readable error messages are display data only and must never be
used to infer one of these classifications.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Generic, TypeVar


class DownloadStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETE = "complete"
    FAILED = "failed"          # Typed failure; only transient kinds may be scheduled
    UNAVAILABLE = "unavailable"  # Permanent error (dead link) — never auto-retried
    UNSUPPORTED = "unsupported"  # Live share, non-PDF payload — never auto-retried; re-queued only on link rotation


class DownloadFailureKind(str, Enum):
    """Stable classifications for unsuccessful download attempts."""

    TRANSIENT = "transient"
    SHARE_UNAVAILABLE = "share_unavailable"
    METADATA_INVALID = "metadata_invalid"
    DECRYPTION_FAILED = "decryption_failed"
    UNSUPPORTED = "unsupported"
    CONFIGURATION = "configuration"
    INTERNAL = "internal"


class DownloadSummaryBucket(str, Enum):
    """User-facing summary bucket selected by download-failure policy."""

    FAILED = "failed"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"


class SourceFailureKind(str, Enum):
    """Stable classifications for freemagazines.top failures."""

    ACCESS_BLOCKED = "access_blocked"
    TRANSIENT = "transient"
    PROTOCOL = "protocol"


@dataclass(frozen=True)
class SourceFailure:
    """Structured, safe-to-present context for a source operation failure.

    ``message`` is intended to contain already-sanitized display text.  Raw
    response bodies, URLs, tokens, and headers do not belong in this model.
    """

    kind: SourceFailureKind
    message: str
    operation: str | None = None
    status_code: int | None = None
    host: str | None = None
    path: str | None = None
    cf_ray: str | None = None


class SourceError(Exception):
    """Exception wrapper for an operation-level :class:`SourceFailure`."""

    def __init__(
        self,
        kind: SourceFailureKind,
        message: str,
        *,
        operation: str | None = None,
        status_code: int | None = None,
        host: str | None = None,
        path: str | None = None,
        cf_ray: str | None = None,
    ) -> None:
        self.failure = SourceFailure(
            kind=kind,
            message=message,
            operation=operation,
            status_code=status_code,
            host=host,
            path=path,
            cf_ray=cf_ray,
        )
        super().__init__(message)

    @property
    def kind(self) -> SourceFailureKind:
        return self.failure.kind

    @property
    def operation(self) -> str | None:
        return self.failure.operation

    @property
    def status_code(self) -> int | None:
        return self.failure.status_code

    @property
    def host(self) -> str | None:
        return self.failure.host

    @property
    def path(self) -> str | None:
        return self.failure.path

    @property
    def cf_ray(self) -> str | None:
        return self.failure.cf_ray


SourceItemT = TypeVar("SourceItemT")


@dataclass
class SourceResult(Generic[SourceItemT]):
    """Result of a source operation, including partial detail failures.

    ``failure`` represents an operation-level failure.  ``failures`` contains
    issue-specific failures that did not discard valid sibling ``items``.
    """

    items: list[SourceItemT] = field(default_factory=list)
    failures: list[SourceFailure] = field(default_factory=list)
    failure: SourceFailure | None = None
    validated_empty: bool = False

    @property
    def success(self) -> bool:
        return self.failure is None

    @property
    def partial(self) -> bool:
        return self.failure is None and bool(self.failures)


class RefreshOutcomeKind(str, Enum):
    """Possible results of refreshing an issue's source page."""

    ROTATED = "rotated"
    UNCHANGED = "unchanged"
    NO_LINK = "no_link"
    SOURCE_BLOCKED = "source_blocked"
    SCRAPE_ERROR = "scrape_error"


@dataclass(frozen=True)
class RefreshOutcome:
    """Structured link-refresh outcome.

    Only ``ROTATED`` may carry a replacement URL.  Blocked and scrape-error
    outcomes may carry a typed source failure for safe diagnostics.
    """

    kind: RefreshOutcomeKind
    url: str | None = None
    failure: SourceFailure | None = None

    def __post_init__(self) -> None:
        if self.kind is RefreshOutcomeKind.ROTATED and not self.url:
            raise ValueError("a rotated refresh outcome requires a URL")
        if self.kind is not RefreshOutcomeKind.ROTATED and self.url is not None:
            raise ValueError("only a rotated refresh outcome may carry a URL")


class RetryAction(str, Enum):
    """Persisted work scheduled for a later daemon cycle."""

    DOWNLOAD = "DOWNLOAD"
    REFRESH_LINK = "REFRESH_LINK"


class PipelineStatus(str, Enum):
    """Pipeline health, intentionally independent from process liveness."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass
class CycleReport:
    """Reconciled counters for one daemon cycle."""

    source_total: int = 0
    source_attempted: int = 0
    source_succeeded: int = 0
    source_empty: int = 0
    source_failed: int = 0
    source_skipped: int = 0
    detail_failures: int = 0
    downloads_queued: int = 0
    downloads_unique: int = 0
    downloads_complete: int = 0
    downloads_unavailable: int = 0
    downloads_unsupported: int = 0
    downloads_failed: int = 0
    pending_refreshes: int = 0
    elapsed_seconds: float = 0.0
    status: PipelineStatus = PipelineStatus.HEALTHY
    reason: str | None = None

    @property
    def source_completed(self) -> int:
        """Number of attempted subscription searches with validated results."""

        return self.source_succeeded + self.source_empty


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
    unsupported: bool = False  # live share but non-PDF payload — terminal, never retried
    failure_kind: DownloadFailureKind | None = None
    attempt_count: int = 0

    def __post_init__(self) -> None:
        """Bridge the legacy ``unsupported`` flag during caller migration.

        Once supplied, ``failure_kind`` is authoritative.  Older callers that
        set only ``unsupported=True`` are upgraded to the typed kind without
        inspecting their display message.
        """

        if self.failure_kind is not None and not isinstance(
            self.failure_kind, DownloadFailureKind
        ):
            self.failure_kind = DownloadFailureKind(self.failure_kind)
        if self.failure_kind is not None:
            self.unsupported = self.failure_kind is DownloadFailureKind.UNSUPPORTED
        elif self.unsupported:
            self.failure_kind = DownloadFailureKind.UNSUPPORTED


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
