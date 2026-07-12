"""Authoritative policy for typed download failures."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from magsync.core.models import (
    DownloadFailureKind,
    DownloadResult,
    DownloadStatus,
    DownloadSummaryBucket,
)


@dataclass(frozen=True)
class DownloadFailurePolicy:
    """All routing decisions derived from a download failure kind."""

    immediate_retry: bool
    refresh_link: bool
    automatic_retry: bool
    final_status: DownloadStatus
    summary_bucket: DownloadSummaryBucket
    log_level: int


DOWNLOAD_FAILURE_POLICIES: dict[DownloadFailureKind, DownloadFailurePolicy] = {
    DownloadFailureKind.TRANSIENT: DownloadFailurePolicy(
        immediate_retry=True,
        refresh_link=False,
        automatic_retry=True,
        final_status=DownloadStatus.FAILED,
        summary_bucket=DownloadSummaryBucket.FAILED,
        log_level=logging.WARNING,
    ),
    DownloadFailureKind.SHARE_UNAVAILABLE: DownloadFailurePolicy(
        immediate_retry=False,
        refresh_link=True,
        automatic_retry=False,
        final_status=DownloadStatus.UNAVAILABLE,
        summary_bucket=DownloadSummaryBucket.UNAVAILABLE,
        log_level=logging.INFO,
    ),
    DownloadFailureKind.METADATA_INVALID: DownloadFailurePolicy(
        immediate_retry=False,
        refresh_link=False,
        automatic_retry=False,
        final_status=DownloadStatus.FAILED,
        summary_bucket=DownloadSummaryBucket.FAILED,
        log_level=logging.ERROR,
    ),
    DownloadFailureKind.DECRYPTION_FAILED: DownloadFailurePolicy(
        immediate_retry=False,
        refresh_link=False,
        automatic_retry=False,
        final_status=DownloadStatus.FAILED,
        summary_bucket=DownloadSummaryBucket.FAILED,
        log_level=logging.ERROR,
    ),
    DownloadFailureKind.UNSUPPORTED: DownloadFailurePolicy(
        immediate_retry=False,
        refresh_link=False,
        automatic_retry=False,
        final_status=DownloadStatus.UNSUPPORTED,
        summary_bucket=DownloadSummaryBucket.UNSUPPORTED,
        log_level=logging.INFO,
    ),
    DownloadFailureKind.CONFIGURATION: DownloadFailurePolicy(
        immediate_retry=False,
        refresh_link=False,
        automatic_retry=False,
        final_status=DownloadStatus.FAILED,
        summary_bucket=DownloadSummaryBucket.FAILED,
        log_level=logging.ERROR,
    ),
    DownloadFailureKind.INTERNAL: DownloadFailurePolicy(
        immediate_retry=False,
        refresh_link=False,
        automatic_retry=False,
        final_status=DownloadStatus.FAILED,
        summary_bucket=DownloadSummaryBucket.FAILED,
        log_level=logging.ERROR,
    ),
}


def get_download_failure_policy(
    kind: DownloadFailureKind | str,
) -> DownloadFailurePolicy:
    """Return policy for ``kind`` without consulting display text."""

    return DOWNLOAD_FAILURE_POLICIES[DownloadFailureKind(kind)]


def policy_for_result(result: DownloadResult) -> DownloadFailurePolicy:
    """Return policy for an unsuccessful result during caller migration.

    Untyped legacy failures are handled conservatively as ``INTERNAL``.  The
    compatibility path is intentionally message-independent.
    """

    if result.success:
        raise ValueError("successful download results do not have failure policy")
    kind = result.failure_kind or DownloadFailureKind.INTERNAL
    return get_download_failure_policy(kind)
