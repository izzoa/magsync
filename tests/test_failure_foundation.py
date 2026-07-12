"""Tests for typed external-failure models and authoritative policy."""

from __future__ import annotations

import logging

import pytest

from magsync.core.diagnostics import sanitize_external_error
from magsync.core.models import (
    CycleReport,
    DownloadFailureKind,
    DownloadResult,
    DownloadStatus,
    DownloadSummaryBucket,
    PipelineStatus,
    RefreshOutcome,
    RefreshOutcomeKind,
    SourceError,
    SourceFailure,
    SourceFailureKind,
    SourceResult,
)
from magsync.core.policy import (
    DOWNLOAD_FAILURE_POLICIES,
    get_download_failure_policy,
    policy_for_result,
)


def test_failure_policy_is_complete_and_has_expected_routing():
    assert set(DOWNLOAD_FAILURE_POLICIES) == set(DownloadFailureKind)

    expected = {
        DownloadFailureKind.TRANSIENT: (
            True,
            False,
            True,
            DownloadStatus.FAILED,
            DownloadSummaryBucket.FAILED,
        ),
        DownloadFailureKind.SHARE_UNAVAILABLE: (
            False,
            True,
            False,
            DownloadStatus.UNAVAILABLE,
            DownloadSummaryBucket.UNAVAILABLE,
        ),
        DownloadFailureKind.METADATA_INVALID: (
            False,
            False,
            False,
            DownloadStatus.FAILED,
            DownloadSummaryBucket.FAILED,
        ),
        DownloadFailureKind.DECRYPTION_FAILED: (
            False,
            False,
            False,
            DownloadStatus.FAILED,
            DownloadSummaryBucket.FAILED,
        ),
        DownloadFailureKind.UNSUPPORTED: (
            False,
            False,
            False,
            DownloadStatus.UNSUPPORTED,
            DownloadSummaryBucket.UNSUPPORTED,
        ),
        DownloadFailureKind.CONFIGURATION: (
            False,
            False,
            False,
            DownloadStatus.FAILED,
            DownloadSummaryBucket.FAILED,
        ),
        DownloadFailureKind.INTERNAL: (
            False,
            False,
            False,
            DownloadStatus.FAILED,
            DownloadSummaryBucket.FAILED,
        ),
    }

    for kind, route in expected.items():
        policy = get_download_failure_policy(kind)
        assert (
            policy.immediate_retry,
            policy.refresh_link,
            policy.automatic_retry,
            policy.final_status,
            policy.summary_bucket,
        ) == route
        assert policy.log_level in {
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
        }


def test_policy_is_message_independent():
    first = DownloadResult(
        success=False,
        error="temporary wording",
        failure_kind=DownloadFailureKind.SHARE_UNAVAILABLE,
    )
    second = DownloadResult(
        success=False,
        error="completely different wording, including the word transient",
        failure_kind=DownloadFailureKind.SHARE_UNAVAILABLE,
    )

    assert policy_for_result(first) == policy_for_result(second)
    assert policy_for_result(first).final_status is DownloadStatus.UNAVAILABLE
    assert policy_for_result(first).refresh_link is True
    assert policy_for_result(first).immediate_retry is False


def test_untyped_legacy_failure_uses_conservative_internal_policy():
    result = DownloadResult(success=False, error="timeout wording is not policy")
    assert policy_for_result(result) == get_download_failure_policy(
        DownloadFailureKind.INTERNAL
    )


def test_download_result_unsupported_compatibility_is_typed_and_authoritative():
    legacy = DownloadResult(success=False, unsupported=True, error="legacy")
    assert legacy.failure_kind is DownloadFailureKind.UNSUPPORTED
    assert legacy.unsupported is True

    typed = DownloadResult(
        success=False,
        unsupported=False,
        failure_kind="unsupported",
    )
    assert typed.failure_kind is DownloadFailureKind.UNSUPPORTED
    assert typed.unsupported is True

    conflicting = DownloadResult(
        success=False,
        unsupported=True,
        failure_kind=DownloadFailureKind.TRANSIENT,
    )
    assert conflicting.failure_kind is DownloadFailureKind.TRANSIENT
    assert conflicting.unsupported is False


def test_source_models_represent_operation_and_partial_failures():
    detail_failure = SourceFailure(
        SourceFailureKind.PROTOCOL,
        "detail response was invalid",
        operation="detail",
        status_code=200,
        host="freemagazines.top",
        path="/issue-2026/",
    )
    partial = SourceResult(items=["valid issue"], failures=[detail_failure])
    assert partial.success is True
    assert partial.partial is True

    blocked = SourceError(
        SourceFailureKind.ACCESS_BLOCKED,
        "source access is blocked",
        operation="search",
        status_code=403,
        host="freemagazines.top",
        path="/",
        cf_ray="safe-ray-id",
    )
    assert blocked.kind is SourceFailureKind.ACCESS_BLOCKED
    assert blocked.failure.status_code == 403
    assert blocked.cf_ray == "safe-ray-id"
    assert str(blocked) == "source access is blocked"


def test_refresh_outcome_enforces_url_shape_contract():
    rotated = RefreshOutcome(
        RefreshOutcomeKind.ROTATED,
        url="https://limewire.com/d/New#secret",
    )
    assert rotated.url is not None

    with pytest.raises(ValueError, match="requires a URL"):
        RefreshOutcome(RefreshOutcomeKind.ROTATED)
    with pytest.raises(ValueError, match="only a rotated"):
        RefreshOutcome(RefreshOutcomeKind.NO_LINK, url="https://example.test/")


def test_cycle_report_exposes_reconciled_source_completion():
    report = CycleReport(
        source_total=25,
        source_attempted=3,
        source_succeeded=1,
        source_empty=1,
        source_failed=1,
        source_skipped=22,
        status=PipelineStatus.DEGRADED,
    )
    assert report.source_completed == 2
    assert report.status is PipelineStatus.DEGRADED


def test_sanitizer_redacts_external_secrets_in_caplog(caplog):
    fragment = "FRAGMENT_SECRET_7391"
    authorization = "AUTHORIZATION_SECRET_6248"
    cookie = "COOKIE_SECRET_9137"
    wrapped_key = "WRAPPED_KEY_SECRET_2874"
    ephemeral_key = "EPHEMERAL_KEY_SECRET_8526"
    credential = "PRESIGNED_CREDENTIAL_SECRET_4128"
    signature = "PRESIGNED_SIGNATURE_SECRET_7853"
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZWNyZXQifQ.signaturePart123"
    raw = "\n".join(
        (
            "operation=download status=403 id=AbC12",
            f"share=https://limewire.com/d/AbC12#{fragment}",
            "storage=https://bucket.example/file.pdf?"
            f"X-Amz-Credential={credential}&X-Amz-Signature={signature}",
            f"Authorization=Bearer {authorization}, status=401",
            f"Cookie: session={cookie}",
            f'{{"passphraseWrappedPrivateKey": "{wrapped_key}"}}',
            f"ephemeral_public_key={ephemeral_key}",
            f"jwt_token={jwt}",
        )
    )

    safe = sanitize_external_error(raw)
    logger = logging.getLogger("magsync.test.external")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        logger.warning("%s", safe)

    output = caplog.text
    for secret in (
        fragment,
        authorization,
        cookie,
        wrapped_key,
        ephemeral_key,
        credential,
        signature,
        jwt,
    ):
        assert secret not in safe
        assert secret not in output

    assert "operation=download" in safe
    assert "status=403" in safe
    assert "status=401" in safe
    assert "id=AbC12" in safe
    assert "https://limewire.com/d/AbC12" in safe
    assert "https://bucket.example/file.pdf" in safe
    assert "[REDACTED]" in safe


def test_sanitizer_is_bounded_and_removes_log_injection():
    safe = sanitize_external_error("line one\r\nline two " + "x" * 100, max_length=40)
    assert len(safe) == 40
    assert "\r" not in safe
    assert "\n" not in safe
    assert safe.endswith("...")

    assert sanitize_external_error("secret", max_length=0) == ""
    with pytest.raises(ValueError, match="non-negative"):
        sanitize_external_error("secret", max_length=-1)
