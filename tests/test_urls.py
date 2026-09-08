"""Tests for strict shared external URL validation."""

from __future__ import annotations

import pytest

from magsync.core.diagnostics import sanitize_external_error
from magsync.core.urls import (
    DownloadHost,
    URLValidationError,
    download_host_of,
    is_valid_download_url,
    is_valid_limewire_share_url,
    is_valid_source_url,
    is_valid_vk_document_url,
    limewire_sharing_id,
    normalize_download_url,
    normalize_limewire_share_url,
    normalize_source_url,
    normalize_vk_document_url,
    validate_limewire_share_url,
    validate_source_origin,
)


@pytest.mark.parametrize(
    ("candidate", "normalized"),
    (
        (
            "https://limewire.com/d/AbC12#key-material",
            "https://limewire.com/d/AbC12#key-material",
        ),
        (
            "https://www.limewire.com/d/AbC12#key-material",
            "https://limewire.com/d/AbC12#key-material",
        ),
        (
            "https://LIMEWIRE.COM:443/d/CaseSensitive#Ab%2F-_~+/%2f",
            "https://limewire.com/d/CaseSensitive#Ab%2F-_~+/%2f",
        ),
    ),
)
def test_limewire_normalization_accepts_only_canonical_hosts(candidate, normalized):
    assert normalize_limewire_share_url(candidate) == normalized
    assert validate_limewire_share_url(candidate) == normalized
    assert is_valid_limewire_share_url(candidate)


def test_limewire_identity_preserves_id_case_and_exact_fragment_bytes():
    first = normalize_limewire_share_url(
        "https://www.limewire.com:443/d/AbC#Key%2FBytes+One"
    )
    second = normalize_limewire_share_url(
        "https://limewire.com/d/AbC#Key%2fBytes+One"
    )
    different = normalize_limewire_share_url(
        "https://limewire.com/d/AbC#Key%2FBytes+Two"
    )

    assert first == "https://limewire.com/d/AbC#Key%2FBytes+One"
    assert second == "https://limewire.com/d/AbC#Key%2fBytes+One"
    assert first != second  # percent-encoding bytes/case are never rewritten
    assert first != different  # same share id, different key identity
    assert limewire_sharing_id(first) == "AbC"


@pytest.mark.parametrize(
    "candidate",
    (
        "http://limewire.com/d/x#key",
        "https://notlimewire.com/d/x#key",
        "https://limewire.com.evil.test/d/x#key",
        "https://evil.test/https://limewire.com/d/x#key",
        "https://user@limewire.com/d/x#key",
        "https://limewire.com:8443/d/x#key",
        "https://limewire.com:/d/x#key",
        "https://limewire.com./d/x#key",
        "https://limewire.com/d/#key",
        "https://limewire.com/d/x/y#key",
        "https://limewire.com/D/x#key",
        "https://limewire.com/d/x",
        "https://limewire.com/d/x#",
        "https://limewire.com/d/x?download=1#key",
        "https://limewire.com/d/x?#key",
        " https://limewire.com/d/x#key",
        "https://limewire.com/d/x#key with space",
        "",
    ),
)
def test_limewire_validation_rejects_unsafe_or_ambiguous_forms(candidate):
    assert not is_valid_limewire_share_url(candidate)
    with pytest.raises(URLValidationError):
        normalize_limewire_share_url(candidate)


def test_invalid_url_exception_does_not_echo_secret_fragment():
    secret = "DO_NOT_ECHO_THIS_FRAGMENT"
    with pytest.raises(URLValidationError) as caught:
        normalize_limewire_share_url(f"https://evil.test/d/x#{secret}")
    assert secret not in str(caught.value)


@pytest.mark.parametrize(
    ("candidate", "normalized"),
    (
        ("https://freemagazines.top", "https://freemagazines.top/"),
        (
            "https://www.freemagazines.top:443/page/2/?s=Food%20Wine",
            "https://freemagazines.top/page/2/?s=Food%20Wine",
        ),
    ),
)
def test_source_origin_normalization(candidate, normalized):
    assert normalize_source_url(candidate) == normalized
    assert validate_source_origin(candidate) == normalized
    assert is_valid_source_url(candidate)


@pytest.mark.parametrize(
    "candidate",
    (
        "http://freemagazines.top/",
        "https://notfreemagazines.top/",
        "https://freemagazines.top.evil.test/",
        "https://user:password@freemagazines.top/",
        "https://freemagazines.top:444/",
        "https://freemagazines.top./",
    ),
)
def test_source_origin_rejects_unsafe_forms(candidate):
    assert not is_valid_source_url(candidate)
    with pytest.raises(URLValidationError):
        validate_source_origin(candidate)


# ---------------------------------------------------------------------------
# VK document URLs and supported-host dispatch
# ---------------------------------------------------------------------------

VK_DOC = "https://vk.com/doc711807114_676564963"


@pytest.mark.parametrize(
    ("candidate", "normalized"),
    (
        (VK_DOC, VK_DOC),
        ("https://www.vk.com/doc711807114_676564963", VK_DOC),
        ("https://VK.COM:443/doc711807114_676564963", VK_DOC),
        # Community-owned documents carry a negative owner id.
        ("https://vk.com/doc-12345_678", "https://vk.com/doc-12345_678"),
        # The one permitted query parameter is preserved.
        (
            "https://vk.com/doc711807114_676564963?hash=abc123",
            "https://vk.com/doc711807114_676564963?hash=abc123",
        ),
        # A fragment is not part of document identity and is dropped.
        ("https://vk.com/doc711807114_676564963#frag", VK_DOC),
    ),
)
def test_vk_normalization_accepts_only_canonical_forms(candidate, normalized):
    assert normalize_vk_document_url(candidate) == normalized
    assert is_valid_vk_document_url(candidate)
    assert normalize_download_url(candidate) == normalized


@pytest.mark.parametrize(
    "candidate",
    (
        "http://vk.com/doc1_2",
        "https://notvk.com/doc1_2",
        "https://vk.com.evil.test/doc1_2",
        "https://evil.test/https://vk.com/doc1_2",
        "https://user@vk.com/doc1_2",
        "https://vk.com:8443/doc1_2",
        "https://vk.com:/doc1_2",
        "https://vk.com./doc1_2",
        "https://vk.com/doc1_2?dl=1",
        "https://vk.com/doc1_2?hash=a&dl=1",
        "https://vk.com/audio1_2",
        "https://vk.com/doc1",
        "https://vk.com/doc1_2_3",
        "https://vk.com/doc1_2/extra",
        "https://vk.com/docabc_def",
        # The signed CDN URL is never a storable identity.
        "https://psv4.userapi.com/s/v1/d/abc/File.pdf",
        " https://vk.com/doc1_2",
        "",
    ),
)
def test_vk_validation_rejects_unsafe_or_ambiguous_forms(candidate):
    assert not is_valid_vk_document_url(candidate)
    assert not is_valid_download_url(candidate)
    with pytest.raises(URLValidationError):
        normalize_vk_document_url(candidate)


def test_download_host_dispatch_selects_the_right_backend():
    assert download_host_of("https://limewire.com/d/AbC12#key") is DownloadHost.LIMEWIRE
    assert download_host_of(VK_DOC) is DownloadHost.VK


@pytest.mark.parametrize(
    "candidate",
    (
        "https://psv4.userapi.com/s/v1/d/abc/File.pdf",
        "https://mega.nz/file/abc#key",
        "https://limewire.com.evil.test/d/x#key",
        "https://vk.com.evil.test/doc1_2",
        "",
    ),
)
def test_unsupported_host_is_refused_by_dispatch(candidate):
    assert not is_valid_download_url(candidate)
    with pytest.raises(URLValidationError):
        download_host_of(candidate)


def test_dispatch_still_enforces_each_hosts_strict_form():
    # Right host, wrong form: must not be routed to that host's backend.
    for candidate in (
        "https://limewire.com/d/x",  # no fragment
        "https://limewire.com/d/x/y#key",
        "https://vk.com/doc1_2?dl=1",
        "https://vk.com/audio1_2",
    ):
        with pytest.raises(URLValidationError):
            download_host_of(candidate)


def test_secret_material_is_redacted_from_diagnostics():
    # A LimeWire fragment is key material; a VK access hash is an access token.
    fragment = "LIMEWIRE_KEY_MATERIAL_XYZ"
    access_hash = "VK_ACCESS_HASH_XYZ"
    limewire = f"https://limewire.com/d/AbC12#{fragment}"
    vk = f"{VK_DOC}?hash={access_hash}"

    for text in (
        sanitize_external_error(f"failed for {limewire}"),
        sanitize_external_error(f"failed for {vk}"),
    ):
        assert fragment not in text
        assert access_hash not in text


def test_vk_validation_error_does_not_echo_access_hash():
    access_hash = "DO_NOT_ECHO_THIS_HASH"
    with pytest.raises(URLValidationError) as caught:
        normalize_vk_document_url(f"https://evil.test/doc1_2?hash={access_hash}")
    assert access_hash not in str(caught.value)
