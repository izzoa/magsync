"""Tests for strict shared external URL validation."""

from __future__ import annotations

import pytest

from magsync.core.urls import (
    URLValidationError,
    is_valid_limewire_share_url,
    is_valid_source_url,
    limewire_sharing_id,
    normalize_limewire_share_url,
    normalize_source_url,
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
