"""Strict external URL validation and normalization.

LimeWire URL fragments are decryption-key material.  Callers may use the
normalized full URL as an internal retry/single-flight identity, but must never
write that identity to logs or persisted diagnostic text.
"""

from __future__ import annotations

from urllib.parse import SplitResult, urlsplit, urlunsplit


LIMEWIRE_HOSTS = frozenset({"limewire.com", "www.limewire.com"})
SOURCE_HOSTS = frozenset({"freemagazines.top", "www.freemagazines.top"})


class URLValidationError(ValueError):
    """A URL failed strict validation.

    The message contains only a reason, never the rejected URL, so propagating
    this exception cannot disclose a LimeWire fragment or embedded credential.
    """


def _split_https_url(url: str, *, allowed_hosts: frozenset[str]) -> SplitResult:
    if not isinstance(url, str) or not url:
        raise URLValidationError("URL is empty")
    if url != url.strip() or any(character.isspace() for character in url):
        raise URLValidationError("URL contains whitespace")
    if any(ord(character) < 32 or ord(character) == 127 for character in url):
        raise URLValidationError("URL contains control characters")

    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise URLValidationError("URL is malformed") from exc

    if parsed.scheme.lower() != "https":
        raise URLValidationError("URL must use HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise URLValidationError("URL credentials are not allowed")
    if hostname is None or hostname.lower() not in allowed_hosts:
        raise URLValidationError("URL host is not allowed")
    if port not in (None, 443):
        raise URLValidationError("URL port is not allowed")

    # Reject ambiguous authority spellings, including an empty port, trailing
    # dot, percent-encoded host, or non-canonical numeric spelling.  The two
    # accepted hosts and an optional literal :443 are the complete allowlist.
    allowed_authorities = allowed_hosts | frozenset(f"{host}:443" for host in allowed_hosts)
    if parsed.netloc.lower() not in allowed_authorities:
        raise URLValidationError("URL authority is not allowed")
    return parsed


def normalize_limewire_share_url(url: str) -> str:
    """Validate and return the canonical full LimeWire share identity.

    The allowed host spellings and explicit default port are canonicalized to
    ``limewire.com``.  Sharing-id case and every character after the first
    ``#`` are preserved byte-for-byte.
    """

    parsed = _split_https_url(url, allowed_hosts=LIMEWIRE_HOSTS)

    # ``SplitResult.query`` cannot distinguish no query from a bare ``?``.
    # Any query delimiter before the fragment is forbidden for share URLs.
    before_fragment = url.split("#", 1)[0]
    if "?" in before_fragment:
        raise URLValidationError("LimeWire share URL query is not allowed")

    if not parsed.path.startswith("/d/") or parsed.path.count("/") != 2:
        raise URLValidationError("LimeWire share path must be /d/<id>")
    sharing_id = parsed.path[len("/d/") :]
    if not sharing_id:
        raise URLValidationError("LimeWire sharing id is empty")

    if "#" not in url:
        raise URLValidationError("LimeWire share fragment is required")
    fragment = url.split("#", 1)[1]
    if not fragment:
        raise URLValidationError("LimeWire share fragment is empty")

    return f"https://limewire.com/d/{sharing_id}#{fragment}"


def validate_limewire_share_url(url: str) -> str:
    """Validate a LimeWire share URL and return its normalized identity."""

    return normalize_limewire_share_url(url)


def is_valid_limewire_share_url(url: str | None) -> bool:
    """Return whether ``url`` is a strict LimeWire share URL."""

    if url is None:
        return False
    try:
        normalize_limewire_share_url(url)
    except (TypeError, URLValidationError):
        return False
    return True


def limewire_sharing_id(url: str) -> str:
    """Return the validated sharing id, safe to use as diagnostic context."""

    normalized = normalize_limewire_share_url(url)
    return urlsplit(normalized).path[len("/d/") :]


def normalize_source_url(url: str) -> str:
    """Validate and canonicalize a freemagazines.top URL.

    Paths, queries, and fragments are retained; only the allowed host spelling
    and explicit default port are canonicalized.  This makes the function
    suitable for both response final-URL checks and source request URLs.
    """

    parsed = _split_https_url(url, allowed_hosts=SOURCE_HOSTS)
    path = parsed.path or "/"
    return urlunsplit(("https", "freemagazines.top", path, parsed.query, parsed.fragment))


def validate_source_origin(url: str) -> str:
    """Validate a source URL's HTTPS origin and return its normalized URL."""

    return normalize_source_url(url)


def is_valid_source_url(url: str | None) -> bool:
    """Return whether ``url`` has a strict allowed source origin."""

    if url is None:
        return False
    try:
        normalize_source_url(url)
    except (TypeError, URLValidationError):
        return False
    return True


# Explicit aliases keep call sites readable when working specifically with the
# freemagazines provider rather than a generic source boundary.
normalize_freemagazines_url = normalize_source_url
validate_freemagazines_url = validate_source_origin
