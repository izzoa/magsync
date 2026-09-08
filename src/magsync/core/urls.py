"""Strict external URL validation and normalization.

LimeWire URL fragments are decryption-key material, and a VK access hash is an
access token.  Callers may use the normalized full URL as an internal
retry/single-flight identity, but must never write that identity to logs or
persisted diagnostic text.

The source serves download links from more than one file host, so a stored
download URL is validated against a *supported host* rather than one host: see
:func:`normalize_download_url`.  Shared safety rules (HTTPS, no credentials, no
nonstandard port, exact host spelling) live in one place so a lookalike host
cannot pass on any per-host branch.
"""

from __future__ import annotations

import re
from enum import Enum
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit, urlunsplit


LIMEWIRE_HOSTS = frozenset({"limewire.com", "www.limewire.com"})
VK_HOSTS = frozenset({"vk.com", "www.vk.com"})
SOURCE_HOSTS = frozenset({"freemagazines.top", "www.freemagazines.top"})

# VK names a document two ways, and the source's endpoint returns both:
#   /doc<owner>_<id>            - canonical; owner ids are negative for
#                                 community-owned documents
#   /s/v<n>/doc/<token>         - a signed viewer link (the majority for
#                                 older posts)
# Both are *viewer pages* carrying the real file URL, so both are retrievable
# by the same backend. The signed form may expire; that is safe to store
# because a stale link fails as an unavailable share and the existing link
# refresh re-resolves the source's masked key for a fresh one.
_VK_DOC_PATH_RE = re.compile(r"^/doc(-?\d{1,20})_(\d{1,20})$")
_VK_SIGNED_DOC_PATH_RE = re.compile(r"^/s/v\d{1,3}/doc/[A-Za-z0-9_-]{16,128}$")
# The only query parameter a VK document URL may carry. Its value is an access
# token and is treated as secret material.
_VK_ALLOWED_QUERY_KEYS = frozenset({"hash"})


class DownloadHost(str, Enum):
    """A file host magsync can retrieve a payload from.

    Values are stable strings so they are safe to report in counters and
    diagnostics; the host is derived from a URL and never persisted as schema.
    """

    LIMEWIRE = "limewire"
    VK = "vk"


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


def normalize_vk_document_url(url: str) -> str:
    """Validate and return the canonical VK document URL.

    Strict form: HTTPS, exact ``vk.com``/``www.vk.com`` host, a path naming
    exactly one document - either ``/doc<owner>_<id>`` or a signed
    ``/s/v<n>/doc/<token>`` viewer link - and no query beyond an optional
    ``hash`` access token.  Any fragment is dropped: it is not part of a
    document's identity, and rejecting one would risk misclassifying an
    otherwise valid link as an unsupported host.
    """

    parsed = _split_https_url(url, allowed_hosts=VK_HOSTS)

    if not (
        _VK_DOC_PATH_RE.match(parsed.path)
        or _VK_SIGNED_DOC_PATH_RE.match(parsed.path)
    ):
        raise URLValidationError(
            "VK document path must be /doc<owner>_<id> or /s/v<n>/doc/<token>"
        )

    query = parsed.query
    if query:
        pairs = parse_qsl(query, keep_blank_values=True, strict_parsing=False)
        if not pairs or any(key not in _VK_ALLOWED_QUERY_KEYS for key, _ in pairs):
            raise URLValidationError("VK document URL query is not allowed")
        query = urlencode(pairs)

    return urlunsplit(("https", "vk.com", parsed.path, query, ""))


def is_valid_vk_document_url(url: str | None) -> bool:
    """Return whether ``url`` is a strict VK document URL."""

    if url is None:
        return False
    try:
        normalize_vk_document_url(url)
    except (TypeError, URLValidationError):
        return False
    return True


# Per-host strict validators, in dispatch order. Adding a backend is an entry
# here plus a downloader dispatch arm.
_DOWNLOAD_HOSTS: tuple[tuple[DownloadHost, frozenset[str], object], ...] = (
    (DownloadHost.LIMEWIRE, LIMEWIRE_HOSTS, normalize_limewire_share_url),
    (DownloadHost.VK, VK_HOSTS, normalize_vk_document_url),
)


def is_supported_download_host(hostname: str | None) -> bool:
    """Return whether ``hostname`` is a host magsync has a backend for.

    Host membership only: a URL on a supported host may still fail that
    host's strict form, which is a malformed link rather than an unsupported
    destination. Callers need to tell those two apart.
    """

    if not hostname:
        return False
    lowered = hostname.lower()
    return any(lowered in hosts for _host, hosts, _normalizer in _DOWNLOAD_HOSTS)


def download_host_of(url: str) -> DownloadHost:
    """Return which supported host ``url`` belongs to, in its strict form.

    Raises :class:`URLValidationError` when the URL is on no supported host or
    fails that host's strict form. The hostname is matched exactly, so a
    lookalike host can never select a backend.
    """

    if not isinstance(url, str) or not url:
        raise URLValidationError("URL is empty")
    try:
        hostname = (urlsplit(url).hostname or "").lower()
    except (TypeError, ValueError) as exc:
        raise URLValidationError("URL is malformed") from exc

    for host, hosts, normalizer in _DOWNLOAD_HOSTS:
        if hostname in hosts:
            normalizer(url)  # enforce that host's strict form
            return host
    raise URLValidationError("URL host is not a supported download host")


def normalize_download_url(url: str) -> str:
    """Validate and canonicalize a download URL on any supported host."""

    host = download_host_of(url)
    for candidate, _hosts, normalizer in _DOWNLOAD_HOSTS:
        if candidate is host:
            return normalizer(url)
    raise URLValidationError("URL host is not a supported download host")


def is_valid_download_url(url: str | None) -> bool:
    """Return whether ``url`` is a strict URL on some supported download host."""

    if url is None:
        return False
    try:
        normalize_download_url(url)
    except (TypeError, URLValidationError):
        return False
    return True


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
