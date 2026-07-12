"""Secret-safe helpers for external diagnostics."""

from __future__ import annotations

import re
from urllib.parse import urlsplit


DEFAULT_EXTERNAL_ERROR_LIMIT = 500
_REDACTED = "[REDACTED]"

# Match URLs before key/value patterns so every query and fragment is removed
# wholesale, including provider-specific credential names not yet known here.
_URL_RE = re.compile(r"https?://[^\s<>]+", re.IGNORECASE)

_PEM_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN [^-\r\n]*PRIVATE KEY-----.*?-----END [^-\r\n]*PRIVATE KEY-----",
    re.IGNORECASE | re.DOTALL,
)

_SENSITIVE_LABEL = r"(?:" + "|".join(
    (
        r"authorization",
        r"proxy[-_]?authorization",
        r"cookie",
        r"set[-_]?cookie",
        r"jwt(?:[-_]?token)?",
        r"(?:access|refresh|id)[-_]?token",
        r"csrf(?:[-_]?token)?",
        r"x[-_]?csrf[-_]?token",
        r"self[-_]?csrf",
        r"api[-_]?key",
        r"x[-_]?api[-_]?key",
        r"client[-_]?secret",
        r"password",
        r"passphrase",
        r"passphrase[-_]?wrapped[-_]?(?:pk|private[-_]?key)",
        r"wrapped[-_]?key",
        r"raw[-_]?key",
        r"private[-_]?key",
        r"encrypted[-_]?private[-_]?key",
        r"ephemeral[-_]?public[-_]?key",
        r"(?:file[-_]?)?encryption[-_]?key",
        r"key[-_]?material",
        r"sharing[-_]?salt",
        r"fragment",
        r"secret",
        r"credential",
        r"signature",
        r"x[-_]?amz[-_]?(?:credential|signature|security[-_]?token)",
        r"aws[-_]?access[-_]?key[-_]?id",
        r"google[-_]?access[-_]?id",
    )
) + r")"

# Header-shaped values may contain spaces (for example ``Bearer <token>``) or
# cookie separators, so redact through the end of the line.  This deliberately
# favors secrecy over retaining unrelated text on a malformed header line.
_SENSITIVE_HEADER_RE = re.compile(
    rf"\b(?P<label>{_SENSITIVE_LABEL})\b[\"']?\s*:\s*[^\r\n]+",
    re.IGNORECASE,
)

_AUTHORIZATION_VALUE_RE = re.compile(
    r"\b(?P<label>(?:proxy[-_]?authorization|authorization))\b[\"']?\s*[:=]\s*"
    r"(?:(?:bearer|basic)\s+)?[^\s,;]+",
    re.IGNORECASE,
)
_COOKIE_VALUE_RE = re.compile(
    r"\b(?P<label>(?:set[-_]?cookie|cookie))\b[\"']?\s*[:=]\s*[^\r\n,]+",
    re.IGNORECASE,
)
_BARE_AUTH_RE = re.compile(
    r"\b(?P<label>bearer|basic)\s+[^\s,;]+",
    re.IGNORECASE,
)

# Assignment-shaped values stop at conventional field delimiters.  Quoted
# JSON values are handled without exposing their content.
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    rf"\b(?P<label>{_SENSITIVE_LABEL})\b[\"']?\s*=\s*"
    r"(?:\"[^\"]*\"|'[^']*'|[^\s,;&]+)",
    re.IGNORECASE,
)

_SENSITIVE_JSON_RE = re.compile(
    rf"(?P<quote>[\"'])(?P<label>{_SENSITIVE_LABEL})(?P=quote)\s*:\s*"
    r"(?:\"[^\"]*\"|'[^']*'|[^\s,;}]+)",
    re.IGNORECASE,
)

# Catch common prose forms such as ``wrapped key is <blob>`` after structured
# assignment forms have been handled.
_SENSITIVE_PROSE_RE = re.compile(
    r"\b(?P<label>(?:wrapped|raw|private|ephemeral public|encryption) key(?: material)?)"
    r"\s+(?:is\s+)?(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)",
    re.IGNORECASE,
)

_JWT_RE = re.compile(
    r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\b"
)
_KNOWN_TOKEN_RE = re.compile(
    r"\b(?:AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{20,}|gh[pousr]_[0-9A-Za-z]{20,})\b"
)


def _sanitize_url(match: re.Match[str]) -> str:
    """Retain only safe scheme/host/path context from one matched URL."""

    raw_url = match.group(0)
    try:
        parsed = urlsplit(raw_url)
        hostname = parsed.hostname
        port = parsed.port
    except (TypeError, ValueError):
        return "[REDACTED-URL]"
    if not hostname:
        return "[REDACTED-URL]"

    scheme = parsed.scheme.lower()
    host = hostname.lower()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if port is not None:
        host = f"{host}:{port}"
    path = parsed.path or "/"
    return f"{scheme}://{host}{path}"


def _replace_labeled_secret(match: re.Match[str]) -> str:
    label = match.group("label")
    return f"{label}={_REDACTED}"


def sanitize_external_error(
    value: object,
    max_length: int = DEFAULT_EXTERNAL_ERROR_LIMIT,
) -> str:
    """Return bounded external error text with credential material removed.

    Safe operation/status/host/path/id wording remains useful for diagnosis.
    URL query strings and fragments are always discarded, regardless of their
    parameter names.  The helper is deterministic and safe to use before
    logging, callbacks, terminal display, or SQLite persistence.
    """

    if max_length < 0:
        raise ValueError("max_length must be non-negative")
    if value is None or max_length == 0:
        return ""

    text = str(value)
    text = _PEM_PRIVATE_KEY_RE.sub("[REDACTED-PRIVATE-KEY]", text)
    text = _URL_RE.sub(_sanitize_url, text)
    text = _SENSITIVE_JSON_RE.sub(_replace_labeled_secret, text)
    text = _SENSITIVE_HEADER_RE.sub(_replace_labeled_secret, text)
    text = _AUTHORIZATION_VALUE_RE.sub(_replace_labeled_secret, text)
    text = _COOKIE_VALUE_RE.sub(_replace_labeled_secret, text)
    text = _SENSITIVE_ASSIGNMENT_RE.sub(_replace_labeled_secret, text)
    text = _SENSITIVE_PROSE_RE.sub(_replace_labeled_secret, text)
    text = _BARE_AUTH_RE.sub(_replace_labeled_secret, text)
    text = _JWT_RE.sub(_REDACTED, text)
    text = _KNOWN_TOKEN_RE.sub(_REDACTED, text)

    # Prevent external input from creating extra log lines and normalize other
    # control characters before applying the storage/display bound.
    text = " ".join(text.split())
    if len(text) <= max_length:
        return text
    if max_length <= 3:
        return text[:max_length]
    return text[: max_length - 3] + "..."
