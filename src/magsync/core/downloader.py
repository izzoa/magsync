"""LimeWire download and decryption pipeline.

Implements the full E2E decryption chain:
URL fragment → PBKDF2 → AES-KW unwrap → ECDH P-256 → AES-256-CTR decrypt
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
from pathlib import Path
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("magsync")
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.keywrap import aes_key_unwrap

from magsync.config import LimeWireConstants, load_config, save_config
from magsync.core.models import DownloadResult, LimeWireSession

# Regex for finding UUIDs in SSR streaming data
UUID_RE = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

# Session establishment retries transient errors at least this many times even
# when retry_attempts=0 — a transient SSR/infra hiccup is not a download failure.
_MIN_SESSION_RETRIES = 2
# Shared pause (seconds) when LimeWire signals a transient throttle.
_SSR_THROTTLE_PAUSE = 20


def _part_path_for(dest: Path, limewire_url: str) -> Path:
    """Return the resume ``.part`` path for this dest+URL, removing stale partials.

    Partials are keyed to the exact share URL (including the #fragment key) via
    a hash-prefix in the filename, so a rotated link never resumes bytes fetched
    for a different blob/key — splicing ciphertexts would decrypt to garbage and
    masquerade as a stale-constants failure. Partials keyed to any other URL,
    and legacy un-fingerprinted ``<dest>.part`` files, are deleted here.
    """
    fp = hashlib.sha256(limewire_url.encode("utf-8")).hexdigest()[:8]
    part_path = dest.parent / f"{dest.name}.{fp}.part"
    if dest.parent.is_dir():
        for existing in dest.parent.iterdir():
            name = existing.name
            if name == part_path.name:
                continue
            if name.startswith(dest.name + ".") and name.endswith(".part"):
                existing.unlink(missing_ok=True)
    return part_path


def _cleanup_part(part_path: Path) -> None:
    """Best-effort ``.part`` removal for terminal outcomes.

    A filesystem error here must never change the download outcome — an
    unsupported skip whose cleanup failed would otherwise resurface as a
    retryable failure and resume the churn it exists to stop.
    """
    try:
        part_path.unlink(missing_ok=True)
    except OSError as e:
        logger.warning(f"Could not remove partial file {part_path}: {e}")


# Share file-name extensions that can never be PDF payloads: skipped before any
# payload bytes are requested. Deliberately a blocklist — an allowlist would
# falsely skip PDFs with dotty names (e.g. "Issue 06.6.2026" → suffix ".2026").
_NON_PDF_EXTENSIONS = frozenset({
    ".zip", ".rar", ".7z", ".gz", ".tar",
    ".mp3", ".m4a", ".m4b", ".mp4", ".m4v", ".mkv", ".avi",
    ".wav", ".flac", ".aac", ".ogg",
    ".epub", ".mobi", ".azw", ".azw3", ".djvu", ".cbz", ".cbr",
    ".txt", ".docx", ".png", ".jpg", ".jpeg", ".gif",
})

# Magic numbers of known non-PDF containers. AES-CTR is unauthenticated, so
# classification is heuristic; every entry is ≥3 bytes to keep the
# false-positive rate on wrong-key garbage below ~1/16M (a bare 2-byte gzip
# magic would be 1/65k, raw MP3 frame-sync ~1/2048 — both excluded).
_NON_PDF_MAGICS = (
    b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08",  # zip (also epub/cbz)
    b"Rar!\x1a\x07",
    b"7z\xbc\xaf\x27\x1c",
    b"\x1f\x8b\x08",  # gzip, incl. deflate method byte
    b"ID3",           # mp3 with ID3 tag
    b"OggS",
)


def _classify_payload(head: bytes) -> str:
    """Classify decrypted output by magic number: "pdf" | "unsupported" | "unknown"."""
    if head[:4] == b"%PDF":
        return "pdf"
    if any(head.startswith(magic) for magic in _NON_PDF_MAGICS):
        return "unsupported"
    if len(head) >= 8 and head[4:8] == b"ftyp":  # mp4/m4a/m4b container
        return "unsupported"
    return "unknown"


_CONTENT_RANGE_RE = re.compile(r"bytes\s+(?:(\d+)-\d+|\*)/(\d+|\*)")


def _content_range_parts(value: str | None) -> tuple[int | None, int | None]:
    """(start, total) from a Content-Range header; None for absent/'*' parts."""
    if not value:
        return None, None
    m = _CONTENT_RANGE_RE.match(value.strip())
    if not m:
        return None, None
    start = int(m.group(1)) if m.group(1) is not None else None
    total = int(m.group(2)) if m.group(2) != "*" else None
    return start, total


def _content_range_total(value: str | None) -> int | None:
    """Total from a Content-Range header ("bytes */N" or "bytes S-E/N")."""
    return _content_range_parts(value)[1]


def parse_limewire_url(url: str) -> tuple[str, str]:
    """Parse a LimeWire share URL into (sharing_id, fragment/passphrase)."""
    parsed = urlparse(url)
    sharing_id = parsed.path.split("/")[-1]
    fragment = parsed.fragment
    if not sharing_id or not fragment:
        raise ValueError(f"Invalid LimeWire URL: {url}")
    return sharing_id, fragment


def _ssr_field(html: str, field: str) -> str | None:
    """Extract a string value from React Router SSR streaming data."""
    pattern = r'\\"' + re.escape(field) + r'\\",\\"([^"\\]+)\\"'
    m = re.search(pattern, html)
    return m.group(1) if m else None


def _is_removed_share(html: str) -> bool:
    """True if the SSR marks this share removed/sanitized (permanently dead).

    LimeWire carries a removed-share error inside the share's
    `sharingBucketContentData` result (an `error` tuple naming `SanitizedError`),
    in either the current escaped streaming format
    (`\\"...\\",\\"ok\\",false,\\"error\\",[\\"SanitizedError\\",...]`) or the
    legacy JSON shape. Anchored to a bounded window after the (format-agnostic)
    marker so an unrelated `SanitizedError` elsewhere on the page can't cause a
    false positive.
    """
    idx = html.rfind("sharingBucketContentData")
    if idx == -1:
        return False
    window = html[idx : idx + 400]
    return re.search(r"error.{0,40}SanitizedError", window) is not None


_ENQUEUE_RE = re.compile(r'streamController\.enqueue\(')


def _decode_react_stream(html: str):
    """Decode LimeWire's React Router turbo-stream SSR into a nested structure.

    The page ships its loader data as one or more ``streamController.enqueue("…")``
    string literals that concatenate into a single flattened array. The array is
    reference-encoded: an object ``{"_K": V}`` maps key ``arr[K]`` (a string) to
    ``resolve(V)``; a list is either a tagged value (``["D", ms]`` → a date) or a
    plain array whose integer elements are indices to resolve and whose
    non-integer elements are literals; a primitive lives at its slot and is
    returned as-is. Negative and out-of-range indices resolve to ``None``.
    Resolution is memoized and cycle-guarded. Returns the resolved root
    (``arr[0]``), or ``None`` when no enqueue payload is present.
    """
    chunks: list[str] = []
    for m in _ENQUEUE_RE.finditer(html):
        # The argument is a JS double-quoted string literal; scan to its close.
        i = m.end()
        if i >= len(html) or html[i] != '"':
            continue
        i += 1
        start = i
        while i < len(html):
            c = html[i]
            if c == "\\":
                i += 2
                continue
            if c == '"':
                break
            i += 1
        try:
            chunks.append(json.loads('"' + html[start:i] + '"'))
        except (ValueError, json.JSONDecodeError):
            continue
    if not chunks:
        return None
    try:
        arr = json.loads("".join(chunks))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(arr, list) or not arr:
        return None

    memo: dict[int, object] = {}

    def resolve(index, active: frozenset):
        if not isinstance(index, int) or index < 0 or index >= len(arr):
            return None
        if index in memo:
            return memo[index]
        if index in active:            # cycle
            return None
        node = arr[index]
        if isinstance(node, dict):
            active2 = active | {index}
            out = {}
            for k, v in node.items():
                key = arr[int(k[1:])] if isinstance(k, str) and k.startswith("_") else k
                out[key] = resolve(v, active2) if isinstance(v, int) else v
            memo[index] = out
            return out
        if isinstance(node, list):
            if node and isinstance(node[0], str):    # tagged value (e.g. ["D", ms])
                memo[index] = node[1] if (node[0] == "D" and len(node) > 1) else node
                return memo[index]
            active2 = active | {index}
            out = [resolve(e, active2) if isinstance(e, int) else e for e in node]
            memo[index] = out
            return out
        memo[index] = node
        return node

    return resolve(0, frozenset())


def _find_key(obj, target):
    """Depth-first search for the first value under ``target`` in a nested dict/list."""
    if isinstance(obj, dict):
        if target in obj:
            return obj[target]
        for v in obj.values():
            found = _find_key(v, target)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for e in obj:
            found = _find_key(e, target)
            if found is not None:
                return found
    return None


# Sentinel distinguishing "share is removed" from "no decodable container".
_REMOVED = object()


def _extract_share_metadata(decoded):
    """Pull share metadata from a decoded turbo-stream.

    Returns a dict of metadata (``bucket_id`` from ``sharingBucket.id``,
    ``content_item_id``/``ephemeral_public_key`` from ``contentItemList[0]``, the
    passphrase-wrapped key from the ``fileEncryptionKeys`` entry matching the
    content item's ``baseFileEncryptionKeyId``, plus file name/size), or
    ``_REMOVED`` when the container is explicitly ``ok: false`` (a removed
    share), or ``None`` when no usable container is found (absent or malformed —
    never treated as removed, so a format drift can't misclassify a live share).
    """
    if decoded is None:
        return None
    container = _find_key(decoded, "sharingBucketContentData")
    if not isinstance(container, dict) or "ok" not in container:
        return None
    if container["ok"] is False:
        return _REMOVED
    if container["ok"] is not True:
        return None
    value = container.get("value")
    if not isinstance(value, dict):
        return None

    bucket = value.get("sharingBucket") or {}
    items = value.get("contentItemList") or []
    keys = value.get("fileEncryptionKeys") or []
    item = items[0] if items else {}

    wrapped = None
    if keys:
        base_key_id = item.get("baseFileEncryptionKeyId")
        match = next((k for k in keys if k.get("id") == base_key_id), keys[0])
        wrapped = match.get("passphraseWrappedPrivateKey")

    size = bucket.get("totalFileSize")
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        size = 0  # advisory only — a malformed SSR size must not drive decisions

    return {
        "bucket_id": bucket.get("id"),
        "content_item_id": item.get("id"),
        "ephemeral_public_key": item.get("ephemeralPublicKey"),
        "passphrase_wrapped_pk": wrapped,
        "file_name": bucket.get("name") or "",
        "file_size": size,
    }


def _extract_ssr_metadata_regex(html: str, sharing_id: str) -> dict:
    """Legacy text-position extraction, kept as a fallback for stream-shape drift.

    Used only when no decodable turbo-stream container is found. This path is
    fragile (it matches UUIDs by position) and MUST NOT override ids resolved
    from a present stream — see ``establish_session``.
    """
    if _is_uuid(sharing_id):
        bucket_id = sharing_id
    else:
        sb_idx = html.find("sharingBucket")
        bucket_match = re.search(UUID_RE, html[sb_idx:]) if sb_idx > -1 else None
        bucket_id = bucket_match.group(0) if bucket_match else None
    ci = re.search(r"contentItemIds.*?(" + UUID_RE + ")", html)
    size_match = re.search(r'"totalFileSize",(\d+)', html)
    return {
        "bucket_id": bucket_id,
        "content_item_id": ci.group(1) if ci else None,
        "ephemeral_public_key": _ssr_field(html, "ephemeralPublicKey"),
        "passphrase_wrapped_pk": _ssr_field(html, "passphraseWrappedPrivateKey"),
        "file_name": _ssr_field(html, "name") or "",
        "file_size": int(size_match.group(1)) if size_match else 0,
    }


async def establish_session(
    limewire_url: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> LimeWireSession:
    """Establish a LimeWire session by visiting a share page.

    Extracts JWT, CSRF token, and SSR metadata from the page response.
    """
    sharing_id, _ = parse_limewire_url(limewire_url)

    should_close = client is None
    if client is None:
        client = httpx.AsyncClient(follow_redirects=True, timeout=60.0, headers={"User-Agent": "Mozilla/5.0"})

    try:
        resp = await client.get(f"https://limewire.com/d/{sharing_id}")
        resp.raise_for_status()
        html = resp.text
        logger.debug(f"LimeWire page response: {resp.status_code}, {len(html)} bytes, sharing_id={sharing_id}")

        # Classify SSR errors BEFORE extracting JWT/CSRF, so a removed page that
        # omits the auth cookie is still classified correctly. Order matters:
        #   Permanent: a removed/sanitized share, caught two ways — the raw-HTML
        #     fast path, then a structural backstop (decoded container ok:false)
        #     for markers that fall outside the raw detector's window. Removed
        #     must take precedence over the generic transient message.
        #   Transient: "Unexpected Server Error" with no removed marker — SSR
        #     backend hiccup (rate limit, bot challenge, datacenter IP). Retry.
        decoded = _decode_react_stream(html)
        meta = _extract_share_metadata(decoded)
        if _is_removed_share(html) or meta is _REMOVED:
            # INFO, not ERROR: a removed share is an expected outcome during bulk
            # retries (the raise below carries the failure). Interactive commands
            # hide INFO by default; the daemon (INFO) still logs it.
            logger.info(
                f"LimeWire SSR: removed/sanitized share "
                f"(sharing_id={sharing_id}, {len(html)} bytes) — link is dead"
            )
            raise RuntimeError("LimeWire share link is unavailable (removed or expired)")
        if "Unexpected Server Error" in html:
            logger.warning(
                f"LimeWire SSR: transient 'Unexpected Server Error' "
                f"(sharing_id={sharing_id}, {len(html)} bytes)"
            )
            raise RuntimeError("LimeWire SSR returned transient server error")

        jwt_token = client.cookies.get("production_access_token")
        if not jwt_token:
            raise RuntimeError("Failed to obtain JWT from LimeWire")

        # Decode JWT payload to get CSRF token
        payload_b64 = jwt_token.split(".")[1] + "==="
        jwt_payload = json.loads(base64.b64decode(payload_b64))
        csrf_token = jwt_payload["csrfToken"]

        # Structural extraction from the turbo-stream. Fall back to the legacy
        # regex path ONLY when no container decoded (whole SSR format changed) —
        # never let it substitute ids for a present-but-incomplete stream, since
        # the regex path matches the wrong (decoy) UUIDs on the current format.
        if meta is None:
            meta = _extract_ssr_metadata_regex(html, sharing_id)

        # UUID-format shares keep sharing_id as the bucket (current behavior);
        # short-ID shares use the decoded sharingBucket.id.
        if _is_uuid(sharing_id):
            meta["bucket_id"] = sharing_id

        # Required ids; the passphrase-wrapped key is required only on the
        # short-ID (passphrase) path — UUID shares derive from the raw fragment.
        missing = [f for f in ("bucket_id", "content_item_id", "ephemeral_public_key") if not meta.get(f)]
        if not _is_uuid(sharing_id) and not meta.get("passphrase_wrapped_pk"):
            missing.append("passphrase_wrapped_pk")
        if missing:
            raise RuntimeError(f"Failed to extract SSR metadata from LimeWire page (missing: {', '.join(missing)})")

        return LimeWireSession(
            jwt_token=jwt_token,
            csrf_token=csrf_token,
            bucket_id=meta["bucket_id"],
            content_item_id=meta["content_item_id"],
            passphrase_wrapped_pk=meta.get("passphrase_wrapped_pk") or "",
            ephemeral_public_key=meta["ephemeral_public_key"],
            file_name=meta.get("file_name") or "",
            file_size=meta.get("file_size") or 0,
        )
    finally:
        if should_close:
            await client.aclose()


def _is_uuid(s: str) -> bool:
    """Check if a string is a UUID (36 chars with dashes)."""
    return len(s) == 36 and s.count("-") == 4


def _b64url_decode(s: str) -> bytes:
    """Decode a base64url string (no padding)."""
    s = s.replace("-", "+").replace("_", "/")
    s += "=" * (4 - len(s) % 4)
    return base64.b64decode(s)


def derive_aes_key(
    sharing_id: str,
    fragment: str,
    passphrase_wrapped_pk_b64: str | None,
    ephemeral_public_key_b64: str,
    constants: LimeWireConstants,
) -> bytes:
    """Derive the AES-256 decryption key from a URL fragment.

    Two paths depending on sharing_id format:
    - Short ID (e.g., "bjAa5"): fragment is a passphrase →
      PBKDF2 → AES-KW unwrap → ECDH → AES key
    - UUID (e.g., "7d08450d-..."): fragment is the raw private key (base64url) →
      ECDH → AES key
    """
    if _is_uuid(sharing_id) or not passphrase_wrapped_pk_b64:
        # Fragment is the raw ECDH private key in base64url
        raw_private_key = _b64url_decode(fragment)
    else:
        # Fragment is a passphrase — derive wrapping key and unwrap
        salt = base64.b64decode(constants.sharing_salt_b64)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=constants.pbkdf2_iterations,
        )
        wrapping_key = kdf.derive(fragment.encode("utf-8"))
        wrapped_pk = base64.b64decode(passphrase_wrapped_pk_b64)
        raw_private_key = aes_key_unwrap(wrapping_key, wrapped_pk)

    # Build ECDH P-256 private key and derive shared secret
    d_int = int.from_bytes(raw_private_key, "big")
    private_key = ec.derive_private_key(d_int, ec.SECP256R1())
    ephemeral_pub_bytes = base64.b64decode(ephemeral_public_key_b64)
    ephemeral_public_key = ec.EllipticCurvePublicKey.from_encoded_point(
        ec.SECP256R1(), ephemeral_pub_bytes
    )
    shared_secret = private_key.exchange(ec.ECDH(), ephemeral_public_key)

    return shared_secret  # 32 bytes = AES-256 key


async def get_download_url(
    session: LimeWireSession,
    *,
    client: httpx.AsyncClient | None = None,
) -> str:
    """Get a presigned S3 download URL from the LimeWire API."""
    should_close = client is None
    if client is None:
        client = httpx.AsyncClient(follow_redirects=True, timeout=60.0, headers={"User-Agent": "Mozilla/5.0"})

    try:
        resp = await client.post(
            f"https://api.limewire.com/sharing/download/{session.bucket_id}",
            headers={
                "X-CSRF-Token": session.csrf_token,
                "Authorization": f"Bearer {session.jwt_token}",
                "Content-Type": "application/json",
            },
            json={"contentItems": [{"id": session.content_item_id}]},
        )
        # A 404 here means the bucket no longer exists → the share is gone.
        # Treat as permanent (UNAVAILABLE) rather than a retryable HTTP error.
        if resp.status_code == 404:
            raise RuntimeError("LimeWire share link is unavailable (removed or expired)")
        resp.raise_for_status()
        data = resp.json()
        items = data.get("contentItems", [])
        if not items:
            raise RuntimeError("No download URLs returned from LimeWire API")
        return items[0]["downloadUrl"]
    finally:
        if should_close:
            await client.aclose()


def decrypt_file(encrypted_data: bytes, aes_key: bytes, constants: LimeWireConstants) -> bytes:
    """Decrypt file content using AES-256-CTR."""
    iv_bytes = base64.b64decode(constants.file_iv_b64)
    nonce = bytearray(16)
    nonce[: len(iv_bytes)] = iv_bytes

    cipher = Cipher(algorithms.AES(aes_key), modes.CTR(bytes(nonce)))
    decryptor = cipher.decryptor()
    return decryptor.update(encrypted_data) + decryptor.finalize()


# Errors that should not be retried (permanent failures).
# Keep this limited to genuinely dead/removed links; decryption problems can be
# caused by stale LimeWire constants or other transient app-side issues.
_PERMANENT_ERRORS = (
    "share link is unavailable",
)


def _is_permanent_error(error: str) -> bool:
    return any(msg in error for msg in _PERMANENT_ERRORS)


# Deterministic decryption failure: the attempt already re-tried the same bytes
# with freshly-extracted constants, so in-process retries cannot change it.
# Status stays FAILED — the next daily cycle re-attempts cheaply (one ranged
# probe + a local decrypt, since the .part is kept).
_DECRYPT_FAILED_MSG = "Decryption failed even after refreshing constants. See UPDATE_KEYS.md."


def _is_no_retry_error(error: str) -> bool:
    return "Decryption failed even after refreshing constants" in error


def _is_unsupported_error(error: str) -> bool:
    """True when the error text marks a live-but-non-PDF payload.

    Display layers (output.py, daemon logs) only see the error string; control
    flow uses ``DownloadResult.unsupported``.
    """
    return "Unsupported payload" in error


async def _establish_session_with_retry(
    limewire_url: str,
    client: httpx.AsyncClient,
    retries: int = 2,
    rate_gate: "RateLimitGate | None" = None,
) -> LimeWireSession:
    """Establish a LimeWire session, retrying transient errors.

    Transient errors (SSR throttle, 429/5xx) are retried at least
    ``_MIN_SESSION_RETRIES`` times even when ``retries`` is 0 — a transient
    session/infra hiccup is not a download failure, so download-level
    ``retry_attempts`` semantics do not apply here. When a shared rate-limit
    gate is provided, a transient failure pauses all concurrent downloads
    before retrying instead of sleeping per-worker.
    """
    total = max(retries, _MIN_SESSION_RETRIES) + 1
    for attempt in range(1, total + 1):
        try:
            return await establish_session(limewire_url, client=client)
        except (RuntimeError, httpx.HTTPStatusError) as e:
            is_transient = (
                (isinstance(e, RuntimeError) and "transient" in str(e))
                or (isinstance(e, httpx.HTTPStatusError) and e.response.status_code in (429, 500, 502, 503, 504))
            )
            if not is_transient or attempt == total:
                raise
            logger.info(f"Session attempt {attempt}/{total} failed ({e}); pausing then retrying...")
            if rate_gate is not None:
                await rate_gate.trigger(_SSR_THROTTLE_PAUSE, reason="LimeWire throttle (transient SSR)")
            else:
                await asyncio.sleep(5 * attempt)  # 5s, 10s


class RateLimitGate:
    """Shared gate that pauses all downloads when a 429 is received.

    When any download hits a 429, it acquires the gate, pauses for
    the Retry-After duration, then releases. Other downloads wait
    on the gate before making API calls.
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._ready.set()
        self._deadline = 0.0

    async def wait(self):
        """Wait until the rate limit gate is open."""
        await self._ready.wait()

    async def trigger(self, retry_after: int = 30, *, reason: str = "Rate limited (429)"):
        """Pause all downloads until ``retry_after`` seconds from now.

        A concurrent trigger requesting a later resume time extends the active
        pause rather than being ignored, and the gate always reopens even if
        this task is cancelled mid-pause.
        """
        now = asyncio.get_running_loop().time()
        self._deadline = max(self._deadline, now + max(retry_after, 1))
        if self._lock.locked():
            # Another task is already pausing; the deadline was extended above.
            await self._ready.wait()
            return
        async with self._lock:
            self._ready.clear()
            logger.warning(f"{reason}. Pausing all downloads...")
            try:
                while True:
                    remaining = self._deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        break
                    await asyncio.sleep(remaining)
            finally:
                self._ready.set()
                logger.info("Rate limit pause ended, resuming downloads")


# Global gate — shared across concurrent downloads within one event loop
_rate_limit_gate: RateLimitGate | None = None


def get_rate_limit_gate() -> RateLimitGate:
    """Get or create the shared rate limit gate."""
    global _rate_limit_gate
    if _rate_limit_gate is None:
        _rate_limit_gate = RateLimitGate()
    return _rate_limit_gate


def _parse_retry_after(response: httpx.Response) -> int:
    """Parse Retry-After header, defaulting to 30 seconds."""
    header = response.headers.get("retry-after", "")
    try:
        return max(int(header), 1)
    except (ValueError, TypeError):
        return 30


async def download_and_decrypt(
    limewire_url: str,
    dest: Path,
    *,
    constants: LimeWireConstants | None = None,
    on_progress: callable | None = None,
    retry_attempts: int | None = None,
    rate_gate: RateLimitGate | None = None,
) -> DownloadResult:
    """Full pipeline: download an encrypted file from LimeWire and decrypt it.

    Retries transient errors with exponential backoff. 429 responses
    trigger a shared pause across all concurrent downloads.
    Permanent errors (dead links, decryption failures) fail immediately.

    Returns a DownloadResult with success status and file path.
    """
    if constants is None:
        cfg = load_config()
        constants = cfg.limewire
        if retry_attempts is None:
            retry_attempts = cfg.download.retry_attempts
    if retry_attempts is None:
        retry_attempts = 2
    if rate_gate is None:
        rate_gate = get_rate_limit_gate()

    total = retry_attempts + 1  # 1 initial attempt + retry_attempts retries
    last_error = ""
    for attempt in range(1, total + 1):
        # Wait if a 429 pause is active
        await rate_gate.wait()

        result = await _download_and_decrypt_once(
            limewire_url, dest, constants=constants, on_progress=on_progress,
            rate_gate=rate_gate, retry_attempts=retry_attempts,
        )
        if result.success:
            return result

        last_error = result.error or "Unknown error"

        if result.unsupported:
            # Terminal by policy: a live share with a non-PDF payload. No retry
            # (and no constants refresh) can change what the payload is.
            logger.info(f"Unsupported payload (no retry): {last_error}")
            return result

        if _is_no_retry_error(last_error):
            # Deterministic for this run: the attempt already re-tried the same
            # bytes with freshly-extracted constants.
            logger.error(f"Giving up without retry: {last_error}")
            return result

        if _is_permanent_error(last_error):
            # INFO: safe only while _PERMANENT_ERRORS is dead-link-only (a dead
            # link during bulk work is expected, not exceptional). If a new
            # permanent-error string is added, re-check whether it should log
            # louder here.
            logger.info(f"Permanent error (no retry): {last_error}")
            return result

        # 429 is handled inside _download_and_decrypt_once via the gate,
        # but we still retry the attempt
        if "429" in last_error or "rate limit" in last_error.lower():
            logger.info(f"Retrying after rate limit (attempt {attempt}/{total})...")
            continue

        if attempt < total:
            delay = 2 ** attempt  # 2s, 4s, 8s, ...
            logger.warning(f"Attempt {attempt}/{total} failed: {last_error}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
        else:
            logger.error(f"All {total} attempts failed: {last_error}")

    return DownloadResult(success=False, error=f"Failed after {total} attempts: {last_error}")


async def _download_and_decrypt_once(
    limewire_url: str,
    dest: Path,
    *,
    constants: LimeWireConstants,
    on_progress: callable | None = None,
    rate_gate: RateLimitGate | None = None,
    retry_attempts: int = 2,
) -> DownloadResult:
    """Single download attempt (no retry)."""
    try:
        return await _do_download(limewire_url, dest, constants=constants,
                                  on_progress=on_progress, rate_gate=rate_gate,
                                  retry_attempts=retry_attempts)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            retry_after = _parse_retry_after(e.response)
            if rate_gate:
                await rate_gate.trigger(retry_after)
            return DownloadResult(success=False, error=f"429 rate limited (retry-after: {retry_after}s)")
        return DownloadResult(success=False, error=f"HTTP {e.response.status_code}: {e}")
    except RuntimeError as e:
        return DownloadResult(success=False, error=str(e))


async def _do_download(
    limewire_url: str,
    dest: Path,
    *,
    constants: LimeWireConstants,
    on_progress: callable | None = None,
    rate_gate: RateLimitGate | None = None,
    retry_attempts: int = 2,
) -> DownloadResult:
    """Inner download logic, separated for 429 handling."""

    sharing_id, fragment = parse_limewire_url(limewire_url)

    async with httpx.AsyncClient(follow_redirects=True, timeout=120.0, headers={"User-Agent": "Mozilla/5.0"}) as client:
        # Auto-extract constants if not yet populated
        if not constants.file_iv_b64 or not constants.sharing_salt_b64:
            logger.info("No encryption constants configured — running auto-extraction...")
            extracted = await auto_extract_constants(client=client)
            if extracted:
                constants = extracted
                try:
                    cfg = load_config()
                    cfg.limewire = constants
                    save_config(cfg)
                    logger.info("Encryption constants saved to config")
                except OSError:
                    logger.info("Config is read-only — constants will be used in memory only")
            else:
                return DownloadResult(
                    success=False,
                    error="No encryption constants configured and auto-extraction failed. See UPDATE_KEYS.md.",
                )

        # Establish session (retry transient SSR errors with backoff)
        session = await _establish_session_with_retry(limewire_url, client, retries=retry_attempts, rate_gate=rate_gate)

        # Payload gate: a share whose file name has a known non-PDF extension
        # is skipped before key derivation, .part preparation, and the
        # download-URL request — no payload bytes are ever requested.
        # Ambiguous suffixes (e.g. ".2026" from a dotty issue name) fall
        # through to the magic-number check after decryption.
        suffix = Path(session.file_name).suffix.lower() if session.file_name else ""
        if suffix in _NON_PDF_EXTENSIONS:
            _cleanup_part(_part_path_for(dest, limewire_url))
            logger.info(f"Unsupported payload (non-PDF): {session.file_name} — skipping")
            return DownloadResult(
                success=False,
                unsupported=True,
                error=f"Unsupported payload: {session.file_name}",
            )

        # Derive key
        aes_key = derive_aes_key(
            sharing_id,
            fragment,
            session.passphrase_wrapped_pk,
            session.ephemeral_public_key,
            constants,
        )

        # Prepare .part file for resumable download (keyed to this URL; stale
        # partials from a different/rotated link are removed)
        dest.parent.mkdir(parents=True, exist_ok=True)
        part_path = _part_path_for(dest, limewire_url)

        existing_bytes = part_path.stat().st_size if part_path.exists() else 0
        if existing_bytes > 0:
            logger.info(f"Resuming download from {existing_bytes:,} bytes")

        # Get presigned URL (the session above is this attempt's — always fresh)
        s3_url = await get_download_url(session, client=client)

        # Fetch. A non-empty .part resumes via a ranged request, and the
        # storage response — never the SSR-advertised size — is the authority
        # for truncating local bytes: SSR reports bucket totals and may drift,
        # so truncating on it could destroy a good download.
        headers = {}
        if existing_bytes > 0:
            headers["Range"] = f"bytes={existing_bytes}-"

        effective_total = 0  # storage-reported object size, once known
        streamed = False     # whether any payload bytes were transferred
        total_downloaded = existing_bytes
        async with client.stream("GET", s3_url, headers=headers) as stream:
            status = stream.status_code
            if status == 416:
                # Range start >= object size. The error body is NEVER written.
                total = _content_range_total(stream.headers.get("content-range"))
                if total is not None and existing_bytes >= total:
                    effective_total = total
                elif total is None and session.file_size > 0 and existing_bytes == session.file_size:
                    # No Content-Range: the SSR size may CONFIRM completeness
                    # (exact match only); it never justifies truncation.
                    effective_total = existing_bytes
                else:
                    return DownloadResult(
                        success=False,
                        error=(
                            f"HTTP 416 but completeness unverifiable (local "
                            f"{existing_bytes:,} bytes, Content-Range total "
                            f"{total if total is not None else 'absent'})"
                        ),
                    )
            elif status in (200, 206):
                if status == 206:
                    start, total = _content_range_parts(stream.headers.get("content-range"))
                    if start != existing_bytes:
                        # AES-CTR is positional: appending a mis-offset body
                        # would splice ciphertext into garbage.
                        return DownloadResult(
                            success=False,
                            error=(
                                f"Resume offset mismatch (requested {existing_bytes}, "
                                f"Content-Range {stream.headers.get('content-range')!r})"
                            ),
                        )
                    effective_total = total or 0
                    mode = "ab"
                else:
                    if existing_bytes > 0:
                        logger.info("Server ignored Range request — restarting from byte 0")
                    content_length = stream.headers.get("content-length", "")
                    effective_total = int(content_length) if content_length.isdigit() else 0
                    mode = "wb"
                    total_downloaded = 0
                streamed = True
                with open(part_path, mode) as f:
                    async for chunk in stream.aiter_bytes(chunk_size=65536):
                        f.write(chunk)
                        total_downloaded += len(chunk)
                        if on_progress:
                            on_progress(total_downloaded, effective_total or session.file_size)
            else:
                # Error bodies must never reach the .part file.
                stream.raise_for_status()
                return DownloadResult(success=False, error=f"Unexpected HTTP {status} from storage")

        part_size = part_path.stat().st_size if part_path.exists() else 0
        if effective_total <= 0:
            effective_total = part_size  # no authoritative size reported → take the file as-is
        if part_size > effective_total:
            with open(part_path, "r+b") as f:
                f.truncate(effective_total)
            logger.info(
                f"Truncated partial file to storage-reported {effective_total:,} bytes "
                f"(reclaimed {part_size - effective_total:,} junk bytes)"
            )
            part_size = effective_total
        if part_size < effective_total:
            # Short read: transient and resumable — not a decryption problem,
            # so validation and self-healing must not run.
            return DownloadResult(
                success=False,
                error=f"incomplete download ({part_size:,} of {effective_total:,} bytes)",
            )
        if not streamed and on_progress:
            on_progress(effective_total, effective_total)

        # Read exactly the object's bytes and decrypt (anything beyond
        # effective_total can never reach the output or the dedup hash)
        encrypted_data = part_path.read_bytes()[:effective_total]
        decrypted = decrypt_file(encrypted_data, aes_key, constants)

        # Classify: a known non-PDF signature means decryption WORKED but the
        # payload is unwanted — terminal, self-healing cannot change it. Only
        # unrecognized output suggests stale constants.
        verdict = _classify_payload(decrypted[:16])
        if verdict == "unknown":
            logger.warning("Decryption produced unrecognized output — attempting self-healing...")
            new_constants = await auto_extract_constants(client=client)
            if not new_constants:
                return DownloadResult(
                    success=False,
                    error="Decryption produced invalid output and could not auto-extract new constants.",
                )
            aes_key = derive_aes_key(
                sharing_id,
                fragment,
                session.passphrase_wrapped_pk,
                session.ephemeral_public_key,
                new_constants,
            )
            decrypted = decrypt_file(encrypted_data, aes_key, new_constants)
            verdict = _classify_payload(decrypted[:16])
            if verdict == "unknown":
                # Keep the .part: it is size-consistent (junk-free up to
                # effective_total), so the next attempt costs one ranged probe
                # plus a local decrypt — not a full re-download.
                return DownloadResult(success=False, error=_DECRYPT_FAILED_MSG)
            if verdict == "pdf":
                logger.info("Self-healing successful — decryption now valid")
                try:
                    cfg = load_config()
                    cfg.limewire = new_constants
                    save_config(cfg)
                    logger.info("Updated constants saved to config")
                except OSError:
                    # Config may be read-only (e.g., Docker :ro mount).
                    # Keep new constants in memory — they'll be used for
                    # remaining downloads this session but won't survive restart.
                    pass
        if verdict == "unsupported":
            _cleanup_part(part_path)
            name = session.file_name or dest.name
            logger.info(f"Unsupported payload (non-PDF content): {name} — skipping")
            return DownloadResult(
                success=False,
                unsupported=True,
                error=f"Unsupported payload: {name}",
            )

        # Compute content hash for deduplication
        file_hash = hashlib.sha256(decrypted).hexdigest()

        # Check for duplicate content in the index
        from magsync.core.index import MagazineIndex
        try:
            idx = MagazineIndex()
            existing_path = idx.find_by_hash(file_hash)
            idx.close()
        except Exception:
            existing_path = None

        if existing_path:
            logger.info(f"Duplicate detected (same content as {existing_path}), skipping save")
            part_path.unlink(missing_ok=True)
            return DownloadResult(
                success=True,
                file_path=Path(existing_path),
                file_size_bytes=len(decrypted),
                sha256=file_hash,
            )

        # Save and clean up .part file
        dest.write_bytes(decrypted)
        part_path.unlink(missing_ok=True)

        return DownloadResult(
            success=True,
            file_path=dest,
            file_size_bytes=len(decrypted),
            sha256=file_hash,
        )


async def auto_extract_constants(
    *,
    client: httpx.AsyncClient | None = None,
) -> LimeWireConstants | None:
    """Fetch LimeWire's current JS bundles and extract encryption constants.

    Returns updated constants, or None if extraction fails.
    """
    should_close = client is None
    if client is None:
        client = httpx.AsyncClient(follow_redirects=True, timeout=60.0, headers={"User-Agent": "Mozilla/5.0"})

    try:
        logger.info("Auto-extracting encryption constants from LimeWire...")

        # Step 1: Get the service worker for file IVs
        logger.info("  Fetching service worker...")
        sw_resp = await client.get("https://limewire.com/build/workers/service-worker.js")
        sw_resp.raise_for_status()
        sw_text = sw_resp.text
        logger.info(f"  Service worker: {len(sw_text)} bytes")

        # Extract static file IVs from service worker
        file_iv = _extract_js_string(sw_text, "mainFileBase64")
        file_name_iv = _extract_js_string(sw_text, "mainFileNameBase64")
        file_sha1_iv = _extract_js_string(sw_text, "mainFileSha1Base64")
        preview_iv = _extract_js_string(sw_text, "previewFileBase64")

        if not file_iv:
            logger.error("  Failed to extract file IV from service worker")
            return None
        logger.info(f"  Extracted file IVs from service worker")

        # Step 2: Get the main page to find chunk URLs
        logger.info("  Fetching LimeWire homepage for JS chunks...")
        page_resp = await client.get("https://limewire.com/")
        page_resp.raise_for_status()
        page_html = page_resp.text

        # Find JS chunk URLs that might contain the salt
        chunk_urls = re.findall(
            r'(?:href|src)="(/build/chunks/[^"]+\.js)"', page_html
        )
        chunk_urls += re.findall(
            r'href="(/build/chunks/[^"]+\.js)"', page_html
        )
        chunk_urls = list(set(chunk_urls))
        logger.info(f"  Found {len(chunk_urls)} JS chunks to search")

        salt_b64 = None
        sharing_iv = None
        chunks_searched = 0

        for chunk_path in chunk_urls:
            if salt_b64 and sharing_iv:
                break
            chunk_url = f"https://limewire.com{chunk_path}"
            try:
                chunk_resp = await client.get(chunk_url)
                chunk_text = chunk_resp.text
                chunks_searched += 1
                if "saltBase64" in chunk_text:
                    extracted_salt = _extract_js_string(chunk_text, "saltBase64")
                    if extracted_salt:
                        salt_b64 = extracted_salt
                        sharing_iv = _extract_js_string(chunk_text, "ivBase64")
                        logger.info(f"  Found salt in chunk {chunks_searched}/{len(chunk_urls)}: {chunk_path}")
                        break
                    # Chunk contains saltBase64 as a reference, not a value — keep searching
            except httpx.HTTPError as e:
                logger.debug(f"  Chunk fetch failed: {chunk_path}: {e}")
                continue

        if not salt_b64:
            logger.error(f"  Failed to find saltBase64 after searching {chunks_searched} chunks")
            return None

        pbkdf2_iterations = 100_000

        constants = LimeWireConstants(
            sharing_salt_b64=salt_b64,
            sharing_iv_b64=sharing_iv or "",
            file_iv_b64=file_iv,
            file_name_iv_b64=file_name_iv or "",
            file_sha1_iv_b64=file_sha1_iv or "",
            preview_iv_b64=preview_iv or "",
            pbkdf2_iterations=pbkdf2_iterations,
        )
        logger.info("  Encryption constants extracted successfully")
        return constants

    except Exception as e:
        logger.error(f"  Auto-extraction failed: {e}")
        return None
    finally:
        if should_close:
            await client.aclose()


def _extract_js_string(js_text: str, key: str) -> str | None:
    """Extract a quoted string value for a key from minified JS."""
    # Matches patterns like: key:"value" or key: "value"
    patterns = [
        re.escape(key) + r'["\s]*:\s*["\']([^"\']+)["\']',
        re.escape(key) + r'","([^"]+)"',
        re.escape(key) + r"','([^']+)'",
    ]
    for pattern in patterns:
        m = re.search(pattern, js_text)
        if m:
            return m.group(1)
    return None
