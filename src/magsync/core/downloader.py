"""LimeWire download and decryption pipeline.

Implements the full E2E decryption chain:
URL fragment → PBKDF2 → AES-KW unwrap → ECDH P-256 → AES-256-CTR decrypt
"""

from __future__ import annotations

import base64
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

        jwt_token = client.cookies.get("production_access_token")
        if not jwt_token:
            raise RuntimeError("Failed to obtain JWT from LimeWire")

        # Decode JWT payload to get CSRF token
        payload_b64 = jwt_token.split(".")[1] + "==="
        jwt_payload = json.loads(base64.b64decode(payload_b64))
        csrf_token = jwt_payload["csrfToken"]

        # Check for server-side errors (removed/expired share links)
        if "Unexpected Server Error" in html or '"error"' in html.split("sharingBucketContentData", 1)[-1][:200]:
            raise RuntimeError("LimeWire share link is unavailable (removed or expired)")

        # Extract bucket_id from SSR data
        # For UUID-format sharing IDs, the sharing_id IS the bucket_id.
        # For short IDs, we need to find the resolved bucket UUID in the HTML.
        if _is_uuid(sharing_id):
            bucket_id = sharing_id
        else:
            sb_idx = html.find("sharingBucket")
            bucket_match = re.search(UUID_RE, html[sb_idx:]) if sb_idx > -1 else None
            bucket_id = bucket_match.group(0) if bucket_match else None
        content_item_id = re.search(
            r"contentItemIds.*?(" + UUID_RE + ")", html
        )
        passphrase_wrapped = _ssr_field(html, "passphraseWrappedPrivateKey")
        ephemeral_pub = _ssr_field(html, "ephemeralPublicKey")

        # Extract file name and size
        file_name = _ssr_field(html, "name") or ""
        size_match = re.search(r'"totalFileSize",(\d+)', html)
        file_size = int(size_match.group(1)) if size_match else 0

        if not all([bucket_id, content_item_id, passphrase_wrapped, ephemeral_pub]):
            missing = []
            if not bucket_id: missing.append("bucket_id")
            if not content_item_id: missing.append("content_item_id")
            if not passphrase_wrapped: missing.append("passphrase_wrapped_pk")
            if not ephemeral_pub: missing.append("ephemeral_public_key")
            raise RuntimeError(f"Failed to extract SSR metadata from LimeWire page (missing: {', '.join(missing)})")

        return LimeWireSession(
            jwt_token=jwt_token,
            csrf_token=csrf_token,
            bucket_id=bucket_id,
            content_item_id=content_item_id.group(1),
            passphrase_wrapped_pk=passphrase_wrapped,
            ephemeral_public_key=ephemeral_pub,
            file_name=file_name,
            file_size=file_size,
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


async def download_and_decrypt(
    limewire_url: str,
    dest: Path,
    *,
    constants: LimeWireConstants | None = None,
    on_progress: callable | None = None,
) -> DownloadResult:
    """Full pipeline: download an encrypted file from LimeWire and decrypt it.

    Returns a DownloadResult with success status and file path.
    """
    if constants is None:
        constants = load_config().limewire

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

        # Establish session
        session = await establish_session(limewire_url, client=client)

        # Derive key
        aes_key = derive_aes_key(
            sharing_id,
            fragment,
            session.passphrase_wrapped_pk,
            session.ephemeral_public_key,
            constants,
        )

        # Get presigned URL
        s3_url = await get_download_url(session, client=client)

        # Download encrypted file
        encrypted_chunks: list[bytes] = []
        total_downloaded = 0
        async with client.stream("GET", s3_url) as stream:
            async for chunk in stream.aiter_bytes(chunk_size=65536):
                encrypted_chunks.append(chunk)
                total_downloaded += len(chunk)
                if on_progress:
                    on_progress(total_downloaded, session.file_size)

        encrypted_data = b"".join(encrypted_chunks)

        # Decrypt
        decrypted = decrypt_file(encrypted_data, aes_key, constants)

        # Validate
        if not decrypted[:4] == b"%PDF":
            logger.warning("Decryption produced invalid output — attempting self-healing...")
            new_constants = await auto_extract_constants(client=client)
            if new_constants:
                aes_key = derive_aes_key(
                    sharing_id,
                    fragment,
                    session.passphrase_wrapped_pk,
                    session.ephemeral_public_key,
                    new_constants,
                )
                decrypted = decrypt_file(encrypted_data, aes_key, new_constants)
                if decrypted[:4] == b"%PDF":
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
                else:
                    return DownloadResult(
                        success=False,
                        error="Decryption failed even after refreshing constants. See UPDATE_KEYS.md.",
                    )
            else:
                return DownloadResult(
                    success=False,
                    error="Decryption produced invalid PDF and could not auto-extract new constants.",
                )

        # Save
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(decrypted)

        return DownloadResult(
            success=True,
            file_path=dest,
            file_size_bytes=len(decrypted),
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
                    salt_b64 = _extract_js_string(chunk_text, "saltBase64")
                    sharing_iv = _extract_js_string(chunk_text, "ivBase64")
                    logger.info(f"  Found salt in chunk {chunks_searched}/{len(chunk_urls)}: {chunk_path}")
                    break
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
