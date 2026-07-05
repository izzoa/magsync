"""Tests for session-retry flooring, throttle, and the rate-limit gate."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from pathlib import Path

import httpx
import pytest

import magsync.core.downloader as dl
from magsync.core.downloader import RateLimitGate, _establish_session_with_retry

URL = "https://limewire.com/d/x#k"
FIXTURES = Path(__file__).parent / "fixtures"

# --- SSR fixtures (current escaped streaming format + legacy JSON shape) ---
# Dead share: removed-share error tuple in its bucket result (also carries the
# generic "Unexpected Server Error" message, so precedence is exercised).
DEAD_CURRENT = (
    '<html><body><script>['
    '\\"sharingId\\",\\"XOmKo\\",\\"sharingBucketContentData\\",{\\"_13\\":14},'
    '\\"ok\\",false,\\"error\\",[\\"SanitizedError\\",17,18,-7],'
    '\\"Error\\",\\"Unexpected Server Error\\"'
    ']</script></body></html>'
)
# Live share: ok:true with key material, no removed marker.
LIVE = (
    '<html><body><script>['
    '\\"sharingId\\",\\"4Kkl8\\",\\"sharingBucketContentData\\",{\\"_17\\":18},'
    '\\"ok\\",true,\\"value\\",{\\"x\\":1},\\"ephemeralPublicKey\\",\\"EPKvalue\\"'
    ']</script></body></html>'
)
# Transient: generic server error, no bucket data / no removed marker.
TRANSIENT = '<html><body>Unexpected Server Error</body></html>'
# Legacy JSON shape (pre-streaming) — backward compatibility.
LEGACY = '<html>"sharingBucketContentData":{"ok":false,"error":{"name":"SanitizedError"}}</html>'


def _mock_client(html: str) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_session_retry_floor_with_retries_zero(monkeypatch):
    # Even with retries=0, transient errors are retried up to the floor (>=2),
    # and the shared gate is engaged between attempts.
    calls = {"n": 0}

    async def fake_establish(url, client=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("LimeWire SSR returned transient server error")
        return "SESSION"

    monkeypatch.setattr(dl, "establish_session", fake_establish)

    gate = RateLimitGate()
    triggered = {"n": 0}

    async def fake_trigger(retry_after=30, *, reason="x"):
        triggered["n"] += 1

    monkeypatch.setattr(gate, "trigger", fake_trigger)

    result = await _establish_session_with_retry(URL, client=None, retries=0, rate_gate=gate)
    assert result == "SESSION"
    assert calls["n"] == 3        # floored to >=2 retries despite retries=0
    assert triggered["n"] == 2    # shared pause engaged before each retry


async def test_session_retry_non_transient_raises_immediately(monkeypatch):
    calls = {"n": 0}

    async def fake_establish(url, client=None):
        calls["n"] += 1
        raise RuntimeError("Failed to extract SSR metadata")  # not transient

    monkeypatch.setattr(dl, "establish_session", fake_establish)

    with pytest.raises(RuntimeError):
        await _establish_session_with_retry(URL, client=None, retries=0, rate_gate=RateLimitGate())
    assert calls["n"] == 1  # no retry on a non-transient error


async def test_retry_log_only_when_retrying(monkeypatch, caplog):
    async def always_transient(url, client=None):
        raise RuntimeError("LimeWire SSR returned transient server error")

    monkeypatch.setattr(dl, "establish_session", always_transient)
    gate = RateLimitGate()

    async def noop_trigger(*a, **k):
        return None

    monkeypatch.setattr(gate, "trigger", noop_trigger)

    with caplog.at_level(logging.INFO, logger="magsync"):
        with pytest.raises(RuntimeError):
            await _establish_session_with_retry(URL, client=None, retries=0, rate_gate=gate)

    msgs = [r.getMessage() for r in caplog.records]
    # 3 attempts (floor) → 2 retries logged, final attempt raises with no retry claim
    assert sum("pausing then retrying" in m for m in msgs) == 2
    assert not any("will retry" in m for m in msgs)


async def test_rate_gate_cancellation_reopens():
    # The critical deadlock fix: a cancelled pause must still reopen the gate.
    gate = RateLimitGate()
    task = asyncio.create_task(gate.trigger(100))
    await asyncio.sleep(0.02)  # let the pause start
    assert not gate._ready.is_set()  # gate closed
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert gate._ready.is_set()  # reopened despite cancellation


async def test_rate_gate_longer_trigger_extends_deadline():
    gate = RateLimitGate()
    holder = asyncio.create_task(gate.trigger(1))  # acquires lock, deadline ~ now+1
    await asyncio.sleep(0.02)
    before = gate._deadline
    extender = asyncio.create_task(gate.trigger(5))  # lock held → extends deadline
    await asyncio.sleep(0.02)
    assert gate._deadline > before  # later resume time wins instead of being ignored
    for t in (holder, extender):
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass


# --- dead-share detection / classification ---

def test_is_removed_share_units():
    assert dl._is_removed_share(DEAD_CURRENT) is True
    assert dl._is_removed_share(LEGACY) is True            # backward-compat
    assert dl._is_removed_share(LIVE) is False
    assert dl._is_removed_share("no markers here") is False
    # stray SanitizedError far from the bucket marker → anchoring rejects it
    assert dl._is_removed_share("SanitizedError" + "x" * 1000 + "sharingBucketContentData" + "y" * 500) is False


async def test_dead_share_current_format_is_permanent():
    async with _mock_client(DEAD_CURRENT) as c:
        with pytest.raises(RuntimeError) as ei:
            await dl.establish_session("https://limewire.com/d/XOmKo#k", client=c)
    assert "unavailable" in str(ei.value)          # permanent, not transient
    assert dl._is_permanent_error(str(ei.value))


async def test_removed_marker_takes_precedence_over_unexpected_error():
    # DEAD_CURRENT contains BOTH "Unexpected Server Error" and the removed marker.
    async with _mock_client(DEAD_CURRENT) as c:
        with pytest.raises(RuntimeError) as ei:
            await dl.establish_session("https://limewire.com/d/XOmKo#k", client=c)
    assert "transient" not in str(ei.value)


async def test_dead_share_classified_before_jwt():
    # No auth cookie set; a dead share must still raise PERMANENT, not "Failed to obtain JWT".
    async with _mock_client(DEAD_CURRENT) as c:
        with pytest.raises(RuntimeError) as ei:
            await dl.establish_session("https://limewire.com/d/XOmKo#k", client=c)
    assert "unavailable" in str(ei.value)
    assert "JWT" not in str(ei.value)


async def test_transient_without_removed_marker():
    async with _mock_client(TRANSIENT) as c:
        with pytest.raises(RuntimeError) as ei:
            await dl.establish_session("https://limewire.com/d/Zzz#k", client=c)
    assert "transient" in str(ei.value)
    assert not dl._is_permanent_error(str(ei.value))


async def test_live_share_not_misclassified():
    assert dl._is_removed_share(LIVE) is False
    async with _mock_client(LIVE) as c:
        with pytest.raises(RuntimeError) as ei:
            await dl.establish_session("https://limewire.com/d/4Kkl8#k", client=c)
    # Passed classification; fails later (no JWT cookie) — NOT removed/transient.
    msg = str(ei.value)
    assert "unavailable" not in msg and "transient" not in msg


async def test_legacy_json_shape_is_permanent():
    async with _mock_client(LEGACY) as c:
        with pytest.raises(RuntimeError) as ei:
            await dl.establish_session("https://limewire.com/d/Leg#k", client=c)
    assert "unavailable" in str(ei.value)


# --- link-keyed partial downloads ---

def test_part_path_changed_url_discards_stale_partial(tmp_path):
    dest = tmp_path / "Mag" / "Title.pdf"
    dest.parent.mkdir(parents=True)
    old = dl._part_path_for(dest, "https://limewire.com/d/OldId#oldkey")
    old.write_bytes(b"partial-bytes")

    new = dl._part_path_for(dest, "https://limewire.com/d/NewId#newkey")
    assert new != old
    assert not old.exists()          # stale partial deleted — never resumed
    assert not new.exists()          # fresh download starts from byte 0


def test_part_path_same_url_resumes(tmp_path):
    dest = tmp_path / "Title.pdf"
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = "https://limewire.com/d/Same#key"
    p1 = dl._part_path_for(dest, url)
    p1.write_bytes(b"resume-me")

    p2 = dl._part_path_for(dest, url)
    assert p2 == p1
    assert p2.read_bytes() == b"resume-me"  # unchanged URL keeps its partial


def test_part_path_fragment_only_rotation_discards_partial(tmp_path):
    # Same sharing ID, different key → different blob; must not resume.
    dest = tmp_path / "Title.pdf"
    p1 = dl._part_path_for(dest, "https://limewire.com/d/Same#key1")
    p1.write_bytes(b"old-blob")
    p2 = dl._part_path_for(dest, "https://limewire.com/d/Same#key2")
    assert p2 != p1
    assert not p1.exists()


def test_part_path_legacy_unfingerprinted_partial_discarded(tmp_path):
    dest = tmp_path / "Title.pdf"
    legacy = tmp_path / "Title.pdf.part"
    legacy.write_bytes(b"legacy")
    dl._part_path_for(dest, "https://limewire.com/d/Any#key")
    assert not legacy.exists()


def test_part_path_ignores_unrelated_files(tmp_path):
    dest = tmp_path / "Title.pdf"
    sibling_pdf = tmp_path / "Title.pdf"           # the final file itself
    sibling_pdf.write_bytes(b"%PDF")
    other_issue = tmp_path / "Other.pdf.deadbeef.part"
    other_issue.write_bytes(b"other")
    dl._part_path_for(dest, "https://limewire.com/d/Any#key")
    assert sibling_pdf.exists()
    assert other_issue.exists()                    # other issues' partials untouched


# --- turbo-stream decoder + structural extraction ---

# The current LimeWire SSR format; the true bucket/item follow textually-earlier
# decoy UUIDs (the file-encryption-key id and the free-user-id).
REAL_BUCKET = "c540acbb-da69-4cf2-9688-5050387c668d"
REAL_ITEM = "eaa1a365-8cb1-4015-9ad2-e296b0d3a4d7"
DECOY_KEY_ID = "3cef2515-b1b7-4ea9-ab7d-c3f88bd4e912"


def _load_live() -> str:
    return (FIXTURES / "limewire_share_live.html").read_text()


def _load_removed() -> str:
    return (FIXTURES / "limewire_share_removed.html").read_text()


def _wrap_stream(flat: list) -> str:
    """Serialize a flat array into an enqueue-wrapped page like LimeWire ships."""
    js = json.dumps(json.dumps(flat, separators=(",", ":")))
    return (
        "<!doctype html><html><body><script>"
        f"streamController.enqueue({js});streamController.close();</script></body></html>"
    )


# `_extract_share_metadata` consumes the DECODED structure, so extractor tests
# pass plain dicts shaped like the resolved container (no turbo-stream needed).
def _decoded_value(value: dict) -> dict:
    return {"sharingBucketContentData": {"ok": True, "value": value}}


def _decoded_container(container: dict) -> dict:
    return {"sharingBucketContentData": container}


def _flatten(obj) -> list:
    """Encode a nested structure into turbo-stream flat-array form (inverse of the
    decoder): objects → ``{"_keyIdx": valIdx}``, lists → ``[elemIdx, …]``,
    primitives → literal slots. Root ends up at index 0."""
    arr: list = []

    def add(v) -> int:
        idx = len(arr)
        arr.append(None)
        if isinstance(v, dict):
            arr[idx] = {f"_{add(k)}": add(val) for k, val in v.items()}
        elif isinstance(v, list):
            arr[idx] = [add(e) for e in v]
        else:
            arr[idx] = v
        return idx

    add(obj)
    return arr


def _wrap_stream_decoded(decoded_root: dict) -> str:
    """Full enqueue-wrapped page whose decode yields ``decoded_root``."""
    return _wrap_stream(_flatten(decoded_root))


def test_decode_resolves_references_objects_and_literals():
    # {"_1":2} → key arr[1]="k", value resolve(2)=arr[2]=7 (primitive slot as-is).
    assert dl._decode_react_stream(_wrap_stream([{"_1": 2}, "k", 7])) == {"k": 7}


def test_decode_array_refs_dates_sentinels_and_cycles():
    flat = [
        {"_1": 2, "_3": 4, "_5": 6, "_7": 500, "_9": 10},  # 0
        "arr",           # 1
        [11],            # 2   → [resolve(11)] = ["z"]
        "date",          # 3
        ["D", 999],      # 4   → 999 (tagged Date)
        "lit",           # 5
        "hello",         # 6   literal
        "oob",           # 7
        "unused",        # 8
        "cyc",           # 9
        {"_9": 10},      # 10  → self-reference via index 10 → cycle → None
        "z",             # 11
    ]
    out = dl._decode_react_stream(_wrap_stream(flat))
    assert out["arr"] == ["z"]        # array element reference
    assert out["date"] == 999         # ["D", ms] → ms
    assert out["lit"] == "hello"      # literal at referenced slot
    assert out["oob"] is None         # out-of-range index → None
    assert out["cyc"] == {"cyc": None}  # cycle guard → None


def test_decode_multiple_chunks_concatenate():
    # One array's serialized form split across two enqueue payloads.
    full = json.dumps([{"_1": 2}, "k", 5], separators=(",", ":"))
    mid = len(full) // 2
    html = (
        "<script>streamController.enqueue(" + json.dumps(full[:mid]) + ");"
        "streamController.enqueue(" + json.dumps(full[mid:]) + ");</script>"
    )
    assert dl._decode_react_stream(html) == {"k": 5}


def test_decode_returns_none_without_stream():
    assert dl._decode_react_stream("<html>no stream here</html>") is None


def test_extract_prefers_true_ids_over_decoys():
    meta = dl._extract_share_metadata(dl._decode_react_stream(_load_live()))
    assert meta["bucket_id"] == REAL_BUCKET
    assert meta["content_item_id"] == REAL_ITEM
    assert meta["bucket_id"] != DECOY_KEY_ID
    assert meta["file_size"] == 6903693
    assert meta["passphrase_wrapped_pk"] and meta["ephemeral_public_key"]


def test_extract_binds_key_by_base_file_encryption_key_id():
    # Content item's baseFileEncryptionKeyId points at the SECOND key; a blind
    # [0] would pick the decoy, so the match must select WRAP_B.
    root = _decoded_value({
        "sharingBucket": {"id": "bucket-uuid", "name": "f.pdf", "totalFileSize": 42},
        "contentItemList": [{
            "id": "the-item", "ephemeralPublicKey": "EPHEM",
            "baseFileEncryptionKeyId": "key-B",
        }],
        "fileEncryptionKeys": [
            {"id": "key-A", "passphraseWrappedPrivateKey": "WRAP_A"},   # decoy first
            {"id": "key-B", "passphraseWrappedPrivateKey": "WRAP_B"},   # the match
        ],
    })
    meta = dl._extract_share_metadata(root)
    assert meta["content_item_id"] == "the-item"
    assert meta["passphrase_wrapped_pk"] == "WRAP_B"


def test_extract_removed_and_malformed():
    assert dl._extract_share_metadata(dl._decode_react_stream(_load_removed())) is dl._REMOVED
    # Container present but no `ok` field (format drift) → None, NOT removed.
    assert dl._extract_share_metadata(_decoded_container({"value": {}})) is None
    # Absent container → None.
    assert dl._extract_share_metadata({"unrelated": 1}) is None
    assert dl._extract_share_metadata(None) is None


# --- establish_session end-to-end against fixtures ---

def _jwt() -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"csrfToken": "csrf-x"}).encode()).rstrip(b"=").decode()
    return f"h.{payload}.s"


def _session_client(html: str, *, set_cookie: bool = True) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        headers = {"Set-Cookie": f"production_access_token={_jwt()}; Path=/"} if set_cookie else {}
        return httpx.Response(200, text=html, headers=headers)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_establish_session_extracts_corrected_ids_short_id():
    async with _session_client(_load_live()) as c:
        s = await dl.establish_session("https://limewire.com/d/PKQbo#s8mzZLKRe7", client=c)
    assert s.bucket_id == REAL_BUCKET          # decoded sharingBucket.id, not the decoy
    assert s.content_item_id == REAL_ITEM
    assert s.bucket_id != DECOY_KEY_ID
    assert s.file_size == 6903693              # totalFileSize, not 0
    assert s.passphrase_wrapped_pk and s.ephemeral_public_key


async def test_establish_session_structural_removed_backstop():
    # Removed marker sits outside _is_removed_share's raw window; only the
    # structural ok:false backstop can catch it.
    assert dl._is_removed_share(_load_removed()) is False
    async with _session_client(_load_removed()) as c:
        with pytest.raises(RuntimeError) as ei:
            await dl.establish_session("https://limewire.com/d/Dead#k", client=c)
    assert "unavailable" in str(ei.value) and "transient" not in str(ei.value)


async def test_establish_session_falls_back_to_regex_when_no_stream(monkeypatch):
    # No turbo-stream, but the legacy escaped format is present → regex fallback.
    html = (
        '<html><body><script>'
        '\\"sharingBucket\\",\\"x\\",\\"' + REAL_BUCKET + '\\",'
        '\\"contentItemIds\\",\\"' + REAL_ITEM + '\\",'
        '\\"passphraseWrappedPrivateKey\\",\\"WRAP\\",\\"ephemeralPublicKey\\",\\"EPK\\"'
        '</script></body></html>'
    )
    assert dl._decode_react_stream(html) is None       # confirm fallback path taken
    async with _session_client(html) as c:
        s = await dl.establish_session("https://limewire.com/d/short#k", client=c)
    assert s.bucket_id == REAL_BUCKET and s.content_item_id == REAL_ITEM


async def test_establish_session_short_id_missing_wrapped_key_fails():
    # Short-ID share decodes but has no passphraseWrappedPrivateKey → extraction
    # failure (not a later opaque crypto failure via the raw-key path).
    root = _decoded_value({
        "sharingBucket": {"id": REAL_BUCKET, "name": "f.pdf", "totalFileSize": 10},
        "contentItemList": [{"id": REAL_ITEM, "ephemeralPublicKey": "EPK"}],
        "fileEncryptionKeys": [{"id": "k", "passphraseWrappedPrivateKey": None}],
    })
    html = _wrap_stream_decoded(root)
    async with _session_client(html) as c:
        with pytest.raises(RuntimeError) as ei:
            await dl.establish_session("https://limewire.com/d/short#k", client=c)
    assert "passphrase_wrapped_pk" in str(ei.value)


async def test_establish_session_uuid_share_keeps_sharing_id_as_bucket():
    uuid = "7d08450d-1111-2222-3333-444455556666"
    # UUID share: bucket stays sharing_id; item/ephemeral come from decode; a
    # missing wrapped key is permitted (raw-key path).
    root = _decoded_value({
        "sharingBucket": {"id": "decoded-bucket-should-not-win", "name": "f.pdf", "totalFileSize": 10},
        "contentItemList": [{"id": REAL_ITEM, "ephemeralPublicKey": "EPK"}],
        "fileEncryptionKeys": [],
    })
    html = _wrap_stream_decoded(root)
    async with _session_client(html) as c:
        s = await dl.establish_session(f"https://limewire.com/d/{uuid}#rawkey", client=c)
    assert s.bucket_id == uuid                 # sharing_id, not decoded sharingBucket.id
    assert s.content_item_id == REAL_ITEM
    assert s.passphrase_wrapped_pk == ""       # absent wrapped key permitted for UUID shares


async def test_permanent_session_error_skips_gate(monkeypatch):
    # _establish_session_with_retry must NOT retry or engage the gate on a permanent error.
    calls = {"n": 0}

    async def fake_establish(url, client=None):
        calls["n"] += 1
        raise RuntimeError("LimeWire share link is unavailable (removed or expired)")

    monkeypatch.setattr(dl, "establish_session", fake_establish)
    gate = RateLimitGate()
    triggered = {"n": 0}

    async def fake_trigger(*a, **k):
        triggered["n"] += 1

    monkeypatch.setattr(gate, "trigger", fake_trigger)

    with pytest.raises(RuntimeError) as ei:
        await _establish_session_with_retry(URL, client=None, retries=2, rate_gate=gate)
    assert "unavailable" in str(ei.value)
    assert calls["n"] == 1       # not retried (permanent)
    assert triggered["n"] == 0   # gate never engaged
