"""Tests for session-retry flooring, throttle, and the rate-limit gate."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
from pathlib import Path

import httpx
import pytest

import magsync.core.downloader as dl
from magsync.config import LimeWireConstants
from magsync.core.downloader import RateLimitGate, _establish_session_with_retry
from magsync.core.models import DownloadFailureKind, DownloadResult, LimeWireSession

URL = "https://limewire.com/d/x#k"
FIXTURES = Path(__file__).parent / "fixtures"
_REAL_ASYNC_CLIENT = httpx.AsyncClient

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


async def test_session_helper_never_owns_retries(monkeypatch):
    # The full download orchestrator owns the only retry budget. This
    # compatibility helper performs exactly one physical session attempt.
    calls = {"n": 0}

    async def fake_establish(url, client=None):
        calls["n"] += 1
        raise RuntimeError("LimeWire SSR returned transient server error")

    monkeypatch.setattr(dl, "establish_session", fake_establish)

    gate = RateLimitGate()
    triggered = {"n": 0}

    async def fake_trigger(retry_after=30, *, reason="x"):
        triggered["n"] += 1

    monkeypatch.setattr(gate, "trigger", fake_trigger)

    with pytest.raises(RuntimeError):
        await _establish_session_with_retry(URL, client=None, retries=9, rate_gate=gate)
    assert calls["n"] == 1
    assert triggered["n"] == 0


async def test_session_retry_non_transient_raises_immediately(monkeypatch):
    calls = {"n": 0}

    async def fake_establish(url, client=None):
        calls["n"] += 1
        raise RuntimeError("Failed to extract SSR metadata")  # not transient

    monkeypatch.setattr(dl, "establish_session", fake_establish)

    with pytest.raises(RuntimeError):
        await _establish_session_with_retry(URL, client=None, retries=0, rate_gate=RateLimitGate())
    assert calls["n"] == 1  # no retry on a non-transient error


async def test_session_helper_does_not_claim_retry(monkeypatch, caplog):
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
    assert not any("retry" in m.lower() for m in msgs)


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
    assert ei.value.kind is DownloadFailureKind.SHARE_UNAVAILABLE


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
    assert ei.value.kind is DownloadFailureKind.TRANSIENT


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


def _load_orphaned() -> str:
    return (FIXTURES / "limewire_share_orphaned.html").read_text()


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


@pytest.mark.parametrize(
    ("base_key_id", "available_key_id"),
    [("missing-key", "decoy-key"), (None, None)],
)
def test_structural_ready_never_substitutes_unmatched_wrapped_key(
    base_key_id,
    available_key_id,
):
    root = _decoded_value({
        "sharingBucket": {"id": "bucket-uuid", "name": "f.pdf"},
        "contentItemList": [{
            "id": "the-item",
            "ephemeralPublicKey": "EPHEM",
            "baseFileEncryptionKeyId": base_key_id,
        }],
        "fileEncryptionKeys": [{
            "id": available_key_id,
            "passphraseWrappedPrivateKey": "DECOY_WRAP",
        }],
    })
    result = dl._extract_share_metadata_state(root, sharing_id="short")
    assert result.state is dl.ShareMetadataState.METADATA_INVALID


def test_extract_removed_and_malformed():
    assert dl._extract_share_metadata(dl._decode_react_stream(_load_removed())) is dl._REMOVED
    # Compatibility extraction stays None, while the discriminated state keeps
    # present malformed/null containers distinct from an absent container.
    assert dl._extract_share_metadata(_decoded_container({"value": {}})) is None
    assert dl._extract_share_metadata_state(
        _decoded_container({"value": {}})
    ).state is dl.ShareMetadataState.METADATA_INVALID
    assert dl._extract_share_metadata_state(
        {"sharingBucketContentData": None}
    ).state is dl.ShareMetadataState.METADATA_INVALID
    assert dl._extract_share_metadata({"unrelated": 1}) is None
    assert dl._extract_share_metadata_state(
        {"unrelated": 1}
    ).state is dl.ShareMetadataState.UNDECODABLE
    assert dl._extract_share_metadata_state(None).state is dl.ShareMetadataState.UNDECODABLE


def test_extract_explicit_empty_list_is_narrow_orphan_candidate():
    result = dl._extract_share_metadata_state(
        dl._decode_react_stream(_load_orphaned()),
        sharing_id="xTsja",
    )
    assert result.state is dl.ShareMetadataState.ORPHAN_CANDIDATE
    assert result.metadata["bucket_id"] == "11111111-2222-4333-8444-555555555555"


@pytest.mark.parametrize("content_items", [None, {}, "", 0])
def test_extract_non_list_content_items_is_metadata_invalid(content_items):
    root = _decoded_value({
        "sharingBucket": {"id": REAL_BUCKET},
        "contentItemList": content_items,
    })
    assert dl._extract_share_metadata_state(
        root,
        sharing_id="short",
    ).state is dl.ShareMetadataState.METADATA_INVALID


# --- establish_session end-to-end against fixtures ---

def _jwt() -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"csrfToken": "csrf-x"}).encode()).rstrip(b"=").decode()
    return f"h.{payload}.s"


def _session_client(html: str, *, set_cookie: bool = True) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        headers = {"Set-Cookie": f"production_access_token={_jwt()}; Path=/"} if set_cookie else {}
        return httpx.Response(200, text=html, headers=headers)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _session_sequence_client(*steps) -> tuple[httpx.AsyncClient, list[httpx.Request]]:
    """Build a session client whose consecutive GETs consume response steps."""

    requests: list[httpx.Request] = []
    remaining = list(steps)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        step = remaining.pop(0)
        if isinstance(step, Exception):
            raise step
        if isinstance(step, tuple):
            status, html = step
        else:
            status, html = 200, step
        headers = {"Set-Cookie": f"production_access_token={_jwt()}; Path=/"}
        return httpx.Response(status, text=html, headers=headers)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), requests


async def test_establish_session_extracts_corrected_ids_short_id():
    async with _session_client(_load_live()) as c:
        s = await dl.establish_session("https://limewire.com/d/PKQbo#s8mzZLKRe7", client=c)
    assert s.bucket_id == REAL_BUCKET          # decoded sharingBucket.id, not the decoy
    assert s.content_item_id == REAL_ITEM
    assert s.bucket_id != DECOY_KEY_ID
    assert s.file_size == 6903693              # totalFileSize, not 0
    assert s.passphrase_wrapped_pk and s.ephemeral_public_key


async def test_orphan_empty_then_empty_is_unavailable_after_two_gets():
    client, requests = _session_sequence_client(_load_orphaned(), _load_orphaned())
    async with client:
        with pytest.raises(dl.DownloadPipelineError) as ei:
            await dl.establish_session("https://limewire.com/d/xTsja#key", client=client)
    assert ei.value.kind is DownloadFailureKind.SHARE_UNAVAILABLE
    assert len(requests) == 2


async def test_orphan_empty_then_ready_recovers_after_two_gets():
    client, requests = _session_sequence_client(_load_orphaned(), _load_live())
    async with client:
        session = await dl.establish_session(
            "https://limewire.com/d/xTsja#key",
            client=client,
        )
    assert session.bucket_id == REAL_BUCKET
    assert session.content_item_id == REAL_ITEM
    assert len(requests) == 2


async def test_orphan_empty_then_removed_is_unavailable_after_two_gets():
    client, requests = _session_sequence_client(_load_orphaned(), _load_removed())
    async with client:
        with pytest.raises(dl.DownloadPipelineError) as ei:
            await dl.establish_session("https://limewire.com/d/xTsja#key", client=client)
    assert ei.value.kind is DownloadFailureKind.SHARE_UNAVAILABLE
    assert len(requests) == 2


async def test_orphan_empty_then_malformed_is_metadata_invalid_after_two_gets():
    malformed = _wrap_stream_decoded(_decoded_value({
        "sharingBucket": {"id": REAL_BUCKET},
        "contentItemList": None,
    }))
    client, requests = _session_sequence_client(_load_orphaned(), malformed)
    async with client:
        with pytest.raises(dl.DownloadPipelineError) as ei:
            await dl.establish_session("https://limewire.com/d/xTsja#key", client=client)
    assert ei.value.kind is DownloadFailureKind.METADATA_INVALID
    assert len(requests) == 2


async def test_orphan_empty_then_transient_stops_after_confirmation():
    client, requests = _session_sequence_client(_load_orphaned(), (503, "busy"))
    async with client:
        with pytest.raises(dl.DownloadPipelineError) as ei:
            await dl.establish_session("https://limewire.com/d/xTsja#key", client=client)
    assert ei.value.kind is DownloadFailureKind.TRANSIENT
    assert ei.value.immediate_retry is False
    assert len(requests) == 2


async def test_decoded_incomplete_item_never_uses_regex_fallback(monkeypatch):
    root = _decoded_value({
        "sharingBucket": {"id": REAL_BUCKET},
        "contentItemList": [{"id": REAL_ITEM}],
        "fileEncryptionKeys": [{"passphraseWrappedPrivateKey": "WRAP"}],
    })
    regex_calls = 0

    def forbidden_regex(*args, **kwargs):
        nonlocal regex_calls
        regex_calls += 1
        return {
            "bucket_id": "fabricated",
            "content_item_id": "fabricated",
            "ephemeral_public_key": "fabricated",
            "passphrase_wrapped_pk": "fabricated",
        }

    monkeypatch.setattr(dl, "_extract_ssr_metadata_regex", forbidden_regex)
    async with _session_client(_wrap_stream_decoded(root)) as client:
        with pytest.raises(dl.DownloadPipelineError) as ei:
            await dl.establish_session("https://limewire.com/d/short#key", client=client)
    assert ei.value.kind is DownloadFailureKind.METADATA_INVALID
    assert regex_calls == 0


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
    assert ei.value.kind is DownloadFailureKind.METADATA_INVALID
    assert "no usable content item" in str(ei.value)


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


# --- download pipeline: payload gate, stream-status handling, classification ---

DL_URL = "https://limewire.com/d/TEST#fragkey"
CONSTS = LimeWireConstants(sharing_salt_b64="eA==", file_iv_b64="eA==")
BLOB = b"%PDF" + bytes(996)          # a 1000-byte "PDF" object
ZIP_BLOB = b"PK\x03\x04" + bytes(996)


def test_classify_payload_units():
    assert dl._classify_payload(b"%PDF-1.7\n") == "pdf"
    for magic in (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08",
                  b"Rar!\x1a\x07\x00", b"7z\xbc\xaf\x27\x1c",
                  b"\x1f\x8b\x08\x00", b"ID3\x04", b"OggS\x00"):
        assert dl._classify_payload(magic + bytes(8)) == "unsupported", magic
    assert dl._classify_payload(b"\x00\x00\x00\x20ftypM4A ") == "unsupported"  # ftyp at offset 4
    assert dl._classify_payload(b"\x1f\x8b\x01" + bytes(8)) == "unknown"  # 2-byte gzip alone: too weak
    assert dl._classify_payload(b"\xffgarbage-not-a-container") == "unknown"
    assert dl._classify_payload(b"") == "unknown"


def test_content_range_parsing():
    assert dl._content_range_parts("bytes */242901023") == (None, 242901023)
    assert dl._content_range_parts("bytes 100-999/1000") == (100, 1000)
    assert dl._content_range_parts("bytes 0-9/*") == (0, None)
    assert dl._content_range_parts(None) == (None, None)
    assert dl._content_range_parts("garbage") == (None, None)
    assert dl._content_range_total("bytes */7") == 7


def test_retry_after_is_bounded():
    assert dl._parse_retry_after(httpx.Response(429)) == 30
    assert dl._parse_retry_after(httpx.Response(429, headers={"Retry-After": "0"})) == 1
    assert dl._parse_retry_after(httpx.Response(429, headers={"Retry-After": "99999"})) == 300


class _StubIdx:
    """Stands in for MagazineIndex so dedup lookups never touch a real DB."""

    def __init__(self, *a, **k):
        pass

    def find_by_hash(self, h):
        return None

    def close(self):
        pass


def _pipeline(monkeypatch, tmp_path, *, file_name="Issue.pdf", file_size=0,
              handler=None, heal_constants=None):
    """Wire _do_download's collaborators to fakes; returns (dest, calls)."""
    calls = {"get_url": 0, "derive": 0, "heal": 0}
    session = LimeWireSession(
        jwt_token="j", csrf_token="c", bucket_id="b", content_item_id="i",
        passphrase_wrapped_pk="", ephemeral_public_key="e",
        file_name=file_name, file_size=file_size,
    )

    async def fake_session(url, client, retries=0, rate_gate=None):
        return session

    def fake_derive(*a, **k):
        calls["derive"] += 1
        return b"\x00" * 32

    async def fake_get_url(sess, client=None):
        calls["get_url"] += 1
        return "https://storage.test/blob"

    async def fake_heal(client=None):
        calls["heal"] += 1
        return heal_constants

    monkeypatch.setattr(dl, "_establish_session_with_retry", fake_session)
    monkeypatch.setattr(dl, "derive_aes_key", fake_derive)
    monkeypatch.setattr(dl, "decrypt_file", lambda data, key, consts: bytes(data))  # identity
    monkeypatch.setattr(dl, "get_download_url", fake_get_url)
    monkeypatch.setattr(dl, "auto_extract_constants", fake_heal)
    monkeypatch.setattr("magsync.core.index.MagazineIndex", _StubIdx)
    if handler is not None:
        real_client = httpx.AsyncClient
        monkeypatch.setattr(
            httpx, "AsyncClient",
            lambda *a, **k: real_client(transport=httpx.MockTransport(handler)),
        )
    dest = tmp_path / "Mag" / "Issue.pdf"
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest, calls


def _part_for(dest: Path) -> Path:
    return dl._part_path_for(dest, DL_URL)


async def test_gate_zip_skips_before_key_derivation(tmp_path, monkeypatch):
    dest, calls = _pipeline(monkeypatch, tmp_path, file_name="The Economist Audio 06.6.2026.zip")
    part = _part_for(dest)
    part.write_bytes(b"poisoned")
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.unsupported is True and result.success is False
    assert "Unsupported payload" in result.error and ".zip" in result.error
    assert calls["derive"] == 0 and calls["get_url"] == 0  # gated before any expensive work
    assert not part.exists()                               # poisoned .part reclaimed


async def test_gate_unlink_failure_still_unsupported(tmp_path, monkeypatch, caplog):
    dest, calls = _pipeline(monkeypatch, tmp_path, file_name="X.zip")
    part = _part_for(dest)
    part.write_bytes(b"data")

    def bad_unlink(self, missing_ok=False):
        raise OSError("read-only filesystem")

    monkeypatch.setattr(Path, "unlink", bad_unlink)
    with caplog.at_level(logging.WARNING, logger="magsync"):
        result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.unsupported is True  # outcome survives cleanup failure
    assert any("Could not remove partial file" in r.getMessage() for r in caplog.records)


async def test_gate_ambiguous_suffix_falls_through_and_downloads(tmp_path, monkeypatch):
    def handler(request):
        assert "range" not in {k.lower() for k in request.headers}  # fresh download
        return httpx.Response(200, content=BLOB)

    dest, calls = _pipeline(monkeypatch, tmp_path, file_name="Issue 06.6.2026", handler=handler)
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is True
    assert dest.read_bytes() == BLOB
    assert not _part_for(dest).exists()


async def test_416_truncates_poisoned_part_and_completes(tmp_path, monkeypatch):
    seen = {}

    def handler(request):
        seen["range"] = request.headers.get("Range")
        return httpx.Response(416, content=b"<Error>xml</Error>",
                              headers={"Content-Range": f"bytes */{len(BLOB)}"})

    dest, calls = _pipeline(monkeypatch, tmp_path, handler=handler)
    part = _part_for(dest)
    junk = b"<Error>appended by old versions</Error>" * 16
    part.write_bytes(BLOB + junk)  # poisoned: complete object + junk tail
    progress: list[tuple[int, int]] = []
    result = await dl._do_download(
        DL_URL, dest, constants=CONSTS,
        on_progress=lambda cur, tot: progress.append((cur, tot)),
    )
    assert result.success is True
    assert seen["range"] == f"bytes={len(BLOB) + len(junk)}-"
    assert dest.read_bytes() == BLOB                              # junk excluded from output
    assert result.sha256 == hashlib.sha256(BLOB).hexdigest()      # ...and from the dedup hash
    assert progress[-1] == (len(BLOB), len(BLOB))                 # final tick, transferless completion
    assert not part.exists()


async def test_416_total_beyond_local_is_transient(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(416, content=b"x", headers={"Content-Range": "bytes */1000"})

    dest, calls = _pipeline(monkeypatch, tmp_path, handler=handler)
    part = _part_for(dest)
    part.write_bytes(bytes(500))
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is False and result.unsupported is False
    assert "unverifiable" in result.error
    assert part.read_bytes() == bytes(500)  # error body never written
    assert calls["heal"] == 0


async def test_416_without_content_range_confirms_via_ssr_exact_match(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(416, content=b"x")  # no Content-Range header

    dest, calls = _pipeline(monkeypatch, tmp_path, file_size=len(BLOB), handler=handler)
    _part_for(dest).write_bytes(BLOB)
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is True and dest.read_bytes() == BLOB


async def test_416_without_content_range_or_ssr_is_transient(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(416, content=b"x")

    dest, calls = _pipeline(monkeypatch, tmp_path, file_size=0, handler=handler)
    part = _part_for(dest)
    part.write_bytes(BLOB)
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is False and "unverifiable" in result.error
    assert part.read_bytes() == BLOB  # never manufactured completion, never truncated


async def test_206_resume_appends_at_verified_offset(tmp_path, monkeypatch):
    head, tail = BLOB[:100], BLOB[100:]

    def handler(request):
        assert request.headers["Range"] == "bytes=100-"
        return httpx.Response(206, content=tail,
                              headers={"Content-Range": f"bytes 100-999/{len(BLOB)}"})

    dest, calls = _pipeline(monkeypatch, tmp_path, handler=handler)
    _part_for(dest).write_bytes(head)
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is True and dest.read_bytes() == BLOB


async def test_206_wrong_offset_rejected_without_write(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(206, content=BLOB,
                              headers={"Content-Range": f"bytes 0-999/{len(BLOB)}"})

    dest, calls = _pipeline(monkeypatch, tmp_path, handler=handler)
    part = _part_for(dest)
    part.write_bytes(BLOB[:100])
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is False and "offset mismatch" in result.error
    assert part.read_bytes() == BLOB[:100]  # mis-offset body never spliced (CTR is positional)


async def test_200_after_range_restarts_from_zero(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(200, content=BLOB)  # server ignored the Range

    dest, calls = _pipeline(monkeypatch, tmp_path, handler=handler)
    _part_for(dest).write_bytes(b"stale-bytes-from-before")
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is True and dest.read_bytes() == BLOB


async def test_http_error_body_never_written(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(500, content=b"<Error>storage exploded</Error>")

    dest, calls = _pipeline(monkeypatch, tmp_path, handler=handler)
    part = _part_for(dest)
    part.write_bytes(BLOB[:100])
    result = await dl._download_and_decrypt_once(
        DL_URL,
        dest,
        constants=CONSTS,
    )
    assert result.failure_kind is DownloadFailureKind.TRANSIENT
    assert part.read_bytes() == BLOB[:100]


async def test_short_fetch_is_transient_without_self_heal(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(206, content=BLOB[100:500],
                              headers={"Content-Range": f"bytes 100-999/{len(BLOB)}"})

    dest, calls = _pipeline(monkeypatch, tmp_path, handler=handler)
    part = _part_for(dest)
    part.write_bytes(BLOB[:100])
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is False and "incomplete download" in result.error
    assert part.stat().st_size == 500  # kept for resume
    assert calls["heal"] == 0          # not a crypto problem


async def test_zip_payload_detected_by_magic_is_terminal(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(200, content=ZIP_BLOB)

    dest, calls = _pipeline(monkeypatch, tmp_path, file_name="Issue 06.6.2026", handler=handler)
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.unsupported is True
    assert calls["heal"] == 0          # decryption worked; healing is pointless
    assert not _part_for(dest).exists()
    assert not dest.exists()


async def test_unknown_magic_heals_once_then_keeps_part(tmp_path, monkeypatch):
    garbage = b"\x81\x9anot-a-known-container" + bytes(976)

    def handler(request):
        return httpx.Response(200, content=garbage)

    dest, calls = _pipeline(monkeypatch, tmp_path, handler=handler, heal_constants=CONSTS)
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is False and result.unsupported is False
    assert result.error == dl._DECRYPT_FAILED_MSG
    assert calls["heal"] == 1
    assert _part_for(dest).exists()    # kept: next attempt is one probe + a local decrypt


async def test_ssr_under_report_never_truncates_good_download(tmp_path, monkeypatch):
    # SSR claims 700 bytes; storage says 1000. Nothing may be cut to 700.
    def handler(request):
        return httpx.Response(416, content=b"x",
                              headers={"Content-Range": f"bytes */{len(BLOB)}"})

    dest, calls = _pipeline(monkeypatch, tmp_path, file_size=700, handler=handler)
    _part_for(dest).write_bytes(BLOB)
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is True
    assert dest.read_bytes() == BLOB   # full object — SSR size was ignored


async def test_ssr_over_report_resolved_by_probe(tmp_path, monkeypatch):
    # SSR claims 1500 bytes; the object is 1000 and fully on disk. One ranged
    # probe (416) resolves it — no eternal "incomplete" limbo.
    def handler(request):
        return httpx.Response(416, content=b"x",
                              headers={"Content-Range": f"bytes */{len(BLOB)}"})

    dest, calls = _pipeline(monkeypatch, tmp_path, file_size=1500, handler=handler)
    _part_for(dest).write_bytes(BLOB)
    result = await dl._do_download(DL_URL, dest, constants=CONSTS)
    assert result.success is True and result.file_size_bytes == len(BLOB)


async def test_download_and_decrypt_returns_unsupported_without_retry(monkeypatch, tmp_path):
    calls = {"n": 0}

    async def fake_once(url, dest, *, constants, on_progress=None, rate_gate=None, retry_attempts=2):
        calls["n"] += 1
        return DownloadResult(success=False, unsupported=True, error="Unsupported payload: x.zip")

    monkeypatch.setattr(dl, "_download_and_decrypt_once", fake_once)
    result = await dl.download_and_decrypt(DL_URL, tmp_path / "x.pdf", constants=CONSTS, retry_attempts=3)
    assert calls["n"] == 1             # terminal: no retries, no backoff
    assert result.unsupported is True


async def test_download_and_decrypt_no_retry_on_deterministic_decrypt_failure(monkeypatch, tmp_path):
    calls = {"n": 0}

    async def fake_once(url, dest, *, constants, on_progress=None, rate_gate=None, retry_attempts=2):
        calls["n"] += 1
        return DownloadResult(
            success=False,
            failure_kind=DownloadFailureKind.DECRYPTION_FAILED,
            error=dl._DECRYPT_FAILED_MSG,
        )

    monkeypatch.setattr(dl, "_download_and_decrypt_once", fake_once)
    result = await dl.download_and_decrypt(DL_URL, tmp_path / "x.pdf", constants=CONSTS, retry_attempts=3)
    assert calls["n"] == 1             # deterministic within one run — retrying repeats the same decrypt
    assert result.error == dl._DECRYPT_FAILED_MSG


# --- exact physical request counts for typed retry ownership ---


class _NoopGate:
    def __init__(self):
        self.triggers: list[int] = []

    async def wait(self):
        return None

    async def trigger(self, retry_after=30, *, reason="Rate limited (429)"):
        self.triggers.append(retry_after)


def _install_transport(monkeypatch, handler):
    def client_factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_ASYNC_CLIENT(*args, **kwargs)

    monkeypatch.setattr(dl.httpx, "AsyncClient", client_factory)


def _cookie_headers() -> dict[str, str]:
    return {"Set-Cookie": f"production_access_token={_jwt()}; Path=/"}


async def _disable_backoff(monkeypatch) -> list[float]:
    sleeps: list[float] = []

    async def record_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(dl.asyncio, "sleep", record_sleep)
    monkeypatch.setattr(dl.random, "uniform", lambda low, high: 0.125)
    return sleeps


async def test_removed_share_uses_one_physical_get_with_large_retry_budget(
    tmp_path,
    monkeypatch,
):
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, text=DEAD_CURRENT)

    _install_transport(monkeypatch, handler)
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/Dead#key",
        tmp_path / "x.pdf",
        constants=CONSTS,
        retry_attempts=9,
        rate_gate=_NoopGate(),
    )
    assert result.failure_kind is DownloadFailureKind.SHARE_UNAVAILABLE
    assert result.attempt_count == 1
    assert len(requests) == 1


async def test_orphan_confirmation_transient_is_not_multiplied_by_retry_budget(
    tmp_path,
    monkeypatch,
):
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(200, text=_load_orphaned(), headers=_cookie_headers())
        return httpx.Response(503, text="busy")

    _install_transport(monkeypatch, handler)
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/xTsja#key",
        tmp_path / "x.pdf",
        constants=CONSTS,
        retry_attempts=9,
        rate_gate=_NoopGate(),
    )
    assert result.failure_kind is DownloadFailureKind.TRANSIENT
    assert result.attempt_count == 1
    assert len(requests) == 2


async def test_transient_share_5xx_uses_one_configured_retry_budget(
    tmp_path,
    monkeypatch,
):
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(503, text="busy")

    _install_transport(monkeypatch, handler)
    sleeps = await _disable_backoff(monkeypatch)
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/Busy#key",
        tmp_path / "x.pdf",
        constants=CONSTS,
        retry_attempts=2,
        rate_gate=_NoopGate(),
    )
    assert result.failure_kind is DownloadFailureKind.TRANSIENT
    assert result.attempt_count == 3
    assert len(requests) == 3
    assert sleeps == [2.125, 4.125]


async def test_share_429_uses_bounded_gate_and_exact_request_budget(
    tmp_path,
    monkeypatch,
):
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(429, headers={"Retry-After": "99999"})

    _install_transport(monkeypatch, handler)
    await _disable_backoff(monkeypatch)
    gate = _NoopGate()
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/Busy#key",
        tmp_path / "x.pdf",
        constants=CONSTS,
        retry_attempts=2,
        rate_gate=gate,
    )
    assert result.failure_kind is DownloadFailureKind.TRANSIENT
    assert result.attempt_count == 3
    assert len(requests) == 3
    assert gate.triggers == [300, 300, 300]


async def test_metadata_invalid_page_gets_no_retry(tmp_path, monkeypatch):
    malformed = _wrap_stream_decoded(_decoded_value({
        "sharingBucket": {"id": REAL_BUCKET},
        "contentItemList": None,
    }))
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, text=malformed, headers=_cookie_headers())

    _install_transport(monkeypatch, handler)
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/Bad#key",
        tmp_path / "x.pdf",
        constants=CONSTS,
        retry_attempts=9,
        rate_gate=_NoopGate(),
    )
    assert result.failure_kind is DownloadFailureKind.METADATA_INVALID
    assert result.attempt_count == 1
    assert len(requests) == 1


async def test_invalid_crypto_configuration_gets_no_retry_or_request(
    tmp_path,
    monkeypatch,
):
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(500)

    _install_transport(monkeypatch, handler)
    invalid = LimeWireConstants(
        sharing_salt_b64="not-base64!",
        file_iv_b64="eA==",
    )
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/BadConfig#key",
        tmp_path / "x.pdf",
        constants=invalid,
        retry_attempts=9,
        rate_gate=_NoopGate(),
    )
    assert result.failure_kind is DownloadFailureKind.CONFIGURATION
    assert result.attempt_count == 1
    assert requests == []


async def test_known_unsupported_extension_stops_after_share_get(tmp_path, monkeypatch):
    root = _decoded_value({
        "sharingBucket": {
            "id": REAL_BUCKET,
            "name": "The Economist Audio.zip",
            "totalFileSize": 10,
        },
        "contentItemList": [{
            "id": REAL_ITEM,
            "ephemeralPublicKey": "EPK",
            "baseFileEncryptionKeyId": "key-1",
        }],
        "fileEncryptionKeys": [{
            "id": "key-1",
            "passphraseWrappedPrivateKey": "WRAP",
        }],
    })
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            text=_wrap_stream_decoded(root),
            headers=_cookie_headers(),
        )

    _install_transport(monkeypatch, handler)
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/Audio#key",
        tmp_path / "x.pdf",
        constants=CONSTS,
        retry_attempts=9,
        rate_gate=_NoopGate(),
    )
    assert result.failure_kind is DownloadFailureKind.UNSUPPORTED
    assert result.attempt_count == 1
    assert len(requests) == 1


async def test_deterministic_decrypt_failure_makes_one_full_external_attempt(
    tmp_path,
    monkeypatch,
):
    requests: list[httpx.Request] = []
    garbage = b"not-a-pdf-or-known-container" + bytes(100)

    def handler(request):
        requests.append(request)
        if request.url.host == "limewire.com":
            return httpx.Response(200, text=_load_live(), headers=_cookie_headers())
        if request.url.host == "api.limewire.com":
            return httpx.Response(200, json={
                "contentItems": [{"downloadUrl": "https://storage.test/blob?secret=value"}],
            })
        return httpx.Response(200, content=garbage)

    async def same_constants(client=None):
        return CONSTS

    _install_transport(monkeypatch, handler)
    monkeypatch.setattr(dl, "derive_aes_key", lambda *args, **kwargs: bytes(32))
    monkeypatch.setattr(dl, "decrypt_file", lambda data, key, constants: bytes(data))
    monkeypatch.setattr(dl, "auto_extract_constants", same_constants)
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/PKQbo#key",
        tmp_path / "x.pdf",
        constants=CONSTS,
        retry_attempts=9,
        rate_gate=_NoopGate(),
    )
    assert result.failure_kind is DownloadFailureKind.DECRYPTION_FAILED
    assert result.attempt_count == 1
    assert [request.url.host for request in requests] == [
        "limewire.com",
        "api.limewire.com",
        "storage.test",
    ]


async def test_api_404_is_share_unavailable_but_storage_404_is_transient(
    tmp_path,
    monkeypatch,
):
    api_requests: list[httpx.Request] = []

    def api_404_handler(request):
        api_requests.append(request)
        if request.url.host == "limewire.com":
            return httpx.Response(200, text=_load_live(), headers=_cookie_headers())
        return httpx.Response(404)

    _install_transport(monkeypatch, api_404_handler)
    monkeypatch.setattr(dl, "derive_aes_key", lambda *args, **kwargs: bytes(32))
    api_result = await dl.download_and_decrypt(
        "https://limewire.com/d/PKQbo#key",
        tmp_path / "api.pdf",
        constants=CONSTS,
        retry_attempts=3,
        rate_gate=_NoopGate(),
    )
    assert api_result.failure_kind is DownloadFailureKind.SHARE_UNAVAILABLE
    assert len(api_requests) == 2

    storage_requests: list[httpx.Request] = []

    def storage_404_handler(request):
        storage_requests.append(request)
        if request.url.host == "limewire.com":
            return httpx.Response(200, text=_load_live(), headers=_cookie_headers())
        if request.url.host == "api.limewire.com":
            return httpx.Response(200, json={
                "contentItems": [{"downloadUrl": "https://storage.test/blob"}],
            })
        return httpx.Response(404)

    _install_transport(monkeypatch, storage_404_handler)
    await _disable_backoff(monkeypatch)
    storage_result = await dl.download_and_decrypt(
        "https://limewire.com/d/PKQbo#key",
        tmp_path / "storage.pdf",
        constants=CONSTS,
        retry_attempts=1,
        rate_gate=_NoopGate(),
    )
    assert storage_result.failure_kind is DownloadFailureKind.TRANSIENT
    assert storage_result.attempt_count == 2
    assert len(storage_requests) == 6


async def test_rotated_transient_contract_is_one_immediate_attempt(
    tmp_path,
    monkeypatch,
):
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(503)

    _install_transport(monkeypatch, handler)
    result = await dl.download_and_decrypt(
        "https://limewire.com/d/Rotated#key",
        tmp_path / "x.pdf",
        constants=CONSTS,
        retry_attempts=0,
        rate_gate=_NoopGate(),
    )
    assert result.failure_kind is DownloadFailureKind.TRANSIENT
    assert result.attempt_count == 1
    assert len(requests) == 1
