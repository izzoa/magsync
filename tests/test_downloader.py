"""Tests for session-retry flooring, throttle, and the rate-limit gate."""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

import magsync.core.downloader as dl
from magsync.core.downloader import RateLimitGate, _establish_session_with_retry

URL = "https://limewire.com/d/x#k"

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
