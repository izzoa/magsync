"""Tests for session-retry flooring, throttle, and the rate-limit gate."""

from __future__ import annotations

import asyncio
import logging

import pytest

import magsync.core.downloader as dl
from magsync.core.downloader import RateLimitGate, _establish_session_with_retry

URL = "https://limewire.com/d/x#k"


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
