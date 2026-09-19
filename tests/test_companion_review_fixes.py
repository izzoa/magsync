"""Fixes for the post-commit review of 2940b73.

Each test pins one defect: concurrent passes staged duplicate exports,
completed requests fell back to ``queued`` before publication, commands for
an issue already in flight succeeded with no outcome, waiters gave up on a
live owner that stopped accepting, and no transaction may span an await.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time

import pytest

from magsync.companion.exports import Exports
from magsync.companion.local import LocalWaiter, accept_local
from magsync.companion.protocol import ProtocolError
from magsync.companion.store import Limits, Store, timestamp
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus
from magsync.core.scraper import ScrapedIssue
from test_companion_hardening import LW, _pdf, add_issue, completing_batch, make_runtime, op_state, remote_scope

FAST = dict(minimum_free_bytes=1024, maximum_download_bytes=1024 * 1024, command_poll_seconds=0.02)


def gated_batch(tmp_path, done):
    """A completing batch that holds every transfer until released."""
    entered, release = asyncio.Event(), asyncio.Event()
    inner = completing_batch(tmp_path, done)

    async def batch(issues, cfg, index, **kwargs):
        entered.set()
        await release.wait()
        return await inner(issues, cfg, index, **kwargs)
    return batch, entered, release


def request_states(store, request_id):
    rows = store.conn.execute("SELECT kind,payload FROM client_events WHERE resource_id=? ORDER BY seq",
                              (request_id,)).fetchall()
    return [json.loads(row["payload"])["state"] for row in rows if row["kind"] == "request.updated"]


def count(store, table):
    return store.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def operation_row(store, operation_id):
    return dict(store.conn.execute("SELECT * FROM operations WHERE id=?", (operation_id,)).fetchone())


# ---------------------------------------------------------------------------
# 12.1 one publisher per issue
# ---------------------------------------------------------------------------

async def test_concurrent_publication_of_one_issue_stages_one_export(tmp_path, monkeypatch):
    runtime, idx, _ = make_runtime(tmp_path, limits=Limits(**FAST))
    issue = add_issue(idx, "Remote Title - June 2025", slug="r")
    data = _pdf(issue)
    original = tmp_path / "r.pdf"
    original.write_bytes(data)
    idx.update_download_status(issue, DownloadStatus.COMPLETE, str(original), len(data), hashlib.sha256(data).hexdigest())
    real_verify = Exports.verify

    def slow_verify(path, *args, **kwargs):
        time.sleep(0.1)  # Hold each staging step in its worker thread so the passes interleave.
        return real_verify(path, *args, **kwargs)

    monkeypatch.setattr(Exports, "verify", staticmethod(slow_verify))
    await runtime.start(background=False)
    store = runtime.store
    try:
        client, scope = remote_scope(store)
        request = store.create_request(client, scope, {"issue_id": store.provider_issue(issue)})
        # The work loop's pass and a command's pass publish the same issue at once.
        await asyncio.gather(runtime._publish_retained(), runtime._publish_retained([request["id"]]))
        assert count(store, "content_objects") == 1
        assert count(store, "deliveries") == 1
        assert len(list((tmp_path / "exports").glob("*.pdf"))) == 1
        assert store.request(client, request["id"])["state"] == "fulfilled"
    finally:
        await runtime.stop()
        idx.close()


# ---------------------------------------------------------------------------
# 12.2 requests stay acquiring until publication
# ---------------------------------------------------------------------------

async def test_completed_request_moves_from_acquiring_to_fulfilled(tmp_path):
    done = []
    runtime, idx, _ = make_runtime(tmp_path, batch=completing_batch(tmp_path, done), limits=Limits(**FAST))
    issue = add_issue(idx, "Remote Title - June 2025", slug="r")
    await runtime.start(background=False)
    store = runtime.store
    try:
        client, scope = remote_scope(store)
        request = store.create_request(client, scope, {"issue_id": store.provider_issue(issue)})
        await runtime.acquire()
        assert done == [issue]
        assert request_states(store, request["id"]) == ["acquiring", "fulfilled"]
    finally:
        await runtime.stop()
        idx.close()


async def test_unpublishable_download_returns_its_requests_to_queued(tmp_path):
    async def corrupt(issues, _cfg, index, **_kwargs):
        for issue in issues:
            path = tmp_path / f"lib-{issue['id']}.pdf"
            path.write_bytes(b"not a pdf")
            index.update_download_status(issue["id"], DownloadStatus.COMPLETE, str(path), 9,
                                         hashlib.sha256(b"not a pdf").hexdigest())
        return [{"issue": issue, "success": True, "error": None, "failure_kind": None} for issue in issues]

    runtime, idx, _ = make_runtime(tmp_path, batch=corrupt, limits=Limits(**FAST))
    issue = add_issue(idx, "Remote Title - June 2025", slug="r")
    await runtime.start(background=False)
    store = runtime.store
    try:
        client, scope = remote_scope(store)
        request = store.create_request(client, scope, {"issue_id": store.provider_issue(issue)})
        await runtime.acquire()
        document = store.request(client, request["id"])
        assert document["state"] == "queued" and document["fulfillment_error"] == "integrity_failed"
        assert request_states(store, request["id"])[-1] == "queued"
        assert count(store, "deliveries") == 0
    finally:
        await runtime.stop()
        idx.close()


# ---------------------------------------------------------------------------
# 12.3 selected acquisitions join transfers in flight
# ---------------------------------------------------------------------------

async def test_request_for_an_issue_in_flight_reports_that_transfer(tmp_path):
    done = []
    batch, entered, release = gated_batch(tmp_path, done)
    runtime, idx, _ = make_runtime(tmp_path, batch=batch, limits=Limits(**FAST))
    issue = add_issue(idx, "Remote Title - June 2025", slug="r")
    await runtime.start(background=False, accept_commands=True)
    store = runtime.store
    try:
        client, first = remote_scope(store, "first")
        second = store.create_scope(client, {"external_id": "second", "label": "second"})["id"]
        body = {"issue_id": store.provider_issue(issue)}
        store.create_request(client, first, body)
        transfer = asyncio.create_task(runtime.acquire())  # The work loop's transfer.
        await asyncio.wait_for(entered.wait(), 5)
        accepted = store.accept(client, "request.create", second, body, "join",
                                lambda: store.create_request(client, second, body))
        command = asyncio.create_task(runtime.execute(operation_row(store, accepted["operation_id"])))
        await asyncio.sleep(0.2)
        assert not command.done()  # It waits for the transfer instead of succeeding early.
        release.set()
        await asyncio.wait_for(asyncio.gather(transfer, command), 5)
        operation = store.operation(client, accepted["operation_id"])
        assert operation["state"] == "succeeded"
        assert [outcome["status"] for outcome in operation["result"]["outcomes"]] == ["complete"]
        assert operation["result"]["physical_attempts"] == 0
        assert done == [issue]
        assert count(store, "content_objects") == 1 and count(store, "deliveries") == 2
    finally:
        release.set()
        await runtime.stop()
        idx.close()


async def test_retry_accepted_during_a_transfer_reports_its_outcome(tmp_path):
    done = []
    batch, entered, release = gated_batch(tmp_path, done)
    runtime, idx, _ = make_runtime(tmp_path, batch=batch, limits=Limits(**FAST))
    issue = add_issue(idx, "Remote Title - June 2025", slug="r")
    await runtime.start(background=False, accept_commands=True)
    store = runtime.store
    try:
        client, scope = remote_scope(store)
        request = store.create_request(client, scope, {"issue_id": store.provider_issue(issue)})
        transfer = asyncio.create_task(runtime.acquire())
        await asyncio.wait_for(entered.wait(), 5)
        # As the API accepts a retry while the issue is downloading: attached.
        accepted = store.accept(client, "request.retry", scope, {"request_id": request["id"], "revision": 2}, "retry")
        with store.transaction():
            store.conn.execute("INSERT INTO operation_requests VALUES(?,?)", (accepted["operation_id"], request["id"]))
            store.conn.execute("UPDATE operations SET result=? WHERE id=?", ('{"attached":true}', accepted["operation_id"]))
        command = asyncio.create_task(runtime.execute(operation_row(store, accepted["operation_id"])))
        await asyncio.sleep(0.2)
        assert not command.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(transfer, command), 5)
        result = store.operation(client, accepted["operation_id"])["result"]
        assert result["attached"] and [outcome["status"] for outcome in result["outcomes"]] == ["complete"]
        assert done == [issue]
    finally:
        release.set()
        await runtime.stop()
        idx.close()


async def test_runtime_fetch_reports_an_issue_the_daemon_is_downloading(tmp_path, capsys):
    import magsync.cli as cli

    done = []
    batch, entered, release = gated_batch(tmp_path, done)
    items = [ScrapedIssue(title="Science News - June 2025", page_url="https://freemagazines.top/sn/",
                          limewire_url=LW.format("sn"))]
    runtime, idx, _ = make_runtime(tmp_path, items=items, batch=batch, limits=Limits(**FAST))
    issue = add_issue(idx, "Science News - June 2025", slug="sn", manual=True)
    await runtime.start(background=False, accept_commands=True)
    store = runtime.store
    try:
        transfer = asyncio.create_task(runtime.acquire())  # The daemon's automatic download.
        await asyncio.wait_for(entered.wait(), 5)
        body = {"query": "Science News", "since": None}
        fetch = accept_local(store, "fetch", body)
        command = asyncio.create_task(runtime.execute(operation_row(store, fetch)))
        await asyncio.sleep(0.2)
        assert not command.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(transfer, command), 5)
        operation = store.operation("local", fetch)
        assert operation["result"]["pending"] == 1
        assert [outcome["status"] for outcome in operation["result"]["outcomes"]] == ["complete"]
        assert done == [issue]
        capsys.readouterr()
        assert cli._render_fetch("fetch", operation, body, store) == 0
        output = capsys.readouterr().out
        assert "✓ Science News - June 2025: downloaded" in output
        assert "Fetched 1 issue: 1 downloaded." in output
    finally:
        release.set()
        await runtime.stop()
        idx.close()


async def test_joined_transfer_left_pending_is_claimed_again(tmp_path):
    done = []
    entered, release = asyncio.Event(), asyncio.Event()
    inner = completing_batch(tmp_path, done)
    calls = []

    async def pause_first(issues, cfg, index, **kwargs):
        calls.append([issue["id"] for issue in issues])
        if len(calls) == 1:
            entered.set()
            await release.wait()
            return []  # Returned without an outcome, as a capacity pause does.
        return await inner(issues, cfg, index, **kwargs)

    runtime, idx, _ = make_runtime(tmp_path, batch=pause_first, limits=Limits(**FAST))
    issue = add_issue(idx, "Remote Title - June 2025", slug="r")
    await runtime.start(background=False, accept_commands=True)
    store = runtime.store
    try:
        client, scope = remote_scope(store)
        body = {"issue_id": store.provider_issue(issue)}
        store.create_request(client, scope, body)
        transfer = asyncio.create_task(runtime.acquire())
        await asyncio.wait_for(entered.wait(), 5)
        accepted = store.accept(client, "request.create", scope, body, "join",
                                lambda: store.create_request(client, scope, body))
        command = asyncio.create_task(runtime.execute(operation_row(store, accepted["operation_id"])))
        await asyncio.sleep(0.1)
        release.set()
        await asyncio.wait_for(asyncio.gather(transfer, command), 5)
        result = store.operation(client, accepted["operation_id"])["result"]
        assert calls == [[issue], [issue]] and done == [issue]
        assert [outcome["status"] for outcome in result["outcomes"]] == ["complete"]
    finally:
        release.set()
        await runtime.stop()
        idx.close()


# ---------------------------------------------------------------------------
# 12.4 waiters follow liveness, not acceptance
# ---------------------------------------------------------------------------

def test_waiter_keeps_waiting_while_the_owner_lives_without_accepting(tmp_path):
    idx = MagazineIndex(tmp_path / "index.db")
    store = Store(idx, Limits())
    queued = accept_local(store, "update", {})
    running = accept_local(store, "update", {})
    with store.transaction():  # Draining before a stop: alive, heartbeating, not accepting.
        store.conn.execute("UPDATE runtime_state SET accepting=0,owner_id='daemon',heartbeat_at=? WHERE id=1",
                           (timestamp(),))
        store.conn.execute("UPDATE operations SET state='running' WHERE id=?", (running,))
    assert LocalWaiter(store, running, store.limits)._step() is None
    assert LocalWaiter(store, queued, store.limits)._step() is None
    assert op_state(store, queued)[0] == "queued"
    with store.transaction():  # The owner released the store.
        store.conn.execute("UPDATE runtime_state SET owner_id=NULL WHERE id=1")
    with pytest.raises(ProtocolError):
        LocalWaiter(store, running, store.limits)._step()
    with pytest.raises(ProtocolError):
        LocalWaiter(store, queued, store.limits)._step()
    assert op_state(store, queued) == ("failed", {"code": "runtime_unavailable"})
    idx.close()


async def test_heartbeat_continues_without_acceptance_while_a_stop_drains(tmp_path):
    done = []
    batch, entered, release = gated_batch(tmp_path, done)
    runtime, idx, _ = make_runtime(tmp_path, batch=batch, limits=Limits(**FAST, heartbeat_seconds=0.05))
    issue = add_issue(idx, "Science News - June 2025", slug="sn", manual=True)
    await runtime.start(background=True)
    store = runtime.store

    def liveness():
        return dict(store.conn.execute("SELECT accepting,owner_id,heartbeat_at FROM runtime_state WHERE id=1").fetchone())

    try:
        await asyncio.wait_for(entered.wait(), 5)
        stopping = asyncio.create_task(runtime.stop())
        await asyncio.sleep(0.1)
        before = liveness()
        await asyncio.sleep(0.25)
        during = liveness()
        assert during["accepting"] == 0 and during["owner_id"]
        assert during["heartbeat_at"] > before["heartbeat_at"]
        release.set()
        await asyncio.wait_for(stopping, 10)
        assert done == [issue]  # The in-flight transfer finished within the grace period.
        assert liveness()["owner_id"] is None
    finally:
        release.set()
        idx.close()


# ---------------------------------------------------------------------------
# 12.8 no transaction spans an await
# ---------------------------------------------------------------------------

async def test_no_transaction_is_open_at_any_await_of_acquisition_and_publication(tmp_path, monkeypatch):
    observed = []
    done = []
    inner = completing_batch(tmp_path, done)

    async def batch(issues, cfg, index, **kwargs):
        for _ in issues:
            observed.append(("batch", index.conn.in_transaction))
            await asyncio.sleep(0)
        return await inner(issues, cfg, index, **kwargs)

    runtime, idx, _ = make_runtime(tmp_path, batch=batch, limits=Limits(**FAST))
    real_to_thread = asyncio.to_thread

    async def watched_to_thread(function, *args, **kwargs):
        observed.append(("thread", idx.conn.in_transaction))
        return await real_to_thread(function, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", watched_to_thread)
    first = add_issue(idx, "Remote Title - June 2025", slug="r")
    second = add_issue(idx, "Remote Title - July 2025", slug="r2", month=7)
    await runtime.start(background=False)
    store = runtime.store
    try:
        client, scope = remote_scope(store)
        for issue in (first, second):
            store.create_request(client, scope, {"issue_id": store.provider_issue(issue)})
        await runtime.acquire()
        assert sorted(done) == sorted([first, second]) and count(store, "deliveries") == 2
        # Worker-thread hops cover publication: source verify and staged copy per issue.
        assert sum(1 for kind, _ in observed if kind == "thread") >= 4
        assert sum(1 for kind, _ in observed if kind == "batch") == 2
        assert not any(in_transaction for _, in_transaction in observed)
    finally:
        await runtime.stop()
        idx.close()
