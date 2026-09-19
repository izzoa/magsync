"""Runtime hardening: recovery, waiters, containment, loops, reconciliation, exports.

Each test pins one defect found in the 2026-09-18 review of the 0.9.0 work.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import sqlite3
from types import SimpleNamespace

import pytest

from magsync.companion.local import LocalWaiter, StillRunning, accept_local
from magsync.companion.ownership import Ownership
from magsync.companion.protocol import ERRORS
from magsync.companion.runtime import Runtime, claimable_candidates
from magsync.companion.store import Limits, Store, timestamp
from magsync.config import Config, load_config
from magsync.core.index import MagazineIndex
from magsync.core.matching import matches_subscription
from magsync.core.models import DownloadStatus, SourceResult, Subscription
from magsync.core.scraper import ScrapedIssue

PDF = b"%PDF-1.7\ncontrolled test fixture\n%%EOF\n"
LW = "https://limewire.com/d/{}#key"


class Source:
    """Scripted source client; records every search and its keyword arguments."""

    def __init__(self, items=None):
        self.items = list(items or [])
        self.calls: list[tuple[str, dict]] = []
        self.circuit_open = False
        self.circuit_failure = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    async def search_with_details(self, query, **kwargs):
        self.calls.append((query, kwargs))
        if not self.items:
            return SourceResult(validated_empty=True)
        return SourceResult(items=list(self.items))


def _pdf(issue_id: int) -> bytes:
    return PDF.replace(b"%%EOF", f"%{issue_id}\n%%EOF".encode())


def completing_batch(tmp_path, done: list):
    async def batch(issues, _cfg, index, on_start=None, **_kwargs):
        results = []
        for issue in issues:
            if on_start is not None:  # As the real batch does, before each transfer.
                on_start(issue)
            data = _pdf(issue["id"])
            path = tmp_path / f"lib-{issue['id']}.pdf"
            path.write_bytes(data)
            index.update_download_status(issue["id"], DownloadStatus.COMPLETE, str(path), len(data),
                                         hashlib.sha256(data).hexdigest())
            done.append(issue["id"])
            results.append({"issue": issue, "success": True, "error": None, "failure_kind": None})
        return results
    return batch


def add_issue(idx, title, *, slug=None, url=True, year=2025, month=6, manual=False) -> int:
    slug = slug or title.lower().replace(" ", "-")
    magazine = idx.get_or_create_magazine(title.split(" - ")[0], title.split(" - ")[0].lower())
    page = f"https://freemagazines.top/{slug}/"
    idx.add_issues(magazine, [{"title": title, "page_url": page, "limewire_url": LW.format(slug) if url else None,
                               "year": year, "month": month}])
    issue_id = idx.conn.execute("SELECT id FROM issues WHERE page_url=?", (page,)).fetchone()[0]
    if manual:
        idx.mark_manual([issue_id])
    return issue_id


def make_runtime(tmp_path, *, subscriptions=(), items=None, limits=None, batch=None, **options):
    idx = MagazineIndex(tmp_path / "index.db")
    snapshot = list(subscriptions)
    cfg = Config(output_dir=str(tmp_path / "out"), subscriptions=snapshot)
    source = Source(items)
    runtime = Runtime(idx, cfg, limits=limits or Limits(minimum_free_bytes=1024, maximum_download_bytes=1024 * 1024),
                      exports=tmp_path / "exports", source_factory=lambda **_: source, batch=batch,
                      require_initialized=False, subscription_loader=lambda: list(snapshot), **options)
    return runtime, idx, source


def op_state(store, operation_id):
    row = store.conn.execute("SELECT state,result FROM operations WHERE id=?", (operation_id,)).fetchone()
    return row["state"], json.loads(row["result"])


def remote_scope(store, external="library"):
    credential = store.provision("Consumer")
    scope = store.create_scope(credential["client_id"], {"external_id": external, "label": external})
    return credential["client_id"], scope["id"]


# ---------------------------------------------------------------------------
# 3.2 recovery and acceptance
# ---------------------------------------------------------------------------

def test_recovery_terminates_local_and_bounds_remote_replays(tmp_path):
    idx = MagazineIndex(tmp_path / "index.db")
    store = Store(idx)
    store.initialize()
    client, scope = remote_scope(store)
    local_op = accept_local(store, "update", {})
    remote_op = store.accept(client, "search", None, {"query": "x", "pages": 1}, "key")["operation_id"]
    output, exports = tmp_path / "out", tmp_path / "exports"
    for recovery in range(1, 5):
        with store.transaction():
            store.conn.execute("UPDATE operations SET state='running' WHERE id IN (?,?) AND state!='failed'",
                               (local_op, remote_op))
        with Ownership(store, output, exports):
            pass
        state, result = op_state(store, local_op)
        assert (state, result) == ("failed", {"code": "interrupted"})
        state, result = op_state(store, remote_op)
        if recovery <= 3:
            assert state == "queued" and result["_recoveries"] == recovery
            assert "_recoveries" not in store.operation(client, remote_op)["result"]
        else:
            assert (state, result) == ("failed", {"code": "runtime_unavailable"})
    # Temporary owners (companion init/recover/purge, terminal commands) never accept.
    assert store.conn.execute("SELECT accepting FROM runtime_state").fetchone()[0] == 0
    idx.close()


# ---------------------------------------------------------------------------
# 3.5 waiter lifecycle
# ---------------------------------------------------------------------------

async def test_abandoned_withdrawn_and_raced_local_operations_never_run(tmp_path):
    runtime, idx, _ = make_runtime(tmp_path)
    await runtime.start(background=False, accept_commands=True)
    store = runtime.store
    try:
        before = load_config().download.max_concurrent
        stale = accept_local(store, "config", {"key": "download.max_concurrent", "value": "9"})
        with store.transaction():
            store.conn.execute("UPDATE operations SET updated_at=? WHERE id=?", (timestamp(-3600), stale))
        withdrawn = accept_local(store, "config", {"key": "download.max_concurrent", "value": "8"})
        assert LocalWaiter(store, withdrawn, store.limits).withdraw()
        raced = accept_local(store, "config", {"key": "download.max_concurrent", "value": "7"})
        row = dict(store.conn.execute("SELECT * FROM operations WHERE id=?", (raced,)).fetchone())
        with store.transaction():  # Another executor started it first.
            store.conn.execute("UPDATE operations SET state='running' WHERE id=?", (raced,))
        await runtime.execute(row)
        await runtime.tick(scan=False)
        assert op_state(store, stale) == ("failed", {"code": "abandoned"})
        assert op_state(store, withdrawn) == ("failed", {"code": "withdrawn"})
        assert op_state(store, raced)[0] == "running"
        assert load_config().download.max_concurrent == before
        with pytest.raises(StillRunning):
            LocalWaiter(store, raced, store.limits)._stopped_waiting()
    finally:
        await runtime.stop()
        idx.close()


# ---------------------------------------------------------------------------
# 4.1 commands beside discovery, 4.2 containment, 4.3 loop robustness
# ---------------------------------------------------------------------------

async def test_commands_complete_while_acquisition_is_stalled(tmp_path):
    release = asyncio.Event()
    entered = asyncio.Event()
    observed = []

    async def stalled(issues, _cfg, index, **_):
        observed.append(index.conn.in_transaction)  # No transaction spans an await.
        entered.set()
        await release.wait()
        return []

    runtime, idx, _ = make_runtime(tmp_path, subscriptions=[Subscription(query="Science News")],
                                   items=[ScrapedIssue(title="Science News - June 2025",
                                                       page_url="https://freemagazines.top/sn/",
                                                       limewire_url=LW.format("sn"))],
                                   batch=stalled, limits=Limits(minimum_free_bytes=1024, maximum_download_bytes=1024 * 1024,
                                                                command_poll_seconds=0.02))
    await runtime.start(background=True)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        operation = accept_local(runtime.store, "config", {"key": "download.scrape_delay", "value": "2.5"})
        for _ in range(250):
            if op_state(runtime.store, operation)[0] == "succeeded":
                break
            await asyncio.sleep(0.02)
        assert op_state(runtime.store, operation)[0] == "succeeded"
        assert load_config().download.scrape_delay == 2.5
        assert observed == [False]
    finally:
        release.set()
        await runtime.stop()
        idx.close()


async def test_one_failing_operation_never_stops_the_runtime(tmp_path, monkeypatch):
    import magsync.companion.commands as commands

    real = commands.execute_local

    async def flaky(runtime, kind, body, operation_id):
        if kind == "update":
            raise TypeError("unexpected")
        return await real(runtime, kind, body, operation_id)

    monkeypatch.setattr(commands, "execute_local", flaky)
    runtime, idx, _ = make_runtime(tmp_path)
    await runtime.start(background=False, accept_commands=True)
    store = runtime.store
    try:
        broken = accept_local(store, "update", {})
        fine = accept_local(store, "subscribe", {"query": "Dish", "since": None, "exact": True})
        empty = accept_local(store, "config", {"key": "download.max_concurrent", "value": None})
        await runtime.tick(scan=False)
        assert op_state(store, broken) == ("failed", {"code": "internal_error"})
        assert op_state(store, fine)[0] == "succeeded"
        assert op_state(store, empty) == ("failed", {"code": "invalid_request"})
        assert [s.query for s in load_config().subscriptions] == ["Dish"]
        runtime.assert_ready()

        client, _scope = remote_scope(store)

        async def exploding(*_args, **_kwargs):
            raise TypeError("unexpected")

        monkeypatch.setattr(runtime, "search", exploding)
        remote = store.accept(client, "search", None, {"query": "x", "pages": 1}, "remote")["operation_id"]
        await runtime.tick(scan=False)
        state, result = op_state(store, remote)
        assert state == "failed" and result["code"] in ERRORS
    finally:
        await runtime.stop()
        idx.close()


async def test_transient_heartbeat_errors_do_not_stop_the_runtime(tmp_path, monkeypatch):
    runtime, idx, _ = make_runtime(tmp_path, limits=Limits(minimum_free_bytes=1024, heartbeat_seconds=0.01,
                                                           heartbeat_stale_seconds=1))
    await runtime.start(background=False, accept_commands=True)
    real = runtime.owner.heartbeat
    failures = {"left": 3}

    def locked(**kwargs):
        if failures["left"]:
            failures["left"] -= 1
            raise sqlite3.OperationalError("database is locked")
        return real(**kwargs)

    monkeypatch.setattr(runtime.owner, "heartbeat", locked)
    try:
        await asyncio.sleep(0.2)
        assert failures["left"] == 0
        assert runtime.ready and not runtime.fatal and runtime.live()
        runtime.assert_ready()
    finally:
        await runtime.stop()
        idx.close()


async def test_repeated_loop_failures_stop_advertising_acceptance(tmp_path, monkeypatch):
    runtime, idx, _ = make_runtime(tmp_path, limits=Limits(minimum_free_bytes=1024, command_poll_seconds=0.001,
                                                           heartbeat_seconds=0.01, heartbeat_stale_seconds=1))

    async def broken(*_args, **_kwargs):
        raise RuntimeError("disk trouble")

    monkeypatch.setattr(runtime, "acquire", broken)
    runtime._next_scan = float("inf")
    await runtime.start(background=True)
    try:
        for _ in range(300):
            if runtime._consecutive_failures >= 3:
                break
            await asyncio.sleep(0.01)
        assert runtime._consecutive_failures >= 3
        assert not runtime.loop_task.done()  # Still running, backing off.
        await asyncio.sleep(0.05)
        accepting = runtime.store.conn.execute("SELECT accepting FROM runtime_state").fetchone()[0]
        assert accepting == 0
    finally:
        await runtime.stop()
        idx.close()


# ---------------------------------------------------------------------------
# 4.4 service lifespan and startup recovery
# ---------------------------------------------------------------------------

async def test_service_exits_when_its_runtime_dies(tmp_path, monkeypatch):
    from magsync.companion.api import create_app

    runtime, idx, _ = make_runtime(tmp_path)

    async def dead_loop():
        return None

    monkeypatch.setattr(runtime, "_work_loop", dead_loop)
    exits = []
    app = create_app(runtime=runtime, background=True, on_runtime_exit=lambda: exits.append(True))
    async with app.router.lifespan_context(app):
        for _ in range(200):
            if exits:
                break
            await asyncio.sleep(0.01)
    assert exits == [True]
    idx.close()


async def test_startup_recovery_hashes_exports_off_the_event_loop(tmp_path, monkeypatch):
    from magsync.companion.exports import Exports

    runtime, idx, _ = make_runtime(tmp_path)
    store = runtime.store
    store.initialize()
    content = tmp_path / "exports" / "c.pdf"
    content.parent.mkdir(parents=True)
    content.write_bytes(PDF)
    with store.transaction():
        store.conn.execute("INSERT INTO content_objects VALUES('c',?,?, 'ready','c.pdf',?,NULL)",
                           (hashlib.sha256(PDF).hexdigest(), len(PDF), timestamp()))
    import time as _time
    real_verify = Exports.verify

    def slow_verify(path, *args, **kwargs):
        _time.sleep(0.3)
        return real_verify(path, *args, **kwargs)

    monkeypatch.setattr(Exports, "verify", staticmethod(slow_verify))
    ticks = []

    async def ticker():
        while True:
            ticks.append(1)
            await asyncio.sleep(0.02)

    task = asyncio.create_task(ticker())
    try:
        await runtime.start(background=False)
    finally:
        task.cancel()
    assert len(ticks) >= 5
    await runtime.stop()
    idx.close()


# ---------------------------------------------------------------------------
# 5.1 / 5.2 incremental reconciliation and remote isolation
# ---------------------------------------------------------------------------

def _active_pairs(store):
    return {(row[0], row[1]) for row in store.conn.execute(
        "SELECT subscription_id, issue_id FROM acquisition_requests WHERE subscription_id IS NOT NULL AND state!='canceled'")}


def _expected_pairs(store):
    expected = set()
    subs = store.conn.execute("""SELECT sub.* FROM subscriptions sub JOIN scopes s ON s.id=sub.scope_id
        JOIN clients c ON c.id=s.client_id WHERE sub.enabled=1 AND sub.tombstone=0 AND s.enabled=1 AND c.enabled=1""").fetchall()
    issues = store.conn.execute("SELECT id,title,year,month FROM issues").fetchall()
    for sub in subs:
        matcher = SimpleNamespace(query=sub["query"], exact=sub["exact"], since=sub["since"])
        for issue in issues:
            if matches_subscription(dict(issue), matcher):
                expected.add((sub["id"], issue["id"]))
    return expected


def test_incremental_materialization_equals_full_evaluation(tmp_path):
    rng = random.Random(7)
    idx = MagazineIndex(tmp_path / "index.db")
    store = Store(idx)
    titles = ["Science News", "Science News Explores", "Dish", "The Economist", "Economist Audio", "Getaway"]
    counter = {"n": 0}

    def insert(count):
        for _ in range(count):
            counter["n"] += 1
            title = rng.choice(titles)
            prefix = "[PDF] " if rng.random() < 0.2 else ""
            add_issue(idx, f"{prefix}{title} - {rng.randint(1, 12):02d} {rng.choice([2023, 2024, 2025])}",
                      slug=f"i{counter['n']}", year=rng.choice([None, 2023, 2024, 2025]), month=rng.choice([None, 3, 9]))

    subscriptions = [Subscription(query="Science News", since="2024-06"), Subscription(query="Dish", exact=True)]
    insert(40)
    store.migrate_local(subscriptions)
    assert _active_pairs(store) == _expected_pairs(store)
    for step in range(12):
        action = step % 4
        if action == 0:
            insert(15)
        elif action == 1:
            subscriptions[0] = Subscription(query="Science News", since=rng.choice([None, "2024-01", "2025"]))
        elif action == 2:  # unsubscribe, then subscribe again with identical settings
            store.reconcile_local(subscriptions[1:])
            store.reconcile_local(subscriptions)
        else:
            from magsync.core.repair import repair_titles
            repair_titles(idx, tmp_path / "out")
        store.migrate_local(subscriptions)
        assert _active_pairs(store) == _expected_pairs(store), f"step {step}"
    idx.close()


def test_unchanged_reconciliation_evaluates_nothing(tmp_path, monkeypatch):
    idx = MagazineIndex(tmp_path / "index.db")
    store = Store(idx)
    for n in range(30):
        add_issue(idx, f"Science News - {n % 12 + 1:02d} 2025", slug=f"sn{n}")
    subscriptions = [Subscription(query="Science News")]
    store.migrate_local(subscriptions)
    calls = []
    real = Store._materialize_one
    monkeypatch.setattr(Store, "_materialize_one", lambda self, sub, since_id, max_id, titles: calls.append((since_id, max_id)) or real(self, sub, since_id, max_id, titles))
    store.migrate_local(subscriptions)
    assert calls == []
    add_issue(idx, "Science News - 12 2026", slug="new", year=2026)
    store.migrate_local(subscriptions)
    assert len(calls) == 1 and calls[0][1] - calls[0][0] == 1
    idx.close()


async def test_remote_client_at_its_cap_never_blocks_local_work(tmp_path, caplog):
    runtime, idx, _ = make_runtime(tmp_path, subscriptions=[Subscription(query="Science News")],
                                   limits=Limits(minimum_free_bytes=1024, pending_requests=1, queue_depth=1,
                                                 commands_per_minute=1))
    store = runtime.store
    store.initialize()
    for n in range(3):
        add_issue(idx, f"Science News - 0{n + 1} 2025", slug=f"sn{n}")
    client, scope = remote_scope(store)
    with store.transaction():  # A remote subscription created before the limit applied.
        store.conn.execute("INSERT INTO subscriptions(id,scope_id,query) VALUES('remote-sub',?, 'Science News')", (scope,))
    caplog.set_level(logging.WARNING, logger="magsync")
    await runtime.start(background=False)
    try:
        local = store.conn.execute("SELECT count(*) FROM acquisition_requests WHERE scope_id='local'").fetchone()[0]
        assert local == 3
        await runtime.discover()
        await runtime.discover()
        assert "remote-sub" in caplog.text and caplog.text.count("remote-sub") == 1
        assert not store.conn.execute("SELECT 1 FROM companion_materialized WHERE subscription_id='remote-sub'").fetchone()
        # Remote admission is still enforced, but never against local commands.
        store.accept(client, "search", None, {"query": "a", "pages": 1}, "k1")
        accept_local(store, "update", {})
        accept_local(store, "update", {})
    finally:
        await runtime.stop()
        idx.close()


# ---------------------------------------------------------------------------
# 6.x claims, 7.x exports
# ---------------------------------------------------------------------------

async def test_parked_requests_cost_nothing_and_capacity_is_per_issue(tmp_path):
    done = []
    runtime, idx, _ = make_runtime(tmp_path, subscriptions=[Subscription(query="Science News")],
                                   batch=completing_batch(tmp_path, done))
    store = runtime.store
    store.initialize()
    with idx.conn:
        magazine = idx.conn.execute("INSERT INTO magazines(title,normalized_title) VALUES('Science News','science news')").lastrowid
        idx.conn.executemany("INSERT INTO issues(magazine_id,title,page_url,limewire_url,year,month) VALUES(?,?,?,?,2025,1)",
                             [(magazine, f"Science News - {n}", f"https://freemagazines.top/p{n}/", LW.format(f"p{n}"))
                              for n in range(5000)])
        idx.conn.execute("INSERT INTO downloads(issue_id,status) SELECT id,'unavailable' FROM issues")
    local_only = add_issue(idx, "Science News - Local 2025", slug="local")
    remote_issue = add_issue(idx, "Other Title - June 2025", slug="remote")
    await runtime.start(background=False)
    try:
        client, scope = remote_scope(store)
        store.create_request(client, scope, {"issue_id": store.provider_issue(remote_issue)})
        candidates = claimable_candidates(store.conn, timestamp())
        assert sorted(row["issue_id"] for row in candidates) == sorted([local_only, remote_issue])
        store.limits.export_bytes = 1
        await runtime.acquire()
        assert done == [local_only]  # The remote issue waits for export capacity alone.
        assert runtime.capacity_blocked is False or runtime._last_claim_blocked
    finally:
        await runtime.stop()
        idx.close()


async def test_publication_failure_leaves_no_stuck_attempts(tmp_path, monkeypatch):
    import magsync.companion.exports as exports_module

    done = []
    runtime, idx, _ = make_runtime(tmp_path, batch=completing_batch(tmp_path, done))
    store = runtime.store
    store.initialize()
    issue_id = add_issue(idx, "Remote Title - June 2025", slug="rt")
    await runtime.start(background=False)
    try:
        client, scope = remote_scope(store)
        store.create_request(client, scope, {"issue_id": store.provider_issue(issue_id)})
        real_replace = exports_module.os.replace

        def failing_replace(source, target):
            if str(target).endswith(".pdf"):
                raise OSError(28, "No space left on device")
            return real_replace(source, target)

        monkeypatch.setattr(exports_module.os, "replace", failing_replace)
        await runtime.acquire()
        assert done == [issue_id]
        assert not store.conn.execute("SELECT 1 FROM acquisition_attempts WHERE state='running'").fetchone()
        assert not store.conn.execute("SELECT 1 FROM acquisition_requests WHERE state='acquiring'").fetchone()
        assert store.conn.execute("SELECT fulfillment_error FROM acquisition_requests").fetchone()[0] == "content_unavailable"
        monkeypatch.setattr(exports_module.os, "replace", real_replace)
        second = add_issue(idx, "Remote Title - July 2025", slug="rt2", month=7)
        store.create_request(client, scope, {"issue_id": store.provider_issue(second)})
        await runtime.acquire()
        assert done == [issue_id, second]
    finally:
        await runtime.stop()
        idx.close()


async def test_stranger_refreshes_cannot_starve_a_wanted_refresh(tmp_path, monkeypatch):
    import magsync.core.batch as batch_module

    runtime, idx, _ = make_runtime(tmp_path)
    past = timestamp(-60)
    strangers = [add_issue(idx, f"Stranger - {n}", slug=f"s{n}") for n in range(150)]
    wanted = add_issue(idx, "Wanted - June 2025", slug="wanted", manual=True)
    for issue_id in strangers + [wanted]:
        idx.conn.execute("UPDATE downloads SET status='unavailable',next_action='REFRESH_LINK',next_retry_at=? WHERE issue_id=?",
                         (past if issue_id != wanted else timestamp(-1), issue_id))
    idx.conn.commit()
    refreshed = []

    async def fake_refresh(issues, _index, _source):
        refreshed.extend(issue["id"] for issue in issues)
        return []

    monkeypatch.setattr(batch_module, "refresh_due_links", fake_refresh)
    await runtime.start(background=False)
    try:
        await runtime.refresh_due()
        assert refreshed == [wanted]
    finally:
        await runtime.stop()
        idx.close()


async def test_local_only_demand_is_never_exported(tmp_path):
    runtime, idx, _ = make_runtime(tmp_path, subscriptions=[Subscription(query="Science News")])
    store = runtime.store
    ids = []
    for n in range(3):
        issue_id = add_issue(idx, f"Science News - 0{n + 1} 2025", slug=f"sn{n}")
        data = _pdf(issue_id)
        path = tmp_path / f"{issue_id}.pdf"
        path.write_bytes(data)
        idx.update_download_status(issue_id, DownloadStatus.COMPLETE, str(path), len(data), hashlib.sha256(data).hexdigest())
        ids.append(issue_id)
    await runtime.start(background=False)
    try:
        # A copy left by the 0.9.0 development build stays unpinned (aged out).
        with store.transaction():
            store.conn.execute("INSERT INTO content_objects VALUES('dev','x',1,'unavailable','dev.pdf',?,?)",
                               (timestamp(), "2026-01-01T00:00:00+00:00"))
            store.conn.execute("INSERT INTO export_intents VALUES('i',?,'dev','o',1,'published',?)", (ids[0], timestamp()))
        await runtime.tick(scan=False)
        states = {row[0] for row in store.conn.execute("SELECT state FROM acquisition_requests WHERE scope_id='local'")}
        assert states == {"fulfilled"}
        assert store.conn.execute("SELECT unpinned_at FROM content_objects WHERE id='dev'").fetchone()[0] == "2026-01-01T00:00:00+00:00"
        with store.transaction():
            store.conn.execute("DELETE FROM export_intents")
            store.conn.execute("DELETE FROM content_objects")
        assert not [p for p in (tmp_path / "exports").iterdir() if p.suffix == ".pdf"]
        # Mixed demand exports once, for the remote library only.
        client, scope = remote_scope(store)
        store.create_request(client, scope, {"issue_id": store.provider_issue(ids[0])})
        await runtime.tick(scan=False)
        assert store.conn.execute("SELECT count(*) FROM content_objects").fetchone()[0] == 1
        assert store.conn.execute("SELECT count(*) FROM deliveries").fetchone()[0] == 1
    finally:
        await runtime.stop()
        idx.close()


async def test_missing_original_is_reacquired_but_corrupt_is_not(tmp_path):
    done = []
    runtime, idx, _ = make_runtime(tmp_path, batch=completing_batch(tmp_path, done))
    store = runtime.store
    store.initialize()
    missing = add_issue(idx, "Remote Title - June 2025", slug="gone")
    corrupt = add_issue(idx, "Remote Title - July 2025", slug="bad", month=7)
    idx.update_download_status(missing, DownloadStatus.COMPLETE, str(tmp_path / "deleted.pdf"), 10, "0" * 64)
    bad = tmp_path / "bad.pdf"
    bad.write_bytes(b"not a pdf at all")
    idx.update_download_status(corrupt, DownloadStatus.COMPLETE, str(bad), 16, hashlib.sha256(b"not a pdf at all").hexdigest())
    await runtime.start(background=False)
    try:
        client, scope = remote_scope(store)
        for issue_id in (missing, corrupt):
            store.create_request(client, scope, {"issue_id": store.provider_issue(issue_id)})
        await runtime.tick(scan=False)
        await runtime.tick(scan=False)
        assert done == [missing]
        assert store.conn.execute("SELECT count(*) FROM deliveries").fetchone()[0] == 1
        status = store.conn.execute("SELECT status FROM downloads WHERE issue_id=?", (corrupt,)).fetchone()[0]
        assert status == "complete"
        errors = {row[0] for row in store.conn.execute("SELECT fulfillment_error FROM acquisition_requests")}
        assert errors == {None, "integrity_failed"}
    finally:
        await runtime.stop()
        idx.close()


async def test_unpublishable_content_is_retried_once_per_discovery(tmp_path, monkeypatch):
    from magsync.companion.exports import Exports

    runtime, idx, _ = make_runtime(tmp_path)
    store = runtime.store
    store.initialize()
    issue_id = add_issue(idx, "Remote Title - June 2025", slug="bad")
    bad = tmp_path / "bad.pdf"
    bad.write_bytes(b"%PDF-1.7\n" + b"0" * 3000)  # No %%EOF near the end.
    idx.update_download_status(issue_id, DownloadStatus.COMPLETE, str(bad), bad.stat().st_size,
                               hashlib.sha256(bad.read_bytes()).hexdigest())
    calls = []
    real_verify = Exports.verify
    monkeypatch.setattr(Exports, "verify", staticmethod(lambda path, *a, **k: calls.append(path) or real_verify(path, *a, **k)))
    await runtime.start(background=False)
    try:
        client, scope = remote_scope(store)
        store.create_request(client, scope, {"issue_id": store.provider_issue(issue_id)})
        for _ in range(5):
            await runtime.tick(scan=False)
        assert len(calls) == 1
        await runtime.discover()
        assert len(calls) == 2
    finally:
        await runtime.stop()
        idx.close()


# ---------------------------------------------------------------------------
# 8.4 notifications, 8.5 logging, 8.6 service interval
# ---------------------------------------------------------------------------

async def test_long_running_runtimes_notify_once_per_flush(tmp_path, monkeypatch):
    import magsync.core.notify as notify

    sent = []
    monkeypatch.setattr(notify, "send_download_summary", lambda issues, _settings: sent.append([i["id"] for i in issues]))
    done = []
    runtime, idx, _ = make_runtime(tmp_path, batch=completing_batch(tmp_path, done), notify=True)
    first = add_issue(idx, "Title A - June 2025", slug="a", manual=True)
    await runtime.start(background=False)
    try:
        await runtime.acquire()
        second = add_issue(idx, "Title B - June 2025", slug="b")
        accept_local(runtime.store, "download", {"issue_ids": [second]})
        await runtime.tick(scan=False)
    finally:
        await runtime.stop()
        idx.close()
    assert done == [first, second]
    assert sent == [[first, second]]


async def test_temporary_runtimes_do_not_notify_and_slow_notifiers_do_not_block(tmp_path, monkeypatch):
    import time as _time

    import magsync.core.notify as notify

    sent = []

    def slow(issues, _settings):
        _time.sleep(0.4)
        sent.append(len(issues))

    monkeypatch.setattr(notify, "send_download_summary", slow)
    done = []
    temporary, idx, _ = make_runtime(tmp_path, batch=completing_batch(tmp_path, done))
    add_issue(idx, "Title A - June 2025", slug="a", manual=True)
    await temporary.start(background=False)
    await temporary.acquire()
    await temporary.stop()
    assert sent == [] and len(done) == 1
    idx.close()

    runtime, idx, _ = make_runtime(tmp_path / "second", batch=completing_batch(tmp_path, done), notify=True,
                                   limits=Limits(minimum_free_bytes=1024, heartbeat_seconds=0.01, heartbeat_stale_seconds=0.2))
    add_issue(idx, "Title B - June 2025", slug="b", manual=True)
    await runtime.start(background=False)
    try:
        await runtime.acquire()
        runtime._flush_notifications()
        await asyncio.sleep(0.3)
        assert runtime.live()  # The heartbeat kept running while the notifier slept.
    finally:
        await runtime.stop()
        idx.close()
    assert sent == [1]


async def test_per_issue_download_lines_and_startup_banner(tmp_path, caplog):
    from magsync.companion.cli import log_startup_banner

    done = []
    runtime, idx, _ = make_runtime(tmp_path, batch=completing_batch(tmp_path, done))
    add_issue(idx, "Science News - June 2025", slug="sn", manual=True)
    caplog.set_level(logging.INFO, logger="magsync")
    await runtime.start(background=False)
    try:
        await runtime.acquire()
    finally:
        await runtime.stop()
        idx.close()
    assert "Downloading: Science News - June 2025" in caplog.text
    assert "Done: Science News - June 2025" in caplog.text
    cfg = Config(output_dir="/magazines", subscriptions=[Subscription(query="Dish", since="2025-01", exact=True)])
    log_startup_banner(logging.getLogger("magsync"), cfg, mode="daemon", interval_label="6h (21600s)")
    for expected in ("daemon starting", "Output directory: /magazines", "Subscriptions: 1",
                     "Interval: 6h (21600s)", "Notifications: disabled", "- Dish (since 2025-01) [exact]"):
        assert expected in caplog.text


async def test_service_honors_the_discovery_interval(tmp_path):
    from magsync.companion.api import create_app
    from typer.testing import CliRunner

    from magsync.cli import app as cli_app

    idx = MagazineIndex(tmp_path / "index.db")
    Store(idx).initialize()
    idx.close()
    app = create_app(db_path=tmp_path / "index.db", config=Config(output_dir=str(tmp_path / "out")),
                     limits=Limits(minimum_free_bytes=1024), exports=tmp_path / "exports",
                     background=False, scan_seconds=1800, notify=True)
    async with app.router.lifespan_context(app):
        runtime = app.state.runtime
        assert runtime.scan_seconds == 1800 and runtime.notify and runtime._accepting
    assert "--interval" in CliRunner().invoke(cli_app, ["serve", "--help"]).output


# ---------------------------------------------------------------------------
# 9.x runtime-executed terminal semantics
# ---------------------------------------------------------------------------

async def test_runtime_subscribe_unsubscribe_and_repair_match_standalone(tmp_path, monkeypatch):
    runtime, idx, _ = make_runtime(tmp_path)
    store = runtime.store
    output = tmp_path / "out"
    tagged_dir = output / "[PDF] Airliner World" / "2024" / "01"
    tagged_dir.mkdir(parents=True)
    old_file = tagged_dir / "[PDF] Airliner World - January 2024.pdf"
    old_file.write_bytes(PDF)
    magazine = idx.get_or_create_magazine("[PDF] Airliner World", "[pdf] airliner world")
    idx.add_issues(magazine, [{"title": "[PDF] Airliner World - January 2024", "page_url": "https://freemagazines.top/aw/",
                               "limewire_url": LW.format("aw"), "year": 2024, "month": 1}])
    issue_id = idx.conn.execute("SELECT id FROM issues").fetchone()[0]
    idx.update_download_status(issue_id, DownloadStatus.COMPLETE, str(old_file), len(PDF), hashlib.sha256(PDF).hexdigest())
    await runtime.start(background=False)
    try:
        first = accept_local(store, "subscribe", {"query": "Dish", "since": None, "exact": False})
        duplicate = accept_local(store, "subscribe", {"query": "dish", "since": "2020-01", "exact": True})
        missing = accept_local(store, "unsubscribe", {"query": "Never Subscribed"})
        repair = accept_local(store, "repair", {"dry_run": False})
        await runtime.tick(scan=False)
        assert op_state(store, first)[1]["outcome"] == "succeeded"
        assert op_state(store, duplicate)[1]["outcome"] == "unchanged"
        assert op_state(store, missing)[1]["outcome"] == "unchanged"
        subs = load_config().subscriptions
        assert [(s.query, s.since, s.exact) for s in subs] == [("Dish", None, False)]
        state, result = op_state(store, repair)
        assert state == "succeeded" and result["moved"] == 1 and result["removed_dirs"] == 1
        assert not tagged_dir.exists()
        monkeypatch.setenv("MAGSYNC_SUBSCRIPTIONS", "Managed")
        managed = accept_local(store, "subscribe", {"query": "Other", "since": None, "exact": False})
        await runtime.tick(scan=False)
        state, result = op_state(store, managed)
        assert state == "failed" and result["code"] == "configuration_managed"
        assert "MAGSYNC_SUBSCRIPTIONS" in result["message"]
    finally:
        await runtime.stop()
        idx.close()


async def test_local_searches_are_not_bounded_by_the_api_page_limit(tmp_path):
    items = [ScrapedIssue(title="Science News - June 2025", page_url="https://freemagazines.top/sn/",
                          limewire_url=LW.format("sn"))]
    runtime, idx, source = make_runtime(tmp_path, items=items)
    await runtime.start(background=False, accept_commands=True)
    try:
        local = accept_local(runtime.store, "search", {"query": "Science News"})
        client, _scope = remote_scope(runtime.store)
        runtime.store.accept(client, "search", None, {"query": "Science News", "pages": 3}, "api")
        await runtime.tick(scan=False)
        assert source.calls[0] == ("Science News", {})
        assert source.calls[1] == ("Science News", {"max_pages": 3})
        assert op_state(runtime.store, local)[1]["added"] == 1
    finally:
        await runtime.stop()
        idx.close()


async def test_starting_daemon_waits_for_a_temporary_owner(tmp_path, caplog):
    import threading
    import time as _time

    runtime, idx, _ = make_runtime(tmp_path)
    started, released = threading.Event(), threading.Event()

    def hold():
        holder = MagazineIndex(tmp_path / "index.db")
        owner = Ownership(Store(holder), tmp_path / "out", tmp_path / "exports")
        owner.acquire(accepting=False)
        started.set()
        _time.sleep(0.5)
        owner.release()
        holder.close()
        released.set()

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert started.wait(5)
    caplog.set_level(logging.INFO, logger="magsync")
    try:
        await runtime.start_when_available(background=False, poll=0.05)
        assert released.is_set()
        assert "Waiting for another magsync command to finish" in caplog.text
    finally:
        await runtime.stop()
        thread.join()
        idx.close()


def test_termination_signal_withdraws_a_queued_command(tmp_path):
    import os
    import signal
    import threading

    if not hasattr(signal, "SIGHUP"):
        pytest.skip("POSIX signals only")
    idx = MagazineIndex(tmp_path / "index.db")
    store = Store(idx, Limits())
    with store.transaction():  # A live, accepting owner that never starts the command.
        store.conn.execute("UPDATE runtime_state SET accepting=1,heartbeat_at=? WHERE id=1", (timestamp(),))
    operation = accept_local(store, "update", {})
    threading.Timer(0.3, lambda: os.kill(os.getpid(), signal.SIGHUP)).start()
    with pytest.raises(KeyboardInterrupt):
        LocalWaiter(store, operation, store.limits).wait()
    assert op_state(store, operation) == ("failed", {"code": "withdrawn"})
    idx.close()
