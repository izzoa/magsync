"""Bounded reads/admission at catalog scale; print measurements for release notes."""
import asyncio
import json
import time
import tracemalloc

import pytest

from magsync.companion.protocol import ProtocolError
from magsync.companion.store import timestamp, uid
from test_companion_api import api
from test_companion_store import store


async def test_large_catalog_bounded_reconciliation_and_queue_lag(api):
    client, rt, first, second, _, _ = api
    store = rt.store
    magazine = store.index.get_or_create_magazine('Scale Fixture', 'scale fixture')
    with store.transaction():
        store.conn.executemany('INSERT INTO issues(magazine_id,title,year,month,page_url) VALUES(?,?,?,?,?)',
            ((magazine, f'Scale Fixture {i}', 2025, 1, f'https://freemagazines.top/scale-{i}') for i in range(30000)))
        ids = [row[0] for row in store.conn.execute('SELECT id FROM issues WHERE magazine_id=?', (magazine,))]
        store.conn.executemany("INSERT INTO downloads(issue_id,status) VALUES(?,'pending')", ((id,) for id in ids))
        store.conn.executemany('INSERT INTO provider_issues VALUES(?,?)', ((uid(), id) for id in ids))
    scopes = []
    for credential in (first, second):
        for number in range(2):
            scope = store.create_scope(credential['client_id'], {'external_id': str(number), 'label': str(number)})
            scopes.append((credential['client_id'], scope['id']))
            with store.transaction():
                for id in ids[:500]:
                    store._new_request(scope['id'], id)
    tracemalloc.start()
    start = time.perf_counter()
    page = store.list_resources(first['client_id'], 'requests', scope_id=scopes[0][1], limit=500)
    status_ms = (time.perf_counter()-start)*1000
    assert len(page['items']) == 500 and page['next'] is None
    with pytest.raises(ProtocolError):
        store.list_resources(first['client_id'], 'requests', scope_id=scopes[0][1], limit=501)
    start = time.perf_counter()
    snapshot = rt.journal.snapshot(first['client_id'])
    snapshot_ms = (time.perf_counter()-start)*1000
    assert len(snapshot['items']) == 100 and snapshot['next']
    start = time.perf_counter()
    feed = rt.journal.events(first['client_id'], limit=100)
    feed_ms = (time.perf_counter()-start)*1000
    assert len(feed['items']) == 100 and feed['has_more']
    assert rt.journal.events(first['client_id'], limit=100) == feed  # dropped reply replay
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    store.limits.command_poll_seconds = .02
    rt._next_scan = time.monotonic()+3600
    start = time.perf_counter()
    response = await client.post('/v1/searches', json={'query': 'Science News', 'pages': 1}, headers={'Idempotency-Key': 'scale-command'})
    operation_id = response.json()['operation_id']
    worker = asyncio.create_task(rt.run())
    try:
        async def finished():
            while store.operation(first['client_id'], operation_id)['state'] in ('queued','running'):
                await asyncio.sleep(.005)
        await asyncio.wait_for(finished(), 5)
        lag_ms = (time.perf_counter()-start)*1000
        assert (await client.get('/health/live')).status_code == 200
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    store.limits.commands_per_minute = 1
    with pytest.raises(ProtocolError) as error:
        store.accept(first['client_id'], 'search', None, {'query': 'more'}, 'rate')
    assert error.value.code == 'rate_limited'
    store.limits.pending_requests = 1000
    with pytest.raises(ProtocolError) as error:
        store.create_request(first['client_id'], scopes[0][1], {'issue_id': store.provider_issue(ids[501])})
    assert error.value.code == 'capacity_exhausted'
    print('SCALE_METRICS '+json.dumps({'catalog_issues': 30000, 'clients': 2, 'scopes': 4, 'requests': 2000,
          'status_page_ms': round(status_ms, 2), 'snapshot_ms': round(snapshot_ms, 2), 'event_page_ms': round(feed_ms, 2),
          'queue_lag_ms': round(lag_ms, 2), 'peak_traced_bytes': peak}))


async def test_reconciliation_and_polling_stay_bounded_at_catalog_scale(tmp_path):
    """50,000 issues and 25 subscriptions: per-command and per-poll work stays small.

    Before incremental reconciliation, a steady-state terminal command spent
    ~10 s at 10,000 issues and a discovery pass blocked the event loop ~55 s at
    20,000; bounds here are generous to stay reliable on slow machines.
    """
    from magsync.companion.runtime import Runtime
    from magsync.companion.store import Limits, Store
    from magsync.config import Config
    from magsync.core.index import MagazineIndex
    from magsync.core.models import Subscription

    idx = MagazineIndex(tmp_path / 'index.db')
    with idx.conn:
        for m in range(1000):
            name = f'Title {m:04d} Monthly'
            magazine = idx.conn.execute('INSERT INTO magazines(title,normalized_title) VALUES(?,?)',
                                        (name, name.lower())).lastrowid
            idx.conn.executemany('INSERT INTO issues(magazine_id,title,year,month,page_url,limewire_url) VALUES(?,?,?,?,?,?)',
                                 ((magazine, f'{name} - {n % 12 + 1:02d} {2015 + n // 12}', 2015 + n // 12, n % 12 + 1,
                                   f'https://freemagazines.top/{m}-{n}/', f'https://limewire.com/d/{m}x{n}#k') for n in range(50)))
        idx.conn.execute("INSERT INTO downloads(issue_id,status) SELECT id, CASE id % 3 WHEN 0 THEN 'complete' "
                         "WHEN 1 THEN 'unavailable' ELSE 'failed' END FROM issues")
    subscriptions = [Subscription(query=f'Title {m:04d} Monthly', since='2016-01') for m in range(25)]
    catalog = Store(idx)
    start = time.perf_counter()
    catalog.migrate_local(subscriptions)
    first_ms = (time.perf_counter() - start) * 1000
    start = time.perf_counter()
    catalog.migrate_local(subscriptions)
    steady_ms = (time.perf_counter() - start) * 1000

    runtime = Runtime(idx, Config(output_dir=str(tmp_path / 'out'), subscriptions=subscriptions),
                      limits=Limits(minimum_free_bytes=1024), exports=tmp_path / 'exports',
                      source_factory=lambda **_: _NoSource(), require_initialized=False,
                      subscription_loader=lambda: subscriptions)
    await runtime.start(background=False)
    try:
        runtime._next_scan = time.monotonic() + 3600
        await runtime.tick(scan=False)  # fulfils retained local demand once
        start = time.perf_counter()
        await runtime.tick(scan=False)
        idle_ms = (time.perf_counter() - start) * 1000
        start = time.perf_counter()
        runtime._reconcile_demand_after_discovery()
        reconcile_ms = (time.perf_counter() - start) * 1000
        start = time.perf_counter()
        report = await runtime.discover()
        discovery_ms = (time.perf_counter() - start) * 1000
        assert report.source_total == 25
    finally:
        await runtime.stop()
        idx.close()
    print('SCALE_METRICS ' + json.dumps({'catalog_issues': 50000, 'subscriptions': 25,
          'first_migration_ms': round(first_ms), 'steady_migration_ms': round(steady_ms),
          'idle_poll_ms': round(idle_ms), 'discovery_reconcile_ms': round(reconcile_ms),
          'discovery_cycle_ms': round(discovery_ms)}))
    assert steady_ms < 2000
    assert idle_ms < 1000
    assert reconcile_ms < 5000
    assert discovery_ms < 5000  # under the 30 s liveness threshold with ample margin
    assert first_ms < 60000


class _NoSource:
    circuit_open = False
    circuit_failure = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    async def search_with_details(self, query, **_kwargs):
        from magsync.core.models import SourceResult
        return SourceResult(validated_empty=True)
