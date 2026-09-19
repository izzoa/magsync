from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from magsync.companion.local import accept_local
from magsync.companion.protocol import ProtocolError
from magsync.companion.runtime import Runtime, FencedIndex
from magsync.companion.store import Limits, Store, timestamp
from magsync.config import Config
from magsync.core.models import DownloadStatus, DownloadFailureKind
from test_companion_store import store, issue, client_scope
from test_companion_api import FakeSource, api, discover_issue, create_scope
from test_companion_exports import PDF


async def test_acceptance_survives_restart_and_fences_stale_completion(store,tmp_path):
    store.initialize()
    public = issue(store)
    c,s = client_scope(store)
    body={'issue_id':public}
    op=store.accept(c['client_id'],'request.create',s['id'],body,'lost',lambda:store.create_request(c['client_id'],s['id'],body))
    cfg=Config(output_dir=str(tmp_path/'out'))
    first=Runtime(store.index,cfg,limits=store.limits,source_factory=lambda **_:FakeSource())
    await first.start(background=False)
    claims,attempts=first.claim()
    assert len(claims)==1
    stale=FencedIndex(first,attempts)
    await first.stop()
    transfers=[]
    async def batch(issues,cfg,index,**_):
        for row in issues:
            transfers.append(row['id'])
            path=tmp_path/'download.pdf';path.write_bytes(PDF)
            index.update_download_status(row['id'],DownloadStatus.COMPLETE,str(path),len(PDF),hashlib.sha256(PDF).hexdigest())
        return []
    second=Runtime(store.index,cfg,limits=store.limits,source_factory=lambda **_:FakeSource(),batch=batch)
    await second.start(background=False)
    try:
        with pytest.raises(ProtocolError):
            stale.update_download_status(claims[0]['id'],DownloadStatus.COMPLETE,'stale',1,'bad')
        await second.tick(scan=False)
        assert len(transfers)==1
        assert store.operation(c['client_id'],op['operation_id'])['state']=='succeeded'
        assert store.accept(c['client_id'],'request.create',s['id'],body,'lost')['operation_id']==op['operation_id']
    finally:
        await second.stop()


async def test_cancel_before_claim_future_policy_and_retry_snapshot(api):
    client,rt,c,other,transfers,source=api
    scope=await create_scope(client,'library')
    public=await discover_issue(client,rt)
    a=rt.store.create_request(c['client_id'],scope['id'],{'issue_id':public['id']})
    rt.store.cancel(c['client_id'],a['id'],a['revision'])
    assert rt.claim()[0]==[]
    b=rt.store.create_request(c['client_id'],scope['id'],{'issue_id':public['id']})
    internal=rt.store.internal_issue(public['id'])
    rt.store.index.record_download_failure(internal,DownloadFailureKind.TRANSIENT,'temporary',next_retry_at='2099-01-01T00:00:00+00:00')
    assert rt.claim()[0]==[]
    # A remote-only failure cannot enter a local invocation snapshot.
    local=accept_local(rt.store,'retry',{})
    assert not rt.store.conn.execute('SELECT 1 FROM operation_requests WHERE operation_id=?',(local,)).fetchone()
    response=await client.post(f"/v1/requests/{b['id']}/retry",headers={'If-Match':str(b['revision']),'Idempotency-Key':'retry'})
    assert response.status_code==202
    await rt.tick(scan=False)
    assert len(transfers)==1
    # A lost response is replayable after execution changed the request revision.
    replay=await client.post(f"/v1/requests/{b['id']}/retry",headers={'If-Match':str(b['revision']),'Idempotency-Key':'retry'})
    assert replay.json()==response.json()


async def test_local_overlap_cancel_and_subscription_edit_share_transfer(api,tmp_path):
    client,rt,c,other,transfers,source=api
    scope=await create_scope(client,'library')
    public=await discover_issue(client,rt)
    explicit=rt.store.create_request(c['client_id'],scope['id'],{'issue_id':public['id']})
    subscription=rt.store.create_subscription(c['client_id'],scope['id'],{'query':'Science News'})
    internal=rt.store.internal_issue(public['id'])
    entered,release=asyncio.Event(),asyncio.Event()
    async def batch(issues,cfg,index,**_):
        entered.set();await release.wait()
        path=tmp_path/'out.pdf';path.write_bytes(PDF)
        index.update_download_status(internal,DownloadStatus.COMPLETE,str(path),len(PDF),hashlib.sha256(PDF).hexdigest())
        transfers.append(internal)
        return []
    rt.batch=batch
    acquiring=asyncio.create_task(rt.acquire())
    await asyncio.wait_for(entered.wait(), 2)
    # A TUI-style selection while the service transfers creates local intent.
    operation=accept_local(rt.store,'download',{'issue_ids':[internal]})
    request=rt.store.request(c['client_id'],explicit['id'])
    rt.store.cancel(c['client_id'],request['id'],request['revision'])
    rt.store.update_subscription(c['client_id'],subscription['id'],{},subscription['revision'],delete=True)
    release.set();await acquiring
    await rt.execute(dict(rt.store.conn.execute('SELECT * FROM operations WHERE id=?',(operation,)).fetchone()))
    assert transfers==[internal]
    assert not rt.store.conn.execute('SELECT 1 FROM deliveries').fetchone()
    assert rt.store.conn.execute("SELECT state FROM acquisition_requests WHERE scope_id='local'").fetchone()[0]=='fulfilled'
    assert rt.store.operation('local',operation)['result']['physical_attempts']==0


async def test_shutdown_grace_and_interrupted_recovery(store,tmp_path):
    store.initialize();public=issue(store);c,s=client_scope(store)
    store.create_request(c['client_id'],s['id'],{'issue_id':public})
    entered=asyncio.Event()
    async def stalled(*args,**kwargs):
        entered.set()
        await asyncio.Event().wait()
    rt=Runtime(store.index,Config(output_dir=str(tmp_path/'out')),limits=Limits(maximum_download_bytes=1024, minimum_free_bytes=1024, shutdown_seconds=.01),source_factory=lambda **_:FakeSource(),batch=stalled)
    await rt.start(background=False)
    rt.loop_task=asyncio.create_task(rt.acquire())
    await asyncio.wait_for(entered.wait(), 2)
    await asyncio.wait_for(rt.stop(),1)
    assert store.conn.execute("SELECT count(*) FROM acquisition_attempts WHERE state='running'").fetchone()[0]==1
    assert not store.conn.execute('SELECT 1 FROM deliveries').fetchone()
    next_rt=Runtime(store.index,rt.config,limits=store.limits,source_factory=lambda **_:FakeSource())
    await next_rt.start(background=False)
    try:
        assert store.conn.execute("SELECT count(*) FROM acquisition_attempts WHERE state='abandoned'").fetchone()[0]==1
        assert store.conn.execute('SELECT status FROM downloads').fetchone()[0]=='pending'
    finally:
        await next_rt.stop()


async def test_capacity_pauses_without_evicting_and_resumes(api):
    client,rt,c,other,transfers,source=api
    scope=await create_scope(client,'library')
    public=await discover_issue(client,rt)
    rt.store.create_request(c['client_id'],scope['id'],{'issue_id':public['id']})
    rt.store.limits.export_bytes=1
    # Capacity pauses exporting work without raising or evicting anything.
    outcome=await rt.acquire()
    assert outcome['outcome']=='capacity_exhausted' and rt.capacity_blocked
    assert not transfers
    rt.store.limits.export_bytes=100*1024**3
    await rt.acquire()
    assert len(transfers)==1 and not rt.capacity_blocked


async def test_unknown_size_stream_limit(tmp_path):
    import httpx
    from magsync.core.downloader import _stream_vk_payload, download_byte_limit, DownloadPipelineError
    class Bytes(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'1234'
            yield b'567890'
    transport=httpx.MockTransport(lambda request:httpx.Response(200,stream=Bytes()))
    token=download_byte_limit.set(5)
    try:
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(DownloadPipelineError):
                await _stream_vk_payload(client,'https://vk.com/doc1_2',tmp_path/'part')
        assert (tmp_path/'part').stat().st_size<=5
    finally:
        download_byte_limit.reset(token)


async def test_successful_shutdown_grace_finishes_current_transfer(store,tmp_path):
    store.initialize();public=issue(store);c,s=client_scope(store)
    store.create_request(c['client_id'],s['id'],{'issue_id':public})
    entered=asyncio.Event()
    async def finishes(issues,cfg,index,**_):
        entered.set()
        await asyncio.sleep(.02)
        path=tmp_path/'grace.pdf';path.write_bytes(PDF)
        row=issues[0]
        index.update_download_status(row['id'],DownloadStatus.COMPLETE,str(path),len(PDF),hashlib.sha256(PDF).hexdigest())
        return []
    rt=Runtime(store.index,Config(output_dir=str(tmp_path/'out')),limits=Limits(maximum_download_bytes=1024, minimum_free_bytes=1024, shutdown_seconds=1),source_factory=lambda **_:FakeSource(),batch=finishes)
    await rt.start(background=False)
    rt.loop_task=asyncio.create_task(rt.acquire())
    await asyncio.wait_for(entered.wait(), 2)
    await rt.stop()
    assert store.conn.execute('SELECT state FROM deliveries').fetchone()[0]=='ready'


def test_healthcheck_tracks_runtime_heartbeat(tmp_path):
    import os
    from magsync.companion.healthcheck import healthy
    heartbeat=tmp_path/'heartbeat';heartbeat.touch()
    os.utime(heartbeat,(100,100))
    assert healthy(heartbeat,now=129,threshold=30)
    assert not healthy(heartbeat,now=131,threshold=30)


async def test_capacity_blocked_operation_remains_recoverable(api):
    client, rt, c, _, transfers, _ = api
    scope = await create_scope(client, 'capacity')
    public = await discover_issue(client, rt)
    accepted = await client.post(f"/v1/scopes/{scope['id']}/requests", json={'issue_id': public['id']}, headers={'Idempotency-Key': 'capacity'})
    operation_id = accepted.json()['operation_id']
    rt.store.limits.export_bytes = 1
    # The poll continues past a capacity pause; the accepted command stays queued.
    await rt.tick(scan=False)
    assert rt.capacity_blocked
    assert rt.store.operation(c['client_id'], operation_id)['state'] == 'queued'
    assert rt.store.conn.execute("SELECT count(*) FROM acquisition_attempts").fetchone()[0] == 0
    rt.store.limits.export_bytes = 1024**3
    await rt.tick(scan=False)
    assert rt.store.operation(c['client_id'], operation_id)['state'] == 'succeeded'
    assert len(transfers) == 1


async def test_real_cli_process_uses_running_owner_and_preserves_retry_summary(api, tmp_path):
    import os
    import sys
    client, rt, c, _, transfers, _ = api
    scope = await create_scope(client, 'remote-only')
    public = await discover_issue(client, rt)
    remote = rt.store.internal_issue(public['id'])
    rt.store.create_request(c['client_id'], scope['id'], {'issue_id': public['id']})
    rt.store.index.record_download_failure(remote, DownloadFailureKind.TRANSIENT, 'temporary', next_retry_at='2099-01-01T00:00:00+00:00')
    # A distinct current local request is captured before the CLI waits.
    local_public = issue(rt.store, year=2024, provenance='manual')
    local = rt.store.internal_issue(local_public)
    accept_local(rt.store, 'download', {'issue_ids': [local]})
    rt.store.conn.execute("UPDATE operations SET state='succeeded' WHERE kind='local.download'")
    rt.store.conn.commit()
    rt.store.index.record_download_failure(local, DownloadFailureKind.TRANSIENT, 'temporary', next_retry_at='2099-01-01T00:00:00+00:00')
    env = {**os.environ, 'MAGSYNC_DB_PATH': str(rt.store.index.db_path), 'MAGSYNC_OUTPUT_DIR': rt.config.output_dir,
           'MAGSYNC_CONFIG_DIR': str(tmp_path/'config'), 'MAGSYNC_EXPORT_DIR': str(rt.owner.exports)}
    process = await asyncio.create_subprocess_exec(sys.executable, '-m', 'magsync', 'retry', '--no-progress',
                                                   env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    async def drive():
        for _ in range(100):
            await rt.tick(scan=False)
            if process.returncode is not None:
                return
            await asyncio.sleep(.05)
    driver = asyncio.create_task(drive())
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 8)
        assert process.returncode == 0, stderr.decode()
        assert 'Retried 1 shared download' in stdout.decode()
        assert '1 excluded: no current local request' in stdout.decode()
        assert transfers == [local]
        assert rt.store.index.conn.execute('SELECT next_retry_at FROM downloads WHERE issue_id=?', (remote,)).fetchone()[0].startswith('2099')
    finally:
        driver.cancel()
        await asyncio.gather(driver, return_exceptions=True)
        if process.returncode is None:
            process.kill()
            await process.wait()


async def test_mid_batch_capacity_pause_releases_claims_and_resumes(api):
    from magsync.core.batch import DownloadCapacityPaused, download_admission
    client, rt, c, _, transfers, _ = api
    scope = await create_scope(client, 'mid-batch')
    public = await discover_issue(client, rt)
    accepted = await client.post(f"/v1/scopes/{scope['id']}/requests", json={'issue_id': public['id']}, headers={'Idempotency-Key':'mid-batch'})
    batch = rt.batch
    async def paused(*args, **kwargs):
        rt.store.limits.export_bytes = 1
        download_admission.get()()
        pytest.fail('Capacity gate allowed a transfer')
    rt.batch = paused
    operation = dict(rt.store.conn.execute('SELECT * FROM operations WHERE id=?', (accepted.json()['operation_id'],)).fetchone())
    await rt.execute(operation)
    assert rt.capacity_blocked
    assert rt.store.operation(c['client_id'], operation['id'])['state'] == 'queued'
    assert not rt.store.conn.execute("SELECT 1 FROM acquisition_attempts WHERE state='running'").fetchone()
    assert rt.store.request(c['client_id'], accepted.json()['resource']['id'])['physical_status'] == 'pending'
    rt.store.limits.export_bytes = 1024**3
    rt.batch = batch
    await rt.tick(scan=False)
    assert len(transfers) == 1
    assert rt.store.operation(c['client_id'], operation['id'])['state'] == 'succeeded'


async def test_active_backfill_source_failure_keeps_runtime_ready(api):
    from magsync.core.models import SourceError, SourceFailure, SourceFailureKind
    client, rt, _, _, _, source = api
    public = await discover_issue(client, rt)
    internal = rt.store.internal_issue(public['id'])
    accept_local(rt.store, 'download', {'issue_ids':[internal]})
    rt.store.conn.execute('UPDATE issues SET limewire_url=NULL WHERE id=?', (internal,))
    rt.store.conn.commit()
    async def blocked(page):
        source.circuit_open = True
        raise SourceError(SourceFailureKind.ACCESS_BLOCKED, 'cookie=secret', operation='detail')
    source.scrape_detail = blocked
    operation_id = accept_local(rt.store, 'backfill', {})
    await rt.execute(dict(rt.store.conn.execute('SELECT * FROM operations WHERE id=?', (operation_id,)).fetchone()))
    op = rt.store.operation('local', operation_id)
    assert op['state']=='blocked' and op['result']['failure_kind']=='access_blocked'
    assert 'secret' not in json.dumps(op)
    rt.assert_ready()
    assert rt.live()
