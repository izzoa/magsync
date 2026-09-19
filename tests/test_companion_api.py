from __future__ import annotations

import hashlib

import httpx
import pytest

from magsync.companion.api import create_app
from magsync.companion.runtime import Runtime
from magsync.config import Config
from magsync.core.models import DownloadStatus, SourceResult, SourceFailure, SourceFailureKind
from magsync.core.scraper import ScrapedIssue
from test_companion_store import store
from test_companion_exports import PDF


class FakeSource:
    circuit_open = False
    circuit_failure = None
    calls = 0
    async def __aenter__(self):
        return self
    async def __aexit__(self, *_):
        pass
    async def search_with_details(self, query, **kwargs):
        self.calls += 1
        if query == 'blocked':
            self.circuit_open = True
            return SourceResult(failure=SourceFailure(SourceFailureKind.ACCESS_BLOCKED, 'secret upstream cookie', operation='search'))
        return SourceResult(items=[ScrapedIssue(title='Science News - June 2025',
                    page_url='https://freemagazines.top/science-news-june-2025/', limewire_url='https://limewire.com/d/aaaa#secret')])


@pytest.fixture
async def api(store, tmp_path):
    store.initialize()
    first = store.provision('Polyreader')
    second = store.provision('Other')
    source = FakeSource()
    transfers = []
    async def batch(issues, cfg, idx, **kwargs):
        for issue in issues:
            transfers.append(issue['id'])
            path = tmp_path / f"original-{issue['id']}.pdf"
            path.write_bytes(PDF)
            idx.update_download_status(issue['id'], DownloadStatus.COMPLETE, str(path), len(PDF), hashlib.sha256(PDF).hexdigest())
        return [{'success':True,'issue':issue} for issue in issues]
    rt = Runtime(store.index, Config(output_dir=str(tmp_path/'out')), limits=store.limits, source_factory=lambda **_:source, batch=batch)
    app = create_app(runtime=rt, background=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test', headers={'Authorization':'Bearer '+first['token']}) as client:
            yield client, rt, first, second, transfers, source


async def create_scope(client, external):
    response = await client.post('/v1/scopes', json={'external_id':external,'label':external}, headers={'Idempotency-Key':external})
    assert response.status_code == 201, response.text
    return response.json()['resource']


async def discover_issue(client, rt):
    response = await client.post('/v1/searches', json={'query':'Science News','pages':1}, headers={'Idempotency-Key':'search'})
    assert response.status_code == 202, response.text
    await rt.tick(scan=False)
    op = (await client.get('/v1/operations/'+response.json()['operation_id'])).json()
    assert op['state'] == 'succeeded', op
    return op['result']['items'][0]


async def test_public_consumer_two_libraries_one_transfer_independent_receipts(api):
    client, rt, first, second, transfers, source = api
    info = await client.get('/v1/info')
    assert info.status_code == 200 and info.json()['media_types'] == ['application/pdf']
    a, b = await create_scope(client,'A'), await create_scope(client,'B')
    issue = await discover_issue(client, rt)
    accepted = []
    for scope in (a,b):
        response = await client.post(f"/v1/scopes/{scope['id']}/requests", json={'issue_id':issue['id']}, headers={'Idempotency-Key':'one'})
        assert response.status_code == 202, response.text
        accepted.append(response.json())
        # Lost acceptance response: exact replay returns the same request and operation.
        repeat = await client.post(f"/v1/scopes/{scope['id']}/requests", json={'issue_id':issue['id']}, headers={'Idempotency-Key':'one'})
        assert repeat.json() == response.json()
    await rt.tick(scan=False)
    assert len(transfers) == 1
    events = (await client.get('/v1/events')).json()
    deliveries = [event['resource'] for event in events['items'] if event['kind']=='delivery.ready']
    assert len(deliveries) == 2
    assert len({d['content_generation'] for d in deliveries}) == 1
    content = await client.get(deliveries[0]['transfer']['http'])
    assert content.content == PDF
    assert hashlib.sha256(content.content).hexdigest() == deliveries[0]['sha256']
    prefix = await client.get(deliveries[0]['transfer']['http'], headers={'Range':'bytes=0-9'})
    suffix = await client.get(deliveries[0]['transfer']['http'], headers={'Range':'bytes=10-', 'If-Range':prefix.headers['etag']})
    assert prefix.status_code == suffix.status_code == 206
    assert prefix.content + suffix.content == PDF
    assert not rt.store.conn.execute('SELECT 1 FROM transfer_leases').fetchone()
    for d in deliveries:
        receipt = {'receipt_id':'copy-for-'+d['scope_id'], 'sha256':d['sha256'],'size':d['size']}
        ack = await client.put(f"/v1/deliveries/{d['id']}/ack", json=receipt)
        assert ack.status_code == 200, ack.text
        assert (await client.put(f"/v1/deliveries/{d['id']}/ack", json=receipt)).json() == ack.json()
    assert rt.store.conn.execute('SELECT count(*) FROM acknowledgments').fetchone()[0] == 2


async def test_protected_routes_isolation_bounds_and_safe_errors(api):
    client, rt, first, second, transfers, source = api
    scope = await create_scope(client, 'scope')
    issue = await discover_issue(client, rt)
    response = await client.post(f"/v1/scopes/{scope['id']}/requests", json={'issue_id':issue['id']}, headers={'Idempotency-Key':'request'})
    req = response.json()['resource']
    other = {'Authorization':'Bearer '+second['token']}
    assert (await client.get('/v1/requests/'+req['id'], headers=other)).status_code == 404
    assert (await client.get('/v1/operations/'+response.json()['operation_id'], headers=other)).status_code == 404
    assert (await client.get('/v1/scopes/local/subscriptions')).status_code == 404
    assert (await client.get('/v1/scopes?limit=501')).status_code == 422
    assert (await client.get('/v1/info', headers={'Authorization':'Bearer invalid'})).status_code == 401
    assert (await client.post('/v1/searches', json={'query':'x'*257}, headers={'Idempotency-Key':'long'})).status_code == 422
    oversized = await client.post('/v1/searches', content=b'x'*17000)
    assert oversized.status_code == 413
    path = await client.post(f"/v1/scopes/{scope['id']}/requests", json={'issue_id':issue['id'], 'destination':'/secret'}, headers={'Idempotency-Key':'path'})
    assert path.status_code == 422 and '/secret' not in path.text
    stale = await client.patch('/v1/scopes/'+scope['id'], json={'label':'new'}, headers={'If-Match':'999'})
    assert stale.status_code == 409
    await client.patch('/v1/scopes/'+scope['id'], json={'enabled':False}, headers={'If-Match':'1'})
    await rt.tick(scan=False)
    assert not transfers
    rt.store.revoke(first['key_id'])
    assert (await client.get('/v1/info')).status_code == 401


async def test_circuit_failure_does_not_disable_local_readiness(api):
    client, rt, first, second, transfers, source = api
    for query in ('blocked', 'another'):
        response = await client.post('/v1/searches', json={'query':query}, headers={'Idempotency-Key':query})
        await rt.tick(scan=False)
        op = (await client.get('/v1/operations/'+response.json()['operation_id'])).json()
        assert op['state'] == 'blocked'
        assert 'cookie' not in str(op) and 'secret' not in str(op)
    assert source.calls == 1
    assert (await client.get('/health/live')).status_code == 200
    assert (await client.get('/health/ready')).status_code == 200
    assert (await client.get('/v1/info')).json()['health']['pipeline'] == 'degraded'


async def test_schema_contract_and_snapshot_recovery(api):
    client, rt, first, second, transfers, source = api
    from magsync.companion.protocol import ROUTES
    schema = (await client.get('/v1/openapi.json')).json()
    assert set(schema['paths']) == set(ROUTES)
    for path, methods in ROUTES.items():
        assert set(schema['paths'][path]) == {m.lower() for m in methods}
    scope = await create_scope(client, 'scope')
    response = await client.post('/v1/snapshots', headers={'Idempotency-Key':'snapshot'})
    assert response.status_code == 201, response.text
    snap = response.json()['resource']
    assert snap['items']
    assert (await client.get('/v1/snapshots/'+snap['id'], headers={'Authorization':'Bearer '+second['token']})).status_code == 404


async def test_snapshot_expiry_during_paging_restarts_without_losing_delivery(api):
    from magsync.companion.store import timestamp
    client, rt, first, _, _, _ = api
    a, b = await create_scope(client, 'A'), await create_scope(client, 'B')
    issue = await discover_issue(client, rt)
    snapshot = (await client.post('/v1/snapshots', headers={'Idempotency-Key': 'before-delivery'})).json()['resource']
    page = (await client.get('/v1/snapshots/'+snapshot['id'], params={'limit': 1})).json()
    assert page['next']
    accepted = await client.post(f"/v1/scopes/{a['id']}/requests", json={'issue_id': issue['id']}, headers={'Idempotency-Key': 'after-snapshot'})
    await rt.tick(scan=False)
    feed = (await client.get('/v1/events', params={'cursor': snapshot['handoff_cursor']})).json()
    assert any(e['kind']=='delivery.ready' for e in feed['items'])
    assert (await client.get('/v1/events', params={'cursor': snapshot['handoff_cursor']})).json() == feed
    rt.store.conn.execute('UPDATE snapshots SET expires_at=? WHERE id=?', (timestamp(-1), snapshot['id']))
    rt.store.conn.commit()
    expired = await client.get('/v1/snapshots/'+snapshot['id'], params={'cursor': page['next'], 'limit': 1})
    assert expired.status_code == 410 and expired.json()['error']['code'] == 'resync_required'
    fresh = (await client.post('/v1/snapshots', headers={'Idempotency-Key': 'fresh'})).json()['resource']
    assert any(i['kind']=='delivery' and i['resource']['state']=='ready' for i in fresh['items'])


def test_published_openapi_headers_pagination_and_errors():
    import json
    from pathlib import Path
    schema = create_app().openapi()
    published = json.loads((Path(__file__).parents[1]/'docs/companion-openapi-v1.json').read_text())
    assert published == schema
    for path, methods in schema['paths'].items():
        for verb, method in methods.items():
            headers = {p['name'] for p in method.get('parameters', []) if p['in']=='header'}
            if verb=='post' and not path.endswith('/cancel'):
                assert 'Idempotency-Key' in headers
            if verb in ('patch','delete') or path.endswith(('/cancel','/retry')):
                assert 'If-Match' in headers
            if path.startswith('/v1'):
                assert method['security'] == [{'HTTPBearer': []}]
                assert method['responses']['401']['content']['application/json']['schema']['$ref'].endswith('ErrorEnvelope')
    assert 'limit' in {p['name'] for p in schema['paths']['/v1/events']['get']['parameters']}
    assert '206' in schema['paths']['/v1/deliveries/{id}/content']['get']['responses']


async def test_public_metadata_redacts_provider_urls_and_paths(api):
    client, rt, _, _, _, _ = api
    issue = await discover_issue(client, rt)
    rt.store.conn.execute('UPDATE issues SET title=? WHERE id=?',
        ('Science https://limewire.com/d/private-share#secret /private/consumer/report.pdf Cookie: private-cookie', rt.store.internal_issue(issue['id'])))
    rt.store.conn.commit()
    response = await client.get('/v1/issues/'+issue['id'])
    assert response.status_code == 200
    assert all(secret not in response.text for secret in ('private-share','secret','/private/consumer','private-cookie'))


def test_base_cli_help_and_missing_service_extra():
    import subprocess
    import sys
    code = '''
import importlib.abc,sys
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self, fullname, path, target=None):
  if fullname.split('.')[0] in ('fastapi','uvicorn'):
   raise ImportError('optional dependency absent')
sys.meta_path.insert(0,Block())
from typer.testing import CliRunner
from magsync.cli import app
assert CliRunner().invoke(app,['--help']).exit_code==0
result=CliRunner().invoke(app,['serve'])
assert result.exit_code==2
assert 'magsync[service]' in result.output
'''
    subprocess.run([sys.executable, '-c', code], check=True, capture_output=True, text=True)
