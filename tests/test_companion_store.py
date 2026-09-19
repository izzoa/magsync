from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict

import pytest

from magsync.companion.ownership import Ownership
from magsync.companion.protocol import (
    AcquisitionRequest, Client, Delivery, DeliveryState, ERRORS, Operation,
    OperationState, ProtocolError, RequestState, ROUTES, Scope, Subscription,
)
from magsync.companion.store import Limits, Store
from magsync.core.index import MagazineIndex
from magsync.core.models import Subscription as LocalSubscription


@pytest.fixture
def store(tmp_path):
    index = MagazineIndex(tmp_path / 'index.db')
    store = Store(index, Limits(maximum_download_bytes=1024, minimum_free_bytes=1024))
    yield store
    index.close()


def issue(store, *, title='Science News - June 2025', year=2025, month=6, provenance=None):
    index = store.index
    magazine = index.get_or_create_magazine('Science News', 'science news')
    index.add_issues(magazine, [{"title": title, "year": year, "month": month,
                               "page_url": 'https://freemagazines.top/' + str(year),
                               "limewire_url": 'https://limewire.com/d/aaaa#secret'}])
    id = index.conn.execute('SELECT id FROM issues WHERE year=?', (year,)).fetchone()[0]
    index.conn.execute('UPDATE downloads SET requested_by=? WHERE issue_id=?', (provenance, id))
    index.conn.commit()
    return store.provider_issue(id)


def client_scope(store, external='library'):
    credential = store.provision('Consumer')
    scope = store.create_scope(credential['client_id'], {'external_id': external, 'label': external})
    return credential, scope


def test_additive_schema_transaction_and_constraints(store):
    id = issue(store, provenance='manual')
    before = dict(store.conn.execute('SELECT * FROM downloads').fetchone())
    store.migrate_local([])
    assert dict(store.conn.execute('SELECT * FROM downloads').fetchone()) == before
    assert store.conn.execute('PRAGMA foreign_key_check').fetchall() == []
    count = store.conn.execute('SELECT count(*) FROM client_events').fetchone()[0]
    with pytest.raises(RuntimeError), store.transaction():
        store.create_scope('local', {'external_id': 'rollback', 'label': 'rollback'})
        raise RuntimeError('abort')
    assert not store.conn.execute("SELECT 1 FROM scopes WHERE external_id='rollback'").fetchone()
    assert store.conn.execute('SELECT count(*) FROM client_events').fetchone()[0] == count
    with pytest.raises(sqlite3.IntegrityError), store.transaction():
        store.conn.execute("INSERT INTO deliveries VALUES('bad','missing','missing','ready','now')")
    assert store.issue(id)['id'] == id


def test_migration_preserves_policy_and_only_known_intent(store):
    issue(store, year=2024, provenance='manual')
    issue(store, year=2025, provenance='subscription')
    issue(store, year=2026, provenance=None)
    issue(store, year=2027, title='Unrelated', provenance='corrupt')
    store.conn.execute("""UPDATE downloads SET status='failed',file_path='/original.pdf',sha256='hash',
        last_error_kind='transient',next_action='DOWNLOAD',next_retry_at='2099-01-01T00:00:00+00:00'""")
    store.conn.commit()
    before = [tuple(row) for row in store.conn.execute('SELECT * FROM downloads')]
    store.migrate_local([LocalSubscription(query='Science News', since='2025-01')])
    rows = store.conn.execute('SELECT * FROM acquisition_requests').fetchall()
    assert len(rows) == 3  # valid manual + two issues explicitly matched by current subscription
    assert [tuple(row) for row in store.conn.execute('SELECT * FROM downloads')] == before
    assert all(row['scope_id'] == 'local' for row in rows)
    store.migrate_local([LocalSubscription(query='Science News', since='2025-01')])
    assert store.conn.execute('SELECT count(*) FROM acquisition_requests').fetchone()[0] == 3
    assert store.conn.execute('SELECT count(*) FROM deliveries').fetchone()[0] == 0


def test_identity_and_missing_store(store, tmp_path):
    with pytest.raises(ProtocolError, match='Initialize'):
        store.identity()
    first = store.initialize()
    assert store.initialize() == first
    assert store.check_identity()['instance_id'] == first['instance_id']
    index2 = MagazineIndex(store.index.db_path)
    assert Store(index2).check_identity()['instance_id'] == first['instance_id']
    index2.close()
    missing = tmp_path / 'missing.db'
    (tmp_path / 'missing.db.identity.json').write_text(json.dumps(first))
    with pytest.raises(ProtocolError):
        MagazineIndex(missing)
    assert not missing.exists()
    store.conn.execute('UPDATE companion_schema SET version=999')
    store.conn.commit()
    with pytest.raises(ProtocolError, match='incompatible'):
        MagazineIndex(store.index.db_path)


def test_ownership_locks_database_and_output(store, tmp_path):
    output, exports = tmp_path / 'out', tmp_path / 'exports'
    with Ownership(store, output, exports) as first:
        with pytest.raises(ProtocolError):
            Ownership(store, output, exports).acquire()
        other_index = MagazineIndex(tmp_path / 'other.db')
        try:
            with pytest.raises(ProtocolError):
                Ownership(Store(other_index), output, tmp_path / 'other-exports').acquire()
        finally:
            other_index.close()
        first.check()
        generation = first.generation
    with Ownership(store, output, exports) as second:
        assert second.generation == generation + 1
        with pytest.raises(ProtocolError):
            first.check()


def test_scoped_subscriptions_and_independent_explicit_request(store):
    public_issue = issue(store)
    c1, s1 = client_scope(store)
    c2, s2 = client_scope(store)
    sub1 = store.create_subscription(c1['client_id'], s1['id'], {'query': 'Science News', 'since': '2024-01'})
    store.create_subscription(c2['client_id'], s2['id'], {'query': 'Science News', 'since': '2026-01'})
    assert len(store.list_resources(c1['client_id'], 'requests', scope_id=s1['id'])['items']) == 1
    assert not store.list_resources(c2['client_id'], 'requests', scope_id=s2['id'])['items']
    explicit = store.create_request(c1['client_id'], s1['id'], {'issue_id': public_issue})
    store.update_subscription(c1['client_id'], sub1['id'], {}, sub1['revision'], delete=True)
    requests = store.list_resources(c1['client_id'], 'requests', scope_id=s1['id'])['items']
    assert {r['state'] for r in requests} == {'queued', 'canceled'}
    assert store.eligible(explicit['id'])
    with pytest.raises(ProtocolError) as exc:
        store.scope(c2['client_id'], s1['id'])
    assert exc.value.code == 'not_found'
    updated = store.update_scope(c1['client_id'], s1['id'], {'label': 'renamed', 'enabled': False}, 1)
    assert updated['id'] == s1['id'] and updated['revision'] == 2
    assert not store.eligible(explicit['id'])
    with pytest.raises(ProtocolError) as exc:
        store.update_scope(c1['client_id'], s1['id'], {'label': 'stale'}, 1)
    assert exc.value.code == 'revision_conflict'


def test_credentials_idempotency_and_atomic_events(store):
    c, s = client_scope(store)
    assert store.authenticate(c['token']) == c['client_id']
    assert c['token'] not in str([tuple(r) for r in store.conn.execute('SELECT * FROM credentials')])
    body = {'external_id': 'second', 'label': 'Second'}
    accept = lambda: store.accept(c['client_id'], 'scope.create', None, body, 'once',
                                 lambda: store.create_scope(c['client_id'], body))
    assert accept() == accept()
    with pytest.raises(ProtocolError) as exc:
        store.accept(c['client_id'], 'scope.create', None, {'label': 'different'}, 'once')
    assert exc.value.code == 'idempotency_conflict'
    store.revoke(c['key_id'])
    with pytest.raises(ProtocolError) as exc:
        store.authenticate(c['token'])
    assert exc.value.code == 'unauthorized'
    seqs = [r[0] for r in store.conn.execute('SELECT seq FROM client_events WHERE client_id=? ORDER BY seq', (c['client_id'],))]
    assert seqs == list(range(1, len(seqs) + 1))


def test_protocol_fixture_roundtrip():
    fixture = json.loads((__import__('pathlib').Path(__file__).parent / 'fixtures/companion/protocol-v1.json').read_text())
    assert fixture['routes'] == ROUTES
    assert fixture['request_states'] == [s.value for s in RequestState]
    assert fixture['operation_states'] == [s.value for s in OperationState]
    assert fixture['delivery_states'] == [s.value for s in DeliveryState]
    assert fixture['errors'] == {code: status for code, (status, _) in ERRORS.items()}
    for resource in fixture['resources'].values():
        assert json.loads(json.dumps(resource)) == resource
