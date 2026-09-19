from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from magsync.companion.exports import Exports
from magsync.companion.journal import Journal
from magsync.companion.ownership import Ownership
from magsync.companion.protocol import ProtocolError
from magsync.companion.store import timestamp
from magsync.core.models import DownloadStatus
from test_companion_store import store, issue, client_scope

PDF = b'%PDF-1.7\ncontrolled test fixture\n%%EOF\n'


@pytest.fixture
def delivery_setup(store, tmp_path):
    store.initialize()
    owner = Ownership(store, tmp_path/'out', tmp_path/'exports').acquire()
    exports = Exports(store, owner)
    journal = Journal(store, exports)
    public = issue(store)
    c, scope = client_scope(store)
    request = store.create_request(c['client_id'], scope['id'], {'issue_id': public})
    original = tmp_path/'original.pdf'
    original.write_bytes(PDF)
    internal = store.internal_issue(public)
    store.index.update_download_status(internal, DownloadStatus.COMPLETE, str(original), len(PDF), hashlib.sha256(PDF).hexdigest())
    yield store, owner, exports, journal, c, scope, request, internal, original
    owner.release()


def test_independent_deliveries_immutable_copy_and_late_request(delivery_setup):
    store, owner, exports, journal, c, scope, req, internal, original = delivery_setup
    c2, s2 = client_scope(store)
    req2 = store.create_request(c2['client_id'], s2['id'], {'issue_id': req['issue_id']})
    deliveries = exports.publish(internal)
    assert len(deliveries) == 2
    assert len({d['content_generation'] for d in deliveries}) == 1
    original.write_bytes(b'changed in place')
    for delivery in deliveries:
        with exports.open_content(delivery['client_id'], delivery['id']) as (stream, data):
            assert stream.read() == PDF
    # Late request can reuse the retained immutable generation even if original moved.
    original.unlink()
    late = store.create_request(c['client_id'], scope['id'], {'issue_id': req['issue_id']})
    assert exports.publish(internal)[0]['request_id'] == late['id']
    with pytest.raises(ProtocolError) as exc:
        exports.delivery(c2['client_id'], next(d['id'] for d in deliveries if d['client_id'] == c['client_id']))
    assert exc.value.code == 'not_found'


@pytest.mark.parametrize('phase', ['intent', 'published', 'committed'])
def test_publication_crashes_recover_without_duplicate(delivery_setup, phase):
    store, owner, exports, journal, c, scope, req, internal, original = delivery_setup
    def crash(at):
        if at == phase:
            raise RuntimeError('crash')
    with pytest.raises(RuntimeError):
        exports.publish(internal, crash=crash)
    exports.recover()
    if phase == 'intent':
        assert store.conn.execute('SELECT count(*) FROM deliveries').fetchone()[0] == 0
        exports.publish(internal)
    assert store.conn.execute('SELECT count(*) FROM deliveries').fetchone()[0] == 1
    assert store.conn.execute("SELECT count(*) FROM client_events WHERE kind='delivery.ready'").fetchone()[0] == 1
    exports.recover()
    assert store.conn.execute('SELECT count(*) FROM deliveries').fetchone()[0] == 1


def test_no_partial_or_mismatched_publication(delivery_setup):
    store, owner, exports, journal, c, scope, req, internal, original = delivery_setup
    original.write_bytes(b'%PDF incomplete')
    with pytest.raises(ProtocolError):
        exports.publish(internal)
    assert store.conn.execute('SELECT count(*) FROM deliveries').fetchone()[0] == 0
    original.write_bytes(PDF)
    store.index.update_download_status(internal, DownloadStatus.DOWNLOADING)
    assert exports.publish(internal) == []


def test_cancel_after_import_acknowledges_history_without_access(delivery_setup):
    store, owner, exports, journal, c, scope, req, internal, original = delivery_setup
    delivery = exports.publish(internal)[0]
    req = store.request(c['client_id'], req['id'])
    store.cancel(c['client_id'], req['id'], req['revision'])
    with pytest.raises(ProtocolError):
        with exports.open_content(c['client_id'], delivery['id']):
            pass
    receipt = {'receipt_id':'consumer-copy', 'sha256':delivery['sha256'], 'size':delivery['size']}
    assert journal.acknowledge(c['client_id'], delivery['id'], receipt) == journal.acknowledge(c['client_id'], delivery['id'], receipt)
    with pytest.raises(ProtocolError):
        journal.acknowledge(c['client_id'], delivery['id'], {**receipt, 'receipt_id':'other'})
    assert original.read_bytes() == PDF


def test_pins_leases_purge_and_unavailable_snapshot(delivery_setup):
    store, owner, exports, journal, c, scope, req, internal, original = delivery_setup
    d = exports.publish(internal)[0]
    store.limits.export_grace_seconds = 0
    assert exports.cleanup() == []
    store.update_scope(c['client_id'], scope['id'], {'enabled':False}, 1)
    assert exports.cleanup() == []
    store.update_scope(c['client_id'], scope['id'], {'enabled':True}, 2)
    with exports.open_content(c['client_id'], d['id']) as (stream, data):
        assert exports.cleanup(purge=d['content_generation']) == []
        assert stream.read() == PDF
    assert exports.cleanup(purge=d['content_generation']) == [d['content_generation']]
    snap = journal.snapshot(c['client_id'])
    assert any(item['kind']=='delivery' and item['resource']['state']=='unavailable' for item in snap['items'])
    assert original.exists()


def test_event_replay_snapshot_boundary_expiry_and_epoch(delivery_setup):
    store, owner, exports, journal, c, scope, req, internal, original = delivery_setup
    c2, s2 = client_scope(store)
    page = journal.events(c['client_id'], limit=1)
    assert journal.events(c['client_id'], limit=1) == page
    with pytest.raises(ProtocolError):
        journal.events(c2['client_id'], page['cursor'])
    store.limits.page_default = 1
    snap = journal.snapshot(c['client_id'])
    exports.publish(internal)
    after = journal.events(c['client_id'], snap['handoff_cursor'], limit=100)
    assert any(e['kind']=='delivery.ready' for e in after['items'])
    store.conn.execute('UPDATE snapshots SET expires_at=?', (timestamp(-1),))
    store.conn.commit()
    with pytest.raises(ProtocolError) as exc:
        journal.snapshot_page(c['client_id'], snap['id'], snap['next'])
    assert exc.value.code == 'resync_required'
    store.limits.event_seconds = -1
    journal.cleanup()
    with pytest.raises(ProtocolError) as exc:
        journal.events(c['client_id'], page['cursor'])
    assert exc.value.code == 'resync_required'
    store.limits.page_default = 100
    assert any(i['kind']=='delivery' for i in journal.snapshot(c['client_id'])['items'])
    old_identity = store.identity()['instance_id']
    journal.rotate_epoch()
    assert store.identity()['instance_id'] == old_identity
    with pytest.raises(ProtocolError) as exc:
        journal.events(c['client_id'], after['cursor'])
    assert exc.value.code == 'resync_required'


def test_mount_views_are_confined_and_client_specific(delivery_setup, tmp_path):
    store, owner, exports, journal, c, scope, req, internal, original = delivery_setup
    c2, s2 = client_scope(store)
    exports = Exports(store, owner, views={c['client_id']:tmp_path/'view'}, trusted_mounts=True)
    d = exports.publish(internal)[0]
    exports.sync_views()
    d = exports.delivery(c['client_id'], d['id'])
    assert (tmp_path/'view'/d['transfer']['mount']).read_bytes() == PDF
    assert len(list((tmp_path/'view').iterdir())) == 1
    with pytest.raises(ProtocolError):
        exports.path('../original.pdf')
    escaped = owner.exports/'escape.pdf'
    escaped.symlink_to(original)
    with pytest.raises(ProtocolError):
        exports.path('escape.pdf')
    store.cancel(c['client_id'], req['id'], store.request(c['client_id'], req['id'])['revision'])
    exports.sync_views()
    assert not list((tmp_path/'view').iterdir())


def test_alias_retained_export_survives_removed_original(delivery_setup):
    store, owner, exports, journal, c, scope, request, internal, original = delivery_setup
    first = exports.publish(internal)[0]
    alias = issue(store, year=2024)
    alias_id = store.internal_issue(alias)
    store.index.update_download_status(alias_id, DownloadStatus.COMPLETE, str(original), len(PDF), hashlib.sha256(PDF).hexdigest())
    store.create_request(c['client_id'], scope['id'], {'issue_id': alias})
    second = exports.publish(alias_id)[0]
    assert second['content_generation'] == first['content_generation']
    original.unlink()
    late = store.create_request(c['client_id'], scope['id'], {'issue_id': alias})
    third = exports.publish(alias_id)[0]
    assert third['request_id'] == late['id'] and third['content_generation'] == first['content_generation']
