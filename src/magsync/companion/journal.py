"""Client-bound replay cursors, consistent snapshots and durable receipts."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json

from .protocol import ProtocolError
from .store import json_text, timestamp, uid


class Journal:
    def __init__(self, store, exports):
        self.store, self.exports = store, exports
        self.conn = store.conn

    def cursor(self, client_id: str, position: int, *, kind: str = 'events', scope: str = '', snapshot: str = '') -> str:
        identity = self.store.identity()
        body = json_text({'instance': identity['instance_id'], 'epoch': identity['recovery_epoch'],
                          'client': client_id, 'position': position, 'kind': kind, 'scope': scope, 'snapshot': snapshot})
        encoded = base64.urlsafe_b64encode(body.encode()).decode().rstrip('=')
        signature = hmac.new(bytes.fromhex(identity['cursor_secret']), encoded.encode(), hashlib.sha256).hexdigest()
        return encoded + '.' + signature

    def decode(self, client_id: str, cursor: str, *, kind: str = 'events', scope: str = '', snapshot: str = '') -> int:
        try:
            if len(cursor) > 2048:
                raise ValueError
            encoded, signature = cursor.split('.')
            identity = self.store.identity()
            expected = hmac.new(bytes.fromhex(identity['cursor_secret']), encoded.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected, signature):
                raise ValueError
            body = json.loads(base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4)))
            if body['client'] != client_id or body['kind'] != kind or body['scope'] != scope or body['snapshot'] != snapshot:
                raise ValueError
            if body['instance'] != identity['instance_id'] or body['epoch'] != identity['recovery_epoch']:
                raise ProtocolError('resync_required')
            position = body['position']
            if type(position) is not int or position < 0:
                raise ValueError
            return position
        except (ValueError, KeyError, TypeError, UnicodeError):
            raise ProtocolError('not_found') from None

    def events(self, client_id: str, cursor: str | None = None, *, limit: int | None = None, scope: str = '') -> dict:
        self.store.client(client_id)
        size = self.store.page_size(limit)
        if scope:
            self.store.scope(client_id, scope)
        position = self.decode(client_id, cursor, scope=scope) if cursor else 0
        with self.store.transaction():
            client = self.conn.execute('SELECT event_floor,event_seq FROM clients WHERE id=?', (client_id,)).fetchone()
            if position < client['event_floor']:
                raise ProtocolError('resync_required')
            # All events are sequenced for one principal. Scope filtering is applied
            # in SQL; the watermark advances even over nonmatching events.
            sql = 'SELECT * FROM client_events WHERE client_id=? AND seq>?'
            args = [client_id, position]
            if scope:
                sql += " AND (json_extract(payload,'$.scope_id')=? OR (kind LIKE 'scope.%' AND resource_id=?))"
                args.extend([scope, scope])
            rows = self.conn.execute(sql + ' ORDER BY seq LIMIT ?', (*args, size + 1)).fetchall()
            page = rows[:size]
            end = page[-1]['seq'] if len(rows) > size else client['event_seq']
        return {'items': [{**{k: r[k] for k in ('id', 'seq', 'kind', 'resource_id', 'created_at')},
                           'resource': json.loads(r['payload'])} for r in page],
                'cursor': self.cursor(client_id, end, scope=scope), 'has_more': len(rows) > size}

    def snapshot(self, client_id: str) -> dict:
        self.store.client(client_id)
        with self.store.transaction():
            active = self.conn.execute('SELECT count(*) FROM snapshots WHERE client_id=? AND expires_at>?',
                                       (client_id, timestamp())).fetchone()[0]
            if active >= self.store.limits.snapshots_per_client:
                raise ProtocolError('capacity_exhausted')
            snapshot_id = uid()
            watermark = self.conn.execute('SELECT event_seq FROM clients WHERE id=?', (client_id,)).fetchone()[0]
            epoch = self.store.identity()['recovery_epoch']
            self.conn.execute('INSERT INTO snapshots VALUES(?,?,?,?,?,?)', (snapshot_id, client_id, epoch,
                              watermark, timestamp(self.store.limits.snapshot_seconds), timestamp()))
            ordinal = 0

            def append(kind, value):
                nonlocal ordinal
                self.conn.execute('INSERT INTO snapshot_items VALUES(?,?,?)',
                                  (snapshot_id, ordinal, json_text({'kind': kind, 'resource': value})))
                ordinal += 1

            for scope in self.conn.execute('SELECT * FROM scopes WHERE client_id=? ORDER BY id', (client_id,)):
                append('scope', dict(scope))
                for sub in self.conn.execute('SELECT id FROM subscriptions WHERE scope_id=? ORDER BY id', (scope['id'],)):
                    append('subscription', self.store.subscription(client_id, sub[0]))
                for req in self.conn.execute('SELECT id FROM acquisition_requests WHERE scope_id=? ORDER BY id', (scope['id'],)):
                    append('request', self.store.request(client_id, req[0]))
                    for delivery in self.conn.execute('SELECT id FROM deliveries WHERE request_id=? ORDER BY id', (req[0],)):
                        append('delivery', self.exports.delivery(client_id, delivery[0], check_content=False))
        return self.snapshot_page(client_id, snapshot_id)

    def snapshot_page(self, client_id: str, snapshot_id: str, cursor: str | None = None, *, limit: int | None = None) -> dict:
        self.store.client(client_id)
        size = self.store.page_size(limit)
        row = self.conn.execute('SELECT * FROM snapshots WHERE id=? AND client_id=?', (snapshot_id, client_id)).fetchone()
        if not row:
            raise ProtocolError('not_found')
        if row['expires_at'] <= timestamp() or row['epoch'] != self.store.identity()['recovery_epoch']:
            raise ProtocolError('resync_required')
        position = self.decode(client_id, cursor, kind='snapshot', snapshot=snapshot_id) if cursor else 0
        rows = self.conn.execute('SELECT * FROM snapshot_items WHERE snapshot_id=? AND ordinal>=? ORDER BY ordinal LIMIT ?',
                                 (snapshot_id, position, size + 1)).fetchall()
        return {'id': snapshot_id, 'items': [json.loads(r['payload']) for r in rows[:size]],
                'next': self.cursor(client_id, position + size, kind='snapshot', snapshot=snapshot_id) if len(rows) > size else None,
                'handoff_cursor': self.cursor(client_id, row['watermark']), 'expires_at': row['expires_at']}

    def acknowledge(self, client_id: str, delivery_id: str, body: dict) -> dict:
        with self.store.transaction():
            # Historical receipt remains legal after cancellation; authentication
            # and principal ownership still apply, while retrieval stays revoked.
            delivery = self.exports.delivery(client_id, delivery_id, check_content=False)
            if body['sha256'] != delivery['sha256'] or body['size'] != delivery['size']:
                raise ProtocolError('receipt_conflict')
            old = self.conn.execute('SELECT * FROM acknowledgments WHERE delivery_id=?', (delivery_id,)).fetchone()
            if old:
                if old['receipt_id'] != body['receipt_id']:
                    raise ProtocolError('receipt_conflict')
                return dict(old)
            self.conn.execute('INSERT INTO acknowledgments VALUES(?,?,?,?,?)',
                              (delivery_id, body['receipt_id'], body['sha256'], body['size'], timestamp()))
            self.conn.execute("UPDATE deliveries SET state='acknowledged' WHERE id=? AND state='ready'", (delivery_id,))
            receipt = dict(self.conn.execute('SELECT * FROM acknowledgments WHERE delivery_id=?', (delivery_id,)).fetchone())
            self.store.event(client_id, 'delivery.acknowledged', delivery_id,
                             {'id': delivery_id, 'scope_id': delivery['scope_id'], 'receipt': receipt})
            return receipt

    def cleanup(self):
        with self.store.transaction():
            cutoff = timestamp(-self.store.limits.event_seconds)
            for row in self.conn.execute('SELECT client_id,max(seq) seq FROM client_events WHERE created_at<? GROUP BY client_id', (cutoff,)).fetchall():
                self.conn.execute('UPDATE clients SET event_floor=max(event_floor,?) WHERE id=?', (row['seq'], row['client_id']))
                self.conn.execute('DELETE FROM client_events WHERE client_id=? AND seq<=?', (row['client_id'], row['seq']))
            self.conn.execute('DELETE FROM idempotency WHERE expires_at<?', (timestamp(),))
            # Keep expired snapshot identity until retention passes so late pages
            # get resync_required rather than silently switching generations.
            self.conn.execute('DELETE FROM snapshot_items WHERE snapshot_id IN (SELECT id FROM snapshots WHERE expires_at<?)', (timestamp(),))
            self.conn.execute('DELETE FROM snapshots WHERE expires_at<?', (timestamp(-self.store.limits.event_seconds),))

    def rotate_epoch(self):
        with self.store.transaction():
            self.conn.execute('UPDATE service_identity SET recovery_epoch=? WHERE id=1', (uid(),))
        self.exports.recover()
        return {k: self.store.identity()[k] for k in ('instance_id', 'recovery_epoch')}
