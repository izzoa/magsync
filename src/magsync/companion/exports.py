"""Independent immutable PDF exports with publication intents and read leases."""
from __future__ import annotations

import hashlib
import asyncio
import os
import shutil
from contextlib import contextmanager
from pathlib import Path

from magsync.core.downloader import _classify_payload

from .protocol import ProtocolError
from .store import timestamp, uid


def _missing_original() -> ProtocolError:
    """The organized original is gone: publication needs a fresh download."""
    error = ProtocolError('content_unavailable')
    error.missing_original = True
    return error


class Exports:
    def __init__(self, store, owner, *, views: dict[str, Path] | None = None, trusted_mounts: bool = False):
        self.store, self.owner, self.conn = store, owner, store.conn
        self.root = owner.exports
        self.root.mkdir(parents=True, exist_ok=True)
        self.views = {}
        self._views_lock = asyncio.Lock()
        if views and not trusted_mounts:
            raise ProtocolError('configuration_managed')
        for client_id, path in (views or {}).items():
            store.client(client_id)
            path = Path(path).expanduser().resolve()
            if path == self.root or path.is_relative_to(self.root) or self.root.is_relative_to(path):
                raise ProtocolError('configuration_managed')
            if any(path == other or path.is_relative_to(other) or other.is_relative_to(path) for other in self.views.values()):
                raise ProtocolError('configuration_managed')
            path.mkdir(parents=True, exist_ok=True)
            self.views[client_id] = path

    @staticmethod
    def verify(path: Path, expected_hash: str | None = None, expected_size: int | None = None) -> tuple[str, int]:
        digest = hashlib.sha256()
        size = 0
        try:
            with path.open('rb') as stream:
                head = stream.read(1024)
                if _classify_payload(head) != 'pdf':
                    raise ProtocolError('integrity_failed')
                digest.update(head)
                size += len(head)
                tail = head
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block)
                    size += len(block)
                    tail = (tail + block)[-1024:]
                # A header alone cannot establish complete bytes.
                if b'%%EOF' not in tail:
                    raise ProtocolError('integrity_failed')
        except OSError:
            raise ProtocolError('content_unavailable') from None
        actual = digest.hexdigest()
        if (expected_hash and expected_hash != actual) or (expected_size is not None and expected_size != size):
            raise ProtocolError('integrity_failed')
        return actual, size

    def path(self, relative: str) -> Path:
        if Path(relative).is_absolute() or '..' in Path(relative).parts:
            raise ProtocolError('content_unavailable')
        candidate = self.root / relative
        if candidate.is_symlink() or not candidate.resolve().is_relative_to(self.root):
            raise ProtocolError('content_unavailable')
        return candidate

    def capacity(self, extra: int = 0) -> dict:
        used = self.conn.execute("SELECT coalesce(sum(size),0) FROM content_objects WHERE state IN ('ready','exporting')").fetchone()[0]
        free = shutil.disk_usage(self.root).free
        available = used + extra <= self.store.limits.export_bytes and free - extra >= self.store.limits.minimum_free_bytes
        return {'export_bytes': used, 'export_limit': self.store.limits.export_bytes, 'free_bytes': free,
                'minimum_free_bytes': self.store.limits.minimum_free_bytes, 'available': available}

    def require_capacity(self, extra: int):
        if not self.capacity(extra)['available']:
            raise ProtocolError('capacity_exhausted')

    def require_download_capacity(self, concurrent: int, *, exporting: bool = True):
        """Reserve headroom before a physical transfer of unknown size.

        Work that will be exported reserves bounded payloads on both storage
        filesystems (part file, organized file and export copy). Local-only
        work never touches the export root, so it reserves one bounded
        transfer on the output filesystem and ignores export capacity.
        """
        limits = self.store.limits
        output = self.owner.output
        if not exporting:
            if shutil.disk_usage(output).free - limits.maximum_download_bytes < limits.minimum_free_bytes:
                raise ProtocolError('capacity_exhausted')
            return
        size = limits.maximum_download_bytes * concurrent
        self.require_capacity(size)
        shared = output.stat().st_dev == self.root.stat().st_dev
        required = size * (3 if shared else 2)
        if shutil.disk_usage(output).free - required < limits.minimum_free_bytes:
            raise ProtocolError('capacity_exhausted')

    def publish(self, issue_id: int, *, crash=None) -> list[dict]:
        steps = self._publication_steps(issue_id, crash=crash)
        value = None
        failure = None
        while True:
            try:
                action = steps.throw(failure) if failure else steps.send(value)
            except StopIteration as result:
                return result.value
            try:
                value, failure = action(), None
            except Exception as exc:
                failure = exc

    async def publish_async(self, issue_id: int) -> list[dict]:
        return await self._async_steps(self._publication_steps(issue_id))

    async def _async_steps(self, steps):
        # Only staging I/O runs in worker threads. Final rename, fencing and
        # ready transactions stay together on the owner event loop. A canceled
        # worker can finish a staging write but can never publish a delivery.
        value = None
        failure = None
        try:
            while True:
                try:
                    action = steps.throw(failure) if failure else steps.send(value)
                except StopIteration as result:
                    return result.value
                try:
                    value, failure = await asyncio.to_thread(action), None
                except Exception as exc:
                    failure = exc
        finally:
            steps.close()

    def _publication_steps(self, issue_id: int, *, crash=None):
        self.owner.check()
        download = self.conn.execute("SELECT * FROM downloads WHERE issue_id=? AND status='complete'", (issue_id,)).fetchone()
        if not download:
            return []
        if not self.store.issue_wanted(issue_id):
            return []
        # Exports exist only for remote demand: local requests are satisfied
        # by the organized download itself, so nothing is copied or pinned.
        if not self.store.remote_wanted(issue_id):
            with self.store.transaction():
                self._fulfill_local(issue_id)
            return []
        if not download['file_path']:
            raise _missing_original()
        # Published bytes outlive title repairs, removed originals and delayed
        # consumers. A recorded new digest selects a new generation instead.
        retained = self.conn.execute("""SELECT o.* FROM content_objects o JOIN export_intents e ON e.content_id=o.id
            WHERE e.issue_id=? AND o.state='ready' AND (? IS NULL OR o.sha256=?)
            ORDER BY o.created_at DESC LIMIT 1""", (issue_id, download['sha256'], download['sha256'])).fetchone()
        if retained:
            try:
                yield lambda: self.verify(self.path(retained['relative_path']), retained['sha256'], retained['size'])
            except ProtocolError:
                self.unavailable(retained['id'])
            else:
                with self.store.transaction():
                    return self._fulfill(issue_id, retained['id'])
        source = Path(download['file_path'])
        try:
            digest, size = yield lambda: self.verify(source, download['sha256'], download['file_size_bytes'])
        except ProtocolError as exc:
            if exc.code == 'content_unavailable':
                raise _missing_original() from None
            raise
        obj = self.conn.execute("SELECT * FROM content_objects WHERE sha256=? AND size=? AND state='ready' ORDER BY created_at DESC LIMIT 1",
                                (digest, size)).fetchone()
        if obj:
            try:
                yield lambda: self.verify(self.path(obj['relative_path']), digest, size)
            except ProtocolError:
                self.unavailable(obj['id'])
                obj = None
        if not obj:
            self.require_capacity(size)
            content_id, intent_id = uid(), uid()
            relative = content_id + '.pdf'
            with self.store.transaction():
                self.owner.check()
                self.conn.execute("INSERT INTO content_objects VALUES(?,?,?,'exporting',?,?,NULL)",
                                  (content_id, digest, size, relative, timestamp()))
                self.conn.execute("INSERT INTO export_intents VALUES(?,?,?,?,?,'staging',?)",
                                  (intent_id, issue_id, content_id, self.owner.owner_id, self.owner.generation, timestamp()))
            if crash:
                crash('intent')
            staged = self.path(content_id + '.part')
            try:
                def copy_stage():
                    with source.open('rb') as original, staged.open('xb') as target:
                        shutil.copyfileobj(original, target, 1024 * 1024)
                        target.flush()
                        os.fsync(target.fileno())
                    self.verify(staged, digest, size)
                yield copy_stage
                self.owner.check()
                os.chmod(staged, 0o444)
                os.replace(staged, self.path(relative))
                self._sync_directory(self.root)
                if crash:
                    crash('published')
                with self.store.transaction():
                    self.owner.check()
                    self.conn.execute("UPDATE content_objects SET state='ready' WHERE id=?", (content_id,))
                    self.conn.execute("UPDATE export_intents SET state='published' WHERE id=?", (intent_id,))
                    deliveries = self._fulfill(issue_id, content_id)
                if crash:
                    crash('committed')
                return deliveries
            except BaseException:
                # Leave the durable intent for recovery; never expose staging bytes.
                raise
        with self.store.transaction():
            # Alias issues need their own retained-object association as well.
            self.conn.execute("INSERT INTO export_intents VALUES(?,?,?,?,?,'published',?)",
                              (uid(), issue_id, obj['id'], self.owner.owner_id, self.owner.generation, timestamp()))
            return self._fulfill(issue_id, obj['id'])

    def _fulfill_local(self, issue_id: int) -> None:
        self.owner.check()
        for req in self.conn.execute("""SELECT r.id FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id
                WHERE r.issue_id=? AND s.client_id='local' AND r.state IN ('queued','acquiring')""", (issue_id,)).fetchall():
            if self.store.eligible(req['id']):
                self.conn.execute("UPDATE acquisition_requests SET state='fulfilled',fulfillment_error=NULL,revision=revision+1 WHERE id=?",
                                  (req['id'],))

    @staticmethod
    def _sync_directory(path: Path):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _fulfill(self, issue_id: int, content_id: str) -> list[dict]:
        self.owner.check()
        deliveries = []
        rows = self.conn.execute("""SELECT r.*,s.client_id FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id
            WHERE r.issue_id=? AND r.state IN ('queued','acquiring','suspended')""", (issue_id,)).fetchall()
        for req in rows:
            if not self.store.eligible(req['id']):
                continue
            # Local requests retain ordinary organized files; no consumer export receipt.
            if req['client_id'] != 'local':
                delivery_id = uid()
                self.conn.execute("INSERT OR IGNORE INTO deliveries VALUES(?,?,?,'ready',?)",
                                  (delivery_id, req['id'], content_id, timestamp()))
                delivery_id = self.conn.execute('SELECT id FROM deliveries WHERE request_id=? AND content_id=?', (req['id'], content_id)).fetchone()[0]
                result = self.delivery(req['client_id'], delivery_id, check_content=False)
                self.store.event(req['client_id'], 'delivery.ready', delivery_id, result)
                deliveries.append(result)
            self.conn.execute("UPDATE acquisition_requests SET state='fulfilled',fulfillment_error=NULL,revision=revision+1 WHERE id=?", (req['id'],))
            self.store.event(req['client_id'], 'request.updated', req['id'], lambda req=req: self.store.request(req['client_id'], req['id']))
        self.conn.execute('UPDATE content_objects SET unpinned_at=NULL WHERE id=?', (content_id,))
        return deliveries

    def delivery(self, client_id: str, delivery_id: str, *, check_content: bool = True) -> dict:
        self.store.client(client_id)
        row = self.conn.execute("""SELECT d.*,r.scope_id,r.issue_id,r.state request_state,s.enabled scope_enabled,
                o.sha256,o.size,o.relative_path,o.state content_state,p.id public_issue_id
                FROM deliveries d JOIN acquisition_requests r ON r.id=d.request_id JOIN scopes s ON s.id=r.scope_id
                JOIN content_objects o ON o.id=d.content_id JOIN provider_issues p ON p.issue_id=r.issue_id
                WHERE d.id=? AND s.client_id=?""", (delivery_id, client_id)).fetchone()
        if not row:
            raise ProtocolError('not_found')
        identity = self.store.identity()
        authorized = bool(row['scope_enabled'] and row['request_state'] != 'canceled' and row['content_state'] == 'ready')
        if check_content and authorized and not self.path(row['relative_path']).is_file():
            self.unavailable(row['content_id'])
            authorized = False
        issue = self.store.issue(row['public_issue_id'])
        result = {'id': delivery_id, 'instance_id': identity['instance_id'], 'recovery_epoch': identity['recovery_epoch'],
                  'client_id': client_id, 'scope_id': row['scope_id'], 'request_id': row['request_id'],
                  'issue_id': row['public_issue_id'], 'content_generation': row['content_id'],
                  'sha256': row['sha256'], 'size': row['size'], 'media_type': 'application/pdf',
                  'title': issue['title'], 'year': issue['year'], 'month': issue['month'], 'source': issue['source'],
                  'state': row['state'] if row['content_state'] == 'ready' else 'unavailable',
                  'transfer': {'http': f'/v1/deliveries/{delivery_id}/content'} if authorized else {}}
        receipt = self.conn.execute('SELECT * FROM acknowledgments WHERE delivery_id=?', (delivery_id,)).fetchone()
        result['receipt'] = dict(receipt) if receipt else None
        if authorized and client_id in self.views and (self.views[client_id] / (delivery_id + '.pdf')).is_file():
            result['transfer']['mount'] = delivery_id + '.pdf'
        return result

    @contextmanager
    def open_content(self, client_id: str, delivery_id: str):
        lease_id = uid()
        with self.store.transaction():
            self.owner.check()
            delivery = self.delivery(client_id, delivery_id)
            if not delivery['transfer']:
                raise ProtocolError('content_unavailable')
            obj = self.conn.execute('SELECT * FROM content_objects WHERE id=?', (delivery['content_generation'],)).fetchone()
            self.conn.execute('INSERT INTO transfer_leases VALUES(?,?,?,?,?)', (lease_id, obj['id'],
                              self.owner.owner_id, self.owner.generation, timestamp()))
            try:
                fd = os.open(self.path(obj['relative_path']), os.O_RDONLY | os.O_NOFOLLOW)
            except OSError:
                raise ProtocolError('content_unavailable') from None
        try:
            with os.fdopen(fd, 'rb') as stream:
                yield stream, delivery
        finally:
            with self.store.transaction():
                self.conn.execute('DELETE FROM transfer_leases WHERE id=?', (lease_id,))

    def unavailable(self, content_id: str):
        with self.store.transaction():
            self.conn.execute("UPDATE content_objects SET state='unavailable' WHERE id=?", (content_id,))
            for row in self.conn.execute("""SELECT d.id,s.client_id,r.scope_id FROM deliveries d
                     JOIN acquisition_requests r ON r.id=d.request_id JOIN scopes s ON s.id=r.scope_id
                     WHERE d.content_id=? AND d.state!='unavailable'""", (content_id,)).fetchall():
                self.conn.execute("UPDATE deliveries SET state='unavailable' WHERE id=?", (row['id'],))
                self.store.event(row['client_id'], 'delivery.unavailable', row['id'],
                                 {'id': row['id'], 'scope_id': row['scope_id'], 'state': 'unavailable'})

    def pinned(self, content_id: str) -> bool:
        if self.conn.execute('SELECT 1 FROM transfer_leases WHERE content_id=?', (content_id,)).fetchone():
            return True
        return bool(self.conn.execute("""SELECT 1 FROM deliveries d JOIN acquisition_requests r ON r.id=d.request_id
            JOIN scopes s ON s.id=r.scope_id JOIN clients c ON c.id=s.client_id
            LEFT JOIN acknowledgments a ON a.delivery_id=d.id WHERE d.content_id=? AND a.delivery_id IS NULL
            AND (r.state!='canceled' OR s.enabled=0 OR c.enabled=0) LIMIT 1""", (content_id,)).fetchone())

    def cleanup(self, *, purge: str | None = None) -> list[str]:
        self.owner.check()
        removed = []
        with self.store.transaction():
            rows = self.conn.execute("SELECT * FROM content_objects WHERE state IN ('ready','unavailable','deleting')").fetchall()
            for row in rows:
                if purge and row['id'] != purge:
                    continue
                if self.conn.execute('SELECT 1 FROM transfer_leases WHERE content_id=?', (row['id'],)).fetchone():
                    continue
                if not purge:
                    if self.pinned(row['id']):
                        self.conn.execute('UPDATE content_objects SET unpinned_at=NULL WHERE id=?', (row['id'],))
                        continue
                    if row['unpinned_at'] is None:
                        self.conn.execute('UPDATE content_objects SET unpinned_at=? WHERE id=?', (timestamp(), row['id']))
                        continue
                    if row['unpinned_at'] > timestamp(-self.store.limits.export_grace_seconds):
                        continue
                self.unavailable(row['id'])
                self.path(row['relative_path']).unlink(missing_ok=True)
                removed.append(row['id'])
        self.sync_views()
        return removed

    def recover(self):
        self.owner.check()
        for obj in self.conn.execute("SELECT * FROM content_objects WHERE state IN ('ready','exporting')").fetchall():
            try:
                self.verify(self.path(obj['relative_path']), obj['sha256'], obj['size'])
            except ProtocolError:
                self._recovered_unavailable(obj)
                continue
            self._recovered_ready(obj)
        self.sync_views()

    async def recover_async(self):
        """Startup recovery that hashes exports off the event loop.

        Verification of a large export root can take minutes; hashing in
        worker threads keeps the runtime heartbeat and command polling live.
        Database transitions stay on the owner event loop.
        """
        self.owner.check()
        for obj in self.conn.execute("SELECT * FROM content_objects WHERE state IN ('ready','exporting')").fetchall():
            try:
                path = self.path(obj['relative_path'])
                await asyncio.to_thread(self.verify, path, obj['sha256'], obj['size'])
            except ProtocolError:
                self._recovered_unavailable(obj)
                continue
            self._recovered_ready(obj)
        await self.sync_views_async()

    def _recovered_unavailable(self, obj):
        self.unavailable(obj['id'])
        self.path(obj['id'] + '.part').unlink(missing_ok=True)

    def _recovered_ready(self, obj):
        with self.store.transaction():
            self.conn.execute("UPDATE content_objects SET state='ready' WHERE id=?", (obj['id'],))
            for intent in self.conn.execute('SELECT * FROM export_intents WHERE content_id=?', (obj['id'],)).fetchall():
                self._fulfill(intent['issue_id'], obj['id'])
                self.conn.execute("UPDATE export_intents SET state='published' WHERE id=?", (intent['id'],))

    def sync_views(self):
        for action in self._view_steps():
            action()

    async def sync_views_async(self):
        async with self._views_lock:
            await self._async_steps(self._view_steps())

    def _view_steps(self):
        for client_id, view in self.views.items():
            if view.is_symlink() or view.resolve() != view:
                raise ProtocolError("content_unavailable")
            allowed = set()
            client = self.conn.execute('SELECT enabled FROM clients WHERE id=?', (client_id,)).fetchone()
            if client and client[0]:
                for row in self.conn.execute("""SELECT d.id,o.relative_path,o.sha256,o.size FROM deliveries d
                    JOIN content_objects o ON o.id=d.content_id JOIN acquisition_requests r ON r.id=d.request_id
                    JOIN scopes s ON s.id=r.scope_id WHERE s.client_id=? AND s.enabled=1 AND r.state!='canceled' AND o.state='ready'""", (client_id,)).fetchall():
                    name = row['id'] + '.pdf'
                    allowed.add(name)
                    destination = view / name
                    if destination.is_symlink() or not destination.resolve().is_relative_to(view):
                        raise ProtocolError('content_unavailable')
                    if not destination.exists():
                        stage = view / (row['id'] + '.part')
                        if stage.is_symlink():
                            raise ProtocolError('content_unavailable')
                        # A unique part permits cancellation/disablement while
                        # copying without competing with another view refresh.
                        stage = view / (row['id'] + '-' + uid() + '.part')
                        def copy_view(stage=stage, row=row):
                            with self.path(row['relative_path']).open('rb') as source, stage.open('xb') as target:
                                shutil.copyfileobj(source, target, 1024 * 1024)
                                target.flush()
                                os.fsync(target.fileno())
                            self.verify(stage, row['sha256'], row['size'])
                        yield copy_view
                        self.owner.check()
                        current = self.delivery(client_id, row['id'], check_content=False)
                        if not current['transfer']:
                            allowed.discard(name)
                            stage.unlink(missing_ok=True)
                            continue
                        os.chmod(stage, 0o444)
                        os.replace(stage, destination)
                        self._sync_directory(view)
            for path in view.iterdir():
                if path.name not in allowed and path.suffix in ('.pdf', '.part'):
                    path.unlink()
