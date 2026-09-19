"""Same-host advisory locks plus durable fencing generations."""
from __future__ import annotations

import json
import os
from pathlib import Path

from magsync.core.locking import NOFOLLOW, lock, unlock

from .protocol import ProtocolError
from .schema import LOCAL_CLIENT
from .store import json_text, timestamp, uid

# Unclean stops a remote operation may survive before it is failed.
MAX_RECOVERIES = 3


class FileLock:
    def __init__(self, path: Path):
        self.path = path
        self.fd: int | None = None

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | NOFOLLOW, 0o600)
        try:
            lock(fd, blocking=False)
        except OSError:
            os.close(fd)
            raise ProtocolError("runtime_unavailable") from None
        except BaseException:
            os.close(fd)
            raise
        self.fd = fd
        return self

    def release(self):
        if self.fd is not None:
            unlock(self.fd)
            os.close(self.fd)
            self.fd = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_exc):
        self.release()


class Ownership:
    def __init__(self, store, output: Path, exports: Path):
        self.store = store
        self.output = output.expanduser().resolve()
        self.exports = exports.expanduser().resolve()
        self.owner_id = uid()
        self.generation: int | None = None
        db = store.index.db_path.expanduser().resolve()
        self.locks = [FileLock(path) for path in sorted({
            Path(str(db) + ".owner.lock"), self.output / ".magsync-owner.lock",
            self.exports / ".magsync-owner.lock",
        })]

    def acquire(self, *, accepting: bool = False):
        """Take exclusive ownership and recover what the previous owner left.

        ``accepting`` advertises that this owner drains the durable command
        queue. Only long-running runtimes accept; a temporary owner (a
        terminal or TUI process running just its own command) never does, so
        nothing is ever queued for a process that will not execute it.
        """
        try:
            for file_lock in self.locks:
                file_lock.acquire()
            with self.store.transaction():
                row = self.store.conn.execute("SELECT * FROM runtime_state WHERE id=1").fetchone()
                # Changing roots with issued content would invalidate transfer identities.
                if row["export_root"] and row["export_root"] != str(self.exports):
                    if self.store.conn.execute("SELECT 1 FROM content_objects LIMIT 1").fetchone():
                        raise ProtocolError("configuration_managed")
                self.generation = row["generation"] + 1
                self.store.conn.execute("""UPDATE runtime_state SET generation=?,owner_id=?,output_root=?,export_root=?,
                    heartbeat_at=?,accepting=? WHERE id=1""", (self.generation, self.owner_id,
                    str(self.output), str(self.exports), timestamp(), 1 if accepting else 0))
                # OS locks prove abandonment. Never infer it from heartbeat age.
                self.store.conn.execute("""UPDATE downloads SET status='pending' WHERE status='downloading'
                    AND issue_id IN (SELECT issue_id FROM acquisition_attempts WHERE state='running')""")
                # Legacy downloading rows have no owner; all current writers must hold these locks.
                self.store.conn.execute("""UPDATE downloads SET status='pending' WHERE status='downloading'
                    AND NOT EXISTS(SELECT 1 FROM acquisition_attempts a WHERE a.issue_id=downloads.issue_id AND a.state='running')""")
                self.store.conn.execute("UPDATE acquisition_attempts SET state='abandoned' WHERE state='running'")
                self._recover_operations()
                self.store.conn.execute("DELETE FROM transfer_leases")
                for request in self.store.conn.execute("""SELECT r.*,s.client_id FROM acquisition_requests r
                    JOIN scopes s ON s.id=r.scope_id WHERE r.state='acquiring'""").fetchall():
                    self.store.conn.execute("UPDATE acquisition_requests SET state='queued',revision=revision+1 WHERE id=?", (request['id'],))
                    self.store.event(request['client_id'], 'request.updated', request['id'],
                        {'id': request['id'], 'scope_id': request['scope_id'], 'state': 'queued', 'revision': request['revision'] + 1})
            return self
        except BaseException:
            self.release()
            raise

    def _recover_operations(self):
        """Settle operations that were running when the previous owner stopped.

        A local terminal operation belonged to a process that has gone: it is
        recorded as interrupted and never executed again. A remote API
        operation is durable, so it is queued again, but at most
        ``MAX_RECOVERIES`` times; an operation that keeps coinciding with
        unclean stops fails instead of looping forever.
        """
        now = timestamp()
        self.store.conn.execute("""UPDATE operations SET state='failed',result=?,updated_at=?
            WHERE state='running' AND client_id=?""", (json_text({'code': 'interrupted'}), now, LOCAL_CLIENT))
        for row in self.store.conn.execute("SELECT id,result FROM operations WHERE state='running'").fetchall():
            try:
                result = json.loads(row['result']) or {}
            except ValueError:
                result = {}
            recoveries = int(result.get('_recoveries', 0)) + 1
            if recoveries > MAX_RECOVERIES:
                self.store.conn.execute("UPDATE operations SET state='failed',result=?,updated_at=? WHERE id=?",
                                        (json_text({'code': 'runtime_unavailable'}), now, row['id']))
            else:
                self.store.conn.execute("UPDATE operations SET state='queued',result=?,updated_at=? WHERE id=?",
                                        (json_text({**result, '_recoveries': recoveries}), now, row['id']))

    def check(self):
        row = self.store.conn.execute("SELECT owner_id,generation FROM runtime_state WHERE id=1").fetchone()
        if not all(file_lock.fd is not None for file_lock in self.locks) or not row or (row[0], row[1]) != (self.owner_id, self.generation):
            raise ProtocolError("runtime_unavailable")

    def heartbeat(self, *, accepting: bool = True):
        self.check()
        with self.store.transaction():
            self.store.conn.execute("UPDATE runtime_state SET heartbeat_at=?,accepting=? WHERE id=1",
                                    (timestamp(), accepting))

    def release(self):
        try:
            if self.generation is not None:
                with self.store.transaction():
                    self.store.conn.execute("UPDATE runtime_state SET accepting=0,owner_id=NULL WHERE id=1 AND owner_id=? AND generation=?",
                                            (self.owner_id, self.generation))
        finally:
            self.generation = None
            for file_lock in reversed(self.locks):
                file_lock.release()

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_exc):
        self.release()
