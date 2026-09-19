"""Durable local command admission and temporary-owner coordination.

A terminal or TUI command either runs under *temporary* ownership (no
long-running owner exists) or submits a durable local operation to the
long-running owner (daemon or service) and observes it until it finishes.

Temporary owners never accept queued work, so nothing is ever queued for a
process that will not run it; a second command waits briefly for one and
then reports that it is busy. A submitted operation belongs to its waiting
terminal: the terminal keeps it alive with a heartbeat and withdraws it if it
stops waiting before the owner starts it, so a command the user abandoned is
never executed later.
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import signal
import sqlite3
import tempfile
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

from .ownership import Ownership
from .protocol import ProtocolError
from .store import LOCAL_CLIENT, LOCAL_SCOPE, Store, json_text, timestamp, uid

owner_context = ContextVar('magsync_owner', default=None)
operation_context = ContextVar('magsync_operation', default=None)
snapshot_context = ContextVar('magsync_snapshot', default=None)

WAITING_NOTICE = 'Waiting for another magsync command to finish… (Ctrl-C to cancel)'
BUSY_MESSAGE = 'Another magsync command is still running; try again when it finishes.'
UNRESPONSIVE_MESSAGE = 'The magsync daemon or service holding this library is not responding; try again shortly.'
STILL_RUNNING_MESSAGE = 'The command is still running in the magsync daemon and will finish there.'
# How often a waiting terminal proves it is still waiting for its operation.
WAITER_HEARTBEAT_SECONDS = 2.0
_POLL_SECONDS = 0.2


class CoordinatorBusy(Exception):
    """Another process owns the store and cannot run this command for us."""


class StillRunning(Exception):
    """The terminal stopped waiting after the owner had started its command."""


class _WaitInterrupted(Exception):
    """A termination or hang-up signal arrived while waiting."""


@contextmanager
def read_only_snapshot(db_path: Path | None = None):
    """Yield an index over a private copy of the store; the original is never written."""
    from magsync.core.index import MagazineIndex, get_db_path
    from urllib.parse import quote
    with tempfile.TemporaryDirectory(prefix='magsync-preview-') as directory:
        copy = Path(directory)/'index.db'
        path = Path(db_path) if db_path is not None else get_db_path()
        if path.exists():
            source = sqlite3.connect('file:' + quote(str(path)) + '?mode=ro', uri=True)
            target = sqlite3.connect(copy)
            try:
                source.backup(target)
            finally:
                target.close()
                source.close()
        token = snapshot_context.set(copy)
        index = MagazineIndex(copy)
        try:
            yield index
        finally:
            index.close()
            snapshot_context.reset(token)


def local_request(store, issue_id: int) -> str:
    row = store.conn.execute("SELECT id FROM acquisition_requests WHERE scope_id='local' AND issue_id=? AND origin='explicit' AND state!='canceled'", (issue_id,)).fetchone()
    with store.transaction():
        request_id = row[0] if row else store._new_request(LOCAL_SCOPE, issue_id)
        operation = operation_context.get()
        if operation:
            store.conn.execute('INSERT OR IGNORE INTO operation_requests VALUES(?,?)', (operation, request_id))
        return request_id


def accept_local(store, kind: str, body: dict) -> str:
    with store.transaction():
        result = store.accept(LOCAL_CLIENT, 'local.' + kind, LOCAL_SCOPE, body, uid())
        operation_id = result['operation_id']
        # Retry selection is fixed at invocation, before waiting for the owner.
        if kind in ('retry', 'download'):
            with store.transaction():
                if kind == 'download':
                    ids = list(dict.fromkeys(body['issue_ids']))
                    if len(ids) > 500:
                        raise ProtocolError('invalid_request')
                    for issue_id in ids:
                        if not store.conn.execute('SELECT 1 FROM issues WHERE id=?', (issue_id,)).fetchone():
                            raise ProtocolError('not_found')
                        request_id = local_request(store, issue_id)
                        store.conn.execute("UPDATE downloads SET requested_by='manual' WHERE issue_id=?", (issue_id,))
                        store.conn.execute('INSERT OR IGNORE INTO operation_requests VALUES(?,?)', (operation_id, request_id))
                else:
                    from magsync.core.urls import is_valid_download_url
                    candidates = store.conn.execute("""SELECT i.id,i.limewire_url FROM downloads d
                        JOIN issues i ON i.id=d.issue_id JOIN magazines m ON m.id=i.magazine_id
                        WHERE d.status IN ('failed','unavailable')
                        AND (? IS NULL OR m.normalized_title LIKE ?)""", (body.get('query'), '%'+(body.get('query') or '')+'%')).fetchall()
                    skipped = excluded = 0
                    for candidate in candidates:
                        rows = store.conn.execute("SELECT id FROM acquisition_requests WHERE scope_id='local' AND issue_id=? AND state!='canceled'", (candidate['id'],)).fetchall()
                        eligible = [row[0] for row in rows if store.eligible(row[0])]
                        if not eligible:
                            excluded += 1
                        elif not is_valid_download_url(candidate['limewire_url'] or ''):
                            skipped += 1
                        else:
                            for request_id in eligible:
                                store.conn.execute('INSERT OR IGNORE INTO operation_requests VALUES(?,?)', (operation_id, request_id))
                    store.conn.execute('UPDATE operations SET result=? WHERE id=?',
                                       (json_text({'skipped': skipped, 'excluded': excluded}), operation_id))
        return operation_id


def live_owner(store, limits) -> bool:
    """True when a long-running owner currently accepts local operations."""
    row = store.conn.execute('SELECT accepting,heartbeat_at FROM runtime_state WHERE id=1').fetchone()
    return bool(row and row['accepting'] and row['heartbeat_at']
                and row['heartbeat_at'] >= timestamp(-limits.heartbeat_stale_seconds))


def owner_alive(store, limits) -> bool:
    """True while an owner holds the store and its heartbeat is fresh.

    Acceptance gates only new submissions. An owner that stopped accepting
    (draining before it stops, or after repeated work failures) still owns,
    and may still finish, the commands already submitted to it.
    """
    row = store.conn.execute('SELECT owner_id,heartbeat_at FROM runtime_state WHERE id=1').fetchone()
    return bool(row and row['owner_id'] and row['heartbeat_at']
                and row['heartbeat_at'] >= timestamp(-limits.heartbeat_stale_seconds))


def _busy(store) -> CoordinatorBusy:
    row = store.conn.execute('SELECT accepting FROM runtime_state WHERE id=1').fetchone()
    # An owner that advertises acceptance but stopped heartbeating is hung,
    # which is a different situation from another command still running.
    return CoordinatorBusy(UNRESPONSIVE_MESSAGE if row and row['accepting'] else BUSY_MESSAGE)


def claim_ownership(owner, store, limits, *, notice=None) -> bool:
    """Take temporary ownership, or report that a live owner will run the command.

    Returns True when this process now owns the store (it must release it)
    and False when the command should be submitted to the live owner. A
    temporary owner is waited for up to ``limits.lock_wait_seconds``;
    afterwards :class:`CoordinatorBusy` is raised and nothing is queued.
    """
    deadline = time.monotonic() + limits.lock_wait_seconds
    noticed = False
    while True:
        try:
            owner.acquire(accepting=False)
            return True
        except ProtocolError as exc:
            if exc.code != 'runtime_unavailable':
                raise
        if live_owner(store, limits):
            return False
        if time.monotonic() >= deadline:
            raise _busy(store)
        if notice is not None and not noticed:
            notice(WAITING_NOTICE)
            noticed = True
        time.sleep(0.5)


@contextmanager
def _termination_signals():
    """Turn SIGTERM/SIGHUP into an exception while a terminal waits (main thread only)."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def interrupt(_signum, _frame):
        raise _WaitInterrupted()

    previous = {}
    for name in ('SIGTERM', 'SIGHUP'):
        signum = getattr(signal, name, None)
        if signum is not None:
            previous[signum] = signal.signal(signum, interrupt)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


class LocalWaiter:
    """Observe one submitted local operation on behalf of its terminal.

    While the operation is queued the waiter refreshes ``updated_at`` (the
    owner abandons queued local operations whose waiter went quiet), prints
    how many operations are ahead once, and withdraws the operation if it
    stops waiting before the owner starts it. It keeps waiting while the
    owner is alive, whether or not that owner still accepts new commands.
    """

    def __init__(self, store, operation_id: str, limits, *, notice=None):
        self.store, self.operation_id, self.limits, self.notice = store, operation_id, limits, notice
        self._started = time.monotonic()
        self._last_touch = 0.0
        self._noticed = False

    def touch(self) -> None:
        with self.store.transaction():
            self.store.conn.execute("UPDATE operations SET updated_at=? WHERE id=? AND state='queued'",
                                    (timestamp(), self.operation_id))

    def withdraw(self, code: str = 'withdrawn') -> bool:
        """Withdraw the operation unless the owner already started it."""
        with self.store.transaction():
            return bool(self.store.conn.execute("""UPDATE operations SET state='failed',result=?,updated_at=?
                WHERE id=? AND state='queued'""", (json_text({'code': code}), timestamp(), self.operation_id)).rowcount)

    def ahead(self) -> int:
        row = self.store.conn.execute('SELECT created_at FROM operations WHERE id=?', (self.operation_id,)).fetchone()
        queued = self.store.conn.execute("""SELECT count(*) FROM operations WHERE state='queued'
            AND (created_at<? OR (created_at=? AND id<?))""", (row[0], row[0], self.operation_id)).fetchone()[0]
        running = self.store.conn.execute("SELECT count(*) FROM operations WHERE state='running'").fetchone()[0]
        return queued + running

    def _step(self):
        """Advance one poll; return the finished operation or None."""
        operation = self.store.operation(LOCAL_CLIENT, self.operation_id)
        state = operation['state']
        if state not in ('queued', 'running'):
            return operation
        if not owner_alive(self.store, self.limits):
            if state == 'queued' and self.withdraw('runtime_unavailable'):
                raise ProtocolError('runtime_unavailable')
            if state == 'running':
                # The owner died mid-command; its successor records it as interrupted.
                raise ProtocolError('runtime_unavailable')
            return None
        if state == 'queued':
            now = time.monotonic()
            if now - self._last_touch >= WAITER_HEARTBEAT_SECONDS:
                self.touch()
                self._last_touch = now
            if self.notice is not None and not self._noticed and now - self._started >= 2:
                self._noticed = True
                self.notice(f'Waiting for the magsync daemon to start this command ({self.ahead()} ahead)…')
        return None

    def _stopped_waiting(self):
        if self.withdraw('withdrawn'):
            return None
        operation = self.store.operation(LOCAL_CLIENT, self.operation_id)
        if operation['state'] == 'running':
            raise StillRunning(STILL_RUNNING_MESSAGE)
        return operation

    def wait(self) -> dict:
        """Block until the operation finishes (terminal path)."""
        with _termination_signals():
            try:
                while True:
                    try:
                        finished = self._step()
                    except sqlite3.OperationalError:
                        finished = None  # A briefly locked database: poll again.
                    if finished is not None:
                        return finished
                    time.sleep(_POLL_SECONDS)
            except (KeyboardInterrupt, _WaitInterrupted):
                operation = self._stopped_waiting()
                if operation is None:
                    raise KeyboardInterrupt from None
                return operation

    async def wait_async(self) -> dict:
        """Await the operation (TUI path); cancellation withdraws it when possible."""
        try:
            while True:
                try:
                    finished = self._step()
                except sqlite3.OperationalError:
                    finished = None
                if finished is not None:
                    return finished
                await asyncio.sleep(_POLL_SECONDS)
        except asyncio.CancelledError:
            try:
                self.withdraw('withdrawn')
            except sqlite3.Error:
                pass
            raise


def _start_own(store, operation_id: str) -> None:
    with store.transaction():
        store.conn.execute("UPDATE operations SET state='running',updated_at=? WHERE id=? AND state='queued'",
                           (timestamp(), operation_id))


def _finish_own(store, operation_id: str, state: str, result: dict) -> None:
    try:
        with store.transaction():
            store.conn.execute('UPDATE operations SET state=?,result=?,updated_at=? WHERE id=?',
                               (state, json_text(result), timestamp(), operation_id))
    except sqlite3.Error:
        pass  # The next owner records a still-running local operation as interrupted.


def _default_render(kind: str, operation: dict, body: dict, store) -> int:
    import json

    import typer
    typer.echo(json.dumps(operation['result'], indent=2))
    return 1 if operation['state'] in ('failed', 'blocked', 'suspended', 'partial') else 0


def coordinated(kind: str, *, read_only=None, render=None, preview=None):
    """Coordinate one terminal command with the store's owner.

    ``read_only(arguments)`` marks invocations that only display state; they
    run directly, with no ownership and no operation. ``preview(arguments)``
    renders a dry run from a private snapshot; other dry runs run the command
    body against a snapshot copy. ``render(kind, operation, arguments, store)``
    prints the result of a command the live owner executed and returns its
    exit code.
    """
    def decorate(function):
        signature = inspect.signature(function)

        @functools.wraps(function)
        def wrapped(*args, **kwargs):
            import typer
            from magsync.config import load_config
            from magsync.core.index import MagazineIndex

            from .cli import export_root, service_limits
            body = dict(signature.bind(*args, **kwargs).arguments)
            if owner_context.get() is not None or (read_only is not None and read_only(body)):
                return function(*args, **kwargs)
            if body.get('dry_run'):
                if preview is not None:
                    return preview(body)
                with read_only_snapshot():
                    return function(*args, **kwargs)
            cfg = load_config()
            if body.get('output'):
                cfg.output_dir = body['output']
            limits = service_limits()
            index = MagazineIndex()
            store = Store(index, limits)
            owner = Ownership(store, Path(cfg.output_dir), export_root(index))
            notice = functools.partial(typer.echo, err=True)
            owned = False
            try:
                owned = claim_ownership(owner, store, limits, notice=notice)
                if not owned:
                    runtime = store.conn.execute('SELECT output_root FROM runtime_state WHERE id=1').fetchone()
                    if body.get('output') and str(Path(body['output']).expanduser().resolve()) != runtime['output_root']:
                        raise ProtocolError('configuration_managed')
                    operation_id = accept_local(store, kind, body)
                    operation = LocalWaiter(store, operation_id, limits, notice=notice).wait()
                    code = (render or _default_render)(kind, operation, body, store)
                    if code:
                        raise typer.Exit(code)
                    return None
                store.migrate_local(cfg.subscriptions)
                operation_id = accept_local(store, kind, body)
                _start_own(store, operation_id)
                token = owner_context.set(owner)
                operation_token = operation_context.set(operation_id)
                try:
                    result = function(*args, **kwargs)
                except typer.Exit as exc:
                    _finish_own(store, operation_id, 'failed' if exc.exit_code else 'succeeded',
                                {'code': 'failed'} if exc.exit_code else {})
                    raise
                except BaseException as exc:
                    # A temporary owner's own operation is never re-queued:
                    # nothing else would run it, and a later owner must not.
                    _finish_own(store, operation_id, 'failed',
                                {'code': 'interrupted' if isinstance(exc, KeyboardInterrupt) else 'internal_error'})
                    raise
                else:
                    _finish_own(store, operation_id, 'succeeded', {})
                    return result
                finally:
                    operation_context.reset(operation_token)
                    owner_context.reset(token)
            except (CoordinatorBusy, StillRunning) as exc:
                typer.echo(str(exc), err=True)
                raise typer.Exit(1) from None
            except ProtocolError as exc:
                typer.echo(exc.message, err=True)
                raise typer.Exit(1) from None
            finally:
                if owned:
                    owner.release()
                index.close()
        return wrapped
    return decorate


async def _report_progress(store, operation_id: str, callback) -> None:
    """Report each selected issue's physical status as it changes."""
    seen: dict[int, tuple] = {}
    while True:
        try:
            rows = store.conn.execute("""SELECT i.id,i.title,d.status,d.last_error_kind FROM operation_requests o
                JOIN acquisition_requests r ON r.id=o.request_id JOIN issues i ON i.id=r.issue_id
                JOIN downloads d ON d.issue_id=i.id WHERE o.operation_id=?""", (operation_id,)).fetchall()
        except sqlite3.OperationalError:
            rows = []
        for row in rows:
            key = (row['status'], row['last_error_kind'])
            if seen.get(row['id']) != key:
                seen[row['id']] = key
                callback({'issue_id': row['id'], 'title': row['title'], 'status': row['status'],
                          'failure_kind': row['last_error_kind']})
        await asyncio.sleep(0.5)


async def submit_local(kind: str, body: dict, config, db_path: Path, *, progress=None, status=None) -> dict:
    """TUI/embedded local client: run as a temporary owner or submit to the live owner.

    ``progress(update)`` receives each selected issue's status changes while
    the operation runs; ``status(message)`` receives waiting notices.
    """
    from magsync.core.index import MagazineIndex

    from .cli import service_limits
    from .runtime import Runtime
    limits = service_limits()
    index = MagazineIndex(db_path)
    store = Store(index, limits)
    runtime = Runtime(index, config, limits=limits, require_initialized=False, accept_commands=False)
    owned = False
    try:
        deadline = time.monotonic() + limits.lock_wait_seconds
        noticed = False
        while True:
            try:
                await runtime.start(background=False)
                owned = True
                break
            except ProtocolError as exc:
                if exc.code != 'runtime_unavailable':
                    raise
            if live_owner(store, limits):
                break
            if time.monotonic() >= deadline:
                raise _busy(store)
            if status is not None and not noticed:
                status('Waiting for another magsync command to finish…')
                noticed = True
            await asyncio.sleep(0.5)
        operation_id = accept_local(store, kind, body)
        reporter = asyncio.create_task(_report_progress(store, operation_id, progress)) if progress else None
        try:
            if owned:
                row = store.conn.execute('SELECT * FROM operations WHERE id=?', (operation_id,)).fetchone()
                await runtime.execute(dict(row))
                return store.operation(LOCAL_CLIENT, operation_id)
            return await LocalWaiter(store, operation_id, limits, notice=status).wait_async()
        finally:
            if reporter is not None:
                reporter.cancel()
                await asyncio.gather(reporter, return_exceptions=True)
    finally:
        if owned:
            await runtime.stop()
        index.close()
