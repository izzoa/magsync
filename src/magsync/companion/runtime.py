"""One same-host owner executes durable commands and shared acquisition work.

A long-running runtime (daemon or service) runs three tasks: a heartbeat, a
work loop (periodic discovery plus automatic acquisition) and a command loop
that starts queued operations as their own tasks. Commands therefore never
wait behind a discovery pass or a batch of downloads. ``tick()`` remains the
composite unit of work used by tests and temporary runtimes.

All tasks share one SQLite connection on one event loop; that is safe because
no transaction is ever held across an ``await``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from magsync.config import ConfigurationConflict, get_config_path, load_config
from magsync.core import batch as batch_module
from magsync.core import downloader as downloader_module
from magsync.core import notify as notify_module
from magsync.core.diagnostics import sanitize_external_error
from magsync.core.index import _utc_timestamp
from magsync.core.matching import canonicalize_for_match
from magsync.core.models import CycleReport, PipelineStatus, SourceFailureKind
from magsync.core.orchestration import (
    _filter_results,
    _index_results,
    _log_cycle_report,
    _park_link_dispositions,
    _reconcile_download_results,
    _resolve_links_for_indexing,
    _source_failure_reason,
)
from magsync.core.scraper import FreemagazinesClient
from magsync.core.urls import URLValidationError, is_valid_download_url, normalize_download_url

from . import healthcheck
from .exports import Exports
from .journal import Journal
from .ownership import Ownership
from .protocol import OperationState, ProtocolError
from .store import LOCAL_CLIENT, Store, json_text, timestamp, uid

logger = logging.getLogger('magsync')

# Acquisition operations that only claim and transfer; a capacity pause keeps
# them queued for a long-running owner to resume.
_SINGLE_PURPOSE = frozenset({'request.create', 'request.retry', 'local.download', 'local.retry'})
_OPERATION_STATES = frozenset(state.value for state in OperationState)
_CONSECUTIVE_FAILURES_BEFORE_UNAVAILABLE = 3


class FencedIndex:
    """A batch may mutate only attempts claimed by its still-live owner."""
    def __init__(self, runtime, attempts: dict[int, str]):
        self.runtime, self.index, self.attempts = runtime, runtime.store.index, attempts

    def check(self, issue_id: int):
        self.runtime.owner.check()
        row = self.index.conn.execute("SELECT * FROM acquisition_attempts WHERE id=? AND issue_id=? AND state='running'",
                                      (self.attempts.get(issue_id), issue_id)).fetchone()
        if not row or row['owner_id'] != self.runtime.owner.owner_id or row['generation'] != self.runtime.owner.generation:
            raise ProtocolError('runtime_unavailable')

    def __getattr__(self, name):
        method = getattr(self.index, name)
        if name in {'update_download_status', 'record_download_result', 'record_download_failure',
                    'rotate_limewire_url', 'resolve_link_refresh', 'schedule_link_refresh', 'clear_link_refresh'}:
            def guarded(issue_id, *args, **kwargs):
                self.check(issue_id)
                return method(issue_id, *args, **kwargs)
            return guarded
        return method


def claimable_candidates(conn, now_text: str, *, retry: bool | str = False,
                         selected: list[str] | None = None) -> list:
    """Candidate issues for a claim, filtered in SQL.

    Requires live demand, a stored link, no running attempt and a physical
    state that the claim mode permits: pending (no action) or a due typed
    transient retry; explicit retries take failed/unavailable rows instead
    (``retry=True``) or in addition (``retry='selected'``). Per-request
    eligibility is still checked by the caller.
    """
    args: list = []
    where = ["r.state IN ('queued','acquiring','suspended')",
             "i.limewire_url IS NOT NULL", "i.limewire_url!=''",
             "NOT EXISTS(SELECT 1 FROM acquisition_attempts a WHERE a.issue_id=r.issue_id AND a.state='running')"]
    due = """((d.status='pending' AND d.next_action IS NULL) OR (d.status='failed' AND d.next_action='DOWNLOAD'
             AND d.next_retry_at IS NOT NULL AND datetime(d.next_retry_at)<=datetime(?)
             AND (d.last_error_kind='transient' OR d.last_error_kind IS NULL)))"""
    if retry is True:
        where.append("d.status IN ('failed','unavailable')")
    elif retry == 'selected':
        where.append(f"({due} OR d.status IN ('failed','unavailable'))")
        args.append(now_text)
    else:
        where.append(due)
        args.append(now_text)
    if selected is not None:
        where.append('r.id IN (' + ','.join('?' for _ in selected) + ')')
        args.extend(selected)
    return conn.execute(f"""SELECT DISTINCT r.issue_id, d.status, i.limewire_url FROM acquisition_requests r
        JOIN downloads d ON d.issue_id=r.issue_id JOIN issues i ON i.id=r.issue_id
        WHERE {' AND '.join(where)}
        ORDER BY CASE WHEN d.status='pending' THEN 0 ELSE 1 END, r.issue_id""", args).fetchall()


def due_refresh_candidates(conn, now_text: str, limit: int = 100) -> list:
    """Due source-only refreshes for issues with live demand (filtered before the limit)."""
    return conn.execute("""SELECT d.issue_id FROM downloads d JOIN issues i ON i.id=d.issue_id
        WHERE d.status IN ('unavailable','unsupported') AND d.next_action='REFRESH_LINK'
        AND d.next_retry_at IS NOT NULL AND datetime(d.next_retry_at)<=datetime(?)
        AND i.page_url IS NOT NULL AND i.page_url!=''
        AND EXISTS(SELECT 1 FROM acquisition_requests r WHERE r.issue_id=d.issue_id AND r.state IN ('queued','acquiring'))
        AND NOT EXISTS(SELECT 1 FROM acquisition_attempts a WHERE a.issue_id=d.issue_id AND a.state='running')
        ORDER BY datetime(d.next_retry_at),d.issue_id LIMIT ?""", (now_text, limit)).fetchall()


def preview_claimable(db_path: Path, subscriptions, *, now=None) -> tuple[list[dict], int]:
    """What the next cycle would claim: cached issues plus the due-refresh count.

    Runs the runtime's own reconciliation and claim predicates on a private
    copy of the store, so the live store is never touched and the preview
    includes every kind of demand the runtime would act on.
    """
    from .local import read_only_snapshot

    now_text = _utc_timestamp(now)
    with read_only_snapshot(db_path) as index:
        store = Store(index)
        store.migrate_local(list(subscriptions))
        store.materialize(local=False, skipped=[])
        store.reconcile_requests()
        issue_ids = []
        for candidate in claimable_candidates(store.conn, now_text):
            if (candidate['issue_id'] not in issue_ids and store.issue_wanted(candidate['issue_id'])
                    and is_valid_download_url(candidate['limewire_url'] or '')):
                issue_ids.append(candidate['issue_id'])
        refreshes = sum(1 for row in due_refresh_candidates(store.conn, now_text, limit=-1)
                        if store.issue_wanted(row[0]))
        return index.get_issues_by_ids(issue_ids), refreshes


class _SearchGroup:
    """Subscriptions sharing one canonical query are searched once per cycle."""

    def __init__(self, query: str):
        self.query = query
        self.all_exact = True
        self.has_local = False
        self.local_all_exact = True

    def add(self, *, exact: bool, local: bool) -> None:
        self.all_exact = self.all_exact and exact
        if local:
            self.has_local = True
            self.local_all_exact = self.local_all_exact and exact

    @property
    def matcher(self):
        # The most permissive member gates link resolution and link-less counts.
        return SimpleNamespace(query=self.query, exact=self.all_exact, since=None)

    @property
    def local_matcher(self):
        # Provenance records only local intent; remote demand lives in requests.
        if not self.has_local:
            return None
        return SimpleNamespace(query=self.query, exact=self.local_all_exact, since=None)


class Runtime:
    def __init__(self, index, config, *, limits=None, exports: Path | None = None,
                 source_factory=FreemagazinesClient, batch=None, views=None, trusted_mounts=False,
                 scan_seconds: float = 6 * 3600, require_initialized: bool = True,
                 accept_commands: bool | None = None, notify: bool = False,
                 logger: logging.Logger | None = None, clock=time.monotonic, utcnow=None,
                 subscription_loader=None):
        self.store = Store(index, limits)
        self.config = config
        self.owner = Ownership(self.store, Path(config.output_dir), exports or Path(os.environ.get('MAGSYNC_EXPORT_DIR', str(index.db_path.parent/'exports'))))
        self.source_factory = source_factory
        self.batch = batch
        self.views, self.trusted_mounts = views, trusted_mounts
        self.scan_seconds = scan_seconds
        self.require_initialized = require_initialized
        self.accept_commands = accept_commands
        self.notify = notify
        self.logger = logger or logging.getLogger('magsync')
        self.clock = clock
        self.utcnow = utcnow or (lambda: datetime.now(timezone.utc))
        self._custom_loader = subscription_loader
        self._config_existed = get_config_path().is_file()
        self.source = None
        self.exports = None
        self.journal = None
        self.coordinator = None
        self.stopping = False
        self.ready = False
        self.fatal = False
        self._accepting = False
        self._stop_event = asyncio.Event()
        self.wakeup = asyncio.Event()
        self.loop_task = None
        self.heartbeat_task = None
        self.command_task = None
        self.work_task = None
        self.last_heartbeat = 0.0
        self.pipeline = 'healthy'
        self._download_blocked = False
        self._export_blocked = False
        self._last_claim_blocked = False
        self._export_pass_blocked = False
        self._next_scan = 0.0
        self._scanned = False
        self._local_snapshot = list(config.subscriptions)
        self._pending_notifications: list[dict] = []
        self._notify_tasks: set[asyncio.Task] = set()
        self._last_notification = self.clock()
        self._consecutive_failures = 0
        self._logged: dict = {}
        self._inflight_operations: set[str] = set()
        self._retired_sources: list = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self, *, background: bool = True, accept_commands: bool | None = None):
        """Acquire ownership and prepare shared state.

        Only a runtime that drains the durable queue (a background runtime, or
        an API server whose loop a harness drives) advertises acceptance.
        """
        if accept_commands is None:
            accept_commands = self.accept_commands if self.accept_commands is not None else background
        if self.require_initialized:
            self.store.check_identity()
        self.owner.acquire(accepting=accept_commands)
        self._accepting = accept_commands
        try:
            if not self.require_initialized:
                self.store.initialize()
            try:
                self._local_snapshot = list(self._load_local_subscriptions())
            except (OSError, ValueError) as exc:
                self.logger.warning('Subscription config could not be read; using the loaded configuration: %s',
                                    sanitize_external_error(exc))
            self.store.migrate_local(self._local_snapshot)
            self.exports = Exports(self.store, self.owner, views=self.views, trusted_mounts=self.trusted_mounts)
            self.journal = Journal(self.store, self.exports)
            await self.exports.recover_async()
            self.source = self.source_factory(scrape_delay=self.config.download.scrape_delay)
            await self.source.__aenter__()
            self.coordinator = batch_module.BatchCoordinator(self.config.download.max_concurrent)
            self.ready = True
            self.last_heartbeat = self.clock()
            self.heartbeat_task = asyncio.create_task(self._heartbeat())
            if background:
                self.loop_task = asyncio.create_task(self.run())
            return self
        except BaseException:
            if self.source is not None:
                try:
                    await self.source.__aexit__(None, None, None)
                except Exception:
                    pass
                self.source = None
            self.owner.release()
            raise

    async def start_when_available(self, *, background: bool = True, accept_commands: bool | None = None,
                                   log_every: float = 30.0, poll: float = 1.0):
        """Start, waiting while a temporary owner (a terminal command) holds the store.

        A live accepting owner is another daemon or service: that is a
        configuration error and raises immediately, as before.
        """
        last_logged = None
        while True:
            try:
                return await self.start(background=background, accept_commands=accept_commands)
            except ProtocolError as exc:
                if exc.code != 'runtime_unavailable' or self._live_accepting_owner():
                    raise
            now = self.clock()
            if last_logged is None or now - last_logged >= log_every:
                self.logger.info('Waiting for another magsync command to finish before starting...')
                last_logged = now
            if await self._wait_stop(poll):
                raise ProtocolError('runtime_unavailable')

    def request_stop(self):
        """Begin a graceful stop (signal handlers); ``stop()`` completes it."""
        self.stopping = True
        self._stop_event.set()
        self.wakeup.set()

    async def stop(self):
        self.stopping, self.ready = True, False
        self._stop_event.set()
        self.wakeup.set()
        try:
            try:
                self.owner.heartbeat(accepting=False)
            except (ProtocolError, sqlite3.Error, OSError):
                pass  # Shutdown must release OS locks even if persistence failed.
            if self.loop_task:
                try:
                    await asyncio.wait_for(asyncio.shield(self.loop_task), self.store.limits.shutdown_seconds)
                except asyncio.TimeoutError:
                    self.loop_task.cancel()
                    await asyncio.gather(self.loop_task, return_exceptions=True)
                except Exception as exc:
                    self._log_once('stop', 'Runtime loop ended with an error: %s', exc, level=logging.ERROR)
            await self._drain_notifications(timeout=30)
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                await asyncio.gather(self.heartbeat_task, return_exceptions=True)
            if self.coordinator:
                await self.coordinator.singleflight.cancel()
            for source in [*self._retired_sources, self.source]:
                if source is not None:
                    try:
                        await source.__aexit__(None, None, None)
                    except Exception:
                        pass
            self._retired_sources = []
        finally:
            self.owner.release()

    # ------------------------------------------------------------------
    # Liveness
    # ------------------------------------------------------------------

    async def _heartbeat(self):
        """Publish liveness every interval; transient errors never stop it.

        Only loss of ownership or of the database file is fatal. A transient
        error (for example a briefly locked database) is retried on the next
        interval; if it persists, liveness lapses on its own threshold.
        """
        while not (self.stopping or self.fatal):
            try:
                if not self.store.index.db_path.exists():
                    raise ProtocolError('runtime_unavailable')
                self.owner.heartbeat(accepting=self._advertise())
                self.last_heartbeat = self.clock()
                self._touch_health_file()
            except ProtocolError:
                self._set_fatal('Runtime lost its ownership or store; stopping')
                return
            except (sqlite3.Error, OSError) as exc:
                self._log_once('heartbeat', 'Runtime heartbeat deferred: %s', exc)
            if await self._wait_stop(self.store.limits.heartbeat_seconds):
                return

    def _touch_health_file(self):
        try:
            healthcheck.HEALTH_CHECK_PATH.touch()
        except OSError as exc:
            self._log_once('health-file', 'Unable to update the health check file: %s', exc)

    def _advertise(self) -> bool:
        """Accept queued commands only while this owner can actually drain them."""
        if not (self.ready and self._accepting) or self.stopping or self.fatal:
            return False
        if self.loop_task is not None and self.loop_task.done():
            return False
        return self._consecutive_failures < _CONSECUTIVE_FAILURES_BEFORE_UNAVAILABLE

    def _live_accepting_owner(self) -> bool:
        row = self.store.conn.execute('SELECT accepting,heartbeat_at FROM runtime_state WHERE id=1').fetchone()
        return bool(row and row['accepting'] and row['heartbeat_at']
                    and row['heartbeat_at'] >= timestamp(-self.store.limits.heartbeat_stale_seconds))

    def assert_ready(self):
        if not self.ready or self.stopping or self.fatal or not self.store.index.db_path.exists():
            raise ProtocolError('runtime_unavailable')
        self.owner.check()
        self.store.check_identity()
        # A write probe detects a read-only/full/unusable store before acceptance.
        with self.store.transaction():
            self.store.conn.execute('UPDATE runtime_state SET accepting=accepting WHERE id=1')

    def live(self) -> bool:
        return self.clock() - self.last_heartbeat < self.store.limits.heartbeat_stale_seconds

    def _set_fatal(self, message: str):
        if not self.fatal:
            self.logger.error(message)
        self.fatal = True
        self.ready = False
        self._stop_event.set()
        self.wakeup.set()

    def _is_fatal(self, exc: ProtocolError) -> bool:
        if exc.code in ('store_uninitialized', 'schema_incompatible'):
            return True
        if exc.code != 'runtime_unavailable':
            return False
        if self.fatal or not self.store.index.db_path.exists():
            return True
        try:
            self.owner.check()
        except ProtocolError:
            return True
        except sqlite3.Error:
            return False
        return False

    def _log_once(self, key, message: str, *args, level: int = logging.WARNING):
        rendered = sanitize_external_error(message % tuple(sanitize_external_error(a) if isinstance(a, BaseException) else a for a in args))
        if self._logged.get(key) != rendered:
            self._logged[key] = rendered
            self.logger.log(level, '%s', rendered)

    async def _wait_stop(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return self._stop_event.is_set()

    async def _wait_stop_or_wakeup(self, timeout: float) -> bool:
        if self._stop_event.is_set():
            return True
        waiters = [asyncio.ensure_future(self._stop_event.wait()), asyncio.ensure_future(self.wakeup.wait())]
        try:
            await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
        self.wakeup.clear()
        return self._stop_event.is_set()

    # ------------------------------------------------------------------
    # Loops
    # ------------------------------------------------------------------

    async def run(self):
        """Supervise the work and command loops; either ending stops the runtime."""
        self.command_task = asyncio.create_task(self._command_loop())
        self.work_task = asyncio.create_task(self._work_loop())
        tasks = {self.command_task, self.work_task}
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if not self.stopping:
                # A loop ended without a stop request: this owner can no longer serve.
                self._set_fatal('Companion runtime stopped unexpectedly')
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _work_loop(self):
        while not (self.stopping or self.fatal):
            delay = self.store.limits.command_poll_seconds
            try:
                if self.clock() >= self._next_scan:
                    await self._scheduled_discovery()
                else:
                    await self.acquire()
                await self.exports.sync_views_async()
                await self._maybe_flush_notifications()
                self._close_retired_sources()
                self._iteration_succeeded()
            except asyncio.CancelledError:
                raise
            except ProtocolError as exc:
                if self._is_fatal(exc):
                    self._set_fatal('Companion runtime lost its store or ownership; stopping')
                    return
                delay = self._iteration_failed('work', exc)
            except Exception as exc:
                delay = self._iteration_failed('work', exc)
            if await self._wait_stop_or_wakeup(delay):
                return

    async def _command_loop(self):
        running: set[asyncio.Task] = set()
        try:
            while not (self.stopping or self.fatal):
                delay = self.store.limits.command_poll_seconds
                try:
                    free = max(0, self.store.limits.command_concurrency - len(running))
                    if free:
                        for operation in self._queued_operations(free):
                            self._inflight_operations.add(operation['id'])
                            task = asyncio.create_task(self._run_operation(operation))
                            running.add(task)
                            task.add_done_callback(running.discard)
                except asyncio.CancelledError:
                    raise
                except (sqlite3.Error, OSError) as exc:
                    self._log_once('commands', 'Command polling deferred: %s', exc)
                if await self._wait_stop_or_wakeup(delay):
                    break
            if running:
                await asyncio.gather(*running, return_exceptions=True)
        finally:
            pending = [task for task in running if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _run_operation(self, operation: dict):
        try:
            await self.execute(operation)
        except asyncio.CancelledError:
            raise
        except ProtocolError as exc:
            if self._is_fatal(exc):
                self._set_fatal('Companion runtime lost its store or ownership; stopping')
            else:
                self._log_once(('operation', operation['kind']), 'Operation %s ended early: %s', operation['kind'], exc.code)
        except Exception as exc:
            self._log_once(('operation', operation['kind']), 'Operation %s ended early: %s', operation['kind'], exc)
        finally:
            self._inflight_operations.discard(operation['id'])

    def _queued_operations(self, limit: int) -> list[dict]:
        rows = self.store.conn.execute("SELECT * FROM operations WHERE state='queued' ORDER BY created_at,id LIMIT ?",
                                       (limit + len(self._inflight_operations),)).fetchall()
        return [dict(row) for row in rows if row['id'] not in self._inflight_operations][:limit]

    def _iteration_succeeded(self):
        self._consecutive_failures = 0

    def _iteration_failed(self, key: str, exc: BaseException) -> float:
        self._consecutive_failures += 1
        self._log_once((key, 'failure'), 'Companion runtime work failed; retrying: %s', exc, level=logging.ERROR)
        # Capped exponential backoff scaled by the poll interval (2s..60s by default).
        return float(min(60 * self.store.limits.command_poll_seconds,
                         self.store.limits.command_poll_seconds * 2 ** min(self._consecutive_failures, 6)))

    async def tick(self, *, scan: bool = True):
        """One composite unit of work: due discovery, queued operations, acquisition."""
        self.assert_ready()
        if scan and self.clock() >= self._next_scan:
            await self._scheduled_discovery()
        for operation in self._queued_operations(50):
            if self.stopping:
                return
            self._inflight_operations.add(operation['id'])
            try:
                await self.execute(operation)
            finally:
                self._inflight_operations.discard(operation['id'])
        if not self.stopping:
            await self.acquire()
            await self.exports.sync_views_async()
            await self._maybe_flush_notifications()

    async def _scheduled_discovery(self):
        self._next_scan = self.clock() + self.scan_seconds
        if self._scanned:
            await self._renew_source()
        self._scanned = True
        await self.discover()

    async def _renew_source(self):
        """Periodic discovery owns circuit reset; command arrivals never do.

        Commands may still be using the previous client, so it is retired and
        closed once no operation is in flight.
        """
        fresh = self.source_factory(scrape_delay=self.config.download.scrape_delay)
        await fresh.__aenter__()
        previous, self.source = self.source, fresh
        if previous is not None:
            self._retired_sources.append(previous)
        self._close_retired_sources()

    def _close_retired_sources(self):
        if self._retired_sources and not self._inflight_operations:
            retired, self._retired_sources = self._retired_sources, []
            for source in retired:
                task = asyncio.create_task(source.__aexit__(None, None, None))
                task.add_done_callback(lambda t: t.exception())

    # ------------------------------------------------------------------
    # Discovery (the daemon cycle)
    # ------------------------------------------------------------------

    def _load_local_subscriptions(self):
        if self._custom_loader is not None:
            return self._custom_loader()
        exists = get_config_path().is_file()
        if self._config_existed and not exists:
            # A vanished file is a failure, not an intent to unsubscribe from everything.
            raise OSError('local configuration disappeared')
        subscriptions = load_config().subscriptions
        self._config_existed = exists
        return subscriptions

    def _search_groups(self, local_subscriptions) -> list[_SearchGroup]:
        groups: dict[str, _SearchGroup] = {}
        order: list[_SearchGroup] = []

        def add(query, exact, local):
            key = canonicalize_for_match(query or '')
            if not key:
                return
            group = groups.get(key)
            if group is None:
                group = groups[key] = _SearchGroup(query)
                order.append(group)
            group.add(exact=bool(exact), local=local)

        for sub in local_subscriptions:
            add(sub.query, sub.exact, True)
        for row in self.store.conn.execute("""SELECT sub.query,sub.exact FROM subscriptions sub
                JOIN scopes s ON s.id=sub.scope_id JOIN clients c ON c.id=s.client_id
                WHERE sub.enabled=1 AND sub.tombstone=0 AND s.enabled=1 AND c.enabled=1 AND s.client_id!=?
                ORDER BY sub.id""", (LOCAL_CLIENT,)).fetchall():
            add(row['query'], row['exact'], False)
        return order

    async def discover(self, *, now=None, subscriptions=None, config_failure_reason: str | None = None) -> CycleReport:
        """Run one discovery cycle: the established daemon cycle over scoped demand.

        Phases: local subscription reconciliation; one search per canonical
        query with circuit/skip accounting, exact filtering, link gating,
        provenance for local intent and parking; demand reconciliation; due
        source-only refreshes; acquisition with counters from returned batch
        results; health classification and persisted pipeline state.
        """
        log = self.logger
        report = CycleReport()
        started = self.clock()
        cycle_at = now or self.utcnow()
        source_expected = False
        source_failed = False
        source_reason: str | None = None
        fatal_reason: str | None = None
        try:
            if subscriptions is None:
                try:
                    subscriptions = list(self._load_local_subscriptions())
                except (OSError, ValueError) as exc:
                    subscriptions = self._local_snapshot
                    config_failure_reason = config_failure_reason or (
                        'Subscription config reload failed; previous snapshot in use')
                    log.warning('%s: %s', config_failure_reason, sanitize_external_error(exc))
            self._local_snapshot = list(subscriptions)
            self.store.reconcile_local(self._local_snapshot)
            # Provenance backfill keeps legacy displays and terminal commands
            # consistent with current local subscriptions (fast, idempotent).
            promoted = self.store.index.promote_subscribed(self._local_snapshot)
            if promoted:
                log.info('Promoted %d cataloged row(s) to subscription provenance', promoted)

            groups = self._search_groups(self._local_snapshot)
            report.source_total = len(groups)
            source_expected = bool(groups)
            for position, group in enumerate(groups):
                if self.source.circuit_open:
                    report.source_skipped += len(groups) - position
                    source_failed = True
                    failure = self.source.circuit_failure
                    if source_reason is None and failure is not None:
                        source_reason = _source_failure_reason(failure)
                    break

                log.info('Searching: %s', group.query)
                report.source_attempted += 1
                source_result = await self.source.search_with_details(group.query)
                report.detail_failures += len(source_result.failures)
                if source_result.failure is not None and source_result.failure.operation == 'detail':
                    # When every advertised detail fails, the scraper promotes
                    # one detail failure to the operation-level failure and
                    # retains the remaining siblings in ``failures``.
                    report.detail_failures += 1
                blocked_result = False

                if source_result.failure is not None:
                    report.source_failed += 1
                    source_failed = True
                    blocked_result = source_result.failure.kind is SourceFailureKind.ACCESS_BLOCKED
                    if source_reason is None:
                        source_reason = _source_failure_reason(source_result.failure)
                    log.warning('Search failed for %s: %s', sanitize_external_error(group.query, 120),
                                _source_failure_reason(source_result.failure))
                else:
                    if source_result.validated_empty:
                        report.source_empty += 1
                    else:
                        report.source_succeeded += 1
                    filtered = _filter_results(source_result.items, group.query, group.all_exact)
                    new = 0
                    if filtered:
                        # Masked links are resolved before indexing, and only
                        # for issues that actually need one.
                        resolution = await _resolve_links_for_indexing(
                            filtered, self.store.index, self.source, subscription=group.matcher)
                        if resolution.failures:
                            report.link_resolution_failures += len(resolution.failures)
                            source_failed = True
                            log.warning('%s: %d issue(s) advertised a download whose link could not be resolved',
                                        sanitize_external_error(group.query, 120), len(resolution.failures))
                            if source_reason is None:
                                source_reason = f'{len(resolution.failures)} download link(s) could not be resolved'
                        outcome = _index_results(resolution.items, self.store.index, self.config,
                                                 subscription=group.matcher, provenance=group.local_matcher)
                        new = outcome.added
                        report.issues_linkless += outcome.linkless
                        # A link that resolved but is unusable is an expected
                        # outcome, not a fault: park it on a schedule so it
                        # stops costing a request every cycle.
                        unsupported, dead = _park_link_dispositions(resolution, self.store.index)
                        report.issues_unsupported_host += unsupported
                        report.issues_dead_link += dead
                        if unsupported:
                            hosts = sorted({host for _issue, host in resolution.unsupported_host})
                            log.info('  %s: %d issue(s) on unsupported host(s) %s - parked for later re-probe',
                                     sanitize_external_error(group.query, 120), unsupported, ', '.join(hosts))
                        if dead:
                            log.info('  %s: %d issue(s) with no available download link - parked for later re-probe',
                                     sanitize_external_error(group.query, 120), dead)
                        if outcome.linkless:
                            log.warning('  %s: %d issue(s) indexed with no usable download link',
                                        sanitize_external_error(group.query, 120), outcome.linkless)
                    if new:
                        log.info('  %s: %d new issues indexed', group.query, new)
                    if source_result.failures:
                        source_failed = True
                        if source_reason is None:
                            source_reason = f'{len(source_result.failures)} source detail request(s) failed'

                if blocked_result:
                    report.source_skipped += len(groups) - position - 1
                    break
                # A detail request can open the circuit while still preserving
                # valid siblings, so check again after the structured result.
                if self.source.circuit_open:
                    report.source_skipped += len(groups) - position - 1
                    source_failed = True
                    failure = self.source.circuit_failure
                    if source_reason is None and failure is not None:
                        source_reason = _source_failure_reason(failure)
                    break

            self._reconcile_demand_after_discovery()

            # Source-only refresh actions run before downloads and never touch
            # the known-dead stored link. An open circuit short-circuits them.
            refresh_results = await self.refresh_due(now=cycle_at)
            if refresh_results:
                source_expected = True
            for refresh_result in refresh_results:
                failure = getattr(refresh_result.get('outcome'), 'failure', None)
                if failure is not None:
                    source_failed = True
                    if source_reason is None:
                        source_reason = _source_failure_reason(failure)
                if refresh_result.get('failure_kind') is not None:
                    source_failed = True
                    if source_reason is None:
                        source_reason = 'Unable to persist source refresh result'

            acquired = await self.acquire(now=cycle_at, report=report, include_errored=True)
            if acquired.get('missing_results'):
                source_reason = source_reason or f"Batch omitted {acquired['missing_results']} claimed result(s)"

            # An immediate dead-link refresh inside the batch may be the first
            # operation to encounter a host-wide source challenge.
            if self.source.circuit_open:
                source_expected = True
                source_failed = True
                failure = self.source.circuit_failure
                if source_reason is None and failure is not None:
                    source_reason = _source_failure_reason(failure)

            report.pending_refreshes = self._pending_refresh_count()
        except asyncio.CancelledError:
            raise
        except ProtocolError as exc:
            if self._is_fatal(exc):
                raise
            fatal_reason = exc.message
        except Exception as exc:
            fatal_reason = sanitize_external_error(exc)

        if fatal_reason is not None:
            report.status = PipelineStatus.FAILED
            report.reason = fatal_reason or 'Local daemon cycle failure'
        elif source_failed or report.detail_failures or report.downloads_failed or config_failure_reason is not None:
            report.status = PipelineStatus.DEGRADED
            report.reason = source_reason
            if report.reason is None and report.link_resolution_failures:
                report.reason = f'{report.link_resolution_failures} download link(s) could not be resolved'
            if report.reason is None and report.detail_failures:
                report.reason = f'{report.detail_failures} detail request(s) failed'
            if report.reason is None and report.downloads_failed:
                report.reason = f'{report.downloads_failed} download(s) failed'
            if report.reason is None and config_failure_reason is not None:
                report.reason = config_failure_reason
        elif report.issues_linkless and report.downloads_queued == 0 and report.pending_refreshes == 0:
            # Backstop: issues a subscription wanted were indexed, nothing was
            # queued, and no pending action explains it. Not healthy, even
            # when every individual phase reported success.
            report.status = PipelineStatus.DEGRADED
            report.reason = (f'{report.issues_linkless} wanted issue(s) indexed with no usable '
                             'download link and no download work queued')
        else:
            report.status = PipelineStatus.HEALTHY

        report.reason = sanitize_external_error(report.reason) if report.reason else None
        report.elapsed_seconds = max(0.0, self.clock() - started)
        source_validated = None if not source_expected else (not source_failed and report.detail_failures == 0)
        try:
            self.store.index.update_pipeline_state(report.status, cycle_at=cycle_at, source_validated=source_validated,
                                                   source_check_at=cycle_at, degraded_reason=report.reason)
        except Exception as exc:
            report.status = PipelineStatus.FAILED
            report.reason = sanitize_external_error(exc) or 'Unable to persist pipeline state'
            log.error('Unable to persist pipeline state: %s', report.reason)
        self.pipeline = report.status.value
        _log_cycle_report(report, log)
        self._flush_notifications()
        return report

    def _reconcile_demand_after_discovery(self):
        self.store.materialize(local=True)
        skipped: list[str] = []
        self.store.materialize(local=False, skipped=skipped)
        for subscription_id in skipped:
            self._log_once(('capacity-subscription', subscription_id),
                           'Remote subscription %s skipped: its client reached the pending-request limit',
                           subscription_id)
        self.store.reconcile_requests()
        self.journal.cleanup()
        self.exports.cleanup()

    def _pending_refresh_count(self) -> int:
        return self.store.conn.execute("""SELECT count(DISTINCT d.issue_id) FROM downloads d
            JOIN acquisition_requests r ON r.issue_id=d.issue_id
            WHERE d.next_action='REFRESH_LINK' AND r.state IN ('queued','acquiring')""").fetchone()[0]

    async def refresh_due(self, *, now=None) -> list[dict]:
        """Refresh due source-only links for wanted issues through fenced attempts."""
        now_text = _utc_timestamp(now or self.utcnow())
        attempts: dict[int, str] = {}
        with self.store.transaction():
            self.owner.check()
            # Wanted-ness is filtered before the limit so stranger rows cannot starve it.
            for row in due_refresh_candidates(self.store.conn, now_text):
                if not self.store.issue_wanted(row[0]):
                    continue
                attempt = uid()
                self.store.conn.execute("INSERT INTO acquisition_attempts VALUES(?,?,?,?,'running',?)",
                                        (attempt, row[0], self.owner.owner_id, self.owner.generation, timestamp()))
                attempts[row[0]] = attempt
        if not attempts:
            return []
        try:
            results = await batch_module.refresh_due_links(
                self.store.index.get_issues_by_ids(list(attempts)), FencedIndex(self, attempts), self.source)
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._finish_attempts(attempts)
            raise
        self._finish_attempts(attempts)
        return results

    def _finish_attempts(self, attempts: dict[int, str]):
        with self.store.transaction():
            self.owner.check()
            self.store.conn.executemany("UPDATE acquisition_attempts SET state='finished' WHERE id=? AND state='running'",
                                        [(attempt,) for attempt in attempts.values()])

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    async def search(self, body: dict, *, subscription=None, detail: bool = False) -> dict:
        """Search the source, index results, and return public catalog items.

        API searches carry a bounded ``pages``; local terminal searches omit it
        and search as deep as the standalone command. ``detail`` adds safe
        failure fields that only local renderers receive.
        """
        if self.source.circuit_open:
            self.pipeline = 'degraded'
            result = {'outcome': 'blocked', 'items': [], 'failure_kind': 'access_blocked'}
            if detail:
                result['failure'] = self._safe_failure(self.source.circuit_failure)
            return result
        pages = body.get('pages')
        if pages:
            result = await self.source.search_with_details(body['query'], max_pages=pages)
        else:
            result = await self.source.search_with_details(body['query'])
        if result.failure:
            self.pipeline = 'degraded'
            outcome = {'outcome': 'blocked' if result.failure.kind.value == 'access_blocked' else 'failed',
                       'items': [], 'failure_kind': result.failure.kind.value}
            if detail:
                outcome['failure'] = self._safe_failure(result.failure)
            return outcome
        resolution = await _resolve_links_for_indexing(result.items, self.store.index, self.source, subscription=subscription)
        indexed = _index_results(resolution.items, self.store.index, self.config)
        unsupported, dead = _park_link_dispositions(resolution, self.store.index)
        ids = self.store.index.issue_ids_for_page_urls([item.page_url for item in result.items])
        items = [self.store.issue(self.store.provider_issue(id)) for id in ids.values()]
        outcome = 'partial' if result.failures or resolution.failures else 'empty' if result.validated_empty else 'succeeded'
        if not items and not result.validated_empty:
            outcome = 'failed'
        if outcome in ('partial', 'failed'):
            self.pipeline = 'degraded'
        return {'outcome': outcome, 'items': items[:self.store.limits.page_max],
                'truncated': len(items) > self.store.limits.page_max, 'added': indexed.added,
                'detail_failures': len(result.failures), 'link_resolution_failures': len(resolution.failures),
                'unsupported_host': unsupported, 'dead_link': dead, 'linkless': indexed.linkless}

    @staticmethod
    def _safe_failure(failure) -> dict | None:
        if failure is None:
            return None
        return {'kind': failure.kind.value, 'message': sanitize_external_error(failure.message or ''),
                'status_code': failure.status_code, 'host': failure.host, 'cf_ray': failure.cf_ray}

    async def execute(self, operation: dict):
        """Execute one queued operation; any failure ends only this operation."""
        operation_id = operation['id']
        client_id = operation['client_id']
        kind = operation['kind']
        local = client_id == LOCAL_CLIENT
        try:
            self.store.client(client_id)
            if operation['scope_id']:
                self.store.scope(client_id, operation['scope_id'], enabled=True)
        except ProtocolError:
            self._finish_if_queued(operation_id, 'suspended', {'code': 'scope_disabled'})
            return
        with self.store.transaction():
            self.owner.check()
            current = self.store.conn.execute('SELECT state,updated_at,result FROM operations WHERE id=?',
                                              (operation_id,)).fetchone()
            if current is None or current['state'] != 'queued':
                return
            if local and current['updated_at'] < timestamp(-self.store.limits.heartbeat_stale_seconds):
                # Its terminal stopped waiting without withdrawing (killed):
                # nobody observes the result, so it must not run.
                self.store.conn.execute("""UPDATE operations SET state='failed',result=?,updated_at=?
                    WHERE id=? AND state='queued'""", (json_text({'code': 'abandoned'}), timestamp(), operation_id))
                return
            started = self.store.conn.execute("""UPDATE operations SET state='running',updated_at=?
                WHERE id=? AND state='queued'""", (timestamp(), operation_id)).rowcount
            if not started:
                return
            try:
                accepted = json.loads(current['result'] or '{}')
            except ValueError:
                accepted = {}
        try:
            state, result = await self._perform(kind, json.loads(operation['body']), operation_id, client_id, accepted)
        except asyncio.CancelledError:
            raise
        except ProtocolError as exc:
            if self._is_fatal(exc):
                raise
            state, result = 'failed', {'code': exc.code}
        except ConfigurationConflict as exc:
            state, result = 'failed', {'code': 'configuration_managed', **({'message': str(exc)} if local else {})}
        except Exception as exc:
            self._log_once(('operation', kind), 'Operation %s failed: %s', kind, exc, level=logging.ERROR)
            # Remote results use only protocol error codes.
            state, result = 'failed', {'code': 'internal_error' if local else 'runtime_unavailable'}
        self.finish_operation(operation_id, state, result)

    async def _perform(self, kind: str, body: dict, operation_id: str, client_id: str, accepted: dict) -> tuple[str, dict]:
        if kind == 'search':
            result = await self.search(body)
            return result['outcome'], result
        if kind in _SINGLE_PURPOSE:
            selected = [row[0] for row in self.store.conn.execute(
                'SELECT request_id FROM operation_requests WHERE operation_id=?', (operation_id,))]
            if accepted.get('attached'):
                result = {'physical_attempts': 0, 'attached': True,
                          'outcomes': [self.store.request(client_id, id) for id in selected]}
            else:
                result = await self.acquire(selected, retry='selected' if kind == 'local.download' else kind.endswith('retry'))
            if kind == 'local.retry':
                result.update({key: value for key, value in accepted.items()
                               if key in ('skipped', 'excluded')})
            if result.get('outcome') == 'capacity_exhausted':
                if self._accepting and client_id != LOCAL_CLIENT:
                    # A long-running owner resumes a remote command when
                    # capacity returns; a waiting terminal is told instead.
                    return 'queued', {**{k: v for k, v in accepted.items() if k.startswith('_') or k in ('skipped', 'excluded')},
                                      'code': 'capacity_exhausted'}
                return 'failed', {'code': 'capacity_exhausted'}
            return 'succeeded', {'requests': selected, **result}
        if kind.startswith('local.'):
            from .commands import execute_local
            result = await execute_local(self, kind.removeprefix('local.'), body, operation_id)
            outcome = result.get('outcome', 'succeeded')
            state = 'succeeded' if outcome == 'unchanged' else outcome
            return (state if state in _OPERATION_STATES else 'succeeded'), result
        return 'failed', {'code': 'incompatible_protocol'}

    def finish_operation(self, operation_id: str, state: str, result: dict):
        with self.store.transaction():
            self.owner.check()
            self.store.conn.execute('UPDATE operations SET state=?,result=?,updated_at=? WHERE id=?',
                                    (state, json_text(result), timestamp(), operation_id))

    def _finish_if_queued(self, operation_id: str, state: str, result: dict):
        with self.store.transaction():
            self.owner.check()
            self.store.conn.execute("UPDATE operations SET state=?,result=?,updated_at=? WHERE id=? AND state='queued'",
                                    (state, json_text(result), timestamp(), operation_id))

    # ------------------------------------------------------------------
    # Acquisition
    # ------------------------------------------------------------------

    def claim(self, selected: list[str] | None = None, *, retry: bool | str = False, now: str | None = None
              ) -> tuple[list[dict], dict[int, str]]:
        """Atomically claim claimable, eligible issues.

        Candidates are selected in SQL (request state, claimable physical
        state/due time, a link, no running attempt), so parked requests cost
        nothing per poll. Capacity is decided per issue: a candidate that
        cannot be admitted is skipped and reported, never raised, so it can
        never block other claims; ``_last_claim_blocked`` reports whether any
        candidate was held back. Returns ``(issues, attempts)``.
        """
        conn = self.store.conn
        now_text = now or _utc_timestamp(self.utcnow())
        attempts: dict[int, str] = {}
        blocked = False
        with self.store.transaction():
            self.owner.check()
            selected_issues = None
            if selected is not None:
                if not selected:
                    self._last_claim_blocked = False
                    return [], {}
                selected_issues = {conn.execute('SELECT issue_id FROM acquisition_requests WHERE id=?', (id,)).fetchone()[0]
                                   for id in selected if self.store.eligible(id)}
            candidates = claimable_candidates(conn, now_text, retry=retry, selected=selected)
            limit = len(selected) if selected is not None else 100
            for candidate in candidates:
                if len(attempts) >= limit:
                    break
                issue_id = candidate['issue_id']
                if selected_issues is not None and issue_id not in selected_issues:
                    continue
                if not self.store.issue_wanted(issue_id) or not is_valid_download_url(candidate['limewire_url'] or ''):
                    continue
                try:
                    self.exports.require_download_capacity(min(len(attempts) + 1, self.config.download.max_concurrent),
                                                           exporting=self.store.remote_wanted(issue_id))
                except ProtocolError as exc:
                    if exc.code != 'capacity_exhausted':
                        raise
                    blocked = True
                    continue
                # Locked transaction rechecks status and exact link.
                changed = conn.execute("""UPDATE downloads SET status='downloading',next_action=NULL,next_retry_at=NULL
                    WHERE issue_id=? AND status=? AND EXISTS(SELECT 1 FROM issues WHERE id=? AND limewire_url=?)""",
                    (issue_id, candidate['status'], issue_id, candidate['limewire_url'])).rowcount
                if not changed:
                    continue
                if retry:
                    conn.execute('UPDATE downloads SET last_error_kind=NULL,last_error=NULL,attempt_count=0,last_attempt_at=NULL WHERE issue_id=?', (issue_id,))
                attempt_id = uid()
                conn.execute("INSERT INTO acquisition_attempts VALUES(?,?,?,?,'running',?)",
                             (attempt_id, issue_id, self.owner.owner_id, self.owner.generation, timestamp()))
                attempts[issue_id] = attempt_id
                for req in conn.execute("SELECT r.id,s.client_id FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id WHERE r.issue_id=? AND r.state!='canceled'", (issue_id,)).fetchall():
                    if self.store.eligible(req['id']):
                        conn.execute("UPDATE acquisition_requests SET state='acquiring',revision=revision+1 WHERE id=?", (req['id'],))
                        self.store.event(req['client_id'], 'request.updated', req['id'],
                                         lambda req=req: self.store.request(req['client_id'], req['id']))
            claimed = self.store.index.get_issues_by_ids(list(attempts))
        self._last_claim_blocked = blocked
        self._set_capacity(download=blocked and not attempts)
        return claimed, attempts

    async def acquire(self, selected: list[str] | None = None, *, retry: bool | str = False, now=None,
                      report: CycleReport | None = None, include_errored: bool = False) -> dict:
        """Fulfill retained bytes, then claim and transfer eligible work.

        Every claimed attempt is settled even when a later step fails, so an
        issue can never be left with a running attempt that blocks future
        claims. Only cancellation (shutdown past its grace) leaves attempts
        running, for the next owner's recovery.
        """
        now_text = _utc_timestamp(now or self.utcnow())
        self._export_pass_blocked = False
        # Fulfill retained bytes first; a late request does not need upstream work.
        await self._publish_retained(selected, include_errored=include_errored)
        self._set_capacity(export=self._export_pass_blocked)
        claimed, attempts = self.claim(selected, retry=retry, now=now_text)
        blocked = self._last_claim_blocked
        identities = self._identities(claimed)
        if report is not None:
            report.downloads_queued = len(claimed)
            report.downloads_unique = len(identities)
        if not claimed:
            return {'physical_attempts': 0, 'outcomes': [], 'outcome': 'capacity_exhausted' if blocked else 'succeeded'}
        self.logger.info('Downloading %d issues (%d unique URLs; max %d concurrent)...',
                         len(claimed), len(identities), self.config.download.max_concurrent)
        fenced = FencedIndex(self, attempts)
        batch = self.batch or batch_module.download_batch
        kwargs = {'source_client': self.source, 'on_start': self._log_download_start}
        if self.batch is None:
            kwargs['coordinator'] = self.coordinator
        exporting = {self._identity(issue) for issue in claimed if self.store.remote_wanted(issue['id'])}
        concurrency = min(len(claimed), self.config.download.max_concurrent)
        started_transfers = 0

        def admit(url: str | None = None):
            nonlocal started_transfers
            needs_export = bool(exporting) if url is None else self._identity({'limewire_url': url}) in exporting
            try:
                self.exports.require_download_capacity(concurrency, exporting=needs_export)
            except ProtocolError as exc:
                if exc.code == 'capacity_exhausted':
                    raise batch_module.DownloadCapacityPaused from None
                raise
            started_transfers += 1

        admission_token = batch_module.download_admission.set(admit)
        byte_limit_token = downloader_module.download_byte_limit.set(self.store.limits.maximum_download_bytes)
        results: list = []
        paused = False
        try:
            results = await batch(claimed, self.config, fenced, **kwargs) or []
        except batch_module.DownloadCapacityPaused:
            paused = True
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._settle_claims(claimed, attempts)
            raise
        finally:
            downloader_module.download_byte_limit.reset(byte_limit_token)
            batch_module.download_admission.reset(admission_token)
        if paused:
            self._set_capacity(download=True)
        rows = self._settle_claims(claimed, attempts)
        for issue in claimed:
            if (rows.get(issue['id']) or {}).get('status') == 'complete':
                await self._publish(issue['id'])
        downloaded = _reconcile_download_results(report if report is not None else CycleReport(), results, self.logger)
        missing = 0 if paused else max(0, len(claimed) - len(results))
        if report is not None and missing:
            report.downloads_failed += missing
        self._queue_notifications(downloaded)
        safe_results = []
        for issue in claimed:
            row = rows.get(issue['id']) or {}
            safe_results.append({'issue_id': self.store.provider_issue(issue['id']), 'status': row.get('status'),
                                 'failure_kind': row.get('last_error_kind'), 'next_action': row.get('next_action'),
                                 'next_retry_at': row.get('next_retry_at')})
        await self.exports.sync_views_async()
        return {'physical_attempts': started_transfers if self.batch is None else len(identities),
                'outcomes': safe_results, 'missing_results': missing,
                'outcome': 'capacity_exhausted' if paused else 'succeeded'}

    def _settle_claims(self, claimed: list[dict], attempts: dict[int, str]) -> dict[int, dict]:
        """Finish each claimed attempt and release its requests, one issue at a time."""
        rows: dict[int, dict] = {}
        for issue in claimed:
            issue_id = issue['id']
            try:
                with self.store.transaction():
                    self.owner.check()
                    conn = self.store.conn
                    row = conn.execute('SELECT * FROM downloads WHERE issue_id=?', (issue_id,)).fetchone()
                    if row is not None and row['status'] == 'downloading':
                        # No outcome was recorded (paused before transfer, or a
                        # batch that returned early): it stays claimable.
                        conn.execute("UPDATE downloads SET status='pending' WHERE issue_id=? AND status='downloading'", (issue_id,))
                        row = conn.execute('SELECT * FROM downloads WHERE issue_id=?', (issue_id,)).fetchone()
                    conn.execute("UPDATE acquisition_attempts SET state='finished' WHERE id=? AND state='running'",
                                 (attempts[issue_id],))
                    for req in conn.execute("""SELECT r.id,s.client_id FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id
                            WHERE r.issue_id=? AND r.state='acquiring'""", (issue_id,)).fetchall():
                        conn.execute("UPDATE acquisition_requests SET state='queued',revision=revision+1 WHERE id=?", (req['id'],))
                        self.store.event(req['client_id'], 'request.updated', req['id'],
                                         lambda req=req: self.store.request(req['client_id'], req['id']))
                rows[issue_id] = dict(row) if row is not None else {}
            except ProtocolError:
                raise
            except sqlite3.Error as exc:
                self._log_once(('settle', issue_id), 'Unable to settle a claimed download; recovery will: %s', exc)
        return rows

    async def _publish_retained(self, selected: list[str] | None = None, *, include_errored: bool = False):
        sql = """SELECT DISTINCT r.issue_id FROM acquisition_requests r JOIN downloads d ON d.issue_id=r.issue_id
                 WHERE d.status='complete' AND r.state='queued'"""
        args: list = []
        if not include_errored:
            sql += ' AND r.fulfillment_error IS NULL'
        if selected is not None:
            if not selected:
                return
            sql += ' AND r.id IN (' + ','.join('?' for _ in selected) + ')'
            args.extend(selected)
        sql += ' LIMIT ?'
        args.append(len(selected) if selected is not None else 100)
        for row in self.store.conn.execute(sql, args).fetchall():
            await self._publish(row[0])

    async def _publish(self, issue_id: int):
        """Publish one issue's exports; failures are recorded for that issue only."""
        try:
            await self.exports.publish_async(issue_id)
        except ProtocolError as exc:
            if self._is_fatal(exc):
                raise
            if exc.code == 'capacity_exhausted':
                self._export_pass_blocked = True
                self._set_capacity(export=True)
                return
            self.record_export_error(issue_id, exc.code, missing_original=getattr(exc, 'missing_original', False))
        except OSError:
            self.record_export_error(issue_id, 'content_unavailable')

    def record_export_error(self, issue_id: int, code: str, *, missing_original: bool = False):
        """Record why publication was withheld, logging only when it changes.

        When remote demand needs an issue whose organized original is gone,
        the download is reset so the ordinary claim reacquires it. A file that
        exists but fails verification is not re-downloaded automatically.
        """
        changed = 0
        reacquire = False
        with self.store.transaction():
            for req in self.store.conn.execute("""SELECT r.id,s.client_id FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id
                WHERE r.issue_id=? AND r.state IN ('queued','acquiring') AND coalesce(r.fulfillment_error,'')!=?""", (issue_id, code)).fetchall():
                self.store.conn.execute('UPDATE acquisition_requests SET fulfillment_error=?,revision=revision+1 WHERE id=?', (code, req['id']))
                self.store.event(req['client_id'], 'request.updated', req['id'],
                                 lambda req=req: self.store.request(req['client_id'], req['id']))
                changed += 1
            if missing_original and self.store.remote_wanted(issue_id):
                row = self.store.conn.execute('SELECT limewire_url FROM issues WHERE id=?', (issue_id,)).fetchone()
                if row and is_valid_download_url(row['limewire_url'] or ''):
                    reacquire = bool(self.store.conn.execute("""UPDATE downloads SET status='pending',file_path=NULL,
                        downloaded_at=NULL,file_size_bytes=NULL,sha256=NULL,next_action=NULL,next_retry_at=NULL
                        WHERE issue_id=? AND status='complete'""", (issue_id,)).rowcount)
        if reacquire:
            self.logger.warning('Organized file for a requested issue is missing; downloading it again')
        elif changed:
            self.logger.warning('Export publication withheld: %s', code)

    @property
    def capacity_blocked(self) -> bool:
        """Whether downloads or export publication are paused for lack of space."""
        return self._download_blocked or self._export_blocked

    @capacity_blocked.setter
    def capacity_blocked(self, value: bool):
        self._set_capacity(download=value, export=value)

    def _set_capacity(self, *, download: bool | None = None, export: bool | None = None):
        before = self.capacity_blocked
        if download is not None:
            self._download_blocked = download
        if export is not None:
            self._export_blocked = export
        after = self.capacity_blocked
        if after != before:
            if after:
                self.logger.warning('Storage capacity exhausted; work that needs more space is paused')
            else:
                self.logger.info('Storage capacity available again')

    @staticmethod
    def _identity(issue: dict) -> str:
        try:
            return normalize_download_url(issue.get('limewire_url') or '')
        except URLValidationError:
            return f"invalid-issue:{issue.get('id')}"

    def _identities(self, claimed: list[dict]) -> set[str]:
        return {self._identity(issue) for issue in claimed}

    def _log_download_start(self, issue: dict) -> None:
        self.logger.info('  Downloading: %s', sanitize_external_error(issue.get('title') or 'Unknown issue', 120))

    # ------------------------------------------------------------------
    # Notifications
    # ------------------------------------------------------------------

    def _queue_notifications(self, issues: list[dict]):
        if self.notify and issues:
            self._pending_notifications.extend(issues)

    async def _maybe_flush_notifications(self):
        if self._pending_notifications and self.clock() - self._last_notification >= 60:
            self._flush_notifications()

    def _flush_notifications(self):
        """Send the pending summary without blocking heartbeats or commands."""
        if not self._pending_notifications:
            return
        issues, self._pending_notifications = self._pending_notifications, []
        self._last_notification = self.clock()
        task = asyncio.create_task(self._send_notification(issues))
        self._notify_tasks.add(task)
        task.add_done_callback(self._notify_tasks.discard)

    async def _send_notification(self, issues: list[dict]):
        try:
            await asyncio.to_thread(notify_module.send_download_summary, issues, self.config.notifications)
        except Exception as exc:
            self.logger.warning('Failed to send notification: %s', sanitize_external_error(exc))

    async def _drain_notifications(self, *, timeout: float):
        self._flush_notifications()
        if self._notify_tasks:
            _done, pending = await asyncio.wait(set(self._notify_tasks), timeout=timeout)
            for task in pending:
                task.cancel()
