"""Local operator commands; importing this module does not import ASGI packages."""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import fields
from pathlib import Path

import typer

from magsync.config import get_db_path, load_config
from magsync.core.index import MagazineIndex

from .ownership import Ownership
from .protocol import ProtocolError
from .store import Limits, Store

companion = typer.Typer(help='Initialize, inspect and recover the companion store.')
clients = typer.Typer(help='Provision and revoke consumer application credentials locally.')


def service_limits() -> Limits:
    limits = Limits()
    for field in fields(limits):
        value = os.environ.get('MAGSYNC_SERVICE__' + field.name.upper())
        if value is not None:
            # Parse by the declared type: float settings may have int defaults.
            caster = float if field.type in ('float', float) else int
            try:
                number = caster(value)
            except ValueError:
                raise typer.BadParameter(f'{field.name} must be a {caster.__name__} value') from None
            if number <= 0:
                raise typer.BadParameter(f'{field.name} must be positive')
            setattr(limits, field.name, number)
    if limits.heartbeat_stale_seconds <= limits.heartbeat_seconds:
        raise typer.BadParameter('heartbeat_stale_seconds must exceed heartbeat_seconds')
    return limits


def export_root(index) -> Path:
    return Path(os.environ.get('MAGSYNC_EXPORT_DIR', str(index.db_path.parent / 'exports')))


def log_startup_banner(logger, cfg, *, mode: str, interval_label: str) -> None:
    """Log the configuration a long-running runtime starts with."""
    from magsync import __version__
    logger.info('magsync v%s %s starting', __version__, mode)
    logger.info('  Output directory: %s', cfg.output_dir)
    logger.info('  Subscriptions: %d', len(cfg.subscriptions))
    logger.info('  Interval: %s', interval_label)
    logger.info('  Notifications: %s', 'enabled' if cfg.notifications.enabled else 'disabled')
    for sub in cfg.subscriptions:
        since = f' (since {sub.since})' if sub.since else ''
        exact = ' [exact]' if sub.exact else ''
        logger.info('    - %s%s%s', sub.query, since, exact)
    if not cfg.subscriptions:
        logger.warning("No subscriptions configured. Add with 'magsync subscribe' or MAGSYNC_SUBSCRIPTIONS env var.")


def open_store() -> tuple[MagazineIndex, Store]:
    if not get_db_path().is_file():
        raise typer.BadParameter('Run magsync companion init first, or restore the store.')
    index = MagazineIndex()
    store = Store(index, service_limits())
    store.check_identity()
    return index, store


def emit(value):
    typer.echo(json.dumps(value, indent=2))


@companion.command('init')
def initialize():
    """Explicitly initialize provider identity over the existing local catalog."""
    index = MagazineIndex()
    store = Store(index, service_limits())
    try:
        with Ownership(store, Path(load_config().output_dir), export_root(index)):
            identity = store.initialize()
            store.migrate_local(load_config().subscriptions)
            emit(identity)
    except ProtocolError as exc:
        raise typer.BadParameter(exc.message) from None
    finally:
        index.close()


@clients.command('create')
def create(label: str):
    """Issue one client key. The token is displayed once; store it securely."""
    index, store = open_store()
    try:
        emit(store.provision(label))
    finally:
        index.close()


@clients.command('rotate')
def rotate(client_id: str, revoke_old: bool = typer.Option(False, '--revoke-old')):
    """Issue a new key, optionally revoking all prior keys immediately."""
    index, store = open_store()
    try:
        emit(store.provision('', client_id=client_id, revoke_old=revoke_old))
    finally:
        index.close()


@clients.command('revoke')
def revoke(key_id: str):
    index, store = open_store()
    try:
        store.revoke(key_id)
        emit({'key_id':key_id, 'revoked':True})
    finally:
        index.close()


@clients.command('disable')
def disable(client_id: str):
    index, store = open_store()
    try:
        store.enable_client(client_id, False)
        emit({'client_id':client_id, 'enabled':False})
    finally:
        index.close()


@clients.command('enable')
def enable(client_id: str):
    index, store = open_store()
    try:
        store.enable_client(client_id, True)
        emit({'client_id':client_id, 'enabled':True})
    finally:
        index.close()


@clients.command('list')
def list_clients():
    index, store = open_store()
    try:
        emit([dict(r) for r in store.conn.execute("SELECT id,label,enabled FROM clients WHERE id!='local'")])
    finally:
        index.close()


@companion.command('status')
def status():
    """Inspect provider identity, queue, outstanding deliveries and storage totals."""
    index, store = open_store()
    try:
        identity = store.identity()
        emit({'instance_id':identity['instance_id'], 'recovery_epoch':identity['recovery_epoch'],
              'runtime':dict(store.conn.execute('SELECT owner_id,generation,heartbeat_at,accepting FROM runtime_state').fetchone()),
              'queued_operations':store.conn.execute("SELECT count(*) FROM operations WHERE state IN ('queued','running')").fetchone()[0],
              'oldest_queued_at':store.conn.execute("SELECT min(created_at) FROM operations WHERE state IN ('queued','running')").fetchone()[0],
              'outstanding_deliveries':[dict(r) for r in store.conn.execute("""SELECT s.client_id,count(*) deliveries,sum(o.size) referenced_bytes
                  FROM deliveries d JOIN content_objects o ON o.id=d.content_id JOIN acquisition_requests r ON r.id=d.request_id
                  JOIN scopes s ON s.id=r.scope_id LEFT JOIN acknowledgments a ON a.delivery_id=d.id
                  WHERE a.delivery_id IS NULL GROUP BY s.client_id""")],
              'export_bytes':store.conn.execute("SELECT coalesce(sum(size),0) FROM content_objects WHERE state='ready'").fetchone()[0],
              'export_limit_bytes':store.limits.export_bytes,
              'free_bytes':shutil.disk_usage(export_root(index) if export_root(index).exists() else index.db_path.parent).free,
              'minimum_free_bytes':store.limits.minimum_free_bytes,
              'content_generations':[dict(r) for r in store.conn.execute('SELECT id,state,size FROM content_objects ORDER BY created_at DESC LIMIT 100')]})
    finally:
        index.close()


@companion.command('recover')
def recover():
    """After a coordinated restore, rotate the recovery epoch and verify exports."""
    from .exports import Exports
    from .journal import Journal
    index, store = open_store()
    try:
        with Ownership(store, Path(load_config().output_dir), export_root(index)) as owner:
            emit(Journal(store, Exports(store, owner)).rotate_epoch())
    finally:
        index.close()


@companion.command('purge')
def purge(content_id: str):
    """Explicitly remove an export generation, preserving unavailable receipts."""
    from .exports import Exports
    index, store = open_store()
    try:
        with Ownership(store, Path(load_config().output_dir), export_root(index)) as owner:
            emit({'removed':Exports(store, owner).cleanup(purge=content_id)})
    finally:
        index.close()


def serve(host: str = typer.Option('127.0.0.1', '--host'), port: int = typer.Option(8765, '--port'),
          interval: str = typer.Option(None, '--interval', '-i',
                                       help='Time between discovery cycles (e.g. 30m, 6h, 1d). Default: MAGSYNC_INTERVAL or 6h')):
    """Run the optional authenticated companion service (one worker)."""
    try:
        import uvicorn
        from .api import create_app
    except ImportError:
        typer.echo('Install HTTP support with: pip install "magsync[service]"', err=True)
        raise typer.Exit(2) from None
    if os.getenv('WEB_CONCURRENCY', '1') != '1':
        raise typer.BadParameter('The companion service requires exactly one worker.')
    from magsync.cli import _configure_daemon_external_logging, _parse_interval
    interval_label = interval or os.environ.get('MAGSYNC_INTERVAL', '6h')
    try:
        scan_seconds = _parse_interval(interval_label)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None
    index, store = open_store()
    exports = export_root(index)
    index.close()
    views = json.loads(os.environ.get('MAGSYNC_CLIENT_EXPORT_VIEWS', '{}'))
    trusted = os.environ.get('MAGSYNC_TRUSTED_MOUNTS') == '1'
    _configure_daemon_external_logging()
    from .safety import configure_service_logging
    configure_service_logging()
    import logging
    log_startup_banner(logging.getLogger('magsync'), load_config(), mode='service',
                       interval_label=f'{interval_label} ({scan_seconds}s)')
    uvicorn.run(create_app(limits=service_limits(), exports=exports, views=views, trusted_mounts=trusted,
                           scan_seconds=scan_seconds, notify=True),
                host=host, port=port, workers=1, access_log=False, timeout_graceful_shutdown=60)
