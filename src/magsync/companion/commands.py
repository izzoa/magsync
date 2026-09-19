"""Terminal command semantics executed by a companion/daemon runtime owner.

Each command mirrors its standalone implementation: the same selection, the
same search depth (local commands are not subject to the remote API's page
bound) and the same outcomes, returned as data the terminal renders.
"""
from __future__ import annotations

from magsync.config import add_subscription, load_config, remove_subscription, set_config_value
from magsync.core.models import DownloadStatus, LinkResolutionKind, SourceError
from magsync.core.organizer import strip_accents
from magsync.core.repair import repair_titles

from .local import local_request
from .protocol import ProtocolError
from .store import LOCAL_SCOPE


def _since(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    parts = value.split('-')
    try:
        return int(parts[0]), (int(parts[1]) if len(parts) > 1 and parts[1] else None)
    except ValueError:
        raise ProtocolError('invalid_request') from None


def _reconcile_local_configuration(runtime) -> None:
    subscriptions = load_config().subscriptions
    runtime._local_snapshot = list(subscriptions)
    runtime.store.reconcile_local(subscriptions)


async def execute_local(runtime, kind: str, body: dict, operation_id: str) -> dict:
    store, index = runtime.store, runtime.store.index
    if kind == 'search':
        return await runtime.search({'query': body['query']}, detail=True)

    if kind == 'fetch':
        found = await runtime.search({'query': body['query']}, detail=True)
        summary = {key: found.get(key) for key in ('added', 'detail_failures', 'failure', 'failure_kind')}
        if found['outcome'] in ('blocked', 'failed'):
            return {**summary, 'outcome': found['outcome']}
        if found['outcome'] == 'empty':
            return {**summary, 'outcome': 'empty', 'pending': 0, 'outcomes': []}
        since_year, since_month = _since(body.get('since'))
        # Same scope as the standalone fetch: every non-complete row matching
        # the query becomes explicit local intent; pending rows download now,
        # and rows another task is downloading are joined for their outcome.
        index.promote_subscribed(runtime._local_snapshot)
        rows = index.get_issues(magazine_title=strip_accents(body['query']).lower(),
                               since_year=since_year, since_month=since_month)
        requests, recoverable = [], 0
        with store.transaction():
            for row in rows:
                if row['download_status'] == DownloadStatus.COMPLETE.value:
                    continue
                request_id = local_request(store, row['id'])
                store.conn.execute("UPDATE downloads SET requested_by='manual' WHERE issue_id=?", (row['id'],))
                store.conn.execute('INSERT OR IGNORE INTO operation_requests VALUES(?,?)', (operation_id, request_id))
                if row['download_status'] in (DownloadStatus.PENDING.value, DownloadStatus.DOWNLOADING.value):
                    requests.append(request_id)
                elif row['download_status'] in ('failed', 'unavailable'):
                    recoverable += 1
        acquired = await runtime.acquire(requests) if requests else {'physical_attempts': 0, 'outcomes': []}
        paused = acquired.get('outcome') == 'capacity_exhausted'
        result = {**summary, **acquired, 'pending': len(requests), 'recoverable': recoverable,
                  'outcome': 'partial' if paused or found['outcome'] == 'partial' else 'succeeded'}
        if paused:
            result['code'] = 'capacity_exhausted'
        return result

    if kind == 'update':
        magazines = index.get_tracked_magazines()
        lines, total_new, incomplete, skipped = [], 0, 0, 0
        for position, magazine in enumerate(magazines):
            found = await runtime.search({'query': magazine['title']}, detail=True)
            lines.append({'title': magazine['title'], 'outcome': found['outcome'], 'added': found.get('added', 0),
                          'detail_failures': found.get('detail_failures', 0), 'failure': found.get('failure')})
            total_new += found.get('added', 0) or 0
            if found['outcome'] in ('blocked', 'failed') or found.get('detail_failures'):
                incomplete += 1
            if found['outcome'] == 'blocked':
                skipped = len(magazines) - position - 1
                break
        store.materialize(local=True)
        return {'outcome': 'partial' if incomplete or skipped else 'succeeded', 'tracked': len(magazines),
                'magazines': lines, 'new': total_new, 'incomplete': incomplete, 'skipped': skipped}

    if kind == 'backfill':
        every = index.get_issues_missing_url(magazine_title=body.get('query'), wanted_only=False)
        rows = every if body.get('include_all') else [r for r in every if store.issue_wanted(r['id'], scope_id=LOCAL_SCOPE)]
        repaired = missing = failed = skipped = 0
        failure_kind = None
        for position, row in enumerate(rows):
            if runtime.source.circuit_open:
                skipped = len(rows) - position
                break
            try:
                detail = await runtime.source.scrape_detail(row['page_url'])
                url = detail.limewire_url
                if not url and detail.download_key:
                    resolved = await runtime.source.resolve_masked_download(row['page_url'], detail.download_key)
                    if resolved.kind is LinkResolutionKind.SUPPORTED:
                        url = resolved.url
                if url:
                    index.set_limewire_url(row['id'], url)
                    repaired += 1
                else:
                    missing += 1
            except SourceError as exc:
                failed += 1
                failure_kind = exc.kind.value
                runtime.pipeline = 'degraded'
        outcome = 'blocked' if runtime.source.circuit_open else 'partial' if failed else 'succeeded'
        return {'outcome': outcome, 'total': len(rows), 'repaired': repaired, 'missing': missing, 'failed': failed,
                'skipped': skipped, 'failure_kind': failure_kind, 'parked_skipped': len(every) - len(rows)}

    if kind == 'subscribe':
        added = add_subscription(body['query'], since=body.get('since'), exact=bool(body.get('exact')))
        _reconcile_local_configuration(runtime)
        return {'outcome': 'succeeded' if added else 'unchanged', 'configuration_source': 'file'}

    if kind == 'unsubscribe':
        removed = remove_subscription(body['query'])
        _reconcile_local_configuration(runtime)
        return {'outcome': 'succeeded' if removed else 'unchanged', 'configuration_source': 'file'}

    if kind == 'config':
        # A missing value is a read that must never reach an owner as a write.
        if body.get('value') in (None, ''):
            raise ProtocolError('invalid_request')
        try:
            set_config_value(body['key'], body['value'])
        except ValueError as exc:
            return {'outcome': 'failed', 'code': 'invalid_request', 'message': str(exc)}
        _reconcile_local_configuration(runtime)
        return {'outcome': 'succeeded', 'configuration_source': 'file'}

    if kind == 'repair':
        return {'outcome': 'succeeded', **repair_titles(index, runtime.config.output_dir).as_dict()}

    raise ProtocolError('incompatible_protocol')
