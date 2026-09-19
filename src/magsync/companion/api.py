"""Optional ASGI adapter for the version 1 companion protocol."""

import asyncio
import logging
import os
import re
import signal
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Generic, TypeVar

from fastapi import Depends, FastAPI, Header, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException

from magsync.config import get_db_path, load_config
from magsync.core.index import MagazineIndex

from .protocol import AcquisitionRequest, ERRORS, ProtocolError, Scope, Subscription
from .runtime import Runtime
from .store import Limits
from .wire import CatalogIssue, Info, OperationDocument, DeliveryDocument, ImportReceipt, EventPage, SnapshotPage


class Input(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)


class NewScope(Input):
    external_id: str = Field(min_length=1, max_length=256)
    label: str = Field(min_length=1, max_length=256)


class EditScope(Input):
    label: str | None = Field(default=None, min_length=1, max_length=256)
    enabled: bool | None = None


class NewSubscription(Input):
    query: str = Field(min_length=1, max_length=256)
    exact: bool = False
    since: str | None = Field(default=None, pattern=r'^\d{4}-(0[1-9]|1[0-2])$')
    enabled: bool = True


class EditSubscription(Input):
    query: str | None = Field(default=None, min_length=1, max_length=256)
    exact: bool | None = None
    since: str | None = Field(default=None, pattern=r'^\d{4}-(0[1-9]|1[0-2])$')
    enabled: bool | None = None


class NewRequest(Input):
    issue_id: str = Field(min_length=1, max_length=64)


class Search(Input):
    query: str = Field(min_length=1, max_length=256)
    pages: int = Field(default=5, ge=1, le=5)


class Receipt(Input):
    receipt_id: str = Field(min_length=1, max_length=256)
    sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    size: int = Field(gt=0)


class ErrorDetail(BaseModel):
    code: str = Field(json_schema_extra={"enum": list(ERRORS)})
    message: str
    correlation_id: str
    retry_at: str | None = None


class ErrorEnvelope(BaseModel):
    error: ErrorDetail


T = TypeVar('T')


class Page(BaseModel, Generic[T]):
    items: list[T]
    next: str | None = None


class Accepted(BaseModel, Generic[T]):
    operation_id: str
    resource: T | None = None


class BodyLimit:
    """Bound chunked bodies before the framework allocates/validates their JSON."""
    def __init__(self, app, maximum: int):
        self.app, self.maximum = app, maximum

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        chunks, size = [], 0
        while True:
            message = await receive()
            if message['type'] == 'http.disconnect':
                return
            size += len(message.get('body', b''))
            if size > self.maximum:
                error = ProtocolError('invalid_request')
                return await JSONResponse(error.to_dict(), status_code=413)(scope, receive, send)
            chunks.append(message)
            if not message.get('more_body', False):
                break
        async def bounded_receive():
            return chunks.pop(0) if chunks else await receive()
        await self.app(scope, bounded_receive, send)


def _terminate_service() -> None:
    """Ask the server to shut down gracefully so a supervisor can restart it."""
    os.kill(os.getpid(), signal.SIGTERM)


def create_app(*, runtime: Runtime | None = None, db_path: Path | None = None, config=None,
               limits: Limits | None = None, exports: Path | None = None, background: bool = True,
               views=None, trusted_mounts: bool = False, scan_seconds: float | None = None,
               notify: bool = False, on_runtime_exit=_terminate_service, logger=None) -> FastAPI:
    configured_limits = limits or (runtime.store.limits if runtime else Limits())

    @asynccontextmanager
    async def lifespan(app):
        owned_index = None
        current = runtime
        if current is None:
            path = db_path or get_db_path()
            # Service startup never bootstraps an empty store accidentally.
            if not path.is_file():
                raise ProtocolError('store_uninitialized')
            owned_index = MagazineIndex(path)
            options = {'scan_seconds': scan_seconds} if scan_seconds else {}
            current = Runtime(owned_index, config or load_config(), limits=configured_limits,
                              exports=exports, views=views, trusted_mounts=trusted_mounts,
                              notify=notify, logger=logger, **options)
        app.state.runtime = current
        watcher = None
        try:
            # The service always drains the durable queue, even when a test
            # harness drives its loop by hand.
            await current.start_when_available(background=background, accept_commands=True)
            if background and current.loop_task is not None:
                watcher = asyncio.create_task(_watch_runtime(current, on_runtime_exit))
            yield
        finally:
            if watcher is not None:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
            if current.owner.generation is not None:
                await current.stop()
            if owned_index:
                owned_index.close()

    async def _watch_runtime(current: Runtime, on_exit) -> None:
        # A dead command/work loop must not leave a live-looking service
        # holding the store: exit so the supervisor restarts it.
        await asyncio.gather(asyncio.shield(current.loop_task), return_exceptions=True)
        if not current.stopping:
            logging.getLogger('magsync').error('Companion runtime stopped unexpectedly; shutting down the service')
            on_exit()

    responses = {code: {'model': ErrorEnvelope} for code in (400,401,404,409,410,413,422,429,503)}
    app = FastAPI(title='MagSync companion API', version='1', lifespan=lifespan,
                  openapi_url=None, docs_url=None, redoc_url=None, responses=responses)
    app.add_middleware(BodyLimit, maximum=configured_limits.body_bytes)
    security = HTTPBearer(auto_error=False)

    def current(request: Request) -> Runtime:
        return request.app.state.runtime

    async def principal(request: Request, credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)]) -> str:
        if credentials is None or credentials.scheme.lower() != 'bearer':
            raise ProtocolError('unauthorized')
        if request.headers.get('X-MagSync-Protocol', '1') != '1':
            raise ProtocolError('incompatible_protocol')
        return current(request).store.authenticate(credentials.credentials)

    Principal = Annotated[str, Depends(principal)]
    Key = Annotated[str, Header(alias='Idempotency-Key', min_length=1, max_length=128)]
    Match = Annotated[str, Header(alias='If-Match')]

    def revision(header: str) -> int:
        try:
            value = int(header.strip('"'))
            if value < 1:
                raise ValueError
            return value
        except ValueError:
            raise ProtocolError('revision_conflict') from None

    @app.exception_handler(ProtocolError)
    async def protocol_error(_request, error):
        return JSONResponse(error.to_dict(), status_code=error.status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request, _error):
        error = ProtocolError('invalid_request')
        return JSONResponse(error.to_dict(), status_code=error.status)

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        error = ProtocolError('incompatible_protocol' if request.url.path.startswith('/v') else 'not_found')
        return JSONResponse(error.to_dict(), status_code=error.status)

    @app.exception_handler(sqlite3.Error)
    @app.exception_handler(OSError)
    async def local_error(_request, _error):
        error = ProtocolError('runtime_unavailable')
        return JSONResponse(error.to_dict(), status_code=error.status)

    @app.exception_handler(Exception)
    async def unexpected_error(_request, _error):
        error = ProtocolError('runtime_unavailable')
        return JSONResponse(error.to_dict(), status_code=error.status)

    def accepted(request, client, kind, scope_id, body, key, mutation=None):
        rt = current(request)
        rt.assert_ready()
        result = rt.store.accept(client, kind, scope_id, body, key, mutation)
        rt.wakeup.set()
        return result

    @app.get('/health/live')
    async def live(request: Request):
        ok = current(request).live()
        return JSONResponse({'live': ok}, status_code=200 if ok else 503)

    @app.get('/health/ready')
    async def ready(request: Request):
        try:
            current(request).assert_ready()
            ok = True
        except (ProtocolError, sqlite3.Error, OSError):
            ok = False
        return JSONResponse({'ready': ok}, status_code=200 if ok else 503)

    @app.get('/v1/info', response_model=Info)
    async def info(request: Request, client: Principal):
        rt = current(request)
        identity = rt.store.identity()
        capabilities = ['scopes', 'subscriptions', 'requests', 'search', 'retry', 'cancel', 'events', 'snapshots', 'http_ranges', 'verified_import_ack']
        if client in rt.exports.views:
            capabilities.append('trusted_client_mount')
        return {'protocol_version':'1', 'instance_id':identity['instance_id'], 'recovery_epoch':identity['recovery_epoch'],
                'capabilities':capabilities, 'media_types':['application/pdf'],
                'retention':{'events_seconds':rt.store.limits.event_seconds, 'idempotency_seconds':rt.store.limits.idempotency_seconds,
                             'snapshot_seconds':rt.store.limits.snapshot_seconds, 'expired_key':'new_command'},
                'limits':{'page_default':rt.store.limits.page_default,'page_max':rt.store.limits.page_max,
                          'query_length':rt.store.limits.query_length,'source_pages':rt.store.limits.source_pages},
                'configuration':{'api_subscriptions':'sqlite','local_subscriptions':'environment' if os.getenv('MAGSYNC_SUBSCRIPTIONS') else 'file'},
                'health':{'live':rt.live(),'ready':rt.ready,'pipeline':rt.pipeline,'capacity_paused':rt.capacity_blocked}}

    @app.get('/v1/openapi.json', include_in_schema=False)
    async def schema(client: Principal):
        return app.openapi()

    @app.post('/v1/searches', status_code=202, response_model=Accepted[dict])
    async def searches(request: Request, body: Search, client: Principal, key: Key):
        if len(body.query) > configured_limits.query_length or body.pages > configured_limits.source_pages:
            raise ProtocolError('invalid_request')
        return accepted(request, client, 'search', None, body.model_dump(), key)

    @app.get('/v1/operations/{id}', response_model=OperationDocument)
    async def operation(request: Request, id: str, client: Principal,
                        offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500)):
        rt = current(request)
        size = rt.store.page_size(limit)
        result = rt.store.operation(client, id)
        items = result['result'].get('items', result['result'].get('outcomes'))
        if items is not None:
            name = 'items' if 'items' in result['result'] else 'outcomes'
            result['result'][name] = items[offset:offset+size]
            result['next_offset'] = offset+size if len(items)>offset+size else None
        return result

    @app.get('/v1/issues/{id}', response_model=CatalogIssue)
    async def issue(request: Request, id: str, client: Principal):
        return current(request).store.issue(id)

    @app.get('/v1/scopes', response_model=Page[Scope])
    async def scopes(request: Request, client: Principal, after: str = '', limit: int = Query(100, ge=1, le=500)):
        return current(request).store.list_resources(client, 'scopes', after=after, limit=limit)

    @app.post('/v1/scopes', status_code=201, response_model=Accepted[Scope])
    async def new_scope(request: Request, body: NewScope, client: Principal, key: Key):
        rt = current(request)
        data = body.model_dump()
        return accepted(request, client, 'scope.create', None, data, key, lambda: rt.store.create_scope(client, data))

    @app.patch('/v1/scopes/{id}', response_model=Scope)
    async def edit_scope(request: Request, id: str, body: EditScope, client: Principal, match: Match):
        rt = current(request)
        rt.assert_ready()
        result = rt.store.update_scope(client, id, body.model_dump(exclude_none=True), revision(match))
        await rt.exports.sync_views_async()
        return result

    @app.get('/v1/scopes/{id}/subscriptions', response_model=Page[Subscription])
    async def subscriptions(request: Request, id: str, client: Principal, after: str = '', limit: int = Query(100, ge=1, le=500)):
        return current(request).store.list_resources(client, 'subscriptions', scope_id=id, after=after, limit=limit)

    @app.post('/v1/scopes/{id}/subscriptions', status_code=201, response_model=Accepted[Subscription])
    async def new_subscription(request: Request, id: str, body: NewSubscription, client: Principal, key: Key):
        rt = current(request)
        data = body.model_dump()
        return accepted(request, client, 'subscription.create', id, data, key, lambda: rt.store.create_subscription(client, id, data))

    @app.patch('/v1/subscriptions/{id}', response_model=Subscription)
    async def edit_subscription(request: Request, id: str, body: EditSubscription, client: Principal, match: Match):
        rt = current(request)
        rt.assert_ready()
        data = body.model_dump(exclude_unset=True)
        if any(value is None for key,value in data.items() if key!='since'):
            raise ProtocolError('invalid_request')
        return rt.store.update_subscription(client, id, data, revision(match))

    @app.delete('/v1/subscriptions/{id}', response_model=Subscription)
    async def delete_subscription(request: Request, id: str, client: Principal, match: Match):
        rt = current(request)
        rt.assert_ready()
        return rt.store.update_subscription(client, id, {}, revision(match), delete=True)

    @app.get('/v1/scopes/{id}/requests', response_model=Page[AcquisitionRequest])
    async def requests(request: Request, id: str, client: Principal, after: str = '', limit: int = Query(100, ge=1, le=500)):
        return current(request).store.list_resources(client, 'requests', scope_id=id, after=after, limit=limit)

    @app.post('/v1/scopes/{id}/requests', status_code=202, response_model=Accepted[AcquisitionRequest])
    async def new_request(request: Request, id: str, body: NewRequest, client: Principal, key: Key):
        rt = current(request)
        data = body.model_dump()
        return accepted(request, client, 'request.create', id, data, key, lambda: rt.store.create_request(client, id, data))

    @app.get('/v1/requests/{id}', response_model=AcquisitionRequest)
    async def get_request(request: Request, id: str, client: Principal):
        return current(request).store.request(client, id)

    @app.post('/v1/requests/{id}/cancel', response_model=AcquisitionRequest)
    async def cancel(request: Request, id: str, client: Principal, match: Match):
        rt = current(request)
        rt.assert_ready()
        result = rt.store.cancel(client, id, revision(match))
        await rt.exports.sync_views_async()
        return result

    @app.post('/v1/requests/{id}/retry', status_code=202, response_model=Accepted[dict])
    async def retry(request: Request, id: str, client: Principal, key: Key, match: Match):
        rt = current(request)
        rt.assert_ready()
        with rt.store.transaction():
            req = rt.store.request(client, id)
            def validate_retry():
                rt.store.revision(req, revision(match))
                if req['state']=='canceled' or req['physical_status']=='unsupported':
                    raise ProtocolError('unsupported')
                return req
            result = accepted(request, client, 'request.retry', req['scope_id'],
                              {'request_id':id, 'revision':revision(match)}, key, validate_retry)
            rt.store.conn.execute('INSERT OR IGNORE INTO operation_requests VALUES(?,?)', (result['operation_id'], id))
            if req['physical_status'] == 'downloading':
                rt.store.conn.execute("UPDATE operations SET result=? WHERE id=? AND state='queued'",
                    ('{"attached":true}', result['operation_id']))
            return result

    @app.get('/v1/events', response_model=EventPage)
    async def events(request: Request, client: Principal, cursor: str | None = None, scope: str = '', limit: int = Query(100, ge=1, le=500)):
        return current(request).journal.events(client, cursor, scope=scope, limit=limit)

    @app.post('/v1/snapshots', status_code=201, response_model=Accepted[SnapshotPage])
    async def snapshot(request: Request, client: Principal, key: Key):
        rt = current(request)
        return accepted(request, client, 'snapshot.create', None, {}, key, lambda: rt.journal.snapshot(client))

    @app.get('/v1/snapshots/{id}', response_model=SnapshotPage)
    async def snapshot_page(request: Request, id: str, client: Principal, cursor: str | None = None, limit: int = Query(100, ge=1, le=500)):
        return current(request).journal.snapshot_page(client, id, cursor, limit=limit)

    @app.get('/v1/deliveries/{id}', response_model=DeliveryDocument, response_model_exclude_none=True)
    async def delivery(request: Request, id: str, client: Principal):
        return current(request).exports.delivery(client, id)

    @app.put('/v1/deliveries/{id}/ack', response_model=ImportReceipt)
    async def ack(request: Request, id: str, body: Receipt, client: Principal):
        rt = current(request)
        rt.assert_ready()
        return rt.journal.acknowledge(client, id, body.model_dump())

    @app.get('/v1/deliveries/{id}/content', response_class=StreamingResponse,
             responses={200:{'content':{'application/pdf':{}}},206:{'content':{'application/pdf':{}}},416:{'description':'Unsatisfiable byte range'}})
    async def content(request: Request, id: str, client: Principal,
                      range_header: Annotated[str | None, Header(alias='Range')] = None,
                      if_range: Annotated[str | None, Header(alias='If-Range')] = None,
                      if_none_match: Annotated[str | None, Header(alias='If-None-Match')] = None):
        rt = current(request)
        lease = rt.exports.open_content(client, id)
        stream, metadata = lease.__enter__()
        size = metadata['size']
        etag = '"' + metadata['content_generation'] + ':' + metadata['sha256'] + '"'
        headers = {'ETag':etag, 'Accept-Ranges':'bytes','Cache-Control':'private, no-cache',
                   'Content-Disposition':f'attachment; filename="{id}.pdf"'}
        if if_none_match == etag:
            lease.__exit__(None,None,None)
            return Response(status_code=304, headers=headers)
        start, end, status = 0, size-1, 200
        if range_header and (if_range is None or if_range == etag):
            match = re.fullmatch(r'bytes=(\d*)-(\d*)', range_header)
            try:
                if not match or not any(match.groups()):
                    raise ValueError
                first, last = match.groups()
                if first:
                    start = int(first)
                    end = min(int(last), size-1) if last else size-1
                else:
                    suffix = int(last)
                    if suffix <= 0:
                        raise ValueError
                    start = max(0, size-suffix)
                if start > end or start >= size:
                    raise ValueError
            except ValueError:
                lease.__exit__(None,None,None)
                return Response(status_code=416, headers={**headers, 'Content-Range':f'bytes */{size}'})
            status = 206
            headers['Content-Range'] = f'bytes {start}-{end}/{size}'
        headers['Content-Length'] = str(end-start+1)
        async def chunks():
            try:
                stream.seek(start)
                remaining = end-start+1
                while remaining:
                    data = await asyncio.to_thread(stream.read, min(1024*1024,remaining))
                    if not data:
                        break
                    remaining -= len(data)
                    yield data
            finally:
                lease.__exit__(None,None,None)
        return StreamingResponse(chunks(), status_code=status, media_type='application/pdf', headers=headers)

    return app
