"""Optional Pydantic response schemas for generated protocol documentation."""
from typing import Any, Literal

from pydantic import BaseModel, Field

from .protocol import OperationState, DeliveryState


class CatalogIssue(BaseModel):
    id: str
    title: str
    year: int | None
    month: int | None
    genre: str | None
    source: Literal['freemagazines.top']


class Retention(BaseModel):
    events_seconds: int
    idempotency_seconds: int
    snapshot_seconds: int
    expired_key: Literal['new_command']


class PageLimits(BaseModel):
    page_default: int
    page_max: int
    query_length: int
    source_pages: int


class Health(BaseModel):
    live: bool
    ready: bool
    pipeline: str
    capacity_paused: bool


class Configuration(BaseModel):
    api_subscriptions: Literal['sqlite']
    local_subscriptions: Literal['environment', 'file']


class Info(BaseModel):
    protocol_version: Literal['1']
    instance_id: str
    recovery_epoch: str
    capabilities: list[str]
    media_types: list[Literal['application/pdf']]
    retention: Retention
    limits: PageLimits
    configuration: Configuration
    health: Health


class OperationDocument(BaseModel):
    id: str
    kind: str
    state: OperationState
    resource_id: str | None
    result: dict[str, Any] = Field(description='Search: outcome, items (CatalogIssue), failure_kind. Acquisition: physical_attempts, outcomes with issue_id/status/failure_kind/next_action/next_retry_at. Terminal errors carry code.')
    created_at: str
    updated_at: str
    next_offset: int | None = None


class ImportReceipt(BaseModel):
    delivery_id: str
    receipt_id: str
    sha256: str
    size: int
    created_at: str


class Transfer(BaseModel):
    http: str | None = None
    mount: str | None = None


class DeliveryDocument(BaseModel):
    id: str
    instance_id: str
    recovery_epoch: str
    client_id: str
    scope_id: str
    request_id: str
    issue_id: str
    content_generation: str
    sha256: str
    size: int
    media_type: Literal['application/pdf']
    title: str
    year: int | None
    month: int | None
    source: Literal['freemagazines.top']
    state: DeliveryState
    transfer: Transfer
    receipt: ImportReceipt | None


class Event(BaseModel):
    id: str
    seq: int
    kind: str
    resource_id: str
    created_at: str
    resource: dict[str, Any] = Field(description='Committed resource or transition patch; apply in seq order. A snapshot restores full state.')


class EventPage(BaseModel):
    items: list[Event]
    cursor: str
    has_more: bool


class SnapshotItem(BaseModel):
    kind: Literal['scope', 'subscription', 'request', 'delivery']
    resource: dict[str, Any]


class SnapshotPage(BaseModel):
    id: str
    items: list[SnapshotItem]
    next: str | None
    handoff_cursor: str
    expires_at: str
