"""Version 1 wire vocabulary, independent of FastAPI and persistence."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4


class RequestState(str, Enum):
    QUEUED = "queued"
    ACQUIRING = "acquiring"
    FULFILLED = "fulfilled"
    CANCELED = "canceled"
    SUSPENDED = "suspended"


class OperationState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    EMPTY = "empty"
    PARTIAL = "partial"
    BLOCKED = "blocked"
    FAILED = "failed"
    SUSPENDED = "suspended"


class DeliveryState(str, Enum):
    READY = "ready"
    ACKNOWLEDGED = "acknowledged"
    UNAVAILABLE = "unavailable"
    CANCELED = "canceled"


ERRORS = {
    "unauthorized": (401, "A valid application credential is required."),
    "not_found": (404, "Resource not found."),
    "invalid_request": (422, "Request does not match the protocol."),
    "incompatible_protocol": (400, "Protocol or capability is not supported."),
    "revision_conflict": (409, "Resource revision has changed."),
    "idempotency_conflict": (409, "Key was already used with different parameters."),
    "configuration_managed": (409, "Configuration is managed externally or has changed."),
    "receipt_conflict": (409, "Receipt or integrity metadata does not match."),
    "scope_disabled": (409, "Scope is disabled."),
    "unsupported": (409, "Current download policy does not permit this action."),
    "resync_required": (410, "Start a new state snapshot to resume reconciliation."),
    "content_unavailable": (410, "Verified content is unavailable."),
    "capacity_exhausted": (429, "Capacity limit reached; retry after capacity is available."),
    "rate_limited": (429, "Command admission limit reached."),
    "runtime_unavailable": (503, "Local acquisition runtime is unavailable."),
    "store_uninitialized": (503, "Initialize or restore the companion store locally."),
    "schema_incompatible": (503, "Store schema is incompatible with this binary."),
    "integrity_failed": (409, "Complete PDF bytes failed integrity verification."),
}


class ProtocolError(Exception):
    """Only fixed messages cross the public boundary; never stringify causes."""

    def __init__(self, code: str, *, retry_at: str | None = None):
        self.code = code
        self.status, self.message = ERRORS[code]
        self.correlation_id = str(uuid4())
        self.retry_at = retry_at
        super().__init__(self.message)

    def to_dict(self) -> dict:
        return {"error": {"code": self.code, "message": self.message,
                          "correlation_id": self.correlation_id, "retry_at": self.retry_at}}


@dataclass(frozen=True)
class Client:
    id: str
    label: str
    enabled: bool = True


@dataclass(frozen=True)
class Scope:
    id: str
    client_id: str
    external_id: str
    label: str
    enabled: bool = True
    revision: int = 1


@dataclass(frozen=True)
class Subscription:
    id: str
    scope_id: str
    query: str
    exact: bool = False
    since: str | None = None
    enabled: bool = True
    tombstone: bool = False
    revision: int = 1


@dataclass(frozen=True)
class AcquisitionRequest:
    id: str
    scope_id: str
    issue_id: str
    origin: str = "explicit"
    subscription_id: str | None = None
    state: RequestState = RequestState.QUEUED
    revision: int = 1
    physical_status: str = "pending"
    failure_kind: str | None = None
    retryable: bool = False
    next_action: str | None = None
    next_retry_at: str | None = None
    fulfillment_error: str | None = None
    cancellation_reason: str | None = None
    created_at: str = ""


@dataclass(frozen=True)
class Operation:
    id: str
    kind: str
    state: OperationState = OperationState.QUEUED
    resource_id: str | None = None
    result: dict = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class Delivery:
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
    title: str
    source: str = "freemagazines.top"
    year: int | None = None
    month: int | None = None
    state: DeliveryState = DeliveryState.READY
    media_type: str = "application/pdf"
    transfer: dict = field(default_factory=dict)
    receipt: dict | None = None


def serialize(value: Any) -> dict:
    return asdict(value)


# Used by schema and consumer-fixture contract checks.
ROUTES = {
    "/v1/info": ["GET"], "/v1/searches": ["POST"],
    "/v1/operations/{id}": ["GET"], "/v1/issues/{id}": ["GET"],
    "/v1/scopes": ["GET", "POST"], "/v1/scopes/{id}": ["PATCH"],
    "/v1/scopes/{id}/subscriptions": ["GET", "POST"],
    "/v1/subscriptions/{id}": ["PATCH", "DELETE"],
    "/v1/scopes/{id}/requests": ["GET", "POST"],
    "/v1/requests/{id}": ["GET"], "/v1/requests/{id}/retry": ["POST"],
    "/v1/requests/{id}/cancel": ["POST"], "/v1/events": ["GET"],
    "/v1/snapshots": ["POST"], "/v1/snapshots/{id}": ["GET"],
    "/v1/deliveries/{id}": ["GET"], "/v1/deliveries/{id}/content": ["GET"],
    "/v1/deliveries/{id}/ack": ["PUT"],
    "/health/live": ["GET"], "/health/ready": ["GET"],
}
