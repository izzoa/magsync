"""Additive transactional schema, without implicit service initialization."""
from __future__ import annotations

import sqlite3

from .protocol import ProtocolError

SCHEMA_VERSION = 1
LOCAL_CLIENT = "local"
LOCAL_SCOPE = "local"

DDL = """
CREATE TABLE IF NOT EXISTS companion_schema (version INTEGER NOT NULL);
CREATE TABLE service_identity (
 id INTEGER PRIMARY KEY CHECK(id=1), instance_id TEXT NOT NULL UNIQUE,
 recovery_epoch TEXT NOT NULL, cursor_secret TEXT NOT NULL,
 protocol_version TEXT NOT NULL, initialized_at TEXT NOT NULL
);
CREATE TABLE clients (
 id TEXT PRIMARY KEY, label TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
 event_seq INTEGER NOT NULL DEFAULT 0, event_floor INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE credentials (
 id TEXT PRIMARY KEY, client_id TEXT NOT NULL REFERENCES clients(id), verifier TEXT NOT NULL,
 revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1)), created_at TEXT NOT NULL
);
CREATE INDEX credentials_client ON credentials(client_id);
CREATE TABLE scopes (
 id TEXT PRIMARY KEY, client_id TEXT NOT NULL REFERENCES clients(id), external_id TEXT NOT NULL,
 label TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
 revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0), UNIQUE(client_id,external_id), UNIQUE(id,client_id)
);
CREATE TABLE subscriptions (
 id TEXT PRIMARY KEY, scope_id TEXT NOT NULL REFERENCES scopes(id), query TEXT NOT NULL,
 exact INTEGER NOT NULL DEFAULT 0 CHECK(exact IN (0,1)), since TEXT,
 enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)), tombstone INTEGER NOT NULL DEFAULT 0,
 revision INTEGER NOT NULL DEFAULT 1, local_key TEXT UNIQUE
);
CREATE INDEX subscriptions_scope ON subscriptions(scope_id,id);
CREATE TABLE provider_issues (
 id TEXT PRIMARY KEY, issue_id INTEGER NOT NULL UNIQUE REFERENCES issues(id)
);
CREATE TABLE acquisition_requests (
 id TEXT PRIMARY KEY, scope_id TEXT NOT NULL REFERENCES scopes(id), issue_id INTEGER NOT NULL REFERENCES issues(id),
 subscription_id TEXT REFERENCES subscriptions(id), origin TEXT NOT NULL CHECK(origin IN ('explicit','subscription')),
 state TEXT NOT NULL DEFAULT 'queued' CHECK(state IN ('queued','acquiring','fulfilled','canceled','suspended')),
 revision INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL,
 cancellation_reason TEXT, fulfillment_error TEXT,
 UNIQUE(subscription_id,issue_id),
 CHECK((origin='subscription' AND subscription_id IS NOT NULL) OR (origin='explicit' AND subscription_id IS NULL))
);
CREATE INDEX requests_issue_state ON acquisition_requests(issue_id,state);
CREATE INDEX requests_scope ON acquisition_requests(scope_id,id);
CREATE TABLE operations (
 id TEXT PRIMARY KEY, client_id TEXT NOT NULL REFERENCES clients(id), scope_id TEXT,
 kind TEXT NOT NULL, body TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
 resource_id TEXT, result TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 FOREIGN KEY(scope_id,client_id) REFERENCES scopes(id,client_id)
);
CREATE INDEX operations_queue ON operations(state,created_at,id);
CREATE INDEX operations_client ON operations(client_id,created_at,id);
CREATE TABLE operation_requests (
 operation_id TEXT NOT NULL REFERENCES operations(id), request_id TEXT NOT NULL REFERENCES acquisition_requests(id),
 PRIMARY KEY(operation_id,request_id)
);
CREATE TABLE idempotency (
 client_id TEXT NOT NULL REFERENCES clients(id), kind TEXT NOT NULL, scope_key TEXT NOT NULL,
 key TEXT NOT NULL, fingerprint TEXT NOT NULL, response TEXT NOT NULL, expires_at TEXT NOT NULL,
 PRIMARY KEY(client_id,kind,scope_key,key)
);
CREATE INDEX idempotency_expiry ON idempotency(expires_at);
CREATE TABLE runtime_state (
 id INTEGER PRIMARY KEY CHECK(id=1), generation INTEGER NOT NULL DEFAULT 0,
 owner_id TEXT, output_root TEXT, export_root TEXT, heartbeat_at TEXT, accepting INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE acquisition_attempts (
 id TEXT PRIMARY KEY, issue_id INTEGER NOT NULL REFERENCES issues(id), owner_id TEXT NOT NULL,
 generation INTEGER NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX attempts_active_issue ON acquisition_attempts(issue_id) WHERE state='running';
CREATE TABLE content_objects (
 id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, size INTEGER NOT NULL CHECK(size>0),
 state TEXT NOT NULL CHECK(state IN ('exporting','ready','unavailable','deleting')),
 relative_path TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL, unpinned_at TEXT
);
CREATE INDEX content_digest ON content_objects(sha256,size,state);
CREATE TABLE export_intents (
 id TEXT PRIMARY KEY, issue_id INTEGER NOT NULL REFERENCES issues(id), content_id TEXT NOT NULL REFERENCES content_objects(id),
 owner_id TEXT NOT NULL, generation INTEGER NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE deliveries (
 id TEXT PRIMARY KEY, request_id TEXT NOT NULL REFERENCES acquisition_requests(id),
 content_id TEXT NOT NULL REFERENCES content_objects(id), state TEXT NOT NULL DEFAULT 'ready',
 created_at TEXT NOT NULL, UNIQUE(request_id,content_id)
);
CREATE INDEX deliveries_content ON deliveries(content_id,state);
CREATE TABLE acknowledgments (
 delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id), receipt_id TEXT NOT NULL,
 sha256 TEXT NOT NULL, size INTEGER NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE transfer_leases (
 id TEXT PRIMARY KEY, content_id TEXT NOT NULL REFERENCES content_objects(id), owner_id TEXT NOT NULL,
 generation INTEGER NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX leases_content ON transfer_leases(content_id);
CREATE TABLE client_events (
 client_id TEXT NOT NULL REFERENCES clients(id), seq INTEGER NOT NULL,
 id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, resource_id TEXT NOT NULL, payload TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(client_id,seq)
);
CREATE INDEX events_retention ON client_events(created_at);
CREATE TABLE snapshots (
 id TEXT PRIMARY KEY, client_id TEXT NOT NULL REFERENCES clients(id), epoch TEXT NOT NULL,
 watermark INTEGER NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX snapshots_client ON snapshots(client_id,expires_at);
CREATE TABLE snapshot_items (
 snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE, ordinal INTEGER NOT NULL,
 payload TEXT NOT NULL, PRIMARY KEY(snapshot_id,ordinal)
);
"""

# Additive, idempotent tables created on every open. They extend version 1
# without a version bump, so stores initialized before they existed gain them.
ADDITIVE_DDL = """
CREATE TABLE IF NOT EXISTS companion_materialized (
 subscription_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, issue_watermark INTEGER NOT NULL
);
"""


def _additive(conn: sqlite3.Connection) -> None:
    for statement in ADDITIVE_DDL.split(";"):
        if statement.strip():
            conn.execute(statement)


def migrate(conn: sqlite3.Connection) -> None:
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE name='companion_schema'").fetchone()
    if exists:
        row = conn.execute("SELECT version FROM companion_schema").fetchone()
        if row is None or row[0] != SCHEMA_VERSION:
            raise ProtocolError("schema_incompatible")
        _additive(conn)
        return
    # executescript implicitly commits: execute statements individually instead.
    conn.execute("BEGIN IMMEDIATE")
    try:
        for statement in DDL.split(";"):
            if statement.strip():
                conn.execute(statement)
        _additive(conn)
        conn.execute("INSERT INTO companion_schema VALUES (?)", (SCHEMA_VERSION,))
        conn.execute("INSERT INTO clients(id,label) VALUES (?,?)", (LOCAL_CLIENT, "Local operator"))
        conn.execute("INSERT INTO scopes(id,client_id,external_id,label) VALUES (?,?,?,?)",
                     (LOCAL_SCOPE, LOCAL_CLIENT, LOCAL_SCOPE, "Local library"))
        conn.execute("INSERT INTO runtime_state(id) VALUES (1)")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
