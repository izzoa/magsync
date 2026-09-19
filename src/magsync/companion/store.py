"""Transactional companion repository and client-scoped domain operations."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from .safety import public_text
from magsync.core.matching import (
    MATCHER_VERSION,
    canonical_issue_title,
    compile_subscription,
    matches_subscription,
)
from magsync.core.models import Subscription as LocalSubscription

from . import PROTOCOL_VERSION
from .protocol import ProtocolError
from .schema import LOCAL_CLIENT, LOCAL_SCOPE


def uid() -> str:
    return str(uuid4())


def timestamp(offset: float = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=offset)).isoformat()


def json_text(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass
class Limits:
    page_default: int = 100
    page_max: int = 500
    body_bytes: int = 16384
    query_length: int = 256
    source_pages: int = 5
    pending_requests: int = 10000
    queue_depth: int = 1000
    commands_per_minute: int = 120
    snapshots_per_client: int = 3
    snapshot_seconds: int = 3600
    event_seconds: int = 30 * 86400
    idempotency_seconds: int = 30 * 86400
    export_grace_seconds: int = 7 * 86400
    export_bytes: int = 100 * 1024**3
    minimum_free_bytes: int = 1024**3
    maximum_download_bytes: int = 1024**3
    command_poll_seconds: float = 1
    heartbeat_seconds: float = 5
    heartbeat_stale_seconds: float = 30
    shutdown_seconds: float = 60
    command_concurrency: int = 4
    lock_wait_seconds: float = 60


class Store:
    def __init__(self, index, limits: Limits | None = None):
        self.index = index
        self.conn = index.conn
        self.limits = limits or Limits()

    @contextmanager
    def transaction(self):
        # SAVEPOINT permits domain methods to compose into one acceptance commit.
        nested = self.conn.in_transaction
        name = "sp_" + uuid4().hex
        self.conn.execute(f"SAVEPOINT {name}" if nested else "BEGIN IMMEDIATE")
        try:
            yield
            self.conn.execute(f"RELEASE {name}") if nested else self.conn.commit()
        except BaseException:
            if nested:
                self.conn.execute(f"ROLLBACK TO {name}")
                self.conn.execute(f"RELEASE {name}")
            else:
                self.conn.rollback()
            raise

    def identity(self) -> dict:
        row = self.conn.execute("SELECT * FROM service_identity WHERE id=1").fetchone()
        if not row:
            raise ProtocolError("store_uninitialized")
        return dict(row)

    def initialize(self) -> dict:
        """Explicit operator action, performed while holding runtime ownership."""
        with self.transaction():
            if not self.conn.execute("SELECT 1 FROM service_identity").fetchone():
                self.conn.execute("INSERT INTO service_identity VALUES(1,?,?,?,?,?)",
                                  (uid(), uid(), secrets.token_hex(32), PROTOCOL_VERSION, timestamp()))
        identity = self.identity()
        marker = Path(str(self.index.db_path) + ".identity.json")
        if marker.exists():
            try:
                saved = json.loads(marker.read_text())
            except (OSError, ValueError):
                raise ProtocolError("store_uninitialized") from None
            if saved.get("instance_id") != identity["instance_id"]:
                raise ProtocolError("store_uninitialized")
        else:
            temporary = marker.with_name(marker.name + "." + uuid4().hex + ".tmp")
            try:
                with temporary.open("x") as stream:
                    os.chmod(temporary, 0o600)
                    stream.write(json_text({"instance_id": identity["instance_id"]}))
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, marker)
                fd = os.open(marker.parent, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            finally:
                temporary.unlink(missing_ok=True)
        return {k: identity[k] for k in ("instance_id", "recovery_epoch", "protocol_version")}

    def check_identity(self) -> dict:
        identity = self.identity()
        try:
            marker = json.loads(Path(str(self.index.db_path) + ".identity.json").read_text())
        except (OSError, ValueError):
            raise ProtocolError("store_uninitialized") from None
        if marker.get("instance_id") != identity["instance_id"]:
            raise ProtocolError("store_uninitialized")
        return identity

    def client(self, client_id: str) -> dict:
        row = self.conn.execute("SELECT id,label,enabled FROM clients WHERE id=?", (client_id,)).fetchone()
        if not row or not row["enabled"]:
            raise ProtocolError("unauthorized")
        return dict(row)

    def provision(self, label: str, *, client_id: str | None = None, revoke_old: bool = False) -> dict:
        with self.transaction():
            if client_id is None:
                client_id = uid()
                self.conn.execute("INSERT INTO clients(id,label) VALUES (?,?)", (client_id, label))
            else:
                self.client(client_id)
                if client_id == LOCAL_CLIENT:
                    raise ProtocolError("not_found")
            if revoke_old:
                self.conn.execute("UPDATE credentials SET revoked=1 WHERE client_id=?", (client_id,))
            key_id, secret = uid(), secrets.token_urlsafe(48)
            verifier = hashlib.sha256(secret.encode()).hexdigest()
            self.conn.execute("INSERT INTO credentials(id,client_id,verifier,created_at) VALUES (?,?,?,?)",
                              (key_id, client_id, verifier, timestamp()))
        return {"client_id": client_id, "key_id": key_id, "token": f"ms1.{key_id}.{secret}"}

    def authenticate(self, token: str) -> str:
        parts = token.split(".")
        if len(parts) != 3 or parts[0] != "ms1" or len(token) > 256:
            raise ProtocolError("unauthorized")
        row = self.conn.execute("SELECT * FROM credentials WHERE id=? AND revoked=0", (parts[1],)).fetchone()
        verifier = hashlib.sha256(parts[2].encode()).hexdigest()
        if not hmac.compare_digest(verifier, row["verifier"] if row else "0" * 64):
            raise ProtocolError("unauthorized")
        self.client(row["client_id"])
        return row["client_id"]

    def revoke(self, key_id: str) -> None:
        with self.transaction():
            if not self.conn.execute("UPDATE credentials SET revoked=1 WHERE id=?", (key_id,)).rowcount:
                raise ProtocolError("not_found")

    def enable_client(self, client_id: str, enabled: bool) -> None:
        if client_id == LOCAL_CLIENT:
            raise ProtocolError("not_found")
        with self.transaction():
            if not self.conn.execute("UPDATE clients SET enabled=? WHERE id=?", (enabled, client_id)).rowcount:
                raise ProtocolError("not_found")
            self.reconcile_requests(client_id=client_id)
            self.event(client_id, "client.updated", client_id, {"enabled": enabled})

    def event(self, client_id: str, kind: str, resource_id: str, payload) -> None:
        """Journal one committed transition for a remote principal.

        ``payload`` may be a zero-argument callable, evaluated only when an
        event is actually written. The reserved local principal has no event
        reader (remote clients can never address it), so nothing is journaled
        for it and its payloads are never built.
        """
        if not self.conn.in_transaction:
            raise RuntimeError("events require their state transaction")
        if client_id == LOCAL_CLIENT:
            return
        if callable(payload):
            payload = payload()
        seq = self.conn.execute("UPDATE clients SET event_seq=event_seq+1 WHERE id=? RETURNING event_seq",
                                (client_id,)).fetchone()[0]
        self.conn.execute("INSERT INTO client_events VALUES(?,?,?,?,?,?,?)",
                          (client_id, seq, uid(), kind, resource_id, json_text(payload), timestamp()))

    @staticmethod
    def revision(row, revision: int | None) -> None:
        if revision is None or row["revision"] != revision:
            raise ProtocolError("revision_conflict")

    def scope(self, client_id: str, scope_id: str, *, enabled: bool = False) -> dict:
        self.client(client_id)
        row = self.conn.execute("SELECT * FROM scopes WHERE id=? AND client_id=?", (scope_id, client_id)).fetchone()
        if not row:
            raise ProtocolError("not_found")
        if enabled and not row["enabled"]:
            raise ProtocolError("scope_disabled")
        return dict(row)

    def create_scope(self, client_id: str, body: dict) -> dict:
        with self.transaction():
            self.client(client_id)
            scope_id = uid()
            try:
                self.conn.execute("INSERT INTO scopes(id,client_id,external_id,label) VALUES (?,?,?,?)",
                                  (scope_id, client_id, body["external_id"], body["label"]))
            except sqlite3.IntegrityError:
                raise ProtocolError("revision_conflict") from None
            result = self.scope(client_id, scope_id)
            self.event(client_id, "scope.created", scope_id, result)
            return result

    def update_scope(self, client_id: str, scope_id: str, body: dict, revision: int) -> dict:
        with self.transaction():
            row = self.scope(client_id, scope_id)
            self.revision(row, revision)
            self.conn.execute("UPDATE scopes SET label=?,enabled=?,revision=revision+1 WHERE id=?",
                              (body.get("label", row["label"]), body.get("enabled", row["enabled"]), scope_id))
            self.reconcile_requests(scope_id=scope_id)
            result = self.scope(client_id, scope_id)
            self.event(client_id, "scope.updated", scope_id, result)
            return result

    def subscription(self, client_id: str, subscription_id: str) -> dict:
        row = self.conn.execute("""SELECT s.* FROM subscriptions s JOIN scopes sc ON sc.id=s.scope_id
                                 WHERE s.id=? AND sc.client_id=?""", (subscription_id, client_id)).fetchone()
        if not row:
            raise ProtocolError("not_found")
        self.scope(client_id, row["scope_id"])
        return {k: row[k] for k in row.keys() if k != "local_key"}

    def create_subscription(self, client_id: str, scope_id: str, body: dict) -> dict:
        with self.transaction():
            self.scope(client_id, scope_id, enabled=True)
            subscription_id = uid()
            self.conn.execute("""INSERT INTO subscriptions(id,scope_id,query,exact,since,enabled)
                                 VALUES (?,?,?,?,?,?)""", (subscription_id, scope_id, body["query"],
                                 body.get("exact", False), body.get("since"), body.get("enabled", True)))
            result = self.subscription(client_id, subscription_id)
            self.event(client_id, "subscription.created", subscription_id, result)
            self.materialize(subscription_id)
            return result

    def update_subscription(self, client_id: str, subscription_id: str, body: dict,
                            revision: int, *, delete: bool = False) -> dict:
        with self.transaction():
            row = self.subscription(client_id, subscription_id)
            self.revision(row, revision)
            self.conn.execute("""UPDATE subscriptions SET query=?,exact=?,since=?,enabled=?,
                                 tombstone=?,revision=revision+1 WHERE id=?""",
                              (body.get("query", row["query"]), body.get("exact", row["exact"]),
                               body.get("since", row["since"]), False if delete else body.get("enabled", row["enabled"]),
                               delete or row["tombstone"], subscription_id))
            if delete:
                # A later un-tombstone must re-evaluate the whole catalog so
                # withdrawn subscription-origin requests can be revived.
                self.conn.execute("DELETE FROM companion_materialized WHERE subscription_id=?", (subscription_id,))
            self.reconcile_requests(scope_id=row["scope_id"])
            self.materialize(subscription_id)
            result = self.subscription(client_id, subscription_id)
            self.event(client_id, "subscription.updated", subscription_id, result)
            return result

    def provider_issue(self, issue_id: int) -> str:
        row = self.conn.execute("SELECT id FROM provider_issues WHERE issue_id=?", (issue_id,)).fetchone()
        if row:
            return row[0]
        with self.transaction():
            self.conn.execute("INSERT OR IGNORE INTO provider_issues VALUES(?,?)", (uid(), issue_id))
            return self.conn.execute("SELECT id FROM provider_issues WHERE issue_id=?", (issue_id,)).fetchone()[0]

    def issue(self, public_id: str) -> dict:
        row = self.conn.execute("""SELECT p.id,i.title,i.year,i.month,i.genre FROM provider_issues p
                                 JOIN issues i ON i.id=p.issue_id WHERE p.id=?""", (public_id,)).fetchone()
        if not row:
            raise ProtocolError("not_found")
        result = dict(row)
        result["title"] = public_text(result["title"], 512)
        result["genre"] = public_text(result["genre"], 128) if result["genre"] else None
        result["source"] = "freemagazines.top"
        return result

    def internal_issue(self, public_id: str) -> int:
        row = self.conn.execute("SELECT issue_id FROM provider_issues WHERE id=?", (public_id,)).fetchone()
        if not row:
            raise ProtocolError("not_found")
        return row[0]

    def pending_count(self, client_id: str) -> int:
        return self.conn.execute("""SELECT count(*) FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id
            WHERE s.client_id=? AND r.state IN ('queued','acquiring','suspended')""", (client_id,)).fetchone()[0]

    def _new_request(self, scope_id: str, issue_id: int, subscription_id: str | None = None,
                     *, check_capacity: bool = True) -> str:
        scope = self.conn.execute("SELECT * FROM scopes WHERE id=?", (scope_id,)).fetchone()
        client_id = scope["client_id"]
        # The pending cap is remote admission control; local intent is exempt.
        if check_capacity and client_id != LOCAL_CLIENT and self.pending_count(client_id) >= self.limits.pending_requests:
            raise ProtocolError("capacity_exhausted")
        request_id = uid()
        self.conn.execute("""INSERT INTO acquisition_requests(id,scope_id,issue_id,subscription_id,origin,created_at)
                             VALUES (?,?,?,?,?,?)""", (request_id, scope_id, issue_id, subscription_id,
                             "subscription" if subscription_id else "explicit", timestamp()))
        self.provider_issue(issue_id)
        self.event(client_id, "request.created", request_id, lambda: self.request(client_id, request_id))
        return request_id

    def create_request(self, client_id: str, scope_id: str, body: dict) -> dict:
        with self.transaction():
            self.scope(client_id, scope_id, enabled=True)
            request_id = self._new_request(scope_id, self.internal_issue(body["issue_id"]))
            return self.request(client_id, request_id)

    def request(self, client_id: str, request_id: str) -> dict:
        row = self.conn.execute("""SELECT r.*,d.status physical_status,d.last_error_kind failure_kind,
                d.next_action,d.next_retry_at,p.id public_issue_id FROM acquisition_requests r
                JOIN scopes s ON s.id=r.scope_id JOIN downloads d ON d.issue_id=r.issue_id
                JOIN provider_issues p ON p.issue_id=r.issue_id WHERE r.id=? AND s.client_id=?""",
                (request_id, client_id)).fetchone()
        if not row:
            raise ProtocolError("not_found")
        self.scope(client_id, row["scope_id"])
        result = dict(row)
        result["issue_id"] = result.pop("public_issue_id")
        result["retryable"] = row["physical_status"] in ("failed", "unavailable") and row["state"] != "canceled"
        return result

    def eligible(self, request_id: str) -> bool:
        row = self.conn.execute("""SELECT r.state,r.subscription_id,s.enabled scope_enabled,c.enabled client_enabled,
                i.title,i.year,i.month FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id
                JOIN clients c ON c.id=s.client_id JOIN issues i ON i.id=r.issue_id WHERE r.id=?""",
                (request_id,)).fetchone()
        if not row or row["state"] == "canceled" or not row["scope_enabled"] or not row["client_enabled"]:
            return False
        if row["subscription_id"] and row["state"] != "fulfilled":
            sub = self.conn.execute("SELECT * FROM subscriptions WHERE id=?", (row["subscription_id"],)).fetchone()
            return bool(sub and sub["enabled"] and not sub["tombstone"] and
                        matches_subscription(dict(row), SimpleNamespace(**dict(sub))))
        return True

    def remote_wanted(self, issue_id: int) -> bool:
        """True when an eligible request from a remote client needs this issue."""
        rows = self.conn.execute("""SELECT r.id FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id
            WHERE r.issue_id=? AND s.client_id!=? AND r.state IN ('queued','acquiring','suspended')""",
            (issue_id, LOCAL_CLIENT)).fetchall()
        return any(self.eligible(row[0]) for row in rows)

    def issue_wanted(self, issue_id: int, *, scope_id: str | None = None) -> bool:
        sql = "SELECT id FROM acquisition_requests WHERE issue_id=? AND state IN ('queued','acquiring','suspended')"
        args = [issue_id]
        if scope_id:
            sql += " AND scope_id=?"
            args.append(scope_id)
        return any(self.eligible(row[0]) for row in self.conn.execute(sql, args))

    def reconcile_requests(self, *, scope_id: str | None = None, client_id: str | None = None) -> None:
        """Suspend, resume or withdraw unfulfilled requests to match current state.

        One joined query supplies each request with its scope/client state,
        subscription definition and issue fields; subscription matching uses
        compiled matchers over cached canonical titles.
        """
        with self.transaction():
            sql = """SELECT r.id, r.state, r.revision, r.scope_id, r.issue_id, r.subscription_id,
                            s.client_id, c.enabled client_enabled, s.enabled scope_enabled,
                            sub.query sub_query, sub.exact sub_exact, sub.since sub_since,
                            sub.enabled sub_enabled, sub.tombstone sub_tombstone,
                            i.title, i.year, i.month
                     FROM acquisition_requests r JOIN scopes s ON s.id=r.scope_id JOIN clients c ON c.id=s.client_id
                     JOIN issues i ON i.id=r.issue_id LEFT JOIN subscriptions sub ON sub.id=r.subscription_id
                     WHERE r.state NOT IN ('canceled','fulfilled')"""
            args = []
            if scope_id:
                sql += " AND r.scope_id=?"
                args.append(scope_id)
            if client_id:
                sql += " AND s.client_id=?"
                args.append(client_id)
            compiled = {}
            for row in self.conn.execute(sql, args).fetchall():
                state = row["state"]
                if not row["scope_enabled"] or not row["client_enabled"]:
                    state = "suspended"
                elif row["subscription_id"]:
                    matcher = compiled.get(row["subscription_id"])
                    if matcher is None:
                        matcher = compiled[row["subscription_id"]] = compile_subscription(SimpleNamespace(
                            query=row["sub_query"], exact=row["sub_exact"], since=row["sub_since"]))
                    if row["sub_tombstone"] or not matcher.matches(canonical_issue_title(row["title"]), row["year"], row["month"]):
                        state = "canceled"
                    elif not row["sub_enabled"]:
                        state = "suspended"
                    elif state == "suspended":
                        state = "queued"
                elif state == "suspended":
                    state = "queued"
                if state != row["state"]:
                    self.conn.execute("UPDATE acquisition_requests SET state=?,cancellation_reason=?,revision=revision+1 WHERE id=?",
                                      (state, 'subscription' if state == 'canceled' else None, row["id"]))
                    self.event(row["client_id"], "request.updated", row["id"],
                               {"id": row["id"], "scope_id": row['scope_id'], "state": state, "revision": row["revision"] + 1})

    @staticmethod
    def _fingerprint(sub) -> str:
        return hashlib.sha256(json_text([MATCHER_VERSION, sub["query"], bool(sub["exact"]), sub["since"]]).encode()).hexdigest()

    def materialize(self, subscription_id: str | None = None, *, local: bool | None = None,
                    skipped: list | None = None) -> int:
        """Create subscription-origin requests for matching issues, incrementally.

        Each subscription evaluates only issues added since its stored
        watermark; a changed definition (query/exact/since/matcher version),
        a removed watermark (tombstone, title repair) or a new subscription
        evaluates the whole catalog. ``local`` limits the pass to the reserved
        local scope (True) or to remote scopes (False). A remote subscription
        whose client reached its pending-request limit is skipped without
        advancing its watermark (reported through ``skipped``) so one client
        can never block local or other clients' reconciliation.
        """
        with self.transaction():
            sql = """SELECT sub.*, s.client_id FROM subscriptions sub JOIN scopes s ON s.id=sub.scope_id
                     JOIN clients c ON c.id=s.client_id WHERE sub.enabled=1 AND sub.tombstone=0 AND s.enabled=1 AND c.enabled=1"""
            args: list = []
            if subscription_id:
                sql += " AND sub.id=?"
                args.append(subscription_id)
            if local is True:
                sql += " AND s.client_id=?"
                args.append(LOCAL_CLIENT)
            elif local is False:
                sql += " AND s.client_id!=?"
                args.append(LOCAL_CLIENT)
            subscriptions = self.conn.execute(sql + " ORDER BY sub.id", args).fetchall()
            if not subscriptions:
                return 0
            max_id = self.conn.execute("SELECT coalesce(max(id),0) FROM issues").fetchone()[0]
            titles: dict[int, str] = {}
            added = 0
            for sub in subscriptions:
                fingerprint = self._fingerprint(sub)
                mark = self.conn.execute("SELECT fingerprint,issue_watermark FROM companion_materialized WHERE subscription_id=?",
                                         (sub["id"],)).fetchone()
                since_id = mark["issue_watermark"] if mark and mark["fingerprint"] == fingerprint else 0
                if since_id < max_id:
                    if sub["client_id"] == LOCAL_CLIENT:
                        added += self._materialize_one(sub, since_id, max_id, titles)
                    else:
                        try:
                            with self.transaction():
                                added += self._materialize_one(sub, since_id, max_id, titles)
                        except ProtocolError as exc:
                            if exc.code != "capacity_exhausted":
                                raise
                            if skipped is not None:
                                skipped.append(sub["id"])
                            continue
                self.conn.execute("""INSERT INTO companion_materialized VALUES(?,?,?)
                    ON CONFLICT(subscription_id) DO UPDATE SET fingerprint=excluded.fingerprint,
                    issue_watermark=excluded.issue_watermark""", (sub["id"], fingerprint, max_id))
            return added

    def _materialize_one(self, sub, since_id: int, max_id: int, titles: dict) -> int:
        matcher = compile_subscription(SimpleNamespace(query=sub["query"], exact=sub["exact"], since=sub["since"]))
        existing = {row["issue_id"]: row for row in self.conn.execute(
            "SELECT issue_id,id,state,cancellation_reason FROM acquisition_requests WHERE subscription_id=?", (sub["id"],))}
        remote = sub["client_id"] != LOCAL_CLIENT
        pending = self.pending_count(sub["client_id"]) if remote else 0
        added = 0
        for issue in self.conn.execute("SELECT id,title,year,month FROM issues WHERE id>? AND id<=? ORDER BY id",
                                       (since_id, max_id)).fetchall():
            title = titles.get(issue["id"])
            if title is None:
                title = titles[issue["id"]] = canonical_issue_title(issue["title"])
            if not matcher.matches(title, issue["year"], issue["month"]):
                continue
            old = existing.get(issue["id"])
            if old:
                # A relaxed floor may restore this same subscription origin;
                # an explicit cancellation is never revived.
                if old['state'] == 'canceled' and old['cancellation_reason'] == 'subscription':
                    self.conn.execute("UPDATE acquisition_requests SET state='queued',cancellation_reason=NULL,revision=revision+1 WHERE id=?", (old['id'],))
                    client = sub["client_id"]
                    self.event(client, 'request.updated', old['id'], lambda old=old: self.request(client, old['id']))
                continue
            if remote:
                if pending >= self.limits.pending_requests:
                    raise ProtocolError("capacity_exhausted")
                pending += 1
            self._new_request(sub["scope_id"], issue["id"], sub["id"], check_capacity=False)
            added += 1
        return added

    def migrate_local(self, subscriptions: list[LocalSubscription]) -> None:
        """Known manual provenance only; current subscriptions explicitly grant matching intent."""
        with self.transaction():
            for row in self.conn.execute("""SELECT d.issue_id FROM downloads d WHERE d.requested_by='manual'
                    AND NOT EXISTS(SELECT 1 FROM acquisition_requests r WHERE r.scope_id=? AND r.issue_id=d.issue_id
                    AND r.origin='explicit')""", (LOCAL_SCOPE,)).fetchall():
                self._new_request(LOCAL_SCOPE, row[0])
            self.reconcile_local(subscriptions)

    def reconcile_local(self, subscriptions: list[LocalSubscription]) -> None:
        with self.transaction():
            keys = set()
            for sub in subscriptions:
                key = hashlib.sha256(json_text([sub.query, sub.exact]).encode()).hexdigest()
                keys.add(key)
                row = self.conn.execute("SELECT * FROM subscriptions WHERE local_key=?", (key,)).fetchone()
                if row:
                    if row["since"] != sub.since or row["tombstone"]:
                        self.update_subscription(LOCAL_CLIENT, row["id"], {"since": sub.since, "enabled": True}, row["revision"])
                        self.conn.execute("UPDATE subscriptions SET tombstone=0 WHERE id=?", (row["id"],))
                        self.conn.execute("DELETE FROM companion_materialized WHERE subscription_id=?", (row["id"],))
                else:
                    result = self.create_subscription(LOCAL_CLIENT, LOCAL_SCOPE, asdict(sub))
                    self.conn.execute("UPDATE subscriptions SET local_key=? WHERE id=?", (key, result["id"]))
            for row in self.conn.execute("SELECT * FROM subscriptions WHERE scope_id=? AND tombstone=0", (LOCAL_SCOPE,)).fetchall():
                if row["local_key"] not in keys:
                    self.update_subscription(LOCAL_CLIENT, row["id"], {}, row["revision"], delete=True)
            self.materialize(local=True)
            self.reconcile_requests(scope_id=LOCAL_SCOPE)

    def cancel(self, client_id: str, request_id: str, revision: int) -> dict:
        with self.transaction():
            row = self.request(client_id, request_id)
            self.revision(row, revision)
            self.conn.execute("UPDATE acquisition_requests SET state='canceled',cancellation_reason='explicit',revision=revision+1 WHERE id=?", (request_id,))
            for delivery in self.conn.execute("SELECT id FROM deliveries WHERE request_id=? AND state='ready'", (request_id,)).fetchall():
                self.conn.execute("UPDATE deliveries SET state='canceled' WHERE id=?", (delivery[0],))
                self.event(client_id, "delivery.canceled", delivery[0], {"id": delivery[0], "state": "canceled"})
            result = self.request(client_id, request_id)
            self.event(client_id, "request.updated", request_id, result)
            return result

    def accept(self, client_id: str, kind: str, scope_id: str | None, body: dict, key: str,
               mutation=None) -> dict:
        if not key or len(key) > 128:
            raise ProtocolError("invalid_request")
        fingerprint = hashlib.sha256(json_text(body).encode()).hexdigest()
        with self.transaction():
            self.client(client_id)
            if scope_id:
                self.scope(client_id, scope_id, enabled=True)
            row = self.conn.execute("""SELECT * FROM idempotency WHERE client_id=? AND kind=? AND scope_key=? AND key=?""",
                                    (client_id, kind, scope_id or "", key)).fetchone()
            if row and row["expires_at"] > timestamp():
                if row["fingerprint"] != fingerprint:
                    raise ProtocolError("idempotency_conflict")
                return json.loads(row["response"])
            if client_id != LOCAL_CLIENT:
                # Remote admission control; a remote flood never locks out the operator.
                pending = self.conn.execute("SELECT count(*) FROM operations WHERE state IN ('queued','running')").fetchone()[0]
                if pending >= self.limits.queue_depth:
                    raise ProtocolError("capacity_exhausted")
                rate = self.conn.execute("SELECT count(*) FROM operations WHERE client_id=? AND created_at>?",
                                         (client_id, timestamp(-60))).fetchone()[0]
                if rate >= self.limits.commands_per_minute:
                    raise ProtocolError("rate_limited")
            resource = mutation() if mutation else None
            operation_id = uid()
            state = "succeeded" if kind in ("scope.create", "subscription.create", "snapshot.create") else "queued"
            self.conn.execute("""INSERT INTO operations(id,client_id,scope_id,kind,body,state,resource_id,created_at,updated_at)
                                 VALUES (?,?,?,?,?,?,?,?,?)""", (operation_id, client_id, scope_id, kind, json_text(body), state,
                                 resource.get("id") if resource else None, timestamp(), timestamp()))
            if resource and kind == "request.create":
                self.conn.execute("INSERT INTO operation_requests VALUES(?,?)", (operation_id, resource["id"]))
            response = {"operation_id": operation_id, "resource": resource}
            self.conn.execute("INSERT OR REPLACE INTO idempotency VALUES(?,?,?,?,?,?,?)",
                              (client_id, kind, scope_id or "", key, fingerprint, json_text(response),
                               timestamp(self.limits.idempotency_seconds)))
            return response

    def operation(self, client_id: str, operation_id: str) -> dict:
        self.client(client_id)
        row = self.conn.execute("SELECT * FROM operations WHERE id=? AND client_id=?", (operation_id, client_id)).fetchone()
        if not row:
            raise ProtocolError("not_found")
        document = {k: row[k] for k in ("id", "kind", "state", "resource_id", "created_at", "updated_at")}
        # Keys prefixed with "_" are private runtime bookkeeping (e.g. recovery counts).
        document["result"] = {k: v for k, v in json.loads(row["result"]).items() if not k.startswith("_")}
        return document

    def page_size(self, limit: int | None) -> int:
        limit = self.limits.page_default if limit is None else limit
        if not 1 <= limit <= self.limits.page_max:
            raise ProtocolError("invalid_request")
        return limit

    def list_resources(self, client_id: str, kind: str, *, scope_id: str | None = None,
                       after: str = "", limit: int | None = None) -> dict:
        self.client(client_id)
        size = self.page_size(limit)
        if scope_id:
            self.scope(client_id, scope_id)
        if kind == "scopes":
            rows = self.conn.execute("SELECT id FROM scopes WHERE client_id=? AND id>? ORDER BY id LIMIT ?",
                                     (client_id, after, size + 1)).fetchall()
            getter = lambda id: self.scope(client_id, id)
        elif kind in ("subscriptions", "requests") and scope_id:
            table = "subscriptions" if kind == "subscriptions" else "acquisition_requests"
            rows = self.conn.execute(f"SELECT id FROM {table} WHERE scope_id=? AND id>? ORDER BY id LIMIT ?",
                                     (scope_id, after, size + 1)).fetchall()
            getter = lambda id: self.subscription(client_id, id) if kind == "subscriptions" else self.request(client_id, id)
        else:
            raise ProtocolError("invalid_request")
        return {"items": [getter(row[0]) for row in rows[:size]],
                "next": rows[size - 1][0] if len(rows) > size else None}
