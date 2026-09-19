# Companion service: protocol 1

MagSync supplies PDFs to a trusted consumer backend such as Polyreader. Polyreader owns people, library permissions, imported files, reading state, AI/TTS and OPDS. The companion API owns acquisition, isolated application scopes, immutable exports and delivery receipts. A service credential identifies an application, never an individual reader.

## Install and initialize

Stop existing writers and back up before upgrading. Install the same MagSync version in every CLI, TUI, daemon and service process sharing this database.

```sh
pip install -e '.[service]'
magsync companion init
magsync clients create Polyreader
magsync serve --host 127.0.0.1 --port 8765
```

Store the returned token securely: it is shown once. SQLite stores its verifier, not the token. Initialization is explicit and preserves existing catalog IDs, paths, hashes and retry times. Only known manual demand and currently matching local subscriptions migrate to the reserved `local` scope; catalog entries do not become remote requests. Service startup requires initialized state. An adjacent `index.db.identity.json` marker detects an accidentally missing database; back it up with the database.

Only one runtime owns a store/output/export root. The service includes discovery and downloads (`--interval` or `MAGSYNC_INTERVAL`, default `6h`); do not also start a daemon against it. Other local CLI/TUI processes submit durable local operations to that owner, which runs them beside discovery and downloads (at most `COMMAND_CONCURRENCY` at once) and returns the same results as standalone commands. With no runtime, a mutating terminal command takes temporary ownership. A temporary owner never accepts other commands: a second terminal command waits up to `LOCK_WAIT_SECONDS`, then reports that another command is running and queues nothing, and a starting daemon/service waits for it. A submitted terminal command stays queued only while its terminal waits; Ctrl-C, a hang-up or a terminated terminal withdraws it before it starts, and a local command interrupted mid-run is recorded as interrupted rather than replayed. A terminal keeps waiting while the owner is alive, even after the owner stops accepting new commands (a graceful stop drains running commands with the heartbeat still going). Remote API operations interrupted by an unclean stop are resumed at most three times. Advisory locks and generation fences prevent concurrent owners, including different databases targeting one output directory. SQLite must reside on a local filesystem on one host, not an NFS/SMB share.

Base installs require neither FastAPI nor Uvicorn. `magsync serve` gives an installation hint when the `service` extra is absent. Protocol version `1` is independent of package version `0.9.0`; clients should check `/v1/info` and capabilities before use.

## Docker

`docker-compose.example.yml` defaults to the daemon, with no API listener. Copy it to `docker-compose.yml` (or pass `-f docker-compose.example.yml` to every command below). Both targets run as `magsync` and persist `/config`, `/data`, `/magazines`, and `/exports`.

```sh
# Default daemon deployment:
docker compose up -d --build magsync

# Switch to the companion; explicitly select the service:
docker compose stop magsync
docker compose --profile companion build magsync-service
docker compose --profile companion run --rm --no-deps magsync-service magsync companion init
docker compose --profile companion run --rm --no-deps magsync-service magsync clients create Polyreader
docker compose --profile companion up -d --no-deps magsync-service
```

Do not use an unqualified `--profile companion up`: it selects the default daemon too. Competing owners fail their lock check. The service exposes port 8765 only inside the Compose network; there are **no host port mappings**. Attach the consumer backend to that private network and use `http://magsync-service:8765`. For access from the host, explicitly add `127.0.0.1:8765:8765` in a local override. Remote exposure requires your TLS reverse proxy. CORS/browser access is disabled by default.

Release automation builds daemon tags at `ghcr.io/izzoa/magsync` and optional service tags at `ghcr.io/izzoa/magsync-service`, for amd64 and arm64. Local build targets are `daemon` and `service`.

## Consumer flow

Use `Authorization: Bearer <token>` on every `/v1` request. Keep credentials out of URLs. Minimal `/health/live` and `/health/ready` probes are unauthenticated. Unsupported `X-MagSync-Protocol` values fail explicitly. The generated [OpenAPI schema](companion-openapi-v1.json) is also available at authenticated `/v1/openapi.json`. Regenerate it with `python scripts/export_companion_schema.py` after installing the service extra.

The examples use `BASE`, `TOKEN`, `SCOPE`, `ISSUE`, and `DELIVERY` values obtained from earlier responses. `curl` and `jq` are client-side example tools, not server dependencies.

```sh
BASE=http://127.0.0.1:8765
# Supply TOKEN securely from your application secret store.
curl --fail -H "Authorization: Bearer $TOKEN" "$BASE/v1/info"

curl --fail -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: register-library-a' \
  -d '{"external_id":"library-a","label":"Library A"}' "$BASE/v1/scopes"

# Use resource.id from the previous response as SCOPE.
curl --fail -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: search-science-1' \
  -d '{"query":"Science News","pages":1}' "$BASE/v1/searches"
# Poll /v1/operations/{operation_id}; use result.items[].id as ISSUE.
# Search indexes metadata and creates no acquisition demand.

curl --fail -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: request-issue-1' \
  -d "{\"issue_id\":\"$ISSUE\"}" "$BASE/v1/scopes/$SCOPE/requests"

curl --fail -H "Authorization: Bearer $TOKEN" "$BASE/v1/events?limit=100"
# Apply events, persist their cursor after applying the page, then poll with it.
# delivery.ready carries the delivery ID, generation, SHA-256, size and transfer.http.
curl --fail -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/deliveries/$DELIVERY/content" -o imported.part
```

Verify SHA-256 and exact byte count against delivery metadata. Atomically import into the consumer's own managed storage before acknowledgment. Deduplicate bytes by instance/issue/content generation; record each request's library membership and its own receipt. Only `application/pdf` is supported. An incomplete or mismatched original never becomes a ready delivery.

```sh
# Fill these with the verified metadata and a stable managed-import receipt ID.
curl --fail -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"receipt_id":"managed-import-123","sha256":"<64 lowercase hex digits>","size":12345}' \
  "$BASE/v1/deliveries/$DELIVERY/ack"
```

Every acquisition request gets its own delivery even when two scopes share one physical download and one export object. Acknowledging one never acknowledges another. Duplicate matching receipts succeed; changed receipt IDs/hash/size conflict. A receipt after cancellation records historical import without restoring access. MagSync never deletes a consumer's imported copy.

### Subscriptions and control

Create subscriptions under `/v1/scopes/{id}/subscriptions` with `query`, `exact`, optional `since` (`YYYY-MM`), and `enabled`. The same query in different scopes retains independent date floors. Explicit requests survive subscription removal. Editing/unsubscribing withdraws only the affected unfulfilled subscription origin. Disabling a scope suspends future execution/retrieval while keeping its stable IDs and outstanding export pins.

Use the current integer revision as `If-Match` for scope/subscription edits, deletes, request cancel and retry. Stale revisions yield `409 revision_conflict`. Retrying an eligible request overrides its download schedule, can attach to an existing transfer, and never drains unrelated requests. `unsupported` payloads stay parked. Canceling the last demand before publication prevents a delivery; another eligible scope may still receive its own.

Work-creating POSTs require `Idempotency-Key`. Persist a stable key across lost responses. Replays with the same principal/type/scope/key and normalized body return the original acceptance. Different bodies yield `409 idempotency_conflict`. Retry bodies include the expected revision, so replay the original revision after a lost reply. Keys expire after the advertised retention window; an expired key creates a **new command**, so reconcile stable resource IDs first.

Accepted commands are durable before `202`. Poll their operation IDs. A request or retry for an issue that is already transferring completes with that transfer's outcome; acquisition results carry one outcome per selected issue. States include queued, running, succeeded, empty, partial, blocked, failed and suspended. Search reports validated emptiness separately from source failure/blocking. Results are bounded: operations use `offset`/`limit` and `next_offset`; resource lists use `after`/`limit` and `next`.

Errors use `{"error":{"code","message","correlation_id","retry_at"}}` with fixed safe messages. Credentials fail with 401; another client's resources return 404; conflicts use 409; expired recovery tokens use 410; admission/capacity uses 429; unavailable local runtime uses 503. Neither raw source secrets nor filesystem paths are API fields.

### Reconnection and content ranges

Event cursors are opaque and bound to the application, instance, recovery epoch and scope filter. Retry a dropped response with the same cursor. Apply stable event IDs in sequence; filtering cannot grant another scope's data. Preserve any `scope` parameter when resuming its cursor.

On `410 resync_required`, POST `/v1/snapshots` with a new idempotency key. Apply `resource.items`, then GET `/v1/snapshots/{resource.id}?cursor=...` until `next` is null. The pages are a fixed materialized state, including unacknowledged/unavailable deliveries. Resume events with `handoff_cursor`; changes committed during pagination appear there. Snapshot expiry requires a new snapshot, never a mixture of generations. A coordinated restore changes `recovery_epoch` but preserves `instance_id`.

For interrupted downloads send `Range: bytes=<verified-prefix-length>-` and `If-Range: <saved ETag>`. Recheck the full digest before importing; discard a prefix when the generation/ETag changes. The server supports one byte range, immutable generation ETags, `If-None-Match`, and 416 with total length. Active streams hold a read lease so normal cleanup cannot remove their object; the lease ends with the response, including when the client disconnects before the first byte.

## Capacity and retention

Settings use `MAGSYNC_SERVICE__` plus the uppercase name below. Values are positive; byte values are integers. `/v1/info` advertises the configured client-facing limits and retention. Set `MAGSYNC_EXPORT_DIR` to the export root (default: `exports` beside the database).

| Setting | Default | Purpose |
|---|---:|---|
| `PAGE_DEFAULT`, `PAGE_MAX` | 100, 500 | Resource/event/snapshot pages |
| `BODY_BYTES`, `QUERY_LENGTH`, `SOURCE_PAGES` | 16384, 256, 5 | HTTP admission/search bounds |
| `PENDING_REQUESTS` | 10000 | Outstanding requests per client |
| `QUEUE_DEPTH`, `COMMANDS_PER_MINUTE` | 1000, 120 | Global work queue and per-client command admission |
| `SNAPSHOTS_PER_CLIENT`, `SNAPSHOT_SECONDS` | 3, 3600 | Concurrent retained snapshots and page lifetime |
| `EVENT_SECONDS`, `IDEMPOTENCY_SECONDS` | 2592000 each | Replay/key retention, 30 days |
| `EXPORT_GRACE_SECONDS` | 604800 | Seven days after the last pin clears |
| `EXPORT_BYTES` | 107374182400 | 100 GiB export budget |
| `MINIMUM_FREE_BYTES` | 1073741824 | 1 GiB free-space reserve |
| `MAXIMUM_DOWNLOAD_BYTES` | 1073741824 | Maximum size of each unknown-size transfer, 1 GiB |
| `COMMAND_POLL_SECONDS` | 1 | Independent of six-hour discovery |
| `HEARTBEAT_SECONDS`, `HEARTBEAT_STALE_SECONDS` | 5, 30 | Responsive event-loop heartbeat; stale must exceed cadence |
| `SHUTDOWN_SECONDS` | 60 | In-flight grace period |
| `COMMAND_CONCURRENCY` | 4 | Queued commands executed at once, beside discovery and downloads |
| `LOCK_WAIT_SECONDS` | 60 | How long a terminal command waits for another terminal command |

Exports exist only for remote demand: an issue wanted only by local subscriptions or terminal requests is fulfilled by the organized file itself, with no export copy. Transfers that will be exported reserve up to three maximum-size copies per concurrent transfer when originals and exports share a filesystem (encrypted input, original, export), plus minimum free space; separate filesystems reserve independently, so three concurrent 1 GiB transfers can require 10 GiB free on shared storage. Local-only transfers reserve one maximum-size transfer plus minimum free space on the output filesystem and never depend on export capacity. Capacity is decided per issue, so an issue waiting for export space never blocks others. One task at a time publishes an issue, and its requests stay `acquiring` until publication fulfills them or returns them to `queued`. Tune the maximum/concurrency to your expected magazines and storage. Streaming transfers exceeding the maximum fail with a typed configuration error; increase the limit and explicitly retry. Capacity exhaustion queues work and resumes when capacity returns; it never evicts unacknowledged bytes.

Acknowledgment does not remove the organized original. When a remote request needs an issue whose organized original was deleted, the issue is downloaded again; an original that exists but fails verification is recorded as `integrity_failed` and retried once per discovery cycle instead of being re-downloaded. Unacknowledged deliveries and active read leases pin exports. Disabled clients/scopes preserve outstanding pins. Event/key/snapshot expiry is independent of delivery retention. Purging/missing/corrupt content becomes visibly unavailable; existing delivery IDs are never rebound to different bytes. Request again after restoring/reacquiring content to obtain a new delivery generation.

The heartbeat tracks a responsive runtime, not upstream success. `/health/ready` checks usable local state; authenticated info reports pipeline degradation and capacity pauses. A blocked source stays live and retains its circuit until an allowed cycle. Container health checks run every 10 seconds with a 30-second startup period. Set the container stop grace above `SHUTDOWN_SECONDS` (Compose service default: 70 seconds).

## Operator commands and trusted mounts

```sh
magsync companion status
magsync clients list
magsync clients rotate CLIENT_ID --revoke-old
magsync clients revoke KEY_ID
magsync clients disable CLIENT_ID
magsync clients enable CLIENT_ID
# Stop the runtime before these exclusive maintenance operations:
magsync companion purge CONTENT_GENERATION
magsync companion recover
```

Status reports provider identity, owner heartbeat, queued work, per-client outstanding references and retained export bytes. Rotation emits only the newly issued secret; listings/status never include token verifiers. `purge` explicitly removes the named generation even when unacknowledged (active leases still prevent deletion). Its deliveries remain as unavailable history. `recover` rotates the epoch and verifies export objects after a coordinated restore.

HTTP is the required transport. Optional mount transfer requires both `MAGSYNC_TRUSTED_MOUNTS=1` and `MAGSYNC_CLIENT_EXPORT_VIEWS='{"CLIENT_ID":"/client-views/polyreader"}'`. Configure **disjoint dedicated directories**, outside the object store, and mount only that client's directory read-only into the trusted backend. Never mount the entire `/exports` store into a consumer. Example consumer volume: `polyreader_view:/magsync-inbox:ro`; the service mounts the same volume writable at its configured view path. `trusted_client_mount` is advertised only for the configured application; delivery metadata includes a relative `DELIVERY_ID.pdf` reference once the view exists. Verify hash/size before import. Canceled/disabled entries are removed on reconciliation. Revocation cannot retract a copy or open filesystem handle already held by a trusted recipient.

Local file subscriptions feed only the reserved local scope; nonempty `MAGSYNC_SUBSCRIPTIONS` replaces only that local list. Remote subscriptions remain in SQLite. If an existing configuration file becomes unreadable or disappears, the last persisted local snapshot is retained. File writes merge changed fields and report same-field/external/environment/read-only conflicts; the file is replaced atomically, or rewritten in place under the same lock when it is a single-file mount. Writers lock `config.toml` itself, so a host process and a container that mounts only the file still take turns. Local retry requires current local demand; remote-only and lapsed-only failures are excluded. Commands handled by an active owner print the same results as standalone commands. `fetch --dry-run` and `daemon --dry-run` list work from a private **cached catalog snapshot**, without source traffic or new intent.

## Upgrade, backup, restore and rollback

1. Stop every daemon/service, CLI and TUI writer. Record the installed version and all storage roots. Do not run an older binary against a migrated store; older versions cannot recognize scoped demand.
2. Back up configuration, the database **with its adjacent identity marker**, all original files and immutable exports as one coordinated set. With all SQLite connections closed, its checkpointed database is safe to copy. For a live database use SQLite's backup API, but still stop acquisition while coordinating the file copies. Never copy only the `.db` file while a live WAL contains writes.
3. Upgrade all writers together. Run `companion init` once for the first companion deployment; it preserves an already initialized identity. Start one runtime, check readiness/info, then reconnect the consumer.
4. To restore a companion backup, stop all writers again and replace the complete matched set at the **same configured paths**. Remove stale `-wal` and `-shm` sidecars only after all writers have stopped and the replacement database is installed. Run `magsync companion recover` before exposure. This preserves instance identity, rotates epoch and verifies exports; clients must snapshot and can replay matching receipts for their already imported files.
5. To roll back the binary, restore its matching **pre-upgrade** database/configuration/files together. Never point an old binary at the new schema. Consumer-owned imports remain independent and must not be deleted during provider rollback.

For Compose named volumes, an offline archive can be made with the service image; no service is started by the overridden Python command:

```sh
docker compose stop magsync magsync-service
mkdir -p backups
docker compose --profile companion run --rm --no-deps -v "$PWD/backups:/backup" \
  magsync-service python -c 'import tarfile; t=tarfile.open("/backup/magsync.tar.gz","w:gz"); [t.add("/"+p,arcname=p) for p in ("config","data","magazines","exports")]; t.close()'
```

Ensure the non-root service UID can write the backup directory. Keep archives private: they include credential verifiers and your PDFs. Restore the archive into empty volumes mounted at those same four paths, then run `companion recover`. Do not use `docker compose down -v` to replace a container: that deletes its persisted state. Ordinary stop/remove/recreate with the same volumes requires no recovery-epoch change.

## Polyreader compatibility matrix

| Polyreader integration need | Protocol 1 support |
|---|---|
| Optional integration | Optional service extra/profile; version/capability discovery; terminal use remains available |
| One source for managed libraries | Client-owned stable scopes and independent query/exact/since settings |
| Authorized manager actions | Application credentials and isolated scope operations; Polyreader authorizes people |
| Shared files, independent membership | Shared acquisition/export object with request-specific delivery/receipt IDs |
| Reliable acquisition control | Durable idempotency, revisions, typed retry/cancel and operation state |
| Recovery after outage | Ordered events, consistent snapshots, handoff cursor and recovery epoch |
| Verified managed import | Immutable PDF, digest/size, ranges or trusted mount, then verified receipt |
| Removal without data loss | Withdraw only the relevant intent; never delete consumer imports |

The fake consumer in `tests/test_companion_api.py` verifies discovery, two libraries, one physical transfer, range reconstruction, digest validation and independent acknowledgments. Recovery/concurrency/scale tests and packaging measurements are recorded in [validation](companion-validation.md).
