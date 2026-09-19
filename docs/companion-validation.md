# Companion protocol 1 validation

Validated on 2026-09-18 for package version 0.9.0, protocol 1, on one development machine (macOS, arm64, Docker Desktop). These are local acceptance results, not a statement about upstream magazine-provider availability.

## Test suite

`python -m pytest -q`: **504 passed**.

- **Daemon behavior on the production path.** The 31 established daemon-cycle tests (`tests/test_daemon.py`, `tests/test_intent_scoping.py`, `tests/test_external_failure_cycle.py`) drive the shared runtime through `_run_daemon_cycle`, a one-cycle wrapper over `Runtime.discover()`. They cover source circuits, parking, health classification, notifications, secret-safe logs and intent scoping.
- **Contract.** The generated OpenAPI equals `docs/companion-openapi-v1.json`. The base CLI imports without FastAPI/Uvicorn and without `fcntl`.
- **Consumer flow.** Two libraries, one physical transfer, independent receipts, digest/size checks, resumed ranges, lost acceptance/feed replies and expired snapshot pages.
- **Coordination.** A real background runtime (its own thread, event loop and SQLite connection) executes terminal commands, which render the same output as standalone commands. Also covered:
  - temporary-owner waiting and busy reporting
  - withdrawal on Ctrl-C and hang-up
  - abandoned and interrupted local commands are never replayed
  - bounded remote recovery
  - one failing command never stops the runtime
  - transient database locks and a dead service loop are handled
- **Configuration.** Single-file bind mounts (in-place rewrite), read-only directories, concurrent external edits and environment-managed settings.
- **Exports and claims.**
  - Local-only demand is never exported.
  - Capacity is decided per issue.
  - A failed publication leaves no stuck attempts.
  - Missing originals are reacquired; corrupt ones are not.
  - Unpublishable content is retried once per discovery cycle.

## Scale

Both fixtures live in `tests/test_companion_scale.py`, which asserts generous bounds. The values below come from two consecutive local runs; they are measurements, not service-latency guarantees.

| 30,000 issues, 2 clients, 4 scopes, 2,000 requests | Observed |
|---|---:|
| 500-request status page | 60–117 ms |
| Materialize a 1,002-resource client snapshot | 240–306 ms |
| 100-event page | 7–10 ms |
| Accepted search to terminal operation state | 21–38 ms |

| 50,000 issues, 25 subscriptions | Observed |
|---|---:|
| First reconciliation after upgrade (one-time) | 5.3–7.5 s |
| Reconciliation per terminal command, nothing changed | 11–14 ms |
| Idle command poll | 37–64 ms |
| Discovery demand reconciliation | 9–12 ms |
| Full discovery cycle (scripted source) | 0.65–0.96 s |

Before incremental reconciliation, the same bookkeeping cost about:
- 10 s per terminal command at 10,000 issues and 20 subscriptions (88 s at 60,000 and 25)
- 55 s of event-loop-blocking work per discovery cycle at 20,000 and 25

## Docker acceptance

`scripts/smoke_companion_docker.py --architecture arm64 --stalled-health` passed. It covered:
- non-root startup and optional-dependency isolation
- no API listener in the daemon image
- authenticated service routes with no host port mapping
- rejection of a competing daemon
- verified transfer and import receipt, disable/enable and credential rotation
- container replacement with persistent identity
- operator status, purge and recovery
- a read-only dedicated consumer view
- a stopped (SIGSTOP) container becoming `unhealthy`

The resources it creates are uniquely named and removed afterwards.

The daemon image was also run with a single-file `config.toml` bind mount, as on the reference NAS deployment. `docker exec … magsync subscribe` and `magsync config <key> <value>` saved in place, and the container stayed healthy with no restarts.

| Target | Platform | Image size (bytes) |
|---|---|---:|
| Daemon | linux/arm64 | 183,711,801 |
| Service | linux/arm64 | 230,954,426 |

amd64 images are built by the release workflow; they were not executed in this validation.
