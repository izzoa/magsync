# Changelog

All notable changes to magsync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.9.0] - 2026-09-19

Adds an optional authenticated **companion service**, so a trusted backend such as Polyreader can manage independent library subscriptions and receive verified PDFs. The daemon, the service and every terminal/TUI command now share **one acquisition runtime** per library. The daemon keeps its established cycle: health classification, parking, notifications and per-issue logs all run through that runtime, and the existing daemon-cycle tests exercise it directly.

> **Upgrading to 0.9.0:** stop every CLI/TUI/daemon writer and upgrade them together. Back up the database with its adjacent identity marker, the configuration and your files first. Never run an older binary against the migrated store; roll back by restoring the matched pre-upgrade backup. The checked-in `docker-compose.yml` is unchanged; the companion profile lives in `docker-compose.example.yml`.

### Added
- **Optional companion service** (`service` extra, `magsync serve`):
  - authenticated protocol 1 with generated OpenAPI
  - client-owned library scopes and independent subscriptions
  - durable idempotent operations with revision checks
  - bounded search and control routes

  `serve` honors `--interval`/`MAGSYNC_INTERVAL` like the daemon and sends the same download notifications.
- **Verified PDF deliveries for remote libraries:**
  - immutable export generations with HTTP ranges and per-request receipts
  - client-scoped events and consistent recovery snapshots
  - retention pins, capacity controls and optional isolated trusted mount views

  Exports are created only for remote demand; local downloads are never copied. Each issue is published by one task at a time, so concurrent passes never stage a duplicate export. Requests go from `acquiring` straight to `fulfilled`. A client that disconnects before a transfer starts releases its read lease at once instead of pinning the export until restart.
- **Operator commands:** explicit initialization, client provisioning/rotation/revocation, status, export purge and recovery-epoch rotation. Credentials are stored only as verifiers.
- **Docker:**
  - separate daemon and service targets
  - multi-architecture release images `ghcr.io/izzoa/magsync` and `ghcr.io/izzoa/magsync-service`
  - `docker-compose.example.yml` with a private-network `companion` profile
- **Dry runs:** `magsync daemon --dry-run` lists the cached downloads and due link refreshes the next cycle would claim. `magsync fetch --dry-run` lists the cached issues a fetch would download. Both read a private snapshot: no source requests and no changes.

### Changed
- **One shared runtime.** Ownership locks, fenced attempts and bounded shutdown keep a single owner per library.
  - While a daemon or service runs, terminal commands are executed by it within about a second, beside discovery and downloads, and print the same results as standalone commands.
  - Read-only commands (viewing configuration, listing subscriptions, dry runs) never involve it.
  - A command for an issue that is already downloading waits for that download and reports its result, instead of reporting nothing or "already downloaded". Remote requests and retries for such an issue complete with that transfer's outcome.
- **Terminal commands are never run behind your back.**
  - A second terminal command waits up to 60 seconds for another one, then reports that it is busy without queuing anything.
  - Ctrl-C or closing the terminal before a submitted command starts cancels it.
  - A command interrupted mid-run is recorded as interrupted and never run later.
  - A command the daemon has started keeps its terminal waiting until it finishes, even while the daemon drains during a graceful stop or has stopped accepting new commands.
- **Failures stay contained.** A failing command ends only itself. A briefly locked database never stops the runtime. A service whose runtime dies exits so its supervisor can restart it.
- **Demand is tracked per library**, separately from physical downloads. Local retries require current local intent and exclude remote-only, lapsed-only, unsupported and never-requested work. Explicit selections remain independent of subscriptions.
- **Reconciliation is incremental and bounded.** At 50,000 issues and 25 subscriptions, a terminal command adds about 15 ms and a full discovery cycle's bookkeeping takes under a second. Local demand is exempt from the remote client's admission limits.
- **Configuration writes merge** only the fields a command changed and detect conflicting external edits. The file is replaced atomically, or rewritten in place under the same lock when `config.toml` is a single-file bind mount. Every writer locks `config.toml` itself, so writers that reach one file through different directories (a host process and a container that bind-mounts only the file) still take turns. Environment-managed or read-only settings produce a clear message instead of a traceback.
- **Terminal and TUI output.**
  - Commands handled by a daemon name each issue with its outcome.
  - The TUI shows per-issue progress and the number of newly indexed issues.
  - `repair-titles` removes emptied folders however it is run.
- **Container health.** The health check uses a five-second runtime heartbeat with a 30-second stale threshold, independent of source health and the discovery interval. Protocol version `1` is independent of the package version.

### Fixed
- `magsync config <section.key> <value>` parses boolean settings such as `notifications.enabled` and comma-separated lists such as `notifications.apprise_urls`. Previously booleans failed to parse and lists were stored as a single string.
- Content deduplication no longer records a new download as a copy of a previously downloaded file that has since been deleted.
- A top-level `output_dir` in `config.toml` is honored when `[general]` does not set one; previously it was silently ignored. `magsync config output_dir …` stores the value under `[general]` and removes the top-level key.

## [0.8.2] - 2026-09-09

Fixes duplicate `[PDF] …` library folders that appeared as soon as 0.8.0/0.8.1 made the legacy back catalogue downloadable — and the quieter half of the same bug, which had been silently skipping every `exact` subscription's back catalogue.

The source no longer emits that label anywhere (verified live: zero occurrences in search listings and detail `og:title`). It survives in **stored** titles from when it did, and indexing deliberately never backfills a title because it drives the derived date fields and magazine association. Those rows sat unclaimable for months, so nobody saw it; then they downloaded under their old names.

### Fixed
- **A leading source format tag is no longer treated as part of a magazine's name.** Title normalization strips it, which fixes directory names, filename prefixes, magazine grouping, and subscription matching at once — all four already routed through the same normalizer. Only an anchored, bracketed, length-bounded tag is removed; bracketed *issue detail* elsewhere in a title is preserved.
- **`exact` subscriptions can claim their legacy back catalogue again.** Claim eligibility compares the **stored** title, so while the tag counted as part of the name `[pdf] afar` never matched `afar`: those issues resolved links fine and were then never claimed. A substring subscription still matched (the name is inside the tagged string), which is why every duplicate folder belonged to one and not a single `exact` subscription had one — the tell that led to this.

### Added
- **`magsync repair-titles`** consolidates what is already stored and on disk: strips the tag from stored titles, re-derives the fields the title determines, re-associates each issue with its correctly-named magazine, moves any already-downloaded file to its corrected path, and updates the recorded path so content deduplication keeps resolving to a real file. It never overwrites an occupied destination (both files are left in place and the collision reported), prunes tagged magazine records and folders left empty, is safe to re-run, and supports `--dry-run` to preview everything first.

## [0.8.1] - 2026-09-08

Fixes a 0.8.0 bug found in its own first production cycle: hundreds of `advertised a download whose link could not be resolved` warnings, while other issues in the same search stored links fine. VK names a document **two** ways and the source returns both — 0.8.0's VK form was generalized from a single observed sample, so the other form was reported as the source breaking its contract when it was behaving correctly.

### Fixed
- **VK's signed `/s/v<n>/doc/<token>` document form is accepted**, alongside the canonical `/doc<owner>_<id>` form. The signed form is the majority for older posts; because `vk.com` *is* a supported host it had fallen into the "supported host, malformed link" branch, so those issues were reported as `PROTOCOL` failures, never indexed, never parked, and therefore **re-resolved on every cycle** — the exact per-cycle retry loop and permanent `degraded` state 0.8.0 set out to remove, still running for that subset. Verified live end-to-end: stored URL → derived CDN URL → HTTP 206 → `application/pdf` → `%PDF-1.6`. Each form is still validated strictly, and the signed CDN *file* path is still refused as a stored identity because it is ephemeral. A stored signed link that later expires fails as an unavailable share and the existing link refresh rotates in a fresh one.
- **Indexing no longer spends source requests on fuzzy-search strangers.** An `Airliner World` search was resolving `aviation-news-*` and `african-aerospace-*`; `BBC Good Food` was resolving `new-scientist-international-*`. Those rows are cataloged without provenance and can never be claimed, so every request spent on them was waste — the same reasoning `backfill-urls` already applies. Resolution is now scoped to issues matching the triggering subscription, using the same canonical matcher that decides provenance moments later, so the two cannot drift. Nothing is lost permanently: subscribing later promotes the row and `backfill-urls` repairs its URL.

### Changed
- The link-refresh log line reads `Refreshed download link` rather than naming LimeWire, since it now reports VK URLs as often as LimeWire ones.

## [0.8.0] - 2026-09-08

Restores downloading after freemagazines.top changed how it publishes download links, and adds its **second file host**. Diagnosed from a live NAS whose daemon had indexed new issues every night for months while queueing **zero** downloads, with no ERROR line in any cycle: the link had moved behind a masked server-side lookup, and the scraper's "no URL found" path was a silent no-op rather than a failure. Probing the fix's own first cycle then revealed the second half — the site serves links from both LimeWire **and VK** (its download button's `lw-vk-` prefix says so), and a VK link was being reported as "the source broke its contract". The LimeWire decryption pipeline was never implicated and is unchanged.

> **Upgrading:** no schema change and no data migration, so rolling back to 0.7.x is clean — unlike the 0.7.0 upgrade. After deploying, run `magsync backfill-urls` once to repair the accumulated backlog of issues indexed without a link.

### Added
- **Masked download-link resolution.** Detail pages render a download trigger carrying an opaque key instead of a link; magsync exchanges that key for the real URL through the source's masked-download endpoint. The key is read from the trigger element via the parsed DOM — by element identity, then class, then attribute — never by matching the inline script that names the endpoint, because this is the *fourth* text-matching break in this pipeline. Inline `data-url`/`href`/whole-page strategies are retained and preferred, so a page still serving a link directly costs no extra request.
- **A VK download backend**, dispatched on the stored URL's host. A VK document needs **no cryptography at all** — no key derivation, no PBKDF2/AES-KW/ECDH/AES-256-CTR, no decryption: magsync derives the direct file URL from the public document page and streams it. Only the stable `vk.com/doc…` URL is stored; the signed CDN URL is **ephemeral, derived fresh on every attempt** (so its lifetime never has to be assumed) and is never persisted or logged. It is read from the page rather than hardcoded, but constrained to VK-operated domains so a page-supplied value cannot redirect retrieval elsewhere. Everything around dispatch — batch orchestration, retry budget, SHA-256 dedup, organized placement, notifications, typed failures, and `%PDF` validation with 0.5.0's storage-authoritative resume — is shared, not duplicated.
- **Host-aware URL validation.** One supported-host validator guards every stored URL and claim predicate, dispatching to per-host strict rules (LimeWire: exact `/d/<id>` plus a non-empty fragment; VK: an exact single-document path and no query beyond an optional access hash). Shared rules — HTTPS, no credentials, no nonstandard port, exact hostname — stay in one place, so a lookalike host cannot pass on either branch. Fragments and access hashes are secret material and stay redacted in diagnostics.
- **Link outcomes are counted and logged distinctly** in the cycle summary (`N link failures, N link-less, N unsupported host, N dead link`), so a source that stops publishing usable links is diagnosable from one line.
- **A health backstop for the next variant of this bug:** a cycle that indexed issues a subscription wanted, queued no download work, and has no pending action explaining it is reported `degraded` even when every individual phase reported success.

### Changed
- **A resolved link is classified by outcome and host instead of collapsing into one failure.** A supported host proceeds; an **unrecognized** host is a terminal `unsupported` outcome (the source honored its contract — only the destination is unusable); a source that reports **no available link** parks the issue as `unavailable`; and only a genuinely broken response (non-JSON, unparseable, missing URL) stays a `PROTOCOL` failure, with a challenge still opening the cycle circuit. Modelling the first two as failures is what made a healthy source look permanently broken. **The persisted `SourceFailureKind` enum is deliberately unchanged** — widening it would have forfeited this release's clean rollback.
- **Unusable links are parked with a scheduled re-probe rather than retried every cycle or abandoned.** A rejected key waits 24 hours; an unsupported host waits 30 days, because a rehost is far less likely than a rotated link. Both are indexed first, so they stay visible and countable. The load-bearing part is that the resolution gate **excludes rows with a pending re-probe**: without it the schedule would be defeated by indexing re-resolving the row on the very next cycle.
- **Neither an unsupported host nor a parked dead link degrades a cycle.** Both are expected steady-state outcomes, so a catalog permanently containing them can still report `healthy` — a status that is always `degraded` diagnoses nothing.
- **Resolution is gated on need** — a genuinely new issue, a row with no usable URL, or an explicit repair (`REFRESH_LINK`, `backfill-urls`). Issues that already have a usable stored URL cost no request, keeping steady-state cost at roughly one extra POST per *new* issue rather than doubling detail-request volume against a challenge-protected source. Resolution shares the cycle client's session, cookie jar, pacing gate, detail-concurrency bound, and circuit; response validation gained a content-type mode so a structured payload validates without bypassing the challenge/origin/status checks.
- **An advertised-but-unresolvable link is a typed failure, not an issue stored with a NULL URL.** A page offering *no* download affordance is still a legitimate link-less catalog entry; a page that advertises one whose link cannot be resolved fails loudly for that issue while leaving its siblings intact.
- **0.5.0's "an `unsupported` row is never automatically re-probed" becomes "re-probed on a long backoff."** This is what makes a rehosted issue recoverable; non-PDF payload rows get the same treatment, since a re-upload may be a real PDF.
- Link refresh and `backfill-urls` resolve masked keys too; without this a masked-only page would report "no download link" forever. A refresh that is still unusable re-parks on its own backoff instead of being cleared.
- Link-less counting is scoped to rows that could ever be work (matching the triggering subscription, or every row when there is no subscription context). A fuzzy-search stranger is cataloged by design, so counting it would report a healthy cycle as broken.
- `MagazineIndex.add_issues` returns an `IndexOutcome` (`added` plus `linkless`) so indexing reports what it stored without a usable URL, distinctly from what it added.

### Fixed
- **The daemon downloads again.** Every cycle had been indexing new issues with no download URL; because every claim path requires a non-empty link, those rows were never claimed, never attempted, never *failed*, and never scheduled a refresh — producing `0 queued/0 unique (0 complete, 0 unavailable, 0 unsupported, 0 failed); 0 refreshes pending` indefinitely, while the cycle was marked `degraded` only by an unrelated detail failure that masked the real problem.
- **VK-hosted issues download instead of being reported as a source protocol failure** and re-attempted every cycle.
- **Re-parking an unsupported-host row no longer downgrades it to `unavailable`**, which had silently lost the distinction between "on a host we cannot use" and "the share is dead".
- A 404 on a non-search, non-detail source request is no longer described as a detail-page 404.

### Removed
- **Proactive per-cycle detection of rotated links for issues that already have a working URL**, traded deliberately for request volume: detecting it requires resolving a key on every search hit on every cycle (~2x source traffic) to find something the reactive path already handles — a dead share fails as `unavailable`, schedules a `REFRESH_LINK`, and that refresh resolves through the same code. The cost is a bounded delay on rotation, not a lost capability.

## [0.7.0] - 2026-07-12

Fixes a 0.6.0 regression observed in production the day 0.6.0 shipped: the redesigned download claim selected **every** linked pending row and every due transient failure with no subscription scoping, so the daemon downloaded and attempted never-subscribed magazines (legacy side-effect rows from fuzzy site-search results). This is the third incident rooted in "indexed ⇒ enqueued"; 0.7.0 removes that root by recording download provenance.

> **Upgrading:** back up `~/.magsync/index.db` before upgrading. **Rolling back to 0.6.x requires restoring that backup** — a 0.6.x daemon ignores the new provenance column and would immediately re-download every row 0.7.0 parks.

### Added
- **Download provenance** (`downloads.requested_by`): `subscription`, `manual`, or empty (a *cataloged* side-effect entry). Indexing still catalogs every scraped issue, but only rows a subscription matched or the user explicitly requested are ever automatic work. Promotion is one-way (`cataloged → subscription → manual`): explicit `fetch`/TUI downloads strengthen subscription rows to `manual`, so an explicit request survives a later unsubscribe; nothing is ever demoted automatically.
- **Idempotent provenance backfill** at daemon startup, each cycle, and CLI entry points: existing rows whose titles match a current subscription are promoted (title-only, so tightening/loosening `since` later still works); everything else — including the legacy zombie rows from the June incident — is parked as `cataloged`, visibly, with its stale download/refresh actions left inert.
- **`cataloged` status rendering** in `magsync search` and the TUI for parked non-complete rows; `get_download_stats` splits actionable `pending` from `cataloged`; the daemon summary's "refreshes pending" counts wanted actions only.
- **`backfill-urls --all`**: default runs repair only wanted rows (skipped never-requested rows are counted); `--all` restores full-catalog repair.

### Changed
- **Daemon claims are intent-scoped** (downloads *and* link refreshes): only `manual` rows plus `subscription` rows matching a **current** subscription — normalized title honoring `exact`, plus the sub's `since` floor — evaluated at claim time. The daemon re-reads subscriptions from config **every cycle**, so unsubscribing or editing `since` in a mounted config file takes effect without a restart (a config read failure falls back to the previous snapshot and marks the cycle degraded — never unscoped work). **BREAKING (behavioral):** never-subscribed rows that 0.6.0 briefly made claimable stop downloading; removing a subscription now stops its queued work at the next cycle; `exact` now applies at claim time (0.5.0 applied it only when indexing).
- **`magsync retry` narrows to wanted rows** — eligibility is provenance-only (an explicit retry still includes rows whose subscription lapsed) — and reports how many failed/unavailable rows were excluded as never requested, with the recovery path: `magsync fetch "<title>"` marks every matching non-complete row `manual` (and says so), after which `retry` picks the failures up.
- **`--dry-run` previews through the claim's own predicates** (shared non-mutating preview), so it now includes due transient retries the old preview omitted and excludes everything the real claim would skip.
- Subscription matching is one canonical matcher everywhere (indexing filter decisions, promotion, claims, retry): the issue's own normalized title, accent-insensitive, with curly/straight apostrophe and en/em-dash variants folded — fixing the known gap where `Cook's Illustrated` failed to match a curly-apostrophe title. Unknown `requested_by` values fail closed (never claimed, logged).

### Fixed
- **The daemon no longer downloads magazines that are not subscribed.** First 0.6.0 NAS cycle: 6 issues claimed while the source was blocked, 5 of them never-subscribed strangers, one of which (an 86 MB "Women's Golf Americas" PDF) downloaded, saved, and emailed. Under 0.7.0 those rows park as `cataloged`; a cycle claims only wanted work, and the parked shares' queued link refreshes stop consuming source requests.
- Link rotation on a parked row still updates the catalog (fresh link, status reset) but can no longer resurrect it into automatic work.

### Removed
- Nothing removed; already-downloaded unwanted files are **not** deleted automatically. To clean up manually: delete the file, then optionally its rows (`DELETE FROM downloads WHERE issue_id = …; DELETE FROM issues WHERE id = …;`) — note the file's SHA-256 stays in the dedup index while its `complete` row remains.

## [0.6.0] - 2026-07-12

### Added
- Typed download and source failures now drive retry, refresh, persistence, summaries, CLI/TUI presentation, and daemon health without parsing human-readable error text.
- Persisted due actions let the daemon retry exhausted transient downloads and source-only link refreshes in later cycles, including across restarts. `magsync retry` remains an atomic manual override scoped to the failed/unavailable rows present when the command starts.
- A cycle-scoped freemagazines.top client reuses cookies and connections, globally paces search/detail/refresh requests, bounds detail concurrency, validates response origin/content, and opens a cycle circuit after a Cloudflare challenge.
- Durable pipeline state records healthy, degraded, or failed cycles separately from the existing process-liveness heartbeat.

### Changed
- LimeWire SSR metadata is classified structurally as ready, removed, orphan-candidate, malformed, or undecodable. The narrow live-bucket/empty-content orphan signature receives exactly one fresh confirmation before it is parked as unavailable.
- One orchestrator now owns each full LimeWire URL's bounded transient retry budget. Exact full URLs, including their fragments, are single-flighted within a batch; aliases retain independent database transitions and callbacks while sharing one physical result.
- Link refresh returns explicit rotated, unchanged, no-link, source-blocked, or scrape-error outcomes. A blocked refresh remains scheduled as source-only work instead of re-requesting a known-dead share.
- CLI and TUI searches distinguish validated empty results from blocked, transient, protocol, and partial-detail outcomes. Incomplete source operations exit nonzero in CLI commands, and the TUI preserves its previous results when the source fails.

### Fixed
- A LimeWire share with `ok:true`, a valid bucket, and an explicit empty `contentItemList` no longer burns repeated metadata-extraction retries for missing `content_item_id` and `ephemeral_public_key`.
- One source-wide Cloudflare challenge no longer produces a request storm across every subscription or masquerades as an empty successful indexing cycle; cached due downloads continue and the cycle is reported degraded.
- Unexpected organizer, database, scrape, and callback failures are isolated per issue so ordinary worker failures do not cancel unrelated downloads.

### Security
- External errors are sanitized and bounded before logging, callbacks, user output, or SQLite persistence. URL fragments and queries, authorization/cookie values, JWT/CSRF data, encryption keys, and presigned-storage credentials are redacted; daemon-mode `httpx`/`httpcore` request logging is suppressed.

## [0.5.0] - 2026-07-10

Ends the permanent nightly failure loop on shares whose payload isn't a PDF (e.g. "The Economist Audio" ZIP editions), and hardens the resume path so a `.part` file can never again be corrupted by the server's own error responses. Diagnosed from a live NAS: two audio issues had been retried every cycle for days — each attempt appended a 633-byte storage-error body to an already-complete `.part` file (15–16 accumulated), re-ran constants self-healing (~30 LimeWire requests) twice, and re-decrypted 242 MB six times, for nothing.

### Added
- **Non-PDF payloads are terminally skipped** with a new `unsupported` download status. Two layers: a **pre-download gate** on the share's file name (a known non-PDF extension like `.zip`/`.mp3`/`.epub` skips before key derivation and without requesting a single payload byte), and **magic-number classification** after decryption (ZIP/RAR/7z/gzip/ID3/OggS/MP4 signatures mean decryption *worked* — the content just isn't a PDF). Unsupported issues are never auto-retried, never saved, and their `.part` files are cleaned up; they are re-probed only when the site rotates the share link (a new blob may be a different type). `magsync retry` and the daemon's startup reset leave them alone; the TUI's select-all won't re-queue them.
- **`unsupported` surfaced everywhere**: batch summaries (`N unsupported (non-PDF)`), daemon cycle log (skips log at INFO as `Skipped (non-PDF)`, not ERROR), `magsync list` status column, TUI (`⊘` marker), and `get_download_stats`.

### Fixed
- **Valid non-PDF downloads were misclassified as decryption failures.** Validation required a `%PDF` header, so a perfectly decrypted ZIP triggered constants self-healing (which "succeeded" — the constants were never stale — and changed nothing), a FAILED status, and infinite daily retries.
- **Storage error bodies were appended to `.part` files.** The streaming loop wrote whatever body arrived with no status check; each 416 added its XML error document to the file. The stream status is now inspected *before* the file is opened for writing: non-2xx bodies are never written, a 200 answering a `Range` request restarts the file from byte zero, and a 206 is accepted only when its `Content-Range` offset matches the local file exactly (AES-CTR is positional — a mis-offset splice would silently decrypt to garbage).
- **Completed downloads re-requested a `Range` beyond EOF every attempt** (HTTP 416 loop). Resume state is now reconciled against the storage layer's own totals: a non-empty `.part` resolves via one ranged probe, and the `Content-Range`/`Content-Length` total — never the SSR-advertised size, which reports bucket totals and can drift — is the only authority for truncating or slicing local bytes. Poisoned `.part` files from earlier versions **self-repair automatically** (416 → truncate to the storage-reported size, zero payload bytes transferred); decryption reads exactly the object's bytes, so trailing junk can never reach the saved file or the dedup SHA-256.
- **A short fetch is now a transient "incomplete download" failure** (kept for resume, no self-healing) instead of being decrypted and misdiagnosed as a crypto failure.

### Changed
- **Self-healing now runs only when decrypted output matches no known file signature** — a true stale-constants signal — instead of on anything that wasn't a PDF.
- **The `.part` is kept after a terminal decryption failure** (unknown output even with fresh constants): the bytes are size-consistent, so the next daily attempt costs one ranged probe plus a local decrypt instead of a full re-download. In-process retries for this deterministic failure are skipped entirely. `.part` cleanup on terminal outcomes is best-effort — a filesystem error (e.g. NAS permissions) logs a warning and never converts a skip back into a retryable failure.
- Removed the 50-minute `.part`-age session refresh — every attempt already establishes a fresh session before requesting the presigned URL, so the check only ever added a redundant second session fetch.
- Documented the `--exact` subscription flag (config `exact = true`, env `!Query` prefix) — substring matching is how "The Economist" pulls in "The Economist Audio" issues in the first place.

## [0.4.0] - 2026-07-05

### Added
- **Coordinated batch progress output** for `fetch`, `retry`, and `backfill-urls`. Previously these commands left the `magsync` logger unconfigured, so its records hit Python's lastResort handler (raw, stderr, WARNING+) and collided with the Rich progress bar on stdout — on a big dead-link backlog that was a wall of ~hundreds of interleaved lines with the bar buried, and no bar at all under `docker exec` without `-t`. Now a single coordinated surface routes logs through the same console as one overall progress bar (logs render *above* the bar), with live outcome counters (downloaded / unavailable / failed).
- **TTY-aware output**: interactive terminal shows the live bar; a non-TTY (piped, `docker exec` without `-t`, cron) shows throttled textual progress lines instead of a garbled bar; the `daemon` is unchanged and never renders a bar.
- **`--verbose/-v`, `--quiet/-q`, `--no-progress` flags** on the bulk commands (plus the `MAGSYNC_NO_PROGRESS` env). Default interactive output is no longer flooded by expected per-issue dead-link lines; `-v` restores them, `-q` shows only the summary (genuine errors still surface).
- The end-of-run summary now reports the **`unavailable` (dead links)** count alongside downloaded/failed, reconciled from the batch results so a batch-level abort is still counted.

### Changed
- The three expected-during-bulk dead-link log messages (removed share, permanent error, "marking unavailable") are now logged at INFO instead of ERROR/WARNING — a dead link during a bulk retry is a normal outcome. The daemon logs at INFO so they still appear in `docker logs`; interactive commands hide them by default and show them under `-v`.

### Fixed
- **`magsync retry` downloaded the entire pending backlog, not just failed downloads.** Every indexed issue starts life `pending`, and issues get indexed as side effects the user never queued (partial-title search results, non-exact subscriptions, `--since`-excluded issues) — only the daemon's cycle applies subscription scoping. `retry` reset failed/unavailable rows and then downloaded *all* pending rows, so a bare `docker exec magsync magsync retry` drained the whole backlog (898 unwanted downloads). It now re-attempts exactly the downloads that were failed/unavailable at invocation (`reset_failed_downloads` returns the reset issue IDs and the new `get_issues_by_ids` feeds the batch); the optional magazine filter still narrows the set. The reset runs as a single write transaction with the status guard re-asserted on the UPDATE, so a retry racing a daemon cycle can never flip an in-flight or completed row back to pending.
- **Link-less failures are no longer stranded as permanently-pending.** A failed download whose issue has no LimeWire URL used to be flipped to `pending` and then silently filtered out of the batch — no longer visible as a failure, never downloadable. Both `retry` and the daemon's startup reset now leave such rows `failed`/`unavailable` (interrupted `downloading` rows still reset unconditionally); `retry` reports the skipped count in its summary (shown under `-q` too) and points at `backfill-urls` to repair them. When *all* failures lack links, `retry` says so instead of the misleading "No failed downloads to retry."

## [0.3.16] - 2026-07-05

Two complementary fixes for the freemagazines.top → LimeWire download path: the site now rotates share links after takedowns, and LimeWire changed its share-page serialization. Either one alone left downloads failing with an identical "share link is unavailable" error; both are needed to download reliably.

### Fixed
- **Downloads 404'd on live, browser-downloadable shares.** LimeWire's share page moved its server-rendered data to a React Router **turbo-stream** — a flat array where fields are index references rather than inline values — but `establish_session` still extracted `bucket_id` and `content_item_id` by text position. It picked up the neighboring UUIDs (the file-encryption-key id as the bucket, the free-user-id as the content item), so `POST /sharing/download/{bucket}` returned 404, surfacing as the *same* "share link is unavailable" message a genuinely removed share produces. magsync now decodes the turbo-stream and reads metadata structurally (`sharingBucket.id`, `contentItemList[0].id`, the `fileEncryptionKeys` entry bound to the content item's `baseFileEncryptionKeyId`, `ephemeralPublicKey`, `name`, `totalFileSize`), verified end-to-end against a live share (valid PDF, correct byte count).
- **`file_size` read 0** for every share (the progress bar/estimate source) — `totalFileSize` is now taken from the decoded stream.
- **Rotated LimeWire links were never picked up, and issues parked `unavailable` never recovered.** freemagazines.top swaps the LimeWire share link on an existing post when the old share is taken down (without bumping the post's modified time), but the index treated `limewire_url` as write-once — so every cycle re-scraped the fresh link, discarded it, and re-downloaded with the frozen dead one until the issue was parked `unavailable` with no path back (`retry` reused the stale URL, `backfill-urls` only fixed NULL URLs). Now:
  - **Refresh on re-scrape**: `add_issues()` replaces a stored `limewire_url` when a re-scrape yields a validated, different link, and resets that issue's `failed`/`unavailable` download back to `pending` so the fresh link is retried (the `sha256`, and any `complete`/in-flight download, are left untouched). The incoming URL is checked with a strict host/path/fragment guard before it can overwrite a known-good value.
  - **Re-scrape on permanent failure**: when a download fails with a permanent "share link is unavailable" error, the batch downloader re-scrapes the page once; if it now carries a different validated link, that link is persisted and the download is retried immediately (once). Only if the page still shows the same dead link — or the re-scrape yields nothing — is the issue parked `unavailable`.

### Changed
- The legacy regex extraction is retained only as a fallback for when the SSR format changes wholesale (no decodable stream); it never overrides ids resolved from a present stream, so a partial format drift fails loudly instead of silently shipping the wrong (decoy) ids.
- Removed-share detection gained a structural backstop: a decoded container reporting `ok:false` is classified permanent even when the `SanitizedError` marker falls outside the raw-HTML detector's window; a container merely missing `ok` is treated as undecodable, never as removed.
- **`.part` resume files are now keyed to the share link that produced them** (a hash of the full URL, including the decryption-key fragment). A refreshed or rotated link starts a clean download instead of resuming bytes from a different encrypted blob (which would decrypt to garbage and misfire self-healing). Legacy un-keyed `.part` files are discarded once on upgrade and re-downloaded.
- **`download_batch()` de-duplicates its input by issue ID**, so overlapping subscriptions that enqueue the same issue no longer download it twice (and a dead link is re-scraped at most once per batch).

## [0.3.14] - 2026-06-08

### Fixed
- **Removed LimeWire shares were retried forever.** After LimeWire changed its share-page SSR serialization (JSON objects → React-Router streaming arrays), the dead-share detector — which matched the old `"sharingBucketContentData":` shape — stopped firing, so removed shares (`SanitizedError`) were misclassified as a transient "Unexpected Server Error" and retried every cycle (and, since 0.3.13, tripped the shared throttle pause, stalling the batch). Detection is now format-agnostic and anchored to the share's error tuple, so removed shares are correctly marked `unavailable` and skipped. Run `magsync retry` to re-attempt if a link returns.
- SSR error classification now runs **before** JWT/CSRF extraction, so a removed page that omits the auth cookie is still classified correctly.
- A download-API `404` (bucket removed) is now treated as permanent (`unavailable`) instead of retried.

## [0.3.13] - 2026-06-07

### Added
- **Shared throttle on transient LimeWire errors**: a transient SSR "server error" (LimeWire's throttle signal) now engages the shared rate-limit gate — pausing all concurrent downloads briefly before retrying — instead of every worker hammering through the throttle. Previously only HTTP 429 triggered the gate.
- **Per-destination batch deduplication**: issues that resolve to the same output file (e.g. hyphen vs en-dash title variants of one issue, which share a LimeWire link) are downloaded once instead of concurrently; the duplicate completes via the existing on-disk dedup.

### Changed
- Session establishment now retries transient errors a minimum number of times even when `retry_attempts=0` (a transient infra hiccup is not a download failure); download-level attempts still honor `retry_attempts` exactly.
- `RateLimitGate` is now concurrency-safe: a longer pause extends a shorter active one, and the gate always reopens even if a paused task is cancelled (previously a cancellation mid-pause could deadlock all downloads).

### Fixed
- A warning is now logged once when `retry_attempts < 1`, since that disables download retries and makes transient LimeWire errors fail immediately.
- Corrected a misleading "will retry" log that printed even when no retry would occur.

## [0.3.12] - 2026-06-07

### Fixed
- **Downloads stopped after the freemagazines.top template change (~2026-05-27)**: the LimeWire link moved from the anchor `href` to a `data-url` attribute, so the scraper extracted no URL and every newly-indexed issue had an empty `limewire_url` — daemons reported "N new indexed, 0 downloaded". The detail-page scraper now reads `data-url`, falls back to the legacy `href`, and finally to a whole-page search, validating that each candidate has a `/d/<id>` path and a non-empty `#fragment` (the decryption key).
- `magsync fetch` now reports issues skipped for a missing download URL instead of silently dropping them.

### Added
- **Self-healing backfill**: re-scraping an already-indexed issue now backfills empty `limewire_url`, `genre`, `file_size`, and `cover_image_url` fields (never overwriting populated values), so `magsync update` automatically repairs issues left without a download URL by the template change.
- **`magsync backfill-urls [magazine]`**: re-scrapes only issues missing a download URL and updates them — faster than a full `update`, and also reaches de-tracked magazines.

## [0.3.11] - 2026-04-07

### Fixed
- `MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS` env var now actually takes effect in batch/daemon downloads. The batch downloader was passing `constants` explicitly, which skipped config loading and ignored the retry setting.

### Changed
- `retry_attempts` now means number of *retries* after the initial attempt (0 = no retry, 2 = 3 total attempts). Default changed from 3 to 2 to preserve the same 3-total-attempts behavior. Session retry also respects this setting (previously hardcoded).
- Updated README directory tree and organization docs to reflect flat file layout (Komga/Kavita compatible).
- Added `MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS` to README environment variables table.

## [0.3.8] - 2026-04-07

### Fixed
- **False positive dead links**: SanitizedError detection now targets the last occurrence of `sharingBucketContentData` (the actual SSR JSON payload) using `rsplit`, instead of the first occurrence which often landed in minified JS error-handling code. Quoted JSON key/value matching (`"SanitizedError"`) prevents false matches against substrings like `SanitizedErrorBoundary`.
- **Session retry now catches HTTP errors**: The session establishment retry loop now catches `httpx.HTTPStatusError` (429, 500, 502, 503, 504) in addition to transient `RuntimeError`, preventing wasted download attempts on server hiccups.
- **Session refresh for expired `.part` files now retries**: The `establish_session` call when refreshing expired presigned URLs (>50 min) is now wrapped in the same retry loop, preventing single transient errors from killing resumed downloads.

### Changed
- Improved diagnostic logging: debug-level log of the SanitizedError context window, and info-level log when both `sharingBucketContentData` and `Unexpected Server Error` coexist in a response.
- Extracted `_establish_session_with_retry()` helper to DRY up session retry logic.

## [0.3.7] - 2026-04-07

### Fixed
- Transient decryption and constant-refresh failures are no longer marked `unavailable` permanently. Only genuinely dead LimeWire share links are classified as permanent failures, so recoverable download issues can be retried normally.
- LimeWire "Unexpected Server Error" SSR responses (common from cloud/datacenter IPs) are now treated as transient and retried with backoff, instead of being permanently marked as dead links. Only `SanitizedError` (genuinely removed shares) is permanent.
- Session establishment now retries up to 3 times with 5s/10s delays for transient SSR errors before falling back to the outer download retry loop.

### Changed
- Improved diagnostic logging for LimeWire error detection: logs now include the sharing ID, response size, and which specific error pattern triggered (SanitizedError vs Unexpected Server Error).

## [0.2.1] - 2026-04-06

### Added
- **Concurrent downloads**: `fetch` and `daemon` commands now download multiple issues simultaneously, bounded by `download.max_concurrent` config (default 3)
- **Resumable downloads**: Interrupted downloads are saved as `.part` files and automatically resumed on next attempt using HTTP Range headers. Expired presigned URLs (>50 min) are refreshed before resuming.
- **Dry run mode**: `magsync fetch --dry-run` and `magsync daemon --dry-run` preview what would be downloaded with estimated total size, without actually downloading
- **Retry command**: `magsync retry [query]` re-attempts all failed downloads, optionally filtered by magazine title
- **Download retry with backoff**: Transient download errors (network, timeout) are automatically retried with exponential backoff (2s, 4s, 8s) up to `retry_attempts` config value. Permanent errors (dead links) fail immediately.
- **Content deduplication**: SHA-256 hash computed for each downloaded PDF. Duplicate files (same content under different titles/URLs) are detected and skipped, saving bandwidth and disk space.
- **429 rate limit handling**: If LimeWire returns HTTP 429, all concurrent downloads pause for the `Retry-After` duration (or 30s default) via a shared `RateLimitGate`, then resume together
- **Concurrent detail page scraping**: Magazine detail pages are now scraped 5-at-a-time instead of sequentially, ~5x faster indexing
- **Unavailable vs failed status**: Dead LimeWire links are now marked `unavailable` (permanent, never auto-retried) instead of `failed` (transient, retried on next daemon startup). `magsync retry` still resets both.
- **Komga/Kavita-compatible flat folder structure**: PDFs now stored as `{Title}/{Title} - {YYYY}-{MM} - {Detail}.pdf` instead of nested `{Title}/{YYYY}/{MM}/` directories. Uniform filenames sort chronologically and display cleanly in media servers.

### Fixed
- LimeWire share links that are removed or expired now show a clear error ("share link is unavailable") instead of a generic SSR metadata failure
- Missing SSR fields now listed by name in error message for easier debugging

### Changed
- Download logic extracted to `core/batch.py` with `download_batch()` using `asyncio.Semaphore` + `asyncio.gather`
- TUI download screen uses concurrent batch downloads

## [0.2.0] - 2026-04-06

### Added
- **Daemon mode**: `magsync daemon` command with configurable interval (`--interval 6h`), runs unattended fetch cycles for all subscribed magazines
- **Subscriptions**: Declarative `[[subscriptions]]` config for auto-fetching magazines, plus `subscribe`/`unsubscribe` CLI commands
- **Environment variable overrides**: All config values overridable via `MAGSYNC_`-prefixed env vars (e.g., `MAGSYNC_OUTPUT_DIR`, `MAGSYNC_SUBSCRIPTIONS`)
- **Notifications**: Apprise integration for download alerts — supports 90+ services (Gotify, Discord, Slack, ntfy, email, etc.) via `MAGSYNC_APPRISE_URLS`
- **HTML email template**: Email notifications use a styled HTML template with download summary, issue counts per magazine, and file sizes
- **Docker support**: Multi-stage Dockerfile, docker-compose.yml, non-root container, health check, multi-arch (amd64 + arm64)
- **GitHub Actions CI/CD**: Automated multi-arch Docker image builds on push to main and version tags, published to GitHub Container Registry (ghcr.io)
- **Config path overrides**: `MAGSYNC_CONFIG_DIR` and `MAGSYNC_DB_PATH` env vars for Docker volume mapping
- **Graceful shutdown**: SIGTERM handling in daemon mode — finishes current download and exits cleanly

### Changed
- Config now supports `[notifications]` section and `[[subscriptions]]` array
- `config.py` refactored to use dynamic path resolution (supports `MAGSYNC_CONFIG_DIR`)

## [0.1.0] - 2026-04-06

### Added
- Initial release of magsync
- **CLI commands**: `search`, `fetch`, `update`, `config` via Typer
- **TUI**: Textual-based terminal UI with search, download progress, and library browser tabs
- **Site scraper**: Search freemagazines.top by magazine title with full pagination support
- **LimeWire downloader**: Pure Python implementation of LimeWire's E2E encrypted download pipeline (PBKDF2 → AES-KW → ECDH P-256 → AES-256-CTR) — no browser required
- **Two LimeWire URL format support**: Short ID (passphrase path) and UUID (raw key path)
- **Self-healing encryption constants**: Auto-extracts fresh constants from LimeWire's JS bundles when decryption fails
- **SQLite magazine index**: Local database tracking magazines, issues, and download status at `~/.magsync/index.db`
- **File organizer**: Parses dates from magazine titles (7+ format variations) and organizes PDFs into `[Magazine Title]/[YYYY]/[MM]/` directory structure
- **Configurable**: TOML config at `~/.magsync/config.toml` for output directory, download settings, and LimeWire encryption constants
- **Rate limiting**: Configurable delay between scraping requests (default 1s)
- **UPDATE_KEYS.md**: Documentation for manually re-extracting LimeWire encryption constants if auto-extraction fails
