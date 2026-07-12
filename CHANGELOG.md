# Changelog

All notable changes to magsync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
