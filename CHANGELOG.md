# Changelog

All notable changes to magsync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
