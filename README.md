<h1 align="center">magsync</h1>
<p align="center"><i>Magazine downloader and organizer for your local library</i></p>

A CLI/TUI tool for indexing and downloading PDF magazines from [freemagazines.top](https://freemagazines.top), organized into a clean local library.

```
~/Magazines/
├── The New Yorker/
│   ├── The New Yorker - 2026-04 - April 13.pdf
│   ├── The New Yorker - 2026-04 - April 6.pdf
│   ├── The New Yorker - 2026-03 - March 23.pdf
│   └── ...
├── The Economist/
└── Science News/
```

## Features

- **Search** magazines by title with full pagination across freemagazines.top
- **Download** PDFs automatically — handles LimeWire's E2E encryption entirely in Python (no browser needed)
- **Organize** into a flat `[Magazine Title]/` structure with uniform filenames that sort chronologically (Komga/Kavita compatible)
- **Track** what you've downloaded with a local SQLite index — never re-download the same issue
- **Update** your index on demand to discover new issues for tracked magazines
- **TUI** for interactive browsing, or **CLI** for scripted/headless use
- **Self-healing** — automatically refreshes encryption constants when LimeWire updates their JS bundles

## Installation

Requires Python 3.11+.

```bash
# From source
git clone https://github.com/yourusername/magsync.git
cd magsync
pip install -e .

# Or with pipx (recommended)
pipx install .
```

## Quick Start

### TUI (Interactive)

```bash
magsync
```

Launches an interactive terminal UI (built with [Textual](https://textual.textualize.io/)) — search, multi-select issues, and watch downloads without leaving the terminal:

```
+----------------------------------------------------------------------------+
|                                  magsync                                   |
+----------------------------------------------------------------------------+
|   [Search]     Downloads      Library                                      |
|                                                                            |
|  +--------------------------------------------------------------------+    |
|  | The New Yorker                                                     |    |
|  +--------------------------------------------------------------------+    |
|                                                                            |
|  [ ] Title                             Year  Month  Size    Status         |
|  ------------------------------------------------------------------------  |
|  [x] The New Yorker - April 13, 2026   2026  04     18 MB   complete       |
|  [x] The New Yorker - April 6, 2026    2026  04     17 MB   complete       |
|  [ ] The New Yorker - March 23, 2026   2026  03     19 MB   pending        |
|  [ ] The New Yorker - March 16, 2026   2026  03     16 MB   pending        |
|  [ ] The New Yorker - March 9, 2026    2026  03     18 MB   pending        |
|  [ ] The New Yorker - March 2, 2026    2026  03     15 MB   failed         |
+----------------------------------------------------------------------------+
|  Found 24 issues (6 new)  -  2 selected                                    |
+----------------------------------------------------------------------------+
|  q Quit    s Search    a Select All    d Download                          |
+----------------------------------------------------------------------------+
```

Three tabs:
- **Search** — type a magazine name, browse results, select issues, download
- **Downloads** — watch live download progress (`✓`/`✗` per issue)
- **Library** — browse your indexed magazines as a tree of title → year → issue, with a download tick per issue:

```
+----------------------------------------------------------------------------+
|                                  magsync                                   |
+----------------------------------------------------------------------------+
|    Search      Downloads     [Library]                                     |
|                                                                            |
|  Magazines                                                                 |
|  +- The New Yorker  (38/52)                                                |
|  |  +- 2026                                                                |
|  |  |  +- [x] The New Yorker - April 13, 2026                              |
|  |  |  +- [x] The New Yorker - April 6, 2026                               |
|  |  |  +- [ ] The New Yorker - March 23, 2026                              |
|  |  |  +- [ ] The New Yorker - March 16, 2026                              |
|  |  +- 2025                                                                |
|  +- The Economist  (12/12)                                                 |
|  |  +- 2026                                                                |
|  +- Science News   (5/9)                                                   |
|     +- 2026                                                                |
+----------------------------------------------------------------------------+
|  3 magazines tracked  -  55 issues  -  50 downloaded                       |
+----------------------------------------------------------------------------+
|  q Quit    s Search    a Select All    d Download                          |
+----------------------------------------------------------------------------+
```

Keyboard shortcuts: `s` (search), `a` (select all), `d` (download selected), `q` (quit)

### CLI

```bash
# Search for a magazine
magsync search "The Economist"

# Download all issues from March 2026 onward
magsync fetch "The New Yorker" --since 2026-03

# Preview what would be downloaded (no actual download)
magsync fetch "The New Yorker" --since 2026-03 --dry-run

# Download to a custom directory
magsync fetch "Science News" --since 2025-01 --output ~/MyMags

# Update index for all tracked magazines
magsync update

# Re-attempt failed downloads (only those — the not-yet-downloaded backlog is never touched)
magsync retry
magsync retry "The Economist"   # limit to one magazine

# Repair indexed issues that are missing a download URL (e.g. after a site change)
magsync backfill-urls
magsync backfill-urls "The Economist"   # limit to one magazine

# View/change configuration
magsync config
magsync config output_dir ~/MyMagazines
```

**Batch output.** `fetch`, `retry`, and `backfill-urls` show one progress bar with live outcome counters (`downloaded`, `unavailable`, `unsupported`, and `failed`) on an interactive terminal, and fall back to periodic textual progress lines when output is piped or run under `docker exec` without a TTY. Expected per-issue unavailable/unsupported messages are hidden by default; use `-v/--verbose` to see them, `-q/--quiet` for the summary only, and `--no-progress` (or `MAGSYNC_NO_PROGRESS=1`) to disable the live bar in scripts. The `daemon` is unaffected — it keeps its structured, timestamped logs.

**Download provenance.** Indexing catalogs every issue a search returns, but cataloging is not a download request: each download row records *who wanted it* (`subscription` when a subscription search matched its title, `manual` when you explicitly fetched or selected it, or nothing — a `cataloged` side-effect entry). Only wanted rows are ever automatic work; the fuzzy strangers freemagazines.top's search returns alongside real matches are cataloged and left alone. Explicit requests are one-way: fetching an issue marks it `manual`, and that outlives a later unsubscribe.

**Retry scope.** The daemon automatically schedules exhausted transient downloads and source-blocked dead-link refreshes for a later due cycle; those UTC schedules survive restarts, and each cycle claims only wanted rows that still match a **current** subscription (title honoring `exact`, plus its `since` floor — the daemon re-reads subscriptions every cycle, so config-file edits apply without a restart). `magsync retry` is an explicit override: it atomically claims exactly the wanted linked `failed`/`unavailable` rows in its invocation snapshot — including rows whose subscription has lapsed — bypasses their current schedule, and never drains unrelated pending, `unsupported`, or never-requested work. Excluded never-requested failures are counted with the recovery path (`magsync fetch "<title>"` marks every matching row requested, then `retry` takes the failures); link-less failures are skipped and counted (also under `-q`) — run `magsync backfill-urls` to repair them first (`--all` to include never-requested rows).

**Status meanings.** `pending` is queued wanted work, `cataloged` is an indexed side-effect entry nobody requested (never auto-downloaded; fetch or select it to make it wanted), `complete` is a stored PDF, `unavailable` is a confirmed dead/orphaned share that may recover only through a refreshed source link or manual retry, `unsupported` is a live non-PDF payload, and `failed` covers a typed transient or deterministic processing failure. Only typed transient failures on wanted rows are automatically scheduled for another download attempt.

**Source access.** A validated freemagazines.top “no results” page is a normal empty result. A Cloudflare challenge, transient outage, or unrecognized page format is reported as a source failure instead; CLI operations exit nonzero, and the TUI keeps its previous results. During a daemon cycle, the first detected challenge stops later source requests while already-cached download work continues. magsync does not provide browser automation, clearance-cookie acquisition, TLS impersonation, proxy rotation, or any other challenge bypass.

## Subscriptions

Manage magazine subscriptions for daemon mode:

```bash
# Add a subscription
magsync subscribe "The New Yorker" --since 2025-01
magsync subscribe "The Economist" --since 2024-06 --exact

# List subscriptions
magsync subscribe

# Remove a subscription
magsync unsubscribe "The Economist"
```

Or configure in `config.toml`:

```toml
[[subscriptions]]
query = "The New Yorker"
since = "2025-01"

[[subscriptions]]
query = "The Economist"
since = "2024-06"
exact = true
```

**Matching scope.** Subscriptions match by substring, so `"The Economist"` also captures sibling titles like *The Economist Audio* or regional editions. Set `exact = true` (CLI: `--exact`; env: prefix the entry with `!`, e.g. `!The Economist`) to index only issues whose normalized title matches the query exactly.

## Docker

Run magsync as an unattended daemon in Docker. Automatically fetches new issues on a schedule.

Each daemon cycle reports separate pipeline health: `healthy` when attempted phases produced validated outcomes, `degraded` when useful work continued alongside source/worker failures, and `failed` when a local/configuration/database problem prevented all intended work. This state is persisted for diagnostics. Docker's `/tmp/magsync-healthy` check remains only a process-liveness heartbeat: external degradation does not trigger a restart loop, while a stalled daemon still becomes unhealthy at the existing threshold.

Subscriptions are re-read from configuration at the start of every cycle, so editing a mounted `config.toml` (unsubscribe, `since`, `exact`) takes effect at the next cycle without restarting the container; env-var subscriptions still require a container recreate by nature. Each cycle downloads and refreshes only *wanted* rows (see **Download provenance** above).

> **Upgrading to 0.7.0:** back up `~/.magsync/index.db` first. Rolling back to 0.6.x requires restoring that backup — a 0.6.x daemon ignores download provenance and would immediately re-download everything 0.7.0 parks as `cataloged`.

### Quick Start

```bash
# Pull the image
docker pull ghcr.io/izzoa/magsync:latest

# Or build locally
docker build -t magsync .

# Start the daemon
docker compose up -d
```

### docker-compose.yml

```yaml
services:
  magsync:
    image: ghcr.io/izzoa/magsync:latest
    container_name: magsync
    volumes:
      - ./config.toml:/config/config.toml
      - magsync_data:/data
      - /path/to/magazines:/magazines
    environment:
      - MAGSYNC_OUTPUT_DIR=/magazines
      - MAGSYNC_INTERVAL=6h
      - MAGSYNC_SUBSCRIPTIONS=The New Yorker:2025-01,The Economist:2024-06
      # - MAGSYNC_APPRISE_URLS=gotify://server/token
    restart: unless-stopped

volumes:
  magsync_data:
```

### Multi-Architecture

Build for both amd64 and arm64 (Raspberry Pi, Synology NAS):

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t magsync:latest .
```

### Environment Variables

All config values can be overridden via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `MAGSYNC_OUTPUT_DIR` | Magazine output directory | `~/Magazines` |
| `MAGSYNC_INTERVAL` | Daemon cycle interval | `6h` |
| `MAGSYNC_SUBSCRIPTIONS` | Comma-separated `query:since` pairs; prefix an entry with `!` for exact title matching (e.g. `!The Economist:2024-06`) | (none) |
| `MAGSYNC_APPRISE_URLS` | Comma-separated [Apprise](https://github.com/caronc/apprise/wiki) notification URLs | (none) |
| `MAGSYNC_CONFIG_DIR` | Config directory path | `~/.magsync` |
| `MAGSYNC_DB_PATH` | SQLite index path | `{config_dir}/index.db` |
| `MAGSYNC_DOWNLOAD__MAX_CONCURRENT` | Max parallel downloads | `3` |
| `MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS` | Retries after a failed download (0 = no retry — **not recommended**; transient LimeWire throttling won't be retried) | `2` |
| `MAGSYNC_DOWNLOAD__SCRAPE_DELAY` | Delay between scrape requests (seconds) | `1.0` |
| `MAGSYNC_NO_PROGRESS` | Disable the live progress bar in bulk commands (use the textual fallback) | (unset) |

### NAS Deployment (Synology, QNAP)

```yaml
volumes:
  - /volume1/docker/magsync/config.toml:/config/config.toml:ro
  - magsync_data:/data
  - /volume1/magazines:/magazines
environment:
  - MAGSYNC_OUTPUT_DIR=/magazines
# Match your NAS user ID:
user: "1026:100"
```

### Notifications

magsync sends notifications via [Apprise](https://github.com/caronc/apprise/wiki) when new issues are downloaded. Supports 90+ services including Gotify, Discord, Slack, ntfy, email, and more.

```bash
# Via environment variable
MAGSYNC_APPRISE_URLS=gotify://myserver:8080/token,discord://webhook_id/webhook_token
```

Or in `config.toml`:

```toml
[notifications]
enabled = true
apprise_urls = ["gotify://myserver:8080/token"]
```

## Configuration

Config lives at `~/.magsync/config.toml` (or `$MAGSYNC_CONFIG_DIR/config.toml`):

```toml
[general]
output_dir = "~/Magazines"

[download]
max_concurrent = 3
retry_attempts = 2
scrape_delay = 1.0

# [limewire] section is auto-populated on first download via self-healing.
# You do not need to configure this manually.

[notifications]
enabled = false
apprise_urls = []

[[subscriptions]]
query = "The New Yorker"
since = "2025-01"
```

## How It Works

### Scraping

magsync searches freemagazines.top (a WordPress site) via its search endpoint (`/?s=query`), follows pagination, and scrapes individual magazine detail pages to extract metadata and download links. The LimeWire link is read from the download button's `data-url` attribute, falling back to a legacy `href` and then a whole-page search so the scraper survives template changes; each candidate is validated to contain the `#fragment` decryption key. If an indexed issue is ever left without a download URL, `magsync update` re-scrapes and backfills it automatically, or run `magsync backfill-urls` to repair only the affected issues.

### Downloading

Downloads go through [LimeWire](https://limewire.com), a file-sharing service that uses end-to-end encryption. magsync implements the full decryption pipeline natively in Python:

1. Visit the LimeWire share page to get a session (JWT + CSRF token)
2. Extract metadata from the server-rendered HTML (bucket ID, encryption keys, etc.)
3. Derive the AES decryption key from the URL fragment:
   - **Short links** (`/d/bjAa5#passphrase`): PBKDF2 → AES-KW unwrap → ECDH key agreement
   - **UUID links** (`/d/{uuid}#base64key`): Direct ECDH key agreement
4. Fetch the presigned S3 download URL via LimeWire's API
5. Download the encrypted blob and decrypt with AES-256-CTR

No browser, Playwright, or Selenium is required or used. Direct source access may still be blocked by an upstream challenge; magsync detects and reports that state but does not attempt to bypass it.

Downloads are resilient to LimeWire throttling: a transient server error pauses all concurrent downloads briefly through a shared gate and consumes one bounded exponential-backoff budget. Issues with the same exact full share URL share one in-flight operation, while URLs with different key fragments remain distinct. Shares LimeWire reports as removed—or confirms twice as a live bucket with an empty content list—are marked unavailable instead of burning ordinary retries.

freemagazines.top rotates a post's LimeWire link when the old share is taken down, so magsync treats the stored link as self-healing: re-scraping an issue refreshes its link if the site now serves a different one (re-queuing any issue previously parked as unavailable), and a download that fails on a dead link re-scrapes the page once and retries immediately with the fresh link before giving up.

**PDFs only.** Some shares carry non-PDF payloads (e.g. *The Economist Audio* ships a ZIP of MP3s even though the site labels everything `[PDF]`). magsync detects these — by the share's file-name extension before downloading, or by the decrypted file's signature after — and marks them `unsupported`: never saved, never auto-retried, `.part` leftovers cleaned up. An `unsupported` issue is re-probed only if the site rotates its share link (the replacement blob might be a real PDF). To keep such titles out of the index entirely, use exact subscription matching (see [Subscriptions](#subscriptions)).

### Self-Healing

LimeWire's encryption constants (salt, IVs) are embedded in their JavaScript bundles and may change on deploys. If decryption produces output matching no known file signature (a decrypted ZIP or other non-PDF is *not* a decryption failure — see above), magsync automatically:

1. Fetches LimeWire's current JS bundles
2. Extracts updated encryption constants
3. Retries decryption with the new constants
4. Persists working constants to your config file

If auto-extraction fails, see [UPDATE_KEYS.md](UPDATE_KEYS.md) for manual extraction instructions.

### Organization

Files are organized in a flat structure per magazine title for Komga/Kavita compatibility. Dates are parsed from titles to build uniform, chronologically-sortable filenames:

- `The New Yorker – April 13, 2026` → `The New Yorker/The New Yorker - 2026-04 - April 13.pdf`
- `The Economist - February 16-23, 2026` → `The Economist/The Economist - 2026-02 - February 16-23.pdf`
- `Science News - Vol 208 No 05, May 2026` → `Science News/Science News - 2026-05 - Vol 208 No 05 May.pdf`
- Undatable issues use the sanitized original title as the filename

## Dependencies

| Package | Purpose |
|---------|---------|
| [httpx](https://www.python-httpx.org/) | Async HTTP client for scraping and API calls |
| [beautifulsoup4](https://www.crummy.com/software/BeautifulSoup/) | HTML parsing |
| [cryptography](https://cryptography.io/) | PBKDF2, AES-KW, ECDH P-256, AES-256-CTR |
| [textual](https://textual.textualize.io/) | Terminal UI framework |
| [typer](https://typer.tiangolo.com/) | CLI framework |
| [rich](https://rich.readthedocs.io/) | Terminal formatting and progress bars |
| [apprise](https://github.com/caronc/apprise) | Notifications (optional) |

## Disclaimer

This software is provided for educational and personal use only. The authors and contributors of this project are not responsible for how it is used. Users are solely responsible for ensuring their use of this tool complies with all applicable laws and regulations in their jurisdiction, including but not limited to copyright law. Downloading copyrighted material without authorization may be illegal in your country. The authors do not endorse, encourage, or condone the use of this software for copyright infringement or any other unlawful activity.

## License

MIT
