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
- **Optional companion API** for Polyreader and other trusted backends: isolated library scopes, durable operations, verified PDF exports and import receipts ([service guide](docs/companion-service.md))
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

# Preview matching cached pending issues (read-only; no source traffic)
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

# One-off: repair stored titles carrying the source's old "[PDF] " label
magsync repair-titles --dry-run   # preview every change first
magsync repair-titles

# View/change configuration
magsync config
magsync config output_dir ~/MyMagazines

# Preview what the next daemon cycle would download (cached catalog; no source traffic)
magsync daemon --dry-run
```

**Running beside a daemon or service.** While a daemon or companion service owns the library, terminal commands are executed by it and print the same tables, messages and per-issue outcomes as standalone commands; commands never wait behind a discovery cycle. Read-only commands (`magsync config` without a value, `magsync subscribe` without a query, and every `--dry-run`) never involve it. If another terminal command is running, a new one waits up to 60 seconds and then reports that it is busy, queuing nothing. Pressing Ctrl-C (or closing the terminal) before the daemon starts a submitted command cancels it; a command is never run later behind your back. A command for an issue the daemon is already downloading waits for that download and reports its result, and a command the daemon has started keeps its terminal waiting through a graceful stop until it finishes.

**Batch output.** The live progress rendering described here applies to standalone commands. `fetch`, `retry`, and `backfill-urls` show one progress bar with live outcome counters (`downloaded`, `unavailable`, `unsupported`, and `failed`) on an interactive terminal, and fall back to periodic textual progress lines when output is piped or run under `docker exec` without a TTY. Expected per-issue unavailable/unsupported messages are hidden by default; use `-v/--verbose` to see them, `-q/--quiet` for the summary only, and `--no-progress` (or `MAGSYNC_NO_PROGRESS=1`) to disable the live bar in scripts. The `daemon` is unaffected — it keeps its structured, timestamped logs.

**Download provenance.** Indexing catalogs every issue a search returns, but cataloging is not a download request: each download row records *who wanted it* (`subscription` when a subscription search matched its title, `manual` when you explicitly fetched or selected it, or nothing — a `cataloged` side-effect entry). Only wanted rows are ever automatic work; the fuzzy strangers freemagazines.top's search returns alongside real matches are cataloged and left alone. Explicit requests are one-way: fetching an issue marks it `manual`, and that outlives a later unsubscribe.

**Retry scope.** The daemon automatically schedules exhausted transient downloads and source-blocked dead-link refreshes for a later due cycle; those UTC schedules survive restarts, and each cycle claims only wanted rows that still match a **current** subscription (title honoring `exact`, plus its `since` floor — the daemon re-reads subscriptions every cycle, so config-file edits apply without a restart). `magsync retry` is an explicit override: it atomically claims exactly the wanted linked `failed`/`unavailable` rows in its invocation snapshot — only while current local intent remains; remote-only or lapsed-only demand is excluded — bypasses their current schedule, and never drains unrelated pending, `unsupported`, or never-requested work. Excluded never-requested failures are counted with the recovery path (`magsync fetch "<title>"` marks every matching row requested, then `retry` takes the failures); link-less failures are skipped and counted (also under `-q`) — run `magsync backfill-urls` to repair them first (`--all` to include never-requested rows).

**Legacy titles.** The source used to prepend a format label to every listing (`[PDF] …`). A leading bracketed tag is normalized away, so it never affects a magazine's name, its folder, or subscription matching — importantly, an `exact` subscription can match such an issue, which it previously could not. Issues indexed before that fix keep the tag in their *stored* title (indexing never rewrites a title, since it drives derived dates and magazine association), which splits the library into duplicate folders like `[PDF] Science News`. Run `magsync repair-titles` once to consolidate: it strips the tag, re-derives the affected fields, re-associates magazines, moves already-downloaded files into their correct folder, updates the recorded path so deduplication keeps resolving, and prunes emptied records. It never overwrites an existing file, and `--dry-run` previews everything.

**Status meanings.** `pending` is queued wanted work, `cataloged` is an indexed side-effect entry nobody requested (never auto-downloaded; fetch or select it to make it wanted), `complete` is a stored PDF, `unavailable` is a confirmed dead/orphaned share that may recover only through a refreshed source link or manual retry, `unsupported` is a live link magsync cannot use — a non-PDF payload, or a link on a file host it has no backend for — and `failed` covers a typed transient or deterministic processing failure. An `unsupported` row and an `unavailable` row both keep a scheduled re-probe (30 days and 24 hours respectively), so an issue the site later rehosts or relinks can recover on its own. Only typed transient failures on wanted rows are automatically scheduled for another download attempt.

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

## Optional companion service

```bash
pip install -e '.[service]'
magsync companion init
magsync clients create Polyreader  # token shown once
magsync serve                     # private loopback binding, port 8765
```

Use one service or daemon per store. Terminal commands share its durable queue when it is running; otherwise they take temporary ownership. `magsync serve` runs discovery on the same schedule as the daemon (`--interval` or `MAGSYNC_INTERVAL`, default `6h`) and sends the same download notifications. Each remote library has an independent client-owned scope, and multiple scopes can share bytes while receiving separate verified-import receipts. Search creates no demand. Cancellation never deletes consumer imports.

The [service guide](docs/companion-service.md) covers API examples, credentials, capacity/retention settings, trusted mounts, Docker switching and coordinated backup/restore. The [version 1 OpenAPI schema](docs/companion-openapi-v1.json) describes the consumer contract. The base CLI/TUI installation stays independent of HTTP dependencies.

## Docker

`docker-compose.example.yml` is a complete starting point: it builds the daemon image and, under the optional `companion` profile, `magsync-service`. Run one of them at a time: stop the daemon and explicitly start the service when switching. The service port is exposed only on the private container network, with no host port mapping. Both images run as a non-root user.

Mount the configuration **directory** (`./config:/config`) so configuration updates (`subscribe`, self-healing encryption constants) are replaced atomically. A single-file mount (`./config.toml:/config/config.toml`) also works: magsync then rewrites the file in place. A read-only mount makes `subscribe`/`config` report the read-only configuration instead of saving.

Run magsync as an unattended daemon in Docker. Automatically fetches new issues on a schedule.

Each daemon cycle reports separate pipeline health: `healthy` when attempted phases produced validated outcomes, `degraded` when useful work continued alongside source/worker failures, and `failed` when a local/configuration/database problem prevented all intended work. A cycle is also `degraded` when a resolution response is structurally broken, and — as a backstop — when it indexed issues a subscription wanted but queued no download work with no pending action explaining it, so a source that stops publishing usable links can never look like a source with no new issues. An issue on an unsupported host or with no available link is **not** a fault: both are parked with a scheduled re-probe and leave the cycle `healthy`, because a status that is always `degraded` diagnoses nothing. The summary counts each cause separately (`N link failures, N link-less, N unsupported host, N dead link`), which is also how you read the split between hosts in your own library. This state is persisted for diagnostics. Docker's `/tmp/magsync-healthy` check remains only a process-liveness heartbeat: external degradation does not trigger a restart loop, while a stalled daemon still becomes unhealthy after the configured stale threshold (30 seconds by default, with a five-second heartbeat).

Subscriptions are re-read from configuration at the start of every cycle, so editing a mounted `config.toml` (unsubscribe, `since`, `exact`) takes effect at the next cycle without restarting the container; env-var subscriptions still require a container recreate by nature. Each cycle downloads and refreshes only *wanted* rows (see **Download provenance** above).

> **Upgrading to 0.9.0:** stop and upgrade all CLI/TUI/daemon/service writers together. Back up the database, its identity marker, configuration, original files and exports. Never run an older binary against the migrated store; restore a matched pre-upgrade backup to roll back. See the [backup and recovery procedure](docs/companion-service.md#upgrade-backup-restore-and-rollback).

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
      - ./config:/config            # directory holding config.toml
      - magsync_data:/data
      - magsync_exports:/exports
      - /path/to/magazines:/magazines
    environment:
      - MAGSYNC_OUTPUT_DIR=/magazines
      - MAGSYNC_INTERVAL=6h
      - MAGSYNC_SUBSCRIPTIONS=The New Yorker:2025-01,The Economist:2024-06
      # - MAGSYNC_APPRISE_URLS=gotify://server/token
    restart: unless-stopped

volumes:
  magsync_data:
  magsync_exports:
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
| `MAGSYNC_INTERVAL` | Daemon/service discovery interval | `6h` |
| `MAGSYNC_SUBSCRIPTIONS` | Comma-separated `query:since` pairs; prefix an entry with `!` for exact title matching (e.g. `!The Economist:2024-06`) | (none) |
| `MAGSYNC_APPRISE_URLS` | Comma-separated [Apprise](https://github.com/caronc/apprise/wiki) notification URLs | (none) |
| `MAGSYNC_CONFIG_DIR` | Config directory path | `~/.magsync` |
| `MAGSYNC_DB_PATH` | SQLite index path | `{config_dir}/index.db` |
| `MAGSYNC_DOWNLOAD__MAX_CONCURRENT` | Max parallel downloads | `3` |
| `MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS` | Retries after a failed download (0 = no retry — **not recommended**; transient LimeWire throttling won't be retried) | `2` |
| `MAGSYNC_DOWNLOAD__SCRAPE_DELAY` | Delay between scrape requests (seconds) | `1.0` |
| `MAGSYNC_NO_PROGRESS` | Disable the live progress bar in bulk commands (use the textual fallback) | (unset) |

### NAS Deployment (Synology, QNAP)

With `:ro`, subscription and config changes report the read-only configuration; drop it to allow updates.

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

Config lives at `~/.magsync/config.toml` (or `$MAGSYNC_CONFIG_DIR/config.toml`). A top-level `output_dir` is also accepted when `[general]` does not set one:

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

magsync searches freemagazines.top (a WordPress site) via its search endpoint (`/?s=query`), follows pagination, and scrapes individual magazine detail pages to extract metadata and download links. The site no longer publishes the download URL in page markup: the detail page renders a download button carrying an opaque key, which magsync exchanges for the real URL via the site's masked-download endpoint. That URL may be on **either of two file hosts** — LimeWire or VK — and magsync supports both. The key is located structurally in the parsed DOM (element identity, then class, then attribute) rather than by pattern-matching the surrounding script, and the older inline forms (`data-url`, a legacy `href`, a whole-page search) are still preferred when present — a page that serves a link directly costs no extra request. Every candidate, resolved or inline, is validated against its host's strict form (a LimeWire share needs its `#fragment` decryption key; a VK document needs an exact single-document path). A link that resolves to any *other* host is recorded as `unsupported` rather than treated as an error. Resolution only runs for issues that actually need a URL — a new issue, a row missing one, or an explicit repair — so it does not multiply source traffic. If an indexed issue is ever left without a download URL, `magsync update` re-scrapes and backfills it automatically, or run `magsync backfill-urls` to repair only the affected issues.

### Downloading

The source publishes links on two file hosts, and the retrieval backend is selected from the stored URL's host. Everything around that choice — batch orchestration, the retry budget, content deduplication, organized placement, notifications, and `%PDF` validation — is shared by both.

**VK documents** are the simple case: no account, no credentials, and **no cryptography at all**. magsync fetches the public document page, derives the direct file URL from it, and streams the PDF (resuming from a partial file when one exists). Only the stable `vk.com/doc…` URL is stored; the signed CDN URL it derives is treated as ephemeral and regenerated on every attempt, so it is never persisted or logged. If a VK link ever required signing in, magsync marks the issue `unsupported` rather than prompting for credentials.

**LimeWire shares** use end-to-end encryption, and magsync implements the full decryption pipeline natively in Python:

1. Visit the LimeWire share page to get a session (JWT + CSRF token)
2. Extract metadata from the server-rendered HTML (bucket ID, encryption keys, etc.)
3. Derive the AES decryption key from the URL fragment:
   - **Short links** (`/d/bjAa5#passphrase`): PBKDF2 → AES-KW unwrap → ECDH key agreement
   - **UUID links** (`/d/{uuid}#base64key`): Direct ECDH key agreement
4. Fetch the presigned S3 download URL via LimeWire's API
5. Download the encrypted blob and decrypt with AES-256-CTR

No browser, Playwright, or Selenium is required or used. Direct source access may still be blocked by an upstream challenge; magsync detects and reports that state but does not attempt to bypass it.

Downloads are resilient to LimeWire throttling: a transient server error pauses all concurrent downloads briefly through a shared gate and consumes one bounded exponential-backoff budget. Issues with the same exact full share URL share one in-flight operation, while URLs with different key fragments remain distinct. Shares LimeWire reports as removed—or confirms twice as a live bucket with an empty content list—are marked unavailable instead of burning ordinary retries.

freemagazines.top rotates a post's LimeWire link when the old share is taken down, so magsync treats the stored link as self-healing — reactively. A download that fails on a dead link re-scrapes the page (resolving the masked key) and retries with the fresh link, and a share confirmed dead schedules a source-only refresh for a later cycle; a rotated link re-queues any issue previously parked as `unavailable`. Parked rows are re-probed on their own schedule rather than every cycle. Ordinary cycles deliberately do **not** re-resolve links for issues that already have a working URL: doing so would cost a request per issue per cycle to detect something the dead-link path already handles, so rotation is picked up when it matters rather than proactively.

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
