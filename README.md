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

Launches the terminal UI with three tabs:
- **Search** — type a magazine name, browse results, select issues, download
- **Downloads** — watch download progress
- **Library** — browse your indexed magazines by title/year/month

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

# View/change configuration
magsync config
magsync config output_dir ~/MyMagazines
```

## Subscriptions

Manage magazine subscriptions for daemon mode:

```bash
# Add a subscription
magsync subscribe "The New Yorker" --since 2025-01
magsync subscribe "The Economist" --since 2024-06

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
```

## Docker

Run magsync as an unattended daemon in Docker. Automatically fetches new issues on a schedule.

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
| `MAGSYNC_SUBSCRIPTIONS` | Comma-separated `query:since` pairs | (none) |
| `MAGSYNC_APPRISE_URLS` | Comma-separated [Apprise](https://github.com/caronc/apprise/wiki) notification URLs | (none) |
| `MAGSYNC_CONFIG_DIR` | Config directory path | `~/.magsync` |
| `MAGSYNC_DB_PATH` | SQLite index path | `{config_dir}/index.db` |
| `MAGSYNC_DOWNLOAD__MAX_CONCURRENT` | Max parallel downloads | `3` |
| `MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS` | Retries after a failed download (0 = no retry) | `2` |
| `MAGSYNC_DOWNLOAD__SCRAPE_DELAY` | Delay between scrape requests (seconds) | `1.0` |

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

magsync searches freemagazines.top (a WordPress site) via its search endpoint (`/?s=query`), follows pagination, and scrapes individual magazine detail pages to extract metadata and download links.

### Downloading

Downloads go through [LimeWire](https://limewire.com), a file-sharing service that uses end-to-end encryption. magsync implements the full decryption pipeline natively in Python:

1. Visit the LimeWire share page to get a session (JWT + CSRF token)
2. Extract metadata from the server-rendered HTML (bucket ID, encryption keys, etc.)
3. Derive the AES decryption key from the URL fragment:
   - **Short links** (`/d/bjAa5#passphrase`): PBKDF2 → AES-KW unwrap → ECDH key agreement
   - **UUID links** (`/d/{uuid}#base64key`): Direct ECDH key agreement
4. Fetch the presigned S3 download URL via LimeWire's API
5. Download the encrypted blob and decrypt with AES-256-CTR

No browser, Playwright, or Selenium required.

### Self-Healing

LimeWire's encryption constants (salt, IVs) are embedded in their JavaScript bundles and may change on deploys. If decryption produces an invalid PDF, magsync automatically:

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
