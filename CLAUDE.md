# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**magsync** is a Python CLI/TUI tool that indexes and downloads PDF magazines from freemagazines.top, organizing them into `[Magazine Title]/[YYYY]/[MM]/` directories. Downloads go through LimeWire's E2E encrypted file-sharing service, which magsync decrypts natively in Python (no browser needed).

## Tech Stack

- **Python 3.11+** with `src/magsync/` layout
- **httpx** + **BeautifulSoup4** for scraping freemagazines.top
- **cryptography** for LimeWire decryption (PBKDF2, AES-KW, ECDH P-256, AES-256-CTR)
- **Textual** for TUI, **Typer** + **Rich** for CLI
- **SQLite** for local magazine index (`~/.magsync/index.db`)

## Build & Run

```bash
pip install -e .           # Install in development mode
magsync                    # Launch TUI
magsync search "query"     # CLI search
magsync fetch "query"      # Download magazines
magsync update             # Refresh index for all tracked magazines
magsync config             # View/set configuration
```

## Architecture

```
src/magsync/
├── __main__.py          # Entry point (TUI default, CLI with subcommands)
├── cli.py               # Typer CLI commands
├── config.py            # ~/.magsync/config.toml management
├── core/
│   ├── models.py        # Dataclasses (Magazine, Issue, DownloadResult, etc.)
│   ├── scraper.py       # freemagazines.top scraping (search, detail pages)
│   ├── downloader.py    # LimeWire session, key derivation, download+decrypt
│   ├── index.py         # SQLite index (magazines, issues, downloads tables)
│   └── organizer.py     # Date parsing, title normalization, file placement
└── tui/
    └── app.py           # Textual TUI (search, downloads, library tabs)
```

## LimeWire Download Pipeline

The download chain (implemented in `core/downloader.py`):
1. `GET /d/{sharing_id}` → JWT cookie + SSR metadata (bucket ID, keys, etc.)
2. **Short ID path**: PBKDF2(fragment, salt) → AES-KW unwrap → ECDH private key
3. **UUID path**: fragment is the raw ECDH private key (base64url)
4. ECDH(private_key, ephemeralPublicKey) → AES-256 shared secret
5. `POST api.limewire.com/sharing/download/{bucket}` → presigned S3 URL
6. Download encrypted blob from S3 → AES-256-CTR decrypt → validate `%PDF` header

Encryption constants (salt, IVs) are in `~/.magsync/config.toml` and auto-extracted from LimeWire's JS bundles if decryption fails (self-healing). See `UPDATE_KEYS.md` for manual extraction process.

## Key Design Decisions

- **No browser dependency** — LimeWire's E2E encryption is implemented in pure Python
- **Self-healing constants** — on decryption failure, auto-scrapes LimeWire's JS bundles for fresh constants
- **Two LimeWire URL formats** — short ID (passphrase path) and UUID (raw key path)
- **User-Agent required** — freemagazines.top returns 403 without a browser User-Agent header

## Documentation Requirements

- **CHANGELOG.md**: ALL changes MUST be reflected in CHANGELOG.md, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.
- **README.md**: Substantial user-facing changes (new commands, new config options, new features, changed behavior) MUST be reflected in README.md.
- **Version bump**: On each release, bump the version in BOTH `pyproject.toml` and `src/magsync/__init__.py` to match the git tag.

## Development Workflow: OpenSpec

This project uses **OpenSpec** for spec-driven development.

| Command | Purpose |
|---------|---------|
| `/opsx:explore` | Think through ideas and investigate problems |
| `/opsx:propose` | Create a new change with proposal, design, and tasks |
| `/opsx:apply` | Implement tasks from an existing change |
| `/opsx:archive` | Archive a completed change |
