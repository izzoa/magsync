"""Configuration management for magsync."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path

from magsync.core.models import Subscription


def _get_app_dir() -> Path:
    return Path(os.environ.get("MAGSYNC_CONFIG_DIR", str(Path.home() / ".magsync")))


def _get_config_path() -> Path:
    return _get_app_dir() / "config.toml"


def _get_db_path() -> Path:
    env_db = os.environ.get("MAGSYNC_DB_PATH")
    if env_db:
        return Path(env_db)
    return _get_app_dir() / "index.db"


# Module-level properties for backward compatibility
APP_DIR = property(lambda self: _get_app_dir())
CONFIG_PATH = property(lambda self: _get_config_path())
DB_PATH = property(lambda self: _get_db_path())


def get_app_dir() -> Path:
    return _get_app_dir()


def get_config_path() -> Path:
    return _get_config_path()


def get_db_path() -> Path:
    return _get_db_path()


@dataclass
class LimeWireConstants:
    sharing_salt_b64: str = ""
    sharing_iv_b64: str = ""
    file_iv_b64: str = ""
    file_name_iv_b64: str = ""
    file_sha1_iv_b64: str = ""
    preview_iv_b64: str = ""
    pbkdf2_iterations: int = 100_000


@dataclass
class DownloadSettings:
    max_concurrent: int = 3
    retry_attempts: int = 3
    scrape_delay: float = 1.0


@dataclass
class NotificationSettings:
    enabled: bool = False
    apprise_urls: list[str] = field(default_factory=list)


@dataclass
class Config:
    output_dir: str = str(Path.home() / "Magazines")
    download: DownloadSettings = field(default_factory=DownloadSettings)
    limewire: LimeWireConstants = field(default_factory=LimeWireConstants)
    notifications: NotificationSettings = field(default_factory=NotificationSettings)
    subscriptions: list[Subscription] = field(default_factory=list)


def _apply_env_overrides(cfg: Config) -> None:
    """Apply MAGSYNC_-prefixed environment variable overrides.

    Convention: MAGSYNC_OUTPUT_DIR, MAGSYNC_DOWNLOAD__MAX_CONCURRENT (double underscore for nesting).
    """
    env_map = {
        "MAGSYNC_OUTPUT_DIR": ("output_dir", None),
        "MAGSYNC_DOWNLOAD__MAX_CONCURRENT": ("max_concurrent", "download"),
        "MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS": ("retry_attempts", "download"),
        "MAGSYNC_DOWNLOAD__SCRAPE_DELAY": ("scrape_delay", "download"),
        "MAGSYNC_LIMEWIRE__SHARING_SALT_B64": ("sharing_salt_b64", "limewire"),
        "MAGSYNC_LIMEWIRE__SHARING_IV_B64": ("sharing_iv_b64", "limewire"),
        "MAGSYNC_LIMEWIRE__FILE_IV_B64": ("file_iv_b64", "limewire"),
        "MAGSYNC_LIMEWIRE__FILE_NAME_IV_B64": ("file_name_iv_b64", "limewire"),
        "MAGSYNC_LIMEWIRE__FILE_SHA1_IV_B64": ("file_sha1_iv_b64", "limewire"),
        "MAGSYNC_LIMEWIRE__PREVIEW_IV_B64": ("preview_iv_b64", "limewire"),
        "MAGSYNC_LIMEWIRE__PBKDF2_ITERATIONS": ("pbkdf2_iterations", "limewire"),
    }

    for env_key, (attr_name, section) in env_map.items():
        val = os.environ.get(env_key)
        if val is None:
            continue
        target = getattr(cfg, section) if section else cfg
        current = getattr(target, attr_name)
        if isinstance(current, int):
            setattr(target, attr_name, int(val))
        elif isinstance(current, float):
            setattr(target, attr_name, float(val))
        elif isinstance(current, bool):
            setattr(target, attr_name, val.lower() in ("true", "1", "yes"))
        else:
            setattr(target, attr_name, val)

    # Subscriptions override (comma-separated query:since pairs)
    subs_env = os.environ.get("MAGSYNC_SUBSCRIPTIONS")
    if subs_env:
        cfg.subscriptions = []
        for entry in subs_env.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if ":" in entry:
                query, since = entry.rsplit(":", 1)
                cfg.subscriptions.append(Subscription(query=query.strip(), since=since.strip()))
            else:
                cfg.subscriptions.append(Subscription(query=entry))

    # Apprise URLs override
    apprise_env = os.environ.get("MAGSYNC_APPRISE_URLS")
    if apprise_env:
        cfg.notifications.apprise_urls = [u.strip() for u in apprise_env.split(",") if u.strip()]
        cfg.notifications.enabled = bool(cfg.notifications.apprise_urls)


def load_config() -> Config:
    """Load config from config.toml, apply env var overrides, return Config."""
    cfg = Config()
    config_path = _get_config_path()
    if config_path.exists():
        with open(config_path, "rb") as f:
            data = tomllib.load(f)
        if "general" in data:
            if "output_dir" in data["general"]:
                cfg.output_dir = data["general"]["output_dir"]
        if "download" in data:
            for key in ("max_concurrent", "retry_attempts", "scrape_delay"):
                if key in data["download"]:
                    setattr(cfg.download, key, data["download"][key])
        if "limewire" in data:
            for f_info in fields(LimeWireConstants):
                if f_info.name in data["limewire"]:
                    setattr(cfg.limewire, f_info.name, data["limewire"][f_info.name])
        if "notifications" in data:
            if "enabled" in data["notifications"]:
                cfg.notifications.enabled = data["notifications"]["enabled"]
            if "apprise_urls" in data["notifications"]:
                cfg.notifications.apprise_urls = data["notifications"]["apprise_urls"]
        if "subscriptions" in data:
            for sub in data["subscriptions"]:
                cfg.subscriptions.append(
                    Subscription(query=sub.get("query", ""), since=sub.get("since"))
                )

    _apply_env_overrides(cfg)
    return cfg


def save_config(cfg: Config) -> None:
    """Save config to config.toml."""
    app_dir = _get_app_dir()
    app_dir.mkdir(parents=True, exist_ok=True)
    config_path = _get_config_path()

    lines = [
        "[general]",
        f'output_dir = "{cfg.output_dir}"',
        "",
        "[download]",
        f"max_concurrent = {cfg.download.max_concurrent}",
        f"retry_attempts = {cfg.download.retry_attempts}",
        f"scrape_delay = {cfg.download.scrape_delay}",
        "",
    ]

    # Only write [limewire] when constants have been populated (via auto-extraction or manual config)
    if cfg.limewire.file_iv_b64:
        lines += [
            "[limewire]",
            f'sharing_salt_b64 = "{cfg.limewire.sharing_salt_b64}"',
            f'sharing_iv_b64 = "{cfg.limewire.sharing_iv_b64}"',
            f'file_iv_b64 = "{cfg.limewire.file_iv_b64}"',
            f'file_name_iv_b64 = "{cfg.limewire.file_name_iv_b64}"',
            f'file_sha1_iv_b64 = "{cfg.limewire.file_sha1_iv_b64}"',
            f'preview_iv_b64 = "{cfg.limewire.preview_iv_b64}"',
            f"pbkdf2_iterations = {cfg.limewire.pbkdf2_iterations}",
            "",
        ]

    lines += [
        "[notifications]",
        f"enabled = {'true' if cfg.notifications.enabled else 'false'}",
        f"apprise_urls = [{', '.join(repr(u) for u in cfg.notifications.apprise_urls)}]",
        "",
    ]

    for sub in cfg.subscriptions:
        lines.append("[[subscriptions]]")
        lines.append(f'query = "{sub.query}"')
        if sub.since:
            lines.append(f'since = "{sub.since}"')
        lines.append("")

    config_path.write_text("\n".join(lines))


def set_config_value(key: str, value: str) -> Config:
    """Set a single config value by dotted key (e.g., 'output_dir', 'download.max_concurrent')."""
    cfg = load_config()
    parts = key.split(".")
    if len(parts) == 1:
        if parts[0] == "output_dir":
            cfg.output_dir = value
        else:
            raise ValueError(f"Unknown config key: {key}")
    elif len(parts) == 2:
        section, name = parts
        target = getattr(cfg, section, None)
        if target is None:
            raise ValueError(f"Unknown config section: {section}")
        if not hasattr(target, name):
            raise ValueError(f"Unknown config key: {key}")
        current = getattr(target, name)
        if isinstance(current, int):
            setattr(target, name, int(value))
        elif isinstance(current, float):
            setattr(target, name, float(value))
        else:
            setattr(target, name, value)
    else:
        raise ValueError(f"Invalid config key format: {key}")
    save_config(cfg)
    return cfg
