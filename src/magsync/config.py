"""Configuration management for magsync."""

from __future__ import annotations

import logging
import copy
import errno
import hashlib
import json
import tempfile
import os
import tomllib
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path

from magsync.core.locking import lock as _lock_fd
from magsync.core.models import Subscription

logger = logging.getLogger("magsync")
_warned_no_retries = False


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
    retry_attempts: int = 2
    scrape_delay: float = 1.0


@dataclass
class NotificationSettings:
    enabled: bool = False
    apprise_urls: list[str] = field(default_factory=list)


@dataclass
class Config:
    output_dir: str = field(default_factory=lambda: str(Path.home() / "Magazines"))
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

    # Subscriptions override (comma-separated query:since pairs, prefix ! for exact)
    # e.g. "!GQ USA:2025-01,The Economist" → GQ USA (exact, since 2025-01), The Economist (partial, all time)
    subs_env = os.environ.get("MAGSYNC_SUBSCRIPTIONS")
    if subs_env:
        cfg.subscriptions = []
        for entry in subs_env.split(","):
            entry = entry.strip()
            if not entry:
                continue
            exact = entry.startswith("!")
            if exact:
                entry = entry[1:]
            if ":" in entry:
                query, since = entry.rsplit(":", 1)
                cfg.subscriptions.append(Subscription(query=query.strip(), since=since.strip(), exact=exact))
            else:
                cfg.subscriptions.append(Subscription(query=entry, exact=exact))

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
        if "general" in data and "output_dir" in data["general"]:
            cfg.output_dir = data["general"]["output_dir"]
        elif isinstance(data.get("output_dir"), str):
            # Hand-written configs may set it at the top level; [general] wins.
            cfg.output_dir = data["output_dir"]
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
                    Subscription(
                        query=sub.get("query", ""),
                        since=sub.get("since"),
                        exact=sub.get("exact", False),
                    )
                )

    cfg._file_values = copy.deepcopy(asdict(cfg))
    _apply_env_overrides(cfg)
    cfg._loaded_values = copy.deepcopy(asdict(cfg))
    cfg._file_revision = hashlib.sha256(config_path.read_bytes() if config_path.exists() else b"").hexdigest()

    global _warned_no_retries
    if cfg.download.retry_attempts < 1 and not _warned_no_retries:
        _warned_no_retries = True
        logger.warning(
            "download.retry_attempts=%d — download retries are disabled; transient "
            "LimeWire errors will not be retried. Set it to >=1 (e.g. "
            "MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS=2) for resilience to transient failures.",
            cfg.download.retry_attempts,
        )
    return cfg


class ConfigurationConflict(OSError):
    """A configuration change could not be made durably effective.

    Raised when a field is environment-managed, changed externally, or the
    configuration cannot be written. The message is a complete sentence safe
    to show to the user.
    """


# Renames onto a mount point fail with EBUSY (EXDEV across filesystems); an
# unwritable directory prevents creating the temporary file next to the target.
_RENAME_IMPOSSIBLE = {errno.EBUSY, errno.EXDEV}
_DIRECTORY_UNWRITABLE = {errno.EACCES, errno.EPERM, errno.EROFS}


def _toml_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    return str(value)


def _read_only() -> ConfigurationConflict:
    return ConfigurationConflict("Configuration file is read-only; the change was not saved.")


def _changed_externally() -> ConfigurationConflict:
    return ConfigurationConflict(
        "Configuration file changed while saving; the change was not saved. Try again."
    )


def _open_for_lock(path: Path):
    """Open the configuration to lock it, creating it (private) when absent."""
    try:
        return open(path, "r+b")
    except FileNotFoundError:
        pass
    except OSError as exc:
        if exc.errno == errno.EROFS:
            raise _read_only() from None
        if exc.errno not in (errno.EACCES, errno.EPERM):
            raise
        # Not writable by this process. Replacing it may still succeed in a
        # writable directory, so a read handle serves as the lock.
        try:
            return open(path, "rb")
        except OSError as inner:
            if inner.errno in _DIRECTORY_UNWRITABLE:
                raise _read_only() from None
            raise
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError as exc:
        if exc.errno in _DIRECTORY_UNWRITABLE:
            raise ConfigurationConflict(
                "Configuration directory is read-only; the change was not saved."
            ) from None
        raise
    return os.fdopen(fd, "r+b")


@contextmanager
def _writer_lock(path: Path):
    """Serialize every writer of one configuration file; yield the locked handle.

    POSIX writers lock the configuration file itself. Writers that replace it
    atomically, writers that must rewrite it in place and writers that see it
    through another directory (a single file bind-mounted into a container)
    therefore all exclude each other, which no lock file beside it can
    guarantee. A replace swaps the file, so a writer that waited re-opens
    until it holds the lock on the file currently at ``path``.

    Windows locks are mandatory and an open file cannot be replaced there, so
    a lock file beside the configuration serializes writers instead.
    """
    if os.name == "nt":  # pragma: no cover - Windows
        try:
            handle = path.with_suffix(".lock").open("a")
        except OSError as exc:
            if exc.errno in _DIRECTORY_UNWRITABLE:
                raise ConfigurationConflict(
                    "Configuration directory is read-only; the change was not saved."
                ) from None
            raise
        with handle:
            _lock_fd(handle.fileno(), blocking=True)
            yield None
        return
    while True:
        with _open_for_lock(path) as handle:
            _lock_fd(handle.fileno(), blocking=True)
            try:
                current = os.stat(path)
            except FileNotFoundError:
                continue
            locked = os.fstat(handle.fileno())
            if (current.st_dev, current.st_ino) != (locked.st_dev, locked.st_ino):
                continue
            yield handle
            return


def _rewrite_in_place(path: Path, payload: bytes, prior: bytes, handle=None) -> None:
    """Rewrite the existing file (same inode) when it cannot be replaced.

    Not crash-atomic; used only when replacement is impossible, under the
    writer lock and after re-checking that nobody changed the file.
    """
    try:
        if handle is not None and handle.writable():
            stream = handle
        else:
            stream = open(path, "r+b" if path.exists() else "w+b")
    except OSError as exc:
        if exc.errno in _DIRECTORY_UNWRITABLE:
            raise _read_only() from None
        raise
    try:
        stream.seek(0)
        if stream.read() != prior:
            raise _changed_externally()
        stream.seek(0)
        stream.write(payload)
        stream.truncate()
        stream.flush()
        os.fsync(stream.fileno())
    except OSError as exc:
        if isinstance(exc, ConfigurationConflict):
            raise
        if exc.errno in _DIRECTORY_UNWRITABLE:
            raise _read_only() from None
        raise
    finally:
        if stream is not handle:
            stream.close()


def _commit(path: Path, payload: bytes, prior: bytes, handle=None) -> None:
    """Atomically replace the configuration, falling back to an in-place rewrite."""
    try:
        stream = tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=".config-", suffix=".tmp", delete=False
        )
    except OSError as exc:
        if exc.errno not in _DIRECTORY_UNWRITABLE:
            raise
        _rewrite_in_place(path, payload, prior, handle)
        return
    temporary = Path(stream.name)
    try:
        with stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if (path.read_bytes() if path.exists() else b"") != prior:
            raise _changed_externally()
        try:
            os.replace(temporary, path)
        except OSError as exc:
            if exc.errno not in _RENAME_IMPOSSIBLE:
                raise
            _rewrite_in_place(path, payload, prior, handle)
            return
        try:
            fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            return  # Directory handles are not openable on every platform.
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def save_config(cfg: Config) -> None:
    """Merge changed fields against the loaded revision under one writer lock.

    Only fields this ``cfg`` changed since it was loaded are written; they are
    merged into the file's current content, so concurrent writers (a
    subscription edit and self-healing constants) never discard each other.
    The file is replaced atomically when possible and rewritten in place when
    it is a mount point or its directory is read-only.
    """
    path = _get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    intended = asdict(cfg)
    baseline = getattr(cfg, "_loaded_values", asdict(Config()))
    original_file = getattr(cfg, "_file_values", asdict(Config()))
    changes = {}
    for section, value in intended.items():
        if isinstance(value, dict):
            for key, item in value.items():
                if item != baseline[section][key]:
                    changes[(section, key)] = item
        elif value != baseline[section]:
            changes[(section, None)] = value
    if not changes and path.exists():
        return
    with _writer_lock(path) as handle:
        if path.exists() and not path.stat().st_mode & 0o222:
            raise _read_only()
        prior = path.read_bytes() if path.exists() else b""
        data = tomllib.loads(prior.decode("utf-8")) if prior else {}
        fresh = load_config()
        if (path.read_bytes() if path.exists() else b"") != prior:
            raise _changed_externally()
        fresh_file = fresh._file_values
        for (section, key), value in changes.items():
            env = 'MAGSYNC_' + (section.upper() if key is None else section.upper() + '__' + key.upper())
            if section == 'notifications':
                env = 'MAGSYNC_APPRISE_URLS'
            if env in os.environ and os.environ[env]:
                raise ConfigurationConflict(
                    f"{env} is set in the environment and controls this setting; "
                    "the change was not saved."
                )
            previous = original_file[section][key] if key else original_file[section]
            current = fresh_file[section][key] if key else fresh_file[section]
            if current != previous and current != value:
                raise ConfigurationConflict(
                    "This setting was changed outside magsync since it was loaded; "
                    "the change was not saved."
                )
            if key:
                data.setdefault(section, {})[key] = value
            elif section == 'output_dir':
                # One source of truth: a hand-written top-level key moves under [general].
                data.pop('output_dir', None)
                data.setdefault('general', {})['output_dir'] = value
            else:
                data[section] = value
        # Keep all unrelated TOML sections, including operator extension keys.
        lines = []
        for section, value in data.items():
            if isinstance(value, dict):
                lines.append('[' + section + ']')
                lines.extend(key + ' = ' + _toml_value(item) for key, item in value.items() if item is not None)
                lines.append('')
            elif isinstance(value, list) and (not value or isinstance(value[0], dict)):
                for item in value:
                    lines.append('[[' + section + ']]')
                    lines.extend(key + ' = ' + _toml_value(val) for key, val in item.items() if val is not None)
                    lines.append('')
            else:
                lines.append(section + ' = ' + _toml_value(value))
        _commit(path, ('\n'.join(lines) + '\n').encode("utf-8"), prior, handle)
    fresh = load_config()
    cfg._file_values = fresh._file_values
    cfg._loaded_values = copy.deepcopy(asdict(cfg))
    cfg._file_revision = fresh._file_revision


def _parse_setting(key: str, current, value: str):
    """Parse a CLI string for a typed setting; raise ValueError with a sentence."""
    if isinstance(current, bool):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "on"):
            return True
        if lowered in ("false", "0", "no", "off"):
            return False
        raise ValueError(f"{key} must be true or false.")
    if isinstance(current, int):
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"{key} must be a whole number.") from None
    if isinstance(current, float):
        try:
            return float(value)
        except ValueError:
            raise ValueError(f"{key} must be a number.") from None
    if isinstance(current, list):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


def set_config_value(key: str, value: str) -> Config:
    """Set a single config value by dotted key (e.g., 'output_dir', 'download.max_concurrent').

    Raises ``ValueError`` (a user-facing sentence) for a missing value or an
    unknown/invalid key, and ``ConfigurationConflict`` when it cannot be saved.
    """
    if value is None:
        raise ValueError(f"A value is required to set {key}.")
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
        if target is None or isinstance(target, (list, str)):
            raise ValueError(f"Unknown config section: {section}")
        if name.startswith("_") or not hasattr(target, name):
            raise ValueError(f"Unknown config key: {key}")
        setattr(target, name, _parse_setting(key, getattr(target, name), value))
    else:
        raise ValueError(f"Invalid config key format: {key}")
    save_config(cfg)
    return cfg


def _same_title(left: str, right: str) -> bool:
    from magsync.core.organizer import strip_accents

    return strip_accents(left).lower() == strip_accents(right).lower()


def add_subscription(query: str, *, since: str | None = None, exact: bool = False) -> bool:
    """Subscribe the local configuration to ``query``.

    Returns False (and writes nothing) when an accent-insensitive match is
    already subscribed. Raises ``ConfigurationConflict`` when it cannot be saved.
    """
    cfg = load_config()
    if any(_same_title(sub.query, query) for sub in cfg.subscriptions):
        return False
    cfg.subscriptions.append(Subscription(query=query, since=since, exact=exact))
    save_config(cfg)
    return True


def remove_subscription(query: str) -> bool:
    """Unsubscribe ``query``; returns False (writing nothing) when not subscribed."""
    cfg = load_config()
    remaining = [sub for sub in cfg.subscriptions if not _same_title(sub.query, query)]
    if len(remaining) == len(cfg.subscriptions):
        return False
    cfg.subscriptions = remaining
    save_config(cfg)
    return True
