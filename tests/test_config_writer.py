"""Configuration writes on filesystems where atomic replacement is impossible."""

from __future__ import annotations

import errno

import pytest

import magsync.config as config_module
from magsync.config import ConfigurationConflict, load_config, save_config
from magsync.core.models import Subscription


@pytest.fixture
def config_dir(tmp_path, monkeypatch):
    directory = tmp_path / "config"
    directory.mkdir()
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(directory))
    for name in ("MAGSYNC_SUBSCRIPTIONS", "MAGSYNC_OUTPUT_DIR", "MAGSYNC_APPRISE_URLS"):
        monkeypatch.delenv(name, raising=False)
    (directory / "config.toml").write_text('[general]\noutput_dir = "/magazines"\n\n[extension]\nvalue = "kept"\n')
    yield directory
    directory.chmod(0o755)


def _busy_replace(*_args, **_kwargs):
    raise OSError(errno.EBUSY, "Device or resource busy")


def test_single_file_mount_is_rewritten_in_place(config_dir, monkeypatch):
    path = config_dir / "config.toml"
    inode = path.stat().st_ino
    monkeypatch.setattr(config_module.os, "replace", _busy_replace)
    cfg = load_config()
    cfg.subscriptions.append(Subscription(query="Science News", since="2025-01"))
    save_config(cfg)
    healed = load_config()
    healed.limewire.file_iv_b64 = "healed-iv"
    save_config(healed)
    saved = load_config()
    assert [s.query for s in saved.subscriptions] == ["Science News"]
    assert saved.limewire.file_iv_b64 == "healed-iv"
    assert saved.output_dir == "/magazines"
    assert 'value = "kept"' in path.read_text()
    assert path.stat().st_ino == inode
    assert not [p for p in config_dir.iterdir() if p.name.startswith(".config-")]


def test_read_only_directory_serializes_on_the_file_itself(config_dir):
    path = config_dir / "config.toml"
    config_dir.chmod(0o555)
    cfg = load_config()
    cfg.subscriptions.append(Subscription(query="Dish", exact=True))
    save_config(cfg)
    saved = load_config()
    assert saved.subscriptions[0].query == "Dish" and saved.subscriptions[0].exact
    assert not (config_dir / "config.lock").exists()
    assert 'value = "kept"' in path.read_text()


def test_in_place_rewrite_detects_a_concurrent_external_edit(config_dir, monkeypatch):
    path = config_dir / "config.toml"

    def edit_then_busy(*_args, **_kwargs):
        path.write_text(path.read_text() + '\n[other]\nkey = "external"\n')
        raise OSError(errno.EBUSY, "Device or resource busy")

    monkeypatch.setattr(config_module.os, "replace", edit_then_busy)
    cfg = load_config()
    cfg.subscriptions.append(Subscription(query="Getaway"))
    with pytest.raises(ConfigurationConflict):
        save_config(cfg)
    assert 'key = "external"' in path.read_text()
    assert "Getaway" not in path.read_text()


def test_unwritable_file_on_a_mount_is_a_conflict(config_dir, monkeypatch):
    monkeypatch.setattr(config_module.os, "replace", _busy_replace)
    real_open = open

    def read_only_open(file, mode="r", *args, **kwargs):
        if str(file).endswith("config.toml") and ("+" in mode or "w" in mode):
            raise OSError(errno.EROFS, "Read-only file system")
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr("builtins.open", read_only_open)
    cfg = load_config()
    cfg.subscriptions.append(Subscription(query="Olive"))
    with pytest.raises(ConfigurationConflict, match="read-only"):
        save_config(cfg)
    assert "Olive" not in (config_dir / "config.toml").read_text()
