"""Configuration writes on filesystems where atomic replacement is impossible."""

from __future__ import annotations

import errno
import os
import threading
import time

import pytest

import magsync.config as config_module
from magsync.config import ConfigurationConflict, load_config, save_config, set_config_value
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


def _contend(path, events, started):
    def contender():
        started.set()
        with config_module._writer_lock(path) as handle:
            events.append(("second", os.fstat(handle.fileno()).st_ino))
    thread = threading.Thread(target=contender, daemon=True)
    thread.start()
    return thread


@pytest.mark.skipif(os.name == "nt", reason="POSIX writers lock the configuration file itself")
def test_writers_seeing_one_file_through_different_directories_share_a_lock(config_dir, tmp_path):
    # A host process and a container that bind-mounts only config.toml see
    # one file through two directories; a hard link reproduces that here.
    mounted = tmp_path / "mounted"
    mounted.mkdir()
    os.link(config_dir / "config.toml", mounted / "config.toml")
    events, started = [], threading.Event()
    with config_module._writer_lock(config_dir / "config.toml"):
        thread = _contend(mounted / "config.toml", events, started)
        assert started.wait(5)
        time.sleep(0.2)
        events.append(("first", None))
    thread.join(5)
    assert [name for name, _ in events] == ["first", "second"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX writers lock the configuration file itself")
def test_a_writer_that_waited_through_a_replace_locks_the_new_file(config_dir, monkeypatch):
    path = config_dir / "config.toml"
    real_lock = config_module._lock_fd
    blocking = threading.Event()

    def lock(fd, **kwargs):
        if threading.current_thread() is not threading.main_thread():
            blocking.set()  # The contender opened the current file and is about to wait.
        return real_lock(fd, **kwargs)

    monkeypatch.setattr(config_module, "_lock_fd", lock)
    events, started = [], threading.Event()
    with config_module._writer_lock(path):
        thread = _contend(path, events, started)
        assert blocking.wait(5)
        replacement = config_dir / "replacement.toml"
        replacement.write_text(path.read_text())
        os.replace(replacement, path)  # Another writer's atomic save.
    thread.join(5)
    assert events == [("second", path.stat().st_ino)]


def test_concurrent_saves_through_a_single_file_mount_keep_both_changes(config_dir, monkeypatch):
    monkeypatch.setattr(config_module.os, "replace", _busy_replace)
    first, second = load_config(), load_config()
    first.subscriptions.append(Subscription(query="Science News"))
    second.limewire.file_iv_b64 = "healed-iv"
    threads = [threading.Thread(target=save_config, args=(cfg,)) for cfg in (first, second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    saved = load_config()
    assert [s.query for s in saved.subscriptions] == ["Science News"]
    assert saved.limewire.file_iv_b64 == "healed-iv"


def test_first_save_creates_a_private_configuration(tmp_path, monkeypatch):
    directory = tmp_path / "fresh"
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(directory))
    for name in ("MAGSYNC_SUBSCRIPTIONS", "MAGSYNC_OUTPUT_DIR", "MAGSYNC_APPRISE_URLS"):
        monkeypatch.delenv(name, raising=False)
    cfg = load_config()
    cfg.subscriptions.append(Subscription(query="Dish"))
    save_config(cfg)
    assert [s.query for s in load_config().subscriptions] == ["Dish"]
    assert (directory / "config.toml").stat().st_mode & 0o077 == 0
    assert not (directory / "config.lock").exists()


def test_top_level_output_dir_is_honored_and_saved_under_general(config_dir):
    path = config_dir / "config.toml"
    path.write_text('output_dir = "/top"\n\n[extension]\nvalue = "kept"\n')
    assert load_config().output_dir == "/top"
    set_config_value("output_dir", "/library")
    assert load_config().output_dir == "/library"
    text = path.read_text()
    assert text.count("output_dir") == 1 and "[general]" in text and 'value = "kept"' in text
    path.write_text('output_dir = "/top"\n\n[general]\noutput_dir = "/general"\n')
    assert load_config().output_dir == "/general"
