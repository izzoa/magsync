"""Configuration commands report rejected changes as sentences, never tracebacks."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from magsync.cli import app
from magsync.config import load_config, set_config_value

runner = CliRunner()


@pytest.fixture
def paths(tmp_path, monkeypatch):
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("MAGSYNC_DB_PATH", str(tmp_path / "data" / "index.db"))
    monkeypatch.setenv("MAGSYNC_EXPORT_DIR", str(tmp_path / "exports"))
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.toml").write_text(f'[general]\noutput_dir = "{tmp_path / "out"}"\n')
    return tmp_path


def test_environment_managed_subscriptions_are_a_clear_conflict(paths, monkeypatch):
    monkeypatch.setenv("MAGSYNC_SUBSCRIPTIONS", "Managed Title")
    result = runner.invoke(app, ["subscribe", "Science News"])
    assert result.exit_code == 1
    assert "MAGSYNC_SUBSCRIPTIONS" in result.output and "not saved" in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "Science News" not in (paths / "config" / "config.toml").read_text()


def test_unknown_key_is_reported_without_a_traceback(paths):
    result = runner.invoke(app, ["config", "download.nonexistent", "4"])
    assert result.exit_code == 1
    assert "Unknown config key: download.nonexistent" in result.output
    assert "Traceback" not in result.output


def test_boolean_and_numeric_values_are_parsed(paths):
    assert runner.invoke(app, ["config", "notifications.enabled", "true"]).exit_code == 0
    assert load_config().notifications.enabled is True
    assert runner.invoke(app, ["config", "notifications.enabled", "off"]).exit_code == 0
    assert load_config().notifications.enabled is False
    bad = runner.invoke(app, ["config", "download.max_concurrent", "many"])
    assert bad.exit_code == 1 and "must be a whole number" in bad.output
    with pytest.raises(ValueError, match="value is required"):
        set_config_value("download.max_concurrent", None)
