"""Tests for the retry_attempts=0 guard warning, boolean overrides and [metadata]."""

from __future__ import annotations

import logging

import pytest

import magsync.config as config_mod
from magsync.config import load_config


def test_retry_attempts_zero_warns_once(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path))  # clean config dir
    monkeypatch.setenv("MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS", "0")
    config_mod._warned_no_retries = False

    with caplog.at_level(logging.WARNING, logger="magsync"):
        load_config()
        load_config()  # second load must NOT warn again

    warnings = [r for r in caplog.records if "retries are disabled" in r.getMessage()]
    assert len(warnings) == 1


def test_retry_attempts_default_does_not_warn(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path))
    monkeypatch.delenv("MAGSYNC_DOWNLOAD__RETRY_ATTEMPTS", raising=False)
    config_mod._warned_no_retries = False

    with caplog.at_level(logging.WARNING, logger="magsync"):
        cfg = load_config()

    assert cfg.download.retry_attempts >= 1
    assert not [r for r in caplog.records if "retries are disabled" in r.getMessage()]


# --- boolean environment overrides and [metadata] ---------------------------------


@pytest.fixture
def clean_config(tmp_path, monkeypatch):
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path))
    monkeypatch.delenv("MAGSYNC_METADATA__STAMP", raising=False)
    return tmp_path


def test_metadata_stamp_defaults_to_true(clean_config):
    assert load_config().metadata.stamp is True


def test_metadata_stamp_loads_from_toml(clean_config):
    (clean_config / "config.toml").write_text("[metadata]\nstamp = false\n")
    assert load_config().metadata.stamp is False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("false", False), ("0", False), ("No", False), ("off", False),
     ("true", True), ("1", True), ("On", True), ("YES", True)],
)
def test_boolean_env_override_is_parsed_as_bool(clean_config, monkeypatch, raw, expected):
    monkeypatch.setenv("MAGSYNC_METADATA__STAMP", raw)
    value = load_config().metadata.stamp
    assert value is expected  # a bool, never the int 0/1


def test_boolean_env_override_wins_over_toml(clean_config, monkeypatch):
    (clean_config / "config.toml").write_text("[metadata]\nstamp = true\n")
    monkeypatch.setenv("MAGSYNC_METADATA__STAMP", "false")
    assert load_config().metadata.stamp is False


def test_invalid_boolean_env_override_names_the_variable(clean_config, monkeypatch):
    monkeypatch.setenv("MAGSYNC_METADATA__STAMP", "maybe")
    with pytest.raises(ValueError, match="MAGSYNC_METADATA__STAMP"):
        load_config()


def test_integer_and_float_env_overrides_still_parse(clean_config, monkeypatch):
    monkeypatch.setenv("MAGSYNC_DOWNLOAD__MAX_CONCURRENT", "5")
    monkeypatch.setenv("MAGSYNC_DOWNLOAD__SCRAPE_DELAY", "2.5")
    cfg = load_config()
    assert cfg.download.max_concurrent == 5
    assert cfg.download.scrape_delay == 2.5
