"""Tests for the retry_attempts=0 guard warning."""

from __future__ import annotations

import logging

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
