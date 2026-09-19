"""Suite-wide isolation from the developer's real magsync installation."""

from __future__ import annotations

import pytest

_MAGSYNC_ENV = (
    "MAGSYNC_CONFIG_DIR", "MAGSYNC_DB_PATH", "MAGSYNC_OUTPUT_DIR", "MAGSYNC_EXPORT_DIR",
    "MAGSYNC_SUBSCRIPTIONS", "MAGSYNC_APPRISE_URLS", "MAGSYNC_INTERVAL",
)


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path_factory, monkeypatch):
    """Point ``~`` at a scratch directory for every test.

    The default configuration directory (``~/.magsync``) and output root
    (``~/Magazines``) both derive from the home directory, and ownership
    places lock files in the output root. Without this, tests that rely on
    defaults would read the developer's real configuration and write lock
    files into their real magazine library.
    """
    home = tmp_path_factory.mktemp("home")
    monkeypatch.setenv("HOME", str(home))
    for name in _MAGSYNC_ENV:
        monkeypatch.delenv(name, raising=False)
    # Runtimes touch the container liveness file; keep tests off the real one.
    from magsync.companion import healthcheck
    monkeypatch.setattr(healthcheck, "HEALTH_CHECK_PATH", home / "magsync-healthy")
