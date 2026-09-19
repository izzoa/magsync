"""Terminal commands against a live owner, a temporary owner, or none.

The live-owner tests run a real background runtime in its own thread and
event loop (with its own SQLite connection), exactly as a daemon would, while
the CLI runs in the main thread through ``CliRunner``.
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
import time

import pytest
from typer.testing import CliRunner

import magsync.cli as cli
from magsync.cli import app
from magsync.companion.cli import service_limits
from magsync.companion.local import coordinated
from magsync.companion.ownership import Ownership
from magsync.companion.runtime import Runtime
from magsync.companion.store import Store
from magsync.config import load_config
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus, SourceResult
from magsync.core.scraper import ScrapedIssue

runner = CliRunner()
PDF = b"%PDF-1.7\ncontrolled test fixture\n%%EOF\n"
LW = "https://limewire.com/d/{}#key"
ISSUE = ScrapedIssue(title="Science News - June 2025", page_url="https://freemagazines.top/sn-june-2025/",
                     limewire_url=LW.format("sn"))


class Source:
    circuit_open = False
    circuit_failure = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    async def search_with_details(self, query, **_kwargs):
        return SourceResult(items=[ISSUE])


@pytest.fixture
def paths(tmp_path, monkeypatch):
    for name, value in {
        "MAGSYNC_CONFIG_DIR": tmp_path / "cfg", "MAGSYNC_DB_PATH": tmp_path / "data" / "index.db",
        "MAGSYNC_EXPORT_DIR": tmp_path / "exports", "MAGSYNC_NO_PROGRESS": "1",
        "MAGSYNC_SERVICE__MINIMUM_FREE_BYTES": "1024",
        "MAGSYNC_SERVICE__MAXIMUM_DOWNLOAD_BYTES": str(1024 * 1024),
        "MAGSYNC_SERVICE__COMMAND_POLL_SECONDS": "0.05",
        "MAGSYNC_SERVICE__HEARTBEAT_SECONDS": "0.2",
    }.items():
        monkeypatch.setenv(name, str(value))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(f'[general]\noutput_dir = "{tmp_path / "out"}"\n')
    return tmp_path


def _operations(paths):
    idx = MagazineIndex(paths / "data" / "index.db")
    try:
        return [tuple(row) for row in idx.conn.execute("SELECT kind,state FROM operations")]
    finally:
        idx.close()


@pytest.fixture
def live_runtime(paths):
    """A background runtime owning the store from another thread."""
    state: dict = {}
    ready = threading.Event()

    async def downloads(issues, _cfg, index, **_kwargs):
        results = []
        for issue in issues:
            target = paths / f"{issue['id']}.pdf"
            target.write_bytes(PDF)
            index.update_download_status(issue["id"], DownloadStatus.COMPLETE, str(target), len(PDF),
                                         hashlib.sha256(PDF).hexdigest())
            results.append({"issue": issue, "success": True, "error": None, "failure_kind": None})
        return results

    def run():
        async def main():
            idx = MagazineIndex(paths / "data" / "index.db")
            runtime = Runtime(idx, load_config(), limits=service_limits(), exports=paths / "exports",
                              source_factory=lambda **_: Source(), batch=downloads, require_initialized=False)
            await runtime.start(background=True)
            state.update(runtime=runtime, loop=asyncio.get_running_loop())
            ready.set()
            await asyncio.gather(runtime.loop_task, return_exceptions=True)
            await runtime.stop()
            idx.close()
        asyncio.run(main())

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert ready.wait(10)
    # Wait for the first heartbeat to advertise acceptance.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        idx = MagazineIndex(paths / "data" / "index.db")
        accepting = idx.conn.execute("SELECT accepting FROM runtime_state").fetchone()[0]
        idx.close()
        if accepting:
            break
        time.sleep(0.05)
    yield state
    state["loop"].call_soon_threadsafe(state["runtime"].request_stop)
    thread.join(20)


def _hold_temporary_owner(paths, seconds: float, started: threading.Event) -> threading.Thread:
    def hold():
        idx = MagazineIndex(paths / "data" / "index.db")
        owner = Ownership(Store(idx), paths / "out", paths / "exports")
        owner.acquire(accepting=False)
        started.set()
        time.sleep(seconds)
        owner.release()
        idx.close()
    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert started.wait(5)
    return thread


# ---------------------------------------------------------------------------
# 3.1 read-only commands never coordinate
# ---------------------------------------------------------------------------

def test_reading_configuration_while_the_daemon_runs_changes_nothing(paths, live_runtime):
    before = (paths / "cfg" / "config.toml").read_text()
    for arguments in (["config", "download.max_concurrent"], ["config", "output_dir"],
                      ["config", "output_dir", ""], ["subscribe"]):
        result = runner.invoke(app, arguments)
        assert result.exit_code == 0, (arguments, result.output)
    assert (paths / "cfg" / "config.toml").read_text() == before
    assert _operations(paths) == []
    runtime = live_runtime["runtime"]
    assert runtime.ready and not runtime.fatal


# ---------------------------------------------------------------------------
# 9.4 runtime-executed commands render like standalone ones
# ---------------------------------------------------------------------------

def test_commands_executed_by_the_daemon_render_like_standalone(paths, live_runtime):
    subscribed = runner.invoke(app, ["subscribe", "Dish", "--since", "2025-01"])
    assert subscribed.exit_code == 0 and "Subscribed to 'Dish' since 2025-01" in subscribed.output
    again = runner.invoke(app, ["subscribe", "dish"])
    assert again.exit_code == 0 and "Already subscribed to 'dish'" in again.output
    unknown = runner.invoke(app, ["unsubscribe", "Never Subscribed"])
    assert unknown.exit_code == 0 and "No subscription found for 'Never Subscribed'" in unknown.output
    changed = runner.invoke(app, ["config", "download.max_concurrent", "4"])
    assert changed.exit_code == 0 and "Set download.max_concurrent = 4" in changed.output
    invalid = runner.invoke(app, ["config", "download.max_concurrent", "many"])
    assert invalid.exit_code == 1 and "must be a whole number" in invalid.output

    searched = runner.invoke(app, ["search", "Science News"])
    assert searched.exit_code == 0, searched.output
    assert "Results for 'Science News'" in searched.output and "Science News - June 2025" in searched.output

    fetched = runner.invoke(app, ["fetch", "Science News"])
    assert fetched.exit_code == 0, fetched.output
    assert "✓ Science News - June 2025: downloaded" in fetched.output

    assert "No failed downloads to retry." in runner.invoke(app, ["retry"]).output
    assert "No titles need repair." in runner.invoke(app, ["repair-titles"]).output
    assert "{" not in fetched.output  # Never a raw operation document.
    states = _operations(paths)
    assert [kind for kind, state in states if state != "succeeded"] == ["local.config"]  # the invalid value


# ---------------------------------------------------------------------------
# 3.4 contention with a temporary owner
# ---------------------------------------------------------------------------

def test_second_command_waits_for_a_temporary_owner_then_runs(paths):
    started = threading.Event()
    holder = _hold_temporary_owner(paths, 1.0, started)
    result = runner.invoke(app, ["subscribe", "Getaway"])
    holder.join()
    assert result.exit_code == 0, result.output
    assert "Waiting for another magsync command to finish" in result.output
    assert [s.query for s in load_config().subscriptions] == ["Getaway"]


def test_busy_temporary_owner_is_reported_and_nothing_is_queued(paths, monkeypatch):
    monkeypatch.setenv("MAGSYNC_SERVICE__LOCK_WAIT_SECONDS", "0.5")
    started = threading.Event()
    holder = _hold_temporary_owner(paths, 2.0, started)
    result = runner.invoke(app, ["subscribe", "Getaway"])
    holder.join()
    assert result.exit_code == 1
    assert "Another magsync command is still running" in result.output
    assert _operations(paths) == []
    assert load_config().subscriptions == []


# ---------------------------------------------------------------------------
# 3.3 a temporary owner's own operation is never re-queued
# ---------------------------------------------------------------------------

def test_failed_or_interrupted_temporary_commands_are_never_replayed(paths, monkeypatch):
    async def exploding(*_args, **_kwargs):
        raise RuntimeError("source client bug")

    monkeypatch.setattr(cli, "search_with_details_result", exploding)
    monkeypatch.setattr(cli, "FreemagazinesClient", lambda **_kw: Source())
    result = runner.invoke(app, ["search", "Science News"])
    assert result.exit_code != 0

    @coordinated("update")
    def interrupted():
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        interrupted()
    assert sorted(_operations(paths)) == [("local.search", "failed"), ("local.update", "failed")]


# ---------------------------------------------------------------------------
# 9.5 dry runs list cached work without source traffic
# ---------------------------------------------------------------------------

def _seed_pending(paths, *, manual=False):
    idx = MagazineIndex(paths / "data" / "index.db")
    magazine = idx.get_or_create_magazine("Science News", "science news")
    idx.add_issues(magazine, [{"title": "Science News - June 2025", "page_url": ISSUE.page_url,
                               "limewire_url": ISSUE.limewire_url, "year": 2025, "month": 6, "file_size": "42 MB"}])
    if manual:
        idx.mark_manual([idx.conn.execute("SELECT id FROM issues").fetchone()[0]])
    idx.close()


def test_dry_runs_list_cached_work_without_source_requests(paths, monkeypatch):
    def no_source(**_kwargs):
        raise AssertionError("dry runs must not contact the source")

    monkeypatch.setattr(cli, "FreemagazinesClient", no_source)
    unknown = runner.invoke(app, ["fetch", "Unknown Title", "--dry-run"])
    assert unknown.exit_code == 0 and "No cached issues match 'Unknown Title'" in unknown.output
    _seed_pending(paths, manual=True)
    preview = runner.invoke(app, ["fetch", "Science News", "--dry-run"])
    assert preview.exit_code == 0, preview.output
    assert "Science News - June 2025" in preview.output and "cached catalog only" in preview.output
    daemon = runner.invoke(app, ["daemon", "--dry-run"])
    assert daemon.exit_code == 0, daemon.output
    assert "Science News - June 2025" in daemon.output and "due link refreshes would be attempted" in daemon.output
    idx = MagazineIndex(paths / "data" / "index.db")
    try:  # The previews ran on private copies: the live store gained no demand.
        assert idx.conn.execute("SELECT count(*) FROM acquisition_requests").fetchone()[0] == 0
        assert idx.conn.execute("SELECT count(*) FROM operations").fetchone()[0] == 0
    finally:
        idx.close()
    bad = runner.invoke(app, ["fetch", "Science News", "--dry-run", "--since", "June"])
    assert bad.exit_code == 2 and "YYYY-MM" in bad.output
