"""Tests for the coordinated batch progress/logging helper and CLI flags."""

from __future__ import annotations

import inspect
import io
import logging

import pytest
from rich.console import Console

import magsync.cli as cli
from magsync.output import LOGGER_NAME, BatchOutput, resolve_mode


class _FakeStdout:
    def __init__(self, tty: bool):
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty

    def write(self, *_a):  # Rich may probe/write; no-op
        pass

    def flush(self):
        pass


def _use_tty(monkeypatch, tty: bool):
    monkeypatch.delenv("MAGSYNC_NO_PROGRESS", raising=False)
    monkeypatch.setattr("sys.stdout", _FakeStdout(tty))


def _capture_console() -> tuple[Console, io.StringIO]:
    buf = io.StringIO()
    # force_terminal=False → plain text, no ANSI (mirrors a piped run)
    return Console(file=buf, force_terminal=False, width=100), buf


def _batch(console, total, **kw) -> BatchOutput:
    kw.setdefault("use_live_bar", False)
    kw.setdefault("log_level", logging.WARNING)
    return BatchOutput(console, total, **kw)


# --- resolve_mode ---------------------------------------------------------

def test_resolve_mode_tty_defaults(monkeypatch):
    _use_tty(monkeypatch, True)
    use_bar, level = resolve_mode(verbose=False, quiet=False, no_progress=False)
    assert use_bar is True and level == logging.WARNING


def test_resolve_mode_non_tty_no_bar(monkeypatch):
    _use_tty(monkeypatch, False)
    use_bar, level = resolve_mode(False, False, False)
    assert use_bar is False and level == logging.WARNING


def test_resolve_mode_no_progress_flag_and_env(monkeypatch):
    _use_tty(monkeypatch, True)
    assert resolve_mode(False, False, True)[0] is False
    monkeypatch.setenv("MAGSYNC_NO_PROGRESS", "1")
    assert resolve_mode(False, False, False)[0] is False


def test_resolve_mode_verbose_and_quiet_levels(monkeypatch):
    _use_tty(monkeypatch, True)
    assert resolve_mode(True, False, False)[1] == logging.DEBUG
    use_bar, level = resolve_mode(False, True, False)
    assert use_bar is False and level == logging.ERROR   # quiet → no bar, errors still surface


def test_resolve_mode_conflict():
    with pytest.raises(ValueError):
        resolve_mode(verbose=True, quiet=True, no_progress=False)


# --- BatchOutput counters + classification --------------------------------

def test_on_complete_classifies_outcomes():
    console, buf = _capture_console()
    with _batch(console, 3) as out:
        out.on_complete({"title": "A"}, True, None)
        out.on_complete({"title": "B"}, False, "LimeWire share link is unavailable (removed or expired)")
        out.on_complete({"title": "C"}, False, "HTTP 500 transient")
    assert out.counts == {"downloaded": 1, "unavailable": 1, "failed": 1}


def test_summarize_reconciles_from_results_including_aborts():
    console, buf = _capture_console()
    # download_batch's abort path returns failures WITHOUT firing callbacks.
    results = [
        {"issue": {}, "success": True},
        {"issue": {}, "success": False, "error": "Encryption constants unavailable"},
        {"issue": {}, "success": False, "error": "Encryption constants unavailable"},
    ]
    with _batch(console, 3) as out:
        pass  # no callbacks fired at all
    counts = out.summarize(results)
    assert counts == {"downloaded": 1, "unavailable": 0, "failed": 2}
    text = buf.getvalue()
    assert "2 failed" in text and "unavailable (dead links)" in text


def test_summary_reports_unavailable_count():
    console, buf = _capture_console()
    results = [{"issue": {}, "success": False, "error": "share link is unavailable"} for _ in range(4)]
    with _batch(console, 4) as out:
        pass
    out.summarize(results)
    assert "4 unavailable (dead links)" in buf.getvalue()


def test_non_tty_emits_throttled_lines_not_one_per_item():
    console, buf = _capture_console()
    with _batch(console, 40) as out:
        for _ in range(40):
            out.record("downloaded")
    lines = [ln for ln in buf.getvalue().splitlines() if ln.startswith("progress:")]
    assert 0 < len(lines) < 40                 # throttled, not one-per-item
    assert any("40/40" in ln for ln in lines)  # includes the final line


# --- logger lifecycle -----------------------------------------------------

def test_logger_state_restored_normal_and_on_exception():
    logger = logging.getLogger(LOGGER_NAME)
    before = (logger.level, logger.propagate, list(logger.handlers))

    console, _ = _capture_console()
    with _batch(console, 1, log_level=logging.DEBUG):
        assert logger.propagate is False
        assert logger.level == logging.DEBUG
    assert (logger.level, logger.propagate, list(logger.handlers)) == before

    with pytest.raises(RuntimeError):
        with _batch(console, 1, log_level=logging.DEBUG):
            raise RuntimeError("boom")
    assert (logger.level, logger.propagate, list(logger.handlers)) == before


def test_verbose_raises_logger_level_so_info_is_emitted():
    console, buf = _capture_console()
    logger = logging.getLogger(LOGGER_NAME)
    # Default (no explicit level) would filter INFO; the helper must raise the
    # logger's own level, not just the handler's, for -v to work.
    with _batch(console, 1, log_level=logging.DEBUG):
        logger.info("hello-verbose-line")
    assert "hello-verbose-line" in buf.getvalue()


def test_quiet_level_hides_info_but_shows_errors():
    console, buf = _capture_console()
    logger = logging.getLogger(LOGGER_NAME)
    with _batch(console, 1, log_level=logging.ERROR):
        logger.info("hidden-info")
        logger.error("visible-error")
    out = buf.getvalue()
    assert "hidden-info" not in out and "visible-error" in out


# --- daemon guard ---------------------------------------------------------

def test_daemon_does_not_use_batch_output_helper():
    src = inspect.getsource(cli.daemon)
    assert "_batch_output" not in src and "BatchOutput" not in src
    assert "basicConfig" in src   # daemon keeps its own structured logging


# --- CLI smoke via CliRunner ---------------------------------------------

from typer.testing import CliRunner  # noqa: E402

from magsync.core.index import MagazineIndex  # noqa: E402
from magsync.core.models import DownloadResult, DownloadStatus  # noqa: E402

runner = CliRunner()


def _seed_failed_issue(tmp_path, monkeypatch):
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MAGSYNC_DB_PATH", str(tmp_path / "index.db"))
    monkeypatch.delenv("MAGSYNC_NO_PROGRESS", raising=False)
    idx = MagazineIndex(db_path=tmp_path / "index.db")
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(mag, [{"title": "Dead Issue - Jan 2026", "page_url": "p1",
                          "limewire_url": "https://limewire.com/d/x#k", "year": 2026, "month": 1}])
    issue_id = idx.get_issues()[0]["id"]
    idx.update_download_status(issue_id, DownloadStatus.FAILED)
    idx.close()


def test_retry_conflicting_flags_exit_2(tmp_path, monkeypatch):
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MAGSYNC_DB_PATH", str(tmp_path / "index.db"))
    result = runner.invoke(cli.app, ["retry", "-v", "-q"])
    assert result.exit_code == 2
    assert "mutually exclusive" in result.output


def test_retry_quiet_shows_summary_not_per_issue(tmp_path, monkeypatch):
    _seed_failed_issue(tmp_path, monkeypatch)

    async def fake_download_batch(issues, cfg, idx, on_start=None, on_complete=None):
        results = []
        for issue in issues:
            if on_complete:
                on_complete(issue, False, "LimeWire share link is unavailable (removed or expired)")
            results.append({"issue": issue, "success": False, "error": "share link is unavailable"})
        return results

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_download_batch)

    result = runner.invoke(cli.app, ["retry", "-q"])
    assert result.exit_code == 0
    assert "unavailable (dead links)" in result.output      # summary present
    assert "Dead Issue" not in result.output                # no per-issue line


def test_retry_verbose_shows_per_issue(tmp_path, monkeypatch):
    _seed_failed_issue(tmp_path, monkeypatch)

    async def fake_download_batch(issues, cfg, idx, on_start=None, on_complete=None):
        results = []
        for issue in issues:
            if on_complete:
                on_complete(issue, False, "LimeWire share link is unavailable (removed or expired)")
            results.append({"issue": issue, "success": False, "error": "share link is unavailable"})
        return results

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_download_batch)

    result = runner.invoke(cli.app, ["retry", "-v"])
    assert result.exit_code == 0
    assert "Dead Issue" in result.output                    # per-issue detail shown
