"""Coordinated batch progress + logging output for CLI bulk commands.

Bulk commands (`fetch`, `retry`, `backfill-urls`) run many items and, without
coordination, interleave a Rich progress bar (stdout) with the `magsync`
logger's records (lastResort → stderr). This module provides a single
coordinated surface: for the duration of a run the `magsync` logger is pointed
at one handler bound to the SAME console as the progress bar, so log lines
render above the live bar instead of clobbering it; the logger's level,
propagation, and handlers are snapshotted and restored on exit.

The daemon does NOT use this module — it keeps its own `logging.basicConfig`.
"""

from __future__ import annotations

import logging
import os
import sys

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

from magsync.core.diagnostics import sanitize_external_error
from magsync.core.models import DownloadFailureKind
from magsync.core.policy import get_download_failure_policy

LOGGER_NAME = "magsync"


def _env_no_progress() -> bool:
    val = os.environ.get("MAGSYNC_NO_PROGRESS", "")
    return val.strip().lower() not in ("", "0", "false", "no")


def resolve_mode(verbose: bool, quiet: bool, no_progress: bool) -> tuple[bool, int]:
    """Resolve (use_live_bar, log_level) from flags + TTY.

    Raises ValueError if verbose and quiet are both set. The log level is set on
    the logger itself (not just the handler) so that `--verbose` INFO/DEBUG
    records aren't filtered before reaching the handler.
    """
    if verbose and quiet:
        raise ValueError("--verbose and --quiet are mutually exclusive")
    use_live_bar = (
        not quiet
        and not no_progress
        and not _env_no_progress()
        and sys.stdout.isatty()
    )
    if verbose:
        log_level = logging.DEBUG
    elif quiet:
        log_level = logging.ERROR       # genuine errors still surface under --quiet
    else:
        log_level = logging.WARNING     # default: hides demoted-to-INFO dead-link lines
    return use_live_bar, log_level


class _ConsoleHandler(logging.Handler):
    """Logging handler that prints through a Rich Console.

    When a `Progress` Live region is active on the same console, output is
    emitted above the bar without corrupting it.
    """

    _STYLES = {logging.ERROR: "red", logging.CRITICAL: "red", logging.WARNING: "yellow"}

    def __init__(self, console: Console, level: int):
        super().__init__(level)
        self.console = console
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.console.print(self.format(record), style=self._STYLES.get(record.levelno), highlight=False)
        except Exception:  # pragma: no cover - defensive, mirrors logging.Handler
            self.handleError(record)


class BatchOutput:
    """Context manager coordinating a progress bar and the `magsync` logger.

    Use as ``with BatchOutput(...) as out:`` then pass ``out.on_start`` /
    ``out.on_complete`` to ``download_batch``, or call ``out.record(label)``
    directly. Call ``out.summarize(results)`` (downloads) after the block, or
    read ``out.counts`` for a caller-specific summary.
    """

    def __init__(
        self,
        console: Console,
        total: int,
        *,
        title: str = "Working",
        use_live_bar: bool,
        log_level: int,
        verbose: bool = False,
    ):
        self.console = console
        self.total = total
        self.title = title
        self.use_live_bar = use_live_bar
        self.log_level = log_level
        self.verbose = verbose
        self.counts: dict[str, int] = {}
        self.done = 0
        self._progress: Progress | None = None
        self._task = None
        self._logger = logging.getLogger(LOGGER_NAME)
        self._saved: tuple | None = None
        self._last_line = 0

    # -- lifecycle -------------------------------------------------------
    def __enter__(self) -> "BatchOutput":
        self._saved = (self._logger.level, self._logger.propagate, list(self._logger.handlers))
        self._logger.setLevel(self.log_level)
        self._logger.propagate = False          # exactly one sink; no root/lastResort dupes
        self._logger.handlers = [_ConsoleHandler(self.console, self.log_level)]
        if self.use_live_bar:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=self.console,
            )
            self._progress.start()
            self._task = self._progress.add_task(self._describe(), total=self.total)
        return self

    def __exit__(self, *exc) -> bool:
        try:
            if self._progress is not None:
                self._progress.stop()
        finally:
            if self._saved is not None:
                level, propagate, handlers = self._saved
                self._logger.setLevel(level)
                self._logger.propagate = propagate
                self._logger.handlers = handlers
        return False

    # -- progress --------------------------------------------------------
    def _counts_str(self) -> str:
        return "  ".join(f"{k} {v}" for k, v in self.counts.items()) or "…"

    def _describe(self) -> str:
        return f"{self.title} — {self._counts_str()}"

    def record(self, label: str) -> None:
        """Record one item's outcome under ``label`` and advance progress."""
        self.counts[label] = self.counts.get(label, 0) + 1
        self.done += 1
        if self._progress is not None:
            self._progress.update(self._task, completed=self.done, description=self._describe())
        else:
            self._maybe_line()

    def _maybe_line(self) -> None:
        # Non-TTY: emit a throttled progress line (~every 10%, capped at 25), not one per item.
        step = max(1, min(25, self.total // 10))
        if self.done >= self.total or self.done - self._last_line >= step:
            self._last_line = self.done
            self.console.print(f"progress: {self.done}/{self.total} ({self._counts_str()})", highlight=False)

    # -- download adapter ------------------------------------------------
    def on_start(self, issue: dict) -> None:  # kept for the download_batch contract
        pass

    def on_complete(
        self,
        issue: dict,
        success: bool,
        error: str | None,
        failure_kind: DownloadFailureKind | str | None = None,
    ) -> None:
        label = "downloaded" if success else self._failure_label(failure_kind)
        if self.verbose:
            title = (issue.get("title") or "")[:50]
            if success:
                self.console.print(f"  [green]✓[/green] {title}", highlight=False)
            else:
                safe_error = sanitize_external_error(error or "Download failed")
                self.console.print(
                    f"  [red]✗[/red] {title}: {safe_error}", highlight=False
                )
        self.record(label)

    @staticmethod
    def _failure_label(
        failure_kind: DownloadFailureKind | str | None,
    ) -> str:
        """Classify a display bucket solely from structured failure state."""
        if failure_kind is None:
            return "failed"
        return get_download_failure_policy(failure_kind).summary_bucket.value

    # -- summary ---------------------------------------------------------
    def summarize(self, results: list[dict]) -> dict[str, int]:
        """Print a run-scoped download summary reconciled from ``download_batch``
        results (covers batch aborts that never fired per-issue callbacks)."""
        counts = {"downloaded": 0, "unavailable": 0, "unsupported": 0, "failed": 0}
        for r in results:
            if r.get("success"):
                counts["downloaded"] += 1
            else:
                failure_kind = r.get("failure_kind")
                if failure_kind is None and r.get("unsupported"):
                    # Compatibility with v0.5 dictionaries: this is an
                    # explicit structured flag, never display-text parsing.
                    failure_kind = DownloadFailureKind.UNSUPPORTED
                counts[self._failure_label(failure_kind)] += 1
        self.console.print(
            f"\n[green]Done![/green] {counts['downloaded']} downloaded, "
            f"{counts['unavailable']} unavailable (dead links), "
            f"{counts['unsupported']} unsupported (non-PDF), {counts['failed']} failed"
        )
        return counts
