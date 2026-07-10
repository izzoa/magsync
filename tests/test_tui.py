"""Tests for TUI download-queue selection."""

from __future__ import annotations

from magsync.tui.app import _is_queueable


def _issue(id: int, status: str | None, url: str | None = "https://limewire.com/d/x#k") -> dict:
    return {"id": id, "download_status": status, "limewire_url": url}


def test_queueable_excludes_unsupported_and_complete():
    selected = {1, 2, 3, 4, 5}
    assert _is_queueable(_issue(1, "pending"), selected)
    assert _is_queueable(_issue(2, "failed"), selected)
    assert not _is_queueable(_issue(3, "complete"), selected)
    # Terminal non-PDF payload: select-all must not re-queue it.
    assert not _is_queueable(_issue(4, "unsupported"), selected)
    assert not _is_queueable(_issue(5, "pending", url=None), selected)  # no link


def test_queueable_requires_selection():
    assert not _is_queueable(_issue(9, "pending"), selected=set())
