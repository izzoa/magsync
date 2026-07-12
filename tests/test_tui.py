"""Tests for TUI download-queue selection."""

from __future__ import annotations

from types import SimpleNamespace

from magsync.core.models import (
    DownloadFailureKind,
    SourceFailure,
    SourceFailureKind,
    SourceResult,
)
from magsync.tui.app import (
    MagSyncApp,
    _download_outcome_label,
    _is_queueable,
    _source_failure_status,
)


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


def test_source_failure_status_is_typed_and_redacted():
    blocked = SourceFailure(
        SourceFailureKind.ACCESS_BLOCKED,
        "challenge at https://freemagazines.top/path?token=secret#fragment",
    )
    transient = SourceFailure(SourceFailureKind.TRANSIENT, "temporary failure")
    protocol = SourceFailure(SourceFailureKind.PROTOCOL, "unknown page")

    blocked_text = _source_failure_status(blocked)
    assert "blocked" in blocked_text.casefold()
    assert "secret" not in blocked_text and "fragment" not in blocked_text
    assert "temporarily unavailable" in _source_failure_status(transient)
    assert "format" in _source_failure_status(protocol)


def test_download_outcome_label_uses_kind_not_message():
    assert _download_outcome_label(True, None) == "downloaded"
    assert (
        _download_outcome_label(False, DownloadFailureKind.SHARE_UNAVAILABLE)
        == "unavailable"
    )
    assert (
        _download_outcome_label(False, DownloadFailureKind.UNSUPPORTED)
        == "unsupported"
    )
    assert _download_outcome_label(False, DownloadFailureKind.TRANSIENT) == "failed"


def test_search_failure_preserves_prior_results(monkeypatch):
    failure = SourceFailure(
        SourceFailureKind.ACCESS_BLOCKED,
        "challenge https://freemagazines.top/?token=secret#fragment",
    )

    async def blocked_search(*_args, **_kwargs):
        return SourceResult(failure=failure)

    monkeypatch.setattr("magsync.tui.app.search_with_details_result", blocked_search)

    class FakeApp:
        cfg = SimpleNamespace(download=SimpleNamespace(scrape_delay=0))
        search_results = [{"id": 7, "title": "Prior"}]
        selected_issues = {7}

        def __init__(self):
            self.statuses: list[str] = []

        def _update_status(self, text: str) -> None:
            self.statuses.append(text)

    fake = FakeApp()
    MagSyncApp.__dict__["_do_search"].__wrapped__(fake, "replacement")

    assert fake.search_results == [{"id": 7, "title": "Prior"}]
    assert fake.selected_issues == {7}
    assert "blocked" in fake.statuses[-1].casefold()
    assert "secret" not in fake.statuses[-1]
