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


def test_populate_table_renders_parked_rows_as_cataloged():
    class FakeTable:
        def __init__(self):
            self.rows = []

        def clear(self):
            self.rows = []

        def add_row(self, *cells, key=None):
            self.rows.append(cells)

    class FakeApp:
        def __init__(self):
            self.table = FakeTable()
            self.statuses = []

        def query_one(self, _selector, _type=None):
            return self.table

        def _update_status(self, text):
            self.statuses.append(text)

    fake = FakeApp()
    issues = [
        {"id": 1, "title": "Wanted", "download_status": "pending",
         "requested_by": "subscription"},
        {"id": 2, "title": "Stranger", "download_status": "pending",
         "requested_by": None},
        {"id": 3, "title": "NonPdf", "download_status": "unsupported",
         "requested_by": "manual"},
    ]
    MagSyncApp._populate_table(fake, issues, new_count=0)

    statuses = [row[5] for row in fake.table.rows]
    assert statuses == ["pending", "cataloged", "⊘ non-PDF"]


def test_do_download_marks_selection_manual(monkeypatch):
    marked: list[list[int]] = []
    attempted: list[int] = []

    async def fake_download_batch(issues, cfg, idx, on_start=None,
                                  on_complete=None, **_kw):
        for issue in issues:
            attempted.append(issue["id"])
        return [{"issue": i, "success": True, "error": None} for i in issues]

    monkeypatch.setattr("magsync.core.batch.download_batch", fake_download_batch)

    class FakeIdx:
        def mark_manual(self, ids):
            marked.append(list(ids))
            return len(ids)

    class FakeApp:
        cfg = SimpleNamespace(download=SimpleNamespace(max_concurrent=2))
        idx = FakeIdx()
        search_results = [
            {"id": 5, "title": "Chosen", "download_status": "pending",
             "limewire_url": "https://limewire.com/d/x#k"},
            {"id": 6, "title": "Unselected", "download_status": "pending",
             "limewire_url": "https://limewire.com/d/y#k"},
        ]
        selected_issues = {5}

        def __init__(self):
            self.statuses = []
            self.app = SimpleNamespace(call_from_thread=lambda fn, *a: fn(*a))

        def _update_status(self, text):
            self.statuses.append(text)

        def _update_download_log(self, text):
            pass

        def _refresh_library(self):
            pass

    fake = FakeApp()
    MagSyncApp.__dict__["_do_download"].__wrapped__(fake)

    # Explicit selection recorded as manual intent before the batch ran.
    assert marked == [[5]]
    assert attempted == [5]
