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
        return {"state":"blocked", "result":{"outcome":"blocked"}}

    monkeypatch.setattr("magsync.tui.app.submit_local", blocked_search)

    class FakeApp:
        cfg = SimpleNamespace(download=SimpleNamespace(scrape_delay=0))
        idx = SimpleNamespace(db_path="test.db")
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
    submitted = []

    async def fake_submit(kind, body, cfg, db_path, **_callbacks):
        submitted.append((kind,body))
        return {'state':'succeeded','result':{'physical_attempts':1,'outcomes':[]}}

    class FakeIndex:
        def __init__(self, _path):
            pass

        def get_issues_by_ids(self, ids):
            return [{"id": 5, "title": "Chosen", "download_status": "complete", "last_error_kind": None}]

        def close(self):
            pass

    monkeypatch.setattr("magsync.tui.app.MagazineIndex", FakeIndex)
    logs = []

    monkeypatch.setattr("magsync.tui.app.submit_local", fake_submit)

    class FakeApp:
        cfg = SimpleNamespace(download=SimpleNamespace(max_concurrent=2))
        idx = SimpleNamespace(db_path="test.db")
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
            logs.append(text)

        def _refresh_library(self):
            pass

    fake = FakeApp()
    MagSyncApp.__dict__["_do_download"].__wrapped__(fake)

    # Confirmation submits exactly displayed selections; the coordinator persists intent.
    assert submitted == [("download", {"issue_ids":[5]})]
    # Outcomes name each selected issue with its marker, not a bare status.
    assert logs[-1] == "✓ Chosen"
    assert "1 selected issues processed" in fake.statuses[-1]


def test_download_progress_names_titles_as_issues_finish(monkeypatch):
    from magsync.tui.app import MagSyncApp

    async def fake_submit(kind, body, cfg, db_path, progress=None, status=None):
        progress({"issue_id": 5, "title": "Chosen", "status": "pending", "failure_kind": None})
        progress({"issue_id": 5, "title": "Chosen", "status": "downloading", "failure_kind": None})
        progress({"issue_id": 5, "title": "Chosen", "status": "unavailable", "failure_kind": "share_unavailable"})
        return {"state": "succeeded", "result": {"physical_attempts": 1, "outcomes": []}}

    class FakeIndex:
        def __init__(self, _path):
            pass

        def get_issues_by_ids(self, ids):
            return [{"id": 5, "title": "Chosen", "download_status": "unavailable",
                     "last_error_kind": "share_unavailable"}]

        def close(self):
            pass

    monkeypatch.setattr("magsync.tui.app.submit_local", fake_submit)
    monkeypatch.setattr("magsync.tui.app.MagazineIndex", FakeIndex)
    logs = []

    class FakeApp:
        cfg = SimpleNamespace()
        idx = SimpleNamespace(db_path="test.db")
        search_results = [{"id": 5, "title": "Chosen", "download_status": "pending",
                           "limewire_url": "https://limewire.com/d/x#k"}]
        selected_issues = {5}

        def __init__(self):
            self.statuses = []
            self.app = SimpleNamespace(call_from_thread=lambda fn, *a: fn(*a))

        def _update_status(self, text):
            self.statuses.append(text)

        def _update_download_log(self, text):
            logs.append(text)

        def _refresh_library(self):
            pass

    fake = FakeApp()
    MagSyncApp.__dict__["_do_download"].__wrapped__(fake)
    assert logs[0] == "○ Chosen (share_unavailable)"  # live, while the command ran
    assert "Processed 1/1" in " ".join(fake.statuses)
    assert logs[-1] == "○ Chosen (share_unavailable)"


def test_search_reports_typed_guidance_and_new_issue_count(monkeypatch):
    from magsync.tui.app import MagSyncApp

    outcomes = [
        {"state": "blocked", "result": {"outcome": "blocked", "failure": {
            "kind": "access_blocked", "message": "challenge https://freemagazines.top/?token=secret"}}},
        {"state": "succeeded", "result": {"outcome": "succeeded", "items": [{"id": "public-1"}], "added": 5,
                                          "detail_failures": 0}},
    ]

    async def fake_submit(kind, body, cfg, db_path, **_callbacks):
        return outcomes.pop(0)

    class FakeIndex:
        def __init__(self, _path):
            pass

        def get_issues_by_ids(self, ids):
            return [{"id": 1, "title": "Found"}]

        def close(self):
            pass

    class FakeStore:
        def __init__(self, _index):
            pass

        def internal_issue(self, public):
            return 1

    monkeypatch.setattr("magsync.tui.app.submit_local", fake_submit)
    monkeypatch.setattr("magsync.tui.app.MagazineIndex", FakeIndex)
    monkeypatch.setattr("magsync.companion.store.Store", FakeStore)
    populated = []

    class FakeApp:
        cfg = SimpleNamespace()
        idx = SimpleNamespace(db_path="test.db")
        search_results = [{"id": 7, "title": "Prior"}]
        selected_issues = {7}

        def __init__(self):
            self.statuses = []
            self.app = SimpleNamespace(call_from_thread=lambda fn, *a: fn(*a))

        def _update_status(self, text):
            self.statuses.append(text)

        def _populate_table(self, rows, new_count, omitted):
            populated.append((rows, new_count, omitted))

    fake = FakeApp()
    MagSyncApp.__dict__["_do_search"].__wrapped__(fake, "Found")
    assert fake.statuses[-1].startswith("Source access is blocked; retry later")
    assert "secret" not in fake.statuses[-1] and fake.search_results == [{"id": 7, "title": "Prior"}]
    MagSyncApp.__dict__["_do_search"].__wrapped__(fake, "Found")
    assert populated == [([{"id": 1, "title": "Found"}], 5, 0)]
