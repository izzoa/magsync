"""Tests that `magsync retry` re-attempts only failed/unavailable rows —
never the pending backlog — and reports link-less failures."""

from __future__ import annotations

from typer.testing import CliRunner

import magsync.cli as cli
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus

runner = CliRunner()

LW = "https://limewire.com/d/{}#key"


def _open_index(tmp_path, monkeypatch) -> MagazineIndex:
    monkeypatch.setenv("MAGSYNC_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MAGSYNC_DB_PATH", str(tmp_path / "index.db"))
    monkeypatch.setenv("MAGSYNC_NO_PROGRESS", "1")
    return MagazineIndex(db_path=tmp_path / "index.db")


def _add_issue(idx: MagazineIndex, key: str, url: str | None, status=None) -> int:
    """Add one wanted (manual-provenance) issue; retry ignores unwanted rows,
    and that exclusion has its own dedicated test below."""
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(mag, [{"title": f"{key} - Jan 2026", "page_url": key,
                          "limewire_url": url, "year": 2026, "month": 1}])
    issue_id = idx.conn.execute(
        "SELECT id FROM issues WHERE page_url = ?", (key,)
    ).fetchone()[0]
    idx.mark_manual([issue_id])
    if status is not None:
        idx.update_download_status(issue_id, status)
    return issue_id


def _fake_batch(attempted: list[int], success: bool = True):
    async def fake_download_batch(issues, cfg, idx, on_start=None, on_complete=None):
        results = []
        for issue in issues:
            attempted.append(issue["id"])
            if on_complete:
                on_complete(issue, success, None if success else "boom")
            results.append({"issue": issue, "success": success,
                            "error": None if success else "boom"})
        return results
    return fake_download_batch


def test_retry_attempts_only_failed_rows_not_backlog(tmp_path, monkeypatch):
    idx = _open_index(tmp_path, monkeypatch)
    failed_id = _add_issue(idx, "failed", LW.format("f"), DownloadStatus.FAILED)
    backlog_ids = [_add_issue(idx, f"backlog{n}", LW.format(n)) for n in range(3)]
    idx.close()

    attempted: list[int] = []
    monkeypatch.setattr("magsync.core.batch.download_batch", _fake_batch(attempted))

    result = runner.invoke(cli.app, ["retry"])
    assert result.exit_code == 0
    assert attempted == [failed_id]  # the pending backlog is never touched

    idx = MagazineIndex(db_path=tmp_path / "index.db")
    for issue_id in backlog_ids:
        status = idx.conn.execute(
            "SELECT status FROM downloads WHERE issue_id = ?", (issue_id,)
        ).fetchone()[0]
        assert status == "pending"
    idx.close()


def test_retry_all_linkless_reports_skipped_and_attempts_nothing(tmp_path, monkeypatch):
    idx = _open_index(tmp_path, monkeypatch)
    linkless_id = _add_issue(idx, "linkless", None, DownloadStatus.FAILED)
    idx.close()

    attempted: list[int] = []
    monkeypatch.setattr("magsync.core.batch.download_batch", _fake_batch(attempted))

    result = runner.invoke(cli.app, ["retry"])
    assert result.exit_code == 0
    assert attempted == []
    assert "no download link" in result.output
    assert "No failed downloads" not in result.output  # would be a lie here

    idx = MagazineIndex(db_path=tmp_path / "index.db")
    status = idx.conn.execute(
        "SELECT status FROM downloads WHERE issue_id = ?", (linkless_id,)
    ).fetchone()[0]
    assert status == "failed"  # preserved for backfill-urls, not stranded
    idx.close()


def test_retry_skipped_count_shown_under_quiet(tmp_path, monkeypatch):
    idx = _open_index(tmp_path, monkeypatch)
    _add_issue(idx, "failed", LW.format("f"), DownloadStatus.FAILED)
    _add_issue(idx, "linkless", None, DownloadStatus.FAILED)
    idx.close()

    attempted: list[int] = []
    monkeypatch.setattr("magsync.core.batch.download_batch", _fake_batch(attempted))

    result = runner.invoke(cli.app, ["retry", "-q"])
    assert result.exit_code == 0
    assert "1 failed download skipped" in result.output
    assert "no download link" in result.output


def test_retry_reports_no_failed_downloads_only_when_none_exist(tmp_path, monkeypatch):
    idx = _open_index(tmp_path, monkeypatch)
    _add_issue(idx, "backlog", LW.format("b"))  # pending only
    idx.close()

    result = runner.invoke(cli.app, ["retry"])
    assert result.exit_code == 0
    assert "No failed downloads to retry" in result.output


def test_retry_leaves_unsupported_untouched(tmp_path, monkeypatch):
    # 'unsupported' is terminal by policy (non-PDF payload): `magsync retry`
    # must not re-queue it even though it has a working link.
    idx = _open_index(tmp_path, monkeypatch)
    unsupported_id = _add_issue(idx, "unsup", LW.format("u"), DownloadStatus.UNSUPPORTED)
    failed_id = _add_issue(idx, "failed", LW.format("f"), DownloadStatus.FAILED)
    idx.close()

    attempted: list[int] = []
    monkeypatch.setattr("magsync.core.batch.download_batch", _fake_batch(attempted))

    result = runner.invoke(cli.app, ["retry"])
    assert result.exit_code == 0
    assert attempted == [failed_id]  # unsupported row never re-queued

    idx = _open_index(tmp_path, monkeypatch)
    status = idx.conn.execute(
        "SELECT status FROM downloads WHERE issue_id = ?", (unsupported_id,)
    ).fetchone()[0]
    idx.close()
    assert status == "unsupported"


def test_retry_bypasses_and_clears_persisted_future_schedule(tmp_path, monkeypatch):
    idx = _open_index(tmp_path, monkeypatch)
    failed_id = _add_issue(idx, "scheduled", LW.format("future"), DownloadStatus.FAILED)
    idx.conn.execute(
        """UPDATE downloads
           SET last_error_kind = 'transient', last_error = 'temporary',
               attempt_count = 4, last_attempt_at = '2026-07-11T00:00:00+00:00',
               next_action = 'DOWNLOAD', next_retry_at = '2099-01-01T00:00:00+00:00'
           WHERE issue_id = ?""",
        (failed_id,),
    )
    idx.conn.commit()
    idx.close()

    attempted: list[int] = []
    monkeypatch.setattr("magsync.core.batch.download_batch", _fake_batch(attempted))

    result = runner.invoke(cli.app, ["retry"])
    assert result.exit_code == 0
    assert attempted == [failed_id]

    idx = _open_index(tmp_path, monkeypatch)
    row = idx.conn.execute(
        """SELECT last_error_kind, last_error, attempt_count, last_attempt_at,
                  next_action, next_retry_at
           FROM downloads WHERE issue_id = ?""",
        (failed_id,),
    ).fetchone()
    idx.close()
    assert dict(row) == {
        "last_error_kind": None,
        "last_error": None,
        "attempt_count": 0,
        "last_attempt_at": None,
        "next_action": None,
        "next_retry_at": None,
    }
