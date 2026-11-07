"""`repair-metadata` executed by a runtime owner: locks, liveness, races, stops."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import threading
import time
from collections import defaultdict

from pdf_factory import make_pdf
from test_companion_hardening import make_runtime, op_state, remote_scope

from magsync.companion import commands
from magsync.companion.commands import execute_local
from magsync.companion.local import accept_local
from magsync.core import repair as repair_mod
from magsync.core.models import DownloadResult, DownloadStatus

BODY = {"query": None, "dry_run": False, "verify": False}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _add(idx, out, n, *, title=None, path=None, data=None):
    title = title or f"AFAR – Issue {n}, March 2025"
    mag = idx.get_or_create_magazine("AFAR", "afar")
    page = f"https://freemagazines.top/afar-{n}/"
    idx.add_issues(mag, [{"title": title, "page_url": page, "limewire_url": f"https://limewire.com/d/a{n}#k",
                          "year": 2025, "month": 3}])
    issue_id = idx.conn.execute("SELECT id FROM issues WHERE page_url=?", (page,)).fetchone()[0]
    if path is None:
        path = out / "AFAR" / f"AFAR - {n}.pdf"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data if data is not None else make_pdf(pages=1 + n % 3))
    idx.update_download_status(issue_id, DownloadStatus.COMPLETE, str(path), path.stat().st_size,
                               _sha(path.read_bytes()))
    return issue_id, path


async def _runtime(tmp_path, **kwargs):
    runtime, idx, _ = make_runtime(tmp_path, **kwargs)
    runtime.store.initialize()
    await runtime.start(background=False)
    return runtime, idx, tmp_path / "out"


async def test_command_stamps_through_the_operation_queue(tmp_path):
    runtime, idx, out = await _runtime(tmp_path)
    try:
        issue_id, path = _add(idx, out, 1)
        before = path.read_bytes()
        operation = accept_local(runtime.store, "metadata", BODY)
        await runtime.tick(scan=False)
        state, result = op_state(runtime.store, operation)
        assert state == "succeeded" and result["counts"]["stamped"] == 1
        placed = path.read_bytes()
        assert placed.startswith(before) and len(placed) > len(before)
        row = idx.conn.execute("SELECT sha256, source_sha256 FROM downloads WHERE issue_id=?", (issue_id,)).fetchone()
        assert (row["sha256"], row["source_sha256"]) == (_sha(placed), _sha(before))
    finally:
        await runtime.stop()
        idx.close()


async def test_maintenance_commands_run_one_at_a_time(tmp_path, monkeypatch):
    runtime, idx, out = await _runtime(tmp_path)
    events = []
    real_prepare = repair_mod.prepare_file

    def slow_prepare(group, **kwargs):
        events.append("prepare-start")
        threading.Event().wait(0.3)
        events.append("prepare-end")
        return real_prepare(group, **kwargs)

    def recording_repair(*_a, **_k):
        events.append("repair-titles")
        return repair_mod.RepairReport()

    monkeypatch.setattr(repair_mod, "prepare_file", slow_prepare)
    monkeypatch.setattr(commands, "repair_titles", recording_repair)
    try:
        _add(idx, out, 1)
        await asyncio.gather(execute_local(runtime, "metadata", BODY, "op-1"),
                             execute_local(runtime, "repair", {"dry_run": False}, "op-2"))
        assert events in (["prepare-start", "prepare-end", "repair-titles"],
                          ["repair-titles", "prepare-start", "prepare-end"])
    finally:
        await runtime.stop()
        idx.close()


async def test_active_download_is_busy(tmp_path):
    runtime, idx, out = await _runtime(tmp_path)
    try:
        issue_id, path = _add(idx, out, 1)
        before = path.read_bytes()
        runtime._active_attempts[issue_id] = "attempt"
        result = await execute_local(runtime, "metadata", BODY, "op")
        assert result["counts"]["busy"] == 1 and path.read_bytes() == before
        assert runtime._publishing == {}  # publication refcounts fully released
    finally:
        runtime._active_attempts.clear()
        await runtime.stop()
        idx.close()


async def test_stop_mid_run_is_partial(tmp_path, monkeypatch):
    runtime, idx, out = await _runtime(tmp_path)
    real_prepare = repair_mod.prepare_file

    def prepare_then_stop(group, **kwargs):
        runtime.stopping = True
        return real_prepare(group, **kwargs)

    monkeypatch.setattr(repair_mod, "prepare_file", prepare_then_stop)
    try:
        for n in range(3):
            _add(idx, out, n)
        result = await execute_local(runtime, "metadata", BODY, "op")
        assert result["interrupted"] and result["outcome"] == "partial" and result["done"] == 1
    finally:
        await runtime.stop()
        idx.close()


async def test_progress_lines_land_in_the_owner_log(tmp_path, caplog):
    runtime, idx, out = await _runtime(tmp_path)
    try:
        for n in range(35):
            _add(idx, out, n)
        with caplog.at_level(logging.INFO, logger="magsync"):
            result = await execute_local(runtime, "metadata", BODY, "op")
        lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("repair-metadata:")]
        assert [line.split()[1] for line in lines] == ["10/35", "20/35", "30/35", "35/35"]
        assert result["counts"]["stamped"] == 35
    finally:
        await runtime.stop()
        idx.close()


async def test_cancelled_task_never_replaces_the_file(tmp_path, monkeypatch):
    runtime, idx, out = await _runtime(tmp_path)
    entered, release = threading.Event(), threading.Event()
    real_prepare = repair_mod.prepare_file

    def gated_prepare(group, **kwargs):
        entered.set()
        release.wait(10)
        return real_prepare(group, **kwargs)  # stages a file after the task is gone

    monkeypatch.setattr(repair_mod, "prepare_file", gated_prepare)
    try:
        _issue, path = _add(idx, out, 1)
        before = path.read_bytes()
        group = repair_mod.metadata_groups(idx)[0]
        task = asyncio.create_task(commands._metadata_one(
            runtime, idx, group, defaultdict(asyncio.Lock), verify=False, dry_run=False, token="tok"))
        await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        release.set()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.5)  # the worker thread finishes staging
        assert path.read_bytes() == before
        assert runtime._publishing == {}
        monkeypatch.setattr(repair_mod, "prepare_file", real_prepare)
        result = await execute_local(runtime, "metadata", BODY, "op")  # sweeps and stamps
        assert result["counts"]["stamped"] == 1
        assert [p.name for p in path.parent.iterdir()] == [path.name]
    finally:
        release.set()
        await runtime.stop()
        idx.close()


async def test_export_publication_and_a_restamp_never_overlap(tmp_path, monkeypatch):
    """Publication holds the issue lock through its verify; the swap must wait."""
    from magsync.companion.exports import Exports

    runtime, idx, out = await _runtime(tmp_path)
    in_verify, overlaps = threading.Event(), []
    real_verify = Exports.verify

    def slow_verify(path, *args, **kwargs):
        in_verify.set()
        try:
            time.sleep(0.4)
            return real_verify(path, *args, **kwargs)
        finally:
            in_verify.clear()

    real_finalize = repair_mod.finalize_file

    def watched_finalize(*args, **kwargs):
        overlaps.append(in_verify.is_set())
        return real_finalize(*args, **kwargs)

    monkeypatch.setattr(Exports, "verify", staticmethod(slow_verify))
    monkeypatch.setattr(repair_mod, "finalize_file", watched_finalize)
    try:
        issue_id, path = _add(idx, out, 1)
        store = runtime.store
        client, scope = remote_scope(store)
        store.create_request(client, scope, {"issue_id": store.provider_issue(issue_id)})
        publish = asyncio.create_task(runtime._publish(issue_id))
        assert await asyncio.to_thread(in_verify.wait, 5)  # publication is mid-verify
        result = await execute_local(runtime, "metadata", BODY, "op")
        await publish
        assert overlaps == [False] and result["counts"]["stamped"] == 1
        errors = {row[0] for row in store.conn.execute("SELECT fulfillment_error FROM acquisition_requests")}
        assert errors == {None}
        assert store.conn.execute("SELECT count(*) FROM deliveries").fetchone()[0] == 1
        row = idx.conn.execute("SELECT sha256, file_size_bytes FROM downloads WHERE issue_id=?", (issue_id,)).fetchone()
        assert (row["sha256"], row["file_size_bytes"]) == (_sha(path.read_bytes()), path.stat().st_size)
    finally:
        await runtime.stop()
        idx.close()


async def test_lost_ownership_ends_the_run_without_leftovers(tmp_path, monkeypatch):
    from magsync.companion.protocol import ProtocolError

    runtime, idx, out = await _runtime(tmp_path)
    try:
        paths = [_add(idx, out, n)[1] for n in range(3)]
        before = [p.read_bytes() for p in paths]

        def lost():
            runtime.fatal = True
            raise ProtocolError("runtime_unavailable")

        monkeypatch.setattr(runtime.owner, "check", lost)
        result = await execute_local(runtime, "metadata", BODY, "op")
        assert result["interrupted"] and result["outcome"] == "partial" and result["done"] == 0
        assert [p.read_bytes() for p in paths] == before
        assert [name for name in os.listdir(paths[0].parent) if name.startswith(".")] == []
    finally:
        monkeypatch.undo()
        runtime.fatal = False
        await runtime.stop()
        idx.close()


async def test_duplicate_completing_mid_restamp_matches_the_final_bytes(tmp_path, monkeypatch):
    runtime, idx, out = await _runtime(tmp_path)
    try:
        first, path = _add(idx, out, 1)
        payload = path.read_bytes()
        mag = idx.get_or_create_magazine("AFAR", "afar")
        idx.add_issues(mag, [{"title": "AFAR – Issue 1, March 2025", "page_url": "https://freemagazines.top/twin/",
                              "year": 2025, "month": 3}])
        twin = idx.conn.execute("SELECT id FROM issues WHERE page_url LIKE '%twin%'").fetchone()[0]
        real_prepare = repair_mod.prepare_file

        def prepare_then_complete_twin(group, **kwargs):
            plan = real_prepare(group, **kwargs)
            done = threading.Event()

            def complete():  # the download's completion runs on the owner loop
                idx.record_download_result(twin, DownloadResult(
                    success=True, file_path=path, file_size_bytes=len(payload), sha256=_sha(payload),
                    source_sha256=_sha(payload), source_size=len(payload), duplicate=True))
                done.set()

            loop.call_soon_threadsafe(complete)
            done.wait(5)
            return plan

        loop = asyncio.get_running_loop()
        monkeypatch.setattr(repair_mod, "prepare_file", prepare_then_complete_twin)
        result = await execute_local(runtime, "metadata", BODY, "op")
        assert result["counts"]["busy"] == 1 and path.read_bytes() == payload

        def identities():
            return {tuple(r) for r in idx.conn.execute(
                "SELECT sha256, file_size_bytes FROM downloads WHERE issue_id IN (?, ?)", (first, twin))}

        assert identities() == {(_sha(payload), len(payload))}
        monkeypatch.setattr(repair_mod, "prepare_file", real_prepare)
        assert (await execute_local(runtime, "metadata", BODY, "op2"))["counts"]["stamped"] == 1
        assert identities() == {(_sha(path.read_bytes()), path.stat().st_size)}
    finally:
        await runtime.stop()
        idx.close()
