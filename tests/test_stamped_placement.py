"""Atomic placement and metadata stamping of new downloads, for every host."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import pathlib
from collections import defaultdict

import pytest
from bookorbit_emulation import bookorbit_view
from pdf_factory import make_pdf

import magsync.core.batch as batch_mod
import magsync.core.downloader as dl
from magsync.config import Config
from magsync.core import pdfmeta
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadFailureKind, DownloadResult, DownloadStatus
from magsync.core.organizer import canonical_file, organize_path

URL = "https://limewire.com/d/AbCdE#key"
SPEC = pdfmeta.stamp_spec_for({"title": "AFAR – Spring 2025", "genre": "Travel"})


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "index.db"
    monkeypatch.setenv("MAGSYNC_DB_PATH", str(path))
    return path


async def _place(dest, payload, stamp=SPEC):
    return await dl._place_payload(
        dest, url=URL, payload_digest=hashlib.sha256(payload), payload_size=len(payload),
        payload=payload, stamp=stamp,
    )


def _hidden(directory):
    return [p.name for p in directory.iterdir() if p.name.startswith(".")]


# --- 4.1 placement helper ------------------------------------------------------------


async def test_stamped_placement_records_both_identities(tmp_path):
    payload, dest = make_pdf(), tmp_path / "AFAR" / "AFAR - 2025-03 - Spring.pdf"
    result = await _place(dest, payload)
    placed = dest.read_bytes()
    assert result.success and placed.startswith(payload) and len(placed) > len(payload)
    assert (result.sha256, result.file_size_bytes) == (_sha(placed), len(placed))
    assert (result.source_sha256, result.source_size) == (_sha(payload), len(payload))
    fp, size, mtime_ns, inode = pdfmeta.parse_stamp_record(result.metadata_stamp)
    st = dest.stat()
    assert (fp, size, mtime_ns, inode) == (pdfmeta.fingerprint(SPEC), st.st_size, st.st_mtime_ns, st.st_ino)
    assert bookorbit_view(placed)["parsed"]["title"] == "AFAR – Spring 2025"
    assert _hidden(dest.parent) == []  # no placement temp left behind


async def test_unstamped_placement_is_byte_identical(tmp_path):
    payload, dest = make_pdf(), tmp_path / "x.pdf"
    result = await _place(dest, payload, stamp=None)
    assert dest.read_bytes() == payload
    assert result.sha256 == result.source_sha256 == _sha(payload)
    assert result.metadata_stamp is None


@pytest.mark.parametrize("payload_kind", ["encrypted", "build_failure"])
async def test_skipped_stamp_places_the_payload_unstamped(tmp_path, monkeypatch, payload_kind):
    payload = make_pdf(encrypt=payload_kind == "encrypted")
    if payload_kind == "build_failure":
        monkeypatch.setattr(dl.pdfmeta, "stamp_payload", lambda *a, **k: pdfmeta.StampOutcome(reason="stamp_failed"))
    dest = tmp_path / "x.pdf"
    result = await _place(dest, payload)
    assert result.success and dest.read_bytes() == payload
    assert result.sha256 == result.source_sha256 and result.metadata_stamp is None


async def test_short_append_is_cut_back_to_the_payload(tmp_path, monkeypatch):
    def short_write(handle, data):
        handle.write(data[: len(data) // 2])
        raise OSError("disk full")

    monkeypatch.setattr(dl, "_append_bytes", short_write)
    payload, dest = make_pdf(), tmp_path / "x.pdf"
    result = await _place(dest, payload)
    assert result.success and dest.read_bytes() == payload
    assert result.sha256 == result.source_sha256 == _sha(payload) and result.metadata_stamp is None


async def test_failed_fsync_after_append_restores_the_payload(tmp_path, monkeypatch):
    real_fsync, calls = os.fsync, []

    def flaky_fsync(fd):
        calls.append(fd)
        if len(calls) == 1:
            raise OSError("I/O error")
        real_fsync(fd)

    monkeypatch.setattr(dl.os, "fsync", flaky_fsync)
    payload, dest = make_pdf(), tmp_path / "x.pdf"
    result = await _place(dest, payload)
    assert result.success and dest.read_bytes() == payload and result.sha256 == _sha(payload)


async def test_unrestorable_append_places_nothing_and_keeps_the_partial(tmp_path, db, monkeypatch):
    def unrestorable(*_a, **_k):
        raise dl._RestoreFailed("truncate failed")

    monkeypatch.setattr(dl, "_append_stamp", unrestorable)
    payload, dest = make_pdf(), tmp_path / "x.pdf"
    part = dl._part_path_for(dest, URL)
    part.write_bytes(payload)
    result = await dl._finalize_plain_download(part, dest, len(payload), url=URL, stamp=SPEC)
    assert not result.success and result.failure_kind is DownloadFailureKind.INTERNAL
    assert not dest.exists()
    assert part.read_bytes() == payload  # still resumable
    assert _hidden(tmp_path) == []


async def test_dest_never_holds_partial_bytes(tmp_path, monkeypatch):
    dest = tmp_path / "x.pdf"
    real_build = dl._build_stamp

    def observe(temp, *args):
        assert not dest.exists() and temp.exists() and temp.name.startswith(".")
        return real_build(temp, *args)

    monkeypatch.setattr(dl, "_build_stamp", observe)

    def crash(*_a, **_k):
        raise OSError("power cut")

    monkeypatch.setattr(dl.os, "replace", crash)
    result = await _place(dest, make_pdf())
    assert not result.success and result.failure_kind is DownloadFailureKind.INTERNAL
    assert not dest.exists() and _hidden(tmp_path) == []


async def test_plain_host_stamps_through_the_partial(tmp_path, db):
    payload, dest = make_pdf(), tmp_path / "x.pdf"
    part = dl._part_path_for(dest, URL)
    part.write_bytes(payload)
    result = await dl._finalize_plain_download(part, dest, len(payload), url=URL, stamp=SPEC)
    assert result.success and not part.exists()
    assert dest.read_bytes().startswith(payload) and result.source_sha256 == _sha(payload)


# --- 4.3 stale temps ------------------------------------------------------------------


def test_stale_placement_temp_is_swept_but_the_partial_is_kept(tmp_path):
    dest = tmp_path / "x.pdf"
    part = dl._part_path_for(dest, URL)
    part.write_bytes(b"%PDF-1.4 partial")
    stale = tmp_path / f".{dest.name}.dead00.stamp"
    stale.write_bytes(b"left by a crash")
    assert dl._part_path_for(dest, URL) == part
    assert part.exists() and not stale.exists()


# --- 4.4 / 4.5 batch ---------------------------------------------------------------


def test_canonical_file_unifies_path_spellings(tmp_path):
    (tmp_path / "lib" / "AFAR").mkdir(parents=True)
    (tmp_path / "link").symlink_to(tmp_path / "lib")
    plain = tmp_path / "lib" / "AFAR" / "x.pdf"
    assert canonical_file(tmp_path / "lib" / ".." / "lib" / "AFAR" / "x.pdf") == canonical_file(plain)
    assert canonical_file(tmp_path / "link" / "AFAR" / "x.pdf") == canonical_file(plain)


def _cfg(tmp_path, *, stamp=True) -> Config:
    cfg = Config()
    cfg.output_dir = str(tmp_path / "out")
    cfg.limewire.file_iv_b64 = cfg.limewire.sharing_salt_b64 = "x"
    cfg.metadata.stamp = stamp
    return cfg


def _issues(idx, *titles, url=URL):
    mag = idx.get_or_create_magazine("AFAR", "afar")
    idx.add_issues(mag, [
        {"title": title, "page_url": f"p{n}", "limewire_url": url, "year": 2025, "month": 3, "genre": "Travel"}
        for n, title in enumerate(titles)
    ])
    return idx.get_issues(status=DownloadStatus.PENDING)


def _recording_download(calls):
    async def fake(url, dest, **kwargs):
        calls.append((dest, kwargs.get("stamp")))
        payload = make_pdf()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(payload)
        return DownloadResult(success=True, file_path=dest, file_size_bytes=len(payload), sha256=_sha(payload),
                              source_sha256=_sha(payload), source_size=len(payload))
    return fake


async def test_aliases_share_the_leaders_name_and_stamp(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(batch_mod, "download_and_decrypt", _recording_download(calls))
    idx = MagazineIndex(db_path=tmp_path / "index.db")
    pending = _issues(idx, "AFAR – Spring 2025", "AFAR – Spring 2025 (Digital Edition)")
    await batch_mod.download_batch(pending, _cfg(tmp_path), idx)
    assert len(calls) == 1
    dest, stamp = calls[0]
    leader = next(i for i in pending if organize_path(i["title"], i["page_url"], _cfg(tmp_path).output_dir) == dest)
    assert stamp == pdfmeta.stamp_spec_for(leader)
    rows = idx.conn.execute("SELECT file_path, sha256 FROM downloads").fetchall()
    assert len({tuple(r) for r in rows}) == 1  # both rows: same file, same recorded hash
    idx.close()


@pytest.mark.parametrize("enabled", [True, False])
async def test_stamping_follows_the_setting(tmp_path, monkeypatch, enabled):
    calls = []
    monkeypatch.setattr(batch_mod, "download_and_decrypt", _recording_download(calls))
    idx = MagazineIndex(db_path=tmp_path / "index.db")
    await batch_mod.download_batch(_issues(idx, "AFAR – Spring 2025"), _cfg(tmp_path, stamp=enabled), idx)
    stamp = calls[0][1]
    assert (stamp is not None) is enabled
    if enabled:
        assert stamp.title == "AFAR – Spring 2025" and stamp.genres == ("Travel",)
    idx.close()


async def test_dest_lock_is_keyed_by_canonical_path(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(batch_mod, "download_and_decrypt", _recording_download(calls))
    locks = defaultdict(asyncio.Lock)
    (tmp_path / "out").mkdir()
    spelled = tmp_path / "out" / ".." / "out" / "x.pdf"
    await batch_mod._perform_url_download(
        URL, spelled, _cfg(tmp_path), asyncio.Semaphore(1), dl.RateLimitGate(), locks,
        retry_attempts=0,
    )
    assert list(locks) == [str(canonical_file(tmp_path / "out" / "x.pdf"))]


@pytest.mark.parametrize("marked", ["own-stamp", "other-stamp", "unmarked"])
async def test_adopting_an_existing_destination_records_its_identity(tmp_path, monkeypatch, marked):
    async def never(*_a, **_k):
        raise AssertionError("an existing destination must not be downloaded again")

    monkeypatch.setattr(batch_mod, "download_and_decrypt", never)
    idx = MagazineIndex(db_path=tmp_path / "index.db")
    pending = _issues(idx, "AFAR – Spring 2025")
    cfg = _cfg(tmp_path)
    dest = organize_path(pending[0]["title"], pending[0]["page_url"], cfg.output_dir)
    dest.parent.mkdir(parents=True)
    payload = make_pdf()
    stamp_for = {"own-stamp": SPEC, "other-stamp": pdfmeta.stamp_spec_for({"title": "Other – May 2020"})}
    spec = stamp_for.get(marked)
    placed = payload + (pdfmeta.stamp_payload(payload, spec).appended if spec else b"")
    dest.write_bytes(placed)
    await batch_mod.download_batch(pending, cfg, idx)
    row = idx.conn.execute(
        "SELECT status, sha256, file_size_bytes, source_sha256, source_size, metadata_stamp FROM downloads"
    ).fetchone()
    assert (row["status"], row["sha256"], row["file_size_bytes"]) == ("complete", _sha(placed), len(placed))
    if marked == "own-stamp":  # provably magsync's stamp of its original under this issue
        assert (row["source_sha256"], row["source_size"]) == (_sha(payload), len(payload))
        assert pdfmeta.parse_stamp_record(row["metadata_stamp"])[0] == pdfmeta.fingerprint(SPEC)
    else:  # an observed hash only: never proof of who wrote the bytes
        assert (row["source_sha256"], row["source_size"], row["metadata_stamp"]) == (None, None, None)
    idx.close()


async def test_unreadable_temp_places_the_payload_unstamped(tmp_path, db, monkeypatch):
    payload, dest = make_pdf(), tmp_path / "x.pdf"
    part = dl._part_path_for(dest, URL)
    part.write_bytes(payload)
    real = pathlib.Path.read_bytes

    def flaky(self):
        if self.name.endswith(".stamp"):
            raise MemoryError("no room for the whole payload")
        return real(self)

    monkeypatch.setattr(pathlib.Path, "read_bytes", flaky)
    result = await dl._finalize_plain_download(part, dest, len(payload), url=URL, stamp=SPEC)
    assert result.success and dest.read_bytes() == payload
    assert result.sha256 == result.source_sha256 and _hidden(tmp_path) == []


@pytest.mark.parametrize("host", ["limewire", "plain"])
async def test_longest_partial_name_also_places(tmp_path, db, host):
    dest = tmp_path / ("A" * 237 + ".pdf")  # 241 bytes: its .part name is 255
    assert len(dl._part_path_for(dest, URL).name) == 255
    payload = make_pdf()
    if host == "limewire":
        result = await _place(dest, payload)
    else:
        part = dl._part_path_for(dest, URL)
        part.write_bytes(payload)
        result = await dl._finalize_plain_download(part, dest, len(payload), url=URL, stamp=SPEC)
    assert result.success and dest.read_bytes().startswith(payload)


async def test_stamp_failures_warn_but_expected_skips_do_not(tmp_path, monkeypatch, caplog):
    with caplog.at_level(logging.INFO, logger="magsync"):
        await _place(tmp_path / "enc.pdf", make_pdf(encrypt=True))
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    caplog.clear()
    monkeypatch.setattr(dl.pdfmeta, "stamp_payload",
                        lambda *a, **k: pdfmeta.StampOutcome(reason=pdfmeta.SKIP_FAILED, error="ValueError: odd"))
    with caplog.at_level(logging.INFO, logger="magsync"):
        await _place(tmp_path / "x.pdf", make_pdf())
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "Metadata stamp failed" in warnings[0] and "odd" in warnings[0]
