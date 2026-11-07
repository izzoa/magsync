"""Tests for `magsync repair-metadata` (stamping already-downloaded files)."""

from __future__ import annotations

import hashlib
import os
import pathlib
import sqlite3
import stat

import pytest
from bookorbit_emulation import bookorbit_view
from pdf_factory import make_pdf, revise
from typer.testing import CliRunner

import magsync.cli as cli_mod
import magsync.core.index as index_mod
import magsync.core.repair as repair_mod
from magsync.cli import app
from magsync.config import Config
from magsync.core import pdfmeta
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus
from magsync.core.repair import repair_metadata

runner = CliRunner()
TITLE = "AFAR – Spring 2025"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Library:
    """An index plus organized files, built the way earlier releases left them."""

    def __init__(self, tmp_path):
        self.db = tmp_path / "index.db"
        self.out = tmp_path / "magazines"
        self.out.mkdir()
        self.idx = MagazineIndex(db_path=self.db)
        self._n = 0

    def add(self, *, title=TITLE, magazine="AFAR", data=None, sha="payload", genre="Travel", path=None):
        self._n += 1
        mag = self.idx.get_or_create_magazine(magazine, magazine.lower())
        page = f"https://freemagazines.top/p{self._n}/"
        self.idx.add_issues(mag, [{"title": title, "page_url": page, "year": 2025, "month": 3, "genre": genre}])
        issue_id = self.idx.conn.execute("SELECT id FROM issues WHERE page_url=?", (page,)).fetchone()[0]
        if path is None:
            path = self.out / magazine / f"{magazine} - {self._n}.pdf"
            path.parent.mkdir(parents=True, exist_ok=True)
        if data is not None:
            path.write_bytes(data)
        recorded = _sha(path.read_bytes()) if sha == "payload" and path.exists() else sha
        self.idx.update_download_status(
            issue_id, DownloadStatus.COMPLETE, str(path), path.stat().st_size if path.exists() else 1, recorded
        )
        return issue_id, path

    def row(self, issue_id):
        return self.idx.conn.execute("SELECT * FROM downloads WHERE issue_id=?", (issue_id,)).fetchone()

    def run(self, **kwargs):
        return repair_metadata(self.idx, self.out, **kwargs)


@pytest.fixture
def lib(tmp_path):
    library = Library(tmp_path)
    yield library
    library.idx.close()


def test_legacy_file_is_stamped_and_both_identities_recorded(lib):
    payload = make_pdf()
    issue_id, path = lib.add(data=payload)
    report = lib.run()
    assert report.counts["stamped"] == 1 and report.outcome == "succeeded"
    placed = path.read_bytes()
    assert placed.startswith(payload) and len(placed) > len(payload)
    row = lib.row(issue_id)
    assert (row["sha256"], row["file_size_bytes"]) == (_sha(placed), len(placed))
    assert (row["source_sha256"], row["source_size"]) == (_sha(payload), len(payload))
    _fp, size, mtime_ns, inode = pdfmeta.parse_stamp_record(row["metadata_stamp"])
    st = path.stat()
    assert (size, mtime_ns, inode) == (st.st_size, st.st_mtime_ns, st.st_ino)
    parsed = bookorbit_view(placed)["parsed"]
    assert (parsed["title"], parsed["seriesName"], parsed["seriesIndex"]) == (TITLE, "AFAR", "20250301")
    assert [p.name for p in path.parent.iterdir()] == [path.name]  # no staging left


def test_restamp_after_title_change_keeps_exactly_one_update(lib):
    payload = make_pdf()
    issue_id, path = lib.add(data=payload)
    lib.run()
    lib.idx.conn.execute("UPDATE issues SET title=? WHERE id=?", ("AFAR – Summer 2025", issue_id))
    lib.idx.conn.commit()
    assert lib.run().counts["stamped"] == 1
    placed = path.read_bytes()
    new_spec = pdfmeta.stamp_spec_for({"title": "AFAR – Summer 2025", "genre": "Travel"})
    assert placed == payload + pdfmeta.stamp_payload(payload, new_spec).appended
    assert bookorbit_view(placed)["parsed"]["seriesIndex"] == "20250601"


def test_rerun_is_current_without_reading_and_verify_reads(lib, monkeypatch):
    _issue_id, path = lib.add(data=make_pdf())
    lib.run()
    before = path.stat()
    reads = []
    real = pathlib.Path.read_bytes
    monkeypatch.setattr(pathlib.Path, "read_bytes", lambda self: reads.append(self) or real(self))
    assert lib.run().counts["current"] == 1
    assert reads == [] and path.stat().st_mtime_ns == before.st_mtime_ns
    report = lib.run(verify=True)
    assert report.counts["current"] == 1 and reads  # hashed, still current, not rewritten
    assert path.stat().st_mtime_ns == before.st_mtime_ns


def test_same_size_edit_with_preserved_mtime_is_caught_by_verify(lib):
    _issue_id, path = lib.add(data=make_pdf())
    lib.run()
    st = path.stat()
    data = bytearray(path.read_bytes())
    data[20] ^= 0xFF
    with open(path, "r+b") as handle:  # same inode, same size
        handle.write(bytes(data))
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert lib.run(verify=True).counts["modified"] == 1
    assert path.read_bytes() == bytes(data)


def test_revision_appended_by_another_tool_is_left_alone(lib):
    _issue_id, path = lib.add(data=make_pdf())
    lib.run()
    edited = revise(path.read_bytes(), {"/Producer": "annotator"})
    path.write_bytes(edited)
    assert lib.run().counts["modified"] == 1
    assert path.read_bytes() == edited


def test_rows_that_disagree_leave_the_file_untouched(lib):
    payload = make_pdf()
    _, path = lib.add(data=payload)
    lib.add(title="AFAR – Summer 2025", path=path)
    assert lib.run().counts["ambiguous"] == 1
    assert path.read_bytes() == payload


def test_magazine_filter_selects_files_not_rows(lib):
    payload = make_pdf()
    first, path = lib.add(data=payload, magazine="AFAR")
    second, _ = lib.add(magazine="AFAR USA", path=path)
    assert lib.run(query="afar usa").counts["stamped"] == 1
    rows = [lib.row(first), lib.row(second)]
    assert {r["sha256"] for r in rows} == {_sha(path.read_bytes())}
    assert all(r["source_sha256"] == _sha(payload) for r in rows)


def test_file_written_back_by_bookorbit_is_edited(lib):
    xmp = (
        b'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        b'<rdf:Description rdf:about="" xmlns:bookorbit="https://bookorbit.app/metadata/1.0/">'
        b"<bookorbit:seriesName>AFAR</bookorbit:seriesName></rdf:Description></rdf:RDF></x:xmpmeta>"
    )
    data = make_pdf(info={"/Title": "Hand edited", "/Creator": "bookorbit"}, xmp=xmp)
    _, path = lib.add(data=data, sha=_sha(b"the original payload"))
    assert lib.run().counts["edited"] == 1
    assert path.read_bytes() == data


def test_missing_file_is_reported(lib):
    _, path = lib.add(data=make_pdf())
    path.unlink()
    assert lib.run().counts["missing"] == 1


def test_replace_whose_index_update_was_lost_is_healed(lib):
    payload = make_pdf()
    issue_id, path = lib.add(data=payload)  # index records the legacy payload hash
    spec = pdfmeta.stamp_spec_for({"title": TITLE, "genre": "Travel"})
    stamped = payload + pdfmeta.stamp_payload(payload, spec).appended
    path.write_bytes(stamped)  # the replace happened; the index update did not
    report = lib.run()
    assert report.counts["current"] == 1 and report.lines[0][2] == "healed"
    assert path.read_bytes() == stamped
    row = lib.row(issue_id)
    assert (row["sha256"], row["source_sha256"]) == (_sha(stamped), _sha(payload))
    assert lib.run().counts["current"] == 1  # and the fast path now applies


def test_file_changed_between_staging_and_finalize_is_busy(lib, monkeypatch):
    payload = make_pdf()
    _, path = lib.add(data=payload)
    real_prepare = repair_mod.prepare_file

    def prepare_then_touch(group, **kwargs):
        plan = real_prepare(group, **kwargs)
        os.utime(path, ns=(1, 1))
        return plan

    monkeypatch.setattr(repair_mod, "prepare_file", prepare_then_touch)
    assert lib.run().counts["busy"] == 1
    assert path.read_bytes() == payload
    assert [p.name for p in path.parent.iterdir()] == [path.name]


def test_permissions_are_preserved_and_stale_staging_removed(lib):
    _, path = lib.add(data=make_pdf())
    path.chmod(0o640)
    stale = path.parent / f".{path.name}.dead00.stamp"
    stale.write_bytes(b"left by a crash")
    assert lib.run().counts["stamped"] == 1
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    assert not stale.exists()


def test_encrypted_file_is_unstampable_but_its_identity_is_recorded(lib):
    data = make_pdf(encrypt=True)
    issue_id, path = lib.add(data=data, sha=None)
    report = lib.run()
    assert report.counts["unstampable"] == 1 and report.lines[0][2] == pdfmeta.SKIP_ENCRYPTED
    assert path.read_bytes() == data
    assert lib.row(issue_id)["sha256"] == _sha(data)


def test_one_failing_file_makes_the_run_partial(lib, monkeypatch):
    lib.add(data=make_pdf())
    lib.add(title="AFAR – Summer 2025", data=make_pdf(pages=3))

    def explode(group, **kwargs):
        if "Summer" in group.rows[0]["title"]:
            raise OSError("disk error")
        return real(group, **kwargs)

    real = repair_mod.prepare_file
    monkeypatch.setattr(repair_mod, "prepare_file", explode)
    report = lib.run()
    assert report.counts == {**report.counts, "stamped": 1, "failed": 1}
    assert report.outcome == "partial"


def test_progress_every_ten_files_and_at_the_end(lib):
    for n in range(12):
        lib.add(title=f"AFAR – Issue {n}, 2025", data=make_pdf(pages=1 + n % 3))
    seen = []
    report = lib.run(on_progress=lambda r: seen.append(r.done))
    assert seen == [10, 12] and report.done == 12


# --- CLI -------------------------------------------------------------------------------


@pytest.fixture
def cli_lib(tmp_path, monkeypatch):
    library = Library(tmp_path)
    monkeypatch.setattr(index_mod, "get_db_path", lambda: library.db)
    monkeypatch.setattr(cli_mod, "load_config", lambda: Config(output_dir=str(library.out)))
    yield library
    library.idx.close()


def _tree(root):
    return sorted((str(p.relative_to(root)), p.stat().st_mtime_ns, p.stat().st_size) for p in root.rglob("*"))


def _rows(db):
    conn = sqlite3.connect(db)
    try:
        return conn.execute("SELECT * FROM downloads ORDER BY issue_id").fetchall()
    finally:
        conn.close()


def test_dry_run_changes_nothing_in_the_library_or_index(cli_lib):
    _, path = cli_lib.add(data=make_pdf())
    (path.parent / f".{path.name}.dead00.stamp").write_bytes(b"stale")  # even leftovers stay
    cli_lib.idx.close()
    tree, rows = _tree(cli_lib.out), _rows(cli_lib.db)
    result = runner.invoke(app, ["repair-metadata", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "would be stamped" in result.output and "Dry run" in result.output
    assert _tree(cli_lib.out) == tree and _rows(cli_lib.db) == rows
    cli_lib.idx = MagazineIndex(db_path=cli_lib.db)


def test_cli_stamps_reports_progress_and_filters(cli_lib):
    _, path = cli_lib.add(data=make_pdf())
    cli_lib.idx.close()
    result = runner.invoke(app, ["repair-metadata", "Other Magazine"])
    assert result.exit_code == 0 and "No downloaded files to check" in result.output
    result = runner.invoke(app, ["repair-metadata", "afar"])
    assert result.exit_code == 0, result.output
    assert "repair-metadata: 1/1 files (stamped 1" in result.output
    assert "1 stamped" in result.output and "Traceback" not in result.output
    assert bookorbit_view(path.read_bytes())["parsed"]["title"] == TITLE
    cli_lib.idx = MagazineIndex(db_path=cli_lib.db)


def test_cli_exits_nonzero_on_partial(cli_lib, monkeypatch):
    cli_lib.add(data=make_pdf())
    cli_lib.idx.close()

    def explode(group, **kwargs):
        raise OSError("disk error")

    monkeypatch.setattr(repair_mod, "prepare_file", explode)
    result = runner.invoke(app, ["repair-metadata"])
    assert result.exit_code == 1 and "disk error" in result.output
    cli_lib.idx = MagazineIndex(db_path=cli_lib.db)


# --- review hardening ------------------------------------------------------------------


def test_adopted_hash_of_an_annotated_file_never_authorizes_truncation(lib):
    payload = make_pdf()
    _first, path = lib.add(data=payload)
    lib.run()
    annotated = revise(path.read_bytes(), {"/Producer": "annotator"})  # marker and prefix intact
    path.write_bytes(annotated)
    # A repost of the same issue adopts the file as found: its hash recorded, nothing proven.
    lib.add(path=path, sha=_sha(annotated))
    report = lib.run(verify=True)
    assert report.counts["modified"] == 1
    assert path.read_bytes() == annotated


def test_bookorbit_edit_is_left_alone_even_when_its_hash_was_recorded(lib):
    xmp = (
        b'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        b'<rdf:Description rdf:about="" xmlns:bookorbit="https://bookorbit.app/metadata/1.0/">'
        b"<bookorbit:seriesName>AFAR</bookorbit:seriesName></rdf:Description></rdf:RDF></x:xmpmeta>"
    )
    data = make_pdf(info={"/Title": "Hand edited", "/Creator": "bookorbit"}, xmp=xmp)
    _, path = lib.add(data=data)  # recorded hash == current bytes
    assert lib.run().counts["edited"] == 1
    assert path.read_bytes() == data


def test_row_reaching_the_file_through_a_differently_named_symlink_is_updated(lib):
    payload = make_pdf()
    first, path = lib.add(data=payload)
    link = path.parent / "AFAR - March 2025 (link).pdf"
    link.symlink_to(path)
    second, _ = lib.add(path=link)
    assert lib.run().counts["stamped"] == 1
    placed = path.read_bytes()
    for issue_id in (first, second):
        row = lib.row(issue_id)
        assert (row["sha256"], row["source_sha256"]) == (_sha(placed), _sha(payload))
    assert lib.run().counts["current"] == 1


def test_staging_sweep_takes_names_literally(lib):
    payload = make_pdf()
    _, path = lib.add(data=payload, magazine="Wired [UK]")
    mine = path.parent / f".{path.name}.dead00.stamp"
    mine.write_bytes(b"left by a crash")
    other = path.parent / f".{path.name.replace('[UK]', 'U')}.cafe00.stamp"
    other.write_bytes(b"another file's staging")
    assert lib.run().counts["stamped"] == 1
    assert not mine.exists() and other.exists()


def test_longest_names_can_be_restamped(lib):
    path = lib.out / "AFAR" / ("A" * 237 + ".pdf")
    path.parent.mkdir(parents=True, exist_ok=True)
    _, path = lib.add(data=make_pdf(), path=path)
    assert lib.run().counts["stamped"] == 1
