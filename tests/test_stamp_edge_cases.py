"""Edge cases the 0.11.0 pre-release review found (hand-built sources).

- A source whose /Size understates the numbers it uses, or whose xref stream
  sits at /Size outside its own /Index (macOS Quartz), must not have a live
  object overwritten by the stamp.
- An all-stream chain whose revisions name different Info dicts must still
  reach Kavita, which applies older stream revisions last.
- An undated issue on a source whose /CreationDate is a reference must stamp.
- A magsync stamp that cannot be redone must keep its rows as they are.
"""

from __future__ import annotations

import io
import re
import shutil
import subprocess

import pytest
from kavita_emulation import kavita_metadata
from pdf_factory import (
    hand_built_stream,
    hand_built_table,
    last_section_form,
    make_pdf,
    with_stream_revision_new_info,
)
from pypdf import PdfReader
from pypdf.generic import IndirectObject
from test_repair_metadata import Library
from test_stamp_versions import _record_as_0_10_did
from test_stamp_versions import _spec as _versions_spec

from magsync.core import pdfmeta

AFAR = {"title": "AFAR – Spring 2025", "page_url": "https://freemagazines.top/afar-spring-2025", "genre": "Travel"}

NUMBERING = {
    "table, /Size understated": hand_built_table(declared_size=7),
    "stream, /Size understated": hand_built_stream(declared_size=6),
    "stream at /Size outside its /Index (Quartz)": hand_built_stream(declared_size=7, xref_number=7, list_itself=False),
}


def _stamp(source: bytes, issue=AFAR) -> bytes:
    outcome = pdfmeta.stamp_payload(source, pdfmeta.stamp_spec_for(issue))
    assert outcome.reason is None, (outcome.reason, outcome.error)
    return source + outcome.appended


def _numbers_in_use(data: bytes) -> set[int]:
    reader = PdfReader(io.BytesIO(data))
    used = {idnum for entries in reader.xref.values() for idnum in entries} | set(reader.xref_objStm)
    used |= {int(n) for n in re.findall(rb"(\d+)\s+\d+\s+obj\b", data)}
    return used


@pytest.mark.parametrize("name", list(NUMBERING))
def test_new_objects_never_reuse_a_number_the_source_uses(name):
    source = NUMBERING[name]
    stamped = _stamp(source)
    written = {int(n) for n in re.findall(rb"(\d+)\s+\d+\s+obj\b", stamped[len(source):])}
    reused = written & _numbers_in_use(source)
    layout = pdfmeta._source_layout(source, PdfReader(io.BytesIO(source)))
    if layout.form == "stream":
        reused -= {layout.root[0]} | ({layout.info[0]} if layout.info else set())  # redefined on purpose
    assert not reused, f"{name}: {sorted(reused)}"
    page = PdfReader(io.BytesIO(stamped)).pages[0]
    assert "Hello magazine" in page.extract_text()


@pytest.mark.skipif(shutil.which("qpdf") is None, reason="qpdf not installed")
@pytest.mark.parametrize("name", list(NUMBERING))
def test_qpdf_still_finds_the_page_content(name, tmp_path):
    path = tmp_path / "stamped.pdf"
    path.write_bytes(_stamp(NUMBERING[name]))
    result = subprocess.run(["qpdf", "--check", str(path)], check=False, capture_output=True, text=True)
    assert "supposed to be a stream" not in result.stdout + result.stderr


def test_a_verify_that_sees_a_lost_page_refuses(monkeypatch):
    original = pdfmeta._write_v2

    def drop_pages(base, reader, layout, spec, **kwargs):
        stamped = original(base, reader, layout, spec, **kwargs)
        return stamped.replace(b"/Count 1", b"/Count 0")

    monkeypatch.setattr(pdfmeta, "_write_v2", drop_pages)
    outcome = pdfmeta.stamp_payload(hand_built_table(), pdfmeta.stamp_spec_for(AFAR))
    assert outcome.reason == pdfmeta.SKIP_FAILED


def test_all_stream_chain_with_differing_info_numbers_takes_a_table_kavita_reads():
    source = with_stream_revision_new_info(hand_built_stream())
    assert last_section_form(source) == "stream"
    assert pdfmeta._source_layout(source, PdfReader(io.BytesIO(source))).form == "table"
    stamped = _stamp(source)
    assert last_section_form(stamped) == "table"
    metadata = kavita_metadata(stamped)
    assert (metadata["Title"], metadata["Series"], metadata["Volume"]) == ("AFAR – Spring 2025", "AFAR", "2025060")
    assert metadata.get("Author", "") == ""
    assert "downmagaz.net" not in repr(metadata) and "another junk" not in repr(metadata)


def test_uniform_all_stream_chain_keeps_the_stream_layout():
    source = hand_built_stream()
    assert pdfmeta._source_layout(source, PdfReader(io.BytesIO(source))).form == "stream"
    assert kavita_metadata(_stamp(source))["Title"] == "AFAR – Spring 2025"


def test_undated_issue_with_an_indirect_creation_date_stamps_and_inlines_it():
    undated = {"title": "Mystery Magazine", "page_url": "https://x/mystery"}
    source = hand_built_table(creation_date_indirect=True)
    stamped = _stamp(source, undated)
    reader = PdfReader(io.BytesIO(stamped))
    assert reader.metadata["/CreationDate"] == "D:20200101000000Z"
    trailer = pdfmeta._newest_trailer(stamped, reader)
    assert not isinstance(trailer["/Info"].raw_get("/CreationDate"), IndirectObject)
    assert kavita_metadata(stamped)["CreationDate"] == "D:20200101000000Z"


@pytest.fixture
def lib(tmp_path):
    library = Library(tmp_path)
    yield library
    library.idx.close()


def test_a_magsync_stamp_that_cannot_be_redone_keeps_its_rows(lib, monkeypatch):
    payload = make_pdf()
    v1 = payload + pdfmeta.stamp_payload(payload, _versions_spec(), version=1).appended
    issue_id, path = lib.add(data=v1)
    _record_as_0_10_did(lib, issue_id, path, payload)
    before = dict(lib.row(issue_id))

    original = pdfmeta.stamp_payload

    def failing(source, spec, *, source_sha256=None, version=None):
        if version in (None, pdfmeta.STAMP_VERSION):
            return pdfmeta.StampOutcome(reason=pdfmeta.SKIP_FAILED, error="simulated")
        return original(source, spec, source_sha256=source_sha256, version=version)

    monkeypatch.setattr(pdfmeta, "stamp_payload", failing)
    report = lib.run()
    assert report.counts["unstampable"] == 1, report.lines
    assert dict(lib.row(issue_id)) == before  # the version-1 stamp record still stands
    assert path.read_bytes() == v1
