"""Embedded-metadata stamping: derivation, packet, container cases, provenance.

Stamped output is checked with ``bookorbit_emulation`` — an independent port of
pdf-lib's parser and BookOrbit's field mapping — never with pdfmeta's own reader.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from bookorbit_emulation import bookorbit_view
from pdf_factory import JUNK, make_pdf, revise
from pypdf import PdfWriter

from magsync.core import pdfmeta
from magsync.core.models import StampSpec
from magsync.core.organizer import parse_date

FIXTURES = Path(__file__).parent / "fixtures" / "pdf"
AFAR = {"title": "AFAR – Spring 2025", "page_url": "https://freemagazines.top/afar-spring-2025", "genre": "Travel, Lifestyle"}
_pdf = make_pdf
_revise = revise


def _spec(issue=AFAR) -> StampSpec:
    spec = pdfmeta.stamp_spec_for(issue)
    assert spec is not None
    return spec


def _stamped(source: bytes, spec: StampSpec) -> bytes:
    outcome = pdfmeta.stamp_payload(source, spec)
    assert outcome.reason is None, outcome.reason
    return source + outcome.appended


# --- date and field derivation ------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "day"),
    [
        ("The Economist – April 4th-10th, 2026", 4),
        ("New Scientist – 21 March 2026", 21),
        ("Weekly – February 16-23, 2026", 16),
        ("Radio Times – 4-10 April 2026", 4),
        ("Weekly – 4th-10th April 2026", 4),
        ("The New Yorker – April 13, 2026", 13),
        ("Monthly – March 2026", None),
        ("AFAR – Spring 2026", None),
        ("Science News – Vol 208 No 05, May 2026", None),
    ],
)
def test_parse_date_captures_day_only_when_stated(title, day):
    assert parse_date(title).day == day


@pytest.mark.parametrize(
    ("issue", "date", "index"),
    [
        ({"title": "Bon Appétit – August 2024"}, "2024-08-01", "20240801"),
        ({"title": "The Economist – April 4th-10th, 2026"}, "2026-04-04", "20260404"),
        ({"title": "New Scientist – 21 March 2026"}, "2026-03-21", "20260321"),
        ({"title": "AFAR – Spring 2025"}, "2025-03-01", "20250301"),
        ({"title": "Annual Guide – 27th Edition, 2026"}, "2026", "20260000"),
        ({"title": "Weekly – February 30, 2026"}, "2026-02-01", "20260201"),
        ({"title": "Radio Times – 4-10 April 2026"}, "2026-04-04", "20260404"),
        ({"title": "The Economist – March 28th-April 3rd, 2026"}, "2026-03-28", "20260328"),
        ({"title": "TV Times – 28 March - 3 April 2026"}, "2026-03-28", "20260328"),
        ({"title": "The Economist – December 27th-January 2nd, 2027"}, "2026-12-27", "20261227"),
        ({"title": "Special Collector Issue", "year": 2025, "month": 6}, "2025-06-01", "20250601"),
        ({"title": "Mystery Magazine", "page_url": "https://x/mystery"}, None, None),
    ],
)
def test_date_and_series_index(issue, date, index):
    spec = _spec(issue)
    assert pdfmeta.date_text(spec) == date
    assert pdfmeta.series_index(spec) == index


def test_spec_strips_source_decorations_and_splits_genres():
    spec = _spec({"title": "[PDF] AFAR – Spring 2025 | Magazine PDF", "genre": " Travel ,Lifestyle,, Travel "})
    assert spec.title == "AFAR – Spring 2025"
    assert spec.series_name == "AFAR"
    assert spec.genres == ("Travel", "Lifestyle")


def test_spec_without_title_is_none():
    assert pdfmeta.stamp_spec_for({"title": "  "}) is None


def test_fingerprint_tracks_metadata_and_record_round_trips():
    spec = _spec()
    other = _spec({**AFAR, "title": "AFAR – Summer 2025"})
    assert pdfmeta.fingerprint(spec) != pdfmeta.fingerprint(other)
    record = pdfmeta.stamp_record(pdfmeta.fingerprint(spec), 10, 20, 30)
    assert pdfmeta.parse_stamp_record(record) == (pdfmeta.fingerprint(spec), 10, 20, 30)
    assert pdfmeta.parse_stamp_record("garbage") is None


# --- the packet -------------------------------------------------------------------------


def test_packet_is_strict_xml_without_late_declaration_or_junk_fields():
    xmp = pdfmeta.build_xmp(_spec(), page_count=114, source_sha256="a" * 64, source_size=100)
    ET.fromstring(xmp)  # strict parser accepts it
    assert b"<?xml" not in xmp
    for absent in (b"dc:creator", b"dc:description", b"pdf:Keywords"):
        assert absent not in xmp
    assert pdfmeta.build_xmp(_spec(), page_count=114, source_sha256="a" * 64, source_size=100) == xmp


def test_packet_omits_unknown_date_and_genre():
    spec = _spec({"title": "Mystery Magazine"})
    xmp = pdfmeta.build_xmp(spec, page_count=1, source_sha256="a" * 64, source_size=1)
    assert b"dc:date" not in xmp and b"seriesIndex" not in xmp and b"dc:subject" not in xmp


# --- stamping across PDF containers -----------------------------------------------------


def _containers() -> dict[str, bytes]:
    base = _pdf()
    once = _revise(base, {"/Producer": "some editor"})
    return {
        "classic": base,
        "xref-stream": once,
        "several-revisions": _revise(once, {"/Creator": "another editor"}),
        "object-stream-catalog": (FIXTURES / "objstm.pdf").read_bytes(),
        "linearized": (FIXTURES / "linearized.pdf").read_bytes(),
        "hybrid-reference": (FIXTURES / "hybrid.pdf").read_bytes(),
        "no-xmp": _pdf(xmp=None),
    }


@pytest.mark.parametrize("kind", list(_containers()))
def test_stamp_reads_back_through_bookorbit_rules(kind):
    source = _containers()[kind]
    spec = _spec()
    stamped = _stamped(source, spec)
    assert stamped[: len(source)] == source
    assert pdfmeta.stamp_payload(source, spec).appended == stamped[len(source):]  # deterministic

    before = bookorbit_view(source)
    view = bookorbit_view(stamped)
    assert {"Root", "Info"} <= set(view["trailer"])
    if "ID" in before["trailer"]:
        assert "ID" in view["trailer"]
    parsed = view["parsed"]
    assert parsed["title"] == "AFAR – Spring 2025"
    assert parsed["authors"] == [] and parsed["description"] is None and parsed["tags"] == []
    assert parsed["genres"] == ["Travel", "Lifestyle"]
    assert parsed["seriesName"] == "AFAR" and parsed["seriesIndex"] == "20250301"
    assert parsed["publishedDate"] == "2025-03-01" and parsed["publishedYear"] == 2025
    assert parsed["pageCount"] == before["parsed"]["pageCount"]
    info = view["info"]
    assert info["Title"] == "AFAR – Spring 2025"
    assert info["Author"] is None and info["Subject"] is None and info["Keywords"] is None
    assert info["Creator"] != "bookorbit"


def test_unchanged_info_still_reaches_the_new_trailer():
    """pypdf omits /Info from the appended trailer when Info is unchanged."""
    spec = _spec()
    source = _revise(_pdf(info={"/Title": spec.title}), {"/Title": spec.title})
    view = bookorbit_view(_stamped(source, spec))
    assert "Info" in view["trailer"]
    assert view["info"]["Title"] == spec.title


@pytest.mark.skipif(shutil.which("pdfinfo") is None, reason="poppler not installed")
def test_poppler_reads_the_stamp(tmp_path):
    path = tmp_path / "stamped.pdf"
    path.write_bytes(_stamped(_pdf(), _spec()))
    out = subprocess.run(["pdfinfo", str(path)], capture_output=True, text=True, check=True).stdout
    assert "AFAR – Spring 2025" in out and JUNK not in out


def test_skip_reasons():
    spec = _spec()
    assert pdfmeta.stamp_payload(_pdf(encrypt=True), spec).reason == pdfmeta.SKIP_ENCRYPTED
    assert pdfmeta.stamp_payload(_pdf()[:-200], spec).reason == pdfmeta.SKIP_TRUNCATED
    assert pdfmeta.stamp_payload(b"not a pdf at all\n%%EOF\n", spec).reason == pdfmeta.SKIP_UNPARSEABLE


def test_oversized_payload_is_skipped(monkeypatch):
    monkeypatch.setattr(pdfmeta, "STAMP_SIZE_LIMIT", 10)
    assert pdfmeta.stamp_payload(_pdf(), _spec()).reason == pdfmeta.SKIP_OVERSIZED


def test_build_error_and_failed_self_check_are_skips(monkeypatch):
    source, spec = _pdf(), _spec()
    monkeypatch.setattr(pdfmeta, "verify_stamp", lambda *a, **k: False)
    assert pdfmeta.stamp_payload(source, spec).reason == pdfmeta.SKIP_FAILED
    monkeypatch.undo()

    def explode(self, stream):
        raise RuntimeError("boom")

    monkeypatch.setattr(PdfWriter, "write", explode)
    assert pdfmeta.stamp_payload(source, spec).reason == pdfmeta.SKIP_FAILED


# --- provenance --------------------------------------------------------------------------


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_recorded_stamped_file_resolves_to_its_original():
    source, spec = _pdf(), _spec()
    stamped = _stamped(source, spec)
    found = pdfmeta.resolve_original(
        stamped, trusted_sha256=_sha(stamped), legacy_sha256=None,
        recorded_source=(_sha(source), len(source)), spec=spec,
    )
    assert (found.kind, found.original_size, found.original_sha256) == ("recorded", len(source), _sha(source))


def test_marker_alone_identifies_the_original_when_index_lacks_it():
    source, spec = _pdf(), _spec()
    stamped = _stamped(source, spec)
    found = pdfmeta.resolve_original(
        stamped, trusted_sha256=_sha(stamped), legacy_sha256=None, recorded_source=None, spec=spec
    )
    assert (found.kind, found.original_size) == ("recorded", len(source))


def test_replace_whose_index_update_was_lost_is_healed():
    source, spec = _pdf(), _spec()
    stamped = _stamped(source, spec)
    found = pdfmeta.resolve_original(
        stamped, trusted_sha256=None, legacy_sha256=_sha(source), recorded_source=None, spec=spec
    )
    assert (found.kind, found.original_size) == ("healed", len(source))


def test_legacy_unstamped_files():
    source = _pdf()
    for trusted, legacy, kind in ((_sha(source), None, "recorded"), (None, _sha(source), "legacy"), (None, None, "legacy")):
        found = pdfmeta.resolve_original(
            source, trusted_sha256=trusted, legacy_sha256=legacy, recorded_source=None, spec=_spec()
        )
        assert (found.kind, found.original_size) == (kind, len(source))


def test_revision_appended_by_another_tool_is_never_discarded():
    source, spec = _pdf(), _spec()
    stamped = _stamped(source, spec)
    edited = _revise(stamped, {"/Producer": "annotator"})  # prefix and marker stay intact
    assert edited.startswith(stamped)
    found = pdfmeta.resolve_original(
        edited, trusted_sha256=_sha(stamped), legacy_sha256=None,
        recorded_source=(_sha(source), len(source)), spec=spec,
    )
    assert found.kind == "modified" and found.original_size is None


def test_marker_that_does_not_verify_is_modified():
    source, spec = _pdf(), _spec()
    stamped = bytearray(_stamped(source, spec))
    stamped[20] ^= 0xFF  # inside the original prefix; size unchanged
    found = pdfmeta.resolve_original(
        bytes(stamped), trusted_sha256=None, legacy_sha256=None, recorded_source=None, spec=spec
    )
    assert found.kind == "modified"


def test_same_size_edit_of_a_recorded_file_is_modified():
    source = _pdf()
    edited = bytearray(source)
    edited[20] ^= 0xFF
    found = pdfmeta.resolve_original(
        bytes(edited), trusted_sha256=None, legacy_sha256=_sha(source), recorded_source=None, spec=_spec()
    )
    assert found.kind == "modified"


def _bookorbit_written() -> bytes:
    xmp = (
        b'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        b'<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/" '
        b'xmlns:bookorbit="https://bookorbit.app/metadata/1.0/"><dc:title>Edited by hand</dc:title>'
        b"<bookorbit:seriesName>AFAR</bookorbit:seriesName></rdf:Description></rdf:RDF></x:xmpmeta>"
    )
    return _pdf(info={"/Title": "Edited by hand", "/Creator": "bookorbit"}, xmp=xmp)


def test_file_written_back_by_bookorbit_is_edited():
    data = _bookorbit_written()
    facts = pdfmeta.inspect_pdf(data)
    assert facts.bookorbit_traces and facts.marker is None
    # Even when a recorded hash matches it exactly (e.g. adopted as found).
    for trusted, legacy in ((None, None), (None, "0" * 64), (_sha(data), None), (None, _sha(data))):
        found = pdfmeta.resolve_original(
            data, trusted_sha256=trusted, legacy_sha256=legacy, recorded_source=None, spec=_spec()
        )
        assert found.kind == "edited"


def test_inspect_reports_marker_and_no_traces_for_own_stamp():
    source = _pdf()
    facts = pdfmeta.inspect_pdf(_stamped(source, _spec()))
    assert facts.marker == pdfmeta.Marker(_sha(source), len(source), pdfmeta.STAMP_VERSION)
    assert facts.bookorbit_traces is False and facts.eof_tail and facts.page_count == 2


def test_packet_with_a_dtd_is_not_parsed():
    assert pdfmeta.read_xmp_fields(b'<!DOCTYPE x [<!ENTITY a "b">]><x/>') == {}


# --- review hardening -------------------------------------------------------------------


def _shift_startxref(data: bytes, delta: int) -> bytes:
    position = data.rfind(b"startxref")
    head, tail = data[:position], data[position:]
    number = int(tail.split()[1])
    return head + tail.replace(str(number).encode(), str(max(number + delta, 0)).encode(), 1)


def _shift_xref_entries(data: bytes, delta: int) -> bytes:
    import re

    return re.sub(
        rb"(\d{10}) (\d{5}) n",
        lambda m: b"%010d %s n" % (int(m.group(1)) + delta, m.group(2)) if int(m.group(1)) else m.group(0),
        data,
    )


@pytest.mark.parametrize("damage", ["startxref+3", "startxref-3", "startxref=0", "entries+3"])
def test_sources_whose_xref_needs_repair_stay_unstamped(damage):
    source = _pdf()
    if damage == "entries+3":
        broken = _shift_xref_entries(source, 3)
    elif damage == "startxref=0":
        broken = _shift_startxref(source, -10**9)
    else:
        broken = _shift_startxref(source, int(damage[-2:]))
    assert broken != source
    assert pdfmeta.stamp_payload(broken, _spec()).reason == pdfmeta.SKIP_UNPARSEABLE


def test_source_ending_in_a_comment_without_eol_still_reaches_bookorbit():
    source = _pdf() + b"%trailing comment"  # no final newline
    stamped = _stamped(source, _spec())
    assert stamped.startswith(source)
    assert bookorbit_view(stamped)["parsed"]["title"] == "AFAR – Spring 2025"


def test_absurd_size_is_refused_quickly():
    import re
    import time

    source = re.sub(rb"/Size \d+", b"/Size 100000000", _pdf(), count=1)
    started = time.monotonic()
    assert pdfmeta.stamp_payload(source, _spec()).reason == pdfmeta.SKIP_UNPARSEABLE
    assert time.monotonic() - started < 5


def test_library_app_creator_is_never_carried_forward():
    source = _pdf(info={"/Title": "x", "/Creator": "BookOrbit", "/Producer": "kept"})
    info = bookorbit_view(_stamped(source, _spec()))["info"]
    assert info["Creator"] is None and info["Producer"] == "kept"


def test_failed_stamp_carries_a_reason(monkeypatch):
    source = _pdf()

    def explode(self, stream):
        raise RuntimeError("boom")

    monkeypatch.setattr(PdfWriter, "write", explode)
    outcome = pdfmeta.stamp_payload(source, _spec())
    assert outcome.reason == pdfmeta.SKIP_FAILED and "boom" in outcome.error


def test_hash_observed_on_adoption_never_authorizes_truncation():
    source, spec = _pdf(), _spec()
    annotated = _revise(_stamped(source, spec), {"/Producer": "annotator"})  # marker intact
    found = pdfmeta.resolve_original(
        annotated, trusted_sha256=None, legacy_sha256=_sha(annotated),
        recorded_source=(_sha(source), len(source)), spec=spec,
    )
    assert found.kind == "modified"
