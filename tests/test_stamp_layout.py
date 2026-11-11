"""Version-2 stamps: the update magsync lays out itself, in the source's form.

Readers disagree about incremental updates. Kavita lets an older table's entry
win and applies older revisions' metadata first, so the update's form and its
object numbers decide whether Kavita sees the stamp at all (design.md,
Decision 5). Stamped output is read back by independent readers: pdf-lib's
emulation (``bookorbit_emulation``), pypdf in strict and lenient mode, poppler
and qpdf where installed, never by pdfmeta's own reader.
"""

from __future__ import annotations

import dataclasses
import datetime
import io
import re
import shutil
import subprocess

import pytest
from bookorbit_emulation import bookorbit_view
from pdf_factory import JUNK, containers, last_section_form, make_pdf
from pypdf import PdfReader

from magsync.core import pdfmeta

AFAR = {"title": "AFAR – Spring 2025", "page_url": "https://freemagazines.top/afar-spring-2025", "genre": "Travel, Lifestyle"}
CONTAINERS = containers()
# The update's form by the source's whole chain (decision B): a stream only when
# every section is a stream. xref-stream and several-revisions end in a stream
# that another tool appended over a table, so they take a table.
UPDATE_FORM = {
    "classic": "table",
    "xref-stream": "table",
    "several-revisions": "table",
    "object-stream-catalog": "stream",
    "linearized": "table",
    "hybrid-reference": "table",
    "no-xmp": "table",
}


def _spec(issue=AFAR):
    spec = pdfmeta.stamp_spec_for(issue)
    assert spec is not None
    return spec


def _stamp(source: bytes, spec=None) -> bytes:
    outcome = pdfmeta.stamp_payload(source, spec or _spec(), version=2)
    assert outcome.reason is None, (outcome.reason, outcome.error)
    return source + outcome.appended


def _newest_refs(data: bytes) -> dict[str, tuple[int, int]]:
    """``/Root``, ``/Info`` and the catalog's ``/Metadata`` as the newest trailer names them."""
    reader = PdfReader(io.BytesIO(data))
    trailer = pdfmeta._newest_trailer(data, reader)
    root = trailer.raw_get("/Root")
    info = trailer.raw_get("/Info")
    metadata = root.get_object().raw_get("/Metadata")
    return {name: (ref.idnum, ref.generation) for name, ref in (("root", root), ("info", info), ("metadata", metadata))}


def _source_size(data: bytes) -> int:
    return int(PdfReader(io.BytesIO(data)).trailer["/Size"])


@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_update_form_follows_the_chain_and_keeps_the_original_prefix(kind):
    source = CONTAINERS[kind]
    stamped = _stamp(source)
    assert stamped[: len(source)] == source
    assert last_section_form(stamped) == UPDATE_FORM[kind]
    assert pdfmeta.stamp_payload(source, _spec(), version=2).appended == stamped[len(source):]  # deterministic
    tail = stamped[len(source):]
    assert b"/FlateDecode" not in tail and b"/ObjStm" not in tail  # nothing compressed


@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_newest_revision_readers_see_the_stamp(kind):
    stamped = _stamp(CONTAINERS[kind])
    for strict in (False, True):
        reader = PdfReader(io.BytesIO(stamped), strict=strict)
        assert reader.metadata["/Title"] == "AFAR – Spring 2025"
        assert len(reader.pages) == len(PdfReader(io.BytesIO(CONTAINERS[kind])).pages)
    view = bookorbit_view(stamped)
    parsed = view["parsed"]
    assert parsed["title"] == "AFAR – Spring 2025"
    assert (parsed["seriesName"], parsed["seriesIndex"]) == ("AFAR", "20250301")
    assert parsed["publishedDate"] == "2025-03-01" and parsed["genres"] == ["Travel", "Lifestyle"]
    assert parsed["authors"] == [] and parsed["description"] is None and parsed["tags"] == []
    assert view["info"]["Title"] == "AFAR – Spring 2025"


def test_every_container_is_classified():
    assert set(UPDATE_FORM) == set(CONTAINERS)


@pytest.mark.parametrize("kind", [kind for kind in CONTAINERS if UPDATE_FORM[kind] == "table"])
def test_after_a_table_the_stamped_objects_take_new_numbers(kind):
    source = CONTAINERS[kind]
    refs = _newest_refs(_stamp(source))
    size = _source_size(source)
    assert all(number >= size for number, _generation in refs.values()), refs


@pytest.mark.parametrize("kind", [kind for kind in CONTAINERS if UPDATE_FORM[kind] == "stream"])
def test_after_an_all_stream_chain_the_catalog_and_info_keep_their_numbers(kind):
    source = CONTAINERS[kind]
    before = PdfReader(io.BytesIO(source)).trailer
    refs = _newest_refs(_stamp(source))
    assert refs["root"] == (before.raw_get("/Root").idnum, before.raw_get("/Root").generation)
    assert refs["info"] == (before.raw_get("/Info").idnum, before.raw_get("/Info").generation)
    assert refs["metadata"][0] >= _source_size(source)


def test_compressed_catalog_and_info_are_rewritten_uncompressed():
    source = CONTAINERS["object-stream-catalog"]
    stamped = _stamp(source)
    refs = _newest_refs(stamped)
    tail = stamped[len(source):]
    for number, generation in (refs["root"], refs["info"]):
        assert re.search(rb"(?<!\d)%d %d obj" % (number, generation), tail)


def test_stamped_info_dict():
    source = make_pdf(info={"/Title": JUNK, "/Author": JUNK, "/Subject": JUNK, "/Keywords": JUNK,
                            "/Producer": "InDesign", "/Creator": "bookorbit", "/CreationDate": "D:20200101000000Z"})
    info = PdfReader(io.BytesIO(_stamp(source))).metadata
    assert info["/Title"] == "AFAR – Spring 2025"
    assert info["/CreationDate"] == "D:20250301120000Z"
    assert (info["/Author"], info["/Subject"], info["/Keywords"]) == ("", "", "")
    assert info["/Producer"] == "InDesign" and "/Creator" not in info


def test_undated_issue_keeps_the_sources_creation_date():
    spec = _spec({"title": "Mystery Magazine", "page_url": "https://x/mystery"})
    dated = make_pdf(info={"/Title": JUNK, "/CreationDate": "D:20200101000000Z"})
    assert PdfReader(io.BytesIO(_stamp(dated, spec))).metadata["/CreationDate"] == "D:20200101000000Z"
    undated = make_pdf(info={"/Title": JUNK})
    assert "/CreationDate" not in PdfReader(io.BytesIO(_stamp(undated, spec))).metadata


@pytest.mark.parametrize("hours", range(-11, 12))
def test_creation_date_keeps_its_calendar_day_from_utc_minus_11_to_plus_11(hours):
    """What an Info-only reader (Stump, Booklore) shows after converting to local time."""
    created = PdfReader(io.BytesIO(_stamp(CONTAINERS["classic"]))).metadata["/CreationDate"]
    moment = datetime.datetime.strptime(created, "D:%Y%m%d%H%M%SZ").replace(tzinfo=datetime.UTC)
    local = moment.astimezone(datetime.timezone(datetime.timedelta(hours=hours)))
    assert local.date() == datetime.date(2025, 3, 1)


@pytest.mark.skipif(shutil.which("qpdf") is None, reason="qpdf not installed")
@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_qpdf_finds_no_structural_errors(kind, tmp_path):
    path = tmp_path / "stamped.pdf"
    path.write_bytes(_stamp(CONTAINERS[kind]))
    result = subprocess.run(["qpdf", "--check", str(path)], check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.skipif(shutil.which("pdfinfo") is None, reason="poppler not installed")
@pytest.mark.parametrize("kind", ["classic", "object-stream-catalog", "hybrid-reference"])
def test_poppler_reads_the_v2_stamp(kind, tmp_path):
    path = tmp_path / "stamped.pdf"
    path.write_bytes(_stamp(CONTAINERS[kind]))
    out = subprocess.run(["pdfinfo", "-isodates", str(path)], capture_output=True, text=True, check=True).stdout
    assert "AFAR – Spring 2025" in out and JUNK not in out
    assert "2025-03-01T12:00:00" in out


# --- the self-check reads every vocabulary and the layout back -------------------------


def _corrupt_packet(monkeypatch, old: bytes, new: bytes):
    original = pdfmeta._build_xmp_v2

    def build(*args, **kwargs):
        packet = original(*args, **kwargs)
        assert old in packet
        return packet.replace(old, new)

    monkeypatch.setattr(pdfmeta, "_build_xmp_v2", build)


def _corrupt_layout(monkeypatch, **changes):
    original = pdfmeta._write_v2

    def write(base, reader, layout, *args, **kwargs):
        return original(base, reader, dataclasses.replace(layout, **changes), *args, **kwargs)

    monkeypatch.setattr(pdfmeta, "_write_v2", write)


def _assert_refused(source=None):
    outcome = pdfmeta.stamp_payload(source or CONTAINERS["classic"], _spec(), version=2)
    assert outcome.reason == pdfmeta.SKIP_FAILED
    assert outcome.error == "the stamped file failed its self-check"


def test_a_wrong_calibre_index_is_refused(monkeypatch):
    _corrupt_packet(monkeypatch, b">2025060<", b">2025061<")
    _assert_refused()


def test_a_missing_calibre_series_is_refused(monkeypatch):
    _corrupt_packet(monkeypatch, b"<rdf:value>AFAR</rdf:value>", b"")
    _assert_refused()


def test_a_wrong_prism_cover_date_is_refused(monkeypatch):
    _corrupt_packet(monkeypatch, b"<prism:coverDate>2025-03<", b"<prism:coverDate>2025-04<")
    _assert_refused()


def test_a_dublin_core_property_outside_its_own_description_is_refused(monkeypatch):
    # Moving dc:format into the BookOrbit block keeps BookOrbit's reading but
    # hides Dublin Core from strict parsers.
    _corrupt_packet(
        monkeypatch,
        b"   <bookorbit:seriesName>",
        b'   <dc:format xmlns:dc="http://purl.org/dc/elements/1.1/">x</dc:format>\n   <bookorbit:seriesName>',
    )
    _assert_refused()


def test_an_unblanked_info_entry_is_refused(monkeypatch):
    original = pdfmeta._stamped_info

    def info(*args, **kwargs):
        stamped = original(*args, **kwargs)
        stamped[pdfmeta.NameObject("/Author")] = pdfmeta.TextStringObject(JUNK)
        return stamped

    monkeypatch.setattr(pdfmeta, "_stamped_info", info)
    _assert_refused()


def test_a_wrong_creation_date_is_refused(monkeypatch):
    original = pdfmeta._stamped_info

    def info(*args, **kwargs):
        stamped = original(*args, **kwargs)
        stamped[pdfmeta.NameObject("/CreationDate")] = pdfmeta.TextStringObject("D:20250301000000Z")
        return stamped

    monkeypatch.setattr(pdfmeta, "_stamped_info", info)
    _assert_refused()


@pytest.mark.parametrize(
    ("kind", "form"), [("classic", "stream"), ("xref-stream", "stream"), ("object-stream-catalog", "table")]
)
def test_an_update_in_the_other_form_is_refused(monkeypatch, kind, form):
    _corrupt_layout(monkeypatch, form=form)
    _assert_refused(CONTAINERS[kind])


def test_a_table_update_reusing_the_catalog_number_is_refused(monkeypatch):
    # A /Size two short makes the writer hand out numbers the source already uses.
    _corrupt_layout(monkeypatch, first_free=_source_size(CONTAINERS["classic"]) - 2)
    _assert_refused()


def test_a_stream_update_moving_the_catalog_is_refused(monkeypatch):
    source = CONTAINERS["object-stream-catalog"]
    _corrupt_layout(monkeypatch, root=(_source_size(source) + 5, 0))
    _assert_refused(source)


def test_a_refused_self_check_places_the_payload_unstamped(monkeypatch, tmp_path):
    _corrupt_packet(monkeypatch, b">2025060<", b">2025061<")
    outcome = pdfmeta.stamp_payload(CONTAINERS["classic"], _spec(), version=2)
    assert outcome.appended is None and outcome.reason == pdfmeta.SKIP_FAILED


# --- the chain walk ---------------------------------------------------------------------


def _layout_form(data: bytes) -> str:
    return pdfmeta._source_layout(data, PdfReader(io.BytesIO(data))).form


@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_chain_walk_classifies_every_container(kind):
    assert _layout_form(CONTAINERS[kind]) == UPDATE_FORM[kind]


def test_an_all_stream_chain_stays_all_stream_after_another_stream_revision():
    from pdf_factory import revise

    revised = revise(CONTAINERS["object-stream-catalog"], {"/Producer": "some editor"})
    assert last_section_form(revised) == "stream" and _layout_form(revised) == "stream"
    stamped = _stamp(revised)
    assert last_section_form(stamped) == "stream"


def test_a_chain_that_loops_or_breaks_takes_the_table_layout():
    source = CONTAINERS["object-stream-catalog"]
    start = pdfmeta._last_startxref(source)
    reader = PdfReader(io.BytesIO(source))
    assert [section.form for section in pdfmeta._chain(source, start, reader)] == ["stream"]
    looped = source.replace(b"/Type /XRef", b"/Type /XRef /Prev %d" % start, 1)
    assert looped != source
    assert pdfmeta._chain(looped, start, PdfReader(io.BytesIO(source))) is None
    assert pdfmeta._chain(source, 3, reader) is None  # an offset that names no section
