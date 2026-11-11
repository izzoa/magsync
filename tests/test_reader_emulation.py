"""The library-app reader oracles behave like the upstream code they port.

Everything here is built from hand-written XMP packets and small pypdf PDFs;
no magsync code is involved, so the oracles can later judge magsync's stamps.
"""

from __future__ import annotations

import ast
import io
from datetime import UTC, datetime
from pathlib import Path

import pytest
import reader_emulation as rem
from pdf_factory import JUNK, JUNK_XMP, containers, make_pdf
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, StreamObject

TITLE = "AFAR – Spring 2025"
NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
RDF_DECL = 'xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"'
BOOKORBIT = "https://bookorbit.app/metadata/1.0/"


def packet(*descriptions: str, rdf_decl_on_xmpmeta: bool = False, xpacket: bool = True) -> bytes:
    meta_decl = f" {RDF_DECL}" if rdf_decl_on_xmpmeta else ""
    rdf_decl = "" if rdf_decl_on_xmpmeta else f" {RDF_DECL}"
    body = (
        f'<x:xmpmeta xmlns:x="adobe:ns:meta/"{meta_decl}><rdf:RDF{rdf_decl}>'
        + "".join(descriptions)
        + "</rdf:RDF></x:xmpmeta>"
    )
    if xpacket:
        body = f'<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>{body}<?xpacket end="w"?>'
    return body.encode()


def dc_desc(*, title=TITLE, date="2025-03-01", creators="Seq", extra="") -> str:
    parts = [
        f'<dc:title><rdf:Alt><rdf:li xml:lang="x-default">{title}</rdf:li></rdf:Alt></dc:title>',
    ]
    if creators:
        parts.append(f"<dc:creator><rdf:{creators}><rdf:li>AFAR Media</rdf:li></rdf:{creators}></dc:creator>")
    if date is not None:
        parts.append(f"<dc:date><rdf:Seq><rdf:li>{date}</rdf:li></rdf:Seq></dc:date>")
    return (
        '<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/">'
        + "".join(parts)
        + extra
        + "</rdf:Description>"
    )


def calibre_desc(*, series="AFAR", index="2025060") -> str:
    return (
        '<rdf:Description rdf:about="" xmlns:calibre="http://calibre-ebook.com/xmp-namespace" '
        'xmlns:calibreSI="http://calibre-ebook.com/xmp-namespace-series-index">'
        f'<calibre:series rdf:parseType="Resource"><rdf:value>{series}</rdf:value>'
        f"<calibreSI:series_index>{index}</calibreSI:series_index></calibre:series>"
        "</rdf:Description>"
    )


def custom_desc(prefix: str, uri: str, **fields: str) -> str:
    body = "".join(f"<{prefix}:{k}>{v}</{prefix}:{k}>" for k, v in fields.items())
    return f'<rdf:Description rdf:about="" xmlns:{prefix}="{uri}">{body}</rdf:Description>'


def pdf(xmp: bytes | None, **info: str) -> bytes:
    return make_pdf(info={f"/{k}": v for k, v in info.items()}, xmp=xmp)


def restamp(data: bytes, xmp: bytes, **info: str) -> bytes:
    """Another tool's incremental revision that swaps in a new XMP packet."""
    writer = PdfWriter(PdfReader(io.BytesIO(data)), incremental=True)
    stream = StreamObject()
    stream.set_data(xmp)
    stream[NameObject("/Type")] = NameObject("/Metadata")
    stream[NameObject("/Subtype")] = NameObject("/XML")
    writer.root_object[NameObject("/Metadata")] = writer._add_object(stream)
    if info:
        writer.add_metadata({f"/{k}": v for k, v in info.items()})
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


READERS = {"booklore": rem.booklore_view, "grimmory": rem.grimmory_view}


def test_oracles_do_not_import_magsync():
    tree = ast.parse(Path(rem.__file__).read_text())
    names = [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
    names += [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    assert not [n for n in names if n.split(".")[0] == "magsync"]


# ----------------------------------------------------------------------------- calibre
def test_calibre_reads_the_calibre_series_block():
    view = rem.calibre_view(packet(dc_desc(), calibre_desc()), now=NOW)
    assert (view["series"], view["series_index"]) == ("AFAR", 2025060.0)
    assert view["title"] == TITLE
    assert view["authors"] == ["AFAR Media"]
    assert (view["pubdate_raw"], view["pubdate_field"]) == ("2025-03-01", "dc:date")
    assert view["pubdate_needs_fill"] is False
    assert view["pubdate"] == "2025-03-01T00:00:00+00:00"


def test_calibre_flags_a_year_only_date_for_fill_in():
    view = rem.calibre_view(packet(dc_desc(date="2026")), now=NOW)
    assert view["pubdate_raw"] == "2026"
    assert view["pubdate_needs_fill"] is True
    assert {"month", "day"} <= set(view["pubdate_filled"])
    # parse_date's default: the current month, day 15.
    assert view["pubdate"] == "2026-11-15T00:00:00+00:00"


def test_calibre_has_no_series_without_a_calibre_series_block():
    view = rem.calibre_view(packet(dc_desc(), custom_desc("bookorbit", BOOKORBIT, seriesName="AFAR")))
    assert (view["series"], view["series_index"]) == (None, None)


def test_calibre_keeps_the_exact_index_as_a_python_float():
    assert rem.calibre_view(packet(calibre_desc(index="20250303")))["series_index"] == 20250303.0


def test_calibre_ignores_plain_text_forms_of_container_properties():
    desc = (
        '<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:pdf="http://ns.adobe.com/pdf/1.3/" xmlns:xmp="http://ns.adobe.com/xap/1.0/">'
        "<dc:title>AFAR</dc:title><dc:publisher>Afar Media</dc:publisher>"
        "<pdf:Keywords>travel</pdf:Keywords><xmp:CreateDate>2025-03-01T12:00:00Z</xmp:CreateDate>"
        "</rdf:Description>"
    )
    view = rem.calibre_view(packet(desc), now=NOW)
    assert (view["title"], view["publisher"], view["tags"]) == (None, None, [])
    assert (view["pubdate_raw"], view["pubdate_field"]) == ("2025-03-01T12:00:00Z", "xmp:CreateDate")


def test_calibre_prefers_info_only_when_info_moddate_beats_the_xmp_metadata_date():
    stamped = packet(
        dc_desc(),
        '<rdf:Description rdf:about="" xmlns:xmp="http://ns.adobe.com/xap/1.0/">'
        "<xmp:MetadataDate>2025-03-01T00:00:00Z</xmp:MetadataDate></rdf:Description>",
    )
    assert rem.calibre_prefers_info(stamped, "2025-03-02T00:00:00Z", now=NOW) is True
    assert rem.calibre_prefers_info(stamped, "2025-02-01T00:00:00Z", now=NOW) is False
    assert rem.calibre_prefers_info(stamped, None, now=NOW) is False
    # Without MetadataDate/ModifyDate calibre never prefers the Info dict.
    assert rem.calibre_prefers_info(packet(dc_desc()), "2030-01-01T00:00:00Z", now=NOW) is False


# ----------------------------------------------------------------------------- Booklore + Grimmory
@pytest.mark.parametrize("reader", READERS)
def test_booklore_and_grimmory_read_the_calibre_series(reader):
    view = READERS[reader](pdf(packet(dc_desc(), calibre_desc()), Title=TITLE))
    assert (view["seriesName"], view["seriesNumber"]) == ("AFAR", 2025060.0)
    assert view["title"] == TITLE


@pytest.mark.parametrize("reader", READERS)
@pytest.mark.parametrize("index", ["20250303", "20250305"])
def test_yyyymmdd_series_numbers_collapse_in_float32(reader, index):
    view = READERS[reader](pdf(packet(dc_desc(), calibre_desc(index=index))))
    assert view["seriesNumber"] == 20250304.0


@pytest.mark.parametrize("reader", READERS)
def test_blank_info_author_gives_no_author(reader):
    view = READERS[reader](pdf(packet(dc_desc(creators=None)), Title=TITLE, Author=""))
    assert view["authors"] == []


def test_booklore_dates_from_info_creation_date_and_ignores_dc_date():
    data = pdf(packet(dc_desc(date="2024-12-25")), CreationDate="D:20250301120000Z")
    assert rem.booklore_view(data)["publishedDate"] == "2025-03-01"
    assert rem.booklore_view(pdf(packet(dc_desc(date="2024-12-25"))))["publishedDate"] is None


def test_booklore_turns_creation_date_into_a_local_date_in_the_jvm_zone():
    data = pdf(None, CreationDate="D:20250301120000+05'00'")  # 07:00 UTC
    assert rem.booklore_view(data)["publishedDate"] == "2025-03-01"
    assert rem.booklore_view(data, zone="America/Los_Angeles")["publishedDate"] == "2025-02-28"
    # Grimmory just reads the digits.
    assert rem.grimmory_view(data)["publishedDate"] == "2025-03-01"


def test_grimmory_prefers_dc_date_over_info_creation_date():
    data = pdf(packet(dc_desc(date="2024-12-25")), CreationDate="D:20250301120000Z")
    assert rem.grimmory_view(data)["publishedDate"] == "2024-12-25"
    # A full XMP datetime fails parsePdfDate, so the Info date stands.
    data = pdf(packet(dc_desc(date="2024-12-25T00:00:00Z")), CreationDate="D:20250301120000Z")
    assert rem.grimmory_view(data)["publishedDate"] == "2025-03-01"


def test_grimmory_finds_bookorbit_series_name_by_local_name():
    xmp = packet(
        dc_desc(), calibre_desc(), custom_desc("bookorbit", BOOKORBIT, seriesName="AFAR Magazine", seriesIndex="2")
    )
    grimmory = rem.grimmory_view(pdf(xmp))
    assert grimmory["seriesName"] == "AFAR Magazine"
    assert grimmory["seriesNumber"] == 2025060.0  # bookorbit:seriesIndex is not seriesNumber
    # Booklore only reads its own namespace.
    assert rem.booklore_view(pdf(xmp))["seriesName"] == "AFAR"


def test_grimmory_refuses_to_pick_between_namespaces_with_different_values():
    other = custom_desc("other", "https://example.org/ns/", seriesName="Something else")
    xmp = packet(dc_desc(), custom_desc("bookorbit", BOOKORBIT, seriesName="AFAR"), other)
    with pytest.raises(rem.GrimmoryAmbiguity):
        rem.grimmory_view(pdf(xmp))
    same = custom_desc("other", "https://example.org/ns/", seriesName="AFAR")
    view = rem.grimmory_view(pdf(packet(dc_desc(), custom_desc("bookorbit", BOOKORBIT, seriesName="AFAR"), same)))
    assert view["seriesName"] == "AFAR"
    assert view["flagged"] == [("seriesName", ["bookorbit:seriesName", "other:seriesName"])]


def test_grimmory_loses_the_packet_when_rdf_is_declared_on_xmpmeta():
    # PDFium4j scans the raw bytes and parses only the <rdf:RDF> block.
    data = pdf(packet(dc_desc(), calibre_desc(), rdf_decl_on_xmpmeta=True), Title="Info title")
    view = rem.grimmory_view(data)
    assert view["xmp_source"].startswith(b"<rdf:RDF>")
    assert (view["title"], view["seriesName"]) == ("Info title", None)
    assert rem.booklore_view(data)["title"] == TITLE


def test_booklore_reads_creators_only_from_rdf_seq():
    data = pdf(packet(dc_desc(creators="Bag")), Author="Info Author")
    assert rem.booklore_view(data)["authors"] == ["Info Author"]
    assert rem.grimmory_view(data)["authors"] == ["AFAR Media"]


@pytest.mark.parametrize("kind", list(containers()))
def test_every_container_shows_the_newest_packet_after_a_revision(kind):
    data = restamp(containers()[kind], packet(dc_desc(), calibre_desc()))
    for reader in READERS.values():
        view = reader(data)
        assert (view["title"], view["seriesName"], view["seriesNumber"]) == (TITLE, "AFAR", 2025060.0)
    assert rem.booklore_view(containers()[kind])["title"] == JUNK


def test_doctype_is_refused():
    hostile = (
        b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY e "boom">]>'
        + packet(dc_desc(title="&e;"))
    )
    with pytest.raises(rem.XmlRefused):
        rem.calibre_view(hostile)
    assert rem.booklore_view(pdf(hostile))["xmp_error"]
    # xmpbox sets disallow-doctype-decl (DomXmpParser.java:83): the piece fails to parse.
    strict = rem.strict_xmp_view(hostile)
    assert [d["kept"] for d in strict["descriptions"]] == [False]
    assert "Undefined" in strict["descriptions"][0]["error"]


# ----------------------------------------------------------------------------- strict xmpbox (JabRef)
def test_strict_drops_a_description_mixing_dc_with_an_unknown_namespace():
    mixed = dc_desc(extra=f'<bookorbit:seriesName xmlns:bookorbit="{BOOKORBIT}">AFAR</bookorbit:seriesName>')
    view = rem.strict_xmp_view(packet(mixed))
    assert [d["kept"] for d in view["descriptions"]] == [False]
    assert "NoSchema" in view["descriptions"][0]["error"]
    assert view["dc"] == []


def test_strict_keeps_dc_alone_next_to_unknown_namespace_descriptions():
    xmp = packet(
        custom_desc("bookorbit", BOOKORBIT, seriesName="AFAR"), dc_desc(title="AFAR"), calibre_desc()
    )
    view = rem.strict_xmp_view(xmp)
    assert [d["kept"] for d in view["descriptions"]] == [False, True, False]
    assert view["dc"] == [
        {"title": "AFAR", "creators": ["AFAR Media"], "subjects": [], "dates": ["2025-03-01"], "description": None}
    ]


def test_strict_reads_utf8_through_pdfdocencoding_like_jabref():
    view = rem.strict_xmp_view(packet(dc_desc()))
    assert view["dc"][0]["title"] == "AFAR â•ﬁ Spring 2025"  # the en dash's UTF-8 bytes
    assert rem.strict_xmp_view(packet(dc_desc()), pdfdoc_decoding=False)["dc"][0]["title"] == TITLE


def test_strict_requires_the_xmp_container_forms():
    assert rem.strict_xmp_view(packet(dc_desc(creators="Bag")))["dc"] == []  # dc:creator must be a Seq
    junk = rem.strict_xmp_view(JUNK_XMP)
    assert [d["kept"] for d in junk["descriptions"]] == [False]
    no_wrapper = rem.strict_xmp_view(packet(dc_desc(), xpacket=False))
    assert "XpacketBadStart" in no_wrapper["descriptions"][0]["error"]


@pytest.mark.parametrize(("pages", "kept"), [("2", True), ("two", False)])
def test_strict_validates_xmp_page_text_npages_as_an_integer(pages, kept):
    # XMPageTextSchema declares NPages as Integer (xmpbox 3.0.8): a number is kept,
    # anything Integer.parseInt rejects drops the Description with a Format error.
    desc = (
        '<rdf:Description rdf:about="" xmlns:xmpTPg="http://ns.adobe.com/xap/1.0/t/pg/">'
        f"<xmpTPg:NPages>{pages}</xmpTPg:NPages></rdf:Description>"
    )
    view = rem.strict_xmp_view(packet(dc_desc(title="AFAR"), desc))
    assert [d["kept"] for d in view["descriptions"]] == [True, kept]
    if not kept:
        assert "Format" in view["descriptions"][1]["error"]
