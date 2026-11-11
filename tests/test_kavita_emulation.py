"""What Kavita reads from magsync-shaped stamps, checked with the ported reader.

Every PDF here is built with pypdf or by hand (never with magsync code) and
read with ``kavita_emulation``, a port of Kavita's own PDF metadata reader.
"""

from __future__ import annotations

import io
import re

import pytest
from kavita_emulation import (
    PdfMetadataExtractorException,
    UnsafeXmpRefused,
    kavita_comicinfo,
    kavita_metadata,
    kavita_walk,
)
from pdf_factory import JUNK, containers, last_section_form, make_pdf
from pypdf import PdfReader, PdfWriter
from pypdf.generic import DictionaryObject, NameObject, StreamObject, TextStringObject

TITLE = "AFAR – Spring 2025"
SERIES = "AFAR"
CALIBRE_INDEX = "2025060"  # ordinal date YYYYDDD
DATE = "2025-03-01"

DC_TITLE = f'<dc:title><rdf:Alt><rdf:li xml:lang="x-default">{TITLE}</rdf:li></rdf:Alt></dc:title>'
DC_DATE = f"<dc:date><rdf:Seq><rdf:li>{DATE}</rdf:li></rdf:Seq></dc:date>"
CALIBRE_SERIES = (
    f'<calibre:series rdf:parseType="Resource"><rdf:value>{SERIES}</rdf:value>'
    f"<calibreSI:series_index>{CALIBRE_INDEX}</calibreSI:series_index></calibre:series>"
)


def xmp_packet(*properties: str, header: bool = True) -> bytes:
    """A single-``rdf:Description`` packet declaring dc, calibre and calibreSI."""
    body = (
        '<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        '<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/"'
        ' xmlns:calibre="http://calibre-ebook.com/xmp-namespace"'
        ' xmlns:calibreSI="http://calibre-ebook.com/xmp-namespace-series-index">'
        + "".join(properties)
        + "</rdf:Description></rdf:RDF></x:xmpmeta>"
    )
    if header:
        body = '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>' + body + '<?xpacket end="w"?>'
    return body.encode()


V1_PACKET = xmp_packet(DC_TITLE, DC_DATE, CALIBRE_SERIES)
CONTAINERS = containers()


def v1_layout_stamp(source: bytes) -> bytes:
    """magsync's current (version 1) update layout, reproduced with pypdf alone.

    One incremental update holding a NEW Info object (the source Info minus
    /Title /Author /Subject /Keywords, plus the new /Title), a NEW /Metadata
    stream, and the rewritten catalog that points at it.
    """
    reader = PdfReader(io.BytesIO(source))
    writer = PdfWriter(reader, incremental=True)
    size = int(reader.trailer["/Size"])
    while len(writer._objects) < size - 1:
        writer._objects.append(None)
    info = DictionaryObject()
    source_info = reader.trailer.get("/Info")
    if source_info is not None:
        for key, value in source_info.get_object().items():
            if key not in ("/Title", "/Author", "/Subject", "/Keywords"):
                info[NameObject(key)] = value
    info[NameObject("/Title")] = TextStringObject(TITLE)
    writer._info_obj = writer._add_object(info)
    stream = StreamObject()
    stream.set_data(V1_PACKET)
    stream[NameObject("/Type")] = NameObject("/Metadata")
    stream[NameObject("/Subtype")] = NameObject("/XML")
    writer.root_object[NameObject("/Metadata")] = writer._add_object(stream)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def append_table_revision(base: bytes, objects: dict[int, bytes], *, info: int) -> bytes:
    """Append a hand-written classic-table revision (20-byte entries, /Prev chained)."""
    reader = PdfReader(io.BytesIO(base))
    root = reader.trailer.raw_get("/Root").idnum
    size = max(int(reader.trailer["/Size"]), max(objects) + 1)
    prev = int(re.findall(rb"startxref\s+(\d+)", base)[-1])
    out = bytearray(base)
    offsets = {}
    for num in sorted(objects):
        offsets[num] = len(out)
        out += b"%d 0 obj\n%s\nendobj\n" % (num, objects[num])
    xref_at = len(out)
    out += b"xref\n"
    for num in sorted(offsets):
        out += b"%d 1\n%010d 00000 n \n" % (num, offsets[num])
    out += b"trailer\n<< /Size %d /Root %d 0 R /Info %d 0 R /Prev %d >>\n" % (size, root, info, prev)
    out += b"startxref\n%d\n%%%%EOF\n" % xref_at
    return bytes(out)


def original_table_offset(data: bytes, num: int) -> int:
    """Object ``num``'s offset in the file's FIRST classic xref table (one 0-based subsection)."""
    table = re.search(rb"xref\s+0 (\d+)\s+", data)
    entry = data[table.end() + 20 * num:table.end() + 20 * num + 20]
    return int(entry[:10])


def newest_offset(data: bytes, num: int) -> int:
    """Where a spec-following reader (pypdf) locates object ``num``."""
    return PdfReader(io.BytesIO(data)).xref[0][num]


# --- the v1 layout ----------------------------------------------------------


def test_v1_classic_resolves_the_original_catalog():
    """Design prediction confirmed: on a table-only source the stamp is invisible.

    The stamp's xref stream locates the rewritten catalog, but following /Prev into
    the source's table overwrites that offset with the original one (upstream L911),
    so Kavita reads the junk packet, and the source Info is applied last.
    """
    source = CONTAINERS["classic"]
    stamped = v1_layout_stamp(source)
    assert last_section_form(stamped) == "stream"
    meta = kavita_metadata(stamped)
    assert meta["Title"] == JUNK
    assert meta["Author"] == JUNK
    assert "Series" not in meta
    assert "Volume" not in meta
    walk = kavita_walk(stamped)
    root = PdfReader(io.BytesIO(source)).trailer.raw_get("/Root").idnum
    assert walk["offsets"][root] == original_table_offset(source, root)
    assert walk["offsets"][root] != newest_offset(stamped, root)
    new_info = int(PdfReader(io.BytesIO(stamped)).trailer.raw_get("/Info").idnum)
    assert walk["applied"] == [(-1, new_info), (root, 1)]  # source Info popped last


def test_v1_xref_stream_also_resolves_the_original_catalog():
    """DISCREPANCY with the design: the predicted read (Title, Series, Volume and
    CreationDate from the new packet, only Author left as junk) does not happen.

    The xref-stream container still ends its /Prev chain in the source's TABLE
    (stamp stream -> pypdf's revision stream -> original table), and that table's
    plain assignment ``_objectOffsets[obj] = offset`` (upstream L911) replaces the
    stamp's catalog offset, exactly as on ``classic``. MetadataObjInObjectCatalog
    therefore reads the original catalog and the junk packet; the new Info is
    applied first and the source Info (obj 1, at its table offset) last.
    """
    source = CONTAINERS["xref-stream"]
    stamped = v1_layout_stamp(source)
    meta = kavita_metadata(stamped)
    assert meta["Title"] == JUNK
    assert meta["Author"] == JUNK
    assert meta["Summary"] == JUNK
    assert meta["Producer"] == "pypdf"  # the table's Info offset beat pypdf's revision too
    for key in ("Series", "Volume", "CreationDate"):
        assert key not in meta
    walk = kavita_walk(stamped)
    assert [form for form, _ in walk["sections"]] == ["stream", "stream", "table"]
    assert walk["offsets"][3] == original_table_offset(source, 3) != newest_offset(stamped, 3)
    assert walk["offsets"][1] == original_table_offset(source, 1) != newest_offset(source, 1)


V1_OUTCOME = {  # container -> (Title, Author, Series) Kavita reads after the v1 stamp
    "classic": (JUNK, JUNK, None),
    "xref-stream": (JUNK, JUNK, None),
    "several-revisions": (JUNK, JUNK, None),
    "linearized": (JUNK, JUNK, None),
    "no-xmp": (JUNK, JUNK, None),
    # Catalog and Info sit in an object stream (type 2 entries, which Kavita never
    # records, upstream L1143), so only the stamp's own objects are found.
    "object-stream-catalog": (TITLE, None, SERIES),
    "hybrid-reference": (TITLE, None, SERIES),
}


@pytest.mark.parametrize("kind", list(V1_OUTCOME))
def test_v1_layout_across_containers(kind):
    meta = kavita_metadata(v1_layout_stamp(CONTAINERS[kind]))
    assert (meta.get("Title"), meta.get("Author"), meta.get("Series")) == V1_OUTCOME[kind]
    if V1_OUTCOME[kind][2] is not None:
        assert meta["Volume"] == CALIBRE_INDEX
        assert meta["CreationDate"] == DATE


# --- unstamped containers ---------------------------------------------------


@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_every_unstamped_container_reads(kind):
    meta = kavita_metadata(CONTAINERS[kind])
    if kind in ("object-stream-catalog", "hybrid-reference"):
        assert meta == {}  # compressed catalog and Info are invisible
    else:
        assert meta["Title"] == JUNK
        assert meta["Author"] == JUNK


def test_classic_and_no_xmp_read_the_junk_title():
    assert kavita_metadata(CONTAINERS["classic"])["Title"] == JUNK
    assert kavita_metadata(CONTAINERS["classic"])["Summary"] == JUNK  # from the junk packet
    no_xmp = kavita_metadata(CONTAINERS["no-xmp"])
    assert no_xmp["Title"] == JUNK
    assert "Summary" not in no_xmp


def test_revision_info_over_a_table_is_ignored():
    """pypdf's /Producer revision (an xref stream) loses to the table's offset for obj 1."""
    assert kavita_metadata(CONTAINERS["xref-stream"])["Producer"] == "pypdf"


def test_encrypted_table_trailer_raises_kavitas_exception():
    with pytest.raises(PdfMetadataExtractorException, match="^Encryption not supported$"):
        kavita_metadata(make_pdf(encrypt=True))


# --- hand-made files --------------------------------------------------------


def test_single_revision_reads_series_volume_title_author_as_written():
    data = make_pdf(info={"/Title": TITLE, "/Author": "AFAR Editors"}, xmp=xmp_packet(DC_TITLE, DC_DATE, CALIBRE_SERIES))
    meta = kavita_metadata(data)
    assert meta["Title"] == TITLE
    assert meta["Author"] == "AFAR Editors"
    assert meta["Series"] == SERIES
    assert meta["Volume"] == CALIBRE_INDEX
    assert meta["CreationDate"] == DATE


def test_xmp_creator_beats_info_author_within_a_revision():
    creator = "<dc:creator><rdf:Seq><rdf:li>Ann</rdf:li><rdf:li>Bob</rdf:li></rdf:Seq></dc:creator>"
    data = make_pdf(info={"/Title": TITLE, "/Author": JUNK}, xmp=xmp_packet(DC_TITLE, creator))
    assert kavita_metadata(data)["Author"] == "Ann,Bob"


def test_later_table_revision_info_overwrites_even_with_an_empty_string():
    base = make_pdf(xmp=None)
    new_info = int(PdfReader(io.BytesIO(base)).trailer["/Size"])
    data = append_table_revision(base, {new_info: rb"<< /Title (AFAR \205 Spring 2025) /Author () >>"}, info=new_info)
    assert last_section_form(data) == "table"
    assert kavita_metadata(data)["Author"] == ""
    assert kavita_metadata(data)["Title"] == TITLE
    assert kavita_walk(data)["applied"] == [(-1, 1), (3, new_info)]


def test_rewriting_an_object_in_a_later_table_revision_is_invisible():
    """The same overwrite (upstream L911) as above, applied to offsets: oldest wins."""
    base = make_pdf(xmp=None)
    data = append_table_revision(base, {1: b"<< /Author () >>"}, info=1)
    assert kavita_metadata(data)["Author"] == JUNK


def test_string_escapes_follow_kavitas_lexer():
    base = make_pdf(xmp=None)
    new_info = int(PdfReader(io.BytesIO(base)).trailer["/Size"])
    data = append_table_revision(base, {new_info: rb"<< /Title (AFAR \(US\) \\ x) /Author <FEFF00410042> >>"},
                                 info=new_info)
    meta = kavita_metadata(data)
    assert meta["Title"] == "AFAR US  x"  # \( \) \\ have no case in the escape switch (L690-729)
    assert meta["Author"] == "AB"


def test_utf16_info_title_written_by_pypdf():
    title = "AFAR 春 2025"  # not PDFDocEncodable, so pypdf writes UTF-16BE with a BOM
    assert kavita_metadata(make_pdf(info={"/Title": title}, xmp=None))["Title"] == title


def test_packet_without_a_leading_processing_instruction_is_unreadable():
    data = make_pdf(xmp=xmp_packet(DC_TITLE, header=False))
    with pytest.raises(PdfMetadataExtractorException, match="^Reached EOF in XMP header$"):
        kavita_metadata(data)


def test_packet_with_a_dtd_is_refused_unparsed():
    packet = V1_PACKET.replace(b"?><x:xmpmeta", b'?><!DOCTYPE x [<!ENTITY e "AFAR">]><x:xmpmeta')
    with pytest.raises(UnsafeXmpRefused):
        kavita_metadata(make_pdf(xmp=packet))


def test_first_alt_entry_wins_over_x_default():
    title = '<dc:title><rdf:Alt><rdf:li xml:lang="de">Fruehling</rdf:li><rdf:li xml:lang="x-default">Spring</rdf:li></rdf:Alt></dc:title>'
    assert kavita_metadata(make_pdf(info={}, xmp=xmp_packet(title)))["Title"] == "Fruehling"


def test_list_comma_only_follows_non_empty_text():
    """GetListFromXmlNode appends "," only when the builder is non-empty (L1403-1406)."""
    def author(*items: str):
        lis = "".join(f"<rdf:li>{item}</rdf:li>" for item in items)
        return kavita_metadata(make_pdf(info={}, xmp=xmp_packet(f"<dc:creator><rdf:Seq>{lis}</rdf:Seq></dc:creator>")))

    assert author("", "Ann")["Author"] == "Ann"
    assert author("Ann", "", "Bob")["Author"] == "Ann,,Bob"
    assert "Author" not in author("", "")  # all empty -> null -> never set


def test_inner_text_drops_whitespace_split_off_by_a_comment():
    """XmlDocument keeps comments as nodes, so "  " before one is its own (dropped) whitespace node."""
    title = "<dc:title><rdf:Alt><rdf:li>  <!-- c -->AFAR<?pi x?></rdf:li></rdf:Alt></dc:title>"
    assert kavita_metadata(make_pdf(info={}, xmp=xmp_packet(title)))["Title"] == "AFAR"


@pytest.mark.parametrize(("rating", "expected"), [("4.1", 4.099999904632568), ("+-Infinity", 0.0)])
def test_comicinfo_user_rating_is_a_float32_try_parse(rating, expected):
    data = make_pdf(info={}, xmp=xmp_packet(f"<calibre:rating>{rating}</calibre:rating>"))
    assert kavita_comicinfo(data)["UserRating"] == expected


# --- ComicInfo --------------------------------------------------------------


def test_comicinfo_series_falls_back_to_title():
    data = make_pdf(info={"/Title": TITLE}, xmp=xmp_packet(DC_TITLE, DC_DATE))
    info = kavita_comicinfo(data)
    assert info["Series"] == TITLE
    assert info["SeriesSort"] == TITLE
    assert info["Volume"] == ""
    assert (info["Year"], info["Month"], info["Day"]) == (2025, 3, 1)


def test_comicinfo_from_calibre_series():
    data = make_pdf(info={"/Title": TITLE, "/Author": " AFAR Editors "}, xmp=V1_PACKET)
    info = kavita_comicinfo(data)
    assert info["Title"] == TITLE
    assert info["Series"] == SERIES
    assert info["Volume"] == CALIBRE_INDEX
    assert info["Writer"] == "AFAR Editors"  # CleanAuthor trims
    assert (info["Year"], info["Month"], info["Day"]) == (2025, 3, 1)
    assert info["Genre"] == ""
    assert info["Tags"] == ""
    assert info["Isbn"] == ""


def test_comicinfo_from_info_dates_and_subject():
    data = make_pdf(info={"/Title": TITLE, "/Subject": "Travel", "/CreationDate": "D:20250301120000Z"}, xmp=None)
    info = kavita_comicinfo(data)
    assert (info["Year"], info["Month"], info["Day"]) == (2025, 3, 1)
    assert info["Genre"] == "Travel"
    assert info["Summary"] == ""  # Info /Subject feeds Genre, never Summary


def test_comicinfo_offset_dates_convert_to_server_time():
    """An explicit offset is converted to local (UTC) time and can change the day."""
    date = "<dc:date><rdf:Seq><rdf:li>2025-03-01T00:00:00+05:00</rdf:li></rdf:Seq></dc:date>"
    info = kavita_comicinfo(make_pdf(info={}, xmp=xmp_packet(DC_TITLE, date)))
    assert (info["Year"], info["Month"], info["Day"]) == (2025, 2, 28)


@pytest.mark.parametrize(
    ("created", "ymd"),
    [
        ("D:20250301230000-05'00'", (2025, 3, 2)),  # "zzz:" format, converted to UTC
        ("D:20250301230000-0500'", (2025, 3, 2)),  # zzz: the ":" between hh and mm is optional
        ("D:20250301230000-5'00'", (2025, 3, 2)),  # zzz: one hour digit is enough
        ("D:20250301230000-05'75'", (0, 0, 0)),  # zzz rejects minutes >= 60; no format matches
    ],
)
def test_comicinfo_pdf_date_offsets_follow_parse_exact_zzz(created, ymd):
    info = kavita_comicinfo(make_pdf(info={"/CreationDate": created}, xmp=None))
    assert (info["Year"], info["Month"], info["Day"]) == ymd


def test_comicinfo_respects_enable_metadata():
    data = make_pdf(xmp=V1_PACKET)
    assert kavita_comicinfo(data, enable_metadata=False) is None
    assert kavita_comicinfo(data)["Series"] == SERIES
