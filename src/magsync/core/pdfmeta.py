"""Embedded PDF metadata: what magsync stamps into a downloaded magazine.

Sources ship PDFs whose embedded metadata is junk (``downmagaz.net`` in every
field). A *stamp* replaces it without rewriting the file: exactly one PDF
incremental update is appended to the untouched payload, holding a fresh XMP
packet, a filtered Info dict and a catalog that points at the new packet.
Readers that follow the newest trailer (BookOrbit's pdf-lib, poppler) see the
stamp; the original bytes stay an exact, verifiable prefix, so the stamp can
always be undone or redone and a library's partial-MD5 file identity survives.

Library apps read different vocabularies, so the packet (stamp version 2)
says the same thing several ways, one ``rdf:Description`` each: Dublin Core;
BookOrbit's ``bookorbit:`` series (BookOrbit matches literal prefixes and reads
series data nowhere else); calibre's ``calibre:series`` (read by calibre,
Kavita, Booklore, Grimmory, Readest, polyreader); PRISM's magazine fields; and
a ``magsync:`` marker recording the original payload's hash, size and the
stamp version, so a file's provenance can be recovered from the file alone.
The Info dict carries the title and the issue date (``/CreationDate``, read
by Booklore and Stump). The update itself is laid out by the source's
cross-reference chain: a stream (catalog and Info redefined under their own
numbers) only for a uniform all-stream chain, otherwise a table with new
object numbers, so readers that resolve revisions unusually (Kavita) still
reach it. Version 1 (magsync 0.10.x) is kept, frozen, only to rebuild
and so recognize files stamped before.
"""

from __future__ import annotations

import calendar
import codecs
import datetime
import hashlib
import io
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.dom import minidom
from xml.etree import ElementTree
from xml.sax.saxutils import escape

from pypdf import PdfReader, PdfWriter
from pypdf.generic import (
    ArrayObject,
    DictionaryObject,
    IndirectObject,
    NameObject,
    NumberObject,
    StreamObject,
    TextStringObject,
    read_object,
)

from magsync.core.matching import canonicalize_for_match
from magsync.core.models import StampSpec
from magsync.core.organizer import normalize_title, parse_date, strip_source_decorations

STAMP_VERSION = 2
# pypdf parses the whole document in memory; larger payloads stay unstamped.
STAMP_SIZE_LIMIT = 512 * 1024 * 1024
# Export integrity treats "%%EOF within the last 1024 bytes" as completeness.
EOF_WINDOW = 1024

RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
DC_NS = "http://purl.org/dc/elements/1.1/"
BOOKORBIT_NS = "https://bookorbit.app/metadata/1.0/"
# calibre's own series vocabulary, the one most library apps read from a PDF.
CALIBRE_NS = "http://calibre-ebook.com/xmp-namespace"
CALIBRE_SI_NS = "http://calibre-ebook.com/xmp-namespace-series-index"
# PRISM's de-facto XMP namespace (calibre, Kavita, ExifTool, Tellico, pikepdf).
PRISM_NS = "http://prismstandard.org/namespaces/basic/2.0/"
MAGSYNC_NS = "urn:magsync:metadata:1"
_XMP_PACKET_ID = "W5M0MpCehiHzreSzNTczkc9d"
# Junk-bearing Info keys never carried into the stamped Info dict.
_DROPPED_INFO_KEYS = ("/Title", "/Author", "/Subject", "/Keywords")
# From version 2 these are written back empty instead of left out.
_BLANKED_INFO_KEYS = ("/Author", "/Subject", "/Keywords")
_XML_INVALID = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f￾￿]")

# Skip reasons (stable; also used as report kinds by repair-metadata).
SKIP_ENCRYPTED = "encrypted"
SKIP_TRUNCATED = "truncated"
SKIP_OVERSIZED = "oversized"
SKIP_UNPARSEABLE = "unparseable"
SKIP_FAILED = "stamp_failed"


# --- what gets stamped ---------------------------------------------------------


def _clean_text(value: str) -> str:
    return _XML_INVALID.sub("", value).strip()


def stamp_spec_for(issue: Mapping[str, Any]) -> StampSpec | None:
    """Derive the stamp for an issue row (or any mapping with the same keys).

    Returns ``None`` when the issue has no usable title.
    """
    title = _clean_text(strip_source_decorations(issue.get("title") or ""))
    if not title:
        return None
    parsed = parse_date(title, issue.get("page_url") or "")
    # A range's first day is the issue date ("March 28th-April 3rd" -> March 28).
    year, month, day = parsed.start or (parsed.year, parsed.month, parsed.day)
    if year is None:
        year, month, day = issue.get("year"), issue.get("month"), None
    if month is not None and not 1 <= month <= 12:
        month = None
    # A day only means something with its month; February 30 is no day at all.
    if month is None or year is None or (day is not None and day > calendar.monthrange(year, month)[1]):
        day = None
    genres = tuple(
        dict.fromkeys(
            _clean_text(part)
            for part in (issue.get("genre") or "").split(",")
            if _clean_text(part)
        )
    )
    return StampSpec(
        title=title,
        series_name=_clean_text(normalize_title(title)),
        year=year,
        month=month,
        day=day,
        genres=genres,
        cover_display=_cover_display(title),
    )


# The separators that end a magazine name in an issue title, in the order the
# organizer tries them when it names the file (``organizer._extract_issue_detail``).
_TITLE_SEPARATORS = (" – ", " - ", " | ", ": ")


def _cover_display(title: str) -> str | None:
    """``prism:coverDisplayDate``: the title's text after the magazine name, year kept."""
    for separator in _TITLE_SEPARATORS:
        if separator in title:
            return _clean_text(title.split(separator, 1)[1]) or None
    return None


def comparison_key(spec: StampSpec) -> dict[str, str | None]:
    """What decides whether two stamps describe the same issue.

    Title and series name are folded the way subscription matching folds them
    (accents, apostrophe and dash variants, case, whitespace): source spelling
    is not a disagreement. Date and series index are compared as they are, so
    rows naming different issues still disagree. Genres are not part of it;
    they are combined across rows instead. Only for deciding: never embedded.
    """
    return {
        "title": canonicalize_for_match(spec.title),
        "series name": canonicalize_for_match(spec.series_name),
        "date": date_text(spec),
        "series index": series_index(spec),
    }


def date_text(spec: StampSpec) -> str | None:
    """``dc:date``: full date (day 01 when unknown), year alone, or nothing."""
    if spec.year is None:
        return None
    if spec.month is None:
        return f"{spec.year:04d}"
    return f"{spec.year:04d}-{spec.month:02d}-{spec.day or 1:02d}"


def series_index(spec: StampSpec) -> str | None:
    """``bookorbit:seriesIndex``: YYYYMMDD, YYYY0000 for year-only, or nothing."""
    if spec.year is None:
        return None
    if spec.month is None:
        return f"{spec.year:04d}0000"
    return f"{spec.year:04d}{spec.month:02d}{spec.day or 1:02d}"


def ordinal_index(spec: StampSpec) -> str | None:
    """``calibreSI:series_index``: the ISO 8601 ordinal date YYYYDDD, YYYY000 for
    year-only, or nothing.

    At most 9999366, so it stays exact where readers keep the index as a 32-bit
    float (exact to 2**24) and under calibre's 10,000,000 editor cap.
    """
    if spec.year is None:
        return None
    if spec.month is None:
        return f"{spec.year:04d}000"
    day_of_year = datetime.date(spec.year, spec.month, spec.day or 1).timetuple().tm_yday
    return f"{spec.year:04d}{day_of_year:03d}"


def cover_date(spec: StampSpec) -> str | None:
    """``prism:coverDate``: the issue date at the precision the title states."""
    if spec.year is None:
        return None
    if spec.month is None:
        return f"{spec.year:04d}"
    if spec.day is None:
        return f"{spec.year:04d}-{spec.month:02d}"
    return f"{spec.year:04d}-{spec.month:02d}-{spec.day:02d}"


def creation_date(spec: StampSpec) -> str | None:
    """Info ``/CreationDate``: the issue date at 12:00 UTC, or nothing.

    Noon keeps the calendar day in every zone from UTC-11 to UTC+11 for readers
    that convert it to a local date; a year-only issue takes 1 January, since a
    bare year parses as midnight and would move west of UTC into the year before.
    """
    if spec.year is None:
        return None
    month = spec.month or 1
    day = (spec.day or 1) if spec.month else 1
    return f"D:{spec.year:04d}{month:02d}{day:02d}120000Z"


def fingerprint(spec: StampSpec, version: int | None = None) -> str:
    """Identify the stamped metadata (and stamp version) compactly.

    ``version`` defaults to the current stamp version. A version-1 fingerprint
    is exactly what magsync 0.10.x recorded, so its records stay comparable.
    """
    version = STAMP_VERSION if version is None else version
    fields: dict[str, Any] = {
        "version": version,
        "title": spec.title,
        "series": spec.series_name,
        "date": date_text(spec),
        "index": series_index(spec),
        "genres": list(spec.genres),
    }
    if version >= 2:
        fields |= {
            "ordinal": ordinal_index(spec),
            "cover date": cover_date(spec),
            "cover display": spec.cover_display,
            "created": creation_date(spec),
        }
    canonical = json.dumps(fields, sort_keys=True, ensure_ascii=False)
    return f"{version}:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def stamp_record(fingerprint_value: str, size: int, mtime_ns: int, inode: int) -> str:
    """``downloads.metadata_stamp``: the fingerprint plus the verified file state."""
    return f"{fingerprint_value}:{size}:{mtime_ns}:{inode}"


def parse_stamp_record(value: str | None) -> tuple[str, int, int, int] | None:
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 5:
        return None
    try:
        return f"{parts[0]}:{parts[1]}", int(parts[2]), int(parts[3]), int(parts[4])
    except ValueError:
        return None


# --- the XMP packet -------------------------------------------------------------


def _element(name: str, text: str) -> str:
    return f"   <{name}>{escape(text)}</{name}>\n"


def _container(name: str, kind: str, items: list[str], *, default_lang: bool = False) -> str:
    lang = ' xml:lang="x-default"' if default_lang else ""
    lis = "".join(f"<rdf:li{lang}>{escape(item)}</rdf:li>" for item in items)
    return f"   <{name}><rdf:{kind}>{lis}</rdf:{kind}></{name}>\n"


def build_xmp(
    spec: StampSpec, *, page_count: int, source_sha256: str, source_size: int, version: int | None = None
) -> bytes:
    """A fresh packet in the given stamp version (default: the current one).

    No ``<?xml?>`` declaration after ``<?xpacket?>`` (strict parsers reject
    it), no timestamps, no padding: identical input gives identical bytes.
    """
    version = STAMP_VERSION if version is None else version
    if version == 1:
        return _build_xmp_v1(spec, page_count=page_count, source_sha256=source_sha256, source_size=source_size)
    if version == 2:
        return _build_xmp_v2(spec, page_count=page_count, source_sha256=source_sha256, source_size=source_size)
    raise ValueError(f"unknown stamp version {version}")


def _packet(descriptions: list[tuple[dict[str, str], str]]) -> bytes:
    """Wrap ``(namespaces, body)`` pairs, one top-level ``rdf:Description`` each."""
    blocks = ""
    for namespaces, body in descriptions:
        declared = "".join(f'\n    xmlns:{prefix}="{uri}"' for prefix, uri in namespaces.items())
        blocks += f'  <rdf:Description rdf:about=""{declared}>\n{body}  </rdf:Description>\n'
    packet = (
        f'<?xpacket begin="\ufeff" id="{_XMP_PACKET_ID}"?>\n'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">\n'
        f' <rdf:RDF xmlns:rdf="{RDF_NS}">\n'
        f"{blocks}"
        " </rdf:RDF>\n"
        "</x:xmpmeta>\n"
        '<?xpacket end="w"?>'
    )
    return packet.encode("utf-8")


def _build_xmp_v2(spec: StampSpec, *, page_count: int, source_sha256: str, source_size: int) -> bytes:
    """Version 2: one ``rdf:Description`` per vocabulary, in a fixed order.

    Dublin Core alone (strict parsers drop a whole Description that holds a
    namespace they do not know), then BookOrbit's, calibre's series, PRISM's
    magazine fields and the magsync marker. ``dc:description`` is present but
    empty: a reader that applies older revisions first (Kavita) only drops the
    source's junk description for a value that is present. Author and keywords
    stay absent (the Info dict blanks them).
    """
    dublin = _element("dc:format", "application/pdf")
    dublin += _container("dc:title", "Alt", [spec.title], default_lang=True)
    dublin += '   <dc:description><rdf:Alt><rdf:li xml:lang="x-default"></rdf:li></rdf:Alt></dc:description>\n'
    date = date_text(spec)
    if date:
        dublin += _container("dc:date", "Seq", [date])
    if spec.genres:
        dublin += _container("dc:subject", "Bag", list(spec.genres))

    bookorbit = _element("bookorbit:seriesName", spec.series_name)
    index = series_index(spec)
    if index:
        bookorbit += _element("bookorbit:seriesIndex", index)
    bookorbit += _element("bookorbit:pageCount", str(page_count))

    ordinal = ordinal_index(spec)
    calibre = (
        '   <calibre:series rdf:parseType="Resource">'
        f"<rdf:value>{escape(spec.series_name)}</rdf:value>"
        + (f"<calibreSI:series_index>{ordinal}</calibreSI:series_index>" if ordinal else "")
        + "</calibre:series>\n"
    )

    prism = _element("prism:publicationName", spec.series_name)
    prism += _element("prism:aggregationType", "magazine")
    covered = cover_date(spec)
    if covered:
        prism += _element("prism:coverDate", covered)
    if spec.cover_display:
        prism += _element("prism:coverDisplayDate", spec.cover_display)

    marker = _element("magsync:sourceSha256", source_sha256)
    marker += _element("magsync:sourceSize", str(source_size))
    marker += _element("magsync:stampVersion", "2")
    return _packet(
        [
            ({"dc": DC_NS}, dublin),
            ({"bookorbit": BOOKORBIT_NS}, bookorbit),
            ({"calibre": CALIBRE_NS, "calibreSI": CALIBRE_SI_NS}, calibre),
            ({"prism": PRISM_NS}, prism),
            ({"magsync": MAGSYNC_NS}, marker),
        ]
    )


def _build_xmp_v1(spec: StampSpec, *, page_count: int, source_sha256: str, source_size: int) -> bytes:
    """Version 1 (magsync 0.10.x), frozen: one ``rdf:Description`` holding
    Dublin Core, BookOrbit and the marker. Only rebuilt to recognize old stamps.
    """
    body = _element("dc:format", "application/pdf")
    body += _container("dc:title", "Alt", [spec.title], default_lang=True)
    date = date_text(spec)
    if date:
        body += _container("dc:date", "Seq", [date])
    if spec.genres:
        body += _container("dc:subject", "Bag", list(spec.genres))
    body += _element("bookorbit:seriesName", spec.series_name)
    index = series_index(spec)
    if index:
        body += _element("bookorbit:seriesIndex", index)
    body += _element("bookorbit:pageCount", str(page_count))
    body += _element("magsync:sourceSha256", source_sha256)
    body += _element("magsync:sourceSize", str(source_size))
    body += _element("magsync:stampVersion", "1")
    packet = (
        f'<?xpacket begin="﻿" id="{_XMP_PACKET_ID}"?>\n'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">\n'
        ' <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">\n'
        '  <rdf:Description rdf:about=""\n'
        '    xmlns:dc="http://purl.org/dc/elements/1.1/"\n'
        f'    xmlns:bookorbit="{BOOKORBIT_NS}"\n'
        f'    xmlns:magsync="{MAGSYNC_NS}">\n'
        f"{body}"
        "  </rdf:Description>\n"
        " </rdf:RDF>\n"
        "</x:xmpmeta>\n"
        '<?xpacket end="w"?>'
    )
    return packet.encode("utf-8")


def read_xmp_fields(xmp: bytes) -> dict[str, list[str]]:
    """Read a packet the way BookOrbit does: literal prefixed names.

    Every ``rdf:Description`` is merged and the first occurrence of a property
    wins; attribute-form properties are ignored. Values are returned as lists
    (container items, or the single text value). ``rdf:Alt`` puts the
    ``x-default`` item first. A packet that declares a DTD is refused.
    """
    if b"<!DOCTYPE" in xmp or b"<!ENTITY" in xmp:
        return {}
    try:
        document = minidom.parseString(xmp)
    except Exception:
        return {}
    fields: dict[str, list[str]] = {}
    for description in document.getElementsByTagName("rdf:Description"):
        for child in description.childNodes:
            if child.nodeType != child.ELEMENT_NODE or child.tagName in fields:
                continue
            fields[child.tagName] = _property_values(child)
    return fields


def _text_of(node) -> str:
    return "".join(
        part.data for part in node.childNodes if part.nodeType in (part.TEXT_NODE, part.CDATA_SECTION_NODE)
    ).strip()


def _property_values(node) -> list[str]:
    for container in node.childNodes:
        if container.nodeType == container.ELEMENT_NODE and container.tagName in (
            "rdf:Alt",
            "rdf:Seq",
            "rdf:Bag",
        ):
            items = [li for li in container.childNodes if li.nodeType == li.ELEMENT_NODE and li.tagName == "rdf:li"]
            if container.tagName == "rdf:Alt":
                items.sort(key=lambda li: li.getAttribute("xml:lang") != "x-default")
            return [_text_of(li) for li in items if _text_of(li)]
    text = _text_of(node)
    return [text] if text else []


def _qualified(namespace: str, local: str) -> str:
    return f"{{{namespace}}}{local}"


def read_xmp_namespaced(xmp: bytes) -> dict[str, Any] | None:
    """Read a packet the way namespace-aware readers do (calibre, Kavita,
    Booklore, polyreader): names are namespace URI plus local name.

    Returns ``{"descriptions": [...], "properties": {...}, "repeated": [...]}``:
    the sorted namespaces each top-level ``rdf:Description`` uses, each
    property by ``{uri}local`` (a list for ``rdf:Alt``/``Seq``/``Bag`` items,
    ``x-default`` first and empty items kept; a dict of child values for a
    resource-typed property; otherwise its text), and the names written more
    than once. ``None`` for a packet that is not XML or declares a DTD.
    """
    if b"<!DOCTYPE" in xmp or b"<!ENTITY" in xmp:
        return None
    try:
        root = ElementTree.fromstring(xmp)
    except ElementTree.ParseError:
        return None
    descriptions: list[tuple[str, ...]] = []
    properties: dict[str, Any] = {}
    repeated: list[str] = []
    for rdf in root.iter(_qualified(RDF_NS, "RDF")):
        for description in rdf:
            if description.tag != _qualified(RDF_NS, "Description"):
                continue
            namespaces = set()
            for element in description:
                namespaces.add(element.tag[1:].partition("}")[0])
                if element.tag in properties:
                    repeated.append(element.tag)
                    continue
                properties[element.tag] = _namespaced_value(element)
            descriptions.append(tuple(sorted(namespaces)))
    return {"descriptions": descriptions, "properties": properties, "repeated": repeated}


def _namespaced_value(element: ElementTree.Element) -> Any:
    for kind in ("Alt", "Seq", "Bag"):
        container = element.find(_qualified(RDF_NS, kind))
        if container is not None:
            items = [li for li in container if li.tag == _qualified(RDF_NS, "li")]
            if kind == "Alt":
                items.sort(key=lambda li: li.get("{http://www.w3.org/XML/1998/namespace}lang") != "x-default")
            return [(li.text or "").strip() for li in items]
    if element.get(_qualified(RDF_NS, "parseType")) == "Resource":
        return {child.tag: (child.text or "").strip() for child in element}
    return (element.text or "").strip()


def expected_namespaced(spec: StampSpec, *, page_count: int, source_sha256: str, source_size: int) -> dict[str, Any]:
    """What a version-2 packet must read back as, derived from ``spec`` itself
    (not from the builder), so the self-check also checks the builder."""
    dc = {
        _qualified(DC_NS, "format"): "application/pdf",
        _qualified(DC_NS, "title"): [spec.title],
        _qualified(DC_NS, "description"): [""],
    }
    if date_text(spec):
        dc[_qualified(DC_NS, "date")] = [date_text(spec)]
    if spec.genres:
        dc[_qualified(DC_NS, "subject")] = list(spec.genres)
    bookorbit = {_qualified(BOOKORBIT_NS, "seriesName"): spec.series_name}
    if series_index(spec):
        bookorbit[_qualified(BOOKORBIT_NS, "seriesIndex")] = series_index(spec)
    bookorbit[_qualified(BOOKORBIT_NS, "pageCount")] = str(page_count)
    series = {_qualified(RDF_NS, "value"): spec.series_name}
    if ordinal_index(spec):
        series[_qualified(CALIBRE_SI_NS, "series_index")] = ordinal_index(spec)
    prism = {
        _qualified(PRISM_NS, "publicationName"): spec.series_name,
        _qualified(PRISM_NS, "aggregationType"): "magazine",
    }
    if cover_date(spec):
        prism[_qualified(PRISM_NS, "coverDate")] = cover_date(spec)
    if spec.cover_display:
        prism[_qualified(PRISM_NS, "coverDisplayDate")] = spec.cover_display
    marker = {
        _qualified(MAGSYNC_NS, "sourceSha256"): source_sha256,
        _qualified(MAGSYNC_NS, "sourceSize"): str(source_size),
        _qualified(MAGSYNC_NS, "stampVersion"): "2",
    }
    return {
        "descriptions": [(DC_NS,), (BOOKORBIT_NS,), (CALIBRE_NS,), (PRISM_NS,), (MAGSYNC_NS,)],
        "properties": {**dc, **bookorbit, _qualified(CALIBRE_NS, "series"): series, **prism, **marker},
        "repeated": [],
    }


# --- PDF inspection -------------------------------------------------------------


@dataclass(frozen=True)
class Marker:
    source_sha256: str
    source_size: int
    version: int


@dataclass(frozen=True)
class PdfFacts:
    encrypted: bool
    eof_tail: bool
    marker: Marker | None
    # Library-app metadata (Info /Creator "bookorbit" or bookorbit: properties)
    # in a newest revision that carries no magsync marker.
    bookorbit_traces: bool
    page_count: int | None


def has_eof_tail(data: bytes) -> bool:
    return b"%%EOF" in data[-EOF_WINDOW:]


def _reader(data: bytes) -> PdfReader:
    return PdfReader(io.BytesIO(data))


def _newest_xmp(reader: PdfReader) -> bytes | None:
    root = reader.trailer.get("/Root")
    if root is None:
        return None
    metadata = root.get_object().get("/Metadata")
    if metadata is None:
        return None
    stream = metadata.get_object()
    return stream.get_data() if isinstance(stream, StreamObject) else None


def _marker_from(fields: Mapping[str, list[str]]) -> Marker | None:
    try:
        sha = fields["magsync:sourceSha256"][0]
        size = int(fields["magsync:sourceSize"][0])
        version = int(fields.get("magsync:stampVersion", ["0"])[0])
    except (KeyError, IndexError, ValueError):
        return None
    if not re.fullmatch(r"[0-9a-f]{64}", sha) or size <= 0:
        return None
    return Marker(sha, size, version)


def inspect_pdf(data: bytes) -> PdfFacts | None:
    """What the newest revision says about this file, or ``None`` if unparseable."""
    try:
        reader = _reader(data)
        if reader.is_encrypted:
            return PdfFacts(True, has_eof_tail(data), None, False, None)
        xmp = _newest_xmp(reader)
        fields = read_xmp_fields(xmp) if xmp else {}
        info = reader.metadata or {}
        creator = str(info.get("/Creator") or "").strip().lower()
        marker = _marker_from(fields)
        # Library metadata in a packet magsync did not write: another tool edited it.
        traces = marker is None and (
            creator == "bookorbit" or any(key.startswith("bookorbit:") for key in fields)
        )
        return PdfFacts(False, has_eof_tail(data), marker, traces, len(reader.pages))
    except Exception:
        return None


# --- building and verifying a stamp ---------------------------------------------


@dataclass(frozen=True)
class StampOutcome:
    """Bytes to append to the payload, or why the payload stays unstamped."""

    appended: bytes | None = None
    reason: str | None = None
    # For SKIP_FAILED: a short, unsanitized summary of what went wrong.
    error: str | None = None


# pypdf's own incremental clone refuses object ids beyond this; a /Size far
# past it is a corrupt trailer, and padding up to it would exhaust memory.
_MAX_OBJECT_ID = 1_000_000


def _xref_is_sound(source: bytes) -> bool:
    """True when the file's cross-reference data is right as written.

    A lenient reader silently repairs a wrong ``startxref`` or wrong object
    offsets; an appended revision would still point (``/Prev``) at the broken
    section and strict readers such as poppler would lose the page tree. So
    the source is read strictly, without repair, and every in-use offset must
    land on its own object header.
    """
    try:
        strict = PdfReader(io.BytesIO(source), strict=True)
        start = strict._startxref
    except Exception:
        return False
    if not isinstance(start, int) or not 0 < start < len(source):
        return False
    if not re.match(rb"xref|\d+\s+\d+\s+obj\b", source[start:start + 32]):
        return False
    for generation, entries in strict.xref.items():
        for idnum, offset in entries.items():
            if not isinstance(offset, int) or offset <= 0:
                continue
            header = rb"\s*%d\s+%d\s+obj\b" % (idnum, generation)
            if not re.match(header, source[offset:offset + 48]):
                return False
    return True


def _title_object(title: str) -> TextStringObject:
    value = TextStringObject(title)
    if not title.isascii():
        # UTF-16BE with BOM: unambiguous in every reader (no PDFDocEncoding guesswork).
        value.autodetect_utf16 = True
        value.autodetect_pdfdocencoding = False
        value.utf16_bom = codecs.BOM_UTF16_BE
    return value


def stamp_payload(
    source: bytes, spec: StampSpec, *, source_sha256: str | None = None, version: int | None = None
) -> StampOutcome:
    """Build the incremental update that stamps ``spec`` onto ``source``.

    ``version`` (default: the current one) selects the stamp format; older
    versions are only rebuilt to recognize files magsync stamped before.
    Never raises: anything that prevents a verified stamp is a skip reason,
    and the caller places the payload unstamped.
    """
    version = STAMP_VERSION if version is None else version
    if len(source) > STAMP_SIZE_LIMIT:
        return StampOutcome(reason=SKIP_OVERSIZED)
    if not has_eof_tail(source):
        return StampOutcome(reason=SKIP_TRUNCATED)
    # The update must start on its own line: appended straight onto a final
    # comment line ("%%EOF\n%x" without EOL), pdf-lib reads the new catalog's
    # header as part of the comment. The original stays an exact prefix.
    base = source if source[-1:] in (b"\n", b"\r") else source + b"\n"
    try:
        reader = _reader(base)
        if reader.is_encrypted:
            return StampOutcome(reason=SKIP_ENCRYPTED)
        size = int(reader.trailer.get("/Size", 0))
        if not 0 < size <= _MAX_OBJECT_ID or not _xref_is_sound(source):
            return StampOutcome(reason=SKIP_UNPARSEABLE)
        page_count = len(reader.pages)
        source_has_id = "/ID" in reader.trailer
        layout = _source_layout(source, reader)
    except Exception:
        return StampOutcome(reason=SKIP_UNPARSEABLE)
    if version not in (1, 2):
        return StampOutcome(reason=SKIP_FAILED, error=f"unknown stamp version {version}")
    source_sha256 = source_sha256 or hashlib.sha256(source).hexdigest()
    try:
        write = _write_v1 if version == 1 else _write_v2
        stamped = write(base, reader, layout, spec, page_count=page_count, source_sha256=source_sha256, source=source)
    except Exception as exc:
        return StampOutcome(reason=SKIP_FAILED, error=f"{type(exc).__name__}: {exc}"[:200])
    if not verify_stamp(
        stamped,
        source,
        spec,
        source_sha256=source_sha256,
        page_count=page_count,
        require_id=source_has_id,
        version=version,
    ):
        return StampOutcome(reason=SKIP_FAILED, error="the stamped file failed its self-check")
    return StampOutcome(appended=stamped[len(source):])


@dataclass(frozen=True)
class _Layout:
    """How to append to the source, from its own cross-reference chain."""

    form: str  # "stream" for a uniform all-stream chain (``_source_layout``), else "table"
    prev: int  # the source's last ``startxref``: the update's ``/Prev``
    size: int  # the newest trailer's ``/Size`` (the frozen version-1 writer pads to it)
    first_free: int  # the first object number nothing in the source uses
    root: tuple[int, int]  # the catalog's object number and generation
    info: tuple[int, int] | None  # the Info dict's, when the trailer references one indirectly


def _last_startxref(data: bytes) -> int:
    found = re.findall(rb"startxref\s+(\d+)", data[-EOF_WINDOW * 4:])
    if not found:
        raise ValueError("no startxref")
    return int(found[-1])


def _section_form(data: bytes, offset: int) -> str:
    if data[offset:offset + 4] == b"xref":
        return "table"
    if re.match(rb"\s*\d+\s+\d+\s+obj\b", data[offset:offset + 32]):
        return "stream"
    raise ValueError("startxref names no cross-reference section")


# Sections a cross-reference chain may have before it is treated as damaged.
_MAX_CHAIN = 1000


@dataclass(frozen=True)
class _Section:
    form: str  # "table" or "stream"
    size: int | None  # its trailer's /Size
    root: tuple[int, int] | None
    info: tuple[int, int] | None
    number: int | None  # an xref stream's own object number


def _reference(value: Any) -> tuple[int, int] | None:
    return (value.idnum, value.generation) if isinstance(value, IndirectObject) else None


def _chain(data: bytes, start: int, reader: PdfReader) -> list[_Section] | None:
    """Every cross-reference section reachable from ``start`` through ``/Prev``
    (and a hybrid table's ``/XRefStm``), newest first; ``None`` when a section
    cannot be read or the chain is implausibly long."""
    sections: list[_Section] = []
    seen: set[int] = set()
    pending: list[tuple[int, str]] = [(start, "/Prev")]
    try:
        while pending:
            offset, via = pending.pop(0)
            if offset in seen:
                if via == "/Prev":
                    return None  # a /Prev loop: damaged
                continue  # a hybrid's /XRefStm may name a section /Prev also reaches
            if len(seen) >= _MAX_CHAIN:
                return None
            seen.add(offset)
            form = _section_form(data, offset)
            trailer = _trailer_at(data, offset, reader)
            if trailer is None:
                return None
            number = None
            if form == "stream":
                header = re.match(rb"\s*(\d+)\s+\d+\s+obj", data[offset:offset + 64])
                number = int(header.group(1)) if header else None
            size = trailer.get("/Size")
            sections.append(
                _Section(
                    form=form,
                    size=int(size) if size is not None else None,
                    root=_reference(trailer.raw_get("/Root") if "/Root" in trailer else None),
                    info=_reference(trailer.raw_get("/Info") if "/Info" in trailer else None),
                    number=number,
                )
            )
            for key in ("/XRefStm", "/Prev"):
                if key in trailer:
                    pending.append((int(trailer[key]), key))
    except (ValueError, TypeError, AttributeError):
        return None
    return sections


def _source_layout(source: bytes, reader: PdfReader) -> _Layout:
    """Decision B (design.md, Decision 5): a stream update only for a chain made
    of streams alone whose every revision names the same catalog and Info dict
    (or none). Kavita lets an older table's entry win, and applies older stream
    revisions after newer ones, so anything else needs a table with numbers no
    older section defines. Anything unreadable also gets the table."""
    prev = _last_startxref(source)
    root = _reference(reader.trailer.raw_get("/Root"))
    info = _reference(reader.trailer.raw_get("/Info") if "/Info" in reader.trailer else None)
    if root is None:
        raise TypeError("the catalog is not an indirect object")
    _section_form(source, prev)  # the last startxref must name a section
    sections = _chain(source, prev, reader) or []
    streams_only = bool(sections) and all(
        section.form == "stream" and section.root in (None, root) and section.info in (None, info)
        for section in sections
    )
    # New numbers must clear every number the source uses, whatever /Size
    # claims: an understated /Size, or an xref stream numbered at /Size and
    # left out of its own /Index (macOS Quartz), would otherwise be reused.
    size = int(reader.trailer["/Size"])
    used = [idnum for entries in reader.xref.values() for idnum in entries] + list(reader.xref_objStm)
    first_free = max(
        [size, max(used, default=0) + 1]
        + [section.size for section in sections if section.size is not None]
        + [section.number + 1 for section in sections if section.number is not None]
    )
    return _Layout(
        form="stream" if streams_only else "table",
        prev=prev,
        size=size,
        first_free=first_free,
        root=root,
        info=info,
    )


def _direct(value: Any) -> Any:
    """A referenced Info value inlined, so readers that cannot follow the
    reference (Kavita, into an object stream) still read it. Streams stay
    references."""
    if isinstance(value, IndirectObject):
        resolved = value.get_object()
        if resolved is not None and not isinstance(resolved, StreamObject):
            return resolved
    return value


def _stamped_info(reader: PdfReader, spec: StampSpec, *, version: int) -> DictionaryObject:
    """The source's Info entries minus the junk carriers, plus the stamp's."""
    source_info = _source_info(reader)
    info = DictionaryObject()
    for key, value in (source_info.items() if isinstance(source_info, DictionaryObject) else ()):
        if key in _DROPPED_INFO_KEYS:
            continue
        if key == "/Creator" and str(_direct(value)).strip().lower() == "bookorbit":
            continue  # never claim to be the library app's own write-back
        info[NameObject(key)] = _direct(value)
    info[NameObject("/Title")] = _title_object(spec.title)
    if version >= 2:
        created = creation_date(spec)
        if created:
            info[NameObject("/CreationDate")] = TextStringObject(created)
        # Present but empty: a reader that applies older revisions first
        # (Kavita) keeps the source's junk unless a later revision sets the key.
        for key in _BLANKED_INFO_KEYS:
            info[NameObject(key)] = TextStringObject("")
    return info


def _source_info(reader: PdfReader) -> DictionaryObject | None:
    """The Info dict the trailer names, resolved (``get`` leaves a reference)."""
    info = reader.trailer["/Info"] if "/Info" in reader.trailer else None  # noqa: SIM401 - get() would not resolve
    return info if isinstance(info, DictionaryObject) else None


def _metadata_stream(xmp: bytes) -> StreamObject:
    stream = StreamObject()
    stream.set_data(xmp)
    stream[NameObject("/Type")] = NameObject("/Metadata")
    stream[NameObject("/Subtype")] = NameObject("/XML")
    return stream


def _write_v1(
    base: bytes, reader: PdfReader, layout: _Layout, spec: StampSpec, *, page_count: int, source_sha256: str,
    source: bytes,
) -> bytes:
    """Version 1 (magsync 0.10.x), frozen: pypdf's incremental writer, which
    always appends a cross-reference stream, a redefined catalog, and a new
    Info dict and packet. Only rebuilt to recognize old stamps."""
    writer = PdfWriter(reader, incremental=True)
    # pypdf numbers new objects after the ones it cloned, which can reuse
    # a number an earlier revision spent on its own xref stream. Readers
    # then disagree about that object, so start new objects at /Size.
    while len(writer._objects) < layout.size - 1:
        writer._objects.append(None)
    info = DictionaryObject()
    for key, value in (writer._info or {}).items():
        if key in _DROPPED_INFO_KEYS:
            continue
        if key == "/Creator" and str(value).strip().lower() == "bookorbit":
            continue
        info[NameObject(key)] = value
    info[NameObject("/Title")] = _title_object(spec.title)
    # Always a fresh object: pypdf only writes /Info into the appended
    # trailer when the Info object is new or changed, and pdf-lib drops
    # Info entirely when the newest xref stream lacks it.
    writer._info_obj = writer._add_object(info)
    xmp = build_xmp(spec, page_count=page_count, source_sha256=source_sha256, source_size=len(source), version=1)
    writer.root_object[NameObject("/Metadata")] = writer._add_object(_metadata_stream(xmp))
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def _write_v2(
    base: bytes, reader: PdfReader, layout: _Layout, spec: StampSpec, *, page_count: int, source_sha256: str,
    source: bytes,
) -> bytes:
    """Version 2: magsync lays the update out itself, by the source's chain.

    When the chain has a table anywhere: a table, and the catalog, Info dict
    and packet under object numbers no older section defines (a reader that
    lets an older table's entry win, as Kavita does, then still reaches them).
    When the chain is streams only: an uncompressed stream, the catalog and
    indirect Info dict redefined under their own numbers (a reader that keeps
    the newest offset and collapses revisions naming the same ``/Root`` and
    ``/Info``, as Kavita does, then reads only the stamp). Objects are
    written uncompressed, so readers that skip compressed entries find them.
    """
    next_number = layout.first_free

    def allocate() -> tuple[int, int]:
        nonlocal next_number
        next_number += 1
        return next_number - 1, 0

    if layout.form == "table":
        root_at, info_at, metadata_at = allocate(), allocate(), allocate()
    else:
        root_at = layout.root
        info_at = layout.info or allocate()
        metadata_at = allocate()
    catalog = DictionaryObject()
    for key, value in reader.trailer["/Root"].items():
        catalog[NameObject(key)] = value
    catalog[NameObject("/Metadata")] = IndirectObject(metadata_at[0], metadata_at[1], reader)
    xmp = build_xmp(spec, page_count=page_count, source_sha256=source_sha256, source_size=len(source), version=2)
    objects = [
        (root_at, catalog),
        (info_at, _stamped_info(reader, spec, version=2)),
        (metadata_at, _metadata_stream(xmp)),
    ]
    out = io.BytesIO()
    out.write(base)
    offsets: dict[int, tuple[int, int]] = {}
    for (number, generation), obj in objects:
        offsets[number] = (out.tell(), generation)
        out.write(b"%d %d obj\n" % (number, generation))
        obj.write_to_stream(out)
        out.write(b"\nendobj\n")
    trailer = DictionaryObject()
    trailer[NameObject("/Root")] = IndirectObject(root_at[0], root_at[1], reader)
    trailer[NameObject("/Info")] = IndirectObject(info_at[0], info_at[1], reader)
    if "/ID" in reader.trailer:
        trailer[NameObject("/ID")] = reader.trailer.raw_get("/ID")
    trailer[NameObject("/Prev")] = NumberObject(layout.prev)
    if layout.form == "table":
        section = out.tell()
        trailer[NameObject("/Size")] = NumberObject(next_number)
        # The free-list head first, as a full table has it: readers that
        # check a table starts at object 0 (pypdf's strict mode) then accept it.
        out.write(b"xref\n0 1\n0000000000 65535 f\r\n")
        for first, run in _runs(sorted(offsets)):
            out.write(b"%d %d\n" % (first, len(run)))
            for number in run:
                offset, generation = offsets[number]
                out.write(b"%010d %05d n\r\n" % (offset, generation))
        out.write(b"trailer\n")
        trailer.write_to_stream(out)
        out.write(b"\n")
    else:
        xref_number = allocate()[0]
        section = out.tell()
        offsets[xref_number] = (section, 0)
        width = max(1, (max(offset for offset, _ in offsets.values()).bit_length() + 7) // 8)
        rows = b""
        index = ArrayObject()
        for first, run in _runs(sorted(offsets)):
            index += [NumberObject(first), NumberObject(len(run))]
            for number in run:
                offset, generation = offsets[number]
                rows += b"\x01" + offset.to_bytes(width, "big") + generation.to_bytes(2, "big")
        stream = StreamObject()
        stream.set_data(rows)
        stream[NameObject("/Type")] = NameObject("/XRef")
        stream[NameObject("/Size")] = NumberObject(next_number)
        stream[NameObject("/W")] = ArrayObject([NumberObject(1), NumberObject(width), NumberObject(2)])
        stream[NameObject("/Index")] = index
        for key, value in trailer.items():
            stream[NameObject(key)] = value
        out.write(b"%d 0 obj\n" % xref_number)
        stream.write_to_stream(out)
        out.write(b"\nendobj\n")
    out.write(b"startxref\n%d\n%%%%EOF\n" % section)
    return out.getvalue()


def _runs(numbers: list[int]) -> list[tuple[int, list[int]]]:
    """Consecutive runs of sorted object numbers, as cross-reference subsections."""
    runs: list[tuple[int, list[int]]] = []
    for number in numbers:
        if runs and runs[-1][1][-1] == number - 1:
            runs[-1][1].append(number)
        else:
            runs.append((number, [number]))
    return runs


def _newest_trailer(data: bytes, reader: PdfReader) -> DictionaryObject | None:
    """The trailer dictionary at the file's last ``startxref`` (no merging)."""
    position = data.rfind(b"startxref")
    if position < 0:
        return None
    match = re.match(rb"startxref\s+(\d+)", data[position:position + 64])
    if not match:
        return None
    return _trailer_at(data, int(match.group(1)), reader)


def _trailer_at(data: bytes, offset: int, reader: PdfReader) -> DictionaryObject | None:
    """The trailer dictionary of the cross-reference section at ``offset``: a
    table's ``trailer``, or an xref stream's own dictionary."""
    stream = io.BytesIO(data)
    if data[offset:offset + 4] == b"xref":
        trailer_at = data.find(b"trailer", offset)
        if trailer_at < 0:
            return None
        stream.seek(trailer_at + len(b"trailer"))
    else:
        header = re.match(rb"\s*\d+\s+\d+\s+obj", data[offset:offset + 64])
        if not header:
            return None
        stream.seek(offset + header.end())
    while True:
        char = stream.read(1)
        if not char:
            return None
        if char not in b" \t\r\n":
            break
    stream.seek(-1, io.SEEK_CUR)
    found = read_object(stream, reader)
    return found if isinstance(found, DictionaryObject) else None


def _defined_after(data: bytes, start: int, reference: Any) -> bool:
    """Whether ``reference`` gets a definition after ``start`` that pdf-lib sees.

    A header on a line that already holds a ``%`` is part of a comment to
    pdf-lib, which scans the file linearly, so it does not count.
    """
    if not isinstance(reference, IndirectObject):
        return False
    pattern = rb"(?<!\d)%d\s+%d\s+obj" % (reference.idnum, reference.generation)
    for match in re.finditer(pattern, data[start:]):
        position = start + match.start()
        line_start = max(data.rfind(b"\n", 0, position), data.rfind(b"\r", 0, position)) + 1
        if b"%" not in data[line_start:position]:
            return True
    return False


def _points_at_xref(data: bytes, offset: Any) -> bool:
    return isinstance(offset, int) and 0 < offset < len(data) and re.match(
        rb"xref|\d+\s+\d+\s+obj\b", data[offset:offset + 32]
    ) is not None


def verify_stamp(
    stamped: bytes,
    source: bytes,
    spec: StampSpec,
    *,
    source_sha256: str,
    page_count: int,
    require_id: bool,
    version: int | None = None,
) -> bool:
    """Check the stamped bytes themselves, as the newest-revision readers will.

    The original is an exact prefix; ``%%EOF`` ends the file; the newest
    trailer carries ``/Root``, ``/Info`` (and ``/ID`` when the source had
    one); the catalog, Info dict and packet are defined in the appended
    section (pdf-lib keeps the last definition in byte order); and every
    stamped field reads back as built. Version 2 also reads the packet
    namespace-aware against values derived from ``spec`` itself, checks every
    stamped Info entry, and checks that the update is laid out as the source
    calls for (``_write_v2``), re-reading the source rather than trusting the
    writer's plan.
    """
    version = STAMP_VERSION if version is None else version
    try:
        if not stamped.startswith(source) or len(stamped) <= len(source) or not has_eof_tail(stamped):
            return False
        reader = _reader(stamped)
        trailer = _newest_trailer(stamped, reader)
        if trailer is None or "/Root" not in trailer or "/Info" not in trailer:
            return False
        if require_id and "/ID" not in trailer:
            return False
        if not _points_at_xref(stamped, trailer.get("/Prev")):
            return False
        # raw_get: subscripting a pypdf dictionary resolves the reference.
        root_ref, info_ref = trailer.raw_get("/Root"), trailer.raw_get("/Info")
        root = root_ref.get_object()
        metadata_ref = root.raw_get("/Metadata")
        start = len(source)
        if not all(_defined_after(stamped, start, ref) for ref in (root_ref, info_ref, metadata_ref)):
            return False
        info = info_ref.get_object()
        if str(info.get("/Title")) != spec.title:
            return False
        packet = metadata_ref.get_object().get_data()
        expected = expected_fields(
            spec, page_count=page_count, source_sha256=source_sha256, source_size=len(source), version=version
        )
        if read_xmp_fields(packet) != expected:
            return False
        if version == 1:
            return True
        if len(reader.pages) != page_count:
            return False
        source_reader = _reader(source)
        return (
            read_xmp_namespaced(packet)
            == expected_namespaced(spec, page_count=page_count, source_sha256=source_sha256, source_size=len(source))
            and _info_is_stamped(info, spec, source_info=_source_info(source_reader))
            and _laid_out_like(stamped, _source_layout(source, source_reader), root_ref, info_ref, metadata_ref)
        )
    except Exception:
        return False


def _info_is_stamped(info: DictionaryObject, spec: StampSpec, *, source_info: Any) -> bool:
    if any(info.get(key) != "" for key in _BLANKED_INFO_KEYS):
        return False
    created = creation_date(spec)
    if created is None:
        kept = source_info.get("/CreationDate") if isinstance(source_info, DictionaryObject) else None
        return _direct(info.get("/CreationDate")) == _direct(kept)
    return info.get("/CreationDate") == created


def _laid_out_like(stamped: bytes, layout: _Layout, root_ref: Any, info_ref: Any, metadata_ref: Any) -> bool:
    """The appended section has the form the source's chain calls for, and
    the stamped objects the numbers that form calls for (``_write_v2``)."""
    if _section_form(stamped, _last_startxref(stamped)) != layout.form:
        return False
    numbers = [(ref.idnum, ref.generation) for ref in (root_ref, info_ref, metadata_ref)]
    if layout.form == "table":
        return all(number >= layout.first_free for number, _generation in numbers)
    if numbers[0] != layout.root or numbers[2][0] < layout.first_free:
        return False
    return numbers[1] == layout.info if layout.info else numbers[1][0] >= layout.first_free


def expected_fields(
    spec: StampSpec, *, page_count: int, source_sha256: str, source_size: int, version: int | None = None
) -> dict[str, list[str]]:
    """The exact property map a stamped packet reads back as, BookOrbit's way."""
    return read_xmp_fields(
        build_xmp(spec, page_count=page_count, source_sha256=source_sha256, source_size=source_size, version=version)
    )


# --- provenance of an existing file ------------------------------------------------


@dataclass(frozen=True)
class Provenance:
    """Who owns a file's bytes, and where its original payload ends.

    ``kind``: ``recorded`` (the whole file is what magsync recorded),
    ``healed`` (magsync's stamp of the original under the current metadata, in
    the version its marker names — a replace whose index update was lost), ``legacy`` (an unmarked file with
    no recorded hash: the whole file is the original), ``edited`` (another
    tool wrote library metadata), or ``modified`` (provenance unproven; the
    file must not be touched).
    """

    kind: str
    current_sha256: str
    original_size: int | None = None
    original_sha256: str | None = None
    # The stamp version the file's marker names, for a marked file.
    stamp_version: int | None = None


def _prefix_matches(data: bytes, sha: str | None, size: int | None) -> bool:
    return bool(sha) and size is not None and 0 < size <= len(data) and hashlib.sha256(data[:size]).hexdigest() == sha


def resolve_original(
    data: bytes,
    *,
    trusted_sha256: str | None,
    legacy_sha256: str | None,
    recorded_source: tuple[str | None, int | None] | None,
    spec: StampSpec | None,
    facts: PdfFacts | None = None,
) -> Provenance:
    """Prove which prefix of ``data`` is the original payload, or refuse.

    ``trusted_sha256`` is a hash magsync recorded when it wrote (or fully
    verified) the file, the only kind that may authorize discarding bytes
    after the original. ``legacy_sha256`` is any other recorded hash (a
    pre-stamping download, or a file adopted as found): it may confirm that
    an unmarked file is unchanged, and stamping such a file only appends.
    A verified prefix alone never authorizes truncation; neither does a hash
    magsync merely observed.
    """
    current = hashlib.sha256(data).hexdigest()
    facts = facts if facts is not None else inspect_pdf(data)
    marker = facts.marker if facts else None
    if marker is None and facts is not None and facts.bookorbit_traces:
        return Provenance("edited", current)
    if trusted_sha256 and current == trusted_sha256:
        if marker is not None:
            if recorded_source and _prefix_matches(data, *recorded_source):
                return Provenance("recorded", current, recorded_source[1], recorded_source[0], marker.version)
            if _prefix_matches(data, marker.source_sha256, marker.source_size):
                return Provenance("recorded", current, marker.source_size, marker.source_sha256, marker.version)
            return Provenance("modified", current)
        if recorded_source and recorded_source[1] not in (None, len(data)):
            return Provenance("modified", current)  # an unmarked file is its own original
        return Provenance("recorded", current, len(data), current)
    if marker is not None:
        if spec is not None and _prefix_matches(data, marker.source_sha256, marker.source_size):
            original = data[: marker.source_size]
            # Rebuilt in the version the marker names: every version magsync
            # ever wrote stays recognizable. An unknown version never matches.
            rebuilt = stamp_payload(original, spec, source_sha256=marker.source_sha256, version=marker.version)
            if rebuilt.appended is not None and original + rebuilt.appended == data:
                return Provenance("healed", current, marker.source_size, marker.source_sha256, marker.version)
        return Provenance("modified", current)
    if legacy_sha256 is None or current == legacy_sha256:
        return Provenance("legacy", current, len(data), current)
    return Provenance("modified", current)


def file_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size
