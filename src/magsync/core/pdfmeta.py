"""Embedded PDF metadata: what magsync stamps into a downloaded magazine.

Sources ship PDFs whose embedded metadata is junk (``downmagaz.net`` in every
field). A *stamp* replaces it without rewriting the file: exactly one PDF
incremental update is appended to the untouched payload, holding a fresh XMP
packet, a filtered Info dict and a catalog that points at the new packet.
Readers that follow the newest trailer (BookOrbit's pdf-lib, poppler) see the
stamp; the original bytes stay an exact, verifiable prefix, so the stamp can
always be undone or redone and a library's partial-MD5 file identity survives.

The packet uses BookOrbit's dialect: it matches literal prefixes (``dc:``,
``bookorbit:``) and ignores namespace URIs and attribute-form properties, and
it reads series data only from ``bookorbit:seriesName``/``seriesIndex``. A
``magsync:`` marker records the original payload's hash and size so a file's
provenance can be recovered from the file alone.
"""

from __future__ import annotations

import calendar
import codecs
import hashlib
import io
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.dom import minidom
from xml.sax.saxutils import escape

from pypdf import PdfReader, PdfWriter
from pypdf.generic import (
    DictionaryObject,
    IndirectObject,
    NameObject,
    StreamObject,
    TextStringObject,
    read_object,
)

from magsync.core.models import StampSpec
from magsync.core.organizer import normalize_title, parse_date, strip_source_decorations

STAMP_VERSION = 1
# pypdf parses the whole document in memory; larger payloads stay unstamped.
STAMP_SIZE_LIMIT = 512 * 1024 * 1024
# Export integrity treats "%%EOF within the last 1024 bytes" as completeness.
EOF_WINDOW = 1024

BOOKORBIT_NS = "https://bookorbit.app/metadata/1.0/"
MAGSYNC_NS = "urn:magsync:metadata:1"
_XMP_PACKET_ID = "W5M0MpCehiHzreSzNTczkc9d"
# Junk-bearing Info keys never carried into the stamped Info dict.
_DROPPED_INFO_KEYS = ("/Title", "/Author", "/Subject", "/Keywords")
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
    )


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


def fingerprint(spec: StampSpec) -> str:
    """Identify the stamped metadata (and stamp version) compactly."""
    canonical = json.dumps(
        {
            "version": STAMP_VERSION,
            "title": spec.title,
            "series": spec.series_name,
            "date": date_text(spec),
            "index": series_index(spec),
            "genres": list(spec.genres),
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return f"{STAMP_VERSION}:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


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


def build_xmp(spec: StampSpec, *, page_count: int, source_sha256: str, source_size: int) -> bytes:
    """A fresh packet: one ``rdf:Description``, child elements, fixed order.

    No ``<?xml?>`` declaration after ``<?xpacket?>`` (strict parsers reject
    it), no timestamps, no padding: identical input gives identical bytes.
    Author, description and keywords are deliberately absent.
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
    body += _element("magsync:stampVersion", str(STAMP_VERSION))
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


def stamp_payload(source: bytes, spec: StampSpec, *, source_sha256: str | None = None) -> StampOutcome:
    """Build the incremental update that stamps ``spec`` onto ``source``.

    Never raises: anything that prevents a verified stamp is a skip reason,
    and the caller places the payload unstamped.
    """
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
    except Exception:
        return StampOutcome(reason=SKIP_UNPARSEABLE)
    source_sha256 = source_sha256 or hashlib.sha256(source).hexdigest()
    try:
        writer = PdfWriter(reader, incremental=True)
        # pypdf numbers new objects after the ones it cloned, which can reuse
        # a number an earlier revision spent on its own xref stream. Readers
        # then disagree about that object, so start new objects at /Size.
        while len(writer._objects) < size - 1:
            writer._objects.append(None)
        info = DictionaryObject()
        for key, value in (writer._info or {}).items():
            if key in _DROPPED_INFO_KEYS:
                continue
            if key == "/Creator" and str(value).strip().lower() == "bookorbit":
                continue  # never claim to be the library app's own write-back
            info[NameObject(key)] = value
        info[NameObject("/Title")] = _title_object(spec.title)
        # Always a fresh object: pypdf only writes /Info into the appended
        # trailer when the Info object is new or changed, and pdf-lib drops
        # Info entirely when the newest xref stream lacks it.
        writer._info_obj = writer._add_object(info)
        xmp = build_xmp(spec, page_count=page_count, source_sha256=source_sha256, source_size=len(source))
        stream = StreamObject()
        stream.set_data(xmp)
        stream[NameObject("/Type")] = NameObject("/Metadata")
        stream[NameObject("/Subtype")] = NameObject("/XML")
        writer.root_object[NameObject("/Metadata")] = writer._add_object(stream)
        out = io.BytesIO()
        writer.write(out)
        stamped = out.getvalue()
    except Exception as exc:
        return StampOutcome(reason=SKIP_FAILED, error=f"{type(exc).__name__}: {exc}"[:200])
    if not verify_stamp(
        stamped, source, spec, source_sha256=source_sha256, page_count=page_count, require_id=source_has_id
    ):
        return StampOutcome(reason=SKIP_FAILED, error="the stamped file failed its self-check")
    return StampOutcome(appended=stamped[len(source):])


def _newest_trailer(data: bytes, reader: PdfReader) -> DictionaryObject | None:
    """The trailer dictionary at the file's last ``startxref`` (no merging)."""
    position = data.rfind(b"startxref")
    if position < 0:
        return None
    match = re.match(rb"startxref\s+(\d+)", data[position:position + 64])
    if not match:
        return None
    offset = int(match.group(1))
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
) -> bool:
    """Check the stamped bytes themselves, as the newest-revision readers will.

    The original is an exact prefix; ``%%EOF`` ends the file; the newest
    trailer carries ``/Root``, ``/Info`` (and ``/ID`` when the source had
    one); the catalog, Info dict and packet are defined in the appended
    section (pdf-lib keeps the last definition in byte order); and every
    stamped field reads back as built.
    """
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
        if str(info_ref.get_object().get("/Title")) != spec.title:
            return False
        fields = read_xmp_fields(metadata_ref.get_object().get_data())
        return fields == expected_fields(spec, page_count=page_count, source_sha256=source_sha256, source_size=len(source))
    except Exception:
        return False


def expected_fields(spec: StampSpec, *, page_count: int, source_sha256: str, source_size: int) -> dict[str, list[str]]:
    """The exact property map a stamped packet reads back as."""
    return read_xmp_fields(
        build_xmp(spec, page_count=page_count, source_sha256=source_sha256, source_size=source_size)
    )


# --- provenance of an existing file ------------------------------------------------


@dataclass(frozen=True)
class Provenance:
    """Who owns a file's bytes, and where its original payload ends.

    ``kind``: ``recorded`` (the whole file is what magsync recorded),
    ``healed`` (magsync's stamp of the original under the current metadata —
    a replace whose index update was lost), ``legacy`` (an unmarked file with
    no recorded hash: the whole file is the original), ``edited`` (another
    tool wrote library metadata), or ``modified`` (provenance unproven; the
    file must not be touched).
    """

    kind: str
    current_sha256: str
    original_size: int | None = None
    original_sha256: str | None = None


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
                return Provenance("recorded", current, recorded_source[1], recorded_source[0])
            if _prefix_matches(data, marker.source_sha256, marker.source_size):
                return Provenance("recorded", current, marker.source_size, marker.source_sha256)
            return Provenance("modified", current)
        if recorded_source and recorded_source[1] not in (None, len(data)):
            return Provenance("modified", current)  # an unmarked file is its own original
        return Provenance("recorded", current, len(data), current)
    if marker is not None:
        if spec is not None and _prefix_matches(data, marker.source_sha256, marker.source_size):
            original = data[: marker.source_size]
            rebuilt = stamp_payload(original, spec, source_sha256=marker.source_sha256)
            if rebuilt.appended is not None and original + rebuilt.appended == data:
                return Provenance("healed", current, marker.source_size, marker.source_sha256)
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
