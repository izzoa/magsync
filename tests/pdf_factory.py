"""Small real PDFs for stamping tests: junk metadata, revisions, encryption."""

from __future__ import annotations

import io
import re
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, StreamObject

JUNK = "downmagaz.net"
JUNK_XMP = (
    '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>'
    '<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
    '<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:pdf="http://ns.adobe.com/pdf/1.3/">'
    f'<dc:title><rdf:Alt><rdf:li xml:lang="x-default">{JUNK}</rdf:li></rdf:Alt></dc:title>'
    f'<dc:description><rdf:Alt><rdf:li xml:lang="x-default">{JUNK}</rdf:li></rdf:Alt></dc:description>'
    f"<dc:creator><rdf:Bag><rdf:li>{JUNK}</rdf:li></rdf:Bag></dc:creator>"
    f"<dc:subject><rdf:Bag><rdf:li>{JUNK}</rdf:li></rdf:Bag></dc:subject>"
    f"<pdf:Keywords>{JUNK}</pdf:Keywords>"
    "</rdf:Description></rdf:RDF></x:xmpmeta><?xpacket end=\"w\"?>"
).encode()


def make_pdf(*, pages=2, info=None, xmp=JUNK_XMP, encrypt=False) -> bytes:
    """A classic-trailer PDF carrying junk Info and a junk XMP packet."""
    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(200, 280)
    writer.add_metadata(info if info is not None else {"/Title": JUNK, "/Author": JUNK, "/Subject": JUNK, "/Keywords": JUNK})
    if xmp is not None:
        stream = StreamObject()
        stream.set_data(xmp)
        stream[NameObject("/Type")] = NameObject("/Metadata")
        stream[NameObject("/Subtype")] = NameObject("/XML")
        writer.root_object[NameObject("/Metadata")] = writer._add_object(stream)
    if encrypt:
        writer.encrypt(user_password="", owner_password="owner", algorithm="RC4-128")
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def revise(data: bytes, info: dict) -> bytes:
    """Append another tool's incremental revision (pypdf writes an xref stream)."""
    writer = PdfWriter(PdfReader(io.BytesIO(data)), incremental=True)
    writer.add_metadata(info)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


FIXTURES = Path(__file__).parent / "fixtures" / "pdf"


def containers() -> dict[str, bytes]:
    """Every source container shape stamping must handle, by name.

    ``classic`` ends in a cross-reference table; ``xref-stream`` and
    ``several-revisions`` end in a stream appended over a table;
    ``object-stream-catalog`` is a pure xref-stream file whose catalog and Info
    dict sit in an object stream; ``linearized`` and ``hybrid-reference`` end in
    a table (the hybrid one with ``/XRefStm`` locating a compressed catalog and
    Info dict).
    """
    base = make_pdf()
    once = revise(base, {"/Producer": "some editor"})
    return {
        "classic": base,
        "xref-stream": once,
        "several-revisions": revise(once, {"/Creator": "another editor"}),
        "object-stream-catalog": (FIXTURES / "objstm.pdf").read_bytes(),
        "linearized": (FIXTURES / "linearized.pdf").read_bytes(),
        "hybrid-reference": (FIXTURES / "hybrid.pdf").read_bytes(),
        "no-xmp": make_pdf(xmp=None),
    }


def last_section_form(data: bytes) -> str:
    """``table`` or ``stream``: the form of the section the last ``startxref`` names."""
    offset = int(re.findall(rb"startxref\s+(\d+)", data)[-1])
    if data[offset:offset + 4] == b"xref":
        return "table"
    header = re.match(rb"\s*\d+\s+\d+\s+obj\s*<<", data[offset:offset + 64])
    if header and b"/XRef" in data[offset:offset + 512].split(b"stream", 1)[0]:
        return "stream"
    raise ValueError("startxref names neither a table nor an xref stream")


# --- hand-built sources for edge cases pypdf will not write ---------------------------

PAGE_TEXT = b"BT /F1 24 Tf 72 700 Td (Hello magazine) Tj ET"


def _objects(*, content_number: int, creation_date_indirect: bool = False) -> dict[int, bytes]:
    info = b"<< /Title (downmagaz.net) /Author (downmagaz.net) /Producer (junk) "
    info += b"/CreationDate 6 0 R >>" if creation_date_indirect else b">>"
    objects = {
        1: b"<< /Type /Catalog /Pages 2 0 R >>",
        2: b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        3: b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> "
           b"/Contents %d 0 R >>" % content_number,
        4: b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        5: info,
        6: b"(D:20200101000000Z)" if creation_date_indirect else b"<< /Dummy true >>",
    }
    objects[content_number] = b"<< /Length %d >>\nstream\n" % len(PAGE_TEXT) + PAGE_TEXT + b"\nendstream"
    return objects


def hand_built_table(*, declared_size: int = 8, creation_date_indirect: bool = False) -> bytes:
    """Objects 1-7 (7 is the page's content stream) and a classic table whose
    trailer claims ``/Size declared_size`` (8 is right; less understates it)."""
    objects = _objects(content_number=7, creation_date_indirect=creation_date_indirect)
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = {}
    for number in sorted(objects):
        offsets[number] = out.tell()
        out.write(b"%d 0 obj\n" % number + objects[number] + b"\nendobj\n")
    xref = out.tell()
    out.write(b"xref\n0 8\n0000000000 65535 f \n")
    for number in range(1, 8):
        out.write(b"%010d 00000 n \n" % offsets[number])
    out.write(b"trailer\n<< /Size %d /Root 1 0 R /Info 5 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (declared_size, xref))
    return out.getvalue()


def hand_built_stream(*, declared_size: int = 8, xref_number: int = 7, list_itself: bool = True) -> bytes:
    """Objects 1-6 (6 is the content stream) and an uncompressed xref stream
    numbered ``xref_number``. ``list_itself=False`` with ``xref_number ==
    declared_size`` is the macOS Quartz shape: the stream sits at /Size and its
    own /Index leaves it out."""
    objects = _objects(content_number=6)
    out = io.BytesIO()
    out.write(b"%PDF-1.5\n%\xe2\xe3\xcf\xd3\n")
    offsets = {}
    for number in sorted(objects):
        offsets[number] = out.tell()
        out.write(b"%d 0 obj\n" % number + objects[number] + b"\nendobj\n")
    xref = out.tell()
    offsets[xref_number] = xref
    listed = sorted(offsets) if list_itself else sorted(n for n in offsets if n != xref_number)
    rows = b"\x00" + (0).to_bytes(4, "big") + b"\xff\xff"
    index = [0, 1]
    for number in listed:
        rows += b"\x01" + offsets[number].to_bytes(4, "big") + b"\x00\x00"
    for number in listed:  # one-entry runs keep the hand-made /Index simple
        index += [number, 1]
    out.write(
        b"%d 0 obj\n<< /Type /XRef /Size %d /W [1 4 2] /Index [%s] /Root 1 0 R /Info 5 0 R /Length %d >>\nstream\n"
        % (xref_number, declared_size, b" ".join(b"%d" % n for n in index), len(rows))
    )
    out.write(rows + b"\nendstream\nendobj\nstartxref\n%d\n%%%%EOF\n" % xref)
    return out.getvalue()


def with_stream_revision_new_info(data: bytes) -> bytes:
    """Append an uncompressed xref-stream revision that moves the Info dict to a
    new object number, so older and newer revisions name different /Info."""
    reader = PdfReader(io.BytesIO(data))
    size = int(reader.trailer["/Size"])
    prev = int(re.findall(rb"startxref\s+(\d+)", data)[-1])
    out = io.BytesIO()
    out.write(data if data.endswith(b"\n") else data + b"\n")
    info_at = out.tell()
    out.write(b"%d 0 obj\n<< /Title (downmagaz.net) /Author (another junk) >>\nendobj\n" % size)
    xref = out.tell()
    rows = b"\x01" + info_at.to_bytes(4, "big") + b"\x00\x00" + b"\x01" + xref.to_bytes(4, "big") + b"\x00\x00"
    out.write(
        b"%d 0 obj\n<< /Type /XRef /Size %d /W [1 4 2] /Index [%d 2] /Root 1 0 R /Info %d 0 R /Prev %d /Length %d >>"
        b"\nstream\n" % (size + 1, size + 2, size, size, prev, len(rows))
    )
    out.write(rows + b"\nendstream\nendobj\nstartxref\n%d\n%%%%EOF\n" % xref)
    return out.getvalue()
