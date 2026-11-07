"""Small real PDFs for stamping tests: junk metadata, revisions, encryption."""

from __future__ import annotations

import io

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
