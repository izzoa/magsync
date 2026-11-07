"""Regenerate the committed PDF container fixtures (needs qpdf on PATH).

Usage: python tests/fixtures/pdf/make_fixtures.py tests/fixtures/pdf
"""
import struct
import subprocess
import sys
import zlib
from pathlib import Path

from pypdf import PdfWriter
from pypdf.generic import NameObject, StreamObject

out = Path(sys.argv[1])
JUNK_XMP = b'''<?xpacket begin="\xef\xbb\xbf" id="W5M0MpCehiHzreSzNTczkc9d"?><x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:pdf="http://ns.adobe.com/pdf/1.3/"><dc:title><rdf:Alt><rdf:li xml:lang="x-default">downmagaz.net</rdf:li></rdf:Alt></dc:title><dc:description><rdf:Alt><rdf:li xml:lang="x-default">downmagaz.net</rdf:li></rdf:Alt></dc:description><dc:creator><rdf:Bag><rdf:li>downmagaz.net</rdf:li></rdf:Bag></dc:creator><dc:subject><rdf:Bag><rdf:li>downmagaz.net</rdf:li></rdf:Bag></dc:subject><pdf:Keywords>downmagaz.net</pdf:Keywords></rdf:Description></rdf:RDF></x:xmpmeta><?xpacket end="w"?>'''

w = PdfWriter()
for _ in range(3):
    w.add_blank_page(200, 280)
w.add_metadata({"/Title": "downmagaz.net", "/Author": "downmagaz.net", "/Subject": "downmagaz.net", "/Keywords": "downmagaz.net"})
s = StreamObject(); s.set_data(JUNK_XMP)
s[NameObject("/Type")] = NameObject("/Metadata"); s[NameObject("/Subtype")] = NameObject("/XML")
w.root_object[NameObject("/Metadata")] = w._add_object(s)
base = out / "_base.pdf"
with open(base, "wb") as fh:
    w.write(fh)
subprocess.run(["qpdf", "--object-streams=generate", str(base), str(out / "objstm.pdf")], check=True)
subprocess.run(["qpdf", "--linearize", str(base), str(out / "linearized.pdf")], check=True)
base.unlink()

# Hybrid-reference file: classic xref table + trailer /XRefStm pointing at an
# xref stream that locates the catalog and Info inside an object stream.
parts, offsets = [], {}
def emit(b): parts.append(b)
def pos(): return sum(len(p) for p in parts)
emit(b"%PDF-1.5\n%\xe2\xe3\xcf\xd3\n")
offsets[2] = pos(); emit(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n")
offsets[3] = pos(); emit(b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 280] /Resources << >> >>\nendobj\n")
o1 = b"<< /Type /Catalog /Pages 2 0 R /Metadata 6 0 R >>"
o5 = b"<< /Title (downmagaz.net) /Author (downmagaz.net) /Subject (downmagaz.net) /Keywords (downmagaz.net) >>"
header = b"1 0 5 %d " % (len(o1) + 1)
objstm = header + o1 + b" " + o5
offsets[4] = pos(); emit(b"4 0 obj\n<< /Type /ObjStm /N 2 /First %d /Length %d >>\nstream\n" % (len(header), len(objstm)) + objstm + b"\nendstream\nendobj\n")
offsets[6] = pos(); emit(b"6 0 obj\n<< /Type /Metadata /Subtype /XML /Length %d >>\nstream\n" % len(JUNK_XMP) + JUNK_XMP + b"\nendstream\nendobj\n")
rows = struct.pack(">BIH", 2, 4, 0) + struct.pack(">BIH", 2, 4, 1)  # objects 1 and 5 in ObjStm 4
xs = zlib.compress(rows)
offsets[7] = pos(); emit(b"7 0 obj\n<< /Type /XRef /Size 8 /W [1 4 2] /Index [1 1 5 1] /Filter /FlateDecode /Length %d >>\nstream\n" % len(xs) + xs + b"\nendstream\nendobj\n")
xref_at = pos()
# Objects 1 and 5 live only in the xref stream: the table skips them.
table = b"xref\n0 1\n0000000000 65535 f \n2 3\n"
for n in (2, 3, 4):
    table += b"%010d 00000 n \n" % offsets[n]
table += b"6 2\n"
for n in (6, 7):
    table += b"%010d 00000 n \n" % offsets[n]
emit(table)
emit(b"trailer\n<< /Size 8 /Root 1 0 R /Info 5 0 R /XRefStm %d /ID [<0123456789abcdef0123456789abcdef> <0123456789abcdef0123456789abcdef>] >>\nstartxref\n%d\n%%%%EOF\n" % (offsets[7], xref_at))
(out / "hybrid.pdf").write_bytes(b"".join(parts))
