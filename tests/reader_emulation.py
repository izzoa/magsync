"""Test oracles: what calibre, Booklore, Grimmory and a strict xmpbox reader see.

Independent Python ports of four library apps' PDF metadata readers, so tests
can check magsync's stamps against the code that will actually read them. Like
``bookorbit_emulation`` this module never imports ``magsync``. Quirks and bugs
are ported on purpose; each carries a comment naming the upstream line.

Upstream sources (fetched with ``gh api`` and read, never built or run):

* ``calibre_view`` / ``calibre_prefers_info``: kovidgoyal/calibre @
  fd2df1f93d90208867f87d70c3ef898e18e336e6.
  ``src/calibre/ebooks/metadata/xmp.py`` (blob 481b6f40d9808aa5f407915a36059c3169420ab9):
  ``parse_xmp_packet``, the XPath helpers, ``read_series``, ``read_xmp_identifers``,
  ``metadata_from_xmp_packet``, ``consolidate_metadata``;
  ``src/calibre/utils/date.py`` (blob 6af7eb7c67dae9c18c532f10464df192218dfb93): ``parse_date``;
  ``src/calibre/ebooks/metadata/__init__.py`` (blob 4d01765db98c1964be97d62bf0962cff06f88dc0):
  ``string_to_authors``, ``check_isbn``, ``check_doi``;
  ``src/calibre/ebooks/metadata/pdf.py`` (blob fad45fefdfd892306d6b1feb85fac19449c26948):
  the Info dict comes from ``pdfinfo -isodates``, the packet from ``pdfinfo -meta``.
* ``booklore_view``: booklore-app/booklore @ 01b3407528a6ce00d064dbb9aa3f69b72d82641b,
  ``booklore-api/src/main/java/org/booklore/service/metadata/extractor/PdfMetadataExtractor.java``
  (blob 888744eb279c5cea0c71e10ff60369fa8b8df612), ``util/SecureXmlUtils.java``
  (blob 45e3f48a556f3345f1640348d12b48d5e9bf8c94). PDFBox 3.0.8 (pinned in
  ``booklore-api/build.gradle``; apache/pdfbox tag 3.0.8 = commit
  9286e47d89d6877005c9d2d0f2fd38793a62519a): ``util/DateConverter.java``
  (blob 0b8c9279e95711079a10af7acbd17d71a7b4dced), ``cos/COSString.java``
  (blob eb7e5c09eec735cd06792e232178f0efdae943e2), ``cos/PDFDocEncoding.java``
  (blob 55f0b977138199930286ac5b0684e68014cc3332), ``cos/COSDictionary.java``
  (blob a519083b9ebb0b2e5ec0ba684741d3af1ac07cfc).
* ``grimmory_view``: grimmory-tools/grimmory @ 37ccdf4dfb364b5bf1517383399cda58fcb50a3b,
  ``backend/src/main/java/org/booklore/service/metadata/extractor/PdfMetadataExtractor.java``
  (blob c41484f2333de0c15264d303f3eb5dbae5cd3193), ``util/SecureXmlUtils.java``
  (blob c5967ed8a8f3ba6abc613aab6321f47b39fd92a2). PDFium4j 1.3.0 (pinned in
  ``backend/build.gradle.kts`` and ``gradle.lockfile``; grimmory-tools/PDFium4j tag v1.3.0 =
  commit 56824fd22d4aaa674490a95aa39e0dbb4c9f3c4e): ``XmpMetadataParser.java``
  (blob 178cec485385cd74ce4a352e50208f2b23f84e43), ``model/XmpMetadata.java``
  (blob 37168b4fcb9f0f644910912264dfe05cb4b9ab06), ``internal/PdfDocumentXmp.java``
  (blob e1f7360f21103bdc2c56031437d8a4b9e8f9028a), ``PdfDocument.java``
  (blob d4b71efb9c51df9a004580e68c987936ed8f5e77), ``internal/PdfDocumentMetadata.java``
  (blob d7cca6273298b9a175169fb390199c0f290c17c1), ``shim/src/pdfium4j_read.cpp``
  (blob 858a6960d8f386e34dd9a2272729f7ef8436cb05).
* ``strict_xmp_view``: JabRef/jabref @ 1f722ca0134a06b889aee92cd5e38fdc9d78087b,
  ``jablib/src/main/java/org/jabref/logic/xmp/XmpUtilReader.java``
  (blob 145d559f00414700ed7dde9cf5db34674e638f2c), ``XmpUtilShared.java``
  (blob 0ea2720c1b81e1096ffd564d08c72093bf5fba88), ``DublinCoreExtractor.java``
  (blob 562db12669b9240392bc04fed5351d1279d6078f), ``model/schema/DublinCoreSchemaCustom.java``
  (blob b0987fd8bb489fb328c6fe49d38178b66d44b758). JabRef pins PDFBox/xmpbox 3.0.8 in
  ``versions/build.gradle.kts`` (blob 76a37a3a940399ca8c7bfa832a056f444b620342):
  ``xmpbox/.../xml/DomXmpParser.java`` (blob 44e944b4da0f2acd869b749230ac28db50976ed9),
  ``xml/DomHelper.java`` (blob 05ada11259c5f9bffc89d95ae98249ea0a2e5fe3),
  ``type/TypeMapping.java`` (blob d59d99ada61b4012c4da9f885f2f3f35ad933841),
  ``xmpbox/.../DateConverter.java`` (blob 3d3b1d9fb6101f80097fafe95627faf6b51783a9),
  ``schema/XMPSchema.java`` (blob c3b3d79aa540951e9e72ce2d1379962a1127b17f),
  ``cos/COSStream.java`` (blob e773bbbde0392254725f9b4f5c49e2b68b2638d1) and the twelve
  schema classes TypeMapping registers (blobs next to their tables below).

Where an input reaches upstream code that is not ported, the oracles raise
``OracleLimitation`` instead of guessing. All XML goes through
``xml.etree.ElementTree``; a DOCTYPE or ENTITY declaration is refused outright.
"""

from __future__ import annotations

import io
import math
import os
import re
import struct
import unicodedata
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from fractions import Fraction
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

from pypdf import PdfReader
from pypdf.generic import (
    ByteStringObject,
    IndirectObject,
    NameObject,
    StreamObject,
    TextStringObject,
)

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
DC = "http://purl.org/dc/elements/1.1/"
XML_NS = "http://www.w3.org/XML/1998/namespace"
XMP = "http://ns.adobe.com/xap/1.0/"
XMP_IDQ = "http://ns.adobe.com/xmp/Identifier/qual/1.0/"
CALIBRE = "http://calibre-ebook.com/xmp-namespace"
CALIBRE_SI = "http://calibre-ebook.com/xmp-namespace-series-index"
BOOKLORE = "http://booklore.org/metadata/1.0/"
PDFA_ID = "http://www.aiim.org/pdfa/ns/id/"


class OracleLimitation(NotImplementedError):
    """The input reaches upstream code this module deliberately does not port."""


class XmlRefused(ValueError):
    """A DOCTYPE or ENTITY declaration: every oracle here refuses DTDs."""


class GrimmoryAmbiguity(ValueError):
    """Two custom namespaces carry one local name with different values.

    PDFium4j copies its custom fields with ``Map.copyOf`` (XmpMetadata.java:75),
    whose iteration order is unspecified, so ``findField`` could return either.
    """


# --------------------------------------------------------------------------- XML
_AMBIGUOUS = object()


def _split(name):
    """``{ns}local`` -> (ns, local); an unqualified name has ns None."""
    if name[:1] == "{":
        ns, _, local = name[1:].partition("}")
        return ns, local
    return None, name


def _infer_prefix(ns, scope, *, element):
    # ElementTree drops prefixes; DOM readers key on them, so recover the prefix
    # from the declarations in scope. Two prefixes bound to one URI is ambiguous.
    if ns is None:
        return None
    if ns == XML_NS:
        return "xml"
    candidates = sorted(p for p, uri in scope.items() if uri == ns and (element or p))
    if len(candidates) == 1:
        return candidates[0] or None
    return _AMBIGUOUS


class _Target:
    """ElementTree builder that also keeps what the DOM-based readers look at."""

    def __init__(self):
        self._tb = ET.TreeBuilder(insert_comments=True, insert_pis=True)
        self._scope = [{}]
        self._pending = {}
        self._stack = []
        self.doc_nodes = []  # document-level ("pi", target, data) and ("root", element)
        self.prefix = {}
        self.attr_prefix = {}
        self.parent = {}
        self.keep = []  # keeps elements alive so id() keys stay unique

    def start_ns(self, prefix, uri):
        self._pending[prefix or ""] = uri

    def end_ns(self, prefix):
        pass

    def start(self, tag, attrib):
        scope = {**self._scope[-1], **self._pending}
        self._pending = {}
        elem = self._tb.start(tag, attrib)
        self.keep.append(elem)
        self.prefix[id(elem)] = _infer_prefix(_split(tag)[0], scope, element=True)
        self.attr_prefix[id(elem)] = {
            name: _infer_prefix(_split(name)[0], scope, element=False) for name in attrib if name[:1] == "{"
        }
        self.parent[id(elem)] = self._stack[-1] if self._stack else None
        if not self._stack:
            self.doc_nodes.append(("root", elem))
        self._stack.append(elem)
        self._scope.append(scope)
        return elem

    def end(self, tag):
        self._stack.pop()
        self._scope.pop()
        return self._tb.end(tag)

    def data(self, text):
        self._tb.data(text)

    def comment(self, text):
        if self._stack:
            return self._tb.comment(text)
        return None

    def pi(self, target, data=None):
        if self._stack:
            return self._tb.pi(target, data)
        self.doc_nodes.append(("pi", target, data or ""))
        return None

    def doctype(self, name, pubid, system):
        raise XmlRefused("DOCTYPE declarations are refused")

    def close(self):
        return self._tb.close()


class _Doc:
    def __init__(self, root, target):
        self.root = root
        self.nodes = target.doc_nodes
        self._prefix = target.prefix
        self._attr_prefix = target.attr_prefix
        self._parent = target.parent
        self._keep = target.keep
        self._order = None

    def prefix(self, el):
        p = self._prefix[id(el)]
        if p is _AMBIGUOUS:
            raise OracleLimitation(f"prefix of {el.tag} is ambiguous (two prefixes bound to one URI)")
        return p

    def attr_prefix(self, el, name):
        p = self._attr_prefix[id(el)].get(name)
        if p is _AMBIGUOUS:
            raise OracleLimitation(f"prefix of attribute {name} is ambiguous")
        return p

    def qname(self, el):
        p = self.prefix(el)
        local = _split(el.tag)[1]
        return f"{p}:{local}" if p else local

    def parent(self, el):
        return self._parent.get(id(el))

    def order(self, el):
        if self._order is None:
            self._order = {id(e): i for i, e in enumerate(_walk(self.root))}
        return self._order[id(el)]


def _parse_xml(data):
    """Parse bytes or str; refuse DTDs; keep comments, PIs and prefixes."""
    probe = data.encode("utf-8", "replace") if isinstance(data, str) else data
    if b"<!DOCTYPE" in probe or b"<!ENTITY" in probe:
        raise XmlRefused("DOCTYPE/ENTITY declarations are refused")
    target = _Target()
    parser = ET.XMLParser(target=target)
    parser.feed(data)
    root = parser.close()
    return _Doc(root, target)


def _kids(el):
    return [c for c in el if isinstance(c.tag, str)]


def _walk(el):
    """``el`` and its element descendants in document order."""
    yield el
    for child in el:
        if isinstance(child.tag, str):
            yield from _walk(child)


def _below(el):
    """Element descendants only: DOM ``Element.getElementsByTagNameNS`` scope."""
    it = _walk(el)
    next(it)
    yield from it


def _named(elements, ns, local):
    return [e for e in elements if _split(e.tag) == (ns, local)]


def _text_children(el):
    """DOM text-node children (expat folds CDATA into text, as DOM2DTM does)."""
    out = [el.text] if el.text else []
    out.extend(c.tail for c in el if c.tail)
    return out


def _text_content(el):
    """DOM ``getTextContent``: descendant text, never comment or PI content."""
    parts = [el.text or ""]
    for child in el:
        if isinstance(child.tag, str):
            parts.append(_text_content(child))
        parts.append(child.tail or "")
    return "".join(parts)


def _text_nodes_in_order(el):
    """(parent, text) for every text node under ``el``, in document order."""
    if el.text:
        yield el, el.text
    for child in el:
        if isinstance(child.tag, str):
            yield from _text_nodes_in_order(child)
        if child.tail:
            yield el, child.tail


# --------------------------------------------------------------------------- Java semantics
def _jws(ch):
    """``Character.isWhitespace``: no-break spaces are not whitespace in Java."""
    if ch in "\t\n\x0b\x0c\r\x1c\x1d\x1e\x1f":
        return True
    if ch in "   ":
        return False
    return unicodedata.category(ch) in ("Zs", "Zl", "Zp")


def _blank(s):
    """``StringUtils.isBlank`` / ``String.isBlank``."""
    return s is None or all(_jws(c) for c in s)


def _jtrim(s):
    """``String.trim``: strips code units <= U+0020 only."""
    i, j = 0, len(s)
    while i < j and s[i] <= " ":
        i += 1
    while j > i and s[j - 1] <= " ":
        j -= 1
    return s[i:j]


def _java_split(s, sep):
    """``String.split`` with a literal separator: trailing empty strings dropped."""
    if sep not in s:
        return [s]
    parts = s.split(sep)
    while parts and parts[-1] == "":
        parts.pop()
    return parts


_JAVA_FP = re.compile(
    r"[+-]?(?:NaN|Infinity"
    r"|(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?[fFdD]?"
    r"|0[xX](?:[0-9a-fA-F]+\.?|[0-9a-fA-F]*\.[0-9a-fA-F]+)[pP][+-]?[0-9]+[fFdD]?)"
)


def _java_fp(s):
    """``FloatingDecimal.readJavaFormatString``: grammar and trim shared by both parsers."""
    t = _jtrim(s)
    if not _JAVA_FP.fullmatch(t):
        raise ValueError(f"NumberFormatException: {s!r}")
    neg = t[0] == "-"
    body = t.lstrip("+-")
    if body in ("NaN", "Infinity"):
        return neg, body, None
    if body[-1] in "fFdD":
        body = body[:-1]
    if body[:2] in ("0x", "0X"):
        return neg, body, float.fromhex(body)
    return neg, body, float(body)


def _java_parse_double(s):
    """``Double.parseDouble``."""
    neg, body, d = _java_fp(s)
    if d is None:
        d = math.nan if body == "NaN" else math.inf
    return -d if neg else d


def _f32_bits(bits):
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def _bits_f32(f):
    return struct.unpack("<I", struct.pack("<f", f))[0]


def _f32(x):
    """Java ``(float)`` / ``Double.floatValue``: round to nearest even, overflow to infinity."""
    if math.isnan(x) or math.isinf(x):
        return x
    try:
        return struct.unpack("<f", struct.pack("<f", x))[0]
    except OverflowError:
        return math.copysign(math.inf, x)


def _java_parse_float(s):
    """``Float.parseFloat``: one correct rounding from the decimal string to float32."""
    neg, body, d = _java_fp(s)
    if d is None:
        f = math.nan if body == "NaN" else math.inf
        return -f if neg else f
    f = _f32(d)
    if f != d and d != 0 and not math.isinf(f):
        # Rounding the double again can only go wrong when the double sits exactly
        # on a float32 midpoint; settle that tie on the exact decimal value.
        b = _bits_f32(f)
        lo, hi = (f, _f32_bits(b + 1)) if f < d else (_f32_bits(b - 1), f)
        if not math.isinf(hi):
            mid = (Fraction(lo) + Fraction(hi)) / 2
            if Fraction(d) == mid:
                exact = Fraction(body) if body[:2] not in ("0x", "0X") else Fraction(d)
                if exact != mid:
                    f = lo if exact < mid else hi
                else:
                    f = lo if _bits_f32(lo) % 2 == 0 else hi
    return -f if neg else f


_JAVA_INT = re.compile(r"[+-]?\d+")


def _java_int(s):
    """``Integer.parseInt``: no trimming; any Unicode decimal digit, as ``Character.digit``."""
    if s is None or not _JAVA_INT.fullmatch(s):
        raise ValueError(f"NumberFormatException: {s!r}")
    v = int(s)
    if not -(2**31) <= v < 2**31:
        raise ValueError(f"NumberFormatException: {s!r}")
    return v


def _local_date(y, m, d):
    """``LocalDate.of`` (proleptic Gregorian, year 0 allowed) as an ISO string."""
    if not 1 <= m <= 12:
        raise ValueError("month")
    leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
    days = (31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)[m - 1]
    if not 1 <= d <= days:
        raise ValueError("day")
    return f"{y:04d}-{m:02d}-{d:02d}"


def _base_name(filename):
    """``FilenameUtils.getBaseName``."""
    if filename is None:
        return None
    return os.path.splitext(os.path.basename(filename))[0]


# --------------------------------------------------------------------------- PDF access (pypdf)
def _open(data):
    reader = PdfReader(io.BytesIO(data))
    if reader.is_encrypted:
        reader.decrypt("")
    # pypdf caches each xref stream object it reads while loading. When a later
    # revision reuses that object number (pypdf's own incremental writer does), the
    # stale cached object beats the newest xref entry. PDFBox, PDFium and poppler
    # resolve through the newest xref, so start from an empty cache.
    reader.resolved_objects.clear()
    return reader


def _resolve(obj):
    while isinstance(obj, IndirectObject):
        obj = obj.get_object()
    return obj


def _info_dict(reader):
    info = _resolve(reader.trailer.get("/Info"))
    return info if isinstance(info, dict) else {}


def _raw_string(obj):
    obj = _resolve(obj)
    if isinstance(obj, ByteStringObject):
        return bytes(obj)
    if isinstance(obj, TextStringObject):
        return obj.original_bytes
    return None


def _catalog_xmp(reader):
    """The newest catalog's decoded /Metadata stream, or None."""
    root = _resolve(reader.trailer.get("/Root"))
    meta = _resolve(root.get("/Metadata")) if isinstance(root, dict) else None
    return meta.get_data() if isinstance(meta, StreamObject) else None


def _pdfbox_doc_table():
    table = ["\x00"] * 256
    for i in range(256):
        # PDFDocEncoding.java:44-55 skips 0x18-0x1F, 0x7F-0xA0 and 0xAD; 0xAD is never
        # set again, so CODE_TO_UNI[0xAD] stays 0 and the byte decodes to U+0000.
        if 0x17 < i < 0x20 or 0x7E < i < 0xA1 or i == 0xAD:
            continue
        table[i] = chr(i)
    table[0x18:0x20] = list("˘ˇˆ˙˝˛˚˜")
    table[0x7F:0xA1] = list(
        "�•†‡…—–ƒ⁄‹›−‰„“”"
        "‘’‚™ﬁﬂŁŒŠŸŽıłœšž"
        "�€"
    )
    return table


_PDFBOX_DOC = _pdfbox_doc_table()


def _pdfbox_string(raw):
    """``COSString.getString`` (COSString.java:236-255): BOM means UTF-16, else PDFDocEncoding."""
    if raw[:2] == b"\xfe\xff":
        return raw[2:].decode("utf-16-be", "replace")
    if raw[:2] == b"\xff\xfe":
        return raw[2:].decode("utf-16-le", "replace")
    return "".join(_PDFBOX_DOC[b] for b in raw)


# =========================================================================== calibre
CALIBRE_NS_MAP = {  # xmp.py:32-51
    "rdf": RDF,
    "dc": DC,
    "pdf": "http://ns.adobe.com/pdf/1.3/",
    "pdfx": "http://ns.adobe.com/pdfx/1.3/",
    "xmp": XMP,
    "xmpidq": XMP_IDQ,
    "xmpMM": "http://ns.adobe.com/xap/1.0/mm/",
    "xmpRights": "http://ns.adobe.com/xap/1.0/rights/",
    "xmpBJ": "http://ns.adobe.com/xap/1.0/bj/",
    "xmpTPg": "http://ns.adobe.com/xap/1.0/t/pg/",
    "xmpDM": "http://ns.adobe.com/xmp/1.0/DynamicMedia/",
    "prism": "http://prismstandard.org/namespaces/basic/2.0/",
    "crossmark": "http://crossref.org/crossmark/1.0/",
    "xml": XML_NS,
    "x": "adobe:ns:meta/",
    "calibre": CALIBRE,
    "calibreSI": CALIBRE_SI,
    "calibreCC": "http://calibre-ebook.com/xmp-namespace-custom-columns",
}
_CALIBRE_XML_DECL = re.compile(r'<\?xml[^<>]+encoding\s*=\s*[\'"](.*?)[\'"][^<>]*>', re.IGNORECASE)  # xmp.py:30
_CALIBRE_AUTHOR_SPLIT = re.compile(r"(?i),?\s+(and|with)\s+")  # metadata/__init__.py:31 (tweak default)


def _cx(qname):
    prefix, local = qname.split(":")
    return f"{{{CALIBRE_NS_MAP[prefix]}}}{local}"


def _calibre_root(raw_bytes):
    # xmp.py:70-86 parse_xmp_packet
    raw_bytes = raw_bytes.strip()
    enc = None
    # xmp.py:73: '<?' makes the '<' optional; harmless, ported as written.
    pat = r"""<?xpacket\s+[^>]*?begin\s*=\s*['"]([^'"]*)['"]"""
    encodings = ("8", "16-le", "16-be", "32-le", "32-be")
    header = raw_bytes[:1024]
    emap = {"﻿".encode("utf-" + x): "utf-" + x for x in encodings}
    emap[b""] = "utf-8"
    for q in encodings:
        m = re.search(pat.encode("utf-" + q), header)
        if m is not None:
            # xmp.py:81: an unrecognised begin= value keeps enc None -> bytes are parsed as-is.
            enc = emap.get(m.group(1), enc)
            break
    # xmp.py:85: a strict decode; invalid bytes raise and calibre drops the whole packet.
    source = raw_bytes if enc is None else _CALIBRE_XML_DECL.sub("", raw_bytes.decode(enc))
    try:
        return _parse_xml(source).root
    except ET.ParseError as exc:
        # xml_parse.py:50: calibre parses with lxml recover=True; ET cannot recover.
        raise OracleLimitation(f"lxml recover=True is not emulated: {exc}") from exc


def _c_all(root, qname):
    tag = _cx(qname)
    return [e for e in _walk(root) if e.tag == tag]  # '//q' from the root: descendant-or-self


def _c_desc(el, qname):
    tag = _cx(qname)
    return [e for e in _below(el) if e.tag == tag]  # 'descendant::q'


def _c_simple(elem):
    # xmp.py:98-103: .text (text before the first child node), else rdf:resource, else ''.
    if elem is not None:
        if elem.text:
            return elem.text
        return elem.get(_cx("rdf:resource"), "")
    return None


def _c_lang_alt(parent):
    # xmp.py:106-113: any descendant rdf:li, x-default first; no container type check.
    lang = _cx("xml:lang")
    items = [li for li in _c_desc(parent, "rdf:li") if li.get(lang) == "x-default"]
    if items:
        return items[0]
    items = _c_desc(parent, "rdf:li")
    return items[0] if items else None


def _c_sequence(parent):
    return [_c_simple(li) for li in _c_desc(parent, "rdf:li")]  # xmp.py:116-119


def _c_multiple(root, qname):
    # xmp.py:132-138: only rdf:li items count, so a plain-text property yields nothing.
    ans = []
    for item in _c_all(root, qname):
        ans += _c_sequence(item)
    seen, out = set(), []
    for v in ans:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return [v for v in out if v]


def _c_first_alt(root, qname):
    # xmp.py:141-147: a property without rdf:li (plain text) is skipped.
    for item in _c_all(root, qname):
        q = _c_simple(_c_lang_alt(item))
        if q:
            return q
    return None


def _c_first_simple(root, qname):
    for item in _c_all(root, qname):  # xmp.py:150-156
        q = _c_simple(item)
        if q:
            return q
    return None


def _c_first_sequence(root, qname):
    # xmp.py:159-163: returns the first item even when it is '' (no truthiness check).
    for item in _c_all(root, qname):
        for ans in _c_sequence(item):
            return ans
    return None


def _c_series(root):
    # xmp.py:166-181 read_series: any calibre:series, any descendant rdf:value; the name is
    # returned unstripped; the index is Python float() of the first parseable
    # calibreSI:series_index, default 1.0.
    for item in _c_all(root, "calibre:series"):
        val = _c_desc(item, "rdf:value")
        if val:
            series = val[0].text
            if series and series.strip():
                series_index = 1.0
                for si in _c_desc(item, "calibreSI:series_index"):
                    try:
                        series_index = float(si.text)
                    except (TypeError, ValueError):
                        continue
                    else:
                        break
                return series, series_index
    return None, None


def _string_to_authors(raw):
    # metadata/__init__.py:34-40
    if not raw:
        return []
    raw = raw.replace("&&", "￿")
    raw = _CALIBRE_AUTHOR_SPLIT.sub("&", raw)
    authors = [a.strip().replace("￿", "&") for a in raw.split("&")]
    return [a for a in authors if a]


def _check_isbn(isbn):
    # metadata/__init__.py:419-460
    if not isbn:
        return None
    isbn = re.sub(r"[^0-9X]", "", isbn.upper())
    if len(isbn) not in (10, 13) or re.match(r"(\d)\1{9,12}$", isbn):
        return None
    try:
        if len(isbn) == 10:
            check = sum((i + 1) * int(isbn[i]) for i in range(9)) % 11
            return isbn if ("X" if check == 10 else str(check)) == isbn[9] else None
        check = 10 - sum((1 if i % 2 == 0 else 3) * int(isbn[i]) for i in range(12)) % 10
        return isbn if str(0 if check == 10 else check) == isbn[12] else None
    except ValueError:
        return None


def _check_doi(doi):
    if not doi:  # metadata/__init__.py:508-515
        return None
    m = re.search(r"10\.\d{4}/\S+", doi)
    return m.group() if m else None


def _c_identifiers(root):
    ids = {}
    for xmpid in _c_all(root, "xmp:Identifier"):  # xmp.py:326-329
        for bag in _kids(xmpid):
            if bag.tag != _cx("rdf:Bag"):
                continue
            for li in _kids(bag):
                if li.tag != _cx("rdf:li"):
                    continue
                # xmp.py:217-218: lxml len() counts comment/PI children too, as does ET here.
                is_resource = li.get(_cx("rdf:parseType")) == "Resource"
                is_resource = is_resource or (len(li) == 1 and li[0].tag == _cx("rdf:Description"))
                pairs = []
                if not is_resource:
                    pairs.append((None, li.text or ""))  # xmp.py:220: no 'continue' after this yield
                value = _c_desc(li, "rdf:value")
                if value:
                    value = value[0].text or ""
                    scheme = _c_desc(li, "xmpidq:Scheme")
                    pairs.append((scheme[0].text or "", value) if scheme else (None, value))
                for scheme, value in pairs:
                    if scheme and value:
                        ids[scheme.lower()] = value
    for namespace in ("prism", "pdfx"):  # xmp.py:331-341
        for scheme in ("isbn", "url", "doi"):  # KNOWN_ID_SCHEMES; each scheme is independent
            if scheme not in ids:
                val = _c_first_simple(root, f"{namespace}:{scheme}")
                if scheme == "isbn":
                    val = _check_isbn(val)
                elif scheme == "doi":
                    val = _check_doi(val)
                if val:
                    ids[scheme] = val
    for scheme, check in (("doi", _check_doi), ("isbn", _check_isbn)):  # xmp.py:344-348
        if scheme not in ids:
            val = check(_c_first_simple(root, "dc:identifier"))
            if val:
                ids[scheme] = val
    return ids


_CAL_DATE = re.compile(
    r"(?P<y>\d{4})(?:(?P<sep>[-/])(?P<mo>\d{1,2})(?:(?P=sep)(?P<d>\d{1,2}))?|(?P<mo8>\d{2})(?P<d8>\d{2}))?"
    r"(?:[Tt ](?P<H>\d{1,2})(?::(?P<M>\d{1,2})(?::(?P<S>\d{1,2})(?:\.(?P<f>\d+))?)?)?)?"
    r"\s*(?P<tz>[Zz]|[+-]\d{1,2}(?::?\d{2})?)?",
    re.ASCII,
)


def _calibre_parse_date(raw, *, assume_utc, now, tz):
    """``calibre.utils.date.parse_date`` (date.py:90-116) for W3C-DTF style strings.

    Returns (aware UTC datetime, the components dateutil took from calibre's
    default). Raises ValueError where dateutil would raise (out-of-range values)
    and OracleLimitation for strings outside the emulated grammar.
    """
    s = raw.strip()
    m = _CAL_DATE.fullmatch(s)
    if m is None:
        raise OracleLimitation(f"dateutil.parser.parse({raw!r}) is not emulated")
    zone = UTC if assume_utc else tz
    base = (now or datetime.now(zone)).astimezone(zone)
    # date.py:110-111: missing parts come from *now* with day=15 and midnight, local zone.
    default = base.replace(day=15, hour=0, minute=0, second=0, microsecond=0)
    filled = []
    month = m.group("mo") or m.group("mo8")
    day = m.group("d") or m.group("d8")
    parts = {}
    for name, value, fallback in (
        ("month", month, default.month),
        ("day", day, default.day),
        ("hour", m.group("H"), 0),
        ("minute", m.group("M"), 0),
        ("second", m.group("S"), 0),
    ):
        if value is None:
            filled.append(name)
            parts[name] = fallback
        else:
            parts[name] = int(value)
    micro = int(m.group("f")[:6].ljust(6, "0")) if m.group("f") else 0
    tzs = m.group("tz")
    if tzs is None:
        filled.append("tz")
        tzinfo_ = default.tzinfo  # date.py:137: fix_tzinfo assumes the local zone
    elif tzs in "Zz":
        tzinfo_ = UTC
    else:
        sign = -1 if tzs[0] == "-" else 1
        digits = tzs[1:].replace(":", "")
        if len(digits) > 2:
            hours, minutes = int(digits[:-2]), int(digits[-2:])
        else:
            hours, minutes = int(digits), 0
        tzinfo_ = timezone(sign * timedelta(hours=hours, minutes=minutes))
    dt = datetime(int(m.group("y")), parts["month"], parts["day"], parts["hour"], parts["minute"],
                  parts["second"], micro, tzinfo=tzinfo_)
    return dt.astimezone(UTC), filled


def _calibre_safe_date(raw, now, tz):
    # xmp.py:232-237 safe_parse_date: falsy -> None, parse errors -> None.
    if not raw:
        return None
    try:
        return _calibre_parse_date(raw, assume_utc=False, now=now, tz=tz)[0]
    except ValueError:
        return None


def _calibre_metadata_date(root, now, tz):
    # xmp.py:282-286 + more_recent (240-248)
    md = _calibre_safe_date(_c_first_simple(root, "xmp:MetadataDate"), now, tz)
    mod = _calibre_safe_date(_c_first_simple(root, "xmp:ModifyDate"), now, tz)
    if md is None:
        return mod
    if mod is None:
        return md
    return max(md, mod)


def calibre_view(xmp: bytes, *, now: datetime | None = None, tz: tzinfo = UTC) -> dict:
    """What calibre's ``metadata_from_xmp_packet`` reads from an XMP packet.

    ``tz`` stands for calibre's ``local_tz`` and ``now`` for the clock its
    ``parse_date`` default is built from. Keys: title, authors, comments,
    publisher, tags, pubdate_raw (the string handed to ``parse_date``),
    pubdate_field ("dc:date" / "xmp:CreateDate" / None), pubdate_needs_fill
    (month or day would come from calibre's default: the current month, day 15),
    pubdate_filled (every component taken from the default), pubdate (UTC ISO
    string or None), pubdate_error, series, series_index (Python float),
    identifiers, metadata_date (UTC ISO string or None).
    """
    root = _calibre_root(xmp)
    title = _c_first_alt(root, "dc:title")  # xmp.py:254
    if title and title.startswith(r"\376\377"):  # xmp.py:256-259 Nitro PDF guard
        raise ValueError("Corrupted XMP metadata packet detected, probably generated by Nitro PDF")
    authors = _c_multiple(root, "dc:creator")  # xmp.py:261-263
    authors = [au for aus in authors for au in _string_to_authors(aus)]
    # xmp.py:264: pdf:Keywords only counts as rdf:li items, so plain-text Keywords are ignored.
    tags = _c_multiple(root, "dc:subject") or _c_multiple(root, "pdf:Keywords")
    comments = _c_first_alt(root, "dc:description")  # xmp.py:267
    publishers = _c_multiple(root, "dc:publisher")  # xmp.py:270-272: a plain-text publisher is ignored
    # xmp.py:274: the first rdf:li of the first dc:date that has one, else xmp:CreateDate.
    raw = _c_first_sequence(root, "dc:date")
    field = "dc:date" if raw else None
    if not raw:
        raw = _c_first_simple(root, "xmp:CreateDate")
        field = "xmp:CreateDate" if raw else None
    pubdate, filled, error = None, [], None
    if raw:
        try:
            dt, filled = _calibre_parse_date(raw, assume_utc=False, now=now, tz=tz)
            pubdate = dt.isoformat()
        except ValueError as exc:  # xmp.py:275-276: a parse error leaves pubdate unset
            error = str(exc)
    series, series_index = _c_series(root)  # xmp.py:295-297
    md = _calibre_metadata_date(root, now, tz)
    return {
        "title": title,
        "authors": authors,
        "comments": comments,
        "publisher": publishers[0] if publishers else None,
        "tags": tags,
        "pubdate_raw": raw,
        "pubdate_field": field,
        "pubdate_needs_fill": bool({"month", "day"} & set(filled)),
        "pubdate_filled": filled,
        "pubdate": pubdate,
        "pubdate_error": error,
        "series": series if series else None,
        "series_index": series_index if series else None,
        "identifiers": _c_identifiers(root),
        "metadata_date": md.isoformat() if md else None,
    }


def calibre_prefers_info(
    xmp: bytes, info_moddate: str | None, *, now: datetime | None = None, tz: tzinfo = UTC
) -> bool:
    """``consolidate_metadata``'s prefer-Info decision (xmp.py:358-398).

    ``info_moddate`` is the ModDate calibre's info dict holds, i.e. what
    ``pdfinfo -isodates`` prints (``"2025-03-01T12:00:00Z"``), or None when the
    PDF has none. True means calibre keeps the Info dict's title, authors and
    tags; every other field still comes from the XMP via ``smart_update``
    (xmp.py:377). An empty packet or one calibre cannot read also leaves the
    Info values in place, so it returns True.
    """
    raw = xmp.rstrip()  # xmp.py:363
    if not raw:
        return True
    try:
        root = _calibre_root(raw)
    except UnicodeDecodeError:
        return True  # xmp.py:367-371: any exception -> return info_mi untouched
    title = _c_first_alt(root, "dc:title")
    if title and title.startswith(r"\376\377"):
        return True  # the Nitro guard raises inside metadata_from_xmp_packet, same outcome
    # xmp.py:379: hasattr(xmp_mi, 'metadata_date') is only true when MetadataDate or
    # ModifyDate parsed (metadata_date is not a standard field; base.py __setattr__).
    md = _calibre_metadata_date(root, now, tz)
    if info_moddate is None or md is None:
        return False
    try:
        info_date, _ = _calibre_parse_date(info_moddate, assume_utc=False, now=now, tz=tz)  # xmp.py:381
    except ValueError:
        return False
    return info_date > md  # xmp.py:387


# =========================================================================== PDFBox DateConverter
_MS_HOUR = 3_600_000
_HALF_DAY = 12 * _MS_HOUR
_DAY = 2 * _HALF_DAY


def _pb_skip(text, index, optionals):
    # DateConverter.java:349-360 skipOptionals: returns the last non-space char skipped.
    retval = " "
    while index < len(text) and text[index] in optionals:
        if text[index] != " ":
            retval = text[index]
        index += 1
    return retval, index


def _pb_field(text, index, maxlen, remedy):
    # DateConverter.java:307-337 parseTimeField
    retval, i = 0, index
    limit = index + min(maxlen, len(text) - index)
    while i < limit and "0" <= text[i] <= "9":
        retval = retval * 10 + ord(text[i]) - 48
        i += 1
    if i == index:
        return remedy, index
    return retval, i


def _pb_restrain(ms):
    # DateConverter.java:208-226 restrainTZoffset (Java remainder keeps the dividend's sign)
    if -14 * _MS_HOUR <= ms <= 14 * _MS_HOUR:
        return ms
    ms = (ms + _HALF_DAY) % _DAY
    if ms == 0:
        return _HALF_DAY
    r = ms - _HALF_DAY
    return 0 if r == -_HALF_DAY else r


def _pb_big_endian(text, pos):
    # DateConverter.java:526-568 parseBigEndianDate
    year, w = _pb_field(text, pos, 4, 0)
    if w != pos + 4:
        return None
    _, w = _pb_skip(text, w, "/- ")
    month, w = _pb_field(text, w, 2, 1)
    _, w = _pb_skip(text, w, "/- ")
    day, w = _pb_field(text, w, 2, 1)
    _, w = _pb_skip(text, w, " T")
    hour, w = _pb_field(text, w, 2, 0)
    _, w = _pb_skip(text, w, ": ")
    minute, w = _pb_field(text, w, 2, 0)
    _, w = _pb_skip(text, w, ": ")
    second, w = _pb_field(text, w, 2, 0)
    c, w = _pb_skip(text, w, ".")
    if c == ".":
        _, w = _pb_field(text, w, 19, 0)
    try:
        # non-lenient GregorianCalendar in UTC; Python's proleptic calendar differs only
        # before the 1582 Julian cutover.
        dt = datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    except ValueError:
        return None
    _, w = _pb_skip(text, w, " ")
    return dt, w


def _pb_tz(text, dt, pos):
    # DateConverter.java:429-473 parseTZoffset (numeric and GMT/UTC/Z branches)
    sign, w = _pb_skip(text, pos, "Z+- ")
    had_gmt = sign == "Z"
    if not had_gmt:
        for word in ("GMT", "UTC"):
            if text.startswith(word, w):
                w += 3
                had_gmt = True
                break
    if had_gmt:
        sign, w = _pb_skip(text, w, "+- ")
    hours, w = _pb_field(text, w, 2, -999)
    _, w = _pb_skip(text, w, "': ")
    minutes, w = _pb_field(text, w, 2, 0)
    _, w = _pb_skip(text, w, "' ")
    if hours != -999:
        offset = _pb_restrain((-1 if sign == "-" else 1) * (hours * _MS_HOUR + minutes * 60_000))
    elif not had_gmt:
        raise OracleLimitation(f"PDFBox TimeZone.getTimeZone(name) branch is not ported: {text!r}")
    else:
        offset = 0
    # adjustTimeZoneNicely (402-408): the fields were set in UTC, shift by -offset.
    return dt - timedelta(milliseconds=offset), w


def _pdfbox_to_calendar(text):
    """``DateConverter.toCalendar(String)`` (719-737) as an aware datetime, or None."""
    if text is None or _jtrim(text) == "":
        return None
    _, pos = _pb_skip(text, 0, " ")
    if text.startswith("D:", pos):
        pos += 2
    if text == "" or _jtrim(text) == "D:":  # parseDate (623)
        return None
    _, pos = _pb_skip(text, pos, " ")
    parsed = _pb_big_endian(text, pos)
    if parsed is not None:
        dt, w = parsed
        if w == len(text):
            return dt
        dt, w = _pb_tz(text, dt, w)
        if w == len(text):
            return dt
    # parseDate (658-689) falls back to SimpleDateFormat patterns; not ported.
    raise OracleLimitation(f"PDFBox SimpleDateFormat fallback is not ported: {text!r}")


# =========================================================================== Booklore
_BOOKLORE_NS = {  # PdfMetadataExtractor.java:586-594
    "dc": DC,
    "rdf": RDF,
    "xmp": XMP,
    "xmpidq": XMP_IDQ,
    "calibre": CALIBRE,
    # :592 maps calibreSI to a URI calibre never writes, so calibreSI:series_index never
    # matches and only the local-name() fallback (:314) finds calibre's index.
    "calibreSI": "http://calibre-ebook.com/xmp-namespace/seriesIndex",
    "booklore": BOOKLORE,
}


def _xp_select(doc, path, context=None):
    """XPath ``//a:b/c:d`` (context None) or ``a:b/c:d`` relative to ``context``.

    A step written ``*local`` is ``*[local-name()='local']``. Direct children
    only between steps, as XPath's ``/`` requires.
    """
    steps = []
    for step in path.split("/"):
        if step.startswith("*"):
            steps.append(("*", step[1:]))
        else:
            prefix, local = step.split(":")
            steps.append((_BOOKLORE_NS[prefix], local))

    def ok(el, st):
        ns, local = _split(el.tag)
        return local == st[1] and (st[0] == "*" or ns == st[0])

    cur = [e for e in (_walk(doc.root) if context is None else _kids(context)) if ok(e, steps[0])]
    for st in steps[1:]:
        cur = [c for e in cur for c in _kids(e) if ok(c, st)]
    uniq = {id(e): e for e in cur}
    return sorted(uniq.values(), key=doc.order)


def _xp_texts(doc, path, context=None):
    """``path/text()`` as a node-set of strings, in document order."""
    wanted = {id(e) for e in _xp_select(doc, path, context)}
    scope = doc.root if context is None else context
    return [t for parent, t in _text_nodes_in_order(scope) if id(parent) in wanted]


def _xp_string(doc, path, context=None):
    """``XPath.evaluate(path + '/text()')``: the first text node's value, else ''."""
    texts = _xp_texts(doc, path, context)
    return texts[0] if texts else ""


def _bl_info(info, key):
    # COSDictionary.java:720-729 getString: only a COSString yields text.
    raw = _raw_string(info.get(key))
    return None if raw is None else _pdfbox_string(raw)


def _bl_split_list(text):
    parts = _java_split(text, ";") if ";" in text else _java_split(text, ",")
    return {_jtrim(p) for p in parts if not _blank(_jtrim(p))}


def booklore_view(data: bytes, *, zone: str = "UTC", filename: str | None = None) -> dict:
    """What Booklore's ``PdfMetadataExtractor.extractMetadata`` builds from a PDF.

    ``zone`` stands for the JVM's ``ZoneId.systemDefault()`` used to turn Info
    /CreationDate into a LocalDate (:554-557); a container without TZ set runs
    in UTC. ``filename`` feeds the title fallback (:86). publishedDate is an
    ISO date string; seriesNumber is the float32 value as a Python float;
    categories, moods and tags are sets (HashSet upstream).
    """
    reader = _open(data)
    info = _info_dict(reader)
    out = {
        "title": None, "authors": [], "description": None, "publisher": None, "publishedDate": None,
        "pageCount": None, "categories": set(), "language": None, "seriesName": None, "seriesNumber": None,
        "seriesTotal": None, "subtitle": None, "isbn10": None, "isbn13": None, "googleId": None, "asin": None,
        "goodreadsId": None, "comicvineId": None, "ranobedbId": None, "hardcoverId": None,
        "hardcoverBookId": None, "lubimyczytacId": None, "moods": set(), "tags": set(), "ratings": {},
        "xmp_error": None,
    }
    # PDFBox always returns a PDDocumentInformation, so the Info block always runs (:80-138).
    title = _bl_info(info, "/Title")
    out["title"] = title if not _blank(title) else _base_name(filename)  # :83-87
    author = _bl_info(info, "/Author")
    if not _blank(author):  # :89-94 parseAuthors splits on [,&]
        authors = [_jtrim(a) for a in re.split(r"[,&]", author) if not _blank(_jtrim(a))]
        if authors:
            out["authors"] = authors
    subject = _bl_info(info, "/Subject")
    if not _blank(subject):
        out["description"] = subject  # :96-98, untrimmed
    publisher = _bl_info(info, "/EBX_PUBLISHER")
    if not _blank(publisher):
        out["publisher"] = publisher  # :100-106
    created = _bl_info(info, "/CreationDate")
    if created is not None:  # :108-113 (COSDictionary.getDate: COSString only)
        cal = _pdfbox_to_calendar(created)
        if cal is not None:
            out["publishedDate"] = cal.astimezone(ZoneInfo(zone)).date().isoformat()
    pages = _resolve(_resolve(reader.trailer["/Root"]).get("/Pages"))
    out["pageCount"] = int(_resolve(pages.get("/Count", 0))) if isinstance(pages, dict) else 0  # :115
    keywords = _bl_info(info, "/Keywords")
    if not _blank(keywords):  # :117-132
        cats = _bl_split_list(keywords)
        if cats:
            out["categories"] = cats
    language = _bl_info(info, "/Language")
    if not _blank(language):
        out["language"] = language  # :134-137

    xmp = _catalog_xmp(reader)
    if xmp is not None:
        raw = xmp.decode("utf-8", "replace")  # :147 IOUtils.toString(is, UTF_8)
        if not _blank(raw):
            try:
                doc = _parse_xml(raw.encode("utf-8"))  # :149-150 namespace-aware DOM
            except (ET.ParseError, XmlRefused) as exc:
                out["xmp_error"] = str(exc)  # :241-243: the XMP part is skipped, Info stays
                doc = None
            if doc is not None:
                _bl_dublin_core(doc, out)
                _bl_calibre(doc, out)
                _bl_booklore(doc, out)
                _bl_identifiers(doc, out)  # :167: runs after, so it overrides booklore:isbn*
    return out


def _bl_dublin_core(doc, out):
    # :254-299. xpathEvaluate trims (:559-562); only the listed container forms match.
    title = _jtrim(_xp_string(doc, "dc:title/rdf:Alt/rdf:li"))  # first li, not x-default
    if not _blank(title):
        out["title"] = title
    description = _jtrim(_xp_string(doc, "dc:description/rdf:Alt/rdf:li"))
    if not _blank(description):
        out["description"] = description
    publisher = _jtrim(_xp_string(doc, "dc:publisher/rdf:Bag/rdf:li"))
    if not _blank(publisher):
        out["publisher"] = publisher
    language = _jtrim(_xp_string(doc, "dc:language/rdf:Bag/rdf:li"))
    if not _blank(language):
        out["language"] = language
    # :274: creators only from rdf:Seq; a Bag of creators is ignored.
    creators = [_jtrim(t) for t in _xp_texts(doc, "dc:creator/rdf:Seq/rdf:li") if not _blank(t)]
    if creators:
        out["authors"] = creators
    # :279: subjects only from rdf:Bag.
    subjects = {_jtrim(t) for t in _xp_texts(doc, "dc:subject/rdf:Bag/rdf:li") if not _blank(t)}
    if subjects:
        known = set()
        # :284-291: only the legacy PascalCase booklore:Moods/Tags strings are removed.
        for legacy in ("booklore:Moods", "booklore:Tags"):
            value = _xp_string(doc, legacy)
            if not _blank(value):
                known.update(_jtrim(p) for p in _java_split(value, ";"))
        subjects -= known
        if subjects:
            out["categories"] = subjects


def _bl_calibre(doc, out):
    # :302-328. The series name is not trimmed (XPath.evaluate, not xpathEvaluate).
    series = _xp_string(doc, "calibre:series/rdf:value")  # rdf:value must be a direct child
    if not _blank(series):
        out["seriesName"] = series
    index = _xp_string(doc, "calibre:series/calibreSI:series_index")  # never matches, see :592
    if _blank(index):
        index = _xp_string(doc, "calibre:series/*series_index")  # :314 local-name() fallback
    if not _blank(index):
        try:
            out["seriesNumber"] = _java_parse_float(index)  # :319 Float.parseFloat -> float32
        except ValueError:
            pass


def _bl_booklore(doc, out):
    # :330-472, all from http://booklore.org/metadata/1.0/ only.
    def field(name):
        return _xp_string(doc, f"booklore:{name}")  # :479

    name = field("seriesName")
    if not _blank(name):
        out["seriesName"] = name  # :334-336, untrimmed
    number = field("seriesNumber")
    if not _blank(number):
        try:
            out["seriesNumber"] = _java_parse_float(_jtrim(number))  # :338-345
        except ValueError:
            pass
    total = field("seriesTotal")
    if not _blank(total):
        try:
            out["seriesTotal"] = _java_int(_jtrim(total))  # :347-354
        except ValueError:
            pass
    subtitle = field("subtitle")
    if _blank(subtitle):
        subtitle = field("Subtitle")
    if not _blank(subtitle):
        out["subtitle"] = _jtrim(subtitle)
    for key, xml_name in (
        ("isbn13", "isbn13"), ("isbn10", "isbn10"), ("googleId", "googleId"), ("goodreadsId", "goodreadsId"),
        ("hardcoverId", "hardcoverId"), ("hardcoverBookId", "hardcoverBookId"), ("asin", "asin"),
        ("comicvineId", "comicvineId"), ("lubimyczytacId", "lubimyczytacId"), ("ranobedbId", "ranobedbId"),
    ):
        value = field(xml_name)
        if not _blank(value):
            out[key] = _jtrim(value)
    for key, legacy in (("moods", "Moods"), ("tags", "Tags")):  # :423-457
        values = {_jtrim(t) for t in _xp_texts(doc, f"booklore:{key}/rdf:Bag/rdf:li") if not _blank(t)}
        if not values:
            text = field(legacy)
            if not _blank(text):
                values = {_jtrim(p) for p in _java_split(text, ";") if not _blank(_jtrim(p))}
        if values:
            out[key] = values
    for new, legacy, key in (  # :460-467
        ("amazonRating", "AmazonRating", "amazonRating"), ("goodreadsRating", "GoodreadsRating", "goodreadsRating"),
        ("hardcoverRating", "HardcoverRating", "hardcoverRating"),
        ("lubimyczytacRating", "LubimyczytacRating", "lubimyczytacRating"),
        ("ranobedbRating", "RanobedbRating", "ranobedbRating"), ("rating", "Rating", "rating"),
    ):
        value = field(new)
        if _blank(value):
            value = field(legacy)
        if not _blank(value):
            try:
                out["ratings"][key] = _java_parse_double(_jtrim(value))
            except ValueError:
                pass


def _bl_identifiers(doc, out):
    # :529-544: Scheme and rdf:value must be direct children of the rdf:li
    # (the rdf:parseType="Resource" form; the nested rdf:Description form is missed).
    ids = {}
    for li in _xp_select(doc, "xmp:Identifier/rdf:Bag/rdf:li"):
        scheme = _jtrim(_xp_string(doc, "xmpidq:Scheme", li))
        value = _jtrim(_xp_string(doc, "rdf:value", li))
        if not _blank(scheme) and not _blank(value):
            ids[scheme.lower()] = value
    if not ids:
        return
    isbn = ids.get("isbn")  # :169-180
    if not _blank(isbn):
        isbn = re.sub(r"[^0-9Xx]", "", isbn)
        out["isbn10" if len(isbn) == 10 else "isbn13"] = isbn
    for key in ("isbn13", "isbn10"):  # :183-197
        value = ids.get(key)
        if not _blank(value):
            cleaned = re.sub(r"[^0-9Xx]", "", value)
            if not _blank(cleaned):
                out[key] = cleaned
    for scheme, key in (
        ("google", "googleId"), ("amazon", "asin"), ("goodreads", "goodreadsId"), ("comicvine", "comicvineId"),
        ("ranobedb", "ranobedbId"), ("hardcover", "hardcoverId"), ("hardcover_book_id", "hardcoverBookId"),
        ("lubimyczytac", "lubimyczytacId"),
    ):
        value = ids.get(scheme)
        if not _blank(value):
            out[key] = value


# =========================================================================== Grimmory / PDFium4j
_P4J_MAX_XMP = 10 * 1024 * 1024  # XmpMetadataParser.java:37
_P4J_CORE = {  # XmpMetadataParser.java:49-58 (attribute filter)
    DC, PDFA_ID, CALIBRE, RDF, "http://www.w3.org/2000/xmlns/", XML_NS, "adobe:ns:meta/",
}
_XMP_START, _XMP_END, _XMP_TERM = b"<?xpacket begin", b"<?xpacket end", b"?>"  # PdfDocumentXmp.java:17-24
_XMPMETA_START, _XMPMETA_END = b"<x:xmpmeta", b"</x:xmpmeta>"
_RDF_START, _RDF_END = b"<rdf:RDF", b"</rdf:RDF>"
_TAIL_SCAN = 256 << 10  # PdfDocumentXmp.java:25


def _p4j_extract(seg):
    # PdfDocumentXmp.java:68-100. Every block start found becomes the new best, so the
    # result is the innermost block (<rdf:RDF ...>) of the LAST packet in byte order.
    search_from, best = 0, b""
    while search_from < len(seg):
        found = [i for i in (seg.find(_XMP_START, search_from), seg.find(_XMPMETA_START, search_from),
                             seg.find(_RDF_START, search_from)) if i >= 0]
        if not found:
            break
        nxt = min(found)
        if seg.startswith(_XMP_START, nxt):
            block, jump = b"", len(_XMP_START)
            end = seg.find(_XMP_END, nxt + len(_XMP_START))  # :122-133 extractXpacketAt
            if end >= 0:
                term = seg.find(_XMP_TERM, end + len(_XMP_END))
                if term >= 0:
                    block = seg[nxt:term + len(_XMP_TERM)]
        else:
            start_pat, end_pat = (_XMPMETA_START, _XMPMETA_END) if seg.startswith(_XMPMETA_START, nxt) else (
                _RDF_START, _RDF_END)
            jump = len(start_pat)
            end = seg.find(end_pat, nxt + len(start_pat))  # :135-149 extractBlock
            block = seg[nxt:end + len(end_pat)] if end >= 0 else b""
        if block and _p4j_meaningful(block):
            best = block
        search_from = nxt + max(1, jump)
    return best


def _p4j_meaningful(block):
    # :114-120 isMeaningfulXmp
    if len(block) < 50:
        return False
    s = block.decode("utf-8", "replace")
    return "<rdf:Description" in s or ":Description" in s


def _p4j_fallback_xmp(data):
    # PdfDocumentXmp.java:30-66 computeFallbackXmp, path branch (PdfDocument.open(Path)).
    size = len(data)
    if size <= 0:
        return b""
    tail_size = min(size, _TAIL_SCAN)
    tail_start = size - tail_size
    tail = _p4j_extract(data[tail_start:])
    if tail_start == 0:
        return tail
    if not tail:
        return _p4j_extract(data)
    if tail.startswith(_XMP_START):
        return tail
    full = _p4j_extract(data)
    return full if full else tail


def _p4j_info_text(info, key):
    # FPDF_GetMetaText decodes the newest Info entry (approximated with pypdf's decoding).
    obj = _resolve(info.get(key))
    if isinstance(obj, TextStringObject):
        return str(obj)
    if isinstance(obj, ByteStringObject):
        return _pdfbox_string(bytes(obj))
    if isinstance(obj, NameObject):
        return str(obj)[1:]
    return None


def _p4j_xmp_bytes(data, info):
    # PdfDocument.java:1109-1138 xmpMetadata(): the shim's pdfium4j_get_xmp_metadata
    # (pdfium4j_read.cpp:263-293) uses FPDF_GetXMPMetadata only when the PDFium build
    # exports it; stock pdfium-binaries (chromium/7834, PDFium4j build.gradle.kts:128)
    # does not, so it asks FPDF_GetMetaText(doc, "xmp"): the Info dict's /xmp entry,
    # returned NUL-terminated (:286-291). Without one, the raw byte scan runs.
    text = _p4j_info_text(info, "/xmp")
    if text:
        return text.encode("utf-8") + b"\0"
    return _p4j_fallback_xmp(data)


def _p4j_sanitize(xmp):
    # XmpMetadataParser.java:151-182 sanitizeXmpString
    if not xmp:
        return xmp
    needs = xmp.find("<?xml") > 0 or "\0" in xmp or "﻿" in xmp
    if not needs:
        return xmp
    cleaned = xmp.replace("\0", "").replace("﻿", "")
    start = cleaned.find("<?xml")
    return cleaned[start:] if start > 0 else cleaned


class _GrimXmp:
    """``org.grimmory.pdfium4j.model.XmpMetadata`` plus its parser's output."""

    def __init__(self):
        self.title = self.description = self.publisher = self.language = self.date = self.rights = None
        self.creators, self.subjects, self.identifiers = [], [], []
        self.calibre_fields, self.custom_fields, self.custom_list_fields = {}, {}, {}
        self.xmp_identifiers = []
        self.flagged = []

    def calibre_series(self):
        s = self.calibre_fields.get("series")  # XmpMetadata.java:96-98
        return s if s is not None and not _blank(s) else None

    def calibre_series_index(self):
        val = self.calibre_fields.get("series_index")  # :101-109 Double.parseDouble
        if val is None or _blank(val):
            return None
        try:
            return _java_parse_double(val)
        except ValueError:
            return None

    def find_field(self, local):
        # XmpMetadata.java:136-150: suffix match over customFields, which is a Map.copyOf
        # (:75) with unspecified order; then an exact key in calibreFields.
        suffix = ":" + local
        hits = [(k, v) for k, v in self.custom_fields.items() if k.endswith(suffix) or k == local]
        if hits:
            if len({v for _, v in hits}) > 1:
                raise GrimmoryAmbiguity(f"findField({local!r}) matches {sorted(k for k, _ in hits)}")
            if len(hits) > 1:
                self.flagged.append((local, sorted(k for k, _ in hits)))
            return hits[0][1]
        return self.calibre_fields.get(local)

    def find_list_field(self, local):
        # XmpMetadata.java:156-164: customListFields is a LinkedHashMap copy (:76-83), ordered.
        suffix = ":" + local
        for key, values in self.custom_list_fields.items():
            if key.endswith(suffix) or key == local:
                return values
        return []


def _p4j_li_values(el):
    # XmpMetadataParser.java:340-352 getListFromRdfContainer: every descendant rdf:li.
    return [_jtrim(t) for t in (_text_content(li) for li in _named(_below(el), RDF, "li")) if not _blank(t)]


def _p4j_extract_doc(doc):
    x = _GrimXmp()
    root = doc.root

    def first_dc_text(local):
        nodes = _named(_walk(root), DC, local)  # :283-297
        if not nodes:
            return None
        elem = nodes[0]
        lis = _named(_below(elem), RDF, "li")
        text = None
        if lis:  # :323-337: getAttributeNS(xml, lang) must equal "x-default" exactly
            pick = next((li for li in lis if li.get(f"{{{XML_NS}}}lang") == "x-default"), lis[0])
            text = _text_content(pick)
        if text is None or _blank(text):
            text = _text_content(elem)
        return None if _blank(text) else _jtrim(text)

    def dc_list(local):
        values = []  # :300-319
        for elem in _named(_walk(root), DC, local):
            vals = _p4j_li_values(elem)
            if vals:
                values.extend(vals)
            else:
                text = _text_content(elem)
                if not _blank(text):
                    values.append(_jtrim(text))
        return values

    x.title, x.creators = first_dc_text("title"), dc_list("creator")
    x.description, x.subjects = first_dc_text("description"), dc_list("subject")
    x.publisher, x.language = first_dc_text("publisher"), first_dc_text("language")
    x.date, x.rights, x.identifiers = first_dc_text("date"), first_dc_text("rights"), dc_list("identifier")

    descriptions = _named(_walk(root), RDF, "Description")
    for desc in descriptions:  # :369-423 extractCalibreFields
        for name, value in desc.attrib.items():
            ns, local = _split(name)
            if ns == CALIBRE:
                x.calibre_fields[local] = value
        for child in _kids(desc):
            ns, local = _split(child.tag)
            if ns != CALIBRE:
                continue
            vals = _p4j_li_values(child)
            if vals:
                x.calibre_fields[local] = ",".join(vals)
                continue
            values = _named(_below(child), RDF, "value")
            if values:
                text = _text_content(values[0])
                if not _blank(text):
                    x.calibre_fields[local] = _jtrim(text)
            else:
                text = "".join(_text_children(child))  # :474-485 direct text only
                if not _blank(text):
                    x.calibre_fields[local] = _jtrim(text)
            si = _named(_below(child), CALIBRE_SI, "series_index")
            if si:
                text = _text_content(si[0])
                if not _blank(text):
                    x.calibre_fields["series_index"] = _jtrim(text)

    for desc in descriptions:  # :206-257 custom fields
        for name, value in desc.attrib.items():
            ns, local = _split(name)
            if ns is not None and ns not in _P4J_CORE:  # :217
                prefix = doc.attr_prefix(desc, name)
                # :223: attribute values are stored raw, even when blank.
                x.custom_fields[f"{prefix}:{local}" if prefix else local] = value
        for child in _kids(desc):
            ns, local = _split(child.tag)
            if ns is None or ns in (RDF, DC, PDFA_ID, CALIBRE, CALIBRE_SI):  # :234-236
                continue
            if ns == XMP and local == "Identifier":  # :237-238
                continue
            prefix = doc.prefix(child)
            key = f"{prefix}:{local}" if prefix else local  # :242-245 keyed by the PREFIX
            vals = _p4j_li_values(child)
            if vals:
                x.custom_list_fields[key] = vals
            else:
                # :251: TEXT_NODE children only; ElementTree folds CDATA into text, so
                # CDATA content (dropped upstream) is kept here.
                text = "".join(_text_children(child))
                if not _blank(text):
                    x.custom_fields[key] = _jtrim(text)

    for id_elem in _named(_walk(root), XMP, "Identifier"):  # :426-461
        for li in _named(_below(id_elem), RDF, "li"):
            scheme = li.get(f"{{{XMP_IDQ}}}Scheme", "")
            if _blank(scheme):
                nodes = _named(_below(li), XMP_IDQ, "Scheme")
                if nodes:
                    scheme = _text_content(nodes[0])
            values = _named(_below(li), RDF, "value")
            value = _text_content(values[0]) if values else ""
            if not _blank(scheme) and not _blank(value):
                x.xmp_identifiers.append((_jtrim(scheme), _jtrim(value)))
    return x


def _p4j_parse(xmp_bytes):
    """``XmpMetadataParser.parse(byte[])`` (:92-106) with its String retry (:118-134)."""
    if not xmp_bytes or len(xmp_bytes) > _P4J_MAX_XMP:
        return _GrimXmp()
    try:
        return _p4j_extract_doc(_parse_xml(xmp_bytes))
    except (ET.ParseError, XmlRefused):
        text = xmp_bytes.decode("utf-8", "replace")
    if _blank(text):
        return _GrimXmp()
    try:
        return _p4j_extract_doc(_parse_xml(_p4j_sanitize(text)))
    except (ET.ParseError, XmlRefused):
        return _GrimXmp()


class _GrimRaw:
    """Grimmory's ``RawXmpMetadata`` (PdfMetadataExtractor.java:381-459)."""

    def __init__(self, doc):
        self.doc = doc

    def _scalar(self, el):
        lis = _named(_below(el), RDF, "li")  # :413-437
        if lis:
            for li in lis:
                # :420 getAttribute("xml:lang") compared case-insensitively.
                if (li.get(f"{{{XML_NS}}}lang") or "").lower() == "x-default":
                    return self._text(li)
            return self._text(lis[0])
        values = _named(_below(el), RDF, "value")
        if values:
            return self._text(values[0])
        return self._text(el)

    @staticmethod
    def _text(el):
        t = _jtrim(_text_content(el))  # :450-458
        return t or None

    def first_text(self, ns, local):
        for el in _named(_walk(self.doc.root), ns, local):  # :383-394
            value = self._scalar(el)
            if value is not None:
                return value
        return None

    def list(self, ns, local):
        values = []  # :396-411
        for el in _named(_walk(self.doc.root), ns, local):
            for li in _named(_below(el), RDF, "li"):
                t = self._text(li)
                if t is not None:
                    values.append(t)
        out = []
        for v in (_jtrim(v) for v in values):
            if not _blank(v) and v not in out:
                out.append(v)
        return out


def _grim_date(pdf_date):
    # PdfMetadataExtractor.java:313-346 parsePdfDate: no time-zone conversion at all.
    if pdf_date is None or _blank(pdf_date):
        return None
    try:
        s = pdf_date.removeprefix("D:")
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", s):
            return _local_date(int(s[:4]), int(s[5:7]), int(s[8:10]))
        # :322-325: cut at '+', else '-' from index 8, else 'Z'. "2025-03-01T00:00:00Z"
        # becomes "2025-03-01T00:00:00", which no pattern below accepts.
        tz = s.find("+")
        if tz < 0:
            tz = s.find("-", 8)
        if tz < 0:
            tz = s.find("Z")
        if tz > 0:
            s = s[:tz]
        if re.fullmatch(r"[0-9]{8,}", s):
            return _local_date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        if re.fullmatch(r"[0-9]{6}", s):
            return _local_date(int(s[:4]), int(s[4:6]), 1)
        if re.fullmatch(r"[0-9]{4}", s):
            return _local_date(int(s[:4]), 1, 1)
    except ValueError:
        pass
    return None


def grimmory_view(data: bytes, *, filename: str | None = None) -> dict:
    """What Grimmory's ``PdfMetadataExtractor.extractMetadata`` builds from a PDF.

    Same keys as ``booklore_view`` plus ``xmp_source`` (the bytes PDFium4j hands
    its parser: a raw-byte scan of the file, see ``_p4j_fallback_xmp``),
    ``custom_fields``, ``custom_list_fields`` and ``flagged`` (local names
    several namespaces carried with one value). Raises ``GrimmoryAmbiguity``
    when they carry different values.
    """
    reader = _open(data)
    info = _info_dict(reader)
    out = {
        "title": None, "authors": [], "description": None, "publisher": None, "publishedDate": None,
        "pageCount": None, "categories": set(), "language": None, "seriesName": None, "seriesNumber": None,
        "seriesTotal": None, "subtitle": None, "isbn10": None, "isbn13": None, "googleId": None, "asin": None,
        "goodreadsId": None, "openlibraryId": None, "comicvineId": None, "ranobedbId": None,
        "lubimyczytacId": None, "hardcoverId": None, "hardcoverBookId": None, "applebooksId": None,
        "moods": set(), "tags": set(), "ratings": {},
    }

    def meta(key):
        # PdfDocument.java:575-580: an empty value is Optional.empty().
        return _p4j_info_text(info, key) or None

    title = meta("/Title")
    out["title"] = title if not _blank(title) else _base_name(filename)  # :73-78
    author = meta("/Author")
    if not _blank(author):  # :80-86
        authors = [_jtrim(a) for a in re.split(r"[,&]", author) if not _blank(_jtrim(a))]
        if authors:
            out["authors"] = authors
    subject = meta("/Subject")
    if not _blank(subject):
        out["description"] = subject
    publisher = meta("/EBX_PUBLISHER")
    if not _blank(publisher):
        out["publisher"] = publisher
    created = meta("/CreationDate")
    if not _blank(created):  # :98-104
        out["publishedDate"] = _grim_date(created)
    out["pageCount"] = len(reader.pages)  # :106
    keywords = meta("/Keywords")
    if not _blank(keywords):  # :108-123
        cats = _bl_split_list(keywords)
        if cats:
            out["categories"] = cats
    language = meta("/Language")
    if not _blank(language):
        out["language"] = language

    xmp_bytes = _p4j_xmp_bytes(data, info)
    out["xmp_source"] = xmp_bytes
    xmp = _p4j_parse(xmp_bytes)  # :130
    raw = None  # :131 parseRawXmp / :365-379: plain DOM parse of the same bytes, no retry
    if xmp_bytes:
        try:
            raw = _GrimRaw(_parse_xml(xmp_bytes))
        except (ET.ParseError, XmlRefused):
            raw = None

    def prefer(structured, ns, local):
        # :360-363 preferStructuredValue
        if structured is not None and not _blank(structured):
            return structured
        fallback = raw.first_text(ns, local) if raw else None
        return fallback if fallback is not None and not _blank(fallback) else None

    def custom(name):
        # :265-274 findCustomField: any namespace by local name, then PascalCase, then
        # the raw DOM in the booklore namespace only.
        value = xmp.find_field(name)
        if value is not None:
            return value
        pascal = name[:1].upper() + name[1:]
        value = xmp.find_field(pascal)
        if value is not None:
            return value
        if raw:
            return raw.first_text(BOOKLORE, name) or raw.first_text(BOOKLORE, pascal)
        return None

    def custom_list(name):
        values = xmp.find_list_field(name)  # :276-295
        if values:
            return values
        pascal = name[:1].upper() + name[1:]
        values = xmp.find_list_field(pascal)
        if values:
            return values
        if raw:
            return raw.list(BOOKLORE, name) or raw.list(BOOKLORE, pascal)
        return []

    for key, value, local in (  # :134-141
        ("title", xmp.title, "title"), ("description", xmp.description, "description"),
        ("publisher", xmp.publisher, "publisher"), ("language", xmp.language, "language"),
    ):
        chosen = prefer(value, DC, local)
        if chosen is not None:
            out[key] = chosen
    creators = xmp.creators or (raw.list(DC, "creator") if raw else [])  # :143-148
    if creators:
        out["authors"] = list(creators)
    # :150-152: dc:date wins over Info /CreationDate whenever parsePdfDate accepts it.
    date_text = prefer(xmp.date, DC, "date")
    if date_text is not None:
        parsed = _grim_date(date_text)
        if parsed is not None:
            out["publishedDate"] = parsed

    groups = {}
    for key in ("moods", "tags"):  # :155-167
        values = list(dict.fromkeys(custom_list(key)))
        if not values:
            text = custom(key)
            if text is not None:
                values = list(dict.fromkeys(_jtrim(p) for p in _java_split(text, ";") if not _blank(_jtrim(p))))
        groups[key] = values
        if values:
            out[key] = set(values)
    subjects = xmp.subjects or (raw.list(DC, "subject") if raw else [])  # :170-178
    if subjects:
        out["categories"] = set(subjects) - set(groups["moods"]) - set(groups["tags"])

    series = xmp.calibre_series()  # :181-182
    if series is not None:
        out["seriesName"] = series
    index = xmp.calibre_series_index()
    if index is not None:
        out["seriesNumber"] = _f32(index)  # Double.parseDouble then floatValue()
    else:  # :185-194: un-prefixed <series_index> found by regex in the raw packet
        m = re.search(r"<series_index>([^<]+)</series_index>", xmp_bytes.decode("utf-8", "replace"))
        if m:
            try:
                out["seriesNumber"] = _java_parse_float(_jtrim(m.group(1)))
            except ValueError:
                pass

    name = custom("seriesName")  # :197-203
    if name is not None:
        out["seriesName"] = name
    number = custom("seriesNumber")
    if number is not None:
        try:
            out["seriesNumber"] = _java_parse_float(number)
        except ValueError:
            pass
    total = custom("seriesTotal")
    if total is not None:
        try:
            out["seriesTotal"] = _java_int(total)
        except ValueError:
            pass
    subtitle = custom("subtitle")
    if subtitle is not None:
        out["subtitle"] = subtitle

    def clean_isbn(v):
        return re.sub(r"[^0-9Xx]", "", v)

    for name, key, fn in (  # :208-220
        ("isbn13", "isbn13", clean_isbn), ("isbn10", "isbn10", clean_isbn), ("googleId", "googleId", None),
        ("goodreadsId", "goodreadsId", None), ("openlibraryId", "openlibraryId", None), ("amazonId", "asin", None),
        ("asin", "asin", None), ("comicvineId", "comicvineId", None), ("ranobedbId", "ranobedbId", None),
        ("lubimyczytacId", "lubimyczytacId", None), ("hardcoverId", "hardcoverId", None),
        ("hardcoverBookId", "hardcoverBookId", None), ("applebooksId", "applebooksId", None),
    ):
        value = custom(name)
        if value is not None:
            out[key] = fn(value) if fn else value
    schemes = {
        "isbn13": "isbn13", "isbn10": "isbn10", "google": "googleId", "amazon": "asin", "asin": "asin",
        "amazonid": "asin", "openlibrary": "openlibraryId", "goodreads": "goodreadsId", "comicvine": "comicvineId",
        "ranobedb": "ranobedbId", "lubimyczytac": "lubimyczytacId", "hardcover": "hardcoverId",
        "hardcover_book_id": "hardcoverBookId", "applebooks": "applebooksId",
    }
    for scheme, value in xmp.xmp_identifiers:  # :223-246
        scheme = scheme.lower()
        if scheme == "isbn":
            cleaned = clean_isbn(value)
            out["isbn10" if len(cleaned) == 10 else "isbn13"] = cleaned
        elif scheme in schemes:
            out[schemes[scheme]] = clean_isbn(value) if scheme in ("isbn13", "isbn10") else value

    for name, fallback in (  # :249-255; mapRating finds e.g. calibre:rating or xmp:Rating too
        ("rating", "Rating"), ("amazonRating", "AmazonRating"), ("goodreadsRating", "GoodreadsRating"),
        ("hardcoverRating", "HardcoverRating"), ("lubimyczytacRating", "LubimyczytacRating"),
        ("ranobedbRating", "RanobedbRating"), ("applebooksRating", "ApplebooksRating"),
    ):
        value = custom(name)
        if value is None:
            value = custom(fallback)
        if value is not None:
            try:
                out["ratings"][name] = _java_parse_double(value)
            except ValueError:
                pass

    out["custom_fields"] = dict(xmp.custom_fields)
    out["custom_list_fields"] = dict(xmp.custom_list_fields)
    out["flagged"] = list(xmp.flagged)
    return out


# =========================================================================== strict xmpbox (JabRef)
def _props(spec):
    table = {}
    for item in spec.split():
        name, _, kind = item.partition("=")
        type_, _, card = kind.partition("/")
        table[name] = (type_, card or "Simple")
    return table


# TypeMapping.java:130-141 registers exactly these twelve schemas (URIs and property
# types from each class's @StructuredType / @PropertyType annotations at tag 3.0.8).
XMPBOX_SCHEMAS = {
    # XMPBasicSchema.java (blob dc79c51cdd32), prefix xmp
    "http://ns.adobe.com/xap/1.0/": _props(
        "Advisory=XPath/Bag BaseURL=URL CreateDate=Date CreatorTool=AgentName Identifier=Text/Bag "
        "Label=Text MetadataDate=Date ModifyDate=Date Nickname=Text Rating=Integer "
        "Thumbnails=Thumbnail/Alt ModifierDate=Date "
    ),
    # DublinCoreSchema.java (blob 0ad3c335b669), prefix dc
    "http://purl.org/dc/elements/1.1/": _props(
        "contributor=Text/Bag coverage=Text creator=Text/Seq date=Date/Seq description=LangAlt "
        "format=MIMEType identifier=Text language=Text/Bag publisher=Text/Bag relation=Text/Bag "
        "rights=LangAlt source=Text subject=Text/Bag title=LangAlt type=Text/Bag "
    ),
    # PDFAExtensionSchema.java (blob abb812788b02), prefix pdfaExtension
    "http://www.aiim.org/pdfa/ns/extension/": _props("schemas=PDFASchema/Bag"),
    # XMPMediaManagementSchema.java (blob b908e1a79d9f), prefix xmpMM
    "http://ns.adobe.com/xap/1.0/mm/": _props(
        "LastURL=URL RenditionOf=ResourceRef SaveID=Integer DerivedFrom=ResourceRef DocumentID=URI "
        "Manager=AgentName ManageTo=URI ManageUI=URI ManagerVariant=Text InstanceID=URI "
        "ManagedFrom=ResourceRef OriginalDocumentID=Text RenditionClass=RenditionClass "
        "RenditionParams=Text VersionID=Text Versions=Version/Seq History=ResourceEvent/Seq "
        "Ingredients=Text/Bag "
    ),
    # AdobePDFSchema.java (blob e474f445d663), prefix pdf
    "http://ns.adobe.com/pdf/1.3/": _props("Keywords=Text PDFVersion=Text Producer=Text"),
    # PDFAIdentificationSchema.java (blob 27bb2b2a64d5), prefix pdfaid
    "http://www.aiim.org/pdfa/ns/id/": _props("part=Integer amd=Text conformance=Text rev=Integer"),
    # XMPRightsManagementSchema.java (blob 1920b359b238), prefix xmpRights
    "http://ns.adobe.com/xap/1.0/rights/": _props(
        "Certificate=URL Marked=Boolean Owner=ProperName/Bag UsageTerms=LangAlt WebStatement=URL"
    ),
    # PhotoshopSchema.java (blob dc7c9aedc451), prefix photoshop
    "http://ns.adobe.com/photoshop/1.0/": _props(
        "AncestorID=URI AuthorsPosition=Text CaptionWriter=ProperName Category=Text City=Text "
        "ColorMode=Integer Country=Text Credit=Text DateCreated=Date DocumentAncestors=Text/Bag "
        "Headline=Text History=Text ICCProfile=Text Instructions=Text Source=Text State=Text "
        "SupplementalCategories=Text TextLayers=Layer/Seq TransmissionReference=Text Urgency=Integer "
    ),
    # XMPBasicJobTicketSchema.java (blob b0eba84d505a), prefix xmpBJ
    "http://ns.adobe.com/xap/1.0/bj/": _props("JobRef=Job/Bag"),
    # ExifSchema.java (blob 1db6b3b357ed), prefix exif
    "http://ns.adobe.com/exif/1.0/": _props(
        "UserComment=LangAlt ExifVersion=Text FlashpixVersion=Text ColorSpace=Integer "
        "ComponentsConfiguration=Integer/Seq CompressedBitsPerPixel=Rational PixelXDimension=Integer "
        "PixelYDimension=Integer RelatedSoundFile=Text DateTimeOriginal=Date DateTimeDigitized=Date "
        "ExposureTime=Rational FNumber=Rational ExposureProgram=Integer SpectralSensitivity=Text "
        "ISOSpeedRatings=Integer/Seq ShutterSpeedValue=Rational ApertureValue=Rational "
        "BrightnessValue=Rational ExposureBiasValue=Rational MaxApertureValue=Rational "
        "SubjectDistance=Rational MeteringMode=Integer LightSource=Integer FlashEnergy=Rational "
        "FocalLength=Rational FocalPlaneXResolution=Rational FocalPlaneYResolution=Rational "
        "SubjectArea=Integer/Seq FocalPlaneResolutionUnit=Integer SubjectLocation=Integer/Seq "
        "ExposureIndex=Rational SensingMethod=Integer FileSource=Integer SceneType=Integer "
        "CustomRendered=Integer WhiteBalance=Integer ExposureMode=Integer DigitalZoomRatio=Rational "
        "FocalLengthIn35mmFilm=Integer SceneCaptureType=Integer GainControl=Integer Contrast=Integer "
        "Saturation=Integer Sharpness=Integer SubjectDistanceRange=Integer ImageUniqueID=Text "
        "GPSVersionID=Text GPSSatellites=Text GPSStatus=Text GPSMeasureMode=Text GPSMapDatum=Text "
        "GPSSpeedRef=Text GPSTrackRef=Text GPSImgDirectionRef=Text GPSDestBearingRef=Text "
        "GPSDestDistanceRef=Text GPSProcessingMethod=Text GPSAreaInformation=Text GPSAltitude=Rational "
        "GPSDOP=Rational GPSSpeed=Rational GPSTrack=Rational GPSImgDirection=Rational "
        "GPSDestBearing=Rational GPSDestDistance=Rational GPSAltitudeRef=Integer GPSDifferential=Integer "
        "GPSTimeStamp=Date OECF=OECF SpatialFrequencyResponse=OECF GPSLatitude=GPSCoordinate "
        "GPSLongitude=GPSCoordinate GPSDestLatitude=GPSCoordinate GPSDestLongitude=GPSCoordinate "
        "CFAPattern=CFAPattern Flash=Flash CFAPatternType=CFAPattern DeviceSettingDescription=DeviceSettings "
    ),
    # TiffSchema.java (blob eaf57ad8c3a7), prefix tiff
    "http://ns.adobe.com/tiff/1.0/": _props(
        "ImageDescription=LangAlt Copyright=LangAlt Artist=ProperName ImageWidth=Integer "
        "ImageLength=Integer BitsPerSample=Integer/Seq Compression=Integer "
        "PhotometricInterpretation=Integer Orientation=Integer SamplesPerPixel=Integer "
        "PlanarConfiguration=Integer YCbCrSubSampling=Integer/Seq YCbCrPositioning=Integer "
        "XResolution=Rational YResolution=Rational ResolutionUnit=Integer TransferFunction=Integer/Seq "
        "WhitePoint=Rational/Seq PrimaryChromaticities=Rational/Seq YCbCrCoefficients=Rational/Seq "
        "ReferenceBlackWhite=Rational/Seq DateTime=Date Software=AgentName Make=ProperName "
        "Model=ProperName "
    ),
    # XMPageTextSchema.java (blob b0cfe18c22b1), prefix xmpTPg; :41-46 declare MaxPageSize and NPages
    # with no card (Simple), so a bare xmpTPg:NPages is a validated Integer, not an unknown property.
    "http://ns.adobe.com/xap/1.0/t/pg/": _props(
        "MaxPageSize=Dimensions NPages=Integer PlateNames=Text/Seq Colorants=Colorant/Seq Fonts=Font/Bag"
    ),
}
# Structured-type namespaces (TypeMapping.java:98-121, the @StructuredType of each type class).
_XMPBOX_STRUCTURED_NS = {
    "http://ns.adobe.com/xap/1.0/g/": ["Colorant"],
    "http://ns.adobe.com/xap/1.0/sType/Font#": ["Font"],
    "http://ns.adobe.com/photoshop/1.0/": ["Layer"],
    "http://ns.adobe.com/xap/1.0/g/img/": ["Thumbnail"],
    "http://ns.adobe.com/xap/1.0/sType/ResourceEvent#": ["ResourceEvent"],
    "http://ns.adobe.com/xap/1.0/sType/ResourceRef#": ["ResourceRef"],
    "http://ns.adobe.com/xap/1.0/sType/Version#": ["Version"],
    "http://www.aiim.org/pdfa/ns/schema#": ["PDFASchema"],
    "http://www.aiim.org/pdfa/ns/field#": ["PDFAField"],
    "http://www.aiim.org/pdfa/ns/property#": ["PDFAProperty"],
    "http://www.aiim.org/pdfa/ns/type#": ["PDFAType"],
    "http://ns.adobe.com/xap/1.0/sType/Job#": ["Job"],
    "http://ns.adobe.com/exif/1.0/": ["OECF", "CFAPattern", "DeviceSettings", "Flash"],
    "http://ns.adobe.com/xap/1.0/sType/Dimensions#": ["Dimensions"],
}
# Field names of the structured types that share a schema namespace (needed by
# getSpecifiedPropertyType's PDFBOX-6133 branch).
_XMPBOX_STRUCTURED_FIELDS = {
    "Layer": {"LayerName", "LayerText"},
    "OECF": {"Columns", "Names", "Rows", "Values"},
    "CFAPattern": {"Columns", "Rows", "Values"},
    "DeviceSettings": {"Columns", "Rows", "Settings"},
    "Flash": {"Fired", "Function", "RedEyeMode", "Mode", "Return"},
}
_XMPBOX_SIMPLE = {  # Types.java: simple == true
    "Text", "Date", "Boolean", "Integer", "Real", "GPSCoordinate", "ProperName", "Locale", "AgentName", "GUID",
    "XPath", "Part", "URL", "URI", "Choice", "MIMEType", "LangAlt", "RenditionClass", "Rational",
}


class _XmpError(Exception):
    """``XmpParsingException``: JabRef drops the Description being parsed."""

    def __init__(self, kind, message):
        super().__init__(f"{kind}: {message}")
        self.kind = kind


def _xmpbox_type(ns, local, parent_type=None):
    # TypeMapping.java:353-416 getSpecifiedPropertyType
    schema = XMPBOX_SCHEMAS.get(ns)
    if schema is not None and local in schema:
        return schema[local]
    types = _XMPBOX_STRUCTURED_NS.get(ns)
    if types is not None:
        if len(types) == 1:
            if schema is None or local in _XMPBOX_STRUCTURED_FIELDS.get(types[0], ()):
                return types[0], "Simple"
            return None
        for t in types:
            if t == parent_type:
                return t, "Simple"
        for t in types:
            if local in _XMPBOX_STRUCTURED_FIELDS.get(t, ()):
                return t, "Simple"
        return None
    if schema is not None:
        return None
    raise _XmpError("InvalidType", f"No descriptor found for {{{ns}}}{local}")


def _xmpbox_check(ns, local, parent_type=None):
    # DomXmpParser.java:1084-1107 checkPropertyDefinition. The nsFinder test can only
    # fail for an element or attribute with no namespace at all.
    if ns is None:
        raise _XmpError("NoSchema", f"Schema is not set in this document, property: {local}")
    if ns not in XMPBOX_SCHEMAS and ns not in _XMPBOX_STRUCTURED_NS:
        raise _XmpError("NoSchema", f"Cannot find a definition for the namespace {ns}, property: {local}")
    return _xmpbox_type(ns, local, parent_type)


_JAVA_LINE_END = "\n\r\u0085  "


def _xmpbox_iso_ok(date):
    # xmpbox DateConverter.java:311-323 fromISO8601: ZonedDateTime with a lenient offset,
    # resolved SMART (day clamped to the month), else a STRICT LocalDateTime.
    m = re.fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2})(?::(\d{2})(?:\.\d{1,9})?)?"
        r"([Zz]|[+-]\d{1,2}(?::?\d{2}(?::?\d{2})?)?)?",
        date,
        re.ASCII,
    )
    if not m:
        return False
    y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
    s = int(m.group(6) or 0)
    if not (1 <= mo <= 12 and 0 <= mi <= 59 and 0 <= s <= 59):
        return False
    if m.group(7):  # SMART: day 1-31 clamps, 24:00 means end of day
        offset = m.group(7)
        if offset not in "Zz":
            hours = int(re.match(r"[+-](\d{1,2})", offset).group(1))
            if hours > 18:
                return False
        return 1 <= d <= 31 and (h <= 23 or (h == 24 and mi == 0 and s == 0))
    try:
        _local_date(y, mo, d)
    except ValueError:
        return False
    return h <= 23


def _xmpbox_date_ok(date):
    """True when xmpbox ``DateConverter.toCalendar`` (76-205) would not throw."""
    if date is None or _jtrim(date) == "":
        return True  # returns null, which DateType.isGoodType accepts
    date = _jtrim(date)
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[^" + _JAVA_LINE_END + "]*", date):  # :94
        return _xmpbox_iso_ok(date)
    date = date.removeprefix("D:")
    pos_t = date.find("T")
    if pos_t not in (10, -1):  # :109-113
        return False
    date = re.sub(r"[-:T]", "", date)  # :115, which also eats a negative offset's '-'
    if len(date) < 4:
        return False
    try:
        _java_int(date[0:4])
        for lo, hi in ((4, 6), (6, 8), (8, 10), (10, 12)):
            if len(date) >= hi:
                _java_int(date[lo:hi])
        tz_pos = 12
        if len(date) == 14 or len(date) - 12 > 5 or (len(date) - 12 == 3 and date.endswith("Z")):
            _java_int(date[12:14])
            tz_pos = 14
        if len(date) >= tz_pos + 1:
            sign = date[tz_pos]
            if sign != "Z":
                if len(date) >= tz_pos + 3:
                    _java_int(date[tz_pos + 1:tz_pos + 3] if sign == "+" else date[tz_pos:tz_pos + 2])
                if sign == "+" and len(date) >= tz_pos + 5:
                    _java_int(date[tz_pos + 3:tz_pos + 5])
                elif sign != "+" and len(date) >= tz_pos + 4:
                    _java_int(date[tz_pos + 2:tz_pos + 4])
    except ValueError:
        return False
    return True


def _xmpbox_value(type_, value, where):
    # TypeMapping.instanciateSimpleProperty -> each type's setValue
    if type_ == "Date":
        ok = _xmpbox_date_ok(value)  # DateType.java:88-118
    elif type_ == "Integer":
        try:
            _java_int(value)  # IntegerType.java:83, no trim
            ok = True
        except ValueError:
            ok = False
    elif type_ == "Boolean":
        ok = _jtrim(value).upper() in ("TRUE", "FALSE")  # BooleanType.java:88
    elif type_ == "Real":
        try:
            _java_parse_float(value)  # RealType.java:84
            ok = True
        except ValueError:
            ok = False
    else:
        ok = True  # the Text family does no validation
    if not ok:
        raise _XmpError("Format", f"value {value!r} is not a valid {type_} in {where}")


def _xmpbox_dom_children(el):
    """Child nodes as DomXmpParser sees them: comments dropped, text merged."""
    out, buf = [], el.text or ""
    for child in el:
        if child.tag is ET.Comment:
            buf += child.tail or ""
            continue
        if buf:
            out.append(("text", buf))
        out.append(("elem" if isinstance(child.tag, str) else "pi", child))
        buf = child.tail or ""
    if buf:
        out.append(("text", buf))
    return out


class _XmpboxState:
    def __init__(self):
        self.dc = None  # the DublinCore schema container: first property per name wins

    def add(self, ns, local, value):
        if ns == DC:
            if self.dc is None:
                self.dc = {}
            self.dc.setdefault(local, value)  # XMPSchema.getAbstractProperty returns the first


def _xmpbox_parse(data):
    """``DomXmpParser.parse`` with strictParsing=true (DomXmpParser.java:129-242).

    Returns the DublinCore properties ({local: value}) or None when the packet
    has no DC schema; raises ``_XmpError`` where xmpbox throws.
    """
    try:
        doc = _parse_xml(data)
    except (ET.ParseError, XmlRefused) as exc:  # :136-141, disallow-doctype-decl at :83
        raise _XmpError("Undefined", f"Failed to parse: {exc}") from exc
    nodes, i = doc.nodes, 0
    if not nodes or nodes[0][0] != "pi":  # :150-155
        raise _XmpError("XpacketBadStart", "xmp should start with a processing instruction")
    _xmpbox_xpacket_begin(nodes[0])
    i = 1
    while i < len(nodes) and nodes[i][0] == "pi":  # :166-170
        i += 1
    if i >= len(nodes) or nodes[i][0] != "root":
        raise _XmpError("NoRootElement", "xmp should contain a root element")
    root = nodes[i][1]
    i += 1
    if i >= len(nodes):  # :184-189
        raise _XmpError("XpacketBadEnd", "xmp should end with a processing instruction")
    _xmpbox_xpacket_end(nodes[i])
    if i + 1 < len(nodes):  # :198-202
        raise _XmpError("XpacketBadEnd", "xmp should end after xpacket end processing instruction")
    rdf = _xmpbox_descriptions_parent(doc, root)
    descriptions = _kids(rdf)  # :223: every element child, rdf:Description or not
    for desc in descriptions:
        if any(doc.prefix(c) == "pdfaExtension" for c in _kids(desc)):  # :261-264 by prefix
            raise OracleLimitation("PDF/A extension schema definitions are not ported")
    state = _XmpboxState()
    for desc in descriptions:
        _xmpbox_description(doc, desc, state)
    return state.dc


def _xmpbox_xpacket_begin(node):
    # DomXmpParser.java:864-919 parseInitialXpacket
    _, target, data = node
    if target != "xpacket":
        raise _XmpError("XpacketBadStart", f"Bad processing instruction name : {target}")
    for token in data.split(" "):  # StringTokenizer(data, " ")
        if not token:
            continue
        if not token.endswith(('"', "'")):
            raise _XmpError("XpacketBadStart", f"Cannot understand PI data part : {token!r}")
        pos = token.find("=" + token[-1])
        if pos <= 0 or len(token) - 1 < pos + 2:
            raise _XmpError("XpacketBadStart", f"Cannot understand PI data part : {token!r}")
        if token[:pos] not in ("id", "begin", "bytes", "encoding"):
            raise _XmpError("XpacketBadStart", f"Unknown attribute in xpacket PI : {token!r}")


def _xmpbox_xpacket_end(node):
    # DomXmpParser.java:921-947 parseEndPacket: the PI target is never checked.
    if node[0] != "pi":
        raise _XmpError("XpacketBadEnd", "xmp should end with a processing instruction")
    data = node[2]
    if not data.startswith("end="):
        raise _XmpError("XpacketBadEnd", "Expected xpacket 'end' attribute (must be present and placed in first)")
    if len(data) <= 5:
        # :929 charAt(5) throws StringIndexOutOfBoundsException, which JabRef does not catch.
        raise OracleLimitation("upstream StringIndexOutOfBoundsException in parseEndPacket")
    if data[5] not in "rw":
        raise _XmpError("XpacketBadEnd", "Expected xpacket 'end' attribute with value 'r' or 'w' ")


def _xmpbox_expect(doc, el, ns, prefix, local):
    # DomXmpParser.java:996-1013 expectNaming: namespace, then the literal prefix, then name.
    el_ns, el_local = _split(el.tag)
    if el_ns != ns:
        raise _XmpError("Format", f"Expecting namespace '{ns}' and found '{el_ns}'")
    if doc.prefix(el) != prefix:
        raise _XmpError("Format", f"Expecting prefix '{prefix}' and found '{doc.prefix(el)}'")
    if el_local != local:
        raise _XmpError("Format", f"Expecting local name '{local}' and found '{el_local}'")


def _xmpbox_descriptions_parent(doc, root):
    # DomXmpParser.java:949-994; blanks removed by removeCommentsAndBlanks (1020-1058)
    # except in an element with a single child node (1027).
    if _split(root.tag)[0] != RDF:
        _xmpbox_expect(doc, root, "adobe:ns:meta/", "x", "xmpmeta")
        nodes = _xmpbox_dom_children(root)
        if len(nodes) > 1:
            nodes = [n for n in nodes if not (n[0] == "text" and _jtrim(n[1]) == "")]
        if not nodes:
            raise _XmpError("Format", "No rdf description found in xmp")
        if len(nodes) > 1:
            raise _XmpError("Format", "More than one element found in x:xmpmeta")
        if nodes[0][0] != "elem":
            raise _XmpError("Format", "x:xmpmeta does not contains rdf:RDF element")
        rdf = nodes[0][1]
    else:
        rdf = root
    _xmpbox_expect(doc, rdf, RDF, "rdf", "RDF")
    return rdf


def _xmpbox_description(doc, desc, state):
    # DomXmpParser.java:305-344 parseDescriptionRoot
    for name, value in desc.attrib.items():
        ns, local = _split(name)
        prefix = doc.attr_prefix(desc, name) if ns else None
        # :317-318: `about && prefix == null || "rdf".equals(prefix)`; xmlns attributes
        # (:322-327) never reach ElementTree's attrib.
        if (local == "about" and prefix is None) or prefix == "rdf":
            continue
        if ns not in XMPBOX_SCHEMAS:
            continue  # :351-357: no schema -> silently ignored, even when strict
        type_ = _xmpbox_check(ns, local)
        if type_ is None:  # :363-369
            raise _XmpError("InvalidType", f"No type defined for {{{ns}}}{local}")
        if type_[0] not in _XMPBOX_SIMPLE or type_[1] != "Simple" or type_[0] == "LangAlt":  # :373-380
            raise _XmpError("InvalidType", f"'{local}' is a structured or array type, but attributes are simple")
        _xmpbox_value(type_[0], value, local)
        state.add(ns, local, ("simple", value))
    for prop in _kids(desc):  # :404-434 parseChildrenAsProperties
        ns, local = _split(prop.tag)
        type_ = _xmpbox_check(ns, local)  # :412
        if ns not in XMPBOX_SCHEMAS:  # :414-418
            raise _XmpError("NoSchema", f"This namespace is not from a schema: {ns}")
        if doc.prefix(prop) == "pdfaExtension":
            continue  # :419-422
        _xmpbox_create(doc, prop, type_, state)


def _xmpbox_create(doc, prop, type_, state):
    # DomXmpParser.java:436-485 createProperty
    ns, local = _split(prop.tag)
    if type_ is None:
        raise _XmpError("InvalidType", f"No type defined for {{{ns}}}{local}")
    kind, card = type_
    if kind == "LangAlt":
        value = _xmpbox_array(doc, prop, "LangAlt", "Alt")  # :456-458
    elif card != "Simple":
        value = _xmpbox_array(doc, prop, kind, card)
    elif kind in _XMPBOX_SIMPLE:
        text = _text_content(prop)  # :557-567 manageSimpleType
        _xmpbox_value(kind, text, local)
        value = ("simple", text)
    else:
        raise OracleLimitation(f"xmpbox structured type {kind} ({local}) is not ported")
    state.add(ns, local, value)


def _xmpbox_unique_child(el):
    kids = _kids(el)  # DomHelper.java:42-61 getUniqueElementChild
    if len(kids) > 1:
        raise _XmpError("Undefined", f"Found two child elements in {el.tag}")
    return kids[0] if kids else None


def _xmpbox_array(doc, prop, item_type, card):
    # DomXmpParser.java:569-626 manageArray
    container = _xmpbox_unique_child(prop)
    if container is None:
        raise _XmpError("Format", f"Invalid array definition, expecting {card} [{_split(prop.tag)[1]}]")
    if _split(container.tag)[1] != card:  # :607-612 compares the local name only
        raise _XmpError("Format", f"Invalid array type, expecting {card} and found {_split(container.tag)[1]}")
    items = []
    for li in _kids(container):  # any element child, rdf:li or not
        item = _xmpbox_li(doc, li, item_type)
        if item is not None:
            items.append(item)
    return ("array", items)


def _xmpbox_li(doc, li, item_type):
    # DomXmpParser.java:657-719 parseLiElement
    if li.get(f"{{{RDF}}}parseType") == "Resource":
        return _xmpbox_li_description(doc, li, _split(li.tag)[1])
    child = _xmpbox_unique_child(li)
    if child is not None:
        return _xmpbox_li_description(doc, child, _split(li.tag)[1])
    text = _text_content(li)
    if item_type in _XMPBOX_SIMPLE:
        _xmpbox_value(item_type, text, _split(li.tag)[1])
        return (text, li.get(f"{{{XML_NS}}}lang"))  # loadAttributes keeps xml:* attributes
    raise OracleLimitation(f"xmpbox structured array item ({item_type}) is not ported")


def _xmpbox_li_description(doc, el, parent_local):
    # DomXmpParser.java:751-862 parseLiDescription, up to where a structured type would
    # be instantiated (that part is not ported).
    kids = _kids(el)
    if not kids:  # :757-760 -> tryParseAttributesAsProperties (1124-1197)
        for name in el.attrib:
            ns, local = _split(name)
            prefix = doc.attr_prefix(el, name) if ns else None
            if ns == XML_NS or prefix == "rdf" or ns is None:
                continue
            _xmpbox_check(ns, local)
            raise OracleLimitation("xmpbox structured values given as attributes are not ported")
        return None
    first = kids[0]
    if doc.qname(first) == "rdf:Description":  # :762, tag name including the prefix
        return _xmpbox_li_description(doc, first, parent_local)
    ns, local = _split(first.tag)
    if _xmpbox_check(ns, local, parent_local) is None:
        raise _XmpError("NoType", f"Property '{local}' not defined in {ns}")
    raise OracleLimitation(f"xmpbox structured type in {ns} is not ported")


def _xmpbox_dc(props):
    """DublinCoreSchema getters as JabRef calls them."""

    def lang_alt(name):
        # XMPSchema.java:1066-1088: only an xml:lang exactly "x-default" counts.
        value = props.get(name)
        if not value or value[0] != "array":
            return None
        return next((text for text, lang in value[1] if lang == "x-default"), None)

    def array(name):
        value = props.get(name)
        return [text for text, _ in value[1]] if value and value[0] == "array" else []

    return {
        "title": lang_alt("title"),
        "creators": array("creator"),
        "subjects": array("subject"),
        # DublinCoreSchemaCustom.java:47-55: the raw date strings, not normalised.
        "dates": array("date"),
        "description": lang_alt("description"),
    }


def strict_xmp_view(xmp: bytes, *, pdfdoc_decoding: bool = True) -> dict:
    """What JabRef reads from an XMP packet through strict xmpbox 3.0.8.

    JabRef (XmpUtilReader.java:122-165) takes the stream as text, cuts it at the
    first ``<rdf:Description`` and the last ``</rdf:Description>``, splits the
    middle on ``</rdf:Description>`` and parses each piece wrapped in the
    original head and tail with a strict ``DomXmpParser``. A piece that throws is
    dropped and the rest continue. Returns ``{"descriptions": [{"kept", "error",
    "dc"}...], "dc": [...]}`` with one ``dc`` dict (title, creators, subjects,
    dates, description) per kept piece that has DublinCore properties.

    ``pdfdoc_decoding`` mirrors ``COSStream.toTextString`` (COSStream.java:363-376),
    which decodes a BOM-less stream as PDFDocEncoding, and ``String.getBytes()``
    (UTF-8 on JDK 18+): UTF-8 text comes back as mojibake. Pass False to model a
    plain xmpbox caller that hands the parser the bytes unchanged.
    """
    text = _pdfbox_string(xmp) if pdfdoc_decoding else xmp.decode("utf-8", "replace")
    start_tag, end_tag = "<rdf:Description", "</rdf:Description>"  # :34-35
    start = text.find(start_tag)
    end = text.rfind(end_tag) + len(end_tag)
    out = {"descriptions": [], "dc": []}
    if start < 0 or start > end or end == len(end_tag) - 1:  # :140-143
        return out
    head, middle, tail = text[:start], text[start:end], text[end:]
    for piece in _java_split(middle, end_tag):  # :148
        data = (head + piece + end_tag + tail).encode("utf-8", "replace")  # :159 getBytes()
        try:
            props = _xmpbox_parse(data)
        except _XmpError as exc:  # XmpUtilShared.java:38-41 -> IOException, caught at :160-162
            out["descriptions"].append({"kept": False, "error": str(exc), "dc": None})
            continue
        dc = _xmpbox_dc(props) if props is not None else None
        out["descriptions"].append({"kept": True, "error": None, "dc": dc})
        if dc is not None:
            out["dc"].append(dc)
    return out
