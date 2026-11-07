"""Test oracle: what BookOrbit would read from a PDF.

A Python re-implementation of the parts of pdf-lib 1.17.1 that BookOrbit's
``pdf-parser.ts`` relies on (``PDFDocument.load``'s forward pass, where the last
object definition in byte order wins and an xref-stream trailer replaces the
trailer info wholesale; catalog/Info lookup; ``extractXmpXml``), plus
BookOrbit's ``parseXmp`` / ``parsePdfBuffer`` field mapping (literal prefixes,
first ``rdf:Description`` wins, attribute-form properties ignored, XMP beats the
Info dict). Ported from bookorbit/bookorbit ``server/src/modules/metadata/lib``.

It is deliberately independent of ``magsync.core.pdfmeta`` so tests do not
check the stamper with its own reader.
"""
import re
import zlib
from xml.parsers import expat

WS = set(b"\x00\t\n\x0c\r ")
DELIM = set(b"()<>[]{}/%")
DIGIT = set(b"0123456789")
NUMERIC = set(b"0123456789+-.")


class Ref(tuple):
    def __new__(cls, n, g):
        return super().__new__(cls, (n, g))

    def __repr__(self):
        return f"{self[0]} {self[1]} R"


class Name(str):
    pass


class PStr:
    def __init__(self, v):
        self.v = v  # raw bytes between parens (escapes unprocessed)

    def as_bytes(self):
        out = bytearray()
        octal = ""
        esc = False
        v = self.v
        for i, b in enumerate(v):
            nxt = v[i + 1] if i + 1 < len(v) else None
            if not esc:
                if b == 0x5C:
                    esc = True
                else:
                    out.append(b)
            else:
                if b in (0x0A, 0x0D):
                    esc = False
                elif b == ord("n"):
                    out.append(0x0A); esc = False
                elif b == ord("r"):
                    out.append(0x0D); esc = False
                elif b == ord("t"):
                    out.append(0x09); esc = False
                elif b == ord("b"):
                    out.append(0x08); esc = False
                elif b == ord("f"):
                    out.append(0x0C); esc = False
                elif b in (0x28, 0x29):
                    out.append(b); esc = False
                elif 0x30 <= b <= 0x37:
                    octal += chr(b)
                    if len(octal) == 3 or not (nxt is not None and 0x30 <= nxt <= 0x37):
                        out.append(int(octal, 8) & 0xFF)
                        octal = ""
                        esc = False
                else:
                    out.append(b); esc = False
        return bytes(out)


class HStr:
    def __init__(self, v):
        self.v = v

    def as_bytes(self):
        h = re.sub(rb"\s", b"", self.v)
        if len(h) % 2:
            h += b"0"
        return bytes.fromhex(h.decode("ascii", "replace"))


PDFDOC = {i: i for i in range(256)}
PDFDOC.update({0x16: 0x17, 0x18: 0x02D8, 0x19: 0x02C7, 0x1A: 0x02C6, 0x1B: 0x02D9, 0x1C: 0x02DD, 0x1D: 0x02DB,
               0x1E: 0x02DA, 0x1F: 0x02DC, 0x7F: 0xFFFD, 0x80: 0x2022, 0x81: 0x2020, 0x82: 0x2021, 0x83: 0x2026,
               0x84: 0x2014, 0x85: 0x2013, 0x86: 0x0192, 0x87: 0x2044, 0x88: 0x2039, 0x89: 0x203A, 0x8A: 0x2212,
               0x8B: 0x2030, 0x8C: 0x201E, 0x8D: 0x201C, 0x8E: 0x201D, 0x8F: 0x2018, 0x90: 0x2019, 0x91: 0x201A,
               0x92: 0x2122, 0x93: 0xFB01, 0x94: 0xFB02, 0x95: 0x0141, 0x96: 0x0152, 0x97: 0x0160, 0x98: 0x0178,
               0x99: 0x017D, 0x9A: 0x0131, 0x9B: 0x0142, 0x9C: 0x0153, 0x9D: 0x0161, 0x9E: 0x017E, 0x9F: 0xFFFD,
               0xA0: 0x20AC, 0xAD: 0xFFFD})


def decode_text(s):
    b = s.as_bytes()
    if b[:2] == b"\xfe\xff":
        return b[2:].decode("utf-16-be", "replace")
    if b[:2] == b"\xff\xfe":
        return b[2:].decode("utf-16-le", "replace")
    return "".join(chr(PDFDOC[x]) for x in b)


class Stream:
    def __init__(self, d, contents):
        self.dict = d
        self.contents = contents


class Invalid:
    pass


class ParseErr(Exception):
    pass


class Parser:
    def __init__(self, data):
        self.b = data
        self.i = 0
        self.n = len(data)
        self.objects = {}
        self.order = []  # (ref, file_offset, origin) for reporting
        self.trailer = {}
        self.trailer_events = []

    # ---- BaseParser primitives
    def done(self):
        return self.i >= self.n

    def peek(self, k=0):
        j = self.i + k
        return self.b[j] if j < self.n else -1

    def skip_ws(self):
        while self.i < self.n and self.b[self.i] in WS:
            self.i += 1

    def skip_comment(self):
        if self.peek() != 0x25:
            return False
        while self.i < self.n:
            if self.b[self.i] in (0x0A, 0x0D):
                return True
            self.i += 1
        return True

    def skip_ws_c(self):
        self.skip_ws()
        while self.skip_comment():
            self.skip_ws()

    def match(self, kw):
        if self.b.startswith(kw, self.i):
            self.i += len(kw)
            return True
        return False

    def raw_int(self):
        s = self.i
        while self.i < self.n and self.b[self.i] in DIGIT:
            self.i += 1
        if s == self.i:
            raise ParseErr("int")
        return int(self.b[s:self.i])

    def raw_number(self):
        s = self.i
        while self.i < self.n and self.b[self.i] in NUMERIC:
            c = self.b[self.i]
            self.i += 1
            if c == 0x2E:
                break
        while self.i < self.n and self.b[self.i] in DIGIT:
            self.i += 1
        txt = self.b[s:self.i].decode()
        try:
            return float(txt) if "." in txt else int(txt)
        except ValueError:
            raise ParseErr("num " + txt)

    # ---- PDFObjectParser
    def parse_object(self):
        self.skip_ws_c()
        if self.match(b"true"):
            return True
        if self.match(b"false"):
            return False
        if self.match(b"null"):
            return None
        c = self.peek()
        if c == 0x3C and self.peek(1) == 0x3C:
            return self.parse_dict_or_stream()
        if c == 0x3C:
            self.i += 1
            s = self.i
            while self.i < self.n and self.b[self.i] != 0x3E:
                self.i += 1
            v = self.b[s:self.i]
            if self.peek() != 0x3E:
                raise ParseErr("hex")
            self.i += 1
            return HStr(v)
        if c == 0x28:
            return self.parse_string()
        if c == 0x2F:
            return self.parse_name()
        if c == 0x5B:
            self.i += 1
            self.skip_ws_c()
            arr = []
            while self.peek() != 0x5D:
                if self.done():
                    raise ParseErr("array")
                arr.append(self.parse_object())
                self.skip_ws_c()
            self.i += 1
            return arr
        if c in NUMERIC:
            first = self.raw_number()
            self.skip_ws_c()
            la = self.i
            if self.peek() in DIGIT:
                second = self.raw_number()
                self.skip_ws_c()
                if self.peek() == ord("R"):
                    self.i += 1
                    return Ref(first, second)
            self.i = la
            return first
        raise ParseErr(f"obj byte {c} at {self.i}")

    def parse_string(self):
        nest = 0
        esc = False
        s = self.i
        while self.i < self.n:
            b = self.b[self.i]
            self.i += 1
            if not esc:
                if b == 0x28:
                    nest += 1
                if b == 0x29:
                    nest -= 1
            if b == 0x5C:
                esc = not esc
            elif esc:
                esc = False
            if nest == 0:
                return PStr(self.b[s + 1:self.i - 1])
        raise ParseErr("paren")

    def parse_name(self):
        if self.peek() != 0x2F:
            raise ParseErr("name")
        self.i += 1
        s = self.i
        while self.i < self.n and self.b[self.i] not in WS and self.b[self.i] not in DELIM:
            self.i += 1
        return Name(self.b[s:self.i].decode("latin-1"))

    def parse_dict(self):
        if not self.match(b"<<"):
            raise ParseErr("dict")
        self.skip_ws_c()
        d = {}
        while not self.done() and self.peek() != 0x3E and self.peek(1) != 0x3E:
            k = self.parse_name()
            v = self.parse_object()
            d[k] = v
            self.skip_ws_c()
        self.skip_ws_c()
        if not self.match(b">>"):
            raise ParseErr("dict end")
        return d

    END_RE = re.compile(rb"stream|\r\nendstream|\rendstream|\nendstream|endstream")

    def fallback_end(self):
        nest = 1
        pos = self.i
        while True:
            m = self.END_RE.search(self.b, pos)
            if not m:
                raise ParseErr("stream fallback")
            if m.group(0) == b"stream":
                nest += 1
            else:
                nest -= 1
            pos = m.end()
            if nest == 0:
                self.i = m.end()
                return m.start()

    def parse_dict_or_stream(self):
        d = self.parse_dict()
        self.skip_ws_c()
        if not (self.match(b"stream \r\n") or self.match(b"stream\r\n") or self.match(b"stream\r")
                or self.match(b"stream\n") or self.match(b"stream")):
            return d
        start = self.i
        L = d.get("Length")
        if isinstance(L, (int, float)) and not isinstance(L, bool):
            end = start + int(L)
            self.i = end
            self.skip_ws_c()
            if not self.match(b"endstream"):
                self.i = start
                end = self.fallback_end()
        else:
            end = self.fallback_end()
        return Stream(d, self.b[start:end])

    # ---- PDFParser
    def obj_header(self):
        self.skip_ws_c()
        n = self.raw_int()
        self.skip_ws_c()
        g = self.raw_int()
        self.skip_ws_c()
        if not self.match(b"obj"):
            raise ParseErr("obj kw")
        return Ref(n, g)

    def match_obj_header(self):
        s = self.i
        try:
            self.obj_header()
            return True
        except ParseErr:
            self.i = s
            return False

    def lookup(self, x):
        if isinstance(x, Ref):
            return self.objects.get(x)
        return x

    def assign(self, ref, obj, origin):
        prev = ref in self.objects
        self.objects[ref] = obj
        self.order.append((ref, origin, prev))

    def parse_indirect_object(self):
        off = self.i
        ref = self.obj_header()
        self.skip_ws_c()
        obj = self.parse_object()
        self.skip_ws_c()
        self.match(b"endobj")
        if isinstance(obj, Stream) and self.lookup(obj.dict.get("Type")) == "ObjStm":
            data = decode_stream(obj)
            first = int(obj.dict["First"])
            count = int(obj.dict["N"])
            sub = Parser(data)
            pairs = []
            for _ in range(count):
                sub.skip_ws_c()
                on = sub.raw_int()
                sub.skip_ws_c()
                o = sub.raw_int()
                pairs.append((on, o))
            for on, o in pairs:
                sub.i = first + o
                child = sub.parse_object()
                self.assign(Ref(on, 0), child, f"objstm {ref} @file {off}")
        elif isinstance(obj, Stream) and self.lookup(obj.dict.get("Type")) == "XRef":
            d = obj.dict
            self.trailer = {"Root": d.get("Root"), "Encrypt": d.get("Encrypt"), "Info": d.get("Info"),
                            "ID": d.get("ID")}
            self.trailer_events.append(("xref-stream", off, dict(self.trailer)))
        else:
            self.assign(ref, obj, f"file @{off}")
        return ref

    def try_invalid(self):
        ref = self.obj_header()
        self.skip_ws_c()
        k = self.b.find(b"endobj", self.i)
        if k < 0:
            raise ParseErr("invalid obj")
        self.i = k + 6
        self.assign(ref, Invalid(), "invalid")

    def parse_indirect_objects(self):
        self.skip_ws_c()
        while not self.done() and self.peek() in DIGIT:
            s = self.i
            try:
                self.parse_indirect_object()
            except ParseErr:
                self.i = s
                self.try_invalid()
            self.skip_ws_c()
            self.skip_jibberish()

    def maybe_xref(self):
        self.skip_ws_c()
        if not self.match(b"xref"):
            return
        self.skip_ws_c()
        while not self.done() and self.peek() in DIGIT:
            self.raw_int()
            self.skip_ws_c()
            self.raw_int()
            self.skip_ws_c()
            if self.peek() in (ord("n"), ord("f")):
                self.i += 1
            self.skip_ws_c()

    def maybe_trailer_dict(self):
        self.skip_ws_c()
        if not self.match(b"trailer"):
            return
        self.skip_ws_c()
        off = self.i
        d = self.parse_dict()
        t = self.trailer
        self.trailer = {k: d.get(k) or t.get(k) for k in ("Root", "Encrypt", "Info", "ID")}
        self.trailer_events.append(("classic-trailer", off, dict(self.trailer)))

    def maybe_startxref(self):
        self.skip_ws_c()
        if not self.match(b"startxref"):
            return
        self.skip_ws_c()
        self.raw_int()
        self.skip_ws()
        self.match(b"%%EOF")
        self.skip_ws_c()
        self.match(b"%%EOF")
        self.skip_ws_c()

    def skip_jibberish(self):
        self.skip_ws_c()
        while not self.done():
            s = self.i
            c = self.b[self.i]
            if 0x20 <= c <= 0x7E and (
                self.match(b"xref") or self.match(b"trailer") or self.match(b"startxref") or self.match_obj_header()
            ):
                self.i = s
                break
            self.i += 1

    def parse_document(self):
        k = self.b.find(b"%PDF-")
        if k < 0:
            raise ParseErr("no header")
        self.i = k + 5
        self.raw_int(); self.i += 1; self.raw_int()
        # skipBinaryHeaderComment
        self.skip_ws_c()
        s = self.i
        try:
            self.obj_header()
            self.i = s
        except ParseErr:
            self.i = s + 1
            self.skip_ws_c()
        prev = None
        while not self.done():
            self.parse_indirect_objects()
            self.maybe_xref()
            self.maybe_trailer_dict()
            self.maybe_startxref()
            self.skip_jibberish()
            if self.i == prev:
                raise ParseErr("stalled")
            prev = self.i
        # maybeRecoverRoot
        cat = self.lookup(self.trailer.get("Root"))
        if not (isinstance(cat, dict) and self.lookup(cat.get("Type")) == "Catalog"):
            for ref in sorted(self.objects, key=lambda r: r[0]):
                o = self.objects[ref]
                if isinstance(o, dict) and self.lookup(o.get("Type")) == "Catalog":
                    self.trailer["Root"] = ref
        self.objects.pop(Ref(0, 0), None)


def decode_stream(s):
    f = s.dict.get("Filter")
    if f is None:
        return s.contents
    filters = f if isinstance(f, list) else [f]
    data = s.contents
    for flt in filters:
        if flt == "FlateDecode":
            d = zlib.decompressobj()
            data = d.decompress(data)
        else:
            raise ParseErr("unsupported filter " + str(flt))
    return data


# ---------------- fast-xml-parser (v5) shape emulation for the options BookOrbit uses -------------
ARRAY_TAGS = {"rdf:li", "rdf:Description"}


def fxp_parse(xml_text):
    p = expat.ParserCreate()  # no namespace processing -> literal prefixed names, like fast-xml-parser
    root = {"children": [], "attrs": {}, "text": ""}
    stack = [root]

    def start(name, attrs):
        node = {"name": name, "children": [], "attrs": attrs, "text": ""}
        stack[-1]["children"].append(node)
        stack.append(node)

    def end(name):
        stack.pop()

    def chars(data):
        stack[-1]["text"] += data

    p.StartElementHandler = start
    p.EndElementHandler = end
    p.CharacterDataHandler = chars
    p.Parse(xml_text.encode("utf-8"), True)

    def conv(node):
        text = node["text"].strip()
        if not node["children"] and not node["attrs"]:
            return text
        obj = {}
        for k, v in node["attrs"].items():
            obj["@_" + k] = v
        for ch in node["children"]:
            val = conv(ch)
            nm = ch["name"]
            if nm in ARRAY_TAGS:
                obj.setdefault(nm, []).append(val)
            elif nm in obj:
                if not isinstance(obj[nm], list):
                    obj[nm] = [obj[nm]]
                obj[nm].append(val)
            else:
                obj[nm] = val
        if text:
            obj["#text"] = text
        return obj

    out = {}
    for ch in root["children"]:
        out[ch["name"]] = conv(ch)
    return out


def get_text(val):
    if isinstance(val, str):
        return val.strip() or None
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, dict):
        alt = val.get("rdf:Alt")
        if alt is not None:
            lis = (alt.get("rdf:li") if isinstance(alt, dict) else None) or []
            d = next((l for l in lis if isinstance(l, dict) and l.get("@_xml:lang") == "x-default"), None)
            pick = d if d is not None else (lis[0] if lis else None)
            if not pick:
                return None
            return (pick.strip() or None) if isinstance(pick, str) else get_text(pick.get("#text"))
        cont = val.get("rdf:Seq") or val.get("rdf:Bag")
        if cont is not None:
            lis = (cont.get("rdf:li") if isinstance(cont, dict) else None) or []
            return get_text(lis[0]) if lis else None
        return get_text(val.get("#text"))
    return None


def get_list(val):
    if not val:
        return []
    if isinstance(val, str):
        return [val.strip()] if val.strip() else []
    if isinstance(val, dict):
        cont = val.get("rdf:Seq") or val.get("rdf:Bag") or val.get("rdf:Alt")
        if cont is not None:
            lis = (cont.get("rdf:li") if isinstance(cont, dict) else None) or []
            return [x for x in ((l.strip() if isinstance(l, str) else (get_text(l) or "")) for l in lis) if x]
    return []


def split_list_text(val):
    t = get_text(val)
    if not t:
        return []
    return [s.strip() for s in re.split(r"[,;]", t) if s.strip()]


DATE_KEY_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")


def is_year(y):
    return 1000 <= y <= 2200


def is_date_key(v):
    m = DATE_KEY_RE.match(v)
    if not m:
        return False
    import datetime
    y, mo, d = map(int, m.groups())
    if not is_year(y):
        return False
    try:
        datetime.date(y, mo, d)
        return True
    except ValueError:
        return False


def parse_published_year(v):
    if not isinstance(v, str):
        return None
    m = re.search(r"(?:^|\D)(\d{4})(?:\D|$)", v.strip())
    if not m:
        return None
    y = int(m.group(1))
    return y if is_year(y) else None


def parse_published_date_key(v):
    if not isinstance(v, str):
        return None
    t = v.strip()
    if not t:
        return None
    if is_date_key(t):
        return t
    m = re.match(r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?:\D.*)?$", t)
    if m:
        k = f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return k if is_date_key(k) else None
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[T\s]", t)
    if m and is_date_key(m.group(1)):
        return m.group(1)
    # month-name branch (JS Date parsing) not emulated; flag if it would apply
    if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b", t, re.IGNORECASE) and re.search(
            r"\b\d{1,2}\b", t) and re.search(r"\b\d{4}\b", t):
        return "<month-name branch: JS Date(...) would be used>"
    return None


SERIES_INDEX_RE = re.compile(r"^\d+(?:\.\d+)?$")


def parse_series_index(v):
    c = v.strip() if isinstance(v, str) else ""
    return c if len(c) <= 20 and SERIES_INDEX_RE.match(c) else None


def parse_xmp(xml_text):
    try:
        root = fxp_parse(xml_text)
    except Exception as e:  # fast-xml-parser is more lenient than expat; report
        return None, f"xml parse error: {e}"
    xm = root.get("x:xmpmeta")
    rdf = (xm.get("rdf:RDF") if isinstance(xm, dict) else None) or root.get("rdf:RDF")
    if not rdf:
        return None, "no rdf:RDF"
    descs = rdf.get("rdf:Description") or [] if isinstance(rdf, dict) else []
    if not descs:
        return None, "no rdf:Description"
    merged = {}
    for d in descs:
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if not k.startswith("@_") and k not in merged:
                merged[k] = v
    px = "bookorbit"
    tags = get_list(merged.get(f"{px}:tags"))

    def num(v):
        t = get_text(v)
        if not t:
            return None
        try:
            return float(t)
        except ValueError:
            return None

    res = {
        "title": get_text(merged.get("dc:title")),
        "subtitle": get_text(merged.get(f"{px}:subtitle")),
        "description": get_text(merged.get("dc:description")),
        "publisher": get_text(merged.get("dc:publisher")),
        "publishedDate": parse_published_date_key(get_text(merged.get("dc:date"))),
        "publishedYear": parse_published_year(get_text(merged.get("dc:date"))),
        "language": get_text(merged.get("dc:language")),
        "authors": get_list(merged.get("dc:creator")),
        "genres": get_list(merged.get("dc:subject")),
        "tags": tags if tags else split_list_text(merged.get("pdf:Keywords")),
        "isbn10": get_text(merged.get(f"{px}:isbn10")),
        "isbn13": get_text(merged.get(f"{px}:isbn13")),
        "seriesName": get_text(merged.get(f"{px}:seriesName")),
        "seriesIndex": parse_series_index(get_text(merged.get(f"{px}:seriesIndex"))),
        "pageCount": num(merged.get(f"{px}:pageCount")),
    }
    return res, {"merged_keys": sorted(merged.keys()), "description_attrs": [
        {k: v for k, v in d.items() if k.startswith("@_") and not k.startswith("@_xmlns")} for d in descs if
        isinstance(d, dict)]}


def page_count(p, catalog):
    seen = set()

    def walk(node_ref):
        node = p.lookup(node_ref)
        if not isinstance(node, dict):
            return 0
        t = p.lookup(node.get("Type"))
        if t == "Page":
            return 1
        kids = p.lookup(node.get("Kids")) or []
        c = 0
        for k in kids:
            if isinstance(k, Ref):
                if k in seen:
                    continue
                seen.add(k)
            c += walk(k)
        return c

    return walk(catalog.get("Pages"))


def clean(v):
    if v is None:
        return None
    s = v.strip()
    return s or None


def info_get(p, info, key):
    v = p.lookup(info.get(key)) if isinstance(info, dict) else None
    if v is None:
        return None
    if isinstance(v, (PStr, HStr)):
        return decode_text(v)
    return f"<non-string {type(v).__name__}>"


def bookorbit_view(data: bytes) -> dict:
    """The trailer, Info dict and parsed fields BookOrbit would see."""
    p = Parser(data)
    p.parse_document()
    root = p.trailer.get("Root")
    catalog = p.lookup(root)
    encrypted = bool(p.lookup(p.trailer.get("Encrypt")))
    meta_ref = catalog.get("Metadata") if isinstance(catalog, dict) else None
    xmp_xml = None
    if not encrypted and meta_ref is not None:
        s = p.lookup(meta_ref) if isinstance(meta_ref, Ref) else meta_ref
        if isinstance(s, Stream):
            try:
                xmp_xml = decode_stream(s).decode("utf-8", "replace")
            except Exception:
                xmp_xml = s.contents.decode("utf-8", "replace")
    info = p.lookup(p.trailer.get("Info"))
    infod = {k: info_get(p, info, k) for k in ("Title", "Author", "Subject", "Keywords", "Creator", "Producer")}
    xmp, _ = parse_xmp(xmp_xml) if xmp_xml else (None, "no xmp")
    has_xmp = xmp is not None
    info_title = clean(infod["Title"])
    info_author = clean(infod["Author"])
    info_subject = clean(infod["Subject"])
    info_kw = [s.strip() for s in re.split(r"[,;]", clean(infod["Keywords"]) or "") if s.strip()]
    is_bo = clean(infod["Creator"]) == "bookorbit"
    pc = page_count(p, catalog)
    parsed = {
        "title": xmp["title"] if has_xmp else info_title,
        "authors": xmp["authors"] if has_xmp else [a.strip() for a in re.split(r"[,;]", info_author or "") if a.strip()],
        "description": xmp["description"] if has_xmp else info_subject,
        "publishedDate": xmp["publishedDate"] if has_xmp else None,
        "publishedYear": xmp["publishedYear"] if has_xmp else None,
        "genres": xmp["genres"] if has_xmp else [],
        "tags": xmp["tags"] if has_xmp else info_kw,
        "seriesName": xmp["seriesName"] if has_xmp else None,
        "seriesIndex": xmp["seriesIndex"] if has_xmp else None,
        "pageCount": (xmp["pageCount"] if xmp["pageCount"] is not None else (None if is_bo else pc)) if has_xmp else pc,
    }
    return {"trailer": dict(p.trailer), "encrypted": encrypted, "info": infod, "parsed": parsed}
