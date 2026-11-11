"""Test oracle: what Kavita would read from a PDF.

Kavita does not use a PDF library for metadata. It walks the file with its own
small lexer (``PdfLexer``), follows the cross-reference chain from the last
``startxref`` (``ReadXRefAndTrailer`` / ``ReadXRefStream``), and then reads the
Info dict and the catalog's XMP packet of every revision it collected
(``ReadMetadata``). This module is a line-by-line Python port of that reader,
plus the part of ``PdfComicInfoExtractor`` that maps its ``_metadata`` dict onto
ComicInfo fields. Quirks and bugs are ported on purpose; each carries a
``# QUIRK (Lnnn)`` comment naming the upstream line it comes from. The ones that
decide what a stamped magazine shows:

* xref *table* entries are stored with overwrite (``_objectOffsets[obj] =``,
  L911) while the walk goes newest section -> ``/Prev`` -> oldest, so the OLDEST
  table's offset wins for any object a later revision rewrote (catalog, Info);
  xref *stream* entries use ``TryAdd`` (L1145), so among streams the newest wins.
* a table trailer is pushed BEFORE its ``/XRefStm`` and ``/Prev`` are followed
  (L1238), an xref stream AFTER its ``/Prev`` (L1150-1155); ``PushMetadataRef``
  blanks a Root/Info equal to the stack top (L1162-1170); ``ReadMetadata`` pops,
  applying Info then XMP per revision (L1256-1264), every string overwriting.
* compressed (type 2) objects are never located, so a catalog or Info dict in an
  object stream is invisible (L1143).

Ported from Kareadita/Kavita, branch ``develop`` at commit
a9cdf1cad9ae14f88a295c3a6fc7993f9823d59d (fetched 2026-11-10):

* ``Kavita.Services/Helpers/PdfMetadataExtractor.cs``
  blob 09f2420ea1276a0eb5f2828633e86aa576fde458 (last changed in c62b20f5).
  Bare ``Lnnn`` references below are lines of this file.
* ``Kavita.Services/Helpers/PdfComicInfoExtractor.cs``
  blob c1efee9104e0d8484211429fc342cb5910c663ea (last changed in e7808411),
  referenced as ``CI:Lnnn``.
* ``Kavita.Services/Extensions/ComicInfoExtensions.cs`` (``CleanComicInfo``)
  blob 1fc2e5de7a9285009bedacb4f2b8fc317fe7f964, referenced as ``CX:Lnnn``.
* ``Kavita.Services/Scanner/Parser.cs`` (``CleanAuthor``, L1209)
  blob ade2caf1bfdcf93333ce265d6b2708d448cbd4b8.
* ``Kavita.Services/Reading/ReadingItemService.cs`` (``ReadComicInfo``: no
  metadata when the library's EnableMetadata is off, L51; exceptions caught and
  the file imported without metadata, L70-74)
  blob ff8893951cacaef9fe8b2a812618315b95f0a1c5, referenced as ``RI:Lnnn``.
* ``Kavita.Models/Entities/Library.cs`` (``EnableMetadata = true``, L55)
  blob f5cbf5d1907def381a19cb227bcc89208b0ba7b7.
* ``Kavita.Models/Metadata/ComicInfo.cs`` (field defaults)
  blob f7c7914409c4997013ee71f003da70d9157a270f.

The .NET ``FileStream`` and the lexer's 1024-byte buffer are emulated exactly
(``Seek``, short ``Read``, ``PutBack``, ``WantLookahead``), because several
quirks depend on them. Where Kavita throws, this module raises
``PdfMetadataExtractorException`` with the upstream message verbatim (including
messages that lack C#'s ``$`` and so print ``{b}`` literally), or
``DotNetException`` for a .NET runtime exception that would escape. Assumptions
the C# leaves to the host: an invariant/en-US current culture and a UTC server
time zone (Kavita's Docker image default). One deliberate refusal that is not
Kavita's: an XMP packet containing ``<!DOCTYPE`` or ``<!ENTITY`` is never parsed
(``UnsafeXmpRefused``), where .NET's XmlDocument would process the DTD.

It is deliberately independent of ``magsync`` so tests never check the stamper
with its own reader.
"""

from __future__ import annotations

import re
import struct
import zlib
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import NamedTuple
from xml.etree import ElementTree as ET


class KavitaReadError(Exception):
    """Kavita could not read the PDF's metadata.

    Kavita's ``ReadingItemService.ReadComicInfo`` catches this (RI:L70-74) and
    imports the file with no ComicInfo, parsing the filename instead.
    """


class PdfMetadataExtractorException(KavitaReadError):
    """Kavita's own exception, carrying the upstream message verbatim."""


class DotNetException(KavitaReadError):
    """A .NET runtime exception (named by ``dotnet_type``) escaping Kavita's code."""

    def __init__(self, dotnet_type: str, message: str):
        super().__init__(f"{dotnet_type}: {message}")
        self.dotnet_type = dotnet_type


class UnsafeXmpRefused(ValueError):
    """Not Kavita: this oracle refuses to parse an XMP packet that carries a DTD."""


# ---------------------------------------------------------------------------
# .NET primitives
# ---------------------------------------------------------------------------

def _dotnet_integer(text: str, bits: int) -> int:
    """``long.Parse`` / ``Convert.ToInt64`` / ``Convert.ToInt32`` (NumberStyles.Integer)."""
    if not re.fullmatch(r"[ \t\n\v\f\r]*[+-]?[0-9]+[ \t\n\v\f\r]*", text):
        raise DotNetException("FormatException", f"The input string '{text}' was not in a correct format.")
    value = int(text)
    if not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
        raise DotNetException("OverflowException", f"Value was either too large or too small for an Int{bits}.")
    return value


def _dotnet_double(text: str) -> float:
    """``double.Parse`` (NumberStyles.Float | AllowThousands, invariant culture assumed)."""
    if not re.fullmatch(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)", text):
        raise DotNetException("FormatException", f"The input string '{text}' was not in a correct format.")
    return float(text)


def _wrap_int64(value: int) -> int:
    return ((value + (1 << 63)) % (1 << 64)) - (1 << 63)


def _to_int32(value: int) -> int:
    """The unchecked C# cast ``(int)someLong``."""
    return ((value + (1 << 31)) % (1 << 32)) - (1 << 31)


# Char.IsWhiteSpace, which String.Trim() uses (not Python's str.isspace).
_DOTNET_WHITESPACE = (
    "\t\n\v\f\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007"
    "\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
)


def _dotnet_trim(text: str) -> str:
    return text.strip(_DOTNET_WHITESPACE)


class _FileStream:
    """``System.IO.FileStream`` over bytes: Seek, short Read at EOF, ReadExactly."""

    def __init__(self, data: bytes):
        self._data = data
        self.position = 0

    def seek(self, offset: int, from_end: bool = False) -> None:
        target = len(self._data) + offset if from_end else offset
        if target < 0:
            raise DotNetException(
                "IOException", "An attempt was made to move the position before the beginning of the stream.")
        self.position = target  # past EOF is allowed; reads then return 0

    def read(self, buffer: bytearray, offset: int, count: int) -> int:
        chunk = self._data[self.position:self.position + max(count, 0)]
        buffer[offset:offset + len(chunk)] = chunk
        self.position += len(chunk)
        return len(chunk)

    def read_exactly(self, buffer: bytearray, offset: int, count: int) -> None:
        if self.position + count > len(self._data):
            raise DotNetException("EndOfStreamException", "Unable to read beyond the end of the stream.")
        self.read(buffer, offset, count)


class _ByteStream:
    """A MemoryStream / ZLibStream as Kavita consumes it: ReadByte, then the rest."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def read_byte(self) -> int:
        if self._pos >= len(self._data):
            return -1
        self._pos += 1
        return self._data[self._pos - 1]

    def read_rest(self) -> bytes:
        rest = self._data[self._pos:]
        self._pos = len(self._data)
        return rest


# ---------------------------------------------------------------------------
# PdfStringBuilder (L75-172)
# ---------------------------------------------------------------------------

_PDFDOC_LOW = [0x02D8, 0x02C7, 0x02C6, 0x02D9, 0x02DD, 0x02DB, 0x02DA, 0x02DC]  # L84-87
_PDFDOC_HIGH = [  # L89-96
    0x2022, 0x2020, 0x2021, 0x2026, 0x2014, 0x2013, 0x0192, 0x2044,
    0x2039, 0x203A, 0x2212, 0x2030, 0x201E, 0x201C, 0x201D, 0x2018,
    0x2019, 0x201A, 0x2122, 0xFB01, 0xFB02, 0x0141, 0x0152, 0x0160,
    # QUIRK (L94): byte 0x9F (undefined in PDFDocEncoding) maps to a plain space.
    0x0178, 0x017D, 0x0131, 0x0142, 0x0153, 0x0161, 0x017E, 0x0020,
    0x20AC,
]


class _PdfStringBuilder:
    """UTF-16BE (with FE FF) or PDFDocEncoding, built from UTF-16 code units like C#'s StringBuilder."""

    def __init__(self):
        self._units: list[int] = []
        self._second_byte = False
        self._prev_byte = 0
        self._is_unicode = False

    def _append_pdfdoc_byte(self, b: int) -> None:
        if 0x18 <= b < 0x20:
            self._units.append(_PDFDOC_LOW[b - 0x18])
        elif 0x80 <= b < 0xA1:
            self._units.append(_PDFDOC_HIGH[b - 0x80])
        else:
            # QUIRK (L110): every other byte is its Latin-1 code point, so 0x7F and 0xAD
            # (undefined in PDFDocEncoding) pass through; FF FE and EF BB BF BOMs are not
            # recognised and come out as PDFDocEncoded text.
            self._units.append(b)

    def append(self, ch: str) -> None:
        # QUIRK (L114-117, used by the \b \f \n \r \t escapes at L692-715): the char goes
        # straight into the builder, bypassing BOM detection and UTF-16 byte pairing, so
        # a pending first BOM byte is lost and a UTF-16 pair can be split.
        self._units.append(ord(ch))

    def append_byte(self, b: int) -> None:
        if not self._units and not self._is_unicode:  # L122
            if self._second_byte and b == 0xFF:  # L127
                self._is_unicode = True
                self._second_byte = False
            elif self._second_byte:  # L131-134
                # QUIRK (L131-134): _secondByte is never cleared on this path.
                self._append_pdfdoc_byte(self._prev_byte)
                self._append_pdfdoc_byte(b)
            elif b == 0xFE:  # L135
                self._second_byte = True
                self._prev_byte = b
            else:
                self._append_pdfdoc_byte(b)
        elif self._is_unicode:  # L144-156
            if self._second_byte:
                self._units.append(((self._prev_byte << 8) | b) & 0xFFFF)
                self._second_byte = False
            else:
                self._prev_byte = b
                self._second_byte = True
        else:
            self._append_pdfdoc_byte(b)

    def to_string(self) -> str:
        if not self._units and self._second_byte:
            # QUIRK (L165-168): also fires in UTF-16 mode, so "FE FF xx" (one odd byte
            # after the BOM) yields the PDFDocEncoded xx; other odd trailing bytes vanish.
            self._append_pdfdoc_byte(self._prev_byte)
        raw = b"".join(u.to_bytes(2, "big") for u in self._units)
        return raw.decode("utf-16-be", "surrogatepass")


# ---------------------------------------------------------------------------
# PdfLexer (L174-802)
# ---------------------------------------------------------------------------

NONE, BOOL, INT, DOUBLE, NAME, STRING = "None", "Bool", "Int", "Double", "Name", "String"
ARRAY_START, ARRAY_END, DICT_START, DICT_END = "ArrayStart", "ArrayEnd", "DictionaryStart", "DictionaryEnd"
STREAM_START, STREAM_END, OBJECT_START, OBJECT_END = "StreamStart", "StreamEnd", "ObjectStart", "ObjectEnd"
OBJECT_REF, KEYWORD, NEWLINE = "ObjectRef", "Keyword", "Newline"


class Token(NamedTuple):
    type: str
    value: object


_LF, _CR, _SP, _TAB, _FF, _NUL = 0x0A, 0x0D, 0x20, 0x09, 0x0C, 0x00
_DIGITS = range(0x30, 0x3A)
_HEX = frozenset(b"0123456789abcdefABCDEF")
_LETTERS = frozenset(b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
_NAME_DELIMS = frozenset(b"()[]{}<>/%")


def _hex_digit(b: int) -> int:  # L603-612
    if 0x30 <= b <= 0x39:
        return b - 0x30
    if 0x61 <= b <= 0x66:
        return b - 0x61 + 10
    if 0x41 <= b <= 0x46:
        return b - 0x41 + 10
    # QUIRK (L610): not an interpolated string; the message is literally "{b}".
    raise PdfMetadataExtractorException("Invalid hex digit, got {b}")


class _Lexer:
    BUFFER_SIZE = 1024  # L176

    def __init__(self, stream: _FileStream):
        self._stream = stream
        self._buffer = bytearray(self.BUFFER_SIZE)
        self._pos = 0
        self._valid = 0

    # -- buffer (L460-503)
    def _next_byte(self) -> int:
        if self._pos >= self._valid:
            self._pos = 0
            self._valid = self._stream.read(self._buffer, 0, self.BUFFER_SIZE)
            if self._valid <= 0:
                raise PdfMetadataExtractorException("End of stream")
        self._pos += 1
        return self._buffer[self._pos - 1]

    def _last_byte(self) -> int:
        return self._buffer[self._pos - 1]

    def _put_back(self) -> None:
        self._pos -= 1

    def _expect_byte(self, expected: int) -> None:
        if self._next_byte() != expected:
            raise PdfMetadataExtractorException(f"Unexpected character, expected {expected}")

    def _want_lookahead(self, length: int) -> None:
        if self._pos + length > self._valid:
            kept = self._valid - self._pos
            self._buffer[0:kept] = self._buffer[self._pos:self._valid]
            self._valid -= self._pos
            self._pos = 0
            self._valid += self._stream.read(self._buffer, self._valid, self.BUFFER_SIZE - self._valid)

    def reset_buffer(self) -> None:  # L279-283
        self._pos = 0
        self._valid = 0

    def test_byte(self, expected: int) -> bool:  # L285-292
        result = self._next_byte() == expected
        self._put_back()
        return result

    # -- tokens
    def next_token(self, report_newlines: bool = False) -> Token:  # L208-277
        while True:
            b = self._next_byte()
            if b == _LF and report_newlines:
                return Token(NEWLINE, True)
            if b == _CR and report_newlines:
                if self._next_byte() != _LF:
                    self._put_back()
                return Token(NEWLINE, True)
            if b in (_SP, _NUL, _TAB, _LF, _FF, _CR):
                continue
            if b == 0x25:  # %
                self._skip_comment()
                continue
            if b in (0x2B, 0x2D, 0x2E) or b in _DIGITS:
                return self._scan_number()
            if b == 0x2F:
                return self._scan_name()
            if b == 0x28:
                return self._scan_string()
            if b == 0x5B:
                return Token(ARRAY_START, True)
            if b == 0x5D:
                return Token(ARRAY_END, True)
            if b == 0x3C:
                if self._next_byte() == 0x3C:
                    return Token(DICT_START, True)
                self._put_back()
                return self._scan_hex_string()
            if b == 0x3E:
                self._expect_byte(0x3E)
                return Token(DICT_END, True)
            if b in _LETTERS:
                return self._scan_keyword()
            # QUIRK (L274): no "$", so the message is literally "{LastByte()}".
            raise PdfMetadataExtractorException("Unexpected byte, got {LastByte()}")

    def expect_newline(self) -> None:  # L294-321
        while True:
            b = self._next_byte()
            if b in (_SP, _TAB, _FF):
                continue
            if b == _LF:
                return
            if b == _CR:
                if self._next_byte() != _LF:
                    self._put_back()
                return
            # QUIRK (L318): literal "{b}" again.
            raise PdfMetadataExtractorException("Unexpected character, expected newline, got {b}")

    def get_xref_start(self) -> int:  # L323-388
        # QUIRK (L860 + L342-382): only the last 32 bytes are searched, and "startxref"
        # counts only as the first token after a line break; a name token swallows the
        # whitespace that ends it (L638-643), so "/Name\nstartxref" hides the keyword.
        while True:
            b = self._next_byte()
            if b not in (_CR, _LF):
                continue  # default (L384-385)
            if b == _CR and self._next_byte() != _LF:  # case '\r' (L332-340)
                self._put_back()
            while True:  # case '\n' (L342-366)
                b = self._next_byte()
                if b == _CR:  # goto case '\r', which falls into case '\n' again
                    if self._next_byte() != _LF:
                        self._put_back()
                    continue
                if b in (_LF, _SP, _TAB, _FF):
                    continue
                self._put_back()
                break
            token = self.next_token(True)
            if token.type == KEYWORD and token.value == "startxref":
                token = self.next_token()
                if token.type == INT:
                    return token.value
                raise PdfMetadataExtractorException("Expected integer after startxref keyword")

    def next_xref_entry(self) -> tuple[bool, int, int]:  # L400-418
        # QUIRK (L402-415): every entry is assumed to be exactly 20 bytes; a 19-byte
        # entry ("... n\n") shifts every later entry.
        self._want_lookahead(20)
        if self._valid - self._pos < 20:
            raise PdfMetadataExtractorException("End of stream")
        entry = "".join(chr(c) if c < 0x80 else "?" for c in self._buffer[self._pos:self._pos + 20])
        offset = _dotnet_integer(_dotnet_trim(entry[0:10]), 64)
        generation = _dotnet_integer(_dotnet_trim(entry[11:16]), 32)
        in_use = entry[17] == "n"
        self._pos += 20
        return in_use, offset, generation

    def stream_object(self, length: int, deflate: bool) -> _ByteStream:  # L420-458
        raw = bytearray()
        self.expect_newline()
        if self._pos < self._valid:
            buffered = min(self._valid - self._pos, length)
            if buffered < 0:  # MemoryStream.Write with a negative count (no /Length)
                raise DotNetException("ArgumentOutOfRangeException", "Non-negative number required. (Parameter 'count')")
            raw += self._buffer[self._pos:self._pos + buffered]
            length -= buffered
            self._pos += buffered
        while length > 0:
            buffered = min(length, self.BUFFER_SIZE)
            self._stream.read_exactly(self._buffer, 0, buffered)
            raw += self._buffer[0:buffered]
            self._pos = 0
            self._valid = 0
            length -= buffered
        if not deflate:
            return _ByteStream(bytes(raw))
        # QUIRK (L450-453): FlateDecode only, and /DecodeParms (PNG predictors) are skipped
        # unread, so a predicted xref stream is read as if it had no predictor.
        try:
            return _ByteStream(zlib.decompressobj().decompress(bytes(raw)))
        except zlib.error as exc:
            raise DotNetException("InvalidDataException", str(exc)) from exc

    def _skip_comment(self) -> None:  # L505-525
        while True:
            b = self._next_byte()
            if b == _LF:
                break
            if b == _CR:
                if self._next_byte() != _LF:
                    self._put_back()
                break

    def _scan_number(self) -> Token:  # L527-601
        has_dot = self._last_byte() == 0x2E
        chars = [chr(self._last_byte())]
        while True:
            b = self._next_byte()
            if b == 0x2E or b in _DIGITS:
                chars.append(chr(b))
                if b == 0x2E:
                    has_dot = True
            else:
                followed_by_space = b in (_SP, _TAB)
                self._put_back()
                break
        text = "".join(chars)
        if has_dot:
            return Token(DOUBLE, _dotnet_double(text))
        if followed_by_space:
            # QUIRK (L562-598): only a space/tab after the number triggers the
            # reference lookahead ("12\n0 R" is two ints); the generation is optional
            # ("12 R" is a reference) and ignored; "12 0 obj" yields ObjectStart even
            # inside a dictionary. The lookahead reads past the 32-byte window into a
            # buffer refill on long runs of blanks/digits and then restores a stale
            # position; at EOF it throws "End of stream".
            self._want_lookahead(32)
            saved = self._pos
            b = self._next_byte()
            while b in (_SP, _TAB):
                b = self._next_byte()
            while b in _DIGITS:
                b = self._next_byte()
            while b in (_SP, _TAB):
                b = self._next_byte()
            if b == 0x52:  # R
                return Token(OBJECT_REF, _dotnet_integer(text, 64))
            if b == 0x6F and self._next_byte() == 0x62 and self._next_byte() == 0x6A:  # obj
                return Token(OBJECT_START, _dotnet_integer(text, 64))
            self._pos = saved
        return Token(INT, _dotnet_integer(text, 64))

    def _scan_name(self) -> Token:  # L614-657
        chars = []
        while True:
            b = self._next_byte()
            if b in _NAME_DELIMS:
                self._put_back()
                return Token(NAME, "".join(chars))
            # QUIRK (L638-643): NUL is whitespace to NextToken but not a name terminator,
            # and the terminating whitespace byte is consumed, not put back.
            if b in (_SP, _TAB, _LF, _FF, _CR):
                return Token(NAME, "".join(chars))
            if b == 0x23:  # '#'
                b1 = self._next_byte()
                b2 = self._next_byte()
                b = (_hex_digit(b1) << 4) | _hex_digit(b2)
            chars.append(chr(b))  # QUIRK (L653): bytes become Latin-1 chars, never UTF-8.

    def _scan_string(self) -> Token:  # L659-737
        sb = _PdfStringBuilder()
        paren_level = 1
        while True:
            b = self._next_byte()
            if b == 0x28:
                paren_level += 1
                sb.append_byte(b)
            elif b == 0x29:
                paren_level -= 1
                if paren_level == 0:
                    return Token(STRING, sb.to_string())
                sb.append_byte(b)
            elif b == 0x5C:
                b = self._next_byte()
                if b == 0x62:
                    sb.append("\b")
                elif b == 0x66:
                    sb.append("\f")
                elif b == 0x6E:
                    sb.append("\n")
                elif b == 0x72:
                    sb.append("\r")
                elif b == 0x74:
                    sb.append("\t")
                elif 0x30 <= b <= 0x37:
                    b2 = self._next_byte()
                    b3 = self._next_byte()
                    # QUIRK (L718-724): exactly three octal digits are required ("\0" or
                    # "\12" followed by a non-digit throws), and the message is literal.
                    if not (0x30 <= b2 <= 0x37 and 0x30 <= b3 <= 0x37):
                        raise PdfMetadataExtractorException("Invalid octal escape, got {b1}{b2}{b3}")
                    sb.append_byte((((b - 0x30) << 6) | ((b2 - 0x30) << 3) | (b3 - 0x30)) & 0xFF)
                # QUIRK (L690-729): the inner switch has no default, so "\(", "\)" and "\\"
                # are DROPPED (the escaped character is lost), as is the byte after a
                # line-continuation backslash ("\\\r\n" keeps the "\n").
            else:
                # QUIRK (L732-733): raw CR / CRLF inside a string is kept, not turned into LF.
                sb.append_byte(b)

    def _scan_hex_string(self) -> Token:  # L739-769
        sb = _PdfStringBuilder()
        while True:
            b = self._next_byte()
            if b in _HEX:
                b1 = self._next_byte()
                if b1 == 0x3E:
                    self._put_back()
                    b1 = 0x30
                # QUIRK (L751-766): whitespace inside a hex string throws (as "Invalid hex
                # digit" or "Invalid hex string"), though the PDF spec allows it.
                sb.append_byte(((_hex_digit(b) << 4) | _hex_digit(b1)) & 0xFF)
            elif b == 0x3E:
                return Token(STRING, sb.to_string())
            else:
                raise PdfMetadataExtractorException("Invalid hex string, got {b}")

    def _scan_keyword(self) -> Token:  # L771-801
        chars = [chr(self._last_byte())]
        while True:
            b = self._next_byte()
            if b in _LETTERS:
                chars.append(chr(b))
            else:
                self._put_back()
                break
        word = "".join(chars)
        special = {"true": (BOOL, True), "false": (BOOL, False), "stream": (STREAM_START, True),
                   "endstream": (STREAM_END, True), "endobj": (OBJECT_END, True)}
        if word in special:
            return Token(*special[word])
        return Token(KEYWORD, word)  # "null", "R", "obj", "xref", "trailer", ...


# ---------------------------------------------------------------------------
# PdfMetadataExtractor (L804-1675)
# ---------------------------------------------------------------------------

XMP_NAMESPACES = {  # L1520-1527
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "calibreSI": "http://calibre-ebook.com/xmp-namespace-series-index",
    "calibre": "http://calibre-ebook.com/xmp-namespace",
    "pdfx": "http://ns.adobe.com/pdfx/1.3/",
    "prism": "http://prismstandard.org/namespaces/basic/2.0/",
    "xmp": "http://ns.adobe.com/xap/1.0/",
}
_INFO_KEYS = ("Title", "Author", "Subject", "Keywords", "Creator", "Producer", "CreationDate", "ModDate")  # L1293-1300


class _MetadataRef:
    def __init__(self, root: int = -1, info: int = -1):
        self.root = root
        self.info = info


class _Extractor:
    def __init__(self, data: bytes):  # L826-834
        self._stream = _FileStream(data)
        self._lexer = _Lexer(self._stream)
        self.object_offsets: dict[int, int] = {}
        self.metadata: dict[str, str] = {}
        self._metadata_ref: list[_MetadataRef] = []  # Stack<MetadataRef>; top is the end
        self.pushed: list[tuple[int, int]] = []
        self.popped: list[tuple[int, int]] = []
        self.sections: list[tuple[str, int]] = []
        try:
            self._read_object_offsets()
            self._read_metadata()
        except RecursionError as exc:  # a /Prev cycle: unbounded recursion in C#
            raise DotNetException("StackOverflowException", "the Kavita process would crash") from exc

    # -- dictionaries (L1545-1650)
    def _parse_dictionary(self, handler) -> None:
        token = self._lexer.next_token()
        if token.type != DICT_START:
            raise PdfMetadataExtractorException("Expected dictionary")
        while True:
            token = self._lexer.next_token()
            if token.type == DICT_END:
                return
            if token.type != NAME:
                raise PdfMetadataExtractorException("Improper token in dictionary")
            value = self._lexer.next_token()
            if not handler(token.value, value):
                self._skip_value(value)

    def _skip_value(self, existing: Token | None = None) -> None:
        token = existing if existing is not None else self._lexer.next_token()
        if token.type in (BOOL, INT, DOUBLE, NAME, STRING, OBJECT_REF, KEYWORD):
            return
        if token.type == ARRAY_START:
            self._skip_array()
            return
        if token.type == DICT_START:
            self._skip_dictionary()
            return
        if token.type == STREAM_START:
            raise PdfMetadataExtractorException(
                "Encountered stream object in unexpected context - PDF may have inline streams in dictionary")
        raise PdfMetadataExtractorException(
            f"Unexpected token type in SkipValue: {token.type} with value: {_cs_str(token.value)}")

    def _skip_array(self) -> None:
        while True:
            token = self._lexer.next_token()
            if token.type == ARRAY_END:
                break
            self._skip_value(token)

    def _skip_dictionary(self) -> None:
        while True:
            token = self._lexer.next_token()
            if token.type == DICT_END:
                break
            if token.type != NAME:
                raise PdfMetadataExtractorException("Expected name in dictionary")
            self._skip_value()

    def _has_object(self, obj: int) -> bool:  # L1666-1669
        return obj in self.object_offsets and self.object_offsets[obj] > 0

    def _seek_object(self, obj: int) -> None:
        if obj not in self.object_offsets:
            raise DotNetException("KeyNotFoundException", f"The given key '{obj}' was not present in the dictionary.")
        self._stream.seek(self.object_offsets[obj])
        self._lexer.reset_buffer()

    # -- cross-reference chain
    def _read_object_offsets(self) -> None:  # L853-865
        self._stream.seek(-32, from_end=True)
        xref_offset = self._lexer.get_xref_start()
        self._read_xref_and_trailer(xref_offset)

    def _read_xref_and_trailer(self, xref_offset: int) -> None:  # L867-927
        self._stream.seek(xref_offset)
        self._lexer.reset_buffer()
        if not self._lexer.test_byte(0x78):  # 'x'
            # QUIRK (L872-877): anything but an 'x' at the offset is taken for an xref
            # stream, so a startxref pointing at whitespace before "xref" throws.
            self._read_xref_stream(xref_offset)
            return
        self.sections.append(("table", xref_offset))
        token = self._lexer.next_token()
        if token.type != KEYWORD or token.value != "xref":
            raise PdfMetadataExtractorException("Expected xref keyword")
        while True:
            token = self._lexer.next_token()
            if token.type == INT:
                start_obj = token.value
                token = self._lexer.next_token()
                if token.type != INT:
                    raise PdfMetadataExtractorException("Expected number of objects in xref subsection")
                num_obj = token.value
                self._lexer.expect_newline()
                for obj in range(start_obj, start_obj + num_obj):
                    in_use, offset, _generation = self._lexer.next_xref_entry()
                    if in_use and offset > 0:
                        # QUIRK (L911): plain assignment, not TryAdd. Sections are read
                        # newest first, so an OLDER table's entry overwrites the newer
                        # offset of any object a later revision rewrote. Free entries do
                        # not remove anything, and generations are ignored.
                        self.object_offsets[obj] = offset
            elif token.type == KEYWORD and token.value == "trailer":
                break
            else:
                raise PdfMetadataExtractorException("Unexpected token in xref")
        self._read_trailer_dictionary()

    def _read_xref_stream(self, xref_offset: int) -> None:  # L929-1156
        self.sections.append(("stream", xref_offset))
        lexer = self._lexer
        token = lexer.next_token()
        if token.type != OBJECT_START:
            raise PdfMetadataExtractorException("Expected obj keyword")
        state = {"length": -1, "size": -1, "deflate": False, "prev": -1, "widths": (-1, -1, -1)}
        sections: deque[tuple[int, int]] = deque()
        meta = _MetadataRef()

        def handler(key: str, value: Token) -> bool:
            if key == "Type":
                if value.type != NAME or value.value != "XRef":
                    raise PdfMetadataExtractorException("Expected /Type to be /XRef")
                return True
            if key == "Length":
                if value.type != INT:
                    raise PdfMetadataExtractorException("Expected integer after /Length")
                state["length"] = value.value
                return True
            if key == "Size":
                if value.type != INT:
                    raise PdfMetadataExtractorException("Expected integer after /Size")
                state["size"] = value.value
                return True
            if key == "Prev":
                if value.type != INT:
                    raise PdfMetadataExtractorException("Expected offset after /Prev")
                state["prev"] = value.value
                return True
            if key == "Index":
                if value.type != ARRAY_START:
                    raise PdfMetadataExtractorException("Expected array after /Index")
                while True:
                    tok = lexer.next_token()
                    if tok.type == ARRAY_END:
                        break
                    if tok.type != INT:
                        raise PdfMetadataExtractorException("Expected integer in /Index array")
                    first = tok.value
                    tok = lexer.next_token()
                    if tok.type != INT:
                        raise PdfMetadataExtractorException("Expected integer pair in /Index array")
                    sections.append((first, tok.value))
                return True
            if key == "W":
                if value.type != ARRAY_START:
                    raise PdfMetadataExtractorException("Expected array after /W")
                widths = []
                for _ in range(3):
                    tok = lexer.next_token()
                    if tok.type != INT:
                        raise PdfMetadataExtractorException("Expected integer in /W array")
                    widths.append(tok.value)
                if lexer.next_token().type != ARRAY_END:
                    raise PdfMetadataExtractorException("Unclosed array after /W")
                state["widths"] = tuple(widths)
                return True
            if key == "Filter":
                # QUIRK (L1059-1072): a filter array, even [/FlateDecode], throws.
                if value.type != NAME:
                    raise PdfMetadataExtractorException("Expected name after /Filter")
                if value.value != "FlateDecode":
                    raise PdfMetadataExtractorException("Unsupported filter, only FlateDecode is supported")
                state["deflate"] = True
                return True
            if key == "Root":
                if value.type != OBJECT_REF:
                    raise PdfMetadataExtractorException("Expected object reference after /Root")
                meta.root = value.value
                return True
            if key == "Info":
                if value.type != OBJECT_REF:
                    raise PdfMetadataExtractorException("Expected object reference after /Info")
                meta.info = value.value
                return True
            # QUIRK (L1094-1095): /Encrypt in an xref-stream dictionary is skipped like any
            # other key, so only a TABLE trailer raises "Encryption not supported".
            return False

        self._parse_dictionary(handler)
        token = lexer.next_token()
        if token.type != STREAM_START:
            raise PdfMetadataExtractorException("Expected xref stream after dictionary")
        stream = lexer.stream_object(_to_int32(state["length"]), state["deflate"])
        if not sections:
            sections.append((0, state["size"]))
        type_width, offset_width, generation_width = state["widths"]
        while sections:
            first, count = sections.popleft()
            for i in range(first, first + count):
                entry_type = 1 if type_width == 0 else 0
                offset = 0
                generation = 0
                # QUIRK (L1130-1141): ReadByte's -1 at end of data is cast to (ushort)
                # 0xFFFF and OR-ed in, so a short stream yields garbage entries.
                for _ in range(type_width):
                    entry_type = _wrap_int64((entry_type << 8) | (stream.read_byte() & 0xFFFF))
                for _ in range(offset_width):
                    offset = _wrap_int64((offset << 8) | (stream.read_byte() & 0xFFFF))
                for _ in range(generation_width):
                    generation = _wrap_int64((generation << 8) | (stream.read_byte() & 0xFFFF))
                if entry_type == 1:
                    # QUIRK (L1143-1146): TryAdd, so the first (newest) stream entry wins;
                    # type 2 (object-stream) entries are never recorded, so a compressed
                    # catalog or Info dict can never be found.
                    self.object_offsets.setdefault(i, offset)
        if state["prev"] > -1:
            # QUIRK (L1150-1155): /Prev is followed BEFORE this section's Root/Info is
            # pushed, so an older revision's refs sit lower on the stack and are popped
            # (applied) AFTER this one's; the opposite of a table trailer (L1238).
            self._read_xref_and_trailer(state["prev"])
        self._push_metadata_ref(meta)

    def _push_metadata_ref(self, meta: _MetadataRef) -> None:  # L1158-1177
        if self._metadata_ref:
            # QUIRK (L1160-1171): compared with the stack top only; an equal Root or
            # Info is blanked, so one revision's catalog/Info is read once even if
            # several revisions name it, and at the position of the earliest push.
            if meta.root == self._metadata_ref[-1].root:
                meta.root = -1
            if meta.info == self._metadata_ref[-1].info:
                meta.info = -1
        if meta.root != -1 or meta.info != -1:
            self._metadata_ref.append(meta)
            self.pushed.append((meta.root, meta.info))

    def _read_trailer_dictionary(self) -> None:  # L1179-1249
        state = {"prev": -1, "xref_stm": -1}
        meta = _MetadataRef()

        def handler(key: str, value: Token) -> bool:
            if key == "Root":
                if value.type != OBJECT_REF:
                    raise PdfMetadataExtractorException("Expected object reference after /Root")
                meta.root = value.value
                return True
            if key == "Prev":
                if value.type != INT:
                    raise PdfMetadataExtractorException("Expected offset after /Prev")
                state["prev"] = value.value
                return True
            if key == "Info":
                if value.type != OBJECT_REF:
                    raise PdfMetadataExtractorException("Expected object reference after /Info")
                meta.info = value.value
                return True
            if key == "XRefStm":
                if value.type != INT:
                    raise PdfMetadataExtractorException("Expected offset after /XRefStm")
                state["xref_stm"] = value.value
                return True
            if key == "Encrypt":
                raise PdfMetadataExtractorException("Encryption not supported")  # L1230-1231
            return False

        self._parse_dictionary(handler)
        # QUIRK (L1238-1248): pushed BEFORE /XRefStm and /Prev are followed, so older
        # table revisions end up above this one on the stack and are applied first.
        self._push_metadata_ref(meta)
        if state["xref_stm"] != -1:
            self._read_xref_and_trailer(state["xref_stm"])
        if state["prev"] != -1:
            self._read_xref_and_trailer(state["prev"])

    # -- metadata
    def _read_metadata(self) -> None:  # L1251-1265
        while self._metadata_ref:
            meta = self._metadata_ref.pop()
            self.popped.append((meta.root, meta.info))
            self._read_metadata_from_info(meta.info)
            self._read_metadata_from_xml(self._metadata_obj_in_object_catalog(meta.root))

    def _read_metadata_from_info(self, info_obj: int) -> None:  # L1267-1339
        if not self._has_object(info_obj):
            return
        self._seek_object(info_obj)
        token = self._lexer.next_token()
        if token.type != OBJECT_START:
            raise PdfMetadataExtractorException("Expected object header")
        indirect: dict[str, int] = {}

        def handler(key: str, value: Token) -> bool:
            if key not in _INFO_KEYS:
                return False
            if value.type == OBJECT_REF:
                indirect[key] = value.value
            elif value.type != STRING:
                # QUIRK (L1304-1307): a null, name or number value for any of these
                # keys aborts the whole read.
                raise PdfMetadataExtractorException("Expected string value")
            else:
                # QUIRK (L1310): always overwrites, even with an empty string, so a later
                # revision's (Author) blanks an earlier "downmagaz.net".
                self.metadata[key] = value.value
            return True

        self._parse_dictionary(handler)
        for key, obj in indirect.items():  # L1321-1338
            # QUIRK (L1322): no HasObject check; a dangling reference throws
            # KeyNotFoundException.
            self._seek_object(obj)
            token = self._lexer.next_token()
            if token.type != OBJECT_START:
                raise PdfMetadataExtractorException("Expected object here")
            token = self._lexer.next_token()
            if token.type != STRING:
                raise PdfMetadataExtractorException("Expected string")
            self.metadata[key] = token.value

    def _metadata_obj_in_object_catalog(self, root_obj: int) -> int:  # L1341-1381
        if not self._has_object(root_obj):
            return -1
        self._seek_object(root_obj)
        token = self._lexer.next_token()
        if token.type != OBJECT_START:
            raise PdfMetadataExtractorException("Expected object header")
        found = [-1]

        def handler(key: str, value: Token) -> bool:
            if key != "Metadata":
                return False
            if value.type != OBJECT_REF:
                raise PdfMetadataExtractorException("Expected object number after /Metadata")
            found[0] = value.value
            return True

        self._parse_dictionary(handler)
        return found[0]

    def _set_metadata(self, key: str, value: str | None) -> None:  # L1414-1419
        if value is None:
            return
        self.metadata[key] = value

    def _read_metadata_from_xml(self, meta: int) -> None:  # L1421-1543
        if not self._has_object(meta):
            return
        self._seek_object(meta)
        token = self._lexer.next_token()
        if token.type != OBJECT_START:
            raise PdfMetadataExtractorException("Expected object header")
        state = {"length": -1, "deflate": False}

        def handler(key: str, value: Token) -> bool:
            if key == "Type":
                if value.type != NAME or value.value != "Metadata":
                    raise PdfMetadataExtractorException("Expected /Type to be /Metadata")
                return True
            if key == "Subtype":
                if value.type != NAME or value.value != "XML":
                    raise PdfMetadataExtractorException("Expected /Subtype to be /XML")
                return True
            if key == "Length":
                # QUIRK (L1459-1467): an indirect /Length ("12 0 R") throws.
                if value.type != INT:
                    raise PdfMetadataExtractorException("Expected integer after /Length")
                state["length"] = value.value
                return True
            if key == "Filter":
                if value.type != NAME:
                    raise PdfMetadataExtractorException("Expected name after /Filter")
                if value.value != "FlateDecode":
                    raise PdfMetadataExtractorException("Unsupported filter, only FlateDecode is supported")
                state["deflate"] = True
                return True
            return False

        self._parse_dictionary(handler)
        token = self._lexer.next_token()
        if token.type != STREAM_START:
            raise PdfMetadataExtractorException("Expected xref stream after dictionary")  # sic (L1493)
        xml_stream = self._lexer.stream_object(_to_int32(state["length"]), state["deflate"])
        # QUIRK (L1498-1515): skips everything up to the first "?>" (the end of the
        # <?xpacket begin?> PI). A packet without a leading PI loses everything up to the
        # trailing <?xpacket end?>, and then XmlDocument finds no root element.
        while True:
            b = xml_stream.read_byte()
            if b < 0:
                raise PdfMetadataExtractorException("Reached EOF in XMP header")
            if b == 0x3F:
                while b == 0x3F:
                    b = xml_stream.read_byte()
                if b == 0x3E:
                    break
        doc = _XmlDoc(xml_stream.read_rest())

        def text(path: str) -> str | None:  # GetTextFromXmlNode (L1387-1391)
            # QUIRK (L1389-1390): the first rdf:li in document order below ANY matching
            # element wins (so an rdf:Alt's first entry, not its x-default, and a later
            # Description's rdf:li beats an earlier simple-valued property); without
            # one, the first element's whole InnerText, which may be "" and still
            # overwrites.
            node = doc.select_single(path + "//rdf:li")
            if node is None:
                node = doc.select_single(path)
            return None if node is None else doc.inner_text(node)

        def text_list(path: str) -> str | None:  # GetListFromXmlNode (L1393-1412)
            # QUIRK (L1395-1411): only rdf:li entries count, joined with "," (no space);
            # a simple-valued property yields nothing. The "," is added only once the
            # builder is non-empty (L1403-1406), so leading empty entries leave no
            # separator (["", "A"] -> "A") and an all-empty list yields nothing.
            joined = ""
            for node in doc.select_nodes(path + "//rdf:li"):
                if joined:
                    joined += ","
                joined += doc.inner_text(node)
            return joined if joined else None

        def first_of(*values: str | None) -> str | None:
            return next((v for v in values if v is not None), None)

        self._set_metadata("CreationDate", first_of(text("//dc:date"), text("//xmp:CreateDate")))
        self._set_metadata("Summary", text("//dc:description"))
        self._set_metadata("Publisher", text("//dc:publisher"))
        self._set_metadata("Author", text_list("//dc:creator"))
        self._set_metadata("Title", text("//dc:title"))
        self._set_metadata("Subject", text_list("//dc:subject"))
        self._set_metadata("Language", text("//dc:language"))
        self._set_metadata("ISBN", first_of(text("//pdfx:isbn"), text("//prism:isbn")))
        self._set_metadata("UserRating", text("//calibre:rating"))
        self._set_metadata("TitleSort", text("//calibre:title_sort"))
        self._set_metadata("Series", text("//calibre:series/rdf:value"))
        self._set_metadata("Volume", text("//calibreSI:series_index"))


def _cs_str(value: object) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    return str(value)


# ---------------------------------------------------------------------------
# XmlDocument + XPath (SelectSingleNode / SelectNodes / InnerText)
# ---------------------------------------------------------------------------

_XML_SPACE = "{http://www.w3.org/XML/1998/namespace}space"
_XML_WS = " \t\r\n"


class _XmlDoc:
    """``XmlDocument.Load`` of what is left of the stream after the header skip."""

    def __init__(self, data: bytes):
        probe = data.replace(b"\x00", b"")
        if b"<!DOCTYPE" in probe or b"<!ENTITY" in probe:
            raise UnsafeXmpRefused("XMP packet carries a DTD; the oracle does not parse it")
        # Comments and PIs are kept as nodes: XmlDocument loads them, so they split the
        # text around them into separate text nodes (each judged whitespace-only on its
        # own), and InnerText then skips them. ElementTree's default builder would drop
        # them and merge "  <!--c-->AFAR" into one non-whitespace "  AFAR".
        parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True, insert_pis=True))
        try:
            self.root = ET.fromstring(data, parser=parser)
        except ET.ParseError as exc:
            raise DotNetException("XmlException", str(exc)) from exc
        self.order = {el: i for i, el in enumerate(self.root.iter())}
        # XmlDocument.PreserveWhitespace is false: whitespace-only text nodes are not
        # loaded unless an xml:space="preserve" scope makes them significant.
        self.preserve: dict[ET.Element, bool] = {}
        stack = [(self.root, False)]
        while stack:
            el, inherited = stack.pop()
            space = el.get(_XML_SPACE)
            self.preserve[el] = inherited if space is None else space == "preserve"
            stack.extend((child, self.preserve[el]) for child in el)

    def select_nodes(self, path: str) -> list[ET.Element]:
        """XPath ``SelectNodes`` for the absolute ``//a:b/c:d//e:f`` paths Kavita uses.

        The context is the DocumentElement, but the paths start with ``//`` and so
        are evaluated from the document node. Results are unique, in document order.
        """
        context: list[ET.Element | None] = [None]  # None is the document node
        for axis, prefix, local in re.findall(r"(//|/)([A-Za-z]+):([A-Za-z_]+)", path):
            tag = f"{{{XMP_NAMESPACES[prefix]}}}{local}"
            found: set[ET.Element] = set()
            for node in context:
                if node is None:
                    candidates = list(self.root.iter()) if axis == "//" else [self.root]
                elif axis == "//":
                    candidates = [el for el in node.iter() if el is not node]
                else:
                    candidates = list(node)
                found.update(el for el in candidates if el.tag == tag)
            context = sorted(found, key=self.order.__getitem__)
        return [el for el in context if el is not None]

    def select_single(self, path: str) -> ET.Element | None:
        nodes = self.select_nodes(path)
        return nodes[0] if nodes else None

    def inner_text(self, node: ET.Element) -> str:
        """``XmlNode.InnerText``: all descendant text, minus unloaded whitespace-only nodes."""
        parts: list[str] = []

        def keep(text: str | None, scope: ET.Element) -> None:
            if text and (self.preserve[scope] or text.strip(_XML_WS)):
                parts.append(text)

        def walk(el: ET.Element) -> None:
            keep(el.text, el)
            for child in el:
                if child.tag not in (ET.Comment, ET.ProcessingInstruction):
                    walk(child)  # a comment's or PI's own content is not InnerText
                keep(child.tail, el)

        walk(node)
        return "".join(parts)


# ---------------------------------------------------------------------------
# PdfComicInfoExtractor (CI) + CleanComicInfo (CX)
# ---------------------------------------------------------------------------

# CI:L26-31 as regexes: "D" and "+" are literals, ":" the time separator, and "zzz"
# is .NET's ParseTimeZoneOffset: a sign, 1-2 hour digits taken greedily (hence the
# possessive "{1,2}+"), an optional ":", then exactly 2 minute digits.
_PDF_DATE_FORMATS = [
    ("D:yyyyMMddHHmmsszzz:", r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})([+-]\d{1,2}+:?\d{2}):"),
    ("D:yyyyMMddHHmmss+", r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})()\+"),
    ("D:yyyyMMddHHmmss", r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})()"),
    ("D:yyyyMMddHHmmzzz:", r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})()([+-]\d{1,2}+:?\d{2}):"),
    ("D:yyyyMMddHHmm+", r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})()()\+"),
    ("D:yyyyMMddHHmm", r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})()()"),
    ("D:yyyyMMddHHzzz:", r"D:(\d{4})(\d{2})(\d{2})(\d{2})()()([+-]\d{1,2}+:?\d{2}):"),
    ("D:yyyyMMddHH+", r"D:(\d{4})(\d{2})(\d{2})(\d{2})()()()\+"),
    ("D:yyyyMMddHH", r"D:(\d{4})(\d{2})(\d{2})(\d{2})()()()"),
    ("D:yyyyMMdd", r"D:(\d{4})(\d{2})(\d{2})()()()()"),
    ("D:yyyyMM", r"D:(\d{4})(\d{2})()()()()()"),
    ("D:yyyy", r"D:(\d{4})()()()()()()"),
]

# The ISO-8601-like shapes DateTime.TryParse(text, InvariantCulture) accepts that
# matter for PDFs and XMP (it accepts much more; see kavita_comicinfo's docstring).
_ISO_DATE = re.compile(
    r"\s*(\d{4})-(\d{1,2})(?:-(\d{1,2})(?:[T ](\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?"
    r"\s*(Z|[+-]\d{2}:?\d{2})?)?)?\s*"
)
_SLASH_DATE = re.compile(r"\s*(?:(\d{4})/(\d{1,2})/(\d{1,2})|(\d{1,2})/(\d{1,2})/(\d{4}))\s*")


def _build_datetime(year, month, day, hour, minute, second, offset) -> datetime | None:
    try:
        moment = datetime(int(year), int(month or 1), int(day or 1), int(hour or 0), int(minute or 0),
                          int(second or 0), tzinfo=UTC)
    except ValueError:
        return None
    if offset:
        # An explicit offset makes .NET convert to the server's local time (assumed UTC
        # here), which can move the calendar day (CI:L48, CI:L61).
        if offset == "Z":
            return moment
        sign, hours, minutes = re.fullmatch(r"([+-])(\d{1,2}+):?(\d{2})", offset).groups()
        if int(minutes) >= 60:  # ParseTimeZoneOffset rejects it, so this format fails
            return None
        return moment - (1 if sign == "+" else -1) * timedelta(hours=int(hours), minutes=int(minutes))
    return moment


def _dotnet_datetime_try_parse(text: str) -> datetime | None:
    match = _ISO_DATE.fullmatch(text)
    if match:
        return _build_datetime(*match.groups())
    match = _SLASH_DATE.fullmatch(text)
    if match:
        y1, m1, d1, m2, d2, y2 = match.groups()
        if y1:
            return _build_datetime(y1, m1, d1, 0, 0, 0, None)
        return _build_datetime(y2, m2, d2, 0, 0, 0, None)  # invariant culture is MM/dd/yyyy
    return None


def _get_datetime_from_text(text: str | None) -> datetime | None:  # CI:L42-65
    if not text:
        return None
    parsed = _dotnet_datetime_try_parse(text)
    if parsed is not None:
        return parsed
    if text[0] != "D":
        text = "D:" + text
    # QUIRK (CI:L56-57): every "'" becomes ":" and every "Z" becomes "+", so a PDF
    # date with a trailing "+05'00" (no closing apostrophe) matches no format.
    text = text.replace("'", ":").replace("Z", "+")
    for _name, pattern in _PDF_DATE_FORMATS:
        match = re.fullmatch(pattern, text)
        if match:
            parsed = _build_datetime(*match.groups())
            if parsed is not None:
                return parsed
    return None


_FLOAT = re.compile(r"[ \t\n\v\f\r]*[+-]?(?:[0-9][0-9,]*\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?[ \t\n\v\f\r]*")
_FLOAT_SYMBOL = re.compile(r"[+-]?infinity|[+-]?nan", re.IGNORECASE)


def _get_float_from_text(text: str | None) -> float | None:  # CI:L33-40
    if not text:
        return None
    # float.TryParse(text, InvariantCulture): NumberStyles.Float | AllowThousands.
    stripped = text.strip(" \t\n\v\f\r")
    # The symbol fallback after a failed numeric parse (.NET Number.Parsing
    # TryParseFloat, trimming like SpanTrim): the text, case-insensitively, is
    # "Infinity", "-Infinity", "NaN", "+Infinity", "+NaN" or "-NaN"; anything else
    # (e.g. "+-Infinity") fails and Kavita keeps UserRating 0.
    special = _FLOAT_SYMBOL.fullmatch(stripped)
    if special:
        return float(special.group(0).lower())
    if not _FLOAT.fullmatch(text):
        return None
    return struct.unpack("f", struct.pack("f", float(stripped.replace(",", ""))))[0]


def _comicinfo_from_metadata(metadata: dict[str, str]) -> dict:  # CI:L72-114
    def get(key: str) -> str | None:  # MaybeGetMetadata (CI:L67-70)
        return metadata.get(key)

    info: dict = {}
    published = _get_datetime_from_text(get("CreationDate"))  # CI:L76
    info["Year"] = published.year if published else 0  # ComicInfo defaults are 0
    info["Month"] = published.month if published else 0
    info["Day"] = published.day if published else 0
    info["Summary"] = get("Summary") or ""  # CI:L85; no Info key ever sets "Summary"
    info["Publisher"] = get("Publisher") or ""  # CI:L86
    info["Writer"] = get("Author") or ""  # CI:L87
    info["Title"] = get("Title") or ""  # CI:L88
    info["TitleSort"] = get("TitleSort") or ""  # CI:L89
    info["Genre"] = get("Subject") or ""  # CI:L90: Info /Subject or the dc:subject list
    info["Tags"] = ""  # never set from a PDF (ComicInfo default); pdf:Keywords is unused
    info["UserRating"] = _get_float_from_text(get("UserRating"))  # CI:L100
    if info["UserRating"] is None:
        info["UserRating"] = 0.0
    # QUIRK (CI:L101): "?? info.Title" falls back to the already-defaulted Title, so a
    # PDF with neither gets Series "".
    series = get("Series")
    info["Series"] = series if series is not None else info["Title"]
    info["SeriesSort"] = info["Series"]  # CI:L102
    volume = get("Volume")
    info["Volume"] = volume if volume is not None else ""  # CI:L103
    # CleanComicInfo (CI:L111 -> CX:L11-45)
    info["Series"] = _dotnet_trim(info["Series"])  # CX:L15
    info["SeriesSort"] = _dotnet_trim(info["SeriesSort"])  # CX:L16
    info["Writer"] = _dotnet_trim(info["Writer"])  # CX:L19 CleanAuthor (Parser.cs L1209-1212)
    info["Publisher"] = _dotnet_trim(info["Publisher"])  # CX:L25
    # QUIRK (CX:L34): Isbn is replaced by ParseGtin(info.GTIN), and GTIN is never set
    # for a PDF, so the ISBN read at CI:L92-98 is always discarded.
    info["Isbn"] = ""
    if info["Volume"]:
        info["Volume"] = _dotnet_trim(info["Volume"])  # CX:L41-44
    return info


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def kavita_walk(data: bytes) -> dict:
    """How Kavita walked the file, for diagnosing a result.

    ``{"sections": [(form, offset), ...] in visit order, "offsets": {obj: offset},
    "pushed": [(root, info), ...], "applied": [(root, info), ...] in pop order
    (-1 = blanked), "metadata": {...}}``.
    """
    ex = _Extractor(data)
    return {"sections": ex.sections, "offsets": dict(ex.object_offsets), "pushed": ex.pushed,
            "applied": ex.popped, "metadata": dict(ex.metadata)}


def kavita_metadata(data: bytes) -> dict[str, str]:
    """Kavita's ``PdfMetadataExtractor._metadata`` for these bytes.

    Keys are those Kavita sets: Title, Author, Subject, Keywords, Creator, Producer,
    CreationDate, ModDate (Info dict) and CreationDate, Summary, Publisher, Author,
    Title, Subject, Language, ISBN, UserRating, TitleSort, Series, Volume (XMP).
    Raises ``KavitaReadError`` (``PdfMetadataExtractorException`` or
    ``DotNetException``) where Kavita throws.
    """
    return dict(_Extractor(data).metadata)


def kavita_comicinfo(data: bytes, *, enable_metadata: bool = True) -> dict | None:
    """The ComicInfo fields ``PdfComicInfoExtractor`` derives from the metadata.

    ``{"Title", "Series", "SeriesSort", "Volume", "Writer", "Summary", "Publisher",
    "Genre", "Tags", "TitleSort", "Isbn": str, "UserRating": float, "Year", "Month",
    "Day": int}`` after ``CleanComicInfo``; ``None`` when the library's
    EnableMetadata is off (it defaults to on). Raises like ``kavita_metadata``;
    Kavita itself would catch that and import the file without metadata.

    Not ported: ``LanguageISO`` (.NET CultureInfo lookup), ``Count`` (needs the
    filename and Kavita's volume regexes). ``DateTime.TryParse`` is approximated
    by the ISO-8601 and slash shapes PDFs and XMP use.
    """
    if not enable_metadata:  # RI:L51
        return None
    return _comicinfo_from_metadata(kavita_metadata(data))
