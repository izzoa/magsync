"""Date parsing and file organization for magsync."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}

SEASON_MONTHS = {
    "spring": 3, "summer": 6, "autumn": 9, "fall": 9, "winter": 12,
}


@dataclass
class ParsedDate:
    year: int | None = None
    month: int | None = None
    # Only set when the title states a day of the month; never used for paths.
    day: int | None = None
    # (year, month, day) where a date range starts, when it starts in an
    # earlier month than ``month`` ("March 28th-April 3rd"): the issue date.
    # Paths keep using ``month``, so existing filenames never move.
    start: tuple[int, int, int] | None = None


def _day(token: str) -> int | None:
    value = int(token)
    return value if 1 <= value <= 31 else None


def _range(year: int, start_month: str, start_day: str, end_month: str, end_day: str) -> ParsedDate:
    first, last = MONTH_NAMES[start_month], MONTH_NAMES[end_month]
    # "December 27th - January 2nd, 2027": the range starts the year before.
    start_year = year - 1 if first > last else year
    day = _day(start_day)
    return ParsedDate(
        year=year,
        month=last,
        day=_day(end_day),
        start=(start_year, first, day) if day else None,
    )


def parse_date(title: str, page_url: str = "") -> ParsedDate:
    """Extract year and month from a magazine title string.

    Handles multiple formats:
    - "April 13, 2026" → (2026, 4)
    - "February 16-23, 2026" → (2026, 2)
    - "March-April 2026" → (2026, 3)
    - "Spring 2026" → (2026, 3)
    - "Vol 208 No 05, May 2026" → (2026, 5)
    - "27th Edition, 2026" → (2026, None)
    """
    title_lower = title.lower()
    months = "january|february|march|april|may|june|july|august|september|october|november|december"

    # Ranges across months: "March 28th-April 3rd, 2026", "28 March - 3 April 2026".
    # The path month stays the end month, as the patterns below always gave.
    m = re.search(
        rf"({months})\s+(\d{{1,2}})(?:st|nd|rd|th)?\s*[-–]\s*({months})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s*(\d{{4}})",
        title_lower,
    )
    if m:
        return _range(int(m.group(5)), m.group(1), m.group(2), m.group(3), m.group(4))
    m = re.search(
        rf"(\d{{1,2}})(?:st|nd|rd|th)?\s+({months})\s*[-–]\s*(\d{{1,2}})(?:st|nd|rd|th)?\s+({months})\s+(\d{{4}})",
        title_lower,
    )
    if m:
        return _range(int(m.group(5)), m.group(2), m.group(1), m.group(4), m.group(3))

    # Pattern 1: "Month DD, YYYY" or "Month DDth-DDth, YYYY" (day = first day)
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s*[-–]\s*\d{1,2}(?:st|nd|rd|th)?)?,?\s*(\d{4})",
        title_lower,
    )
    if m:
        return ParsedDate(
            year=int(m.group(3)), month=MONTH_NAMES[m.group(1)], day=_day(m.group(2))
        )

    # Pattern 2: "DD Month YYYY", "DDth Month YYYY" or "DD-DD Month YYYY" (day = first day)
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?(?:\s*[-–]\s*\d{1,2}(?:st|nd|rd|th)?)?\s+"
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s+(\d{4})",
        title_lower,
    )
    if m:
        return ParsedDate(
            year=int(m.group(3)), month=MONTH_NAMES[m.group(2)], day=_day(m.group(1))
        )

    # Pattern 3: "Month-Month YYYY" or "Month/Month YYYY"
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s*[-–/]\s*"
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s+(\d{4})",
        title_lower,
    )
    if m:
        return ParsedDate(year=int(m.group(3)), month=MONTH_NAMES[m.group(1)])

    # Pattern 4: "Month YYYY" (no day)
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s+(\d{4})",
        title_lower,
    )
    if m:
        return ParsedDate(year=int(m.group(2)), month=MONTH_NAMES[m.group(1)])

    # Pattern 5: Season YYYY
    m = re.search(r"(spring|summer|autumn|fall|winter)\s+(\d{4})", title_lower)
    if m:
        return ParsedDate(year=int(m.group(2)), month=SEASON_MONTHS[m.group(1)])

    # Pattern 6: Abbreviated month (e.g., "Apr 2026", "Sep-Oct 2026")
    m = re.search(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b"
        r"(?:\s*[-–/]\s*\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b)?"
        r"\s+(\d{4})",
        title_lower,
    )
    if m:
        return ParsedDate(year=int(m.group(2)), month=MONTH_NAMES[m.group(1)])

    # Pattern 7: Year only (fallback from title)
    m = re.search(r"\b(20\d{2})\b", title)
    if m:
        return ParsedDate(year=int(m.group(1)), month=None)

    # Pattern 8: Year from URL slug (last resort)
    if page_url:
        m = re.search(r"-(20\d{2})(?:[/-]|$)", page_url)
        if m:
            return ParsedDate(year=int(m.group(1)), month=None)

    return ParsedDate()


def canonical_file(path: str | Path) -> Path:
    """One identity per library file: symlinks resolved, ``..`` and ``~`` collapsed.

    Non-strict, so a destination that does not exist yet still resolves
    through its existing parent directories. Grouping, per-file locks,
    adoption and duplicate completion all key on this.
    """
    return Path(path).expanduser().resolve()


def strip_accents(s: str) -> str:
    """Strip accent marks from a string, preserving base characters.

    "Bon Appétit" → "Bon Appetit"
    "Zürich" → "Zurich"
    """
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


# The source used to prepend a format label to every listing ("[PDF] ..."),
# and issues indexed back then still carry it in their stored title. Anchored,
# bracketed and length-bounded so bracketed *issue detail* elsewhere survives.
_FORMAT_TAG_RE = re.compile(r"^\[[A-Za-z0-9+/.\- ]{1,12}\]\s*")


def strip_format_tag(title: str) -> str:
    """Remove a leading bracketed source format tag from a title.

    The tag is source presentation, not part of the publication's name.
    Leaving it in splits the library into duplicate directories, and because
    claim eligibility compares the *stored* title it also makes an ``exact``
    subscription unable to match its own back catalogue.
    """
    if not title:
        return title
    return _FORMAT_TAG_RE.sub("", title, count=1)


# The source's site-name suffix on page titles, as a family rather than one
# spelling: "... | Magazine PDF", "... | Download Magazine PDF",
# "... | Magazine [PDF]", "... - Magazine PDF", "... | Free Download", and a
# bare separator left dangling at the end ("... August 2026 -"). Issues indexed
# while the scraper did not strip a spelling still carry it in their stored
# title. One trailing segment per match: the separator needs whitespace before
# it (so hyphenated words survive), and the decoration comes from a closed
# vocabulary, so an unknown ending stays visible rather than being eaten.
_SOURCE_SUFFIX_RE = re.compile(
    r"\s+[|\-–—]"
    r"(?:\s+(?:(?:Download\s+)?(?:Magazine\s+)?(?:PDF|\[PDF\]|\(PDF\))|Free\s+Download))?"
    r"\s*$",
    re.IGNORECASE,
)
# "... - | Free Download" is a decoration after a dangling separator.
_MAX_SUFFIX_SEGMENTS = 3


def strip_source_decorations(title: str) -> str:
    """Remove source presentation from a title: format tag and site suffix.

    Neither is part of the publication's name or issue. A trailing suffix is
    worse than a tag: on a title with no other separator it becomes part of
    the magazine name, splitting the library and blocking ``exact`` matches,
    and elsewhere it ends up in the issue detail of the filename and in the
    title stamped into the PDF.
    """
    if not title:
        return title
    title = strip_format_tag(title)
    for _ in range(_MAX_SUFFIX_SEGMENTS):
        stripped = _SOURCE_SUFFIX_RE.sub("", title, count=1)
        if stripped == title:
            break
        title = stripped
    return title


def normalize_title(title: str) -> str:
    """Strip date/issue information and accents from a title to get the base magazine name.

    "The New Yorker – April 13, 2026" → "The New Yorker"
    "Bon Appétit – March 2026" → "Bon Appetit"
    "Science News – Vol 208 No 05, May 2026" → "Science News"
    "[PDF] Airliner World – January 2024" → "Airliner World"
    "GQ USA Fall 2026 | Magazine PDF" → "GQ USA Fall"
    """
    # Source presentation is never part of the magazine's name.
    title = strip_source_decorations(title)
    # Remove everything after common separators (–, -, |, :) if followed by date-like content
    separators = [" – ", " - ", " | ", ": "]
    for sep in separators:
        if sep in title:
            parts = title.split(sep, 1)
            suffix_lower = parts[1].lower()
            # Check if suffix looks like a date/issue string
            has_date_words = any(
                w in suffix_lower
                for w in list(MONTH_NAMES.keys()) + list(SEASON_MONTHS.keys())
                + ["vol", "issue", "edition", "no.", "no "]
            )
            has_year = bool(re.search(r"\b20\d{2}\b", suffix_lower))
            if has_date_words or has_year:
                return strip_accents(parts[0].strip())

    # If no separator found, try to strip trailing year
    m = re.match(r"^(.+?)\s+\d{4}$", title)
    if m:
        return strip_accents(m.group(1).strip())

    return strip_accents(title.strip())


def _extract_issue_detail(title: str, norm_title: str) -> str:
    """Extract the issue-specific detail from a full title.

    "The Economist - April 4th-10th, 2026" → "April 4th-10th"
    "Science News - Vol 208 No 05, May 2026" → "Vol 208 No 05 May"
    "New Scientist - 21 March 2026" → "21 March"
    """
    # Remove the magazine name prefix (try common separators)
    detail = title
    for sep in [" – ", " - ", " | ", ": "]:
        if sep in title:
            detail = title.split(sep, 1)[1].strip()
            break

    # Remove trailing year
    detail = re.sub(r",?\s*\b20\d{2}\b\s*$", "", detail).strip()
    # Remove leading/trailing punctuation and clean up stray commas
    detail = detail.strip(" ,.-–")
    detail = re.sub(r"\s*,\s*$", "", detail)

    return detail if detail else "Issue"


def _sanitize_filename(s: str) -> str:
    """Sanitize a string for use as a filename."""
    s = re.sub(r'[<>:"/\\|?*]', " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def organize_path(
    title: str,
    page_url: str,
    output_dir: str,
    filename: str | None = None,
) -> Path:
    """Determine the full output path for a magazine PDF.

    Returns: {output_dir}/{Magazine Title}/{Magazine Title} - {YYYY}-{MM} - {Detail}.pdf

    Flat structure for Komga/Kavita compatibility. Filenames are uniform
    and sort chronologically.
    """
    title = strip_source_decorations(title)
    parsed = parse_date(title, page_url)
    norm_title = normalize_title(title)

    if not filename:
        if not parsed.year:
            # No date patterns matched — fall back to sanitized original title
            filename = _sanitize_filename(title) + ".pdf"
        else:
            # Build uniform filename: {Title} - {YYYY}-{MM} - {Detail}.pdf
            if parsed.month:
                date_part = f"{parsed.year}-{parsed.month:02d}"
            else:
                date_part = str(parsed.year)

            detail = _extract_issue_detail(title, norm_title)
            detail = _sanitize_filename(detail)

            parts = [_sanitize_filename(norm_title), date_part]
            if detail:
                parts.append(detail)
            filename = " - ".join(parts) + ".pdf"

    return Path(output_dir) / _sanitize_filename(norm_title) / filename
