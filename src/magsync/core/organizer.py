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

    # Pattern 1: "Month DD, YYYY" or "Month DD-DD, YYYY"
    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s+\d{1,2}(?:\s*[-–]\s*\d{1,2})?,?\s*(\d{4})",
        title_lower,
    )
    if m:
        return ParsedDate(year=int(m.group(2)), month=MONTH_NAMES[m.group(1)])

    # Pattern 2: "DD Month YYYY" (European format)
    m = re.search(
        r"\d{1,2}\s+(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s+(\d{4})",
        title_lower,
    )
    if m:
        return ParsedDate(year=int(m.group(2)), month=MONTH_NAMES[m.group(1)])

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


def strip_accents(s: str) -> str:
    """Strip accent marks from a string, preserving base characters.

    "Bon Appétit" → "Bon Appetit"
    "Zürich" → "Zurich"
    """
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def normalize_title(title: str) -> str:
    """Strip date/issue information and accents from a title to get the base magazine name.

    "The New Yorker – April 13, 2026" → "The New Yorker"
    "Bon Appétit – March 2026" → "Bon Appetit"
    "Science News – Vol 208 No 05, May 2026" → "Science News"
    """
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


def organize_path(
    title: str,
    page_url: str,
    output_dir: str,
    filename: str | None = None,
) -> Path:
    """Determine the full output path for a magazine PDF.

    Returns: {output_dir}/{Normalized Title}/{YYYY}/{MM}/{filename}.pdf
    """
    parsed = parse_date(title, page_url)
    norm_title = normalize_title(title)

    # Build path components
    parts = [output_dir, norm_title]

    if parsed.year:
        parts.append(str(parsed.year))
        if parsed.month:
            parts.append(f"{parsed.month:02d}")
        else:
            parts.append("00-Unknown")
    else:
        parts.append("Unknown")

    # Generate filename if not provided
    if not filename:
        # Sanitize title for filename
        safe_title = re.sub(r'[<>:"/\\|?*]', "_", title)
        safe_title = re.sub(r"\s+", "_", safe_title)
        filename = f"{safe_title}.pdf"

    return Path(*parts) / filename
