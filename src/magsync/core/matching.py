"""Canonical subscription matching shared by promotion, claims, and filtering.

One matcher, one definition of "matches": re-encounter promotion, the startup
backfill, claim/preview eligibility, and result filtering must never drift
apart again (they did in 0.6.0, which downloaded never-subscribed magazines).

Two deliberately different tests live here:

- ``title_match`` — provenance promotion.  Title only; a subscription's
  ``since`` floor is *excluded* so that loosening the floor later can revive
  already-promoted rows at claim time without any re-scrape.
- ``passes_since`` — claim-time eligibility, applied on top of ``title_match``
  for ``subscription``-provenance rows.  Reproduces the pre-0.6.0
  ``get_issues`` floor semantics, including excluding unparsed-year issues
  from ``since``-scoped work.

Matching always evaluates the *issue's own* title (``normalize_title`` of it),
never a stored magazine row's ``normalized_title`` — historic fuzzy-search
grouping can file stranger issues under a subscribed magazine record.

Canonicalization here folds apostrophe and dash variants so curly punctuation
in scraped titles cannot defeat a straight-punctuation subscription (a known
gap).  It is scoped to matching only: ``normalize_title`` itself, magazine
grouping, and organizer paths are unchanged.
"""

from __future__ import annotations

import re
from typing import Iterable, Mapping, Protocol

from magsync.core.organizer import normalize_title, strip_accents


class SubscriptionLike(Protocol):
    query: str
    since: str | None
    exact: bool


_APOSTROPHE_VARIANTS = str.maketrans({
    "‘": "'",  # ' left single quote
    "’": "'",  # ' right single quote (curly apostrophe)
    "ʼ": "'",  # ʼ modifier letter apostrophe
    "`": "'",
})

_DASH_VARIANTS = str.maketrans({
    "–": "-",  # – en dash
    "—": "-",  # — em dash
    "−": "-",  # − minus sign
})

_WHITESPACE_RE = re.compile(r"\s+")


def canonicalize_for_match(text: str) -> str:
    """Fold a title or query into the form both sides of a match compare in."""
    folded = strip_accents(text).translate(_APOSTROPHE_VARIANTS).translate(_DASH_VARIANTS)
    return _WHITESPACE_RE.sub(" ", folded).strip().casefold()


def title_match(issue_title: str, sub: SubscriptionLike) -> bool:
    """Promotion test: does the issue's own title match this subscription?

    Substring containment for ordinary subscriptions, equality for ``exact``.
    ``since`` is deliberately not consulted here (see module docstring).
    """
    if not issue_title or not sub.query:
        return False
    title = canonicalize_for_match(normalize_title(issue_title))
    query = canonicalize_for_match(sub.query)
    if not title or not query:
        return False
    if sub.exact:
        return title == query
    return query in title


def parse_since(since: str | None) -> tuple[int, int | None] | None:
    """Parse a subscription's ``YYYY[-MM]`` floor; malformed values mean no floor.

    A malformed floor must not strand every issue of an otherwise-valid
    subscription (fail-closed here would mean "never download anything"), so
    it degrades to unbounded — the pre-0.6.0 permissive default.
    """
    if not since:
        return None
    parts = str(since).split("-")
    try:
        year = int(parts[0])
        month = int(parts[1]) if len(parts) > 1 and parts[1] else None
    except ValueError:
        return None
    if month is not None and not 1 <= month <= 12:
        month = None
    return (year, month)


def passes_since(year: int | None, month: int | None, sub: SubscriptionLike) -> bool:
    """Eligibility floor, reproducing ``get_issues``'s since semantics.

    With a floor set, an issue with no parsed year fails (as SQL NULL
    comparisons excluded it pre-0.6.0); year-only issues fail a year+month
    floor unless their year is strictly greater.
    """
    floor = parse_since(sub.since)
    if floor is None:
        return True
    since_year, since_month = floor
    if year is None:
        return False
    if since_month is None:
        return year >= since_year
    if year > since_year:
        return True
    return year == since_year and month is not None and month >= since_month


def matches_subscription(issue: Mapping, sub: SubscriptionLike) -> bool:
    """Full claim-time eligibility against one subscription: title and since."""
    return title_match(issue.get("title") or "", sub) and passes_since(
        issue.get("year"), issue.get("month"), sub
    )


def title_matches_any(issue_title: str, subscriptions: Iterable[SubscriptionLike]) -> bool:
    """Promotion test against a subscription snapshot (title only)."""
    return any(title_match(issue_title, sub) for sub in subscriptions)


def eligible_for_any(issue: Mapping, subscriptions: Iterable[SubscriptionLike]) -> bool:
    """Claim-time eligibility against a snapshot: some sub matches title AND since."""
    return any(matches_subscription(issue, sub) for sub in subscriptions)
