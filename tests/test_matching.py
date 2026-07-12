"""Canonical subscription matcher: the one definition of "matches"."""

from magsync.core.matching import (
    canonicalize_for_match,
    eligible_for_any,
    matches_subscription,
    parse_since,
    passes_since,
    title_match,
    title_matches_any,
)
from magsync.core.models import Subscription


def sub(query, since=None, exact=False):
    return Subscription(query=query, since=since, exact=exact)


class TestCanonicalization:
    def test_curly_apostrophe_folds_to_straight(self):
        assert canonicalize_for_match("Cook’s Illustrated") == "cook's illustrated"

    def test_en_and_em_dash_fold_to_hyphen(self):
        assert canonicalize_for_match("Getaway – April") == "getaway - april"
        assert canonicalize_for_match("Getaway — April") == "getaway - april"

    def test_accents_and_case_fold(self):
        assert canonicalize_for_match("Bon Appétit") == "bon appetit"

    def test_whitespace_collapses(self):
        assert canonicalize_for_match("  The   Economist ") == "the economist"


class TestTitleMatch:
    def test_substring_for_non_exact(self):
        assert title_match("The Economist Audio - June 2026", sub("The Economist"))

    def test_exact_requires_equality(self):
        s = sub("The Economist", exact=True)
        assert title_match("The Economist - June 2026", s)
        assert not title_match("The Economist Audio - June 2026", s)

    def test_curly_apostrophe_title_matches_straight_query(self):
        assert title_match("Cook’s Illustrated - August 2026", sub("Cook's Illustrated"))

    def test_en_dash_title_still_matches(self):
        # The dash variant changes date-suffix stripping, not the base title.
        assert title_match("Getaway – April/May 2026", sub("Getaway"))
        assert title_match("Getaway - April/May 2026", sub("Getaway"))

    def test_stranger_does_not_match(self):
        assert not title_match("Women's Golf Americas - Spring 2026", sub("Getaway"))
        assert not title_matches_any(
            "Military Aviation World War II Air Combat – Issue 2",
            [sub("Aviation News"), sub("New Scientist")],
        )

    def test_empty_inputs_never_match(self):
        assert not title_match("", sub("Getaway"))
        assert not title_match("Getaway - April 2026", sub(""))

    def test_since_is_ignored_by_title_match(self):
        # Promotion must not consult the floor.
        assert title_match("Getaway - April 2024", sub("Getaway", since="2026-01"))


class TestSince:
    def test_malformed_since_means_no_floor(self):
        assert parse_since("not-a-date") is None
        assert parse_since("") is None
        assert passes_since(1999, 1, sub("X", since="not-a-date"))

    def test_year_only_floor(self):
        s = sub("X", since="2025")
        assert passes_since(2025, None, s)
        assert passes_since(2026, 1, s)
        assert not passes_since(2024, 12, s)

    def test_year_month_floor(self):
        s = sub("X", since="2025-06")
        assert passes_since(2025, 6, s)
        assert passes_since(2026, 1, s)
        assert not passes_since(2025, 5, s)
        # Year-only issue fails a year+month floor unless strictly newer.
        assert not passes_since(2025, None, s)
        assert passes_since(2026, None, s)

    def test_unparsed_year_fails_scoped_floor(self):
        assert not passes_since(None, None, sub("X", since="2025"))
        assert passes_since(None, None, sub("X"))  # no floor: passes


class TestEligibility:
    def test_eligibility_is_title_and_since_from_same_sub(self):
        issue = {"title": "Getaway - April 2024", "year": 2024, "month": 4}
        # Title matches sub A (but fails A's floor); floor passes sub B (but
        # title doesn't match B) — neither alone may qualify the issue.
        a = sub("Getaway", since="2026-01")
        b = sub("Dish")
        assert not eligible_for_any(issue, [a, b])
        assert eligible_for_any(issue, [sub("Getaway", since="2024-01")])

    def test_matches_subscription_uses_issue_title_not_magazine(self):
        issue = {
            "title": "Women's Golf Americas - Spring 2026",
            "year": 2026,
            "month": 4,
            "magazine_title": "Getaway",
            "normalized_title": "getaway",
        }
        assert not matches_subscription(issue, sub("Getaway"))
