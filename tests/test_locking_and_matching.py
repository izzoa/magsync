"""Portable locking import safety and compiled-matcher equivalence."""

from __future__ import annotations

import random
import subprocess
import sys
from types import SimpleNamespace

from magsync.core.matching import (
    canonical_issue_title,
    compile_subscription,
    matches_subscription,
    title_match,
)


def test_base_cli_imports_without_fcntl():
    code = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "fcntl":
            raise ImportError("no fcntl")
sys.meta_path.insert(0, Block())
sys.modules.pop("fcntl", None)
import magsync.cli
import magsync.core.locking as locking
assert locking.fcntl is None
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)


_TITLES = [
    "Science News - June 2025", "SCIENCE NEWS – July 2025", "Science News Explores – 2024",
    "[PDF] Science News - May 2023", "Scіence News", "The New Yorker – June 1, 2026",
    "Women’s Health - Spring 2026", "Women's Health", "Économiste — Hors-série 2025",
    "Economist", "The Economist Audio - Alias A", "Getaway – April/May 2026",
    "", "   ", "Dish", "Dish Magazine Issue 12",
]
_QUERIES = [
    "Science News", "science news", "SCIENCE", "Women's Health", "Women’s Health",
    "Economiste", "économiste", "The Economist", "Getaway", "Dish", "", "  ",
    "New Yorker", "Getaway - April",
]
_SINCE = [None, "2025", "2025-06", "2026-13", "abc", "2024-", ""]


def test_compiled_matcher_is_equivalent_to_reference():
    rng = random.Random(20260918)
    for _ in range(5000):
        title = rng.choice(_TITLES)
        sub = SimpleNamespace(
            query=rng.choice(_QUERIES), exact=rng.choice([True, False, 0, 1]),
            since=rng.choice(_SINCE),
        )
        year = rng.choice([None, 2023, 2024, 2025, 2026])
        month = rng.choice([None, 1, 5, 6, 12])
        compiled = compile_subscription(sub)
        canonical = canonical_issue_title(title)
        assert compiled.matches_title(canonical) == title_match(title, sub), (title, sub)
        expected = matches_subscription({"title": title, "year": year, "month": month}, sub)
        assert compiled.matches(canonical, year, month) == expected, (title, sub, year, month)
