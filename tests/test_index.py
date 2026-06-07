"""Tests for add_issues backfill behavior and missing-URL queries."""

from __future__ import annotations

from magsync.core.index import MagazineIndex

LW_A = "https://limewire.com/d/aaaa#k1"
LW_B = "https://limewire.com/d/bbbb#k2"


def _index(tmp_path) -> MagazineIndex:
    return MagazineIndex(db_path=tmp_path / "index.db")


def test_backfill_fills_null_limewire_url(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    added = idx.add_issues(mag, [{"title": "T", "page_url": "p1", "limewire_url": None}])
    assert added == 1

    # Re-scrape now yields a URL for the same page_url → backfill, not a new row.
    added2 = idx.add_issues(mag, [{"title": "T", "page_url": "p1", "limewire_url": LW_A}])
    assert added2 == 0

    rows = idx.get_issues()
    assert rows[0]["limewire_url"] == LW_A
    idx.close()


def test_backfill_does_not_overwrite_populated_url(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(mag, [{"title": "T", "page_url": "p1", "limewire_url": LW_A}])
    idx.add_issues(mag, [{"title": "T", "page_url": "p1", "limewire_url": LW_B}])

    rows = idx.get_issues()
    assert rows[0]["limewire_url"] == LW_A  # original preserved
    idx.close()


def test_title_is_not_backfilled(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(mag, [{"title": "Original Title", "page_url": "p1", "limewire_url": None}])
    idx.add_issues(mag, [{"title": "Changed Title", "page_url": "p1", "limewire_url": LW_A}])

    rows = idx.get_issues()
    assert rows[0]["title"] == "Original Title"  # title untouched (drives derived fields)
    assert rows[0]["limewire_url"] == LW_A  # leaf field still backfilled
    idx.close()


def test_genuinely_new_page_url_is_counted(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    assert idx.add_issues(mag, [{"title": "A", "page_url": "pa", "limewire_url": LW_A}]) == 1
    assert idx.add_issues(mag, [{"title": "B", "page_url": "pb", "limewire_url": LW_B}]) == 1
    idx.close()


def test_get_issues_missing_url_and_set(tmp_path):
    idx = _index(tmp_path)
    mag = idx.get_or_create_magazine("Mag", "mag")
    idx.add_issues(
        mag,
        [
            {"title": "A", "page_url": "pa", "limewire_url": None},
            {"title": "B", "page_url": "pb", "limewire_url": LW_B},
        ],
    )
    missing = idx.get_issues_missing_url()
    assert [m["page_url"] for m in missing] == ["pa"]

    idx.set_limewire_url(missing[0]["id"], LW_A)
    assert idx.get_issues_missing_url() == []
    idx.close()


def test_get_issues_missing_url_filtered_by_magazine(tmp_path):
    idx = _index(tmp_path)
    a = idx.get_or_create_magazine("Alpha", "alpha")
    b = idx.get_or_create_magazine("Beta", "beta")
    idx.add_issues(a, [{"title": "A", "page_url": "pa", "limewire_url": None}])
    idx.add_issues(b, [{"title": "B", "page_url": "pb", "limewire_url": None}])

    missing = idx.get_issues_missing_url(magazine_title="alpha")
    assert [m["page_url"] for m in missing] == ["pa"]
    idx.close()
