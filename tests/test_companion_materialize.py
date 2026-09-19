"""Incremental demand reconciliation, its persistence and transaction safety."""

from __future__ import annotations

import pytest

from magsync.companion.store import Limits, Store
from magsync.core.index import MagazineIndex


@pytest.fixture
def index(tmp_path):
    idx = MagazineIndex(tmp_path / "index.db")
    yield idx
    idx.close()


def _tables(idx):
    return {row[0] for row in idx.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def test_additive_table_is_created_for_existing_v1_stores(tmp_path, index):
    assert "companion_materialized" in _tables(index)
    index.conn.execute("DROP TABLE companion_materialized")
    index.conn.commit()
    index.close()
    reopened = MagazineIndex(tmp_path / "index.db")
    try:
        assert "companion_materialized" in _tables(reopened)
    finally:
        reopened.close()


def test_title_repair_clears_every_watermark(index):
    magazine = index.get_or_create_magazine("Science News", "science news")
    index.add_issues(magazine, [{"title": "[PDF] Science News - June 2025", "year": 2025, "month": 6,
                                 "page_url": "https://freemagazines.top/sn-2025"}])
    issue_id = index.conn.execute("SELECT id FROM issues").fetchone()[0]
    index.conn.execute("INSERT INTO companion_materialized VALUES ('sub','fp',99)")
    index.conn.commit()
    index.repair_issue_title(issue_id, title="Science News - June 2025", magazine_id=magazine,
                             year=2025, month=6, date_raw="Science News - June 2025")
    assert index.conn.execute("SELECT count(*) FROM companion_materialized").fetchone()[0] == 0


def test_committing_index_methods_refuse_to_run_inside_a_transaction(index):
    store = Store(index, Limits())
    magazine = index.get_or_create_magazine("Science News", "science news")
    with pytest.raises(RuntimeError, match="transaction"):
        with store.transaction():
            index.add_issues(magazine, [{"title": "Science News - May 2025",
                                         "page_url": "https://freemagazines.top/sn-may"}])
    assert index.conn.execute("SELECT count(*) FROM issues").fetchone()[0] == 0
    with pytest.raises(RuntimeError):
        with store.transaction():
            index.update_pipeline_state("healthy")
