"""The source containers stamping is tested against have the shapes they claim."""

from __future__ import annotations

import io

import pytest
from pdf_factory import containers, last_section_form
from pypdf import PdfReader

EXPECTED = {
    "classic": ("table", 2),
    "xref-stream": ("stream", 2),
    "several-revisions": ("stream", 2),
    "object-stream-catalog": ("stream", 3),
    "linearized": ("table", 3),
    "hybrid-reference": ("table", 1),
    "no-xmp": ("table", 2),
}


def test_every_container_is_covered():
    assert set(containers()) == set(EXPECTED)


@pytest.mark.parametrize("kind", list(EXPECTED))
def test_container_form_and_page_count(kind):
    data = containers()[kind]
    form, pages = EXPECTED[kind]
    assert last_section_form(data) == form
    assert len(PdfReader(io.BytesIO(data)).pages) == pages


def test_compressed_catalog_and_info_fixtures():
    """Both object-stream fixtures keep the catalog and Info dict compressed."""
    for kind in ("object-stream-catalog", "hybrid-reference"):
        reader = PdfReader(io.BytesIO(containers()[kind]))
        root = reader.trailer.raw_get("/Root").idnum
        info = reader.trailer.raw_get("/Info").idnum
        assert {root, info} <= set(reader.xref_objStm), kind
