"""Version-2 stamps read through each target app's own reader.

Every reader here is an independent port of the app's source (see each
emulation module's docstring for the upstream commit); none uses magsync's code.
"""

from __future__ import annotations

import io

import pytest
from kavita_emulation import kavita_comicinfo, kavita_metadata
from pdf_factory import containers
from pypdf import PdfReader
from reader_emulation import (
    booklore_view,
    calibre_prefers_info,
    calibre_view,
    grimmory_view,
    strict_xmp_view,
)

from magsync.core import pdfmeta

AFAR = {"title": "AFAR – Spring 2025", "page_url": "https://freemagazines.top/afar-spring-2025", "genre": "Travel, Lifestyle"}
CONTAINERS = containers()


def _spec(issue=AFAR):
    return pdfmeta.stamp_spec_for(issue)


def _stamped(source: bytes, spec=None) -> bytes:
    outcome = pdfmeta.stamp_payload(source, spec or _spec())
    assert outcome.reason is None, (outcome.reason, outcome.error)
    return source + outcome.appended


def _packet(data: bytes) -> bytes:
    return PdfReader(io.BytesIO(data)).trailer["/Root"]["/Metadata"].get_data()


# --- the packet, as calibre and strict XMP parsers read it --------------------------------


@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_calibre_reads_series_index_date_and_genres(kind):
    view = calibre_view(_packet(_stamped(CONTAINERS[kind])))
    assert (view["series"], view["series_index"]) == ("AFAR", 2025060.0)
    assert view["title"] == "AFAR – Spring 2025"
    assert view["pubdate_raw"] == "2025-03-01" and not view["pubdate_needs_fill"]
    assert view["tags"] == ["Travel", "Lifestyle"]
    assert view["authors"] == [] and not view["comments"]


def test_calibre_fills_in_a_year_only_date():
    """calibre invents month and day for a bare year; the stamp keeps dc:date honest anyway."""
    spec = _spec({"title": "Annual Guide – 27th Edition, 2026"})
    view = calibre_view(_packet(_stamped(CONTAINERS["classic"], spec)))
    assert view["pubdate_raw"] == "2026" and view["pubdate_needs_fill"]
    assert (view["series"], view["series_index"]) == ("Annual Guide", 2026000.0)


def test_calibre_keeps_the_packet_authoritative():
    """No xmp:MetadataDate is written, so calibre never prefers the Info dict."""
    packet = _packet(_stamped(CONTAINERS["classic"]))
    assert calibre_prefers_info(packet, "2030-01-01T00:00:00+00:00") is False


def test_strict_parser_keeps_dublin_core_and_drops_only_unknown_vocabularies():
    view = strict_xmp_view(_packet(_stamped(CONTAINERS["classic"])), pdfdoc_decoding=False)
    kept = [block["kept"] for block in view["descriptions"]]
    assert kept == [True, False, False, False, False]  # Dublin Core alone survives
    (dc,) = view["dc"]
    assert dc["title"] == "AFAR – Spring 2025"
    assert (dc["dates"], dc["subjects"], dc["creators"]) == (["2025-03-01"], ["Travel", "Lifestyle"], [])


# --- whole files, as Booklore and Grimmory read them ------------------------------------


@pytest.mark.parametrize("kind", list(CONTAINERS))
@pytest.mark.parametrize("zone", ["Pacific/Pago_Pago", "UTC", "Pacific/Kiritimati"])
def test_booklore_reads_series_and_takes_the_date_from_info(kind, zone):
    view = booklore_view(_stamped(CONTAINERS[kind]), zone=zone)
    assert (view["seriesName"], view["seriesNumber"]) == ("AFAR", 2025060.0)
    assert view["title"] == "AFAR – Spring 2025"
    assert view["authors"] in ([], set(), None) and not view["description"]
    if zone != "Pacific/Kiritimati":  # UTC+14 is beyond the noon margin, as the spec says
        assert view["publishedDate"] == "2025-03-01"


def test_booklore_ignores_dc_date_so_an_undated_issue_keeps_the_sources_date():
    spec = _spec({"title": "Mystery Magazine", "page_url": "https://x/mystery"})
    view = booklore_view(_stamped(CONTAINERS["classic"], spec), zone="UTC")
    source_date = PdfReader(io.BytesIO(CONTAINERS["classic"])).metadata.get("/CreationDate")
    assert (view["publishedDate"] is None) == (source_date is None)


@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_grimmory_reads_series_index_and_dc_date(kind):
    view = grimmory_view(_stamped(CONTAINERS[kind]))
    assert (view["seriesName"], view["seriesNumber"]) == ("AFAR", 2025060.0)
    assert view["publishedDate"] == "2025-03-01"
    assert view["flagged"] == []  # no local name repeated across namespaces


def test_grimmorys_local_name_matching_sees_the_same_series_twice():
    """bookorbit:seriesName and calibre:series agree, so Grimmory's order never matters."""
    view = grimmory_view(_stamped(CONTAINERS["classic"]))
    assert view["custom_fields"]["bookorbit:seriesName"] == "AFAR"
    assert view["seriesName"] == "AFAR"


# --- whole files, as Kavita reads them (older table entries win; older revisions applied first) ---


@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_kavita_reads_the_stamp_whatever_the_sources_chain(kind):
    data = _stamped(CONTAINERS[kind])
    metadata = kavita_metadata(data)
    assert metadata["Title"] == "AFAR – Spring 2025"
    assert (metadata["Series"], metadata["Volume"]) == ("AFAR", "2025060")
    assert metadata["CreationDate"] == "2025-03-01"
    # Present but empty in the stamped revision: the source's junk is replaced.
    assert metadata.get("Author", "") == "" and metadata.get("Summary", "") == ""
    assert metadata.get("Keywords", "") == ""
    assert "downmagaz.net" not in repr(metadata)


@pytest.mark.parametrize("kind", list(CONTAINERS))
def test_kavita_comicinfo_groups_issues_under_the_magazine(kind):
    info = kavita_comicinfo(_stamped(CONTAINERS[kind]))
    assert (info["Series"], info["Volume"]) == ("AFAR", "2025060")
    assert (info["Year"], info["Month"], info["Day"]) == (2025, 3, 1)
    assert info["Writer"] == "" and "downmagaz.net" not in repr(info)


def test_kavita_reads_a_version_1_stamp_over_a_table_as_junk():
    """Why version 2 exists: the 0.10.x layout is invisible to Kavita when a table is in the chain."""
    for kind in ("classic", "xref-stream"):
        source = CONTAINERS[kind]
        v1 = source + pdfmeta.stamp_payload(source, _spec(), version=1).appended
        assert kavita_metadata(v1)["Title"] == "downmagaz.net"
