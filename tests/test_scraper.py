"""Tests for LimeWire URL extraction in the detail-page scraper."""

from __future__ import annotations

import httpx

from magsync.core.scraper import _valid_limewire_url, scrape_detail_page

PAGE_URL = "https://freemagazines.top/the-economist-uk-6-june-2026/"


def _client(html: str) -> httpx.AsyncClient:
    """An AsyncClient whose every GET returns the given HTML (no network)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_data_url_markup_extracts_url_with_fragment():
    # Current freemagazines.top template: real URL is in data-url, href is "#".
    html = (
        '<html><body>'
        '<a href="#" data-url="https://limewire.com/d/4Kkl8#sg7w7sWnEZ" '
        'class="lw-vk-download-btn is-waiting" rel="nofollow noopener">'
        '<span class="btn-text">Scroll down to download</span></a>'
        '</body></html>'
    )
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url == "https://limewire.com/d/4Kkl8#sg7w7sWnEZ"


async def test_legacy_href_markup_still_extracts_url_with_fragment():
    html = (
        '<html><body>'
        '<a href="https://limewire.com/d/4Kkl8#sg7w7sWnEZ">Download</a>'
        '</body></html>'
    )
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url == "https://limewire.com/d/4Kkl8#sg7w7sWnEZ"


async def test_no_limewire_reference_returns_none():
    html = '<html><body><p>No download here.</p></body></html>'
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url is None


async def test_regex_fallback_for_non_carrier_element():
    # URL only in inline script text — no data-url attribute and no anchor href,
    # so only the whole-page regex fallback can find it.
    html = (
        '<html><body>'
        '<script>var u = "https://limewire.com/d/9ZxQ#fragKey42";</script>'
        '</body></html>'
    )
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url == "https://limewire.com/d/9ZxQ#fragKey42"


async def test_fragmentless_candidate_rejected():
    html = '<html><body><a href="#" data-url="https://limewire.com/d/abc">x</a></body></html>'
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url is None


async def test_valid_candidate_wins_over_malformed():
    # First data-url is fragmentless (invalid); the second is valid.
    html = (
        '<html><body>'
        '<a href="#" data-url="https://limewire.com/d/abc">bad</a>'
        '<a href="#" data-url="https://limewire.com/d/Good1#key9">ok</a>'
        '</body></html>'
    )
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url == "https://limewire.com/d/Good1#key9"


def test_valid_limewire_url_helper():
    assert (
        _valid_limewire_url("https://limewire.com/d/4Kkl8#sg7w7sWnEZ")
        == "https://limewire.com/d/4Kkl8#sg7w7sWnEZ"
    )
    assert _valid_limewire_url("https://limewire.com/d/abc") is None  # no fragment
    assert _valid_limewire_url("#") is None
    assert _valid_limewire_url("") is None
    assert _valid_limewire_url(None) is None
    # HTML entities are unescaped before validation.
    assert (
        _valid_limewire_url("https://limewire.com/d/x#a&amp;b")
        == "https://limewire.com/d/x#a&b"
    )
