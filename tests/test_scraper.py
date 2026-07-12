"""Tests for validated, cycle-scoped source scraping."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest

from magsync.core.models import SourceError, SourceFailureKind
from magsync.core.scraper import (
    FreemagazinesClient,
    _valid_limewire_url,
    scrape_detail_page,
)

PAGE_URL = "https://freemagazines.top/the-economist-uk-6-june-2026/"


def _client(html: str) -> httpx.AsyncClient:
    """An AsyncClient whose every GET returns the given HTML (no network)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, headers={"content-type": "text/html"})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@asynccontextmanager
async def _source(handler, **kwargs):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    ) as http_client:
        yield FreemagazinesClient(http_client=http_client, scrape_delay=0, **kwargs)


def _html(
    status: int,
    body: str,
    *,
    headers: dict[str, str] | None = None,
    request: httpx.Request | None = None,
) -> httpx.Response:
    response_headers = {"content-type": "text/html"}
    response_headers.update(headers or {})
    return httpx.Response(status, text=body, headers=response_headers, request=request)


async def test_data_url_markup_extracts_url_with_fragment():
    # Current freemagazines.top template: real URL is in data-url, href is "#".
    html = (
        "<html><body>"
        '<a href="#" data-url="https://limewire.com/d/4Kkl8#sg7w7sWnEZ" '
        'class="lw-vk-download-btn is-waiting" rel="nofollow noopener">'
        '<span class="btn-text">Scroll down to download</span></a>'
        "</body></html>"
    )
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url == "https://limewire.com/d/4Kkl8#sg7w7sWnEZ"


async def test_legacy_href_markup_still_extracts_url_with_fragment():
    html = (
        "<html><body>"
        '<a href="https://limewire.com/d/4Kkl8#sg7w7sWnEZ">Download</a>'
        "</body></html>"
    )
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url == "https://limewire.com/d/4Kkl8#sg7w7sWnEZ"


async def test_no_limewire_reference_returns_none():
    html = "<html><body><p>No download here.</p></body></html>"
    async with _client(html) as client:
        issue = await scrape_detail_page(PAGE_URL, client=client)
    assert issue.limewire_url is None


async def test_regex_fallback_for_non_carrier_element():
    # URL only in inline script text — no data-url attribute and no anchor href,
    # so only the whole-page regex fallback can find it.
    html = (
        "<html><body>"
        '<script>var u = "https://limewire.com/d/9ZxQ#fragKey42";</script>'
        "</body></html>"
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
        "<html><body>"
        '<a href="#" data-url="https://limewire.com/d/abc">bad</a>'
        '<a href="#" data-url="https://limewire.com/d/Good1#key9">ok</a>'
        "</body></html>"
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


@pytest.mark.parametrize("status", [200, 403])
async def test_challenge_header_blocks_200_and_403(status, caplog):
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return _html(
            status,
            "<html>secret challenge token must not be logged</html>",
            headers={"cf-mitigated": "challenge", "cf-ray": "abc123-SJC"},
        )

    async with _source(handler) as source:
        with caplog.at_level("WARNING", logger="magsync"):
            result = await source.search("Example")

        assert result.failure is not None
        assert result.failure.kind is SourceFailureKind.ACCESS_BLOCKED
        assert result.failure.status_code == status
        assert result.failure.host == "freemagazines.top"
        assert result.failure.cf_ray == "abc123-SJC"
        assert source.circuit_open

    assert requests == 1
    assert "secret challenge token" not in caplog.text


async def test_challenge_body_backstop_requires_combined_markers():
    challenge_body = (
        "<html><head><title>Just a moment...</title></head>"
        "<body>Enable JavaScript and cookies to continue</body></html>"
    )

    async with _source(lambda request: _html(200, challenge_body)) as source:
        result = await source.search("Example")

    assert result.failure is not None
    assert result.failure.kind is SourceFailureKind.ACCESS_BLOCKED


async def test_validated_no_results_is_distinct_from_unknown_html():
    responses = iter(
        [
            _html(
                200,
                '<html><body class="search-no-results"><h1>Nothing Found</h1></body></html>',
            ),
            _html(200, "<html><body><h1>Maintenance</h1></body></html>"),
        ]
    )

    async with _source(lambda request: next(responses)) as source:
        empty = await source.search("Missing")
        unknown = await source.search("Unknown")

    assert empty.success
    assert empty.validated_empty
    assert empty.items == []
    assert unknown.failure is not None
    assert unknown.failure.kind is SourceFailureKind.PROTOCOL
    assert not unknown.validated_empty


async def test_first_page_404_is_protocol_failure():
    async with _source(lambda request: _html(404, "not found")) as source:
        result = await source.search("Example")

    assert result.failure is not None
    assert result.failure.kind is SourceFailureKind.PROTOCOL
    assert result.failure.status_code == 404


async def test_later_page_404_is_expected_pagination_end():
    requests: list[str] = []
    page_one = (
        '<html><body><a href="https://freemagazines.top/example-july-2026/">Issue</a>'
        '<a class="next" href="https://freemagazines.top/page/2/?s=Example">Next</a>'
        "</body></html>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path == "/page/2/":
            return _html(404, "not found")
        return _html(200, page_one)

    async with _source(handler) as source:
        result = await source.search("Example")

    assert result.success
    assert [item.page_url for item in result.items] == [
        "https://freemagazines.top/example-july-2026/"
    ]
    assert requests == ["/", "/page/2/"]


async def test_wrong_final_origin_is_protocol_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "freemagazines.top":
            return httpx.Response(
                302, headers={"location": "https://notfreemagazines.top/redirected"}
            )
        return _html(200, "<html></html>")

    async with _source(handler) as source:
        result = await source.search("Example")

    assert result.failure is not None
    assert result.failure.kind is SourceFailureKind.PROTOCOL
    assert result.failure.host == "notfreemagazines.top"


async def test_non_html_response_is_protocol_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"items": []},
            headers={"content-type": "application/json"},
        )

    async with _source(handler) as source:
        result = await source.search("Example")

    assert result.failure is not None
    assert result.failure.kind is SourceFailureKind.PROTOCOL


@pytest.mark.parametrize("status", [429, 500, 503])
async def test_transient_http_status_is_typed(status):
    async with _source(lambda request: _html(status, "temporary")) as source:
        result = await source.search("Example")

    assert result.failure is not None
    assert result.failure.kind is SourceFailureKind.TRANSIENT
    assert result.failure.status_code == status


async def test_network_failure_is_typed_transient():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection failed", request=request)

    async with _source(handler) as source:
        result = await source.search("Example")

    assert result.failure is not None
    assert result.failure.kind is SourceFailureKind.TRANSIENT


async def test_global_pacing_covers_search_detail_and_later_search():
    now = [0.0]
    starts: list[float] = []

    async def fake_sleep(delay: float) -> None:
        now[0] += delay

    def handler(request: httpx.Request) -> httpx.Response:
        starts.append(now[0])
        if request.url.path == PAGE_URL.removeprefix("https://freemagazines.top"):
            return _html(200, "<html><head><title>Issue</title></head></html>")
        return _html(
            200, '<html><body class="search-no-results">Nothing Found</body></html>'
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        source = FreemagazinesClient(
            http_client=http_client,
            scrape_delay=1.0,
            _clock=lambda: now[0],
            _sleep=fake_sleep,
        )
        await source.search("First")
        await source.scrape_detail(PAGE_URL)
        await source.search("Second")

    assert starts == [0.0, 1.0, 2.0]


async def test_http_client_cookie_jar_is_reused():
    seen_cookies: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_cookies.append(request.headers.get("cookie"))
        headers = (
            {"set-cookie": "source_session=abc; Path=/"}
            if len(seen_cookies) == 1
            else None
        )
        return _html(
            200,
            '<html><body class="search-no-results">Nothing Found</body></html>',
            headers=headers,
        )

    async with _source(handler) as source:
        await source.search("First")
        await source.search("Second")

    assert seen_cookies == [None, "source_session=abc"]


async def test_open_circuit_short_circuits_all_later_source_calls():
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return _html(403, "blocked", headers={"cf-mitigated": "challenge"})

    async with _source(handler) as source:
        first = await source.search("First")
        second = await source.search("Second")
        with pytest.raises(SourceError) as detail_error:
            await source.scrape_detail(PAGE_URL)

    assert first.failure is not None
    assert second.failure is not None
    assert second.failure.kind is SourceFailureKind.ACCESS_BLOCKED
    assert detail_error.value.kind is SourceFailureKind.ACCESS_BLOCKED
    assert requests == 1

    # A fresh cycle/client has a closed circuit and permits a new probe.
    async with _source(
        lambda request: _html(
            200,
            '<html><body class="search-no-results">Nothing Found</body></html>',
        )
    ) as fresh_source:
        fresh = await fresh_source.search("Fresh")
    assert fresh.success


async def test_detail_failure_is_isolated_from_valid_sibling():
    search_page = (
        "<html><body>"
        '<a href="https://freemagazines.top/good-july-2026/">Good</a>'
        '<a href="https://freemagazines.top/bad-july-2026/">Bad</a>'
        "</body></html>"
    )
    request_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        request_paths.append(request.url.path)
        if request.url.path == "/":
            return _html(200, search_page)
        if request.url.path == "/good-july-2026/":
            return _html(
                200,
                '<html><head><meta property="og:title" content="Good July 2026"></head>'
                '<body><a data-url="https://limewire.com/d/Good#key">Download</a></body></html>',
            )
        return httpx.Response(
            200,
            json={"unexpected": True},
            headers={"content-type": "application/json"},
        )

    async with _source(handler, detail_concurrency=2) as source:
        result = await source.search_with_details("Example")

    assert result.success
    assert result.partial
    assert [item.title for item in result.items] == ["Good July 2026"]
    assert len(result.failures) == 1
    assert result.failures[0].kind is SourceFailureKind.PROTOCOL
    assert sorted(request_paths) == ["/", "/bad-july-2026/", "/good-july-2026/"]


async def test_concurrent_details_obey_global_pacing_and_concurrency_bound():
    now = [0.0]
    starts: list[float] = []
    active_details = 0
    maximum_active_details = 0

    async def fake_sleep(delay: float) -> None:
        now[0] += delay

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active_details, maximum_active_details
        starts.append(now[0])
        if request.url.path == "/":
            links = "".join(
                f'<a href="https://freemagazines.top/issue-{number}-2026/">Issue</a>'
                for number in range(3)
            )
            return _html(200, f"<html><body>{links}</body></html>")

        active_details += 1
        maximum_active_details = max(maximum_active_details, active_details)
        await asyncio.sleep(0)
        active_details -= 1
        return _html(200, "<html><head><title>Issue</title></head></html>")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        source = FreemagazinesClient(
            http_client=http_client,
            scrape_delay=1.0,
            detail_concurrency=2,
            _clock=lambda: now[0],
            _sleep=fake_sleep,
        )
        result = await source.search_with_details("Issue")

    assert result.success
    assert len(result.items) == 3
    assert starts == [0.0, 1.0, 2.0, 3.0]
    assert maximum_active_details == 2
