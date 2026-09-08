"""Tests for validated, cycle-scoped source scraping."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest

from magsync.core.models import (
    LinkResolutionKind,
    SourceError,
    SourceFailureKind,
)
from magsync.core.scraper import (
    FreemagazinesClient,
    ScrapedIssue,
    _parse_detail_page,
    _valid_download_url,
    resolve_masked_links,
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


# Live template as of 2026-09-08: no limewire URL anywhere in the markup; the
# real link is behind a masked server-side lookup keyed by this attribute.
MASKED_TRIGGER = (
    '<button type="button" id="lw-vk-js-trigger" class="lw-vk-download-btn" '
    'data-key="dl_key_7d40aadd760489dfd1138c652765a2e8">Download</button>'
)


def test_masked_trigger_yields_download_key_and_no_inline_url():
    html = f"<html><body>{MASKED_TRIGGER}</body></html>"
    issue = _parse_detail_page(html, PAGE_URL)
    assert issue.limewire_url is None
    assert issue.download_key == "dl_key_7d40aadd760489dfd1138c652765a2e8"


def test_masked_key_found_by_class_without_trigger_id():
    # Element identity may change; the class-based path must still find it.
    html = (
        '<html><body><button class="lw-vk-download-btn" '
        'data-key="dl_key_abcdef0123456789">Download</button></body></html>'
    )
    issue = _parse_detail_page(html, PAGE_URL)
    assert issue.download_key == "dl_key_abcdef0123456789"


def test_masked_key_found_by_attribute_alone():
    # Neither the id nor the class survives, but the attribute still carries it.
    html = (
        '<html><body><a href="#" data-key="dl_key_0123456789abcdef">'
        "Download</a></body></html>"
    )
    issue = _parse_detail_page(html, PAGE_URL)
    assert issue.download_key == "dl_key_0123456789abcdef"


def test_inline_url_wins_over_masked_trigger():
    # An inline URL is authoritative and must cost no resolution request.
    html = (
        "<html><body>"
        '<a href="#" data-url="https://limewire.com/d/4Kkl8#sg7w7sWnEZ">ok</a>'
        f"{MASKED_TRIGGER}"
        "</body></html>"
    )
    issue = _parse_detail_page(html, PAGE_URL)
    assert issue.limewire_url == "https://limewire.com/d/4Kkl8#sg7w7sWnEZ"
    assert issue.download_key is None


def test_no_download_affordance_yields_neither_url_nor_key():
    html = "<html><body><p>No download here.</p></body></html>"
    issue = _parse_detail_page(html, PAGE_URL)
    assert issue.limewire_url is None
    assert issue.download_key is None


def test_ai_share_data_url_attributes_are_not_mistaken_for_a_key():
    # The live page carries many unrelated data-url attributes (AI summary
    # links). They must yield neither a URL nor a download key.
    html = (
        '<html><body><a data-url="https://claude.ai/new?q=Summarize+this">'
        "Ask</a></body></html>"
    )
    issue = _parse_detail_page(html, PAGE_URL)
    assert issue.limewire_url is None
    assert issue.download_key is None


def test_malformed_download_key_is_rejected():
    # Too short, and unsafe characters: never post page-controlled junk.
    for bad in ('data-key="short"', 'data-key="dl key with spaces"', 'data-key=""'):
        html = f"<html><body><button {bad}>x</button></body></html>"
        assert _parse_detail_page(html, PAGE_URL).download_key is None


def test_parse_detail_page_is_pure_and_needs_no_client():
    # Called with no HTTP client at all: parsing must never perform I/O.
    issue = _parse_detail_page(
        f"<html><body>{MASKED_TRIGGER}</body></html>", PAGE_URL
    )
    assert issue.download_key
    assert issue.limewire_url is None


def test_valid_download_url_helper():
    assert (
        _valid_download_url("https://limewire.com/d/4Kkl8#sg7w7sWnEZ")
        == "https://limewire.com/d/4Kkl8#sg7w7sWnEZ"
    )
    assert _valid_download_url("https://limewire.com/d/abc") is None  # no fragment
    assert _valid_download_url("#") is None
    assert _valid_download_url("") is None
    assert _valid_download_url(None) is None
    # HTML entities are unescaped before validation.
    assert (
        _valid_download_url("https://limewire.com/d/x#a&amp;b")
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


# ---------------------------------------------------------------------------
# Masked download key resolution
# ---------------------------------------------------------------------------

RESOLVED_URL = "https://limewire.com/d/uTlMU#JryQqusx4O"
KEY = "dl_key_7d40aadd760489dfd1138c652765a2e8"
MASKED_PATH = "/wp-admin/admin-ajax.php"


def _json_response(payload, *, status: int = 200, headers=None) -> httpx.Response:
    response_headers = {"content-type": "application/json"}
    response_headers.update(headers or {})
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return httpx.Response(status, text=body, headers=response_headers)


def _ok_payload(url: str = RESOLVED_URL) -> dict:
    return {"success": True, "data": {"url": url}}


async def test_masked_key_resolves_to_validated_share_url():
    async with _source(lambda request: _json_response(_ok_payload())) as source:
        resolution = await source.resolve_masked_download(PAGE_URL, KEY)
    assert resolution.kind is LinkResolutionKind.SUPPORTED
    assert resolution.url == RESOLVED_URL


async def test_resolution_posts_multipart_action_key_and_referer():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["referer"] = request.headers.get("referer")
        seen["body"] = request.content.decode("utf-8", "replace")
        seen["content_type"] = request.headers.get("content-type", "")
        return _json_response(_ok_payload())

    async with _source(handler) as source:
        await source.resolve_masked_download(PAGE_URL, KEY)

    assert seen["method"] == "POST"
    assert seen["path"] == MASKED_PATH
    assert seen["referer"] == PAGE_URL
    assert seen["content_type"].startswith("multipart/form-data")
    assert "get_masked_download" in seen["body"]
    assert KEY in seen["body"]


async def test_resolution_skips_request_when_circuit_already_open():
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return _html(403, "blocked", headers={"cf-mitigated": "challenge"})

    async with _source(handler) as source:
        await source.search("Example")
        assert source.circuit_open
        before = len(requests)
        with pytest.raises(SourceError) as excinfo:
            await source.resolve_masked_download(PAGE_URL, KEY)

    assert excinfo.value.kind is SourceFailureKind.ACCESS_BLOCKED
    assert len(requests) == before  # no resolution request was made


async def test_resolution_challenge_is_blocked_and_opens_circuit():
    def handler(request: httpx.Request) -> httpx.Response:
        return _html(200, "blocked", headers={"cf-mitigated": "challenge"})

    async with _source(handler) as source:
        with pytest.raises(SourceError) as excinfo:
            await source.resolve_masked_download(PAGE_URL, KEY)
        assert source.circuit_open

    assert excinfo.value.kind is SourceFailureKind.ACCESS_BLOCKED


@pytest.mark.parametrize("status", [429, 500, 503])
async def test_resolution_transient_status_is_typed(status):
    async with _source(lambda request: _json_response({}, status=status)) as source:
        with pytest.raises(SourceError) as excinfo:
            await source.resolve_masked_download(PAGE_URL, KEY)
    assert excinfo.value.kind is SourceFailureKind.TRANSIENT


async def test_resolution_network_error_is_transient():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection failed", request=request)

    async with _source(handler) as source:
        with pytest.raises(SourceError) as excinfo:
            await source.resolve_masked_download(PAGE_URL, KEY)
    assert excinfo.value.kind is SourceFailureKind.TRANSIENT


@pytest.mark.parametrize(
    "payload",
    [
        {"success": True, "data": {}},
        {"success": True},
        {"success": True, "data": {"url": None}},
        {"success": True, "data": "not-an-object"},
        [],
        "not json at all",
    ],
)
async def test_resolution_unsuccessful_or_malformed_payload_is_protocol(payload):
    async with _source(lambda request: _json_response(payload)) as source:
        with pytest.raises(SourceError) as excinfo:
            await source.resolve_masked_download(PAGE_URL, KEY)
    assert excinfo.value.kind is SourceFailureKind.PROTOCOL


async def test_resolution_html_body_is_protocol_and_is_not_scanned_for_a_link():
    # An HTML body must be rejected outright, never regexed for a share URL.
    body = f'<html><body><a href="{RESOLVED_URL}">x</a></body></html>'

    async with _source(lambda request: _html(200, body)) as source:
        with pytest.raises(SourceError) as excinfo:
            await source.resolve_masked_download(PAGE_URL, KEY)
    assert excinfo.value.kind is SourceFailureKind.PROTOCOL


@pytest.mark.parametrize(
    "bad_url",
    [
        "https://limewire.com/d/abc",
        "https://limewire.com/other/abc#frag",
        "http://limewire.com/d/abc#frag",
        "https://limewire.com/d/abc?x=1#frag",
    ],
)
async def test_resolution_rejects_non_strict_share_url(bad_url):
    async with _source(lambda request: _json_response(_ok_payload(bad_url))) as source:
        with pytest.raises(SourceError) as excinfo:
            await source.resolve_masked_download(PAGE_URL, KEY)
    assert excinfo.value.kind is SourceFailureKind.PROTOCOL


@pytest.mark.parametrize("bad_key", ["", "short", "has spaces", None])
async def test_resolution_rejects_malformed_key_without_a_request(bad_key):
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return _json_response(_ok_payload())

    async with _source(handler) as source:
        with pytest.raises(SourceError) as excinfo:
            await source.resolve_masked_download(PAGE_URL, bad_key)

    assert excinfo.value.kind is SourceFailureKind.PROTOCOL
    assert requests == []


async def test_resolution_obeys_global_pacing_with_detail_requests():
    now = [0.0]
    starts: list[float] = []

    async def fake_sleep(delay: float) -> None:
        now[0] += delay

    def handler(request: httpx.Request) -> httpx.Response:
        starts.append(now[0])
        if request.url.path == MASKED_PATH:
            return _json_response(_ok_payload())
        return _html(200, "<html><head><title>Issue</title></head></html>")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        source = FreemagazinesClient(
            http_client=http_client,
            scrape_delay=1.0,
            _clock=lambda: now[0],
            _sleep=fake_sleep,
        )
        await source.scrape_detail(PAGE_URL)
        await source.resolve_masked_download(PAGE_URL, KEY)
        await source.resolve_masked_download(PAGE_URL, KEY)

    assert starts == [0.0, 1.0, 2.0]


async def test_concurrent_resolutions_obey_concurrency_bound():
    now = [0.0]
    active = 0
    maximum_active = 0

    async def fake_sleep(delay: float) -> None:
        now[0] += delay

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0)
        active -= 1
        return _json_response(_ok_payload())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        source = FreemagazinesClient(
            http_client=http_client,
            scrape_delay=1.0,
            detail_concurrency=2,
            _clock=lambda: now[0],
            _sleep=fake_sleep,
        )
        resolved = await asyncio.gather(
            *(source.resolve_masked_download(PAGE_URL, KEY) for _ in range(4))
        )

    assert [r.url for r in resolved] == [RESOLVED_URL] * 4
    assert maximum_active == 2


# ---------------------------------------------------------------------------
# The need-gated resolution pass
# ---------------------------------------------------------------------------


def _issue(slug: str, *, url: str | None = None, key: str | None = None) -> ScrapedIssue:
    return ScrapedIssue(
        title=slug,
        page_url=f"https://freemagazines.top/{slug}/",
        limewire_url=url,
        download_key=key,
    )


async def test_resolution_pass_skips_issues_that_do_not_need_a_link():
    # The volume gate: an issue whose stored URL is fine costs no request.
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return _json_response(_ok_payload())

    issues = [_issue("already-linked", key=KEY)]
    async with _source(handler) as source:
        result = await resolve_masked_links(
            issues, source, needs_link=lambda issue: False
        )

    assert requests == []
    assert result.failures == []
    assert [i.page_url for i in result.items] == [issues[0].page_url]
    # Left untouched: indexing treats an absent URL as "no information".
    assert result.items[0].limewire_url is None


async def test_resolution_pass_resolves_only_the_issues_that_need_a_link():
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return _json_response(_ok_payload())

    needed = _issue("needs-link", key=KEY)
    skipped = _issue("has-link", key=KEY)
    async with _source(handler) as source:
        result = await resolve_masked_links(
            [needed, skipped],
            source,
            needs_link=lambda issue: issue.page_url == needed.page_url,
        )

    assert len(requests) == 1
    resolved = {i.page_url: i.limewire_url for i in result.items}
    assert resolved[needed.page_url] == RESOLVED_URL
    assert resolved[skipped.page_url] is None


async def test_inline_url_needs_no_resolution_request():
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return _json_response(_ok_payload())

    inline = "https://limewire.com/d/Inline1#frag9"
    async with _source(handler) as source:
        result = await resolve_masked_links([_issue("inline", url=inline)], source)

    assert requests == []
    assert result.items[0].limewire_url == inline


async def test_structurally_broken_response_is_still_a_failure():
    # A malformed payload is a genuine contract break and must stay loud.
    async with _source(lambda request: _json_response({"success": True})) as source:
        result = await resolve_masked_links([_issue("broken", key=KEY)], source)

    assert result.items == []
    assert len(result.failures) == 1
    assert result.failures[0].kind is SourceFailureKind.PROTOCOL


async def test_rejected_key_is_a_dead_link_disposition_not_a_failure():
    # The source answered correctly and says there is no link: an outcome for
    # the retry schedule, not a cycle-degrading error.
    async with _source(lambda request: _json_response({"success": False})) as source:
        result = await resolve_masked_links([_issue("expired", key=KEY)], source)

    assert result.failures == []
    assert [i.title for i in result.dead_link] == ["expired"]
    # Still indexed, so it is countable and parkable rather than re-discovered.
    assert [i.title for i in result.items] == ["expired"]
    assert result.items[0].limewire_url is None


async def test_unsupported_host_is_a_disposition_carrying_only_the_host():
    payload = _ok_payload("https://mega.nz/file/abcdef#somekey")
    async with _source(lambda request: _json_response(payload)) as source:
        result = await resolve_masked_links([_issue("elsewhere", key=KEY)], source)

    assert result.failures == []
    assert len(result.unsupported_host) == 1
    issue, host = result.unsupported_host[0]
    assert issue.title == "elsewhere"
    assert host == "mega.nz"
    # The unvalidated URL is never carried anywhere.
    assert result.items[0].limewire_url is None


async def test_malformed_link_on_a_supported_host_is_still_a_failure():
    # Right host, broken form: that is the source breaking its contract, not
    # an unusable destination.
    payload = _ok_payload("https://limewire.com/d/abc")  # no fragment
    async with _source(lambda request: _json_response(payload)) as source:
        result = await resolve_masked_links([_issue("badform", key=KEY)], source)

    assert result.unsupported_host == []
    assert len(result.failures) == 1
    assert result.failures[0].kind is SourceFailureKind.PROTOCOL


async def test_source_failure_kind_enum_is_unchanged():
    # Unsupported hosts and dead links are outcomes, not new failure kinds:
    # this enum is persisted, and widening it would break rollback.
    assert [kind.value for kind in SourceFailureKind] == [
        "access_blocked",
        "transient",
        "protocol",
    ]


async def test_page_with_no_download_affordance_is_not_a_failure():
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return _json_response(_ok_payload())

    async with _source(handler) as source:
        result = await resolve_masked_links([_issue("no-affordance")], source)

    assert requests == []
    assert result.failures == []
    assert len(result.items) == 1
    assert result.items[0].limewire_url is None


async def test_resolution_failure_preserves_valid_siblings():
    def handler(request: httpx.Request) -> httpx.Response:
        body = request.content.decode("utf-8", "replace")
        if "dl_key_bad" in body:
            return _json_response({"success": True})  # malformed: no url
        return _json_response(_ok_payload())

    issues = [
        _issue("bad", key="dl_key_baddddddddddddd"),
        _issue("good", key=KEY),
    ]
    async with _source(handler) as source:
        result = await resolve_masked_links(issues, source)

    assert [i.title for i in result.items] == ["good"]
    assert result.items[0].limewire_url == RESOLVED_URL
    assert len(result.failures) == 1
    assert result.failures[0].kind is SourceFailureKind.PROTOCOL


async def test_open_circuit_reports_blocked_once_and_makes_no_requests():
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return _html(403, "blocked", headers={"cf-mitigated": "challenge"})

    async with _source(handler) as source:
        await source.search("Example")
        assert source.circuit_open
        before = len(requests)
        result = await resolve_masked_links(
            [_issue(f"issue-{n}", key=KEY) for n in range(4)], source
        )

    assert len(requests) == before  # blocked issues make no resolution request
    assert result.items == []
    # Reported once, not once per issue.
    assert len(result.failures) == 1
    assert result.failures[0].kind is SourceFailureKind.ACCESS_BLOCKED
