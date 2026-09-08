"""Validated, cycle-scoped scraper for freemagazines.top."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from html import unescape
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from magsync.core.models import (
    LinkResolution,
    LinkResolutionKind,
    SourceError,
    SourceFailure,
    SourceFailureKind,
    SourceResult,
)
from magsync.core.urls import (
    URLValidationError,
    is_supported_download_host,
    normalize_download_url,
    normalize_source_url,
    validate_source_origin,
)

BASE_URL = "https://freemagazines.top"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

logger = logging.getLogger("magsync")

_LIMEWIRE_RE = re.compile(r"https://(?:www\.)?limewire\.com/d/[^\s\"'<>]+")
_SAFE_HOST_RE = re.compile(r"[a-z0-9.-]{1,253}", re.IGNORECASE)
_SAFE_CF_RAY_RE = re.compile(r"[a-z0-9-]{1,128}", re.IGNORECASE)
_NO_RESULTS_SELECTORS = (
    ".search-no-results",
    ".no-results",
    ".nothing-found",
    "article.not-found",
)
_NO_RESULTS_PHRASES = (
    "nothing found",
    "no results found",
    "no search results",
    "sorry, but nothing matched your search terms",
)
_CHALLENGE_BODY_MARKER_PAIRS = (
    (b"<title>just a moment...</title>", b"enable javascript and cookies to continue"),
    (b"/cdn-cgi/challenge-platform/", b"_cf_chl_opt"),
)

# The source may expose an issue's share link only through a masked
# server-side lookup: the detail page renders a trigger carrying an opaque
# key, which is exchanged for the real URL at this endpoint. These live
# together so the next template reshuffle is a constant change, not a rewrite.
_MASKED_DOWNLOAD_PATH = "/wp-admin/admin-ajax.php"
_MASKED_DOWNLOAD_ACTION = "get_masked_download"
_MASKED_ACTION_FIELD = "action"
_MASKED_KEY_FIELD = "download_key"
_MASKED_TRIGGER_ID = "lw-vk-js-trigger"
_MASKED_TRIGGER_CLASS = "lw-vk-download-btn"
_MASKED_KEY_ATTR = "data-key"
# The key is opaque, so constrain it to a safe charset and length rather than
# its current "dl_key_<hex>" spelling: a format change must not break
# discovery, but page-controlled data must never be posted verbatim.
_MASKED_KEY_RE = re.compile(r"[A-Za-z0-9_.:-]{8,128}")
_MASKED_RESPONSE_TYPES = frozenset({"application/json", "text/json"})
_HTML_RESPONSE_TYPES = frozenset({"text/html", "application/xhtml+xml"})


def _safe_resolved_host(candidate: str) -> str | None:
    """Return a bounded, safe hostname from an unvalidated resolved URL.

    Only the hostname is ever taken from a URL that failed strict validation:
    the URL itself must never reach the index, a log line, or a callback.
    Returns ``None`` when the value is not even a plausible http(s) URL.
    """
    try:
        parsed = urlparse(candidate)
    except (TypeError, ValueError):
        return None
    if parsed.scheme.lower() not in ("http", "https"):
        return None
    host = parsed.hostname
    if not host or not _SAFE_HOST_RE.fullmatch(host):
        return None
    return host.lower()


def _valid_download_url(candidate: str | None) -> str | None:
    """Return a normalized strict URL on any supported host, or ``None``.

    Candidates are HTML-unescaped because the fallback scanner operates on raw
    markup. Validation is centralized in :mod:`magsync.core.urls`; in
    particular, lookalike hosts, credentials, unexpected queries, unsafe
    ports, and paths that are not a valid share/document form are rejected
    here. The source serves links from more than one file host, so an inline
    candidate on any supported host is accepted.
    """
    if not candidate:
        return None
    try:
        return normalize_download_url(unescape(candidate.strip()))
    except URLValidationError:
        return None


@dataclass
class ScrapedIssue:
    title: str
    page_url: str
    cover_image_url: str | None = None
    limewire_url: str | None = None
    genre: str | None = None
    file_size: str | None = None
    # Set only when the page advertises a masked download and no inline URL was
    # found: the caller decides whether this issue needs a resolution request.
    download_key: str | None = None


@dataclass(frozen=True)
class _ValidatedHTML:
    text: str
    final_url: str


class _RequestStartLimiter:
    """Serialize source request starts and enforce a minimum global spacing."""

    def __init__(
        self,
        delay: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._delay = max(0.0, delay)
        self._clock = clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._last_started: float | None = None

    async def wait(self) -> None:
        async with self._lock:
            now = self._clock()
            if self._last_started is not None:
                remaining = self._delay - (now - self._last_started)
                if remaining > 0:
                    await self._sleep(remaining)
                    now = self._clock()
            self._last_started = now


class FreemagazinesClient:
    """One reusable, validated source session for a command or daemon cycle.

    The object owns the cookie jar, global request-start limiter, bounded detail
    concurrency, and source challenge circuit. Construct a new instance for a
    new command/cycle; an opened circuit intentionally cannot be reset in place.
    """

    def __init__(
        self,
        *,
        scrape_delay: float = 1.0,
        detail_concurrency: int = 5,
        timeout: float = 30.0,
        http_client: httpx.AsyncClient | None = None,
        _clock: Callable[[], float] = time.monotonic,
        _sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if detail_concurrency < 1:
            raise ValueError("detail_concurrency must be at least 1")
        if scrape_delay < 0:
            raise ValueError("scrape_delay cannot be negative")

        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            headers=DEFAULT_HEADERS,
        )
        self._limiter = _RequestStartLimiter(
            scrape_delay,
            clock=_clock,
            sleep=_sleep,
        )
        self._detail_semaphore = asyncio.Semaphore(detail_concurrency)
        self._circuit_failure: SourceFailure | None = None

    async def __aenter__(self) -> FreemagazinesClient:
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the internally-created HTTP session, if any."""
        if self._owns_http_client:
            await self._http_client.aclose()

    @property
    def circuit_open(self) -> bool:
        return self._circuit_failure is not None

    @property
    def circuit_failure(self) -> SourceFailure | None:
        return self._circuit_failure

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Expose the reused session for diagnostics and focused integration."""
        return self._http_client

    def _raise_if_circuit_open(self, *, operation: str) -> None:
        failure = self._circuit_failure
        if failure is None:
            return
        raise SourceError(
            failure.kind,
            failure.message,
            operation=operation,
            status_code=failure.status_code,
            host=failure.host,
            path=failure.path,
            cf_ray=failure.cf_ray,
        )

    @staticmethod
    def _safe_context(url: httpx.URL | str) -> tuple[str | None, str | None]:
        parsed = urlparse(str(url))
        host = parsed.hostname
        if not host or not _SAFE_HOST_RE.fullmatch(host):
            host = None
        path = parsed.path[:256] if parsed.path else "/"
        if any(ord(char) < 32 for char in path):
            path = None
        return host, path

    @staticmethod
    def _safe_cf_ray(response: httpx.Response) -> str | None:
        value = response.headers.get("cf-ray", "").strip()
        return value if _SAFE_CF_RAY_RE.fullmatch(value) else None

    @staticmethod
    def _has_challenge_body(response: httpx.Response) -> bool:
        # Bound inspection and require a pair of Cloudflare-specific markers so
        # generic "enable JavaScript" copy cannot open the host-wide circuit.
        sample = response.content[:262_144].lower()
        return any(
            first in sample and second in sample
            for first, second in _CHALLENGE_BODY_MARKER_PAIRS
        )

    def _challenge_error(
        self, response: httpx.Response, *, operation: str
    ) -> SourceError:
        host, path = self._safe_context(response.url)
        cf_ray = self._safe_cf_ray(response)
        error = SourceError(
            SourceFailureKind.ACCESS_BLOCKED,
            "Source access is blocked by a challenge; try again later",
            operation=operation,
            status_code=response.status_code,
            host=host,
            path=path,
            cf_ray=cf_ray,
        )
        self._circuit_failure = error.failure
        logger.warning(
            "Source access blocked: status=%s host=%s cf_ray=%s",
            response.status_code,
            host or "unknown",
            cf_ray or "none",
        )
        return error

    def _validate_response(
        self,
        response: httpx.Response,
        *,
        operation: str,
        later_page: bool,
        expected_types: frozenset[str] = _HTML_RESPONSE_TYPES,
    ) -> _ValidatedHTML | None:
        # Validation order is security- and behavior-significant. Challenges
        # are detected before status/body parsing, including HTTP 200 pages.
        if response.headers.get(
            "cf-mitigated", ""
        ).strip().casefold() == "challenge" or self._has_challenge_body(response):
            raise self._challenge_error(response, operation=operation)

        host, path = self._safe_context(response.url)
        try:
            final_url = validate_source_origin(str(response.url))
        except URLValidationError as exc:
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Source response had an unexpected final origin",
                operation=operation,
                status_code=response.status_code,
                host=host,
                path=path,
            ) from exc

        if response.status_code == 404 and later_page:
            return None
        if response.status_code == 404:
            if operation == "search":
                message = "Source returned HTTP 404 for the first search page"
            elif operation == "detail":
                message = "Source detail page returned HTTP 404"
            else:
                message = f"Source returned HTTP 404 for the {operation} request"
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                message,
                operation=operation,
                status_code=404,
                host=host,
                path=path,
            )
        if response.status_code == 429 or response.status_code >= 500:
            raise SourceError(
                SourceFailureKind.TRANSIENT,
                f"Source temporarily returned HTTP {response.status_code}",
                operation=operation,
                status_code=response.status_code,
                host=host,
                path=path,
            )
        if not 200 <= response.status_code < 300:
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                f"Source returned unexpected HTTP {response.status_code}",
                operation=operation,
                status_code=response.status_code,
                host=host,
                path=path,
            )

        content_type = (
            response.headers.get("content-type", "").split(";", 1)[0].strip().casefold()
        )
        if content_type not in expected_types:
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Source response had an unexpected content type",
                operation=operation,
                status_code=response.status_code,
                host=host,
                path=path,
            )

        return _ValidatedHTML(response.text, final_url)

    async def _request_html(
        self,
        url: str,
        *,
        operation: str,
        later_page: bool = False,
    ) -> _ValidatedHTML | None:
        self._raise_if_circuit_open(operation=operation)
        await self._limiter.wait()
        # A concurrent request may have opened the circuit while this request
        # was waiting for the global pacing lock.
        self._raise_if_circuit_open(operation=operation)

        host, path = self._safe_context(url)
        try:
            response = await self._http_client.get(url)
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            raise SourceError(
                SourceFailureKind.TRANSIENT,
                "Source request failed transiently",
                operation=operation,
                host=host,
                path=path,
            ) from exc

        return self._validate_response(
            response,
            operation=operation,
            later_page=later_page,
        )

    async def _request_masked_download(
        self, *, key: str, referer: str, operation: str
    ) -> _ValidatedHTML:
        """POST one masked-key exchange through the shared validated session."""
        url = f"{BASE_URL}{_MASKED_DOWNLOAD_PATH}"
        self._raise_if_circuit_open(operation=operation)
        await self._limiter.wait()
        # A concurrent request may have opened the circuit while this request
        # was waiting for the global pacing lock.
        self._raise_if_circuit_open(operation=operation)

        host, path = self._safe_context(url)
        try:
            response = await self._http_client.post(
                url,
                # Multipart with no filenames, matching the browser's FormData.
                files={
                    _MASKED_ACTION_FIELD: (None, _MASKED_DOWNLOAD_ACTION),
                    _MASKED_KEY_FIELD: (None, key),
                },
                headers={"Referer": referer},
            )
        except asyncio.CancelledError:
            raise
        except httpx.RequestError as exc:
            raise SourceError(
                SourceFailureKind.TRANSIENT,
                "Source request failed transiently",
                operation=operation,
                host=host,
                path=path,
            ) from exc

        validated = self._validate_response(
            response,
            operation=operation,
            later_page=False,
            expected_types=_MASKED_RESPONSE_TYPES,
        )
        assert validated is not None  # resolution never uses later-page semantics
        return validated

    async def resolve_masked_download(
        self, page_url: str, download_key: str
    ) -> LinkResolution:
        """Exchange a masked download key for a classified link resolution.

        Runs through this client's session, cookie jar, pacing gate, detail
        concurrency bound, and circuit - exactly like the detail request that
        produced the key - so resolving links can neither burst past the
        configured source rate nor bypass challenge detection.

        Must not be called while already holding the detail semaphore: the
        resolution pass is deliberately separate from detail scraping.

        Raises :class:`SourceError` only for genuine source failures -
        challenge, transient, and structurally broken payloads. A rejected key
        and an unsupported destination host are returned as outcomes.
        """
        operation = "resolve"
        key = (download_key or "").strip()
        if not _MASKED_KEY_RE.fullmatch(key):
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Masked download key was not a usable token",
                operation=operation,
            )
        try:
            referer = normalize_source_url(page_url)
        except URLValidationError as exc:
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Detail page URL is not an allowed source URL",
                operation=operation,
            ) from exc

        async with self._detail_semaphore:
            validated = await self._request_masked_download(
                key=key, referer=referer, operation=operation
            )

        host, path = self._safe_context(f"{BASE_URL}{_MASKED_DOWNLOAD_PATH}")
        try:
            payload = json.loads(validated.text)
        except ValueError as exc:
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Masked download response was not valid JSON",
                operation=operation,
                host=host,
                path=path,
            ) from exc

        if not isinstance(payload, dict):
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Masked download response was not an object",
                operation=operation,
                host=host,
                path=path,
            )

        # The source answered correctly and reports no available link. That is
        # a dead or expired key, not a broken contract, so it is an outcome
        # for the caller's retry schedule rather than a cycle-degrading error.
        if not payload.get("success"):
            return LinkResolution(LinkResolutionKind.DEAD_LINK)

        data = payload.get("data")
        raw_url = data.get("url") if isinstance(data, dict) else None
        if not isinstance(raw_url, str) or not raw_url.strip():
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Masked download response contained no URL",
                operation=operation,
                host=host,
                path=path,
            )

        resolved = _valid_download_url(raw_url)
        if resolved is not None:
            return LinkResolution(LinkResolutionKind.SUPPORTED, url=resolved)

        # Strict validation failed. Distinguish a link on a host we simply
        # cannot retrieve from - the source honored its contract and only the
        # destination is unusable - from a malformed link on a host we do
        # support, which is a genuine contract break.
        resolved_host = _safe_resolved_host(raw_url)
        if resolved_host is None or is_supported_download_host(resolved_host):
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Masked download response URL failed strict validation",
                operation=operation,
                host=host,
                path=path,
            )
        return LinkResolution(
            LinkResolutionKind.UNSUPPORTED_HOST, host=resolved_host
        )

    async def search(
        self,
        query: str,
        *,
        max_pages: int = 50,
    ) -> SourceResult[ScrapedIssue]:
        """Return a structured, validated search result."""
        try:
            return await self._search(query, max_pages=max_pages)
        except SourceError as exc:
            return SourceResult(failure=exc.failure)

    async def _search(
        self,
        query: str,
        *,
        max_pages: int,
    ) -> SourceResult[ScrapedIssue]:
        if max_pages < 1:
            raise ValueError("max_pages must be at least 1")

        issues: list[ScrapedIssue] = []
        seen_urls: set[str] = set()

        for page in range(1, max_pages + 1):
            if page == 1:
                url = f"{BASE_URL}/?s={quote_plus(query)}"
            else:
                url = f"{BASE_URL}/page/{page}/?s={quote_plus(query)}"

            validated = await self._request_html(
                url,
                operation="search",
                later_page=page > 1,
            )
            if validated is None:
                break

            soup = BeautifulSoup(validated.text, "html.parser")
            page_issues = _parse_search_issues(soup, validated.final_url, seen_urls)
            if not page_issues:
                if _is_recognized_no_results(soup):
                    if page == 1:
                        return SourceResult(items=[], validated_empty=True)
                    break
                host, path = self._safe_context(validated.final_url)
                raise SourceError(
                    SourceFailureKind.PROTOCOL,
                    "Source search page had no issues or recognized no-results marker",
                    operation="search",
                    status_code=200,
                    host=host,
                    path=path,
                )

            issues.extend(page_issues)
            if page == max_pages or not _has_next_search_page(
                soup, page, validated.final_url
            ):
                break

        return SourceResult(items=issues)

    async def scrape_detail(self, page_url: str) -> ScrapedIssue:
        """Fetch and parse one detail page through this client's shared state."""
        try:
            normalized_page_url = normalize_source_url(page_url)
        except URLValidationError as exc:
            raise SourceError(
                SourceFailureKind.PROTOCOL,
                "Detail page URL is not an allowed source URL",
                operation="detail",
            ) from exc

        async with self._detail_semaphore:
            validated = await self._request_html(
                normalized_page_url,
                operation="detail",
            )
        assert validated is not None  # detail requests never use later-page semantics
        return _parse_detail_page(validated.text, normalized_page_url)

    async def search_with_details(
        self,
        query: str,
        *,
        max_pages: int = 50,
    ) -> SourceResult[ScrapedIssue]:
        """Search and isolate issue-specific detail failures."""
        summaries = await self.search(query, max_pages=max_pages)
        if summaries.failure is not None or not summaries.items:
            return summaries

        async def scrape_one(
            summary: ScrapedIssue,
        ) -> tuple[ScrapedIssue | None, SourceFailure | None]:
            try:
                detail = await self.scrape_detail(summary.page_url)
            except asyncio.CancelledError:
                raise
            except SourceError as exc:
                return None, exc.failure
            except Exception:
                host, path = self._safe_context(summary.page_url)
                return None, SourceFailure(
                    SourceFailureKind.PROTOCOL,
                    "Unable to parse source detail page",
                    operation="detail",
                    host=host,
                    path=path,
                )

            if not detail.cover_image_url and summary.cover_image_url:
                detail.cover_image_url = summary.cover_image_url
            return detail, None

        outcomes = await asyncio.gather(
            *(scrape_one(summary) for summary in summaries.items)
        )
        detailed = [detail for detail, _failure in outcomes if detail is not None]
        failures = [failure for _detail, failure in outcomes if failure is not None]

        if not detailed and failures:
            # An operation in which every advertised issue was omitted must not
            # collapse to a successful empty search in legacy or new callers.
            primary_index = next(
                (
                    index
                    for index, failure in enumerate(failures)
                    if failure.kind is SourceFailureKind.ACCESS_BLOCKED
                ),
                0,
            )
            primary = failures[primary_index]
            remaining = failures[:primary_index] + failures[primary_index + 1 :]
            return SourceResult(failure=primary, failures=remaining)
        return SourceResult(items=detailed, failures=failures)


def _parse_search_issues(
    soup: BeautifulSoup,
    final_url: str,
    seen_urls: set[str],
) -> list[ScrapedIssue]:
    page_issues: list[ScrapedIssue] = []
    for link in soup.select("a[href]"):
        href = unescape(str(link.get("href", "")).strip())
        if not href:
            continue
        try:
            normalized = normalize_source_url(urljoin(final_url, href))
        except URLValidationError:
            continue

        parsed = urlparse(normalized)
        if (
            parsed.path in {"", "/"}
            or parsed.query
            or parsed.fragment
            or "/page/" in parsed.path
            or "/wp-content/" in parsed.path
        ):
            continue

        slug = parsed.path.rstrip("/").rsplit("/", 1)[-1]
        if not re.search(r"20\d{2}", slug) or normalized in seen_urls:
            continue

        seen_urls.add(normalized)
        img = link.find("img")
        cover = str(img.get("src")) if img and img.get("src") else None
        page_issues.append(
            ScrapedIssue(
                title="",
                page_url=normalized,
                cover_image_url=cover,
            )
        )
    return page_issues


def _is_recognized_no_results(soup: BeautifulSoup) -> bool:
    if any(soup.select_one(selector) is not None for selector in _NO_RESULTS_SELECTORS):
        return True
    text = " ".join(soup.stripped_strings).casefold()
    return any(phrase in text for phrase in _NO_RESULTS_PHRASES)


def _has_next_search_page(
    soup: BeautifulSoup, current_page: int, final_url: str
) -> bool:
    expected_page = current_page + 1
    for link in soup.select("a[href], link[href]"):
        rel = {str(value).casefold() for value in (link.get("rel") or [])}
        classes = {str(value).casefold() for value in (link.get("class") or [])}
        href = str(link.get("href", ""))
        if "next" in rel or "next" in classes:
            return True
        try:
            parsed = urlparse(normalize_source_url(urljoin(final_url, href)))
        except URLValidationError:
            continue
        if f"/page/{expected_page}/" in parsed.path:
            return True
    return False


def _extract_download_key(soup: BeautifulSoup) -> str | None:
    """Locate the masked download trigger's opaque key, structurally.

    The key is read from the trigger element's own attribute via the parsed
    DOM - by element identity, then class, then any element carrying the
    attribute - and never by matching the inline script that happens to name
    the endpoint today.  A cosmetic template edit must not silently disable
    link discovery (this is the fourth such break in this pipeline).
    """
    candidates: list = []
    by_id = soup.find(id=_MASKED_TRIGGER_ID)
    if by_id is not None:
        candidates.append(by_id)
    candidates.extend(soup.find_all(class_=_MASKED_TRIGGER_CLASS))
    candidates.extend(soup.find_all(attrs={_MASKED_KEY_ATTR: True}))
    for tag in candidates:
        raw = tag.get(_MASKED_KEY_ATTR)
        if not isinstance(raw, str):
            continue
        key = raw.strip()
        if _MASKED_KEY_RE.fullmatch(key):
            return key
    return None


def _parse_detail_page(html: str, page_url: str) -> ScrapedIssue:
    soup = BeautifulSoup(html, "html.parser")

    og_title = soup.find("meta", property="og:title")
    if og_title:
        raw_title = str(og_title.get("content", ""))
        title = raw_title.replace(" | Download Magazine PDF", "").strip()
    else:
        title_tag = soup.find("title")
        title = title_tag.get_text().strip() if title_tag else ""

    limewire_url = None
    for tag in soup.find_all(attrs={"data-url": True}):
        limewire_url = _valid_download_url(tag.get("data-url"))
        if limewire_url:
            break
    if not limewire_url:
        for anchor in soup.find_all("a", href=True):
            limewire_url = _valid_download_url(anchor["href"])
            if limewire_url:
                break
    if not limewire_url:
        for match in _LIMEWIRE_RE.finditer(html):
            limewire_url = _valid_download_url(match.group(0))
            if limewire_url:
                break

    genre = None
    genre_match = re.search(r"\*\*Genre:\*\*\s*(.+)", html)
    if genre_match:
        genre = genre_match.group(1).strip()
    else:
        for bold in soup.find_all("strong"):
            if "Genre" in bold.get_text():
                next_text = bold.next_sibling
                if next_text:
                    genre = str(next_text).strip().strip(":").strip()
                    break

    file_size = None
    text = soup.get_text()
    size_match = re.search(r"Requirements:.*?(\d+\s*MB)", text)
    if size_match:
        file_size = size_match.group(1)

    og_image = soup.find("meta", property="og:image")
    cover = (
        str(og_image.get("content")) if og_image and og_image.get("content") else None
    )

    # An inline URL is authoritative and costs no extra request, so a key is
    # only surfaced when discovery would otherwise come up empty.
    download_key = None if limewire_url else _extract_download_key(soup)

    return ScrapedIssue(
        title=title,
        page_url=page_url,
        cover_image_url=cover,
        limewire_url=limewire_url,
        genre=genre,
        file_size=file_size,
        download_key=download_key,
    )


@dataclass
class ResolvedLinkBatch:
    """Outcome of a resolution pass: indexable items plus dispositions.

    ``items`` carries every issue that should be indexed, including those with
    no usable link - a disposed issue is still stored so it is countable and
    can be parked, rather than silently re-discovered every cycle.
    ``unsupported_host`` pairs an issue with the bare hostname its link
    resolved to; the unvalidated URL is deliberately never carried. The
    caller owns all index mutation: this module has no database access.
    """

    items: list[ScrapedIssue] = field(default_factory=list)
    failures: list[SourceFailure] = field(default_factory=list)
    unsupported_host: list[tuple[ScrapedIssue, str]] = field(default_factory=list)
    dead_link: list[ScrapedIssue] = field(default_factory=list)


async def resolve_masked_links(
    issues: list[ScrapedIssue],
    client: FreemagazinesClient,
    *,
    needs_link: Callable[[ScrapedIssue], bool] | None = None,
) -> ResolvedLinkBatch:
    """Resolve masked download keys for the issues that actually need a link.

    Issues already carrying an inline URL, and issues ``needs_link`` reports as
    already having a usable stored URL, are returned untouched at zero request
    cost.  That gate is what keeps link discovery from doubling source traffic:
    in steady state only genuinely new issues are resolved.  ``needs_link`` of
    ``None`` means "resolve everything that needs it", for explicit repair
    operations (link refresh, URL backfill).

    Resolution outcomes are classified rather than collapsed into a failure:
    a link on a supported host fills the issue's URL, an unsupported
    destination host and a rejected key are reported as dispositions for the
    caller to park, and only a structurally broken response is a typed
    failure.  A page with no download affordance at all is a legitimate
    link-less outcome and is not a failure.

    Returned issues never carry a resolved-but-unvalidated URL: every resolved
    candidate has passed the strict supported-host validator.
    """
    batch = ResolvedLinkBatch()
    items = batch.items
    failures = batch.failures
    blocked: SourceFailure | None = None

    for issue in issues:
        # Nothing to resolve: an inline URL wins, and a page with no download
        # affordance is legitimately link-less.
        if issue.limewire_url or not issue.download_key:
            items.append(issue)
            continue
        # Already has a usable stored URL; skipping is safe because indexing
        # treats an absent incoming URL as "no information" and never clears
        # the stored one.
        if needs_link is not None and not needs_link(issue):
            items.append(issue)
            continue
        if client.circuit_open:
            # Report the block once; these issues are blocked, not link-less.
            if blocked is None:
                blocked = client.circuit_failure
                if blocked is not None:
                    failures.append(blocked)
            continue
        try:
            resolution = await client.resolve_masked_download(
                issue.page_url, issue.download_key
            )
        except asyncio.CancelledError:
            raise
        except SourceError as exc:
            failures.append(exc.failure)
            continue

        if resolution.kind is LinkResolutionKind.SUPPORTED:
            items.append(replace(issue, limewire_url=resolution.url))
            continue

        # Disposed issues are still indexed (link-less) so they are countable
        # and can be parked on a schedule; the caller applies the disposition.
        items.append(issue)
        if resolution.kind is LinkResolutionKind.UNSUPPORTED_HOST:
            batch.unsupported_host.append((issue, resolution.host or "unknown"))
        else:
            batch.dead_link.append(issue)

    return batch


def _raise_result_failure(result: SourceResult[ScrapedIssue]) -> None:
    failure = result.failure
    if failure is None:
        return
    raise SourceError(
        failure.kind,
        failure.message,
        operation=failure.operation,
        status_code=failure.status_code,
        host=failure.host,
        path=failure.path,
        cf_ray=failure.cf_ray,
    )


async def search_result(
    query: str,
    *,
    max_pages: int = 50,
    scrape_delay: float = 1.0,
    client: FreemagazinesClient | None = None,
) -> SourceResult[ScrapedIssue]:
    """Structured search API for callers that need failure/empty semantics."""
    if client is not None:
        return await client.search(query, max_pages=max_pages)
    async with FreemagazinesClient(scrape_delay=scrape_delay) as source:
        return await source.search(query, max_pages=max_pages)


async def search(
    query: str,
    *,
    max_pages: int = 50,
    scrape_delay: float = 1.0,
    client: FreemagazinesClient | None = None,
) -> list[ScrapedIssue]:
    """Backward-compatible list API with typed failures and validated empty."""
    result = await search_result(
        query,
        max_pages=max_pages,
        scrape_delay=scrape_delay,
        client=client,
    )
    _raise_result_failure(result)
    return result.items


async def scrape_detail_page(
    page_url: str,
    *,
    client: httpx.AsyncClient | FreemagazinesClient | None = None,
) -> ScrapedIssue:
    """Backward-compatible detail API using validated shared state when passed."""
    if isinstance(client, FreemagazinesClient):
        return await client.scrape_detail(page_url)
    if isinstance(client, httpx.AsyncClient):
        # Preserve the existing raw-client injection API. New multi-request
        # callers should pass FreemagazinesClient so pacing/circuit state is
        # shared as well as cookies and connections.
        source = FreemagazinesClient(scrape_delay=0, http_client=client)
        return await source.scrape_detail(page_url)
    async with FreemagazinesClient(scrape_delay=0) as source:
        return await source.scrape_detail(page_url)


async def search_with_details_result(
    query: str,
    *,
    max_pages: int = 50,
    scrape_delay: float = 1.0,
    scrape_concurrency: int = 5,
    client: FreemagazinesClient | None = None,
) -> SourceResult[ScrapedIssue]:
    """Structured search/detail API retaining per-detail failures."""
    if client is not None:
        return await client.search_with_details(query, max_pages=max_pages)
    async with FreemagazinesClient(
        scrape_delay=scrape_delay,
        detail_concurrency=scrape_concurrency,
    ) as source:
        return await source.search_with_details(query, max_pages=max_pages)


async def search_with_details(
    query: str,
    *,
    max_pages: int = 50,
    scrape_delay: float = 1.0,
    scrape_concurrency: int = 5,
    client: FreemagazinesClient | None = None,
) -> list[ScrapedIssue]:
    """Backward-compatible detailed list API.

    New callers that need the partial-detail count should use
    :func:`search_with_details_result`.
    """
    result = await search_with_details_result(
        query,
        max_pages=max_pages,
        scrape_delay=scrape_delay,
        scrape_concurrency=scrape_concurrency,
        client=client,
    )
    _raise_result_failure(result)
    return result.items
