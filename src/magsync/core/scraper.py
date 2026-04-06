"""Scraper for freemagazines.top."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

BASE_URL = "https://freemagazines.top"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}


@dataclass
class ScrapedIssue:
    title: str
    page_url: str
    cover_image_url: str | None = None
    limewire_url: str | None = None
    genre: str | None = None
    file_size: str | None = None


async def search(
    query: str,
    *,
    max_pages: int = 50,
    scrape_delay: float = 1.0,
) -> list[ScrapedIssue]:
    """Search freemagazines.top for magazines matching a query.

    Follows pagination to collect all matching issues.
    """
    issues: list[ScrapedIssue] = []
    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0, headers=DEFAULT_HEADERS) as client:
        page = 1
        while page <= max_pages:
            url = f"{BASE_URL}/?s={quote_plus(query)}"
            if page > 1:
                url = f"{BASE_URL}/page/{page}/?s={quote_plus(query)}"

            resp = await client.get(url)
            if resp.status_code == 404:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.select("a[href]")

            page_issues: list[ScrapedIssue] = []
            seen_urls = {i.page_url for i in issues}

            for link in links:
                href = link.get("href", "")
                if (
                    not href
                    or not href.startswith(BASE_URL + "/")
                    or href == BASE_URL + "/"
                    or "/?s=" in href
                    or "/page/" in href
                    or href.endswith("#")
                    or "#" in href
                ):
                    continue

                # Skip non-article links (assets, social, etc.)
                if any(
                    x in href
                    for x in [
                        "wp-content",
                        "twitter.com",
                        "facebook.com",
                        "reddit.com",
                        "telegram.me",
                        "getpocket.com",
                    ]
                ):
                    continue

                # Skip category/tag pages — magazine issue URLs always contain a year
                slug = href.rstrip("/").rsplit("/", 1)[-1]
                if not re.search(r"20\d{2}", slug):
                    continue

                if href in seen_urls:
                    continue
                seen_urls.add(href)

                img = link.find("img")
                cover = img.get("src") if img else None

                page_issues.append(
                    ScrapedIssue(
                        title="",  # Will be filled from detail page
                        page_url=href,
                        cover_image_url=cover,
                    )
                )

            if not page_issues:
                break

            issues.extend(page_issues)
            page += 1

            if page <= max_pages:
                await asyncio.sleep(scrape_delay)

    return issues


async def scrape_detail_page(
    page_url: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> ScrapedIssue:
    """Scrape a magazine detail page to extract full metadata."""
    should_close = client is None
    if client is None:
        client = httpx.AsyncClient(follow_redirects=True, timeout=30.0, headers=DEFAULT_HEADERS)

    try:
        resp = await client.get(page_url)
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # Extract title from <title> tag or og:title
        og_title = soup.find("meta", property="og:title")
        if og_title:
            raw_title = og_title.get("content", "")
            title = raw_title.replace(" | Download Magazine PDF", "").strip()
        else:
            title_tag = soup.find("title")
            title = title_tag.get_text().strip() if title_tag else ""

        # Extract LimeWire download URL
        limewire_url = None
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if "limewire.com/d/" in href:
                limewire_url = href
                break

        # Extract genre
        genre = None
        genre_match = re.search(r"\*\*Genre:\*\*\s*(.+)", html)
        if not genre_match:
            bold_tags = soup.find_all("strong")
            for b in bold_tags:
                if "Genre" in b.get_text():
                    next_text = b.next_sibling
                    if next_text:
                        genre = str(next_text).strip().strip(":").strip()
                        break

        # Extract file size from "Requirements:" text
        file_size = None
        text = soup.get_text()
        size_match = re.search(r"Requirements:.*?(\d+\s*MB)", text)
        if size_match:
            file_size = size_match.group(1)

        # Extract cover image
        og_image = soup.find("meta", property="og:image")
        cover = og_image.get("content") if og_image else None

        return ScrapedIssue(
            title=title,
            page_url=page_url,
            cover_image_url=cover,
            limewire_url=limewire_url,
            genre=genre,
            file_size=file_size,
        )
    finally:
        if should_close:
            await client.aclose()


async def search_with_details(
    query: str,
    *,
    max_pages: int = 50,
    scrape_delay: float = 1.0,
    scrape_concurrency: int = 5,
) -> list[ScrapedIssue]:
    """Search and then scrape detail pages concurrently."""
    results = await search(query, max_pages=max_pages, scrape_delay=scrape_delay)

    if not results:
        return []

    semaphore = asyncio.Semaphore(scrape_concurrency)

    async def _scrape_one(result: ScrapedIssue, client: httpx.AsyncClient) -> ScrapedIssue:
        async with semaphore:
            detail = await scrape_detail_page(result.page_url, client=client)
            if not detail.cover_image_url and result.cover_image_url:
                detail.cover_image_url = result.cover_image_url
            return detail

    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0, headers=DEFAULT_HEADERS) as client:
        tasks = [_scrape_one(r, client) for r in results]
        detailed = await asyncio.gather(*tasks)

    return list(detailed)
