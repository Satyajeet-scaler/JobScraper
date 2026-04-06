"""Lightweight HTML snippets (e.g. page title) for LinkedIn job pages."""

from __future__ import annotations

from bs4 import BeautifulSoup


def parse_recruiter_snippet(html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "lxml")
    title = soup.find("title")
    return {
        "page_title": title.get_text(strip=True) if title else None,
    }
