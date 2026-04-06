"""Parse LinkedIn job view pages for *Meet the hiring team* recruiter contacts only."""

from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup

PRIMARY_SECTION_HEADING = "Meet the hiring team"
SECTION_HEADING = PRIMARY_SECTION_HEADING
_IN_HREF = re.compile(r"https?://(?:www\.)?linkedin\.com/in/[^?\s#]+", re.I)


def _parse_section_by_heading(html: str, heading: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    heading_node = None
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "p", "span", "div"]):
        if tag.get_text(" ", strip=True) == heading:
            heading_node = tag
            break

    if heading_node is None:
        return {
            "section_found": False,
            "section_heading": heading,
            "recruiters": [],
        }

    recruiters: list[dict[str, str | None]] = []
    seen: set[str] = set()
    section_root = heading_node.find_parent(["section", "article"])
    if section_root is None:
        section_root = heading_node.parent
    if section_root is None:
        section_root = heading_node

    for a in section_root.find_all("a", href=_IN_HREF):
        raw = (a.get("href") or "").strip().split("?")[0].split("#")[0]
        if "linkedin.com/in/" not in raw:
            continue
        href = raw if raw.startswith("http") else "https://www.linkedin.com" + raw
        if href in seen:
            continue
        seen.add(href)
        text = a.get_text(" ", strip=True)
        name: str | None = None
        headline: str | None = None
        if "•" in text:
            parts = [p.strip() for p in text.split("•", 1)]
            name, headline = parts[0], parts[1] if len(parts) > 1 else None
        elif text:
            name = text.split("\n")[0].strip()
        if headline:
            headline = re.sub(
                r"\s*Job poster\s*$",
                "",
                headline,
                flags=re.I,
            ).strip() or None
        recruiters.append(
            {
                "name": name,
                "headline": headline,
                "profile_url": href,
            }
        )

    return {
        "section_found": True,
        "section_heading": heading,
        "recruiters": recruiters,
    }


def parse_meet_the_hiring_team(html: str) -> dict[str, Any]:
    """Parse recruiter contacts from the *Meet the hiring team* section only."""
    return _parse_section_by_heading(html, PRIMARY_SECTION_HEADING)
