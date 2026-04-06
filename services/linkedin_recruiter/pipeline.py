"""
LinkedIn job URLs → *Meet the hiring team* recruiter rows (LinkedIn only).

Requires ``data/linkedin_storage.json`` from manual login (``linkedin_manual_login.py``) or
``POST /internal/linkedin-session``.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Sequence

from services.linkedin_recruiter.fetch import fetch_html_playwright
from services.linkedin_recruiter.jobs import PRIMARY_SECTION_HEADING, parse_meet_the_hiring_team
from services.linkedin_recruiter.snippet import parse_recruiter_snippet
from services.linkedin_session import get_linkedin_storage_path

logger = logging.getLogger(__name__)


def is_linkedin_job_url(url: str) -> bool:
    """True if ``url`` looks like a LinkedIn job posting."""
    return "linkedin.com/jobs" in url


async def scrape_linkedin_job_recruiters(
    job_urls: Sequence[str],
    *,
    storage_state_path: str | Path | None = None,
    timeout_ms: float = 60_000.0,
    hydration_wait_s: float = 5.0,
    headless: bool = True,
    strict_job_urls: bool = False,
) -> list[dict[str, Any]]:
    """
    For each LinkedIn job URL, load the page with Playwright and parse *Meet the hiring team* only.
    """
    if strict_job_urls:
        bad = [u for u in job_urls if not is_linkedin_job_url(u)]
        if bad:
            raise ValueError(f"Not LinkedIn job URLs (strict_job_urls): {bad!r}")

    state_path = storage_state_path
    if state_path is None:
        candidate = get_linkedin_storage_path()
        if candidate.is_file():
            state_path = candidate

    logger.info(
        "LinkedIn recruiter scrape: %d URL(s), storage_state=%s",
        len(job_urls),
        state_path,
    )
    results: list[dict[str, Any]] = []
    for i, url in enumerate(job_urls, start=1):
        if not is_linkedin_job_url(url):
            logger.warning("LinkedIn skip (%d/%d): not a job URL: %s", i, len(job_urls), url)
            results.append(
                {
                    "url": url,
                    "error": "skipped: not a linkedin.com/jobs URL",
                    "section_found": False,
                    "recruiters": [],
                    "page_title": None,
                }
            )
            continue
        try:
            logger.info("LinkedIn fetch (%d/%d): %s", i, len(job_urls), url)
            html = await fetch_html_playwright(
                url,
                storage_state_path=state_path,
                timeout_ms=timeout_ms,
                hydration_wait_s=hydration_wait_s,
                headless=headless,
            )
            parsed = parse_meet_the_hiring_team(html)
            parsed["page_title"] = parse_recruiter_snippet(html).get("page_title")
            nrec = len(parsed.get("recruiters") or [])
            logger.info(
                "LinkedIn parsed (%d/%d): section_found=%s recruiters=%d",
                i,
                len(job_urls),
                parsed.get("section_found"),
                nrec,
            )
            results.append({"url": url, **parsed})
        except Exception as e:
            logger.exception("LinkedIn failed (%d/%d): %s", i, len(job_urls), url)
            results.append(
                {
                    "url": url,
                    "error": str(e),
                    "section_found": False,
                    "section_heading": PRIMARY_SECTION_HEADING,
                    "recruiters": [],
                    "page_title": None,
                }
            )

    logger.info("LinkedIn recruiter scrape finished: %d result(s)", len(results))
    return results


def scrape_linkedin_job_recruiters_sync(
    job_urls: Sequence[str],
    *,
    storage_state_path: str | Path | None = None,
    timeout_ms: float = 60_000.0,
    hydration_wait_s: float = 5.0,
    headless: bool = True,
    strict_job_urls: bool = False,
) -> list[dict[str, Any]]:
    """Sync wrapper around :func:`scrape_linkedin_job_recruiters`."""
    return asyncio.run(
        scrape_linkedin_job_recruiters(
            job_urls,
            storage_state_path=storage_state_path,
            timeout_ms=timeout_ms,
            hydration_wait_s=hydration_wait_s,
            headless=headless,
            strict_job_urls=strict_job_urls,
        )
    )
