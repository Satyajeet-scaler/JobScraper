"""
After relevant jobs are classified: scrape LinkedIn *Meet the hiring team* and write rows
to a recruiters worksheet (one row per recruiter with a profile URL only).
"""

from __future__ import annotations

import logging
import os
from time import sleep
from typing import Any, Callable, TypeVar

T = TypeVar("T")

# Returned with row count: LinkedIn job URLs that had ≥1 recruiter with a profile URL
LinkedinRecruiterSheetResult = tuple[int, frozenset[str]]

from services.google_sheets import GoogleSheetsWriter
from services.linkedin_recruiter.pipeline import is_linkedin_job_url, scrape_linkedin_job_recruiters_sync
from services.linkedin_session import get_linkedin_storage_path

logger = logging.getLogger(__name__)


def _retry_sheet_write(action: Callable[[], T], retries: int, initial_delay_seconds: float) -> T:
    delay = initial_delay_seconds
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            return action()
        except Exception as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            sleep(delay)
            delay *= 2
    raise RuntimeError(f"Google Sheets write failed after {retries} attempts: {last_error}") from last_error


def write_linkedin_recruiters_for_relevant_jobs(
    run_date: str,
    relevant_jobs: list[dict[str, Any]],
) -> LinkedinRecruiterSheetResult:
    """
    Scrape LinkedIn recruiter cards for relevant LinkedIn job URLs and append rows to
    ``RECRUITERS_INFO_WORKSHEET`` (default ``recruiters_info_{run_date}``).

    Only outputs rows where ``recruiter_profile_url`` is non-empty.

    Returns ``(rows_written, frozenset of job_url)`` for jobs that had at least one such recruiter.
    """
    empty: LinkedinRecruiterSheetResult = (0, frozenset())
    if os.getenv("LINKEDIN_RECRUITER_SHEET_ENABLED", "true").lower() not in ("1", "true", "yes"):
        logger.info("linkedin recruiter sheet skipped (LINKEDIN_RECRUITER_SHEET_ENABLED=false)")
        return empty

    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.warning("linkedin recruiter sheet skipped: GOOGLE_SPREADSHEET_ID not set")
        return empty

    storage = get_linkedin_storage_path()
    if not storage.is_file():
        logger.warning(
            "linkedin recruiter sheet skipped: no Playwright session at %s "
            "(run linkedin_manual_login.py locally or POST to /internal/linkedin-session)",
            storage,
        )
        return empty

    li_relevant: list[dict[str, Any]] = []
    for job in relevant_jobs:
        url = (job.get("job_url") or "").strip()
        if not url or not is_linkedin_job_url(url):
            continue
        li_relevant.append(job)

    if not li_relevant:
        logger.info("linkedin recruiter sheet: no LinkedIn jobs in relevant list")
        return empty

    unique_urls = list(dict.fromkeys(j["job_url"].strip() for j in li_relevant if j.get("job_url")))

    headless = os.getenv("LINKEDIN_HEADLESS", "true").lower() in ("1", "true", "yes")
    logger.info(
        "linkedin recruiter scrape: unique_linkedin_job_urls=%s headless=%s",
        len(unique_urls),
        headless,
    )

    results = scrape_linkedin_job_recruiters_sync(
        unique_urls,
        storage_state_path=storage,
        headless=headless,
    )
    by_url: dict[str, dict[str, Any]] = {r["url"]: r for r in results}

    rows: list[dict[str, Any]] = []
    job_urls_with_recruiter_profile: set[str] = set()
    for job in li_relevant:
        url = (job.get("job_url") or "").strip()
        res = by_url.get(url, {})
        err = res.get("error")
        recruiters = res.get("recruiters") or []
        for rec in recruiters:
            profile_url = (rec.get("profile_url") or "").strip()
            if not profile_url:
                continue
            job_urls_with_recruiter_profile.add(url)
            rows.append(
                {
                    "run_date": run_date,
                    "relevant_jobs_tab": f"relevant_jobs_{run_date}",
                    "job_url": url,
                    "title": job.get("title", ""),
                    "company": job.get("company", ""),
                    "site": job.get("site", ""),
                    "matched_role": job.get("matched_role", ""),
                    "role_category": job.get("role_category", ""),
                    "priority": job.get("priority", ""),
                    "recruiter_name": rec.get("name") or "",
                    "recruiter_headline": rec.get("headline") or "",
                    "recruiter_profile_url": profile_url,
                    "meet_the_team_section_found": res.get("section_found", False),
                    "scrape_error": err or "",
                }
            )

    tab = os.getenv("RECRUITERS_INFO_WORKSHEET") or f"recruiters_info_{run_date}"
    chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    url_set = frozenset(job_urls_with_recruiter_profile)
    if not rows:
        logger.info("linkedin recruiter sheet: no rows with profile URLs; skipping sheet write tab=%s", tab)
        return 0, url_set

    _retry_sheet_write(
        action=lambda: writer.write_rows(tab, rows, chunk_size=chunk_size),
        retries=3,
        initial_delay_seconds=1.0,
    )
    logger.info("linkedin recruiter sheet wrote tab=%s rows=%s jobs_with_recruiters=%s", tab, len(rows), len(url_set))
    return len(rows), url_set
