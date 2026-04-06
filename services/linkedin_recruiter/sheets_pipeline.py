"""
After relevant jobs are classified: scrape LinkedIn *Meet the hiring team* and write rows
to a recruiters worksheet.

Also writes fallback rows for LinkedIn jobs where recruiter profiles were not found,
using company-contact emails from a contacts sheet.
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

    li_relevant: list[dict[str, Any]] = []
    for job in relevant_jobs:
        url = (job.get("job_url") or "").strip()
        if not url or not is_linkedin_job_url(url):
            continue
        li_relevant.append(job)

    by_url: dict[str, dict[str, Any]] = {}
    if li_relevant:
        storage = get_linkedin_storage_path()
        if not storage.is_file():
            logger.warning(
                "linkedin recruiter scrape skipped: no Playwright session at %s; "
                "LinkedIn recruiter profiles will be unavailable, fallback email matching still runs.",
                storage,
            )
            storage = None

        unique_urls = list(dict.fromkeys(j["job_url"].strip() for j in li_relevant if j.get("job_url")))
        max_urls = int(os.getenv("LINKEDIN_RECRUITER_MAX_URLS_PER_RUN", "60"))
        if max_urls > 0 and len(unique_urls) > max_urls:
            logger.info(
                "linkedin recruiter scrape: capping urls from %s to %s (LINKEDIN_RECRUITER_MAX_URLS_PER_RUN)",
                len(unique_urls),
                max_urls,
            )
            unique_urls = unique_urls[:max_urls]
        if storage is not None:
            headless = os.getenv("LINKEDIN_HEADLESS", "true").lower() in ("1", "true", "yes")
            force_fail_timeout_s = float(os.getenv("LINKEDIN_RECRUITER_FORCE_FAIL_TIMEOUT_S", "15"))
            recycle_every = int(os.getenv("LINKEDIN_RECRUITER_RECYCLE_EVERY", "25"))
            logger.info(
                "linkedin recruiter scrape: unique_linkedin_job_urls=%s headless=%s force_fail_timeout_s=%s recycle_every=%s",
                len(unique_urls),
                headless,
                force_fail_timeout_s,
                recycle_every,
            )
            results = scrape_linkedin_job_recruiters_sync(
                unique_urls,
                storage_state_path=storage,
                headless=headless,
                force_fail_timeout_s=force_fail_timeout_s,
                recycle_every=recycle_every,
                timeout_ms=max(15000.0, force_fail_timeout_s * 1000.0),
            )
            by_url = {r["url"]: r for r in results}
    else:
        logger.info("linkedin recruiter sheet: no LinkedIn URLs found; will only try company-contact fallback rows")

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
                    "recruiter_email": "",
                    "meet_the_team_section_found": res.get("section_found", False),
                    "recruiter_source": "linkedin_meet_the_team",
                    "scrape_error": err or "",
                }
            )

    contacts_map = _load_company_contact_email_map()
    if contacts_map:
        for job in relevant_jobs:
            url = (job.get("job_url") or "").strip()
            site = str(job.get("site") or "").strip().lower()
            # For LinkedIn only, add fallback rows when recruiter profiles were not found.
            if site == "linkedin" and url and url in job_urls_with_recruiter_profile:
                continue
            company = (job.get("company") or "").strip()
            if not company:
                continue
            normalized = _normalize_company_name(company)
            if not normalized:
                continue
            emails = contacts_map.get(normalized, [])
            if not emails:
                continue
            rows.append(
                {
                    "run_date": run_date,
                    "relevant_jobs_tab": f"relevant_jobs_{run_date}",
                    "job_url": url,
                    "title": job.get("title", ""),
                    "company": company,
                    "site": job.get("site", ""),
                    "matched_role": job.get("matched_role", ""),
                    "role_category": job.get("role_category", ""),
                    "priority": job.get("priority", ""),
                    "recruiter_name": "",
                    "recruiter_headline": "",
                    "recruiter_profile_url": "",
                    "recruiter_email": ",".join(emails),
                    "meet_the_team_section_found": False,
                    "recruiter_source": "company_contacts_sheet",
                    "scrape_error": "",
                }
            )

    tab = os.getenv("RECRUITERS_INFO_WORKSHEET") or f"recruiters_info_{run_date}"
    chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    url_set = frozenset(job_urls_with_recruiter_profile)
    if not rows:
        logger.info("linkedin recruiter sheet: no recruiter/profile/contact rows; skipping sheet write tab=%s", tab)
        return 0, url_set

    _retry_sheet_write(
        action=lambda: writer.write_rows(tab, rows, chunk_size=chunk_size),
        retries=3,
        initial_delay_seconds=1.0,
    )
    logger.info("linkedin recruiter sheet wrote tab=%s rows=%s jobs_with_recruiters=%s", tab, len(rows), len(url_set))
    return len(rows), url_set


def _load_company_contact_email_map() -> dict[str, list[str]]:
    spreadsheet_id = os.getenv("COMPANY_CONTACTS_SPREADSHEET_ID")
    if not spreadsheet_id:
        return {}
    sheet_name = os.getenv("COMPANY_CONTACTS_SHEET_NAME", "Data_")
    company_header = os.getenv("COMPANY_CONTACTS_COMPANY_COLUMN", "Name of company").strip().lower()
    email_header = os.getenv("COMPANY_CONTACTS_EMAIL_COLUMN", "Email Address").strip().lower()

    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        worksheet = writer.sheet.worksheet(sheet_name)
        rows = worksheet.get_all_values()
    except Exception as exc:
        logger.warning("linkedin recruiter sheet: contacts source unavailable sheet=%s err=%s", sheet_name, exc)
        return {}

    if len(rows) <= 1:
        return {}
    headers = rows[0]
    header_map = {h.strip().lower(): i for i, h in enumerate(headers) if h.strip()}
    company_idx = header_map.get(company_header)
    email_idx = header_map.get(email_header)
    if company_idx is None or email_idx is None:
        logger.warning(
            "linkedin recruiter sheet: contacts headers missing company=%s email=%s",
            company_header,
            email_header,
        )
        return {}

    by_company: dict[str, set[str]] = {}
    for row in rows[1:]:
        company = row[company_idx].strip() if len(row) > company_idx else ""
        email = row[email_idx].strip() if len(row) > email_idx else ""
        if not company or not email:
            continue
        normalized = _normalize_company_name(company)
        if not normalized:
            continue
        by_company.setdefault(normalized, set()).add(email)
    return {k: sorted(v) for k, v in by_company.items()}


def _normalize_company_name(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    if not text:
        return ""
    return "".join(text.split())
