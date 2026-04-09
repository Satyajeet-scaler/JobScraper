import logging
import os
import traceback
import uuid
from datetime import date, datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from pandas import to_datetime

from services.apify_wellfound import normalize_wellfound_item, scrape_wellfound_jobs
from services.google_sheets import GoogleSheetsWriter

logger = logging.getLogger(__name__)
WELLFOUND_RUN_METRICS: dict[str, dict[str, Any]] = {}

TARGET_ROLES = [
    "Developer",
    "Data Engineer",
    "Data Analyst",
    "Data Scientist",
    "Devops Engineer",
    "Platform Engineer",
]


def run_wellfound_scrape_only_pipeline(run_id: str | None = None) -> dict[str, Any]:
    """
    Scrape Wellfound (India), keep target roles and last-24h jobs, then write to wellfound_jobs_{date}.
    """
    pipeline_run_id = run_id or str(uuid.uuid4())
    run_date = date.today().isoformat()
    started_at = perf_counter()
    WELLFOUND_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": run_date,
    }

    location = os.getenv("APIFY_WELLFOUND_LOCATION", "india")
    results_wanted = int(os.getenv("APIFY_MAX_JOBS_WELLFOUND", "100"))
    max_pages = int(os.getenv("APIFY_WELLFOUND_MAX_PAGES", "20"))
    use_proxy = os.getenv("APIFY_WELLFOUND_USE_PROXY", "true").lower() in ("1", "true", "yes")
    proxy_groups = _parse_csv_env(os.getenv("APIFY_WELLFOUND_PROXY_GROUPS", "RESIDENTIAL"))

    try:
        logger.info(
            "wellfound-only pipeline[%s] started location=%s results_wanted=%s max_pages=%s",
            pipeline_run_id,
            location,
            results_wanted,
            max_pages,
        )
        raw_rows = scrape_wellfound_jobs(
            location=location,
            results_wanted=results_wanted,
            max_pages=max_pages,
            use_apify_proxy=use_proxy,
            apify_proxy_groups=proxy_groups,
        )

        normalized_rows: list[dict[str, Any]] = []
        for raw in raw_rows:
            normalized = normalize_wellfound_item(raw)
            matched_role = _match_target_role(
                title=str(normalized.get("title") or ""),
                description=str(normalized.get("description") or ""),
            )
            if not matched_role:
                continue
            normalized["role_query"] = matched_role
            normalized_rows.append(normalized)

        last_24h_rows = _filter_jobs_last_24_hours(normalized_rows)

        enriched_rows: list[dict[str, Any]] = []
        for row in last_24h_rows:
            copy = dict(row)
            copy["run_date"] = run_date
            copy["source"] = "wellfound"
            enriched_rows.append(copy)

        spreadsheet_id = os.getenv("WELLFOUND_GOOGLE_SPREADSHEET_ID") or os.getenv("GOOGLE_SPREADSHEET_ID")
        if not spreadsheet_id:
            raise RuntimeError("Set WELLFOUND_GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_ID.")

        tab_name = os.getenv("WELLFOUND_SCRAPED_TAB_TEMPLATE", "wellfound_jobs_{date}").format(date=run_date)
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
        writer.write_rows(tab_name, enriched_rows, chunk_size=chunk_size)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": run_date,
            "tab_name": tab_name,
            "raw_count": len(raw_rows),
            "role_filtered_count": len(normalized_rows),
            "last_24h_count": len(last_24h_rows),
            "scraped_count": len(enriched_rows),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        WELLFOUND_RUN_METRICS[pipeline_run_id] = metrics
        logger.info(
            "wellfound-only pipeline[%s] completed raw=%s role_filtered=%s last_24h=%s written=%s",
            pipeline_run_id,
            len(raw_rows),
            len(normalized_rows),
            len(last_24h_rows),
            len(enriched_rows),
        )
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": run_date,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        WELLFOUND_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("wellfound-only pipeline[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_wellfound_run_metrics(run_id: str) -> dict[str, Any] | None:
    return WELLFOUND_RUN_METRICS.get(run_id)


def _parse_csv_env(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [part.strip() for part in raw_value.split(",") if part.strip()]


def _match_target_role(title: str, description: str) -> str | None:
    text = f"{title} {description}".lower()
    excluded_tokens = (
        "qa",
        "quality assurance",
        "sdet",
        "test engineer",
        "tester",
        "support engineer",
        "technical support",
    )
    if any(token in text for token in excluded_tokens):
        return None

    role_keywords: dict[str, tuple[str, ...]] = {
        "Developer": (
            "developer",
            "software engineer",
            "software developer",
            "backend engineer",
            "frontend engineer",
            "full stack",
            "fullstack",
            "sde",
        ),
        "Data Engineer": (
            "data engineer",
            "etl",
            "data pipeline",
            "analytics engineer",
            "databricks",
            "big data",
        ),
        "Data Analyst": (
            "data analyst",
            "business analyst",
            "product analyst",
            "bi analyst",
            "business intelligence",
        ),
        "Data Scientist": (
            "data scientist",
            "machine learning engineer",
            "ml engineer",
            "ai engineer",
            "nlp engineer",
            "computer vision",
        ),
        "Devops Engineer": (
            "devops",
            "ci/cd",
            "release engineer",
            "infrastructure engineer",
        ),
        "Platform Engineer": (
            "platform engineer",
            "cloud platform",
            "cloud engineer",
        ),
    }

    for role_name in TARGET_ROLES:
        if any(keyword in text for keyword in role_keywords.get(role_name, ())):
            return role_name
    return None


def _filter_jobs_last_24_hours(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    filtered: list[dict[str, Any]] = []
    for job in jobs:
        raw_date = job.get("date_posted")
        if raw_date in (None, ""):
            continue
        parsed = to_datetime(raw_date, utc=True, errors="coerce")
        if str(parsed) == "NaT":
            continue
        if parsed >= cutoff:
            filtered.append(job)
    return filtered
