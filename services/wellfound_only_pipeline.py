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
    Scrape Wellfound for each target role and write to wellfound_jobs_{date}.
    """
    return _run_wellfound_scrape_only_pipeline(
        run_id=run_id,
        time_filter_enabled=True,
        hours_old=24,
        target_roles=None,
        per_role_jobs=50,
    )


def _run_wellfound_scrape_only_pipeline(
    run_id: str | None = None,
    time_filter_enabled: bool = True,
    hours_old: int = 24,
    target_roles: list[str] | None = None,
    per_role_jobs: int = 50,
) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    run_date = date.today().isoformat()
    started_at = perf_counter()
    WELLFOUND_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": run_date,
    }

    selected_roles = _resolve_target_roles(target_roles)
    resolved_hours_old = max(1, int(hours_old))

    location = os.getenv("APIFY_WELLFOUND_LOCATION", "india")
    resolved_per_role_jobs = max(1, int(per_role_jobs))
    max_pages = int(os.getenv("APIFY_WELLFOUND_MAX_PAGES", "20"))
    use_proxy = os.getenv("APIFY_WELLFOUND_USE_PROXY", "true").lower() in ("1", "true", "yes")
    proxy_groups = _parse_csv_env(os.getenv("APIFY_WELLFOUND_PROXY_GROUPS", "RESIDENTIAL"))

    try:
        logger.info(
            "wellfound-only pipeline[%s] started location=%s per_role_jobs=%s max_pages=%s time_filter=%s hours_old=%s roles=%s",
            pipeline_run_id,
            location,
            resolved_per_role_jobs,
            max_pages,
            time_filter_enabled,
            resolved_hours_old,
            selected_roles,
        )
        raw_count = 0
        normalized_rows: list[dict[str, Any]] = []
        for role in selected_roles:
            role_raw_rows = scrape_wellfound_jobs(
                location=location,
                results_wanted=resolved_per_role_jobs,
                max_pages=max_pages,
                keyword=role,
                use_apify_proxy=use_proxy,
                apify_proxy_groups=proxy_groups,
            )
            raw_count += len(role_raw_rows)

            for raw in role_raw_rows:
                normalized = normalize_wellfound_item(raw)
                normalized["role_query"] = role
                normalized_rows.append(normalized)

        filtered_rows = (
            _filter_jobs_last_n_hours(normalized_rows, hours_old=resolved_hours_old)
            if time_filter_enabled
            else list(normalized_rows)
        )

        enriched_rows: list[dict[str, Any]] = []
        for row in filtered_rows:
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
            "raw_count": raw_count,
            "collected_count": len(normalized_rows),
            "roles_scraped": selected_roles,
            "per_role_jobs": resolved_per_role_jobs,
            "time_filtered_count": len(filtered_rows),
            "time_filter_enabled": time_filter_enabled,
            "hours_old": resolved_hours_old,
            "target_roles": selected_roles,
            "scraped_count": len(enriched_rows),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        WELLFOUND_RUN_METRICS[pipeline_run_id] = metrics
        logger.info(
            "wellfound-only pipeline[%s] completed raw=%s post_time=%s written=%s",
            pipeline_run_id,
            raw_count,
            len(filtered_rows),
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


def run_wellfound_scrape_only_pipeline_with_filters(
    run_id: str | None = None,
    time_filter_enabled: bool = True,
    hours_old: int = 24,
    target_roles: list[str] | None = None,
    per_role_jobs: int = 50,
) -> dict[str, Any]:
    return _run_wellfound_scrape_only_pipeline(
        run_id=run_id,
        time_filter_enabled=time_filter_enabled,
        hours_old=hours_old,
        target_roles=target_roles,
        per_role_jobs=per_role_jobs,
    )


def _parse_csv_env(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [part.strip() for part in raw_value.split(",") if part.strip()]


def _resolve_target_roles(target_roles: list[str] | None) -> list[str]:
    if not target_roles:
        return list(TARGET_ROLES)

    canonical = {role.lower(): role for role in TARGET_ROLES}
    resolved: list[str] = []
    invalid: list[str] = []
    for role in target_roles:
        normalized = (role or "").strip().lower()
        if not normalized:
            continue
        matched = canonical.get(normalized)
        if not matched:
            invalid.append(role)
            continue
        if matched not in resolved:
            resolved.append(matched)

    if invalid:
        raise ValueError(
            "Invalid target_roles values: "
            + ", ".join(invalid)
            + ". Allowed values: "
            + ", ".join(TARGET_ROLES)
        )

    return resolved or list(TARGET_ROLES)


def _filter_jobs_last_n_hours(jobs: list[dict[str, Any]], hours_old: int) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(hours_old)))
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
