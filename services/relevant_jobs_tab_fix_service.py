import logging
import os
import traceback
import uuid
from datetime import datetime
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

from services.google_sheets import GoogleSheetsWriter

logger = logging.getLogger(__name__)

RELEVANT_JOBS_TAB_FIX_RUN_METRICS: dict[str, dict[str, Any]] = {}


def run_fix_relevant_jobs_tab(run_id: str | None = None, run_date: str | None = None) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = _resolve_run_date(run_date)
    started_at = perf_counter()
    RELEVANT_JOBS_TAB_FIX_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
    }
    try:
        spreadsheet_id = _require_spreadsheet_id()
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        recruiters_tab = os.getenv("RECRUITERS_INFO_WORKSHEET") or f"recruiters_info_{resolved_run_date}"
        relevant_tab = f"relevant_jobs_{resolved_run_date}"

        recruiters_ws = writer.open_worksheet(recruiters_tab)
        recruiters_raw = writer.worksheet_get_all_values(
            recruiters_ws,
            f"relevant_jobs_tab_fix:{recruiters_tab}:get_all_values",
        )
        if len(recruiters_raw) <= 1:
            raise RuntimeError(f"No recruiter rows found in worksheet '{recruiters_tab}'.")

        relevant_ws = writer.open_worksheet(relevant_tab)
        relevant_raw = writer.worksheet_get_all_values(
            relevant_ws,
            f"relevant_jobs_tab_fix:{relevant_tab}:get_all_values",
        )
        if len(relevant_raw) <= 1:
            raise RuntimeError(f"No relevant jobs rows found in worksheet '{relevant_tab}'.")

        recruiter_headers = [str(cell or "").strip().lower() for cell in recruiters_raw[0]]
        job_url_col_idx = _find_header_index(recruiter_headers, ["job_url", "url"])
        rel_tab_col_idx = _find_header_index(recruiter_headers, ["relevant_jobs_tab", "relevant tab"])
        if job_url_col_idx is None:
            raise RuntimeError(f"'job_url' column is required in worksheet '{recruiters_tab}'.")
        if rel_tab_col_idx is None:
            raise RuntimeError(f"'relevant_jobs_tab' column is required in worksheet '{recruiters_tab}'.")

        relevant_headers = [str(cell or "").strip().lower() for cell in relevant_raw[0]]
        relevant_job_url_idx = _find_header_index(relevant_headers, ["job_url", "url"])
        if relevant_job_url_idx is None:
            raise RuntimeError(f"'job_url' column is required in worksheet '{relevant_tab}'.")

        relevant_urls = {
            str(row[relevant_job_url_idx]).strip().lower()
            for row in relevant_raw[1:]
            if len(row) > relevant_job_url_idx and str(row[relevant_job_url_idx]).strip()
        }
        if not relevant_urls:
            raise RuntimeError(f"No usable job URLs found in worksheet '{relevant_tab}'.")

        updated_rows = 0
        matched_rows = 0
        unmatched_rows = 0
        column_values: list[list[str]] = []
        for row in recruiters_raw[1:]:
            job_url = str(row[job_url_col_idx]).strip().lower() if len(row) > job_url_col_idx else ""
            existing = str(row[rel_tab_col_idx]).strip() if len(row) > rel_tab_col_idx else ""
            if job_url and job_url in relevant_urls:
                target = relevant_tab
                matched_rows += 1
            else:
                target = existing
                if job_url:
                    unmatched_rows += 1
            if target != existing:
                updated_rows += 1
            column_values.append([target])

        start_row = 2
        end_row = start_row + len(column_values) - 1
        col_letter = _column_letter(rel_tab_col_idx + 1)
        rng = f"{col_letter}{start_row}:{col_letter}{end_row}"
        writer.worksheet_update(
            recruiters_ws,
            rng,
            column_values,
            f"relevant_jobs_tab_fix:{recruiters_tab}:update_relevant_jobs_tab",
        )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "recruiters_tab": recruiters_tab,
            "relevant_tab": relevant_tab,
            "total_rows": len(column_values),
            "matched_rows": matched_rows,
            "unmatched_rows": unmatched_rows,
            "updated_rows": updated_rows,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        RELEVANT_JOBS_TAB_FIX_RUN_METRICS[pipeline_run_id] = metrics
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        RELEVANT_JOBS_TAB_FIX_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("relevant-jobs-tab-fix[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_relevant_jobs_tab_fix_run_metrics(run_id: str) -> dict[str, Any] | None:
    return RELEVANT_JOBS_TAB_FIX_RUN_METRICS.get(run_id)


def _resolve_run_date(run_date: str | None) -> str:
    if run_date and run_date.strip():
        return run_date.strip()
    tz = ZoneInfo(os.getenv("CRON_TIMEZONE", "Asia/Kolkata"))
    return datetime.now(tz).strftime("%Y-%m-%d")


def _require_spreadsheet_id() -> str:
    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    return spreadsheet_id


def _find_header_index(headers: list[str], candidates: list[str]) -> int | None:
    for candidate in candidates:
        normalized = candidate.strip().lower()
        if normalized in headers:
            return headers.index(normalized)
    for i, header in enumerate(headers):
        for candidate in candidates:
            normalized = candidate.strip().lower()
            if normalized in header or header in normalized:
                return i
    return None


def _column_letter(index: int) -> str:
    letters = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters
