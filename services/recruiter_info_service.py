import logging
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.linkedin_recruiter.sheets_pipeline import write_linkedin_recruiters_for_relevant_jobs

logger = logging.getLogger(__name__)

RECRUITER_INFO_RUN_METRICS: dict[str, dict[str, Any]] = {}


def run_recruiter_info_extraction(run_id: str | None = None, run_date: str | None = None) -> dict[str, Any]:
    """
    Read `relevant_jobs_{date}` from GOOGLE_SPREADSHEET_ID and write `recruiters_info_{date}`.

    Behavior:
    - LinkedIn rows: scrape 'Meet the hiring team' recruiter profiles when possible.
    - Any row with no recruiter profile (or non-LinkedIn): try internal POC matching via contacts tab (Data_).
    """
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    started_at = perf_counter()
    RECRUITER_INFO_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
    }

    try:
        relevant_jobs = _read_relevant_jobs(resolved_run_date)
        rows_written, urls_with_recruiters = write_linkedin_recruiters_for_relevant_jobs(
            run_date=resolved_run_date,
            relevant_jobs=relevant_jobs,
        )
        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "relevant_input_count": len(relevant_jobs),
            "recruiters_rows_written": rows_written,
            "jobs_with_recruiter_profiles_count": len(urls_with_recruiters),
            "relevant_tab": f"relevant_jobs_{resolved_run_date}",
            "recruiters_tab": f"recruiters_info_{resolved_run_date}",
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        RECRUITER_INFO_RUN_METRICS[pipeline_run_id] = metrics
        logger.info(
            "recruiter-info[%s] completed relevant=%s rows_written=%s",
            pipeline_run_id,
            len(relevant_jobs),
            rows_written,
        )
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
        RECRUITER_INFO_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("recruiter-info[%s] failed: %s", pipeline_run_id, exc)
        raise


def _read_relevant_jobs(run_date: str) -> list[dict[str, Any]]:
    spreadsheet_id = _require_spreadsheet_id()
    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    tab = f"relevant_jobs_{run_date}"
    ws = writer.open_worksheet(tab)
    raw = writer.worksheet_get_all_values(ws, f"recruiter_info:{tab}:get_all_values")
    rows = worksheet_row_dicts(raw)
    if not rows:
        raise RuntimeError(f"No rows found in worksheet {tab}.")
    # worksheet_row_dicts returns dict[str,str]; keep as dict for downstream
    return [dict(r) for r in rows]


def _require_spreadsheet_id() -> str:
    import os

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    return spreadsheet_id


def get_recruiter_info_run_metrics(run_id: str) -> dict[str, Any] | None:
    return RECRUITER_INFO_RUN_METRICS.get(run_id)

