import logging
import re
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.pipeline import _classify_relevant_jobs, _dedupe_jobs

logger = logging.getLogger(__name__)

WELLFOUND_CLASSIFY_RUN_METRICS: dict[str, dict[str, Any]] = {}
_WELLFOUND_SCRAPED_TAB_RE = re.compile(r"^wellfound_jobs_(\d{4}-\d{2}-\d{2})$")


def run_wellfound_classify_only_pipeline(
    run_id: str | None = None,
    run_date: str | None = None,
) -> dict[str, Any]:
    """
    Read wellfound_jobs_{date}, run Gemini relevance classification, and write wellfound_relevant_jobs_{date}.
    """
    pipeline_run_id = run_id or str(uuid.uuid4())
    started_at = perf_counter()
    resolved_run_date = _resolve_wellfound_run_date(run_date)
    WELLFOUND_CLASSIFY_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
    }

    try:
        scraped_rows = _read_wellfound_rows(resolved_run_date)
        deduped_rows = _dedupe_jobs(scraped_rows)
        relevant_rows, classifier_metrics = _classify_relevant_jobs(deduped_rows)
        relevant_rows_deduped = _dedupe_jobs(relevant_rows)
        _write_wellfound_relevant_rows(run_date=resolved_run_date, relevant_rows=relevant_rows_deduped)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "scraped_input_count": len(scraped_rows),
            "deduped_input_count": len(deduped_rows),
            "relevant_count": len(relevant_rows_deduped),
            "classification_errors": classifier_metrics.get("classification_errors", 0),
            "source_scraped_tab": f"wellfound_jobs_{resolved_run_date}",
            "relevant_tab": f"wellfound_relevant_jobs_{resolved_run_date}",
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        WELLFOUND_CLASSIFY_RUN_METRICS[pipeline_run_id] = metrics
        logger.info(
            "wellfound-classify-only[%s] completed date=%s scraped=%s deduped=%s relevant=%s",
            pipeline_run_id,
            resolved_run_date,
            len(scraped_rows),
            len(deduped_rows),
            len(relevant_rows_deduped),
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
        WELLFOUND_CLASSIFY_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("wellfound-classify-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_wellfound_classify_run_metrics(run_id: str) -> dict[str, Any] | None:
    return WELLFOUND_CLASSIFY_RUN_METRICS.get(run_id)


def _resolve_wellfound_run_date(run_date: str | None) -> str:
    if run_date and run_date.strip():
        return run_date.strip()

    today = date.today().isoformat()
    titles = _list_worksheet_titles()
    today_tab = f"wellfound_jobs_{today}"
    if today_tab in titles:
        return today

    latest = _latest_wellfound_scraped_tab_date(titles)
    if latest:
        return latest

    raise RuntimeError("No wellfound_jobs_{date} worksheet found to classify.")


def _list_worksheet_titles() -> list[str]:
    writer = _get_writer()
    return [ws.title for ws in writer.list_worksheets()]


def _latest_wellfound_scraped_tab_date(titles: list[str]) -> str | None:
    dates: list[str] = []
    for title in titles:
        match = _WELLFOUND_SCRAPED_TAB_RE.match(title)
        if match:
            dates.append(match.group(1))
    return max(dates) if dates else None


def _read_wellfound_rows(run_date: str) -> list[dict[str, Any]]:
    writer = _get_writer()
    tab_name = f"wellfound_jobs_{run_date}"
    worksheet = writer.open_worksheet(tab_name)
    raw = writer.worksheet_get_all_values(worksheet, f"wellfound_read:{tab_name}:get_all_values")
    rows = worksheet_row_dicts(raw)
    if not rows:
        raise RuntimeError(f"No rows found in worksheet {tab_name}.")
    return [dict(row) for row in rows]


def _write_wellfound_relevant_rows(run_date: str, relevant_rows: list[dict[str, Any]]) -> None:
    writer = _get_writer()
    tab_name = f"wellfound_relevant_jobs_{run_date}"
    chunk_size = _get_chunk_size()
    writer.write_rows(tab_name, relevant_rows, chunk_size=chunk_size)


def _get_writer() -> GoogleSheetsWriter:
    import os

    spreadsheet_id = (os.getenv("WELLFOUND_GOOGLE_SPREADSHEET_ID") or os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Set WELLFOUND_GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_ID.")
    return GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)


def _get_chunk_size() -> int:
    import os

    return max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
