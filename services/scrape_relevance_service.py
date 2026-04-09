import logging
import re
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.pipeline import (
    _classify_relevant_jobs,
    _dedupe_jobs,
    _scrape_target_jobs,
    _write_relevant_jobs_to_google_sheets,
    _write_scraped_jobs_to_google_sheets,
)

logger = logging.getLogger(__name__)

SCRAPE_ONLY_RUN_METRICS: dict[str, dict[str, Any]] = {}
CLASSIFY_ONLY_RUN_METRICS: dict[str, dict[str, Any]] = {}
_SCRAPED_TAB_RE = re.compile(r"^scraped_jobs_(\d{4}-\d{2}-\d{2})$")


def run_scrape_jobs_only(run_id: str | None = None, run_date: str | None = None) -> dict[str, Any]:
    """Scrape all sources and write scraped_jobs_{date}."""
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    started_at = perf_counter()
    SCRAPE_ONLY_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
    }

    try:
        scraped = _scrape_target_jobs()
        deduped = _dedupe_jobs(scraped)
        _write_scraped_jobs_to_google_sheets(run_date=resolved_run_date, scraped_jobs=deduped)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "scraped_count": len(scraped),
            "deduped_count": len(deduped),
            "scraped_tab": f"scraped_jobs_{resolved_run_date}",
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        SCRAPE_ONLY_RUN_METRICS[pipeline_run_id] = metrics
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
        SCRAPE_ONLY_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("scrape-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def run_classify_relevant_only(run_id: str | None = None, run_date: str | None = None) -> dict[str, Any]:
    """Read scraped_jobs_{date}, classify, and write relevant_jobs_{date}."""
    pipeline_run_id = run_id or str(uuid.uuid4())
    started_at = perf_counter()
    resolved_run_date = _resolve_scraped_run_date(run_date)
    CLASSIFY_ONLY_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
    }
    try:
        scraped_rows = _read_scraped_rows(resolved_run_date)
        deduped = _dedupe_jobs(scraped_rows)
        relevant, classifier_metrics = _classify_relevant_jobs(deduped)
        relevant_deduped = _dedupe_jobs(relevant)
        _write_relevant_jobs_to_google_sheets(run_date=resolved_run_date, relevant_jobs=relevant_deduped)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "scraped_input_count": len(scraped_rows),
            "deduped_input_count": len(deduped),
            "relevant_count": len(relevant_deduped),
            "classification_errors": classifier_metrics.get("classification_errors", 0),
            "source_scraped_tab": f"scraped_jobs_{resolved_run_date}",
            "relevant_tab": f"relevant_jobs_{resolved_run_date}",
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        CLASSIFY_ONLY_RUN_METRICS[pipeline_run_id] = metrics
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
        CLASSIFY_ONLY_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("classify-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def _resolve_scraped_run_date(run_date: str | None) -> str:
    """If date is given use it; else prefer today tab, fallback to latest scraped tab."""
    if run_date and run_date.strip():
        return run_date.strip()
    today = date.today().isoformat()
    titles = _list_worksheet_titles()
    if f"scraped_jobs_{today}" in titles:
        return today
    latest = _latest_scraped_tab_date(titles)
    if latest:
        return latest
    raise RuntimeError("No scraped_jobs_{date} worksheet found to classify.")


def _list_worksheet_titles() -> list[str]:
    writer = _get_writer()
    return [ws.title for ws in writer.sheet.worksheets()]


def _latest_scraped_tab_date(titles: list[str]) -> str | None:
    dates: list[str] = []
    for title in titles:
        m = _SCRAPED_TAB_RE.match(title)
        if m:
            dates.append(m.group(1))
    return max(dates) if dates else None


def _read_scraped_rows(run_date: str) -> list[dict[str, Any]]:
    writer = _get_writer()
    tab = f"scraped_jobs_{run_date}"
    ws = writer.sheet.worksheet(tab)
    rows = worksheet_row_dicts(ws)
    if not rows:
        raise RuntimeError(f"No rows found in worksheet {tab}.")
    return [dict(r) for r in rows]


def _get_writer() -> GoogleSheetsWriter:
    import os

    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    return GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)


def get_scrape_only_run_metrics(run_id: str) -> dict[str, Any] | None:
    return SCRAPE_ONLY_RUN_METRICS.get(run_id)


def get_classify_only_run_metrics(run_id: str) -> dict[str, Any] | None:
    return CLASSIFY_ONLY_RUN_METRICS.get(run_id)
