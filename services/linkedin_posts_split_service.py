import logging
import re
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

from services.apify_linkedin_posts import normalize_linkedin_post_item, scrape_linkedin_posts
from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.linkedin_posts_pipeline import (
    _build_actor_input,
    _classify_relevant_posts,
    _collect_source_columns,
    _dedupe_linkedin_relevant_rows,
    _write_linkedin_posts_relevant_only,
    _write_linkedin_posts_scraped_only,
)

logger = logging.getLogger(__name__)

LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS: dict[str, dict[str, Any]] = {}
LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS: dict[str, dict[str, Any]] = {}
_SCRAPED_TAB_RE = re.compile(r"^linkedin_posts_scraped_(\d{4}-\d{2}-\d{2})$")


def run_linkedin_posts_scrape_only(run_id: str | None = None, run_date: str | None = None) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    started_at = perf_counter()
    LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
    }
    try:
        actor_input = _build_actor_input()
        raw_rows = scrape_linkedin_posts(actor_input)
        source_columns = _collect_source_columns(raw_rows)
        normalized = [normalize_linkedin_post_item(row) for row in raw_rows]
        _write_linkedin_posts_scraped_only(run_date=resolved_run_date, scraped_rows=normalized)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "scraped_count": len(normalized),
            "source_columns": source_columns,
            "scraped_tab": f"linkedin_posts_scraped_{resolved_run_date}",
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS[pipeline_run_id] = metrics
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
        LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("linkedin-posts-scrape-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def run_linkedin_posts_classify_only(run_id: str | None = None, run_date: str | None = None) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    started_at = perf_counter()
    resolved_run_date = _resolve_scraped_run_date(run_date)
    LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
    }
    try:
        scraped_rows = _read_scraped_rows(resolved_run_date)
        relevant_rows, classification_errors = _classify_relevant_posts(scraped_rows)
        relevant_rows_deduped = _dedupe_linkedin_relevant_rows(relevant_rows)
        _write_linkedin_posts_relevant_only(run_date=resolved_run_date, relevant_rows=relevant_rows_deduped)
        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "scraped_input_count": len(scraped_rows),
            "relevant_count": len(relevant_rows_deduped),
            "classification_errors": classification_errors,
            "source_scraped_tab": f"linkedin_posts_scraped_{resolved_run_date}",
            "relevant_tab": f"linkedin_posts_relevant_{resolved_run_date}",
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS[pipeline_run_id] = metrics
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
        LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("linkedin-posts-classify-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def _resolve_scraped_run_date(run_date: str | None) -> str:
    if run_date and run_date.strip():
        return run_date.strip()
    today = date.today().isoformat()
    titles = _list_worksheet_titles()
    today_tab = f"linkedin_posts_scraped_{today}"
    if today_tab in titles:
        return today
    latest = _latest_scraped_tab_date(titles)
    if latest:
        return latest
    raise RuntimeError("No linkedin_posts_scraped_{date} worksheet found to classify.")


def _list_worksheet_titles() -> list[str]:
    writer = _get_writer()
    return [ws.title for ws in writer.sheet.worksheets()]


def _latest_scraped_tab_date(titles: list[str]) -> str | None:
    dates: list[str] = []
    for title in titles:
        match = _SCRAPED_TAB_RE.match(title)
        if match:
            dates.append(match.group(1))
    return max(dates) if dates else None


def _read_scraped_rows(run_date: str) -> list[dict[str, Any]]:
    writer = _get_writer()
    tab = f"linkedin_posts_scraped_{run_date}"
    worksheet = writer.sheet.worksheet(tab)
    rows = worksheet_row_dicts(worksheet)
    if not rows:
        raise RuntimeError(f"No rows found in worksheet {tab}.")
    return [dict(r) for r in rows]


def _get_writer() -> GoogleSheetsWriter:
    import os

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    return GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)


def get_linkedin_posts_scrape_only_metrics(run_id: str) -> dict[str, Any] | None:
    return LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS.get(run_id)


def get_linkedin_posts_classify_only_metrics(run_id: str) -> dict[str, Any] | None:
    return LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS.get(run_id)
