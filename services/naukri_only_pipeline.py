import logging
import os
import uuid
import traceback
from datetime import date
from time import perf_counter
from typing import Any

from services.apify_naukri import normalize_naukri_item, scrape_naukri_jobs
from services.description_text_parts import apply_three_part_text_columns
from services.google_sheets import GoogleSheetsWriter

logger = logging.getLogger(__name__)
NAUKRI_RUN_METRICS: dict[str, dict[str, Any]] = {}


def run_naukri_scrape_only_pipeline(run_id: str | None = None) -> dict[str, Any]:
    """
    Scrape only Naukri via Apify and write scraped rows to a dedicated sheet tab.
    No classification and no Slack delivery.
    """
    pipeline_run_id = run_id or str(uuid.uuid4())
    run_date = date.today().isoformat()
    started_at = perf_counter()
    NAUKRI_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": run_date,
    }

    keyword = os.getenv(
        "APIFY_NAUKRI_KEYWORD",
        "developer, data engineer, data analyst, data scientist, devops engineer, platform engineer,",
    )
    max_jobs = int(os.getenv("APIFY_MAX_JOBS_NAUKRI", "100"))
    freshness = os.getenv("APIFY_FRESHNESS", "1")
    fetch_details = os.getenv("APIFY_FETCH_DETAILS", "false").lower() == "true"

    try:
        logger.info(
            "naukri-only pipeline[%s] started keyword=%s max_jobs=%s freshness=%s",
            pipeline_run_id,
            keyword,
            max_jobs,
            freshness,
        )
        rows = scrape_naukri_jobs(
            keyword=keyword,
            max_jobs=max_jobs,
            freshness=freshness,
            fetch_details=fetch_details,
        )
        # Add run metadata columns while preserving all scraped columns from actor output.
        enriched_rows: list[dict[str, Any]] = []
        for row in rows:
            copy = dict(row)
            normalized = normalize_naukri_item(copy)
            if normalized.get("description"):
                copy["description"] = normalized.get("description")
            copy["run_date"] = run_date
            copy["source"] = "naukri"
            enriched_rows.append(copy)

        spreadsheet_id = os.getenv("NAUKRI_GOOGLE_SPREADSHEET_ID") or os.getenv("GOOGLE_SPREADSHEET_ID")
        if not spreadsheet_id:
            raise RuntimeError("Set NAUKRI_GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_ID.")

        tab_name = os.getenv("NAUKRI_SCRAPED_TAB_TEMPLATE", "naukri_scraped_jobs_{date}").format(date=run_date)
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        rows_for_sheet, overflow_rows, overflow_chars = apply_three_part_text_columns(enriched_rows, "description")
        if overflow_rows:
            logger.warning(
                "description split truncated rows=%s overflow_chars=%s tab=%s",
                overflow_rows,
                overflow_chars,
                tab_name,
            )
        writer.write_rows(
            tab_name,
            rows_for_sheet,
            chunk_size=max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200"))),
        )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": run_date,
            "tab_name": tab_name,
            "scraped_count": len(rows_for_sheet),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        NAUKRI_RUN_METRICS[pipeline_run_id] = metrics
        logger.info("naukri-only pipeline[%s] completed scraped_count=%s", pipeline_run_id, len(rows_for_sheet))
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
        NAUKRI_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("naukri-only pipeline[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_naukri_run_metrics(run_id: str) -> dict[str, Any] | None:
    return NAUKRI_RUN_METRICS.get(run_id)

