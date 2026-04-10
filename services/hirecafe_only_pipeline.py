import logging
import os
import traceback
import uuid
from datetime import datetime
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

from services.google_sheets import GoogleSheetsWriter
from services.hire_cafe import normalize_hirecafe_item, scrape_hirecafe_jobs

logger = logging.getLogger(__name__)
HIRECAFE_RUN_METRICS: dict[str, dict[str, Any]] = {}


def _sheet_run_date_ist() -> str:
    """YYYY-MM-DD in ``CRON_TIMEZONE`` (default Asia/Kolkata / IST), not the host system date."""
    tz = ZoneInfo(os.getenv("CRON_TIMEZONE", "Asia/Kolkata"))
    return datetime.now(tz).strftime("%Y-%m-%d")


def run_hirecafe_scrape_only_pipeline(run_id: str | None = None) -> dict[str, Any]:
    """
    Scrape only HireCafe and write normalized rows to a dedicated sheet tab.
    No classification and no Slack delivery.
    """
    pipeline_run_id = run_id or str(uuid.uuid4())
    run_date = _sheet_run_date_ist()
    started_at = perf_counter()
    HIRECAFE_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": run_date,
    }

    max_samples = int(os.getenv("HIRECAFE_MAX_SAMPLES", "200"))

    try:
        logger.info(
            "hirecafe-only pipeline[%s] started max_samples=%s",
            pipeline_run_id,
            max_samples,
        )
        raw_jobs = scrape_hirecafe_jobs(max_samples=max_samples)

        enriched_rows: list[dict[str, Any]] = []
        for raw in raw_jobs:
            normalized = normalize_hirecafe_item(raw)
            normalized["run_date"] = run_date
            normalized["role_query"] = "hire.cafe"
            enriched_rows.append(normalized)

        spreadsheet_id = os.getenv("HIRECAFE_GOOGLE_SPREADSHEET_ID") or os.getenv("GOOGLE_SPREADSHEET_ID")
        if not spreadsheet_id:
            raise RuntimeError("Set HIRECAFE_GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_ID.")

        tab_template = os.getenv("HIRECAFE_SCRAPED_TAB_TEMPLATE", "hirecafe_scraped_jobs_{date}")
        tab_name = tab_template.format(date=run_date)
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
        writer.write_rows(tab_name, enriched_rows, chunk_size=chunk_size)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": run_date,
            "tab_name": tab_name,
            "scraped_count": len(enriched_rows),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        HIRECAFE_RUN_METRICS[pipeline_run_id] = metrics
        logger.info(
            "hirecafe-only pipeline[%s] completed scraped_count=%s",
            pipeline_run_id, len(enriched_rows),
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
        HIRECAFE_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("hirecafe-only pipeline[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_hirecafe_run_metrics(run_id: str) -> dict[str, Any] | None:
    return HIRECAFE_RUN_METRICS.get(run_id)
