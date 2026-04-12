import logging
import os
import traceback
import uuid
from datetime import datetime
from time import perf_counter
from typing import Any
from zoneinfo import ZoneInfo

from services.google_sheets import GoogleSheetsWriter
from services.hirist import HiristTechService, normalize_hirist_item

logger = logging.getLogger(__name__)
HIRIST_RUN_METRICS: dict[str, dict[str, Any]] = {}


def _sheet_run_date_ist() -> str:
    """YYYY-MM-DD in ``CRON_TIMEZONE`` (default Asia/Kolkata / IST), not the host system date."""
    tz = ZoneInfo(os.getenv("CRON_TIMEZONE", "Asia/Kolkata"))
    return datetime.now(tz).strftime("%Y-%m-%d")


def run_hirist_scrape_only_pipeline(run_id: str | None = None) -> dict[str, Any]:
    """
    Scrape only Hirist.tech and write normalized rows to a dedicated sheet tab.
    No classification and no Slack delivery.
    """
    pipeline_run_id = run_id or str(uuid.uuid4())
    run_date = _sheet_run_date_ist()
    started_at = perf_counter()
    HIRIST_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": run_date,
    }

    try:
        logger.info("hirist-only pipeline[%s] started", pipeline_run_id)

        hirist_max_scrolls = int(os.getenv("HIRIST_MAX_SCROLLS", "250"))
        hirist_max_runtime = int(os.getenv("HIRIST_MAX_RUNTIME_SECONDS", "300"))
        hirist_max_idle = int(os.getenv("HIRIST_MAX_IDLE_SECONDS", "90"))
        hirist_min_scroll_delay = float(os.getenv("HIRIST_MIN_SCROLL_DELAY_SECONDS", "1.0"))
        hirist_max_scroll_delay = float(os.getenv("HIRIST_MAX_SCROLL_DELAY_SECONDS", "2.0"))
        hirist_headless = os.getenv("HIRIST_HEADLESS", "true").lower() not in ("0", "false", "no")
        hirist_recent_hours = int(os.getenv("HIRIST_RECENT_MAX_AGE_HOURS", "24"))
        hirist_include_desc = os.getenv("HIRIST_INCLUDE_JOB_DESCRIPTION", "true").lower() not in ("0", "false", "no")

        result = HiristTechService.scrape_hirist_categories(
            max_scrolls=hirist_max_scrolls,
            max_runtime_seconds=hirist_max_runtime,
            max_idle_seconds=hirist_max_idle,
            min_scroll_delay_seconds=hirist_min_scroll_delay,
            max_scroll_delay_seconds=hirist_max_scroll_delay,
            headless=hirist_headless,
            recent_job_max_age_hours=hirist_recent_hours,
            include_job_description=hirist_include_desc,
        )

        enriched_rows: list[dict[str, Any]] = []
        for card in result.get("recent_jobs") or []:
            normalized = normalize_hirist_item(card)
            normalized["run_date"] = run_date
            normalized["role_query"] = "hirist.tech"
            enriched_rows.append(normalized)

        spreadsheet_id = os.getenv("HIRIST_GOOGLE_SPREADSHEET_ID") or os.getenv("GOOGLE_SPREADSHEET_ID")
        if not spreadsheet_id:
            raise RuntimeError("Set HIRIST_GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_ID.")

        tab_template = os.getenv("HIRIST_SCRAPED_TAB_TEMPLATE", "hirist_scraped_jobs_{date}")
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
            "output_file": result.get("output_file"),
            "total_recent_jobs": result.get("total_recent_jobs"),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        HIRIST_RUN_METRICS[pipeline_run_id] = metrics
        logger.info(
            "hirist-only pipeline[%s] completed scraped_count=%s",
            pipeline_run_id,
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
        HIRIST_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("hirist-only pipeline[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_hirist_run_metrics(run_id: str) -> dict[str, Any] | None:
    return HIRIST_RUN_METRICS.get(run_id)
