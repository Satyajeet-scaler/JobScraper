import logging
import os
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

from services.apify_linkedin_posts import normalize_linkedin_post_item, scrape_linkedin_posts
from services.mysql_linkedin_posts_store import (
    fetch_unclassified_linkedin_posts,
    mark_linkedin_posts_classify_done,
    upsert_linkedin_post,
    upsert_linkedin_post_relevance,
)
from services.linkedin_posts_pipeline import (
    _build_actor_input,
    _classify_relevant_posts,
    _collect_source_columns,
    _dedupe_linkedin_relevant_rows,
)

logger = logging.getLogger(__name__)

LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS: dict[str, dict[str, Any]] = {}
LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS: dict[str, dict[str, Any]] = {}


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

        count = 0
        for row in normalized:
            try:
                upsert_linkedin_post(row)
                count += 1
            except Exception as exc:
                logger.warning(
                    "linkedin-posts-scrape-only[%s] mysql upsert failed url=%s err=%s",
                    pipeline_run_id,
                    row.get("post_url"),
                    exc,
                )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "scraped_count": len(normalized),
            "mysql_upserted_count": count,
            "source_columns": source_columns,
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
        batch_size = max(1, int(os.getenv("LINKEDIN_POSTS_CLASSIFY_BATCH_SIZE", "30")))
        scraped_rows = fetch_unclassified_linkedin_posts(
            requested_role="",
            run_date=resolved_run_date,
            limit=batch_size,
        )
        relevant_rows, classification_errors = _classify_relevant_posts(scraped_rows)
        relevant_rows_deduped = _dedupe_linkedin_relevant_rows(relevant_rows)

        rel_count = 0
        for row in relevant_rows_deduped:
            try:
                upsert_linkedin_post_relevance(row)
                rel_count += 1
            except Exception as exc:
                logger.warning(
                    "linkedin-posts-classify-only[%s] mysql relevance upsert failed url=%s err=%s",
                    pipeline_run_id,
                    row.get("post_url"),
                    exc,
                )

        post_ids = [int(r.get("id") or 0) for r in scraped_rows if int(r.get("id") or 0) > 0]
        if post_ids:
            mark_linkedin_posts_classify_done(post_ids=post_ids)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "scraped_input_count": len(scraped_rows),
            "relevant_count": len(relevant_rows_deduped),
            "mysql_relevance_upserted_count": rel_count,
            "classification_errors": classification_errors,
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
    from services.mysql_linkedin_posts_store import _db
    sql = """
    SELECT MAX(run_date) AS latest_run_date
    FROM linkedin_posts
    WHERE requested_role = ''
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone() or {}
    latest = str((row or {}).get("latest_run_date") or "").strip()
    if latest:
        return latest
    raise RuntimeError("No linkedin_posts rows found in MySQL to classify.")


def get_linkedin_posts_scrape_only_metrics(run_id: str) -> dict[str, Any] | None:
    return LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS.get(run_id)


def get_linkedin_posts_classify_only_metrics(run_id: str) -> dict[str, Any] | None:
    return LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS.get(run_id)
