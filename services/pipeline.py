import json
import logging
import math
import os
import uuid
import traceback
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import date, datetime, timedelta, timezone
from time import perf_counter, sleep
from typing import Any

import requests
from jobspy import scrape_jobs
from pandas import DataFrame, to_datetime

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.linkedin_recruiter.sheets_pipeline import write_linkedin_recruiters_for_relevant_jobs
from services.linkedin_posts_pipeline import run_linkedin_posts_pipeline, post_linkedin_posts_slack_handover
from services.slack_handover_notify import (
    load_linkedin_relevant_posts_from_sheet,
    send_handover_notifications,
    send_slack_text,
)
from services.apify_naukri import normalize_naukri_item, scrape_naukri_jobs
try:
    import google.generativeai as genai
except ImportError:  # pragma: no cover - optional dependency behavior
    genai = None

TARGET_ROLES = [
    "Developer",
    "Data Engineer",
    "Data Analyst",
    "Data Scientist",
    "Devops Engineer",
    "Platform Engineer",
    "SRE",
    "QA",
    "SDET",
]
TARGET_SITES = ["linkedin", "indeed"]
PIPELINE_RUN_METRICS: dict[str, dict[str, Any]] = {}
logger = logging.getLogger(__name__)


def run_daily_jobs_pipeline(run_id: str | None = None) -> dict[str, Any]:
    """Orchestrates daily scraping, relevance filtering, sheets write, and Slack alert."""
    pipeline_run_id = run_id or str(uuid.uuid4())
    run_date = date.today().isoformat()
    started_at = perf_counter()
    PIPELINE_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": run_date,
        "started": True,
    }

    linkedin_posts_metrics: dict[str, Any] | None = None
    linkedin_posts_error: str = ""
    run_linkedin_posts_enabled = os.getenv("LINKEDIN_POSTS_PIPELINE_ENABLED", "true").lower() in ("1", "true", "yes")

    try:
        logger.info("pipeline[%s] started run_date=%s", pipeline_run_id, run_date)
        scraped = _scrape_target_jobs()
        logger.info("pipeline[%s] scrape completed scraped_count=%s", pipeline_run_id, len(scraped))
        deduped = _dedupe_jobs(scraped)
        logger.info("pipeline[%s] dedupe completed deduped_count=%s", pipeline_run_id, len(deduped))
        _write_scraped_jobs_to_google_sheets(run_date=run_date, scraped_jobs=deduped)
        logger.info("pipeline[%s] scraped_jobs sheet write completed", pipeline_run_id)

        relevant, classifier_metrics = _classify_relevant_jobs(deduped)
        logger.info(
            "pipeline[%s] classification completed relevant_count=%s classification_errors=%s",
            pipeline_run_id,
            len(relevant),
            classifier_metrics["classification_errors"],
        )
        _write_relevant_jobs_to_google_sheets(run_date=run_date, relevant_jobs=relevant)
        logger.info("pipeline[%s] relevant_jobs sheet write completed", pipeline_run_id)

        # --- Phase 1: scrape + classify + write sheets in PARALLEL ---
        def _recruiter_scrape_flow() -> int:
            """Scrape recruiters and write sheet; returns rows_written."""
            try:
                rows_written, _ = write_linkedin_recruiters_for_relevant_jobs(
                    run_date=run_date,
                    relevant_jobs=relevant,
                )
                logger.info(
                    "pipeline[%s] linkedin recruiters sheet completed rows_written=%s",
                    pipeline_run_id, rows_written,
                )
                return rows_written
            except Exception as exc:
                logger.warning(
                    "pipeline[%s] linkedin recruiters sheet failed but pipeline will continue: %s",
                    pipeline_run_id, exc,
                )
                return 0

        def _linkedin_posts_scrape_flow() -> tuple[dict[str, Any] | None, str]:
            """Scrape + classify + write sheets (NO Slack); returns (metrics, error)."""
            if not run_linkedin_posts_enabled:
                return None, ""
            try:
                return run_linkedin_posts_pipeline(send_slack=False), ""
            except Exception as exc:
                logger.warning("pipeline[%s] linkedin-posts pipeline failed: %s", pipeline_run_id, exc)
                return None, str(exc)

        logger.info("pipeline[%s] starting recruiter + linkedin-posts scraping in parallel", pipeline_run_id)
        with ThreadPoolExecutor(max_workers=2) as executor:
            recruiter_future: Future = executor.submit(_recruiter_scrape_flow)
            linkedin_posts_future: Future = executor.submit(_linkedin_posts_scrape_flow)

        recruiter_sheet_rows = recruiter_future.result()
        linkedin_posts_metrics, linkedin_posts_error = linkedin_posts_future.result()
        logger.info("pipeline[%s] parallel scraping completed", pipeline_run_id)

        # --- Phase 2: send ALL Slack messages SEQUENTIALLY ---
        # Case 3 (recruiter detail) + Case 2 (internal POC) first
        handover_notifications_sent = 0
        recruiter_case3_count = 0
        recruiter_case2_count = 0
        try:
            handover_notifications_sent = _post_recruiter_handover_notifications(run_date=run_date)
            recruiter_case3_count, recruiter_case2_count = _get_recruiter_handover_case_counts(run_date=run_date)
            logger.info(
                "pipeline[%s] recruiter handover notifications sent=%s",
                pipeline_run_id, handover_notifications_sent,
            )
        except Exception as exc:
            logger.warning(
                "pipeline[%s] recruiter handover notifications failed but pipeline will continue: %s",
                pipeline_run_id, exc,
            )

        # Case 1 (LinkedIn posts) after recruiter cases
        if linkedin_posts_metrics and linkedin_posts_metrics.get("status") == "completed":
            try:
                post_linkedin_posts_slack_handover(
                    run_date=run_date,
                    scraped_rows=[],
                    relevant_rows=_load_relevant_linkedin_posts_from_sheet(run_date),
                )
            except Exception as exc:
                logger.warning(
                    "pipeline[%s] linkedin-posts slack handover failed: %s",
                    pipeline_run_id, exc,
                )

        linkedin_case1_count = (
            int(linkedin_posts_metrics.get("relevant_count", 0)) if linkedin_posts_metrics else 0
        )
        logger.info(
            "pipeline[%s] final summary: leads_scraped=%s relevant=%s recruiter_detail=%s internal_poc=%s linkedin_posts=%s",
            pipeline_run_id, len(deduped), len(relevant), recruiter_case3_count, recruiter_case2_count, linkedin_case1_count,
        )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": run_date,
            "scraped_count": len(scraped),
            "deduped_count": len(deduped),
            "relevant_count": len(relevant),
            "classification_errors": classifier_metrics["classification_errors"],
            "recruiter_sheet_rows": recruiter_sheet_rows,
            "handover_notifications_sent": handover_notifications_sent,
            "handover_case3_recruiter_detail_count": recruiter_case3_count,
            "handover_case2_internal_poc_count": recruiter_case2_count,
            "handover_case1_linkedin_post_count": linkedin_case1_count,
            "linkedin_posts_enabled": run_linkedin_posts_enabled,
            "linkedin_posts_status": (
                "completed"
                if linkedin_posts_metrics and linkedin_posts_metrics.get("status") == "completed"
                else ("failed" if linkedin_posts_error else "disabled")
            ),
            "linkedin_posts_scraped_count": (
                linkedin_posts_metrics.get("scraped_count") if linkedin_posts_metrics else 0
            ),
            "linkedin_posts_relevant_count": (
                linkedin_posts_metrics.get("relevant_count") if linkedin_posts_metrics else 0
            ),
            "linkedin_posts_error": linkedin_posts_error,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        PIPELINE_RUN_METRICS[pipeline_run_id] = metrics
        logger.info("pipeline[%s] completed duration_seconds=%s", pipeline_run_id, metrics["duration_seconds"])
        return metrics
    except Exception as exc:
        tb = traceback.format_exc()
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": run_date,
            "error": str(exc),
            "traceback": tb,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        PIPELINE_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("pipeline[%s] failed: %s", pipeline_run_id, exc)
        raise


def _scrape_target_jobs() -> list[dict[str, Any]]:
    location = os.getenv("DAILY_PIPELINE_LOCATION", "India")
    country_indeed = os.getenv("DAILY_PIPELINE_COUNTRY_INDEED", "india")
    default_results_wanted = int(os.getenv("DAILY_PIPELINE_RESULTS_WANTED", "30"))
    linkedin_results_wanted = int(os.getenv("JOBSPY_RESULTS_WANTED_LINKEDIN", str(default_results_wanted)))
    indeed_results_wanted = int(os.getenv("JOBSPY_RESULTS_WANTED_INDEED", str(default_results_wanted)))

    naukri_max_jobs = int(os.getenv("APIFY_MAX_JOBS_NAUKRI", str(default_results_wanted)))
    naukri_freshness = os.getenv("APIFY_FRESHNESS", "1")
    naukri_fetch_details = os.getenv("APIFY_FETCH_DETAILS", "false").lower() == "true"
    naukri_keyword = os.getenv(
        "APIFY_NAUKRI_KEYWORD",
        "developer, data engineer, data analyst, data scientist, devops engineer, platform engineer, sre, qa engineer, sdet,",
    )

    all_jobs: list[dict[str, Any]] = []
    # Naukri via Apify (single call by default using combined keyword)
    if os.getenv("APIFY_TOKEN"):
        logger.info(
            "naukri scrape single-call keyword=%s max_jobs=%s freshness=%s",
            naukri_keyword,
            naukri_max_jobs,
            naukri_freshness,
        )
        try:
            naukri_raw = _retry(
                action=lambda: scrape_naukri_jobs(
                    keyword=naukri_keyword,
                    max_jobs=naukri_max_jobs,
                    freshness=naukri_freshness,
                    fetch_details=naukri_fetch_details,
                ),
                retries=2,
                initial_delay_seconds=2.0,
            )
            naukri_items: list[dict[str, Any]] = []
            for raw in naukri_raw:
                normalized = normalize_naukri_item(raw)
                normalized["role_query"] = "multi"
                naukri_items.append(normalized)
            logger.info("naukri scrape completed fetched=%s", len(naukri_items))
            all_jobs.extend(naukri_items)
        except Exception as exc:
            logger.warning("naukri scrape failed: %s", exc)
    else:
        logger.info("naukri scrape skipped (APIFY_TOKEN not set)")

    for role in TARGET_ROLES:
        logger.info(
            "scrape role=%s linkedin_results=%s indeed_results=%s naukri_max=%s hours_old=24",
            role,
            linkedin_results_wanted,
            indeed_results_wanted,
            naukri_max_jobs,
        )

        # LinkedIn via JobSpy
        linkedin_df = _retry(
            action=lambda: scrape_jobs(
                site_name=["linkedin"],
                search_term=role,
                location=location,
                results_wanted=linkedin_results_wanted,
                hours_old=24,
                linkedin_fetch_description=True,
                offset=0,
                verbose=0,
            ),
            retries=3,
            initial_delay_seconds=1.5,
        )
        linkedin_items = _sanitize_for_json(_dataframe_to_response(linkedin_df))
        for item in linkedin_items:
            item["role_query"] = role
        logger.info("scrape role=%s linkedin_fetched=%s", role, len(linkedin_items))
        all_jobs.extend(linkedin_items)

        # Indeed via JobSpy
        indeed_df = _retry(
            action=lambda: scrape_jobs(
                site_name=["indeed"],
                search_term=role,
                location=location,
                country_indeed=country_indeed,
                results_wanted=indeed_results_wanted,
                hours_old=24,
                offset=0,
                verbose=0,
            ),
            retries=3,
            initial_delay_seconds=1.5,
        )
        indeed_items = _sanitize_for_json(_dataframe_to_response(indeed_df))
        for item in indeed_items:
            item["role_query"] = role
        logger.info("scrape role=%s indeed_fetched=%s", role, len(indeed_items))
        all_jobs.extend(indeed_items)
    normalized_jobs = [_normalize_job(job) for job in all_jobs]
    _log_naukri_missing_fields(normalized_jobs)
    filtered_jobs = _filter_jobs_last_24_hours(normalized_jobs)
    logger.info(
        "scrape time filter applied before=%s after=%s dropped=%s",
        len(normalized_jobs),
        len(filtered_jobs),
        len(normalized_jobs) - len(filtered_jobs),
    )
    return filtered_jobs


def _dataframe_to_response(jobs_df: DataFrame) -> list[dict[str, Any]]:
    normalized_df = jobs_df.where(jobs_df.notna(), None)
    return normalized_df.to_dict(orient="records")


def _sanitize_for_json(value: Any):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(item) for item in value]
    return value


def _normalize_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_date": date.today().isoformat(),
        "role_query": job.get("role_query"),
        "site": job.get("site") or "unknown",
        "title": job.get("title"),
        "company": job.get("company") or job.get("company_name"),
        "location": job.get("location"),
        "date_posted": job.get("date_posted"),
        "job_url": job.get("job_url") or job.get("job_url_direct"),
        "description": job.get("description"),
        "experience": job.get("experience"),
        "salary": job.get("salary"),
        "job_type": job.get("job_type"),
        "raw_payload": job,
    }


def _dedupe_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for job in jobs:
        site = (job.get("site") or "").strip().lower()
        job_url = (job.get("job_url") or "").strip()
        if not job_url:
            continue
        key = (site, job_url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(job)
    return deduped


def _log_naukri_missing_fields(jobs: list[dict[str, Any]]) -> None:
    naukri_jobs = [j for j in jobs if (j.get("site") or "").strip().lower() == "naukri"]
    if not naukri_jobs:
        return

    def missing_count(field: str) -> int:
        return sum(1 for j in naukri_jobs if not (j.get(field) or "").strip())

    missing_job_url = missing_count("job_url")
    missing_title = missing_count("title")
    missing_company = missing_count("company")
    missing_location = missing_count("location")
    missing_date_posted = sum(1 for j in naukri_jobs if j.get("date_posted") in (None, "", "NaT"))

    if missing_job_url or missing_title or missing_company or missing_location or missing_date_posted:
        logger.warning(
            "naukri missing fields total=%s missing_job_url=%s missing_title=%s missing_company=%s missing_location=%s missing_date_posted=%s",
            len(naukri_jobs),
            missing_job_url,
            missing_title,
            missing_company,
            missing_location,
            missing_date_posted,
        )
    else:
        logger.info("naukri fields complete total=%s", len(naukri_jobs))


def _filter_jobs_last_24_hours(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    filtered: list[dict[str, Any]] = []
    for job in jobs:
        raw_date = job.get("date_posted")
        if raw_date in (None, ""):
            # Keep when source doesn't provide date reliably.
            filtered.append(job)
            continue

        parsed = to_datetime(raw_date, utc=True, errors="coerce")
        if str(parsed) == "NaT":
            # Keep unparseable values to avoid dropping potentially valid jobs.
            filtered.append(job)
            continue

        if parsed >= cutoff:
            filtered.append(job)
    return filtered


def _classify_relevant_jobs(jobs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
    ai_url = os.getenv("AI_CLASSIFIER_URL")
    ai_token = os.getenv("AI_CLASSIFIER_TOKEN")
    ai_batch_size = max(1, int(os.getenv("AI_CLASSIFIER_BATCH_SIZE", "50")))
    prompt_template = os.getenv(
        "AI_RELEVANCE_PROMPT",
        _default_relevance_prompt(),
    )

    relevant_jobs: list[dict[str, Any]] = []
    classification_errors = 0

    batches = _chunk(jobs, ai_batch_size)
    logger.info(
        "classification mode=%s jobs=%s batches=%s batch_size=%s workers=%s",
        "gemini" if gemini_api_key else ("external_ai" if ai_url else "keyword_fallback"),
        len(jobs),
        len(batches),
        ai_batch_size,
        1,
    )
    for idx, batch in enumerate(batches, start=1):
        logger.info("classification batch=%s/%s size=%s", idx, len(batches), len(batch))
        if gemini_api_key:
            try:
                decisions = _classify_batch_with_gemini(
                    jobs=batch,
                    prompt=prompt_template,
                    api_key=gemini_api_key,
                    model_name=gemini_model,
                )
            except Exception as exc:
                classification_errors += len(batch)
                logger.warning("classification batch error: %s", exc)
                continue
            for job, decision in zip(batch, decisions):
                if decision.get("is_relevant"):
                    enriched = dict(job)
                    enriched["matched_role"] = decision.get("matched_role", "")
                    enriched["role_category"] = decision.get("role_category", "")
                    enriched["priority"] = decision.get("priority", "")
                    enriched["reason"] = decision.get("reason", "")
                    enriched["confidence"] = decision.get("confidence", "")
                    relevant_jobs.append(enriched)
            logger.info(
                "classification batch=%s/%s completed relevant_total_so_far=%s errors_so_far=%s",
                idx,
                len(batches),
                len(relevant_jobs),
                classification_errors,
            )
            continue

        for job in batch:
            try:
                decision = _classify_single_job(
                    job,
                    gemini_api_key,
                    gemini_model,
                    ai_url,
                    ai_token,
                    prompt_template,
                )
            except Exception as exc:
                classification_errors += 1
                logger.warning("classification error: %s", exc)
                continue
            if decision.get("is_relevant"):
                enriched = dict(job)
                enriched["matched_role"] = decision.get("matched_role", "")
                enriched["role_category"] = decision.get("role_category", "")
                enriched["priority"] = decision.get("priority", "")
                enriched["reason"] = decision.get("reason", "")
                enriched["confidence"] = decision.get("confidence", "")
                relevant_jobs.append(enriched)
        logger.info(
            "classification batch=%s/%s completed relevant_total_so_far=%s errors_so_far=%s",
            idx,
            len(batches),
            len(relevant_jobs),
            classification_errors,
        )

    return relevant_jobs, {"classification_errors": classification_errors}


def _classify_single_job(
    job: dict[str, Any],
    gemini_api_key: str | None,
    gemini_model: str,
    ai_url: str | None,
    ai_token: str | None,
    prompt: str,
) -> dict[str, Any]:
    if gemini_api_key:
        return _classify_with_gemini(
            job=job,
            prompt=prompt,
            api_key=gemini_api_key,
            model_name=gemini_model,
        )

    if ai_url:
        payload = {
            "prompt": prompt,
            "job": {
                "title": job.get("title"),
                "company": job.get("company"),
                "location": job.get("location"),
                "description": job.get("description"),
                "experience": job.get("experience"),
                "salary": job.get("salary"),
                "job_type": job.get("job_type"),
                "site": job.get("site"),
                "job_url": job.get("job_url"),
            },
        }
        headers = {"Content-Type": "application/json"}
        if ai_token:
            headers["Authorization"] = f"Bearer {ai_token}"
        response = _retry(
            action=lambda: requests.post(ai_url, json=payload, headers=headers, timeout=45),
            retries=3,
            initial_delay_seconds=1.0,
        )
        response.raise_for_status()
        data = _normalize_classifier_decision(response.json())
        if "is_relevant" not in data:
            raise ValueError("AI response missing relevance field")
        return data

    text = f"{job.get('title', '')} {job.get('description', '')}".lower()
    for role in TARGET_ROLES:
        if role.lower() in text:
            return {
                "is_relevant": True,
                "matched_role": role,
                "reason": "Matched role keyword in title/description.",
                "confidence": 0.6,
            }
    return {"is_relevant": False, "matched_role": "", "reason": "No role match.", "confidence": 0.0}


def _classify_with_gemini(
    job: dict[str, Any],
    prompt: str,
    api_key: str,
    model_name: str,
) -> dict[str, Any]:
    if genai is None:
        raise RuntimeError("google-generativeai package is not installed.")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name, system_instruction=prompt)
    description = (job.get("description") or "")[:4000]
    content = (
        "Return ONLY JSON with keys: relevant, reason, role_category, priority.\n"
        "Job payload:\n"
        f"{json.dumps({'title': job.get('title'), 'company': job.get('company'), 'location': job.get('location'), 'description': description, 'experience': job.get('experience'), 'salary': job.get('salary'), 'job_type': job.get('job_type'), 'site': job.get('site'), 'job_url': job.get('job_url')}, ensure_ascii=True)}"
    )

    response = _retry(
        action=lambda: model.generate_content(content),
        retries=3,
        initial_delay_seconds=1.0,
    )
    parsed = _parse_classifier_json(getattr(response, "text", "") or "")
    normalized = _normalize_classifier_decision(parsed)
    if "is_relevant" not in normalized:
        raise ValueError("Gemini response missing relevance field.")
    return {
        "is_relevant": bool(normalized.get("is_relevant")),
        "matched_role": str(normalized.get("matched_role", "")),
        "role_category": str(normalized.get("role_category", "")),
        "priority": str(normalized.get("priority", "")),
        "reason": str(normalized.get("reason", "")),
        "confidence": normalized.get("confidence", 0),
    }


def _classify_batch_with_gemini(
    jobs: list[dict[str, Any]],
    prompt: str,
    api_key: str,
    model_name: str,
) -> list[dict[str, Any]]:
    if genai is None:
        raise RuntimeError("google-generativeai package is not installed.")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name, system_instruction=prompt)
    payload = [
        {
            "row": idx,
            "title": job.get("title"),
            "company": job.get("company"),
            "location": job.get("location"),
            "description": (job.get("description") or "")[:3000],
            "experience": job.get("experience"),
            "salary": job.get("salary"),
            "job_type": job.get("job_type"),
            "site": job.get("site"),
            "job_url": job.get("job_url"),
        }
        for idx, job in enumerate(jobs, start=1)
    ]
    content = (
        "Classify all rows below in one response.\n"
        "Return ONLY a JSON array.\n"
        "Each returned item must include: row, relevant, reason, role_category, priority.\n"
        "Only include items where relevant=true.\n"
        f"{json.dumps(payload, ensure_ascii=True)}"
    )
    response = _retry(
        action=lambda: model.generate_content(content),
        retries=3,
        initial_delay_seconds=1.0,
    )
    parsed_any = _parse_classifier_json(getattr(response, "text", "") or "")
    if not isinstance(parsed_any, list):
        raise ValueError("Gemini batch response must be a JSON array.")

    by_row: dict[int, dict[str, Any]] = {}
    for item in parsed_any:
        if not isinstance(item, dict):
            continue
        row_idx = item.get("row")
        if isinstance(row_idx, int):
            normalized = _normalize_classifier_decision(item)
            # Ensure relevance field exists; treat missing as not relevant.
            if "is_relevant" not in normalized:
                normalized["is_relevant"] = False
            by_row[row_idx] = normalized

    decisions: list[dict[str, Any]] = []
    for idx in range(1, len(jobs) + 1):
        normalized = by_row.get(
            idx,
            {
                "is_relevant": False,
                "matched_role": "",
                "role_category": "",
                "priority": "",
                "reason": "Missing row decision from batch classifier.",
                "confidence": 0,
            },
        )
        decisions.append(normalized)
    return decisions




def _parse_classifier_json(raw_text: str) -> Any:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()

    try:
        data = json.loads(text)
        if isinstance(data, (dict, list)):
            return data
    except json.JSONDecodeError:
        pass

    obj_start = text.find("{")
    obj_end = text.rfind("}")
    arr_start = text.find("[")
    arr_end = text.rfind("]")
    if arr_start != -1 and arr_end != -1 and arr_end > arr_start:
        candidate = text[arr_start : arr_end + 1]
        data = json.loads(candidate)
        if isinstance(data, list):
            return data
    if obj_start != -1 and obj_end != -1 and obj_end > obj_start:
        candidate = text[obj_start : obj_end + 1]
        data = json.loads(candidate)
        if isinstance(data, dict):
            return data

    raise ValueError("Unable to parse classifier JSON response.")


def _normalize_classifier_decision(parsed: dict[str, Any]) -> dict[str, Any]:
    """Normalize multiple response schemas to one internal format."""
    if "is_relevant" in parsed:
        return parsed

    if "relevant" in parsed:
        return {
            "is_relevant": bool(parsed.get("relevant")),
            "matched_role": parsed.get("matched_role") or parsed.get("role_category", ""),
            "role_category": parsed.get("role_category", ""),
            "priority": parsed.get("priority", ""),
            "reason": parsed.get("reason", ""),
            "confidence": parsed.get("confidence", 0),
        }

    return parsed


def _default_relevance_prompt() -> str:
    return """# System Prompt: Job Listing Classifier for Tech Roles in India
# VERSION: Final (April 2026) — All iterations incorporated
# USE FOR: Scraped job listings from Naukri, Indeed, LinkedIn Jobs, HiringCafe, Wellfound
# EXPECTED REJECTION RATE: 50-65%

You are a job listing classifier. Your job is to evaluate each job listing and determine if it is a **genuine, relevant tech job opening from a real employer in India** that a placement team should pursue for their learners (primarily B.Tech / B.E. graduates with 0-3 years experience).

**The core question is NOT just "Is this a real job?" — it is "Is this company the actual employer, and can the placement team reach the hiring company through this listing?"** A genuine job at Google posted by a staffing agency is less valuable than a mediocre job at a startup posted by the startup itself. Prioritize direct employer listings over middlemen.

---

## Your Task

For each job listing provided (containing fields like company, title, location, description, experience, salary, job_type, job_url, site), respond ONLY with valid JSON:

```json
{
  "relevant": true or false,
  "reason": "brief explanation",
  "role_category": "one of: Developer | Data Engineer | Data Analyst | DevOps | Platform Engineer | SRE | QA | SDET | Data Scientist | Mixed",
  "priority": "P1 / P2 / P3 / P4"
}
```

If `relevant` is false, set `role_category` and `priority` to null.

---

## EVALUATION ORDER — Apply these checks in sequence. REJECT at the first failure:

### CHECK 1: Is the company the ACTUAL employer? (Section A)
### CHECK 2: Is this a target role? (Section B)
### CHECK 3: Is this in a target city? (Section C)
### CHECK 4: Is this spam/junk? (Section D)
### CHECK 5: Does education match? (Section E)

---

## SECTION A — EMPLOYER AUTHENTICITY (Most Important Rule)

**This is the single most important check.** The company must be the real employer building products/services — not a middleman posting on behalf of an unnamed client.

### A1. Keyword detection in company — REJECT if it contains:
Staffing, Consulting (combined with IT/HR/Solutions), Placement, Recruitment, Manpower, HR Services, Resources (as standalone company name), Talent (as standalone company name)

### A2. Named staffing agencies — auto-REJECT:
Scoutit, Haystack, People Prime Worldwide, CLIQHR, Codenera Recruitment, Fourways Consulting, Converse Placement, Connect IO IT, PRACYVA, ValueMap Technologies, Adam Infotech, Galaxy i Technologies, Numentica LLC, Odiware Technologies, Martinet Technologies, GK HR Consulting, Allyted Solutions, Spencer Ogden, GFMNOW, ImpaxionInc, Amakshar Technology Solutions, TeamLease (when posting for unnamed clients), Fiddle Leaf Solutions, Brevish Global Consulting, The Join Business Placement Consultancy, TIGI HR, THRIVE CAREER TODAY, Rekruton Technologies, Robral Technologies, Crossing Hurdles, eJAmerica, hackajob, Largeton Group, Oretes Consulting, Dark Bears Software, Metizsoft Solutions, Salvik India, Bebo Technologies, Digi Rush Solutions, mars webs solution, Variance InfoTech, Elovient Software Solutions, Sails Software, Nelgates Technologies, Wits Innovation Lab, F Jobs By Fashion TV India, 5 Exceptions Software Solutions, Shanno Corporate Solutions

### A3. JD-level detection — REJECT if the description says:
- "Hiring for our client" / "on behalf of" / "for a leading MNC" / "for a Fortune 500 company" (without naming the actual company)
- "Contract to hire with [unnamed company]" / "C2H opportunity"
- Generic description with no specific company culture, product, or team details

### A4. EXCEPTION for named clients:
If the listing names BOTH the staffing firm AND the actual client company (e.g., "TeamLease hiring for Flipkart"), mark as relevant but note "Posted via staffing agency — actual employer is [X]"

### A5. REJECT gig / freelance platforms:
Turing, Mindrift, Toptal, Upwork — gig matching, not direct employers.

### A6. REJECT crowdsourcing platforms:
Peroptyx, Appen, Lionbridge AI — data labeling task platforms.

### A7. REJECT internship mills / tiny unknowns:
Very small unknown companies offering "internships" with no real presence. Examples: Inficore Soft, AJ Media, Webs X UM, Webs IT Solution, HireX, MTEHealthIQ, MedTourEasy, Growth Heist. Clues: no recognizable brand, unpaid/very low stipend, generic description.

### A8. REJECT job board / aggregator company pages:
ICCCSAI, ITBOTS.IN, AI Jobs India, Career FI India, Best Talent Reach Job Board, The Job Company, Manufacturing Job's, Valtax Industries, WFH RECRUITMENT SERVICES, ProfileNext Career Services, Besant Technologies Porur, CyberWarLab, Frontlines EduTech (FLM), Software Testing Studio, Mechanical Engineering Design Hub, WORKNNECT, Next Innovate Techno Solutions, Headwy Consulting, Quikhyr.ai

### A9. REJECT regional LinkedIn showcase pages:
Tamil Nadu Jobs, Kerala Jobs, Bihar Jobs, Maharashtra Jobs, Haryana Jobs, Karnataka Jobs, West Bengal Jobs, Chattisgarh Jobs, Jharkhand Jobs, Uttarakhand Jobs, Punjab Jobs, Gujarat Jobs, Uttar Pradesh Jobs, Madhya Pradesh Jobs

### A10. REJECT freelance roles:
Any listing with "Freelancing" or "Freelance" in the title.

### How to detect a REAL employer (KEEP):
- Product companies: Google, Amazon, Flipkart, Razorpay, CRED, Swiggy, Zomato, PhonePe, Postman, etc.
- IT services (own bench): TCS, Infosys, Wipro, HCLTech, Cognizant, Accenture, Capgemini, Tech Mahindra, Mphasis, LTIMindtree
- Consulting (own teams): Deloitte, PwC, EY, KPMG
- MNC GCCs: Goldman Sachs, JPMorgan, Salesforce, Adobe, Microsoft, Wells Fargo, Uber, Cisco, Siemens, etc.
- Funded startups: any YC/Sequoia/Accel-backed startup with a real product

---

## SECTION B — TARGET ROLES (9 categories)

### 1. Developer
Software Engineer, Software Developer, Backend Developer, Frontend Developer, Full Stack Developer, Fullstack Developer, Web Developer, Mobile Developer, iOS Developer, Android Developer, SDE, SDE-1, SDE-2, Python Developer, Java Developer, React Developer, Node Developer, .NET Developer, MERN Stack Developer, MEAN Stack Developer, PHP Developer, Golang Developer, Ruby Developer, Application Developer, Product Engineer

### 2. Data Engineer
Data Engineer, ETL Developer, Big Data Engineer, Spark Engineer, Databricks Engineer, Data Pipeline Engineer, Analytics Engineer (if engineering-focused), Data Platform Engineer

### 3. Data Analyst (STRICT — Core Data Analytics ONLY)

**Only these three core roles are accepted:**
- **Data Analyst** — SQL, Excel, Python, dashboards, data visualization, reporting, insights
- **Business Analyst** — requirements gathering, data-driven decisions, dashboards, process improvement (NOT strategy consulting, NOT finance/accounting)
- **Product Analyst** — product metrics, user analytics, A/B testing, funnel analysis

**Also include IF the description confirms core data analytics work (SQL, dashboards, Tableau/Power BI):**
BI Analyst, Business Intelligence Analyst, Analytics Analyst, Data Analytics Analyst, MIS Analyst, Reporting Analyst

**REJECT ALL domain-specific "Analyst" roles — 30+ titles that are NOT Data Analyst:**
Risk Analyst, Credit Risk Analyst, Risk & Underwriting Analyst, Finance Analyst, Financial Analyst, FP&A Analyst, Investment Analyst, Fraud Analyst, Anti-Money Laundering Analyst, Compliance Analyst, Governance Analyst, Regulatory Analyst, Security Analyst, Threat Analyst, SOC Analyst, Cybersecurity Analyst, Operations Analyst, Supply Chain Analyst, Logistics Analyst, Marketing Analyst, Media Analyst, Biddable Media Analyst, Digital Marketing Analyst, Marine Analyst, Insurance Analyst, Actuarial Analyst, Cost Analyst, Pricing Analyst, Revenue Analyst, Equity Analyst, Trading Analyst, Portfolio Analyst, Trade Implementation Analyst, Collections Analyst, Asset Servicing Analyst, Cost Basis Reporting Analyst, Procurement Analyst, Vendor Analyst, Category Analyst, HR Analyst, People Analyst, Compensation Analyst, Payroll Analyst, Clinical Data Analyst, Pharma Analyst, Medical Analyst, Geospatial Analyst, GIS Analyst, Static Data Analyst, Reference Data Analyst, Master Data Analyst, Sustainability Data Analyst, Quantitative Analytics Specialist, Quantitative Analyst, Consultant, Associate Consultant, Advisory Analyst, Strategy Analyst, EDI Quality Assurance Analyst, Research Analyst (unless explicitly SQL/dashboard focused)

**Quality Analyst -> classify under QA/SDET, not Data Analyst**

**Ambiguous title ("Analyst" / "Senior Analyst" with no qualifier):**
Include ONLY if description mentions at least 2 of: SQL, Python, Tableau, Power BI, dashboards, data visualization, A/B testing, funnel analysis. When in doubt, REJECT.

### 4. DevOps Engineer
DevOps Engineer, DevOps Specialist, CI/CD Engineer, Release Engineer, Build Engineer, Infrastructure Engineer (if DevOps-focused)

### 5. Platform Engineer
Platform Engineer, Cloud Engineer, Cloud Platform Engineer, Infrastructure Engineer (if platform/cloud-focused)

### 6. SRE (Site Reliability Engineer)
Site Reliability Engineer, SRE, Reliability Engineer, Production Engineer

### 7. QA (Quality Assurance)
QA Engineer, Quality Analyst, Quality Assurance Engineer, Test Engineer, Manual Tester, Automation Tester, Test Lead (if hands-on), Performance Tester, QA Analyst

### 8. SDET (Software Development Engineer in Test)
SDET, Software Development Engineer in Test, Automation Engineer (testing-focused), Test Automation Engineer, QA Automation Engineer

### 9. Data Scientist
Data Scientist, Applied Scientist, Research Scientist (if ML/data-focused), ML Engineer, Machine Learning Engineer, AI Engineer, MLOps Engineer, Deep Learning Engineer, NLP Engineer, Computer Vision Engineer, Applied Data Scientist

### REJECT — NOT target roles:
- AI Architect, AI Product Owner, AI Consultant (strategy, not hands-on)
- Data Science Trainer, AI Trainer (teaching, not practitioner)
- Hardware Design, Embedded, Electrical, Mechanical Engineer
- SAP Consultant, ERP Consultant, OFSAA Developer
- Content Writer, Video Analyst, Graphic Designer, PowerPoint Specialist
- Network Engineer, Telecom Engineer
- Program Manager, Project Manager, Product Manager, Scrum Master
- Technical Support, Support Analyst, Helpdesk
- Data Annotation, Data Labeling, ML Data Associate, Content Annotator, LLM Annotator
- Internal IDs ("4605084-Assistant Manager") with no role context
- Supply Chain / Operations Manager / Business Operations

---

## SECTION C — LOCATION FILTER

**Include ONLY:**
- Bangalore / Bengaluru (KA, IN / Karnataka)
- Delhi / New Delhi / NCR / Noida / Gurugram / Gurgaon (DL, IN / HR, IN / UP, IN for Noida)
- Chennai (TN, IN / Tamil Nadu)
- Mumbai / Navi Mumbai / Pune (MH, IN / Maharashtra)
- Hyderabad (TS, IN / Telangana)
- Remote / WFH / Work from Home (India-based)
- PAN India / "India" / "IN"

**REJECT:** Ahmedabad (GJ), Jaipur (RJ), Kolkata (WB), Kochi, Coimbatore, Indore, Chandigarh (CH), Bhubaneswar (OR), Goa (GA), Lucknow, Nagpur, Nashik, Vadodara, Jammu, international locations

**AP, IN (Andhra Pradesh):** Usually NOT Hyderabad (Hyderabad = TS, IN). Exclude AP unless description explicitly says Hyderabad.
**MH, IN (Maharashtra):** Covers Mumbai + Pune — include. If description specifies Nagpur/Nashik, exclude.
**Location blank:** Check description for city clues. If none, mark relevant but note "Location unclear — verify"

---

## SECTION D — SPAM / JUNK

- NISH TECHNOLOGIES, SNESTRON, CodTech — mass spam
- "Apply for Referral" + "Multiple Positions" + "Salary Range 5 to 10 LPA" — referral farming
- "Comment #Interested" — engagement farming
- "Share your Resume with 1Lakh+ HR" — lead generation
- "Dear Hiring Team" in description — job seeker resume, not a job listing

---

## SECTION E — EDUCATION FILTER

**REJECT if description explicitly REQUIRES (not "preferred"):**
MBA / PGDM, PhD / Doctorate, CA / CFA / CPA, MD / MBBS

**INCLUDE:** B.Tech/B.E., "B.Tech/M.Tech" (either accepted), "MBA preferred but not required", no education mentioned

---

## EDGE CASES

1. **"Senior Associate" at EY/PwC/Deloitte:** Check description for analytics, BI tools, coding, dashboards. If yes, classify. If purely strategy/consulting, exclude.
2. **Amazon "Business Intelligence Engineer":** Data Analyst if dashboards/SQL/reporting focused. Data Engineer if pipelines/infra focused.
3. **"Data Analytics Engineer":** Analytics -> Data Analyst. Engineering -> Data Engineer. Include either way.
4. **Company name blank:** Check description and job_url for clues. Naukri URLs contain company in slug.
5. **"Full Stack + DevOps":** Mark `role_category: "Mixed"` and include.
6. **Title says "Software Engineer" but description is QA/Testing:** Classify as QA/SDET. Go by description, not title.
7. **"Intern" roles:** Include, tag P1.
8. **"Contract" roles:** Include — real jobs.
9. **10+ years experience:** Include, tag P4.
10. **Staffing detection from description:** If company looks real but description says "hiring for our client" -> REJECT. Exception: both firms named -> note actual employer.

---

## PRIORITY TAGGING

Use the `experience` field (if available) as the primary signal, then `job_type` (e.g. "internship" -> P1, "contract" -> include), then fall back to title keywords:
- **P1 — Fresher/Entry:** "intern", "junior", "trainee", "fresher", "entry level", "GET", "SDE-1", "Engineer I", "Software Engineer 1", "associate" (no "senior"), Experience 0-2 years, job_type is "internship", description says "freshers welcome", "campus hiring", "2024/2025/2026 batch"
- **P2 — Early Career (1-3 years):** "SDE-2", "SDE II", "Engineer II", Experience 1-3 years
- **P3 — Mid-Level (3-6 years):** Experience 3-6 years, or no data available
- **P4 — Senior (6+):** "senior", "sr.", "lead", "staff", "principal", "architect", "manager", "VP", "director", Experience 5+ years

---

## OUTPUT FORMAT

Respond ONLY with valid JSON. No preamble, no markdown backticks.

```json
[
  {"row": 1, "relevant": true, "reason": "Data Analyst at Uber, Hyderabad", "role_category": "Data Analyst", "priority": "P3"},
  {"row": 2, "relevant": false, "reason": "Scoutit is a staffing agency", "role_category": null, "priority": null},
  {"row": 3, "relevant": false, "reason": "Risk Analyst — domain-specific, not core DA", "role_category": null, "priority": null},
  {"row": 4, "relevant": false, "reason": "Location is Ahmedabad, outside target cities", "role_category": null, "priority": null},
  {"row": 5, "relevant": true, "reason": "Data Scientist at Swiggy, Bangalore", "role_category": "Data Scientist", "priority": "P4"},
  {"row": 6, "relevant": false, "reason": "description says hiring for unnamed client — staffing firm", "role_category": null, "priority": null},
  {"row": 7, "relevant": true, "reason": "SDET at Microsoft, Pune", "role_category": "SDET", "priority": "P2"}
]
```"""


def _get_sheets_writer_and_chunk_size() -> tuple[GoogleSheetsWriter, int]:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required for pipeline runs.")

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    sheet_chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
    return writer, sheet_chunk_size


def _write_scraped_jobs_to_google_sheets(
    run_date: str,
    scraped_jobs: list[dict[str, Any]],
) -> None:
    writer, sheet_chunk_size = _get_sheets_writer_and_chunk_size()
    scraped_tab = f"scraped_jobs_{run_date}"
    logger.info(
        "sheets write scraped spreadsheet_id=%s scraped_tab=%s scraped_count=%s chunk_size=%s",
        writer.spreadsheet_id,
        scraped_tab,
        len(scraped_jobs),
        sheet_chunk_size,
    )
    _retry(
        action=lambda: writer.write_rows(scraped_tab, scraped_jobs, chunk_size=sheet_chunk_size),
        retries=3,
        initial_delay_seconds=1.0,
    )
    logger.info(
        "sheets write scraped completed scraped_tab=%s rows=%s",
        scraped_tab,
        len(scraped_jobs),
    )


def _write_relevant_jobs_to_google_sheets(
    run_date: str,
    relevant_jobs: list[dict[str, Any]],
) -> None:
    writer, sheet_chunk_size = _get_sheets_writer_and_chunk_size()
    relevant_tab = f"relevant_jobs_{run_date}"
    logger.info(
        "sheets write relevant spreadsheet_id=%s relevant_tab=%s relevant_count=%s chunk_size=%s",
        writer.spreadsheet_id,
        relevant_tab,
        len(relevant_jobs),
        sheet_chunk_size,
    )
    _retry(
        action=lambda: writer.write_rows(relevant_tab, relevant_jobs, chunk_size=sheet_chunk_size),
        retries=3,
        initial_delay_seconds=1.0,
    )
    logger.info(
        "sheets write relevant completed relevant_tab=%s rows=%s",
        relevant_tab,
        len(relevant_jobs),
    )


def _post_recruiter_handover_notifications(run_date: str) -> int:
    """Recruiter LinkedIn profile + internal POC handovers (reads recruiters sheet)."""
    summary = send_handover_notifications(
        run_date,
        send_linkedin_post=False,
        send_recruiter_info=True,
        send_internal_poc=True,
    )
    sent = int(summary.get("recruiter_messages_sent", 0))
    logger.info(
        "handover slack posted recruiter messages=%s (detail_leads=%s internal_poc_leads=%s)",
        sent,
        summary.get("recruiter_detail_leads"),
        summary.get("internal_poc_leads"),
    )
    return sent


def _load_relevant_linkedin_posts_from_sheet(run_date: str) -> list[dict[str, Any]]:
    """Read linkedin_posts_relevant_{date} tab to get rows for Slack handover."""
    return load_linkedin_relevant_posts_from_sheet(run_date)


def _get_recruiter_handover_case_counts(run_date: str) -> tuple[int, int]:
    """Return (case3_recruiter_detail_count, case2_internal_poc_count) for recruiters_info rows."""
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        return 0, 0
    recruiters_tab = os.getenv("RECRUITERS_INFO_WORKSHEET") or f"recruiters_info_{run_date}"
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.sheet.worksheet(recruiters_tab)
        rows = worksheet_row_dicts(ws)
    except Exception as exc:
        logger.warning("handover summary counts skipped: recruiters tab unavailable: %s", exc)
        return 0, 0

    case3 = 0
    case2 = 0
    for row in rows:
        row_run_date = (row.get("run_date") or "").strip()
        if row_run_date and row_run_date != run_date:
            continue
        recruiter_profile_url = (row.get("recruiter_profile_url") or "").strip()
        recruiter_email = (row.get("recruiter_email") or "").strip()
        if recruiter_profile_url:
            case3 += 1
        elif recruiter_email:
            case2 += 1
    return case3, case2


def _post_daily_pipeline_final_summary(
    *,
    run_date: str,
    leads_scraped: int,
    relevant: int,
    recruiter_detail_available: int,
    internal_poc: int,
    linkedin_post_leads: int,
) -> None:
    handover_total = recruiter_detail_available + internal_poc + linkedin_post_leads
    text = (
        f"Data ({run_date}):\n"
        f"Leads Scraped: {leads_scraped}\n"
        f"Relevant: {relevant}\n"
        f"Handover: {handover_total}\n"
        f"Internal POC: {internal_poc}\n"
        f"Recruiter Detail available: {recruiter_detail_available}\n"
        f"Lead Type = Linkedin Post: {linkedin_post_leads}"
    )
    send_slack_text(text, sleep_after=0, log_skip_message=None)


def _pretty_platform_label(site_value: Any) -> str:
    site = str(site_value or "").strip().lower()
    mapping = {
        "linkedin": "LinkedIn",
        "indeed": "Indeed",
        "naukri": "Naukri",
    }
    if site in mapping:
        return mapping[site]
    return site_value if str(site_value or "").strip() else "-"


def get_pipeline_run_metrics(run_id: str) -> dict[str, Any] | None:
    return PIPELINE_RUN_METRICS.get(run_id)


def _retry(action, retries: int, initial_delay_seconds: float):
    delay = initial_delay_seconds
    last_error = None
    for attempt in range(retries):
        try:
            return action()
        except Exception as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            sleep(delay)
            delay *= 2
    raise RuntimeError(f"Operation failed after {retries} attempts: {last_error}") from last_error


def _chunk(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]
