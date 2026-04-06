import json
import logging
import math
import os
import uuid
import traceback
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import perf_counter, sleep
from typing import Any

import requests
from jobspy import scrape_jobs
from pandas import DataFrame, to_datetime

from services.google_sheets import GoogleSheetsWriter
from services.linkedin_recruiter.sheets_pipeline import write_linkedin_recruiters_for_relevant_jobs
from services.linkedin_posts_pipeline import run_linkedin_posts_pipeline
from services.apify_naukri import normalize_naukri_item, scrape_naukri_jobs
try:
    import google.generativeai as genai
except ImportError:  # pragma: no cover - optional dependency behavior
    genai = None

TARGET_ROLES = [
    "Developer",
    "Data Engineer",
    "Data Analyst",
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
    run_linkedin_posts_in_parallel = os.getenv("LINKEDIN_POSTS_PIPELINE_ENABLED", "false").lower() in ("1", "true", "yes")

    try:
        logger.info("pipeline[%s] started run_date=%s", pipeline_run_id, run_date)
        linkedin_posts_future = None
        with ThreadPoolExecutor(max_workers=1) as side_executor:
            if run_linkedin_posts_in_parallel:
                linkedin_posts_future = side_executor.submit(run_linkedin_posts_pipeline)
                logger.info("pipeline[%s] linkedin-posts side pipeline started in parallel", pipeline_run_id)

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

            recruiter_sheet_rows = 0
            linkedin_jobs_with_recruiter_profiles: frozenset[str] = frozenset()
            try:
                recruiter_sheet_rows, linkedin_jobs_with_recruiter_profiles = (
                    write_linkedin_recruiters_for_relevant_jobs(
                        run_date=run_date,
                        relevant_jobs=relevant,
                    )
                )
                logger.info(
                    "pipeline[%s] linkedin recruiters sheet completed rows_written=%s jobs_with_profiles=%s",
                    pipeline_run_id,
                    recruiter_sheet_rows,
                    len(linkedin_jobs_with_recruiter_profiles),
                )
            except Exception as exc:
                logger.warning(
                    "pipeline[%s] linkedin recruiters sheet failed but pipeline will continue: %s",
                    pipeline_run_id,
                    exc,
                )

            slack_jobs = _relevant_jobs_for_slack_with_recruiter_info(
                relevant,
                linkedin_job_urls_with_recruiters=linkedin_jobs_with_recruiter_profiles,
            )
            _post_slack_summary(
                run_date=run_date,
                scraped_jobs=deduped,
                relevant_jobs_total=len(relevant),
                slack_jobs=slack_jobs,
            )
            logger.info("pipeline[%s] slack post completed", pipeline_run_id)
            try:
                _update_company_match_sheet_with_ai(run_date=run_date, relevant_jobs=relevant)
                logger.info("pipeline[%s] company match sheet update completed", pipeline_run_id)
            except Exception as exc:
                logger.warning(
                    "pipeline[%s] company match step failed but pipeline will continue: %s",
                    pipeline_run_id,
                    exc,
                )

            if linkedin_posts_future is not None:
                try:
                    linkedin_posts_metrics = linkedin_posts_future.result()
                except Exception as exc:
                    linkedin_posts_error = str(exc)
                    logger.warning("pipeline[%s] linkedin-posts side pipeline failed: %s", pipeline_run_id, exc)

            metrics = {
                "run_id": pipeline_run_id,
                "status": "completed",
                "run_date": run_date,
                "scraped_count": len(scraped),
                "deduped_count": len(deduped),
                "relevant_count": len(relevant),
                "classification_errors": classifier_metrics["classification_errors"],
                "recruiter_sheet_rows": recruiter_sheet_rows,
                "linkedin_posts_parallel_enabled": run_linkedin_posts_in_parallel,
                "linkedin_posts_parallel_status": (
                    "completed"
                    if linkedin_posts_metrics and linkedin_posts_metrics.get("status") == "completed"
                    else ("failed" if linkedin_posts_error else "disabled")
                ),
                "linkedin_posts_parallel_scraped_count": (
                    linkedin_posts_metrics.get("scraped_count") if linkedin_posts_metrics else 0
                ),
                "linkedin_posts_parallel_relevant_count": (
                    linkedin_posts_metrics.get("relevant_count") if linkedin_posts_metrics else 0
                ),
                "linkedin_posts_parallel_error": linkedin_posts_error,
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
        "developer, data engineer, data analyst, devops engineer, platform engineer, sre, qa engineer, sdet,",
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
                linkedin_fetch_description=False,
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
    ai_max_workers = max(1, int(os.getenv("AI_CLASSIFIER_MAX_WORKERS", "6")))
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
        ai_max_workers,
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

        with ThreadPoolExecutor(max_workers=ai_max_workers) as executor:
            future_to_job = {
                executor.submit(
                    _classify_single_job,
                    job,
                    gemini_api_key,
                    gemini_model,
                    ai_url,
                    ai_token,
                    prompt_template,
                ): job
                for job in batch
            }
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                try:
                    decision = future.result()
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
    model = genai.GenerativeModel(model_name)
    # Limit large descriptions to keep token usage/cost bounded.
    description = (job.get("description") or "")[:4000]
    content = (
        f"{prompt}\n\n"
        "Return ONLY JSON with keys: relevant, reason, role_category, priority.\n"
        "Job payload:\n"
        f"{json.dumps({'title': job.get('title'), 'company': job.get('company'), 'location': job.get('location'), 'description': description, 'site': job.get('site'), 'job_url': job.get('job_url')}, ensure_ascii=True)}"
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
    model = genai.GenerativeModel(model_name)
    payload = [
        {
            "row": idx,
            "title": job.get("title"),
            "company": job.get("company"),
            "location": job.get("location"),
            "description": (job.get("description") or "")[:1500],
            "site": job.get("site"),
            "job_url": job.get("job_url"),
        }
        for idx, job in enumerate(jobs, start=1)
    ]
    content = (
        f"{prompt}\n\n"
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

You are a job listing classifier. Your job is to evaluate each job listing and determine if it is a genuine, relevant tech job opening from a real employer in India that a placement team should pursue for their learners (primarily B.Tech / B.E. graduates with 0-3 years experience).

## Your Task

For each job listing provided (containing fields like Company_Name, Title, Location, JD, Experience, job_url, platform), respond with:
{
  "relevant": true or false,
  "reason": "brief explanation",
  "role_category": "one of: Developer | Data Engineer | Data Analyst | DevOps | Platform Engineer | SRE | QA | SDET | Mixed",
  "priority": "P1 / P2 / P3 / P4"
}

## TARGET ROLES — Mark as relevant if the role falls into ANY of these categories:

### 1. Developer
Software Engineer, Software Developer, Backend Developer, Frontend Developer, Full Stack Developer, Fullstack Developer, Web Developer, Mobile Developer, iOS Developer, Android Developer, SDE, SDE-1, SDE-2, Python Developer, Java Developer, React Developer, Node Developer, .NET Developer, MERN Stack Developer, MEAN Stack Developer, PHP Developer, Golang Developer, Ruby Developer, Application Developer, Product Engineer

### 2. Data Engineer
Data Engineer, ETL Developer, Big Data Engineer, Spark Engineer, Databricks Engineer, Data Pipeline Engineer, Analytics Engineer (if engineering-focused), Data Platform Engineer

### 3. Data Analyst
Data Analyst, Business Analyst, Product Analyst, BI Analyst, Business Intelligence Analyst, Business Intelligence Engineer, Analytics Analyst, Reporting Analyst, MIS Analyst, Data Analytics Analyst, Research Analyst (data-focused), Data Associate (analytics-focused)

### 4. DevOps Engineer
DevOps Engineer, DevOps Specialist, CI/CD Engineer, Release Engineer, Build Engineer, Infrastructure Engineer (if DevOps-focused)

### 5. Platform Engineer
Platform Engineer, Cloud Engineer, Cloud Platform Engineer, Infrastructure Engineer (if platform/cloud-focused)

### 6. SRE (Site Reliability Engineer)
Site Reliability Engineer, SRE, Reliability Engineer, Production Engineer

### 7. QA (Quality Assurance)
QA Engineer, Quality Analyst, Quality Assurance Engineer, Test Engineer, Manual Tester, Automation Tester, Test Lead (if still hands-on), Performance Tester, QA Analyst

### 8. SDET (Software Development Engineer in Test)
SDET, Software Development Engineer in Test, Automation Engineer (testing-focused), Test Automation Engineer, QA Automation Engineer

## LOCATION FILTER — Only include jobs in these locations:

Include ONLY jobs based in:
- Bangalore / Bengaluru (including "KA, IN", "Karnataka")
- Delhi / New Delhi / NCR / Noida / Gurugram / Gurgaon (including "HR, IN", "UP, IN" for Noida, "DL, IN")
- Chennai (including "TN, IN", "Tamil Nadu")
- Mumbai / Navi Mumbai (including "MH, IN", "Maharashtra" — note: this also covers Pune)
- Pune (including "MH, IN", "Maharashtra")
- Hyderabad (including "TS, IN", "Telangana", "AP, IN" for Hyderabad-adjacent)
- Remote / Work from Home / WFH (if explicitly open to India-based candidates)
- PAN India
- India (generic, no specific city)

Exclude jobs based in:
- International locations (US, UK, Singapore, Dubai, etc.) unless explicitly open to India-based remote
- Tier 2/3 Indian cities not in the list above (Jaipur, Ahmedabad, Kochi, Coimbatore, Kolkata, Indore, Vadodara, Jammu, etc.)
- If location is blank/missing: Check the JD for location clues. If JD mentions one of the target cities, include. If no clues at all, mark relevant but note "Location unclear — verify"

## EDUCATION FILTER — Exclude jobs requiring advanced degrees:

Exclude if the JD explicitly REQUIRES (not just preferred or nice to have):
- MBA / PGDM as a mandatory qualification
- M.Tech / M.E. as a mandatory qualification (unless it says "B.Tech / M.Tech" meaning either is accepted)
- PhD / Doctorate as a mandatory qualification
- CA / CFA / CPA as a mandatory qualification (these are finance certifications, not tech)
- MD / MBBS or any medical degree

Include if:
- JD says "B.Tech / B.E." or "Bachelor's degree in CS/IT" — this is your target
- JD says "B.Tech / M.Tech" or "Bachelor's or Master's" — either is accepted, include
- JD says "MBA preferred but not required" — include (it's not mandatory)
- JD doesn't mention education — include (most tech roles assume B.Tech)

## EXCLUDE — Mark as relevant: false if ANY of these are true:

### A. Not a Target Role
- Finance Analyst / FP&A / CA roles (accounting, not tech)
- Security Analyst / Threat Analyst / SOC Analyst (infosec — not one of the 8 target roles)
- Fraud Analyst / Collections Analyst / Compliance Analyst / Risk & Underwriting Analyst (operations)
- IAM Governance Analyst (IT governance)
- Materials Data Analyst / Mechanical Engineer context (engineering, not tech)
- Data Annotation / Labeling / Tagging roles (data labeling, not analysis or engineering)
- ML Data Associate (data labeling for ML training)
- Content Annotator / LLM Annotator
- Program Manager / Project Manager / Product Manager / Scrum Master (management, not hands-on tech)
- Technical Support / Support Analyst / Helpdesk (support, not engineering)
- SAP Consultant / ERP Consultant (enterprise software consulting)
- Network Engineer / Telecom Engineer (networking, not in target roles)
- Embedded Engineer / Hardware Engineer (not software)
- Data Scientist / ML Engineer / AI Engineer (DS/ML roles — not in the 8 target categories unless the JD clearly describes a Data Analyst or Data Engineer role with a mismatched title)
- Assistant Manager / Lead Assistant Manager with internal IDs (e.g., "4605084-Assistant Manager") — generic corporate titles with no tech context
- Supply Chain / Operations Manager / Business Operations — operations, not tech
- Cost Basis Reporting Analyst / Static Data Analyst / Reference Data roles — data management, not analytics

### B. Not a Real Employer (middlemen, gig platforms, spam)
- Staffing aggregators: Scoutit, Haystack, or any company clearly a recruitment agency posting on behalf of an unnamed client.
- Gig / freelance platforms: Turing, Mindrift, Toptal, Upwork.
- Internship mills / tiny unknowns with no real company presence and generic unpaid/low stipend JD.
- Crowdsourcing platforms: Peroptyx, Appen, Lionbridge AI.
- Freelance roles in title.

### C. Spam / Junk Patterns
- NISH TECHNOLOGIES, SNESTRON, CodTech (mass spam posters)
- Referral farming templates and engagement farming templates
- Lead generation schemes and repeated regional duplicate pages

### D. Content Creator / Aggregator Posts
- Multi-company aggregator posts
- Reshared posts without hiring ownership
- Educational/motivational posts mentioning hiring
- Job seeker resume/self-posts
- Personal stories mentioning hiring in passing

### E. Wrong Location
- Jobs outside the target location list above

### F. Education Mismatch
- Jobs requiring MBA, PhD, M.Tech as sole qualification, CA, CFA, MD

## EDGE CASES — How to Handle:

1. Generic title "Analyst": if JD has SQL/Power BI/Tableau/Excel/dashboards/data visualization => Data Analyst; if testing => QA; if security/compliance => exclude.
2. Senior Associate at consulting firms: include only if JD has data analytics/BI/coding/dashboards/testing/DevOps.
3. Amazon "Business Intelligence Engineer": include as Data Analyst.
4. Data Analytics Engineer: analytics-focused => Data Analyst; engineering-focused => Data Engineer.
5. Company name blank: use JD/URL clues; do not auto-exclude.
6. EXL/Genpact/Wipro generic IDs like "4605084-Assistant Manager": exclude.
7. Full Stack + DevOps: classify as Mixed.
8. Title says Software Engineer but JD is QA/Testing: classify as QA/SDET.
9. SRE vs DevOps ambiguity: choose the more prominent one.
10. Intern roles: include and tag P1.
11. Contract/Contractual roles: include.
12. Roles requiring 10+ years: include but tag P4.
13. "MH, IN" covers Mumbai and Pune, but exclude other Maharashtra cities when explicitly mentioned (e.g., Nagpur, Nashik).

## PRIORITY TAGGING

Always include a priority field:
- P1 — Fresher/Entry-Level
- P2 — Early Career (1-3 years)
- P3 — Mid-Level (3-6 years)
- P4 — Senior (6+ years)

## OUTPUT FORMAT

For each listing, respond ONLY with valid JSON. No preamble, no markdown backticks, no text outside JSON.
Single example:
{"relevant": true, "reason": "SDE-1 at Amazon, Bangalore", "role_category": "Developer", "priority": "P1"}

Multiple example:
[
  {"row": 1, "relevant": true, "reason": "Data Analyst at Uber, Hyderabad", "role_category": "Data Analyst", "priority": "P3"},
  {"row": 2, "relevant": false, "reason": "Turing is a gig platform, not an employer", "role_category": null, "priority": null},
  {"row": 3, "relevant": false, "reason": "Location is Jaipur, outside target cities", "role_category": null, "priority": null}
]
"""


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


def _relevant_jobs_for_slack_with_recruiter_info(
    relevant_jobs: list[dict[str, Any]],
    *,
    linkedin_job_urls_with_recruiters: frozenset[str],
) -> list[dict[str, Any]]:
    """Per-job Slack digest: only classified rows whose job_url got ≥1 LinkedIn recruiter profile URL."""
    if not linkedin_job_urls_with_recruiters:
        return []
    out: list[dict[str, Any]] = []
    for job in relevant_jobs:
        url = (job.get("job_url") or "").strip()
        if url in linkedin_job_urls_with_recruiters:
            out.append(job)
    return out


def _post_slack_summary(
    run_date: str,
    scraped_jobs: list[dict[str, Any]],
    relevant_jobs_total: int,
    slack_jobs: list[dict[str, Any]],
) -> None:
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        logger.info("slack skipped: SLACK_WEBHOOK_URL not configured")
        return

    slack_channel = os.getenv("SLACK_CHANNEL", "relevant-scraped-jobs")
    slack_username = os.getenv("SLACK_USERNAME", "Karan Bot")
    slack_icon_emoji = os.getenv("SLACK_ICON_EMOJI", ":karandeep:")

    summary_body = (
        f"Daily jobs digest ({run_date})\n"
        f"- Scraped (deduped): {len(scraped_jobs)}\n"
        f"- Relevant (classified): {relevant_jobs_total}\n"
        f"- Slack job posts (LinkedIn + recruiter profile): {len(slack_jobs)}"
    )
    _retry(
        action=lambda: _post_slack_payload(
            webhook_url=slack_webhook_url,
            text=summary_body,
            channel=slack_channel,
            username=slack_username,
            icon_emoji=slack_icon_emoji,
        ),
        retries=3,
        initial_delay_seconds=1.0,
    ).raise_for_status()
    if relevant_jobs_total == 0:
        _retry(
            action=lambda: _post_slack_payload(
                webhook_url=slack_webhook_url,
                text="No relevant jobs today.",
                channel=slack_channel,
                username=slack_username,
                icon_emoji=slack_icon_emoji,
            ),
            retries=3,
            initial_delay_seconds=1.0,
        ).raise_for_status()
        logger.info("slack posted no relevant jobs message")
        return

    if not slack_jobs:
        _retry(
            action=lambda: _post_slack_payload(
                webhook_url=slack_webhook_url,
                text="No LinkedIn jobs with recruiter profile URLs in this digest.",
                channel=slack_channel,
                username=slack_username,
                icon_emoji=slack_icon_emoji,
            ),
            retries=3,
            initial_delay_seconds=1.0,
        ).raise_for_status()
        logger.info("slack posted no recruiter-profile jobs message")
        return

    sent_messages = 0
    for job in slack_jobs:
        company = (job.get("company") or "-").strip() or "-"
        title = (job.get("title") or "-").strip() or "-"
        location = (job.get("location") or "-").strip() or "-"
        platform = _pretty_platform_label(job.get("site"))
        posted_date = str(job.get("date_posted") or "-")
        url = (job.get("job_url") or "-").strip() or "-"

        message = (
            f"Company Name: {company}\n"
            f"Title: {title}\n"
            f"Location: {location}\n"
            f"Platform: {platform}\n"
            f"Posted Date: {posted_date}\n"
            f"URL : {url}"
        )
        _retry(
            action=lambda: _post_slack_payload(
                webhook_url=slack_webhook_url,
                text=message,
                channel=slack_channel,
                username=slack_username,
                icon_emoji=slack_icon_emoji,
            ),
            retries=3,
            initial_delay_seconds=1.0,
        ).raise_for_status()
        sent_messages += 1
        sleep(1)
    logger.info(
        "slack posted per-job messages=%s slack_jobs_with_recruiter_info=%s",
        sent_messages,
        len(slack_jobs),
    )


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


def _update_company_match_sheet_with_ai(
    run_date: str,
    relevant_jobs: list[dict[str, Any]],
) -> None:
    """
    Post-slack step:
    - Read target sheet with column `company`
    - AI-match target company -> relevant_jobs_{date} company
    - Write matched relevant sheet row number(s) into the column right next to `company`
    """
    company_match_enabled = os.getenv("COMPANY_MATCH_ENABLED", "true").lower() == "true"

    started = perf_counter()
    max_seconds = max(10, int(os.getenv("COMPANY_MATCH_MAX_SECONDS", "300")))

    if not relevant_jobs:
        logger.info("company match skipped: no relevant jobs")
        return

    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        logger.info("company match skipped: GEMINI_API_KEY not set")
        return
    if genai is None:
        logger.info("company match skipped: google-generativeai package unavailable")
        return

    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.info("company match skipped: GOOGLE_SPREADSHEET_ID not set")
        return

    target_tab = os.getenv("COMPANY_MATCH_SHEET_NAME")
    if not target_tab:
        logger.info("company match skipped: COMPANY_MATCH_SHEET_NAME not set")
        return

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    try:
        worksheet = writer.sheet.worksheet(target_tab)
    except Exception as exc:
        logger.warning("company match skipped: target tab not found tab=%s err=%s", target_tab, exc)
        return

    headers = worksheet.row_values(1)
    if not headers:
        logger.warning("company match skipped: target tab has no header row")
        return

    header_map = {h.strip().lower(): i + 1 for i, h in enumerate(headers)}
    company_col = header_map.get("company")
    if not company_col:
        logger.warning("company match skipped: required `company` column not found")
        return
    target_write_col = company_col + 1
    if target_write_col > len(headers):
        target_col_letter = GoogleSheetsWriter._column_letter(target_write_col)
        worksheet.update(f"{target_col_letter}1", [["matched_relevant_rows"]])
        headers.append("matched_relevant_rows")
        logger.info(
            "company match created output column next to `company` at %s1",
            target_col_letter,
        )

    rows = worksheet.get_all_values()
    if len(rows) <= 1:
        logger.info("company match skipped: target tab has no data rows")
        return

    relevant_companies = []
    for idx, job in enumerate(relevant_jobs, start=2):  # relevant sheet row index starts at 2
        company = (job.get("company") or "").strip()
        if company:
            relevant_companies.append({"relevant_row": idx, "company": company})
    if not relevant_companies:
        logger.info("company match skipped: relevant jobs missing company values")
        return

    target_rows = []
    for row_idx in range(2, len(rows) + 1):
        row_vals = rows[row_idx - 1]
        company = row_vals[company_col - 1].strip() if len(row_vals) >= company_col else ""
        if company:
            target_rows.append({"sheet_row": row_idx, "company": company})

    if not target_rows:
        logger.info("company match skipped: no company rows in target tab")
        return

    matches: dict[int, str] = {}
    if not company_match_enabled:
        logger.info("company match running in string mode (COMPANY_MATCH_ENABLED=false)")
        matches = _match_companies_by_string(
            target_rows=target_rows,
            relevant_companies=relevant_companies,
        )
    else:
        model_name = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        batch_size = max(1, int(os.getenv("COMPANY_MATCH_BATCH_SIZE", "40")))

        batches = _chunk(target_rows, batch_size)
        logger.info(
            "company match started target_rows=%s relevant_rows=%s batches=%s batch_size=%s max_seconds=%s",
            len(target_rows),
            len(relevant_companies),
            len(batches),
            batch_size,
            max_seconds,
        )

        for idx, batch in enumerate(batches, start=1):
            elapsed = perf_counter() - started
            if elapsed > max_seconds:
                logger.warning(
                    "company match timeout reached after %.2fs at batch %s/%s; stopping early",
                    elapsed,
                    idx,
                    len(batches),
                )
                break

            logger.info("company match batch=%s/%s size=%s", idx, len(batches), len(batch))
            batch_matches = _match_companies_with_ai_batch(
                batch=batch,
                relevant_companies=relevant_companies,
                api_key=gemini_api_key,
                model_name=model_name,
            )
            matches.update(batch_matches)
            logger.info("company match batch=%s/%s completed matched_rows_so_far=%s", idx, len(batches), len(matches))

    if not matches:
        logger.info("company match completed: no matches found")
        return

    target_col_letter = GoogleSheetsWriter._column_letter(target_write_col)
    updates = []
    for sheet_row, relevant_row in matches.items():
        updates.append({"range": f"{target_col_letter}{sheet_row}", "values": [[relevant_row]]})
    worksheet.batch_update(updates)
    logger.info(
        "company match updated rows=%s tab=%s target_col=%s",
        len(updates),
        target_tab,
        target_col_letter,
    )


def _match_companies_with_ai_batch(
    batch: list[dict[str, Any]],
    relevant_companies: list[dict[str, Any]],
    api_key: str,
    model_name: str,
) -> dict[int, str]:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)
    content = (
        "Match companies between two lists using similarity.\n"
        "Input A: relevant sheet rows with keys {relevant_row, company}.\n"
        "Input B: target sheet rows with keys {sheet_row, company}.\n"
        "Return ONLY a JSON array of matches.\n"
        "Each item must be one of:\n"
        "- {sheet_row: number, matched_relevant_rows: [number, ...]}\n"
        "- {sheet_row: number, matched_relevant_row: number|null}\n"
        "Return all good matches for each sheet_row. Use empty array or null when no good match.\n"
        "STRICT OUTPUT RULES:\n"
        "- No markdown\n"
        "- No explanation text\n"
        "- No code fences\n"
        "- No trailing text before/after JSON\n"
        "- Output must start with '[' and end with ']'\n\n"
        f"RelevantRows={json.dumps(relevant_companies, ensure_ascii=True)}\n"
        f"TargetRows={json.dumps(batch, ensure_ascii=True)}"
    )
    response = _retry(
        action=lambda: model.generate_content(content),
        retries=3,
        initial_delay_seconds=1.0,
    )
    raw_text = getattr(response, "text", "") or ""
    try:
        parsed = _parse_classifier_json(raw_text)
    except Exception:
        # Fallback: some models may emit one JSON object per line.
        parsed = _parse_json_objects_from_lines(raw_text)
    if not isinstance(parsed, list):
        return {}

    matches: dict[int, str] = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        sheet_row = item.get("sheet_row")
        if not isinstance(sheet_row, int):
            continue

        matched_rows = item.get("matched_relevant_rows")
        if isinstance(matched_rows, list):
            cleaned = [str(r) for r in matched_rows if isinstance(r, int)]
            if cleaned:
                matches[sheet_row] = ",".join(cleaned)
            continue

        matched_row = item.get("matched_relevant_row")
        if isinstance(matched_row, int):
            matches[sheet_row] = str(matched_row)
    return matches


def _match_companies_by_string(
    target_rows: list[dict[str, Any]],
    relevant_companies: list[dict[str, Any]],
) -> dict[int, str]:
    by_company: dict[str, list[int]] = {}
    for item in relevant_companies:
        normalized = _normalize_company_name(item.get("company"))
        if not normalized:
            continue
        by_company.setdefault(normalized, []).append(int(item["relevant_row"]))

    matches: dict[int, str] = {}
    for target in target_rows:
        normalized = _normalize_company_name(target.get("company"))
        if not normalized:
            continue
        candidate_rows = by_company.get(normalized, [])
        if candidate_rows:
            matches[int(target["sheet_row"])] = ",".join(str(x) for x in candidate_rows)
    logger.info("company match string mode completed matched_rows=%s", len(matches))
    return matches


def _normalize_company_name(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    if not text:
        return ""
    return " ".join(text.split())


def _parse_json_objects_from_lines(raw_text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line in raw_text.splitlines():
        candidate = line.strip().strip(",")
        if not candidate:
            continue
        if not candidate.startswith("{") or not candidate.endswith("}"):
            continue
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            items.append(parsed)
    return items


def _post_slack_payload(
    webhook_url: str,
    text: str,
    channel: str,
    username: str,
    icon_emoji: str,
) -> requests.Response:
    payload = {
        "text": text,
        "channel": channel,
        "username": username,
        "icon_emoji": icon_emoji,
    }
    return requests.post(
        webhook_url,
        data={"payload": json.dumps(payload, ensure_ascii=True)},
        timeout=20,
    )


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
