import json
import logging
import os
import traceback
import uuid
from datetime import date
from time import perf_counter, sleep
from typing import Any

import requests

from services.apify_linkedin_posts import normalize_linkedin_post_item, scrape_linkedin_posts
from services.google_sheets import GoogleSheetsWriter

try:
    import google.generativeai as genai
except ImportError:  # pragma: no cover - optional dependency behavior
    genai = None

logger = logging.getLogger(__name__)
LINKEDIN_POSTS_RUN_METRICS: dict[str, dict[str, Any]] = {}


def run_linkedin_posts_pipeline(run_id: str | None = None) -> dict[str, Any]:
    """
    Scrape LinkedIn posts via Apify, run dedicated relevancy filter, and write sheets.
    """
    pipeline_run_id = run_id or str(uuid.uuid4())
    run_date = date.today().isoformat()
    started_at = perf_counter()
    LINKEDIN_POSTS_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": run_date,
    }

    try:
        actor_input = _build_actor_input()
        logger.info(
            "linkedin-posts pipeline[%s] started queries=%s max_posts=%s",
            pipeline_run_id,
            len(actor_input.get("searchQueries") or []),
            actor_input.get("maxPosts"),
        )
        raw_rows = scrape_linkedin_posts(actor_input)
        source_columns = _collect_source_columns(raw_rows)
        normalized = [normalize_linkedin_post_item(row) for row in raw_rows]

        relevant_rows, classification_errors = _classify_relevant_posts(normalized)
        _write_linkedin_posts_sheets(run_date=run_date, scraped_rows=normalized, relevant_rows=relevant_rows)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": run_date,
            "scraped_count": len(normalized),
            "relevant_count": len(relevant_rows),
            "classification_errors": classification_errors,
            "source_columns": source_columns,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        LINKEDIN_POSTS_RUN_METRICS[pipeline_run_id] = metrics
        logger.info(
            "linkedin-posts pipeline[%s] completed scraped=%s relevant=%s",
            pipeline_run_id,
            len(normalized),
            len(relevant_rows),
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
        LINKEDIN_POSTS_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("linkedin-posts pipeline[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_linkedin_posts_run_metrics(run_id: str) -> dict[str, Any] | None:
    return LINKEDIN_POSTS_RUN_METRICS.get(run_id)


def _build_actor_input() -> dict[str, Any]:
    queries = _parse_queries_from_env(
        os.getenv(
            "APIFY_LINKEDIN_POST_QUERIES",
            json.dumps(
                [
                    "hiring data analyst Bangalore",
                    "hiring data analyst Hyderabad",
                    "hiring software engineer Bangalore",
                    "hiring software engineer Hyderabad",
                    "hiring backend developer India",
                    "hiring data engineer Bangalore",
                    "hiring data engineer India",
                    "hiring DevOps engineer Bangalore",
                    "hiring DevOps engineer India",
                    "hiring QA engineer Bangalore",
                    "hiring SDET India",
                    "hiring SRE Bangalore",
                    "hiring platform engineer India",
                    "hiring freshers engineer India",
                    "hiring freshers data analyst India",
                ]
            ),
        )
    )
    if not queries:
        raise RuntimeError("APIFY_LINKEDIN_POST_QUERIES resolved to empty list.")

    return {
        "contentType": os.getenv("APIFY_LINKEDIN_POSTS_CONTENT_TYPE", "all"),
        # Prefer MAX_POSTS for simple config; keep old key for backward compatibility.
        "maxPosts": int(os.getenv("MAX_POSTS") or os.getenv("APIFY_LINKEDIN_POSTS_MAX_POSTS", "30")),
        "postNestedComments": os.getenv("APIFY_LINKEDIN_POSTS_NESTED_COMMENTS", "false").lower() == "true",
        "postNestedReactions": os.getenv("APIFY_LINKEDIN_POSTS_NESTED_REACTIONS", "false").lower() == "true",
        "postedLimit": os.getenv("APIFY_LINKEDIN_POSTS_POSTED_LIMIT", "24h"),
        "postedLimitDate": os.getenv("APIFY_LINKEDIN_POSTS_POSTED_LIMIT_DATE", ""),
        "scrapeComments": os.getenv("APIFY_LINKEDIN_POSTS_SCRAPE_COMMENTS", "false").lower() == "true",
        "scrapeReactions": os.getenv("APIFY_LINKEDIN_POSTS_SCRAPE_REACTIONS", "false").lower() == "true",
        "searchQueries": queries,
        "sortBy": os.getenv("APIFY_LINKEDIN_POSTS_SORT_BY", "date"),
    }


def _parse_queries_from_env(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    if raw.startswith("["):
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            raise RuntimeError("APIFY_LINKEDIN_POST_QUERIES must be JSON array or pipe-separated string.")
        return [str(x).strip() for x in parsed if str(x).strip()]
    return [x.strip() for x in raw.split("|") if x.strip()]


def _classify_relevant_posts(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
    ai_url = os.getenv("AI_CLASSIFIER_URL")
    ai_token = os.getenv("AI_CLASSIFIER_TOKEN")
    prompt_template = os.getenv("AI_RELEVANCE_PROMPT_LINKEDIN_POSTS", _default_linkedin_posts_prompt())

    relevant_rows: list[dict[str, Any]] = []
    errors = 0
    for row in rows:
        try:
            decision = _classify_single_post(
                row=row,
                gemini_api_key=gemini_api_key,
                gemini_model=gemini_model,
                ai_url=ai_url,
                ai_token=ai_token,
                prompt=prompt_template,
            )
        except Exception:
            errors += 1
            continue

        enriched = dict(row)
        enriched["is_relevant"] = bool(decision.get("is_relevant"))
        enriched["role_category"] = str(decision.get("role_category", ""))
        enriched["priority"] = str(decision.get("priority", ""))
        enriched["reason"] = str(decision.get("reason", ""))
        enriched["confidence"] = decision.get("confidence", 0)
        if enriched["is_relevant"]:
            relevant_rows.append(enriched)
    return relevant_rows, errors


def _classify_single_post(
    row: dict[str, Any],
    gemini_api_key: str | None,
    gemini_model: str,
    ai_url: str | None,
    ai_token: str | None,
    prompt: str,
) -> dict[str, Any]:
    if gemini_api_key:
        if genai is None:
            raise RuntimeError("google-generativeai package is not installed.")
        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel(gemini_model)
        payload = {
            "post_text": (row.get("post_text") or "")[:3000],
            "job_title_hint": row.get("job_title_hint"),
            "company": row.get("company"),
            "author_name": row.get("author_name"),
            "author_profile_url": row.get("author_profile_url"),
            "post_url": row.get("post_url"),
            "search_query": row.get("search_query"),
            "posted_at": row.get("posted_at"),
        }
        content = (
            f"{prompt}\n\n"
            "Return ONLY JSON with keys: relevant, reason, role_category, priority.\n"
            f"{json.dumps(payload, ensure_ascii=True)}"
        )
        response = _retry(action=lambda: model.generate_content(content), retries=3, initial_delay_seconds=1.0)
        return _normalize_classifier_decision(_parse_json_obj(getattr(response, "text", "") or ""))

    if ai_url:
        headers = {"Content-Type": "application/json"}
        if ai_token:
            headers["Authorization"] = f"Bearer {ai_token}"
        payload = {
            "prompt": prompt,
            "post": {
                "post_text": row.get("post_text"),
                "job_title_hint": row.get("job_title_hint"),
                "company": row.get("company"),
                "author_name": row.get("author_name"),
                "author_profile_url": row.get("author_profile_url"),
                "post_url": row.get("post_url"),
                "search_query": row.get("search_query"),
                "posted_at": row.get("posted_at"),
            },
        }
        response = _retry(
            action=lambda: requests.post(ai_url, json=payload, headers=headers, timeout=45),
            retries=3,
            initial_delay_seconds=1.0,
        )
        response.raise_for_status()
        return _normalize_classifier_decision(response.json())

    text = f"{row.get('search_query', '')} {row.get('post_text', '')}".lower()
    keep_tokens = ("hiring", "opening", "vacancy", "apply", "job", "engineer", "analyst", "developer", "sdet")
    is_relevant = any(token in text for token in keep_tokens)
    return {
        "is_relevant": is_relevant,
        "role_category": "",
        "priority": "",
        "reason": "Keyword fallback classifier used.",
        "confidence": 0.4 if is_relevant else 0.1,
    }


def _write_linkedin_posts_sheets(
    run_date: str,
    scraped_rows: list[dict[str, Any]],
    relevant_rows: list[dict[str, Any]],
) -> None:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required for LinkedIn posts pipeline.")

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
    scraped_tab = os.getenv("LINKEDIN_POSTS_SCRAPED_TAB_TEMPLATE", "linkedin_posts_scraped_{date}").format(date=run_date)
    relevant_tab = os.getenv("LINKEDIN_POSTS_RELEVANT_TAB_TEMPLATE", "linkedin_posts_relevant_{date}").format(date=run_date)

    writer.write_rows(scraped_tab, _with_run_date(scraped_rows, run_date), chunk_size=chunk_size)
    writer.write_rows(relevant_tab, _with_run_date(relevant_rows, run_date), chunk_size=chunk_size)


def _with_run_date(rows: list[dict[str, Any]], run_date: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        copy = dict(row)
        copy["run_date"] = run_date
        output.append(copy)
    return output


def _collect_source_columns(rows: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            ordered.append(str(key))
    return ordered


def _parse_json_obj(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("Classifier output must be a JSON object.")
    return parsed


def _normalize_classifier_decision(parsed: dict[str, Any]) -> dict[str, Any]:
    if "is_relevant" in parsed:
        return parsed
    if "relevant" in parsed:
        return {
            "is_relevant": bool(parsed.get("relevant")),
            "role_category": parsed.get("role_category", ""),
            "priority": parsed.get("priority", ""),
            "reason": parsed.get("reason", ""),
            "confidence": parsed.get("confidence", 0),
        }
    return parsed


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


def _default_linkedin_posts_prompt() -> str:
    return """You are a classifier for LinkedIn hiring posts.

Mark as relevant only when post content clearly indicates active hiring for technical roles in India,
preferably one of: Developer, Data Engineer, Data Analyst, DevOps, Platform Engineer, SRE, QA, SDET.

Exclude motivational posts, generic engagement posts, course ads, agency spam, and non-job posts.

Return strict JSON:
{
  "relevant": true or false,
  "reason": "short reason",
  "role_category": "Developer|Data Engineer|Data Analyst|DevOps|Platform Engineer|SRE|QA|SDET|Mixed|Unknown",
  "priority": "P1|P2|P3|P4|"
}
"""
