import json
import logging
import os
import re
import traceback
import uuid
from datetime import date
from time import perf_counter, sleep
from typing import Any

import requests

from services.apify_linkedin_posts import normalize_linkedin_post_item, scrape_linkedin_posts
from services.google_sheets import GoogleSheetsWriter
from services.slack_handover_notify import send_linkedin_post_handover_messages, slack_notify_defaults_from_env

try:
    import google.generativeai as genai
except ImportError:  # pragma: no cover - optional dependency behavior
    genai = None

logger = logging.getLogger(__name__)
LINKEDIN_POSTS_RUN_METRICS: dict[str, dict[str, Any]] = {}
_SLACK_TEXT_SOFT_LIMIT = 3500


def run_linkedin_posts_pipeline(
    run_id: str | None = None,
    *,
    send_slack: bool = True,
    run_date: str | None = None,
) -> dict[str, Any]:
    """
    Scrape LinkedIn posts via Apify, run dedicated relevancy filter, and write sheets.

    When called from the daily pipeline with ``send_slack=False``, Slack messages are
    deferred so the caller can send them after all other case messages finish (avoids
    interleaving from parallel threads).
    """
    pipeline_run_id = run_id or str(uuid.uuid4())
    run_date = (run_date or date.today().isoformat()).strip()
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

        logger.info(
            "linkedin-posts pipeline[%s] classification started rows=%s",
            pipeline_run_id,
            len(normalized),
        )
        relevant_rows, classification_errors = _classify_relevant_posts(normalized)
        relevant_rows = _dedupe_linkedin_relevant_rows(relevant_rows)
        logger.info(
            "linkedin-posts pipeline[%s] classification ended relevant=%s errors=%s",
            pipeline_run_id,
            len(relevant_rows),
            classification_errors,
        )
        _write_linkedin_posts_sheets(run_date=run_date, scraped_rows=normalized, relevant_rows=relevant_rows)

        if send_slack:
            try:
                post_linkedin_posts_slack_handover(
                    run_date=run_date,
                    scraped_rows=normalized,
                    relevant_rows=relevant_rows,
                )
            except Exception as exc:
                logger.warning("linkedin-posts slack notification failed (sheets already written): %s", exc)

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
                    "hiring data scientist Bangalore",
                    "hiring data scientist India",
                    "hiring ML engineer India",
                    "hiring DevOps engineer Bangalore",
                    "hiring DevOps engineer India",
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
    mode = "gemini" if gemini_api_key else ("external_ai" if ai_url else "keyword_fallback")
    logger.info("linkedin-posts classification mode=%s rows=%s model=%s", mode, len(rows), gemini_model)

    relevant_rows: list[dict[str, Any]] = []
    errors = 0
    gemini_batch_size = max(1, int(os.getenv("LINKEDIN_POSTS_GEMINI_BATCH_SIZE", "60")))
    if gemini_api_key and gemini_batch_size > 1:
        batches = _chunk(rows, gemini_batch_size)
        logger.info(
            "linkedin-posts gemini batching enabled batches=%s batch_size=%s",
            len(batches),
            gemini_batch_size,
        )
        for bidx, batch in enumerate(batches, start=1):
            logger.info(
                "linkedin-posts classification batch=%s/%s started size=%s",
                bidx,
                len(batches),
                len(batch),
            )
            try:
                decisions = _classify_batch_posts_with_gemini(
                    rows=batch,
                    prompt=prompt_template,
                    api_key=gemini_api_key,
                    model_name=gemini_model,
                )
            except Exception as exc:
                errors += len(batch)
                logger.exception(
                    "linkedin-posts classification batch error batch=%s/%s size=%s err=%s",
                    bidx,
                    len(batches),
                    len(batch),
                    exc,
                )
                continue
            for row, decision in zip(batch, decisions):
                enriched = dict(row)
                enriched["is_relevant"] = bool(decision.get("is_relevant"))
                enriched["role_category"] = str(decision.get("role_category", ""))
                enriched["priority"] = str(decision.get("priority", ""))
                enriched["tier"] = str(decision.get("tier", ""))
                enriched["author_company"] = str(decision.get("author_company", ""))
                enriched["hiring_company"] = str(decision.get("hiring_company", ""))
                enriched["reason"] = str(decision.get("reason", ""))
                enriched["confidence"] = decision.get("confidence", 0)
                if enriched["is_relevant"]:
                    relevant_rows.append(enriched)
            logger.info(
                "linkedin-posts classification batch=%s/%s completed relevant_total=%s errors=%s",
                bidx,
                len(batches),
                len(relevant_rows),
                errors,
            )
        logger.info(
            "linkedin-posts classification completed mode=%s relevant=%s errors=%s",
            mode,
            len(relevant_rows),
            errors,
        )
        return relevant_rows, errors

    for idx, row in enumerate(rows, start=1):
        try:
            decision = _classify_single_post(
                row=row,
                gemini_api_key=gemini_api_key,
                gemini_model=gemini_model,
                ai_url=ai_url,
                ai_token=ai_token,
                prompt=prompt_template,
            )
        except Exception as exc:
            errors += 1
            if errors <= 5:
                logger.warning(
                    "linkedin-posts classification error row=%s/%s mode=%s post_id=%s err=%s",
                    idx,
                    len(rows),
                    mode,
                    row.get("post_id"),
                    exc,
                )
            continue

        enriched = dict(row)
        enriched["is_relevant"] = bool(decision.get("is_relevant"))
        enriched["role_category"] = str(decision.get("role_category", ""))
        enriched["priority"] = str(decision.get("priority", ""))
        enriched["tier"] = str(decision.get("tier", ""))
        enriched["author_company"] = str(decision.get("author_company", ""))
        enriched["hiring_company"] = str(decision.get("hiring_company", ""))
        enriched["reason"] = str(decision.get("reason", ""))
        enriched["confidence"] = decision.get("confidence", 0)
        if enriched["is_relevant"]:
            relevant_rows.append(enriched)
    logger.info(
        "linkedin-posts classification completed mode=%s relevant=%s errors=%s",
        mode,
        len(relevant_rows),
        errors,
    )
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
        model = genai.GenerativeModel(gemini_model, system_instruction=prompt)
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
            "Return ONLY JSON with keys: relevant, reason, tier, role_category, "
            "author_company, hiring_company (optional: priority if legacy).\n"
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

    text = f"{row.get('search_query', '')} {row.get('post_text', '')}"
    text_lower = text.lower()
    if re.search(r"\breferral\b", text_lower):
        return {
            "is_relevant": False,
            "role_category": "",
            "priority": "",
            "tier": "",
            "author_company": "",
            "hiring_company": "",
            "reason": "Keyword fallback: post contains referral (out of scope).",
            "confidence": 0.1,
        }
    keep_tokens = ("hiring", "opening", "vacancy", "apply", "job", "engineer", "analyst", "developer", "sdet")
    is_relevant = any(token in text_lower for token in keep_tokens)
    return {
        "is_relevant": is_relevant,
        "role_category": "",
        "priority": "",
        "tier": "",
        "author_company": "",
        "hiring_company": "",
        "reason": "Keyword fallback classifier used.",
        "confidence": 0.4 if is_relevant else 0.1,
    }


def _write_linkedin_posts_sheets(
    run_date: str,
    scraped_rows: list[dict[str, Any]],
    relevant_rows: list[dict[str, Any]],
) -> None:
    _write_linkedin_posts_scraped_only(run_date=run_date, scraped_rows=scraped_rows)
    _write_linkedin_posts_relevant_only(run_date=run_date, relevant_rows=relevant_rows)


def _write_linkedin_posts_scraped_only(
    run_date: str,
    scraped_rows: list[dict[str, Any]],
) -> None:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required for LinkedIn posts pipeline.")

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
    scraped_tab = os.getenv("LINKEDIN_POSTS_SCRAPED_TAB_TEMPLATE", "linkedin_posts_scraped_{date}").format(date=run_date)
    writer.write_rows(scraped_tab, _with_run_date(scraped_rows, run_date), chunk_size=chunk_size)


def _write_linkedin_posts_relevant_only(
    run_date: str,
    relevant_rows: list[dict[str, Any]],
) -> None:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required for LinkedIn posts pipeline.")

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
    relevant_tab = os.getenv("LINKEDIN_POSTS_RELEVANT_TAB_TEMPLATE", "linkedin_posts_relevant_{date}").format(date=run_date)
    deduped_relevant_rows = _dedupe_linkedin_relevant_rows(relevant_rows)
    writer.write_rows(relevant_tab, _with_run_date(deduped_relevant_rows, run_date), chunk_size=chunk_size)


def _with_run_date(rows: list[dict[str, Any]], run_date: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        copy = dict(row)
        copy["run_date"] = run_date
        output.append(copy)
    return output


def _dedupe_linkedin_relevant_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen_post_urls: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        post_url = str(row.get("post_url") or "").strip()
        if not post_url:
            deduped.append(row)
            continue

        if post_url in seen_post_urls:
            continue
        seen_post_urls.add(post_url)
        deduped.append(row)

    return deduped


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


def _parse_json_array(raw_text: str) -> list[Any]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("[")
        end = text.rfind("]")
        if start == -1 or end == -1 or end <= start:
            raise
        parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, list):
        raise ValueError("Classifier output must be a JSON array.")
    return parsed


def _normalize_classifier_decision(parsed: dict[str, Any]) -> dict[str, Any]:
    if "is_relevant" in parsed:
        return {
            "is_relevant": bool(parsed.get("is_relevant")),
            "role_category": str(parsed.get("role_category", "")),
            "priority": str(parsed.get("priority", "")),
            "tier": str(parsed.get("tier", "")),
            "author_company": str(parsed.get("author_company", "")),
            "hiring_company": str(parsed.get("hiring_company", "")),
            "reason": str(parsed.get("reason", "")),
            "confidence": parsed.get("confidence", 0),
        }
    if "relevant" in parsed:
        return {
            "is_relevant": bool(parsed.get("relevant")),
            "role_category": str(parsed.get("role_category", "")),
            "priority": str(parsed.get("priority", "")),
            "tier": str(parsed.get("tier", "")),
            "author_company": str(parsed.get("author_company", "")),
            "hiring_company": str(parsed.get("hiring_company", "")),
            "reason": str(parsed.get("reason", "")),
            "confidence": parsed.get("confidence", 0),
        }
    return parsed


def _classify_batch_posts_with_gemini(
    *,
    rows: list[dict[str, Any]],
    prompt: str,
    api_key: str,
    model_name: str,
) -> list[dict[str, Any]]:
    if genai is None:
        raise RuntimeError("google-generativeai package is not installed.")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name, system_instruction=prompt)

    compact_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        compact_rows.append(
            {
                "row": idx,
                "post_text": (row.get("post_text") or "")[:2500],
                "job_title_hint": row.get("job_title_hint"),
                "company": row.get("company"),
                "author_name": row.get("author_name"),
                "author_profile_url": row.get("author_profile_url"),
                "post_url": row.get("post_url"),
                "search_query": row.get("search_query"),
                "posted_at": row.get("posted_at"),
            }
        )

    content = (
        "Classify EACH row in the JSON array below.\n"
        "Return ONLY a JSON array with one object per row and keys: "
        "row, relevant, reason, tier, role_category, author_company, hiring_company "
        "(optional: priority for backward compatibility).\n"
        f"{json.dumps(compact_rows, ensure_ascii=True)}"
    )
    response = _retry(action=lambda: model.generate_content(content), retries=3, initial_delay_seconds=1.0)
    response_text = getattr(response, "text", "") or ""
    try:
        parsed = _parse_json_array(response_text)
    except Exception as exc:
        snippet = response_text[:1000].replace("\n", " ")
        logger.error(
            "linkedin-posts gemini batch parse error rows=%s model=%s err=%s response_snippet=%s",
            len(rows),
            model_name,
            exc,
            snippet,
        )
        raise
    by_row: dict[int, dict[str, Any]] = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        try:
            row_idx = int(item.get("row"))
        except (TypeError, ValueError):
            continue
        by_row[row_idx] = _normalize_classifier_decision(item)

    decisions: list[dict[str, Any]] = []
    for idx in range(1, len(rows) + 1):
        decisions.append(
            by_row.get(
                idx,
                {
                    "is_relevant": False,
                    "role_category": "",
                    "priority": "",
                    "tier": "",
                    "author_company": "",
                    "hiring_company": "",
                    "reason": "Missing row decision from batch classifier.",
                    "confidence": 0,
                },
            )
        )
    return decisions


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


def _default_linkedin_posts_prompt() -> str:
    return """# System Prompt: LinkedIn Hiring Post Classifier
# VERSION: Final (April 2026) — SRE/QA/SDET excluded; referral posts hard-rejected
# USE FOR: Scraped LinkedIn posts from harvestapi/linkedin-post-search Apify actor
# EXPECTED REJECTION RATE: 90-96%

You are a LinkedIn hiring post classifier for a placement team. Your job is to evaluate each LinkedIn post and determine if it is a high-value lead — meaning the placement team can reach the hiring company through this author.

Core question: Can the placement team reach the hiring company through this author?

---

## Your Task

For each post provided (content, author/name, author/info, author/type, author/linkedinUrl, linkedinUrl), respond ONLY with valid JSON:

{
  "relevant": true or false,
  "reason": "brief explanation",
  "tier": "T1 or T2",
  "role_category": "Developer | Data Engineer | Data Analyst | DevOps | Platform Engineer | Data Scientist | Mixed",
  "author_company": "extracted company name where author works",
  "hiring_company": "extracted company being hired for"
}

If relevant is false, set tier, role_category, author_company, hiring_company to null.

---

## EVALUATION ORDER (reject at first failure)

1) Is author from hiring company?
2) Is author page a real employer page (not job board/aggregator)?
3) Is role in target role list?
4) Is location in target set?
5) Referral / referral-farming (HARD REJECT — see Rule 5)
6) Is post spam/template/aggregator?
7) Education mismatch filter

---

## RULE 1 — AUTHOR MUST BE FROM HIRING COMPANY

Keep:
- Employee/leader/founder at company X posting hiring for company X (T1)
- Internal recruiter/TA/HR at company X posting hiring for company X (T2)
- Company page of the actual hiring company posting its own role (T1)

Reject:
- Reshares by people not employed at hiring company
- Third-party recruiters/staffing agencies posting for clients
- Influencers/content creators/job-alert pages/students/job-seekers

If author employer does not match hiring company, reject.

---

## RULE 2 — PAGE MUST BE ACTUAL EMPLOYER

Reject company/job-board pages that post other companies' jobs.
Reject regional showcase pages (e.g., "Tamil Nadu Jobs", etc.).
If page name != hiring company and acts as aggregator, reject.

---

## RULE 3 — TARGET ROLES (ONLY 6 CATEGORIES)

1) Developer
2) Data Engineer
3) Data Analyst (strict core analytics only)
4) DevOps
5) Platform Engineer
6) Data Scientist

### HARD OUT-OF-SCOPE (ALWAYS REJECT EVEN IF AUTHOR/COMPANY IS PERFECT)
- SRE / Site Reliability Engineer / Reliability Engineer / Production Engineer
- QA / Quality Assurance / Test Engineer / Manual Tester / Automation Tester / Performance Tester / Quality Analyst
- SDET / Software Development Engineer in Test / Test Automation Engineer / QA Automation Engineer

If post is only SRE/QA/SDET hiring, return relevant=false.
If post has mixed roles and only non-target in-scope role is absent, reject.
If post includes at least one target role AND other out-of-scope roles, keep only when clearly not bulk-aggregator and set role_category=Mixed.

---

## RULE 4 — TARGET CITIES

Include only:
Bangalore/Bengaluru, Delhi/NCR/Noida/Gurugram/Gurgaon, Chennai, Mumbai/Navi Mumbai/Pune, Hyderabad, Remote India/WFH, PAN India/India.

Reject non-target/international locations.

If location missing: keep only if all other rules strongly pass.

---

## RULE 5 — REFERRAL LEADS (HARD REJECT)

**Always reject** (relevant=false) if the post text contains the word **referral** as a whole word (case-insensitive), including:
- "Referral", "referral", "#referral", "DM for referral", "employee referral", "referral available", "looking for referral", referral-only hiring threads

This filters referral-farming leads; the placement team wants direct hiring posts, not referral solicitation.

Do not treat unrelated substrings that are not the word "referral" as a hit.

---

## RULE 6 — SPAM / TEMPLATE / AGGREGATOR

Reject:
- "Comment interested", WhatsApp/Telegram group pushes
- Bulk posts with many unrelated companies/roles
- Repetitive copy-paste template hiring spam
- Lead generation/job seeker posts

---

## RULE 7 — EDUCATION FILTER

Reject only when sole mandatory qualification is MBA/PhD/CA/CFA etc and role is otherwise not in target pipeline intent.

---

## TIER (for relevant posts only)

T1:
- Employee/leader/founder/company page at hiring company

T2:
- Internal recruiter/TA at hiring company

---

## OUTPUT FORMAT

Return ONLY valid JSON (no markdown, no prose):

[
  {"row": 1, "relevant": true, "reason": "Author is employee at hiring company posting company role", "tier": "T1", "role_category": "Developer", "author_company": "Nike", "hiring_company": "Nike"},
  {"row": 2, "relevant": false, "reason": "Author not from hiring company", "tier": null, "role_category": null, "author_company": null, "hiring_company": null},
  {"row": 3, "relevant": false, "reason": "SDET role is out of scope for this pipeline", "tier": null, "role_category": null, "author_company": null, "hiring_company": null},
  {"row": 4, "relevant": false, "reason": "Referral solicitation — post contains referral keyword", "tier": null, "role_category": null, "author_company": null, "hiring_company": null}
]
"""


def post_linkedin_posts_slack_handover(
    run_date: str,
    scraped_rows: list[dict[str, Any]],
    relevant_rows: list[dict[str, Any]],
) -> None:
    """Send LinkedIn posts handover to Slack, grouped by owner.

    Public so the daily pipeline can call it after all other case messages finish.
    """
    logger.info("linkedin-posts slack handover scraped=%s relevant=%s", len(scraped_rows), len(relevant_rows))
    send_linkedin_post_handover_messages(
        relevant_rows,
        run_date=run_date,
        defaults=slack_notify_defaults_from_env(),
    )



def _chunk_slack_entries(prefix: str, entries: list[str]) -> list[str]:
    """
    Split large handover payloads into Slack-safe chunks.
    """
    if not entries:
        return [prefix.rstrip()]
    chunks: list[str] = []
    current = prefix
    for entry in entries:
        candidate = f"{current}\n\n{entry}" if current.strip() != prefix.strip() else f"{current}{entry}"
        if len(candidate) <= _SLACK_TEXT_SOFT_LIMIT:
            current = candidate
            continue
        if current.strip():
            chunks.append(current)
        # If one entry itself is huge, hard-truncate rather than fail entire handover.
        if len(prefix) + len(entry) > _SLACK_TEXT_SOFT_LIMIT:
            allowed = max(200, _SLACK_TEXT_SOFT_LIMIT - len(prefix) - 20)
            entry = entry[:allowed] + "\n... (truncated)"
        current = f"{prefix}{entry}"
    if current.strip():
        chunks.append(current)
    return chunks
