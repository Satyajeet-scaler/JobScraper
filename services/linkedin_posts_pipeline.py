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
from services.handover_owners import load_owner_rows_for_handover

try:
    import google.generativeai as genai
except ImportError:  # pragma: no cover - optional dependency behavior
    genai = None

logger = logging.getLogger(__name__)
LINKEDIN_POSTS_RUN_METRICS: dict[str, dict[str, Any]] = {}
_SLACK_TEXT_SOFT_LIMIT = 3500


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

        logger.info(
            "linkedin-posts pipeline[%s] classification started rows=%s",
            pipeline_run_id,
            len(normalized),
        )
        relevant_rows, classification_errors = _classify_relevant_posts(normalized)
        logger.info(
            "linkedin-posts pipeline[%s] classification ended relevant=%s errors=%s",
            pipeline_run_id,
            len(relevant_rows),
            classification_errors,
        )
        _write_linkedin_posts_sheets(run_date=run_date, scraped_rows=normalized, relevant_rows=relevant_rows)
        try:
            _post_linkedin_posts_slack_summary(
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
    model = genai.GenerativeModel(model_name)

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
        f"{prompt}\n\n"
        "Classify EACH row in the JSON array below.\n"
        "Return ONLY a JSON array with one object per row and keys: "
        "row, relevant, reason, role_category, priority.\n"
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


def _slack_display_field(value: Any, default: str = "-") -> str:
    """Slack text must be built from strings; Apify fields may be dict/list."""
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip() or default
    if isinstance(value, dict):
        for key in ("search", "query", "keyword", "text", "name", "title"):
            inner = value.get(key)
            if isinstance(inner, str) and inner.strip():
                return inner.strip()
        try:
            return json.dumps(value, ensure_ascii=True)[:500]
        except (TypeError, ValueError):
            return str(value)[:500]
    if isinstance(value, (list, tuple)):
        try:
            return json.dumps(value, ensure_ascii=True)[:500]
        except (TypeError, ValueError):
            return str(value)[:500]
    return str(value).strip() or default


def _deep_get(payload: Any, path: str) -> Any:
    cur = payload
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def _pick_first(payload: dict[str, Any], paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _deep_get(payload, path)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _slack_author_from_row(row: dict[str, Any]) -> str:
    direct = _slack_display_field(row.get("author_name"), default="")
    if direct:
        return direct
    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        return "-"
    return _slack_display_field(
        _pick_first(
            raw,
            (
                "author.name",
                "author.fullName",
                "authorName",
                "profileName",
                "name",
                "author.info",
            ),
        )
    )


def _slack_company_from_row(row: dict[str, Any]) -> str:
    direct = _slack_display_field(row.get("company"), default="")
    if direct:
        return direct
    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        return "-"
    return _slack_display_field(
        _pick_first(
            raw,
            (
                "companyName",
                "company.name",
                "author.company",
                "author.info",
                "organizationName",
            ),
        )
    )


def _slack_post_url_from_row(row: dict[str, Any]) -> str:
    direct = _slack_display_field(row.get("post_url"), default="")
    if direct:
        return direct
    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        return "-"
    return _slack_display_field(
        _pick_first(
            raw,
            (
                "linkedinUrl",
                "linkedinPostUrl",
                "postUrl",
                "url",
                "activityUrl",
                "author.linkedinUrl",
            ),
        )
    )


def _post_linkedin_posts_slack_summary(
    run_date: str,
    scraped_rows: list[dict[str, Any]],
    relevant_rows: list[dict[str, Any]],
) -> None:
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        logger.info("linkedin-posts slack skipped: SLACK_WEBHOOK_URL not configured")
        return

    # Keep LinkedIn-post notifications in the same channel as daily job handovers.
    slack_channel = os.getenv("SLACK_CHANNEL", "relevant-scraped-jobs")
    slack_username = os.getenv("SLACK_USERNAME", "Karan Bot")
    slack_icon_emoji = os.getenv("SLACK_ICON_EMOJI", ":karandeep:")

    logger.info("linkedin-posts slack handover scraped=%s relevant=%s", len(scraped_rows), len(relevant_rows))

    if not relevant_rows:
        logger.info("linkedin-posts slack: no relevant posts to send")
        return

    owner_rows = load_owner_rows_for_handover()
    if not owner_rows:
        logger.warning(
            "linkedin-posts handover: owner sheet unavailable; posting without owner assignment "
            "(set GOOGLE_SPREADSHEET_ID and owner_slack_ID tab)"
        )

    # 1) Send the heading message
    heading = ":rotating_light: *INCOMING LINKEDIN JOB POST VIA VALIDATED AUTHOR*"
    _retry(
        action=lambda: _post_slack_payload(
            webhook_url=slack_webhook_url,
            text=heading,
            channel=slack_channel,
            username=slack_username,
            icon_emoji=slack_icon_emoji,
        ),
        retries=3,
        initial_delay_seconds=1.0,
    ).raise_for_status()
    sleep(1)

    # 2) Send one message per post with round-robin owner assignment
    sent = 0
    for idx, row in enumerate(relevant_rows):
        author = _slack_author_from_row(row)
        url = _slack_post_url_from_row(row)

        if owner_rows:
            owner = owner_rows[idx % len(owner_rows)]
            owner_name = (owner.get("owner_name") or "Owner").strip() or "Owner"
            owner_slack_id = (owner.get("owner_slack_id") or "").strip()
            owner_tag = f"*{owner_name}* (<@{owner_slack_id}>)" if owner_slack_id else f"*{owner_name}*"
        else:
            owner_tag = "*Unassigned*"

        message = (
            f"{owner_tag}\n"
            f"{url}\n"
            f'This is lead posted by author "{author}"\n'
            "Note: Please consume the lead in next 2 hours and update"
        )
        _retry(
            action=lambda m=message: _post_slack_payload(
                webhook_url=slack_webhook_url,
                text=m,
                channel=slack_channel,
                username=slack_username,
                icon_emoji=slack_icon_emoji,
            ),
            retries=3,
            initial_delay_seconds=1.0,
        ).raise_for_status()
        sent += 1
        sleep(1)

    logger.info("linkedin-posts handover sent heading + %s individual post messages", sent)



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
