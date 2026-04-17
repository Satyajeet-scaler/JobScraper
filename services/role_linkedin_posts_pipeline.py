import json
import logging
import os
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any
from urllib.parse import urlparse

import gspread

from services.apify_linkedin_posts import normalize_linkedin_post_item, scrape_linkedin_posts
from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.linkedin_posts_pipeline import (
    _build_actor_input,
    _classify_relevant_posts,
    _dedupe_linkedin_relevant_rows,
)
from services.role_pipeline import _role_slug
from services.slack_handover_notify import (
    send_linkedin_post_handover_messages,
    slack_notify_defaults_from_env,
)

logger = logging.getLogger(__name__)

ROLE_LINKEDIN_POSTS_SCRAPE_RUN_METRICS: dict[str, dict[str, Any]] = {}
ROLE_LINKEDIN_POSTS_CLASSIFY_RUN_METRICS: dict[str, dict[str, Any]] = {}
ROLE_LINKEDIN_POSTS_NOTIFY_RUN_METRICS: dict[str, dict[str, Any]] = {}


def run_role_linkedin_posts_scrape_only(
    run_id: str | None = None,
    run_date: str | None = None,
    role: str | None = None,
    queries: list[str] | None = None,
) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    resolved_role = _validate_role(role)
    role_slug = _role_slug(resolved_role)
    started_at = perf_counter()
    ROLE_LINKEDIN_POSTS_SCRAPE_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
        "role": resolved_role,
        "role_slug": role_slug,
    }
    try:
        actor_input = _build_actor_input()
        actor_input = _apply_role_scrape_overrides(actor_input, resolved_role)
        actor_input["searchQueries"] = _resolve_role_queries(resolved_role, queries)
        raw_rows = scrape_linkedin_posts(actor_input)
        normalized = [normalize_linkedin_post_item(row) for row in raw_rows]
        enriched_rows = _enrich_role_context(normalized, resolved_role, role_slug)
        scraped_tab = _role_linkedin_scraped_tab_name(role_slug=role_slug, run_date=resolved_run_date)
        existing_scraped_rows = _read_rows_from_tab(scraped_tab, allow_missing=True)
        scrape_run_seq = _next_run_sequence(existing_scraped_rows, seq_field="role_linkedin_posts_run_seq")
        new_rows = _filter_new_rows_by_post_url(enriched_rows, existing_scraped_rows)
        new_rows_with_run = _attach_run_tracking(
            rows=new_rows,
            run_id=pipeline_run_id,
            run_seq=scrape_run_seq,
            run_id_field="role_linkedin_posts_run_id",
            run_seq_field="role_linkedin_posts_run_seq",
        )
        appended_count = _append_rows_to_tab(scraped_tab, new_rows_with_run)
        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "queries": actor_input["searchQueries"],
            "scraped_count": len(enriched_rows),
            "existing_scraped_count": len(existing_scraped_rows),
            "new_scraped_count": appended_count,
            "total_scraped_count_after_append": len(existing_scraped_rows) + appended_count,
            "scraped_run_seq": scrape_run_seq,
            "scraped_tab": scraped_tab,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_LINKEDIN_POSTS_SCRAPE_RUN_METRICS[pipeline_run_id] = metrics
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_LINKEDIN_POSTS_SCRAPE_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("role-linkedin-posts-scrape-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def run_role_linkedin_posts_classify_only(
    run_id: str | None = None,
    run_date: str | None = None,
    role: str | None = None,
    post_classify_notify_enabled: bool | None = None,
) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    resolved_role = _validate_role(role)
    role_slug = _role_slug(resolved_role)
    started_at = perf_counter()
    ROLE_LINKEDIN_POSTS_CLASSIFY_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
        "role": resolved_role,
        "role_slug": role_slug,
    }
    try:
        scraped_tab = _role_linkedin_scraped_tab_name(role_slug=role_slug, run_date=resolved_run_date)
        relevant_tab = _role_linkedin_relevant_tab_name(role_slug=role_slug, run_date=resolved_run_date)
        scraped_rows = _read_rows_from_tab(scraped_tab)
        deduped_scraped = _dedupe_rows_by_post_url(scraped_rows)
        existing_relevant_rows = _read_rows_from_tab(relevant_tab, allow_missing=True)
        relevant_run_seq = _next_run_sequence(existing_relevant_rows, seq_field="role_linkedin_posts_classify_run_seq")
        classify_input_rows = _filter_new_rows_by_post_url(deduped_scraped, existing_relevant_rows)
        if classify_input_rows:
            relevant_rows, classification_errors = _classify_relevant_posts_for_role_pipeline(
                classify_input_rows,
                role=resolved_role,
            )
        else:
            relevant_rows, classification_errors = [], 0
        relevant_rows = _enrich_role_context(relevant_rows, resolved_role, role_slug)
        relevant_rows = _dedupe_linkedin_relevant_rows(relevant_rows)
        relevant_new_rows = _filter_new_rows_by_post_url(relevant_rows, existing_relevant_rows)
        relevant_rows_with_run = _attach_run_tracking(
            rows=relevant_new_rows,
            run_id=pipeline_run_id,
            run_seq=relevant_run_seq,
            run_id_field="role_linkedin_posts_classify_run_id",
            run_seq_field="role_linkedin_posts_classify_run_seq",
        )
        appended_relevant_count = _append_rows_to_tab(relevant_tab, relevant_rows_with_run)
        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "scraped_input_count": len(scraped_rows),
            "deduped_input_count": len(deduped_scraped),
            "classify_input_count": len(classify_input_rows),
            "existing_relevant_count": len(existing_relevant_rows),
            "relevant_count": len(relevant_rows),
            "new_relevant_count": appended_relevant_count,
            "total_relevant_count_after_append": len(existing_relevant_rows) + appended_relevant_count,
            "classification_errors": classification_errors,
            "relevant_run_seq": relevant_run_seq,
            "source_scraped_tab": scraped_tab,
            "relevant_tab": relevant_tab,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        notify_enabled = (
            post_classify_notify_enabled
            if post_classify_notify_enabled is not None
            else os.getenv("ROLE_LINKEDIN_POSTS_POST_CLASSIFY_NOTIFY_ENABLED", "true").lower() in ("1", "true", "yes")
        )
        metrics["post_classify_notify_enabled"] = notify_enabled
        if notify_enabled:
            notify_summary = send_role_linkedin_posts_notifications(
                run_date=resolved_run_date,
                role=resolved_role,
                upstream_run_id=pipeline_run_id,
            )
            metrics["post_classify_notify_summary"] = notify_summary
        ROLE_LINKEDIN_POSTS_CLASSIFY_RUN_METRICS[pipeline_run_id] = metrics
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_LINKEDIN_POSTS_CLASSIFY_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("role-linkedin-posts-classify-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def send_role_linkedin_posts_notifications(
    run_date: str | None = None,
    *,
    role: str | None = None,
    upstream_run_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    notify_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    resolved_role = _validate_role(role)
    role_slug = _role_slug(resolved_role)
    started_at = perf_counter()
    ROLE_LINKEDIN_POSTS_NOTIFY_RUN_METRICS[notify_run_id] = {
        "run_id": notify_run_id,
        "status": "running",
        "run_date": resolved_run_date,
        "role": resolved_role,
        "role_slug": role_slug,
    }
    try:
        relevant_tab = _role_linkedin_relevant_tab_name(role_slug=role_slug, run_date=resolved_run_date)
        relevant_rows = _read_rows_from_tab(relevant_tab, allow_missing=True)
        filtered_rows = _filter_relevant_rows_for_notify(
            relevant_rows,
            run_date=resolved_run_date,
            upstream_run_id=upstream_run_id,
        )
        unsent_rows = _filter_rows_without_assigned_owner(filtered_rows)
        defaults = slack_notify_defaults_from_env()
        messages_sent = send_linkedin_post_handover_messages(
            unsent_rows,
            run_date=resolved_run_date,
            defaults=defaults,
            persist_assigned_owner_tab=relevant_tab,
        )
        summary = {
            "run_id": notify_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "upstream_run_id": upstream_run_id or "",
            "relevant_tab": relevant_tab,
            "input_relevant_count": len(relevant_rows),
            "notified_relevant_count": len(unsent_rows),
            "messages_sent": messages_sent,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_LINKEDIN_POSTS_NOTIFY_RUN_METRICS[notify_run_id] = summary
        return summary
    except Exception as exc:
        summary = {
            "run_id": notify_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_LINKEDIN_POSTS_NOTIFY_RUN_METRICS[notify_run_id] = summary
        logger.exception("role-linkedin-posts-notify[%s] failed: %s", notify_run_id, exc)
        raise


def get_role_linkedin_posts_scrape_run_metrics(run_id: str) -> dict[str, Any] | None:
    return ROLE_LINKEDIN_POSTS_SCRAPE_RUN_METRICS.get(run_id)


def get_role_linkedin_posts_classify_run_metrics(run_id: str) -> dict[str, Any] | None:
    return ROLE_LINKEDIN_POSTS_CLASSIFY_RUN_METRICS.get(run_id)


def get_role_linkedin_posts_notify_run_metrics(run_id: str) -> dict[str, Any] | None:
    return ROLE_LINKEDIN_POSTS_NOTIFY_RUN_METRICS.get(run_id)


def _validate_role(role: str | None) -> str:
    resolved = (role or "").strip()
    if not resolved:
        raise ValueError("role is required.")
    return resolved


def _role_linkedin_scraped_tab_name(*, role_slug: str, run_date: str) -> str:
    template = (
        os.getenv("ROLE_LINKEDIN_POSTS_SCRAPED_TAB_TEMPLATE")
        or "role_linkedin_posts_scraped_{role_slug}_{date}"
    ).strip()
    return template.format(role_slug=role_slug, date=run_date)


def _role_linkedin_relevant_tab_name(*, role_slug: str, run_date: str) -> str:
    template = (
        os.getenv("ROLE_LINKEDIN_POSTS_RELEVANT_TAB_TEMPLATE")
        or "role_linkedin_posts_relevant_{role_slug}_{date}"
    ).strip()
    return template.format(role_slug=role_slug, date=run_date)


def _resolve_role_queries(role: str, override_queries: list[str] | None) -> list[str]:
    """
    Resolution order (highest first):
      1. ``override_queries`` arg (e.g. from API call)
      2. ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON`` -> role -> ``scrape.queries``
      3. ``ROLE_LINKEDIN_POSTS_QUERY_TEMPLATE`` env (single template, all roles)
      4. Built-in default template.
    """
    if override_queries:
        cleaned = [q.strip() for q in override_queries if q and q.strip()]
        if cleaned:
            return cleaned

    role_cfg = _resolve_linkedin_role_config(role)
    scrape_cfg = role_cfg.get("scrape") if isinstance(role_cfg, dict) else None
    if isinstance(scrape_cfg, dict):
        raw_queries = scrape_cfg.get("queries")
        if isinstance(raw_queries, list):
            cleaned = [str(q).strip() for q in raw_queries if str(q or "").strip()]
            if cleaned:
                return cleaned

    raw_template = (
        os.getenv("ROLE_LINKEDIN_POSTS_QUERY_TEMPLATE")
        or "hiring {role} India|hiring {role} Bangalore|hiring {role} Hyderabad"
    )
    queries = [part.strip().format(role=role) for part in raw_template.split("|") if part.strip()]
    if not queries:
        raise RuntimeError("ROLE_LINKEDIN_POSTS_QUERY_TEMPLATE resolved to empty queries.")
    return queries


def _classify_relevant_posts_for_role_pipeline(
    rows: list[dict[str, Any]],
    role: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """
    Role pipeline wrapper around LinkedIn-post classifier prompt.

    Resolution order (highest first):
      1. ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON`` -> role -> ``ai_relevance_prompt``
      2. ``ROLE_LINKEDIN_POSTS_AI_RELEVANCE_PROMPT`` (global role-pipeline override)
      3. ``AI_RELEVANCE_PROMPT_LINKEDIN_POSTS`` / built-in (existing fallback)
    """
    role_prompt: str | None = None
    if role:
        try:
            role_cfg = _resolve_linkedin_role_config(role)
        except Exception:
            role_cfg = {}
        candidate = role_cfg.get("ai_relevance_prompt") if isinstance(role_cfg, dict) else None
        if isinstance(candidate, dict):
            candidate = candidate.get("prompt") or candidate.get("value")
        if isinstance(candidate, str) and candidate.strip():
            role_prompt = candidate

    if not role_prompt:
        role_prompt = os.getenv("ROLE_LINKEDIN_POSTS_AI_RELEVANCE_PROMPT")

    if not role_prompt:
        return _classify_relevant_posts(rows)

    previous_prompt = os.environ.get("AI_RELEVANCE_PROMPT_LINKEDIN_POSTS")
    try:
        os.environ["AI_RELEVANCE_PROMPT_LINKEDIN_POSTS"] = role_prompt
        return _classify_relevant_posts(rows)
    finally:
        if previous_prompt is None:
            os.environ.pop("AI_RELEVANCE_PROMPT_LINKEDIN_POSTS", None)
        else:
            os.environ["AI_RELEVANCE_PROMPT_LINKEDIN_POSTS"] = previous_prompt


# ---------------------------------------------------------------------------
# Per-role config (ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON)
# ---------------------------------------------------------------------------
#
# Single env var drives all per-role behaviour for the LinkedIn-posts role
# pipeline. Shape::
#
#     {
#       "data analyst": {
#         "scrape": {
#           "queries": ["hiring Data Analyst India", ...],
#           "max_posts": 30,
#           "posted_limit": "24h",
#           "posted_limit_date": "",
#           "content_type": "all",
#           "sort_by": "date",
#           "scrape_comments": false,
#           "scrape_reactions": false,
#           "post_nested_comments": false,
#           "post_nested_reactions": false
#         },
#         "ai_relevance_prompt": "..."
#       },
#       "software developer": { ... },
#       "devops": { ... }
#     }
#
# ``scrape`` keys map onto the Apify actor input fields used by
# ``services.linkedin_posts_pipeline._build_actor_input`` (snake_case here ->
# camelCase in the actor input). Any unset key falls back to the existing env
# var for that field.

_LINKEDIN_SCRAPE_KEY_MAP: dict[str, str] = {
    "queries": "searchQueries",
    "max_posts": "maxPosts",
    "posted_limit": "postedLimit",
    "posted_limit_date": "postedLimitDate",
    "content_type": "contentType",
    "sort_by": "sortBy",
    "scrape_comments": "scrapeComments",
    "scrape_reactions": "scrapeReactions",
    "post_nested_comments": "postNestedComments",
    "post_nested_reactions": "postNestedReactions",
}

_LINKEDIN_SCRAPE_INT_KEYS = {"max_posts"}
_LINKEDIN_SCRAPE_BOOL_KEYS = {
    "scrape_comments",
    "scrape_reactions",
    "post_nested_comments",
    "post_nested_reactions",
}


def _load_role_linkedin_config_map() -> dict[str, dict[str, Any]]:
    """Parse ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON`` into a per-role dict.

    Returns ``{}`` when the env var is unset, empty, malformed JSON, or not a
    JSON object. Role keys are normalised to lowercase + stripped.
    """
    raw = (os.getenv("ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("invalid ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON: %s", exc)
        return {}
    if not isinstance(parsed, dict):
        logger.warning("ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON must be a JSON object")
        return {}

    out: dict[str, dict[str, Any]] = {}
    for role_key, role_cfg in parsed.items():
        if not isinstance(role_key, str) or not isinstance(role_cfg, dict):
            continue
        out[role_key.strip().lower()] = role_cfg
    return out


def linkedin_posts_config_role_labels() -> list[str]:
    """Top-level role keys from ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON``, in order.

    Used by the role LinkedIn posts cron to decide which roles to scrape/classify
    when that env var is set. Each label is the JSON key after ``strip()`` (so
    casing matches what you wrote in the file). Empty list if unset, invalid
    JSON, or no valid entries.
    """
    raw = (os.getenv("ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON") or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, dict):
        return []
    out: list[str] = []
    for role_key, role_cfg in parsed.items():
        if not isinstance(role_key, str) or not isinstance(role_cfg, dict):
            continue
        label = role_key.strip()
        if label:
            out.append(label)
    return out


def _resolve_linkedin_role_config(role: str) -> dict[str, Any]:
    """Return the per-role config dict for *role* or ``{}`` if not configured."""
    key = (role or "").strip().lower()
    if not key:
        return {}
    return _load_role_linkedin_config_map().get(key, {})


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "on"):
            return True
        if lowered in ("0", "false", "no", "off"):
            return False
    return None


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _apply_role_scrape_overrides(actor_input: dict[str, Any], role: str) -> dict[str, Any]:
    """Overlay per-role ``scrape`` overrides on top of ``actor_input``.

    Non-destructive: keys absent from the role's ``scrape`` block are left
    untouched, so existing env-driven defaults still apply.
    """
    role_cfg = _resolve_linkedin_role_config(role)
    scrape_cfg = role_cfg.get("scrape") if isinstance(role_cfg, dict) else None
    if not isinstance(scrape_cfg, dict):
        return actor_input

    for snake_key, actor_key in _LINKEDIN_SCRAPE_KEY_MAP.items():
        if snake_key not in scrape_cfg:
            continue
        raw_value = scrape_cfg[snake_key]
        if snake_key == "queries":
            # queries handled by _resolve_role_queries; skip to avoid double work.
            continue
        if snake_key in _LINKEDIN_SCRAPE_BOOL_KEYS:
            coerced = _coerce_bool(raw_value)
            if coerced is None:
                logger.warning(
                    "ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON: ignoring non-bool %s for role=%s",
                    snake_key, role,
                )
                continue
            actor_input[actor_key] = coerced
        elif snake_key in _LINKEDIN_SCRAPE_INT_KEYS:
            coerced = _coerce_int(raw_value)
            if coerced is None:
                logger.warning(
                    "ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON: ignoring non-int %s for role=%s",
                    snake_key, role,
                )
                continue
            actor_input[actor_key] = coerced
        else:
            actor_input[actor_key] = raw_value
    return actor_input


def _enrich_role_context(rows: list[dict[str, Any]], role: str, role_slug: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        copy = dict(row)
        copy["requested_role"] = role
        copy["role_slug"] = role_slug
        output.append(copy)
    return output


def _filter_relevant_rows_for_notify(
    rows: list[dict[str, Any]],
    *,
    run_date: str,
    upstream_run_id: str | None,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        row_date = str(row.get("run_date") or "").strip()
        if row_date and row_date != run_date:
            continue
        if upstream_run_id:
            row_upstream = str(row.get("role_linkedin_posts_classify_run_id") or "").strip()
            if row_upstream != upstream_run_id:
                continue
        output.append(dict(row))
    return output


def _filter_rows_without_assigned_owner(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        assigned_owner = str(row.get("assigned owner") or row.get("assigned_owner") or "").strip()
        if assigned_owner:
            continue
        out.append(dict(row))
    return out


def _dedupe_rows_by_post_url(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        post_url = _normalized_post_url(row.get("post_url"))
        if not post_url:
            continue
        if post_url in seen:
            continue
        seen.add(post_url)
        out.append(row)
    return out


def _filter_new_rows_by_post_url(
    rows: list[dict[str, Any]],
    existing_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    for row in existing_rows:
        post_url = _normalized_post_url(row.get("post_url"))
        if post_url:
            seen.add(post_url)
    out: list[dict[str, Any]] = []
    for row in rows:
        post_url = _normalized_post_url(row.get("post_url"))
        if not post_url:
            continue
        if post_url in seen:
            continue
        seen.add(post_url)
        out.append(row)
    return out


def _attach_run_tracking(
    *,
    rows: list[dict[str, Any]],
    run_id: str,
    run_seq: int,
    run_id_field: str,
    run_seq_field: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        enriched["run_date"] = str(row.get("run_date") or date.today().isoformat())
        enriched[run_id_field] = run_id
        enriched[run_seq_field] = run_seq
        out.append(enriched)
    return out


def _normalized_post_url(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    netloc = parsed.netloc.lower().strip()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = (parsed.path or "").strip().rstrip("/")
    if not netloc and not path:
        return text.rstrip("/")
    return f"{netloc}{path}"


def _next_run_sequence(existing_rows: list[dict[str, Any]], *, seq_field: str) -> int:
    max_seen = 0
    for row in existing_rows:
        raw = str(row.get(seq_field) or "").strip()
        if not raw:
            continue
        try:
            value = int(float(raw))
        except ValueError:
            continue
        if value > max_seen:
            max_seen = value
    return max_seen + 1


def _read_rows_from_tab(tab_name: str, allow_missing: bool = False) -> list[dict[str, Any]]:
    writer = _get_writer()
    try:
        ws = writer.open_worksheet(tab_name)
    except gspread.WorksheetNotFound:
        if allow_missing:
            return []
        raise
    raw = writer.worksheet_get_all_values(ws, f"role_linkedin_posts:{tab_name}:get_all_values")
    rows = worksheet_row_dicts(raw)
    if not rows and not allow_missing:
        raise RuntimeError(f"No rows found in worksheet {tab_name}.")
    return [dict(row) for row in rows]


def _append_rows_to_tab(tab_name: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    writer = _get_writer()
    chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
    headers = _derive_headers(rows)
    values = [[_stringify_cell(row.get(col)) for col in headers] for row in rows]
    writer.append_to_worksheet(
        worksheet_title=tab_name,
        data_rows=values,
        header_row=headers,
        chunk_size=chunk_size,
    )
    return len(rows)


def _derive_headers(rows: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    headers: list[str] = []
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            headers.append(key)
    return headers


def _stringify_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        import json

        return json.dumps(value, ensure_ascii=True, default=str)
    return str(value)


def _get_writer() -> GoogleSheetsWriter:
    spreadsheet_id = (
        os.getenv("ROLE_LINKEDIN_POSTS_GOOGLE_SPREADSHEET_ID")
        or os.getenv("GOOGLE_SPREADSHEET_ID")
        or ""
    ).strip()
    if not spreadsheet_id:
        raise RuntimeError("Set ROLE_LINKEDIN_POSTS_GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_ID.")
    return GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
