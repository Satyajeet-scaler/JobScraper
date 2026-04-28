import json
import logging
import os
import traceback
import uuid
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import urlparse

from services.apify_linkedin_posts import normalize_linkedin_post_item, scrape_linkedin_posts
from services.linkedin_posts_pipeline import (
    _build_actor_input,
    _classify_relevant_posts,
    _dedupe_linkedin_relevant_rows,
)
from services.mysql_linkedin_posts_store import (
    existing_linkedin_post_url_normalized_set,
    fetch_unclassified_linkedin_posts,
    fetch_unsent_relevant_linkedin_posts_for_role,
    mark_linkedin_post_handover_sent,
    mark_linkedin_posts_classify_done,
    upsert_linkedin_post,
    upsert_linkedin_post_relevance,
)
from services.role_pipeline import _role_slug
from services.slack_handover_notify import (
    format_linkedin_post_lead,
    heading_for_case,
    load_owner_rows_for_handover,
    owner_tag_for_handover,
    send_slack_text,
    slack_notify_defaults_from_env,
    HandoverSlackCase,
)

logger = logging.getLogger(__name__)

ROLE_LINKEDIN_POSTS_SCRAPE_RUN_METRICS: dict[str, dict[str, Any]] = {}
ROLE_LINKEDIN_POSTS_CLASSIFY_RUN_METRICS: dict[str, dict[str, Any]] = {}
ROLE_LINKEDIN_POSTS_NOTIFY_RUN_METRICS: dict[str, dict[str, Any]] = {}

_linkedin_posts_role_config_file_cache: dict[str, Any] | None = None
_linkedin_posts_role_config_file_cache_valid: bool = False
_linkedin_posts_ai_prompts_file_cache: dict[str, str] | None = None
_linkedin_posts_ai_prompts_file_cache_valid: bool = False


def invalidate_role_linkedin_posts_role_config_cache() -> None:
    """Clear cached file-backed LinkedIn-posts role config and optional AI prompts map."""
    global _linkedin_posts_role_config_file_cache, _linkedin_posts_role_config_file_cache_valid
    global _linkedin_posts_ai_prompts_file_cache, _linkedin_posts_ai_prompts_file_cache_valid
    _linkedin_posts_role_config_file_cache = None
    _linkedin_posts_role_config_file_cache_valid = False
    _linkedin_posts_ai_prompts_file_cache = None
    _linkedin_posts_ai_prompts_file_cache_valid = False


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

        run_d = date.fromisoformat(resolved_run_date)
        try:
            existing_mysql_urls = existing_linkedin_post_url_normalized_set(
                requested_role=resolved_role,
                run_date=run_d,
            )
        except Exception as exc:
            logger.warning("role-linkedin-posts: could not load MySQL URL set for dedupe: %s", exc)
            existing_mysql_urls = set()

        new_rows = _filter_new_rows_against_normalized_url_set(enriched_rows, existing_mysql_urls)
        scrape_run_seq = 1  # MySQL dedupe makes run_seq less meaningful; keep for metrics compat
        new_rows_with_run = _attach_run_tracking(
            rows=new_rows,
            run_id=pipeline_run_id,
            run_seq=scrape_run_seq,
            run_id_field="role_linkedin_posts_run_id",
            run_seq_field="role_linkedin_posts_run_seq",
        )

        mysql_count = 0
        for row in new_rows_with_run:
            try:
                upsert_linkedin_post(row)
                mysql_count += 1
            except Exception as exc:
                logger.warning(
                    "role-linkedin-posts-scrape-only[%s] mysql upsert failed url=%s err=%s",
                    pipeline_run_id,
                    row.get("post_url"),
                    exc,
                )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "queries": actor_input["searchQueries"],
            "scraped_count": len(enriched_rows),
            "existing_mysql_url_count": len(existing_mysql_urls),
            "new_scraped_count": len(new_rows_with_run),
            "mysql_upserted_count": mysql_count,
            "scraped_run_seq": scrape_run_seq,
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
        batch_size = max(1, int(os.getenv("ROLE_LINKEDIN_POSTS_CLASSIFY_BATCH_SIZE", "30")))
        run_date_obj = date.fromisoformat(resolved_run_date)

        total_unclassified = count_unclassified_linkedin_posts(
            requested_role=resolved_role,
            run_date=run_date_obj,
        )
        estimated_batches = (total_unclassified + batch_size - 1) // batch_size
        logger.info(
            "role-linkedin-posts-classify-only[%s] starting classify for role=%s, run_date=%s, total_unclassified=%d, estimated_batches=%d",
            pipeline_run_id,
            resolved_role,
            resolved_run_date,
            total_unclassified,
            estimated_batches,
        )

        total_classified = 0
        total_relevant = 0
        total_errors = 0
        total_mysql_relevance = 0
        batch_seq = 0

        while True:
            classify_input_rows = fetch_unclassified_linkedin_posts(
                requested_role=resolved_role,
                run_date=run_date_obj,
                limit=batch_size,
            )
            if not classify_input_rows:
                break

            batch_seq += 1

            relevant_rows, classification_errors = _classify_relevant_posts_for_role_pipeline(
                classify_input_rows,
                role=resolved_role,
            )
            total_errors += classification_errors
            relevant_rows = _enrich_role_context(relevant_rows, resolved_role, role_slug)
            relevant_rows = _dedupe_linkedin_relevant_rows(relevant_rows)

            rel_count = 0
            for row in relevant_rows:
                row["classify_run_id"] = pipeline_run_id
                row["classify_run_seq"] = batch_seq
                try:
                    upsert_linkedin_post_relevance(row)
                    rel_count += 1
                except Exception as exc:
                    logger.warning(
                        "role-linkedin-posts-classify-only[%s] mysql relevance upsert failed url=%s err=%s",
                        pipeline_run_id,
                        row.get("post_url"),
                        exc,
                    )

            post_ids = [int(r.get("id") or 0) for r in classify_input_rows if int(r.get("id") or 0) > 0]
            if post_ids:
                mark_linkedin_posts_classify_done(post_ids=post_ids)

            total_classified += len(classify_input_rows)
            total_relevant += len(relevant_rows)
            total_mysql_relevance += rel_count

            logger.info(
                "role-linkedin-posts-classify-only[%s] batch=%d/%d classified=%d, relevance_saved=%d, relevant_found=%d",
                pipeline_run_id,
                batch_seq,
                estimated_batches,
                len(classify_input_rows),
                rel_count,
                len(relevant_rows),
            )

        logger.info(
            "role-linkedin-posts-classify-only[%s] classify complete: total_classified=%d, total_relevant=%d, total_errors=%d",
            pipeline_run_id,
            total_classified,
            total_relevant,
            total_errors,
        )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "classify_batches": batch_seq,
            "classify_input_count": total_classified,
            "relevant_count": total_relevant,
            "mysql_relevance_upserted_count": total_mysql_relevance,
            "classification_errors": total_errors,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
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
        relevant_rows = fetch_unsent_relevant_linkedin_posts_for_role(
            role=resolved_role,
            run_date=resolved_run_date,
            upstream_run_id=upstream_run_id,
        )
        defaults = slack_notify_defaults_from_env()
        messages_sent, notified_rows = _send_linkedin_post_handover_messages_from_db(
            relevant_rows,
            defaults=defaults,
        )
        summary = {
            "run_id": notify_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "upstream_run_id": upstream_run_id or "",
            "input_relevant_count": len(relevant_rows),
            "notified_relevant_count": notified_rows,
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


def _send_linkedin_post_handover_messages_from_db(
    relevant_rows: list[dict[str, Any]],
    *,
    defaults: Any,
) -> tuple[int, int]:
    """Send LinkedIn handover from DB rows and mark DB handover_sent on success."""
    if not defaults.webhook_url:
        logger.info("linkedin-posts slack skipped: SLACK_WEBHOOK_URL not configured")
        return 0, 0
    if not relevant_rows:
        logger.info("linkedin-posts slack: no relevant posts to send")
        return 0, 0

    sent = 0
    notified_rows = 0
    if not send_slack_text(heading_for_case(HandoverSlackCase.LINKEDIN_POST), defaults=defaults, sleep_after=1.0):
        return 0, 0
    sent += 1

    owner_rows = load_owner_rows_for_handover()
    if owner_rows:
        owner_buckets: dict[int, list[dict[str, Any]]] = {i: [] for i in range(len(owner_rows))}
        for idx, row in enumerate(relevant_rows):
            owner_buckets[idx % len(owner_rows)].append(row)
        for owner_idx, owner in enumerate(owner_rows):
            bucket = owner_buckets.get(owner_idx, [])
            if not bucket:
                continue
            owner_tag = owner_tag_for_handover(owner)
            owner_name = (owner.get("owner_name") or "").strip()
            for row in bucket:
                post_id = int(row.get("linkedin_post_id") or row.get("id") or 0)
                if post_id <= 0:
                    continue
                author = str(row.get("author_name") or "").strip() or "-"
                url = str(row.get("post_url") or "").strip() or "-"
                msg = format_linkedin_post_lead(owner_tag, url, author)
                if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                    sent += 1
                    notified_rows += 1
                    mark_linkedin_post_handover_sent(post_id, owner_name)
    else:
        for row in relevant_rows:
            post_id = int(row.get("linkedin_post_id") or row.get("id") or 0)
            if post_id <= 0:
                continue
            author = str(row.get("author_name") or "").strip() or "-"
            url = str(row.get("post_url") or "").strip() or "-"
            msg = format_linkedin_post_lead("*Unassigned*", url, author)
            if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                sent += 1
                notified_rows += 1
                mark_linkedin_post_handover_sent(post_id, "")
    logger.info("linkedin-posts handover (db) sent %s slack messages", sent)
    return sent, notified_rows


def get_role_linkedin_posts_classify_run_metrics(run_id: str) -> dict[str, Any] | None:
    return ROLE_LINKEDIN_POSTS_CLASSIFY_RUN_METRICS.get(run_id)


def get_role_linkedin_posts_notify_run_metrics(run_id: str) -> dict[str, Any] | None:
    return ROLE_LINKEDIN_POSTS_NOTIFY_RUN_METRICS.get(run_id)


def _validate_role(role: str | None) -> str:
    resolved = (role or "").strip()
    if not resolved:
        raise ValueError("role is required.")
    return resolved


def _resolve_role_queries(role: str, override_queries: list[str] | None) -> list[str]:
    """
    Resolution order (highest first):
      1. ``override_queries`` arg (e.g. from API call)
      2. ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_FILE`` / ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON`` -> role -> ``scrape.queries``
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


def _load_linkedin_ai_prompts_from_file() -> dict[str, str]:
    """Optional map from ``ROLE_LINKEDIN_POSTS_AI_PROMPTS_FILE``."""
    global _linkedin_posts_ai_prompts_file_cache, _linkedin_posts_ai_prompts_file_cache_valid
    raw_path = (os.getenv("ROLE_LINKEDIN_POSTS_AI_PROMPTS_FILE") or "").strip()
    if not raw_path:
        return {}
    if _linkedin_posts_ai_prompts_file_cache_valid and _linkedin_posts_ai_prompts_file_cache is not None:
        return _linkedin_posts_ai_prompts_file_cache
    p = Path(raw_path).expanduser().resolve()
    if not p.is_file():
        logger.warning("ROLE_LINKEDIN_POSTS_AI_PROMPTS_FILE not found: %s", p)
        _linkedin_posts_ai_prompts_file_cache = {}
        _linkedin_posts_ai_prompts_file_cache_valid = True
        return _linkedin_posts_ai_prompts_file_cache
    try:
        parsed = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("invalid ROLE_LINKEDIN_POSTS_AI_PROMPTS_FILE %s: %s", p, exc)
        _linkedin_posts_ai_prompts_file_cache = {}
        _linkedin_posts_ai_prompts_file_cache_valid = True
        return _linkedin_posts_ai_prompts_file_cache
    out: dict[str, str] = {}
    if isinstance(parsed, dict):
        for role_key, val in parsed.items():
            if isinstance(role_key, str) and isinstance(val, str) and val.strip():
                out[role_key.strip().lower()] = val
    _linkedin_posts_ai_prompts_file_cache = out
    _linkedin_posts_ai_prompts_file_cache_valid = True
    logger.info(
        "Loaded ROLE_LINKEDIN_POSTS_AI_PROMPTS_FILE: %d roles from %s",
        len(out),
        p,
    )
    return out


def _classify_relevant_posts_for_role_pipeline(
    rows: list[dict[str, Any]],
    role: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """
    Role pipeline wrapper around LinkedIn-post classifier prompt.

    Resolution order (highest first):
      1. ``ROLE_LINKEDIN_POSTS_AI_PROMPTS_FILE`` map entry for the role
      2. ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON`` / file -> role -> ``ai_relevance_prompt``
      3. ``ROLE_LINKEDIN_POSTS_AI_RELEVANCE_PROMPT`` (global role-pipeline override)
      4. ``AI_RELEVANCE_PROMPT_LINKEDIN_POSTS`` / built-in (existing fallback)
    """
    role_prompt: str | None = None
    if role:
        key_lc = (role or "").strip().lower()
        prompts_map = _load_linkedin_ai_prompts_from_file()
        if key_lc and key_lc in prompts_map:
            role_prompt = prompts_map[key_lc]
        if not role_prompt:
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
# Per-role config (ROLE_LINKEDIN_POSTS_ROLE_CONFIG_FILE or ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON)
# ---------------------------------------------------------------------------
#
# A JSON file path or env string drives per-role behaviour for the LinkedIn-posts role
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


def _resolve_linkedin_posts_role_config_parsed() -> dict[str, Any] | None:
    """Load top-level JSON object from ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_FILE`` or env string."""
    global _linkedin_posts_role_config_file_cache, _linkedin_posts_role_config_file_cache_valid
    file_raw = (os.getenv("ROLE_LINKEDIN_POSTS_ROLE_CONFIG_FILE") or "").strip()
    if file_raw:
        p = Path(file_raw).expanduser().resolve()
        if p.is_file():
            if _linkedin_posts_role_config_file_cache_valid and _linkedin_posts_role_config_file_cache is not None:
                return _linkedin_posts_role_config_file_cache
            try:
                parsed = json.loads(p.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                logger.warning("invalid ROLE_LINKEDIN_POSTS_ROLE_CONFIG_FILE %s: %s", p, exc)
            else:
                if not isinstance(parsed, dict):
                    logger.warning(
                        "ROLE_LINKEDIN_POSTS_ROLE_CONFIG_FILE must be a JSON object: %s",
                        p,
                    )
                else:
                    _linkedin_posts_role_config_file_cache = parsed
                    _linkedin_posts_role_config_file_cache_valid = True
                    logger.info(
                        "Loaded ROLE_LINKEDIN_POSTS_ROLE_CONFIG_FILE: %d roles from %s",
                        len(parsed),
                        p,
                    )
                    return parsed
    raw = (os.getenv("ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON") or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("invalid ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON: %s", exc)
        return None
    if not isinstance(parsed, dict):
        logger.warning("ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON must be a JSON object")
        return None
    return parsed


def _load_role_linkedin_config_map() -> dict[str, dict[str, Any]]:
    """Parse LinkedIn-posts role config from file or ``ROLE_LINKEDIN_POSTS_ROLE_CONFIG_JSON``.

    Returns ``{}`` when unset, empty, malformed JSON, or not a JSON object.
    Role keys are normalised to lowercase + stripped.
    """
    parsed = _resolve_linkedin_posts_role_config_parsed()
    if not parsed:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for role_key, role_cfg in parsed.items():
        if not isinstance(role_key, str) or not isinstance(role_cfg, dict):
            continue
        out[role_key.strip().lower()] = role_cfg
    return out


def linkedin_posts_config_role_labels() -> list[str]:
    """Top-level role keys from LinkedIn-posts role config (file or env), in order.

    Used by the role LinkedIn posts cron to decide which roles to scrape/classify
    when that config is set. Each label is the JSON key after ``strip()`` (so
    casing matches what you wrote in the file). Empty list if unset, invalid
    JSON, or no valid entries.
    """
    parsed = _resolve_linkedin_posts_role_config_parsed()
    if not parsed:
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
    return _filter_new_rows_against_normalized_url_set(rows, seen)


def _filter_new_rows_against_normalized_url_set(
    rows: list[dict[str, Any]],
    existing_normalized_urls: set[str],
) -> list[dict[str, Any]]:
    seen: set[str] = set(existing_normalized_urls)
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
