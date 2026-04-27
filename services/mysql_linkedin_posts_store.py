from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from services.mysql_recruiter_store import _db

logger = logging.getLogger(__name__)


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, default=str)
    return str(value)


def _to_date(value: Any) -> date:
    raw = str(value or "").strip()
    if not raw:
        return date.today()
    try:
        # Expected format: 2026-04-27T... or 2026-04-27 ...
        parts_t = raw.split("T")
        date_part = parts_t[0]
        parts_space = date_part.split(" ")
        final_date_str = parts_space[0]
        return date.fromisoformat(final_date_str)
    except Exception:
        return date.today()


def _json_dump(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), default=str)


def _normalized_post_url(raw_url: Any) -> str:
    from urllib.parse import urlparse
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


def upsert_linkedin_post(row: dict[str, Any]) -> int:
    """Upsert a row into linkedin_posts and return its ID."""
    post_url = str(row.get("post_url") or "").strip()
    post_url_normalized = _normalized_post_url(post_url)
    if not post_url_normalized:
        raise ValueError("post_url is required to upsert linkedin_posts.")

    requested_role = str(row.get("requested_role") or "").strip()
    run_date = _to_date(row.get("run_date"))

    payload = {
        "post_url": post_url,
        "post_url_normalized": post_url_normalized,
        "post_id": _safe_str(row.get("post_id")),
        "search_query": _safe_str(row.get("search_query")),
        "content_type": _safe_str(row.get("content_type")),
        "post_text": _safe_str(row.get("post_text")),
        "posted_at": _safe_str(row.get("posted_at")),
        "author_name": _safe_str(row.get("author_name")),
        "author_profile_url": _safe_str(row.get("author_profile_url")),
        "author_info": _safe_str(row.get("author_info")),
        "author_type": _safe_str(row.get("author_type")),
        "company": _safe_str(row.get("company")),
        "job_title_hint": _safe_str(row.get("job_title_hint")),
        "likes_count": _safe_int(row.get("likes_count")),
        "comments_count": _safe_int(row.get("comments_count")),
        "reposts_count": _safe_int(row.get("reposts_count")),
        "requested_role": requested_role,
        "role_slug": row.get("role_slug"),
        "run_date": run_date,
        "run_id": row.get("run_id") or row.get("role_linkedin_posts_run_id"),
        "run_seq": _safe_int(row.get("run_seq") or row.get("role_linkedin_posts_run_seq")),
        "raw_payload_json": _json_dump(row.get("raw_payload")),
    }

    sql = """
    INSERT INTO linkedin_posts (
        post_url, post_url_normalized, post_id, search_query, content_type, post_text, posted_at,
        author_name, author_profile_url, author_info, author_type, company, job_title_hint,
        likes_count, comments_count, reposts_count, requested_role, role_slug, run_date,
        run_id, run_seq, raw_payload_json
    ) VALUES (
        %(post_url)s, %(post_url_normalized)s, %(post_id)s, %(search_query)s, %(content_type)s, %(post_text)s, %(posted_at)s,
        %(author_name)s, %(author_profile_url)s, %(author_info)s, %(author_type)s, %(company)s, %(job_title_hint)s,
        %(likes_count)s, %(comments_count)s, %(reposts_count)s, %(requested_role)s, %(role_slug)s, %(run_date)s,
        %(run_id)s, %(run_seq)s, CAST(%(raw_payload_json)s AS JSON)
    )
    ON DUPLICATE KEY UPDATE
        search_query = VALUES(search_query),
        content_type = VALUES(content_type),
        post_text = VALUES(post_text),
        posted_at = VALUES(posted_at),
        author_name = VALUES(author_name),
        author_profile_url = VALUES(author_profile_url),
        author_info = VALUES(author_info),
        author_type = VALUES(author_type),
        company = VALUES(company),
        job_title_hint = VALUES(job_title_hint),
        likes_count = VALUES(likes_count),
        comments_count = VALUES(comments_count),
        reposts_count = VALUES(reposts_count),
        run_id = VALUES(run_id),
        run_seq = VALUES(run_seq),
        raw_payload_json = VALUES(raw_payload_json),
        updated_at = CURRENT_TIMESTAMP
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)
            cur.execute(
                """
                SELECT id FROM linkedin_posts
                WHERE post_url_normalized=%s AND requested_role=%s AND run_date=%s
                LIMIT 1
                """,
                (post_url_normalized, requested_role, run_date),
            )
            row_id = cur.fetchone() or {}
    return int(row_id.get("id") or 0)


def upsert_linkedin_post_relevance(row: dict[str, Any]) -> None:
    """Upsert classification results. Maps normalized boolean fields too."""
    post_id = upsert_linkedin_post(row)
    if not post_id:
        logger.warning("upsert_linkedin_post_relevance: upsert_linkedin_post failed to return id")
        return

    is_relevant = bool(row.get("is_relevant") or row.get("relevant"))
    
    payload = {
        "linkedin_post_id": post_id,
        "is_relevant": is_relevant,
        "tier": _safe_str(row.get("tier")),
        "role_category": _safe_str(row.get("role_category")),
        "reason": _safe_str(row.get("reason")),
        "author_company": _safe_str(row.get("author_company")),
        "hiring_company": _safe_str(row.get("hiring_company")),
        "confidence": row.get("confidence"),
        "priority": _safe_str(row.get("priority")),
        "assigned_owner": _safe_str(row.get("assigned_owner") or row.get("assigned owner")),
        "handover_sent": bool(row.get("handover_sent")),
        "classify_run_id": row.get("classify_run_id") or row.get("role_linkedin_posts_classify_run_id"),
        "classify_run_seq": _safe_int(row.get("classify_run_seq") or row.get("role_linkedin_posts_classify_run_seq")),
    }

    sql = """
    INSERT INTO linkedin_post_relevance (
        linkedin_post_id, is_relevant, tier, role_category, reason,
        author_company, hiring_company, confidence, priority,
        assigned_owner, handover_sent, classify_run_id, classify_run_seq
    ) VALUES (
        %(linkedin_post_id)s, %(is_relevant)s, %(tier)s, %(role_category)s, %(reason)s,
        %(author_company)s, %(hiring_company)s, %(confidence)s, %(priority)s,
        %(assigned_owner)s, %(handover_sent)s, %(classify_run_id)s, %(classify_run_seq)s
    )
    ON DUPLICATE KEY UPDATE
        is_relevant = VALUES(is_relevant),
        tier = VALUES(tier),
        role_category = VALUES(role_category),
        reason = VALUES(reason),
        author_company = VALUES(author_company),
        hiring_company = VALUES(hiring_company),
        confidence = VALUES(confidence),
        priority = VALUES(priority),
        assigned_owner = VALUES(assigned_owner),
        handover_sent = VALUES(handover_sent),
        classify_run_id = VALUES(classify_run_id),
        classify_run_seq = VALUES(classify_run_seq),
        updated_at = CURRENT_TIMESTAMP
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)


def fetch_relevant_linkedin_posts_for_role(*, role: str, run_date: str) -> list[dict[str, Any]]:
    """Fetch relevant posts for a specific role and run date (replaces sheet read for Slack notify)."""
    sql = """
    SELECT 
        lp.*,
        lpr.is_relevant, lpr.tier, lpr.role_category, lpr.reason,
        lpr.author_company AS ai_author_company, lpr.hiring_company AS ai_hiring_company,
        lpr.confidence AS ai_confidence, lpr.priority AS ai_priority,
        lpr.assigned_owner, lpr.handover_sent,
        lpr.classify_run_id, lpr.classify_run_seq
    FROM linkedin_posts lp
    JOIN linkedin_post_relevance lpr ON lp.id = lpr.linkedin_post_id
    WHERE lp.requested_role = %s AND lp.run_date = %s AND lpr.is_relevant = TRUE
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role, run_date))
            rows = cur.fetchall() or []
            
        # For backward compatibility with Slack notification logic that expects "raw_payload"
    out: list[dict[str, Any]] = []
    for r in rows:
        item = dict(r)
        raw_json = item.get("raw_payload_json")
        if raw_json:
            try:
                item["raw_payload"] = json.loads(str(raw_json))
            except Exception:
                item["raw_payload"] = {}
        out.append(item)
    return out


def mark_linkedin_post_handover_sent(post_id: int, owner: str) -> bool:
    """Update handover status in DB."""
    sql = """
    UPDATE linkedin_post_relevance 
    SET handover_sent = TRUE, assigned_owner = %s, updated_at = CURRENT_TIMESTAMP 
    WHERE linkedin_post_id = %s
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (owner, post_id))
            return cur.rowcount > 0


def fetch_all_relevant_linkedin_posts(run_date: str) -> list[dict[str, Any]]:
    """Fetch all relevant posts across all roles for a run date (for global handover)."""
    sql = """
    SELECT 
        lp.*,
        lpr.is_relevant, lpr.tier, lpr.role_category, lpr.reason,
        lpr.author_company AS ai_author_company, lpr.hiring_company AS ai_hiring_company,
        lpr.confidence AS ai_confidence, lpr.priority AS ai_priority,
        lpr.assigned_owner, lpr.handover_sent,
        lpr.classify_run_id, lpr.classify_run_seq
    FROM linkedin_posts lp
    JOIN linkedin_post_relevance lpr ON lp.id = lpr.linkedin_post_id
    WHERE lp.run_date = %s AND lpr.is_relevant = TRUE
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_date,))
            rows = cur.fetchall() or []
            
    out: list[dict[str, Any]] = []
    for r in rows:
        item = dict(r)
        raw_json = item.get("raw_payload_json")
        if raw_json:
            try:
                item["raw_payload"] = json.loads(str(raw_json))
            except Exception:
                item["raw_payload"] = {}
        out.append(item)
    return out
