from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from services.mysql_recruiter_store import _db

logger = logging.getLogger(__name__)

CLASSIFY_STATUS_PENDING = "pending"
CLASSIFY_STATUS_PROCESSING = "processing"
CLASSIFY_STATUS_DONE = "done"
CLASSIFY_STATUS_FAILED = "failed"


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any, max_len: int | None = None) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=True, default=str)
    else:
        text = str(value)
    if max_len and len(text) > max_len:
        return text[:max_len]
    return text


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


def _coerce_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return _to_date(value)


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


def existing_linkedin_post_url_normalized_set(*, requested_role: str, run_date: date) -> set[str]:
    """Return normalized post_url values already stored for this role and run_date (scrape dedupe)."""
    role = (requested_role or "").strip()
    if not role:
        return set()
    sql = """
    SELECT post_url_normalized
    FROM linkedin_posts
    WHERE requested_role = %s AND run_date = %s
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role, run_date))
            rows = cur.fetchall() or []
    out: set[str] = set()
    for r in rows:
        u = str((r or {}).get("post_url_normalized") or "").strip()
        if u:
            out.add(u)
    return out


def fetch_pending_linkedin_posts_for_classify(
    *,
    requested_role: str,
    run_date: date | str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    role = (requested_role or "").strip()
    if not role:
        return []
    run_date_obj = _coerce_date(run_date)
    limit = max(1, int(limit))
    sql = """
    SELECT
        id,
        post_url, post_url_normalized, post_id, search_query, content_type, post_text, posted_at,
        author_name, author_profile_url, author_info, author_type, company, job_title_hint,
        likes_count, comments_count, reposts_count, requested_role, role_slug, run_date, run_id, run_seq,
        raw_payload_json, classify_status
    FROM linkedin_posts
    WHERE requested_role = %s
      AND run_date = %s
      AND classify_status IN (%s, %s)
    ORDER BY id ASC
    LIMIT %s
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    role,
                    run_date_obj,
                    CLASSIFY_STATUS_PENDING,
                    CLASSIFY_STATUS_FAILED,
                    limit,
                ),
            )
            rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        raw_json = item.get("raw_payload_json")
        if raw_json:
            try:
                item["raw_payload"] = json.loads(str(raw_json))
            except Exception:
                item["raw_payload"] = {}
        out.append(item)
    return out


def mark_linkedin_posts_classify_processing(*, post_ids: list[int]) -> int:
    if not post_ids:
        return 0
    cleaned = [int(pid) for pid in post_ids if int(pid) > 0]
    if not cleaned:
        return 0
    placeholders = ",".join(["%s"] * len(cleaned))
    sql = f"""
    UPDATE linkedin_posts
    SET classify_status = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE id IN ({placeholders})
      AND classify_status IN (%s, %s)
    """
    params: list[Any] = [
        CLASSIFY_STATUS_PROCESSING,
        *cleaned,
        CLASSIFY_STATUS_PENDING,
        CLASSIFY_STATUS_FAILED,
    ]
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return int(cur.rowcount or 0)


def mark_linkedin_post_classify_done(*, post_id: int) -> None:
    sql = """
    UPDATE linkedin_posts
    SET classify_status = %s,
        classified_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = %s
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (CLASSIFY_STATUS_DONE, int(post_id)))


def mark_linkedin_posts_classify_done(*, post_ids: list[int]) -> int:
    if not post_ids:
        return 0
    cleaned = [int(pid) for pid in post_ids if int(pid) > 0]
    if not cleaned:
        return 0
    placeholders = ",".join(["%s"] * len(cleaned))
    sql = f"""
    UPDATE linkedin_posts
    SET classify_status = %s,
        classified_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE id IN ({placeholders})
    """
    params: list[Any] = [CLASSIFY_STATUS_DONE, *cleaned]
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return int(cur.rowcount or 0)


def mark_linkedin_post_classify_failed(*, post_id: int, error: str) -> None:
    _ = error
    sql = """
    UPDATE linkedin_posts
    SET classify_status = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = %s
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (CLASSIFY_STATUS_FAILED, int(post_id)))


def mark_linkedin_posts_classify_failed(*, post_ids: list[int], error: str) -> int:
    _ = error
    if not post_ids:
        return 0
    cleaned = [int(pid) for pid in post_ids if int(pid) > 0]
    if not cleaned:
        return 0
    placeholders = ",".join(["%s"] * len(cleaned))
    sql = f"""
    UPDATE linkedin_posts
    SET classify_status = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE id IN ({placeholders})
    """
    params: list[Any] = [CLASSIFY_STATUS_FAILED, *cleaned]
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return int(cur.rowcount or 0)


def count_linkedin_posts_by_classify_status(*, requested_role: str, run_date: date | str) -> dict[str, int]:
    role = (requested_role or "").strip()
    if not role:
        return {}
    run_date_obj = _coerce_date(run_date)
    sql = """
    SELECT classify_status, COUNT(*) AS c
    FROM linkedin_posts
    WHERE requested_role = %s AND run_date = %s
    GROUP BY classify_status
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role, run_date_obj))
            rows = cur.fetchall() or []
    out: dict[str, int] = {}
    for row in rows:
        key = str((row or {}).get("classify_status") or "").strip()
        if not key:
            continue
        out[key] = int((row or {}).get("c") or 0)
    return out


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
        "post_url_normalized": _safe_str(post_url_normalized, 512),
        "post_id": _safe_str(row.get("post_id"), 255),
        "search_query": _safe_str(row.get("search_query"), 512),
        "content_type": _safe_str(row.get("content_type"), 50),
        "post_text": _safe_str(row.get("post_text")),
        "posted_at": _safe_str(row.get("posted_at"), 120),
        "author_name": _safe_str(row.get("author_name"), 512),
        "author_profile_url": _safe_str(row.get("author_profile_url")),
        "author_info": _safe_str(row.get("author_info")),
        "author_type": _safe_str(row.get("author_type"), 100),
        "company": _safe_str(row.get("company"), 255),
        "job_title_hint": _safe_str(row.get("job_title_hint"), 512),
        "likes_count": _safe_int(row.get("likes_count")),
        "comments_count": _safe_int(row.get("comments_count")),
        "reposts_count": _safe_int(row.get("reposts_count")),
        "requested_role": _safe_str(requested_role, 100),
        "role_slug": _safe_str(row.get("role_slug"), 100),
        "run_date": run_date,
        "run_id": _safe_str(row.get("run_id") or row.get("role_linkedin_posts_run_id"), 64),
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
            res_id = int(row_id.get("id") or 0)
    if res_id:
        logger.info("Successfully upserted linkedin_post id=%d url_normalized=%s", res_id, post_url_normalized)
    else:
        logger.warning("upsert_linkedin_post: failed to retrieve id for %s", post_url_normalized)
    return res_id


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
        "tier": _safe_str(row.get("tier"), 10),
        "role_category": _safe_str(row.get("role_category"), 255),
        "reason": _safe_str(row.get("reason")),
        "author_company": _safe_str(row.get("author_company"), 255),
        "hiring_company": _safe_str(row.get("hiring_company"), 255),
        "confidence": _safe_str(row.get("confidence"), 120),
        "priority": _safe_str(row.get("priority"), 120),
        "assigned_owner": _safe_str(row.get("assigned_owner") or row.get("assigned owner"), 255),
        "handover_sent": bool(row.get("handover_sent")),
        "classify_run_id": _safe_str(row.get("classify_run_id") or row.get("role_linkedin_posts_classify_run_id"), 64),
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
    logger.info("Successfully upserted linkedin_post_relevance for post_id=%d", post_id)


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


def fetch_unsent_relevant_linkedin_posts_for_role(
    *,
    role: str,
    run_date: str,
    upstream_run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch relevant + unsent rows for role/date (optional classify run scope)."""
    sql = """
    SELECT
        lp.id AS linkedin_post_id,
        lp.*,
        lpr.is_relevant, lpr.tier, lpr.role_category, lpr.reason,
        lpr.author_company AS ai_author_company, lpr.hiring_company AS ai_hiring_company,
        lpr.confidence AS ai_confidence, lpr.priority AS ai_priority,
        lpr.assigned_owner, lpr.handover_sent,
        lpr.classify_run_id, lpr.classify_run_seq
    FROM linkedin_posts lp
    JOIN linkedin_post_relevance lpr ON lp.id = lpr.linkedin_post_id
    WHERE lp.requested_role = %s
      AND lp.run_date = %s
      AND lpr.is_relevant = TRUE
      AND lpr.handover_sent = FALSE
    """
    params: list[Any] = [role, run_date]
    if upstream_run_id:
        sql += " AND lpr.classify_run_id = %s"
        params.append(upstream_run_id)
    sql += " ORDER BY lp.id ASC"
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
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
