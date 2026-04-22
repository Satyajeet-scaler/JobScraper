from __future__ import annotations

import json
from datetime import date
from typing import Any

from services.jobs_schema_mapping import canonicalize_recruiter_row, normalize_job_url
from services.mysql_recruiter_store import _db


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _to_date(value: Any) -> date:
    raw = str(value or "").strip()
    if not raw:
        return date.today()
    return date.fromisoformat(raw.split("T")[0].split(" ")[0])


def _json_dump(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), default=str)


def upsert_job(row: dict[str, Any]) -> int:
    site = str(row.get("site") or "").strip().lower() or "unknown"
    job_url = str(row.get("job_url") or "").strip()
    job_url_normalized = normalize_job_url(job_url)
    if not job_url_normalized:
        raise ValueError("job_url is required to upsert jobs.")

    payload = {
        "site": site,
        "job_url": job_url,
        "job_url_normalized": job_url_normalized,
        "title": row.get("title"),
        "company": row.get("company"),
        "location": row.get("location"),
        "date_posted": row.get("date_posted"),
        "requested_role": row.get("requested_role"),
        "run_date": _to_date(row.get("run_date")),
    }

    sql = """
    INSERT INTO jobs (
        site, job_url, job_url_normalized, title, company, location, date_posted, requested_role, run_date
    ) VALUES (
        %(site)s, %(job_url)s, %(job_url_normalized)s, %(title)s, %(company)s, %(location)s, %(date_posted)s, %(requested_role)s, %(run_date)s
    )
    ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        company = VALUES(company),
        location = VALUES(location),
        date_posted = VALUES(date_posted),
        updated_at = CURRENT_TIMESTAMP
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)
            cur.execute(
                """
                SELECT id FROM jobs
                WHERE site=%s AND job_url_normalized=%s AND requested_role <=> %s AND run_date=%s
                LIMIT 1
                """,
                (site, job_url_normalized, payload["requested_role"], payload["run_date"]),
            )
            row_id = cur.fetchone() or {}
    return int(row_id.get("id") or 0)


def upsert_job_scrape(row: dict[str, Any]) -> None:
    job_id = upsert_job(row)
    description_full = str(row.get("description_full") or row.get("description") or "").strip()
    sql = """
    INSERT INTO job_scrapes (
        job_id, role_query, experience, salary, job_type, description_full, raw_payload_json,
        role_pipeline_run_id, role_pipeline_run_seq
    ) VALUES (
        %(job_id)s, %(role_query)s, %(experience)s, %(salary)s, %(job_type)s, %(description_full)s, CAST(%(raw_payload_json)s AS JSON),
        %(role_pipeline_run_id)s, %(role_pipeline_run_seq)s
    )
    ON DUPLICATE KEY UPDATE
        role_query = VALUES(role_query),
        experience = VALUES(experience),
        salary = VALUES(salary),
        job_type = VALUES(job_type),
        description_full = VALUES(description_full),
        raw_payload_json = VALUES(raw_payload_json),
        updated_at = CURRENT_TIMESTAMP
    """
    payload = {
        "job_id": job_id,
        "role_query": row.get("role_query"),
        "experience": row.get("experience"),
        "salary": row.get("salary"),
        "job_type": row.get("job_type"),
        "description_full": description_full,
        "raw_payload_json": _json_dump(row.get("raw_payload")),
        "role_pipeline_run_id": row.get("role_pipeline_run_id"),
        "role_pipeline_run_seq": _safe_int(row.get("role_pipeline_run_seq")),
    }
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)


def upsert_job_relevance(row: dict[str, Any]) -> None:
    job_id = upsert_job(row)
    sql = """
    INSERT INTO job_relevance (
        job_id, is_relevant, matched_role, role_category, priority, reason, company_size, confidence,
        assigned_owner, handover_sent
    ) VALUES (
        %(job_id)s, %(is_relevant)s, %(matched_role)s, %(role_category)s, %(priority)s, %(reason)s, %(company_size)s, %(confidence)s,
        %(assigned_owner)s, %(handover_sent)s
    )
    ON DUPLICATE KEY UPDATE
        matched_role = VALUES(matched_role),
        role_category = VALUES(role_category),
        priority = VALUES(priority),
        reason = VALUES(reason),
        company_size = VALUES(company_size),
        confidence = VALUES(confidence),
        assigned_owner = VALUES(assigned_owner),
        handover_sent = VALUES(handover_sent),
        updated_at = CURRENT_TIMESTAMP
    """
    payload = {
        "job_id": job_id,
        "is_relevant": True,
        "matched_role": row.get("matched_role"),
        "role_category": row.get("role_category"),
        "priority": row.get("priority"),
        "reason": row.get("reason"),
        "company_size": row.get("company_size"),
        "confidence": row.get("confidence"),
        "assigned_owner": row.get("assigned_owner") or row.get("assigned owner"),
        "handover_sent": str(row.get("handover_sent") or "").strip().lower() in ("1", "true", "yes"),
    }
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)


def upsert_job_recruiter_contact(row: dict[str, Any]) -> None:
    mapped = canonicalize_recruiter_row(row)
    job_id = upsert_job(mapped)
    sql = """
    INSERT INTO job_recruiter_contacts (
        job_id, run_date, recruiter_name, recruiter_headline, recruiter_profile_url, recruiter_profile_url_normalized,
        recruiter_email, recruiter_source, scrape_error,
        assigned_owner, handover_sent
    ) VALUES (
        %(job_id)s, %(run_date)s, %(recruiter_name)s, %(recruiter_headline)s, %(recruiter_profile_url)s, %(recruiter_profile_url_normalized)s,
        %(recruiter_email)s, %(recruiter_source)s, %(scrape_error)s,
        %(assigned_owner)s, %(handover_sent)s
    )
    ON DUPLICATE KEY UPDATE
        recruiter_name = VALUES(recruiter_name),
        recruiter_headline = VALUES(recruiter_headline),
        scrape_error = VALUES(scrape_error),
        assigned_owner = VALUES(assigned_owner),
        updated_at = CURRENT_TIMESTAMP
    """
    payload = {
        "job_id": job_id,
        "run_date": _to_date(mapped.get("run_date")),
        "recruiter_name": mapped.get("recruiter_name"),
        "recruiter_headline": mapped.get("recruiter_headline"),
        "recruiter_profile_url": mapped.get("recruiter_profile_url"),
        "recruiter_profile_url_normalized": mapped.get("normalized_recruiter_profile_url")
        or mapped.get("recruiter_profile_url_normalized")
        or normalize_job_url(mapped.get("recruiter_profile_url")),
        "recruiter_email": mapped.get("recruiter_email"),
        "recruiter_source": mapped.get("recruiter_source"),
        "scrape_error": mapped.get("scrape_error"),
        "assigned_owner": mapped.get("assigned_owner"),
        "handover_sent": str(mapped.get("handover_sent") or "").strip().lower() in ("1", "true", "yes"),
    }
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)


def fetch_recruiter_rows_for_role(*, role: str, run_date: str, upstream_run_id: str | None = None) -> list[dict[str, Any]]:
    sql = """
    SELECT
        c.run_date, j.job_url, j.title, j.company, j.site,
        r.matched_role, r.role_category, r.priority,
        c.recruiter_name, c.recruiter_headline, c.recruiter_profile_url, c.recruiter_email,
        c.recruiter_source, c.scrape_error, c.assigned_owner, c.handover_sent
    FROM job_recruiter_contacts c
    JOIN jobs j ON j.id = c.job_id
    LEFT JOIN job_relevance r ON r.job_id = j.id
    WHERE j.requested_role = %s AND c.run_date = %s
    """
    params: list[Any] = [role, run_date]
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    return [dict(row) for row in rows]


def fetch_jd_rows_for_role(*, role: str, run_date: str) -> list[dict[str, Any]]:
    sql = """
    SELECT
        c.id AS recruiter_contact_id,
        c.run_date,
        j.job_url,
        j.title,
        j.company,
        COALESCE(s.description_full, '') AS jd
    FROM job_recruiter_contacts c
    JOIN jobs j ON j.id = c.job_id
    LEFT JOIN job_scrapes s ON s.job_id = j.id
    WHERE j.requested_role = %s AND c.run_date = %s
    ORDER BY c.id ASC
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role, run_date))
            rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["_jd_key"] = "jd"
        item["_recruiter_sheet_row_number"] = str(item.get("recruiter_contact_id") or "")
        out.append(item)
    return out


def mark_recruiter_contacts_handover_sent(
    *,
    role: str,
    run_date: str,
    identities: set[tuple[str, str, str, str]],
) -> int:
    if not identities:
        return 0
    updated = 0
    sql = """
    UPDATE job_recruiter_contacts c
    JOIN jobs j ON j.id = c.job_id
    SET c.handover_sent = TRUE, c.updated_at = CURRENT_TIMESTAMP
    WHERE j.requested_role = %s
      AND c.run_date = %s
      AND j.job_url_normalized = %s
      AND LOWER(COALESCE(c.recruiter_profile_url, '')) = %s
      AND LOWER(COALESCE(c.recruiter_email, '')) = %s
      AND LOWER(COALESCE(c.recruiter_source, '')) = %s
    """
    with _db() as conn:
        with conn.cursor() as cur:
            for job_key, profile, email, source in identities:
                cur.execute(sql, (role, run_date, job_key, profile, email, source))
                updated += int(cur.rowcount or 0)
    return updated


def upsert_job_candidate_match(row: dict[str, Any], candidate_email: str, ai_score: int | None, ai_reason: str | None) -> None:
    job_id = upsert_job(row)
    sql = """
    INSERT INTO job_candidate_matches (
        job_id, run_date, role_slug, candidate_email, ai_score, ai_reason
    ) VALUES (
        %(job_id)s, %(run_date)s, %(role_slug)s, %(candidate_email)s, %(ai_score)s, %(ai_reason)s
    )
    ON DUPLICATE KEY UPDATE
        ai_score = VALUES(ai_score),
        ai_reason = VALUES(ai_reason),
        updated_at = CURRENT_TIMESTAMP
    """
    payload = {
        "job_id": job_id,
        "run_date": _to_date(row.get("run_date")),
        "role_slug": row.get("role_slug"),
        "candidate_email": str(candidate_email).strip().lower(),
        "ai_score": ai_score,
        "ai_reason": ai_reason,
    }
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, payload)


def fetch_candidate_match_counts_for_role(*, role_slug: str, run_date: str) -> dict[str, int]:
    """Returns mapping of normalized job_url to count of candidates with ai_score > 70."""
    sql = """
    SELECT j.job_url_normalized, COUNT(c.id) as gt_70_count
    FROM job_candidate_matches c
    JOIN jobs j ON j.id = c.job_id
    WHERE c.role_slug = %s AND c.run_date = %s AND c.ai_score > 70
    GROUP BY j.job_url_normalized
    """
    out: dict[str, int] = {}
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role_slug, run_date))
            rows = cur.fetchall() or []
            for r in rows:
                out[r["job_url_normalized"]] = int(r["gt_70_count"])
    return out


def fetch_evaluated_job_urls_for_role(*, role_slug: str, run_date: str) -> set[str]:
    """Returns set of normalized job_urls that have been evaluated for this role/date."""
    sql = """
    SELECT DISTINCT j.job_url_normalized
    FROM job_candidate_matches c
    JOIN jobs j ON j.id = c.job_id
    WHERE c.role_slug = %s AND c.run_date = %s
    """
    out: set[str] = set()
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role_slug, run_date))
            rows = cur.fetchall() or []
            for r in rows:
                out.add(r["job_url_normalized"])
    return out


def fetch_all_candidate_match_counts(*, run_date: str) -> dict[str, int]:
    """Returns mapping of raw job_url to count of candidates with ai_score > 70 across all roles."""
    sql = """
    SELECT j.job_url, COUNT(c.id) as gt_70_count
    FROM job_candidate_matches c
    JOIN jobs j ON j.id = c.job_id
    WHERE c.run_date = %s AND c.ai_score > 70
    GROUP BY j.job_url
    """
    out: dict[str, int] = {}
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_date,))
            rows = cur.fetchall() or []
            for r in rows:
                out[str(r["job_url"])] = int(r["gt_70_count"])
    return out


# ---------------------------------------------------------------------------
# Pipeline stage tracking: fetch unprocessed + mark processed
# ---------------------------------------------------------------------------


def fetch_unclassified_jobs_for_role(*, role: str, run_date: str) -> list[dict[str, Any]]:
    """Return scraped jobs that have not yet been classified (relevancy_checked=FALSE)."""
    sql = """
    SELECT
        j.id AS _job_id,
        j.site,
        j.job_url,
        j.job_url_normalized,
        j.title,
        j.company,
        j.location,
        j.date_posted,
        j.requested_role,
        j.run_date,
        COALESCE(s.role_query, '') AS role_query,
        COALESCE(s.description_full, '') AS description,
        COALESCE(s.experience, '') AS experience,
        COALESCE(s.salary, '') AS salary,
        COALESCE(s.job_type, '') AS job_type
    FROM jobs j
    LEFT JOIN job_scrapes s ON s.job_id = j.id
    WHERE j.requested_role = %s
      AND j.run_date = %s
      AND j.relevancy_checked = FALSE
    ORDER BY j.id ASC
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role, run_date))
            rows = cur.fetchall() or []
    return [dict(r) for r in rows]


def mark_jobs_relevancy_checked(job_ids: list[int]) -> int:
    """Set relevancy_checked=TRUE for the given job IDs."""
    if not job_ids:
        return 0
    placeholders = ",".join(["%s"] * len(job_ids))
    sql = f"UPDATE jobs SET relevancy_checked = TRUE, updated_at = CURRENT_TIMESTAMP WHERE id IN ({placeholders})"
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(job_ids))
            return int(cur.rowcount or 0)


def fetch_recruiter_unchecked_jobs_for_role(*, role: str, run_date: str) -> list[dict[str, Any]]:
    """Return classified jobs that have not yet had recruiter info extracted."""
    sql = """
    SELECT
        j.id AS _job_id,
        j.site,
        j.job_url,
        j.job_url_normalized,
        j.title,
        j.company,
        j.location,
        j.date_posted,
        j.requested_role,
        j.run_date,
        r.matched_role,
        r.role_category,
        r.priority
    FROM jobs j
    LEFT JOIN job_relevance r ON r.job_id = j.id
    WHERE j.requested_role = %s
      AND j.run_date = %s
      AND j.relevancy_checked = TRUE
      AND j.recruiter_info_checked = FALSE
    ORDER BY j.id ASC
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role, run_date))
            rows = cur.fetchall() or []
    return [dict(r) for r in rows]


def mark_jobs_recruiter_info_checked(job_ids: list[int]) -> int:
    """Set recruiter_info_checked=TRUE for the given job IDs."""
    if not job_ids:
        return 0
    placeholders = ",".join(["%s"] * len(job_ids))
    sql = f"UPDATE jobs SET recruiter_info_checked = TRUE, updated_at = CURRENT_TIMESTAMP WHERE id IN ({placeholders})"
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(job_ids))
            return int(cur.rowcount or 0)


def fetch_jd_eval_pending_jobs_for_role(*, role: str, run_date: str) -> list[dict[str, Any]]:
    """Return jobs that have recruiter contacts and have not yet been through candidate JD eval.

    Only jobs with at least one entry in ``job_recruiter_contacts`` are returned,
    ensuring we only evaluate JDs where a recruiter was actually found.
    """
    sql = """
    SELECT DISTINCT
        j.id AS _job_id,
        j.site,
        j.job_url,
        j.job_url_normalized,
        j.title,
        j.company,
        j.location,
        j.requested_role,
        j.run_date,
        COALESCE(s.description_full, '') AS jd
    FROM jobs j
    INNER JOIN job_recruiter_contacts rc ON rc.job_id = j.id
    LEFT JOIN job_scrapes s ON s.job_id = j.id
    WHERE j.requested_role = %s
      AND j.run_date = %s
      AND j.candidates_jd_eval_done = FALSE
    ORDER BY j.id ASC
    """
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (role, run_date))
            rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["_jd_key"] = "jd"
        item["_recruiter_sheet_row_number"] = str(item.get("_job_id") or "")
        out.append(item)
    return out


def mark_jobs_jd_eval_done(job_ids: list[int]) -> int:
    """Set candidates_jd_eval_done=TRUE for the given job IDs."""
    if not job_ids:
        return 0
    placeholders = ",".join(["%s"] * len(job_ids))
    sql = f"UPDATE jobs SET candidates_jd_eval_done = TRUE, updated_at = CURRENT_TIMESTAMP WHERE id IN ({placeholders})"
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(job_ids))
            return int(cur.rowcount or 0)
