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
