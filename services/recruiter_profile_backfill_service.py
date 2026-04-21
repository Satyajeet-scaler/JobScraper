import logging
import os
import re
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

import requests

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.role_pipeline import role_relevant_tab_name
from services.role_recruiter_info_service import role_recruiters_tab_name_for_role

logger = logging.getLogger(__name__)

RECRUITER_PROFILE_BACKFILL_RUN_METRICS: dict[str, dict[str, Any]] = {}
DEFAULT_RECRUITER_TITLES: tuple[str, ...] = (
    "Recruiter",
    "Talent Acquisition",
    "Talent Acquisition Specialist",
    "Technical Recruiter",
    "Hiring Manager",
)

_DEFAULT_COMPANY_SIZE_ALLOWLIST = "startup,mid_level"


def _company_size_allowlist() -> frozenset[str] | None:
    """When None, company_size filtering is disabled (all candidate rows pass)."""
    raw = os.getenv("RECRUITER_PROFILE_BACKFILL_COMPANY_SIZE_ALLOWLIST")
    if raw is None:
        raw = _DEFAULT_COMPANY_SIZE_ALLOWLIST
    stripped = raw.strip()
    if not stripped or stripped == "*":
        return None
    parts = [x.strip().lower() for x in stripped.split(",") if x.strip()]
    return frozenset(parts) if parts else None


def _normalized_company_size(row: dict[str, Any]) -> str:
    return str(row.get("company_size") or "").strip().lower()


def _passes_company_size_filter(row: dict[str, Any], allowlist: frozenset[str] | None) -> bool:
    if allowlist is None:
        return True
    return _normalized_company_size(row) in allowlist


def run_recruiter_profile_backfill(
    run_id: str | None = None,
    run_date: str | None = None,
    role: str | None = None,
    relevant_tab: str | None = None,
    recruiters_tab: str | None = None,
) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    started_at = perf_counter()
    resolved_role = (role or "").strip()
    explicit_relevant_tab = (relevant_tab or "").strip()
    explicit_recruiters_tab = (recruiters_tab or "").strip()

    RECRUITER_PROFILE_BACKFILL_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
        "role": resolved_role,
        "relevant_tab": explicit_relevant_tab,
        "recruiters_tab": explicit_recruiters_tab,
    }

    try:
        resolved_tabs = _resolve_tabs(
            run_date=resolved_run_date,
            role=resolved_role,
            relevant_tab=explicit_relevant_tab,
            recruiters_tab=explicit_recruiters_tab,
        )
        relevant_rows = _read_rows_from_tab(resolved_tabs["relevant_tab"])
        existing_recruiter_rows = _read_rows_from_tab(
            resolved_tabs["recruiters_tab"],
            allow_missing=True,
        )

        existing_with_profile_urls = _job_urls_with_recruiter_profile(existing_recruiter_rows)
        base_candidates = [
            dict(row)
            for row in relevant_rows
            if _normalized_job_url(row) and _normalized_job_url(row) not in existing_with_profile_urls
        ]
        size_allowlist = _company_size_allowlist()
        if size_allowlist is None:
            candidates = base_candidates
            jobs_skipped_by_company_size = 0
        else:
            candidates = [
                row for row in base_candidates if _passes_company_size_filter(row, size_allowlist)
            ]
            jobs_skipped_by_company_size = len(base_candidates) - len(candidates)
        enriched_rows, lush_metrics = _build_rows_from_lusha(
            run_date=resolved_run_date,
            relevant_tab=resolved_tabs["relevant_tab"],
            candidate_jobs=candidates,
        )
        rows_written = _append_recruiter_rows(
            tab_name=resolved_tabs["recruiters_tab"],
            rows=enriched_rows,
            dedupe_existing_on=("job_url", "recruiter_profile_url", "recruiter_source"),
        )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "relevant_tab": resolved_tabs["relevant_tab"],
            "recruiters_tab": resolved_tabs["recruiters_tab"],
            "relevant_rows_scanned": len(relevant_rows),
            "existing_recruiter_rows_scanned": len(existing_recruiter_rows),
            "jobs_skipped_with_existing_profile_url": len(existing_with_profile_urls),
            "candidate_jobs_before_company_size_filter": len(base_candidates),
            "jobs_skipped_by_company_size": jobs_skipped_by_company_size,
            "candidate_jobs_for_backfill": len(candidates),
            "jobs_searched_on_lusha": lush_metrics["jobs_searched_on_lusha"],
            "jobs_with_lusha_search_hits": lush_metrics["jobs_with_lusha_search_hits"],
            "enrich_calls": lush_metrics["enrich_calls"],
            "lusha_credits_charged_total": lush_metrics["lusha_credits_charged_total"],
            "rows_with_lusha_linkedin_url": len(enriched_rows),
            "jobs_with_new_recruiter_profiles_found": lush_metrics["jobs_with_new_recruiter_profiles_found"],
            "recruiter_rows_appended": rows_written,
            "lusha_error_count": lush_metrics["lusha_error_count"],
            "lusha_errors_sample": lush_metrics["lusha_errors"][:20],
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        RECRUITER_PROFILE_BACKFILL_RUN_METRICS[pipeline_run_id] = metrics
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "relevant_tab": explicit_relevant_tab,
            "recruiters_tab": explicit_recruiters_tab,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        RECRUITER_PROFILE_BACKFILL_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("recruiter-profile-backfill[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_recruiter_profile_backfill_run_metrics(run_id: str) -> dict[str, Any] | None:
    return RECRUITER_PROFILE_BACKFILL_RUN_METRICS.get(run_id)


def _resolve_tabs(
    *,
    run_date: str,
    role: str,
    relevant_tab: str,
    recruiters_tab: str,
) -> dict[str, str]:
    if role and relevant_tab:
        raise ValueError("Provide either role or relevant_tab, not both.")
    if role:
        resolved_relevant = role_relevant_tab_name(role=role, run_date=run_date)
        resolved_recruiters = recruiters_tab or role_recruiters_tab_name_for_role(role=role, run_date=run_date)
        return {"relevant_tab": resolved_relevant, "recruiters_tab": resolved_recruiters}
    if relevant_tab:
        resolved_recruiters = recruiters_tab or _derive_recruiters_tab_from_relevant_tab(relevant_tab)
        return {"relevant_tab": relevant_tab, "recruiters_tab": resolved_recruiters}
    return {
        "relevant_tab": f"relevant_jobs_{run_date}",
        "recruiters_tab": recruiters_tab or f"recruiters_info_{run_date}",
    }


def _derive_recruiters_tab_from_relevant_tab(relevant_tab: str) -> str:
    legacy_match = re.fullmatch(r"relevant_jobs_(\d{4}-\d{2}-\d{2})", relevant_tab)
    if legacy_match:
        return f"recruiters_info_{legacy_match.group(1)}"

    role_match = re.fullmatch(r"role_relevant_([a-z0-9_]+)_(\d{4}-\d{2}-\d{2})", relevant_tab)
    if role_match:
        role_slug = role_match.group(1)
        run_date = role_match.group(2)
        template = (
            os.getenv("ROLE_PIPELINE_RECRUITERS_TAB_TEMPLATE")
            or "role_recruiters_info_{role_slug}_{date}"
        ).strip()
        return template.format(role_slug=role_slug, date=run_date)

    raise ValueError(
        "Could not derive recruiters tab from relevant_tab. "
        "Provide recruiters_tab explicitly when using custom tab names."
    )


def _read_rows_from_tab(tab_name: str, allow_missing: bool = False) -> list[dict[str, str]]:
    spreadsheet_id = _require_spreadsheet_id()
    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    try:
        ws = writer.open_worksheet(tab_name)
    except Exception:
        if allow_missing:
            return []
        raise
    raw = writer.worksheet_get_all_values(ws, f"recruiter_profile_backfill:{tab_name}:get_all_values")
    rows = [dict(r) for r in worksheet_row_dicts(raw)]
    if not rows and not allow_missing:
        raise RuntimeError(f"No rows found in worksheet {tab_name}.")
    return rows


def _job_urls_with_recruiter_profile(rows: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in rows:
        profile = str(row.get("recruiter_profile_url") or "").strip()
        if not profile:
            continue
        job_url = _normalized_job_url(row)
        if not job_url:
            continue
        out.add(job_url)
    return out


def _normalized_job_url(row: dict[str, Any]) -> str:
    return str(row.get("job_url") or "").strip()


def _require_spreadsheet_id() -> str:
    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    return spreadsheet_id


def _build_rows_from_lusha(
    *,
    run_date: str,
    relevant_tab: str,
    candidate_jobs: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    jobs_with_profile: set[str] = set()
    jobs_searched = 0
    jobs_with_hits = 0
    enrich_calls = 0
    credits_charged_total = 0
    lush_errors: list[str] = []
    titles = _recruiter_titles()

    for job in candidate_jobs:
        company = str(job.get("company") or "").strip()
        job_url = _normalized_job_url(job)
        if not company or not job_url:
            continue
        jobs_searched += 1
        try:
            search_response, contacts = _lusha_contact_search(
                company=company,
                location=str(job.get("location") or "").strip(),
                recruiter_titles=titles,
            )
            if contacts:
                jobs_with_hits += 1
            request_id = str(search_response.get("requestId") or "").strip()
            for contact in contacts[: _lusha_top_contacts_per_job()]:
                contact_id = _extract_contact_id(contact)
                if not contact_id or not request_id:
                    continue
                enrich_calls += 1
                enrich_response = _lusha_contact_enrich(
                    request_id=request_id,
                    contact_ids=[contact_id],
                )
                charged = _extract_credits_charged(enrich_response)
                credits_charged_total += charged
                enriched_contact = _extract_enriched_contact(
                    enrich_response,
                    contact_id=contact_id,
                )
                profile_url = _extract_linkedin_url_from_enrich(enriched_contact)
                if not profile_url:
                    continue
                jobs_with_profile.add(job_url)
                rows.append(
                    {
                        "run_date": run_date,
                        "relevant_jobs_tab": relevant_tab,
                        "job_url": job_url,
                        "title": job.get("title", ""),
                        "company": company,
                        "site": job.get("site", ""),
                        "matched_role": job.get("matched_role", ""),
                        "role_category": job.get("role_category", ""),
                        "priority": job.get("priority", ""),
                        "recruiter_name": _extract_name_from_enrich(enriched_contact, contact),
                        "recruiter_headline": _extract_headline_from_enrich(enriched_contact, contact),
                        "recruiter_profile_url": profile_url,
                        "recruiter_email": "",
                        "meet_the_team_section_found": False,
                        "recruiter_source": "lusha_search",
                        "scrape_error": "",
                        "lusha_search_response_json": search_response,
                        "lusha_enrich_response_json": enrich_response,
                        "lusha_request_id": request_id,
                        "lusha_contact_id": contact_id,
                        "lusha_credits_charged": charged,
                    }
                )
        except Exception as exc:
            lush_errors.append(f"{job_url}: {exc}")

    metrics = {
        "jobs_searched_on_lusha": jobs_searched,
        "jobs_with_lusha_search_hits": jobs_with_hits,
        "enrich_calls": enrich_calls,
        "lusha_credits_charged_total": credits_charged_total,
        "jobs_with_new_recruiter_profiles_found": len(jobs_with_profile),
        "lusha_error_count": len(lush_errors),
        "lusha_errors": lush_errors,
    }
    return rows, metrics


def _recruiter_titles() -> list[str]:
    raw = (os.getenv("LUSHA_RECRUITER_TITLES") or "").strip()
    if not raw:
        return list(DEFAULT_RECRUITER_TITLES)
    titles = [part.strip() for part in raw.split(",") if part.strip()]
    return titles or list(DEFAULT_RECRUITER_TITLES)


def _lusha_top_contacts_per_job() -> int:
    return max(1, int((os.getenv("LUSHA_TOP_CONTACTS_PER_JOB") or "1").strip() or "1"))


def _lusha_base_url() -> str:
    return (os.getenv("LUSHA_BASE_URL") or "https://api.lusha.com").strip().rstrip("/")


def _lusha_timeout_seconds() -> float:
    return max(1.0, float((os.getenv("LUSHA_TIMEOUT_SECONDS") or "20").strip() or "20"))


def _lusha_retry_count() -> int:
    return max(0, int((os.getenv("LUSHA_RETRY_COUNT") or "2").strip() or "2"))


def _lusha_headers() -> dict[str, str]:
    api_key = (os.getenv("LUSHA_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("LUSHA_API_KEY is required for recruiter profile backfill.")
    return {
        "accept": "application/json",
        "content-type": "application/json",
        # Keep both header variants for compatibility across plans.
        "api_key": api_key,
        "x-api-key": api_key,
    }


def _lusha_contact_search(
    *,
    company: str,
    location: str,
    recruiter_titles: list[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload: dict[str, Any] = {
        "pages": {"page": 0, "size": 10},
        "filters": {
            "contacts": {
                "include": {
                    "jobTitles": recruiter_titles,
                }
            },
            "companies": {
                "include": {
                    "names": [company],
                }
            },
        },
    }
    if location:
        payload["filters"]["contacts"]["include"]["locations"] = [location]

    response = _lusha_request_json(
        method="POST",
        path="/prospecting/contact/search",
        json_payload=payload,
    )
    return response, _extract_contact_candidates(response)


def _lusha_contact_enrich(*, request_id: str, contact_ids: list[str]) -> dict[str, Any]:
    payload = {"requestId": request_id, "contactIds": contact_ids}
    return _lusha_request_json(
        method="POST",
        path="/prospecting/contact/enrich",
        json_payload=payload,
    )


def _lusha_request_json(
    *,
    method: str,
    path: str,
    json_payload: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{_lusha_base_url()}{path}"
    retries = _lusha_retry_count()
    timeout = _lusha_timeout_seconds()
    headers = _lusha_headers()
    attempt = 0
    while True:
        attempt += 1
        try:
            response = requests.request(
                method=method,
                url=url,
                params=query,
                json=json_payload,
                headers=headers,
                timeout=timeout,
            )
            if response.status_code >= 400:
                raise RuntimeError(f"Lusha {method} {path} failed [{response.status_code}] {response.text[:400]}")
            data = response.json()
            return data if isinstance(data, dict) else {"data": data}
        except Exception:
            if attempt > retries + 1:
                raise


def _extract_contact_candidates(response: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for key in ("contacts", "results", "items", "data"):
        value = response.get(key)
        if isinstance(value, list):
            candidates.extend([item for item in value if isinstance(item, dict)])
    data_obj = response.get("data")
    if isinstance(data_obj, dict):
        for key in ("contacts", "results", "items"):
            value = data_obj.get(key)
            if isinstance(value, list):
                candidates.extend([item for item in value if isinstance(item, dict)])
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in candidates:
        cid = _extract_contact_id(item)
        marker = cid or str(item.get("id") or "")
        if not marker or marker in seen:
            continue
        seen.add(marker)
        deduped.append(item)
    return deduped


def _extract_contact_id(contact: dict[str, Any]) -> str:
    for key in ("contactId", "contact_id", "id"):
        value = contact.get(key)
        if value:
            return str(value).strip()
    return ""


def _extract_linkedin_url_from_enrich(contact: dict[str, Any]) -> str:
    data = contact.get("data")
    if isinstance(data, dict):
        social_links = data.get("socialLinks")
        if isinstance(social_links, dict):
            linkedin = social_links.get("linkedin")
            if isinstance(linkedin, str) and linkedin.strip():
                return linkedin.strip()
    linkedin_direct = contact.get("linkedinUrl")
    if isinstance(linkedin_direct, str) and linkedin_direct.strip():
        return linkedin_direct.strip()
    return ""


def _extract_name_from_enrich(contact: dict[str, Any], search_contact: dict[str, Any]) -> str:
    data = contact.get("data")
    if isinstance(data, dict):
        full_name = str(data.get("fullName") or "").strip()
        if full_name:
            return full_name
        first = str(data.get("firstName") or "").strip()
        last = str(data.get("lastName") or "").strip()
        combined = " ".join(part for part in (first, last) if part).strip()
        if combined:
            return combined
    return str(search_contact.get("name") or "").strip()


def _extract_headline_from_enrich(contact: dict[str, Any], search_contact: dict[str, Any]) -> str:
    data = contact.get("data")
    if isinstance(data, dict):
        title = data.get("jobTitle")
        if isinstance(title, str) and title.strip():
            return title.strip()
        if isinstance(title, dict):
            title_text = str(title.get("title") or "").strip()
            if title_text:
                return title_text
    return str(search_contact.get("jobTitle") or "").strip()


def _extract_enriched_contact(response: dict[str, Any], *, contact_id: str) -> dict[str, Any]:
    contacts = response.get("contacts")
    if not isinstance(contacts, list):
        return {}
    for item in contacts:
        if not isinstance(item, dict):
            continue
        if str(item.get("id") or "").strip() == contact_id:
            return item
    for item in contacts:
        if isinstance(item, dict):
            return item
    return {}


def _extract_credits_charged(response: dict[str, Any]) -> int:
    raw = response.get("creditsCharged")
    if raw is None:
        return 0
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return 0


def _append_recruiter_rows(
    *,
    tab_name: str,
    rows: list[dict[str, Any]],
    dedupe_existing_on: tuple[str, ...],
) -> int:
    if not rows:
        return 0
    spreadsheet_id = _require_spreadsheet_id()
    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    existing_rows = _read_rows_from_tab(tab_name, allow_missing=True)
    existing_keys = {_row_identity_key(row, dedupe_existing_on) for row in existing_rows}
    to_write: list[dict[str, Any]] = []
    for row in rows:
        key = _row_identity_key(row, dedupe_existing_on)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        to_write.append(row)
    if not to_write:
        return 0
    headers = _derive_headers(to_write)
    data_rows = [[_stringify_cell(row.get(col)) for col in headers] for row in to_write]
    chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
    writer.append_to_worksheet(
        worksheet_title=tab_name,
        data_rows=data_rows,
        header_row=headers,
        chunk_size=chunk_size,
    )
    return len(to_write)


def _row_identity_key(row: dict[str, Any], cols: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(row.get(col) or "").strip() for col in cols)


def _derive_headers(rows: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    headers: list[str] = []
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            headers.append(key)
    return headers or ["message"]


def _stringify_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        import json

        return json.dumps(value, ensure_ascii=True, default=str)
    return str(value)
