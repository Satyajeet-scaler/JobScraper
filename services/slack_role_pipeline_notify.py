from __future__ import annotations

import logging
from datetime import date
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import (
    load_internal_poc_tag_rows,
    load_owner_rows_for_handover,
    worksheet_row_dicts,
)
from services.role_pipeline import _role_slug
from services.role_recruiter_info_service import _role_recruiters_tab_name
from services.slack_handover_notify import (
    HEADING_INTERNAL_POC,
    HEADING_RECRUITER_DETAIL,
    format_internal_poc_lead,
    format_recruiter_detail_lead,
    internal_poc_email_owner_map,
    internal_poc_owner_tag_line,
    load_candidate_match_count_map,
    match_internal_poc_owners_ordered,
    owner_tag_for_handover,
    send_slack_text,
    slack_notify_defaults_from_env,
)

logger = logging.getLogger(__name__)


def send_role_handover_notifications(
    run_date: str | None = None,
    *,
    role: str | None = None,
    upstream_run_id: str | None = None,
) -> dict[str, Any]:
    resolved_date = (run_date or date.today().isoformat()).strip()
    resolved_role = (role or "").strip()
    if not resolved_role:
        raise ValueError("role is required.")
    role_slug = _role_slug(resolved_role)
    recruiters_tab = _role_recruiters_tab_name(role_slug=role_slug, run_date=resolved_date)
    defaults = slack_notify_defaults_from_env()
    out = {
        "run_date": resolved_date,
        "role": resolved_role,
        "role_slug": role_slug,
        "recruiters_tab": recruiters_tab,
        "recruiter_messages_sent": 0,
        "recruiter_detail_leads": 0,
        "internal_poc_leads": 0,
        "skipped_reason": None,
        "upstream_run_id": upstream_run_id or "",
    }
    if not defaults.webhook_url:
        out["skipped_reason"] = "SLACK_WEBHOOK_URL not configured"
        return out

    recruiter_rows = _read_role_recruiter_rows(recruiters_tab)
    if not recruiter_rows:
        out["skipped_reason"] = "no recruiter rows"
        return out

    case3, case2 = _split_recruiter_cases(
        recruiter_rows,
        resolved_date,
        upstream_run_id=upstream_run_id,
    )
    out["recruiter_detail_leads"] = len(case3)
    out["internal_poc_leads"] = len(case2)
    candidate_match_count_map = load_candidate_match_count_map(resolved_date)

    owner_rows = load_owner_rows_for_handover() or []
    if case3 and owner_rows:
        if send_slack_text(HEADING_RECRUITER_DETAIL, defaults=defaults, sleep_after=1.0):
            out["recruiter_messages_sent"] += 1
            owner_buckets: dict[int, list[dict[str, str]]] = {i: [] for i in range(len(owner_rows))}
            for idx, row in enumerate(case3):
                owner_buckets[idx % len(owner_rows)].append(row)
            for owner_idx, owner in enumerate(owner_rows):
                for row in owner_buckets.get(owner_idx, []):
                    tag = owner_tag_for_handover(owner)
                    company = (row.get("company") or "-").strip() or "-"
                    role_category = (row.get("role_category") or row.get("matched_role") or "-").strip() or "-"
                    job_url = (row.get("job_url") or "-").strip() or "-"
                    profile_url = (row.get("recruiter_profile_url") or "-").strip() or "-"
                    count = candidate_match_count_map.get(_normalize_job_key(job_url), 0)
                    msg = format_recruiter_detail_lead(tag, company, role_category, job_url, profile_url, count)
                    if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                        out["recruiter_messages_sent"] += 1

    if case2:
        if send_slack_text(HEADING_INTERNAL_POC, defaults=defaults, sleep_after=1.0):
            out["recruiter_messages_sent"] += 1
            poc_email_map = internal_poc_email_owner_map(load_internal_poc_tag_rows())
            for row in case2:
                raw_email = (row.get("recruiter_email") or "").strip()
                matched_owners = match_internal_poc_owners_ordered(raw_email, poc_email_map)
                tag = internal_poc_owner_tag_line(matched_owners)
                company = (row.get("company") or "-").strip() or "-"
                role_category = (row.get("role_category") or row.get("matched_role") or "-").strip() or "-"
                job_url = (row.get("job_url") or "-").strip() or "-"
                count = candidate_match_count_map.get(_normalize_job_key(job_url), 0)
                msg = format_internal_poc_lead(tag, company, role_category, job_url, raw_email or "-", count)
                if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                    out["recruiter_messages_sent"] += 1

    return out


def _read_role_recruiter_rows(tab: str) -> list[dict[str, str]]:
    import os

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        return []
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(tab)
        raw = writer.worksheet_get_all_values(ws, f"role_slack_handover:{tab}:get_all_values")
    except Exception as exc:
        logger.warning("role slack handover: recruiters tab unavailable tab=%s err=%s", tab, exc)
        return []
    return [dict(r) for r in worksheet_row_dicts(raw)]


def _split_recruiter_cases(
    rows: list[dict[str, str]],
    run_date: str,
    *,
    upstream_run_id: str | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    case3: list[dict[str, str]] = []
    case2: list[dict[str, str]] = []
    for row in rows:
        row_run_date = (row.get("run_date") or "").strip()
        if row_run_date and row_run_date != run_date:
            continue
        if upstream_run_id:
            row_upstream = (row.get("role_pipeline_upstream_run_id") or "").strip()
            if row_upstream != upstream_run_id:
                continue
        profile = (row.get("recruiter_profile_url") or "").strip()
        email = (row.get("recruiter_email") or "").strip()
        if profile:
            case3.append(row)
        elif email:
            case2.append(row)
    return case3, case2


def _normalize_job_key(url: str) -> str:
    from services.slack_handover_notify import _normalize_job_url_for_match

    return _normalize_job_url_for_match(url)

