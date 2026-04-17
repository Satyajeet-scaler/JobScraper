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
from services.role_recruiter_info_service import role_recruiters_tab_name_for_role
from services.slack_relevant_jobs_handover import _resolve_min_candidate_match
from services.slack_handover_notify import (
    HEADING_INTERNAL_POC,
    HEADING_RECRUITER_DETAIL,
    format_internal_poc_lead,
    format_recruiter_detail_lead,
    internal_poc_email_owner_map,
    internal_poc_owner_tag_line,
    load_candidate_match_count_map_for_role,
    match_internal_poc_owners_ordered,
    owner_tag_for_handover,
    persist_assigned_owner_from_email_map,
    persist_assigned_owner_round_robin,
    recruiter_row_role_label_for_slack,
    send_slack_text,
    slack_notify_defaults_from_env,
)

logger = logging.getLogger(__name__)


def send_role_handover_notifications(
    run_date: str | None = None,
    *,
    role: str | None = None,
    upstream_run_id: str | None = None,
    send_recruiter_info: bool = True,
    send_internal_poc: bool = True,
) -> dict[str, Any]:
    resolved_date = (run_date or date.today().isoformat()).strip()
    resolved_role = (role or "").strip()
    if not resolved_role:
        raise ValueError("role is required.")
    role_slug = _role_slug(resolved_role)
    recruiters_tab = role_recruiters_tab_name_for_role(role=resolved_role, run_date=resolved_date)
    defaults = slack_notify_defaults_from_env()
    min_candidate_match = _resolve_min_candidate_match(resolved_role)
    out = {
        "run_date": resolved_date,
        "role": resolved_role,
        "role_slug": role_slug,
        "recruiters_tab": recruiters_tab,
        "min_candidate_match": min_candidate_match,
        "recruiter_messages_sent": 0,
        "recruiter_detail_leads": 0,
        "internal_poc_leads": 0,
        "skipped_reason": None,
        "upstream_run_id": upstream_run_id or "",
        "assigned_owner_rows_updated": 0,
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
    case3 = [row for row in case3 if _is_assigned_owner_empty(row)]
    case2 = [row for row in case2 if _is_assigned_owner_empty(row)]
    candidate_match_count_map = load_candidate_match_count_map_for_role(
        role=resolved_role,
        run_date=resolved_date,
    )
    n_case3_before = len(case3)
    n_case2_before = len(case2)
    case3 = [
        row
        for row in case3
        if _row_meets_recruiter_handover_threshold(
            row, candidate_match_count_map, min_candidate_match
        )
    ]
    case2 = [
        row
        for row in case2
        if _row_meets_recruiter_handover_threshold(
            row, candidate_match_count_map, min_candidate_match
        )
    ]
    if n_case3_before != len(case3) or n_case2_before != len(case2):
        logger.info(
            "role slack handover: role=%s min_candidate_match=%s recruiter cases "
            "case3 %s->%s case2 %s->%s",
            resolved_role,
            min_candidate_match,
            n_case3_before,
            len(case3),
            n_case2_before,
            len(case2),
        )
    out["recruiter_detail_leads"] = len(case3)
    out["internal_poc_leads"] = len(case2)

    owner_rows = load_owner_rows_for_handover() or []
    sent_case3_keys: set[tuple[str, str, str, str]] = set()
    if send_recruiter_info and case3 and owner_rows:
        if send_slack_text(HEADING_RECRUITER_DETAIL, defaults=defaults, sleep_after=1.0):
            out["recruiter_messages_sent"] += 1
            owner_buckets: dict[int, list[dict[str, str]]] = {i: [] for i in range(len(owner_rows))}
            for idx, row in enumerate(case3):
                owner_buckets[idx % len(owner_rows)].append(row)
            for owner_idx, owner in enumerate(owner_rows):
                for row in owner_buckets.get(owner_idx, []):
                    tag = owner_tag_for_handover(owner)
                    company = (row.get("company") or "-").strip() or "-"
                    role_category = recruiter_row_role_label_for_slack(row)
                    job_url = (row.get("job_url") or "-").strip() or "-"
                    profile_url = (row.get("recruiter_profile_url") or "-").strip() or "-"
                    count = candidate_match_count_map.get(_normalize_job_key(job_url), 0)
                    msg = format_recruiter_detail_lead(tag, company, role_category, job_url, profile_url, count)
                    if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                        out["recruiter_messages_sent"] += 1
                        sent_case3_keys.add(_recruiter_row_identity(row))

    spreadsheet_id = _google_spreadsheet_id()
    if sent_case3_keys and owner_rows and spreadsheet_id:
        persist_assigned_owner_round_robin(
            spreadsheet_id=spreadsheet_id,
            worksheet_title=recruiters_tab,
            owner_rows=owner_rows,
            selector=lambda row: _is_recruiter_case3_selected(
                row,
                run_date=resolved_date,
                upstream_run_id=upstream_run_id,
                selected_keys=sent_case3_keys,
            ),
        )
        out["assigned_owner_rows_updated"] += len(sent_case3_keys)

    sent_case2_keys: set[tuple[str, str, str]] = set()
    if send_internal_poc and case2:
        if send_slack_text(HEADING_INTERNAL_POC, defaults=defaults, sleep_after=1.0):
            out["recruiter_messages_sent"] += 1
            poc_email_map = internal_poc_email_owner_map(load_internal_poc_tag_rows())
            for row in case2:
                raw_email = (row.get("recruiter_email") or "").strip()
                matched_owners = match_internal_poc_owners_ordered(raw_email, poc_email_map)
                tag = internal_poc_owner_tag_line(matched_owners)
                company = (row.get("company") or "-").strip() or "-"
                role_category = recruiter_row_role_label_for_slack(row)
                job_url = (row.get("job_url") or "-").strip() or "-"
                count = candidate_match_count_map.get(_normalize_job_key(job_url), 0)
                msg = format_internal_poc_lead(tag, company, role_category, job_url, raw_email or "-", count)
                if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                    out["recruiter_messages_sent"] += 1
                    sent_case2_keys.add(_recruiter_case2_identity(row))
            if sent_case2_keys and spreadsheet_id:
                persist_assigned_owner_from_email_map(
                    spreadsheet_id=spreadsheet_id,
                    worksheet_title=recruiters_tab,
                    email_map=poc_email_map,
                    selector=lambda row: _is_recruiter_case2_selected(
                        row,
                        run_date=resolved_date,
                        upstream_run_id=upstream_run_id,
                        selected_keys=sent_case2_keys,
                    ),
                )
                out["assigned_owner_rows_updated"] += len(sent_case2_keys)

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


def _row_meets_recruiter_handover_threshold(
    row: dict[str, str],
    candidate_match_count_map: dict[str, int],
    min_count: int,
) -> bool:
    """Filter recruiter-sheet rows by ``ai_score_gt_70_count`` (from candidate_match tab)."""
    job_url = (row.get("job_url") or "").strip()
    if not job_url or job_url == "-":
        return min_count <= 0
    count = candidate_match_count_map.get(_normalize_job_key(job_url), 0)
    return count >= min_count


def _is_assigned_owner_empty(row: dict[str, str]) -> bool:
    return not str(row.get("assigned owner") or row.get("assigned_owner") or "").strip()


def _google_spreadsheet_id() -> str:
    import os

    return (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()


def _recruiter_row_identity(row: dict[str, str]) -> tuple[str, str, str, str]:
    job = _normalize_job_key(str(row.get("job_url") or ""))
    profile = str(row.get("recruiter_profile_url") or "").strip().lower()
    email = str(row.get("recruiter_email") or "").strip().lower()
    source = str(row.get("recruiter_source") or "").strip().lower()
    return (job, profile, email, source)


def _recruiter_case2_identity(row: dict[str, str]) -> tuple[str, str, str]:
    job = _normalize_job_key(str(row.get("job_url") or ""))
    email = str(row.get("recruiter_email") or "").strip().lower()
    source = str(row.get("recruiter_source") or "").strip().lower()
    return (job, email, source)


def _is_recruiter_case3_selected(
    row: dict[str, str],
    *,
    run_date: str,
    upstream_run_id: str | None,
    selected_keys: set[tuple[str, str, str, str]],
) -> bool:
    row_date = (row.get("run_date") or "").strip()
    if row_date and row_date != run_date:
        return False
    if upstream_run_id:
        if (row.get("role_pipeline_upstream_run_id") or "").strip() != upstream_run_id:
            return False
    if not (row.get("recruiter_profile_url") or "").strip():
        return False
    if not _is_assigned_owner_empty(row):
        return False
    key = _recruiter_row_identity(row)
    return key in selected_keys


def _is_recruiter_case2_selected(
    row: dict[str, str],
    *,
    run_date: str,
    upstream_run_id: str | None,
    selected_keys: set[tuple[str, str, str]],
) -> bool:
    row_date = (row.get("run_date") or "").strip()
    if row_date and row_date != run_date:
        return False
    if upstream_run_id:
        if (row.get("role_pipeline_upstream_run_id") or "").strip() != upstream_run_id:
            return False
    has_profile = bool((row.get("recruiter_profile_url") or "").strip())
    has_email = bool((row.get("recruiter_email") or "").strip())
    if has_profile or not has_email:
        return False
    if not _is_assigned_owner_empty(row):
        return False
    key = _recruiter_case2_identity(row)
    return key in selected_keys

