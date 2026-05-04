from __future__ import annotations

import logging
from datetime import date
from typing import Any

from services.handover_owners import load_owner_rows_for_handover
from services.handover_owner_state import get_start_owner_index, update_last_owner
from services.role_pipeline import _role_slug
from services.role_recruiter_info_service import role_recruiters_tab_name_for_role
from services.mysql_jobs_store import mark_recruiter_contacts_handover_sent
from services.slack_relevant_jobs_handover import (
    _resolve_min_candidate_match,
    _role_includes_candidate_match_in_slack,
)
from services.slack_handover_notify import (
    HEADING_RECRUITER_DETAIL,
    format_recruiter_detail_lead,
    load_candidate_match_count_map_for_role,
    owner_tag_for_handover,
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
) -> dict[str, Any]:
    """Post LinkedIn Meet The Team (Case 3) Slack handovers for a role."""
    resolved_date = (run_date or date.today().isoformat()).strip()
    resolved_role = (role or "").strip()
    if not resolved_role:
        raise ValueError("role is required.")
    role_slug = _role_slug(resolved_role)
    recruiters_tab = role_recruiters_tab_name_for_role(role=resolved_role, run_date=resolved_date)
    defaults = slack_notify_defaults_from_env()
    min_candidate_match = _resolve_min_candidate_match(resolved_role)
    include_cm_slack = _role_includes_candidate_match_in_slack(resolved_role)
    out: dict[str, Any] = {
        "run_date": resolved_date,
        "role": resolved_role,
        "role_slug": role_slug,
        "recruiters_tab": recruiters_tab,
        "min_candidate_match": min_candidate_match,
        "recruiter_messages_sent": 0,
        "recruiter_detail_leads": 0,
        "skipped_reason": None,
        "upstream_run_id": upstream_run_id or "",
        "assigned_owner_rows_updated": 0,
    }
    if not defaults.webhook_url:
        out["skipped_reason"] = "SLACK_WEBHOOK_URL not configured"
        return out

    # Fetch directly from MySQL (combining case3 recruiter contacts + AI candidate matches)
    from services.mysql_jobs_store import fetch_pending_recruiter_slack_handovers, mark_recruiter_contacts_handover_sent
    from services.slack_relevant_jobs_handover import _owner_display_name

    case3_raw = fetch_pending_recruiter_slack_handovers(role=resolved_role, run_date=resolved_date)
    if not case3_raw:
        out["skipped_reason"] = "no pending recruiter handovers found in db"
        return out

    case3 = []
    for db_row in case3_raw:
        count = int(db_row.get("_candidate_match_count") or 0)
        if count >= min_candidate_match:
            case3.append(db_row)

    out["recruiter_detail_leads"] = len(case3)
    owner_rows = load_owner_rows_for_handover() or []
    sent_assignments: list[tuple[str, int]] = []

    if send_recruiter_info and case3 and owner_rows:
        state_key = f"handover:role_recruiter:{role_slug}"
        start_index = get_start_owner_index(state_key, owner_rows)
        if send_slack_text(HEADING_RECRUITER_DETAIL, defaults=defaults, sleep_after=1.0):
            out["recruiter_messages_sent"] += 1
            owner_buckets: dict[int, list[dict[str, Any]]] = {i: [] for i in range(len(owner_rows))}
            for idx, row in enumerate(case3):
                owner_buckets[(start_index + idx) % len(owner_rows)].append(row)
            last_assigned_index = -1
            for owner_idx, owner in enumerate(owner_rows):
                owner_name = _owner_display_name(owner)
                bucket = owner_buckets.get(owner_idx, [])
                if not bucket:
                    continue
                bucket_sent = False
                for row in bucket:
                    tag = owner_tag_for_handover(owner)
                    company = (row.get("company") or "-").strip() or "-"
                    role_category = recruiter_row_role_label_for_slack(row)
                    job_url = (row.get("job_url") or "-").strip() or "-"
                    profile_url = (row.get("recruiter_profile_url") or "-").strip() or "-"
                    count = int(row.get("_candidate_match_count") or 0)
                    msg = format_recruiter_detail_lead(
                        tag,
                        company,
                        role_category,
                        job_url,
                        profile_url,
                        count,
                        include_candidate_match=include_cm_slack,
                    )
                    if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                        out["recruiter_messages_sent"] += 1
                        bucket_sent = True
                        sent_assignments.append((row["_rc_id"], owner_name))
                if bucket_sent:
                    last_assigned_index = owner_idx
            if last_assigned_index != -1:
                update_last_owner(state_key, owner_rows, last_assigned_index)

    if sent_assignments:
        out["assigned_owner_rows_updated"] = len(sent_assignments)
        out["handover_sent_rows_updated"] = mark_recruiter_contacts_handover_sent(sent_assignments)
    else:
        out["handover_sent_rows_updated"] = 0

    return out


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


def _is_handover_sent(row: dict[str, str]) -> bool:
    return str(row.get("handover_sent") or "").strip().lower() in ("1", "true", "yes")


def _google_spreadsheet_id() -> str:
    import os

    return (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()


def _recruiter_row_identity(row: dict[str, str]) -> tuple[str, str, str, str]:
    job = _normalize_job_key(str(row.get("job_url") or ""))
    profile = str(row.get("recruiter_profile_url") or "").strip().lower()
    email = str(row.get("recruiter_email") or "").strip().lower()
    source = str(row.get("recruiter_source") or "").strip().lower()
    return (job, profile, email, source)


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
    key = _recruiter_row_identity(row)
    return key in selected_keys
