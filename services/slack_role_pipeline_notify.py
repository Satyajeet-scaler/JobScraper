from __future__ import annotations

import logging
from datetime import date
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import load_owner_rows_for_handover, worksheet_row_dicts
from services.role_pipeline import _role_slug
from services.role_recruiter_info_service import role_recruiters_tab_name_for_role
from services.mysql_jobs_store import fetch_recruiter_rows_for_role, mark_recruiter_contacts_handover_sent
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
    """Post Case 3 (recruiter LinkedIn profile) Slack handovers for a role.

    Internal POC (Case 2) Slack messages are intentionally not sent from this
    notifier; use the global ``send_handover_notifications`` if needed.
    """
    resolved_date = (run_date or date.today().isoformat()).strip()
    resolved_role = (role or "").strip()
    if not resolved_role:
        raise ValueError("role is required.")
    role_slug = _role_slug(resolved_role)
    recruiters_tab = role_recruiters_tab_name_for_role(role=resolved_role, run_date=resolved_date)
    defaults = slack_notify_defaults_from_env()
    min_candidate_match = _resolve_min_candidate_match(resolved_role)
    include_cm_slack = _role_includes_candidate_match_in_slack(resolved_role)
    out = {
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

    recruiter_rows = _read_role_recruiter_rows(
        recruiters_tab,
        role=resolved_role,
        run_date=resolved_date,
        upstream_run_id=upstream_run_id,
    )
    if not recruiter_rows:
        out["skipped_reason"] = "no recruiter rows"
        return out

    case3 = _split_recruiter_case3_rows(
        recruiter_rows,
        resolved_date,
        upstream_run_id=upstream_run_id,
    )
    case3 = [row for row in case3 if not _is_handover_sent(row)]
    candidate_match_count_map = load_candidate_match_count_map_for_role(
        role=resolved_role,
        run_date=resolved_date,
    )
    n_case3_before = len(case3)
    case3 = [
        row
        for row in case3
        if _row_meets_recruiter_handover_threshold(
            row, candidate_match_count_map, min_candidate_match
        )
    ]
    if n_case3_before != len(case3):
        logger.info(
            "role slack handover: role=%s min_candidate_match=%s recruiter cases case3 %s->%s",
            resolved_role,
            min_candidate_match,
            n_case3_before,
            len(case3),
        )
    out["recruiter_detail_leads"] = len(case3)

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
    if sent_case3_keys:
        out["handover_sent_rows_updated"] = mark_recruiter_contacts_handover_sent(
            role=resolved_role,
            run_date=resolved_date,
            identities=sent_case3_keys,
        )
    else:
        out["handover_sent_rows_updated"] = 0

    return out


def _read_role_recruiter_rows(
    tab: str,
    *,
    role: str,
    run_date: str,
    upstream_run_id: str | None = None,
) -> list[dict[str, str]]:
    import os

    use_mysql = (os.getenv("ROLE_PIPELINE_MYSQL_READ_ENABLED") or "false").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if use_mysql:
        rows = fetch_recruiter_rows_for_role(role=role, run_date=run_date, upstream_run_id=upstream_run_id)
        return [dict(r) for r in rows]

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


def _split_recruiter_case3_rows(
    rows: list[dict[str, str]],
    run_date: str,
    *,
    upstream_run_id: str | None = None,
) -> list[dict[str, str]]:
    case3: list[dict[str, str]] = []
    for row in rows:
        row_run_date = (row.get("run_date") or "").strip()
        if row_run_date and row_run_date != run_date:
            continue
        if upstream_run_id:
            row_upstream = (row.get("role_pipeline_upstream_run_id") or "").strip()
            if row_upstream and row_upstream != upstream_run_id:
                continue
        profile = (row.get("recruiter_profile_url") or "").strip()
        if profile:
            case3.append(row)
    return case3


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
