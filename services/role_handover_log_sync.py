"""Append role-pipeline handover leads to the shared handover log sheet."""

from __future__ import annotations

import logging
import os
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_log_sync import HANDOVER_LOG_HEADER
from services.handover_owners import worksheet_row_dicts
from services.linkedin_posts_slack_row import slack_post_url_from_row
from services.role_linkedin_posts_pipeline import _role_linkedin_relevant_tab_name
from services.role_pipeline import _role_slug
from services.role_recruiter_info_service import _role_recruiters_tab_name

logger = logging.getLogger(__name__)


def _load_rows(tab: str) -> list[dict[str, Any]]:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        return []
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(tab)
        raw = writer.worksheet_get_all_values(ws, f"role_handover_log_sync:{tab}:get_all_values")
    except Exception as exc:
        logger.warning("role_handover_log_sync tab unavailable tab=%s err=%s", tab, exc)
        return []
    return [dict(r) for r in worksheet_row_dicts(raw)]


def _recruiter_row_to_log_cells(row: dict[str, Any]) -> list[str]:
    def g(key: str) -> str:
        return str(row.get(key) or "").strip()

    return [
        g("run_date"),
        g("job_url"),
        g("company"),
        g("title"),
        g("assigned owner"),
        "",
        "",
        "",
    ]


def _linkedin_row_to_log_cells(row: dict[str, Any]) -> list[str]:
    run_date = str(row.get("run_date") or "").strip()
    link = slack_post_url_from_row(row).strip()
    if link in ("", "-"):
        link = str(row.get("post_url") or "").strip()
    owner = str(row.get("assigned owner") or "").strip()
    return [run_date, link, "NA", "NA", owner, "", "", ""]


def _log_row_key(cells: list[str]) -> tuple[str, str, str, str, str]:
    """Stable uniqueness key for handover log append dedupe."""
    padded = list(cells) + [""] * max(0, 5 - len(cells))
    return tuple(str(padded[idx] or "").strip() for idx in range(5))


def _load_existing_log_keys(*, log_id: str, worksheet_name: str) -> set[tuple[str, str, str, str, str]]:
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=log_id)
        ws = writer.open_worksheet(worksheet_name)
        raw = writer.worksheet_get_all_values(ws, f"role_handover_log_sync_existing:{worksheet_name}:get_all_values")
    except Exception:
        return set()
    rows = worksheet_row_dicts(raw)
    out: set[tuple[str, str, str, str, str]] = set()
    for row in rows:
        key = (
            str(row.get("Date") or "").strip(),
            str(row.get("Link to Job") or "").strip(),
            str(row.get("Company name") or "").strip(),
            str(row.get("Title") or "").strip(),
            str(row.get("Owner") or "").strip(),
        )
        if any(key):
            out.add(key)
    return out


def sync_role_handover_log_to_sheet(*, run_date: str, role: str) -> dict[str, Any]:
    """
    Append role recruiter + role LinkedIn relevant rows to HANDOVER_LOG sheet.
    Uses the same target schema/header as the original handover log sync.
    """
    log_id = (os.getenv("HANDOVER_LOG_SPREADSHEET_ID") or "").strip()
    if not log_id:
        return {"skipped": True, "reason": "HANDOVER_LOG_SPREADSHEET_ID not set"}

    worksheet_name = (os.getenv("HANDOVER_LOG_WORKSHEET_NAME") or "Handover log").strip() or "Handover log"
    main_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not main_id:
        return {"skipped": True, "reason": "GOOGLE_SPREADSHEET_ID not set"}

    resolved_role = (role or "").strip()
    if not resolved_role:
        return {"skipped": True, "reason": "role is required"}
    role_slug = _role_slug(resolved_role)

    recruiters_tab = _role_recruiters_tab_name(role_slug=role_slug, run_date=run_date)
    linkedin_tab = _role_linkedin_relevant_tab_name(role_slug=role_slug, run_date=run_date)
    recruiter_rows = [r for r in _load_rows(recruiters_tab) if (str(r.get("run_date") or "").strip() in ("", run_date))]
    linkedin_rows = [r for r in _load_rows(linkedin_tab) if (str(r.get("run_date") or "").strip() in ("", run_date))]

    data_rows: list[list[str]] = []
    for row in recruiter_rows:
        data_rows.append(_recruiter_row_to_log_cells(row))
    for row in linkedin_rows:
        data_rows.append(_linkedin_row_to_log_cells(row))

    existing_keys = _load_existing_log_keys(log_id=log_id, worksheet_name=worksheet_name)
    new_rows: list[list[str]] = []
    for row in data_rows:
        key = _log_row_key(row)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_rows.append(row)

    if not new_rows:
        logger.info(
            "role_handover_log_sync run_date=%s role=%s no new rows (recruiters=%s linkedin=%s)",
            run_date,
            resolved_role,
            len(recruiter_rows),
            len(linkedin_rows),
        )
        return {
            "skipped": False,
            "run_date": run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "recruiters_tab": recruiters_tab,
            "linkedin_tab": linkedin_tab,
            "recruiter_handover_rows": len(recruiter_rows),
            "linkedin_relevant_rows": len(linkedin_rows),
            "candidate_rows": len(data_rows),
            "rows_appended": 0,
        }

    try:
        writer = GoogleSheetsWriter(spreadsheet_id=log_id)
        writer.append_to_worksheet(
            worksheet_name,
            new_rows,
            header_row=HANDOVER_LOG_HEADER,
        )
    except Exception as exc:
        logger.exception("role_handover_log_sync append failed run_date=%s role=%s err=%s", run_date, resolved_role, exc)
        return {
            "skipped": False,
            "error": str(exc),
            "run_date": run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "recruiters_tab": recruiters_tab,
            "linkedin_tab": linkedin_tab,
            "recruiter_handover_rows": len(recruiter_rows),
            "linkedin_relevant_rows": len(linkedin_rows),
            "candidate_rows": len(data_rows),
            "rows_appended": 0,
        }

    logger.info(
        "role_handover_log_sync appended run_date=%s role=%s rows=%s (recruiters=%s linkedin=%s) sheet=%s tab=%s",
        run_date,
        resolved_role,
        len(new_rows),
        len(recruiter_rows),
        len(linkedin_rows),
        log_id,
        worksheet_name,
    )
    return {
        "skipped": False,
        "run_date": run_date,
        "role": resolved_role,
        "role_slug": role_slug,
        "recruiters_tab": recruiters_tab,
        "linkedin_tab": linkedin_tab,
        "recruiter_handover_rows": len(recruiter_rows),
        "linkedin_relevant_rows": len(linkedin_rows),
        "candidate_rows": len(data_rows),
        "rows_appended": len(new_rows),
        "worksheet": worksheet_name,
    }

