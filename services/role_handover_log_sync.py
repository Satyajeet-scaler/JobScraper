"""Append role-pipeline handover leads to the shared handover log sheet using table reads only."""

from __future__ import annotations

import logging
import os
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_log_sync import HANDOVER_LOG_HEADER, _get_owner, _recruiter_row_to_log_cells
from services.handover_owners import worksheet_row_dicts
from services.linkedin_posts_slack_row import slack_post_url_from_row
from services.mysql_jobs_store import fetch_unsynced_recruiter_rows_for_role, mark_recruiter_contacts_log_synced
from services.mysql_linkedin_posts_store import fetch_unsynced_relevant_linkedin_posts_for_role, mark_linkedin_posts_log_synced
from services.role_pipeline import _role_slug

logger = logging.getLogger(__name__)


def _linkedin_row_to_log_cells(row: dict[str, Any]) -> list[str]:
    run_date = str(row.get("run_date") or "").strip()
    link = slack_post_url_from_row(row).strip()
    if link in ("", "-"):
        link = str(row.get("post_url") or "").strip()
    owner = _get_owner(row)
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
    Append role recruiter rows + role LinkedIn rows to HANDOVER_LOG sheet.

    Reads unsynced rows directly from MySQL tables (job_recruiter_contacts and
    linkedin_post_relevance) and marks them as synced after a successful sheet append.
    """
    log_id = (os.getenv("HANDOVER_LOG_SPREADSHEET_ID") or "").strip()
    if not log_id:
        return {"skipped": True, "reason": "HANDOVER_LOG_SPREADSHEET_ID not set"}

    worksheet_name = (os.getenv("HANDOVER_LOG_WORKSHEET_NAME") or "Handover log").strip() or "Handover log"

    resolved_role = (role or "").strip()
    if not resolved_role:
        return {"skipped": True, "reason": "role is required"}
    role_slug = _role_slug(resolved_role)

    recruiter_rows_for_log = fetch_unsynced_recruiter_rows_for_role(
        role=resolved_role,
        run_date=run_date,
    )
    try:
        linkedin_rows = fetch_unsynced_relevant_linkedin_posts_for_role(
            role=resolved_role,
            run_date=run_date,
        )
    except Exception as exc:
        logger.warning("role_handover_log_sync: failed to load linkedin rows from mysql role=%s err=%s", resolved_role, exc)
        linkedin_rows = []

    data_rows: list[list[str]] = []
    for row in recruiter_rows_for_log:
        data_rows.append(_recruiter_row_to_log_cells(dict(row)))
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

    summary_base = {
        "run_date": run_date,
        "role": resolved_role,
        "role_slug": role_slug,
        "recruiter_rows_for_log": len(recruiter_rows_for_log),
        "linkedin_relevant_rows": len(linkedin_rows),
        "candidate_rows": len(data_rows),
    }

    if not new_rows:
        logger.info(
            "role_handover_log_sync run_date=%s role=%s no new rows (recruiters=%s linkedin=%s)",
            run_date,
            resolved_role,
            len(recruiter_rows_for_log),
            len(linkedin_rows),
        )
        return {"skipped": False, **summary_base, "rows_appended": 0}

    try:
        writer = GoogleSheetsWriter(spreadsheet_id=log_id)
        writer.append_to_worksheet(
            worksheet_name,
            new_rows,
            header_row=HANDOVER_LOG_HEADER,
        )
    except Exception as exc:
        logger.exception(
            "role_handover_log_sync append failed run_date=%s role=%s err=%s",
            run_date,
            resolved_role,
            exc,
        )
        return {"skipped": False, "error": str(exc), **summary_base, "rows_appended": 0}

    # Mark rows as synced in MySQL after successful sheet append
    rc_ids = [int(row["_rc_id"]) for row in recruiter_rows_for_log if row.get("_rc_id")]
    if rc_ids:
        synced_count = mark_recruiter_contacts_log_synced(rc_ids=rc_ids)
        logger.info("role_handover_log_sync marked %s recruiter contacts as synced", synced_count)

    post_ids = [int(row["linkedin_post_id"]) for row in linkedin_rows if row.get("linkedin_post_id")]
    if post_ids:
        synced_count = mark_linkedin_posts_log_synced(post_ids=post_ids)
        logger.info("role_handover_log_sync marked %s linkedin posts as synced", synced_count)

    logger.info(
        "role_handover_log_sync appended run_date=%s role=%s rows=%s (recruiters=%s linkedin=%s) sheet=%s tab=%s",
        run_date,
        resolved_role,
        len(new_rows),
        len(recruiter_rows_for_log),
        len(linkedin_rows),
        log_id,
        worksheet_name,
    )
    return {
        "skipped": False,
        **summary_base,
        "rows_appended": len(new_rows),
        "worksheet": worksheet_name,
    }
