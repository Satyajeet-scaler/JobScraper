"""Append handover leads to a separate Google Sheet after Slack handover (assigned owner columns populated)."""

from __future__ import annotations

import logging
import os
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.linkedin_posts_slack_row import slack_post_url_from_row
from services.slack_handover_notify import (
    linkedin_posts_relevant_tab_name,
    load_recruiter_rows_split_for_handover,
)

logger = logging.getLogger(__name__)

HANDOVER_LOG_HEADER: list[str] = [
    "Date",
    "Link to Job",
    "Company name",
    "Title",
    "Owner",
    "Relevant",
    "Status",
    "Job Owner",
]


def _load_linkedin_relevant_rows(run_date: str) -> list[dict[str, Any]]:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        return []
    tab = linkedin_posts_relevant_tab_name(run_date)
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.sheet.worksheet(tab)
        rows = worksheet_row_dicts(ws)
        logger.info("handover_log_sync loaded %s linkedin relevant rows from %s", len(rows), tab)
        return list(rows)
    except Exception as exc:
        logger.warning("handover_log_sync linkedin tab unavailable tab=%s err=%s", tab, exc)
        return []


def _recruiter_row_to_log_cells(row: dict[str, str]) -> list[str]:
    def g(key: str) -> str:
        return (row.get(key) or "").strip()

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
    # Company name + Title are not used for LinkedIn post leads; sheet convention is NA.
    return [run_date, link, "NA", "NA", owner, "", "", ""]


def sync_handover_log_to_sheet(run_date: str) -> dict[str, Any]:
    """
    Read ``recruiters_info_*`` (Case 2 + Case 3 rows) and ``linkedin_posts_relevant_*``,
    map to eight columns, append to ``HANDOVER_LOG_SPREADSHEET_ID`` / ``HANDOVER_LOG_WORKSHEET_NAME``.

    No-op if ``HANDOVER_LOG_SPREADSHEET_ID`` is unset.
    """
    log_id = (os.getenv("HANDOVER_LOG_SPREADSHEET_ID") or "").strip()
    if not log_id:
        return {"skipped": True, "reason": "HANDOVER_LOG_SPREADSHEET_ID not set"}

    worksheet_name = (os.getenv("HANDOVER_LOG_WORKSHEET_NAME") or "Handover log").strip() or "Handover log"

    main_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not main_id:
        return {"skipped": True, "reason": "GOOGLE_SPREADSHEET_ID not set"}

    _, case3, case2 = load_recruiter_rows_split_for_handover(run_date)
    recruiter_rows = case3 + case2
    linkedin_rows = _load_linkedin_relevant_rows(run_date)

    data_rows: list[list[str]] = []
    for row in recruiter_rows:
        data_rows.append(_recruiter_row_to_log_cells(row))
    for row in linkedin_rows:
        data_rows.append(_linkedin_row_to_log_cells(row))

    if not data_rows:
        logger.info(
            "handover_log_sync run_date=%s no rows to append (recruiters=%s linkedin=%s)",
            run_date,
            len(recruiter_rows),
            len(linkedin_rows),
        )
        return {
            "skipped": False,
            "run_date": run_date,
            "recruiter_handover_rows": len(recruiter_rows),
            "linkedin_relevant_rows": len(linkedin_rows),
            "rows_appended": 0,
        }

    try:
        writer = GoogleSheetsWriter(spreadsheet_id=log_id)
        writer.append_to_worksheet(
            worksheet_name,
            data_rows,
            header_row=HANDOVER_LOG_HEADER,
        )
    except Exception as exc:
        logger.exception("handover_log_sync append failed run_date=%s err=%s", run_date, exc)
        return {
            "skipped": False,
            "error": str(exc),
            "run_date": run_date,
            "recruiter_handover_rows": len(recruiter_rows),
            "linkedin_relevant_rows": len(linkedin_rows),
            "rows_appended": 0,
        }

    logger.info(
        "handover_log_sync appended run_date=%s rows=%s (recruiters=%s linkedin=%s) sheet=%s tab=%s",
        run_date,
        len(data_rows),
        len(recruiter_rows),
        len(linkedin_rows),
        log_id,
        worksheet_name,
    )
    return {
        "skipped": False,
        "run_date": run_date,
        "recruiter_handover_rows": len(recruiter_rows),
        "linkedin_relevant_rows": len(linkedin_rows),
        "rows_appended": len(data_rows),
        "worksheet": worksheet_name,
    }
