"""Shared owner sheet loading for round-robin Slack handover (jobs + LinkedIn posts)."""

from __future__ import annotations

import logging
import os
from typing import Any

from services.google_sheets import GoogleSheetsWriter

logger = logging.getLogger(__name__)


def worksheet_row_dicts(worksheet: Any) -> list[dict[str, str]]:
    values = worksheet.get_all_values()
    if len(values) <= 1:
        return []
    headers = [str(h or "").strip() for h in values[0]]
    out: list[dict[str, str]] = []
    for raw_row in values[1:]:
        row: dict[str, str] = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            row[header.strip().lower()] = raw_row[idx].strip() if idx < len(raw_row) else ""
        out.append(row)
    return out


def load_owner_rows_for_handover() -> list[dict[str, str]] | None:
    """
    Read ``OWNER_SHEET_NAME`` (default ``owner_slack_ID``) from ``GOOGLE_SPREADSHEET_ID``.
    Returns None if spreadsheet missing or sheet unreadable.
    """
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        return None
    owner_tab = os.getenv("OWNER_SHEET_NAME", "owner_slack_ID")
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        owners_ws = writer.sheet.worksheet(owner_tab)
    except Exception as exc:
        logger.warning("handover owner sheet unavailable tab=%s err=%s", owner_tab, exc)
        return None
    rows = worksheet_row_dicts(owners_ws)
    return rows if rows else None
