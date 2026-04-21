#!/usr/bin/env python3
from __future__ import annotations

import re
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.mysql_jobs_store import upsert_job_recruiter_contact, upsert_job_relevance, upsert_job_scrape


SCRAPED_RE = re.compile(r"^role_scraped_[a-z0-9_]+_\d{4}-\d{2}-\d{2}$")
RELEVANT_RE = re.compile(r"^role_relevant_[a-z0-9_]+_\d{4}-\d{2}-\d{2}$")
RECRUITER_RE = re.compile(r"^role_recruiters_info_[a-z0-9_]+_\d{4}-\d{2}-\d{2}$")


def _read_rows(writer: GoogleSheetsWriter, tab: str) -> list[dict[str, Any]]:
    ws = writer.open_worksheet(tab)
    raw = writer.worksheet_get_all_values(ws, f"backfill:{tab}:get_all_values")
    return [dict(r) for r in worksheet_row_dicts(raw)]


def main() -> None:
    import os

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    worksheets = writer.list_worksheets()
    tabs = [ws.title for ws in worksheets]

    scraped_tabs = [t for t in tabs if SCRAPED_RE.match(t)]
    relevant_tabs = [t for t in tabs if RELEVANT_RE.match(t)]
    recruiter_tabs = [t for t in tabs if RECRUITER_RE.match(t)]

    counts = {"scrapes": 0, "relevance": 0, "recruiter_contacts": 0}
    for tab in sorted(scraped_tabs):
        for row in _read_rows(writer, tab):
            upsert_job_scrape(row)
            counts["scrapes"] += 1
    for tab in sorted(relevant_tabs):
        for row in _read_rows(writer, tab):
            upsert_job_relevance(row)
            counts["relevance"] += 1
    for tab in sorted(recruiter_tabs):
        for row in _read_rows(writer, tab):
            upsert_job_recruiter_contact(row)
            counts["recruiter_contacts"] += 1

    print("Backfill complete:", counts)


if __name__ == "__main__":
    main()
