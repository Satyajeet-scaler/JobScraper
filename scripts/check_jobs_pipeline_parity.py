#!/usr/bin/env python3
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.mysql_recruiter_store import _db


SCRAPED_RE = re.compile(r"^role_scraped_([a-z0-9_]+)_(\d{4}-\d{2}-\d{2})$")
RELEVANT_RE = re.compile(r"^role_relevant_([a-z0-9_]+)_(\d{4}-\d{2}-\d{2})$")
RECRUITER_RE = re.compile(r"^role_recruiters_info_([a-z0-9_]+)_(\d{4}-\d{2}-\d{2})$")


def _sheet_count_map(writer: GoogleSheetsWriter) -> dict[tuple[str, str], dict[str, int]]:
    out: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"scraped": 0, "relevant": 0, "recruiter": 0})
    for ws in writer.list_worksheets():
        title = ws.title
        for pattern, key in ((SCRAPED_RE, "scraped"), (RELEVANT_RE, "relevant"), (RECRUITER_RE, "recruiter")):
            m = pattern.match(title)
            if not m:
                continue
            role_slug, run_date = m.group(1), m.group(2)
            raw = writer.worksheet_get_all_values(ws, f"parity:{title}:get_all_values")
            out[(role_slug, run_date)][key] = len(worksheet_row_dicts(raw))
    return out


def _db_count_map() -> dict[tuple[str, str], dict[str, int]]:
    sql = """
    SELECT
        COALESCE(requested_role, '') AS role_name,
        DATE_FORMAT(run_date, '%%Y-%%m-%%d') AS run_date,
        COUNT(*) AS jobs_count
    FROM jobs
    GROUP BY requested_role, run_date
    """
    out: dict[tuple[str, str], dict[str, int]] = {}
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            jobs_rows = cur.fetchall() or []
            for row in jobs_rows:
                key = (str(row.get("role_name") or "").strip().lower().replace(" ", "_"), row["run_date"])
                out[key] = {"jobs": int(row.get("jobs_count") or 0), "scraped": 0, "relevant": 0, "recruiter": 0}

            cur.execute("SELECT COUNT(*) AS c FROM job_scrapes")
            total_scrapes = int((cur.fetchone() or {}).get("c") or 0)
            cur.execute("SELECT COUNT(*) AS c FROM job_relevance")
            total_relevance = int((cur.fetchone() or {}).get("c") or 0)
            cur.execute("SELECT COUNT(*) AS c FROM job_recruiter_contacts")
            total_recruiter = int((cur.fetchone() or {}).get("c") or 0)
    out[("_totals", "_all")] = {
        "jobs": sum(v.get("jobs", 0) for v in out.values()),
        "scraped": total_scrapes,
        "relevant": total_relevance,
        "recruiter": total_recruiter,
    }
    return out


def main() -> None:
    import os

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    sheet_map = _sheet_count_map(writer)
    db_map = _db_count_map()
    print("Parity report:")
    for key in sorted(sheet_map.keys()):
        sheet_counts = sheet_map[key]
        db_counts = db_map.get(key, {})
        print(
            key,
            "sheet=", sheet_counts,
            "db=", {k: db_counts.get(k, 0) for k in ("jobs", "scraped", "relevant", "recruiter")},
        )
    print("DB totals:", db_map.get(("_totals", "_all"), {}))


if __name__ == "__main__":
    main()
