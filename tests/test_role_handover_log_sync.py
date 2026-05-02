#!/usr/bin/env python3
"""Test role handover log sync by printing the JSON payload that would be written to sheets.

Connects to MySQL using remote-shell env vars and reads unsynced rows without
mutating sheet or DB state.
"""
from __future__ import annotations

import json
import os
import sys

# Ensure project root is on path so imports resolve
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.handover_log_sync import _recruiter_row_to_log_cells
from services.role_handover_log_sync import _linkedin_row_to_log_cells, _log_row_key
from services.mysql_jobs_store import fetch_unsynced_recruiter_rows_for_role
from services.mysql_linkedin_posts_store import fetch_unsynced_relevant_linkedin_posts_for_role


def main() -> None:
    role = os.getenv("TEST_ROLE", "software developer")
    run_date = os.getenv("TEST_RUN_DATE", "2026-05-01")

    recruiter_rows = fetch_unsynced_recruiter_rows_for_role(role=role, run_date=run_date)
    linkedin_rows = fetch_unsynced_relevant_linkedin_posts_for_role(role=role, run_date=run_date)

    data_rows: list[list[str]] = []
    for row in recruiter_rows:
        data_rows.append(_recruiter_row_to_log_cells(dict(row)))
    for row in linkedin_rows:
        data_rows.append(_linkedin_row_to_log_cells(row))

    # Build deduped view (same logic as sync function)
    existing_keys: set[tuple[str, str, str, str, str]] = set()
    new_rows: list[list[str]] = []
    for row in data_rows:
        key = _log_row_key(row)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_rows.append(row)

    output = {
        "role": role,
        "run_date": run_date,
        "recruiter_unsynced_count": len(recruiter_rows),
        "linkedin_unsynced_count": len(linkedin_rows),
        "total_rows_to_append": len(new_rows),
        "sample_rows": new_rows[:10],
        "all_rows": new_rows,
    }

    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
