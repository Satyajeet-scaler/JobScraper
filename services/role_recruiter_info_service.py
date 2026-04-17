import logging
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

from services.role_pipeline import _read_rows_from_tab, _role_slug, _relevant_tab_name
from services.linkedin_recruiter.sheets_pipeline import write_linkedin_recruiters_for_relevant_jobs

logger = logging.getLogger(__name__)

ROLE_RECRUITER_INFO_RUN_METRICS: dict[str, dict[str, Any]] = {}


def run_role_recruiter_info_extraction(
    run_id: str | None = None,
    run_date: str | None = None,
    role: str | None = None,
    upstream_run_id: str | None = None,
    upstream_run_seq: int | None = None,
) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    resolved_role = (role or "").strip()
    if not resolved_role:
        raise ValueError("role is required.")
    role_slug = _role_slug(resolved_role)
    started_at = perf_counter()

    ROLE_RECRUITER_INFO_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
        "role": resolved_role,
        "role_slug": role_slug,
    }

    try:
        relevant_tab = _relevant_tab_name(role_slug=role_slug, run_date=resolved_run_date)
        recruiters_tab = _role_recruiters_tab_name(role_slug=role_slug, run_date=resolved_run_date)
        relevant_jobs_all = _read_rows_from_tab(relevant_tab)
        relevant_jobs = _filter_rows_for_upstream_run(
            relevant_jobs_all,
            upstream_run_id=upstream_run_id,
        )
        recruiters_existing_rows = _read_recruiter_rows(recruiters_tab)
        recruiter_run_seq = _next_recruiter_run_sequence(recruiters_existing_rows)
        rows_written, urls_with_recruiters = write_linkedin_recruiters_for_relevant_jobs(
            run_date=resolved_run_date,
            relevant_jobs=relevant_jobs,
            recruiters_tab=recruiters_tab,
            relevant_jobs_tab=relevant_tab,
            append_mode=True,
            dedupe_existing_on=("job_url", "recruiter_profile_url", "recruiter_email", "recruiter_source"),
            extra_columns={
                "role_pipeline_upstream_run_id": upstream_run_id or "",
                "role_pipeline_upstream_run_seq": upstream_run_seq or "",
                "role_pipeline_recruiter_run_id": pipeline_run_id,
                "role_pipeline_recruiter_run_seq": recruiter_run_seq,
            },
        )
        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "upstream_run_id": upstream_run_id or "",
            "upstream_run_seq": upstream_run_seq or "",
            "relevant_total_count": len(relevant_jobs_all),
            "relevant_input_count": len(relevant_jobs),
            "recruiters_rows_written": rows_written,
            "jobs_with_recruiter_profiles_count": len(urls_with_recruiters),
            "recruiter_run_seq": recruiter_run_seq,
            "relevant_tab": relevant_tab,
            "recruiters_tab": recruiters_tab,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_RECRUITER_INFO_RUN_METRICS[pipeline_run_id] = metrics
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_RECRUITER_INFO_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("role-recruiter-info[%s] failed: %s", pipeline_run_id, exc)
        raise


def _role_recruiters_tab_name(*, role_slug: str, run_date: str) -> str:
    import os

    template = (
        os.getenv("ROLE_PIPELINE_RECRUITERS_TAB_TEMPLATE")
        or "role_recruiters_info_{role_slug}_{date}"
    ).strip()
    return template.format(role_slug=role_slug, date=run_date)


def role_recruiters_tab_name_for_role(*, role: str, run_date: str) -> str:
    """Worksheet name for the role recruiters tab (same as ``run_role_recruiter_info_extraction``)."""
    return _role_recruiters_tab_name(role_slug=_role_slug(role), run_date=run_date)


def get_role_recruiter_info_run_metrics(run_id: str) -> dict[str, Any] | None:
    return ROLE_RECRUITER_INFO_RUN_METRICS.get(run_id)


def _filter_rows_for_upstream_run(
    rows: list[dict[str, Any]],
    *,
    upstream_run_id: str | None,
) -> list[dict[str, Any]]:
    if not upstream_run_id:
        return rows
    selected: list[dict[str, Any]] = []
    for row in rows:
        row_run = str(row.get("role_pipeline_run_id") or "").strip()
        if row_run == upstream_run_id:
            selected.append(row)
    return selected


def _read_recruiter_rows(tab: str) -> list[dict[str, str]]:
    import os
    from services.google_sheets import GoogleSheetsWriter
    from services.handover_owners import worksheet_row_dicts

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        return []
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(tab)
        raw = writer.worksheet_get_all_values(ws, f"role_recruiter_existing:{tab}:get_all_values")
    except Exception:
        return []
    return [dict(r) for r in worksheet_row_dicts(raw)]


def _next_recruiter_run_sequence(existing_rows: list[dict[str, Any]]) -> int:
    max_seen = 0
    for row in existing_rows:
        raw = str(row.get("role_pipeline_recruiter_run_seq") or "").strip()
        if not raw:
            continue
        try:
            value = int(float(raw))
        except ValueError:
            continue
        if value > max_seen:
            max_seen = value
    return max_seen + 1

