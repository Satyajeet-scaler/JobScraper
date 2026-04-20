import logging
import re
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.linkedin_recruiter.pipeline import is_linkedin_job_url
from services.linkedin_recruiter.sheets_pipeline import write_linkedin_recruiters_for_relevant_jobs
from services.role_pipeline import role_relevant_tab_name
from services.role_recruiter_info_service import role_recruiters_tab_name_for_role

logger = logging.getLogger(__name__)

RECRUITER_PROFILE_BACKFILL_RUN_METRICS: dict[str, dict[str, Any]] = {}


def run_recruiter_profile_backfill(
    run_id: str | None = None,
    run_date: str | None = None,
    role: str | None = None,
    relevant_tab: str | None = None,
    recruiters_tab: str | None = None,
) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    started_at = perf_counter()
    resolved_role = (role or "").strip()
    explicit_relevant_tab = (relevant_tab or "").strip()
    explicit_recruiters_tab = (recruiters_tab or "").strip()

    RECRUITER_PROFILE_BACKFILL_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
        "role": resolved_role,
        "relevant_tab": explicit_relevant_tab,
        "recruiters_tab": explicit_recruiters_tab,
    }

    try:
        resolved_tabs = _resolve_tabs(
            run_date=resolved_run_date,
            role=resolved_role,
            relevant_tab=explicit_relevant_tab,
            recruiters_tab=explicit_recruiters_tab,
        )
        relevant_rows = _read_rows_from_tab(resolved_tabs["relevant_tab"])
        existing_recruiter_rows = _read_rows_from_tab(
            resolved_tabs["recruiters_tab"],
            allow_missing=True,
        )

        existing_with_profile_urls = _job_urls_with_recruiter_profile(existing_recruiter_rows)
        candidates = [
            dict(row)
            for row in relevant_rows
            if _normalized_job_url(row) and _normalized_job_url(row) not in existing_with_profile_urls
        ]
        candidate_linkedin_jobs = [row for row in candidates if is_linkedin_job_url(_normalized_job_url(row))]

        rows_written, urls_with_recruiters = write_linkedin_recruiters_for_relevant_jobs(
            run_date=resolved_run_date,
            relevant_jobs=candidates,
            recruiters_tab=resolved_tabs["recruiters_tab"],
            relevant_jobs_tab=resolved_tabs["relevant_tab"],
            append_mode=True,
            dedupe_existing_on=("job_url", "recruiter_profile_url", "recruiter_email", "recruiter_source"),
            include_company_contacts_fallback=False,
        )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "relevant_tab": resolved_tabs["relevant_tab"],
            "recruiters_tab": resolved_tabs["recruiters_tab"],
            "relevant_rows_scanned": len(relevant_rows),
            "existing_recruiter_rows_scanned": len(existing_recruiter_rows),
            "jobs_skipped_with_existing_profile_url": len(existing_with_profile_urls),
            "candidate_jobs_for_backfill": len(candidates),
            "candidate_linkedin_jobs_for_backfill": len(candidate_linkedin_jobs),
            "jobs_with_new_recruiter_profiles_found": len(urls_with_recruiters),
            "recruiter_rows_appended": rows_written,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        RECRUITER_PROFILE_BACKFILL_RUN_METRICS[pipeline_run_id] = metrics
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "relevant_tab": explicit_relevant_tab,
            "recruiters_tab": explicit_recruiters_tab,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        RECRUITER_PROFILE_BACKFILL_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("recruiter-profile-backfill[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_recruiter_profile_backfill_run_metrics(run_id: str) -> dict[str, Any] | None:
    return RECRUITER_PROFILE_BACKFILL_RUN_METRICS.get(run_id)


def _resolve_tabs(
    *,
    run_date: str,
    role: str,
    relevant_tab: str,
    recruiters_tab: str,
) -> dict[str, str]:
    if role and relevant_tab:
        raise ValueError("Provide either role or relevant_tab, not both.")
    if role:
        resolved_relevant = role_relevant_tab_name(role=role, run_date=run_date)
        resolved_recruiters = recruiters_tab or role_recruiters_tab_name_for_role(role=role, run_date=run_date)
        return {"relevant_tab": resolved_relevant, "recruiters_tab": resolved_recruiters}
    if relevant_tab:
        resolved_recruiters = recruiters_tab or _derive_recruiters_tab_from_relevant_tab(relevant_tab)
        return {"relevant_tab": relevant_tab, "recruiters_tab": resolved_recruiters}
    return {
        "relevant_tab": f"relevant_jobs_{run_date}",
        "recruiters_tab": recruiters_tab or f"recruiters_info_{run_date}",
    }


def _derive_recruiters_tab_from_relevant_tab(relevant_tab: str) -> str:
    import os

    legacy_match = re.fullmatch(r"relevant_jobs_(\d{4}-\d{2}-\d{2})", relevant_tab)
    if legacy_match:
        return f"recruiters_info_{legacy_match.group(1)}"

    role_match = re.fullmatch(r"role_relevant_([a-z0-9_]+)_(\d{4}-\d{2}-\d{2})", relevant_tab)
    if role_match:
        role_slug = role_match.group(1)
        run_date = role_match.group(2)
        template = (
            os.getenv("ROLE_PIPELINE_RECRUITERS_TAB_TEMPLATE")
            or "role_recruiters_info_{role_slug}_{date}"
        ).strip()
        return template.format(role_slug=role_slug, date=run_date)

    raise ValueError(
        "Could not derive recruiters tab from relevant_tab. "
        "Provide recruiters_tab explicitly when using custom tab names."
    )


def _read_rows_from_tab(tab_name: str, allow_missing: bool = False) -> list[dict[str, str]]:
    spreadsheet_id = _require_spreadsheet_id()
    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    try:
        ws = writer.open_worksheet(tab_name)
    except Exception:
        if allow_missing:
            return []
        raise
    raw = writer.worksheet_get_all_values(ws, f"recruiter_profile_backfill:{tab_name}:get_all_values")
    rows = [dict(r) for r in worksheet_row_dicts(raw)]
    if not rows and not allow_missing:
        raise RuntimeError(f"No rows found in worksheet {tab_name}.")
    return rows


def _job_urls_with_recruiter_profile(rows: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in rows:
        profile = str(row.get("recruiter_profile_url") or "").strip()
        if not profile:
            continue
        job_url = _normalized_job_url(row)
        if not job_url:
            continue
        out.add(job_url)
    return out


def _normalized_job_url(row: dict[str, Any]) -> str:
    return str(row.get("job_url") or "").strip()


def _require_spreadsheet_id() -> str:
    import os

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    return spreadsheet_id
