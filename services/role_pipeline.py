import logging
import os
import re
import json
import traceback
import uuid
from datetime import date
from time import perf_counter
from typing import Any

import gspread

from services.description_text_parts import apply_three_part_text_columns
from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.pipeline import _classify_relevant_jobs, _dedupe_jobs, _parse_csv_env, _retry
from services.role_scrapers import SCRAPER_REGISTRY

logger = logging.getLogger(__name__)

ROLE_SCRAPE_RUN_METRICS: dict[str, dict[str, Any]] = {}
ROLE_CLASSIFY_RUN_METRICS: dict[str, dict[str, Any]] = {}
ROLE_PIPELINE_ALLOWED_SOURCES = SCRAPER_REGISTRY.available_sources()

# Unified per-role / per-scraper config.  Overridable via env
# ROLE_PIPELINE_ROLE_CONFIG_JSON (see _load_role_config_map).
#
# Each role maps to a dict of per-source config, plus optional top-level keys
# consumed by the role pipeline itself:
#   - "ai_relevance_prompt": role-specific prompt override for the classifier.
#   - "handover":            {"min_candidate_match": N} -- per-role Slack rules.
ROLE_CONFIG_MAP: dict[str, dict[str, dict[str, Any]]] = {
    "data analyst": {
        "jobspy":     {"query": "Data Analyst"},
        "naukri":     {"query": "Data Analyst"},
        "wellfound":  {"query": "Data Analyst"},
        "hirist":     {"url": "https://www.hirist.tech/c/data-analytics-bi-jobs?ref=topnavigation"},
    },
    "software developer": {
        "jobspy":     {"query": "Software Developer"},
        "naukri":     {"query": "Software Developer"},
        "wellfound":  {"query": "Software Developer"},
    },
    "devops": {
        "jobspy":     {"query": "DevOps Engineer"},
        "naukri":     {"query": "DevOps Engineer"},
        "wellfound":  {"query": "DevOps Engineer"},
    },
}


def run_role_scrape_only(
    run_id: str | None = None,
    run_date: str | None = None,
    role: str | None = None,
    sources: list[str] | None = None,
) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    resolved_role = _validate_role(role)
    resolved_sources = _resolve_sources(sources)
    role_slug = _role_slug(resolved_role)
    started_at = perf_counter()

    ROLE_SCRAPE_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
        "role": resolved_role,
        "role_slug": role_slug,
        "sources": sorted(resolved_sources),
    }

    try:
        scraped = _scrape_role_jobs(resolved_role, resolved_sources)
        deduped = _dedupe_jobs(scraped)
        scraped_tab = _scraped_tab_name(role_slug=role_slug, run_date=resolved_run_date)
        existing_scraped_rows = _read_rows_from_tab(scraped_tab, allow_missing=True)
        scrape_run_seq = _next_run_sequence(existing_scraped_rows)
        new_rows = _filter_extra_jobs_by_site_job_url(
            rows=deduped,
            existing_rows=existing_scraped_rows,
        )
        new_rows_with_run = _attach_run_tracking(
            rows=new_rows,
            run_id=pipeline_run_id,
            run_seq=scrape_run_seq,
        )
        appended_count = _append_rows_to_tab(scraped_tab, new_rows_with_run)
        total_after_append = len(existing_scraped_rows) + appended_count

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "sources": sorted(resolved_sources),
            "scraped_count": len(scraped),
            "deduped_count": len(deduped),
            "existing_scraped_count": len(existing_scraped_rows),
            "new_scraped_count": appended_count,
            "total_scraped_count_after_append": total_after_append,
            "scraped_run_seq": scrape_run_seq,
            "scraped_tab": scraped_tab,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_SCRAPE_RUN_METRICS[pipeline_run_id] = metrics
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "sources": sorted(resolved_sources),
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        ROLE_SCRAPE_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("role-scrape-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def run_role_classify_only(
    run_id: str | None = None,
    run_date: str | None = None,
    role: str | None = None,
    post_classify_chain_enabled: bool | None = None,
) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_role = _validate_role(role)
    role_slug = _role_slug(resolved_role)
    resolved_run_date = (run_date or date.today().isoformat()).strip()
    started_at = perf_counter()

    ROLE_CLASSIFY_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
        "role": resolved_role,
        "role_slug": role_slug,
    }

    try:
        scraped_tab = _scraped_tab_name(role_slug=role_slug, run_date=resolved_run_date)
        relevant_tab = _relevant_tab_name(role_slug=role_slug, run_date=resolved_run_date)

        scraped_rows = _read_rows_from_tab(scraped_tab)
        deduped_scraped = _dedupe_jobs(scraped_rows)
        existing_relevant_rows = _read_rows_from_tab(relevant_tab, allow_missing=True)
        relevant_run_seq = _next_run_sequence(existing_relevant_rows)
        classify_input_rows = _filter_extra_jobs_by_site_job_url(
            rows=deduped_scraped,
            existing_rows=existing_relevant_rows,
        )

        if classify_input_rows:
            relevant, classifier_metrics = _classify_relevant_jobs_for_role_pipeline(
                classify_input_rows,
                role=resolved_role,
            )
        else:
            relevant = []
            classifier_metrics = {"classification_errors": 0}

        relevant_deduped = _dedupe_jobs(relevant)
        relevant_new_rows = _filter_extra_jobs_by_site_job_url(
            rows=relevant_deduped,
            existing_rows=existing_relevant_rows,
        )
        relevant_rows_with_run = _attach_run_tracking(
            rows=relevant_new_rows,
            run_id=pipeline_run_id,
            run_seq=relevant_run_seq,
        )
        appended_relevant_count = _append_rows_to_tab(relevant_tab, relevant_rows_with_run)
        total_relevant_after_append = len(existing_relevant_rows) + appended_relevant_count
        relevant_tab = _relevant_tab_name(role_slug=role_slug, run_date=resolved_run_date)

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "role": resolved_role,
            "role_slug": role_slug,
            "scraped_input_count": len(scraped_rows),
            "deduped_input_count": len(deduped_scraped),
            "classify_input_count": len(classify_input_rows),
            "existing_relevant_count": len(existing_relevant_rows),
            "relevant_count": len(relevant_deduped),
            "new_relevant_count": appended_relevant_count,
            "total_relevant_count_after_append": total_relevant_after_append,
            "relevant_run_seq": relevant_run_seq,
            "classification_errors": classifier_metrics.get("classification_errors", 0),
            "source_scraped_tab": scraped_tab,
            "relevant_tab": relevant_tab,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        chain_enabled = (
            post_classify_chain_enabled
            if post_classify_chain_enabled is not None
            else os.getenv("ROLE_PIPELINE_POST_CLASSIFY_CHAIN_ENABLED", "false").lower() in ("1", "true", "yes")
        )
        metrics["post_classify_chain_enabled"] = chain_enabled
        if chain_enabled:
            recruiter_summary: dict[str, Any] = {"status": "skipped"}
            slack_summary: dict[str, Any] = {"status": "skipped"}
            try:
                from services.role_recruiter_info_service import run_role_recruiter_info_extraction

                recruiter_summary = run_role_recruiter_info_extraction(
                    run_id=str(uuid.uuid4()),
                    run_date=resolved_run_date,
                    role=resolved_role,
                    upstream_run_id=pipeline_run_id,
                    upstream_run_seq=relevant_run_seq,
                )
            except Exception as exc:
                recruiter_summary = {"status": "failed", "error": str(exc)}
                logger.warning(
                    "role classify chain recruiter step failed run_id=%s role=%s date=%s err=%s",
                    pipeline_run_id,
                    resolved_role,
                    resolved_run_date,
                    exc,
                )
            try:
                from services.slack_role_pipeline_notify import send_role_handover_notifications

                slack_summary = send_role_handover_notifications(
                    run_date=resolved_run_date,
                    role=resolved_role,
                    upstream_run_id=pipeline_run_id,
                )
            except Exception as exc:
                slack_summary = {"status": "failed", "error": str(exc)}
                logger.warning(
                    "role classify chain slack step failed run_id=%s role=%s date=%s err=%s",
                    pipeline_run_id,
                    resolved_role,
                    resolved_run_date,
                    exc,
                )
            metrics["post_classify_recruiter_summary"] = recruiter_summary
            metrics["post_classify_slack_summary"] = slack_summary
        ROLE_CLASSIFY_RUN_METRICS[pipeline_run_id] = metrics
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
        ROLE_CLASSIFY_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("role-classify-only[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_role_scrape_run_metrics(run_id: str) -> dict[str, Any] | None:
    return ROLE_SCRAPE_RUN_METRICS.get(run_id)


def get_role_classify_run_metrics(run_id: str) -> dict[str, Any] | None:
    return ROLE_CLASSIFY_RUN_METRICS.get(run_id)


def _validate_role(role: str | None) -> str:
    resolved = (role or "").strip()
    if not resolved:
        raise ValueError("role is required.")
    return resolved


def _role_slug(role: str) -> str:
    lowered = role.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return slug or "role"


def _scraped_tab_name(*, role_slug: str, run_date: str) -> str:
    template = (
        os.getenv("ROLE_PIPELINE_SCRAPED_TAB_TEMPLATE")
        or "role_scraped_{role_slug}_{date}"
    ).strip()
    return template.format(role_slug=role_slug, date=run_date)


def _relevant_tab_name(*, role_slug: str, run_date: str) -> str:
    template = (
        os.getenv("ROLE_PIPELINE_RELEVANT_TAB_TEMPLATE")
        or "role_relevant_{role_slug}_{date}"
    ).strip()
    return template.format(role_slug=role_slug, date=run_date)


def role_relevant_tab_name(*, role: str, run_date: str) -> str:
    """Public helper: return the role pipeline's relevant-jobs tab name."""
    return _relevant_tab_name(role_slug=_role_slug(role), run_date=run_date)


def _read_rows_from_tab(tab_name: str, allow_missing: bool = False) -> list[dict[str, Any]]:
    writer = _get_writer()
    try:
        ws = writer.open_worksheet(tab_name)
    except gspread.WorksheetNotFound:
        if allow_missing:
            return []
        raise
    raw = writer.worksheet_get_all_values(ws, f"role_pipeline:{tab_name}:get_all_values")
    rows = worksheet_row_dicts(raw)
    if not rows and not allow_missing:
        raise RuntimeError(f"No rows found in worksheet {tab_name}.")
    return [dict(r) for r in rows]


def _append_rows_to_tab(tab_name: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    writer = _get_writer()
    chunk_size = max(1, int(os.getenv("GOOGLE_SHEETS_WRITE_CHUNK_SIZE", "200")))
    rows_for_sheet, overflow_rows, overflow_chars = apply_three_part_text_columns(rows, "description")
    if overflow_rows:
        logger.warning(
            "description split truncated rows=%s overflow_chars=%s tab=%s",
            overflow_rows,
            overflow_chars,
            tab_name,
        )
    headers = _derive_headers(rows_for_sheet)
    data_rows = [[_stringify_cell(row.get(col)) for col in headers] for row in rows_for_sheet]
    writer.append_to_worksheet(
        worksheet_title=tab_name,
        data_rows=data_rows,
        header_row=headers,
        chunk_size=chunk_size,
    )
    return len(rows_for_sheet)


def _get_writer() -> GoogleSheetsWriter:
    spreadsheet_id = (
        os.getenv("ROLE_PIPELINE_GOOGLE_SPREADSHEET_ID")
        or os.getenv("GOOGLE_SPREADSHEET_ID")
        or ""
    ).strip()
    if not spreadsheet_id:
        raise RuntimeError("Set ROLE_PIPELINE_GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_ID.")
    return GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)


def _scrape_role_jobs(role: str, enabled_sources: set[str]) -> list[dict[str, Any]]:
    """Scrape jobs for *role* from every enabled source using the scraper registry."""
    all_jobs: list[dict[str, Any]] = []
    role_label = role.strip()
    full_role_config = _resolve_role_config(role_label)

    for source_name in sorted(enabled_sources):
        adapter = SCRAPER_REGISTRY.get(source_name)
        if adapter is None:
            logger.warning("role-pipeline: no adapter registered for source=%s, skipping", source_name)
            continue

        source_cfg = full_role_config.get(source_name) or {}
        try:
            source_jobs = adapter.scrape_for_role(role_label, source_cfg)
            all_jobs.extend(source_jobs)
            logger.info(
                "role-pipeline source=%s role=%s scraped_count=%s",
                source_name, role_label, len(source_jobs),
            )
        except Exception as exc:
            logger.warning(
                "role-pipeline %s scrape failed role=%s err=%s",
                source_name, role_label, exc,
            )

    normalized_jobs = [_normalize_job(job) for job in all_jobs]
    return normalized_jobs


def _resolve_sources(sources: list[str] | None) -> set[str]:
    if not sources:
        return set(ROLE_PIPELINE_ALLOWED_SOURCES)
    normalized = {(source or "").strip().lower() for source in sources if (source or "").strip()}
    if not normalized:
        return set(ROLE_PIPELINE_ALLOWED_SOURCES)
    invalid = sorted(normalized - ROLE_PIPELINE_ALLOWED_SOURCES)
    if invalid:
        raise ValueError(
            "Invalid sources: "
            + ", ".join(invalid)
            + ". Allowed sources: "
            + ", ".join(sorted(ROLE_PIPELINE_ALLOWED_SOURCES))
        )
    return normalized


def _normalize_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_date": date.today().isoformat(),
        "requested_role": job.get("requested_role") or job.get("role_query"),
        "role_query": job.get("role_query"),
        "site": job.get("site") or "unknown",
        "title": job.get("title"),
        "company": job.get("company") or job.get("company_name"),
        "location": job.get("location"),
        "date_posted": job.get("date_posted"),
        "job_url": job.get("job_url") or job.get("job_url_direct"),
        "description": job.get("description"),
        "experience": job.get("experience"),
        "salary": job.get("salary"),
        "job_type": job.get("job_type"),
        "raw_payload": job.get("raw_payload") if isinstance(job.get("raw_payload"), dict) else job,
    }





def _derive_headers(rows: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    headers: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)
    return headers or ["message"]


def _stringify_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, default=str)
    return str(value)


def _job_identity_key(row: dict[str, Any]) -> tuple[str, str] | None:
    site = str(row.get("site") or "").strip().lower()
    job_url = str(row.get("job_url") or "").strip()
    if not site or not job_url:
        return None
    return (site, job_url)


def _filter_extra_jobs_by_site_job_url(
    *,
    rows: list[dict[str, Any]],
    existing_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    for row in existing_rows:
        key = _job_identity_key(row)
        if key:
            seen.add(key)

    out: list[dict[str, Any]] = []
    for row in rows:
        key = _job_identity_key(row)
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _next_run_sequence(existing_rows: list[dict[str, Any]]) -> int:
    max_seen = 0
    for row in existing_rows:
        raw = str(row.get("role_pipeline_run_seq") or "").strip()
        if not raw:
            continue
        try:
            current = int(float(raw))
        except ValueError:
            continue
        if current > max_seen:
            max_seen = current
    return max_seen + 1


def _attach_run_tracking(
    *,
    rows: list[dict[str, Any]],
    run_id: str,
    run_seq: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        enriched["role_pipeline_run_id"] = run_id
        enriched["role_pipeline_run_seq"] = run_seq
        out.append(enriched)
    return out


def _resolve_role_config(role: str) -> dict[str, Any]:
    """Return per-source + reserved-key config for *role*.

    Resolution order:
    1. ``ROLE_PIPELINE_ROLE_CONFIG_JSON`` env variable (highest priority)
    2. In-code ``ROLE_CONFIG_MAP`` constant
    3. Fallback: ``{"query": role}`` for every registered source

    Returned dict may contain per-source entries (``jobspy``, ``naukri``, ...)
    and reserved keys (``ai_relevance_prompt``, ``handover``).
    """
    key = (role or "").strip().lower()
    if not key:
        raise ValueError("role is required.")

    env_map = _load_role_config_map()
    if key in env_map:
        return env_map[key]

    if key in ROLE_CONFIG_MAP:
        return ROLE_CONFIG_MAP[key]

    # Default fallback — every source gets the role as a query string.
    return {src: {"query": role} for src in SCRAPER_REGISTRY.available_sources()}


def _classify_relevant_jobs_for_role_pipeline(
    jobs: list[dict[str, Any]],
    role: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Role-pipeline wrapper around shared classifier.

    Per-role prompt resolution order:
    1. ``ai_relevance_prompt`` key inside the role's entry in
       ``ROLE_PIPELINE_ROLE_CONFIG_JSON`` / ``ROLE_CONFIG_MAP``.
    2. ``ROLE_PIPELINE_AI_RELEVANCE_PROMPT`` (global role-pipeline override).
    3. Default ``AI_RELEVANCE_PROMPT`` / built-in prompt (existing fallback).
    """
    role_prompt: str | None = None
    if role:
        try:
            role_cfg = _resolve_role_config(role)
        except Exception:
            role_cfg = {}
        candidate = (role_cfg or {}).get("ai_relevance_prompt")
        if isinstance(candidate, dict):
            candidate = candidate.get("prompt") or candidate.get("value")
        if isinstance(candidate, str) and candidate.strip():
            role_prompt = candidate

    if not role_prompt:
        role_prompt = os.getenv("ROLE_PIPELINE_AI_RELEVANCE_PROMPT")

    if not role_prompt:
        return _classify_relevant_jobs(jobs)

    previous_prompt = os.environ.get("AI_RELEVANCE_PROMPT")
    try:
        os.environ["AI_RELEVANCE_PROMPT"] = role_prompt
        return _classify_relevant_jobs(jobs)
    finally:
        if previous_prompt is None:
            os.environ.pop("AI_RELEVANCE_PROMPT", None)
        else:
            os.environ["AI_RELEVANCE_PROMPT"] = previous_prompt


def _load_role_config_map() -> dict[str, dict[str, dict[str, Any]]]:
    """Parse the unified ``ROLE_PIPELINE_ROLE_CONFIG_JSON`` env variable.

    Expected shape::

        {
          "data analyst": {
            "jobspy":    {"query": "Data Analyst"},
            "naukri":    {"query": "Data Analyst"},
            "wellfound": {"query": "Data Analyst"},
            "hirist":    {"url": "https://..."},
            "hirecafe":  {"search_url": "https://..."}
          },
          "software engineer": { ... }
        }

    Also accepts the *old* format where each source value is a plain
    string (treated as ``{"query": value}``) for backward compatibility.

    Reserved top-level keys under each role (preserved as-is, not wrapped):
    - ``ai_relevance_prompt`` (str): per-role classifier prompt override.
    - ``handover`` (dict): per-role Slack handover rules, e.g.
      ``{"min_candidate_match": 10}``.
    """
    raw = (os.getenv("ROLE_PIPELINE_ROLE_CONFIG_JSON") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("invalid ROLE_PIPELINE_ROLE_CONFIG_JSON: %s", exc)
        return {}
    if not isinstance(parsed, dict):
        logger.warning("ROLE_PIPELINE_ROLE_CONFIG_JSON must be a JSON object")
        return {}

    reserved_role_keys = {"ai_relevance_prompt", "handover"}

    out: dict[str, dict[str, Any]] = {}
    for role_key, source_map in parsed.items():
        if not isinstance(role_key, str) or not isinstance(source_map, dict):
            continue
        normalised_sources: dict[str, Any] = {}
        for src_name, src_val in source_map.items():
            if not isinstance(src_name, str):
                continue
            key_lower = src_name.strip().lower()
            if key_lower in reserved_role_keys:
                normalised_sources[key_lower] = src_val
                continue
            if isinstance(src_val, str):
                # Backward compat: plain string → {"query": value}
                normalised_sources[key_lower] = {"query": src_val.strip()}
            elif isinstance(src_val, dict):
                normalised_sources[key_lower] = src_val
            else:
                logger.warning(
                    "ROLE_PIPELINE_ROLE_CONFIG_JSON: ignoring invalid value for role=%s source=%s",
                    role_key, src_name,
                )
        if normalised_sources:
            out[role_key.strip().lower()] = normalised_sources
    return out
