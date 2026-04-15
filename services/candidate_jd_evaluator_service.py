import json
import logging
import os
import re
import traceback
import uuid
from datetime import datetime
from time import perf_counter, sleep
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts

try:
    import google.generativeai as genai
except ImportError:  # pragma: no cover - optional dependency behavior
    genai = None

logger = logging.getLogger(__name__)

CANDIDATE_JD_EVALUATOR_RUN_METRICS: dict[str, dict[str, Any]] = {}


def run_candidate_jd_evaluator(run_id: str | None = None, run_date: str | None = None) -> dict[str, Any]:
    pipeline_run_id = run_id or str(uuid.uuid4())
    resolved_run_date = _resolve_run_date(run_date)
    started_at = perf_counter()
    CANDIDATE_JD_EVALUATOR_RUN_METRICS[pipeline_run_id] = {
        "run_id": pipeline_run_id,
        "status": "running",
        "run_date": resolved_run_date,
    }
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=_require_spreadsheet_id())
        candidates = _read_candidates(writer)
        recruiters_tab = os.getenv("RECRUITERS_INFO_WORKSHEET") or f"recruiters_info_{resolved_run_date}"
        jd_rows = _read_jd_rows_for_date(writer, resolved_run_date, recruiters_tab)
        results: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        combined_output_rows: list[dict[str, Any]] = []
        output_sheet = _build_output_sheet_name(resolved_run_date)

        for idx, jd_row in enumerate(jd_rows, start=1):
            try:
                jd_context = _build_jd_context(jd_row, idx)
                ranked = _evaluate_candidates_for_jd(candidates, jd_context["jd_text"])
                summary = _ai_gt_70_summary(ranked)
                summary_row = _build_candidate_match_summary_row(summary, jd_context)
                combined_output_rows.append(summary_row)
                results.append(
                    {
                        "jd_index": idx,
                        "job_id": jd_context["job_id"],
                        "job_title": jd_context["job_title"],
                        "output_sheet": output_sheet,
                        "rows_written": 1,
                        "top_score": _top_score(ranked),
                        "ai_unavailable_count": len([row for row in ranked if row.get("ai_score") is None]),
                        "ai_score_gt_70_count": summary["count"],
                        "ai_score_gt_70_emails": summary["emails_csv"],
                    }
                )
                logger.info(
                    "candidate-jd-evaluator jd complete run_id=%s jd_index=%s total_jds=%s recruiter_row=%s job_id=%s job_title=%s rows_written=%s ai_gt_70_count=%s",
                    pipeline_run_id,
                    idx,
                    len(jd_rows),
                    jd_context.get("recruiter_sheet_row_number", ""),
                    jd_context["job_id"],
                    jd_context["job_title"],
                    1,
                    summary["count"],
                )
            except Exception as exc:
                logger.exception("candidate-jd-evaluator failed for jd_index=%s: %s", idx, exc)
                failures.append(
                    {
                        "jd_index": str(idx),
                        "error": str(exc),
                    }
                )

        if not results:
            raise RuntimeError("No JD evaluations completed successfully for the selected run date.")

        logger.info(
            "candidate-jd-evaluator sheet write starting run_id=%s tab=%s row_count=%s failed_jds=%s",
            pipeline_run_id,
            output_sheet,
            len(combined_output_rows),
            len(failures),
        )
        writer.write_rows(output_sheet, combined_output_rows)
        logger.info(
            "candidate-jd-evaluator sheet write done run_id=%s tab=%s row_count=%s",
            pipeline_run_id,
            output_sheet,
            len(combined_output_rows),
        )

        metrics = {
            "run_id": pipeline_run_id,
            "status": "completed",
            "run_date": resolved_run_date,
            "candidate_count": len(candidates),
            "jd_count": len(jd_rows),
            "output_sheet": output_sheet,
            "total_rows_written": len(combined_output_rows),
            "successful_jd_runs": len(results),
            "failed_jd_runs": len(failures),
            "jd_results": results,
            "failures": failures,
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        CANDIDATE_JD_EVALUATOR_RUN_METRICS[pipeline_run_id] = metrics
        return metrics
    except Exception as exc:
        metrics = {
            "run_id": pipeline_run_id,
            "status": "failed",
            "run_date": resolved_run_date,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "duration_seconds": round(perf_counter() - started_at, 2),
        }
        CANDIDATE_JD_EVALUATOR_RUN_METRICS[pipeline_run_id] = metrics
        logger.exception("candidate-jd-evaluator[%s] failed: %s", pipeline_run_id, exc)
        raise


def get_candidate_jd_evaluator_run_metrics(run_id: str) -> dict[str, Any] | None:
    return CANDIDATE_JD_EVALUATOR_RUN_METRICS.get(run_id)


def _resolve_run_date(run_date: str | None) -> str:
    if run_date and run_date.strip():
        return run_date.strip()
    tz = ZoneInfo(os.getenv("CRON_TIMEZONE", "Asia/Kolkata"))
    return datetime.now(tz).strftime("%Y-%m-%d")


def _require_spreadsheet_id() -> str:
    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")
    return spreadsheet_id


def _read_candidates(writer: GoogleSheetsWriter) -> list[dict[str, Any]]:
    sheet_name = (os.getenv("CANDIDATES_SHEET_NAME") or "Candidates").strip()
    ws = writer.open_worksheet(sheet_name)
    rows = worksheet_row_dicts(writer.worksheet_get_all_values(ws, f"candidate_jd_eval:{sheet_name}:get_all_values"))
    if not rows:
        raise RuntimeError(f"No candidate rows found in worksheet '{sheet_name}'.")
    required = ["email", "yoe", "validated skills with experience in month"]
    _require_columns(rows, required, f"candidate sheet '{sheet_name}'")
    out: list[dict[str, Any]] = []
    for row in rows:
        email = (row.get("email") or "").strip()
        if not email:
            continue
        out.append(
            {
                "email": email,
                "yoe": _to_float(row.get("yoe")),
                "notice_period": (row.get("notice period") or "").strip(),
                "work_experience": (row.get("work experience") or "").strip(),
                "projects": (row.get("projects") or "").strip(),
                "skills": (row.get("validated skills with experience in month") or "").strip(),
                "job_function": (row.get("recommended_job_function") or "").strip(),
                "company_type": (row.get("company type") or "").strip(),
                "domain": (row.get("domain") or "").strip(),
            }
        )
    if not out:
        raise RuntimeError(f"No valid candidate rows with email found in worksheet '{sheet_name}'.")
    return out


def _read_jd_rows_for_date(
    writer: GoogleSheetsWriter,
    run_date: str,
    recruiters_tab: str,
) -> list[dict[str, str]]:
    recruiters_ws = writer.open_worksheet(recruiters_tab)
    recruiter_rows = worksheet_row_dicts(
        writer.worksheet_get_all_values(
            recruiters_ws,
            f"candidate_jd_eval:{recruiters_tab}:get_all_values",
        )
    )
    if not recruiter_rows:
        raise RuntimeError(f"No rows found in recruiter info worksheet '{recruiters_tab}'.")

    date_key = _find_first_key(recruiter_rows[0], ["run_date", "date"])
    tab_key = _find_first_key(recruiter_rows[0], ["relevant_jobs_tab", "relevant tab"])
    job_url_key = _find_first_key(recruiter_rows[0], ["job_url", "url"])
    if not date_key:
        raise RuntimeError(f"Could not find run date column in recruiter worksheet '{recruiters_tab}'.")
    if not tab_key:
        raise RuntimeError(
            f"Could not find 'relevant_jobs_tab' column in recruiter worksheet '{recruiters_tab}'."
        )

    jd_rows: list[dict[str, str]] = []
    relevant_rows_cache: dict[str, list[dict[str, str]]] = {}
    for recruiter_sheet_row_num, row in enumerate(recruiter_rows, start=2):
        if _normalize_date(row.get(date_key, "")) != run_date:
            continue
        raw_tab_value = (row.get(tab_key) or "").strip()
        relevant_tab = raw_tab_value if raw_tab_value.startswith("relevant_jobs_") else f"relevant_jobs_{run_date}"
        row_number = _parse_int(raw_tab_value)
        if relevant_tab not in relevant_rows_cache:
            relevant_rows_cache[relevant_tab] = _load_relevant_rows(writer, relevant_tab)
        relevant_rows = relevant_rows_cache[relevant_tab]
        matched = _match_relevant_job_row(
            recruiter_row=row,
            relevant_rows=relevant_rows,
            job_url_key=job_url_key,
            row_number=row_number,
        )
        if not matched:
            continue
        jd_text = _extract_jd_text(matched)
        if not jd_text:
            continue
        merged = dict(row)
        merged["jd"] = jd_text
        merged["job_url"] = _extract_job_url(matched, row, job_url_key)
        merged["_jd_key"] = "jd"
        merged["_recruiter_sheet_row_number"] = str(recruiter_sheet_row_num)
        jd_rows.append(merged)

    if not jd_rows:
        raise RuntimeError(
            f"No JD rows could be mapped from '{recruiters_tab}' to relevant_jobs tabs for run_date '{run_date}'."
        )
    return jd_rows


def _load_relevant_rows(writer: GoogleSheetsWriter, relevant_tab: str) -> list[dict[str, str]]:
    ws = writer.open_worksheet(relevant_tab)
    raw = writer.worksheet_get_all_values(
        ws,
        f"candidate_jd_eval:{relevant_tab}:get_all_values",
    )
    if len(raw) <= 1:
        return []
    headers = [str(h or "").strip().lower() for h in raw[0]]
    out: list[dict[str, str]] = []
    for sheet_row_num, raw_row in enumerate(raw[1:], start=2):
        row: dict[str, str] = {"_sheet_row_number": str(sheet_row_num)}
        for idx, header in enumerate(headers):
            if not header:
                continue
            row[header] = raw_row[idx].strip() if idx < len(raw_row) else ""
        out.append(row)
    return out


def _match_relevant_job_row(
    recruiter_row: dict[str, str],
    relevant_rows: list[dict[str, str]],
    job_url_key: str | None,
    row_number: int | None,
) -> dict[str, str] | None:
    if not relevant_rows:
        return None
    if row_number is not None:
        for row in relevant_rows:
            if _parse_int(row.get("_sheet_row_number")) == row_number:
                return row
    recruiter_job_url = _normalize_job_url(recruiter_row.get(job_url_key or "", "") if job_url_key else "")
    if recruiter_job_url:
        by_url = {
            _normalize_job_url(row.get("job_url") or ""): row
            for row in relevant_rows
            if _normalize_job_url(row.get("job_url") or "")
        }
        match = by_url.get(recruiter_job_url)
        if match:
            return match

    rec_title = (recruiter_row.get("title") or "").strip().lower()
    rec_company = (recruiter_row.get("company") or "").strip().lower()
    if rec_title and rec_company:
        for row in relevant_rows:
            title = (row.get("title") or "").strip().lower()
            company = (row.get("company") or "").strip().lower()
            if title == rec_title and company == rec_company:
                return row
    return None


def _extract_jd_text(relevant_row: dict[str, str]) -> str:
    jd_key = _find_first_key(
        relevant_row,
        [
            "description",
            "job_description",
            "job description",
            "jd",
            "jd_text",
            "content",
        ],
    )
    if not jd_key:
        return ""
    return (relevant_row.get(jd_key) or "").strip()


def _build_jd_context(jd_row: dict[str, str], index: int) -> dict[str, str]:
    title_key = _find_first_key(jd_row, ["job_title", "job title", "title"])
    jd_key = jd_row["_jd_key"]
    # TODO: Change this temporary tab naming key to a stable business/job identifier.
    job_id = (jd_row.get("_recruiter_sheet_row_number") or "").strip() or f"jd_{index}"
    job_title = (jd_row.get(title_key, "") if title_key else "").strip() or f"JD {index}"
    jd_text = (jd_row.get(jd_key) or "").strip()
    if not jd_text:
        raise RuntimeError(f"JD text is empty for row index {index}.")
    recruiter_sheet_row_number = (jd_row.get("_recruiter_sheet_row_number") or "").strip()
    return {
        "job_id": job_id,
        "job_title": job_title,
        "job_url": (jd_row.get("job_url") or "").strip(),
        "jd_text": jd_text,
        "recruiter_sheet_row_number": recruiter_sheet_row_number,
    }


def _evaluate_candidates_for_jd(candidates: list[dict[str, Any]], jd_text: str) -> list[dict[str, Any]]:
    jd_parsed = _parse_jd_with_gemini(jd_text)
    required_skills = [
        {
            "skill": str(skill_obj.get("skill") or "").strip().lower(),
            "required_months": int(skill_obj.get("required_months") or 0),
        }
        for skill_obj in (jd_parsed.get("required_skills") or [])
        if isinstance(skill_obj, dict) and str(skill_obj.get("skill") or "").strip()
    ]
    if not required_skills:
        raise RuntimeError("JD parser did not return required_skills.")
    min_yoe = int(jd_parsed.get("min_yoe") or 0)
    preferred_skills = [
        str(item).strip().lower() for item in (jd_parsed.get("preferred_skills") or []) if str(item).strip()
    ]
    local_scored = [_score_candidate_local(candidate, required_skills, preferred_skills, min_yoe) for candidate in candidates]
    local_scored.sort(key=lambda item: item["local_score"], reverse=True)
    local_top_n = max(1, int(os.getenv("LOCAL_TOP_N", "75")))
    output_top_n = max(1, int(os.getenv("OUTPUT_TOP_N", "50")))
    shortlisted = local_scored[:local_top_n]
    _score_shortlisted_with_ai(shortlisted, jd_text, local_top_n=local_top_n)
    shortlisted.sort(key=lambda item: _sort_ai_score(item.get("ai_score")), reverse=True)
    return shortlisted[:output_top_n]


def _score_candidate_local(
    candidate: dict[str, Any],
    required_skills: list[dict[str, Any]],
    preferred_skills: list[str],
    min_yoe: int,
) -> dict[str, Any]:
    skill_map = _parse_skill_map(candidate.get("skills", ""))
    points_per_skill = 50 / max(1, len(required_skills))
    skill_score = 0.0
    matched_required: list[str] = []
    missing_required: list[str] = []
    for req in required_skills:
        skill = req["skill"]
        required_months = req["required_months"]
        candidate_months = _find_skill_match(skill_map, skill)
        if candidate_months is None:
            missing_required.append(skill)
            continue
        if required_months <= 0:
            skill_score += points_per_skill
            matched_required.append(skill)
            continue
        ratio = min(candidate_months / required_months, 1.0)
        if ratio >= 1.0:
            skill_score += points_per_skill
        elif ratio >= 0.8:
            skill_score += points_per_skill * 0.8
        elif ratio >= 0.6:
            skill_score += points_per_skill * 0.5
        elif ratio >= 0.4:
            skill_score += points_per_skill * 0.2
        matched_required.append(
            f"{skill} ({candidate_months}mo/{required_months}mo)" if ratio < 1 else f"{skill} ({candidate_months}mo)"
        )

    yoe = float(candidate.get("yoe") or 0)
    yoe_score = 0
    if yoe >= min_yoe:
        yoe_score = 30
    elif yoe >= min_yoe * 0.8:
        yoe_score = 21
    elif yoe >= min_yoe * 0.6:
        yoe_score = 12
    elif yoe >= min_yoe * 0.4:
        yoe_score = 5

    notice_days = _notice_to_days(candidate.get("notice_period", ""))
    notice_score = 20 if notice_days <= 30 else 0
    matched_preferred = [skill for skill in preferred_skills if _find_skill_match(skill_map, skill) is not None]
    local_total = round(skill_score + yoe_score + notice_score)
    return {
        **candidate,
        "local_score": local_total,
        "skill_score": round(skill_score),
        "yoe_score": yoe_score,
        "notice_score": notice_score,
        "notice_days": notice_days,
        "matched_required": matched_required,
        "missing_required": missing_required,
        "matched_preferred": matched_preferred,
        "ai_score": None,
        "ai_reason": "",
    }


def _score_shortlisted_with_ai(shortlisted: list[dict[str, Any]], jd_text: str, local_top_n: int) -> None:
    if not shortlisted:
        return
    initial_response = _score_candidates_with_gemini(jd_text, shortlisted, local_top_n)
    _merge_ai_scores(shortlisted, initial_response)
    max_retries = max(0, int(os.getenv("MAX_RETRIES", "3")))
    attempt = 0
    while attempt < max_retries:
        missing = [candidate for candidate in shortlisted if candidate.get("ai_score") is None]
        output_top_n = max(1, int(os.getenv("OUTPUT_TOP_N", "50")))
        if not missing:
            break
        if len(shortlisted) - len(missing) >= output_top_n:
            break
        attempt += 1
        sleep(1.5)
        retry_response = _score_candidates_with_gemini(jd_text, missing, len(missing))
        _merge_ai_scores(missing, retry_response)

    for candidate in shortlisted:
        if candidate.get("ai_score") is None:
            candidate["ai_reason"] = "AI score unavailable."


def _parse_jd_with_gemini(jd_text: str) -> dict[str, Any]:
    prompt = (
        "Extract structured information from this job description.\n"
        "Return ONLY valid JSON — no markdown, no explanation.\n\n"
        f"JD:\n{jd_text}\n\n"
        "Return this JSON structure:\n"
        "{\n"
        '  "job_title": "string",\n'
        '  "role_type": "one of: Backend SDE, Frontend SDE, Full Stack SDE, Data Scientist, Data Analyst, Data Engineer, ML Engineer, AI Engineer, DevOps, Other",\n'
        '  "min_yoe": 0,\n'
        '  "required_skills": [\n'
        '    { "skill": "Python", "required_months": 0 }\n'
        "  ],\n"
        '  "preferred_skills": ["skill1", "skill2"]\n'
        "}\n"
        "Rules:\n"
        "- required_skills should include must-have skills.\n"
        "- Convert year requirements to months where available.\n"
        "- Normalize skill names."
    )
    parsed = _gemini_json(prompt)
    if not isinstance(parsed, dict):
        raise RuntimeError("JD parser returned invalid response shape.")
    return parsed


def _score_candidates_with_gemini(
    jd_text: str,
    candidates: list[dict[str, Any]],
    requested_count: int,
) -> list[dict[str, Any]] | None:
    profile_separator = "\n\n---\n\n"
    profiles = []
    for idx, candidate in enumerate(candidates, start=1):
        profiles.append(
            "\n".join(
                [
                    f"Candidate {idx}:",
                    f"Email: {candidate['email']}",
                    f"Total Years of Experience: {candidate.get('yoe', 0)} years",
                    f"Notice Period: {candidate.get('notice_period') or 'Not provided'}",
                    f"Tech Stack & Validated Skills: {candidate.get('skills') or 'Not provided'}",
                    f"Work Experience: {candidate.get('work_experience') or 'Not provided'}",
                    f"Projects: {candidate.get('projects') or 'Not provided'}",
                    f"Domain: {candidate.get('domain') or 'Not provided'}",
                    f"Job Function: {candidate.get('job_function') or 'Not provided'}",
                    f"Company Type: {candidate.get('company_type') or 'Not provided'}",
                    f"Required Skills Matched: {', '.join(candidate.get('matched_required') or []) or 'None'}",
                    f"Required Skills Missing: {', '.join(candidate.get('missing_required') or []) or 'None'}",
                ]
            )
        )
    prompt = (
        "You are a senior technical recruiter with deep expertise in evaluating engineering talent.\n"
        f"You have been given a job description and {requested_count} candidate profiles.\n"
        "Score each candidate 0-100 for fit against the JD.\n"
        "Be strict and differentiated across scores.\n"
        "Return score for EVERY candidate.\n\n"
        "JOB DESCRIPTION:\n"
        f"{jd_text}\n\n"
        "CANDIDATE PROFILES:\n"
        f"{profile_separator.join(profiles)}\n\n"
        "Return ONLY a valid JSON array:\n"
        "[\n"
        "  {\n"
        '    "candidate_number": 1,\n'
        '    "email": "candidate email exactly as given",\n'
        '    "ai_score": 0,\n'
        '    "reason": "2-3 sentences on fit and gaps"\n'
        "  }\n"
        "]"
    )
    parsed = _gemini_json(prompt)
    if not isinstance(parsed, list):
        return None
    return [item for item in parsed if isinstance(item, dict)]


def _transient_gemini_exception_types() -> tuple[type[BaseException], ...]:
    try:
        from google.api_core import exceptions as gexc

        names = (
            "DeadlineExceeded",
            "ServiceUnavailable",
            "InternalServerError",
            "TooManyRequests",
            "ResourceExhausted",
        )
        return tuple(getattr(gexc, n) for n in names if hasattr(gexc, n))
    except ImportError:
        return ()


_TRANSIENT_GEMINI_EXCEPTIONS = _transient_gemini_exception_types()


def _is_transient_gemini_error(exc: BaseException) -> bool:
    if _TRANSIENT_GEMINI_EXCEPTIONS and isinstance(exc, _TRANSIENT_GEMINI_EXCEPTIONS):
        return True
    msg = str(exc).lower()
    if "deadline" in msg or "504" in msg or "503" in msg or "429" in msg:
        return True
    return False


def _gemini_json(prompt: str) -> Any:
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is required for candidate evaluation.")
    if genai is None:
        raise RuntimeError("google-generativeai package is not installed.")
    model_name = (os.getenv("GEMINI_MODEL") or "gemini-1.5-flash").strip()
    max_attempts = max(1, int(os.getenv("GEMINI_API_MAX_RETRIES", "3")))
    base_delay = float(os.getenv("GEMINI_API_RETRY_BASE_DELAY_SEC", "2.0"))
    timeout_sec = max(30.0, float(os.getenv("GEMINI_REQUEST_TIMEOUT_SEC", "600")))
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)
    request_options: dict[str, Any] = {"timeout": timeout_sec}
    for attempt in range(1, max_attempts + 1):
        try:
            try:
                response = model.generate_content(
                    prompt,
                    generation_config={"temperature": 0.1},
                    request_options=request_options,
                )
            except TypeError:
                response = model.generate_content(
                    prompt,
                    generation_config={"temperature": 0.1},
                )
            text = (getattr(response, "text", "") or "").strip()
            return _parse_json_payload(text)
        except Exception as exc:
            if not _is_transient_gemini_error(exc) or attempt >= max_attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "candidate-jd-evaluator Gemini transient error attempt=%s/%s delay=%.1fs: %s",
                attempt,
                max_attempts,
                delay,
                exc,
            )
            sleep(delay)


def _parse_json_payload(raw_text: str) -> Any:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    arr_start = text.find("[")
    arr_end = text.rfind("]")
    if arr_start != -1 and arr_end != -1 and arr_end > arr_start:
        return json.loads(text[arr_start : arr_end + 1])
    obj_start = text.find("{")
    obj_end = text.rfind("}")
    if obj_start != -1 and obj_end != -1 and obj_end > obj_start:
        return json.loads(text[obj_start : obj_end + 1])
    raise ValueError("Unable to parse JSON from Gemini response.")


def _merge_ai_scores(candidates: list[dict[str, Any]], ai_rows: list[dict[str, Any]] | None) -> None:
    if not ai_rows:
        return
    for row in ai_rows:
        email = str(row.get("email") or "").strip().lower()
        number = row.get("candidate_number")
        matched = None
        if email:
            matched = next((item for item in candidates if item.get("email", "").strip().lower() == email), None)
        if not matched and isinstance(number, int) and 1 <= number <= len(candidates):
            matched = candidates[number - 1]
        if not matched:
            continue
        score = row.get("ai_score")
        matched["ai_score"] = int(score) if isinstance(score, (int, float)) else None
        matched["ai_reason"] = str(row.get("reason") or "").strip()


def _build_candidate_match_summary_row(summary: dict[str, Any], jd_context: dict[str, str]) -> dict[str, Any]:
    """Single row for candidate_match sheet: job_url plus AI>70 summary (same AI logic as before)."""
    return {
        "job_url": (jd_context.get("job_url") or "").strip(),
        "ai_score_gt_70_count": summary.get("count", 0),
        "ai_score_gt_70_emails": summary.get("emails_csv", ""),
    }


def _build_output_sheet_name(run_date: str) -> str:
    return f"candidate_match_{run_date}"


def _ai_gt_70_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    qualified_emails: list[str] = []
    for candidate in candidates:
        score = candidate.get("ai_score")
        if not isinstance(score, (int, float)):
            continue
        if float(score) <= 70:
            continue
        email = str(candidate.get("email") or "").strip()
        if email:
            qualified_emails.append(email)
    return {
        "count": len(qualified_emails),
        "emails_csv": ", ".join(qualified_emails),
    }


def _extract_job_url(matched_row: dict[str, str], recruiter_row: dict[str, str], recruiter_job_url_key: str | None) -> str:
    matched_job_url_key = _find_first_key(matched_row, ["job_url", "url", "link"])
    if matched_job_url_key:
        url = (matched_row.get(matched_job_url_key) or "").strip()
        if url:
            return url
    if recruiter_job_url_key:
        return (recruiter_row.get(recruiter_job_url_key) or "").strip()
    return ""


def _top_score(candidates: list[dict[str, Any]]) -> int:
    scores = [int(item["ai_score"]) for item in candidates if item.get("ai_score") is not None]
    return max(scores) if scores else 0


def _sort_ai_score(score: Any) -> float:
    if score is None:
        return -1.0
    try:
        return float(score)
    except (TypeError, ValueError):
        return -1.0


def _to_float(value: Any) -> float:
    try:
        return float(str(value or "").strip())
    except ValueError:
        return 0.0


def _parse_int(value: Any) -> int | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return int(text)
    except ValueError:
        return None


def _normalize_job_url(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    netloc = parsed.netloc.lower().strip()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = (parsed.path or "").rstrip("/")
    return f"{netloc}{path}".strip()


def _require_columns(rows: list[dict[str, str]], columns: list[str], source_name: str) -> None:
    first = rows[0] if rows else {}
    missing = [column for column in columns if column not in first]
    if missing:
        raise RuntimeError(f"Missing required columns in {source_name}: {', '.join(missing)}")


def _find_first_key(row: dict[str, str], candidates: list[str]) -> str | None:
    keys = [key.strip().lower() for key in row.keys()]
    for candidate in candidates:
        normalized = candidate.strip().lower()
        if normalized in keys:
            return normalized
    for key in keys:
        for candidate in candidates:
            normalized = candidate.strip().lower()
            if normalized in key or key in normalized:
                return key
    return None


def _normalize_date(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.split("T")[0].split(" ")[0].strip()
    m = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$", text)
    if m:
        year, month, day = m.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$", text)
    if m:
        day, month, year = m.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return text


def _parse_skill_map(skill_text: str) -> dict[str, int]:
    out: dict[str, int] = {}
    if not skill_text:
        return out
    parts = skill_text.split("|||") if "|||" in skill_text else skill_text.split("|")
    for part in parts:
        match = re.match(r"^(.+?)\s*\((\d+)\s*mo\)", part.strip(), flags=re.IGNORECASE)
        if not match:
            continue
        skill = match.group(1).strip().lower()
        months = int(match.group(2))
        out[skill] = months
    return out


def _find_skill_match(skill_map: dict[str, int], jd_skill: str) -> int | None:
    normalized = jd_skill.strip().lower()
    for candidate_skill, months in skill_map.items():
        if candidate_skill in normalized or normalized in candidate_skill:
            return months
    return None


def _notice_to_days(value: str) -> int:
    text = str(value or "").strip().lower()
    if not text:
        return 10**9
    if "immediate" in text:
        return 0
    m = re.search(r"(\d+)", text)
    if not m:
        return 10**9
    n = int(m.group(1))
    if "month" in text:
        return n * 30
    if "week" in text:
        return n * 7
    return n
