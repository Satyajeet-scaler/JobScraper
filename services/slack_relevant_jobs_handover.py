"""
Role-pipeline relevant-jobs Slack handover.

This service reads rows directly from a role's relevant-jobs tab
(``role_relevant_{role_slug}_{date}``) -- NOT from the recruiter info tab --
and posts incoming lead messages to Slack with per-role filtering rules.

Per-role rules (``handover.min_candidate_match``):
  - Data Analyst:       all relevant jobs are handed over (Slack text omits candidate-match line).
  - Software Developer: only jobs with candidate match count >= 10.
  - DevOps:             only jobs with candidate match count >= 10.

Dedup across repeated cron runs in the same day uses a ``handover_sent``
column on the relevant tab. Once a row has been posted to Slack, the column
is stamped with a timestamp so subsequent runs skip it.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import load_owner_rows_for_handover, worksheet_row_dicts
from services.role_pipeline import _resolve_role_config, role_relevant_tab_name, _role_slug
from services.slack_handover_notify import (
    _normalize_job_url_for_match,
    owner_tag_for_handover,
    send_slack_text,
    slack_notify_defaults_from_env,
)

logger = logging.getLogger(__name__)


HANDOVER_SENT_COLUMN = "handover_sent"
ASSIGNED_OWNER_COLUMN = "assigned owner"
HEADING_RELEVANT_LEADS = ":rotating_light: *INCOMING RELEVANT LEADS*"


def _role_heading(role: str) -> str:
    """Top-level Slack heading for one role's batch."""
    return f"{HEADING_RELEVANT_LEADS} — *{role.strip()}*"


# Default per-role handover rules. Overridable per role via the
# ``handover`` key inside ROLE_PIPELINE_ROLE_CONFIG_JSON / ROLE_CONFIG_MAP.
DEFAULT_HANDOVER_RULES: dict[str, dict[str, Any]] = {
    "data analyst":       {"min_candidate_match": 0},
    "software developer": {"min_candidate_match": 10},
    "devops":             {"min_candidate_match": 10},
}


def send_relevant_jobs_handover(
    run_date: str | None = None,
    *,
    role: str | None = None,
) -> dict[str, Any]:
    """Send Slack leads for relevant jobs of *role* on *run_date*.

    Returns a summary dict with counts and skipped-reason info.
    """
    resolved_date = (run_date or date.today().isoformat()).strip()
    resolved_role = (role or "").strip()
    if not resolved_role:
        raise ValueError("role is required.")
    role_slug = _role_slug(resolved_role)
    relevant_tab = role_relevant_tab_name(role=resolved_role, run_date=resolved_date)

    out: dict[str, Any] = {
        "run_date": resolved_date,
        "role": resolved_role,
        "role_slug": role_slug,
        "relevant_tab": relevant_tab,
        "relevant_rows_total": 0,
        "eligible_after_filter": 0,
        "messages_sent": 0,
        "handover_sent_rows_updated": 0,
        "assigned_owner_rows_updated": 0,
        "min_candidate_match": _resolve_min_candidate_match(resolved_role),
        "skipped_reason": None,
    }

    defaults = slack_notify_defaults_from_env()
    if not defaults.webhook_url:
        out["skipped_reason"] = "SLACK_WEBHOOK_URL not configured"
        return out

    spreadsheet_id = _role_pipeline_spreadsheet_id()
    if not spreadsheet_id:
        out["skipped_reason"] = "ROLE_PIPELINE_GOOGLE_SPREADSHEET_ID/GOOGLE_SPREADSHEET_ID not configured"
        return out

    relevant_rows = _read_relevant_rows(spreadsheet_id, relevant_tab)
    out["relevant_rows_total"] = len(relevant_rows)
    if not relevant_rows:
        out["skipped_reason"] = "no relevant rows"
        return out

    candidate_count_map = _load_role_candidate_match_count_map(
        role=resolved_role,
        run_date=resolved_date,
    )

    min_count = out["min_candidate_match"]
    include_cm_slack = _role_includes_candidate_match_in_slack(resolved_role)
    eligible: list[dict[str, str]] = []
    for row in relevant_rows:
        if not _is_handover_sent_empty(row):
            continue
        if not _is_assigned_owner_empty(row):
            continue
        job_url = (row.get("job_url") or "").strip()
        if not job_url:
            continue
        count = candidate_count_map.get(_normalize_job_url_for_match(job_url), 0)
        if count < min_count:
            continue
        row["_candidate_match_count"] = str(count)
        eligible.append(row)

    out["eligible_after_filter"] = len(eligible)
    if not eligible:
        out["skipped_reason"] = out["skipped_reason"] or "no eligible rows after filter"
        return out

    owner_rows = load_owner_rows_for_handover() or []
    if not owner_rows:
        out["skipped_reason"] = "no owner rows from handover_owners sheet"
        return out

    # Top-level heading per role (one per call -> "group by roles" in Slack).
    if not send_slack_text(_role_heading(resolved_role), defaults=defaults, sleep_after=1.0):
        out["skipped_reason"] = "failed to post heading"
        return out
    out["messages_sent"] += 1

    sent_identities: set[tuple[str, str]] = set()
    sent_timestamp = _now_iso()

    # Round-robin allocation, then group consecutively by owner so each owner
    # receives one sub-heading followed by all of their leads.
    owner_buckets: dict[int, list[dict[str, str]]] = {i: [] for i in range(len(owner_rows))}
    for idx, row in enumerate(eligible):
        owner_buckets[idx % len(owner_rows)].append(row)

    # Iterate owner-by-owner so all leads for one owner are posted back-to-back.
    # Owner tag is inlined on each lead (no separate sub-heading).
    for owner_idx, owner in enumerate(owner_rows):
        bucket = owner_buckets.get(owner_idx, [])
        if not bucket:
            continue
        owner_tag = owner_tag_for_handover(owner)
        for row in bucket:
            company = (row.get("company") or "-").strip() or "-"
            lead_role = (row.get("matched_role") or row.get("role_category") or row.get("title") or "-").strip() or "-"
            job_url = (row.get("job_url") or "-").strip() or "-"
            count = int(row.get("_candidate_match_count") or 0)
            msg = format_relevant_jobs_lead(
                owner_tag=owner_tag,
                company=company,
                role=lead_role,
                job_url=job_url,
                candidate_match_count=count,
                include_candidate_match=include_cm_slack,
            )
            if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                out["messages_sent"] += 1
                sent_identities.add(_relevant_row_identity(row))

    if sent_identities:
        updated = _persist_handover_markers(
            spreadsheet_id=spreadsheet_id,
            worksheet_title=relevant_tab,
            owner_rows=owner_rows,
            sent_identities=sent_identities,
            sent_timestamp=sent_timestamp,
        )
        out["handover_sent_rows_updated"] = updated
        out["assigned_owner_rows_updated"] = updated

    return out


def format_relevant_jobs_lead(
    *,
    company: str,
    role: str,
    job_url: str,
    candidate_match_count: int,
    owner_tag: str | None = None,
    include_candidate_match: bool = True,
) -> str:
    """Slack message body for a single relevant-jobs lead.

    ``owner_tag`` is optional: when leads are posted as part of an
    owner-grouped batch the tag is sent once as a sub-heading, so each
    individual lead body omits it. Pass ``owner_tag`` to render the
    legacy single-message format with the tag inlined.
    """
    head = f"{owner_tag.strip()}\n" if owner_tag and owner_tag.strip() else ""
    body = (
        f"{head}"
        f"Company: {company}\n"
        f"Role: {role}\n"
        f"Job Url: {job_url}\n"
    )
    if include_candidate_match:
        body += f"Candidate match: {candidate_match_count} candidate(s) with AI score > 70"
    return body.rstrip("\n")


def _role_includes_candidate_match_in_slack(role: str) -> bool:
    return role.strip().lower() != "data analyst"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _role_pipeline_spreadsheet_id() -> str:
    return (
        os.getenv("ROLE_PIPELINE_GOOGLE_SPREADSHEET_ID")
        or os.getenv("GOOGLE_SPREADSHEET_ID")
        or ""
    ).strip()


def _read_relevant_rows(spreadsheet_id: str, tab: str) -> list[dict[str, str]]:
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(tab)
        raw = writer.worksheet_get_all_values(
            ws,
            f"relevant_jobs_handover:{tab}:get_all_values",
        )
    except Exception as exc:
        logger.warning(
            "relevant jobs handover: tab unavailable tab=%s err=%s",
            tab,
            exc,
        )
        return []
    return [dict(r) for r in worksheet_row_dicts(raw)]


def _is_handover_sent_empty(row: dict[str, str]) -> bool:
    return not str(row.get(HANDOVER_SENT_COLUMN) or "").strip()


def _is_assigned_owner_empty(row: dict[str, str]) -> bool:
    return not str(
        row.get(ASSIGNED_OWNER_COLUMN) or row.get("assigned_owner") or ""
    ).strip()


def _relevant_row_identity(row: dict[str, str]) -> tuple[str, str]:
    job_url = _normalize_job_url_for_match(row.get("job_url") or "")
    site = str(row.get("site") or "").strip().lower()
    return (site, job_url)


def _resolve_min_candidate_match(role: str) -> int:
    """Per-role threshold from role config, falling back to defaults."""
    try:
        role_cfg = _resolve_role_config(role)
    except Exception:
        role_cfg = {}
    handover_cfg = role_cfg.get("handover") if isinstance(role_cfg, dict) else None
    if isinstance(handover_cfg, dict):
        raw = handover_cfg.get("min_candidate_match")
        parsed = _parse_int(raw)
        if parsed is not None:
            return parsed

    default = DEFAULT_HANDOVER_RULES.get(role.strip().lower())
    if default is not None:
        return int(default.get("min_candidate_match", 0))
    return 0


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _now_iso() -> str:
    try:
        tz = ZoneInfo(os.getenv("CRON_TIMEZONE", "Asia/Kolkata"))
    except Exception:
        tz = None
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S") if tz else datetime.utcnow().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _load_role_candidate_match_count_map(
    *,
    role: str,
    run_date: str,
) -> dict[str, int]:
    """Read role-specific ``candidate_match_{role_slug}_{date}`` (with fallback
    to the generic ``candidate_match_{date}`` tab), returning normalized
    ``job_url -> ai_score_gt_70_count``.
    """
    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        return {}

    role_slug = _role_slug(role)
    role_tab = _role_candidate_match_tab(role_slug=role_slug, run_date=run_date)
    fallback_tab = _generic_candidate_match_tab(run_date)

    rows = _read_candidate_match_rows(spreadsheet_id, role_tab)
    if not rows and fallback_tab != role_tab:
        rows = _read_candidate_match_rows(spreadsheet_id, fallback_tab)

    out: dict[str, int] = {}
    for row in rows:
        job_url = (row.get("job_url") or row.get("url") or row.get("link") or "").strip()
        key = _normalize_job_url_for_match(job_url)
        if not key:
            continue
        count = _parse_int(row.get("ai_score_gt_70_count")) or 0
        out[key] = count
    return out


def _role_candidate_match_tab(*, role_slug: str, run_date: str) -> str:
    template = (
        os.getenv("ROLE_PIPELINE_CANDIDATE_MATCH_TAB_TEMPLATE")
        or "candidate_match_{role_slug}_{date}"
    ).strip()
    return template.format(role_slug=role_slug, date=run_date)


def _generic_candidate_match_tab(run_date: str) -> str:
    template = (os.getenv("CANDIDATE_MATCH_WORKSHEET_TEMPLATE") or "candidate_match_{date}").strip()
    return template.replace("{date}", run_date)


def _read_candidate_match_rows(spreadsheet_id: str, tab: str) -> list[dict[str, str]]:
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(tab)
        raw = writer.worksheet_get_all_values(
            ws,
            f"relevant_jobs_handover_candidate_match:{tab}:get_all_values",
        )
    except Exception as exc:
        logger.info(
            "relevant jobs handover: candidate_match tab unavailable tab=%s err=%s",
            tab,
            exc,
        )
        return []
    return [dict(r) for r in worksheet_row_dicts(raw)]


def _persist_handover_markers(
    *,
    spreadsheet_id: str,
    worksheet_title: str,
    owner_rows: list[dict[str, str]],
    sent_identities: set[tuple[str, str]],
    sent_timestamp: str,
) -> int:
    """Write ``assigned owner`` (round-robin) and ``handover_sent`` timestamp
    back to the relevant tab for every row in ``sent_identities``.

    Returns the number of rows updated.
    """
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(worksheet_title)
        values = writer.worksheet_get_all_values(
            ws,
            f"relevant_jobs_handover:{worksheet_title}:get_all_values",
        )
        if not values:
            return 0
        headers = [str(h or "").strip() for h in values[0]]
        data_rows = [list(r) for r in values[1:]]

        normalized_headers = [h.lower() for h in headers]
        assigned_col = _ensure_header_column(headers, normalized_headers, ASSIGNED_OWNER_COLUMN)
        sent_col = _ensure_header_column(headers, normalized_headers, HANDOVER_SENT_COLUMN)

        for row in data_rows:
            while len(row) < len(headers):
                row.append("")

        owner_names = [_owner_display_name(o) for o in owner_rows] or ["Unassigned"]

        selected_positions: list[int] = []
        for pos, row in enumerate(data_rows):
            row_dict: dict[str, str] = {}
            for idx, header in enumerate(normalized_headers):
                row_dict[header] = row[idx].strip() if idx < len(row) else ""
            identity = _relevant_row_identity(row_dict)
            if identity not in sent_identities:
                continue
            if row_dict.get(HANDOVER_SENT_COLUMN):
                continue
            selected_positions.append(pos)

        if not selected_positions:
            return 0

        for order_idx, pos in enumerate(selected_positions):
            data_rows[pos][assigned_col] = owner_names[order_idx % len(owner_names)]
            data_rows[pos][sent_col] = sent_timestamp

        writer.worksheet_update(
            ws,
            "A1",
            [headers],
            f"relevant_jobs_handover:{worksheet_title}:update_headers",
        )

        _write_single_column(writer, ws, worksheet_title, assigned_col, data_rows)
        _write_single_column(writer, ws, worksheet_title, sent_col, data_rows)

        logger.info(
            "relevant jobs handover: persisted markers sheet=%s tab=%s updated_rows=%s",
            spreadsheet_id,
            worksheet_title,
            len(selected_positions),
        )
        return len(selected_positions)
    except Exception as exc:
        logger.warning(
            "relevant jobs handover: failed to persist markers sheet=%s tab=%s err=%s",
            spreadsheet_id,
            worksheet_title,
            exc,
        )
        return 0


def _ensure_header_column(
    headers: list[str],
    normalized_headers: list[str],
    header_name: str,
) -> int:
    lower = header_name.lower()
    if lower in normalized_headers:
        return normalized_headers.index(lower)
    headers.append(header_name)
    normalized_headers.append(lower)
    return len(headers) - 1


def _column_letter(index: int) -> str:
    letters = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _write_single_column(
    writer: GoogleSheetsWriter,
    ws: Any,
    worksheet_title: str,
    col_idx: int,
    data_rows: list[list[str]],
) -> None:
    end_row = len(data_rows) + 1
    if end_row < 2:
        return
    col_values = [[row[col_idx]] for row in data_rows]
    col_letter = _column_letter(col_idx + 1)
    rng = f"{col_letter}2:{col_letter}{end_row}"
    writer.worksheet_update(
        ws,
        rng,
        col_values,
        f"relevant_jobs_handover:{worksheet_title}:update_col_{col_letter}",
    )


def _owner_display_name(owner: dict[str, str]) -> str:
    name = (owner.get("owner_name") or "").strip()
    if name:
        return name
    email = (owner.get("owner_email") or "").strip()
    if email:
        return email
    sid = (owner.get("owner_slack_id") or "").strip()
    return sid or "Owner"


__all__ = [
    "send_relevant_jobs_handover",
    "format_relevant_jobs_lead",
    "HEADING_RELEVANT_LEADS",
    "DEFAULT_HANDOVER_RULES",
    "HANDOVER_SENT_COLUMN",
]
