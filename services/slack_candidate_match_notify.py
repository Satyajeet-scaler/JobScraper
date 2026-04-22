"""Slack notifications for candidate_match sheet: one message per job (URL + AI>70 count)."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import gspread

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.slack_handover_notify import send_slack_text, slack_notify_defaults_from_env

logger = logging.getLogger(__name__)


def _resolve_run_date(run_date: str | None) -> str:
    if run_date and str(run_date).strip():
        return str(run_date).strip()
    tz = ZoneInfo(os.getenv("CRON_TIMEZONE", "Asia/Kolkata"))
    return datetime.now(tz).strftime("%Y-%m-%d")


def _worksheet_title_for_date(run_date: str) -> str:
    template = (os.getenv("CANDIDATE_MATCH_WORKSHEET_TEMPLATE") or "candidate_match_{date}").strip()
    return template.replace("{date}", run_date)


def _find_key(row: dict[str, str], candidates: list[str]) -> str | None:
    keys = list(row.keys())
    for want in candidates:
        w = want.strip().lower()
        for k in keys:
            if k.lower() == w:
                return k
    for want in candidates:
        w = want.strip().lower()
        for k in keys:
            kl = k.lower()
            if w in kl or kl in w:
                return k
    return None


def _parse_count(raw: str) -> int | None:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def send_candidate_match_slack_notifications(run_date: str | None = None) -> dict[str, Any]:
    """
    Read ``candidate_match_{run_date}`` (or ``CANDIDATE_MATCH_WORKSHEET_TEMPLATE``) and post one Slack
    message per row: job URL and count of candidates with AI score > 70.
    Uses the same ``SLACK_*`` env defaults as handover.
    """
    resolved = _resolve_run_date(run_date)
    tab = _worksheet_title_for_date(resolved)
    defaults = slack_notify_defaults_from_env()

    if not defaults.webhook_url:
        out = {
            "run_date": resolved,
            "worksheet": tab,
            "rows_read": 0,
            "messages_sent": 0,
            "skipped_reason": "SLACK_WEBHOOK_URL not configured",
        }
        logger.info("candidate-match slack skipped: %s", out["skipped_reason"])
        return out

    use_mysql = (os.getenv("ROLE_PIPELINE_MYSQL_READ_ENABLED") or "false").strip().lower() in ("1", "true", "yes")
    sleep_between = float(os.getenv("CANDIDATE_MATCH_SLACK_SLEEP_SEC", "1.0"))
    
    if use_mysql:
        from services.mysql_jobs_store import fetch_all_candidate_match_counts
        counts_map = fetch_all_candidate_match_counts(run_date=resolved)
        messages_sent = 0
        date_header = f":calendar: *Candidate match date:* `{resolved}`"
        if send_slack_text(date_header, defaults=defaults, sleep_after=sleep_between, log_skip_message=None):
            messages_sent += 1
        for job_url, count_val in counts_map.items():
            text = f"*Candidate match* — *{count_val}* candidate(s) with AI score > 70\n{job_url}"
            if send_slack_text(text, defaults=defaults, sleep_after=sleep_between, log_skip_message=None):
                messages_sent += 1
        return {
            "run_date": resolved,
            "worksheet": "mysql_db",
            "rows_read": len(counts_map),
            "messages_sent": messages_sent,
            "skipped_reason": None,
        }

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        out = {
            "run_date": resolved,
            "worksheet": tab,
            "rows_read": 0,
            "messages_sent": 0,
            "skipped_reason": "GOOGLE_SPREADSHEET_ID not configured",
        }
        logger.info("candidate-match slack skipped: %s", out["skipped_reason"])
        return out

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    try:
        ws = writer.open_worksheet(tab)
    except gspread.WorksheetNotFound:
        out = {
            "run_date": resolved,
            "worksheet": tab,
            "rows_read": 0,
            "messages_sent": 0,
            "skipped_reason": "worksheet not found",
        }
        logger.info("candidate-match slack skipped tab=%s: %s", tab, out["skipped_reason"])
        return out

    raw = writer.worksheet_get_all_values(ws, f"candidate_match_slack:{tab}:get_all_values")
    rows = worksheet_row_dicts(raw)
    if not rows:
        out = {
            "run_date": resolved,
            "worksheet": tab,
            "rows_read": 0,
            "messages_sent": 0,
            "skipped_reason": "no data rows",
        }
        logger.info("candidate-match slack skipped tab=%s: %s", tab, out["skipped_reason"])
        return out

    sample = rows[0]
    url_key = _find_key(sample, ["job_url", "url", "link"])
    count_key = _find_key(sample, ["ai_score_gt_70_count", "ai score gt 70 count"])
    if not url_key or not count_key:
        out = {
            "run_date": resolved,
            "worksheet": tab,
            "rows_read": len(rows),
            "messages_sent": 0,
            "skipped_reason": f"missing columns need job_url-like and ai_score_gt_70_count-like; url_key={url_key!r} count_key={count_key!r}",
        }
        logger.warning("candidate-match slack skipped tab=%s: %s", tab, out["skipped_reason"])
        return out

    sleep_between = float(os.getenv("CANDIDATE_MATCH_SLACK_SLEEP_SEC", "1.0"))
    messages_sent = 0
    date_header = f":calendar: *Candidate match date:* `{resolved}`"
    if send_slack_text(
        date_header,
        defaults=defaults,
        sleep_after=sleep_between,
        log_skip_message=None,
    ):
        messages_sent += 1

    for row in rows:
        job_url = (row.get(url_key) or "").strip()
        count_val = _parse_count(row.get(count_key) or "")
        if count_val is None:
            count_val = 0
        if not job_url:
            logger.warning("candidate-match slack skipping row with empty job_url tab=%s", tab)
            continue
        text = (
            f"*Candidate match* — *{count_val}* candidate(s) with AI score > 70\n"
            f"{job_url}"
        )
        if send_slack_text(
            text,
            defaults=defaults,
            sleep_after=sleep_between,
            log_skip_message=None,
        ):
            messages_sent += 1

    return {
        "run_date": resolved,
        "worksheet": tab,
        "rows_read": len(rows),
        "messages_sent": messages_sent,
        "skipped_reason": None,
    }
