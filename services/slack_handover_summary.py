"""Slack handover summary sender for jobs + LinkedIn posts pipelines."""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import worksheet_row_dicts
from services.slack_handover_notify import merge_slack_defaults, send_slack_text

logger = logging.getLogger(__name__)


def _load_rows(tab_name: str) -> list[dict[str, str]]:
    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        logger.info("handover summary: GOOGLE_SPREADSHEET_ID not configured")
        return []
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        worksheet = writer.open_worksheet(tab_name)
        raw = writer.worksheet_get_all_values(worksheet, f"handover_summary:{tab_name}:get_all_values")
    except Exception as exc:
        logger.warning("handover summary: worksheet unavailable tab=%s err=%s", tab_name, exc)
        return []
    return worksheet_row_dicts(raw)


def _count_recruiter_case3(rows: list[dict[str, str]]) -> int:
    return sum(1 for row in rows if (row.get("recruiter_profile_url") or "").strip())


def _count_recruiter_case2(rows: list[dict[str, str]]) -> int:
    total = 0
    for row in rows:
        has_profile = bool((row.get("recruiter_profile_url") or "").strip())
        has_email = bool((row.get("recruiter_email") or "").strip())
        if (not has_profile) and has_email:
            total += 1
    return total


def build_handover_summary(
    run_date: str | None = None,
    *,
    send_linkedin_post: bool = True,
    send_recruiter_info: bool = True,
    send_internal_poc: bool = True,
) -> dict[str, Any]:
    rd = (run_date or date.today().isoformat()).strip()

    scraped_jobs_rows = _load_rows(f"scraped_jobs_{rd}")
    relevant_jobs_rows = _load_rows(f"relevant_jobs_{rd}")
    recruiter_rows = _load_rows(f"recruiters_info_{rd}")
    linkedin_posts_scraped_rows = _load_rows(f"linkedin_posts_scraped_{rd}")
    linkedin_posts_relevant_rows = _load_rows(f"linkedin_posts_relevant_{rd}")

    scraped_jobs_count = len(scraped_jobs_rows)
    relevant_jobs_count = len(relevant_jobs_rows)
    linkedin_recruiter_count = _count_recruiter_case3(recruiter_rows)
    internal_poc_count = _count_recruiter_case2(recruiter_rows)
    linkedin_posts_scraped_count = len(linkedin_posts_scraped_rows)
    linkedin_posts_relevant_count = len(linkedin_posts_relevant_rows)

    handover_count = 0
    if send_recruiter_info:
        handover_count += linkedin_recruiter_count
    if send_internal_poc:
        handover_count += internal_poc_count
    if send_linkedin_post:
        handover_count += linkedin_posts_relevant_count

    return {
        "run_date": rd,
        "send_linkedin_post": send_linkedin_post,
        "send_recruiter_info": send_recruiter_info,
        "send_internal_poc": send_internal_poc,
        "scraped_jobs_count": scraped_jobs_count,
        "relevant_jobs_count": relevant_jobs_count,
        "linkedin_recruiter_count": linkedin_recruiter_count,
        "internal_poc_count": internal_poc_count,
        "linkedin_posts_scraped_count": linkedin_posts_scraped_count,
        "linkedin_posts_relevant_count": linkedin_posts_relevant_count,
        "handover_count": handover_count,
    }


def format_handover_summary_message(summary: dict[str, Any]) -> str:
    parts: list[str] = [":bar_chart: *DAILY HANDOVER SUMMARY*"]
    parts.append(f"run_date: {summary['run_date']}")
    parts.append("")

    all_three = (
        summary["send_linkedin_post"]
        and summary["send_recruiter_info"]
        and summary["send_internal_poc"]
    )
    if all_three:
        parts.extend(
            [
                f"job scraped - {summary['scraped_jobs_count']}",
                f"relevant - {summary['relevant_jobs_count']}",
                (
                    f"handover - {summary['linkedin_posts_relevant_count']} + "
                    f"{summary['internal_poc_count']} + {summary['linkedin_recruiter_count']} = "
                    f"{summary['handover_count']}"
                ),
                f"linkedin total post count- {summary['linkedin_posts_scraped_count']}",
                f"relevant post classified count- {summary['linkedin_posts_relevant_count']}",
                f"internal poc count- {summary['internal_poc_count']}",
                f"linkedin recruiter details count - {summary['linkedin_recruiter_count']}",
            ]
        )
        return "\n".join(parts).strip()

    recruiter_and_poc_enabled = summary["send_recruiter_info"] and summary["send_internal_poc"]
    if recruiter_and_poc_enabled:
        parts.extend(
            [
                f"scraped jobs count: {summary['scraped_jobs_count']}",
                f"relevant jobs count: {summary['relevant_jobs_count']}",
                "",
                f"linkedin recruiter count: {summary['linkedin_recruiter_count']}",
                "",
                f"internal poc matched jobs count: {summary['internal_poc_count']}",
                "",
            ]
        )
    elif summary["send_recruiter_info"]:
        parts.extend(
            [
                f"scraped jobs count: {summary['scraped_jobs_count']}",
                f"relevant jobs count: {summary['relevant_jobs_count']}",
                f"linkedin recruiter count: {summary['linkedin_recruiter_count']}",
                "",
            ]
        )
    elif summary["send_internal_poc"]:
        parts.extend(
            [
                f"scraped jobs count: {summary['scraped_jobs_count']}",
                f"relevant jobs count: {summary['relevant_jobs_count']}",
                f"internal poc matched jobs count: {summary['internal_poc_count']}",
                "",
            ]
        )

    if summary["send_linkedin_post"]:
        parts.extend(
            [
                f"linkedin post scraped count: {summary['linkedin_posts_scraped_count']}",
                f"relevant post classified count: {summary['linkedin_posts_relevant_count']}",
                "",
            ]
        )

    return "\n".join(parts).strip()


def send_handover_summary_to_slack(
    run_date: str | None = None,
    *,
    send_linkedin_post: bool = True,
    send_recruiter_info: bool = True,
    send_internal_poc: bool = True,
    webhook_url: str | None = None,
    channel: str | None = None,
    username: str | None = None,
    icon_emoji: str | None = None,
) -> dict[str, Any]:
    summary = build_handover_summary(
        run_date=run_date,
        send_linkedin_post=send_linkedin_post,
        send_recruiter_info=send_recruiter_info,
        send_internal_poc=send_internal_poc,
    )
    defaults = merge_slack_defaults(
        webhook_url=webhook_url,
        channel=channel,
        username=username,
        icon_emoji=icon_emoji,
    )

    if not defaults.webhook_url:
        summary["summary_message_sent"] = False
        summary["skipped_reason"] = "SLACK_WEBHOOK_URL not configured"
        return summary

    message = format_handover_summary_message(summary)
    sent = send_slack_text(message, defaults=defaults, sleep_after=0.0)
    summary["summary_message_sent"] = bool(sent)
    summary["summary_message"] = message
    return summary
