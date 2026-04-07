"""Slack handover notifications: LinkedIn post, recruiter LinkedIn profile, internal POC."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from enum import Enum
from time import sleep
from typing import Any, Callable, TypeVar

import requests

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import load_owner_rows_for_handover, worksheet_row_dicts
from services.linkedin_posts_slack_row import slack_author_from_row, slack_post_url_from_row

logger = logging.getLogger(__name__)

T = TypeVar("T")


class HandoverSlackCase(str, Enum):
    LINKEDIN_POST = "linkedin_post"
    RECRUITER_DETAIL = "recruiter_detail"
    INTERNAL_POC = "internal_poc"


HEADING_LINKEDIN_POST = ":rotating_light: *INCOMING LINKEDIN JOB POST VIA VALIDATED AUTHOR*"
HEADING_RECRUITER_DETAIL = (
    ":rotating_light: *INCOMING LEAD WITH RECRUITER DETAILS AVAILABLE ON LINKEDIN*"
)
HEADING_INTERNAL_POC = ":rotating_light: *INCOMING LEAD with EXISTING INTERNAL POC*"


def heading_for_case(case: HandoverSlackCase | str) -> str:
    c = HandoverSlackCase(case) if isinstance(case, str) else case
    return {
        HandoverSlackCase.LINKEDIN_POST: HEADING_LINKEDIN_POST,
        HandoverSlackCase.RECRUITER_DETAIL: HEADING_RECRUITER_DETAIL,
        HandoverSlackCase.INTERNAL_POC: HEADING_INTERNAL_POC,
    }[c]


@dataclass(frozen=True)
class SlackNotifyDefaults:
    webhook_url: str | None
    channel: str
    username: str
    icon_emoji: str


def slack_notify_defaults_from_env() -> SlackNotifyDefaults:
    return SlackNotifyDefaults(
        webhook_url=(os.getenv("SLACK_WEBHOOK_URL") or None),
        channel=os.getenv("SLACK_CHANNEL", "relevant-scraped-jobs"),
        username=os.getenv("SLACK_USERNAME", "Karan Bot"),
        icon_emoji=os.getenv("SLACK_ICON_EMOJI", ":karandeep:"),
    )


def merge_slack_defaults(
    *,
    webhook_url: str | None = None,
    channel: str | None = None,
    username: str | None = None,
    icon_emoji: str | None = None,
) -> SlackNotifyDefaults:
    """Env-based defaults with optional per-call overrides (``None`` keeps env value)."""
    base = slack_notify_defaults_from_env()
    return SlackNotifyDefaults(
        webhook_url=webhook_url if webhook_url is not None else base.webhook_url,
        channel=channel if channel is not None else base.channel,
        username=username if username is not None else base.username,
        icon_emoji=icon_emoji if icon_emoji is not None else base.icon_emoji,
    )


def owner_tag_for_handover(owner: dict[str, str]) -> str:
    name = (owner.get("owner_name") or "Owner").strip() or "Owner"
    sid = (owner.get("owner_slack_id") or "").strip()
    return f"*{name}* (<@{sid}>)" if sid else f"*{name}*"


def load_recruiter_rows_split_for_handover(
    run_date: str,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    """Load recruiters sheet; return (all_filtered_rows, case3_profile_rows, case2_email_rows)."""
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.info("handover sheets skipped: GOOGLE_SPREADSHEET_ID not configured")
        return [], [], []

    recruiters_tab = os.getenv("RECRUITERS_INFO_WORKSHEET") or f"recruiters_info_{run_date}"
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        recruiters_ws = writer.sheet.worksheet(recruiters_tab)
    except Exception as exc:
        logger.warning("recruiter sheet unavailable tab=%s err=%s", recruiters_tab, exc)
        return [], [], []

    recruiter_rows = worksheet_row_dicts(recruiters_ws)
    filtered: list[dict[str, str]] = []
    for row in recruiter_rows:
        row_run_date = (row.get("run_date") or "").strip()
        if row_run_date and row_run_date != run_date:
            continue
        filtered.append(row)

    case3: list[dict[str, str]] = []
    case2: list[dict[str, str]] = []
    for row in filtered:
        if (row.get("recruiter_profile_url") or "").strip():
            case3.append(row)
        elif (row.get("recruiter_email") or "").strip():
            case2.append(row)
    return filtered, case3, case2


def load_linkedin_relevant_posts_from_sheet(run_date: str) -> list[dict[str, Any]]:
    """Read ``linkedin_posts_relevant_{run_date}`` tab."""
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        return []
    tab = f"linkedin_posts_relevant_{run_date}"
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.sheet.worksheet(tab)
        rows = worksheet_row_dicts(ws)
        logger.info("loaded %s relevant linkedin posts from sheet %s", len(rows), tab)
        return list(rows)
    except Exception as exc:
        logger.warning("failed to load relevant linkedin posts sheet=%s err=%s", tab, exc)
        return []


def send_recruiter_handover_case(
    rows: list[dict[str, str]],
    owner_rows: list[dict[str, str]],
    case: HandoverSlackCase,
    *,
    defaults: SlackNotifyDefaults,
) -> int:
    """One case heading + round-robin owner assignment. Returns Slack POST count (0 if none)."""
    if not rows or not owner_rows:
        return 0
    sent = 0
    if not send_slack_text(heading_for_case(case), defaults=defaults, sleep_after=1.0):
        return 0
    sent += 1

    owner_buckets: dict[int, list[dict[str, str]]] = {i: [] for i in range(len(owner_rows))}
    for idx, row in enumerate(rows):
        owner_buckets[idx % len(owner_rows)].append(row)

    for owner_idx, owner in enumerate(owner_rows):
        bucket = owner_buckets.get(owner_idx, [])
        if not bucket:
            continue
        tag = owner_tag_for_handover(owner)
        for row in bucket:
            company = (row.get("company") or "-").strip() or "-"
            role = (row.get("role_category") or row.get("matched_role") or "-").strip() or "-"
            job_url = (row.get("job_url") or "-").strip() or "-"
            if case == HandoverSlackCase.RECRUITER_DETAIL:
                profile_url = (row.get("recruiter_profile_url") or "-").strip() or "-"
                msg = format_recruiter_detail_lead(tag, company, role, job_url, profile_url)
            else:
                poc_email = (row.get("recruiter_email") or "-").strip() or "-"
                msg = format_internal_poc_lead(tag, company, role, job_url, poc_email)
            if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                sent += 1
    return sent


def send_linkedin_post_handover_messages(
    relevant_rows: list[dict[str, Any]],
    *,
    defaults: SlackNotifyDefaults | None = None,
) -> int:
    """Heading + per-post messages (owners round-robin or Unassigned). Returns POST count."""
    d = defaults or slack_notify_defaults_from_env()
    if not d.webhook_url:
        logger.info("linkedin-posts slack skipped: SLACK_WEBHOOK_URL not configured")
        return 0
    if not relevant_rows:
        logger.info("linkedin-posts slack: no relevant posts to send")
        return 0

    owner_rows_opt = load_owner_rows_for_handover()
    if not owner_rows_opt:
        logger.warning(
            "linkedin-posts handover: owner sheet unavailable; posting without owner assignment "
            "(set GOOGLE_SPREADSHEET_ID and owner_slack_ID tab)"
        )

    sent = 0
    if not send_slack_text(heading_for_case(HandoverSlackCase.LINKEDIN_POST), defaults=d, sleep_after=1.0):
        return 0
    sent += 1

    if owner_rows_opt:
        owner_buckets: dict[int, list[dict[str, Any]]] = {i: [] for i in range(len(owner_rows_opt))}
        for idx, row in enumerate(relevant_rows):
            owner_buckets[idx % len(owner_rows_opt)].append(row)
        for owner_idx, owner in enumerate(owner_rows_opt):
            bucket = owner_buckets.get(owner_idx, [])
            if not bucket:
                continue
            owner_tag = owner_tag_for_handover(owner)
            for row in bucket:
                author = slack_author_from_row(row)
                url = slack_post_url_from_row(row)
                msg = format_linkedin_post_lead(owner_tag, url, author)
                if send_slack_text(msg, defaults=d, sleep_after=1.0):
                    sent += 1
    else:
        for row in relevant_rows:
            author = slack_author_from_row(row)
            url = slack_post_url_from_row(row)
            msg = format_linkedin_post_lead("*Unassigned*", url, author)
            if send_slack_text(msg, defaults=d, sleep_after=1.0):
                sent += 1

    logger.info("linkedin-posts handover sent %s slack messages", sent)
    return sent


def send_handover_notifications(
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
    """
    Read recruiter + LinkedIn relevant sheets for ``run_date`` (default: today) and post to Slack.

    Flags select which handover types to send. Uses ``SLACK_*`` env vars unless overridden.
    """
    rd = run_date or date.today().isoformat()
    defaults = merge_slack_defaults(
        webhook_url=webhook_url,
        channel=channel,
        username=username,
        icon_emoji=icon_emoji,
    )

    result: dict[str, Any] = {
        "run_date": rd,
        "skipped_reason": None,
        "recruiter_messages_sent": 0,
        "linkedin_messages_sent": 0,
        "recruiter_detail_leads": 0,
        "internal_poc_leads": 0,
        "linkedin_post_leads": 0,
    }

    if not defaults.webhook_url:
        result["skipped_reason"] = "SLACK_WEBHOOK_URL not configured"
        logger.info("handover slack skipped: SLACK_WEBHOOK_URL not configured")
        return result

    _, case3, case2 = load_recruiter_rows_split_for_handover(rd)
    result["recruiter_detail_leads"] = len(case3)
    result["internal_poc_leads"] = len(case2)

    owner_rows_opt = load_owner_rows_for_handover()
    owner_rows = owner_rows_opt if owner_rows_opt else []

    need_recruiter_owners = (send_recruiter_info and case3) or (send_internal_poc and case2)
    if need_recruiter_owners and not owner_rows:
        logger.warning("handover slack skipped: owner sheet has no rows (required for recruiter handovers)")

    if owner_rows:
        if send_recruiter_info and case3:
            result["recruiter_messages_sent"] += send_recruiter_handover_case(
                case3, owner_rows, HandoverSlackCase.RECRUITER_DETAIL, defaults=defaults
            )
        if send_internal_poc and case2:
            result["recruiter_messages_sent"] += send_recruiter_handover_case(
                case2, owner_rows, HandoverSlackCase.INTERNAL_POC, defaults=defaults
            )

    if send_linkedin_post:
        linkedin_rows = load_linkedin_relevant_posts_from_sheet(rd)
        result["linkedin_post_leads"] = len(linkedin_rows)
        result["linkedin_messages_sent"] = send_linkedin_post_handover_messages(
            linkedin_rows, defaults=defaults
        )

    logger.info(
        "send_handover_notifications run_date=%s recruiter_msgs=%s linkedin_msgs=%s",
        rd,
        result["recruiter_messages_sent"],
        result["linkedin_messages_sent"],
    )
    return result


def post_slack_payload(
    webhook_url: str,
    text: str,
    *,
    channel: str,
    username: str,
    icon_emoji: str,
) -> requests.Response:
    payload: dict[str, Any] = {
        "text": text,
        "channel": channel,
        "username": username,
        "icon_emoji": icon_emoji,
    }
    return requests.post(
        webhook_url,
        data={"payload": json.dumps(payload, ensure_ascii=True)},
        timeout=20,
    )


def retry_slack_action(
    action: Callable[[], T],
    *,
    retries: int = 3,
    initial_delay_seconds: float = 1.0,
) -> T:
    delay = initial_delay_seconds
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            return action()
        except Exception as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            sleep(delay)
            delay *= 2
    raise RuntimeError(f"Slack post failed after {retries} attempts: {last_error}") from last_error


def send_slack_text(
    text: str,
    *,
    defaults: SlackNotifyDefaults | None = None,
    webhook_url: str | None = None,
    channel: str | None = None,
    username: str | None = None,
    icon_emoji: str | None = None,
    sleep_after: float = 1.0,
    retries: int = 3,
    initial_delay_seconds: float = 1.0,
    log_skip_message: str | None = "slack handover skipped: SLACK_WEBHOOK_URL not configured",
) -> bool:
    """POST one message using env defaults unless overridden. Returns False if webhook missing."""
    d = defaults or slack_notify_defaults_from_env()
    url = webhook_url if webhook_url is not None else d.webhook_url
    if not url:
        if log_skip_message:
            logger.info(log_skip_message)
        return False
    ch = channel if channel is not None else d.channel
    un = username if username is not None else d.username
    em = icon_emoji if icon_emoji is not None else d.icon_emoji
    retry_slack_action(
        lambda: post_slack_payload(
            url,
            text,
            channel=ch,
            username=un,
            icon_emoji=em,
        ).raise_for_status(),
        retries=retries,
        initial_delay_seconds=initial_delay_seconds,
    )
    if sleep_after > 0:
        sleep(sleep_after)
    return True


def format_linkedin_post_lead(owner_tag: str, post_url: str, author: str) -> str:
    return (
        f"{owner_tag}\n"
        f"{post_url}\n"
        f'This is lead posted by author "{author}"\n'
        "Note: Please consume the lead in next 2 hours and update"
    )


def format_recruiter_detail_lead(
    owner_tag: str,
    company: str,
    role: str,
    job_url: str,
    recruiter_profile_url: str,
) -> str:
    return (
        f"{owner_tag}\n"
        f"Company: {company}\n"
        f"Role: {role}\n"
        f"Job URL: {job_url}\n"
        f"Recruiter Profile: {recruiter_profile_url}"
    )


def format_internal_poc_lead(
    owner_tag: str,
    company: str,
    role: str,
    job_url: str,
    poc_email: str,
) -> str:
    return (
        f"{owner_tag}\n"
        f"Company: {company}\n"
        f"Role: {role}\n"
        f"Job URL: {job_url}\n"
        f"Internal POC Email: {poc_email}"
    )


def send_handover_case_batch(
    case: HandoverSlackCase | str,
    lead_bodies: list[str],
    *,
    include_heading: bool = True,
    defaults: SlackNotifyDefaults | None = None,
    sleep_between: float = 1.0,
) -> int:
    """Send the standard case heading (optional) then each lead body. Returns POST count."""
    sent = 0
    d = defaults
    if include_heading:
        if not send_slack_text(
            heading_for_case(case),
            defaults=d,
            sleep_after=sleep_between,
        ):
            return 0
        sent += 1
    for body in lead_bodies:
        if send_slack_text(body, defaults=d, sleep_after=sleep_between):
            sent += 1
    return sent

