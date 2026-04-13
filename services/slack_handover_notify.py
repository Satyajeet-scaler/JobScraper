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
from services.handover_owners import (
    load_internal_poc_tag_rows,
    load_owner_rows_for_handover,
    worksheet_row_dicts,
)
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


def normalize_email(value: str | None) -> str:
    """Strip and lowercase for matching ``recruiter_email`` to internal POC sheet rows."""
    if not value:
        return ""
    return value.strip().lower()


def _poc_tag_row_email(row: dict[str, str]) -> str:
    return (row.get("owner_email") or row.get("email") or "").strip()


def internal_poc_email_owner_map(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    """Map normalized email -> owner row; first row wins; log duplicates."""
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        key = normalize_email(_poc_tag_row_email(row))
        if not key:
            continue
        if key in out:
            logger.warning("internal POC tag sheet: duplicate email %s (extra row ignored)", key)
            continue
        out[key] = row
    return out


def split_recruiter_email_field(raw: str | None) -> list[str]:
    """
    ``recruiters_info.recruiter_email`` may list multiple addresses (comma or semicolon).
    Returns stripped tokens in order; drops empties.
    """
    if not raw:
        return []
    parts: list[str] = []
    for chunk in raw.replace(";", ",").split(","):
        t = chunk.strip()
        if t:
            parts.append(t)
    return parts


def match_internal_poc_owners_ordered(
    raw_recruiter_email: str,
    email_map: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    """
    Every distinct token that appears in ``email_map`` becomes one tagged owner, in field order.
    Duplicate tokens (same email twice) are only tagged once.
    """
    seen_matched: set[str] = set()
    out: list[dict[str, str]] = []
    for token in split_recruiter_email_field(raw_recruiter_email):
        key = normalize_email(token)
        if not key or key in seen_matched:
            continue
        if key in email_map:
            seen_matched.add(key)
            out.append(email_map[key])
    return out


def internal_poc_owner_tag_line(matched_owners: list[dict[str, str]]) -> str:
    """Slack tag line: one owner or several joined with `` · ``."""
    if not matched_owners:
        return "*Unassigned*"
    if len(matched_owners) == 1:
        return owner_tag_for_handover(matched_owners[0])
    return " · ".join(owner_tag_for_handover(o) for o in matched_owners)


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
        recruiters_ws = writer.open_worksheet(recruiters_tab)
        raw = writer.worksheet_get_all_values(
            recruiters_ws,
            f"slack_handover_recruiters:{recruiters_tab}:get_all_values",
        )
    except Exception as exc:
        logger.warning("recruiter sheet unavailable tab=%s err=%s", recruiters_tab, exc)
        return [], [], []

    recruiter_rows = worksheet_row_dicts(raw)
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


def linkedin_posts_relevant_tab_name(run_date: str) -> str:
    """Tab name for relevant LinkedIn posts (matches ``LINKEDIN_POSTS_RELEVANT_TAB_TEMPLATE``)."""
    return os.getenv("LINKEDIN_POSTS_RELEVANT_TAB_TEMPLATE", "linkedin_posts_relevant_{date}").format(
        date=run_date
    )


def load_linkedin_relevant_posts_from_sheet(run_date: str) -> list[dict[str, Any]]:
    """Read ``linkedin_posts_relevant_{run_date}`` tab."""
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        return []
    tab = linkedin_posts_relevant_tab_name(run_date)
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(tab)
        raw = writer.worksheet_get_all_values(ws, f"slack_handover_linkedin_relevant:{tab}:get_all_values")
        rows = worksheet_row_dicts(raw)
        logger.info("loaded %s relevant linkedin posts from sheet %s", len(rows), tab)
        return list(rows)
    except Exception as exc:
        logger.warning("failed to load relevant linkedin posts sheet=%s err=%s", tab, exc)
        return []


def send_recruiter_handover_case(
    rows: list[dict[str, str]],
    owner_rows: list[dict[str, str]],
    *,
    run_date: str,
    defaults: SlackNotifyDefaults,
) -> int:
    """Case 3: heading + round-robin owners from ``owner_slack_ID``. Returns Slack POST count."""
    if not rows or not owner_rows:
        return 0
    sent = 0
    if not send_slack_text(
        heading_for_case(HandoverSlackCase.RECRUITER_DETAIL), defaults=defaults, sleep_after=1.0
    ):
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
            profile_url = (row.get("recruiter_profile_url") or "-").strip() or "-"
            msg = format_recruiter_detail_lead(tag, company, role, job_url, profile_url)
            if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                sent += 1
    _persist_recruiter_detail_assigned_owner(run_date=run_date, owner_rows=owner_rows)
    return sent


def send_internal_poc_handover_case(
    rows: list[dict[str, str]],
    *,
    run_date: str,
    defaults: SlackNotifyDefaults,
) -> int:
    """
    Case 2: heading + per-lead tag(s) from ``INTERNAL_POC_TAG_SHEET_NAME`` (all matching emails), else *Unassigned*.
    Does not use the main owner round-robin sheet.
    """
    if not rows:
        return 0
    poc_sheet_rows = load_internal_poc_tag_rows()
    email_map = internal_poc_email_owner_map(poc_sheet_rows)
    sent = 0
    if not send_slack_text(
        heading_for_case(HandoverSlackCase.INTERNAL_POC), defaults=defaults, sleep_after=1.0
    ):
        return 0
    sent += 1

    for row in rows:
        poc_email = (row.get("recruiter_email") or "-").strip() or "-"
        raw_for_match = "" if poc_email == "-" else poc_email
        tokens = split_recruiter_email_field(raw_for_match)
        matched_owners = match_internal_poc_owners_ordered(raw_for_match, email_map)
        unmatched_nk: set[str] = set()
        unmatched_tokens: list[str] = []
        for t in tokens:
            nk = normalize_email(t)
            if not nk:
                continue
            if nk not in email_map and nk not in unmatched_nk:
                unmatched_nk.add(nk)
                unmatched_tokens.append(t)
        if unmatched_tokens:
            logger.warning(
                "internal POC handover: no Slack tag mapping for email(s) %s (recruiter_email=%s)",
                ", ".join(unmatched_tokens),
                poc_email,
            )
        elif tokens and not matched_owners:
            logger.warning(
                "internal POC handover: no Slack tag mapping for recruiter_email=%s",
                poc_email,
            )
        tag = internal_poc_owner_tag_line(matched_owners)
        company = (row.get("company") or "-").strip() or "-"
        role = (row.get("role_category") or row.get("matched_role") or "-").strip() or "-"
        job_url = (row.get("job_url") or "-").strip() or "-"
        msg = format_internal_poc_lead(tag, company, role, job_url, poc_email)
        if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
            sent += 1

    _persist_internal_poc_assigned_owner(run_date=run_date, email_map=email_map)
    return sent


def send_linkedin_post_handover_messages(
    relevant_rows: list[dict[str, Any]],
    *,
    run_date: str | None = None,
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
        _persist_linkedin_posts_assigned_owner(
            run_date=run_date,
            owner_rows=owner_rows_opt,
        )
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

    if send_recruiter_info and case3 and not owner_rows:
        logger.warning(
            "handover slack: owner sheet has no rows; Case 3 (recruiter detail) handover skipped"
        )

    if owner_rows and send_recruiter_info and case3:
        result["recruiter_messages_sent"] += send_recruiter_handover_case(
            case3, owner_rows, run_date=rd, defaults=defaults
        )

    if send_internal_poc and case2:
        result["recruiter_messages_sent"] += send_internal_poc_handover_case(
            case2, run_date=rd, defaults=defaults
        )

    if send_linkedin_post:
        linkedin_rows = load_linkedin_relevant_posts_from_sheet(rd)
        result["linkedin_post_leads"] = len(linkedin_rows)
        result["linkedin_messages_sent"] = send_linkedin_post_handover_messages(
            linkedin_rows, run_date=rd, defaults=defaults
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


def _owner_display_name(owner: dict[str, str]) -> str:
    name = (owner.get("owner_name") or "").strip()
    if name:
        return name
    owner_email = (owner.get("owner_email") or "").strip()
    if owner_email:
        return owner_email
    sid = (owner.get("owner_slack_id") or "").strip()
    return sid or "Owner"


def _column_letter(index: int) -> str:
    letters = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _persist_recruiter_detail_assigned_owner(
    *,
    run_date: str,
    owner_rows: list[dict[str, str]],
) -> None:
    """Round-robin ``assigned owner`` for Case 3 rows (recruiter profile URL present)."""
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id or not owner_rows:
        return
    tab = os.getenv("RECRUITERS_INFO_WORKSHEET") or f"recruiters_info_{run_date}"

    def selector(row: dict[str, str]) -> bool:
        row_run_date = (row.get("run_date") or "").strip()
        if row_run_date and row_run_date != run_date:
            return False
        return bool((row.get("recruiter_profile_url") or "").strip())

    _persist_assigned_owner_column(
        spreadsheet_id=spreadsheet_id,
        worksheet_title=tab,
        owner_rows=owner_rows,
        selector=selector,
    )


def _persist_internal_poc_assigned_owner(
    *,
    run_date: str,
    email_map: dict[str, dict[str, str]],
) -> None:
    """Set ``assigned owner`` per Case 2 row from ``INTERNAL_POC_TAG_SHEET_NAME`` email match."""
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        return
    tab = os.getenv("RECRUITERS_INFO_WORKSHEET") or f"recruiters_info_{run_date}"

    def selector_case2(row: dict[str, str]) -> bool:
        row_run_date = (row.get("run_date") or "").strip()
        if row_run_date and row_run_date != run_date:
            return False
        has_profile = bool((row.get("recruiter_profile_url") or "").strip())
        has_email = bool((row.get("recruiter_email") or "").strip())
        return (not has_profile) and has_email

    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(tab)
        values = writer.worksheet_get_all_values(ws, f"persist_internal_poc:{tab}:get_all_values")
        if not values:
            return
        headers = [str(h or "").strip() for h in values[0]]
        data_rows = [list(r) for r in values[1:]]

        normalized_headers = [h.lower() for h in headers]
        assigned_header = "assigned owner"
        if assigned_header in normalized_headers:
            assigned_col_idx = normalized_headers.index(assigned_header)
        else:
            headers.append(assigned_header)
            assigned_col_idx = len(headers) - 1
            normalized_headers.append(assigned_header)

        for row in data_rows:
            while len(row) < len(headers):
                row.append("")

        updated = 0
        for row in data_rows:
            row_dict: dict[str, str] = {}
            for idx, header in enumerate(normalized_headers):
                row_dict[header] = row[idx].strip() if idx < len(row) else ""
            if not selector_case2(row_dict):
                continue
            raw_rec = (row_dict.get("recruiter_email") or "").strip()
            matched_owners = match_internal_poc_owners_ordered(raw_rec, email_map)
            if matched_owners:
                row[assigned_col_idx] = "; ".join(_owner_display_name(o) for o in matched_owners)
            else:
                row[assigned_col_idx] = "Unassigned"
            updated += 1

        if not updated:
            return

        writer.worksheet_update(ws, "A1", [headers], f"persist_internal_poc:{tab}:update_headers")
        col_letter = _column_letter(assigned_col_idx + 1)
        end_row = len(data_rows) + 1
        if end_row >= 2:
            col_values = [[row[assigned_col_idx]] for row in data_rows]
            rng = f"{col_letter}2:{col_letter}{end_row}"
            writer.worksheet_update(ws, rng, col_values, f"persist_internal_poc:{tab}:update_column")
        logger.info(
            "internal POC assigned owner persisted sheet=%s tab=%s updated_rows=%s",
            spreadsheet_id,
            tab,
            updated,
        )
    except Exception as exc:
        logger.warning(
            "failed to persist internal POC assigned owner sheet=%s tab=%s err=%s",
            spreadsheet_id,
            tab,
            exc,
        )


def _persist_linkedin_posts_assigned_owner(
    *,
    run_date: str | None,
    owner_rows: list[dict[str, str]],
) -> None:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id or not owner_rows:
        return
    rd = (run_date or date.today().isoformat()).strip()
    tab = linkedin_posts_relevant_tab_name(rd)

    def selector(row: dict[str, str]) -> bool:
        row_run_date = (row.get("run_date") or "").strip()
        return not row_run_date or row_run_date == rd

    _persist_assigned_owner_column(
        spreadsheet_id=spreadsheet_id,
        worksheet_title=tab,
        owner_rows=owner_rows,
        selector=selector,
    )


def _persist_assigned_owner_column(
    *,
    spreadsheet_id: str,
    worksheet_title: str,
    owner_rows: list[dict[str, str]],
    selector: Callable[[dict[str, str]], bool],
) -> None:
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(worksheet_title)
        values = writer.worksheet_get_all_values(
            ws,
            f"persist_assigned_owner:{worksheet_title}:get_all_values",
        )
        if not values:
            return
        headers = [str(h or "").strip() for h in values[0]]
        data_rows = [list(r) for r in values[1:]]

        normalized_headers = [h.lower() for h in headers]
        assigned_header = "assigned owner"
        if assigned_header in normalized_headers:
            assigned_col_idx = normalized_headers.index(assigned_header)
        else:
            headers.append(assigned_header)
            assigned_col_idx = len(headers) - 1
            normalized_headers.append(assigned_header)

        for row in data_rows:
            while len(row) < len(headers):
                row.append("")

        selected_row_positions: list[int] = []
        for pos, row in enumerate(data_rows):
            row_dict: dict[str, str] = {}
            for idx, header in enumerate(normalized_headers):
                row_dict[header] = row[idx].strip() if idx < len(row) else ""
            if selector(row_dict):
                selected_row_positions.append(pos)

        if not selected_row_positions:
            return

        owner_names = [_owner_display_name(owner) for owner in owner_rows]
        for idx, row_pos in enumerate(selected_row_positions):
            data_rows[row_pos][assigned_col_idx] = owner_names[idx % len(owner_names)]

        # Ensure header exists before writing column values.
        writer.worksheet_update(
            ws,
            "A1",
            [headers],
            f"persist_assigned_owner:{worksheet_title}:update_headers",
        )

        col_letter = _column_letter(assigned_col_idx + 1)
        end_row = len(data_rows) + 1
        if end_row >= 2:
            col_values = [[row[assigned_col_idx]] for row in data_rows]
            rng = f"{col_letter}2:{col_letter}{end_row}"
            writer.worksheet_update(
                ws,
                rng,
                col_values,
                f"persist_assigned_owner:{worksheet_title}:update_column",
            )
        logger.info(
            "assigned owner persisted sheet=%s tab=%s updated_rows=%s",
            spreadsheet_id,
            worksheet_title,
            len(selected_row_positions),
        )
    except Exception as exc:
        logger.warning(
            "failed to persist assigned owner sheet=%s tab=%s err=%s",
            spreadsheet_id,
            worksheet_title,
            exc,
        )

