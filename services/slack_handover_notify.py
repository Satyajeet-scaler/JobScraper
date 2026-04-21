"""Slack handover notifications: LinkedIn post and recruiter LinkedIn profile."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from enum import Enum
from time import sleep
from typing import Any, Callable, TypeVar
from urllib.parse import urlparse

import requests

from services.google_sheets import GoogleSheetsWriter
from services.handover_owners import (
    load_owner_rows_for_handover,
    worksheet_row_dicts,
)
from services.linkedin_posts_slack_row import slack_author_from_row, slack_post_url_from_row

logger = logging.getLogger(__name__)

T = TypeVar("T")


class HandoverSlackCase(str, Enum):
    LINKEDIN_POST = "linkedin_post"
    RECRUITER_DETAIL = "recruiter_detail"


HEADING_LINKEDIN_POST = ":rotating_light: *INCOMING LINKEDIN JOB POST VIA VALIDATED AUTHOR*"
HEADING_RECRUITER_DETAIL = (
    ":rotating_light: *INCOMING LEAD WITH RECRUITER DETAILS AVAILABLE ON LINKEDIN*"
)


def heading_for_case(case: HandoverSlackCase | str) -> str:
    c = HandoverSlackCase(case) if isinstance(case, str) else case
    return {
        HandoverSlackCase.LINKEDIN_POST: HEADING_LINKEDIN_POST,
        HandoverSlackCase.RECRUITER_DETAIL: HEADING_RECRUITER_DETAIL,
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


def recruiter_row_role_label_for_slack(row: dict[str, str]) -> str:
    """Best label for the ``Role:`` line in recruiter handover messages.

    Reads ``role_category`` / ``matched_role`` (and space-separated header variants
    from human-edited sheets). If those are empty, uses the job listing ``title``.

    The JSON/API ``role`` (e.g. Software Developer) only selects *which tab* to read;
    it is not copied onto each row — per-row text comes from the recruiter sheet.
    """
    for key in (
        "role_category",
        "role category",
        "matched_role",
        "matched role",
    ):
        v = (row.get(key) or "").strip()
        if v:
            return v
    title = (row.get("title") or "").strip()
    if title:
        return title
    return "-"


def load_recruiter_rows_split_for_handover(
    run_date: str,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Load recruiters sheet; return (all_filtered_rows, case3_profile_rows)."""
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.info("handover sheets skipped: GOOGLE_SPREADSHEET_ID not configured")
        return [], []

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
        return [], []

    recruiter_rows = worksheet_row_dicts(raw)
    filtered: list[dict[str, str]] = []
    for row in recruiter_rows:
        row_run_date = (row.get("run_date") or "").strip()
        if row_run_date and row_run_date != run_date:
            continue
        filtered.append(row)

    case3: list[dict[str, str]] = []
    for row in filtered:
        if (row.get("recruiter_profile_url") or "").strip():
            case3.append(row)
    return filtered, case3


def linkedin_posts_relevant_tab_name(run_date: str) -> str:
    """Tab name for relevant LinkedIn posts (matches ``LINKEDIN_POSTS_RELEVANT_TAB_TEMPLATE``)."""
    return os.getenv("LINKEDIN_POSTS_RELEVANT_TAB_TEMPLATE", "linkedin_posts_relevant_{date}").format(
        date=run_date
    )


def candidate_match_tab_name(run_date: str) -> str:
    """Tab name for candidate match summary rows."""
    template = (os.getenv("CANDIDATE_MATCH_WORKSHEET_TEMPLATE") or "candidate_match_{date}").strip()
    return template.replace("{date}", run_date)


def _normalize_job_url_for_match(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    netloc = parsed.netloc.lower().strip()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = (parsed.path or "").rstrip("/")
    return f"{netloc}{path}".strip()


def _parse_candidate_match_count(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def load_candidate_match_count_map(run_date: str) -> dict[str, int]:
    """
    Read candidate match tab and return normalized job_url -> ai_score_gt_70_count.
    Missing/invalid counts are treated as zero.
    """
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id:
        return {}
    tab = candidate_match_tab_name(run_date)
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(tab)
        raw = writer.worksheet_get_all_values(ws, f"slack_handover_candidate_match:{tab}:get_all_values")
        rows = worksheet_row_dicts(raw)
    except Exception as exc:
        logger.warning("failed to load candidate match sheet=%s err=%s", tab, exc)
        return {}

    out: dict[str, int] = {}
    for row in rows:
        job_url = (row.get("job_url") or row.get("url") or row.get("link") or "").strip()
        key = _normalize_job_url_for_match(job_url)
        if not key:
            continue
        count = _parse_candidate_match_count(row.get("ai_score_gt_70_count"))
        out[key] = count
    return out


def load_candidate_match_count_map_for_role(*, role: str, run_date: str) -> dict[str, int]:
    """Like ``load_candidate_match_count_map`` but prefers ``candidate_match_{role_slug}_{date}``.

    Falls back to the generic ``candidate_match_{date}`` tab when the role tab
    is missing or empty (same behavior as role-pipeline relevant-jobs tooling).
    """
    from services.role_pipeline import _role_slug

    spreadsheet_id = (os.getenv("GOOGLE_SPREADSHEET_ID") or "").strip()
    if not spreadsheet_id:
        return {}
    role_slug = _role_slug(role)
    role_template = (
        os.getenv("ROLE_PIPELINE_CANDIDATE_MATCH_TAB_TEMPLATE")
        or "candidate_match_{role_slug}_{date}"
    ).strip()
    role_tab = role_template.format(role_slug=role_slug, date=run_date)
    gen_template = (os.getenv("CANDIDATE_MATCH_WORKSHEET_TEMPLATE") or "candidate_match_{date}").strip()
    fallback_tab = gen_template.replace("{date}", run_date)

    rows: list[dict[str, str]] = []
    try:
        writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
        ws = writer.open_worksheet(role_tab)
        raw = writer.worksheet_get_all_values(
            ws,
            f"slack_handover_candidate_match_role:{role_tab}:get_all_values",
        )
        rows = worksheet_row_dicts(raw)
    except Exception as exc:
        logger.warning("failed to load role candidate match sheet=%s err=%s", role_tab, exc)
        rows = []

    if not rows and fallback_tab != role_tab:
        try:
            writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
            ws = writer.open_worksheet(fallback_tab)
            raw = writer.worksheet_get_all_values(
                ws,
                f"slack_handover_candidate_match_fallback:{fallback_tab}:get_all_values",
            )
            rows = worksheet_row_dicts(raw)
        except Exception as exc:
            logger.warning("failed to load fallback candidate match sheet=%s err=%s", fallback_tab, exc)
            return {}

    out: dict[str, int] = {}
    for row in rows:
        job_url = (row.get("job_url") or row.get("url") or row.get("link") or "").strip()
        key = _normalize_job_url_for_match(job_url)
        if not key:
            continue
        count = _parse_candidate_match_count(row.get("ai_score_gt_70_count"))
        out[key] = count
    return out


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
    candidate_match_count_map: dict[str, int],
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
            role = recruiter_row_role_label_for_slack(row)
            job_url = (row.get("job_url") or "-").strip() or "-"
            profile_url = (row.get("recruiter_profile_url") or "-").strip() or "-"
            matched_count = candidate_match_count_map.get(_normalize_job_url_for_match(job_url), 0)
            msg = format_recruiter_detail_lead(tag, company, role, job_url, profile_url, matched_count)
            if send_slack_text(msg, defaults=defaults, sleep_after=1.0):
                sent += 1
    _persist_recruiter_detail_assigned_owner(run_date=run_date, owner_rows=owner_rows)
    return sent


def send_linkedin_post_handover_messages(
    relevant_rows: list[dict[str, Any]],
    *,
    run_date: str | None = None,
    defaults: SlackNotifyDefaults | None = None,
    persist_assigned_owner_tab: str | None = None,
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
            worksheet_title=persist_assigned_owner_tab,
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
        "linkedin_post_leads": 0,
    }

    if not defaults.webhook_url:
        result["skipped_reason"] = "SLACK_WEBHOOK_URL not configured"
        logger.info("handover slack skipped: SLACK_WEBHOOK_URL not configured")
        return result

    _, case3 = load_recruiter_rows_split_for_handover(rd)
    result["recruiter_detail_leads"] = len(case3)
    candidate_match_count_map = load_candidate_match_count_map(rd)

    owner_rows_opt = load_owner_rows_for_handover()
    owner_rows = owner_rows_opt if owner_rows_opt else []

    if send_recruiter_info and case3 and not owner_rows:
        logger.warning(
            "handover slack: owner sheet has no rows; Case 3 (recruiter detail) handover skipped"
        )

    if owner_rows and send_recruiter_info and case3:
        result["recruiter_messages_sent"] += send_recruiter_handover_case(
            case3,
            owner_rows,
            run_date=rd,
            defaults=defaults,
            candidate_match_count_map=candidate_match_count_map,
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
    candidate_match_count: int,
    *,
    include_candidate_match: bool = True,
) -> str:
    body = (
        f"{owner_tag}\n"
        f"Company: {company}\n"
        f"Role: {role}\n"
        f"Job URL: {job_url}\n"
        f"Recruiter Profile: {recruiter_profile_url}\n"
    )
    if include_candidate_match:
        body += f"Candidate match — {candidate_match_count} candidate(s) with AI score > 70"
    return body.rstrip("\n")


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


def _persist_linkedin_posts_assigned_owner(
    *,
    run_date: str | None,
    owner_rows: list[dict[str, str]],
    worksheet_title: str | None = None,
) -> None:
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    if not spreadsheet_id or not owner_rows:
        return
    rd = (run_date or date.today().isoformat()).strip()
    tab = worksheet_title or linkedin_posts_relevant_tab_name(rd)

    def selector(row: dict[str, str]) -> bool:
        row_run_date = (row.get("run_date") or "").strip()
        return not row_run_date or row_run_date == rd

    _persist_assigned_owner_column(
        spreadsheet_id=spreadsheet_id,
        worksheet_title=tab,
        owner_rows=owner_rows,
        selector=selector,
    )


def persist_assigned_owner_round_robin(
    *,
    spreadsheet_id: str,
    worksheet_title: str,
    owner_rows: list[dict[str, str]],
    selector: Callable[[dict[str, str]], bool],
) -> None:
    """Public helper to persist round-robin assigned owner for selected rows."""
    if not spreadsheet_id or not owner_rows:
        return
    _persist_assigned_owner_column(
        spreadsheet_id=spreadsheet_id,
        worksheet_title=worksheet_title,
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

