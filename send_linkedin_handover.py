#!/usr/bin/env python3
"""
Send LinkedIn post handover messages to Slack.

Format:
1) One case heading message.
2) Per owner: one owner-tag message, then one message per post (Author / Company / Post Url).

Reads from the latest linkedin_posts_relevant_{date} Google Sheet tab.

Env required:
- SLACK_WEBHOOK_URL
- GOOGLE_SPREADSHEET_ID
- GOOGLE_SERVICE_ACCOUNT_JSON

Optional env:
- SLACK_CHANNEL (default: relevant-scraped-jobs)
- SLACK_USERNAME (default: Karan Bot)
- SLACK_ICON_EMOJI (default: :karandeep:)
- OWNER_SHEET_NAME (default: owner_slack_ID)

Usage:
  python send_linkedin_handover.py                   # uses today's date
  python send_linkedin_handover.py 2026-04-07        # uses specific date
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import date
from time import sleep
from typing import Any

import requests


def _post_slack(webhook_url: str, text: str, channel: str, username: str, icon_emoji: str) -> None:
    payload = {
        "text": text,
        "channel": channel,
        "username": username,
        "icon_emoji": icon_emoji,
    }
    resp = requests.post(
        webhook_url,
        data={"payload": json.dumps(payload, ensure_ascii=True)},
        timeout=20,
    )
    resp.raise_for_status()


def _d(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text if text else "-"


def _load_sheet_rows(spreadsheet_id: str, tab_name: str) -> list[dict[str, str]]:
    from services.google_sheets import GoogleSheetsWriter

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    ws = writer.sheet.worksheet(tab_name)
    records = ws.get_all_values()
    if len(records) <= 1:
        return []
    headers = [h.strip().lower() for h in records[0]]
    rows: list[dict[str, str]] = []
    for row in records[1:]:
        d = {}
        for i, h in enumerate(headers):
            d[h] = row[i] if i < len(row) else ""
        rows.append(d)
    return rows


def _load_owner_rows(spreadsheet_id: str, tab_name: str) -> list[dict[str, str]]:
    from services.google_sheets import GoogleSheetsWriter

    writer = GoogleSheetsWriter(spreadsheet_id=spreadsheet_id)
    ws = writer.sheet.worksheet(tab_name)
    records = ws.get_all_values()
    if len(records) <= 1:
        return []
    headers = [h.strip().lower() for h in records[0]]
    rows: list[dict[str, str]] = []
    for row in records[1:]:
        d = {}
        for i, h in enumerate(headers):
            d[h] = row[i] if i < len(row) else ""
        rows.append(d)
    return rows


def main() -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        raise RuntimeError("SLACK_WEBHOOK_URL is required.")
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required.")

    channel = os.getenv("SLACK_CHANNEL", "relevant-scraped-jobs")
    username = os.getenv("SLACK_USERNAME", "Karan Bot")
    icon_emoji = os.getenv("SLACK_ICON_EMOJI", ":karandeep:")
    owner_tab = os.getenv("OWNER_SHEET_NAME", "owner_slack_ID")

    run_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    relevant_tab = f"linkedin_posts_relevant_{run_date}"

    print(f"Loading relevant posts from {relevant_tab} ...")
    rows = _load_sheet_rows(spreadsheet_id, relevant_tab)
    if not rows:
        print("No relevant rows found.")
        return
    print(f"Found {len(rows)} relevant post(s).")

    print(f"Loading owners from {owner_tab} ...")
    owners = _load_owner_rows(spreadsheet_id, owner_tab)
    if not owners:
        print("No owner rows found; sending without owner assignment.")

    # Round-robin assign rows to owners
    if owners:
        owner_buckets: dict[int, list[dict[str, str]]] = defaultdict(list)
        for idx, row in enumerate(rows):
            owner_buckets[idx % len(owners)].append(row)
    else:
        owner_buckets = {0: rows}
        owners = [{"owner_name": "-", "owner_slack_id": "", "owner_email": "-"}]

    # 1) Send case heading
    heading = (
        ":rotating_light: *INCOMING LINKEDIN JOB POST VIA VALIDATED AUTHOR*\n"
        "Note : Please consume the lead in next 2 hours\n"
        "---"
    )
    _post_slack(webhook_url, heading, channel, username, icon_emoji)
    sleep(1)
    sent = 1

    # 2) Per owner: one owner message, then one message per post
    for owner_idx, owner in enumerate(owners):
        bucket = owner_buckets.get(owner_idx, [])
        if not bucket:
            continue
        owner_name = _d(owner.get("owner_name"))
        owner_slack_id = (owner.get("owner_slack_id") or "").strip()
        owner_email = _d(owner.get("owner_email"))
        owner_tag = f"<@{owner_slack_id}>" if owner_slack_id else owner_name

        owner_msg = f"*Owner : {owner_tag} ({owner_email})*"
        _post_slack(webhook_url, owner_msg, channel, username, icon_emoji)
        sleep(1)
        sent += 1

        for row in bucket:
            author = _d(row.get("author_name"))
            company = _d(row.get("company"))
            post_url = _d(row.get("post_url"))

            post_msg = (
                f"Author: {author}\n"
                f"Company: {company}\n"
                f"Post Url: {post_url}"
            )
            _post_slack(webhook_url, post_msg, channel, username, icon_emoji)
            sleep(1)
            sent += 1

    print(f"Sent {sent} Slack message(s) for {len(rows)} post(s) across {len([b for b in owner_buckets.values() if b])} owner(s).")


if __name__ == "__main__":
    main()
