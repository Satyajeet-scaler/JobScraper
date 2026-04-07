"""Parse LinkedIn post sheet rows for Slack handover text (shared by pipelines and slack service)."""

from __future__ import annotations

import json
from typing import Any


def slack_display_field(value: Any, default: str = "-") -> str:
    """Slack text must be built from strings; Apify fields may be dict/list."""
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip() or default
    if isinstance(value, dict):
        for key in ("search", "query", "keyword", "text", "name", "title"):
            inner = value.get(key)
            if isinstance(inner, str) and inner.strip():
                return inner.strip()
        try:
            return json.dumps(value, ensure_ascii=True)[:500]
        except (TypeError, ValueError):
            return str(value)[:500]
    if isinstance(value, (list, tuple)):
        try:
            return json.dumps(value, ensure_ascii=True)[:500]
        except (TypeError, ValueError):
            return str(value)[:500]
    return str(value).strip() or default


def _deep_get(payload: Any, path: str) -> Any:
    cur = payload
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def _pick_first(payload: dict[str, Any], paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _deep_get(payload, path)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def slack_author_from_row(row: dict[str, Any]) -> str:
    direct = slack_display_field(row.get("author_name"), default="")
    if direct:
        return direct
    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        return "-"
    return slack_display_field(
        _pick_first(
            raw,
            (
                "author.name",
                "author.fullName",
                "authorName",
                "profileName",
                "name",
                "author.info",
            ),
        )
    )


def slack_company_from_row(row: dict[str, Any]) -> str:
    direct = slack_display_field(row.get("company"), default="")
    if direct:
        return direct
    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        return "-"
    return slack_display_field(
        _pick_first(
            raw,
            (
                "companyName",
                "company.name",
                "author.company",
                "author.info",
                "organizationName",
            ),
        )
    )


def slack_post_url_from_row(row: dict[str, Any]) -> str:
    direct = slack_display_field(row.get("post_url"), default="")
    if direct:
        return direct
    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        return "-"
    return slack_display_field(
        _pick_first(
            raw,
            (
                "linkedinUrl",
                "linkedinPostUrl",
                "postUrl",
                "url",
                "activityUrl",
                "author.linkedinUrl",
            ),
        )
    )
