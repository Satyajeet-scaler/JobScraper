import json
import os
import re
from typing import Any

from apify_client import ApifyClient


DEFAULT_LINKEDIN_POSTS_ACTOR_ID = "buIWk2uOUzTmcLsuB"
_AUTHOR_INFO_COMPANY = re.compile(r"(?:@| at )\s*([A-Za-z0-9][A-Za-z0-9&.,()'’\\\\ -]{1,80})", re.I)


def _deep_get(payload: Any, path: str) -> Any:
    cur = payload
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def _first_non_empty_deep(payload: dict[str, Any], paths: list[str]) -> Any:
    for path in paths:
        value = _deep_get(payload, path)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


def _extract_company_from_author_info(info: Any) -> str | None:
    if not isinstance(info, str):
        return None
    m = _AUTHOR_INFO_COMPANY.search(info)
    if not m:
        return None
    company = (m.group(1) or "").strip(" .,-|")
    if not company:
        return None
    # avoid capturing "Hiring" etc
    lowered = company.lower()
    if lowered.startswith(("hiring", "recruiting", "looking", "open")):
        return None
    return company


def scrape_linkedin_posts(run_input: dict[str, Any]) -> list[dict[str, Any]]:
    """Run the LinkedIn-posts actor on Apify and return dataset items."""
    token = os.getenv("APIFY_TOKEN")
    if not token:
        raise RuntimeError("APIFY_TOKEN is required to scrape LinkedIn posts via Apify.")

    actor_id = os.getenv("APIFY_LINKEDIN_POSTS_ACTOR_ID", DEFAULT_LINKEDIN_POSTS_ACTOR_ID)
    client = ApifyClient(token)
    run = client.actor(actor_id).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        return []

    items: list[dict[str, Any]] = []
    for item in client.dataset(dataset_id).iterate_items():
        if isinstance(item, dict):
            items.append(item)
    return items


def normalize_linkedin_post_item(item: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize actor output into a stable shape while preserving original payload.
    Actor schemas evolve, so keep best-effort field mapping.
    """
    raw_search = _first_non_empty(item, ["searchQuery", "query", "keyword"])
    # Newer actor shape has nested author + linkedinUrl + content + query.
    author_name = _first_non_empty_deep(item, ["author.name"]) or _first_non_empty(
        item, ["authorName", "profileName", "name"]
    )
    author_profile_url = _first_non_empty_deep(item, ["author.linkedinUrl"]) or _first_non_empty(
        item, ["authorProfileUrl", "profileUrl", "authorUrl"]
    )
    company = _first_non_empty_deep(item, ["company.name"]) or _first_non_empty(
        item, ["companyName", "company", "organizationName"]
    )
    if not company:
        company = _extract_company_from_author_info(_first_non_empty_deep(item, ["author.info"]))
    return {
        "site": "linkedin_posts",
        "search_query": _coerce_search_query_string(raw_search),
        "content_type": _first_non_empty(item, ["contentType", "type"]),
        "post_url": _first_non_empty(item, ["linkedinUrl", "postUrl", "url", "postURL", "linkedinPostUrl", "activityUrl"]),
        "post_id": _first_non_empty(item, ["postId", "id", "urn", "entityId", "shareUrn"]),
        "post_text": _first_non_empty(item, ["content", "text", "postText", "description"]),
        "posted_at": _first_non_empty(item, ["postedAt", "createdAt", "timestamp", "date"]),
        "author_name": author_name,
        "author_profile_url": author_profile_url,
        "company": company,
        "job_title_hint": _first_non_empty(item, ["title", "jobTitle", "headline"]),
        "likes_count": _first_non_empty(item, ["likesCount", "numLikes", "reactionsCount"]),
        "comments_count": _first_non_empty(item, ["commentsCount", "numComments"]),
        "reposts_count": _first_non_empty(item, ["repostsCount", "sharesCount", "numShares"]),
        "raw_payload": item,
    }


def _coerce_search_query_string(value: Any) -> str | None:
    """Actor may return query as a string or nested dict (e.g. search + filters)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        for key in ("search", "query", "keyword", "text"):
            inner = value.get(key)
            if isinstance(inner, str) and inner.strip():
                return inner.strip()
        try:
            return json.dumps(value, ensure_ascii=True)
        except (TypeError, ValueError):
            return str(value)
    return str(value).strip() or None


def _first_non_empty(payload: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None
