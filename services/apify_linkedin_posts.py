import json
import logging
import os
import re
import time
from typing import Any

from apify_client import ApifyClient

logger = logging.getLogger(__name__)


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

    fetch_timeout_s = max(30, int(float(os.getenv("APIFY_LINKEDIN_POSTS_DATASET_FETCH_TIMEOUT_S", "180"))))
    page_size = max(1, int(float(os.getenv("APIFY_LINKEDIN_POSTS_DATASET_PAGE_SIZE", "200"))))
    max_items = max(1, int(float(os.getenv("APIFY_LINKEDIN_POSTS_DATASET_MAX_ITEMS", "10000"))))

    dataset_client = client.dataset(dataset_id)
    started_at = time.monotonic()
    dataset_info = dataset_client.get() or {}
    expected_item_count = int(dataset_info.get("itemCount") or 0)
    target_count = min(expected_item_count, max_items) if expected_item_count > 0 else max_items

    logger.info(
        "apify linkedin-posts dataset fetch start dataset_id=%s expected_item_count=%s page_size=%d timeout_s=%d max_items=%d",
        dataset_id,
        expected_item_count,
        page_size,
        fetch_timeout_s,
        max_items,
    )

    items: list[dict[str, Any]] = []
    offset = 0
    while offset < target_count:
        elapsed = time.monotonic() - started_at
        if elapsed > fetch_timeout_s:
            logger.warning(
                "apify linkedin-posts dataset fetch timeout dataset_id=%s fetched=%d expected_item_count=%s elapsed_s=%.1f",
                dataset_id,
                len(items),
                expected_item_count,
                elapsed,
            )
            break

        page = dataset_client.list_items(limit=min(page_size, target_count - offset), offset=offset)
        page_items = page.get("items") if isinstance(page, dict) else getattr(page, "items", None)
        if not page_items:
            break

        dict_batch = [item for item in page_items if isinstance(item, dict)]
        items.extend(dict_batch)
        offset += len(page_items)

        logger.info(
            "apify linkedin-posts dataset fetch progress dataset_id=%s fetched=%d expected_item_count=%s",
            dataset_id,
            len(items),
            expected_item_count,
        )

        if len(page_items) < min(page_size, target_count - (offset - len(page_items))):
            break

    logger.info(
        "apify linkedin-posts dataset fetch done dataset_id=%s fetched=%d expected_item_count=%s",
        dataset_id,
        len(items),
        expected_item_count,
    )
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
        "author_info": _first_non_empty_deep(item, ["author.info"]),
        "author_type": _first_non_empty_deep(item, ["author.type"]) or _first_non_empty(item, ["authorType", "type"]),
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
