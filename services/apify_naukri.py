import os
from typing import Any, Optional

from apify_client import ApifyClient


def scrape_naukri_jobs(
    keyword: str,
    max_jobs: int,
    freshness: str = "all",
    fetch_details: bool = False,
) -> list[dict[str, Any]]:
    """
    Scrape Naukri jobs using an Apify Actor.

    Required env:
    - APIFY_TOKEN
    Optional env:
    - APIFY_ACTOR_ID (default: alpcnRV9YI9lYVPWk)
    """
    token = os.getenv("APIFY_TOKEN")
    if not token:
        raise RuntimeError("APIFY_TOKEN is required to scrape Naukri via Apify.")

    actor_id = os.getenv("APIFY_ACTOR_ID", "alpcnRV9YI9lYVPWk")
    client = ApifyClient(token)

    # Actor schema requires searchUrl to be a string (not null).
    # Also avoid passing nulls for optional fields because many actors validate types strictly.
    run_input = _drop_nones(
        {
        "keyword": keyword,
        "searchUrl": "",
        "fetchDetails": fetch_details,
        "maxJobs": max_jobs,
        "freshness": freshness,
        "sortBy": "relevance",
        "experience": "all",
        "workMode": None,
        "cities": None,
        "department": None,
        "salaryRange": None,
        "companyType": None,
        "roleCategory": None,
        "stipend": None,
        "duration": None,
        "ugCourse": None,
        "pgCourse": None,
        "postedBy": None,
        "industry": None,
        "topCompanies": None,
        "jobIds": None,
        }
    )

    run = client.actor(actor_id).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        return []

    items: list[dict[str, Any]] = []
    for item in client.dataset(dataset_id).iterate_items():
        if isinstance(item, dict):
            items.append(item)
    return items


def normalize_naukri_item(item: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize an Apify Naukri item into our internal schema fields.

    Note: Actor output keys can vary. We use a best-effort mapping and keep raw_payload.
    """
    # Map actor-native keys to the same core shape we use for JobSpy rows.
    title = _first_non_empty(item, ["title", "jobTitle", "position", "name"])
    company = _first_non_empty(item, ["companyName", "company", "employerName"])
    location = _first_non_empty(item, ["location", "jobLocation", "locations"])
    job_url = _first_non_empty(item, ["jdURL", "url", "jobUrl", "jobURL", "link"])
    description = _first_non_empty(item, ["jobDescription", "description", "jd", "details"])
    date_posted = _first_non_empty(item, ["createdDate", "datePosted", "postedAt", "postedOn", "date_posted"])
    experience = _first_non_empty(item, ["experienceText", "experience"])
    salary = _first_non_empty(item, ["salary", "salaryDetail"])
    currency = _first_non_empty(item, ["currency"])
    job_id = _first_non_empty(item, ["jobId", "id"])

    return {
        "site": "naukri",
        "title": title,
        "company": company,
        "location": location,
        "job_url": job_url,
        "description": description,
        "date_posted": date_posted,
        "experience": experience,
        "salary": salary,
        "currency": currency,
        "job_id": job_id,
        "raw_payload": item,
    }


def _first_non_empty(payload: dict[str, Any], keys: list[str]) -> Optional[Any]:
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


def _drop_nones(payload: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in payload.items() if v is not None}

