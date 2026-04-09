import os
from datetime import datetime, timezone
from typing import Any

from apify_client import ApifyClient


DEFAULT_WELLFOUND_ACTOR_ID = "VR4qlxv7R9mnuyw8j"
DEFAULT_WELLFOUND_LOCATION = "united-states"
DEFAULT_WELLFOUND_PROXY_GROUPS = ["RESIDENTIAL"]


def scrape_wellfound_jobs(
    location: str = DEFAULT_WELLFOUND_LOCATION,
    results_wanted: int = 20,
    max_pages: int = 20,
    keyword: str | None = None,
    *,
    use_apify_proxy: bool = True,
    apify_proxy_groups: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Scrape Wellfound jobs using an Apify Actor.

    Required env:
    - APIFY_TOKEN
    Optional env:
    - APIFY_WELLFOUND_ACTOR_ID (default: VR4qlxv7R9mnuyw8j)
    """
    token = os.getenv("APIFY_TOKEN")
    if not token:
        raise RuntimeError("APIFY_TOKEN is required to scrape Wellfound via Apify.")

    actor_id = os.getenv("APIFY_WELLFOUND_ACTOR_ID", DEFAULT_WELLFOUND_ACTOR_ID)
    client = ApifyClient(token)

    run_input: dict[str, Any] = {
        "location": (location or DEFAULT_WELLFOUND_LOCATION).strip() or DEFAULT_WELLFOUND_LOCATION,
        "results_wanted": int(results_wanted),
        "max_pages": int(max_pages),
    }
    if keyword and keyword.strip():
        run_input["keyword"] = keyword.strip()
    if use_apify_proxy:
        run_input["proxyConfiguration"] = {
            "useApifyProxy": True,
            "apifyProxyGroups": apify_proxy_groups or DEFAULT_WELLFOUND_PROXY_GROUPS,
        }

    run = client.actor(actor_id).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        return []

    items: list[dict[str, Any]] = []
    for item in client.dataset(dataset_id).iterate_items():
        if isinstance(item, dict):
            items.append(item)
    return items


def normalize_wellfound_item(item: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a Wellfound actor item to our internal schema.
    """
    experience = _format_experience_range(
        _to_int(item.get("yearsExperienceMin")),
        _to_int(item.get("yearsExperienceMax")),
    )

    return {
        "site": "wellfound",
        "title": _first_non_empty(item, ["title", "primaryRoleTitle"]),
        "company": _first_non_empty(item, ["company"]),
        "location": _normalize_location(item.get("location")),
        "job_url": _first_non_empty(item, ["applyUrl", "companyUrl"]),
        "description": _first_non_empty(item, ["description_text", "description_html"]),
        "date_posted": _normalize_posted_date(item.get("postedDate")),
        "experience": experience,
        "salary": _stringify_salary(item.get("salary")),
        "job_type": _first_non_empty(item, ["jobType"]),
        "job_id": _first_non_empty(item, ["id"]),
        "raw_payload": item,
    }


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


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _format_experience_range(min_years: int | None, max_years: int | None) -> str | None:
    if min_years is None and max_years is None:
        return None
    if min_years is not None and max_years is not None:
        return f"{min_years}-{max_years} years"
    if min_years is not None:
        return f"{min_years}+ years"
    return f"Up to {max_years} years"


def _normalize_location(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if isinstance(value, list):
        parts = [str(part).strip() for part in value if str(part).strip()]
        return ", ".join(parts) if parts else None
    if isinstance(value, dict):
        for key in ("name", "city", "state", "country"):
            field = value.get(key)
            if isinstance(field, str) and field.strip():
                return field.strip()
        return None
    return str(value)


def _stringify_salary(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        currency = value.get("currency") or value.get("currencyCode")
        minimum = value.get("min") or value.get("minimum")
        maximum = value.get("max") or value.get("maximum")
        interval = value.get("interval") or value.get("period")

        values = []
        if minimum is not None:
            values.append(str(minimum))
        if maximum is not None:
            values.append(str(maximum))
        if values:
            amount = " - ".join(values)
            if currency:
                amount = f"{currency} {amount}"
            if interval:
                return f"{amount} / {interval}"
            return amount

    return str(value)


def _normalize_posted_date(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        if cleaned.isdigit():
            as_int = _to_int(cleaned)
            if as_int is not None:
                return _epoch_to_iso(as_int)
        return cleaned

    if isinstance(value, (int, float)):
        return _epoch_to_iso(int(value))

    return str(value)


def _epoch_to_iso(epoch: int) -> str | None:
    # Accept both milliseconds and seconds and normalize to UTC ISO.
    if epoch <= 0:
        return None
    if epoch > 10_000_000_000:
        epoch = int(epoch / 1000)
    try:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return None
