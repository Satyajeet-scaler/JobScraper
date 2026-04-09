#!/usr/bin/env python3
import argparse
import json
import os
import time
from typing import Any

from apify_client import ApifyClient

from services.apify_wellfound import DEFAULT_WELLFOUND_ACTOR_ID


def _run_case(
    client: ApifyClient,
    actor_id: str,
    name: str,
    run_input: dict[str, Any],
    sample_limit: int,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    result: dict[str, Any] = {
        "case": name,
        "input": run_input,
        "accepted": False,
        "duration_seconds": 0.0,
    }

    try:
        started_run = client.actor(actor_id).start(run_input=run_input)
        run_id = started_run.get("id")
        run = client.run(run_id).wait_for_finish(wait_secs=1800)
        dataset_id = run.get("defaultDatasetId")
        items: list[dict[str, Any]] = []
        total_items: int | None = None

        if dataset_id:
            dataset_info = client.dataset(dataset_id).get() or {}
            item_count = dataset_info.get("itemCount")
            if isinstance(item_count, int):
                total_items = item_count
            page = client.dataset(dataset_id).list_items(limit=max(1, sample_limit))
            if total_items is None:
                total_items = page.total
            items = [it for it in (page.items or []) if isinstance(it, dict)]
            if items and (total_items is None or total_items <= 0):
                total_items = len(items)

        result.update(
            {
                "accepted": True,
                "run_status": run.get("status"),
                "run_id": run.get("id"),
                "dataset_id": dataset_id,
                "total_items": total_items,
                "first_item_keys": sorted(items[0].keys()) if items else [],
                "sample_item": _compact_sample_item(items[0]) if items else None,
            }
        )
    except Exception as exc:
        result.update(
            {
                "error_type": type(exc).__name__,
                "error": str(exc).strip().splitlines()[0],
            }
        )

    result["duration_seconds"] = round(time.perf_counter() - started_at, 2)
    return result


def _build_test_cases(location: str, results_wanted: int, max_pages: int) -> list[tuple[str, dict[str, Any]]]:
    return [
        (
            "baseline_with_proxy",
            {
                "location": location,
                "results_wanted": results_wanted,
                "max_pages": max_pages,
                "proxyConfiguration": {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"],
                },
            },
        ),
        (
            "no_proxy_configuration",
            {
                "location": location,
                "results_wanted": max(1, min(results_wanted, 5)),
                "max_pages": max_pages,
            },
        ),
        (
            "missing_location_uses_default",
            {
                "results_wanted": max(1, min(results_wanted, 5)),
                "max_pages": max_pages,
            },
        ),
        (
            "invalid_unknown_key",
            {
                "location": location,
                "results_wanted": max(1, min(results_wanted, 5)),
                "max_pages": max_pages,
                "unknownFlag": True,
            },
        ),
        (
            "invalid_results_type",
            {
                "location": location,
                "results_wanted": str(max(1, min(results_wanted, 5))),
                "max_pages": max_pages,
            },
        ),
    ]


def _compact_sample_item(item: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key in (
        "id",
        "title",
        "company",
        "location",
        "salary",
        "jobType",
        "postedDate",
        "applyUrl",
        "yearsExperienceMin",
        "yearsExperienceMax",
        "remote",
        "source",
    ):
        summary[key] = item.get(key)

    description = item.get("description_text")
    if isinstance(description, str):
        summary["description_preview"] = description[:220]
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe Wellfound Apify actor input validation and output schema."
    )
    parser.add_argument("--location", default="united-states", help="Wellfound location slug")
    parser.add_argument("--results-wanted", type=int, default=5, help="results_wanted value")
    parser.add_argument("--max-pages", type=int, default=1, help="max_pages value")
    parser.add_argument("--sample-limit", type=int, default=1, help="how many dataset rows to sample")
    args = parser.parse_args()

    token = os.getenv("APIFY_TOKEN")
    if not token:
        raise RuntimeError("APIFY_TOKEN env var is required.")

    actor_id = os.getenv("APIFY_WELLFOUND_ACTOR_ID", DEFAULT_WELLFOUND_ACTOR_ID)
    client = ApifyClient(token)

    cases = _build_test_cases(
        location=args.location,
        results_wanted=max(1, args.results_wanted),
        max_pages=max(1, args.max_pages),
    )

    case_results = [
        _run_case(client, actor_id, name, payload, sample_limit=max(1, args.sample_limit))
        for name, payload in cases
    ]

    accepted = [row for row in case_results if row.get("accepted")]
    rejected = [row for row in case_results if not row.get("accepted")]
    output_key_union = sorted(
        {key for row in accepted for key in (row.get("first_item_keys") or [])}
    )

    summary = {
        "actor_id": actor_id,
        "accepted_cases": len(accepted),
        "rejected_cases": len(rejected),
        "accepted_case_names": [row["case"] for row in accepted],
        "rejected_case_names": [row["case"] for row in rejected],
        "observed_output_keys": output_key_union,
        "cases": case_results,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=True, default=str))


if __name__ == "__main__":
    main()
