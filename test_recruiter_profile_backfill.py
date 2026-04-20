#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def _http_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    payload = None
    req_headers = dict(headers or {})
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        req_headers["content-type"] = "application/json"
    req = urllib.request.Request(url=url, method=method, headers=req_headers, data=payload)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
            if not raw.strip():
                return {}
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {method} {url}: {raw}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Request failed for {method} {url}: {exc}") from exc


def _build_query(args: argparse.Namespace) -> str:
    params: dict[str, str] = {}
    if args.run_date:
        params["run_date"] = args.run_date
    if args.role:
        params["role"] = args.role
    if args.relevant_tab:
        params["relevant_tab"] = args.relevant_tab
    if args.recruiters_tab:
        params["recruiters_tab"] = args.recruiters_tab
    encoded = urllib.parse.urlencode(params)
    return f"?{encoded}" if encoded else ""


def _print_summary(metrics: dict[str, Any]) -> None:
    keys = [
        "status",
        "run_date",
        "role",
        "relevant_tab",
        "recruiters_tab",
        "relevant_rows_scanned",
        "jobs_skipped_with_existing_profile_url",
        "candidate_jobs_for_backfill",
        "candidate_linkedin_jobs_for_backfill",
        "jobs_with_new_recruiter_profiles_found",
        "recruiter_rows_appended",
        "duration_seconds",
        "error",
    ]
    compact = {k: metrics.get(k) for k in keys if k in metrics}
    print(json.dumps(compact, indent=2, ensure_ascii=True, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trigger and poll recruiter profile backfill API run."
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("BACKFILL_TEST_BASE_URL", "http://127.0.0.1:8000"),
        help="API base URL (default: BACKFILL_TEST_BASE_URL or http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("INTERNAL_TRIGGER_TOKEN", ""),
        help="x-internal-token (default: INTERNAL_TRIGGER_TOKEN env var)",
    )
    parser.add_argument("--run-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--role", default=None, help="Role label for role pipeline mode")
    parser.add_argument("--relevant-tab", default=None, help="Explicit relevant jobs tab")
    parser.add_argument("--recruiters-tab", default=None, help="Explicit recruiters info tab")
    parser.add_argument("--poll-interval", type=float, default=3.0, help="Status poll seconds")
    parser.add_argument("--timeout", type=float, default=600.0, help="Max wait seconds")
    parser.add_argument(
        "--require-appended",
        action="store_true",
        help="Exit non-zero if recruiter_rows_appended is 0",
    )
    parser.add_argument(
        "--print-full",
        action="store_true",
        help="Print full metrics payload on completion",
    )
    args = parser.parse_args()

    if not args.token:
        raise RuntimeError("Missing token. Pass --token or set INTERNAL_TRIGGER_TOKEN.")
    if args.role and args.relevant_tab:
        raise RuntimeError("Use either --role or --relevant-tab, not both.")

    base_url = args.base_url.rstrip("/")
    headers = {"x-internal-token": args.token}
    query = _build_query(args)

    start_url = f"{base_url}/internal/run-recruiter-profile-backfill{query}"
    started = _http_json("POST", start_url, headers=headers)
    run_id = str(started.get("run_id") or "").strip()
    if not run_id:
        raise RuntimeError(f"Unexpected start response: {started}")
    print(f"Started run_id={run_id}")

    status_url = f"{base_url}/internal/run-recruiter-profile-backfill/{urllib.parse.quote(run_id)}"
    started_at = time.perf_counter()
    while True:
        metrics = _http_json("GET", status_url, headers=headers)
        status_value = str(metrics.get("status") or "").strip().lower()
        if status_value and status_value != "running":
            if args.print_full:
                print(json.dumps(metrics, indent=2, ensure_ascii=True, default=str))
            else:
                _print_summary(metrics)
            if status_value != "completed":
                raise SystemExit(1)
            appended = int(metrics.get("recruiter_rows_appended") or 0)
            if args.require_appended and appended <= 0:
                print("Backfill completed but no recruiter rows were appended.", file=sys.stderr)
                raise SystemExit(2)
            raise SystemExit(0)

        elapsed = time.perf_counter() - started_at
        if elapsed >= args.timeout:
            print(f"Timed out after {args.timeout:.1f}s waiting for run {run_id}", file=sys.stderr)
            raise SystemExit(3)
        time.sleep(max(0.2, args.poll_interval))


if __name__ == "__main__":
    main()
