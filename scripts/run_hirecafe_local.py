#!/usr/bin/env python3
"""
Run HireCafe scraping locally without starting FastAPI.

Examples:
  python scripts/run_hirecafe_local.py
  python scripts/run_hirecafe_local.py --max-samples 80
  python scripts/run_hirecafe_local.py --output data/hirecafe_local_jobs.json
  python scripts/run_hirecafe_local.py --pipeline
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _default_output_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return ROOT / "data" / f"hirecafe_local_{stamp}.json"


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )


def _run_scrape_only(
    max_samples: int,
    output_path: Path,
    search_url: str | None,
) -> dict[str, Any]:
    from services.hire_cafe import normalize_hirecafe_item, scrape_hirecafe_jobs

    raw_jobs = scrape_hirecafe_jobs(
        max_samples=max_samples,
        search_url=(search_url or "").strip() or None,
    )
    normalized_jobs = [normalize_hirecafe_item(row) for row in raw_jobs]

    payload = {
        "status": "completed",
        "mode": "scrape-only",
        "scraped_count": len(normalized_jobs),
        "max_samples": max_samples,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "jobs": normalized_jobs,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True, default=str) + "\n",
        encoding="utf-8",
    )

    return {
        "status": "completed",
        "mode": "scrape-only",
        "scraped_count": len(normalized_jobs),
        "output_file": str(output_path),
    }


def _run_pipeline(max_samples: int, search_url: str | None) -> dict[str, Any]:
    from services.hirecafe_only_pipeline import run_hirecafe_scrape_only_pipeline

    os.environ["HIRECAFE_MAX_SAMPLES"] = str(max_samples)
    metrics = run_hirecafe_scrape_only_pipeline(
        search_url=(search_url or "").strip() or None,
    )
    return {
        "status": metrics.get("status", "completed"),
        "mode": "pipeline",
        "run_id": metrics.get("run_id"),
        "scraped_count": metrics.get("scraped_count"),
        "tab_name": metrics.get("tab_name"),
        "run_date": metrics.get("run_date"),
        "duration_seconds": metrics.get("duration_seconds"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run HireCafe scraping locally without starting the API server."
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=int(os.getenv("HIRECAFE_MAX_SAMPLES", "200")),
        help="Maximum jobs to capture (default: env HIRECAFE_MAX_SAMPLES or 200).",
    )
    parser.add_argument(
        "--url",
        default=os.getenv("HIRECAFE_SEARCH_URL", "").strip() or None,
        help=(
            "HireCafe URL/searchState to scrape. "
            "Defaults to env HIRECAFE_SEARCH_URL when present."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_default_output_path(),
        help=(
            "Output JSON path for scrape-only mode "
            "(default: data/hirecafe_local_<timestamp>.json)."
        ),
    )
    parser.add_argument(
        "--pipeline",
        action="store_true",
        help=(
            "Run the existing HireCafe sheet-writing pipeline instead of local JSON output. "
            "Requires Google Sheets env vars."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("APP_LOG_LEVEL", "INFO"),
        help="Logging level (default: env APP_LOG_LEVEL or INFO).",
    )
    parser.add_argument(
        "--browser-mode",
        choices=("stealth", "detectable"),
        default=os.getenv("HIRECAFE_BROWSER_MODE", "stealth"),
        help=(
            "Browser mode: stealth (undetected-chromedriver) or detectable "
            "(intentionally easier for Cloudflare to challenge)."
        ),
    )
    parser.add_argument(
        "--detectable-headed",
        action="store_true",
        help="When browser-mode=detectable, run with visible Chrome window (not headless).",
    )
    parser.add_argument(
        "--observe-pages",
        action="store_true",
        help=(
            "Capture challenge/page observation artifacts (PNG + HTML + metadata JSON) "
            "to debug_screenshots/hirecafe_cloudflare by default."
        ),
    )
    parser.add_argument(
        "--observe-dir",
        type=Path,
        default=Path(os.getenv("HIRECAFE_OBSERVE_DIR", "debug_screenshots/hirecafe_cloudflare")),
        help="Directory for --observe-pages artifacts.",
    )
    args = parser.parse_args()

    _configure_logging(args.log_level)

    os.environ["HIRECAFE_BROWSER_MODE"] = args.browser_mode
    if args.detectable_headed:
        os.environ["HIRECAFE_DETECTABLE_HEADLESS"] = "false"
    if args.observe_pages:
        os.environ["HIRECAFE_OBSERVE_PAGES"] = "true"
        os.environ["HIRECAFE_OBSERVE_DIR"] = str(args.observe_dir)

    try:
        if args.max_samples <= 0:
            raise ValueError("--max-samples must be a positive integer.")

        if args.pipeline:
            result = _run_pipeline(args.max_samples, args.url)
        else:
            result = _run_scrape_only(args.max_samples, args.output, args.url)

        result["browser_mode"] = args.browser_mode
        result["observe_pages"] = bool(args.observe_pages)
        if args.url:
            result["url"] = args.url
        if args.observe_pages:
            result["observe_dir"] = str(args.observe_dir)

        print(json.dumps(result, indent=2, ensure_ascii=True, default=str))
        return 0
    except Exception as exc:
        error_payload = {
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
        print(json.dumps(error_payload, indent=2, ensure_ascii=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
