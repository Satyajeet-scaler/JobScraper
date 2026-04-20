#!/usr/bin/env python3
"""
Standalone probe to observe Cloudflare challenge HTML on hiring.cafe.

This script intentionally uses an easily detectable Selenium Chrome profile,
so Cloudflare challenge pages are more likely to appear for inspection.

It captures snapshots (PNG + HTML + JSON metadata) for:
- initial landing
- challenge detected
- challenge still active at timeout
- challenge passed and hiring.cafe page ready

Example:
  python3 scripts/probe_hirecafe_cloudflare.py --headed
  python3 scripts/probe_hirecafe_cloudflare.py --attempts 5 --timeout-seconds 45
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_URL = os.getenv("HIRECAFE_SEARCH_URL", "https://hiring.cafe/")

CLOUDFLARE_MARKERS = (
    "just a moment",
    "checking your browser",
    "verify you are human",
    "performing security verification",
    "this website uses a security service",
    "ray id:",
    "security check",
)

CLOUDFLARE_HTML_MARKERS = (
    "/cdn-cgi/challenge-platform/",
    "cf-turnstile-response",
    "cf-chl-widget",
    "challenge-success-text",
    "challenge-error-text",
)


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)


def _launch_detectable_chrome(
    headless: bool,
    chrome_binary: str,
    chromedriver_path: str,
    window_size: str,
):
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService
    except Exception as exc:  # pragma: no cover - depends on runtime env
        raise RuntimeError(
            "Selenium is required for this probe. Install dependencies with: pip install -r requirements.txt"
        ) from exc

    options = ChromeOptions()

    # Deliberately detectable profile (no stealth hardening).
    options.add_argument("--enable-automation")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--incognito")
    options.add_argument(f"--window-size={window_size}")

    if headless:
        options.add_argument("--headless=new")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "HeadlessChrome/124.0.0.0 Safari/537.36"
        )

    if chrome_binary:
        options.binary_location = chrome_binary

    options.set_capability("goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"})

    if chromedriver_path:
        service = ChromeService(executable_path=chromedriver_path)
    else:
        service = ChromeService()

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(120)
    return driver


def _probe_cloudflare_state(driver) -> dict[str, Any]:
    state: dict[str, Any] = {
        "active": False,
        "url": "",
        "title": "",
        "marker_hits": [],
        "html_marker_hits": [],
        "selector_hits": {},
        "strong_signal": False,
        "signal_score": 0,
    }

    try:
        state["url"] = driver.current_url or ""
    except Exception:
        pass

    try:
        state["title"] = driver.title or ""
    except Exception:
        pass

    try:
        page = driver.page_source or ""
        page_lower = page.lower()
        content = f"{state['title']}\n{page}".lower()
        state["marker_hits"] = [marker for marker in CLOUDFLARE_MARKERS if marker in content]
        state["html_marker_hits"] = [marker for marker in CLOUDFLARE_HTML_MARKERS if marker in page_lower]
    except Exception:
        pass

    try:
        selector_hits = driver.execute_script(
            "const bodyText=((document.body&&document.body.innerText)||'').toLowerCase();"
            "return {"
            "cf_iframe: !!document.querySelector(\"iframe[src*='challenges.cloudflare.com']\"),"
            "turnstile_input: !!document.querySelector(\"input[name='cf-turnstile-response'], input[id^='cf-chl-widget'][id$='_response']\"),"
            "turnstile_widget: !!document.querySelector(\"div.cf-turnstile\"),"
            "challenge_form: !!document.querySelector(\"form#challenge-form, #challenge-stage\"),"
            "challenge_platform_script: !!document.querySelector(\"script[src*='/cdn-cgi/challenge-platform/']\"),"
            "turnstile_script: !!document.querySelector(\"script[src*='challenges.cloudflare.com/turnstile']\"),"
            "challenge_state_nodes: !!document.querySelector(\"#challenge-success-text, #challenge-error-text, .loading-verifying, .ray-id\"),"
            "meta_refresh: !!document.querySelector(\"meta[http-equiv='refresh'][content]\"),"
            "ray_id_text: /ray id:/.test(bodyText),"
            "verify_copy: /performing security verification|this website uses a security service|just a moment|verify you are human|security check|checking your browser/.test(bodyText),"
            "challenge_text: /just a moment|verify you are human|security check|checking your browser|performing security verification/.test(bodyText)"
            "};"
        ) or {}
    except Exception:
        selector_hits = {}

    state["selector_hits"] = selector_hits

    strong_signal = any(
        bool(selector_hits.get(k))
        for k in (
            "cf_iframe",
            "turnstile_input",
            "challenge_form",
            "challenge_platform_script",
            "turnstile_script",
            "challenge_state_nodes",
        )
    )
    weak_score = (
        len(state.get("marker_hits", []))
        + len(state.get("html_marker_hits", []))
        + int(bool(selector_hits.get("verify_copy")))
        + int(bool(selector_hits.get("ray_id_text")))
        + int(bool(selector_hits.get("meta_refresh")))
    )
    state["strong_signal"] = strong_signal
    state["signal_score"] = (3 if strong_signal else 0) + weak_score
    state["active"] = bool(strong_signal or weak_score >= 2)
    return state


def _probe_hiring_cafe_ready(driver) -> dict[str, Any]:
    challenge = _probe_cloudflare_state(driver)
    state: dict[str, Any] = {
        "ready": False,
        "reason": "unknown",
        "challenge_active": bool(challenge.get("active")),
        "url": challenge.get("url", ""),
        "ready_state": None,
        "has_next_data": False,
        "has_next_root": False,
        "has_viewjob_link": False,
    }

    try:
        dom_state = driver.execute_script(
            "return {"
            "readyState: document.readyState || null,"
            "hasNextData: !!document.querySelector(\"script#__NEXT_DATA__\"),"
            "hasNextRoot: !!document.querySelector(\"#__next\"),"
            "hasViewjobLink: !!document.querySelector(\"a[href*='/viewjob/']\")"
            "};"
        ) or {}
    except Exception:
        dom_state = {}

    state["ready_state"] = dom_state.get("readyState")
    state["has_next_data"] = bool(dom_state.get("hasNextData"))
    state["has_next_root"] = bool(dom_state.get("hasNextRoot"))
    state["has_viewjob_link"] = bool(dom_state.get("hasViewjobLink"))

    on_hiring_cafe = "hiring.cafe" in str(state.get("url", "")).lower()
    ready_state_ok = state["ready_state"] in ("interactive", "complete")
    app_shell = bool(
        state["has_next_data"] or state["has_next_root"] or state["has_viewjob_link"]
    )

    if state["challenge_active"]:
        state["reason"] = "challenge_active"
    elif not on_hiring_cafe:
        state["reason"] = "not_on_hiring_cafe"
    elif not ready_state_ok:
        state["reason"] = "dom_not_ready"
    elif not app_shell:
        state["reason"] = "app_shell_missing"
    else:
        state["reason"] = "ready"
        state["ready"] = True
    return state


def _extract_structure(driver) -> dict[str, Any]:
    try:
        return driver.execute_script(
            "const iframes=[...document.querySelectorAll('iframe')]"
            ".slice(0,8)"
            ".map((el)=>({id:el.id||null,name:el.name||null,src:el.src||null,title:el.title||null}));"
            "const forms=[...document.querySelectorAll('form')]"
            ".slice(0,8)"
            ".map((el)=>({id:el.id||null,action:el.getAttribute('action')||null,method:el.getAttribute('method')||null}));"
            "const scriptIds=[...document.querySelectorAll('script[id]')]"
            ".slice(0,20)"
            ".map((el)=>el.id);"
            "const bodyText=((document.body&&document.body.innerText)||'').replace(/\\s+/g,' ').trim();"
            "return {"
            "document_ready_state: document.readyState || null,"
            "title: document.title || null,"
            "iframe_count: document.querySelectorAll('iframe').length,"
            "form_count: document.querySelectorAll('form').length,"
            "turnstile_input_count: document.querySelectorAll(\"input[name='cf-turnstile-response']\").length,"
            "turnstile_widget_count: document.querySelectorAll('div.cf-turnstile').length,"
            "challenge_form_present: !!document.querySelector(\"form#challenge-form, #challenge-stage\"),"
            "next_data_present: !!document.querySelector(\"script#__NEXT_DATA__\"),"
            "next_root_present: !!document.querySelector(\"#__next\"),"
            "viewjob_link_count: document.querySelectorAll(\"a[href*='/viewjob/']\").length,"
            "iframes: iframes,"
            "forms: forms,"
            "script_ids: scriptIds,"
            "text_preview: bodyText.slice(0, 1200)"
            "};"
        ) or {}
    except Exception:
        return {}


def _capture_snapshot(
    driver,
    output_dir: Path,
    stage: str,
    extra: dict[str, Any],
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    base = output_dir / f"{_utc_stamp()}_{_safe_name(stage)}"
    png_path = base.with_suffix(".png")
    html_path = base.with_suffix(".html")
    json_path = base.with_suffix(".json")

    screenshot_ok = False
    try:
        screenshot_ok = bool(driver.save_screenshot(str(png_path)))
    except Exception:
        screenshot_ok = False

    html_text = ""
    try:
        html_text = driver.page_source or ""
        html_path.write_text(html_text, encoding="utf-8")
    except Exception:
        pass

    metadata = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "url": extra.get("url", ""),
        "title": extra.get("title", ""),
        "screenshot_ok": screenshot_ok,
        "screenshot_path": str(png_path),
        "html_path": str(html_path),
        "html_length": len(html_text),
        "cloudflare": extra.get("cloudflare", {}),
        "page_ready": extra.get("page_ready", {}),
        "dom_structure": extra.get("dom_structure", {}),
    }
    json_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=True, default=str) + "\n",
        encoding="utf-8",
    )

    return {
        "stage": stage,
        "metadata": str(json_path),
        "html": str(html_path),
        "screenshot": str(png_path),
    }


def _run_attempt(args: argparse.Namespace, output_dir: Path, attempt_no: int) -> dict[str, Any]:
    driver = _launch_detectable_chrome(
        headless=not args.headed,
        chrome_binary=args.chrome_binary,
        chromedriver_path=args.chromedriver_path,
        window_size=args.window_size,
    )

    snapshots: list[dict[str, str]] = []
    events: list[dict[str, Any]] = []
    challenge_detected = False
    page_ready_after_challenge = False
    started = time.time()

    try:
        url = args.url
        if args.append_probe_param:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}cf_probe_attempt={attempt_no}_{int(started)}"

        driver.get(url)

        initial_cf = _probe_cloudflare_state(driver)
        initial_page = _probe_hiring_cafe_ready(driver)
        initial_structure = _extract_structure(driver)

        snapshots.append(
            _capture_snapshot(
                driver,
                output_dir,
                f"attempt_{attempt_no}_initial_landing",
                {
                    "url": initial_cf.get("url", ""),
                    "title": initial_cf.get("title", ""),
                    "cloudflare": initial_cf,
                    "page_ready": initial_page,
                    "dom_structure": initial_structure,
                },
            )
        )

        while time.time() - started <= args.timeout_seconds:
            cf_state = _probe_cloudflare_state(driver)
            ready_state = _probe_hiring_cafe_ready(driver)
            event = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "attempt": attempt_no,
                "cloudflare_active": bool(cf_state.get("active")),
                "marker_hits": cf_state.get("marker_hits", []),
                "selector_hits": cf_state.get("selector_hits", {}),
                "ready": bool(ready_state.get("ready")),
                "ready_reason": ready_state.get("reason"),
                "url": cf_state.get("url", ""),
                "title": cf_state.get("title", ""),
            }
            events.append(event)

            if cf_state.get("active") and not challenge_detected:
                challenge_detected = True
                snapshots.append(
                    _capture_snapshot(
                        driver,
                        output_dir,
                        f"attempt_{attempt_no}_challenge_detected",
                        {
                            "url": cf_state.get("url", ""),
                            "title": cf_state.get("title", ""),
                            "cloudflare": cf_state,
                            "page_ready": ready_state,
                            "dom_structure": _extract_structure(driver),
                        },
                    )
                )

            if challenge_detected and (not cf_state.get("active")) and ready_state.get("ready"):
                page_ready_after_challenge = True
                snapshots.append(
                    _capture_snapshot(
                        driver,
                        output_dir,
                        f"attempt_{attempt_no}_challenge_passed_page_ready",
                        {
                            "url": cf_state.get("url", ""),
                            "title": cf_state.get("title", ""),
                            "cloudflare": cf_state,
                            "page_ready": ready_state,
                            "dom_structure": _extract_structure(driver),
                        },
                    )
                )
                break

            if args.capture_each_poll and cf_state.get("active"):
                snapshots.append(
                    _capture_snapshot(
                        driver,
                        output_dir,
                        f"attempt_{attempt_no}_challenge_poll",
                        {
                            "url": cf_state.get("url", ""),
                            "title": cf_state.get("title", ""),
                            "cloudflare": cf_state,
                            "page_ready": ready_state,
                            "dom_structure": _extract_structure(driver),
                        },
                    )
                )

            time.sleep(max(0.1, args.poll_seconds))

        final_cf = _probe_cloudflare_state(driver)
        final_ready = _probe_hiring_cafe_ready(driver)

        if challenge_detected and not page_ready_after_challenge:
            snapshots.append(
                _capture_snapshot(
                    driver,
                    output_dir,
                    f"attempt_{attempt_no}_challenge_active_or_unverified_end",
                    {
                        "url": final_cf.get("url", ""),
                        "title": final_cf.get("title", ""),
                        "cloudflare": final_cf,
                        "page_ready": final_ready,
                        "dom_structure": _extract_structure(driver),
                    },
                )
            )

        if not challenge_detected:
            snapshots.append(
                _capture_snapshot(
                    driver,
                    output_dir,
                    f"attempt_{attempt_no}_no_challenge_detected",
                    {
                        "url": final_cf.get("url", ""),
                        "title": final_cf.get("title", ""),
                        "cloudflare": final_cf,
                        "page_ready": final_ready,
                        "dom_structure": _extract_structure(driver),
                    },
                )
            )

        return {
            "attempt": attempt_no,
            "challenge_detected": challenge_detected,
            "page_ready_after_challenge": page_ready_after_challenge,
            "duration_seconds": round(time.time() - started, 2),
            "events": events,
            "snapshots": snapshots,
        }
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Open hiring.cafe in a detectable browser and capture Cloudflare challenge HTML structure."
        )
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="Target URL (default: HIRECAFE_SEARCH_URL or https://hiring.cafe/)")
    parser.add_argument("--attempts", type=int, default=3, help="How many browser attempts to run (default: 3).")
    parser.add_argument("--timeout-seconds", type=int, default=45, help="Poll timeout per attempt (default: 45).")
    parser.add_argument("--poll-seconds", type=float, default=1.0, help="Polling interval in seconds (default: 1.0).")
    parser.add_argument("--headed", action="store_true", help="Run visible browser window instead of headless.")
    parser.add_argument("--capture-each-poll", action="store_true", help="Capture snapshot on every challenge-active poll.")
    parser.add_argument("--append-probe-param", action="store_true", help="Append a unique query parameter per attempt.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("debug_screenshots/hirecafe_cloudflare_probe"),
        help="Directory where snapshots and summary are written.",
    )
    parser.add_argument("--chrome-binary", default=os.getenv("HIRECAFE_CHROME_BINARY", ""), help="Optional Chrome/Chromium binary path.")
    parser.add_argument("--chromedriver-path", default=os.getenv("HIRECAFE_CHROMEDRIVER_PATH", ""), help="Optional chromedriver path.")
    parser.add_argument("--window-size", default="1366,768", help="Browser window size as W,H (default: 1366,768).")

    args = parser.parse_args()

    if args.attempts <= 0:
        print("--attempts must be > 0", file=sys.stderr)
        return 2
    if args.timeout_seconds <= 0:
        print("--timeout-seconds must be > 0", file=sys.stderr)
        return 2

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    run_started = time.time()
    summary: dict[str, Any] = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "url": args.url,
        "attempts_requested": args.attempts,
        "timeout_seconds": args.timeout_seconds,
        "poll_seconds": args.poll_seconds,
        "headed": bool(args.headed),
        "output_dir": str(output_dir),
        "results": [],
        "challenge_detected": False,
        "page_ready_after_challenge": False,
    }

    try:
        for attempt_no in range(1, args.attempts + 1):
            print(f"[probe] attempt {attempt_no}/{args.attempts} starting")
            result = _run_attempt(args, output_dir, attempt_no)
            summary["results"].append(result)

            if result.get("challenge_detected"):
                summary["challenge_detected"] = True
            if result.get("page_ready_after_challenge"):
                summary["page_ready_after_challenge"] = True

            print(
                json.dumps(
                    {
                        "attempt": attempt_no,
                        "challenge_detected": result.get("challenge_detected"),
                        "page_ready_after_challenge": result.get("page_ready_after_challenge"),
                        "duration_seconds": result.get("duration_seconds"),
                    },
                    ensure_ascii=True,
                )
            )

            if summary["challenge_detected"] and summary["page_ready_after_challenge"]:
                break

        summary["duration_seconds"] = round(time.time() - run_started, 2)
        summary["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        summary_path = output_dir / f"{_utc_stamp()}_probe_summary.json"
        summary_path.write_text(
            json.dumps(summary, indent=2, ensure_ascii=True, default=str) + "\n",
            encoding="utf-8",
        )

        print(f"[probe] summary: {summary_path}")
        print(
            json.dumps(
                {
                    "status": "completed",
                    "challenge_detected": summary["challenge_detected"],
                    "page_ready_after_challenge": summary["page_ready_after_challenge"],
                    "output_dir": str(output_dir),
                    "summary_file": str(summary_path),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
                indent=2,
                ensure_ascii=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
