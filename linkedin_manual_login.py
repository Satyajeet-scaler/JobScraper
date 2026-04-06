#!/usr/bin/env python3
"""
Open Chromium for manual LinkedIn login; save Playwright storage to data/linkedin_storage.json.

Run from the job_scaper directory (or anywhere with PYTHONPATH=job_scaper). Re-run when the
session expires. Uses the same path as ``services.linkedin_session.get_linkedin_storage_path``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

# Allow `python linkedin_manual_login.py` from job_scaper without package install
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


async def _main(storage_path: Path | None, *, upload: bool = True) -> None:
    from playwright.async_api import async_playwright

    from services.linkedin_session import get_linkedin_storage_path

    out = storage_path if storage_path is not None else get_linkedin_storage_path()
    out.parent.mkdir(parents=True, exist_ok=True)

    print(
        "Opening Chromium — log in to LinkedIn in the browser window.\n"
        "Complete any 2FA or CAPTCHA there. When you are logged in and the page looks ready,\n"
        "return to this terminal and press Enter to save the session.\n"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        try:
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )
            page = await context.new_page()
            await page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")

            await asyncio.to_thread(input, "Press Enter here after you are logged in... ")

            await context.storage_state(path=str(out))
        finally:
            await browser.close()

    print(f"Saved session to {out}")
    if upload:
        _upload_session_to_railway_if_configured(out)


def _upload_session_to_railway_if_configured(storage_path: Path) -> None:
    """
    If LINKEDIN_SESSION_UPLOAD_URL and INTERNAL_TRIGGER_TOKEN are set, POST the JSON to
    Railway ``POST /internal/linkedin-session`` (same as manual curl).
    """
    url = (os.getenv("LINKEDIN_SESSION_UPLOAD_URL") or "").strip()
    token = (os.getenv("INTERNAL_TRIGGER_TOKEN") or "").strip()
    if not url:
        print(
            "Tip: set LINKEDIN_SESSION_UPLOAD_URL (e.g. https://<your-app>.up.railway.app/internal/linkedin-session) "
            "and INTERNAL_TRIGGER_TOKEN to upload this file to the server automatically next time."
        )
        return
    if not token:
        print("LINKEDIN_SESSION_UPLOAD_URL is set but INTERNAL_TRIGGER_TOKEN is missing; skipping upload.")
        return

    try:
        import requests
    except ImportError:
        print("requests is required for upload; pip install requests")
        sys.exit(1)

    try:
        raw = storage_path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Could not read session file for upload: {exc}")
        sys.exit(1)

    try:
        response = requests.post(
            url.rstrip("/"),
            json=data,
            headers={
                "Content-Type": "application/json",
                "x-internal-token": token,
            },
            timeout=60,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Upload to Railway failed: {exc}")
        if hasattr(exc, "response") and exc.response is not None:
            print(getattr(exc.response, "text", "")[:2000])
        sys.exit(1)

    try:
        body = response.json()
    except json.JSONDecodeError:
        body = response.text
    print(f"Uploaded session to Railway: {body}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Save LinkedIn Playwright session (manual login) for job_scaper",
    )
    parser.add_argument(
        "output",
        nargs="?",
        type=Path,
        default=None,
        help="Output JSON path (default: get_linkedin_storage_path() -> data/linkedin_storage.json)",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip POST to Railway even when LINKEDIN_SESSION_UPLOAD_URL is set.",
    )
    args = parser.parse_args()
    asyncio.run(_main(args.output, upload=not args.no_upload))


if __name__ == "__main__":
    main()
