#!/usr/bin/env python3
"""
Open Chromium for manual LinkedIn login; save Playwright storage to data/linkedin_storage.json.

Run from the job_scaper directory (or anywhere with PYTHONPATH=job_scaper). Re-run when the
session expires. Uses the same path as ``services.linkedin_session.get_linkedin_storage_path``.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Allow `python linkedin_manual_login.py` from job_scaper without package install
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


async def _main(storage_path: Path | None) -> None:
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
    args = parser.parse_args()
    asyncio.run(_main(args.output))


if __name__ == "__main__":
    main()
