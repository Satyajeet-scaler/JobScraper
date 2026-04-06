"""Load job page HTML with Playwright (session from ``linkedin_session`` storage)."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


async def fetch_html_playwright(
    url: str,
    *,
    timeout_ms: float = 60_000.0,
    storage_state_path: str | Path | None = None,
    headless: bool = True,
    hydration_wait_s: float = 5.0,
    retry_count: int = 3,
    retry_base_delay_s: float = 1.0,
) -> str:
    """Load page in Chromium; use saved LinkedIn session when ``storage_state_path`` exists."""
    from playwright.async_api import async_playwright

    logger.debug(
        "Playwright fetch headless=%s storage=%s url=%s",
        headless,
        bool(storage_state_path and Path(storage_state_path).is_file()),
        url,
    )
    state = Path(storage_state_path) if storage_state_path else None
    ctx_kwargs: dict = {
        "viewport": {"width": 1280, "height": 800},
        "locale": "en-US",
    }
    if state is not None and state.is_file():
        ctx_kwargs["storage_state"] = str(state)

    async def _fetch_once() -> str:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            try:
                context = await browser.new_context(**ctx_kwargs)
                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_ms))
                if hydration_wait_s > 0:
                    await asyncio.sleep(hydration_wait_s)
                html = await page.content()
                logger.debug("Playwright got %d bytes for %s", len(html), url)
                return html
            finally:
                await browser.close()

    max_attempts = retry_count + 1
    attempt = 1
    while True:
        try:
            return await _fetch_once()
        except Exception:
            if attempt >= max_attempts:
                raise
            delay = retry_base_delay_s * (2 ** (attempt - 1))
            logger.warning(
                "Playwright fetch retry %d/%d in %.1fs for %s",
                attempt,
                retry_count,
                delay,
                url,
            )
            await asyncio.sleep(delay)
            attempt += 1
