"""Load job page HTML with Playwright (session from ``linkedin_session`` storage)."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from pathlib import Path
from typing import Any, Sequence

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


async def fetch_html_playwright_many(
    urls: Sequence[str],
    *,
    timeout_ms: float = 60_000.0,
    storage_state_path: str | Path | None = None,
    headless: bool = True,
    hydration_wait_s: float = 5.0,
    retry_count: int = 3,
    retry_base_delay_s: float = 1.0,
    force_fail_timeout_s: float = 15.0,
    recycle_every: int = 25,
) -> list[dict[str, Any]]:
    """
    Fetch many URLs using one browser/context to reduce startup overhead.
    Returns a list aligned with input URLs: each item has `url` and either `html` or `error`.
    """
    from playwright.async_api import async_playwright

    state = Path(storage_state_path) if storage_state_path else None
    ctx_kwargs: dict[str, Any] = {
        "viewport": {"width": 1280, "height": 800},
        "locale": "en-US",
    }
    if state is not None and state.is_file():
        ctx_kwargs["storage_state"] = str(state)

    async with async_playwright() as p:
        browser = None
        context = None

        async def _open_session():
            nonlocal browser, context
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(**ctx_kwargs)

        async def _close_session():
            nonlocal browser, context
            if context is not None:
                with suppress(Exception):
                    await context.close()
                context = None
            if browser is not None:
                with suppress(Exception):
                    await browser.close()
                browser = None

        await _open_session()
        try:
            out: list[dict[str, Any]] = []
            total = len(urls)
            for idx, url in enumerate(urls, start=1):
                if recycle_every > 0 and idx > 1 and (idx - 1) % recycle_every == 0:
                    logger.info("Playwright recycle browser/context at %d/%d", idx, total)
                    await _close_session()
                    await _open_session()
                max_attempts = retry_count + 1
                attempt = 1
                last_error: Exception | None = None
                while attempt <= max_attempts:
                    if context is None:
                        await _open_session()
                    page = await context.new_page()
                    try:
                        logger.info("Playwright fetch (%d/%d) attempt=%d url=%s", idx, total, attempt, url)

                        async def _fetch_once() -> str:
                            await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_ms))
                            if hydration_wait_s > 0:
                                await asyncio.sleep(hydration_wait_s)
                            return await page.content()

                        if force_fail_timeout_s > 0:
                            html = await asyncio.wait_for(_fetch_once(), timeout=force_fail_timeout_s)
                        else:
                            html = await _fetch_once()
                        out.append({"url": url, "html": html})
                        last_error = None
                        break
                    except Exception as exc:
                        last_error = exc
                        if attempt >= max_attempts:
                            break
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
                    finally:
                        with suppress(Exception):
                            await page.close()
                if last_error is not None:
                    out.append({"url": url, "error": str(last_error)})
            return out
        finally:
            await _close_session()
