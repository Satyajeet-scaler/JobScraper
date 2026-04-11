"""Load job page HTML with Playwright (session from ``linkedin_session`` storage)."""

from __future__ import annotations

import asyncio
import logging
import os
import random
from contextlib import suppress
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)


def _recruiter_between_jobs_delay_s() -> float:
    """Random pause before opening the next job URL (mimics human think-time)."""
    lo = float(os.getenv("LINKEDIN_RECRUITER_BETWEEN_JOBS_MIN_S", "1.2"))
    hi = float(os.getenv("LINKEDIN_RECRUITER_BETWEEN_JOBS_MAX_S", "3.8"))
    if hi <= 0 or lo < 0:
        return 0.0
    if hi < lo:
        lo, hi = hi, lo
    return random.uniform(lo, hi)


def _recruiter_recycle_extra_delay_s() -> float:
    """Extra pause after closing/reopening the browser (on recycle)."""
    lo = float(os.getenv("LINKEDIN_RECRUITER_RECYCLE_EXTRA_MIN_S", "1.5"))
    hi = float(os.getenv("LINKEDIN_RECRUITER_RECYCLE_EXTRA_MAX_S", "4.0"))
    if hi <= 0 or lo < 0:
        return 0.0
    if hi < lo:
        lo, hi = hi, lo
    return random.uniform(lo, hi)


def _jittered_hydration_wait_s(base: float) -> float:
    """Slightly vary the post-load wait so timing is not identical every page."""
    if base <= 0:
        return 0.0
    jitter = float(os.getenv("LINKEDIN_RECRUITER_HYDRATION_JITTER", "0.18"))
    if jitter <= 0:
        return base
    scaled = base * random.uniform(max(0.0, 1.0 - jitter), 1.0 + jitter)
    return max(0.25, scaled)


def _micro_pause_after_navigation_s() -> float:
    """Small random delay right after ``goto`` (read / layout settle) before hydration wait."""
    cap = float(os.getenv("LINKEDIN_RECRUITER_MICRO_PAUSE_MAX_S", "0.45"))
    if cap <= 0:
        return 0.0
    return random.uniform(0.06, cap)


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
                mp = _micro_pause_after_navigation_s()
                if mp > 0:
                    await asyncio.sleep(mp)
                hw = _jittered_hydration_wait_s(hydration_wait_s)
                if hw > 0:
                    await asyncio.sleep(hw)
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
                if idx > 1:
                    gap = _recruiter_between_jobs_delay_s()
                    if gap > 0:
                        logger.debug(
                            "Playwright human-like pause %.2fs before job %d/%d",
                            gap,
                            idx,
                            total,
                        )
                        await asyncio.sleep(gap)
                if recycle_every > 0 and idx > 1 and (idx - 1) % recycle_every == 0:
                    logger.info("Playwright recycle browser/context at %d/%d", idx, total)
                    await _close_session()
                    await _open_session()
                    extra = _recruiter_recycle_extra_delay_s()
                    if extra > 0:
                        logger.debug("Playwright recycle extra pause %.2fs", extra)
                        await asyncio.sleep(extra)
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
                            mp = _micro_pause_after_navigation_s()
                            if mp > 0:
                                await asyncio.sleep(mp)
                            hw = _jittered_hydration_wait_s(hydration_wait_s)
                            if hw > 0:
                                await asyncio.sleep(hw)
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
