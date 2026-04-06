"""
LinkedIn automated login via Playwright (Chromium) and persist Playwright ``storage_state`` JSON.

Use when other components need a logged-in session file (e.g. recruiter scraping later).

Environment (required for unattended login)
-------------------------------------------
- ``LINKEDIN_EMAIL`` or ``LINKEDIN_USERNAME``
- ``LINKEDIN_PASSWORD``

Optional
--------
- ``LINKEDIN_STORAGE_PATH`` — output file (default: ``<job_scaper>/data/linkedin_storage.json``)
- ``LINKEDIN_LOGIN_URL`` — default ``https://www.linkedin.com/login/in``
- ``LINKEDIN_HEADLESS`` — ``true`` / ``false`` (default ``true``)
- ``LINKEDIN_LOGIN_TIMEOUT_MS`` — max wait for post-login navigation (default ``120000``)
- ``LINKEDIN_CHECKPOINT_WAIT_MS`` — if a security checkpoint appears **with a visible browser**
  (``LINKEDIN_HEADLESS=false``), keep polling this long for you to finish CAPTCHA/2FA in the
  window (default ``300000``, i.e. 5 minutes). Headless runs still fail fast on checkpoint.
- ``LINKEDIN_DELAY_AFTER_PAGE_LOAD_S`` — seconds to wait after the login page loads before typing
  (default ``0``). Example: ``10`` to slow down automation.
- ``LINKEDIN_DELAY_BETWEEN_FIELDS_S`` — seconds to wait after filling the email field before
  filling the password (default ``0``).

**Railway / headless:** Use ``LINKEDIN_HEADLESS=true``. For a reliable session without server-side
checkpoints, sign in elsewhere and ``POST`` ``storage_state`` JSON to ``/internal/linkedin-session``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_SERVICE_ROOT = Path(__file__).resolve().parent.parent


def get_linkedin_storage_path() -> Path:
    """
    Path for Playwright ``storage_state`` JSON.

    Override with ``LINKEDIN_STORAGE_PATH`` (e.g. Railway volume: ``/data/linkedin_storage.json``).
    """
    raw = os.environ.get("LINKEDIN_STORAGE_PATH")
    if raw:
        return Path(raw).expanduser().resolve()
    return _SERVICE_ROOT / "data" / "linkedin_storage.json"


def _env_email() -> str:
    return (os.environ.get("LINKEDIN_EMAIL") or os.environ.get("LINKEDIN_USERNAME") or "").strip()


def _env_password() -> str:
    return os.environ.get("LINKEDIN_PASSWORD") or ""


def _env_headless() -> bool:
    return os.environ.get("LINKEDIN_HEADLESS", "true").lower() in ("1", "true", "yes")


def _env_login_url() -> str:
    """Default is https://www.linkedin.com/login/in (standard LinkedIn sign-in page)."""
    return os.environ.get("LINKEDIN_LOGIN_URL", "https://www.linkedin.com/login/in").strip()


def _env_nav_timeout_ms() -> int:
    return int(os.environ.get("LINKEDIN_LOGIN_TIMEOUT_MS", "120000"))


def _env_checkpoint_wait_ms() -> int:
    """Max time to wait on checkpoint/challenge when browser is visible (manual completion)."""
    return int(os.environ.get("LINKEDIN_CHECKPOINT_WAIT_MS", "300000"))


def _env_delay_after_page_load_s() -> float:
    return float(os.environ.get("LINKEDIN_DELAY_AFTER_PAGE_LOAD_S", "0"))


def _env_delay_between_fields_s() -> float:
    return float(os.environ.get("LINKEDIN_DELAY_BETWEEN_FIELDS_S", "0"))


async def _click_login_button(page: Any) -> None:
    """Submit the email/password form (not social login). Prefer role name, then primary submit."""
    # Accessible names vary by locale; regex covers common English variants.
    btn = page.get_by_role("button", name=re.compile(r"^(Sign in|Sign In|Log in|Login)$", re.I))
    if await btn.count() > 0:
        await btn.first.click()
        return
    # Fallback: primary submit on the email/password form.
    sub = page.locator(
        "form[action*='login'] button[type='submit'], "
        "button.btn__primary--large[type='submit'], "
        "button[type='submit']"
    )
    await sub.first.wait_for(state="visible", timeout=15_000)
    await sub.first.click()


def _is_checkpoint_url(url: str) -> bool:
    u = url.lower()
    return "checkpoint" in u or "challenge" in u or "captcha" in u


def _looks_logged_in(url: str) -> bool:
    u = url.lower()
    if "linkedin.com" not in u:
        return False
    if _is_checkpoint_url(u):
        return False
    if "/login" in u or "uas/login" in u:
        return False
    return True


async def _wait_until_logged_in_or_timeout(
    page: Any,
    *,
    poll_interval_s: float,
    max_wait_ms: int,
    phase: str,
) -> bool:
    """Poll ``page.url`` until :func:`_looks_logged_in` or timeout."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait_ms / 1000.0
    while loop.time() < deadline:
        await asyncio.sleep(poll_interval_s)
        url = page.url
        if _looks_logged_in(url):
            logger.info("%s: logged in (url=%s)", phase, url)
            return True
        if _is_checkpoint_url(url):
            logger.info(
                "%s: still on checkpoint/challenge — complete it in the browser if needed (url=%s)",
                phase,
                url,
            )
        else:
            logger.debug("%s: waiting (url=%s)", phase, url)
    return False


async def login_linkedin_save_storage_async(
    *,
    storage_path: str | Path | None = None,
    email: str | None = None,
    password: str | None = None,
    headless: bool | None = None,
    login_url: str | None = None,
    navigation_timeout_ms: int | None = None,
) -> Path:
    """
    Log in to LinkedIn and write Playwright ``storage_state`` to disk.

    Returns the path written. Raises ``ValueError`` if credentials are missing,
    ``RuntimeError`` if login fails or a security checkpoint is shown.
    """
    from playwright.async_api import async_playwright

    email = (email or _env_email()).strip()
    password = password or _env_password()
    if not email or not password:
        raise ValueError(
            "Set LINKEDIN_EMAIL (or LINKEDIN_USERNAME) and LINKEDIN_PASSWORD in the environment "
            "(or pass email= and password=)."
        )

    out = Path(storage_path) if storage_path is not None else get_linkedin_storage_path()
    out.parent.mkdir(parents=True, exist_ok=True)

    headless = _env_headless() if headless is None else headless
    login_url = login_url or _env_login_url()
    nav_timeout_ms = navigation_timeout_ms if navigation_timeout_ms is not None else _env_nav_timeout_ms()
    checkpoint_wait_ms = _env_checkpoint_wait_ms()
    delay_after_load_s = _env_delay_after_page_load_s()
    delay_between_fields_s = _env_delay_between_fields_s()
    poll_interval_s = 0.5
    max_polls = max(10, int(nav_timeout_ms / (poll_interval_s * 1000)))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        try:
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                },
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            page = await context.new_page()
            await page.goto(login_url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            logger.info("Opened login URL: %s", page.url)
            if delay_after_load_s > 0:
                logger.info(
                    "Waiting LINKEDIN_DELAY_AFTER_PAGE_LOAD_S=%s s before filling credentials",
                    delay_after_load_s,
                )
                await asyncio.sleep(delay_after_load_s)

            user_loc = (
                page.locator("#username")
                .or_(page.locator('input[name="session_key"]'))
                .or_(page.locator('input[autocomplete="username"]'))
            )
            await user_loc.first.wait_for(state="visible", timeout=30_000)
            await user_loc.first.click()
            await user_loc.first.fill(email)
            if delay_between_fields_s > 0:
                await asyncio.sleep(delay_between_fields_s)

            pwd_loc = (
                page.locator("#password")
                .or_(page.locator('input[name="session_password"]'))
                .or_(page.locator('input[autocomplete="current-password"]'))
            )
            await pwd_loc.first.fill(password)

            await _click_login_button(page)

            logged_in = False
            for i in range(max_polls):
                await asyncio.sleep(poll_interval_s)
                url = page.url

                if _is_checkpoint_url(url):
                    if headless:
                        raise RuntimeError(
                            "LinkedIn showed a security checkpoint or challenge. "
                            "Headless mode cannot complete it. Run with LINKEDIN_HEADLESS=false, "
                            "finish CAPTCHA/2FA in the browser window, and increase "
                            "LINKEDIN_CHECKPOINT_WAIT_MS if needed (default 5 minutes)."
                        )
                    logger.warning(
                        "Checkpoint/challenge page — complete verification in the Chromium window. "
                        "Waiting up to %s ms (LINKEDIN_CHECKPOINT_WAIT_MS).",
                        checkpoint_wait_ms,
                    )
                    ok = await _wait_until_logged_in_or_timeout(
                        page,
                        poll_interval_s=1.0,
                        max_wait_ms=checkpoint_wait_ms,
                        phase="checkpoint",
                    )
                    if ok:
                        logged_in = True
                        break
                    raise RuntimeError(
                        "LinkedIn checkpoint not cleared in time "
                        f"(last URL: {page.url}). Complete verification faster or raise "
                        "LINKEDIN_CHECKPOINT_WAIT_MS."
                    )

                err_sel = page.locator(
                    "#error-for-password, #error-for-username, .form__label--error, [role='alert']"
                )
                if await err_sel.count() > 0:
                    txt = await err_sel.first.text_content()
                    if txt and txt.strip():
                        raise RuntimeError(f"LinkedIn login error: {txt.strip()}")

                if _looks_logged_in(url):
                    logged_in = True
                    break

                if i > 0 and i % 8 == 0:
                    logger.debug("Still waiting for post-login URL, current=%s", url)

            if not logged_in:
                raise RuntimeError(
                    "LinkedIn login did not reach a logged-in page in time "
                    f"(last URL: {page.url}). Check credentials and LINKEDIN_LOGIN_URL."
                )

            await asyncio.sleep(1.0)
            await context.storage_state(path=str(out))
            logger.info("Saved LinkedIn storage state to %s", out)
            return out
        finally:
            try:
                await browser.close()
            except Exception as exc:
                # Common after Ctrl+C: driver connection is already gone.
                logger.debug("Browser close skipped or failed: %s", exc)


def login_linkedin_save_storage_sync(**kwargs: Any) -> Path:
    """Synchronous wrapper around :func:`login_linkedin_save_storage_async`."""
    return asyncio.run(login_linkedin_save_storage_async(**kwargs))


def validate_playwright_storage_state(data: Any) -> dict[str, Any]:
    """Ensure JSON looks like a Playwright ``storage_state`` export."""
    if not isinstance(data, dict):
        raise ValueError("Body must be a JSON object")
    cookies = data.get("cookies")
    if cookies is None:
        raise ValueError(
            "Missing 'cookies'; expected Playwright storage_state "
            "(from context.storage_state() or equivalent export)"
        )
    if not isinstance(cookies, list):
        raise ValueError("'cookies' must be a list")
    return data


def save_linkedin_storage_state_json(data: dict[str, Any]) -> Path:
    """Write validated storage state to :func:`get_linkedin_storage_path`."""
    validate_playwright_storage_state(data)
    path = get_linkedin_storage_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    logger.info(
        "Saved LinkedIn storage state to %s (%d cookies)",
        path,
        len(data.get("cookies", [])),
    )
    return path
