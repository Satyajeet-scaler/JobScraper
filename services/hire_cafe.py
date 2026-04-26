"""
HireCafe scraper — captures job payloads from hiring.cafe by intercepting
network responses via Chrome DevTools Protocol using undetected-chromedriver.

Requires xvfb virtual display on headless servers (the uvicorn process should
be launched via ``xvfb-run -a``).
"""

import glob
import html
import json
import logging
import math
import os
import random
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from services.hirecafe_carousel import (
    _click_carousel_next,
    _dedupe_card_elements,
    _grid_children_cards,
    _infinite_scroll_root,
    _viewjob_hrefs_in_card,
)

logger = logging.getLogger(__name__)

_DEFAULT_HIRECAFE_SEARCH_URL = (
    "https://hiring.cafe/?searchState=%7B%22locations%22%3A%5B%7B%22formatted_address%22%3A%22India%22%2C%22types%22%3A%5B%22country%22%5D%2C%22geometry%22%3A%7B%22location%22%3A%7B%22lat%22%3A19.0748%2C%22lon%22%3A72.8856%7D%7D%2C%22id%22%3A%22user_country%22%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22India%22%2C%22short_name%22%3A%22IN%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22options%22%3A%7B%22flexible_regions%22%3A%5B%22anywhere_in_continent%22%2C%22anywhere_in_world%22%5D%7D%7D%5D%2C%22jobTitleQuery%22%3A%22%5C%22data+analyst%5C%22%22%7D"
)
HIRECAFE_SEARCH_URL = os.getenv("HIRECAFE_SEARCH_URL", _DEFAULT_HIRECAFE_SEARCH_URL).strip()

CLOUDFLARE_WAIT_SECONDS = int(os.getenv("HIRECAFE_CLOUDFLARE_WAIT_SECONDS", "10"))
CLOUDFLARE_CLEAR_TIMEOUT_SECONDS = int(os.getenv("HIRECAFE_CF_CLEAR_TIMEOUT_SECONDS", "35"))
POST_VERIFY_WAIT_SECONDS = int(os.getenv("HIRECAFE_POST_VERIFY_WAIT_SECONDS", "8"))

HARDCODED_CF_CLICK_X = int(os.getenv("HIRECAFE_CF_CLICK_X", "544"))
HARDCODED_CF_CLICK_Y = int(os.getenv("HIRECAFE_CF_CLICK_Y", "334"))

SCROLL_PIXELS = int(os.getenv("HIRECAFE_SCROLL_PIXELS", "1200"))
MIN_SCROLL_DELAY_SECONDS = float(os.getenv("HIRECAFE_MIN_SCROLL_DELAY_SECONDS", "0.7"))
MAX_SCROLL_DELAY_SECONDS = float(os.getenv("HIRECAFE_MAX_SCROLL_DELAY_SECONDS", "1.8"))

MAX_RUNTIME_SECONDS = int(os.getenv("HIRECAFE_MAX_RUNTIME_SECONDS", "300"))
MAX_IDLE_SECONDS = int(os.getenv("HIRECAFE_MAX_IDLE_SECONDS", "90"))
MAX_SCROLLS = int(os.getenv("HIRECAFE_MAX_SCROLLS", "500"))
HEARTBEAT_EVERY_SECONDS = int(os.getenv("HIRECAFE_HEARTBEAT_EVERY_SECONDS", "15"))

CAROUSEL_CLICK_DELAY = float(os.getenv("HIRECAFE_CAROUSEL_CLICK_DELAY", "1.5"))
PHASE2_MAX_CAROUSEL_CLICKS = max(1, int(os.getenv("HIRECAFE_PHASE2_MAX_CAROUSEL_CLICKS", "40")))
BOTTOM_IDLE_SCROLLS = int(os.getenv("HIRECAFE_BOTTOM_IDLE_SCROLLS", "5"))
CAROUSEL_ENABLED = os.getenv("HIRECAFE_CAROUSEL_ENABLED", "true").lower() not in ("false", "0", "no")
PRE_SCROLL_ESCAPE = os.getenv("HIRECAFE_PRE_SCROLL_ESCAPE", "true").lower() not in ("false", "0", "no")

HIRECAFE_BROWSER_MODE = os.getenv("HIRECAFE_BROWSER_MODE", "stealth").strip().lower()
HIRECAFE_DETECTABLE_HEADLESS = os.getenv("HIRECAFE_DETECTABLE_HEADLESS", "true").lower() not in (
    "false", "0", "no",
)

# ---------------------------------------------------------------------------
#  Persistent browser profile
# ---------------------------------------------------------------------------
HIRECAFE_CHROME_PROFILE_DIR = os.getenv(
    "HIRECAFE_CHROME_PROFILE_DIR", "data/chrome_profile"
).strip()

# ---------------------------------------------------------------------------
#  TLS / header alignment — matches Debian bookworm Chromium 131
# ---------------------------------------------------------------------------
_CHROME_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_CHROME_SEC_CH_UA = '"Chromium";v="131", "Not_A Brand";v="24"'

# ---------------------------------------------------------------------------
#  Stealth JavaScript — injected via Page.addScriptToEvaluateOnNewDocument
#  Eliminates common headless fingerprints before any page loads.
# ---------------------------------------------------------------------------
_STEALTH_JS = """
// 1. Remove navigator.webdriver flag
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined,
    configurable: true
});

// 2. Realistic hardware concurrency & device memory
Object.defineProperty(navigator, 'hardwareConcurrency', {
    get: () => 8,
    configurable: true
});
Object.defineProperty(navigator, 'deviceMemory', {
    get: () => 8,
    configurable: true
});

// 3. Override navigator.languages
Object.defineProperty(navigator, 'languages', {
    get: () => ['en-US', 'en'],
    configurable: true
});

// 4. WebGL vendor/renderer spoofing
(function() {
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) return 'Google Inc. (NVIDIA)';
        if (parameter === 37446) return 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 SUPER, OpenGL 4.5)';
        return getParameter.call(this, parameter);
    };
    if (typeof WebGL2RenderingContext !== 'undefined') {
        const getParameter2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Google Inc. (NVIDIA)';
            if (parameter === 37446) return 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 SUPER, OpenGL 4.5)';
            return getParameter2.call(this, parameter);
        };
    }
})();

// 5. Fake plugins (headless Chrome returns empty)
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [
            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer',
              description: 'Portable Document Format',
              length: 1, item: (i) => ({ type: 'application/x-google-chrome-pdf' }) },
            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
              description: '', length: 1,
              item: (i) => ({ type: 'application/pdf' }) },
            { name: 'Native Client', filename: 'internal-nacl-plugin',
              description: '', length: 2,
              item: (i) => ({ type: i === 0 ? 'application/x-nacl' : 'application/x-pnacl' }) }
        ];
        arr.item = (i) => arr[i] || null;
        arr.namedItem = (n) => arr.find(p => p.name === n) || null;
        arr.refresh = () => {};
        return arr;
    },
    configurable: true
});

// 6. Fix Notification.permission
try {
    Object.defineProperty(Notification, 'permission', {
        get: () => 'default',
        configurable: true
    });
} catch(e) {}

// 7. Fix navigator.permissions.query
(function() {
    const origQuery = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = function(params) {
        if (params && params.name === 'notifications') {
            return Promise.resolve({ state: 'prompt', onchange: null });
        }
        return origQuery(params);
    };
})();

// 8. Mask automation-related properties
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

// 9. Chrome runtime (headless lacks this)
if (!window.chrome) {
    window.chrome = {};
}
if (!window.chrome.runtime) {
    window.chrome.runtime = {
        connect: function() {},
        sendMessage: function() {},
        id: undefined
    };
}
"""
HIRECAFE_CHROMEDRIVER_PATH = os.getenv("HIRECAFE_CHROMEDRIVER_PATH", "").strip()
HIRECAFE_CHROME_BINARY = os.getenv("HIRECAFE_CHROME_BINARY", "").strip()

HIRECAFE_OBSERVE_PAGES = os.getenv("HIRECAFE_OBSERVE_PAGES", "false").lower() not in (
    "false", "0", "no",
)
HIRECAFE_OBSERVE_DIR = os.getenv(
    "HIRECAFE_OBSERVE_DIR",
    "debug_screenshots/hirecafe_cloudflare",
).strip()

HIRECAFE_PAGE_READY_TIMEOUT_SECONDS = int(
    os.getenv("HIRECAFE_PAGE_READY_TIMEOUT_SECONDS", "45")
)
HIRECAFE_PAGE_READY_POLL_SECONDS = float(
    os.getenv("HIRECAFE_PAGE_READY_POLL_SECONDS", "1.0")
)

HIRECAFE_MAX_PAGES = max(1, int(os.getenv("HIRECAFE_MAX_PAGES", "20")))
HIRECAFE_PAGINATION_WAIT_SECONDS = max(1, int(os.getenv("HIRECAFE_PAGINATION_WAIT_SECONDS", "15")))
HIRECAFE_CARD_CLICK_PAUSE_SECONDS = float(os.getenv("HIRECAFE_CARD_CLICK_PAUSE_SECONDS", "0.45"))
HIRECAFE_PAGINATION_BOTTOM_SCROLL_STEPS = max(
    1,
    int(os.getenv("HIRECAFE_PAGINATION_BOTTOM_SCROLL_STEPS", "14")),
)
HIRECAFE_PAGINATION_BOTTOM_SCROLL_PAUSE_SECONDS = float(
    os.getenv("HIRECAFE_PAGINATION_BOTTOM_SCROLL_PAUSE_SECONDS", "0.35")
)

CLOUDFLARE_MARKERS = (
    "just a moment",
    "performing security verification",
    "verify you are human",
    "this website uses a security service",
    "ray id:",
    "checking your browser",
    "security check",
)

CLOUDFLARE_HTML_MARKERS = (
    "/cdn-cgi/challenge-platform/",
    "cf-turnstile-response",
    "cf-chl-widget",
    "challenge-success-text",
    "challenge-error-text",
)


def _probe_cloudflare_challenge(driver) -> dict[str, Any]:
    """Collect challenge signals from title/text plus known Cloudflare selectors."""
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
        title = (driver.title or "").strip()
        page = (driver.page_source or "")
        page_lower = page.lower()
        content = f"{title.lower()}\n{page_lower}"
        state["title"] = title
        state["marker_hits"] = [
            marker for marker in CLOUDFLARE_MARKERS if marker in content
        ]
        state["html_marker_hits"] = [
            marker for marker in CLOUDFLARE_HTML_MARKERS if marker in page_lower
        ]
    except Exception:
        pass

    try:
        selector_hits = driver.execute_script(
            "const bodyText=((document.body&&document.body.innerText)||'').toLowerCase();"
            "return {"
            "cf_iframe: !!document.querySelector(\"iframe[src*='challenges.cloudflare.com']\"),"
            "cf_turnstile: !!document.querySelector(\"input[name='cf-turnstile-response'], div.cf-turnstile\"),"
            "cf_turnstile_input: !!document.querySelector(\"input[name='cf-turnstile-response'], input[id^='cf-chl-widget'][id$='_response']\"),"
            "cf_turnstile_widget: !!document.querySelector(\"div.cf-turnstile\"),"
            "cf_challenge_form: !!document.querySelector(\"form#challenge-form, #challenge-stage\"),"
            "cf_challenge_platform_script: !!document.querySelector(\"script[src*='/cdn-cgi/challenge-platform/']\"),"
            "cf_turnstile_script: !!document.querySelector(\"script[src*='challenges.cloudflare.com/turnstile']\"),"
            "cf_challenge_state_nodes: !!document.querySelector(\"#challenge-success-text, #challenge-error-text, .loading-verifying, .ray-id\"),"
            "cf_meta_refresh: !!document.querySelector(\"meta[http-equiv='refresh'][content]\"),"
            "cf_ray_id_text: /ray id:/.test(bodyText),"
            "cf_verify_copy: /performing security verification|this website uses a security service|just a moment|security check|verify you are human|checking your browser/.test(bodyText),"
            "cf_challenge_text: /just a moment|security check|verify you are human|performing security verification|checking your browser/.test(bodyText)"
            "};"
        ) or {}
    except Exception:
        selector_hits = {}

    state["selector_hits"] = selector_hits

    strong_signal = any(
        bool(selector_hits.get(k))
        for k in (
            "cf_iframe",
            "cf_turnstile_input",
            "cf_challenge_form",
            "cf_challenge_platform_script",
            "cf_turnstile_script",
            "cf_challenge_state_nodes",
        )
    )

    weak_score = (
        len(state.get("marker_hits", []))
        + len(state.get("html_marker_hits", []))
        + int(bool(selector_hits.get("cf_verify_copy")))
        + int(bool(selector_hits.get("cf_ray_id_text")))
        + int(bool(selector_hits.get("cf_meta_refresh")))
    )

    state["strong_signal"] = strong_signal
    state["signal_score"] = (3 if strong_signal else 0) + weak_score
    state["active"] = bool(strong_signal or weak_score >= 2)
    return state


def _is_cloudflare_challenge_active(driver) -> bool:
    return bool(_probe_cloudflare_challenge(driver).get("active"))


def _wait_for_cloudflare_clearance(
    driver,
    timeout_seconds: int,
    poll_interval_seconds: float = 1.0,
) -> tuple[bool, dict[str, Any]]:
    deadline = time.time() + max(0, timeout_seconds)
    last_state = _probe_cloudflare_challenge(driver)
    while time.time() < deadline:
        last_state = _probe_cloudflare_challenge(driver)
        if not last_state.get("active"):
            return True, last_state
        time.sleep(max(0.1, poll_interval_seconds))
    last_state = _probe_cloudflare_challenge(driver)
    return (not last_state.get("active")), last_state


def _probe_hiring_cafe_page_ready(driver) -> dict[str, Any]:
    """Probe whether the post-challenge hiring.cafe app shell is ready."""
    challenge = _probe_cloudflare_challenge(driver)
    state: dict[str, Any] = {
        "ready": False,
        "reason": "unknown",
        "url": challenge.get("url", ""),
        "ready_state": None,
        "has_next_data": False,
        "has_next_root": False,
        "has_viewjob_link": False,
        "challenge": challenge,
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
    has_app_shell = bool(
        state["has_next_data"] or state["has_next_root"] or state["has_viewjob_link"]
    )
    ready_state_ok = state["ready_state"] in ("interactive", "complete")
    challenge_active = bool(challenge.get("active"))

    if challenge_active:
        state["reason"] = "challenge_active"
    elif not on_hiring_cafe:
        state["reason"] = "not_on_hiring_cafe"
    elif not ready_state_ok:
        state["reason"] = "dom_not_ready"
    elif not has_app_shell:
        state["reason"] = "app_shell_missing"
    else:
        state["reason"] = "ready"
        state["ready"] = True

    return state


def _wait_for_hiring_cafe_page_ready(
    driver,
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> tuple[bool, dict[str, Any]]:
    deadline = time.time() + max(0, timeout_seconds)
    last_state = _probe_hiring_cafe_page_ready(driver)
    while time.time() < deadline:
        last_state = _probe_hiring_cafe_page_ready(driver)
        if last_state.get("ready"):
            return True, last_state
        time.sleep(max(0.1, poll_interval_seconds))
    last_state = _probe_hiring_cafe_page_ready(driver)
    return bool(last_state.get("ready")), last_state


def _capture_observation_artifacts(
    driver,
    stage: str,
    extra: Optional[dict[str, Any]] = None,
) -> None:
    """Save screenshot + HTML (+ metadata) for Cloudflare challenge debugging."""
    if not HIRECAFE_OBSERVE_PAGES:
        return

    try:
        output_dir = Path(HIRECAFE_OBSERVE_DIR)
        if not output_dir.is_absolute():
            output_dir = Path.cwd() / output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        safe_stage = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stage)
        base = output_dir / f"{stamp}_{safe_stage}"

        screenshot_path = base.with_suffix(".png")
        html_path = base.with_suffix(".html")
        meta_path = base.with_suffix(".json")

        screenshot_ok = False
        try:
            screenshot_ok = bool(driver.save_screenshot(str(screenshot_path)))
        except Exception:
            screenshot_ok = False

        try:
            html_path.write_text(driver.page_source or "", encoding="utf-8")
        except Exception:
            pass

        metadata = {
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
            "screenshot_ok": screenshot_ok,
            "screenshot_path": str(screenshot_path),
            "html_path": str(html_path),
            "url": getattr(driver, "current_url", ""),
            "title": getattr(driver, "title", ""),
            "extra": extra or {},
        }
        meta_path.write_text(
            json.dumps(metadata, ensure_ascii=True, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        logger.info("hirecafe observe stage=%s wrote %s", stage, meta_path)
    except Exception as exc:
        logger.info("hirecafe observe capture failed stage=%s: %s", stage, type(exc).__name__)


def _launch_detectable_driver():
    """Launch a deliberately easier-to-detect browser profile for CF testing."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options as SeleniumChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService
    except Exception as exc:
        logger.warning(
            "hirecafe detectable mode unavailable (%s), falling back to stealth",
            type(exc).__name__,
        )
        return None

    options = SeleniumChromeOptions()
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    options.add_argument("--enable-automation")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1366,768")
    options.add_argument("--no-sandbox")

    if HIRECAFE_DETECTABLE_HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "HeadlessChrome/124.0.0.0 Safari/537.36"
        )

    if HIRECAFE_CHROME_BINARY:
        options.binary_location = HIRECAFE_CHROME_BINARY

    if HIRECAFE_CHROMEDRIVER_PATH:
        service = ChromeService(executable_path=HIRECAFE_CHROMEDRIVER_PATH)
    else:
        service = ChromeService()

    logger.info(
        "hirecafe launching detectable selenium browser headless=%s",
        HIRECAFE_DETECTABLE_HEADLESS,
    )
    return webdriver.Chrome(service=service, options=options)


def _cleanup_profile_locks(profile_dir: str) -> None:
    """Remove stale Chrome lock files that prevent profile reuse."""
    lock_names = ("SingletonLock", "SingletonCookie", "SingletonSocket")
    for name in lock_names:
        lock_path = os.path.join(profile_dir, name)
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
                logger.info("hirecafe removed stale lock: %s", lock_path)
        except Exception as exc:
            logger.debug("hirecafe lock cleanup failed %s: %s", name, exc)


def _inject_stealth_scripts(driver) -> None:
    """Inject anti-fingerprint JavaScript before any page loads."""
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": _STEALTH_JS},
        )
        logger.info("hirecafe stealth scripts injected")
    except Exception as exc:
        logger.warning("hirecafe stealth injection failed: %s", exc)

    # Align sec-ch-ua / sec-ch-ua-platform headers with the spoofed UA
    try:
        driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
            "headers": {
                "sec-ch-ua": _CHROME_SEC_CH_UA,
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Linux"',
            }
        })
    except Exception:
        pass


def _bezier_point(
    t: float,
    p0: tuple[float, float],
    p1: tuple[float, float],
    p2: tuple[float, float],
    p3: tuple[float, float],
) -> tuple[float, float]:
    """Evaluate a cubic Bézier curve at parameter *t* ∈ [0, 1]."""
    u = 1 - t
    x = u**3 * p0[0] + 3 * u**2 * t * p1[0] + 3 * u * t**2 * p2[0] + t**3 * p3[0]
    y = u**3 * p0[1] + 3 * u**2 * t * p1[1] + 3 * u * t**2 * p2[1] + t**3 * p3[1]
    return (x, y)


def _human_mouse_path(
    x0: float, y0: float, x1: float, y1: float,
) -> list[tuple[float, float]]:
    """Generate 20-40 waypoints along a cubic Bézier from (x0,y0) to (x1,y1)
    with Gaussian pixel jitter simulating hand tremor."""
    steps = random.randint(20, 40)
    dx = x1 - x0
    dy = y1 - y0
    # Two random control points — create an organic bow
    cp1 = (
        x0 + dx * random.uniform(0.2, 0.4) + random.uniform(-60, 60),
        y0 + dy * random.uniform(0.0, 0.3) + random.uniform(-40, 40),
    )
    cp2 = (
        x0 + dx * random.uniform(0.6, 0.8) + random.uniform(-60, 60),
        y0 + dy * random.uniform(0.7, 1.0) + random.uniform(-40, 40),
    )
    p0 = (x0, y0)
    p3 = (x1, y1)
    path: list[tuple[float, float]] = []
    for i in range(steps + 1):
        t = i / steps
        bx, by = _bezier_point(t, p0, cp1, cp2, p3)
        # Gaussian jitter — skip on first/last point to hit origin/target exactly
        if 0 < i < steps:
            bx += random.gauss(0, 1.5)
            by += random.gauss(0, 1.5)
        path.append((bx, by))
    return path


def _human_click(driver, x: int, y: int) -> bool:
    """Move the mouse along a Bézier curve to (x, y) with realistic timing,
    then perform a press-hold-release click sequence."""
    try:
        # Random origin offset (upper-left of target)
        ox = x + random.randint(-180, -40)
        oy = y + random.randint(-120, -25)
        path = _human_mouse_path(float(ox), float(oy), float(x), float(y))

        for wx, wy in path:
            driver.execute_cdp_cmd(
                "Input.dispatchMouseEvent",
                {"type": "mouseMoved", "x": int(wx), "y": int(wy)},
            )
            time.sleep(random.uniform(0.008, 0.025))

        # Pre-click hover pause
        time.sleep(random.uniform(0.08, 0.20))

        driver.execute_cdp_cmd(
            "Input.dispatchMouseEvent",
            {"type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1},
        )
        # Hold duration — simulate finger on trackpad
        time.sleep(random.uniform(0.05, 0.12))
        driver.execute_cdp_cmd(
            "Input.dispatchMouseEvent",
            {"type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1},
        )
        return True
    except Exception as exc:
        logger.info("hirecafe human click failed at (%s,%s): %s", x, y, type(exc).__name__)
        return False


def _launch_hirecafe_driver():
    options = uc.ChromeOptions()
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    if HIRECAFE_BROWSER_MODE == "detectable":
        detectable_driver = _launch_detectable_driver()
        if detectable_driver is not None:
            return detectable_driver

    # --- Chrome hardening flags ---
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-agent={_CHROME_UA}")

    # --- Persistent browser profile ---
    profile_dir = HIRECAFE_CHROME_PROFILE_DIR
    if profile_dir:
        profile_path = Path(profile_dir)
        if not profile_path.is_absolute():
            profile_path = Path.cwd() / profile_path
        profile_path.mkdir(parents=True, exist_ok=True)
        _cleanup_profile_locks(str(profile_path))
        options.add_argument(f"--user-data-dir={profile_path}")
        logger.info("hirecafe using persistent profile: %s", profile_path)

    is_server = os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("PORT")
    if is_server:
        logger.info("hirecafe detected server environment, using system chromium binaries")
        driver = uc.Chrome(
            options=options,
            browser_executable_path="/usr/bin/chromium",
            driver_executable_path="/usr/bin/chromedriver",
        )
    else:
        driver = uc.Chrome(options=options, version_main=147)

    _inject_stealth_scripts(driver)
    return driver


def _press_escape_before_scroll(driver) -> None:
    """Dismiss overlays / blur focused inputs so the feed receives scroll events."""
    if not PRE_SCROLL_ESCAPE:
        return
    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys(Keys.ESCAPE)
        time.sleep(0.15)
    except Exception as exc:
        logger.debug("hirecafe pre-scroll Escape: %s", type(exc).__name__)


def _click_viewport_coordinate(driver, x: int, y: int) -> bool:
    """Click at viewport coordinates using human-like Bézier mouse movement."""
    return _human_click(driver, x, y)


def _extract_viewjob_id(url_or_href: str) -> str | None:
    """Extract the job slug/ID from a viewjob URL or href path."""
    if "viewjob/" not in url_or_href:
        return None
    slug = url_or_href.split("viewjob/")[-1]
    slug = slug.split("?")[0].split("#")[0]
    if slug.endswith(".json"):
        slug = slug[:-5]
    return slug.strip("/") or None


def _ingest_from_performance_logs(
    driver,
    job_samples: list[dict[str, Any]],
    seen_urls: set[str],
    seen_ids: set[str],
    max_samples: int,
) -> int:
    """Read CDP performance logs and append new viewjob JSON payloads.
    Returns count of newly captured jobs."""
    new_count = 0
    try:
        logs = driver.get_log("performance")
    except Exception:
        return 0
    for log_entry in logs:
        if len(job_samples) >= max_samples:
            break
        try:
            message = json.loads(log_entry["message"])["message"]
            if message["method"] != "Network.responseReceived":
                continue
            resp = message["params"]["response"]
            url = resp.get("url", "")
            if "viewjob/" not in url or ".json" not in url:
                continue
            if url in seen_urls:
                continue
            if resp.get("status") != 200:
                continue
            job_id = _extract_viewjob_id(url)
            if job_id and job_id in seen_ids:
                continue
            req_id = message["params"]["requestId"]
            body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
            job_data = json.loads(body["body"])
            job_samples.append(job_data)
            seen_urls.add(url)
            if job_id:
                seen_ids.add(job_id)
            new_count += 1
            logger.debug(
                "hirecafe captured %s/%s url=%s",
                len(job_samples), max_samples, url.split("/")[-1][:30],
            )
        except Exception:
            pass
    return new_count


def _scroll_feed_and_window(driver, scroll_root, pixels: int) -> None:
    """Scroll both the infinite-scroll inner container and the browser window."""
    try:
        driver.execute_script(
            "var el=arguments[0],px=arguments[1];"
            "if(el)el.scrollTop+=px;"
            "window.scrollBy(0,px);",
            scroll_root, pixels,
        )
    except Exception:
        try:
            driver.execute_script("window.scrollBy(0,%d);" % pixels)
        except Exception:
            pass


def _is_at_bottom(driver, scroll_root) -> bool:
    """Check if the scroll container (or window) has reached the bottom."""
    try:
        return bool(driver.execute_script(
            "var el=arguments[0];"
            "if(el)return el.scrollTop+el.clientHeight>=el.scrollHeight-50;"
            "return (window.innerHeight+window.scrollY)>=document.body.scrollHeight-50;",
            scroll_root,
        ))
    except Exception:
        return False


def _fetch_missing_jobs_via_dom(
    driver,
    card,
    job_samples: list[dict[str, Any]],
    seen_ids: set[str],
    seen_urls: set[str],
    max_samples: int,
) -> int:
    """Fetch viewjob JSON for hrefs visible in a card's DOM but not yet captured.
    Uses in-page fetch() as a fallback for jobs missed by CDP performance logs."""
    new_count = 0
    try:
        hrefs = _viewjob_hrefs_in_card(driver, card)
    except Exception:
        return 0

    missing_ids = []
    for href in hrefs:
        jid = _extract_viewjob_id(href)
        if jid and jid not in seen_ids:
            missing_ids.append(jid)

    if not missing_ids:
        return 0

    try:
        build_id = driver.execute_script(
            "try{return JSON.parse(document.getElementById('__NEXT_DATA__').textContent).buildId}"
            "catch(e){return null}"
        )
    except Exception:
        build_id = None

    if not build_id:
        logger.debug("hirecafe dom-fetch: could not determine Next.js buildId, skipping")
        return 0

    for jid in missing_ids:
        if len(job_samples) >= max_samples:
            break
        url = f"/_next/data/{build_id}/viewjob/{jid}.json"
        try:
            result = driver.execute_async_script(
                "var url=arguments[0],done=arguments[arguments.length-1];"
                "fetch(url,{credentials:'include'})"
                ".then(function(r){return r.ok?r.json():null})"
                ".then(function(d){done(d?JSON.stringify(d):null)})"
                ".catch(function(){done(null)});",
                url,
            )
            if not result:
                continue
            job_data = json.loads(result)
            if not job_data.get("pageProps"):
                continue
            job_samples.append(job_data)
            seen_ids.add(jid)
            canonical = f"https://hiring.cafe{url}"
            seen_urls.add(canonical)
            new_count += 1
            logger.debug(
                "hirecafe dom-fetch captured %s/%s id=%s",
                len(job_samples), max_samples, jid[:30],
            )
        except Exception:
            pass
    return new_count


def _collect_jobs_for_card_until_no_new(
    driver,
    card,
    job_samples: list[dict[str, Any]],
    seen_ids: set[str],
    seen_urls: set[str],
    *,
    max_samples: int,
    click_pause_seconds: float,
    max_carousel_clicks: int | None = None,
) -> dict[str, Any]:
    """Capture card jobs from current state, then click carousel until no new job appears.

    Strategy:
    1) Store currently visible job info for the card.
    2) Click carousel next.
    3) Stop when a click yields no new captured jobs and no newly visible ``viewjob`` href.
    """
    before_card = len(job_samples)

    initial_logs = _ingest_from_performance_logs(
        driver,
        job_samples,
        seen_urls,
        seen_ids,
        max_samples,
    )
    initial_dom = _fetch_missing_jobs_via_dom(
        driver,
        card,
        job_samples,
        seen_ids,
        seen_urls,
        max_samples,
    )

    try:
        seen_card_hrefs = set(_viewjob_hrefs_in_card(driver, card))
    except Exception:
        seen_card_hrefs = set()

    click_count = 0
    stop_reason = ""

    while len(job_samples) < max_samples:
        if max_carousel_clicks is not None and click_count >= max_carousel_clicks:
            stop_reason = "max_carousel_clicks_reached"
            break

        if not _click_carousel_next(driver, card):
            stop_reason = "next_button_not_clickable"
            break

        click_count += 1
        time.sleep(max(0.0, click_pause_seconds))

        before_click = len(job_samples)
        _ingest_from_performance_logs(
            driver,
            job_samples,
            seen_urls,
            seen_ids,
            max_samples,
        )
        _fetch_missing_jobs_via_dom(
            driver,
            card,
            job_samples,
            seen_ids,
            seen_urls,
            max_samples,
        )

        try:
            current_hrefs = set(_viewjob_hrefs_in_card(driver, card))
        except Exception:
            current_hrefs = set()

        newly_visible_hrefs = current_hrefs - seen_card_hrefs
        seen_card_hrefs.update(current_hrefs)
        new_jobs_this_click = len(job_samples) - before_click

        if new_jobs_this_click <= 0 and not newly_visible_hrefs:
            stop_reason = "no_new_job_after_click"
            break

    if not stop_reason:
        if len(job_samples) >= max_samples:
            stop_reason = "max_samples_reached"
        else:
            stop_reason = "completed"

    return {
        "new_jobs": len(job_samples) - before_card,
        "clicks": click_count,
        "visible_hrefs": len(seen_card_hrefs),
        "initial_new_jobs": initial_logs + initial_dom,
        "stop_reason": stop_reason,
    }


def _find_card_grid_root(driver: Any) -> tuple[Any | None, list[Any], dict[str, Any]]:
    candidates = driver.find_elements(By.CSS_SELECTOR, "div[class*='grid-cols-1']")
    best_root = None
    best_cards: list[Any] = []
    best_score = -1
    evaluated = 0

    for node in candidates:
        try:
            cls = (node.get_attribute("class") or "").strip()
            if "grid" not in cls:
                continue

            direct_cards = node.find_elements(
                By.XPATH,
                "./div[.//a[contains(@href,'/viewjob/')]]",
            )
            if len(direct_cards) < 2:
                continue

            total_links = len(node.find_elements(By.CSS_SELECTOR, "a[href*='/viewjob/']"))
            score = len(direct_cards) * 10 + total_links
            evaluated += 1
            if score > best_score:
                best_score = score
                best_root = node
                best_cards = direct_cards
        except Exception:
            continue

    if not best_root:
        return None, [], {
            "candidate_count": len(candidates),
            "evaluated_candidates": evaluated,
            "selected_score": None,
        }

    try:
        best_cards = _dedupe_card_elements(driver, best_cards)
    except Exception:
        pass

    return best_root, best_cards, {
        "candidate_count": len(candidates),
        "evaluated_candidates": evaluated,
        "selected_score": best_score,
        "selected_card_count": len(best_cards),
    }


def _scroll_to_bottom(driver: Any) -> None:
    last_height = -1
    stable_count = 0

    for _ in range(HIRECAFE_PAGINATION_BOTTOM_SCROLL_STEPS):
        try:
            height = int(driver.execute_script(
                "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);"
            ) or 0)
        except Exception:
            break

        try:
            driver.execute_script("window.scrollTo(0, arguments[0]);", max(0, height))
        except Exception:
            break

        time.sleep(max(0.05, HIRECAFE_PAGINATION_BOTTOM_SCROLL_PAUSE_SECONDS))

        if height == last_height:
            stable_count += 1
            if stable_count >= 2:
                break
        else:
            stable_count = 0
        last_height = height


def _inspect_pagination_component(driver: Any) -> dict[str, Any]:
    try:
        data = driver.execute_script(
            "const norm=(s)=>String(s||'').replace(/\\s+/g,' ').trim();"
            "const docH=Math.max(document.body.scrollHeight||0,document.documentElement.scrollHeight||0);"
            "const nodes=[...document.querySelectorAll('nav, footer, div')];"
            "const cands=[];"
            "for(const el of nodes){"
            "  const text=norm(el.innerText);"
            "  if(!text) continue;"
            "  const controls=[...el.querySelectorAll('button, a, [role=\"button\"]')];"
            "  if(controls.length<1) continue;"
            "  const mapped=controls.map((b,i)=>({"
            "    index:i,"
            "    text:norm(b.innerText),"
            "    aria:norm(b.getAttribute('aria-label')),"
            "    title:norm(b.getAttribute('title')),"
            "    className:norm(b.className),"
            "    tag:(b.tagName||'').toLowerCase(),"
            "    ariaCurrent:norm(b.getAttribute('aria-current')).toLowerCase(),"
            "    ariaDisabled:norm(b.getAttribute('aria-disabled')).toLowerCase(),"
            "    disabled:!!b.disabled||norm(b.getAttribute('aria-disabled')).toLowerCase()==='true'"
            "  }));"
            "  const numericButtons=mapped.filter(b=>/^\\d+$/.test(b.text)).length;"
            "  const hasPage=/page\\s*\\d+\\s*of\\s*\\d+/i.test(text);"
            "  const hasNext=mapped.some(b=>{const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();return (meta.includes('next')||meta.includes('→')||meta.includes('›')||meta.includes('»'))&&!meta.includes('dark mode');});"
            "  const hasPrev=mapped.some(b=>{const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();return (meta.includes('prev')||meta.includes('previous')||meta.includes('←')||meta.includes('‹')||meta.includes('«'));});"
            "  const filterChipCopy=/departments|salary|commitment|experience|job titles|benefits|encouraged to apply/i.test(text);"
            "  const cls=String(el.className||'');"
            "  const likely=/border-t|bg-gray-50|pagination|justify-center|bottom/i.test(cls);"
            "  const rect=el.getBoundingClientRect();"
            "  const absTop=rect.top+window.scrollY;"
            "  const nearBottom=absTop>(docH*0.45);"
            "  if(filterChipCopy && !hasPage && numericButtons===0) continue;"
            "  if(!(hasPage || numericButtons>=2 || (hasNext && numericButtons>=1))) continue;"
            "  if(!nearBottom && !hasPage && numericButtons<3) continue;"
            "  const score=(hasPage?120:0)+(nearBottom?40:0)+(numericButtons*8)+(hasNext?25:0)+(hasPrev?10:0)+(likely?10:0)-(filterChipCopy?35:0);"
            "  cands.push({el:el,text:text,cls:cls,controls:mapped,hasPage:hasPage,score:score,absTop:absTop});"
            "}"
            "if(!cands.length)return {found:false};"
            "cands.sort((a,b)=>(b.score-a.score)||(b.absTop-a.absTop));"
            "const top=cands[0];"
            "const root=top.el;"
            "const controls=[...root.querySelectorAll('button, a, [role=\"button\"]')];"
            "const buttons=controls.map((b,i)=>({"
            "  index:i,text:norm(b.innerText),aria:norm(b.getAttribute('aria-label')),title:norm(b.getAttribute('title')),"
            "  className:norm(b.className),tag:(b.tagName||'').toLowerCase(),"
            "  ariaCurrent:norm(b.getAttribute('aria-current')).toLowerCase(),"
            "  ariaDisabled:norm(b.getAttribute('aria-disabled')).toLowerCase(),"
            "  disabled:!!b.disabled||norm(b.getAttribute('aria-disabled')).toLowerCase()==='true'"
            "}));"
            "const m=/page\\s*(\\d+)\\s*of\\s*(\\d+)/i.exec(top.text);"
            "let currentPage=m?parseInt(m[1],10):null;"
            "if(currentPage===null){"
            "  const activeNumeric=buttons.find(b=>/^\\d+$/.test(b.text)&&(b.ariaCurrent==='page'||/active|selected|current/i.test(b.className)));"
            "  if(activeNumeric) currentPage=parseInt(activeNumeric.text,10);"
            "}"
            "let totalPages=m?parseInt(m[2],10):null;"
            "if(totalPages===null){"
            "  const nums=buttons.filter(b=>/^\\d+$/.test(b.text)).map(b=>parseInt(b.text,10)).filter(n=>Number.isFinite(n));"
            "  if(nums.length) totalPages=Math.max(...nums);"
            "}"
            "let nextIndex=-1;"
            "for(const b of buttons){"
            "  const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();"
            "  if((meta.includes('next')||meta.includes('→')||meta.includes('›')||meta.includes('»'))&&!meta.includes('dark mode')){"
            "    nextIndex=b.index;break;"
            "  }"
            "}"
            "if(nextIndex===-1&&currentPage!==null){"
            "  const candidates=buttons.filter(b=>!b.disabled&&/^\\d+$/.test(b.text)).map(b=>({i:b.index,n:parseInt(b.text,10)})).filter(x=>x.n>currentPage);"
            "  if(candidates.length){candidates.sort((a,b)=>a.n-b.n);nextIndex=candidates[0].i;}"
            "}"
            "if(nextIndex===-1){"
            "  const nonNum=buttons.filter(b=>!b.disabled && !/^\\d+$/.test(b.text) && !((b.text+' '+b.aria+' '+b.title).toLowerCase().includes('dark mode')));"
            "  if(nonNum.length)nextIndex=nonNum[nonNum.length-1].index;"
            "}"
            "if(nextIndex===-1){"
            "  const enabled=buttons.filter(b=>!b.disabled && !((b.text+' '+b.aria+' '+b.title).toLowerCase().includes('dark mode')));"
            "  if(enabled.length>=2)nextIndex=enabled[enabled.length-1].index;"
            "}"
            "return {"
            "  found:true,"
            "  text:top.text,"
            "  className:top.cls,"
            "  selectedScore:top.score,"
            "  selectedAbsTop:top.absTop,"
            "  buttonCount:buttons.length,"
            "  buttons:buttons,"
            "  currentPage:currentPage,"
            "  totalPages:totalPages,"
            "  nextButtonIndex:nextIndex"
            "};"
        )
        return data or {"found": False}
    except Exception as exc:
        return {"found": False, "error": type(exc).__name__}


def _click_next_pagination(driver: Any) -> dict[str, Any]:
    try:
        result = driver.execute_script(
            "const norm=(s)=>String(s||'').replace(/\\s+/g,' ').trim();"
            "const docH=Math.max(document.body.scrollHeight||0,document.documentElement.scrollHeight||0);"
            "const nodes=[...document.querySelectorAll('nav, footer, div')];"
            "const cands=[];"
            "for(const el of nodes){"
            "  const text=norm(el.innerText);"
            "  if(!text) continue;"
            "  const controls=[...el.querySelectorAll('button, a, [role=\"button\"]')];"
            "  if(controls.length<1) continue;"
            "  const mapped=controls.map((b,i)=>({"
            "    i:i,text:norm(b.innerText),aria:norm(b.getAttribute('aria-label')),title:norm(b.getAttribute('title')),"
            "    className:norm(b.className),ariaCurrent:norm(b.getAttribute('aria-current')).toLowerCase(),"
            "    disabled:!!b.disabled||norm(b.getAttribute('aria-disabled')).toLowerCase()==='true'"
            "  }));"
            "  const numericButtons=mapped.filter(b=>/^\\d+$/.test(b.text)).length;"
            "  const hasPage=/page\\s*\\d+\\s*of\\s*\\d+/i.test(text);"
            "  const hasNext=mapped.some(b=>{const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();return (meta.includes('next')||meta.includes('→')||meta.includes('›')||meta.includes('»'))&&!meta.includes('dark mode');});"
            "  const hasPrev=mapped.some(b=>{const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();return (meta.includes('prev')||meta.includes('previous')||meta.includes('←')||meta.includes('‹')||meta.includes('«'));});"
            "  const filterChipCopy=/departments|salary|commitment|experience|job titles|benefits|encouraged to apply/i.test(text);"
            "  const cls=String(el.className||'');"
            "  const likely=/border-t|bg-gray-50|pagination|justify-center|bottom/i.test(cls);"
            "  const rect=el.getBoundingClientRect();"
            "  const absTop=rect.top+window.scrollY;"
            "  const nearBottom=absTop>(docH*0.45);"
            "  if(filterChipCopy && !hasPage && numericButtons===0) continue;"
            "  if(!(hasPage || numericButtons>=2 || (hasNext && numericButtons>=1))) continue;"
            "  if(!nearBottom && !hasPage && numericButtons<3) continue;"
            "  const score=(hasPage?120:0)+(nearBottom?40:0)+(numericButtons*8)+(hasNext?25:0)+(hasPrev?10:0)+(likely?10:0)-(filterChipCopy?35:0);"
            "  cands.push({el:el,text:text,score:score,absTop:absTop});"
            "}"
            "if(!cands.length)return {clicked:false,reason:'pagination_not_found'};"
            "cands.sort((a,b)=>(b.score-a.score)||(b.absTop-a.absTop));"
            "const root=cands[0].el;"
            "const controls=[...root.querySelectorAll('button, a, [role=\"button\"]')];"
            "if(!controls.length)return {clicked:false,reason:'no_buttons'};"
            "const mapped=controls.map((b,i)=>({"
            "  i:i,text:norm(b.innerText),aria:norm(b.getAttribute('aria-label')),title:norm(b.getAttribute('title')),"
            "  className:norm(b.className),ariaCurrent:norm(b.getAttribute('aria-current')).toLowerCase(),"
            "  disabled:!!b.disabled||norm(b.getAttribute('aria-disabled')).toLowerCase()==='true'"
            "}));"
            "let currentPage=null;"
            "const activeNumeric=mapped.find(b=>/^\\d+$/.test(b.text)&&(b.ariaCurrent==='page'||/active|selected|current/i.test(b.className)));"
            "if(activeNumeric) currentPage=parseInt(activeNumeric.text,10);"
            "let idx=-1;"
            "for(const b of mapped){"
            "  const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();"
            "  if((meta.includes('next')||meta.includes('→')||meta.includes('›')||meta.includes('»'))&&!meta.includes('dark mode')){idx=b.i;break;}"
            "}"
            "if(idx===-1&&currentPage!==null){"
            "  const candidates=mapped.filter(b=>!b.disabled&&/^\\d+$/.test(b.text)).map(b=>({i:b.i,n:parseInt(b.text,10)})).filter(x=>x.n>currentPage);"
            "  if(candidates.length){candidates.sort((a,b)=>a.n-b.n);idx=candidates[0].i;}"
            "}"
            "if(idx===-1){"
            "  const nonNum=mapped.filter(b=>!b.disabled && !/^\\d+$/.test(b.text) && !((b.text+' '+b.aria+' '+b.title).toLowerCase().includes('dark mode')));"
            "  if(nonNum.length)idx=nonNum[nonNum.length-1].i;"
            "}"
            "if(idx===-1){"
            "  const enabled=mapped.filter(b=>!b.disabled && !((b.text+' '+b.aria+' '+b.title).toLowerCase().includes('dark mode')));"
            "  if(enabled.length>=2)idx=enabled[enabled.length-1].i;"
            "}"
            "if(idx===-1)return {clicked:false,reason:'next_button_not_resolved'};"
            "const btn=controls[idx];"
            "const isDisabled=!!btn.disabled||btn.getAttribute('aria-disabled')==='true';"
            "if(isDisabled)return {clicked:false,reason:'next_button_disabled',index:idx};"
            "try{btn.scrollIntoView({behavior:'auto',block:'center'});}catch(e){}"
            "btn.click();"
            "return {clicked:true,index:idx};"
        )
        return result or {"clicked": False, "reason": "unknown"}
    except Exception as exc:
        return {"clicked": False, "reason": type(exc).__name__}


def _page_signature(driver: Any, cards: list[Any]) -> str:
    parts: list[str] = []
    for card in cards[:8]:
        try:
            hrefs = sorted(_viewjob_hrefs_in_card(driver, card))
            if hrefs:
                parts.append(hrefs[0])
        except Exception:
            continue
    if parts:
        return "|".join(parts)

    try:
        pag = _inspect_pagination_component(driver)
        txt = str(pag.get("text", ""))
        return txt.strip()
    except Exception:
        return ""


def _wait_for_page_change(
    driver: Any,
    old_signature: str,
    timeout_seconds: int,
) -> tuple[bool, str]:
    deadline = time.time() + max(1, timeout_seconds)
    latest = old_signature
    while time.time() < deadline:
        root, cards, _ = _find_card_grid_root(driver)
        if root:
            latest = _page_signature(driver, cards)
            if latest and latest != old_signature:
                return True, latest
        time.sleep(0.6)
    return False, latest


def _click_card_surface(driver: Any, card: Any) -> bool:
    try:
        clicked = driver.execute_script(
            "const card=arguments[0];"
            "const btn=card.querySelector('button:not([disabled])');"
            "if(btn){btn.click();return true;}"
            "card.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));"
            "return true;",
            card,
        )
        return bool(clicked)
    except Exception:
        try:
            card.click()
            return True
        except Exception:
            return False


def _process_cards_on_page(
    driver: Any,
    cards: list[Any],
    job_samples: list[dict[str, Any]],
    seen_urls: set[str],
    seen_ids: set[str],
    *,
    max_samples: int,
) -> dict[str, int]:
    processed = 0
    page_new_jobs = 0

    for card in cards:
        if len(job_samples) >= max_samples:
            break

        try:
            if not card.is_displayed():
                continue
        except Exception:
            continue

        processed += 1

        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior:'auto',block:'center'});",
                card,
            )
            time.sleep(0.2)
        except Exception:
            pass

        _click_card_surface(driver, card)
        time.sleep(max(0.0, HIRECAFE_CARD_CLICK_PAUSE_SECONDS))

        card_result = _collect_jobs_for_card_until_no_new(
            driver,
            card,
            job_samples,
            seen_ids,
            seen_urls,
            max_samples=max_samples,
            click_pause_seconds=CAROUSEL_CLICK_DELAY,
            max_carousel_clicks=PHASE2_MAX_CAROUSEL_CLICKS,
        )
        page_new_jobs += int(card_result.get("new_jobs", 0))

    return {
        "cards_seen": len(cards),
        "cards_processed": processed,
        "new_jobs": page_new_jobs,
    }


def _scrape_paginated_card_pages(
    driver: Any,
    job_samples: list[dict[str, Any]],
    seen_urls: set[str],
    seen_ids: set[str],
    *,
    max_samples: int,
    max_pages: int,
) -> int:
    pages_visited = 0
    visited_signatures: set[str] = set()

    for loop_idx in range(1, max_pages + 1):
        if len(job_samples) >= max_samples:
            logger.info("hirecafe stop: max_samples reached")
            break

        root, cards, grid_meta = _find_card_grid_root(driver)
        pagination_meta = _inspect_pagination_component(driver)

        if not root or not cards:
            logger.warning(
                "hirecafe stop: card grid not found loop=%s candidates=%s pagination_found=%s",
                loop_idx,
                grid_meta.get("candidate_count"),
                pagination_meta.get("found"),
            )
            break

        signature = _page_signature(driver, cards)
        if signature and signature in visited_signatures:
            logger.info("hirecafe stop: repeated page signature")
            break
        if signature:
            visited_signatures.add(signature)

        page_no = int(pagination_meta.get("currentPage") or loop_idx)
        total_pages = pagination_meta.get("totalPages")

        before_page = len(job_samples)
        card_metrics = _process_cards_on_page(
            driver,
            cards,
            job_samples,
            seen_urls,
            seen_ids,
            max_samples=max_samples,
        )
        after_page = len(job_samples)

        pages_visited += 1
        logger.info(
            "hirecafe page=%s/%s cards=%s new_jobs=%s total=%s",
            page_no,
            total_pages if total_pages is not None else "?",
            card_metrics.get("cards_processed"),
            after_page - before_page,
            after_page,
        )

        if len(job_samples) >= max_samples:
            break

        if total_pages is not None and page_no >= int(total_pages):
            logger.info("hirecafe stop: last page reached")
            break

        old_signature = signature
        _scroll_to_bottom(driver)
        click_next = _click_next_pagination(driver)
        if not click_next.get("clicked"):
            logger.info(
                "hirecafe stop: next-page click failed reason=%s",
                click_next.get("reason"),
            )
            break

        changed, new_signature = _wait_for_page_change(
            driver,
            old_signature=old_signature,
            timeout_seconds=HIRECAFE_PAGINATION_WAIT_SECONDS,
        )
        if not changed:
            logger.info(
                "hirecafe stop: page did not change after next click signature=%s",
                new_signature,
            )
            break

        time.sleep(0.8)

    return pages_visited


def _navigate_to_hirecafe_ready_page(driver: Any, target_url: str) -> None:
    logger.info("hirecafe navigating to url=%s", target_url)
    driver.get(target_url)

    logger.info("hirecafe waiting %ss for initial load and Cloudflare check", CLOUDFLARE_WAIT_SECONDS)
    time.sleep(max(0, CLOUDFLARE_WAIT_SECONDS))

    initial_challenge = _probe_cloudflare_challenge(driver)
    logger.info(
        "hirecafe initial challenge probe active=%s markers=%s selectors=%s url=%s",
        initial_challenge.get("active"),
        initial_challenge.get("marker_hits"),
        initial_challenge.get("selector_hits"),
        initial_challenge.get("url"),
    )
    _capture_observation_artifacts(
        driver,
        "initial_landing",
        {"challenge": initial_challenge, "target_url": target_url},
    )

    if initial_challenge.get("active"):
        logger.info("hirecafe detected active Cloudflare challenge")
        _capture_observation_artifacts(
            driver,
            "challenge_detected",
            {"challenge": initial_challenge},
        )
        clicked = False

        try:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframes:
                src = iframe.get_attribute("src")
                if src and "cloudflare" in src.lower():
                    logger.info("Found Cloudflare iframe, attempting checkbox click")
                    driver.switch_to.frame(iframe)
                    checkbox = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='checkbox'], .ctp-checkbox-container, .mark"))
                    )
                    checkbox.click()
                    clicked = True
                    logger.info("Cloudflare checkbox click sent")
                    break
        except Exception as exc:
            logger.info("Cloudflare iframe click unavailable: %s", type(exc).__name__)
        finally:
            try:
                driver.switch_to.default_content()
            except Exception:
                pass

        if not clicked:
            clicked = _click_viewport_coordinate(driver, HARDCODED_CF_CLICK_X, HARDCODED_CF_CLICK_Y)
            if clicked:
                logger.info(
                    "hirecafe sent fallback coordinate click x=%s y=%s",
                    HARDCODED_CF_CLICK_X,
                    HARDCODED_CF_CLICK_Y,
                )

        _capture_observation_artifacts(
            driver,
            "challenge_after_click_attempt",
            {"clicked": clicked},
        )

        cleared, clear_state = _wait_for_cloudflare_clearance(
            driver,
            CLOUDFLARE_CLEAR_TIMEOUT_SECONDS,
        )
        if cleared:
            logger.info(
                "hirecafe cloudflare cleared markers=%s selectors=%s",
                clear_state.get("marker_hits"),
                clear_state.get("selector_hits"),
            )
            _capture_observation_artifacts(
                driver,
                "challenge_cleared",
                {"challenge": clear_state},
            )
        else:
            logger.warning(
                "hirecafe cloudflare still active after %ss markers=%s selectors=%s",
                CLOUDFLARE_CLEAR_TIMEOUT_SECONDS,
                clear_state.get("marker_hits"),
                clear_state.get("selector_hits"),
            )
            _capture_observation_artifacts(
                driver,
                "challenge_uncleared",
                {"challenge": clear_state},
            )
    else:
        logger.info("hirecafe cloudflare challenge not detected")

    page_ready, page_state = _wait_for_hiring_cafe_page_ready(
        driver,
        HIRECAFE_PAGE_READY_TIMEOUT_SECONDS,
        HIRECAFE_PAGE_READY_POLL_SECONDS,
    )
    if page_ready:
        logger.info(
            "hirecafe page readiness verified reason=%s ready_state=%s "
            "next_data=%s next_root=%s viewjob_link=%s",
            page_state.get("reason"),
            page_state.get("ready_state"),
            page_state.get("has_next_data"),
            page_state.get("has_next_root"),
            page_state.get("has_viewjob_link"),
        )
        _capture_observation_artifacts(
            driver,
            "post_challenge_page_ready",
            {"page_state": page_state},
        )
        if POST_VERIFY_WAIT_SECONDS > 0:
            logger.info(
                "hirecafe post-ready wait %ss before scraping",
                POST_VERIFY_WAIT_SECONDS,
            )
            time.sleep(max(0, POST_VERIFY_WAIT_SECONDS))
    else:
        logger.warning(
            "hirecafe page readiness not verified within %ss reason=%s",
            HIRECAFE_PAGE_READY_TIMEOUT_SECONDS,
            page_state.get("reason"),
        )
        _capture_observation_artifacts(
            driver,
            "post_challenge_page_not_ready",
            {"page_state": page_state},
        )


def scrape_hirecafe_jobs(
    max_samples: int = 200,
    search_url: str | None = None,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    """
    Launch Chrome, navigate to a HireCafe search URL, then scrape jobs from the
    paginated card list by visiting pages and expanding card carousels.

    Returns a list of raw response dicts (each has ``pageProps.job``).
    """
    target_url = (search_url or "").strip() or HIRECAFE_SEARCH_URL
    page_limit = max_pages if max_pages is not None else HIRECAFE_MAX_PAGES
    page_limit = max(1, int(page_limit))

    logger.info(
        "hirecafe launching browser mode=%s max_samples=%s max_pages=%s url=%s",
        HIRECAFE_BROWSER_MODE,
        max_samples,
        page_limit,
        target_url,
    )
    driver = _launch_hirecafe_driver()

    try:
        _navigate_to_hirecafe_ready_page(driver, target_url)

        job_samples: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        seen_ids: set[str] = set()

        pages_visited = _scrape_paginated_card_pages(
            driver,
            job_samples,
            seen_urls,
            seen_ids,
            max_samples=max_samples,
            max_pages=page_limit,
        )

        logger.info(
            "hirecafe scrape complete: pages_visited=%s total=%s jobs",
            pages_visited,
            len(job_samples),
        )
        return job_samples
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _strip_html(value: str) -> str:
    """Unescape HTML entities and strip tags, returning plain text."""
    decoded = html.unescape(value)
    if "<" in decoded and ">" in decoded:
        return BeautifulSoup(decoded, "html.parser").get_text(separator="\n", strip=True)
    return decoded


def _strip_html_recursively(data: Any) -> Any:
    """Walk dicts/lists and strip HTML from every string leaf."""
    if isinstance(data, str):
        return _strip_html(data)
    if isinstance(data, dict):
        return {k: _strip_html_recursively(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_strip_html_recursively(item) for item in data]
    return data


def _build_salary(v5: dict[str, Any]) -> Optional[str]:
    """Best-effort salary string from v5_processed_job_data compensation fields."""
    for period in ("yearly", "monthly", "hourly", "weekly", "bi-weekly"):
        lo = v5.get(f"{period}_min_compensation")
        hi = v5.get(f"{period}_max_compensation")
        if lo is not None or hi is not None:
            parts = []
            if lo is not None:
                parts.append(str(lo))
            if hi is not None:
                parts.append(str(hi))
            return f"{' - '.join(parts)} ({period})"
    return None


def normalize_hirecafe_item(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a single hiring.cafe network payload into the flat dict shape
    that ``_normalize_job()`` in the pipeline expects.

    Mirrors the pattern of ``normalize_naukri_item`` in apify_naukri.py.
    """
    job_raw = raw.get("pageProps", {}).get("job", {})
    job = _strip_html_recursively(job_raw)

    job_info = job.get("job_information") or {}
    v5 = job.get("v5_processed_job_data") or {}
    company_data = job.get("enriched_company_data") or {}

    title = job_info.get("title") or v5.get("core_job_title")
    company_name = company_data.get("name") or v5.get("company_name")
    location = v5.get("formatted_workplace_location")
    date_posted = v5.get("estimated_publish_date")
    job_url = job.get("apply_url")
    description = job_info.get("description", "")

    experience_yoe = v5.get("min_industry_and_role_yoe")
    experience = str(experience_yoe) if experience_yoe is not None else None

    commitment = v5.get("commitment")
    job_type = ", ".join(commitment) if isinstance(commitment, list) else commitment

    salary = _build_salary(v5)

    return {
        "site": "hire.cafe",
        "title": title,
        "company": company_name,
        "location": location,
        "job_url": job_url,
        "description": description,
        "date_posted": date_posted,
        "experience": experience,
        "salary": salary,
        "job_type": job_type,
        "raw_payload": job,
    }
