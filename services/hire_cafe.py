"""
HireCafe scraper — captures job payloads from hiring.cafe by intercepting
network responses via Chrome DevTools Protocol using undetected-chromedriver.

Requires xvfb virtual display on headless servers (the uvicorn process should
be launched via ``xvfb-run -a``).
"""

import html
import json
import logging
import math
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

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

HARDCODED_CF_CLICK_X = int(os.getenv("HIRECAFE_CF_CLICK_X", "532"))
HARDCODED_CF_CLICK_Y = int(os.getenv("HIRECAFE_CF_CLICK_Y", "336"))
HIRECAFE_CF_MULTI_CLICK_DELAY_SECONDS = float(
    os.getenv("HIRECAFE_CF_MULTI_CLICK_DELAY_SECONDS", "0.9")
)
HIRECAFE_CF_POST_CLICK_SETTLE_SECONDS = float(
    os.getenv("HIRECAFE_CF_POST_CLICK_SETTLE_SECONDS", "1.8")
)
HIRECAFE_INITIAL_PAGE_EXTRA_WAIT_SECONDS = float(
    os.getenv("HIRECAFE_INITIAL_PAGE_EXTRA_WAIT_SECONDS", "0")
)
HIRECAFE_CHALLENGE_STABILIZE_SECONDS = float(
    os.getenv("HIRECAFE_CHALLENGE_STABILIZE_SECONDS", "0")
)
HIRECAFE_CF_CLICK_STRATEGY_VERSION = "2026-04-20.v4.verify-text-square-probe"

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
HIRECAFE_CHROMEDRIVER_PATH = os.getenv("HIRECAFE_CHROMEDRIVER_PATH", "").strip()
HIRECAFE_CHROME_BINARY = os.getenv("HIRECAFE_CHROME_BINARY", "").strip()
HIRECAFE_USE_SYSTEM_BINARIES = os.getenv("HIRECAFE_USE_SYSTEM_BINARIES", "auto").strip().lower()
HIRECAFE_CHROME_USER_DATA_DIR = os.getenv("HIRECAFE_CHROME_USER_DATA_DIR", "").strip()
HIRECAFE_CHROME_PROFILE_DIR = os.getenv("HIRECAFE_CHROME_PROFILE_DIR", "").strip()
HIRECAFE_HEADLESS = os.getenv("HIRECAFE_HEADLESS", "true").strip().lower() not in (
    "false", "0", "no",
)
_HIRECAFE_UC_VERSION_MAIN_RAW = os.getenv("HIRECAFE_UC_VERSION_MAIN", "147").strip()
try:
    HIRECAFE_UC_VERSION_MAIN: int | None = int(_HIRECAFE_UC_VERSION_MAIN_RAW)
except ValueError:
    logger.warning(
        "hirecafe invalid HIRECAFE_UC_VERSION_MAIN=%r; falling back to uc auto version",
        _HIRECAFE_UC_VERSION_MAIN_RAW,
    )
    HIRECAFE_UC_VERSION_MAIN = None

HIRECAFE_OBSERVE_PAGES = os.getenv("HIRECAFE_OBSERVE_PAGES", "false").lower() not in (
    "false", "0", "no",
)
HIRECAFE_OBSERVE_DIR = os.getenv(
    "HIRECAFE_OBSERVE_DIR",
    "debug_screenshots/hirecafe_cloudflare",
).strip()
_HIRECAFE_OBSERVE_STEP_COUNTER = 0

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

_PRODUCTION_ENV_KEYS = (
    "RAILWAY_ENVIRONMENT",
    "RAILWAY_PROJECT_ID",
    "RAILWAY_SERVICE_ID",
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

# ---------------------------------------------------------------------------
#  Browser identity constants — must match the Chromium version in the Docker
#  image (Debian bookworm chromium package ≈ 131).
# ---------------------------------------------------------------------------
_CHROME_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)
_CHROME_SEC_CH_UA = '"Chromium";v="147", "Not_A Brand";v="24"'
_CHROME_SEC_CH_UA_PLATFORM = '"Linux"'


# ---------------------------------------------------------------------------
#  Stealth JS injection — runs before any page loads.
# ---------------------------------------------------------------------------
_STEALTH_JS = r"""
// --- navigator.webdriver ---------------------------------------------------
delete Object.getPrototypeOf(navigator).webdriver;

// --- navigator.hardwareConcurrency ----------------------------------------
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});

// --- navigator.deviceMemory -----------------------------------------------
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});

// --- WebGL vendor / renderer -----------------------------------------------
(function() {
    const getParam = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {
        if (param === 37445) return 'Google Inc. (NVIDIA)';
        if (param === 37446) return 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 SUPER, OpenGL 4.5)';
        return getParam.call(this, param);
    };
    if (typeof WebGL2RenderingContext !== 'undefined') {
        const getParam2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(param) {
            if (param === 37445) return 'Google Inc. (NVIDIA)';
            if (param === 37446) return 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 SUPER, OpenGL 4.5)';
            return getParam2.call(this, param);
        };
    }
})();

// --- navigator.plugins (non-empty) -----------------------------------------
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [{
            name: 'Chrome PDF Plugin',
            description: 'Portable Document Format',
            filename: 'internal-pdf-viewer',
            length: 1,
            0: {type: 'application/x-google-chrome-pdf', suffixes: 'pdf',
                description: 'Portable Document Format', enabledPlugin: null}
        }];
        arr.refresh = () => {};
        Object.setPrototypeOf(arr, PluginArray.prototype);
        return arr;
    }
});

// --- navigator.permissions.query -------------------------------------------
(function() {
    const origQuery = navigator.permissions && navigator.permissions.query;
    if (origQuery) {
        navigator.permissions.query = function(desc) {
            if (desc && desc.name === 'notifications') {
                return Promise.resolve({state: Notification.permission || 'default'});
            }
            return origQuery.call(this, desc);
        };
    }
})();
"""


def _inject_stealth_scripts(driver) -> None:
    """Inject anti-fingerprint JS patches via CDP before any page loads."""
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": _STEALTH_JS},
        )
        # Override screen metrics to hide headless 800x600 defaults
        driver.execute_cdp_cmd(
            "Emulation.setDeviceMetricsOverride",
            {
                "width": 1920,
                "height": 1080,
                "deviceScaleFactor": 1,
                "mobile": False,
                "screenWidth": 1920,
                "screenHeight": 1080,
            }
        )
        logger.info("hirecafe stealth scripts injected")
    except Exception as exc:
        logger.warning(
            "hirecafe stealth script injection failed: %s",
            type(exc).__name__,
        )

    # Align HTTP headers with the UA string.
    try:
        driver.execute_cdp_cmd(
            "Network.setExtraHTTPHeaders",
            {
                "headers": {
                    "sec-ch-ua": _CHROME_SEC_CH_UA,
                    "sec-ch-ua-platform": _CHROME_SEC_CH_UA_PLATFORM,
                }
            },
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
#  Bézier-curve human-like mouse movement helpers
# ---------------------------------------------------------------------------

def _bezier_point(
    t: float,
    p0: tuple[float, float],
    p1: tuple[float, float],
    p2: tuple[float, float],
    p3: tuple[float, float],
) -> tuple[float, float]:
    """Evaluate a cubic Bézier curve at parameter *t* ∈ [0, 1]."""
    u = 1.0 - t
    tt = t * t
    uu = u * u
    uuu = uu * u
    ttt = tt * t
    x = uuu * p0[0] + 3 * uu * t * p1[0] + 3 * u * tt * p2[0] + ttt * p3[0]
    y = uuu * p0[1] + 3 * uu * t * p1[1] + 3 * u * tt * p2[1] + ttt * p3[1]
    return (x, y)


def _human_mouse_path(
    x0: int, y0: int, x1: int, y1: int,
    steps: int | None = None,
) -> list[tuple[int, int]]:
    """Generate a list of *(x, y)* waypoints along a jittered cubic Bézier
    curve from *(x0, y0)* to *(x1, y1)*."""
    if steps is None:
        steps = random.randint(20, 40)

    dx = x1 - x0
    dy = y1 - y0
    dist = math.hypot(dx, dy) or 1.0

    # Two random control-points that bow the curve naturally.
    spread = max(40, dist * 0.35)
    cp1 = (
        x0 + dx * random.uniform(0.15, 0.45) + random.gauss(0, spread * 0.3),
        y0 + dy * random.uniform(0.0, 0.35) + random.gauss(0, spread * 0.3),
    )
    cp2 = (
        x0 + dx * random.uniform(0.55, 0.85) + random.gauss(0, spread * 0.25),
        y0 + dy * random.uniform(0.65, 1.0) + random.gauss(0, spread * 0.25),
    )

    p0 = (float(x0), float(y0))
    p3 = (float(x1), float(y1))
    path: list[tuple[int, int]] = []
    for i in range(steps + 1):
        t = i / steps
        bx, by = _bezier_point(t, p0, cp1, cp2, p3)
        # Gaussian jitter (±1-3 px) except at endpoints.
        if 0 < i < steps:
            bx += random.gauss(0, 1.5)
            by += random.gauss(0, 1.5)
        path.append((int(round(bx)), int(round(by))))

    # Always end exactly on target.
    path[-1] = (x1, y1)
    return path



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
            "cf_challenge_text: /just a moment|security check|verify you are human|performing security verification|checking your browser/.test(bodyText),"
            "cf_turnstile_success: (function(){"
            "  var inp=document.querySelector(\"input[name='cf-turnstile-response']\");"
            "  if(inp && inp.value && inp.value.length>30) return 'token_present';"
            "  var st=document.querySelector('#challenge-success-text');"
            "  if(st && (st.offsetWidth > 0 || st.offsetHeight > 0)) return 'success_text_visible';"
            "  var iframes=document.querySelectorAll(\"iframe[src*='challenges.cloudflare.com']\");"
            "  for(var fr of iframes){try{if(fr.getAttribute('data-cdata')&&fr.style.display!=='none') return 'iframe_active';}catch(e){}}"
            "  return false;"
            "})()"
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
    state["turnstile_success"] = selector_hits.get("cf_turnstile_success") or False
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
            # Also verify the Turnstile token appeared (poll up to 5 s).
            if last_state.get("turnstile_success"):
                return True, last_state
            ts_deadline = time.time() + 5.0
            while time.time() < ts_deadline:
                last_state = _probe_cloudflare_challenge(driver)
                if last_state.get("turnstile_success"):
                    return True, last_state
                time.sleep(0.5)
            # Even without a token, if the challenge is gone we accept it.
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

        global _HIRECAFE_OBSERVE_STEP_COUNTER
        _HIRECAFE_OBSERVE_STEP_COUNTER += 1

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        safe_stage = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stage)
        base = output_dir / f"{stamp}_{_HIRECAFE_OBSERVE_STEP_COUNTER:05d}_{safe_stage}"

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
            "observe_step_index": _HIRECAFE_OBSERVE_STEP_COUNTER,
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


def _observe_step(
    driver,
    stage: str,
    extra: Optional[dict[str, Any]] = None,
) -> None:
    """Capture a step-level snapshot when observation mode is enabled."""
    if not HIRECAFE_OBSERVE_PAGES:
        return
    _capture_observation_artifacts(driver, stage, extra)


def _is_production_runtime() -> bool:
    return any(os.getenv(key) for key in _PRODUCTION_ENV_KEYS)


def _is_docker_runtime() -> bool:
    if Path("/.dockerenv").is_file():
        return True

    cgroup_path = Path("/proc/1/cgroup")
    if not cgroup_path.is_file():
        return False

    try:
        cgroup_text = cgroup_path.read_text(encoding="utf-8", errors="ignore").lower()
    except Exception:
        return False

    return any(marker in cgroup_text for marker in ("docker", "containerd", "podman", "kubepods"))


def _detectable_mode_block_runtime() -> str | None:
    if _is_production_runtime():
        return "production"
    if _is_docker_runtime():
        return "docker"
    return None


def _resolve_hirecafe_browser_mode() -> tuple[str, str, str | None]:
    requested_mode = (HIRECAFE_BROWSER_MODE or "").strip().lower()
    if requested_mode not in ("stealth", "detectable"):
        logger.warning(
            "hirecafe unknown browser mode=%s, forcing stealth",
            requested_mode or "<empty>",
        )
        return requested_mode or "stealth", "stealth", "invalid_mode"

    blocked_runtime = _detectable_mode_block_runtime()
    if requested_mode == "detectable" and blocked_runtime:
        return requested_mode, "stealth", blocked_runtime

    return requested_mode, requested_mode, None


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

    if HIRECAFE_DETECTABLE_HEADLESS and HIRECAFE_HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument(f"--user-agent={_CHROME_UA}")
    if HIRECAFE_CHROME_USER_DATA_DIR:
        options.add_argument(f"--user-data-dir={HIRECAFE_CHROME_USER_DATA_DIR}")
    if HIRECAFE_CHROME_PROFILE_DIR:
        options.add_argument(f"--profile-directory={HIRECAFE_CHROME_PROFILE_DIR}")

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


def _launch_hirecafe_driver(browser_mode: str = "stealth"):
    options = uc.ChromeOptions()
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    options.add_argument("--disable-dev-shm-usage")
    # Removing --disable-gpu to allow WebGL context to actually initialize
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument(f"--user-agent={_CHROME_UA}")
    if HIRECAFE_HEADLESS:
        options.add_argument("--headless=new")
    if HIRECAFE_CHROME_USER_DATA_DIR:
        options.add_argument(f"--user-data-dir={HIRECAFE_CHROME_USER_DATA_DIR}")
    if HIRECAFE_CHROME_PROFILE_DIR:
        options.add_argument(f"--profile-directory={HIRECAFE_CHROME_PROFILE_DIR}")

    if browser_mode == "detectable":
        detectable_driver = _launch_detectable_driver()
        if detectable_driver is not None:
            return detectable_driver

    system_browser = Path("/usr/bin/chromium").is_file()
    system_driver = Path("/usr/bin/chromedriver").is_file()
    if HIRECAFE_USE_SYSTEM_BINARIES in ("1", "true", "yes"):
        use_system_binaries = True
        use_system_reason = "env_forced_true"
    elif HIRECAFE_USE_SYSTEM_BINARIES in ("0", "false", "no"):
        use_system_binaries = False
        use_system_reason = "env_forced_false"
    else:
        # Auto mode: only force system binaries in known production runtimes.
        use_system_binaries = _is_production_runtime() and system_browser and system_driver
        use_system_reason = "auto_production_only"

    driver = None
    if use_system_binaries:
        logger.info(
            "hirecafe using system chromium binaries (reason=%s browser=%s driver=%s)",
            use_system_reason,
            system_browser,
            system_driver,
        )
        kwargs = {
            "options": options,
            "browser_executable_path": "/usr/bin/chromium",
            "driver_executable_path": "/usr/bin/chromedriver",
        }
        if HIRECAFE_UC_VERSION_MAIN is not None:
             kwargs["version_main"] = HIRECAFE_UC_VERSION_MAIN
        driver = uc.Chrome(**kwargs)
    else:
        logger.info(
            "hirecafe using bundled undetected-chromedriver binaries (reason=%s browser=%s driver=%s)",
            use_system_reason,
            system_browser,
            system_driver,
        )
        if HIRECAFE_UC_VERSION_MAIN is None:
            driver = uc.Chrome(options=options)
        else:
            driver = uc.Chrome(options=options, version_main=HIRECAFE_UC_VERSION_MAIN)

    # Inject stealth patches before any page loads.
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
    """Move the pointer to *(x, y)* along a human-like Bézier curve, then click."""
    try:
        # Pick a random starting position offset from the target.
        start_x = _clamp_int(
            x + random.randint(-180, -40),
            0,
            1919,
        )
        start_y = _clamp_int(
            y + random.randint(-120, -25),
            0,
            1079,
        )

        # Generate a curved, jittered path (20–40 waypoints).
        path = _human_mouse_path(start_x, start_y, x, y)

        for wx, wy in path:
            driver.execute_cdp_cmd(
                "Input.dispatchMouseEvent",
                {
                    "type": "mouseMoved",
                    "x": _clamp_int(wx, 0, 1919),
                    "y": _clamp_int(wy, 0, 1079),
                },
            )
            # Variable micro-delay between waypoints (8–25 ms).
            time.sleep(random.uniform(0.008, 0.025))

        # Pre-click hover pause — humans don’t click the instant the cursor arrives.
        time.sleep(random.uniform(0.08, 0.20))

        driver.execute_cdp_cmd(
            "Input.dispatchMouseEvent",
            {"type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1},
        )
        # Hold duration — simulate physical button depression.
        time.sleep(random.uniform(0.05, 0.12))
        driver.execute_cdp_cmd(
            "Input.dispatchMouseEvent",
            {"type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1},
        )
        return True
    except Exception as exc:
        logger.info("hirecafe Bézier click failed at (%s,%s): %s", x, y, type(exc).__name__)
        return False


def _clamp_int(value: int, lower: int, upper: int) -> int:
    if lower > upper:
        return value
    return max(lower, min(upper, value))


def _normalize_viewport(raw_viewport: Any) -> dict[str, int]:
    width = 1920
    height = 1080
    if isinstance(raw_viewport, dict):
        try:
            width = int(raw_viewport.get("width") or width)
        except Exception:
            pass
        try:
            height = int(raw_viewport.get("height") or height)
        except Exception:
            pass

    return {
        "width": max(1, width),
        "height": max(1, height),
    }


def _normalize_box(raw_box: Any, viewport: dict[str, int]) -> dict[str, int] | None:
    if not isinstance(raw_box, dict):
        return None

    try:
        x1 = int(float(raw_box.get("x1")))
        y1 = int(float(raw_box.get("y1")))
        x2 = int(float(raw_box.get("x2")))
        y2 = int(float(raw_box.get("y2")))
    except Exception:
        return None

    max_x = max(0, viewport["width"] - 1)
    max_y = max(0, viewport["height"] - 1)
    x1 = _clamp_int(x1, 0, max_x)
    x2 = _clamp_int(x2, 0, max_x)
    y1 = _clamp_int(y1, 0, max_y)
    y2 = _clamp_int(y2, 0, max_y)

    if x2 <= x1 or y2 <= y1:
        return None

    return {
        "x1": x1,
        "y1": y1,
        "x2": x2,
        "y2": y2,
        "width": x2 - x1,
        "height": y2 - y1,
    }


def _build_checkbox_anchor_candidates(
    checkbox_box: dict[str, int],
    viewport: dict[str, int],
) -> list[dict[str, Any]]:
    x1 = checkbox_box["x1"]
    y1 = checkbox_box["y1"]
    x2 = checkbox_box["x2"]
    y2 = checkbox_box["y2"]

    cx = int((x1 + x2) / 2)
    cy = int((y1 + y2) / 2)
    w = max(1, x2 - x1)
    h = max(1, y2 - y1)

    inset_x = max(2, int(w * 0.22))
    inset_y = max(2, int(h * 0.22))

    raw_points = [
        (cx, cy, "P1"),
        (int(x1 + inset_x), cy, "P2"),
        (int(x2 - inset_x), cy, "P3"),
        (cx, int(y1 + inset_y), "P4"),
    ]

    max_x = max(0, viewport["width"] - 1)
    max_y = max(0, viewport["height"] - 1)
    seen: set[tuple[int, int]] = set()
    candidates: list[dict[str, Any]] = []

    for x, y, label in raw_points:
        px = _clamp_int(int(x), 0, max_x)
        py = _clamp_int(int(y), 0, max_y)
        key = (px, py)
        if key in seen:
            continue
        seen.add(key)
        candidates.append({
            "x": px,
            "y": py,
            "label": label,
            "source": "checkbox_anchor",
        })

    return candidates


def _build_widget_fallback_candidates(
    widget_box: dict[str, int],
    viewport: dict[str, int],
) -> list[dict[str, Any]]:
    x1 = widget_box["x1"]
    y1 = widget_box["y1"]
    x2 = widget_box["x2"]
    y2 = widget_box["y2"]

    w = max(1, x2 - x1)
    h = max(1, y2 - y1)
    raw_points = [
        (int(x1 + 0.20 * w), int(y1 + 0.50 * h), "P1"),
        (int(x1 + 0.32 * w), int(y1 + 0.50 * h), "P2"),
        (int(x1 + 0.50 * w), int(y1 + 0.50 * h), "P3"),
        (int(x1 + 0.20 * w), int(y1 + 0.68 * h), "P4"),
    ]

    max_x = max(0, viewport["width"] - 1)
    max_y = max(0, viewport["height"] - 1)
    seen: set[tuple[int, int]] = set()
    candidates: list[dict[str, Any]] = []

    for x, y, label in raw_points:
        px = _clamp_int(int(x), 0, max_x)
        py = _clamp_int(int(y), 0, max_y)
        key = (px, py)
        if key in seen:
            continue
        seen.add(key)
        candidates.append({
            "x": px,
            "y": py,
            "label": label,
            "source": "widget_fallback",
        })

    return candidates


def _build_text_visual_fallback_candidates(
    widget_box: dict[str, int],
    viewport: dict[str, int],
) -> list[dict[str, Any]]:
    """Generate clicks close to the expected Turnstile checkbox hotspot near the widget's left edge."""
    x1 = widget_box["x1"]
    y1 = widget_box["y1"]
    x2 = widget_box["x2"]
    y2 = widget_box["y2"]

    w = max(1, x2 - x1)
    h = max(1, y2 - y1)
    raw_points = [
        (int(x1 + 0.07 * w), int(y1 + 0.50 * h), "T1"),
        (int(x1 + 0.10 * w), int(y1 + 0.50 * h), "T2"),
        (int(x1 + 0.13 * w), int(y1 + 0.50 * h), "T3"),
        (int(x1 + 0.07 * w), int(y1 + 0.64 * h), "T4"),
    ]

    max_x = max(0, viewport["width"] - 1)
    max_y = max(0, viewport["height"] - 1)
    seen: set[tuple[int, int]] = set()
    candidates: list[dict[str, Any]] = []

    for x, y, label in raw_points:
        px = _clamp_int(int(x), 0, max_x)
        py = _clamp_int(int(y), 0, max_y)
        key = (px, py)
        if key in seen:
            continue
        seen.add(key)
        candidates.append({
            "x": px,
            "y": py,
            "label": label,
            "source": "text_visual_fallback",
        })

    return candidates


def _resolve_cloudflare_click_plan(driver) -> dict[str, Any]:
    raw: dict[str, Any] = {}
    js_error: str | None = None

    try:
        raw = driver.execute_script(
            "const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));"
            "const vw=Math.round(window.innerWidth||document.documentElement.clientWidth||1920);"
            "const vh=Math.round(window.innerHeight||document.documentElement.clientHeight||1080);"
            "const toBox=(el)=>{"
            "if(!el)return null;"
            "const r=el.getBoundingClientRect();"
            "if(!r||r.width<=0||r.height<=0)return null;"
            "return {x1:Math.round(r.left),y1:Math.round(r.top),x2:Math.round(r.right),y2:Math.round(r.bottom)};"
            "};"
            "const checkboxSelectors=[\"input[type='checkbox']\",\"[role='checkbox']\",\".ctp-checkbox-container\",\".mark\",\"label.ctp-checkbox-label\",\"label[for*='cf-chl']\"];"
            "const checkboxCandidates=[];"
            "for(const sel of checkboxSelectors){"
            "for(const el of document.querySelectorAll(sel)){"
            "const box=toBox(el);"
            "if(!box)continue;"
            "const w=Math.max(1,box.x2-box.x1);"
            "const h=Math.max(1,box.y2-box.y1);"
            "const minSide=Math.min(w,h);"
            "const maxSide=Math.max(w,h);"
            "if(minSide<10||maxSide>70)continue;"
            "if((maxSide/minSide)>1.6)continue;"
            "const leftBias=1-Math.min(1,Math.max(0,box.x1/Math.max(1,vw)));"
            "const sizeBias=1-Math.min(1,Math.abs(minSide-24)/24);"
            "const score=(leftBias*0.65)+(sizeBias*0.35);"
            "checkboxCandidates.push({selector:sel,score:score,box:box});"
            "}"
            "}"
            "checkboxCandidates.sort((a,b)=>b.score-a.score);"
            "const checkbox=checkboxCandidates.length?checkboxCandidates[0]:null;"
            "const iframeSelectors=[\"iframe[src*='challenges.cloudflare.com']\",\"iframe[src*='turnstile']\",\"iframe[src*='cloudflare']\"];"
            "const iframeCandidates=[];"
            "for(const sel of iframeSelectors){"
            "for(const el of document.querySelectorAll(sel)){"
            "const box=toBox(el);"
            "if(!box)continue;"
            "const w=Math.max(1,box.x2-box.x1);"
            "const h=Math.max(1,box.y2-box.y1);"
            "if(w<120||h<30)continue;"
            "const leftBias=1-Math.min(1,Math.max(0,box.x1/Math.max(1,vw)));"
            "const score=(leftBias*0.6)+(Math.min(1,w/420)*0.25)+(Math.min(1,h/140)*0.15);"
            "iframeCandidates.push({selector:sel,score:score,box:box});"
            "}"
            "}"
            "iframeCandidates.sort((a,b)=>b.score-a.score);"
            "const iframe=iframeCandidates.length?iframeCandidates[0]:null;"
            "const hiddenInputCandidates=[];"
            "const hiddenInputs=[...document.querySelectorAll(\"input[name='cf-turnstile-response'], input[id^='cf-chl-widget'][id$='_response']\")];"
            "for(const inputEl of hiddenInputs){"
            "let node=inputEl;"
            "for(let depth=0;depth<8 && node;depth++){"
            "const box=toBox(node);"
            "if(box){"
            "const w=Math.max(1,box.x2-box.x1);"
            "const h=Math.max(1,box.y2-box.y1);"
            "if(w>=140 && w<=520 && h>=32 && h<=220){"
            "const leftBias=1-Math.min(1,Math.max(0,box.x1/Math.max(1,vw)));"
            "const widthBias=1-Math.min(1,Math.abs(w-320)/320);"
            "const heightBias=1-Math.min(1,Math.abs(h-78)/78);"
            "const depthPenalty=Math.min(0.2,depth*0.02);"
            "const score=(leftBias*0.45)+(widthBias*0.33)+(heightBias*0.22)-depthPenalty;"
            "hiddenInputCandidates.push({score:score,box:box,depth:depth});"
            "}"
            "}"
            "node=node.parentElement;"
            "}"
            "}"
            "hiddenInputCandidates.sort((a,b)=>b.score-a.score);"
            "const hiddenInputAnchor=hiddenInputCandidates.length?hiddenInputCandidates[0]:null;"
            "const textVisualCandidates=[];"
            "const pushTextVisual=(box,source)=>{"
            "if(!box)return;"
            "const w=Math.max(1,box.x2-box.x1);"
            "const h=Math.max(1,box.y2-box.y1);"
            "if(w<140||w>960||h<24||h>280)return;"
            "const leftBias=1-Math.min(1,Math.max(0,box.x1/Math.max(1,vw)));"
            "const widthBias=1-Math.min(1,Math.abs(w-300)/420);"
            "const heightBias=1-Math.min(1,Math.abs(h-56)/130);"
            "const score=(leftBias*0.44)+(widthBias*0.34)+(heightBias*0.22);"
            "textVisualCandidates.push({score:score,source:source,box:box});"
            "};"
            "const textVisualSelectors=[\"#hQLfM7\",\".main-content #hQLfM7\",\"[id*='cf-chl-widget'][id$='_container']\"];"
            "for(const sel of textVisualSelectors){"
            "for(const el of document.querySelectorAll(sel)){"
            "pushTextVisual(toBox(el),sel);"
            "}"
            "}"
            "const mainContentBox=toBox(document.querySelector('.main-content'));"
            "let challengeHeadingBox=null;"
            "for(const el of document.querySelectorAll('h2,p,div,span')){"
            "const txt=(el.innerText||'').toLowerCase();"
            "if(!txt)continue;"
            "if(!(txt.includes('performing security verification')||txt.includes('verify you are human')||txt.includes('this website uses a security service')))continue;"
            "const box=toBox(el);"
            "if(!box)continue;"
            "challengeHeadingBox=box;"
            "break;"
            "}"
            "if(mainContentBox&&challengeHeadingBox){"
            "const estY1=clamp(Math.round(challengeHeadingBox.y2+12),mainContentBox.y1+42,Math.max(mainContentBox.y1+42,mainContentBox.y2-76));"
            "const estBox={"
            "x1:clamp(Math.round(mainContentBox.x1+24),0,vw-1),"
            "y1:clamp(estY1,0,vh-1),"
            "x2:clamp(Math.round(mainContentBox.x1+340),0,vw-1),"
            "y2:clamp(Math.round(estY1+56),0,vh-1)"
            "};"
            "if(estBox.x2>estBox.x1&&estBox.y2>estBox.y1)pushTextVisual(estBox,'main_content_text_estimate');"
            "}"
            "textVisualCandidates.sort((a,b)=>b.score-a.score);"
            "const textVisualAnchor=textVisualCandidates.length?textVisualCandidates[0]:null;"
            "const findVerifyPromptAndSquare=()=>{"
            "const promptNeedles=['verify you are human','verify you are a human'];"
            "const nodes=[...document.querySelectorAll('label,span,p,div')];"
            "let promptEl=null;"
            "let promptBox=null;"
            "for(const node of nodes){"
            "const txt=((node.innerText||'')+'').toLowerCase().replace(/\\s+/g,' ').trim();"
            "if(!txt)continue;"
            "if(!promptNeedles.some((needle)=>txt.includes(needle)))continue;"
            "const b=toBox(node);"
            "if(!b)continue;"
            "const bw=Math.max(1,b.x2-b.x1);"
            "const bh=Math.max(1,b.y2-b.y1);"
            "if(bw<70||bw>520||bh>48)continue;"
            "promptEl=node;"
            "promptBox=b;"
            "break;"
            "}"
            "if(!promptEl||!promptBox)return {promptBox:null,checkboxBox:null};"
            "const containerCandidates=[];"
            "let ptr=promptEl;"
            "for(let depth=0;depth<6&&ptr;depth++){"
            "const box=toBox(ptr);"
            "if(box){"
            "const w=Math.max(1,box.x2-box.x1);"
            "const h=Math.max(1,box.y2-box.y1);"
            "if(w>=120&&w<=780&&h>=24&&h<=240){"
            "const leftBias=1-Math.min(1,Math.max(0,box.x1/Math.max(1,vw)));"
            "const score=(leftBias*0.6)+(Math.min(1,w/420)*0.25)+(Math.min(1,h/120)*0.15);"
            "containerCandidates.push({score:score,box:box,el:ptr});"
            "}"
            "}"
            "ptr=ptr.parentElement;"
            "}"
            "containerCandidates.sort((a,b)=>b.score-a.score);"
            "const container=containerCandidates.length?containerCandidates[0]:null;"
            "const scope=(container&&container.el)?container.el:promptEl.parentElement||document.body;"
            "const squareCandidates=[];"
            "const all=[...scope.querySelectorAll('*')];"
            "for(const el of all){"
            "if(el===promptEl)continue;"
            "const b=toBox(el);"
            "if(!b)continue;"
            "const w=Math.max(1,b.x2-b.x1);"
            "const h=Math.max(1,b.y2-b.y1);"
            "const minSide=Math.min(w,h);"
            "const maxSide=Math.max(w,h);"
            "if(minSide<10||maxSide>34)continue;"
            "if((maxSide/minSide)>1.28)continue;"
            "const isLeft=(b.x2<=promptBox.x1+8);"
            "const verticalOverlap=Math.min(b.y2,promptBox.y2)-Math.max(b.y1,promptBox.y1);"
            "if(!isLeft||verticalOverlap<4)continue;"
            "const centerY=Math.round((b.y1+b.y2)/2);"
            "const promptCenterY=Math.round((promptBox.y1+promptBox.y2)/2);"
            "const yDelta=Math.abs(centerY-promptCenterY);"
            "const xGap=Math.max(0,promptBox.x1-b.x2);"
            "if(xGap>42)continue;"
            "const squareBias=1-Math.min(1,Math.abs(minSide-14)/14);"
            "const yBias=1-Math.min(1,yDelta/16);"
            "const gapBias=1-Math.min(1,xGap/26);"
            "const score=(squareBias*0.45)+(yBias*0.35)+(gapBias*0.20);"
            "squareCandidates.push({score:score,box:b});"
            "}"
            "squareCandidates.sort((a,b)=>b.score-a.score);"
            "return {"
            "promptBox:promptBox,"
            "checkboxBox:squareCandidates.length?squareCandidates[0].box:null,"
            "containerBox:container?container.box:null"
            "};"
            "};"
            "const verifyPromptProbe=findVerifyPromptAndSquare();"
            "let method='none';"
            "let checkboxBox=null;"
            "let widgetBox=null;"
            "let hiddenInputBox=null;"
            "let textVisualBox=null;"
            "let textVisualSource=null;"
            "if(checkbox){"
            "checkboxBox=checkbox.box;"
            "method='checkbox_dom';"
            "widgetBox={x1:checkboxBox.x1-10,y1:checkboxBox.y1-14,x2:checkboxBox.x2+260,y2:checkboxBox.y2+14};"
            "}else if(iframe){"
            "const ib=iframe.box;"
            "const iw=Math.max(1,ib.x2-ib.x1);"
            "const ih=Math.max(1,ib.y2-ib.y1);"
            "const side=clamp(Math.round(Math.min(Math.max(ih*0.42,18),Math.min(iw*0.20,30))),16,34);"
            "const cx=clamp(Math.round(ib.x1+Math.max(14,iw*0.12)),ib.x1+2,ib.x2-2);"
            "const cy=clamp(Math.round(ib.y1+(ih/2)),ib.y1+2,ib.y2-2);"
            "checkboxBox={x1:cx-Math.round(side/2),y1:cy-Math.round(side/2),x2:cx+Math.round(side/2),y2:cy+Math.round(side/2)};"
            "method='iframe_anchor';"
            "widgetBox={x1:checkboxBox.x1-10,y1:checkboxBox.y1-14,x2:checkboxBox.x2+260,y2:checkboxBox.y2+14};"
            "}else if(hiddenInputAnchor){"
            "hiddenInputBox=hiddenInputAnchor.box;"
            "const hb=hiddenInputBox;"
            "const hw=Math.max(1,hb.x2-hb.x1);"
            "const hh=Math.max(1,hb.y2-hb.y1);"
            "const side=clamp(Math.round(Math.min(Math.max(hh*0.45,18),30)),16,34);"
            "const cx=clamp(Math.round(hb.x1+Math.min(78,Math.max(24,hw*0.20))),hb.x1+2,hb.x2-2);"
            "const cy=clamp(Math.round(hb.y1+(hh*0.50)),hb.y1+2,hb.y2-2);"
            "checkboxBox={x1:cx-Math.round(side/2),y1:cy-Math.round(side/2),x2:cx+Math.round(side/2),y2:cy+Math.round(side/2)};"
            "widgetBox=hb;"
            "method='hidden_input_anchor';"
            "}else if(textVisualAnchor){"
            "textVisualBox=textVisualAnchor.box;"
            "textVisualSource=textVisualAnchor.source;"
            "const tb=textVisualBox;"
            "const tw=Math.max(1,tb.x2-tb.x1);"
            "const th=Math.max(1,tb.y2-tb.y1);"
            "const side=clamp(Math.round(Math.min(Math.max(th*0.45,18),30)),16,34);"
            "const cx=clamp(Math.round(tb.x1+Math.max(16,Math.min(42,tw*0.09))),tb.x1+2,tb.x2-2);"
            "const cy=clamp(Math.round(tb.y1+(th*0.50)),tb.y1+2,tb.y2-2);"
            "checkboxBox={x1:cx-Math.round(side/2),y1:cy-Math.round(side/2),x2:cx+Math.round(side/2),y2:cy+Math.round(side/2)};"
            "widgetBox=tb;"
            "method='text_visual_anchor';"
            "}else if(verifyPromptProbe&&verifyPromptProbe.checkboxBox){"
            "checkboxBox=verifyPromptProbe.checkboxBox;"
            "widgetBox=verifyPromptProbe.containerBox||{"
            "x1:clamp(Math.round(checkboxBox.x1-8),0,vw-1),"
            "y1:clamp(Math.round(checkboxBox.y1-12),0,vh-1),"
            "x2:clamp(Math.round(checkboxBox.x2+260),0,vw-1),"
            "y2:clamp(Math.round(checkboxBox.y2+12),0,vh-1)"
            "};"
            "method='verify_text_square_probe';"
            "}"
            "return {"
            "viewport:{width:vw,height:vh},"
            "method:method,"
            "checkbox_box:checkboxBox,"
            "iframe_box:iframe?iframe.box:null,"
            "hidden_input_box:hiddenInputBox,"
            "text_visual_box:textVisualBox,"
            "text_visual_source:textVisualSource,"
            "verify_prompt_box:verifyPromptProbe?verifyPromptProbe.promptBox:null,"
            "widget_box:widgetBox,"
            "checkbox_selector:checkbox?checkbox.selector:null,"
            "iframe_selector:iframe?iframe.selector:null"
            "};"
        ) or {}
    except Exception as exc:
        js_error = type(exc).__name__
        raw = {}

    viewport = _normalize_viewport(raw.get("viewport"))
    checkbox_box = _normalize_box(raw.get("checkbox_box"), viewport)
    iframe_box = _normalize_box(raw.get("iframe_box"), viewport)
    hidden_input_box = _normalize_box(raw.get("hidden_input_box"), viewport)
    text_visual_box = _normalize_box(raw.get("text_visual_box"), viewport)
    text_visual_source = raw.get("text_visual_source")
    verify_prompt_box = _normalize_box(raw.get("verify_prompt_box"), viewport)
    widget_box = _normalize_box(raw.get("widget_box"), viewport)

    method = str(raw.get("method") or "none")
    if js_error:
        method = "js_error"

    candidates: list[dict[str, Any]] = []
    if checkbox_box:
        candidates.extend(_build_checkbox_anchor_candidates(checkbox_box, viewport))
    elif method == "text_visual_anchor" and widget_box:
        candidates.extend(_build_text_visual_fallback_candidates(widget_box, viewport))
    elif widget_box:
        candidates.extend(_build_widget_fallback_candidates(widget_box, viewport))

    legacy_x = _clamp_int(HARDCODED_CF_CLICK_X, 0, max(0, viewport["width"] - 1))
    legacy_y = _clamp_int(HARDCODED_CF_CLICK_Y, 0, max(0, viewport["height"] - 1))
    candidates.append({
        "x": legacy_x,
        "y": legacy_y,
        "label": "LEGACY",
        "source": "legacy_hardcoded",
    })

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    for candidate in candidates:
        key = (int(candidate["x"]), int(candidate["y"]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)

    for idx, candidate in enumerate(deduped, start=1):
        candidate["rank"] = idx
        candidate["recommended"] = idx == 1

    return {
        "method": method,
        "viewport": viewport,
        "checkbox_box": checkbox_box,
        "iframe_box": iframe_box,
        "hidden_input_box": hidden_input_box,
        "text_visual_box": text_visual_box,
        "text_visual_source": text_visual_source,
        "verify_prompt_box": verify_prompt_box,
        "widget_box": widget_box,
        "checkbox_selector": raw.get("checkbox_selector"),
        "iframe_selector": raw.get("iframe_selector"),
        "click_candidates": deduped,
        "js_error": js_error,
    }


def _execute_cloudflare_single_click(
    driver,
    click_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    clicked = False
    cleared = False
    last_state = _probe_cloudflare_challenge(driver)

    if not click_candidates:
        return {
            "clicked": False,
            "cleared": False,
            "attempt_count": 0,
            "attempt_limit": 0,
            "attempts": attempts,
            "last_state": last_state,
        }

    candidate = click_candidates[0]
    x = int(candidate.get("x", HARDCODED_CF_CLICK_X))
    y = int(candidate.get("y", HARDCODED_CF_CLICK_Y))
    rank = int(candidate.get("rank", 1))
    label = str(candidate.get("label") or "P1")
    source = str(candidate.get("source") or "unknown")
    sent = _click_viewport_coordinate(driver, x, y)
    clicked = clicked or sent

    post_click_wait = max(
        0.15,
        HIRECAFE_CF_MULTI_CLICK_DELAY_SECONDS,
        HIRECAFE_CF_POST_CLICK_SETTLE_SECONDS,
    )
    time.sleep(post_click_wait)
    last_state = _probe_cloudflare_challenge(driver)
    challenge_active = bool(last_state.get("active"))
    logger.info(
        "hirecafe challenge single-click rank=%s label=%s source=%s x=%s y=%s sent=%s challenge_active=%s",
        rank,
        label,
        source,
        x,
        y,
        sent,
        challenge_active,
    )

    attempts.append({
        "rank": rank,
        "label": label,
        "source": source,
        "x": x,
        "y": y,
        "sent": sent,
        "challenge_active": challenge_active,
    })
    if not challenge_active:
        cleared = True

    return {
        "clicked": clicked,
        "cleared": cleared,
        "attempt_count": len(attempts),
        "attempt_limit": 1,
        "attempts": attempts,
        "last_state": last_state,
    }


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
    page_no: int,
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
        card_no = processed

        _observe_step(
            driver,
            f"page_{page_no:03d}_card_{card_no:03d}_before_click",
            {
                "cards_seen": len(cards),
                "jobs_total_before_card": len(job_samples),
            },
        )

        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior:'auto',block:'center'});",
                card,
            )
            time.sleep(0.2)
        except Exception:
            pass

        card_clicked = _click_card_surface(driver, card)
        time.sleep(max(0.0, HIRECAFE_CARD_CLICK_PAUSE_SECONDS))

        _observe_step(
            driver,
            f"page_{page_no:03d}_card_{card_no:03d}_after_click",
            {
                "clicked": card_clicked,
                "jobs_total_after_click": len(job_samples),
            },
        )

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

        _observe_step(
            driver,
            f"page_{page_no:03d}_card_{card_no:03d}_after_collect",
            {
                "card_result": card_result,
                "jobs_total_after_collect": len(job_samples),
            },
        )

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
        _observe_step(
            driver,
            f"pagination_loop_{loop_idx:03d}_start",
            {
                "loop_idx": loop_idx,
                "max_pages": max_pages,
                "max_samples": max_samples,
                "current_total_jobs": len(job_samples),
            },
        )

        if len(job_samples) >= max_samples:
            logger.info("hirecafe stop: max_samples reached")
            _observe_step(
                driver,
                f"pagination_loop_{loop_idx:03d}_stop_max_samples",
                {"current_total_jobs": len(job_samples)},
            )
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
            _observe_step(
                driver,
                f"pagination_loop_{loop_idx:03d}_grid_not_found",
                {
                    "grid_meta": grid_meta,
                    "pagination_meta": pagination_meta,
                },
            )
            break

        signature = _page_signature(driver, cards)
        if signature and signature in visited_signatures:
            logger.info("hirecafe stop: repeated page signature")
            _observe_step(
                driver,
                f"pagination_loop_{loop_idx:03d}_stop_repeated_signature",
                {"signature": signature},
            )
            break
        if signature:
            visited_signatures.add(signature)

        page_no = int(pagination_meta.get("currentPage") or loop_idx)
        total_pages = pagination_meta.get("totalPages")

        _observe_step(
            driver,
            f"pagination_loop_{loop_idx:03d}_before_page_cards",
            {
                "page_no": page_no,
                "total_pages": total_pages,
                "grid_meta": grid_meta,
                "pagination_meta": pagination_meta,
                "signature": signature,
            },
        )

        before_page = len(job_samples)
        card_metrics = _process_cards_on_page(
            driver,
            cards,
            job_samples,
            seen_urls,
            seen_ids,
            max_samples=max_samples,
            page_no=page_no,
        )
        after_page = len(job_samples)

        _observe_step(
            driver,
            f"pagination_loop_{loop_idx:03d}_after_page_cards",
            {
                "page_no": page_no,
                "total_pages": total_pages,
                "card_metrics": card_metrics,
                "new_jobs": after_page - before_page,
                "total_jobs": after_page,
            },
        )

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
            _observe_step(
                driver,
                f"pagination_loop_{loop_idx:03d}_stop_after_cards_max_samples",
                {"total_jobs": len(job_samples)},
            )
            break

        if total_pages is not None and page_no >= int(total_pages):
            logger.info("hirecafe stop: last page reached")
            _observe_step(
                driver,
                f"pagination_loop_{loop_idx:03d}_stop_last_page",
                {
                    "page_no": page_no,
                    "total_pages": total_pages,
                    "total_jobs": len(job_samples),
                },
            )
            break

        old_signature = signature

        _observe_step(
            driver,
            f"pagination_loop_{loop_idx:03d}_before_scroll_bottom",
            {"page_signature": old_signature},
        )
        _scroll_to_bottom(driver)

        _observe_step(
            driver,
            f"pagination_loop_{loop_idx:03d}_after_scroll_bottom",
            {"page_signature": old_signature},
        )

        click_next = _click_next_pagination(driver)

        _observe_step(
            driver,
            f"pagination_loop_{loop_idx:03d}_after_next_click_attempt",
            {"click_next": click_next},
        )

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
            _observe_step(
                driver,
                f"pagination_loop_{loop_idx:03d}_stop_page_unchanged",
                {
                    "old_signature": old_signature,
                    "new_signature": new_signature,
                },
            )
            break

        _observe_step(
            driver,
            f"pagination_loop_{loop_idx:03d}_page_changed",
            {
                "old_signature": old_signature,
                "new_signature": new_signature,
            },
        )

        time.sleep(0.8)

    return pages_visited


def _navigate_to_hirecafe_ready_page(driver: Any, target_url: str) -> None:
    logger.info("hirecafe navigating to url=%s", target_url)
    driver.get(target_url)
    _observe_step(driver, "navigate_after_get", {"target_url": target_url})

    base_wait_seconds = max(0.0, float(CLOUDFLARE_WAIT_SECONDS))
    extra_initial_wait_seconds = max(0.0, HIRECAFE_INITIAL_PAGE_EXTRA_WAIT_SECONDS)
    initial_wait_seconds = base_wait_seconds + extra_initial_wait_seconds
    logger.info(
        "hirecafe waiting %ss for initial load and Cloudflare check (base=%ss extra=%ss)",
        initial_wait_seconds,
        base_wait_seconds,
        extra_initial_wait_seconds,
    )
    time.sleep(initial_wait_seconds)
    _observe_step(
        driver,
        "navigate_after_initial_wait",
        {
            "cloudflare_wait_seconds": CLOUDFLARE_WAIT_SECONDS,
            "extra_initial_wait_seconds": extra_initial_wait_seconds,
            "initial_wait_seconds": initial_wait_seconds,
        },
    )

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
        _observe_step(driver, "challenge_before_click_attempt", {"challenge": initial_challenge})
        _capture_observation_artifacts(
            driver,
            "challenge_before_click",
            {"challenge": initial_challenge},
        )

        challenge_stabilize_seconds = max(0.0, HIRECAFE_CHALLENGE_STABILIZE_SECONDS)
        if challenge_stabilize_seconds > 0:
            logger.info(
                "hirecafe waiting %ss for challenge UI stabilization before click planning",
                challenge_stabilize_seconds,
            )
            time.sleep(challenge_stabilize_seconds)
            _observe_step(
                driver,
                "challenge_after_stabilize_wait",
                {
                    "wait_seconds": challenge_stabilize_seconds,
                    "challenge": _probe_cloudflare_challenge(driver),
                },
            )

        click_plan = _resolve_cloudflare_click_plan(driver)
        logger.info(
            "hirecafe challenge click-plan strategy=%s method=%s candidates=%s checkbox_box=%s iframe_box=%s hidden_input_box=%s text_visual_box=%s text_visual_source=%s",
            HIRECAFE_CF_CLICK_STRATEGY_VERSION,
            click_plan.get("method"),
            len(click_plan.get("click_candidates") or []),
            click_plan.get("checkbox_box"),
            click_plan.get("iframe_box"),
            click_plan.get("hidden_input_box"),
            click_plan.get("text_visual_box"),
            click_plan.get("text_visual_source"),
        )
        single_click_result: dict[str, Any] = {
            "clicked": False,
            "cleared": False,
            "attempt_count": 0,
            "attempt_limit": 1,
            "attempts": [],
            "last_state": initial_challenge,
        }
        click_candidates = click_plan.get("click_candidates") or [
            {
                "rank": 1,
                "recommended": True,
                "label": "LEGACY",
                "source": "fallback_hardcoded",
                "x": HARDCODED_CF_CLICK_X,
                "y": HARDCODED_CF_CLICK_Y,
            }
        ]
        
        target = click_candidates[0]
        logger.info(
            "hirecafe challenge single-click strategy=%s method=%s selection_source=%s x=%s y=%s",
            HIRECAFE_CF_CLICK_STRATEGY_VERSION,
            click_plan.get("method"),
            target.get("source"),
            target.get("x"),
            target.get("y"),
        )
        single_click_result = _execute_cloudflare_single_click(driver, click_candidates)
        clicked = bool(single_click_result.get("clicked"))
        
        logger.info("hirecafe challenge single-click completed, sleeping 10s as requested")
        time.sleep(10.0)

        _capture_observation_artifacts(
            driver,
            "challenge_immediate_after_click",
            {
                "clicked": clicked,
                "x": HARDCODED_CF_CLICK_X,
                "y": HARDCODED_CF_CLICK_Y,
                "single_click_result": single_click_result,
            },
        )
        # Poll for Turnstile success instead of a fixed sleep.
        _ts_deadline = time.time() + 5.0
        while time.time() < _ts_deadline:
            _ts_state = _probe_cloudflare_challenge(driver)
            if _ts_state.get("turnstile_success") or not _ts_state.get("active"):
                break
            time.sleep(0.5)
        _capture_observation_artifacts(
            driver,
            "challenge_turnstile_poll_after_click",
            {
                "clicked": clicked,
                "x": HARDCODED_CF_CLICK_X,
                "y": HARDCODED_CF_CLICK_Y,
                "single_click_result": single_click_result,
            },
        )

        _observe_step(
            driver,
            "challenge_after_fallback_click",
            {
                "clicked": clicked,
                "legacy_x": HARDCODED_CF_CLICK_X,
                "legacy_y": HARDCODED_CF_CLICK_Y,
                "click_plan": click_plan,
                "single_click_result": single_click_result,
            },
        )

        _capture_observation_artifacts(
            driver,
            "challenge_after_click_attempt",
            {
                "clicked": clicked,
                "click_plan": click_plan,
                "single_click_result": single_click_result,
            },
        )

        _observe_step(
            driver,
            "challenge_before_clearance_wait",
            {"clear_timeout_seconds": CLOUDFLARE_CLEAR_TIMEOUT_SECONDS},
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
                "hirecafe cloudflare still active after click markers=%s selectors=%s",
                clear_state.get("marker_hits"),
                clear_state.get("selector_hits"),
            )
            _capture_observation_artifacts(
                driver,
                "challenge_uncleared",
                {
                    "challenge": clear_state,
                },
            )
    else:
        logger.info("hirecafe cloudflare challenge not detected")
        _observe_step(driver, "challenge_not_detected", {"challenge": initial_challenge})

    _observe_step(
        driver,
        "page_ready_wait_start",
        {
            "timeout_seconds": HIRECAFE_PAGE_READY_TIMEOUT_SECONDS,
            "poll_seconds": HIRECAFE_PAGE_READY_POLL_SECONDS,
        },
    )

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
            _observe_step(
                driver,
                "post_ready_wait_complete",
                {"post_verify_wait_seconds": POST_VERIFY_WAIT_SECONDS},
            )
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
    requested_mode, effective_mode, blocked_runtime = _resolve_hirecafe_browser_mode()

    if requested_mode != effective_mode and blocked_runtime in ("production", "docker"):
        logger.warning(
            "hirecafe requested browser mode=%s blocked in %s runtime, forcing mode=%s",
            requested_mode,
            blocked_runtime,
            effective_mode,
        )

    logger.info(
        "hirecafe launching browser requested_mode=%s effective_mode=%s "
        "max_samples=%s max_pages=%s url=%s",
        requested_mode,
        effective_mode,
        max_samples,
        page_limit,
        target_url,
    )
    driver = _launch_hirecafe_driver(browser_mode=effective_mode)
    _observe_step(
        driver,
        "driver_launched",
        {
            "requested_browser_mode": requested_mode,
            "effective_browser_mode": effective_mode,
            "mode_blocked_runtime": blocked_runtime,
            "max_samples": max_samples,
            "max_pages": page_limit,
            "target_url": target_url,
        },
    )

    try:
        _navigate_to_hirecafe_ready_page(driver, target_url)
        _observe_step(driver, "navigate_complete", {"target_url": target_url})

        job_samples: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        seen_ids: set[str] = set()

        _observe_step(
            driver,
            "pagination_scrape_start",
            {
                "max_samples": max_samples,
                "max_pages": page_limit,
            },
        )

        pages_visited = _scrape_paginated_card_pages(
            driver,
            job_samples,
            seen_urls,
            seen_ids,
            max_samples=max_samples,
            max_pages=page_limit,
        )

        _observe_step(
            driver,
            "pagination_scrape_complete",
            {
                "pages_visited": pages_visited,
                "total_jobs": len(job_samples),
            },
        )

        logger.info(
            "hirecafe scrape complete: pages_visited=%s total=%s jobs",
            pages_visited,
            len(job_samples),
        )
        _observe_step(
            driver,
            "scrape_complete",
            {
                "pages_visited": pages_visited,
                "total_jobs": len(job_samples),
            },
        )
        return job_samples
    except Exception as exc:
        _observe_step(
            driver,
            "scrape_exception",
            {
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        raise
    finally:
        _observe_step(driver, "driver_before_quit")
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
