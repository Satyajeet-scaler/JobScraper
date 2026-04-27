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

from playwright.sync_api import Page, ElementHandle, Response
from camoufox.sync_api import Camoufox
from bs4 import BeautifulSoup

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

HARDCODED_CF_CLICK_X = int(os.getenv("HIRECAFE_CF_CLICK_X", "340"))
HARDCODED_CF_CLICK_Y = int(os.getenv("HIRECAFE_CF_CLICK_Y", "334"))

SCROLL_PIXELS = int(os.getenv("HIRECAFE_SCROLL_PIXELS", "1200"))
MIN_SCROLL_DELAY_SECONDS = float(os.getenv("HIRECAFE_MIN_SCROLL_DELAY_SECONDS", "0.7"))
MAX_SCROLL_DELAY_SECONDS = float(os.getenv("HIRECAFE_MAX_SCROLL_DELAY_SECONDS", "1.8"))

MAX_RUNTIME_SECONDS = int(os.getenv("HIRECAFE_MAX_RUNTIME_SECONDS", "300"))
MAX_IDLE_SECONDS = int(os.getenv("HIRECAFE_MAX_IDLE_SECONDS", "90"))
MAX_SCROLLS = int(os.getenv("HIRECAFE_MAX_SCROLLS", "500"))
HEARTBEAT_EVERY_SECONDS = int(os.getenv("HIRECAFE_HEARTBEAT_EVERY_SECONDS", "15"))

CAROUSEL_CLICK_DELAY = float(os.getenv("HIRECAFE_CAROUSEL_CLICK_DELAY", "0.5"))
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


def _probe_cloudflare_challenge(page: Page) -> dict[str, Any]:
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
        state["url"] = page.url or ""
    except Exception:
        pass

    try:
        title = (page.title() or "").strip()
        html_content = (page.content() or "")
        page_lower = html_content.lower()
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
        selector_hits = page.evaluate(
            """
            () => {
              const bodyText = ((document.body && document.body.innerText) || '').toLowerCase();
              return {
                cf_iframe: !!document.querySelector("iframe[src*='challenges.cloudflare.com']"),
                cf_turnstile: !!document.querySelector("input[name='cf-turnstile-response'], div.cf-turnstile"),
                cf_turnstile_input: !!document.querySelector("input[name='cf-turnstile-response'], input[id^='cf-chl-widget'][id$='_response']"),
                cf_turnstile_widget: !!document.querySelector("div.cf-turnstile"),
                cf_challenge_form: !!document.querySelector("form#challenge-form, #challenge-stage"),
                cf_challenge_platform_script: !!document.querySelector("script[src*='/cdn-cgi/challenge-platform/']"),
                cf_turnstile_script: !!document.querySelector("script[src*='challenges.cloudflare.com/turnstile']"),
                cf_challenge_state_nodes: !!document.querySelector("#challenge-success-text, #challenge-error-text, .loading-verifying, .ray-id"),
                cf_meta_refresh: !!document.querySelector("meta[http-equiv='refresh'][content]"),
                cf_ray_id_text: /ray id:/.test(bodyText),
                cf_verify_copy: /performing security verification|this website uses a security service|just a moment|security check|verify you are human|checking your browser/.test(bodyText),
                cf_challenge_text: /just a moment|security check|verify you are human|performing security verification|checking your browser/.test(bodyText)
              };
            }
            """
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


def _is_cloudflare_challenge_active(page: Page) -> bool:
    return bool(_probe_cloudflare_challenge(page).get("active"))


def _wait_for_cloudflare_clearance(
    page: Page,
    timeout_seconds: int,
    poll_interval_seconds: float = 1.0,
) -> tuple[bool, dict[str, Any]]:
    deadline = time.time() + max(0, timeout_seconds)
    last_state = _probe_cloudflare_challenge(page)
    
    # Track when we last attempted a manual nudge
    last_nudge_time = 0.0
    
    while time.time() < deadline:
        last_state = _probe_cloudflare_challenge(page)
        if not last_state.get("active"):
            return True, last_state
            
        # If it's been more than 8 seconds since landing or last nudge, try a manual click nudge
        if time.time() - last_nudge_time > 8.0:
            _attempt_turnstile_manual_nudge(page)
            last_nudge_time = time.time()

        time.sleep(max(0.1, poll_interval_seconds))
    
    last_state = _probe_cloudflare_challenge(page)
    return (not last_state.get("active")), last_state


def _attempt_turnstile_manual_nudge(page: Page) -> bool:
    """Detect and click the Turnstile checkbox using cross-frame JavaScript."""
    try:
        # Check every frame for the checkbox
        all_frames = page.frames
        logger.info("hirecafe turnstile nudge: checking %s frames", len(all_frames))
        
        for frame in all_frames:
            try:
                # This script returns the bounding box of the checkbox if found inside the frame
                box = frame.evaluate("""
                    () => {
                        const selectors = [
                            '.ctp-checkbox-label',
                            '.ctp-checkbox-container',
                            '#checkbox',
                            '[type="checkbox"]',
                            '#challenge-stage div'
                        ];
                        for (const selector of selectors) {
                            const el = document.querySelector(selector);
                            if (el && el.offsetWidth > 0 && el.offsetHeight > 0) {
                                const rect = el.getBoundingClientRect();
                                return {
                                    x: rect.left,
                                    y: rect.top,
                                    width: rect.width,
                                    height: rect.height,
                                    found: true,
                                    selector: selector
                                };
                            }
                        }
                        return null;
                    }
                """)
                
                if box and box.get("found"):
                    iframe_handle = frame.frame_element()
                    if not iframe_handle:
                        # Could be the main frame which doesn't have an iframe_handle
                        cx = box["x"] + box["width"] / 2 + random.uniform(-3, 3)
                        cy = box["y"] + box["height"] / 2 + random.uniform(-3, 3)
                    else:
                        iframe_box = iframe_handle.bounding_box()
                        if not iframe_box:
                            continue
                        cx = iframe_box["x"] + box["x"] + box["width"] / 2 + random.uniform(-3, 3)
                        cy = iframe_box["y"] + box["y"] + box["height"] / 2 + random.uniform(-3, 3)
                    
                    logger.info("hirecafe turnstile nudge: found %s in frame %s", box["selector"], frame.url)
                    page.mouse.click(cx, cy)
                    logger.info("hirecafe turnstile nudge: clicked coordinates (%s, %s)", int(cx), int(cy))
                    
                    time.sleep(1.0)
                    _capture_observation_artifacts(page, "post_turnstile_nudge")
                    return True
            except Exception:
                continue
                
        # Fallback: Hardcoded Click
        viewport = page.viewport_size
        logger.info("hirecafe turnstile nudge: JS finding failed. Viewport: %s. Trying hardcoded fallback: (%s, %s)", 
                    viewport, HARDCODED_CF_CLICK_X, HARDCODED_CF_CLICK_Y)
        
        # Click with a tiny bit of jitter even for hardcoded
        cx = HARDCODED_CF_CLICK_X + random.uniform(-2, 2)
        cy = HARDCODED_CF_CLICK_Y + random.uniform(-2, 2)
        
        page.mouse.click(cx, cy)
        logger.info("hirecafe turnstile nudge: clicked hardcoded coordinates (%s, %s)", int(cx), int(cy))
        
        # Inject a visual marker so we can see the click location in the screenshot
        marker_js = f"""
            (() => {{
                const dot = document.createElement('div');
                dot.id = 'debug-click-marker';
                dot.style.position = 'fixed';
                dot.style.left = '{cx - 5}px';
                dot.style.top = '{cy - 5}px';
                dot.style.width = '10px';
                dot.style.height = '10px';
                dot.style.backgroundColor = 'red';
                dot.style.borderRadius = '50%';
                dot.style.zIndex = '999999';
                dot.style.pointerEvents = 'none';
                dot.style.border = '2px solid white';
                document.body.appendChild(dot);
            }})()
        """
        try:
            page.evaluate(marker_js)
        except:
            pass

        # Immediate verification
        time.sleep(1.0)
        _capture_observation_artifacts(page, "post_turnstile_nudge_hardcoded_1s")
        
        # Delayed verification (to see if it clears)
        time.sleep(2.0)
        _capture_observation_artifacts(page, "post_turnstile_nudge_hardcoded_3s")
        
        # Cleanup marker (optional, but good practice)
        try:
            page.evaluate("() => { const d = document.getElementById('debug-click-marker'); if(d) d.remove(); }")
        except:
            pass
            
        return True

    except Exception as exc:
        logger.info("hirecafe turnstile nudge error: %s", exc)
    return False


def _get_camoufox_config() -> dict[str, Any]:
    """Camoufox config with anti-detection and optional proxy."""
    return {
        "headless": os.getenv("HIRECAFE_HEADLESS", "false").lower() == "true",
        "humanize": True,
        "browser": "firefox",
    }


def _capture_observation_artifacts(
    page: Page,
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
            page.screenshot(path=str(screenshot_path))
            screenshot_ok = True
        except Exception:
            screenshot_ok = False

        try:
            html_path.write_text(page.content() or "", encoding="utf-8")
        except Exception:
            pass

        metadata = {
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "stage": stage,
            "screenshot_ok": screenshot_ok,
            "screenshot_path": str(screenshot_path),
            "html_path": str(html_path),
            "url": page.url or "",
            "title": page.title() or "",
            "extra": extra or {},
        }
        meta_path.write_text(
            json.dumps(metadata, ensure_ascii=True, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        logger.info("hirecafe observe stage=%s wrote %s", stage, meta_path)
    except Exception as exc:
        logger.info("hirecafe observe capture failed stage=%s: %s", stage, type(exc).__name__)


def _press_escape_before_scroll(page: Page) -> None:
    """Dismiss overlays / blur focused inputs so the feed receives scroll events."""
    if not PRE_SCROLL_ESCAPE:
        return
    try:
        page.keyboard.press("Escape")
        time.sleep(0.05)
    except Exception:
        pass


def _click_viewport_coordinate(page: Page, x: int, y: int) -> bool:
    """Click at viewport coordinates using Camoufox's human-like movement."""
    try:
        page.mouse.click(x, y)
        return True
    except Exception as exc:
        logger.debug("hirecafe click coordinate failed: %s", exc)
        return False


def _extract_viewjob_id(url_or_href: str) -> str | None:
    """Extract the job slug/ID from a viewjob URL or href path."""
    if "viewjob/" not in url_or_href:
        return None
    slug = url_or_href.split("viewjob/")[-1]
    slug = slug.split("?")[0].split("#")[0]
    if slug.endswith(".json"):
        slug = slug[:-5]
    return slug.strip("/") or None


def _probe_cloudflare_challenge(page: Page) -> dict[str, Any]:
    """Check for Cloudflare/Turnstile presence markers."""
    try:
        content = (page.content() or "").lower()
        marker_hits = [m for m in CLOUDFLARE_MARKERS if m in content]
        
        # Also check for iframe
        iframe_found = page.query_selector("iframe[src*='cloudflare']") is not None
        
        return {
            "active": len(marker_hits) > 0 or iframe_found,
            "marker_hits": marker_hits,
            "iframe_found": iframe_found
        }
    except Exception:
        return {"active": False, "marker_hits": [], "iframe_found": False}


def _wait_for_cloudflare_clearance(page: Page, timeout: int) -> tuple[bool, dict[str, Any]]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        res = _probe_cloudflare_challenge(page)
        if not res["active"]:
            return True, res
        time.sleep(1.0)
    return False, _probe_cloudflare_challenge(page)


def _wait_for_hiring_cafe_page_ready(page: Page, timeout: int) -> tuple[bool, dict[str, Any]]:
    """Wait for core Hiring.Cafe UI elements to appear."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        # Check for card grid or specific Next.js markers
        grid = page.query_selector("div[class*='grid'], main")
        has_next_data = page.query_selector("#__NEXT_DATA__") is not None
        
        if grid and has_next_data:
            return True, {"reason": "elements_found", "has_next_data": has_next_data}
        time.sleep(1.0)
    return False, {"reason": "timeout"}


def _fetch_missing_jobs_via_dom(
    page: Page,
    card: ElementHandle,
    job_samples: list[dict[str, Any]],
    seen_ids: set[str],
    seen_urls: set[str],
    max_samples: int,
) -> int:
    """Fetch viewjob JSON for hrefs visible in a card's DOM but not yet captured.
    Uses in-page fetch() via page.evaluate as a fallback."""
    new_count: int = 0
    try:
        hrefs = _viewjob_hrefs_in_card(page, card)
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
        build_id = page.evaluate(
            "() => { try { return JSON.parse(document.getElementById('__NEXT_DATA__').textContent).buildId; } catch(e) { return null; } }"
        )
    except Exception:
        build_id = None

    if not build_id:
        return 0

    for jid in missing_ids:
        if len(job_samples) >= max_samples:
            break
        
        path = f"/_next/data/{build_id}/viewjob/{jid}.json"
        try:
            # Execute fetch in-page
            result_json = page.evaluate(
                f"(url) => fetch(url, {{credentials:'include'}}).then(r => r.ok ? r.json() : null).then(d => d ? JSON.stringify(d) : null)",
                path
            )
            if not result_json:
                continue
                
            job_data = json.loads(result_json)
            if not job_data.get("pageProps"):
                continue
                
            job_samples.append(job_data)
            seen_ids.add(jid)
            seen_urls.add(f"https://hiring.cafe{path}")
            new_count += 1
            logger.debug("DOM-fetch captured id=%s", jid)
        except Exception:
            pass
            
    return new_count


def _collect_jobs_for_card_until_no_new(
    page: Page,
    card: ElementHandle,
    job_samples: list[dict[str, Any]],
    seen_ids: set[str],
    seen_urls: set[str],
    *,
    max_samples: int,
) -> int:
    """
    Capture card jobs from current state, then click carousel next until no new job appears.
    Relies on response interception in the main loop to populate job_samples.
    """
    before_card = len(job_samples)
    
    # Trigger initial capture via DOM nudge
    _fetch_missing_jobs_via_dom(page, card, job_samples, seen_ids, seen_urls, max_samples)

    try:
        seen_card_hrefs = set(_viewjob_hrefs_in_card(page, card))
    except Exception:
        seen_card_hrefs = set()

    click_count: int = 0
    while len(job_samples) < max_samples:
        if click_count >= PHASE2_MAX_CAROUSEL_CLICKS:
            break

        if not _click_carousel_next(page, card):
            break

        click_count += 1
        time.sleep(max(0.0, CAROUSEL_CLICK_DELAY))

        before_click = len(job_samples)
        
        # Nudge DOM again
        _fetch_missing_jobs_via_dom(page, card, job_samples, seen_ids, seen_urls, max_samples)

        try:
            current_hrefs = set(_viewjob_hrefs_in_card(page, card))
        except Exception:
            current_hrefs = set()

        newly_visible_hrefs = current_hrefs - seen_card_hrefs
        seen_card_hrefs.update(current_hrefs)
        new_jobs_this_click = len(job_samples) - before_click

        if new_jobs_this_click <= 0 and not newly_visible_hrefs:
            break

    return len(job_samples) - before_card


def _find_card_grid_root(page: Page) -> tuple[ElementHandle | None, list[ElementHandle], dict[str, Any]]:
    """
    Search for the main job results container.
    Returns (root_element, card_elements, metadata).
    """
    candidates = page.query_selector_all("div[class*='grid-cols-1']")
    best_root = None
    best_cards: list[ElementHandle] = []
    best_score = -1
    evaluated: int = 0

    for node in candidates:
        try:
            cls = (node.evaluate("el => el.className") or "").strip()
            if "grid" not in cls:
                continue

            direct_cards = _grid_children_cards(node)
            if len(direct_cards) < 2:
                continue

            total_links = len(node.query_selector_all("a[href*='/viewjob/']"))
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
        best_cards = _dedupe_card_elements(page, best_cards)
    except Exception:
        pass

    return best_root, best_cards, {
        "candidate_count": len(candidates),
        "evaluated_candidates": evaluated,
        "selected_score": best_score,
        "selected_card_count": len(best_cards),
    }


def _scroll_to_bottom(page: Page) -> None:
    """Scroll to the bottom of the page in steps to ensure all content is loaded."""
    last_height = -1
    stable_count = 0

    for _ in range(HIRECAFE_PAGINATION_BOTTOM_SCROLL_STEPS):
        try:
            height = page.evaluate(
                "Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)"
            )
        except Exception:
            break

        try:
            page.evaluate(f"window.scrollTo(0, {height})")
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


def _inspect_pagination_component(page: Page) -> dict[str, Any]:
    try:
        data = page.evaluate(
            """
            () => {
                const norm=(s)=>String(s||'').replace(/\\s+/g,' ').trim();
                const docH=Math.max(document.body.scrollHeight||0,document.documentElement.scrollHeight||0);
                const nodes=[...document.querySelectorAll('nav, footer, div')];
                const cands=[];
                for(const el of nodes){
                  const text=norm(el.innerText);
                  if(!text) continue;
                  const controls=[...el.querySelectorAll('button, a, [role="button"]')];
                  if(controls.length<1) continue;
                  const mapped=controls.map((b,i)=>({
                    index:i,
                    text:norm(b.innerText),
                    aria:norm(b.getAttribute('aria-label')),
                    title:norm(b.getAttribute('title')),
                    className:norm(b.className),
                    tag:(b.tagName||'').toLowerCase(),
                    ariaCurrent:norm(b.getAttribute('aria-current')).toLowerCase(),
                    ariaDisabled:norm(b.getAttribute('aria-disabled')).toLowerCase(),
                    disabled:!!b.disabled||norm(b.getAttribute('aria-disabled')).toLowerCase()==='true'
                  }));
                  const numericButtons=mapped.filter(b=>/^\\d+$/.test(b.text)).length;
                  const hasPage=/page\\s*\\d+\\s*of\\s*\\d+/i.test(text);
                  const hasNext=mapped.some(b=>{const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();return (meta.includes('next')||meta.includes('→')||meta.includes('›')||meta.includes('»'))&&!meta.includes('dark mode');});
                  const hasPrev=mapped.some(b=>{const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();return (meta.includes('prev')||meta.includes('previous')||meta.includes('←')||meta.includes('‹')||meta.includes('«'));});
                  const filterChipCopy=/departments|salary|commitment|experience|job titles|benefits|encouraged to apply/i.test(text);
                  const cls=String(el.className||'');
                  const likely=/border-t|bg-gray-50|pagination|justify-center|bottom/i.test(cls);
                  const rect=el.getBoundingClientRect();
                  const absTop=rect.top+window.scrollY;
                  const nearBottom=absTop>(docH*0.45);
                  if(filterChipCopy && !hasPage && numericButtons===0) continue;
                  if(!(hasPage || numericButtons>=2 || (hasNext && numericButtons>=1))) continue;
                  if(!nearBottom && !hasPage && numericButtons<3) continue;
                  const score=(hasPage?120:0)+(nearBottom?40:0)+(numericButtons*8)+(hasNext?25:0)+(hasPrev?10:0)+(likely?10:0)-(filterChipCopy?35:0);
                  cands.push({el:el,text:text,cls:cls,controls:mapped,hasPage:hasPage,score:score,absTop:absTop});
                }
                if(!cands.length)return {found:false};
                cands.sort((a,b)=>(b.score-a.score)||(b.absTop-a.absTop));
                const top=cands[0];
                const root=top.el;
                const controls=[...root.querySelectorAll('button, a, [role="button"]')];
                const buttons = controls.map((b,i)=>({
                  index:i,text:norm(b.innerText),aria:norm(b.getAttribute('aria-label')),title:norm(b.getAttribute('title')),
                  className:norm(b.className),tag:(b.tagName||'').toLowerCase(),
                  ariaCurrent:norm(b.getAttribute('aria-current')).toLowerCase(),
                  ariaDisabled:norm(b.getAttribute('aria-disabled')).toLowerCase(),
                  disabled:!!b.disabled||norm(b.getAttribute('aria-disabled')).toLowerCase()==='true'
                }));
                const m=/page\\s*(\\d+)\\s*of\\s*(\\d+)/i.exec(top.text);
                let currentPage=m?parseInt(m[1],10):null;
                if(currentPage===null){
                  const activeNumeric=buttons.find(b=>/^\\d+$/.test(b.text)&&(b.ariaCurrent==='page'||/active|selected|current/i.test(b.className)));
                  if(activeNumeric) currentPage=parseInt(activeNumeric.text,10);
                }
                let totalPages=m?parseInt(m[2],10):null;
                if(totalPages===null){
                  const nums=buttons.filter(b=>/^\\d+$/.test(b.text)).map(b=>parseInt(b.text,10)).filter(n=>Number.isFinite(n));
                  if(nums.length) totalPages=Math.max(...nums);
                }
                let nextIndex=-1;
                for(const b of buttons){
                  const meta=(b.text+' '+b.aria+' '+b.title).toLowerCase();
                  if((meta.includes('next')||meta.includes('→')||meta.includes('›')||meta.includes('»'))&&!meta.includes('dark mode')){
                    nextIndex=b.index;break;
                  }
                }
                if(nextIndex===-1&&currentPage!==null){
                  const candidates=buttons.filter(b=>!b.disabled&&/^\\d+$/.test(b.text)).map(b=>({i:b.index,n:parseInt(b.text,10)})).filter(x=>x.n>currentPage);
                  if(candidates.length){candidates.sort((a,b)=>a.n-b.n);nextIndex=candidates[0].i;}
                }
                return {
                  found:true,
                  text:top.text,
                  className:top.cls,
                  selectedScore:top.score,
                  selectedAbsTop:top.absTop,
                  buttonCount:buttons.length,
                  buttons:buttons,
                  currentPage:currentPage,
                  totalPages:totalPages,
                  nextButtonIndex:nextIndex
                };
            }
            """
        )
        return data or {"found": False}
    except Exception as exc:
        return {"found": False, "error": str(exc)}


def _click_next_pagination(page: Page) -> dict[str, Any]:
    try:
        # We reuse the logic from _inspect_pagination_component or click directly
        comp = _inspect_pagination_component(page)
        if not comp or not comp.get("found"):
            return {"status": "component_not_found"}

        buttons = comp.get("buttons", [])
        next_idx = comp.get("nextButtonIndex", -1)
        
        if next_idx == -1:
            return {"status": "next_button_not_found"}

        # Click via JS for reliability
        clicked = page.evaluate(
            """
            (idx) => {
                const norm=(s)=>String(s||'').replace(/\\s+/g,' ').trim();
                const nodes=[...document.querySelectorAll('nav, footer, div')];
                const cands=[];
                for(const el of nodes){
                  const text=norm(el.innerText);
                  if(!text) continue;
                  const controls=[...el.querySelectorAll('button, a, [role="button"]')];
                  if(controls.length<1) continue;
                  if (controls[idx]) {
                      controls[idx].scrollIntoView({behavior:'auto',block:'center'});
                      controls[idx].click();
                      return true;
                  }
                }
                return false;
            }
            """,
            next_idx
        )
        return {"clicked": True, "index": next_idx} if clicked else {"clicked": False, "reason": "js_click_failed"}
    except Exception as exc:
        return {"clicked": False, "reason": "exception", "error": str(exc)}


def _page_signature(page: Page, cards: list[ElementHandle]) -> str:
    """Generate a stable signature for the current page content to detect transitions."""
    parts: list[str] = []
    for card in cards[:8]:
        try:
            hrefs = sorted(_viewjob_hrefs_in_card(page, card))
            if hrefs:
                parts.append(hrefs[0])
        except Exception:
            continue
    if parts:
        return "|".join(parts)

    try:
        pag = _inspect_pagination_component(page)
        txt = str(pag.get("text", ""))
        return txt.strip()
    except Exception:
        return ""


def _wait_for_page_change(
    page: Page,
    old_signature: str,
    timeout_seconds: int,
) -> tuple[bool, str]:
    deadline = time.time() + max(1, timeout_seconds)
    latest = old_signature
    while time.time() < deadline:
        root, cards, _ = _find_card_grid_root(page)
        if root:
            latest = _page_signature(page, cards)
            if latest and latest != old_signature:
                return True, latest
        time.sleep(0.6)
    return False, latest


def _click_card_surface(page: Page, card: ElementHandle) -> bool:
    """Attempt to click the card surface using JS event dispatch."""
    try:
        clicked = page.evaluate(
            """
            (card) => {
                const btn=card.querySelector('button:not([disabled])');
                if(btn){btn.click();return true;}
                card.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,view:window}));
                return true;
            }
            """,
            card
        )
        return bool(clicked)
    except Exception:
        try:
            card.click()
            return True
        except Exception:
            return False


def _process_cards_on_page(
    page: Page,
    cards: list[ElementHandle],
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
            if not card.is_visible():
                continue
        except Exception:
            continue

        processed += 1

        try:
            card.scroll_into_view_if_needed()
            time.sleep(0.2)
        except Exception:
            pass

        _click_card_surface(page, card)
        time.sleep(max(0.0, HIRECAFE_CARD_CLICK_PAUSE_SECONDS))

        card_result = _collect_jobs_for_card_until_no_new(
            page,
            card,
            job_samples,
            seen_ids,
            seen_urls,
            max_samples=max_samples,
        )
        page_new_jobs += card_result

    return {
        "cards_seen": len(cards),
        "cards_processed": processed,
        "new_jobs": page_new_jobs,
    }


def _scrape_paginated_card_pages(
    page: Page,
    job_samples: list[dict[str, Any]],
    seen_urls: set[str],
    seen_ids: set[str],
    *,
    max_samples: int,
    max_pages: int,
) -> int:
    pages_visited: int = 0
    visited_signatures: set[str] = set()

    for loop_idx in range(1, max_pages + 1):
        if len(job_samples) >= max_samples:
            logger.info("hirecafe stop: max_samples reached")
            break

        root, cards, grid_meta = _find_card_grid_root(page)
        pagination_meta = _inspect_pagination_component(page)

        if not root or not cards:
            logger.warning(
                "hirecafe stop: card grid not found loop=%s candidates=%s pagination_found=%s",
                loop_idx,
                grid_meta.get("candidate_count"),
                pagination_meta.get("found"),
            )
            break

        signature = _page_signature(page, cards)
        if signature and signature in visited_signatures:
            logger.info("hirecafe stop: repeated page signature")
            break
        if signature:
            visited_signatures.add(signature)

        page_no = int(pagination_meta.get("currentPage") or loop_idx)
        total_pages = pagination_meta.get("totalPages")

        before_page = len(job_samples)
        card_metrics = _process_cards_on_page(
            page,
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
        _scroll_to_bottom(page)
        click_next = _click_next_pagination(page)
        if not click_next.get("clicked"):
            logger.info(
                "hirecafe stop: next-page click failed %s",
                click_next
            )
            break

        time.sleep(0.8)

    return pages_visited


def _navigate_to_hirecafe_ready_page(page: Page, target_url: str) -> None:
    logger.info("hirecafe navigating to url=%s", target_url)
    page.goto(target_url, wait_until="networkidle")

    logger.info("hirecafe waiting %ss for initial load and Cloudflare check", CLOUDFLARE_WAIT_SECONDS)
    time.sleep(max(0, CLOUDFLARE_WAIT_SECONDS))

    initial_challenge = _probe_cloudflare_challenge(page)
    logger.info("hirecafe initial challenge probe active=%s markers=%s url=%s", 
                initial_challenge.get("active"), initial_challenge.get("marker_hits"), page.url)
    
    _capture_observation_artifacts(page, "initial_landing", {"challenge": initial_challenge, "target_url": target_url})

    if initial_challenge.get("active"):
        logger.info("hirecafe detected active Cloudflare challenge. Camoufox should handle it, but we can nudge.")
        _attempt_turnstile_manual_nudge(page)

        cleared, clear_state = _wait_for_cloudflare_clearance(page, CLOUDFLARE_CLEAR_TIMEOUT_SECONDS)
        if cleared:
            logger.info("hirecafe cloudflare cleared")
        else:
            logger.warning("hirecafe cloudflare still active")

    page_ready, page_state = _wait_for_hiring_cafe_page_ready(page, HIRECAFE_PAGE_READY_TIMEOUT_SECONDS)
    if page_ready:
        logger.info("hirecafe page readiness verified")
        if POST_VERIFY_WAIT_SECONDS > 0:
            time.sleep(POST_VERIFY_WAIT_SECONDS)
    else:
        logger.warning("hirecafe page readiness not verified: %s", page_state.get("reason"))


def scrape_hirecafe_jobs(
    max_samples: int = 200,
    search_url: str | None = None,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    """
    Launch Camoufox, navigate to a HireCafe search URL, then scrape jobs.
    Uses Playwright response interception for data capture.
    """
    target_url = (search_url or "").strip() or HIRECAFE_SEARCH_URL
    page_limit = max_pages if max_pages is not None else HIRECAFE_MAX_PAGES
    page_limit = max(1, int(page_limit))

    job_samples: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    seen_ids: set[str] = set()

    with Camoufox(headless=HIRECAFE_BROWSER_MODE == "headless") as browser:
        page = browser.new_page()
        
        # Intercept job JSON data
        def on_response(response: Response):
            url = response.url
            if "viewjob/" in url and ".json" in url and response.status == 200:
                if url not in seen_urls:
                    try:
                        data = response.json()
                        jid = _extract_viewjob_id(url)
                        if jid and jid not in seen_ids:
                            job_samples.append(data)
                            seen_urls.add(url)
                            seen_ids.add(jid)
                            logger.debug("Captured job: %s", jid)
                    except Exception:
                        pass

        page.on("response", on_response)

        try:
            _navigate_to_hirecafe_ready_page(page, target_url)
            
            pages_visited = _scrape_paginated_card_pages(
                page,
                job_samples,
                seen_urls,
                seen_ids,
                max_samples=max_samples,
                max_pages=page_limit,
            )

            logger.info("hirecafe scrape complete: pages_visited=%s total=%s jobs",
                        pages_visited, len(job_samples))
            return job_samples
        except Exception as e:
            logger.error("Scrape failed: %s", e, exc_info=True)
            return job_samples


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
