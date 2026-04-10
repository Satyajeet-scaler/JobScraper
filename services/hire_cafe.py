"""
HireCafe scraper — captures job payloads from hiring.cafe by intercepting
network responses via Chrome DevTools Protocol using undetected-chromedriver.

Requires xvfb virtual display on headless servers (the uvicorn process should
be launched via ``xvfb-run -a``).
"""

import html
import json
import logging
import os
import random
import time
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

HIRECAFE_SEARCH_URL = (
    "https://hiring.cafe/?searchState="
    "%7B%22dateFetchedPastNDays%22%3A2%2C%22locations%22%3A%5B%7B%22id%22%3A%22lhY1yZQBoEtHp_8UEq3V%22"
    "%2C%22types%22%3A%5B%22country%22%5D%2C%22address_components%22%3A%5B%7B%22long_name%22%3A%22India%22"
    "%2C%22short_name%22%3A%22IN%22%2C%22types%22%3A%5B%22country%22%5D%7D%5D%2C%22formatted_address%22%3A%22India"
    "%22%2C%22population%22%3A1352617328%2C%22workplace_types%22%3A%5B%5D%2C%22options%22%3A%7B%22flexible_regions%22"
    "%3A%5B%22anywhere_in_continent%22%2C%22anywhere_in_world%22%5D%7D%7D%5D%2C%22jobTitleQuery%22%3A%22%5C%22developer%5C%22"
    "%2C+%5C%22data+engineer%5C%22%2C+%5C%22data+scientist%5C%22%2C+%5C%22data+analyst%5C%22%2C+%5C%22devops+engineer%5C%22"
    "%2C+%5C%22platform+engineer%5C%22%22%7D"
)

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
BOTTOM_IDLE_SCROLLS = int(os.getenv("HIRECAFE_BOTTOM_IDLE_SCROLLS", "5"))
CAROUSEL_ENABLED = os.getenv("HIRECAFE_CAROUSEL_ENABLED", "true").lower() not in ("false", "0", "no")
PRE_SCROLL_ESCAPE = os.getenv("HIRECAFE_PRE_SCROLL_ESCAPE", "true").lower() not in ("false", "0", "no")

CLOUDFLARE_MARKERS = (
    "just a moment",
    "performing security verification",
    "verify you are human",
    "cloudflare",
)


def _is_cloudflare_challenge_active(driver) -> bool:
    try:
        title = (driver.title or "").lower()
        page = (driver.page_source or "").lower()
        content = f"{title}\n{page}"
        return any(marker in content for marker in CLOUDFLARE_MARKERS)
    except Exception:
        return False


def _wait_for_cloudflare_clearance(driver, timeout_seconds: int, poll_interval_seconds: float = 1.0) -> bool:
    deadline = time.time() + max(0, timeout_seconds)
    while time.time() < deadline:
        if not _is_cloudflare_challenge_active(driver):
            return True
        time.sleep(max(0.1, poll_interval_seconds))
    return not _is_cloudflare_challenge_active(driver)


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
    try:
        driver.execute_cdp_cmd(
            "Input.dispatchMouseEvent",
            {"type": "mouseMoved", "x": x, "y": y, "button": "left", "clickCount": 1},
        )
        driver.execute_cdp_cmd(
            "Input.dispatchMouseEvent",
            {"type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1},
        )
        driver.execute_cdp_cmd(
            "Input.dispatchMouseEvent",
            {"type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1},
        )
        return True
    except Exception as exc:
        logger.info("hirecafe hardcoded click failed at (%s,%s): %s", x, y, type(exc).__name__)
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


def scrape_hirecafe_jobs(max_samples: int = 200) -> list[dict[str, Any]]:
    """
    Launch Chrome, navigate to hiring.cafe India search, scroll to the bottom
    capturing ``viewjob/*.json`` network responses (Phase 1), then expand card
    carousels bottom-to-top to pick up hidden jobs (Phase 2).

    Returns a list of raw response dicts (each has ``pageProps.job``).
    """
    logger.info("hirecafe launching undetected-chromedriver max_samples=%s", max_samples)
    options = uc.ChromeOptions()
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    is_server = os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("PORT")
    if is_server:
        logger.info("hirecafe detected server environment, using system chromium binaries")
        driver = uc.Chrome(
            options=options,
            browser_executable_path="/usr/bin/chromium",
            driver_executable_path="/usr/bin/chromedriver",
        )
    else:
        driver = uc.Chrome(options=options, version_main=145)

    try:
        logger.info("hirecafe navigating to hiring.cafe")
        driver.get(HIRECAFE_SEARCH_URL)

        logger.info("hirecafe waiting %ss for initial load and Cloudflare check", CLOUDFLARE_WAIT_SECONDS)
        time.sleep(max(0, CLOUDFLARE_WAIT_SECONDS))

        if _is_cloudflare_challenge_active(driver):
            logger.info("hirecafe detected active Cloudflare challenge")
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

            cleared = _wait_for_cloudflare_clearance(driver, CLOUDFLARE_CLEAR_TIMEOUT_SECONDS)
            if cleared:
                logger.info(
                    "hirecafe cloudflare cleared, waiting %ss for jobs payloads to load",
                    POST_VERIFY_WAIT_SECONDS,
                )
                time.sleep(max(0, POST_VERIFY_WAIT_SECONDS))
            else:
                logger.warning(
                    "hirecafe cloudflare still active after %ss, continuing guarded scraping",
                    CLOUDFLARE_CLEAR_TIMEOUT_SECONDS,
                )
        else:
            logger.info("hirecafe cloudflare challenge not detected")

        job_samples: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        seen_ids: set[str] = set()

        # =============================================================
        # PHASE 1 — Scroll to bottom, capturing viewjob JSON
        # =============================================================
        _press_escape_before_scroll(driver)
        logger.info("hirecafe phase-1: starting scroll-to-bottom")
        scroll_root = _infinite_scroll_root(driver)
        if scroll_root:
            logger.info("hirecafe phase-1: found inner scroll container")
        else:
            logger.info("hirecafe phase-1: no inner scroll container, window scroll only")

        scroll_delay_min = max(0.0, MIN_SCROLL_DELAY_SECONDS)
        scroll_delay_max = max(scroll_delay_min, MAX_SCROLL_DELAY_SECONDS)

        loop_start_time = time.time()
        last_progress_time = loop_start_time
        last_heartbeat_time = loop_start_time
        scroll_count = 0
        consecutive_idle_scrolls = 0

        while len(job_samples) < max_samples:
            now = time.time()
            elapsed = now - loop_start_time
            idle_for = now - last_progress_time

            if elapsed >= MAX_RUNTIME_SECONDS:
                logger.warning(
                    "hirecafe phase-1 stop: max runtime elapsed=%ss limit=%ss captured=%s",
                    int(elapsed), MAX_RUNTIME_SECONDS, len(job_samples),
                )
                break

            if idle_for >= MAX_IDLE_SECONDS:
                logger.warning(
                    "hirecafe phase-1 stop: idle timeout idle=%ss limit=%ss captured=%s",
                    int(idle_for), MAX_IDLE_SECONDS, len(job_samples),
                )
                break

            if scroll_count >= MAX_SCROLLS:
                logger.warning(
                    "hirecafe phase-1 stop: max scrolls=%s limit=%s captured=%s",
                    scroll_count, MAX_SCROLLS, len(job_samples),
                )
                break

            at_bottom = _is_at_bottom(driver, scroll_root)
            if at_bottom and consecutive_idle_scrolls >= BOTTOM_IDLE_SCROLLS:
                logger.info(
                    "hirecafe phase-1 stop: reached bottom "
                    "(idle_scrolls=%s, at_bottom=True) captured=%s",
                    consecutive_idle_scrolls, len(job_samples),
                )
                break

            if now - last_heartbeat_time >= HEARTBEAT_EVERY_SECONDS:
                logger.info(
                    "hirecafe phase-1 heartbeat elapsed=%ss idle=%ss scrolls=%s "
                    "captured=%s/%s at_bottom=%s idle_scrolls=%s",
                    int(elapsed), int(idle_for), scroll_count,
                    len(job_samples), max_samples, at_bottom,
                    consecutive_idle_scrolls,
                )
                last_heartbeat_time = now

            _scroll_feed_and_window(driver, scroll_root, SCROLL_PIXELS)
            scroll_count += 1

            before = len(job_samples)
            _ingest_from_performance_logs(
                driver, job_samples, seen_urls, seen_ids, max_samples,
            )
            new_this_scroll = len(job_samples) - before

            if new_this_scroll > 0:
                last_progress_time = time.time()
                consecutive_idle_scrolls = 0
            else:
                consecutive_idle_scrolls += 1

            if len(job_samples) >= max_samples:
                break

            time.sleep(random.uniform(scroll_delay_min, scroll_delay_max))

        logger.info(
            "hirecafe phase-1 finished: captured=%s scrolls=%s",
            len(job_samples), scroll_count,
        )

        # =============================================================
        # PHASE 2 — Carousel expansion, bottom-to-top
        # =============================================================
        if len(job_samples) >= max_samples:
            logger.info(
                "hirecafe phase-2 skipped: max_samples already reached (%s)",
                len(job_samples),
            )
        elif not CAROUSEL_ENABLED:
            logger.info("hirecafe phase-2 skipped: HIRECAFE_CAROUSEL_ENABLED=false")
        else:
            logger.info("hirecafe phase-2: starting carousel expansion (bottom-to-top)")
            scroll_root = scroll_root or _infinite_scroll_root(driver)
            if not scroll_root:
                logger.warning("hirecafe phase-2: no scroll root, skipping")
            else:
                cards = _grid_children_cards(scroll_root)
                cards = _dedupe_card_elements(driver, cards)
                cards.reverse()
                logger.info(
                    "hirecafe phase-2: %s card(s) to process (bottom-to-top)",
                    len(cards),
                )

                phase2_start = time.time()
                cards_processed = 0
                carousel_jobs_found = 0

                for card_idx, card in enumerate(cards):
                    if len(job_samples) >= max_samples:
                        logger.info("hirecafe phase-2: max_samples reached, stopping")
                        break
                    if time.time() - phase2_start > MAX_RUNTIME_SECONDS:
                        logger.warning("hirecafe phase-2: max runtime exceeded, stopping")
                        break

                    try:
                        if not card.is_displayed():
                            continue
                    except Exception:
                        continue

                    try:
                        has_job = card.find_elements(
                            By.CSS_SELECTOR, 'a[href*="/viewjob/"]',
                        )
                        if not has_job:
                            continue
                    except Exception:
                        continue

                    try:
                        driver.execute_script(
                            "arguments[0].scrollIntoView({behavior:'auto',block:'center'});",
                            card,
                        )
                        time.sleep(0.3)
                    except Exception:
                        pass

                    before_card = len(job_samples)
                    click_count = 0

                    while len(job_samples) < max_samples:
                        if not _click_carousel_next(driver, card):
                            break
                        click_count += 1
                        time.sleep(CAROUSEL_CLICK_DELAY)

                        new_from_logs = _ingest_from_performance_logs(
                            driver, job_samples, seen_urls, seen_ids, max_samples,
                        )
                        new_from_dom = _fetch_missing_jobs_via_dom(
                            driver, card, job_samples, seen_ids,
                            seen_urls, max_samples,
                        )

                        if new_from_logs == 0 and new_from_dom == 0:
                            time.sleep(CAROUSEL_CLICK_DELAY * 0.5)
                            extra_logs = _ingest_from_performance_logs(
                                driver, job_samples, seen_urls, seen_ids,
                                max_samples,
                            )
                            extra_dom = _fetch_missing_jobs_via_dom(
                                driver, card, job_samples, seen_ids,
                                seen_urls, max_samples,
                            )
                            if extra_logs == 0 and extra_dom == 0:
                                break

                    card_new = len(job_samples) - before_card
                    carousel_jobs_found += card_new
                    cards_processed += 1
                    if click_count > 0:
                        logger.info(
                            "hirecafe phase-2: card %s/%s — %s click(s), "
                            "%s new job(s), total=%s/%s",
                            card_idx + 1, len(cards), click_count,
                            card_new, len(job_samples), max_samples,
                        )

                logger.info(
                    "hirecafe phase-2 finished: processed=%s cards, "
                    "found=%s new jobs via carousel, total=%s",
                    cards_processed, carousel_jobs_found, len(job_samples),
                )

        logger.info("hirecafe scrape complete: total=%s jobs", len(job_samples))
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
