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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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


def scrape_hirecafe_jobs(max_samples: int = 200) -> list[dict[str, Any]]:
    """
    Launch Chrome, navigate to hiring.cafe India search, scroll and intercept
    ``viewjob/*.json`` network responses to capture job payloads.

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
        logger.info("hirecafe starting scroll loop")

        scroll_delay_min = max(0.0, MIN_SCROLL_DELAY_SECONDS)
        scroll_delay_max = max(scroll_delay_min, MAX_SCROLL_DELAY_SECONDS)

        loop_start_time = time.time()
        last_progress_time = loop_start_time
        last_heartbeat_time = loop_start_time
        scroll_count = 0

        while len(job_samples) < max_samples:
            now = time.time()
            elapsed = now - loop_start_time
            idle_for = now - last_progress_time

            if elapsed >= MAX_RUNTIME_SECONDS:
                logger.warning(
                    "hirecafe stopping scroll: max runtime reached elapsed=%ss limit=%ss captured=%s",
                    int(elapsed),
                    MAX_RUNTIME_SECONDS,
                    len(job_samples),
                )
                break

            if idle_for >= MAX_IDLE_SECONDS:
                logger.warning(
                    "hirecafe stopping scroll: idle timeout reached idle=%ss limit=%ss captured=%s",
                    int(idle_for),
                    MAX_IDLE_SECONDS,
                    len(job_samples),
                )
                break

            if scroll_count >= MAX_SCROLLS:
                logger.warning(
                    "hirecafe stopping scroll: max scrolls reached scrolls=%s limit=%s captured=%s",
                    scroll_count,
                    MAX_SCROLLS,
                    len(job_samples),
                )
                break

            if now - last_heartbeat_time >= HEARTBEAT_EVERY_SECONDS:
                logger.info(
                    "hirecafe heartbeat elapsed=%ss idle=%ss scrolls=%s captured=%s/%s",
                    int(elapsed),
                    int(idle_for),
                    scroll_count,
                    len(job_samples),
                    max_samples,
                )
                last_heartbeat_time = now

            try:
                driver.execute_script("window.scrollBy(0, %d);" % SCROLL_PIXELS)
            except Exception:
                pass
            scroll_count += 1

            logs = driver.get_log("performance")
            for log_entry in logs:
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

                    req_id = message["params"]["requestId"]
                    body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
                    job_data = json.loads(body["body"])
                    job_samples.append(job_data)
                    seen_urls.add(url)
                    last_progress_time = time.time()
                    logger.debug(
                        "hirecafe captured %s/%s url=%s",
                        len(job_samples), max_samples, url.split("/")[-1][:30],
                    )
                    if len(job_samples) >= max_samples:
                        break
                except Exception:
                    pass

            if len(job_samples) >= max_samples:
                break

            time.sleep(random.uniform(scroll_delay_min, scroll_delay_max))

        logger.info("hirecafe scroll loop finished captured=%s", len(job_samples))
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
