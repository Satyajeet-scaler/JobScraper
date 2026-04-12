"""
Hirist.tech scraper — captures job list JSON via Chrome performance logs
(undetected-chromedriver). On headless servers use xvfb (see hire_cafe module).
"""

import base64
import json
import os
import random
import re
import subprocess
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any

import requests
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

_DEFAULT_TARGET_URLS = [
    "https://www.hirist.tech/c/data-analytics-bi-jobs?ref=topnavigation",
    "https://www.hirist.tech/k/it-operations-jobs?ref=topnavigation",
    "https://www.hirist.tech/c/backend-development-jobs?ref=topnavigation",
    "https://www.hirist.tech/c/ai-ml-jobs?ref=topnavigation",
]


def _target_urls_from_env() -> list[str]:
    raw = os.getenv("HIRIST_TARGET_URLS", "").strip()
    if not raw:
        return list(_DEFAULT_TARGET_URLS)
    parts: list[str] = []
    for line in raw.replace("\n", ",").split(","):
        u = line.strip()
        if u:
            parts.append(u)
    return parts if parts else list(_DEFAULT_TARGET_URLS)


def _default_output_dir() -> str:
    return os.getenv("HIRIST_OUTPUT_DIR", "data/hirist")


def _format_location(locations: Any) -> str | None:
    if locations is None:
        return None
    if isinstance(locations, str):
        return locations.strip() or None
    if isinstance(locations, list):
        out: list[str] = []
        for item in locations:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
            elif isinstance(item, dict):
                name = item.get("name") or item.get("locationName") or item.get("city")
                if isinstance(name, str) and name.strip():
                    out.append(name.strip())
        return ", ".join(out) if out else None
    return str(locations)


def _experience_string(min_y: Any, max_y: Any) -> str | None:
    if min_y is None and max_y is None:
        return None
    try:
        mn = int(min_y) if min_y is not None else None
    except (TypeError, ValueError):
        mn = None
    try:
        mx = int(max_y) if max_y is not None else None
    except (TypeError, ValueError):
        mx = None
    if mn is not None and mx is not None:
        return f"{mn}-{mx}" if mn != mx else str(mn)
    if mn is not None:
        return str(mn)
    if mx is not None:
        return str(mx)
    return None


def _date_posted_iso(card: dict[str, Any]) -> Any:
    ms = card.get("createdTimeMs")
    if isinstance(ms, (int, float)) and ms > 0:
        sec = ms / 1000.0 if ms >= 100_000_000_000 else float(ms)
        return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
    ct = card.get("createdTime")
    if isinstance(ct, (int, float)) and ct > 0:
        sec = float(ct) / 1000.0 if ct >= 100_000_000_000 else float(ct)
        return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
    if isinstance(ct, str) and ct.strip():
        return ct.strip()
    return None


def normalize_hirist_item(card: dict[str, Any]) -> dict[str, Any]:
    """Map a Hirist job card (from recent_jobs) into the pipeline pre-normalize shape."""
    job_url = str(card.get("jobDetailUrl") or "").strip()
    title = card.get("title") or card.get("jobdesignation")
    company = card.get("companyName")
    location = _format_location(card.get("locations"))
    description = card.get("jobDescription")
    if isinstance(description, str):
        description = description.strip() or None

    tags = card.get("tags")
    job_type = None
    if isinstance(tags, list) and tags:
        job_type = ", ".join(str(t) for t in tags if t)

    return {
        "site": "hirist.tech",
        "title": title,
        "company": company,
        "location": location,
        "job_url": job_url or None,
        "description": description,
        "date_posted": _date_posted_iso(card),
        "experience": _experience_string(card.get("minExperienceYears"), card.get("maxExperienceYears")),
        "salary": None,
        "job_type": job_type,
        "raw_payload": card,
    }


class HiristTechService:
    TARGET_URLS = _DEFAULT_TARGET_URLS
    HUMAN_MIN_DELAY_SECONDS = 1.0
    HUMAN_MAX_DELAY_SECONDS = 2.0
    HUMAN_LONG_PAUSE_CHANCE = 0.15
    JOB_LIST_API_HINTS = (
        "gladiator.hirist.tech/job/category/",
        "gladiator.hirist.tech/job/keyword/",
    )
    HTTP_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }

    @staticmethod
    def _is_railway() -> bool:
        return bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("PORT"))

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _human_pause(min_seconds: float = 1.0, max_seconds: float = 2.0) -> None:
        if min_seconds < 0:
            min_seconds = 0
        if max_seconds < min_seconds:
            max_seconds = min_seconds

        time.sleep(random.uniform(min_seconds, max_seconds))

    @staticmethod
    def _human_scroll_pixels() -> int:
        return random.randint(900, 1700)

    @staticmethod
    def _detect_chrome_major_version() -> int | None:
        override = os.getenv("CHROME_VERSION_MAIN")
        if override and override.isdigit():
            return int(override)

        candidates = [
            ["/usr/bin/chromium", "--version"],
            ["chromium", "--version"],
            ["google-chrome", "--version"],
            ["google-chrome-stable", "--version"],
        ]

        for cmd in candidates:
            try:
                out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True).strip()
                match = re.search(r"(\d+)\.\d+\.\d+\.\d+", out)
                if match:
                    return int(match.group(1))
            except Exception:
                continue

        return None

    @staticmethod
    def _build_driver(headless: bool) -> uc.Chrome:
        options = uc.ChromeOptions()
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        options.add_argument("--window-size=1440,2200")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")

        if headless:
            options.add_argument("--headless=new")

        version_main = HiristTechService._detect_chrome_major_version()
        chrome_kwargs: dict[str, Any] = {"options": options}
        if version_main:
            chrome_kwargs["version_main"] = version_main

        if HiristTechService._is_railway():
            chrome_kwargs["browser_executable_path"] = "/usr/bin/chromium"
            chrome_kwargs["driver_executable_path"] = "/usr/bin/chromedriver"

        return uc.Chrome(**chrome_kwargs)

    @staticmethod
    def _wait_for_page_ready(driver: uc.Chrome, timeout_seconds: int) -> None:
        WebDriverWait(driver, timeout_seconds).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

    @staticmethod
    def _js_click_text(driver: uc.Chrome, text: str) -> bool:
        script = """
            const wanted = arguments[0].trim().toLowerCase();
            const elements = Array.from(document.querySelectorAll("button, a, span, div, li"));
            const visible = (el) => {
                const style = window.getComputedStyle(el);
                const rect = el.getBoundingClientRect();
                return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
            };
            for (const el of elements) {
                const textValue = (el.innerText || "").replace(/\\s+/g, " ").trim().toLowerCase();
                if (!textValue) continue;
                if (textValue === wanted || textValue.includes(wanted)) {
                    if (!visible(el)) continue;
                    el.click();
                    return true;
                }
            }
            return false;
        """
        return bool(driver.execute_script(script, text))

    @staticmethod
    def _apply_posting_filter_under_3_days(driver: uc.Chrome, timeout_seconds: int = 20) -> bool:
        wait = WebDriverWait(driver, timeout_seconds)

        posting_candidates = [
            (By.ID, "lotus-select-posting"),
            (By.CSS_SELECTOR, "div[role='combobox'][id*='posting']"),
            (By.XPATH, "//div[@role='combobox' and contains(normalize-space(), 'Posting') ]"),
        ]

        posting_control = None
        for locator in posting_candidates:
            try:
                posting_control = wait.until(EC.element_to_be_clickable(locator))
                break
            except Exception:
                continue

        if posting_control is None:
            return False

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", posting_control)
        HiristTechService._human_pause(
            HiristTechService.HUMAN_MIN_DELAY_SECONDS,
            HiristTechService.HUMAN_MAX_DELAY_SECONDS,
        )

        try:
            posting_control.click()
        except Exception:
            driver.execute_script("arguments[0].click();", posting_control)

        HiristTechService._human_pause(
            HiristTechService.HUMAN_MIN_DELAY_SECONDS,
            HiristTechService.HUMAN_MAX_DELAY_SECONDS,
        )

        option_candidates = [
            (By.XPATH, "//li[@role='option' and contains(normalize-space(), '< 3 Days')]"),
            (By.XPATH, "//li[@role='option' and contains(normalize-space(), '<3 Days')]"),
            (By.XPATH, "//li[@role='option' and contains(normalize-space(), '3 Days')]"),
        ]

        option_selected = False
        for locator in option_candidates:
            try:
                option = wait.until(EC.element_to_be_clickable(locator))
                option.click()
                option_selected = True
                HiristTechService._human_pause(
                    HiristTechService.HUMAN_MIN_DELAY_SECONDS,
                    HiristTechService.HUMAN_MAX_DELAY_SECONDS,
                )
                break
            except Exception:
                continue

        if not option_selected:
            return False

        try:
            apply_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Apply']"))
            )
            apply_button.click()
        except Exception:
            HiristTechService._js_click_text(driver, "Apply")

        HiristTechService._human_pause(
            HiristTechService.HUMAN_MIN_DELAY_SECONDS,
            HiristTechService.HUMAN_MAX_DELAY_SECONDS,
        )

        try:
            selected_value = (posting_control.text or "").strip().lower()
            return "3 day" in selected_value
        except Exception:
            return True

    @staticmethod
    def _is_json_like(mime_type: str, url: str) -> bool:
        mime = (mime_type or "").lower()
        lower_url = (url or "").lower()
        if "json" in mime:
            return True
        json_url_hints = [".json", "/api/", "graphql", "ajax", "jobs", "search"]
        return any(hint in lower_url for hint in json_url_hints)

    @staticmethod
    def _is_hirist_job_list_api(url: str) -> bool:
        lower_url = (url or "").lower()
        return any(hint in lower_url for hint in HiristTechService.JOB_LIST_API_HINTS)

    @staticmethod
    def _extract_jobs_from_job_list_payload(
        parsed_body: dict[str, Any],
        response_url: str,
        page_url: str,
    ) -> list[dict[str, Any]]:
        jobs = parsed_body.get("data")
        if not isinstance(jobs, list):
            return []

        normalized: list[dict[str, Any]] = []
        for job in jobs:
            if not isinstance(job, dict):
                continue

            normalized_job = {
                **job,
                "_source_response_url": response_url,
                "_source_page_url": page_url,
            }
            normalized.append(normalized_job)

        return normalized

    @staticmethod
    def _collect_job_list_payloads(driver: uc.Chrome, page_url: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        payloads: list[dict[str, Any]] = []
        jobs: list[dict[str, Any]] = []
        try:
            logs = driver.get_log("performance")
        except Exception:
            return payloads, jobs

        for log in logs:
            try:
                message = json.loads(log["message"]).get("message", {})
                if message.get("method") != "Network.responseReceived":
                    continue

                params = message.get("params", {})
                request_id = params.get("requestId")
                response = params.get("response", {})
                status = int(response.get("status", 0))
                url = str(response.get("url", ""))
                mime_type = str(response.get("mimeType", ""))

                if status != 200 or not request_id:
                    continue
                if not HiristTechService._is_hirist_job_list_api(url):
                    continue
                if not HiristTechService._is_json_like(mime_type, url):
                    continue

                body_data = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                raw_body = body_data.get("body", "")
                if body_data.get("base64Encoded"):
                    raw_body = base64.b64decode(raw_body).decode("utf-8", errors="ignore")

                decoded = json.loads(raw_body)
                if not isinstance(decoded, dict):
                    continue

                parsed_jobs = HiristTechService._extract_jobs_from_job_list_payload(decoded, url, page_url)
                if not parsed_jobs:
                    continue

                payloads.append(
                    {
                        "captured_at": HiristTechService._utc_now_iso(),
                        "page_url": page_url,
                        "response_url": url,
                        "mime_type": mime_type,
                        "status": status,
                        "data": decoded,
                    }
                )
                jobs.extend(parsed_jobs)
            except Exception:
                continue

        return payloads, jobs

    @staticmethod
    def _dedupe_payloads(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []

        for item in payloads:
            try:
                signature = json.dumps(
                    {
                        "response_url": item.get("response_url"),
                        "data": item.get("data"),
                    },
                    sort_keys=True,
                    ensure_ascii=False,
                )
            except Exception:
                continue

            if signature in seen:
                continue

            seen.add(signature)
            unique.append(item)

        return unique

    @staticmethod
    def _dedupe_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[Any, str]] = set()
        unique: list[dict[str, Any]] = []

        for job in jobs:
            job_id = job.get("id")
            job_url = str(job.get("jobDetailUrl") or "").strip().lower()
            key = (job_id, job_url)
            if key in seen:
                continue
            seen.add(key)
            unique.append(job)

        return unique

    @staticmethod
    def _to_epoch_ms(value: Any) -> int | None:
        if not isinstance(value, (int, float)):
            return None

        numeric = int(value)
        if numeric <= 0:
            return None

        if numeric < 100_000_000_000:
            return numeric * 1000

        return numeric

    @staticmethod
    def _job_created_epoch_ms(job: dict[str, Any]) -> int | None:
        created_ms = HiristTechService._to_epoch_ms(job.get("createdTimeMs"))
        if created_ms is not None:
            return created_ms

        return HiristTechService._to_epoch_ms(job.get("createdTime"))

    @staticmethod
    def _job_card_from_raw(job: dict[str, Any], now_ms: int) -> dict[str, Any]:
        created_ms = HiristTechService._job_created_epoch_ms(job)
        posted_age_hours = None
        if created_ms is not None and now_ms >= created_ms:
            posted_age_hours = round((now_ms - created_ms) / 3_600_000, 2)

        company_data = job.get("companyData") if isinstance(job.get("companyData"), dict) else {}
        recruiter = job.get("recruiter") if isinstance(job.get("recruiter"), dict) else {}

        tags: list[str] = []
        raw_tags = job.get("tags")
        if isinstance(raw_tags, list):
            for tag in raw_tags:
                if isinstance(tag, dict) and isinstance(tag.get("name"), str):
                    tags.append(tag["name"])

        return {
            "id": job.get("id"),
            "title": job.get("title"),
            "jobdesignation": job.get("jobdesignation"),
            "jobDetailUrl": job.get("jobDetailUrl"),
            "minExperienceYears": job.get("min"),
            "maxExperienceYears": job.get("max"),
            "locations": job.get("location") or job.get("locations") or [],
            "createdTime": job.get("createdTime"),
            "createdTimeMs": job.get("createdTimeMs"),
            "postedAgeHours": posted_age_hours,
            "applyStatus": job.get("applyStatus"),
            "applyCount": job.get("applyCount"),
            "companyName": company_data.get("companyName"),
            "recruiterName": recruiter.get("recruiterName"),
            "tags": tags,
            "sourcePageUrl": job.get("_source_page_url"),
            "sourceResponseUrl": job.get("_source_response_url"),
        }

    @staticmethod
    def _build_recent_job_views(
        jobs: list[dict[str, Any]],
        max_age_hours: int,
    ) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        max_age_ms = max_age_hours * 3_600_000

        recent_jobs: list[dict[str, Any]] = []
        jobs_data_per_url: dict[str, list[dict[str, Any]]] = {}

        for job in jobs:
            created_ms = HiristTechService._job_created_epoch_ms(job)
            if created_ms is None:
                continue

            age_ms = now_ms - created_ms
            if age_ms < 0 or age_ms > max_age_ms:
                continue

            card = HiristTechService._job_card_from_raw(job, now_ms)
            recent_jobs.append(card)

            source_url = str(job.get("_source_response_url") or "")
            if source_url:
                jobs_data_per_url.setdefault(source_url, []).append(card)

        for source_url in jobs_data_per_url:
            jobs_data_per_url[source_url].sort(
                key=lambda item: item.get("createdTimeMs") or item.get("createdTime") or 0,
                reverse=True,
            )

        recent_jobs.sort(
            key=lambda item: item.get("createdTimeMs") or item.get("createdTime") or 0,
            reverse=True,
        )

        return recent_jobs, jobs_data_per_url

    @staticmethod
    def _normalize_description_text(raw: str) -> str:
        text = BeautifulSoup(raw or "", "html.parser").get_text("\n", strip=True)
        text = unescape(text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _extract_job_description_from_html(html: str) -> tuple[str | None, str | None]:
        soup = BeautifulSoup(html, "html.parser")

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            payload = (script.string or script.text or "").strip()
            if not payload:
                continue

            try:
                parsed = json.loads(payload)
            except Exception:
                continue

            entries = parsed if isinstance(parsed, list) else [parsed]
            for item in entries:
                if not isinstance(item, dict):
                    continue
                if str(item.get("@type", "")).lower() != "jobposting":
                    continue

                description = item.get("description")
                if isinstance(description, str) and description.strip():
                    normalized = HiristTechService._normalize_description_text(description)
                    if normalized:
                        return normalized, "json-ld"

        heading_tags = soup.find_all(["h1", "h2", "h3", "div", "span", "p"])
        for heading in heading_tags:
            title = " ".join(heading.get_text(" ", strip=True).split())
            if title.lower() != "job description":
                continue

            container = heading.find_parent("section") or heading.find_parent("div")
            if container:
                container_text = container.get_text("\n", strip=True)
                container_text = re.sub(r"^\s*job description\s*", "", container_text, flags=re.I)
                normalized = HiristTechService._normalize_description_text(container_text)
                if normalized:
                    return normalized, "dom-job-description-section"

        meta_desc = soup.find("meta", attrs={"name": "description"}) or soup.find(
            "meta", attrs={"property": "og:description"}
        )
        if meta_desc:
            content = str(meta_desc.get("content") or "").strip()
            if content:
                normalized = HiristTechService._normalize_description_text(content)
                if normalized:
                    return normalized, "meta-description"

        return None, None

    @staticmethod
    def _fetch_job_description(job_url: str, timeout_seconds: int = 30) -> tuple[str | None, str | None]:
        if not job_url:
            return None, None

        try:
            response = requests.get(job_url, headers=HiristTechService.HTTP_HEADERS, timeout=timeout_seconds)
            if response.status_code != 200:
                return None, None
            return HiristTechService._extract_job_description_from_html(response.text)
        except Exception:
            return None, None

    @staticmethod
    def _enrich_jobs_with_descriptions(
        recent_jobs: list[dict[str, Any]],
        min_delay_seconds: float = 0.6,
        max_delay_seconds: float = 1.4,
    ) -> int:
        cache: dict[str, tuple[str | None, str | None]] = {}
        enriched_count = 0

        for job in recent_jobs:
            job_url = str(job.get("jobDetailUrl") or "").strip()
            if not job_url:
                job["jobDescription"] = None
                job["jobDescriptionSource"] = None
                continue

            if job_url not in cache:
                HiristTechService._human_pause(min_delay_seconds, max_delay_seconds)
                cache[job_url] = HiristTechService._fetch_job_description(job_url)

            description, source = cache[job_url]
            job["jobDescription"] = description
            job["jobDescriptionSource"] = source
            if description:
                enriched_count += 1

        return enriched_count

    @staticmethod
    def scrape_hirist_categories(
        max_scrolls: int = 250,
        max_runtime_seconds: int = 300,
        max_idle_seconds: int = 90,
        min_scroll_delay_seconds: float = 1.0,
        max_scroll_delay_seconds: float = 2.0,
        headless: bool = True,
        output_dir: str | None = None,
        recent_job_max_age_hours: int = 24,
        include_job_description: bool = True,
        target_urls: list[str] | None = None,
    ) -> dict[str, Any]:
        if min_scroll_delay_seconds < 0:
            min_scroll_delay_seconds = 0
        if max_scroll_delay_seconds < min_scroll_delay_seconds:
            max_scroll_delay_seconds = min_scroll_delay_seconds

        resolved_output_dir = output_dir if output_dir is not None else _default_output_dir()
        os.makedirs(resolved_output_dir, exist_ok=True)

        page_urls = target_urls if target_urls is not None else _target_urls_from_env()

        driver = HiristTechService._build_driver(headless=headless)
        all_payloads: list[dict[str, Any]] = []
        all_jobs: list[dict[str, Any]] = []
        page_summaries: list[dict[str, Any]] = []

        try:
            for page_url in page_urls:
                page_payloads: list[dict[str, Any]] = []
                page_jobs: list[dict[str, Any]] = []
                page_start = time.time()
                last_new_data_at = page_start
                filter_applied = False
                scroll_count = 0
                error_message = None

                try:
                    driver.get(page_url)
                    HiristTechService._wait_for_page_ready(driver, timeout_seconds=45)
                    HiristTechService._human_pause(1.3, 2.6)
                    filter_applied = HiristTechService._apply_posting_filter_under_3_days(driver, timeout_seconds=25)

                    while scroll_count < max_scrolls:
                        now = time.time()
                        elapsed = now - page_start
                        idle_time = now - last_new_data_at

                        if elapsed >= max_runtime_seconds or idle_time >= max_idle_seconds:
                            break

                        HiristTechService._human_pause(min_scroll_delay_seconds, max_scroll_delay_seconds)

                        driver.execute_script("window.scrollBy(0, arguments[0]);", HiristTechService._human_scroll_pixels())
                        scroll_count += 1

                        if random.random() < HiristTechService.HUMAN_LONG_PAUSE_CHANCE:
                            HiristTechService._human_pause(2.5, 4.0)
                        else:
                            HiristTechService._human_pause(min_scroll_delay_seconds, max_scroll_delay_seconds)

                        captured_payloads, captured_jobs = HiristTechService._collect_job_list_payloads(driver, page_url)
                        if captured_payloads or captured_jobs:
                            page_payloads.extend(captured_payloads)
                            page_jobs.extend(captured_jobs)
                            last_new_data_at = time.time()

                    trailing_payloads, trailing_jobs = HiristTechService._collect_job_list_payloads(driver, page_url)
                    if trailing_payloads or trailing_jobs:
                        page_payloads.extend(trailing_payloads)
                        page_jobs.extend(trailing_jobs)
                except TimeoutException:
                    error_message = "Timed out while loading or interacting with Hirist page"
                except Exception as exc:
                    error_message = str(exc)

                unique_page_payloads = HiristTechService._dedupe_payloads(page_payloads)
                unique_page_jobs = HiristTechService._dedupe_jobs(page_jobs)
                all_payloads.extend(unique_page_payloads)
                all_jobs.extend(unique_page_jobs)

                page_summaries.append(
                    {
                        "page_url": page_url,
                        "filter_applied": filter_applied,
                        "scrolls": scroll_count,
                        "payloads_captured": len(unique_page_payloads),
                        "jobs_captured": len(unique_page_jobs),
                        "error": error_message,
                    }
                )
        finally:
            try:
                driver.quit()
            except Exception:
                pass

        unique_payloads = HiristTechService._dedupe_payloads(all_payloads)
        unique_jobs = HiristTechService._dedupe_jobs(all_jobs)
        recent_jobs, jobs_data_per_url = HiristTechService._build_recent_job_views(
            unique_jobs,
            max_age_hours=recent_job_max_age_hours,
        )
        descriptions_enriched = 0
        if include_job_description and recent_jobs:
            descriptions_enriched = HiristTechService._enrich_jobs_with_descriptions(recent_jobs)

        recent_counts_per_page: dict[str, int] = {}
        for job in recent_jobs:
            page_url = str(job.get("sourcePageUrl") or "")
            if page_url:
                recent_counts_per_page[page_url] = recent_counts_per_page.get(page_url, 0) + 1

        for page_summary in page_summaries:
            page_url = str(page_summary.get("page_url") or "")
            page_summary["recent_jobs_captured"] = recent_counts_per_page.get(page_url, 0)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = Path(resolved_output_dir) / f"hirist_network_capture_{stamp}.json"
        output_document: dict[str, Any] = {
            "source": "hirist.tech",
            "captured_at": HiristTechService._utc_now_iso(),
            "pages": page_summaries,
            "total_payloads": len(unique_payloads),
            "total_jobs": len(unique_jobs),
            "recent_job_max_age_hours": recent_job_max_age_hours,
            "total_recent_jobs": len(recent_jobs),
            "job_descriptions_enriched": descriptions_enriched,
            "payloads": unique_payloads,
            "jobs": unique_jobs,
            "jobs_data_per_url": jobs_data_per_url,
            "recent_jobs": recent_jobs,
        }

        output_path.write_text(
            json.dumps(output_document, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return {
            "output_file": str(output_path),
            "pages": page_summaries,
            "total_payloads": len(unique_payloads),
            "total_jobs": len(unique_jobs),
            "total_recent_jobs": len(recent_jobs),
            "job_descriptions_enriched": descriptions_enriched,
            "recent_jobs": recent_jobs,
            "unique_jobs": unique_jobs,
        }
