"""
Pluggable scraper adapters for the role pipeline.

Each adapter wraps an existing scraper behind a common Protocol so
``role_pipeline._scrape_role_jobs()`` can iterate over a registry
instead of hardcoding if/else blocks per source.

Configuration
-------------
A single env variable ``ROLE_PIPELINE_ROLE_CONFIG_JSON`` drives all
per-role / per-scraper behaviour.  Example::

    ROLE_PIPELINE_ROLE_CONFIG_JSON = '{
      "data analyst": {
        "jobspy":     { "query": "Data Analyst" },
        "naukri":     { "query": "Data Analyst" },
        "wellfound":  { "query": "Data Analyst" },
        "hirist":     { "url": "https://www.hirist.tech/c/data-analytics-bi-jobs?ref=topnavigation" },
        "hirecafe":   { "search_url": "https://hiring.cafe/?searchState=..." }
      }
    }'

Each adapter reads the keys it cares about from its own config dict
and falls back to env-var defaults when a key is absent.
"""

import logging
import math
import os
from typing import Any, Protocol, runtime_checkable

from services.pipeline import _retry, _parse_csv_env

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class RoleJobScraper(Protocol):
    """Interface every role-pipeline scraper adapter must satisfy."""

    @property
    def name(self) -> str:
        """Unique source identifier, e.g. 'naukri', 'jobspy'."""
        ...

    def scrape_for_role(
        self,
        role: str,
        role_config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Scrape jobs for *role* using adapter-specific logic.

        Parameters
        ----------
        role : str
            Human-readable role label, e.g. ``"Data Analyst"``.
        role_config : dict
            Adapter-specific config pulled from the unified role config map.
            E.g. ``{"query": "Data Analyst"}`` or ``{"url": "https://..."}``.
            May be empty if no per-role override was set; adapters should
            fall back to sensible defaults (usually *role* itself as the query).
        """
        ...


# ---------------------------------------------------------------------------
# Naukri Adapter
# ---------------------------------------------------------------------------


class NaukriAdapter:
    """Wraps ``apify_naukri.scrape_naukri_jobs`` + normalisation."""

    @property
    def name(self) -> str:
        return "naukri"

    def scrape_for_role(
        self,
        role: str,
        role_config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        from services.apify_naukri import normalize_naukri_item, scrape_naukri_jobs

        if not os.getenv("APIFY_TOKEN"):
            logger.info("naukri-adapter skipped: APIFY_TOKEN not set")
            return []

        query = role_config.get("query") or role
        max_jobs = int(
            role_config.get("max_jobs")
            or os.getenv("APIFY_MAX_JOBS_NAUKRI", os.getenv("DAILY_PIPELINE_RESULTS_WANTED", "30")),
        )
        freshness = role_config.get("freshness") or os.getenv("APIFY_FRESHNESS", "1")
        fetch_details = (
            role_config.get("fetch_details")
            if role_config.get("fetch_details") is not None
            else os.getenv("APIFY_FETCH_DETAILS", "false").lower() == "true"
        )

        raw_items = _retry(
            action=lambda: scrape_naukri_jobs(
                keyword=query,
                max_jobs=max_jobs,
                freshness=freshness,
                fetch_details=fetch_details,
            ),
            retries=2,
            initial_delay_seconds=2.0,
        )

        jobs: list[dict[str, Any]] = []
        for raw in raw_items:
            normalized = normalize_naukri_item(raw)
            normalized["requested_role"] = role
            normalized["role_query"] = query
            jobs.append(normalized)
        return jobs


# ---------------------------------------------------------------------------
# Wellfound Adapter
# ---------------------------------------------------------------------------


class WellfoundAdapter:
    """Wraps ``apify_wellfound.scrape_wellfound_jobs`` + normalisation."""

    @property
    def name(self) -> str:
        return "wellfound"

    def scrape_for_role(
        self,
        role: str,
        role_config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        from services.apify_wellfound import normalize_wellfound_item, scrape_wellfound_jobs

        if not os.getenv("APIFY_TOKEN"):
            logger.info("wellfound-adapter skipped: APIFY_TOKEN not set")
            return []

        wellfound_enabled = os.getenv("APIFY_WELLFOUND_ENABLED", "true").lower() in ("1", "true", "yes")
        if not wellfound_enabled:
            logger.info("wellfound-adapter skipped: APIFY_WELLFOUND_ENABLED=false")
            return []

        query = role_config.get("query") or role
        location = role_config.get("location") or os.getenv("APIFY_WELLFOUND_LOCATION", "india")
        results = int(role_config.get("results_wanted") or os.getenv("APIFY_MAX_JOBS_WELLFOUND_PER_ROLE", "50"))
        max_pages = int(role_config.get("max_pages") or os.getenv("APIFY_WELLFOUND_MAX_PAGES", "20"))
        use_proxy = os.getenv("APIFY_WELLFOUND_USE_PROXY", "true").lower() in ("1", "true", "yes")
        proxy_groups = _parse_csv_env(os.getenv("APIFY_WELLFOUND_PROXY_GROUPS", "RESIDENTIAL"))

        raw_items = _retry(
            action=lambda: scrape_wellfound_jobs(
                location=location,
                results_wanted=results,
                max_pages=max_pages,
                keyword=query,
                use_apify_proxy=use_proxy,
                apify_proxy_groups=proxy_groups,
            ),
            retries=2,
            initial_delay_seconds=2.0,
        )

        jobs: list[dict[str, Any]] = []
        for raw in raw_items:
            normalized = normalize_wellfound_item(raw)
            normalized["requested_role"] = role
            normalized["role_query"] = query
            jobs.append(normalized)
        return jobs


# ---------------------------------------------------------------------------
# JobSpy Adapter  (LinkedIn + Indeed via jobspy library)
# ---------------------------------------------------------------------------


class JobSpyAdapter:
    """Wraps ``jobspy.scrape_jobs`` for LinkedIn + Indeed."""

    @property
    def name(self) -> str:
        return "jobspy"

    def scrape_for_role(
        self,
        role: str,
        role_config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        from jobspy import scrape_jobs

        query = role_config.get("query") or role
        location = role_config.get("location") or os.getenv("DAILY_PIPELINE_LOCATION", "India")
        country_indeed = role_config.get("country_indeed") or os.getenv("DAILY_PIPELINE_COUNTRY_INDEED", "india")
        default_results = int(os.getenv("DAILY_PIPELINE_RESULTS_WANTED", "30"))
        linkedin_results = int(
            role_config.get("linkedin_results") or os.getenv("JOBSPY_RESULTS_WANTED_LINKEDIN", str(default_results))
        )
        indeed_results = int(
            role_config.get("indeed_results") or os.getenv("JOBSPY_RESULTS_WANTED_INDEED", str(default_results))
        )

        all_jobs: list[dict[str, Any]] = []

        # LinkedIn
        linkedin_df = _retry(
            action=lambda: scrape_jobs(
                site_name=["linkedin"],
                search_term=query,
                location=location,
                results_wanted=linkedin_results,
                hours_old=24,
                linkedin_fetch_description=True,
                offset=0,
                verbose=0,
            ),
            retries=3,
            initial_delay_seconds=1.5,
        )
        linkedin_items = _sanitize_for_json(_dataframe_to_response(linkedin_df))
        for item in linkedin_items:
            item["requested_role"] = role
            item["role_query"] = query
        all_jobs.extend(linkedin_items)

        # Indeed
        indeed_df = _retry(
            action=lambda: scrape_jobs(
                site_name=["indeed"],
                search_term=query,
                location=location,
                country_indeed=country_indeed,
                results_wanted=indeed_results,
                hours_old=24,
                offset=0,
                verbose=0,
            ),
            retries=3,
            initial_delay_seconds=1.5,
        )
        indeed_items = _sanitize_for_json(_dataframe_to_response(indeed_df))
        for item in indeed_items:
            item["requested_role"] = role
            item["role_query"] = query
        all_jobs.extend(indeed_items)

        return all_jobs


# ---------------------------------------------------------------------------
# Hirist Adapter
# ---------------------------------------------------------------------------


class HiristAdapter:
    """Wraps ``HiristTechService.scrape_hirist_categories`` + normalisation."""

    @property
    def name(self) -> str:
        return "hirist"

    def scrape_for_role(
        self,
        role: str,
        role_config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        from services.hirist import HiristTechService, normalize_hirist_item

        # The Hirist scraper needs a category URL, not a keyword.
        target_url = (
            role_config.get("url")
            or os.getenv("ROLE_PIPELINE_HIRIST_FIXED_URL", "https://www.hirist.tech/c/data-analytics-bi-jobs?ref=topnavigation")
        )
        target_urls = [target_url]

        max_scrolls = int(role_config.get("max_scrolls") or os.getenv("HIRIST_MAX_SCROLLS", "250"))
        max_runtime = int(role_config.get("max_runtime_seconds") or os.getenv("HIRIST_MAX_RUNTIME_SECONDS", "300"))
        max_idle = int(role_config.get("max_idle_seconds") or os.getenv("HIRIST_MAX_IDLE_SECONDS", "90"))
        min_scroll_delay = float(role_config.get("min_scroll_delay") or os.getenv("HIRIST_MIN_SCROLL_DELAY_SECONDS", "1.0"))
        max_scroll_delay = float(role_config.get("max_scroll_delay") or os.getenv("HIRIST_MAX_SCROLL_DELAY_SECONDS", "2.0"))
        headless = os.getenv("HIRIST_HEADLESS", "true").lower() not in ("0", "false", "no")
        recent_hours = int(role_config.get("recent_hours") or os.getenv("HIRIST_RECENT_MAX_AGE_HOURS", "24"))
        include_desc = os.getenv("HIRIST_INCLUDE_JOB_DESCRIPTION", "true").lower() not in ("0", "false", "no")

        hirist_result = _retry(
            action=lambda: HiristTechService.scrape_hirist_categories(
                max_scrolls=max_scrolls,
                max_runtime_seconds=max_runtime,
                max_idle_seconds=max_idle,
                min_scroll_delay_seconds=min_scroll_delay,
                max_scroll_delay_seconds=max_scroll_delay,
                headless=headless,
                recent_job_max_age_hours=recent_hours,
                include_job_description=include_desc,
                target_urls=target_urls,
            ),
            retries=2,
            initial_delay_seconds=5.0,
        )

        role_query = role_config.get("role_query") or f"hirist_{role.lower().replace(' ', '_')}"
        jobs: list[dict[str, Any]] = []
        for card in hirist_result.get("recent_jobs") or []:
            normalized = normalize_hirist_item(card)
            normalized["requested_role"] = role
            normalized["role_query"] = role_query
            jobs.append(normalized)
        return jobs


# ---------------------------------------------------------------------------
# HireCafe Adapter
# ---------------------------------------------------------------------------


class HireCafeAdapter:
    """Wraps ``hire_cafe.scrape_hirecafe_jobs`` + normalisation."""

    @property
    def name(self) -> str:
        return "hirecafe"

    def scrape_for_role(
        self,
        role: str,
        role_config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        from services.hire_cafe import normalize_hirecafe_item, scrape_hirecafe_jobs
        search_url = (role_config.get("search_url") or "").strip() or None

        max_samples = int(role_config.get("max_samples") or os.getenv("HIRECAFE_MAX_SAMPLES", "200"))

        raw_items = _retry(
            action=lambda: scrape_hirecafe_jobs(
                max_samples=max_samples,
                search_url=search_url,
            ),
            retries=2,
            initial_delay_seconds=5.0,
        )

        jobs: list[dict[str, Any]] = []
        for raw in raw_items:
            normalized = normalize_hirecafe_item(raw)
            normalized["requested_role"] = role
            normalized["role_query"] = role_config.get("role_query") or "hire.cafe"
            jobs.append(normalized)
        return jobs


# ---------------------------------------------------------------------------
# Scraper Registry
# ---------------------------------------------------------------------------


class ScraperRegistry:
    """Simple dict-backed lookup of scraper adapters by source name."""

    def __init__(self) -> None:
        self._scrapers: dict[str, RoleJobScraper] = {}

    def register(self, scraper: RoleJobScraper) -> None:
        self._scrapers[scraper.name] = scraper

    def get(self, name: str) -> RoleJobScraper | None:
        return self._scrapers.get(name)

    def available_sources(self) -> set[str]:
        return set(self._scrapers.keys())


# Module-level singleton — populated once at import time.
SCRAPER_REGISTRY = ScraperRegistry()
SCRAPER_REGISTRY.register(NaukriAdapter())
SCRAPER_REGISTRY.register(WellfoundAdapter())
SCRAPER_REGISTRY.register(JobSpyAdapter())
SCRAPER_REGISTRY.register(HiristAdapter())
SCRAPER_REGISTRY.register(HireCafeAdapter())


# ---------------------------------------------------------------------------
# Shared helpers (copied from role_pipeline to avoid circular imports)
# ---------------------------------------------------------------------------


def _dataframe_to_response(jobs_df: Any) -> list[dict[str, Any]]:
    normalized_df = jobs_df.where(jobs_df.notna(), None)
    return normalized_df.to_dict(orient="records")


def _sanitize_for_json(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(item) for item in value]
    return value
