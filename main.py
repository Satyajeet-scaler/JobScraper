import json
import math
import os
import uuid
import logging
from pathlib import Path
from typing import Literal, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi.encoders import jsonable_encoder
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import FileResponse, JSONResponse
from jobspy import scrape_jobs
from pandas import DataFrame

from services.linkedin_session import (
    get_linkedin_storage_path,
    login_linkedin_save_storage_sync,
    save_linkedin_storage_state_json,
)
from services.pipeline import get_pipeline_run_metrics, run_daily_jobs_pipeline
from services.naukri_only_pipeline import get_naukri_run_metrics, run_naukri_scrape_only_pipeline
from services.linkedin_posts_pipeline import get_linkedin_posts_run_metrics, run_linkedin_posts_pipeline


app = FastAPI(
    title="India Jobs API",
    description="Fetch India-focused job listings from supported JobSpy sources.",
    version="1.0.0",
)
BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)
_scheduler: Optional[BackgroundScheduler] = None


def _configure_logging() -> None:
    level_name = os.getenv("APP_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )
    else:
        root.setLevel(level)

SupportedSite = Literal[
    "linkedin",
    "indeed",
    "glassdoor",
    "google",
    "bayt",
    "naukri",
]


def dataframe_to_response(jobs_df: DataFrame) -> list[dict]:
    """
    Convert a pandas DataFrame into JSON-serializable dicts.
    NaN values become None.
    """
    normalized_df = jobs_df.where(jobs_df.notna(), None)
    return normalized_df.to_dict(orient="records")


def sanitize_for_json(value):
    """
    Recursively convert non-JSON-safe values into safe equivalents.
    In particular, JSON disallows NaN/Infinity, so map them to None.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(item) for item in value]
    return value


def validate_site_specific_constraints(
    site_name: list[SupportedSite],
    google_search_term: Optional[str],
) -> None:
    site_set = set(site_name)

    if "google" in site_set and not google_search_term:
        raise HTTPException(
            status_code=400,
            detail="google_search_term is required when site_name includes google.",
        )


def validate_internal_trigger_token(internal_token: Optional[str]) -> None:
    expected_token = os.getenv("INTERNAL_TRIGGER_TOKEN")
    if not expected_token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="INTERNAL_TRIGGER_TOKEN is not configured on server.",
        )

    if not internal_token or internal_token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized internal trigger.",
        )


def _run_daily_jobs_from_scheduler() -> None:
    run_id = str(uuid.uuid4())
    logger.info("scheduler triggered daily pipeline run_id=%s", run_id)
    run_daily_jobs_pipeline(run_id)


def _run_linkedin_auto_login_and_log(job_id: str) -> None:
    """Run Playwright LinkedIn login; log saved storage JSON (contains session secrets)."""
    try:
        path = login_linkedin_save_storage_sync()
        raw = path.read_text(encoding="utf-8")
        try:
            pretty = json.dumps(json.loads(raw), indent=2, ensure_ascii=False)
        except json.JSONDecodeError:
            pretty = raw
        logger.info(
            "linkedin_auto_login[%s] ok path=%s bytes=%d\n%s",
            job_id,
            path,
            len(raw.encode("utf-8")),
            pretty,
        )
    except Exception as exc:
        logger.exception("linkedin_auto_login[%s] failed: %s", job_id, exc)


def _build_scheduler() -> BackgroundScheduler:
    timezone_name = os.getenv("CRON_TIMEZONE", "Asia/Kolkata")
    cron_hour = int(os.getenv("DAILY_CRON_HOUR", "0"))
    cron_minute = int(os.getenv("DAILY_CRON_MINUTE", "0"))
    timezone = ZoneInfo(timezone_name)

    scheduler = BackgroundScheduler(timezone=timezone)
    scheduler.add_job(
        _run_daily_jobs_from_scheduler,
        trigger=CronTrigger(hour=cron_hour, minute=cron_minute, timezone=timezone),
        id="daily-jobs-pipeline",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    return scheduler


@app.on_event("startup")
def startup_event() -> None:
    _configure_logging()
    global _scheduler
    enabled = os.getenv("ENABLE_INTERNAL_CRON", "false").lower() == "true"
    if not enabled:
        logger.info("internal scheduler disabled (ENABLE_INTERNAL_CRON=false)")
        return

    if _scheduler and _scheduler.running:
        return

    _scheduler = _build_scheduler()
    _scheduler.start()
    logger.info(
        "internal scheduler started timezone=%s hour=%s minute=%s",
        os.getenv("CRON_TIMEZONE", "Asia/Kolkata"),
        os.getenv("DAILY_CRON_HOUR", "0"),
        os.getenv("DAILY_CRON_MINUTE", "0"),
    )


@app.on_event("shutdown")
def shutdown_event() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("internal scheduler stopped")
    _scheduler = None


@app.get("/health")
def health_check() -> dict:
    return {"status": "ok"}


@app.get("/")
def home() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.post("/internal/run-daily-jobs")
def run_daily_jobs(
    background_tasks: BackgroundTasks,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)

    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_daily_jobs_pipeline, run_id)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted"},
    )


@app.get("/internal/run-daily-jobs/{run_id}")
def get_daily_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_pipeline_run_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/run-naukri-scrape")
def run_naukri_scrape(
    background_tasks: BackgroundTasks,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)

    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_naukri_scrape_only_pipeline, run_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted"},
    )


@app.get("/internal/run-naukri-scrape/{run_id}")
def get_naukri_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_naukri_run_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/run-linkedin-posts")
def run_linkedin_posts(
    background_tasks: BackgroundTasks,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)

    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_linkedin_posts_pipeline, run_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted"},
    )


@app.get("/internal/run-linkedin-posts/{run_id}")
def get_linkedin_posts_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_linkedin_posts_run_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/linkedin-auto-login")
def trigger_linkedin_auto_login(
    background_tasks: BackgroundTasks,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    """
    Run automated LinkedIn login (env credentials + Playwright) in the background.
    On success, the saved ``storage_state`` JSON is written to application logs (sensitive).
    """
    validate_internal_trigger_token(x_internal_token)

    job_id = str(uuid.uuid4())
    background_tasks.add_task(_run_linkedin_auto_login_and_log, job_id)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "job_id": job_id,
            "status": "accepted",
            "storage_path": str(get_linkedin_storage_path()),
            "detail": "Login runs in background; check server logs for saved JSON or errors.",
        },
    )


@app.post("/internal/linkedin-session")
async def save_linkedin_session(
    request: Request,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    """
    Save Playwright ``storage_state`` JSON (e.g. local ``linkedin_storage.json``) to
    ``LINKEDIN_STORAGE_PATH`` on the server.
    Accepts ``application/json`` body or ``multipart/form-data`` with field ``file``.
    """
    validate_internal_trigger_token(x_internal_token)
    content_type = request.headers.get("content-type", "")

    try:
        if "multipart/form-data" in content_type:
            form = await request.form()
            upload = form.get("file")
            if upload is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Missing form field 'file'.",
                )
            raw_bytes = await upload.read()
            data = json.loads(raw_bytes.decode("utf-8"))
        else:
            body = await request.body()
            if not body.strip():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Empty body.",
                )
            data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON: {exc}",
        ) from exc

    try:
        path = save_linkedin_storage_state_json(data)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return JSONResponse(
        content={
            "status": "saved",
            "path": str(path),
            "cookies": len(data.get("cookies", [])),
        }
    )


@app.get("/jobs")
def get_linkedin_jobs(
    site_name: list[SupportedSite] = Query(
        default=["linkedin"],
        description=(
            "One or more job sources. Supported values: "
            "linkedin, indeed, glassdoor, google, bayt, naukri."
        ),
    ),
    search_term: str = Query(
        default="software engineer",
        description="Keywords for job search (example: backend engineer, data engineer).",
    ),
    google_search_term: Optional[str] = Query(
        default=None,
        description="Required when site_name includes google. Use a specific Google Jobs query.",
    ),
    location: str = Query(
        default="India",
        description="Location to search jobs in.",
    ),
    results_wanted: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Number of job listings to fetch (1-100).",
    ),
    country_indeed: str = Query(
        default="india",
        description="Country filter for Indeed and Glassdoor.",
    ),
    hours_old: Optional[int] = Query(
        default=None,
        ge=1,
        le=720,
        description="Fetch jobs posted in the last N hours.",
    ),
    linkedin_fetch_description: bool = Query(
        default=False,
        description="Whether to fetch full LinkedIn descriptions (slower).",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Start index for paged scraping.",
    ),
    verbose: int = Query(
        default=0,
        ge=0,
        le=2,
        description="JobSpy verbosity level (0-2).",
    ),
) -> JSONResponse:
    validate_site_specific_constraints(
        site_name=site_name,
        google_search_term=google_search_term,
    )

    try:
        jobs_df = scrape_jobs(
            site_name=site_name,
            search_term=search_term,
            google_search_term=google_search_term,
            location=location,
            results_wanted=results_wanted,
            country_indeed=country_indeed,
            hours_old=hours_old,
            linkedin_fetch_description=linkedin_fetch_description,
            offset=offset,
            verbose=verbose,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch jobs from JobSpy for sites {site_name}: {exc}",
        ) from exc

    jobs = sanitize_for_json(dataframe_to_response(jobs_df))
    return JSONResponse(
        content=jsonable_encoder(
            {
            "count": len(jobs),
            "source": site_name,
            "search_term": search_term,
            "google_search_term": google_search_term,
            "location": location,
            "country_indeed": country_indeed,
            "jobs": jobs,
            }
        )
    )
