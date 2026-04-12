import ctypes
import gc
import json
import math
import multiprocessing
import os
import sys
import uuid
import logging
from pathlib import Path
from typing import Any, Literal, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi.encoders import jsonable_encoder
from fastapi import BackgroundTasks, Body, FastAPI, Header, HTTPException, Query, Request, status
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
from services.wellfound_only_pipeline import (
    get_wellfound_run_metrics,
    run_wellfound_scrape_only_pipeline_with_filters,
)
from services.wellfound_classify_pipeline import (
    get_wellfound_classify_run_metrics,
    run_wellfound_classify_only_pipeline,
)
from services.hirecafe_only_pipeline import get_hirecafe_run_metrics, run_hirecafe_scrape_only_pipeline
from services.linkedin_posts_pipeline import get_linkedin_posts_run_metrics, run_linkedin_posts_pipeline
from services.scrape_relevance_service import (
    get_classify_only_run_metrics,
    get_scrape_only_run_metrics,
    run_classify_relevant_only,
    run_scrape_jobs_only,
)
from services.recruiter_info_service import get_recruiter_info_run_metrics, run_recruiter_info_extraction
from services.linkedin_posts_split_service import (
    get_linkedin_posts_classify_only_metrics,
    get_linkedin_posts_scrape_only_metrics,
    run_linkedin_posts_classify_only,
    run_linkedin_posts_scrape_only,
)
from services.handover_log_sync import sync_handover_log_to_sheet
from services.slack_handover_notify import send_handover_notifications
from services.slack_handover_summary import send_handover_summary_to_slack


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


def _cron_today() -> str:
    """Return today's date as YYYY-MM-DD in the cron timezone, not the server system timezone."""
    from datetime import datetime
    tz = ZoneInfo(os.getenv("CRON_TIMEZONE", "Asia/Kolkata"))
    return datetime.now(tz).strftime("%Y-%m-%d")


def _run_in_subprocess(func, *args, **kwargs) -> None:
    """
    Run func(*args, **kwargs) in a forked child process.
    When the child exits, the OS reclaims ALL its memory instantly --
    no heap fragmentation, no malloc_trim needed.
    The parent (FastAPI) process stays at baseline RSS.
    """
    name = getattr(func, "__name__", str(func))
    proc = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
    proc.start()
    proc.join()
    if proc.exitcode == 0:
        logger.info("subprocess %s pid=%d completed successfully", name, proc.pid)
    else:
        logger.error("subprocess %s pid=%d exited with code %d", name, proc.pid, proc.exitcode or -1)


def _scrape_jobs_work(run_id: str, run_date: str) -> None:
    _configure_logging()
    logger.info("scheduler triggered scrape-only run_id=%s run_date=%s", run_id, run_date)
    run_scrape_jobs_only(run_id=run_id, run_date=run_date)


def _classify_relevant_work(run_id: str, run_date: str) -> None:
    _configure_logging()
    logger.info("scheduler triggered classify-only run_id=%s run_date=%s", run_id, run_date)
    run_classify_relevant_only(run_id=run_id, run_date=run_date)


def _recruiter_info_work(run_id: str, run_date: str) -> None:
    _configure_logging()
    logger.info("scheduler triggered recruiter-info run_id=%s run_date=%s", run_id, run_date)
    run_recruiter_info_extraction(run_id=run_id, run_date=run_date)


def _linkedin_posts_scrape_work(run_id: str, run_date: str) -> None:
    _configure_logging()
    logger.info("scheduler triggered linkedin-posts-scrape run_id=%s run_date=%s", run_id, run_date)
    run_linkedin_posts_scrape_only(run_id=run_id, run_date=run_date)


def _linkedin_posts_classify_work(run_id: str, run_date: str) -> None:
    _configure_logging()
    logger.info("scheduler triggered linkedin-posts-classify run_id=%s run_date=%s", run_id, run_date)
    run_linkedin_posts_classify_only(run_id=run_id, run_date=run_date)


def _slack_handover_work(run_date: str) -> None:
    _configure_logging()
    logger.info("scheduler triggered slack-handover run_date=%s", run_date)
    summary = send_handover_notifications(
        run_date=run_date,
        send_linkedin_post=True,
        send_recruiter_info=True,
        send_internal_poc=True,
    )
    logger.info("scheduler slack-handover summary=%s", summary)
    summary_counts = send_handover_summary_to_slack(
        run_date=run_date,
        send_linkedin_post=True,
        send_recruiter_info=True,
        send_internal_poc=True,
    )
    logger.info("scheduler slack-handover summary-counts=%s", summary_counts)
    try:
        log_sync = sync_handover_log_to_sheet(run_date)
        logger.info("scheduler handover-log-sync result=%s", log_sync)
    except Exception as exc:
        logger.exception("scheduler handover-log-sync failed: %s", exc)


def _run_scrape_jobs_from_scheduler() -> None:
    _run_in_subprocess(_scrape_jobs_work, str(uuid.uuid4()), _cron_today())


def _run_classify_relevant_from_scheduler() -> None:
    _run_in_subprocess(_classify_relevant_work, str(uuid.uuid4()), _cron_today())


def _run_recruiter_info_from_scheduler() -> None:
    _run_in_subprocess(_recruiter_info_work, str(uuid.uuid4()), _cron_today())


def _run_linkedin_posts_scrape_from_scheduler() -> None:
    _run_in_subprocess(_linkedin_posts_scrape_work, str(uuid.uuid4()), _cron_today())


def _run_linkedin_posts_classify_from_scheduler() -> None:
    _run_in_subprocess(_linkedin_posts_classify_work, str(uuid.uuid4()), _cron_today())


def _run_slack_handover_from_scheduler() -> None:
    _run_in_subprocess(_slack_handover_work, _cron_today())


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


def _get_rss_mb() -> float:
    """Current RSS in MiB (Linux /proc, fallback to resource module)."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        return 0.0


def _malloc_trim() -> bool:
    """Ask glibc to return free heap pages to the OS. Linux-only, no-op elsewhere."""
    if sys.platform != "linux":
        return False
    try:
        libc = ctypes.CDLL("libc.so.6")
        return libc.malloc_trim(0) != 0
    except (OSError, AttributeError):
        return False


def free_memory() -> dict[str, Any]:
    """
    Aggressively free process memory after heavy cron workloads.
    1. Clear all in-process run-metrics dicts (stale data that grows unbounded).
    2. Full GC sweep.
    3. malloc_trim to release freed heap pages back to the OS.
    """
    rss_before = _get_rss_mb()

    from services.pipeline import PIPELINE_RUN_METRICS
    from services.scrape_relevance_service import SCRAPE_ONLY_RUN_METRICS, CLASSIFY_ONLY_RUN_METRICS
    from services.recruiter_info_service import RECRUITER_INFO_RUN_METRICS
    from services.linkedin_posts_pipeline import LINKEDIN_POSTS_RUN_METRICS
    from services.linkedin_posts_split_service import (
        LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS,
        LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS,
    )
    from services.naukri_only_pipeline import NAUKRI_RUN_METRICS
    from services.wellfound_only_pipeline import WELLFOUND_RUN_METRICS
    from services.wellfound_classify_pipeline import WELLFOUND_CLASSIFY_RUN_METRICS
    from services.hirecafe_only_pipeline import HIRECAFE_RUN_METRICS

    all_metrics = [
        PIPELINE_RUN_METRICS,
        SCRAPE_ONLY_RUN_METRICS,
        CLASSIFY_ONLY_RUN_METRICS,
        RECRUITER_INFO_RUN_METRICS,
        LINKEDIN_POSTS_RUN_METRICS,
        LINKEDIN_POSTS_SCRAPE_ONLY_RUN_METRICS,
        LINKEDIN_POSTS_CLASSIFY_ONLY_RUN_METRICS,
        NAUKRI_RUN_METRICS,
        WELLFOUND_RUN_METRICS,
        WELLFOUND_CLASSIFY_RUN_METRICS,
        HIRECAFE_RUN_METRICS,
    ]
    entries_cleared = sum(len(m) for m in all_metrics)
    for m in all_metrics:
        m.clear()

    gc.collect()
    trimmed = _malloc_trim()

    rss_after = _get_rss_mb()
    result = {
        "rss_before_mb": round(rss_before, 1),
        "rss_after_mb": round(rss_after, 1),
        "freed_mb": round(rss_before - rss_after, 1),
        "metrics_entries_cleared": entries_cleared,
        "gc_collected": True,
        "malloc_trimmed": trimmed,
    }
    logger.info("free_memory result=%s", result)
    return result


def _run_free_memory_from_scheduler() -> None:
    logger.info("scheduler triggered post-cron memory cleanup")
    free_memory()


def _build_scheduler() -> BackgroundScheduler:
    timezone_name = os.getenv("CRON_TIMEZONE", "Asia/Kolkata")
    timezone = ZoneInfo(timezone_name)

    scheduler = BackgroundScheduler(timezone=timezone)
    scheduler.add_job(
        _run_scrape_jobs_from_scheduler,
        trigger=CronTrigger(hour=0, minute=10, timezone=timezone),
        id="daily-scrape-jobs-only",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        _run_classify_relevant_from_scheduler,
        trigger=CronTrigger(hour=1, minute=0, timezone=timezone),
        id="daily-classify-relevant-only",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        _run_recruiter_info_from_scheduler,
        trigger=CronTrigger(hour=3, minute=0, timezone=timezone),
        id="daily-recruiter-info-only",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        _run_linkedin_posts_scrape_from_scheduler,
        trigger=CronTrigger(hour=4, minute=0, timezone=timezone),
        id="daily-linkedin-posts-scrape-only",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        _run_linkedin_posts_classify_from_scheduler,
        trigger=CronTrigger(hour=5, minute=0, timezone=timezone),
        id="daily-linkedin-posts-classify-only",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        _run_slack_handover_from_scheduler,
        trigger=CronTrigger(hour=9, minute=30, timezone=timezone),
        id="daily-slack-handover-all-cases",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        _run_free_memory_from_scheduler,
        trigger=CronTrigger(hour=9, minute=45, timezone=timezone),
        id="daily-post-cron-memory-cleanup",
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
        (
            "internal scheduler started timezone=%s "
            "scrape=%s classify=%s recruiter=%s linkedin_scrape=%s linkedin_classify=%s slack=%s memory_cleanup=%s"
        ),
        os.getenv("CRON_TIMEZONE", "Asia/Kolkata"),
        "00:10",
        "01:00",
        "03:00",
        "04:00",
        "05:00",
        "09:30",
        "09:45",
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


@app.get("/internal/debug-time")
def debug_time() -> dict:
    from datetime import date, datetime
    tz = ZoneInfo(os.getenv("CRON_TIMEZONE", "Asia/Kolkata"))
    utc = ZoneInfo("UTC")
    return {
        "cron_today": _cron_today(),
        "date_today_system": date.today().isoformat(),
        "now_cron_tz": datetime.now(tz).isoformat(),
        "now_utc": datetime.now(utc).isoformat(),
        "cron_timezone": os.getenv("CRON_TIMEZONE", "Asia/Kolkata"),
    }


@app.post("/internal/free-memory")
def trigger_free_memory(
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    """Force garbage collection and release heap memory back to the OS."""
    validate_internal_trigger_token(x_internal_token)
    result = free_memory()
    return JSONResponse(content=jsonable_encoder(result))


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


@app.post("/internal/run-scrape-jobs")
def run_scrape_jobs(
    background_tasks: BackgroundTasks,
    run_date: Optional[str] = Query(default=None, description="Optional date YYYY-MM-DD"),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    resolved_date = run_date or _cron_today()
    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_scrape_jobs_only, run_id, resolved_date)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted", "run_date": resolved_date},
    )


@app.get("/internal/run-scrape-jobs/{run_id}")
def get_scrape_jobs_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_scrape_only_run_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/run-classify-relevant")
def run_classify_relevant(
    background_tasks: BackgroundTasks,
    run_date: Optional[str] = Query(default=None, description="Optional date YYYY-MM-DD"),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    resolved_date = run_date or _cron_today()
    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_classify_relevant_only, run_id, resolved_date)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted", "run_date": resolved_date},
    )


@app.get("/internal/run-classify-relevant/{run_id}")
def get_classify_relevant_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_classify_only_run_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/run-recruiter-info")
def run_recruiter_info(
    background_tasks: BackgroundTasks,
    run_date: Optional[str] = Query(default=None, description="Optional date YYYY-MM-DD"),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    resolved_date = run_date or _cron_today()
    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_recruiter_info_extraction, run_id, resolved_date)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted", "run_date": resolved_date},
    )


@app.get("/internal/run-recruiter-info/{run_id}")
def get_recruiter_info_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_recruiter_info_run_metrics(run_id)
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


@app.post("/internal/run-wellfound-scrape")
def run_wellfound_scrape(
    background_tasks: BackgroundTasks,
    time_filter: bool = Query(default=True, description="Apply posted-within-hours filter before writing sheet."),
    hours_old: int = Query(default=24, ge=1, le=720, description="Keep jobs posted in last N hours when time_filter=true."),
    per_role_jobs: int = Query(default=50, ge=1, le=200, description="Jobs to request from Wellfound for each selected role."),
    target_roles: Optional[str] = Query(
        default=None,
        description=(
            "Comma-separated role names to scrape. "
            "Allowed: Developer, Data Engineer, Data Analyst, Data Scientist, Devops Engineer, Platform Engineer"
        ),
    ),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)

    run_id = str(uuid.uuid4())
    selected_roles = (
        [part.strip() for part in (target_roles or "").split(",") if part.strip()]
        if target_roles
        else None
    )
    background_tasks.add_task(
        run_wellfound_scrape_only_pipeline_with_filters,
        run_id,
        time_filter,
        hours_old,
        selected_roles,
        per_role_jobs,
    )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "run_id": run_id,
            "status": "accepted",
            "time_filter": time_filter,
            "hours_old": hours_old,
            "per_role_jobs": per_role_jobs,
            "target_roles": selected_roles,
        },
    )


@app.get("/internal/run-wellfound-scrape/{run_id}")
def get_wellfound_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_wellfound_run_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/run-wellfound-classify")
def run_wellfound_classify(
    background_tasks: BackgroundTasks,
    run_date: Optional[str] = Query(default=None, description="Optional date YYYY-MM-DD"),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)

    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_wellfound_classify_only_pipeline, run_id, run_date)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted", "run_date": run_date or _cron_today()},
    )


@app.get("/internal/run-wellfound-classify/{run_id}")
def get_wellfound_classify_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_wellfound_classify_run_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/run-hirecafe-scrape")
def run_hirecafe_scrape(
    background_tasks: BackgroundTasks,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)

    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_hirecafe_scrape_only_pipeline, run_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted"},
    )


@app.get("/internal/run-hirecafe-scrape/{run_id}")
def get_hirecafe_run_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_hirecafe_run_metrics(run_id)
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


@app.post("/internal/run-linkedin-posts-scrape")
def run_linkedin_posts_scrape(
    background_tasks: BackgroundTasks,
    run_date: Optional[str] = Query(default=None, description="Optional date YYYY-MM-DD"),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    resolved_date = run_date or _cron_today()
    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_linkedin_posts_scrape_only, run_id, resolved_date)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted", "run_date": resolved_date},
    )


@app.get("/internal/run-linkedin-posts-scrape/{run_id}")
def get_linkedin_posts_scrape_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_linkedin_posts_scrape_only_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/run-linkedin-posts-classify")
def run_linkedin_posts_classify(
    background_tasks: BackgroundTasks,
    run_date: Optional[str] = Query(default=None, description="Optional date YYYY-MM-DD"),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    resolved_date = run_date or _cron_today()
    run_id = str(uuid.uuid4())
    background_tasks.add_task(run_linkedin_posts_classify_only, run_id, resolved_date)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"run_id": run_id, "status": "accepted", "run_date": resolved_date},
    )


@app.get("/internal/run-linkedin-posts-classify/{run_id}")
def get_linkedin_posts_classify_status(
    run_id: str,
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    validate_internal_trigger_token(x_internal_token)
    metrics = get_linkedin_posts_classify_only_metrics(run_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="Run ID not found.")
    return JSONResponse(content=metrics)


@app.post("/internal/send-slack-handover")
def internal_send_slack_handover(
    body: dict[str, Any] = Body(default_factory=dict),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    """
    Send Slack handovers by reading ``recruiters_info_{date}`` and ``linkedin_posts_relevant_{date}``.

    JSON body (all optional except flags default true):
    - ``run_date``: YYYY-MM-DD (default: today on server)
    - ``send_linkedin_post``: bool (default true)
    - ``send_recruiter_info``: recruiter LinkedIn profile case (default true)
    - ``send_internal_poc``: internal POC email case (default true)
    - Optional Slack overrides: ``webhook_url``, ``channel``, ``username``, ``icon_emoji``
    """
    validate_internal_trigger_token(x_internal_token)
    run_date = body.get("run_date")
    if run_date is not None and not isinstance(run_date, str):
        raise HTTPException(status_code=400, detail="run_date must be a string YYYY-MM-DD or omitted.")
    if run_date is None:
        run_date = _cron_today()

    def _bool_flag(key: str, default: bool = True) -> bool:
        if key not in body:
            return default
        v = body[key]
        if isinstance(v, bool):
            return v
        raise HTTPException(status_code=400, detail=f"{key} must be a boolean.")

    result = send_handover_notifications(
        run_date,
        send_linkedin_post=_bool_flag("send_linkedin_post", True),
        send_recruiter_info=_bool_flag("send_recruiter_info", True),
        send_internal_poc=_bool_flag("send_internal_poc", True),
        webhook_url=body.get("webhook_url") if isinstance(body.get("webhook_url"), str) else None,
        channel=body.get("channel") if isinstance(body.get("channel"), str) else None,
        username=body.get("username") if isinstance(body.get("username"), str) else None,
        icon_emoji=body.get("icon_emoji") if isinstance(body.get("icon_emoji"), str) else None,
    )
    try:
        result["handover_log_sync"] = sync_handover_log_to_sheet(run_date)
    except Exception as exc:
        logger.exception("handover_log_sync after internal send-slack-handover failed: %s", exc)
        result["handover_log_sync"] = {"error": str(exc)}
    return JSONResponse(content=jsonable_encoder(result))


@app.post("/internal/send-slack-handover-summary")
def internal_send_slack_handover_summary(
    body: dict[str, Any] = Body(default_factory=dict),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    """
    Send Slack handover summary counts.

    JSON body (all optional):
    - ``run_date``: YYYY-MM-DD (default: today in cron timezone)
    - ``send_linkedin_post``: bool (default true)
    - ``send_recruiter_info``: bool (default true)
    - ``send_internal_poc``: bool (default true)
    - Optional Slack overrides: ``webhook_url``, ``channel``, ``username``, ``icon_emoji``
    """
    validate_internal_trigger_token(x_internal_token)
    run_date = body.get("run_date")
    if run_date is not None and not isinstance(run_date, str):
        raise HTTPException(status_code=400, detail="run_date must be a string YYYY-MM-DD or omitted.")
    if run_date is None:
        run_date = _cron_today()

    def _bool_flag(key: str, default: bool = True) -> bool:
        if key not in body:
            return default
        v = body[key]
        if isinstance(v, bool):
            return v
        raise HTTPException(status_code=400, detail=f"{key} must be a boolean.")

    result = send_handover_summary_to_slack(
        run_date=run_date,
        send_linkedin_post=_bool_flag("send_linkedin_post", True),
        send_recruiter_info=_bool_flag("send_recruiter_info", True),
        send_internal_poc=_bool_flag("send_internal_poc", True),
        webhook_url=body.get("webhook_url") if isinstance(body.get("webhook_url"), str) else None,
        channel=body.get("channel") if isinstance(body.get("channel"), str) else None,
        username=body.get("username") if isinstance(body.get("username"), str) else None,
        icon_emoji=body.get("icon_emoji") if isinstance(body.get("icon_emoji"), str) else None,
    )
    return JSONResponse(content=jsonable_encoder(result))


@app.post("/internal/sync-handover-log")
def internal_sync_handover_log(
    run_date: Optional[str] = Query(
        default=None,
        description="Pipeline date YYYY-MM-DD (tabs recruiters_info_{date}, linkedin_posts_relevant_{date}). Defaults to today in cron timezone.",
    ),
    x_internal_token: Optional[str] = Header(default=None),
) -> JSONResponse:
    """
    Append rows to ``HANDOVER_LOG_SPREADSHEET_ID`` from recruiter + LinkedIn relevant sheets for ``run_date``.
    Requires Slack handover to have run first so ``assigned owner`` is populated (unless backfilling manually).
    """
    validate_internal_trigger_token(x_internal_token)
    if run_date is not None:
        run_date = run_date.strip()
        if not run_date:
            raise HTTPException(status_code=400, detail="run_date must be a non-empty YYYY-MM-DD string.")
    resolved = run_date or _cron_today()
    result = sync_handover_log_to_sheet(resolved)
    return JSONResponse(content=jsonable_encoder(result))


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
