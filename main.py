import math
from pathlib import Path
from typing import Literal, Optional

from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from jobspy import scrape_jobs
from pandas import DataFrame


app = FastAPI(
    title="India Jobs API",
    description="Fetch India-focused job listings from supported JobSpy sources.",
    version="1.0.0",
)
BASE_DIR = Path(__file__).resolve().parent

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


@app.get("/health")
def health_check() -> dict:
    return {"status": "ok"}


@app.get("/")
def home() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


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
