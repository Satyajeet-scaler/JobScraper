# India Jobs API (JobSpy)

Simple FastAPI service that fetches jobs for India-focused searches using `python-jobspy`.

## Python Version Requirement

Use **Python 3.11** for this project.

`python-jobspy` currently installs `numpy==1.26.3`, and with Python `3.14` pip may try to build NumPy from source and fail on macOS.

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If you already created a `3.14` venv, delete and recreate it:

```bash
deactivate 2>/dev/null || true
rm -rf .venv
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Run API

```bash
uvicorn main:app --reload
```

The API will be available at:

- `http://127.0.0.1:8000`
- Interactive docs: `http://127.0.0.1:8000/docs`
- Simple frontend: `http://127.0.0.1:8000/`

## Deploy To Railway

### Option A (recommended): Deploy via Dockerfile + GitHub

1. Push this project to a GitHub repository.
2. In Railway, click **New Project** -> **Deploy from GitHub repo**.
3. Select your repo. Railway will auto-detect the `Dockerfile`.
4. Deploy.
5. Open the generated Railway domain and test:
   - `/health`
   - `/docs`
   - `/` (frontend)

This project already includes:
- `Dockerfile` (Python 3.11, production uvicorn command)
- `.dockerignore`

### Option B: Railway CLI

```bash
npm i -g @railway/cli
railway login
railway init
railway up
```

After deploy:

```bash
railway open
```

## Endpoints

- `GET /health` - basic service health
- `GET /jobs` - fetch jobs from India-supported sources
  - defaults to LinkedIn, and you can pass one or more `site_name` values

### Example

```bash
curl "http://127.0.0.1:8000/jobs?search_term=python%20developer&location=India&results_wanted=15&hours_old=72"
```

Multiple sources example:

```bash
curl "http://127.0.0.1:8000/jobs?site_name=linkedin&site_name=indeed&search_term=software%20engineer&location=India&country_indeed=india"
```

### Query Params

- `site_name` (repeatable, default: `linkedin`)
  - Supported values: `linkedin`, `indeed`, `glassdoor`, `google`, `bayt`, `naukri`
- `search_term` (default: `software engineer`)
- `google_search_term` (required when `site_name=google`)
- `location` (default: `India`)
- `country_indeed` (default: `india`, for Indeed/Glassdoor)
- `results_wanted` (default: `20`, range: `1-100`)
- `hours_old` (optional, range: `1-720`)
- `linkedin_fetch_description` (default: `false`)
- `offset` (default: `0`)
- `verbose` (`0-2`)

### Site-specific constraints

- Google: requires `google_search_term`

## HireCafe Cloudflare Strategy

The HireCafe scraper now uses a guarded Cloudflare flow in browser mode:

- initial wait after page load
- challenge detection using page markers
- iframe checkbox click attempt + fallback coordinate click
- wait for challenge clearance
- post-verification wait so job APIs can load
- randomized scrolling delay to avoid fixed timing patterns

Tune via environment variables:

- `HIRECAFE_CLOUDFLARE_WAIT_SECONDS` (default `10`)
- `HIRECAFE_CF_CLEAR_TIMEOUT_SECONDS` (default `35`)
- `HIRECAFE_POST_VERIFY_WAIT_SECONDS` (default `8`)
- `HIRECAFE_CF_CLICK_X` / `HIRECAFE_CF_CLICK_Y` (default `544` / `334`)
- `HIRECAFE_MIN_SCROLL_DELAY_SECONDS` / `HIRECAFE_MAX_SCROLL_DELAY_SECONDS` (default `0.7` / `1.8`)
- `HIRECAFE_SCROLL_PIXELS` (default `1200`)
- `HIRECAFE_MAX_RUNTIME_SECONDS` (default `300`)
- `HIRECAFE_MAX_IDLE_SECONDS` (default `90`)
- `HIRECAFE_MAX_SCROLLS` (default `500`)
- `HIRECAFE_HEARTBEAT_EVERY_SECONDS` (default `15`)
