# Production image for Railway (see railway.toml). Playwright needs Chromium + OS libs for headless use.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    chromium \
    chromium-driver \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Playwright Chromium (e.g. headless LinkedIn flows); install browsers after pip
RUN playwright install --with-deps chromium

COPY . /app

CMD sh -c "xvfb-run -a uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"
