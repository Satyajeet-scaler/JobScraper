# Production image for Railway (see railway.toml). Playwright needs Chromium + OS libs.
# Xvfb lets headed Chromium run in a container without a physical display (see docker-entrypoint.sh).
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV DISPLAY=:99

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    xvfb \
    x11-utils \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# LinkedIn automation: browser binaries and Debian deps (must run after pip install playwright)
RUN playwright install --with-deps chromium

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

COPY . /app

ENTRYPOINT ["docker-entrypoint.sh"]
