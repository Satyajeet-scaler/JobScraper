# Production image for Railway (see railway.toml). Playwright needs Chromium + OS libs for headless use.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Limit glibc malloc arenas to reduce memory fragmentation across threads.
# Default is 8*ncpus; each arena retains freed memory independently,
# preventing malloc_trim from releasing it back to the OS.
ENV MALLOC_ARENA_MAX=2
# Allocations >= 64KB use mmap instead of sbrk; mmap'd memory is returned
# to the OS immediately when freed (no fragmentation).
ENV MALLOC_MMAP_THRESHOLD_=65536
ENV MALLOC_TRIM_THRESHOLD_=131072
ENV MALLOC_MMAP_MAX_=65536

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    chromium \
    chromium-driver \
    xauth \
    xvfb \
    fontconfig \
    fonts-liberation \
    fonts-dejavu-core \
    fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Playwright Chromium (e.g. headless LinkedIn flows); install browsers after pip
RUN playwright install --with-deps chromium

COPY . /app

# Non-root user — reduces detection of headless bots running as uid 0
# Give the user ownership of chromedriver so undetected_chromedriver can patch it
RUN groupadd -g 1000 appuser && useradd -u 1000 -g appuser -m -d /home/appuser appuser \
    && chown -R appuser:appuser /app \
    && chown appuser:appuser /usr/bin/chromedriver
USER appuser

# Realistic screen resolution (1920x1080x24) for Xvfb
CMD sh -c "xvfb-run -a --server-args='-screen 0 1920x1080x24' uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"
