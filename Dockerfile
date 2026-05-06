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
# Playwright browsers in a shared location (not /root/.cache)
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers

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
    gosu \
    x11-xserver-utils \
    libgtk-3-0 \
    libasound2 \
    libx11-xcb1 \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — reduces bot detection flags from running as uid 0
RUN groupadd -r appuser && useradd -r -g appuser -m -d /home/appuser appuser

# No longer patching chromedriver for undetected-chromedriver

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Playwright Chromium & Camoufox Firefox
# Install browser binaries as root, then ensure appuser owns the cache
RUN playwright install --with-deps chromium \
    && python3 -m camoufox fetch \
    && mkdir -p /home/appuser/.cache/camoufox \
    && (cp -r /root/.cache/camoufox/* /home/appuser/.cache/camoufox/ 2>/dev/null || true) \
    && chown -R appuser:appuser /home/appuser/.cache /opt/pw-browsers \
    && chmod -R 755 /home/appuser/.cache /opt/pw-browsers

COPY . /app

# Compile the setuid root helper so the unprivileged appuser can drop page caches.
RUN gcc -o /usr/local/bin/drop_page_cache /app/drop_page_cache.c \
    && chmod 4755 /usr/local/bin/drop_page_cache \
    && rm /app/drop_page_cache.c

# Ensure app dir and browser cache are accessible by appuser
RUN mkdir -p /data/chrome_profile /home/appuser/.cache \
    && chown -R appuser:appuser /app /data /home/appuser

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
