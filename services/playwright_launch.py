"""
Shared Playwright/Chromium launch options for production scrapers.

Why these flags matter on Railway:
  - --disable-dev-shm-usage: Chromium puts IPC/GPU shared memory in /dev/shm
    by default. /dev/shm is a tmpfs and every byte counts toward the
    container cgroup memory. Forcing Chromium to use /tmp instead lets us
    reclaim it (we delete /tmp leftovers in main._cleanup_browser_tmpfs).
  - --no-sandbox: required when running as a non-root user inside many
    Linux containers; otherwise Chromium fails to start.
  - --disable-gpu / --disable-software-rasterizer: no display server, no
    point keeping a renderer cache around.
  - --disable-extensions / --disable-component-extensions-with-background-pages:
    drop a chunk of RSS we never need server-side.
  - --memory-pressure-off: tells Chromium not to grow renderer caches based
    on detected free memory; we want it to release pages aggressively.
  - --renderer-process-limit=1: single renderer process; we only ever scrape
    one tab at a time.
"""
from __future__ import annotations

CHROMIUM_LOW_MEMORY_ARGS: list[str] = [
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-extensions",
    "--disable-component-extensions-with-background-pages",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-breakpad",
    "--disable-client-side-phishing-detection",
    "--disable-default-apps",
    "--disable-hang-monitor",
    "--disable-popup-blocking",
    "--disable-prompt-on-repost",
    "--disable-sync",
    "--metrics-recording-only",
    "--no-first-run",
    "--mute-audio",
    "--memory-pressure-off",
    "--renderer-process-limit=1",
]


def chromium_launch_kwargs(headless: bool = True) -> dict:
    """Standard kwargs for ``playwright.chromium.launch`` in production scrapers."""
    return {
        "headless": headless,
        "args": list(CHROMIUM_LOW_MEMORY_ARGS),
    }
