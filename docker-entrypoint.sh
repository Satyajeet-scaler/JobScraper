#!/bin/sh
set -e
# Virtual framebuffer: Chromium/Playwright get a display without a real monitor.
# xvfb-run sets DISPLAY (here :99) for this process and its children.
exec xvfb-run -n 99 -s "-ac -screen 0 1280x800x24 -noreset" \
  uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
