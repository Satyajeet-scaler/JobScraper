#!/bin/sh
set -e
# Virtual framebuffer so Chromium/Playwright see DISPLAY=:99 without a real monitor.
# (xvfb-run can fail in some cloud runtimes; starting Xvfb + wait is more reliable.)

Xvfb :99 -screen 0 1280x800x24 -ac +extension RANDR -nolisten tcp -noreset &
export DISPLAY=:99

# Wait until the X server accepts connections (avoids racing uvicorn before X is up).
i=0
while [ "$i" -lt 60 ]; do
  if xdpyinfo -display :99 >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.25
done
if ! xdpyinfo -display :99 >/dev/null 2>&1; then
  echo "docker-entrypoint: Xvfb did not become ready on :99" >&2
  exit 1
fi

exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
