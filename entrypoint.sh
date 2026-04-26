#!/bin/sh
# Fix ownership of the Railway-mounted /data volume at runtime,
# then drop privileges to appuser before starting the app.

# Ensure /data is writable by appuser (volume is mounted as root)
if [ -d /data ]; then
    chown -R appuser:appuser /data 2>/dev/null || true
fi

# Export HOME and XAUTHORITY so xvfb-run works under gosu
export HOME=/home/appuser
export XAUTHORITY=/home/appuser/.Xauthority

exec gosu appuser xvfb-run -a --server-args="-screen 0 1920x1080x24" \
    uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
