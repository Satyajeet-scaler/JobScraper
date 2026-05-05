#!/bin/sh
set -e
# Enable tracing for debugging if needed (remove # if you want to see every command)
# set -x

echo "[*] Entrypoint started as $(whoami)"

# Ensure /data exists and is writable by appuser
if [ -d /data ]; then
    echo "[*] Fixing permissions on /data..."
    chown -R appuser:appuser /data 2>/dev/null || true
    echo "[*] /data permissions fixed."
else
    echo "[!] /data directory not found, skipping chown."
fi

# Create a setuid helper so the unprivileged appuser can drop kernel page caches.
# This is the main lever for reducing Railway's billed cgroup memory after heavy I/O.
cat > /usr/local/bin/drop_page_cache << 'HELPEREOF'
#!/bin/sh
# setuid root helper — must be installed by root before dropping privileges.
sync
echo 1 > /proc/sys/vm/drop_caches
HELPEREOF
chmod 4755 /usr/local/bin/drop_page_cache
echo "[*] setuid helper /usr/local/bin/drop_page_cache installed."

# Ensure appuser home is writable for xvfb authority files
# Export environment
export HOME=/home/appuser
export XAUTHORITY=/home/appuser/.Xauthority
export DISPLAY=:99
export PYTHONUNBUFFERED=1

echo "[*] Starting Xvfb in background on ${DISPLAY}..."
# Start Xvfb
Xvfb :99 -screen 0 1920x1080x24 -ac +extension RANDR +render -noreset >> /tmp/xvfb.log 2>&1 &
XVFB_PID=$!

# Wait for Xvfb to be ready
echo "[*] Waiting for Xvfb to be ready..."
timeout 10 sh -c "until xset -display ${DISPLAY} q > /dev/null 2>&1; do sleep 0.1; done" || {
    echo "[!] Xvfb failed to start. Logs:"
    cat /tmp/xvfb.log
    exit 1
}
echo "[*] Xvfb is ready."

# Drop privileges and start uvicorn
echo "[*] Starting uvicorn as appuser..."
exec gosu appuser uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level info



