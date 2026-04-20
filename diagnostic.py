#!/usr/bin/env python3
"""
Bot Fingerprint Audit — run inside the Docker container to see exactly what
Cloudflare (and any other bot-detector) observes about our browser profile.

Usage (from host):
  docker run --rm \
    -v "$PWD/debug_screenshots:/app/debug_screenshots" \
    jobscraper-hirecafe-debug:local \
    sh -c "xvfb-run -a --server-args='-screen 0 1920x1080x24' python diagnostic.py"

Output:
  debug_screenshots/audit_sannysoft.png    — visual pass/fail grid
  debug_screenshots/audit_fingerprint.png  — Canvas / WebGL / Audio fingerprints
  stdout                                    — key pass/fail lines
"""

import json
import os
import sys
import time

import undetected_chromedriver as uc

# ---------------------------------------------------------------------------
# Mirror the same stealth options used in hire_cafe._launch_hirecafe_driver
# so the audit reflects the production fingerprint.
# ---------------------------------------------------------------------------
_CHROME_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

OUTPUT_DIR = os.environ.get("DIAGNOSTIC_OUTPUT_DIR", "/app/debug_screenshots")

# ── Stealth JS (same as hire_cafe._inject_stealth_scripts) ────────────────
STEALTH_JS = """
delete Object.getPrototypeOf(navigator).webdriver;
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
const getParam = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(p) {
    if (p === 37445) return 'Google Inc. (Intel)';
    if (p === 37446) return 'ANGLE (Intel, Mesa Intel(R) UHD Graphics 630, OpenGL 4.6)';
    return getParam.call(this, p);
};
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [{
            name: 'Chrome PDF Plugin',
            description: 'Portable Document Format',
            filename: 'internal-pdf-viewer',
            length: 1,
            0: {type: 'application/x-google-chrome-pdf', suffixes: 'pdf',
                description: 'Portable Document Format', enabledPlugin: null}
        }];
        arr.refresh = () => {};
        Object.setPrototypeOf(arr, PluginArray.prototype);
        return arr;
    }
});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
"""


def build_options():
    """Build ChromeOptions identical to the production scraper."""
    opts = uc.ChromeOptions()
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-sandbox")
    opts.add_argument(f"--user-agent={_CHROME_UA}")
    # Explicitly use new headless to match Docker CMD
    opts.add_argument("--headless=new")
    return opts


def inject_stealth(driver):
    """Inject stealth overrides via CDP, same as production."""
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": STEALTH_JS},
        )
        driver.execute_cdp_cmd(
            "Emulation.setDeviceMetricsOverride",
            {
                "width": 1920,
                "height": 1080,
                "deviceScaleFactor": 1,
                "mobile": False,
                "screenWidth": 1920,
                "screenHeight": 1080,
            }
        )
    except Exception as exc:
        print(f"[WARN] CDP stealth injection failed: {exc}", file=sys.stderr)


def run_audit():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    system_browser = "/usr/bin/chromium"
    system_driver = "/usr/bin/chromedriver"

    kwargs = {"options": build_options()}
    kwargs["version_main"] = 147
    if os.path.isfile(system_browser):
        kwargs["browser_executable_path"] = system_browser
    if os.path.isfile(system_driver):
        kwargs["driver_executable_path"] = system_driver

    print("=" * 60)
    print("  BOT FINGERPRINT AUDIT")
    print("=" * 60)
    print(f"  Browser : {kwargs.get('browser_executable_path', 'bundled')}")
    print(f"  Driver  : {kwargs.get('driver_executable_path', 'bundled')}")
    print(f"  UA      : {_CHROME_UA}")
    print(f"  Output  : {OUTPUT_DIR}")
    print("=" * 60)

    driver = uc.Chrome(**kwargs)
    inject_stealth(driver)

    # ── 1. SannySoft bot detection audit ──────────────────────────────────
    print("\n[1/3] Loading bot.sannysoft.com …")
    driver.get("https://bot.sannysoft.com/")
    time.sleep(6)
    ss1 = os.path.join(OUTPUT_DIR, "audit_sannysoft.png")
    driver.save_screenshot(ss1)
    print(f"  → saved {ss1}")

    # Extract key results from the page
    try:
        results = driver.execute_script("""
            const rows = document.querySelectorAll('table tr');
            const out = {};
            for (const row of rows) {
                const cells = row.querySelectorAll('td');
                if (cells.length >= 2) {
                    const key = (cells[0].innerText || '').trim();
                    const val = (cells[1].innerText || '').trim();
                    const cls = cells[1].className || '';
                    if (key) out[key] = {value: val, pass: cls.includes('passed')};
                }
            }
            return out;
        """)
        print("\n  Key results:")
        critical_keys = [
            "User Agent", "WebDriver", "WebDriver (New)",
            "Chrome (New)", "Plugins", "Languages",
            "WebGL Vendor", "WebGL Renderer", "Hairline Feature",
            "Screen Size",
        ]
        for k in critical_keys:
            if k in results:
                r = results[k]
                status = "✅ PASS" if r["pass"] else "❌ FAIL"
                print(f"    {status}  {k}: {r['value'][:80]}")
            else:
                print(f"    ⚠️  SKIP  {k}: not found")

        # Dump full results to JSON
        results_path = os.path.join(OUTPUT_DIR, "audit_sannysoft.json")
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  → full results: {results_path}")
    except Exception as exc:
        print(f"  [WARN] Could not extract table results: {exc}")

    # ── 2. CreepJS fingerprint ────────────────────────────────────────────
    print("\n[2/3] Loading CreepJS fingerprint page …")
    try:
        driver.get("https://abrahamjuliot.github.io/creepjs/")
        time.sleep(10)
        ss2 = os.path.join(OUTPUT_DIR, "audit_creepjs.png")
        driver.save_screenshot(ss2)
        print(f"  → saved {ss2}")
    except Exception as exc:
        print(f"  [WARN] CreepJS failed: {exc}")

    # ── 3. Browser leaks quick check ──────────────────────────────────────
    print("\n[3/3] Loading browserleaks.com/javascript …")
    try:
        driver.get("https://browserleaks.com/javascript")
        time.sleep(6)
        ss3 = os.path.join(OUTPUT_DIR, "audit_browserleaks.png")
        driver.save_screenshot(ss3)
        print(f"  → saved {ss3}")
    except Exception as exc:
        print(f"  [WARN] BrowserLeaks failed: {exc}")

    driver.quit()
    print("\n" + "=" * 60)
    print("  AUDIT COMPLETE — review screenshots in debug_screenshots/")
    print("=" * 60)


if __name__ == "__main__":
    run_audit()
