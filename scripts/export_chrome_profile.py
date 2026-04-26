import io
import json
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

# Ensure we can import from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
import undetected_chromedriver as uc


TARGET_URL = "https://hiring.cafe"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "chrome_profile"

# Files/dirs to strip — they block profile reuse
LOCK_FILES = ("SingletonLock", "SingletonCookie", "SingletonSocket")


def main() -> None:
    tmp_profile = tempfile.mkdtemp(prefix="hirecafe_profile_")
    print(f"[*] Temp profile: {tmp_profile}")

    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={tmp_profile}")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    print("[*] Launching Chrome — navigate and pass Cloudflare manually...")
    driver = uc.Chrome(options=options, version_main=147)

    try:
        driver.get(TARGET_URL)
        input(
            "\n"
            ">>> Browser is open. Pass the Cloudflare challenge manually.\n"
            ">>> Once the hiring.cafe page loads fully, press ENTER here to save the profile.\n"
        )
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # Copy profile to output dir
    if OUTPUT_DIR.exists():
        print(f"[*] Removing old profile at {OUTPUT_DIR}")
        shutil.rmtree(OUTPUT_DIR)

    print(f"[*] Copying profile to {OUTPUT_DIR}")
    # Ignore lock files/sockets that cause shutil.copytree to fail
    shutil.copytree(
        tmp_profile,
        str(OUTPUT_DIR),
        ignore=shutil.ignore_patterns("Singleton*", "lockfile", "*.pma"),
    )

    # Clean up temp
    shutil.rmtree(tmp_profile, ignore_errors=True)

    profile_size = sum(
        f.stat().st_size for f in OUTPUT_DIR.rglob("*") if f.is_file()
    )
    print(f"\n[✓] Profile saved locally: {OUTPUT_DIR}")
    print(f"    Size: {profile_size / 1024 / 1024:.1f} MB")

    # Optional Remote Upload
    print("\n" + "=" * 60)
    print("OPTIONAL: Upload profile to Railway server")
    print("=" * 60)
    server_url = input("Server Base URL (e.g. https://your-app.railway.app) [skip]: ").strip()
    if not server_url:
        print("[*] Skipped upload.")
        return

    token = input("INTERNAL_TRIGGER_TOKEN: ").strip()
    if not token:
        print("[!] Token required for upload. Skipped.")
        return

    print(f"[*] Zipping profile for upload...")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(OUTPUT_DIR):
            for file in files:
                p = Path(root) / file
                zf.write(p, p.relative_to(OUTPUT_DIR))

    zip_buffer.seek(0)
    endpoint = f"{server_url.rstrip('/')}/internal/hirecafe/upload-profile"
    print(f"[*] Uploading to {endpoint}...")

    try:
        resp = requests.post(
            endpoint,
            files={"file": ("profile.zip", zip_buffer, "application/zip")},
            headers={"x-internal-token": token},
            timeout=120,
        )
        if resp.status_code == 200:
            print(f"[✓] Success: {resp.json()}")
        else:
            print(f"[!] Upload failed ({resp.status_code}): {resp.text}")
    except Exception as exc:
        print(f"[!] Upload error: {exc}")


if __name__ == "__main__":
    main()
