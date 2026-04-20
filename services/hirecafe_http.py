"""
TLS-fingerprint-aligned HTTP helper for HireCafe secondary JSON fetches.

Uses ``curl_cffi`` to present a Chrome-131-like TLS fingerprint, ensuring
that out-of-browser ``/viewjob/*.json`` requests look identical to what
the real Chrome session would send.

Usage::

    from services.hirecafe_http import hirecafe_fetch_json
    data = hirecafe_fetch_json("https://hiring.cafe/_next/data/.../viewjob/abc.json")
"""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Must match the UA injected in hire_cafe._CHROME_UA.
_CHROME_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

_DEFAULT_HEADERS = {
    "User-Agent": _CHROME_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="147", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


def hirecafe_fetch_json(
    url: str,
    *,
    timeout: float = 15.0,
    extra_headers: Optional[dict[str, str]] = None,
    cookies: Optional[dict[str, str]] = None,
) -> Optional[dict[str, Any]]:
    """Fetch *url* using a Chrome-131 TLS fingerprint and return parsed JSON.

    Returns ``None`` on any failure (network error, non-200 status, decode
    error) so the caller can fall back gracefully.
    """
    try:
        from curl_cffi import requests as cffi_requests  # type: ignore[import-untyped]
    except ImportError:
        logger.warning(
            "hirecafe_http curl_cffi not installed – falling back to stdlib urllib"
        )
        return _fallback_fetch(url, timeout=timeout)

    headers = {**_DEFAULT_HEADERS, **(extra_headers or {})}
    try:
        resp = cffi_requests.get(
            url,
            headers=headers,
            cookies=cookies,
            timeout=timeout,
            impersonate="chrome131",
        )
        if resp.status_code != 200:
            logger.debug(
                "hirecafe_http status=%s for %s", resp.status_code, url[:80]
            )
            return None
        return resp.json()  # type: ignore[no-any-return]
    except Exception as exc:
        logger.debug("hirecafe_http fetch error: %s %s", type(exc).__name__, url[:80])
        return None


def _fallback_fetch(
    url: str,
    *,
    timeout: float = 15.0,
) -> Optional[dict[str, Any]]:
    """Best-effort fetch using stdlib ``urllib`` when ``curl_cffi`` is absent."""
    import json
    import urllib.request

    req = urllib.request.Request(url, headers={**_DEFAULT_HEADERS})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode("utf-8"))  # type: ignore[no-any-return]
    except Exception:
        return None
