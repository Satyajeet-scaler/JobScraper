from __future__ import annotations

from typing import Any
from urllib.parse import urlparse


def normalize_job_url(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    netloc = parsed.netloc.lower().strip()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = (parsed.path or "").rstrip("/")
    return f"{netloc}{path}".strip()


def canonicalize_recruiter_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    assigned_owner = out.get("assigned_owner")
    if not assigned_owner:
        assigned_owner = out.get("assigned owner")
    out["assigned_owner"] = str(assigned_owner or "").strip()

    raw_tab = str(out.get("relevant_jobs_tab") or "").strip()
    out["relevant_tab_name"] = ""
    out["relevant_row_number"] = None
    if raw_tab:
        try:
            out["relevant_row_number"] = int(raw_tab)
        except ValueError:
            out["relevant_tab_name"] = raw_tab

    description = " ".join(
        part.strip()
        for part in (
            str(out.get("description") or ""),
            str(out.get("description_2") or ""),
            str(out.get("description_3") or ""),
        )
        if part and part.strip()
    ).strip()
    if description:
        out["description_full"] = description

    out["job_url_normalized"] = normalize_job_url(out.get("job_url"))
    return out
