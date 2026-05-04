"""Persist and retrieve the last assigned owner for Slack handover round-robin.

Uses MySQL so state survives across process restarts and deployment replacements.
"""

from __future__ import annotations

import logging
from typing import Any

from services.mysql_recruiter_store import _db

logger = logging.getLogger(__name__)


def _owner_stable_identifier(owner: dict[str, str]) -> str:
    """Return the most stable identifier for an owner row.

    Priority: slack_id > email > name.  This lets the sheet be reordered
    without losing continuity.
    """
    sid = (owner.get("owner_slack_id") or "").strip()
    if sid:
        return sid
    email = (owner.get("owner_email") or "").strip()
    if email:
        return email
    name = (owner.get("owner_name") or "").strip()
    if name:
        return name
    return ""


def _find_owner_index(owner_rows: list[dict[str, str]], identifier: str) -> int:
    """Return the index of the owner whose stable id matches *identifier*.

    Returns ``-1`` when no match is found.
    """
    if not identifier:
        return -1
    for idx, owner in enumerate(owner_rows):
        if _owner_stable_identifier(owner) == identifier:
            return idx
    return -1


def get_start_owner_index(
    state_key: str,
    owner_rows: list[dict[str, str]],
) -> int:
    """Return the owner index that the next handover run should start from.

    Logic:
        1. Read ``last_owner_identifier`` from MySQL for *state_key*.
        2. If none → return ``0``.
        3. Find the owner's current position in *owner_rows*.
        4. If found at index ``N`` → return ``(N + 1) % len(owner_rows)``.
        5. If not found (owner removed) → return ``0``.
    """
    if not owner_rows:
        return 0

    last_id = ""
    try:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_owner_identifier FROM handover_owner_state WHERE state_key = %s",
                    (state_key,),
                )
                row = cur.fetchone()
                if row:
                    last_id = (row.get("last_owner_identifier") or "").strip()
    except Exception as exc:
        logger.warning("handover_owner_state get failed key=%s err=%s", state_key, exc)
        return 0

    if not last_id:
        return 0

    last_idx = _find_owner_index(owner_rows, last_id)
    if last_idx == -1:
        logger.info(
            "handover_owner_state previous owner not found in current sheet key=%s id=%s; resetting",
            state_key,
            last_id,
        )
        return 0

    return (last_idx + 1) % len(owner_rows)


def update_last_owner(
    state_key: str,
    owner_rows: list[dict[str, str]],
    last_assigned_index: int,
) -> None:
    """Persist the identifier of the owner at *last_assigned_index*.

    Does nothing when *owner_rows* is empty or the index is out of range.
    """
    if not owner_rows or last_assigned_index < 0 or last_assigned_index >= len(owner_rows):
        return

    identifier = _owner_stable_identifier(owner_rows[last_assigned_index])
    if not identifier:
        logger.warning(
            "handover_owner_state cannot persist owner with empty identifier key=%s idx=%s",
            state_key,
            last_assigned_index,
        )
        return

    try:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO handover_owner_state (state_key, last_owner_identifier)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        last_owner_identifier = VALUES(last_owner_identifier),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (state_key, identifier),
                )
    except Exception as exc:
        logger.warning(
            "handover_owner_state update failed key=%s id=%s err=%s",
            state_key,
            identifier,
            exc,
        )
