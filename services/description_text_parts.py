import logging
import os
from collections.abc import Iterable, Mapping
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_PART_CHAR_LIMIT = 48000
_DEFAULT_OVERFLOW_NOTICE = " ... [TRUNCATED: exceeded 3-column text storage limit]"


def read_positive_int_env(env_name: str, default: int) -> int:
    raw = (os.getenv(env_name) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning("invalid integer env var %s=%r; using default=%s", env_name, raw, default)
        return default
    return parsed if parsed > 0 else default


def resolve_sheet_text_part_limit() -> int:
    fallback_limit = read_positive_int_env("GOOGLE_SHEETS_MAX_CELL_CHARS", _DEFAULT_PART_CHAR_LIMIT)
    return read_positive_int_env("SHEETS_TEXT_PART_MAX_CHARS", fallback_limit)


def cap_text(value: Any, max_chars: int) -> str:
    text = _coerce_text(value)
    if max_chars <= 0:
        return text
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def combine_three_part_text(row: Mapping[str, Any], base_field: str) -> str:
    part_1 = _coerce_text(row.get(base_field))
    part_2 = _coerce_text(row.get(f"{base_field}_2"))
    part_3 = _coerce_text(row.get(f"{base_field}_3"))
    if part_2 or part_3:
        return f"{part_1}{part_2}{part_3}"
    return part_1


def with_three_part_text_columns(
    row: Mapping[str, Any],
    base_field: str,
    *,
    part_char_limit: int | None = None,
    overflow_notice: str = _DEFAULT_OVERFLOW_NOTICE,
) -> tuple[dict[str, Any], int]:
    effective_limit = max(1, part_char_limit or resolve_sheet_text_part_limit())
    text = combine_three_part_text(row, base_field)

    part_1 = text[:effective_limit]
    part_2 = text[effective_limit : effective_limit * 2]
    part_3 = text[effective_limit * 2 : effective_limit * 3]

    overflow_chars = max(0, len(text) - (effective_limit * 3))
    if overflow_chars > 0:
        safe_notice = overflow_notice or ""
        if len(safe_notice) >= effective_limit:
            safe_notice = safe_notice[: max(0, effective_limit - 1)]
        keep_chars = max(0, effective_limit - len(safe_notice))
        part_3 = text[effective_limit * 2 : (effective_limit * 2) + keep_chars] + safe_notice

    updated = dict(row)
    updated[base_field] = part_1
    updated[f"{base_field}_2"] = part_2
    updated[f"{base_field}_3"] = part_3
    return updated, overflow_chars


def apply_three_part_text_columns(
    rows: Iterable[Mapping[str, Any]],
    base_field: str,
    *,
    part_char_limit: int | None = None,
) -> tuple[list[dict[str, Any]], int, int]:
    output_rows: list[dict[str, Any]] = []
    overflow_rows = 0
    overflow_chars = 0

    for row in rows:
        updated, overflow = with_three_part_text_columns(
            row,
            base_field,
            part_char_limit=part_char_limit,
        )
        output_rows.append(updated)
        if overflow > 0:
            overflow_rows += 1
            overflow_chars += overflow

    return output_rows, overflow_rows, overflow_chars


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)
