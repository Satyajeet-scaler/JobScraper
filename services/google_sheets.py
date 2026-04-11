import json
import logging
import os
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
logger = logging.getLogger(__name__)


class GoogleSheetsWriter:
    """Writes pipeline outputs into date-based Google Sheet tabs."""

    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.client = gspread.authorize(self._build_credentials())
        self.sheet = self.client.open_by_key(self.spreadsheet_id)

    def _build_credentials(self) -> Credentials:
        service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")

        if service_account_json:
            info = json.loads(service_account_json)
            return Credentials.from_service_account_info(info, scopes=SCOPES)

        if service_account_file:
            return Credentials.from_service_account_file(service_account_file, scopes=SCOPES)

        raise RuntimeError(
            "Missing Google credentials. Set GOOGLE_SERVICE_ACCOUNT_JSON "
            "or GOOGLE_SERVICE_ACCOUNT_FILE."
        )

    def write_rows(
        self,
        worksheet_title: str,
        rows: list[dict[str, Any]],
        chunk_size: int = 200,
    ) -> None:
        headers = self._derive_headers(rows)
        worksheet = self._replace_worksheet(
            worksheet_title=worksheet_title,
            row_count=max(2, len(rows) + 1),
            col_count=max(1, len(headers)),
        )
        worksheet.update("A1", [headers])

        if not rows:
            return

        batch_values: list[list[str]] = []
        for row in rows:
            batch_values.append([self._stringify(row.get(col)) for col in headers])

        start_row = 2
        for idx in range(0, len(batch_values), chunk_size):
            chunk = batch_values[idx : idx + chunk_size]
            end_row = start_row + len(chunk) - 1
            end_col_letter = self._column_letter(len(headers))
            worksheet.update(f"A{start_row}:{end_col_letter}{end_row}", chunk)
            start_row = end_row + 1

    def append_to_worksheet(
        self,
        worksheet_title: str,
        data_rows: list[list[str]],
        *,
        header_row: list[str] | None = None,
        chunk_size: int = 200,
    ) -> None:
        """
        Append rows to an existing worksheet, or create the tab if missing.
        If the worksheet is empty, writes ``header_row`` first (if provided), then data.
        """
        if not data_rows and not header_row:
            return

        try:
            ws = self.sheet.worksheet(worksheet_title)
        except gspread.WorksheetNotFound:
            col_n = max(
                len(header_row) if header_row else 0,
                max((len(r) for r in data_rows), default=0),
                8,
            )
            row_n = max(len(data_rows) + (2 if header_row else 1) + 10, 100)
            ws = self.sheet.add_worksheet(title=worksheet_title, rows=row_n, cols=col_n)

        existing = ws.get_all_values()
        if not existing:
            if header_row:
                ws.append_rows([header_row], value_input_option="USER_ENTERED")

        if not data_rows:
            return

        for idx in range(0, len(data_rows), chunk_size):
            chunk = data_rows[idx : idx + chunk_size]
            safe_chunk: list[list[str]] = []
            for row in chunk:
                safe_row = [self._stringify(c) for c in row]
                safe_chunk.append(safe_row)
            ws.append_rows(safe_chunk, value_input_option="USER_ENTERED")

    def _replace_worksheet(
        self,
        worksheet_title: str,
        row_count: int,
        col_count: int,
    ):
        existing = self._find_worksheet(worksheet_title)
        if existing:
            self.sheet.del_worksheet(existing)

        return self.sheet.add_worksheet(
            title=worksheet_title,
            rows=row_count,
            cols=col_count,
        )

    def _find_worksheet(self, worksheet_title: str):
        try:
            return self.sheet.worksheet(worksheet_title)
        except gspread.WorksheetNotFound:
            return None

    @staticmethod
    def _derive_headers(rows: list[dict[str, Any]]) -> list[str]:
        seen: set[str] = set()
        headers: list[str] = []
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    headers.append(key)
        return headers or ["message"]

    @staticmethod
    def _stringify(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            # Job payloads may include date/datetime objects from pandas/JobSpy.
            # `default=str` keeps sheet writes resilient instead of failing serialization.
            text = json.dumps(value, ensure_ascii=True, default=str)
        else:
            text = str(value)

        # Google Sheets rejects any cell > 50,000 characters.
        # Keep a small safety buffer and truncate consistently.
        max_cell_chars = int(os.getenv("GOOGLE_SHEETS_MAX_CELL_CHARS", "48000"))
        if len(text) > max_cell_chars:
            logger.warning(
                "google sheets cell value truncated original_len=%s max_len=%s",
                len(text),
                max_cell_chars,
            )
            truncated_notice = " ... [TRUNCATED: exceeded Google Sheets cell limit]"
            keep = max(0, max_cell_chars - len(truncated_notice))
            return text[:keep] + truncated_notice
        return text

    @staticmethod
    def _column_letter(index: int) -> str:
        letters = ""
        current = index
        while current > 0:
            current, remainder = divmod(current - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters
