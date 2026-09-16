"""Read-only Sheets audit access preserving numeric precision and time-of-day.

The normal project reader intentionally returns presentation-formatted cells.
Audits must instead read underlying values: a date display format must not
hide a stored timestamp. Integer date serials still remain date-only.
No writes, provider requests, or freshness fallback is performed here.
"""
from __future__ import annotations

import importlib
from typing import Any, Callable, Optional

MODULES = (
    "integrations.google_sheets_service", "core.integrations.google_sheets_service",
    "google_sheets_service", "core.google_sheets_service",
)


def make_reader(service_factory: Callable[[], Any], retry: Optional[Callable] = None) -> Callable:
    def read(spreadsheet_id: str, range_name: str) -> list:
        def operation() -> list:
            result = service_factory().spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name,
                majorDimension="ROWS", valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="SERIAL_NUMBER",
            ).execute()
            values = result.get("values", [])
            if not isinstance(values, list):
                raise TypeError("Sheets values response must be a list")
            return values
        return retry("Read Audit Range", operation) if retry else operation()
    return read


def resolve_audit_reader() -> Optional[Callable]:
    for name in MODULES:
        try:
            module = importlib.import_module(name)
        except ImportError:
            continue
        factory = getattr(module, "get_sheets_service", None)
        if callable(factory):
            retry = getattr(module, "_retry_sheet_op", None)
            return make_reader(factory, retry if callable(retry) else None)
    return None
