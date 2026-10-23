"""Bounded native Sheets reads for the TFB Site; no quotes, writes, or models.

Only the fixed capture plans published in TFB_Cloud_Source_Sync.mjs are
accepted.  The Site remains responsible for selected-symbol matching and for
the complete capture's before/after generation and financial-schema checks.
"""
from __future__ import annotations

import json
import math
import re
import threading
import time
from dataclasses import dataclass
from typing import Any

SPREADSHEET_ID = "1QaOSNHqKlCaqlnaB45O4RlajWeMNe5cAE-r2HGwsG04"
SHEETS_URL = "https://sheets.googleapis.com/v4/spreadsheets/" + SPREADSHEET_ID
CELL_FIELDS = "formattedValue,effectiveValue,effectiveFormat(numberFormat)"
METADATA_FIELDS = ("spreadsheetId,properties(title,timeZone),"
                   "sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))")
GRID_FIELDS = ("spreadsheetId,properties(timeZone),sheets("
               "properties(sheetId,title,gridProperties(rowCount,columnCount)),"
               "data(startRow,startColumn,rowData(values(" + CELL_FIELDS + "))))")
MAX_BODY_BYTES = 4096
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_READ_SECONDS = 30
MAX_ENRICHMENT_ROWS = 20
MAX_CELLS = 42000
_READ_SLOTS = threading.BoundedSemaphore(2)
_A1 = re.compile(r"'([A-Za-z0-9_]+)'!A([1-9][0-9]*):([A-Z]+)([1-9][0-9]*)\Z")

# title: (fixed sheet ID, maximum capture rows, capture columns, required)
READ_LIMITS = {
    "_Status": (1752783095, 18, 13, True),
    "Top_10_Investments": (1333995836, 80, 30, True),
    "Portfolio_Decision": (917293608, 60, 26, True),
    "My_Portfolio": (83423516, 64, 122, True),
    "Daily_Review_2026_10_14": (20261014, 28, 8, False),
    "_Portfolio_CostBasis": (647241069, 1000, 23, False),
    "_Cash_Snapshot": (176574791, 1000, 5, False),
}
ENRICHMENT_LIMITS = {
    "Market_Leaders": (1952607146, 10000, 115, False),
    "Global_Markets": (1608764774, 10000, 115, False),
    "Commodities_FX": (1986097637, 10000, 115, False),
    "Mutual_Funds": (1905078810, 10000, 115, False),
}


class SourceReadError(Exception):
    def __init__(self, code: str, status: int = 502):
        super().__init__(code)
        self.code, self.status = code, status


def _invalid() -> None:
    raise SourceReadError("source_request_invalid", 422)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict:
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError("duplicate JSON field")
        out[key] = value
    return out


def _reject_constant(value: str) -> None:
    raise ValueError("nonfinite JSON number")


def decode_request(raw: bytes) -> dict:
    if len(raw) > MAX_BODY_BYTES:
        raise SourceReadError("source_request_too_large", 413)
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                           parse_constant=_reject_constant)
    except (ValueError, UnicodeError, RecursionError):
        _invalid()
    if not isinstance(value, dict):
        _invalid()
    if value == {"operation": "metadata"}:
        return value
    if (set(value) != {"operation", "ranges", "cellFields"}
            or value["operation"] != "cells" or value["cellFields"] != CELL_FIELDS
            or not isinstance(value["ranges"], list)
            or not 1 <= len(value["ranges"]) <= MAX_ENRICHMENT_ROWS
            or any(not isinstance(r, str) or len(r) > 100 or not _A1.fullmatch(r)
                   for r in value["ranges"])
            or len(set(value["ranges"])) != len(value["ranges"])):
        _invalid()
    return value


def _integer(value: Any, minimum: int = 1) -> bool:
    return type(value) is int and minimum <= value <= 10_000_000


def validated_metadata(raw: Any) -> dict:
    if (not isinstance(raw, dict) or raw.get("spreadsheetId") != SPREADSHEET_ID
            or not isinstance(raw.get("properties"), dict)
            or raw["properties"].get("timeZone") not in ("Etc/GMT-3", "Asia/Riyadh")
            or not isinstance(raw.get("sheets"), list) or len(raw["sheets"]) > 200):
        raise SourceReadError("source_metadata_changed")
    sheets, seen = [], set()
    limits = {**READ_LIMITS, **ENRICHMENT_LIMITS}
    for sheet in raw["sheets"]:
        p = sheet.get("properties") if isinstance(sheet, dict) else None
        if not isinstance(p, dict) or not isinstance(p.get("title"), str):
            raise SourceReadError("source_metadata_changed")
        title = p["title"]
        if title not in limits:
            continue  # No unrelated tab metadata is exposed by this relay.
        grid = p.get("gridProperties")
        if (title in seen or type(p.get("sheetId")) is not int
                or p["sheetId"] != limits[title][0] or not isinstance(grid, dict)
                or not _integer(grid.get("rowCount"))
                or not _integer(grid.get("columnCount"))):
            raise SourceReadError("source_metadata_changed")
        seen.add(title)
        sheets.append({"properties": {"title": title, "sheetId": p["sheetId"],
                       "gridProperties": {"rowCount": grid["rowCount"],
                                          "columnCount": grid["columnCount"]}}})
    if any(required and title not in seen for title, (_, _, _, required) in READ_LIMITS.items()):
        raise SourceReadError("source_required_tab_missing")
    properties = {"timeZone": raw["properties"]["timeZone"]}
    title = raw["properties"].get("title")
    if isinstance(title, str) and len(title) <= 1000:
        properties["title"] = title
    return {"spreadsheetId": SPREADSHEET_ID, "properties": properties, "sheets": sheets}


def _column(number: int) -> str:
    out = ""
    while number:
        number, digit = divmod(number - 1, 26)
        out = chr(65 + digit) + out
    return out


def _sheet_map(metadata: dict) -> dict:
    return {s["properties"]["title"]: s["properties"] for s in metadata["sheets"]}


def capture_plans(metadata: dict) -> dict[str, list[str]]:
    sheets = _sheet_map(metadata)

    def rectangle(title: str, rows: int, cols: int) -> str | None:
        if title not in sheets:
            return None
        grid = sheets[title]["gridProperties"]
        return f"'{title}'!A1:{_column(min(cols, grid['columnCount']))}{min(rows, grid['rowCount'])}"

    def plan(specs: list[tuple[str, int, int]]) -> list[str]:
        return [value for title, rows, cols in specs if (value := rectangle(title, rows, cols))]

    out = {
        "perimeter": plan([("_Status", 18, 13), ("_Portfolio_CostBasis", 1000, 23),
                           ("Top_10_Investments", 16, 2), ("Portfolio_Decision", 5, 3),
                           ("_Cash_Snapshot", 1000, 5)]),
        "core": plan([("Top_10_Investments", 80, 30), ("Portfolio_Decision", 60, 26),
                      ("My_Portfolio", 64, 122), ("Daily_Review_2026_10_14", 28, 8)]),
    }
    if all(title in sheets and sheets[title]["gridProperties"]["rowCount"] <= 10000
           and sheets[title]["gridProperties"]["columnCount"] >= 115
           for title in ENRICHMENT_LIMITS):
        indices = []
        for title in ENRICHMENT_LIMITS:
            rows = sheets[title]["gridProperties"]["rowCount"]
            indices.append(f"'{title}'!A1:DK1")
            if rows > 1:
                indices.append(f"'{title}'!A2:A{rows}")
        out["index"] = indices
    return out


@dataclass(frozen=True)
class ReadRectangle:
    title: str
    first_row: int  # zero-based, inclusive
    last_row: int   # zero-based, exclusive
    columns: int


def authorized_ranges(ranges: list[str], metadata: dict) -> list[ReadRectangle]:
    sheets = _sheet_map(metadata)
    rectangles = []
    for value in ranges:
        match = _A1.fullmatch(value)
        if not match:
            _invalid()
        title, first, end_col, last = match.groups()
        columns = 0
        for letter in end_col:
            columns = columns * 26 + ord(letter) - 64
        first, last = int(first), int(last)
        if title not in sheets:
            _invalid()
        grid = sheets[title]["gridProperties"]
        if first > last or last > grid["rowCount"] or columns > grid["columnCount"]:
            _invalid()
        rectangles.append(ReadRectangle(title, first - 1, last, columns))
    plans = capture_plans(metadata)
    is_plan = ranges in plans.values()
    is_enrichment = ("index" in plans and len(rectangles) <= MAX_ENRICHMENT_ROWS
                     and all(r.title in ENRICHMENT_LIMITS and r.first_row >= 1
                             and r.last_row == r.first_row + 1 and r.columns == 115
                             for r in rectangles))
    if not (is_plan or is_enrichment) or sum((r.last_row-r.first_row)*r.columns for r in rectangles) > MAX_CELLS:
        _invalid()
    return rectangles


def _valid_cell(cell: Any) -> bool:
    if not isinstance(cell, dict) or set(cell) - {"formattedValue", "effectiveValue", "effectiveFormat"}:
        return False
    if "formattedValue" in cell and not isinstance(cell["formattedValue"], str):
        return False
    if "effectiveValue" in cell:
        value = cell["effectiveValue"]
        if not isinstance(value, dict) or len(value) != 1:
            return False
        key, v = next(iter(value.items()))
        if (key == "stringValue" and not isinstance(v, str)
                or key == "boolValue" and type(v) is not bool
                or key == "numberValue" and (type(v) not in (int, float) or not math.isfinite(v))
                or key == "errorValue" and (not isinstance(v, dict)
                    or set(v) - {"type", "message"}
                    or any(not isinstance(item, str) for item in v.values()))
                or key not in {"stringValue", "boolValue", "numberValue", "errorValue"}):
            return False
    if "effectiveFormat" in cell:
        fmt = cell["effectiveFormat"]
        if not isinstance(fmt, dict) or set(fmt) - {"numberFormat"}:
            return False
        if "numberFormat" in fmt:
            number = fmt["numberFormat"]
            if (not isinstance(number, dict) or set(number) - {"type", "pattern"}
                    or any(not isinstance(v, str) for v in number.values())):
                return False
    return True


def validate_cells(raw: Any, metadata: dict, rectangles: list[ReadRectangle]) -> dict:
    def fail() -> None:
        raise SourceReadError("source_grid_changed")

    if (not isinstance(raw, dict) or set(raw) - {"spreadsheetId", "properties", "sheets"}
            or raw.get("spreadsheetId") != SPREADSHEET_ID
            or raw.get("properties") != {"timeZone": metadata["properties"]["timeZone"]}
            or not isinstance(raw.get("sheets"), list)):
        fail()
    sheets, seen, seen_origins = _sheet_map(metadata), set(), set()
    requested = {r.title for r in rectangles}
    for sheet in raw["sheets"]:
        if not isinstance(sheet, dict) or set(sheet) - {"properties", "data"}:
            fail()
        properties = sheet.get("properties")
        title = properties.get("title") if isinstance(properties, dict) else None
        if title not in requested or title in seen or properties != sheets[title]:
            fail()
        seen.add(title)
        blocks = sheet.get("data")
        if not isinstance(blocks, list) or not blocks:
            fail()
        for block in blocks:
            if not isinstance(block, dict) or set(block) - {"startRow", "startColumn", "rowData"}:
                fail()
            row, col = block.get("startRow", 0), block.get("startColumn", 0)
            if not _integer(row, 0) or type(col) is not int or col != 0 or (title, row) in seen_origins:
                fail()
            matches = [r for r in rectangles if r.title == title and r.first_row == row]
            if len(matches) != 1:
                fail()
            seen_origins.add((title, row))
            rectangle = matches[0]
            rows = block.get("rowData", [])
            if not isinstance(rows, list) or row + len(rows) > rectangle.last_row:
                fail()
            for data in rows:
                if not isinstance(data, dict) or set(data) - {"values"}:
                    fail()
                values = data.get("values", [])
                if (not isinstance(values, list) or len(values) > rectangle.columns
                        or any(not _valid_cell(cell) for cell in values)):
                    fail()
    if seen != requested or seen_origins != {(r.title, r.first_row) for r in rectangles}:
        fail()
    return raw  # Native values, displays, formats, and coordinates stay intact.


def _fetch(session: Any, *, fields: str, ranges: list[str] | None, deadline: float) -> dict:
    if time.monotonic() >= deadline:
        raise SourceReadError("source_read_timeout", 504)
    params = [("fields", fields), ("includeGridData", "true" if ranges else "false")]
    params.extend(("ranges", value) for value in (ranges or []))
    remaining = max(0.1, deadline - time.monotonic())
    with session.get(SHEETS_URL, params=params, stream=True, allow_redirects=False,
                     timeout=(min(5, remaining), min(10, remaining)),
                     max_allowed_time=remaining,
                     headers={"Cache-Control": "no-cache", "Accept": "application/json"}) as response:
        if response.status_code != 200:
            raise SourceReadError("source_unavailable")
        length = response.headers.get("Content-Length", "")
        if length.isdigit() and int(length) > MAX_RESPONSE_BYTES:
            raise SourceReadError("source_response_too_large")
        content = bytearray()
        for chunk in response.iter_content(chunk_size=65536):
            if time.monotonic() >= deadline:
                raise SourceReadError("source_read_timeout", 504)
            if len(content) + len(chunk) > MAX_RESPONSE_BYTES:
                raise SourceReadError("source_response_too_large")
            content.extend(chunk)
    try:
        value = json.loads(content.decode("utf-8"), object_pairs_hook=_unique_object,
                           parse_constant=_reject_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise SourceReadError("source_response_invalid") from None
    if not isinstance(value, dict):
        raise SourceReadError("source_response_invalid")
    return value


def read_source(body: dict) -> dict:
    """Read metadata afresh for every operation, without storing source data."""
    if not _READ_SLOTS.acquire(blocking=False):
        raise SourceReadError("source_read_busy", 429)
    session = None
    try:
        # Lazy import: mounting this optional read surface never initializes a
        # Google client, loads credentials, or touches the investment engine.
        from integrations.google_sheets_service import create_readonly_sheets_session

        session = create_readonly_sheets_session()
        deadline = time.monotonic() + MAX_READ_SECONDS
        metadata = validated_metadata(_fetch(session, fields=METADATA_FIELDS, ranges=None, deadline=deadline))
        if body["operation"] == "metadata":
            return metadata
        rectangles = authorized_ranges(body["ranges"], metadata)
        cells = _fetch(session, fields=GRID_FIELDS, ranges=body["ranges"], deadline=deadline)
        return validate_cells(cells, metadata, rectangles)
    except SourceReadError:
        raise
    except Exception:
        # Never send or log provider exceptions, native source cells, URLs
        # containing credentials, request headers, or credential-manager data.
        raise SourceReadError("source_unavailable") from None
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
        _READ_SLOTS.release()
