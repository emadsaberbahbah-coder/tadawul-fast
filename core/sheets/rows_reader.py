# -*- coding: utf-8 -*-
"""
core/sheets/rows_reader.py
================================================================================
TADAWUL FAST BRIDGE — CANONICAL SHEET ROWS READER (v1.0.0)
================================================================================

WHY THIS MODULE EXISTS
----------------------
data_engine_v2.DataEngineV5._bind_rows_reader() resolves a "rows reader" by:
    1. returning an injected self._rows_reader_input (constructor arg), else
    2. import_module("core.sheets.rows_reader" | "sheets.rows_reader" |
       "core.rows_reader") and instantiating RowsReader / Reader, else
    3. returning None.

The factory get_engine(rows_reader=None) never injects path (1), and the module
in path (2) -- THIS FILE -- did not exist, so _bind_rows_reader() returned None
on every worker. With a None reader, the engine's get_sheet_rows() external-rows
branch is skipped and every instrument sheet falls through to the engine-only
path (fresh provider fetch). That path NEVER reads the existing sheet row, so a
page whose data is authored by the USER in the sheet -- My_Portfolio's manual
Position Qty / Avg Cost -- is served with those cells unread: position_cost /
position_value / unrealized_pl / unrealized_pl_pct (and the weight columns they
feed) come back blank on every holding, even though the engine's
_compute_position_math is correct and would multiply them out the instant it is
handed a row carrying qty + cost.

The market pages do not notice the dead reader: their symbols come from an env
universe (PAGE_SYMBOL_ENV_KEYS) and their data is fully provider-sourced, so the
engine-only path is exactly right for them. My_Portfolio is the one page that
needs the sheet's own cells preserved, so it is the one page that breaks.

WHAT THIS MODULE DOES
---------------------
Provides the RowsReader the binder already looks for. RowsReader.get_rows_for_page
reads the live sheet grid for the page via core.symbols_reader.GoogleSheetsReader
(read-only Sheets v4, UNFORMATTED_VALUE) and maps each data row onto the engine's
own canonical keys using get_sheet_spec(page) -- i.e. the SAME (headers, keys)
contract the engine projects with -- by normalized header-name match (so it is
robust to column reordering). The result is a list of canonical-keyed row dicts
the engine's external-rows merge then folds the fresh live quote into
(live-overwrite refreshes engine-owned fields; the manual fields in
_V577_MANUAL_FIELDS, including position_qty / avg_cost, are preserved), after
which _compute_position_math finally has its inputs.

SAFETY (no regression by construction)
--------------------------------------
- Env gate TFB_SHEET_ROWS_READER (default ON). Set 0/false/off and
  get_rows_for_page returns [] -> the engine treats the reader as empty ->
  engine-only path -> byte-identical to today.
- SCOPED to My_Portfolio. For every other page get_rows_for_page returns [], so
  the market / global / commodity / fund pages keep their exact env-seeded +
  fresh-fetch path. The blast radius is the single page that is broken.
- FAIL-OPEN. Any failure (no creds, API error, bad range, schema miss, mapping
  miss) returns [] -> engine-only path -> today's behavior. The reader can only
  ADD the manual cells when everything succeeds; it can never make a page worse.
- READ-ONLY. GoogleSheetsReader requests the spreadsheets.readonly scope; this
  module never writes.

The engine requires NO change: _bind_rows_reader discovers this module by name.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

__version__ = "1.0.0"

# The one page whose rows must be read from the sheet (manual, user-authored cells).
_PORTFOLIO_CANON = "myportfolio"

# Spreadsheet-id env keys (mirrors core.symbols_reader._default_spreadsheet_id).
_SPREADSHEET_ID_ENV_KEYS = (
    "DEFAULT_SPREADSHEET_ID",
    "SPREADSHEET_ID",
    "GOOGLE_SPREADSHEET_ID",
    "GOOGLE_SHEET_ID",
)

# Upper bound on the A1 row span we ever request (a portfolio never approaches this).
_MAX_ROW_SPAN = 10000


def _truthy(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    s = str(value).strip().lower()
    if s == "":
        return default
    return s not in ("0", "false", "off", "no", "n")


def _reader_enabled() -> bool:
    # Default ON; explicit 0/false/off disables -> engine-only path (today).
    return _truthy(os.getenv("TFB_SHEET_ROWS_READER"), default=True)


def _norm(text: Any) -> str:
    # Lowercase + drop every non-alphanumeric char. "Position Qty" -> "positionqty",
    # "Avg Cost" -> "avgcost", "Unrealized P/L %" -> "unrealizedpl". Used both to
    # detect the target page and to match sheet headers against engine headers.
    return re.sub(r"[^a-z0-9]+", "", str(text or "").strip().lower())


def _resolve_spreadsheet_id() -> str:
    for k in _SPREADSHEET_ID_ENV_KEYS:
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    # Last resort: ask symbols_reader (covers settings-sourced ids).
    try:
        from core.symbols_reader import _default_spreadsheet_id  # type: ignore
        sid = _default_spreadsheet_id(None)
        if sid:
            return str(sid).strip()
    except Exception:
        pass
    return ""


def _load_sheet_spec(page: str) -> Tuple[List[str], List[str]]:
    # Reuse the engine's authoritative resolver so the keys we emit are EXACTLY
    # the keys the engine projects with. Imported lazily: the engine imports THIS
    # module lazily (inside _bind_rows_reader at call time), so data_engine_v2 is
    # already fully loaded by the time we run -- there is no import cycle.
    from core.data_engine_v2 import get_sheet_spec  # type: ignore
    headers, keys = get_sheet_spec(page)
    return list(headers or []), list(keys or [])


def _new_grid_reader() -> Any:
    from core.symbols_reader import GoogleSheetsReader  # type: ignore
    return GoogleSheetsReader()


def _col_letter(col: int) -> str:
    letters = ""
    c = max(1, int(col))
    while c > 0:
        c -= 1
        letters = chr(65 + (c % 26)) + letters
        c //= 26
    return letters or "A"


def _quote_sheet(name: str) -> str:
    n = str(name or "").strip() or "Sheet1"
    if any(ch in n for ch in ("'", " ", "-", "!", ":", "/", "\\", ".", ",")):
        return "'" + n.replace("'", "''") + "'"
    return n


def _grid_to_rows(
    grid: List[List[Any]],
    headers: List[str],
    keys: List[str],
    limit: int,
    offset: int,
) -> List[Dict[str, Any]]:
    """Map a raw sheet grid (header row + data rows) onto canonical-keyed dicts.

    Column resolution is by normalized header-name match against the engine's
    (headers, keys) contract, so the mapping is exact regardless of the sheet's
    physical column order. Only columns whose header matches a canonical header
    are emitted -- never positional guesses -- and empty cells are left absent so
    the engine's own fill/overwrite logic owns them.
    """
    if not grid or len(grid) < 2 or not keys:
        return []

    # engine header (normalized) -> canonical key
    norm_to_key: Dict[str, str] = {}
    for i, h in enumerate(headers):
        if i >= len(keys):
            break
        nk = _norm(h)
        if nk and nk not in norm_to_key:
            norm_to_key[nk] = keys[i]

    # sheet column index -> canonical key (by normalized header match)
    sheet_header = grid[0] if isinstance(grid[0], (list, tuple)) else []
    col_to_key: Dict[int, str] = {}
    for j, sh in enumerate(sheet_header):
        key = norm_to_key.get(_norm(sh))
        if key:
            col_to_key[j] = key
    if not col_to_key:
        return []

    data = grid[1:]
    lo = max(0, int(offset))
    hi = lo + max(1, int(limit))
    out: List[Dict[str, Any]] = []
    for raw in data[lo:hi]:
        if not isinstance(raw, (list, tuple)):
            continue
        row: Dict[str, Any] = {}
        for j, key in col_to_key.items():
            if j < len(raw):
                val = raw[j]
                # Skip genuinely empty cells; an absent key lets the engine fill it.
                if val is None or (isinstance(val, str) and val.strip() == ""):
                    continue
                row[key] = val
        # Keep only rows that resolved a symbol (the engine's merge key).
        sym = row.get("symbol")
        if sym is not None and str(sym).strip() != "":
            out.append(row)
    return out


class RowsReader:
    """Canonical sheet rows reader discovered by DataEngineV5._bind_rows_reader."""

    def get_rows_for_page(self, page: str, limit: int = 2000, offset: int = 0) -> List[Dict[str, Any]]:
        # Gate OFF -> [] -> engine-only path (byte-identical to today).
        if not _reader_enabled():
            return []
        # SCOPE -> only the page whose cells are user-authored.
        if _norm(page) != _PORTFOLIO_CANON:
            return []
        try:
            sid = _resolve_spreadsheet_id()
            if not sid:
                logger.debug("[rows_reader v%s] no spreadsheet id resolved", __version__)
                return []
            headers, keys = _load_sheet_spec(page)
            if not keys:
                return []
            reader = _new_grid_reader()
            last_col = _col_letter(max(1, len(headers)))
            last_row = max(2, min(int(offset) + int(limit) + 1, _MAX_ROW_SPAN))
            rng = "%s!A1:%s%d" % (_quote_sheet(page), last_col, last_row)
            grid = reader.read_range(sid, rng)
            rows = _grid_to_rows(grid, headers, keys, limit=limit, offset=offset)
            logger.info(
                "[rows_reader v%s] %s -> %d sheet row(s) mapped (grid=%d, cols=%d)",
                __version__, page, len(rows), (len(grid) if grid else 0), len(headers),
            )
            return rows
        except Exception as exc:  # fail-open
            logger.warning(
                "[rows_reader v%s] read failed for %s: %s: %s -> engine-only path",
                __version__, page, exc.__class__.__name__, exc,
            )
            return []

    # Alias method names the binder may probe (defensive; get_rows_for_page is primary).
    def get_rows(self, page: str, limit: int = 2000, offset: int = 0) -> List[Dict[str, Any]]:
        return self.get_rows_for_page(page, limit=limit, offset=offset)

    def read_rows(self, page: str, limit: int = 2000, offset: int = 0) -> List[Dict[str, Any]]:
        return self.get_rows_for_page(page, limit=limit, offset=offset)


# Alias class name the binder also accepts (getattr(mod, "RowsReader") or "Reader").
Reader = RowsReader
