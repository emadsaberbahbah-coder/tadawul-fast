#!/usr/bin/env python3
# scripts/setup_sheet_headers.py  (FULL REPLACEMENT)
"""
setup_sheet_headers.py
===========================================================
TADAWUL FAST BRIDGE – SHEET INITIALIZER (v1.4.0)
===========================================================

Purpose
- Writes canonical headers to Google Sheet tabs (Row 5 by default).
- Keeps tabs aligned with backend canonical schemas (core.schemas + routes/ai_analysis.py).
- Prevents "missing column" errors / misalignment.

Key upgrades (v1.4.0)
- ✅ Import path fixed: uses top-level google_sheets_service (not integrations.*) to match repo reality.
- ✅ Robust spreadsheet-id resolution:
    - settings.default_spreadsheet_id
    - env vars DEFAULT_SPREADSHEET_ID, TFB_SPREADSHEET_ID, SPREADSHEET_ID, GOOGLE_SHEETS_ID
- ✅ Uses per-tab headers FIRST when core.schemas.get_headers_for_sheet(tab) is available.
- ✅ Adds Forecast/Expected columns (8) + Forecast Updated (Riyadh) safety.
- ✅ Better "row not empty" detection:
    - checks existing values across full header width
    - and checks any value beyond width (A..ZZ) to avoid false overwrite risk
- ✅ Tab alias normalization (KSA/Global/Leaders/etc) and sheet-name mapping via symbols_reader.PAGE_REGISTRY if present.
- ✅ Write API compatibility: supports write_range / update_values / write_grid_chunked
- ✅ Optional creation of missing tabs (if sheets_service exposes ensure_tab / ensure_sheet)

Usage
  python scripts/setup_sheet_headers.py
  python scripts/setup_sheet_headers.py --sheet-id <ID> --tabs Market_Leaders KSA_Tadawul
  python scripts/setup_sheet_headers.py --force
  python scripts/setup_sheet_headers.py --list --tabs KSA_Tadawul
  python scripts/setup_sheet_headers.py --create-missing-tabs

Defaults
- Target Row: 5 (A5:...) to match your dashboard header row.
- Default Tabs: Market_Leaders, KSA_Tadawul, Global_Markets, Mutual_Funds, Commodities_FX, My_Portfolio, Market_Scan
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

SCRIPT_VERSION = "1.4.0"

# -----------------------------------------------------------------------------
# Path safety: allow running from project root or scripts/
# -----------------------------------------------------------------------------
def _ensure_project_root_on_path() -> None:
    try:
        here = os.path.dirname(os.path.abspath(__file__))  # .../scripts
        parent = os.path.dirname(here)  # project root
        for p in (here, parent):
            if p and p not in sys.path:
                sys.path.insert(0, p)
    except Exception:
        pass


_ensure_project_root_on_path()

# -----------------------------------------------------------------------------
# Imports (project)
# -----------------------------------------------------------------------------
try:
    from env import settings  # type: ignore
except Exception:
    settings = None  # type: ignore

try:
    import google_sheets_service as sheets  # type: ignore
except Exception as e:
    print(f"❌ Critical: Could not import google_sheets_service. err={e}")
    print("Tip: run from project root where google_sheets_service.py exists.")
    raise SystemExit(1)

# symbols_reader is optional (used for PAGE_REGISTRY -> sheet_name mapping)
try:
    import symbols_reader  # type: ignore
except Exception:
    symbols_reader = None  # type: ignore

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("SheetSetup")

_A1_CELL_RE = re.compile(r"^\$?[A-Za-z]+\$?\d+$")


# ============================================================
# Header Sets (Fallbacks)
# ============================================================

STANDARD_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Sector",
    "Sub Sector",
    "Market",
    "Currency",
    "Listing Date",
    "Price",
    "Prev Close",
    "Change",
    "Change %",
    "Day High",
    "Day Low",
    "52W High",
    "52W Low",
    "52W Position %",
    "Volume",
    "Avg Vol 30D",
    "Value Traded",
    "Turnover %",
    "Shares Outstanding",
    "Free Float %",
    "Market Cap",
    "Free Float Mkt Cap",
    "Liquidity Score",
    "EPS (TTM)",
    "Forward EPS",
    "P/E (TTM)",
    "Forward P/E",
    "P/B",
    "P/S",
    "EV/EBITDA",
    "Dividend Yield",
    "Dividend Rate",
    "Payout Ratio",
    "ROE",
    "ROA",
    "Net Margin",
    "EBITDA Margin",
    "Revenue Growth",
    "Net Income Growth",
    "Beta",
    "Volatility 30D",
    "RSI 14",
    "Fair Value",
    "Upside %",
    "Valuation Label",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Opportunity Score",
    "Risk Score",
    "Overall Score",
    "Rec Badge",
    "Momentum Badge",
    "Opportunity Badge",
    "Risk Badge",
    "Error",
    "Recommendation",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]

EXTENDED_ANALYTICS_HEADERS: List[str] = [
    "Returns 1W %",
    "Returns 1M %",
    "Returns 3M %",
    "Returns 6M %",
    "Returns 12M %",
    "MA20",
    "MA50",
    "MA200",
    # Agreed Forecast / Expected (8)
    "Forecast Price (1M)",
    "Expected ROI % (1M)",
    "Forecast Price (3M)",
    "Expected ROI % (3M)",
    "Forecast Price (12M)",
    "Expected ROI % (12M)",
    "Forecast Confidence",
    "Forecast Updated (UTC)",
    # Extra safety for Riyadh view
    "Forecast Updated (Riyadh)",
    "Forecast Method",
    "History Points",
    "History Source",
    "History Last (UTC)",
]

CANONICAL_HEADERS: List[str] = STANDARD_HEADERS + EXTENDED_ANALYTICS_HEADERS

SCAN_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin Pages",
    "Name",
    "Market",
    "Currency",
    "Price",
    "Change %",
    "Market Cap",
    "P/E (TTM)",
    "P/B",
    "Dividend Yield",
    "ROE",
    "ROA",
    "Value Score",
    "Quality Score",
    "Momentum Score",
    "Risk Score",
    "Opportunity Score",
    "Overall Score",
    "Recommendation",
    "Rec Badge",
    "Fair Value",
    "Upside %",
    "Valuation Label",
    "Data Quality",
    "Sources",
    "Last Updated (UTC)",
    "Error",
]

INSIGHTS_MIN_HEADERS: List[str] = [
    "Section",
    "Key",
    "Value",
    "As Of (UTC)",
    "As Of (Riyadh)",
]


# ============================================================
# Helpers
# ============================================================

def _get_spreadsheet_id(cli_arg: Optional[str]) -> str:
    if cli_arg and str(cli_arg).strip():
        return str(cli_arg).strip()

    # settings.default_spreadsheet_id (preferred)
    try:
        if settings is not None:
            sid = (getattr(settings, "default_spreadsheet_id", None) or "").strip()
            if sid:
                return sid
    except Exception:
        pass

    # env fallbacks
    for k in ("DEFAULT_SPREADSHEET_ID", "TFB_SPREADSHEET_ID", "SPREADSHEET_ID", "GOOGLE_SHEETS_ID"):
        v = (os.getenv(k, "") or "").strip()
        if v:
            return v
    return ""


def _canon_key(user_key: str) -> str:
    k = (user_key or "").strip().upper()
    k = k.replace("-", "_").replace(" ", "_")
    aliases = {
        "KSA": "KSA_TADAWUL",
        "TADAWUL": "KSA_TADAWUL",
        "KSA_TASI": "KSA_TADAWUL",
        "GLOBAL": "GLOBAL_MARKETS",
        "GLOBAL_SHARES": "GLOBAL_MARKETS",
        "GLOBAL_MARKET": "GLOBAL_MARKETS",
        "FUNDS": "MUTUAL_FUNDS",
        "MUTUALFUND": "MUTUAL_FUNDS",
        "MUTUAL_FUND": "MUTUAL_FUNDS",
        "FX": "COMMODITIES_FX",
        "COMMODITIES": "COMMODITIES_FX",
        "PORTFOLIO": "MY_PORTFOLIO",
        "MYPORTFOLIO": "MY_PORTFOLIO",
        "LEADERS": "MARKET_LEADERS",
        "MARKETLEADERS": "MARKET_LEADERS",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
        "INSIGHTS_AI": "INSIGHTS_ANALYSIS",
        "AI": "INSIGHTS_ANALYSIS",
        "SCAN": "MARKET_SCAN",
    }
    return aliases.get(k, k)


def _parse_tabs_list(tabs: Optional[Sequence[str]]) -> List[str]:
    if not tabs:
        return []
    out: List[str] = []
    for x in tabs:
        if x is None:
            continue
        s = str(x).strip()
        if not s:
            continue
        out.extend([p.strip() for p in s.split(",") if p.strip()])
    # keep order, dedupe
    seen = set()
    final: List[str] = []
    for t in out:
        if t not in seen:
            seen.add(t)
            final.append(t)
    return final


def _col_to_a1(col_num_1based: int) -> str:
    """1 -> A, 26 -> Z, 27 -> AA"""
    n = int(col_num_1based)
    if n <= 0:
        return "A"
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _safe_sheet_name(sheet_name: str) -> str:
    name = (sheet_name or "").strip().replace("'", "''")
    return f"'{name}'"


def _resolve_tab_name(tab_or_key: str) -> str:
    """
    Accepts either a sheet tab name ("Market_Leaders") or a PAGE_REGISTRY key ("MARKET_LEADERS").
    Tries:
      1) If looks like exact sheet name -> keep
      2) symbols_reader.PAGE_REGISTRY[key].sheet_name if available
      3) common mapping (KSA_TADAWUL -> KSA_Tadawul, etc)
      4) fallback to original
    """
    t_in = (tab_or_key or "").strip()
    if not t_in:
        return ""

    key = _canon_key(t_in)

    # If user passed something with lowercase/underscore that is already a sheet name, keep it
    # (We still try registry in case they provided KEY format.)
    if symbols_reader is not None:
        try:
            reg = getattr(symbols_reader, "PAGE_REGISTRY", None)
            if isinstance(reg, dict):
                cfg = reg.get(key) or reg.get(key.lower()) or reg.get(key.title())
                if cfg is not None:
                    if isinstance(cfg, dict):
                        nm = (cfg.get("sheet_name") or cfg.get("tab") or cfg.get("name") or "").strip()
                        if nm:
                            return nm
                    nm = (getattr(cfg, "sheet_name", None) or getattr(cfg, "tab_name", None) or "").strip()
                    if nm:
                        return nm
        except Exception:
            pass

    fallback_map = {
        "KSA_TADAWUL": "KSA_Tadawul",
        "GLOBAL_MARKETS": "Global_Markets",
        "MUTUAL_FUNDS": "Mutual_Funds",
        "COMMODITIES_FX": "Commodities_FX",
        "MY_PORTFOLIO": "My_Portfolio",
        "MARKET_LEADERS": "Market_Leaders",
        "INSIGHTS_ANALYSIS": "Insights_Analysis",
        "MARKET_SCAN": "Market_Scan",
    }
    if key in fallback_map:
        return fallback_map[key]

    return t_in


def _read_range(sid: str, a1_range: str) -> List[List[Any]]:
    fn = getattr(sheets, "read_range", None)
    if callable(fn):
        return fn(sid, a1_range)  # type: ignore
    fn2 = getattr(sheets, "get_values", None)
    if callable(fn2):
        return fn2(sid, a1_range)  # type: ignore
    raise RuntimeError("google_sheets_service has no read_range/get_values function.")


def _write_values(sid: str, a1_start: str, values: List[List[Any]], *, value_input: str = "RAW") -> int:
    """
    Best-effort write. Tries:
      - write_range(sid, a1_start, values)
      - update_values(sid, a1_range, values, value_input=?)
      - write_grid_chunked(sid, sheet_name, start_cell, grid, value_input=?)
    Returns updated cells (best-effort int).
    """
    vi = (value_input or "RAW").strip().upper()
    if vi not in ("RAW", "USER_ENTERED"):
        vi = "RAW"

    fn = getattr(sheets, "write_range", None)
    if callable(fn):
        updated = fn(sid, a1_start, values)  # type: ignore
        try:
            return int(updated or 0)
        except Exception:
            return 0

    fn2 = getattr(sheets, "update_values", None)
    if callable(fn2):
        updated = fn2(sid, a1_start, values, value_input=vi)  # type: ignore
        try:
            return int(updated or 0)
        except Exception:
            return 0

    # If user has chunked grid writer, parse a1_start into tab + cell
    fn3 = getattr(sheets, "write_grid_chunked", None)
    if callable(fn3):
        # a1_start format: "'Tab'!A5" or "Tab!A5" or just "A5"
        if "!" in a1_start:
            tab_part, cell = a1_start.split("!", 1)
            tab_name = tab_part.strip().strip("'")
            start_cell = cell.strip()
        else:
            # cannot infer tab, fail
            raise RuntimeError("write_grid_chunked requires explicit 'Tab'!A5 style a1_start.")
        grid = values
        try:
            updated = fn3(sid, tab_name, start_cell, grid, value_input=vi)  # type: ignore
        except TypeError:
            updated = fn3(sid, tab_name, start_cell, grid)  # type: ignore
        try:
            return int(updated or 0)
        except Exception:
            return 0

    raise RuntimeError("No supported write function found (write_range/update_values/write_grid_chunked).")


def _ensure_tab_exists(sid: str, tab: str) -> bool:
    """
    Optional: create missing tab if sheets_service supports it.
    Returns True if ensured/exists, False if not supported/failed.
    """
    for name in ("ensure_tab", "ensure_sheet", "create_tab_if_missing", "ensure_worksheet"):
        fn = getattr(sheets, name, None)
        if callable(fn):
            try:
                fn(sid, tab)  # type: ignore
                return True
            except Exception:
                return False
    return False


def _row_has_any_values(sid: str, tab: str, row: int, width: int) -> bool:
    """
    Checks if the row has any values:
      - across A..(width)
      - AND also across A..ZZ (safety: user may have more columns than our header list)
    Returns True if non-empty, False if empty or unreadable.
    """
    def _has_values(a1: str) -> bool:
        try:
            existing = _read_range(sid, a1)
            if not existing or not existing[0]:
                return False
            for v in existing[0]:
                if v is None:
                    continue
                if str(v).strip() != "":
                    return True
            return False
        except Exception:
            return False

    last_col = _col_to_a1(max(1, width))
    rng1 = f"{_safe_sheet_name(tab)}!A{row}:{last_col}{row}"
    if _has_values(rng1):
        return True

    # extra safety check across A..ZZ
    rng2 = f"{_safe_sheet_name(tab)}!A{row}:ZZ{row}"
    return _has_values(rng2)


def _try_headers_from_core_schemas(tab: str) -> Optional[List[str]]:
    """
    Best-effort: import core.schemas.get_headers_for_sheet(tab).
    Returns list[str] if available.
    """
    try:
        mod = __import__("core.schemas", fromlist=["get_headers_for_sheet"])
        fn = getattr(mod, "get_headers_for_sheet", None)
        if callable(fn):
            res = fn(tab)
            if isinstance(res, list) and all(isinstance(x, str) for x in res) and len(res) >= 10:
                return res
    except Exception:
        return None
    return None


def _pick_headers_for_tab(tab: str, *, use_schema: bool = True) -> List[str]:
    tl = (tab or "").strip().lower()

    if use_schema:
        h = _try_headers_from_core_schemas(tab)
        if h:
            return h

    if "insights" in tl:
        return INSIGHTS_MIN_HEADERS

    if "scan" in tl or "advisor" in tl:
        return SCAN_HEADERS

    return CANONICAL_HEADERS


# ============================================================
# Main
# ============================================================
def main() -> int:
    p = argparse.ArgumentParser(description="Initialize Google Sheet Headers (TFB)")
    p.add_argument("--sheet-id", help="Target Spreadsheet ID")
    p.add_argument("--tabs", nargs="*", help="Specific tabs to update (default: dashboard set)")
    p.add_argument("--row", type=int, default=5, help="Header row number (default: 5)")
    p.add_argument("--force", action="store_true", help="Overwrite header row even if not empty")
    p.add_argument("--dry-run", action="store_true", help="Print actions but do not write anything")
    p.add_argument("--list", action="store_true", help="Print headers for each tab (no write)")
    p.add_argument("--no-schema", action="store_true", help="Do NOT attempt core.schemas headers (use fallbacks only)")
    p.add_argument("--value-input", default="RAW", choices=["RAW", "USER_ENTERED", "raw", "user_entered"], help="Sheets write mode")
    p.add_argument("--create-missing-tabs", action="store_true", help="Attempt to create missing tabs (if supported)")

    args = p.parse_args()

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("❌ No Spreadsheet ID found. Set DEFAULT_SPREADSHEET_ID or use --sheet-id.")
        return 1

    # Initialize Service (best-effort). Some repos expose get_sheets_service, others init lazily.
    try:
        svc = getattr(sheets, "get_sheets_service", None)
        if callable(svc):
            _ = svc()  # type: ignore
    except Exception as e:
        logger.error("❌ Sheets service init failed: %s", e)
        return 1

    tabs_in = _parse_tabs_list(args.tabs)
    if not tabs_in:
        tabs_in = [
            "Market_Leaders",
            "KSA_Tadawul",
            "Global_Markets",
            "Mutual_Funds",
            "Commodities_FX",
            "My_Portfolio",
            "Market_Scan",
        ]
        logger.info("No tabs specified, defaulting to: %s", tabs_in)

    # Resolve keys -> actual sheet tab names
    targets: List[str] = []
    for t in tabs_in:
        nm = _resolve_tab_name(t)
        if nm:
            targets.append(nm)

    logger.info("TFB Sheet Setup v%s", SCRIPT_VERSION)
    logger.info("Target Spreadsheet: %s", sid)
    logger.info("Header Row: %s", args.row)
    logger.info("Schema headers: %s", "OFF" if args.no_schema else "ON")
    logger.info("Value input: %s", str(args.value_input).strip().upper())
    if args.dry_run:
        logger.info("DRY RUN: no writes will be performed.")
    if args.list:
        logger.info("LIST MODE: no writes will be performed.")

    ok_count = 0
    skip_count = 0
    fail_count = 0

    for tab in targets:
        # Optionally ensure tab exists
        if args.create_missing_tabs:
            ensured = _ensure_tab_exists(sid, tab)
            if ensured:
                logger.info("Ensured tab exists: %s", tab)

        headers = _pick_headers_for_tab(tab, use_schema=(not args.no_schema))
        width = len(headers)

        if args.list:
            print(f"\n[{tab}] ({width} headers)")
            for i, h in enumerate(headers, 1):
                print(f"{i:>3}. {h}")
            continue

        # Skip if not forced and row already has values
        if not args.force and _row_has_any_values(sid, tab, args.row, width):
            logger.warning("⚠️  Skipping '%s': Row %s is not empty. Use --force to overwrite.", tab, args.row)
            skip_count += 1
            continue

        start_cell = f"A{int(args.row)}"
        a1_start = f"{_safe_sheet_name(tab)}!{start_cell}"

        if args.dry_run:
            logger.info("[DRY] Would write %s headers to %s", width, a1_start)
            ok_count += 1
            continue

        try:
            logger.info("👉 Writing %s headers to %s ...", width, a1_start)
            updated = _write_values(
                sid,
                a1_start,
                [headers],
                value_input=str(args.value_input or "RAW").strip().upper(),
            )
            if int(updated or 0) > 0:
                logger.info("✅ Success: '%s' updated. cells=%s", tab, int(updated or 0))
                ok_count += 1
            else:
                logger.warning("⚠️  Write returned 0 updates for '%s'. Check permissions/tab existence.", tab)
                fail_count += 1
        except Exception as e:
            logger.error("❌ Failed processing '%s': %s", tab, e)
            if "Unable to parse range" in str(e):
                logger.error("   (Hint: Does the tab exist in the spreadsheet? Use --create-missing-tabs)")
            fail_count += 1

    if not args.list:
        logger.info("✨ Header setup complete.")
        logger.info("✅ OK: %s | ⚠️ Skipped: %s | ❌ Failed: %s", ok_count, skip_count, fail_count)

    return 0 if fail_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
