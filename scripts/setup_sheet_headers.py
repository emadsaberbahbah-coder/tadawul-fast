#!/usr/bin/env python3
# scripts/setup_sheet_headers.py
"""
setup_sheet_headers.py
===========================================================
TADAWUL FAST BRIDGE – SHEET INITIALIZER (v1.3.0)
===========================================================

Purpose
- Writes canonical headers to Google Sheet tabs (Row 5 by default).
- Keeps tabs aligned with backend canonical schemas (routes/ai_analysis.py + core/data_engine_v2.py).
- Prevents "missing column" errors / misalignment.

Key upgrades (v1.3.0)
- ✅ Attempts to load per-tab canonical headers from `core.schemas.get_headers_for_sheet(tab)`
    (if available). Falls back to built-in headers if not.
- ✅ Uses your agreed Forecast/Expected columns:
    Forecast Price (1M), Expected ROI % (1M),
    Forecast Price (3M), Expected ROI % (3M),
    Forecast Price (12M), Expected ROI % (12M),
    Forecast Confidence, Forecast Updated (UTC)
  (+ Forecast Updated (Riyadh) included as extra safety)
- ✅ Better "row not empty" detection across the full header width
- ✅ --dry-run, --list, and safer defaults for dashboard tabs

Usage
  python scripts/setup_sheet_headers.py
  python scripts/setup_sheet_headers.py --sheet-id <ID> --tabs Market_Leaders KSA_Tadawul
  python scripts/setup_sheet_headers.py --force
  python scripts/setup_sheet_headers.py --list --tabs KSA_Tadawul

Defaults
- Target Row: 5 (A5:...) to match your dashboard header row.
- Default Tabs: Market_Leaders, KSA_Tadawul, Global_Markets, Mutual_Funds, Commodities_FX, My_Portfolio, Market_Scan
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import List, Optional

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from integrations import google_sheets_service as sheets
    from env import settings  # type: ignore
except ImportError:
    print("❌ Critical: Could not import project modules. Run from project root.")
    sys.exit(1)

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("SheetSetup")


# ============================================================
# Header Sets (Fallbacks)
# ============================================================

# Standard “data table” headers aligned to your dashboard style.
# (Rank + Origin included to match your Sheets & outputs.)
STANDARD_HEADERS = [
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

# Extended analytics + forecasting (kept AFTER standard headers)
EXTENDED_ANALYTICS_HEADERS = [
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
    # Extra safety for your Riyadh view
    "Forecast Updated (Riyadh)",
    "Forecast Method",
    "History Points",
    "History Source",
    "History Last (UTC)",
]

CANONICAL_HEADERS = STANDARD_HEADERS + EXTENDED_ANALYTICS_HEADERS

# Smaller subset for "Market_Scan" summary tab
SCAN_HEADERS = [
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

# Insights_Analysis is usually a composed layout, not a flat table;
# keep a conservative header row if user insists on initializing it.
INSIGHTS_MIN_HEADERS = [
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
    if cli_arg:
        return cli_arg.strip()

    # Prefer settings if available
    sid = ""
    try:
        sid = str(getattr(settings, "DEFAULT_SPREADSHEET_ID", "") or "").strip()
    except Exception:
        sid = ""

    if sid:
        return sid

    return (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()


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


def _row_has_any_values(sid: str, tab: str, row: int, width: int) -> bool:
    """
    Checks if the row has any values across A..(width).
    Returns True if non-empty, False if empty or unreadable.
    """
    last_col = _col_to_a1(max(1, width))
    rng = f"'{tab}'!A{row}:{last_col}{row}"
    try:
        existing = sheets.read_range(sid, rng)
        if not existing or not existing[0]:
            return False
        # Any non-empty cell?
        for v in existing[0]:
            if v is None:
                continue
            if str(v).strip() != "":
                return True
        return False
    except Exception:
        # If the tab doesn't exist, read_range may fail; treat as empty here.
        return False


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

    args = p.parse_args()

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("❌ No Spreadsheet ID found. Set DEFAULT_SPREADSHEET_ID or use --sheet-id.")
        return 1

    # Initialize Service
    try:
        svc = sheets.get_sheets_service()
        if not svc:
            logger.error("❌ Failed to initialize Sheets service (check credentials).")
            return 1
    except Exception as e:
        logger.error(f"❌ Service Init Failed: {e}")
        return 1

    targets = args.tabs
    if not targets:
        targets = [
            "Market_Leaders",
            "KSA_Tadawul",
            "Global_Markets",
            "Mutual_Funds",
            "Commodities_FX",
            "My_Portfolio",
            "Market_Scan",
        ]
        logger.info(f"No tabs specified, defaulting to: {targets}")

    logger.info(f"Target Spreadsheet: {sid}")
    logger.info(f"Header Row: {args.row}")
    logger.info(f"Schema headers: {'OFF' if args.no_schema else 'ON'}")
    if args.dry_run:
        logger.info("DRY RUN: no writes will be performed.")
    if args.list:
        logger.info("LIST MODE: no writes will be performed.")

    ok_count = 0
    skip_count = 0
    fail_count = 0

    for tab in targets:
        headers = _pick_headers_for_tab(tab, use_schema=(not args.no_schema))
        width = len(headers)

        if args.list:
            print(f"\n[{tab}] ({width} headers)")
            for i, h in enumerate(headers, 1):
                print(f"{i:>3}. {h}")
            continue

        # Skip if not forced and row already has values
        if not args.force:
            if _row_has_any_values(sid, tab, args.row, width):
                logger.warning(f"⚠️  Skipping '{tab}': Row {args.row} is not empty. Use --force to overwrite.")
                skip_count += 1
                continue

        range_start = f"'{tab}'!A{args.row}"

        if args.dry_run:
            logger.info(f"[DRY] Would write {width} headers to {range_start}")
            ok_count += 1
            continue

        try:
            logger.info(f"👉 Writing {width} headers to '{tab}' @ row {args.row}...")
            updated = sheets.write_range(sid, range_start, [headers])

            if updated and updated > 0:
                logger.info(f"✅ Success: '{tab}' updated.")
                ok_count += 1
            else:
                logger.warning(f"⚠️  Write returned 0 updates for '{tab}'. Check permissions/tab existence.")
                fail_count += 1

        except Exception as e:
            logger.error(f"❌ Failed processing '{tab}': {e}")
            if "Unable to parse range" in str(e):
                logger.error("   (Hint: Does the tab exist in the spreadsheet?)")
            fail_count += 1

    if not args.list:
        logger.info("\n✨ Header setup complete.")
        logger.info(f"✅ OK: {ok_count} | ⚠️ Skipped: {skip_count} | ❌ Failed: {fail_count}")

    return 0 if fail_count == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
