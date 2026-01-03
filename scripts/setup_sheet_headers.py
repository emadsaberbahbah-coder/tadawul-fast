#!/usr/bin/env python3
# scripts/setup_sheet_headers.py
"""
setup_sheet_headers.py
===========================================================
TADAWUL FAST BRIDGE – SHEET INITIALIZER (v1.0.0)
===========================================================

Purpose
- automatically writes the correct canonical headers to your Google Sheet tabs.
- Ensures columns match what `run_dashboard_sync.py` and `routes/ai_analysis.py` expect.
- Helps avoid "missing column" errors or data misalignment.

Usage
  python scripts/setup_sheet_headers.py
  python scripts/setup_sheet_headers.py --sheet-id <ID> --tabs Market_Scan KSA_Tadawul

Defaults
- Target Row: 5 (A5:...) to match default start_cell.
- Tabs: "Market_Scan", "KSA_Tadawul", "Global_Markets" if not specified.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from integrations import google_sheets_service as sheets
    from env import settings
except ImportError:
    print("❌ Critical: Could not import project modules. Run from project root.")
    sys.exit(1)

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("SheetSetup")

# The canonical list of headers (Standard 59 + Extras)
# Must align with routes/ai_analysis.py _DEFAULT_HEADERS_EXTENDED
CANONICAL_HEADERS = [
    "Symbol", "Company Name", "Sector", "Sub-Sector", "Market", "Currency", "Listing Date",
    "Last Price", "Previous Close", "Price Change", "Percent Change",
    "Day High", "Day Low", "52W High", "52W Low", "52W Position %",
    "Volume", "Avg Volume (30D)", "Value Traded", "Turnover %",
    "Shares Outstanding", "Free Float %", "Market Cap", "Free Float Market Cap",
    "Liquidity Score", "EPS (TTM)", "Forward EPS", "P/E (TTM)", "Forward P/E",
    "P/B", "P/S", "EV/EBITDA", "Dividend Yield %", "Dividend Rate", "Payout Ratio %",
    "ROE %", "ROA %", "Net Margin %", "EBITDA Margin %", "Revenue Growth %", "Net Income Growth %",
    "Beta", "Volatility (30D)", "RSI (14)",
    "Fair Value", "Upside %", "Valuation Label",
    "Value Score", "Quality Score", "Momentum Score", "Opportunity Score", "Risk Score", "Overall Score",
    "Error", "Recommendation", "Data Source", "Data Quality",
    "Last Updated (UTC)", "Last Updated (Riyadh)",
    # Extended columns often used in dashboards
    "Returns 1W %", "Returns 1M %", "Returns 3M %", "Returns 6M %", "Returns 12M %",
    "MA20", "MA50", "MA200",
    "Expected Return 1M %", "Expected Return 3M %", "Expected Return 12M %",
    "Expected Price 1M", "Expected Price 3M", "Expected Price 12M",
    "Confidence Score", "Forecast Method",
    "History Points", "History Source", "History Last (UTC)"
]

# Smaller subset for "Market_Scan" summary tab
SCAN_HEADERS = [
    "Rank", "Symbol", "Origin Pages", "Name", "Market", "Currency", "Price", "Change %",
    "Market Cap", "P/E (TTM)", "P/B", "Dividend Yield %", "ROE %", "ROA %",
    "Value Score", "Quality Score", "Momentum Score", "Risk Score", "Opportunity Score", "Overall Score", "Recommendation",
    "Fair Value", "Upside %", "Valuation Label",
    "Data Quality", "Sources", "Last Updated (UTC)", "Error"
]


def _get_spreadsheet_id(cli_arg: str | None) -> str:
    if cli_arg:
        return cli_arg
    return (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()


def main() -> int:
    p = argparse.ArgumentParser(description="Initialize Google Sheet Headers")
    p.add_argument("--sheet-id", help="Target Spreadsheet ID")
    p.add_argument("--tabs", nargs="*", help="Specific tabs to update (default: standard set)")
    p.add_argument("--row", type=int, default=5, help="Header row number (default: 5)")
    p.add_argument("--force", action="store_true", help="Write headers even if sheet not empty (overwrites row)")
    
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

    # Default tabs if none provided
    targets = args.tabs
    if not targets:
        targets = ["Market_Scan", "KSA_Tadawul", "Global_Markets"]
        logger.info(f"No tabs specified, defaulting to: {targets}")

    logger.info(f"Target Spreadsheet: {sid}")
    logger.info(f"Writing headers to Row {args.row}...")

    for tab in targets:
        is_scan = "scan" in tab.lower() or "advisor" in tab.lower()
        headers = SCAN_HEADERS if is_scan else CANONICAL_HEADERS
        
        range_name = f"'{tab}'!A{args.row}"
        
        try:
            # 1. Check if tab exists/has data (unless forced)
            if not args.force:
                existing = sheets.read_range(sid, f"'{tab}'!A{args.row}:C{args.row}")
                if existing and existing[0]:
                    logger.warning(f"⚠️  Skipping '{tab}': Row {args.row} is not empty. Use --force to overwrite.")
                    continue

            # 2. Write Headers
            logger.info(f"👉 Writing {len(headers)} headers to '{tab}'...")
            updated = sheets.write_range(sid, range_name, [headers])
            
            if updated > 0:
                logger.info(f"✅ Success: '{tab}' updated.")
            else:
                logger.warning(f"⚠️  Write returned 0 updates for '{tab}'. Check permissions/tab existence.")
                
        except Exception as e:
            logger.error(f"❌ Failed processing '{tab}': {e}")
            if "Unable to parse range" in str(e):
                logger.error("   (Hint: Does the tab exist in the spreadsheet?)")

    logger.info("\n✨ Header setup complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
