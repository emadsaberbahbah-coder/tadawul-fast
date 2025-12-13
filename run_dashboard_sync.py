"""
run_dashboard_sync.py
===========================================================
TADAWUL FAST BRIDGE – DASHBOARD SYNCHRONIZER (v2.0)
===========================================================

What this script does:
1. Connects to your Google Spreadsheet (DEFAULT_SPREADSHEET_ID).
2. Reads symbols from specific tabs (KSA, Global, Portfolio, etc.) using `symbols_reader`.
3. Fetches fresh data from the V2 Data Engine.
4. Writes updated rows back to the sheets using `google_sheets_service`.

Prerequisites:
- `env.py` configured with credentials and spreadsheet ID.
- `pip install -r requirements.txt`

Usage:
    python run_dashboard_sync.py          # Sync all configured pages
    python run_dashboard_sync.py --ksa    # Sync only KSA market
    python run_dashboard_sync.py --global # Sync only Global market
"""

from __future__ import annotations

import argparse
import logging
import time
import sys
from datetime import datetime

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("DashboardSync")

# --- IMPORTS ---
try:
    from env import settings
    import symbols_reader
    import google_sheets_service as sheets_service
except ImportError as e:
    logger.error(f"Import failed: {e}. Run from project root.")
    sys.exit(1)

# --- CONFIGURATION ---
# Map Logical Keys (symbols_reader) to Update Logic
SYNC_MAP = [
    {
        "key": "KSA_TADAWUL",
        "method": sheets_service.refresh_sheet_with_enriched_quotes,
        "desc": "KSA Tadawul Market"
    },
    {
        "key": "GLOBAL_MARKETS",
        "method": sheets_service.refresh_sheet_with_enriched_quotes,
        "desc": "Global Markets"
    },
    {
        "key": "MUTUAL_FUNDS",
        "method": sheets_service.refresh_sheet_with_enriched_quotes,
        "desc": "Mutual Funds"
    },
    {
        "key": "COMMODITIES_FX",
        "method": sheets_service.refresh_sheet_with_enriched_quotes,
        "desc": "Commodities & FX"
    },
    {
        "key": "MY_PORTFOLIO",
        "method": sheets_service.refresh_sheet_with_enriched_quotes,
        "desc": "My Portfolio"
    },
    {
        "key": "MARKET_LEADERS",
        "method": sheets_service.refresh_sheet_with_enriched_quotes,
        "desc": "Market Leaders"
    },
    {
        "key": "INSIGHTS_ANALYSIS",
        "method": sheets_service.refresh_sheet_with_ai_analysis, # Uses AI Analysis endpoint
        "desc": "Insights & AI Analysis"
    }
]

def sync_page(page_conf: dict, dry_run: bool = False):
    key = page_conf["key"]
    desc = page_conf["desc"]
    update_func = page_conf["method"]
    
    logger.info(f"--- Syncing: {desc} ({key}) ---")
    
    # 1. Read Symbols
    try:
        data = symbols_reader.get_page_symbols(key)
        all_syms = data.get("all", [])
        
        if not all_syms:
            logger.warning(f"No symbols found in sheet for {key}. Skipping.")
            return
            
        logger.info(f"Found {len(all_syms)} symbols (KSA: {len(data['ksa'])}, Global: {len(data['global'])})")
        
        if dry_run:
            logger.info("[DRY RUN] Would update sheet with these symbols.")
            return

        # 2. Perform Update
        # We need the sheet name to tell the service where to write
        # symbols_reader has the config registry which holds the sheet name
        page_config = symbols_reader.PAGE_REGISTRY.get(key)
        if not page_config:
            logger.error(f"Registry config missing for {key}")
            return

        start_t = time.time()
        
        # Call the google_sheets_service function
        result = update_func(
            sid=settings.default_spreadsheet_id,
            sheet_name=page_config.sheet_name,
            tickers=all_syms,
            clear=False # Usually safer to overwrite than clear to preserve formatting
        )
        
        duration = time.time() - start_t
        
        if result.get("status") == "success":
            logger.info(f"✅ Success: Updated {result.get('rows_written')} rows in {duration:.2f}s")
        else:
            logger.error(f"❌ Failed: {result}")

    except Exception as e:
        logger.exception(f"Error syncing {key}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Synchronize Dashboard Sheets")
    parser.add_argument("--dry-run", action="store_true", help="Read symbols but do not write data")
    parser.add_argument("--ksa", action="store_true", help="Sync only KSA Tadawul")
    parser.add_argument("--global", action="store_true", help="Sync only Global Markets")
    parser.add_argument("--portfolio", action="store_true", help="Sync only Portfolio")
    parser.add_argument("--insights", action="store_true", help="Sync only Insights/AI")
    
    args = parser.parse_args()
    
    if not settings.default_spreadsheet_id:
        logger.error("DEFAULT_SPREADSHEET_ID is not set in env.py or environment variables.")
        sys.exit(1)

    logger.info(f"Target Spreadsheet ID: {settings.default_spreadsheet_id}")

    # Determine what to run
    tasks = []
    
    if args.ksa:
        tasks = [p for p in SYNC_MAP if p["key"] == "KSA_TADAWUL"]
    elif getattr(args, "global"): # 'global' is a keyword, access safely
        tasks = [p for p in SYNC_MAP if p["key"] == "GLOBAL_MARKETS"]
    elif args.portfolio:
        tasks = [p for p in SYNC_MAP if p["key"] == "MY_PORTFOLIO"]
    elif args.insights:
        tasks = [p for p in SYNC_MAP if p["key"] == "INSIGHTS_ANALYSIS"]
    else:
        # Run ALL
        tasks = SYNC_MAP

    if not tasks:
        logger.warning("No matching tasks found.")
        return

    logger.info(f"Starting sync for {len(tasks)} pages...")
    
    for task in tasks:
        sync_page(task, dry_run=args.dry_run)
        # Small sleep between sheets to be nice to Google API quotas
        if not args.dry_run:
            time.sleep(2)

    logger.info("=== Dashboard Sync Complete ===")

if __name__ == "__main__":
    main()
