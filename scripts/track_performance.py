#!/usr/bin/env python3
# scripts/track_performance.py
"""
track_performance.py
===========================================================
TADAWUL FAST BRIDGE – PERFORMANCE BENCHMARKER (v1.0.0)
===========================================================
LEADER EDITION – PROOF OF ROI

Purpose:
- Monitors the accuracy of "Buy" and "Strong Buy" recommendations.
- Saves active recommendations to a 'Performance_Log' tab.
- Automatically calculates "Realized ROI" once the timeframe (1W/1M/3M) is reached.
- Provides a "Win Rate" diagnostic for the Investment Advisor logic.

Usage:
  python scripts/track_performance.py --record    # Records current top picks
  python scripts/track_performance.py --audit     # Updates ROI for existing logs
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

# =============================================================================
# Path & Dependency Setup
# =============================================================================
def _ensure_project_root_on_path() -> None:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        parent = os.path.dirname(here)
        for p in (here, parent):
            if p and p not in sys.path:
                sys.path.insert(0, p)
    except Exception:
        pass

_ensure_project_root_on_path()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("PerformanceTrack")

try:
    from env import settings
    import google_sheets_service as sheets
    from core.data_engine_v2 import get_engine
except ImportError as e:
    logger.error("Critical Dependency Missing: %s", e)
    sys.exit(1)

# =============================================================================
# Constants & Defaults
# =============================================================================
PERF_SHEET_NAME = "Performance_Log"
PERF_HEADERS = [
    "Date Recorded", "Symbol", "Recommendation", "Entry Price", 
    "Expected ROI %", "Target Date", "Status", 
    "Current Price", "Realized ROI %", "Accuracy", "Last Updated (Riyadh)"
]

# =============================================================================
# Utility Helpers
# =============================================================================
def _riyadh_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))

def _riyadh_iso() -> str:
    return _riyadh_now().isoformat()

def _to_float(x: Any) -> float:
    try: return float(str(x).replace("%", "").replace(",", "").strip())
    except: return 0.0

# =============================================================================
# Core Logic
# =============================================================================
async def record_recommendations(sid: str):
    """Fetches current 'Top Opportunities' and logs them for tracking."""
    logger.info("📡 Fetching top picks to record...")
    
    # 1. Read the 'Market_Scan' or 'Investment_Advisor' output
    try:
        # We try to get data from the Market_Scan sheet (Row 6 downwards)
        raw_data = sheets.read_range(sid, "Market_Scan!A6:P50")
        if not raw_data:
            logger.warning("No data found in Market_Scan to record.")
            return
    except Exception as e:
        logger.error("Failed to read Market_Scan: %s", e)
        return

    to_record = []
    timestamp = _riyadh_now().strftime("%Y-%m-%d")
    
    for row in raw_data:
        if len(row) < 10: continue
        
        symbol = str(row[1]).strip()
        reco = str(row[9]).strip().upper()
        price = _to_float(row[5])
        roi = _to_float(row[14]) # Expected ROI 1M
        
        if reco in ("BUY", "STRONG BUY") and symbol:
            # Set a target date 30 days from now
            target_date = (_riyadh_now() + timedelta(days=30)).strftime("%Y-%m-%d")
            
            to_record.append([
                timestamp, symbol, reco, price, 
                roi, target_date, "ACTIVE", 
                price, 0.0, "PENDING", _riyadh_iso()
            ])

    if not to_record:
        logger.info("No 'BUY' signals found to log today.")
        return

    # 2. Append to Performance_Log
    logger.info("✍️ Logging %d new recommendations...", len(to_record))
    sheets.append_rows(sid, f"{PERF_SHEET_NAME}!A6", to_record)
    logger.info("✅ Recording complete.")

async def audit_existing_logs(sid: str):
    """Checks previously recorded logs and updates their realized ROI."""
    logger.info("🔍 Auditing existing performance logs...")
    
    try:
        # Read the log (limiting to last 200 entries)
        log_range = f"{PERF_SHEET_NAME}!A6:K200"
        rows = sheets.read_range(sid, log_range)
        if not rows:
            logger.info("Performance log is empty.")
            return
    except Exception:
        logger.warning("Performance_Log sheet might not exist yet.")
        return

    engine = await get_engine()
    tickers = [str(r[1]).strip() for r in rows if len(r) > 1 and str(r[1]).strip()]
    
    if not tickers: return

    # Batch fetch current prices
    logger.info("🔄 Fetching current prices for %d assets...", len(tickers))
    quotes_list = await engine.get_enriched_quotes(tickers, refresh=True)
    quotes_map = {q.get("symbol"): q for q in quotes_list if isinstance(q, dict)}

    updated_rows = []
    wins, losses = 0, 0

    for i, row in enumerate(rows):
        if len(row) < 7: 
            updated_rows.append(row)
            continue
            
        symbol = str(row[1]).strip()
        entry_price = _to_float(row[3])
        status = str(row[6]).upper()
        
        quote = quotes_map.get(symbol) or {}
        current_price = _to_float(quote.get("current_price") or quote.get("price"))
        
        if current_price > 0 and entry_price > 0:
            realized_roi = ((current_price / entry_price) - 1.0) * 100.0
            row[7] = current_price
            row[8] = round(realized_roi, 2)
            
            # Update status if target date passed
            try:
                target_dt = datetime.strptime(str(row[5]), "%Y-%m-%d")
                if datetime.now() > target_dt:
                    row[6] = "MATURED"
            except: pass
            
            # Accuracy Logic
            if realized_roi > 0: 
                row[9] = "WIN ✅"
                wins += 1
            else: 
                row[9] = "LOSS ❌"
                losses += 1
                
            row[10] = _riyadh_iso()
            
        updated_rows.append(row)

    # Write back the updated grid
    sheets.write_range(sid, log_range, updated_rows)
    
    win_rate = (wins / (wins + losses)) * 100 if (wins + losses) > 0 else 0
    logger.info("✅ Audit Complete. Real-time Win Rate: %.1f%%", win_rate)

# =============================================================================
# Main Entry
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="TFB Performance Benchmarker")
    parser.add_argument("--record", action="store_true", help="Record current signals")
    parser.add_argument("--audit", action="store_true", help="Update existing logs with current prices")
    parser.add_argument("--sheet-id", help="Override Spreadsheet ID")
    args = parser.parse_args()

    sid = args.sheet_id or getattr(settings, "default_spreadsheet_id", "") or os.getenv("DEFAULT_SPREADSHEET_ID")
    if not sid:
        logger.error("No Spreadsheet ID found.")
        sys.exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        if args.record:
            loop.run_until_complete(record_recommendations(sid))
        
        if args.audit:
            loop.run_until_complete(audit_existing_logs(sid))
            
        if not args.record and not args.audit:
            logger.info("No action specified. Use --record or --audit.")
            
    finally:
        loop.close()

if __name__ == "__main__":
    main()
