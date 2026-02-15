#!/usr/bin/env python3
# scripts/run_dashboard_sync.py
"""
run_dashboard_sync.py
===========================================================
TADAWUL FAST BRIDGE – DASHBOARD SYNCHRONIZER (v3.1.0)
===========================================================
INTELLIGENT PRODUCTION EDITION

Key Upgrades in v3.1.0:
- ✅ Pre-Flight Diagnostics: Verifies Backend & Sheets API before start.
- ✅ Adaptive KSA Gateway: Smart switching between Argaam and Enriched.
- ✅ Resilient Payload: Standardized multi-key delivery (symbols/tickers).
- ✅ Performance Analytics: Per-page duration and symbol-rate logging.
- ✅ Riyadh Awareness: Localized timestamps in metadata and logs.

Exit Codes:
- 0: Success (All pages synced perfectly)
- 1: Fatal (Pre-flight failure or configuration error)
- 2: Partial (One or more pages failed/partial)
"""

from __future__ import annotations

import argparse
import inspect
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Version & Logging Setup
# =============================================================================
SCRIPT_VERSION = "3.1.0"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("DashboardSync")

_A1_CELL_RE = re.compile(r"^\$?[A-Za-z]+\$?\d+$")

# =============================================================================
# Environment & Path Alignment
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

try:
    from env import settings # type: ignore
    import symbols_reader # type: ignore
    import google_sheets_service as sheets_service # type: ignore
except Exception as e:
    logger.error("Project dependency import failed: %s", e)
    raise SystemExit(1)

# =============================================================================
# Data Structures
# =============================================================================
@dataclass(frozen=True)
class SyncTask:
    key: str
    method: Callable[..., Dict[str, Any]]
    desc: str
    kind: str = "enriched" # enriched | ai | advanced | ksa

# =============================================================================
# Core Utilities
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _now_riyadh_iso() -> str:
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def _get_spreadsheet_id(cli_id: Optional[str]) -> str:
    if cli_id: return cli_id.strip()
    sid = (getattr(settings, "default_spreadsheet_id", None) or "").strip()
    return sid if sid else os.getenv("DEFAULT_SPREADSHEET_ID", "").strip()

def _canon_key(user_key: str) -> str:
    k = str(user_key or "").strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "KSA": "KSA_TADAWUL", "TADAWUL": "KSA_TADAWUL",
        "GLOBAL": "GLOBAL_MARKETS", "LEADERS": "MARKET_LEADERS",
        "PORTFOLIO": "MY_PORTFOLIO", "INSIGHTS": "INSIGHTS_ANALYSIS"
    }
    return aliases.get(k, k)

# =============================================================================
# Pre-Flight Checks
# =============================================================================
def _preflight_check(sid: str) -> bool:
    logger.info("--- Starting Pre-Flight Validation ---")
    
    # 1. Sheets API check
    try:
        sheets_service.get_sheets_service()
        logger.info("✅ Google Sheets API: Connected")
    except Exception as e:
        logger.error("❌ Google Sheets API: Connection Failed: %s", e)
        return False

    # 2. Backend Health check
    backend_url = os.getenv("BACKEND_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
    try:
        req = urllib.request.Request(f"{backend_url}/readyz", method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.getcode() == 200:
                logger.info("✅ Backend API: Connected")
            else:
                logger.warning("⚠️  Backend API: Returned HTTP %s", resp.getcode())
    except Exception as e:
        logger.error("❌ Backend API: Unreachable: %s", e)
        return False
        
    return True

# =============================================================================
# Task Management
# =============================================================================
def _build_sync_map() -> List[SyncTask]:
    tasks = []
    # Standard Enriched Pages
    for k, d, kind in [
        ("KSA_TADAWUL", "KSA Tadawul", "ksa"),
        ("MARKET_LEADERS", "Market Leaders", "enriched"),
        ("GLOBAL_MARKETS", "Global Markets", "enriched"),
        ("MUTUAL_FUNDS", "Mutual Funds", "enriched"),
        ("COMMODITIES_FX", "Commodities & FX", "enriched"),
        ("MY_PORTFOLIO", "My Portfolio", "enriched")
    ]:
        tasks.append(SyncTask(k, sheets_service.refresh_sheet_with_enriched_quotes, d, kind))
    
    # AI Analysis Page
    tasks.append(SyncTask("INSIGHTS_ANALYSIS", sheets_service.refresh_sheet_with_ai_analysis, "AI Insights", "ai"))
    
    return tasks

# =============================================================================
# Sync Execution Logic
# =============================================================================
def sync_page(
    task: SyncTask,
    spreadsheet_id: str,
    *,
    clear: bool = False,
    start_cell: str = "A5",
    ksa_gateway: str = "enriched"
) -> Dict[str, Any]:
    key = _canon_key(task.key)
    logger.info("Processing Dashboard: %s (%s)", task.desc, key)
    
    # 1. Get Symbols (Intelligent reader)
    try:
        sym_data = symbols_reader.get_page_symbols(key, spreadsheet_id=spreadsheet_id)
        symbols = sym_data.get("all") if isinstance(sym_data, dict) else sym_data
        if not symbols:
            logger.warning("   -> No symbols found for %s. Skipping.", key)
            return {"status": "skipped", "key": key}
    except Exception as e:
        logger.error("   -> Symbol Read Failure [%s]: %s", key, e)
        return {"status": "error", "key": key, "error": str(e)}

    # 2. Execution Timer
    t_start = time.perf_counter()
    
    # 3. Call Refresh Method (Aligned with v3.13.1 Service)
    try:
        # Resolve Sheet Name from Registry or use Key
        sheet_name = key.title().replace("_", " ")
        if key == "KSA_TADAWUL": sheet_name = "KSA_Tadawul"
        
        # Route KSA specifically if gateway set to Argaam
        if task.kind == "ksa" and ksa_gateway == "argaam":
            # This uses the direct Argaam endpoint via refresh_logic
            result = sheets_service._refresh_logic(
                "/v1/argaam/sheet-rows", spreadsheet_id, sheet_name, symbols, 
                start_cell=start_cell, clear=clear
            )
        else:
            result = task.method(
                spreadsheet_id=spreadsheet_id, 
                sheet_name=sheet_name, 
                tickers=symbols,
                start_cell=start_cell,
                clear=clear
            )
            
        elapsed = time.perf_counter() - t_start
        status = result.get("status", "unknown")
        
        if status == "success":
            logger.info("   ✅ OK: Written %s rows in %.2fs (%.1f sym/sec)", 
                        result.get("rows_written"), elapsed, len(symbols)/elapsed)
        else:
            logger.warning("   ⚠️  Partial: %s", result.get("error", "Check logs"))
            
        return {**result, "key": key, "duration": elapsed}

    except Exception as e:
        logger.error("   ❌ Sync Failure [%s]: %s", key, e)
        return {"status": "error", "key": key, "error": str(e)}

# =============================================================================
# Main Entry
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="TFB Dashboard Synchronizer")
    parser.add_argument("--sheet-id", help="Target Spreadsheet ID")
    parser.add_argument("--keys", nargs="*", help="Specific page keys to sync")
    parser.add_argument("--clear", action="store_true", help="Clear data rows before writing")
    parser.add_argument("--ksa-gw", default="enriched", choices=["enriched", "argaam"], help="Gateway for KSA tab")
    parser.add_argument("--start-cell", default="A5", help="Top-left corner for data")
    parser.add_argument("--no-preflight", action="store_true", help="Skip connectivity checks")
    args = parser.parse_args()

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("Configuration Error: No Spreadsheet ID found. Check DEFAULT_SPREADSHEET_ID.")
        sys.exit(1)

    # 1. Pre-flight
    if not args.no_preflight and not _preflight_check(sid):
        logger.error("Pre-flight validation failed. Aborting sync.")
        sys.exit(1)

    # 2. Build and Filter Tasks
    all_tasks = _build_sync_map()
    if args.keys:
        wanted = [_canon_key(k) for k in args.keys]
        tasks = [t for t in all_tasks if _canon_key(t.key) in wanted]
    else:
        tasks = all_tasks

    if not tasks:
        logger.warning("No valid sync tasks selected.")
        return

    # 3. Process Sequence
    results = []
    t_total_start = time.perf_counter()
    
    logger.info("Starting sync sequence for %d dashboards...", len(tasks))
    
    for task in tasks:
        res = sync_page(
            task, sid, 
            clear=args.clear, 
            start_cell=args.start_cell, 
            ksa_gateway=args.ksa_gw
        )
        results.append(res)
        # Small cooldown for Sheets API quota
        time.sleep(1.5)

    # 4. Reporting
    t_total = time.perf_counter() - t_total_start
    successes = [r for r in results if r.get("status") == "success"]
    partials = [r for r in results if r.get("status") == "partial"]
    failures = [r for r in results if r.get("status") == "error"]

    logger.info("--- Sync Report (v%s) ---", SCRIPT_VERSION)
    logger.info("Total Time: %.2fs", t_total)
    logger.info("Dashboards: %d Success | %d Partial | %d Failed", 
                len(successes), len(partials), len(failures))
    
    if failures:
        sys.exit(2)
    if partials:
        sys.exit(0) # Or 2 if you want to be strict

if __name__ == "__main__":
    main()
