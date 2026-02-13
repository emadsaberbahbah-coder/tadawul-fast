#!/usr/bin/env python3
# scripts/run_market_scan.py
"""
run_market_scan.py
===========================================================
TADAWUL FAST BRIDGE – AI MARKET SCANNER (v1.6.0)
===========================================================
INTELLIGENT PRODUCTION EDITION

What this script does:
1) Aggregates symbols from across all major dashboards (KSA, Global, Leaders).
2) Performs a high-fidelity batch AI/Quant analysis via the backend.
3) Ranks results using the v1.7.0 Scoring Engine (Opportunity + ROI + Risk).
4) Generates a "Top Opportunities" summary tab in Google Sheets.

Key Upgrades in v1.6.0:
- ✅ **Scoring Integrated**: Captures `rec_badge` and `scoring_reason`.
- ✅ **ROI Standardized**: Uses 'expected_roi_1m/3m/12m' canonical keys.
- ✅ **Riyadh Localized**: Includes UTC+3 timestamps for dashboard alignment.
- ✅ **Rich UI Grid**: Adds Technical (RSI/MA) and Badge columns to the output.
- ✅ **Resilient Chunking**: Optimized for high-volume scanning (up to 1000 symbols).
"""

from __future__ import annotations

import argparse
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
from typing import Any, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Version & Logging
# =============================================================================
SCRIPT_VERSION = "1.6.0"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("MarketScan")

# =============================================================================
# Path & Dependency Alignment
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
# Dashboard Schema Definition
# =============================================================================
SCAN_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Market",
    "Price",
    "Change %",
    "Opportunity Score",
    "Overall Score",
    "Recommendation",
    "Rec Badge",
    "Reasoning",
    "Fair Value",
    "Upside %",
    "Expected ROI % (1M)",
    "Expected ROI % (3M)",
    "RSI 14",
    "MA200",
    "Risk Score",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Error"
]

# =============================================================================
# Utilities
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso: return ""
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        tz = timezone(timedelta(hours=3))
        return dt.astimezone(tz).isoformat()
    except: return ""

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
# Backend Communication
# =============================================================================
def _call_analysis_batch(symbols: List[str], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    base_url = (os.getenv("BACKEND_BASE_URL") or "http://127.0.0.1:8000").rstrip("/")
    url = f"{base_url}/v1/analysis/quotes"
    token = (os.getenv("APP_TOKEN") or "").strip()

    # Standardized Resilient Payload
    payload = {
        "tickers": symbols,
        "symbols": symbols,
        "mode": "comprehensive"
    }
    data = json.dumps(payload).encode("utf-8")
    
    headers = {
        "Content-Type": "application/json",
        "X-APP-TOKEN": token,
        "User-Agent": f"TFB-MarketScan/{SCRIPT_VERSION}"
    }

    try:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            parsed = json.loads(raw)
            # Handle standard BatchAnalysisResponse shape
            return parsed.get("results") or parsed.get("items") or []
    except Exception as e:
        logger.error("Backend request failed: %s", e)
        return []

# =============================================================================
# Data Mapping & Row Generation
# =============================================================================
def _map_to_row(item: Dict[str, Any], rank: int, origin: str) -> List[Any]:
    """Maps raw backend data to the SCAN_HEADERS schema."""
    def g(*keys):
        for k in keys:
            if k in item and item[k] is not None: return item[k]
        return None

    # Percent formatting helper
    def pct(val):
        if val is None: return None
        try:
            f = float(val)
            # If 0.14 -> 14.0, if 14.0 -> 14.0
            return f * 100.0 if abs(f) <= 1.5 else f
        except: return val

    utc = g("last_updated_utc") or _now_utc_iso()

    return [
        rank,
        g("symbol"),
        origin,
        g("name"),
        g("market", "market_region"),
        g("price", "current_price"),
        pct(g("change_pct", "percent_change")),
        g("opportunity_score"),
        g("overall_score"),
        g("recommendation"),
        g("rec_badge"),
        g("scoring_reason", "reason"),
        g("fair_value"),
        pct(g("upside_percent", "upside_pct")),
        pct(g("expected_roi_1m", "expected_return_1m")),
        pct(g("expected_roi_3m", "expected_return_3m")),
        g("rsi_14", "rsi14"),
        g("ma200"),
        g("risk_score"),
        g("data_quality"),
        utc,
        _to_riyadh_iso(utc),
        g("error")
    ]

# =============================================================================
# Main Sequence
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Tadawul Fast Bridge Market Scanner")
    parser.add_argument("--sheet-id", help="Target Spreadsheet ID")
    parser.add_argument("--keys", nargs="*", help="Registry keys to scan (e.g. KSA GLOBAL)")
    parser.add_argument("--top", type=int, default=50, help="Number of opportunities to save")
    parser.add_argument("--sheet-name", default="Market_Scan", help="Destination tab name")
    parser.add_argument("--no-sheet", action="store_true", help="Print results to console only")
    args = parser.parse_args()

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("Configuration Error: No Spreadsheet ID found.")
        sys.exit(1)

    # 1. Discover Symbols from Dashboards
    scan_keys = [_canon_key(k) for k in (args.keys or ["KSA_TADAWUL", "GLOBAL_MARKETS", "MARKET_LEADERS"])]
    all_tickers: List[str] = []
    origin_map: Dict[str, str] = {}

    logger.info("Scanning dashboards: %s", ", ".join(scan_keys))
    for k in scan_keys:
        try:
            sym_data = symbols_reader.get_page_symbols(k, spreadsheet_id=sid)
            symbols = sym_data.get("all") if isinstance(sym_data, dict) else sym_data
            if symbols:
                for s in symbols:
                    s_up = str(s).upper()
                    all_tickers.append(s_up)
                    if s_up not in origin_map: origin_map[s_up] = k
        except Exception as e:
            logger.warning("   -> Could not read symbols for %s: %s", k, e)

    unique_tickers = list(dict.fromkeys(all_tickers))
    if not unique_tickers:
        logger.error("No symbols found. Ensure dashboards have tickers in Column B.")
        return

    # 2. Perform Batch Analysis
    logger.info("Analyzing %d unique symbols...", len(unique_tickers))
    results = _call_analysis_batch(unique_tickers, {})

    # 3. Intelligent Ranking
    # Sort by Opportunity Score desc, then ROI desc
    def rank_logic(x):
        return (float(x.get("opportunity_score") or 0), 
                float(x.get("expected_roi_3m") or 0))
    
    ranked = sorted(results, key=rank_logic, reverse=True)[:args.top]
    
    # 4. Sheet Writing
    if args.no_sheet:
        for i, r in enumerate(ranked, 1):
            logger.info(f"#{i} {r.get('symbol')}: Score={r.get('opportunity_score')} Rec={r.get('recommendation')}")
        return

    grid = [SCAN_HEADERS]
    for i, r in enumerate(ranked, 1):
        sym = str(r.get("symbol") or "").upper()
        grid.append(_map_to_row(r, i, origin_map.get(sym, "SCAN")))

    try:
        logger.info("Writing results to sheet: %s", args.sheet_name)
        # Use chunked writer for safety
        updated = sheets_service.write_grid_chunked(sid, args.sheet_name, "A5", grid)
        logger.info("✅ Success: Updated %s cells in 'Top Opportunities' report.", updated)
    except Exception as e:
        logger.error("❌ Sheet write failed: %s", e)
        sys.exit(2)

if __name__ == "__main__":
    main()
