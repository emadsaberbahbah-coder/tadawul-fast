#!/usr/bin/env python3
# scripts/audit_data_quality.py
"""
audit_data_quality.py
===========================================================
TADAWUL FAST BRIDGE – DATA & STRATEGY AUDITOR (v1.2.0)
===========================================================
ADVANCED ANALYTICS EDITION

Purpose:
- Scans all configured symbols in your Dashboards.
- Identifies "Zombie" tickers (stale data, zero price, delisted).
- **NEW**: Validates Technical Integrity (History depth for MACD/Trend).
- **NEW**: "Reality Check" compares AI Trend Signals vs Realized 1W Returns.
- Generates a "Clean-Up Report" and "System Self-Diagnosis".

Usage:
  python scripts/audit_data_quality.py
  python scripts/audit_data_quality.py --keys MARKET_LEADERS KSA_TADAWUL
  python scripts/audit_data_quality.py --json-out audit_report.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
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

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("DataAudit")

try:
    from env import settings  # type: ignore
    import symbols_reader  # type: ignore
    from core.data_engine_v2 import get_engine  # type: ignore
except ImportError as e:
    logger.error("Critical Import Error: %s", e)
    logger.error("Ensure you are running from the project root.")
    sys.exit(1)

# =============================================================================
# Audit Rules & Logic
# =============================================================================
AUDIT_VERSION = "1.2.0"

# Thresholds for "Bad Data"
THRESHOLDS = {
    "stale_hours": 72,      # Data older than 3 days is suspect
    "min_price": 0.01,      # Price below this is considered "Zero/Null"
    "min_confidence": 30.0, # Forecast confidence below 30% is weak
    "min_history": 35,      # Need 35+ pts for MACD(26) + signal(9) calc
    "trend_tolerance": 4.0  # % move against trend to trigger a flag
}

def _parse_iso_time(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str: return None
    try:
        return datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
    except:
        return None

def _is_stale(last_updated: Optional[str]) -> bool:
    dt = _parse_iso_time(last_updated)
    if not dt: return True
    delta = datetime.now(timezone.utc) - dt
    return delta.total_seconds() > (THRESHOLDS["stale_hours"] * 3600)

def _perform_self_review(q: Dict[str, Any]) -> List[str]:
    """
    Advanced Self-Review: Checks if Technicals/AI match Reality.
    """
    reviews = []
    
    # Extract Metrics
    trend_sig = str(q.get("trend_signal") or "NEUTRAL").upper()
    ret_1w = float(q.get("returns_1w") or 0)
    vol_30d = float(q.get("volatility_30d") or 0)
    exp_roi_1m = float(q.get("expected_roi_1m") or 0)
    macd_hist = float(q.get("macd_hist") or 0)
    
    # 1. Trend Reality Check
    # If AI says UPTREND but we lost >4% this week -> Trend Broken or Lagging
    if trend_sig == "UPTREND" and ret_1w < -THRESHOLDS["trend_tolerance"]:
        reviews.append("TREND_BREAK_BEAR")
    # If AI says DOWNTREND but we gained >4% this week -> Trend Reversal
    elif trend_sig == "DOWNTREND" and ret_1w > THRESHOLDS["trend_tolerance"]:
        reviews.append("TREND_BREAK_BULL")
        
    # 2. Momentum Divergence
    # MACD says bullish (pos hist) but price falling hard
    if macd_hist > 0 and ret_1w < -5.0:
        reviews.append("MOM_DIVERGENCE_BEAR")
        
    # 3. Forecast Plausibility
    # Forecasting +20% month on a low vol stock is suspicious
    if exp_roi_1m > 20.0 and vol_30d < 10.0:
        reviews.append("AGGRESSIVE_FORECAST")
        
    # 4. Risk Calibration
    risk_score = float(q.get("risk_score") or 50)
    # High risk score but very low volatility? System might be too conservative.
    if risk_score > 85 and vol_30d < 10.0:
        reviews.append("RISK_OVEREST")

    return reviews

def _audit_quote(symbol: str, q: Dict[str, Any]) -> Dict[str, Any]:
    """Analyzes data integrity AND technical sufficiency."""
    issues = []
    
    # 1. Price Integrity
    price = q.get("current_price")
    if price is None or float(price) < THRESHOLDS["min_price"]:
        issues.append("ZERO_PRICE")

    # 2. Freshness
    last_upd = q.get("last_updated_utc")
    if _is_stale(last_upd):
        issues.append("STALE_DATA")

    # 3. Technical Integrity (New in v1.2.0)
    hist_pts = int(q.get("history_points") or 0)
    if hist_pts < THRESHOLDS["min_history"]:
        issues.append(f"INSUFFICIENT_HISTORY ({hist_pts}<{THRESHOLDS['min_history']})")
    
    # 4. Meta
    err = q.get("error")
    if err:
        issues.append(f"PROVIDER_ERROR: {str(err)[:30]}...")
        
    # 5. Self Review (Strategy Check)
    strategy_notes = _perform_self_review(q)
    
    status = "OK"
    if "ZERO_PRICE" in issues or "PROVIDER_ERROR" in issues:
        status = "CRITICAL"
    elif issues:
        status = "WARNING"
    elif strategy_notes:
        status = "REVIEW" # Data valid, but model divergent

    # Ensure Confidence is float
    conf = 0.0
    try:
        conf = float(q.get("forecast_confidence") or 0) * 100
    except: pass

    return {
        "symbol": symbol,
        "status": status,
        "issues": issues,
        "strategy_notes": strategy_notes,
        "price": price,
        "last_updated": last_upd,
        "source": q.get("data_source", "unknown"),
        "confidence": round(conf, 1),
        "history_points": hist_pts
    }

# =============================================================================
# Batch Processor
# =============================================================================
async def _run_audit(keys: List[str], sid: str):
    engine = await get_engine()
    if not engine:
        logger.error("Failed to initialize Data Engine.")
        return

    logger.info("🚀 Starting Data Quality & Strategy Audit (v%s)", AUDIT_VERSION)
    
    all_reports = []
    # Statistics for System Self-Diagnosis
    stats = {
        "total_assets": 0,
        "trend_breaks": 0,
        "data_holes": 0,
        "aggressive_forecasts": 0
    }
    
    for key in keys:
        logger.info("🔍 Scanning Page: %s", key)
        
        # 1. Read Symbols
        try:
            sym_data = symbols_reader.get_page_symbols(key, spreadsheet_id=sid)
            symbols = sym_data.get("all") if isinstance(sym_data, dict) else sym_data
        except Exception as e:
            logger.warning("   Skipping %s: %s", key, e)
            continue
            
        if not symbols:
            logger.info("   -> Empty page.")
            continue

        # 2. Fetch Data (Force Refresh to check live provider health)
        logger.info("   -> Fetching data for %d symbols...", len(symbols))
        if hasattr(engine, "get_enriched_quotes"):
            quotes_list = await engine.get_enriched_quotes(symbols, refresh=True)
            quotes_map = {s: q for s, q in zip(symbols, quotes_list)}
        else:
            quotes_map = {}
            for s in symbols:
                quotes_map[s] = await engine.get_enriched_quote(s, refresh=True)

        # 3. Analyze
        page_issues = 0
        for sym in symbols:
            stats["total_assets"] += 1
            q = quotes_map.get(sym) or {}
            report = _audit_quote(sym, q)
            report["origin_page"] = key
            
            # Aggregate stats
            for note in report["strategy_notes"]:
                if "TREND_BREAK" in note: stats["trend_breaks"] += 1
                if "AGGRESSIVE" in note: stats["aggressive_forecasts"] += 1
            
            for issue in report["issues"]:
                if "INSUFFICIENT" in issue or "ZERO_PRICE" in issue: stats["data_holes"] += 1

            if report["status"] != "OK":
                page_issues += 1
                all_reports.append(report)
        
        logger.info("   -> Found %d alerts on %s", page_issues, key)

    return all_reports, stats

# =============================================================================
# Main
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="TFB Data Quality & Strategy Auditor")
    parser.add_argument("--keys", nargs="*", default=None, help="Specific pages to audit")
    parser.add_argument("--json-out", help="Save report to JSON file")
    parser.add_argument("--sheet-id", help="Override Spreadsheet ID")
    args = parser.parse_args()

    sid = args.sheet_id or getattr(settings, "default_spreadsheet_id", "") or os.getenv("DEFAULT_SPREADSHEET_ID")
    if not sid:
        logger.error("No Spreadsheet ID found.")
        sys.exit(1)

    target_keys = args.keys
    if not target_keys:
        target_keys = list(symbols_reader.PAGE_REGISTRY.keys())

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        reports, stats = loop.run_until_complete(_run_audit(target_keys, sid))
    finally:
        loop.close()

    if not reports:
        logger.info("\n✅ AUDIT COMPLETE: System is 100% healthy.")
        return

    logger.info("\n=== ⚠️  AUDIT REPORT: %d ITEMS REQUIRING ATTENTION ===", len(reports))
    print(f"{'SYMBOL':<12} | {'STATUS':<10} | {'SOURCE':<10} | {'DETAILS'}")
    print("-" * 100)
    
    for r in reports:
        details = r["issues"] + r["strategy_notes"]
        details_str = ", ".join(details)
        print(f"{r['symbol']:<12} | {r['status']:<10} | {r['source']:<10} | {details_str}")

    print("-" * 100)
    
    # SYSTEM DIAGNOSIS
    total = stats["total_assets"] or 1
    trend_fail_rate = (stats["trend_breaks"] / total) * 100
    data_fail_rate = (stats["data_holes"] / total) * 100

    logger.info("🧠 SYSTEM DIAGNOSIS (v%s):", AUDIT_VERSION)
    logger.info(f"   Analyzed: {total} Assets")
    
    if data_fail_rate > 10:
        logger.warning(f"   🔴 Data Integrity Critical: {data_fail_rate:.1f}% of assets have broken data/history.")
        logger.warning("      Action: Check provider API keys or clean up delisted tickers.")
    else:
        logger.info(f"   🟢 Data Integrity: Healthy ({data_fail_rate:.1f}% failure rate)")

    if trend_fail_rate > 25:
        logger.warning(f"   🟠 Strategy Divergence: {trend_fail_rate:.1f}% of assets moving against Trend Signal.")
        logger.warning("      Action: Market may be choppy. Consider increasing 'trend_30d' sensitivity in engine.")
    else:
        logger.info(f"   🟢 Strategy Alignment: Good ({trend_fail_rate:.1f}% divergence)")

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump({
                "audit_time": _utc_now_iso(), 
                "system_stats": stats,
                "alerts": reports
            }, f, indent=2)
        logger.info("Detailed report saved to %s", args.json_out)

def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

if __name__ == "__main__":
    main()
