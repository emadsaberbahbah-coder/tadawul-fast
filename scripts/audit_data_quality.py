#!/usr/bin/env python3
# scripts/audit_data_quality.py
"""
audit_data_quality.py
===========================================================
TADAWUL FAST BRIDGE – DATA & STRATEGY AUDITOR (v1.1.0)
===========================================================
FINANCIAL LEADER EDITION

Purpose:
- Scans all configured symbols in your Dashboards.
- Identifies "Zombie" tickers (stale data, zero price, delisted).
- **NEW**: Performs "System Self-Review" by back-testing current scores against realized performance.
- Flags low-quality forecasts (low confidence, missing history).
- Generates a "Clean-Up Report" and "Strategy Performance" summary.

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
AUDIT_VERSION = "1.1.0"

# Thresholds for "Bad Data"
THRESHOLDS = {
    "stale_hours": 72,      # Data older than 3 days is suspect (weekends excluded logic simplified)
    "min_price": 0.01,      # Price below this is considered "Zero/Null"
    "min_confidence": 30.0, # Forecast confidence below 30% is weak
    "min_history": 20       # Fewer than 20 history points is insufficient for analysis
}

def _parse_iso_time(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str: return None
    try:
        return datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
    except:
        return None

def _is_stale(last_updated: Optional[str]) -> bool:
    dt = _parse_iso_time(last_updated)
    if not dt: return True # Missing time = Stale
    
    # Simple check: Is it older than threshold?
    # (In a real trading app, we'd handle weekends, but 72h covers Friday-Sunday gaps usually)
    delta = datetime.now(timezone.utc) - dt
    return delta.total_seconds() > (THRESHOLDS["stale_hours"] * 3600)

def _perform_self_review(q: Dict[str, Any]) -> List[str]:
    """
    Analyzes the 'Strategy Quality' by comparing scores vs reality.
    This creates a feedback loop for self-learning.
    """
    reviews = []
    
    # 1. Momentum Validation (Did high momentum align with recent gains?)
    # Score 0-100 vs Returns %
    mom_score = float(q.get("momentum_score") or 50)
    ret_1m = float(q.get("returns_1m") or 0)
    
    if mom_score > 75 and ret_1m < -5.0:
        reviews.append("MOM_FAIL_BULL") # System said UP, Market went DOWN
    elif mom_score < 25 and ret_1m > 5.0:
        reviews.append("MOM_FAIL_BEAR") # System said DOWN, Market went UP
        
    # 2. Risk Calibration (Did high risk score match high volatility?)
    risk_score = float(q.get("risk_score") or 50)
    vol_30d = float(q.get("volatility_30d") or 0)
    
    if risk_score > 80 and vol_30d < 15.0:
        reviews.append("RISK_OVEREST") # Flagging safe stocks as risky?
    elif risk_score < 30 and vol_30d > 50.0:
        reviews.append("RISK_UNDEREST") # Missing dangerous volatility?
        
    # 3. Forecast Plausibility (Is the 1M target unrealistically far from trend?)
    exp_roi_1m = float(q.get("expected_roi_1m") or 0)
    if exp_roi_1m > 20.0 and ret_1m < -10.0:
        reviews.append("FCST_CONTRARIAN") # Betting on a massive V-shape recovery

    return reviews

def _audit_quote(symbol: str, q: Dict[str, Any]) -> Dict[str, Any]:
    """Analyzes a single quote for quality issues AND strategy performance."""
    issues = []
    
    # 1. Price Integrity
    price = q.get("current_price")
    if price is None or float(price) < THRESHOLDS["min_price"]:
        issues.append("ZERO_PRICE")

    # 2. Freshness
    last_upd = q.get("last_updated_utc")
    if _is_stale(last_upd):
        issues.append("STALE_DATA")

    # 3. Forecast Quality
    conf = float(q.get("forecast_confidence") or 0) * 100
    if conf < THRESHOLDS["min_confidence"]:
        issues.append("LOW_CONFIDENCE")
        
    hist_pts = int(q.get("history_points") or 0)
    if hist_pts < THRESHOLDS["min_history"]:
        issues.append("NO_HISTORY")

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
        status = "REVIEW" # Data is good, but strategy might be misaligned

    return {
        "symbol": symbol,
        "status": status,
        "issues": issues,
        "strategy_notes": strategy_notes,
        "price": price,
        "last_updated": last_upd,
        "source": q.get("data_source", "unknown"),
        "confidence": round(conf, 1)
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
    strategy_stats = {"reviewed": 0, "divergences": 0}
    
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

        # 2. Fetch Data (Force Refresh)
        logger.info("   -> Fetching data for %d symbols...", len(symbols))
        # Using batch fetch from engine
        if hasattr(engine, "get_enriched_quotes"):
            quotes_list = await engine.get_enriched_quotes(symbols, refresh=True)
            quotes_map = {s: q for s, q in zip(symbols, quotes_list)}
        else:
            # Fallback serial
            quotes_map = {}
            for s in symbols:
                quotes_map[s] = await engine.get_enriched_quote(s, refresh=True)

        # 3. Analyze
        page_issues = 0
        for sym in symbols:
            q = quotes_map.get(sym) or {}
            report = _audit_quote(sym, q)
            report["origin_page"] = key
            
            if report["strategy_notes"]:
                strategy_stats["divergences"] += 1
            strategy_stats["reviewed"] += 1
            
            if report["status"] != "OK":
                page_issues += 1
                all_reports.append(report)
        
        logger.info("   -> Found %d issues/alerts on %s", page_issues, key)

    return all_reports, strategy_stats

# =============================================================================
# Main
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="TFB Data Quality & Strategy Auditor")
    parser.add_argument("--keys", nargs="*", default=None, help="Specific pages to audit")
    parser.add_argument("--json-out", help="Save report to JSON file")
    parser.add_argument("--sheet-id", help="Override Spreadsheet ID")
    args = parser.parse_args()

    # Resolve Sheet ID
    sid = args.sheet_id or getattr(settings, "default_spreadsheet_id", "") or os.getenv("DEFAULT_SPREADSHEET_ID")
    if not sid:
        logger.error("No Spreadsheet ID found.")
        sys.exit(1)

    # Resolve Keys
    target_keys = args.keys
    if not target_keys:
        # Default to all known keys in registry
        target_keys = list(symbols_reader.PAGE_REGISTRY.keys())

    # Run Async Loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        reports, stats = loop.run_until_complete(_run_audit(target_keys, sid))
    finally:
        loop.close()

    # Summary
    if not reports:
        logger.info("\n✅ AUDIT COMPLETE: No data quality issues found! Your dashboard is clean.")
        return

    logger.info("\n=== ⚠️  AUDIT REPORT: %d ITEMS ===", len(reports))
    print(f"{'SYMBOL':<12} | {'STATUS':<10} | {'SOURCE':<10} | {'DETAILS'}")
    print("-" * 80)
    
    for r in reports:
        # Combine issues and strategy notes
        details = r["issues"] + r["strategy_notes"]
        details_str = ", ".join(details)
        print(f"{r['symbol']:<12} | {r['status']:<10} | {r['source']:<10} | {details_str}")

    print("-" * 80)
    
    # Self-Learning / Strategy Review Section
    div_rate = (stats['divergences'] / stats['reviewed'] * 100) if stats['reviewed'] else 0
    logger.info("🧠 SYSTEM SELF-REVIEW:")
    logger.info(f"   Analyzed {stats['reviewed']} assets.")
    logger.info(f"   Strategy Divergence Rate: {div_rate:.1f}%")
    if div_rate > 20:
        logger.warning("   -> High divergence detected. Consider adjusting Scoring Engine weights.")
    else:
        logger.info("   -> Strategy is tracking well with market reality.")
    
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump({
                "audit_time": _utc_now_iso(), 
                "stats": stats,
                "issues": reports
            }, f, indent=2)
        logger.info("Report saved to %s", args.json_out)

def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

if __name__ == "__main__":
    main()
