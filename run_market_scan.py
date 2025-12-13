"""
run_market_scan.py
===========================================================
TADAWUL FAST BRIDGE – MARKET SCANNER & ANALYZER (v2.0.0)
===========================================================

What it does:
1. Initializes the Unified Data Engine (V2).
2. Fetches real-time data + fundamentals for a watchlist.
3. Applies the AI Scoring Engine (Value, Quality, Momentum).
4. Generates a ranked "Opportunity Scoreboard" in the console.
5. Exports a full detailed CSV report compatible with Excel/Sheets.

Usage:
    python run_market_scan.py

Configuration (Env Vars):
    SCAN_TICKERS="1120.SR,2222.SR,AAPL,MSFT"
    SCAN_TICKERS_FILE="watchlist.txt"  # Path to file with one ticker per line
    SCAN_TOP_N="20"                    # How many top picks to show
    SCAN_MIN_OPP_SCORE="60"            # Minimum score filter
"""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

# --- SETUP PATH ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# --- IMPORTS ---
try:
    from env import settings
    from core.data_engine_v2 import DataEngine, UnifiedQuote
    from core.enriched_quote import EnrichedQuote
    from core.scoring_engine import enrich_with_scores
except ImportError as e:
    print(f"CRITICAL ERROR: Could not import core modules. Run from project root.\nError: {e}")
    sys.exit(1)

# Optional pretty printing
try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

# --- LOGGING ---
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("MarketScan")

# --- DEFAULTS ---
DEFAULT_KSA = ["1120.SR", "1180.SR", "2222.SR", "2010.SR", "7010.SR", "4030.SR", "7202.SR", "5110.SR"]
DEFAULT_GLOBAL = ["AAPL", "NVDA", "MSFT", "TSLA", "AMZN", "GOOGL", "EURUSD=X", "BTC-USD", "GC=F"]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def load_watchlist() -> List[str]:
    """
    Load tickers from File > Env Var > Defaults.
    """
    # 1. File
    fname = os.getenv("SCAN_TICKERS_FILE", "")
    if fname and os.path.exists(fname):
        logger.info(f"Loading watchlist from {fname}...")
        with open(fname, 'r', encoding='utf-8') as f:
            # Handle CSV or plain text
            lines = [line.strip().split(',')[0] for line in f if line.strip() and not line.startswith("#")]
        return list(dict.fromkeys(lines)) # Dedupe

    # 2. Env Var
    env_tickers = os.getenv("SCAN_TICKERS", "")
    if env_tickers:
        logger.info("Loading watchlist from SCAN_TICKERS env var...")
        return [t.strip() for t in env_tickers.split(",") if t.strip()]

    # 3. Defaults
    logger.info("Using default KSA + Global watchlist...")
    return DEFAULT_KSA + DEFAULT_GLOBAL

def format_currency(val: float | None, symbol: str) -> str:
    if val is None: return "-"
    return f"{val:,.2f} {symbol}"

def format_pct(val: float | None) -> str:
    if val is None: return "-"
    prefix = "+" if val > 0 else ""
    return f"{prefix}{val:.2f}%"

def get_badge(reco: str | None) -> str:
    r = (reco or "").upper()
    if r == "STRONG BUY": return "🔥 STRONG BUY"
    if r == "BUY": return "✅ BUY"
    if r == "SELL": return "🛑 SELL"
    if r == "REDUCE": return "⚠️ REDUCE"
    return "⏸ HOLD"

# =============================================================================
# MAIN LOGIC
# =============================================================================

async def main():
    start_time = time.time()
    
    # 1. Configuration
    tickers = load_watchlist()
    top_n = int(os.getenv("SCAN_TOP_N", "25"))
    min_score = float(os.getenv("SCAN_MIN_OPP_SCORE", "0"))
    
    print("\n" + "="*60)
    print(f" 🚀 MARKET SCANNER v2.0")
    print(f" 📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f" 🎯 Targets: {len(tickers)} symbols")
    print(f" 🧠 Engine: Core V2 + AI Scoring")
    print("="*60 + "\n")

    # 2. Initialize Engine
    engine = DataEngine()
    
    # 3. Fetch Data
    logger.info("Fetching data... (this may take a moment)")
    unified_quotes = await engine.get_enriched_quotes(tickers)
    
    # 4. Enrich & Score
    analyzed_data: List[EnrichedQuote] = []
    
    for uq in unified_quotes:
        # Convert to Enriched schema
        eq = EnrichedQuote.from_unified(uq)
        # Apply Scoring Engine logic (Value, Quality, Momentum)
        scored = enrich_with_scores(eq)
        analyzed_data.append(scored)

    # 5. Sort by Opportunity Score
    ranked = sorted(
        analyzed_data, 
        key=lambda x: (x.opportunity_score or 0, x.data_quality != "MISSING"), 
        reverse=True
    )

    valid_results = [x for x in ranked if x.data_quality != "MISSING"]
    missing_results = [x for x in ranked if x.data_quality == "MISSING"]

    # 6. Display Report
    print(f"\n🏆 TOP OPPORTUNITIES (Score >= {min_score})")
    print("-" * 80)
    
    display_rows = []
    for q in valid_results:
        if (q.opportunity_score or 0) < min_score: continue
        
        display_rows.append([
            q.symbol,
            format_currency(q.current_price, q.currency or ""),
            format_pct(q.percent_change),
            f"{q.opportunity_score or 0:.1f}",
            get_badge(q.recommendation),
            format_pct(q.upside_percent),
            f"{q.value_score or 0:.0f}/{q.quality_score or 0:.0f}/{q.momentum_score or 0:.0f}"
        ])
    
    # Limit display
    final_display = display_rows[:top_n]

    if HAS_TABULATE:
        headers = ["Symbol", "Price", "Change", "Opp Score", "Recommendation", "Upside", "V/Q/M Scores"]
        print(tabulate(final_display, headers=headers, tablefmt="simple_grid"))
    else:
        # Simple fallback
        print(f"{'SYMBOL':<10} {'PRICE':<15} {'SCORE':<6} {'RECO':<12} {'UPSIDE':<8}")
        for row in final_display:
            print(f"{row[0]:<10} {row[1]:<15} {row[3]:<6} {row[4]:<12} {row[5]:<8}")

    # 7. KSA Specific Snapshot
    print(f"\n🇸🇦 KSA SNAPSHOT")
    print("-" * 80)
    ksa_rows = [row for row in display_rows if ".SR" in row[0]]
    if ksa_rows:
        if HAS_TABULATE:
            print(tabulate(ksa_rows[:10], headers=["Symbol", "Price", "Change", "Opp", "Reco", "Upside", "Scores"], tablefmt="simple"))
        else:
            for r in ksa_rows[:5]: print(f"{r[0]}: {r[1]} ({r[2]}) - {r[4]}")
    else:
        print("No KSA data found in valid results.")

    if missing_results:
        print(f"\n⚠️ MISSING DATA: {len(missing_results)} symbols (Check logs/tickers)")
        if len(missing_results) < 10:
            print(", ".join([m.symbol for m in missing_results]))

    # 8. CSV Export
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"scan_results_{timestamp}.csv"
    
    logger.info(f"Exporting full report to {filename}...")
    
    csv_headers = [
        "Symbol", "Name", "Market", "Sector", "Price", "Change %", "Opportunity Score", 
        "Recommendation", "Value Score", "Quality Score", "Momentum Score",
        "Fair Value", "Upside %", "PE (TTM)", "PB", "Div Yield %", "ROE %", 
        "Data Quality", "Error"
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)
        
        for q in ranked:
            writer.writerow([
                q.symbol,
                q.name,
                q.market,
                q.sector,
                q.current_price,
                q.percent_change,
                q.opportunity_score,
                q.recommendation,
                q.value_score,
                q.quality_score,
                q.momentum_score,
                q.fair_value,
                q.upside_percent,
                q.pe_ttm,
                q.pb,
                q.dividend_yield,
                q.roe,
                q.data_quality,
                q.error
            ])

    print(f"\n✅ Done in {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
