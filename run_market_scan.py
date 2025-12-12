"""
run_market_scan.py
===========================================================
TADAWUL FAST BRIDGE – MARKET SCANNER & ANALYZER
Version: 1.2.0 (Engine-safe + Env-safe + Order-safe)
===========================================================

What it does
------------
1) Loads env.py if available (otherwise uses OS env vars).
2) Initializes core.data_engine_v2.DataEngine safely (kwargs if supported).
3) Fetches enriched quotes for a mixed KSA + Global watchlist.
4) Ranks by Opportunity Score and prints a "briefing" report.
5) Exports a full CSV report to ./analysis_report_YYYYMMDD_HHMMSSZ.csv

Usage
-----
python run_market_scan.py

Optional env vars
-----------------
SCAN_TICKERS="1120.SR,2222.SR,AAPL,MSFT"
SCAN_TICKERS_FILE="watchlist.txt"        # one symbol per line OR CSV with "Symbol" column
SCAN_TOP_N="25"
SCAN_MIN_OPP_SCORE="60"
"""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, (os.getenv("LOG_LEVEL", "INFO") or "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("MarketScan")

# Ensure imports from project root
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from core.data_engine_v2 import DataEngine  # type: ignore
except Exception as e:
    logger.error("Import Error: %s. Run this script from the project root.", e)
    raise

# env is optional (script must run even without it)
try:
    import env  # type: ignore
except Exception:
    env = None  # type: ignore


# ---------------------------------------------------------------------
# Default Watchlists (used if SCAN_TICKERS / SCAN_TICKERS_FILE not set)
# ---------------------------------------------------------------------

KSA_WATCHLIST = [
    "1120.SR",  # Al Rajhi
    "1180.SR",  # SNB
    "2222.SR",  # Aramco
    "2010.SR",  # SABIC
    "7010.SR",  # STC
    "4030.SR",  # Bahri
    "7202.SR",  # Solutions
]

GLOBAL_WATCHLIST = [
    "AAPL",
    "NVDA",
    "MSFT",
    "TSLA",
    "AMZN",
    "GOOGL",
    "EURUSD=X",
    "BTC-USD",
]


# ---------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------

def _qget(obj: Any, *names: str, default: Any = None) -> Any:
    """Get attribute/field from pydantic model or dict with fallbacks."""
    if obj is None:
        return default
    for n in names:
        try:
            if hasattr(obj, n):
                v = getattr(obj, n)
                if v is not None:
                    return v
        except Exception:
            pass
        if isinstance(obj, dict) and n in obj and obj[n] is not None:
            return obj[n]
    return default


def _to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()  # type: ignore
        except Exception:
            pass
    if isinstance(obj, dict):
        return obj
    return getattr(obj, "__dict__", {}) or {}


def _normalize_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", ".SR")
    if s.isdigit():
        s = f"{s}.SR"
    return s


def _dedupe_keep_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        xx = _normalize_symbol(x)
        if not xx or xx in seen:
            continue
        seen.add(xx)
        out.append(xx)
    return out


def _read_tickers_file(path: str) -> List[str]:
    path = (path or "").strip()
    if not path:
        return []
    if not os.path.exists(path):
        raise FileNotFoundError(f"SCAN_TICKERS_FILE not found: {path}")

    # If CSV with header including "Symbol" or "Ticker"
    if path.lower().endswith(".csv"):
        out: List[str] = []
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = row.get("Symbol") or row.get("Ticker") or row.get("symbol") or row.get("ticker") or ""
                sym = _normalize_symbol(sym)
                if sym:
                    out.append(sym)
        return out

    # Otherwise: one symbol per line (ignore empty/#comment)
    out2: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out2.append(_normalize_symbol(line))
    return out2


def _split_markets(symbols: Sequence[str]) -> Tuple[List[str], List[str]]:
    ksa: List[str] = []
    glob: List[str] = []
    for s in symbols:
        (ksa if s.endswith(".SR") else glob).append(s)
    return ksa, glob


def _fmt_money(val: Any, ccy: str) -> str:
    try:
        if val is None:
            return "-"
        v = float(val)
        return f"{v:,.2f} {ccy or ''}".strip()
    except Exception:
        return "-"


def _fmt_pct(val: Any) -> str:
    try:
        if val is None:
            return "-"
        v = float(val)
        return f"{v:+.2f}%"
    except Exception:
        return "-"


def _badge_reco(reco: str) -> str:
    r = (reco or "").strip().upper()
    if r == "STRONG_BUY":
        return "🔥 STRONG_BUY"
    if r == "BUY":
        return "✅ BUY"
    if r == "SELL":
        return "🛑 SELL"
    if r == "HOLD":
        return "⏸ HOLD"
    return r or "-"


def _print_header(title: str) -> None:
    print("\n" + "=" * 78)
    print(f" {title}")
    print("=" * 78)


# ---------------------------------------------------------------------
# Engine initialization (kwargs-safe)
# ---------------------------------------------------------------------

def _env_get(name: str, default: Any = None) -> Any:
    if env is not None and hasattr(env, name):
        try:
            return getattr(env, name)
        except Exception:
            return default
    return os.getenv(name, default)


def _build_engine() -> DataEngine:
    kwargs: Dict[str, Any] = {}

    enabled = _env_get("ENABLED_PROVIDERS", None)
    if isinstance(enabled, (list, tuple)) and enabled:
        kwargs["enabled_providers"] = list(enabled)
    elif isinstance(enabled, str) and enabled.strip():
        kwargs["enabled_providers"] = [x.strip() for x in enabled.split(",") if x.strip()]

    # Optional knobs (only passed if present)
    for k_env, k_arg in [
        ("ENGINE_CACHE_TTL_SECONDS", "cache_ttl"),
        ("DATAENGINE_CACHE_TTL", "cache_ttl"),
        ("ENGINE_PROVIDER_TIMEOUT_SECONDS", "provider_timeout"),
        ("DATAENGINE_TIMEOUT", "provider_timeout"),
        ("ENGINE_ENABLE_ADVANCED_ANALYSIS", "enable_advanced_analysis"),
        ("ENABLE_ADVANCED_ANALYSIS", "enable_advanced_analysis"),
    ]:
        v = _env_get(k_env, None)
        if v is not None and k_arg not in kwargs:
            kwargs[k_arg] = v

    try:
        return DataEngine(**kwargs) if kwargs else DataEngine()
    except TypeError:
        # Signature mismatch -> fall back to default ctor
        return DataEngine()


# ---------------------------------------------------------------------
# Ticker source selection
# ---------------------------------------------------------------------

def _load_watchlist() -> List[str]:
    raw_inline = (os.getenv("SCAN_TICKERS", "") or "").strip()
    raw_file = (os.getenv("SCAN_TICKERS_FILE", "") or "").strip()

    if raw_file:
        return _dedupe_keep_order(_read_tickers_file(raw_file))

    if raw_inline:
        return _dedupe_keep_order([x.strip() for x in raw_inline.split(",") if x.strip()])

    return _dedupe_keep_order(KSA_WATCHLIST + GLOBAL_WATCHLIST)


# ---------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------

async def run_analysis() -> None:
    app_env = str(_env_get("APP_ENV", "production") or "production")
    base_url = str(_env_get("BACKEND_BASE_URL", "") or "")
    providers = _env_get("ENABLED_PROVIDERS", None)

    tickers = _load_watchlist()
    ksa_syms, global_syms = _split_markets(tickers)

    top_n = int(os.getenv("SCAN_TOP_N", "25") or 25)
    min_opp = float(os.getenv("SCAN_MIN_OPP_SCORE", "60") or 60)

    _print_header(f"🚀 MARKET SCAN (env={app_env})")
    print(f"Tickers: {len(tickers)} | KSA: {len(ksa_syms)} | Global: {len(global_syms)}")
    print(f"BACKEND_BASE_URL: {base_url or '-'}")
    print(f"ENABLED_PROVIDERS: {providers or '-'}")

    logger.info("Initializing DataEngine v2...")
    engine = _build_engine()

    logger.info("Fetching enriched quotes for %d symbols...", len(tickers))
    t0 = datetime.now(timezone.utc)

    quotes = await engine.get_enriched_quotes(tickers)  # expected: list aligned with input
    dt = (datetime.now(timezone.utc) - t0).total_seconds()
    logger.info("Fetch complete in %.2fs.", dt)

    # Normalize into dicts for stable handling
    qdicts: List[Dict[str, Any]] = []
    for q in quotes or []:
        d = _to_dict(q)
        if not d:
            d = {"data_quality": "MISSING", "error": "Empty quote object returned"}
        # enforce symbol
        d["symbol"] = _normalize_symbol(str(d.get("symbol") or d.get("ticker") or ""))
        qdicts.append(d)

    # Identify missing
    def _is_missing(d: Dict[str, Any]) -> bool:
        dq = str(d.get("data_quality") or d.get("data_quality_level") or "MISSING").upper()
        return dq == "MISSING" or bool(d.get("error")) and dq in ("MISSING", "ERROR")

    valid = [d for d in qdicts if not _is_missing(d)]
    missing = [d for d in qdicts if _is_missing(d)]

    # Rank by opportunity score
    def _opp(d: Dict[str, Any]) -> float:
        try:
            v = d.get("opportunity_score")
            return float(v) if v is not None else 0.0
        except Exception:
            return 0.0

    ranked = sorted(valid, key=_opp, reverse=True)

    # ---------------------------------------------------------
    # REPORT A: TOP OPPORTUNITIES
    # ---------------------------------------------------------
    _print_header(f"🏆 TOP OPPORTUNITIES (OppScore >= {min_opp:.0f})")
    print(f"{'SYMBOL':<12} | {'PRICE':<18} | {'OPP':>6} | {'RECO':<14} | {'UPSIDE':>8} | {'QUALITY':<10}")
    print("-" * 78)

    shown = 0
    for d in ranked:
        opp = _opp(d)
        if opp < min_opp:
            continue

        sym = str(d.get("symbol") or "-")
        ccy = str(d.get("currency") or "")
        price = d.get("last_price", d.get("price"))
        reco = str(d.get("recommendation") or "")
        upside = d.get("upside_percent")

        dq = str(d.get("data_quality") or "OK")
        print(
            f"{sym:<12} | "
            f"{_fmt_money(price, ccy):<18} | "
            f"{opp:>6.1f} | "
            f"{_badge_reco(reco):<14} | "
            f"{(_fmt_pct(upside) if upside is not None else '-'):>8} | "
            f"{dq:<10}"
        )

        shown += 1
        if shown >= top_n:
            break

    if shown == 0:
        print("No symbols met the threshold. Try lowering SCAN_MIN_OPP_SCORE.")

    # ---------------------------------------------------------
    # REPORT B: KSA SNAPSHOT
    # ---------------------------------------------------------
    _print_header("🇸🇦 KSA TADAWUL SNAPSHOT")
    ksa_ranked = [d for d in ranked if str(d.get("symbol") or "").endswith(".SR") or str(d.get("market") or "").upper() == "KSA"]
    if not ksa_ranked:
        print("No KSA data returned. (Check Argaam/Tadawul gateway + enabled providers.)")
    else:
        print(f"{'SYMBOL':<12} | {'LAST':>12} | {'CHG%':>8} | {'VAL_LABEL':<14} | {'OPP':>6}")
        print("-" * 78)
        for d in ksa_ranked[: min(25, len(ksa_ranked))]:
            sym = str(d.get("symbol") or "-")
            last_price = d.get("last_price")
            chg = d.get("change_percent")
            val_label = str(d.get("valuation_label") or "-")
            opp = _opp(d)
            print(f"{sym:<12} | {str(last_price or '-'):>12} | {_fmt_pct(chg):>8} | {val_label:<14} | {opp:>6.1f}")

    # ---------------------------------------------------------
    # REPORT C: MISSING / ERRORS
    # ---------------------------------------------------------
    if missing:
        _print_header("⚠️ MISSING / ERROR SYMBOLS")
        for d in missing:
            sym = str(d.get("symbol") or d.get("ticker") or "-")
            err = str(d.get("error") or "MISSING")
            dq = str(d.get("data_quality") or "MISSING")
            print(f"- {sym} | {dq} | {err}")

    # ---------------------------------------------------------
    # EXPORT CSV
    # ---------------------------------------------------------
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    filename = f"analysis_report_{stamp}.csv"
    logger.info("Exporting CSV: %s", filename)

    columns = [
        "Symbol",
        "Name",
        "Market",
        "Currency",
        "Last Price",
        "Previous Close",
        "Change",
        "Change %",
        "52W High",
        "52W Low",
        "52W Position %",
        "Fair Value",
        "Upside %",
        "Value Score",
        "Quality Score",
        "Momentum Score",
        "Opportunity Score",
        "Recommendation",
        "Valuation Label",
        "RSI 14",
        "Volatility 30D",
        "Market Cap",
        "P/E (TTM)",
        "P/B",
        "Dividend Yield",
        "ROE",
        "Net Margin",
        "Data Quality",
        "Data Source",
        "Error",
    ]

    def _row(d: Dict[str, Any]) -> List[Any]:
        return [
            d.get("symbol"),
            d.get("name"),
            d.get("market") or d.get("market_region"),
            d.get("currency"),
            d.get("last_price") or d.get("price"),
            d.get("previous_close"),
            d.get("change"),
            d.get("change_percent"),
            d.get("high_52w") or d.get("fifty_two_week_high"),
            d.get("low_52w") or d.get("fifty_two_week_low"),
            d.get("position_52w_percent") or d.get("fifty_two_week_position_pct"),
            d.get("fair_value"),
            d.get("upside_percent"),
            d.get("value_score"),
            d.get("quality_score"),
            d.get("momentum_score"),
            d.get("opportunity_score"),
            d.get("recommendation"),
            d.get("valuation_label"),
            d.get("rsi_14"),
            d.get("volatility_30d") or d.get("volatility_30d_percent"),
            d.get("market_cap"),
            d.get("pe_ttm") or d.get("pe_ratio"),
            d.get("pb") or d.get("pb_ratio"),
            d.get("dividend_yield") or d.get("dividend_yield_percent"),
            d.get("roe") or d.get("roe_percent"),
            d.get("net_margin_percent") or d.get("profit_margin"),
            d.get("data_quality"),
            d.get("data_source"),
            d.get("error"),
        ]

    # Export ranked (valid first) then missing
    export_rows = ranked + missing

    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for d in export_rows:
            w.writerow(_row(d))

    print(f"\n✅ Analysis Complete. CSV saved: {filename}\n")


if __name__ == "__main__":
    # Windows safety
    if os.name == "nt":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        except Exception:
            pass

    asyncio.run(run_analysis())
