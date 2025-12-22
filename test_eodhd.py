# test_eodhd.py
"""
EODHD Data Quality Test Script – Enhanced (Aligned with FinanceEngine v2.0.0)
----------------------------------------------------------------------------
FULL UPDATED SCRIPT (ONE-SHOT) – v2.0.0

What this fixes/improves:
✅ Uses env var EODHD_API_TOKEN (fallback demo) instead of hardcoding
✅ Uses requests + retries + timeouts (avoids random network errors)
✅ Validates response shape and prints clean diagnostics
✅ Builds a clean Pandas DataFrame with proper dtypes
✅ Adds data quality report: missing %, duplicates, gaps, outliers, volume checks
✅ Optional: saves CSV to ./out/<ticker>_eod.csv

Why not use eodhd SDK here?
- Your main app/FinanceEngine uses direct REST (requests).
- This script matches that approach for consistent behavior and easier debugging.

Usage examples:
  python test_eodhd.py
  python test_eodhd.py --symbol AAPL --from 2024-01-01 --period d --save
  python test_eodhd.py --symbol GOOGL.US --from 2023-01-01 --save

Env:
  EODHD_API_TOKEN=xxxxxx
"""

from __future__ import annotations

import os
import sys
import math
import json
import argparse
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =============================================================================
# Config / HTTP
# =============================================================================

BASE_URL = "https://eodhd.com/api"
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
API_TOKEN = os.getenv("EODHD_API_TOKEN", "demo")


def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if math.isnan(v):
                return None
            return v
        s = str(x).strip().replace(",", "")
        if not s or s.lower() in {"na", "n/a", "none", "null", "-"}:
            return None
        return float(s)
    except Exception:
        return None


def to_date(s: str) -> Optional[pd.Timestamp]:
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return None


def fetch_json(session: requests.Session, url: str) -> Any:
    r = session.get(url, timeout=DEFAULT_TIMEOUT)
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Non-JSON response ({r.status_code}): {r.text[:200]}")

    # EODHD sometimes returns dict error payload
    if r.status_code >= 400:
        msg = data.get("message") if isinstance(data, dict) else str(data)
        raise RuntimeError(f"EODHD HTTP {r.status_code}: {msg}")

    if isinstance(data, dict) and ("code" in data and "message" in data):
        raise RuntimeError(f"EODHD error: {data.get('message')}")

    return data


# =============================================================================
# Fetchers
# =============================================================================

def fetch_eod(symbol: str, from_date: str, period: str = "d") -> List[Dict[str, Any]]:
    """
    Fetch EOD (end-of-day) prices.
    period param supported by EODHD on some endpoints; /eod uses daily by default.
    We'll keep period arg for compatibility but still call /eod.
    """
    s = make_session()
    url = f"{BASE_URL}/eod/{symbol}?api_token={API_TOKEN}&fmt=json&from={from_date}"
    data = fetch_json(s, url)

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected EOD shape: {type(data)} -> {str(data)[:200]}")
    return data


def fetch_fundamentals(symbol: str) -> Dict[str, Any]:
    s = make_session()
    url = f"{BASE_URL}/fundamentals/{symbol}?api_token={API_TOKEN}&fmt=json"
    data = fetch_json(s, url)
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected fundamentals shape: {type(data)} -> {str(data)[:200]}")
    return data


# =============================================================================
# DataFrame + Quality
# =============================================================================

def build_df(eod: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(eod or [])
    if df.empty:
        return df

    # Normalize column names that sometimes appear
    # Expected: date, open, high, low, close, adjusted_close, volume
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        df = df.drop_duplicates(subset=["date"], keep="last")
        df = df.set_index("date")

    for c in ["open", "high", "low", "close", "adjusted_close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Prefer adjusted_close if close missing
    if "close" in df.columns and "adjusted_close" in df.columns:
        df["close"] = df["close"].fillna(df["adjusted_close"])

    # Basic sanity bounds
    for c in ["open", "high", "low", "close", "adjusted_close"]:
        if c in df.columns:
            df.loc[df[c] <= 0, c] = pd.NA

    if "volume" in df.columns:
        df.loc[df["volume"] < 0, "volume"] = pd.NA

    return df


def quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"status": "EMPTY", "rows": 0}

    report: Dict[str, Any] = {}
    report["status"] = "OK"
    report["rows"] = int(len(df))
    report["start"] = str(df.index.min().date()) if df.index.notna().any() else None
    report["end"] = str(df.index.max().date()) if df.index.notna().any() else None

    # Missing percentages
    miss = df.isna().mean().to_dict()
    report["missing_pct"] = {k: round(v * 100, 2) for k, v in miss.items()}

    # Duplicate index check (should be none after build_df)
    report["duplicate_dates"] = int(df.index.duplicated().sum())

    # Gaps in business days
    idx = df.index
    if len(idx) >= 2:
        # Build expected business days between min/max and compare
        expected = pd.bdate_range(idx.min(), idx.max())
        missing_dates = expected.difference(idx)
        report["missing_business_days_count"] = int(len(missing_dates))
        report["missing_business_days_sample"] = [d.strftime("%Y-%m-%d") for d in missing_dates[:10]]
    else:
        report["missing_business_days_count"] = 0
        report["missing_business_days_sample"] = []

    # Outlier checks (returns)
    if "close" in df.columns:
        closes = df["close"].dropna()
        if len(closes) >= 10:
            rets = closes.pct_change().dropna()
            report["return_min_pct"] = round(float(rets.min() * 100), 2)
            report["return_max_pct"] = round(float(rets.max() * 100), 2)
            report["vol_30d_ann_pct"] = None
            if len(rets) >= 10:
                vol = rets.tail(30).std(ddof=1) if len(rets) >= 30 else rets.std(ddof=1)
                if pd.notna(vol):
                    report["vol_30d_ann_pct"] = round(float(vol * math.sqrt(252) * 100), 2)

            # flag extreme returns
            extreme = rets[rets.abs() > 0.25]  # >25% daily move
            report["extreme_return_days_count"] = int(len(extreme))
            report["extreme_return_days_sample"] = [
                f"{d.strftime('%Y-%m-%d')}:{round(v*100,2)}%"
                for d, v in extreme.head(10).items()
            ]
        else:
            report["return_min_pct"] = None
            report["return_max_pct"] = None
            report["vol_30d_ann_pct"] = None
            report["extreme_return_days_count"] = 0
            report["extreme_return_days_sample"] = []
    return report


def print_summary(symbol: str, df: pd.DataFrame, fundamentals: Optional[Dict[str, Any]], report: Dict[str, Any]) -> None:
    print("\n" + "=" * 80)
    print(f"EODHD DATA QUALITY REPORT | {symbol}")
    print("=" * 80)

    if df is None or df.empty:
        print("No EOD data returned.")
        return

    cols = list(df.columns)
    print(f"Rows: {len(df)} | Columns: {cols}")
    print(f"Date range: {report.get('start')}  ->  {report.get('end')}")
    print("\nHead (first 5 rows):")
    print(df.head(5))
    print("\nTail (last 5 rows):")
    print(df.tail(5))

    print("\nMissing % by column:")
    for k, v in (report.get("missing_pct") or {}).items():
        print(f"  - {k}: {v}%")

    print("\nGaps:")
    print(f"  Missing business days count: {report.get('missing_business_days_count')}")
    sample = report.get("missing_business_days_sample") or []
    if sample:
        print(f"  Sample missing days: {', '.join(sample)}")

    if report.get("vol_30d_ann_pct") is not None:
        print("\nRisk:")
        print(f"  Volatility 30D (annualized): {report.get('vol_30d_ann_pct')}%")
        print(f"  Return min/max: {report.get('return_min_pct')}% / {report.get('return_max_pct')}%")

    if report.get("extreme_return_days_count"):
        print("\n⚠️ Extreme daily moves (>25%):")
        for s in report.get("extreme_return_days_sample", []):
            print(f"  - {s}")

    if fundamentals:
        gen = fundamentals.get("General", {}) if isinstance(fundamentals.get("General", {}), dict) else {}
        val = fundamentals.get("Valuation", {}) if isinstance(fundamentals.get("Valuation", {}), dict) else {}
        hi = fundamentals.get("Highlights", {}) if isinstance(fundamentals.get("Highlights", {}), dict) else {}
        print("\nFundamentals snapshot:")
        print(f"  Name: {gen.get('Name')}")
        print(f"  Sector: {gen.get('Sector')} | Industry: {gen.get('Industry')}")
        print(f"  Currency: {gen.get('CurrencyCode')}")
        print(f"  Market Cap: {val.get('MarketCapitalization') or hi.get('MarketCapitalization')}")
        print(f"  Trailing PE: {val.get('TrailingPE')}")
        print(f"  Price/Book: {val.get('PriceBookMRQ')}")

    print("=" * 80 + "\n")


# =============================================================================
# Main
# =============================================================================

def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser(description="EODHD EOD + Fundamentals data quality tester")
    p.add_argument("--symbol", default="AAPL.US", help="Ticker symbol, e.g. AAPL.US, GOOGL.US, TSLA.US")
    p.add_argument("--from", dest="from_date", default="2024-01-01", help="Start date YYYY-MM-DD")
    p.add_argument("--period", default="d", choices=["d", "w", "m"], help="Interval (kept for compatibility)")
    p.add_argument("--no-fundamentals", action="store_true", help="Skip fundamentals fetch")
    p.add_argument("--save", action="store_true", help="Save CSV to ./out/")
    args = p.parse_args(argv)

    symbol = args.symbol.strip().upper()
    from_date = args.from_date.strip()

    # Basic date validation
    try:
        _ = dt.datetime.strptime(from_date, "%Y-%m-%d")
    except Exception:
        print("ERROR: --from must be YYYY-MM-DD")
        return 2

    print(f"Using token: {'demo' if API_TOKEN == 'demo' else '***'}")
    print(f"Fetching EOD for {symbol} from {from_date} ...")

    try:
        eod = fetch_eod(symbol, from_date=from_date, period=args.period)
    except Exception as e:
        print(f"\nERROR fetching EOD: {e}")
        return 1

    df = build_df(eod)
    rep = quality_report(df)

    fundamentals = None
    if not args.no_fundamentals:
        try:
            fundamentals = fetch_fundamentals(symbol)
        except Exception as e:
            print(f"\nWARNING: fundamentals failed: {e}")
            fundamentals = None

    print_summary(symbol, df, fundamentals, rep)

    if args.save and df is not None and not df.empty:
        os.makedirs("out", exist_ok=True)
        out_path = os.path.join("out", f"{symbol.replace('.', '_')}_eod.csv")
        df.to_csv(out_path)
        print(f"Saved CSV: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
