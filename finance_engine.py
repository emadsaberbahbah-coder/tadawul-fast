# finance_engine.py
"""
FinanceEngine – Hardened + Efficient (EODHD + Fallback History + Forecast + Risk + Shariah)
------------------------------------------------------------------------------------------
FULL UPDATED SCRIPT (ONE-SHOT) – v2.2.0

Goals:
✅ Reduce failures (especially 401/403 Non-JSON) with clearer error handling
✅ Efficiency: shared requests.Session, retries, short timeouts, optional caching
✅ Symbol normalization:
   - AAPL -> AAPL.US
   - AAPL.US stays
   - 1120 -> 1120.SR
   - 1120.SR stays
✅ If EODHD blocks a GLOBAL ticker (403/401), fallback to STOOQ for price history
   (still returns technicals + forecasting; fundamentals may be limited if EODHD blocked)
✅ Injects derived Technicals from history into fundamentals
✅ Forecasting:
   - Prophet if available + enabled + enough data
   - Otherwise fast linear fallback trend
✅ Shariah: returns YES/NO/UNKNOWN (UNKNOWN if insufficient financials)
✅ Score: 0–100 (value + income + momentum + risk)

Public API (used by app.py):
  get_stock_data(ticker) -> (fundamentals_dict, hist_list_of_dicts)
  run_ai_forecasting(hist_data) -> dict
  check_shariah_compliance(fundamentals) -> dict
  generate_score(fundamentals, ai_data) -> float
"""

from __future__ import annotations

import os
import io
import math
import datetime as dt
from typing import Any, Dict, Optional, Tuple, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Prophet is optional (fallback will work if missing or disabled)
try:
    from prophet import Prophet  # type: ignore
    _PROPHET_INSTALLED = True
except Exception:
    Prophet = None  # type: ignore
    _PROPHET_INSTALLED = False


# =============================================================================
# Helpers
# =============================================================================

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if math.isnan(v):
                return None
            return v
        s = str(x).strip().replace(",", "")
        if not s or s.lower() in {"na", "n/a", "none", "null", "-"}:
            return None
        if s.endswith("%"):
            return float(s[:-1].strip())
        return float(s)
    except Exception:
        return None


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """(a-b)/b * 100"""
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b * 100.0


def _latest_period_key(period_dict: Dict[str, Any]) -> Optional[str]:
    """Return most recent key like '2025-09-30'."""
    if not isinstance(period_dict, dict) or not period_dict:
        return None

    def _to_date(k: str) -> dt.date:
        try:
            return dt.datetime.strptime(k[:10], "%Y-%m-%d").date()
        except Exception:
            return dt.date(1900, 1, 1)

    keys = list(period_dict.keys())
    keys.sort(key=_to_date, reverse=True)
    return keys[0] if keys else None


def _extract_hist_df(hist_data: Any) -> pd.DataFrame:
    """
    Expects list of dict:
      {date, open, high, low, close, adjusted_close, volume}
    (works for EODHD and our STOOQ fallback after normalization)
    """
    df = pd.DataFrame(hist_data or [])
    if df.empty:
        return df

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
    else:
        df["date"] = pd.NaT

    for c in ["open", "high", "low", "close", "adjusted_close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "close" in df.columns and "adjusted_close" in df.columns:
        df["close"] = df["close"].fillna(df["adjusted_close"])

    return df


def _compute_technicals_from_hist(df: pd.DataFrame) -> Dict[str, Any]:
    """Compute basic technicals (fast) from history dataframe."""
    if df is None or df.empty or "close" not in df.columns:
        return {}

    df2 = df.dropna(subset=["close"]).copy()
    if df2.empty:
        return {}

    last = df2.iloc[-1]
    prev = df2.iloc[-2] if len(df2) >= 2 else None

    current_price = float(last["close"]) if pd.notna(last["close"]) else None
    previous_close = float(prev["close"]) if prev is not None and pd.notna(prev["close"]) else None

    change = (current_price - previous_close) if current_price is not None and previous_close is not None else None
    change_pct = _pct(current_price, previous_close)

    day_high = float(last["high"]) if "high" in df2.columns and pd.notna(last.get("high")) else current_price
    day_low = float(last["low"]) if "low" in df2.columns and pd.notna(last.get("low")) else current_price

    window_52w = df2.tail(252)
    high_52w = float(window_52w["close"].max()) if not window_52w.empty else None
    low_52w = float(window_52w["close"].min()) if not window_52w.empty else None

    vol = float(last["volume"]) if "volume" in df2.columns and pd.notna(last.get("volume")) else None
    avg_vol_30d = float(df2["volume"].tail(30).mean()) if "volume" in df2.columns and len(df2["volume"].dropna()) else None

    # Volatility (30D annualized)
    rets = df2["close"].pct_change().dropna()
    vol_30d_ann = None
    if len(rets) >= 10:
        vol_30d = rets.tail(30).std(ddof=1) if len(rets) >= 30 else rets.std(ddof=1)
        if pd.notna(vol_30d):
            vol_30d_ann = float(vol_30d) * math.sqrt(252)

    # Max drawdown (90D)
    max_dd_90d = None
    closes_90 = df2["close"].tail(90)
    if len(closes_90) >= 10:
        peak = float(closes_90.iloc[0])
        dd_min = 0.0
        for p in closes_90:
            p = float(p)
            peak = max(peak, p)
            dd = (p / peak) - 1.0
            dd_min = min(dd_min, dd)
        max_dd_90d = float(dd_min)

    return {
        "Price": current_price,
        "PreviousClose": previous_close,
        "Change": change,
        "ChangePercent": change_pct,
        "DayHigh": day_high,
        "DayLow": day_low,
        "52WeekHigh": high_52w,
        "52WeekLow": low_52w,
        "Volume": vol,
        "AvgVolume30D": avg_vol_30d,
        "Volatility30D_Ann": vol_30d_ann,
        "MaxDrawdown90D": max_dd_90d,
        "AsOfDate": last["date"].strftime("%Y-%m-%d") if pd.notna(last.get("date")) else None,
    }


def _linear_fallback_forecast(df: pd.DataFrame, horizon_days: int) -> Tuple[Optional[float], Optional[float]]:
    """Very fast fallback forecast: linear regression on close vs day index."""
    try:
        df2 = df.dropna(subset=["date", "close"]).tail(365).copy()
        if df2.empty or len(df2) < 30:
            return None, None

        y = df2["close"].values.astype(float)
        x = (df2["date"] - df2["date"].min()).dt.days.values.astype(float)

        x_mean, y_mean = x.mean(), y.mean()
        denom = ((x - x_mean) ** 2).sum()
        if denom == 0:
            return float(y[-1]), 40.0

        slope = ((x - x_mean) * (y - y_mean)).sum() / denom
        intercept = y_mean - slope * x_mean

        future_x = x.max() + float(horizon_days)
        pred = intercept + slope * future_x

        # Confidence from relative residual error
        y_hat = intercept + slope * x
        resid = y - y_hat
        resid_std = float(resid.std()) if len(resid) > 5 else 0.0
        level = float(y[-1]) if y[-1] else 1.0
        rel_err = min(1.0, resid_std / max(1e-6, abs(level)))
        conf = max(10.0, min(90.0, 100.0 * (1.0 - rel_err)))

        return float(pred), float(conf)
    except Exception:
        return None, None


def _nearest_pred(forecast: pd.DataFrame, target_date: pd.Timestamp) -> Optional[float]:
    """Pick yhat for exact ds if exists else nearest date."""
    try:
        if forecast.empty or "ds" not in forecast.columns or "yhat" not in forecast.columns:
            return None
        exact = forecast.loc[forecast["ds"] == target_date]
        if not exact.empty:
            return float(exact.iloc[0]["yhat"])
        idx = (forecast["ds"] - target_date).abs().idxmin()
        return float(forecast.loc[idx, "yhat"])
    except Exception:
        return None


# =============================================================================
# Finance Engine
# =============================================================================

class FinanceEngine:
    def __init__(self, api_key: str):
        self.api_key = (api_key or "").strip()
        self.base_url = "https://eodhd.com/api"

        self.timeout = float(os.getenv("HTTP_TIMEOUT", "20"))
        self.hist_years = int(os.getenv("HIST_YEARS", "2"))
        self.enable_prophet = str(os.getenv("ENABLE_PROPHET", "true")).strip().lower() in {"1", "true", "yes", "y"}

        # If your EODHD plan blocks fundamentals, set EODHD_SKIP_FUNDAMENTALS=true
        self.skip_fundamentals = str(os.getenv("EODHD_SKIP_FUNDAMENTALS", "false")).strip().lower() in {"1", "true", "yes", "y"}

        # Session + retries
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "EmadBahbah-FinEngine/2.2.0",
            "Accept": "application/json,text/plain,*/*",
        })
        retries = Retry(
            total=3,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # small in-memory cache (optional)
        self._cache: Dict[str, Tuple[float, Any]] = {}
        self.cache_ttl_sec = float(os.getenv("HTTP_CACHE_TTL_SEC", "0"))  # 0 disables

    # -------------------------------------------------------------------------
    # Symbol normalization
    # -------------------------------------------------------------------------
    def _normalize_symbol(self, ticker: str) -> str:
        """
        Normalize for EODHD:
          - AAPL -> AAPL.US
          - AAPL.US stays
          - 1120 -> 1120.SR
          - 1120.SR stays
        """
        t = (ticker or "").strip().upper()
        if not t:
            return t

        if t.isdigit():
            return f"{t}.SR"

        if "." in t:
            return t

        return f"{t}.US"

    def _is_ksa(self, symbol: str) -> bool:
        s = (symbol or "").strip().upper()
        return s.endswith(".SR") or s.replace(".", "").isdigit()

    # -------------------------------------------------------------------------
    # HTTP helpers
    # -------------------------------------------------------------------------
    def _cache_get(self, key: str) -> Optional[Any]:
        if self.cache_ttl_sec <= 0:
            return None
        hit = self._cache.get(key)
        if not hit:
            return None
        ts, data = hit
        if (dt.datetime.utcnow().timestamp() - ts) <= self.cache_ttl_sec:
            return data
        self._cache.pop(key, None)
        return None

    def _cache_set(self, key: str, data: Any) -> None:
        if self.cache_ttl_sec <= 0:
            return
        self._cache[key] = (dt.datetime.utcnow().timestamp(), data)

    def _get_json(self, url: str) -> Any:
        cached = self._cache_get(url)
        if cached is not None:
            return cached

        r = self.session.get(url, timeout=self.timeout)
        ct = (r.headers.get("Content-Type") or "").lower()
        text_snip = (r.text or "")[:240].replace("\n", " ").strip()

        # Handle non-json replies (common with 401/403)
        if "json" not in ct:
            if r.status_code in (401, 403):
                raise RuntimeError(
                    f"EODHD {r.status_code} (Non-JSON). Token/plan likely blocked. "
                    f"URL={url.split('?')[0]} | Body='{text_snip}'"
                )
            raise RuntimeError(f"Non-JSON response ({r.status_code}) from EODHD. Body='{text_snip}'")

        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"Invalid JSON ({r.status_code}) from EODHD. Body='{text_snip}'")

        # HTTP error
        if r.status_code >= 400:
            msg = None
            if isinstance(data, dict):
                msg = data.get("message") or data.get("error") or str(data)
            raise RuntimeError(f"EODHD HTTP {r.status_code}: {msg or 'request failed'}")

        # Sometimes EODHD returns {"code":..., "message":...} with 200
        if isinstance(data, dict) and ("code" in data and "message" in data):
            raise RuntimeError(f"EODHD error: {data.get('message')}")

        self._cache_set(url, data)
        return data

    # -------------------------------------------------------------------------
    # EODHD fetchers
    # -------------------------------------------------------------------------
    def _fetch_eodhd_fundamentals(self, symbol: str) -> Dict[str, Any]:
        url = f"{self.base_url}/fundamentals/{symbol}?api_token={self.api_key}&fmt=json"
        data = self._get_json(url)
        return data if isinstance(data, dict) else {}

    def _fetch_eodhd_history(self, symbol: str) -> List[Dict[str, Any]]:
        start = dt.date.today() - dt.timedelta(days=365 * self.hist_years)
        url = f"{self.base_url}/eod/{symbol}?api_token={self.api_key}&fmt=json&from={start.isoformat()}"
        data = self._get_json(url)
        return data if isinstance(data, list) else []

    # -------------------------------------------------------------------------
    # STOOQ fallback (GLOBAL only)
    # -------------------------------------------------------------------------
    def _to_stooq_symbol(self, symbol: str) -> Optional[str]:
        """
        STOOQ supports many US tickers with lower-case ".us"
        Example: AAPL.US -> aapl.us
        If not US, return None.
        """
        s = (symbol or "").strip().upper()
        if not s or "." not in s:
            return None
        base, exch = s.rsplit(".", 1)
        if exch != "US":
            return None
        return f"{base.lower()}.us"

    def _fetch_stooq_history(self, symbol: str) -> List[Dict[str, Any]]:
        stooq_sym = self._to_stooq_symbol(symbol)
        if not stooq_sym:
            return []

        # Daily data CSV
        url = f"https://stooq.com/q/d/l/?s={stooq_sym}&i=d"
        r = self.session.get(url, timeout=self.timeout)
        txt = (r.text or "").strip()

        if r.status_code >= 400 or "No data" in txt or len(txt) < 50:
            return []

        df = pd.read_csv(io.StringIO(txt))
        # Expected columns: Date, Open, High, Low, Close, Volume
        if df.empty or "Date" not in df.columns:
            return []

        # Keep last N years
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        cutoff = pd.Timestamp(dt.date.today() - dt.timedelta(days=365 * self.hist_years))
        df = df[df["Date"] >= cutoff]

        out: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            out.append({
                "date": row["Date"].strftime("%Y-%m-%d"),
                "open": _safe_float(row.get("Open")),
                "high": _safe_float(row.get("High")),
                "low": _safe_float(row.get("Low")),
                "close": _safe_float(row.get("Close")),
                "adjusted_close": None,
                "volume": _safe_float(row.get("Volume")),
            })
        return out

    # -------------------------------------------------------------------------
    # Public: get_stock_data
    # -------------------------------------------------------------------------
    def get_stock_data(self, ticker: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Fetch fundamentals + historical EOD.
        If EODHD denies GLOBAL symbol (401/403), fallback to STOOQ history (prices only).
        """
        symbol = self._normalize_symbol(ticker)
        if not symbol:
            return {}, []

        # 1) HISTORY first (because the entire pipeline needs prices)
        hist_data: List[Dict[str, Any]] = []
        hist_source = "EODHD"

        try:
            hist_data = self._fetch_eodhd_history(symbol)
        except Exception as e_hist:
            # fallback only for GLOBAL
            if not self._is_ksa(symbol):
                fb = self._fetch_stooq_history(symbol)
                if fb:
                    hist_data = fb
                    hist_source = "STOOQ"
                else:
                    raise RuntimeError(f"History fetch failed for {symbol}. EODHD error: {e_hist}") from e_hist
            else:
                raise

        # 2) FUNDAMENTALS (optional)
        fund_data: Dict[str, Any] = {}
        fund_source = "EODHD"

        if self.skip_fundamentals:
            fund_source = "SKIPPED"
            fund_data = {}
        else:
            try:
                fund_data = self._fetch_eodhd_fundamentals(symbol)
            except Exception as e_fund:
                # Keep pipeline alive if we still have history (GLOBAL only)
                if hist_source == "STOOQ":
                    fund_data = {}
                    fund_source = "BLOCKED"
                else:
                    # If history is from EODHD but fundamentals blocked, we can still proceed with empty fundamentals
                    fund_data = {}
                    fund_source = f"ERROR:{type(e_fund).__name__}"

        # 3) Inject technicals from history into fundamentals
        df = _extract_hist_df(hist_data)
        tech = _compute_technicals_from_hist(df)

        # Ensure structure
        if not isinstance(fund_data, dict):
            fund_data = {}

        fund_data.setdefault("General", {})
        fund_data.setdefault("Technicals", {})
        fund_data.setdefault("Highlights", {})
        fund_data.setdefault("Valuation", {})
        fund_data.setdefault("SharesStats", {})
        fund_data.setdefault("Financials", {})
        fund_data.setdefault("meta", {})

        if isinstance(fund_data["Technicals"], dict):
            for k, v in tech.items():
                # Always prefer fresh derived technicals
                fund_data["Technicals"][k] = v

        # Attach meta
        if isinstance(fund_data["meta"], dict):
            fund_data["meta"]["data_source"] = f"{fund_source}+{hist_source}"
            fund_data["meta"]["symbol_normalized"] = symbol
            fund_data["meta"]["asof_date"] = tech.get("AsOfDate")
            if hist_source == "STOOQ" and fund_source != "EODHD":
                fund_data["meta"]["note"] = "Fundamentals unavailable (EODHD blocked). Using STOOQ for price history."

        return fund_data, hist_data

    # -------------------------------------------------------------------------
    # Public: Shariah Compliance (approximation)
    # -------------------------------------------------------------------------
    def check_shariah_compliance(self, fundamentals: Dict[str, Any]) -> Dict[str, Any]:
        """
        Approximate Shariah screening:
        - Debt / MarketCap < 33%
        - Cash / MarketCap < 33%

        Returns compliant:
          True / False / None   (None = UNKNOWN due to missing data)
        """
        try:
            fundamentals = fundamentals or {}

            val = fundamentals.get("Valuation", {}) if isinstance(fundamentals.get("Valuation", {}), dict) else {}
            highlights = fundamentals.get("Highlights", {}) if isinstance(fundamentals.get("Highlights", {}), dict) else {}
            fin = fundamentals.get("Financials", {}) if isinstance(fundamentals.get("Financials", {}), dict) else {}

            bs = fin.get("Balance_Sheet", {}) if isinstance(fin.get("Balance_Sheet", {}), dict) else {}
            bsq = bs.get("quarterly", {}) if isinstance(bs.get("quarterly", {}), dict) else {}

            last_q = _latest_period_key(bsq)
            if not last_q:
                return {"compliant": None, "status": "UNKNOWN", "note": "Insufficient Balance Sheet Data"}

            row = bsq.get(last_q, {}) if isinstance(bsq.get(last_q, {}), dict) else {}

            long_debt = _safe_float(row.get("longTermDebt"))
            short_debt = _safe_float(row.get("shortTermDebt"))
            cash = _safe_float(row.get("cash")) or _safe_float(row.get("cashAndCashEquivalents"))

            market_cap = (
                _safe_float(val.get("MarketCapitalization"))
                or _safe_float(highlights.get("MarketCapitalization"))
                or _safe_float(highlights.get("MarketCapitalizationMln"))
            )

            # Convert mln if needed
            if _safe_float(highlights.get("MarketCapitalizationMln")) is not None and market_cap is not None and market_cap < 1e6:
                market_cap = market_cap * 1_000_000

            if market_cap is None or market_cap <= 0:
                return {"compliant": None, "status": "UNKNOWN", "note": "Missing Market Cap", "period": last_q}

            missing_components = []
            if long_debt is None and short_debt is None:
                missing_components.append("Debt")
            if cash is None:
                missing_components.append("Cash")

            total_debt = (long_debt or 0.0) + (short_debt or 0.0)
            cash_v = (cash or 0.0)

            debt_ratio = (total_debt / market_cap) * 100.0
            cash_ratio = (cash_v / market_cap) * 100.0

            if missing_components:
                return {
                    "compliant": None,
                    "status": "UNKNOWN",
                    "note": f"Missing components: {', '.join(missing_components)}",
                    "debt_ratio": round(debt_ratio, 2),
                    "cash_ratio": round(cash_ratio, 2),
                    "period": last_q,
                }

            is_ok = (debt_ratio < 33.0) and (cash_ratio < 33.0)
            return {
                "compliant": bool(is_ok),
                "status": "YES" if is_ok else "NO",
                "debt_ratio": round(debt_ratio, 2),
                "cash_ratio": round(cash_ratio, 2),
                "period": last_q,
            }

        except Exception:
            return {"compliant": None, "status": "UNKNOWN", "note": "Insufficient Data"}

    # -------------------------------------------------------------------------
    # Public: AI Forecasting
    # -------------------------------------------------------------------------
    def run_ai_forecasting(self, hist_data: Any) -> Dict[str, Any]:
        """
        Forecasts future prices (30D/90D).
        - Prophet if installed+enabled and enough history
        - Otherwise linear fallback

        Returns keys expected by app.py:
          current_price, previous_close, price_change, percent_change,
          day_high, day_low, high_52w, low_52w, volume,
          predicted_price_30d, expected_roi_pct,
          predicted_price_90d, expected_roi_90d_pct,
          confidence (0-100), trend, data_source
        """
        df = _extract_hist_df(hist_data)
        if df.empty or "close" not in df.columns:
            return {"error": "Forecasting failed: empty history"}

        df = df.dropna(subset=["date", "close"]).copy()
        if df.empty:
            return {"error": "Forecasting failed: no valid close values"}

        tech = _compute_technicals_from_hist(df)
        current_price = _safe_float(tech.get("Price"))
        if current_price is None:
            return {"error": "Forecasting failed: missing current price"}

        last_date = pd.to_datetime(df.iloc[-1]["date"]).normalize()
        target_30 = (last_date + pd.Timedelta(days=30)).normalize()
        target_90 = (last_date + pd.Timedelta(days=90)).normalize()

        # Prophet path
        if self.enable_prophet and _PROPHET_INSTALLED and Prophet is not None and len(df) >= 160:
            try:
                pdf = df[["date", "close"]].rename(columns={"date": "ds", "close": "y"}).copy()
                pdf["ds"] = pd.to_datetime(pdf["ds"]).dt.normalize()
                pdf["y"] = pd.to_numeric(pdf["y"], errors="coerce")
                pdf = pdf.dropna(subset=["ds", "y"])

                m = Prophet(
                    daily_seasonality=False,
                    weekly_seasonality=True,
                    yearly_seasonality=True,
                    interval_width=0.8,
                    changepoint_prior_scale=0.08,
                )
                m.fit(pdf)

                future = m.make_future_dataframe(periods=90, freq="D")
                future["ds"] = pd.to_datetime(future["ds"]).dt.normalize()
                forecast = m.predict(future)
                forecast["ds"] = pd.to_datetime(forecast["ds"]).dt.normalize()

                pred_30 = _nearest_pred(forecast, target_30)
                pred_90 = _nearest_pred(forecast, target_90)

                if pred_30 is not None:
                    roi_30 = ((pred_30 - current_price) / current_price) * 100.0
                else:
                    roi_30 = None

                if pred_90 is not None:
                    roi_90 = ((pred_90 - current_price) / current_price) * 100.0
                else:
                    roi_90 = None

                # Confidence from interval width near 30D
                row30 = forecast.loc[(forecast["ds"] - target_30).abs().idxmin()]
                yhat = float(row30["yhat"])
                lower = float(row30.get("yhat_lower", yhat))
                upper = float(row30.get("yhat_upper", yhat))
                width = abs(upper - lower) / max(1e-6, abs(yhat))
                width = min(1.0, width)
                conf = max(5.0, min(95.0, 100.0 * (1.0 - width)))

                trend = "POSITIVE" if (roi_30 is not None and roi_30 > 0) else "NEGATIVE" if roi_30 is not None else "UNKNOWN"

                return {
                    "current_price": round(current_price, 2),
                    "predicted_price_30d": round(pred_30, 2) if pred_30 is not None else None,
                    "expected_roi_pct": round(roi_30, 2) if roi_30 is not None else None,
                    "predicted_price_90d": round(pred_90, 2) if pred_90 is not None else None,
                    "expected_roi_90d_pct": round(roi_90, 2) if roi_90 is not None else None,
                    "confidence": round(conf, 0),
                    "trend": trend,
                    "previous_close": tech.get("PreviousClose"),
                    "price_change": tech.get("Change"),
                    "percent_change": tech.get("ChangePercent"),
                    "day_high": tech.get("DayHigh"),
                    "day_low": tech.get("DayLow"),
                    "high_52w": tech.get("52WeekHigh"),
                    "low_52w": tech.get("52WeekLow"),
                    "volume": tech.get("Volume"),
                    "data_source": "Prophet",
                }
            except Exception:
                # fall through to linear
                pass

        # Linear fallback
        pred30, conf30 = _linear_fallback_forecast(df, 30)
        pred90, conf90 = _linear_fallback_forecast(df, 90)
        conf = conf30 if conf30 is not None else conf90

        roi_30 = ((pred30 - current_price) / current_price) * 100.0 if pred30 is not None else None
        roi_90 = ((pred90 - current_price) / current_price) * 100.0 if pred90 is not None else None
        trend = "POSITIVE" if (roi_30 is not None and roi_30 > 0) else "NEGATIVE" if roi_30 is not None else "UNKNOWN"

        return {
            "current_price": round(current_price, 2),
            "predicted_price_30d": round(pred30, 2) if pred30 is not None else None,
            "expected_roi_pct": round(roi_30, 2) if roi_30 is not None else None,
            "predicted_price_90d": round(pred90, 2) if pred90 is not None else None,
            "expected_roi_90d_pct": round(roi_90, 2) if roi_90 is not None else None,
            "confidence": round(conf, 0) if conf is not None else None,
            "trend": trend,
            "previous_close": tech.get("PreviousClose"),
            "price_change": tech.get("Change"),
            "percent_change": tech.get("ChangePercent"),
            "day_high": tech.get("DayHigh"),
            "day_low": tech.get("DayLow"),
            "high_52w": tech.get("52WeekHigh"),
            "low_52w": tech.get("52WeekLow"),
            "volume": tech.get("Volume"),
            "data_source": "LinearFallback",
        }

    # -------------------------------------------------------------------------
    # Public: Scoring (0-100)
    # -------------------------------------------------------------------------
    def generate_score(self, fundamentals: Dict[str, Any], ai_data: Dict[str, Any]) -> float:
        """
        Scoring weights (fast, explainable):
        - Value: P/E, P/B
        - Income: Dividend yield
        - Momentum: AI ROI30, trend, confidence
        - Risk: volatility, drawdown (from Technicals)

        Returns: 0..100
        """
        score = 50.0
        fundamentals = fundamentals or {}
        ai_data = ai_data or {}

        val = fundamentals.get("Valuation", {}) if isinstance(fundamentals.get("Valuation", {}), dict) else {}
        highlights = fundamentals.get("Highlights", {}) if isinstance(fundamentals.get("Highlights", {}), dict) else {}
        tech = fundamentals.get("Technicals", {}) if isinstance(fundamentals.get("Technicals", {}), dict) else {}

        # Value
        pe = _safe_float(val.get("TrailingPE")) or _safe_float(highlights.get("PERatio"))
        pb = _safe_float(val.get("PriceBookMRQ")) or _safe_float(highlights.get("PriceBook"))

        if pe is not None:
            if 0 < pe < 18:
                score += 10
            elif 18 <= pe < 30:
                score += 5
            elif pe >= 60:
                score -= 10

        if pb is not None:
            if 0 < pb < 2:
                score += 6
            elif pb >= 8:
                score -= 6

        # Dividend
        div_y = _safe_float(highlights.get("DividendYield"))
        if div_y is not None:
            if div_y >= 4:
                score += 6
            elif 2 <= div_y < 4:
                score += 3

        # Momentum (AI)
        roi30 = _safe_float(ai_data.get("expected_roi_pct"))
        conf = _safe_float(ai_data.get("confidence"))

        if roi30 is not None:
            if roi30 >= 10:
                score += 18
            elif roi30 >= 5:
                score += 12
            elif roi30 >= 0:
                score += 6
            else:
                score -= 10

        trend = (ai_data.get("trend") or "").upper()
        if trend == "POSITIVE":
            score += 6
        elif trend == "NEGATIVE":
            score -= 6

        if conf is not None:
            score += max(-3.0, min(5.0, (conf - 50.0) / 10.0))

        # Risk
        vol_ann = _safe_float(tech.get("Volatility30D_Ann"))
        dd90 = _safe_float(tech.get("MaxDrawdown90D"))

        if vol_ann is not None:
            if vol_ann > 0.60:
                score -= 10
            elif vol_ann > 0.45:
                score -= 6
            elif vol_ann < 0.25:
                score += 4

        if dd90 is not None:
            if dd90 <= -0.30:
                score -= 10
            elif dd90 <= -0.20:
                score -= 6
            elif dd90 >= -0.10:
                score += 2

        score = max(0.0, min(100.0, score))
        return float(round(score, 2))
