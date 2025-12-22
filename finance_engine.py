# finance_engine.py
"""
FinanceEngine – Enhanced (EODHD + AI Forecast + Risk + Shariah)
--------------------------------------------------------------
FULL REPLACEMENT – v2.2.2 (Render-safe, app.py-compatible)

Fixes:
✅ Correct EODHD domain: https://eodhistoricaldata.com/api
✅ KSA normalization: 1120 -> 1120.SR, keeps .SR
✅ Clear 401/403 messages + demo-token hints
✅ Partial-data mode (fundamentals may fail; history/technicals still run)
✅ run_ai_forecasting returns top-level keys expected by app.py
✅ Prophet forecast uses 30D/90D target dates + nearest prediction
✅ Linear fallback forecast + confidence if Prophet unavailable/fails
✅ DataQuality in fundamentals["meta"]["data_quality"]: OK / PARTIAL / ERROR
"""

from __future__ import annotations

import os
import math
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Prophet optional
try:
    from prophet import Prophet  # type: ignore
    _PROPHET_AVAILABLE = True
except Exception:
    Prophet = None  # type: ignore
    _PROPHET_AVAILABLE = False


# =============================================================================
# Helpers
# =============================================================================

def _utc_now_str() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat(sep=" ")


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
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b * 100.0


def _latest_period_key(period_dict: Dict[str, Any]) -> Optional[str]:
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
    df = pd.DataFrame(hist_data or [])
    if df.empty:
        return df

    if "date" not in df.columns:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    for c in ["open", "high", "low", "close", "adjusted_close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "close" in df.columns and "adjusted_close" in df.columns:
        df["close"] = df["close"].fillna(df["adjusted_close"])

    return df


def _compute_technicals_from_hist(df: pd.DataFrame) -> Dict[str, Any]:
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
    avg_vol_30d = float(df2["volume"].tail(30).mean()) if "volume" in df2.columns and not df2["volume"].tail(30).empty else None

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
            peak = max(peak, float(p))
            dd = (float(p) / peak) - 1.0
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
    """Simple linear regression fallback (fast, no dependencies). Returns (pred, confidence 0-100)."""
    try:
        df2 = df.dropna(subset=["close", "date"]).tail(365).copy()
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

        # confidence from residuals
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
    try:
        if forecast is None or forecast.empty:
            return None
        if "ds" not in forecast.columns or "yhat" not in forecast.columns:
            return None

        ds = pd.to_datetime(forecast["ds"], errors="coerce")
        if ds.isna().all():
            return None

        # exact match
        exact = forecast.loc[ds == target_date]
        if not exact.empty:
            return float(exact.iloc[0]["yhat"])

        # nearest
        diffs = (ds - target_date).abs()
        idx = diffs.idxmin()
        return float(forecast.loc[idx, "yhat"])
    except Exception:
        return None


def _confidence_from_interval(forecast: pd.DataFrame, target_date: pd.Timestamp) -> Optional[float]:
    """Confidence based on interval width around target date; 0-100."""
    try:
        if forecast is None or forecast.empty:
            return None
        if "ds" not in forecast.columns or "yhat" not in forecast.columns:
            return None

        ds = pd.to_datetime(forecast["ds"], errors="coerce")
        diffs = (ds - target_date).abs()
        idx = diffs.idxmin()

        row = forecast.loc[idx]
        yhat = _safe_float(row.get("yhat"))
        low = _safe_float(row.get("yhat_lower", yhat))
        up = _safe_float(row.get("yhat_upper", yhat))
        if yhat is None or low is None or up is None:
            return None

        width = abs(up - low) / max(1e-6, abs(yhat))
        width = min(1.0, max(0.0, width))
        conf = max(5.0, min(95.0, 100.0 * (1.0 - width)))
        return float(conf)
    except Exception:
        return None


# =============================================================================
# FinanceEngine
# =============================================================================

class FinanceEngine:
    def __init__(self, api_key: str):
        self.api_key = (api_key or "").strip()

        # ✅ Correct base domain
        self.base_url = "https://eodhistoricaldata.com/api"

        self.default_exchange = os.getenv("DEFAULT_EOD_EXCHANGE", "US").strip().upper()
        self.timeout = float(os.getenv("HTTP_TIMEOUT", "20"))

        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # -------------------------
    # Symbol normalization
    # -------------------------
    def _normalize_symbol(self, ticker: str) -> str:
        """
        EODHD equities typically require exchange suffix:
          COST -> COST.US
          GOOGL -> GOOGL.US
          1120 -> 1120.SR
          1120.SR stays as-is
          EURUSD.FOREX stays as-is
        """
        t = (ticker or "").strip().upper()
        if not t:
            return t

        # numeric Tadawul style
        if t.isdigit():
            return f"{t}.SR"

        # already has suffix
        if "." in t:
            return t

        return f"{t}.{self.default_exchange}"

    def _is_demo_key(self) -> bool:
        return self.api_key.lower() == "demo" or self.api_key == ""

    # -------------------------
    # HTTP JSON helper
    # -------------------------
    def _get_json(self, url: str) -> Any:
        r = self.session.get(url, timeout=self.timeout)
        ct = (r.headers.get("Content-Type") or "").lower()

        if r.status_code in (401, 403):
            hint = "EODHD token missing/invalid or plan limitation."
            if self._is_demo_key():
                hint = (
                    "You are using DEMO token. DEMO works only for limited tickers. "
                    "Set EODHD_API_TOKEN in Render environment variables."
                )
            text_snip = (r.text or "")[:200].replace("\n", " ").strip()
            raise RuntimeError(f"EODHD HTTP {r.status_code} ({hint}) | body={text_snip}")

        if "json" not in ct:
            text_snip = (r.text or "")[:200].replace("\n", " ").strip()
            raise RuntimeError(f"Non-JSON response ({r.status_code}) from EODHD | body={text_snip}")

        data = r.json()

        if r.status_code >= 400:
            msg = None
            if isinstance(data, dict):
                msg = data.get("message") or data.get("error") or str(data)
            raise RuntimeError(f"EODHD HTTP {r.status_code}: {msg or 'request failed'}")

        # Sometimes EODHD returns {"code":..., "message":...} with 200
        if isinstance(data, dict) and ("code" in data and "message" in data):
            raise RuntimeError(f"EODHD error: {data.get('message')}")

        return data

    # -------------------------------------------------------------------------
    # Data Fetch
    # -------------------------------------------------------------------------
    def get_stock_data(self, ticker: str) -> Tuple[Dict[str, Any], Any]:
        """
        Returns (fundamentals_dict, hist_data)

        Adds:
          fundamentals["meta"] = { data_quality, errors, symbol_used, updated_at_utc, provider, demo_key }
          fundamentals["Technicals"] derived from history (injected)
        """
        symbol = self._normalize_symbol(ticker)
        errors: List[str] = []
        fund_data: Dict[str, Any] = {}
        hist_data: Any = []

        # Fundamentals
        try:
            fund_url = f"{self.base_url}/fundamentals/{symbol}?api_token={self.api_key}&fmt=json"
            fd = self._get_json(fund_url)
            if isinstance(fd, dict):
                fund_data = fd
            else:
                errors.append("Fundamentals returned non-dict JSON")
        except Exception as e:
            errors.append(f"Fundamentals: {e}")

        # History (2 years)
        try:
            start = dt.date.today() - dt.timedelta(days=365 * 2)
            hist_url = f"{self.base_url}/eod/{symbol}?api_token={self.api_key}&fmt=json&from={start.isoformat()}"
            hist_data = self._get_json(hist_url)
        except Exception as e:
            errors.append(f"History: {e}")
            hist_data = []

        df = _extract_hist_df(hist_data)
        tech = _compute_technicals_from_hist(df)

        if not isinstance(fund_data, dict):
            fund_data = {}

        fund_data.setdefault("Technicals", {})
        if isinstance(fund_data["Technicals"], dict):
            for k, v in tech.items():
                if fund_data["Technicals"].get(k) is None:
                    fund_data["Technicals"][k] = v

        ok_fund = bool(fund_data)
        ok_hist = bool(df is not None and not df.empty)

        if ok_fund and ok_hist:
            dq = "OK"
        elif ok_fund or ok_hist:
            dq = "PARTIAL"
        else:
            dq = "ERROR"

        fund_data.setdefault("meta", {})
        if isinstance(fund_data["meta"], dict):
            fund_data["meta"].update({
                "data_quality": dq,
                "errors": errors[:8],
                "symbol_used": symbol,
                "updated_at_utc": _utc_now_str(),
                "provider": "EODHD",
                "demo_key": self._is_demo_key(),
            })

        # lightweight convenience fields for app.py
        if "market" not in fund_data:
            fund_data["market"] = "KSA" if symbol.endswith(".SR") else "GLOBAL"
        if "currency" not in fund_data:
            # EODHD fundamentals usually provide currency in General.CurrencyCode
            gen = fund_data.get("General", {}) if isinstance(fund_data.get("General", {}), dict) else {}
            fund_data["currency"] = gen.get("CurrencyCode")

        return fund_data, hist_data

    # -------------------------------------------------------------------------
    # Shariah Compliance (approximation)
    # -------------------------------------------------------------------------
    def check_shariah_compliance(self, fundamentals: Dict[str, Any]) -> Dict[str, Any]:
        """
        Approximate Shariah screening:
        - Debt / MarketCap < 33%
        - Cash / MarketCap < 33%

        Returns:
          compliant: True / False / None (None = UNKNOWN)
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

            # convert mln if needed
            if market_cap is not None and market_cap < 1e6 and _safe_float(highlights.get("MarketCapitalizationMln")) is not None:
                market_cap = market_cap * 1_000_000

            if market_cap is None or market_cap <= 0:
                return {"compliant": None, "status": "UNKNOWN", "note": "Missing Market Cap", "period": last_q}

            missing = []
            if long_debt is None and short_debt is None:
                missing.append("Debt")
            if cash is None:
                missing.append("Cash")

            total_debt = (long_debt or 0.0) + (short_debt or 0.0)
            cash_v = (cash or 0.0)

            debt_ratio = (total_debt / market_cap) * 100.0
            cash_ratio = (cash_v / market_cap) * 100.0

            if missing:
                return {
                    "compliant": None,
                    "status": "UNKNOWN",
                    "note": f"Missing components: {', '.join(missing)}",
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
    # AI Forecasting
    # -------------------------------------------------------------------------
    def run_ai_forecasting(self, hist_data: Any) -> Dict[str, Any]:
        """
        Returns keys that app.py expects:
          current_price, predicted_price_30d, expected_roi_pct,
          predicted_price_90d, expected_roi_90d_pct,
          confidence (0-100), trend,
          previous_close, price_change, percent_change, day_high, day_low,
          high_52w, low_52w, volume,
          data_source
        """
        df = _extract_hist_df(hist_data)
        if df.empty or "close" not in df.columns:
            return {
                "error": "Forecasting failed: empty/invalid history",
                "data_source": "EODHD",
            }

        df = df.dropna(subset=["date", "close"]).copy()
        tech = _compute_technicals_from_hist(df)

        if df.empty:
            return {"error": "Forecasting failed: invalid history", "data_source": "EODHD"}

        current_price = float(df.iloc[-1]["close"])
        last_date = pd.to_datetime(df.iloc[-1]["date"]).normalize()
        target_30 = (last_date + pd.Timedelta(days=30)).normalize()
        target_90 = (last_date + pd.Timedelta(days=90)).normalize()

        # Not enough rows? return current + technicals
        if len(df) < 60:
            return {
                "current_price": round(current_price, 2),
                "predicted_price_30d": None,
                "expected_roi_pct": None,
                "predicted_price_90d": None,
                "expected_roi_90d_pct": None,
                "confidence": None,
                "trend": "UNKNOWN",
                "previous_close": tech.get("PreviousClose"),
                "price_change": tech.get("Change"),
                "percent_change": tech.get("ChangePercent"),
                "day_high": tech.get("DayHigh"),
                "day_low": tech.get("DayLow"),
                "high_52w": tech.get("52WeekHigh"),
                "low_52w": tech.get("52WeekLow"),
                "volume": tech.get("Volume"),
                "data_source": "EODHD",
            }

        # Prophet
        if _PROPHET_AVAILABLE and Prophet is not None and len(df) >= 120:
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
                    changepoint_prior_scale=float(os.getenv("PROPHET_CPS", "0.08")),
                )
                m.fit(pdf)

                future = m.make_future_dataframe(periods=90, freq="D")
                future["ds"] = pd.to_datetime(future["ds"]).dt.normalize()
                forecast = m.predict(future)
                forecast["ds"] = pd.to_datetime(forecast["ds"]).dt.normalize()

                pred_30 = _nearest_pred(forecast, target_30)
                pred_90 = _nearest_pred(forecast, target_90)
                conf = _confidence_from_interval(forecast, target_30)

                if pred_30 is None or pred_90 is None:
                    raise RuntimeError("Prophet forecast missing targets")

                roi_30 = ((pred_30 - current_price) / current_price) * 100.0 if current_price else None
                roi_90 = ((pred_90 - current_price) / current_price) * 100.0 if current_price else None
                trend = "POSITIVE" if (roi_30 is not None and roi_30 > 0) else "NEGATIVE" if roi_30 is not None else "UNKNOWN"

                return {
                    "current_price": round(current_price, 2),
                    "predicted_price_30d": round(float(pred_30), 2),
                    "expected_roi_pct": round(float(roi_30), 2) if roi_30 is not None else None,
                    "predicted_price_90d": round(float(pred_90), 2),
                    "expected_roi_90d_pct": round(float(roi_90), 2) if roi_90 is not None else None,
                    "confidence": round(float(conf), 0) if conf is not None else None,
                    "trend": trend,
                    "previous_close": tech.get("PreviousClose"),
                    "price_change": tech.get("Change"),
                    "percent_change": tech.get("ChangePercent"),
                    "day_high": tech.get("DayHigh"),
                    "day_low": tech.get("DayLow"),
                    "high_52w": tech.get("52WeekHigh"),
                    "low_52w": tech.get("52WeekLow"),
                    "volume": tech.get("Volume"),
                    "data_source": "EODHD+Prophet",
                }
            except Exception as e:
                # Prophet failed; fallback below
                # (Don't crash the pipeline)
                print(f"[FinanceEngine] Prophet failed: {e}")

        # Fallback (linear)
        pred30, conf30 = _linear_fallback_forecast(df, 30)
        pred90, conf90 = _linear_fallback_forecast(df, 90)
        conf = conf30 if conf30 is not None else conf90

        roi_30 = ((pred30 - current_price) / current_price) * 100.0 if pred30 is not None and current_price else None
        roi_90 = ((pred90 - current_price) / current_price) * 100.0 if pred90 is not None and current_price else None
        trend = "POSITIVE" if (roi_30 is not None and roi_30 > 0) else "NEGATIVE" if roi_30 is not None else "UNKNOWN"

        return {
            "current_price": round(current_price, 2),
            "predicted_price_30d": round(float(pred30), 2) if pred30 is not None else None,
            "expected_roi_pct": round(float(roi_30), 2) if roi_30 is not None else None,
            "predicted_price_90d": round(float(pred90), 2) if pred90 is not None else None,
            "expected_roi_90d_pct": round(float(roi_90), 2) if roi_90 is not None else None,
            "confidence": round(float(conf), 0) if conf is not None else None,
            "trend": trend,
            "previous_close": tech.get("PreviousClose"),
            "price_change": tech.get("Change"),
            "percent_change": tech.get("ChangePercent"),
            "day_high": tech.get("DayHigh"),
            "day_low": tech.get("DayLow"),
            "high_52w": tech.get("52WeekHigh"),
            "low_52w": tech.get("52WeekLow"),
            "volume": tech.get("Volume"),
            "data_source": "EODHD+FallbackTrend",
        }

    # -------------------------------------------------------------------------
    # Scoring (0-100)
    # -------------------------------------------------------------------------
    def generate_score(self, fundamentals: Dict[str, Any], ai_data: Dict[str, Any]) -> float:
        """
        Value + Income + Momentum + Risk + Confidence (0–100)
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

        # Income
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

        if ai_data.get("trend") == "POSITIVE":
            score += 6
        elif ai_data.get("trend") == "NEGATIVE":
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
