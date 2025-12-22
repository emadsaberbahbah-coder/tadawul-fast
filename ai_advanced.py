# ai_advanced.py
"""
Advanced AI Analysis Layer – Enhanced (Explainable, Fast, Robust)
----------------------------------------------------------------
FULL REVISED SCRIPT (ONE-SHOT) – v1.2.0

Goals:
✅ Pure-python analytics (NO LLM required)
✅ Strong data-quality handling (works even with partial fundamentals/history)
✅ Uses BOTH:
   - hist_data (preferred for indicators)
   - fundamentals["Technicals"] (fallback if history missing)
✅ Produces consistent, sheet-friendly outputs:
   - opportunity_score (0-100)
   - risk_bucket (LOW / MODERATE / HIGH)
   - recommendation (STRONG BUY / BUY / HOLD / WATCH / SELL)
   - reasons / catalysts / risks (lists)
   - ai_summary (single compact string)
   - extra fields for debug & columns (RSI, MACD, momentum, volatility, drawdown, etc.)

Designed to plug into app.py with:
  analyze(ticker, fundamentals, hist_data, ai_forecast, shariah, base_score) -> dict
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =============================================================================
# Utilities
# =============================================================================

def _sf(x: Any) -> Optional[float]:
    """Safe float parser."""
    try:
        if x is None or isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            return None if math.isnan(v) else v
        s = str(x).strip().replace(",", "")
        if not s or s.lower() in {"na", "n/a", "none", "null", "-"}:
            return None
        v = float(s)
        return None if math.isnan(v) else v
    except Exception:
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """(a-b)/b * 100"""
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b * 100.0


def _hist_df(hist_data: Any) -> pd.DataFrame:
    """
    EODHD /eod returns list[dict]:
      {date, open, high, low, close, adjusted_close, volume}
    Normalize to index=date and numeric.
    """
    df = pd.DataFrame(hist_data or [])
    if df.empty:
        return df

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        df = df.drop_duplicates(subset=["date"], keep="last")
        df = df.set_index("date")
    else:
        # no dates => cannot compute indicators reliably
        return pd.DataFrame()

    for c in ["open", "high", "low", "close", "adjusted_close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "close" in df.columns and "adjusted_close" in df.columns:
        df["close"] = df["close"].fillna(df["adjusted_close"])

    return df


def _ema(series: pd.Series, span: int) -> Optional[float]:
    s = series.dropna()
    if len(s) < max(20, span + 5):
        return None
    return float(s.ewm(span=span, adjust=False).mean().iloc[-1])


def _ma(series: pd.Series, window: int) -> Optional[float]:
    s = series.dropna()
    if len(s) < window:
        return None
    return float(s.rolling(window).mean().iloc[-1])


def _rsi(series: pd.Series, period: int = 14) -> Optional[float]:
    s = series.dropna()
    if len(s) < period + 10:
        return None
    delta = s.diff().dropna()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.rolling(period).mean().iloc[-1]
    avg_loss = loss.rolling(period).mean().iloc[-1]
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (macd_line, signal_line, histogram)
    """
    s = series.dropna()
    if len(s) < slow + signal + 20:
        return None, None, None
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    sig_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - sig_line
    return float(macd_line.iloc[-1]), float(sig_line.iloc[-1]), float(hist.iloc[-1])


def _vol_ann(series: pd.Series) -> Optional[float]:
    """30D stdev of daily returns annualized."""
    s = series.dropna()
    if len(s) < 40:
        return None
    r = s.pct_change().dropna()
    if len(r) < 20:
        return None
    vol30 = r.tail(30).std(ddof=1)
    if pd.isna(vol30):
        return None
    return float(vol30 * math.sqrt(252))


def _max_dd(series: pd.Series, lookback: int = 90) -> Optional[float]:
    s = series.dropna()
    if len(s) < 30:
        return None
    tail = s.tail(lookback)
    if tail.empty:
        return None
    peak = tail.iloc[0]
    dd_min = 0.0
    for p in tail:
        peak = max(peak, p)
        dd = (p / peak) - 1.0
        dd_min = min(dd_min, dd)
    return float(dd_min)


def _momentum_pct(series: pd.Series, days: int) -> Optional[float]:
    s = series.dropna()
    if len(s) < days + 5:
        return None
    cur = float(s.iloc[-1])
    past = float(s.iloc[-(days + 1)])
    return _pct(cur, past)


def _infer_current_price(fundamentals: Dict[str, Any]) -> Optional[float]:
    tech = fundamentals.get("Technicals", {}) if isinstance(fundamentals.get("Technicals", {}), dict) else {}
    return _sf(tech.get("Price")) or _sf(fundamentals.get("Technicals", {}).get("LastPrice") if isinstance(fundamentals.get("Technicals", {}), dict) else None)


def _extract_metric(fundamentals: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    Best-effort extraction from EODHD fundamentals.
    Not all tickers have all fields; we keep it safe.
    """
    val = fundamentals.get("Valuation", {}) if isinstance(fundamentals.get("Valuation", {}), dict) else {}
    hi = fundamentals.get("Highlights", {}) if isinstance(fundamentals.get("Highlights", {}), dict) else {}
    gen = fundamentals.get("General", {}) if isinstance(fundamentals.get("General", {}), dict) else {}
    tech = fundamentals.get("Technicals", {}) if isinstance(fundamentals.get("Technicals", {}), dict) else {}

    pe = _sf(val.get("TrailingPE")) or _sf(hi.get("PERatio"))
    pb = _sf(val.get("PriceBookMRQ")) or _sf(hi.get("PriceBook"))
    ps = _sf(val.get("PriceSalesTTM")) or _sf(hi.get("PriceSalesTTM"))
    ev_ebitda = _sf(val.get("EVToEBITDA")) or _sf(hi.get("EVToEBITDA"))
    divy = _sf(hi.get("DividendYield"))  # often percent
    roe = _sf(hi.get("ReturnOnEquityTTM")) or _sf(hi.get("ROE"))
    roa = _sf(hi.get("ReturnOnAssetsTTM")) or _sf(hi.get("ROA"))
    mcap = _sf(val.get("MarketCapitalization")) or _sf(hi.get("MarketCapitalization")) or _sf(hi.get("MarketCapitalizationMln"))
    if mcap is not None and mcap < 1e6 and _sf(hi.get("MarketCapitalizationMln")) is not None:
        mcap = mcap * 1_000_000

    # Liquidity proxies
    vol = _sf(tech.get("Volume"))
    avg_vol = _sf(tech.get("AvgVolume30D"))
    price = _sf(tech.get("Price")) or _infer_current_price(fundamentals)

    value_traded = (price * vol) if (price is not None and vol is not None) else None

    return {
        "pe": pe,
        "pb": pb,
        "ps": ps,
        "ev_ebitda": ev_ebitda,
        "divy": divy,
        "roe": roe,
        "roa": roa,
        "mcap": mcap,
        "price": price,
        "volume": vol,
        "avg_volume_30d": avg_vol,
        "value_traded": value_traded,
        "sector": (gen.get("Sector") if isinstance(gen, dict) else None),
    }


def _data_quality_flags(fundamentals: Dict[str, Any], df: pd.DataFrame, ai_forecast: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a simple, explainable quality signal so your sheet can show why data is missing.
    """
    flags: List[str] = []

    # History
    hist_rows = 0 if df is None or df.empty else int(len(df))
    if hist_rows < 60:
        flags.append("HISTORY_SHORT")

    # Fundamentals
    val = fundamentals.get("Valuation", {}) if isinstance(fundamentals.get("Valuation", {}), dict) else {}
    if not val:
        flags.append("FUNDAMENTALS_THIN")

    # Forecast
    if ai_forecast.get("error"):
        flags.append("FORECAST_ERROR")

    quality = 100.0
    if "HISTORY_SHORT" in flags:
        quality -= 25
    if "FUNDAMENTALS_THIN" in flags:
        quality -= 20
    if "FORECAST_ERROR" in flags:
        quality -= 30

    quality = _clamp(quality, 0, 100)

    return {
        "data_quality_score": round(quality, 0),
        "data_quality_flags": flags,
        "hist_rows": hist_rows,
    }


# =============================================================================
# Public API
# =============================================================================

def analyze(
    ticker: str,
    fundamentals: Dict[str, Any],
    hist_data: Any,
    ai_forecast: Dict[str, Any],
    shariah: Dict[str, Any],
    base_score: Optional[float],
) -> Dict[str, Any]:
    """
    Returns a robust advanced analysis bundle.

    Scoring Philosophy (explainable):
    - Start from base_score (from FinanceEngine.generate_score)
    - Add signals from:
        Value (PE/PB/EV-EBITDA),
        Momentum (MA regime, RSI, MACD, 1M/3M momentum),
        Risk (volatility, drawdown, liquidity proxies),
        AI (ROI30 + confidence),
        Shariah bonus/penalty.
    - Output:
        opportunity_score (0-100), risk bucket, recommendation, reasons/catalysts/risks, summary.
    """

    t = (ticker or "").strip().upper()
    fundamentals = fundamentals or {}
    ai_forecast = ai_forecast or {}
    shariah = shariah or {}

    # Build DF from history (preferred)
    df = _hist_df(hist_data)
    closes = df["close"] if (df is not None and not df.empty and "close" in df.columns) else pd.Series(dtype=float)

    # Indicators from history (if available)
    rsi14 = _rsi(closes, 14)
    ma20 = _ma(closes, 20)
    ma50 = _ma(closes, 50)
    ma200 = _ma(closes, 200)
    macd_line, macd_sig, macd_hist = _macd(closes)
    vol_ann = _vol_ann(closes)
    dd90 = _max_dd(closes, 90)
    mom_1m = _momentum_pct(closes, 21)     # ~1 month trading days
    mom_3m = _momentum_pct(closes, 63)     # ~3 months trading days

    # If history missing, we still can use some from fundamentals["Technicals"]
    tech = fundamentals.get("Technicals", {}) if isinstance(fundamentals.get("Technicals", {}), dict) else {}
    if vol_ann is None:
        vol_ann = _sf(tech.get("Volatility30D_Ann"))
    if dd90 is None:
        dd90 = _sf(tech.get("MaxDrawdown90D"))

    # Extract best-effort fundamentals
    m = _extract_metric(fundamentals)
    pe, pb, ps, ev_ebitda = m["pe"], m["pb"], m["ps"], m["ev_ebitda"]
    divy, roe, roa, mcap = m["divy"], m["roe"], m["roa"], m["mcap"]
    price, volume, avg_vol_30d, value_traded = m["price"], m["volume"], m["avg_volume_30d"], m["value_traded"]
    sector = m["sector"]

    # AI fields
    roi30 = _sf(ai_forecast.get("expected_roi_pct"))
    roi90 = _sf(ai_forecast.get("expected_roi_90d_pct"))
    conf = _sf(ai_forecast.get("confidence"))
    trend = (ai_forecast.get("trend") or "UNKNOWN")

    # Data quality
    dq = _data_quality_flags(fundamentals, df, ai_forecast)
    dq_score = _sf(dq.get("data_quality_score")) or 70.0

    # Start score
    score = float(base_score if base_score is not None else 50.0)

    reasons: List[str] = []
    catalysts: List[str] = []
    risks: List[str] = []

    # =============================================================================
    # Momentum regime (MAs, MACD)
    # =============================================================================
    if ma20 is not None and ma50 is not None:
        if ma20 > ma50:
            score += 4
            reasons.append("Short-term trend improving (MA20 > MA50).")
        else:
            score -= 2
            risks.append("Short-term trend weakening (MA20 < MA50).")

    if ma50 is not None and ma200 is not None:
        if ma50 > ma200:
            score += 7
            reasons.append("Long-term uptrend (MA50 > MA200).")
        else:
            score -= 4
            risks.append("Long-term downtrend (MA50 < MA200).")

    if macd_hist is not None:
        if macd_hist > 0:
            score += 3
            catalysts.append("MACD histogram positive (bullish momentum).")
        else:
            score -= 2
            risks.append("MACD histogram negative (bearish momentum).")

    # RSI
    if rsi14 is not None:
        if rsi14 < 35:
            score += 4
            catalysts.append("Oversold (RSI < 35) – rebound potential.")
        elif rsi14 > 70:
            score -= 4
            risks.append("Overbought (RSI > 70) – pullback risk.")

    # Price momentum 1M/3M
    if mom_1m is not None:
        if mom_1m >= 5:
            score += 3
            reasons.append("Strong 1M momentum.")
        elif mom_1m <= -5:
            score -= 3
            risks.append("Weak 1M momentum (recent decline).")

    if mom_3m is not None:
        if mom_3m >= 8:
            score += 4
            reasons.append("Strong 3M momentum.")
        elif mom_3m <= -8:
            score -= 4
            risks.append("Weak 3M momentum (medium-term decline).")

    # =============================================================================
    # Valuation & quality
    # =============================================================================
    if pe is not None:
        if 0 < pe < 18:
            score += 6
            reasons.append("Attractive valuation (P/E < 18).")
        elif 18 <= pe < 30:
            score += 2
            reasons.append("Reasonable valuation (P/E 18–30).")
        elif pe >= 60:
            score -= 6
            risks.append("Very high valuation (P/E > 60).")

    if pb is not None:
        if 0 < pb < 2:
            score += 3
            reasons.append("Low P/B supports value.")
        elif pb >= 8:
            score -= 3
            risks.append("High P/B suggests expensive pricing vs book.")

    if ev_ebitda is not None:
        if 0 < ev_ebitda < 10:
            score += 3
            reasons.append("EV/EBITDA indicates reasonable valuation.")
        elif ev_ebitda > 25:
            score -= 3
            risks.append("High EV/EBITDA suggests expensive valuation.")

    if roe is not None:
        if roe >= 15:
            score += 3
            reasons.append("Strong profitability (ROE ≥ 15%).")
        elif roe < 5:
            score -= 2
            risks.append("Weak ROE (< 5%).")

    if roa is not None and roa >= 6:
        score += 1
        reasons.append("Healthy ROA supports efficiency.")

    if divy is not None:
        # EODHD often returns dividend yield as percent
        if divy >= 4:
            score += 3
            reasons.append("High dividend yield supports total return.")
        elif divy >= 2:
            score += 1
            reasons.append("Dividend yield supports stability.")

    # =============================================================================
    # Risk (volatility, drawdown, liquidity)
    # =============================================================================
    # Volatility & drawdown
    if vol_ann is not None:
        if vol_ann > 0.60:
            score -= 10
            risks.append("High volatility (annualized > 60%).")
        elif vol_ann > 0.45:
            score -= 6
            risks.append("Elevated volatility (annualized > 45%).")
        elif vol_ann < 0.25:
            score += 2
            reasons.append("Low volatility supports stability.")

    if dd90 is not None:
        if dd90 <= -0.30:
            score -= 8
            risks.append("Large 90D drawdown (>30%) indicates elevated risk.")
        elif dd90 <= -0.20:
            score -= 4
            risks.append("Meaningful 90D drawdown (>20%).")
        elif dd90 >= -0.10:
            score += 1
            reasons.append("Limited drawdown improves risk profile.")

    # Liquidity proxy
    if value_traded is not None:
        if value_traded < 2_000_000:
            score -= 3
            risks.append("Low liquidity (value traded is small).")
        elif value_traded > 20_000_000:
            score += 1
            reasons.append("Good liquidity (high value traded).")

    # Market cap (stability bias)
    if mcap is not None:
        if mcap >= 50_000_000_000:
            score += 2
            reasons.append("Large market cap supports stability.")
        elif mcap < 2_000_000_000:
            score -= 2
            risks.append("Small/medium market cap increases volatility risk.")

    # =============================================================================
    # AI forecast impact (ROI + confidence)
    # =============================================================================
    if roi30 is not None:
        if roi30 >= 10:
            score += 9
            catalysts.append("AI forecast shows strong 30D upside.")
        elif roi30 >= 5:
            score += 6
            catalysts.append("AI forecast shows moderate 30D upside.")
        elif roi30 >= 0:
            score += 2
            reasons.append("AI forecast slightly positive (30D).")
        else:
            score -= 7
            risks.append("AI forecast negative expected ROI (30D).")

    if roi90 is not None and roi90 >= 12:
        score += 2
        catalysts.append("AI 90D forecast suggests sustained upside.")

    if trend == "POSITIVE":
        score += 2
    elif trend == "NEGATIVE":
        score -= 2

    if conf is not None:
        # gentle confidence adjustment
        score += _clamp((conf - 50.0) / 15.0, -3.0, 4.0)

    # Penalize if data quality is weak
    if dq_score < 60:
        score -= 4
        risks.append("Data quality is weak (limited history/fundamentals).")

    # =============================================================================
    # Shariah
    # =============================================================================
    compliant = bool(shariah.get("compliant", False))
    if compliant:
        score += 2
        reasons.append("Shariah compliant (ratio screening passed).")
    else:
        score -= 4
        risks.append("Not Shariah compliant (ratio screening failed).")

    # Clamp
    score = _clamp(score, 0.0, 100.0)

    # =============================================================================
    # Risk bucket (multi-factor)
    # =============================================================================
    risk_bucket = "MODERATE"
    # Primary: volatility
    if vol_ann is not None:
        if vol_ann <= 0.30:
            risk_bucket = "LOW"
        elif vol_ann >= 0.55:
            risk_bucket = "HIGH"

    # Override to HIGH if drawdown severe
    if dd90 is not None and dd90 <= -0.35:
        risk_bucket = "HIGH"

    # If market cap very large + low vol, keep LOW
    if risk_bucket != "HIGH" and mcap is not None and mcap >= 100_000_000_000 and (vol_ann is not None and vol_ann <= 0.35):
        risk_bucket = "LOW"

    # =============================================================================
    # Recommendation
    # =============================================================================
    if score >= 85 and (roi30 or 0) >= 7 and compliant:
        rec = "STRONG BUY"
    elif score >= 72 and compliant:
        rec = "BUY"
    elif score >= 55:
        rec = "HOLD"
    elif score >= 45:
        rec = "WATCH"
    else:
        rec = "SELL"

    # =============================================================================
    # Build compact summary (single string)
    # =============================================================================
    parts: List[str] = []
    if roi30 is not None:
        parts.append(f"ROI30={roi30:.1f}%")
    if roi90 is not None:
        parts.append(f"ROI90={roi90:.1f}%")
    if conf is not None:
        parts.append(f"Conf={conf:.0f}/100")
    if vol_ann is not None:
        parts.append(f"Vol={vol_ann*100:.0f}%")
    if dd90 is not None:
        parts.append(f"DD90={dd90*100:.0f}%")
    if rsi14 is not None:
        parts.append(f"RSI={rsi14:.0f}")
    parts.append(f"Risk={risk_bucket}")
    parts.append(f"Action={rec}")
    parts.append(f"DQ={int(dq_score)}")

    ai_summary = " | ".join(parts)

    # Limit list sizes for sheet readability
    reasons = reasons[:6]
    catalysts = catalysts[:6]
    risks = risks[:6]

    # A sheet-friendly “why selected?” short string
    why_selected = "; ".join((reasons + catalysts)[:4]) if (reasons or catalysts) else "Insufficient signals"

    return {
        # Primary outputs
        "opportunity_score": round(float(score), 2),
        "risk_bucket": risk_bucket,
        "recommendation": rec,
        "ai_summary": ai_summary,

        # Explainability
        "reasons": reasons,
        "catalysts": catalysts,
        "risks": risks,
        "why_selected": why_selected,

        # Key indicators (optional columns)
        "sector": sector,
        "pe": pe,
        "pb": pb,
        "ev_ebitda": ev_ebitda,
        "div_yield": divy,
        "roe": roe,
        "roa": roa,
        "market_cap": mcap,
        "price": price,
        "value_traded": value_traded,
        "avg_volume_30d": avg_vol_30d,

        "rsi14": round(rsi14, 1) if rsi14 is not None else None,
        "ma20": round(ma20, 2) if ma20 is not None else None,
        "ma50": round(ma50, 2) if ma50 is not None else None,
        "ma200": round(ma200, 2) if ma200 is not None else None,
        "macd": round(macd_line, 4) if macd_line is not None else None,
        "macd_signal": round(macd_sig, 4) if macd_sig is not None else None,
        "macd_hist": round(macd_hist, 4) if macd_hist is not None else None,
        "momentum_1m_pct": round(mom_1m, 2) if mom_1m is not None else None,
        "momentum_3m_pct": round(mom_3m, 2) if mom_3m is not None else None,
        "vol_ann": vol_ann,
        "max_dd_90d": dd90,

        # Data quality
        "data_quality_score": dq.get("data_quality_score"),
        "data_quality_flags": dq.get("data_quality_flags"),
        "hist_rows": dq.get("hist_rows"),
    }
