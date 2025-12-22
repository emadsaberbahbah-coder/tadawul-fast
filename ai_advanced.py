# ai_advanced.py
"""
Advanced AI Analysis Layer – v1.0.0
-----------------------------------
Pure-python analytics (no LLM required), explainable, fast.

Inputs:
- ticker, fundamentals, hist_data, ai_forecast, shariah, base_score
Outputs:
- opportunity_score (0-100)
- risk_bucket (LOW / MODERATE / HIGH)
- reasons (list[str])
- catalysts (list[str])
- risks (list[str])
- recommendation (STRONG BUY / BUY / HOLD / WATCH / SELL)
- ai_summary (single string)

Designed to plug into app.py.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd


def _sf(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        v = float(str(x).replace(",", "").strip())
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def _hist_df(hist_data: Any) -> pd.DataFrame:
    df = pd.DataFrame(hist_data or [])
    if df.empty:
        return df
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        df = df.drop_duplicates(subset=["date"], keep="last")
        df = df.set_index("date")
    for c in ["open", "high", "low", "close", "adjusted_close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "close" in df.columns and "adjusted_close" in df.columns:
        df["close"] = df["close"].fillna(df["adjusted_close"])
    return df


def _rsi(series: pd.Series, period: int = 14) -> Optional[float]:
    s = series.dropna()
    if len(s) < period + 5:
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


def _ma(series: pd.Series, window: int) -> Optional[float]:
    s = series.dropna()
    if len(s) < window:
        return None
    return float(s.rolling(window).mean().iloc[-1])


def _vol_ann(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    if len(s) < 20:
        return None
    r = s.pct_change().dropna()
    if len(r) < 10:
        return None
    return float(r.tail(30).std(ddof=1) * math.sqrt(252))


def _max_dd(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    if len(s) < 30:
        return None
    peak = s.iloc[0]
    dd_min = 0.0
    for p in s.tail(90):
        peak = max(peak, p)
        dd = (p / peak) - 1.0
        dd_min = min(dd_min, dd)
    return float(dd_min)


def analyze(
    ticker: str,
    fundamentals: Dict[str, Any],
    hist_data: Any,
    ai_forecast: Dict[str, Any],
    shariah: Dict[str, Any],
    base_score: Optional[float],
) -> Dict[str, Any]:

    df = _hist_df(hist_data)
    closes = df["close"] if (not df.empty and "close" in df.columns) else pd.Series(dtype=float)

    # --- indicators
    rsi14 = _rsi(closes, 14)
    ma20 = _ma(closes, 20)
    ma50 = _ma(closes, 50)
    ma200 = _ma(closes, 200)
    vol = _vol_ann(closes)
    dd = _max_dd(closes)

    # --- fundamentals
    val = fundamentals.get("Valuation", {}) if isinstance(fundamentals.get("Valuation", {}), dict) else {}
    hi = fundamentals.get("Highlights", {}) if isinstance(fundamentals.get("Highlights", {}), dict) else {}
    pe = _sf(val.get("TrailingPE")) or _sf(hi.get("PERatio"))
    pb = _sf(val.get("PriceBookMRQ")) or _sf(hi.get("PriceBook"))
    divy = _sf(hi.get("DividendYield"))  # often percent

    # --- AI
    roi30 = _sf(ai_forecast.get("expected_roi_pct"))
    conf = _sf(ai_forecast.get("confidence"))

    # --- scoring blend
    score = float(base_score or 50.0)
    reasons: List[str] = []
    catalysts: List[str] = []
    risks: List[str] = []

    # Momentum (MA + RSI)
    if ma20 and ma50 and ma20 > ma50:
        score += 5
        reasons.append("Short-term trend improving (MA20 > MA50).")
    if ma50 and ma200 and ma50 > ma200:
        score += 8
        reasons.append("Long-term trend positive (MA50 > MA200).")

    if rsi14 is not None:
        if rsi14 < 35:
            score += 4
            catalysts.append("Oversold signal (RSI<35) may mean rebound potential.")
        elif rsi14 > 70:
            score -= 5
            risks.append("Overbought signal (RSI>70) – pullback risk.")

    # Valuation
    if pe is not None:
        if 0 < pe < 18:
            score += 6
            reasons.append("Attractive valuation (P/E < 18).")
        elif pe > 50:
            score -= 6
            risks.append("High valuation (P/E > 50) increases downside risk.")

    if pb is not None and pb > 8:
        score -= 3
        risks.append("High P/B ratio suggests expensive pricing vs book value.")

    if divy is not None and divy >= 3:
        score += 3
        reasons.append("Dividend yield supports total return.")

    # Risk
    if vol is not None:
        if vol > 0.60:
            score -= 10
            risks.append("High volatility (annualized > 60%).")
        elif vol < 0.25:
            score += 3
            reasons.append("Low volatility supports stability.")

    if dd is not None and dd <= -0.25:
        score -= 6
        risks.append("Recent large drawdown indicates elevated risk.")

    # AI ROI
    if roi30 is not None:
        if roi30 >= 10:
            score += 10
            catalysts.append("AI forecast shows strong 30D upside potential.")
        elif roi30 < 0:
            score -= 8
            risks.append("AI forecast indicates negative expected ROI (30D).")

    # Confidence
    if conf is not None:
        score += max(-2, min(4, (conf - 50) / 12))

    # Shariah
    compliant = bool(shariah.get("compliant", False))
    if compliant:
        score += 2
        reasons.append("Shariah compliant (ratio screening passed).")
    else:
        score -= 4
        risks.append("Not Shariah compliant (ratio screening failed).")

    # Clamp
    score = max(0.0, min(100.0, score))

    # Risk bucket
    risk_bucket = "MODERATE"
    if vol is not None:
        if vol <= 0.30:
            risk_bucket = "LOW"
        elif vol >= 0.55:
            risk_bucket = "HIGH"

    # Recommendation
    if score >= 85 and (roi30 or 0) >= 7:
        rec = "STRONG BUY"
    elif score >= 70:
        rec = "BUY"
    elif score >= 55:
        rec = "HOLD"
    elif score >= 45:
        rec = "WATCH"
    else:
        rec = "SELL"

    summary_parts = []
    if roi30 is not None:
        summary_parts.append(f"ROI30={roi30:.1f}%")
    if conf is not None:
        summary_parts.append(f"Conf={conf:.0f}/100")
    if vol is not None:
        summary_parts.append(f"Vol={vol*100:.0f}%")
    if dd is not None:
        summary_parts.append(f"DD90={dd*100:.0f}%")
    summary_parts.append(f"Risk={risk_bucket}")
    summary_parts.append(f"Action={rec}")

    return {
        "opportunity_score": round(score, 2),
        "risk_bucket": risk_bucket,
        "recommendation": rec,
        "reasons": reasons[:6],
        "catalysts": catalysts[:6],
        "risks": risks[:6],
        "ai_summary": " | ".join(summary_parts),
        "rsi14": round(rsi14, 1) if rsi14 is not None else None,
        "ma20": round(ma20, 2) if ma20 is not None else None,
        "ma50": round(ma50, 2) if ma50 is not None else None,
        "ma200": round(ma200, 2) if ma200 is not None else None,
        "vol_ann": vol,
        "max_dd_90d": dd,
    }
