# core/investment_advisor_engine.py
"""
Investment Advisor Engine (v1.0.0) — PROD SAFE

Design
- Uses existing DataEngine via request.app.state.engine (preferred) or lazy fallback.
- Works with already-enriched fields in UnifiedQuote:
    - symbol, name, price/current_price
    - forecast_price_1m / forecast_price_3m
    - expected_roi_1m / expected_roi_3m (or expected_return_1m/3m)
    - risk_bucket, confidence_bucket
    - overall_score, recommendation
- Returns a Sheets-friendly response envelope with stable keys.

No heavy AI/news in v1 (keeps prod safe).
Later we can add optional news sentiment as a safe enrichment layer.
"""

from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _ratio_to_percent(v: Any) -> Optional[float]:
    x = _safe_float(v)
    if x is None:
        return None
    if -1.0 <= x <= 1.0:
        return x * 100.0
    return x


def _get(obj: Any, *names: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for n in names:
            if n in obj and obj[n] is not None:
                return obj[n]
        return None
    for n in names:
        try:
            v = getattr(obj, n, None)
            if v is not None:
                return v
        except Exception:
            pass
    return None


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _normalize_bucket(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip().capitalize()
    if s in ("Low", "Moderate", "High"):
        return s
    # allow numeric
    try:
        f = float(x)
        if f >= 70:
            return "High"
        if f >= 45:
            return "Moderate"
        return "Low"
    except Exception:
        return None


def _passes_bucket_filter(val: Optional[str], want: Optional[str]) -> bool:
    if not want:
        return True
    if not val:
        return False
    return val.strip().capitalize() == want.strip().capitalize()


async def _resolve_engine(request) -> Optional[Any]:
    try:
        eng = getattr(getattr(request, "app", None), "state", None)
        if eng:
            engine = getattr(eng, "engine", None)
            if engine is not None:
                return engine
    except Exception:
        pass

    # fallback singleton (lazy, safe)
    try:
        from core.data_engine_v2 import get_engine  # type: ignore
        return await _maybe_await(get_engine())
    except Exception:
        return None


def _extract_roi_and_forecast(uq: Any) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns:
      roi_1m_pct, roi_3m_pct, fp1, fp3
    """
    roi_1m = _ratio_to_percent(_get(uq, "expected_roi_1m", "expected_return_1m"))
    roi_3m = _ratio_to_percent(_get(uq, "expected_roi_3m", "expected_return_3m"))

    fp1 = _safe_float(_get(uq, "forecast_price_1m", "expected_price_1m"))
    fp3 = _safe_float(_get(uq, "forecast_price_3m", "expected_price_3m"))

    # If forecast missing but ROI exists, derive from current price
    price = _safe_float(_get(uq, "current_price", "last_price", "price"))
    if price is not None and price > 0:
        if fp1 is None and roi_1m is not None:
            fp1 = price * (1.0 + roi_1m / 100.0)
        if fp3 is None and roi_3m is not None:
            fp3 = price * (1.0 + roi_3m / 100.0)

    return roi_1m, roi_3m, fp1, fp3


def _gain_loss(amount: float, roi_pct: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if amount <= 0 or roi_pct is None:
        return None, None
    gain = amount * (roi_pct / 100.0)
    # simple symmetric risk placeholder (v1) — later replace with VaR/vol-based band
    loss = amount * (abs(min(roi_pct, 0.0)) / 100.0) if roi_pct < 0 else amount * (roi_pct / 200.0)
    return round(gain, 2), round(loss, 2)


async def build_recommendations(*, request, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry used by routes/investment_advisor.py
    """
    sources_in = payload.get("sources") or payload.get("source") or "ALL"
    if isinstance(sources_in, str) and sources_in.strip().upper() == "ALL":
        sources = list(DEFAULT_SOURCES)
    elif isinstance(sources_in, list) and sources_in:
        sources = [str(x).strip() for x in sources_in if str(x).strip()]
    else:
        sources = list(DEFAULT_SOURCES)

    risk_want = _normalize_bucket(payload.get("risk"))
    conf_want = _normalize_bucket(payload.get("confidence"))

    req_roi_1m = _safe_float(payload.get("required_roi_1m"))
    req_roi_3m = _safe_float(payload.get("required_roi_3m"))
    max_items = int(_safe_float(payload.get("max_items")) or 10)
    max_items = max(1, min(50, max_items))

    amount = _safe_float(payload.get("amount")) or 0.0
    if amount < 0:
        amount = 0.0

    engine = await _resolve_engine(request)
    if engine is None:
        return {"status": "error", "error": "Engine unavailable", "items": [], "timestamp_utc": _now_utc_iso()}

    # Candidate universe strategy (v1):
    # - If payload provides tickers, analyze those
    # - Else: we cannot read your Google Sheets from backend directly (unless you already have an endpoint),
    #         so we return an instructive error for now.
    tickers = payload.get("tickers") or payload.get("symbols") or []
    if not isinstance(tickers, list) or not tickers:
        return {
            "status": "error",
            "error": "No tickers provided. v1 requires payload.tickers (later we will add 'pull from sheet sources' via your /v1/enriched/* endpoints).",
            "items": [],
            "timestamp_utc": _now_utc_iso(),
            "sources_used": sources,
        }

    # Fetch enriched quotes in batch (best effort)
    fn = getattr(engine, "get_enriched_quotes", None) or getattr(engine, "get_quotes", None)
    if not callable(fn):
        return {"status": "error", "error": "Engine does not support batch quotes", "items": [], "timestamp_utc": _now_utc_iso()}

    # best-effort include history/fundamentals if supported
    try:
        res = await _maybe_await(fn(tickers, refresh=False, include_history=True, include_fundamentals=True))
    except TypeError:
        res = await _maybe_await(fn(tickers))
    except Exception as exc:
        return {"status": "error", "error": f"Engine fetch error: {exc}", "items": [], "timestamp_utc": _now_utc_iso()}

    quotes: List[Any] = []
    if isinstance(res, dict):
        quotes = list(res.values())
    elif isinstance(res, list):
        quotes = res
    else:
        quotes = list(res or [])

    items: List[Dict[str, Any]] = []

    for uq in quotes:
        sym = str(_get(uq, "symbol") or "").strip().upper()
        if not sym:
            continue

        price = _safe_float(_get(uq, "current_price", "last_price", "price"))
        risk_bucket = _normalize_bucket(_get(uq, "risk_bucket"))
        conf_bucket = _normalize_bucket(_get(uq, "confidence_bucket"))

        if not _passes_bucket_filter(risk_bucket, risk_want):
            continue
        if not _passes_bucket_filter(conf_bucket, conf_want):
            continue

        roi_1m, roi_3m, fp1, fp3 = _extract_roi_and_forecast(uq)

        if req_roi_1m is not None and (roi_1m is None or roi_1m < req_roi_1m):
            continue
        if req_roi_3m is not None and (roi_3m is None or roi_3m < req_roi_3m):
            continue

        gain1, loss1 = _gain_loss(amount, roi_1m)
        gain3, loss3 = _gain_loss(amount, roi_3m)

        items.append(
            {
                "symbol": sym,
                "name": _get(uq, "name", "company_name"),
                "price": price,
                "risk_bucket": risk_bucket,
                "confidence_bucket": conf_bucket,
                "overall_score": _safe_float(_get(uq, "overall_score")),
                "recommendation": _get(uq, "recommendation"),
                "roi_1m_pct": roi_1m,
                "roi_3m_pct": roi_3m,
                "forecast_price_1m": fp1,
                "forecast_price_3m": fp3,
                "amount": amount,
                "expected_gain_1m": gain1,
                "expected_loss_1m": loss1,
                "expected_gain_3m": gain3,
                "expected_loss_3m": loss3,
            }
        )

    # rank: higher overall_score then higher roi_3m
    def _key(it: Dict[str, Any]) -> Tuple[float, float]:
        s = it.get("overall_score")
        r = it.get("roi_3m_pct")
        return (float(s) if s is not None else 0.0, float(r) if r is not None else -999.0)

    items.sort(key=_key, reverse=True)
    items = items[:max_items]

    return {
        "status": "ok",
        "sources_used": sources,
        "filters": {
            "risk": risk_want,
            "confidence": conf_want,
            "required_roi_1m": req_roi_1m,
            "required_roi_3m": req_roi_3m,
            "max_items": max_items,
            "amount": amount,
        },
        "count": len(items),
        "items": items,
        "timestamp_utc": _now_utc_iso(),
    }
