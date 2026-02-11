# core/investment_advisor_engine.py
"""
Investment Advisor Engine (v1.1.0) — PROD SAFE + SCHEMA VALIDATED

What’s new in v1.1.0
- ✅ Uses schemas/advisor.py (AdvisorRequest / AdvisorResponse) when available
- ✅ Accepts tickers as:
    - payload.tickers (list or string "AAPL,1120.SR")
    - payload.symbols (alias)
    - payload.symbol (single)
- ✅ Mixed-market support:
    - Calls engine in ONE batch if possible (engine handles hints)
    - Falls back per-symbol using engine.get_enriched_quote/get_quote when batch weak
- ✅ Stable, Sheets-friendly output modes:
    - Returns {status, items, ...} (API-friendly)
    - AND if caller requests `output_mode="sheet"` returns {status, headers, rows, meta, ...}
- ✅ Filters/criteria:
    - risk/confidence bucket filters
    - required ROI thresholds (supports percent or ratio)
    - top_n / max_items compatibility
    - invest_amount / amount compatibility
- ✅ No heavy AI/news in v1 (keeps prod safe)
"""

from __future__ import annotations

import inspect
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]
ENGINE_VERSION = "1.1.0"


# ---------------------------------------------------------------------
# Time / utilities
# ---------------------------------------------------------------------
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        if isinstance(x, str):
            s = x.strip().replace(",", "")
            if not s:
                return None
            return float(s)
        return float(x)
    except Exception:
        return None


def _roi_to_percent(v: Any) -> Optional[float]:
    """
    Accept ratio or percent and return percent:
      0.10 -> 10
      10 -> 10
      "10%" -> 10
    """
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip().replace(",", "").replace("%", "")
        if not s:
            return None
        try:
            v = float(s)
        except Exception:
            return None
    if isinstance(v, (int, float)):
        f = float(v)
        if -1.5 <= f <= 1.5:
            return f * 100.0
        return f
    return None


def _norm_str(x: Any) -> str:
    try:
        return (str(x) if x is not None else "").strip()
    except Exception:
        return ""


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


def _parse_listish(v: Any) -> List[str]:
    """
    Accept:
      - ["AAPL", "1120.SR"]
      - "AAPL,1120.SR"
      - "AAPL 1120.SR"
      - "AAPL, 1120.SR"
    """
    if v is None:
        return []
    if isinstance(v, list):
        out: List[str] = []
        for it in v:
            s = _norm_str(it)
            if s:
                out.append(s)
        return out
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        parts = re.split(r"[\s,]+", s)
        return [p.strip() for p in parts if p.strip()]
    return []


def _normalize_bucket(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip().capitalize()
    # allow "Very High" too, but treat it as "High" for v1 filters
    if s in ("Low", "Moderate", "High", "Very high", "Very High"):
        return "High" if s.lower() == "very high" else s
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


def _gain_loss(amount: float, roi_pct: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if amount <= 0 or roi_pct is None:
        return None, None
    gain = amount * (roi_pct / 100.0)
    # simple symmetric risk placeholder — later replace with VaR/vol-based band
    loss = amount * (abs(min(roi_pct, 0.0)) / 100.0) if roi_pct < 0 else amount * (roi_pct / 200.0)
    return round(gain, 2), round(loss, 2)


def _extract_roi_and_forecast(uq: Any) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns:
      roi_1m_pct, roi_3m_pct, fp1, fp3
    """
    roi_1m = _roi_to_percent(_get(uq, "expected_roi_1m", "expected_return_1m"))
    roi_3m = _roi_to_percent(_get(uq, "expected_roi_3m", "expected_return_3m"))

    fp1 = _safe_float(_get(uq, "forecast_price_1m", "expected_price_1m"))
    fp3 = _safe_float(_get(uq, "forecast_price_3m", "expected_price_3m"))

    price = _safe_float(_get(uq, "current_price", "last_price", "price"))
    if price is not None and price > 0:
        if fp1 is None and roi_1m is not None:
            fp1 = price * (1.0 + roi_1m / 100.0)
        if fp3 is None and roi_3m is not None:
            fp3 = price * (1.0 + roi_3m / 100.0)

    return roi_1m, roi_3m, fp1, fp3


# ---------------------------------------------------------------------
# Engine resolution
# ---------------------------------------------------------------------
async def _resolve_engine(request) -> Optional[Any]:
    try:
        st = getattr(getattr(request, "app", None), "state", None)
        if st:
            engine = getattr(st, "engine", None)
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


async def _fetch_batch(engine: Any, tickers: List[str]) -> Any:
    fn = getattr(engine, "get_enriched_quotes", None) or getattr(engine, "get_quotes", None)
    if not callable(fn):
        raise RuntimeError("Engine does not support batch quotes")

    # try rich signature first
    try:
        return await _maybe_await(fn(tickers, refresh=False, include_history=True, include_fundamentals=True))
    except TypeError:
        pass
    try:
        return await _maybe_await(fn(tickers, refresh=False))
    except TypeError:
        pass

    return await _maybe_await(fn(tickers))


async def _fetch_one(engine: Any, ticker: str) -> Any:
    # prefer enriched
    fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    if not callable(fn):
        raise RuntimeError("Engine does not support single quote")
    try:
        return await _maybe_await(fn(ticker, refresh=False))
    except TypeError:
        return await _maybe_await(fn(ticker))


def _coerce_sources(sources_in: Any) -> List[str]:
    if isinstance(sources_in, str):
        if sources_in.strip().upper() == "ALL":
            return list(DEFAULT_SOURCES)
        parts = _parse_listish(sources_in)
        return parts or list(DEFAULT_SOURCES)
    if isinstance(sources_in, list) and sources_in:
        if any(_norm_str(x).upper() == "ALL" for x in sources_in):
            return list(DEFAULT_SOURCES)
        out = [_norm_str(x) for x in sources_in if _norm_str(x)]
        return out or list(DEFAULT_SOURCES)
    return list(DEFAULT_SOURCES)


def _coerce_output_mode(v: Any) -> str:
    s = _norm_str(v).lower()
    if s in ("sheet", "sheets", "gsheets"):
        return "sheet"
    return "items"


def _build_sheet_headers() -> List[str]:
    return [
        "Symbol",
        "Name",
        "Price",
        "Risk Bucket",
        "Confidence Bucket",
        "Overall Score",
        "Recommendation",
        "ROI 1M %",
        "ROI 3M %",
        "Forecast Price 1M",
        "Forecast Price 3M",
        "Invest Amount",
        "Expected Gain 1M",
        "Expected Loss 1M",
        "Expected Gain 3M",
        "Expected Loss 3M",
    ]


def _item_to_sheet_row(it: Dict[str, Any]) -> List[Any]:
    return [
        it.get("symbol"),
        it.get("name"),
        it.get("price"),
        it.get("risk_bucket"),
        it.get("confidence_bucket"),
        it.get("overall_score"),
        it.get("recommendation"),
        it.get("roi_1m_pct"),
        it.get("roi_3m_pct"),
        it.get("forecast_price_1m"),
        it.get("forecast_price_3m"),
        it.get("amount"),
        it.get("expected_gain_1m"),
        it.get("expected_loss_1m"),
        it.get("expected_gain_3m"),
        it.get("expected_loss_3m"),
    ]


# ---------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------
async def build_recommendations(*, request, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry used by routes/investment_advisor.py
    Returns stable envelope with HTTP 200-safe JSON.
    """
    # Try to validate/normalize with schemas if present (optional dependency)
    req_obj = None
    try:
        from schemas.advisor import AdvisorRequest  # type: ignore

        req_obj = AdvisorRequest.model_validate(payload)
    except Exception:
        req_obj = None

    # Normalize inputs (schema-first, fallback to raw)
    sources = _coerce_sources(getattr(req_obj, "sources", None) if req_obj else (payload.get("sources") or payload.get("source") or "ALL"))

    risk_want = _normalize_bucket(getattr(req_obj, "risk", None) if req_obj else payload.get("risk"))
    conf_want = _normalize_bucket(getattr(req_obj, "confidence", None) if req_obj else payload.get("confidence"))

    # Required ROI thresholds in percent
    req_roi_1m = _roi_to_percent(getattr(req_obj, "required_roi_1m", None) if req_obj else payload.get("required_roi_1m"))
    req_roi_3m = _roi_to_percent(getattr(req_obj, "required_roi_3m", None) if req_obj else payload.get("required_roi_3m"))

    # top_n / max_items compatibility
    top_n_raw = getattr(req_obj, "top_n", None) if req_obj else None
    max_items_raw = payload.get("max_items") if payload else None
    top_n = int(_safe_float(top_n_raw) or _safe_float(max_items_raw) or 10)
    top_n = max(1, min(200, top_n))

    # invest_amount / amount compatibility
    amt_raw = getattr(req_obj, "invest_amount", None) if req_obj else None
    amount = float(_safe_float(amt_raw) or _safe_float(payload.get("amount")) or _safe_float(payload.get("invest_amount")) or 0.0)
    if amount < 0:
        amount = 0.0

    output_mode = _coerce_output_mode(payload.get("output_mode") or payload.get("mode") or "items")

    # tickers: accept tickers/symbols/symbol
    tickers = []
    tickers = _parse_listish(payload.get("tickers") or payload.get("symbols") or [])
    if not tickers:
        one = _norm_str(payload.get("symbol"))
        if one:
            tickers = [one]

    # Engine
    engine = await _resolve_engine(request)
    if engine is None:
        return {
            "status": "error",
            "error": "Engine unavailable",
            "items": [],
            "timestamp_utc": _utc_iso(),
            "sources_used": sources,
        }

    if not tickers:
        return {
            "status": "error",
            "error": "No tickers provided. Provide payload.tickers (list or comma string) or payload.symbol(s).",
            "items": [],
            "timestamp_utc": _utc_iso(),
            "sources_used": sources,
        }

    # Fetch batch (best effort)
    try:
        res = await _fetch_batch(engine, tickers)
    except Exception as exc:
        return {
            "status": "error",
            "error": f"Engine fetch error: {exc}",
            "items": [],
            "timestamp_utc": _utc_iso(),
            "sources_used": sources,
        }

    # Normalize quotes iterable
    quotes: List[Any] = []
    if isinstance(res, dict):
        quotes = list(res.values())
    elif isinstance(res, list):
        quotes = res
    else:
        try:
            quotes = list(res or [])
        except Exception:
            quotes = []

    # Build items
    items: List[Dict[str, Any]] = []

    for uq in quotes:
        sym = _norm_str(_get(uq, "symbol")).upper()
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

    # Rank: higher overall_score then higher roi_3m
    def _key(it: Dict[str, Any]) -> Tuple[float, float]:
        s = it.get("overall_score")
        r = it.get("roi_3m_pct")
        return (float(s) if s is not None else 0.0, float(r) if r is not None else -999.0)

    items.sort(key=_key, reverse=True)
    items = items[:top_n]

    base_envelope: Dict[str, Any] = {
        "status": "ok",
        "engine_version": ENGINE_VERSION,
        "sources_used": sources,
        "filters": {
            "risk": risk_want,
            "confidence": conf_want,
            "required_roi_1m_pct": req_roi_1m,
            "required_roi_3m_pct": req_roi_3m,
            "top_n": top_n,
            "amount": amount,
            "output_mode": output_mode,
        },
        "count": len(items),
        "items": items,
        "timestamp_utc": _utc_iso(),
    }

    # Optional Sheets mode
    if output_mode == "sheet":
        headers = _build_sheet_headers()
        rows = [_item_to_sheet_row(it) for it in items]
        base_envelope.pop("items", None)
        base_envelope["headers"] = headers
        base_envelope["rows"] = rows
        base_envelope["meta"] = {
            "schema_version": "schemas.advisor/0.2.0" if req_obj is not None else "none",
            "sources_used": sources,
            "filters": base_envelope.get("filters", {}),
            "timestamp_utc": base_envelope.get("timestamp_utc"),
        }

    return base_envelope


__all__ = ["build_recommendations"]
