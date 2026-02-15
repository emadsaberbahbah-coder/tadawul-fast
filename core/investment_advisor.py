"""
Tadawul Fast Bridge — Investment Advisor Core (GOOGLE SHEETS SAFE)
File: core/investment_advisor.py
FULL REPLACEMENT — v1.4.0 (ADVANCED + SNAPSHOT FALLBACK + GAIN/LOSS)

Public contract:
  run_investment_advisor(payload_dict, engine=...) -> {"headers":[...], "rows":[...], "meta":{...}}

v1.4.0 Enhancements:
- ✅ Snapshot First, Fallback Second:
    If engine snapshots are missing, it will use payload["tickers"/"symbols"] and call engine.get_enriched_quotes().
- ✅ Expected Gain/Loss:
    Adds Expected Gain/Loss 1M & 3M based on allocated amount and expected ROI.
- ✅ Full Forecast Columns:
    Includes forecast_price_1m/3m/12m + expected_roi_12m.
- ✅ Liquidity proxy:
    If liquidity_score missing, computes a proxy from value_traded and applies a guard/penalty.
- ✅ Dedup by symbol:
    If a symbol appears multiple times across sources, keeps the best candidate (highest score).
- ✅ Diversification controls:
    payload options: diversify_by_sector (bool), max_per_sector (int)
- ✅ Optional News hook:
    include_news + best-effort module call to enrich missing news scores (never hard-fails).

Notes:
- Expects sheet snapshots to be cached via engine.set_cached_sheet_snapshot().
- Router should pass engine=app.state.engine
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math
import time
from datetime import datetime, timezone, timedelta

TT_ADVISOR_CORE_VERSION = "1.4.0"

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v or "").strip().lower() in _TRUTHY


def _norm_key(k: Any) -> str:
    s = str(k or "").strip().lower()
    return " ".join(s.split())


def _safe_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    s = str(x).strip().replace(",", "")
    if s in ("", "NA", "N/A", "null", "None", "-", "—", "none"):
        return None
    s = s.replace("%", "")
    try:
        f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    f = _to_float(x)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def _as_ratio(x: Any) -> Optional[float]:
    """
    Accepts percent-ish or ratio and returns ratio:
      - 0.10 => 0.10
      - 10   => 0.10
      - "10%" => 0.10
    """
    f = _to_float(x)
    if f is None:
        return None
    if f > 1.5:
        return f / 100.0
    return f


def _norm_bucket(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    if not s:
        return ""
    # risk
    if s in ("low", "low risk", "low-risk", "conservative"):
        return "Low"
    if s in ("moderate", "medium", "mid", "balanced"):
        return "Moderate"
    if s in ("high", "high risk", "high-risk", "aggressive", "growth"):
        return "High"
    if s in ("very high", "very-high", "speculative"):
        return "Very High"
    # confidence
    if s in ("high confidence", "high-conf", "highconf"):
        return "High"
    if s in ("moderate confidence", "medium confidence", "mid confidence"):
        return "Moderate"
    if s in ("low confidence", "low-conf", "lowconf"):
        return "Low"
    return str(x).strip().title()


def _get_any(row: Dict[str, Any], *names: str) -> Any:
    """
    Case/space-insensitive key lookup for dict rows.
    Uses cached normalized-key map in row["_nmap"] for speed.
    """
    if not row:
        return None

    for n in names:
        if n in row:
            return row.get(n)

    nmap = row.get("_nmap")
    if not isinstance(nmap, dict):
        nmap = {}
        for k in row.keys():
            if k == "_nmap":
                continue
            nk = _norm_key(k)
            if nk and nk not in nmap:
                nmap[nk] = k
        row["_nmap"] = nmap

    for n in names:
        nk = _norm_key(n)
        k0 = nmap.get(nk)
        if k0 is not None:
            return row.get(k0)

    return None


def _rows_to_dicts(headers: List[Any], rows: List[Any], sheet_name: str, limit: int) -> List[Dict[str, Any]]:
    h = [str(x).strip() for x in (headers or [])]
    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows or []):
        if i >= limit:
            break
        if not isinstance(r, list):
            continue
        d = {h[j]: (r[j] if j < len(r) else None) for j in range(len(h))}
        d["_Sheet"] = sheet_name
        out.append(d)
    return out


# ---------------------------------------------------------------------
# Engine snapshot read
# ---------------------------------------------------------------------
def _engine_get_snapshot(engine: Any, sheet_name: str) -> Optional[Dict[str, Any]]:
    if engine is None:
        return None
    fn = getattr(engine, "get_cached_sheet_snapshot", None)
    if callable(fn):
        try:
            snap = fn(sheet_name)
            return snap if isinstance(snap, dict) else None
        except Exception:
            return None
    return None


def _engine_get_multi_snapshots(engine: Any, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
    if engine is None:
        return {}
    fn = getattr(engine, "get_cached_multi_sheet_snapshots", None)
    if callable(fn):
        try:
            out = fn(sheet_names)
            if isinstance(out, dict):
                return {str(k): v for k, v in out.items() if isinstance(v, dict)}
        except Exception:
            return {}
    out2: Dict[str, Dict[str, Any]] = {}
    for s in sheet_names or []:
        snap = _engine_get_snapshot(engine, s)
        if snap:
            out2[str(s)] = snap
    return out2


def _normalize_sources(sources: Any) -> List[str]:
    if sources is None:
        return ["ALL"]
    if isinstance(sources, str):
        sources = [sources]
    lst = [str(s).strip() for s in sources if str(s).strip()]
    if not lst:
        return ["ALL"]
    if any(str(x).strip().upper() == "ALL" for x in lst):
        return list(DEFAULT_SOURCES)
    return lst


def _try_get_universe_rows(
    sources: List[str],
    *,
    max_rows_per_source: int = 5000,
    engine: Any = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    meta: Dict[str, Any] = {
        "sources": sources,
        "engine": type(engine).__name__ if engine is not None else None,
        "items": [],
        "mode": "engine_snapshot",
    }

    if engine is None:
        return [], meta, "Missing engine instance (routes must pass engine=app.state.engine)."

    snaps = _engine_get_multi_snapshots(engine, sources)
    out_rows: List[Dict[str, Any]] = []

    for sheet in sources:
        snap = snaps.get(sheet) or _engine_get_snapshot(engine, sheet)
        if not snap:
            meta["items"].append({"sheet": sheet, "cached": False, "rows": 0, "headers": 0})
            continue

        headers = snap.get("headers") or []
        rows = snap.get("rows") or []
        meta["items"].append(
            {
                "sheet": sheet,
                "cached": True,
                "rows": len(rows) if isinstance(rows, list) else 0,
                "headers": len(headers) if isinstance(headers, list) else 0,
                "cached_at_utc": snap.get("cached_at_utc"),
            }
        )
        if isinstance(headers, list) and isinstance(rows, list):
            out_rows.extend(_rows_to_dicts(headers, rows, sheet_name=sheet, limit=max_rows_per_source))

    if not out_rows:
        return [], meta, (
            "No cached sheet snapshots found. Ensure your sheet routes call "
            "engine.set_cached_sheet_snapshot(sheet_name, headers, rows, meta) "
            "after fetching each page."
        )

    return out_rows, meta, None


async def _fallback_rows_from_payload_tickers(payload: Dict[str, Any], engine: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    """
    If snapshots are missing, but payload includes tickers/symbols, fetch quotes via engine.
    Returns rows in dict form (like sheet rows) with _Sheet="PAYLOAD".
    """
    meta = {"mode": "payload_tickers", "count": 0}
    if engine is None:
        return [], meta, "Missing engine."

    tickers = payload.get("tickers") or payload.get("symbols") or payload.get("symbol_list")
    if isinstance(tickers, str):
        # allow comma/space separated
        raw = tickers.replace(";", ",").replace("\n", " ").replace("\t", " ")
        parts = []
        for p in raw.replace(",", " ").split():
            if p.strip():
                parts.append(p.strip())
        tickers = parts

    if not isinstance(tickers, list) or not tickers:
        return [], meta, "No payload tickers provided."

    # cap to avoid extreme loads
    max_n = int(_to_int(payload.get("max_payload_tickers")) or 2500)
    tickers = [str(x).strip() for x in tickers if str(x).strip()][: max(1, min(10_000, max_n))]

    refresh_quotes = bool(_truthy(payload.get("refresh_quotes", False)))
    fields = payload.get("fields")  # optional slim response from engine

    fn = getattr(engine, "get_enriched_quotes", None)
    if not callable(fn):
        return [], meta, "Engine does not support get_enriched_quotes."

    try:
        quotes = await fn(tickers, refresh=refresh_quotes, fields=fields)
    except Exception as e:
        return [], meta, f"Engine quote fetch failed: {e}"

    rows: List[Dict[str, Any]] = []
    for q in quotes or []:
        if not isinstance(q, dict):
            continue
        d = dict(q)
        d["_Sheet"] = "PAYLOAD"
        # match sheet-like keys when possible
        d.setdefault("Symbol", d.get("symbol") or d.get("symbol_normalized"))
        d.setdefault("Name", d.get("name"))
        d.setdefault("Price", d.get("current_price"))
        d.setdefault("Market", d.get("market"))
        d.setdefault("Currency", d.get("currency"))
        d.setdefault("Overall Score", d.get("overall_score"))
        d.setdefault("Risk Score", d.get("risk_score"))
        d.setdefault("Momentum Score", d.get("momentum_score"))
        d.setdefault("Value Score", d.get("value_score"))
        d.setdefault("Quality Score", d.get("quality_score"))
        d.setdefault("Volatility 30D", d.get("volatility_30d"))
        d.setdefault("Trend Signal", d.get("trend_signal"))
        d.setdefault("Liquidity Score", d.get("liquidity_score"))
        d.setdefault("Data Source", d.get("data_source"))
        d.setdefault("Data Quality", d.get("data_quality"))
        d.setdefault("Last Updated (UTC)", d.get("last_updated_utc"))
        d.setdefault("Last Updated (Riyadh)", d.get("last_updated_riyadh"))
        # forecasts
        d.setdefault("Forecast Price (1M)", d.get("forecast_price_1m"))
        d.setdefault("Expected ROI % (1M)", d.get("expected_roi_1m"))
        d.setdefault("Forecast Price (3M)", d.get("forecast_price_3m"))
        d.setdefault("Expected ROI % (3M)", d.get("expected_roi_3m"))
        d.setdefault("Forecast Price (12M)", d.get("forecast_price_12m"))
        d.setdefault("Expected ROI % (12M)", d.get("expected_roi_12m"))
        # news
        d.setdefault("News Score", d.get("news_score"))
        rows.append(d)

    meta["count"] = len(rows)
    return rows, meta, None


# ---------------------------------------------------------------------
# Optional News batch hook (best-effort)
# ---------------------------------------------------------------------
def _try_batch_news_scores(symbols: List[str]) -> Dict[str, float]:
    """
    Best-effort import:
      core.news_intelligence.get_news_scores(symbols) -> dict
    Never raises.
    """
    try:
        from core import news_intelligence as ni  # type: ignore

        for fn_name in ("get_news_scores", "batch_news_scores", "score_symbols"):
            fn = getattr(ni, fn_name, None)
            if callable(fn):
                try:
                    out = fn(symbols)
                    if isinstance(out, dict):
                        res: Dict[str, float] = {}
                        for k, v in out.items():
                            f = _to_float(v)
                            if f is not None:
                                res[str(k).strip().upper()] = float(f)
                        return res
                except Exception:
                    return {}
        return {}
    except Exception:
        return {}


# ---------------------------------------------------------------------
# Candidate model
# ---------------------------------------------------------------------
@dataclass
class Candidate:
    symbol: str
    name: str
    sheet: str
    market: str
    sector: str
    currency: str
    price: Optional[float]

    risk_bucket: str
    confidence_bucket: str

    forecast_1m: Optional[float]
    exp_roi_1m: Optional[float]  # ratio
    forecast_3m: Optional[float]
    exp_roi_3m: Optional[float]  # ratio
    forecast_12m: Optional[float]
    exp_roi_12m: Optional[float]  # ratio

    overall_score: Optional[float]
    risk_score: Optional[float]
    momentum_score: Optional[float]
    value_score: Optional[float]
    quality_score: Optional[float]

    news_score: Optional[float] = None
    liquidity_score: Optional[float] = None
    value_traded: Optional[float] = None
    volume: Optional[float] = None
    volatility: Optional[float] = None
    trend_signal: str = ""

    recommendation: str = ""
    data_source: str = ""
    data_quality: str = ""
    last_updated_utc: str = ""
    last_updated_riyadh: str = ""

    advisor_score: float = 0.0
    reason: str = ""


def _extract_candidate(row: Dict[str, Any]) -> Optional[Candidate]:
    symbol = _safe_str(_get_any(row, "Symbol", "Fund Symbol", "Ticker", "Code", "Fund Code", "symbol"))
    if not symbol or symbol.upper() == "SYMBOL":
        return None

    name = _safe_str(_get_any(row, "Name", "Company Name", "Fund Name", "Instrument", "Long Name", "Short Name", "name"))
    sheet = _safe_str(_get_any(row, "_Sheet", "Origin")) or ""
    sector = _safe_str(_get_any(row, "Sector", "Industry", "Category"))

    market = _safe_str(_get_any(row, "Market", "market")) or ("KSA" if symbol.endswith(".SR") else "GLOBAL")
    currency = _safe_str(_get_any(row, "Currency", "currency")) or ""

    price = _to_float(_get_any(row, "Price", "Last", "Close", "NAV per Share", "NAV", "Last Price", "current_price"))

    risk_bucket = _norm_bucket(_get_any(row, "Risk Bucket", "Risk", "Risk Level") or "")
    confidence_bucket = _norm_bucket(_get_any(row, "Confidence Bucket", "Confidence") or "")

    forecast_1m = _to_float(_get_any(row, "Forecast Price (1M)", "Forecast Price 1M", "forecast_price_1m", "expected_price_1m"))
    exp_roi_1m = _as_ratio(_get_any(row, "Expected ROI % (1M)", "expected_roi_1m", "expected_return_1m"))

    forecast_3m = _to_float(_get_any(row, "Forecast Price (3M)", "Forecast Price 3M", "forecast_price_3m", "expected_price_3m"))
    exp_roi_3m = _as_ratio(_get_any(row, "Expected ROI % (3M)", "expected_roi_3m", "expected_return_3m"))

    forecast_12m = _to_float(_get_any(row, "Forecast Price (12M)", "Forecast Price 12M", "forecast_price_12m", "expected_price_12m"))
    exp_roi_12m = _as_ratio(_get_any(row, "Expected ROI % (12M)", "expected_roi_12m", "expected_return_12m"))

    overall_score = _to_float(_get_any(row, "Overall Score", "Opportunity Score", "Score", "overall_score"))
    risk_score = _to_float(_get_any(row, "Risk Score", "risk_score"))
    momentum_score = _to_float(_get_any(row, "Momentum Score", "momentum_score"))
    value_score = _to_float(_get_any(row, "Value Score", "value_score"))
    quality_score = _to_float(_get_any(row, "Quality Score", "quality_score"))

    news_score = _to_float(_get_any(row, "News Score", "News Sentiment", "Sentiment Score", "news_score"))
    liquidity_score = _to_float(_get_any(row, "Liquidity Score", "liquidity_score"))
    value_traded = _to_float(_get_any(row, "Value Traded", "value_traded"))
    volume = _to_float(_get_any(row, "Volume", "volume"))
    volatility = _to_float(_get_any(row, "Volatility (30D)", "Volatility 30D", "volatility_30d"))
    trend_signal = _safe_str(_get_any(row, "Trend Signal", "trend_signal")).upper()

    reco = _safe_str(_get_any(row, "Recommendation", "recommendation")).upper()
    if reco not in ("BUY", "HOLD", "REDUCE", "SELL", "STRONG BUY"):
        reco = ""

    data_source = _safe_str(_get_any(row, "Data Source", "data_source"))
    data_quality = _safe_str(_get_any(row, "Data Quality", "data_quality"))

    last_utc = _safe_str(_get_any(row, "Last Updated (UTC)", "last_updated_utc")) or ""
    last_riy = _safe_str(_get_any(row, "Last Updated (Riyadh)", "last_updated_riyadh")) or ""

    return Candidate(
        symbol=symbol.strip().upper(),
        name=name,
        sheet=sheet,
        market=market,
        sector=sector,
        currency=currency,
        price=price,
        risk_bucket=risk_bucket,
        confidence_bucket=confidence_bucket,
        forecast_1m=forecast_1m,
        exp_roi_1m=exp_roi_1m,
        forecast_3m=forecast_3m,
        exp_roi_3m=exp_roi_3m,
        forecast_12m=forecast_12m,
        exp_roi_12m=exp_roi_12m,
        overall_score=overall_score,
        risk_score=risk_score,
        momentum_score=momentum_score,
        value_score=value_score,
        quality_score=quality_score,
        news_score=news_score,
        liquidity_score=liquidity_score,
        value_traded=value_traded,
        volume=volume,
        volatility=volatility,
        trend_signal=trend_signal,
        recommendation=reco,
        data_source=data_source,
        data_quality=data_quality,
        last_updated_utc=last_utc,
        last_updated_riyadh=last_riy,
    )


# ---------------------------------------------------------------------
# Liquidity proxy
# ---------------------------------------------------------------------
def _liquidity_proxy_score(c: Candidate) -> Optional[float]:
    """
    If liquidity_score missing, derive from value_traded.
    Returns 0..100.
    """
    if c.liquidity_score is not None:
        return c.liquidity_score
    vt = c.value_traded
    if vt is None or vt <= 0:
        # fallback: use volume * price if present
        if c.volume is not None and c.price is not None and c.volume > 0 and c.price > 0:
            vt = c.volume * c.price
        else:
            return None
    try:
        lo = math.log10(50_000.0)
        hi = math.log10(10_000_000.0)
        x = math.log10(max(1.0, vt))
        score = (x - lo) / (hi - lo) * 100.0
        return max(0.0, min(100.0, score))
    except Exception:
        return None


# ---------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------
def _passes_filters(
    c: Candidate,
    risk: str,
    confidence: str,
    req_roi_1m: Optional[float],
    req_roi_3m: Optional[float],
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    exclude_sectors: Optional[List[str]] = None,
    min_liquidity_score: Optional[float] = 25.0,
) -> Tuple[bool, str]:
    if risk and c.risk_bucket:
        user_rb = _norm_bucket(risk)
        cand_rb = _norm_bucket(c.risk_bucket)
        allowed = {user_rb}
        if user_rb == "Moderate":
            allowed.add("Low")
        if user_rb == "High":
            allowed.update({"Moderate", "Low"})
        if user_rb == "Very High":
            allowed.update({"High", "Moderate", "Low"})
        if cand_rb and cand_rb not in allowed:
            return False, f"Risk mismatch (Req: {user_rb}, Got: {cand_rb})"

    if confidence and c.confidence_bucket:
        user_cb = _norm_bucket(confidence)
        cand_cb = _norm_bucket(c.confidence_bucket)
        min_levels = {"High": 3, "Moderate": 2, "Low": 1}
        if min_levels.get(cand_cb, 0) < min_levels.get(user_cb, 0):
            return False, "Confidence too low"

    if req_roi_1m is not None:
        if c.exp_roi_1m is None or c.exp_roi_1m < req_roi_1m:
            return False, "ROI 1M < Target"

    if req_roi_3m is not None:
        if c.exp_roi_3m is None or c.exp_roi_3m < req_roi_3m:
            return False, "ROI 3M < Target"

    if min_price is not None and c.price is not None and c.price < min_price:
        return False, "Price too low"
    if max_price is not None and c.price is not None and c.price > max_price:
        return False, "Price too high"

    if exclude_sectors and c.sector:
        if any(s.lower() in c.sector.lower() for s in exclude_sectors):
            return False, f"Sector excluded: {c.sector}"

    # Liquidity guard
    if min_liquidity_score is not None:
        ls = _liquidity_proxy_score(c)
        if ls is not None and ls < float(min_liquidity_score):
            return False, "Low Liquidity"

    return True, ""


# ---------------------------------------------------------------------
# Score computation
# ---------------------------------------------------------------------
def _compute_advisor_score(
    c: Candidate,
    req_roi_1m: Optional[float],
    req_roi_3m: Optional[float],
    risk_profile: str,
    include_news: bool,
) -> Tuple[float, str]:
    base = c.overall_score if c.overall_score is not None else 50.0
    score = float(base)
    reasons: List[str] = []

    # ROI uplift
    if c.exp_roi_1m is not None:
        thr = req_roi_1m if req_roi_1m is not None else 0.0
        uplift = max(0.0, (c.exp_roi_1m - thr)) * 100.0
        score += min(20.0, uplift * 0.5)
        if uplift > 1.0:
            reasons.append(f"ROI1M={c.exp_roi_1m:.1%}")

    if c.exp_roi_3m is not None:
        thr = req_roi_3m if req_roi_3m is not None else 0.0
        uplift = max(0.0, (c.exp_roi_3m - thr)) * 100.0
        score += min(25.0, uplift * 0.4)
        if uplift > 1.0:
            reasons.append(f"ROI3M={c.exp_roi_3m:.1%}")

    # Factor boosts
    for label, val, cap, weight in (
        ("Quality", c.quality_score, 8.0, 0.08),
        ("Momentum", c.momentum_score, 8.0, 0.08),
        ("Value", c.value_score, 8.0, 0.08),
    ):
        if val is not None and val > 60:
            boost = min(cap, (val - 50) * weight)
            score += boost
            reasons.append(f"{label}++")

    # Risk penalty by profile
    risk_tol = _norm_bucket(risk_profile)
    risk_val = c.risk_score if c.risk_score is not None else 50.0

    penalty_mult = 0.2
    if risk_tol == "Low":
        penalty_mult = 0.5
    elif risk_tol == "Moderate":
        penalty_mult = 0.2
    elif risk_tol in ("High", "Very High"):
        penalty_mult = 0.05

    if risk_val > 50:
        penalty = (risk_val - 50.0) * penalty_mult
        score -= penalty
        if penalty > 2.0:
            reasons.append(f"Risk-{int(penalty)}")

    # Liquidity bonus/penalty
    ls = _liquidity_proxy_score(c)
    if ls is not None:
        if ls >= 70:
            score += 2.0
            reasons.append("Liquidity+")
        elif ls < 35:
            score -= 3.0
            reasons.append("Liquidity-")

    # News integration
    if include_news and c.news_score is not None and c.news_score != 0:
        if abs(c.news_score) <= 10:
            score += c.news_score
            if c.news_score > 1:
                reasons.append("News+")
            elif c.news_score < -1:
                reasons.append("News-")
        else:
            nb = (c.news_score - 50) * 0.1
            score += nb
            if nb > 1:
                reasons.append("News+")

    # Trend
    if c.trend_signal == "UPTREND":
        score += 5.0
        reasons.append("TrendUp")
    elif c.trend_signal == "DOWNTREND":
        score -= 5.0
        reasons.append("TrendDown")

    score = max(0.0, min(100.0, score))
    if c.price is not None and c.price <= 0:
        score = 0.0

    return score, "; ".join(reasons[:6])


# ---------------------------------------------------------------------
# Allocation
# ---------------------------------------------------------------------
def _allocate_amount(ranked: List[Candidate], total_amount: float, top_n: int, min_alloc_ratio: float = 0.02) -> List[Dict[str, Any]]:
    picks = ranked[: max(0, int(top_n))]
    if not picks:
        return []

    total_amount = float(total_amount or 0.0)
    if total_amount <= 0:
        return [{"symbol": c.symbol, "weight": 0.0, "amount": 0.0} for c in picks]

    scores = [math.pow(max(1.0, c.advisor_score), 2) for c in picks]
    ssum = sum(scores) or 1.0
    weights = [s / ssum for s in scores]

    # floor small allocations then re-normalize
    floored = [w if w >= min_alloc_ratio else 0.0 for w in weights]
    s2 = sum(floored)
    if s2 <= 0:
        floored = weights
        s2 = sum(floored) or 1.0
    weights2 = [w / s2 for w in floored]

    out = []
    for c, w in zip(picks, weights2):
        out.append({"symbol": c.symbol, "weight": w, "amount": total_amount * w})
    return out


def _dedup_best(cands: List[Candidate]) -> List[Candidate]:
    best: Dict[str, Candidate] = {}
    for c in cands:
        k = c.symbol.upper().strip()
        if not k:
            continue
        prev = best.get(k)
        if prev is None:
            best[k] = c
        else:
            # keep higher advisor_score; tie-break by overall_score then ROI3m
            a = (c.advisor_score, c.overall_score or 0.0, c.exp_roi_3m or 0.0)
            b = (prev.advisor_score, prev.overall_score or 0.0, prev.exp_roi_3m or 0.0)
            if a > b:
                best[k] = c
    return list(best.values())


def _apply_diversification(cands: List[Candidate], max_per_sector: int) -> List[Candidate]:
    if max_per_sector <= 0:
        return cands
    counts: Dict[str, int] = {}
    out: List[Candidate] = []
    for c in cands:
        sec = (c.sector or "UNKNOWN").strip().upper() or "UNKNOWN"
        n = counts.get(sec, 0)
        if n >= max_per_sector:
            continue
        counts[sec] = n + 1
        out.append(c)
    return out


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------
def run_investment_advisor(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    """
    Router should call: run_investment_advisor(payload, engine=app.state.engine)
    """
    t0 = time.time()

    headers = [
        "Rank",
        "Symbol",
        "Origin",
        "Name",
        "Market",
        "Sector",
        "Currency",
        "Price",
        "Advisor Score",
        "Recommendation",
        "Action",
        "Risk Bucket",
        "Confidence Bucket",
        "Expected ROI % (1M)",
        "Forecast Price (1M)",
        "Expected ROI % (3M)",
        "Forecast Price (3M)",
        "Expected ROI % (12M)",
        "Forecast Price (12M)",
        "Allocation %",
        "Allocation Amount",
        "Expected Gain/Loss 1M",
        "Expected Gain/Loss 3M",
        "Liquidity Score",
        "Volatility (30D)",
        "Trend Signal",
        "News Score",
        "Reason (Explain)",
        "Data Source",
        "Data Quality",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
    ]

    try:
        sources = _normalize_sources(payload.get("sources") or ["ALL"])
        risk = _norm_bucket(payload.get("risk") or "")
        confidence = _norm_bucket(payload.get("confidence") or "")

        req_roi_1m = _as_ratio(payload.get("required_roi_1m"))
        req_roi_3m = _as_ratio(payload.get("required_roi_3m"))

        top_n = _to_int(payload.get("top_n")) or 10
        top_n = max(1, min(200, top_n))

        invest_amount = _to_float(payload.get("invest_amount")) or 0.0
        currency = _safe_str(payload.get("currency") or "SAR").upper() or "SAR"

        include_news = bool(_truthy(payload.get("include_news", True)))

        # Filters
        min_price = _to_float(payload.get("min_price"))
        max_price = _to_float(payload.get("max_price"))
        exclude_sectors = payload.get("exclude_sectors")
        if not isinstance(exclude_sectors, list):
            exclude_sectors = None

        min_liquidity_score = _to_float(payload.get("min_liquidity_score"))
        if min_liquidity_score is None:
            min_liquidity_score = 25.0

        diversify_by_sector = bool(_truthy(payload.get("diversify_by_sector", False)))
        max_per_sector = int(_to_int(payload.get("max_per_sector")) or 3)
        max_per_sector = max(1, min(20, max_per_sector))

        universe_rows, fetch_meta, fetch_err = _try_get_universe_rows(sources, engine=engine)

        # Fallback to payload tickers if snapshots missing
        fallback_meta = None
        if (not universe_rows) and bool(_truthy(payload.get("use_payload_tickers_when_missing_cache", True))):
            try:
                # run async fallback in sync function: use event loop safely
                # If already in an event loop (FastAPI async route), prefer calling this advisor from sync wrapper.
                import asyncio

                try:
                    loop = asyncio.get_running_loop()
                    # already running: create task and wait (not ideal in sync, but best-effort)
                    rows2, fallback_meta, err2 = loop.run_until_complete(_fallback_rows_from_payload_tickers(payload, engine))  # type: ignore
                except RuntimeError:
                    rows2, fallback_meta, err2 = asyncio.run(_fallback_rows_from_payload_tickers(payload, engine))
                if rows2:
                    universe_rows = rows2
                    fetch_meta = {"mode": "payload_tickers", "fallback": fallback_meta}
                    fetch_err = None
                elif fetch_err is None and err2:
                    fetch_err = err2
            except Exception:
                # keep original fetch_err
                pass

        candidates: List[Candidate] = []
        dropped = {"no_symbol": 0, "filter": 0, "bad_row": 0}

        for r in universe_rows:
            try:
                c = _extract_candidate(r)
                if c is None:
                    dropped["no_symbol"] += 1
                    continue

                ok, _why = _passes_filters(
                    c,
                    risk,
                    confidence,
                    req_roi_1m,
                    req_roi_3m,
                    min_price=min_price,
                    max_price=max_price,
                    exclude_sectors=exclude_sectors,
                    min_liquidity_score=min_liquidity_score,
                )
                if not ok:
                    dropped["filter"] += 1
                    continue

                c.advisor_score, c.reason = _compute_advisor_score(c, req_roi_1m, req_roi_3m, risk or "Moderate", include_news)
                candidates.append(c)
            except Exception:
                dropped["bad_row"] += 1

        # Best-of dedup
        candidates = _dedup_best(candidates)

        # Optional: if include_news and missing many news scores, try batch fill
        if include_news:
            missing = [c.symbol for c in candidates if c.news_score is None]
            if missing:
                news_map = _try_batch_news_scores(missing)
                if news_map:
                    for c in candidates:
                        if c.news_score is None:
                            v = news_map.get(c.symbol.upper())
                            if v is not None:
                                c.news_score = v

        # Sort: advisor_score primary, then ROI3m, then ROI1m
        candidates.sort(
            key=lambda x: (x.advisor_score, (x.exp_roi_3m or 0.0), (x.exp_roi_1m or 0.0)),
            reverse=True,
        )

        # Diversification (after sorting)
        if diversify_by_sector:
            candidates = _apply_diversification(candidates, max_per_sector=max_per_sector)

        allocations = _allocate_amount(candidates, invest_amount, top_n, min_alloc_ratio=float(_to_float(payload.get("min_alloc_ratio")) or 0.02))
        alloc_map = {a["symbol"]: a for a in allocations}

        # Time localization
        now_utc = datetime.now(timezone.utc).isoformat()
        tz_riyadh = timezone(timedelta(hours=3))
        now_riyadh = datetime.now(tz_riyadh).isoformat()

        rows: List[List[Any]] = []
        for i, c in enumerate(candidates[:top_n], start=1):
            alloc = alloc_map.get(c.symbol, {"weight": 0.0, "amount": 0.0})
            w = float(alloc.get("weight", 0.0) or 0.0)
            amt = float(alloc.get("amount", 0.0) or 0.0)

            # candidate ratios -> sheet % values
            exp1_pct = (c.exp_roi_1m * 100.0) if c.exp_roi_1m is not None else None
            exp3_pct = (c.exp_roi_3m * 100.0) if c.exp_roi_3m is not None else None
            exp12_pct = (c.exp_roi_12m * 100.0) if c.exp_roi_12m is not None else None

            # Expected gain/loss = allocated amount * ROI ratio
            gain1 = (amt * c.exp_roi_1m) if (c.exp_roi_1m is not None) else None
            gain3 = (amt * c.exp_roi_3m) if (c.exp_roi_3m is not None) else None

            # Action logic: prefer recommendation if available
            action = "WATCH"
            rec = (c.recommendation or "").strip().upper()
            if rec:
                if rec == "BUY":
                    action = "BUY"
                elif rec == "SELL":
                    action = "REDUCE"
                elif rec == "REDUCE":
                    action = "REDUCE"
                elif rec == "HOLD":
                    action = "HOLD"
                elif rec == "STRONG BUY":
                    action = "STRONG BUY"
            else:
                if c.advisor_score >= 80:
                    action = "STRONG BUY"
                elif c.advisor_score >= 65:
                    action = "BUY"
                elif c.advisor_score >= 50:
                    action = "HOLD"
                else:
                    action = "REDUCE"

            ls = _liquidity_proxy_score(c)

            rows.append(
                [
                    i,
                    c.symbol,
                    c.sheet,
                    c.name,
                    c.market,
                    c.sector,
                    c.currency or currency,
                    c.price,
                    round(c.advisor_score, 2),
                    rec or "",
                    action,
                    c.risk_bucket,
                    c.confidence_bucket,
                    exp1_pct,
                    c.forecast_1m,
                    exp3_pct,
                    c.forecast_3m,
                    exp12_pct,
                    c.forecast_12m,
                    round(w * 100.0, 2),
                    round(amt, 2),
                    round(gain1, 2) if isinstance(gain1, (int, float)) else gain1,
                    round(gain3, 2) if isinstance(gain3, (int, float)) else gain3,
                    round(ls, 2) if isinstance(ls, (int, float)) else ls,
                    c.volatility,
                    c.trend_signal,
                    c.news_score,
                    c.reason,
                    c.data_source or "engine_snapshot",
                    c.data_quality or ("FULL" if c.price is not None else "PARTIAL"),
                    c.last_updated_utc or now_utc,
                    c.last_updated_riyadh or now_riyadh,
                ]
            )

        meta = {
            "ok": True,
            "core_version": TT_ADVISOR_CORE_VERSION,
            "criteria": {
                "sources": sources,
                "risk": risk,
                "confidence": confidence,
                "required_roi_1m_ratio": req_roi_1m,
                "required_roi_3m_ratio": req_roi_3m,
                "top_n": top_n,
                "invest_amount": invest_amount,
                "currency": currency,
                "include_news": include_news,
                "min_price": min_price,
                "max_price": max_price,
                "exclude_sectors": exclude_sectors,
                "min_liquidity_score": min_liquidity_score,
                "diversify_by_sector": diversify_by_sector,
                "max_per_sector": max_per_sector,
            },
            "fetch": fetch_meta,
            "counts": {
                "universe_rows": len(universe_rows),
                "candidates_after_filters": len(candidates),
                "returned_rows": len(rows),
                "dropped": dropped,
            },
            "runtime_ms": int((time.time() - t0) * 1000),
        }

        if fetch_err:
            meta["warning"] = fetch_err
        if not rows and not fetch_err:
            meta["warning"] = "No candidates matched your filters (ROI/Risk/Liquidity) or universe is empty."

        return {"headers": headers, "rows": rows, "meta": meta}

    except Exception as exc:
        meta = {
            "ok": False,
            "core_version": TT_ADVISOR_CORE_VERSION,
            "error": str(exc),
            "runtime_ms": int((time.time() - t0) * 1000),
        }
        return {"headers": headers, "rows": [], "meta": meta}


__all__ = ["run_investment_advisor", "TT_ADVISOR_CORE_VERSION"]
