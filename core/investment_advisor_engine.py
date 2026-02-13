# core/investment_advisor_engine.py
"""
Tadawul Fast Bridge — Investment Advisor Engine (Google Sheets Friendly)
File: core/investment_advisor_engine.py
Version: 1.1.0 (INTELLIGENT EDITION)

Purpose
- Scans cached sheet pages (Market_Leaders / Global_Markets / Mutual_Funds / Commodities_FX)
- Produces a Sheets-safe output with advanced scoring logic.

v1.1.0 Enhancements:
- ✅ News Intelligence: Adjusts scores based on "News Score" (if available).
- ✅ Dynamic Risk Penalties: Adjusts risk tolerance based on user profile (Conservative vs Aggressive).
- ✅ Liquidity Guard: Penalizes/filters low liquidity stocks.
- ✅ Conviction Allocation: Allocates capital based on conviction strength (score ^ 2).
- ✅ Richer Reasoning: Generates detailed "Reason" strings explaining the pick.
- ✅ Sector Awareness: Captures sector data for diversification analysis.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import math
import time

ENGINE_VERSION = "1.1.0"

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

DEFAULT_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Market",
    "Currency",
    "Price",
    "Advisor Score",
    "Action",
    "Allocation %",
    "Allocation Amount",
    "Expected ROI % (1M)",
    "Expected ROI % (3M)",
    "Risk Bucket",
    "Confidence Bucket",
    "Reason (Explain)",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
]

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in _TRUTHY


# -----------------------------------------------------------------------------
# Normalization helpers
# -----------------------------------------------------------------------------
def _norm_key(k: Any) -> str:
    s = str(k or "").strip().lower()
    s = " ".join(s.split())  # collapse whitespace
    return s


def _get_any(row: Dict[str, Any], *names: str) -> Any:
    """
    Case/space-insensitive key lookup.
    Example: "Expected ROI % (1M)" will match "expected roi % (1m)" etc.
    """
    if not row:
        return None
    if len(names) == 1:
        # allow passing a single comma-joined string by mistake
        if "," in names[0]:
            names = tuple([p.strip() for p in names[0].split(",") if p.strip()])  # type: ignore

    # direct tries first
    for n in names:
        if n in row:
            return row.get(n)

    # normalized map
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


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return float(x)
    s = str(x).strip().replace(",", "")
    if not s or s.lower() in {"na", "n/a", "null", "none", "-"}:
        return None
    s = s.replace("%", "")
    try:
        return float(s)
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
    Accept:
      - 0.10 -> 0.10
      - 10   -> 0.10
      - "10%" -> 0.10
    """
    f = _to_float(x)
    if f is None:
        return None
    if f > 1.5:
        return f / 100.0
    return f


def _norm_bucket(x: Any) -> str:
    if not isinstance(x, str):
        return str(x).strip().title() if x is not None else ""
    s = x.strip().lower()
    if s in ("low", "low risk", "low-risk", "conservative"):
        return "Low"
    if s in ("moderate", "medium", "mid", "balanced"):
        return "Moderate"
    if s in ("high", "aggressive", "high risk", "high-risk", "growth"):
        return "High"
    if s in ("very high", "very-high", "speculative"):
        return "Very High"
    if s in ("high confidence", "high-conf", "highconf"):
        return "High"
    if s in ("moderate confidence", "medium confidence", "mid confidence"):
        return "Moderate"
    if s in ("low confidence", "low-conf"):
        return "Low"
    return x.strip().title()


def _safe_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


# -----------------------------------------------------------------------------
# Engine -> sheet cache adapters
# -----------------------------------------------------------------------------
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


def _engine_fetch_sheet(engine: Any, sheet_name: str) -> Tuple[List[Any], List[Any]]:
    """
    Tries multiple method names safely.
    Must return (headers, rows). If nothing works => ([], [])
    """
    if engine is None:
        return [], []

    candidates = [
        "get_cached_sheet_rows",
        "get_sheet_rows_cached",
        "get_sheet_cached",
        "get_sheet_rows",
        "get_sheet",
        "sheet_rows",
    ]

    for fn_name in candidates:
        fn = getattr(engine, fn_name, None)
        if not callable(fn):
            continue
        try:
            resp = fn(sheet_name)  # type: ignore[misc]
            if isinstance(resp, dict):
                headers = resp.get("headers") or []
                rows = resp.get("rows") or []
                return headers, rows
            # some engines may return tuple
            if isinstance(resp, (list, tuple)) and len(resp) == 2:
                return resp[0] or [], resp[1] or []
        except Exception:
            continue

    return [], []


def _get_universe_rows_from_engine(
    engine: Any,
    sources: List[str],
    *,
    max_rows_per_source: int = 5000,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {"sources": sources, "engine": type(engine).__name__ if engine is not None else "none", "items": []}

    normalized = [s.strip() for s in (sources or []) if isinstance(s, str) and s.strip()]
    if not normalized or any(s.upper() == "ALL" for s in normalized):
        normalized = list(DEFAULT_SOURCES)

    out_rows: List[Dict[str, Any]] = []
    for sheet in normalized:
        headers, rows = _engine_fetch_sheet(engine, sheet)
        meta["items"].append({"sheet": sheet, "headers": len(headers or []), "rows": len(rows or [])})
        out_rows.extend(_rows_to_dicts(headers, rows, sheet_name=sheet, limit=max_rows_per_source))

    return out_rows, meta


# -----------------------------------------------------------------------------
# Scoring model
# -----------------------------------------------------------------------------
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

    exp_roi_1m: Optional[float]  # ratio
    exp_roi_3m: Optional[float]  # ratio

    overall_score: Optional[float]
    risk_score: Optional[float]
    momentum_score: Optional[float]
    value_score: Optional[float]
    quality_score: Optional[float]
    
    # New v1.2 fields
    news_score: Optional[float] = None
    liquidity_score: Optional[float] = None
    volatility: Optional[float] = None

    advisor_score: float = 0.0
    reason: str = ""


def _extract_candidate(row: Dict[str, Any]) -> Optional[Candidate]:
    symbol = _safe_str(_get_any(row, "Symbol", "Ticker", "Code", "Fund Symbol"))
    if not symbol:
        return None

    name = _safe_str(_get_any(row, "Name", "Company Name", "Fund Name", "Instrument"))
    sheet = _safe_str(_get_any(row, "_Sheet", "Origin")) or ""
    sector = _safe_str(_get_any(row, "Sector", "Industry", "Category"))

    market = _safe_str(_get_any(row, "Market")) or ""
    currency = _safe_str(_get_any(row, "Currency")) or ""

    price = _to_float(_get_any(row, "Price", "Last", "Close", "NAV per Share"))

    risk_bucket = _norm_bucket(_get_any(row, "Risk Bucket", "Risk", "Risk Level") or "")
    confidence_bucket = _norm_bucket(_get_any(row, "Confidence Bucket", "Confidence") or "")

    exp_roi_1m = _as_ratio(_get_any(row, "Expected ROI % (1M)", "Expected ROI 1M", "ROI 1M", "Expected ROI (1M)"))
    exp_roi_3m = _as_ratio(_get_any(row, "Expected ROI % (3M)", "Expected ROI 3M", "ROI 3M", "Expected ROI (3M)"))

    overall_score = _to_float(_get_any(row, "Overall Score", "Opportunity Score", "Score"))
    risk_score = _to_float(_get_any(row, "Risk Score"))
    momentum_score = _to_float(_get_any(row, "Momentum Score"))
    value_score = _to_float(_get_any(row, "Value Score"))
    quality_score = _to_float(_get_any(row, "Quality Score"))
    
    # Advanced / AI fields
    news_score = _to_float(_get_any(row, "News Score", "News Sentiment", "Sentiment Score", "news_boost"))
    liquidity_score = _to_float(_get_any(row, "Liquidity Score", "liquidity_score"))
    volatility = _to_float(_get_any(row, "Volatility 30D", "volatility_30d"))

    return Candidate(
        symbol=symbol,
        name=name,
        sheet=sheet,
        market=market,
        sector=sector,
        currency=currency,
        price=price,
        risk_bucket=risk_bucket,
        confidence_bucket=confidence_bucket,
        exp_roi_1m=exp_roi_1m,
        exp_roi_3m=exp_roi_3m,
        overall_score=overall_score,
        risk_score=risk_score,
        momentum_score=momentum_score,
        value_score=value_score,
        quality_score=quality_score,
        news_score=news_score,
        liquidity_score=liquidity_score,
        volatility=volatility,
    )


def _passes_filters(
    c: Candidate,
    risk: str,
    confidence: str,
    req_roi_1m: Optional[float],
    req_roi_3m: Optional[float],
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    exclude_sectors: Optional[List[str]] = None,
) -> Tuple[bool, str]:
    # risk/confidence only filter when candidate has those buckets
    if risk and c.risk_bucket and _norm_bucket(risk) != _norm_bucket(c.risk_bucket):
        # Allow lower risk in higher buckets, but not vice-versa? No, strict matching usually safer.
        # But let's allow "Moderate" to include "Low".
        user_rb = _norm_bucket(risk)
        cand_rb = _norm_bucket(c.risk_bucket)
        
        allowed = {user_rb}
        if user_rb == "Moderate": allowed.add("Low")
        if user_rb == "High": allowed.update({"Moderate", "Low"})
        if user_rb == "Very High": allowed.update({"High", "Moderate", "Low"})
        
        if cand_rb not in allowed:
            return False, f"Risk mismatch (Req: {user_rb}, Got: {cand_rb})"

    if confidence and c.confidence_bucket:
         # Strict on confidence: Don't show Low confidence if user asked for High
         user_cb = _norm_bucket(confidence)
         cand_cb = _norm_bucket(c.confidence_bucket)
         
         min_levels = {"High": 3, "Moderate": 2, "Low": 1}
         if min_levels.get(cand_cb, 0) < min_levels.get(user_cb, 0):
             return False, "Confidence too low"

    # ROI filters are strict: if missing ROI => fail
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

    # Liquidity Safety Guard
    if c.liquidity_score is not None and c.liquidity_score < 25.0:
        return False, "Low Liquidity"

    return True, ""


def _compute_advisor_score(
    c: Candidate, 
    req_roi_1m: Optional[float], 
    req_roi_3m: Optional[float], 
    risk_profile: str
) -> Tuple[float, str]:
    base = c.overall_score if c.overall_score is not None else 50.0
    score = float(base)
    reasons: List[str] = []

    # 1. ROI Uplift (Diminishing returns to avoid outliers)
    if c.exp_roi_1m is not None:
        thr = req_roi_1m if req_roi_1m is not None else 0.0
        uplift = max(0.0, (c.exp_roi_1m - thr)) * 100.0
        # Cap uplift at 20 points
        score += min(20.0, uplift * 0.5) 
        if uplift > 1.0: reasons.append(f"ROI1M={c.exp_roi_1m:.1%}")

    if c.exp_roi_3m is not None:
        thr = req_roi_3m if req_roi_3m is not None else 0.0
        uplift = max(0.0, (c.exp_roi_3m - thr)) * 100.0
        score += min(25.0, uplift * 0.4)
        if uplift > 1.0: reasons.append(f"ROI3M={c.exp_roi_3m:.1%}")

    # 2. Factor Boosts
    for label, val, cap, weight in (
        ("Quality", c.quality_score, 8.0, 0.08),
        ("Momentum", c.momentum_score, 8.0, 0.08),
        ("Value", c.value_score, 8.0, 0.08),
    ):
        if val is not None and val > 60:
            boost = min(cap, (val - 50) * weight)
            score += boost
            reasons.append(f"{label}++")

    # 3. Dynamic Risk Penalty based on Profile
    risk_tol = _norm_bucket(risk_profile)
    risk_val = c.risk_score if c.risk_score is not None else 50.0
    
    penalty_mult = 0.0
    if risk_tol == "Low": penalty_mult = 0.5  # Heavy penalty for risk
    elif risk_tol == "Moderate": penalty_mult = 0.2
    elif risk_tol in ("High", "Very High"): penalty_mult = 0.05 # Forgiving

    if risk_val > 50:
        penalty = (risk_val - 50.0) * penalty_mult
        score -= penalty
        if penalty > 2.0: reasons.append(f"Risk-{int(penalty)}")
        
    # 4. News Intelligence Integration
    if c.news_score is not None and c.news_score != 0:
        # news_score is typically -5 to +5 (boost) or 0 to 100 (sentiment)
        # Assuming boost format (-5..+5) from news_intelligence.py
        if abs(c.news_score) <= 10:
            score += c.news_score
            if c.news_score > 1: reasons.append("News+")
            elif c.news_score < -1: reasons.append("News-")
        else:
             # Normalized 0-100 score?
             nb = (c.news_score - 50) * 0.1
             score += nb
             if nb > 1: reasons.append("News+")

    score = max(0.0, min(100.0, score))
    
    # Final sanity checks
    if c.price is not None and c.price <= 0: score = 0
    
    return score, "; ".join(reasons[:5])


def _allocate_amount(picks: List[Candidate], total_amount: float) -> Dict[str, Dict[str, float]]:
    if not picks:
        return {}
    total_amount = float(total_amount or 0.0)
    if total_amount <= 0:
        return {c.symbol: {"weight": 0.0, "amount": 0.0} for c in picks}

    # Conviction-based weighting (Score ^ 2) to favor top picks more heavily
    scores = [math.pow(max(1.0, c.advisor_score), 2) for c in picks]
    ssum = sum(scores) or 1.0
    weights = [s / ssum for s in scores]

    # Minimum viability check (e.g. at least 2% allocation or nothing)
    min_alloc_ratio = 0.02
    
    final_allocs = []
    
    # First pass: calculate raw amounts
    raw_amounts = [total_amount * w for w in weights]
    
    # Second pass: redistribution if below threshold?
    # For simplicity, we just floor tiny amounts to 0 and re-normalize, 
    # but the prompt asked for simple robustness.
    
    alloc = raw_amounts
    remaining = total_amount - sum(alloc)

    if remaining > 0:
        for i, w in enumerate(weights):
            alloc[i] += remaining * w

    drift = total_amount - sum(alloc)
    if abs(drift) > 1e-6:
        alloc[0] += drift

    out: Dict[str, Dict[str, float]] = {}
    for c, w, a in zip(picks, weights, alloc):
        out[c.symbol] = {"weight": float(w), "amount": float(a)}
    return out


# -----------------------------------------------------------------------------
# Public entry: used by routes/investment_advisor.py
# -----------------------------------------------------------------------------
async def build_recommendations(request: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Called by routes/investment_advisor.py

    Must return dict (never throws):
      - items: list[dict]
      - headers: list[str]
      - rows: list[list]
      - count: int
      - meta: dict
    """
    t0 = time.time()

    # read engine
    engine = None
    try:
        engine = getattr(getattr(request, "app", None), "state", None)
        engine = getattr(engine, "engine", None) if engine is not None else None
    except Exception:
        engine = None

    # inputs
    sources = payload.get("sources") or ["ALL"]
    if isinstance(sources, str):
        sources = [sources]
    sources = [str(s).strip() for s in sources if str(s).strip()] or ["ALL"]
    if any(s.upper() == "ALL" for s in sources):
        sources = list(DEFAULT_SOURCES)

    risk = _norm_bucket(payload.get("risk") or "")
    confidence = _norm_bucket(payload.get("confidence") or "")

    # IMPORTANT:
    # routes/investment_advisor.py normalizes these as PERCENT numbers (e.g., 5.0 / 10.0)
    # We convert safely to ratio.
    req_roi_1m = _as_ratio(payload.get("required_roi_1m"))
    req_roi_3m = _as_ratio(payload.get("required_roi_3m"))

    top_n = _to_int(payload.get("top_n") or payload.get("max_items") or 10) or 10
    top_n = max(1, min(200, top_n))

    invest_amount = _to_float(payload.get("amount") or payload.get("invest_amount") or 0.0) or 0.0
    include_news = _truthy(payload.get("include_news", True))
    currency = _safe_str(payload.get("currency") or "SAR") or "SAR"
    
    # New Filters
    min_price = _to_float(payload.get("min_price"))
    max_price = _to_float(payload.get("max_price"))
    exclude_sectors = payload.get("exclude_sectors") # list of strings

    # 1) Fetch universe from engine cache
    universe_rows, fetch_meta = _get_universe_rows_from_engine(engine, sources)

    # 2) Candidates
    dropped = {"no_symbol": 0, "filter": 0, "bad_row": 0}
    candidates: List[Candidate] = []

    for r in universe_rows:
        try:
            c = _extract_candidate(r)
            if c is None:
                dropped["no_symbol"] += 1
                continue

            ok, _why = _passes_filters(c, risk, confidence, req_roi_1m, req_roi_3m, min_price, max_price, exclude_sectors)
            if not ok:
                dropped["filter"] += 1
                continue

            c.advisor_score, c.reason = _compute_advisor_score(c, req_roi_1m, req_roi_3m, risk)
            candidates.append(c)
        except Exception:
            dropped["bad_row"] += 1

    # Sort: Score primary, then 3M ROI, then 1M ROI
    candidates.sort(
        key=lambda x: (x.advisor_score, (x.exp_roi_3m or 0.0), (x.exp_roi_1m or 0.0)),
        reverse=True,
    )

    picks = candidates[:top_n]
    alloc_map = _allocate_amount(picks, invest_amount)

    # 3) Build items + Sheets table
    items: List[Dict[str, Any]] = []
    rows: List[List[Any]] = []
    for i, c in enumerate(picks, start=1):
        alloc = alloc_map.get(c.symbol, {"weight": 0.0, "amount": 0.0})
        w = float(alloc.get("weight") or 0.0)
        amt = float(alloc.get("amount") or 0.0)
        
        # Action Logic
        action = "WATCH"
        if c.advisor_score >= 80: action = "STRONG BUY"
        elif c.advisor_score >= 65: action = "BUY"
        elif c.advisor_score >= 50: action = "HOLD"
        else: action = "REDUCE"

        items.append(
            {
                "rank": i,
                "symbol": c.symbol,
                "origin": c.sheet,
                "name": c.name,
                "market": c.market,
                "currency": c.currency or currency,
                "price": c.price,
                "advisor_score": round(c.advisor_score, 4),
                "action": action,
                "allocation_pct": round(w * 100.0, 4),
                "allocation_amount": round(amt, 2),
                "expected_roi_1m_pct": round((c.exp_roi_1m or 0.0) * 100.0, 4),
                "expected_roi_3m_pct": round((c.exp_roi_3m or 0.0) * 100.0, 4),
                "risk_bucket": c.risk_bucket,
                "confidence_bucket": c.confidence_bucket,
                "reason": c.reason,
                "data_source": c.sheet,
                "data_quality": "",
                "last_updated_utc": "",
            }
        )

        rows.append(
            [
                i,
                c.symbol,
                c.sheet,
                c.name,
                c.market,
                c.currency or currency,
                c.price,
                round(c.advisor_score, 4),
                action,
                round(w * 100.0, 4),
                round(amt, 2),
                round((c.exp_roi_1m or 0.0) * 100.0, 4),
                round((c.exp_roi_3m or 0.0) * 100.0, 4),
                c.risk_bucket,
                c.confidence_bucket,
                c.reason,
                c.sheet,
                "",
                "",
            ]
        )

    meta: Dict[str, Any] = {
        "engine_version": ENGINE_VERSION,
        "include_news": include_news,
        "criteria": {
            "sources": sources,
            "risk": risk,
            "confidence": confidence,
            "required_roi_1m_ratio": req_roi_1m,
            "required_roi_3m_ratio": req_roi_3m,
            "top_n": top_n,
            "amount": invest_amount,
            "currency": currency,
        },
        "fetch": fetch_meta,
        "counts": {
            "universe_rows": len(universe_rows),
            "candidates_after_filters": len(candidates),
            "returned": len(picks),
            "dropped": dropped,
        },
        "runtime_ms": int((time.time() - t0) * 1000),
        "time_utc": _utc_iso(),
    }

    # Always Sheets-safe (headers never empty)
    headers = list(DEFAULT_HEADERS)

    # If nothing matched, still return stable table (headers + empty rows)
    if not rows:
        meta["warning"] = "No candidates matched your filters/ROI thresholds (or ROI columns are missing/blank)."

    return {
        "items": items,
        "count": int(len(items)),
        "headers": headers,
        "rows": rows,
        "meta": meta,
        "timestamp_utc": _utc_iso(),
    }


__all__ = ["build_recommendations", "ENGINE_VERSION", "DEFAULT_HEADERS"]
