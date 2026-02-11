# core/investment_advisor.py
"""
Tadawul Fast Bridge — Investment Advisor Core
File: core/investment_advisor.py
Version: 1.0.0 (REVISED)

Contract (kept):
  run_investment_advisor(payload_dict) -> {"headers": [...], "rows": [...], "meta": {...}}

What was wrong in v0.1.0 (your file)
- It tried to call module-level functions in core.data_engine_v2 (get_cached_pages / get_cached_sheet_rows),
  but your running system initializes an ENGINE instance at request.app.state.engine.
  So the advisor universe scan often becomes 0 rows => 0 candidates => empty output.

What this revision fixes
- ✅ Pulls cached rows from the ENGINE instance (best-effort) rather than assuming module-level functions.
- ✅ Still supports the old module-level fallback if you later add those functions.
- ✅ Adds tolerant, case/space-insensitive header lookup so “Expected ROI % (1M)” variations still work.
- ✅ Produces stable headers + rows for GAS even if nothing matches.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math
import time


TT_ADVISOR_CORE_VERSION = "1.0.0"

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]


# -----------------------------------------------------------------------------
# Helpers: parsing + normalization
# -----------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v or "").strip().lower() in _TRUTHY


def _norm_key(k: Any) -> str:
    s = str(k or "").strip().lower()
    return " ".join(s.split())


def _get_any(row: Dict[str, Any], *names: str) -> Any:
    """
    Case/space-insensitive key lookup for dict rows coming from Sheets.
    """
    if not row:
        return None

    # direct hit
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


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return float(x)
    s = str(x).strip().replace(",", "")
    if s in ("", "NA", "N/A", "null", "None", "-", "none"):
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


def _norm_bucket(x: Any) -> str:
    if not isinstance(x, str):
        return str(x).strip().title() if x is not None else ""
    s = x.strip().lower()
    if s in ("low", "low risk", "low-risk", "conservative"):
        return "Low"
    if s in ("moderate", "medium", "mid", "balanced"):
        return "Moderate"
    if s in ("high", "aggressive", "high risk", "high-risk"):
        return "High"
    if s in ("very high", "very-high", "speculative"):
        return "Very High"
    # Confidence
    if s in ("high confidence", "high-conf", "highconf"):
        return "High"
    if s in ("moderate confidence", "medium confidence", "mid confidence"):
        return "Moderate"
    if s in ("low confidence", "low-conf"):
        return "Low"
    return x.strip().title()


def _as_ratio(x: Any) -> Optional[float]:
    """
    Accepts:
      - 0.10 => 10%
      - 10   => 10% (treated as percent)
      - "10%" => 10%
    Returns ratio (0.10) or None
    """
    f = _to_float(x)
    if f is None:
        return None
    if f > 1.5:
        return f / 100.0
    return f


def _safe_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


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


# -----------------------------------------------------------------------------
# Universe fetch (ENGINE-FIRST, module fallback)
# -----------------------------------------------------------------------------
def _engine_fetch_sheet(engine: Any, sheet_name: str) -> Tuple[List[Any], List[Any]]:
    """
    Best-effort adapter to whatever your DataEngine exposes.
    Returns (headers, rows). If nothing works => ([], []).
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
                return resp.get("headers") or [], resp.get("rows") or []
            if isinstance(resp, (list, tuple)) and len(resp) == 2:
                return resp[0] or [], resp[1] or []
        except Exception:
            continue

    return [], []


def _try_get_universe_rows(
    sources: List[str],
    *,
    max_rows_per_source: int = 5000,
    engine: Any = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns:
      - rows: list of dict rows (normalized)
      - meta: diagnostics

    Priority:
      1) Use provided ENGINE instance (request.app.state.engine)
      2) Fallback to module-level core.data_engine_v2 functions if they exist
    """
    meta: Dict[str, Any] = {"sources": sources, "engine": type(engine).__name__ if engine is not None else None, "items": []}

    # normalize sources
    normalized_sources = [s.strip() for s in (sources or []) if isinstance(s, str) and s.strip()]
    if not normalized_sources:
        normalized_sources = ["ALL"]
    if "ALL" in [s.upper() for s in normalized_sources]:
        normalized_sources = list(DEFAULT_SOURCES)

    out_rows: List[Dict[str, Any]] = []

    # Strategy 1: ENGINE instance (preferred)
    if engine is not None:
        for sheet in normalized_sources:
            headers, rows = _engine_fetch_sheet(engine, sheet)
            meta["items"].append({"sheet": sheet, "rows": len(rows or []), "headers": len(headers or [])})
            out_rows.extend(_rows_to_dicts(headers, rows, sheet_name=sheet, limit=max_rows_per_source))
        return out_rows, meta

    # Strategy 2: module fallback (if you add these later)
    try:
        import core.data_engine_v2 as de  # type: ignore
        meta["engine"] = getattr(de, "__name__", "core.data_engine_v2")

        if hasattr(de, "get_cached_pages"):
            resp = de.get_cached_pages(normalized_sources)  # type: ignore
            items = resp.get("items", []) if isinstance(resp, dict) else []
            for it in items:
                sheet = it.get("sheet") or it.get("name") or "UNKNOWN"
                headers = it.get("headers") or []
                rows = it.get("rows") or []
                meta["items"].append({"sheet": sheet, "rows": len(rows), "headers": len(headers)})
                out_rows.extend(_rows_to_dicts(headers, rows, sheet_name=str(sheet), limit=max_rows_per_source))
            return out_rows, meta

        if hasattr(de, "get_cached_sheet_rows"):
            for sheet in normalized_sources:
                resp = de.get_cached_sheet_rows(sheet)  # type: ignore
                if not isinstance(resp, dict):
                    continue
                headers = resp.get("headers") or []
                rows = resp.get("rows") or []
                meta["items"].append({"sheet": sheet, "rows": len(rows), "headers": len(headers)})
                out_rows.extend(_rows_to_dicts(headers, rows, sheet_name=sheet, limit=max_rows_per_source))
            return out_rows, meta
    except Exception as exc:
        raise RuntimeError("No engine provided and data_engine_v2 fallback unavailable.") from exc

    raise RuntimeError(
        "No engine provided and data_engine_v2 does not expose get_cached_pages(...) or get_cached_sheet_rows(...)."
    )


# -----------------------------------------------------------------------------
# Scoring model
# -----------------------------------------------------------------------------
@dataclass
class Candidate:
    symbol: str
    name: str
    sheet: str
    price: Optional[float]
    risk_bucket: str
    confidence_bucket: str

    forecast_1m: Optional[float]
    exp_roi_1m: Optional[float]  # ratio
    forecast_3m: Optional[float]
    exp_roi_3m: Optional[float]  # ratio

    overall_score: Optional[float]
    risk_score: Optional[float]
    momentum_score: Optional[float]
    value_score: Optional[float]
    quality_score: Optional[float]

    advisor_score: float = 0.0
    reason: str = ""


def _extract_candidate(row: Dict[str, Any]) -> Optional[Candidate]:
    symbol = _safe_str(_get_any(row, "Symbol", "Fund Symbol", "Ticker", "Code"))
    if not symbol:
        return None

    name = _safe_str(_get_any(row, "Name", "Company Name", "Fund Name", "Instrument"))
    sheet = _safe_str(_get_any(row, "_Sheet", "Origin")) or ""

    price = _to_float(_get_any(row, "Price", "Last", "Close", "NAV per Share"))

    risk_bucket = _norm_bucket(_get_any(row, "Risk Bucket", "Risk", "Risk Level") or "")
    confidence_bucket = _norm_bucket(_get_any(row, "Confidence Bucket", "Confidence") or "")

    forecast_1m = _to_float(_get_any(row, "Forecast Price (1M)", "Forecast Price 1M", "Forecast 1M"))
    exp_roi_1m = _as_ratio(_get_any(row, "Expected ROI % (1M)", "Expected ROI 1M", "ROI 1M"))

    forecast_3m = _to_float(_get_any(row, "Forecast Price (3M)", "Forecast Price 3M", "Forecast 3M"))
    exp_roi_3m = _as_ratio(_get_any(row, "Expected ROI % (3M)", "Expected ROI 3M", "ROI 3M"))

    overall_score = _to_float(_get_any(row, "Overall Score", "Opportunity Score", "Score"))
    risk_score = _to_float(_get_any(row, "Risk Score"))
    momentum_score = _to_float(_get_any(row, "Momentum Score"))
    value_score = _to_float(_get_any(row, "Value Score"))
    quality_score = _to_float(_get_any(row, "Quality Score"))

    return Candidate(
        symbol=symbol,
        name=name,
        sheet=sheet,
        price=price,
        risk_bucket=risk_bucket,
        confidence_bucket=confidence_bucket,
        forecast_1m=forecast_1m,
        exp_roi_1m=exp_roi_1m,
        forecast_3m=forecast_3m,
        exp_roi_3m=exp_roi_3m,
        overall_score=overall_score,
        risk_score=risk_score,
        momentum_score=momentum_score,
        value_score=value_score,
        quality_score=quality_score,
    )


def _passes_filters(
    c: Candidate,
    risk: str,
    confidence: str,
    req_roi_1m: Optional[float],
    req_roi_3m: Optional[float],
) -> Tuple[bool, str]:
    if risk and c.risk_bucket and _norm_bucket(risk) != _norm_bucket(c.risk_bucket):
        return False, "Risk filter"
    if confidence and c.confidence_bucket and _norm_bucket(confidence) != _norm_bucket(c.confidence_bucket):
        return False, "Confidence filter"

    if req_roi_1m is not None:
        if c.exp_roi_1m is None or c.exp_roi_1m < req_roi_1m:
            return False, "ROI 1M"
    if req_roi_3m is not None:
        if c.exp_roi_3m is None or c.exp_roi_3m < req_roi_3m:
            return False, "ROI 3M"

    return True, ""


def _compute_advisor_score(c: Candidate, req_roi_1m: Optional[float], req_roi_3m: Optional[float]) -> Tuple[float, str]:
    base = c.overall_score if c.overall_score is not None else 50.0
    score = float(base)
    reasons: List[str] = []

    if c.exp_roi_1m is not None:
        thr = req_roi_1m if req_roi_1m is not None else 0.0
        uplift = max(0.0, (c.exp_roi_1m - thr)) * 100.0
        score += min(15.0, uplift * 0.4)
        reasons.append(f"ROI1M={c.exp_roi_1m:.2%}")

    if c.exp_roi_3m is not None:
        thr = req_roi_3m if req_roi_3m is not None else 0.0
        uplift = max(0.0, (c.exp_roi_3m - thr)) * 100.0
        score += min(20.0, uplift * 0.35)
        reasons.append(f"ROI3M={c.exp_roi_3m:.2%}")

    for label, val, cap, weight in (
        ("Quality", c.quality_score, 6.0, 0.06),
        ("Momentum", c.momentum_score, 6.0, 0.06),
        ("Value", c.value_score, 6.0, 0.06),
    ):
        if val is not None:
            score += min(cap, max(0.0, val) * weight)
            reasons.append(f"{label}={val:.1f}")

    if c.risk_score is not None:
        penalty = max(0.0, c.risk_score - 50.0) * 0.10
        score -= min(12.0, penalty)
        reasons.append(f"RiskScore={c.risk_score:.1f}")

    score = max(0.0, min(100.0, score))
    return score, "; ".join(reasons[:6])


# -----------------------------------------------------------------------------
# Allocation
# -----------------------------------------------------------------------------
def _allocate_amount(ranked: List[Candidate], total_amount: float, top_n: int) -> List[Dict[str, Any]]:
    picks = ranked[: max(0, int(top_n))]
    if not picks:
        return []

    total_amount = float(total_amount or 0.0)
    if total_amount <= 0:
        return [{"symbol": c.symbol, "weight": 0.0, "amount": 0.0} for c in picks]

    scores = [max(1.0, c.advisor_score) for c in picks]
    ssum = sum(scores) or 1.0
    weights = [s / ssum for s in scores]

    min_amt = total_amount * 0.01
    if min_amt * len(picks) > total_amount:
        min_amt = 0.0

    alloc_amounts = [min_amt for _ in picks]
    remaining = total_amount - sum(alloc_amounts)

    if remaining > 0:
        for i, w in enumerate(weights):
            alloc_amounts[i] += remaining * w

    drift = total_amount - sum(alloc_amounts)
    if abs(drift) > 1e-6:
        alloc_amounts[0] += drift

    out = []
    for c, w, a in zip(picks, weights, alloc_amounts):
        out.append({"symbol": c.symbol, "weight": w, "amount": a})
    return out


# -----------------------------------------------------------------------------
# Public entry point (contract kept)
# -----------------------------------------------------------------------------
def run_investment_advisor(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    """
    Contract kept.
    Added optional param:
      engine: request.app.state.engine (pass from route/engine wrapper)

    payload keys supported:
      - sources (list or "ALL")
      - risk, confidence
      - required_roi_1m, required_roi_3m   (ratio or percent)
      - top_n
      - invest_amount
      - include_news
      - currency
    """
    t0 = time.time()

    sources = payload.get("sources") or ["ALL"]
    if isinstance(sources, str):
        sources = [sources]
    sources = [str(s).strip() for s in sources if str(s).strip()] or ["ALL"]

    risk = _norm_bucket(payload.get("risk") or "")
    confidence = _norm_bucket(payload.get("confidence") or "")

    req_roi_1m = _as_ratio(payload.get("required_roi_1m"))
    req_roi_3m = _as_ratio(payload.get("required_roi_3m"))

    top_n = _to_int(payload.get("top_n")) or 10
    top_n = max(1, min(200, top_n))

    invest_amount = _to_float(payload.get("invest_amount")) or 0.0
    currency = _safe_str(payload.get("currency") or "SAR")

    include_news = bool(_truthy(payload.get("include_news", True)))  # placeholder

    # 1) Fetch universe
    universe_rows, fetch_meta = _try_get_universe_rows(sources, engine=engine)

    # 2) Extract candidates
    candidates: List[Candidate] = []
    dropped = {"no_symbol": 0, "filter": 0, "bad_row": 0}
    for r in universe_rows:
        try:
            c = _extract_candidate(r)
            if c is None:
                dropped["no_symbol"] += 1
                continue

            ok, _reason = _passes_filters(c, risk, confidence, req_roi_1m, req_roi_3m)
            if not ok:
                dropped["filter"] += 1
                continue

            c.advisor_score, c.reason = _compute_advisor_score(c, req_roi_1m, req_roi_3m)
            candidates.append(c)
        except Exception:
            dropped["bad_row"] += 1

    # 3) Rank
    candidates.sort(key=lambda x: (x.advisor_score, (x.exp_roi_3m or 0.0), (x.exp_roi_1m or 0.0)), reverse=True)

    # 4) Allocate
    allocations = _allocate_amount(candidates, invest_amount, top_n)
    alloc_map = {a["symbol"]: a for a in allocations}

    # 5) Output (stable)
    headers = [
        "Rank",
        "Symbol",
        "Name",
        "Source Sheet",
        "Price",
        "Risk Bucket",
        "Confidence Bucket",
        "Advisor Score",
        "Expected ROI % (1M)",
        "Forecast Price (1M)",
        "Expected ROI % (3M)",
        "Forecast Price (3M)",
        "Weight",
        f"Allocated Amount ({currency})",
        f"Expected Gain/Loss 1M ({currency})",
        f"Expected Gain/Loss 3M ({currency})",
        "Reason (Explain)",
    ]

    rows: List[List[Any]] = []
    for i, c in enumerate(candidates[:top_n], start=1):
        alloc = alloc_map.get(c.symbol, {"weight": 0.0, "amount": 0.0})
        amt = float(alloc.get("amount", 0.0) or 0.0)
        w = float(alloc.get("weight", 0.0) or 0.0)

        gl_1m = amt * (c.exp_roi_1m if c.exp_roi_1m is not None else 0.0)
        gl_3m = amt * (c.exp_roi_3m if c.exp_roi_3m is not None else 0.0)

        rows.append(
            [
                i,
                c.symbol,
                c.name,
                c.sheet,
                c.price,
                c.risk_bucket,
                c.confidence_bucket,
                round(c.advisor_score, 2),
                (c.exp_roi_1m * 100.0) if c.exp_roi_1m is not None else None,
                c.forecast_1m,
                (c.exp_roi_3m * 100.0) if c.exp_roi_3m is not None else None,
                c.forecast_3m,
                round(w, 6),
                round(amt, 2),
                round(gl_1m, 2),
                round(gl_3m, 2),
                c.reason,
            ]
        )

    meta = {
        "ok": True,
        "core_version": TT_ADVISOR_CORE_VERSION,
        "include_news": include_news,
        "criteria": {
            "sources": sources,
            "risk": risk,
            "confidence": confidence,
            "required_roi_1m_ratio": req_roi_1m,
            "required_roi_3m_ratio": req_roi_3m,
            "top_n": top_n,
            "invest_amount": invest_amount,
            "currency": currency,
        },
        "fetch": fetch_meta,
        "counts": {
            "universe_rows": len(universe_rows),
            "candidates_after_filters": len(candidates),
            "dropped": dropped,
        },
        "runtime_ms": int((time.time() - t0) * 1000),
    }

    if not rows:
        meta["warning"] = "No candidates matched your filters/ROI thresholds (or ROI columns are missing/blank)."

    return {"headers": headers, "rows": rows, "meta": meta}


__all__ = ["run_investment_advisor", "TT_ADVISOR_CORE_VERSION"]
