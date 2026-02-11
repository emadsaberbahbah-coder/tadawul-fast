# core/investment_advisor.py
"""
Tadawul Fast Bridge — Investment Advisor Core (GOOGLE SHEETS SAFE)
File: core/investment_advisor.py
FULL REPLACEMENT — v1.1.0

Public contract:
  run_investment_advisor(payload_dict, engine=...) -> {"headers":[...], "rows":[...], "meta":{...}}

Key fixes
- ✅ ENGINE-FIRST universe scan:
    pulls snapshots from the ENGINE instance:
      - engine.get_cached_sheet_snapshot(sheet_name)
      - engine.get_cached_multi_sheet_snapshots([...])
  (compatible with your DataEngine v2.13+ snapshot cache)
- ✅ Robust candidate parsing across ALL pages:
    Market_Leaders / Global_Markets / Mutual_Funds / Commodities_FX
  using tolerant, case/space-insensitive header matching.
- ✅ ROI parsing hardened:
    handles percent (e.g., 12 or "12%") and ratio (0.12)
- ✅ Always returns stable headers for GAS (never empty)
- ✅ Never raises outward (returns ok/error in meta)
- ✅ Allocation + expected gain/loss uses ROI ratios

Notes
- This core expects sheet snapshots to be cached by your sheet routes:
    engine.set_cached_sheet_snapshot(sheet_name, headers, rows, meta)
  If no snapshots exist (cold start), you'll get zero candidates with a helpful meta error.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math
import time


TT_ADVISOR_CORE_VERSION = "1.1.0"

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok"}


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
      - "0.10" => 0.10
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
    if s in ("high", "high risk", "high-risk", "aggressive"):
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
    Adds a cached normalized-key map in row["_nmap"] for speed.
    """
    if not row:
        return None

    # direct hit first
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
# Universe fetch (ENGINE snapshot cache first)
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
    # fallback: loop get_cached_sheet_snapshot
    out2: Dict[str, Dict[str, Any]] = {}
    for s in sheet_names or []:
        snap = _engine_get_snapshot(engine, s)
        if snap:
            out2[str(s)] = snap
    return out2


def _try_get_universe_rows(
    sources: List[str],
    *,
    max_rows_per_source: int = 5000,
    engine: Any = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    """
    Returns:
      - rows: list[dict]
      - meta: diagnostics
      - err: optional string
    """
    meta: Dict[str, Any] = {
        "sources": sources,
        "engine": type(engine).__name__ if engine is not None else None,
        "items": [],
        "mode": "engine_snapshot",
    }

    # normalize sources
    normalized_sources = [s.strip() for s in (sources or []) if isinstance(s, str) and s.strip()]
    if not normalized_sources:
        normalized_sources = ["ALL"]
    if "ALL" in [s.upper() for s in normalized_sources]:
        normalized_sources = list(DEFAULT_SOURCES)

    if engine is None:
        return [], meta, "Missing engine instance (routes must pass engine=app.state.engine)."

    snaps = _engine_get_multi_snapshots(engine, normalized_sources)
    out_rows: List[Dict[str, Any]] = []

    for sheet in normalized_sources:
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


# ---------------------------------------------------------------------
# Scoring model
# ---------------------------------------------------------------------
@dataclass
class Candidate:
    symbol: str
    name: str
    sheet: str
    market: str
    currency: str
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
    # symbols across sheets
    symbol = _safe_str(_get_any(row, "Symbol", "Fund Symbol", "Ticker", "Code", "Fund Code"))
    if not symbol or symbol.upper() == "SYMBOL":
        return None

    name = _safe_str(_get_any(row, "Name", "Company Name", "Fund Name", "Instrument", "Long Name", "Short Name"))
    sheet = _safe_str(_get_any(row, "_Sheet", "Origin")) or ""

    market = _safe_str(_get_any(row, "Market")) or ("KSA" if symbol.endswith(".SR") else "GLOBAL")
    currency = _safe_str(_get_any(row, "Currency")) or ""

    # price across sheets
    price = _to_float(_get_any(row, "Price", "Last", "Close", "NAV per Share", "NAV", "Last Price"))

    risk_bucket = _norm_bucket(_get_any(row, "Risk Bucket", "Risk", "Risk Level") or "")
    confidence_bucket = _norm_bucket(_get_any(row, "Confidence Bucket", "Confidence") or "")

    # Forecast + ROI aliases (support both “Expected ROI % (1M)” and canonical engine fields if present)
    forecast_1m = _to_float(
        _get_any(
            row,
            "Forecast Price (1M)",
            "Forecast Price 1M",
            "Forecast 1M",
            "forecast_price_1m",
            "expected_price_1m",
        )
    )
    exp_roi_1m = _as_ratio(
        _get_any(
            row,
            "Expected ROI % (1M)",
            "Expected ROI 1M",
            "ROI 1M",
            "expected_roi_1m",
            "expected_return_1m",
        )
    )

    forecast_3m = _to_float(
        _get_any(
            row,
            "Forecast Price (3M)",
            "Forecast Price 3M",
            "Forecast 3M",
            "forecast_price_3m",
            "expected_price_3m",
        )
    )
    exp_roi_3m = _as_ratio(
        _get_any(
            row,
            "Expected ROI % (3M)",
            "Expected ROI 3M",
            "ROI 3M",
            "expected_roi_3m",
            "expected_return_3m",
        )
    )

    overall_score = _to_float(_get_any(row, "Overall Score", "Opportunity Score", "Score", "overall_score"))
    risk_score = _to_float(_get_any(row, "Risk Score", "risk_score"))
    momentum_score = _to_float(_get_any(row, "Momentum Score", "momentum_score"))
    value_score = _to_float(_get_any(row, "Value Score", "value_score"))
    quality_score = _to_float(_get_any(row, "Quality Score", "quality_score"))

    return Candidate(
        symbol=symbol.strip().upper(),
        name=name,
        sheet=sheet,
        market=market,
        currency=currency,
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


# ---------------------------------------------------------------------
# Allocation
# ---------------------------------------------------------------------
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

    # 1% minimum each (if feasible)
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


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------
def run_investment_advisor(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    """
    Router should call: run_investment_advisor(payload, engine=app.state.engine)
    """
    t0 = time.time()

    # stable headers for GAS (must never be empty)
    headers = [
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

    try:
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
        currency = _safe_str(payload.get("currency") or "SAR").upper() or "SAR"
        include_news = bool(_truthy(payload.get("include_news", True)))  # placeholder

        universe_rows, fetch_meta, fetch_err = _try_get_universe_rows(sources, engine=engine)

        candidates: List[Candidate] = []
        dropped = {"no_symbol": 0, "filter": 0, "bad_row": 0}

        for r in universe_rows:
            try:
                c = _extract_candidate(r)
                if c is None:
                    dropped["no_symbol"] += 1
                    continue

                ok, _why = _passes_filters(c, risk, confidence, req_roi_1m, req_roi_3m)
                if not ok:
                    dropped["filter"] += 1
                    continue

                c.advisor_score, c.reason = _compute_advisor_score(c, req_roi_1m, req_roi_3m)
                candidates.append(c)
            except Exception:
                dropped["bad_row"] += 1

        candidates.sort(
            key=lambda x: (x.advisor_score, (x.exp_roi_3m or 0.0), (x.exp_roi_1m or 0.0)),
            reverse=True,
        )

        allocations = _allocate_amount(candidates, invest_amount, top_n)
        alloc_map = {a["symbol"]: a for a in allocations}

        rows: List[List[Any]] = []
        for i, c in enumerate(candidates[:top_n], start=1):
            alloc = alloc_map.get(c.symbol, {"weight": 0.0, "amount": 0.0})
            w = float(alloc.get("weight", 0.0) or 0.0)
            amt = float(alloc.get("amount", 0.0) or 0.0)

            exp1 = (c.exp_roi_1m * 100.0) if c.exp_roi_1m is not None else None
            exp3 = (c.exp_roi_3m * 100.0) if c.exp_roi_3m is not None else None

            # very simple action label (can be improved later)
            action = "BUY" if c.advisor_score >= 75 else ("HOLD" if c.advisor_score >= 55 else "REDUCE")

            rows.append(
                [
                    i,
                    c.symbol,
                    c.sheet,
                    c.name,
                    c.market,
                    c.currency or currency,
                    c.price,
                    round(c.advisor_score, 2),
                    action,
                    round(w * 100.0, 2),
                    round(amt, 2),
                    exp1,
                    exp3,
                    c.risk_bucket,
                    c.confidence_bucket,
                    c.reason,
                    "engine_sheet_cache",
                    "FULL" if c.price is not None else "PARTIAL",
                    None,
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
                "returned_rows": len(rows),
                "dropped": dropped,
            },
            "runtime_ms": int((time.time() - t0) * 1000),
        }

        if fetch_err:
            meta["warning"] = fetch_err
        if not rows and not fetch_err:
            meta["warning"] = "No candidates matched your filters/ROI thresholds (or ROI columns are missing/blank)."

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
