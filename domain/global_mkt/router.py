from __future__ import annotations

import os
import math
import hashlib
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, Depends, HTTPException, Body, Query

from config import get_settings


# =============================================================================
# Auth dependency (safe import)
# =============================================================================
try:
    from core.security import require_app_token  # type: ignore
except Exception:  # pragma: no cover
    def require_app_token() -> None:  # fallback (no-op)
        return None


# =============================================================================
# Dynamic model registry (safe import)
# =============================================================================
def _get_dynamic_model(page_id: str):
    """
    Your project may expose one of:
      - dynamic.registry.get_model(page_id)
      - dynamic.registry.get_page_model(page_id)
      - dynamic.registry.get_or_create_model(page_id)
    We support all to avoid breakage.
    """
    try:
        from dynamic import registry  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dynamic registry import failed: {e}")

    for fn_name in ("get_model", "get_page_model", "get_or_create_model"):
        fn = getattr(registry, fn_name, None)
        if callable(fn):
            return fn(page_id)

    raise HTTPException(status_code=500, detail="Dynamic registry has no supported model getter.")


# =============================================================================
# Optional storage hook (safe import)
# =============================================================================
def _append_snapshot_safe(page_id: str, region: str, schema_hash: str, rows: List[Dict[str, Any]]) -> str:
    """
    If you have a storage repository it will be used.
    Otherwise we keep in-memory (still works for /top7 right away).
    Returns a storage label.
    """
    # Try project repository (postgres or sheets)
    try:
        from storage.repository import append_snapshot  # type: ignore
        append_snapshot(page_id=page_id, region=region, schema_hash=schema_hash, payload=rows, metadata={})
        return "repository"
    except Exception:
        # No repository available -> memory
        _MEM_LATEST[(region, page_id)] = rows
        return "memory"


# In-memory latest cache (fallback)
_MEM_LATEST: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}


# =============================================================================
# Helpers
# =============================================================================
def _compute_schema_hash_from_yaml(page_id: str) -> str:
    """
    Compute sha256 from the YAML file bytes.
    This matches your Phase-1 schema evolution concept.
    """
    settings = get_settings()
    base_dir = getattr(settings, "dynamic_pages_dir", None) or os.getenv("DYNAMIC_PAGES_DIR", "config/dynamic_pages")
    path = os.path.join(base_dir, f"{page_id}.yaml")
    try:
        with open(path, "rb") as f:
            data = f.read()
        return hashlib.sha256(data).hexdigest()
    except Exception as e:
        # If file not found, return "unknown" but do not crash
        return f"unknown:{str(e)[:60]}"


def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
    """
    Accept:
      - JSON array of rows: [ {...}, {...} ]
      - Object with rows:  { "rows": [ {...} ] }
    """
    if isinstance(payload, list):
        if all(isinstance(x, dict) for x in payload):
            return payload  # type: ignore
        raise HTTPException(status_code=422, detail="Invalid body list. Expected list of JSON objects.")
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list) and all(isinstance(x, dict) for x in rows):
            return rows  # type: ignore
    raise HTTPException(status_code=422, detail="Invalid body. Send either a JSON array of rows, or {\"rows\": [...]}.")


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _safe_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        if isinstance(v, bool):
            return None
        return float(v)
    except Exception:
        return None


def _score_inverse_metric(v: Optional[float], good_low: float, bad_high: float) -> Optional[float]:
    """
    Lower is better (e.g., PE, PB, Debt/Equity)
    Returns 0..100
    """
    if v is None or not math.isfinite(v):
        return None
    if v <= good_low:
        return 100.0
    if v >= bad_high:
        return 0.0
    # linear interpolation
    return 100.0 * (bad_high - v) / (bad_high - good_low)


def _score_direct_metric(v: Optional[float], bad_low: float, good_high: float) -> Optional[float]:
    """
    Higher is better (e.g., ROE, margins, growth)
    Returns 0..100
    """
    if v is None or not math.isfinite(v):
        return None
    if v <= bad_low:
        return 0.0
    if v >= good_high:
        return 100.0
    return 100.0 * (v - bad_low) / (good_high - bad_low)


def _avg(scores: List[Optional[float]]) -> Optional[float]:
    xs = [x for x in scores if x is not None and math.isfinite(x)]
    if not xs:
        return None
    return float(sum(xs) / len(xs))


def _compute_upside(row: Dict[str, Any]) -> Optional[float]:
    px = _safe_float(row.get("last_price"))
    tp = _safe_float(row.get("analyst_target_price"))
    if px is None or tp is None or px == 0:
        return None
    return 100.0 * (tp - px) / px


def _compute_scores_and_reco(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Computes (if missing):
      - upside_to_target_percent
      - expected_roi_30d/90d/365d (simple phase-1 heuristic)
      - value_score, quality_score, momentum_score, risk_score, opportunity_score
      - recommendation, rationale
    """
    upside = _compute_upside(row)
    if row.get("upside_to_target_percent") in (None, "") and upside is not None:
        row["upside_to_target_percent"] = round(upside, 4)

    # Expected ROI heuristic:
    # - If upside is known, distribute it across horizons
    # - Otherwise, use momentum (return_1m/3m/1y) as proxy
    if upside is not None:
        if row.get("expected_roi_30d") in (None, ""):
            row["expected_roi_30d"] = round(0.25 * upside, 4)
        if row.get("expected_roi_90d") in (None, ""):
            row["expected_roi_90d"] = round(0.50 * upside, 4)
        if row.get("expected_roi_365d") in (None, ""):
            row["expected_roi_365d"] = round(1.00 * upside, 4)
    else:
        r1m = _safe_float(row.get("return_1m"))
        r3m = _safe_float(row.get("return_3m"))
        r1y = _safe_float(row.get("return_1y"))
        if row.get("expected_roi_30d") in (None, "") and r1m is not None:
            row["expected_roi_30d"] = round(r1m, 4)
        if row.get("expected_roi_90d") in (None, "") and r3m is not None:
            row["expected_roi_90d"] = round(r3m, 4)
        if row.get("expected_roi_365d") in (None, "") and r1y is not None:
            row["expected_roi_365d"] = round(r1y, 4)

    # -----------------------------
    # Value score
    # -----------------------------
    pe = _safe_float(row.get("pe_ttm"))
    pb = _safe_float(row.get("pb"))
    ps = _safe_float(row.get("ps"))
    ev_e = _safe_float(row.get("ev_ebitda"))
    divy = _safe_float(row.get("dividend_yield"))

    value_score = _avg([
        _score_inverse_metric(pe, good_low=8, bad_high=35),
        _score_inverse_metric(pb, good_low=1, bad_high=8),
        _score_inverse_metric(ps, good_low=1, bad_high=12),
        _score_inverse_metric(ev_e, good_low=6, bad_high=25),
        _score_direct_metric(divy, bad_low=0, good_high=6),
    ])
    if row.get("value_score") in (None, "") and value_score is not None:
        row["value_score"] = round(_clamp(value_score), 2)

    # -----------------------------
    # Quality score
    # -----------------------------
    roe = _safe_float(row.get("roe"))
    roa = _safe_float(row.get("roa"))
    gm = _safe_float(row.get("gross_margin"))
    om = _safe_float(row.get("operating_margin"))
    nm = _safe_float(row.get("net_margin"))
    rev_g = _safe_float(row.get("revenue_growth_yoy"))

    quality_score = _avg([
        _score_direct_metric(roe, bad_low=0, good_high=25),
        _score_direct_metric(roa, bad_low=0, good_high=12),
        _score_direct_metric(gm, bad_low=0, good_high=60),
        _score_direct_metric(om, bad_low=0, good_high=35),
        _score_direct_metric(nm, bad_low=0, good_high=25),
        _score_direct_metric(rev_g, bad_low=-10, good_high=25),
    ])
    if row.get("quality_score") in (None, "") and quality_score is not None:
        row["quality_score"] = round(_clamp(quality_score), 2)

    # -----------------------------
    # Momentum score
    # -----------------------------
    pos52 = _safe_float(row.get("position_52w_percent"))
    rsi = _safe_float(row.get("rsi_14"))
    r1w = _safe_float(row.get("return_1w"))
    r1m = _safe_float(row.get("return_1m"))
    r3m = _safe_float(row.get("return_3m"))

    # RSI target range ~ 45-70 is "healthy momentum" (avoid extremes)
    rsi_score = None
    if rsi is not None:
        if 45 <= rsi <= 70:
            rsi_score = 85.0
        elif 35 <= rsi < 45 or 70 < rsi <= 80:
            rsi_score = 60.0
        else:
            rsi_score = 35.0

    momentum_score = _avg([
        _score_direct_metric(pos52, bad_low=0, good_high=100),
        rsi_score,
        _score_direct_metric(r1w, bad_low=-8, good_high=8),
        _score_direct_metric(r1m, bad_low=-15, good_high=15),
        _score_direct_metric(r3m, bad_low=-25, good_high=25),
    ])
    if row.get("momentum_score") in (None, "") and momentum_score is not None:
        row["momentum_score"] = round(_clamp(momentum_score), 2)

    # -----------------------------
    # Risk score (higher = worse risk)
    # -----------------------------
    vol30 = _safe_float(row.get("volatility_30d"))
    dd1y = _safe_float(row.get("max_drawdown_1y"))
    beta = _safe_float(row.get("beta"))
    de = _safe_float(row.get("debt_to_equity"))

    # Convert risk metrics to "badness" 0..100
    risk_bad = _avg([
        _score_direct_metric(vol30, bad_low=10, good_high=60),    # higher vol => higher badness
        _score_direct_metric(abs(dd1y) if dd1y is not None else None, bad_low=10, good_high=60),
        _score_direct_metric(abs(beta) if beta is not None else None, bad_low=0.8, good_high=2.0),
        _score_direct_metric(de, bad_low=0.5, good_high=3.0),
    ])
    if risk_bad is not None:
        # Ensure it behaves as "risk_score" where higher is worse
        risk_score = _clamp(risk_bad)
        if row.get("risk_score") in (None, ""):
            row["risk_score"] = round(risk_score, 2)
    else:
        risk_score = None

    # -----------------------------
    # Opportunity score
    # -----------------------------
    vs = _safe_float(row.get("value_score"))
    qs = _safe_float(row.get("quality_score"))
    ms = _safe_float(row.get("momentum_score"))
    rs = _safe_float(row.get("risk_score"))  # higher=worse

    # Weighted blend with missing-values handling
    parts: List[Tuple[str, float, Optional[float]]] = [
        ("value", 0.30, vs),
        ("quality", 0.30, qs),
        ("momentum", 0.25, ms),
        ("risk_inv", 0.15, (100.0 - rs) if rs is not None else None),
    ]
    present = [(name, w, v) for (name, w, v) in parts if v is not None and math.isfinite(v)]
    if present:
        wsum = sum(w for _, w, _ in present)
        opp = sum((w / wsum) * float(v) for _, w, v in present)  # type: ignore[arg-type]
        opp = _clamp(opp)
        if row.get("opportunity_score") in (None, ""):
            row["opportunity_score"] = round(opp, 2)
    else:
        opp = None

    # -----------------------------
    # Recommendation + rationale
    # -----------------------------
    if row.get("recommendation") in (None, "") and opp is not None:
        if opp >= 75:
            reco = "BUY"
        elif opp >= 55:
            reco = "HOLD"
        else:
            reco = "WATCH"
        row["recommendation"] = reco

    if row.get("rationale") in (None, "") and opp is not None:
        clues = []
        if vs is not None and vs >= 70:
            clues.append("strong value")
        if qs is not None and qs >= 70:
            clues.append("high quality")
        if ms is not None and ms >= 70:
            clues.append("positive momentum")
        if rs is not None and rs >= 70:
            clues.append("elevated risk")
        if upside is not None and upside >= 10:
            clues.append("target upside")
        if not clues:
            clues = ["balanced signals"]
        row["rationale"] = (", ".join(clues))[:380]

    # Mark source/quality if not provided
    if row.get("data_source") in (None, ""):
        row["data_source"] = row.get("data_source") or "ingest"
    if row.get("data_quality") in (None, ""):
        row["data_quality"] = row.get("data_quality") or "mixed"

    return row


# =============================================================================
# Router
# =============================================================================
router = APIRouter(
    prefix="/api/v1/global",
    tags=["global"],
    dependencies=[Depends(require_app_token)],
)


@router.post("/ingest/{page_id}")
def ingest_global(
    page_id: str,
    payload: Any = Body(...),
) -> Dict[str, Any]:
    """
    Validates rows against the YAML-driven Pydantic model.
    Additionally computes analysis fields for:
      - page_02_market_summary_global (scores, expected ROI, recommendation)
    """
    rows_in = _extract_rows(payload)
    model = _get_dynamic_model(page_id)
    schema_hash = _compute_schema_hash_from_yaml(page_id)

    validated_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for idx, r in enumerate(rows_in):
        try:
            obj = model.model_validate(r)
            d = obj.model_dump()
            # Auto-compute analysis fields for the Global Market Summary page
            if page_id == "page_02_market_summary_global":
                d = _compute_scores_and_reco(d)
            validated_rows.append(d)
        except Exception as e:
            errors.append({"index": idx, "error": str(e)})

    accepted = len(validated_rows)
    rejected = len(errors)

    storage_label = _append_snapshot_safe(
        page_id=page_id,
        region="global",
        schema_hash=schema_hash,
        rows=validated_rows,
    )

    return {
        "ok": True,
        "storage": storage_label,
        "page_id": page_id,
        "region": "global",
        "schema_hash": schema_hash,
        "accepted_count": accepted,
        "rejected_count": rejected,
        "validated_rows": validated_rows,
        "errors": errors,
    }


@router.get("/top7")
def top7_opportunities(
    page_id: str = Query("page_02_market_summary_global"),
    limit: int = Query(7, ge=1, le=50),
    min_market_cap: Optional[float] = Query(None, ge=0),
) -> Dict[str, Any]:
    """
    Returns Top-N opportunities from the most recent ingested batch.
    Source preference:
      1) repository (if available)
      2) in-memory latest (fallback)
    """
    # Try repository recent() if available
    rows: List[Dict[str, Any]] = []
    try:
        from storage.repository import recent  # type: ignore
        rows = recent(page_id=page_id, region="global", limit=500) or []
        if isinstance(rows, dict) and "payload" in rows:
            rows = rows["payload"] or []
    except Exception:
        rows = _MEM_LATEST.get(("global", page_id), []) or []

    # Ensure scores exist
    cooked: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        rr = dict(r)
        if page_id == "page_02_market_summary_global":
            rr = _compute_scores_and_reco(rr)
        cooked.append(rr)

    # Filter
    if min_market_cap is not None:
        cooked = [r for r in cooked if (_safe_float(r.get("market_cap")) or 0.0) >= float(min_market_cap)]

    # Sort by opportunity_score desc
    cooked.sort(key=lambda r: (_safe_float(r.get("opportunity_score")) or -1e9), reverse=True)

    top = cooked[:limit]
    return {
        "ok": True,
        "page_id": page_id,
        "region": "global",
        "count_source_rows": len(cooked),
        "returned": len(top),
        "top": top,
    }
