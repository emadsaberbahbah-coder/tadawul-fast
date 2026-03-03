#!/usr/bin/env python3
# core/analysis/top10_selector.py
"""
================================================================================
Top 10 Selector — v1.1.0 (PHASE 6)
================================================================================
Must:
- read criteria (from criteria_model or Insights_Analysis sheet top-block)
- map period (days) -> ROI column (1M/3M/12M)
- filter across all eligible pages
- enforce: moderate/low risk + high confidence
- rank by expected ROI (tie-break by confidence / overall_score)

Startup-safe:
- No network I/O at import time
- Uses engine snapshots (cached) best-effort
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger("core.analysis.top10_selector")

RIYADH_TZ = timezone(timedelta(hours=3))


# -----------------------------------------------------------------------------
# Safe helpers
# -----------------------------------------------------------------------------
def _safe_str(x: Any, default: str = "") -> str:
    try:
        s = str(x).strip()
        return s if s else default
    except Exception:
        return default


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            f = float(x)
        else:
            s = str(x).strip().replace(",", "")
            if not s or s.lower() in {"na", "n/a", "null", "none"}:
                return default
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _as_fraction(x: Any) -> Optional[float]:
    """
    Accepts:
      - "12%" -> 0.12
      - 12    -> 0.12
      - 0.12  -> 0.12
    """
    f = _safe_float(x, None)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


def _norm_key(k: Any) -> str:
    return re.sub(r"\s+", " ", _safe_str(k).lower()).strip()


def _to_payload(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            return md(mode="python")  # pydantic v2
        except Exception:
            try:
                return md()  # type: ignore
            except Exception:
                return {}
    d = getattr(obj, "dict", None)
    if callable(d):
        try:
            return d()
        except Exception:
            return {}
    try:
        return dict(getattr(obj, "__dict__", {})) or {}
    except Exception:
        return {}


# -----------------------------------------------------------------------------
# Period mapping (days -> horizon)
# -----------------------------------------------------------------------------
def map_period_days_to_horizon(days: int) -> str:
    """
    Conservative mapping aligned with your spec (days are the truth):
      <= 45  -> 1M
      <= 135 -> 3M
      else   -> 12M
    """
    d = int(days or 0)
    if d <= 45:
        return "1m"
    if d <= 135:
        return "3m"
    return "12m"


def horizon_to_roi_key(h: str) -> str:
    h = (h or "").strip().lower()
    if h == "1m":
        return "expected_roi_1m"
    if h == "3m":
        return "expected_roi_3m"
    return "expected_roi_12m"


def horizon_to_forecast_price_key(h: str) -> str:
    h = (h or "").strip().lower()
    if h == "1m":
        return "forecast_price_1m"
    if h == "3m":
        return "forecast_price_3m"
    return "forecast_price_12m"


# -----------------------------------------------------------------------------
# Criteria (prefers core.analysis.criteria_model when available)
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class Criteria:
    pages_selected: List[str]
    invest_period_days: int

    # filters (fractions for ROI/confidence)
    min_expected_roi: Optional[float] = None
    max_risk_score: float = 60.0
    min_confidence: float = 0.70
    enforce_risk_confidence: bool = True

    # output
    top_n: int = 10
    enrich_final: bool = True

    def horizon(self) -> str:
        return map_period_days_to_horizon(self.invest_period_days)


def _criteria_from_dict(d: Dict[str, Any]) -> Criteria:
    pages = d.get("pages_selected") or d.get("pages") or d.get("selected_pages") or []
    if isinstance(pages, str):
        pages = [p.strip() for p in pages.replace(",", " ").split() if p.strip()]
    if not isinstance(pages, list) or not pages:
        pages = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"]

    invest_days = int(_safe_float(d.get("invest_period_days") or d.get("investment_period_days") or d.get("period_days") or 90, 90) or 90)
    invest_days = max(1, invest_days)

    min_roi = _as_fraction(d.get("min_expected_roi") or d.get("min_roi") or d.get("required_return") or d.get("required_roi"))
    max_risk = float(_safe_float(d.get("max_risk_score") or d.get("max_risk") or 60.0, 60.0) or 60.0)
    max_risk = _clamp(max_risk, 0.0, 100.0)

    min_conf = _safe_float(d.get("min_confidence") or d.get("min_ai_confidence") or d.get("min_confidence_score") or 0.70, 0.70)
    if min_conf is None:
        min_conf = 0.70
    min_conf = float(min_conf / 100.0) if min_conf > 1.5 else float(min_conf)
    min_conf = _clamp(min_conf, 0.0, 1.0)

    enforce = bool(d.get("enforce_risk_confidence", True))

    top_n = int(_safe_float(d.get("top_n") or 10, 10) or 10)
    top_n = max(1, min(50, top_n))

    enrich_final = bool(d.get("enrich_final", True))

    return Criteria(
        pages_selected=[str(p).strip() for p in pages if str(p).strip()],
        invest_period_days=invest_days,
        min_expected_roi=min_roi,
        max_risk_score=max_risk,
        min_confidence=min_conf,
        enforce_risk_confidence=enforce,
        top_n=top_n,
        enrich_final=enrich_final,
    )


def merge_criteria_overrides(base: Criteria, overrides: Dict[str, Any]) -> Criteria:
    d = {
        "pages_selected": list(base.pages_selected),
        "invest_period_days": base.invest_period_days,
        "min_expected_roi": base.min_expected_roi,
        "max_risk_score": base.max_risk_score,
        "min_confidence": base.min_confidence,
        "enforce_risk_confidence": base.enforce_risk_confidence,
        "top_n": base.top_n,
        "enrich_final": base.enrich_final,
    }
    d.update({k: v for k, v in (overrides or {}).items() if v is not None})
    return _criteria_from_dict(d)


def load_criteria_best_effort(engine: Any) -> Criteria:
    """
    Priority:
      1) core.analysis.criteria_model.read_criteria_from_insights(engine) (if exists)
      2) parse from Insights_Analysis snapshot top-block (heuristic)
      3) defaults
    """
    # 1) criteria_model helper
    try:
        from core.analysis.criteria_model import read_criteria_from_insights  # type: ignore

        c = read_criteria_from_insights(engine)
        d = c.model_dump(mode="python") if hasattr(c, "model_dump") else (c if isinstance(c, dict) else _to_payload(c))
        return _criteria_from_dict(d)
    except Exception:
        pass

    # 2) heuristic parse from snapshot
    try:
        snap = _get_snapshot(engine, "Insights_Analysis")
        d2 = _parse_insights_top_block(snap)
        if d2:
            return _criteria_from_dict(d2)
    except Exception:
        pass

    # 3) defaults
    return Criteria(
        pages_selected=["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"],
        invest_period_days=90,
        min_expected_roi=None,
        max_risk_score=60.0,
        min_confidence=0.70,
        enforce_risk_confidence=True,
        top_n=10,
        enrich_final=True,
    )


# -----------------------------------------------------------------------------
# Engine snapshots (best effort)
# -----------------------------------------------------------------------------
def _get_snapshot(engine: Any, page: str) -> Optional[Dict[str, Any]]:
    if engine is None:
        return None
    fn = getattr(engine, "get_cached_sheet_snapshot", None)
    if callable(fn):
        try:
            snap = fn(page)
            return snap if isinstance(snap, dict) else None
        except Exception:
            return None
    return None


def _get_multi_snapshots(engine: Any, pages: List[str]) -> Dict[str, Dict[str, Any]]:
    if engine is None:
        return {}
    fn = getattr(engine, "get_cached_multi_sheet_snapshots", None)
    if callable(fn):
        try:
            out = fn(pages)
            if isinstance(out, dict):
                return {str(k): v for k, v in out.items() if isinstance(v, dict)}
        except Exception:
            pass

    res: Dict[str, Dict[str, Any]] = {}
    for p in pages:
        snap = _get_snapshot(engine, p)
        if snap:
            res[p] = snap
    return res


def _parse_insights_top_block(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Heuristic: the top block is often key/value pairs in col A/B.
    We scan first ~50 rows.
    """
    if not snapshot or not isinstance(snapshot, dict):
        return {}
    rows = snapshot.get("rows")
    if not isinstance(rows, list) or not rows:
        return {}

    out: Dict[str, Any] = {}

    for r in rows[:50]:
        if not isinstance(r, (list, tuple)) or len(r) < 2:
            continue
        k = _norm_key(r[0])
        v = r[1] if len(r) > 1 else None
        if not k:
            continue

        if ("invest" in k and "period" in k) or k in {"investment period", "investment period (days)", "period (days)", "period_days"}:
            out["invest_period_days"] = v
        elif ("pages" in k and ("selected" in k or "include" in k)) or k in {"pages", "selected pages"}:
            out["pages_selected"] = v
        elif "min roi" in k or "min expected roi" in k or "required roi" in k or "required return" in k:
            out["min_expected_roi"] = v
        elif "max risk" in k or "risk max" in k:
            out["max_risk_score"] = v
        elif "min confidence" in k or "ai confidence" in k or "min ai confidence" in k:
            out["min_confidence"] = v
        elif ("top" in k and "n" in k) or "top10" in k or "top 10" in k:
            out["top_n"] = v

    return out


# -----------------------------------------------------------------------------
# Candidate extraction & ranking
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class Candidate:
    symbol: str
    source_page: str
    roi: float
    confidence: float
    overall_score: float
    risk_score: float
    row: Dict[str, Any]


def _normalize_symbol(sym: str) -> str:
    s = _safe_str(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    # if numeric KSA code, force .SR
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _eligible_pages(criteria: Criteria) -> List[str]:
    pages_in = [p for p in (criteria.pages_selected or []) if _safe_str(p)]
    if not pages_in:
        pages_in = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"]

    # normalize via page_catalog if available
    try:
        from core.sheets.page_catalog import normalize_page_name  # type: ignore

        tmp: List[str] = []
        for p in pages_in:
            try:
                tmp.append(normalize_page_name(p, allow_output_pages=True))
            except Exception:
                tmp.append(str(p).strip())
        pages_in = tmp
    except Exception:
        pages_in = [str(p).strip() for p in pages_in]

    blocked = {"Insights_Analysis", "Data_Dictionary", "Top_10_Investments"}
    pages_in = [p for p in pages_in if p and p not in blocked]

    # dedup preserve order
    seen: Set[str] = set()
    out: List[str] = []
    for p in pages_in:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _idx_map(headers: List[Any]) -> Dict[str, int]:
    m: Dict[str, int] = {}
    for i, h in enumerate(headers or []):
        k = _norm_key(h)
        if k and k not in m:
            m[k] = i
    return m


def _pick_idx(m: Dict[str, int], variants: Sequence[str]) -> Optional[int]:
    for v in variants:
        if not v:
            continue
        nk = _norm_key(v)
        if nk in m:
            return m[nk]
    return None


def _cell(row: Any, idx: Optional[int]) -> Any:
    if idx is None:
        return None
    if not isinstance(row, (list, tuple)):
        return None
    return row[idx] if idx < len(row) else None


def _risk_from_bucket(bucket: str) -> float:
    b = _safe_str(bucket).lower()
    if "very low" in b:
        return 25.0
    if "low" in b:
        return 35.0
    if "moderate" in b or "medium" in b:
        return 55.0
    if "very high" in b:
        return 90.0
    if "high" in b:
        return 75.0
    return 60.0


def _extract_candidate_from_row(
    *,
    row: List[Any],
    idx: Dict[str, int],
    page: str,
    horizon: str,
) -> Optional[Tuple[str, float, float, float, float, Dict[str, Any]]]:
    # symbol
    sym = _cell(row, _pick_idx(idx, ["symbol", "ticker", "code", "fund symbol", "fund code"]))
    sym = _normalize_symbol(_safe_str(sym))
    if not sym:
        return None

    # ROI
    roi_key = horizon_to_roi_key(horizon)
    roi_val = _cell(row, _pick_idx(idx, [
        roi_key,
        f"expected roi % ({horizon.upper()})",
        f"expected roi ({horizon.upper()})",
        "expected roi % (1m)" if horizon == "1m" else "",
        "expected roi % (3m)" if horizon == "3m" else "",
        "expected roi % (12m)" if horizon == "12m" else "",
        "expected roi 1m" if horizon == "1m" else "",
        "expected roi 3m" if horizon == "3m" else "",
        "expected roi 12m" if horizon == "12m" else "",
    ]))
    roi = _as_fraction(roi_val)
    if roi is None:
        return None

    # confidence
    conf_val = _cell(row, _pick_idx(idx, ["forecast_confidence", "forecast confidence", "confidence_score", "confidence score", "ai confidence", "confidence"]))
    conf = _safe_float(conf_val, 0.0) or 0.0
    conf = float(conf / 100.0) if conf > 1.5 else float(conf)
    conf = _clamp(conf, 0.0, 1.0)

    # overall_score
    os_val = _cell(row, _pick_idx(idx, ["overall_score", "overall score", "opportunity score", "score"]))
    overall = _safe_float(os_val, 0.0) or 0.0
    overall = float(overall * 100.0) if 0.0 < overall <= 1.5 else float(overall)
    overall = _clamp(overall, 0.0, 100.0)

    # risk_score
    rs_val = _cell(row, _pick_idx(idx, ["risk_score", "risk score"]))
    risk = _safe_float(rs_val, None)
    if risk is None:
        bucket = _cell(row, _pick_idx(idx, ["risk_bucket", "risk bucket", "risk"]))
        risk = _risk_from_bucket(_safe_str(bucket))
    else:
        risk = float(risk * 100.0) if 0.0 < risk <= 1.5 else float(risk)
        risk = _clamp(risk, 0.0, 100.0)

    # minimal raw row dict (canonical-ish keys)
    name = _cell(row, _pick_idx(idx, ["name", "company name", "fund name", "long name", "instrument"]))
    currency = _cell(row, _pick_idx(idx, ["currency", "ccy"]))
    exchange = _cell(row, _pick_idx(idx, ["exchange", "market", "region"]))
    price = _cell(row, _pick_idx(idx, ["current_price", "current price", "price", "last price", "last", "close", "nav", "nav per share"]))
    rec = _cell(row, _pick_idx(idx, ["recommendation", "action"]))

    fp_key = horizon_to_forecast_price_key(horizon)
    fp = _cell(row, _pick_idx(idx, [
        fp_key,
        f"forecast price ({horizon.upper()})",
        f"forecast {horizon.upper()}",
    ]))

    raw: Dict[str, Any] = {
        "symbol": sym,
        "name": name,
        "currency": currency,
        "exchange": exchange,
        "current_price": price,
        "recommendation": rec,
        roi_key: roi,
        fp_key: fp,
        "forecast_confidence": conf,
        "overall_score": overall,
        "risk_score": risk,
        "source_page": page,
    }
    return sym, float(roi), conf, overall, float(risk), raw


async def _enrich_top(engine: Any, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Optional enrichment for final Top-N to fill missing keys.
    Uses engine.get_enriched_quote if available.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if engine is None or not symbols:
        return out
    fn = getattr(engine, "get_enriched_quote", None)
    if not callable(fn):
        return out

    conc = 8
    try:
        conc = int(os.getenv("TOP10_ENRICH_CONCURRENCY", "8") or "8")
    except Exception:
        conc = 8
    sem = asyncio.Semaphore(max(3, min(20, conc)))

    async def one(sym: str) -> None:
        async with sem:
            try:
                r = fn(sym)
                if inspect.isawaitable(r):
                    r = await r
                out[sym] = _to_payload(r)
            except Exception:
                out[sym] = {"symbol": sym, "error": "enrich_failed"}

    await asyncio.gather(*(one(s) for s in symbols), return_exceptions=False)
    return out


async def select_top10(*, engine: Any, criteria: Criteria) -> Tuple[List[Candidate], Dict[str, Any]]:
    """
    Returns (candidates, meta).
    """
    t0 = time.time()
    horizon = criteria.horizon()
    pages = _eligible_pages(criteria)
    snaps = _get_multi_snapshots(engine, pages)

    total_rows = 0
    scanned_pages = 0
    candidates: List[Candidate] = []

    for page in pages:
        snap = snaps.get(page)
        if not snap:
            continue
        headers = snap.get("headers") or []
        rows = snap.get("rows") or []
        if not isinstance(headers, list) or not isinstance(rows, list) or not headers:
            continue

        scanned_pages += 1
        total_rows += len(rows)

        idx = _idx_map(headers)

        for r in rows:
            if not isinstance(r, (list, tuple)):
                continue
            extracted = _extract_candidate_from_row(row=list(r), idx=idx, page=page, horizon=horizon)
            if not extracted:
                continue
            sym, roi, conf, overall, risk, raw = extracted

            # hard filters
            if criteria.min_expected_roi is not None and roi < float(criteria.min_expected_roi):
                continue

            if criteria.enforce_risk_confidence:
                if risk > float(criteria.max_risk_score):
                    continue
                if conf < float(criteria.min_confidence):
                    continue

            candidates.append(
                Candidate(
                    symbol=sym,
                    source_page=page,
                    roi=roi,
                    confidence=conf,
                    overall_score=overall,
                    risk_score=risk,
                    row=raw,
                )
            )

    # Deduplicate by symbol (keep best by ROI, then confidence, then overall_score)
    best: Dict[str, Candidate] = {}
    for c in candidates:
        prev = best.get(c.symbol)
        if prev is None or (c.roi, c.confidence, c.overall_score) > (prev.roi, prev.confidence, prev.overall_score):
            best[c.symbol] = c

    deduped = list(best.values())
    deduped.sort(key=lambda c: (c.roi, c.confidence, c.overall_score), reverse=True)

    top = deduped[: max(1, int(criteria.top_n or 10))]

    # optional enrich top
    enrich_map: Dict[str, Dict[str, Any]] = {}
    if criteria.enrich_final and top:
        try:
            enrich_map = await _enrich_top(engine, [c.symbol for c in top])
        except Exception:
            enrich_map = {}

    for c in top:
        e = enrich_map.get(c.symbol)
        if isinstance(e, dict) and e:
            merged = dict(c.row)
            merged.update(e)  # enrichment wins
            c.row = merged  # ok with slots (same attribute)

    meta = {
        "horizon": horizon,
        "roi_key": horizon_to_roi_key(horizon),
        "pages_scanned": pages,
        "snapshots_found": sorted([p for p in pages if p in snaps]),
        "scanned_pages": scanned_pages,
        "rows_scanned": total_rows,
        "candidates": len(candidates),
        "deduped": len(deduped),
        "returned": len(top),
        "duration_ms": round((time.time() - t0) * 1000.0, 3),
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
    }
    return top, meta


# -----------------------------------------------------------------------------
# Output rows for Top_10_Investments (route will normalize to schema)
# -----------------------------------------------------------------------------
def build_top10_output_rows(candidates: List[Candidate], *, horizon: str) -> List[Dict[str, Any]]:
    roi_key = horizon_to_roi_key(horizon)
    fp_key = horizon_to_forecast_price_key(horizon)

    out: List[Dict[str, Any]] = []
    for i, c in enumerate(candidates, start=1):
        raw = c.row or {}
        row = {
            "rank": i,
            "source_page": c.source_page,
            "symbol": c.symbol,
            "name": raw.get("name") or raw.get("company_name"),
            "currency": raw.get("currency"),
            "exchange": raw.get("exchange"),
            "current_price": raw.get("current_price") or raw.get("price"),
            "risk_score": raw.get("risk_score") if raw.get("risk_score") is not None else c.risk_score,
            "overall_score": raw.get("overall_score") if raw.get("overall_score") is not None else c.overall_score,
            "forecast_confidence": raw.get("forecast_confidence") if raw.get("forecast_confidence") is not None else c.confidence,
            roi_key: raw.get(roi_key) if raw.get(roi_key) is not None else c.roi,
            fp_key: raw.get(fp_key),
            "recommendation": raw.get("recommendation"),
            "last_updated_utc": raw.get("last_updated_utc") or _now_utc_iso(),
            "last_updated_riyadh": raw.get("last_updated_riyadh") or _now_riyadh_iso(),
        }
        out.append(row)
    return out


__all__ = [
    "Criteria",
    "Candidate",
    "load_criteria_best_effort",
    "merge_criteria_overrides",
    "map_period_days_to_horizon",
    "horizon_to_roi_key",
    "horizon_to_forecast_price_key",
    "select_top10",
    "build_top10_output_rows",
]
