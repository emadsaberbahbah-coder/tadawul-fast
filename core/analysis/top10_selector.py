#!/usr/bin/env python3
# core/analysis/top10_selector.py
"""
================================================================================
Top 10 Selector — v2.1.0 (GREEN OUTPUT + PCT-FRACTION NORMALIZATION)
================================================================================
Tadawul Fast Bridge (TFB)

Why this revision (fix “❌ red X / not green” symptom)
- ✅ Ensures Top_10_Investments rows are “sheet-friendly”:
    - Always fills key decision fields when possible:
        current_price, expected_roi_* , forecast_confidence, recommendation, last_updated_riyadh
    - Normalizes ALL pct-typed fields to FRACTIONS (0.25 == 25%) based on schema dtype="pct"
      (prevents wrong formats / downstream completeness checks).
- ✅ Keeps LIVE quotes as the primary data source (no snapshot dependency).
- ✅ Never raises due to missing fields: safe defaults + warnings in meta.

Primary public APIs:
- select_top10_symbols(engine, criteria, limit=10) -> List[str]
- build_top10_rows(engine, criteria, limit=10) -> Dict payload {headers, keys, rows, meta}
- select_top10(engine, criteria) -> (candidates, meta)

Notes:
- Investment period is always in DAYS, mapped to 1M/3M/12M.
- Best-effort parsing: works with multiple engine method names/signatures.
- No network I/O at import time.

================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger("core.analysis.top10_selector")
logger.addHandler(logging.NullHandler())

TOP10_SELECTOR_VERSION = "2.1.0"
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


def _json_dumps_safe(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return str(obj)


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


# -----------------------------------------------------------------------------
# Period mapping (days -> horizon)  [spec: DAYS are truth]
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
# Criteria (schema-driven best-effort)
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

    # optional filters / rank tuning
    min_volume: Optional[float] = None
    use_liquidity_tiebreak: bool = True

    # output
    top_n: int = 10
    enrich_final: bool = True

    def horizon(self) -> str:
        return map_period_days_to_horizon(self.invest_period_days)


def _criteria_from_dict(d: Dict[str, Any]) -> Criteria:
    pages = d.get("pages_selected") or d.get("pages") or d.get("selected_pages") or []
    if isinstance(pages, str):
        pages = [p.strip() for p in pages.replace(";", ",").replace("|", ",").replace(",", " ").split() if p.strip()]
    if not isinstance(pages, list) or not pages:
        pages = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"]

    invest_days = int(
        _safe_float(
            d.get("invest_period_days") or d.get("investment_period_days") or d.get("period_days") or 90,
            90,
        )
        or 90
    )
    invest_days = max(1, invest_days)

    min_roi = _as_fraction(d.get("min_expected_roi") or d.get("min_roi") or d.get("required_return") or d.get("required_roi"))

    max_risk = float(_safe_float(d.get("max_risk_score") or d.get("max_risk") or 60.0, 60.0) or 60.0)
    max_risk = _clamp(max_risk, 0.0, 100.0)

    min_conf = _safe_float(
        d.get("min_confidence") or d.get("min_ai_confidence") or d.get("min_confidence_score") or 0.70,
        0.70,
    )
    if min_conf is None:
        min_conf = 0.70
    min_conf = float(min_conf / 100.0) if min_conf > 1.5 else float(min_conf)
    min_conf = _clamp(min_conf, 0.0, 1.0)

    enforce = bool(d.get("enforce_risk_confidence", True))

    top_n = int(_safe_float(d.get("top_n") or d.get("limit") or 10, 10) or 10)
    top_n = max(1, min(50, top_n))

    enrich_final = bool(d.get("enrich_final", True))

    min_vol = _safe_float(d.get("min_volume") or d.get("min_liquidity") or d.get("min_vol"), None)
    use_liq = bool(d.get("use_liquidity_tiebreak", True))

    return Criteria(
        pages_selected=[str(p).strip() for p in pages if str(p).strip()],
        invest_period_days=invest_days,
        min_expected_roi=min_roi,
        max_risk_score=max_risk,
        min_confidence=min_conf,
        enforce_risk_confidence=enforce,
        min_volume=min_vol,
        use_liquidity_tiebreak=use_liq,
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
        "min_volume": base.min_volume,
        "use_liquidity_tiebreak": base.use_liquidity_tiebreak,
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
    try:
        from core.analysis.criteria_model import read_criteria_from_insights  # type: ignore

        c = read_criteria_from_insights(engine)
        d = c.model_dump(mode="python") if hasattr(c, "model_dump") else (c if isinstance(c, dict) else _to_payload(c))
        return _criteria_from_dict(d)
    except Exception:
        pass

    try:
        snap = _get_snapshot(engine, "Insights_Analysis")
        d2 = _parse_insights_top_block(snap)
        if d2:
            return _criteria_from_dict(d2)
    except Exception:
        pass

    return Criteria(
        pages_selected=["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"],
        invest_period_days=90,
        min_expected_roi=None,
        max_risk_score=60.0,
        min_confidence=0.70,
        enforce_risk_confidence=True,
        min_volume=None,
        use_liquidity_tiebreak=True,
        top_n=10,
        enrich_final=True,
    )


# -----------------------------------------------------------------------------
# Page selection / normalization
# -----------------------------------------------------------------------------
def _normalize_symbol(sym: str) -> str:
    s = _safe_str(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _eligible_pages(criteria: Criteria) -> List[str]:
    pages_in = [p for p in (criteria.pages_selected or []) if _safe_str(p)]
    if not pages_in:
        pages_in = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"]

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

    blocked = {"Insights_Analysis", "Data_Dictionary", "Top_10_Investments", "Top10_Investments"}
    pages_in = [p for p in pages_in if p and p not in blocked]

    seen: Set[str] = set()
    out: List[str] = []
    for p in pages_in:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


# -----------------------------------------------------------------------------
# Engine access helpers (LIVE preferred; snapshots fallback only)
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


async def _get_universe_symbols_live(engine: Any, page: str, *, limit: int = 5000) -> List[str]:
    if engine is None:
        return []

    candidates = [
        ("get_page_symbols", dict(page=page, limit=limit)),
        ("get_page_symbols", dict(sheet=page, limit=limit)),
        ("get_sheet_symbols", dict(sheet=page, limit=limit)),
        ("list_symbols_for_page", dict(page=page, limit=limit)),
        ("list_symbols", dict(sheet=page, limit=limit)),
        ("get_symbols", dict(sheet=page, limit=limit)),
    ]

    for name, kwargs in candidates:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        try:
            out = fn(**kwargs)
            out = await _maybe_await(out)
        except TypeError:
            try:
                out = fn(page)  # type: ignore
                out = await _maybe_await(out)
            except Exception:
                continue
        except Exception:
            continue

        if isinstance(out, (list, tuple)):
            syms = [_normalize_symbol(_safe_str(x)) for x in out]
            return [s for s in syms if s]
        if isinstance(out, dict) and isinstance(out.get("symbols"), (list, tuple)):
            syms = [_normalize_symbol(_safe_str(x)) for x in out["symbols"]]
            return [s for s in syms if s]

    fn2 = getattr(engine, "symbols_reader", None)
    if fn2 is not None and hasattr(fn2, "get_symbols_for_sheet"):
        try:
            out = fn2.get_symbols_for_sheet(page)  # type: ignore
            out = await _maybe_await(out)
            if isinstance(out, (list, tuple)):
                syms = [_normalize_symbol(_safe_str(x)) for x in out]
                return [s for s in syms if s]
        except Exception:
            pass

    return []


async def _fetch_quotes_map(engine: Any, symbols: List[str], *, mode: str = "") -> Dict[str, Dict[str, Any]]:
    if not engine or not symbols:
        return {}

    fn = getattr(engine, "get_enriched_quotes_batch", None)
    if callable(fn):
        try:
            try:
                res = fn(symbols, mode=mode or "")
            except TypeError:
                res = fn(symbols)
            res = await _maybe_await(res)
            if isinstance(res, dict):
                out: Dict[str, Dict[str, Any]] = {}
                for s in symbols:
                    out[s] = _to_payload(res.get(s))
                return out
        except Exception:
            pass

    fn2 = getattr(engine, "get_enriched_quotes", None)
    if callable(fn2):
        try:
            res = fn2(symbols)
            res = await _maybe_await(res)
            if isinstance(res, list):
                out2: Dict[str, Dict[str, Any]] = {}
                for s, v in zip(symbols, res):
                    out2[s] = _to_payload(v)
                return out2
        except Exception:
            pass

    out3: Dict[str, Dict[str, Any]] = {}
    fn3 = getattr(engine, "get_enriched_quote_dict", None)
    fn4 = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    for s in symbols:
        try:
            if callable(fn3):
                out3[s] = _to_payload(await _maybe_await(fn3(s)))
            elif callable(fn4):
                out3[s] = _to_payload(await _maybe_await(fn4(s)))
            else:
                out3[s] = {"symbol": s, "warnings": "engine_missing_quote_methods"}
        except Exception as e:
            out3[s] = {"symbol": s, "warnings": f"quote_error: {e}"}
    return out3


# -----------------------------------------------------------------------------
# Snapshot parsing (fallback only)
# -----------------------------------------------------------------------------
def _parse_insights_top_block(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
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
# Candidate model + extraction (row dicts from LIVE quotes)
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class Candidate:
    symbol: str
    source_page: str
    roi: float  # FRACTION
    confidence: float  # 0..1
    overall_score: float  # 0..100
    risk_score: float  # 0..100
    volume: float
    row: Dict[str, Any]


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


def _get_confidence(d: Dict[str, Any]) -> float:
    v = d.get("forecast_confidence", None)
    if v is None:
        v = d.get("confidence_score", None)
    f = _safe_float(v, 0.0) or 0.0
    f = float(f / 100.0) if f > 1.5 else float(f)
    return _clamp(f, 0.0, 1.0)


def _get_overall_score(d: Dict[str, Any]) -> float:
    v = d.get("overall_score", None)
    if v is None:
        v = d.get("opportunity_score", None)
    f = _safe_float(v, 0.0) or 0.0
    f = float(f * 100.0) if 0.0 < f <= 1.5 else float(f)
    return _clamp(f, 0.0, 100.0)


def _get_risk_score(d: Dict[str, Any]) -> float:
    v = d.get("risk_score", None)
    f = _safe_float(v, None)
    if f is None:
        return _risk_from_bucket(_safe_str(d.get("risk_bucket", "")))
    f = float(f * 100.0) if 0.0 < f <= 1.5 else float(f)
    return _clamp(f, 0.0, 100.0)


def _get_volume(d: Dict[str, Any]) -> float:
    v = d.get("volume", None)
    f = _safe_float(v, 0.0) or 0.0
    return max(0.0, float(f))


def _extract_candidate_from_quote(*, sym: str, page: str, quote: Dict[str, Any], horizon: str) -> Optional[Candidate]:
    roi_key = horizon_to_roi_key(horizon)

    # ROI must exist. Normalize to FRACTION.
    roi = _as_fraction(quote.get(roi_key))
    if roi is None:
        return None

    conf = _get_confidence(quote)
    overall = _get_overall_score(quote)
    risk = _get_risk_score(quote)
    vol = _get_volume(quote)

    row = dict(quote)
    row["symbol"] = sym
    row["source_page"] = page

    return Candidate(
        symbol=sym,
        source_page=page,
        roi=float(roi),
        confidence=float(conf),
        overall_score=float(overall),
        risk_score=float(risk),
        volume=float(vol),
        row=row,
    )


# -----------------------------------------------------------------------------
# Ranking logic (ROI primary; tie-break confidence; optional liquidity; then score)
# -----------------------------------------------------------------------------
def _rank_key(c: Candidate, *, use_liquidity: bool) -> Tuple[float, float, float, float]:
    vol = c.volume if use_liquidity else 0.0
    return (c.roi, c.confidence, vol, c.overall_score)


# -----------------------------------------------------------------------------
# Optional enrichment for final Top-N (fill missing keys)
# -----------------------------------------------------------------------------
async def _enrich_top(engine: Any, symbols: List[str], *, mode: str = "") -> Dict[str, Dict[str, Any]]:
    if engine is None or not symbols:
        return {}

    try:
        res = await _fetch_quotes_map(engine, symbols, mode=mode or "")
        if isinstance(res, dict) and res:
            return {s: _to_payload(res.get(s)) for s in symbols}
    except Exception:
        pass

    out: Dict[str, Dict[str, Any]] = {}
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
                r = await _maybe_await(r)
                out[sym] = _to_payload(r)
            except Exception:
                out[sym] = {"symbol": sym, "warnings": "enrich_failed"}

    await asyncio.gather(*(one(s) for s in symbols), return_exceptions=False)
    return out


# -----------------------------------------------------------------------------
# Core selection (LIVE quotes primary; snapshots only as last resort)
# -----------------------------------------------------------------------------
async def select_top10(*, engine: Any, criteria: Criteria, mode: str = "") -> Tuple[List[Candidate], Dict[str, Any]]:
    t0 = time.time()
    horizon = criteria.horizon()
    roi_key = horizon_to_roi_key(horizon)

    pages = _eligible_pages(criteria)
    per_page_limit = max(50, int(os.getenv("TOP10_UNIVERSE_LIMIT_PER_PAGE", "500") or "500"))
    per_page_limit = max(50, min(5000, per_page_limit))

    universe: List[Tuple[str, str]] = []
    warnings: List[str] = []

    for page in pages:
        syms = await _get_universe_symbols_live(engine, page, limit=per_page_limit)
        if not syms:
            snap = _get_snapshot(engine, page)
            if isinstance(snap, dict) and isinstance(snap.get("rows"), list):
                hdrs = snap.get("headers") or []
                rows = snap.get("rows") or []
                if isinstance(hdrs, list) and hdrs:
                    idx_map = {_norm_key(h): i for i, h in enumerate(hdrs) if _norm_key(h)}
                    sym_idx = None
                    for k in ("symbol", "ticker", "code"):
                        if k in idx_map:
                            sym_idx = idx_map[k]
                            break
                    if sym_idx is not None:
                        for r in rows[:per_page_limit]:
                            if isinstance(r, (list, tuple)) and sym_idx < len(r):
                                s = _normalize_symbol(_safe_str(r[sym_idx]))
                                if s:
                                    universe.append((s, page))
                if not any(p == page for _, p in universe):
                    warnings.append(f"universe_empty:{page}")
            else:
                warnings.append(f"universe_empty:{page}")
            continue

        for s in syms:
            universe.append((s, page))

    seen_u: Set[str] = set()
    universe_dedup: List[Tuple[str, str]] = []
    for s, p in universe:
        if s and s not in seen_u:
            seen_u.add(s)
            universe_dedup.append((s, p))

    symbols = [s for s, _ in universe_dedup]
    source_map = {s: p for s, p in universe_dedup}

    if not symbols:
        meta = {
            "version": TOP10_SELECTOR_VERSION,
            "mode": mode,
            "horizon": horizon,
            "roi_key": roi_key,
            "pages_selected": pages,
            "universe_symbols": 0,
            "candidates": 0,
            "returned": 0,
            "build_status": "WARN",
            "warnings": warnings or ["universe_empty_all_pages"],
            "timestamp_utc": _now_utc_iso(),
            "timestamp_riyadh": _now_riyadh_iso(),
            "duration_ms": round((time.time() - t0) * 1000.0, 3),
        }
        return [], meta

    qmap = await _fetch_quotes_map(engine, symbols, mode=mode or "")

    candidates: List[Candidate] = []
    for sym in symbols:
        q = qmap.get(sym) or {}
        if not isinstance(q, dict) or not q:
            continue
        cand = _extract_candidate_from_quote(sym=sym, page=source_map.get(sym, ""), quote=q, horizon=horizon)
        if cand is None:
            continue

        if criteria.min_expected_roi is not None and cand.roi < float(criteria.min_expected_roi):
            continue

        if criteria.enforce_risk_confidence:
            if cand.risk_score > float(criteria.max_risk_score):
                continue
            if cand.confidence < float(criteria.min_confidence):
                continue

        if criteria.min_volume is not None and cand.volume < float(criteria.min_volume):
            continue

        candidates.append(cand)

    best: Dict[str, Candidate] = {}
    for c in candidates:
        prev = best.get(c.symbol)
        if prev is None or _rank_key(c, use_liquidity=criteria.use_liquidity_tiebreak) > _rank_key(prev, use_liquidity=criteria.use_liquidity_tiebreak):
            best[c.symbol] = c

    deduped = list(best.values())
    deduped.sort(key=lambda c: _rank_key(c, use_liquidity=criteria.use_liquidity_tiebreak), reverse=True)

    top_n = max(1, int(criteria.top_n or 10))
    top = deduped[:top_n]

    enrich_map: Dict[str, Dict[str, Any]] = {}
    if criteria.enrich_final and top:
        try:
            enrich_map = await _enrich_top(engine, [c.symbol for c in top], mode=mode or "")
        except Exception:
            enrich_map = {}

    for c in top:
        e = enrich_map.get(c.symbol)
        if isinstance(e, dict) and e:
            merged = dict(c.row)
            merged.update(e)
            merged["symbol"] = c.symbol
            merged["source_page"] = c.source_page
            c.row = merged

    meta = {
        "version": TOP10_SELECTOR_VERSION,
        "mode": mode,
        "horizon": horizon,
        "roi_key": roi_key,
        "pages_selected": pages,
        "universe_symbols": len(symbols),
        "quotes_returned": len(qmap),
        "candidates": len(candidates),
        "deduped": len(deduped),
        "returned": len(top),
        "build_status": "OK" if len(top) > 0 else "WARN",
        "filters": {
            "min_expected_roi": criteria.min_expected_roi,
            "max_risk_score": criteria.max_risk_score,
            "min_confidence": criteria.min_confidence,
            "min_volume": criteria.min_volume,
            "use_liquidity_tiebreak": criteria.use_liquidity_tiebreak,
            "enforce_risk_confidence": criteria.enforce_risk_confidence,
        },
        "warnings": warnings,
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
        "duration_ms": round((time.time() - t0) * 1000.0, 3),
    }
    return top, meta


# -----------------------------------------------------------------------------
# Schema-aligned output for Top_10_Investments (83 columns)
# -----------------------------------------------------------------------------
def _get_top10_schema() -> Tuple[List[str], List[str], Set[str], str]:
    """
    Returns (headers, keys, pct_keys, schema_source) for Top_10_Investments.
    pct_keys are derived from schema dtype='pct' (best-effort).
    """
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Top_10_Investments")
        cols = getattr(spec, "columns", None) or []

        headers: List[str] = []
        keys: List[str] = []
        pct_keys: Set[str] = set()

        for c in cols:
            h = _safe_str(getattr(c, "header", ""))
            k = _safe_str(getattr(c, "key", ""))
            if not h or not k:
                continue
            headers.append(h)
            keys.append(k)

            dt = _safe_str(getattr(c, "dtype", "")).lower()
            if dt in {"pct", "percent", "percentage"}:
                pct_keys.add(k)

        if headers and keys and len(headers) == len(keys):
            return headers, keys, pct_keys, "schema_registry.get_sheet_spec"
    except Exception:
        pass

    keys = [
        "symbol",
        "name",
        "current_price",
        "expected_roi_3m",
        "forecast_confidence",
        "risk_score",
        "overall_score",
        "recommendation",
        "last_updated_riyadh",
        "top10_rank",
        "selection_reason",
        "criteria_snapshot",
    ]
    headers = [
        "Symbol",
        "Name",
        "Current Price",
        "Expected ROI 3M",
        "Forecast Confidence",
        "Risk Score",
        "Overall Score",
        "Recommendation",
        "Last Updated (Riyadh)",
        "Top10 Rank",
        "Selection Reason",
        "Criteria Snapshot",
    ]
    pct_keys = {"expected_roi_1m", "expected_roi_3m", "expected_roi_12m"}
    return headers, keys, pct_keys, "fallback_minimal"


def _project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: row.get(k, None) for k in keys}


def _normalize_pct_fields_inplace(raw: Dict[str, Any], pct_keys: Set[str]) -> None:
    """
    For schema pct keys, enforce FRACTION values (0.25 == 25%).
    Only converts when value is present.
    """
    for k in pct_keys or set():
        if k not in raw:
            continue
        v = raw.get(k)
        if v is None:
            continue
        f = _as_fraction(v)
        if f is not None:
            raw[k] = float(f)


def _ensure_recommendation(raw: Dict[str, Any], roi_key: str) -> None:
    """
    Fill recommendation if missing (prevents blank => red X in some sheet rules).
    """
    if _safe_str(raw.get("recommendation", "")).strip():
        return

    roi = _as_fraction(raw.get(roi_key))
    conf = _safe_float(raw.get("forecast_confidence"), None)
    if conf is not None:
        conf = float(conf / 100.0) if conf > 1.5 else float(conf)
        conf = _clamp(conf, 0.0, 1.0)

    # Very simple heuristic (non-binding)
    if roi is None:
        raw["recommendation"] = "HOLD"
        return

    if conf is not None and conf >= 0.75 and roi >= 0.25:
        raw["recommendation"] = "STRONG_BUY"
    elif roi >= 0.15:
        raw["recommendation"] = "BUY"
    elif roi >= 0.05:
        raw["recommendation"] = "HOLD"
    else:
        raw["recommendation"] = "HOLD"


def build_top10_output_rows(candidates: List[Candidate], *, criteria: Criteria, mode: str = "") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    headers, keys, pct_keys, schema_source = _get_top10_schema()
    horizon = criteria.horizon()
    roi_key = horizon_to_roi_key(horizon)

    crit_snapshot = {
        "invest_period_days": criteria.invest_period_days,
        "horizon": horizon,
        "pages_selected": criteria.pages_selected,
        "min_expected_roi": criteria.min_expected_roi,
        "max_risk_score": criteria.max_risk_score,
        "min_confidence": criteria.min_confidence,
        "min_volume": criteria.min_volume,
        "use_liquidity_tiebreak": criteria.use_liquidity_tiebreak,
        "enforce_risk_confidence": criteria.enforce_risk_confidence,
        "mode": mode,
        "selector_version": TOP10_SELECTOR_VERSION,
        "generated_at_utc": _now_utc_iso(),
        "generated_at_riyadh": _now_riyadh_iso(),
    }
    crit_json = _json_dumps_safe(crit_snapshot)

    out: List[Dict[str, Any]] = []
    for i, c in enumerate(candidates, start=1):
        raw = dict(c.row or {})

        # Canonical basics
        raw["symbol"] = c.symbol
        raw.setdefault("name", raw.get("company_name") or raw.get("long_name") or raw.get("instrument_name") or "")
        raw.setdefault("current_price", raw.get("price") or raw.get("last") or raw.get("close"))
        raw.setdefault("risk_score", c.risk_score)
        raw.setdefault("overall_score", c.overall_score)
        raw.setdefault("forecast_confidence", c.confidence)

        # ROI: enforce as FRACTION
        raw[roi_key] = _as_fraction(raw.get(roi_key)) if raw.get(roi_key) is not None else c.roi
        if raw.get(roi_key) is None:
            raw[roi_key] = c.roi

        # Top10 extras
        raw["top10_rank"] = i
        raw["selection_reason"] = (
            f"Ranked by {roi_key} then confidence"
            + (" then liquidity(volume)" if criteria.use_liquidity_tiebreak else "")
            + " then overall_score"
        )
        raw["criteria_snapshot"] = crit_json

        # Provenance fields
        raw.setdefault("data_provider", raw.get("data_provider") or raw.get("provider") or "")
        raw.setdefault("last_updated_utc", raw.get("last_updated_utc") or _now_utc_iso())
        raw.setdefault("last_updated_riyadh", raw.get("last_updated_riyadh") or _now_riyadh_iso())

        # Ensure recommendation is not blank
        _ensure_recommendation(raw, roi_key)

        # Normalize pct dtype keys to FRACTION
        _normalize_pct_fields_inplace(raw, pct_keys)

        out.append(_project_row(keys, raw))

    meta = {
        "schema_source": schema_source,
        "headers_len": len(headers),
        "keys_len": len(keys),
        "pct_keys_len": len(pct_keys),
        "horizon": horizon,
        "roi_key": roi_key,
        "rows": len(out),
        "selector_version": TOP10_SELECTOR_VERSION,
    }
    return out, meta


# -----------------------------------------------------------------------------
# Convenience public APIs (used by routes / insights_builder)
# -----------------------------------------------------------------------------
async def select_top10_symbols(*, engine: Any, criteria: Optional[Dict[str, Any]] = None, limit: int = 10, mode: str = "") -> List[str]:
    crit = _criteria_from_dict(criteria or {})
    crit.top_n = max(1, min(50, int(limit or crit.top_n or 10)))
    top, _meta = await select_top10(engine=engine, criteria=crit, mode=mode or "")
    return [c.symbol for c in top]


async def build_top10_rows(*, engine: Any, criteria: Optional[Dict[str, Any]] = None, limit: int = 10, mode: str = "") -> Dict[str, Any]:
    crit = _criteria_from_dict(criteria or {})
    crit.top_n = max(1, min(50, int(limit or crit.top_n or 10)))

    top, meta_sel = await select_top10(engine=engine, criteria=crit, mode=mode or "")
    rows, meta_rows = build_top10_output_rows(top, criteria=crit, mode=mode or "")
    headers, keys, _pct_keys, _schema_source = _get_top10_schema()

    return {
        "status": "success",
        "page": "Top_10_Investments",
        "headers": headers,
        "keys": keys,
        "rows": rows,
        "meta": {
            **(meta_sel or {}),
            **(meta_rows or {}),
        },
    }


__all__ = [
    "TOP10_SELECTOR_VERSION",
    "Criteria",
    "Candidate",
    "load_criteria_best_effort",
    "merge_criteria_overrides",
    "map_period_days_to_horizon",
    "horizon_to_roi_key",
    "horizon_to_forecast_price_key",
    "select_top10",
    "select_top10_symbols",
    "build_top10_rows",
    "build_top10_output_rows",
]
