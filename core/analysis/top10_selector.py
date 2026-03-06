#!/usr/bin/env python3
# core/analysis/top10_selector.py
"""
================================================================================
Top 10 Selector — v3.1.0
================================================================================
LIVE • SCHEMA-FIRST • TOP10-METADATA COMPLETE • ROUTE-COMPATIBLE • SHEET-FRIENDLY

Purpose
-------
Produce fully populated, schema-aligned Top_10_Investments rows with stable
selection logic and guaranteed Top10-only fields.

Primary guarantees
------------------
- Always returns Top_10_Investments rows projected to the canonical schema
- Always attempts to populate the Top10-only fields:
    - top10_rank
    - selection_reason
    - criteria_snapshot
- Compatible with flexible route-builder calls:
    - engine=...
    - request=..., settings=..., body=...
    - mode=...
    - limit=...
- Uses live quotes when available, snapshot fallback only when necessary
- Does not fail only because some optional fields are missing
- No network I/O at import-time

Public APIs
-----------
- select_top10_symbols(...)
- build_top10_rows(...)
- select_top10(...)
- build_top10_output_rows(...)
- build_rows(...)                        # route-friendly canonical builder
- build_top_10_investments_rows(...)
- build_top10_investments_rows(...)
- build_top_10_rows(...)
- get_top10_rows(...)

Notes
-----
- Investment period is always in DAYS and mapped internally to 1M / 3M / 12M
- Live universe + live quote execution path is primary
- Snapshot parsing is fallback only
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

logger = logging.getLogger("core.analysis.top10_selector")
logger.addHandler(logging.NullHandler())

TOP10_SELECTOR_VERSION = "3.1.0"
TOP10_PAGE_NAME = "Top_10_Investments"
RIYADH_TZ = timezone(timedelta(hours=3))


# =============================================================================
# Safe helpers
# =============================================================================
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


def _safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    if x is None:
        return default
    try:
        return int(float(x))
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


def _as_pct_points(x: Any) -> Optional[float]:
    f = _safe_float(x, None)
    if f is None:
        return None
    if abs(f) <= 1.5:
        return f * 100.0
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
            return md(mode="python")
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
        return json.dumps(obj, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        return str(obj)


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for item in items:
        s = _safe_str(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _is_truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = _safe_str(v).lower()
    return s in {"1", "true", "yes", "y", "on"}


def _first_non_none(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def _dict_get_ci(d: Mapping[str, Any], *keys: str) -> Any:
    if not isinstance(d, Mapping):
        return None
    norm = {_norm_key(k): v for k, v in d.items()}
    for k in keys:
        nk = _norm_key(k)
        if nk in norm:
            return norm[nk]
    return None


# =============================================================================
# Period mapping
# =============================================================================
def map_period_days_to_horizon(days: int) -> str:
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


def _horizon_label(horizon: str) -> str:
    h = (horizon or "").lower()
    if h == "1m":
        return "1M"
    if h == "3m":
        return "3M"
    return "12M"


# =============================================================================
# Criteria
# =============================================================================
@dataclass(slots=True)
class Criteria:
    pages_selected: List[str]
    invest_period_days: int

    min_expected_roi: Optional[float] = None
    max_risk_score: float = 60.0
    min_confidence: float = 0.70
    enforce_risk_confidence: bool = True

    min_volume: Optional[float] = None
    use_liquidity_tiebreak: bool = True

    top_n: int = 10
    enrich_final: bool = True

    def horizon(self) -> str:
        return map_period_days_to_horizon(self.invest_period_days)


def _criteria_from_dict(d: Dict[str, Any]) -> Criteria:
    pages = (
        d.get("pages_selected")
        or d.get("pages")
        or d.get("selected_pages")
        or d.get("pagesSelected")
        or d.get("page_selection")
        or []
    )
    if isinstance(pages, str):
        parts = re.split(r"[,\;\|\n]+", pages)
        pages = [p.strip() for p in parts if p.strip()]
    if not isinstance(pages, list) or not pages:
        pages = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"]

    invest_days = int(
        _safe_float(
            d.get("invest_period_days")
            or d.get("investment_period_days")
            or d.get("period_days")
            or d.get("horizon_days")
            or d.get("investment_period")
            or 90,
            90,
        )
        or 90
    )
    invest_days = max(1, invest_days)

    min_roi = _as_fraction(
        d.get("min_expected_roi")
        or d.get("min_roi")
        or d.get("required_return")
        or d.get("required_roi")
        or d.get("required_return_pct")
    )

    max_risk = float(_safe_float(d.get("max_risk_score") or d.get("max_risk") or 60.0, 60.0) or 60.0)
    max_risk = _clamp(max_risk, 0.0, 100.0)

    min_conf = _safe_float(
        d.get("min_confidence")
        or d.get("min_ai_confidence")
        or d.get("min_confidence_score")
        or 0.70,
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


# =============================================================================
# Request / route compatibility helpers
# =============================================================================
def _resolve_engine_from_request(request: Any) -> Any:
    try:
        app = getattr(request, "app", None)
        state = getattr(app, "state", None)
        eng = getattr(state, "engine", None)
        if eng is not None:
            return eng
    except Exception:
        pass
    return None


def _resolve_limit(limit: Any, criteria: Optional[Dict[str, Any]], body: Optional[Dict[str, Any]]) -> int:
    v = _first_non_none(
        limit,
        (criteria or {}).get("top_n") if isinstance(criteria, dict) else None,
        (criteria or {}).get("limit") if isinstance(criteria, dict) else None,
        (body or {}).get("top_n") if isinstance(body, dict) else None,
        (body or {}).get("limit") if isinstance(body, dict) else None,
        10,
    )
    n = int(_safe_float(v, 10) or 10)
    return max(1, min(50, n))


def _merge_criteria_sources(
    *,
    engine: Any,
    criteria: Optional[Dict[str, Any]],
    body: Optional[Dict[str, Any]],
    limit: Optional[int],
) -> Criteria:
    base = load_criteria_best_effort(engine)

    merged: Dict[str, Any] = {}
    if isinstance(body, dict):
        merged.update(body)
    if isinstance(criteria, dict):
        merged.update(criteria)

    crit = merge_criteria_overrides(base, merged)
    crit.top_n = _resolve_limit(limit, criteria, body)
    return crit


def _extract_route_mode(mode: Any, body: Optional[Dict[str, Any]]) -> str:
    m = _safe_str(mode)
    if m:
        return m
    if isinstance(body, dict):
        return _safe_str(body.get("mode"))
    return ""


# =============================================================================
# Page selection / normalization
# =============================================================================
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
    return _dedupe_keep_order(pages_in)


# =============================================================================
# Engine / snapshot / row access
# =============================================================================
def _get_snapshot(engine: Any, page: str) -> Optional[Dict[str, Any]]:
    if engine is None:
        return None
    for fn_name, kwargs in [
        ("get_cached_sheet_snapshot", {"page": page}),
        ("get_cached_sheet_snapshot", {"sheet": page}),
        ("get_sheet_snapshot", {"page": page}),
        ("get_sheet_snapshot", {"sheet": page}),
    ]:
        fn = getattr(engine, fn_name, None)
        if callable(fn):
            try:
                snap = fn(**kwargs)
                if isinstance(snap, dict):
                    return snap
            except TypeError:
                try:
                    snap = fn(page)
                    if isinstance(snap, dict):
                        return snap
                except Exception:
                    pass
            except Exception:
                pass
    return None


def _row_list_from_snapshot(snapshot: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(snapshot, dict):
        return []

    rows = snapshot.get("rows") or snapshot.get("data") or snapshot.get("items") or []
    keys = snapshot.get("keys") or []
    headers = snapshot.get("headers") or []

    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return [dict(r) for r in rows if isinstance(r, dict)]

    if isinstance(rows, list) and rows and isinstance(rows[0], (list, tuple)):
        cols = keys if isinstance(keys, list) and keys else headers
        if not isinstance(cols, list) or not cols:
            return []
        out: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, (list, tuple)):
                continue
            obj: Dict[str, Any] = {}
            for i, col in enumerate(cols):
                obj[_safe_str(col)] = row[i] if i < len(row) else None
            out.append(obj)
        return out

    return []


async def _get_page_rows_live(engine: Any, page: str, *, limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Best-effort access to engine sheet/page rows for candidate extraction.
    """
    if engine is None:
        return []

    candidates = [
        ("get_sheet_rows", {"sheet": page, "limit": limit}),
        ("get_sheet_rows", {"page": page, "limit": limit}),
        ("get_page_rows", {"page": page, "limit": limit}),
        ("get_page_rows", {"sheet": page, "limit": limit}),
        ("sheet_rows", {"sheet": page, "limit": limit}),
        ("sheet_rows", {"page": page, "limit": limit}),
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
                out = fn(page)
                out = await _maybe_await(out)
            except Exception:
                continue
        except Exception:
            continue

        if isinstance(out, dict):
            rows = out.get("rows") or out.get("data") or out.get("items") or []
            if isinstance(rows, list):
                normalized = [_to_payload(r) for r in rows]
                if normalized:
                    return normalized
        elif isinstance(out, list):
            normalized = [_to_payload(r) for r in out]
            if normalized:
                return normalized

    snap = _get_snapshot(engine, page)
    return _row_list_from_snapshot(snap)


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
            syms = [s for s in syms if s]
            if syms:
                return syms

        if isinstance(out, dict) and isinstance(out.get("symbols"), (list, tuple)):
            syms = [_normalize_symbol(_safe_str(x)) for x in out["symbols"]]
            syms = [s for s in syms if s]
            if syms:
                return syms

    fn2 = getattr(engine, "symbols_reader", None)
    if fn2 is not None and hasattr(fn2, "get_symbols_for_sheet"):
        try:
            out = fn2.get_symbols_for_sheet(page)  # type: ignore
            out = await _maybe_await(out)
            if isinstance(out, (list, tuple)):
                syms = [_normalize_symbol(_safe_str(x)) for x in out]
                syms = [s for s in syms if s]
                if syms:
                    return syms
        except Exception:
            pass

    # fallback from page rows / snapshot rows
    rows = await _get_page_rows_live(engine, page, limit=limit)
    syms2: List[str] = []
    for r in rows:
        sym = _extract_symbol_from_row(r)
        if sym:
            syms2.append(sym)
    return _dedupe_keep_order(syms2)


def _dict_is_symbol_map(d: Dict[str, Any], symbols: List[str]) -> bool:
    if not isinstance(d, dict) or not symbols:
        return False
    symset = set(symbols)
    keys = [k for k in d.keys() if isinstance(k, str)]
    if not keys:
        return False
    hit = sum(1 for k in keys if k in symset)
    return hit >= max(1, min(3, len(symset)))


async def _fetch_quotes_map(engine: Any, symbols: List[str], *, mode: str = "") -> Dict[str, Dict[str, Any]]:
    if not engine or not symbols:
        return {}

    batch_candidates = [
        "get_enriched_quotes_batch",
        "get_analysis_quotes_batch",
        "get_quotes_batch",
        "get_enriched_quotes",
        "quotes_batch",
    ]

    for name in batch_candidates:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue

        try:
            try:
                res = fn(symbols, mode=mode or "")
            except TypeError:
                res = fn(symbols)
            res = await _maybe_await(res)

            if isinstance(res, dict):
                if _dict_is_symbol_map(res, symbols):
                    return {s: _to_payload(res.get(s)) for s in symbols}
                nested = res.get("data") or res.get("rows") or res.get("items")
                if isinstance(nested, dict) and _dict_is_symbol_map(nested, symbols):
                    return {s: _to_payload(nested.get(s)) for s in symbols}
                if isinstance(nested, list):
                    return {s: _to_payload(v) for s, v in zip(symbols, nested)}
            elif isinstance(res, list):
                return {s: _to_payload(v) for s, v in zip(symbols, res)}
        except Exception:
            continue

    out: Dict[str, Dict[str, Any]] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)

    for s in symbols:
        try:
            if callable(per_dict_fn):
                try:
                    out[s] = _to_payload(await _maybe_await(per_dict_fn(s, mode=mode or "")))
                except TypeError:
                    out[s] = _to_payload(await _maybe_await(per_dict_fn(s)))
            elif callable(per_fn):
                try:
                    out[s] = _to_payload(await _maybe_await(per_fn(s, mode=mode or "")))
                except TypeError:
                    out[s] = _to_payload(await _maybe_await(per_fn(s)))
            else:
                out[s] = {"symbol": s, "warnings": "engine_missing_quote_methods"}
        except Exception as e:
            out[s] = {"symbol": s, "warnings": f"quote_error: {e}"}

    return out


# =============================================================================
# Snapshot parsing fallback
# =============================================================================
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


# =============================================================================
# Candidate model + extraction
# =============================================================================
@dataclass(slots=True)
class Candidate:
    symbol: str
    source_page: str
    roi: float
    confidence: float
    overall_score: float
    risk_score: float
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


def _extract_symbol_from_row(row: Dict[str, Any]) -> str:
    sym = _first_non_none(
        row.get("symbol"),
        row.get("ticker"),
        row.get("code"),
        row.get("Symbol"),
        row.get("Ticker"),
        row.get("Code"),
    )
    return _normalize_symbol(_safe_str(sym))


def _extract_candidate_from_quote(*, sym: str, page: str, quote: Dict[str, Any], horizon: str) -> Optional[Candidate]:
    roi_key = horizon_to_roi_key(horizon)
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


def _extract_candidate_from_row(*, page: str, row: Dict[str, Any], horizon: str) -> Optional[Candidate]:
    if not isinstance(row, dict):
        return None

    sym = _extract_symbol_from_row(row)
    if not sym:
        return None

    roi_key = horizon_to_roi_key(horizon)
    roi = _as_fraction(_first_non_none(row.get(roi_key), row.get("upside_pct"), row.get("target_return")))
    if roi is None:
        return None

    conf = _get_confidence(row)
    overall = _get_overall_score(row)
    risk = _get_risk_score(row)
    vol = _get_volume(row)

    rr = dict(row)
    rr["symbol"] = sym
    rr["source_page"] = page

    return Candidate(
        symbol=sym,
        source_page=page,
        roi=float(roi),
        confidence=float(conf),
        overall_score=float(overall),
        risk_score=float(risk),
        volume=float(vol),
        row=rr,
    )


def _rank_key(c: Candidate, *, use_liquidity: bool) -> Tuple[float, float, float, float]:
    vol = c.volume if use_liquidity else 0.0
    return (c.roi, c.confidence, vol, c.overall_score)


# =============================================================================
# Optional enrichment for final Top-N
# =============================================================================
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


# =============================================================================
# Core selection
# =============================================================================
async def select_top10(*, engine: Any, criteria: Criteria, mode: str = "") -> Tuple[List[Candidate], Dict[str, Any]]:
    t0 = time.time()
    horizon = criteria.horizon()
    roi_key = horizon_to_roi_key(horizon)

    pages = _eligible_pages(criteria)
    try:
        per_page_limit = int(os.getenv("TOP10_UNIVERSE_LIMIT_PER_PAGE", "500") or "500")
    except Exception:
        per_page_limit = 500
    per_page_limit = max(50, min(5000, per_page_limit))

    universe: List[Tuple[str, str]] = []
    warnings: List[str] = []

    for page in pages:
        page_symbols = await _get_universe_symbols_live(engine, page, limit=per_page_limit)
        page_rows = await _get_page_rows_live(engine, page, limit=per_page_limit)

        if page_symbols:
            for s in page_symbols:
                universe.append((s, page))
        else:
            warnings.append(f"universe_empty:{page}")

        # Additional fallback: if rows contain directly scoreable candidates, use them later
        # through row-candidate extraction even when symbol listing is weak.
        if not page_symbols and not page_rows:
            snap = _get_snapshot(engine, page)
            if isinstance(snap, dict) and isinstance(snap.get("rows"), list):
                warnings.append(f"snapshot_present_but_no_symbols:{page}")

    seen_u: Set[str] = set()
    universe_dedup: List[Tuple[str, str]] = []
    for s, p in universe:
        if s and s not in seen_u:
            seen_u.add(s)
            universe_dedup.append((s, p))

    symbols = [s for s, _ in universe_dedup]
    source_map = {s: p for s, p in universe_dedup}

    qmap: Dict[str, Dict[str, Any]] = {}
    if symbols:
        qmap = await _fetch_quotes_map(engine, symbols, mode=mode or "")

    candidates: List[Candidate] = []

    # Primary: candidates from live quotes
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

    # Secondary fallback: direct candidates from page rows/snapshots
    if not candidates:
        row_based_count = 0
        for page in pages:
            rows = await _get_page_rows_live(engine, page, limit=per_page_limit)
            for row in rows:
                cand = _extract_candidate_from_row(page=page, row=row, horizon=horizon)
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
                row_based_count += 1

        if row_based_count > 0:
            warnings.append(f"used_row_based_fallback:{row_based_count}")

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
        except Exception as e:
            warnings.append(f"enrich_failed:{e}")
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


# =============================================================================
# Schema helpers for Top_10_Investments
# =============================================================================
def _get_top10_schema() -> Tuple[List[str], List[str], Set[str], str]:
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec(TOP10_PAGE_NAME)
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
    pct_keys = {"expected_roi_1m", "expected_roi_3m", "expected_roi_12m", "forecast_confidence"}
    return headers, keys, pct_keys, "fallback_minimal"


def _project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: row.get(k, None) for k in keys}


def _normalize_pct_fields_inplace(raw: Dict[str, Any], pct_keys: Set[str]) -> None:
    for k in pct_keys or set():
        if k not in raw:
            continue
        v = raw.get(k)
        if v is None:
            continue
        f = _as_fraction(v)
        if f is not None:
            raw[k] = float(f)


def _score_text(label: str, value: Any) -> Optional[str]:
    f = _safe_float(value, None)
    if f is None:
        return None
    if 0.0 < f <= 1.5 and label.lower() not in {"risk", "confidence"}:
        f = f * 100.0
    return f"{label}={round(float(f), 2)}"


def _pct_text(label: str, value: Any) -> Optional[str]:
    f = _as_fraction(value)
    if f is None:
        return None
    return f"{label}={round(float(f) * 100.0, 2)}%"


def _ensure_recommendation(raw: Dict[str, Any], roi_key: str) -> None:
    if _safe_str(raw.get("recommendation", "")).strip():
        return

    roi = _as_fraction(raw.get(roi_key))
    conf = _safe_float(raw.get("forecast_confidence"), None)
    if conf is not None:
        conf = float(conf / 100.0) if conf > 1.5 else float(conf)
        conf = _clamp(conf, 0.0, 1.0)

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


def _ensure_recommendation_reason(raw: Dict[str, Any], roi_key: str) -> None:
    if _safe_str(raw.get("recommendation_reason")):
        return

    parts: List[str] = []
    rec = _safe_str(raw.get("recommendation"))
    if rec:
        parts.append(f"Recommendation={rec}")

    roi_text = _pct_text(roi_key.replace("expected_", "").replace("_", " ").upper(), raw.get(roi_key))
    if roi_text:
        parts.append(roi_text)

    conf_text = _pct_text("Confidence", raw.get("forecast_confidence"))
    if conf_text:
        parts.append(conf_text)

    risk_bucket = _safe_str(raw.get("risk_bucket"))
    if risk_bucket:
        parts.append(f"Risk={risk_bucket}")

    overall = _score_text("Overall", raw.get("overall_score"))
    if overall:
        parts.append(overall)

    raw["recommendation_reason"] = " | ".join(parts) if parts else None


def _ensure_rank_overall(raw: Dict[str, Any], rank_value: int) -> None:
    if raw.get("rank_overall") is None:
        raw["rank_overall"] = rank_value


def _ensure_period_fields(raw: Dict[str, Any], criteria: Criteria, horizon: str) -> None:
    if raw.get("horizon_days") is None:
        raw["horizon_days"] = int(criteria.invest_period_days)
    if not _safe_str(raw.get("invest_period_label")):
        raw["invest_period_label"] = _horizon_label(horizon)


def _ensure_timestamps(raw: Dict[str, Any]) -> None:
    if not raw.get("last_updated_utc"):
        raw["last_updated_utc"] = _now_utc_iso()
    if not raw.get("last_updated_riyadh"):
        raw["last_updated_riyadh"] = _now_riyadh_iso()


def _build_criteria_snapshot(criteria: Criteria, *, mode: str) -> Dict[str, Any]:
    return {
        "invest_period_days": criteria.invest_period_days,
        "horizon": criteria.horizon(),
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


def _build_selection_reason(raw: Dict[str, Any], *, roi_key: str, criteria: Criteria) -> str:
    parts: List[str] = []

    roi = _pct_text(roi_key.replace("expected_", "").replace("_", " ").upper(), raw.get(roi_key))
    if roi:
        parts.append(roi)

    conf = _pct_text("Confidence", raw.get("forecast_confidence"))
    if conf:
        parts.append(conf)

    risk_bucket = _safe_str(raw.get("risk_bucket"))
    if risk_bucket:
        parts.append(f"Risk={risk_bucket}")

    overall = _score_text("Overall", raw.get("overall_score"))
    if overall:
        parts.append(overall)

    opp = _score_text("Opportunity", raw.get("opportunity_score"))
    if opp:
        parts.append(opp)

    if criteria.use_liquidity_tiebreak:
        vol = _safe_float(raw.get("volume"), None)
        if vol is not None:
            parts.append(f"Volume={round(float(vol), 2)}")

    if not parts:
        return (
            f"Selected using {roi_key}, confidence, "
            + ("liquidity, " if criteria.use_liquidity_tiebreak else "")
            + "and overall score"
        )

    return " | ".join(parts)


def _ensure_required_top10_fields(raw: Dict[str, Any], *, rank: int, criteria: Criteria, horizon: str, mode: str) -> None:
    roi_key = horizon_to_roi_key(horizon)
    forecast_price_key = horizon_to_forecast_price_key(horizon)

    raw.setdefault("name", raw.get("company_name") or raw.get("long_name") or raw.get("instrument_name") or "")
    raw.setdefault("current_price", raw.get("price") or raw.get("last") or raw.get("close"))
    raw.setdefault("forecast_confidence", raw.get("confidence_score") if raw.get("forecast_confidence") is None else raw.get("forecast_confidence"))

    if raw.get(roi_key) is None:
        for alt in ("expected_roi_3m", "expected_roi_1m", "expected_roi_12m", "upside_pct", "target_return"):
            if raw.get(alt) is not None:
                raw[roi_key] = raw.get(alt)
                break

    raw["top10_rank"] = rank
    raw["criteria_snapshot"] = _json_dumps_safe(_build_criteria_snapshot(criteria, mode=mode))
    raw["selection_reason"] = _build_selection_reason(raw, roi_key=roi_key, criteria=criteria)

    _ensure_timestamps(raw)
    _ensure_recommendation(raw, roi_key)
    _ensure_recommendation_reason(raw, roi_key)
    _ensure_rank_overall(raw, rank)
    _ensure_period_fields(raw, criteria, horizon)

    if raw.get(forecast_price_key) is None and raw.get("current_price") is not None and raw.get(roi_key) is not None:
        cp = _safe_float(raw.get("current_price"), None)
        roi = _as_fraction(raw.get(roi_key))
        if cp is not None and roi is not None:
            raw[forecast_price_key] = float(cp) * (1.0 + float(roi))

    raw.setdefault("data_provider", raw.get("data_provider") or raw.get("provider") or "")


def _schema_contains(keys: Sequence[str], needle: str) -> bool:
    return any(str(k) == needle for k in keys)


# =============================================================================
# Top10 output builder
# =============================================================================
def build_top10_output_rows(candidates: List[Candidate], *, criteria: Criteria, mode: str = "") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    headers, keys, pct_keys, schema_source = _get_top10_schema()
    horizon = criteria.horizon()
    roi_key = horizon_to_roi_key(horizon)
    forecast_price_key = horizon_to_forecast_price_key(horizon)

    warnings: List[str] = []
    rows_out: List[Dict[str, Any]] = []

    required_top10_fields = ["top10_rank", "selection_reason", "criteria_snapshot"]
    missing_required_fields = [f for f in required_top10_fields if not _schema_contains(keys, f)]

    for i, c in enumerate(candidates, start=1):
        raw = dict(c.row or {})

        raw["symbol"] = c.symbol
        raw.setdefault("source_page", c.source_page)
        raw.setdefault("risk_score", c.risk_score)
        raw.setdefault("overall_score", c.overall_score)
        raw.setdefault("forecast_confidence", c.confidence)

        if raw.get(roi_key) is None:
            raw[roi_key] = c.roi

        _ensure_required_top10_fields(raw, rank=i, criteria=criteria, horizon=horizon, mode=mode)
        _normalize_pct_fields_inplace(raw, pct_keys)

        if raw.get("current_price") is None:
            warnings.append(f"missing_current_price:{c.symbol}")
        if raw.get(roi_key) is None:
            warnings.append(f"missing_roi:{c.symbol}")
        if raw.get("forecast_confidence") is None:
            warnings.append(f"missing_confidence:{c.symbol}")
        if not _safe_str(raw.get("recommendation")):
            warnings.append(f"missing_recommendation:{c.symbol}")
        if not _safe_str(raw.get("selection_reason")):
            warnings.append(f"missing_selection_reason:{c.symbol}")
        if not _safe_str(raw.get("criteria_snapshot")):
            warnings.append(f"missing_criteria_snapshot:{c.symbol}")
        if raw.get("top10_rank") is None:
            warnings.append(f"missing_top10_rank:{c.symbol}")

        rows_out.append(_project_row(keys, raw))

    meta = {
        "schema_source": schema_source,
        "headers_len": len(headers),
        "keys_len": len(keys),
        "pct_keys_len": len(pct_keys),
        "horizon": horizon,
        "roi_key": roi_key,
        "forecast_price_key": forecast_price_key,
        "rows": len(rows_out),
        "selector_version": TOP10_SELECTOR_VERSION,
        "warnings": warnings,
        "missing_required_schema_fields": missing_required_fields,
        "required_top10_fields_present_in_schema": len(missing_required_fields) == 0,
    }
    return rows_out, meta


# =============================================================================
# Canonical route-friendly public builders
# =============================================================================
async def select_top10_symbols(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
) -> List[str]:
    engine = engine or _resolve_engine_from_request(request)
    crit = _merge_criteria_sources(engine=engine, criteria=criteria, body=body, limit=limit)
    mode2 = _extract_route_mode(mode, body)
    top, _meta = await select_top10(engine=engine, criteria=crit, mode=mode2)
    return [c.symbol for c in top]


async def build_top10_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
) -> Dict[str, Any]:
    engine = engine or _resolve_engine_from_request(request)
    crit = _merge_criteria_sources(engine=engine, criteria=criteria, body=body, limit=limit)
    mode2 = _extract_route_mode(mode, body)

    top, meta_sel = await select_top10(engine=engine, criteria=crit, mode=mode2)
    rows, meta_rows = build_top10_output_rows(top, criteria=crit, mode=mode2)
    headers, keys, _pct_keys, _schema_source = _get_top10_schema()

    status_out = "success" if rows else "warn"
    warnings = list((meta_sel or {}).get("warnings") or [])
    if not rows:
        warnings.append("top10_empty_after_selection")

    return {
        "status": status_out,
        "page": TOP10_PAGE_NAME,
        "headers": headers,
        "keys": keys,
        "rows": rows,
        "meta": {
            **(meta_sel or {}),
            **(meta_rows or {}),
            "criteria": {
                "pages_selected": crit.pages_selected,
                "invest_period_days": crit.invest_period_days,
                "horizon": crit.horizon(),
                "top_n": crit.top_n,
                "min_expected_roi": crit.min_expected_roi,
                "max_risk_score": crit.max_risk_score,
                "min_confidence": crit.min_confidence,
                "min_volume": crit.min_volume,
                "use_liquidity_tiebreak": crit.use_liquidity_tiebreak,
                "enforce_risk_confidence": crit.enforce_risk_confidence,
            },
            "warnings": warnings,
            "engine_present": engine is not None,
            "request_present": request is not None,
        },
    }


async def build_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    body: Optional[Dict[str, Any]] = None,
    criteria: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    mode: str = "",
) -> List[Dict[str, Any]]:
    """
    Canonical route-friendly builder.
    This is the safest export for routes that call generic builders.
    """
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        body=body,
        criteria=criteria,
        limit=_resolve_limit(limit, criteria, body),
        mode=mode,
    )
    rows = payload.get("rows")
    return rows if isinstance(rows, list) else []


# Backward-compatible aliases some routes/builders may try
async def build_top_10_investments_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
    )
    rows = payload.get("rows")
    return rows if isinstance(rows, list) else []


async def build_top10_investments_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
    )
    rows = payload.get("rows")
    return rows if isinstance(rows, list) else []


async def build_top_10_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
    )
    rows = payload.get("rows")
    return rows if isinstance(rows, list) else []


async def get_top10_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
    )
    rows = payload.get("rows")
    return rows if isinstance(rows, list) else []


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
    "build_rows",
    "build_top_10_investments_rows",
    "build_top10_investments_rows",
    "build_top_10_rows",
    "get_top10_rows",
]
