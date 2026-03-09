#!/usr/bin/env python3
# core/analysis/top10_selector.py
"""
================================================================================
Top 10 Selector — v3.7.0
================================================================================
LIVE • SCHEMA-FIRST • TOP10-METADATA COMPLETE • ROUTE-COMPATIBLE • SHEET-FRIENDLY
FAIL-SAFE • BODY-AWARE • SETTINGS-AWARE • CONTRACT-HARDENED • FALLBACK-ENHANCED
PERFORMANCE-GUARDED • CONCURRENCY-AWARE • UNIVERSE-CAPPED • JSON-SAFE
KWARG-TOLERANT • DISPATCH-FRIENDLY • SPECIAL-PAGE SAFE

Purpose
-------
Produce fully populated, schema-aligned Top_10_Investments rows with stable
selection logic and guaranteed Top10-only fields.

Primary guarantees
------------------
- Always attempts to return a valid payload contract for Top_10_Investments
- Never raises to caller for normal route execution; returns WARN payload on error
- Accepts flexible route-builder calls and extra kwargs safely
- Keeps Top10-only fields guaranteed:
    - top10_rank
    - selection_reason
    - criteria_snapshot
- Uses live quotes when available, snapshot fallback only when necessary
- No network I/O at import-time

New in v3.7.0
-------------
- ✅ FIX: public APIs now accept extra kwargs safely (prevents dispatcher TypeError / 502)
- ✅ FIX: build_top10_rows is fully fail-safe and always returns a payload contract
- ✅ FIX: response payload is JSON-safe (NaN/Inf/datetime/Decimal/set handled)
- ✅ FIX: headers now align with display headers while keys remain explicit separately
- ✅ FIX: better schema loading from schema_registry via multiple fallback patterns
- ✅ FIX: concurrent page fetchers no longer fail whole request on one page error
- ✅ FIX: richer error metadata for easier production diagnostics
- ✅ IMPROVE: body / settings / request / kwargs are merged more predictably
- ✅ IMPROVE: special-page route compatibility for sheet/page/name/tab/request_id/etc.

Public APIs
-----------
- select_top10_symbols(...)
- build_top10_rows(...)
- build_top10_output_rows(...)
- build_rows(...)
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
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

logger = logging.getLogger("core.analysis.top10_selector")
logger.addHandler(logging.NullHandler())

TOP10_SELECTOR_VERSION = "3.7.0"
TOP10_PAGE_NAME = "Top_10_Investments"
RIYADH_TZ = timezone(timedelta(hours=3))

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}


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
        if isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
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
        return dict(obj)

    md = getattr(obj, "model_dump", None)
    if callable(md):
        try:
            out = md(mode="python")
            return out if isinstance(out, dict) else {}
        except Exception:
            try:
                out = md()  # type: ignore[misc]
                return out if isinstance(out, dict) else {}
            except Exception:
                return {}

    d = getattr(obj, "dict", None)
    if callable(d):
        try:
            out = d()
            return out if isinstance(out, dict) else {}
        except Exception:
            return {}

    try:
        dd = getattr(obj, "__dict__", None)
        if isinstance(dd, dict):
            return dict(dd)
    except Exception:
        pass

    return {}


def _coerce_mapping(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, Mapping):
        try:
            return dict(obj)
        except Exception:
            return {}
    return _to_payload(obj)


def _json_safe_value(obj: Any) -> Any:
    """
    Convert arbitrary Python objects into JSON-safe primitives.
    """
    if obj is None:
        return None

    if isinstance(obj, bool):
        return obj

    if isinstance(obj, (int, str)):
        return obj

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    if isinstance(obj, Decimal):
        try:
            f = float(obj)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return str(obj)

    if isinstance(obj, (datetime, date, dt_time)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8", errors="replace")
        except Exception:
            return str(obj)

    if isinstance(obj, (set, tuple)):
        return [_json_safe_value(x) for x in obj]

    if isinstance(obj, list):
        return [_json_safe_value(x) for x in obj]

    if isinstance(obj, Mapping):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            out[_safe_str(k)] = _json_safe_value(v)
        return out

    d = _to_payload(obj)
    if d:
        return _json_safe_value(d)

    try:
        return str(obj)
    except Exception:
        return None


def _json_dumps_safe(obj: Any) -> str:
    try:
        return json.dumps(_json_safe_value(obj), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        try:
            return json.dumps(str(obj), ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return "null"


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


def _coerce_bool(v: Any, default: bool) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return bool(v)
    s = _safe_str(v).lower()
    if not s:
        return default
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


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


def _coerce_to_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    if isinstance(v, set):
        return list(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        return [p.strip() for p in re.split(r"[,\;\|\n]+", s) if p.strip()]
    return [v]


def _rows_to_matrix(keys: Sequence[str], rows: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    matrix: List[List[Any]] = []
    for row in rows:
        matrix.append([_json_safe_value(row.get(k)) for k in keys])
    return matrix


def _matrix_to_rows(matrix: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(matrix, list) or not matrix or not keys:
        return []
    out: List[Dict[str, Any]] = []
    kk = [str(k) for k in keys]
    for row in matrix:
        if not isinstance(row, (list, tuple)):
            continue
        obj: Dict[str, Any] = {}
        for i, key in enumerate(kk):
            obj[key] = row[i] if i < len(row) else None
        out.append(obj)
    return out


def _extract_rows_from_any(obj: Any) -> List[Dict[str, Any]]:
    """
    Best-effort list-of-dicts extractor from various payload shapes.
    Supports:
      - rows: [dict]
      - data/items/results/quotes: [dict]
      - rows_matrix + keys/headers
      - nested data dicts
    """
    if obj is None:
        return []

    if isinstance(obj, list):
        out: List[Dict[str, Any]] = []
        for item in obj:
            if isinstance(item, dict):
                out.append(dict(item))
            else:
                d = _to_payload(item)
                if d:
                    out.append(d)
        return out

    if isinstance(obj, dict):
        if isinstance(obj.get("data"), dict):
            nested_rows = _extract_rows_from_any(obj.get("data"))
            if nested_rows:
                return nested_rows

        for key in ("rows", "data", "items", "quotes", "results", "records"):
            v = obj.get(key)
            if isinstance(v, list):
                if v and isinstance(v[0], dict):
                    return [dict(r) for r in v if isinstance(r, dict)]
                if v and isinstance(v[0], (list, tuple)):
                    keys = obj.get("keys") or obj.get("headers") or obj.get("columns") or []
                    if isinstance(keys, list) and keys:
                        return _matrix_to_rows(v, [str(k) for k in keys])

        rows_matrix = obj.get("rows_matrix") or obj.get("matrix")
        if isinstance(rows_matrix, list) and rows_matrix:
            keys = obj.get("keys") or obj.get("headers") or obj.get("columns") or []
            if isinstance(keys, list) and keys:
                return _matrix_to_rows(rows_matrix, [str(k) for k in keys])

        return []

    d = _to_payload(obj)
    if d:
        return _extract_rows_from_any(d)

    return []


def _env_int(name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        val = int(str(os.getenv(name, default)).strip())
    except Exception:
        val = int(default)
    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


def _env_csv(name: str) -> List[str]:
    raw = _safe_str(os.getenv(name, ""))
    if not raw:
        return []
    return _dedupe_keep_order([p.strip() for p in re.split(r"[,\;\|\n]+", raw) if p.strip()])


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

    # New in v3.6.0
    pages_explicit: bool = False

    def horizon(self) -> str:
        return map_period_days_to_horizon(self.invest_period_days)


def _criteria_from_dict(d: Dict[str, Any]) -> Criteria:
    page_keys = (
        "pages_selected",
        "pages",
        "selected_pages",
        "pagesSelected",
        "page_selection",
    )
    explicit_pages = any(k in d and d.get(k) not in (None, "", [], (), {}) for k in page_keys)

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

    enforce = _coerce_bool(d.get("enforce_risk_confidence", True), True)

    top_n = int(_safe_float(d.get("top_n") or d.get("limit") or 10, 10) or 10)
    top_n = max(1, min(50, top_n))

    enrich_final = _coerce_bool(d.get("enrich_final", True), True)

    min_vol = _safe_float(d.get("min_volume") or d.get("min_liquidity") or d.get("min_vol"), None)
    use_liq = _coerce_bool(d.get("use_liquidity_tiebreak", True), True)

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
        pages_explicit=explicit_pages,
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
    crit = _criteria_from_dict(d)
    if not crit.pages_explicit:
        crit.pages_explicit = bool(getattr(base, "pages_explicit", False))
    return crit


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
        pages_explicit=False,
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


def _request_to_payload(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    if isinstance(request, dict):
        return dict(request)
    return _to_payload(request)


def _settings_to_payload(settings: Any) -> Dict[str, Any]:
    if settings is None:
        return {}
    if isinstance(settings, dict):
        out = dict(settings)
    else:
        out = _to_payload(settings)

    nested = out.get("criteria")
    if isinstance(nested, dict):
        merged = dict(out)
        merged.update(nested)
        return merged
    return out


def _extract_body_criteria(body: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(body, dict):
        return {}

    merged: Dict[str, Any] = {}
    merged.update(body)

    for nested_key in ("criteria", "filters", "criteria_snapshot", "settings"):
        nested = body.get(nested_key)
        if isinstance(nested, dict):
            merged.update(nested)
            if isinstance(nested.get("criteria"), dict):
                merged.update(nested.get("criteria"))

    return merged


def _normalize_public_extras(extra: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Preserve unknown route kwargs by merging them into body-like payload.
    This avoids dispatcher TypeError / incompatibility when route builders pass
    sheet/page/tab/request_id/symbols/tickers/etc.
    """
    out = dict(extra or {})
    # Remove internal noise if present
    for k in list(out.keys()):
        if k.startswith("_"):
            out.pop(k, None)
    return out


def _merge_body_with_extras(body: Optional[Dict[str, Any]], extra: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if isinstance(body, dict):
        merged.update(body)
    extra2 = _normalize_public_extras(extra)
    # Put extras after body so dispatcher/runtime keys are visible to downstream parsing
    merged.update({k: v for k, v in extra2.items() if v is not None})
    return merged


def _resolve_limit(
    limit: Any,
    criteria: Optional[Dict[str, Any]],
    body: Optional[Dict[str, Any]],
    settings: Optional[Dict[str, Any]] = None,
    request_payload: Optional[Dict[str, Any]] = None,
) -> int:
    body_criteria = _extract_body_criteria(body)
    settings_criteria = _extract_body_criteria(settings or {})
    request_criteria = _extract_body_criteria(request_payload or {})

    v = _first_non_none(
        limit,
        (criteria or {}).get("top_n") if isinstance(criteria, dict) else None,
        (criteria or {}).get("limit") if isinstance(criteria, dict) else None,
        settings_criteria.get("top_n"),
        settings_criteria.get("limit"),
        body_criteria.get("top_n"),
        body_criteria.get("limit"),
        request_criteria.get("top_n"),
        request_criteria.get("limit"),
        10,
    )
    n = int(_safe_float(v, 10) or 10)
    return max(1, min(50, n))


def _merge_criteria_sources(
    *,
    engine: Any,
    criteria: Optional[Dict[str, Any]],
    body: Optional[Dict[str, Any]],
    settings: Optional[Dict[str, Any]],
    request_payload: Optional[Dict[str, Any]],
    limit: Optional[int],
) -> Criteria:
    base = load_criteria_best_effort(engine)

    merged: Dict[str, Any] = {}
    merged.update(_extract_body_criteria(request_payload or {}))
    merged.update(_extract_body_criteria(settings or {}))
    merged.update(_extract_body_criteria(body or {}))

    if isinstance(criteria, dict):
        merged.update(criteria)

    crit = merge_criteria_overrides(base, merged)
    crit.top_n = _resolve_limit(limit, criteria, body, settings=settings, request_payload=request_payload)

    explicit_pages = any(
        k in merged and merged.get(k) not in (None, "", [], (), {})
        for k in ("pages_selected", "pages", "selected_pages", "pagesSelected", "page_selection")
    )
    if explicit_pages:
        crit.pages_explicit = True

    return crit


def _extract_route_mode(mode: Any, body: Optional[Dict[str, Any]], settings: Optional[Dict[str, Any]], request_payload: Optional[Dict[str, Any]]) -> str:
    m = _safe_str(mode)
    if m:
        return m
    for src in (body, settings, request_payload):
        if isinstance(src, dict):
            mv = _safe_str(src.get("mode"))
            if mv:
                return mv
    return ""


def _extract_direct_symbols_from_sources(*sources: Optional[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for src in sources:
        payload = _extract_body_criteria(src if isinstance(src, dict) else {})
        for key in (
            "symbols",
            "tickers",
            "tickers_list",
            "symbol_list",
            "selected_symbols",
            "direct_symbols",
        ):
            vals = _coerce_to_list(payload.get(key))
            for v in vals:
                sym = _normalize_symbol(_safe_str(v))
                if sym:
                    out.append(sym)
    return _dedupe_keep_order(out)


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


def _default_pages_for_unconstrained(pages: List[str]) -> List[str]:
    env_pages = _env_csv("TOP10_UNCONSTRAINED_DEFAULT_PAGES")
    if env_pages:
        return _dedupe_keep_order(env_pages)

    preferred_order = ["Market_Leaders", "Global_Markets", "My_Portfolio", "Mutual_Funds", "Commodities_FX"]
    preferred = [p for p in preferred_order if p in pages]
    if preferred:
        return preferred[:2]
    return pages[:2] if pages else ["Market_Leaders", "Global_Markets"]


def _compute_dynamic_limits(criteria: Criteria, pages: List[str], direct_symbols: List[str], unconstrained: bool) -> Tuple[int, int, int]:
    """
    Returns:
      per_page_limit
      max_universe_symbols
      row_fallback_limit
    """
    base_per_page = _env_int("TOP10_UNIVERSE_LIMIT_PER_PAGE", 250, lo=25, hi=5000)
    base_total = _env_int("TOP10_MAX_UNIVERSE_SYMBOLS", 250, lo=25, hi=5000)
    top_n = max(1, int(criteria.top_n or 10))

    if direct_symbols:
        wanted = max(len(direct_symbols), top_n * 4)
        return (
            min(base_per_page, max(10, wanted)),
            min(base_total, max(10, wanted)),
            min(base_per_page, max(10, wanted)),
        )

    if unconstrained:
        per_page = min(base_per_page, max(25, top_n * 8))
        total = min(base_total, max(40, top_n * 20))
        row_limit = min(per_page, max(20, top_n * 5))
        return per_page, total, row_limit

    if len(pages) >= 4:
        per_page = min(base_per_page, max(40, top_n * 15))
        total = min(base_total, max(80, top_n * 35))
        row_limit = min(per_page, max(30, top_n * 8))
        return per_page, total, row_limit

    per_page = min(base_per_page, max(50, top_n * 20))
    total = min(base_total, max(100, top_n * 50))
    row_limit = min(per_page, max(40, top_n * 10))
    return per_page, total, row_limit


def _allow_row_fallback(criteria: Criteria, direct_symbols: List[str], pages: List[str]) -> bool:
    if direct_symbols:
        return True

    env_force = os.getenv("TOP10_ENABLE_ROW_FALLBACK")
    if env_force is not None:
        return _coerce_bool(env_force, True)

    # Broad or unconstrained requests should avoid very expensive row fallback scans.
    if not criteria.pages_explicit and len(pages) > 2:
        return False

    if criteria.pages_explicit and len(pages) <= 2:
        return True

    return len(pages) <= 2


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
                if inspect.isawaitable(snap):
                    return None
                if isinstance(snap, dict):
                    return snap
            except TypeError:
                try:
                    snap = fn(page)
                    if inspect.isawaitable(snap):
                        return None
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
    rows_matrix = snapshot.get("rows_matrix") or snapshot.get("matrix")

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

    if isinstance(rows_matrix, list) and rows_matrix:
        cols = keys if isinstance(keys, list) and keys else headers
        if isinstance(cols, list) and cols:
            return _matrix_to_rows(rows_matrix, [str(c) for c in cols])

    return []


async def _call_engine_variants(engine: Any, method_names: Sequence[str], variants: Sequence[Dict[str, Any]], positional: Optional[List[Any]] = None) -> Any:
    if engine is None:
        return None
    for name in method_names:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        for kwargs in variants:
            try:
                out = fn(**kwargs)
                return await _maybe_await(out)
            except TypeError:
                pass
            except Exception:
                continue
        if positional:
            try:
                out = fn(*positional)
                return await _maybe_await(out)
            except Exception:
                continue
    return None


async def _get_page_rows_live(engine: Any, page: str, *, limit: int = 5000, mode: str = "", body: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    if engine is None:
        return []

    body_obj = dict(body or {})
    variants = [
        {"sheet": page, "sheet_name": page, "page": page, "limit": limit, "mode": mode, "body": body_obj},
        {"sheet": page, "page": page, "limit": limit, "mode": mode},
        {"sheet_name": page, "limit": limit, "mode": mode, "body": body_obj},
        {"page": page, "limit": limit, "mode": mode, "body": body_obj},
        {"sheet": page, "limit": limit},
        {"page": page, "limit": limit},
        {"sheet_name": page, "limit": limit},
    ]

    out = await _call_engine_variants(
        engine,
        method_names=(
            "get_sheet_rows",
            "get_page_rows",
            "sheet_rows",
            "build_sheet_rows",
            "get_sheet",
        ),
        variants=variants,
        positional=[page],
    )

    rows = _extract_rows_from_any(out)
    if rows:
        return rows

    snap = _get_snapshot(engine, page)
    return _row_list_from_snapshot(snap)


async def _get_universe_symbols_live(
    engine: Any,
    page: str,
    *,
    limit: int = 5000,
    mode: str = "",
    body: Optional[Dict[str, Any]] = None,
    allow_rows_fallback: bool = True,
) -> List[str]:
    if engine is None:
        return []

    body_obj = dict(body or {})
    variants = [
        {"page": page, "limit": limit, "mode": mode, "body": body_obj},
        {"sheet": page, "limit": limit, "mode": mode, "body": body_obj},
        {"sheet_name": page, "limit": limit, "mode": mode, "body": body_obj},
        {"page": page, "limit": limit},
        {"sheet": page, "limit": limit},
        {"sheet_name": page, "limit": limit},
    ]

    out = await _call_engine_variants(
        engine,
        method_names=(
            "get_page_symbols",
            "get_sheet_symbols",
            "list_symbols_for_page",
            "list_symbols",
            "get_symbols",
        ),
        variants=variants,
        positional=[page],
    )

    if isinstance(out, (list, tuple)):
        syms = [_normalize_symbol(_safe_str(x)) for x in out]
        syms = [s for s in syms if s]
        if syms:
            return _dedupe_keep_order(syms)

    if isinstance(out, dict):
        for key in ("symbols", "tickers", "items", "data"):
            v = out.get(key)
            if isinstance(v, (list, tuple)):
                syms = [_normalize_symbol(_safe_str(x)) for x in v]
                syms = [s for s in syms if s]
                if syms:
                    return _dedupe_keep_order(syms)

    fn2 = getattr(engine, "symbols_reader", None)
    if fn2 is not None and hasattr(fn2, "get_symbols_for_sheet"):
        try:
            out2 = fn2.get_symbols_for_sheet(page)  # type: ignore[attr-defined]
            out2 = await _maybe_await(out2)
            if isinstance(out2, (list, tuple)):
                syms = [_normalize_symbol(_safe_str(x)) for x in out2]
                syms = [s for s in syms if s]
                if syms:
                    return _dedupe_keep_order(syms)
        except Exception:
            pass

    if not allow_rows_fallback:
        return []

    rows = await _get_page_rows_live(engine, page, limit=limit, mode=mode, body=body)
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


def _symbol_from_quote_like(d: Dict[str, Any]) -> str:
    return _normalize_symbol(
        _safe_str(
            _first_non_none(
                d.get("symbol"),
                d.get("ticker"),
                d.get("code"),
                d.get("Symbol"),
                d.get("Ticker"),
                d.get("Code"),
            )
        )
    )


async def _fetch_quotes_map(engine: Any, symbols: List[str], *, mode: str = "", body: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    if not engine or not symbols:
        return {}

    body_obj = dict(body or {})
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
                res = fn(symbols=symbols, mode=mode or "", body=body_obj)
            except TypeError:
                try:
                    res = fn(symbols=symbols, mode=mode or "")
                except TypeError:
                    try:
                        res = fn(symbols)
                    except TypeError:
                        res = fn(symbols, mode=mode or "")
            res = await _maybe_await(res)

            if isinstance(res, dict):
                if _dict_is_symbol_map(res, symbols):
                    return {s: _coerce_mapping(res.get(s)) for s in symbols}

                nested = res.get("data") or res.get("rows") or res.get("items") or res.get("quotes") or res.get("results")
                if isinstance(nested, dict) and _dict_is_symbol_map(nested, symbols):
                    return {s: _coerce_mapping(nested.get(s)) for s in symbols}

                rows = _extract_rows_from_any(res)
                if rows:
                    out_rows: Dict[str, Dict[str, Any]] = {}
                    for item in rows:
                        sym = _symbol_from_quote_like(item)
                        if sym:
                            out_rows[sym] = dict(item)
                    if out_rows:
                        return {s: out_rows.get(s, {}) for s in symbols}

            elif isinstance(res, list):
                out_rows2: Dict[str, Dict[str, Any]] = {}
                for item in res:
                    d = _to_payload(item)
                    sym = _symbol_from_quote_like(d)
                    if sym:
                        out_rows2[sym] = d
                if out_rows2:
                    return {s: out_rows2.get(s, {}) for s in symbols}
                return {s: _coerce_mapping(v) for s, v in zip(symbols, res)}
        except Exception:
            continue

    out: Dict[str, Dict[str, Any]] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)

    for s in symbols:
        try:
            if callable(per_dict_fn):
                try:
                    out[s] = _coerce_mapping(await _maybe_await(per_dict_fn(s, mode=mode or "", body=body_obj)))
                except TypeError:
                    try:
                        out[s] = _coerce_mapping(await _maybe_await(per_dict_fn(s, mode=mode or "")))
                    except TypeError:
                        out[s] = _coerce_mapping(await _maybe_await(per_dict_fn(s)))
            elif callable(per_fn):
                try:
                    out[s] = _coerce_mapping(await _maybe_await(per_fn(s, mode=mode or "", body=body_obj)))
                except TypeError:
                    try:
                        out[s] = _coerce_mapping(await _maybe_await(per_fn(s, mode=mode or "")))
                    except TypeError:
                        out[s] = _coerce_mapping(await _maybe_await(per_fn(s)))
            else:
                out[s] = {"symbol": s, "warnings": "engine_missing_quote_methods"}
        except Exception as e:
            out[s] = {"symbol": s, "warnings": f"quote_error:{e}"}

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
    v = _first_non_none(
        d.get("forecast_confidence"),
        d.get("confidence_score"),
        d.get("confidence"),
        _dict_get_ci(d, "Forecast Confidence", "Confidence Score", "Confidence"),
    )
    f = _safe_float(v, 0.0) or 0.0
    f = float(f / 100.0) if f > 1.5 else float(f)
    return _clamp(f, 0.0, 1.0)


def _get_overall_score(d: Dict[str, Any]) -> float:
    v = _first_non_none(
        d.get("overall_score"),
        d.get("opportunity_score"),
        d.get("score"),
        _dict_get_ci(d, "Overall Score", "Opportunity Score", "Score"),
    )
    f = _safe_float(v, 0.0) or 0.0
    f = float(f * 100.0) if 0.0 < f <= 1.5 else float(f)
    return _clamp(f, 0.0, 100.0)


def _get_risk_score(d: Dict[str, Any]) -> float:
    v = _first_non_none(
        d.get("risk_score"),
        d.get("risk"),
        _dict_get_ci(d, "Risk Score", "Risk"),
    )
    f = _safe_float(v, None)
    if f is None:
        bucket = _first_non_none(d.get("risk_bucket"), _dict_get_ci(d, "Risk Bucket"))
        return _risk_from_bucket(_safe_str(bucket))
    f = float(f * 100.0) if 0.0 < f <= 1.5 else float(f)
    return _clamp(f, 0.0, 100.0)


def _get_volume(d: Dict[str, Any]) -> float:
    v = _first_non_none(
        d.get("volume"),
        d.get("avg_volume_10d"),
        d.get("avg_volume_30d"),
        _dict_get_ci(d, "Volume", "Avg Volume 10D", "Avg Volume 30D"),
    )
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


def _extract_roi_from_any(d: Dict[str, Any], horizon: str) -> Optional[float]:
    roi_key = horizon_to_roi_key(horizon)
    candidates = [
        d.get(roi_key),
        d.get("expected_roi_3m"),
        d.get("expected_roi_1m"),
        d.get("expected_roi_12m"),
        d.get("upside_pct"),
        d.get("target_return"),
        d.get("expected_return"),
        d.get("return_pct"),
        _dict_get_ci(
            d,
            roi_key,
            "Expected ROI 3M",
            "Expected ROI 1M",
            "Expected ROI 12M",
            "Upside %",
            "Target Return",
            "Expected Return",
        ),
    ]
    for v in candidates:
        roi = _as_fraction(v)
        if roi is not None:
            return roi
    return None


def _extract_candidate_from_quote(*, sym: str, page: str, quote: Dict[str, Any], horizon: str) -> Optional[Candidate]:
    roi = _extract_roi_from_any(quote, horizon)
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

    roi = _extract_roi_from_any(row, horizon)
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


def _make_placeholder_candidate(sym: str, page: str, quote_like: Optional[Dict[str, Any]] = None) -> Candidate:
    q = dict(quote_like or {})
    q["symbol"] = sym
    q["source_page"] = page
    q.setdefault("warnings", "placeholder_candidate_no_roi")
    q.setdefault("expected_roi_3m", 0.0)
    q.setdefault("forecast_confidence", _get_confidence(q))
    q.setdefault("overall_score", _get_overall_score(q))
    q.setdefault("risk_score", _get_risk_score(q))
    return Candidate(
        symbol=sym,
        source_page=page,
        roi=0.0,
        confidence=float(_get_confidence(q)),
        overall_score=float(_get_overall_score(q)),
        risk_score=float(_get_risk_score(q)),
        volume=float(_get_volume(q)),
        row=q,
    )


def _rank_key(c: Candidate, *, use_liquidity: bool) -> Tuple[float, float, float, float]:
    vol = c.volume if use_liquidity else 0.0
    return (c.roi, c.confidence, vol, c.overall_score)


# =============================================================================
# Optional enrichment for final Top-N
# =============================================================================
async def _enrich_top(engine: Any, symbols: List[str], *, mode: str = "", body: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    if engine is None or not symbols:
        return {}

    try:
        res = await _fetch_quotes_map(engine, symbols, mode=mode or "", body=body or {})
        if isinstance(res, dict) and res:
            return {s: _coerce_mapping(res.get(s)) for s in symbols}
    except Exception:
        pass

    out: Dict[str, Dict[str, Any]] = {}
    fn = getattr(engine, "get_enriched_quote", None)
    if not callable(fn):
        return out

    conc = _env_int("TOP10_ENRICH_CONCURRENCY", 8, lo=3, hi=20)
    sem = asyncio.Semaphore(conc)

    async def one(sym: str) -> None:
        async with sem:
            try:
                try:
                    r = fn(sym, mode=mode or "", body=body or {})
                except TypeError:
                    try:
                        r = fn(sym, mode=mode or "")
                    except TypeError:
                        r = fn(sym)
                r = await _maybe_await(r)
                out[sym] = _coerce_mapping(r)
            except Exception:
                out[sym] = {"symbol": sym, "warnings": "enrich_failed"}

    await asyncio.gather(*(one(s) for s in symbols), return_exceptions=True)
    return out


# =============================================================================
# Core selection
# =============================================================================
async def _fetch_page_symbols_concurrent(
    engine: Any,
    pages: List[str],
    *,
    per_page_limit: int,
    mode: str,
    body: Optional[Dict[str, Any]],
) -> Dict[str, List[str]]:
    concurrency = _env_int("TOP10_PAGE_FETCH_CONCURRENCY", 6, lo=1, hi=20)
    sem = asyncio.Semaphore(concurrency)
    out: Dict[str, List[str]] = {}

    async def one(page: str) -> None:
        async with sem:
            try:
                syms = await _get_universe_symbols_live(
                    engine,
                    page,
                    limit=per_page_limit,
                    mode=mode,
                    body=body,
                    allow_rows_fallback=False,
                )
                out[page] = syms
            except Exception:
                out[page] = []

    await asyncio.gather(*(one(p) for p in pages), return_exceptions=True)
    return out


async def _fetch_page_rows_concurrent(
    engine: Any,
    pages: List[str],
    *,
    row_limit: int,
    mode: str,
    body: Optional[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    concurrency = _env_int("TOP10_PAGE_ROW_FETCH_CONCURRENCY", 4, lo=1, hi=12)
    sem = asyncio.Semaphore(concurrency)
    out: Dict[str, List[Dict[str, Any]]] = {}

    async def one(page: str) -> None:
        async with sem:
            try:
                rows = await _get_page_rows_live(engine, page, limit=row_limit, mode=mode, body=body)
                out[page] = rows
            except Exception:
                out[page] = []

    await asyncio.gather(*(one(p) for p in pages), return_exceptions=True)
    return out


async def select_top10(
    *,
    engine: Any,
    criteria: Criteria,
    mode: str = "",
    direct_symbols: Optional[List[str]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Candidate], Dict[str, Any]]:
    t0 = time.time()

    try:
        stage_started = time.time()

        horizon = criteria.horizon()
        roi_key = horizon_to_roi_key(horizon)

        pages = _eligible_pages(criteria)
        warnings: List[str] = []
        fallback_trace: List[str] = []
        stage_ms: Dict[str, float] = {}

        direct_symbols = [_normalize_symbol(s) for s in (direct_symbols or []) if _normalize_symbol(s)]
        direct_symbols = _dedupe_keep_order(direct_symbols)

        unconstrained = (not direct_symbols) and (not bool(getattr(criteria, "pages_explicit", False)))
        if unconstrained:
            narrowed = _default_pages_for_unconstrained(pages)
            if narrowed != pages:
                warnings.append(f"unconstrained_request_pages_narrowed:{','.join(narrowed)}")
            pages = narrowed

        per_page_limit, max_universe_symbols, row_fallback_limit = _compute_dynamic_limits(criteria, pages, direct_symbols, unconstrained)
        allow_row_fallback = _allow_row_fallback(criteria, direct_symbols, pages)

        universe: List[Tuple[str, str]] = []

        if direct_symbols:
            default_page = pages[0] if pages else "Market_Leaders"
            for s in direct_symbols:
                universe.append((s, default_page))
            warnings.append(f"used_direct_symbols:{len(direct_symbols)}")

        # ---------------------------------------------------------------------
        # Stage 1: symbol universe discovery (symbols only, concurrent)
        # ---------------------------------------------------------------------
        page_symbol_map: Dict[str, List[str]] = {}
        if not universe and pages:
            try:
                page_symbol_map = await _fetch_page_symbols_concurrent(
                    engine,
                    pages,
                    per_page_limit=per_page_limit,
                    mode=mode,
                    body=body,
                )
            except Exception as e:
                warnings.append(f"page_symbol_fetch_error:{e}")
                page_symbol_map = {}

            for page in pages:
                page_symbols = page_symbol_map.get(page) or []
                if page_symbols:
                    for s in page_symbols:
                        universe.append((s, page))
                else:
                    warnings.append(f"universe_empty:{page}")

        stage_ms["universe_symbols_ms"] = round((time.time() - stage_started) * 1000.0, 3)

        # Lazy row-only universe fallback if symbols are totally empty and request is not broad/unconstrained
        if not universe and pages and allow_row_fallback:
            stage_started = time.time()
            try:
                page_rows_map = await _fetch_page_rows_concurrent(
                    engine,
                    pages,
                    row_limit=row_fallback_limit,
                    mode=mode,
                    body=body,
                )
            except Exception as e:
                warnings.append(f"page_row_discovery_error:{e}")
                page_rows_map = {}

            for page in pages:
                rows = page_rows_map.get(page) or []
                if rows:
                    discovered = 0
                    for row in rows:
                        sym = _extract_symbol_from_row(row)
                        if sym:
                            universe.append((sym, page))
                            discovered += 1
                    if discovered:
                        fallback_trace.append(f"rows_only_universe:{page}:{discovered}")

            stage_ms["row_universe_fallback_ms"] = round((time.time() - stage_started) * 1000.0, 3)
        elif not universe and pages and not allow_row_fallback:
            warnings.append("skipped_row_universe_fallback_for_broad_or_unconstrained_request")

        # Deduplicate and cap universe BEFORE quote collection
        seen_u: Set[str] = set()
        universe_dedup: List[Tuple[str, str]] = []
        for s, p in universe:
            if s and s not in seen_u:
                seen_u.add(s)
                universe_dedup.append((s, p))

        if len(universe_dedup) > max_universe_symbols:
            warnings.append(f"universe_capped:{len(universe_dedup)}->{max_universe_symbols}")
            universe_dedup = universe_dedup[:max_universe_symbols]

        symbols = [s for s, _ in universe_dedup]
        source_map = {s: p for s, p in universe_dedup}

        # ---------------------------------------------------------------------
        # Stage 2: quote collection
        # ---------------------------------------------------------------------
        stage_started = time.time()
        qmap: Dict[str, Dict[str, Any]] = {}
        if symbols:
            qmap = await _fetch_quotes_map(engine, symbols, mode=mode or "", body=body or {})
        stage_ms["quote_fetch_ms"] = round((time.time() - stage_started) * 1000.0, 3)

        # ---------------------------------------------------------------------
        # Stage 3: primary candidate extraction from quotes
        # ---------------------------------------------------------------------
        stage_started = time.time()
        candidates: List[Candidate] = []
        quote_candidates_unfiltered: List[Candidate] = []

        for sym in symbols:
            q = qmap.get(sym) or {}
            if not isinstance(q, dict) or not q:
                continue

            cand = _extract_candidate_from_quote(sym=sym, page=source_map.get(sym, ""), quote=q, horizon=horizon)
            if cand is None:
                continue

            quote_candidates_unfiltered.append(cand)

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

        stage_ms["quote_candidate_filter_ms"] = round((time.time() - stage_started) * 1000.0, 3)

        # ---------------------------------------------------------------------
        # Stage 4: lazy row fallback only if still empty and allowed
        # ---------------------------------------------------------------------
        row_candidates_unfiltered: List[Candidate] = []
        if not candidates and pages and allow_row_fallback:
            stage_started = time.time()
            row_based_count = 0
            try:
                page_rows_map2 = await _fetch_page_rows_concurrent(
                    engine,
                    pages,
                    row_limit=row_fallback_limit,
                    mode=mode,
                    body=body,
                )
            except Exception as e:
                warnings.append(f"row_candidate_fetch_error:{e}")
                page_rows_map2 = {}

            for page in pages:
                rows = page_rows_map2.get(page) or []
                for row in rows:
                    cand = _extract_candidate_from_row(page=page, row=row, horizon=horizon)
                    if cand is None:
                        continue

                    row_candidates_unfiltered.append(cand)

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
            stage_ms["row_candidate_fallback_ms"] = round((time.time() - stage_started) * 1000.0, 3)
        elif not candidates and pages and not allow_row_fallback:
            warnings.append("skipped_row_candidate_fallback_for_broad_or_unconstrained_request")

        # ---------------------------------------------------------------------
        # Stage 5: relaxed fallbacks
        # ---------------------------------------------------------------------
        if not candidates and quote_candidates_unfiltered:
            candidates = list(quote_candidates_unfiltered)
            warnings.append(f"used_relaxed_quote_fallback:{len(candidates)}")

        if not candidates and row_candidates_unfiltered:
            candidates = list(row_candidates_unfiltered)
            warnings.append(f"used_relaxed_row_fallback:{len(candidates)}")

        if not candidates and direct_symbols:
            relaxed_count = 0
            if not qmap:
                qmap = await _fetch_quotes_map(engine, direct_symbols, mode=mode or "", body=body or {})

            for sym in direct_symbols:
                q = qmap.get(sym) or {}
                if not isinstance(q, dict):
                    q = {}

                roi = _extract_roi_from_any(q, horizon)
                conf = _get_confidence(q)
                overall = _get_overall_score(q)
                risk = _get_risk_score(q)
                vol = _get_volume(q)

                row = dict(q)
                row["symbol"] = sym
                row["source_page"] = source_map.get(sym, pages[0] if pages else "Market_Leaders")

                if roi is None:
                    roi = 0.0
                    row.setdefault("warnings", "direct_symbol_relaxed_without_roi")

                candidates.append(
                    Candidate(
                        symbol=sym,
                        source_page=row["source_page"],
                        roi=float(roi),
                        confidence=float(conf),
                        overall_score=float(overall),
                        risk_score=float(risk),
                        volume=float(vol),
                        row=row,
                    )
                )
                relaxed_count += 1

            if relaxed_count > 0:
                warnings.append(f"used_direct_symbol_relaxed_fallback:{relaxed_count}")

        if not candidates and universe_dedup:
            placeholder_count = 0
            for sym, page in universe_dedup[: max(1, int(criteria.top_n or 10))]:
                candidates.append(_make_placeholder_candidate(sym, page, qmap.get(sym)))
                placeholder_count += 1
            if placeholder_count > 0:
                warnings.append(f"used_placeholder_symbol_fallback:{placeholder_count}")

        # ---------------------------------------------------------------------
        # Stage 6: dedupe + rank + final top
        # ---------------------------------------------------------------------
        stage_started = time.time()
        best: Dict[str, Candidate] = {}
        for c in candidates:
            prev = best.get(c.symbol)
            if prev is None or _rank_key(c, use_liquidity=criteria.use_liquidity_tiebreak) > _rank_key(prev, use_liquidity=criteria.use_liquidity_tiebreak):
                best[c.symbol] = c

        deduped = list(best.values())
        deduped.sort(key=lambda c: _rank_key(c, use_liquidity=criteria.use_liquidity_tiebreak), reverse=True)

        top_n = max(1, int(criteria.top_n or 10))
        top = deduped[:top_n]
        stage_ms["rank_and_slice_ms"] = round((time.time() - stage_started) * 1000.0, 3)

        # ---------------------------------------------------------------------
        # Stage 7: enrich final Top-N only
        # ---------------------------------------------------------------------
        stage_started = time.time()
        enrich_map: Dict[str, Dict[str, Any]] = {}
        if criteria.enrich_final and top:
            try:
                enrich_map = await _enrich_top(engine, [c.symbol for c in top], mode=mode or "", body=body or {})
            except Exception as e:
                warnings.append(f"enrich_failed:{e}")
                enrich_map = {}
        stage_ms["final_enrich_ms"] = round((time.time() - stage_started) * 1000.0, 3)

        for c in top:
            e = enrich_map.get(c.symbol)
            if isinstance(e, dict) and e:
                merged = dict(c.row)
                merged.update(e)
                merged["symbol"] = c.symbol
                merged["source_page"] = c.source_page
                c.row = merged

        total_ms = round((time.time() - t0) * 1000.0, 3)

        meta = {
            "version": TOP10_SELECTOR_VERSION,
            "mode": mode,
            "horizon": horizon,
            "roi_key": roi_key,
            "pages_selected": pages,
            "pages_explicit": bool(getattr(criteria, "pages_explicit", False)),
            "request_unconstrained": unconstrained,
            "direct_symbols_count": len(direct_symbols or []),
            "per_page_limit": per_page_limit,
            "row_fallback_limit": row_fallback_limit,
            "max_universe_symbols": max_universe_symbols,
            "allow_row_fallback": allow_row_fallback,
            "universe_symbols": len(symbols),
            "quotes_returned": len([s for s, q in qmap.items() if isinstance(q, dict) and q]),
            "quote_candidates_unfiltered": len(quote_candidates_unfiltered),
            "row_candidates_unfiltered": len(row_candidates_unfiltered),
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
            "fallback_trace": fallback_trace,
            "stage_durations_ms": stage_ms,
            "timestamp_utc": _now_utc_iso(),
            "timestamp_riyadh": _now_riyadh_iso(),
            "duration_ms": total_ms,
        }
        return top, meta

    except Exception as e:
        logger.exception("Top10 select_top10 fatal error")
        return [], {
            "version": TOP10_SELECTOR_VERSION,
            "mode": mode,
            "build_status": "ERROR",
            "error": _safe_str(e),
            "error_type": type(e).__name__,
            "timestamp_utc": _now_utc_iso(),
            "timestamp_riyadh": _now_riyadh_iso(),
            "duration_ms": round((time.time() - t0) * 1000.0, 3),
            "warnings": ["select_top10_failed"],
        }


# =============================================================================
# Schema helpers for Top_10_Investments
# =============================================================================
def _columns_from_spec_like(spec: Any) -> List[Any]:
    if spec is None:
        return []

    cols = getattr(spec, "columns", None)
    if isinstance(cols, list) and cols:
        return cols

    d = _to_payload(spec)
    if d:
        cols2 = d.get("columns")
        if isinstance(cols2, list) and cols2:
            return cols2

    return []


def _registry_sheet_obj(registry: Any, sheet_name: str) -> Any:
    if registry is None:
        return None

    # direct mapping
    if isinstance(registry, dict):
        if sheet_name in registry:
            return registry.get(sheet_name)
        norm = {_norm_key(k): v for k, v in registry.items()}
        return norm.get(_norm_key(sheet_name))

    # object with attributes
    for attr in ("sheets", "registry", "specs", "schemas", "sheet_specs"):
        obj = getattr(registry, attr, None)
        if isinstance(obj, dict):
            if sheet_name in obj:
                return obj.get(sheet_name)
            norm = {_norm_key(k): v for k, v in obj.items()}
            hit = norm.get(_norm_key(sheet_name))
            if hit is not None:
                return hit

    return None


def _get_top10_schema() -> Tuple[List[str], List[str], Set[str], str, List[str]]:
    # Primary: canonical schema registry function
    try:
        from core.sheets import schema_registry as sr  # type: ignore

        # 1) get_sheet_spec(...)
        get_sheet_spec = getattr(sr, "get_sheet_spec", None)
        if callable(get_sheet_spec):
            for name in (TOP10_PAGE_NAME, "Top10_Investments", "Top 10 Investments"):
                try:
                    spec = get_sheet_spec(name)
                    cols = _columns_from_spec_like(spec)
                    if cols:
                        headers: List[str] = []
                        keys: List[str] = []
                        pct_keys: Set[str] = set()
                        for c in cols:
                            cd = _coerce_mapping(c)
                            if not cd:
                                cd = {
                                    "header": _safe_str(getattr(c, "header", "")),
                                    "key": _safe_str(getattr(c, "key", "")),
                                    "dtype": _safe_str(getattr(c, "dtype", "")),
                                }
                            h = _safe_str(cd.get("header"))
                            k = _safe_str(cd.get("key"))
                            if not h or not k:
                                continue
                            headers.append(h)
                            keys.append(k)
                            dt = _safe_str(cd.get("dtype")).lower()
                            if dt in {"pct", "percent", "percentage"}:
                                pct_keys.add(k)

                        if headers and keys and len(headers) == len(keys):
                            hdrs2, keys2, pct2, injected = _ensure_schema_contract(headers, keys, pct_keys)
                            return hdrs2, keys2, pct2, "schema_registry.get_sheet_spec", injected
                except Exception:
                    pass

        # 2) registry-like dict/object
        for attr in ("SCHEMA_REGISTRY", "SHEET_SPECS", "REGISTRY", "SCHEMA_BY_SHEET", "SHEETS"):
            reg = getattr(sr, attr, None)
            sheet_obj = _registry_sheet_obj(reg, TOP10_PAGE_NAME)
            cols = _columns_from_spec_like(sheet_obj)
            if cols:
                headers = []
                keys = []
                pct_keys: Set[str] = set()
                for c in cols:
                    cd = _coerce_mapping(c)
                    if not cd:
                        cd = {
                            "header": _safe_str(getattr(c, "header", "")),
                            "key": _safe_str(getattr(c, "key", "")),
                            "dtype": _safe_str(getattr(c, "dtype", "")),
                        }
                    h = _safe_str(cd.get("header"))
                    k = _safe_str(cd.get("key"))
                    if not h or not k:
                        continue
                    headers.append(h)
                    keys.append(k)
                    dt = _safe_str(cd.get("dtype")).lower()
                    if dt in {"pct", "percent", "percentage"}:
                        pct_keys.add(k)

                if headers and keys and len(headers) == len(keys):
                    hdrs2, keys2, pct2, injected = _ensure_schema_contract(headers, keys, pct_keys)
                    return hdrs2, keys2, pct2, f"schema_registry.{attr}", injected
    except Exception:
        pass

    # Minimal but safe fallback
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
    hdrs2, keys2, pct2, injected = _ensure_schema_contract(headers, keys, pct_keys)
    return hdrs2, keys2, pct2, "fallback_minimal", injected


def _ensure_schema_contract(
    display_headers: Sequence[str],
    keys: Sequence[str],
    pct_keys: Set[str],
) -> Tuple[List[str], List[str], Set[str], List[str]]:
    hdrs = list(display_headers)
    ks = list(keys)
    missing: List[str] = []

    for field in TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(TOP10_REQUIRED_HEADERS.get(field, field))
            missing.append(field)

    return hdrs, ks, set(pct_keys or set()), missing


def _project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe_value(row.get(k, None)) for k in keys}


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
        "pages_explicit": criteria.pages_explicit,
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


def _mirror_runtime_aliases(raw: Dict[str, Any], *, rank: int, criteria: Criteria, horizon: str, mode: str) -> None:
    raw.setdefault("top10_rank", rank)
    raw.setdefault("selection_reason", _build_selection_reason(raw, roi_key=horizon_to_roi_key(horizon), criteria=criteria))
    raw.setdefault("criteria_snapshot", _json_dumps_safe(_build_criteria_snapshot(criteria, mode=mode)))

    if raw.get("Top10 Rank") is None:
        raw["Top10 Rank"] = raw.get("top10_rank")
    if raw.get("Selection Reason") is None:
        raw["Selection Reason"] = raw.get("selection_reason")
    if raw.get("Criteria Snapshot") is None:
        raw["Criteria Snapshot"] = raw.get("criteria_snapshot")


def _ensure_required_top10_fields(raw: Dict[str, Any], *, rank: int, criteria: Criteria, horizon: str, mode: str) -> None:
    roi_key = horizon_to_roi_key(horizon)
    forecast_price_key = horizon_to_forecast_price_key(horizon)

    raw.setdefault("name", raw.get("company_name") or raw.get("long_name") or raw.get("instrument_name") or raw.get("Name") or "")
    raw.setdefault("current_price", raw.get("price") or raw.get("last") or raw.get("close") or raw.get("Current Price"))
    raw.setdefault("forecast_confidence", raw.get("confidence_score") if raw.get("forecast_confidence") is None else raw.get("forecast_confidence"))

    if raw.get(roi_key) is None:
        for alt in (
            "expected_roi_3m",
            "expected_roi_1m",
            "expected_roi_12m",
            "upside_pct",
            "target_return",
            "expected_return",
            "return_pct",
        ):
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

    raw.setdefault("data_provider", raw.get("data_provider") or raw.get("provider") or raw.get("source") or "")
    raw.setdefault("provider", raw.get("data_provider") or raw.get("provider") or raw.get("source") or "")

    _mirror_runtime_aliases(raw, rank=rank, criteria=criteria, horizon=horizon, mode=mode)


def _schema_contains(keys: Sequence[str], needle: str) -> bool:
    return any(str(k) == needle for k in keys)


# =============================================================================
# Top10 output builder (internal)
# =============================================================================
def _build_top10_output_rows_internal(
    candidates: List[Candidate],
    *,
    criteria: Criteria,
    mode: str = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    display_headers, keys, pct_keys, schema_source, injected_fields = _get_top10_schema()
    horizon = criteria.horizon()
    roi_key = horizon_to_roi_key(horizon)
    forecast_price_key = horizon_to_forecast_price_key(horizon)

    warnings: List[str] = []
    rows_out: List[Dict[str, Any]] = []

    missing_required_fields = [f for f in TOP10_REQUIRED_FIELDS if not _schema_contains(keys, f)]

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
        "display_headers_len": len(display_headers),
        "keys_len": len(keys),
        "pct_keys_len": len(pct_keys),
        "horizon": horizon,
        "roi_key": roi_key,
        "forecast_price_key": forecast_price_key,
        "rows": len(rows_out),
        "selector_version": TOP10_SELECTOR_VERSION,
        "warnings": warnings,
        "missing_required_schema_fields": missing_required_fields,
        "injected_required_schema_fields": injected_fields,
        "required_top10_fields_present_in_schema": len(missing_required_fields) == 0,
        "header_map": [{"key": k, "header": h} for k, h in zip(keys, display_headers)],
    }
    return rows_out, meta


def _build_empty_top10_payload(
    *,
    criteria: Optional[Criteria],
    mode: str,
    warnings: Optional[List[str]] = None,
    error: Optional[str] = None,
    error_type: Optional[str] = None,
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    display_headers, keys, _pct_keys, schema_source, injected = _get_top10_schema()
    crit = criteria or Criteria(
        pages_selected=["Market_Leaders", "Global_Markets"],
        invest_period_days=90,
        top_n=10,
    )

    meta: Dict[str, Any] = {
        "version": TOP10_SELECTOR_VERSION,
        "selector_version": TOP10_SELECTOR_VERSION,
        "build_status": "WARN" if not error else "ERROR",
        "schema_source": schema_source,
        "required_top10_fields": list(TOP10_REQUIRED_FIELDS),
        "injected_required_schema_fields": injected,
        "criteria": {
            "pages_selected": crit.pages_selected,
            "pages_explicit": crit.pages_explicit,
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
        "warnings": _dedupe_keep_order(list(warnings or [])),
        "timestamp_utc": _now_utc_iso(),
        "timestamp_riyadh": _now_riyadh_iso(),
        "response_contract": {
            "headers_are_display_headers": True,
            "keys_present": True,
            "display_headers_present": True,
            "rows_are_projected_to_keys": True,
            "rows_matrix_present": True,
            "json_safe_payload": True,
        },
    }

    if error:
        meta["error"] = error
    if error_type:
        meta["error_type"] = error_type
    if meta_extra:
        meta.update(_json_safe_value(meta_extra) or {})

    payload = {
        "status": "error" if error else "warn",
        "page": TOP10_PAGE_NAME,
        "sheet": TOP10_PAGE_NAME,
        "route_family": "top10",
        "dispatch": "top10_selector",
        "headers": list(display_headers),
        "keys": list(keys),
        "display_headers": list(display_headers),
        "sheet_headers": list(display_headers),
        "column_headers": list(display_headers),
        "header_map": [{"key": k, "header": h} for k, h in zip(keys, display_headers)],
        "rows": [],
        "rows_matrix": [],
        "count": 0,
        "quotes": [],
        "meta": _json_safe_value(meta),
    }
    return _json_safe_value(payload)  # type: ignore[return-value]


def _finalize_payload_json_safe(payload: Dict[str, Any]) -> Dict[str, Any]:
    safe = _json_safe_value(payload)
    return safe if isinstance(safe, dict) else {}


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
    **kwargs: Any,
) -> List[str]:
    try:
        body_payload = _merge_body_with_extras(body, kwargs)
        request_payload = _request_to_payload(request)
        settings_payload = _settings_to_payload(settings)

        engine = engine or _resolve_engine_from_request(request)
        crit = _merge_criteria_sources(
            engine=engine,
            criteria=criteria,
            body=body_payload,
            settings=settings_payload,
            request_payload=request_payload,
            limit=limit,
        )
        mode2 = _extract_route_mode(mode, body_payload, settings_payload, request_payload)
        direct_symbols = _extract_direct_symbols_from_sources(criteria, body_payload, settings_payload, request_payload)

        top, _meta = await select_top10(
            engine=engine,
            criteria=crit,
            mode=mode2,
            direct_symbols=direct_symbols,
            body=body_payload or request_payload,
        )
        return [c.symbol for c in top]
    except Exception:
        logger.exception("Top10 select_top10_symbols failed")
        return []


async def build_top10_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
    **kwargs: Any,
) -> Dict[str, Any]:
    t0 = time.time()
    crit_fallback: Optional[Criteria] = None

    try:
        body_payload = _merge_body_with_extras(body, kwargs)
        request_payload = _request_to_payload(request)
        settings_payload = _settings_to_payload(settings)

        engine = engine or _resolve_engine_from_request(request)
        crit = _merge_criteria_sources(
            engine=engine,
            criteria=criteria,
            body=body_payload,
            settings=settings_payload,
            request_payload=request_payload,
            limit=limit,
        )
        crit_fallback = crit

        mode2 = _extract_route_mode(mode, body_payload, settings_payload, request_payload)
        direct_symbols = _extract_direct_symbols_from_sources(criteria, body_payload, settings_payload, request_payload)

        top, meta_sel = await select_top10(
            engine=engine,
            criteria=crit,
            mode=mode2,
            direct_symbols=direct_symbols,
            body=body_payload or request_payload,
        )
        rows, meta_rows = _build_top10_output_rows_internal(top, criteria=crit, mode=mode2)
        display_headers, keys, _pct_keys, _schema_source, _injected = _get_top10_schema()

        status_out = "success" if rows else "warn"
        warnings = list((meta_sel or {}).get("warnings") or [])
        warnings.extend(list((meta_rows or {}).get("warnings") or []))
        warnings = _dedupe_keep_order(warnings)
        if not rows:
            warnings.append("top10_empty_after_selection")

        payload = {
            "status": status_out,
            "page": TOP10_PAGE_NAME,
            "sheet": TOP10_PAGE_NAME,
            "route_family": "top10",
            "dispatch": "top10_selector",
            "headers": list(display_headers),
            "keys": list(keys),
            "display_headers": list(display_headers),
            "sheet_headers": list(display_headers),
            "column_headers": list(display_headers),
            "header_map": [{"key": k, "header": h} for k, h in zip(keys, display_headers)],
            "rows": rows,
            "rows_matrix": _rows_to_matrix(keys, rows),
            "count": len(rows),
            "quotes": rows,
            "meta": {
                **(meta_sel or {}),
                **(meta_rows or {}),
                "criteria": {
                    "pages_selected": crit.pages_selected,
                    "pages_explicit": crit.pages_explicit,
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
                "direct_symbols": direct_symbols,
                "warnings": warnings,
                "engine_present": engine is not None,
                "request_present": request is not None,
                "settings_present": settings is not None,
                "kwargs_received": sorted([str(k) for k in kwargs.keys()]),
                "response_contract": {
                    "headers_are_display_headers": True,
                    "keys_present": True,
                    "display_headers_present": True,
                    "rows_are_projected_to_keys": True,
                    "rows_matrix_present": True,
                    "json_safe_payload": True,
                },
                "duration_ms_public_builder": round((time.time() - t0) * 1000.0, 3),
            },
        }
        return _finalize_payload_json_safe(payload)

    except Exception as e:
        logger.exception("Top10 build_top10_rows fatal error")
        return _build_empty_top10_payload(
            criteria=crit_fallback,
            mode=mode,
            warnings=["build_top10_rows_failed"],
            error=_safe_str(e),
            error_type=type(e).__name__,
            meta_extra={
                "kwargs_received": sorted([str(k) for k in kwargs.keys()]),
                "duration_ms_public_builder": round((time.time() - t0) * 1000.0, 3),
            },
        )


async def build_top10_output_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Route-compatible public alias.
    Historically this name appeared in dispatcher call chains, so it must accept
    the same flexible keyword signature as build_top10_rows(...).
    """
    return await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
        **kwargs,
    )


async def build_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    body: Optional[Dict[str, Any]] = None,
    criteria: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    mode: str = "",
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        body=body,
        criteria=criteria,
        limit=_resolve_limit(limit, criteria, _merge_body_with_extras(body, kwargs), settings=_settings_to_payload(settings), request_payload=_request_to_payload(request)),
        mode=mode,
        **kwargs,
    )
    rows = payload.get("rows")
    return rows if isinstance(rows, list) else []


async def build_top_10_investments_rows(
    *,
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    criteria: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    limit: int = 10,
    mode: str = "",
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
        **kwargs,
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
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
        **kwargs,
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
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
        **kwargs,
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
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    payload = await build_top10_rows(
        engine=engine,
        request=request,
        settings=settings,
        criteria=criteria,
        body=body,
        limit=limit,
        mode=mode,
        **kwargs,
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
