#!/usr/bin/env python3
# core/analysis/insights_builder.py
"""
================================================================================
Insights Analysis Builder — v1.7.0
(SCHEMA-FIRST / TIME-BOXED / TOP10-CONTEXT-AWARE / IMPORT-SAFE / NUMERIC-SAFE)
================================================================================
Tadawul Fast Bridge (TFB)

What this revision improves
- FIX: keeps PS/runtime-compatible payload parsing for rows / row_objects / rows_matrix.
- FIX: adds per-call async timeout guards for quote and Top10 lookups.
- FIX: adds a soft overall build budget so Insights can finish before outer route timeouts.
- FIX: keeps Build Status and warning rows always present even when upstream calls degrade.
- FIX: emits meta.warnings for downstream route diagnostics.
- SAFE: no network calls at import time.
- SAFE: best-effort engine access only.
- SAFE: builder never raises for normal route execution; returns schema-correct rows.
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.analysis.insights_builder")
logger.addHandler(logging.NullHandler())

INSIGHTS_BUILDER_VERSION = "1.7.0"
_RIYADH_TZ = timezone(timedelta(hours=3))
_FALLBACK_KEYS = ["section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"]
_FALLBACK_HEADERS = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
_DEFAULT_QUOTES_TIMEOUT_SEC = float(os.getenv("TFB_INSIGHTS_QUOTES_TIMEOUT_SEC", "4.0") or "4.0")
_DEFAULT_TOP10_TIMEOUT_SEC = float(os.getenv("TFB_INSIGHTS_TOP10_TIMEOUT_SEC", "4.0") or "4.0")
_DEFAULT_BUILD_BUDGET_SEC = float(os.getenv("TFB_INSIGHTS_BUILD_BUDGET_SEC", "10.0") or "10.0")
_DEFAULT_MAX_SYMBOLS_PER_UNIVERSE = int(os.getenv("TFB_INSIGHTS_MAX_SYMBOLS_PER_UNIVERSE", "12") or "12")


# -----------------------------------------------------------------------------
# Small utils
# -----------------------------------------------------------------------------
def _safe_str(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _now_riyadh_iso() -> str:
    return datetime.now(_RIYADH_TZ).isoformat()


def _as_float(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        s = str(v).strip().replace(",", "")
        if s.endswith("%"):
            s = s[:-1].strip()
        x = float(s)
        return None if x != x else x
    except Exception:
        return None


def _as_fraction(v: Any) -> Optional[float]:
    x = _as_float(v)
    if x is None:
        return None
    if abs(x) >= 1.5:
        return x / 100.0
    return x


def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def _pct_points(v: Any) -> Optional[float]:
    x = _as_float(v)
    if x is None:
        return None
    if abs(x) <= 1.5:
        return x * 100.0
    return x


def _fmt_pct(v: Any) -> str:
    x = _pct_points(v)
    return "" if x is None else f"{x:.2f}%"


def _fmt_num(v: Any) -> str:
    x = _as_float(v)
    return _safe_str(v) if x is None else f"{x:.2f}"


def _csv_list(raw: str) -> List[str]:
    items = [p.strip() for p in (raw or "").replace("\n", ",").split(",") if p.strip()]
    out: List[str] = []
    seen = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _env_csv(name: str, default_csv: str) -> List[str]:
    return _csv_list(os.getenv(name, default_csv))


def _compact_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return _safe_str(obj)


def _safe_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = _safe_str(v).lower()
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = _safe_str(value)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    key_list = [_safe_str(k) for k in keys if _safe_str(k)]
    return [[row.get(k, "") for k in key_list] for row in rows if isinstance(row, Mapping)]


def _is_probable_symbol_token(value: Any) -> bool:
    s = _safe_str(value)
    if not s or len(s) > 32:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._=^-:")
    return all(ch in allowed for ch in s)


def _is_signature_mismatch_typeerror(exc: TypeError) -> bool:
    msg = _safe_str(exc).lower()
    markers = (
        "unexpected keyword",
        "positional argument",
        "required positional argument",
        "takes no keyword",
        "takes from",
        "takes exactly",
        "got an unexpected keyword",
        "got multiple values for argument",
        "missing 1 required positional argument",
        "missing required positional argument",
        "keyword-only argument",
    )
    return any(marker in msg for marker in markers)


def _remaining_budget(deadline: Optional[float]) -> Optional[float]:
    if deadline is None:
        return None
    remaining = deadline - asyncio.get_running_loop().time()
    return max(0.01, remaining)


async def _await_with_timeout(awaitable: Any, timeout_sec: Optional[float], label: str) -> Any:
    value = awaitable if inspect.isawaitable(awaitable) else None
    if value is None:
        return awaitable
    try:
        if timeout_sec is None:
            return await value
        return await asyncio.wait_for(value, timeout=timeout_sec)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(f"{label} timed out after {timeout_sec:.2f}s") from exc


# -----------------------------------------------------------------------------
# Dict/model extraction helpers
# -----------------------------------------------------------------------------
def _extract_row_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    try:
        if hasattr(v, "model_dump") and callable(getattr(v, "model_dump")):
            d = v.model_dump(mode="python")  # type: ignore[attr-defined]
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    try:
        if hasattr(v, "dict") and callable(getattr(v, "dict")):
            d = v.dict()  # type: ignore[attr-defined]
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    try:
        d = getattr(v, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass
    return {}


def _rows_from_matrix_payload(matrix: Any, cols: Sequence[Any]) -> List[Dict[str, Any]]:
    keys = [_safe_str(c) for c in cols if _safe_str(c)]
    if not keys:
        return []
    out: List[Dict[str, Any]] = []
    for row in matrix or []:
        if not isinstance(row, (list, tuple)):
            continue
        d = {key: row[i] if i < len(row) else None for i, key in enumerate(keys)}
        out.append(d)
    return out


def _looks_like_explicit_row_dict(d: Mapping[str, Any]) -> bool:
    keyset = {str(k) for k in d.keys()}
    if {"section", "item", "metric"}.issubset(keyset):
        return True
    return bool(keyset & {"symbol", "recommendation", "overall_score", "expected_roi_3m", "current_price"})


def _coerce_rows_list(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        if payload and isinstance(payload[0], (list, tuple)):
            return []
        return [d for item in payload if (d := _extract_row_dict(item))]
    if isinstance(payload, dict):
        if payload:
            maybe_symbol_map = True
            symbol_rows: List[Dict[str, Any]] = []
            for k, v in payload.items():
                if not isinstance(v, dict) or not _is_probable_symbol_token(k):
                    maybe_symbol_map = False
                    break
                row = dict(v)
                row.setdefault("symbol", _safe_str(k))
                symbol_rows.append(row)
            if maybe_symbol_map and symbol_rows:
                return symbol_rows
        for key in ("row_objects", "rowObjects", "records", "items", "data", "quotes", "rows", "results"):
            val = payload.get(key)
            if isinstance(val, list):
                if not val:
                    continue
                if isinstance(val[0], dict):
                    return [dict(r) for r in val if isinstance(r, dict)]
                if isinstance(val[0], (list, tuple)):
                    cols = payload.get("keys") or payload.get("headers") or payload.get("columns") or []
                    if isinstance(cols, list) and cols:
                        rows_from_matrix = _rows_from_matrix_payload(val, cols)
                        if rows_from_matrix:
                            return rows_from_matrix
                out_list = [d for item in val if (d := _extract_row_dict(item))]
                if out_list:
                    return out_list
            if isinstance(val, dict):
                nested = _coerce_rows_list(val)
                if nested:
                    return nested
        rows_matrix = payload.get("rows_matrix") or payload.get("matrix")
        if isinstance(rows_matrix, list):
            cols = payload.get("keys") or payload.get("headers") or payload.get("columns") or []
            if isinstance(cols, list) and cols:
                rows_from_matrix = _rows_from_matrix_payload(rows_matrix, cols)
                if rows_from_matrix:
                    return rows_from_matrix
        d0 = _extract_row_dict(payload)
        if d0 and _looks_like_explicit_row_dict(d0):
            return [d0]
        for key in ("result", "payload", "response", "output"):
            nested_rows = _coerce_rows_list(payload.get(key))
            if nested_rows:
                return nested_rows
    return []


def _maybe_rows_from_payload(payload: Any) -> List[Dict[str, Any]]:
    rows = _coerce_rows_list(payload)
    if rows:
        return rows
    d = _extract_row_dict(payload)
    if d:
        for key in ("top10_rows", "insights_rows", "analysis_rows"):
            rows2 = _coerce_rows_list(d.get(key))
            if rows2:
                return rows2
    return []


# -----------------------------------------------------------------------------
# Schema helpers
# -----------------------------------------------------------------------------
def _spec_columns(spec: Any) -> List[Any]:
    if spec is None:
        return []
    if isinstance(spec, dict):
        if isinstance(spec.get("columns"), list):
            return spec["columns"]
        if isinstance(spec.get("fields"), list):
            return spec["fields"]
        if len(spec) == 1:
            only_val = next(iter(spec.values()))
            if isinstance(only_val, dict):
                if isinstance(only_val.get("columns"), list):
                    return only_val["columns"]
                if isinstance(only_val.get("fields"), list):
                    return only_val["fields"]
    cols = getattr(spec, "columns", None)
    if isinstance(cols, list):
        return cols
    fields = getattr(spec, "fields", None)
    if isinstance(fields, list):
        return fields
    return []


def get_insights_schema() -> Tuple[List[str], List[str], str]:
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Insights_Analysis")
        cols = _spec_columns(spec)
        headers: List[str] = []
        keys: List[str] = []
        for c in cols:
            if isinstance(c, Mapping):
                h = _safe_str(c.get("header") or c.get("display_header") or c.get("label") or c.get("title"))
                k = _safe_str(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
            else:
                h = _safe_str(getattr(c, "header", None) or getattr(c, "display_header", None) or getattr(c, "label", None) or getattr(c, "title", None))
                k = _safe_str(getattr(c, "key", None) or getattr(c, "field", None) or getattr(c, "name", None) or getattr(c, "id", None))
            if h or k:
                headers.append(h or k.replace("_", " ").title())
                keys.append(k or h.lower().replace(" ", "_"))
        if headers and keys and len(headers) == len(keys):
            return headers, keys, "schema_registry.get_sheet_spec"
        if isinstance(spec, Mapping):
            h2 = spec.get("headers") or spec.get("display_headers")
            k2 = spec.get("keys") or spec.get("fields")
            if isinstance(h2, list) and isinstance(k2, list) and h2 and k2 and len(h2) == len(k2):
                headers2 = [_safe_str(x) for x in h2 if _safe_str(x)]
                keys2 = [_safe_str(x) for x in k2 if _safe_str(x)]
                if headers2 and keys2 and len(headers2) == len(keys2):
                    return headers2, keys2, "schema_registry.mapping_fields"
    except Exception as e:
        logger.debug("get_insights_schema: schema_registry unavailable: %r", e)
    return list(_FALLBACK_HEADERS), list(_FALLBACK_KEYS), "fallback"


def _get_criteria_fields() -> List[Dict[str, Any]]:
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Insights_Analysis")
        cfs = getattr(spec, "criteria_fields", None)
        if cfs is None and isinstance(spec, Mapping):
            cfs = spec.get("criteria_fields")
        out: List[Dict[str, Any]] = []
        for cf in list(cfs or []):
            if isinstance(cf, Mapping):
                out.append({
                    "key": _safe_str(cf.get("key", "")),
                    "label": _safe_str(cf.get("label", "")) or _safe_str(cf.get("key", "")),
                    "dtype": _safe_str(cf.get("dtype", "str")) or "str",
                    "default": cf.get("default", ""),
                    "notes": _safe_str(cf.get("notes", "")),
                })
            else:
                out.append({
                    "key": _safe_str(getattr(cf, "key", "")),
                    "label": _safe_str(getattr(cf, "label", "")) or _safe_str(getattr(cf, "key", "")),
                    "dtype": _safe_str(getattr(cf, "dtype", "str")) or "str",
                    "default": getattr(cf, "default", ""),
                    "notes": _safe_str(getattr(cf, "notes", "")),
                })
        out = [x for x in out if x.get("key")]
        if out:
            return out
    except Exception:
        pass
    return [
        {"key": "risk_level", "label": "Risk Level", "dtype": "str", "default": "Moderate", "notes": "Low / Moderate / High."},
        {"key": "confidence_level", "label": "Confidence Level", "dtype": "str", "default": "High", "notes": "High / Medium / Low."},
        {"key": "invest_period_days", "label": "Investment Period (Days)", "dtype": "int", "default": "90", "notes": "Always treated in DAYS internally."},
        {"key": "required_return_pct", "label": "Required Return %", "dtype": "pct", "default": "0.10", "notes": "Minimum expected ROI threshold."},
        {"key": "amount", "label": "Amount", "dtype": "float", "default": "0", "notes": "Investment amount (optional)."},
    ]


# -----------------------------------------------------------------------------
# Criteria normalization / context helpers
# -----------------------------------------------------------------------------
def _normalize_criteria_input(criteria: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    c = dict(criteria or {})
    pages = c.get("pages_selected") or c.get("pages") or c.get("selected_pages") or []
    if isinstance(pages, str):
        pages = _csv_list(pages)
    if not isinstance(pages, list):
        pages = []
    pages = [_safe_str(x) for x in pages if _safe_str(x)]
    invest_days = _as_int(c.get("invest_period_days") or c.get("investment_period_days") or c.get("period_days") or c.get("horizon_days") or 90)
    if invest_days is None or invest_days <= 0:
        invest_days = 90
    min_roi = c.get("min_expected_roi")
    if min_roi is None:
        min_roi = c.get("min_roi")
    if min_roi is None:
        min_roi = c.get("required_return_pct")
    min_roi_frac = _as_fraction(min_roi)
    max_risk = _as_float(c.get("max_risk_score") or c.get("max_risk") or 60.0)
    if max_risk is None:
        max_risk = 60.0
    min_conf = _as_fraction(c.get("min_confidence") or c.get("min_ai_confidence") or c.get("min_confidence_score") or 0.70)
    if min_conf is None:
        min_conf = 0.70
    min_volume = _as_float(c.get("min_volume") or c.get("min_liquidity") or c.get("min_vol"))
    top_n = _as_int(c.get("top_n") or c.get("limit") or 10)
    if top_n is None or top_n <= 0:
        top_n = 10
    normalized = {
        "pages_selected": pages or ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"],
        "invest_period_days": invest_days,
        "horizon_days": invest_days,
        "min_expected_roi": min_roi_frac,
        "max_risk_score": max_risk,
        "min_confidence": min_conf,
        "min_volume": min_volume,
        "use_liquidity_tiebreak": _safe_bool(c.get("use_liquidity_tiebreak", True), True),
        "enforce_risk_confidence": _safe_bool(c.get("enforce_risk_confidence", True), True),
        "top_n": max(1, min(50, top_n)),
        "enrich_final": _safe_bool(c.get("enrich_final", True), True),
    }
    for k, v in c.items():
        if k not in normalized and v is not None:
            normalized[k] = v
    return normalized


def _days_to_horizon(days: int) -> str:
    d = int(days or 0)
    if d <= 45:
        return "1M"
    if d <= 135:
        return "3M"
    return "12M"


def _criteria_snapshot_text(criteria: Optional[Dict[str, Any]]) -> str:
    return _compact_json(_normalize_criteria_input(criteria))


def _criteria_summary_note(criteria: Optional[Dict[str, Any]]) -> str:
    norm = _normalize_criteria_input(criteria)
    roi = norm.get("min_expected_roi")
    roi_txt = _fmt_pct(roi) if roi is not None else "N/A"
    conf_txt = _fmt_pct(norm.get("min_confidence"))
    return (
        f"Horizon={_days_to_horizon(int(norm['invest_period_days']))} "
        f"| Days={norm['invest_period_days']} "
        f"| Min ROI={roi_txt} "
        f"| Max Risk={_fmt_num(norm.get('max_risk_score'))} "
        f"| Min Confidence={conf_txt}"
    )


# -----------------------------------------------------------------------------
# Row builder
# -----------------------------------------------------------------------------
def _make_row(*, keys: Sequence[str], section: str, item: str, metric: str, value: Any, symbol: str = "", notes: str = "", last_updated_riyadh: Optional[str] = None) -> Dict[str, Any]:
    ts = last_updated_riyadh or _now_riyadh_iso()
    base = {
        "section": _safe_str(section) or "General",
        "item": _safe_str(item) or "Item",
        "symbol": _safe_str(symbol),
        "metric": _safe_str(metric) or "metric",
        "value": "" if value is None else value,
        "notes": _safe_str(notes),
        "last_updated_riyadh": ts,
    }
    return {k: base.get(k, "") for k in keys}


def _warning_row(keys: Sequence[str], label: str, notes: str, ts: str) -> Dict[str, Any]:
    return _make_row(
        keys=keys,
        section="System",
        item="Warning",
        symbol="",
        metric=label,
        value="WARN",
        notes=notes,
        last_updated_riyadh=ts,
    )


def build_criteria_rows(*, criteria: Optional[Dict[str, Any]] = None, last_updated_riyadh: Optional[str] = None) -> List[Dict[str, Any]]:
    _, keys, _ = get_insights_schema()
    ts = last_updated_riyadh or _now_riyadh_iso()
    fields = _get_criteria_fields()
    crit = _normalize_criteria_input(criteria)
    rows: List[Dict[str, Any]] = []
    for f in fields:
        k = f["key"]
        label = f["label"]
        v = crit.get(k, f.get("default", ""))
        note = f.get("notes", "")
        if k.endswith("_pct") or k.endswith("_percent") or "return" in k or "confidence" in k:
            vnum = _as_fraction(v)
            if vnum is not None:
                note = (note + " " if note else "") + f"(display: {_fmt_pct(vnum)})"
                v = vnum
        rows.append(_make_row(keys=keys, section="Criteria", item=label, symbol="", metric=k, value=v, notes=note, last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section="Criteria", item="Criteria Snapshot", symbol="", metric="criteria_snapshot", value=_criteria_snapshot_text(crit), notes="Compact JSON snapshot used by Top10 / advisor contextual logic.", last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section="Criteria", item="Criteria Summary", symbol="", metric="criteria_summary", value=_days_to_horizon(int(crit["invest_period_days"])), notes=_criteria_summary_note(crit), last_updated_riyadh=ts))
    return rows


# -----------------------------------------------------------------------------
# Engine integration
# -----------------------------------------------------------------------------
async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


async def _fetch_quotes_map(engine: Any, symbols: List[str], *, mode: str = "", timeout_sec: Optional[float] = None) -> Dict[str, Dict[str, Any]]:
    if not engine or not symbols:
        return {}
    requested = _dedupe_keep_order(symbols)

    def _rows_to_symbol_map(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            sym = _safe_str(row.get("symbol") or row.get("ticker") or row.get("code"))
            if sym:
                out[sym] = dict(row)
        return out

    fn = getattr(engine, "get_enriched_quotes_batch", None)
    if callable(fn):
        try:
            try:
                res = await _await_with_timeout(_maybe_await(fn(requested, mode=mode or "")), timeout_sec, "get_enriched_quotes_batch")
            except TypeError as exc:
                if not _is_signature_mismatch_typeerror(exc):
                    raise
                res = await _await_with_timeout(_maybe_await(fn(requested)), timeout_sec, "get_enriched_quotes_batch")
            if isinstance(res, dict):
                rows_from_payload = _maybe_rows_from_payload(res)
                if rows_from_payload:
                    row_map = _rows_to_symbol_map(rows_from_payload)
                    return {s: row_map.get(s, {"symbol": s}) for s in requested}
                return {s: _extract_row_dict(res.get(s)) if isinstance(res.get(s), dict) else {"symbol": s} for s in requested}
        except Exception:
            pass

    fn2 = getattr(engine, "get_enriched_quotes", None)
    if callable(fn2):
        try:
            res = await _await_with_timeout(_maybe_await(fn2(requested)), timeout_sec, "get_enriched_quotes")
            rows_from_payload = _maybe_rows_from_payload(res)
            if rows_from_payload:
                row_map = _rows_to_symbol_map(rows_from_payload)
                return {s: row_map.get(s, {"symbol": s}) for s in requested}
            if isinstance(res, list):
                return {s: _extract_row_dict(v) for s, v in zip(requested, res)}
        except Exception:
            pass

    out3: Dict[str, Dict[str, Any]] = {}
    fn3 = getattr(engine, "get_enriched_quote_dict", None)
    fn4 = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    per_symbol_timeout = None if timeout_sec is None else max(0.5, timeout_sec / max(1, len(requested)))
    for s in requested:
        try:
            if callable(fn3):
                out3[s] = _extract_row_dict(await _await_with_timeout(_maybe_await(fn3(s)), per_symbol_timeout, f"quote:{s}"))
            elif callable(fn4):
                out3[s] = _extract_row_dict(await _await_with_timeout(_maybe_await(fn4(s)), per_symbol_timeout, f"quote:{s}"))
            else:
                out3[s] = {"symbol": s, "warnings": "engine_missing_quote_methods"}
        except Exception as e:
            out3[s] = {"symbol": s, "warnings": f"quote_error: {e}"}
    return out3


async def _fetch_top10_payload(engine: Any, criteria: Optional[Dict[str, Any]] = None, *, limit: int = 10, mode: str = "", timeout_sec: Optional[float] = None) -> Dict[str, Any]:
    if not engine:
        return {}
    try:
        from core.analysis.top10_selector import build_top10_rows  # type: ignore

        payload = await _await_with_timeout(
            _maybe_await(build_top10_rows(engine=engine, criteria=criteria or {}, limit=limit, mode=mode or "")),
            timeout_sec,
            "build_top10_rows",
        )
        if isinstance(payload, dict):
            return payload
        rows = _maybe_rows_from_payload(payload)
        if rows:
            return {"rows": rows}
    except Exception:
        pass

    candidate_names = ("build_top10_rows", "get_top10_rows", "top10_rows", "build_top10", "get_top10_investments", "select_top10")
    for name in candidate_names:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        variants = [
            {"criteria": criteria or {}, "limit": limit, "mode": mode or ""},
            {"criteria": criteria or {}, "limit": limit},
            {"limit": limit},
            {},
        ]
        for kwargs in variants:
            try:
                payload = await _await_with_timeout(_maybe_await(fn(**kwargs)), timeout_sec, name)
                if isinstance(payload, dict):
                    return payload
                rows = _maybe_rows_from_payload(payload)
                if rows:
                    return {"rows": rows}
            except TypeError as exc:
                if _is_signature_mismatch_typeerror(exc):
                    continue
                break
            except Exception:
                continue
    return {}


async def _fetch_top10_symbols(engine: Any, criteria: Optional[Dict[str, Any]] = None, *, limit: int = 10, timeout_sec: Optional[float] = None) -> List[str]:
    if not engine:
        return []
    try:
        from core.analysis.top10_selector import select_top10_symbols  # type: ignore

        syms = await _await_with_timeout(_maybe_await(select_top10_symbols(engine=engine, criteria=criteria or {}, limit=limit)), timeout_sec, "select_top10_symbols")
        if isinstance(syms, (list, tuple)):
            return _dedupe_keep_order(syms)[: max(1, limit)]
    except Exception:
        pass

    candidates = ["get_top10_symbols", "select_top10_symbols", "top10_symbols", "get_top10_investments", "select_top10", "build_top10", "compute_top10"]
    for name in candidates:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        try:
            try:
                res = await _await_with_timeout(_maybe_await(fn(criteria=criteria or {}, limit=limit)), timeout_sec, name)
            except TypeError as exc1:
                if not _is_signature_mismatch_typeerror(exc1):
                    raise
                try:
                    res = await _await_with_timeout(_maybe_await(fn(limit=limit)), timeout_sec, name)
                except TypeError as exc2:
                    if not _is_signature_mismatch_typeerror(exc2):
                        raise
                    res = await _await_with_timeout(_maybe_await(fn()), timeout_sec, name)
        except Exception:
            continue
        if isinstance(res, (list, tuple)):
            out_syms: List[str] = []
            for item in res:
                if isinstance(item, str):
                    out_syms.append(item)
                elif isinstance(item, dict):
                    out_syms.append(_safe_str(item.get("symbol") or item.get("ticker") or item.get("code")))
                else:
                    d = _extract_row_dict(item)
                    out_syms.append(_safe_str(d.get("symbol") or d.get("ticker") or d.get("code")))
            return _dedupe_keep_order(out_syms)[: max(1, limit)]
        if isinstance(res, dict):
            if isinstance(res.get("symbols"), (list, tuple)):
                return _dedupe_keep_order(res["symbols"])[: max(1, limit)]
            rows = _maybe_rows_from_payload(res)
            if rows:
                return _dedupe_keep_order([_safe_str(r.get("symbol") or r.get("ticker") or r.get("code")) for r in rows])[: max(1, limit)]
    return []


# -----------------------------------------------------------------------------
# Insight computations
# -----------------------------------------------------------------------------
def _pick(d: Dict[str, Any], key: str) -> Any:
    try:
        return d.get(key)
    except Exception:
        return None


def _coverage_count(qmap: Dict[str, Dict[str, Any]], key: str) -> int:
    count = 0
    for d in qmap.values():
        v = _pick(d, key)
        if v is None or _safe_str(v) == "":
            continue
        count += 1
    return count


def _scored_list(qmap: Dict[str, Dict[str, Any]], key: str) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for sym, d in qmap.items():
        x = _as_float(_pick(d, key))
        if x is not None:
            out.append((sym, x))
    return out


def _avg(vals: List[float]) -> Optional[float]:
    return None if not vals else (sum(vals) / float(len(vals)))


def _build_universe_snapshot_rows(*, keys: Sequence[str], section: str, symbols: List[str], qmap: Dict[str, Dict[str, Any]], ts: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.append(_make_row(keys=keys, section=section, item="Universe Size", symbol="", metric="count", value=len(symbols), notes="Number of symbols requested for this section.", last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section=section, item="Coverage", symbol="", metric="coverage_current_price", value=f"{_coverage_count(qmap, 'current_price')}/{len(symbols)}", notes="How many symbols returned current_price.", last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section=section, item="Coverage", symbol="", metric="coverage_percent_change", value=f"{_coverage_count(qmap, 'percent_change')}/{len(symbols)}", notes="How many symbols returned percent_change.", last_updated_riyadh=ts))

    movers = _scored_list(qmap, "percent_change")
    if movers:
        movers.sort(key=lambda t: t[1], reverse=True)
        top_sym, top_pc = movers[0]
        low_sym, low_pc = movers[-1]
        avg_pc = _avg([x for _, x in movers])
        rows.append(_make_row(keys=keys, section=section, item="Top Gainer", symbol=top_sym, metric="percent_change", value=_pct_points(top_pc), notes=f"Highest percent_change in this universe (display: {_fmt_pct(top_pc)}).", last_updated_riyadh=ts))
        rows.append(_make_row(keys=keys, section=section, item="Top Loser", symbol=low_sym, metric="percent_change", value=_pct_points(low_pc), notes=f"Lowest percent_change in this universe (display: {_fmt_pct(low_pc)}).", last_updated_riyadh=ts))
        if avg_pc is not None:
            rows.append(_make_row(keys=keys, section=section, item="Average Change", symbol="", metric="avg_percent_change", value=_pct_points(avg_pc), notes=f"Average percent_change across symbols with data (display: {_fmt_pct(avg_pc)}).", last_updated_riyadh=ts))
    else:
        rows.append(_make_row(keys=keys, section=section, item="Snapshot", symbol="", metric="status", value="No movers data", notes="percent_change not available yet for this universe.", last_updated_riyadh=ts))

    roi3 = _scored_list(qmap, "expected_roi_3m")
    if roi3:
        roi3.sort(key=lambda t: t[1], reverse=True)
        sym, val = roi3[0]
        rows.append(_make_row(keys=keys, section=section, item="Best Expected ROI (3M)", symbol=sym, metric="expected_roi_3m", value=_pct_points(val), notes=f"Highest expected_roi_3m (display: {_fmt_pct(val)}).", last_updated_riyadh=ts))

    vol90 = _scored_list(qmap, "volatility_90d")
    if vol90:
        vol90.sort(key=lambda t: t[1], reverse=True)
        sym, val = vol90[0]
        rows.append(_make_row(keys=keys, section=section, item="Highest Volatility (90D)", symbol=sym, metric="volatility_90d", value=_pct_points(val), notes=f"Highest volatility_90d (display: {_fmt_pct(val)}).", last_updated_riyadh=ts))

    conf = _scored_list(qmap, "forecast_confidence")
    if conf:
        avg_conf = _avg([x for _, x in conf])
        if avg_conf is not None:
            rows.append(_make_row(keys=keys, section=section, item="Average Forecast Confidence", symbol="", metric="avg_forecast_confidence", value=avg_conf, notes="Average forecast_confidence across symbols with data.", last_updated_riyadh=ts))
    return rows


def _build_portfolio_kpi_rows(*, keys: Sequence[str], section: str, symbols: List[str], qmap: Dict[str, Dict[str, Any]], ts: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total_cost = 0.0
    total_value = 0.0
    have_any_position = False
    for sym in symbols:
        d = qmap.get(sym) or {}
        qty = _as_float(_pick(d, "position_qty"))
        avg_cost = _as_float(_pick(d, "avg_cost"))
        px = _as_float(_pick(d, "current_price"))
        if qty is None or avg_cost is None:
            continue
        have_any_position = True
        total_cost += float(qty) * float(avg_cost)
        if px is not None:
            total_value += float(qty) * float(px)
    if not have_any_position:
        rows.append(_make_row(keys=keys, section=section, item="Portfolio KPIs", symbol="", metric="status", value="Unavailable", notes="position_qty / avg_cost not present (or portfolio universe not provided).", last_updated_riyadh=ts))
        return rows
    unreal = total_value - total_cost
    unreal_pct = (unreal / total_cost) if total_cost > 0 else None
    rows.append(_make_row(keys=keys, section=section, item="Portfolio Cost", symbol="", metric="total_cost", value=total_cost, notes="Sum(position_qty * avg_cost).", last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section=section, item="Portfolio Value", symbol="", metric="total_value", value=total_value, notes="Sum(position_qty * current_price) where available.", last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section=section, item="Unrealized P/L", symbol="", metric="unrealized_pl", value=unreal, notes="total_value - total_cost.", last_updated_riyadh=ts))
    if unreal_pct is not None:
        rows.append(_make_row(keys=keys, section=section, item="Unrealized P/L %", symbol="", metric="unrealized_pl_pct", value=_pct_points(unreal_pct), notes=f"unrealized_pl / total_cost (display: {_fmt_pct(unreal_pct)}).", last_updated_riyadh=ts))
    return rows


def _normalize_top10_rows(top10_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _maybe_rows_from_payload(top10_payload)


def _build_top10_context_rows(*, keys: Sequence[str], top10_payload: Dict[str, Any], criteria: Optional[Dict[str, Any]], ts: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    norm_criteria = _normalize_criteria_input(criteria)
    top_rows = _normalize_top10_rows(top10_payload)
    if not top_rows:
        rows.append(_make_row(keys=keys, section="Top 10 Investments", item="Status", symbol="", metric="top10_status", value="Unavailable", notes="Top10 payload is empty.", last_updated_riyadh=ts))
        return rows
    rows.append(_make_row(keys=keys, section="Top 10 Investments", item="Status", symbol="", metric="top10_count", value=len(top_rows), notes="Top10 rows generated through selector/engine path.", last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section="Top 10 Investments", item="Criteria Snapshot", symbol="", metric="criteria_snapshot", value=_criteria_snapshot_text(norm_criteria), notes=_criteria_summary_note(norm_criteria), last_updated_riyadh=ts))
    for i, raw in enumerate(top_rows[:10], start=1):
        if not isinstance(raw, dict):
            continue
        sym = _safe_str(raw.get("symbol") or raw.get("ticker") or raw.get("code"))
        name = _safe_str(raw.get("name"))
        rank = _as_int(raw.get("top10_rank")) or i
        days = int(norm_criteria["invest_period_days"])
        roi_horizon_key = "expected_roi_1m" if days <= 45 else ("expected_roi_3m" if days <= 135 else "expected_roi_12m")
        roi_val = raw.get(roi_horizon_key)
        reco = _safe_str(raw.get("recommendation"))
        sel_reason = _safe_str(raw.get("selection_reason"))
        reco_reason = _safe_str(raw.get("recommendation_reason"))
        conf = raw.get("forecast_confidence")
        overall = raw.get("overall_score")
        risk_bucket = _safe_str(raw.get("risk_bucket"))
        note_parts: List[str] = []
        if name:
            note_parts.append(name)
        if reco:
            note_parts.append(f"reco={reco}")
        if conf is not None:
            note_parts.append(f"conf={_fmt_pct(conf)}")
        if risk_bucket:
            note_parts.append(f"risk={risk_bucket}")
        if sel_reason:
            note_parts.append(f"why={sel_reason}")
        elif reco_reason:
            note_parts.append(f"why={reco_reason}")
        rows.append(_make_row(keys=keys, section="Top 10 Investments", item=f"#{rank}", symbol=sym, metric=roi_horizon_key, value=_pct_points(roi_val) if roi_val is not None else "", notes=" | ".join(note_parts) if note_parts else "Top10 ranked item.", last_updated_riyadh=ts))
        if overall is not None:
            rows.append(_make_row(keys=keys, section="Top 10 Context", item=f"#{rank} Overall Score", symbol=sym, metric="overall_score", value=overall, notes=f"Rank={rank}" + (f" | {name}" if name else ""), last_updated_riyadh=ts))
        if sel_reason or reco_reason:
            rows.append(_make_row(keys=keys, section="Top 10 Context", item=f"#{rank} Selection Logic", symbol=sym, metric="selection_reason", value=rank, notes=sel_reason or reco_reason, last_updated_riyadh=ts))
    return rows


# -----------------------------------------------------------------------------
# Auto-universe defaults
# -----------------------------------------------------------------------------
def _default_universes() -> Dict[str, List[str]]:
    indices = _env_csv("TFB_INSIGHTS_INDICES", "TASI,NOMU,^GSPC,^IXIC,^FTSE")
    commodities_fx = _env_csv("TFB_INSIGHTS_COMMODITIES_FX", "GC=F,BZ=F,USDSAR=X,EURUSD=X")
    return {"Indices & Benchmarks": indices, "Commodities & FX": commodities_fx}


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
async def build_insights_analysis_rows(
    *,
    engine: Optional[Any] = None,
    criteria: Optional[Dict[str, Any]] = None,
    universes: Optional[Dict[str, Sequence[str]]] = None,
    symbols: Optional[Sequence[str]] = None,
    mode: str = "",
    include_criteria_rows: bool = True,
    include_system_rows: bool = True,
    auto_universe_when_empty: bool = True,
    include_top10_section: bool = True,
    include_portfolio_kpis: bool = True,
    max_symbols_per_universe: int = _DEFAULT_MAX_SYMBOLS_PER_UNIVERSE,
    quotes_timeout_sec: float = _DEFAULT_QUOTES_TIMEOUT_SEC,
    top10_timeout_sec: float = _DEFAULT_TOP10_TIMEOUT_SEC,
    build_budget_sec: float = _DEFAULT_BUILD_BUDGET_SEC,
) -> Dict[str, Any]:
    headers, keys, schema_source = get_insights_schema()
    ts = _now_riyadh_iso()
    norm_criteria = _normalize_criteria_input(criteria)
    warnings: List[str] = []
    rows: List[Dict[str, Any]] = []
    deadline = asyncio.get_running_loop().time() + max(1.0, float(build_budget_sec))

    if include_criteria_rows:
        rows.extend(build_criteria_rows(criteria=norm_criteria, last_updated_riyadh=ts))

    if include_system_rows:
        schema_version = ""
        try:
            from core.sheets.schema_registry import SCHEMA_VERSION as _SV  # type: ignore
            schema_version = _safe_str(_SV)
        except Exception:
            schema_version = ""
        rows.append(_make_row(keys=keys, section="System", item="Builder Version", symbol="", metric="insights_builder_version", value=INSIGHTS_BUILDER_VERSION, notes="core/analysis/insights_builder.py", last_updated_riyadh=ts))
        if schema_version:
            rows.append(_make_row(keys=keys, section="System", item="Schema Version", symbol="", metric="schema_version", value=schema_version, notes="core/sheets/schema_registry.py", last_updated_riyadh=ts))

    eff_universes: Dict[str, List[str]] = {}
    if universes:
        for name, seq in universes.items():
            sym_list = _dedupe_keep_order(seq or [])
            if sym_list:
                eff_universes[_safe_str(name) or "Universe"] = sym_list
    if not eff_universes and symbols:
        sym_list2 = _dedupe_keep_order(symbols or [])
        if sym_list2:
            eff_universes["Selected Symbols"] = sym_list2
    auto_used = False
    if not eff_universes and engine and auto_universe_when_empty:
        eff_universes.update(_default_universes())
        auto_used = True

    build_ok = bool(engine) and bool(eff_universes)
    rows.append(_make_row(keys=keys, section="System", item="Build Status", symbol="", metric="build_status", value="OK" if build_ok else "WARN", notes="OK = engine + universes available. WARN = criteria/system only (no engine or no universes).", last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section="Summary", item="Sections Included", symbol="", metric="sections", value=", ".join(list(eff_universes.keys())) if eff_universes else "None", notes="Auto-universe applied." if auto_used else "Universes provided by caller (or none).", last_updated_riyadh=ts))
    rows.append(_make_row(keys=keys, section="Summary", item="Criteria Summary", symbol="", metric="criteria_summary", value=_days_to_horizon(int(norm_criteria["invest_period_days"])), notes=_criteria_summary_note(norm_criteria), last_updated_riyadh=ts))

    if not engine or not eff_universes:
        if not engine:
            rows.append(_make_row(keys=keys, section="Summary", item="Engine", symbol="", metric="engine_status", value="Not provided", notes="No engine passed; returning schema-correct rows (criteria/system/summary only).", last_updated_riyadh=ts))
        elif not eff_universes:
            rows.append(_make_row(keys=keys, section="Summary", item="Universes", symbol="", metric="universe_status", value="Empty", notes="No universes/symbols provided and auto-universe disabled.", last_updated_riyadh=ts))
        return {
            "status": "success",
            "page": "Insights_Analysis",
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "row_objects": rows,
            "rows_matrix": _rows_to_matrix(rows, keys),
            "meta": {
                "schema_source": schema_source,
                "generated_at_riyadh": ts,
                "engine_used": bool(engine),
                "auto_universe_used": auto_used,
                "universes": list(eff_universes.keys()),
                "mode": mode,
                "builder_version": INSIGHTS_BUILDER_VERSION,
                "criteria_snapshot": _criteria_snapshot_text(norm_criteria),
                "warnings": warnings,
            },
        }

    try:
        cap = max(1, min(int(max_symbols_per_universe), 500))
    except Exception:
        cap = _DEFAULT_MAX_SYMBOLS_PER_UNIVERSE

    for section_name, sym_list in eff_universes.items():
        remaining = _remaining_budget(deadline)
        if remaining is not None and remaining <= 0.10:
            warnings.append(f"Skipped section '{section_name}' because build budget was exhausted.")
            rows.append(_warning_row(keys, "build_budget_exhausted", f"Skipped section '{section_name}' because build budget was exhausted.", ts))
            break
        syms = _dedupe_keep_order(sym_list or [])[:cap]
        try:
            qmap = await _fetch_quotes_map(engine, syms, mode=mode or "", timeout_sec=min(quotes_timeout_sec, remaining or quotes_timeout_sec))
        except Exception as exc:
            qmap = {}
            warnings.append(f"Quote fetch degraded for section '{section_name}': {exc}")
            rows.append(_warning_row(keys, "quote_fetch_degraded", f"Section '{section_name}' degraded: {exc}", ts))
        rows.extend(_build_universe_snapshot_rows(keys=keys, section=section_name, symbols=syms, qmap=qmap, ts=ts))
        if include_portfolio_kpis and section_name.strip().lower() in {"my_portfolio", "portfolio", "my portfolio", "selected symbols"}:
            rows.extend(_build_portfolio_kpi_rows(keys=keys, section="Portfolio KPIs", symbols=syms, qmap=qmap, ts=ts))

    if include_top10_section:
        remaining = _remaining_budget(deadline)
        if remaining is not None and remaining <= 0.25:
            warnings.append("Skipped Top10 context because the remaining build budget was too small.")
            rows.append(_warning_row(keys, "top10_skipped", "Skipped Top10 context because the remaining build budget was too small.", ts))
        else:
            top10_payload: Dict[str, Any] = {}
            try:
                top10_payload = await _fetch_top10_payload(engine, criteria=norm_criteria, limit=10, mode=mode or "", timeout_sec=min(top10_timeout_sec, remaining or top10_timeout_sec))
            except Exception as exc:
                warnings.append(f"Top10 payload degraded: {exc}")
                rows.append(_warning_row(keys, "top10_payload_degraded", f"Top10 payload degraded: {exc}", ts))
            if top10_payload:
                rows.extend(_build_top10_context_rows(keys=keys, top10_payload=top10_payload, criteria=norm_criteria, ts=ts))
            else:
                try:
                    top10_syms = await _fetch_top10_symbols(engine, criteria=norm_criteria, limit=10, timeout_sec=min(top10_timeout_sec, _remaining_budget(deadline) or top10_timeout_sec))
                except Exception as exc:
                    top10_syms = []
                    warnings.append(f"Top10 symbols degraded: {exc}")
                    rows.append(_warning_row(keys, "top10_symbols_degraded", f"Top10 symbols degraded: {exc}", ts))
                if top10_syms:
                    try:
                        qmap10 = await _fetch_quotes_map(engine, top10_syms, mode=mode or "", timeout_sec=min(quotes_timeout_sec, _remaining_budget(deadline) or quotes_timeout_sec))
                    except Exception as exc:
                        qmap10 = {}
                        warnings.append(f"Top10 quote enrichment degraded: {exc}")
                        rows.append(_warning_row(keys, "top10_quote_degraded", f"Top10 quote enrichment degraded: {exc}", ts))
                    rows.append(_make_row(keys=keys, section="Top 10 Investments", item="Status", symbol="", metric="top10_count", value=len(top10_syms), notes="Top10 symbols generated (best-effort symbol-only fallback).", last_updated_riyadh=ts))
                    for i, sym in enumerate(top10_syms, start=1):
                        d = qmap10.get(sym) or {}
                        roi3 = _as_float(_pick(d, "expected_roi_3m"))
                        conf = _as_float(_pick(d, "forecast_confidence"))
                        reco = _safe_str(_pick(d, "recommendation"))
                        name = _safe_str(_pick(d, "name"))
                        note_parts: List[str] = []
                        if name:
                            note_parts.append(name)
                        if conf is not None:
                            note_parts.append(f"conf={_fmt_pct(conf)}")
                        if reco:
                            note_parts.append(f"reco={reco}")
                        notes = " | ".join(note_parts) if note_parts else "Top10 item."
                        if roi3 is not None:
                            notes = notes + f" (display: {_fmt_pct(roi3)})"
                        rows.append(_make_row(keys=keys, section="Top 10 Investments", item=f"#{i}", symbol=sym, metric="expected_roi_3m", value=_pct_points(roi3) if roi3 is not None else "", notes=notes, last_updated_riyadh=ts))
                else:
                    rows.append(_make_row(keys=keys, section="Top 10 Investments", item="Status", symbol="", metric="top10_status", value="Unavailable", notes="No Top10 method found or returned empty.", last_updated_riyadh=ts))

    if warnings:
        rows.insert(0, _warning_row(keys, "builder_warnings", " | ".join(warnings[:3]), ts))

    return {
        "status": "partial" if warnings else "success",
        "page": "Insights_Analysis",
        "headers": headers,
        "keys": keys,
        "rows": rows,
        "row_objects": rows,
        "rows_matrix": _rows_to_matrix(rows, keys),
        "meta": {
            "schema_source": schema_source,
            "generated_at_riyadh": ts,
            "engine_used": True,
            "auto_universe_used": auto_used,
            "universes": list(eff_universes.keys()),
            "mode": mode,
            "builder_version": INSIGHTS_BUILDER_VERSION,
            "criteria_snapshot": _criteria_snapshot_text(norm_criteria),
            "warnings": warnings,
            "quotes_timeout_sec": quotes_timeout_sec,
            "top10_timeout_sec": top10_timeout_sec,
            "build_budget_sec": build_budget_sec,
        },
    }


__all__ = [
    "INSIGHTS_BUILDER_VERSION",
    "get_insights_schema",
    "build_criteria_rows",
    "build_insights_analysis_rows",
]
