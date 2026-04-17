#!/usr/bin/env python3
# core/analysis/insights_builder.py
"""
================================================================================
Insights Analysis Builder — v1.6.0
(SCHEMA-FIRST / TOP10-CONTEXT-AWARE / IMPORT-SAFE / NUMERIC-SAFE / ROBUST)
================================================================================
Tadawul Fast Bridge (TFB)

What this revision improves
- FIX: accepts row_objects / rowObjects / rows_matrix / matrix payload shapes.
- FIX: supports symbol-map payloads and nested payload/result envelopes.
- FIX: keeps Top10 parsing aligned with revised engine / selector outputs.
- FIX: retries alternate call signatures only for real signature-mismatch TypeErrors.
- FIX: keeps direct symbol order stable and de-duplicated.
- FIX: emits row_objects and rows_matrix in the final envelope for downstream stability.
- FIX: improves quote-batch parsing when engine returns payload-style dicts instead of raw symbol maps.
- FIX: keeps Build Status rows always present (never blank).
- SAFE: no network calls at import time.
- SAFE: best-effort engine access only.
- SAFE: builder never raises for normal route execution; returns schema-correct rows.
================================================================================
"""

from __future__ import annotations

import inspect
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.analysis.insights_builder")
logger.addHandler(logging.NullHandler())

INSIGHTS_BUILDER_VERSION = "3.0.0"
_RIYADH_TZ = timezone(timedelta(hours=3))
# v3.0.0: 9-column schema (schema_registry v3.4.0 INSIGHTS_ANALYSIS_FIELDS)
_FALLBACK_KEYS: List[str] = [
    "section", "item", "symbol", "metric",
    "value", "signal", "priority", "notes", "as_of_riyadh",
]
_FALLBACK_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric",
    "Value", "Signal", "Priority", "Notes", "Last Updated (Riyadh)",
]
# Backward-compat signal constants (v2.0.0 callers)
_SIGNAL_UP       = "BUY"
_SIGNAL_DOWN     = "SELL"
_SIGNAL_NEUTRAL  = "HOLD"
_SIGNAL_HIGH     = "ALERT"
_SIGNAL_MODERATE = "HOLD"
_SIGNAL_LOW      = "INFO"
_SIGNAL_OK       = "INFO"
_SIGNAL_WARN     = "ALERT"
_SIGNAL_ALERT    = "ALERT"
# Priority vocabulary
_PRI_HIGH   = "High"
_PRI_MEDIUM = "Medium"
_PRI_LOW    = "Low"


# -----------------------------------------------------------------------------
# Small utils
# -----------------------------------------------------------------------------
def _safe_str(v: Any) -> str:
    try:
        s = str(v)
    except Exception:
        return ""
    return s.strip()


def _now_riyadh_iso() -> str:
    return datetime.now(_RIYADH_TZ).isoformat()


def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, bool):
            return None
        s = str(v).strip().replace(",", "")
        if s.endswith("%"):
            s = s[:-1].strip()
        x = float(s)
        if x != x:  # NaN
            return None
        return x
    except Exception:
        return None


def _as_fraction(v: Any) -> Optional[float]:
    x = _as_float(v)
    if x is None:
        return None
    if abs(x) > 1.5:
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
    """
    Normalize percent values into percent-points:
      - 0.12 -> 12.0
      - 12 -> 12.0
      - "12%" -> 12.0
    """
    x = _as_float(v)
    if x is None:
        return None
    if abs(x) <= 1.5:
        return x * 100.0
    return x


def _fmt_pct(v: Any) -> str:
    x = _pct_points(v)
    if x is None:
        return ""
    return f"{x:.2f}%"


def _fmt_num(v: Any) -> str:
    x = _as_float(v)
    if x is None:
        return _safe_str(v)
    return f"{x:.2f}"


def _fmt_int(v: Any) -> str:
    x = _as_int(v)
    if x is None:
        return _safe_str(v)
    return str(x)


def _csv_list(raw: str) -> List[str]:
    items: List[str] = []
    for part in (raw or "").replace("\n", ",").split(","):
        s = part.strip()
        if s:
            items.append(s)
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
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
    out: List[List[Any]] = []
    key_list = [_safe_str(k) for k in keys if _safe_str(k)]
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        out.append([row.get(k, "") for k in key_list])
    return out


def _is_probable_symbol_token(value: Any) -> bool:
    s = _safe_str(value)
    if not s:
        return False
    if len(s) > 32:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._=^-:")
    return all(ch in allowed for ch in s)


def _is_signature_mismatch_typeerror(exc: TypeError) -> bool:
    msg = _safe_str(exc).lower()
    if not msg:
        return False
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
        if hasattr(v, "__dict__"):
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
        d: Dict[str, Any] = {}
        for i, key in enumerate(keys):
            d[key] = row[i] if i < len(row) else None
        out.append(d)
    return out


def _looks_like_explicit_row_dict(d: Mapping[str, Any]) -> bool:
    keyset = {str(k) for k in d.keys()}
    if {"section", "item", "metric"}.issubset(keyset):
        return True
    if keyset & {"symbol", "recommendation", "overall_score", "expected_roi_3m", "current_price"}:
        return True
    return False


def _coerce_rows_list(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []

    if isinstance(payload, list):
        out: List[Dict[str, Any]] = []
        if payload and isinstance(payload[0], (list, tuple)):
            return []
        for item in payload:
            d = _extract_row_dict(item)
            if d:
                out.append(d)
        return out

    if isinstance(payload, dict):
        # symbol-map payloads: {"AAPL": {...}, "MSFT": {...}}
        if payload:
            maybe_symbol_map = True
            symbol_rows: List[Dict[str, Any]] = []
            for k, v in payload.items():
                if not isinstance(v, dict) or not _is_probable_symbol_token(k):
                    maybe_symbol_map = False
                    break
                row = dict(v)
                if not row.get("symbol"):
                    row["symbol"] = _safe_str(k)
                symbol_rows.append(row)
            if maybe_symbol_map and symbol_rows:
                return symbol_rows

        for key in (
            "row_objects",
            "rowObjects",
            "records",
            "items",
            "data",
            "quotes",
            "rows",
            "results",
        ):
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
                out_list: List[Dict[str, Any]] = []
                for item in val:
                    d = _extract_row_dict(item)
                    if d:
                        out_list.append(d)
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
            nested = payload.get(key)
            nested_rows = _coerce_rows_list(nested)
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
            val = d.get(key)
            rows2 = _coerce_rows_list(val)
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
    """
    v3.0.0: Returns (headers, keys, source_marker) for the 9-column Insights_Analysis schema.
    Falls back to _FALLBACK_HEADERS / _FALLBACK_KEYS (9 cols) if registry unavailable.
    """
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
                h = _safe_str(
                    getattr(c, "header", None)
                    or getattr(c, "display_header", None)
                    or getattr(c, "label", None)
                    or getattr(c, "title", None)
                )
                k = _safe_str(
                    getattr(c, "key", None)
                    or getattr(c, "field", None)
                    or getattr(c, "name", None)
                    or getattr(c, "id", None)
                )

            if h or k:
                headers.append(h or k.replace("_", " ").title())
                keys.append(k or h.lower().replace(" ", "_"))

        if headers and keys and len(headers) == len(keys) and len(keys) >= 7:
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

    return list(_FALLBACK_HEADERS), list(_FALLBACK_KEYS), "hardcoded_fallback_9col"


def _get_criteria_fields() -> List[Dict[str, Any]]:
    """
    Reads criteria_fields from schema_registry for Insights_Analysis (best-effort).
    Returns list of dicts: {key,label,dtype,default,notes}
    """
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Insights_Analysis")
        cfs = getattr(spec, "criteria_fields", None)

        if cfs is None and isinstance(spec, Mapping):
            cfs = spec.get("criteria_fields")

        out: List[Dict[str, Any]] = []
        for cf in list(cfs or []):
            if isinstance(cf, Mapping):
                out.append(
                    {
                        "key": _safe_str(cf.get("key", "")),
                        "label": _safe_str(cf.get("label", "")) or _safe_str(cf.get("key", "")),
                        "dtype": _safe_str(cf.get("dtype", "str")) or "str",
                        "default": cf.get("default", ""),
                        "notes": _safe_str(cf.get("notes", "")),
                    }
                )
            else:
                out.append(
                    {
                        "key": _safe_str(getattr(cf, "key", "")),
                        "label": _safe_str(getattr(cf, "label", "")) or _safe_str(getattr(cf, "key", "")),
                        "dtype": _safe_str(getattr(cf, "dtype", "str")) or "str",
                        "default": getattr(cf, "default", ""),
                        "notes": _safe_str(getattr(cf, "notes", "")),
                    }
                )
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

    invest_days = _as_int(
        c.get("invest_period_days")
        or c.get("investment_period_days")
        or c.get("period_days")
        or c.get("horizon_days")
        or 90
    )
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

    min_conf = _as_fraction(
        c.get("min_confidence")
        or c.get("min_ai_confidence")
        or c.get("min_confidence_score")
        or 0.70
    )
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
    norm = _normalize_criteria_input(criteria)
    return _compact_json(norm)


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
def _make_row(
    *,
    keys: Sequence[str],
    section: str,
    item: str,
    metric: str,
    value: Any,
    # v3.0.0: new 9-col schema fields
    signal:   str = "",
    priority: str = "",         # NEW: High / Medium / Low
    # Unchanged
    symbol: str = "",
    notes:  str = "",
    as_of_riyadh: Optional[str] = None,
    # Backward-compat aliases
    last_updated_riyadh: Optional[str] = None,
    category: str = "",   # v2.0 field: silently accepted, ignored
    score: Any = "",      # v2.0 field: silently accepted, ignored
) -> Dict[str, Any]:
    """
    v3.0.0: 9-column row builder.  New: signal, priority. Removed: category, score.
    Old callers passing category= / score= / last_updated_riyadh= are silently accepted.
    """
    ts = as_of_riyadh or last_updated_riyadh or _now_riyadh_iso()

    sec = _safe_str(section) or "General"
    it  = _safe_str(item)    or "Item"
    met = _safe_str(metric)  or "metric"
    sym = _safe_str(symbol)

    if value is None:
        val_out: Any = ""
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        val_out = value
    else:
        xf  = _as_float(value)
        xi  = _as_int(value)
        raw_s = _safe_str(value)
        if xi is not None and raw_s.isdigit():
            val_out = xi
        elif xf is not None and raw_s != "":
            val_out = xf
        else:
            val_out = raw_s

    base: Dict[str, Any] = {
        "section":        sec,
        "item":           it,
        "symbol":         sym,
        "metric":         met,
        "value":          val_out,
        "signal":         _safe_str(signal),
        "priority":       _safe_str(priority),
        "notes":          _safe_str(notes),
        "as_of_riyadh":   ts,
        "last_updated_riyadh": ts,   # backward-compat
    }

    out: Dict[str, Any] = {}
    for k in keys:
        out[k] = base.get(k, "")
    return out


def build_criteria_rows(
    *,
    criteria: Optional[Dict[str, Any]] = None,
    last_updated_riyadh: Optional[str] = None,
) -> List[Dict[str, Any]]:
    _, keys, _ = get_insights_schema()
    ts = last_updated_riyadh or _now_riyadh_iso()

    fields = _get_criteria_fields()
    crit = _normalize_criteria_input(criteria)

    rows: List[Dict[str, Any]] = []
    for f in fields:
        k = f["key"]
        label = f["label"]
        v = crit.get(k, f.get("default", ""))

        if k.endswith("_pct") or k.endswith("_percent") or "return" in k or "confidence" in k:
            vnum = _as_fraction(v)
            note = f.get("notes", "")
            if vnum is not None:
                note = (note + " " if note else "") + f"(display: {_fmt_pct(vnum)})"
                v = vnum
            rows.append(
                _make_row(
                    keys=keys,
                    section="Criteria",
                    item=label,
                    symbol="",
                    metric=k,
                    value=v,
                    notes=note,
                    last_updated_riyadh=ts,
                )
            )
        else:
            rows.append(
                _make_row(
                    keys=keys,
                    section="Criteria",
                    item=label,
                    symbol="",
                    metric=k,
                    value=v,
                    notes=f.get("notes", ""),
                    last_updated_riyadh=ts,
                )
            )

    rows.append(
        _make_row(
            keys=keys,
            section="Criteria",
            item="Criteria Snapshot",
            symbol="",
            metric="criteria_snapshot",
            value=_criteria_snapshot_text(crit),
            notes="Compact JSON snapshot used by Top10 / advisor contextual logic.",
            last_updated_riyadh=ts,
        )
    )

    rows.append(
        _make_row(
            keys=keys,
            section="Criteria",
            item="Criteria Summary",
            symbol="",
            metric="criteria_summary",
            value=_days_to_horizon(int(crit["invest_period_days"])),
            notes=_criteria_summary_note(crit),
            last_updated_riyadh=ts,
        )
    )

    return rows


# -----------------------------------------------------------------------------
# Engine integration
# -----------------------------------------------------------------------------
async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


async def _fetch_quotes_map(engine: Any, symbols: List[str], *, mode: str = "") -> Dict[str, Dict[str, Any]]:
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

    # batch dict API
    fn = getattr(engine, "get_enriched_quotes_batch", None)
    if callable(fn):
        try:
            try:
                res = await _maybe_await(fn(requested, mode=mode or ""))
            except TypeError as exc:
                if not _is_signature_mismatch_typeerror(exc):
                    raise
                res = await _maybe_await(fn(requested))
            if isinstance(res, dict):
                rows_from_payload = _maybe_rows_from_payload(res)
                if rows_from_payload:
                    row_map = _rows_to_symbol_map(rows_from_payload)
                    return {s: row_map.get(s, {"symbol": s}) for s in requested}
                out: Dict[str, Dict[str, Any]] = {}
                for s in requested:
                    out[s] = _extract_row_dict(res.get(s)) if isinstance(res.get(s), dict) else {"symbol": s}
                return out
        except Exception:
            pass

    # batch list API
    fn2 = getattr(engine, "get_enriched_quotes", None)
    if callable(fn2):
        try:
            res = await _maybe_await(fn2(requested))
            rows_from_payload = _maybe_rows_from_payload(res)
            if rows_from_payload:
                row_map = _rows_to_symbol_map(rows_from_payload)
                return {s: row_map.get(s, {"symbol": s}) for s in requested}
            if isinstance(res, list):
                out2: Dict[str, Dict[str, Any]] = {}
                for s, v in zip(requested, res):
                    out2[s] = _extract_row_dict(v)
                return out2
        except Exception:
            pass

    # single quote APIs
    out3: Dict[str, Dict[str, Any]] = {}
    fn3 = getattr(engine, "get_enriched_quote_dict", None)
    fn4 = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    for s in requested:
        try:
            if callable(fn3):
                out3[s] = _extract_row_dict(await _maybe_await(fn3(s)))
            elif callable(fn4):
                out3[s] = _extract_row_dict(await _maybe_await(fn4(s)))
            else:
                out3[s] = {"symbol": s, "warnings": "engine_missing_quote_methods"}
        except Exception as e:
            out3[s] = {"symbol": s, "warnings": f"quote_error: {e}"}
    return out3


async def _fetch_top10_payload(
    engine: Any,
    criteria: Optional[Dict[str, Any]] = None,
    *,
    limit: int = 10,
    mode: str = "",
) -> Dict[str, Any]:
    if not engine:
        return {}

    # preferred selector path
    try:
        from core.analysis.top10_selector import build_top10_rows  # type: ignore

        payload = await _maybe_await(
            build_top10_rows(
                engine=engine,
                criteria=criteria or {},
                limit=limit,
                mode=mode or "",
            )
        )
        if isinstance(payload, dict):
            return payload
        rows = _maybe_rows_from_payload(payload)
        if rows:
            return {"rows": rows}
    except Exception:
        pass

    # engine fallbacks
    candidate_names = (
        "build_top10_rows",
        "get_top10_rows",
        "top10_rows",
        "build_top10",
        "get_top10_investments",
        "select_top10",
    )

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
                payload = await _maybe_await(fn(**kwargs))
                if isinstance(payload, dict):
                    return payload
                rows = _maybe_rows_from_payload(payload)
                if rows:
                    return {"rows": rows}
            except TypeError as exc:
                if _is_signature_mismatch_typeerror(exc):
                    continue
                logger.debug("_fetch_top10_payload: runtime TypeError from %s: %r", name, exc)
                break
            except Exception:
                continue

    return {}


async def _fetch_top10_symbols(
    engine: Any,
    criteria: Optional[Dict[str, Any]] = None,
    *,
    limit: int = 10,
) -> List[str]:
    if not engine:
        return []

    try:
        from core.analysis.top10_selector import select_top10_symbols  # type: ignore

        syms = await _maybe_await(select_top10_symbols(engine=engine, criteria=criteria or {}, limit=limit))
        if isinstance(syms, (list, tuple)):
            return _dedupe_keep_order(syms)[: max(1, limit)]
    except Exception:
        pass

    candidates = [
        "get_top10_symbols",
        "select_top10_symbols",
        "top10_symbols",
        "get_top10_investments",
        "select_top10",
        "build_top10",
        "compute_top10",
    ]
    for name in candidates:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        try:
            try:
                res = await _maybe_await(fn(criteria=criteria or {}, limit=limit))  # type: ignore[arg-type]
            except TypeError as exc1:
                if not _is_signature_mismatch_typeerror(exc1):
                    raise
                try:
                    res = await _maybe_await(fn(limit=limit))  # type: ignore[misc]
                except TypeError as exc2:
                    if not _is_signature_mismatch_typeerror(exc2):
                        raise
                    res = await _maybe_await(fn())  # type: ignore[misc]
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
                out_syms2: List[str] = []
                for r in rows:
                    out_syms2.append(_safe_str(r.get("symbol") or r.get("ticker") or r.get("code")))
                return _dedupe_keep_order(out_syms2)[: max(1, limit)]

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
    c = 0
    for _, d in qmap.items():
        v = _pick(d, key)
        if v is None or _safe_str(v) == "":
            continue
        c += 1
    return c


def _scored_list(qmap: Dict[str, Dict[str, Any]], key: str) -> List[Tuple[str, float]]:
    scored: List[Tuple[str, float]] = []
    for sym, d in qmap.items():
        x = _as_float(_pick(d, key))
        if x is None:
            continue
        scored.append((sym, x))
    return scored


def _avg(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return sum(vals) / float(len(vals))


def _build_universe_snapshot_rows(
    *,
    keys: Sequence[str],
    section: str,
    symbols: List[str],
    qmap: Dict[str, Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Universe Size",
            symbol="",
            metric="count",
            value=len(symbols),
            notes="Number of symbols requested for this section.",
            last_updated_riyadh=ts,
        )
    )

    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Coverage",
            symbol="",
            metric="coverage_current_price",
            value=f"{_coverage_count(qmap, 'current_price')}/{len(symbols)}",
            notes="How many symbols returned current_price.",
            last_updated_riyadh=ts,
        )
    )
    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Coverage",
            symbol="",
            metric="coverage_percent_change",
            value=f"{_coverage_count(qmap, 'percent_change')}/{len(symbols)}",
            notes="How many symbols returned percent_change.",
            last_updated_riyadh=ts,
        )
    )

    movers = _scored_list(qmap, "percent_change")
    if movers:
        movers.sort(key=lambda t: t[1], reverse=True)
        top_sym, top_pc = movers[0]
        low_sym, low_pc = movers[-1]
        avg_pc = _avg([x for _, x in movers])

        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Top Gainer",
                symbol=top_sym,
                metric="percent_change",
                value=_pct_points(top_pc),
                notes=f"Highest percent_change in this universe (display: {_fmt_pct(top_pc)}).",
                last_updated_riyadh=ts,
            )
        )
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Top Loser",
                symbol=low_sym,
                metric="percent_change",
                value=_pct_points(low_pc),
                notes=f"Lowest percent_change in this universe (display: {_fmt_pct(low_pc)}).",
                last_updated_riyadh=ts,
            )
        )
        if avg_pc is not None:
            rows.append(
                _make_row(
                    keys=keys,
                    section=section,
                    item="Average Change",
                    symbol="",
                    metric="avg_percent_change",
                    value=_pct_points(avg_pc),
                    notes=f"Average percent_change across symbols with data (display: {_fmt_pct(avg_pc)}).",
                    last_updated_riyadh=ts,
                )
            )
    else:
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Snapshot",
                symbol="",
                metric="status",
                value="No movers data",
                notes="percent_change not available yet for this universe.",
                last_updated_riyadh=ts,
            )
        )

    roi3 = _scored_list(qmap, "expected_roi_3m")
    if roi3:
        roi3.sort(key=lambda t: t[1], reverse=True)
        sym, val = roi3[0]
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Best Expected ROI (3M)",
                symbol=sym,
                metric="expected_roi_3m",
                value=_pct_points(val),
                notes=f"Highest expected_roi_3m (display: {_fmt_pct(val)}).",
                last_updated_riyadh=ts,
            )
        )

    vol90 = _scored_list(qmap, "volatility_90d")
    if vol90:
        vol90.sort(key=lambda t: t[1], reverse=True)
        sym, val = vol90[0]
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Highest Volatility (90D)",
                symbol=sym,
                metric="volatility_90d",
                value=_pct_points(val),
                notes=f"Highest volatility_90d (display: {_fmt_pct(val)}).",
                last_updated_riyadh=ts,
            )
        )

    conf = _scored_list(qmap, "forecast_confidence")
    if conf:
        avg_conf = _avg([x for _, x in conf])
        if avg_conf is not None:
            rows.append(
                _make_row(
                    keys=keys,
                    section=section,
                    item="Average Forecast Confidence",
                    symbol="",
                    metric="avg_forecast_confidence",
                    value=avg_conf,
                    notes="Average forecast_confidence across symbols with data.",
                    last_updated_riyadh=ts,
                )
            )

    return rows


def _build_portfolio_kpi_rows(
    *,
    keys: Sequence[str],
    section: str,
    symbols: List[str],
    qmap: Dict[str, Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
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
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Portfolio KPIs",
                symbol="",
                metric="status",
                value="Unavailable",
                notes="position_qty / avg_cost not present (or portfolio universe not provided).",
                last_updated_riyadh=ts,
            )
        )
        return rows

    unreal = total_value - total_cost
    unreal_pct = (unreal / total_cost) if total_cost > 0 else None

    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Portfolio Cost",
            symbol="",
            metric="total_cost",
            value=total_cost,
            notes="Sum(position_qty * avg_cost).",
            last_updated_riyadh=ts,
        )
    )
    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Portfolio Value",
            symbol="",
            metric="total_value",
            value=total_value,
            notes="Sum(position_qty * current_price) where available.",
            last_updated_riyadh=ts,
        )
    )
    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Unrealized P/L",
            symbol="",
            metric="unrealized_pl",
            value=unreal,
            notes="total_value - total_cost.",
            last_updated_riyadh=ts,
        )
    )
    if unreal_pct is not None:
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Unrealized P/L %",
                symbol="",
                metric="unrealized_pl_pct",
                value=_pct_points(unreal_pct),
                notes=f"unrealized_pl / total_cost (display: {_fmt_pct(unreal_pct)}).",
                last_updated_riyadh=ts,
            )
        )
    return rows


def _normalize_top10_rows(top10_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _maybe_rows_from_payload(top10_payload)


def _build_top10_context_rows(
    *,
    keys: Sequence[str],
    top10_payload: Dict[str, Any],
    criteria: Optional[Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    norm_criteria = _normalize_criteria_input(criteria)
    top_rows = _normalize_top10_rows(top10_payload)

    if not top_rows:
        rows.append(
            _make_row(
                keys=keys,
                section="Top 10 Investments",
                item="Status",
                symbol="",
                metric="top10_status",
                value="Unavailable",
                notes="Top10 payload is empty.",
                last_updated_riyadh=ts,
            )
        )
        return rows

    rows.append(
        _make_row(
            keys=keys,
            section="Top 10 Investments",
            item="Status",
            symbol="",
            metric="top10_count",
            value=len(top_rows),
            notes="Top10 rows generated through selector/engine path.",
            last_updated_riyadh=ts,
        )
    )

    rows.append(
        _make_row(
            keys=keys,
            section="Top 10 Investments",
            item="Criteria Snapshot",
            symbol="",
            metric="criteria_snapshot",
            value=_criteria_snapshot_text(norm_criteria),
            notes=_criteria_summary_note(norm_criteria),
            last_updated_riyadh=ts,
        )
    )

    for i, raw in enumerate(top_rows[:10], start=1):
        if not isinstance(raw, dict):
            continue

        sym = _safe_str(raw.get("symbol") or raw.get("ticker") or raw.get("code"))
        name = _safe_str(raw.get("name"))
        rank = _as_int(raw.get("top10_rank"))
        if rank is None:
            rank = i

        roi_horizon_key = "expected_roi_3m"
        days = int(norm_criteria["invest_period_days"])
        if days <= 45:
            roi_horizon_key = "expected_roi_1m"
        elif days <= 135:
            roi_horizon_key = "expected_roi_3m"
        else:
            roi_horizon_key = "expected_roi_12m"

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

        rows.append(
            _make_row(
                keys=keys,
                section="Top 10 Investments",
                item=f"#{rank}",
                symbol=sym,
                metric=roi_horizon_key,
                value=_pct_points(roi_val) if roi_val is not None else "",
                notes=" | ".join(note_parts) if note_parts else "Top10 ranked item.",
                last_updated_riyadh=ts,
            )
        )

        if overall is not None:
            rows.append(
                _make_row(
                    keys=keys,
                    section="Top 10 Context",
                    item=f"#{rank} Overall Score",
                    symbol=sym,
                    metric="overall_score",
                    value=overall,
                    notes=f"Rank={rank}" + (f" | {name}" if name else ""),
                    last_updated_riyadh=ts,
                )
            )

        if sel_reason or reco_reason:
            rows.append(
                _make_row(
                    keys=keys,
                    section="Top 10 Context",
                    item=f"#{rank} Selection Logic",
                    symbol=sym,
                    metric="selection_reason",
                    value=rank,
                    notes=sel_reason or reco_reason,
                    last_updated_riyadh=ts,
                )
            )

    return rows


# -----------------------------------------------------------------------------
# Auto-universe defaults
# -----------------------------------------------------------------------------
def _default_universes() -> Dict[str, List[str]]:
    indices = _env_csv("TFB_INSIGHTS_INDICES", "TASI,NOMU,^GSPC,^IXIC,^FTSE")
    commodities_fx = _env_csv("TFB_INSIGHTS_COMMODITIES_FX", "GC=F,BZ=F,USDSAR=X,EURUSD=X")
    return {
        "Indices & Benchmarks": indices,
        "Commodities & FX": commodities_fx,
    }


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
# =============================================================================
# Priority / signal helpers (v3.0.0)
# =============================================================================

def _signal_from_reco(reco: str) -> str:
    r = _safe_str(reco).upper().replace(" ", "_").replace("-", "_")
    return {"STRONG_BUY":"STRONG_BUY","BUY":"BUY","HOLD":"HOLD","NEUTRAL":"HOLD",
            "REDUCE":"SELL","SELL":"SELL","ACCUMULATE":"BUY","AVOID":"SELL"}.get(r, "")

def _priority_from_rank(rank: int) -> str:
    if rank <= 3: return _PRI_HIGH
    if rank <= 7: return _PRI_MEDIUM
    return _PRI_LOW

def _priority_from_score(score: Optional[float]) -> str:
    if score is None: return _PRI_LOW
    if score >= 75: return _PRI_HIGH
    if score >= 55: return _PRI_MEDIUM
    return _PRI_LOW

def _priority_from_risk(risk: Optional[float]) -> str:
    if risk is None: return _PRI_LOW
    if risk >= 75: return _PRI_HIGH
    if risk >= 55: return _PRI_MEDIUM
    return _PRI_LOW


# =============================================================================
# Section 3: Risk Alerts  (v3.0.0 — was embedded in Market Summary)
# =============================================================================

def _build_risk_alert_rows(
    *,
    keys: Sequence[str],
    all_qmap: Dict[str, Dict[str, Any]],
    norm_criteria: Dict[str, Any],
    ts: str,
) -> List[Dict[str, Any]]:
    """Section 3 — Risk Alerts. Signal: ALERT / HOLD. Priority: High / Medium / Low."""
    rows: List[Dict[str, Any]] = []
    if not all_qmap:
        return rows

    max_risk = _as_float(norm_criteria.get("max_risk_score") or 60.0) or 60.0
    at_risk:  List[Tuple[float, str, Dict[str, Any]]] = []
    caution:  List[Tuple[str, str, str]] = []

    for sym, d in all_qmap.items():
        risk_sc = _as_float(d.get("risk_score"))
        dd1y    = _as_float(d.get("max_drawdown_1y"))
        rsi_sig = _safe_str(d.get("rsi_signal") or "").lower()
        vol30d  = _pct_points(d.get("volatility_30d"))

        if risk_sc is not None and risk_sc >= 55.0:
            at_risk.append((risk_sc, sym, d))
        if dd1y is not None:
            dd_abs = abs(_pct_points(dd1y) or 0)
            if dd_abs >= 30.0:
                caution.append((sym, f"Max drawdown 1Y: {dd_abs:.1f}%", _PRI_HIGH if dd_abs >= 40 else _PRI_MEDIUM))
        if rsi_sig == "overbought":
            caution.append((sym, "RSI Overbought — consider reducing position", _PRI_MEDIUM))
        if vol30d is not None and vol30d >= 40.0:
            caution.append((sym, f"High volatility 30D: {vol30d:.1f}%", _PRI_MEDIUM))

    if not at_risk and not caution:
        rows.append(_make_row(
            keys=keys, section="Risk Alerts", item="Summary",
            symbol="", metric="risk_alert_count",
            value=0, signal=_SIGNAL_OK, priority=_PRI_LOW,
            notes="No high-risk conditions detected.",
            as_of_riyadh=ts,
        ))
        return rows

    at_risk.sort(key=lambda t: t[0], reverse=True)
    high_count = sum(1 for r, _, _ in at_risk if r >= 70)
    rows.append(_make_row(
        keys=keys, section="Risk Alerts", item="Summary",
        symbol="", metric="at_risk_count",
        value=len(at_risk), signal=_SIGNAL_ALERT if high_count > 0 else _SIGNAL_WARN,
        priority=_PRI_HIGH if high_count > 0 else _PRI_MEDIUM,
        notes=f"{len(at_risk)} symbol(s) with risk_score > 55. {high_count} above 70.",
        as_of_riyadh=ts,
    ))

    for risk_sc, sym, d in at_risk[:8]:
        rb   = _safe_str(d.get("risk_bucket") or "")
        reco = _safe_str(d.get("recommendation") or "")
        vol  = _pct_points(d.get("volatility_30d"))
        pri  = _PRI_HIGH if risk_sc >= 70 else _PRI_MEDIUM
        sig  = _SIGNAL_ALERT if risk_sc >= max_risk else _SIGNAL_WARN
        note = f"Risk={round(risk_sc, 1)}"
        if rb:   note += f" | Bucket={rb}"
        if reco: note += f" | Reco={reco}"
        if vol:  note += f" | Vol30D={vol:.1f}%"
        rows.append(_make_row(
            keys=keys, section="Risk Alerts", item=f"High Risk – {sym}",
            symbol=sym, metric="risk_score",
            value=round(risk_sc, 1), signal=sig, priority=pri,
            notes=note, as_of_riyadh=ts,
        ))

    for sym, reason, pri in caution[:6]:
        rows.append(_make_row(
            keys=keys, section="Risk Alerts", item=f"Caution – {sym}",
            symbol=sym, metric="caution_flag",
            value="Caution", signal=_SIGNAL_ALERT, priority=pri,
            notes=reason, as_of_riyadh=ts,
        ))

    return rows


# =============================================================================
# Section 4: Short-Term Opportunities  (NEW v3.0.0)
# =============================================================================

def _build_short_term_rows(
    *,
    keys: Sequence[str],
    all_qmap: Dict[str, Dict[str, Any]],
    ts: str,
    min_tech_score: float = 58.0,
    max_items: int = 7,
) -> List[Dict[str, Any]]:
    """
    Section 4 — Short-Term Opportunities (NEW v3.0.0).
    Uses scoring.py v3.0.0 fields: technical_score, short_term_signal, rsi_signal.
    Qualifying: technical_score >= min_tech_score AND short_term_signal in (BUY, STRONG_BUY).
    """
    rows: List[Dict[str, Any]] = []
    if not all_qmap:
        return rows

    candidates: List[Tuple[float, str, Dict[str, Any]]] = []
    for sym, d in all_qmap.items():
        tech   = _as_float(d.get("technical_score"))
        st_sig = _safe_str(d.get("short_term_signal") or "").upper()
        if tech is None or tech < min_tech_score:
            continue
        if st_sig not in ("BUY", "STRONG_BUY"):
            continue
        candidates.append((tech, sym, d))

    if not candidates:
        rows.append(_make_row(
            keys=keys, section="Short-Term Opportunities",
            item="Status", symbol="", metric="st_opportunities_count",
            value=0, signal=_SIGNAL_OK, priority=_PRI_LOW,
            notes=f"No symbols with technical_score ≥ {min_tech_score:.0f} + ST signal BUY.",
            as_of_riyadh=ts,
        ))
        return rows

    candidates.sort(key=lambda t: t[0], reverse=True)
    rows.append(_make_row(
        keys=keys, section="Short-Term Opportunities",
        item="Summary", symbol="", metric="st_opportunities_count",
        value=len(candidates), signal="BUY", priority=_PRI_HIGH,
        notes=f"{len(candidates)} symbol(s) with strong technical setup (scoring.py v3.0.0).",
        as_of_riyadh=ts,
    ))

    for tech, sym, d in candidates[:max_items]:
        st_sig   = _safe_str(d.get("short_term_signal") or "").upper()
        rsi_sig  = _safe_str(d.get("rsi_signal") or "")
        rsi_val  = _as_float(d.get("rsi_14"))
        vol_r    = _as_float(d.get("volume_ratio"))
        drp      = _as_float(d.get("day_range_position"))
        period   = _safe_str(d.get("invest_period_label") or "")
        roi_1m   = _pct_points(d.get("expected_roi_1m"))
        upside   = _as_float(d.get("upside_pct"))
        name     = _safe_str(d.get("name") or sym)

        sig  = "STRONG_BUY" if st_sig == "STRONG_BUY" else "BUY"
        pri  = _PRI_HIGH if (st_sig == "STRONG_BUY" or tech >= 75) else (_PRI_MEDIUM if tech >= 65 else _PRI_LOW)

        note_parts = [f"Tech={round(tech, 1)}"]
        if rsi_sig:  note_parts.append(f"RSI={rsi_sig}" + (f"({rsi_val:.0f})" if rsi_val else ""))
        if vol_r:    note_parts.append(f"VolRatio={round(vol_r, 2)}x")
        if drp is not None: note_parts.append(f"DayPos={round(drp*100, 0):.0f}%")
        if period:   note_parts.append(f"Horizon={period}")
        if upside:   note_parts.append(f"Upside={_fmt_pct(upside)}")
        if name != sym: note_parts.append(name)

        rows.append(_make_row(
            keys=keys, section="Short-Term Opportunities",
            item=f"{st_sig} – {sym}", symbol=sym, metric="technical_score",
            value=f"{round(tech, 1)}" + (f" | ROI={roi_1m:.2f}%" if roi_1m is not None else ""),
            signal=sig, priority=pri,
            notes=" | ".join(note_parts),
            as_of_riyadh=ts,
        ))

    return rows


# =============================================================================
# Section 5: Portfolio KPIs  (upgraded from Portfolio Health v2.0.0)
# =============================================================================

def _build_portfolio_kpis_rows(
    *,
    keys: Sequence[str],
    symbols: List[str],
    qmap: Dict[str, Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
    """
    Section 5 — Portfolio KPIs (v3.0.0 upgrade).
    Classic P/L + NEW: stop_loss distances, weight deviations, rebalance signals.
    """
    rows: List[Dict[str, Any]] = []
    total_cost = total_value = total_day_pl = 0.0
    at_risk_count = position_count = 0
    have_positions = False
    stop_alerts:  List[Tuple[str, float, str]] = []
    rebal_alerts: List[Tuple[str, str, float]] = []

    for sym in symbols:
        d     = qmap.get(sym) or {}
        qty   = _as_float(d.get("position_qty"))
        avg_c = _as_float(d.get("avg_cost"))
        px    = _as_float(d.get("current_price"))
        day_pl= _as_float(d.get("day_pl"))
        risk_sc = _as_float(d.get("risk_score"))
        dist_sl = _as_float(d.get("distance_to_sl_pct"))
        w_dev   = _as_float(d.get("weight_deviation"))
        rebal   = _safe_str(d.get("rebalance_signal") or "")

        if qty is None or avg_c is None or qty == 0:
            continue
        have_positions = True
        position_count += 1
        cost = float(qty) * float(avg_c)
        total_cost += cost
        if px:       total_value  += float(qty) * float(px)
        if day_pl:   total_day_pl += float(day_pl)
        if risk_sc and risk_sc > 60.0: at_risk_count += 1

        if dist_sl is not None:
            dp = _pct_points(dist_sl) or 0.0
            if 0 < dp < 3.0:   stop_alerts.append((sym, dp, _PRI_HIGH))
            elif 3.0 <= dp < 10.0: stop_alerts.append((sym, dp, _PRI_MEDIUM))

        if w_dev is not None:
            dev_abs = abs(_pct_points(w_dev) or 0.0)
            if dev_abs >= 5.0:
                sig_rb = _safe_str(rebal or ("Add" if (w_dev or 0) < 0 else "Trim"))
                rebal_alerts.append((sym, sig_rb, dev_abs))

    if not have_positions:
        rows.append(_make_row(
            keys=keys, section="Portfolio KPIs", item="Portfolio",
            symbol="", metric="status",
            value="No Positions", signal=_SIGNAL_OK, priority=_PRI_LOW,
            notes="No position_qty / avg_cost found. Enter holdings in My_Portfolio.",
            as_of_riyadh=ts,
        ))
        return rows

    unrealized_pl = total_value - total_cost
    unreal_pct    = (unrealized_pl / total_cost) if total_cost > 0 else None
    pl_pct_pts    = _pct_points(unreal_pct) or 0.0
    pl_signal     = _SIGNAL_OK if pl_pct_pts >= 0 else _SIGNAL_ALERT
    health_score  = max(0.0, min(100.0, 60.0 + min(30.0, max(-30.0, pl_pct_pts)) -
                                  (at_risk_count / position_count * 20.0 if position_count else 0)))

    rows.append(_make_row(
        keys=keys, section="Portfolio KPIs", item="Positions",
        symbol="", metric="position_count",
        value=position_count, signal="", priority=_PRI_LOW,
        notes=f"Health={round(health_score, 1)}/100. {at_risk_count} position(s) risk_score > 60.",
        as_of_riyadh=ts,
    ))
    rows.append(_make_row(
        keys=keys, section="Portfolio KPIs", item="Total Value",
        symbol="", metric="total_value",
        value=round(total_value, 2), signal="", priority=_PRI_LOW,
        notes=f"Cost basis: {round(total_cost, 2)}",
        as_of_riyadh=ts,
    ))
    rows.append(_make_row(
        keys=keys, section="Portfolio KPIs", item="Unrealized P/L",
        symbol="", metric="unrealized_pl",
        value=round(unrealized_pl, 2), signal=pl_signal,
        priority=_PRI_HIGH if pl_pct_pts < -15 else (_PRI_MEDIUM if pl_pct_pts < -5 else _PRI_LOW),
        notes=f"{_fmt_pct(unreal_pct)} | total_value − total_cost.",
        as_of_riyadh=ts,
    ))
    if total_day_pl != 0.0:
        rows.append(_make_row(
            keys=keys, section="Portfolio KPIs", item="Today's P/L",
            symbol="", metric="day_pl",
            value=round(total_day_pl, 2),
            signal=_SIGNAL_OK if total_day_pl > 0 else _SIGNAL_ALERT,
            priority=_PRI_MEDIUM if abs(total_day_pl) > total_cost * 0.01 else _PRI_LOW,
            notes=f"Sum(day_pl) across {position_count} positions.",
            as_of_riyadh=ts,
        ))

    # NEW v3.0.0: Stop loss alerts
    for sym, dist_pp, pri in sorted(stop_alerts, key=lambda t: t[1]):
        rows.append(_make_row(
            keys=keys, section="Portfolio KPIs", item=f"Near SL – {sym}",
            symbol=sym, metric="distance_to_sl_pct",
            value=f"{dist_pp:.2f}%", signal=_SIGNAL_ALERT, priority=pri,
            notes=f"Price only {dist_pp:.2f}% above stop loss. {'Review immediately.' if pri == _PRI_HIGH else 'Monitor closely.'}",
            as_of_riyadh=ts,
        ))

    # NEW v3.0.0: Weight deviation alerts
    for sym, sig_rb, dev_abs in sorted(rebal_alerts, key=lambda t: t[2], reverse=True):
        pri    = _PRI_HIGH if dev_abs >= 10.0 else _PRI_MEDIUM
        sig_out= "BUY" if sig_rb.lower() in ("add", "buy") else "SELL"
        rows.append(_make_row(
            keys=keys, section="Portfolio KPIs", item=f"Rebalance – {sym}",
            symbol=sym, metric="weight_deviation",
            value=f"{dev_abs:.1f}% → {sig_rb}", signal=sig_out, priority=pri,
            notes=f"Portfolio weight drifted {dev_abs:.1f}% from target. Action: {sig_rb}.",
            as_of_riyadh=ts,
        ))

    return rows


# =============================================================================
# Section 6: Macro Signals  (NEW v3.0.0)
# =============================================================================

def _build_macro_signal_rows(
    *,
    keys: Sequence[str],
    all_qmap: Dict[str, Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
    """
    Section 6 — Macro Signals (NEW v3.0.0).
    Scans Global_Markets rows for sector_signal, vs_sp500_ytd, analyst_consensus.
    """
    rows: List[Dict[str, Any]] = []
    if not all_qmap:
        return rows

    sector_signals: Dict[str, List[str]] = {}
    outperformers:  List[Tuple[float, str]] = []
    underperformers:List[Tuple[float, str]] = []

    for sym, d in all_qmap.items():
        sec_sig = _safe_str(d.get("sector_signal") or "").lower()
        sector  = _safe_str(d.get("sector") or "")
        vs_sp   = _pct_points(d.get("vs_sp500_ytd"))
        if sec_sig in ("bullish", "bearish", "neutral") and sector:
            sector_signals.setdefault(sector, []).append(sec_sig)
        if vs_sp is not None:
            if vs_sp >= 5.0:  outperformers.append((vs_sp, sym))
            elif vs_sp <= -5.0: underperformers.append((vs_sp, sym))

    if not sector_signals and not outperformers and not underperformers:
        rows.append(_make_row(
            keys=keys, section="Macro Signals", item="Status",
            symbol="", metric="macro_signal_count",
            value="No Data", signal=_SIGNAL_OK, priority=_PRI_LOW,
            notes="No sector_signal or vs_sp500_ytd data from Global_Markets universe.",
            as_of_riyadh=ts,
        ))
        return rows

    for sector, sl in sorted(sector_signals.items()):
        bullish = sl.count("bullish"); bearish = sl.count("bearish"); total = len(sl)
        if bullish > bearish:
            sig = "BUY"; dominant = f"Bullish ({bullish}/{total})"
        elif bearish > bullish:
            sig = "SELL"; dominant = f"Bearish ({bearish}/{total})"
        else:
            sig = "HOLD"; dominant = f"Neutral ({total})"
        pri = _PRI_HIGH if bullish + bearish >= total * 0.7 else _PRI_MEDIUM
        rows.append(_make_row(
            keys=keys, section="Macro Signals", item=f"Sector – {sector}",
            symbol="", metric="sector_signal",
            value=dominant, signal=sig, priority=pri,
            notes=f"{sector}: {bullish} Bullish / {bearish} Bearish / {total-bullish-bearish} Neutral.",
            as_of_riyadh=ts,
        ))

    outperformers.sort(key=lambda t: t[0], reverse=True)
    for vs_sp, sym in outperformers[:4]:
        rows.append(_make_row(
            keys=keys, section="Macro Signals", item=f"Outperform – {sym}",
            symbol=sym, metric="vs_sp500_ytd",
            value=f"+{vs_sp:.2f}% vs S&P 500", signal="BUY",
            priority=_PRI_HIGH if vs_sp >= 10.0 else _PRI_MEDIUM,
            notes=f"YTD return exceeds S&P 500 by {vs_sp:.2f}% — strong relative momentum.",
            as_of_riyadh=ts,
        ))

    underperformers.sort(key=lambda t: t[0])
    for vs_sp, sym in underperformers[:4]:
        rows.append(_make_row(
            keys=keys, section="Macro Signals", item=f"Underperform – {sym}",
            symbol=sym, metric="vs_sp500_ytd",
            value=f"{vs_sp:.2f}% vs S&P 500", signal="SELL",
            priority=_PRI_HIGH if vs_sp <= -10.0 else _PRI_MEDIUM,
            notes=f"YTD return lags S&P 500 by {abs(vs_sp):.2f}%.",
            as_of_riyadh=ts,
        ))

    return rows


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
    max_symbols_per_universe: int = _DEFAULT_MAX_SYMBOLS_PER_UNIV,
    quotes_timeout_sec: float = _DEFAULT_QUOTES_TIMEOUT_SEC,
    top10_timeout_sec: float = _DEFAULT_TOP10_TIMEOUT_SEC,
    build_budget_sec: float = _DEFAULT_BUILD_BUDGET_SEC,
) -> Dict[str, Any]:
    """
    v3.0.0: Build Insights_Analysis page rows — 9-col, 6-section executive layout.

    Sections:
      1. Market Summary          — trend signals per universe
      2. Top Picks               — top10 with signal + priority (was Top Opportunities)
      3. Risk Alerts             — high risk_score, overbought RSI, large drawdowns (NEW own section)
      4. Short-Term Opportunities— technical_score + short_term_signal from scoring.py v3.0.0 (NEW)
      5. Portfolio KPIs          — P/L + stop distances + weight deviations (upgraded)
      6. Macro Signals           — sector_signal, vs_sp500_ytd, analyst_consensus (NEW)

    All v1.6.0 public API params preserved. New params add timeout/budget control.
    """
    headers, keys, schema_source = get_insights_schema()
    ts              = _now_riyadh_iso()
    norm_criteria   = _normalize_criteria_input(criteria)
    warnings: List[str] = []
    rows: List[Dict[str, Any]] = []

    # Section enable flags (from criteria dict + function params)
    do_market_summary = norm_criteria.get("include_market_summary", True)
    do_top_picks      = include_top10_section and norm_criteria.get("include_top_opportunities", True)
    do_risk_alerts    = True  # always on
    do_short_term     = norm_criteria.get("include_short_term", True)
    do_portfolio_kpis = include_portfolio_kpis and norm_criteria.get("include_portfolio_health", True)
    do_risk_scenarios = norm_criteria.get("include_risk_scenarios", True)
    do_macro_signals  = norm_criteria.get("include_macro_signals", True)

    deadline = asyncio.get_running_loop().time() + max(1.0, float(build_budget_sec))

    # ── Criteria block ────────────────────────────────────────────────────
    if include_criteria_rows:
        rows.extend(build_criteria_rows(criteria=norm_criteria, last_updated_riyadh=ts))

    # ── System rows ───────────────────────────────────────────────────────
    if include_system_rows:
        rows.append(_make_row(
            keys=keys, section="System", item="Builder Version",
            symbol="", metric="insights_builder_version",
            value=INSIGHTS_BUILDER_VERSION, signal="", priority=_PRI_LOW,
            notes="core/analysis/insights_builder.py v3.0.0 — 9-col / 6-section schema",
            as_of_riyadh=ts,
        ))

    # ── Resolve universes ─────────────────────────────────────────────────
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
    rows.append(_make_row(
        keys=keys, section="System", item="Build Status",
        symbol="", metric="build_status",
        value="OK" if build_ok else "WARN",
        signal=_SIGNAL_OK if build_ok else _SIGNAL_WARN, priority=_PRI_LOW,
        notes="OK = engine + universes available. WARN = criteria/system only.",
        as_of_riyadh=ts,
    ))

    if not engine or not eff_universes:
        msg = "No engine passed." if not engine else "No universes/symbols provided."
        rows.append(_make_row(
            keys=keys, section="System", item="Engine",
            symbol="", metric="engine_status",
            value="Not provided" if not engine else "Universes Empty",
            signal=_SIGNAL_WARN, priority=_PRI_MEDIUM,
            notes=msg + " Returning criteria/system rows only.",
            as_of_riyadh=ts,
        ))
        if do_risk_scenarios:
            rows.extend(_build_risk_scenario_rows(keys=keys, norm_criteria=norm_criteria, ts=ts))
        return _wrap_result(
            headers=headers, keys=keys, rows=rows, schema_source=schema_source,
            ts=ts, engine_used=bool(engine), auto_used=auto_used,
            eff_universes=eff_universes, mode=mode, norm_criteria=norm_criteria,
            warnings=warnings,
        )

    cap = max(1, min(int(max_symbols_per_universe), 500))
    all_qmap: Dict[str, Dict[str, Any]] = {}
    portfolio_qmap: Dict[str, Dict[str, Any]] = {}
    portfolio_symbols: List[str] = []

    # ── Section 1: Market Summary — fetch quotes per universe ─────────────
    for section_name, sym_list in eff_universes.items():
        remaining = _remaining_budget(deadline)
        if remaining is not None and remaining <= 0.10:
            warnings.append(f"Skipped '{section_name}' — budget exhausted.")
            rows.append(_make_row(
                keys=keys, section="System", item="Warning",
                symbol="", metric="build_budget_exhausted",
                value="WARN", signal=_SIGNAL_WARN, priority=_PRI_MEDIUM,
                notes=f"Skipped '{section_name}' — build budget exhausted.",
                as_of_riyadh=ts,
            ))
            break

        syms = _dedupe_keep_order(sym_list or [])[:cap]
        try:
            qmap = await _fetch_quotes_map(
                engine, syms, mode=mode or "",
                timeout_sec=min(quotes_timeout_sec, remaining or quotes_timeout_sec),
            )
        except Exception as exc:
            qmap = {}
            warnings.append(f"Quote fetch degraded for '{section_name}': {exc}")

        all_qmap.update(qmap)

        if do_market_summary:
            rows.extend(_build_universe_snapshot_rows(
                keys=keys, section=section_name, symbols=syms, qmap=qmap, ts=ts,
            ))

        if section_name.strip().lower() in {"my_portfolio", "portfolio", "my portfolio", "selected symbols"}:
            portfolio_qmap    = qmap
            portfolio_symbols = syms

    # ── Section 2: Top Picks ──────────────────────────────────────────────
    if do_top_picks:
        remaining = _remaining_budget(deadline)
        if remaining is not None and remaining > 0.25:
            top10_payload: Dict[str, Any] = {}
            try:
                top10_payload = await _fetch_top10_payload(
                    engine, criteria=norm_criteria,
                    limit=int(norm_criteria.get("top_n", 10)),
                    mode=mode or "",
                    timeout_sec=min(top10_timeout_sec, remaining or top10_timeout_sec),
                )
            except Exception as exc:
                warnings.append(f"Top Picks payload degraded: {exc}")

            if top10_payload:
                rows.extend(_build_top10_context_rows(
                    keys=keys, top10_payload=top10_payload,
                    criteria=norm_criteria, ts=ts,
                ))
            else:
                top10_syms: List[str] = []
                try:
                    top10_syms = await _fetch_top10_symbols(
                        engine, criteria=norm_criteria,
                        limit=int(norm_criteria.get("top_n", 10)),
                        timeout_sec=min(top10_timeout_sec, _remaining_budget(deadline) or top10_timeout_sec),
                    )
                except Exception:
                    pass
                if top10_syms:
                    try:
                        qmap10 = await _fetch_quotes_map(
                            engine, top10_syms, mode=mode or "",
                            timeout_sec=min(quotes_timeout_sec, _remaining_budget(deadline) or quotes_timeout_sec),
                        )
                        all_qmap.update(qmap10)
                    except Exception:
                        qmap10 = {}
                    # Build simple top picks from symbol map
                    rows.append(_make_row(
                        keys=keys, section="Top Picks", item="Summary",
                        symbol="", metric="top_picks_count",
                        value=len(top10_syms), signal="", priority=_PRI_LOW,
                        notes=f"Top {len(top10_syms)} picks (symbol-only fallback).",
                        as_of_riyadh=ts,
                    ))
                    for i, sym in enumerate(top10_syms, start=1):
                        d     = qmap10.get(sym) or {}
                        roi3  = _as_float(_pick(d, "expected_roi_3m"))
                        reco  = _safe_str(_pick(d, "recommendation") or "")
                        name  = _safe_str(_pick(d, "name") or sym)
                        tech  = _as_float(_pick(d, "technical_score"))
                        st    = _safe_str(_pick(d, "short_term_signal") or "")
                        sig   = _signal_from_reco(reco)
                        pri   = _priority_from_rank(i)
                        note  = f"Reco={reco}" if reco else ""
                        if tech: note += f" | Tech={round(tech, 1)}"
                        if st:   note += f" | ST={st}"
                        if name != sym: note += f" | {name}"
                        rows.append(_make_row(
                            keys=keys, section="Top Picks",
                            item=f"#{i} {sym}", symbol=sym, metric="expected_roi_3m",
                            value=_fmt_pct(roi3) if roi3 is not None else reco,
                            signal=sig, priority=pri,
                            notes=note.strip(" |") or "Top pick.",
                            as_of_riyadh=ts,
                        ))

    # ── Section 3: Risk Alerts ────────────────────────────────────────────
    if do_risk_alerts and all_qmap:
        rows.extend(_build_risk_alert_rows(
            keys=keys, all_qmap=all_qmap, norm_criteria=norm_criteria, ts=ts,
        ))

    # ── Section 4: Short-Term Opportunities (NEW v3.0.0) ─────────────────
    if do_short_term and all_qmap:
        rows.extend(_build_short_term_rows(keys=keys, all_qmap=all_qmap, ts=ts))

    # ── Section 5: Portfolio KPIs ─────────────────────────────────────────
    if do_portfolio_kpis:
        if portfolio_symbols and portfolio_qmap:
            rows.extend(_build_portfolio_kpis_rows(
                keys=keys, symbols=portfolio_symbols, qmap=portfolio_qmap, ts=ts,
            ))
        else:
            rows.append(_make_row(
                keys=keys, section="Portfolio KPIs", item="Status",
                symbol="", metric="portfolio_status",
                value="Not Included", signal=_SIGNAL_OK, priority=_PRI_LOW,
                notes="My_Portfolio not in universes. Add My_Portfolio to pages_selected.",
                as_of_riyadh=ts,
            ))

    # ── Risk Scenarios (kept for backward compat) ─────────────────────────
    if do_risk_scenarios:
        rows.extend(_build_risk_scenario_rows(keys=keys, norm_criteria=norm_criteria, ts=ts))

    # ── Section 6: Macro Signals (NEW v3.0.0) ─────────────────────────────
    if do_macro_signals and all_qmap:
        rows.extend(_build_macro_signal_rows(keys=keys, all_qmap=all_qmap, ts=ts))

    if warnings:
        rows.insert(0, _make_row(
            keys=keys, section="System", item="Builder Warnings",
            symbol="", metric="builder_warnings",
            value="WARN", signal=_SIGNAL_WARN, priority=_PRI_MEDIUM,
            notes=" | ".join(warnings[:3]),
            as_of_riyadh=ts,
        ))

    return _wrap_result(
        headers=headers, keys=keys, rows=rows, schema_source=schema_source,
        ts=ts, engine_used=True, auto_used=auto_used,
        eff_universes=eff_universes, mode=mode, norm_criteria=norm_criteria,
        warnings=warnings,
    )


def _wrap_result(
    *,
    headers: List[str],
    keys: List[str],
    rows: List[Dict[str, Any]],
    schema_source: str,
    ts: str,
    engine_used: bool,
    auto_used: bool,
    eff_universes: Dict[str, List[str]],
    mode: str,
    norm_criteria: Dict[str, Any],
    warnings: List[str],
) -> Dict[str, Any]:
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
            "schema_columns": len(keys),
            "generated_at_riyadh": ts,
            "engine_used": engine_used,
            "auto_universe_used": auto_used,
            "universes": list(eff_universes.keys()),
            "mode": mode,
            "builder_version": INSIGHTS_BUILDER_VERSION,
            "criteria_snapshot": _criteria_snapshot_text(norm_criteria),
            "warnings": warnings,
        },
    }


__all__ = [
    "INSIGHTS_BUILDER_VERSION",
    "get_insights_schema",
    "build_criteria_rows",
    "build_insights_analysis_rows",
]
