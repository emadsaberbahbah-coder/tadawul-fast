#!/usr/bin/env python3
# core/analysis/insights_builder.py
"""
================================================================================
Insights Analysis Builder -- v2.0.0
(EXECUTIVE-LAYOUT / 4-SECTION / SCHEMA-10-COL / SCENARIO-AWARE / TIME-BOXED)
================================================================================
Tadawul Fast Bridge (TFB)

v2.0.0 changes vs v1.7.0
--------------------------
FIX: Schema updated to 10 columns (schema_registry v3.0.0).
  Old fallback was 7 columns. New: section, category, item, symbol, metric,
  value, signal, score, notes, last_updated_riyadh.
  All _make_row() calls now populate the 3 new columns.

FIX: _make_row() signature extended with category, signal, score parameters.
  Callers using keyword args are fully backward-compatible (all new params
  default to empty/"").

REDESIGN: Executive 4-section layout replacing the raw universe-snapshot rows.
  Section 1 -- Market Summary     (one sub-row per universe with trend signal)
  Section 2 -- Risk Scenarios     (3 rows: Conservative / Moderate / Aggressive)
  Section 3 -- Top Opportunities  (Top 10 ranked with score + signal)
  Section 4 -- Portfolio Health   (P/L KPIs with OK/WARN/ALERT signal)

  Sections are controlled by criteria_model.AdvisorCriteria include flags:
    include_market_summary, include_risk_scenarios,
    include_top_opportunities, include_portfolio_health.
  All default to True; callers can disable individual sections.

ENH: Integrates criteria_model.signal_for_value() for consistent Signal column.
ENH: Integrates criteria_model.build_scenario_specs() for Risk Scenarios section.
ENH: Portfolio Health now counts at-risk positions (risk_score > max_risk) and
  day P/L from My_Portfolio's day_pl field.
ENH: Market Summary derives a BULLISH/BEARISH/NEUTRAL trend signal per universe
  from average percent_change and average overall_score.

Preserved from v1.7.0:
  All async timeout guards and build budget logic.
  All engine integration patterns (batch, per-symbol, fallback chains).
  All payload parsing helpers (_coerce_rows_list, etc.).
  build_criteria_rows() API is unchanged.
  get_insights_schema() API is unchanged.
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

INSIGHTS_BUILDER_VERSION = "2.0.0"
_RIYADH_TZ = timezone(timedelta(hours=3))

# 10-column schema (aligned with schema_registry v3.0.0)
_FALLBACK_KEYS = [
    "section", "category", "item", "symbol",
    "metric", "value", "signal", "score",
    "notes", "last_updated_riyadh",
]
_FALLBACK_HEADERS = [
    "Section", "Category", "Item", "Symbol",
    "Metric", "Value", "Signal", "Score",
    "Notes", "Last Updated (Riyadh)",
]

# Signal vocabulary (matches schema_registry Signal col description)
_SIGNAL_UP       = "UP"
_SIGNAL_DOWN     = "DOWN"
_SIGNAL_NEUTRAL  = "NEUTRAL"
_SIGNAL_HIGH     = "HIGH"
_SIGNAL_MODERATE = "MODERATE"
_SIGNAL_LOW      = "LOW"
_SIGNAL_OK       = "OK"
_SIGNAL_WARN     = "WARN"
_SIGNAL_ALERT    = "ALERT"

# Env-configurable timeouts
_DEFAULT_QUOTES_TIMEOUT_SEC     = float(os.getenv("TFB_INSIGHTS_QUOTES_TIMEOUT_SEC", "4.0") or "4.0")
_DEFAULT_TOP10_TIMEOUT_SEC      = float(os.getenv("TFB_INSIGHTS_TOP10_TIMEOUT_SEC", "4.0") or "4.0")
_DEFAULT_BUILD_BUDGET_SEC       = float(os.getenv("TFB_INSIGHTS_BUILD_BUDGET_SEC", "10.0") or "10.0")
_DEFAULT_MAX_SYMBOLS_PER_UNIV   = int(os.getenv("TFB_INSIGHTS_MAX_SYMBOLS_PER_UNIVERSE", "12") or "12")


# =============================================================================
# Small utilities
# =============================================================================

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
    return x / 100.0 if abs(x) >= 1.5 else x


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
    return x * 100.0 if abs(x) <= 1.5 else x


def _fmt_pct(v: Any) -> str:
    x = _pct_points(v)
    return "" if x is None else f"{x:.2f}%"


def _fmt_num(v: Any, decimals: int = 2) -> str:
    x = _as_float(v)
    return _safe_str(v) if x is None else f"{x:.{decimals}f}"


def _csv_list(raw: str) -> List[str]:
    items = [p.strip() for p in (raw or "").replace("\n", ",").split(",") if p.strip()]
    seen: set = set()
    out: List[str] = []
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
    seen: set = set()
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


def _avg(vals: List[float]) -> Optional[float]:
    return None if not vals else sum(vals) / float(len(vals))


def _remaining_budget(deadline: Optional[float]) -> Optional[float]:
    if deadline is None:
        return None
    remaining = asyncio.get_running_loop().time() - deadline
    # Note: deadline - current_time
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


def _is_signature_mismatch_typeerror(exc: TypeError) -> bool:
    msg = _safe_str(exc).lower()
    markers = (
        "unexpected keyword", "positional argument", "required positional argument",
        "takes no keyword", "takes from", "takes exactly", "got an unexpected keyword",
        "got multiple values", "missing 1 required", "keyword-only argument",
    )
    return any(m in msg for m in markers)


def _is_probable_symbol_token(value: Any) -> bool:
    s = _safe_str(value)
    if not s or len(s) > 32:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._=^-:")
    return all(ch in allowed for ch in s)


# =============================================================================
# Signal helpers
# =============================================================================

def _signal_from_reco(reco: str) -> str:
    """Map recommendation string to Signal column value."""
    r = _safe_str(reco).upper().replace(" ", "_").replace("-", "_")
    return {
        "STRONG_BUY": "STRONG_BUY",
        "BUY": "BUY",
        "HOLD": "HOLD",
        "NEUTRAL": "NEUTRAL",
        "REDUCE": "REDUCE",
        "SELL": "SELL",
        "ACCUMULATE": "BUY",
        "AVOID": "SELL",
    }.get(r, "")


def _signal_from_percent_change(avg_pct: Optional[float]) -> str:
    """UP/DOWN/NEUTRAL based on average percent change (fraction)."""
    if avg_pct is None:
        return _SIGNAL_NEUTRAL
    pp = _pct_points(avg_pct) or 0.0
    if pp > 0.5:
        return _SIGNAL_UP
    if pp < -0.5:
        return _SIGNAL_DOWN
    return _SIGNAL_NEUTRAL


def _signal_from_score(score: Optional[float], *, high: float = 65.0, low: float = 40.0) -> str:
    """HIGH/MODERATE/LOW based on 0-100 score."""
    if score is None:
        return _SIGNAL_NEUTRAL
    if score >= high:
        return _SIGNAL_HIGH
    if score >= low:
        return _SIGNAL_MODERATE
    return _SIGNAL_LOW


def _signal_from_pl_pct(pl_pct: Optional[float]) -> str:
    """OK/WARN/ALERT based on P/L %."""
    if pl_pct is None:
        return _SIGNAL_NEUTRAL
    pct = _pct_points(pl_pct) or 0.0
    if pct >= 0:
        return _SIGNAL_OK
    if pct >= -10.0:
        return _SIGNAL_WARN
    return _SIGNAL_ALERT


# =============================================================================
# Dict/model extraction helpers (preserved from v1.7.0)
# =============================================================================

def _extract_row_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    for method in ("model_dump", "dict"):
        try:
            fn = getattr(v, method, None)
            if callable(fn):
                d = fn(mode="python") if method == "model_dump" else fn()
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
        out.append({key: row[i] if i < len(row) else None for i, key in enumerate(keys)})
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


# =============================================================================
# Schema helpers
# =============================================================================

def get_insights_schema() -> Tuple[List[str], List[str], str]:
    """
    Return (headers, keys, source_marker) for the Insights_Analysis schema.
    Reads from schema_registry v3.0.0 (10 columns).
    Falls back to _FALLBACK_HEADERS / _FALLBACK_KEYS if registry unavailable.
    """
    try:
        from core.sheets.schema_registry import get_sheet_headers, get_sheet_keys  # type: ignore
        headers = get_sheet_headers("Insights_Analysis")
        keys = get_sheet_keys("Insights_Analysis")
        if headers and keys and len(headers) == len(keys) == 10:
            return headers, keys, "schema_registry"
    except Exception as e:
        logger.debug("get_insights_schema: schema_registry unavailable: %r", e)

    # Fallback: full 10-column schema
    return list(_FALLBACK_HEADERS), list(_FALLBACK_KEYS), "fallback"


def _get_criteria_fields() -> List[Dict[str, Any]]:
    """Return criteria field definitions from schema_registry or hardcoded fallback."""
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore
        spec = get_sheet_spec("Insights_Analysis")
        cfs = getattr(spec, "criteria_fields", None)
        if cfs is None and isinstance(spec, Mapping):
            cfs = spec.get("criteria_fields")
        out: List[Dict[str, Any]] = []
        for cf in list(cfs or []):
            if isinstance(cf, Mapping):
                entry = {k: cf.get(k, "") for k in ("key", "label", "dtype", "default", "notes")}
            else:
                entry = {k: getattr(cf, k, "") for k in ("key", "label", "dtype", "default", "notes")}
            if entry.get("key"):
                out.append(entry)
        if out:
            return out
    except Exception:
        pass
    # Aligned with schema_registry v3.0.0 _insights_criteria_fields()
    return [
        {"key": "risk_level",          "label": "Risk Level",              "dtype": "str",   "default": "Moderate", "notes": "Low / Moderate / High."},
        {"key": "confidence_level",    "label": "Confidence Level",        "dtype": "str",   "default": "High",     "notes": "High / Medium / Low."},
        {"key": "invest_period_days",  "label": "Investment Period (Days)", "dtype": "int",   "default": "90",       "notes": "Always treated in DAYS."},
        {"key": "required_return_pct", "label": "Required Return %",       "dtype": "pct",   "default": "0.10",     "notes": "Minimum expected ROI."},
        {"key": "max_risk_score",      "label": "Max Risk Score",          "dtype": "float", "default": "60",       "notes": "Risk score ceiling."},
        {"key": "pages_selected",      "label": "Pages Selected",          "dtype": "str",   "default": "",         "notes": "CSV of pages."},
        {"key": "amount",              "label": "Amount",                  "dtype": "float", "default": "0",        "notes": "Investment amount."},
        {"key": "min_expected_roi_pct","label": "Min Expected ROI %",      "dtype": "pct",   "default": "0.00",     "notes": "Filter floor for ROI."},
        {"key": "min_ai_confidence",   "label": "Min AI Confidence",       "dtype": "float", "default": "0.60",     "notes": "Filter floor for confidence."},
    ]


# =============================================================================
# Criteria normalization
# =============================================================================

def _normalize_criteria_input(criteria: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize any criteria dict to canonical internal keys."""
    c = dict(criteria or {})

    pages = c.get("pages_selected") or c.get("pages") or c.get("selected_pages") or []
    if isinstance(pages, str):
        pages = _csv_list(pages)
    if not isinstance(pages, list):
        pages = []
    pages = [_safe_str(x) for x in pages if _safe_str(x)]

    # FIX: accept both invest_period_days (new) and investment_period_days (old)
    invest_days = _as_int(
        c.get("invest_period_days") or c.get("investment_period_days") or
        c.get("period_days") or c.get("horizon_days") or 90
    )
    if not invest_days or invest_days <= 0:
        invest_days = 90

    # FIX: accept min_expected_roi_pct (new) and min_roi/required_return_pct (old)
    min_roi = (
        c.get("min_expected_roi_pct") or c.get("min_expected_roi") or
        c.get("min_roi") or c.get("required_return_pct")
    )
    min_roi_frac = _as_fraction(min_roi)

    max_risk = _as_float(c.get("max_risk_score") or c.get("max_risk") or 60.0) or 60.0
    min_conf = _as_fraction(
        c.get("min_ai_confidence") or c.get("min_confidence") or c.get("min_confidence_score") or 0.60
    ) or 0.60
    top_n = max(1, min(50, _as_int(c.get("top_n") or c.get("limit") or 10) or 10))

    normalized = {
        "pages_selected": pages or ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "My_Portfolio"],
        "invest_period_days": invest_days,
        "horizon_days": invest_days,
        "min_expected_roi": min_roi_frac,
        "max_risk_score": max_risk,
        "min_confidence": min_conf,
        "min_volume": _as_float(c.get("min_volume") or c.get("min_liquidity")),
        "use_liquidity_tiebreak": _safe_bool(c.get("use_liquidity_tiebreak", True), True),
        "top_n": top_n,
        "enrich_final": _safe_bool(c.get("enrich_final", True), True),
        # Section flags from criteria_model.AdvisorCriteria
        "include_market_summary":     _safe_bool(c.get("include_market_summary", True), True),
        "include_risk_scenarios":     _safe_bool(c.get("include_risk_scenarios", True), True),
        "include_top_opportunities":  _safe_bool(c.get("include_top_opportunities", True), True),
        "include_portfolio_health":   _safe_bool(c.get("include_portfolio_health", True), True),
        # Risk level for scenario generation
        "risk_level":        _safe_str(c.get("risk_level") or "Moderate"),
        "required_return_pct": _as_fraction(c.get("required_return_pct") or c.get("required_return") or 0.10) or 0.10,
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
    return (
        f"Horizon={_days_to_horizon(int(norm['invest_period_days']))} "
        f"| Days={norm['invest_period_days']} "
        f"| Min ROI={roi_txt} "
        f"| Max Risk={_fmt_num(norm.get('max_risk_score'))} "
        f"| Min Confidence={_fmt_pct(norm.get('min_confidence'))}"
    )


# =============================================================================
# Row builder (10-column schema)
# =============================================================================

def _make_row(
    *,
    keys: Sequence[str],
    section: str,
    item: str,
    metric: str,
    value: Any,
    # New in v2.0.0 (schema_registry v3.0.0 new columns)
    category: str = "",
    signal: str = "",
    score: Any = "",
    # Preserved
    symbol: str = "",
    notes: str = "",
    last_updated_riyadh: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a single Insights_Analysis row dict aligned with the 10-column schema.

    New parameters vs v1.7.0:
      category -- sub-group within section (e.g. "Conservative", "KSA Watchlist")
      signal   -- UP/DOWN/NEUTRAL/HIGH/MODERATE/LOW/OK/WARN/ALERT
      score    -- numeric 0-100 score if applicable (empty string if N/A)
    """
    ts = last_updated_riyadh or _now_riyadh_iso()
    base = {
        "section":              _safe_str(section) or "General",
        "category":             _safe_str(category),
        "item":                 _safe_str(item) or "Item",
        "symbol":               _safe_str(symbol),
        "metric":               _safe_str(metric) or "metric",
        "value":                "" if value is None else value,
        "signal":               _safe_str(signal),
        "score":                "" if score is None or _safe_str(score) == "" else score,
        "notes":                _safe_str(notes),
        "last_updated_riyadh":  ts,
    }
    return {k: base.get(k, "") for k in keys}


def _warning_row(keys: Sequence[str], label: str, notes: str, ts: str) -> Dict[str, Any]:
    return _make_row(
        keys=keys, section="System", item="Warning",
        category="", symbol="", metric=label,
        value="WARN", signal=_SIGNAL_WARN, score="",
        notes=notes, last_updated_riyadh=ts,
    )


# =============================================================================
# Criteria block builder
# =============================================================================

def build_criteria_rows(
    *,
    criteria: Optional[Dict[str, Any]] = None,
    last_updated_riyadh: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Build the criteria block rows for the top of Insights_Analysis.
    One row per criteria field + a snapshot row.
    API unchanged from v1.7.0.
    """
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
        if k.endswith("_pct") or "return" in k or "confidence" in k or "roi" in k:
            vnum = _as_fraction(v)
            if vnum is not None:
                note = (note + " " if note else "") + f"(display: {_fmt_pct(vnum)})"
                v = vnum
        rows.append(_make_row(
            keys=keys, section="Criteria", category="Settings",
            item=label, symbol="", metric=k,
            value=v, signal="", score="",
            notes=note, last_updated_riyadh=ts,
        ))

    rows.append(_make_row(
        keys=keys, section="Criteria", category="Snapshot",
        item="Criteria Summary", symbol="", metric="criteria_summary",
        value=_days_to_horizon(int(crit["invest_period_days"])), signal="", score="",
        notes=_criteria_summary_note(crit), last_updated_riyadh=ts,
    ))
    return rows


# =============================================================================
# Engine integration (preserved from v1.7.0 with minor fixes)
# =============================================================================

async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


async def _fetch_quotes_map(
    engine: Any,
    symbols: List[str],
    *,
    mode: str = "",
    timeout_sec: Optional[float] = None,
) -> Dict[str, Dict[str, Any]]:
    if not engine or not symbols:
        return {}
    requested = _dedupe_keep_order(symbols)

    def _to_symbol_map(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if isinstance(row, Mapping):
                sym = _safe_str(row.get("symbol") or row.get("ticker") or row.get("code"))
                if sym:
                    out[sym] = dict(row)
        return out

    for method_name in ("get_enriched_quotes_batch", "get_enriched_quotes"):
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        try:
            try:
                res = await _await_with_timeout(
                    _maybe_await(fn(requested, mode=mode or "")), timeout_sec, method_name
                )
            except TypeError as exc:
                if not _is_signature_mismatch_typeerror(exc):
                    raise
                res = await _await_with_timeout(_maybe_await(fn(requested)), timeout_sec, method_name)
            if isinstance(res, dict):
                rows_from_payload = _maybe_rows_from_payload(res)
                if rows_from_payload:
                    row_map = _to_symbol_map(rows_from_payload)
                    return {s: row_map.get(s, {"symbol": s}) for s in requested}
                return {s: _extract_row_dict(res.get(s)) if isinstance(res.get(s), dict) else {"symbol": s} for s in requested}
            if isinstance(res, list):
                row_map = _to_symbol_map([_extract_row_dict(x) for x in res])
                return {s: row_map.get(s, {"symbol": s}) for s in requested}
        except Exception:
            continue

    # Per-symbol fallback
    out3: Dict[str, Dict[str, Any]] = {}
    fn3 = getattr(engine, "get_enriched_quote_dict", None)
    fn4 = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    per_sym_timeout = None if timeout_sec is None else max(0.5, timeout_sec / max(1, len(requested)))
    for s in requested:
        try:
            if callable(fn3):
                out3[s] = _extract_row_dict(await _await_with_timeout(_maybe_await(fn3(s)), per_sym_timeout, f"quote:{s}"))
            elif callable(fn4):
                out3[s] = _extract_row_dict(await _await_with_timeout(_maybe_await(fn4(s)), per_sym_timeout, f"quote:{s}"))
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
    timeout_sec: Optional[float] = None,
) -> Dict[str, Any]:
    if not engine:
        return {}
    try:
        from core.analysis.top10_selector import build_top10_rows  # type: ignore
        payload = await _await_with_timeout(
            _maybe_await(build_top10_rows(engine=engine, criteria=criteria or {}, limit=limit, mode=mode or "")),
            timeout_sec, "build_top10_rows",
        )
        if isinstance(payload, dict):
            return payload
        rows = _maybe_rows_from_payload(payload)
        if rows:
            return {"rows": rows}
    except Exception:
        pass

    for name in ("build_top10_rows", "get_top10_rows", "top10_rows", "build_top10", "get_top10_investments", "select_top10"):
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        for kwargs in [
            {"criteria": criteria or {}, "limit": limit, "mode": mode or ""},
            {"criteria": criteria or {}, "limit": limit},
            {"limit": limit},
            {},
        ]:
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


async def _fetch_top10_symbols(
    engine: Any,
    criteria: Optional[Dict[str, Any]] = None,
    *,
    limit: int = 10,
    timeout_sec: Optional[float] = None,
) -> List[str]:
    if not engine:
        return []
    for name in ("get_top10_symbols", "select_top10_symbols", "top10_symbols", "compute_top10"):
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        for kwargs in [{"criteria": criteria or {}, "limit": limit}, {"limit": limit}, {}]:
            try:
                res = await _await_with_timeout(_maybe_await(fn(**kwargs)), timeout_sec, name)
            except TypeError as exc:
                if _is_signature_mismatch_typeerror(exc):
                    continue
                break
            except Exception:
                continue
            if isinstance(res, (list, tuple)):
                syms: List[str] = []
                for item in res:
                    if isinstance(item, str):
                        syms.append(item)
                    else:
                        d = _extract_row_dict(item)
                        syms.append(_safe_str(d.get("symbol") or d.get("ticker")))
                return _dedupe_keep_order(syms)[:limit]
            if isinstance(res, dict):
                rows = _maybe_rows_from_payload(res)
                if rows:
                    return _dedupe_keep_order([_safe_str(r.get("symbol") or r.get("ticker")) for r in rows])[:limit]
    return []


# =============================================================================
# Quote data helpers
# =============================================================================

def _pick(d: Dict[str, Any], key: str) -> Any:
    try:
        return d.get(key)
    except Exception:
        return None


def _scored_list(qmap: Dict[str, Dict[str, Any]], key: str) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for sym, d in qmap.items():
        x = _as_float(_pick(d, key))
        if x is not None:
            out.append((sym, x))
    return out


def _coverage_count(qmap: Dict[str, Dict[str, Any]], key: str) -> int:
    return sum(1 for d in qmap.values() if _pick(d, key) is not None and _safe_str(_pick(d, key)) != "")


# =============================================================================
# Section 1: Market Summary
# =============================================================================

def _build_market_summary_rows(
    *,
    keys: Sequence[str],
    section_name: str,
    symbols: List[str],
    qmap: Dict[str, Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
    """
    Section 1 -- Market Summary.

    One header row per universe summarising:
      - Universe size + coverage
      - Average percent change + trend signal (UP/DOWN/NEUTRAL)
      - Average overall_score + signal (HIGH/MODERATE/LOW)
      - Top gainer and top loser
      - Best expected ROI

    Uses Signal column: UP/DOWN/NEUTRAL for price trend, HIGH/MODERATE/LOW for scores.
    """
    rows: List[Dict[str, Any]] = []

    # Header row
    n = len(symbols)
    covered = _coverage_count(qmap, "current_price")
    rows.append(_make_row(
        keys=keys, section="Market Summary", category=section_name,
        item="Universe", symbol="", metric="universe_size",
        value=n, signal="", score="",
        notes=f"{covered}/{n} symbols have current price data.",
        last_updated_riyadh=ts,
    ))

    # Average percent change -> trend signal
    movers = _scored_list(qmap, "percent_change")
    avg_pct = _avg([x for _, x in movers])
    trend_signal = _signal_from_percent_change(avg_pct)
    rows.append(_make_row(
        keys=keys, section="Market Summary", category=section_name,
        item="Trend", symbol="", metric="avg_percent_change",
        value=_fmt_pct(avg_pct) if avg_pct is not None else "N/A",
        signal=trend_signal, score="",
        notes=f"Average percent change across {len(movers)} symbols with data.",
        last_updated_riyadh=ts,
    ))

    # Top gainer / loser
    if movers:
        movers.sort(key=lambda t: t[1], reverse=True)
        top_sym, top_pc = movers[0]
        low_sym, low_pc = movers[-1]
        rows.append(_make_row(
            keys=keys, section="Market Summary", category=section_name,
            item="Top Gainer", symbol=top_sym, metric="percent_change",
            value=_fmt_pct(top_pc), signal=_SIGNAL_UP, score="",
            notes=f"Highest percent change in {section_name}.",
            last_updated_riyadh=ts,
        ))
        if len(movers) > 1:
            rows.append(_make_row(
                keys=keys, section="Market Summary", category=section_name,
                item="Top Loser", symbol=low_sym, metric="percent_change",
                value=_fmt_pct(low_pc), signal=_SIGNAL_DOWN, score="",
                notes=f"Lowest percent change in {section_name}.",
                last_updated_riyadh=ts,
            ))

    # Average overall_score -> quality signal
    scores = _scored_list(qmap, "overall_score")
    avg_score = _avg([x for _, x in scores])
    if avg_score is not None:
        score_signal = _signal_from_score(avg_score)
        rows.append(_make_row(
            keys=keys, section="Market Summary", category=section_name,
            item="Avg Overall Score", symbol="", metric="avg_overall_score",
            value=round(avg_score, 1), signal=score_signal, score=round(avg_score, 1),
            notes=f"Average overall_score across {len(scores)} scored symbols.",
            last_updated_riyadh=ts,
        ))

    # Best expected ROI
    roi3 = _scored_list(qmap, "expected_roi_3m")
    if roi3:
        roi3.sort(key=lambda t: t[1], reverse=True)
        best_sym, best_roi = roi3[0]
        rows.append(_make_row(
            keys=keys, section="Market Summary", category=section_name,
            item="Best ROI (3M)", symbol=best_sym, metric="expected_roi_3m",
            value=_fmt_pct(best_roi), signal=_SIGNAL_UP if (best_roi or 0) > 0 else _SIGNAL_DOWN, score="",
            notes=f"Highest expected_roi_3m in {section_name}.",
            last_updated_riyadh=ts,
        ))

    return rows


# =============================================================================
# Section 2: Risk Scenarios
# =============================================================================

def _build_risk_scenario_rows(
    *,
    keys: Sequence[str],
    norm_criteria: Dict[str, Any],
    ts: str,
) -> List[Dict[str, Any]]:
    """
    Section 2 -- Risk Scenarios.

    3 rows: Conservative / Moderate / Aggressive.
    Uses criteria_model.build_scenario_specs() for consistent parameters.
    Signal: LOW / MODERATE / HIGH per scenario.
    Score: expected return as a score (0-100).
    """
    rows: List[Dict[str, Any]] = []

    # Attempt to use criteria_model for typed scenario generation
    scenarios_data: List[Dict[str, Any]] = []
    try:
        from core.analysis.criteria_model import AdvisorCriteria, build_scenario_specs  # type: ignore
        criteria_obj = AdvisorCriteria.from_kv_map(norm_criteria)
        specs = build_scenario_specs(criteria_obj)
        for spec in specs:
            scenarios_data.append({
                "label":          spec.label,
                "signal":         spec.signal,
                "max_risk":       spec.max_risk,
                "min_roi":        spec.min_roi,
                "required_return": spec.required_return,
                "min_confidence": spec.min_confidence,
                "horizon":        spec.horizon,
                "notes":          spec.notes,
            })
    except Exception:
        # Hardcoded fallback aligned with criteria_model.to_scenario_variants()
        horizon = _days_to_horizon(int(norm_criteria.get("invest_period_days", 90)))
        scenarios_data = [
            {
                "label": "Conservative", "signal": _SIGNAL_LOW, "max_risk": 40.0,
                "min_roi": 0.03, "required_return": 0.05, "min_confidence": 0.70,
                "horizon": horizon,
                "notes": "Risk ceiling: 40 | Min ROI: 3.0% | Min Confidence: 70% | Horizon: " + horizon,
            },
            {
                "label": "Moderate", "signal": _SIGNAL_MODERATE, "max_risk": 60.0,
                "min_roi": 0.07, "required_return": 0.10, "min_confidence": 0.60,
                "horizon": horizon,
                "notes": "Risk ceiling: 60 | Min ROI: 7.0% | Min Confidence: 60% | Horizon: " + horizon,
            },
            {
                "label": "Aggressive", "signal": _SIGNAL_HIGH, "max_risk": 80.0,
                "min_roi": 0.15, "required_return": 0.20, "min_confidence": 0.45,
                "horizon": horizon,
                "notes": "Risk ceiling: 80 | Min ROI: 15.0% | Min Confidence: 45% | Horizon: " + horizon,
            },
        ]

    # Header row
    rows.append(_make_row(
        keys=keys, section="Risk Scenarios", category="Overview",
        item="Scenarios", symbol="", metric="scenario_count",
        value=len(scenarios_data), signal="", score="",
        notes="Three risk profiles based on your criteria. Choose the scenario that matches your tolerance.",
        last_updated_riyadh=ts,
    ))

    for s in scenarios_data:
        label         = s.get("label", "Scenario")
        signal        = s.get("signal", _SIGNAL_MODERATE)
        max_risk      = s.get("max_risk", 60.0)
        min_roi       = s.get("min_roi", 0.0)
        req_return    = s.get("required_return", 0.10)
        min_conf      = s.get("min_confidence", 0.60)
        horizon       = s.get("horizon", "3M")
        notes         = s.get("notes", "")
        # Score: map required_return to 0-100 (0% return = 0, 20%+ = 100)
        return_score  = round(min(100.0, max(0.0, float(req_return) * 500.0)), 1)

        rows.append(_make_row(
            keys=keys, section="Risk Scenarios", category=label,
            item=label, symbol="", metric="scenario_profile",
            value=f"Return>={_fmt_pct(req_return)} | Risk<={max_risk:.0f} | Conf>={_fmt_pct(min_conf)}",
            signal=signal, score=return_score,
            notes=notes or (
                f"Max risk: {max_risk:.0f} | "
                f"Required return: {_fmt_pct(req_return)} | "
                f"Min confidence: {_fmt_pct(min_conf)} | "
                f"Horizon: {horizon}"
            ),
            last_updated_riyadh=ts,
        ))

    return rows


# =============================================================================
# Section 3: Top Opportunities
# =============================================================================

def _build_top_opportunities_rows(
    *,
    keys: Sequence[str],
    top10_payload: Dict[str, Any],
    norm_criteria: Dict[str, Any],
    ts: str,
) -> List[Dict[str, Any]]:
    """
    Section 3 -- Top Opportunities.

    Populates Score (overall_score) and Signal (from recommendation).
    """
    rows: List[Dict[str, Any]] = []
    top_rows = _maybe_rows_from_payload(top10_payload)

    if not top_rows:
        rows.append(_make_row(
            keys=keys, section="Top Opportunities", category="Status",
            item="Status", symbol="", metric="top10_status",
            value="Unavailable", signal=_SIGNAL_WARN, score="",
            notes="Top10 payload is empty or engine unavailable.",
            last_updated_riyadh=ts,
        ))
        return rows

    days = int(norm_criteria.get("invest_period_days", 90))
    roi_key = "expected_roi_1m" if days <= 45 else ("expected_roi_3m" if days <= 135 else "expected_roi_12m")

    rows.append(_make_row(
        keys=keys, section="Top Opportunities", category="Summary",
        item="Count", symbol="", metric="top10_count",
        value=len(top_rows), signal="", score="",
        notes=f"Top {len(top_rows)} selected by criteria. Horizon={_days_to_horizon(days)}.",
        last_updated_riyadh=ts,
    ))

    for i, raw in enumerate(top_rows[:10], start=1):
        if not isinstance(raw, dict):
            continue
        sym        = _safe_str(raw.get("symbol") or raw.get("ticker"))
        name       = _safe_str(raw.get("name") or "")
        rank       = _as_int(raw.get("top10_rank")) or i
        roi_val    = raw.get(roi_key)
        reco       = _safe_str(raw.get("recommendation") or "")
        sel_reason = _safe_str(raw.get("selection_reason") or raw.get("recommendation_reason") or "")
        conf       = raw.get("forecast_confidence")
        overall    = _as_float(raw.get("overall_score"))
        risk_bkt   = _safe_str(raw.get("risk_bucket") or "")

        # Signal from recommendation
        sig = _signal_from_reco(reco) or _signal_from_score(overall)

        note_parts: List[str] = []
        if name:
            note_parts.append(name)
        if reco:
            note_parts.append(f"reco={reco}")
        if conf is not None:
            note_parts.append(f"conf={_fmt_pct(conf)}")
        if risk_bkt:
            note_parts.append(f"risk={risk_bkt}")
        if sel_reason:
            note_parts.append(sel_reason)

        rows.append(_make_row(
            keys=keys, section="Top Opportunities", category=f"Rank {rank}",
            item=f"#{rank} {sym}", symbol=sym, metric=roi_key,
            value=_fmt_pct(roi_val) if roi_val is not None else "",
            signal=sig, score=round(overall, 1) if overall is not None else "",
            notes=" | ".join(note_parts) if note_parts else "Top10 ranked item.",
            last_updated_riyadh=ts,
        ))

    return rows


# =============================================================================
# Section 4: Portfolio Health
# =============================================================================

def _build_portfolio_health_rows(
    *,
    keys: Sequence[str],
    symbols: List[str],
    qmap: Dict[str, Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
    """
    Section 4 -- Portfolio Health.

    Reads from My_Portfolio's dedicated fields (portfolio_table schema v3.0.0):
      position_qty, avg_cost, current_price -> position_cost, position_value, unrealized_pl
      day_pl -> today's movement
      risk_score -> at-risk count

    Signal: OK (positive P/L), WARN (small negative), ALERT (large negative).
    Score: portfolio health score based on P/L % and at-risk count.
    """
    rows: List[Dict[str, Any]] = []

    total_cost = 0.0
    total_value = 0.0
    total_day_pl = 0.0
    at_risk_count = 0
    position_count = 0
    have_positions = False

    for sym in symbols:
        d = qmap.get(sym) or {}
        qty     = _as_float(_pick(d, "position_qty"))
        avg_c   = _as_float(_pick(d, "avg_cost"))
        px      = _as_float(_pick(d, "current_price"))
        day_pl  = _as_float(_pick(d, "day_pl"))
        risk_sc = _as_float(_pick(d, "risk_score"))

        if qty is None or avg_c is None or qty == 0:
            continue

        have_positions = True
        position_count += 1
        cost  = float(qty) * float(avg_c)
        total_cost += cost
        if px is not None:
            total_value += float(qty) * float(px)
        if day_pl is not None:
            total_day_pl += float(day_pl)
        if risk_sc is not None and risk_sc > 60.0:
            at_risk_count += 1

    if not have_positions:
        rows.append(_make_row(
            keys=keys, section="Portfolio Health", category="Status",
            item="Portfolio", symbol="", metric="status",
            value="No Positions", signal=_SIGNAL_NEUTRAL, score="",
            notes="No position_qty / avg_cost found. Enter your holdings in My_Portfolio.",
            last_updated_riyadh=ts,
        ))
        return rows

    unrealized_pl = total_value - total_cost
    unreal_pct    = (unrealized_pl / total_cost) if total_cost > 0 else None
    pl_signal     = _signal_from_pl_pct(unreal_pct)

    # Health score: 100 = all positive, decreases with negative P/L and at-risk positions
    health_score = 60.0
    if unreal_pct is not None:
        pct_pts = _pct_points(unreal_pct) or 0.0
        health_score += min(30.0, max(-30.0, pct_pts))
    if position_count > 0:
        at_risk_ratio = at_risk_count / position_count
        health_score -= at_risk_ratio * 20.0
    health_score = round(min(100.0, max(0.0, health_score)), 1)

    rows.append(_make_row(
        keys=keys, section="Portfolio Health", category="Summary",
        item="Positions", symbol="", metric="position_count",
        value=position_count, signal="", score=health_score,
        notes=f"{at_risk_count} position(s) with risk_score > 60.",
        last_updated_riyadh=ts,
    ))
    rows.append(_make_row(
        keys=keys, section="Portfolio Health", category="Cost & Value",
        item="Total Cost", symbol="", metric="total_cost",
        value=round(total_cost, 2), signal="", score="",
        notes="Sum(position_qty * avg_cost).",
        last_updated_riyadh=ts,
    ))
    rows.append(_make_row(
        keys=keys, section="Portfolio Health", category="Cost & Value",
        item="Total Value", symbol="", metric="total_value",
        value=round(total_value, 2), signal="", score="",
        notes="Sum(position_qty * current_price).",
        last_updated_riyadh=ts,
    ))
    rows.append(_make_row(
        keys=keys, section="Portfolio Health", category="P/L",
        item="Unrealized P/L", symbol="", metric="unrealized_pl",
        value=round(unrealized_pl, 2), signal=pl_signal, score=health_score,
        notes="total_value - total_cost.",
        last_updated_riyadh=ts,
    ))
    if unreal_pct is not None:
        rows.append(_make_row(
            keys=keys, section="Portfolio Health", category="P/L",
            item="Unrealized P/L %", symbol="", metric="unrealized_pl_pct",
            value=_fmt_pct(unreal_pct), signal=pl_signal, score="",
            notes=f"unrealized_pl / total_cost. Signal: {pl_signal}.",
            last_updated_riyadh=ts,
        ))
    if total_day_pl != 0.0:
        day_signal = _SIGNAL_UP if total_day_pl > 0 else _SIGNAL_DOWN
        rows.append(_make_row(
            keys=keys, section="Portfolio Health", category="P/L",
            item="Today's P/L", symbol="", metric="day_pl",
            value=round(total_day_pl, 2), signal=day_signal, score="",
            notes="Sum(day_pl) from My_Portfolio positions.",
            last_updated_riyadh=ts,
        ))
    if at_risk_count > 0:
        rows.append(_make_row(
            keys=keys, section="Portfolio Health", category="Risk",
            item="At-Risk Positions", symbol="", metric="at_risk_count",
            value=at_risk_count, signal=_SIGNAL_WARN if at_risk_count < position_count else _SIGNAL_ALERT,
            score="",
            notes=f"{at_risk_count}/{position_count} positions have risk_score > 60. Review or reduce.",
            last_updated_riyadh=ts,
        ))

    return rows


# =============================================================================
# Auto-universe defaults
# =============================================================================

def _default_universes() -> Dict[str, List[str]]:
    indices       = _env_csv("TFB_INSIGHTS_INDICES",       "TASI,NOMU,^GSPC,^IXIC,^FTSE")
    commodities_fx = _env_csv("TFB_INSIGHTS_COMMODITIES_FX","GC=F,BZ=F,USDSAR=X,EURUSD=X")
    return {"Indices & Benchmarks": indices, "Commodities & FX": commodities_fx}


# =============================================================================
# Main builder (executive 4-section layout)
# =============================================================================

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
    Build the Insights_Analysis page rows (10-column executive layout).

    Generates 4 sections:
      1. Market Summary    -- trend signals per universe (include_market_summary)
      2. Risk Scenarios    -- Conservative/Moderate/Aggressive (include_risk_scenarios)
      3. Top Opportunities -- Top 10 with score + signal (include_top10_section)
      4. Portfolio Health  -- P/L KPIs with OK/WARN/ALERT (include_portfolio_kpis)

    Section flags are read from both function parameters and criteria dict.
    Criteria dict flags take precedence if explicitly set.
    """
    headers, keys, schema_source = get_insights_schema()
    ts = _now_riyadh_iso()
    norm_criteria = _normalize_criteria_input(criteria)
    warnings: List[str] = []
    rows: List[Dict[str, Any]] = []

    # Merge section flags: function params OR criteria dict
    do_market_summary    = include_top10_section and norm_criteria.get("include_market_summary", True)
    do_risk_scenarios    = norm_criteria.get("include_risk_scenarios", True)
    do_top_opportunities = include_top10_section and norm_criteria.get("include_top_opportunities", True)
    do_portfolio_health  = include_portfolio_kpis and norm_criteria.get("include_portfolio_health", True)

    deadline = asyncio.get_running_loop().time() + max(1.0, float(build_budget_sec))

    # --- Criteria block ---
    if include_criteria_rows:
        rows.extend(build_criteria_rows(criteria=norm_criteria, last_updated_riyadh=ts))

    # --- System rows ---
    if include_system_rows:
        schema_version = ""
        try:
            from core.sheets.schema_registry import SCHEMA_VERSION as _SV  # type: ignore
            schema_version = _safe_str(_SV)
        except Exception:
            pass
        rows.append(_make_row(
            keys=keys, section="System", category="Version",
            item="Builder Version", symbol="", metric="insights_builder_version",
            value=INSIGHTS_BUILDER_VERSION, signal="", score="",
            notes="core/analysis/insights_builder.py",
            last_updated_riyadh=ts,
        ))
        if schema_version:
            rows.append(_make_row(
                keys=keys, section="System", category="Version",
                item="Schema Version", symbol="", metric="schema_version",
                value=schema_version, signal="", score="",
                notes="core/sheets/schema_registry.py",
                last_updated_riyadh=ts,
            ))

    # --- Resolve universes ---
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
        keys=keys, section="System", category="Status",
        item="Build Status", symbol="", metric="build_status",
        value="OK" if build_ok else "WARN",
        signal=_SIGNAL_OK if build_ok else _SIGNAL_WARN, score="",
        notes="OK = engine + universes available. WARN = criteria/system only.",
        last_updated_riyadh=ts,
    ))

    # Early exit if engine or universes unavailable
    if not engine or not eff_universes:
        msg = "No engine passed." if not engine else "No universes/symbols provided."
        rows.append(_make_row(
            keys=keys, section="System", category="Status",
            item="Engine", symbol="", metric="engine_status",
            value="Not provided" if not engine else "Universes Empty",
            signal=_SIGNAL_WARN, score="",
            notes=msg + " Returning criteria/system rows only.",
            last_updated_riyadh=ts,
        ))
        # Still generate Risk Scenarios even without engine
        if do_risk_scenarios:
            rows.extend(_build_risk_scenario_rows(keys=keys, norm_criteria=norm_criteria, ts=ts))
        return _wrap_result(headers=headers, keys=keys, rows=rows, schema_source=schema_source,
                            ts=ts, engine_used=bool(engine), auto_used=auto_used,
                            eff_universes=eff_universes, mode=mode, norm_criteria=norm_criteria,
                            warnings=warnings, quotes_timeout_sec=quotes_timeout_sec,
                            top10_timeout_sec=top10_timeout_sec, build_budget_sec=build_budget_sec)

    cap = max(1, min(int(max_symbols_per_universe), 500))

    # ===========================================================================
    # Section 1: Market Summary (one block per universe)
    # ===========================================================================
    portfolio_qmap: Dict[str, Dict[str, Any]] = {}
    portfolio_symbols: List[str] = []

    for section_name, sym_list in eff_universes.items():
        remaining = _remaining_budget(deadline)
        if remaining is not None and remaining <= 0.10:
            warnings.append(f"Skipped '{section_name}' -- build budget exhausted.")
            rows.append(_warning_row(keys, "build_budget_exhausted", f"Skipped '{section_name}' -- budget.", ts))
            break

        syms = _dedupe_keep_order(sym_list or [])[:cap]
        try:
            qmap = await _fetch_quotes_map(engine, syms, mode=mode or "",
                                            timeout_sec=min(quotes_timeout_sec, remaining or quotes_timeout_sec))
        except Exception as exc:
            qmap = {}
            warnings.append(f"Quote fetch degraded for '{section_name}': {exc}")
            rows.append(_warning_row(keys, "quote_fetch_degraded", f"'{section_name}': {exc}", ts))

        if do_market_summary:
            rows.extend(_build_market_summary_rows(
                keys=keys, section_name=section_name, symbols=syms, qmap=qmap, ts=ts,
            ))

        # Capture portfolio data for Section 4
        if section_name.strip().lower() in {"my_portfolio", "portfolio", "my portfolio", "selected symbols"}:
            portfolio_qmap = qmap
            portfolio_symbols = syms

    # ===========================================================================
    # Section 2: Risk Scenarios
    # ===========================================================================
    if do_risk_scenarios:
        rows.extend(_build_risk_scenario_rows(keys=keys, norm_criteria=norm_criteria, ts=ts))

    # ===========================================================================
    # Section 3: Top Opportunities
    # ===========================================================================
    if do_top_opportunities:
        remaining = _remaining_budget(deadline)
        if remaining is not None and remaining <= 0.25:
            warnings.append("Skipped Top Opportunities -- build budget too small.")
            rows.append(_warning_row(keys, "top10_skipped", "Skipped Top Opportunities -- budget.", ts))
        else:
            top10_payload: Dict[str, Any] = {}
            try:
                top10_payload = await _fetch_top10_payload(
                    engine, criteria=norm_criteria, limit=norm_criteria.get("top_n", 10),
                    mode=mode or "", timeout_sec=min(top10_timeout_sec, remaining or top10_timeout_sec),
                )
            except Exception as exc:
                warnings.append(f"Top10 payload degraded: {exc}")
                rows.append(_warning_row(keys, "top10_payload_degraded", f"Top10 degraded: {exc}", ts))

            if top10_payload:
                rows.extend(_build_top_opportunities_rows(
                    keys=keys, top10_payload=top10_payload, norm_criteria=norm_criteria, ts=ts,
                ))
            else:
                # Fallback: symbol-only top10 with quote enrichment
                try:
                    top10_syms = await _fetch_top10_symbols(
                        engine, criteria=norm_criteria, limit=norm_criteria.get("top_n", 10),
                        timeout_sec=min(top10_timeout_sec, _remaining_budget(deadline) or top10_timeout_sec),
                    )
                except Exception as exc:
                    top10_syms = []
                    warnings.append(f"Top10 symbols degraded: {exc}")

                if top10_syms:
                    try:
                        qmap10 = await _fetch_quotes_map(
                            engine, top10_syms, mode=mode or "",
                            timeout_sec=min(quotes_timeout_sec, _remaining_budget(deadline) or quotes_timeout_sec),
                        )
                    except Exception as exc:
                        qmap10 = {}
                        warnings.append(f"Top10 quote enrichment degraded: {exc}")

                    rows.extend(_build_top_opportunities_rows(
                        keys=keys, top10_payload={"rows": [
                            {**qmap10.get(s, {"symbol": s}), "symbol": s, "top10_rank": i}
                            for i, s in enumerate(top10_syms, 1)
                        ]}, norm_criteria=norm_criteria, ts=ts,
                    ))
                else:
                    rows.append(_make_row(
                        keys=keys, section="Top Opportunities", category="Status",
                        item="Status", symbol="", metric="top10_status",
                        value="Unavailable", signal=_SIGNAL_WARN, score="",
                        notes="No Top10 method found or returned empty.",
                        last_updated_riyadh=ts,
                    ))

    # ===========================================================================
    # Section 4: Portfolio Health
    # ===========================================================================
    if do_portfolio_health:
        if portfolio_symbols and portfolio_qmap:
            rows.extend(_build_portfolio_health_rows(
                keys=keys, symbols=portfolio_symbols, qmap=portfolio_qmap, ts=ts,
            ))
        else:
            # Try to fetch My_Portfolio specifically if not in universes
            remaining = _remaining_budget(deadline)
            if remaining and remaining > 0.5:
                portfolio_universe = eff_universes.get("My_Portfolio") or eff_universes.get("my_portfolio")
                if not portfolio_universe:
                    rows.append(_make_row(
                        keys=keys, section="Portfolio Health", category="Status",
                        item="Status", symbol="", metric="portfolio_status",
                        value="Not Included", signal=_SIGNAL_NEUTRAL, score="",
                        notes="My_Portfolio not in universes. Add My_Portfolio to pages_selected criteria.",
                        last_updated_riyadh=ts,
                    ))
            else:
                rows.append(_warning_row(keys, "portfolio_health_skipped", "Portfolio Health skipped -- budget.", ts))

    # Prepend warning summary if any
    if warnings:
        rows.insert(0, _warning_row(keys, "builder_warnings", " | ".join(warnings[:3]), ts))

    return _wrap_result(
        headers=headers, keys=keys, rows=rows, schema_source=schema_source,
        ts=ts, engine_used=True, auto_used=auto_used,
        eff_universes=eff_universes, mode=mode, norm_criteria=norm_criteria,
        warnings=warnings, quotes_timeout_sec=quotes_timeout_sec,
        top10_timeout_sec=top10_timeout_sec, build_budget_sec=build_budget_sec,
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
    quotes_timeout_sec: float,
    top10_timeout_sec: float,
    build_budget_sec: float,
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
            "criteria_snapshot": _compact_json(norm_criteria),
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
