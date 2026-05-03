#!/usr/bin/env python3
"""
core/analysis/insights_builder.py
================================================================================
Insights Analysis Builder -- v6.0.0
(SCHEMA-CORRECT / TIMESTAMP-FIX / SIGNAL-PRIORITY-PRESERVED / V5-V7 ALIGNED)
================================================================================
Tadawul Fast Bridge (TFB)

Purpose
-------
Builds Insights_Analysis page rows for the TFB dashboard. Generates a
multi-section executive summary aligned with the canonical 7-column
Insights_Analysis schema in `core.sheets.schema_registry` v2.5.0.

Sections
--------
  1. Market Summary             -- Trend signals per universe (gainers/losers/vol)
  2. Top Picks                  -- Top 10 investments with signals and priorities
  3. Risk Alerts                -- High risk_score, overbought RSI, drawdowns
  4. Short-Term Opportunities   -- technical_score + short_term_signal
  5. Portfolio KPIs             -- P/L, stop distances, weight deviations
  6. Macro Signals              -- sector signals, vs_sp500_ytd
  7. Risk Scenarios             -- Conservative / Moderate / Aggressive

Aligned with the v5/v7 view-aware family:
  - core.sheets.schema_registry v2.5.0  (Insights_Analysis = 7 columns)
  - core.scoring                v5.0.0
  - core.reco_normalize         v7.0.0  (5-tier rec vocabulary)
  - core.analysis.criteria_model v3.0.0

================================================================================
v6.0.0 Changes (from v5.0.0)
================================================================================

CRITICAL FIX: 7-column schema alignment
---------------------------------------
v5.0.0's `_FALLBACK_HEADERS` / `_FALLBACK_KEYS` listed 9 columns including
`signal` and `priority` — but the canonical registry contract for
Insights_Analysis is 7 columns:

    ["section", "item", "symbol", "metric", "value", "notes",
     "last_updated_riyadh"]

When `get_insights_schema()` reads from the registry (the normal path),
the projection in `_make_row()` returned 7 columns from the registry's
keys list. The `signal` / `priority` values built by every section
builder were silently dropped before reaching the sheet.

v6.0.0:
  - `_FALLBACK_HEADERS` and `_FALLBACK_KEYS` are now exactly 7 columns,
    matching the registry verbatim.
  - When the schema does NOT include `signal` / `priority` columns,
    those values are embedded into the `notes` field as a `[SIGNAL|PRIORITY]`
    prefix so the information is preserved end-to-end. No more silent
    data loss.
  - When the schema DOES include those columns (legacy or future
    extension), they go into their own columns as before.

CRITICAL FIX: timestamp key was being lost
------------------------------------------
v5.0.0's `_make_row()` built rows with `"as_of_riyadh": timestamp`, but
the registry's canonical key is `"last_updated_riyadh"`. The schema
projection step used `full_row.get("last_updated_riyadh", "")` — which
returned `""` because the timestamp lived under `as_of_riyadh` instead.
Every Insights row in production was emitting a blank timestamp.

v6.0.0:
  - `_make_row()` now writes the timestamp under BOTH `last_updated_riyadh`
    (the registry-canonical key) AND `as_of_riyadh` (legacy alias) in
    `full_row`. Whichever one the schema asks for, it gets a value.
  - `_make_row()` now also accepts a `last_updated_riyadh` keyword argument
    in addition to the legacy `as_of_riyadh`. Both resolve to the same
    timestamp; either is fine.

NEW: 5-tier recommendation token recognition
--------------------------------------------
The Top Picks section's `_SIGNAL_UP` mapping now also recognises the
v7.0.0 5-tier vocabulary (`STRONG_BUY`, `BUY` keep BUY signal; `SELL`
and `REDUCE` map to SELL signal; `HOLD` maps to neutral). Previously
only `BUY`/`STRONG_BUY` were detected.

================================================================================
v5.0.0 Changes (PRESERVED)
================================================================================

  - SyntaxError fix: `_build_top_picks_rows` no longer uses `await` inside
    a non-async def. Pure helper; caller fetches quotes.
  - Horizon thresholds unified at 45 / 120 days (criteria_model alignment).
  - Signal mapping delegates to `criteria_model.signal_for_value` where
    possible.
  - Risk Scenarios uses `criteria_model.build_scenario_specs` (real, not
    "Placeholder" stub).
  - Timeouts (`quotes_timeout_sec` / `top10_timeout_sec`) are actually
    enforced via `asyncio.wait_for` (in v4.0.0 they were ignored).
  - Removed dead types (RowData, Signal/Priority/BuildStatus enums) and
    unused imports.
  - Consolidated engine method-dispatch retry loops into
    `_invoke_engine_method`.

Design Rules
------------
  - No network calls at import time
  - Best-effort engine access only; never raises for normal route execution
  - Returns schema-correct rows even when the engine is missing
  - Thread-safe where applicable
  - Fully type-hinted
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INSIGHTS_BUILDER_VERSION = "6.0.0"

# Riyadh timezone (UTC+3, no DST)
_RIYADH_TZ = timezone(timedelta(hours=3))

# Schema fallback: exactly 7 columns matching schema_registry v2.5.0's
# Insights_Analysis spec. Do NOT add `signal` or `priority` columns here —
# the registry treats those as info to be embedded in `notes` (handled by
# `_make_row`). Do NOT rename `last_updated_riyadh` to `as_of_riyadh`; the
# registry uses the former and sheet writers depend on it.
_FALLBACK_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric",
    "Value", "Notes", "Last Updated (Riyadh)",
]
_FALLBACK_KEYS: List[str] = [
    "section", "item", "symbol", "metric",
    "value", "notes", "last_updated_riyadh",
]

# Signal vocabulary used as `notes` markers (NOT as separate columns).
# When the schema gains explicit signal/priority columns, _make_row routes
# values to those columns instead.
_SIGNAL_UP = "BUY"
_SIGNAL_DOWN = "SELL"
_SIGNAL_NEUTRAL = "HOLD"
_SIGNAL_OK = "INFO"
_SIGNAL_WARN = "ALERT"
_SIGNAL_ALERT = "ALERT"

# v6.0.0: 5-tier recommendation tokens that map to bullish/bearish signals
_BULLISH_RECOS = frozenset({"BUY", "STRONG_BUY"})
_BEARISH_RECOS = frozenset({"SELL", "REDUCE"})

# Priority vocabulary
_PRI_HIGH = "High"
_PRI_MEDIUM = "Medium"
_PRI_LOW = "Low"

# Default limits
_DEFAULT_MAX_SYMBOLS_PER_UNIVERSE = 100
_DEFAULT_QUOTES_TIMEOUT_SEC = 10.0
_DEFAULT_TOP10_TIMEOUT_SEC = 10.0
_DEFAULT_BUILD_BUDGET_SEC = 30.0


# ---------------------------------------------------------------------------
# Internal data container
# ---------------------------------------------------------------------------

@dataclass
class BuildContext:
    """Context object passed between section builders."""
    keys: List[str]
    ts: str
    norm_criteria: Dict[str, Any]
    warnings: List[str] = field(default_factory=list)
    all_quotes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    portfolio_quotes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    portfolio_symbols: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pure Utility Functions
# ---------------------------------------------------------------------------

def _safe_str(value: Any) -> str:
    """Safely convert any value to a stripped string; None -> ''."""
    if value is None:
        return ""
    try:
        return str(value).strip()
    except Exception:
        return ""


def _now_riyadh_iso() -> str:
    """Current Riyadh time (UTC+3) in ISO format."""
    return datetime.now(_RIYADH_TZ).isoformat()


def _as_float(value: Any) -> Optional[float]:
    """Safely convert to float; returns None for unparseable input or NaN."""
    if value is None or isinstance(value, bool):
        return None
    try:
        s = _safe_str(value).replace(",", "")
        if not s:
            return None
        if s.endswith("%"):
            s = s[:-1].strip()
        result = float(s)
        return None if result != result else result  # NaN check
    except (ValueError, TypeError):
        return None


def _as_int(value: Any) -> Optional[int]:
    """Safely convert to int; returns None if not possible."""
    f = _as_float(value)
    return int(f) if f is not None else None


def _as_ratio(value: Any) -> Optional[float]:
    """Convert percent-like value to ratio (0.12 = 12%)."""
    f = _as_float(value)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _as_percent_points(value: Any) -> Optional[float]:
    """Convert value to percent points (0.12 -> 12.0, 12 -> 12.0)."""
    f = _as_float(value)
    if f is None:
        return None
    if abs(f) <= 1.5:
        return f * 100.0
    return f


def _format_percent(value: Any) -> str:
    """Format value as "X.XX%"."""
    p = _as_percent_points(value)
    return f"{p:.2f}%" if p is not None else ""


def _format_number(value: Any) -> str:
    """Format value as "X.XX"."""
    f = _as_float(value)
    return f"{f:.2f}" if f is not None else _safe_str(value)


def _to_bool(value: Any, default: bool = False) -> bool:
    """Convert various representations to boolean."""
    if isinstance(value, bool):
        return value
    s = _safe_str(value).lower()
    if s in {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f", "disabled", "disable"}:
        return False
    return default


def _clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp a value between min and max."""
    return max(min_val, min(value, max_val))


def _dedupe_keep_order(items: Sequence[Any]) -> List[str]:
    """Deduplicate items while preserving first-seen order; stringifies."""
    seen: set = set()
    result: List[str] = []
    for item in items:
        s = _safe_str(item)
        if s and s not in seen:
            seen.add(s)
            result.append(s)
    return result


def _split_csv(raw: str) -> List[str]:
    """Split CSV (accepting newlines) into deduped, non-empty strings."""
    result: List[str] = []
    for part in (raw or "").replace("\n", ",").split(","):
        s = part.strip()
        if s:
            result.append(s)
    return _dedupe_keep_order(result)


def _env_csv(name: str, default: str) -> List[str]:
    """Read a CSV-style environment variable."""
    return _split_csv(os.getenv(name, default))


def _compact_json(obj: Any) -> str:
    """Compact JSON dump; falls back to str() on failure."""
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return _safe_str(obj)


def _is_probable_symbol(value: Any) -> bool:
    """Heuristic: does `value` look like a ticker / symbol?"""
    s = _safe_str(value)
    if not s or len(s) > 32:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._=^-:")
    return all(ch in allowed for ch in s)


def _is_signature_mismatch(error: TypeError) -> bool:
    """Check whether a TypeError was raised due to a function-signature mismatch."""
    msg = _safe_str(error).lower()
    markers = (
        "unexpected keyword", "positional argument", "required positional",
        "takes no keyword", "takes from", "takes exactly",
        "got an unexpected keyword", "got multiple values",
        "missing 1 required positional", "missing required positional",
        "keyword-only argument",
    )
    return any(marker in msg for marker in markers)


# ---------------------------------------------------------------------------
# Payload Extraction Helpers
# ---------------------------------------------------------------------------

def _extract_row_dict(obj: Any) -> Dict[str, Any]:
    """Extract a plain dict from dict / pydantic v1 or v2 / dataclass / object."""
    if isinstance(obj, dict):
        return dict(obj)

    # Pydantic v2
    try:
        if hasattr(obj, "model_dump") and callable(obj.model_dump):
            result = obj.model_dump(mode="python")
            if isinstance(result, dict):
                return result
    except Exception:
        pass

    # Pydantic v1
    try:
        if hasattr(obj, "dict") and callable(obj.dict):
            result = obj.dict()
            if isinstance(result, dict):
                return result
    except Exception:
        pass

    # Dataclass or simple object with __dict__
    try:
        result = getattr(obj, "__dict__", None)
        if isinstance(result, dict):
            return dict(result)
    except Exception:
        pass

    return {}


def _rows_from_matrix(matrix: Any, columns: Sequence[Any]) -> List[Dict[str, Any]]:
    """Convert a list-of-lists matrix into list-of-dicts using provided headers."""
    keys = [_safe_str(c) for c in columns if _safe_str(c)]
    if not keys or not isinstance(matrix, (list, tuple)):
        return []

    result: List[Dict[str, Any]] = []
    for row in matrix:
        if not isinstance(row, (list, tuple)):
            continue
        row_dict: Dict[str, Any] = {}
        for i, key in enumerate(keys):
            if i < len(row):
                row_dict[key] = row[i]
        result.append(row_dict)
    return result


def _coerce_dict_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract rows from a dict payload -- split out from _coerce_to_rows for clarity."""
    # Check for symbol map: {"AAPL": {...}, "MSFT": {...}}
    if payload:
        is_symbol_map = True
        symbol_rows: List[Dict[str, Any]] = []
        for key, value in payload.items():
            if not isinstance(value, dict) or not _is_probable_symbol(key):
                is_symbol_map = False
                break
            row = dict(value)
            row.setdefault("symbol", _safe_str(key))
            symbol_rows.append(row)
        if is_symbol_map and symbol_rows:
            return symbol_rows

    # Common container keys
    for key in ("row_objects", "rowObjects", "records", "items", "data", "quotes", "rows", "results"):
        value = payload.get(key)
        if not isinstance(value, list) or not value:
            continue
        if isinstance(value[0], dict):
            return [dict(row) for row in value if isinstance(row, dict)]
        if isinstance(value[0], (list, tuple)):
            cols = payload.get("keys") or payload.get("headers") or payload.get("columns") or []
            if isinstance(cols, list) and cols:
                matrix_rows = _rows_from_matrix(value, cols)
                if matrix_rows:
                    return matrix_rows

    # Direct matrix
    matrix = payload.get("rows_matrix") or payload.get("matrix")
    if isinstance(matrix, list):
        cols = payload.get("keys") or payload.get("headers") or payload.get("columns") or []
        if isinstance(cols, list) and cols:
            matrix_rows = _rows_from_matrix(matrix, cols)
            if matrix_rows:
                return matrix_rows

    # Single row dict?
    single = _extract_row_dict(payload)
    if single and any(k in single for k in ("section", "item", "symbol", "metric")):
        return [single]

    # Recurse into common nesting
    for key in ("result", "payload", "response", "output"):
        nested = payload.get(key)
        nested_rows = _coerce_to_rows(nested)
        if nested_rows:
            return nested_rows

    return []


def _coerce_to_rows(payload: Any) -> List[Dict[str, Any]]:
    """Coerce various payload shapes into a list of row dicts."""
    if payload is None:
        return []

    if isinstance(payload, list):
        if not payload:
            return []
        if isinstance(payload[0], dict):
            return [dict(r) for r in payload if isinstance(r, dict)]
        result: List[Dict[str, Any]] = []
        for item in payload:
            d = _extract_row_dict(item)
            if d:
                result.append(d)
        return result

    if isinstance(payload, dict):
        return _coerce_dict_payload(payload)

    return []


# ---------------------------------------------------------------------------
# Schema Helpers
# ---------------------------------------------------------------------------

def _extract_columns_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    """Extract (headers, keys) from a sheet spec object or dict."""
    headers: List[str] = []
    keys: List[str] = []

    cols = getattr(spec, "columns", None)
    if cols is None and isinstance(spec, dict):
        cols = spec.get("columns") or spec.get("fields") or []
    if not isinstance(cols, list):
        return headers, keys

    for col in cols:
        if isinstance(col, dict):
            h = _safe_str(
                col.get("header") or col.get("display_header")
                or col.get("label") or col.get("title")
            )
            k = _safe_str(
                col.get("key") or col.get("field")
                or col.get("name") or col.get("id")
            )
        else:
            h = _safe_str(
                getattr(col, "header", None) or getattr(col, "display_header", None)
                or getattr(col, "label", None)
            )
            k = _safe_str(
                getattr(col, "key", None) or getattr(col, "field", None)
                or getattr(col, "name", None)
            )
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or h.lower().replace(" ", "_"))

    # Fallback: direct headers/keys on the spec
    if not headers and isinstance(spec, dict):
        h2 = spec.get("headers") or spec.get("display_headers")
        k2 = spec.get("keys") or spec.get("fields")
        if isinstance(h2, list) and isinstance(k2, list) and h2 and k2:
            headers = [_safe_str(x) for x in h2 if _safe_str(x)]
            keys = [_safe_str(x) for x in k2 if _safe_str(x)]

    return headers, keys


def get_insights_schema() -> Tuple[List[str], List[str], str]:
    """
    Return the Insights_Analysis schema.

    v6.0.0: registry contract is exactly 7 columns
    (section, item, symbol, metric, value, notes, last_updated_riyadh).
    The fallback below mirrors that.

    Returns:
        (headers, keys, source_marker)
    """
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore
        spec = get_sheet_spec("Insights_Analysis")
        headers, keys = _extract_columns_from_spec(spec)
        # Registry v2.5.0 expects 7 columns; accept anything ≥6 to be tolerant.
        if headers and keys and len(headers) == len(keys) and len(keys) >= 6:
            return headers, keys, "schema_registry.get_sheet_spec"
    except Exception as exc:
        logger.debug("get_insights_schema: schema_registry unavailable: %s", exc)

    return list(_FALLBACK_HEADERS), list(_FALLBACK_KEYS), "hardcoded_fallback_7col"


def _get_criteria_fields() -> List[Dict[str, Any]]:
    """Return criteria-block field definitions, from schema_registry or defaults."""
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore
        spec = get_sheet_spec("Insights_Analysis")
        criteria_fields = getattr(spec, "criteria_fields", None)
        if criteria_fields is None and isinstance(spec, dict):
            criteria_fields = spec.get("criteria_fields")

        result: List[Dict[str, Any]] = []
        for cf in list(criteria_fields or []):
            if isinstance(cf, dict):
                result.append({
                    "key": _safe_str(cf.get("key", "")),
                    "label": _safe_str(cf.get("label", "")) or _safe_str(cf.get("key", "")),
                    "dtype": _safe_str(cf.get("dtype", "str")) or "str",
                    "default": cf.get("default", ""),
                    "notes": _safe_str(cf.get("notes", "")),
                })
            else:
                result.append({
                    "key": _safe_str(getattr(cf, "key", "")),
                    "label": _safe_str(getattr(cf, "label", "")) or _safe_str(getattr(cf, "key", "")),
                    "dtype": _safe_str(getattr(cf, "dtype", "str")) or "str",
                    "default": getattr(cf, "default", ""),
                    "notes": _safe_str(getattr(cf, "notes", "")),
                })
        result = [x for x in result if x.get("key")]
        if result:
            return result
    except Exception:
        pass

    # Fallback defaults -- mirror criteria_model.AdvisorCriteria fields
    return [
        {"key": "risk_level", "label": "Risk Level", "dtype": "str", "default": "Moderate",
         "notes": "Low / Moderate / High"},
        {"key": "confidence_level", "label": "Confidence Level", "dtype": "str", "default": "High",
         "notes": "High / Medium / Low"},
        {"key": "invest_period_days", "label": "Investment Period (Days)", "dtype": "int", "default": 90,
         "notes": "Always treated in DAYS (mapped to 1M/3M/12M)"},
        {"key": "required_return_pct", "label": "Required Return %", "dtype": "pct", "default": 0.10,
         "notes": "Minimum expected ROI threshold"},
        {"key": "min_expected_roi", "label": "Min Expected ROI %", "dtype": "pct", "default": 0.0,
         "notes": "Minimum ROI filter"},
        {"key": "max_risk_score", "label": "Max Risk Score", "dtype": "float", "default": 60.0,
         "notes": "0-100; symbols above this get filtered/alerted"},
        {"key": "min_confidence", "label": "Min Confidence", "dtype": "pct", "default": 0.70,
         "notes": "Minimum AI forecast confidence (0-1)"},
        {"key": "top_n", "label": "Top N", "dtype": "int", "default": 10,
         "notes": "Top-N selection count"},
        {"key": "amount", "label": "Amount", "dtype": "float", "default": 0.0,
         "notes": "Investment amount (optional)"},
    ]


# ---------------------------------------------------------------------------
# Criteria Helpers (aligned with criteria_model)
# ---------------------------------------------------------------------------

def _days_to_horizon(days: int) -> str:
    """Convert days to horizon label. Thresholds match criteria_model.py (45 / 120)."""
    try:
        from core.analysis.criteria_model import map_days_to_horizon
        return map_days_to_horizon(days)
    except Exception:
        d = max(1, int(days) if days else 1)
        if d <= 45:
            return "1M"
        if d <= 120:
            return "3M"
        return "12M"


def _horizon_roi_key(horizon: str) -> str:
    """Return the expected_roi_* field key for a horizon."""
    try:
        from core.analysis.criteria_model import horizon_to_expected_roi_key
        return horizon_to_expected_roi_key(horizon)
    except Exception:
        return {"1M": "expected_roi_1m", "3M": "expected_roi_3m"}.get(horizon, "expected_roi_12m")


def _normalize_criteria(criteria: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize criteria dict with defaults; tolerant to many label variations."""
    c = dict(criteria or {})

    # Pages
    pages = c.get("pages_selected") or c.get("pages") or c.get("selected_pages") or []
    if isinstance(pages, str):
        pages = _split_csv(pages)
    if not isinstance(pages, list):
        pages = []
    pages = [_safe_str(p) for p in pages if _safe_str(p)]

    # Investment period
    invest_days = _as_int(
        c.get("invest_period_days")
        or c.get("investment_period_days")
        or c.get("period_days")
        or c.get("horizon_days")
        or 90
    )
    if invest_days is None or invest_days <= 0:
        invest_days = 90

    # Min ROI (as ratio)
    min_roi_ratio = _as_ratio(
        c.get("min_expected_roi") or c.get("min_roi") or c.get("required_return_pct")
    )

    # Max risk
    max_risk = _as_float(c.get("max_risk_score") or c.get("max_risk"))
    if max_risk is None:
        max_risk = 60.0

    # Min confidence
    min_conf = _as_ratio(c.get("min_confidence") or c.get("min_ai_confidence"))
    if min_conf is None:
        min_conf = 0.70

    # Top N
    top_n = _as_int(c.get("top_n") or c.get("limit") or 10)
    if top_n is None or top_n <= 0:
        top_n = 10

    return {
        "risk_level": _safe_str(c.get("risk_level") or "Moderate"),
        "confidence_level": _safe_str(c.get("confidence_level") or "High"),
        "pages_selected": pages or [
            "Market_Leaders", "Global_Markets", "Mutual_Funds",
            "Commodities_FX", "My_Portfolio",
        ],
        "invest_period_days": invest_days,
        "horizon_days": invest_days,
        "min_expected_roi": min_roi_ratio,
        "max_risk_score": max_risk,
        "min_confidence": min_conf,
        "min_volume": _as_float(c.get("min_volume") or c.get("min_liquidity")),
        "use_liquidity_tiebreak": _to_bool(c.get("use_liquidity_tiebreak", True), True),
        "enforce_risk_confidence": _to_bool(c.get("enforce_risk_confidence", True), True),
        "top_n": int(_clamp(float(top_n), 1.0, 200.0)),
        "enrich_final": _to_bool(c.get("enrich_final", True), True),
        # Section flags
        "include_market_summary": _to_bool(c.get("include_market_summary", True), True),
        "include_top_opportunities": _to_bool(c.get("include_top_opportunities", True), True),
        "include_portfolio_health": _to_bool(c.get("include_portfolio_health", True), True),
        "include_risk_scenarios": _to_bool(c.get("include_risk_scenarios", True), True),
        "include_short_term": _to_bool(c.get("include_short_term", True), True),
        "include_macro_signals": _to_bool(c.get("include_macro_signals", True), True),
    }


def _criteria_snapshot(criteria: Dict[str, Any]) -> str:
    """Compact JSON snapshot of criteria."""
    return _compact_json(criteria)


def _criteria_summary(criteria: Dict[str, Any]) -> str:
    """Human-readable one-line summary."""
    roi = criteria.get("min_expected_roi")
    roi_text = _format_percent(roi) if roi is not None else "N/A"
    conf_text = _format_percent(criteria.get("min_confidence"))
    return (
        f"Horizon={_days_to_horizon(int(criteria['invest_period_days']))} | "
        f"Days={criteria['invest_period_days']} | "
        f"Min ROI={roi_text} | "
        f"Max Risk={_format_number(criteria.get('max_risk_score'))} | "
        f"Min Confidence={conf_text}"
    )


# ---------------------------------------------------------------------------
# Row Builder (v6.0.0: schema-correct, no data loss)
# ---------------------------------------------------------------------------

def _format_notes_with_markers(notes: str, signal: str, priority: str) -> str:
    """
    Embed signal / priority into a notes string as a `[SIGNAL|PRIORITY]` prefix.

    v6.0.0: The Insights_Analysis schema is 7 columns and does NOT include
    dedicated `signal` or `priority` columns. To preserve the semantic
    information that section builders compute, those values are prepended
    to `notes`. Format:

        signal + priority both set: "[BUY|HIGH] <original notes>"
        only signal set:            "[BUY] <original notes>"
        only priority set:          "[HIGH] <original notes>"
        neither set:                 <original notes>

    If `notes` is empty, just the marker is returned (or "" if no markers).
    """
    sig = _safe_str(signal)
    pri = _safe_str(priority)
    base = _safe_str(notes)
    markers = [m for m in (sig, pri) if m]
    if not markers:
        return base
    prefix = "[" + " | ".join(markers) + "]"
    return f"{prefix} {base}" if base else prefix


def _make_row(
    keys: Sequence[str],
    *,
    section: str,
    item: str,
    metric: str,
    value: Any,
    symbol: str = "",
    signal: str = "",
    priority: str = "",
    notes: str = "",
    as_of_riyadh: Optional[str] = None,
    last_updated_riyadh: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a row dict aligned with the current schema keys.

    v6.0.0 fixes two production bugs:

    1. TIMESTAMP KEY MISMATCH:
       v5.0.0 wrote the timestamp under `as_of_riyadh` only, but the
       canonical registry key is `last_updated_riyadh`. The schema
       projection emitted blank timestamps in production. v6.0.0 writes
       the timestamp under BOTH keys in `full_row`, so whichever the
       schema asks for, it gets a value. The function also accepts a
       `last_updated_riyadh` keyword argument for callers using the
       canonical name.

    2. SIGNAL/PRIORITY DATA LOSS:
       The 7-column registry schema for Insights_Analysis does NOT include
       `signal` or `priority` columns. v5.0.0 packed those into `full_row`
       but they were silently dropped during projection. v6.0.0 detects
       whether the schema includes those columns:
       - If yes: route `signal` / `priority` to their own columns.
       - If no: embed them into `notes` as a `[SIGNAL|PRIORITY]` prefix
         (see `_format_notes_with_markers`).
       Either way, the information now flows end-to-end.
    """
    timestamp = last_updated_riyadh or as_of_riyadh or _now_riyadh_iso()

    # Format value: keep numbers numeric for sheet formatting
    if value is None:
        formatted_value: Any = ""
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        formatted_value = value
    else:
        formatted_value = _safe_str(value)

    # v6.0.0: schema-aware signal/priority placement
    has_signal_col = "signal" in keys
    has_priority_col = "priority" in keys

    sig_for_col = _safe_str(signal) if has_signal_col else ""
    pri_for_col = _safe_str(priority) if has_priority_col else ""

    # Anything that won't fit into a dedicated column gets embedded in notes.
    sig_for_notes = "" if has_signal_col else _safe_str(signal)
    pri_for_notes = "" if has_priority_col else _safe_str(priority)
    notes_out = _format_notes_with_markers(notes, sig_for_notes, pri_for_notes)

    # full_row exposes BOTH timestamp aliases so projection always finds one.
    full_row: Dict[str, Any] = {
        "section": _safe_str(section) or "General",
        "item": _safe_str(item) or "Item",
        "symbol": _safe_str(symbol),
        "metric": _safe_str(metric) or "metric",
        "value": formatted_value,
        "signal": sig_for_col,
        "priority": pri_for_col,
        "notes": notes_out,
        # v6.0.0: write both keys; schema can ask for whichever it likes
        "last_updated_riyadh": timestamp,
        "as_of_riyadh": timestamp,
    }

    # Only expose columns that are part of the current schema
    return {k: full_row.get(k, "") for k in keys}


def _rows_to_matrix(rows: Sequence[Dict[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    """Convert list of row dicts to a matrix (list of lists)."""
    key_list = [_safe_str(k) for k in keys if _safe_str(k)]
    return [
        [row.get(k, "") for k in key_list]
        for row in rows
        if isinstance(row, dict)
    ]


# ---------------------------------------------------------------------------
# Criteria Rows Builder
# ---------------------------------------------------------------------------

def build_criteria_rows(
    criteria: Optional[Dict[str, Any]] = None,
    last_updated_riyadh: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build the criteria block rows for the top of Insights_Analysis."""
    _, keys, _ = get_insights_schema()
    timestamp = last_updated_riyadh or _now_riyadh_iso()
    norm_criteria = _normalize_criteria(criteria)
    fields = _get_criteria_fields()

    rows: List[Dict[str, Any]] = []

    for field_def in fields:
        key = field_def["key"]
        label = field_def["label"]
        value = norm_criteria.get(key, field_def.get("default", ""))

        # Format percentage-like fields with a display note
        if key.endswith("_pct") or "return" in key or "confidence" in key:
            value_ratio = _as_ratio(value)
            if value_ratio is not None:
                base_note = field_def.get("notes", "")
                display_note = f"display: {_format_percent(value_ratio)}"
                full_note = f"{base_note} ({display_note})" if base_note else display_note
                rows.append(_make_row(
                    keys=keys,
                    section="Criteria",
                    item=label,
                    metric=key,
                    value=value_ratio,
                    notes=full_note,
                    last_updated_riyadh=timestamp,
                ))
                continue

        rows.append(_make_row(
            keys=keys,
            section="Criteria",
            item=label,
            metric=key,
            value=value,
            notes=field_def.get("notes", ""),
            last_updated_riyadh=timestamp,
        ))

    # Snapshot
    rows.append(_make_row(
        keys=keys,
        section="Criteria",
        item="Criteria Snapshot",
        metric="criteria_snapshot",
        value=_criteria_snapshot(norm_criteria),
        notes="Compact JSON snapshot used by Top10 / advisor contextual logic",
        last_updated_riyadh=timestamp,
    ))

    # Summary
    rows.append(_make_row(
        keys=keys,
        section="Criteria",
        item="Criteria Summary",
        metric="criteria_summary",
        value=_days_to_horizon(int(norm_criteria["invest_period_days"])),
        notes=_criteria_summary(norm_criteria),
        last_updated_riyadh=timestamp,
    ))

    return rows


# ---------------------------------------------------------------------------
# Engine Integration (Async with real timeouts)
# ---------------------------------------------------------------------------

async def _maybe_await(obj: Any) -> Any:
    """Await if awaitable; return as-is otherwise."""
    if inspect.isawaitable(obj):
        return await obj
    return obj


async def _invoke_with_timeout(coro_factory, timeout_sec: float) -> Any:
    """Run `coro_factory()` under `asyncio.wait_for`; return None on timeout/failure."""
    try:
        return await asyncio.wait_for(coro_factory(), timeout=max(0.1, timeout_sec))
    except asyncio.TimeoutError:
        logger.debug("engine call timed out after %.1fs", timeout_sec)
        return None
    except Exception as exc:
        logger.debug("engine call failed: %s", exc)
        return None


async def _invoke_engine_method(
    engine: Any,
    method_names: Sequence[str],
    kwargs_attempts: Sequence[Dict[str, Any]],
    timeout_sec: float,
) -> Any:
    """
    Try a sequence of method names on `engine`, each with several kwargs
    variants. Returns the first non-None result, or None.
    """
    if not engine:
        return None

    for name in method_names:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue

        for kwargs in kwargs_attempts:
            result = await _invoke_with_timeout(
                lambda fn=fn, kwargs=kwargs: _maybe_await(fn(**kwargs)),
                timeout_sec,
            )
            if result is not None:
                return result

    return None


def _rows_to_quote_map(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Convert quote rows into a symbol -> row dict map."""
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        sym = _safe_str(row.get("symbol") or row.get("ticker") or row.get("code"))
        if sym:
            result[sym] = dict(row)
    return result


async def _fetch_quotes(
    engine: Any,
    symbols: Sequence[str],
    *,
    mode: str = "",
    timeout_sec: float = _DEFAULT_QUOTES_TIMEOUT_SEC,
) -> Dict[str, Dict[str, Any]]:
    """Fetch quotes for a list of symbols from the engine, with real timeout."""
    if not engine or not symbols:
        return {}

    requested = _dedupe_keep_order(symbols)

    # Batch dict API
    batch_fn = getattr(engine, "get_enriched_quotes_batch", None)
    if callable(batch_fn):
        for kwargs in ({"mode": mode}, {}):
            try:
                result = await _invoke_with_timeout(
                    lambda fn=batch_fn, kw=kwargs: _maybe_await(fn(requested, **kw)),
                    timeout_sec,
                )
            except TypeError as e:
                if _is_signature_mismatch(e):
                    continue
                result = None
            if result is None:
                continue
            if isinstance(result, dict):
                rows = _coerce_to_rows(result)
                if rows:
                    row_map = _rows_to_quote_map(rows)
                    return {s: row_map.get(s, {"symbol": s}) for s in requested}
                # Raw symbol map
                return {
                    s: _extract_row_dict(result.get(s)) if isinstance(result.get(s), dict)
                    else {"symbol": s}
                    for s in requested
                }

    # Batch list API
    list_fn = getattr(engine, "get_enriched_quotes", None)
    if callable(list_fn):
        result = await _invoke_with_timeout(
            lambda: _maybe_await(list_fn(requested)),
            timeout_sec,
        )
        if result is not None:
            rows = _coerce_to_rows(result)
            if rows:
                row_map = _rows_to_quote_map(rows)
                return {s: row_map.get(s, {"symbol": s}) for s in requested}
            if isinstance(result, list):
                return {s: _extract_row_dict(v) for s, v in zip(requested, result)}

    # Per-symbol fallback
    output: Dict[str, Dict[str, Any]] = {}
    single_dict_fn = getattr(engine, "get_enriched_quote_dict", None)
    single_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    per_symbol_budget = timeout_sec / max(1, len(requested))

    for sym in requested:
        fn = single_dict_fn if callable(single_dict_fn) else single_fn
        if not callable(fn):
            output[sym] = {"symbol": sym, "warning": "engine_missing_quote_methods"}
            continue
        result = await _invoke_with_timeout(
            lambda fn=fn, sym=sym: _maybe_await(fn(sym)),
            per_symbol_budget,
        )
        output[sym] = _extract_row_dict(result) if result is not None else {
            "symbol": sym, "warning": "quote_error_or_timeout",
        }

    return output


async def _fetch_top10_payload(
    engine: Any,
    criteria: Optional[Dict[str, Any]] = None,
    *,
    limit: int = 10,
    mode: str = "",
    timeout_sec: float = _DEFAULT_TOP10_TIMEOUT_SEC,
) -> Dict[str, Any]:
    """Fetch Top 10 investments payload from engine, with real timeout."""
    if not engine:
        return {}

    norm_criteria = _normalize_criteria(criteria)
    top_n = int(norm_criteria.get("top_n", limit))

    # Try top10_selector module first
    try:
        from core.analysis.top10_selector import build_top10_rows  # type: ignore
        result = await _invoke_with_timeout(
            lambda: _maybe_await(build_top10_rows(
                engine=engine, criteria=norm_criteria, limit=top_n, mode=mode,
            )),
            timeout_sec,
        )
        if result is not None:
            if isinstance(result, dict):
                return result
            rows = _coerce_to_rows(result)
            if rows:
                return {"rows": rows}
    except ImportError:
        pass

    # Engine fallback
    result = await _invoke_engine_method(
        engine,
        method_names=(
            "build_top10_rows", "get_top10_rows", "top10_rows",
            "build_top10", "get_top10_investments", "select_top10",
        ),
        kwargs_attempts=(
            {"criteria": norm_criteria, "limit": top_n, "mode": mode},
            {"criteria": norm_criteria, "limit": top_n},
            {"limit": top_n},
            {},
        ),
        timeout_sec=timeout_sec,
    )

    if result is None:
        return {}
    if isinstance(result, dict):
        return result
    rows = _coerce_to_rows(result)
    return {"rows": rows} if rows else {}


async def _fetch_top10_symbols(
    engine: Any,
    criteria: Optional[Dict[str, Any]] = None,
    *,
    limit: int = 10,
    timeout_sec: float = _DEFAULT_TOP10_TIMEOUT_SEC,
) -> List[str]:
    """Fetch just the Top 10 symbols from engine."""
    if not engine:
        return []

    norm_criteria = _normalize_criteria(criteria)
    top_n = int(norm_criteria.get("top_n", limit))

    # Try selector module first
    try:
        from core.analysis.top10_selector import select_top10_symbols  # type: ignore
        result = await _invoke_with_timeout(
            lambda: _maybe_await(select_top10_symbols(
                engine=engine, criteria=norm_criteria, limit=top_n,
            )),
            timeout_sec,
        )
        if isinstance(result, (list, tuple)):
            return _dedupe_keep_order(result)[:top_n]
    except ImportError:
        pass

    # Engine fallback
    result = await _invoke_engine_method(
        engine,
        method_names=(
            "get_top10_symbols", "select_top10_symbols", "top10_symbols",
            "get_top10_investments", "select_top10",
        ),
        kwargs_attempts=(
            {"criteria": norm_criteria, "limit": top_n},
            {"limit": top_n},
            {},
        ),
        timeout_sec=timeout_sec,
    )

    if isinstance(result, (list, tuple)):
        symbols: List[str] = []
        for item in result:
            if isinstance(item, str):
                symbols.append(item)
            elif isinstance(item, dict):
                symbols.append(_safe_str(item.get("symbol") or item.get("ticker") or item.get("code")))
            else:
                d = _extract_row_dict(item)
                symbols.append(_safe_str(d.get("symbol") or d.get("ticker") or d.get("code")))
        return _dedupe_keep_order(symbols)[:top_n]

    if isinstance(result, dict):
        if isinstance(result.get("symbols"), (list, tuple)):
            return _dedupe_keep_order(result["symbols"])[:top_n]
        rows = _coerce_to_rows(result)
        if rows:
            return _dedupe_keep_order(
                _safe_str(r.get("symbol") or r.get("ticker") or r.get("code"))
                for r in rows
            )[:top_n]

    return []


# ---------------------------------------------------------------------------
# Section Builders (unchanged from v5.0.0 except _make_row routing fix)
# ---------------------------------------------------------------------------

def _build_universe_snapshot_rows(
    ctx: BuildContext,
    section_name: str,
    symbols: List[str],
    quotes: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build Market Summary rows for one universe."""
    rows: List[Dict[str, Any]] = []

    rows.append(_make_row(
        keys=ctx.keys, section=section_name, item="Universe Size",
        metric="count", value=len(symbols),
        notes="Number of symbols requested for this section",
        last_updated_riyadh=ctx.ts,
    ))

    coverage_price = sum(1 for d in quotes.values() if _as_float(d.get("current_price")) is not None)
    coverage_change = sum(1 for d in quotes.values() if _as_float(d.get("percent_change")) is not None)

    rows.append(_make_row(
        keys=ctx.keys, section=section_name, item="Coverage",
        metric="coverage_current_price", value=f"{coverage_price}/{len(symbols)}",
        notes="Symbols with current_price available",
        last_updated_riyadh=ctx.ts,
    ))
    rows.append(_make_row(
        keys=ctx.keys, section=section_name, item="Coverage",
        metric="coverage_percent_change", value=f"{coverage_change}/{len(symbols)}",
        notes="Symbols with percent_change available",
        last_updated_riyadh=ctx.ts,
    ))

    # Movers
    movers = [(s, pc) for s, d in quotes.items()
              if (pc := _as_float(d.get("percent_change"))) is not None]
    if movers:
        movers.sort(key=lambda x: x[1], reverse=True)
        top_sym, top_pc = movers[0]
        bottom_sym, bottom_pc = movers[-1]
        avg_pc = sum(pc for _, pc in movers) / len(movers)

        rows.append(_make_row(
            keys=ctx.keys, section=section_name, item="Top Gainer",
            symbol=top_sym, metric="percent_change",
            value=_as_percent_points(top_pc),
            notes=f"Highest percent_change in this universe (display: {_format_percent(top_pc)})",
            last_updated_riyadh=ctx.ts,
        ))
        rows.append(_make_row(
            keys=ctx.keys, section=section_name, item="Top Loser",
            symbol=bottom_sym, metric="percent_change",
            value=_as_percent_points(bottom_pc),
            notes=f"Lowest percent_change in this universe (display: {_format_percent(bottom_pc)})",
            last_updated_riyadh=ctx.ts,
        ))
        rows.append(_make_row(
            keys=ctx.keys, section=section_name, item="Average Change",
            metric="avg_percent_change", value=_as_percent_points(avg_pc),
            notes=f"Average percent_change across symbols (display: {_format_percent(avg_pc)})",
            last_updated_riyadh=ctx.ts,
        ))
    else:
        rows.append(_make_row(
            keys=ctx.keys, section=section_name, item="Snapshot",
            metric="status", value="No movers data",
            notes="percent_change not available for this universe",
            last_updated_riyadh=ctx.ts,
        ))

    # Best expected ROI
    roi_items = [(s, roi) for s, d in quotes.items()
                 if (roi := _as_float(d.get("expected_roi_3m"))) is not None]
    if roi_items:
        roi_items.sort(key=lambda x: x[1], reverse=True)
        best_sym, best_roi = roi_items[0]
        rows.append(_make_row(
            keys=ctx.keys, section=section_name, item="Best Expected ROI (3M)",
            symbol=best_sym, metric="expected_roi_3m",
            value=_as_percent_points(best_roi),
            notes=f"Highest expected_roi_3m (display: {_format_percent(best_roi)})",
            last_updated_riyadh=ctx.ts,
        ))

    # Highest volatility
    vol_items = [(s, v) for s, d in quotes.items()
                 if (v := _as_float(d.get("volatility_90d"))) is not None]
    if vol_items:
        vol_items.sort(key=lambda x: x[1], reverse=True)
        vol_sym, vol_val = vol_items[0]
        rows.append(_make_row(
            keys=ctx.keys, section=section_name, item="Highest Volatility (90D)",
            symbol=vol_sym, metric="volatility_90d",
            value=_as_percent_points(vol_val),
            notes=f"Highest volatility_90d (display: {_format_percent(vol_val)})",
            last_updated_riyadh=ctx.ts,
        ))

    # Average confidence
    conf_items = [c for d in quotes.values()
                  if (c := _as_float(d.get("forecast_confidence"))) is not None]
    if conf_items:
        avg_conf = sum(conf_items) / len(conf_items)
        rows.append(_make_row(
            keys=ctx.keys, section=section_name, item="Average Forecast Confidence",
            metric="avg_forecast_confidence", value=avg_conf,
            notes="Average forecast_confidence across symbols with data",
            last_updated_riyadh=ctx.ts,
        ))

    return rows


def _build_risk_alert_rows(
    ctx: BuildContext,
    quotes: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build Risk Alerts section rows."""
    rows: List[Dict[str, Any]] = []
    if not quotes:
        return rows

    max_risk = ctx.norm_criteria.get("max_risk_score", 60.0)
    high_risk: List[Tuple[float, str, Dict[str, Any]]] = []
    cautions: List[Tuple[str, str, str]] = []

    for sym, d in quotes.items():
        risk_score = _as_float(d.get("risk_score"))
        drawdown = _as_float(d.get("max_drawdown_1y"))
        rsi_signal = _safe_str(d.get("rsi_signal", "")).lower()
        vol_30d = _as_percent_points(d.get("volatility_30d"))

        if risk_score is not None and risk_score >= 55.0:
            high_risk.append((risk_score, sym, d))
        if drawdown is not None:
            dd_abs = abs(_as_percent_points(drawdown) or 0.0)
            if dd_abs >= 30.0:
                priority = _PRI_HIGH if dd_abs >= 40.0 else _PRI_MEDIUM
                cautions.append((sym, f"Max drawdown 1Y: {dd_abs:.1f}%", priority))
        if rsi_signal == "overbought":
            cautions.append((sym, "RSI Overbought -- consider reducing position", _PRI_MEDIUM))
        if vol_30d is not None and vol_30d >= 40.0:
            cautions.append((sym, f"High volatility 30D: {vol_30d:.1f}%", _PRI_MEDIUM))

    if not high_risk and not cautions:
        rows.append(_make_row(
            keys=ctx.keys, section="Risk Alerts", item="Summary",
            metric="risk_alert_count", value=0,
            signal=_SIGNAL_OK, priority=_PRI_LOW,
            notes="No high-risk conditions detected",
            last_updated_riyadh=ctx.ts,
        ))
        return rows

    high_risk.sort(key=lambda x: x[0], reverse=True)
    high_count = sum(1 for r, _, _ in high_risk if r >= 70.0)

    rows.append(_make_row(
        keys=ctx.keys, section="Risk Alerts", item="Summary",
        metric="at_risk_count", value=len(high_risk),
        signal=_SIGNAL_ALERT if high_count > 0 else _SIGNAL_WARN,
        priority=_PRI_HIGH if high_count > 0 else _PRI_MEDIUM,
        notes=f"{len(high_risk)} symbol(s) with risk_score > 55. {high_count} above 70.",
        last_updated_riyadh=ctx.ts,
    ))

    for risk_score, sym, d in high_risk[:8]:
        risk_bucket = _safe_str(d.get("risk_bucket", ""))
        reco = _safe_str(d.get("recommendation", ""))
        vol = _as_percent_points(d.get("volatility_30d"))
        priority = _PRI_HIGH if risk_score >= 70.0 else _PRI_MEDIUM
        signal = _SIGNAL_ALERT if risk_score >= max_risk else _SIGNAL_WARN

        note_parts = [f"Risk={risk_score:.1f}"]
        if risk_bucket:
            note_parts.append(f"Bucket={risk_bucket}")
        if reco:
            note_parts.append(f"Reco={reco}")
        if vol is not None:
            note_parts.append(f"Vol30D={vol:.1f}%")

        rows.append(_make_row(
            keys=ctx.keys, section="Risk Alerts", item=f"High Risk -- {sym}",
            symbol=sym, metric="risk_score", value=round(risk_score, 1),
            signal=signal, priority=priority,
            notes=" | ".join(note_parts),
            last_updated_riyadh=ctx.ts,
        ))

    for sym, reason, priority in cautions[:6]:
        rows.append(_make_row(
            keys=ctx.keys, section="Risk Alerts", item=f"Caution -- {sym}",
            symbol=sym, metric="caution_flag", value="Caution",
            signal=_SIGNAL_ALERT, priority=priority,
            notes=reason, last_updated_riyadh=ctx.ts,
        ))

    return rows


def _build_short_term_rows(
    ctx: BuildContext,
    quotes: Dict[str, Dict[str, Any]],
    min_tech_score: float = 58.0,
    max_items: int = 7,
) -> List[Dict[str, Any]]:
    """Build Short-Term Opportunities section rows."""
    rows: List[Dict[str, Any]] = []
    if not quotes:
        return rows

    candidates: List[Tuple[float, str, Dict[str, Any]]] = []
    for sym, d in quotes.items():
        tech_score = _as_float(d.get("technical_score"))
        st_signal = _safe_str(d.get("short_term_signal", "")).upper()
        if tech_score is None or tech_score < min_tech_score:
            continue
        if st_signal not in ("BUY", "STRONG_BUY"):
            continue
        candidates.append((tech_score, sym, d))

    if not candidates:
        rows.append(_make_row(
            keys=ctx.keys, section="Short-Term Opportunities", item="Status",
            metric="st_opportunities_count", value=0,
            signal=_SIGNAL_OK, priority=_PRI_LOW,
            notes=f"No symbols with technical_score >= {min_tech_score:.0f} + ST signal BUY",
            last_updated_riyadh=ctx.ts,
        ))
        return rows

    candidates.sort(key=lambda x: x[0], reverse=True)

    rows.append(_make_row(
        keys=ctx.keys, section="Short-Term Opportunities", item="Summary",
        metric="st_opportunities_count", value=len(candidates),
        signal="BUY", priority=_PRI_HIGH,
        notes=f"{len(candidates)} symbol(s) with strong technical setup",
        last_updated_riyadh=ctx.ts,
    ))

    for tech_score, sym, d in candidates[:max_items]:
        st_signal = _safe_str(d.get("short_term_signal", "")).upper()
        rsi_signal = _safe_str(d.get("rsi_signal", ""))
        rsi_value = _as_float(d.get("rsi_14"))
        volume_ratio = _as_float(d.get("volume_ratio"))
        day_range = _as_float(d.get("day_range_position"))
        period = _safe_str(d.get("invest_period_label", ""))
        roi_1m = _as_percent_points(d.get("expected_roi_1m"))
        upside = _as_float(d.get("upside_pct"))
        name = _safe_str(d.get("name", sym))

        signal = "STRONG_BUY" if st_signal == "STRONG_BUY" else "BUY"
        priority = (
            _PRI_HIGH if (st_signal == "STRONG_BUY" or tech_score >= 75) else
            _PRI_MEDIUM if tech_score >= 65 else _PRI_LOW
        )

        note_parts = [f"Tech={tech_score:.1f}"]
        if rsi_signal:
            rsi_text = f"({rsi_value:.0f})" if rsi_value is not None else ""
            note_parts.append(f"RSI={rsi_signal}{rsi_text}")
        if volume_ratio is not None:
            note_parts.append(f"VolRatio={volume_ratio:.2f}x")
        if day_range is not None:
            note_parts.append(f"DayPos={day_range * 100:.0f}%")
        if period:
            note_parts.append(f"Horizon={period}")
        if upside is not None:
            note_parts.append(f"Upside={_format_percent(upside)}")
        if name and name != sym:
            note_parts.append(name)

        value_str = f"{tech_score:.1f}"
        if roi_1m is not None:
            value_str += f" | ROI={roi_1m:.2f}%"

        rows.append(_make_row(
            keys=ctx.keys, section="Short-Term Opportunities",
            item=f"{st_signal} -- {sym}", symbol=sym, metric="technical_score",
            value=value_str, signal=signal, priority=priority,
            notes=" | ".join(note_parts), last_updated_riyadh=ctx.ts,
        ))

    return rows


def _build_portfolio_kpis_rows(
    ctx: BuildContext,
    symbols: List[str],
    quotes: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build Portfolio KPIs section rows."""
    rows: List[Dict[str, Any]] = []

    total_cost = 0.0
    total_value = 0.0
    total_day_pl = 0.0
    position_count = 0
    at_risk_count = 0
    have_positions = False

    stop_alerts: List[Tuple[str, float, str]] = []
    rebal_alerts: List[Tuple[str, str, float]] = []

    for sym in symbols:
        d = quotes.get(sym, {})
        qty = _as_float(d.get("position_qty"))
        avg_cost = _as_float(d.get("avg_cost"))
        current_price = _as_float(d.get("current_price"))
        day_pl = _as_float(d.get("day_pl"))
        risk_score = _as_float(d.get("risk_score"))
        dist_to_sl = _as_float(d.get("distance_to_sl_pct"))
        weight_dev = _as_float(d.get("weight_deviation"))
        rebalance = _safe_str(d.get("rebalance_signal") or d.get("rebalance", ""))

        if qty is None or avg_cost is None or qty == 0:
            continue

        have_positions = True
        position_count += 1
        cost = qty * avg_cost
        total_cost += cost
        if current_price is not None:
            total_value += qty * current_price
        if day_pl is not None:
            total_day_pl += day_pl
        if risk_score is not None and risk_score > 60.0:
            at_risk_count += 1

        if dist_to_sl is not None:
            dist_pp = _as_percent_points(dist_to_sl) or 0.0
            if 0 < dist_pp < 3.0:
                stop_alerts.append((sym, dist_pp, _PRI_HIGH))
            elif 3.0 <= dist_pp < 10.0:
                stop_alerts.append((sym, dist_pp, _PRI_MEDIUM))

        if weight_dev is not None:
            dev_abs = abs(_as_percent_points(weight_dev) or 0.0)
            if dev_abs >= 5.0:
                action = rebalance or ("Add" if weight_dev < 0 else "Trim")
                rebal_alerts.append((sym, action, dev_abs))

    if not have_positions:
        rows.append(_make_row(
            keys=ctx.keys, section="Portfolio KPIs", item="Portfolio",
            metric="status", value="No Positions",
            signal=_SIGNAL_OK, priority=_PRI_LOW,
            notes="No position_qty / avg_cost found. Enter holdings in My_Portfolio.",
            last_updated_riyadh=ctx.ts,
        ))
        return rows

    unrealized_pl = total_value - total_cost
    unrealized_pct = (unrealized_pl / total_cost) if total_cost > 0 else None
    pl_pct_pts = _as_percent_points(unrealized_pct) or 0.0
    pl_signal = _SIGNAL_OK if pl_pct_pts >= 0 else _SIGNAL_ALERT

    health_score = 60.0 + min(30.0, max(-30.0, pl_pct_pts))
    if position_count > 0:
        health_score -= (at_risk_count / position_count) * 20.0
    health_score = _clamp(health_score, 0.0, 100.0)

    rows.append(_make_row(
        keys=ctx.keys, section="Portfolio KPIs", item="Positions",
        metric="position_count", value=position_count,
        priority=_PRI_LOW,
        notes=f"Health={health_score:.1f}/100. {at_risk_count} position(s) with risk_score > 60",
        last_updated_riyadh=ctx.ts,
    ))
    rows.append(_make_row(
        keys=ctx.keys, section="Portfolio KPIs", item="Total Value",
        metric="total_value", value=round(total_value, 2),
        priority=_PRI_LOW,
        notes=f"Cost basis: {round(total_cost, 2)}",
        last_updated_riyadh=ctx.ts,
    ))
    rows.append(_make_row(
        keys=ctx.keys, section="Portfolio KPIs", item="Unrealized P/L",
        metric="unrealized_pl", value=round(unrealized_pl, 2),
        signal=pl_signal,
        priority=_PRI_HIGH if pl_pct_pts < -15 else (_PRI_MEDIUM if pl_pct_pts < -5 else _PRI_LOW),
        notes=f"{_format_percent(unrealized_pct)} | total_value - total_cost",
        last_updated_riyadh=ctx.ts,
    ))

    if total_day_pl != 0.0:
        rows.append(_make_row(
            keys=ctx.keys, section="Portfolio KPIs", item="Today's P/L",
            metric="day_pl", value=round(total_day_pl, 2),
            signal=_SIGNAL_OK if total_day_pl > 0 else _SIGNAL_ALERT,
            priority=_PRI_MEDIUM if abs(total_day_pl) > total_cost * 0.01 else _PRI_LOW,
            notes=f"Sum(day_pl) across {position_count} positions",
            last_updated_riyadh=ctx.ts,
        ))

    for sym, dist_pp, priority in sorted(stop_alerts, key=lambda x: x[1]):
        rows.append(_make_row(
            keys=ctx.keys, section="Portfolio KPIs", item=f"Near SL -- {sym}",
            symbol=sym, metric="distance_to_sl_pct",
            value=f"{dist_pp:.2f}%",
            signal=_SIGNAL_ALERT, priority=priority,
            notes=(
                f"Price only {dist_pp:.2f}% above stop loss. "
                f"{'Review immediately.' if priority == _PRI_HIGH else 'Monitor closely.'}"
            ),
            last_updated_riyadh=ctx.ts,
        ))

    for sym, action, dev_abs in sorted(rebal_alerts, key=lambda x: x[2], reverse=True):
        priority = _PRI_HIGH if dev_abs >= 10.0 else _PRI_MEDIUM
        signal = _SIGNAL_UP if action.lower() in ("add", "buy") else _SIGNAL_DOWN
        rows.append(_make_row(
            keys=ctx.keys, section="Portfolio KPIs", item=f"Rebalance -- {sym}",
            symbol=sym, metric="weight_deviation",
            value=f"{dev_abs:.1f}% -> {action}",
            signal=signal, priority=priority,
            notes=f"Portfolio weight drifted {dev_abs:.1f}% from target. Action: {action}",
            last_updated_riyadh=ctx.ts,
        ))

    return rows


def _build_macro_signal_rows(
    ctx: BuildContext,
    quotes: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build Macro Signals section rows."""
    rows: List[Dict[str, Any]] = []
    if not quotes:
        return rows

    sector_signals: Dict[str, List[str]] = {}
    outperformers: List[Tuple[float, str]] = []
    underperformers: List[Tuple[float, str]] = []

    for sym, d in quotes.items():
        sector_signal = _safe_str(d.get("sector_signal", "")).lower()
        sector = _safe_str(d.get("sector", ""))
        vs_sp500 = _as_percent_points(d.get("vs_sp500_ytd"))

        if sector_signal in ("bullish", "bearish", "neutral") and sector:
            sector_signals.setdefault(sector, []).append(sector_signal)
        if vs_sp500 is not None:
            if vs_sp500 >= 5.0:
                outperformers.append((vs_sp500, sym))
            elif vs_sp500 <= -5.0:
                underperformers.append((vs_sp500, sym))

    if not sector_signals and not outperformers and not underperformers:
        rows.append(_make_row(
            keys=ctx.keys, section="Macro Signals", item="Status",
            metric="macro_signal_count", value="No Data",
            signal=_SIGNAL_OK, priority=_PRI_LOW,
            notes="No sector_signal or vs_sp500_ytd data from Global_Markets universe",
            last_updated_riyadh=ctx.ts,
        ))
        return rows

    for sector, signals in sorted(sector_signals.items()):
        bullish = signals.count("bullish")
        bearish = signals.count("bearish")
        total = len(signals)
        if bullish > bearish:
            signal, dominant = _SIGNAL_UP, f"Bullish ({bullish}/{total})"
        elif bearish > bullish:
            signal, dominant = _SIGNAL_DOWN, f"Bearish ({bearish}/{total})"
        else:
            signal, dominant = _SIGNAL_NEUTRAL, f"Neutral ({total})"
        priority = _PRI_HIGH if bullish + bearish >= total * 0.7 else _PRI_MEDIUM

        rows.append(_make_row(
            keys=ctx.keys, section="Macro Signals", item=f"Sector -- {sector}",
            metric="sector_signal", value=dominant,
            signal=signal, priority=priority,
            notes=f"{sector}: {bullish} Bullish / {bearish} Bearish / {total - bullish - bearish} Neutral",
            last_updated_riyadh=ctx.ts,
        ))

    outperformers.sort(key=lambda x: x[0], reverse=True)
    for vs_sp, sym in outperformers[:4]:
        rows.append(_make_row(
            keys=ctx.keys, section="Macro Signals", item=f"Outperform -- {sym}",
            symbol=sym, metric="vs_sp500_ytd",
            value=f"+{vs_sp:.2f}% vs S&P 500",
            signal=_SIGNAL_UP,
            priority=_PRI_HIGH if vs_sp >= 10.0 else _PRI_MEDIUM,
            notes=f"YTD return exceeds S&P 500 by {vs_sp:.2f}% -- strong relative momentum",
            last_updated_riyadh=ctx.ts,
        ))

    underperformers.sort(key=lambda x: x[0])
    for vs_sp, sym in underperformers[:4]:
        rows.append(_make_row(
            keys=ctx.keys, section="Macro Signals", item=f"Underperform -- {sym}",
            symbol=sym, metric="vs_sp500_ytd",
            value=f"{vs_sp:.2f}% vs S&P 500",
            signal=_SIGNAL_DOWN,
            priority=_PRI_HIGH if vs_sp <= -10.0 else _PRI_MEDIUM,
            notes=f"YTD return lags S&P 500 by {abs(vs_sp):.2f}%",
            last_updated_riyadh=ctx.ts,
        ))

    return rows


def _reco_to_signal(recommendation: str) -> str:
    """
    v6.0.0: map a 5-tier recommendation token to a row-level signal.

    Mapping:
      STRONG_BUY / BUY  -> _SIGNAL_UP   ("BUY")
      SELL / REDUCE     -> _SIGNAL_DOWN ("SELL")
      HOLD / unknown    -> _SIGNAL_NEUTRAL ("HOLD")
    """
    r = _safe_str(recommendation).upper()
    if r in _BULLISH_RECOS:
        return _SIGNAL_UP
    if r in _BEARISH_RECOS:
        return _SIGNAL_DOWN
    return _SIGNAL_NEUTRAL


def _build_top_picks_rows(
    ctx: BuildContext,
    top10_payload: Dict[str, Any],
    top10_quotes: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Build Top Picks section rows from top10 payload.

    v6.0.0: signal mapping recognises full v7.0.0 5-tier vocabulary
    (STRONG_BUY/BUY -> BUY signal, SELL/REDUCE -> SELL signal,
    HOLD -> neutral).
    """
    rows: List[Dict[str, Any]] = []
    top_rows = _coerce_to_rows(top10_payload)

    # Symbols-only fallback path
    if not top_rows:
        top10_symbols = top10_payload.get("symbols") if isinstance(top10_payload, dict) else None
        if not isinstance(top10_symbols, list) or not top10_symbols:
            rows.append(_make_row(
                keys=ctx.keys, section="Top Picks", item="Status",
                metric="top10_status", value="Unavailable",
                notes="Top10 payload is empty",
                last_updated_riyadh=ctx.ts,
            ))
            return rows

        quotes = top10_quotes or {}
        rows.append(_make_row(
            keys=ctx.keys, section="Top Picks", item="Summary",
            metric="top_picks_count", value=len(top10_symbols),
            priority=_PRI_LOW,
            notes=f"Top {len(top10_symbols)} picks (symbol-only fallback)",
            last_updated_riyadh=ctx.ts,
        ))

        for i, sym in enumerate(top10_symbols[:10], 1):
            d = quotes.get(sym, {})
            roi_3m = _as_float(d.get("expected_roi_3m"))
            recommendation = _safe_str(d.get("recommendation", ""))
            name = _safe_str(d.get("name", sym))
            priority = _PRI_HIGH if i <= 3 else (_PRI_MEDIUM if i <= 7 else _PRI_LOW)
            signal = _reco_to_signal(recommendation)

            note = f"Reco={recommendation}" if recommendation else ""
            if name and name != sym:
                note = f"{note} | {name}" if note else name

            rows.append(_make_row(
                keys=ctx.keys, section="Top Picks", item=f"#{i} {sym}",
                symbol=sym, metric="expected_roi_3m",
                value=_format_percent(roi_3m) if roi_3m is not None else recommendation,
                signal=signal, priority=priority,
                notes=note or "Top pick",
                last_updated_riyadh=ctx.ts,
            ))
        return rows

    # Full-payload path
    rows.append(_make_row(
        keys=ctx.keys, section="Top Picks", item="Status",
        metric="top10_count", value=len(top_rows),
        notes="Top10 rows generated through selector/engine path",
        last_updated_riyadh=ctx.ts,
    ))
    rows.append(_make_row(
        keys=ctx.keys, section="Top Picks", item="Criteria Snapshot",
        metric="criteria_snapshot", value=_criteria_snapshot(ctx.norm_criteria),
        notes=_criteria_summary(ctx.norm_criteria),
        last_updated_riyadh=ctx.ts,
    ))

    horizon = _days_to_horizon(int(ctx.norm_criteria.get("invest_period_days", 90)))
    roi_key = _horizon_roi_key(horizon)

    for i, raw in enumerate(top_rows[:10], 1):
        if not isinstance(raw, dict):
            continue

        sym = _safe_str(raw.get("symbol") or raw.get("ticker") or raw.get("code"))
        name = _safe_str(raw.get("name"))
        rank = _as_int(raw.get("top10_rank")) or i
        roi_val = raw.get(roi_key)
        recommendation = _safe_str(raw.get("recommendation"))
        selection_reason = _safe_str(raw.get("selection_reason"))
        recommendation_reason = _safe_str(raw.get("recommendation_reason"))
        confidence = raw.get("forecast_confidence")
        overall = raw.get("overall_score")
        risk_bucket = _safe_str(raw.get("risk_bucket"))

        note_parts: List[str] = []
        if name:
            note_parts.append(name)
        if recommendation:
            note_parts.append(f"reco={recommendation}")
        if confidence is not None:
            note_parts.append(f"conf={_format_percent(confidence)}")
        if risk_bucket:
            note_parts.append(f"risk={risk_bucket}")
        if selection_reason:
            note_parts.append(f"why={selection_reason}")
        elif recommendation_reason:
            note_parts.append(f"why={recommendation_reason}")

        priority = _PRI_HIGH if rank <= 3 else (_PRI_MEDIUM if rank <= 7 else _PRI_LOW)
        signal = _reco_to_signal(recommendation)

        rows.append(_make_row(
            keys=ctx.keys, section="Top Picks", item=f"#{rank}",
            symbol=sym, metric=roi_key,
            value=_format_percent(roi_val) if roi_val is not None else "",
            notes=" | ".join(note_parts) if note_parts else "Top10 ranked item",
            signal=signal, priority=priority,
            last_updated_riyadh=ctx.ts,
        ))

        if overall is not None:
            rows.append(_make_row(
                keys=ctx.keys, section="Top Picks Context",
                item=f"#{rank} Overall Score", symbol=sym,
                metric="overall_score", value=overall,
                notes=f"Rank={rank}" + (f" | {name}" if name else ""),
                last_updated_riyadh=ctx.ts,
            ))

        if selection_reason or recommendation_reason:
            rows.append(_make_row(
                keys=ctx.keys, section="Top Picks Context",
                item=f"#{rank} Selection Logic", symbol=sym,
                metric="selection_reason", value=rank,
                notes=selection_reason or recommendation_reason,
                last_updated_riyadh=ctx.ts,
            ))

    return rows


def _build_risk_scenario_rows(ctx: BuildContext) -> List[Dict[str, Any]]:
    """
    Build Risk Scenarios section rows using criteria_model.build_scenario_specs.
    """
    rows: List[Dict[str, Any]] = []

    try:
        from core.analysis.criteria_model import AdvisorCriteria, build_scenario_specs
    except Exception as exc:
        logger.debug("Risk scenarios unavailable: %s", exc)
        return rows

    nc = ctx.norm_criteria
    try:
        # Translate normalized criteria keys to AdvisorCriteria field names
        criteria = AdvisorCriteria(
            risk_level=_safe_str(nc.get("risk_level", "Moderate")) or "Moderate",
            confidence_level=_safe_str(nc.get("confidence_level", "High")) or "High",
            invest_period_days=int(nc.get("invest_period_days", 90)),
            required_return_pct=float(nc.get("min_expected_roi") or 0.10),
            min_expected_roi_pct=float(nc.get("min_expected_roi") or 0.0),
            min_ai_confidence=float(nc.get("min_confidence") or 0.60),
            max_risk_score=float(nc.get("max_risk_score") or 60.0),
            top_n=int(nc.get("top_n", 10)),
        )
        specs = build_scenario_specs(criteria)
    except Exception as exc:
        logger.debug("Risk scenario construction failed: %s", exc)
        return rows

    for spec in specs:
        priority = (
            _PRI_HIGH if spec.label == "Aggressive" else
            _PRI_MEDIUM if spec.label == "Moderate" else _PRI_LOW
        )
        rows.append(_make_row(
            keys=ctx.keys, section="Risk Scenarios", item=spec.label,
            metric="scenario_signal", value=spec.signal,
            signal=spec.signal, priority=priority,
            notes=spec.notes,
            last_updated_riyadh=ctx.ts,
        ))

    return rows


# ---------------------------------------------------------------------------
# Default Universe Builder
# ---------------------------------------------------------------------------

def _default_universes() -> Dict[str, List[str]]:
    """Get default universes from environment variables or hardcoded fallbacks."""
    indices = _env_csv("TFB_INSIGHTS_INDICES", "TASI,NOMU,^GSPC,^IXIC,^FTSE")
    commodities = _env_csv("TFB_INSIGHTS_COMMODITIES_FX", "GC=F,BZ=F,USDSAR=X,EURUSD=X")
    return {
        "Indices & Benchmarks": indices,
        "Commodities & FX": commodities,
    }


# ---------------------------------------------------------------------------
# Main Builder
# ---------------------------------------------------------------------------

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
    """Build Insights_Analysis page rows."""
    headers, keys, schema_source = get_insights_schema()
    timestamp = _now_riyadh_iso()
    norm_criteria = _normalize_criteria(criteria)
    warnings: List[str] = []

    # Section flags
    do_market_summary = norm_criteria.get("include_market_summary", True)
    do_top_picks = include_top10_section and norm_criteria.get("include_top_opportunities", True)
    do_risk_alerts = True  # always on; cheap
    do_short_term = norm_criteria.get("include_short_term", True)
    do_portfolio_kpis = include_portfolio_kpis and norm_criteria.get("include_portfolio_health", True)
    do_risk_scenarios = norm_criteria.get("include_risk_scenarios", True)
    do_macro_signals = norm_criteria.get("include_macro_signals", True)

    ctx = BuildContext(
        keys=keys, ts=timestamp, norm_criteria=norm_criteria, warnings=warnings,
    )

    rows: List[Dict[str, Any]] = []

    if include_criteria_rows:
        rows.extend(build_criteria_rows(criteria=norm_criteria, last_updated_riyadh=timestamp))

    if include_system_rows:
        rows.append(_make_row(
            keys=keys, section="System", item="Builder Version",
            metric="insights_builder_version", value=INSIGHTS_BUILDER_VERSION,
            priority=_PRI_LOW,
            notes=f"core/analysis/insights_builder.py v{INSIGHTS_BUILDER_VERSION} -- 7-col schema (registry v2.5.0)",
            last_updated_riyadh=timestamp,
        ))

    # Resolve universes
    effective_universes: Dict[str, List[str]] = {}
    if universes:
        for name, seq in universes.items():
            sym_list = _dedupe_keep_order(seq or [])
            if sym_list:
                effective_universes[_safe_str(name) or "Universe"] = sym_list

    if not effective_universes and symbols:
        sym_list = _dedupe_keep_order(symbols or [])
        if sym_list:
            effective_universes["Selected Symbols"] = sym_list

    auto_used = False
    if not effective_universes and engine and auto_universe_when_empty:
        effective_universes.update(_default_universes())
        auto_used = True

    build_ok = bool(engine) and bool(effective_universes)

    rows.append(_make_row(
        keys=keys, section="System", item="Build Status",
        metric="build_status", value="OK" if build_ok else "WARN",
        signal=_SIGNAL_OK if build_ok else _SIGNAL_WARN, priority=_PRI_LOW,
        notes="OK = engine + universes available. WARN = criteria/system only",
        last_updated_riyadh=timestamp,
    ))

    # Early return: no engine or no universes
    if not engine or not effective_universes:
        msg = "No engine passed" if not engine else "No universes/symbols provided"
        rows.append(_make_row(
            keys=keys, section="System", item="Engine",
            metric="engine_status",
            value="Not provided" if not engine else "Universes Empty",
            signal=_SIGNAL_WARN, priority=_PRI_MEDIUM,
            notes=f"{msg}. Returning criteria/system rows only.",
            last_updated_riyadh=timestamp,
        ))
        if do_risk_scenarios:
            rows.extend(_build_risk_scenario_rows(ctx))
        return {
            "status": "partial",
            "page": "Insights_Analysis",
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "row_objects": rows,
            "rows_matrix": _rows_to_matrix(rows, keys),
            "meta": {
                "schema_source": schema_source,
                "schema_columns": len(keys),
                "generated_at_riyadh": timestamp,
                "engine_used": bool(engine),
                "auto_universe_used": auto_used,
                "universes": list(effective_universes.keys()),
                "mode": mode,
                "builder_version": INSIGHTS_BUILDER_VERSION,
                "criteria_snapshot": _criteria_snapshot(norm_criteria),
                "warnings": warnings,
            },
        }

    # Budget tracking
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(1.0, build_budget_sec)

    def _remaining() -> float:
        return deadline - loop.time()

    # Fetch quotes for all universes
    for section_name, sym_list in effective_universes.items():
        remaining = _remaining()
        if remaining <= 0.1:
            warnings.append(f"Skipped '{section_name}' -- budget exhausted")
            rows.append(_make_row(
                keys=keys, section="System", item="Warning",
                metric="build_budget_exhausted", value="WARN",
                signal=_SIGNAL_WARN, priority=_PRI_MEDIUM,
                notes=f"Skipped '{section_name}' -- build budget exhausted",
                last_updated_riyadh=timestamp,
            ))
            break

        syms = _dedupe_keep_order(sym_list or [])[:max_symbols_per_universe]
        try:
            quotes = await _fetch_quotes(
                engine, syms, mode=mode,
                timeout_sec=min(quotes_timeout_sec, remaining),
            )
        except Exception as exc:
            quotes = {}
            warnings.append(f"Quote fetch degraded for '{section_name}': {exc}")

        ctx.all_quotes.update(quotes)

        if do_market_summary:
            rows.extend(_build_universe_snapshot_rows(ctx, section_name, syms, quotes))

        if section_name.strip().lower() in {"my_portfolio", "portfolio", "my portfolio"}:
            ctx.portfolio_quotes = quotes
            ctx.portfolio_symbols = syms

    # Top Picks
    if do_top_picks and _remaining() > 0.25:
        top10_payload: Dict[str, Any] = {}
        try:
            top10_payload = await _fetch_top10_payload(
                engine, criteria=norm_criteria,
                limit=int(norm_criteria.get("top_n", 10)),
                mode=mode,
                timeout_sec=min(top10_timeout_sec, _remaining()),
            )
        except Exception as exc:
            warnings.append(f"Top Picks payload degraded: {exc}")

        if top10_payload:
            rows.extend(_build_top_picks_rows(ctx, top10_payload))
        else:
            # Symbols-only fallback
            top10_symbols = await _fetch_top10_symbols(
                engine, criteria=norm_criteria,
                limit=int(norm_criteria.get("top_n", 10)),
                timeout_sec=min(top10_timeout_sec, _remaining()),
            )
            if top10_symbols:
                try:
                    top10_quotes = await _fetch_quotes(
                        engine, top10_symbols, mode=mode,
                        timeout_sec=min(quotes_timeout_sec, _remaining()),
                    )
                    ctx.all_quotes.update(top10_quotes)
                except Exception:
                    top10_quotes = {}
                rows.extend(_build_top_picks_rows(
                    ctx, {"symbols": top10_symbols}, top10_quotes=top10_quotes,
                ))

    # Risk Alerts
    if do_risk_alerts and ctx.all_quotes:
        rows.extend(_build_risk_alert_rows(ctx, ctx.all_quotes))

    # Short-Term Opportunities
    if do_short_term and ctx.all_quotes:
        rows.extend(_build_short_term_rows(ctx, ctx.all_quotes))

    # Portfolio KPIs
    if do_portfolio_kpis:
        if ctx.portfolio_symbols and ctx.portfolio_quotes:
            rows.extend(_build_portfolio_kpis_rows(
                ctx, ctx.portfolio_symbols, ctx.portfolio_quotes,
            ))
        else:
            rows.append(_make_row(
                keys=keys, section="Portfolio KPIs", item="Status",
                metric="portfolio_status", value="Not Included",
                signal=_SIGNAL_OK, priority=_PRI_LOW,
                notes="My_Portfolio not in universes. Add My_Portfolio to pages_selected",
                last_updated_riyadh=timestamp,
            ))

    # Risk Scenarios
    if do_risk_scenarios:
        rows.extend(_build_risk_scenario_rows(ctx))

    # Macro Signals
    if do_macro_signals and ctx.all_quotes:
        rows.extend(_build_macro_signal_rows(ctx, ctx.all_quotes))

    # Prepend warnings banner if any
    if warnings:
        rows.insert(0, _make_row(
            keys=keys, section="System", item="Builder Warnings",
            metric="builder_warnings", value="WARN",
            signal=_SIGNAL_WARN, priority=_PRI_MEDIUM,
            notes=" | ".join(warnings[:3]),
            last_updated_riyadh=timestamp,
        ))

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
            "generated_at_riyadh": timestamp,
            "engine_used": True,
            "auto_universe_used": auto_used,
            "universes": list(effective_universes.keys()),
            "mode": mode,
            "builder_version": INSIGHTS_BUILDER_VERSION,
            "criteria_snapshot": _criteria_snapshot(norm_criteria),
            "warnings": warnings,
        },
    }


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    "INSIGHTS_BUILDER_VERSION",
    "get_insights_schema",
    "build_criteria_rows",
    "build_insights_analysis_rows",
]
