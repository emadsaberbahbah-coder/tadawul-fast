#!/usr/bin/env python3
"""
routes/ai_analysis.py
================================================================================
AI Analysis Routes — v4.2.0
================================================================================

v4.2.0 changes vs v4.1.0
--------------------------
FIX CRITICAL: _SchemaAdapter fallback keys/headers for Insights_Analysis updated
  to the canonical 9-column schema (schema_registry v3.4.0):
    OLD (v4.1.0): section/metric/value/unit/direction/commentary/updated_at (7 cols)
    NEW (v4.2.0): section/item/symbol/metric/value/signal/priority/notes/as_of_riyadh
  When schema_registry is unavailable, the fallback now produces valid 9-col rows
  instead of misaligned 7-col rows that fail the test_schema_alignment suite.

FIX: _build_insights_from_rows rewritten to produce v3.4.0 canonical fields:
  - added: item (display label), symbol ("" for aggregate rows), priority
  - added: as_of_riyadh (was updated_at)
  - replaced: direction → signal (Positive→BUY, Negative→SELL, Neutral→HOLD)
  - merged:   commentary + unit → notes (single field)
  - removed:  unit, direction, commentary, updated_at (old non-canonical fields)

FIX: _SchemaAdapter imports use multi-path fallback chains so the adapter
  resolves correctly regardless of module layout:
    core.sheets.schema_registry → core.schema_registry → schema_registry

ENH: _build_recommendations now initialises trade setup fields
  (entry_price, stop_loss_suggested, take_profit_suggested, risk_reward_ratio)
  to None so downstream Top10 schema projection never produces short rows.

Preserved from v4.1.0 (no other behavioral changes):
  _calc_score, _selection_reason, _criteria_snapshot, _load_source_rows,
  all route endpoints, request config parsing, engine resolution.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import importlib
import math
import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, HTTPException, Query


VERSION = "4.2.0"
ROUTE_FAMILY = "ai_analysis"
DEFAULT_ANALYSIS_SOURCE_PAGE = os.getenv("TFB_ANALYSIS_SOURCE_PAGE", "Market_Leaders")
DEFAULT_LIMIT  = max(1, min(int(os.getenv("TFB_AI_ANALYSIS_DEFAULT_LIMIT", "50")), 500))
MAX_LIMIT      = max(10, min(int(os.getenv("TFB_AI_ANALYSIS_MAX_LIMIT", "500")), 2000))
DEFAULT_TOP_N  = max(1, min(int(os.getenv("TFB_AI_ANALYSIS_TOP_N", "10")), 25))


router             = APIRouter(tags=["AI Analysis"])
router_hyphen      = APIRouter(prefix="/v1/ai-analysis",  tags=["AI Analysis"])
router_underscore  = APIRouter(prefix="/v1/ai_analysis",  tags=["AI Analysis"])


# =============================================================================
# Helpers
# =============================================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return float(value)
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text:
        return None
    try:
        number = float(text)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _import_attr_multi(module_names: Sequence[str], attr_name: str) -> Any:
    """Try each module in order; return first successful attr lookup."""
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
            attr = getattr(module, attr_name, None)
            if attr is not None:
                return attr
        except Exception:
            continue
    return None


def _import_attr(module_name: str, attr_name: str) -> Any:
    return _import_attr_multi([module_name], attr_name)


# =============================================================================
# Schema adapter — multi-path fallback
# v4.2.0: Insights_Analysis fallback schema updated to 9-col v3.4.0 canonical.
# =============================================================================
_SR_MODULES = (
    "core.sheets.schema_registry",
    "core.schema_registry",
    "schema_registry",
)

# v4.2.0: canonical 9-col Insights_Analysis schema (schema_registry v3.4.0)
_INSIGHTS_FALLBACK_KEYS: List[str] = [
    "section", "item", "symbol", "metric", "value",
    "signal", "priority", "notes", "as_of_riyadh",
]
_INSIGHTS_FALLBACK_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric", "Value",
    "Signal", "Priority", "Notes", "Last Updated (Riyadh)",
]

# Direction → Signal vocabulary mapping (v4.2.0)
_DIRECTION_TO_SIGNAL: Dict[str, str] = {
    "positive": "BUY",
    "very positive": "STRONG_BUY",
    "strong": "STRONG_BUY",
    "negative": "SELL",
    "very negative": "STRONG_SELL",
    "caution": "ALERT",
    "warning": "ALERT",
    "neutral": "HOLD",
    "mixed": "HOLD",
}

# Direction → Priority mapping (v4.2.0)
_DIRECTION_TO_PRIORITY: Dict[str, str] = {
    "positive":      "High",
    "very positive": "High",
    "strong":        "High",
    "negative":      "High",
    "very negative": "High",
    "caution":       "Medium",
    "warning":       "Medium",
    "neutral":       "Low",
    "mixed":         "Low",
}


def _direction_to_signal(direction: str) -> str:
    return _DIRECTION_TO_SIGNAL.get(direction.lower().strip(), "HOLD")


def _direction_to_priority(direction: str) -> str:
    return _DIRECTION_TO_PRIORITY.get(direction.lower().strip(), "Low")


@dataclass(frozen=True)
class RequestConfig:
    source_page:  str
    limit:        int
    include_rows: bool
    mode:         str
    symbols:      List[str]
    as_of:        str


class _SchemaAdapter:
    """
    v4.2.0: All schema_registry imports now use 3-path multi-fallback.
    Insights_Analysis fallback updated to 9-col v3.4.0 canonical schema.
    """
    def __init__(self) -> None:
        self._get_schema          = _import_attr_multi(_SR_MODULES, "get_schema")
        self._get_headers         = _import_attr_multi(_SR_MODULES, "get_headers") \
                                    or _import_attr_multi(_SR_MODULES, "get_sheet_headers")
        self._get_display_headers = _import_attr_multi(_SR_MODULES, "get_display_headers") \
                                    or _import_attr_multi(_SR_MODULES, "get_sheet_headers")
        self._get_keys            = _import_attr_multi(_SR_MODULES, "get_keys") \
                                    or _import_attr_multi(_SR_MODULES, "get_sheet_keys")
        self._project_rows        = _import_attr_multi(_SR_MODULES, "project_rows_to_schema")
        self._rows_to_matrix      = _import_attr_multi(_SR_MODULES, "rows_to_matrix")
        self._supported_pages     = _import_attr_multi(_SR_MODULES, "get_supported_pages") \
                                    or _import_attr_multi(_SR_MODULES, "list_sheets")

    def supported_pages(self) -> List[str]:
        if callable(self._supported_pages):
            try:
                return list(self._supported_pages())
            except Exception:
                pass
        return ["Insights_Analysis", "Top_10_Investments", "Market_Leaders",
                "My_Portfolio", "Global_Markets", "Commodities_FX", "Mutual_Funds"]

    def keys(self, page: str) -> List[str]:
        if callable(self._get_keys):
            try:
                result = list(self._get_keys(page))
                if result:
                    return result
            except Exception:
                pass
        # v4.2.0: 9-col canonical fallback for Insights_Analysis
        if page == "Insights_Analysis":
            return list(_INSIGHTS_FALLBACK_KEYS)
        return ["section", "metric", "value", "unit", "direction", "commentary", "updated_at"]

    def headers(self, page: str) -> List[str]:
        if callable(self._get_headers):
            try:
                result = list(self._get_headers(page))
                if result:
                    return result
            except Exception:
                pass
        # v4.2.0: 9-col canonical fallback for Insights_Analysis
        if page == "Insights_Analysis":
            return list(_INSIGHTS_FALLBACK_HEADERS)
        return self.keys(page)

    def display_headers(self, page: str) -> List[str]:
        if callable(self._get_display_headers):
            try:
                result = list(self._get_display_headers(page))
                if result:
                    return result
            except Exception:
                pass
        # v4.2.0: 9-col canonical fallback for Insights_Analysis
        if page == "Insights_Analysis":
            return list(_INSIGHTS_FALLBACK_HEADERS)
        return self.headers(page)

    def normalize_page(self, page: Optional[str]) -> str:
        if not page:
            return DEFAULT_ANALYSIS_SOURCE_PAGE
        if callable(self._get_schema):
            try:
                schema = self._get_schema(page)
                return str(getattr(schema, "page", page))
            except Exception:
                pass
        requested = str(page).strip()
        supported = {p.lower(): p for p in self.supported_pages()}
        return supported.get(requested.lower(), requested)

    def project_rows(self, page: str, rows: Iterable[Any]) -> List[Dict[str, Any]]:
        rows_list = list(rows or [])
        if callable(self._project_rows):
            try:
                return list(self._project_rows(page, rows_list))
            except Exception:
                pass
        keys = self.keys(page)
        out: List[Dict[str, Any]] = []
        for row in rows_list:
            if isinstance(row, Mapping):
                projected = {key: row.get(key) for key in keys}
            elif isinstance(row, (list, tuple)):
                projected = {key: row[i] if i < len(row) else None for i, key in enumerate(keys)}
            else:
                projected = {key: None for key in keys}
            out.append(projected)
        return out

    def rows_to_matrix(self, page: str, rows: Iterable[Any]) -> List[List[Any]]:
        rows_list = list(rows or [])
        if callable(self._rows_to_matrix):
            try:
                return list(self._rows_to_matrix(page, rows_list))
            except Exception:
                pass
        keys      = self.keys(page)
        projected = self.project_rows(page, rows_list)
        return [[row.get(key) for key in keys] for row in projected]


SCHEMA = _SchemaAdapter()


# =============================================================================
# Request parsing
# =============================================================================
def _parse_symbol_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts: List[str] = []
        for item in value:
            parts.extend(_parse_symbol_list(item))
        return parts
    text = str(value).replace(";", ",").replace("|", ",")
    out: List[str] = []
    for part in text.split(","):
        item = part.strip()
        if item:
            out.append(item)
    seen:   set       = set()
    unique: List[str] = []
    for item in out:
        key = item.upper()
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def _parse_config(
    page:         Optional[str],
    limit:        Optional[int],
    include_rows: Optional[bool],
    mode:         Optional[str],
    symbols:      Any,
    payload:      Optional[Mapping[str, Any]],
) -> RequestConfig:
    payload = dict(payload or {})
    source_page = SCHEMA.normalize_page(
        _coalesce(
            page,
            payload.get("page"), payload.get("sheet"), payload.get("sheet_name"),
            payload.get("name"), payload.get("tab"),
            payload.get("source_page"), payload.get("source"),
            DEFAULT_ANALYSIS_SOURCE_PAGE,
        )
    )
    raw_limit = _coalesce(limit, payload.get("limit"), payload.get("top_n"), DEFAULT_LIMIT)
    try:
        resolved_limit = int(raw_limit)
    except Exception:
        resolved_limit = DEFAULT_LIMIT
    resolved_limit = max(1, min(resolved_limit, MAX_LIMIT))

    resolved_include_rows = bool(
        _coalesce(include_rows, payload.get("include_rows"), payload.get("include_data"), True)
    )
    resolved_mode    = _clean_text(_coalesce(mode, payload.get("mode"), "analysis")) or "analysis"
    resolved_symbols = _parse_symbol_list(
        _coalesce(symbols, payload.get("symbols"), payload.get("tickers"), payload.get("direct_symbols"))
    )
    return RequestConfig(
        source_page=source_page, limit=resolved_limit,
        include_rows=resolved_include_rows, mode=resolved_mode,
        symbols=resolved_symbols, as_of=_iso_now(),
    )


# =============================================================================
# Engine resolution
# =============================================================================
def _resolve_engine() -> Tuple[Optional[Any], str]:
    candidates: Sequence[Tuple[str, str]] = (
        ("core.data_engine_v2", "get_global_engine"),
        ("core.data_engine_v2", "get_engine"),
        ("core.data_engine_v2", "engine"),
        ("core.data_engine",    "get_global_engine"),
        ("core.data_engine",    "get_engine"),
        ("core.data_engine",    "engine"),
    )
    for module_name, attr_name in candidates:
        attr = _import_attr(module_name, attr_name)
        try:
            if callable(attr):
                engine = attr()
                if engine is not None:
                    return engine, f"{module_name}.{attr_name}"
            elif attr is not None:
                return attr, f"{module_name}.{attr_name}"
        except Exception:
            continue
    return None, "unavailable"


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


async def _engine_get_rows(
    page: str, config: RequestConfig
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    engine, engine_source = _resolve_engine()
    if engine is None:
        return [], {"engine_present": False, "engine_source": engine_source}

    payload_variants: List[Dict[str, Any]] = [
        {"page": page, "limit": config.limit, "symbols": config.symbols, "mode": config.mode},
        {"sheet_name": page, "limit": config.limit, "symbols": config.symbols, "mode": config.mode},
        {"name": page, "limit": config.limit, "symbols": config.symbols, "mode": config.mode},
    ]
    method_names = (
        "get_sheet_rows", "get_sheet_rows_sync", "build_sheet_rows",
        "build_page_rows", "get_rows", "sheet_rows",
    )

    for method_name in method_names:
        method = getattr(engine, method_name, None)
        if not callable(method):
            continue
        for pl in payload_variants:
            try:
                result = await _maybe_await(method(**pl))
            except TypeError:
                try:
                    result = await _maybe_await(method(page=page, limit=config.limit))
                except Exception:
                    continue
            except Exception:
                continue
            rows, meta = _normalize_engine_result(page, result)
            if rows or meta.get("schema_only"):
                meta.update({
                    "engine_present": True,
                    "engine_source": engine_source,
                    "engine_method": method_name,
                })
                return rows, meta

    return [], {"engine_present": True, "engine_source": engine_source, "engine_method": None}


def _normalize_engine_result(
    page: str, result: Any
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {"schema_only": False}
    if result is None:
        return [], meta

    keys = SCHEMA.keys(page)

    if isinstance(result, Mapping):
        meta_fields = result.get("meta") if isinstance(result.get("meta"), Mapping) else {}
        if meta_fields:
            meta.update(dict(meta_fields))

        candidate_rows = (
            result.get("rows") or result.get("data") or result.get("items")
            or result.get("results") or result.get("quotes") or []
        )
        if candidate_rows and isinstance(candidate_rows, list):
            if isinstance(candidate_rows[0], Mapping):
                return SCHEMA.project_rows(page, candidate_rows), meta
            if isinstance(candidate_rows[0], (list, tuple)):
                projected = [{key: row[i] if i < len(row) else None for i, key in enumerate(keys)} for row in candidate_rows]
                return projected, meta

        rows_matrix = result.get("rows_matrix")
        if rows_matrix and isinstance(rows_matrix, list) and isinstance(rows_matrix[0], (list, tuple)):
            projected = [{key: row[i] if i < len(row) else None for i, key in enumerate(keys)} for row in rows_matrix]
            return projected, meta

        if result.get("schema_only") or result.get("headers_only"):
            meta["schema_only"] = True
            return [], meta

        if any(k in result for k in keys):
            return SCHEMA.project_rows(page, [result]), meta

    if isinstance(result, list):
        if not result:
            return [], meta
        if isinstance(result[0], Mapping):
            return SCHEMA.project_rows(page, result), meta
        if isinstance(result[0], (list, tuple)):
            projected = [{key: row[i] if i < len(row) else None for i, key in enumerate(keys)} for row in result]
            return projected, meta

    return [], meta


# =============================================================================
# Scoring helpers
# =============================================================================
def _calc_score(row: Mapping[str, Any]) -> float:
    opportunity  = _safe_float(row.get("opportunity_score"))
    overall      = _safe_float(row.get("overall_score"))
    confidence   = _safe_float(row.get("confidence"))
    forecast_conf= _safe_float(row.get("forecast_confidence"))
    valuation    = _safe_float(row.get("valuation_score"))
    momentum     = _safe_float(row.get("momentum_score"))
    quality      = _safe_float(row.get("quality_score"))
    growth       = _safe_float(row.get("growth_score"))
    value        = _safe_float(row.get("value_score"))
    risk         = _safe_float(row.get("risk_score"))
    roi_3m       = _safe_float(row.get("expected_roi_3m"))
    roi_1m       = _safe_float(row.get("expected_roi_1m"))

    positives = [v for v in [opportunity, overall, confidence, forecast_conf, valuation, momentum, quality, growth, value] if v is not None]
    base = sum(positives) / max(1, len(positives)) if positives else 0.0
    if roi_3m is not None:
        base += roi_3m * 100 * 0.25
    elif roi_1m is not None:
        base += roi_1m * 100 * 0.15
    if risk is not None:
        base -= risk * 0.20
    return round(base, 4)


def _selection_reason(row: Mapping[str, Any]) -> str:
    parts: List[str] = []
    roi_3m      = _safe_float(row.get("expected_roi_3m"))
    confidence  = _safe_float(_coalesce(row.get("forecast_confidence"), row.get("confidence")))
    valuation   = _safe_float(row.get("valuation_score"))
    momentum    = _safe_float(row.get("momentum_score"))
    risk_bucket = _clean_text(row.get("risk_bucket"))
    recommendation = _clean_text(row.get("recommendation"))

    if roi_3m       is not None: parts.append(f"3M ROI {roi_3m:.2%}")
    if confidence   is not None: parts.append(f"confidence {confidence:.1f}")
    if valuation    is not None: parts.append(f"valuation {valuation:.1f}")
    if momentum     is not None: parts.append(f"momentum {momentum:.1f}")
    if risk_bucket:              parts.append(f"risk {risk_bucket}")
    if recommendation:           parts.append(f"signal {recommendation}")

    if not parts:
        symbol = _clean_text(row.get("symbol")) or "item"
        return f"Selected using blended fallback ranking for {symbol}."
    return " | ".join(parts[:5])


def _criteria_snapshot(row: Mapping[str, Any], blended_score: float) -> str:
    fields = [
        ("overall", _safe_float(row.get("overall_score"))),
        ("opp",     _safe_float(row.get("opportunity_score"))),
        ("risk",    _safe_float(row.get("risk_score"))),
        ("roi3m",   _safe_float(row.get("expected_roi_3m"))),
        ("conf",    _safe_float(_coalesce(row.get("forecast_confidence"), row.get("confidence")))),
    ]
    parts = [f"blend={blended_score:.2f}"]
    for key, value in fields:
        if value is None:
            continue
        if key == "roi3m":
            parts.append(f"{key}={value:.2%}")
        else:
            parts.append(f"{key}={value:.2f}")
    return "; ".join(parts)


def _nonempty_ratio(row: Mapping[str, Any]) -> float:
    if not row:
        return 0.0
    filled = sum(1 for value in row.values() if value not in (None, "", [], {}, ()))
    return round((filled / max(1, len(row))) * 100.0, 2)


# =============================================================================
# Data loading
# =============================================================================
async def _load_source_rows(
    config: RequestConfig,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows, meta = await _engine_get_rows(config.source_page, config)

    if config.symbols:
        wanted = {s.upper() for s in config.symbols}
        rows = [row for row in rows if _clean_text(row.get("symbol")).upper() in wanted]

    if config.limit and len(rows) > config.limit:
        rows = rows[:config.limit]

    meta.update({
        "source_page":        config.source_page,
        "requested_symbols":  config.symbols,
        "row_count":          len(rows),
    })
    return rows, meta


# =============================================================================
# Insights builder — v4.2.0: produces 9-col v3.4.0 canonical rows
# =============================================================================
def _build_insights_from_rows(
    source_page:  str,
    source_rows:  Sequence[Mapping[str, Any]],
    as_of:        str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def add(
        section:   str,
        item:      str,
        metric:    str,
        value:     Any,
        direction: str,
        commentary: str,
        unit:      str = "",
        symbol:    str = "",
    ) -> None:
        """
        v4.2.0: produces a canonical 9-col Insights_Analysis row:
          section, item, symbol, metric, value, signal, priority, notes, as_of_riyadh
        """
        sig      = _direction_to_signal(direction)
        priority = _direction_to_priority(direction)
        # Compose notes from commentary + unit context
        if unit:
            notes = f"{commentary} ({unit})" if commentary else unit
        else:
            notes = commentary
        out.append({
            "section":      section,
            "item":         item,          # display label (was metric in v4.1.0)
            "symbol":       symbol,        # empty for aggregate rows
            "metric":       metric,        # canonical metric key
            "value":        value,
            "signal":       sig,           # BUY / SELL / HOLD / ALERT / STRONG_BUY
            "priority":     priority,      # High / Medium / Low
            "notes":        notes,
            "as_of_riyadh": as_of,
        })

    if not source_rows:
        add("Coverage", "Rows Loaded", "row_count", 0, "Neutral",
            f"No source rows were available from {source_page}.", "rows")
        return SCHEMA.project_rows("Insights_Analysis", out)

    row_count = len(source_rows)
    scores    = [_calc_score(row) for row in source_rows]
    avg_score = round(sum(scores) / max(1, len(scores)), 2)
    avg_fill  = round(sum(_nonempty_ratio(row) for row in source_rows) / max(1, row_count), 2)

    roi3_list       = [v for v in (_safe_float(row.get("expected_roi_3m"))            for row in source_rows) if v is not None]
    risk_list       = [v for v in (_safe_float(row.get("risk_score"))                 for row in source_rows) if v is not None]
    confidence_list = [v for v in (_safe_float(_coalesce(row.get("forecast_confidence"), row.get("confidence"))) for row in source_rows) if v is not None]

    best_row    = max(source_rows, key=_calc_score)
    best_symbol = _clean_text(best_row.get("symbol")) or _clean_text(best_row.get("name")) or "N/A"
    best_signal = _clean_text(_coalesce(best_row.get("recommendation"), best_row.get("signal"), "Watch"))

    # ── Coverage ──────────────────────────────────────────────────────────────
    add("Market Summary", "Rows Loaded", "row_count", row_count, "Neutral",
        f"AI analysis used {row_count} rows from {source_page}.", "rows")
    add("Market Summary", "Average Fill Ratio", "avg_fill_pct", avg_fill,
        "Positive" if avg_fill >= 60 else "Neutral",
        "Higher fill ratio means stronger input coverage for scoring.", "%")

    # ── Top Picks ────────────────────────────────────────────────────────────
    add("Top Picks", f"Best Candidate: {best_symbol}", "best_symbol",
        best_symbol, "Positive",
        f"Top blended candidate currently signals {best_signal}.",
        symbol=best_symbol)
    add("Top Picks", "Average Blended Score", "avg_blend_score", avg_score,
        "Positive" if avg_score >= 50 else "Neutral",
        "Average of fallback composite ranking across loaded rows.", "score")

    # ── Returns ───────────────────────────────────────────────────────────────
    if roi3_list:
        avg_roi3 = round(sum(roi3_list) / len(roi3_list), 4)
        add("Short-Term Signals", "Average Expected ROI 3M", "avg_roi_3m",
            round(avg_roi3 * 100.0, 2),
            "Positive" if avg_roi3 > 0 else "Negative",
            "Average expected 3-month return from available model fields.", "%")

    # ── Risk Alerts ───────────────────────────────────────────────────────────
    if risk_list:
        avg_risk = round(sum(risk_list) / len(risk_list), 2)
        add("Risk Alerts", "Average Risk Score", "avg_risk_score", avg_risk,
            "Positive" if avg_risk <= 40 else "Neutral" if avg_risk <= 60 else "Negative",
            "Lower risk score generally improves ranking stability.", "score")

    # ── Confidence ────────────────────────────────────────────────────────────
    if confidence_list:
        avg_conf = round(sum(confidence_list) / len(confidence_list), 2)
        add("Market Summary", "Average Confidence", "avg_confidence", avg_conf,
            "Positive" if avg_conf >= 60 else "Neutral",
            "Average confidence using forecast confidence or overall confidence when available.", "score")

    # ── Distribution ──────────────────────────────────────────────────────────
    risk_buckets: Dict[str, int] = {}
    rec_counts:   Dict[str, int] = {}
    for row in source_rows:
        rb = _clean_text(row.get("risk_bucket")) or "Unclassified"
        rc = _clean_text(_coalesce(row.get("recommendation"), row.get("signal"))) or "Unspecified"
        risk_buckets[rb] = risk_buckets.get(rb, 0) + 1
        rec_counts[rc]   = rec_counts.get(rc, 0) + 1

    top_risk_bucket    = max(risk_buckets.items(), key=lambda x: x[1])[0] if risk_buckets else "N/A"
    top_recommendation = max(rec_counts.items(),   key=lambda x: x[1])[0] if rec_counts   else "N/A"

    add("Risk Alerts", "Dominant Risk Bucket", "dominant_risk_bucket",
        top_risk_bucket, "Neutral",
        "Most frequent risk bucket across analyzed rows.", "bucket")
    add("Top Picks", "Dominant Recommendation", "dominant_recommendation",
        top_recommendation, "Neutral",
        "Most common recommendation / signal in the analyzed universe.", "signal")

    return SCHEMA.project_rows("Insights_Analysis", out)


# =============================================================================
# Recommendations builder
# =============================================================================
def _build_recommendations(
    source_rows: Sequence[Mapping[str, Any]], top_n: int
) -> List[Dict[str, Any]]:
    ranked: List[Tuple[float, Dict[str, Any]]] = []
    for row in source_rows:
        blended = _calc_score(row)
        base    = dict(row)
        base["_blended_score"] = blended
        ranked.append((blended, base))

    ranked.sort(
        key=lambda item: (
            item[0],
            _safe_float(item[1].get("expected_roi_3m"))  or -999999,
            _safe_float(item[1].get("confidence")) or _safe_float(item[1].get("forecast_confidence")) or -999999,
        ),
        reverse=True,
    )

    selected = ranked[:max(1, top_n)]
    out: List[Dict[str, Any]] = []
    for idx, (blended, row) in enumerate(selected, start=1):
        new_row = dict(row)
        new_row["top10_rank"]       = idx
        new_row["selection_reason"] = _selection_reason(row)
        new_row["criteria_snapshot"] = _criteria_snapshot(row, blended)
        # v4.2.0: init trade setup fields so schema projection never produces short rows
        for _ts_key in ("entry_price", "stop_loss_suggested", "take_profit_suggested", "risk_reward_ratio"):
            new_row.setdefault(_ts_key, None)
        out.append(new_row)
    return out


# =============================================================================
# Meta / payload builders
# =============================================================================
def _meta_block(
    config:      RequestConfig,
    source_meta: Mapping[str, Any],
    *,
    route:       str,
    row_count:   int,
    schema_page: str,
) -> Dict[str, Any]:
    return {
        "ok":               True,
        "route_family":     ROUTE_FAMILY,
        "route":            route,
        "version":          VERSION,
        "schema_page":      schema_page,
        "source_page":      config.source_page,
        "mode":             config.mode,
        "requested_limit":  config.limit,
        "row_count":        row_count,
        "requested_symbols": list(config.symbols),
        "generated_at_utc": config.as_of,
        **dict(source_meta or {}),
    }


def _sheet_rows_payload(
    schema_page: str,
    rows:        Sequence[Mapping[str, Any]],
    meta:        Mapping[str, Any],
) -> Dict[str, Any]:
    rows_list = SCHEMA.project_rows(schema_page, rows)
    return {
        "ok":              True,
        "page":            schema_page,
        "sheet_name":      schema_page,
        "keys":            SCHEMA.keys(schema_page),
        "headers":         SCHEMA.headers(schema_page),
        "display_headers": SCHEMA.display_headers(schema_page),
        "rows":            rows_list,
        "rows_matrix":     SCHEMA.rows_to_matrix(schema_page, rows_list),
        "meta":            dict(meta),
    }


def _analysis_payload(
    insights_rows: Sequence[Mapping[str, Any]],
    meta:          Mapping[str, Any],
    *,
    include_rows:  bool,
) -> Dict[str, Any]:
    rows_list = SCHEMA.project_rows("Insights_Analysis", insights_rows)
    payload: Dict[str, Any] = {
        "ok":       True,
        "page":     "Insights_Analysis",
        "insights": rows_list,
        "count":    len(rows_list),
        "meta":     dict(meta),
    }
    if include_rows:
        payload.update({
            "keys":            SCHEMA.keys("Insights_Analysis"),
            "headers":         SCHEMA.headers("Insights_Analysis"),
            "display_headers": SCHEMA.display_headers("Insights_Analysis"),
            "rows":            rows_list,
            "rows_matrix":     SCHEMA.rows_to_matrix("Insights_Analysis", rows_list),
        })
    return payload


def _recommendations_payload(
    rows:         Sequence[Mapping[str, Any]],
    meta:         Mapping[str, Any],
    *,
    include_rows: bool,
) -> Dict[str, Any]:
    rows_list = SCHEMA.project_rows("Top_10_Investments", rows)
    payload: Dict[str, Any] = {
        "ok":              True,
        "page":            "Top_10_Investments",
        "recommendations": rows_list,
        "count":           len(rows_list),
        "meta":            dict(meta),
    }
    if include_rows:
        payload.update({
            "keys":            SCHEMA.keys("Top_10_Investments"),
            "headers":         SCHEMA.headers("Top_10_Investments"),
            "display_headers": SCHEMA.display_headers("Top_10_Investments"),
            "rows":            rows_list,
            "rows_matrix":     SCHEMA.rows_to_matrix("Top_10_Investments", rows_list),
        })
    return payload


# =============================================================================
# Analysis runners
# =============================================================================
async def _run_analysis(config: RequestConfig) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    source_rows, source_meta = await _load_source_rows(config)
    insights = _build_insights_from_rows(config.source_page, source_rows, config.as_of)
    meta = _meta_block(config, source_meta, route="run", row_count=len(insights), schema_page="Insights_Analysis")
    meta["source_rows_used"] = len(source_rows)
    return insights, meta


async def _run_recommendations(
    config: RequestConfig, top_n: Optional[int] = None
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    source_rows, source_meta = await _load_source_rows(config)
    chosen_n        = max(1, min(int(top_n or DEFAULT_TOP_N), 50))
    recommendations = _build_recommendations(source_rows, chosen_n)
    meta = _meta_block(config, source_meta, route="recommendations", row_count=len(recommendations), schema_page="Top_10_Investments")
    meta["source_rows_used"] = len(source_rows)
    meta["top_n"]            = chosen_n
    return recommendations, meta


# =============================================================================
# Routes
# =============================================================================
@router_hyphen.get("/health")
@router_underscore.get("/health")
def health() -> Dict[str, Any]:
    engine, engine_source = _resolve_engine()
    return {
        "ok":              True,
        "service":         "AI Analysis Routes",
        "route_family":    ROUTE_FAMILY,
        "version":         VERSION,
        "engine_present":  engine is not None,
        "engine_source":   engine_source,
        "supported_pages": SCHEMA.supported_pages(),
        "generated_at_utc": _iso_now(),
    }


@router_hyphen.get("/metrics")
@router_underscore.get("/metrics")
def metrics() -> Dict[str, Any]:
    return {
        "ok":                True,
        "route_family":      ROUTE_FAMILY,
        "version":           VERSION,
        "default_source_page": DEFAULT_ANALYSIS_SOURCE_PAGE,
        "default_limit":     DEFAULT_LIMIT,
        "max_limit":         MAX_LIMIT,
        "default_top_n":     DEFAULT_TOP_N,
        "generated_at_utc":  _iso_now(),
    }


@router_hyphen.get("/run")
@router_underscore.get("/run")
async def run_get(
    page:         Optional[str] = Query(default=None),
    limit:        Optional[int] = Query(default=None),
    include_rows: bool          = Query(default=True),
    mode:         Optional[str] = Query(default="analysis"),
    symbols:      Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    config = _parse_config(page, limit, include_rows, mode, symbols, payload=None)
    insights, meta = await _run_analysis(config)
    return _analysis_payload(insights, meta, include_rows=config.include_rows)


@router_hyphen.post("/run")
@router_underscore.post("/run")
async def run_post(payload: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    config = _parse_config(None, None, None, None, None, payload=payload)
    insights, meta = await _run_analysis(config)
    return _analysis_payload(insights, meta, include_rows=config.include_rows)


@router_hyphen.get("/insights")
@router_underscore.get("/insights")
async def insights_get(
    page:         Optional[str] = Query(default=None),
    limit:        Optional[int] = Query(default=None),
    include_rows: bool          = Query(default=True),
    mode:         Optional[str] = Query(default="analysis"),
    symbols:      Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    config = _parse_config(page, limit, include_rows, mode, symbols, payload=None)
    insights, meta = await _run_analysis(config)
    meta["route"] = "insights"
    return _analysis_payload(insights, meta, include_rows=config.include_rows)


@router_hyphen.post("/insights")
@router_underscore.post("/insights")
async def insights_post(payload: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    config = _parse_config(None, None, None, None, None, payload=payload)
    insights, meta = await _run_analysis(config)
    meta["route"] = "insights"
    return _analysis_payload(insights, meta, include_rows=config.include_rows)


@router_hyphen.get("/recommendations")
@router_underscore.get("/recommendations")
async def recommendations_get(
    page:         Optional[str]   = Query(default=None),
    limit:        Optional[int]   = Query(default=None),
    include_rows: bool            = Query(default=True),
    mode:         Optional[str]   = Query(default="analysis"),
    symbols:      Optional[str]   = Query(default=None),
    top_n:        Optional[int]   = Query(default=None),
) -> Dict[str, Any]:
    config = _parse_config(page, limit, include_rows, mode, symbols, payload=None)
    rows, meta = await _run_recommendations(config, top_n=top_n)
    return _recommendations_payload(rows, meta, include_rows=config.include_rows)


@router_hyphen.post("/recommendations")
@router_underscore.post("/recommendations")
async def recommendations_post(payload: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    config = _parse_config(None, None, None, None, None, payload=payload)
    rows, meta = await _run_recommendations(config, top_n=payload.get("top_n"))
    return _recommendations_payload(rows, meta, include_rows=config.include_rows)


@router_hyphen.get("/sheet-rows")
@router_underscore.get("/sheet-rows")
@router_hyphen.get("/sheet_rows")
@router_underscore.get("/sheet_rows")
async def sheet_rows_get(
    page:          Optional[str] = Query(default=None),
    limit:         Optional[int] = Query(default=None),
    include_rows:  bool          = Query(default=True),
    mode:          Optional[str] = Query(default="analysis"),
    symbols:       Optional[str] = Query(default=None),
    analysis_page: Optional[str] = Query(default="Insights_Analysis"),
    top_n:         Optional[int] = Query(default=None),
) -> Dict[str, Any]:
    config      = _parse_config(page, limit, include_rows, mode, symbols, payload=None)
    target_page = _norm_text(analysis_page)
    if target_page in {"top_10_investments", "top10", "recommendations", "top_10"}:
        rows, meta = await _run_recommendations(config, top_n=top_n)
        meta["route"] = "sheet-rows"
        return _sheet_rows_payload("Top_10_Investments", rows, meta)
    insights, meta = await _run_analysis(config)
    meta["route"] = "sheet-rows"
    return _sheet_rows_payload("Insights_Analysis", insights, meta)


@router_hyphen.post("/sheet-rows")
@router_underscore.post("/sheet-rows")
@router_hyphen.post("/sheet_rows")
@router_underscore.post("/sheet_rows")
async def sheet_rows_post(payload: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    config      = _parse_config(None, None, None, None, None, payload=payload)
    target_page = _norm_text(_coalesce(payload.get("analysis_page"), payload.get("target_page"), "Insights_Analysis"))
    if target_page in {"top_10_investments", "top10", "recommendations", "top_10"}:
        rows, meta = await _run_recommendations(config, top_n=payload.get("top_n"))
        meta["route"] = "sheet-rows"
        return _sheet_rows_payload("Top_10_Investments", rows, meta)
    insights, meta = await _run_analysis(config)
    meta["route"] = "sheet-rows"
    return _sheet_rows_payload("Insights_Analysis", insights, meta)


@router_hyphen.get("/top10")
@router_underscore.get("/top10")
async def top10_get(
    page:         Optional[str] = Query(default=None),
    limit:        Optional[int] = Query(default=None),
    include_rows: bool          = Query(default=True),
    mode:         Optional[str] = Query(default="analysis"),
    symbols:      Optional[str] = Query(default=None),
    top_n:        Optional[int] = Query(default=None),
) -> Dict[str, Any]:
    config = _parse_config(page, limit, include_rows, mode, symbols, payload=None)
    rows, meta = await _run_recommendations(config, top_n=top_n)
    meta["route"] = "top10"
    return _recommendations_payload(rows, meta, include_rows=config.include_rows)


@router_hyphen.post("/top10")
@router_underscore.post("/top10")
async def top10_post(payload: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    config = _parse_config(None, None, None, None, None, payload=payload)
    rows, meta = await _run_recommendations(config, top_n=payload.get("top_n"))
    meta["route"] = "top10"
    return _recommendations_payload(rows, meta, include_rows=config.include_rows)


# Canonical export
router.include_router(router_hyphen)
router.include_router(router_underscore)


__all__ = [
    "VERSION", "ROUTE_FAMILY",
    "router", "router_hyphen", "router_underscore",
    "health", "metrics",
    "run_get", "run_post",
    "insights_get", "insights_post",
    "recommendations_get", "recommendations_post",
    "sheet_rows_get", "sheet_rows_post",
    "top10_get", "top10_post",
]
