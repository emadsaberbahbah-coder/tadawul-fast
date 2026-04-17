#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/enriched_quote.py
===============================================================================
TFB Enriched Quote Core — v3.4.0
===============================================================================
CORE-ONLY • SCHEMA-FIRST • PAGE-CANONICAL • SPECIAL-PAGE SAFE • ENGINE-TOLERANT
JSON-SAFE • SYNC/ASYNC SAFE • ROUTE-FRIENDLY • LIGHTWEIGHT • IMPORT-SAFE

Why this revision
-----------------
- FIX: defines rows->matrix conversion used by the response envelope.
- FIX: adds engine auto-resolution from request/app state and tolerant module
  factories / singletons when the caller does not pass an engine explicitly.
- FIX: preserves known-page contracts even when schema_registry is unavailable by
  falling back to canonical contracts from core.data_engine_v2 when available.
- FIX: special pages use builder-first execution, then engine sheet-row fallback,
  while remaining schema-safe for Top10 / Insights / Data Dictionary.
- FIX: instrument symbol requests can recover through sheet-row execution if
  quote methods are unavailable or return sparse payloads.
- FIX: normalizes rows from dicts, matrices, dataclasses, pydantic models, and
  mixed envelopes more consistently.
- SAFE: no network calls at import time, no route publishing here.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import importlib
import inspect
import logging
import math
import time
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger("core.enriched_quote")
logger.addHandler(logging.NullHandler())

MODULE_VERSION = "3.4.0"

# -----------------------------------------------------------------------------
# Canonical page groups
# -----------------------------------------------------------------------------
INSTRUMENT_PAGES = {
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
}

SPECIAL_PAGES = {
    "Top_10_Investments",
    "Insights_Analysis",
    "Data_Dictionary",
}

DEFAULT_PAGE = "Market_Leaders"
DEFAULT_LIMIT = 50
DEFAULT_TIMEOUT_SEC = 25.0
SPARSE_ROW_THRESHOLD = 8

# -----------------------------------------------------------------------------
# Fallback aliases (used only if page_catalog is unavailable)
# -----------------------------------------------------------------------------
_PAGE_ALIASES = {
    "market_leaders": "Market_Leaders",
    "market-leaders": "Market_Leaders",
    "global_markets": "Global_Markets",
    "global-markets": "Global_Markets",
    "commodities_fx": "Commodities_FX",
    "commodities-fx": "Commodities_FX",
    "commodities_and_fx": "Commodities_FX",
    "mutual_funds": "Mutual_Funds",
    "mutual-funds": "Mutual_Funds",
    "my_portfolio": "My_Portfolio",
    "my-portfolio": "My_Portfolio",
    "my_investments": "My_Portfolio",
    "my-investments": "My_Portfolio",
    "top_10_investments": "Top_10_Investments",
    "top-10-investments": "Top_10_Investments",
    "top10": "Top_10_Investments",
    "insights_analysis": "Insights_Analysis",
    "insights-analysis": "Insights_Analysis",
    "data_dictionary": "Data_Dictionary",
    "data-dictionary": "Data_Dictionary",
}

# -----------------------------------------------------------------------------
# Engine / service method candidates
# -----------------------------------------------------------------------------
_BATCH_QUOTE_METHODS = (
    "get_enriched_quotes_batch",
    "get_analysis_quotes_batch",
    "get_quotes_batch",
    "quotes_batch",
    "get_enriched_quotes",
    "get_quotes",
)

_SINGLE_QUOTE_METHODS = (
    "get_enriched_quote_dict",
    "get_quote_dict",
    "get_analysis_row_dict",
    "get_enriched_quote",
    "get_quote",
    "get_analysis_row",
)

_SHEET_ROW_METHODS = (
    "get_sheet_rows",
    "get_page_rows",
    "sheet_rows",
    "build_sheet_rows",
    "get_sheet",
    "build_rows_for_sheet",
)

_SNAPSHOT_METHODS = (
    "get_cached_sheet_snapshot",
    "get_sheet_snapshot",
    "get_cached_sheet_rows",
)

_ENGINE_ATTR_CANDIDATES = (
    "engine",
    "data_engine",
    "data_engine_v2",
    "tfb_engine",
)

_ENGINE_MODULE_CANDIDATES = (
    "core.data_engine_v2",
    "core.data_engine",
)

_ENGINE_FACTORY_NAMES = (
    "get_engine",
    "get_data_engine",
    "build_engine",
    "create_engine",
    "get_or_create_engine",
)

_ENGINE_SINGLETON_NAMES = (
    "engine",
    "data_engine",
    "ENGINE",
    "DATA_ENGINE",
)

# -----------------------------------------------------------------------------
# Optional page catalog / schema registry
# -----------------------------------------------------------------------------
_catalog_normalize_page_name = None
_catalog_route_family        = None
_catalog_is_instrument_page  = None

for _pcat_path in ("core.sheets.page_catalog", "core.page_catalog", "page_catalog"):
    try:
        _pcat_mod = importlib.import_module(_pcat_path)
        _f1 = getattr(_pcat_mod, "get_route_family",    None)
        _f2 = getattr(_pcat_mod, "is_instrument_page",  None)
        _f3 = getattr(_pcat_mod, "normalize_page_name", None)
        if callable(_f3):
            _catalog_route_family        = _f1
            _catalog_is_instrument_page  = _f2
            _catalog_normalize_page_name = _f3
            break
    except Exception:
        continue
del _pcat_path

if _catalog_normalize_page_name is None:
    def _catalog_normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore[misc]
        return str(name or "").strip()

if _catalog_route_family is None:
    def _catalog_route_family(name: str) -> str:  # type: ignore[misc]
        if name == "Top_10_Investments":  return "top10"
        if name == "Insights_Analysis":   return "insights"
        if name == "Data_Dictionary":     return "dictionary"
        return "instrument"

if _catalog_is_instrument_page is None:
    def _catalog_is_instrument_page(name: str) -> bool:  # type: ignore[misc]
        return _catalog_route_family(name) == "instrument"

_get_sheet_spec = None
for _sreg_path in ("core.sheets.schema_registry", "core.schema_registry", "schema_registry"):
    try:
        _sreg_mod = importlib.import_module(_sreg_path)
        _fn = getattr(_sreg_mod, "get_sheet_spec", None)
        if callable(_fn):
            _get_sheet_spec = _fn
            break
    except Exception:
        continue
del _sreg_path

if _get_sheet_spec is None:
    def _get_sheet_spec(sheet_name: str) -> Any:  # type: ignore[misc]
        raise KeyError("schema_registry unavailable")


# -----------------------------------------------------------------------------
# Canonical contract fallbacks
# -----------------------------------------------------------------------------
def _title_header(key: str) -> str:
    txt = str(key or "").replace("_", " ").strip()
    if not txt:
        return ""
    words = []
    for part in txt.split():
        up = part.upper()
        if up in {"TTM", "ROI", "RSI14", "EPS", "EV", "P", "W", "D", "Y", "FX", "UTC"}:
            words.append(up)
        elif part.lower() in {"m", "d", "w", "y"}:
            words.append(part.upper())
        else:
            words.append(part.capitalize())
    return " ".join(words)


_FALLBACK_INSTRUMENT_KEYS: List[str] = [
    # Identity (8)
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    # Price base (10)
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    # Price new (1)
    "price_change_5d",
    # Volume (6)
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "volume_ratio",
    # Fundamentals equity (13)
    "beta_5y", "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio",
    "revenue_ttm", "revenue_growth_yoy", "gross_margin", "operating_margin",
    "profit_margin", "debt_to_equity", "free_cash_flow_ttm",
    # Fundamentals quality (2) — v3.4.0 new
    "roe", "roa",
    # Risk (8)
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    # Technicals (4) — v3.4.0 new
    "rsi_signal", "technical_score", "day_range_position", "atr_14",
    # Valuation (7)
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "valuation_score", "upside_pct",
    # Forecast (9)
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    # Scores (6)
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score",
    # Decision (8) — v3.4.0 new additions
    "analyst_rating", "target_price", "upside_downside_pct",
    "recommendation", "signal", "trend_1m", "trend_3m", "trend_12m",
    # Recommendation new (4) — v3.4.0 new
    "short_term_signal", "recommendation_reason", "invest_period_label", "horizon_days",
    # Rank (1)
    "rank_overall",
    # Portfolio light (6)
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    # Provenance (4)
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    # Provider-specific extras (preserved from v3.3.0)
    "primary_provider", "classification_source", "history_source",
    "fundamentals_source", "forecast_source", "source_page", "row_status", "degraded",
]

# v3.4.0: added trade setup fields from top10_selector v4.9.0
_FALLBACK_TOP10_EXTRA_KEYS = [
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
    "entry_price",
    "stop_loss_suggested",
    "take_profit_suggested",
    "risk_reward_ratio",
]

# v3.4.0: 9-col schema (schema_registry v3.4.0 / insights_builder v3.0.0)
# OLD (v3.3.0): section, item, metric, value, notes, source, sort_order  (7 cols)
# NEW (v3.4.0): section, item, symbol, metric, value, signal, priority, notes, as_of_riyadh (9 cols)
_FALLBACK_INSIGHTS_KEYS = [
    "section",
    "item",
    "symbol",      # v3.4.0: added
    "metric",
    "value",
    "signal",      # v3.4.0: was "source" / non-canonical
    "priority",    # v3.4.0: High / Medium / Low
    "notes",
    "as_of_riyadh",# v3.4.0: was sort_order / as_of_utc
]

_FALLBACK_DATA_DICTIONARY_KEYS = [
    "sheet",
    "group",
    "header",
    "key",
    "dtype",
    "fmt",
    "required",
    "source",
    "notes",
]


def _contract_from_keys(keys: Sequence[str]) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    keys_list = [str(k) for k in keys if _strip(k)]
    headers = [_title_header(k) or k for k in keys_list]
    columns = [
        {
            "group": None,
            "header": headers[idx],
            "key": key,
            "dtype": None,
            "fmt": None,
            "required": False,
            "source": None,
            "notes": None,
        }
        for idx, key in enumerate(keys_list)
    ]
    return headers, keys_list, list(headers), columns


def _canonical_contract_fallback(page: str) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    # Prefer contracts from data_engine_v2 if available.
    for module_name in ("core.data_engine_v2", "core.data_engine"):
        try:
            mod = importlib.import_module(module_name)
        except Exception:
            continue

        contracts = getattr(mod, "STATIC_CANONICAL_SHEET_CONTRACTS", None)
        if isinstance(contracts, Mapping) and page in contracts:
            entry = contracts.get(page) or {}
            headers = [str(x) for x in _as_list(entry.get("headers")) if _strip(x)]
            keys = [str(x) for x in _as_list(entry.get("keys")) if _strip(x)]
            if keys:
                display_headers = headers or [_title_header(k) or k for k in keys]
                columns = [
                    {
                        "group": None,
                        "header": display_headers[idx] if idx < len(display_headers) else (_title_header(key) or key),
                        "key": key,
                        "dtype": None,
                        "fmt": None,
                        "required": False,
                        "source": "static_canonical_contract",
                        "notes": None,
                    }
                    for idx, key in enumerate(keys)
                ]
                return headers or display_headers, keys, display_headers, columns

        if page in INSTRUMENT_PAGES:
            headers = [str(x) for x in _as_list(getattr(mod, "INSTRUMENT_CANONICAL_HEADERS", [])) if _strip(x)]
            keys = [str(x) for x in _as_list(getattr(mod, "INSTRUMENT_CANONICAL_KEYS", [])) if _strip(x)]
            if keys:
                display_headers = headers or [_title_header(k) or k for k in keys]
                columns = [
                    {
                        "group": None,
                        "header": display_headers[idx] if idx < len(display_headers) else (_title_header(key) or key),
                        "key": key,
                        "dtype": None,
                        "fmt": None,
                        "required": False,
                        "source": "instrument_canonical_contract",
                        "notes": None,
                    }
                    for idx, key in enumerate(keys)
                ]
                return headers or display_headers, keys, display_headers, columns

    if page == "Top_10_Investments":
        return _contract_from_keys(_FALLBACK_INSTRUMENT_KEYS + _FALLBACK_TOP10_EXTRA_KEYS)
    if page == "Insights_Analysis":
        return _contract_from_keys(_FALLBACK_INSIGHTS_KEYS)
    if page == "Data_Dictionary":
        headers = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
        keys = list(_FALLBACK_DATA_DICTIONARY_KEYS)
        columns = [
            {
                "group": None,
                "header": headers[idx],
                "key": key,
                "dtype": None,
                "fmt": None,
                "required": False,
                "source": "fallback_data_dictionary_contract",
                "notes": None,
            }
            for idx, key in enumerate(keys)
        ]
        return headers, keys, list(headers), columns
    if page in INSTRUMENT_PAGES:
        return _contract_from_keys(_FALLBACK_INSTRUMENT_KEYS)
    return _contract_from_keys(["status", "error"])


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------
def _strip(value: Any) -> str:
    if value is None:
        return ""
    try:
        s = str(value).strip()
        return "" if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return ""


def _truthy(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = _strip(value).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_int(value: Any, default: int) -> int:
    try:
        if value is None or value == "" or isinstance(value, bool):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _clean_mode(mode: Any) -> str:
    m = _strip(mode).lower()
    if not m:
        return ""
    if m in {"auto"}:
        return "auto"
    if m in {"live", "live_quotes", "quotes"}:
        return "live_quotes"
    if m in {"sheet", "live_sheet", "rows", "sheet_rows"}:
        return "live_sheet"
    if m in {"snapshot", "snapshots"}:
        return "snapshot"
    return m


def _infer_mode(page: str, symbols: Sequence[str], requested_mode: str) -> str:
    mode = _clean_mode(requested_mode)
    if mode and mode != "auto":
        return mode
    if page in SPECIAL_PAGES:
        return "live_sheet"
    if symbols:
        return "live_quotes"
    return "live_sheet"


def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, Decimal):
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return str(value)

    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if is_dataclass(value):
        try:
            return _json_safe(asdict(value))
        except Exception:
            return str(value)

    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass

    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _json_safe(value.dict())
    except Exception:
        pass

    try:
        if hasattr(value, "__dict__"):
            return _json_safe(vars(value))
    except Exception:
        pass

    try:
        return str(value)
    except Exception:
        return None


def _as_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Mapping):
        try:
            return dict(value)
        except Exception:
            return {}
    if is_dataclass(value):
        try:
            dumped = asdict(value)
            return dumped if isinstance(dumped, dict) else {}
        except Exception:
            return {}
    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            dumped = value.model_dump(mode="python")
            return dumped if isinstance(dumped, dict) else {}
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            dumped = value.dict()
            return dumped if isinstance(dumped, dict) else {}
    except Exception:
        pass
    try:
        d = vars(value)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    if isinstance(value, str):
        return [value]
    try:
        if isinstance(value, Iterable):
            return list(value)
    except Exception:
        pass
    return [value]


def _compact_dict(d: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, str) and not _strip(v):
            continue
        out[str(k)] = v
    return out


def _merge_dicts(*parts: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for part in parts:
        if isinstance(part, Mapping):
            out.update(dict(part))
    return out


def _request_id(request: Any, explicit: Optional[str] = None) -> str:
    if _strip(explicit):
        return _strip(explicit)
    try:
        if request is not None:
            headers = getattr(request, "headers", {}) or {}
            hdr = _strip(headers.get("X-Request-ID"))
            if hdr:
                return hdr
            st = getattr(request, "state", None)
            rid = _strip(getattr(st, "request_id", ""))
            if rid:
                return rid
    except Exception:
        pass
    try:
        import uuid
        return uuid.uuid4().hex[:12]
    except Exception:
        return "core-enriched"


def _extract_request_query(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    try:
        qp = getattr(request, "query_params", None)
        if qp is None:
            return {}
        data = dict(qp)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _extract_request_path_params(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    try:
        pp = getattr(request, "path_params", None)
        if pp is None:
            return {}
        data = dict(pp)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _extract_settings_dict(settings: Any) -> Dict[str, Any]:
    if settings is None:
        return {}
    if isinstance(settings, Mapping):
        return dict(settings)
    try:
        d = vars(settings)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _normalize_symbol(symbol: Any) -> str:
    s = _strip(symbol).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _normalize_symbols(value: Any) -> List[str]:
    if value is None:
        return []
    out: List[str] = []
    seen = set()

    def _push(v: Any) -> None:
        sym = _normalize_symbol(v)
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)

    if isinstance(value, str):
        raw = value.replace(";", ",").replace("\n", ",").replace("\t", ",")
        for part in raw.split(","):
            _push(part)
        return out

    if isinstance(value, (list, tuple, set)):
        for item in value:
            _push(item)
        return out

    _push(value)
    return out


def _extract_symbols(*sources: Mapping[str, Any], explicit_symbol: Any = None, explicit_symbols: Any = None) -> List[str]:
    out: List[str] = []
    seen = set()

    def _add_many(vals: List[str]) -> None:
        for v in vals:
            if v and v not in seen:
                seen.add(v)
                out.append(v)

    _add_many(_normalize_symbols(explicit_symbols))
    _add_many(_normalize_symbols(explicit_symbol))

    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in ("symbols", "tickers", "tickers_list", "symbol", "ticker", "code", "codes"):
            if key in source:
                _add_many(_normalize_symbols(source.get(key)))

    return out


def _normalize_page(raw: Any) -> str:
    text = _strip(raw)
    if not text:
        return DEFAULT_PAGE

    try:
        page = _catalog_normalize_page_name(text, allow_output_pages=True)
        page = _strip(page)
        if page:
            return page
    except TypeError:
        try:
            page = _catalog_normalize_page_name(text)
            page = _strip(page)
            if page:
                return page
        except Exception:
            pass
    except Exception:
        pass

    key = text.lower().replace(" ", "_")
    return _PAGE_ALIASES.get(key, text)


def _route_family(page: str) -> str:
    try:
        fam = _strip(_catalog_route_family(page))
        if fam:
            return fam
    except Exception:
        pass
    if page == "Top_10_Investments":
        return "top10"
    if page == "Insights_Analysis":
        return "insights"
    if page == "Data_Dictionary":
        return "dictionary"
    return "instrument"


def _is_instrument_page(page: str) -> bool:
    try:
        return bool(_catalog_is_instrument_page(page))
    except Exception:
        return page in INSTRUMENT_PAGES


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    key_list = [str(k) for k in keys if _strip(k)]
    if not key_list:
        return []
    out: List[List[Any]] = []
    for row in rows:
        rowd = dict(row or {})
        out.append([_json_safe(rowd.get(k)) for k in key_list])
    return out


def _page_schema(page: str) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    headers: List[str] = []
    keys: List[str] = []
    display_headers: List[str] = []
    columns: List[Dict[str, Any]] = []

    try:
        spec = _get_sheet_spec(page)
    except Exception:
        spec = None

    if spec is None:
        return _canonical_contract_fallback(page)

    cols = getattr(spec, "columns", None)
    if cols is None and isinstance(spec, Mapping):
        cols = spec.get("columns") or []

    for col in _as_list(cols):
        cd = col if isinstance(col, Mapping) else _as_dict(col)
        key = _strip(cd.get("key") or cd.get("field") or cd.get("name"))
        header = _strip(cd.get("header") or cd.get("label") or key)
        if not key:
            continue
        keys.append(key)
        headers.append(header or key)
        display_headers.append(header or key)
        columns.append(
            _json_safe(
                {
                    "group": _strip(cd.get("group")) or None,
                    "header": header or key,
                    "key": key,
                    "dtype": _strip(cd.get("dtype")) or None,
                    "fmt": _strip(cd.get("fmt")) or None,
                    "required": bool(cd.get("required")),
                    "source": _strip(cd.get("source")) or None,
                    "notes": _strip(cd.get("notes")) or None,
                }
            )
        )

    if not keys:
        return _canonical_contract_fallback(page)

    return headers, keys, display_headers or headers, columns


def _row_from_any(value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return {str(k): v for k, v in value.items()}
    d = _as_dict(value)
    return d if d else None


def _extract_rows_like(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []

    if isinstance(payload, list):
        rows = []
        for item in payload:
            rd = _row_from_any(item)
            if rd:
                rows.append(rd)
        return rows

    if isinstance(payload, Mapping):
        for key in ("row_objects", "records", "items", "rows", "data", "results", "quotes"):
            value = payload.get(key)
            if isinstance(value, list):
                rows = []
                for item in value:
                    rd = _row_from_any(item)
                    if rd:
                        rows.append(rd)
                if rows:
                    return rows

        for key in ("row", "quote"):
            value = payload.get(key)
            rd = _row_from_any(value)
            if rd:
                return [rd]

        if _looks_like_row(payload):
            rd = _row_from_any(payload)
            return [rd] if rd else []

        for key in ("payload", "result"):
            nested = payload.get(key)
            nested_rows = _extract_rows_like(nested)
            if nested_rows:
                return nested_rows

    return []


def _extract_matrix_like(payload: Any) -> Optional[List[List[Any]]]:
    if not isinstance(payload, Mapping):
        return None

    for key in ("rows_matrix", "matrix"):
        value = payload.get(key)
        if isinstance(value, list):
            return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]

    rows_value = payload.get("rows")
    if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
        return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows_value]

    for key in ("payload", "result", "data"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            mx = _extract_matrix_like(nested)
            if mx is not None:
                return mx

    return None


def _matrix_to_rows(matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    key_list = [str(k) for k in keys]
    for row in matrix:
        vals = list(row) if isinstance(row, (list, tuple)) else [row]
        item: Dict[str, Any] = {}
        for idx, key in enumerate(key_list):
            item[key] = vals[idx] if idx < len(vals) else None
        rows.append(item)
    return rows


def _looks_like_row(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    keys = {str(k).lower() for k in value.keys()}
    return bool(keys & {
        "symbol", "ticker", "name", "company_name", "current_price",
        "section", "metric", "header", "key"
    })


def _payload_meta(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    meta = payload.get("meta")
    return dict(meta) if isinstance(meta, Mapping) else {}


def _payload_status(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return "success" if payload is not None else "partial"
    return _strip(payload.get("status")) or "success"


def _payload_error(payload: Any) -> Optional[str]:
    if not isinstance(payload, Mapping):
        return None
    err = payload.get("error") or payload.get("detail") or payload.get("message")
    txt = _strip(err)
    return txt or None


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    result = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _call_variants(fn: Callable[..., Any], variants: Sequence[Dict[str, Any]], timeout_sec: float) -> Any:
    last_exc: Optional[Exception] = None
    for kwargs in variants:
        try:
            cleaned = _compact_dict(kwargs)
            if timeout_sec > 0:
                return await asyncio.wait_for(_call_maybe_async(fn, **cleaned), timeout=timeout_sec)
            return await _call_maybe_async(fn, **cleaned)
        except TypeError as e:
            last_exc = e
            continue
        except Exception as e:
            last_exc = e
            break
    if last_exc:
        raise last_exc
    return None


def _method_variants(
    *,
    page: str,
    body: Mapping[str, Any],
    symbols: Sequence[str],
    limit: int,
    mode: str,
    request: Any,
    settings: Any,
    schema_only: bool,
    headers_only: bool,
    table_mode: bool,
) -> List[Dict[str, Any]]:
    symbol = symbols[0] if symbols else None
    common = {
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "name": page,
        "tab": page,
        "body": dict(body),
        "request": request,
        "settings": settings,
        "limit": limit,
        "mode": mode,
        "schema_only": schema_only,
        "headers_only": headers_only,
        "table_mode": table_mode,
    }

    variants = [
        {**common, "symbols": list(symbols), "tickers": list(symbols), "symbol": symbol, "ticker": symbol},
        {**common, "symbols": list(symbols), "tickers": list(symbols)},
        {**common, "symbol": symbol, "ticker": symbol},
        {**common},
        {"page": page, "symbols": list(symbols), "limit": limit, "body": dict(body), "mode": mode},
        {"sheet": page, "symbols": list(symbols), "limit": limit, "body": dict(body), "mode": mode},
        {"page": page, "symbol": symbol, "limit": limit, "mode": mode},
        {"sheet": page, "symbol": symbol, "limit": limit, "mode": mode},
        {"body": dict(body), "limit": limit},
        {"symbol": symbol},
        {"symbols": list(symbols)},
        {},
    ]

    out: List[Dict[str, Any]] = []
    seen = set()
    for item in variants:
        sig = repr(sorted(_compact_dict(item).items(), key=lambda kv: kv[0]))
        if sig not in seen:
            seen.add(sig)
            out.append(item)
    return out


def _symbol_candidates(symbol: Any) -> List[str]:
    norm = _normalize_symbol(symbol)
    base = _strip(symbol)
    items = [base, base.upper(), norm, norm.upper()]
    if norm.endswith(".SR"):
        code = norm[:-3]
        items.extend([code, code.upper(), f"TADAWUL:{code}"])
    out: List[str] = []
    seen = set()
    for item in items:
        s = _strip(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _row_richness(row: Optional[Dict[str, Any]]) -> int:
    if not isinstance(row, dict):
        return 0
    important = (
        "symbol",
        "ticker",
        "name",
        "company_name",
        "current_price",
        "exchange",
        "currency",
        "country",
        "sector",
        "industry",
        "market_cap",
        "volume",
        "overall_score",
        "opportunity_score",
        "recommendation",
        "recommendation_reason",
        "selection_reason",
        "metric",
        "value",
        "header",
        "key",
    )
    score = 0
    for key in important:
        val = row.get(key)
        if val is None:
            continue
        if isinstance(val, str) and not _strip(val):
            continue
        score += 1
    return score


def _is_sparse_row(row: Optional[Dict[str, Any]]) -> bool:
    return _row_richness(row) < SPARSE_ROW_THRESHOLD


def _merge_payload_dicts(base: Optional[Dict[str, Any]], addon: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base or {})
    if not isinstance(addon, dict):
        return out
    for k, v in addon.items():
        if v is None:
            continue
        if isinstance(v, str) and not _strip(v):
            continue
        out[k] = v
    return out


def _schema_projection(row: Mapping[str, Any], keys: Sequence[str]) -> Dict[str, Any]:
    if not keys:
        return {str(k): _json_safe(v) for k, v in row.items()}
    out: Dict[str, Any] = {}
    rowd = dict(row or {})
    for key in keys:
        out[key] = _json_safe(rowd.get(key))
    return out


def _normalize_rows(rows: Sequence[Mapping[str, Any]], keys: Sequence[str], page: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        rd = dict(row or {})
        if "symbol" not in rd and "ticker" in rd:
            rd["symbol"] = rd.get("ticker")
        if "ticker" not in rd and "symbol" in rd:
            rd["ticker"] = rd.get("symbol")

        if page == "Top_10_Investments":
            if rd.get("top10_rank") is None and rd.get("rank_overall") is not None:
                rd["top10_rank"] = rd.get("rank_overall")
            if rd.get("selection_reason") in {None, ""}:
                reco   = _strip(rd.get("recommendation"))
                roi    = rd.get("expected_roi_3m")
                conf   = rd.get("forecast_confidence")
                tech   = rd.get("technical_score")
                st_sig = _strip(rd.get("short_term_signal"))
                reason_parts = [f"{reco or 'Candidate'}"]
                if roi  is not None: reason_parts.append(f"ROI 3M={roi}")
                if conf is not None: reason_parts.append(f"Confidence={conf}")
                if tech is not None: reason_parts.append(f"Tech={round(float(tech), 1)}")
                if st_sig:            reason_parts.append(f"ST={st_sig}")
                if reco or roi is not None or conf is not None:
                    rd["selection_reason"] = " | ".join(reason_parts)
            if rd.get("criteria_snapshot") in {None, ""}:
                rd["criteria_snapshot"] = None
            # v3.4.0: initialise trade setup fields to None if not provided upstream
            for _ts_key in ("entry_price", "stop_loss_suggested", "take_profit_suggested", "risk_reward_ratio"):
                if _ts_key not in rd:
                    rd[_ts_key] = None

        out.append(_schema_projection(rd, keys))
    return out


def _safe_status(status: Any, rows: Sequence[Mapping[str, Any]], headers: Sequence[str], error: Optional[str], warnings: Sequence[str]) -> str:
    st = _strip(status).lower()
    if st in {"success", "ok"}:
        return "success"
    if st in {"error", "failed", "fail"}:
        return "error"
    if rows or headers:
        return "partial" if error or warnings else "success"
    return "error" if error else "partial"


def _resolve_app_state_engine(request: Any) -> Any:
    try:
        app = getattr(request, "app", None)
        state = getattr(app, "state", None)
        if state is None:
            return None
        for name in _ENGINE_ATTR_CANDIDATES:
            obj = getattr(state, name, None)
            if obj is not None:
                return obj
    except Exception:
        return None
    return None


async def _resolve_engine(engine: Any, request: Any = None) -> Any:
    if engine is not None:
        return engine

    app_engine = _resolve_app_state_engine(request)
    if app_engine is not None:
        return app_engine

    for module_name in _ENGINE_MODULE_CANDIDATES:
        try:
            mod = importlib.import_module(module_name)
        except Exception:
            continue

        for name in _ENGINE_SINGLETON_NAMES:
            obj = getattr(mod, name, None)
            if obj is not None:
                return obj

        for name in _ENGINE_FACTORY_NAMES:
            fn = getattr(mod, name, None)
            if not callable(fn):
                continue
            try:
                result = await _call_variants(
                    fn,
                    [
                        {"request": request},
                        {},
                    ],
                    timeout_sec=DEFAULT_TIMEOUT_SEC,
                )
                if result is not None:
                    return result
            except Exception:
                continue

    return None


async def _call_special_builder(
    *,
    page: str,
    engine: Any,
    request: Any,
    settings: Any,
    body: Mapping[str, Any],
    limit: int,
    mode: str,
    timeout_sec: float,
) -> Any:
    merged_body = dict(body or {})
    criteria = merged_body.get("criteria") if isinstance(merged_body.get("criteria"), dict) else None

    def _variants() -> List[Dict[str, Any]]:
        base = {
            "engine": engine,
            "request": request,
            "settings": settings,
            "body": merged_body,
            "criteria": criteria,
            "page": page,
            "sheet": page,
            "mode": mode,
            "limit": limit,
        }
        return [
            {**base},
            {**base, "payload": merged_body},
            {
                "request": request,
                "settings": settings,
                "body": merged_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "engine": engine,
                "body": merged_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "body": merged_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "payload": merged_body,
                "limit": limit,
                "mode": mode,
            },
            {},
        ]

    candidates: List[Tuple[str, Sequence[str]]] = []
    if page == "Top_10_Investments":
        candidates = [
            ("core.analysis.top10_selector", (
                "build_top10_rows",
                "build_top10_output_rows",
                "build_top10_investments_rows",
                "select_top10",
                "select_top10_symbols",
            )),
            ("core.investment_advisor_engine", (
                "run_investment_advisor_engine",
                "run_advisor_engine",
            )),
            ("core.investment_advisor", (
                "run_investment_advisor",
                "run_advisor",
                "_run_investment_advisor_impl",
                "_run_advisor_impl",
            )),
        ]
    elif page == "Insights_Analysis":
        candidates = [
            ("core.analysis.insights_builder", (
                "build_insights_analysis_rows",
                "build_insights_rows",
                "build_insights_output_rows",
                "build_insights_analysis",
            )),
            ("core.investment_advisor_engine", (
                "run_investment_advisor_engine",
                "run_advisor_engine",
            )),
        ]
    elif page == "Data_Dictionary":
        candidates = [
            ("core.sheets.data_dictionary", (
                "build_data_dictionary_rows",
                "get_data_dictionary_rows",
                "generate_data_dictionary",
            )),
        ]

    for module_name, fn_names in candidates:
        try:
            mod = importlib.import_module(module_name)
        except Exception:
            continue

        for fn_name in fn_names:
            fn = getattr(mod, fn_name, None)
            if not callable(fn):
                continue
            try:
                result = await _call_variants(fn, _variants(), timeout_sec=timeout_sec)
            except Exception as exc:
                logger.warning("Special builder %s.%s failed: %s", module_name, fn_name, exc)
                continue

            if result is not None:
                return result

    if page == "Data_Dictionary":
        dd_rows = _build_data_dictionary_from_schema()
        if dd_rows:
            return {"status": "success", "rows": dd_rows, "meta": {"dispatch": "schema_fallback"}}

    return None


def _build_data_dictionary_from_schema() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page_names = list(INSTRUMENT_PAGES) + ["Top_10_Investments", "Insights_Analysis", "Data_Dictionary"]
    seen = set()

    for sheet_name in page_names:
        if sheet_name in seen:
            continue
        seen.add(sheet_name)
        headers, keys, _, columns = _page_schema(str(sheet_name))
        if columns:
            for col in columns:
                rows.append(
                    {
                        "sheet": str(sheet_name),
                        "group": col.get("group"),
                        "header": col.get("header"),
                        "key": col.get("key"),
                        "dtype": col.get("dtype"),
                        "fmt": col.get("fmt"),
                        "required": col.get("required"),
                        "source": col.get("source"),
                        "notes": col.get("notes"),
                    }
                )
            continue

        for idx, key in enumerate(keys):
            header = headers[idx] if idx < len(headers) else (_title_header(key) or key)
            rows.append(
                {
                    "sheet": str(sheet_name),
                    "group": None,
                    "header": header,
                    "key": key,
                    "dtype": None,
                    "fmt": None,
                    "required": False,
                    "source": "schema_fallback",
                    "notes": None,
                }
            )
    return rows


async def _engine_sheet_rows(
    *,
    engine: Any,
    page: str,
    body: Mapping[str, Any],
    symbols: Sequence[str],
    request: Any,
    settings: Any,
    mode: str,
    limit: int,
    schema_only: bool,
    headers_only: bool,
    table_mode: bool,
    timeout_sec: float,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str, Optional[str]]:
    if engine is None:
        return [], {}, "partial", "engine_unavailable"

    variants = _method_variants(
        page=page,
        body=body,
        symbols=symbols,
        limit=limit,
        mode=mode,
        request=request,
        settings=settings,
        schema_only=schema_only,
        headers_only=headers_only,
        table_mode=table_mode,
    )

    for method_name in _SHEET_ROW_METHODS + _SNAPSHOT_METHODS:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        try:
            payload = await _call_variants(fn, variants, timeout_sec=timeout_sec)
        except Exception:
            continue

        rows = _extract_rows_like(payload)
        if not rows:
            matrix = _extract_matrix_like(payload)
            if matrix is not None:
                _, keys, _, _ = _page_schema(page)
                rows = _matrix_to_rows(matrix, keys)

        meta = _payload_meta(payload)
        meta.setdefault("dispatch", f"engine.{method_name}")
        status = _payload_status(payload)
        error = _payload_error(payload)
        if rows or isinstance(payload, Mapping):
            return rows, meta, status, error

    return [], {"dispatch": "engine.sheet_rows_missing"}, "partial", "no_sheet_rows_returned"


async def _engine_quotes(
    *,
    engine: Any,
    page: str,
    symbols: Sequence[str],
    request: Any,
    settings: Any,
    body: Mapping[str, Any],
    mode: str,
    limit: int,
    timeout_sec: float,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str, Optional[str]]:
    if engine is None:
        return [], {"dispatch": "engine.missing"}, "partial", "engine_unavailable"

    if not symbols:
        return [], {"dispatch": "quotes.no_symbols"}, "success", None

    variants = _method_variants(
        page=page,
        body=body,
        symbols=symbols,
        limit=limit,
        mode=mode,
        request=request,
        settings=settings,
        schema_only=False,
        headers_only=False,
        table_mode=False,
    )

    quote_map: Dict[str, Dict[str, Any]] = {}
    meta: Dict[str, Any] = {}
    status = "success"
    error: Optional[str] = None

    def _put(sym_key: Any, row: Dict[str, Any]) -> None:
        for cand in _symbol_candidates(sym_key):
            if cand not in quote_map:
                quote_map[cand] = dict(row)
            else:
                quote_map[cand] = _merge_payload_dicts(quote_map[cand], row)

    def _get(sym: str) -> Dict[str, Any]:
        for cand in _symbol_candidates(sym):
            if cand in quote_map:
                return quote_map[cand]
        return {}

    # Batch first
    for method_name in _BATCH_QUOTE_METHODS:
        fn = getattr(engine, method_name, None)
        if not callable(fn):
            continue
        try:
            payload = await _call_variants(fn, variants, timeout_sec=timeout_sec)
        except Exception:
            continue

        meta = _merge_payload_dicts(meta, _payload_meta(payload))
        meta.setdefault("dispatch", f"engine.{method_name}")
        status = _payload_status(payload) or status
        error = _payload_error(payload) or error

        if isinstance(payload, Mapping):
            rows = _extract_rows_like(payload)
            if rows:
                for idx, row in enumerate(rows):
                    sym = _strip(row.get("symbol") or row.get("ticker"))
                    if not sym and idx < len(symbols):
                        sym = str(symbols[idx])
                    if sym:
                        _put(sym, row)
            else:
                for k, v in payload.items():
                    if isinstance(v, Mapping):
                        rd = dict(v)
                        if _looks_like_row(rd) or _strip(k):
                            _put(k, rd)
        elif isinstance(payload, list):
            for idx, item in enumerate(payload):
                row = _row_from_any(item)
                if not row:
                    continue
                sym = _strip(row.get("symbol") or row.get("ticker"))
                if not sym and idx < len(symbols):
                    sym = str(symbols[idx])
                if sym:
                    _put(sym, row)

        if all(_get(sym) and not _is_sparse_row(_get(sym)) for sym in symbols):
            break

    # Rehydrate sparse / missing rows with single methods
    sparse = [sym for sym in symbols if not _get(sym) or _is_sparse_row(_get(sym))]
    if sparse:
        for sym in sparse:
            best_row = _get(sym)
            for method_name in _SINGLE_QUOTE_METHODS:
                fn = getattr(engine, method_name, None)
                if not callable(fn):
                    continue
                per_variants = [
                    {"symbol": sym, "page": page, "sheet": page, "mode": mode, "request": request, "settings": settings},
                    {"ticker": sym, "page": page, "sheet": page, "mode": mode},
                    {"symbol": sym},
                    {"ticker": sym},
                ]
                try:
                    payload = await _call_variants(fn, per_variants, timeout_sec=timeout_sec)
                except Exception:
                    continue
                rows = _extract_rows_like(payload)
                row = rows[0] if rows else (_row_from_any(payload) if _looks_like_row(payload) else None)
                if row:
                    best_row = _merge_payload_dicts(best_row, row)
                    _put(sym, best_row)
                    break

    out_rows: List[Dict[str, Any]] = []
    for sym in symbols:
        row = _get(sym)
        if not row:
            row = {"symbol": sym, "ticker": sym, "error": "missing_row"}
        elif "symbol" not in row and "ticker" in row:
            row["symbol"] = row.get("ticker")
        elif "ticker" not in row and "symbol" in row:
            row["ticker"] = row.get("symbol")
        out_rows.append(row)

    if any(_strip(r.get("error")) for r in out_rows):
        status = "error" if all(_strip(r.get("error")) for r in out_rows) else "partial"

    return out_rows, meta, status, error


def _derive_headers_keys(
    *,
    page: str,
    rows: Sequence[Mapping[str, Any]],
    source_meta: Mapping[str, Any],
) -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    headers, keys, display_headers, columns = _page_schema(page)
    if keys:
        return headers, keys, display_headers or headers, columns

    meta_headers = source_meta.get("headers")
    meta_keys = source_meta.get("keys")
    meta_display = source_meta.get("display_headers")
    if isinstance(meta_headers, list) and isinstance(meta_keys, list) and meta_keys:
        return (
            [str(x) for x in meta_headers],
            [str(x) for x in meta_keys],
            [str(x) for x in (meta_display or meta_headers)],
            [],
        )

    seen = set()
    derived_keys: List[str] = []
    for row in rows:
        for key in row.keys():
            ks = str(key)
            if ks not in seen:
                seen.add(ks)
                derived_keys.append(ks)

    if not derived_keys:
        return _canonical_contract_fallback(page)

    derived_headers = [_title_header(k) or k for k in derived_keys]
    return derived_headers, derived_keys, list(derived_headers), []


def _envelope(
    *,
    page: str,
    route_family: str,
    headers: Sequence[str],
    keys: Sequence[str],
    display_headers: Sequence[str],
    columns: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    include_matrix: bool,
    request_id: str,
    started_at: float,
    mode: str,
    status: str,
    error: Optional[str],
    warnings: Sequence[str],
    meta_extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows_out = [dict(r) for r in rows]
    payload = {
        "status": status,
        "page": page,
        "sheet": page,
        "route_family": route_family,
        "headers": list(headers),
        "keys": list(keys),
        "display_headers": list(display_headers or headers),
        "sheet_headers": list(display_headers or headers),
        "column_headers": list(display_headers or headers),
        "columns": [_json_safe(c) for c in columns],
        "rows": rows_out,
        "rows_matrix": _rows_to_matrix(rows_out, keys) if include_matrix and keys else None,
        "data": rows_out,
        "items": rows_out,
        "quotes": rows_out,
        "count": len(rows_out),
        "error": error,
        "version": MODULE_VERSION,
        "request_id": request_id,
        "warnings": list(warnings),
        "meta": {
            "module": "core.enriched_quote",
            "module_version": MODULE_VERSION,
            "mode": mode,
            "count": len(rows_out),
            "duration_ms": round((time.perf_counter() - started_at) * 1000.0, 3) if started_at is not None else None,
            **(meta_extra or {}),
        },
    }

    if len(rows_out) == 1:
        payload["row"] = rows_out[0]
        payload["quote"] = rows_out[0]

    return _json_safe(payload)


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
async def build_enriched_quote_payload(
    engine: Any = None,
    request: Any = None,
    settings: Any = None,
    body: Optional[Mapping[str, Any]] = None,
    page: Optional[str] = None,
    symbol: Optional[str] = None,
    symbols: Optional[Sequence[str]] = None,
    mode: Optional[str] = None,
    limit: Optional[int] = None,
    schema_only: Optional[bool] = None,
    headers_only: Optional[bool] = None,
    include_matrix: Optional[bool] = None,
    table_mode: Optional[bool] = None,
    request_id: Optional[str] = None,
    **extra: Any,
) -> Dict[str, Any]:
    started_at = time.perf_counter()

    request_query = _extract_request_query(request)
    request_path = _extract_request_path_params(request)
    settings_dict = _extract_settings_dict(settings)
    body_dict = dict(body or {})
    extra_dict = dict(extra or {})

    merged = _merge_dicts(request_query, request_path, settings_dict, body_dict, extra_dict)

    page_norm = _normalize_page(
        page
        or merged.get("page")
        or merged.get("sheet")
        or merged.get("sheet_name")
        or merged.get("tab")
        or merged.get("name")
        or DEFAULT_PAGE
    )
    route_family = _route_family(page_norm)

    req_id = _request_id(request, request_id)
    limit_out = max(1, min(5000, int(limit) if isinstance(limit, int) and limit > 0 else _to_int(merged.get("limit"), DEFAULT_LIMIT)))
    schema_only_out = bool(schema_only) if isinstance(schema_only, bool) else _truthy(merged.get("schema_only"), False)
    headers_only_out = bool(headers_only) if isinstance(headers_only, bool) else _truthy(merged.get("headers_only"), False)
    include_matrix_out = bool(include_matrix) if isinstance(include_matrix, bool) else _truthy(merged.get("include_matrix"), True)
    table_mode_out = bool(table_mode) if isinstance(table_mode, bool) else _truthy(merged.get("table_mode"), False)

    resolved_symbols = _extract_symbols(body_dict, request_query, request_path, extra_dict, explicit_symbol=symbol, explicit_symbols=symbols)
    mode_out = _infer_mode(page_norm, resolved_symbols, mode or merged.get("mode"))

    warnings: List[str] = []
    rows: List[Dict[str, Any]] = []
    source_meta: Dict[str, Any] = {}
    status_out = "success"
    error_out: Optional[str] = None

    engine_obj = await _resolve_engine(engine, request)

    # Special pages first
    if page_norm in SPECIAL_PAGES:
        if schema_only_out or headers_only_out:
            headers, keys, display_headers, columns = _page_schema(page_norm)
            return _envelope(
                page=page_norm,
                route_family=route_family,
                headers=headers,
                keys=keys,
                display_headers=display_headers,
                columns=columns,
                rows=[],
                include_matrix=include_matrix_out,
                request_id=req_id,
                started_at=started_at,
                mode=mode_out,
                status="success",
                error=None,
                warnings=["schema_only_special_page"] if schema_only_out or headers_only_out else [],
                meta_extra={"schema_only": True, "headers_only": headers_only_out, "engine_present": engine_obj is not None},
            )

        try:
            special_payload = await _call_special_builder(
                page=page_norm,
                engine=engine_obj,
                request=request,
                settings=settings,
                body=merged,
                limit=limit_out,
                mode=mode_out,
                timeout_sec=DEFAULT_TIMEOUT_SEC,
            )
        except Exception as exc:
            special_payload = None
            warnings.append(f"special_builder_error:{type(exc).__name__}")

        rows = _extract_rows_like(special_payload)
        source_meta = _payload_meta(special_payload)
        status_out = _payload_status(special_payload)
        error_out = _payload_error(special_payload)

        if not rows:
            eng_rows, eng_meta, eng_status, eng_error = await _engine_sheet_rows(
                engine=engine_obj,
                page=page_norm,
                body=merged,
                symbols=resolved_symbols,
                request=request,
                settings=settings,
                mode=mode_out,
                limit=limit_out,
                schema_only=False,
                headers_only=False,
                table_mode=True,
                timeout_sec=DEFAULT_TIMEOUT_SEC,
            )
            rows = eng_rows
            source_meta = _merge_payload_dicts(source_meta, eng_meta)
            status_out = eng_status or status_out
            error_out = eng_error or error_out
            if not rows:
                warnings.append("special_page_engine_fallback_empty")

    # Instrument quote mode
    elif resolved_symbols and _is_instrument_page(page_norm):
        quote_rows, quote_meta, quote_status, quote_error = await _engine_quotes(
            engine=engine_obj,
            page=page_norm,
            symbols=resolved_symbols[:limit_out],
            request=request,
            settings=settings,
            body=merged,
            mode=mode_out,
            limit=limit_out,
            timeout_sec=DEFAULT_TIMEOUT_SEC,
        )
        rows = quote_rows
        source_meta = quote_meta
        status_out = quote_status
        error_out = quote_error

        if (not rows) or all(_is_sparse_row(r) for r in rows):
            sheet_rows, sheet_meta, sheet_status, sheet_error = await _engine_sheet_rows(
                engine=engine_obj,
                page=page_norm,
                body=merged,
                symbols=resolved_symbols,
                request=request,
                settings=settings,
                mode="live_sheet",
                limit=limit_out,
                schema_only=False,
                headers_only=False,
                table_mode=True,
                timeout_sec=DEFAULT_TIMEOUT_SEC,
            )
            if sheet_rows:
                rows = [_merge_payload_dicts(rows[idx] if idx < len(rows) else {}, row) for idx, row in enumerate(sheet_rows)]
                source_meta = _merge_payload_dicts(source_meta, sheet_meta)
                if sheet_status:
                    status_out = sheet_status
                error_out = sheet_error or error_out
                warnings.append("quotes_rehydrated_from_sheet_rows")

    # Table mode / page mode
    else:
        sheet_rows, sheet_meta, sheet_status, sheet_error = await _engine_sheet_rows(
            engine=engine_obj,
            page=page_norm,
            body=merged,
            symbols=resolved_symbols,
            request=request,
            settings=settings,
            mode=mode_out,
            limit=limit_out,
            schema_only=schema_only_out,
            headers_only=headers_only_out,
            table_mode=table_mode_out or (not resolved_symbols),
            timeout_sec=DEFAULT_TIMEOUT_SEC,
        )
        rows = sheet_rows
        source_meta = sheet_meta
        status_out = sheet_status
        error_out = sheet_error

    headers, keys, display_headers, columns = _derive_headers_keys(
        page=page_norm,
        rows=rows,
        source_meta=source_meta,
    )

    if schema_only_out or headers_only_out:
        rows = []
    else:
        rows = _normalize_rows(rows[:limit_out], keys, page_norm)

    if not rows and not (schema_only_out or headers_only_out):
        if error_out:
            warnings.append("no_rows_with_error")
        else:
            warnings.append("no_rows_returned")

    status_final = _safe_status(status_out, rows, headers, error_out, warnings)

    return _envelope(
        page=page_norm,
        route_family=route_family,
        headers=headers,
        keys=keys,
        display_headers=display_headers,
        columns=columns,
        rows=rows,
        include_matrix=include_matrix_out,
        request_id=req_id,
        started_at=started_at,
        mode=mode_out,
        status=status_final,
        error=error_out,
        warnings=warnings,
        meta_extra={
            "special_page": page_norm in SPECIAL_PAGES,
            "instrument_page": _is_instrument_page(page_norm),
            "requested_symbols": resolved_symbols,
            "schema_only": schema_only_out,
            "headers_only": headers_only_out,
            "table_mode": table_mode_out,
            "engine_present": engine_obj is not None,
            "engine_type": type(engine_obj).__name__ if engine_obj is not None else "none",
            "source_meta_keys": sorted(list(source_meta.keys())) if isinstance(source_meta, Mapping) else [],
        },
    )


async def build_enriched_sheet_rows_payload(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_enriched_quote_payload(*args, **kwargs)


async def get_enriched_quote_payload(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_enriched_quote_payload(*args, **kwargs)


async def enriched_quote(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_enriched_quote_payload(*args, **kwargs)


async def quote(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return await build_enriched_quote_payload(*args, **kwargs)


def build_enriched_quote_payload_sync(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(build_enriched_quote_payload(*args, **kwargs))
    raise RuntimeError(
        "build_enriched_quote_payload_sync() cannot run inside an active event loop. "
        "Use `await build_enriched_quote_payload(...)` instead."
    )


# Intentionally core-only; route wrapper owns HTTP path publishing.
router = None


def get_router() -> None:
    return None


__all__ = [
    "MODULE_VERSION",
    "build_enriched_quote_payload",
    "build_enriched_sheet_rows_payload",
    "build_enriched_quote_payload_sync",
    "get_enriched_quote_payload",
    "enriched_quote",
    "quote",
    "get_router",
    "router",
]
