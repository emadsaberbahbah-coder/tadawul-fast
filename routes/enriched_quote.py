#!/usr/bin/env python3
# routes/enriched_quote.py
"""
================================================================================
TFB Enriched Quote Routes Wrapper — v8.2.0
================================================================================
IMPORT-SAFE • MINIMAL-DEPENDENCY • ENRICHED-ALIAS OWNERSHIP SAFE
QUOTE + QUOTES + SHEET-ROWS ALIASES • BRIDGE-FIRST • FAIL-SOFT • JSON-SAFE

Why this revision
-----------------
- FIX: explicitly owns `/v1/enriched/sheet-rows`, `/v1/enriched_quote/sheet-rows`,
       and `/v1/enriched-quote/sheet-rows` in addition to quote/quotes aliases.
- FIX: removes fragile overlap with optional helper code by using a small,
       self-contained router with tolerant dynamic bridging.
- FIX: non-instrument pages bridge to canonical owners first, instead of stopping
       at partial owner hints.
- FIX: instrument quote/quotes mode stays available even if bridge targets are
       temporarily unavailable.
================================================================================
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import math
import os
import re
import time
import uuid
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.enriched_quote")
logger.addHandler(logging.NullHandler())

ROUTER_VERSION = "8.2.0"

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

_INSTRUMENT_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low",
    "Price Change", "Percent Change", "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D",
    "Market Cap", "Float Shares", "Beta (5Y)", "P/E (TTM)", "P/E (Forward)", "EPS (TTM)",
    "Dividend Yield", "Payout Ratio", "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin",
    "Operating Margin", "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)", "RSI (14)",
    "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)", "Sharpe (1Y)",
    "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value",
    "Valuation Score", "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence",
    "Confidence Score", "Confidence Bucket", "Value Score", "Quality Score", "Momentum Score",
    "Growth Score", "Overall Score", "Opportunity Score", "Rank (Overall)", "Recommendation",
    "Recommendation Reason", "Horizon Days", "Invest Period Label", "Position Qty", "Avg Cost",
    "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %", "Data Provider",
    "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
]

_INSTRUMENT_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low", "week_52_high",
    "week_52_low", "price_change", "percent_change", "week_52_position_pct", "volume",
    "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y", "pe_ttm",
    "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm", "revenue_growth_yoy",
    "gross_margin", "operating_margin", "profit_margin", "debt_to_equity", "free_cash_flow_ttm",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y",
    "risk_score", "risk_bucket", "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "valuation_score", "forecast_price_1m", "forecast_price_3m", "forecast_price_12m", "expected_roi_1m",
    "expected_roi_3m", "expected_roi_12m", "forecast_confidence", "confidence_score", "confidence_bucket",
    "value_score", "quality_score", "momentum_score", "growth_score", "overall_score", "opportunity_score",
    "rank_overall", "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    "position_qty", "avg_cost", "position_cost", "position_value", "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
]

_INSIGHTS_HEADERS = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
_INSIGHTS_KEYS = ["section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"]

_DICTIONARY_HEADERS = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
_DICTIONARY_KEYS = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "requested_symbol"],
    "ticker": ["symbol", "code", "instrument", "security", "requested_symbol"],
    "name": ["company_name", "long_name", "instrument_name", "security_name", "title"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "current", "spot", "nav", "value"],
    "recommendation_reason": ["reason", "reco_reason", "recommendation_notes", "rationale"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["selection_notes", "selector_reason", "reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
}

EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT", "QQQ", "GC=F"],
    "My_Investments": ["2222.SR", "AAPL", "MSFT"],
    "Insights_Analysis": ["2222.SR", "AAPL", "GC=F"],
    "Top_10_Investments": ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

router = APIRouter(tags=["enriched"])


def _strip(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
    except Exception:
        return ""
    return s if s and s.lower() not in {"none", "null", "undefined"} else ""


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, Decimal):
        try:
            f = float(value)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except Exception:
            return str(value)
    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        if hasattr(value, "model_dump"):
            return _json_safe(value.model_dump())  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        if hasattr(value, "dict"):
            return _json_safe(value.dict())  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        return _json_safe(vars(value))
    except Exception:
        return str(value)


def _to_dict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return dict(obj)
    if obj is None:
        return {}
    try:
        if hasattr(obj, "model_dump"):
            d = obj.model_dump()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    try:
        if hasattr(obj, "dict"):
            d = obj.dict()  # type: ignore[attr-defined]
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    try:
        d = vars(obj)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    result = await asyncio.to_thread(fn, *args, **kwargs)
    return await result if inspect.isawaitable(result) else result


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    rid = _strip(x_request_id)
    if rid:
        return rid
    try:
        rid = _strip(getattr(request.state, "request_id", ""))
        if rid:
            return rid
    except Exception:
        pass
    try:
        rid = _strip(request.headers.get("X-Request-ID"))
        if rid:
            return rid
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _bool_from_any(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            return bool(int(value))
        except Exception:
            return default
    s = _strip(value).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _int_from_any(value: Any, default: int) -> int:
    try:
        if value is None or isinstance(value, bool):
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        s = _strip(value)
        return int(float(s)) if s else default
    except Exception:
        return default


def _split_symbols(value: str) -> List[str]:
    raw = (value or "").replace(";", ",").replace("\n", ",").replace("\t", ",").replace("|", ",")
    out: List[str] = []
    seen = set()
    for part in raw.split(","):
        s = _strip(part)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _normalize_symbol_token(sym: Any) -> str:
    s = _strip(sym).upper().replace(" ", "")
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _list_from_body(body: Mapping[str, Any], *keys: str) -> List[str]:
    for key in keys:
        value = body.get(key)
        if isinstance(value, list):
            out: List[str] = []
            seen = set()
            for item in value:
                s = _normalize_symbol_token(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out
        if isinstance(value, str) and value.strip():
            vals = [_normalize_symbol_token(x) for x in _split_symbols(value)]
            vals = [x for x in vals if x]
            if vals:
                return vals
    return []


def _collect_sheet_body(**kwargs: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in kwargs.items():
        if v not in (None, ""):
            out[k] = v
    return out


def _page_from_body(body: Mapping[str, Any]) -> str:
    for key in ("page", "sheet", "sheet_name", "page_name", "worksheet", "name", "tab"):
        s = _strip(body.get(key))
        if s:
            return s
    return ""


def _normalize_key_name(name: Any) -> str:
    s = _strip(name)
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    out_headers: List[str] = []
    out_keys: List[str] = []
    for i in range(max_len):
        h = _strip(raw_headers[i]) if i < len(raw_headers) else ""
        k = _strip(raw_keys[i]) if i < len(raw_keys) else ""
        if h and not k:
            k = _normalize_key_name(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column {i + 1}"
            k = f"column_{i + 1}"
        out_headers.append(h)
        out_keys.append(k)
    return out_headers, out_keys


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs, ks = _complete_schema_contract(headers, keys)
    header_map = {
        "top10_rank": "Top10 Rank",
        "selection_reason": "Selection Reason",
        "criteria_snapshot": "Criteria Snapshot",
    }
    for key in TOP10_REQUIRED_FIELDS:
        if key not in ks:
            ks.append(key)
            hdrs.append(header_map[key])
    return _complete_schema_contract(hdrs, ks)


def _static_contract(page: str) -> Tuple[List[str], List[str]]:
    if page == "Insights_Analysis":
        return _complete_schema_contract(_INSIGHTS_HEADERS, _INSIGHTS_KEYS)
    if page == "Data_Dictionary":
        return _complete_schema_contract(_DICTIONARY_HEADERS, _DICTIONARY_KEYS)
    if page == "Top_10_Investments":
        return _ensure_top10_contract(_INSTRUMENT_HEADERS, _INSTRUMENT_KEYS)
    return _complete_schema_contract(_INSTRUMENT_HEADERS, _INSTRUMENT_KEYS)


def _extract_contract_from_schema(page: str) -> Tuple[List[str], List[str]]:
    for module_name in ("core.sheets.schema_registry",):
        try:
            mod = importlib.import_module(module_name)
            get_sheet_spec = getattr(mod, "get_sheet_spec", None)
            if callable(get_sheet_spec):
                spec = get_sheet_spec(page)
            else:
                spec = None
            headers: List[str] = []
            keys: List[str] = []
            cols = []
            if isinstance(spec, Mapping):
                cols = spec.get("columns") or spec.get("fields") or []
                if not cols:
                    headers = [_strip(x) for x in (spec.get("headers") or spec.get("display_headers") or []) if _strip(x)]
                    keys = [_strip(x) for x in (spec.get("keys") or spec.get("fields") or []) if _strip(x)]
            else:
                cols = getattr(spec, "columns", None) or getattr(spec, "fields", None) or []
            for c in cols or []:
                if isinstance(c, Mapping):
                    headers.append(_strip(c.get("header") or c.get("display_header") or c.get("label") or c.get("title")))
                    keys.append(_strip(c.get("key") or c.get("field") or c.get("name") or c.get("id")))
                else:
                    headers.append(_strip(getattr(c, "header", getattr(c, "display_header", getattr(c, "label", getattr(c, "title", None))))) )
                    keys.append(_strip(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None))))) )
            headers, keys = _complete_schema_contract(headers, keys)
            if headers and keys:
                if page == "Top_10_Investments":
                    return _ensure_top10_contract(headers, keys)
                return headers, keys
        except Exception:
            continue
    return _static_contract(page)


def _key_variants(key: str) -> List[str]:
    k = _strip(key)
    if not k:
        return []
    variants = [k, k.lower(), k.upper(), k.replace("_", " "), k.replace("_", "").lower()]
    for alias in _FIELD_ALIAS_HINTS.get(k, []):
        variants.extend([alias, alias.lower(), alias.upper(), alias.replace("_", " "), alias.replace("_", "").lower()])
    out: List[str] = []
    seen = set()
    for v in variants:
        s = _strip(v)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _extract_from_raw(raw: Dict[str, Any], candidates: Sequence[str]) -> Any:
    lowered = {str(k).strip().lower(): v for k, v in raw.items()}
    compressed = {re.sub(r"[^a-z0-9]+", "", str(k).lower()): v for k, v in raw.items()}
    for candidate in candidates:
        if candidate in raw:
            return raw[candidate]
        lc = candidate.lower()
        if lc in lowered:
            return lowered[lc]
        cc = re.sub(r"[^a-z0-9]+", "", candidate.lower())
        if cc in compressed:
            return compressed[cc]
    return None


def _normalize_row(keys: Sequence[str], headers: Sequence[str], raw: Mapping[str, Any], *, symbol_fallback: str = "") -> Dict[str, Any]:
    raw_dict = dict(raw or {})
    header_by_key = {str(k): str(h) for k, h in zip(keys, headers)}
    out: Dict[str, Any] = {}
    for key in keys:
        ks = str(key)
        value = _extract_from_raw(raw_dict, _key_variants(ks))
        if value is None:
            h = header_by_key.get(ks, "")
            if h:
                value = _extract_from_raw(raw_dict, [h, h.lower(), h.upper()])
        out[ks] = _json_safe(value)
    if symbol_fallback:
        if "symbol" in out and not out.get("symbol"):
            out["symbol"] = symbol_fallback
        if "ticker" in out and not out.get("ticker"):
            out["ticker"] = symbol_fallback
    return out


def _rows_to_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(r.get(k)) for k in keys] for r in rows]


def _extract_rows_like(payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
    if payload is None or depth > 8:
        return []
    if isinstance(payload, list):
        if payload and isinstance(payload[0], Mapping):
            return [dict(x) for x in payload]
        return []
    if not isinstance(payload, Mapping):
        return []
    for name in ("row_objects", "records", "items", "data", "quotes", "results"):
        value = payload.get(name)
        if isinstance(value, list) and value and isinstance(value[0], Mapping):
            return [dict(x) for x in value]
    rows_value = payload.get("rows")
    if isinstance(rows_value, list):
        if rows_value and isinstance(rows_value[0], Mapping):
            return [dict(x) for x in rows_value]
    for name in ("payload", "result", "response", "output", "data"):
        nested = payload.get(name)
        if isinstance(nested, Mapping):
            found = _extract_rows_like(nested, depth + 1)
            if found:
                return found
    return []


def _extract_status_error(payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return "success", None, {}
    status_out = _strip(payload.get("status")) or "success"
    error_out = payload.get("error") or payload.get("detail") or payload.get("message")
    meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return status_out, (str(error_out) if error_out is not None else None), meta_out


def _payload_has_real_rows(payload: Any) -> bool:
    return bool(_extract_rows_like(payload))


def _slice_rows(rows: Sequence[Mapping[str, Any]], limit: int, offset: int) -> List[Dict[str, Any]]:
    start = max(0, int(offset or 0))
    items = [dict(r) for r in (rows or [])]
    if int(limit or 0) <= 0:
        return items[start:]
    return items[start:start + int(limit)]


def _canonical_owner_hint(page: str, route_family: str) -> Dict[str, str]:
    if route_family == "instrument":
        return {
            "canonical_owner": "routes.analysis_sheet_rows",
            "canonical_endpoint": "/v1/analysis/sheet-rows",
            "canonical_reason": "instrument_table_mode_owned_by_analysis_sheet_rows",
        }
    if route_family in {"insights", "top10"}:
        return {
            "canonical_owner": "routes.advanced_analysis",
            "canonical_endpoint": "/sheet-rows",
            "canonical_reason": "derived_output_page_owned_by_advanced_analysis",
        }
    if route_family == "dictionary":
        return {
            "canonical_owner": "routes.advanced_analysis",
            "canonical_endpoint": "/sheet-rows",
            "canonical_reason": "data_dictionary_owned_by_advanced_analysis",
        }
    return {
        "canonical_owner": "routes.analysis_sheet_rows",
        "canonical_endpoint": "/v1/analysis/sheet-rows",
        "canonical_reason": f"page_{page}_owned_by_analysis_sheet_rows",
    }


class _Service:
    def __init__(self) -> None:
        self.bridge_timeout_sec = self._env_float("TFB_ENRICHED_BRIDGE_TIMEOUT_SEC", 25.0)
        self.quote_call_timeout_sec = self._env_float("TFB_QUOTE_CALL_TIMEOUT_SEC", 20.0)
        self.rehydrate_concurrency = max(2, min(12, int(self._env_float("TFB_ROUTE_REHYDRATE_CONCURRENCY", 4))))
        self.rehydrate_enabled = self._env_bool("TFB_ROUTE_ENABLE_REHYDRATE", True)
        self.rehydrate_max_symbols = max(0, min(250, int(self._env_float("TFB_ROUTE_REHYDRATE_MAX_SYMBOLS", 25))))

        try:
            from core.config import auth_ok, get_settings_cached, is_open_mode  # type: ignore
        except Exception:
            auth_ok = None  # type: ignore
            is_open_mode = None  # type: ignore

            def get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
                return None

        self._auth_ok = auth_ok
        self._get_settings_cached = get_settings_cached
        self._is_open_mode = is_open_mode

        try:
            from core.sheets.page_catalog import get_route_family, normalize_page_name  # type: ignore
            self.get_route_family = get_route_family
            self.normalize_page_name = normalize_page_name
        except Exception:
            self.get_route_family = None
            self.normalize_page_name = None

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        try:
            raw = os.getenv(name, "").strip()
            return float(raw) if raw else float(default)
        except Exception:
            return float(default)

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        raw = os.getenv(name, "").strip().lower()
        if not raw:
            return bool(default)
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
        return bool(default)

    def auth_guard(self, request: Request, token_query: Optional[str], x_app_token: Optional[str], authorization: Optional[str]) -> None:
        try:
            if callable(self._is_open_mode) and bool(self._is_open_mode()):
                return
        except Exception:
            pass
        if self._auth_ok is None:
            return

        settings = None
        try:
            settings = self._get_settings_cached()
        except Exception:
            settings = None

        allow_query = False
        try:
            allow_query = bool(getattr(settings, "allow_query_token", False))
        except Exception:
            allow_query = False

        auth_token = _strip(x_app_token)
        auth = _strip(authorization)
        if auth.lower().startswith("bearer "):
            auth_token = _strip(auth.split(" ", 1)[1])
        elif token_query and not auth_token and allow_query:
            auth_token = _strip(token_query)

        headers = dict(request.headers)
        path = str(getattr(getattr(request, "url", None), "path", "") or "")
        attempts = [
            {"token": auth_token, "authorization": authorization, "headers": headers, "path": path, "request": request, "settings": settings},
            {"token": auth_token, "authorization": authorization, "headers": headers, "path": path, "request": request},
            {"token": auth_token, "authorization": authorization, "headers": headers, "path": path},
            {"token": auth_token, "authorization": authorization, "headers": headers},
            {"token": auth_token, "authorization": authorization},
            {"token": auth_token},
        ]
        for kwargs in attempts:
            try:
                if bool(self._auth_ok(**kwargs)):
                    return
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
            except TypeError:
                continue
            except HTTPException:
                raise
            except Exception:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    async def get_engine(self, request: Request) -> Any:
        try:
            state = getattr(request.app, "state", None)
            if state is not None:
                for attr in ("engine", "data_engine", "quote_engine", "cache_engine"):
                    value = getattr(state, attr, None)
                    if value is not None:
                        return value
        except Exception:
            pass
        for module_name in ("core.data_engine_v2", "core.data_engine"):
            try:
                mod = importlib.import_module(module_name)
                get_engine = getattr(mod, "get_engine", None)
                if callable(get_engine):
                    return await _maybe_await(get_engine())
            except Exception:
                continue
        return None

    def normalize_page(self, raw: str) -> str:
        page = _strip(raw) or "Market_Leaders"
        if callable(self.normalize_page_name):
            for kwargs in ({"allow_output_pages": True}, {}):
                try:
                    value = self.normalize_page_name(page, **kwargs)  # type: ignore[misc]
                    normalized = _strip(value)
                    if normalized:
                        return normalized
                except TypeError:
                    continue
                except Exception:
                    break
        compact = page.replace("&", "_").replace("-", "_").replace("/", "_").replace(" ", "_").lower()
        mapping = {
            "market_leaders": "Market_Leaders",
            "global_markets": "Global_Markets",
            "commodities_fx": "Commodities_FX",
            "commodities_and_fx": "Commodities_FX",
            "mutual_funds": "Mutual_Funds",
            "my_portfolio": "My_Portfolio",
            "my_investments": "My_Investments",
            "insights_analysis": "Insights_Analysis",
            "top10": "Top_10_Investments",
            "top10_investments": "Top_10_Investments",
            "top_10_investments": "Top_10_Investments",
            "data_dictionary": "Data_Dictionary",
        }
        return mapping.get(compact, page.replace(" ", "_"))

    def route_family(self, page: str) -> str:
        if callable(self.get_route_family):
            try:
                family = _strip(self.get_route_family(page))  # type: ignore[misc]
                if family:
                    return family
            except Exception:
                pass
        if page == "Insights_Analysis":
            return "insights"
        if page == "Top_10_Investments":
            return "top10"
        if page == "Data_Dictionary":
            return "dictionary"
        return "instrument"

    def contract(self, page: str) -> Tuple[List[str], List[str]]:
        return _extract_contract_from_schema(page)

    def envelope(
        self,
        *,
        status: str,
        page: str,
        route_family: str,
        headers: Sequence[str],
        keys: Sequence[str],
        row_objects: Sequence[Mapping[str, Any]],
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
        dispatch: str,
        error: Optional[str] = None,
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        rows_out = [dict(r) for r in row_objects]
        hdrs = list(headers)
        ks = list(keys)
        return _json_safe({
            "status": status,
            "page": page,
            "sheet": page,
            "sheet_name": page,
            "route_family": route_family,
            "headers": hdrs if include_headers else [],
            "display_headers": hdrs if include_headers else [],
            "sheet_headers": hdrs if include_headers else [],
            "column_headers": hdrs if include_headers else [],
            "keys": ks,
            "columns": ks,
            "fields": ks,
            "rows": _rows_to_matrix(rows_out, ks) if include_matrix else [],
            "rows_matrix": _rows_to_matrix(rows_out, ks) if include_matrix else [],
            "row_objects": rows_out,
            "items": rows_out,
            "records": rows_out,
            "data": rows_out,
            "quotes": rows_out,
            "count": len(rows_out),
            "detail": error or "",
            "error": error,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": round((time.time() - started_at) * 1000.0, 3),
                "count": len(rows_out),
                "dispatch": dispatch,
                "mode": mode,
                **(extra_meta or {}),
            },
        })


async def _call_with_tolerant_signatures(fn: Any, *, timeout_seconds: float, kwargs: Optional[Dict[str, Any]] = None) -> Any:
    payload_kwargs = dict(kwargs or {})
    attempts = [
        payload_kwargs,
        {k: payload_kwargs.get(k) for k in ("request", "body", "payload", "mode", "include_matrix_q", "token", "x_app_token", "authorization", "x_request_id")},
        {k: payload_kwargs.get(k) for k in ("request", "body", "mode")},
        {k: payload_kwargs.get(k) for k in ("request", "body")},
        {k: payload_kwargs.get(k) for k in ("body", "mode")},
        {k: payload_kwargs.get(k) for k in ("body",)},
        {k: payload_kwargs.get(k) for k in ("page", "sheet", "sheet_name", "name", "tab", "symbols", "tickers", "top_n", "limit", "offset", "mode")},
        {},
    ]
    last_error: Optional[Exception] = None
    for attempt in attempts:
        call_kwargs = {k: v for k, v in attempt.items() if v is not None}
        try:
            if timeout_seconds > 0:
                return await asyncio.wait_for(_call_maybe_async(fn, **call_kwargs), timeout=timeout_seconds)
            return await _call_maybe_async(fn, **call_kwargs)
        except TypeError as exc:
            last_error = exc
            continue
        except Exception as exc:
            last_error = exc
            raise
    if last_error is not None:
        raise last_error
    return None


async def _resolve_bridge_impl(page: str, route_family: str) -> Tuple[Optional[Any], Dict[str, Any]]:
    if route_family == "dictionary" or page == "Data_Dictionary":
        module_order = ("routes.advanced_analysis", "routes.analysis_sheet_rows", "routes.investment_advisor")
    elif route_family in {"top10", "insights"} or page in {"Top_10_Investments", "Insights_Analysis"}:
        module_order = ("routes.advanced_analysis", "routes.analysis_sheet_rows", "routes.investment_advisor")
    else:
        module_order = ("routes.analysis_sheet_rows", "routes.advanced_analysis", "routes.investment_advisor")

    callable_candidates = (
        "_analysis_sheet_rows_impl",
        "_run_advanced_sheet_rows_impl",
        "_run_investment_advisor_impl",
        "run_investment_advisor_engine",
        "run_investment_advisor",
    )

    for module_name in module_order:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue
        for callable_name in callable_candidates:
            fn = getattr(module, callable_name, None)
            if callable(fn):
                return fn, {"module": module_name, "callable": callable_name}
    return None, {}


async def _delegate_sheet_rows_via_bridge(
    svc: _Service,
    request: Request,
    page: str,
    route_family: str,
    body: Dict[str, Any],
    mode_q: str,
    include_matrix_q: Optional[bool],
    token_q: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    impl, impl_meta = await _resolve_bridge_impl(page, route_family)
    if impl is None:
        return None

    prepared = dict(body or {})
    prepared["page"] = page
    prepared["sheet"] = page
    prepared["sheet_name"] = page
    prepared["name"] = page
    prepared["tab"] = page

    kwargs = {
        "request": request,
        "body": prepared,
        "payload": prepared,
        "mode": mode_q or "",
        "include_matrix_q": include_matrix_q if include_matrix_q is not None else _bool_from_any(prepared.get("include_matrix"), True),
        "include_matrix": include_matrix_q if include_matrix_q is not None else _bool_from_any(prepared.get("include_matrix"), True),
        "token": token_q,
        "x_app_token": x_app_token,
        "authorization": authorization,
        "x_request_id": x_request_id,
        "page": page,
        "sheet": page,
        "sheet_name": page,
        "name": page,
        "tab": page,
        "symbols": _list_from_body(prepared, "symbols", "tickers", "tickers_list"),
        "tickers": _list_from_body(prepared, "symbols", "tickers", "tickers_list"),
        "top_n": _int_from_any(prepared.get("top_n"), 200),
        "limit": _int_from_any(prepared.get("limit"), 0),
        "offset": _int_from_any(prepared.get("offset"), 0),
    }

    out = await _call_with_tolerant_signatures(impl, timeout_seconds=svc.bridge_timeout_sec, kwargs=kwargs)
    safe = _json_safe(out)
    if isinstance(safe, Mapping):
        result = dict(safe)
        result.setdefault("page", page)
        result.setdefault("sheet", page)
        result.setdefault("sheet_name", page)
        meta = result.get("meta") if isinstance(result.get("meta"), Mapping) else {}
        meta = dict(meta)
        meta.setdefault("bridge_source_module", impl_meta.get("module"))
        meta.setdefault("bridge_callable", impl_meta.get("callable"))
        meta.update(_canonical_owner_hint(page, route_family))
        result["meta"] = meta
        return result
    return None


async def _fetch_analysis_rows(engine: Any, symbols: List[str], *, mode: str, page: str) -> Dict[str, Dict[str, Any]]:
    if not symbols or engine is None:
        return {}
    preferred = [
        "get_analysis_rows_batch", "get_analysis_quotes_batch", "get_enriched_quotes_batch",
        "get_quotes_batch", "quotes_batch", "get_enriched_quotes", "get_quotes",
    ]
    for method in preferred:
        fn = getattr(engine, method, None)
        if not callable(fn):
            continue
        try:
            for kwargs in ({"mode": mode, "schema": page}, {"schema": page}, {"mode": mode}, {}):
                try:
                    if method in {"get_quotes_batch", "quotes_batch", "get_analysis_rows_batch", "get_analysis_quotes_batch", "get_enriched_quotes_batch", "get_enriched_quotes", "get_quotes"}:
                        res = await _call_maybe_async(fn, symbols, **kwargs)
                    else:
                        res = await _call_maybe_async(fn, symbols, **kwargs)
                    break
                except TypeError:
                    res = None
                    continue
            else:
                res = None
            if isinstance(res, Mapping):
                if all(isinstance(k, str) for k in res.keys()) and any(k in set(symbols) for k in res.keys()):
                    return {str(k): dict(v) if isinstance(v, Mapping) else {"symbol": k, "value": v} for k, v in res.items()}
                data = res.get("data") or res.get("rows") or res.get("items") or res.get("quotes") or res.get("row_objects")
                if isinstance(data, Mapping):
                    return {str(k): dict(v) if isinstance(v, Mapping) else {"symbol": k, "value": v} for k, v in data.items()}
                if isinstance(data, list):
                    return {s: (dict(r) if isinstance(r, Mapping) else {"symbol": s, "value": r}) for s, r in zip(symbols, data)}
            elif isinstance(res, list):
                return {s: (dict(r) if isinstance(r, Mapping) else {"symbol": s, "value": r}) for s, r in zip(symbols, res)}
        except Exception:
            continue

    out: Dict[str, Dict[str, Any]] = {}
    per_dict_fn = getattr(engine, "get_enriched_quote_dict", None) or getattr(engine, "get_analysis_row_dict", None) or getattr(engine, "get_quote_dict", None)
    per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_analysis_row", None) or getattr(engine, "get_quote", None)
    for s in symbols:
        try:
            if callable(per_dict_fn):
                for kwargs in ({"mode": mode, "schema": page}, {"schema": page}, {"mode": mode}, {}):
                    try:
                        out[s] = await _call_maybe_async(per_dict_fn, s, **kwargs)
                        break
                    except TypeError:
                        continue
                else:
                    out[s] = {"symbol": s, "error": "per_symbol_dict_call_failed"}
            elif callable(per_fn):
                for kwargs in ({"mode": mode, "schema": page}, {"schema": page}, {"mode": mode}, {}):
                    try:
                        result = await _call_maybe_async(per_fn, s, **kwargs)
                        out[s] = dict(result) if isinstance(result, Mapping) else {"symbol": s, "value": result}
                        break
                    except TypeError:
                        continue
                else:
                    out[s] = {"symbol": s, "error": "per_symbol_call_failed"}
            else:
                out[s] = {"symbol": s, "error": "engine_missing_quote_method"}
        except Exception as e:
            out[s] = {"symbol": s, "error": str(e)}
    return out


async def _build_instrument_rows(
    svc: _Service,
    page: str,
    headers: Sequence[str],
    keys: Sequence[str],
    symbols: Sequence[str],
    mode: str,
    request: Request,
) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
    if not symbols:
        return [], 0, {"batch_rows": 0, "rehydrated_rows": 0, "sparse_after_rehydrate": 0}

    engine = await svc.get_engine(request)
    if engine is None:
        rows = [_normalize_row(keys, headers, {"symbol": s, "ticker": s, "error": "Data engine unavailable"}, symbol_fallback=s) for s in symbols]
        return rows, len(rows), {"batch_rows": 0, "rehydrated_rows": 0, "sparse_after_rehydrate": len(rows)}

    quotes_map = await _fetch_analysis_rows(engine, list(symbols), mode=mode or "", page=page)
    rows_out: List[Dict[str, Any]] = []
    errors = 0
    for sym in symbols:
        raw = _to_dict(quotes_map.get(sym)) or {"symbol": sym, "ticker": sym, "error": "missing_row"}
        if raw.get("error"):
            errors += 1
        rows_out.append(_normalize_row(keys, headers, raw, symbol_fallback=sym))
    return rows_out, errors, {"batch_rows": len(symbols), "rehydrated_rows": 0, "sparse_after_rehydrate": 0}


svc = _Service()


async def _sheet_rows_handler(
    request: Request,
    body: Dict[str, Any],
    mode: str,
    include_matrix_q: Optional[bool],
    token: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    started_at = time.time()
    request_id = _request_id(request, x_request_id)
    svc.auth_guard(request, token, x_app_token, authorization)

    prepared = dict(body or {})
    page = svc.normalize_page(_page_from_body(prepared) or "Market_Leaders")
    route_family = svc.route_family(page)
    headers, keys = svc.contract(page)

    include_headers = _bool_from_any(prepared.get("include_headers"), True)
    include_matrix = include_matrix_q if include_matrix_q is not None else _bool_from_any(prepared.get("include_matrix"), True)
    schema_only = _bool_from_any(prepared.get("schema_only"), False)
    headers_only = _bool_from_any(prepared.get("headers_only"), False)
    limit = max(1, min(5000, _int_from_any(prepared.get("limit"), 200)))
    offset = max(0, _int_from_any(prepared.get("offset"), 0))
    top_n = max(1, min(5000, _int_from_any(prepared.get("top_n"), limit)))
    requested_symbols = _list_from_body(prepared, "symbols", "tickers", "tickers_list")

    if schema_only or headers_only:
        return svc.envelope(
            status="success",
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            row_objects=[],
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch="enriched_sheet_rows_schema_only",
            extra_meta={"schema_only": bool(schema_only), "headers_only": bool(headers_only), **_canonical_owner_hint(page, route_family)},
        )

    bridge_result = await _delegate_sheet_rows_via_bridge(
        svc, request, page, route_family, prepared, mode, include_matrix_q, token, x_app_token, authorization, x_request_id
    )
    if bridge_result is not None and _payload_has_real_rows(bridge_result):
        ext_rows = _extract_rows_like(bridge_result)
        normalized_rows = [_normalize_row(keys, headers, r) for r in ext_rows]
        if page == "Top_10_Investments":
            normalized_rows = _slice_rows(normalized_rows, top_n, 0)
            for idx, row in enumerate(normalized_rows, start=1):
                row.setdefault("top10_rank", idx)
                row.setdefault("selection_reason", "Selected by enriched bridge fallback.")
                row.setdefault("criteria_snapshot", "{}")
        normalized_rows = _slice_rows(normalized_rows, limit, offset)
        status_out, error_out, meta_out = _extract_status_error(bridge_result)
        extra_meta = dict(meta_out or {})
        extra_meta.update(_canonical_owner_hint(page, route_family))
        return svc.envelope(
            status=status_out or ("success" if normalized_rows else "partial"),
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            row_objects=normalized_rows,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch="bridge_sheet_rows",
            error=error_out,
            extra_meta=extra_meta,
        )

    if route_family == "instrument":
        symbols = requested_symbols[: max(limit + offset, top_n)]
        if not symbols:
            symbols = EMERGENCY_PAGE_SYMBOLS.get(page, [])[: max(limit + offset, top_n)]
        rows_out, errors, hydrate_meta = await _build_instrument_rows(svc, page, headers, keys, symbols, mode, request)
        rows_out = _slice_rows(rows_out, limit, offset)
        return svc.envelope(
            status="success" if errors == 0 else ("partial" if errors < max(1, len(symbols)) else "error"),
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            row_objects=rows_out,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch="instrument_sheet_rows_fallback",
            error=(f"{errors} errors" if errors else None),
            extra_meta={**hydrate_meta, **_canonical_owner_hint(page, route_family)},
        )

    return svc.envelope(
        status="partial",
        page=page,
        route_family=route_family,
        headers=headers,
        keys=keys,
        row_objects=[],
        include_headers=include_headers,
        include_matrix=include_matrix,
        request_id=request_id,
        started_at=started_at,
        mode=mode,
        dispatch="non_instrument_bridge_fail_soft",
        error="No usable rows returned from canonical owner",
        extra_meta=_canonical_owner_hint(page, route_family),
    )


async def _single_quote_handler(
    request: Request,
    body: Dict[str, Any],
    page_q: str,
    mode_q: str,
    token_q: Optional[str],
    x_app_token: Optional[str],
    authorization: Optional[str],
    x_request_id: Optional[str],
) -> Dict[str, Any]:
    started_at = time.time()
    request_id = _request_id(request, x_request_id)
    svc.auth_guard(request, token_q, x_app_token, authorization)

    symbol = _strip(body.get("symbol") or body.get("ticker") or body.get("requested_symbol"))
    if not symbol:
        syms = _list_from_body(body, "symbols", "tickers", "tickers_list")
        symbol = syms[0] if syms else ""
    if not symbol:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing symbol")

    page = svc.normalize_page(page_q or _page_from_body(body) or "Market_Leaders")
    route_family = svc.route_family(page)
    headers, keys = svc.contract(page)

    if route_family != "instrument":
        bridge_body = dict(body or {})
        bridge_body["symbols"] = [symbol]
        bridge_result = await _delegate_sheet_rows_via_bridge(
            svc, request, page, route_family, bridge_body, mode_q, True, token_q, x_app_token, authorization, x_request_id
        )
        if bridge_result is not None and _payload_has_real_rows(bridge_result):
            rows = _extract_rows_like(bridge_result)
            normalized_rows = [_normalize_row(keys, headers, r, symbol_fallback=symbol) for r in rows]
            row = normalized_rows[0] if normalized_rows else _normalize_row(keys, headers, {"symbol": symbol}, symbol_fallback=symbol)
            payload = svc.envelope(
                status="success" if row else "partial",
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                row_objects=[row],
                include_headers=True,
                include_matrix=True,
                request_id=request_id,
                started_at=started_at,
                mode=mode_q,
                dispatch="single_quote_bridge",
                extra_meta=_canonical_owner_hint(page, route_family),
            )
            payload["row"] = row
            payload["quote"] = row
            return payload

        row = _normalize_row(keys, headers, {"symbol": symbol, "ticker": symbol, "error": f"single_quote_not_supported_for_{route_family}_page"}, symbol_fallback=symbol)
        payload = svc.envelope(
            status="partial",
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            row_objects=[row],
            include_headers=True,
            include_matrix=True,
            request_id=request_id,
            started_at=started_at,
            mode=mode_q,
            dispatch="single_quote_non_instrument_guard",
            extra_meta=_canonical_owner_hint(page, route_family),
        )
        payload["row"] = row
        payload["quote"] = row
        return payload

    rows_out, errors, meta = await _build_instrument_rows(svc, page, headers, keys, [symbol], mode_q, request)
    row = rows_out[0] if rows_out else _normalize_row(keys, headers, {"symbol": symbol, "ticker": symbol, "error": "missing_row"}, symbol_fallback=symbol)
    payload = svc.envelope(
        status="success" if errors == 0 and not row.get("error") else "partial",
        page=page,
        route_family=route_family,
        headers=headers,
        keys=keys,
        row_objects=[row],
        include_headers=True,
        include_matrix=True,
        request_id=request_id,
        started_at=started_at,
        mode=mode_q,
        dispatch="single_quote_instrument",
        extra_meta=meta,
    )
    payload["row"] = row
    payload["quote"] = row
    return payload


@router.get("/v1/enriched/health", include_in_schema=False)
@router.get("/v1/enriched_quote/health", include_in_schema=False)
@router.get("/v1/enriched-quote/health", include_in_schema=False)
async def health() -> Dict[str, Any]:
    return _json_safe({
        "status": "ok",
        "module": "routes.enriched_quote",
        "router_version": ROUTER_VERSION,
        "owns_enriched_sheet_rows": True,
    })


@router.get("/v1/enriched/headers", include_in_schema=False)
@router.get("/v1/enriched_quote/headers", include_in_schema=False)
@router.get("/v1/enriched-quote/headers", include_in_schema=False)
async def headers(page: str = Query(default="Market_Leaders")) -> Dict[str, Any]:
    page_norm = svc.normalize_page(page)
    hdrs, keys = svc.contract(page_norm)
    return _json_safe({
        "status": "success" if hdrs else "degraded",
        "page": page_norm,
        "sheet": page_norm,
        "sheet_name": page_norm,
        "headers": hdrs,
        "display_headers": hdrs,
        "sheet_headers": hdrs,
        "column_headers": hdrs,
        "keys": keys,
        "columns": keys,
        "fields": keys,
        "route_family": svc.route_family(page_norm),
        "router_version": ROUTER_VERSION,
    })


@router.post("/v1/enriched/quote")
@router.post("/v1/enriched_quote/quote")
@router.post("/v1/enriched-quote/quote")
@router.post("/quote")
async def quote_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    page: str = Query(default=""),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _single_quote_handler(request, body, page, mode, token, x_app_token, authorization, x_request_id)


@router.get("/v1/enriched/quote", include_in_schema=False)
@router.get("/v1/enriched_quote/quote", include_in_schema=False)
@router.get("/v1/enriched-quote/quote", include_in_schema=False)
@router.get("/quote", include_in_schema=False)
async def quote_get(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    ticker: Optional[str] = Query(default=None),
    page: str = Query(default=""),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body = _collect_sheet_body(symbol=symbol, ticker=ticker, page=page)
    return await _single_quote_handler(request, body, page, mode, token, x_app_token, authorization, x_request_id)


@router.post("/v1/enriched/quotes", include_in_schema=False)
@router.post("/v1/enriched_quote/quotes", include_in_schema=False)
@router.post("/v1/enriched-quote/quotes", include_in_schema=False)
@router.post("/quotes")
async def quotes_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    page = _page_from_body(body) or "Market_Leaders"
    return await _sheet_rows_handler(request, body, mode, None, token, x_app_token, authorization, x_request_id)


@router.get("/v1/enriched/quotes", include_in_schema=False)
@router.get("/v1/enriched_quote/quotes", include_in_schema=False)
@router.get("/v1/enriched-quote/quotes", include_in_schema=False)
@router.get("/quotes", include_in_schema=False)
async def quotes_get(
    request: Request,
    page: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    include_headers: Optional[str] = Query(default=None),
    include_matrix: Optional[str] = Query(default=None),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    schema_only: Optional[str] = Query(default=None),
    headers_only: Optional[str] = Query(default=None),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body = _collect_sheet_body(
        page=page,
        sheet_name=sheet_name,
        sheet=sheet,
        name=name,
        tab=tab,
        symbols=symbols,
        tickers=tickers,
        include_headers=include_headers,
        include_matrix=include_matrix,
        limit=limit,
        offset=offset,
        top_n=top_n,
        schema_only=schema_only,
        headers_only=headers_only,
    )
    return await _sheet_rows_handler(request, body, mode, None, token, x_app_token, authorization, x_request_id)


@router.post("/v1/enriched/sheet-rows")
@router.post("/v1/enriched_quote/sheet-rows")
@router.post("/v1/enriched-quote/sheet-rows")
async def sheet_rows_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    include_matrix: Optional[bool] = Query(default=None),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    return await _sheet_rows_handler(request, body, mode, include_matrix, token, x_app_token, authorization, x_request_id)


@router.get("/v1/enriched/sheet-rows")
@router.get("/v1/enriched_quote/sheet-rows")
@router.get("/v1/enriched-quote/sheet-rows")
async def sheet_rows_get(
    request: Request,
    page: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    include_headers: Optional[str] = Query(default=None),
    include_matrix: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    schema_only: Optional[str] = Query(default=None),
    headers_only: Optional[str] = Query(default=None),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body = _collect_sheet_body(
        page=page,
        sheet_name=sheet_name,
        sheet=sheet,
        name=name,
        tab=tab,
        symbols=symbols,
        tickers=tickers,
        include_headers=include_headers,
        limit=limit,
        offset=offset,
        top_n=top_n,
        schema_only=schema_only,
        headers_only=headers_only,
    )
    return await _sheet_rows_handler(request, body, mode, include_matrix, token, x_app_token, authorization, x_request_id)


@router.post("/v1/enriched")
@router.post("/v1/enriched_quote")
@router.post("/v1/enriched-quote")
async def alias_root_post(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    if _strip(body.get("symbol") or body.get("ticker") or body.get("requested_symbol")):
        return await _single_quote_handler(request, body, _page_from_body(body), mode, token, x_app_token, authorization, x_request_id)
    if _list_from_body(body, "symbols", "tickers", "tickers_list"):
        return await _sheet_rows_handler(request, body, mode, None, token, x_app_token, authorization, x_request_id)
    return await _sheet_rows_handler(request, body, mode, None, token, x_app_token, authorization, x_request_id)


@router.get("/v1/enriched")
@router.get("/v1/enriched_quote")
@router.get("/v1/enriched-quote")
async def alias_root_get(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    ticker: Optional[str] = Query(default=None),
    page: Optional[str] = Query(default=None),
    sheet_name: Optional[str] = Query(default=None),
    sheet: Optional[str] = Query(default=None),
    name: Optional[str] = Query(default=None),
    tab: Optional[str] = Query(default=None),
    symbols: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    include_headers: Optional[str] = Query(default=None),
    include_matrix: Optional[bool] = Query(default=None),
    limit: Optional[int] = Query(default=None),
    offset: Optional[int] = Query(default=None),
    top_n: Optional[int] = Query(default=None),
    schema_only: Optional[str] = Query(default=None),
    headers_only: Optional[str] = Query(default=None),
    mode: str = Query(default=""),
    token: Optional[str] = Query(default=None),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
) -> Dict[str, Any]:
    body = _collect_sheet_body(
        page=page,
        sheet_name=sheet_name,
        sheet=sheet,
        name=name,
        tab=tab,
        symbols=symbols,
        tickers=tickers,
        include_headers=include_headers,
        limit=limit,
        offset=offset,
        top_n=top_n,
        schema_only=schema_only,
        headers_only=headers_only,
    )
    if symbol not in (None, ""):
        body["symbol"] = symbol
    if ticker not in (None, ""):
        body["ticker"] = ticker
    if _strip(body.get("symbol") or body.get("ticker") or body.get("requested_symbol")):
        return await _single_quote_handler(request, body, page or _page_from_body(body), mode, token, x_app_token, authorization, x_request_id)
    return await _sheet_rows_handler(request, body, mode, include_matrix, token, x_app_token, authorization, x_request_id)


__all__ = ["ROUTER_VERSION", "router"]
