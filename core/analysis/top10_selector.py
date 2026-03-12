#!/usr/bin/env python3
# routes/enriched_quote.py
"""
================================================================================
TFB Enriched Quote Routes Wrapper — v7.0.0
================================================================================
RENDER-SAFE • SCHEMA-AWARE • PAGE-DISPATCH SAFE • HEADER/ROW BALANCED
SPECIAL-BUILDER PRESERVING • ASYNC-SAFE • GET+POST COMPATIBLE
QUERY/BODY MERGE • LEGACY ALIAS SAFE • TABLE-MODE SAFE • CORE-WRAPPER SAFE
TOP10-SPECIAL SAFE • SCHEMA-ONLY SAFE • TIMEOUT-GUARDED • NULL-SAFE
JSON-SAFE • CONTRACT-HARDENED • SPECIAL-ROUTE FAIL-SAFE
ENGINE-FALLBACK SAFE • HYDRATION-HARDENED • SPARSE-QUOTE SAFE
HYPHEN-ALIAS RESTORED • BACKWARD-COMPATIBLE • THREAD-OFFLOADED
CANONICAL-SCHEMA PRESERVING • PARTIAL-CONTRACT SAFE

Why this revision
-----------------
- ✅ FIX: Top_10_Investments canonical route contract is preserved even if a special
        builder returns partial headers/keys
- ✅ FIX: special-page payload normalization now prefers canonical schema when
        result contracts are missing / short / mismatched
- ✅ FIX: rows from special builders are re-projected to canonical keys before
        envelope output to prevent schema drift
- ✅ FIX: rows_matrix / list-of-lists payloads are safely normalized through source
        keys first, then projected to output keys
- ✅ FIX: extra meta added for contract source / canonical enforcement visibility
- ✅ FIX: stronger protection against partial 12-col / short Top10 payload leakage
- ✅ SAFE: no network I/O at import-time
- ✅ SAFE: no Prometheus metric creation here
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import math
import os
import time
import uuid
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.enriched_quote")

ROUTER_VERSION = "7.0.0"

router: Optional[APIRouter] = None

_SPECIAL_PAGES = {"Top_10_Investments", "Insights_Analysis", "Data_Dictionary"}
_STRICT_CANONICAL_PAGES = {"Top_10_Investments", "Insights_Analysis", "Data_Dictionary"}


# =============================================================================
# Pure helpers
# =============================================================================
_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "symbol_code"],
    "ticker": ["symbol", "code", "instrument", "security"],
    "name": ["company_name", "long_name", "instrument_name", "security_name"],
    "current_price": [
        "price",
        "last_price",
        "last",
        "close",
        "market_price",
        "current",
        "spot",
        "nav",
    ],
    "recommendation_reason": ["reason", "reco_reason", "recommendation_notes"],
    "top10_rank": ["rank", "top_rank"],
    "selection_reason": ["selection_notes", "selector_reason"],
    "criteria_snapshot": ["criteria", "criteria_json", "snapshot"],
}


def _strip(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
        return s if s and s.lower() != "none" else ""
    except Exception:
        return ""


def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, str)):
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

    if hasattr(value, "__dict__"):
        try:
            return _json_safe(vars(value))
        except Exception:
            pass

    try:
        return str(value)
    except Exception:
        return None


def _as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    if isinstance(v, set):
        return list(v)
    if isinstance(v, str):
        return [v]
    if isinstance(v, Mapping):
        return [v]
    if isinstance(v, Iterable):
        try:
            return list(v)
        except Exception:
            return [v]
    return [v]


def _split_symbols_string(v: str) -> List[str]:
    raw = (v or "").replace(";", ",").replace("\n", ",").replace("\t", ",")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    out: List[str] = []
    seen = set()
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _clean_string_list(v: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in _as_list(v):
        s = _strip(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _get_list(body: Mapping[str, Any], *keys: str) -> List[str]:
    for k in keys:
        v = body.get(k)
        if isinstance(v, list):
            out: List[str] = []
            seen = set()
            for item in v:
                s = _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out
        if isinstance(v, str) and v.strip():
            parts = _split_symbols_string(v)
            if parts:
                return parts
    return []


def _get_bool(body: Mapping[str, Any], key: str, default: bool) -> bool:
    v = body.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return default
    return default


def _get_int(body: Mapping[str, Any], key: str, default: int) -> int:
    v = body.get(key)
    try:
        if isinstance(v, bool):
            return default
        if isinstance(v, int):
            return v
        if isinstance(v, float) and int(v) == v:
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            if s:
                return int(float(s))
    except Exception:
        pass
    return int(default)


def _get_float_env(name: str, default: float) -> float:
    try:
        raw = os.getenv(name, "").strip()
        if not raw:
            return float(default)
        return float(raw)
    except Exception:
        return float(default)


def _extract_token(
    x_app_token: Optional[str],
    authorization: Optional[str],
    token_query: Optional[str],
) -> str:
    tok = _strip(x_app_token)
    auth = _strip(authorization)

    if auth.lower().startswith("bearer "):
        return _strip(auth.split(" ", 1)[1])
    if tok:
        return tok
    return _strip(token_query)


def _rows_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows]


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)

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
        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass

    return {}


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


async def _call_func_threadsafe(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    result = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def _normalize_item_to_row(item: Any, keys: Sequence[str]) -> Dict[str, Any]:
    if isinstance(item, Mapping):
        return {k: _json_safe(item.get(k)) for k in keys}

    if hasattr(item, "__dict__"):
        d = _model_to_dict(item)
        return {k: _json_safe(d.get(k)) for k in keys}

    if isinstance(item, (list, tuple)):
        vals = list(item)
        return {k: _json_safe(vals[i] if i < len(vals) else None) for i, k in enumerate(keys)}

    out = {k: None for k in keys}
    if keys:
        out[keys[0]] = _json_safe(item)
    return out


def _first_list(*vals: Any) -> List[Any]:
    for v in vals:
        if isinstance(v, list):
            return v
    return []


def _request_id(request: Request, x_request_id: Optional[str]) -> str:
    if x_request_id and _strip(x_request_id):
        return _strip(x_request_id)
    try:
        rid = _strip(getattr(request.state, "request_id", ""))
        if rid:
            return rid
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


def _collect_query_body_for_sheet_rows(
    *,
    page: Optional[str],
    sheet_name: Optional[str],
    sheet: Optional[str],
    name: Optional[str],
    tab: Optional[str],
    symbols: Optional[str],
    tickers: Optional[str],
    include_headers: Optional[str],
    include_matrix: Optional[str],
    limit: Optional[int],
    offset: Optional[int],
    top_n: Optional[int],
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    for k, v in {
        "page": page,
        "sheet_name": sheet_name,
        "sheet": sheet,
        "name": name,
        "tab": tab,
        "symbols": symbols,
        "tickers": tickers,
        "include_headers": include_headers,
        "include_matrix": include_matrix,
        "limit": limit,
        "offset": offset,
        "top_n": top_n,
    }.items():
        if v not in (None, ""):
            body[k] = v
    return body


def _collect_query_body_for_quote(
    *,
    symbol: Optional[str],
    ticker: Optional[str],
    page: Optional[str],
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    if symbol not in (None, ""):
        body["symbol"] = symbol
    if ticker not in (None, ""):
        body["ticker"] = ticker
    if page not in (None, ""):
        body["page"] = page
    return body


def _collect_query_body_for_hyphen_bridge(
    *,
    symbol: Optional[str],
    ticker: Optional[str],
    page: Optional[str],
    sheet_name: Optional[str],
    sheet: Optional[str],
    name: Optional[str],
    tab: Optional[str],
    symbols: Optional[str],
    tickers: Optional[str],
    include_headers: Optional[str],
    include_matrix: Optional[str],
    limit: Optional[int],
    offset: Optional[int],
    top_n: Optional[int],
) -> Dict[str, Any]:
    body = _collect_query_body_for_sheet_rows(
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
    )
    if symbol not in (None, ""):
        body["symbol"] = symbol
    if ticker not in (None, ""):
        body["ticker"] = ticker
    return body


def _merged_special_body(body: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(body or {})
    criteria_merged: Dict[str, Any] = {}

    def _merge_from_mapping(m: Any) -> None:
        if isinstance(m, Mapping):
            for k, v in m.items():
                if v is not None:
                    criteria_merged[k] = v

    _merge_from_mapping(body.get("criteria"))
    _merge_from_mapping(body.get("filters"))

    settings = body.get("settings")
    if isinstance(settings, Mapping):
        _merge_from_mapping(settings)
        _merge_from_mapping(settings.get("criteria"))

    payload = body.get("payload")
    if isinstance(payload, Mapping):
        _merge_from_mapping(payload)
        _merge_from_mapping(payload.get("criteria"))

    if criteria_merged:
        out["criteria"] = criteria_merged
        for promoted_key in (
            "pages_selected",
            "pages",
            "selected_pages",
            "direct_symbols",
            "invest_period_days",
            "investment_period_days",
            "period_days",
            "horizon_days",
            "top_n",
            "min_expected_roi",
            "max_risk_score",
            "min_confidence",
            "min_volume",
            "use_liquidity_tiebreak",
            "enforce_risk_confidence",
        ):
            if out.get(promoted_key) in (None, "", [], {}):
                if promoted_key in criteria_merged:
                    out[promoted_key] = criteria_merged[promoted_key]

    top_level_direct = _get_list(out, "direct_symbols", "symbols", "tickers", "tickers_list")
    nested_direct = _get_list(criteria_merged, "direct_symbols", "symbols", "tickers")
    if not top_level_direct and nested_direct:
        out["direct_symbols"] = nested_direct

    top_level_pages = _get_list(out, "pages_selected", "pages", "selected_pages")
    nested_pages = _get_list(criteria_merged, "pages_selected", "pages", "selected_pages")
    if not top_level_pages and nested_pages:
        out["pages_selected"] = nested_pages

    return out


def _special_schema_only_requested(body: Mapping[str, Any]) -> bool:
    return any(_get_bool(body, key, False) for key in ("schema_only", "headers_only", "keys_only"))


def _has_payload_value(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, float):
        return not (math.isnan(v) or math.isinf(v))
    return True


def _merge_payload_dicts(base: Optional[Dict[str, Any]], addon: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base or {})
    if not isinstance(addon, dict):
        return out
    for k, v in addon.items():
        if _has_payload_value(v):
            out[k] = v
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


def _symbol_match_candidates(sym: Any) -> List[str]:
    base = _strip(sym)
    norm = _normalize_symbol_token(base)
    items = [base, base.upper(), norm, norm.upper()]
    if norm.endswith(".SR"):
        code = norm[:-3]
        items.extend([code, code.upper(), f"TADAWUL:{code}"])

    seen = set()
    out: List[str] = []
    for item in items:
        item2 = _strip(item)
        if item2 and item2 not in seen:
            seen.add(item2)
            out.append(item2)
    return out


def _payload_row_richness(d: Optional[Dict[str, Any]]) -> int:
    if not isinstance(d, dict):
        return 0
    important = (
        "symbol",
        "name",
        "exchange",
        "currency",
        "country",
        "sector",
        "industry",
        "current_price",
        "previous_close",
        "open_price",
        "day_high",
        "day_low",
        "volume",
        "market_cap",
        "pe_ttm",
        "pb_ratio",
        "ps_ratio",
        "forecast_confidence",
        "risk_score",
        "risk_bucket",
        "overall_score",
        "opportunity_score",
        "recommendation",
        "recommendation_reason",
        "data_provider",
        "last_updated_utc",
        "last_updated_riyadh",
        "top10_rank",
        "selection_reason",
        "criteria_snapshot",
    )
    return sum(1 for k in important if _has_payload_value(d.get(k)))


def _is_sparse_payload_row(d: Optional[Dict[str, Any]], threshold: int = 10) -> bool:
    return _payload_row_richness(d) < threshold


# =============================================================================
# Shared lazy service layer
# =============================================================================
class _Service:
    def __init__(self, reason: str):
        self.reason = reason
        self.special_builder_timeout_sec = max(0.0, _get_float_env("TFB_SPECIAL_BUILDER_TIMEOUT_SEC", 45.0))
        self.engine_call_timeout_sec = max(0.0, _get_float_env("TFB_ENGINE_CALL_TIMEOUT_SEC", 45.0))
        self.quote_call_timeout_sec = max(0.0, _get_float_env("TFB_QUOTE_CALL_TIMEOUT_SEC", 45.0))

        try:
            from core.config import auth_ok as _auth_ok  # type: ignore
            from core.config import get_settings_cached as _get_settings_cached  # type: ignore
        except Exception:
            _auth_ok = None  # type: ignore

            def _get_settings_cached(*args: Any, **kwargs: Any) -> Any:  # type: ignore
                return None

        self._auth_ok = _auth_ok
        self._get_settings_cached = _get_settings_cached

        try:
            from core.sheets.page_catalog import (  # type: ignore
                CANONICAL_PAGES,
                get_route_family,
                is_instrument_page,
                normalize_page_name,
            )
            from core.sheets.schema_registry import get_sheet_spec  # type: ignore

            self._HAS_SCHEMA = True
            self.CANONICAL_PAGES = list(CANONICAL_PAGES)
            self.get_route_family = get_route_family
            self.is_instrument_page = is_instrument_page
            self.normalize_page_name = normalize_page_name
            self.get_sheet_spec = get_sheet_spec
        except Exception as e:
            self._HAS_SCHEMA = False
            self.CANONICAL_PAGES = []
            self._schema_import_error = e

            def get_route_family(_: str) -> str:
                return "instrument"

            def is_instrument_page(_: str) -> bool:
                return True

            def normalize_page_name(page: str, *, allow_output_pages: bool = True) -> str:
                return _strip(page) or "Market_Leaders"

            def get_sheet_spec(_: str) -> Any:
                raise KeyError(f"schema_registry unavailable: {e}")

            self.get_route_family = get_route_family
            self.is_instrument_page = is_instrument_page
            self.normalize_page_name = normalize_page_name
            self.get_sheet_spec = get_sheet_spec

        try:
            from core.sheets.data_dictionary import build_data_dictionary_rows  # type: ignore
        except Exception:
            build_data_dictionary_rows = None  # type: ignore
        self.build_data_dictionary_rows = build_data_dictionary_rows

        build_insights_rows = None
        try:
            from core.analysis.insights_builder import build_insights_analysis_rows as build_insights_rows  # type: ignore
        except Exception:
            try:
                from core.analysis.insights_builder import build_insights_rows as build_insights_rows  # type: ignore
            except Exception:
                build_insights_rows = None  # type: ignore
        self.build_insights_rows = build_insights_rows

        build_top10_rows = None
        try:
            from core.analysis.top10_selector import build_top10_rows as build_top10_rows  # type: ignore
        except Exception:
            try:
                from core.analysis.top10_selector import build_top10_output_rows as build_top10_rows  # type: ignore
            except Exception:
                build_top10_rows = None  # type: ignore
        self.build_top10_rows = build_top10_rows

        try:
            from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
        except Exception:
            core_get_sheet_rows = None  # type: ignore
        self.core_get_sheet_rows = core_get_sheet_rows

    def auth_guard(
        self,
        auth_token: str,
        authorization: Optional[str],
        x_app_token: Optional[str],
        token_query_used: bool,
    ) -> None:
        if self._auth_ok is None:
            return

        allow_query = False
        try:
            s = self._get_settings_cached()
            allow_query = bool(getattr(s, "allow_query_token", False))
        except Exception:
            allow_query = False

        if token_query_used and not allow_query:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Query token not allowed")

        attempts = [
            {
                "token": auth_token,
                "authorization": authorization,
                "headers": {
                    "X-APP-TOKEN": x_app_token,
                    "Authorization": authorization,
                },
            },
            {"token": auth_token, "authorization": authorization},
            {"token": auth_token},
        ]

        for kwargs in attempts:
            try:
                ok = bool(self._auth_ok(**kwargs))
                if ok:
                    return
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
            except TypeError:
                continue
            except HTTPException:
                raise
            except Exception:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    async def get_engine(self, request: Optional[Request] = None) -> Any:
        try:
            if request is not None:
                st = getattr(request.app, "state", None)
                if st and getattr(st, "engine", None) is not None:
                    return st.engine
        except Exception:
            pass

        try:
            from core.data_engine_v2 import get_engine  # type: ignore
            return await _maybe_await(get_engine())
        except Exception:
            pass

        try:
            from core.data_engine import get_engine  # type: ignore
            return await _maybe_await(get_engine())
        except Exception:
            return None

    def _fallback_normalize_page(self, raw: str) -> str:
        s = _strip(raw)
        if not s:
            return "Market_Leaders"

        compact = (
            s.replace("&", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace(" ", "_")
            .lower()
        )

        mapping = {
            "market_leaders": "Market_Leaders",
            "global_markets": "Global_Markets",
            "commodities_fx": "Commodities_FX",
            "commodities___fx": "Commodities_FX",
            "commodities_and_fx": "Commodities_FX",
            "mutual_funds": "Mutual_Funds",
            "my_portfolio": "My_Portfolio",
            "insights_analysis": "Insights_Analysis",
            "insights___analysis": "Insights_Analysis",
            "top_10_investments": "Top_10_Investments",
            "top10_investments": "Top_10_Investments",
            "top10": "Top_10_Investments",
            "data_dictionary": "Data_Dictionary",
        }
        return mapping.get(compact, s)

    def normalize_page(self, raw: str) -> str:
        page = _strip(raw) or "Market_Leaders"
        try:
            out = self.normalize_page_name(page, allow_output_pages=True)
            if _strip(out):
                return str(out)
        except Exception:
            pass
        return self._fallback_normalize_page(page)

    def _fallback_route_family(self, page: str) -> str:
        p = self._fallback_normalize_page(page)
        if p == "Insights_Analysis":
            return "insights"
        if p == "Top_10_Investments":
            return "top10"
        if p == "Data_Dictionary":
            return "dictionary"
        return "instrument"

    def route_family(self, page: str) -> str:
        try:
            fam = _strip(self.get_route_family(page))
            if fam:
                return fam
        except Exception:
            pass
        return self._fallback_route_family(page)

    def _schema_from_data_dictionary(self, page: str) -> Tuple[List[str], List[str]]:
        if not callable(self.build_data_dictionary_rows):
            return [], []

        headers: List[str] = []
        keys: List[str] = []

        try:
            try:
                rows = self.build_data_dictionary_rows(include_meta_sheet=True)
            except TypeError:
                rows = self.build_data_dictionary_rows()

            page_norm = self.normalize_page(page)

            for row in _as_list(rows):
                if isinstance(row, Mapping):
                    sheet_name = _strip(row.get("sheet") or row.get("Sheet") or row.get("page") or "")
                    if sheet_name != page_norm:
                        continue
                    h = _strip(row.get("header") or row.get("Header"))
                    k = _strip(row.get("key") or row.get("Key"))
                else:
                    d = _model_to_dict(row)
                    sheet_name = _strip(d.get("sheet") or d.get("Sheet") or d.get("page") or "")
                    if sheet_name != page_norm:
                        continue
                    h = _strip(d.get("header") or d.get("Header"))
                    k = _strip(d.get("key") or d.get("Key"))

                if h:
                    headers.append(h)
                if k:
                    keys.append(k)

        except Exception:
            return [], []

        return headers, keys

    def schema_for_page(self, page: str) -> Tuple[List[str], List[str]]:
        headers: List[str] = []
        keys: List[str] = []

        if self._HAS_SCHEMA:
            try:
                spec = self.get_sheet_spec(page)
                cols = list(getattr(spec, "columns", None) or [])

                if not cols and isinstance(spec, Mapping):
                    cols = list(spec.get("columns") or [])
                    if not cols and (spec.get("headers") or spec.get("keys")):
                        hs = list(spec.get("headers") or [])
                        ks = list(spec.get("keys") or [])
                        if hs:
                            headers = [_strip(h) for h in hs if _strip(h)]
                        if ks:
                            keys = [_strip(k) for k in ks if _strip(k)]

                if cols:
                    for c in cols:
                        if isinstance(c, Mapping):
                            h = _strip(c.get("header"))
                            k = _strip(c.get("key"))
                        else:
                            h = _strip(getattr(c, "header", None))
                            k = _strip(getattr(c, "key", None))
                        if h:
                            headers.append(h)
                        if k:
                            keys.append(k)
            except Exception:
                headers, keys = [], []

        if not headers or not keys:
            dd_headers, dd_keys = self._schema_from_data_dictionary(page)
            if dd_headers:
                headers = dd_headers
            if dd_keys:
                keys = dd_keys

        page_norm = self.normalize_page(page)

        if page_norm == "Data_Dictionary" and (not headers or not keys):
            headers = [
                "Sheet",
                "Group",
                "Header",
                "Key",
                "DType",
                "Format",
                "Required",
                "Source",
                "Notes",
            ]
            keys = [
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

        if not headers and keys:
            headers = list(keys)
        if not keys and headers:
            keys = [h.lower().replace(" ", "_") for h in headers]

        if not headers or not keys:
            return (["Symbol", "Error"], ["symbol", "error"])

        return headers, keys

    def _page_prefers_canonical_contract(self, page_norm: str, route_family: str) -> bool:
        page2 = self.normalize_page(page_norm)
        return page2 in _STRICT_CANONICAL_PAGES or route_family in {"top10", "insights", "dictionary"}

    def _clean_contract(self, headers: Sequence[Any], keys: Sequence[Any]) -> Tuple[List[str], List[str]]:
        return _clean_string_list(headers), _clean_string_list(keys)

    def _resolve_output_contract(
        self,
        *,
        page_norm: str,
        route_family: str,
        canonical_headers: Sequence[str],
        canonical_keys: Sequence[str],
        result_headers: Sequence[Any],
        result_keys: Sequence[Any],
    ) -> Tuple[List[str], List[str], str]:
        can_headers, can_keys = self._clean_contract(canonical_headers, canonical_keys)
        res_headers, res_keys = self._clean_contract(result_headers, result_keys)

        strict = self._page_prefers_canonical_contract(page_norm, route_family)
        if strict:
            if can_headers and can_keys:
                if not res_headers or not res_keys:
                    return can_headers, can_keys, "canonical_missing_result_contract"
                if len(res_headers) != len(can_headers) or len(res_keys) != len(can_keys):
                    return can_headers, can_keys, "canonical_special_contract_mismatch"
                return can_headers, can_keys, "canonical_special_contract"
            if res_headers and res_keys:
                return res_headers, res_keys, "result_contract_no_canonical"
            return list(canonical_headers), list(canonical_keys), "fallback_no_contract"

        headers_out = res_headers or can_headers
        keys_out = res_keys or can_keys
        if not headers_out and keys_out:
            headers_out = list(keys_out)
        if not keys_out and headers_out:
            keys_out = [h.lower().replace(" ", "_") for h in headers_out]
        return headers_out, keys_out, ("result_contract" if res_keys else "canonical_contract")

    def _key_variants(self, key: str) -> List[str]:
        k = _strip(key)
        if not k:
            return []

        variants = [
            k,
            k.lower(),
            k.upper(),
            k.replace("_", " "),
            k.replace("_", "").lower(),
        ]
        for alias in _FIELD_ALIAS_HINTS.get(k, []):
            variants.extend(
                [
                    alias,
                    alias.lower(),
                    alias.upper(),
                    alias.replace("_", " "),
                    alias.replace("_", "").lower(),
                ]
            )

        seen = set()
        out: List[str] = []
        for v in variants:
            if v and v not in seen:
                seen.add(v)
                out.append(v)
        return out

    def _extract_from_raw(self, raw: Dict[str, Any], candidates: Sequence[str]) -> Any:
        raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}
        for candidate in candidates:
            if candidate in raw:
                return raw.get(candidate)
            lc = candidate.lower()
            if lc in raw_ci:
                return raw_ci.get(lc)
        return None

    def normalize_row(self, keys: Sequence[str], raw: Mapping[str, Any], *, symbol_fallback: str = "") -> Dict[str, Any]:
        raw_dict = dict(raw or {})
        out: Dict[str, Any] = {}
        for k in keys:
            out[k] = _json_safe(self._extract_from_raw(raw_dict, self._key_variants(k)))

        if symbol_fallback:
            if "symbol" in out and not out.get("symbol"):
                out["symbol"] = symbol_fallback
            if "Symbol" in out and not out.get("Symbol"):
                out["Symbol"] = symbol_fallback
            if "ticker" in out and not out.get("ticker"):
                out["ticker"] = symbol_fallback

        return out

    def extract_rows_like(self, payload: Any) -> List[Dict[str, Any]]:
        if payload is None:
            return []

        if isinstance(payload, list):
            if payload and isinstance(payload[0], (list, tuple)):
                return []
            return [x if isinstance(x, Mapping) else _model_to_dict(x) for x in payload]

        if isinstance(payload, dict):
            for name in ("rows", "data", "items", "records", "quotes"):
                value = payload.get(name)
                if isinstance(value, list):
                    if value and isinstance(value[0], (list, tuple)):
                        continue
                    return [x if isinstance(x, Mapping) else _model_to_dict(x) for x in value]

                if isinstance(value, dict):
                    for inner_name in ("rows", "data", "items", "records", "quotes"):
                        inner = value.get(inner_name)
                        if isinstance(inner, list):
                            if inner and isinstance(inner[0], (list, tuple)):
                                continue
                            return [x if isinstance(x, Mapping) else _model_to_dict(x) for x in inner]

            for name in ("payload", "result"):
                nested = payload.get(name)
                if isinstance(nested, dict):
                    rows = self.extract_rows_like(nested)
                    if rows:
                        return rows

        return []

    def extract_matrix_like(self, payload: Any) -> Optional[List[List[Any]]]:
        if isinstance(payload, dict):
            value = payload.get("rows_matrix")
            if isinstance(value, list):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]

            rows_value = payload.get("rows")
            if isinstance(rows_value, list) and rows_value and isinstance(rows_value[0], (list, tuple)):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows_value]

            for name in ("data", "payload", "result"):
                nested = payload.get(name)
                if isinstance(nested, dict):
                    mx = self.extract_matrix_like(nested)
                    if mx is not None:
                        return mx

        return None

    def extract_status_error_meta(self, payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
        if not isinstance(payload, dict):
            return "success", None, {}

        status_out = _strip(payload.get("status")) or "success"
        error_out = payload.get("error")
        meta_out = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        return status_out, (str(error_out) if error_out is not None else None), meta_out

    def matrix_to_rows(self, matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for row in matrix:
            vals = list(row) if isinstance(row, (list, tuple)) else [row]
            rows.append({k: _json_safe(vals[i] if i < len(vals) else None) for i, k in enumerate(keys)})
        return rows

    def payload_has_rows(self, payload: Any) -> bool:
        rows = self.extract_rows_like(payload)
        if rows:
            return True
        mx = self.extract_matrix_like(payload)
        return bool(mx)

    async def _call_builder_flexible(
        self,
        builder: Any,
        *,
        request: Request,
        engine: Any,
        body: Mapping[str, Any],
        limit: int,
        mode: str,
    ) -> Any:
        if not callable(builder):
            return None

        settings = None
        try:
            settings = self._get_settings_cached()
        except Exception:
            settings = None

        prepared_body = _merged_special_body(body)
        criteria = prepared_body.get("criteria") if isinstance(prepared_body.get("criteria"), dict) else None

        attempts: List[Dict[str, Any]] = [
            {
                "engine": engine,
                "request": request,
                "settings": settings,
                "body": prepared_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
                "page": prepared_body.get("page") or prepared_body.get("sheet") or prepared_body.get("sheet_name"),
                "sheet": prepared_body.get("sheet") or prepared_body.get("page"),
                "sheet_name": prepared_body.get("sheet_name") or prepared_body.get("page"),
                "name": prepared_body.get("name"),
                "tab": prepared_body.get("tab"),
                "request_id": prepared_body.get("request_id"),
                "symbols": prepared_body.get("symbols"),
                "tickers": prepared_body.get("tickers"),
            },
            {
                "request": request,
                "settings": settings,
                "body": prepared_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "engine": engine,
                "body": prepared_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "body": prepared_body,
                "criteria": criteria,
                "limit": limit,
                "mode": mode,
            },
            {
                "payload": prepared_body,
                "limit": limit,
                "mode": mode,
            },
            {
                "data": prepared_body,
                "limit": limit,
                "mode": mode,
            },
            {},
        ]

        last_exc: Optional[Exception] = None

        for kwargs in attempts:
            try:
                return await _call_func_threadsafe(builder, **kwargs)
            except TypeError as e:
                last_exc = e
                continue
            except Exception as e:
                last_exc = e
                break

        if last_exc:
            raise last_exc
        return None

    def _normalize_rows_from_any(self, rows_raw: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        for item in _as_list(rows_raw):
            row_map = item if isinstance(item, Mapping) else _model_to_dict(item)
            if row_map:
                rows_out.append(self.normalize_row(keys, row_map))
            else:
                rows_out.append(_normalize_item_to_row(item, keys))
        return rows_out

    def _rows_from_result_with_contracts(
        self,
        *,
        result: Any,
        source_keys: Sequence[str],
        output_keys: Sequence[str],
    ) -> List[Dict[str, Any]]:
        if result is None:
            return []

        if isinstance(result, list):
            if result and isinstance(result[0], (list, tuple)):
                rows_mid = self.matrix_to_rows(result, source_keys)
                return [self.normalize_row(output_keys, r) for r in rows_mid]
            return self._normalize_rows_from_any(result, output_keys)

        if isinstance(result, dict):
            rows_raw = result.get("rows")
            rows_matrix_raw = result.get("rows_matrix")

            if isinstance(rows_raw, list) and rows_raw:
                if isinstance(rows_raw[0], (list, tuple)):
                    rows_mid = self.matrix_to_rows(rows_raw, source_keys)
                    return [self.normalize_row(output_keys, r) for r in rows_mid]
                return self._normalize_rows_from_any(rows_raw, output_keys)

            if isinstance(rows_matrix_raw, list) and rows_matrix_raw:
                rows_mid = self.matrix_to_rows(rows_matrix_raw, source_keys)
                return [self.normalize_row(output_keys, r) for r in rows_mid]

            matrix_candidate = self.extract_matrix_like(result)
            if matrix_candidate:
                rows_mid = self.matrix_to_rows(matrix_candidate, source_keys)
                return [self.normalize_row(output_keys, r) for r in rows_mid]

            rows_candidate = self.extract_rows_like(result)
            if rows_candidate:
                return self._normalize_rows_from_any(rows_candidate, output_keys)

        return self._normalize_rows_from_any(result, output_keys)

    def _envelope(
        self,
        *,
        status: str,
        page: str,
        route_family: str,
        headers: Sequence[str],
        keys: Sequence[str],
        rows: Sequence[Mapping[str, Any]],
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
        dispatch: str,
        error: Optional[str] = None,
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        rows_out = [dict(r) for r in rows]
        payload = {
            "status": status,
            "page": page,
            "route_family": route_family,
            "headers": list(headers) if include_headers else [],
            "keys": list(keys),
            "display_headers": list(headers),
            "sheet_headers": list(headers),
            "column_headers": list(headers),
            "rows": rows_out,
            "rows_matrix": _rows_matrix(rows_out, keys) if include_matrix else None,
            "quotes": rows_out,
            "data": rows_out,
            "error": error,
            "version": ROUTER_VERSION,
            "request_id": request_id,
            "meta": {
                "duration_ms": (time.time() - started_at) * 1000.0,
                "count": len(rows_out),
                "dispatch": dispatch,
                "mode": mode,
                **(extra_meta or {}),
            },
        }
        return _json_safe(payload)

    def payload_from_result(
        self,
        *,
        result: Any,
        page_norm: str,
        route_family: str,
        headers: Sequence[str],
        keys: Sequence[str],
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
        dispatch: str,
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        canonical_headers = list(headers)
        canonical_keys = list(keys)

        if isinstance(result, dict):
            result_headers = _first_list(
                result.get("display_headers"),
                result.get("sheet_headers"),
                result.get("column_headers"),
                result.get("headers"),
            )
            result_keys = _first_list(result.get("keys"))

            source_headers, source_keys, contract_source = self._resolve_output_contract(
                page_norm=page_norm,
                route_family=route_family,
                canonical_headers=canonical_headers,
                canonical_keys=canonical_keys,
                result_headers=result_headers,
                result_keys=result_keys,
            )

            # source contract for matrix interpretation should still prefer explicit result keys if present
            source_keys_for_input = _clean_string_list(result_keys) or list(source_keys)

            rows_out = self._rows_from_result_with_contracts(
                result=result,
                source_keys=source_keys_for_input,
                output_keys=source_keys,
            )

            status_out, error_out, meta_in = self.extract_status_error_meta(result)
            page_out = self.normalize_page(_strip(result.get("page")) or page_norm)
            route_family_out = _strip(result.get("route_family")) or route_family

            payload = self._envelope(
                status=status_out,
                page=page_out,
                route_family=route_family_out,
                headers=source_headers,
                keys=source_keys,
                rows=rows_out,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch=dispatch,
                error=error_out,
                extra_meta={
                    **meta_in,
                    "contract_source": contract_source,
                    "canonical_headers_count": len(canonical_headers),
                    "canonical_keys_count": len(canonical_keys),
                    "result_headers_count": len(_clean_string_list(result_headers)),
                    "result_keys_count": len(_clean_string_list(result_keys)),
                    **(extra_meta or {}),
                },
            )
            return payload

        rows_out = self._normalize_rows_from_any(result, canonical_keys)
        return self._envelope(
            status="success",
            page=page_norm,
            route_family=route_family,
            headers=canonical_headers,
            keys=canonical_keys,
            rows=rows_out,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch=dispatch,
            extra_meta={
                "contract_source": "canonical_non_dict_result",
                **(extra_meta or {}),
            },
        )

    async def call_engine_page_payload_best_effort(
        self,
        *,
        engine: Any,
        page: str,
        limit: int,
        offset: int,
        mode: str,
        body: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        candidates: List[Any] = []

        if self.core_get_sheet_rows is not None:
            candidates.append(self.core_get_sheet_rows)

        if engine is not None:
            for name in ("get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows", "get_sheet"):
                fn = getattr(engine, name, None)
                if callable(fn):
                    candidates.append(fn)

        body2 = dict(body or {})
        body2.setdefault("page", page)
        body2.setdefault("sheet", page)
        body2.setdefault("sheet_name", page)

        for fn in candidates:
            call_specs = [
                ((), {"sheet": page, "sheet_name": page, "page": page, "limit": limit, "offset": offset, "mode": mode, "body": body2}),
                ((), {"sheet": page, "sheet_name": page, "page": page, "limit": limit, "offset": offset, "mode": mode}),
                ((), {"sheet": page, "page": page, "limit": limit, "offset": offset, "body": body2}),
                ((), {"sheet": page, "page": page, "limit": limit, "offset": offset}),
                ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": body2}),
                ((page,), {"limit": limit, "offset": offset, "mode": mode}),
                ((page,), {"limit": limit, "offset": offset}),
                ((page,), {}),
            ]

            last_exc: Optional[Exception] = None
            for args, kwargs in call_specs:
                try:
                    if self.engine_call_timeout_sec > 0:
                        res = await asyncio.wait_for(
                            _call_func_threadsafe(fn, *args, **kwargs),
                            timeout=self.engine_call_timeout_sec,
                        )
                    else:
                        res = await _call_func_threadsafe(fn, *args, **kwargs)

                    if isinstance(res, dict):
                        return _json_safe(res)
                    if isinstance(res, list):
                        return {"status": "success", "rows": _json_safe(res)}
                    return {"status": "success", "rows": []}
                except asyncio.TimeoutError as e:
                    last_exc = e
                    continue
                except TypeError as e:
                    last_exc = e
                    continue
                except Exception as e:
                    last_exc = e
                    break

            if last_exc:
                continue

        return None

    async def call_table_mode_best_effort(
        self,
        *,
        engine: Any,
        page: str,
        limit: int,
        offset: int,
        mode: str,
        body: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        return await self.call_engine_page_payload_best_effort(
            engine=engine,
            page=page,
            limit=limit,
            offset=offset,
            mode=mode,
            body=body,
        )

    def build_schema_only_payload(
        self,
        *,
        page_norm: str,
        route_family: str,
        headers: Sequence[str],
        keys: Sequence[str],
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
        dispatch_name: str,
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._envelope(
            status="success",
            page=page_norm,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=[],
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch=dispatch_name,
            extra_meta={"schema_only": True, "contract_source": "canonical_schema_only", **(extra_meta or {})},
        )

    async def build_dictionary_page_payload(
        self,
        *,
        engine: Any,
        request: Request,
        page_norm: str,
        headers: Sequence[str],
        keys: Sequence[str],
        body: Mapping[str, Any],
        limit: int,
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
    ) -> Dict[str, Any]:
        rows_out: List[Dict[str, Any]] = []

        if callable(self.build_data_dictionary_rows):
            try:
                rows_raw = await _call_func_threadsafe(self.build_data_dictionary_rows, include_meta_sheet=True)
                rows_out = self._normalize_rows_from_any(rows_raw, keys)
            except TypeError:
                try:
                    rows_raw = await _call_func_threadsafe(self.build_data_dictionary_rows)
                    rows_out = self._normalize_rows_from_any(rows_raw, keys)
                except Exception as e:
                    logger.warning("Data dictionary builder failed: %s", e)
                    rows_out = []
            except Exception as e:
                logger.warning("Data dictionary builder failed: %s", e)
                rows_out = []

        if rows_out:
            return self._envelope(
                status="success",
                page=page_norm,
                route_family="dictionary",
                headers=headers,
                keys=keys,
                rows=rows_out,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch="data_dictionary",
                extra_meta={"contract_source": "canonical_dictionary_builder"},
            )

        engine_payload = await self.call_engine_page_payload_best_effort(
            engine=engine,
            page=page_norm,
            limit=limit,
            offset=0,
            mode=mode,
            body=dict(body or {}),
        )
        if isinstance(engine_payload, dict):
            return self.payload_from_result(
                result=engine_payload,
                page_norm=page_norm,
                route_family="dictionary",
                headers=headers,
                keys=keys,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch="data_dictionary_engine_fallback",
                extra_meta={"warning": "local_dictionary_builder_empty"},
            )

        return self.build_schema_only_payload(
            page_norm=page_norm,
            route_family="dictionary",
            headers=headers,
            keys=keys,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch_name="data_dictionary_schema_only",
            extra_meta={"warning": "dictionary_builder_unavailable"},
        )

    async def build_special_builder_payload(
        self,
        *,
        builder: Any,
        dispatch_name: str,
        route_family: str,
        request: Request,
        engine: Any,
        page_norm: str,
        headers: Sequence[str],
        keys: Sequence[str],
        body: Mapping[str, Any],
        limit: int,
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
    ) -> Dict[str, Any]:
        async def _engine_fallback_payload(tag: str, warning_text: str) -> Optional[Dict[str, Any]]:
            engine_payload = await self.call_engine_page_payload_best_effort(
                engine=engine,
                page=page_norm,
                limit=limit,
                offset=0,
                mode=mode,
                body=dict(body or {}),
            )
            if isinstance(engine_payload, dict):
                return self.payload_from_result(
                    result=engine_payload,
                    page_norm=page_norm,
                    route_family=route_family,
                    headers=headers,
                    keys=keys,
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=started_at,
                    mode=mode,
                    dispatch=f"{dispatch_name}_{tag}",
                    extra_meta={"warning": warning_text},
                )
            return None

        if not callable(builder):
            fallback_payload = await _engine_fallback_payload("engine_fallback", f"{dispatch_name}_builder_unavailable")
            if fallback_payload is not None:
                return fallback_payload

            return self.build_schema_only_payload(
                page_norm=page_norm,
                route_family=route_family,
                headers=headers,
                keys=keys,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch_name=dispatch_name,
                extra_meta={"warning": f"{dispatch_name}_builder_unavailable"},
            )

        try:
            if self.special_builder_timeout_sec > 0:
                result = await asyncio.wait_for(
                    self._call_builder_flexible(
                        builder,
                        request=request,
                        engine=engine,
                        body=body,
                        limit=limit,
                        mode=mode,
                    ),
                    timeout=self.special_builder_timeout_sec,
                )
            else:
                result = await self._call_builder_flexible(
                    builder,
                    request=request,
                    engine=engine,
                    body=body,
                    limit=limit,
                    mode=mode,
                )
        except asyncio.TimeoutError:
            logger.warning(
                "%s builder timed out after %.2fs for page=%s",
                dispatch_name,
                self.special_builder_timeout_sec,
                page_norm,
            )
            fallback_payload = await _engine_fallback_payload("engine_fallback", f"{dispatch_name}_builder_timeout")
            if fallback_payload is not None:
                return fallback_payload

            return self.build_schema_only_payload(
                page_norm=page_norm,
                route_family=route_family,
                headers=headers,
                keys=keys,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch_name=dispatch_name,
                extra_meta={
                    "warning": f"{dispatch_name}_builder_timeout",
                    "builder_timeout_sec": self.special_builder_timeout_sec,
                },
            )
        except Exception as e:
            logger.warning("%s builder failed: %s", dispatch_name, e)
            fallback_payload = await _engine_fallback_payload("engine_fallback", f"{dispatch_name}_builder_failed")
            if fallback_payload is not None:
                return fallback_payload

            return self._envelope(
                status="partial",
                page=page_norm,
                route_family=route_family,
                headers=headers,
                keys=keys,
                rows=[],
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch=dispatch_name,
                error=f"{type(e).__name__}: {e}",
                extra_meta={"warning": f"{dispatch_name}_builder_failed"},
            )

        payload = self.payload_from_result(
            result=result,
            page_norm=page_norm,
            route_family=route_family,
            headers=headers,
            keys=keys,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch=dispatch_name,
            extra_meta={"builder_payload_preserved": True},
        )

        if not _special_schema_only_requested(body) and not self.payload_has_rows(result):
            fallback_payload = await _engine_fallback_payload("engine_fallback", f"{dispatch_name}_builder_empty")
            if fallback_payload is not None and self.payload_has_rows(fallback_payload):
                return fallback_payload

        return payload


def _build_router(reason: str, core_router: Optional[APIRouter]) -> APIRouter:
    svc = _Service(reason=reason)
    root = APIRouter(tags=["tfb"])

    enriched = APIRouter(prefix="/v1/enriched", tags=["enriched"])
    enriched_quote_alias = APIRouter(prefix="/v1/enriched_quote", tags=["enriched"])
    enriched_hyphen = APIRouter(prefix="/v1/enriched-quote", tags=["enriched"])
    legacy = APIRouter(tags=["quotes"])

    async def _health_payload() -> Dict[str, Any]:
        return _json_safe(
            {
                "status": "ok",
                "module": "routes.enriched_quote",
                "router_version": ROUTER_VERSION,
                "reason": reason,
                "schema_available": bool(getattr(svc, "_HAS_SCHEMA", False)),
            }
        )

    def _page_from_body(body: Mapping[str, Any]) -> str:
        return _strip(
            body.get("page")
            or body.get("sheet_name")
            or body.get("sheetName")
            or body.get("sheet")
            or body.get("name")
            or body.get("tab")
            or ""
        )

    @enriched.get("/health", include_in_schema=False)
    async def enriched_health() -> Dict[str, Any]:
        return await _health_payload()

    @enriched_quote_alias.get("/health", include_in_schema=False)
    async def enriched_quote_alias_health() -> Dict[str, Any]:
        return await _health_payload()

    @enriched_hyphen.get("/health", include_in_schema=False)
    async def enriched_hyphen_health() -> Dict[str, Any]:
        return await _health_payload()

    @enriched.get("/headers", include_in_schema=False)
    async def enriched_headers(page: str = Query(default="Market_Leaders")) -> Dict[str, Any]:
        page_norm = svc.normalize_page(page)
        headers, keys = svc.schema_for_page(page_norm)
        return _json_safe(
            {
                "status": "success" if headers else "degraded",
                "page": page_norm,
                "headers": headers,
                "keys": keys,
                "display_headers": headers,
                "router_version": ROUTER_VERSION,
                "reason": None if headers else reason,
                "route_family": svc.route_family(page_norm),
            }
        )

    @enriched_quote_alias.get("/headers", include_in_schema=False)
    async def enriched_quote_alias_headers(page: str = Query(default="Market_Leaders")) -> Dict[str, Any]:
        page_norm = svc.normalize_page(page)
        headers, keys = svc.schema_for_page(page_norm)
        return _json_safe(
            {
                "status": "success" if headers else "degraded",
                "page": page_norm,
                "headers": headers,
                "keys": keys,
                "display_headers": headers,
                "router_version": ROUTER_VERSION,
                "reason": None if headers else reason,
                "route_family": svc.route_family(page_norm),
            }
        )

    @enriched_hyphen.get("/headers", include_in_schema=False)
    async def enriched_hyphen_headers(page: str = Query(default="Market_Leaders")) -> Dict[str, Any]:
        page_norm = svc.normalize_page(page)
        headers, keys = svc.schema_for_page(page_norm)
        return _json_safe(
            {
                "status": "success" if headers else "degraded",
                "page": page_norm,
                "headers": headers,
                "keys": keys,
                "display_headers": headers,
                "router_version": ROUTER_VERSION,
                "reason": None if headers else reason,
                "route_family": svc.route_family(page_norm),
            }
        )

    async def _build_instrument_rows(
        page_norm: str,
        keys: Sequence[str],
        symbols: Sequence[str],
        mode_q: str,
        request: Request,
    ) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
        if not symbols:
            return [], 0, {
                "batch_rows": 0,
                "rehydrated_rows": 0,
                "sparse_after_rehydrate": 0,
            }

        engine = await svc.get_engine(request)
        if engine is None:
            rows = [
                svc.normalize_row(
                    keys,
                    {"symbol": s, "ticker": s, "error": "Data engine unavailable"},
                    symbol_fallback=s,
                )
                for s in symbols
            ]
            return rows, len(rows), {
                "batch_rows": 0,
                "rehydrated_rows": 0,
                "sparse_after_rehydrate": len(rows),
            }

        quotes_map: Dict[str, Dict[str, Any]] = {}
        batch_rows = 0
        rehydrated_rows = 0

        def _put_quote(sym_key: Any, payload: Dict[str, Any]) -> None:
            if not isinstance(payload, dict):
                payload = {}
            for candidate in _symbol_match_candidates(sym_key):
                if candidate not in quotes_map:
                    quotes_map[candidate] = dict(payload)
                else:
                    quotes_map[candidate] = _merge_payload_dicts(quotes_map.get(candidate), payload)

        def _get_quote(sym: str) -> Dict[str, Any]:
            for candidate in _symbol_match_candidates(sym):
                if candidate in quotes_map and isinstance(quotes_map[candidate], dict):
                    return quotes_map[candidate]
            return {}

        try:
            batch_fn = getattr(engine, "get_enriched_quotes_batch", None) or getattr(engine, "get_quotes_batch", None)

            if callable(batch_fn):
                got = None
                try:
                    if svc.quote_call_timeout_sec > 0:
                        got = await asyncio.wait_for(
                            _call_func_threadsafe(batch_fn, symbols=list(symbols), mode=mode_q or "", schema=page_norm),
                            timeout=svc.quote_call_timeout_sec,
                        )
                    else:
                        got = await _call_func_threadsafe(batch_fn, symbols=list(symbols), mode=mode_q or "", schema=page_norm)
                except TypeError:
                    try:
                        if svc.quote_call_timeout_sec > 0:
                            got = await asyncio.wait_for(
                                _call_func_threadsafe(batch_fn, symbols=list(symbols), mode=mode_q or ""),
                                timeout=svc.quote_call_timeout_sec,
                            )
                        else:
                            got = await _call_func_threadsafe(batch_fn, symbols=list(symbols), mode=mode_q or "")
                    except TypeError:
                        try:
                            if svc.quote_call_timeout_sec > 0:
                                got = await asyncio.wait_for(
                                    _call_func_threadsafe(batch_fn, symbols=list(symbols)),
                                    timeout=svc.quote_call_timeout_sec,
                                )
                            else:
                                got = await _call_func_threadsafe(batch_fn, symbols=list(symbols))
                        except TypeError:
                            if svc.quote_call_timeout_sec > 0:
                                got = await asyncio.wait_for(
                                    _call_func_threadsafe(batch_fn, list(symbols)),
                                    timeout=svc.quote_call_timeout_sec,
                                )
                            else:
                                got = await _call_func_threadsafe(batch_fn, list(symbols))

                if isinstance(got, dict):
                    for k, v in got.items():
                        d = v if isinstance(v, dict) else _model_to_dict(v)
                        _put_quote(k, d)
                    batch_rows = len([s for s in symbols if _get_quote(s)])
                elif isinstance(got, list):
                    for idx, item in enumerate(got):
                        d = item if isinstance(item, dict) else _model_to_dict(item)
                        key_sym = _strip(d.get("symbol") or d.get("ticker") or d.get("Symbol") or d.get("Ticker"))
                        if not key_sym and idx < len(symbols):
                            key_sym = str(symbols[idx])
                        if key_sym:
                            _put_quote(key_sym, d)
                    batch_rows = len([s for s in symbols if _get_quote(s)])
                else:
                    for s in symbols:
                        _put_quote(s, {"symbol": s, "ticker": s, "error": "batch_return_shape_unsupported"})
            else:
                for s in symbols:
                    _put_quote(s, {"symbol": s, "ticker": s, "error": "engine_missing_batch_quote_methods"})
        except Exception as e:
            for s in symbols:
                _put_quote(s, {"symbol": s, "ticker": s, "error": f"{type(e).__name__}: {e}"})

        sparse_syms = [s for s in symbols if not _get_quote(s) or _is_sparse_payload_row(_get_quote(s))]

        per_dict_fn = getattr(engine, "get_enriched_quote_dict", None) or getattr(engine, "get_quote_dict", None)
        per_fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)

        if sparse_syms and (callable(per_dict_fn) or callable(per_fn)):
            sem = asyncio.Semaphore(max(2, min(12, int(_get_float_env("TFB_ROUTE_REHYDRATE_CONCURRENCY", 6)))))

            async def _rehydrate_one(sym: str) -> None:
                nonlocal rehydrated_rows
                async with sem:
                    fresh: Dict[str, Any] = {}
                    try:
                        if callable(per_dict_fn):
                            try:
                                if svc.quote_call_timeout_sec > 0:
                                    fresh = await asyncio.wait_for(
                                        _call_func_threadsafe(per_dict_fn, sym, schema=page_norm),
                                        timeout=svc.quote_call_timeout_sec,
                                    )
                                else:
                                    fresh = await _call_func_threadsafe(per_dict_fn, sym, schema=page_norm)
                            except TypeError:
                                try:
                                    if svc.quote_call_timeout_sec > 0:
                                        fresh = await asyncio.wait_for(
                                            _call_func_threadsafe(per_dict_fn, sym),
                                            timeout=svc.quote_call_timeout_sec,
                                        )
                                    else:
                                        fresh = await _call_func_threadsafe(per_dict_fn, sym)
                                except Exception:
                                    fresh = {}
                        elif callable(per_fn):
                            try:
                                if svc.quote_call_timeout_sec > 0:
                                    fresh = await asyncio.wait_for(
                                        _call_func_threadsafe(per_fn, sym, schema=page_norm),
                                        timeout=svc.quote_call_timeout_sec,
                                    )
                                else:
                                    fresh = await _call_func_threadsafe(per_fn, sym, schema=page_norm)
                            except TypeError:
                                try:
                                    if svc.quote_call_timeout_sec > 0:
                                        fresh = await asyncio.wait_for(
                                            _call_func_threadsafe(per_fn, sym),
                                            timeout=svc.quote_call_timeout_sec,
                                        )
                                    else:
                                        fresh = await _call_func_threadsafe(per_fn, sym)
                                except Exception:
                                    fresh = {}
                        fresh = fresh if isinstance(fresh, dict) else _model_to_dict(fresh)
                    except Exception as e:
                        fresh = {"symbol": sym, "ticker": sym, "error": f"rehydrate_failed:{type(e).__name__}: {e}"}

                    old = _get_quote(sym)
                    merged = _merge_payload_dicts(old, fresh)
                    if merged:
                        _put_quote(sym, merged)
                        rehydrated_rows += 1

            await asyncio.gather(*[_rehydrate_one(s) for s in sparse_syms], return_exceptions=True)

        rows_out: List[Dict[str, Any]] = []
        errors = 0
        sparse_after_rehydrate = 0

        for s in symbols:
            raw = _get_quote(s) or {"symbol": s, "ticker": s, "error": "missing_row"}
            if raw.get("error"):
                errors += 1
            if _is_sparse_payload_row(raw):
                sparse_after_rehydrate += 1
            rows_out.append(svc.normalize_row(keys, raw, symbol_fallback=s))

        return rows_out, errors, {
            "batch_rows": batch_rows,
            "rehydrated_rows": rehydrated_rows,
            "sparse_after_rehydrate": sparse_after_rehydrate,
        }

    async def _handle_single_quote(
        request: Request,
        body: Dict[str, Any],
        page_q: str,
        mode_q: str,
        token_q: Optional[str],
        x_app_token: Optional[str],
        authorization: Optional[str],
        x_request_id: Optional[str],
    ) -> Dict[str, Any]:
        start = time.time()
        request_id = _request_id(request, x_request_id)

        token_query_used = bool(token_q and _strip(token_q))
        auth_token = _extract_token(x_app_token, authorization, token_q)
        svc.auth_guard(auth_token, authorization, x_app_token, token_query_used)

        symbol = _strip(body.get("symbol") or body.get("ticker") or body.get("requested_symbol") or "")
        if not symbol:
            syms = _get_list(body, "symbols", "tickers")
            symbol = syms[0] if syms else ""
        if not symbol:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing symbol")

        page_raw = page_q or _strip(body.get("page") or body.get("sheet_name") or body.get("sheet") or "Market_Leaders")
        page_norm = svc.normalize_page(page_raw)
        headers, keys = svc.schema_for_page(page_norm)
        route_family = svc.route_family(page_norm)

        if route_family != "instrument":
            row = svc.normalize_row(
                keys or ["symbol", "error"],
                {"symbol": symbol, "ticker": symbol, "error": f"single_quote_not_supported_for_{route_family}_page"},
                symbol_fallback=symbol,
            )
            payload = svc._envelope(
                status="partial",
                page=page_norm,
                route_family=route_family,
                headers=headers,
                keys=keys or ["symbol", "error"],
                rows=[row],
                include_headers=True,
                include_matrix=False,
                request_id=request_id,
                started_at=start,
                mode=mode_q,
                dispatch="special_guard",
                extra_meta={"contract_source": "canonical_single_quote_special_guard"},
            )
            payload["row"] = row
            payload["quote"] = row
            payload["data"] = row
            return payload

        rows_out, errors, hydrate_meta = await _build_instrument_rows(page_norm, keys or ["symbol", "error"], [symbol], mode_q, request)
        row = rows_out[0] if rows_out else svc.normalize_row(
            keys or ["symbol", "error"],
            {"symbol": symbol, "ticker": symbol, "error": "missing_row"},
            symbol_fallback=symbol,
        )
        status_out = "success" if errors == 0 and not row.get("error") else "partial"

        payload = svc._envelope(
            status=status_out,
            page=page_norm,
            route_family=route_family,
            headers=headers,
            keys=keys or ["symbol", "error"],
            rows=[row],
            include_headers=True,
            include_matrix=False,
            request_id=request_id,
            started_at=start,
            mode=mode_q,
            dispatch="instrument",
            extra_meta={"contract_source": "canonical_instrument_single", **hydrate_meta},
        )
        payload["row"] = row
        payload["quote"] = row
        payload["data"] = row
        return payload

    async def _handle_sheet_rows(
        request: Request,
        body: Dict[str, Any],
        mode_q: str,
        token_q: Optional[str],
        x_app_token: Optional[str],
        authorization: Optional[str],
        x_request_id: Optional[str],
    ) -> Dict[str, Any]:
        start = time.time()
        request_id = _request_id(request, x_request_id)

        token_query_used = bool(token_q and _strip(token_q))
        auth_token = _extract_token(x_app_token, authorization, token_q)
        svc.auth_guard(auth_token, authorization, x_app_token, token_query_used)

        body_prepared = _merged_special_body(body or {})

        page_raw = _strip(
            body_prepared.get("page")
            or body_prepared.get("sheet_name")
            or body_prepared.get("sheetName")
            or body_prepared.get("sheet")
            or body_prepared.get("name")
            or body_prepared.get("tab")
            or "Market_Leaders"
        )
        page_norm = svc.normalize_page(page_raw)
        route_family = svc.route_family(page_norm)

        symbols = _get_list(body_prepared, "symbols", "tickers", "tickers_list")
        include_matrix = _get_bool(body_prepared, "include_matrix", True)
        include_headers = _get_bool(body_prepared, "include_headers", True)

        top_n = _get_int(body_prepared, "top_n", 200)
        limit = _get_int(body_prepared, "limit", 0)
        offset = max(0, _get_int(body_prepared, "offset", 0))
        if limit > 0:
            top_n = limit
        top_n = max(1, min(5000, top_n))
        symbols = symbols[:top_n]

        headers, keys = svc.schema_for_page(page_norm)
        schema_keys = keys or ["symbol", "error"]

        engine = await svc.get_engine(request)

        if route_family != "instrument":
            try:
                if _special_schema_only_requested(body_prepared):
                    return svc.build_schema_only_payload(
                        page_norm=page_norm,
                        route_family=route_family,
                        headers=headers,
                        keys=schema_keys,
                        include_headers=include_headers,
                        include_matrix=include_matrix,
                        request_id=request_id,
                        started_at=start,
                        mode=mode_q,
                        dispatch_name=f"{route_family}_schema_only",
                    )

                if route_family == "dictionary":
                    return await svc.build_dictionary_page_payload(
                        engine=engine,
                        request=request,
                        page_norm=page_norm,
                        headers=headers,
                        keys=schema_keys,
                        body=body_prepared,
                        limit=top_n,
                        include_headers=include_headers,
                        include_matrix=include_matrix,
                        request_id=request_id,
                        started_at=start,
                        mode=mode_q,
                    )

                if route_family == "insights":
                    return await svc.build_special_builder_payload(
                        builder=svc.build_insights_rows,
                        dispatch_name="insights_builder",
                        route_family=route_family,
                        request=request,
                        engine=engine,
                        page_norm=page_norm,
                        headers=headers,
                        keys=schema_keys,
                        body=body_prepared,
                        limit=top_n,
                        include_headers=include_headers,
                        include_matrix=include_matrix,
                        request_id=request_id,
                        started_at=start,
                        mode=mode_q,
                    )

                if route_family == "top10":
                    return await svc.build_special_builder_payload(
                        builder=svc.build_top10_rows,
                        dispatch_name="top10_selector",
                        route_family=route_family,
                        request=request,
                        engine=engine,
                        page_norm=page_norm,
                        headers=headers,
                        keys=schema_keys,
                        body=body_prepared,
                        limit=top_n,
                        include_headers=include_headers,
                        include_matrix=include_matrix,
                        request_id=request_id,
                        started_at=start,
                        mode=mode_q,
                    )

                return svc._envelope(
                    status="success",
                    page=page_norm,
                    route_family=route_family,
                    headers=headers,
                    keys=schema_keys,
                    rows=[],
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=start,
                    mode=mode_q,
                    dispatch="special_unknown",
                    extra_meta={"contract_source": "canonical_special_unknown"},
                )
            except HTTPException:
                raise
            except Exception as e:
                logger.exception("Special sheet-rows dispatch failed for page=%s family=%s", page_norm, route_family)
                return svc._envelope(
                    status="partial",
                    page=page_norm,
                    route_family=route_family,
                    headers=headers,
                    keys=schema_keys,
                    rows=[],
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=start,
                    mode=mode_q,
                    dispatch="special_dispatch_safe_fallback",
                    error=f"{type(e).__name__}: {e}",
                    extra_meta={"warning": "special_dispatch_exception", "contract_source": "canonical_special_exception"},
                )

        if not symbols:
            payload = await svc.call_table_mode_best_effort(
                engine=engine,
                page=page_norm,
                limit=max(1, limit) if limit > 0 else 2000,
                offset=offset,
                mode=mode_q or "",
                body=body_prepared,
            )

            if payload is None:
                return svc._envelope(
                    status="success",
                    page=page_norm,
                    route_family=route_family,
                    headers=headers,
                    keys=schema_keys,
                    rows=[],
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=start,
                    mode=mode_q,
                    dispatch="schema_only_explicit",
                    extra_meta={"requested": 0, "errors": 0, "contract_source": "canonical_no_table_payload"},
                )

            result = svc.payload_from_result(
                result=payload,
                page_norm=page_norm,
                route_family=route_family,
                headers=headers,
                keys=schema_keys,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=start,
                mode=mode_q,
                dispatch="table_mode",
                extra_meta={"requested": 0, "errors": 0},
            )

            rows_out = result.get("rows") if isinstance(result.get("rows"), list) else []
            if offset > 0:
                rows_out = rows_out[offset:]
            if limit > 0:
                rows_out = rows_out[:limit]
            result["rows"] = rows_out
            if include_matrix:
                result["rows_matrix"] = _rows_matrix(rows_out, result.get("keys") or schema_keys)
            else:
                result["rows_matrix"] = None
            if isinstance(result.get("meta"), dict):
                result["meta"]["count"] = len(rows_out)
                result["meta"]["rows"] = len(rows_out)
            return _json_safe(result)

        rows_out, errors, hydrate_meta = await _build_instrument_rows(page_norm, schema_keys, symbols, mode_q, request)
        status_out = "success" if errors == 0 else ("partial" if errors < len(symbols) else "error")

        return svc._envelope(
            status=status_out,
            page=page_norm,
            route_family=route_family,
            headers=headers,
            keys=schema_keys,
            rows=rows_out,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode_q,
            dispatch="instrument",
            error=f"{errors} errors" if errors else None,
            extra_meta={"requested": len(symbols), "errors": errors, "contract_source": "canonical_instrument_batch", **hydrate_meta},
        )

    async def _handle_hyphen_bridge(
        request: Request,
        body: Dict[str, Any],
        mode_q: str,
        token_q: Optional[str],
        x_app_token: Optional[str],
        authorization: Optional[str],
        x_request_id: Optional[str],
    ) -> Dict[str, Any]:
        start = time.time()
        request_id = _request_id(request, x_request_id)

        prepared = dict(body or {})

        page_raw = _page_from_body(prepared)
        page_norm = svc.normalize_page(page_raw or "Market_Leaders")
        route_family = svc.route_family(page_norm)

        symbol = _strip(prepared.get("symbol") or prepared.get("ticker") or prepared.get("requested_symbol"))
        list_symbols = _get_list(prepared, "symbols", "tickers", "tickers_list")
        explicit_selector = bool(page_raw)

        force_sheet_rows = explicit_selector or bool(list_symbols) or (route_family != "instrument")

        try:
            if symbol and not force_sheet_rows:
                return await _handle_single_quote(
                    request=request,
                    body=prepared,
                    page_q=_strip(prepared.get("page")),
                    mode_q=mode_q,
                    token_q=token_q,
                    x_app_token=x_app_token,
                    authorization=authorization,
                    x_request_id=x_request_id,
                )

            return await _handle_sheet_rows(
                request=request,
                body=prepared,
                mode_q=mode_q,
                token_q=token_q,
                x_app_token=x_app_token,
                authorization=authorization,
                x_request_id=x_request_id,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Hyphen root bridge failed for page=%s family=%s", page_norm, route_family)
            headers, keys = svc.schema_for_page(page_norm)
            schema_keys = keys or ["symbol", "error"]

            return svc._envelope(
                status="partial",
                page=page_norm,
                route_family=route_family,
                headers=headers,
                keys=schema_keys,
                rows=[],
                include_headers=True,
                include_matrix=True,
                request_id=request_id,
                started_at=start,
                mode=mode_q,
                dispatch="hyphen_bridge_safe_fallback",
                error=f"{type(e).__name__}: {e}",
                extra_meta={
                    "warning": "hyphen_bridge_exception",
                    "forced_sheet_rows": force_sheet_rows,
                    "contract_source": "canonical_hyphen_exception",
                },
            )

    @enriched.post("/quote")
    async def enriched_quote_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.post("/quote")
    async def enriched_quote_alias_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched_hyphen.post("/quote")
    async def enriched_hyphen_quote_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched.get("/quote", include_in_schema=False)
    async def enriched_quote_get(
        request: Request,
        symbol: Optional[str] = Query(default=None),
        ticker: Optional[str] = Query(default=None),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_quote(symbol=symbol, ticker=ticker, page=page)
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.get("/quote", include_in_schema=False)
    async def enriched_quote_alias_get(
        request: Request,
        symbol: Optional[str] = Query(default=None),
        ticker: Optional[str] = Query(default=None),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_quote(symbol=symbol, ticker=ticker, page=page)
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched_hyphen.get("/quote", include_in_schema=False)
    async def enriched_hyphen_quote_get(
        request: Request,
        symbol: Optional[str] = Query(default=None),
        ticker: Optional[str] = Query(default=None),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_quote(symbol=symbol, ticker=ticker, page=page)
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched.post("/quotes", include_in_schema=False)
    async def enriched_quotes_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.post("/quotes", include_in_schema=False)
    async def enriched_quote_alias_quotes_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_hyphen.post("/quotes", include_in_schema=False)
    async def enriched_hyphen_quotes_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched.get("/quotes", include_in_schema=False)
    async def enriched_quotes_get(
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_sheet_rows(
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
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.get("/quotes", include_in_schema=False)
    async def enriched_quote_alias_quotes_get(
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_sheet_rows(
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
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_hyphen.get("/quotes", include_in_schema=False)
    async def enriched_hyphen_quotes_get(
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_sheet_rows(
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
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched.post("/sheet-rows")
    async def enriched_sheet_rows_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.post("/sheet-rows")
    async def enriched_quote_alias_sheet_rows_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_hyphen.post("/sheet-rows")
    async def enriched_hyphen_sheet_rows_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched.get("/sheet-rows")
    async def enriched_sheet_rows_get(
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_sheet_rows(
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
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_quote_alias.get("/sheet-rows")
    async def enriched_quote_alias_sheet_rows_get(
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_sheet_rows(
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
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched_hyphen.get("/sheet-rows")
    async def enriched_hyphen_sheet_rows_get(
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_sheet_rows(
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
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @root.post("/v1/enriched-quote")
    async def enriched_hyphen_root_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_hyphen_bridge(
            request=request,
            body=body,
            mode_q=mode,
            token_q=token,
            x_app_token=x_app_token,
            authorization=authorization,
            x_request_id=x_request_id,
        )

    @root.get("/v1/enriched-quote")
    async def enriched_hyphen_root_get(
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
        include_matrix: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None),
        offset: Optional[int] = Query(default=None),
        top_n: Optional[int] = Query(default=None),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_hyphen_bridge(
            symbol=symbol,
            ticker=ticker,
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
        )
        return await _handle_hyphen_bridge(
            request=request,
            body=body,
            mode_q=mode,
            token_q=token,
            x_app_token=x_app_token,
            authorization=authorization,
            x_request_id=x_request_id,
        )

    @legacy.post("/quote")
    async def quote_alias_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @legacy.get("/quote", include_in_schema=False)
    async def quote_alias_get(
        request: Request,
        symbol: Optional[str] = Query(default=None),
        ticker: Optional[str] = Query(default=None),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_quote(symbol=symbol, ticker=ticker, page=page)
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @legacy.post("/quotes")
    async def quotes_primary_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @legacy.get("/quotes", include_in_schema=False)
    async def quotes_primary_get(
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_sheet_rows(
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
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @legacy.post("/sheet-rows", include_in_schema=False)
    async def sheet_rows_alias_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @legacy.get("/sheet-rows", include_in_schema=False)
    async def sheet_rows_alias_get(
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token (query only if allowed)"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_query_body_for_sheet_rows(
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
        )
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    root.include_router(enriched)
    root.include_router(enriched_quote_alias)
    root.include_router(enriched_hyphen)
    root.include_router(legacy)

    if isinstance(core_router, APIRouter):
        try:
            root.include_router(core_router)
        except Exception as e:
            logger.warning("Including core enriched router failed: %s", e)

    return root


def get_router() -> APIRouter:
    global router
    if router is not None:
        return router

    core_r: Optional[APIRouter] = None
    reason = "wrapper_ok"

    try:
        mod = importlib.import_module("core.enriched_quote")
        r = getattr(mod, "router", None)
        if r is None:
            fn = getattr(mod, "get_router", None)
            if callable(fn):
                r = fn()
        if isinstance(r, APIRouter):
            core_r = r
        else:
            reason = "core.enriched_quote_no_router"
    except Exception as e:
        reason = f"core_import_failed: {type(e).__name__}: {e}"
        core_r = None

    router = _build_router(reason=reason, core_router=core_r)
    return router


def mount(app: Any) -> None:
    r = get_router()
    try:
        app.include_router(r)
        logger.info("Mounted enriched wrapper router. version=%s", ROUTER_VERSION)
    except Exception as e:
        logger.error("Mount failed for routes.enriched_quote: %s", e)
        raise


__all__ = ["ROUTER_VERSION", "get_router", "mount", "router"]
