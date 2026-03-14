#!/usr/bin/env python3
# routes/enriched_quote.py
"""
================================================================================
TFB Enriched Quote Routes Wrapper — v7.0.0
================================================================================
LEANER • SCHEMA-AWARE • PAGE-DISPATCH SAFE • GET+POST COMPATIBLE
ROOT /v1/enriched-quote COMPATIBLE • SPECIAL-PAGE SAFE • THREAD-OFFLOADED
JSON-SAFE • TABLE-MODE SAFE • INSTRUMENT HYDRATION SAFE

Design goals
------------
- Keep public endpoint compatibility already used by tests/legacy clients
- Keep enriched route strong for normal instrument pages
- Use direct page dispatch from page_catalog + schema_registry
- Keep special-page handling explicit:
    - Insights_Analysis
    - Top_10_Investments
    - Data_Dictionary
- Reduce oversized compatibility/fallback layers
- Preserve stable response envelope:
    headers + keys + rows + rows_matrix + quotes + data + meta
- No network calls at import time
================================================================================
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

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "symbol_code"],
    "ticker": ["symbol", "code", "instrument", "security"],
    "name": ["company_name", "long_name", "instrument_name", "security_name"],
    "current_price": ["price", "last_price", "last", "close", "market_price", "current", "spot", "nav"],
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
    except Exception:
        return ""
    return s if s and s.lower() != "none" else ""


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


def _model_to_dict(obj: Any) -> Dict[str, Any]:
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
    if _strip(x_request_id):
        return _strip(x_request_id)

    try:
        rid = _strip(getattr(request.state, "request_id", ""))
        if rid:
            return rid
    except Exception:
        pass

    return uuid.uuid4().hex[:12]


def _split_csv(value: str) -> List[str]:
    raw = (value or "").replace(";", ",").replace("\n", ",").replace("\t", ",")
    out: List[str] = []
    seen = set()
    for part in raw.split(","):
        s = _strip(part)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _list_from_body(body: Mapping[str, Any], *keys: str) -> List[str]:
    for key in keys:
        value = body.get(key)

        if isinstance(value, list):
            out: List[str] = []
            seen = set()
            for item in value:
                s = _strip(item)
                if s and s not in seen:
                    seen.add(s)
                    out.append(s)
            if out:
                return out

        if isinstance(value, str) and value.strip():
            vals = _split_csv(value)
            if vals:
                return vals

    return []


def _bool_from_body(body: Mapping[str, Any], key: str, default: bool) -> bool:
    value = body.get(key)

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        try:
            return bool(int(value))
        except Exception:
            return default

    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False

    return default


def _int_from_body(body: Mapping[str, Any], key: str, default: int) -> int:
    value = body.get(key)
    try:
        if isinstance(value, bool):
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str) and value.strip():
            return int(float(value.strip()))
    except Exception:
        pass
    return default


def _first_list(*values: Any) -> List[Any]:
    for value in values:
        if isinstance(value, list):
            return value
    return []


def _rows_matrix(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows]


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

    out: List[str] = []
    seen = set()
    for item in items:
        s = _strip(item)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
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
    )
    return sum(1 for key in important if d.get(key) not in (None, "", [], {}))


def _is_sparse_payload_row(d: Optional[Dict[str, Any]], threshold: int = 10) -> bool:
    return _payload_row_richness(d) < threshold


def _merge_dicts(base: Optional[Dict[str, Any]], addon: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base or {})
    if not isinstance(addon, dict):
        return out

    for key, value in addon.items():
        if value is not None and value != "":
            out[key] = value
    return out


def _special_schema_only_requested(body: Mapping[str, Any]) -> bool:
    return any(_bool_from_body(body, key, False) for key in ("schema_only", "headers_only", "keys_only"))


def _merged_special_body(body: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(body or {})
    criteria: Dict[str, Any] = {}

    def _merge_from(value: Any) -> None:
        if isinstance(value, Mapping):
            for k, v in value.items():
                if v is not None:
                    criteria[k] = v

    _merge_from(body.get("criteria"))
    _merge_from(body.get("filters"))

    settings = body.get("settings")
    if isinstance(settings, Mapping):
        _merge_from(settings)
        _merge_from(settings.get("criteria"))

    payload = body.get("payload")
    if isinstance(payload, Mapping):
        _merge_from(payload)
        _merge_from(payload.get("criteria"))

    if criteria:
        out["criteria"] = criteria
        for key in (
            "pages_selected",
            "pages",
            "selected_pages",
            "direct_symbols",
            "symbols",
            "tickers",
            "invest_period_days",
            "investment_period_days",
            "horizon_days",
            "period_days",
            "top_n",
            "min_expected_roi",
            "max_risk_score",
            "min_confidence",
            "min_volume",
            "use_liquidity_tiebreak",
            "enforce_risk_confidence",
        ):
            if out.get(key) in (None, "", [], {}):
                if key in criteria:
                    out[key] = criteria[key]

    return out


class _Service:
    def __init__(self, reason: str):
        self.reason = reason
        self.special_builder_timeout_sec = self._env_float("TFB_SPECIAL_BUILDER_TIMEOUT_SEC", 45.0)
        self.engine_call_timeout_sec = self._env_float("TFB_ENGINE_CALL_TIMEOUT_SEC", 45.0)
        self.quote_call_timeout_sec = self._env_float("TFB_QUOTE_CALL_TIMEOUT_SEC", 45.0)
        self.rehydrate_concurrency = max(2, min(12, int(self._env_float("TFB_ROUTE_REHYDRATE_CONCURRENCY", 6))))

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
            from core.sheets.page_catalog import get_route_family, is_instrument_page, normalize_page_name  # type: ignore
            from core.sheets.schema_registry import get_sheet_spec  # type: ignore
            self._has_schema = True
        except Exception as e:
            self._has_schema = False
            self._schema_error = e

            def get_route_family(_: str) -> str:
                return "instrument"

            def is_instrument_page(_: str) -> bool:
                return True

            def normalize_page_name(page: str, allow_output_pages: bool = True) -> str:
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

        self.build_insights_rows = self._resolve_first_callable(
            "core.analysis.insights_builder",
            ("build_insights_analysis_rows", "build_insights_rows"),
        )
        self.build_top10_rows = self._resolve_first_callable(
            "core.analysis.top10_selector",
            ("build_top10_rows", "build_top10_output_rows"),
        )

        try:
            from core.data_engine import get_sheet_rows as core_get_sheet_rows  # type: ignore
        except Exception:
            core_get_sheet_rows = None  # type: ignore
        self.core_get_sheet_rows = core_get_sheet_rows

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        try:
            raw = os.getenv(name, "").strip()
            return float(raw) if raw else float(default)
        except Exception:
            return float(default)

    @staticmethod
    def _resolve_first_callable(module_name: str, names: Sequence[str]) -> Any:
        try:
            mod = importlib.import_module(module_name)
        except Exception:
            return None

        for name in names:
            fn = getattr(mod, name, None)
            if callable(fn):
                return fn
        return None

    def auth_guard(
        self,
        request: Request,
        token_query: Optional[str],
        x_app_token: Optional[str],
        authorization: Optional[str],
    ) -> None:
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

        token = _strip(x_app_token)
        auth = _strip(authorization)

        if auth.lower().startswith("bearer "):
            token = _strip(auth.split(" ", 1)[1])
        elif not token and allow_query:
            token = _strip(token_query)

        headers = dict(request.headers)
        path = str(getattr(getattr(request, "url", None), "path", "") or "")

        attempts = [
            {"token": token, "authorization": authorization, "headers": headers, "path": path, "request": request, "settings": settings},
            {"token": token, "authorization": authorization, "headers": headers, "path": path, "request": request},
            {"token": token, "authorization": authorization, "headers": headers, "path": path},
            {"token": token, "authorization": authorization, "headers": headers},
            {"token": token, "authorization": authorization},
            {"token": token},
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

    async def get_engine(self, request: Optional[Request]) -> Any:
        try:
            if request is not None:
                state = getattr(request.app, "state", None)
                if state is not None and getattr(state, "engine", None) is not None:
                    return state.engine
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
        try:
            normalized = self.normalize_page_name(page, allow_output_pages=True)
            if _strip(normalized):
                return str(normalized)
        except Exception:
            pass

        compact = page.replace("&", "_").replace("-", "_").replace("/", "_").replace(" ", "_").lower()
        mapping = {
            "market_leaders": "Market_Leaders",
            "global_markets": "Global_Markets",
            "commodities_fx": "Commodities_FX",
            "commodities_and_fx": "Commodities_FX",
            "mutual_funds": "Mutual_Funds",
            "my_portfolio": "My_Portfolio",
            "insights_analysis": "Insights_Analysis",
            "top10": "Top_10_Investments",
            "top10_investments": "Top_10_Investments",
            "top_10_investments": "Top_10_Investments",
            "data_dictionary": "Data_Dictionary",
        }
        return mapping.get(compact, page)

    def route_family(self, page: str) -> str:
        try:
            family = _strip(self.get_route_family(page))
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

    def _schema_from_data_dictionary(self, page: str) -> Tuple[List[str], List[str]]:
        if not callable(self.build_data_dictionary_rows):
            return [], []

        try:
            try:
                rows = self.build_data_dictionary_rows(include_meta_sheet=True)
            except TypeError:
                rows = self.build_data_dictionary_rows()
        except Exception:
            return [], []

        headers: List[str] = []
        keys: List[str] = []
        for row in rows if isinstance(rows, list) else []:
            d = row if isinstance(row, Mapping) else _model_to_dict(row)
            sheet_name = _strip(d.get("sheet") or d.get("Sheet") or d.get("page"))
            if sheet_name != page:
                continue
            h = _strip(d.get("header") or d.get("Header"))
            k = _strip(d.get("key") or d.get("Key"))
            if h:
                headers.append(h)
            if k:
                keys.append(k)

        return headers, keys

    def schema_for_page(self, page: str) -> Tuple[List[str], List[str]]:
        headers: List[str] = []
        keys: List[str] = []

        if self._has_schema:
            try:
                spec = self.get_sheet_spec(page)
                cols = list(getattr(spec, "columns", None) or [])

                if not cols and isinstance(spec, Mapping):
                    cols = list(spec.get("columns") or [])
                    if not cols:
                        headers = [_strip(x) for x in list(spec.get("headers") or []) if _strip(x)]
                        keys = [_strip(x) for x in list(spec.get("keys") or []) if _strip(x)]

                for col in cols:
                    d = col if isinstance(col, Mapping) else _model_to_dict(col)
                    h = _strip(d.get("header") or getattr(col, "header", None))
                    k = _strip(d.get("key") or getattr(col, "key", None))
                    if h:
                        headers.append(h)
                    if k:
                        keys.append(k)
            except Exception:
                headers, keys = [], []

        if not headers or not keys:
            dd_headers, dd_keys = self._schema_from_data_dictionary(page)
            headers = headers or dd_headers
            keys = keys or dd_keys

        if page == "Data_Dictionary" and (not headers or not keys):
            headers = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
            keys = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]

        if not headers and keys:
            headers = list(keys)
        if not keys and headers:
            keys = [h.lower().replace(" ", "_") for h in headers]

        return (headers or ["Symbol", "Error"], keys or ["symbol", "error"])

    def _key_variants(self, key: str) -> List[str]:
        base = _strip(key)
        if not base:
            return []

        variants = [base, base.lower(), base.upper(), base.replace("_", " "), base.replace("_", "").lower()]
        for alias in _FIELD_ALIAS_HINTS.get(base, []):
            variants.extend([alias, alias.lower(), alias.upper(), alias.replace("_", " "), alias.replace("_", "").lower()])

        out: List[str] = []
        seen = set()
        for value in variants:
            s = _strip(value)
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        return out

    def _extract_from_raw(self, raw: Dict[str, Any], candidates: Sequence[str]) -> Any:
        lowered = {str(k).strip().lower(): v for k, v in raw.items()}
        for candidate in candidates:
            if candidate in raw:
                return raw[candidate]
            lc = candidate.lower()
            if lc in lowered:
                return lowered[lc]
        return None

    def normalize_row(self, keys: Sequence[str], raw: Mapping[str, Any], *, symbol_fallback: str = "") -> Dict[str, Any]:
        raw_dict = dict(raw or {})
        row = {k: _json_safe(self._extract_from_raw(raw_dict, self._key_variants(k))) for k in keys}

        if symbol_fallback:
            if "symbol" in row and not row.get("symbol"):
                row["symbol"] = symbol_fallback
            if "ticker" in row and not row.get("ticker"):
                row["ticker"] = symbol_fallback

        return row

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
                    nested = self.extract_rows_like(value)
                    if nested:
                        return nested

            for name in ("payload", "result"):
                if isinstance(payload.get(name), dict):
                    nested = self.extract_rows_like(payload.get(name))
                    if nested:
                        return nested

        return []

    def extract_matrix_like(self, payload: Any) -> Optional[List[List[Any]]]:
        if isinstance(payload, dict):
            value = payload.get("rows_matrix")
            if isinstance(value, list):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]

            rows = payload.get("rows")
            if isinstance(rows, list) and rows and isinstance(rows[0], (list, tuple)):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows]

            for name in ("data", "payload", "result"):
                nested = payload.get(name)
                if isinstance(nested, dict):
                    found = self.extract_matrix_like(nested)
                    if found is not None:
                        return found

        return None

    def extract_status_error_meta(self, payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
        if not isinstance(payload, dict):
            return "success", None, {}

        status_out = _strip(payload.get("status")) or "success"
        error_out = payload.get("error")
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        return status_out, (str(error_out) if error_out is not None else None), meta

    def matrix_to_rows(self, matrix: Sequence[Sequence[Any]], keys: Sequence[str]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for row in matrix:
            vals = list(row) if isinstance(row, (list, tuple)) else [row]
            rows.append({k: _json_safe(vals[i] if i < len(vals) else None) for i, k in enumerate(keys)})
        return rows

    def payload_has_rows(self, payload: Any) -> bool:
        return bool(self.extract_rows_like(payload) or self.extract_matrix_like(payload))

    def envelope(
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
        return _json_safe(
            {
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
        )

    def payload_from_result(
        self,
        *,
        result: Any,
        page: str,
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
        if isinstance(result, dict):
            result_headers = _first_list(
                result.get("display_headers"),
                result.get("sheet_headers"),
                result.get("column_headers"),
                result.get("headers"),
            )
            result_keys = _first_list(result.get("keys"))

            headers_out = [str(x) for x in (result_headers or list(headers)) if _strip(x)] or list(headers)
            keys_out = [str(x) for x in (result_keys or list(keys)) if _strip(x)] or list(keys)

            rows_raw = result.get("rows")
            rows_matrix_raw = result.get("rows_matrix")
            rows_out: List[Dict[str, Any]] = []

            if isinstance(rows_raw, list) and rows_raw:
                for item in rows_raw:
                    d = item if isinstance(item, Mapping) else _model_to_dict(item)
                    rows_out.append(self.normalize_row(keys_out, d) if d else {k: None for k in keys_out})
            elif isinstance(rows_matrix_raw, list) and rows_matrix_raw:
                rows_out = self.matrix_to_rows(rows_matrix_raw, keys_out)
            else:
                extracted = self.extract_rows_like(result)
                if extracted:
                    rows_out = [self.normalize_row(keys_out, r) for r in extracted]
                else:
                    matrix = self.extract_matrix_like(result)
                    if matrix:
                        rows_out = self.matrix_to_rows(matrix, keys_out)

            status_out, error_out, meta_in = self.extract_status_error_meta(result)
            return self.envelope(
                status=status_out,
                page=_strip(result.get("page")) or page,
                route_family=_strip(result.get("route_family")) or route_family,
                headers=headers_out,
                keys=keys_out,
                rows=rows_out,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch=dispatch,
                error=error_out,
                extra_meta={**meta_in, **(extra_meta or {})},
            )

        rows_out: List[Dict[str, Any]] = []
        for item in (result if isinstance(result, list) else [result]):
            d = item if isinstance(item, Mapping) else _model_to_dict(item)
            rows_out.append(self.normalize_row(keys, d) if d else {k: None for k in keys})

        return self.envelope(
            status="success",
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows_out,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch=dispatch,
            extra_meta=extra_meta,
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

        if callable(self.core_get_sheet_rows):
            candidates.append(self.core_get_sheet_rows)

        if engine is not None:
            for name in ("get_sheet_rows", "get_page_rows", "sheet_rows", "build_sheet_rows", "get_sheet"):
                fn = getattr(engine, name, None)
                if callable(fn):
                    candidates.append(fn)

        payload = dict(body or {})
        payload.setdefault("page", page)
        payload.setdefault("sheet", page)
        payload.setdefault("sheet_name", page)

        signatures = [
            ((), {"sheet": page, "page": page, "sheet_name": page, "limit": limit, "offset": offset, "mode": mode, "body": payload}),
            ((), {"sheet": page, "page": page, "sheet_name": page, "limit": limit, "offset": offset, "mode": mode}),
            ((), {"sheet": page, "page": page, "limit": limit, "offset": offset}),
            ((page,), {"limit": limit, "offset": offset, "mode": mode, "body": payload}),
            ((page,), {"limit": limit, "offset": offset, "mode": mode}),
            ((page,), {"limit": limit, "offset": offset}),
            ((page,), {}),
        ]

        for fn in candidates:
            for args, kwargs in signatures:
                try:
                    if self.engine_call_timeout_sec > 0:
                        res = await asyncio.wait_for(
                            _call_maybe_async(fn, *args, **kwargs),
                            timeout=self.engine_call_timeout_sec,
                        )
                    else:
                        res = await _call_maybe_async(fn, *args, **kwargs)

                    if isinstance(res, dict):
                        return _json_safe(res)
                    if isinstance(res, list):
                        return {"status": "success", "rows": _json_safe(res)}
                    return {"status": "success", "rows": []}
                except TypeError:
                    continue
                except asyncio.TimeoutError:
                    break
                except Exception:
                    break

        return None

    async def call_builder_best_effort(
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

        prepared = _merged_special_body(body)
        criteria = prepared.get("criteria") if isinstance(prepared.get("criteria"), dict) else None

        attempts = [
            {"engine": engine, "request": request, "settings": settings, "body": prepared, "criteria": criteria, "limit": limit, "mode": mode},
            {"request": request, "settings": settings, "body": prepared, "criteria": criteria, "limit": limit, "mode": mode},
            {"engine": engine, "body": prepared, "criteria": criteria, "limit": limit, "mode": mode},
            {"body": prepared, "criteria": criteria, "limit": limit, "mode": mode},
            {"payload": prepared, "limit": limit, "mode": mode},
            {},
        ]

        last_exc: Optional[Exception] = None
        for kwargs in attempts:
            try:
                return await _call_maybe_async(builder, **kwargs)
            except TypeError as e:
                last_exc = e
                continue
            except Exception as e:
                last_exc = e
                break

        if last_exc is not None:
            raise last_exc
        return None

    async def build_special_payload(
        self,
        *,
        builder: Any,
        builder_name: str,
        request: Request,
        engine: Any,
        page: str,
        route_family: str,
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
        async def _engine_fallback(reason_text: str) -> Optional[Dict[str, Any]]:
            payload = await self.call_engine_page_payload_best_effort(
                engine=engine,
                page=page,
                limit=limit,
                offset=0,
                mode=mode,
                body=dict(body or {}),
            )
            if isinstance(payload, dict):
                return self.payload_from_result(
                    result=payload,
                    page=page,
                    route_family=route_family,
                    headers=headers,
                    keys=keys,
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=started_at,
                    mode=mode,
                    dispatch=f"{builder_name}_engine_fallback",
                    extra_meta={"warning": reason_text},
                )
            return None

        if not callable(builder):
            fallback = await _engine_fallback(f"{builder_name}_unavailable")
            if fallback is not None:
                return fallback

            return self.envelope(
                status="partial",
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                rows=[],
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch=f"{builder_name}_schema_only",
                error=f"{builder_name} unavailable",
                extra_meta={"schema_only": True},
            )

        try:
            if self.special_builder_timeout_sec > 0:
                result = await asyncio.wait_for(
                    self.call_builder_best_effort(
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
                result = await self.call_builder_best_effort(
                    builder,
                    request=request,
                    engine=engine,
                    body=body,
                    limit=limit,
                    mode=mode,
                )
        except asyncio.TimeoutError:
            fallback = await _engine_fallback(f"{builder_name}_timeout")
            if fallback is not None:
                return fallback

            return self.envelope(
                status="partial",
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                rows=[],
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch=builder_name,
                error=f"{builder_name} timed out",
                extra_meta={"builder_timeout_sec": self.special_builder_timeout_sec},
            )
        except Exception as e:
            fallback = await _engine_fallback(f"{builder_name}_failed")
            if fallback is not None:
                return fallback

            return self.envelope(
                status="partial",
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                rows=[],
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=started_at,
                mode=mode,
                dispatch=builder_name,
                error=f"{type(e).__name__}: {e}",
            )

        payload = self.payload_from_result(
            result=result,
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch=builder_name,
            extra_meta={"builder_payload_preserved": True},
        )

        if not _special_schema_only_requested(body) and not self.payload_has_rows(result):
            fallback = await _engine_fallback(f"{builder_name}_empty")
            if fallback is not None and self.payload_has_rows(fallback):
                return fallback

        return payload


def _build_router(reason: str, core_router: Optional[APIRouter]) -> APIRouter:
    svc = _Service(reason)
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
                "schema_available": bool(svc._has_schema),
            }
        )

    def _page_from_body(body: Mapping[str, Any]) -> str:
        return _strip(body.get("page") or body.get("sheet_name") or body.get("sheet") or body.get("name") or body.get("tab"))

    def _collect_sheet_rows_body(
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

    def _collect_quote_body(*, symbol: Optional[str], ticker: Optional[str], page: Optional[str]) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if symbol not in (None, ""):
            body["symbol"] = symbol
        if ticker not in (None, ""):
            body["ticker"] = ticker
        if page not in (None, ""):
            body["page"] = page
        return body

    async def _build_instrument_rows(
        page: str,
        keys: Sequence[str],
        symbols: Sequence[str],
        mode: str,
        request: Request,
    ) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
        if not symbols:
            return [], 0, {"batch_rows": 0, "rehydrated_rows": 0, "sparse_after_rehydrate": 0}

        engine = await svc.get_engine(request)
        if engine is None:
            rows = [
                svc.normalize_row(keys, {"symbol": s, "ticker": s, "error": "Data engine unavailable"}, symbol_fallback=s)
                for s in symbols
            ]
            return rows, len(rows), {"batch_rows": 0, "rehydrated_rows": 0, "sparse_after_rehydrate": len(rows)}

        quotes_map: Dict[str, Dict[str, Any]] = {}
        batch_rows = 0
        rehydrated_rows = 0

        def _put_quote(sym_key: Any, payload: Dict[str, Any]) -> None:
            payload = payload if isinstance(payload, dict) else {}
            for candidate in _symbol_match_candidates(sym_key):
                quotes_map[candidate] = _merge_dicts(quotes_map.get(candidate), payload)

        def _get_quote(sym: str) -> Dict[str, Any]:
            for candidate in _symbol_match_candidates(sym):
                if candidate in quotes_map and isinstance(quotes_map[candidate], dict):
                    return quotes_map[candidate]
            return {}

        batch_fn = None
        for name in ("get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch"):
            candidate = getattr(engine, name, None)
            if callable(candidate):
                batch_fn = candidate
                break

        if callable(batch_fn):
            signatures = [
                ((), {"symbols": list(symbols), "mode": mode, "schema": page}),
                ((), {"symbols": list(symbols), "mode": mode}),
                ((), {"symbols": list(symbols)}),
                ((list(symbols),), {}),
            ]

            for args, kwargs in signatures:
                try:
                    if svc.quote_call_timeout_sec > 0:
                        got = await asyncio.wait_for(
                            _call_maybe_async(batch_fn, *args, **kwargs),
                            timeout=svc.quote_call_timeout_sec,
                        )
                    else:
                        got = await _call_maybe_async(batch_fn, *args, **kwargs)

                    if isinstance(got, dict):
                        for k, v in got.items():
                            _put_quote(k, v if isinstance(v, dict) else _model_to_dict(v))
                    elif isinstance(got, list):
                        for idx, item in enumerate(got):
                            d = item if isinstance(item, dict) else _model_to_dict(item)
                            sym_key = _strip(d.get("symbol") or d.get("ticker") or d.get("Symbol") or d.get("Ticker"))
                            if not sym_key and idx < len(symbols):
                                sym_key = symbols[idx]
                            if sym_key:
                                _put_quote(sym_key, d)
                    break
                except TypeError:
                    continue
                except Exception as e:
                    for s in symbols:
                        _put_quote(s, {"symbol": s, "ticker": s, "error": f"{type(e).__name__}: {e}"})
                    break
        else:
            for s in symbols:
                _put_quote(s, {"symbol": s, "ticker": s, "error": "engine_missing_batch_quote_methods"})

        batch_rows = len([s for s in symbols if _get_quote(s)])
        sparse_symbols = [s for s in symbols if not _get_quote(s) or _is_sparse_payload_row(_get_quote(s))]

        per_fn = None
        for name in ("get_enriched_quote_dict", "get_quote_dict", "get_enriched_quote", "get_quote"):
            candidate = getattr(engine, name, None)
            if callable(candidate):
                per_fn = candidate
                break

        if sparse_symbols and callable(per_fn):
            sem = asyncio.Semaphore(svc.rehydrate_concurrency)

            async def _rehydrate(sym: str) -> None:
                nonlocal rehydrated_rows
                async with sem:
                    signatures = [((sym,), {"schema": page}), ((sym,), {})]
                    fresh: Dict[str, Any] = {}

                    for args, kwargs in signatures:
                        try:
                            if svc.quote_call_timeout_sec > 0:
                                got = await asyncio.wait_for(
                                    _call_maybe_async(per_fn, *args, **kwargs),
                                    timeout=svc.quote_call_timeout_sec,
                                )
                            else:
                                got = await _call_maybe_async(per_fn, *args, **kwargs)
                            fresh = got if isinstance(got, dict) else _model_to_dict(got)
                            break
                        except TypeError:
                            continue
                        except Exception as e:
                            fresh = {"symbol": sym, "ticker": sym, "error": f"rehydrate_failed:{type(e).__name__}: {e}"}
                            break

                    _put_quote(sym, _merge_dicts(_get_quote(sym), fresh))
                    rehydrated_rows += 1

            await asyncio.gather(*[_rehydrate(sym) for sym in sparse_symbols], return_exceptions=True)

        rows_out: List[Dict[str, Any]] = []
        errors = 0
        sparse_after = 0

        for sym in symbols:
            raw = _get_quote(sym) or {"symbol": sym, "ticker": sym, "error": "missing_row"}
            if raw.get("error"):
                errors += 1
            if _is_sparse_payload_row(raw):
                sparse_after += 1
            rows_out.append(svc.normalize_row(keys, raw, symbol_fallback=sym))

        return rows_out, errors, {
            "batch_rows": batch_rows,
            "rehydrated_rows": rehydrated_rows,
            "sparse_after_rehydrate": sparse_after,
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
        svc.auth_guard(request, token_q, x_app_token, authorization)

        symbol = _strip(body.get("symbol") or body.get("ticker") or body.get("requested_symbol"))
        if not symbol:
            syms = _list_from_body(body, "symbols", "tickers")
            symbol = syms[0] if syms else ""
        if not symbol:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing symbol")

        page = svc.normalize_page(page_q or _strip(body.get("page") or body.get("sheet_name") or body.get("sheet") or "Market_Leaders"))
        headers, keys = svc.schema_for_page(page)
        route_family = svc.route_family(page)
        keys = keys or ["symbol", "error"]

        if route_family != "instrument":
            row = svc.normalize_row(
                keys,
                {"symbol": symbol, "ticker": symbol, "error": f"single_quote_not_supported_for_{route_family}_page"},
                symbol_fallback=symbol,
            )
            payload = svc.envelope(
                status="partial",
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                rows=[row],
                include_headers=True,
                include_matrix=False,
                request_id=request_id,
                started_at=start,
                mode=mode_q,
                dispatch="special_guard",
            )
            payload["row"] = row
            payload["quote"] = row
            payload["data"] = row
            return payload

        rows_out, errors, meta = await _build_instrument_rows(page, keys, [symbol], mode_q, request)
        row = rows_out[0] if rows_out else svc.normalize_row(
            keys,
            {"symbol": symbol, "ticker": symbol, "error": "missing_row"},
            symbol_fallback=symbol,
        )

        payload = svc.envelope(
            status="success" if errors == 0 and not row.get("error") else "partial",
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=[row],
            include_headers=True,
            include_matrix=False,
            request_id=request_id,
            started_at=start,
            mode=mode_q,
            dispatch="instrument",
            extra_meta=meta,
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
        svc.auth_guard(request, token_q, x_app_token, authorization)

        prepared = _merged_special_body(body or {})
        page = svc.normalize_page(_page_from_body(prepared) or "Market_Leaders")
        route_family = svc.route_family(page)
        headers, keys = svc.schema_for_page(page)
        keys = keys or ["symbol", "error"]

        include_headers = _bool_from_body(prepared, "include_headers", True)
        include_matrix = _bool_from_body(prepared, "include_matrix", True)
        top_n = max(1, min(5000, _int_from_body(prepared, "top_n", 200)))
        limit = max(0, min(5000, _int_from_body(prepared, "limit", 0)))
        offset = max(0, _int_from_body(prepared, "offset", 0))
        if limit > 0:
            top_n = limit

        symbols = _list_from_body(prepared, "symbols", "tickers", "tickers_list")[:top_n]
        engine = await svc.get_engine(request)

        if route_family != "instrument":
            if _special_schema_only_requested(prepared):
                return svc.envelope(
                    status="success",
                    page=page,
                    route_family=route_family,
                    headers=headers,
                    keys=keys,
                    rows=[],
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=start,
                    mode=mode_q,
                    dispatch=f"{route_family}_schema_only",
                    extra_meta={"schema_only": True},
                )

            if route_family == "dictionary":
                rows_out: List[Dict[str, Any]] = []
                if callable(svc.build_data_dictionary_rows):
                    try:
                        rows_raw = await _call_maybe_async(svc.build_data_dictionary_rows, include_meta_sheet=True)
                    except TypeError:
                        rows_raw = await _call_maybe_async(svc.build_data_dictionary_rows)
                    except Exception:
                        rows_raw = []

                    rows_out = [
                        svc.normalize_row(keys, r if isinstance(r, Mapping) else _model_to_dict(r))
                        for r in (rows_raw if isinstance(rows_raw, list) else [])
                    ]

                if rows_out:
                    if offset > 0:
                        rows_out = rows_out[offset:]
                    if limit > 0:
                        rows_out = rows_out[:limit]

                    return svc.envelope(
                        status="success",
                        page=page,
                        route_family=route_family,
                        headers=headers,
                        keys=keys,
                        rows=rows_out,
                        include_headers=include_headers,
                        include_matrix=include_matrix,
                        request_id=request_id,
                        started_at=start,
                        mode=mode_q,
                        dispatch="data_dictionary",
                    )

            builder = svc.build_insights_rows if route_family == "insights" else svc.build_top10_rows if route_family == "top10" else None
            builder_name = "insights_builder" if route_family == "insights" else "top10_selector" if route_family == "top10" else route_family

            return await svc.build_special_payload(
                builder=builder,
                builder_name=builder_name,
                request=request,
                engine=engine,
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                body=prepared,
                limit=top_n,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=start,
                mode=mode_q,
            )

        if not symbols:
            payload = await svc.call_engine_page_payload_best_effort(
                engine=engine,
                page=page,
                limit=(limit or 2000),
                offset=offset,
                mode=mode_q,
                body=prepared,
            )

            if payload is None:
                return svc.envelope(
                    status="success",
                    page=page,
                    route_family=route_family,
                    headers=headers,
                    keys=keys,
                    rows=[],
                    include_headers=include_headers,
                    include_matrix=include_matrix,
                    request_id=request_id,
                    started_at=start,
                    mode=mode_q,
                    dispatch="schema_only_table_mode",
                )

            raw_rows = svc.extract_rows_like(payload)
            if not raw_rows:
                matrix = svc.extract_matrix_like(payload)
                if matrix:
                    raw_rows = svc.matrix_to_rows(matrix, keys)

            rows_out = [svc.normalize_row(keys, r) for r in raw_rows]
            if offset > 0:
                rows_out = rows_out[offset:]
            if limit > 0:
                rows_out = rows_out[:limit]

            status_out, error_out, meta_in = svc.extract_status_error_meta(payload)
            result = svc.envelope(
                status=status_out,
                page=page,
                route_family=route_family,
                headers=headers,
                keys=keys,
                rows=rows_out,
                include_headers=include_headers,
                include_matrix=include_matrix,
                request_id=request_id,
                started_at=start,
                mode=mode_q,
                dispatch="table_mode",
                error=error_out,
                extra_meta=meta_in,
            )
            if include_matrix and isinstance(payload.get("rows_matrix"), list):
                result["rows_matrix"] = _json_safe(payload.get("rows_matrix"))
            return result

        rows_out, errors, hydrate_meta = await _build_instrument_rows(page, keys, symbols, mode_q, request)
        return svc.envelope(
            status="success" if errors == 0 else ("partial" if errors < len(symbols) else "error"),
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            rows=rows_out,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode_q,
            dispatch="instrument",
            error=f"{errors} errors" if errors else None,
            extra_meta={"requested": len(symbols), "errors": errors, **hydrate_meta},
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
        prepared = dict(body or {})
        page = svc.normalize_page(_page_from_body(prepared) or "Market_Leaders")
        route_family = svc.route_family(page)

        symbol = _strip(prepared.get("symbol") or prepared.get("ticker") or prepared.get("requested_symbol"))
        list_symbols = _list_from_body(prepared, "symbols", "tickers", "tickers_list")
        explicit_page = bool(_page_from_body(prepared))
        force_sheet_rows = explicit_page or bool(list_symbols) or route_family != "instrument"

        if symbol and not force_sheet_rows:
            return await _handle_single_quote(
                request,
                prepared,
                _strip(prepared.get("page")),
                mode_q,
                token_q,
                x_app_token,
                authorization,
                x_request_id,
            )

        return await _handle_sheet_rows(
            request,
            prepared,
            mode_q,
            token_q,
            x_app_token,
            authorization,
            x_request_id,
        )

    @enriched.get("/health", include_in_schema=False)
    @enriched_quote_alias.get("/health", include_in_schema=False)
    @enriched_hyphen.get("/health", include_in_schema=False)
    async def health() -> Dict[str, Any]:
        return await _health_payload()

    @enriched.get("/headers", include_in_schema=False)
    @enriched_quote_alias.get("/headers", include_in_schema=False)
    @enriched_hyphen.get("/headers", include_in_schema=False)
    async def headers(page: str = Query(default="Market_Leaders")) -> Dict[str, Any]:
        page_norm = svc.normalize_page(page)
        hdrs, keys = svc.schema_for_page(page_norm)
        return _json_safe(
            {
                "status": "success" if hdrs else "degraded",
                "page": page_norm,
                "headers": hdrs,
                "keys": keys,
                "display_headers": hdrs,
                "route_family": svc.route_family(page_norm),
                "router_version": ROUTER_VERSION,
                "reason": None if hdrs else reason,
            }
        )

    @enriched.post("/quote")
    @enriched_quote_alias.post("/quote")
    @enriched_hyphen.post("/quote")
    @legacy.post("/quote")
    async def quote_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched.get("/quote", include_in_schema=False)
    @enriched_quote_alias.get("/quote", include_in_schema=False)
    @enriched_hyphen.get("/quote", include_in_schema=False)
    @legacy.get("/quote", include_in_schema=False)
    async def quote_get(
        request: Request,
        symbol: Optional[str] = Query(default=None),
        ticker: Optional[str] = Query(default=None),
        page: str = Query(default="", description="Optional page name for schema selection"),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_quote_body(symbol=symbol, ticker=ticker, page=page)
        return await _handle_single_quote(request, body, page, mode, token, x_app_token, authorization, x_request_id)

    @enriched.post("/quotes", include_in_schema=False)
    @enriched_quote_alias.post("/quotes", include_in_schema=False)
    @enriched_hyphen.post("/quotes", include_in_schema=False)
    @legacy.post("/quotes")
    async def quotes_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched.get("/quotes", include_in_schema=False)
    @enriched_quote_alias.get("/quotes", include_in_schema=False)
    @enriched_hyphen.get("/quotes", include_in_schema=False)
    @legacy.get("/quotes", include_in_schema=False)
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
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_sheet_rows_body(
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
    @enriched_quote_alias.post("/sheet-rows")
    @enriched_hyphen.post("/sheet-rows")
    @legacy.post("/sheet-rows", include_in_schema=False)
    async def sheet_rows_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_sheet_rows(request, body, mode, token, x_app_token, authorization, x_request_id)

    @enriched.get("/sheet-rows")
    @enriched_quote_alias.get("/sheet-rows")
    @enriched_hyphen.get("/sheet-rows")
    @legacy.get("/sheet-rows", include_in_schema=False)
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
        include_matrix: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None),
        offset: Optional[int] = Query(default=None),
        top_n: Optional[int] = Query(default=None),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_sheet_rows_body(
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
    async def hyphen_root_post(
        request: Request,
        body: Dict[str, Any] = Body(default_factory=dict),
        mode: str = Query(default="", description="Optional mode hint"),
        token: Optional[str] = Query(default=None, description="Auth token"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        return await _handle_hyphen_bridge(request, body, mode, token, x_app_token, authorization, x_request_id)

    @root.get("/v1/enriched-quote")
    async def hyphen_root_get(
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
        token: Optional[str] = Query(default=None, description="Auth token"),
        x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        x_request_id: Optional[str] = Header(default=None, alias="X-Request-ID"),
    ) -> Dict[str, Any]:
        body = _collect_sheet_rows_body(
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

        return await _handle_hyphen_bridge(request, body, mode, token, x_app_token, authorization, x_request_id)

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

    core_router: Optional[APIRouter] = None
    reason = "wrapper_ok"

    try:
        mod = importlib.import_module("core.enriched_quote")
        core_router = getattr(mod, "router", None)
        if core_router is None:
            fn = getattr(mod, "get_router", None)
            if callable(fn):
                core_router = fn()
        if not isinstance(core_router, APIRouter):
            core_router = None
            reason = "core.enriched_quote_no_router"
    except Exception as e:
        reason = f"core_import_failed: {type(e).__name__}: {e}"
        core_router = None

    router = _build_router(reason, core_router)
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
