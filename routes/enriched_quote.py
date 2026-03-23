#!/usr/bin/env python3
# routes/enriched_quote.py
"""
================================================================================
TFB Enriched Quote Routes Wrapper — v7.5.0
================================================================================
SCHEMA-AWARE • PAGE-DISPATCH SAFE • GET+POST COMPATIBLE
ROOT /v1/enriched-quote COMPATIBLE • SPECIAL-PAGE SAFE • THREAD-OFFLOADED
JSON-SAFE • TABLE-MODE SAFE • INSTRUMENT HYDRATION SAFE • LATENCY-FIRST
WRAPPER-PAYLOAD SAFE • ROUTE-TOLERANT • CONTRACT-ALIGNED
ROW-OBJECTS + MATRIX • SHARED-PIPELINE ALIGNED

What v7.5.0 improves
--------------------
- ✅ FIX: enriched router is now quote-owned only; it no longer acts as a
        generic sheet-row owner for non-instrument pages.
- ✅ FIX: `/quotes` is limited to instrument quote batches and returns a clear
        owner hint when callers try to use advanced/dictionary pages here.
- ✅ FIX: `/v1/enriched*/sheet-rows` routes are removed so generic sheet rows
        stay with the canonical analysis/advanced routers.
- ✅ FIX: root `/v1/enriched-quote` bridge still supports single and batch
        quote flows, but now fences non-instrument page requests cleanly.
- ✅ FIX: stable output contract remains aligned with advisor families while
        preserving schema_only / headers_only for instrument quote testing.
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
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, status

logger = logging.getLogger("routes.enriched_quote")

ROUTER_VERSION = "7.5.0"
router: Optional[APIRouter] = None

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)

_FIELD_ALIAS_HINTS: Dict[str, List[str]] = {
    "symbol": ["ticker", "code", "instrument", "security", "symbol_code", "requested_symbol"],
    "ticker": ["symbol", "code", "instrument", "security", "requested_symbol"],
    "name": ["company_name", "long_name", "instrument_name", "security_name", "title"],
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

    try:
        rid = _strip(request.headers.get("X-Request-ID"))
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


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))

    hdrs: List[str] = []
    ks: List[str] = []

    for i in range(max_len):
        h = _strip(raw_headers[i]) if i < len(raw_headers) else ""
        k = _strip(raw_keys[i]) if i < len(raw_keys) else ""

        if h and not k:
            k = re.sub(r"[^a-z0-9]+", "_", h.lower()).strip("_")
        elif k and not h:
            h = k.replace("_", " ").title()
        elif not h and not k:
            h = f"Column {i + 1}"
            k = f"column_{i + 1}"

        hdrs.append(h)
        ks.append(k)

    return hdrs, ks


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs = list(headers or [])
    ks = list(keys or [])

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


def _rows_matrix_from_objects(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> List[List[Any]]:
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


def _looks_like_symbol_token(x: Any) -> bool:
    s = _strip(x).upper()
    if not s or " " in s or len(s) > 24:
        return False
    return bool(re.fullmatch(r"[A-Z0-9\.\=\-\^:_/]{1,24}", s))


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
    return sum(1 for key in important if d.get(key) not in (None, "", [], {}))


def _is_sparse_payload_row(d: Optional[Dict[str, Any]], threshold: int = 6) -> bool:
    return _payload_row_richness(d) < threshold


def _merge_dicts(base: Optional[Dict[str, Any]], addon: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base or {})
    if not isinstance(addon, dict):
        return out

    for key, value in addon.items():
        if value is not None and value != "":
            out[key] = value
    return out


def _looks_like_explicit_row_dict(d: Any) -> bool:
    if not isinstance(d, Mapping) or not d:
        return False

    keyset = {str(k) for k in d.keys()}
    if keyset & {"symbol", "ticker", "code", "requested_symbol"}:
        return True
    if {"sheet", "header", "key"}.issubset(keyset):
        return True
    if {"section", "item"}.issubset(keyset):
        return True
    if {"top10_rank", "selection_reason"}.issubset(keyset):
        return True
    if keyset & {"recommendation", "current_price", "overall_score"}:
        return True

    return False


def _rows_from_matrix(rows_matrix: Any, cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not isinstance(rows_matrix, list) or not cols:
        return []
    keys = [_strip(c) for c in cols if _strip(c)]
    if not keys:
        return []

    out: List[Dict[str, Any]] = []
    for row in rows_matrix:
        if not isinstance(row, (list, tuple)):
            continue
        vals = list(row)
        out.append({keys[i]: (vals[i] if i < len(vals) else None) for i in range(len(keys))})
    return out


class _Service:
    def __init__(self, reason: str):
        self.reason = reason
        self.special_builder_timeout_sec = self._env_float("TFB_SPECIAL_BUILDER_TIMEOUT_SEC", 45.0)
        self.engine_call_timeout_sec = self._env_float("TFB_ENGINE_CALL_TIMEOUT_SEC", 25.0)
        self.quote_call_timeout_sec = self._env_float("TFB_QUOTE_CALL_TIMEOUT_SEC", 20.0)
        self.rehydrate_concurrency = max(2, min(12, int(self._env_float("TFB_ROUTE_REHYDRATE_CONCURRENCY", 4))))
        self.rehydrate_enabled = self._env_bool("TFB_ROUTE_ENABLE_REHYDRATE", True)
        self.rehydrate_max_symbols = max(0, min(250, int(self._env_float("TFB_ROUTE_REHYDRATE_MAX_SYMBOLS", 25))))
        self.rehydrate_sparse_threshold = max(2, min(20, int(self._env_float("TFB_ROUTE_SPARSE_THRESHOLD", 6))))

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
    def _env_bool(name: str, default: bool) -> bool:
        raw = os.getenv(name, "").strip().lower()
        if not raw:
            return bool(default)
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
        return bool(default)

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
            "my_investments": "My_Investments",
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

    def _schema_columns_from_any(self, spec: Any) -> List[Any]:
        if spec is None:
            return []

        if isinstance(spec, dict) and len(spec) == 1 and "columns" not in spec and "fields" not in spec:
            first_val = list(spec.values())[0]
            if isinstance(first_val, dict) and ("columns" in first_val or "fields" in first_val):
                spec = first_val

        cols = getattr(spec, "columns", None)
        if isinstance(cols, list) and cols:
            return cols

        fields = getattr(spec, "fields", None)
        if isinstance(fields, list) and fields:
            return fields

        if isinstance(spec, Mapping):
            cols2 = spec.get("columns") or spec.get("fields")
            if isinstance(cols2, list) and cols2:
                return cols2

        try:
            d = getattr(spec, "__dict__", None)
            if isinstance(d, dict):
                cols3 = d.get("columns") or d.get("fields")
                if isinstance(cols3, list) and cols3:
                    return cols3
        except Exception:
            pass

        return []

    def _schema_keys_headers_from_spec(self, spec: Any) -> Tuple[List[str], List[str]]:
        headers: List[str] = []
        keys: List[str] = []

        cols = self._schema_columns_from_any(spec)
        for c in cols:
            if isinstance(c, Mapping):
                h = _strip(c.get("header") or c.get("display_header") or c.get("displayHeader") or c.get("label") or c.get("title"))
                k = _strip(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
            else:
                h = _strip(
                    getattr(
                        c,
                        "header",
                        getattr(c, "display_header", getattr(c, "displayHeader", getattr(c, "label", getattr(c, "title", None)))),
                    )
                )
                k = _strip(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))

            if h or k:
                headers.append(h or k.replace("_", " ").title())
                keys.append(k or re.sub(r"[^a-z0-9]+", "_", h.lower()).strip("_"))

        if not headers and not keys and isinstance(spec, Mapping):
            headers2 = spec.get("headers") or spec.get("display_headers")
            keys2 = spec.get("keys") or spec.get("columns") or spec.get("fields")
            if isinstance(headers2, list):
                headers = [_strip(x) for x in headers2 if _strip(x)]
            if isinstance(keys2, list):
                keys = [_strip(x) for x in keys2 if _strip(x)]

        return _complete_schema_contract(headers, keys)

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

    def page_is_known(self, page: str) -> bool:
        if not _strip(page):
            return False

        if self._has_schema:
            try:
                spec = self.get_sheet_spec(page)
                headers, keys = self._schema_keys_headers_from_spec(spec)
                if headers and keys:
                    return True
            except Exception:
                pass

        dd_headers, dd_keys = self._schema_from_data_dictionary(page)
        return bool(dd_headers and dd_keys)

    def schema_for_page(self, page: str) -> Tuple[List[str], List[str]]:
        headers: List[str] = []
        keys: List[str] = []

        if self._has_schema:
            try:
                spec = self.get_sheet_spec(page)
                headers, keys = self._schema_keys_headers_from_spec(spec)
            except Exception:
                headers, keys = [], []

        if not headers or not keys:
            dd_headers, dd_keys = self._schema_from_data_dictionary(page)
            headers = headers or dd_headers
            keys = keys or dd_keys

        if page == "Data_Dictionary" and (not headers or not keys):
            headers = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
            keys = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]

        if page == "Top_10_Investments":
            headers, keys = _ensure_top10_contract(headers, keys)

        if not headers and keys:
            headers = [k.replace("_", " ").title() for k in keys]
        if not keys and headers:
            keys = [re.sub(r"[^a-z0-9]+", "_", h.lower()).strip("_") for h in headers]

        headers, keys = _complete_schema_contract(headers or ["Symbol", "Error"], keys or ["symbol", "error"])
        return headers, keys

    def _key_variants(self, key: str) -> List[str]:
        base = _strip(key)
        if not base:
            return []

        variants = [
            base,
            base.lower(),
            base.upper(),
            base.replace("_", " "),
            re.sub(r"[^a-z0-9]+", "", base.lower()),
        ]
        for alias in _FIELD_ALIAS_HINTS.get(base, []):
            variants.extend(
                [
                    alias,
                    alias.lower(),
                    alias.upper(),
                    alias.replace("_", " "),
                    re.sub(r"[^a-z0-9]+", "", alias.lower()),
                ]
            )

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

    def normalize_row(self, keys: Sequence[str], raw: Mapping[str, Any], *, symbol_fallback: str = "") -> Dict[str, Any]:
        raw_dict = dict(raw or {})
        row = {k: _json_safe(self._extract_from_raw(raw_dict, self._key_variants(k))) for k in keys}

        if symbol_fallback:
            if "symbol" in row and not row.get("symbol"):
                row["symbol"] = symbol_fallback
            if "ticker" in row and not row.get("ticker"):
                row["ticker"] = symbol_fallback

        return row

    def extract_rows_like(self, payload: Any, depth: int = 0) -> List[Dict[str, Any]]:
        if payload is None or depth > 8:
            return []

        if isinstance(payload, list):
            if not payload:
                return []
            if all(isinstance(x, Mapping) for x in payload):
                return [dict(x) for x in payload if isinstance(x, Mapping)]
            model_rows = [_model_to_dict(x) for x in payload]
            model_rows = [r for r in model_rows if isinstance(r, dict) and r]
            if model_rows and all(_looks_like_explicit_row_dict(r) or _strip(r.get("symbol") or r.get("ticker")) for r in model_rows):
                return model_rows
            return []

        if not isinstance(payload, Mapping):
            d = _model_to_dict(payload)
            return [d] if _looks_like_explicit_row_dict(d) else []

        if _looks_like_explicit_row_dict(payload):
            return [dict(payload)]

        maybe_symbol_map = True
        rows_from_symbol_map: List[Dict[str, Any]] = []
        symbol_like_keys = 0
        for k, v in payload.items():
            if not isinstance(v, Mapping):
                maybe_symbol_map = False
                break
            if not _looks_like_symbol_token(k):
                maybe_symbol_map = False
                break
            symbol_like_keys += 1
            row = dict(v)
            if not row.get("symbol") and not row.get("ticker"):
                row["symbol"] = _strip(k)
                row["ticker"] = _strip(k)
            rows_from_symbol_map.append(row)
        if maybe_symbol_map and symbol_like_keys > 0 and rows_from_symbol_map:
            return rows_from_symbol_map

        keys_like = self.extract_keys_like(payload)
        if not keys_like:
            hdrs = self.extract_headers_like(payload)
            if hdrs:
                keys_like = [re.sub(r"[^a-z0-9]+", "_", h.lower()).strip("_") for h in hdrs if _strip(h)]

        for key in ("row_objects", "records", "items", "data", "quotes", "rows", "results", "recommendations"):
            value = payload.get(key)

            if isinstance(value, list):
                rows = self.extract_rows_like(value, depth + 1)
                if rows:
                    return rows
                if value and any(isinstance(x, (list, tuple)) for x in value) and keys_like:
                    rows = _rows_from_matrix(value, keys_like)
                    if rows:
                        return rows

            if isinstance(value, Mapping):
                rows = self.extract_rows_like(value, depth + 1)
                if rows:
                    return rows

        rows_matrix = payload.get("rows_matrix") or payload.get("matrix") or payload.get("values")
        if isinstance(rows_matrix, list) and keys_like:
            rows = _rows_from_matrix(rows_matrix, keys_like)
            if rows:
                return rows

        for key in ("payload", "result", "response", "output", "data"):
            value = payload.get(key)
            if value is not None and value is not payload:
                rows = self.extract_rows_like(value, depth + 1)
                if rows:
                    return rows

        return []

    def extract_matrix_like(self, payload: Any, depth: int = 0) -> Optional[List[List[Any]]]:
        if depth > 8:
            return None

        if isinstance(payload, dict):
            for name in ("rows_matrix", "matrix", "values"):
                value = payload.get(name)
                if isinstance(value, list):
                    return [list(r) if isinstance(r, (list, tuple)) else [r] for r in value]

            rows = payload.get("rows")
            if isinstance(rows, list) and rows and isinstance(rows[0], (list, tuple)):
                return [list(r) if isinstance(r, (list, tuple)) else [r] for r in rows]

            for name in ("data", "payload", "result", "response", "output"):
                nested = payload.get(name)
                if isinstance(nested, dict):
                    found = self.extract_matrix_like(nested, depth + 1)
                    if found is not None:
                        return found

        return None

    def extract_headers_like(self, payload: Any, depth: int = 0) -> List[str]:
        if depth > 8 or not isinstance(payload, dict):
            return []

        for name in ("display_headers", "sheet_headers", "column_headers", "headers"):
            value = payload.get(name)
            if isinstance(value, list):
                out = [_strip(x) for x in value if _strip(x)]
                if out:
                    return out

        columns = payload.get("columns")
        if isinstance(columns, list):
            out = [_strip(x) for x in columns if _strip(x)]
            if out and any((" " in x) or x.istitle() for x in out):
                return out

        for name in ("payload", "result", "response", "output", "data"):
            nested = payload.get(name)
            if isinstance(nested, dict):
                found = self.extract_headers_like(nested, depth + 1)
                if found:
                    return found

        return []

    def extract_keys_like(self, payload: Any, depth: int = 0) -> List[str]:
        if depth > 8 or not isinstance(payload, dict):
            return []

        for name in ("keys", "fields", "column_keys", "schema_keys", "columns"):
            value = payload.get(name)
            if isinstance(value, list):
                out = [_strip(x) for x in value if _strip(x)]
                if out:
                    return out

        for name in ("payload", "result", "response", "output", "data"):
            nested = payload.get(name)
            if isinstance(nested, dict):
                found = self.extract_keys_like(nested, depth + 1)
                if found:
                    return found

        return []

    def extract_status_error_meta(self, payload: Any) -> Tuple[str, Optional[str], Dict[str, Any]]:
        if not isinstance(payload, dict):
            return "success", None, {}

        status_out = _strip(payload.get("status")) or "success"
        error_out = payload.get("error")
        if error_out in (None, ""):
            error_out = payload.get("detail") or payload.get("message")
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

    def payload_has_structural_value(self, payload: Any) -> bool:
        return bool(self.payload_has_rows(payload) or self.extract_headers_like(payload) or self.extract_keys_like(payload))

    def _top10_coverage_score(self, payload: Any) -> int:
        rows = self.extract_rows_like(payload)
        if not rows:
            return 0
        score = 0
        for field in TOP10_REQUIRED_FIELDS:
            if any(isinstance(r, Mapping) and r.get(field) not in (None, "", [], {}) for r in rows):
                score += 1
        return score

    def payload_quality_score(self, payload: Any, page: str = "") -> int:
        if payload is None:
            return -10
        if isinstance(payload, list):
            return 100 if payload else 0
        if not isinstance(payload, dict):
            return 0

        score = 0
        rows_like = self.extract_rows_like(payload)
        matrix_like = self.extract_matrix_like(payload)
        headers_like = self.extract_headers_like(payload)
        keys_like = self.extract_keys_like(payload)

        if rows_like:
            score += 100 + min(25, len(rows_like))
        if matrix_like:
            score += 85 + min(15, len(matrix_like))
        if headers_like:
            score += 8
        if keys_like:
            score += 8

        if self.normalize_page(page) == "Top_10_Investments":
            score += 10 * self._top10_coverage_score(payload)

        status_out, error_out, meta_in = self.extract_status_error_meta(payload)

        if _strip(status_out).lower() == "success":
            score += 4
        elif _strip(status_out).lower() == "partial":
            score += 2
        elif _strip(status_out).lower() == "warn":
            score += 1
        elif _strip(status_out).lower() == "error":
            score -= 3

        if isinstance(meta_in, dict) and meta_in.get("known_sheets"):
            score += 1

        if _strip(error_out):
            score -= 6

        return score

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
        matrix = _rows_matrix_from_objects(rows_out, ks)

        return _json_safe(
            {
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
                "rows": matrix if include_matrix else [],
                "rows_matrix": matrix if include_matrix else [],
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
                    "duration_ms": (time.time() - started_at) * 1000.0,
                    "count": len(rows_out),
                    "row_object_count": len(rows_out),
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
        page_out = page
        headers_out = list(headers)
        keys_out = list(keys)
        row_objects: List[Dict[str, Any]] = []
        status_out = "success"
        error_out: Optional[str] = None
        meta_in: Dict[str, Any] = {}

        if isinstance(result, dict):
            result_headers = self.extract_headers_like(result)
            result_keys = self.extract_keys_like(result)

            headers_out = [str(x) for x in (result_headers or list(headers)) if _strip(x)] or list(headers)
            keys_out = [str(x) for x in (result_keys or list(keys)) if _strip(x)] or list(keys)

            if page == "Top_10_Investments":
                headers_out, keys_out = _ensure_top10_contract(headers_out, keys_out)
            else:
                headers_out, keys_out = _complete_schema_contract(headers_out, keys_out)

            raw_rows = self.extract_rows_like(result)
            if not raw_rows:
                matrix = self.extract_matrix_like(result)
                if matrix:
                    raw_rows = self.matrix_to_rows(matrix, keys_out)

            row_objects = [self.normalize_row(keys_out, r) for r in raw_rows]
            page_out = _strip(result.get("page") or result.get("sheet") or result.get("sheet_name")) or page
            status_out, error_out, meta_in = self.extract_status_error_meta(result)

        elif isinstance(result, list):
            row_objects = [self.normalize_row(keys_out, r if isinstance(r, Mapping) else _model_to_dict(r)) for r in result]
        else:
            d = result if isinstance(result, Mapping) else _model_to_dict(result)
            if d:
                row_objects = [self.normalize_row(keys_out, d)]

        return self.envelope(
            status=status_out,
            page=page_out,
            route_family=route_family,
            headers=headers_out,
            keys=keys_out,
            row_objects=row_objects,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch=dispatch,
            error=error_out,
            extra_meta={**meta_in, **(extra_meta or {})},
        )

    async def call_shared_page_pipeline(
        self,
        *,
        page: str,
        payload: Dict[str, Any],
        request: Request,
        engine: Any,
        prefer_engine_sheet: bool,
    ) -> Optional[Dict[str, Any]]:
        try:
            mod = importlib.import_module("routes.investment_advisor")
            fn = getattr(mod, "_run_page_pipeline", None)
            if not callable(fn):
                return None

            out = await _call_maybe_async(
                fn,
                page=page,
                payload=payload,
                request=request,
                engine=engine,
                timeout_sec=max(5.0, min(180.0, self.engine_call_timeout_sec)),
                prefer_engine_sheet=prefer_engine_sheet,
            )
            return out if isinstance(out, dict) else None
        except Exception:
            return None

    def _coerce_engine_result_payload(self, result: Any, page: str) -> Dict[str, Any]:
        if isinstance(result, dict):
            return _json_safe(result)

        if isinstance(result, list):
            if result and isinstance(result[0], (list, tuple)):
                return {"status": "success", "page": page, "rows_matrix": _json_safe(result)}
            return {"status": "success", "page": page, "row_objects": _json_safe(result)}

        if result is None:
            return {"status": "partial", "page": page, "row_objects": []}

        d = _model_to_dict(result)
        if d:
            return {"status": "success", "page": page, "row_objects": [d]}

        return {"status": "success", "page": page, "row_objects": []}

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
        seen_ids = set()

        if engine is not None:
            for name in (
                "get_sheet_rows",
                "get_page_rows",
                "sheet_rows",
                "build_sheet_rows",
                "execute_sheet_rows",
                "run_sheet_rows",
                "get_rows_for_sheet",
                "get_rows_for_page",
                "get_sheet",
                "build_sheet",
                "get_cached_sheet_rows",
                "get_sheet_snapshot",
                "get_cached_sheet_snapshot",
                "get_page_snapshot",
            ):
                fn = getattr(engine, name, None)
                if callable(fn) and id(fn) not in seen_ids:
                    seen_ids.add(id(fn))
                    candidates.append(fn)

        if callable(self.core_get_sheet_rows) and id(self.core_get_sheet_rows) not in seen_ids:
            seen_ids.add(id(self.core_get_sheet_rows))
            candidates.append(self.core_get_sheet_rows)

        payload = dict(body or {})
        payload["page"] = page
        payload["sheet"] = page
        payload["sheet_name"] = page
        payload.setdefault("limit", limit)
        payload.setdefault("offset", offset)
        if mode not in (None, ""):
            payload.setdefault("mode", mode)
        payload.setdefault("include_headers", True)
        payload.setdefault("include_matrix", True)

        attempts: List[Tuple[str, Tuple[Any, ...], Dict[str, Any]]] = [
            ("payload_only", (payload,), {}),
            (
                "page_kwargs_full",
                (),
                {
                    "page": page,
                    "sheet": page,
                    "sheet_name": page,
                    "limit": limit,
                    "offset": offset,
                    "mode": mode,
                    "body": payload,
                },
            ),
            (
                "page_kwargs_simple",
                (),
                {
                    "page": page,
                    "limit": limit,
                    "offset": offset,
                    "mode": mode,
                },
            ),
            (
                "sheet_kwargs_simple",
                (),
                {
                    "sheet": page,
                    "limit": limit,
                    "offset": offset,
                    "mode": mode,
                },
            ),
            ("page_only", (page,), {"limit": limit, "offset": offset}),
        ]

        best_payload: Optional[Dict[str, Any]] = None
        best_score = -9999

        for fn in candidates:
            fn_name = getattr(fn, "__name__", str(fn))

            for _label, args, kwargs in attempts:
                try:
                    if self.engine_call_timeout_sec > 0:
                        res = await asyncio.wait_for(
                            _call_maybe_async(fn, *args, **kwargs),
                            timeout=self.engine_call_timeout_sec,
                        )
                    else:
                        res = await _call_maybe_async(fn, *args, **kwargs)
                except TypeError:
                    continue
                except asyncio.TimeoutError:
                    logger.warning("Engine table-mode candidate timed out. fn=%s page=%s", fn_name, page)
                    break
                except Exception:
                    continue

                payload_out = self._coerce_engine_result_payload(res, page)
                score = self.payload_quality_score(payload_out, page=page)

                if score > best_score:
                    best_score = score
                    best_payload = payload_out

                if self.payload_has_rows(payload_out):
                    return payload_out

                if score >= 95 and self.payload_has_structural_value(payload_out):
                    return payload_out

        return best_payload

    async def build_data_dictionary_payload(
        self,
        *,
        page: str,
        route_family: str,
        headers: Sequence[str],
        keys: Sequence[str],
        include_headers: bool,
        include_matrix: bool,
        request_id: str,
        started_at: float,
        mode: str,
    ) -> Dict[str, Any]:
        row_objects: List[Dict[str, Any]] = []

        if callable(self.build_data_dictionary_rows):
            try:
                try:
                    rows_raw = await _call_maybe_async(self.build_data_dictionary_rows, include_meta_sheet=True)
                except TypeError:
                    rows_raw = await _call_maybe_async(self.build_data_dictionary_rows)
            except Exception:
                rows_raw = []

            for item in rows_raw if isinstance(rows_raw, list) else []:
                row_objects.append(self.normalize_row(keys, item if isinstance(item, Mapping) else _model_to_dict(item)))

        return self.envelope(
            status="success" if row_objects else "partial",
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            row_objects=row_objects,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=started_at,
            mode=mode,
            dispatch="data_dictionary",
            error=None if row_objects else "data_dictionary_rows_unavailable",
        )


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
        schema_only: Optional[str] = None,
        headers_only: Optional[str] = None,
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
            "schema_only": schema_only,
            "headers_only": headers_only,
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


    def _canonical_owner_hint(page: str, route_family: str) -> Dict[str, str]:
        if route_family == "instrument":
            return {
                "canonical_owner": "routes.analysis_sheet_rows",
                "canonical_endpoint": "/v1/analysis/sheet-rows",
                "canonical_reason": "instrument_table_mode_owned_by_analysis_sheet_rows",
            }
        if route_family in {"insights", "top10"}:
            return {
                "canonical_owner": "routes.investment_advisor",
                "canonical_endpoint": "/v1/advanced/sheet-rows",
                "canonical_reason": "derived_output_page_owned_by_investment_advisor",
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
        for name in ("get_enriched_quotes_batch", "get_analysis_quotes_batch", "get_quotes_batch", "quotes_batch"):
            candidate = getattr(engine, name, None)
            if callable(candidate):
                batch_fn = candidate
                break

        if callable(batch_fn):
            signatures = [
                ((), {"symbols": list(symbols), "mode": mode, "schema": page}),
                ((), {"symbols": list(symbols), "mode": mode}),
                ((), {"symbols": list(symbols)}),
                ((list(symbols),), {"mode": mode, "schema": page}),
                ((list(symbols),), {"mode": mode}),
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
        sparse_symbols = [
            s for s in symbols
            if not _get_quote(s) or _is_sparse_payload_row(_get_quote(s), threshold=svc.rehydrate_sparse_threshold)
        ]

        per_fn = None
        for name in ("get_enriched_quote_dict", "get_quote_dict", "get_enriched_quote", "get_quote"):
            candidate = getattr(engine, name, None)
            if callable(candidate):
                per_fn = candidate
                break

        if sparse_symbols and callable(per_fn) and svc.rehydrate_enabled and len(sparse_symbols) <= svc.rehydrate_max_symbols:
            sem = asyncio.Semaphore(svc.rehydrate_concurrency)

            async def _rehydrate(sym: str) -> None:
                nonlocal rehydrated_rows
                async with sem:
                    signatures = [((sym,), {"schema": page}), ((sym,), {}), ((), {"symbol": sym, "schema": page}), ((), {"symbol": sym})]
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
            if _is_sparse_payload_row(raw, threshold=svc.rehydrate_sparse_threshold):
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
                row_objects=[row],
                include_headers=True,
                include_matrix=True,
                request_id=request_id,
                started_at=start,
                mode=mode_q,
                dispatch="special_guard",
            )
            payload["row"] = row
            payload["quote"] = row
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
            row_objects=[row],
            include_headers=True,
            include_matrix=True,
            request_id=request_id,
            started_at=start,
            mode=mode_q,
            dispatch="instrument",
            extra_meta=meta,
        )
        payload["row"] = row
        payload["quote"] = row
        return payload

    async def _handle_quotes_batch(
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

        prepared = dict(body or {})
        page = svc.normalize_page(_page_from_body(prepared) or "Market_Leaders")
        route_family = svc.route_family(page)
        headers, keys = svc.schema_for_page(page)
        keys = keys or ["symbol", "error"]

        include_headers = _bool_from_body(prepared, "include_headers", True)
        include_matrix = _bool_from_body(prepared, "include_matrix", True)
        schema_only = _bool_from_body(prepared, "schema_only", False)
        headers_only = _bool_from_body(prepared, "headers_only", False)

        top_n = max(1, min(5000, _int_from_body(prepared, "top_n", 200)))
        limit = max(0, min(5000, _int_from_body(prepared, "limit", 0)))
        if limit > 0:
            top_n = limit

        symbols = _list_from_body(prepared, "symbols", "tickers", "tickers_list")[:top_n]

        if route_family != "instrument":
            hint = _canonical_owner_hint(page, route_family)
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
                started_at=start,
                mode=mode_q,
                dispatch="owner_guard",
                error="enriched_quote_batch_not_supported_for_non_instrument_page",
                extra_meta={
                    "page_known": svc.page_is_known(page),
                    "requested_symbols": len(symbols),
                    **hint,
                },
            )

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
                started_at=start,
                mode=mode_q,
                dispatch="instrument_schema_only",
                extra_meta={"schema_only": bool(schema_only), "headers_only": bool(headers_only)},
            )

        if not symbols:
            hint = _canonical_owner_hint(page, route_family)
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
                started_at=start,
                mode=mode_q,
                dispatch="instrument_guard",
                error="symbols_or_tickers_required_for_enriched_quotes",
                extra_meta={
                    "page_known": svc.page_is_known(page),
                    **hint,
                },
            )

        rows_out, errors, hydrate_meta = await _build_instrument_rows(page, keys, symbols, mode_q, request)
        return svc.envelope(
            status="success" if errors == 0 else ("partial" if errors < len(symbols) else "error"),
            page=page,
            route_family=route_family,
            headers=headers,
            keys=keys,
            row_objects=rows_out,
            include_headers=include_headers,
            include_matrix=include_matrix,
            request_id=request_id,
            started_at=start,
            mode=mode_q,
            dispatch="instrument_batch",
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

        symbol = _strip(prepared.get("symbol") or prepared.get("ticker") or prepared.get("requested_symbol"))
        list_symbols = _list_from_body(prepared, "symbols", "tickers", "tickers_list")
        explicit_page = bool(_page_from_body(prepared))

        if symbol and not explicit_page and not list_symbols:
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

        return await _handle_quotes_batch(
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
        return await _handle_quotes_batch(request, body, mode, token, x_app_token, authorization, x_request_id)

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
        schema_only: Optional[str] = Query(default=None),
        headers_only: Optional[str] = Query(default=None),
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
            schema_only=schema_only,
            headers_only=headers_only,
        )
        return await _handle_quotes_batch(request, body, mode, token, x_app_token, authorization, x_request_id)


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
        schema_only: Optional[str] = Query(default=None),
        headers_only: Optional[str] = Query(default=None),
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
            schema_only=schema_only,
            headers_only=headers_only,
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
