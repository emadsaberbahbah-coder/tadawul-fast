#!/usr/bin/env python3
"""
tests/test_schema_alignment.py
--------------------------------------------------------------------------------
PHASE 9 — Schema Alignment & Route Contract Regression Suite (v9.1.0)

What this revision improves
1) Supports more import layouts for schema registry and Data_Dictionary modules
2) Builds a local app through main.create_app() when available, with auth opened safely
3) Uses a schema-aware stub engine with stable contract helpers for local route tests
4) Tests canonical schema endpoints with route fallbacks (/v1/schema/* and /schema/*)
5) Verifies mounted */sheet-rows endpoints without external network calls
6) Makes Data_Dictionary parsing more tolerant to implementation differences
7) Keeps Top_10_Investments contract protection in place

Design goals
- No external network calls
- Local FastAPI app only
- Compatible with the current controlled-route ownership model
- Deterministic auth bypass for local tests only
- Schema registry remains the authoritative source of truth
"""

from __future__ import annotations

import importlib
import inspect
import os
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import pytest


# =============================================================================
# Required sheets (single canonical list)
# =============================================================================
REQUIRED_SHEETS: List[str] = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
]

TOP10_REQUIRED_KEYS = {"top10_rank", "selection_reason", "criteria_snapshot"}
TOP10_RECOMMENDATION_KEYS = {"recommendation", "recommendation_reason"}
DATA_DICTIONARY_EXPECTED_COLS = 9


# =============================================================================
# Helpers — robust imports
# =============================================================================
def _import_any(*candidates: str):
    last = None
    for name in candidates:
        try:
            return importlib.import_module(name)
        except Exception as e:  # pragma: no cover - diagnostic only
            last = e
    raise RuntimeError(f"Unable to import any of: {candidates}. Last error: {last!r}")


def _first_callable(obj: Any, names: Sequence[str]) -> Optional[Any]:
    for n in names:
        fn = getattr(obj, n, None)
        if callable(fn):
            return fn
    return None


def _obj_to_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if is_dataclass(x):
        try:
            return asdict(x)
        except Exception:
            pass
    md = getattr(x, "model_dump", None)
    if callable(md):
        try:
            return md(mode="python")
        except Exception:
            try:
                return md()
            except Exception:
                pass
    d = getattr(x, "dict", None)
    if callable(d):
        try:
            return d()
        except Exception:
            pass
    raw = getattr(x, "__dict__", None)
    return dict(raw) if isinstance(raw, dict) else {}


def _norm(s: str) -> str:
    return " ".join((str(s or "").strip().lower()).split())


def _snake_like(s: str) -> str:
    txt = str(s or "").strip().replace("%", " pct").replace("/", " ")
    out: List[str] = []
    prev_us = False
    for ch in txt:
        if ch.isalnum():
            out.append(ch.lower())
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    res = "".join(out).strip("_")
    while "__" in res:
        res = res.replace("__", "_")
    return res


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for v in values:
        s = str(v).strip() if v is not None else ""
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# =============================================================================
# Schema registry access (authoritative)
# =============================================================================
def _load_schema_module():
    return _import_any(
        "core.sheets.schema_registry",
        "sheets.schema_registry",
        "schema_registry",
        "core.schema_registry",
    )


def _schema_sheet_headers(sr: Any, sheet: str) -> List[str]:
    fn = _first_callable(sr, ["get_sheet_headers", "sheet_headers_for", "headers_for_sheet"])
    if fn:
        try:
            return list(fn(sheet))
        except Exception:
            pass

    spec_fn = _first_callable(sr, ["get_sheet_spec", "sheet_spec_for", "get_spec"])
    spec = spec_fn(sheet) if callable(spec_fn) else None
    cols = getattr(spec, "columns", None) or _obj_to_dict(spec).get("columns") or []
    out: List[str] = []
    for c in cols:
        d = _obj_to_dict(c)
        h = d.get("header") or getattr(c, "header", None)
        k = d.get("key") or getattr(c, "key", None)
        out.append(str(h or k or "").strip())
    return [x for x in out if x]


def _schema_sheet_keys(sr: Any, sheet: str) -> List[str]:
    fn = _first_callable(sr, ["get_sheet_keys", "sheet_keys_for", "keys_for_sheet"])
    if fn:
        try:
            return list(fn(sheet))
        except Exception:
            pass

    spec_fn = _first_callable(sr, ["get_sheet_spec", "sheet_spec_for", "get_spec"])
    spec = spec_fn(sheet) if callable(spec_fn) else None
    cols = getattr(spec, "columns", None) or _obj_to_dict(spec).get("columns") or []
    out: List[str] = []
    for c in cols:
        d = _obj_to_dict(c)
        k = d.get("key") or getattr(c, "key", None)
        h = d.get("header") or getattr(c, "header", None)
        out.append(str(k or _snake_like(h) or "").strip())
    return [x for x in out if x]


def _schema_registry_mapping(sr: Any) -> Dict[str, Any]:
    for attr in ("SCHEMA_REGISTRY", "REGISTRY", "SHEET_REGISTRY", "SCHEMA_BY_SHEET", "SHEET_SCHEMAS"):
        reg = getattr(sr, attr, None)
        if isinstance(reg, dict):
            return dict(reg)

    fn = _first_callable(sr, ["get_schema_registry", "schema_registry", "get_registry", "registry"])
    if fn:
        reg = fn()
        if isinstance(reg, dict):
            return dict(reg)

    raise AssertionError("Could not locate schema registry mapping in schema_registry module")


# =============================================================================
# Data_Dictionary generator (must be schema-driven)
# =============================================================================
def _load_data_dictionary_rows_dicts() -> List[Dict[str, Any]]:
    mod = _import_any(
        "core.sheets.data_dictionary",
        "sheets.data_dictionary",
        "core.data_dictionary",
        "data_dictionary",
    )

    fn = _first_callable(
        mod,
        [
            "build_data_dictionary_rows",
            "generate_data_dictionary_rows",
            "get_data_dictionary_rows",
            "generate_rows",
        ],
    )
    if fn:
        res = fn()
        if inspect.isawaitable(res):
            raise AssertionError("Data_Dictionary generator returned awaitable; keep it sync for tests.")
        if isinstance(res, list) and (not res or isinstance(res[0], dict)):
            return [r for r in res if isinstance(r, dict)]
        if isinstance(res, dict):
            rows = res.get("rows") or res.get("data") or res.get("items") or []
            if isinstance(rows, list) and (not rows or isinstance(rows[0], dict)):
                return [r for r in rows if isinstance(r, dict)]

    fn2 = _first_callable(
        mod,
        [
            "build_data_dictionary_values",
            "generate_data_dictionary_values",
            "build_data_dictionary",
            "generate_data_dictionary",
        ],
    )
    if not fn2:
        raise AssertionError("No recognized Data_Dictionary generator function found.")

    res2 = fn2()
    if inspect.isawaitable(res2):
        raise AssertionError("Data_Dictionary values generator returned awaitable; keep it sync for tests.")
    if not isinstance(res2, list) or (res2 and not isinstance(res2[0], (list, tuple))):
        raise AssertionError(f"Unrecognized Data_Dictionary values generator result type: {type(res2)}")

    values = res2
    if not values:
        return []

    headers = [str(x) for x in values[0]]
    rows = values[1:]
    hmap = {_norm(h): i for i, h in enumerate(headers)}

    def _first_idx(*names: str) -> Optional[int]:
        for n in names:
            if n in hmap:
                return hmap[n]
        return None

    sheet_idx = _first_idx("sheet", "sheet name", "tab", "page")
    key_idx = _first_idx("key", "column key", "field", "schema key")
    if sheet_idx is None or key_idx is None:
        raise AssertionError(f"Cannot parse Data_Dictionary values: headers={headers[:12]}")

    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, (list, tuple)):
            continue
        sheet = r[sheet_idx] if sheet_idx < len(r) else None
        key = r[key_idx] if key_idx < len(r) else None
        if not sheet or not key:
            continue
        out.append({"sheet": str(sheet), "key": str(key)})
    return out


def _dd_map_sheet_to_keys(dd_rows: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for r in dd_rows:
        sheet = r.get("sheet") or r.get("Sheet") or r.get("page") or r.get("Page") or r.get("tab") or r.get("Tab")
        key = r.get("key") or r.get("Key") or r.get("field") or r.get("Field") or r.get("column_key") or r.get("Column Key")
        if not sheet or not key:
            continue
        out.setdefault(str(sheet), set()).add(str(key))
    return out


# =============================================================================
# Local auth patching for deterministic tests
# =============================================================================
class _DummySettings:
    allow_query_token = True
    open_mode = True
    require_auth = False
    auth_header_name = "X-APP-TOKEN"
    service_name = "TFB Test"
    app_name = "TFB Test"
    app_version = "test"
    environment = "test"
    env = "test"
    timezone = "Asia/Riyadh"
    backend_base_url = ""
    engine_cache_ttl_sec = 1
    APP_NAME = "TFB Test"
    APP_VERSION = "test"
    APP_ENV = "test"
    ENABLE_SWAGGER = True
    ENABLE_REDOC = False
    PRESTART_MOUNT_ROUTES = True
    INIT_ENGINE_ON_BOOT = False
    INIT_ENGINE_STRICT = False
    REQUIRE_AUTH = False
    OPEN_MODE = True
    AUTH_HEADER_NAME = "X-APP-TOKEN"
    ENABLE_CORS_ALL_ORIGINS = True
    CORS_ORIGINS = ""
    BACKEND_BASE_URL = ""
    ENGINE_CACHE_TTL_SEC = 1
    MAX_REQUESTS_PER_MINUTE = 1000


def _patch_auth_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPEN_MODE", "1")
    monkeypatch.setenv("TFB_OPEN_MODE", "1")
    monkeypatch.setenv("REQUIRE_AUTH", "0")
    monkeypatch.setenv("TFB_REQUIRE_AUTH", "0")
    monkeypatch.setenv("ALLOW_QUERY_TOKEN", "1")
    monkeypatch.setenv("TFB_ALLOW_QUERY_TOKEN", "1")
    monkeypatch.setenv("APP_TOKEN", "test-token")
    monkeypatch.setenv("BACKEND_TOKEN", "test-token")
    monkeypatch.setenv("X_APP_TOKEN", "test-token")
    monkeypatch.setenv("TFB_APP_TOKEN", "test-token")
    monkeypatch.setenv("ENABLE_SWAGGER", "1")
    monkeypatch.setenv("ENABLE_REDOC", "0")
    monkeypatch.setenv("INIT_ENGINE_ON_BOOT", "0")
    monkeypatch.setenv("PRESTART_MOUNT_ROUTES", "1")
    monkeypatch.setenv("ROUTES_STRICT_IMPORT", "0")

    for mod_name in ("config", "core.config"):
        try:
            cfg = importlib.import_module(mod_name)
            monkeypatch.setattr(cfg, "is_open_mode", lambda: True, raising=False)
            monkeypatch.setattr(cfg, "auth_ok", lambda *args, **kwargs: True, raising=False)
            monkeypatch.setattr(cfg, "get_settings_cached", lambda: _DummySettings(), raising=False)
            monkeypatch.setattr(cfg, "get_settings", lambda: _DummySettings(), raising=False)
            monkeypatch.setattr(cfg, "reload_settings", lambda: _DummySettings(), raising=False)
        except Exception:
            pass


def _auth_headers() -> Dict[str, str]:
    return {
        "X-APP-TOKEN": "test-token",
        "X-API-Key": "test-token",
        "Authorization": "Bearer test-token",
        "X-Request-ID": "pytest-schema-alignment",
    }


# =============================================================================
# FastAPI sheet-rows contract tests (local app + stub engine)
# =============================================================================
class _StubEngine:
    """Minimal schema-aware engine for local route tests."""

    def __init__(self, sr: Any):
        self._sr = sr

    def _payload(self, sheet: str, *, limit: int = 2000, offset: int = 0, mode: str = "") -> Dict[str, Any]:
        headers = _schema_sheet_headers(self._sr, sheet)
        keys = _schema_sheet_keys(self._sr, sheet)
        return {
            "status": "success",
            "sheet": sheet,
            "page": sheet,
            "headers": headers,
            "keys": keys,
            "rows": [],
            "row_objects": [],
            "rows_matrix": [],
            "data": [],
            "items": [],
            "quotes": [],
            "meta": {"stub": True, "limit": limit, "offset": offset, "mode": mode},
        }

    async def get_sheet_rows(self, *, sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None):
        return self._payload(sheet, limit=limit, offset=offset, mode=mode)

    async def get_sheet_rows_sync(self, *, sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None):
        return self._payload(sheet, limit=limit, offset=offset, mode=mode)

    async def build_sheet_rows(self, *, sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None):
        return self._payload(sheet, limit=limit, offset=offset, mode=mode)

    async def get_enriched_quotes_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None):
        return {s: {"symbol": s} for s in symbols or []}

    async def get_quotes_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None):
        return {s: {"symbol": s} for s in symbols or []}

    async def get_enriched_quote_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None):
        return {"symbol": symbol}

    async def health(self):
        return {"status": "ok", "stub": True}


def _build_test_app(monkeypatch: pytest.MonkeyPatch, sr: Any) -> Any:
    _patch_auth_open(monkeypatch)

    try:
        main_mod = _import_any("main")
        create_app = getattr(main_mod, "create_app", None)
        if callable(create_app):
            app = create_app()
            app.state.engine = _StubEngine(sr)
            app.state.engine_ready = True
            return app
    except Exception:
        pass

    from fastapi import FastAPI

    app = FastAPI(title="TFB Test App")
    routes_pkg = _import_any("routes")
    mount_fn = _first_callable(routes_pkg, ["mount_all_routers", "mount_routers", "mount_all", "mount_routes", "mount_all_routes"])
    if not callable(mount_fn):
        raise AssertionError("No recognized routes mount function found in routes package.")

    try:
        mount_fn(app, strict=False)
    except TypeError:
        mount_fn(app)

    app.state.engine = _StubEngine(sr)
    app.state.engine_ready = True
    return app


def _find_sheet_rows_endpoints(app: Any) -> List[Tuple[str, str]]:
    found: Set[Tuple[str, str]] = set()
    for r in getattr(app.router, "routes", []) or []:
        p = str(getattr(r, "path", "") or "")
        methods = {str(m).upper() for m in (getattr(r, "methods", None) or set())}
        if p.endswith("/sheet-rows") or p == "/sheet-rows":
            for m in methods:
                if m in {"GET", "POST"}:
                    found.add((m, p))
    if not found:
        raise AssertionError("No GET/POST */sheet-rows endpoint found in mounted routes.")
    return sorted(found)


def _extract_contract_payload(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}

    contract_keys = {"headers", "keys", "rows", "rows_matrix", "row_objects"}
    if contract_keys.intersection(set(data.keys())):
        return data

    for k in ("data", "result", "payload"):
        v = data.get(k)
        if isinstance(v, dict) and contract_keys.intersection(set(v.keys())):
            return v

    return data


def _extract_schema_pages(data: Any) -> List[str]:
    if data is None:
        return []
    if isinstance(data, list):
        out: List[str] = []
        for item in data:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                candidate = item.get("page") or item.get("sheet") or item.get("name") or item.get("id")
                if candidate:
                    out.append(str(candidate))
        return _dedupe_keep_order(out)

    if isinstance(data, dict):
        for k in ("pages", "items", "results"):
            if k in data:
                return _extract_schema_pages(data.get(k))
        if "data" in data:
            return _extract_schema_pages(data.get("data"))
    return []


def _sheet_rows_request_kwargs(method: str, sheet: str) -> Dict[str, Any]:
    if method == "GET":
        return {
            "params": {
                "sheet": sheet,
                "sheet_name": sheet,
                "page": sheet,
                "name": sheet,
                "tab": sheet,
                "symbols": "",
                "tickers": "",
                "include_matrix": "true",
                "include_headers": "true",
                "limit": 1,
            }
        }
    return {
        "json": {
            "sheet": sheet,
            "sheet_name": sheet,
            "page": sheet,
            "name": sheet,
            "tab": sheet,
            "symbols": [],
            "tickers": [],
            "include_matrix": True,
            "include_headers": True,
            "limit": 1,
            "offset": 0,
        }
    }


def _first_successful_response(client: Any, candidates: Sequence[str], *, headers: Dict[str, str]) -> Tuple[str, Any]:
    last = None
    for path in candidates:
        r = client.get(path, headers=headers)
        if r.status_code == 200:
            return path, r
        last = r
    if last is None:
        raise AssertionError(f"No candidate routes tested: {candidates}")
    raise AssertionError(f"All candidate routes failed: {candidates}. last={last.status_code} {last.text[:300]}")


# =============================================================================
# Tests
# =============================================================================
def test_schema_registry_has_all_required_sheets():
    sr = _load_schema_module()
    reg = _schema_registry_mapping(sr)
    missing = [s for s in REQUIRED_SHEETS if s not in reg]
    assert not missing, f"schema_registry missing sheets: {missing}"


def test_no_duplicate_headers_or_keys_per_sheet():
    sr = _load_schema_module()
    for sheet in REQUIRED_SHEETS:
        headers = _schema_sheet_headers(sr, sheet)
        keys = _schema_sheet_keys(sr, sheet)
        assert headers and keys, f"Sheet '{sheet}' has empty headers/keys."
        assert len(headers) == len(keys), f"Sheet '{sheet}' headers/keys length mismatch."

        hn = [_norm(h) for h in headers if str(h).strip()]
        kn = [_norm(k) for k in keys if str(k).strip()]
        dup_headers = sorted({h for h in hn if h and hn.count(h) > 1})
        dup_keys = sorted({k for k in kn if k and kn.count(k) > 1})
        assert not dup_headers, f"Duplicate headers in sheet '{sheet}': {dup_headers}"
        assert not dup_keys, f"Duplicate keys in sheet '{sheet}': {dup_keys}"


def test_top10_schema_has_required_special_fields():
    sr = _load_schema_module()
    keys = set(_schema_sheet_keys(sr, "Top_10_Investments"))
    missing = sorted(TOP10_REQUIRED_KEYS - keys)
    assert not missing, f"Top_10_Investments missing special keys: {missing}"


def test_top10_schema_has_recommendation_fields():
    sr = _load_schema_module()
    keys = set(_schema_sheet_keys(sr, "Top_10_Investments"))
    missing = sorted(TOP10_RECOMMENDATION_KEYS - keys)
    assert not missing, f"Top_10_Investments missing recommendation keys: {missing}"


def test_data_dictionary_matches_schema_registry():
    sr = _load_schema_module()
    dd_rows = _load_data_dictionary_rows_dicts()
    dd_map = _dd_map_sheet_to_keys(dd_rows)

    for sheet in REQUIRED_SHEETS:
        schema_keys = {k for k in _schema_sheet_keys(sr, sheet) if str(k).strip()}
        assert schema_keys, f"Schema has no keys for sheet '{sheet}'."
        dd_keys = {k for k in dd_map.get(sheet, set()) if str(k).strip()}
        assert dd_keys, f"Data_Dictionary missing entries for sheet '{sheet}'."

        missing_in_dd = sorted(schema_keys - dd_keys)
        extra_in_dd = sorted(dd_keys - schema_keys)
        assert not missing_in_dd, f"Data_Dictionary missing keys for '{sheet}': {missing_in_dd[:25]}"
        assert not extra_in_dd, f"Data_Dictionary has extra keys for '{sheet}': {extra_in_dd[:25]}"


def test_schema_pages_endpoint_returns_required_sheets(monkeypatch: pytest.MonkeyPatch):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        _, r = _first_successful_response(client, ["/v1/schema/pages", "/schema/pages"], headers=_auth_headers())
        pages = _extract_schema_pages(r.json())
        missing = [s for s in REQUIRED_SHEETS if s not in pages]
        assert not missing, f"schema pages endpoint missing required sheets: {missing}. got={pages}"


def test_sheet_spec_endpoint_is_reachable_and_schema_shaped(monkeypatch: pytest.MonkeyPatch):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        path, r = _first_successful_response(client, ["/v1/schema/sheet-spec", "/schema/sheet-spec"], headers=_auth_headers())
        data = r.json()
        payload = data.get("data") if isinstance(data, dict) else None
        assert isinstance(payload, (dict, list)), f"Unexpected {path} payload type: {type(payload)}"

        if isinstance(payload, dict):
            for sheet in REQUIRED_SHEETS:
                assert sheet in payload, f"{path} missing sheet '{sheet}'"
                spec = payload[sheet]
                assert isinstance(spec, dict), f"{path} entry for '{sheet}' is not dict"
                headers = spec.get("headers") or []
                keys = spec.get("keys") or []
                assert headers == _schema_sheet_headers(sr, sheet), f"{path} headers mismatch for '{sheet}'"
                assert keys == _schema_sheet_keys(sr, sheet), f"{path} keys mismatch for '{sheet}'"


def test_data_dictionary_endpoint_has_9_column_contract(monkeypatch: pytest.MonkeyPatch):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)
    from fastapi.testclient import TestClient

    candidates = [
        "/v1/schema/data-dictionary",
        "/v1/schema/data_dictionary",
        "/schema/data-dictionary",
        "/schema/data_dictionary",
    ]

    with TestClient(app) as client:
        path, r = _first_successful_response(client, candidates, headers=_auth_headers())
        payload = _extract_contract_payload(r.json())
        headers = payload.get("headers") or []
        rows = payload.get("rows") or []
        assert isinstance(headers, list), f"{path} headers is not a list"
        assert len(headers) == DATA_DICTIONARY_EXPECTED_COLS, f"{path} must have exactly {DATA_DICTIONARY_EXPECTED_COLS} headers, got {len(headers)}"
        assert isinstance(rows, list), f"{path} rows is not a list"
        assert rows, f"{path} rows is empty"

        if rows and isinstance(rows[0], dict):
            got_keys = set(rows[0].keys())
            expected_keys = set(_schema_sheet_keys(sr, "Data_Dictionary"))
            assert expected_keys.issubset(got_keys), f"{path} row keys mismatch. expected subset={expected_keys}, got={got_keys}"


def test_sheet_rows_returns_exact_schema_headers_and_keys_for_each_sheet(monkeypatch: pytest.MonkeyPatch):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)
    app.state.engine = _StubEngine(sr)
    app.state.engine_ready = True
    from fastapi.testclient import TestClient

    endpoints = _find_sheet_rows_endpoints(app)
    with TestClient(app) as client:
        for sheet in REQUIRED_SHEETS:
            expected_headers = _schema_sheet_headers(sr, sheet)
            expected_keys = _schema_sheet_keys(sr, sheet)
            assert expected_headers and expected_keys

            for method, path in endpoints:
                kwargs = _sheet_rows_request_kwargs(method, sheet)
                headers = _auth_headers()
                r = client.get(path, headers=headers, **kwargs) if method == "GET" else client.post(path, headers=headers, **kwargs)
                assert r.status_code == 200, f"{method} {path} failed for '{sheet}': {r.status_code} {r.text[:300]}"

                data = _extract_contract_payload(r.json())
                got_headers = data.get("headers") or []
                got_keys = data.get("keys") or []
                assert got_headers == expected_headers, (
                    f"{method} {path} headers mismatch for '{sheet}':\n"
                    f"expected({len(expected_headers)}): {expected_headers[:12]}...\n"
                    f"got({len(got_headers)}): {got_headers[:12]}..."
                )
                if got_keys:
                    assert got_keys == expected_keys, (
                        f"{method} {path} keys mismatch for '{sheet}':\n"
                        f"expected({len(expected_keys)}): {expected_keys[:12]}...\n"
                        f"got({len(got_keys)}): {got_keys[:12]}..."
                    )

                rows = data.get("rows") or []
                if isinstance(rows, list) and rows:
                    for i, row in enumerate(rows[:10]):
                        if isinstance(row, dict):
                            for k in expected_keys:
                                assert k in row, f"{method} {path} missing key '{k}' in '{sheet}' row#{i + 1}"
                        elif isinstance(row, list):
                            assert len(row) == len(expected_keys), f"{method} {path} row length mismatch for '{sheet}' row#{i + 1}"

                matrix = data.get("rows_matrix")
                if isinstance(matrix, list) and matrix:
                    for i, row in enumerate(matrix[:10]):
                        assert isinstance(row, list)
                        assert len(row) == len(expected_keys), f"{method} {path} rows_matrix length mismatch for '{sheet}' row#{i + 1}"


def test_top10_sheet_rows_includes_special_headers(monkeypatch: pytest.MonkeyPatch):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)
    from fastapi.testclient import TestClient

    endpoints = _find_sheet_rows_endpoints(app)
    target_endpoints = [(m, p) for (m, p) in endpoints if any(x in p.lower() for x in ("investment", "advisor", "advanced", "sheet-rows"))]
    if not target_endpoints:
        target_endpoints = endpoints

    with TestClient(app) as client:
        for method, path in target_endpoints:
            kwargs = _sheet_rows_request_kwargs(method, "Top_10_Investments")
            headers = _auth_headers()
            r = client.get(path, headers=headers, **kwargs) if method == "GET" else client.post(path, headers=headers, **kwargs)
            assert r.status_code == 200, f"{method} {path} failed for Top_10_Investments: {r.status_code} {r.text[:300]}"

            data = _extract_contract_payload(r.json())
            got_keys = set(data.get("keys") or [])
            missing = sorted(TOP10_REQUIRED_KEYS - got_keys)
            assert not missing, f"{method} {path} Top_10_Investments missing required keys: {missing}"

            reco_missing = sorted(TOP10_RECOMMENDATION_KEYS - got_keys)
            assert not reco_missing, f"{method} {path} Top_10_Investments missing recommendation keys: {reco_missing}"
