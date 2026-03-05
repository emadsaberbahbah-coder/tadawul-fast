#!/usr/bin/env python3
"""
tests/test_schema_alignment.py
--------------------------------------------------------------------------------
PHASE 8 — Schema Alignment & Quality Gates (CI Regression Catcher)

What this test suite enforces
1) schema_registry has all required sheets (single canonical list)
2) no duplicate headers/keys per sheet
3) Data_Dictionary generator matches schema_registry (no drift)
4) sheet-rows endpoints return EXACT schema headers/keys length + order per sheet

Design goals
- Resilient to minor implementation differences (different module/function names)
- No external network calls:
  - We create a local FastAPI app and mount routers via routes.mount_routers()
  - We mount a stub engine on app.state.engine

Run:
  pytest -q
"""

from __future__ import annotations

import inspect
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pytest


# =============================================================================
# Required sheets (single view)
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


# =============================================================================
# Helpers — robust imports
# =============================================================================
def _import_any(*candidates: str):
    last = None
    for name in candidates:
        try:
            mod = __import__(name, fromlist=["__all__"])
            return mod
        except Exception as e:
            last = e
    raise RuntimeError(f"Unable to import any of: {candidates}. Last error: {last!r}")


def _first_callable(mod: Any, names: Sequence[str]) -> Optional[Any]:
    for n in names:
        fn = getattr(mod, n, None)
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
    try:
        return dict(getattr(x, "__dict__", {}))
    except Exception:
        return {}


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


# =============================================================================
# Schema registry access (authoritative)
# =============================================================================
def _load_schema_module():
    return _import_any("core.sheets.schema_registry")


def _schema_sheet_headers(sr: Any, sheet: str) -> List[str]:
    fn = getattr(sr, "get_sheet_headers", None)
    if callable(fn):
        return list(fn(sheet))
    # fallback: from spec.columns
    spec = sr.get_sheet_spec(sheet) if callable(getattr(sr, "get_sheet_spec", None)) else None
    cols = getattr(spec, "columns", None) or []
    out = []
    for c in cols:
        h = getattr(c, "header", None) or _obj_to_dict(c).get("header")
        k = getattr(c, "key", None) or _obj_to_dict(c).get("key")
        out.append(str(h or k or "").strip())
    return out


def _schema_sheet_keys(sr: Any, sheet: str) -> List[str]:
    fn = getattr(sr, "get_sheet_keys", None)
    if callable(fn):
        return list(fn(sheet))
    spec = sr.get_sheet_spec(sheet) if callable(getattr(sr, "get_sheet_spec", None)) else None
    cols = getattr(spec, "columns", None) or []
    out = []
    for c in cols:
        k = getattr(c, "key", None) or _obj_to_dict(c).get("key")
        if k:
            out.append(str(k).strip())
    return out


def _schema_registry_mapping(sr: Any) -> Dict[str, Any]:
    for attr in ("SCHEMA_REGISTRY", "REGISTRY", "SHEET_REGISTRY"):
        reg = getattr(sr, attr, None)
        if isinstance(reg, dict):
            return dict(reg)
    fn = _first_callable(sr, ["get_schema_registry", "schema_registry", "get_registry", "registry"])
    if fn:
        reg = fn()
        if isinstance(reg, dict):
            return dict(reg)
    raise AssertionError("Could not locate schema registry mapping in core.sheets.schema_registry")


# =============================================================================
# Data_Dictionary generator (must be schema-driven)
# =============================================================================
def _load_data_dictionary_rows_dicts() -> List[Dict[str, Any]]:
    """
    Preferred: core.sheets.data_dictionary.build_data_dictionary_rows -> list[dict]
    Fallbacks: core.data_dictionary.* or values builders (2D arrays)
    """
    # Preferred module (your current design)
    try:
        mod = _import_any("core.sheets.data_dictionary")
    except Exception:
        mod = _import_any("core.data_dictionary")

    # Preferred function returns list[dict]
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

    # Fallback: values builder (2D array)
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

    # Expect: list[list] with optional header row
    if not isinstance(res2, list) or (res2 and not isinstance(res2[0], (list, tuple))):
        raise AssertionError(f"Unrecognized Data_Dictionary values generator result type: {type(res2)}")

    values = res2
    if not values:
        return []

    # If the first row looks like headers, use it
    headers = [str(x) for x in values[0]]
    rows = values[1:]

    hmap = {_norm(h): i for i, h in enumerate(headers)}
    sheet_idx = hmap.get("sheet") or hmap.get("sheet name") or hmap.get("tab") or hmap.get("page")
    key_idx = hmap.get("key") or hmap.get("column key") or hmap.get("field") or hmap.get("schema key")

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
    """
    Map: sheet -> set(keys) from Data_Dictionary rows.
    Supports either Data_Dictionary schema keys or human headers.
    """
    out: Dict[str, Set[str]] = {}
    for r in dd_rows:
        if not isinstance(r, dict):
            continue
        sheet = (
            r.get("sheet")
            or r.get("Sheet")
            or r.get("page")
            or r.get("Page")
            or r.get("tab")
            or r.get("Tab")
        )
        key = r.get("key") or r.get("Key") or r.get("field") or r.get("Field") or r.get("column_key") or r.get("Column Key")
        if not sheet or not key:
            continue
        out.setdefault(str(sheet), set()).add(str(key))
    return out


# =============================================================================
# FastAPI sheet-rows contract tests (local app + stub engine)
# =============================================================================
class _StubEngine:
    """
    Minimal engine that returns schema-correct shapes with no network calls.
    If any route/builder calls into the engine, it won't crash.
    """

    def __init__(self, sr: Any):
        self._sr = sr

    # Engine-native sheet rows
    async def get_sheet_rows(self, *, sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None):
        headers = _schema_sheet_headers(self._sr, sheet)
        keys = _schema_sheet_keys(self._sr, sheet)
        return {
            "status": "success",
            "sheet": sheet,
            "page": sheet,
            "headers": headers,
            "keys": keys,
            "rows": [],
            "rows_matrix": [],
            "meta": {"stub": True, "limit": limit, "offset": offset, "mode": mode},
        }

    # Quote batch methods (if any router uses them)
    async def get_enriched_quotes_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None):
        out: Dict[str, Dict[str, Any]] = {}
        for s in symbols or []:
            out[s] = {"symbol": s}
        return out

    async def get_enriched_quote_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None):
        return {"symbol": symbol}

    async def health(self):
        return {"status": "ok", "stub": True}


def _build_test_app() -> Any:
    """
    Build a local FastAPI app and mount routers using routes.mount_routers().
    This avoids relying on main.py structure and keeps tests deterministic.
    """
    from fastapi import FastAPI  # local import for pytest

    app = FastAPI(title="TFB Test App")

    # Mount routes (your dynamic loader)
    routes_pkg = _import_any("routes")
    mount_fn = getattr(routes_pkg, "mount_routers", None)
    if not callable(mount_fn):
        raise AssertionError("routes.mount_routers not found; cannot mount routers for tests.")

    mount_fn(app, strict=False)
    return app


def _find_sheet_rows_paths(app: Any) -> List[str]:
    """
    Return all POST endpoints ending with /sheet-rows (supports multiple families).
    """
    paths: Set[str] = set()
    for r in getattr(app.router, "routes", []) or []:
        p = getattr(r, "path", "") or ""
        methods = getattr(r, "methods", None) or set()
        if "POST" in {str(m).upper() for m in methods} and str(p).endswith("/sheet-rows"):
            paths.add(str(p))
    if not paths:
        raise AssertionError("No POST */sheet-rows endpoint found in mounted routes.")
    return sorted(paths)


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

        # duplicates ignoring case/whitespace
        hn = [_norm(h) for h in headers if str(h).strip()]
        kn = [_norm(k) for k in keys if str(k).strip()]

        dup_headers = sorted({h for h in hn if h and hn.count(h) > 1})
        dup_keys = sorted({k for k in kn if k and kn.count(k) > 1})

        assert not dup_headers, f"Duplicate headers in sheet '{sheet}': {dup_headers}"
        assert not dup_keys, f"Duplicate keys in sheet '{sheet}': {dup_keys}"


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


def test_sheet_rows_returns_exact_schema_headers_and_keys_for_each_sheet():
    sr = _load_schema_module()
    app = _build_test_app()

    # stub engine for any builder/route that tries to call it
    try:
        app.state.engine = _StubEngine(sr)
        app.state.engine_ready = True
    except Exception:
        pass

    from fastapi.testclient import TestClient  # pytest-only import

    paths = _find_sheet_rows_paths(app)

    with TestClient(app) as client:
        for sheet in REQUIRED_SHEETS:
            expected_headers = _schema_sheet_headers(sr, sheet)
            expected_keys = _schema_sheet_keys(sr, sheet)
            assert expected_headers and expected_keys

            # Call ANY available sheet-rows path; all must obey schema-first contract
            for path in paths:
                payload = {
                    # accept multiple body keys across routers
                    "sheet": sheet,
                    "sheet_name": sheet,
                    "page": sheet,
                    # keep symbols empty => schema-only for instrument/table pages (no engine calls)
                    "symbols": [],
                    "tickers": [],
                    "include_matrix": True,
                    "limit": 1,
                    "offset": 0,
                }
                r = client.post(path, json=payload)
                assert r.status_code == 200, f"{path} failed for '{sheet}': {r.status_code} {r.text[:250]}"
                data = r.json()

                headers = data.get("headers") or []
                keys = data.get("keys") or []

                assert headers == expected_headers, (
                    f"{path} headers mismatch for '{sheet}':\n"
                    f"expected({len(expected_headers)}): {expected_headers[:12]}...\n"
                    f"got({len(headers)}): {headers[:12]}..."
                )

                # If keys are provided, they must match exactly
                if keys:
                    assert keys == expected_keys, (
                        f"{path} keys mismatch for '{sheet}':\n"
                        f"expected({len(expected_keys)}): {expected_keys[:12]}...\n"
                        f"got({len(keys)}): {keys[:12]}..."
                    )

                # Quality: rows (if any) must align to keys
                rows = data.get("rows") or []
                if isinstance(rows, list) and rows:
                    # rows may be dicts or lists depending on router
                    for i, row in enumerate(rows[:10]):
                        if isinstance(row, dict):
                            for k in expected_keys:
                                assert k in row, f"{path} missing key '{k}' in '{sheet}' row#{i+1}"
                        elif isinstance(row, list):
                            assert len(row) == len(expected_keys), f"{path} row length mismatch for '{sheet}' row#{i+1}"

                # If rows_matrix exists, it must match schema length
                matrix = data.get("rows_matrix")
                if isinstance(matrix, list) and matrix:
                    for i, row in enumerate(matrix[:10]):
                        assert isinstance(row, list)
                        assert len(row) == len(expected_keys), f"{path} rows_matrix length mismatch for '{sheet}' row#{i+1}"
