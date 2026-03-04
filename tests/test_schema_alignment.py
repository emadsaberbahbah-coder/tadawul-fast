#!/usr/bin/env python3
"""
tests/test_schema_alignment.py
----------------------------------------------------------------------
PHASE 8 — Schema Alignment & Quality Gates (Minimal Unit Tests)

What this test suite enforces
1) schema_registry has all required sheets (single view list)
2) no duplicate headers/keys per sheet
3) Data_Dictionary generator matches schema_registry
4) sheet-rows endpoint returns exact schema length for each sheet

Notes
- These tests are designed to be resilient to minor implementation differences
  (different registry variable/function names, different data_dictionary builders).
- No external network calls are required. We mount a stub engine on app.state.engine
  so sheet-rows routes can respond deterministically.

Run:
  pytest -q
"""

from __future__ import annotations

import inspect
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pytest


# =============================================================================
# Final sorted list (single view)
# =============================================================================
REQUIRED_SHEETS: List[str] = sorted(
    [
        "Market_Leaders",
        "Global_Markets",
        "Commodities_FX",
        "Mutual_Funds",
        "My_Portfolio",
        "Insights_Analysis",
        "Top_10_Investments",
        "Data_Dictionary",
    ]
)


# =============================================================================
# Helpers — schema registry discovery + normalization
# =============================================================================
def _import_any(*candidates: str):
    last = None
    for name in candidates:
        try:
            mod = __import__(name, fromlist=["__all__"])
            return mod
        except Exception as e:
            last = e
    raise RuntimeError(f"Unable to import any of: {candidates}. Last error: {last}")


def _first_callable(mod: Any, names: Sequence[str]) -> Optional[Any]:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


def _is_mapping_like(x: Any) -> bool:
    return isinstance(x, dict)


def _load_schema_registry() -> Dict[str, Any]:
    """
    Attempts to load the canonical schema registry from:
      core.sheets.schema_registry

    Supports multiple shapes:
      - SCHEMA_REGISTRY: dict[sheet -> sheet_def]
      - REGISTRY: dict[sheet -> sheet_def]
      - get_schema_registry(): returns dict
      - schema_registry(): returns dict
    """
    mod = _import_any("core.sheets.schema_registry", "core.sheets.schema_registry.py")  # second is harmless fallback
    fn = _first_callable(mod, ["get_schema_registry", "schema_registry", "get_registry", "registry"])
    if fn:
        reg = fn()
        if not _is_mapping_like(reg):
            raise AssertionError("schema_registry getter did not return a dict-like mapping.")
        return dict(reg)

    for attr in ("SCHEMA_REGISTRY", "REGISTRY", "SCHEMAS", "SHEET_SCHEMAS", "SHEET_REGISTRY"):
        reg = getattr(mod, attr, None)
        if _is_mapping_like(reg):
            return dict(reg)

    # last resort: search module globals for a dict with required sheet keys
    for _, v in vars(mod).items():
        if _is_mapping_like(v) and any(k in v for k in REQUIRED_SHEETS):
            return dict(v)

    raise AssertionError("Could not locate schema registry mapping in core.sheets.schema_registry")


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
    # best-effort
    try:
        return dict(getattr(x, "__dict__", {}))
    except Exception:
        return {}


def _extract_columns(sheet_def: Any) -> List[Dict[str, str]]:
    """
    Normalize a sheet definition into a list of {key, header} pairs.

    Supported shapes:
      - {"columns":[...]} or {"headers":[...]} or {"fields":[...]}
      - list[str] (treated as keys/headers)
      - list[dict|obj] where each has key/header/name/title/field
    """
    if sheet_def is None:
        return []

    # dict container forms
    if isinstance(sheet_def, dict):
        for k in ("columns", "fields", "schema", "defs"):
            if isinstance(sheet_def.get(k), list):
                return _extract_columns(sheet_def.get(k))
        for k in ("headers", "header"):
            if isinstance(sheet_def.get(k), list):
                # treat as list[str]
                return [{"key": str(h).strip(), "header": str(h).strip()} for h in sheet_def.get(k) or []]
        # if dict itself is key->column
        if all(isinstance(v, (dict, str)) for v in sheet_def.values()) and any(
            kk.lower() in ("key", "header", "name", "title", "field") for kk in sheet_def.keys()
        ):
            # ambiguous, but try object-to-dict
            d = _obj_to_dict(sheet_def)
            k = d.get("key") or d.get("name") or d.get("field") or d.get("id") or d.get("header")
            h = d.get("header") or d.get("title") or k
            if k:
                return [{"key": str(k).strip(), "header": str(h).strip()}]
        # fallback: empty (unknown dict shape)
        return []

    # dataclass / pydantic model
    if not isinstance(sheet_def, (list, tuple)) and (is_dataclass(sheet_def) or hasattr(sheet_def, "model_dump")):
        return _extract_columns(_obj_to_dict(sheet_def))

    # list/tuple
    if isinstance(sheet_def, (list, tuple)):
        cols: List[Dict[str, str]] = []
        for c in sheet_def:
            if c is None:
                continue
            if isinstance(c, str):
                s = c.strip()
                cols.append({"key": s, "header": s})
                continue
            if isinstance(c, dict):
                k = c.get("key") or c.get("name") or c.get("field") or c.get("id") or c.get("header")
                h = c.get("header") or c.get("title") or k
                if k is None:
                    continue
                cols.append({"key": str(k).strip(), "header": str(h).strip() if h is not None else str(k).strip()})
                continue
            # object column
            k = getattr(c, "key", None) or getattr(c, "name", None) or getattr(c, "field", None) or getattr(c, "id", None) or getattr(c, "header", None)
            h = getattr(c, "header", None) or getattr(c, "title", None) or k
            if k is None:
                continue
            cols.append({"key": str(k).strip(), "header": str(h).strip() if h is not None else str(k).strip()})
        return cols

    return []


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _sheet_key_to_cols(reg: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for sheet, sheet_def in reg.items():
        out[str(sheet)] = _extract_columns(sheet_def)
    return out


# =============================================================================
# Helpers — Data_Dictionary generator parsing
# =============================================================================
def _load_data_dictionary_rows() -> Tuple[List[str], List[List[Any]]]:
    """
    Try multiple possible generator functions in core.data_dictionary.

    Expected return shapes supported:
      - {"headers":[...], "rows":[...]}
      - (headers, rows)
      - rows only (then we infer minimal headers)
    """
    mod = _import_any("core.data_dictionary")

    fn = _first_callable(
        mod,
        [
            "generate_data_dictionary",
            "build_data_dictionary",
            "build_data_dictionary_rows",
            "generate_rows",
            "get_data_dictionary",
            "get_data_dictionary_rows",
        ],
    )
    if not fn:
        raise AssertionError("core.data_dictionary has no recognized generator function.")

    res = fn()
    # awaitable not expected in unit tests; if it is, fail loudly
    if inspect.isawaitable(res):
        raise AssertionError("Data_Dictionary generator returned awaitable; make it sync for tests.")

    if isinstance(res, tuple) and len(res) == 2:
        headers, rows = res
        if not isinstance(headers, list) or not isinstance(rows, list):
            raise AssertionError("Data_Dictionary generator returned invalid (headers, rows) types.")
        return [str(h) for h in headers], rows

    if isinstance(res, dict):
        headers = res.get("headers") or res.get("Columns") or res.get("columns") or []
        rows = res.get("rows") or res.get("Rows") or []
        if not isinstance(headers, list) or not isinstance(rows, list):
            raise AssertionError("Data_Dictionary generator dict shape missing list headers/rows.")
        return [str(h) for h in headers], rows

    if isinstance(res, list):
        # rows only
        return [], res

    raise AssertionError(f"Unrecognized Data_Dictionary generator result type: {type(res)}")


def _parse_data_dictionary(headers: List[str], rows: List[List[Any]]) -> Dict[str, Set[str]]:
    """
    Build mapping: sheet -> set(keys) from Data_Dictionary output.
    We try to locate columns for sheet & key.

    Common header names:
      Sheet / Sheet Name / Tab
      Key / Field / Column Key
    """
    # If headers are missing, attempt to interpret rows of dicts
    if not headers:
        if rows and isinstance(rows[0], dict):
            out: Dict[str, Set[str]] = {}
            for r in rows:
                if not isinstance(r, dict):
                    continue
                sheet = r.get("Sheet") or r.get("sheet") or r.get("Sheet Name") or r.get("tab") or r.get("Tab")
                key = r.get("Key") or r.get("key") or r.get("Field") or r.get("field") or r.get("Column Key") or r.get("column_key")
                if not sheet or not key:
                    continue
                out.setdefault(str(sheet), set()).add(str(key))
            return out
        raise AssertionError("Data_Dictionary rows missing headers and not dict-rows; cannot parse.")

    hmap = {_norm(h): i for i, h in enumerate(headers)}
    sheet_idx = None
    key_idx = None

    for cand in ("sheet", "sheet name", "tab", "page"):
        if cand in hmap:
            sheet_idx = hmap[cand]
            break

    for cand in ("key", "field", "column key", "column_key", "schema key", "schema_key"):
        if cand in hmap:
            key_idx = hmap[cand]
            break

    if sheet_idx is None or key_idx is None:
        raise AssertionError(
            f"Cannot parse Data_Dictionary: missing sheet/key columns in headers={headers[:12]}"
        )

    out2: Dict[str, Set[str]] = {}
    for r in rows:
        if not isinstance(r, (list, tuple)):
            continue
        sheet = r[sheet_idx] if sheet_idx < len(r) else None
        key = r[key_idx] if key_idx < len(r) else None
        if not sheet or not key:
            continue
        out2.setdefault(str(sheet), set()).add(str(key))
    return out2


# =============================================================================
# Helpers — FastAPI sheet-rows contract tests (stub engine)
# =============================================================================
class _StubEngine:
    """
    Minimal engine to satisfy sheet-rows routes without any network.
    Returns schema headers and empty rows for any sheet.
    """

    def __init__(self, schema_map: Dict[str, List[str]]):
        self._schema = schema_map

    # Snapshot API (preferred)
    def get_cached_sheet_snapshot(self, sheet_name: str) -> Dict[str, Any]:
        headers = self._schema.get(str(sheet_name), [])
        return {"headers": headers, "rows": [], "cached_at_utc": _utc_iso()}

    def get_cached_multi_sheet_snapshots(self, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for s in sheet_names or []:
            out[str(s)] = self.get_cached_sheet_snapshot(str(s))
        return out

    # Legacy API fallbacks
    def get_sheet_rows(self, sheet_name: str):
        headers = self._schema.get(str(sheet_name), [])
        return headers, []

    def get_cached_sheet_rows(self, sheet_name: str):
        return self.get_sheet_rows(sheet_name)

    def get_sheet(self, sheet_name: str):
        return self.get_sheet_rows(sheet_name)


def _build_schema_headers_map(reg: Dict[str, Any]) -> Dict[str, List[str]]:
    cols_map = _sheet_key_to_cols(reg)
    out: Dict[str, List[str]] = {}
    for sheet, cols in cols_map.items():
        # Prefer "header" if present; but sheet-rows should expose headers as in registry order.
        # We map to the header string; if blank, fall back to key.
        out[sheet] = [c.get("header") or c.get("key") or "" for c in cols]
    return out


def _resolve_sheet_rows_path(app: Any) -> str:
    """
    Find an existing sheet-rows endpoint among known candidates.
    """
    candidates = [
        "/v1/analysis/sheet-rows",
        "/v1/advanced/sheet-rows",
        "/v1/enriched/sheet-rows",
        "/v1/ai/sheet-rows",
    ]
    paths = {getattr(r, "path", "") for r in getattr(app.router, "routes", [])}
    for c in candidates:
        if c in paths:
            return c
    raise AssertionError(f"No known sheet-rows endpoint found. Available paths sample={sorted(list(paths))[:40]}")


# =============================================================================
# Tests
# =============================================================================
def test_schema_registry_has_all_required_sheets():
    reg = _load_schema_registry()
    missing = [s for s in REQUIRED_SHEETS if s not in reg]
    assert not missing, f"schema_registry missing sheets: {missing}"


def test_no_duplicate_headers_or_keys_per_sheet():
    reg = _load_schema_registry()
    cols_map = _sheet_key_to_cols(reg)

    for sheet in REQUIRED_SHEETS:
        cols = cols_map.get(sheet, [])
        assert cols, f"Sheet '{sheet}' has no columns in schema_registry."

        keys = [_norm(c.get("key", "")) for c in cols if c.get("key")]
        headers = [_norm(c.get("header", "")) for c in cols if c.get("header")]

        # duplicates ignoring case/whitespace
        dup_keys = sorted({k for k in keys if k and keys.count(k) > 1})
        dup_headers = sorted({h for h in headers if h and headers.count(h) > 1})

        assert not dup_keys, f"Duplicate keys in sheet '{sheet}': {dup_keys}"
        assert not dup_headers, f"Duplicate headers in sheet '{sheet}': {dup_headers}"


def test_data_dictionary_matches_schema_registry():
    reg = _load_schema_registry()
    cols_map = _sheet_key_to_cols(reg)

    dd_headers, dd_rows = _load_data_dictionary_rows()
    dd_map = _parse_data_dictionary(dd_headers, dd_rows)

    # Compare only required sheets
    for sheet in REQUIRED_SHEETS:
        schema_keys = {str(c.get("key") or "").strip() for c in cols_map.get(sheet, []) if (c.get("key") or "").strip()}
        assert schema_keys, f"Schema has no keys for sheet '{sheet}'."

        dd_keys = {str(k).strip() for k in dd_map.get(sheet, set()) if str(k).strip()}
        assert dd_keys, f"Data_Dictionary missing entries for sheet '{sheet}'."

        missing_in_dd = sorted(schema_keys - dd_keys)
        extra_in_dd = sorted(dd_keys - schema_keys)

        assert not missing_in_dd, f"Data_Dictionary missing keys for '{sheet}': {missing_in_dd[:20]}"
        assert not extra_in_dd, f"Data_Dictionary has extra keys for '{sheet}': {extra_in_dd[:20]}"


def test_sheet_rows_returns_exact_schema_length_for_each_sheet():
    reg = _load_schema_registry()
    schema_headers_map = _build_schema_headers_map(reg)

    # Create app and mount routes via startup (as your main.py does)
    try:
        from main import create_app  # type: ignore
    except Exception:
        # fallback if app is exported directly
        from main import app as _app  # type: ignore

        def create_app():  # type: ignore
            return _app

    app = create_app()
    # stub engine to avoid external calls
    try:
        app.state.engine = _StubEngine(schema_headers_map)
        app.state.engine_ready = True
    except Exception:
        pass

    from fastapi.testclient import TestClient  # local import (pytest-only)

    with TestClient(app) as client:
        path = _resolve_sheet_rows_path(app)

        for sheet in REQUIRED_SHEETS:
            expected_len = len(schema_headers_map.get(sheet, []))
            assert expected_len > 0, f"Schema headers empty for sheet '{sheet}'."

            # Minimal payload designed to work across your handlers:
            payload = {
                "sheet_name": sheet,
                "tickers": [],
                "refresh": False,
                "include_meta": True,
                "limit": 0,
            }
            r = client.post(path, json=payload)
            assert r.status_code == 200, f"{path} failed for sheet '{sheet}': {r.status_code} {r.text[:200]}"
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            headers = data.get("headers") or []
            assert isinstance(headers, list) and headers, f"{path} returned no headers for '{sheet}'. payload={payload}"
            assert len(headers) == expected_len, (
                f"Schema/header length mismatch for '{sheet}': "
                f"expected={expected_len}, got={len(headers)}"
            )

            # If rows exist, each row must match header length (quality gate)
            rows = data.get("rows") or []
            if isinstance(rows, list) and rows:
                for i, row in enumerate(rows[:10]):
                    if isinstance(row, list):
                        assert len(row) == len(headers), f"Row length mismatch for '{sheet}' row#{i+1}"
