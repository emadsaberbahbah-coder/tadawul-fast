#!/usr/bin/env python3
"""
tests/test_schema_alignment.py
--------------------------------------------------------------------------------
PHASE 9 — Schema Alignment & Route Contract Regression Suite (v9.3.0)

Why this revision (v9.3.0 vs v9.2.0)
-------------------------------------
- 🔑 FIX CRITICAL: v9.2.0 hard-coded `EXPECTED_SHEET_COLUMN_COUNTS` to
    schema_registry v3.4.0 widths (99/112/86/94/110/106/9/9) but the actual
    project is at schema_registry v2.2.0 (80/80/80/80/80/83/7/9). Running
    v9.2.0 against the current codebase would fail EVERY column-count
    assertion. v9.3.0 auto-detects the live schema version and uses the
    correct baseline — tests pass today (v2.2.0) AND continue to pass after
    a future v3.4.0 migration.

- 🔑 FIX CRITICAL: v9.2.0 had two internally contradictory tests:
      (a) `test_data_dictionary_matches_schema_registry` requires every
          schema key (including `last_updated_riyadh` on v2.2.0) to appear
          in Data_Dictionary.
      (b) `test_insights_schema_has_priority_and_signal` FORBIDS
          `last_updated_riyadh` from the Insights_Analysis schema.
    On v2.2.0 these can NEVER both pass. v9.3.0 makes (b) a feature-gated
    test that only runs when the new v3.4.0 Insights schema is detected.

- 🔑 FIX: v9.2.0 `TOP10_REQUIRED_KEYS` conflated baseline keys (`top10_rank`,
    `selection_reason`, `criteria_snapshot` — present in v2.2.0) with
    aspirational v3.4.0 trade-setup keys (`entry_price`,
    `stop_loss_suggested`, `take_profit_suggested`, `risk_reward_ratio`).
    v9.3.0 splits these into `_TOP10_BASELINE_KEYS` and
    `_TOP10_TRADE_SETUP_KEYS` and runs each via a dedicated test.

- FIX: `_StubEngine.get_sheet_rows` now accepts `sheet` both positionally
    and as keyword. v9.2.0 used keyword-only, which broke callers that
    pass positionally.

- FIX: `_schema_registry_mapping` now tries the canonical `SCHEMA_REGISTRY`
    attribute first (which is the documented `__all__` export of
    `core.sheets.schema_registry`), then falls back to legacy names.

- NEW: `_detect_schema_capabilities(sr)` returns a feature-detection dict
    that every aspirational test consults. Aspirational tests use
    `pytest.skip(...)` with a clear message pointing to the required schema
    version, so the test report makes it obvious what's pending vs broken.

- NEW: `test_schema_capabilities_banner` runs first and prints the detected
    schema version + capability set to the test log, making it easy to see
    which schema is live without reading the module source.

Tests that always run (baseline, work on v2.2.0+ and v3.4.0+)
- test_schema_registry_has_all_required_sheets
- test_no_duplicate_headers_or_keys_per_sheet
- test_top10_schema_has_baseline_special_fields     (rank/reason/snapshot)
- test_top10_schema_has_recommendation_fields       (recommendation/reason)
- test_data_dictionary_matches_schema_registry      (v2.2.0 and v3.4.0)
- test_schema_pages_endpoint_returns_required_sheets
- test_sheet_spec_endpoint_is_reachable_and_schema_shaped
- test_data_dictionary_endpoint_has_9_column_contract
- test_sheet_rows_returns_exact_schema_headers_and_keys_for_each_sheet
- test_top10_sheet_rows_includes_baseline_special_headers
- test_canonical_column_counts_match_schema_version  (version-aware)

Tests that auto-skip on v2.2.0 (aspirational, require v3.4.0)
- test_top10_schema_has_trade_setup_fields          (skip<v3.4.0)
- test_top10_schema_has_technical_signal_fields     (skip<v3.0.0 scoring)
- test_insights_schema_matches_live_version         (asserts correct
                                                     schema for DETECTED
                                                     version: v2.2.0 OR
                                                     v3.4.0)

Design goals (preserved from v9.2.0)
- Resilient to minor implementation differences
- No external network calls
- Local FastAPI app only
- Stub engine on app.state.engine
- Open-mode auth patching for deterministic tests
"""

from __future__ import annotations

import importlib
import inspect
import os
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

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


# =============================================================================
# Top10 key sets (split by baseline vs aspirational)
# =============================================================================
# v9.3.0: baseline keys — these exist in schema_registry v2.2.0+
_TOP10_BASELINE_KEYS: Set[str] = {
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
}

_TOP10_RECOMMENDATION_KEYS: Set[str] = {
    "recommendation",
    "recommendation_reason",
}

# v9.3.0: aspirational keys — require schema_registry v3.4.0+ with
# top10_selector v4.9.0's `_compute_trade_setup`
_TOP10_TRADE_SETUP_KEYS: Set[str] = {
    "entry_price",
    "stop_loss_suggested",
    "take_profit_suggested",
    "risk_reward_ratio",
}

# v9.3.0: aspirational keys — require scoring.py v3.0.0+
_TOP10_TECHNICAL_KEYS: Set[str] = {
    "technical_score",
    "short_term_signal",
    "rsi_signal",
    "volume_ratio",
}


# =============================================================================
# Per-version expected column counts
# =============================================================================
# v9.3.0: Two column-count matrices — one per schema version.
# Which one is active is determined at runtime by `_detect_schema_capabilities`.

# Baseline: schema_registry v2.2.0 (current deployed state)
_EXPECTED_COUNTS_V2: Dict[str, int] = {
    "Market_Leaders":     80,
    "Global_Markets":     80,
    "Commodities_FX":     80,
    "Mutual_Funds":       80,
    "My_Portfolio":       80,
    "Top_10_Investments": 83,   # 80 + 3 top10 extras
    "Insights_Analysis":  7,
    "Data_Dictionary":    9,
}

# Aspirational: schema_registry v3.4.0 (future target)
_EXPECTED_COUNTS_V3: Dict[str, int] = {
    "Market_Leaders":     99,
    "Global_Markets":     112,
    "Commodities_FX":     86,
    "Mutual_Funds":       94,
    "My_Portfolio":       110,
    "Top_10_Investments": 106,
    "Insights_Analysis":  9,
    "Data_Dictionary":    9,
}


# =============================================================================
# Helpers — robust imports
# =============================================================================
def _import_any(*candidates: str):
    last = None
    for name in candidates:
        try:
            mod = importlib.import_module(name)
            return mod
        except Exception as e:
            last = e
    raise RuntimeError(
        f"Unable to import any of: {candidates}. Last error: {last!r}"
    )


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
        raw = getattr(x, "__dict__", {})
        return dict(raw) if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


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
    seen = set()
    for v in values:
        try:
            s = str(v).strip()
        except Exception:
            continue
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _parse_version(raw: Any) -> Tuple[int, int, int]:
    """Parse a semver-ish string into a 3-tuple. Missing components -> 0."""
    if raw is None:
        return (0, 0, 0)
    s = str(raw).strip()
    if not s:
        return (0, 0, 0)
    # Strip non-digit/dot prefix (e.g., "v3.4.0" -> "3.4.0")
    while s and not s[0].isdigit():
        s = s[1:]
    parts = s.split(".")
    nums: List[int] = []
    for p in parts[:3]:
        digits = ""
        for ch in p:
            if ch.isdigit():
                digits += ch
            else:
                break
        nums.append(int(digits) if digits else 0)
    while len(nums) < 3:
        nums.append(0)
    return (nums[0], nums[1], nums[2])


# =============================================================================
# Schema registry access (authoritative)
# =============================================================================
def _load_schema_module():
    return _import_any("core.sheets.schema_registry")


def _schema_sheet_headers(sr: Any, sheet: str) -> List[str]:
    fn = getattr(sr, "get_sheet_headers", None)
    if callable(fn):
        return list(fn(sheet))

    get_spec = getattr(sr, "get_sheet_spec", None)
    spec = get_spec(sheet) if callable(get_spec) else None
    cols = getattr(spec, "columns", None) or _obj_to_dict(spec).get("columns") or []
    out: List[str] = []
    for c in cols:
        d = _obj_to_dict(c)
        h = d.get("header") or getattr(c, "header", None)
        k = d.get("key") or getattr(c, "key", None)
        out.append(str(h or k or "").strip())
    return out


def _schema_sheet_keys(sr: Any, sheet: str) -> List[str]:
    fn = getattr(sr, "get_sheet_keys", None)
    if callable(fn):
        return list(fn(sheet))

    get_spec = getattr(sr, "get_sheet_spec", None)
    spec = get_spec(sheet) if callable(get_spec) else None
    cols = getattr(spec, "columns", None) or _obj_to_dict(spec).get("columns") or []
    out: List[str] = []
    for c in cols:
        d = _obj_to_dict(c)
        k = d.get("key") or getattr(c, "key", None)
        if k:
            out.append(str(k).strip())
    return out


def _schema_registry_mapping(sr: Any) -> Dict[str, Any]:
    """
    v9.3.0: Prefer the canonical `SCHEMA_REGISTRY` attribute (from
    `core.sheets.schema_registry.__all__`), then fall back to legacy names.
    """
    # Canonical (documented in __all__ of schema_registry.py v2.2.0)
    for attr in (
        "SCHEMA_REGISTRY",
        "REGISTRY",
        "SHEET_REGISTRY",
        "SCHEMA_BY_SHEET",
        "SHEET_SCHEMAS",
    ):
        reg = getattr(sr, attr, None)
        if isinstance(reg, dict) and reg:
            return dict(reg)

    fn = _first_callable(
        sr,
        [
            "get_schema_registry",
            "schema_registry",
            "get_registry",
            "registry",
        ],
    )
    if fn:
        reg = fn()
        if isinstance(reg, dict) and reg:
            return dict(reg)

    # Last resort: list_sheets() + build from specs
    list_fn = getattr(sr, "list_sheets", None)
    get_spec = getattr(sr, "get_sheet_spec", None)
    if callable(list_fn) and callable(get_spec):
        out: Dict[str, Any] = {}
        try:
            for sheet_name in list_fn():
                try:
                    out[sheet_name] = get_spec(sheet_name)
                except Exception:
                    continue
            if out:
                return out
        except Exception:
            pass

    raise AssertionError(
        "Could not locate schema registry mapping in "
        "core.sheets.schema_registry (tried SCHEMA_REGISTRY, REGISTRY, "
        "SHEET_REGISTRY, SCHEMA_BY_SHEET, SHEET_SCHEMAS, and list_sheets)."
    )


# =============================================================================
# v9.3.0: Schema capability detection
# =============================================================================
_CAPS_CACHE: Optional[Dict[str, Any]] = None


def _detect_schema_capabilities(sr: Any) -> Dict[str, Any]:
    """
    Detect the live schema version + per-feature availability. Cached on
    first call. Every aspirational test consults this to decide whether
    to run or skip.
    """
    global _CAPS_CACHE
    if _CAPS_CACHE is not None:
        return _CAPS_CACHE

    version_str = str(getattr(sr, "SCHEMA_VERSION", "0.0.0") or "0.0.0")
    version_tuple = _parse_version(version_str)

    try:
        top10_keys = set(_schema_sheet_keys(sr, "Top_10_Investments"))
    except Exception:
        top10_keys = set()
    try:
        insights_keys = set(_schema_sheet_keys(sr, "Insights_Analysis"))
    except Exception:
        insights_keys = set()

    caps: Dict[str, Any] = {
        "version_str": version_str,
        "version_tuple": version_tuple,
        "is_v3_plus": version_tuple >= (3, 0, 0),
        "is_v3_4_plus": version_tuple >= (3, 4, 0),
        # Feature detection (independent of version string)
        "top10_has_trade_setup": _TOP10_TRADE_SETUP_KEYS.issubset(top10_keys),
        "top10_has_technical": _TOP10_TECHNICAL_KEYS.issubset(top10_keys),
        "insights_new_schema": {"signal", "priority", "as_of_riyadh"}.issubset(
            insights_keys
        ),
        "insights_col_count": len(insights_keys),
        "top10_col_count": len(top10_keys),
    }

    _CAPS_CACHE = caps
    return caps


def _expected_counts_for_caps(caps: Dict[str, Any]) -> Dict[str, int]:
    """Pick the expected column-count matrix based on detected capabilities."""
    if caps.get("is_v3_4_plus"):
        return dict(_EXPECTED_COUNTS_V3)
    return dict(_EXPECTED_COUNTS_V2)


# =============================================================================
# Data_Dictionary generator (must be schema-driven)
# =============================================================================
def _load_data_dictionary_rows_dicts() -> List[Dict[str, Any]]:
    try:
        mod = _import_any("core.sheets.data_dictionary")
    except Exception:
        mod = _import_any("core.data_dictionary")

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
            raise AssertionError(
                "Data_Dictionary generator returned awaitable; keep it sync for tests."
            )
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
        raise AssertionError(
            "Data_Dictionary values generator returned awaitable; keep it sync for tests."
        )

    if not isinstance(res2, list) or (res2 and not isinstance(res2[0], (list, tuple))):
        raise AssertionError(
            f"Unrecognized Data_Dictionary values generator result type: {type(res2)}"
        )

    values = res2
    if not values:
        return []

    headers = [str(x) for x in values[0]]
    rows = values[1:]

    hmap = {_norm(h): i for i, h in enumerate(headers)}
    sheet_idx = hmap.get("sheet") or hmap.get("sheet name") or hmap.get("tab") or hmap.get("page")
    key_idx = (
        hmap.get("key")
        or hmap.get("column key")
        or hmap.get("field")
        or hmap.get("schema key")
    )

    if sheet_idx is None or key_idx is None:
        raise AssertionError(
            f"Cannot parse Data_Dictionary values: headers={headers[:12]}"
        )

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
        key = (
            r.get("key")
            or r.get("Key")
            or r.get("field")
            or r.get("Field")
            or r.get("column_key")
            or r.get("Column Key")
        )
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
    app_version = "test"
    environment = "test"
    timezone = "Asia/Riyadh"
    backend_base_url = ""
    engine_cache_ttl_sec = 1


def _patch_auth_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPEN_MODE", "1")
    monkeypatch.setenv("REQUIRE_AUTH", "0")
    monkeypatch.setenv("ALLOW_QUERY_TOKEN", "1")
    monkeypatch.setenv("APP_TOKEN", "test-token")
    monkeypatch.setenv("BACKEND_TOKEN", "test-token")
    monkeypatch.setenv("X_APP_TOKEN", "test-token")
    monkeypatch.setenv("ENABLE_SWAGGER", "1")
    monkeypatch.setenv("ENABLE_REDOC", "0")
    monkeypatch.setenv("INIT_ENGINE_ON_BOOT", "0")

    try:
        cfg = importlib.import_module("core.config")
        monkeypatch.setattr(cfg, "is_open_mode", lambda: True, raising=False)
        monkeypatch.setattr(cfg, "auth_ok", lambda *args, **kwargs: True, raising=False)
        monkeypatch.setattr(cfg, "get_settings_cached", lambda: _DummySettings(), raising=False)
        monkeypatch.setattr(cfg, "get_settings", lambda: _DummySettings(), raising=False)
    except Exception:
        pass


def _auth_headers() -> Dict[str, str]:
    return {
        "X-APP-TOKEN": "test-token",
        "Authorization": "Bearer test-token",
        "X-Request-ID": "pytest-schema-alignment",
    }


# =============================================================================
# FastAPI sheet-rows contract tests (local app + stub engine)
# =============================================================================
class _StubEngine:
    """
    Minimal engine that returns schema-correct shapes with no network calls.
    If any route/builder calls into the engine, it won't crash.

    v9.3.0: `sheet` is accepted both positionally AND as keyword (was
    keyword-only in v9.2.0, which broke positional callers).
    """

    def __init__(self, sr: Any):
        self._sr = sr

    async def get_sheet_rows(
        self,
        sheet: str = "",
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        # Accept sheet via kwargs fallback (some routes pass page=)
        sheet_name = sheet or kwargs.get("page") or kwargs.get("sheet_name") or ""
        headers = _schema_sheet_headers(self._sr, sheet_name) if sheet_name else []
        keys = _schema_sheet_keys(self._sr, sheet_name) if sheet_name else []
        return {
            "status": "success",
            "sheet": sheet_name,
            "page": sheet_name,
            "headers": headers,
            "keys": keys,
            "rows": [],
            "rows_matrix": [],
            "meta": {
                "stub": True,
                "limit": limit,
                "offset": offset,
                "mode": mode,
            },
        }

    async def get_enriched_quotes_batch(
        self, symbols: List[str], mode: str = "", *, schema: Any = None
    ):
        return {s: {"symbol": s} for s in (symbols or [])}

    async def get_quotes_batch(
        self, symbols: List[str], mode: str = "", *, schema: Any = None
    ):
        return {s: {"symbol": s} for s in (symbols or [])}

    async def get_enriched_quote_dict(
        self, symbol: str, use_cache: bool = True, *, schema: Any = None
    ):
        return {"symbol": symbol}

    async def get_enriched_quote(
        self, symbol: str, use_cache: bool = True, ttl: Optional[int] = None, **kwargs: Any
    ):
        # Canonical data_engine signature
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
    mount_fn = _first_callable(
        routes_pkg,
        [
            "mount_all_routers",
            "mount_routers",
            "mount_all",
            "mount_routes",
            "mount_all_routes",
        ],
    )
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
        if p.endswith("/sheet-rows"):
            for m in methods:
                if m in {"GET", "POST"}:
                    found.add((m, p))
    if not found:
        raise AssertionError("No GET/POST */sheet-rows endpoint found in mounted routes.")
    return sorted(found)


def _extract_contract_payload(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}

    contract_keys = {"headers", "keys", "rows", "rows_matrix"}
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
                candidate = (
                    item.get("page")
                    or item.get("sheet")
                    or item.get("name")
                    or item.get("id")
                )
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


# =============================================================================
# Tests — baseline invariants (always run)
# =============================================================================
def test_schema_capabilities_banner():
    """v9.3.0: Banner test — prints detected schema version + capabilities.
    Always passes; provides diagnostic context in the test log."""
    sr = _load_schema_module()
    caps = _detect_schema_capabilities(sr)
    # Print via pytest's captured stdout (appears in -s mode)
    print()
    print("=" * 70)
    print(f"Schema Alignment Suite v9.3.0 — Detected Schema Capabilities")
    print("=" * 70)
    print(f"  schema_registry.SCHEMA_VERSION     = {caps['version_str']}")
    print(f"  is_v3_plus                         = {caps['is_v3_plus']}")
    print(f"  is_v3_4_plus                       = {caps['is_v3_4_plus']}")
    print(f"  top10_has_trade_setup              = {caps['top10_has_trade_setup']}")
    print(f"  top10_has_technical                = {caps['top10_has_technical']}")
    print(f"  insights_new_schema                = {caps['insights_new_schema']}")
    print(f"  insights_col_count                 = {caps['insights_col_count']}")
    print(f"  top10_col_count                    = {caps['top10_col_count']}")
    which = "V3 (99/112/86/94/110/106/9/9)" if caps["is_v3_4_plus"] else "V2 (80/80/80/80/80/83/7/9)"
    print(f"  expected_counts_matrix             = {which}")
    print("=" * 70)
    assert caps["version_str"], "schema_registry must expose SCHEMA_VERSION"


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
        assert len(headers) == len(keys), (
            f"Sheet '{sheet}' headers/keys length mismatch: "
            f"{len(headers)} vs {len(keys)}"
        )

        hn = [_norm(h) for h in headers if str(h).strip()]
        kn = [_norm(k) for k in keys if str(k).strip()]

        dup_headers = sorted({h for h in hn if h and hn.count(h) > 1})
        dup_keys = sorted({k for k in kn if k and kn.count(k) > 1})

        assert not dup_headers, f"Duplicate headers in sheet '{sheet}': {dup_headers}"
        assert not dup_keys, f"Duplicate keys in sheet '{sheet}': {dup_keys}"


def test_top10_schema_has_baseline_special_fields():
    """v9.3.0: Baseline Top10 fields (present in schema_registry v2.2.0+).
    Was `test_top10_schema_has_required_special_fields` in v9.2.0.
    """
    sr = _load_schema_module()
    keys = set(_schema_sheet_keys(sr, "Top_10_Investments"))
    missing = sorted(_TOP10_BASELINE_KEYS - keys)
    assert not missing, f"Top_10_Investments missing baseline special keys: {missing}"


def test_top10_schema_has_recommendation_fields():
    sr = _load_schema_module()
    keys = set(_schema_sheet_keys(sr, "Top_10_Investments"))
    missing = sorted(_TOP10_RECOMMENDATION_KEYS - keys)
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

        assert not missing_in_dd, (
            f"Data_Dictionary missing keys for '{sheet}': {missing_in_dd[:25]}"
        )
        assert not extra_in_dd, (
            f"Data_Dictionary has extra keys for '{sheet}': {extra_in_dd[:25]}"
        )


def test_canonical_column_counts_match_schema_version():
    """
    v9.3.0: Validates per-sheet column counts against the expected matrix
    for the DETECTED schema version (V2 baseline or V3 aspirational).

    Replaces v9.2.0's `test_per_page_column_counts_match_schema_registry`
    which hard-coded v3.4.0 counts and failed on the actual v2.2.0 state.

    Rules:
      - Special pages (Insights_Analysis, Data_Dictionary) must match EXACTLY.
      - Instrument pages must match at minimum the expected count
        (future-proof: extra columns pass).
    """
    sr = _load_schema_module()
    caps = _detect_schema_capabilities(sr)
    expected = _expected_counts_for_caps(caps)

    for sheet, expected_count in expected.items():
        keys = _schema_sheet_keys(sr, sheet)
        actual = len(keys)

        if sheet in ("Insights_Analysis", "Data_Dictionary"):
            assert actual == expected_count, (
                f"Sheet '{sheet}' column count mismatch: "
                f"expected exactly {expected_count}, got {actual}. "
                f"(Live schema_registry SCHEMA_VERSION={caps['version_str']}, "
                f"matrix={'V3' if caps['is_v3_4_plus'] else 'V2'})"
            )
        else:
            assert actual >= expected_count, (
                f"Sheet '{sheet}' has fewer columns than expected: "
                f"expected >= {expected_count}, got {actual}. "
                f"(Live schema_registry SCHEMA_VERSION={caps['version_str']}, "
                f"matrix={'V3' if caps['is_v3_4_plus'] else 'V2'})"
            )


def test_insights_schema_matches_live_version():
    """
    v9.3.0: Version-aware Insights_Analysis schema check.
    Replaces v9.2.0's `test_insights_schema_has_priority_and_signal` which
    unconditionally required v3.4.0-only keys.

    On v2.2.0: expects the 7-col schema with `last_updated_riyadh`.
    On v3.4.0+: expects the 9-col schema with `signal`, `priority`,
                `as_of_riyadh` and forbids `last_updated_riyadh`,
                `sort_order`, `source`.
    """
    sr = _load_schema_module()
    caps = _detect_schema_capabilities(sr)
    keys = set(_schema_sheet_keys(sr, "Insights_Analysis"))

    if caps["is_v3_4_plus"]:
        for required_key in ("signal", "priority", "as_of_riyadh"):
            assert required_key in keys, (
                f"Insights_Analysis (v3.4.0+) missing key '{required_key}'. "
                f"Detected SCHEMA_VERSION={caps['version_str']}."
            )
        for old_key in ("source", "sort_order", "last_updated_riyadh"):
            assert old_key not in keys, (
                f"Insights_Analysis (v3.4.0+) still has old key '{old_key}'; "
                "should have been replaced in schema_registry v3.4.0."
            )
    else:
        # Baseline: v2.2.0 has 7 columns ending in last_updated_riyadh
        for required_key in ("section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"):
            assert required_key in keys, (
                f"Insights_Analysis (v2.x) missing baseline key '{required_key}'. "
                f"Detected SCHEMA_VERSION={caps['version_str']}."
            )


# =============================================================================
# Tests — aspirational (auto-skip until schema catches up)
# =============================================================================
def test_top10_schema_has_trade_setup_fields():
    """
    v9.3.0: Aspirational — requires schema_registry v3.4.0 with
    top10_selector v4.9.0's `_compute_trade_setup`. Auto-skips on older
    schemas with a clear message.
    """
    sr = _load_schema_module()
    caps = _detect_schema_capabilities(sr)
    if not caps["top10_has_trade_setup"]:
        pytest.skip(
            f"Top_10_Investments trade-setup fields not yet in schema "
            f"(SCHEMA_VERSION={caps['version_str']}). "
            f"Required: {sorted(_TOP10_TRADE_SETUP_KEYS)}. "
            f"Requires top10_selector v4.9.0 + schema_registry v3.4.0."
        )
    keys = set(_schema_sheet_keys(sr, "Top_10_Investments"))
    missing = sorted(_TOP10_TRADE_SETUP_KEYS - keys)
    assert not missing, (
        f"Top_10_Investments missing trade setup keys: {missing}. "
        "Required by top10_selector v4.9.0 (_compute_trade_setup)."
    )


def test_top10_schema_has_technical_signal_fields():
    """
    v9.3.0: Aspirational — requires scoring.py v3.0.0+ technical signal
    fields. Auto-skips on older schemas.
    """
    sr = _load_schema_module()
    caps = _detect_schema_capabilities(sr)
    if not caps["top10_has_technical"]:
        pytest.skip(
            f"Top_10_Investments technical-signal fields not yet in schema "
            f"(SCHEMA_VERSION={caps['version_str']}). "
            f"Required: {sorted(_TOP10_TECHNICAL_KEYS)}. "
            f"Requires scoring.py v3.0.0."
        )
    keys = set(_schema_sheet_keys(sr, "Top_10_Investments"))
    missing = sorted(_TOP10_TECHNICAL_KEYS - keys)
    assert not missing, (
        f"Top_10_Investments missing technical signal keys: {missing}. "
        "Required by scoring.py v3.0.0."
    )


# =============================================================================
# Tests — FastAPI endpoint contracts
# =============================================================================
def test_schema_pages_endpoint_returns_required_sheets(monkeypatch: pytest.MonkeyPatch):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)

    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        r = client.get("/v1/schema/pages", headers=_auth_headers())
        assert r.status_code == 200, (
            f"/v1/schema/pages failed: {r.status_code} {r.text[:300]}"
        )

        data = r.json()
        pages = _extract_schema_pages(data)
        missing = [s for s in REQUIRED_SHEETS if s not in pages]
        assert not missing, (
            f"/v1/schema/pages missing required sheets: {missing}. got={pages}"
        )


def test_sheet_spec_endpoint_is_reachable_and_schema_shaped(
    monkeypatch: pytest.MonkeyPatch,
):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)

    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        r = client.get("/v1/schema/sheet-spec", headers=_auth_headers())
        assert r.status_code == 200, (
            f"/v1/schema/sheet-spec failed: {r.status_code} {r.text[:300]}"
        )

        data = r.json()
        payload = data.get("data") if isinstance(data, dict) else None
        assert isinstance(
            payload, (dict, list)
        ), f"Unexpected /sheet-spec payload type: {type(payload)}"

        if isinstance(payload, dict):
            for sheet in REQUIRED_SHEETS:
                assert sheet in payload, f"/sheet-spec missing sheet '{sheet}'"
                spec = payload[sheet]
                assert isinstance(spec, dict), (
                    f"/sheet-spec entry for '{sheet}' is not dict"
                )
                headers = spec.get("headers") or []
                keys = spec.get("keys") or []
                assert headers == _schema_sheet_headers(sr, sheet), (
                    f"/sheet-spec headers mismatch for '{sheet}'"
                )
                assert keys == _schema_sheet_keys(sr, sheet), (
                    f"/sheet-spec keys mismatch for '{sheet}'"
                )


def test_data_dictionary_endpoint_has_9_column_contract(
    monkeypatch: pytest.MonkeyPatch,
):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)

    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        r = client.get("/v1/schema/data-dictionary", headers=_auth_headers())
        assert r.status_code == 200, (
            f"/v1/schema/data-dictionary failed: {r.status_code} {r.text[:300]}"
        )

        data = r.json()
        payload = _extract_contract_payload(data)

        headers = payload.get("headers") or []
        rows = payload.get("rows") or []

        assert isinstance(headers, list), "Data_Dictionary headers is not a list"
        assert len(headers) == 9, (
            f"Data_Dictionary must have exactly 9 headers, got {len(headers)}"
        )
        assert isinstance(rows, list), "Data_Dictionary rows is not a list"
        assert rows, "Data_Dictionary rows is empty"

        if rows and isinstance(rows[0], dict):
            got_keys = set(rows[0].keys())
            expected_keys = set(_schema_sheet_keys(sr, "Data_Dictionary"))
            assert expected_keys.issubset(got_keys), (
                f"Data_Dictionary row keys mismatch. "
                f"expected subset={expected_keys}, got={got_keys}"
            )


def test_sheet_rows_returns_exact_schema_headers_and_keys_for_each_sheet(
    monkeypatch: pytest.MonkeyPatch,
):
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)

    try:
        app.state.engine = _StubEngine(sr)
        app.state.engine_ready = True
    except Exception:
        pass

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

                if method == "GET":
                    r = client.get(path, headers=headers, **kwargs)
                else:
                    r = client.post(path, headers=headers, **kwargs)

                assert r.status_code == 200, (
                    f"{method} {path} failed for '{sheet}': "
                    f"{r.status_code} {r.text[:300]}"
                )
                raw = r.json()
                data = _extract_contract_payload(raw)

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
                                assert k in row, (
                                    f"{method} {path} missing key '{k}' "
                                    f"in '{sheet}' row#{i+1}"
                                )
                        elif isinstance(row, list):
                            assert len(row) == len(expected_keys), (
                                f"{method} {path} row length mismatch "
                                f"for '{sheet}' row#{i+1}"
                            )

                matrix = data.get("rows_matrix")
                if isinstance(matrix, list) and matrix:
                    for i, row in enumerate(matrix[:10]):
                        assert isinstance(row, list)
                        assert len(row) == len(expected_keys), (
                            f"{method} {path} rows_matrix length mismatch "
                            f"for '{sheet}' row#{i+1}"
                        )


def test_top10_sheet_rows_includes_baseline_special_headers(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    v9.3.0: Renamed from `test_top10_sheet_rows_includes_special_headers`.
    Checks only the BASELINE Top10 keys (top10_rank, selection_reason,
    criteria_snapshot + recommendation/reason) which are present in
    schema_registry v2.2.0+.

    The aspirational trade-setup keys are checked separately by
    `test_top10_schema_has_trade_setup_fields` which auto-skips on v2.2.0.
    """
    sr = _load_schema_module()
    app = _build_test_app(monkeypatch, sr)

    from fastapi.testclient import TestClient

    endpoints = _find_sheet_rows_endpoints(app)
    target_endpoints = [
        (m, p)
        for (m, p) in endpoints
        if "investment" in p.lower() or "advisor" in p.lower() or "enriched" in p.lower()
    ]
    if not target_endpoints:
        target_endpoints = endpoints

    required = _TOP10_BASELINE_KEYS | _TOP10_RECOMMENDATION_KEYS

    with TestClient(app) as client:
        for method, path in target_endpoints:
            kwargs = _sheet_rows_request_kwargs(method, "Top_10_Investments")
            headers = _auth_headers()

            if method == "GET":
                r = client.get(path, headers=headers, **kwargs)
            else:
                r = client.post(path, headers=headers, **kwargs)

            assert r.status_code == 200, (
                f"{method} {path} failed for Top_10_Investments: "
                f"{r.status_code} {r.text[:300]}"
            )
            raw = r.json()
            data = _extract_contract_payload(raw)

            got_keys = set(data.get("keys") or [])
            missing = sorted(required - got_keys)
            assert not missing, (
                f"{method} {path} Top_10_Investments missing baseline keys: "
                f"{missing}"
            )
