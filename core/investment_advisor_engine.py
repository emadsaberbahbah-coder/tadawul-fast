# core/investment_advisor_engine.py
"""
Tadawul Fast Bridge — Investment Advisor Engine (Google Sheets Friendly)
File: core/investment_advisor_engine.py

FULL REPLACEMENT — v1.3.0 (LINKED TO core/investment_advisor.py + SNAPSHOT ADAPTER + GAS-SAFE)

Purpose
- Provides a stable "engine layer" entry point for Investment Advisor routes.
- Delegates ALL scoring/allocation logic to core/investment_advisor.py (single source of truth).
- Adds a Snapshot Adapter so older engines (without get_cached_sheet_snapshot) still work.

Key Benefits (v1.3.0)
- ✅ Linked: Uses core.investment_advisor.run_investment_advisor (same output, same scoring).
- ✅ Snapshot Adapter: Works with engines exposing:
    - get_cached_sheet_snapshot / get_cached_multi_sheet_snapshots (preferred)
    - OR legacy methods: get_cached_sheet_rows, get_sheet_rows_cached, get_sheet_rows, get_sheet, sheet_rows...
- ✅ GAS Safe: headers ALWAYS present; rows ALWAYS list.
- ✅ Better Diagnostics: meta includes adapter_mode, method_used, snapshot coverage.

Public contract:
  run_investment_advisor(payload_dict, engine=...) -> {"headers":[...], "rows":[...], "items":[...], "meta":{...}}
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import time

ENGINE_VERSION = "1.3.0"

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

# Keep stable schema aligned with core/investment_advisor.py (v1.5.0)
DEFAULT_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Market",
    "Currency",
    "Price",
    "Advisor Score",
    "Action",
    "Allocation %",
    "Allocation Amount",
    "Forecast Price (1M)",
    "Expected ROI % (1M)",
    "Expected Gain/Loss (1M)",
    "Forecast Price (3M)",
    "Expected ROI % (3M)",
    "Expected Gain/Loss (3M)",
    "Forecast Price (12M)",
    "Expected ROI % (12M)",
    "Expected Gain/Loss (12M)",
    "Risk Bucket",
    "Confidence Bucket",
    "Reason (Explain)",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_dict(x: Any) -> bool:
    return isinstance(x, dict)


def _is_list(x: Any) -> bool:
    return isinstance(x, list)


def _safe_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _engine_call_sheet_method(engine: Any, sheet_name: str) -> Tuple[List[Any], List[Any], Optional[str]]:
    """
    Tries multiple method names safely.
    Returns (headers, rows, method_used).
    """
    if engine is None:
        return [], [], None

    # Preferred snapshot methods
    preferred = [
        "get_cached_sheet_snapshot",
        "get_sheet_snapshot",
    ]

    # Legacy methods (common in older builds)
    legacy = [
        "get_cached_sheet_rows",
        "get_sheet_rows_cached",
        "get_sheet_cached",
        "get_sheet_rows",
        "get_sheet",
        "sheet_rows",
    ]

    for fn_name in preferred + legacy:
        fn = getattr(engine, fn_name, None)
        if not callable(fn):
            continue
        try:
            resp = fn(sheet_name)  # type: ignore[misc]
            # snapshot dict
            if _is_dict(resp):
                headers = resp.get("headers") or []
                rows = resp.get("rows") or []
                return _safe_list(headers), _safe_list(rows), fn_name
            # tuple/list (headers, rows)
            if isinstance(resp, (list, tuple)) and len(resp) == 2:
                return _safe_list(resp[0]), _safe_list(resp[1]), fn_name
        except Exception:
            continue

    return [], [], None


def _engine_call_multi_snapshot(engine: Any, sheet_names: List[str]) -> Tuple[Dict[str, Dict[str, Any]], Optional[str]]:
    """
    Preferred: get_cached_multi_sheet_snapshots(list[str]) -> dict[sheet] = snapshot
    """
    if engine is None:
        return {}, None
    fn = getattr(engine, "get_cached_multi_sheet_snapshots", None)
    if callable(fn):
        try:
            out = fn(sheet_names)  # type: ignore[misc]
            if isinstance(out, dict):
                cleaned: Dict[str, Dict[str, Any]] = {}
                for k, v in out.items():
                    if isinstance(v, dict):
                        cleaned[str(k)] = v
                return cleaned, "get_cached_multi_sheet_snapshots"
        except Exception:
            return {}, "get_cached_multi_sheet_snapshots"
    return {}, None


# -----------------------------------------------------------------------------
# Snapshot Adapter (makes old engines compatible with core/investment_advisor.py)
# -----------------------------------------------------------------------------
class EngineSnapshotAdapter:
    """
    Adapts an engine that may not implement get_cached_sheet_snapshot()
    into an object that DOES, so core/investment_advisor.py works.

    This adapter:
    - implements get_cached_sheet_snapshot(sheet)
    - implements get_cached_multi_sheet_snapshots(sheets) if possible
    - passes-through optional news hooks if present
    """

    def __init__(self, base_engine: Any):
        self._base = base_engine
        self._adapter_cached_at = _utc_iso()

    def __repr__(self) -> str:
        return f"<EngineSnapshotAdapter base={type(self._base).__name__}>"

    # Optional pass-throughs if your system uses them elsewhere
    def set_cached_sheet_snapshot(self, sheet_name: str, headers: List[Any], rows: List[Any], meta: Dict[str, Any]) -> None:
        fn = getattr(self._base, "set_cached_sheet_snapshot", None)
        if callable(fn):
            try:
                fn(sheet_name, headers, rows, meta)  # type: ignore[misc]
            except Exception:
                pass

    def get_cached_sheet_snapshot(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        # If base already supports snapshots, use it directly
        direct = getattr(self._base, "get_cached_sheet_snapshot", None)
        if callable(direct):
            try:
                snap = direct(sheet_name)  # type: ignore[misc]
                return snap if isinstance(snap, dict) else None
            except Exception:
                return None

        # Otherwise fallback to legacy fetch
        headers, rows, method_used = _engine_call_sheet_method(self._base, sheet_name)
        if not headers and not rows:
            return None

        return {
            "headers": headers,
            "rows": rows,
            "meta": {"adapter_mode": "legacy_fetch", "method_used": method_used},
            "cached_at_utc": self._adapter_cached_at,
        }

    def get_cached_multi_sheet_snapshots(self, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
        # If base supports multi snapshot, use it
        multi, method_used = _engine_call_multi_snapshot(self._base, sheet_names)
        if multi:
            return multi

        # Else build manually via single snapshot fallback
        out: Dict[str, Dict[str, Any]] = {}
        for s in sheet_names or []:
            snap = self.get_cached_sheet_snapshot(s)
            if isinstance(snap, dict):
                # keep method_used trace
                if method_used and isinstance(snap.get("meta"), dict):
                    snap["meta"]["multi_method_used"] = method_used
                out[str(s)] = snap
        return out

    # Optional news hooks (core/investment_advisor.py will try these names)
    def get_news_score(self, symbol: str) -> Any:
        fn = getattr(self._base, "get_news_score", None)
        if callable(fn):
            return fn(symbol)  # type: ignore[misc]
        raise AttributeError("Base engine has no get_news_score")

    def get_cached_news_score(self, symbol: str) -> Any:
        fn = getattr(self._base, "get_cached_news_score", None)
        if callable(fn):
            return fn(symbol)  # type: ignore[misc]
        raise AttributeError("Base engine has no get_cached_news_score")

    def news_get_score(self, symbol: str) -> Any:
        fn = getattr(self._base, "news_get_score", None)
        if callable(fn):
            return fn(symbol)  # type: ignore[misc]
        raise AttributeError("Base engine has no news_get_score")


# -----------------------------------------------------------------------------
# Public Entry Point
# -----------------------------------------------------------------------------
def run_investment_advisor(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    """
    Main Engine entry:
    - wraps provided engine with SnapshotAdapter (if needed)
    - delegates to core/investment_advisor.run_investment_advisor

    Returns GAS-safe structure:
      {"headers":[...], "rows":[...], "items":[...], "meta":{...}}
    """
    t0 = time.time()

    # Always keep GAS-safe baseline
    safe_out: Dict[str, Any] = {
        "headers": list(DEFAULT_HEADERS),
        "rows": [],
        "items": [],
        "meta": {
            "ok": False,
            "engine_version": ENGINE_VERSION,
            "error": "Not executed",
            "runtime_ms": 0,
        },
    }

    try:
        # Import the single source of truth (your upgraded core module)
        from .investment_advisor import run_investment_advisor as core_run
        from .investment_advisor import TT_ADVISOR_CORE_VERSION as CORE_VERSION

        # Normalize engine object for snapshot access
        adapter_mode = "none"
        eng_obj = engine
        if engine is not None:
            # If it doesn't have get_cached_sheet_snapshot, adapt it
            if not callable(getattr(engine, "get_cached_sheet_snapshot", None)):
                eng_obj = EngineSnapshotAdapter(engine)
                adapter_mode = "wrapped_legacy_engine"
            else:
                adapter_mode = "native_snapshot_engine"

        # Run core advisor (does scoring + allocation + dedupe + schema)
        result = core_run(payload or {}, engine=eng_obj)

        # GAS safety enforcement (headers always present)
        headers = result.get("headers")
        rows = result.get("rows")
        items = result.get("items")

        if not isinstance(headers, list) or not headers:
            headers = list(DEFAULT_HEADERS)
        if not isinstance(rows, list):
            rows = []
        if not isinstance(items, list):
            items = []

        meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
        meta = dict(meta)
        meta.setdefault("ok", True)
        meta["engine_version"] = ENGINE_VERSION
        meta["core_version"] = meta.get("core_version") or CORE_VERSION
        meta["adapter_mode"] = adapter_mode
        meta["runtime_ms_engine_layer"] = int((time.time() - t0) * 1000)

        return {"headers": headers, "rows": rows, "items": items, "meta": meta}

    except Exception as exc:
        safe_out["meta"] = {
            "ok": False,
            "engine_version": ENGINE_VERSION,
            "error": str(exc),
            "runtime_ms": int((time.time() - t0) * 1000),
        }
        return safe_out


# Compatibility alias (if any older routes import this name)
def run_investment_advisor_engine(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    return run_investment_advisor(payload, engine=engine)


__all__ = [
    "run_investment_advisor",
    "run_investment_advisor_engine",
    "ENGINE_VERSION",
    "DEFAULT_HEADERS",
]
