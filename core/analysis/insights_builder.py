#!/usr/bin/env python3
# core/analysis/insights_builder.py
"""
================================================================================
Insights Analysis Builder — v1.0.0 (SCHEMA-FIRST / 7-COL OUTPUT / STARTUP-SAFE)
================================================================================
Tadawul Fast Bridge (TFB)

Purpose
- Build rows for the "Insights_Analysis" sheet using schema_registry as the
  single source of truth for headers/keys.
- Always outputs the 7-column Insights_Analysis layout (no 80-col fallback).

Design rules
- ✅ Import-safe: NO network calls at import time
- ✅ Schema-first: keys/order come from core/sheets/schema_registry.py
- ✅ Criteria embedded: Advisor_Criteria is represented as rows in section="Criteria"
- ✅ Best-effort engine integration: if engine is provided, optionally compute
  simple insight summaries for provided symbol universes.

Expected Insights_Analysis columns (schema_registry keys)
- section
- item
- symbol
- metric
- value
- notes
- last_updated_riyadh

Public API
- build_insights_analysis_rows(...)
- build_criteria_rows(...)
- get_insights_schema()
================================================================================
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger("core.analysis.insights_builder")
logger.addHandler(logging.NullHandler())

INSIGHTS_BUILDER_VERSION = "1.0.0"

_RIYADH_TZ = timezone(timedelta(hours=3))


# -----------------------------------------------------------------------------
# Schema helpers (schema_registry is authoritative)
# -----------------------------------------------------------------------------
def _safe_str(v: Any) -> str:
    try:
        return str(v).strip()
    except Exception:
        return ""


def _now_riyadh_iso() -> str:
    return datetime.now(_RIYADH_TZ).isoformat()


def get_insights_schema() -> Tuple[List[str], List[str], str]:
    """
    Returns (headers, keys, source_marker).
    If schema_registry is unavailable, returns a safe 7-key fallback.
    """
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Insights_Analysis")
        cols = getattr(spec, "columns", None) or []
        headers = [str(getattr(c, "header")) for c in cols if getattr(c, "header", None)]
        keys = [str(getattr(c, "key")) for c in cols if getattr(c, "key", None)]
        if headers and keys and len(headers) == len(keys):
            return headers, keys, "schema_registry.get_sheet_spec"
    except Exception as e:
        logger.debug("get_insights_schema: schema_registry unavailable: %r", e)

    # fallback 7-col contract (matches schema_registry intent)
    keys = ["section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"]
    headers = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
    return headers, keys, "fallback"


def _get_criteria_fields() -> List[Dict[str, Any]]:
    """
    Reads criteria_fields from schema_registry for Insights_Analysis (best-effort).
    Returns list of dicts: {key,label,dtype,default,notes}
    """
    try:
        from core.sheets.schema_registry import get_sheet_spec  # type: ignore

        spec = get_sheet_spec("Insights_Analysis")
        cfs = getattr(spec, "criteria_fields", None) or ()
        out: List[Dict[str, Any]] = []
        for cf in cfs:
            out.append(
                {
                    "key": _safe_str(getattr(cf, "key", "")),
                    "label": _safe_str(getattr(cf, "label", "")) or _safe_str(getattr(cf, "key", "")),
                    "dtype": _safe_str(getattr(cf, "dtype", "str")) or "str",
                    "default": getattr(cf, "default", ""),
                    "notes": _safe_str(getattr(cf, "notes", "")),
                }
            )
        return [x for x in out if x.get("key")]
    except Exception:
        # safe minimal defaults (aligned with your schema_registry example)
        return [
            {"key": "risk_level", "label": "Risk Level", "dtype": "str", "default": "Moderate", "notes": "Low / Moderate / High."},
            {"key": "confidence_level", "label": "Confidence Level", "dtype": "str", "default": "High", "notes": "High / Medium / Low."},
            {"key": "invest_period_days", "label": "Investment Period (Days)", "dtype": "int", "default": "90", "notes": "Always treated in DAYS internally."},
            {"key": "required_return_pct", "label": "Required Return %", "dtype": "pct", "default": "0.10", "notes": "Minimum expected ROI threshold."},
            {"key": "amount", "label": "Amount", "dtype": "float", "default": "0", "notes": "Investment amount (optional)."},
        ]


# -----------------------------------------------------------------------------
# Row builder
# -----------------------------------------------------------------------------
def _make_row(
    *,
    keys: Sequence[str],
    section: str,
    item: str,
    metric: str,
    value: Any,
    symbol: str = "",
    notes: str = "",
    last_updated_riyadh: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a single Insights_Analysis row dict, strictly keyed by schema keys.
    Unknown keys are filled with None.
    """
    ts = last_updated_riyadh or _now_riyadh_iso()

    base: Dict[str, Any] = {
        "section": section,
        "item": item,
        "symbol": symbol or "",
        "metric": metric,
        "value": value if value is not None else "",
        "notes": notes or "",
        "last_updated_riyadh": ts,
    }

    # project to exact key order
    out: Dict[str, Any] = {}
    for k in keys:
        out[k] = base.get(k, None)
    return out


def build_criteria_rows(
    *,
    criteria: Optional[Dict[str, Any]] = None,
    last_updated_riyadh: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Represent Advisor Criteria as rows within Insights_Analysis (section="Criteria").
    This avoids needing a separate removed sheet (Advisor_Criteria).
    """
    _, keys, _ = get_insights_schema()
    ts = last_updated_riyadh or _now_riyadh_iso()

    fields = _get_criteria_fields()
    crit = dict(criteria or {})

    rows: List[Dict[str, Any]] = []
    for f in fields:
        k = f["key"]
        label = f["label"]
        v = crit.get(k, f.get("default", ""))
        notes = f.get("notes", "")
        rows.append(
            _make_row(
                keys=keys,
                section="Criteria",
                item=label,
                symbol="",
                metric=k,
                value=v,
                notes=notes,
                last_updated_riyadh=ts,
            )
        )
    return rows


# -----------------------------------------------------------------------------
# Engine integration (best-effort)
# -----------------------------------------------------------------------------
async def _maybe_await(v: Any) -> Any:
    try:
        if hasattr(v, "__await__"):
            return await v  # type: ignore[misc]
    except Exception:
        pass
    return v


async def _fetch_quotes_map(engine: Any, symbols: List[str], *, mode: str = "") -> Dict[str, Dict[str, Any]]:
    """
    Best-effort: returns {symbol: rowdict}.
    Never raises; returns empty on failure.
    """
    if not engine or not symbols:
        return {}

    # Prefer DataEngineV5 method
    fn = getattr(engine, "get_enriched_quotes_batch", None)
    if callable(fn):
        try:
            res = fn(symbols, mode=mode or "")
            res = await _maybe_await(res)
            if isinstance(res, dict):
                # ensure dict values are dict-like
                out: Dict[str, Dict[str, Any]] = {}
                for s in symbols:
                    v = res.get(s)
                    if isinstance(v, dict):
                        out[s] = v
                    else:
                        # best-effort coerce
                        try:
                            if hasattr(v, "model_dump"):
                                out[s] = v.model_dump(mode="python")  # type: ignore
                            elif hasattr(v, "dict"):
                                out[s] = v.dict()  # type: ignore
                            else:
                                out[s] = {"symbol": s, "value": v}
                        except Exception:
                            out[s] = {"symbol": s, "error": "unserializable"}
                return out
        except Exception:
            pass

    # Fallback list batch
    fn2 = getattr(engine, "get_enriched_quotes", None)
    if callable(fn2):
        try:
            res = fn2(symbols)
            res = await _maybe_await(res)
            if isinstance(res, list):
                out2: Dict[str, Dict[str, Any]] = {}
                for s, v in zip(symbols, res):
                    if isinstance(v, dict):
                        out2[s] = v
                    else:
                        try:
                            if hasattr(v, "model_dump"):
                                out2[s] = v.model_dump(mode="python")  # type: ignore
                            elif hasattr(v, "dict"):
                                out2[s] = v.dict()  # type: ignore
                            else:
                                out2[s] = {"symbol": s, "value": v}
                        except Exception:
                            out2[s] = {"symbol": s, "error": "unserializable"}
                return out2
        except Exception:
            pass

    # Per-symbol fallback
    out3: Dict[str, Dict[str, Any]] = {}
    fn3 = getattr(engine, "get_enriched_quote_dict", None)
    fn4 = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    for s in symbols:
        try:
            if callable(fn3):
                v = fn3(s)
                v = await _maybe_await(v)
                out3[s] = v if isinstance(v, dict) else {"symbol": s, "value": v}
            elif callable(fn4):
                v = fn4(s)
                v = await _maybe_await(v)
                if isinstance(v, dict):
                    out3[s] = v
                else:
                    try:
                        if hasattr(v, "model_dump"):
                            out3[s] = v.model_dump(mode="python")  # type: ignore
                        elif hasattr(v, "dict"):
                            out3[s] = v.dict()  # type: ignore
                        else:
                            out3[s] = {"symbol": s, "value": v}
                    except Exception:
                        out3[s] = {"symbol": s, "error": "unserializable"}
            else:
                out3[s] = {"symbol": s, "error": "engine_missing_quote_methods"}
        except Exception as e:
            out3[s] = {"symbol": s, "error": str(e)}
    return out3


def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        if x != x:  # NaN
            return None
        return x
    except Exception:
        return None


def _fmt_pct(v: Any) -> str:
    x = _as_float(v)
    if x is None:
        return ""
    return f"{x:.2f}%"


def _fmt_num(v: Any) -> str:
    x = _as_float(v)
    if x is None:
        return _safe_str(v)
    return f"{x:.2f}"


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
async def build_insights_analysis_rows(
    *,
    engine: Optional[Any] = None,
    criteria: Optional[Dict[str, Any]] = None,
    universes: Optional[Dict[str, Sequence[str]]] = None,
    mode: str = "",
    include_criteria_rows: bool = True,
    include_system_rows: bool = True,
    max_symbols_per_universe: int = 25,
) -> Dict[str, Any]:
    """
    Build Insights_Analysis payload (headers/keys/rows) aligned to schema_registry.

    universes (optional) example:
      {
        "Market Leaders": ["2222.SR","2030.SR",...],
        "Global": ["AAPL","MSFT",...]
      }

    Returns dict envelope:
      {
        "status": "success",
        "page": "Insights_Analysis",
        "headers": [...],
        "keys": [...],
        "rows": [...],
        "meta": {...}
      }
    """
    headers, keys, schema_source = get_insights_schema()
    ts = _now_riyadh_iso()

    rows: List[Dict[str, Any]] = []

    # Criteria rows (represents Advisor_Criteria inside Insights_Analysis)
    if include_criteria_rows:
        rows.extend(build_criteria_rows(criteria=criteria, last_updated_riyadh=ts))

    # System rows (safe, zero network)
    if include_system_rows:
        # schema version (best-effort)
        schema_version = ""
        try:
            from core.sheets.schema_registry import SCHEMA_VERSION  # type: ignore

            schema_version = str(SCHEMA_VERSION)
        except Exception:
            schema_version = ""

        rows.append(
            _make_row(
                keys=keys,
                section="System",
                item="Builder Version",
                symbol="",
                metric="insights_builder_version",
                value=INSIGHTS_BUILDER_VERSION,
                notes="core/analysis/insights_builder.py",
                last_updated_riyadh=ts,
            )
        )
        if schema_version:
            rows.append(
                _make_row(
                    keys=keys,
                    section="System",
                    item="Schema Version",
                    symbol="",
                    metric="schema_version",
                    value=schema_version,
                    notes="core/sheets/schema_registry.py",
                    last_updated_riyadh=ts,
                )
            )

    # If no engine or no universes, return what we have (still schema-correct)
    universes = universes or {}
    if not engine or not universes:
        return {
            "status": "success",
            "page": "Insights_Analysis",
            "headers": headers,
            "keys": keys,
            "rows": rows,
            "meta": {
                "schema_source": schema_source,
                "generated_at_riyadh": ts,
                "engine_used": bool(engine),
                "universes": list(universes.keys()),
            },
        }

    # For each universe section, compute simple snapshots (best-effort)
    for section_name, sym_list in universes.items():
        syms = [_safe_str(s) for s in (sym_list or []) if _safe_str(s)]
        if not syms:
            continue
        syms = syms[: max(1, min(int(max_symbols_per_universe), 500))]

        qmap = await _fetch_quotes_map(engine, syms, mode=mode or "")
        # collect percent_change
        scored: List[Tuple[str, float]] = []
        for s in syms:
            d = qmap.get(s) or {}
            pc = _as_float(d.get("percent_change") if isinstance(d, dict) else None)
            if pc is not None:
                scored.append((s, pc))

        rows.append(
            _make_row(
                keys=keys,
                section=section_name,
                item="Universe Size",
                symbol="",
                metric="count",
                value=len(syms),
                notes="Requested symbols in this universe",
                last_updated_riyadh=ts,
            )
        )

        if not scored:
            rows.append(
                _make_row(
                    keys=keys,
                    section=section_name,
                    item="Snapshot",
                    symbol="",
                    metric="status",
                    value="No percent_change available",
                    notes="Provider may not have returned percent_change yet.",
                    last_updated_riyadh=ts,
                )
            )
            continue

        scored.sort(key=lambda t: t[1], reverse=True)
        top_sym, top_pct = scored[0]
        low_sym, low_pct = scored[-1]
        avg_pct = sum(x[1] for x in scored) / max(1, len(scored))

        rows.append(
            _make_row(
                keys=keys,
                section=section_name,
                item="Top Gainer",
                symbol=top_sym,
                metric="percent_change",
                value=_fmt_pct(top_pct),
                notes="Highest percent_change among provided symbols",
                last_updated_riyadh=ts,
            )
        )
        rows.append(
            _make_row(
                keys=keys,
                section=section_name,
                item="Top Loser",
                symbol=low_sym,
                metric="percent_change",
                value=_fmt_pct(low_pct),
                notes="Lowest percent_change among provided symbols",
                last_updated_riyadh=ts,
            )
        )
        rows.append(
            _make_row(
                keys=keys,
                section=section_name,
                item="Average Change",
                symbol="",
                metric="avg_percent_change",
                value=_fmt_pct(avg_pct),
                notes="Average percent_change across symbols with data",
                last_updated_riyadh=ts,
            )
        )

    return {
        "status": "success",
        "page": "Insights_Analysis",
        "headers": headers,
        "keys": keys,
        "rows": rows,
        "meta": {
            "schema_source": schema_source,
            "generated_at_riyadh": ts,
            "engine_used": True,
            "universes": list(universes.keys()),
            "mode": mode,
        },
    }


__all__ = [
    "INSIGHTS_BUILDER_VERSION",
    "get_insights_schema",
    "build_criteria_rows",
    "build_insights_analysis_rows",
]
