#!/usr/bin/env python3
# core/analysis/insights_builder.py
"""
================================================================================
Insights Analysis Builder — v1.1.0 (REAL ROWS / AUTO-UNIVERSE / 7-COL OUTPUT)
================================================================================
Tadawul Fast Bridge (TFB)

Fix objective (Phase B / Script #2):
- ✅ Insights_Analysis must NOT be empty/useless.
- ✅ When symbols/universes are empty, auto-build a meaningful “dashboard universe”
  (indices + commodities/FX + Top10 + portfolio KPIs best-effort).
- ✅ Populate section/item/metric/value/notes consistently (no null shells).
- ✅ Always output the 7-column Insights_Analysis layout (no 80-col fallback).

Design rules
- ✅ Import-safe: NO network calls at import time
- ✅ Schema-first: keys/order come from core/sheets/schema_registry.py
- ✅ Best-effort engine integration: never raises; returns meaningful rows even
  if engine is missing or providers return partial data.

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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger("core.analysis.insights_builder")
logger.addHandler(logging.NullHandler())

INSIGHTS_BUILDER_VERSION = "1.1.0"

_RIYADH_TZ = timezone(timedelta(hours=3))


# -----------------------------------------------------------------------------
# Small utils
# -----------------------------------------------------------------------------
def _safe_str(v: Any) -> str:
    try:
        s = str(v)
    except Exception:
        return ""
    return s.strip()


def _now_riyadh_iso() -> str:
    return datetime.now(_RIYADH_TZ).isoformat()


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


def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(float(v))
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


def _fmt_int(v: Any) -> str:
    x = _as_int(v)
    if x is None:
        return _safe_str(v)
    return str(x)


def _csv_list(raw: str) -> List[str]:
    items: List[str] = []
    for part in (raw or "").replace("\n", ",").split(","):
        s = part.strip()
        if s:
            items.append(s)
    # de-dupe preserve order
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _env_csv(name: str, default_csv: str) -> List[str]:
    return _csv_list(os.getenv(name, default_csv))


# -----------------------------------------------------------------------------
# Schema helpers (schema_registry is authoritative)
# -----------------------------------------------------------------------------
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
# Row builder (ALWAYS fills section/item/metric/value/notes)
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
    ts = last_updated_riyadh or _now_riyadh_iso()

    # enforce non-empty shell fields
    sec = _safe_str(section) or "General"
    it = _safe_str(item) or "Item"
    met = _safe_str(metric) or "metric"
    sym = _safe_str(symbol)

    # values MUST be Apps Script friendly (string-safe)
    if value is None:
        val = ""
    elif isinstance(value, (int, float)):
        val = str(value)
    else:
        val = _safe_str(value)

    base: Dict[str, Any] = {
        "section": sec,
        "item": it,
        "symbol": sym,
        "metric": met,
        "value": val,
        "notes": _safe_str(notes),
        "last_updated_riyadh": ts,
    }

    out: Dict[str, Any] = {}
    for k in keys:
        out[k] = base.get(k, "")
    return out


def build_criteria_rows(
    *,
    criteria: Optional[Dict[str, Any]] = None,
    last_updated_riyadh: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Criteria as rows under section="Criteria" (keeps sheet self-explanatory).
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

        # format common fields (optional)
        if k.endswith("_pct") or k.endswith("_percent") or "return" in k:
            if isinstance(v, (int, float)):
                v = f"{float(v) * 100:.2f}%" if float(v) <= 1.5 else f"{float(v):.2f}%"
        rows.append(
            _make_row(
                keys=keys,
                section="Criteria",
                item=label,
                symbol="",
                metric=k,
                value=v,
                notes=f.get("notes", ""),
                last_updated_riyadh=ts,
            )
        )
    return rows


# -----------------------------------------------------------------------------
# Engine integration (best-effort, never raises)
# -----------------------------------------------------------------------------
async def _maybe_await(v: Any) -> Any:
    try:
        if hasattr(v, "__await__"):
            return await v  # type: ignore[misc]
    except Exception:
        pass
    return v


def _extract_row_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    try:
        if hasattr(v, "model_dump"):
            return v.model_dump(mode="python")  # type: ignore
        if hasattr(v, "dict"):
            return v.dict()  # type: ignore
    except Exception:
        pass
    return {}


async def _fetch_quotes_map(engine: Any, symbols: List[str], *, mode: str = "") -> Dict[str, Dict[str, Any]]:
    """
    Best-effort: returns {symbol: rowdict}. Never raises; returns {} on failure.
    """
    if not engine or not symbols:
        return {}

    # Preferred batch method
    fn = getattr(engine, "get_enriched_quotes_batch", None)
    if callable(fn):
        try:
            res = await _maybe_await(fn(symbols, mode=mode or ""))
            if isinstance(res, dict):
                out: Dict[str, Dict[str, Any]] = {}
                for s in symbols:
                    out[s] = _extract_row_dict(res.get(s))
                return out
        except Exception:
            pass

    # Fallback list batch
    fn2 = getattr(engine, "get_enriched_quotes", None)
    if callable(fn2):
        try:
            res = await _maybe_await(fn2(symbols))
            if isinstance(res, list):
                out2: Dict[str, Dict[str, Any]] = {}
                for s, v in zip(symbols, res):
                    out2[s] = _extract_row_dict(v)
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
                out3[s] = _extract_row_dict(await _maybe_await(fn3(s)))
            elif callable(fn4):
                out3[s] = _extract_row_dict(await _maybe_await(fn4(s)))
            else:
                out3[s] = {"symbol": s, "warnings": "engine_missing_quote_methods"}
        except Exception as e:
            out3[s] = {"symbol": s, "warnings": f"quote_error: {e}"}
    return out3


async def _fetch_top10_symbols(engine: Any, criteria: Optional[Dict[str, Any]] = None, *, limit: int = 10) -> List[str]:
    """
    Best-effort discovery of Top10 symbols without requiring snapshots.
    We try common method names across phases. If none exist, returns [].
    """
    if not engine:
        return []

    # If the repo has a dedicated selector module (preferred), try it first (lazy import).
    try:
        from core.analysis.top10_selector import select_top10_symbols  # type: ignore

        syms = await _maybe_await(select_top10_symbols(engine=engine, criteria=criteria or {}, limit=limit))
        if isinstance(syms, (list, tuple)):
            out = [_safe_str(x) for x in syms if _safe_str(x)]
            return out[: max(1, limit)]
    except Exception:
        pass

    # Otherwise probe engine methods (legacy/variant names).
    candidates = [
        "get_top10_symbols",
        "select_top10_symbols",
        "top10_symbols",
        "get_top10_investments",
        "select_top10",
        "build_top10",
        "compute_top10",
    ]
    for name in candidates:
        fn = getattr(engine, name, None)
        if not callable(fn):
            continue
        try:
            res = await _maybe_await(fn(criteria=criteria or {}, limit=limit))  # type: ignore[arg-type]
        except TypeError:
            try:
                res = await _maybe_await(fn(limit=limit))  # type: ignore
            except TypeError:
                try:
                    res = await _maybe_await(fn())  # type: ignore
                except Exception:
                    continue
        except Exception:
            continue

        # Parse results
        if isinstance(res, (list, tuple)):
            out_syms: List[str] = []
            for item in res:
                if isinstance(item, str):
                    out_syms.append(item)
                elif isinstance(item, dict):
                    out_syms.append(_safe_str(item.get("symbol") or item.get("ticker") or item.get("code")))
                else:
                    d = _extract_row_dict(item)
                    out_syms.append(_safe_str(d.get("symbol") or d.get("ticker") or d.get("code")))
            out = [_safe_str(x) for x in out_syms if _safe_str(x)]
            # de-dupe preserve order
            seen = set()
            uniq: List[str] = []
            for s in out:
                if s not in seen:
                    seen.add(s)
                    uniq.append(s)
            return uniq[: max(1, limit)]

        if isinstance(res, dict):
            # might be {"rows":[...]} or {"symbols":[...]}
            if isinstance(res.get("symbols"), (list, tuple)):
                out = [_safe_str(x) for x in res["symbols"] if _safe_str(x)]
                return out[: max(1, limit)]
            rows = res.get("rows") or res.get("items")
            if isinstance(rows, (list, tuple)):
                out_syms2 = []
                for r in rows:
                    if isinstance(r, dict):
                        out_syms2.append(_safe_str(r.get("symbol") or r.get("ticker") or r.get("code")))
                out2 = [_safe_str(x) for x in out_syms2 if _safe_str(x)]
                return out2[: max(1, limit)]

    return []


# -----------------------------------------------------------------------------
# Insight computations
# -----------------------------------------------------------------------------
def _pick(d: Dict[str, Any], key: str) -> Any:
    try:
        return d.get(key)
    except Exception:
        return None


def _coverage_count(qmap: Dict[str, Dict[str, Any]], key: str) -> int:
    c = 0
    for _, d in qmap.items():
        v = _pick(d, key)
        if v is None or _safe_str(v) == "":
            continue
        c += 1
    return c


def _scored_list(qmap: Dict[str, Dict[str, Any]], key: str) -> List[Tuple[str, float]]:
    scored: List[Tuple[str, float]] = []
    for sym, d in qmap.items():
        x = _as_float(_pick(d, key))
        if x is None:
            continue
        scored.append((sym, x))
    return scored


def _avg(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return sum(vals) / float(len(vals))


def _build_universe_snapshot_rows(
    *,
    keys: Sequence[str],
    section: str,
    symbols: List[str],
    qmap: Dict[str, Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
    """
    Generates meaningful snapshot rows for a universe.
    """
    rows: List[Dict[str, Any]] = []

    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Universe Size",
            symbol="",
            metric="count",
            value=len(symbols),
            notes="Number of symbols requested for this section.",
            last_updated_riyadh=ts,
        )
    )

    # Coverage metrics
    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Coverage",
            symbol="",
            metric="coverage_current_price",
            value=f"{_coverage_count(qmap, 'current_price')}/{len(symbols)}",
            notes="How many symbols returned current_price.",
            last_updated_riyadh=ts,
        )
    )
    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Coverage",
            symbol="",
            metric="coverage_percent_change",
            value=f"{_coverage_count(qmap, 'percent_change')}/{len(symbols)}",
            notes="How many symbols returned percent_change.",
            last_updated_riyadh=ts,
        )
    )

    # Movers by percent_change
    movers = _scored_list(qmap, "percent_change")
    if movers:
        movers.sort(key=lambda t: t[1], reverse=True)
        top_sym, top_pc = movers[0]
        low_sym, low_pc = movers[-1]
        avg_pc = _avg([x for _, x in movers])

        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Top Gainer",
                symbol=top_sym,
                metric="percent_change",
                value=_fmt_pct(top_pc),
                notes="Highest percent_change in this universe.",
                last_updated_riyadh=ts,
            )
        )
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Top Loser",
                symbol=low_sym,
                metric="percent_change",
                value=_fmt_pct(low_pc),
                notes="Lowest percent_change in this universe.",
                last_updated_riyadh=ts,
            )
        )
        if avg_pc is not None:
            rows.append(
                _make_row(
                    keys=keys,
                    section=section,
                    item="Average Change",
                    symbol="",
                    metric="avg_percent_change",
                    value=_fmt_pct(avg_pc),
                    notes="Average percent_change across symbols with data.",
                    last_updated_riyadh=ts,
                )
            )
    else:
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Snapshot",
                symbol="",
                metric="status",
                value="No movers data",
                notes="percent_change not available yet for this universe.",
                last_updated_riyadh=ts,
            )
        )

    # Expected ROI (3M) snapshot (best effort)
    roi3 = _scored_list(qmap, "expected_roi_3m")
    if roi3:
        roi3.sort(key=lambda t: t[1], reverse=True)
        sym, val = roi3[0]
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Best Expected ROI (3M)",
                symbol=sym,
                metric="expected_roi_3m",
                value=_fmt_pct(val),
                notes="Highest expected_roi_3m in this universe (if computed).",
                last_updated_riyadh=ts,
            )
        )

    # Risk snapshot (volatility_90d)
    vol90 = _scored_list(qmap, "volatility_90d")
    if vol90:
        vol90.sort(key=lambda t: t[1], reverse=True)
        sym, val = vol90[0]
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Highest Volatility (90D)",
                symbol=sym,
                metric="volatility_90d",
                value=_fmt_pct(val),
                notes="Highest volatility_90d in this universe (if computed).",
                last_updated_riyadh=ts,
            )
        )

    # Confidence snapshot
    conf = _scored_list(qmap, "forecast_confidence")
    if conf:
        avg_conf = _avg([x for _, x in conf])
        if avg_conf is not None:
            rows.append(
                _make_row(
                    keys=keys,
                    section=section,
                    item="Average Forecast Confidence",
                    symbol="",
                    metric="avg_forecast_confidence",
                    value=_fmt_num(avg_conf),
                    notes="Average forecast_confidence across symbols with data.",
                    last_updated_riyadh=ts,
                )
            )

    return rows


def _build_portfolio_kpi_rows(
    *,
    keys: Sequence[str],
    section: str,
    symbols: List[str],
    qmap: Dict[str, Dict[str, Any]],
    ts: str,
) -> List[Dict[str, Any]]:
    """
    Computes portfolio KPIs if position_qty / avg_cost are available.
    If not available, returns an informative row (still non-empty).
    """
    rows: List[Dict[str, Any]] = []

    total_cost = 0.0
    total_value = 0.0
    have_any_position = False

    for sym in symbols:
        d = qmap.get(sym) or {}
        qty = _as_float(_pick(d, "position_qty"))
        avg_cost = _as_float(_pick(d, "avg_cost"))
        px = _as_float(_pick(d, "current_price"))

        if qty is None or avg_cost is None:
            continue
        have_any_position = True
        total_cost += float(qty) * float(avg_cost)
        if px is not None:
            total_value += float(qty) * float(px)

    if not have_any_position:
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Portfolio KPIs",
                symbol="",
                metric="status",
                value="Unavailable",
                notes="position_qty / avg_cost not present in returned rows (or portfolio universe not provided).",
                last_updated_riyadh=ts,
            )
        )
        return rows

    unreal = total_value - total_cost
    unreal_pct = (unreal / total_cost) * 100.0 if total_cost > 0 else None

    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Portfolio Cost",
            symbol="",
            metric="total_cost",
            value=_fmt_num(total_cost),
            notes="Sum(position_qty * avg_cost).",
            last_updated_riyadh=ts,
        )
    )
    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Portfolio Value",
            symbol="",
            metric="total_value",
            value=_fmt_num(total_value),
            notes="Sum(position_qty * current_price) where available.",
            last_updated_riyadh=ts,
        )
    )
    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Unrealized P/L",
            symbol="",
            metric="unrealized_pl",
            value=_fmt_num(unreal),
            notes="total_value - total_cost.",
            last_updated_riyadh=ts,
        )
    )
    if unreal_pct is not None:
        rows.append(
            _make_row(
                keys=keys,
                section=section,
                item="Unrealized P/L %",
                symbol="",
                metric="unrealized_pl_pct",
                value=_fmt_pct(unreal_pct),
                notes="unrealized_pl / total_cost.",
                last_updated_riyadh=ts,
            )
        )
    return rows


# -----------------------------------------------------------------------------
# Auto-universe defaults (when user passes no symbols)
# -----------------------------------------------------------------------------
def _default_universes() -> Dict[str, List[str]]:
    """
    You can override defaults via env vars:
      TFB_INSIGHTS_INDICES
      TFB_INSIGHTS_COMMODITIES_FX
    """
    indices = _env_csv(
        "TFB_INSIGHTS_INDICES",
        # includes your dashboard intent: TASI, NOMU, S&P500, NASDAQ, FTSE (+ safe Yahoo fallbacks)
        "TASI,NOMU,^GSPC,^IXIC,^FTSE",
    )
    commodities_fx = _env_csv(
        "TFB_INSIGHTS_COMMODITIES_FX",
        # your dashboard intent: Gold, Brent (+ optional FX)
        "GC=F,BZ=F,USDSAR=X,EURUSD=X",
    )
    return {
        "Indices & Benchmarks": indices,
        "Commodities & FX": commodities_fx,
    }


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
async def build_insights_analysis_rows(
    *,
    engine: Optional[Any] = None,
    criteria: Optional[Dict[str, Any]] = None,
    universes: Optional[Dict[str, Sequence[str]]] = None,
    symbols: Optional[Sequence[str]] = None,
    mode: str = "",
    include_criteria_rows: bool = True,
    include_system_rows: bool = True,
    auto_universe_when_empty: bool = True,
    include_top10_section: bool = True,
    include_portfolio_kpis: bool = True,
    max_symbols_per_universe: int = 25,
) -> Dict[str, Any]:
    """
    Build Insights_Analysis payload (headers/keys/rows) aligned to schema_registry.

    - If `universes` is provided, we use it.
    - Else if `symbols` is provided, we treat them as a single universe.
    - Else if empty and engine exists and auto_universe_when_empty is True,
      we build default universes (indices + commodities/FX), plus Top10 + portfolio KPIs best-effort.

    Returns envelope:
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

    # Criteria rows (keeps criteria visible even if the criteria block is metadata-driven)
    if include_criteria_rows:
        rows.extend(build_criteria_rows(criteria=criteria, last_updated_riyadh=ts))

    # System rows (safe, zero network)
    if include_system_rows:
        schema_version = ""
        try:
            from core.sheets.schema_registry import SCHEMA_VERSION as _SV  # type: ignore

            schema_version = _safe_str(_SV)
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

    # Build effective universes
    eff_universes: Dict[str, List[str]] = {}

    if universes:
        for name, seq in (universes or {}).items():
            sym_list = [_safe_str(s) for s in (seq or []) if _safe_str(s)]
            if sym_list:
                eff_universes[_safe_str(name) or "Universe"] = sym_list

    if not eff_universes and symbols:
        sym_list2 = [_safe_str(s) for s in (symbols or []) if _safe_str(s)]
        if sym_list2:
            eff_universes["Selected Symbols"] = sym_list2

    # Auto universe when empty
    auto_used = False
    if (not eff_universes) and engine and auto_universe_when_empty:
        eff_universes.update(_default_universes())
        auto_used = True

    # Summary row (always)
    rows.append(
        _make_row(
            keys=keys,
            section="Summary",
            item="Sections Included",
            symbol="",
            metric="sections",
            value=", ".join(list(eff_universes.keys())) if eff_universes else "None",
            notes="Universes used to build insights (auto-universe applied when empty)." if auto_used else "Universes provided by caller.",
            last_updated_riyadh=ts,
        )
    )

    # If no engine, we still return meaningful rows (criteria/system/summary)
    if not engine or not eff_universes:
        if not engine:
            rows.append(
                _make_row(
                    keys=keys,
                    section="Summary",
                    item="Engine",
                    symbol="",
                    metric="engine_status",
                    value="Not provided",
                    notes="No engine passed; returning schema-correct rows (criteria/system/summary only).",
                    last_updated_riyadh=ts,
                )
            )
        elif not eff_universes:
            rows.append(
                _make_row(
                    keys=keys,
                    section="Summary",
                    item="Universes",
                    symbol="",
                    metric="universe_status",
                    value="Empty",
                    notes="No universes/symbols provided and auto-universe disabled.",
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
                "engine_used": bool(engine),
                "auto_universe_used": auto_used,
                "universes": list(eff_universes.keys()),
                "mode": mode,
            },
        }

    # For each universe section, compute snapshots (best-effort)
    for section_name, sym_list in eff_universes.items():
        syms = [_safe_str(s) for s in (sym_list or []) if _safe_str(s)]
        if not syms:
            continue

        # clamp
        try:
            cap = max(1, min(int(max_symbols_per_universe), 500))
        except Exception:
            cap = 25
        syms = syms[:cap]

        qmap = await _fetch_quotes_map(engine, syms, mode=mode or "")
        rows.extend(
            _build_universe_snapshot_rows(
                keys=keys,
                section=section_name,
                symbols=syms,
                qmap=qmap,
                ts=ts,
            )
        )

        # If this is a portfolio universe (common name), add KPIs
        if include_portfolio_kpis and section_name.strip().lower() in {"my_portfolio", "portfolio", "my portfolio"}:
            rows.extend(
                _build_portfolio_kpi_rows(
                    keys=keys,
                    section="Portfolio KPIs",
                    symbols=syms,
                    qmap=qmap,
                    ts=ts,
                )
            )

    # Top10 section (best effort)
    if include_top10_section:
        top10_syms = await _fetch_top10_symbols(engine, criteria=criteria, limit=10)
        if top10_syms:
            qmap10 = await _fetch_quotes_map(engine, top10_syms, mode=mode or "")
            rows.append(
                _make_row(
                    keys=keys,
                    section="Top 10 Investments",
                    item="Status",
                    symbol="",
                    metric="top10_count",
                    value=len(top10_syms),
                    notes="Top10 symbols generated in live mode (best-effort).",
                    last_updated_riyadh=ts,
                )
            )
            # show one row per symbol (compact)
            for i, sym in enumerate(top10_syms, start=1):
                d = qmap10.get(sym) or {}
                roi3 = _as_float(_pick(d, "expected_roi_3m"))
                conf = _as_float(_pick(d, "forecast_confidence"))
                reco = _safe_str(_pick(d, "recommendation"))
                name = _safe_str(_pick(d, "name"))

                v = _fmt_pct(roi3) if roi3 is not None else ""
                note_parts = []
                if name:
                    note_parts.append(name)
                if conf is not None:
                    note_parts.append(f"conf={_fmt_num(conf)}")
                if reco:
                    note_parts.append(f"reco={reco}")
                notes = " | ".join(note_parts) if note_parts else "Top10 item (details depend on engine fields availability)."

                rows.append(
                    _make_row(
                        keys=keys,
                        section="Top 10 Investments",
                        item=f"#{i}",
                        symbol=sym,
                        metric="expected_roi_3m",
                        value=v or "N/A",
                        notes=notes,
                        last_updated_riyadh=ts,
                    )
                )
        else:
            rows.append(
                _make_row(
                    keys=keys,
                    section="Top 10 Investments",
                    item="Status",
                    symbol="",
                    metric="top10_status",
                    value="Unavailable",
                    notes="No Top10 method found or returned empty. This will be fixed when top10_selector + route are aligned.",
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
            "auto_universe_used": auto_used,
            "universes": list(eff_universes.keys()),
            "mode": mode,
            "builder_version": INSIGHTS_BUILDER_VERSION,
        },
    }


__all__ = [
    "INSIGHTS_BUILDER_VERSION",
    "get_insights_schema",
    "build_criteria_rows",
    "build_insights_analysis_rows",
]
