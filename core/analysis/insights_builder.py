#!/usr/bin/env python3
# core/analysis/insights_builder.py
"""
================================================================================
Insights Analysis Builder — v1.2.0 (GREEN STATUS + PCT FIX + NUMERIC VALUES)
================================================================================
Tadawul Fast Bridge (TFB)

Why this revision (fix the “❌ red X / not green” symptom)
- ✅ Adds explicit System status rows: Build Status = OK / WARN (never blank).
- ✅ Fixes percent formatting:
    - If engine returns fractions (0.12) -> shows 12.00%
    - If engine returns percent-points (12) -> shows 12.00%
- ✅ Keeps VALUE column numeric when possible (better for Google Sheets formulas/icons).
- ✅ Still import-safe: NO network calls at import time.
- ✅ Still schema-first: keys/order come from core/sheets/schema_registry.py.

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

import inspect
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("core.analysis.insights_builder")
logger.addHandler(logging.NullHandler())

INSIGHTS_BUILDER_VERSION = "1.2.0"
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


def _pct_points(v: Any) -> Optional[float]:
    """
    Normalize percent values into percent-points:
      - 0.12 -> 12.0
      - 12 -> 12.0
      - "12%" -> 12.0 (best-effort via float)
    """
    x = _as_float(v)
    if x is None:
        return None
    # fraction heuristic
    if abs(x) <= 1.5:
        return x * 100.0
    return x


def _fmt_pct(v: Any) -> str:
    x = _pct_points(v)
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
        return [
            {"key": "risk_level", "label": "Risk Level", "dtype": "str", "default": "Moderate", "notes": "Low / Moderate / High."},
            {"key": "confidence_level", "label": "Confidence Level", "dtype": "str", "default": "High", "notes": "High / Medium / Low."},
            {"key": "invest_period_days", "label": "Investment Period (Days)", "dtype": "int", "default": "90", "notes": "Always treated in DAYS internally."},
            {"key": "required_return_pct", "label": "Required Return %", "dtype": "pct", "default": "0.10", "notes": "Minimum expected ROI threshold."},
            {"key": "amount", "label": "Amount", "dtype": "float", "default": "0", "notes": "Investment amount (optional)."},
        ]


# -----------------------------------------------------------------------------
# Row builder (ALWAYS fills required fields)
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

    sec = _safe_str(section) or "General"
    it = _safe_str(item) or "Item"
    met = _safe_str(metric) or "metric"
    sym = _safe_str(symbol)

    # VALUE: keep numeric when possible (for Google Sheets conditional icons)
    val_out: Any
    if value is None:
        val_out = ""
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        val_out = value
    else:
        # try numeric casting first
        xf = _as_float(value)
        xi = _as_int(value)
        if xi is not None and _safe_str(value).strip().isdigit():
            val_out = xi
        elif xf is not None and _safe_str(value) != "":
            val_out = xf
        else:
            val_out = _safe_str(value)

    base: Dict[str, Any] = {
        "section": sec,
        "item": it,
        "symbol": sym,
        "metric": met,
        "value": val_out,
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
    _, keys, _ = get_insights_schema()
    ts = last_updated_riyadh or _now_riyadh_iso()

    fields = _get_criteria_fields()
    crit = dict(criteria or {})

    rows: List[Dict[str, Any]] = []
    for f in fields:
        k = f["key"]
        label = f["label"]
        v = crit.get(k, f.get("default", ""))

        # normalize pct-looking fields to friendly display text in notes, keep value numeric where possible
        if k.endswith("_pct") or k.endswith("_percent") or "return" in k:
            # keep numeric if possible; notes shows formatted string
            vnum = _as_float(v)
            note = f.get("notes", "")
            if vnum is not None:
                note = (note + " " if note else "") + f"(display: {_fmt_pct(vnum)})"
                v = vnum  # store numeric
            rows.append(
                _make_row(
                    keys=keys,
                    section="Criteria",
                    item=label,
                    symbol="",
                    metric=k,
                    value=v,
                    notes=note,
                    last_updated_riyadh=ts,
                )
            )
        else:
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
    if inspect.isawaitable(v):
        return await v
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
    if not engine or not symbols:
        return {}

    fn = getattr(engine, "get_enriched_quotes_batch", None)
    if callable(fn):
        try:
            # allow engines that accept (symbols, mode=...)
            try:
                res = await _maybe_await(fn(symbols, mode=mode or ""))
            except TypeError:
                res = await _maybe_await(fn(symbols))
            if isinstance(res, dict):
                out: Dict[str, Dict[str, Any]] = {}
                for s in symbols:
                    out[s] = _extract_row_dict(res.get(s))
                return out
        except Exception:
            pass

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
    if not engine:
        return []

    # Preferred selector module (lazy import)
    try:
        from core.analysis.top10_selector import select_top10_symbols  # type: ignore

        syms = await _maybe_await(select_top10_symbols(engine=engine, criteria=criteria or {}, limit=limit))
        if isinstance(syms, (list, tuple)):
            out = [_safe_str(x) for x in syms if _safe_str(x)]
            return out[: max(1, limit)]
    except Exception:
        pass

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
            try:
                res = await _maybe_await(fn(criteria=criteria or {}, limit=limit))  # type: ignore[arg-type]
            except TypeError:
                try:
                    res = await _maybe_await(fn(limit=limit))  # type: ignore
                except TypeError:
                    res = await _maybe_await(fn())  # type: ignore
        except Exception:
            continue

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
            seen = set()
            uniq: List[str] = []
            for s in out:
                if s not in seen:
                    seen.add(s)
                    uniq.append(s)
            return uniq[: max(1, limit)]

        if isinstance(res, dict):
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
                value=_pct_points(top_pc),
                notes=f"Highest percent_change in this universe (display: {_fmt_pct(top_pc)}).",
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
                value=_pct_points(low_pc),
                notes=f"Lowest percent_change in this universe (display: {_fmt_pct(low_pc)}).",
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
                    value=_pct_points(avg_pc),
                    notes=f"Average percent_change across symbols with data (display: {_fmt_pct(avg_pc)}).",
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
                value=_pct_points(val),
                notes=f"Highest expected_roi_3m (display: {_fmt_pct(val)}).",
                last_updated_riyadh=ts,
            )
        )

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
                value=_pct_points(val),
                notes=f"Highest volatility_90d (display: {_fmt_pct(val)}).",
                last_updated_riyadh=ts,
            )
        )

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
                    value=avg_conf,
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
                notes="position_qty / avg_cost not present (or portfolio universe not provided).",
                last_updated_riyadh=ts,
            )
        )
        return rows

    unreal = total_value - total_cost
    unreal_pct = (unreal / total_cost) if total_cost > 0 else None  # fraction

    rows.append(
        _make_row(
            keys=keys,
            section=section,
            item="Portfolio Cost",
            symbol="",
            metric="total_cost",
            value=total_cost,
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
            value=total_value,
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
            value=unreal,
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
                value=_pct_points(unreal_pct),  # percent-points
                notes=f"unrealized_pl / total_cost (display: {_fmt_pct(unreal_pct)}).",
                last_updated_riyadh=ts,
            )
        )
    return rows


# -----------------------------------------------------------------------------
# Auto-universe defaults
# -----------------------------------------------------------------------------
def _default_universes() -> Dict[str, List[str]]:
    indices = _env_csv("TFB_INSIGHTS_INDICES", "TASI,NOMU,^GSPC,^IXIC,^FTSE")
    commodities_fx = _env_csv("TFB_INSIGHTS_COMMODITIES_FX", "GC=F,BZ=F,USDSAR=X,EURUSD=X")
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
    headers, keys, schema_source = get_insights_schema()
    ts = _now_riyadh_iso()

    rows: List[Dict[str, Any]] = []

    if include_criteria_rows:
        rows.extend(build_criteria_rows(criteria=criteria, last_updated_riyadh=ts))

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

    auto_used = False
    if (not eff_universes) and engine and auto_universe_when_empty:
        eff_universes.update(_default_universes())
        auto_used = True

    # Always add build-status (this is what makes the sheet show ✅ instead of ❌)
    build_ok = bool(engine) and bool(eff_universes)
    rows.append(
        _make_row(
            keys=keys,
            section="System",
            item="Build Status",
            symbol="",
            metric="build_status",
            value="OK" if build_ok else "WARN",
            notes="OK = engine + universes available. WARN = criteria/system only (no engine or no universes).",
            last_updated_riyadh=ts,
        )
    )

    rows.append(
        _make_row(
            keys=keys,
            section="Summary",
            item="Sections Included",
            symbol="",
            metric="sections",
            value=", ".join(list(eff_universes.keys())) if eff_universes else "None",
            notes="Auto-universe applied." if auto_used else "Universes provided by caller (or none).",
            last_updated_riyadh=ts,
        )
    )

    # No engine or no universes -> still return GREEN/WARN rows (never empty)
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
                "builder_version": INSIGHTS_BUILDER_VERSION,
            },
        }

    # Build universe snapshots
    for section_name, sym_list in eff_universes.items():
        syms = [_safe_str(s) for s in (sym_list or []) if _safe_str(s)]
        if not syms:
            continue

        try:
            cap = max(1, min(int(max_symbols_per_universe), 500))
        except Exception:
            cap = 25
        syms = syms[:cap]

        qmap = await _fetch_quotes_map(engine, syms, mode=mode or "")
        rows.extend(_build_universe_snapshot_rows(keys=keys, section=section_name, symbols=syms, qmap=qmap, ts=ts))

        if include_portfolio_kpis and section_name.strip().lower() in {"my_portfolio", "portfolio", "my portfolio"}:
            rows.extend(_build_portfolio_kpi_rows(keys=keys, section="Portfolio KPIs", symbols=syms, qmap=qmap, ts=ts))

    # Top10 section
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
                    notes="Top10 symbols generated (best-effort).",
                    last_updated_riyadh=ts,
                )
            )
            for i, sym in enumerate(top10_syms, start=1):
                d = qmap10.get(sym) or {}
                roi3 = _as_float(_pick(d, "expected_roi_3m"))
                conf = _as_float(_pick(d, "forecast_confidence"))
                reco = _safe_str(_pick(d, "recommendation"))
                name = _safe_str(_pick(d, "name"))

                note_parts = []
                if name:
                    note_parts.append(name)
                if conf is not None:
                    note_parts.append(f"conf={_fmt_num(conf)}")
                if reco:
                    note_parts.append(f"reco={reco}")
                notes = " | ".join(note_parts) if note_parts else "Top10 item."

                rows.append(
                    _make_row(
                        keys=keys,
                        section="Top 10 Investments",
                        item=f"#{i}",
                        symbol=sym,
                        metric="expected_roi_3m",
                        value=_pct_points(roi3) if roi3 is not None else "",
                        notes=(notes + (f" (display: {_fmt_pct(roi3)})" if roi3 is not None else "")),
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
                    notes="No Top10 method found or returned empty.",
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
