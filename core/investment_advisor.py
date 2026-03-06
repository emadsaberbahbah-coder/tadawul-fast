#!/usr/bin/env python3
# core/investment_advisor.py
"""
================================================================================
Investment Advisor Core — v4.2.0 (LIVE-BY-DEFAULT / SNAPSHOT-FALLBACK / SAFE)
================================================================================

Why this revision (recommendation_reason issue)
- Your advisor output already produced recommendation labels such as BUY / HOLD / SELL.
- But in some cases the scoring layer returned:
    recommendation = "BUY"
    recommendation_reason = null
- This revision guarantees that recommendation_reason is always synthesized
  when the upstream scoring module does not provide one.

What v4.2.0 changes
1) ✅ Keeps current live/snapshot advisor flow unchanged.
2) ✅ Adds synthesized recommendation_reason fallback when missing/blank.
3) ✅ Builds explanation from available score and market context:
   - overall score
   - expected ROI
   - confidence
   - risk
   - valuation / momentum / quality / opportunity
   - liquidity
4) ✅ Preserves upstream reason when the scoring module already provides one.

Entry:
- run_investment_advisor(payload: dict, engine: Any=None) -> dict

================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import threading
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# ---------------------------------------------------------------------------
# High-Performance JSON fallback (optional)
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj, default=str).decode("utf-8")

except Exception:

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str, ensure_ascii=False)

# ---------------------------------------------------------------------------
# Optional scientific stack (safe)
# ---------------------------------------------------------------------------
try:
    import numpy as np  # type: ignore

    HAS_NUMPY = True
except Exception:
    np = None  # type: ignore
    HAS_NUMPY = False

try:
    from scipy import optimize  # type: ignore

    HAS_SCIPY = True
except Exception:
    optimize = None  # type: ignore
    HAS_SCIPY = False

# ---------------------------------------------------------------------------
# Optional monitoring/tracing (safe)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter as PromCounter, Histogram as PromHistogram  # type: ignore

    _PROMETHEUS_AVAILABLE = True
    advisor_requests_total = PromCounter("advisor_requests_total", "Total advisor requests", ["status", "strategy"])
    advisor_optimization_duration = PromHistogram(
        "advisor_optimization_duration_seconds",
        "Time spent optimizing portfolios",
        ["strategy"],
    )
except Exception:
    _PROMETHEUS_AVAILABLE = False

    class _DummyMetric:
        def labels(self, *args, **kwargs):  # noqa
            return self

        def inc(self, *args, **kwargs):  # noqa
            return None

        def observe(self, *args, **kwargs):  # noqa
            return None

    advisor_requests_total = _DummyMetric()
    advisor_optimization_duration = _DummyMetric()

try:
    from opentelemetry import trace  # type: ignore

    tracer = trace.get_tracer(__name__)
except Exception:

    class _DummySpan:
        def set_attribute(self, *args, **kwargs):  # noqa
            return None

        def __enter__(self):  # noqa
            return self

        def __exit__(self, *args, **kwargs):  # noqa
            return None

    class _DummyTracer:
        def start_as_current_span(self, *args, **kwargs):  # noqa
            return _DummySpan()

    tracer = _DummyTracer()

# ---------------------------------------------------------------------------
# Phase 1: page_catalog + schema_registry (optional imports)
# ---------------------------------------------------------------------------
try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore
    from core.sheets.page_catalog import CANONICAL_PAGES as _CANONICAL_PAGES  # type: ignore

    _HAS_PAGE_CATALOG = True
except Exception:
    _HAS_PAGE_CATALOG = False
    _CANONICAL_PAGES = set()  # type: ignore

    def _normalize_page_name(name: str, allow_output_pages: bool = False) -> str:  # type: ignore
        return str(name or "").strip()

try:
    from core.sheets.schema_registry import get_sheet_spec as _get_sheet_spec  # type: ignore

    _HAS_SCHEMA = True
except Exception:
    _HAS_SCHEMA = False

    def _get_sheet_spec(sheet_name: str) -> Any:  # type: ignore
        raise KeyError("schema_registry unavailable")

# ---------------------------------------------------------------------------
# Phase 4 scoring module (required; safe fallback)
# ---------------------------------------------------------------------------
try:
    from core.scoring import compute_scores as _compute_scores  # type: ignore

    _HAS_SCORING = True
except Exception:
    _HAS_SCORING = False

    def _compute_scores(row: Dict[str, Any], *, settings: Any = None) -> Dict[str, Any]:  # type: ignore
        return {
            "risk_score": None,
            "overall_score": None,
            "valuation_score": None,
            "momentum_score": None,
            "confidence_score": None,
            "value_score": None,
            "quality_score": None,
            "opportunity_score": None,
            "recommendation": "HOLD",
            "recommendation_reason": "Scoring module unavailable.",
        }

# ---------------------------------------------------------------------------
# Version / constants
# ---------------------------------------------------------------------------
__version__ = "4.2.0"
ADVISOR_VERSION = __version__

logger = logging.getLogger("core.investment_advisor")

DEFAULT_SOURCES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}

RIYADH_TZ = timezone(timedelta(hours=3))


# =============================================================================
# Enums
# =============================================================================
class RiskProfile(Enum):
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"
    VERY_AGGRESSIVE = "very_aggressive"


class AllocationStrategy(Enum):
    EQUAL_WEIGHT = "equal_weight"
    MARKET_CAP = "market_cap"
    RISK_PARITY = "risk_parity"
    MINIMUM_VARIANCE = "minimum_variance"
    MAXIMUM_SHARPE = "maximum_sharpe"
    BLACK_LITTERMAN = "black_litterman"


class AdvisorDataMode(Enum):
    SNAPSHOT = "snapshot"
    LIVE_SHEET = "live_sheet"
    LIVE_QUOTES = "live_quotes"
    AUTO = "auto"


# =============================================================================
# Safe sync/async bridging (works inside FastAPI event loop)
# =============================================================================
def _in_running_loop() -> bool:
    try:
        loop = asyncio.get_running_loop()
        return bool(loop.is_running())
    except Exception:
        return False


def _run_coro_in_thread(coro: Any, timeout: float) -> Tuple[Optional[Any], Optional[BaseException], bool]:
    box: Dict[str, Any] = {"result": None, "error": None}

    def _worker():
        try:
            box["result"] = asyncio.run(asyncio.wait_for(coro, timeout=timeout))
        except BaseException as e:  # noqa
            box["error"] = e

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout + 0.25)
    if t.is_alive():
        return None, None, True
    return box["result"], box["error"], False


def _safe_call(engine: Any, method: str, *args: Any, timeout: float = 6.0, **kwargs: Any) -> Tuple[Optional[Any], Optional[str]]:
    """
    Calls sync/async engine methods safely from a sync context.
    Returns (result, error_str_or_None)
    """
    if engine is None:
        return None, "engine_none"
    fn = getattr(engine, method, None)
    if not callable(fn):
        return None, "method_not_found"

    try:
        out = fn(*args, **kwargs)
        if inspect.isawaitable(out):
            if _in_running_loop():
                res, err, timed_out = _run_coro_in_thread(out, timeout=timeout)
                if timed_out:
                    return None, f"timeout:{timeout}s"
                if err is not None:
                    return None, f"exception:{type(err).__name__}:{err}"
                return res, None
            try:
                res = asyncio.run(asyncio.wait_for(out, timeout=timeout))
                return res, None
            except asyncio.TimeoutError:
                return None, f"timeout:{timeout}s"
        return out, None
    except Exception as e:
        return None, f"exception:{type(e).__name__}:{e}"


# =============================================================================
# Helpers
# =============================================================================
def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in _TRUTHY


def _safe_str(x: Any, default: str = "") -> str:
    try:
        return str(x).strip() if x is not None else default
    except Exception:
        return default


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            f = float(x)
        else:
            s = str(x).strip().replace(",", "")
            if not s or s.lower() in {"na", "n/a", "none", "null"}:
                return None
            if s.endswith("%"):
                f = float(s[:-1].strip()) / 100.0
            else:
                f = float(s)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    f = _to_float(x)
    return int(f) if f is not None else None


def _as_ratio(x: Any) -> Optional[float]:
    """
    Keep ratios as fractions:
      0.12 => 0.12
      12   => 0.12
      "12%" => 0.12
    """
    f = _to_float(x)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _iso_riyadh(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ).isoformat()


def _parse_iso_to_dt(v: Any) -> Optional[datetime]:
    s = _safe_str(v)
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _norm_key(k: Any) -> str:
    return " ".join(_safe_str(k).lower().split())


def _snake_like(header: str) -> str:
    s = _safe_str(header)
    s = s.strip().replace("%", " pct").replace("/", " ")
    out = []
    prev_us = False
    for ch in s:
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


def _get_any(row: Dict[str, Any], *names: str) -> Any:
    if not row:
        return None
    for n in names:
        if n in row:
            return row[n]
    nmap = row.get("_nmap")
    if not isinstance(nmap, dict):
        nmap = {_norm_key(k): k for k in row.keys() if k != "_nmap" and _norm_key(k)}
        row["_nmap"] = nmap
    for n in names:
        nk = _norm_key(n)
        if nk in nmap:
            return row.get(nmap[nk])
    return None


def _norm_symbol(symbol: str) -> str:
    s = _safe_str(symbol).upper().replace(" ", "")
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    return s


def _canonical_symbol(symbol: str) -> str:
    s = _norm_symbol(symbol)
    if not s:
        return ""
    if s.endswith(".SR") and s[:-3].isdigit():
        return "KSA:" + s[:-3]
    if s.isdigit():
        return "KSA:" + s
    if s.startswith("^"):
        return "IDX:" + s
    if "." in s:
        return "GLOBAL:" + s.split(".", 1)[0]
    return "GLOBAL:" + s


def _symbol_variants(symbol: str) -> Set[str]:
    s = _norm_symbol(symbol)
    variants: Set[str] = set()
    if not s:
        return variants
    variants.add(s)
    if s.endswith(".SR") and s[:-3].isdigit():
        variants.add(s[:-3])
    elif s.isdigit():
        variants.add(s + ".SR")
    if "." in s:
        base = s.split(".", 1)[0]
        if base:
            variants.add(base)
    return variants


def _clean_reason_text(text: Any) -> str:
    s = _safe_str(text)
    if not s:
        return ""
    s = " ".join(s.split())
    s = s.strip(" ;,.-")
    return s


def _fmt_pct_from_ratio(value: Optional[float], digits: int = 1) -> Optional[str]:
    if value is None:
        return None
    try:
        return f"{float(value) * 100:.{digits}f}%"
    except Exception:
        return None


def _fmt_num(value: Optional[float], digits: int = 1) -> Optional[str]:
    if value is None:
        return None
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return None


# =============================================================================
# Mode selection (LIVE by default)
# =============================================================================
def _parse_advisor_data_mode(payload: Dict[str, Any]) -> AdvisorDataMode:
    raw = (
        (payload or {}).get("advisor_data_mode")
        or (payload or {}).get("data_mode")
        or (payload or {}).get("mode")
        or ""
    )
    raw = str(raw).strip().lower()
    if not raw:
        raw = str(os.getenv("ADVISOR_DATA_MODE", "live_quotes") or "live_quotes").strip().lower()

    mapping = {
        "snapshot": AdvisorDataMode.SNAPSHOT,
        "snapshots": AdvisorDataMode.SNAPSHOT,
        "live_sheet": AdvisorDataMode.LIVE_SHEET,
        "sheet": AdvisorDataMode.LIVE_SHEET,
        "live_quotes": AdvisorDataMode.LIVE_QUOTES,
        "quotes": AdvisorDataMode.LIVE_QUOTES,
        "live": AdvisorDataMode.LIVE_QUOTES,
        "auto": AdvisorDataMode.AUTO,
    }
    return mapping.get(raw, AdvisorDataMode.LIVE_QUOTES)


# =============================================================================
# Schema-aware row building from snapshots / sheet_rows
# =============================================================================
def _schema_header_to_key(sheet_name: str, header: str) -> str:
    if _HAS_SCHEMA:
        try:
            spec = _get_sheet_spec(sheet_name)
            cols = getattr(spec, "columns", None) or []
            h_norm = _norm_key(header)
            for c in cols:
                h = _norm_key(getattr(c, "header", "") or "")
                if h and h == h_norm:
                    k = getattr(c, "key", None)
                    if k:
                        return str(k)
        except Exception:
            pass
    return _snake_like(header)


def _rows_matrix_to_keyed_dicts(
    sheet_name: str,
    headers: List[Any],
    rows: List[Any],
    keys: Optional[List[str]] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    if not isinstance(rows, list) or not rows:
        return []

    if keys and isinstance(keys, list) and all(isinstance(k, str) for k in keys) and keys:
        col_keys = list(keys)
    else:
        hdrs = [str(x).strip() for x in (headers or [])]
        if not hdrs:
            try:
                max_len = max(len(r) for r in rows if isinstance(r, (list, tuple)))
            except Exception:
                max_len = 0
            hdrs = [f"col_{i+1}" for i in range(max_len)]
        col_keys = [_schema_header_to_key(sheet_name, h) for h in hdrs]

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        if i >= limit:
            break
        if not isinstance(r, (list, tuple)):
            continue
        d: Dict[str, Any] = {}
        for j, k in enumerate(col_keys):
            d[k] = r[j] if j < len(r) else None
        d["_Sheet"] = sheet_name
        out.append(d)
    return out


def _normalize_source_pages(sources: List[str]) -> List[str]:
    raw: List[str] = []
    for s in sources or []:
        if not s:
            continue
        if isinstance(s, str) and s.strip().upper() == "ALL":
            raw.extend(DEFAULT_SOURCES)
        else:
            raw.append(str(s).strip())

    if not raw:
        raw = list(DEFAULT_SOURCES)

    out: List[str] = []
    seen: Set[str] = set()
    for s in raw:
        try:
            page = _normalize_page_name(s, allow_output_pages=False) if _HAS_PAGE_CATALOG else s
        except Exception:
            page = s

        if page in {"KSA_TADAWUL", "Advisor_Criteria", "AI_Opportunity_Report"}:
            continue
        if page in {"Data_Dictionary", "Top_10_Investments", "Insights_Analysis"}:
            continue

        if page and page not in seen:
            seen.add(page)
            out.append(page)

    return out or list(DEFAULT_SOURCES)


# =============================================================================
# Engine snapshot + live sheet builders
# =============================================================================
def _engine_get_multi_snapshots(engine: Any, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
    if engine is None:
        return {}
    fn = getattr(engine, "get_cached_multi_sheet_snapshots", None)
    if callable(fn):
        res, err = _safe_call(engine, "get_cached_multi_sheet_snapshots", sheet_names, timeout=6.0)
        if err is None and isinstance(res, dict):
            return {str(k): v for k, v in res.items() if isinstance(v, dict)}
    out: Dict[str, Dict[str, Any]] = {}
    fn2 = getattr(engine, "get_cached_sheet_snapshot", None)
    if callable(fn2):
        for s in sheet_names or []:
            r, e = _safe_call(engine, "get_cached_sheet_snapshot", str(s), timeout=4.0)
            if e is None and isinstance(r, dict):
                out[str(s)] = r
    return out


def _engine_get_sheet_rows_payload(engine: Any, sheet_name: str, limit: int = 5000) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Calls engine.get_sheet_rows (or equivalents) using keyword-only signatures first.
    Expected (best effort) output shapes:
      {headers, keys, rows, rows_matrix, meta, ...}
    """
    if engine is None:
        return None, "engine_none"

    candidates = ["get_sheet_rows", "sheet_rows", "build_sheet_rows", "get_sheet", "fetch_sheet", "get_rows"]
    for m in candidates:
        if not callable(getattr(engine, m, None)):
            continue

        body = {"include_matrix": True, "mode": "advisor"}
        variants = [
            ((), {"sheet": sheet_name, "limit": limit, "offset": 0, "mode": "advisor", "body": body}),
            ((), {"sheet": sheet_name, "limit": limit, "offset": 0, "body": body}),
            ((), {"sheet": sheet_name, "limit": limit}),
            ((), {"sheet": sheet_name}),
            ((), {"page": sheet_name, "limit": limit, "offset": 0, "mode": "advisor", "body": body}),
            ((sheet_name,), {}),
        ]

        last_err = None
        for args, kwargs in variants:
            res, err = _safe_call(engine, m, *args, timeout=8.0, **kwargs)
            if err is None and isinstance(res, dict):
                return res, None
            last_err = err
        if last_err:
            continue

    return None, "sheet_rows_method_not_found_or_failed"


def _engine_get_quotes_batch(engine: Any, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Best-effort batch quote fetch.
    Returns {symbol: quote_dict}
    """
    if engine is None:
        return {}
    syms = [s for s in (_safe_str(x).strip() for x in (symbols or [])) if s]
    if not syms:
        return {}

    methods = ["get_enriched_quotes_batch", "get_quotes_batch", "fetch_quotes_batch"]
    for m in methods:
        if not callable(getattr(engine, m, None)):
            continue

        variants = [
            ((), {"symbols": syms}),
            ((syms,), {}),
            ((), {"tickers": syms}),
        ]
        for args, kwargs in variants:
            res, err = _safe_call(engine, m, *args, timeout=12.0, **kwargs)
            if err is None and isinstance(res, dict):
                out: Dict[str, Dict[str, Any]] = {}
                for k, v in res.items():
                    if isinstance(v, dict):
                        out[_safe_str(k)] = dict(v)
                if out:
                    return out
    return {}


def _merge_non_destructive(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(dst or {})
    for k, v in (src or {}).items():
        if v is None:
            continue
        if k not in out or out.get(k) in (None, "", [], {}):
            out[k] = v
    return out


def _extract_symbols_from_rows(rows: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        s = r.get("symbol") or r.get("ticker") or r.get("code") or r.get("Symbol") or r.get("Ticker")
        s = _safe_str(s).strip()
        if s:
            out.append(_norm_symbol(s))
    seen = set()
    uniq = []
    for s in out:
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


# =============================================================================
# Universe fetch (snapshot vs live)
# =============================================================================
def _fetch_universe(
    sources: List[str],
    *,
    engine: Any,
    payload: Dict[str, Any],
    max_rows_per_source: int = 5000,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    mode = _parse_advisor_data_mode(payload or {})
    sources_norm = _normalize_source_pages(sources)

    meta: Dict[str, Any] = {
        "sources": sources_norm,
        "engine": type(engine).__name__ if engine is not None else None,
        "items": [],
        "advisor_data_mode": mode.value,
        "mode_used": None,
    }

    if engine is None:
        return [], meta, "Missing engine instance"

    if mode in (AdvisorDataMode.SNAPSHOT, AdvisorDataMode.AUTO):
        snaps = _engine_get_multi_snapshots(engine, sources_norm)
        out_rows: List[Dict[str, Any]] = []

        for sheet in sources_norm:
            snap = snaps.get(sheet)
            if not isinstance(snap, dict):
                meta["items"].append({"sheet": sheet, "cached": False})
                continue

            headers = snap.get("headers") or []
            keys = snap.get("keys") or snap.get("schema_keys") or None
            rows = snap.get("rows")
            rows_matrix = snap.get("rows_matrix")

            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                for d in rows[:max_rows_per_source]:
                    if isinstance(d, dict):
                        dd = dict(d)
                        dd["_Sheet"] = sheet
                        out_rows.append(dd)
                meta["items"].append({"sheet": sheet, "cached": True, "rows": len(rows), "dict_rows": True})
            else:
                matrix = rows_matrix if isinstance(rows_matrix, list) else rows if isinstance(rows, list) else []
                if isinstance(matrix, list) and matrix:
                    dicts = _rows_matrix_to_keyed_dicts(
                        sheet,
                        headers=headers if isinstance(headers, list) else [],
                        rows=matrix,
                        keys=keys if isinstance(keys, list) else None,
                        limit=max_rows_per_source,
                    )
                    out_rows.extend(dicts)
                    meta["items"].append({"sheet": sheet, "cached": True, "rows": len(matrix), "dict_rows": False})
                else:
                    meta["items"].append({"sheet": sheet, "cached": True, "rows": 0})

        if out_rows:
            meta["mode_used"] = "snapshot"
            return out_rows, meta, None

        if mode == AdvisorDataMode.SNAPSHOT:
            meta["mode_used"] = "snapshot"
            return [], meta, "No cached sheet snapshots found (or snapshots empty)"

    live_rows: List[Dict[str, Any]] = []
    live_items: List[Dict[str, Any]] = []
    for sheet in sources_norm:
        payload_rows, err = _engine_get_sheet_rows_payload(engine, sheet, limit=max_rows_per_source)
        if err is not None or not isinstance(payload_rows, dict):
            live_items.append({"sheet": sheet, "live": True, "ok": False, "error": err})
            continue

        headers = payload_rows.get("headers") or []
        keys = payload_rows.get("keys") or payload_rows.get("schema_keys") or None
        rows = payload_rows.get("rows")
        rows_matrix = payload_rows.get("rows_matrix")

        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            for d in rows[:max_rows_per_source]:
                if isinstance(d, dict):
                    dd = dict(d)
                    dd["_Sheet"] = sheet
                    live_rows.append(dd)
            live_items.append({"sheet": sheet, "live": True, "ok": True, "rows": len(rows), "dict_rows": True})
        else:
            matrix = rows_matrix if isinstance(rows_matrix, list) else rows if isinstance(rows, list) else []
            if isinstance(matrix, list) and matrix:
                dicts = _rows_matrix_to_keyed_dicts(
                    sheet,
                    headers=headers if isinstance(headers, list) else [],
                    rows=matrix,
                    keys=keys if isinstance(keys, list) else None,
                    limit=max_rows_per_source,
                )
                live_rows.extend(dicts)
                live_items.append({"sheet": sheet, "live": True, "ok": True, "rows": len(matrix), "dict_rows": False})
            else:
                live_items.append({"sheet": sheet, "live": True, "ok": True, "rows": 0})

    meta["items"] = live_items
    if not live_rows:
        meta["mode_used"] = "live_sheet"
        return [], meta, "Live sheet fetch returned no rows"

    if mode in (AdvisorDataMode.LIVE_QUOTES, AdvisorDataMode.AUTO):
        syms = _extract_symbols_from_rows(live_rows)
        qmap = _engine_get_quotes_batch(engine, syms)
        if qmap:
            enriched: List[Dict[str, Any]] = []
            for r in live_rows:
                sym = _norm_symbol(_safe_str(r.get("symbol") or r.get("ticker") or r.get("code") or ""))
                q = qmap.get(sym) or {}
                enriched.append(_merge_non_destructive(r, q if isinstance(q, dict) else {}))
            live_rows = enriched
            meta["live_quotes_enriched"] = True
            meta["live_quotes_count"] = len(qmap)
            meta["mode_used"] = "live_quotes"
        else:
            meta["live_quotes_enriched"] = False
            meta["mode_used"] = "live_sheet"
    else:
        meta["mode_used"] = "live_sheet"

    return live_rows, meta, None


# =============================================================================
# Domain dataclasses
# =============================================================================
@dataclass(slots=True)
class Security:
    symbol: str
    canonical: str
    name: str
    sheet: str
    market: str
    sector: str
    currency: str = "SAR"

    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    week_52_position_pct: Optional[float] = None

    volume: Optional[float] = None
    avg_vol_30d: Optional[float] = None
    value_traded: Optional[float] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None

    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    eps_ttm: Optional[float] = None
    forward_eps: Optional[float] = None
    dividend_yield: Optional[float] = None
    payout_ratio: Optional[float] = None
    beta: Optional[float] = None

    roe: Optional[float] = None
    roa: Optional[float] = None
    net_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None

    rsi_14: Optional[float] = None
    volatility_30d: Optional[float] = None
    max_drawdown_30d: Optional[float] = None

    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None

    valuation_score: Optional[float] = None
    momentum_score: Optional[float] = None
    quality_score: Optional[float] = None
    value_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    confidence_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None

    recommendation: str = "HOLD"
    recommendation_reason: str = ""

    risk_bucket: str = "Moderate"
    confidence_bucket: str = "Moderate"

    data_provider: str = ""
    data_quality: str = "FAIR"
    last_updated_utc: Optional[datetime] = None

    advisor_score: float = 0.0
    allocation_weight: float = 0.0
    allocation_amount: float = 0.0
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if self.last_updated_utc is not None:
            d["last_updated_utc"] = _iso_utc(self.last_updated_utc)
            d["last_updated_riyadh"] = _iso_riyadh(self.last_updated_utc)
        return {k: v for k, v in d.items() if v is not None}


@dataclass(slots=True)
class Portfolio:
    securities: List[Security] = field(default_factory=list)
    total_value: float = 0.0
    expected_return: float = 0.0
    expected_volatility: float = 0.0
    sharpe_ratio: float = 0.0
    concentration_score: float = 0.0
    sector_exposure: Dict[str, float] = field(default_factory=dict)
    currency_exposure: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_value": self.total_value,
            "expected_return": self.expected_return,
            "expected_volatility": self.expected_volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "concentration_score": self.concentration_score,
            "sector_exposure": self.sector_exposure,
            "currency_exposure": self.currency_exposure,
            "securities": [s.to_dict() for s in self.securities],
        }


@dataclass(slots=True)
class AdvisorRequest:
    sources: List[str] = field(default_factory=lambda: ["ALL"])
    tickers: Optional[List[str]] = None

    risk_profile: RiskProfile = RiskProfile.MODERATE
    risk_bucket: str = ""
    confidence_bucket: str = ""

    required_roi_1m: Optional[float] = None
    required_roi_3m: Optional[float] = None
    required_roi_12m: Optional[float] = None

    invest_amount: float = 0.0
    currency: str = "SAR"
    top_n: int = 20
    allocation_strategy: AllocationStrategy = AllocationStrategy.MAXIMUM_SHARPE

    min_price: Optional[float] = None
    max_price: Optional[float] = None
    exclude_sectors: Optional[List[str]] = None
    allowed_markets: Optional[List[str]] = None
    min_liquidity_score: float = 25.0
    min_advisor_score: Optional[float] = None

    max_position_pct: float = 0.35
    min_position_pct: float = 0.02
    max_sector_pct: float = 0.40
    max_currency_pct: float = 0.50

    use_row_updated_at: bool = True
    debug: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AdvisorRequest":
        sources = data.get("sources", ["ALL"])
        if isinstance(sources, str):
            sources = [sources]
        sources = [str(s).strip() for s in sources if str(s).strip()]
        if not sources:
            sources = ["ALL"]

        tickers = None
        tickers_in = data.get("tickers") or data.get("symbols")
        if tickers_in:
            if isinstance(tickers_in, str):
                tickers = [x for x in tickers_in.replace(",", " ").split() if x.strip()]
            elif isinstance(tickers_in, list):
                tickers = [str(x) for x in tickers_in if x]

        try:
            risk_profile = RiskProfile(str(data.get("risk_profile", "moderate")).lower())
        except Exception:
            risk_profile = RiskProfile.MODERATE

        try:
            allocation_strategy = AllocationStrategy(str(data.get("allocation_strategy", "maximum_sharpe")).lower())
        except Exception:
            allocation_strategy = AllocationStrategy.MAXIMUM_SHARPE

        return cls(
            sources=sources,
            tickers=tickers,
            risk_profile=risk_profile,
            risk_bucket=_safe_str(data.get("risk_bucket") or data.get("risk") or "").strip(),
            confidence_bucket=_safe_str(data.get("confidence_bucket") or data.get("confidence") or "").strip(),
            required_roi_1m=_as_ratio(data.get("required_roi_1m")),
            required_roi_3m=_as_ratio(data.get("required_roi_3m")),
            required_roi_12m=_as_ratio(data.get("required_roi_12m")),
            invest_amount=_to_float(data.get("invest_amount")) or 0.0,
            currency=_safe_str(data.get("currency", "SAR")).upper(),
            top_n=max(1, min(200, _to_int(data.get("top_n")) or 20)),
            allocation_strategy=allocation_strategy,
            min_price=_to_float(data.get("min_price")),
            max_price=_to_float(data.get("max_price")),
            exclude_sectors=data.get("exclude_sectors"),
            allowed_markets=data.get("allowed_markets"),
            min_liquidity_score=_to_float(data.get("min_liquidity_score")) or 25.0,
            min_advisor_score=_to_float(data.get("min_advisor_score")),
            max_position_pct=_as_ratio(data.get("max_position_pct")) or 0.35,
            min_position_pct=_as_ratio(data.get("min_position_pct")) or 0.02,
            max_sector_pct=_as_ratio(data.get("max_sector_pct")) or 0.40,
            max_currency_pct=_as_ratio(data.get("max_currency_pct")) or 0.50,
            use_row_updated_at=_truthy(data.get("use_row_updated_at", True)),
            debug=_truthy(data.get("debug", False)),
        )


# =============================================================================
# Liquidity + buckets
# =============================================================================
def _compute_liquidity_score(row: Dict[str, Any]) -> Optional[float]:
    vt = _to_float(_get_any(row, "value_traded", "traded_value"))
    if vt is not None and vt > 0:
        try:
            logv = math.log10(max(1.0, vt))
            score = ((logv - 6.0) / (11.0 - 6.0)) * 75.0 + 20.0
            return max(0.0, min(100.0, score))
        except Exception:
            return None

    vol = _to_float(_get_any(row, "volume"))
    if vol is not None and vol > 0:
        try:
            logv = math.log10(max(1.0, vol))
            score = ((logv - 5.0) / (9.0 - 5.0)) * 70.0 + 20.0
            return max(0.0, min(100.0, score))
        except Exception:
            return None

    mc = _to_float(_get_any(row, "market_cap"))
    if mc is not None and mc > 0:
        try:
            logm = math.log10(max(1.0, mc))
            score = ((logm - 7.0) / (12.0 - 7.0)) * 70.0 + 20.0
            return max(0.0, min(100.0, score))
        except Exception:
            return None

    return None


def _risk_bucket_from_risk_score(risk_score: Optional[float]) -> str:
    if risk_score is None:
        return "Moderate"
    r = float(risk_score)
    if r <= 25:
        return "Low"
    if r <= 50:
        return "Moderate"
    if r <= 75:
        return "High"
    return "Very High"


def _confidence_bucket_from_confidence_score(confidence_score: Optional[float]) -> str:
    if confidence_score is None:
        return "Moderate"
    c = float(confidence_score)
    if c >= 85:
        return "Very High"
    if c >= 70:
        return "High"
    if c >= 50:
        return "Moderate"
    if c >= 35:
        return "Low"
    return "Very Low"


# =============================================================================
# Extract Security from a schema-keyed row dict
# =============================================================================
def _extract_security(row: Dict[str, Any]) -> Optional[Security]:
    sym = _norm_symbol(_safe_str(_get_any(row, "symbol", "ticker", "code", "Symbol", "Ticker")))
    if not sym or sym == "SYMBOL":
        return None

    name = _safe_str(_get_any(row, "name", "company_name", "Name", "Company Name"))
    sheet = _safe_str(_get_any(row, "_Sheet", "origin", "sheet")) or ""
    market = _safe_str(_get_any(row, "market", "Market")) or ("KSA" if sym.endswith(".SR") or sym.isdigit() else "GLOBAL")
    sector = _safe_str(_get_any(row, "sector", "Sector")) or ""
    currency = _safe_str(_get_any(row, "currency", "Currency")) or "SAR"

    s = Security(
        symbol=sym,
        canonical=_canonical_symbol(sym),
        name=name,
        sheet=sheet,
        market=market,
        sector=sector,
        currency=currency,
    )

    s.current_price = _to_float(_get_any(row, "current_price", "price"))
    s.previous_close = _to_float(_get_any(row, "previous_close", "prev_close"))
    s.day_high = _to_float(_get_any(row, "day_high", "high"))
    s.day_low = _to_float(_get_any(row, "day_low", "low"))
    s.week_52_high = _to_float(_get_any(row, "week_52_high", "high_52w"))
    s.week_52_low = _to_float(_get_any(row, "week_52_low", "low_52w"))
    s.week_52_position_pct = _as_ratio(_get_any(row, "week_52_position_pct", "position_52w_pct"))

    s.volume = _to_float(_get_any(row, "volume"))
    s.avg_vol_30d = _to_float(_get_any(row, "avg_vol_30d", "avg_volume_30d"))
    s.value_traded = _to_float(_get_any(row, "value_traded"))
    s.market_cap = _to_float(_get_any(row, "market_cap"))
    s.shares_outstanding = _to_float(_get_any(row, "shares_outstanding"))
    s.free_float = _as_ratio(_get_any(row, "free_float"))

    s.pe_ttm = _to_float(_get_any(row, "pe_ttm", "pe_ratio"))
    s.forward_pe = _to_float(_get_any(row, "forward_pe"))
    s.pb = _to_float(_get_any(row, "pb"))
    s.ps = _to_float(_get_any(row, "ps"))
    s.eps_ttm = _to_float(_get_any(row, "eps_ttm"))
    s.forward_eps = _to_float(_get_any(row, "forward_eps"))
    s.dividend_yield = _as_ratio(_get_any(row, "dividend_yield"))
    s.payout_ratio = _as_ratio(_get_any(row, "payout_ratio"))
    s.beta = _to_float(_get_any(row, "beta"))

    s.roe = _as_ratio(_get_any(row, "roe"))
    s.roa = _as_ratio(_get_any(row, "roa"))
    s.net_margin = _as_ratio(_get_any(row, "net_margin"))
    s.ebitda_margin = _as_ratio(_get_any(row, "ebitda_margin"))
    s.revenue_growth = _as_ratio(_get_any(row, "revenue_growth"))
    s.net_income_growth = _as_ratio(_get_any(row, "net_income_growth"))

    s.rsi_14 = _to_float(_get_any(row, "rsi_14"))
    s.volatility_30d = _as_ratio(_get_any(row, "volatility_30d"))
    s.max_drawdown_30d = _as_ratio(_get_any(row, "max_drawdown_30d"))

    s.forecast_price_1m = _to_float(_get_any(row, "forecast_price_1m"))
    s.forecast_price_3m = _to_float(_get_any(row, "forecast_price_3m"))
    s.forecast_price_12m = _to_float(_get_any(row, "forecast_price_12m"))

    s.expected_roi_1m = _as_ratio(_get_any(row, "expected_roi_1m"))
    s.expected_roi_3m = _as_ratio(_get_any(row, "expected_roi_3m"))
    s.expected_roi_12m = _as_ratio(_get_any(row, "expected_roi_12m"))
    s.forecast_confidence = _as_ratio(_get_any(row, "forecast_confidence"))

    s.data_provider = _safe_str(_get_any(row, "data_provider", "provider"))
    s.data_quality = _safe_str(_get_any(row, "data_quality", "Data Quality")) or "FAIR"

    dt = _parse_iso_to_dt(_get_any(row, "last_updated_utc", "Last Updated (UTC)", "updated_at_utc"))
    s.last_updated_utc = dt

    return s


# =============================================================================
# Recommendation explanation synthesis
# =============================================================================
def _build_recommendation_reason(
    security: Security,
    *,
    recommendation: Optional[str] = None,
    upstream_reason: Any = None,
    liquidity_score: Optional[float] = None,
) -> str:
    upstream = _clean_reason_text(upstream_reason)
    if upstream:
        return upstream

    rec = _safe_str(recommendation or security.recommendation or "HOLD").upper()
    parts: List[str] = []

    roi_3m = security.expected_roi_3m
    roi_1m = security.expected_roi_1m
    conf = security.confidence_score
    risk = security.risk_score
    overall = security.overall_score
    value = security.value_score
    quality = security.quality_score
    valuation = security.valuation_score
    momentum = security.momentum_score
    opportunity = security.opportunity_score
    price = security.current_price
    fp3 = security.forecast_price_3m

    if rec == "BUY":
        if roi_3m is not None and roi_3m > 0:
            parts.append(f"positive 3M upside ({_fmt_pct_from_ratio(roi_3m)})")
        elif roi_1m is not None and roi_1m > 0:
            parts.append(f"positive 1M upside ({_fmt_pct_from_ratio(roi_1m)})")

        if conf is not None and conf >= 70:
            parts.append(f"high confidence ({_fmt_num(conf, 0)}/100)")
        if overall is not None and overall >= 65:
            parts.append(f"strong overall score ({_fmt_num(overall, 0)}/100)")
        if valuation is not None and valuation >= 60:
            parts.append(f"supportive valuation ({_fmt_num(valuation, 0)}/100)")
        if quality is not None and quality >= 60:
            parts.append(f"good quality profile ({_fmt_num(quality, 0)}/100)")
        if momentum is not None and momentum >= 60:
            parts.append(f"positive momentum ({_fmt_num(momentum, 0)}/100)")
        if opportunity is not None and opportunity >= 60:
            parts.append(f"opportunity score is favorable ({_fmt_num(opportunity, 0)}/100)")
        if risk is not None and risk <= 45:
            parts.append(f"contained risk ({_fmt_num(risk, 0)}/100)")
        if price is not None and fp3 is not None and fp3 > price:
            parts.append(f"forecast price is above current price ({_fmt_num(fp3, 2)} vs {_fmt_num(price, 2)})")

    elif rec == "SELL":
        if roi_3m is not None and roi_3m < 0:
            parts.append(f"negative 3M outlook ({_fmt_pct_from_ratio(roi_3m)})")
        elif roi_1m is not None and roi_1m < 0:
            parts.append(f"negative 1M outlook ({_fmt_pct_from_ratio(roi_1m)})")

        if risk is not None and risk >= 65:
            parts.append(f"elevated risk ({_fmt_num(risk, 0)}/100)")
        if overall is not None and overall <= 40:
            parts.append(f"weak overall score ({_fmt_num(overall, 0)}/100)")
        if valuation is not None and valuation <= 40:
            parts.append(f"valuation is not supportive ({_fmt_num(valuation, 0)}/100)")
        if momentum is not None and momentum <= 40:
            parts.append(f"weak momentum ({_fmt_num(momentum, 0)}/100)")
        if confidence is not None and conf is not None and conf <= 40:
            parts.append(f"low confidence ({_fmt_num(conf, 0)}/100)")
        if price is not None and fp3 is not None and fp3 < price:
            parts.append(f"forecast price is below current price ({_fmt_num(fp3, 2)} vs {_fmt_num(price, 2)})")

    else:
        if overall is not None:
            if 45 <= overall <= 65:
                parts.append(f"balanced overall score ({_fmt_num(overall, 0)}/100)")
            elif overall > 65:
                parts.append(f"good score but not strong enough for upgrade ({_fmt_num(overall, 0)}/100)")
            elif overall < 45:
                parts.append(f"weaker score but not enough for downgrade ({_fmt_num(overall, 0)}/100)")

        if roi_3m is not None:
            if abs(roi_3m) <= 0.03:
                parts.append(f"limited expected 3M move ({_fmt_pct_from_ratio(roi_3m)})")
            elif roi_3m > 0:
                parts.append(f"upside exists but is moderate ({_fmt_pct_from_ratio(roi_3m)})")
            else:
                parts.append(f"downside exists but remains moderate ({_fmt_pct_from_ratio(roi_3m)})")

        if conf is not None and 45 <= conf <= 69:
            parts.append(f"moderate confidence ({_fmt_num(conf, 0)}/100)")
        if risk is not None and 40 <= risk <= 60:
            parts.append(f"risk is moderate ({_fmt_num(risk, 0)}/100)")

    if liquidity_score is not None:
        if liquidity_score >= 65:
            parts.append(f"healthy liquidity ({_fmt_num(liquidity_score, 0)}/100)")
        elif liquidity_score <= 25:
            parts.append(f"liquidity is weak ({_fmt_num(liquidity_score, 0)}/100)")

    if value is not None and value >= 65 and "supportive valuation" not in " | ".join(parts):
        parts.append(f"value score is supportive ({_fmt_num(value, 0)}/100)")

    if not parts:
        if rec == "BUY":
            return "BUY because the combined score profile is favorable versus current risk."
        if rec == "SELL":
            return "SELL because the combined score profile is unfavorable versus expected return."
        return "HOLD because the current risk-return profile is balanced."

    sentence = f"{rec} because " + ", ".join(parts[:4]) + "."
    return sentence


# =============================================================================
# Filtering + advisor score
# =============================================================================
def _passes_filters(s: Security, req: AdvisorRequest, liquidity_score: Optional[float]) -> Tuple[bool, str]:
    if req.allowed_markets:
        allowed = {str(m).upper() for m in req.allowed_markets if m}
        if s.market.upper() not in allowed:
            return False, f"Market excluded: {s.market}"

    if req.min_price is not None and s.current_price is not None and s.current_price < req.min_price:
        return False, "Price too low"
    if req.max_price is not None and s.current_price is not None and s.current_price > req.max_price:
        return False, "Price too high"

    if req.exclude_sectors and s.sector:
        for excl in req.exclude_sectors:
            if excl and excl.lower() in s.sector.lower():
                return False, "Sector excluded"

    if liquidity_score is not None and liquidity_score < float(req.min_liquidity_score or 0.0):
        return False, "Liquidity too low"

    if req.required_roi_1m is not None and (s.expected_roi_1m is None or s.expected_roi_1m < req.required_roi_1m):
        return False, "ROI 1M below target"
    if req.required_roi_3m is not None and (s.expected_roi_3m is None or s.expected_roi_3m < req.required_roi_3m):
        return False, "ROI 3M below target"
    if req.required_roi_12m is not None and (s.expected_roi_12m is None or s.expected_roi_12m < req.required_roi_12m):
        return False, "ROI 12M below target"

    if req.min_advisor_score is not None and s.advisor_score < req.min_advisor_score:
        return False, "Advisor score too low"

    return True, ""


def _compute_advisor_score(s: Security, liq: Optional[float]) -> Tuple[float, str]:
    reasons: List[str] = []
    base = s.overall_score if s.overall_score is not None else 50.0
    score = float(base)
    reasons.append(f"Overall:{score:.0f}")

    roi3 = s.expected_roi_3m
    if roi3 is not None:
        bump = max(0.0, min(18.0, roi3 * 100.0 * 0.45))
        score += bump
        if bump >= 3:
            reasons.append(f"ROI3M:+{bump:.0f}")

    conf = s.confidence_score if s.confidence_score is not None else 50.0
    cb = max(0.0, min(12.0, (conf - 50.0) * 0.18))
    score += cb
    if cb >= 2:
        reasons.append("Conf+")

    risk = s.risk_score if s.risk_score is not None else 50.0
    rp = max(0.0, min(20.0, (risk - 45.0) * 0.35))
    score -= rp
    if rp >= 3:
        reasons.append("Risk-")

    if liq is not None:
        lb = max(-3.0, min(4.0, (liq - 50.0) * 0.06))
        score += lb
        if lb >= 2:
            reasons.append("Liq+")

    score = max(0.0, min(100.0, score))
    return score, "; ".join(reasons[:6])


def _deduplicate_securities(securities: List[Security]) -> Tuple[List[Security], int]:
    best: Dict[str, Security] = {}
    removed = 0

    def key_score(x: Security) -> Tuple[float, float, float]:
        return (float(x.advisor_score or 0.0), float(x.expected_roi_3m or 0.0), float(x.confidence_score or 0.0))

    for s in securities:
        k = s.canonical or s.symbol
        ex = best.get(k)
        if ex is None or key_score(s) > key_score(ex):
            if ex is not None:
                removed += 1
            best[k] = s
        else:
            removed += 1

    return list(best.values()), removed


# =============================================================================
# Portfolio optimization (safe fallbacks)
# =============================================================================
class PortfolioOptimizer:
    def __init__(self, risk_free_rate: float = 0.04):
        self.risk_free_rate = float(risk_free_rate)

    def _returns(self, securities: List[Security]) -> List[float]:
        out = []
        for s in securities:
            r = s.expected_roi_12m if s.expected_roi_12m is not None else (s.expected_roi_3m if s.expected_roi_3m is not None else 0.05)
            out.append(float(r))
        return out

    def _vols(self, securities: List[Security]) -> List[float]:
        out = []
        for s in securities:
            v = s.volatility_30d
            if v is None:
                rb = s.risk_bucket.lower()
                v = 0.12 if "low" in rb else 0.18 if "moderate" in rb else 0.26
            out.append(float(v))
        return out

    def _corr(self, securities: List[Security]) -> Optional[Any]:
        if not HAS_NUMPY:
            return None
        n = len(securities)
        corr = np.eye(n)  # type: ignore
        for i in range(n):
            for j in range(i + 1, n):
                same_sector = bool(securities[i].sector) and securities[i].sector == securities[j].sector
                corr[i, j] = corr[j, i] = 0.70 if same_sector else 0.30
        return corr

    def optimize_equal_weight(self, securities: List[Security]) -> List[float]:
        n = len(securities)
        return [1.0 / n] * n if n > 0 else []

    def optimize_market_cap(self, securities: List[Security]) -> List[float]:
        caps = [float(s.market_cap or 0.0) for s in securities]
        total = sum(caps)
        if total <= 0:
            return self.optimize_equal_weight(securities)
        return [c / total for c in caps]

    def optimize_minimum_variance(self, securities: List[Security]) -> List[float]:
        if not (HAS_NUMPY and HAS_SCIPY) or len(securities) < 2:
            return self.optimize_equal_weight(securities)

        n = len(securities)
        vols = np.array(self._vols(securities))  # type: ignore
        corr = self._corr(securities)
        if corr is None:
            return self.optimize_equal_weight(securities)
        cov = np.outer(vols, vols) * corr  # type: ignore

        x0 = np.array([1.0 / n] * n)  # type: ignore
        bounds = [(0.0, 1.0)] * n
        cons = [{"type": "eq", "fun": lambda w: float(np.sum(w) - 1.0)}]  # type: ignore

        def obj(w):
            return float(np.dot(w.T, np.dot(cov, w)))  # type: ignore

        res = optimize.minimize(obj, x0, method="SLSQP", bounds=bounds, constraints=cons)  # type: ignore
        return res.x.tolist() if getattr(res, "success", False) else self.optimize_equal_weight(securities)

    def optimize_maximum_sharpe(self, securities: List[Security]) -> List[float]:
        if not (HAS_NUMPY and HAS_SCIPY) or len(securities) < 2:
            return self.optimize_equal_weight(securities)

        n = len(securities)
        rets = np.array(self._returns(securities))  # type: ignore
        vols = np.array(self._vols(securities))  # type: ignore
        corr = self._corr(securities)
        if corr is None:
            return self.optimize_equal_weight(securities)
        cov = np.outer(vols, vols) * corr  # type: ignore

        x0 = np.array([1.0 / n] * n)  # type: ignore
        bounds = [(0.0, 1.0)] * n
        cons = [{"type": "eq", "fun": lambda w: float(np.sum(w) - 1.0)}]  # type: ignore

        def neg_sharpe(w):
            vol = float(np.sqrt(np.dot(w.T, np.dot(cov, w))))  # type: ignore
            if vol <= 0:
                return 0.0
            ret = float(np.sum(rets * w))  # type: ignore
            return -((ret - self.risk_free_rate) / vol)

        res = optimize.minimize(neg_sharpe, x0, method="SLSQP", bounds=bounds, constraints=cons)  # type: ignore
        return res.x.tolist() if getattr(res, "success", False) else self.optimize_equal_weight(securities)

    def optimize_risk_parity(self, securities: List[Security]) -> List[float]:
        if not (HAS_NUMPY and HAS_SCIPY) or len(securities) < 2:
            return self.optimize_equal_weight(securities)

        n = len(securities)
        vols = np.array(self._vols(securities))  # type: ignore
        corr = self._corr(securities)
        if corr is None:
            return self.optimize_equal_weight(securities)
        cov = np.outer(vols, vols) * corr  # type: ignore

        x0 = np.array([1.0 / n] * n)  # type: ignore
        bounds = [(0.01, 1.0)] * n
        cons = [{"type": "eq", "fun": lambda w: float(np.sum(w) - 1.0)}]  # type: ignore

        def obj(w):
            port_var = float(np.dot(w.T, np.dot(cov, w)))  # type: ignore
            if port_var <= 0:
                return 1e6
            mrc = (w * np.dot(cov, w)) / port_var  # type: ignore
            tgt = 1.0 / n
            return float(np.sum((mrc - tgt) ** 2))  # type: ignore

        res = optimize.minimize(obj, x0, method="SLSQP", bounds=bounds, constraints=cons)  # type: ignore
        return res.x.tolist() if getattr(res, "success", False) else self.optimize_equal_weight(securities)

    def optimize_black_litterman(self, securities: List[Security]) -> List[float]:
        if not (HAS_NUMPY and HAS_SCIPY) or len(securities) < 2:
            return self.optimize_maximum_sharpe(securities)

        n = len(securities)
        caps = np.array([float(s.market_cap or 1e9) for s in securities])  # type: ignore
        w_mkt = caps / float(np.sum(caps))  # type: ignore

        vols = np.array(self._vols(securities))  # type: ignore
        corr = self._corr(securities)
        if corr is None:
            return w_mkt.tolist()  # type: ignore
        cov = np.outer(vols, vols) * corr  # type: ignore

        delta = 2.5
        pi = delta * np.dot(cov, w_mkt)  # type: ignore

        P = np.eye(n)  # type: ignore
        scores = np.array([float(s.advisor_score or 50.0) for s in securities])  # type: ignore
        Q = np.clip((scores - 50.0) / 100.0, 0.0, 0.25)  # type: ignore

        omega_diag = np.array([(0.12 / (float(s.advisor_score or 50.0) + 1.0)) ** 2 for s in securities])  # type: ignore
        Omega = np.diag(omega_diag)  # type: ignore

        tau = 0.05
        try:
            tau_cov_inv = np.linalg.inv(tau * cov)  # type: ignore
            omega_inv = np.linalg.inv(Omega)  # type: ignore
            mid = np.linalg.inv(tau_cov_inv + np.dot(P.T, np.dot(omega_inv, P)))  # type: ignore
            rhs = np.dot(tau_cov_inv, pi) + np.dot(P.T, np.dot(omega_inv, Q))  # type: ignore
            bl_ret = np.dot(mid, rhs)  # type: ignore
        except Exception:
            bl_ret = pi  # type: ignore

        bounds = [(0.0, 1.0)] * n
        cons = [{"type": "eq", "fun": lambda w: float(np.sum(w) - 1.0)}]  # type: ignore

        def neg_sharpe(w):
            vol = float(np.sqrt(np.dot(w.T, np.dot(cov, w))))  # type: ignore
            if vol <= 0:
                return 0.0
            ret = float(np.sum(bl_ret * w))  # type: ignore
            return -((ret - self.risk_free_rate) / vol)

        res = optimize.minimize(neg_sharpe, w_mkt, method="SLSQP", bounds=bounds, constraints=cons)  # type: ignore
        return res.x.tolist() if getattr(res, "success", False) else w_mkt.tolist()  # type: ignore

    def optimize(self, securities: List[Security], strategy: AllocationStrategy) -> List[float]:
        if not securities:
            return []
        if strategy == AllocationStrategy.EQUAL_WEIGHT:
            return self.optimize_equal_weight(securities)
        if strategy == AllocationStrategy.MARKET_CAP:
            return self.optimize_market_cap(securities)
        if strategy == AllocationStrategy.MINIMUM_VARIANCE:
            return self.optimize_minimum_variance(securities)
        if strategy == AllocationStrategy.MAXIMUM_SHARPE:
            return self.optimize_maximum_sharpe(securities)
        if strategy == AllocationStrategy.RISK_PARITY:
            return self.optimize_risk_parity(securities)
        if strategy == AllocationStrategy.BLACK_LITTERMAN:
            return self.optimize_black_litterman(securities)
        return self.optimize_equal_weight(securities)


def _apply_weight_constraints(weights: List[float], *, min_w: float, max_w: float) -> List[float]:
    if not weights:
        return []
    w = [max(min_w, min(max_w, float(x))) for x in weights]
    s = sum(w)
    if s <= 0:
        return []
    return [x / s for x in w]


def _analyze_portfolio(securities: List[Security], weights: List[float], total_value: float) -> Portfolio:
    p = Portfolio(securities=securities, total_value=float(total_value or 0.0))
    if not securities or not weights or len(securities) != len(weights):
        return p

    sec_exp: Dict[str, float] = defaultdict(float)
    ccy_exp: Dict[str, float] = defaultdict(float)
    for s, w in zip(securities, weights):
        if s.sector:
            sec_exp[s.sector] += float(w)
        if s.currency:
            ccy_exp[s.currency] += float(w)
    p.sector_exposure = dict(sec_exp)
    p.currency_exposure = dict(ccy_exp)
    p.concentration_score = float(sum(float(w) * float(w) for w in weights))

    rets = []
    vols = []
    for s in securities:
        r = s.expected_roi_3m if s.expected_roi_3m is not None else 0.05
        v = s.volatility_30d if s.volatility_30d is not None else 0.18
        rets.append(float(r))
        vols.append(float(v))

    p.expected_return = float(sum(r * w for r, w in zip(rets, weights)))
    p.expected_volatility = float(math.sqrt(sum((v * w) ** 2 for v, w in zip(vols, weights))))
    if p.expected_volatility > 0:
        p.sharpe_ratio = (p.expected_return - 0.02) / p.expected_volatility

    return p


# =============================================================================
# Main entry
# =============================================================================
def run_investment_advisor(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    """
    Returns:
      {"headers": [...], "rows": [...], "items": [...], "meta": {...}}
    """
    with tracer.start_as_current_span("run_investment_advisor") as span:
        start_time = time.time()

        headers = [
            "Rank",
            "Symbol",
            "Origin",
            "Name",
            "Market",
            "Sector",
            "Currency",
            "Price",
            "Advisor Score",
            "Recommendation",
            "Allocation %",
            "Allocation Amount",
            "Forecast Price (1M)",
            "Expected ROI % (1M)",
            "Forecast Price (3M)",
            "Expected ROI % (3M)",
            "Forecast Price (12M)",
            "Expected ROI % (12M)",
            "Risk Bucket",
            "Confidence Bucket",
            "Liquidity Score",
            "Data Quality",
            "Reason",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
        ]

        try:
            req = AdvisorRequest.from_dict(payload or {})
            mode = _parse_advisor_data_mode(payload or {})

            span.set_attribute("top_n", req.top_n)
            span.set_attribute("strategy", req.allocation_strategy.value)
            span.set_attribute("advisor_data_mode", mode.value)

            if engine is None:
                return {
                    "headers": headers,
                    "rows": [],
                    "items": [],
                    "meta": {
                        "ok": False,
                        "version": ADVISOR_VERSION,
                        "error": "Missing engine instance",
                        "advisor_data_mode": mode.value,
                        "runtime_ms": int((time.time() - start_time) * 1000),
                    },
                }

            universe_rows, fetch_meta, fetch_err = _fetch_universe(
                req.sources,
                engine=engine,
                payload=payload or {},
                max_rows_per_source=5000,
            )

            ticker_variants: Set[str] = set()
            ticker_canon: Set[str] = set()
            if req.tickers:
                for t in req.tickers:
                    ticker_variants.update(_symbol_variants(t))
                    c = _canonical_symbol(t)
                    if c:
                        ticker_canon.add(c)

            securities: List[Security] = []
            dropped = {"no_symbol": 0, "invalid_data": 0, "filtered": 0, "ticker_not_requested": 0}

            for r in universe_rows:
                try:
                    s = _extract_security(r)
                    if s is None:
                        dropped["no_symbol"] += 1
                        continue
                    if ticker_variants and not (s.symbol in ticker_variants or s.canonical in ticker_canon):
                        dropped["ticker_not_requested"] += 1
                        continue

                    score_patch = _compute_scores(r, settings=None)
                    s.valuation_score = _to_float(score_patch.get("valuation_score"))
                    s.momentum_score = _to_float(score_patch.get("momentum_score"))
                    s.quality_score = _to_float(score_patch.get("quality_score"))
                    s.value_score = _to_float(score_patch.get("value_score"))
                    s.opportunity_score = _to_float(score_patch.get("opportunity_score"))
                    s.confidence_score = _to_float(score_patch.get("confidence_score"))
                    s.risk_score = _to_float(score_patch.get("risk_score"))
                    s.overall_score = _to_float(score_patch.get("overall_score"))
                    s.recommendation = _safe_str(score_patch.get("recommendation") or "HOLD").upper()

                    liq = _compute_liquidity_score(r)

                    s.recommendation_reason = _build_recommendation_reason(
                        s,
                        recommendation=s.recommendation,
                        upstream_reason=score_patch.get("recommendation_reason"),
                        liquidity_score=liq,
                    )

                    s.risk_bucket = _risk_bucket_from_risk_score(s.risk_score)
                    s.confidence_bucket = _confidence_bucket_from_confidence_score(s.confidence_score)

                    s.advisor_score, s.reason = _compute_advisor_score(s, liq)

                    ok, _why = _passes_filters(s, req, liq)
                    if not ok:
                        dropped["filtered"] += 1
                        continue

                    securities.append(s)

                except Exception as e:
                    dropped["invalid_data"] += 1
                    if req.debug:
                        logger.debug("row error: %s", e)

            if not securities:
                advisor_requests_total.labels(status="error", strategy=req.allocation_strategy.value).inc()
                return {
                    "headers": headers,
                    "rows": [],
                    "items": [],
                    "meta": {
                        "ok": False,
                        "version": ADVISOR_VERSION,
                        "advisor_data_mode": mode.value,
                        "error": "No valid securities after filters",
                        "fetch": fetch_meta,
                        "warning": fetch_err,
                        "dropped": dropped,
                        "runtime_ms": int((time.time() - start_time) * 1000),
                    },
                }

            securities, dedupe_removed = _deduplicate_securities(securities)

            securities.sort(
                key=lambda x: (float(x.advisor_score or 0.0), float(x.expected_roi_3m or 0.0), float(x.confidence_score or 0.0)),
                reverse=True,
            )
            top = securities[: req.top_n]

            opt = PortfolioOptimizer()
            opt_start = time.time()
            weights = opt.optimize(top, req.allocation_strategy)
            advisor_optimization_duration.labels(strategy=req.allocation_strategy.value).observe(time.time() - opt_start)

            if not weights or len(weights) != len(top):
                weights = opt.optimize_equal_weight(top)

            weights = _apply_weight_constraints(weights, min_w=float(req.min_position_pct), max_w=float(req.max_position_pct))
            if not weights or len(weights) != len(top):
                weights = opt.optimize_equal_weight(top)

            total_amt = float(req.invest_amount or 0.0)
            for s, w in zip(top, weights):
                s.allocation_weight = float(w)
                s.allocation_amount = float(total_amt * float(w))

            portfolio = _analyze_portfolio(top, weights, total_amt)

            rows: List[List[Any]] = []
            items: List[Dict[str, Any]] = []

            as_of_dt = _now_utc()
            for i, s in enumerate(top, 1):
                dt_used = s.last_updated_utc if (req.use_row_updated_at and s.last_updated_utc) else as_of_dt
                liq_score = _compute_liquidity_score({"value_traded": s.value_traded, "volume": s.volume, "market_cap": s.market_cap}) or 0.0

                rows.append(
                    [
                        i,
                        s.symbol,
                        s.sheet,
                        s.name,
                        s.market,
                        s.sector,
                        s.currency,
                        round(s.current_price, 6) if s.current_price is not None else None,
                        round(s.advisor_score, 2),
                        s.recommendation,
                        round(s.allocation_weight * 100.0, 2),
                        round(s.allocation_amount, 2),
                        s.forecast_price_1m,
                        round((s.expected_roi_1m or 0.0) * 100.0, 2) if s.expected_roi_1m is not None else None,
                        s.forecast_price_3m,
                        round((s.expected_roi_3m or 0.0) * 100.0, 2) if s.expected_roi_3m is not None else None,
                        s.forecast_price_12m,
                        round((s.expected_roi_12m or 0.0) * 100.0, 2) if s.expected_roi_12m is not None else None,
                        s.risk_bucket,
                        s.confidence_bucket,
                        round(liq_score, 2),
                        s.data_quality,
                        s.reason,
                        _iso_utc(dt_used),
                        _iso_riyadh(dt_used),
                    ]
                )

                items.append(
                    {
                        "rank": i,
                        "symbol": s.symbol,
                        "canonical": s.canonical,
                        "origin": s.sheet,
                        "name": s.name,
                        "market": s.market,
                        "sector": s.sector,
                        "currency": s.currency,
                        "current_price": s.current_price,
                        "advisor_score": round(s.advisor_score, 2),
                        "recommendation": s.recommendation,
                        "recommendation_reason": s.recommendation_reason,
                        "allocation_pct": round(s.allocation_weight * 100.0, 2),
                        "allocation_amount": round(s.allocation_amount, 2),
                        "forecast_price_1m": s.forecast_price_1m,
                        "expected_roi_1m": s.expected_roi_1m,
                        "forecast_price_3m": s.forecast_price_3m,
                        "expected_roi_3m": s.expected_roi_3m,
                        "forecast_price_12m": s.forecast_price_12m,
                        "expected_roi_12m": s.expected_roi_12m,
                        "risk_score": s.risk_score,
                        "risk_bucket": s.risk_bucket,
                        "confidence_score": s.confidence_score,
                        "confidence_bucket": s.confidence_bucket,
                        "data_quality": s.data_quality,
                        "last_updated_utc": _iso_utc(dt_used),
                        "last_updated_riyadh": _iso_riyadh(dt_used),
                        "reason": s.reason,
                    }
                )

            meta: Dict[str, Any] = {
                "ok": True,
                "version": ADVISOR_VERSION,
                "advisor_data_mode": mode.value,
                "request": {
                    "sources": _normalize_source_pages(req.sources),
                    "tickers": req.tickers,
                    "risk_profile": req.risk_profile.value,
                    "top_n": req.top_n,
                    "allocation_strategy": req.allocation_strategy.value,
                    "invest_amount": req.invest_amount,
                },
                "fetch": fetch_meta,
                "counts": {
                    "universe_rows": len(universe_rows),
                    "securities_after_filters": len(securities),
                    "dedupe_removed": dedupe_removed,
                    "returned": len(rows),
                    "dropped": dropped,
                },
                "portfolio": portfolio.to_dict(),
                "schema": {
                    "page_catalog": bool(_HAS_PAGE_CATALOG),
                    "schema_registry": bool(_HAS_SCHEMA),
                    "scoring_module": bool(_HAS_SCORING),
                    "numpy": bool(HAS_NUMPY),
                    "scipy": bool(HAS_SCIPY),
                },
                "warning": fetch_err,
                "runtime_ms": int((time.time() - start_time) * 1000),
            }

            advisor_requests_total.labels(status="success", strategy=req.allocation_strategy.value).inc()
            return {"headers": headers, "rows": rows, "items": items, "meta": meta}

        except Exception as e:
            advisor_requests_total.labels(status="error", strategy=str(payload.get("allocation_strategy", "unknown"))).inc()
            meta = {
                "ok": False,
                "version": ADVISOR_VERSION,
                "advisor_data_mode": _parse_advisor_data_mode(payload or {}).value,
                "error": str(e),
                "runtime_ms": int((time.time() - start_time) * 1000),
            }
            if _truthy((payload or {}).get("debug", False)):
                meta["traceback"] = traceback.format_exc()
            return {"headers": headers, "rows": [], "items": [], "meta": meta}


__all__ = [
    "run_investment_advisor",
    "ADVISOR_VERSION",
    "RiskProfile",
    "AllocationStrategy",
    "AdvisorDataMode",
    "Security",
    "Portfolio",
    "AdvisorRequest",
]
