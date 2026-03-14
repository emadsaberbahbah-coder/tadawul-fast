#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/investment_advisor_engine.py
================================================================================
Investment Advisor Engine — v5.1.0
================================================================================
CANONICAL | RUNNER-EXPORT SAFE | ROUTE-COMPATIBLE | HORIZON-AWARE
ADAPTER-READY | FALLBACK-ADVISOR SAFE | IMPORT-SAFE

Why this revision
-----------------
- ✅ FIX: exports module-level runner names expected by route layers:
      - run_investment_advisor
      - run_advisor
      - execute_investment_advisor
      - execute_advisor
- ✅ FIX: exports create_engine_adapter(...) expected by routes/advisor.py.
- ✅ FIX: adds warm_cache / warm_snapshots / preload_snapshots / build_snapshot_cache.
- ✅ FIX: supports broader input contracts:
      - payload / body / request_data / criteria / params
      - symbols / tickers / direct_symbols
      - risk_level / risk_profile
      - confidence_level / confidence_bucket
      - investment_period_days / horizon_days / invest_period_days
      - min_expected_roi / min_roi
      - top_n / limit
- ✅ FIX: provides a real fallback advisor path when core.investment_advisor.py
      is unavailable or returns invalid output.
- ✅ FIX: get_sheet_rows(...) can now return Top_10_Investments-style payload.
- ✅ FIX: canonical enrichment always attempts to populate:
      - recommendation_reason
      - horizon_days
      - invest_period_label
- ✅ SAFE: no network calls at import-time.
- ✅ SAFE: tolerant to repo differences and method naming differences.

Expected usage
--------------
- from core.investment_advisor_engine import InvestmentAdvisorEngine
- engine = InvestmentAdvisorEngine(data_engine=..., quote_engine=...)
- result = engine.run(payload)

Primary public methods
----------------------
- run(payload)
- run_async(payload)
- get_sheet_rows(...)
- get_cached_sheet_snapshot(...)
- get_cached_multi_sheet_snapshots(...)
- get_enriched_quotes_batch(...)
- warm_cache(...)
- warm_snapshots(...)
- preload_snapshots(...)
- build_snapshot_cache(...)

Module-level exports expected by routers
----------------------------------------
- run_investment_advisor(...)
- run_advisor(...)
- execute_investment_advisor(...)
- execute_advisor(...)
- create_engine_adapter(...)
================================================================================
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
import math
import os
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

# -----------------------------------------------------------------------------
# Optional high-performance JSON
# -----------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj, default=str).decode("utf-8")

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

except Exception:

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False, default=str)

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)


logger = logging.getLogger("core.investment_advisor_engine")

RIYADH_TZ = timezone(timedelta(hours=3))
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "ok", "enabled", "active"}

TOP10_PAGE_NAME = "Top_10_Investments"

DEFAULT_SOURCE_PAGES = [
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
]

DERIVED_OR_NON_SOURCE_PAGES = {
    "KSA_TADAWUL",
    "Advisor_Criteria",
    "AI_Opportunity_Report",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
}

_TOP10_FALLBACK_HEADERS = [
    "Symbol",
    "Name",
    "Current Price",
    "Forecast Price 1M",
    "Forecast Price 3M",
    "Forecast Price 12M",
    "Expected ROI 1M",
    "Expected ROI 3M",
    "Expected ROI 12M",
    "Forecast Confidence",
    "Confidence Score",
    "Confidence Bucket",
    "Risk Score",
    "Risk Bucket",
    "Overall Score",
    "Opportunity Score",
    "Recommendation",
    "Recommendation Reason",
    "Horizon Days",
    "Invest Period Label",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Top10 Rank",
    "Selection Reason",
    "Criteria Snapshot",
]

_TOP10_FALLBACK_KEYS = [
    "symbol",
    "name",
    "current_price",
    "forecast_price_1m",
    "forecast_price_3m",
    "forecast_price_12m",
    "expected_roi_1m",
    "expected_roi_3m",
    "expected_roi_12m",
    "forecast_confidence",
    "confidence_score",
    "confidence_bucket",
    "risk_score",
    "risk_bucket",
    "overall_score",
    "opportunity_score",
    "recommendation",
    "recommendation_reason",
    "horizon_days",
    "invest_period_label",
    "last_updated_utc",
    "last_updated_riyadh",
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
]


# -----------------------------------------------------------------------------
# Optional imports
# -----------------------------------------------------------------------------
try:
    from core.sheets.page_catalog import normalize_page_name as _normalize_page_name  # type: ignore

    _HAS_PAGE_CATALOG = True
except Exception:
    _HAS_PAGE_CATALOG = False

    def _normalize_page_name(name: str, allow_output_pages: bool = True) -> str:  # type: ignore
        return str(name or "").strip()


try:
    from core.sheets.schema_registry import get_sheet_spec as _get_sheet_spec  # type: ignore

    _HAS_SCHEMA = True
except Exception:
    _HAS_SCHEMA = False

    def _get_sheet_spec(sheet_name: str) -> Any:  # type: ignore
        raise KeyError("schema_registry unavailable")


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in _TRUTHY


def _safe_str(v: Any, default: str = "") -> str:
    try:
        if v is None:
            return default
        s = str(v).strip()
        return default if s.lower() in {"none", "null", "nil"} else s
    except Exception:
        return default


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            f = float(v)
        else:
            s = str(v).strip().replace(",", "")
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


def _to_int(v: Any) -> Optional[int]:
    f = _to_float(v)
    return int(f) if f is not None else None


def _as_ratio(v: Any) -> Optional[float]:
    f = _to_float(v)
    if f is None:
        return None
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _norm_key(s: Any) -> str:
    return " ".join(_safe_str(s).lower().split())


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


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _iso_riyadh(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(RIYADH_TZ).isoformat()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_dt(v: Any) -> Optional[datetime]:
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


def _clean_reason_text(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    s = " ".join(s.split())
    return s.strip(" ;,.-")


def _pick(d: Dict[str, Any], *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)

    nmap = d.get("_nmap")
    if not isinstance(nmap, dict):
        nmap = {_norm_key(k): k for k in d.keys() if k != "_nmap"}
        d["_nmap"] = nmap

    for k in keys:
        nk = _norm_key(k)
        if nk in nmap:
            return d.get(nmap[nk])
    return None


def _normalize_symbol(symbol: Any) -> str:
    s = _safe_str(symbol).upper().replace(" ", "")
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1]
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    return s


def _canonical_symbol(symbol: Any) -> str:
    s = _normalize_symbol(symbol)
    if not s:
        return ""
    if s.endswith(".SR") and s[:-3].isdigit():
        return f"KSA:{s[:-3]}"
    if s.isdigit():
        return f"KSA:{s}"
    if "." in s:
        return f"GLOBAL:{s.split('.', 1)[0]}"
    if s.startswith("^"):
        return f"IDX:{s}"
    return f"GLOBAL:{s}"


def _safe_jsonable(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, Mapping):
        return {str(k): _safe_jsonable(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_safe_jsonable(v) for v in value]

    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            return _safe_jsonable(value.model_dump(mode="python"))
    except Exception:
        pass

    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _safe_jsonable(value.dict())
    except Exception:
        pass

    try:
        if hasattr(value, "__dict__"):
            return _safe_jsonable(vars(value))
    except Exception:
        pass

    return _safe_str(value)


def _payload_to_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Mapping):
        try:
            return dict(value)
        except Exception:
            return {}

    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            dumped = value.model_dump(mode="python")
            if isinstance(dumped, dict):
                return dumped
    except Exception:
        pass

    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            dumped = value.dict()
            if isinstance(dumped, dict):
                return dumped
    except Exception:
        pass

    try:
        if hasattr(value, "__dict__"):
            d = vars(value)
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass

    return {"result": value}


def _dedupe_keep_order(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        s = _safe_str(value)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# -----------------------------------------------------------------------------
# Sync/async bridge
# -----------------------------------------------------------------------------
def _in_running_loop() -> bool:
    try:
        loop = asyncio.get_running_loop()
        return bool(loop.is_running())
    except Exception:
        return False


def _run_coro_in_thread(coro: Any, timeout: float = 20.0) -> Tuple[Optional[Any], Optional[BaseException], bool]:
    box: Dict[str, Any] = {"result": None, "error": None}

    def _worker() -> None:
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


def _safe_call(obj: Any, method_name: str, *args: Any, timeout: float = 20.0, **kwargs: Any) -> Tuple[Optional[Any], Optional[str]]:
    if obj is None:
        return None, "target_none"
    fn = getattr(obj, method_name, None)
    if not callable(fn):
        return None, "method_not_found"

    try:
        out = fn(*args, **kwargs)
        if inspect.isawaitable(out):
            if _in_running_loop():
                res, err, to = _run_coro_in_thread(out, timeout=timeout)
                if to:
                    return None, f"timeout:{timeout}s"
                if err is not None:
                    return None, f"exception:{type(err).__name__}:{err}"
                return res, None
            try:
                return asyncio.run(asyncio.wait_for(out, timeout=timeout)), None
            except asyncio.TimeoutError:
                return None, f"timeout:{timeout}s"
        return out, None
    except Exception as e:
        return None, f"exception:{type(e).__name__}:{e}"


# -----------------------------------------------------------------------------
# Criteria model
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class AdvisorCriteria:
    sources: List[str] = field(default_factory=lambda: list(DEFAULT_SOURCE_PAGES))
    tickers: Optional[List[str]] = None

    risk_profile: str = "moderate"
    risk_bucket: str = ""
    confidence_bucket: str = ""
    confidence_level: str = ""

    invest_amount: float = 0.0
    currency: str = "SAR"
    top_n: int = 20
    allocation_strategy: str = "maximum_sharpe"

    invest_period_days: Optional[int] = None
    invest_period_label: str = ""
    horizon_days: Optional[int] = None

    required_roi_1m: Optional[float] = None
    required_roi_3m: Optional[float] = None
    required_roi_12m: Optional[float] = None
    min_expected_roi: Optional[float] = None
    min_confidence: Optional[float] = None

    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_liquidity_score: float = 25.0
    min_advisor_score: Optional[float] = None
    max_risk_score: Optional[float] = None
    min_volume: Optional[float] = None

    exclude_sectors: Optional[List[str]] = None
    allowed_markets: Optional[List[str]] = None

    use_row_updated_at: bool = True
    debug: bool = False

    raw: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "sources": self.sources,
            "tickers": self.tickers,
            "risk_profile": self.risk_profile,
            "risk_bucket": self.risk_bucket,
            "confidence_bucket": self.confidence_bucket,
            "confidence_level": self.confidence_level,
            "invest_amount": self.invest_amount,
            "currency": self.currency,
            "top_n": self.top_n,
            "limit": self.top_n,
            "allocation_strategy": self.allocation_strategy,
            "required_roi_1m": self.required_roi_1m,
            "required_roi_3m": self.required_roi_3m,
            "required_roi_12m": self.required_roi_12m,
            "min_expected_roi": self.min_expected_roi,
            "min_confidence": self.min_confidence,
            "min_price": self.min_price,
            "max_price": self.max_price,
            "min_liquidity_score": self.min_liquidity_score,
            "min_advisor_score": self.min_advisor_score,
            "max_risk_score": self.max_risk_score,
            "min_volume": self.min_volume,
            "exclude_sectors": self.exclude_sectors,
            "allowed_markets": self.allowed_markets,
            "use_row_updated_at": self.use_row_updated_at,
            "debug": self.debug,
            "horizon_days": self.horizon_days,
            "invest_period_days": self.invest_period_days,
            "invest_period_label": self.invest_period_label,
        }
        out.update(self.raw or {})
        return out


# -----------------------------------------------------------------------------
# Core callable discovery
# -----------------------------------------------------------------------------
def _lazy_core_run_investment_advisor() -> Optional[Callable[..., Any]]:
    try:
        mod = importlib.import_module("core.investment_advisor")
    except Exception:
        return None

    for name in (
        "run_investment_advisor",
        "run_advisor",
        "execute_investment_advisor",
        "execute_advisor",
    ):
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn
    return None


# -----------------------------------------------------------------------------
# Payload/criteria normalization
# -----------------------------------------------------------------------------
def _list_or_none(v: Any) -> Optional[List[str]]:
    if v is None:
        return None
    if isinstance(v, str):
        vals = [x for x in v.replace(",", " ").split() if x.strip()]
        vals = [_normalize_symbol(x) for x in vals if _normalize_symbol(x)]
        return vals or None
    if isinstance(v, list):
        vals = [_normalize_symbol(x) for x in v if _normalize_symbol(x)]
        return vals or None
    if isinstance(v, tuple):
        return _list_or_none(list(v))
    if isinstance(v, set):
        return _list_or_none(list(v))
    val = _normalize_symbol(v)
    return [val] if val else None


def _normalize_sources(raw_sources: Any) -> List[str]:
    if isinstance(raw_sources, str):
        raw_list = [raw_sources]
    elif isinstance(raw_sources, list):
        raw_list = [str(x) for x in raw_sources if x]
    else:
        raw_list = ["ALL"]

    out: List[str] = []
    seen = set()

    for s in raw_list:
        txt = _safe_str(s)
        if not txt:
            continue
        if txt.upper() == "ALL":
            for page in DEFAULT_SOURCE_PAGES:
                if page not in seen:
                    out.append(page)
                    seen.add(page)
            continue

        try:
            page = _normalize_page_name(txt, allow_output_pages=False) if _HAS_PAGE_CATALOG else txt
        except Exception:
            page = txt

        page = _safe_str(page)
        if not page:
            continue
        if page in DERIVED_OR_NON_SOURCE_PAGES:
            continue
        if page not in seen:
            out.append(page)
            seen.add(page)

    return out or list(DEFAULT_SOURCE_PAGES)


def _period_label_from_days(days: Optional[int]) -> str:
    if days is None:
        return ""
    d = int(days)
    if d <= 0:
        return ""
    if d <= 31:
        return "1M"
    if d <= 92:
        return "3M"
    if d <= 183:
        return "6M"
    if d <= 366:
        return "12M"
    return f"{d}D"


def _infer_horizon_days(payload: Dict[str, Any]) -> Tuple[Optional[int], str]:
    explicit = (
        _to_int(payload.get("horizon_days"))
        or _to_int(payload.get("invest_period_days"))
        or _to_int(payload.get("investment_period_days"))
        or _to_int(payload.get("period_days"))
        or _to_int(payload.get("days"))
    )
    if explicit and explicit > 0:
        return explicit, _period_label_from_days(explicit)

    label = _safe_str(
        payload.get("invest_period_label")
        or payload.get("investment_period_label")
        or payload.get("period_label")
        or payload.get("horizon_label")
    ).upper()

    label_map = {
        "1M": 30,
        "30D": 30,
        "30": 30,
        "3M": 90,
        "90D": 90,
        "90": 90,
        "6M": 180,
        "180D": 180,
        "180": 180,
        "12M": 365,
        "1Y": 365,
        "365D": 365,
        "365": 365,
    }
    if label in label_map:
        return label_map[label], _period_label_from_days(label_map[label])

    if payload.get("required_roi_1m") is not None:
        return 30, "1M"
    if payload.get("required_roi_3m") is not None:
        return 90, "3M"
    if payload.get("required_roi_12m") is not None:
        return 365, "12M"

    if payload.get("min_expected_roi") is not None or payload.get("min_roi") is not None:
        return 90, "3M"

    return 90, "3M"


def _normalize_incoming_payload(
    payload: Optional[Dict[str, Any]] = None,
    *,
    body: Optional[Dict[str, Any]] = None,
    request_data: Optional[Dict[str, Any]] = None,
    criteria: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for src in (params, request_data, body, criteria, payload):
        if isinstance(src, dict):
            out.update(src)

    if "tickers" not in out or not _list_or_none(out.get("tickers")):
        sym_in = out.get("symbols")
        if sym_in is None:
            sym_in = out.get("symbol")
        if sym_in is None:
            sym_in = out.get("direct_symbols")
        if sym_in is not None:
            out["tickers"] = _list_or_none(sym_in)

    if not out.get("sources"):
        source_candidates = []
        page = _safe_str(out.get("page") or out.get("sheet") or out.get("sheet_name") or out.get("name"))
        if page:
            source_candidates.append(page)
        if out.get("pages_selected"):
            source_candidates.extend(list(out.get("pages_selected") or []))
        if out.get("pages"):
            source_candidates.extend(list(out.get("pages") or []))
        if out.get("selected_pages"):
            source_candidates.extend(list(out.get("selected_pages") or []))

        out["sources"] = source_candidates or ["ALL"]

    risk_level = _safe_str(out.get("risk_level"))
    risk_profile = _safe_str(out.get("risk_profile"))
    if risk_level and not risk_profile:
        out["risk_profile"] = risk_level
    if risk_profile and not risk_level:
        out["risk_level"] = risk_profile

    if out.get("top_n") is None and out.get("limit") is not None:
        out["top_n"] = out.get("limit")
    if out.get("limit") is None and out.get("top_n") is not None:
        out["limit"] = out.get("top_n")

    if out.get("horizon_days") is None and out.get("investment_period_days") is not None:
        out["horizon_days"] = out.get("investment_period_days")
    if out.get("invest_period_days") is None:
        if out.get("investment_period_days") is not None:
            out["invest_period_days"] = out.get("investment_period_days")
        elif out.get("horizon_days") is not None:
            out["invest_period_days"] = out.get("horizon_days")

    if out.get("min_expected_roi") is None and out.get("min_roi") is not None:
        out["min_expected_roi"] = out.get("min_roi")
    if out.get("min_roi") is None and out.get("min_expected_roi") is not None:
        out["min_roi"] = out.get("min_expected_roi")

    cleaned: Dict[str, Any] = {}
    for k, v in out.items():
        if v is None:
            continue
        if isinstance(v, str) and not _safe_str(v):
            continue
        cleaned[k] = v

    return cleaned


def _build_criteria(payload: Dict[str, Any]) -> AdvisorCriteria:
    horizon_days, invest_period_label = _infer_horizon_days(payload or {})
    min_expected_roi = _as_ratio((payload or {}).get("min_expected_roi") or (payload or {}).get("min_roi"))

    required_roi_1m = _as_ratio((payload or {}).get("required_roi_1m"))
    required_roi_3m = _as_ratio((payload or {}).get("required_roi_3m"))
    required_roi_12m = _as_ratio((payload or {}).get("required_roi_12m"))

    if min_expected_roi is not None:
        if horizon_days and horizon_days <= 31 and required_roi_1m is None:
            required_roi_1m = min_expected_roi
        elif horizon_days and horizon_days <= 92 and required_roi_3m is None:
            required_roi_3m = min_expected_roi
        elif required_roi_12m is None:
            required_roi_12m = min_expected_roi

    return AdvisorCriteria(
        sources=_normalize_sources((payload or {}).get("sources")),
        tickers=_list_or_none((payload or {}).get("tickers") or (payload or {}).get("symbols") or (payload or {}).get("direct_symbols")),
        risk_profile=_safe_str((payload or {}).get("risk_profile") or (payload or {}).get("risk_level") or "moderate").lower() or "moderate",
        risk_bucket=_safe_str((payload or {}).get("risk_bucket") or (payload or {}).get("risk")),
        confidence_bucket=_safe_str((payload or {}).get("confidence_bucket") or (payload or {}).get("confidence_level") or (payload or {}).get("confidence")),
        confidence_level=_safe_str((payload or {}).get("confidence_level") or (payload or {}).get("confidence_bucket")),
        invest_amount=_to_float((payload or {}).get("invest_amount")) or 0.0,
        currency=_safe_str((payload or {}).get("currency") or "SAR").upper() or "SAR",
        top_n=max(1, min(200, _to_int((payload or {}).get("top_n") or (payload or {}).get("limit")) or 20)),
        allocation_strategy=_safe_str((payload or {}).get("allocation_strategy") or "maximum_sharpe").lower(),
        invest_period_days=horizon_days,
        invest_period_label=invest_period_label,
        horizon_days=horizon_days,
        required_roi_1m=required_roi_1m,
        required_roi_3m=required_roi_3m,
        required_roi_12m=required_roi_12m,
        min_expected_roi=min_expected_roi,
        min_confidence=_to_float((payload or {}).get("min_confidence") or (payload or {}).get("min_ai_confidence")),
        min_price=_to_float((payload or {}).get("min_price")),
        max_price=_to_float((payload or {}).get("max_price")),
        min_liquidity_score=_to_float((payload or {}).get("min_liquidity_score")) or 25.0,
        min_advisor_score=_to_float((payload or {}).get("min_advisor_score")),
        max_risk_score=_to_float((payload or {}).get("max_risk_score")),
        min_volume=_to_float((payload or {}).get("min_volume")),
        exclude_sectors=(payload or {}).get("exclude_sectors"),
        allowed_markets=(payload or {}).get("allowed_markets"),
        use_row_updated_at=_truthy((payload or {}).get("use_row_updated_at", True)),
        debug=_truthy((payload or {}).get("debug", False)),
        raw=dict(payload or {}),
    )


# -----------------------------------------------------------------------------
# Reason synthesis
# -----------------------------------------------------------------------------
def _fmt_pct(r: Optional[float], digits: int = 1) -> Optional[str]:
    if r is None:
        return None
    try:
        return f"{float(r) * 100:.{digits}f}%"
    except Exception:
        return None


def _fmt_num(v: Optional[float], digits: int = 1) -> Optional[str]:
    if v is None:
        return None
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return None


def _build_fallback_recommendation_reason(item: Dict[str, Any], criteria: AdvisorCriteria) -> str:
    rec = _safe_str(item.get("recommendation") or "HOLD").upper()
    current_price = _to_float(item.get("current_price") or item.get("price"))
    fp1 = _to_float(item.get("forecast_price_1m"))
    fp3 = _to_float(item.get("forecast_price_3m"))
    fp12 = _to_float(item.get("forecast_price_12m"))
    roi1 = _as_ratio(item.get("expected_roi_1m"))
    roi3 = _as_ratio(item.get("expected_roi_3m"))
    roi12 = _as_ratio(item.get("expected_roi_12m"))
    overall = _to_float(item.get("overall_score"))
    confidence = _to_float(item.get("confidence_score") or item.get("forecast_confidence"))
    risk = _to_float(item.get("risk_score"))
    valuation = _to_float(item.get("valuation_score"))
    momentum = _to_float(item.get("momentum_score"))
    quality = _to_float(item.get("quality_score"))
    opportunity = _to_float(item.get("opportunity_score"))
    liquidity = _to_float(item.get("liquidity_score"))

    horizon = criteria.horizon_days or 90
    if horizon <= 31:
        horizon_label = "1M"
        chosen_roi = roi1 if roi1 is not None else roi3
        chosen_fp = fp1 if fp1 is not None else fp3
    elif horizon <= 92:
        horizon_label = "3M"
        chosen_roi = roi3 if roi3 is not None else roi12
        chosen_fp = fp3 if fp3 is not None else fp12
    else:
        horizon_label = criteria.invest_period_label or _period_label_from_days(horizon) or "12M"
        chosen_roi = roi12 if roi12 is not None else roi3
        chosen_fp = fp12 if fp12 is not None else fp3

    parts: List[str] = []

    if rec == "BUY":
        if chosen_roi is not None and chosen_roi > 0:
            parts.append(f"expected {horizon_label} upside is {_fmt_pct(chosen_roi)}")
        if chosen_fp is not None and current_price is not None and chosen_fp > current_price:
            parts.append(f"forecast price {_fmt_num(chosen_fp, 2)} is above current price {_fmt_num(current_price, 2)}")
        if confidence is not None and confidence >= 70:
            parts.append(f"confidence is high at {_fmt_num(confidence, 0)}/100")
        if overall is not None and overall >= 65:
            parts.append(f"overall score is strong at {_fmt_num(overall, 0)}/100")
        if valuation is not None and valuation >= 60:
            parts.append(f"valuation is supportive at {_fmt_num(valuation, 0)}/100")
        if quality is not None and quality >= 60:
            parts.append(f"quality is supportive at {_fmt_num(quality, 0)}/100")
        if momentum is not None and momentum >= 60:
            parts.append(f"momentum is positive at {_fmt_num(momentum, 0)}/100")
        if opportunity is not None and opportunity >= 60:
            parts.append(f"opportunity score is favorable at {_fmt_num(opportunity, 0)}/100")
        if risk is not None and risk <= 45:
            parts.append(f"risk remains contained at {_fmt_num(risk, 0)}/100")

    elif rec == "SELL":
        if chosen_roi is not None and chosen_roi < 0:
            parts.append(f"expected {horizon_label} return is negative at {_fmt_pct(chosen_roi)}")
        if chosen_fp is not None and current_price is not None and chosen_fp < current_price:
            parts.append(f"forecast price {_fmt_num(chosen_fp, 2)} is below current price {_fmt_num(current_price, 2)}")
        if risk is not None and risk >= 65:
            parts.append(f"risk is elevated at {_fmt_num(risk, 0)}/100")
        if overall is not None and overall <= 40:
            parts.append(f"overall score is weak at {_fmt_num(overall, 0)}/100")
        if momentum is not None and momentum <= 40:
            parts.append(f"momentum is weak at {_fmt_num(momentum, 0)}/100")
        if confidence is not None and confidence <= 40:
            parts.append(f"confidence is low at {_fmt_num(confidence, 0)}/100")

    else:
        if chosen_roi is not None:
            if abs(chosen_roi) <= 0.03:
                parts.append(f"expected {horizon_label} move is limited at {_fmt_pct(chosen_roi)}")
            elif chosen_roi > 0:
                parts.append(f"upside exists but remains moderate at {_fmt_pct(chosen_roi)}")
            else:
                parts.append(f"downside exists but remains moderate at {_fmt_pct(chosen_roi)}")
        if overall is not None:
            parts.append(f"overall score is balanced at {_fmt_num(overall, 0)}/100")
        if risk is not None and 40 <= risk <= 60:
            parts.append(f"risk is moderate at {_fmt_num(risk, 0)}/100")
        if confidence is not None and 45 <= confidence <= 69:
            parts.append(f"confidence is moderate at {_fmt_num(confidence, 0)}/100")

    if liquidity is not None:
        if liquidity >= 65:
            parts.append(f"liquidity is healthy at {_fmt_num(liquidity, 0)}/100")
        elif liquidity <= 25:
            parts.append(f"liquidity is weak at {_fmt_num(liquidity, 0)}/100")

    if not parts:
        if rec == "BUY":
            return f"BUY because the {horizon_label} risk-return profile is favorable."
        if rec == "SELL":
            return f"SELL because the {horizon_label} risk-return profile is unfavorable."
        return f"HOLD because the {horizon_label} risk-return profile is balanced."

    return f"{rec} because " + ", ".join(parts[:4]) + "."


# -----------------------------------------------------------------------------
# Sheet/item extraction helpers
# -----------------------------------------------------------------------------
def _headers_from_schema(sheet_name: str) -> Tuple[List[str], List[str]]:
    if _HAS_SCHEMA:
        try:
            spec = _get_sheet_spec(sheet_name)
            cols = getattr(spec, "columns", None) or []

            keys = [getattr(c, "key", "") for c in cols]
            headers = [getattr(c, "header", "") for c in cols]

            keys = [k for k in keys if isinstance(k, str) and k]
            headers = [h for h in headers if isinstance(h, str) and h]
            if keys and headers and len(keys) == len(headers):
                return headers, keys
        except Exception:
            pass

    if sheet_name == TOP10_PAGE_NAME:
        return list(_TOP10_FALLBACK_HEADERS), list(_TOP10_FALLBACK_KEYS)

    return [], []


def _rows_from_headers_and_matrix(headers: Sequence[Any], rows: Sequence[Any]) -> List[Dict[str, Any]]:
    keys = [_snake_like(_safe_str(h)) for h in headers]
    out: List[Dict[str, Any]] = []
    for r in rows:
        if isinstance(r, Mapping):
            out.append(dict(r))
            continue
        if isinstance(r, (list, tuple)):
            d: Dict[str, Any] = {}
            for i, key in enumerate(keys):
                d[key] = r[i] if i < len(r) else None
            out.append(d)
    return out


def _extract_items_from_payload(payload: Any) -> List[Dict[str, Any]]:
    p = _payload_to_dict(payload)

    for key in ("items", "recommendations", "data", "results", "records", "quotes"):
        val = p.get(key)
        if isinstance(val, list):
            dicts = [dict(x) for x in val if isinstance(x, dict)]
            if dicts:
                return dicts

    rows = p.get("rows")
    headers = p.get("headers") or p.get("keys")
    if isinstance(rows, list) and rows and isinstance(headers, list) and headers:
        return _rows_from_headers_and_matrix(headers, rows)

    rows_matrix = p.get("rows_matrix")
    if isinstance(rows_matrix, list) and rows_matrix and isinstance(headers, list) and headers:
        return _rows_from_headers_and_matrix(headers, rows_matrix)

    result = p.get("result")
    if isinstance(result, list):
        dicts = [dict(x) for x in result if isinstance(x, dict)]
        if dicts:
            return dicts

    return []


def _choose_horizon_fields(criteria: AdvisorCriteria) -> Tuple[str, str]:
    h = criteria.horizon_days or 90
    if h <= 31:
        return "expected_roi_1m", "forecast_price_1m"
    if h <= 92:
        return "expected_roi_3m", "forecast_price_3m"
    return "expected_roi_12m", "forecast_price_12m"


def _derive_recommendation(item: Dict[str, Any], criteria: AdvisorCriteria) -> str:
    chosen_roi_key, _ = _choose_horizon_fields(criteria)
    chosen_roi = _as_ratio(item.get(chosen_roi_key))
    confidence = _to_float(item.get("confidence_score") or item.get("forecast_confidence"))
    risk = _to_float(item.get("risk_score"))
    overall = _to_float(item.get("overall_score") or item.get("opportunity_score"))

    min_roi = criteria.min_expected_roi
    max_risk = criteria.max_risk_score
    min_conf = criteria.min_confidence

    if chosen_roi is None:
        chosen_roi = _as_ratio(item.get("expected_roi_3m"))

    if chosen_roi is not None and chosen_roi < -0.03:
        return "SELL"

    buy_conditions = 0
    if chosen_roi is not None and chosen_roi > 0.03:
        buy_conditions += 1
    if overall is not None and overall >= 60:
        buy_conditions += 1
    if confidence is not None and confidence >= 60:
        buy_conditions += 1
    if risk is not None and risk <= 55:
        buy_conditions += 1

    if min_roi is not None and chosen_roi is not None and chosen_roi < min_roi:
        return "HOLD"
    if min_conf is not None and confidence is not None and confidence < min_conf:
        return "HOLD"
    if max_risk is not None and risk is not None and risk > max_risk:
        return "HOLD"

    if buy_conditions >= 2:
        return "BUY"

    if chosen_roi is not None and chosen_roi < 0:
        return "SELL"

    return "HOLD"


def _row_sort_tuple(item: Dict[str, Any], criteria: AdvisorCriteria) -> Tuple[float, float, float, float]:
    chosen_roi_key, _ = _choose_horizon_fields(criteria)
    roi = _as_ratio(item.get(chosen_roi_key))
    if roi is None:
        roi = _as_ratio(item.get("expected_roi_3m"))
    if roi is None:
        roi = _as_ratio(item.get("expected_roi_1m"))
    if roi is None:
        roi = -999.0

    overall = _to_float(item.get("overall_score") or item.get("opportunity_score"))
    if overall is None:
        overall = -999.0

    confidence = _to_float(item.get("confidence_score") or item.get("forecast_confidence"))
    if confidence is None:
        confidence = -999.0

    risk = _to_float(item.get("risk_score"))
    if risk is None:
        risk = 999.0

    return (roi, overall, confidence, -risk)


def _project_top10_items(items: List[Dict[str, Any]], criteria: AdvisorCriteria) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
    headers, keys = _headers_from_schema(TOP10_PAGE_NAME)
    if not headers or not keys:
        headers, keys = list(_TOP10_FALLBACK_HEADERS), list(_TOP10_FALLBACK_KEYS)

    chosen_roi_key, chosen_fp_key = _choose_horizon_fields(criteria)
    criteria_snapshot = json_dumps(criteria.to_payload())

    sorted_items = sorted(items, key=lambda x: _row_sort_tuple(x, criteria), reverse=True)
    projected: List[Dict[str, Any]] = []

    for idx, src in enumerate(sorted_items[: max(1, criteria.top_n)], start=1):
        item = dict(src)

        item["symbol"] = _normalize_symbol(item.get("symbol") or item.get("ticker") or item.get("code"))
        item["name"] = item.get("name") or item.get("company_name") or item.get("security_name")
        item["current_price"] = _to_float(item.get("current_price") or item.get("price"))
        item["forecast_price_1m"] = _to_float(item.get("forecast_price_1m"))
        item["forecast_price_3m"] = _to_float(item.get("forecast_price_3m"))
        item["forecast_price_12m"] = _to_float(item.get("forecast_price_12m"))
        item["expected_roi_1m"] = _as_ratio(item.get("expected_roi_1m"))
        item["expected_roi_3m"] = _as_ratio(item.get("expected_roi_3m"))
        item["expected_roi_12m"] = _as_ratio(item.get("expected_roi_12m"))
        item["forecast_confidence"] = _to_float(item.get("forecast_confidence"))
        item["confidence_score"] = _to_float(item.get("confidence_score") or item.get("forecast_confidence"))
        item["confidence_bucket"] = _safe_str(item.get("confidence_bucket") or criteria.confidence_bucket or criteria.confidence_level)
        item["risk_score"] = _to_float(item.get("risk_score"))
        item["risk_bucket"] = _safe_str(item.get("risk_bucket") or criteria.risk_bucket)
        item["overall_score"] = _to_float(item.get("overall_score"))
        item["opportunity_score"] = _to_float(item.get("opportunity_score") or item.get("overall_score"))

        if not _safe_str(item.get("recommendation")):
            item["recommendation"] = _derive_recommendation(item, criteria)

        item["horizon_days"] = _to_int(item.get("horizon_days")) or criteria.horizon_days
        item["invest_period_label"] = _safe_str(item.get("invest_period_label")) or criteria.invest_period_label

        now_dt = _now_utc()
        last_utc = _safe_str(item.get("last_updated_utc"))
        if not last_utc:
            last_utc = _iso_utc(now_dt)
        item["last_updated_utc"] = last_utc

        last_riyadh = _safe_str(item.get("last_updated_riyadh"))
        if not last_riyadh:
            parsed = _parse_iso_dt(last_utc) or now_dt
            last_riyadh = _iso_riyadh(parsed)
        item["last_updated_riyadh"] = last_riyadh

        reason = _clean_reason_text(item.get("recommendation_reason"))
        if not reason:
            reason = _build_fallback_recommendation_reason(item, criteria)
        item["recommendation_reason"] = reason

        if _is_blank(item.get("top10_rank")):
            item["top10_rank"] = idx

        if _is_blank(item.get("selection_reason")):
            chosen_roi = _as_ratio(item.get(chosen_roi_key))
            chosen_fp = _to_float(item.get(chosen_fp_key))
            current_price = _to_float(item.get("current_price"))
            parts: List[str] = []
            if chosen_roi is not None:
                parts.append(f"{chosen_roi_key}={round(chosen_roi * 100, 2)}%")
            if chosen_fp is not None and current_price is not None:
                parts.append(f"target={round(chosen_fp, 2)} vs current={round(current_price, 2)}")
            if item.get("overall_score") is not None:
                parts.append(f"overall={item.get('overall_score')}")
            if item.get("confidence_score") is not None:
                parts.append(f"confidence={item.get('confidence_score')}")
            if item.get("risk_score") is not None:
                parts.append(f"risk={item.get('risk_score')}")
            item["selection_reason"] = " | ".join(parts) if parts else item["recommendation_reason"]

        if _is_blank(item.get("criteria_snapshot")):
            item["criteria_snapshot"] = criteria_snapshot

        projected.append({k: item.get(k) for k in keys})

    return headers, keys, projected


# -----------------------------------------------------------------------------
# Canonical output enrichment
# -----------------------------------------------------------------------------
def _ensure_items_shape(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = result.get("items")
    if isinstance(items, list) and all(isinstance(x, dict) for x in items):
        return [dict(x) for x in items]

    rows = result.get("rows")
    headers = result.get("headers") or result.get("keys")
    if isinstance(headers, list) and isinstance(rows, list):
        return _rows_from_headers_and_matrix(headers, rows)

    rows_matrix = result.get("rows_matrix")
    if isinstance(headers, list) and isinstance(rows_matrix, list):
        return _rows_from_headers_and_matrix(headers, rows_matrix)

    return []


def _enrich_item_with_context(item: Dict[str, Any], criteria: AdvisorCriteria, meta: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(item or {})

    out["symbol"] = _normalize_symbol(out.get("symbol") or out.get("ticker") or out.get("code"))
    out["canonical"] = out.get("canonical") or _canonical_symbol(out.get("symbol"))
    out["invest_period_label"] = _safe_str(out.get("invest_period_label")) or criteria.invest_period_label
    out["horizon_days"] = _to_int(out.get("horizon_days")) or criteria.horizon_days
    out["invest_period_days"] = _to_int(out.get("invest_period_days")) or criteria.invest_period_days or criteria.horizon_days

    if not out.get("last_updated_utc"):
        now_dt = _now_utc()
        out["last_updated_utc"] = _iso_utc(now_dt)
        out["last_updated_riyadh"] = _iso_riyadh(now_dt)
    elif not out.get("last_updated_riyadh"):
        parsed = _parse_iso_dt(out.get("last_updated_utc")) or _now_utc()
        out["last_updated_riyadh"] = _iso_riyadh(parsed)

    reason = _clean_reason_text(out.get("recommendation_reason"))
    if not reason:
        reason = _build_fallback_recommendation_reason(out, criteria)
    out["recommendation_reason"] = reason

    out["advisor_context"] = {
        "horizon_days": out.get("horizon_days"),
        "invest_period_label": out.get("invest_period_label"),
        "risk_profile": criteria.risk_profile,
        "risk_bucket": criteria.risk_bucket,
        "confidence_bucket": criteria.confidence_bucket,
        "allocation_strategy": criteria.allocation_strategy,
    }

    return out


def _enrich_result(result: Dict[str, Any], criteria: AdvisorCriteria) -> Dict[str, Any]:
    out = dict(result or {})
    items = _ensure_items_shape(out)
    meta = dict(out.get("meta") or {})

    enriched_items = [_enrich_item_with_context(x, criteria, meta) for x in items]
    out["items"] = enriched_items

    headers = out.get("headers")
    rows = out.get("rows")

    if isinstance(headers, list) and isinstance(rows, list):
        hdr_index = {str(h): i for i, h in enumerate(headers)}
        need_new_headers = []
        for h in ["Recommendation Reason", "Horizon Days", "Invest Period Label"]:
            if h not in hdr_index:
                need_new_headers.append(h)

        if need_new_headers:
            headers = list(headers) + need_new_headers
            hdr_index = {str(h): i for i, h in enumerate(headers)}
            new_rows: List[List[Any]] = []
            for idx, r in enumerate(rows):
                rr = list(r) if isinstance(r, (list, tuple)) else []
                while len(rr) < len(headers):
                    rr.append(None)
                item = enriched_items[idx] if idx < len(enriched_items) else {}
                rr[hdr_index["Recommendation Reason"]] = item.get("recommendation_reason")
                rr[hdr_index["Horizon Days"]] = item.get("horizon_days")
                rr[hdr_index["Invest Period Label"]] = item.get("invest_period_label")
                new_rows.append(rr)
            out["headers"] = headers
            out["rows"] = new_rows

    meta["engine_version"] = "5.1.0"
    meta["core_advisor_available"] = bool(_lazy_core_run_investment_advisor())
    meta["criteria"] = {
        "sources": criteria.sources,
        "tickers": criteria.tickers,
        "risk_profile": criteria.risk_profile,
        "risk_bucket": criteria.risk_bucket,
        "confidence_bucket": criteria.confidence_bucket,
        "invest_amount": criteria.invest_amount,
        "currency": criteria.currency,
        "top_n": criteria.top_n,
        "allocation_strategy": criteria.allocation_strategy,
        "horizon_days": criteria.horizon_days,
        "invest_period_days": criteria.invest_period_days,
        "invest_period_label": criteria.invest_period_label,
        "required_roi_1m": criteria.required_roi_1m,
        "required_roi_3m": criteria.required_roi_3m,
        "required_roi_12m": criteria.required_roi_12m,
        "min_expected_roi": criteria.min_expected_roi,
    }
    meta["context_propagation"] = {
        "recommendation_reason": True,
        "horizon_days": True,
        "invest_period_label": True,
    }
    out["meta"] = meta
    return out


# -----------------------------------------------------------------------------
# Engine class
# -----------------------------------------------------------------------------
class InvestmentAdvisorEngine:
    """
    Thin but intelligent engine layer:
    - normalizes advisory request/criteria
    - delegates to data_engine / quote_engine where available
    - calls core.investment_advisor.run_investment_advisor(...) when available
    - enriches final canonical output with advisory context
    - falls back to an internal lightweight advisor flow when core advisor is absent
    """

    def __init__(
        self,
        *,
        data_engine: Any = None,
        quote_engine: Any = None,
        cache_engine: Any = None,
        settings: Optional[Dict[str, Any]] = None,
        cache_strategy: str = "memory",
        cache_ttl: int = 600,
    ) -> None:
        self.data_engine = data_engine
        self.quote_engine = quote_engine or data_engine
        self.cache_engine = cache_engine or data_engine
        self.settings = dict(settings or {})
        self.cache_strategy = _safe_str(cache_strategy or "memory").lower() or "memory"
        self.cache_ttl = _to_int(cache_ttl) or 600

    # ------------------------------------------------------------------
    # warm/cache helpers expected by routes/advisor.py
    # ------------------------------------------------------------------
    def warm_cache(self, sheet_names: Optional[List[str]] = None) -> Dict[str, Any]:
        sheets = _normalize_sources(sheet_names or ["ALL"])
        warmed = self.get_cached_multi_sheet_snapshots(sheets)
        return {
            "ok": True,
            "sheet_count": len(sheets),
            "snapshot_count": len(warmed),
            "sheets": sheets,
        }

    def warm_snapshots(self, sheet_names: Optional[List[str]] = None) -> Dict[str, Any]:
        return self.warm_cache(sheet_names)

    def preload_snapshots(self, sheet_names: Optional[List[str]] = None) -> Dict[str, Any]:
        return self.warm_cache(sheet_names)

    def build_snapshot_cache(self, sheet_names: Optional[List[str]] = None) -> Dict[str, Any]:
        return self.warm_cache(sheet_names)

    # ------------------------------------------------------------------
    # engine compatibility methods used by route/core layers
    # ------------------------------------------------------------------
    def get_cached_multi_sheet_snapshots(self, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
        sheet_names = [str(x) for x in (sheet_names or []) if str(x).strip()]
        out: Dict[str, Dict[str, Any]] = {}

        targets = [self.cache_engine, self.data_engine]
        for target in targets:
            if target is None:
                continue

            for method in ("get_cached_multi_sheet_snapshots", "get_multi_sheet_snapshots"):
                res, err = _safe_call(target, method, sheet_names)
                if err is None and isinstance(res, dict):
                    for k, v in res.items():
                        if isinstance(v, dict):
                            out[str(k)] = v
                    if out:
                        return out

        for s in sheet_names:
            one = self.get_cached_sheet_snapshot(s)
            if isinstance(one, dict):
                out[s] = one
        return out

    def get_cached_sheet_snapshot(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        targets = [self.cache_engine, self.data_engine]
        for target in targets:
            if target is None:
                continue
            for method in ("get_cached_sheet_snapshot", "get_sheet_snapshot", "read_cached_sheet_snapshot"):
                res, err = _safe_call(target, method, sheet_name)
                if err is None and isinstance(res, dict):
                    return res
        return None

    def get_quote_dict(self, symbol: str) -> Dict[str, Any]:
        batch = self.get_enriched_quotes_batch([symbol])
        return batch.get(_normalize_symbol(symbol), {})

    def get_analysis_quotes_batch(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        return self.get_enriched_quotes_batch(symbols)

    def get_enriched_quotes_batch(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        syms = [_normalize_symbol(x) for x in (symbols or []) if _normalize_symbol(x)]
        if not syms:
            return {}

        targets = [self.quote_engine, self.data_engine]
        methods = [
            "get_enriched_quotes_batch",
            "get_analysis_quotes_batch",
            "get_quotes_batch",
            "fetch_quotes_batch",
            "batch_quotes",
        ]
        for target in targets:
            if target is None:
                continue
            for method in methods:
                for args, kwargs in [
                    ((), {"symbols": syms}),
                    ((syms,), {}),
                    ((), {"tickers": syms}),
                ]:
                    res, err = _safe_call(target, method, *args, **kwargs)
                    if err is None and isinstance(res, dict):
                        out: Dict[str, Dict[str, Any]] = {}
                        for k, v in res.items():
                            if isinstance(v, dict):
                                out[_normalize_symbol(k)] = dict(v)
                        if out:
                            return out
        return {}

    def _delegate_sheet_rows(
        self,
        *,
        page: str,
        limit: int = 5000,
        offset: int = 0,
        mode: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        targets = [self.data_engine]
        method_candidates = [
            "get_sheet_rows",
            "sheet_rows",
            "build_sheet_rows",
            "get_sheet",
            "fetch_sheet",
            "get_rows",
            "get_page_rows",
        ]

        for target in targets:
            if target is None:
                continue
            for method in method_candidates:
                variants = [
                    ((), {"sheet": page, "limit": limit, "offset": offset, "mode": mode, "body": body or {}}),
                    ((), {"page": page, "limit": limit, "offset": offset, "mode": mode, "body": body or {}}),
                    ((), {"sheet_name": page, "limit": limit, "offset": offset, "mode": mode, "body": body or {}}),
                    ((), {"sheet": page, "limit": limit, "offset": offset}),
                    ((), {"page": page, "limit": limit, "offset": offset}),
                    ((page,), {}),
                ]
                for args, kw in variants:
                    res, err = _safe_call(target, method, *args, **{k: v for k, v in kw.items() if v is not None})
                    if err is None and isinstance(res, dict):
                        return res

        return {
            "status": "error",
            "page": page,
            "sheet": page,
            "headers": [],
            "keys": [],
            "rows": [],
            "rows_matrix": [],
            "data": [],
            "items": [],
            "meta": {"ok": False, "sheet": page, "error": "sheet rows unavailable"},
        }

    def _collect_source_items(self, criteria: AdvisorCriteria) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []

        for page in criteria.sources or DEFAULT_SOURCE_PAGES:
            body = {"tickers": criteria.tickers, "symbols": criteria.tickers, "limit": max(criteria.top_n * 10, 50)}
            payload = self._delegate_sheet_rows(page=page, limit=max(criteria.top_n * 10, 50), offset=0, mode="live_quotes", body=body)
            items = _extract_items_from_payload(payload)
            if not items:
                continue
            for item in items:
                item = dict(item)
                item["_source_page"] = page
                collected.append(item)

        if criteria.tickers:
            allowed = {_normalize_symbol(x) for x in criteria.tickers}
            filtered: List[Dict[str, Any]] = []
            for item in collected:
                sym = _normalize_symbol(_pick(item, "symbol", "ticker", "code"))
                if sym in allowed:
                    filtered.append(item)
            collected = filtered

        deduped: List[Dict[str, Any]] = []
        seen = set()
        for item in collected:
            sym = _normalize_symbol(_pick(item, "symbol", "ticker", "code"))
            if not sym or sym in seen:
                continue
            seen.add(sym)
            item["symbol"] = sym
            deduped.append(item)

        return deduped

    def _fallback_run(self, payload: Dict[str, Any], criteria: AdvisorCriteria) -> Dict[str, Any]:
        source_items = self._collect_source_items(criteria)

        if not source_items:
            return {
                "status": "partial",
                "page": TOP10_PAGE_NAME,
                "sheet": TOP10_PAGE_NAME,
                "headers": list(_TOP10_FALLBACK_HEADERS),
                "keys": list(_TOP10_FALLBACK_KEYS),
                "rows": [],
                "rows_matrix": [],
                "items": [],
                "recommendations": [],
                "count": 0,
                "meta": {
                    "ok": False,
                    "engine_version": "5.1.0",
                    "fallback_mode": True,
                    "error": "no source rows available for fallback advisor",
                    "criteria": asdict(criteria),
                },
            }

        headers, keys, projected = _project_top10_items(source_items, criteria)
        rows_matrix = [[row.get(k) for k in keys] for row in projected]

        return {
            "status": "success" if projected else "partial",
            "page": TOP10_PAGE_NAME,
            "sheet": TOP10_PAGE_NAME,
            "route_family": "top10",
            "headers": headers,
            "keys": keys,
            "rows": rows_matrix,
            "rows_matrix": rows_matrix,
            "items": projected,
            "data": projected,
            "recommendations": projected,
            "count": len(projected),
            "meta": {
                "ok": True,
                "engine_version": "5.1.0",
                "fallback_mode": True,
                "dispatch": "internal_fallback_advisor",
                "criteria": asdict(criteria),
            },
        }

    # ------------------------------------------------------------------
    # primary runner
    # ------------------------------------------------------------------
    def run(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = _normalize_incoming_payload(payload or {})
        criteria = _build_criteria(payload)

        forwarded_payload = criteria.to_payload()
        forwarded_payload.setdefault(
            "advisor_data_mode",
            payload.get("advisor_data_mode") or payload.get("data_mode") or payload.get("mode") or "live_quotes",
        )

        core_runner = _lazy_core_run_investment_advisor()
        if callable(core_runner):
            try:
                result = core_runner(forwarded_payload, engine=self)
            except TypeError:
                try:
                    result = core_runner(payload=forwarded_payload, engine=self)
                except TypeError:
                    try:
                        result = core_runner(body=forwarded_payload, engine=self)
                    except Exception as e:
                        result = {
                            "status": "error",
                            "headers": [],
                            "rows": [],
                            "items": [],
                            "meta": {
                                "ok": False,
                                "engine_version": "5.1.0",
                                "core_error": f"{type(e).__name__}: {e}",
                                "criteria": asdict(criteria),
                            },
                        }
                except Exception as e:
                    result = {
                        "status": "error",
                        "headers": [],
                        "rows": [],
                        "items": [],
                        "meta": {
                            "ok": False,
                            "engine_version": "5.1.0",
                            "core_error": f"{type(e).__name__}: {e}",
                            "criteria": asdict(criteria),
                        },
                    }
            except Exception as e:
                result = {
                    "status": "error",
                    "headers": [],
                    "rows": [],
                    "items": [],
                    "meta": {
                        "ok": False,
                        "engine_version": "5.1.0",
                        "core_error": f"{type(e).__name__}: {e}",
                        "criteria": asdict(criteria),
                    },
                }

            if isinstance(result, dict):
                extracted = _extract_items_from_payload(result)
                if extracted or result.get("rows") or result.get("items") or result.get("recommendations"):
                    enriched = _enrich_result(result, criteria)
                    return _safe_jsonable(enriched)

        fallback = self._fallback_run(payload, criteria)
        enriched_fallback = _enrich_result(fallback, criteria)
        return _safe_jsonable(enriched_fallback)

    async def run_async(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.run(payload)

    # ------------------------------------------------------------------
    # sheet/page compatibility
    # ------------------------------------------------------------------
    def get_sheet_rows(
        self,
        sheet: Optional[str] = None,
        limit: int = 5000,
        offset: int = 0,
        mode: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        page = sheet or kwargs.get("sheet_name") or kwargs.get("page") or kwargs.get("name") or ""
        page = _normalize_page_name(page, allow_output_pages=True) if page else page
        page = _safe_str(page)

        if page == TOP10_PAGE_NAME:
            payload = dict(body or {})
            payload.setdefault("page", TOP10_PAGE_NAME)
            payload.setdefault("sheet", TOP10_PAGE_NAME)
            payload.setdefault("limit", limit)
            payload.setdefault("top_n", limit)

            if "tickers" not in payload and kwargs.get("tickers") is not None:
                payload["tickers"] = kwargs.get("tickers")
            if "symbols" not in payload and kwargs.get("symbols") is not None:
                payload["symbols"] = kwargs.get("symbols")
            if "sources" not in payload and kwargs.get("sources") is not None:
                payload["sources"] = kwargs.get("sources")

            result = self.run(payload)
            result = _payload_to_dict(result)

            headers = result.get("headers") or []
            keys = result.get("keys") or []
            items = _extract_items_from_payload(result)

            if not headers or not keys:
                headers, keys = _headers_from_schema(TOP10_PAGE_NAME)
                if not headers or not keys:
                    headers, keys = list(_TOP10_FALLBACK_HEADERS), list(_TOP10_FALLBACK_KEYS)

            if not items and isinstance(result.get("rows"), list) and isinstance(headers, list):
                items = _rows_from_headers_and_matrix(headers, result.get("rows") or [])

            rows = [[item.get(k) for k in keys] for item in items[: max(1, limit)]]
            response = {
                "status": result.get("status") or ("success" if rows else "partial"),
                "page": TOP10_PAGE_NAME,
                "sheet": TOP10_PAGE_NAME,
                "route_family": "top10",
                "headers": headers,
                "keys": keys,
                "rows": rows,
                "rows_matrix": rows,
                "items": items[: max(1, limit)],
                "data": items[: max(1, limit)],
                "meta": dict(result.get("meta") or {}),
            }
            return _safe_jsonable(response)

        return _safe_jsonable(
            self._delegate_sheet_rows(
                page=page,
                limit=limit,
                offset=offset,
                mode=mode,
                body=body,
                **kwargs,
            )
        )

    # ------------------------------------------------------------------
    # convenience health/debug snapshot
    # ------------------------------------------------------------------
    def health_snapshot(self) -> Dict[str, Any]:
        return {
            "ok": True,
            "engine_version": "5.1.0",
            "core_advisor_available": bool(_lazy_core_run_investment_advisor()),
            "page_catalog": bool(_HAS_PAGE_CATALOG),
            "schema_registry": bool(_HAS_SCHEMA),
            "data_engine": type(self.data_engine).__name__ if self.data_engine is not None else None,
            "quote_engine": type(self.quote_engine).__name__ if self.quote_engine is not None else None,
            "cache_engine": type(self.cache_engine).__name__ if self.cache_engine is not None else None,
            "cache_strategy": self.cache_strategy,
            "cache_ttl": self.cache_ttl,
            "timestamp_utc": _iso_utc(_now_utc()),
        }

    def health(self) -> Dict[str, Any]:
        return self.health_snapshot()

    def health_check(self) -> Dict[str, Any]:
        return self.health_snapshot()

    def get_health(self) -> Dict[str, Any]:
        return self.health_snapshot()


# -----------------------------------------------------------------------------
# module-level adapter / runner exports expected by routers
# -----------------------------------------------------------------------------
def create_engine_adapter(
    engine: Any,
    *,
    cache_strategy: str = "memory",
    cache_ttl: int = 600,
    settings: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> InvestmentAdvisorEngine:
    if isinstance(engine, InvestmentAdvisorEngine):
        engine.cache_strategy = _safe_str(cache_strategy or engine.cache_strategy).lower() or "memory"
        engine.cache_ttl = _to_int(cache_ttl) or engine.cache_ttl
        if settings:
            try:
                engine.settings.update(dict(settings))
            except Exception:
                pass
        return engine

    return InvestmentAdvisorEngine(
        data_engine=engine,
        quote_engine=engine,
        cache_engine=engine,
        settings=settings or kwargs.get("settings") or {},
        cache_strategy=cache_strategy,
        cache_ttl=cache_ttl,
    )


def _resolve_engine_for_module_runner(
    engine: Any = None,
    data_engine: Any = None,
    quote_engine: Any = None,
    cache_engine: Any = None,
    settings: Optional[Dict[str, Any]] = None,
    cache_strategy: str = "memory",
    cache_ttl: int = 600,
) -> InvestmentAdvisorEngine:
    base = engine or data_engine or quote_engine or cache_engine
    return create_engine_adapter(
        base,
        cache_strategy=cache_strategy,
        cache_ttl=cache_ttl,
        settings=settings,
    )


def run_investment_advisor_engine(
    payload: Optional[Dict[str, Any]] = None,
    *,
    data_engine: Any = None,
    quote_engine: Any = None,
    cache_engine: Any = None,
    settings: Optional[Dict[str, Any]] = None,
    cache_strategy: str = "memory",
    cache_ttl: int = 600,
) -> Dict[str, Any]:
    engine = _resolve_engine_for_module_runner(
        data_engine=data_engine,
        quote_engine=quote_engine,
        cache_engine=cache_engine,
        settings=settings,
        cache_strategy=cache_strategy,
        cache_ttl=cache_ttl,
    )
    return engine.run(payload or {})


def run_investment_advisor(
    payload: Optional[Dict[str, Any]] = None,
    *,
    body: Optional[Dict[str, Any]] = None,
    request_data: Optional[Dict[str, Any]] = None,
    criteria: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    engine: Any = None,
    data_engine: Any = None,
    quote_engine: Any = None,
    cache_engine: Any = None,
    settings: Optional[Dict[str, Any]] = None,
    cache_strategy: str = "memory",
    cache_ttl: int = 600,
    debug: bool = False,
    request: Any = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    merged = _normalize_incoming_payload(
        payload=payload,
        body=body,
        request_data=request_data,
        criteria=criteria,
        params=params,
    )
    if debug and "debug" not in merged:
        merged["debug"] = True

    adapter = _resolve_engine_for_module_runner(
        engine=engine,
        data_engine=data_engine,
        quote_engine=quote_engine,
        cache_engine=cache_engine,
        settings=settings,
        cache_strategy=cache_strategy,
        cache_ttl=cache_ttl,
    )
    return adapter.run(merged)


def run_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return run_investment_advisor(*args, **kwargs)


def execute_investment_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return run_investment_advisor(*args, **kwargs)


def execute_advisor(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return run_investment_advisor(*args, **kwargs)


def create_investment_advisor(
    engine: Any = None,
    *,
    data_engine: Any = None,
    quote_engine: Any = None,
    cache_engine: Any = None,
    settings: Optional[Dict[str, Any]] = None,
    cache_strategy: str = "memory",
    cache_ttl: int = 600,
    **kwargs: Any,
) -> InvestmentAdvisorEngine:
    base = engine or data_engine or quote_engine or cache_engine
    return create_engine_adapter(
        base,
        cache_strategy=cache_strategy,
        cache_ttl=cache_ttl,
        settings=settings or kwargs.get("settings") or {},
    )


def create_advisor(*args: Any, **kwargs: Any) -> InvestmentAdvisorEngine:
    return create_investment_advisor(*args, **kwargs)


__all__ = [
    "InvestmentAdvisorEngine",
    "AdvisorCriteria",
    "create_engine_adapter",
    "create_investment_advisor",
    "create_advisor",
    "run_investment_advisor_engine",
    "run_investment_advisor",
    "run_advisor",
    "execute_investment_advisor",
    "execute_advisor",
]
