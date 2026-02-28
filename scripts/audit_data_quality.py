#!/usr/bin/env python3
"""
scripts/audit_data_quality.py
================================================================================
TADAWUL FAST BRIDGE — ENTERPRISE DATA QUALITY AUDITOR (v4.4.0)
================================================================================
Aligned • Production-safe • Engine-compatible • Async-safe exports • Deterministic

What’s improved vs v4.3.0
- ✅ Stronger timestamp parsing (ISO + common formats + epoch sec/ms)
- ✅ More robust engine compatibility (dict/list/tuple/None; input-order fallback)
- ✅ Safer anomaly/issue handling (stable dedupe, consistent enums)
- ✅ Better export safety (single event-loop per phase, async-safe file I/O)
- ✅ Cleaner CLI UX (alerts-only, include-ok, json indent, top-N console summary)

CLI examples
- python scripts/audit_data_quality.py --keys MARKET_LEADERS MY_PORTFOLIO --sheet-id "<SID>" --refresh 1 --json-out report.json --csv-out alerts.csv
- python scripts/audit_data_quality.py --keys MARKET_LEADERS --max-symbols 200 --refresh 0 --alerts-only 1
- python scripts/audit_data_quality.py --keys MARKET_LEADERS --include-ok 1 --top 20

Environment
- DEFAULT_SPREADSHEET_ID : fallback Sheet ID
- AUDIT_BATCH_SIZE       : default 200
- AUDIT_MAX_WORKERS      : thread pool size for file exports
- AUDIT_HMAC_KEY         : optional HMAC signing for exported JSON

Exit codes (stable)
- 0 = no MEDIUM/HIGH/CRITICAL
- 1 = at least one MEDIUM
- 2 = at least one HIGH
- 3 = at least one CRITICAL
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import csv
import hashlib
import hmac
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

# -----------------------------------------------------------------------------
# High-performance JSON (orjson optional)
# -----------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, default: Optional[Callable] = None, indent: int = 0) -> str:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, default=default or str, option=opt).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        return orjson.loads(v)

    _HAS_ORJSON = True
except Exception:
    import json  # noqa

    def json_dumps(v: Any, *, default: Optional[Callable] = None, indent: int = 0) -> str:
        return json.dumps(v, default=default or str, ensure_ascii=False, indent=(indent if indent else None))

    def json_loads(v: Union[str, bytes]) -> Any:
        return json.loads(v)

    _HAS_ORJSON = False

# -----------------------------------------------------------------------------
# Optional numeric/data libs (guarded)
# -----------------------------------------------------------------------------
try:
    import numpy as np  # type: ignore

    _HAS_NUMPY = True
except Exception:
    np = None  # type: ignore
    _HAS_NUMPY = False

try:
    import pandas as pd  # type: ignore

    _HAS_PANDAS = True
except Exception:
    pd = None  # type: ignore
    _HAS_PANDAS = False

try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    _HAS_PARQUET = True
except Exception:
    pa = None  # type: ignore
    pq = None  # type: ignore
    _HAS_PARQUET = False

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
SERVICE_VERSION = "4.4.0"
logger = logging.getLogger("TFB.Audit")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# -----------------------------------------------------------------------------
# Executors (global, persistent) - used ONLY for file writes / CPU-light work
# -----------------------------------------------------------------------------
def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))

_MAX_WORKERS = _clamp_int(int(os.getenv("AUDIT_MAX_WORKERS", "8") or "8"), 2, 32)
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=_MAX_WORKERS,
    thread_name_prefix="AuditWorker",
)

# -----------------------------------------------------------------------------
# Enums
# -----------------------------------------------------------------------------
class AuditSeverity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"
    OK = "OK"


class DataQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"


class ProviderHealth(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"
    UNKNOWN = "UNKNOWN"


class AnomalyType(str, Enum):
    PRICE_SPIKE = "PRICE_SPIKE"
    VOLUME_SURGE = "VOLUME_SURGE"
    STALE_DATA = "STALE_DATA"
    MISSING_DATA = "MISSING_DATA"
    OUTLIER = "OUTLIER"


class AlertPriority(str, Enum):
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"
    P4 = "P4"
    P5 = "P5"


# -----------------------------------------------------------------------------
# Config + Results
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class AuditConfig:
    stale_hours: float = 72.0
    hard_stale_hours: float = 168.0
    min_price: float = 0.01
    max_price: float = 1_000_000.0
    max_abs_change_pct: float = 60.0
    min_confidence_pct: float = 30.0

    # history expectations (lightweight)
    min_history_points_soft: int = 60
    min_history_points_hard: int = 20

    # output (informational)
    export_formats: List[str] = field(default_factory=lambda: ["json", "csv"])

    def to_dict(self) -> Dict[str, Any]:
        return {k: getattr(self, k) for k in self.__slots__}


@dataclass(slots=True)
class RemediationAction:
    issue: str
    action: str
    priority: AlertPriority
    estimated_time_minutes: int
    automated: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass(slots=True)
class AuditResult:
    symbol: str
    page: str
    provider: str
    severity: AuditSeverity
    data_quality: DataQuality
    provider_health: ProviderHealth

    price: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    age_hours: Optional[float] = None
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    history_points: int = 0
    confidence_pct: float = 0.0

    issues: List[str] = field(default_factory=list)
    strategy_notes: List[str] = field(default_factory=list)
    anomalies: List[AnomalyType] = field(default_factory=list)

    remediation_actions: List[RemediationAction] = field(default_factory=list)
    root_cause: Optional[str] = None
    error: Optional[str] = None

    audit_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    audit_version: str = SERVICE_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "page": self.page,
            "provider": self.provider,
            "severity": self.severity.value,
            "data_quality": self.data_quality.value,
            "provider_health": self.provider_health.value,
            "price": self.price,
            "change_pct": self.change_pct,
            "volume": self.volume,
            "market_cap": self.market_cap,
            "age_hours": self.age_hours,
            "last_updated_utc": self.last_updated_utc,
            "last_updated_riyadh": self.last_updated_riyadh,
            "history_points": self.history_points,
            "confidence_pct": self.confidence_pct,
            "issues": list(self.issues),
            "strategy_notes": list(self.strategy_notes),
            "anomalies": [a.value for a in self.anomalies],
            "remediation_actions": [
                {
                    "issue": a.issue,
                    "action": a.action,
                    "priority": a.priority.value,
                    "estimated_time_minutes": a.estimated_time_minutes,
                    "automated": a.automated,
                    "created_at": a.created_at,
                }
                for a in self.remediation_actions
            ],
            "root_cause": self.root_cause,
            "error": self.error,
            "audit_timestamp": self.audit_timestamp,
            "audit_version": self.audit_version,
        }


@dataclass(slots=True)
class AuditSummary:
    total_assets: int = 0
    by_severity: Dict[str, int] = field(default_factory=dict)
    by_quality: Dict[str, int] = field(default_factory=dict)
    by_provider_health: Dict[str, int] = field(default_factory=dict)
    issue_counts: Dict[str, int] = field(default_factory=dict)
    anomaly_counts: Dict[str, int] = field(default_factory=dict)
    audit_duration_ms: float = 0.0
    audit_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    audit_version: str = SERVICE_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_assets": self.total_assets,
            "by_severity": dict(self.by_severity),
            "by_quality": dict(self.by_quality),
            "by_provider_health": dict(self.by_provider_health),
            "issue_counts": dict(self.issue_counts),
            "anomaly_counts": dict(self.anomaly_counts),
            "audit_duration_ms": self.audit_duration_ms,
            "audit_timestamp": self.audit_timestamp,
            "audit_version": self.audit_version,
        }


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_RIYADH_TZ = timezone(timedelta(hours=3))


def safe_str(x: Any, default: str = "") -> str:
    """Extremely defensive string coercion (never throws)."""
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            s = x.decode("utf-8", errors="replace").strip()
            return s if s else default
        s = str(x).strip()
        return s if s else default
    except Exception:
        return default


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(utc_iso_str: Optional[str] = None) -> str:
    try:
        if utc_iso_str:
            dt = _parse_any_time(utc_iso_str)
        else:
            dt = datetime.now(timezone.utc)
        if not dt:
            dt = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_RIYADH_TZ).isoformat()
    except Exception:
        return datetime.now(_RIYADH_TZ).isoformat()


def _parse_any_time(value: Any) -> Optional[datetime]:
    """
    Accepts:
    - ISO strings (with optional Z)
    - common formats: "YYYY-MM-DD HH:MM:SS" (UTC assumed)
    - epoch seconds/ms (int/float or digit strings)
    Never throws.
    """
    if value is None:
        return None

    # epoch numeric
    if isinstance(value, (int, float)) and value > 0:
        try:
            secs = float(value) / 1000.0 if float(value) > 2_000_000_000 else float(value)
            return datetime.fromtimestamp(secs, tz=timezone.utc)
        except Exception:
            return None

    s = safe_str(value, "")
    if not s:
        return None

    s = s.replace("Z", "+00:00").strip()

    # epoch numeric string
    if s.isdigit():
        try:
            n = int(s)
            secs = n / 1000.0 if n > 2_000_000_000 else n
            return datetime.fromtimestamp(secs, tz=timezone.utc)
        except Exception:
            pass

    # ISO
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass

    # common fallbacks
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt2 = datetime.strptime(s, fmt)  # noqa: DTZ007
            return dt2.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    return None


def _age_hours(utc_like: Any) -> Optional[float]:
    dt = _parse_any_time(utc_like)
    if not dt:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600.0
    except Exception:
        return None


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        if isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            f = float(x)
            return default if (math.isnan(f) or math.isinf(f)) else f
        s = safe_str(x, "")
        if not s:
            return default
        s = s.translate(_ARABIC_DIGITS).replace(",", "")
        if s.endswith("%"):
            s = s[:-1].strip()
        f = float(s) if s else default
        if f is None:
            return default
        return default if (math.isnan(f) or math.isinf(f)) else float(f)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        v = _safe_float(x, None)
        return int(round(v)) if v is not None else default
    except Exception:
        return default


def _norm_symbol(sym: Any) -> str:
    s = safe_str(sym, "").strip().upper()
    if not s:
        return ""
    s = s.translate(_ARABIC_DIGITS)
    s = s.replace("TADAWUL:", "").replace(".TADAWUL", "")
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _dedupe_preserve(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for x in items:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _dedupe_enum_preserve(items: Iterable[AnomalyType]) -> List[AnomalyType]:
    out: List[AnomalyType] = []
    seen: Set[str] = set()
    for x in items:
        try:
            key = x.value
        except Exception:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out


async def _maybe_await(x: Any) -> Any:
    import inspect

    return await x if inspect.isawaitable(x) else x


def _chunk(lst: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        return [lst]
    return [lst[i : i + n] for i in range(0, len(lst), n)]


# -----------------------------------------------------------------------------
# Deps: symbols_reader + core.data_engine
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class Deps:
    ok: bool
    symbols_reader: Any
    data_engine: Any
    err: Optional[str] = None


def _load_deps() -> Deps:
    sr = None
    de = None
    err = None

    # symbols reader: try a few known locations
    for mod_name in ("integrations.symbols_reader", "symbols_reader", "integrations.symbols", "core.symbols_reader"):
        try:
            sr = __import__(mod_name, fromlist=["*"])
            break
        except Exception:
            sr = None

    # data engine wrapper (preferred)
    try:
        from core import data_engine as _de  # type: ignore

        de = _de
    except Exception as e:
        de = None
        err = f"core.data_engine import failed: {e}"

    ok = bool(sr and de)
    return Deps(ok=ok, symbols_reader=sr, data_engine=de, err=err)


DEPS = _load_deps()


def _get_page_symbols(symbols_reader: Any, page_key: str, spreadsheet_id: str) -> List[str]:
    """
    Supports:
      - symbols_reader.get_page_symbols(key, spreadsheet_id=...)
      - symbols_reader.get_symbols_for_page(...)
      - symbols_reader.read_page_symbols(...)
    """
    if symbols_reader is None:
        return []

    candidates = [
        getattr(symbols_reader, "get_page_symbols", None),
        getattr(symbols_reader, "get_symbols_for_page", None),
        getattr(symbols_reader, "read_page_symbols", None),
    ]

    for fn in candidates:
        if not callable(fn):
            continue
        try:
            res = fn(page_key, spreadsheet_id=spreadsheet_id)
            if isinstance(res, (list, tuple, set)):
                return [_norm_symbol(x) for x in res if _norm_symbol(x)]
        except TypeError:
            try:
                res = fn(spreadsheet_id, page_key)
                if isinstance(res, (list, tuple, set)):
                    return [_norm_symbol(x) for x in res if _norm_symbol(x)]
            except Exception:
                pass
        except Exception:
            pass

    return []


# -----------------------------------------------------------------------------
# Fetch quotes (aligned with core.data_engine public API)
# -----------------------------------------------------------------------------
def _obj_to_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if hasattr(x, "model_dump") and callable(getattr(x, "model_dump")):
        try:
            return x.model_dump()
        except Exception:
            pass
    if hasattr(x, "dict") and callable(getattr(x, "dict")):
        try:
            return x.dict()
        except Exception:
            pass
    try:
        return dict(getattr(x, "__dict__", {}) or {})
    except Exception:
        return {}


def _coerce_symbol_from_quote(q: Dict[str, Any]) -> str:
    for k in ("symbol", "requested_symbol", "ticker", "code", "id"):
        s = _norm_symbol(q.get(k))
        if s:
            return s
    return ""


def _pick_timestamp_utc(q: Dict[str, Any]) -> str:
    """
    Tries multiple keys; returns best-effort ISO string or "".
    Never throws.
    """
    for k in (
        "last_updated_utc",
        "forecast_updated_utc",
        "timestamp_utc",
        "updated_at_utc",
        "updated_utc",
        "ts_utc",
    ):
        v = q.get(k)
        dt = _parse_any_time(v)
        if dt:
            return dt.astimezone(timezone.utc).isoformat()
    for k in ("timestamp", "ts", "updated_at"):
        v = q.get(k)
        dt = _parse_any_time(v)
        if dt:
            return dt.astimezone(timezone.utc).isoformat()
    return ""


async def _fetch_quotes_map(
    symbols: List[str],
    *,
    use_cache: bool,
    ttl: Optional[int],
    batch_size: int,
) -> Dict[str, Dict[str, Any]]:
    """
    Uses `core.data_engine.get_enriched_quotes()` if available; otherwise falls back to per-symbol calls.
    Returns: { SYMBOL: quote_dict }
    """
    de = DEPS.data_engine
    if de is None:
        return {s: {"error": "core.data_engine not available"} for s in symbols}

    symbols = _dedupe_preserve([_norm_symbol(s) for s in symbols if _norm_symbol(s)])
    if not symbols:
        return {}

    out: Dict[str, Dict[str, Any]] = {}

    get_many = getattr(de, "get_enriched_quotes", None)
    get_one = getattr(de, "get_enriched_quote", None)

    # Batch path
    if callable(get_many):
        for part in _chunk(symbols, max(1, int(batch_size))):
            try:
                res = await _maybe_await(get_many(part, use_cache=use_cache, ttl=ttl))

                if res is None:
                    for s in part:
                        out[s] = {"error": "engine returned None"}
                    continue

                # dict mapping
                if isinstance(res, dict):
                    for k, v in res.items():
                        kk = _norm_symbol(k)
                        if kk:
                            out[kk] = _obj_to_dict(v)
                    # ensure placeholders for missing
                    for s in part:
                        out.setdefault(s, {"error": "missing quote row"})
                    continue

                # list/tuple result
                if isinstance(res, (list, tuple)):
                    # Prefer embedded symbol mapping
                    temp: Dict[str, Dict[str, Any]] = {}
                    for item in res:
                        qd = _obj_to_dict(item)
                        sym = _coerce_symbol_from_quote(qd)
                        if sym:
                            temp[sym] = qd

                    if temp:
                        out.update(temp)
                        # Fill missing by index alignment (best-effort)
                        for idx, s in enumerate(part):
                            if s in out:
                                continue
                            if idx < len(res):
                                out[s] = _obj_to_dict(res[idx])
                            else:
                                out[s] = {"error": "missing quote row"}
                        continue

                    # Otherwise assume order aligns with input
                    for idx, s in enumerate(part):
                        if idx < len(res):
                            out[s] = _obj_to_dict(res[idx])
                        else:
                            out[s] = {"error": "missing quote row"}
                    continue

                # Unexpected type
                for s in part:
                    out[s] = {"error": f"unexpected get_enriched_quotes type: {type(res).__name__}"}

            except Exception as e:
                for s in part:
                    out[s] = {"error": f"get_enriched_quotes failed: {e}"}

        # fill missing with per-symbol (rare)
        missing = [s for s in symbols if s not in out]
        if missing and callable(get_one):
            for s in missing:
                try:
                    q = await _maybe_await(get_one(s, use_cache=use_cache, ttl=ttl))
                    out[s] = _obj_to_dict(q)
                except Exception as e:
                    out[s] = {"error": f"get_enriched_quote failed: {e}"}

        return out

    # Per-symbol fallback
    if callable(get_one):
        sem = asyncio.Semaphore(24)

        async def one(s: str) -> Tuple[str, Dict[str, Any]]:
            async with sem:
                try:
                    q = await _maybe_await(get_one(s, use_cache=use_cache, ttl=ttl))
                    return s, _obj_to_dict(q)
                except Exception as e:
                    return s, {"error": f"get_enriched_quote failed: {e}"}

        pairs = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=False)
        return {s: q for s, q in pairs}

    return {s: {"error": "No core.data_engine quote functions available"} for s in symbols}


# -----------------------------------------------------------------------------
# Audit logic
# -----------------------------------------------------------------------------
def _coerce_confidence_pct(x: Any) -> float:
    v = _safe_float(x, 0.0) or 0.0
    return float(v * 100.0) if 0.0 <= v <= 1.0 else float(v)


def _infer_history_points(q: Dict[str, Any]) -> int:
    if "history_points" in q:
        return _safe_int(q.get("history_points"), 0)
    if "history_len" in q:
        return _safe_int(q.get("history_len"), 0)
    h = q.get("history")
    if isinstance(h, list):
        return len(h)
    return 0


def _derive_provider(q: Dict[str, Any]) -> str:
    return safe_str(q.get("data_source") or q.get("provider") or q.get("source") or "unknown", "unknown") or "unknown"


def _detect_basic_anomalies(q: Dict[str, Any], config: AuditConfig) -> List[AnomalyType]:
    anomalies: List[AnomalyType] = []
    chg = _safe_float(q.get("percent_change") or q.get("change_pct"), None)
    if chg is not None and abs(chg) >= max(30.0, config.max_abs_change_pct * 0.75):
        anomalies.append(AnomalyType.PRICE_SPIKE)

    vol = _safe_float(q.get("volume"), None)
    if vol is not None and vol >= 2_000_000:
        anomalies.append(AnomalyType.VOLUME_SURGE)

    ts = _pick_timestamp_utc(q)
    age = _age_hours(ts)
    if age is not None and age >= config.stale_hours:
        anomalies.append(AnomalyType.STALE_DATA)

    if q.get("error"):
        anomalies.append(AnomalyType.MISSING_DATA)

    return anomalies


def _remediation_for_issues(issues: List[str]) -> List[RemediationAction]:
    actions: List[RemediationAction] = []
    for i in issues[:3]:
        if i in {"ZERO_PRICE", "PROVIDER_ERROR", "HARD_STALE_DATA"}:
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Failover provider / bypass cache / trigger rescue path",
                    priority=AlertPriority.P1,
                    estimated_time_minutes=10,
                    automated=True,
                )
            )
        elif i in {"STALE_DATA"}:
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Lower cache TTL / verify provider update frequency",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=30,
                    automated=False,
                )
            )
        elif i in {"INSUFFICIENT_HISTORY"}:
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Use shorter indicator windows / mark as recently listed",
                    priority=AlertPriority.P4,
                    estimated_time_minutes=15,
                    automated=False,
                )
            )
        else:
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Review mapping keys / provider payload fields",
                    priority=AlertPriority.P4,
                    estimated_time_minutes=20,
                    automated=False,
                )
            )
    return actions


def _compute_quality_score(q: Dict[str, Any], config: AuditConfig) -> Tuple[float, DataQuality]:
    score = 100.0
    price = _safe_float(q.get("current_price") or q.get("price"), None)
    if price is None:
        score -= 35
    elif price <= 0 or price < config.min_price:
        score -= 25
    elif price > config.max_price:
        score -= 10

    ts = _pick_timestamp_utc(q)
    age = _age_hours(ts)
    if age is None:
        score -= 20
    elif age > config.hard_stale_hours:
        score -= 25
    elif age > config.stale_hours:
        score -= 12

    if q.get("error"):
        score -= 30

    hist = _infer_history_points(q)
    if hist < config.min_history_points_hard:
        score -= 20
    elif hist < config.min_history_points_soft:
        score -= 8

    conf = _coerce_confidence_pct(q.get("forecast_confidence"))
    if 0 < conf < config.min_confidence_pct:
        score -= 5

    score = float(max(0.0, min(100.0, score)))
    if score >= 90:
        return score, DataQuality.EXCELLENT
    if score >= 75:
        return score, DataQuality.GOOD
    if score >= 60:
        return score, DataQuality.FAIR
    if score >= 40:
        return score, DataQuality.POOR
    return score, DataQuality.CRITICAL


def _compute_severity(issues: List[str], quality: DataQuality) -> AuditSeverity:
    if any(i in {"PROVIDER_ERROR", "ZERO_PRICE", "HARD_STALE_DATA", "ZOMBIE_TICKER"} for i in issues):
        return AuditSeverity.CRITICAL
    if issues and quality in {DataQuality.POOR, DataQuality.CRITICAL}:
        return AuditSeverity.HIGH
    if issues:
        return AuditSeverity.MEDIUM
    return AuditSeverity.OK


def _compute_provider_health(q: Dict[str, Any], config: AuditConfig) -> ProviderHealth:
    if q.get("error"):
        return ProviderHealth.UNHEALTHY
    age = _age_hours(_pick_timestamp_utc(q))
    if age is None:
        return ProviderHealth.UNKNOWN
    if age > config.hard_stale_hours:
        return ProviderHealth.UNHEALTHY
    if age > config.stale_hours:
        return ProviderHealth.DEGRADED
    return ProviderHealth.HEALTHY


async def _audit_one(symbol: str, page: str, q: Dict[str, Any], config: AuditConfig) -> AuditResult:
    symbol_n = _norm_symbol(symbol) or safe_str(symbol, "UNKNOWN")
    provider = _derive_provider(q)

    price = _safe_float(q.get("current_price") or q.get("price"), None)
    chg_pct = _safe_float(q.get("percent_change") or q.get("change_pct"), None)
    volume = _safe_int(q.get("volume"), 0) or None
    mcap = _safe_float(q.get("market_cap"), None)

    last_upd = _pick_timestamp_utc(q)
    age = _age_hours(last_upd) if last_upd else None

    issues: List[str] = []

    if price is None or price < config.min_price:
        issues.append("ZERO_PRICE")
    if price is not None and price > config.max_price:
        issues.append("PRICE_TOO_HIGH")

    if not last_upd:
        issues.append("NO_TIMESTAMP")
    elif age is not None and age > config.hard_stale_hours:
        issues.append("HARD_STALE_DATA")
    elif age is not None and age > config.stale_hours:
        issues.append("STALE_DATA")

    hist_pts = _infer_history_points(q)
    if hist_pts < config.min_history_points_hard:
        issues.append("INSUFFICIENT_HISTORY")

    if chg_pct is not None and abs(chg_pct) > config.max_abs_change_pct:
        issues.append("EXTREME_DAILY_MOVE")

    if q.get("error"):
        issues.append("PROVIDER_ERROR")

    dq_raw = safe_str(q.get("data_quality"), "").strip().upper()
    if ("ZERO_PRICE" in issues and ("STALE_DATA" in issues or "HARD_STALE_DATA" in issues)) or dq_raw in {"MISSING", "BAD"}:
        issues.append("ZOMBIE_TICKER")

    _, quality = _compute_quality_score(q, config)
    anomalies = _dedupe_enum_preserve(_detect_basic_anomalies(q, config))
    severity = _compute_severity(issues, quality)
    phealth = _compute_provider_health(q, config)

    strategy_notes: List[str] = []
    r1w = _safe_float(q.get("returns_1w"), None)
    trend = safe_str(q.get("trend_signal"), "").strip().upper()
    if trend == "UPTREND" and r1w is not None and r1w < -4.0:
        strategy_notes.append("TREND_BREAK_BEAR")
    if trend == "DOWNTREND" and r1w is not None and r1w > 4.0:
        strategy_notes.append("TREND_BREAK_BULL")

    remediation = _remediation_for_issues(issues)

    return AuditResult(
        symbol=symbol_n,
        page=page,
        provider=provider,
        severity=severity,
        data_quality=quality,
        provider_health=phealth,
        price=price,
        change_pct=chg_pct,
        volume=volume,
        market_cap=mcap,
        age_hours=age,
        last_updated_utc=(last_upd or None),
        last_updated_riyadh=_riyadh_iso(last_upd) if last_upd else _riyadh_iso(),
        history_points=hist_pts,
        confidence_pct=round(_coerce_confidence_pct(q.get("forecast_confidence")), 1),
        issues=_dedupe_preserve(issues),
        strategy_notes=_dedupe_preserve(strategy_notes),
        anomalies=anomalies,
        remediation_actions=remediation,
        root_cause=("Provider data unavailable" if "PROVIDER_ERROR" in issues else None),
        error=(safe_str(q.get("error"), "") or None),
    )


def _generate_summary(results: List[AuditResult]) -> AuditSummary:
    s = AuditSummary()
    s.total_assets = len(results)
    for r in results:
        s.by_severity[r.severity.value] = s.by_severity.get(r.severity.value, 0) + 1
        s.by_quality[r.data_quality.value] = s.by_quality.get(r.data_quality.value, 0) + 1
        s.by_provider_health[r.provider_health.value] = s.by_provider_health.get(r.provider_health.value, 0) + 1
        for issue in r.issues:
            s.issue_counts[issue] = s.issue_counts.get(issue, 0) + 1
        for an in r.anomalies:
            s.anomaly_counts[an.value] = s.anomaly_counts.get(an.value, 0) + 1
    return s


# -----------------------------------------------------------------------------
# Exporters (async-safe)
# -----------------------------------------------------------------------------
def _maybe_sign_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    key = safe_str(os.getenv("AUDIT_HMAC_KEY"), "")
    if not key:
        return payload
    try:
        body = json_dumps(payload).encode("utf-8")
        sig = hmac.new(key.encode("utf-8"), body, hashlib.sha256).hexdigest()
        payload2 = dict(payload)
        payload2["_hmac_sha256"] = sig
        return payload2
    except Exception:
        return payload


async def _export_json(path: str, data: Dict[str, Any], *, indent: int = 0) -> None:
    loop = asyncio.get_running_loop()
    payload = _maybe_sign_json(data)

    def _write() -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(json_dumps(payload, indent=int(indent)))

    await loop.run_in_executor(_CPU_EXECUTOR, _write)


async def _export_csv(path: str, results: List[AuditResult]) -> None:
    if not results:
        return

    loop = asyncio.get_running_loop()
    headers = [
        "symbol",
        "page",
        "provider",
        "severity",
        "data_quality",
        "provider_health",
        "price",
        "change_pct",
        "age_hours",
        "history_points",
        "confidence_pct",
        "issues",
        "anomalies",
        "error",
        "last_updated_utc",
    ]

    def _write() -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for r in results:
                d = r.to_dict()
                w.writerow(
                    {
                        "symbol": d.get("symbol"),
                        "page": d.get("page"),
                        "provider": d.get("provider"),
                        "severity": d.get("severity"),
                        "data_quality": d.get("data_quality"),
                        "provider_health": d.get("provider_health"),
                        "price": d.get("price"),
                        "change_pct": d.get("change_pct"),
                        "age_hours": d.get("age_hours"),
                        "history_points": d.get("history_points"),
                        "confidence_pct": d.get("confidence_pct"),
                        "issues": ", ".join(d.get("issues") or []),
                        "anomalies": ", ".join(d.get("anomalies") or []),
                        "error": d.get("error"),
                        "last_updated_utc": d.get("last_updated_utc"),
                    }
                )

    await loop.run_in_executor(_CPU_EXECUTOR, _write)


async def _export_parquet(path: str, results: List[AuditResult]) -> None:
    if not (_HAS_PARQUET and _HAS_PANDAS and results):
        return

    loop = asyncio.get_running_loop()

    def _write() -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([r.to_dict() for r in results])  # type: ignore
        table = pa.Table.from_pandas(df)  # type: ignore
        pq.write_table(table, path)  # type: ignore

    await loop.run_in_executor(_CPU_EXECUTOR, _write)


async def _export_excel(path: str, results: List[AuditResult], summary: AuditSummary) -> None:
    if not _HAS_PANDAS:
        return

    loop = asyncio.get_running_loop()

    def _write() -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:  # type: ignore
            pd.DataFrame([r.to_dict() for r in results]).to_excel(writer, sheet_name="Audit Results", index=False)  # type: ignore
            pd.DataFrame([summary.to_dict()]).to_excel(writer, sheet_name="Summary", index=False)  # type: ignore

    await loop.run_in_executor(_CPU_EXECUTOR, _write)


async def _run_exports(export_tasks: List[Awaitable[None]]) -> None:
    if not export_tasks:
        return
    await asyncio.gather(*export_tasks)


# -----------------------------------------------------------------------------
# Orchestrator
# -----------------------------------------------------------------------------
async def run_audit(
    *,
    keys: List[str],
    spreadsheet_id: str,
    refresh: bool,
    concurrency: int,
    max_symbols_per_page: int,
    config: AuditConfig,
) -> Tuple[List[AuditResult], AuditSummary]:
    if not DEPS.ok:
        logger.error(
            "Dependencies not loaded. symbols_reader=%s, data_engine=%s, err=%s",
            bool(DEPS.symbols_reader),
            bool(DEPS.data_engine),
            DEPS.err,
        )
        return [], AuditSummary()

    sid = (spreadsheet_id or os.getenv("DEFAULT_SPREADSHEET_ID", "")).strip()
    if not sid:
        logger.error("No spreadsheet_id provided and DEFAULT_SPREADSHEET_ID is empty.")
        return [], AuditSummary()

    use_cache = not bool(refresh)
    ttl = None  # respect engine defaults
    batch_size = max(50, int(os.getenv("AUDIT_BATCH_SIZE", "200") or "200"))

    sem = asyncio.Semaphore(max(1, min(int(concurrency), 48)))

    all_results: List[AuditResult] = []
    start = time.time()

    for key in keys:
        page_key = (key or "").strip()
        if not page_key:
            continue

        symbols = _get_page_symbols(DEPS.symbols_reader, page_key, sid)
        symbols = _dedupe_preserve(symbols)
        if max_symbols_per_page > 0:
            symbols = symbols[: int(max_symbols_per_page)]

        if not symbols:
            logger.warning("No symbols for page=%s", page_key)
            continue

        logger.info("Page=%s | symbols=%s | use_cache=%s", page_key, len(symbols), use_cache)

        quotes_map = await _fetch_quotes_map(symbols, use_cache=use_cache, ttl=ttl, batch_size=batch_size)

        async def audit_one(sym: str) -> AuditResult:
            async with sem:
                q = quotes_map.get(sym) or {}
                return await _audit_one(sym, page_key, q, config)

        page_results = await asyncio.gather(*[audit_one(s) for s in symbols], return_exceptions=False)
        all_results.extend(page_results)

    summary = _generate_summary(all_results)
    summary.audit_duration_ms = (time.time() - start) * 1000.0
    return all_results, summary


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def _severity_rank(s: AuditSeverity) -> int:
    order = {
        AuditSeverity.CRITICAL: 0,
        AuditSeverity.HIGH: 1,
        AuditSeverity.MEDIUM: 2,
        AuditSeverity.LOW: 3,
        AuditSeverity.INFO: 4,
        AuditSeverity.OK: 9,
    }
    return order.get(s, 99)


def main() -> None:
    p = argparse.ArgumentParser(description="TFB Data Quality Auditor")
    p.add_argument("--keys", nargs="*", default=["MARKET_LEADERS"], help="Sheet page keys to audit")
    p.add_argument("--sheet-id", dest="sheet_id", default="", help="Google Sheet ID (fallback: DEFAULT_SPREADSHEET_ID)")
    p.add_argument("--refresh", type=int, default=1, help="1=bypass cache (fresh), 0=use cache")
    p.add_argument("--concurrency", type=int, default=12)
    p.add_argument("--max-symbols", type=int, default=0, help="limit symbols per page (0=all)")

    p.add_argument("--json-out", default="", help="write JSON report")
    p.add_argument("--csv-out", default="", help="write CSV alerts")
    p.add_argument("--parquet-out", default="", help="write Parquet (optional deps)")
    p.add_argument("--excel-out", default="", help="write Excel (requires pandas+openpyxl)")

    p.add_argument("--alerts-only", type=int, default=0, help="1=only export alerts; 0=export all results in JSON")
    p.add_argument("--include-ok", type=int, default=0, help="1=include OK rows in console summary")
    p.add_argument("--top", type=int, default=15, help="top N rows to print to console")
    p.add_argument("--json-indent", type=int, default=0, help="JSON indentation (0=no pretty print)")
    args = p.parse_args()

    config = AuditConfig()

    t0 = time.time()
    results, summary = asyncio.run(
        run_audit(
            keys=list(args.keys or []),
            spreadsheet_id=args.sheet_id,
            refresh=bool(args.refresh),
            concurrency=int(args.concurrency),
            max_symbols_per_page=int(args.max_symbols),
            config=config,
        )
    )
    summary.audit_duration_ms = (time.time() - t0) * 1000.0

    # Alerts
    alerts = [r for r in results if r.severity != AuditSeverity.OK]

    # Console snapshot (optional, safe)
    try:
        top_n = max(0, int(args.top))
        if top_n > 0:
            view = results if int(args.include_ok) == 1 else alerts
            view_sorted = sorted(
                view,
                key=lambda r: (_severity_rank(r.severity), r.page, r.symbol),
            )
            logger.info("Top findings (n=%s):", min(top_n, len(view_sorted)))
            for r in view_sorted[:top_n]:
                msg = f"{r.severity.value:<8} | {r.page:<18} | {r.symbol:<12} | age_h={r.age_hours if r.age_hours is not None else 'NA'} | issues={','.join(r.issues[:3])}"
                if r.error:
                    msg += f" | err={r.error[:120]}"
                logger.info(msg)
    except Exception:
        pass

    # Exports (run gather inside event loop)
    export_tasks: List[Awaitable[None]] = []

    if args.json_out:
        if int(args.alerts_only) == 1:
            payload = {"summary": summary.to_dict(), "alerts": [r.to_dict() for r in alerts]}
        else:
            payload = {"summary": summary.to_dict(), "results": [r.to_dict() for r in results], "alerts": [r.to_dict() for r in alerts]}
        export_tasks.append(_export_json(args.json_out, payload, indent=int(args.json_indent)))

    if args.csv_out:
        export_tasks.append(_export_csv(args.csv_out, alerts))

    if args.parquet_out:
        export_tasks.append(_export_parquet(args.parquet_out, results))

    if args.excel_out:
        export_tasks.append(_export_excel(args.excel_out, results, summary))

    if export_tasks:
        asyncio.run(_run_exports(export_tasks))

    logger.info(
        "Audit Complete | assets=%s | critical=%s | high=%s | medium=%s | duration_ms=%.0f",
        summary.total_assets,
        summary.by_severity.get("CRITICAL", 0),
        summary.by_severity.get("HIGH", 0),
        summary.by_severity.get("MEDIUM", 0),
        summary.audit_duration_ms,
    )

    # Exit code policy (stable)
    exit_code = 0
    if summary.by_severity.get("CRITICAL", 0) > 0:
        exit_code = 3
    elif summary.by_severity.get("HIGH", 0) > 0:
        exit_code = 2
    elif summary.by_severity.get("MEDIUM", 0) > 0:
        exit_code = 1

    try:
        _CPU_EXECUTOR.shutdown(wait=False, cancel_futures=True)  # py3.9+
    except Exception:
        try:
            _CPU_EXECUTOR.shutdown(wait=False)
        except Exception:
            pass

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
