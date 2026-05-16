#!/usr/bin/env python3
"""
scripts/audit_data_quality.py
================================================================================
TADAWUL FAST BRIDGE — ENTERPRISE DATA QUALITY AUDITOR (v4.7.0)
================================================================================
Aligned • Production-safe • Engine-compatible • Async-safe exports • Deterministic

What's improved vs v4.6.0  —  ENGINE v5.72.0 + ENRICHED_QUOTE v4.6.0 ALIGNMENT
- 🔑 ALIGN: data_engine_v2 v5.72.0 (May 2026 audit) + enriched_quote v4.6.0
      added seven new warning tag families that v4.6.0 of this script
      didn't recognize. v4.7.0 wires them into the engine-warning
      surfacing path so they appear as audit issues and roll up into
      summary counts:
        - "sanitized:{field}_out_of_range"     (engine sanitized an
            extreme provider value: ev_ebitda, pe_ttm, debt_to_equity,
            etc.) → PROVIDER_VALUE_SANITIZED (LOW)
        - "v572:exchange_corrected_from=X"     (engine force-overrode
            a mislabeled foreign exchange, e.g. NASDAQ→Boursa Kuwait
            for *.KW tickers) → IDENTITY_CORRECTED_BY_ENGINE (INFO)
        - "v572:currency_corrected_from=X"     (engine force-overrode
            currency, e.g. USD→KWD for Kuwait tickers) → same audit
            tag (the bare "v572" key catches both v572:* variants)
        - "empty_row_no_provider_data"         (engine detected a row
            with no usable provider data and reset recommendation to
            NA) → EMPTY_ROW (HIGH)
        - "market_cap_currency_suspect"        (engine flagged a
            market_cap value as inconsistent with its currency label)
            → MARKET_CAP_CURRENCY_SUSPECT (MEDIUM)
        - "revenue_currency_suspect"           (same for revenue)
            → REVENUE_CURRENCY_SUSPECT (MEDIUM)
        - "quote_current_price_missing"        (provider quote endpoint
            returned no price; engine fell back to chart) →
            QUOTE_PRICE_MISSING (MEDIUM)
        - "inferred_from"                      (bare key matching any
            "inferred_from=*" tag added when identity came from the
            suffix table rather than the provider) →
            IDENTITY_INFERRED_FROM_SUFFIX (INFO)
- 🔑 SEVERITY: `_compute_severity` re-tiered to keep INFO/LOW tags
      from bubbling to MEDIUM by default. Rows with ONLY info-tier tags
      now report INFO; rows with only info+low tags report LOW. Specific
      data-integrity tags (MARKET_CAP_CURRENCY_SUSPECT etc.) still hit
      MEDIUM; EMPTY_ROW joins UNIT_DRIFT_*/PROVIDER_PERCENT_DROPPED at
      HIGH. Exit code semantics are unchanged: INFO and LOW both → 0.
- 🔑 REMEDIATION: `_remediation_for_issues` extended with six new
      action paths so the alert payload tells operators what to do
      (e.g. "force refresh and verify symbol is still listed" for
      EMPTY_ROW; "verify currency mapping in symbols_reader" for the
      currency-suspect tags).
- ARCH: severity-tier sets (`_SEVERITY_CRITICAL_TAGS`, `_HIGH_`, `_MEDIUM_`,
      `_LOW_`, `_INFO_`) hoisted to module level so tests and summary
      builders can reference them. Internal-only — not exported.
- KEEP: every v4.6.0 capability preserved. percent_change FRACTION
      contract, UNIT_DRIFT defense layer, percent-warning surfacing,
      `--legacy-percent-units` and `AUDIT_PERCENT_CONTRACT` flags,
      HMAC signing, all four export formats, exit codes.

What's improved vs v4.5.0  —  DATA_ENGINE_V2 v5.67.0 FRACTION-CONTRACT ALIGNMENT
- 🔑 ALIGN: data_engine_v2 v5.67.0 (May 13 2026) switched four percent-unit
      fields from POINTS to FRACTION to match 04_Format.gs v2.7.0's
      `_KNOWN_FRACTION_PERCENT_COLUMNS_`:
        - percent_change             ([-0.50, +0.50] daily cap)
        - upside_pct                 ([-0.90, +2.00] cap)
        - max_drawdown_1y            (signed; [-1.0, 0])
        - var_95_1d                  (typically < 0.10)
      v4.5.0 still treated `percent_change` as POINTS — so its 60% daily
      threshold (`max_abs_change_pct`) and the 30% PRICE_SPIKE threshold
      in `_detect_basic_anomalies` compared against fraction values like
      0.025 (= 2.5% daily) and never fired for any move under +/-60% in
      fraction form (= +/-6000% in points). SILENT REGRESSION: legitimate
      +/-60% daily moves on v5.67.0 data went unflagged.
- 🔑 NEW: `AuditConfig.percent_change_contract` — defaults to "fraction"
      (v5.67.0+ canonical). Legacy "points" mode preserved for pre-v5.67.0
      cached/snapshot data via `--legacy-percent-units 1` CLI flag or
      `AUDIT_PERCENT_CONTRACT=points` env var.
- 🔑 NEW: `_as_percent_change_points()` helper — converts fraction or
      points input to consistent POINTS form using the configured contract.
- 🔑 NEW: UNIT_DRIFT defense layer. `_audit_unit_drift_fraction_fields()`
      flags rows where any of the four v5.67.0 fraction fields exceeds
      plausible bounds:
        - |percent_change| > 1.0  (>100% daily move impossible)
        - |upside_pct| > 5.0      (>500% upside ≈ synthesizer bug)
        - |max_drawdown_1y| > 1.0 (>100% drawdown impossible)
        - |var_95_1d| > 1.0       (>100% VaR impossible)
- 🔑 NEW: Engine-warning-tag surfacing (mirrors scoring.py v5.2.5 PHASE-L):
        - "percent_change_recomputed"            → PROVIDER_PERCENT_RECOMPUTED (LOW)
        - "percent_change_clamped_from_provider" → PROVIDER_PERCENT_CLAMPED (MEDIUM)
        - "percent_change_suspect_dropped"       → PROVIDER_PERCENT_DROPPED (HIGH)
- 🔑 NEW: `AuditConfig.enable_unit_drift_detection` (default True).
- KEEP: every v4.5.0 fix preserved — `_get_page_symbols` dict handling,
      `_extract_symbols_from_reader_payload`, `_TRUTHY`/`_FALSY`,
      `SCRIPT_VERSION` alias, executor try/finally, async-safe exports,
      HMAC signing, exit codes.

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
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union

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
SERVICE_VERSION = "4.7.0"
SCRIPT_VERSION = SERVICE_VERSION  # v4.5.0: alias for cross-script consistency

logger = logging.getLogger("TFB.Audit")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# -----------------------------------------------------------------------------
# Executors
# -----------------------------------------------------------------------------
def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))


_MAX_WORKERS = _clamp_int(int(os.getenv("AUDIT_MAX_WORKERS", "8") or "8"), 2, 32)
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=_MAX_WORKERS,
    thread_name_prefix="AuditWorker",
)


def _shutdown_executor(wait: bool = False) -> None:
    """Idempotent shutdown helper. Safe to call multiple times / in finally."""
    try:
        _CPU_EXECUTOR.shutdown(wait=wait, cancel_futures=True)
    except Exception:
        try:
            _CPU_EXECUTOR.shutdown(wait=wait)
        except Exception:
            pass


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
# v4.6.0 — Unit-contract constants
# -----------------------------------------------------------------------------
PERCENT_CONTRACT_FRACTION = "fraction"
PERCENT_CONTRACT_POINTS = "points"
_PERCENT_CONTRACT_CHOICES: Set[str] = {PERCENT_CONTRACT_FRACTION, PERCENT_CONTRACT_POINTS}

_UNIT_DRIFT_BOUNDS_FRACTION: Dict[str, float] = {
    "percent_change":   1.0,
    "upside_pct":       5.0,
    "max_drawdown_1y":  1.0,
    "var_95_1d":        1.0,
}

_ENGINE_WARNING_TO_ISSUE: Dict[str, str] = {
    # v4.6.0: percent_change repair tags
    "percent_change_recomputed":            "PROVIDER_PERCENT_RECOMPUTED",
    "percent_change_clamped_from_provider": "PROVIDER_PERCENT_CLAMPED",
    "percent_change_suspect_dropped":       "PROVIDER_PERCENT_DROPPED",
    # v4.7.0: engine v5.72.0 + enriched_quote v4.6.0 tag families.
    # "sanitized" and "v572" are BARE KEYS (matched after splitting on
    # ":") so they catch every "sanitized:<field>_out_of_range" and
    # every "v572:<field>_corrected_from=X" variant in one entry.
    "sanitized":                            "PROVIDER_VALUE_SANITIZED",
    "v572":                                 "IDENTITY_CORRECTED_BY_ENGINE",
    "inferred_from":                        "IDENTITY_INFERRED_FROM_SUFFIX",
    "empty_row_no_provider_data":           "EMPTY_ROW",
    "market_cap_currency_suspect":          "MARKET_CAP_CURRENCY_SUSPECT",
    "revenue_currency_suspect":             "REVENUE_CURRENCY_SUSPECT",
    "quote_current_price_missing":          "QUOTE_PRICE_MISSING",
}


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

    min_history_points_soft: int = 60
    min_history_points_hard: int = 20

    # v4.6.0 — unit-contract for percent_change interpretation.
    # "fraction" matches engine v5.67.0+ (default). Use "points" for
    # pre-v5.67.0 cached/snapshot data.
    percent_change_contract: str = PERCENT_CONTRACT_FRACTION

    # v4.6.0 — toggle the UNIT_DRIFT_* defense layer.
    enable_unit_drift_detection: bool = True

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

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


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
    if value is None:
        return None

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

    if s.isdigit():
        try:
            n = int(s)
            secs = n / 1000.0 if n > 2_000_000_000 else n
            return datetime.fromtimestamp(secs, tz=timezone.utc)
        except Exception:
            pass

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass

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
# v4.6.0 — Engine v5.67.0 contract helpers
# -----------------------------------------------------------------------------

def _as_percent_change_points(value: Any, contract: str = PERCENT_CONTRACT_FRACTION) -> Optional[float]:
    """
    v4.6.0: Normalize a `percent_change`-style value to consistent POINTS form
    using the configured contract.

    Engine v5.67.0+ emit contract is FRACTION (e.g., 0.025 for +2.5% daily).
    Pre-v5.67.0 engines emitted POINTS (e.g., 2.5).

    Behavior:
      contract == "fraction"  ->  multiply input by 100 (assumes fraction storage)
      contract == "points"    ->  pass through (assumes points storage)
      unknown contract        ->  defaults to "fraction"

    None inputs pass through as None. Invalid coercions return None.
    """
    f = _safe_float(value, None)
    if f is None:
        return None

    c = (contract or PERCENT_CONTRACT_FRACTION).strip().lower()
    if c == PERCENT_CONTRACT_POINTS:
        return f
    return f * 100.0


def _warning_tags_from_row(q: Mapping[str, Any]) -> Set[str]:
    """
    v4.6.0: Parse the row's `warnings` field into a set of normalized tags.
    Mirrors the scoring.py v5.2.5 `_warning_tags_from_row` helper.

    Handles:
      - None / "" / missing -> empty set
      - "; "-joined string (engine v5.67.0 canonical shape)
      - Accidental List[str] / Tuple[str] / Set[str] input

    For "key:value" tags both the full tag and the bare key are added.
    """
    if q is None:
        return set()
    if not isinstance(q, Mapping):
        return set()
    raw = q.get("warnings")
    if raw is None:
        return set()

    parts: List[str] = []
    if isinstance(raw, str):
        for piece in raw.split(";"):
            s = piece.strip()
            if s:
                parts.append(s)
    elif isinstance(raw, (list, tuple, set)):
        for item in raw:
            if item is None:
                continue
            try:
                s = str(item).strip()
            except Exception:
                continue
            if s:
                parts.append(s)
    else:
        try:
            s = str(raw).strip()
        except Exception:
            s = ""
        if s:
            parts.append(s)

    out: Set[str] = set()
    for p in parts:
        out.add(p)
        if ":" in p:
            bare = p.split(":", 1)[0].strip()
            if bare:
                out.add(bare)
    return out


def _audit_unit_drift_fraction_fields(q: Mapping[str, Any]) -> List[str]:
    """
    v4.6.0: UNIT_DRIFT defense layer.

    Engine v5.67.0 emits four fields in FRACTION form with bounded cap
    ranges. If any of these arrives with magnitude exceeding plausible
    bounds, the data has almost certainly come from a pre-v5.67.0 source
    (still in POINTS form) or there's a regression in the emit path.

    Bounds per `_UNIT_DRIFT_BOUNDS_FRACTION`:
      - |percent_change| > 1.0  : >100% daily impossible
      - |upside_pct| > 5.0      : >500% upside ≈ synthesizer / unit bug
      - |max_drawdown_1y| > 1.0 : >100% drawdown impossible
      - |var_95_1d| > 1.0       : >100% VaR impossible

    Returns a list of issue tag strings. Empty list when all fields look
    legitimate or are missing.
    """
    if not isinstance(q, Mapping):
        return []

    out: List[str] = []

    pc_raw = q.get("percent_change") if "percent_change" in q else q.get("change_pct")
    pc = _safe_float(pc_raw, None)
    if pc is not None and abs(pc) > _UNIT_DRIFT_BOUNDS_FRACTION["percent_change"]:
        out.append("UNIT_DRIFT_PERCENT_CHANGE")

    up = _safe_float(q.get("upside_pct"), None)
    if up is not None and abs(up) > _UNIT_DRIFT_BOUNDS_FRACTION["upside_pct"]:
        out.append("UNIT_DRIFT_UPSIDE")

    dd = _safe_float(q.get("max_drawdown_1y"), None)
    if dd is not None and abs(dd) > _UNIT_DRIFT_BOUNDS_FRACTION["max_drawdown_1y"]:
        out.append("UNIT_DRIFT_DRAWDOWN")

    var = _safe_float(q.get("var_95_1d"), None)
    if var is not None and abs(var) > _UNIT_DRIFT_BOUNDS_FRACTION["var_95_1d"]:
        out.append("UNIT_DRIFT_VAR")

    return out


def _audit_engine_warning_tags(q: Mapping[str, Any]) -> List[str]:
    """
    v4.6.0: Surface engine-side data repairs as audit issues.

      "percent_change_recomputed"            -> PROVIDER_PERCENT_RECOMPUTED
      "percent_change_clamped_from_provider" -> PROVIDER_PERCENT_CLAMPED
      "percent_change_suspect_dropped"       -> PROVIDER_PERCENT_DROPPED
    """
    tags = _warning_tags_from_row(q)
    if not tags:
        return []

    out: List[str] = []
    for engine_tag, audit_tag in _ENGINE_WARNING_TO_ISSUE.items():
        if engine_tag in tags:
            out.append(audit_tag)
    return out


# -----------------------------------------------------------------------------
# Deps: symbols_reader + core.data_engine
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class Deps:
    ok: bool
    symbols_reader: Any
    symbols_reader_module_name: str
    data_engine: Any
    err: Optional[str] = None


def _load_deps() -> Deps:
    sr = None
    sr_name = ""
    de = None
    err = None

    for mod_name in (
        "integrations.symbols_reader",
        "symbols_reader",
        "integrations.symbols",
        "core.symbols_reader",
    ):
        try:
            sr = __import__(mod_name, fromlist=["*"])
            sr_name = mod_name
            break
        except Exception:
            sr = None

    for de_name in ("core.data_engine", "core.data_engine_v2"):
        try:
            de = __import__(de_name, fromlist=["*"])
            break
        except Exception as e:
            de = None
            err = f"{de_name} import failed: {e}"

    ok = bool(sr and de)
    return Deps(
        ok=ok,
        symbols_reader=sr,
        symbols_reader_module_name=sr_name,
        data_engine=de,
        err=err,
    )


DEPS = _load_deps()


def _extract_symbols_from_reader_payload(res: Any) -> List[str]:
    """
    v4.5.0: canonical extractor for symbols_reader return payloads.
    Handles both list-style and dict-style returns.
    """
    if res is None:
        return []

    if isinstance(res, (list, tuple, set)):
        return [_norm_symbol(x) for x in res if _norm_symbol(x)]

    if isinstance(res, dict):
        for key in ("symbols", "all"):
            seq = res.get(key)
            if isinstance(seq, (list, tuple, set)) and seq:
                return [_norm_symbol(x) for x in seq if _norm_symbol(x)]

        merged: List[str] = []
        seen: Set[str] = set()
        for key in ("ksa", "global"):
            seq = res.get(key)
            if not isinstance(seq, (list, tuple, set)):
                continue
            for x in seq:
                nx = _norm_symbol(x)
                if nx and nx not in seen:
                    seen.add(nx)
                    merged.append(nx)
        if merged:
            return merged

        by_type = res.get("by_type")
        if isinstance(by_type, dict):
            out: List[str] = []
            seen2: Set[str] = set()
            for _bucket, seq in by_type.items():
                if not isinstance(seq, (list, tuple, set)):
                    continue
                for x in seq:
                    nx = _norm_symbol(x)
                    if nx and nx not in seen2:
                        seen2.add(nx)
                        out.append(nx)
            if out:
                return out

    return []


def _get_page_symbols(symbols_reader: Any, page_key: str, spreadsheet_id: str) -> List[str]:
    """
    v4.5.0: handles both dict-style and list-style return types.
    """
    if symbols_reader is None:
        return []

    named_candidates: Tuple[Tuple[str, Dict[str, Any]], ...] = (
        ("get_page_symbols", {"call_style": "positional_key"}),
        ("get_symbols_for_page", {"call_style": "kw_page"}),
        ("list_symbols_for_page", {"call_style": "kw_page"}),
        ("get_symbols", {"call_style": "kw_page"}),
        ("list_symbols", {"call_style": "kw_page"}),
        ("read_page_symbols", {"call_style": "positional_key"}),
    )

    for attr_name, meta in named_candidates:
        fn = getattr(symbols_reader, attr_name, None)
        if not callable(fn):
            continue

        call_style = meta.get("call_style")
        res: Any = None
        try:
            if call_style == "positional_key":
                try:
                    res = fn(page_key, spreadsheet_id=spreadsheet_id)
                except TypeError:
                    try:
                        res = fn(spreadsheet_id, page_key)
                    except Exception:
                        res = None
            elif call_style == "kw_page":
                try:
                    res = fn(page=page_key, spreadsheet_id=spreadsheet_id)
                except TypeError:
                    try:
                        res = fn(sheet=page_key, spreadsheet_id=spreadsheet_id)
                    except TypeError:
                        try:
                            res = fn(page_key, spreadsheet_id=spreadsheet_id)
                        except Exception:
                            res = None
        except Exception:
            res = None

        extracted = _extract_symbols_from_reader_payload(res)
        if extracted:
            return _dedupe_preserve(extracted)

    return []


# -----------------------------------------------------------------------------
# Fetch quotes
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
    de = DEPS.data_engine
    if de is None:
        return {s: {"error": "core.data_engine not available"} for s in symbols}

    symbols = _dedupe_preserve([_norm_symbol(s) for s in symbols if _norm_symbol(s)])
    if not symbols:
        return {}

    out: Dict[str, Dict[str, Any]] = {}

    get_many = getattr(de, "get_enriched_quotes", None)
    get_one = getattr(de, "get_enriched_quote", None)

    if callable(get_many):
        for part in _chunk(symbols, max(1, int(batch_size))):
            try:
                res = await _maybe_await(get_many(part, use_cache=use_cache, ttl=ttl))

                if res is None:
                    for s in part:
                        out[s] = {"error": "engine returned None"}
                    continue

                if isinstance(res, dict):
                    for k, v in res.items():
                        kk = _norm_symbol(k)
                        if kk:
                            out[kk] = _obj_to_dict(v)
                    for s in part:
                        out.setdefault(s, {"error": "missing quote row"})
                    continue

                if isinstance(res, (list, tuple)):
                    temp: Dict[str, Dict[str, Any]] = {}
                    for item in res:
                        qd = _obj_to_dict(item)
                        sym = _coerce_symbol_from_quote(qd)
                        if sym:
                            temp[sym] = qd

                    if temp:
                        out.update(temp)
                        for idx, s in enumerate(part):
                            if s in out:
                                continue
                            if idx < len(res):
                                out[s] = _obj_to_dict(res[idx])
                            else:
                                out[s] = {"error": "missing quote row"}
                        continue

                    for idx, s in enumerate(part):
                        if idx < len(res):
                            out[s] = _obj_to_dict(res[idx])
                        else:
                            out[s] = {"error": "missing quote row"}
                    continue

                for s in part:
                    out[s] = {"error": f"unexpected get_enriched_quotes type: {type(res).__name__}"}

            except Exception as e:
                for s in part:
                    out[s] = {"error": f"get_enriched_quotes failed: {e}"}

        missing = [s for s in symbols if s not in out]
        if missing and callable(get_one):
            for s in missing:
                try:
                    q = await _maybe_await(get_one(s, use_cache=use_cache, ttl=ttl))
                    out[s] = _obj_to_dict(q)
                except Exception as e:
                    out[s] = {"error": f"get_enriched_quote failed: {e}"}

        return out

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
# Audit logic — v4.6.0 Phase Q applied
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
    """
    v4.6.0: PRICE_SPIKE check uses `_as_percent_change_points()` so the
    engine v5.67.0 fraction contract maps correctly to the points-form
    30%-threshold.
    """
    anomalies: List[AnomalyType] = []

    raw_chg = q.get("percent_change") if "percent_change" in q else q.get("change_pct")
    chg = _as_percent_change_points(raw_chg, config.percent_change_contract)

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
    """
    v4.6.0: extended with paths for UNIT_DRIFT_* and PROVIDER_PERCENT_* tags.
    """
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
        elif i in {
            "UNIT_DRIFT_PERCENT_CHANGE",
            "UNIT_DRIFT_UPSIDE",
            "UNIT_DRIFT_DRAWDOWN",
            "UNIT_DRIFT_VAR",
        }:
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Force refresh row (--refresh 1) to invoke engine v5.67.0+; if drift persists, check engine emit path for unit regression",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=15,
                    automated=False,
                )
            )
        elif i == "PROVIDER_PERCENT_DROPPED":
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Investigate provider feed: percent_change garbage with no recoverable ground-truth (price/previous_close missing)",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=20,
                    automated=False,
                )
            )
        elif i == "PROVIDER_PERCENT_CLAMPED":
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Provider sent percent_change outside ±50% daily cap; engine clamped. Verify provider unit contract.",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=20,
                    automated=False,
                )
            )
        elif i == "PROVIDER_PERCENT_RECOMPUTED":
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Engine recomputed percent_change from price/previous_close (provider value disagreed). No action unless widespread.",
                    priority=AlertPriority.P4,
                    estimated_time_minutes=10,
                    automated=False,
                )
            )
        # v4.7.0: new tag families from engine v5.72.0 + enriched_quote v4.6.0
        elif i == "EMPTY_ROW":
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Engine detected no usable provider data for this row. Force refresh; if still empty, verify symbol is still listed / actively traded on its exchange.",
                    priority=AlertPriority.P1,
                    estimated_time_minutes=15,
                    automated=True,
                )
            )
        elif i in {"MARKET_CAP_CURRENCY_SUSPECT", "REVENUE_CURRENCY_SUSPECT"}:
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Provider value magnitude disagrees with its currency label (likely foreign listing tagged USD). Verify currency mapping in symbols_reader / provider; engine v5.72.0 force-correct should resolve once locale is canonical.",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=20,
                    automated=False,
                )
            )
        elif i == "QUOTE_PRICE_MISSING":
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Provider quote endpoint returned no price; engine fell back to chart data. Watch for pattern — isolated cases are self-healing; widespread misses point to provider outage or symbol expiry.",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=15,
                    automated=False,
                )
            )
        elif i == "PROVIDER_VALUE_SANITIZED":
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Engine sanitized one or more extreme financial-ratio values (P/E, D/E, EV/EBITDA, etc.). Inspect the row's `warnings` field for the specific sanitized:<field>_out_of_range tag. No action unless the underlying provider value drift is widespread.",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=15,
                    automated=False,
                )
            )
        elif i == "IDENTITY_CORRECTED_BY_ENGINE":
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Engine v5.72.0 force-overrode a mislabeled exchange or currency (typical for foreign tickers tagged with US exchange/USD). Operational info — no action needed; the row is now correct.",
                    priority=AlertPriority.P5,
                    estimated_time_minutes=5,
                    automated=False,
                )
            )
        elif i == "IDENTITY_INFERRED_FROM_SUFFIX":
            actions.append(
                RemediationAction(
                    issue=i,
                    action="Engine inferred exchange/currency/country from the symbol suffix table because the provider did not supply them. Operational info — no action needed.",
                    priority=AlertPriority.P5,
                    estimated_time_minutes=5,
                    automated=False,
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
    """
    v4.6.0: unit-drift penalty (10 pts per drift, cap 30) when enabled.
    """
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

    if config.enable_unit_drift_detection:
        drift_count = len(_audit_unit_drift_fraction_fields(q))
        if drift_count > 0:
            score -= min(30.0, 10.0 * drift_count)

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


# v4.7.0: severity-tier sets used by _compute_severity. Hoisted to module
# level so tests and summary builders can reference them.
_SEVERITY_CRITICAL_TAGS: Set[str] = {
    "PROVIDER_ERROR", "ZERO_PRICE", "HARD_STALE_DATA", "ZOMBIE_TICKER",
}
_SEVERITY_HIGH_TAGS: Set[str] = {
    "UNIT_DRIFT_PERCENT_CHANGE",
    "UNIT_DRIFT_UPSIDE",
    "UNIT_DRIFT_DRAWDOWN",
    "UNIT_DRIFT_VAR",
    "PROVIDER_PERCENT_DROPPED",
    "EMPTY_ROW",  # v4.7.0
}
_SEVERITY_MEDIUM_TAGS: Set[str] = {
    "PROVIDER_PERCENT_CLAMPED",
    # v4.7.0
    "MARKET_CAP_CURRENCY_SUSPECT",
    "REVENUE_CURRENCY_SUSPECT",
    "QUOTE_PRICE_MISSING",
}
_SEVERITY_LOW_TAGS: Set[str] = {
    "PROVIDER_PERCENT_RECOMPUTED",
    "PROVIDER_VALUE_SANITIZED",  # v4.7.0
}
_SEVERITY_INFO_TAGS: Set[str] = {
    # v4.7.0
    "IDENTITY_CORRECTED_BY_ENGINE",
    "IDENTITY_INFERRED_FROM_SUFFIX",
}


def _compute_severity(issues: List[str], quality: DataQuality) -> AuditSeverity:
    """
    v4.7.0: re-tiered to keep INFO/LOW tags from bubbling to MEDIUM by
    default.

      CRITICAL: PROVIDER_ERROR / ZERO_PRICE / HARD_STALE_DATA / ZOMBIE_TICKER
      HIGH:     UNIT_DRIFT_* / PROVIDER_PERCENT_DROPPED / EMPTY_ROW
                (or any non-empty issue when quality is POOR/CRITICAL)
      MEDIUM:   PROVIDER_PERCENT_CLAMPED / MARKET_CAP_CURRENCY_SUSPECT /
                REVENUE_CURRENCY_SUSPECT / QUOTE_PRICE_MISSING
                (or any other non-info, non-low issue)
      LOW:      PROVIDER_PERCENT_RECOMPUTED / PROVIDER_VALUE_SANITIZED
                (when the row's issues are confined to LOW + INFO tags)
      INFO:     IDENTITY_CORRECTED_BY_ENGINE / IDENTITY_INFERRED_FROM_SUFFIX
                (when the row's issues are confined to INFO tags)
      OK:       no issues

    Exit-code semantics are preserved: INFO and LOW both yield exit 0.
    """
    issues_set = set(issues)

    if issues_set & _SEVERITY_CRITICAL_TAGS:
        return AuditSeverity.CRITICAL

    if issues_set & _SEVERITY_HIGH_TAGS:
        return AuditSeverity.HIGH

    if issues_set and quality in {DataQuality.POOR, DataQuality.CRITICAL}:
        return AuditSeverity.HIGH

    if issues_set & _SEVERITY_MEDIUM_TAGS:
        return AuditSeverity.MEDIUM

    # v4.7.0: classify pure-INFO and INFO+LOW issue sets distinctly.
    # Order matters: check the stricter (INFO-only) subset before the
    # looser (INFO|LOW) one.
    if issues_set and issues_set <= _SEVERITY_INFO_TAGS:
        return AuditSeverity.INFO

    if issues_set and issues_set <= (_SEVERITY_INFO_TAGS | _SEVERITY_LOW_TAGS):
        return AuditSeverity.LOW

    if issues_set:
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
    """
    v4.6.0 changes:
      - percent_change normalized via `_as_percent_change_points`.
      - Unit-drift detection via `_audit_unit_drift_fraction_fields`.
      - Engine-warning surfacing via `_audit_engine_warning_tags`.
      - `AuditResult.change_pct` always in POINTS form for display.
    """
    symbol_n = _norm_symbol(symbol) or safe_str(symbol, "UNKNOWN")
    provider = _derive_provider(q)

    price = _safe_float(q.get("current_price") or q.get("price"), None)

    raw_chg = q.get("percent_change") if "percent_change" in q else q.get("change_pct")
    chg_pct = _as_percent_change_points(raw_chg, config.percent_change_contract)

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

    if config.enable_unit_drift_detection:
        drift_tags = _audit_unit_drift_fraction_fields(q)
        for tag in drift_tags:
            if tag not in issues:
                issues.append(tag)

    warning_issue_tags = _audit_engine_warning_tags(q)
    for tag in warning_issue_tags:
        if tag not in issues:
            issues.append(tag)

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
        "symbol", "page", "provider", "severity", "data_quality",
        "provider_health", "price", "change_pct", "age_hours",
        "history_points", "confidence_pct", "issues", "anomalies",
        "error", "last_updated_utc",
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
            "Dependencies not loaded. symbols_reader=%s (%s), data_engine=%s, err=%s",
            bool(DEPS.symbols_reader),
            DEPS.symbols_reader_module_name or "n/a",
            bool(DEPS.data_engine),
            DEPS.err,
        )
        return [], AuditSummary()

    logger.info(
        "Deps OK | symbols_reader=%s | data_engine=%s | percent_contract=%s | unit_drift=%s",
        DEPS.symbols_reader_module_name or "(anon)",
        getattr(DEPS.data_engine, "__name__", "core.data_engine"),
        config.percent_change_contract,
        config.enable_unit_drift_detection,
    )

    sid = (spreadsheet_id or os.getenv("DEFAULT_SPREADSHEET_ID", "")).strip()
    if not sid:
        logger.error("No spreadsheet_id provided and DEFAULT_SPREADSHEET_ID is empty.")
        return [], AuditSummary()

    use_cache = not bool(refresh)
    ttl = None
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


def _resolve_percent_contract_from_env_and_flag(flag_legacy: int) -> str:
    """
    Contract resolution order (introduced v4.6.0, preserved in v4.7.0):
      1. CLI flag --legacy-percent-units 1   -> "points"
      2. AUDIT_PERCENT_CONTRACT env var      -> "points" / "fraction"
      3. Default                              -> "fraction" (v5.67.0+,
         still canonical in current engine v5.72.0)
    """
    if int(flag_legacy or 0) == 1:
        return PERCENT_CONTRACT_POINTS
    env = (os.getenv("AUDIT_PERCENT_CONTRACT") or "").strip().lower()
    if env in _PERCENT_CONTRACT_CHOICES:
        return env
    return PERCENT_CONTRACT_FRACTION


def main() -> None:
    p = argparse.ArgumentParser(description=f"TFB Data Quality Auditor v{SERVICE_VERSION}")
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

    # v4.6.0: unit-contract flags
    p.add_argument(
        "--legacy-percent-units",
        type=int,
        default=0,
        help="v4.6.0: 1=assume percent_change in POINTS form (pre-v5.67.0 engine); 0=FRACTION (v5.67.0+ default)",
    )
    p.add_argument(
        "--no-unit-drift",
        type=int,
        default=0,
        help="v4.6.0: 1=disable UNIT_DRIFT_* detection (not recommended); 0=enabled (default)",
    )

    args = p.parse_args()

    config = AuditConfig(
        percent_change_contract=_resolve_percent_contract_from_env_and_flag(int(args.legacy_percent_units or 0)),
        enable_unit_drift_detection=(int(args.no_unit_drift or 0) != 1),
    )

    exit_code = 0
    try:
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

        alerts = [r for r in results if r.severity != AuditSeverity.OK]

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

        if summary.by_severity.get("CRITICAL", 0) > 0:
            exit_code = 3
        elif summary.by_severity.get("HIGH", 0) > 0:
            exit_code = 2
        elif summary.by_severity.get("MEDIUM", 0) > 0:
            exit_code = 1
    finally:
        _shutdown_executor(wait=False)

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
