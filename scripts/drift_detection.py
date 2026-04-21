#!/usr/bin/env python3
# scripts/drift_detection.py
"""
================================================================================
TADAWUL FAST BRIDGE — ML MODEL DRIFT DETECTOR (v4.3.0)
================================================================================
QUANTUM EDITION | ISOLATION FOREST | KS/PSI | SAFE OTEL | SAMA AUDIT SIGNATURE

Why this revision (v4.3.0 vs v4.2.0)
-------------------------------------
- 🔑 FIX CRITICAL: `_load_from_live_source` probed
     `integrations.google_sheets_service.get_rows_for_sheet` /
     `read_rows_for_sheet` / `get_sheet_rows`. Per
     `google_sheets_service v5.5.3`'s `__all__`, none of those functions
     are public — the module exposes `read_range()`, `read_ranges_batch()`,
     `refresh_sheet_with_*`, etc., but NO row-oriented reader.
     v4.2.0 would therefore log "google_sheets_service has no
     get_rows_for_sheet — skipping" for every page, collect zero rows,
     and exit with "No live data loaded from Google Sheets."
     v4.3.0 uses `core.data_engine.get_sheet_rows(sheet, limit, ...)` as
     the canonical async reader (matches `data_engine v6.x __all__` and
     `core.data_engine_v2 v7` which uses the same discovery contract),
     with fallbacks to `core.data_engine_v2.get_sheet_rows`, then
     `google_sheets_service.read_range()` as a last resort.

- 🔑 FIX HIGH: v4.2.0 called `reader(page, spreadsheet_id, limit=limit)`
     with `spreadsheet_id` as a positional 2nd arg. Per
     `core.data_engine_v2 v7._call_rows_reader`, the canonical signature
     variants are `(sheet=..., limit=...)` / `(sheet_name=..., limit=...)`
     / `(page=..., limit=...)` — none accept `spreadsheet_id` positional.
     v4.3.0 invokes the canonical signature correctly and lets the reader
     module resolve `spreadsheet_id` internally via its default config.

- FIX: Module discovery in live mode is cached ONCE at function entry
     instead of re-importing inside the per-page loop.

- FIX: `_TRUTHY` / `_FALSY` realigned to exact `main._TRUTHY` / `_FALSY`
     vocabulary. Added `_env_bool` helper. Matches `cleanup_cache.py v4.1.0`
     and `audit_data_quality.py v4.5.0`.

- FIX: `SERVICE_VERSION = SCRIPT_VERSION` alias (cross-script consistency).

- FIX: `_CPU_EXECUTOR.shutdown()` wrapped in `try/finally` in main_async
     AND main. Leak-proof on all exception paths.

- FIX: `_shutdown_executor(wait)` idempotent helper (matches
     `audit_data_quality.py v4.5.0` / `cleanup_cache.py v4.1.0`).

- FIX: Mock data generation uses a local `np.random.default_rng(42)`
     instead of `np.random.seed(42)` which pollutes the global state.

- FIX: Live-mode bootstrap (first run with no snapshot) now SKIPS analysis
     after saving the baseline snapshot. v4.2.0 ran full KS/PSI analysis on
     baseline == current, producing an all-zeros report which is misleading.

- KEEP: All 4.2.0 CLI flags (`--mode`, `--baseline`, `--current`,
     `--spreadsheet-id`, `--source-pages`, `--baseline-snapshot`,
     `--features`, `--no-audit`, `--force-drift`). All env var semantics.
     Auto-detect logic for `_resolve_mode_and_sources`. PSI / KS /
     Wasserstein / IsolationForest analysis. HMAC SAMA audit signing.
     Slack webhook. TraceContext OTEL. File loading (.csv/.json/.parquet).

Modes
-----
    mock  python drift_detection.py --mode mock --force-drift
    file  python drift_detection.py --mode file --baseline b.csv --current c.csv
    live  python drift_detection.py --mode live --source-pages Market_Leaders,Global_Markets

Env vars (for headless/cron)
----------------------------
    DRIFT_MODE                mock|file|live  (default: auto-detect)
    DRIFT_BASELINE_PATH       file path for baseline dataset
    DRIFT_CURRENT_PATH        file path for current dataset
    DEFAULT_SPREADSHEET_ID    Google Sheets ID (for live mode)
    DRIFT_SOURCE_PAGES        comma-separated sheet tabs for live mode
    DRIFT_BASELINE_SNAPSHOT   JSON/CSV file for live-mode baseline cache
    DRIFT_SLACK_WEBHOOK       Slack webhook URL
    DRIFT_AUDIT_SECRET        HMAC signing secret
    CORE_TRACING_ENABLED      enable OTEL spans
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import hashlib
import hmac
import importlib
import inspect
import logging
import math
import os
import random
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, Awaitable

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=opt, default=str).decode("utf-8")

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:
    import json  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None, default=str, ensure_ascii=False)

    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
SCRIPT_VERSION = "4.3.0"
SERVICE_VERSION = SCRIPT_VERSION  # v4.3.0: cross-script alias

# ---------------------------------------------------------------------------
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
# ---------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_bool(name: str, default: bool = False) -> bool:
    """Project-aligned env bool parser."""
    try:
        raw = (os.getenv(name, "") or "").strip().lower()
    except Exception:
        return bool(default)
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


# ---------------------------------------------------------------------------
# Optional ML stack
# ---------------------------------------------------------------------------
_ML_AVAILABLE = True
try:
    import numpy as np  # type: ignore
except Exception:
    np = None  # type: ignore
    _ML_AVAILABLE = False

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore
    _ML_AVAILABLE = False

# SciPy
try:
    from scipy.stats import ks_2samp, wasserstein_distance  # type: ignore
    _SCIPY_AVAILABLE = True
except Exception:
    ks_2samp = None  # type: ignore
    wasserstein_distance = None  # type: ignore
    _SCIPY_AVAILABLE = False

# Sklearn
try:
    from sklearn.ensemble import IsolationForest  # type: ignore
    _SKLEARN_AVAILABLE = True
except Exception:
    IsolationForest = None  # type: ignore
    _SKLEARN_AVAILABLE = False

# ---------------------------------------------------------------------------
# Webhook stack
# ---------------------------------------------------------------------------
try:
    import aiohttp  # type: ignore
    _AIOHTTP_AVAILABLE = True
except Exception:
    aiohttp = None  # type: ignore
    _AIOHTTP_AVAILABLE = False

# ---------------------------------------------------------------------------
# OpenTelemetry tracing (optional)
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    _OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    _OTEL_AVAILABLE = False

_TRACING_ENABLED = _env_bool("CORE_TRACING_ENABLED", False)


class TraceContext:
    """
    Safe OTEL context wrapper.

    tracer.start_as_current_span() returns a context manager; we must
    enter it to obtain the span instance.
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._tracer = (
            trace.get_tracer(__name__)
            if (_OTEL_AVAILABLE and _TRACING_ENABLED and trace)
            else None
        )
        self._cm = None
        self.span = None

    def __enter__(self):
        if self._tracer:
            self._cm = self._tracer.start_as_current_span(self.name)
            self.span = self._cm.__enter__()
            if self.attributes and self.span:
                try:
                    self.span.set_attributes(self.attributes)
                except Exception:
                    pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and _OTEL_AVAILABLE:
            if exc_val:
                try:
                    self.span.record_exception(exc_val)
                    self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))  # type: ignore
                except Exception:
                    pass
        if self._cm:
            try:
                return self._cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                return False
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


# =============================================================================
# Core Configuration & Logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DriftDetector")

_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="DriftWorker"
)


def _shutdown_executor(wait: bool = False) -> None:
    """Idempotent shutdown helper. Safe to call multiple times."""
    try:
        _CPU_EXECUTOR.shutdown(wait=wait, cancel_futures=True)
    except TypeError:
        # python < 3.9 fallback
        try:
            _CPU_EXECUTOR.shutdown(wait=wait)
        except Exception:
            pass
    except Exception:
        pass


async def _maybe_await(x: Any) -> Any:
    """Project-aligned awaitable probe via inspect.isawaitable."""
    if inspect.isawaitable(x):
        return await x
    return x


# =============================================================================
# Enums and Data Models
# =============================================================================
class DriftSeverity(str, Enum):
    NONE = "none"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(slots=True)
class FeatureDriftStat:
    feature_name: str
    ks_stat: float
    p_value: float
    wasserstein_dist: float
    psi_score: float
    is_drifted: bool
    severity: DriftSeverity


@dataclass(slots=True)
class ModelDriftReport:
    model_name: str
    timestamp_utc: str
    total_features: int
    drifted_features: int
    overall_severity: DriftSeverity
    feature_stats: List[FeatureDriftStat]
    confidence_drop: float
    anomaly_rate: float
    recommendation: str          # ML retraining action (not a financial signal)
    top_drift_features: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "timestamp_utc": self.timestamp_utc,
            "total_features": self.total_features,
            "drifted_features": self.drifted_features,
            "overall_severity": self.overall_severity.value,
            "confidence_drop": round(float(self.confidence_drop), 6),
            "anomaly_rate": round(float(self.anomaly_rate), 6),
            "recommendation": self.recommendation,
            "top_drift_features": self.top_drift_features,
            "features": [asdict(f) | {"severity": f.severity.value} for f in self.feature_stats],
            "version": SCRIPT_VERSION,
        }


# =============================================================================
# Full Jitter Exponential Backoff (kept for future retry paths)
# =============================================================================
class FullJitterBackoff:
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 0.8,
        max_delay: float = 15.0,
    ):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.05, float(base_delay))
        self.max_delay = max(0.2, float(max_delay))

    async def execute_async(
        self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> Any:
        last_exc: Optional[BaseException] = None
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exc = e
                if attempt >= self.max_retries - 1:
                    raise
                cap = min(self.max_delay, self.base_delay * (2 ** attempt))
                sleep_time = random.uniform(0.0, cap)
                logger.warning(
                    "Operation failed: %s. Retry in %.2fs (%d/%d)",
                    e, sleep_time, attempt + 1, self.max_retries,
                )
                await asyncio.sleep(sleep_time)
        raise last_exc  # type: ignore


# =============================================================================
# Utility: Signed audit log entry (HMAC)
# =============================================================================
def _audit_secret() -> bytes:
    s = (os.getenv("DRIFT_AUDIT_SECRET") or os.getenv("AUDIT_SECRET") or "").strip()
    if not s:
        s = "CHANGE_ME_DRIFT_AUDIT_SECRET"
    return s.encode("utf-8")


def sign_audit(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Produces a deterministic signature over canonical JSON bytes."""
    canonical = json_dumps(payload, indent=0)
    sig = hmac.new(_audit_secret(), canonical.encode("utf-8"), hashlib.sha256).hexdigest()
    return {"signature": sig, "algo": "HMAC-SHA256", "canonical": canonical}


# =============================================================================
# Drift Core
# =============================================================================
def _to_np(x: Any) -> "np.ndarray":
    return np.asarray(x, dtype=float)  # type: ignore


def _finite(arr: "np.ndarray") -> "np.ndarray":
    return arr[np.isfinite(arr)]  # type: ignore


def calculate_psi(
    expected: "np.ndarray",
    actual: "np.ndarray",
    buckets: int = 10,
) -> float:
    """
    PSI using quantile buckets based on expected distribution.
    Robust for constant arrays.
    """
    expected = _finite(expected)
    actual = _finite(actual)
    if expected.size == 0 or actual.size == 0:
        return 0.0

    if float(np.std(expected)) < 1e-12:  # type: ignore
        return 0.25 if float(np.std(actual)) > 1e-6 else 0.0

    buckets = max(5, min(50, int(buckets)))
    qs = np.linspace(0, 1, buckets + 1)  # type: ignore
    cuts = np.quantile(expected, qs)  # type: ignore
    cuts = np.unique(cuts)  # type: ignore
    if cuts.size < 3:
        return 0.0

    exp_counts, _ = np.histogram(expected, bins=cuts)  # type: ignore
    act_counts, _ = np.histogram(actual, bins=cuts)  # type: ignore

    exp_pct = exp_counts / max(1, expected.size)
    act_pct = act_counts / max(1, actual.size)

    eps = 1e-6
    exp_pct = np.clip(exp_pct, eps, 1)  # type: ignore
    act_pct = np.clip(act_pct, eps, 1)  # type: ignore

    psi = float(np.sum((exp_pct - act_pct) * np.log(exp_pct / act_pct)))  # type: ignore
    if math.isnan(psi) or math.isinf(psi):
        return 0.0
    return psi


def detect_feature_drift(
    baseline: "np.ndarray",
    current: "np.ndarray",
    feature_name: str,
    *,
    ks_p_crit: float = 0.01,
    psi_warn: float = 0.20,
    psi_crit: float = 0.25,
) -> FeatureDriftStat:
    baseline = _finite(baseline)
    current = _finite(current)

    ks_stat = 0.0
    p_value = 1.0
    wd = 0.0

    if _SCIPY_AVAILABLE and ks_2samp is not None:
        ks_stat, p_value = ks_2samp(baseline, current)
        ks_stat = float(ks_stat)
        p_value = float(p_value)

    if _SCIPY_AVAILABLE and wasserstein_distance is not None:
        wd = float(wasserstein_distance(baseline, current))

    psi = float(calculate_psi(baseline, current, buckets=10))

    is_drifted = False
    severity = DriftSeverity.NONE

    if psi >= psi_crit or (p_value < ks_p_crit and psi >= psi_warn):
        is_drifted = True
        severity = DriftSeverity.CRITICAL if psi >= psi_crit else DriftSeverity.WARNING
    elif psi >= psi_warn or p_value < ks_p_crit:
        is_drifted = True
        severity = DriftSeverity.WARNING

    return FeatureDriftStat(
        feature_name=feature_name,
        ks_stat=ks_stat,
        p_value=p_value,
        wasserstein_dist=wd,
        psi_score=psi,
        is_drifted=is_drifted,
        severity=severity,
    )


def analyze_model_drift(
    model_name: str,
    baseline_df: "pd.DataFrame",
    current_df: "pd.DataFrame",
    *,
    min_samples: int = 30,
    top_k: int = 8,
) -> ModelDriftReport:
    features = [c for c in baseline_df.columns if c in current_df.columns]
    feature_stats: List[FeatureDriftStat] = []

    for feature in features:
        b = baseline_df[feature].dropna().values
        c = current_df[feature].dropna().values
        if len(b) < min_samples or len(c) < min_samples:
            continue
        stat = detect_feature_drift(_to_np(b), _to_np(c), feature)
        feature_stats.append(stat)

    drifted_count = sum(1 for f in feature_stats if f.is_drifted)
    total = len(feature_stats)
    drift_pct = drifted_count / max(1, total)

    overall = DriftSeverity.NONE
    if (
        any(f.severity == DriftSeverity.CRITICAL for f in feature_stats)
        or drift_pct >= 0.30
    ):
        overall = DriftSeverity.CRITICAL
    elif drift_pct >= 0.15:
        overall = DriftSeverity.WARNING

    # ML retraining action text — not a financial recommendation
    recommendation = "No action required."
    if overall == DriftSeverity.CRITICAL:
        recommendation = "IMMEDIATE RETRAINING REQUIRED. Model degraded."
    elif overall == DriftSeverity.WARNING:
        recommendation = "Schedule retraining soon. Monitor performance."

    # IsolationForest anomaly on current (optional)
    anomaly_rate = 0.0
    if (
        _SKLEARN_AVAILABLE
        and IsolationForest is not None
        and total > 0
        and len(current_df) >= max(80, min_samples)
    ):
        try:
            use_df = current_df[features].copy()
            use_df = use_df.apply(pd.to_numeric, errors="coerce")  # type: ignore
            use_df = use_df.fillna(use_df.mean(numeric_only=True))
            X = use_df.values
            iso = IsolationForest(contamination=0.05, random_state=42)
            preds = iso.fit_predict(X)
            anomaly_rate = float((preds == -1).sum() / max(1, len(preds)))
        except Exception:
            anomaly_rate = 0.0

    top = sorted(feature_stats, key=lambda x: x.psi_score, reverse=True)[:top_k]
    top_names = [
        f"{t.feature_name} (PSI={t.psi_score:.3f}, p={t.p_value:.3g})"
        for t in top
        if t.is_drifted
    ][:top_k]

    return ModelDriftReport(
        model_name=model_name,
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
        total_features=total,
        drifted_features=drifted_count,
        overall_severity=overall,
        feature_stats=feature_stats,
        confidence_drop=0.0,
        anomaly_rate=anomaly_rate,
        recommendation=recommendation,
        top_drift_features=top_names,
    )


# =============================================================================
# Webhook Alerting
# =============================================================================
async def send_slack_alert(
    webhook_url: str,
    report: ModelDriftReport,
    audit_sig: Dict[str, Any],
) -> None:
    if not _AIOHTTP_AVAILABLE or not webhook_url:
        return

    sev = report.overall_severity.value.upper()
    color = "danger" if report.overall_severity == DriftSeverity.CRITICAL else "warning"
    text = (
        f"*🚨 Model Drift Detected: {report.model_name}*\n"
        f"Severity: {sev}\n"
        f"Drifted Features: {report.drifted_features}/{report.total_features}\n"
        f"Anomaly Rate: {report.anomaly_rate:.1%}\n"
        f"Top: {', '.join(report.top_drift_features[:5]) if report.top_drift_features else 'N/A'}\n"
        f"Recommendation: {report.recommendation}\n"
        f"AuditSig: `{audit_sig.get('signature','')[:16]}...`"
    )

    payload = {"attachments": [{"color": color, "text": text}]}

    try:
        timeout = aiohttp.ClientTimeout(total=6)  # type: ignore
        async with aiohttp.ClientSession(timeout=timeout) as session:  # type: ignore
            await session.post(webhook_url, json=payload)
        logger.info("Sent drift alert to Slack.")
    except Exception as e:
        logger.error("Failed to send Slack alert: %s", e)


# =============================================================================
# File-based data loading
# =============================================================================
def _load_df(path: str) -> "pd.DataFrame":
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    suf = p.suffix.lower()
    if suf in (".parquet", ".pq"):
        return pd.read_parquet(p)  # type: ignore
    if suf in (".csv",):
        return pd.read_csv(p)  # type: ignore
    if suf in (".json",):
        return pd.read_json(p)  # type: ignore
    raise ValueError(f"Unsupported file type: {suf} (use .csv/.json/.parquet)")


# =============================================================================
# Live data source loader (v4.3.0: canonical core.data_engine.get_sheet_rows)
# =============================================================================
# v4.2.0 probed `integrations.google_sheets_service.get_rows_for_sheet` which
# does not exist in that module's `__all__`. The canonical rows-fetching API
# is `core.data_engine.get_sheet_rows(sheet, limit, ...)` (async) per
# `data_engine v6.x __all__`. v4.3.0 uses it, with fallbacks.
# =============================================================================

@dataclass(slots=True)
class _LiveReaderInfo:
    """Records which module + function succeeded on first probe."""
    module_path: str = ""
    function_name: str = ""
    is_async: bool = False
    source: str = ""  # "engine_v1" | "engine_v2" | "sheets_raw"


async def _discover_engine_reader() -> Optional[Tuple[_LiveReaderInfo, Callable]]:
    """
    Probe `core.data_engine.get_sheet_rows` first, then
    `core.data_engine_v2.get_sheet_rows`.
    Both are async per their `__all__` definitions.
    """
    for mod_path, source_label in (
        ("core.data_engine", "engine_v1"),
        ("core.data_engine_v2", "engine_v2"),
    ):
        try:
            mod = importlib.import_module(mod_path)
        except Exception as e:
            logger.debug("drift live reader: %s import failed: %s", mod_path, e)
            continue

        fn = getattr(mod, "get_sheet_rows", None)
        if callable(fn):
            info = _LiveReaderInfo(
                module_path=mod_path,
                function_name="get_sheet_rows",
                is_async=inspect.iscoroutinefunction(fn),
                source=source_label,
            )
            return info, fn

        # Also probe sync variant
        fn_sync = getattr(mod, "get_sheet_rows_sync", None)
        if callable(fn_sync):
            info = _LiveReaderInfo(
                module_path=mod_path,
                function_name="get_sheet_rows_sync",
                is_async=False,
                source=source_label,
            )
            return info, fn_sync

    return None


def _extract_rows_from_engine_payload(payload: Any) -> List[Dict[str, Any]]:
    """
    core.data_engine.get_sheet_rows returns a dict with keys like
    `rows`, `data`, `items`, `records`, `quotes` — all the same list of
    dict-rows. Extract in priority order.
    """
    if payload is None:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("rows", "data", "items", "records", "quotes"):
            seq = payload.get(key)
            if isinstance(seq, list) and seq and isinstance(seq[0], dict):
                return [r for r in seq if isinstance(r, dict)]
    return []


async def _call_engine_reader(
    fn: Callable,
    info: _LiveReaderInfo,
    sheet: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """Invoke get_sheet_rows with canonical kw-first signature."""
    attempts: Tuple[Tuple[Tuple, Dict[str, Any]], ...] = (
        ((), {"sheet": sheet, "limit": limit}),
        ((), {"sheet_name": sheet, "limit": limit}),
        ((), {"page": sheet, "limit": limit}),
        ((sheet,), {"limit": limit}),
        ((sheet,), {}),
    )

    for args, kwargs in attempts:
        try:
            if info.is_async:
                result = await fn(*args, **kwargs)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    _CPU_EXECUTOR, lambda: fn(*args, **kwargs)
                )
                result = await _maybe_await(result)

            rows = _extract_rows_from_engine_payload(result)
            if rows:
                return rows
            # Empty but successful → return empty (sheet has no data)
            return []
        except TypeError:
            # Signature mismatch — try next variant
            continue
        except Exception as e:
            logger.warning(
                "drift live reader: %s.%s failed on %s: %s",
                info.module_path, info.function_name, sheet, e,
            )
            return []

    return []


async def _fallback_read_range(
    spreadsheet_id: str,
    page: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """
    Last-resort fallback: use `google_sheets_service.read_range()` to pull
    raw cell values for `<page>!A1:CC<limit+1>`, then manually extract
    headers from row 0 and build dict-rows.
    """
    mod = None
    for mod_path in (
        "integrations.google_sheets_service",
        "core.integrations.google_sheets_service",
        "google_sheets_service",
        "core.google_sheets_service",
    ):
        try:
            mod = importlib.import_module(mod_path)
            break
        except Exception:
            continue

    if mod is None:
        return []

    read_range = getattr(mod, "read_range", None)
    if not callable(read_range):
        return []

    # Build A1 range: up to 81 columns (AA..CC) is safe for 80-col sheets.
    end_col = "CC"
    max_rows = max(2, int(limit) + 1)  # +1 for header
    range_str = f"{page}!A1:{end_col}{max_rows}"

    loop = asyncio.get_running_loop()
    try:
        grid = await loop.run_in_executor(
            _CPU_EXECUTOR,
            lambda: read_range(spreadsheet_id, range_str),
        )
    except Exception as e:
        logger.warning("drift live reader raw fallback failed for %s: %s", page, e)
        return []

    if not isinstance(grid, list) or len(grid) < 2:
        return []

    headers_row = grid[0]
    if not isinstance(headers_row, list):
        return []

    headers = [str(h).strip() if h is not None else "" for h in headers_row]
    dict_rows: List[Dict[str, Any]] = []
    for row in grid[1:]:
        if not isinstance(row, list):
            continue
        padded = list(row) + [None] * max(0, len(headers) - len(row))
        padded = padded[: len(headers)]
        dict_rows.append(
            {headers[i]: padded[i] for i in range(len(headers)) if headers[i]}
        )
    return dict_rows


async def _load_from_live_source(
    pages: List[str],
    spreadsheet_id: str,
    *,
    limit: int = 2000,
    numeric_cols: Optional[List[str]] = None,
) -> "pd.DataFrame":
    """
    Pull live data from Google Sheets via `core.data_engine.get_sheet_rows`
    and return a numeric-only DataFrame.
    """
    if pd is None or np is None:
        raise ImportError("pandas and numpy are required for live data loading")

    # v4.3.0: discover reader ONCE, cache for all pages.
    reader_bundle = await _discover_engine_reader()
    rows_all: List[Dict[str, Any]] = []

    if reader_bundle is not None:
        info, fn = reader_bundle
        logger.info(
            "Live reader: %s.%s (source=%s, async=%s)",
            info.module_path, info.function_name, info.source, info.is_async,
        )
        for page in pages:
            try:
                page_rows = await _call_engine_reader(fn, info, page, limit)
                if page_rows:
                    rows_all.extend(page_rows)
                    logger.info("  Live: loaded %d rows from %s", len(page_rows), page)
                else:
                    logger.warning("  Live: 0 rows from %s via engine reader", page)
            except Exception as e:
                logger.warning("Failed loading live data from %s: %s", page, e)

    # Fallback: try raw read_range + manual header extraction
    if not rows_all:
        logger.warning(
            "Engine reader yielded no rows. "
            "Falling back to google_sheets_service.read_range() for raw cells."
        )
        for page in pages:
            try:
                page_rows = await _fallback_read_range(spreadsheet_id, page, limit)
                if page_rows:
                    rows_all.extend(page_rows)
                    logger.info("  Live(raw): loaded %d rows from %s", len(page_rows), page)
            except Exception as e:
                logger.warning("Raw-fallback failed on %s: %s", page, e)

    if not rows_all:
        raise RuntimeError(
            "No live data loaded from Google Sheets. "
            "Check DEFAULT_SPREADSHEET_ID, credentials, and page names. "
            "Tried: core.data_engine.get_sheet_rows → core.data_engine_v2.get_sheet_rows → "
            "google_sheets_service.read_range"
        )

    df = pd.DataFrame(rows_all)  # type: ignore

    # Keep only numeric columns that coerce cleanly
    numeric_df = df.apply(pd.to_numeric, errors="coerce")  # type: ignore
    numeric_df = numeric_df.dropna(axis=1, how="all")

    if numeric_cols:
        keep = [c for c in numeric_cols if c in numeric_df.columns]
        if keep:
            numeric_df = numeric_df[keep]

    if numeric_df.empty:
        raise RuntimeError(
            "Live data loaded but no numeric columns found for drift analysis."
        )

    logger.info(
        "Live data: %d rows × %d numeric columns",
        len(numeric_df), len(numeric_df.columns),
    )
    return numeric_df


# =============================================================================
# Mode resolution
# =============================================================================
def _resolve_mode_and_sources(args: "argparse.Namespace") -> str:
    """
    Determine the data source mode.

    Priority:
    1. --mode CLI arg (explicit)
    2. DRIFT_MODE env var
    3. Auto-detect from available sources:
       - If DRIFT_BASELINE_PATH + DRIFT_CURRENT_PATH are set → file
       - If DEFAULT_SPREADSHEET_ID is set → live
       - Otherwise → mock (with a clear warning)
    """
    mode_arg = (getattr(args, "mode", "") or "").strip().lower()
    if mode_arg in ("mock", "file", "live"):
        return mode_arg

    mode_env = (os.getenv("DRIFT_MODE") or "").strip().lower()
    if mode_env in ("mock", "file", "live"):
        return mode_env

    has_file_paths = bool(
        (getattr(args, "baseline", "") or os.getenv("DRIFT_BASELINE_PATH", "")).strip()
        and (getattr(args, "current", "") or os.getenv("DRIFT_CURRENT_PATH", "")).strip()
    )
    has_spreadsheet = bool((os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip())

    if has_file_paths:
        logger.info(
            "Auto-detected mode: file "
            "(--baseline/--current or DRIFT_BASELINE_PATH/DRIFT_CURRENT_PATH set)"
        )
        return "file"
    if has_spreadsheet:
        logger.info("Auto-detected mode: live (DEFAULT_SPREADSHEET_ID set)")
        return "live"

    logger.warning(
        "drift_detection: No data source configured. Running on SYNTHETIC MOCK DATA. "
        "For real drift detection set --mode live (and DEFAULT_SPREADSHEET_ID) "
        "or --mode file --baseline <path> --current <path>."
    )
    return "mock"


# =============================================================================
# Mock data generator (v4.3.0: local RNG, no global seed pollution)
# =============================================================================
def generate_mock_data(drift: bool = False) -> Tuple["pd.DataFrame", "pd.DataFrame"]:
    # v4.3.0: np.random.default_rng(seed) creates an isolated generator
    # and does NOT set the global numpy random state.
    rng = np.random.default_rng(42)  # type: ignore
    n = 1000
    baseline = pd.DataFrame(  # type: ignore
        {
            "pe_ratio":   rng.normal(15, 5, n),
            "volatility": rng.lognormal(0, 0.5, n),
            "rsi":        rng.uniform(30, 70, n),
        }
    )
    if drift:
        current = pd.DataFrame(  # type: ignore
            {
                "pe_ratio":   rng.normal(25, 8, n),
                "volatility": rng.lognormal(0.5, 0.8, n),
                "rsi":        rng.uniform(30, 70, n),
            }
        )
    else:
        current = pd.DataFrame(  # type: ignore
            {
                "pe_ratio":   rng.normal(15.2, 5.1, n),
                "volatility": rng.lognormal(0.05, 0.52, n),
                "rsi":        rng.uniform(29, 71, n),
            }
        )
    return baseline, current


# =============================================================================
# Orchestrator
# =============================================================================
async def run_drift_analysis(
    model_name: str,
    baseline_df: "pd.DataFrame",
    current_df: "pd.DataFrame",
    *,
    webhook_url: Optional[str],
    export_dir: str,
    export_audit: bool = True,
) -> ModelDriftReport:
    loop = asyncio.get_running_loop()

    with TraceContext("drift_analysis", {"model": model_name}):
        report: ModelDriftReport = await loop.run_in_executor(
            _CPU_EXECUTOR,
            analyze_model_drift,
            model_name,
            baseline_df,
            current_df,
        )

    logger.info("Drift Analysis Complete: %s", report.overall_severity.value.upper())
    logger.info("Drifted Features: %d/%d", report.drifted_features, report.total_features)
    logger.info("Anomaly Rate: %.2f%%", report.anomaly_rate * 100)
    logger.info("Recommendation: %s", report.recommendation)

    os.makedirs(export_dir, exist_ok=True)
    ts = int(time.time())
    export_path = os.path.join(
        export_dir, f"drift_report_{model_name}_{ts}.json"
    )

    audit_payload = {
        "event": "model_drift_report",
        "model_name": model_name,
        "timestamp_utc": report.timestamp_utc,
        "severity": report.overall_severity.value,
        "drifted_features": report.drifted_features,
        "total_features": report.total_features,
        "anomaly_rate": report.anomaly_rate,
        "version": SCRIPT_VERSION,
    }
    audit_sig = sign_audit(audit_payload)

    def _write() -> None:
        payload = report.to_dict()
        payload["audit"] = audit_payload | audit_sig
        with open(export_path, "w", encoding="utf-8") as f:
            f.write(json_dumps(payload, indent=2))

    await loop.run_in_executor(_CPU_EXECUTOR, _write)
    logger.info("Report exported to %s", export_path)

    if export_audit:
        audit_path = os.path.join(
            export_dir, f"drift_audit_{model_name}_{ts}.json"
        )

        def _write_audit() -> None:
            with open(audit_path, "w", encoding="utf-8") as f:
                f.write(json_dumps(audit_payload | audit_sig, indent=2))

        await loop.run_in_executor(_CPU_EXECUTOR, _write_audit)
        logger.info("Audit exported to %s", audit_path)

    if report.overall_severity != DriftSeverity.NONE and webhook_url:
        await send_slack_alert(webhook_url, report, audit_sig)

    return report


async def main_async(args: argparse.Namespace) -> int:
    if not _ML_AVAILABLE:
        logger.error("numpy + pandas are required for drift detection.")
        return 1

    exit_code = 0
    try:
        # Resolve data source mode BEFORE loading anything.
        mode = _resolve_mode_and_sources(args)
        logger.info("Drift detection mode: %s", mode)

        baseline_df: "pd.DataFrame"
        current_df: "pd.DataFrame"
        bootstrap_only = False  # v4.3.0: signals first-run snapshot save

        if mode == "file":
            baseline_path = (
                getattr(args, "baseline", "") or os.getenv("DRIFT_BASELINE_PATH", "")
            )
            current_path = (
                getattr(args, "current", "") or os.getenv("DRIFT_CURRENT_PATH", "")
            )
            if not baseline_path or not current_path:
                logger.error(
                    "mode=file but no baseline/current paths. "
                    "Provide --baseline + --current or set "
                    "DRIFT_BASELINE_PATH + DRIFT_CURRENT_PATH."
                )
                return 1
            logger.info("Loading baseline from: %s", baseline_path)
            logger.info("Loading current from:  %s", current_path)
            baseline_df = _load_df(baseline_path)
            current_df = _load_df(current_path)

        elif mode == "live":
            spreadsheet_id = (
                getattr(args, "spreadsheet_id", "")
                or os.getenv("DEFAULT_SPREADSHEET_ID", "")
                or os.getenv("SPREADSHEET_ID", "")
            ).strip()
            if not spreadsheet_id:
                logger.error(
                    "mode=live but DEFAULT_SPREADSHEET_ID not set. "
                    "Set it in the environment or pass --spreadsheet-id."
                )
                return 1

            raw_pages = (
                getattr(args, "source_pages", "")
                or os.getenv("DRIFT_SOURCE_PAGES", "")
                or "Market_Leaders,Global_Markets,Mutual_Funds"
            )
            pages = [p.strip() for p in raw_pages.split(",") if p.strip()]
            logger.info("Loading live data from sheets: %s", pages)

            current_df = await _load_from_live_source(
                pages, spreadsheet_id, limit=2000
            )

            snapshot_path = (
                getattr(args, "baseline_snapshot", "")
                or os.getenv("DRIFT_BASELINE_SNAPSHOT", "")
            ).strip()

            if snapshot_path and Path(snapshot_path).exists():
                logger.info("Loading baseline snapshot from: %s", snapshot_path)
                baseline_df = _load_df(snapshot_path)
                common_cols = [
                    c for c in current_df.columns if c in baseline_df.columns
                ]
                if not common_cols:
                    logger.warning(
                        "Baseline snapshot has no overlapping columns with live data. "
                        "Saving current as new baseline snapshot and exiting."
                    )
                    current_df.to_csv(snapshot_path, index=False)
                    return 0
                baseline_df = baseline_df[common_cols]
                current_df = current_df[common_cols]
            else:
                # v4.3.0: first-run bootstrap — save snapshot, skip analysis.
                logger.warning(
                    "No baseline snapshot found (DRIFT_BASELINE_SNAPSHOT=%r). "
                    "First-run bootstrap: saving current live data as baseline "
                    "and SKIPPING analysis. Next run will detect drift.",
                    snapshot_path or "not set",
                )
                baseline_df = current_df.copy()
                if snapshot_path:
                    current_df.to_csv(snapshot_path, index=False)
                    logger.info("Saved baseline snapshot to: %s", snapshot_path)
                bootstrap_only = True

        else:  # mode == "mock"
            logger.info(
                "Using synthetic mock data (drift=%s)",
                bool(getattr(args, "force_drift", False)),
            )
            baseline_df, current_df = generate_mock_data(
                drift=bool(getattr(args, "force_drift", False))
            )

        if bootstrap_only:
            # v4.3.0: don't run analysis on baseline == current (all-zeros noise)
            logger.info("Bootstrap complete. Run again to detect drift vs saved snapshot.")
            return 0

        if args.features:
            cols = [c.strip() for c in args.features.split(",") if c.strip()]
            keep = [
                c for c in cols
                if c in baseline_df.columns and c in current_df.columns
            ]
            if keep:
                baseline_df = baseline_df[keep]
                current_df = current_df[keep]

        await run_drift_analysis(
            model_name=args.model_name,
            baseline_df=baseline_df,
            current_df=current_df,
            webhook_url=args.webhook,
            export_dir=args.export_dir,
            export_audit=not args.no_audit,
        )
    except Exception as e:
        logger.exception("Drift detection failed: %s", e)
        exit_code = 1
    finally:
        # v4.3.0: executor shutdown always runs (even on exceptions)
        _shutdown_executor(wait=False)

    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Tadawul ML Drift Detector v{SCRIPT_VERSION}"
    )
    parser.add_argument(
        "--model-name", default="ensemble_v1",
        help="Model name label for reporting",
    )
    parser.add_argument(
        "--export-dir", default="reports",
        help="Directory to save drift reports",
    )
    parser.add_argument(
        "--webhook", default=os.getenv("DRIFT_SLACK_WEBHOOK", ""),
        help="Slack webhook URL (optional)",
    )
    parser.add_argument(
        "--force-drift", action="store_true",
        help="Force synthetic drift (mock mode only)",
    )
    parser.add_argument(
        "--mode",
        default=os.getenv("DRIFT_MODE", ""),
        choices=["mock", "file", "live", ""],
        help="Data source mode: mock|file|live. Default: auto-detect from env vars.",
    )
    # File mode
    parser.add_argument(
        "--baseline",
        default=os.getenv("DRIFT_BASELINE_PATH", ""),
        help="Baseline dataset path (.csv/.json/.parquet). "
             "Also: DRIFT_BASELINE_PATH env var.",
    )
    parser.add_argument(
        "--current",
        default=os.getenv("DRIFT_CURRENT_PATH", ""),
        help="Current dataset path (.csv/.json/.parquet). "
             "Also: DRIFT_CURRENT_PATH env var.",
    )
    # Live mode
    parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("DEFAULT_SPREADSHEET_ID", ""),
        help="Google Sheets spreadsheet ID for live mode. "
             "Also: DEFAULT_SPREADSHEET_ID env var.",
    )
    parser.add_argument(
        "--source-pages",
        default=os.getenv(
            "DRIFT_SOURCE_PAGES", "Market_Leaders,Global_Markets,Mutual_Funds"
        ),
        help="Comma-separated sheet tabs for live mode. "
             "Also: DRIFT_SOURCE_PAGES env var.",
    )
    parser.add_argument(
        "--baseline-snapshot",
        default=os.getenv("DRIFT_BASELINE_SNAPSHOT", ""),
        help="JSON/CSV path for live-mode baseline cache. "
             "Also: DRIFT_BASELINE_SNAPSHOT env var.",
    )
    parser.add_argument(
        "--features", default="",
        help="Comma-separated feature whitelist (optional)",
    )
    parser.add_argument(
        "--no-audit", action="store_true",
        help="Disable audit signature export files",
    )

    args = parser.parse_args()

    try:
        sys.exit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        logger.info("Drift detection interrupted by user.")
        _shutdown_executor(wait=False)
        sys.exit(130)
    except Exception:
        _shutdown_executor(wait=False)
        raise


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "DriftSeverity",
    "FeatureDriftStat",
    "ModelDriftReport",
    "FullJitterBackoff",
    "TraceContext",
    "calculate_psi",
    "detect_feature_drift",
    "analyze_model_drift",
    "generate_mock_data",
    "run_drift_analysis",
    "sign_audit",
    "main_async",
    "main",
]


if __name__ == "__main__":
    main()
