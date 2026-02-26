#!/usr/bin/env python3
# scripts/drift_detection.py
"""
================================================================================
TADAWUL FAST BRIDGE – ML MODEL DRIFT DETECTOR (v4.1.0)
================================================================================
QUANTUM EDITION | ISOLATION FOREST | KS/PSI | SAFE OTEL | SAMA AUDIT SIGNATURE

Fixes & upgrades in v4.1.0
- ✅ Fix OTEL bug: start_as_current_span() returns a context manager, not a span
  (prevents: '_AgnosticContextManager' has no attribute 'set_attributes')
- ✅ Robust optional-ML handling (runs even if SciPy/Sklearn are missing; degrades safely)
- ✅ Deterministic CPU executor + no thread recreation overhead
- ✅ Safer PSI (quantile buckets, avoids divide-by-zero, handles constant arrays)
- ✅ Adds signed audit log entry (HMAC-SHA256) with canonical JSON
- ✅ Adds baseline/current loading from CSV/Parquet (optional) while keeping mock mode
- ✅ Slack/Webhook alert hardened (timeout + silent fail if aiohttp missing)
- ✅ Output includes top drift features summary for quick diagnosis

Core Capabilities
- Concept drift detection: KS-test (if available), PSI, Wasserstein (if available)
- Regime anomaly: IsolationForest (if available)
- Signed audit record for alerts/events (SAMA-style integrity)
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import hashlib
import hmac
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
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, Awaitable

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

_TRACING_ENABLED = (os.getenv("CORE_TRACING_ENABLED", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

class TraceContext:
    """
    Safe OTEL context wrapper.

    IMPORTANT:
    tracer.start_as_current_span() returns a context manager.
    We must enter it to obtain the span instance.
    """
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._tracer = trace.get_tracer(__name__) if (_OTEL_AVAILABLE and _TRACING_ENABLED and trace) else None
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
SCRIPT_VERSION = "4.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DriftDetector")

_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="DriftWorker")

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
    recommendation: str
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
# Full Jitter Exponential Backoff
# =============================================================================

class FullJitterBackoff:
    def __init__(self, max_retries: int = 3, base_delay: float = 0.8, max_delay: float = 15.0):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = max(0.05, float(base_delay))
        self.max_delay = max(0.2, float(max_delay))

    async def execute_async(self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
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
                logger.warning(f"Operation failed: {e}. Retry in {sleep_time:.2f}s ({attempt+1}/{self.max_retries})")
                await asyncio.sleep(sleep_time)
        raise last_exc  # type: ignore

# =============================================================================
# Utility: Signed audit log entry (HMAC)
# =============================================================================

def _audit_secret() -> bytes:
    # Set DRIFT_AUDIT_SECRET in Render env for real use
    s = (os.getenv("DRIFT_AUDIT_SECRET") or os.getenv("AUDIT_SECRET") or "").strip()
    if not s:
        # non-empty default prevents crashes; do NOT rely on this for real auditing
        s = "CHANGE_ME_DRIFT_AUDIT_SECRET"
    return s.encode("utf-8")

def sign_audit(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces a deterministic signature over canonical JSON bytes.
    """
    canonical = json_dumps(payload, indent=0)
    sig = hmac.new(_audit_secret(), canonical.encode("utf-8"), hashlib.sha256).hexdigest()
    return {"signature": sig, "algo": "HMAC-SHA256", "canonical": canonical}

# =============================================================================
# Drift Core
# =============================================================================

def _to_np(x: Any) -> "np.ndarray":
    # safe conversion; caller guarantees numpy exists
    return np.asarray(x, dtype=float)  # type: ignore

def _finite(arr: "np.ndarray") -> "np.ndarray":
    # remove NaN/Inf
    return arr[np.isfinite(arr)]  # type: ignore

def calculate_psi(expected: "np.ndarray", actual: "np.ndarray", buckets: int = 10) -> float:
    """
    PSI using quantile buckets based on expected distribution.
    Robust for constant arrays.
    """
    expected = _finite(expected)
    actual = _finite(actual)
    if expected.size == 0 or actual.size == 0:
        return 0.0

    # If expected is constant, PSI is not meaningful; return 0 unless actual differs a lot
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

    # Defaults when SciPy not available
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

    # Decision: PSI is primary, KS p-value is secondary (when available)
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
    if any(f.severity == DriftSeverity.CRITICAL for f in feature_stats) or drift_pct >= 0.30:
        overall = DriftSeverity.CRITICAL
    elif drift_pct >= 0.15:
        overall = DriftSeverity.WARNING

    recommendation = "No action required."
    if overall == DriftSeverity.CRITICAL:
        recommendation = "IMMEDIATE RETRAINING REQUIRED. Model degraded."
    elif overall == DriftSeverity.WARNING:
        recommendation = "Schedule retraining soon. Monitor performance."

    # IsolationForest anomaly on current (optional)
    anomaly_rate = 0.0
    if _SKLEARN_AVAILABLE and IsolationForest is not None and total > 0 and len(current_df) >= max(80, min_samples):
        try:
            use_df = current_df[features].copy()
            # keep numeric only; coerce
            use_df = use_df.apply(pd.to_numeric, errors="coerce")  # type: ignore
            use_df = use_df.fillna(use_df.mean(numeric_only=True))
            X = use_df.values
            iso = IsolationForest(contamination=0.05, random_state=42)
            preds = iso.fit_predict(X)
            anomaly_rate = float((preds == -1).sum() / max(1, len(preds)))
        except Exception:
            anomaly_rate = 0.0

    # top drift features by PSI
    top = sorted(feature_stats, key=lambda x: x.psi_score, reverse=True)[:top_k]
    top_names = [f"{t.feature_name} (PSI={t.psi_score:.3f}, p={t.p_value:.3g})" for t in top if t.is_drifted][:top_k]

    return ModelDriftReport(
        model_name=model_name,
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
        total_features=total,
        drifted_features=drifted_count,
        overall_severity=overall,
        feature_stats=feature_stats,
        confidence_drop=0.0,  # placeholder: wire to your prediction logs later
        anomaly_rate=anomaly_rate,
        recommendation=recommendation,
        top_drift_features=top_names,
    )

# =============================================================================
# Webhook Alerting
# =============================================================================

async def send_slack_alert(webhook_url: str, report: ModelDriftReport, audit_sig: Dict[str, Any]) -> None:
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
        logger.error(f"Failed to send Slack alert: {e}")

# =============================================================================
# Data loading
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

def generate_mock_data(drift: bool = False) -> Tuple["pd.DataFrame", "pd.DataFrame"]:
    np.random.seed(42)  # type: ignore
    n = 1000
    baseline = pd.DataFrame({  # type: ignore
        "pe_ratio": np.random.normal(15, 5, n),  # type: ignore
        "volatility": np.random.lognormal(0, 0.5, n),  # type: ignore
        "rsi": np.random.uniform(30, 70, n),  # type: ignore
    })
    if drift:
        current = pd.DataFrame({  # type: ignore
            "pe_ratio": np.random.normal(25, 8, n),  # type: ignore
            "volatility": np.random.lognormal(0.5, 0.8, n),  # type: ignore
            "rsi": np.random.uniform(30, 70, n),  # type: ignore
        })
    else:
        current = pd.DataFrame({  # type: ignore
            "pe_ratio": np.random.normal(15.2, 5.1, n),  # type: ignore
            "volatility": np.random.lognormal(0.05, 0.52, n),  # type: ignore
            "rsi": np.random.uniform(29, 71, n),  # type: ignore
        })
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

    logger.info(f"Drift Analysis Complete: {report.overall_severity.value.upper()}")
    logger.info(f"Drifted Features: {report.drifted_features}/{report.total_features}")
    logger.info(f"Anomaly Rate: {report.anomaly_rate:.2%}")
    logger.info(f"Recommendation: {report.recommendation}")

    os.makedirs(export_dir, exist_ok=True)
    ts = int(time.time())
    export_path = os.path.join(export_dir, f"drift_report_{model_name}_{ts}.json")

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

    def _write():
        payload = report.to_dict()
        payload["audit"] = audit_payload | audit_sig
        with open(export_path, "w", encoding="utf-8") as f:
            f.write(json_dumps(payload, indent=2))

    await loop.run_in_executor(_CPU_EXECUTOR, _write)
    logger.info(f"Report exported to {export_path}")

    if export_audit:
        audit_path = os.path.join(export_dir, f"drift_audit_{model_name}_{ts}.json")
        def _write_audit():
            with open(audit_path, "w", encoding="utf-8") as f:
                f.write(json_dumps(audit_payload | audit_sig, indent=2))
        await loop.run_in_executor(_CPU_EXECUTOR, _write_audit)
        logger.info(f"Audit exported to {audit_path}")

    if report.overall_severity != DriftSeverity.NONE and webhook_url:
        await send_slack_alert(webhook_url, report, audit_sig)

    return report

async def main_async(args: argparse.Namespace) -> int:
    if not _ML_AVAILABLE:
        logger.error("numpy + pandas are required for drift detection.")
        return 1

    # Load data
    if args.baseline and args.current:
        logger.info("Loading baseline/current datasets from disk...")
        baseline_df = _load_df(args.baseline)
        current_df = _load_df(args.current)
    else:
        logger.info("Using synthetic mock data...")
        baseline_df, current_df = generate_mock_data(drift=bool(args.force_drift))

    # Select features if specified
    if args.features:
        cols = [c.strip() for c in args.features.split(",") if c.strip()]
        keep = [c for c in cols if c in baseline_df.columns and c in current_df.columns]
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

    _CPU_EXECUTOR.shutdown(wait=False)
    return 0

def main() -> None:
    parser = argparse.ArgumentParser(description=f"Tadawul ML Drift Detector v{SCRIPT_VERSION}")
    parser.add_argument("--model-name", default="ensemble_v1", help="Model name label for reporting")
    parser.add_argument("--export-dir", default="reports", help="Directory to save drift reports")
    parser.add_argument("--webhook", default=os.getenv("DRIFT_SLACK_WEBHOOK", ""), help="Slack webhook URL (optional)")
    parser.add_argument("--force-drift", action="store_true", help="Force synthetic drift (mock mode)")
    parser.add_argument("--baseline", default="", help="Baseline dataset path (.csv/.json/.parquet)")
    parser.add_argument("--current", default="", help="Current dataset path (.csv/.json/.parquet)")
    parser.add_argument("--features", default="", help="Comma-separated feature whitelist (optional)")
    parser.add_argument("--no-audit", action="store_true", help="Disable audit signature export files")

    args = parser.parse_args()

    try:
        sys.exit(asyncio.run(main_async(args)))
    except KeyboardInterrupt:
        logger.info("Drift detection interrupted by user.")
        _CPU_EXECUTOR.shutdown(wait=False)
        sys.exit(130)

if __name__ == "__main__":
    main()
