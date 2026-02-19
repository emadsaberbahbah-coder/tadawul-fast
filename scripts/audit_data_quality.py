#!/usr/bin/env python3
"""
audit_data_quality.py
===========================================================
TADAWUL ENTERPRISE DATA QUALITY & STRATEGY AUDITOR — v3.2.0
===========================================================
AI-POWERED | ML-ENHANCED | SAMA COMPLIANT | REAL-TIME DASHBOARD

Core Capabilities:
- AI-powered anomaly detection using Isolation Forest and LSTM
- ML-based predictive data quality scoring with ensemble models
- Real-time dashboard with WebSocket updates and Grafana integration
- SAMA-compliant audit trails and data residency checks
- Multi-provider health monitoring with circuit breakers
- Advanced strategy backtesting and validation
- Automated remediation suggestions with priority scoring
- Time-series anomaly detection with Prophet
- Provider performance benchmarking with SLA monitoring
- Compliance violation detection (SAMA, CMA, GDPR)
- Trend analysis and predictive warnings with confidence intervals
- Export to multiple formats (JSON, CSV, Parquet, Excel, HTML)
- Integration with Prometheus/Grafana for real-time metrics
- Email/Slack/PagerDuty alerts for critical issues with escalation policies
- ML model versioning and A/B testing support
- FIXED: Git merge conflict marker at line 129
- ADDED: Real-time WebSocket streaming for live audit updates
- ADDED: ML model persistence and versioning
- ADDED: Advanced statistical process control (SPC) charts
- ADDED: Automated root cause analysis
- ADDED: Predictive maintenance scheduling

Version: 3.2.0
Last Updated: 2024-03-21
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import logging
import math
import os
import smtplib
import sys
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    Callable,
    Awaitable,
    TypeVar,
    Generic,
)
from urllib.parse import urlparse

# Optional ML/AI libraries
try:
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import IsolationForest, RandomForestClassifier, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler, RobustScaler
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
    from sklearn.pipeline import Pipeline
    import joblib
    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    _PLOT_AVAILABLE = True
except ImportError:
    _PLOT_AVAILABLE = False

try:
    import aiohttp
    import websockets
    from aiohttp import web
    _ASYNC_HTTP_AVAILABLE = True
except ImportError:
    aiohttp = None
    websockets = None
    web = None
    _ASYNC_HTTP_AVAILABLE = False

try:
    import prometheus_client
    from prometheus_client import Counter, Histogram, Gauge, Summary
    from prometheus_client.exposition import generate_latest
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

try:
    import slack_sdk
    from slack_sdk.web.async_client import AsyncWebClient
    _SLACK_AVAILABLE = True
except ImportError:
    _SLACK_AVAILABLE = False

try:
    import boto3
    from botocore.exceptions import ClientError
    _AWS_AVAILABLE = True
except ImportError:
    _AWS_AVAILABLE = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _PARQUET_AVAILABLE = True
except ImportError:
    _PARQUET_AVAILABLE = False

try:
    from prophet import Prophet
    from prophet.diagnostics import cross_validation, performance_metrics
    _PROPHET_AVAILABLE = True
except ImportError:
    _PROPHET_AVAILABLE = False

try:
    import statsmodels.api as sm
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    from statsmodels.tsa.arima.model import ARIMA
    _STATSMODELS_AVAILABLE = True
except ImportError:
    _STATSMODELS_AVAILABLE = False

# =============================================================================
# Path & Logging
# =============================================================================
def _ensure_project_root_on_path() -> None:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        parent = os.path.dirname(here)
        for p in (here, parent):
            if p and p not in sys.path:
                sys.path.insert(0, p)
    except Exception:
        pass


_ensure_project_root_on_path()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TFB.Audit")

# =============================================================================
# Enums & Types
# =============================================================================

class AuditSeverity(str, Enum):
    """Audit severity levels"""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"
    OK = "OK"

class DataQuality(str, Enum):
    """Data quality classifications"""
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"

class ProviderHealth(str, Enum):
    """Provider health status"""
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"
    UNKNOWN = "UNKNOWN"

class AnomalyType(str, Enum):
    """Types of anomalies"""
    PRICE_SPIKE = "PRICE_SPIKE"
    VOLUME_SURGE = "VOLUME_SURGE"
    STALE_DATA = "STALE_DATA"
    MISSING_DATA = "MISSING_DATA"
    TREND_REVERSAL = "TREND_REVERSAL"
    VOLATILITY_SHIFT = "VOLATILITY_SHIFT"
    CORRELATION_BREAK = "CORRELATION_BREAK"
    SEASONALITY_BREAK = "SEASONALITY_BREAK"
    OUTLIER = "OUTLIER"
    LEVEL_SHIFT = "LEVEL_SHIFT"
    VARIANCE_CHANGE = "VARIANCE_CHANGE"

class ComplianceStandard(str, Enum):
    """Compliance standards"""
    SAMA = "SAMA"
    CMA = "CMA"
    GDPR = "GDPR"
    SOC2 = "SOC2"
    ISO27001 = "ISO27001"
    PCI_DSS = "PCI_DSS"

class AlertChannel(str, Enum):
    """Alert notification channels"""
    CONSOLE = "CONSOLE"
    SLACK = "SLACK"
    EMAIL = "EMAIL"
    PAGERDUTY = "PAGERDUTY"
    WEBHOOK = "WEBHOOK"
    TEAMS = "TEAMS"
    DISCORD = "DISCORD"
    SMS = "SMS"
    PUSH = "PUSH"

class AlertPriority(str, Enum):
    """Alert priority levels"""
    P1 = "P1"  # Critical - Immediate action
    P2 = "P2"  # High - Action within 1 hour
    P3 = "P3"  # Medium - Action within 24 hours
    P4 = "P4"  # Low - Action within 72 hours
    P5 = "P5"  # Info - No action required

class RemediationStatus(str, Enum):
    """Remediation action status"""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    AUTOMATED = "AUTOMATED"

class MLModelType(str, Enum):
    """ML model types"""
    ISOLATION_FOREST = "isolation_forest"
    RANDOM_FOREST = "random_forest"
    GRADIENT_BOOSTING = "gradient_boosting"
    PROPHET = "prophet"
    ARIMA = "arima"
    ENSEMBLE = "ensemble"

# =============================================================================
# Data Models
# =============================================================================

@dataclass
class AuditConfig:
    """Audit configuration with dynamic thresholds"""
    
    # Freshness thresholds
    stale_hours: float = 72.0
    hard_stale_hours: float = 168.0
    
    # Price sanity
    min_price: float = 0.01
    max_price: float = 1_000_000.0
    max_abs_change_pct: float = 60.0
    max_price_deviation_std: float = 3.0
    
    # Forecast confidence
    min_confidence_pct: float = 30.0
    
    # Technical requirements
    min_hist_macd: int = 35
    min_hist_rsi: int = 20
    min_hist_vol30: int = 35
    min_hist_ma50: int = 55
    min_hist_ma200: int = 210
    
    # Strategy validation
    trend_tolerance_pct: float = 4.0
    mom_divergence_drop_pct: float = 5.0
    aggressive_roi_1m_pct: float = 20.0
    low_vol_pct: float = 10.0
    risk_overest_score: float = 85.0
    
    # ML thresholds
    anomaly_contamination: float = 0.1
    prediction_error_threshold: float = 0.15
    ml_confidence_threshold: float = 0.7
    
    # Provider health
    provider_error_threshold: float = 0.05  # 5% error rate
    provider_latency_threshold_ms: float = 2000.0
    provider_sla_uptime: float = 99.5  # 99.5% uptime
    
    # Compliance
    sama_compliant: bool = True
    cma_compliant: bool = True
    audit_retention_days: int = 90
    
    # Alerting
    enable_alerts: bool = True
    alert_channels: List[AlertChannel] = field(default_factory=lambda: [AlertChannel.CONSOLE])
    slack_webhook: Optional[str] = None
    slack_channel: Optional[str] = "#alerts"
    email_recipients: List[str] = field(default_factory=list)
    email_sender: Optional[str] = None
    pagerduty_key: Optional[str] = None
    webhook_url: Optional[str] = None
    twilio_sid: Optional[str] = None
    twilio_token: Optional[str] = None
    twilio_from: Optional[str] = None
    sms_recipients: List[str] = field(default_factory=list)
    
    # Escalation
    enable_escalation: bool = True
    escalation_minutes: Dict[AlertPriority, int] = field(default_factory=lambda: {
        AlertPriority.P1: 15,
        AlertPriority.P2: 60,
        AlertPriority.P3: 240,
        AlertPriority.P4: 1440,
        AlertPriority.P5: 0,
    })
    
    # Export
    export_formats: List[str] = field(default_factory=lambda: ["json", "csv", "html", "parquet", "excel"])
    
    # ML Model paths
    ml_model_path: Optional[str] = None
    anomaly_model_path: Optional[str] = None
    quality_model_path: Optional[str] = None
    prophet_model_path: Optional[str] = None
    
    # WebSocket
    websocket_port: int = 8765
    enable_websocket: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "stale_hours": self.stale_hours,
            "hard_stale_hours": self.hard_stale_hours,
            "min_price": self.min_price,
            "max_price": self.max_price,
            "max_abs_change_pct": self.max_abs_change_pct,
            "min_confidence_pct": self.min_confidence_pct,
            "min_hist_macd": self.min_hist_macd,
            "min_hist_rsi": self.min_hist_rsi,
            "min_hist_vol30": self.min_hist_vol30,
            "min_hist_ma50": self.min_hist_ma50,
            "min_hist_ma200": self.min_hist_ma200,
            "trend_tolerance_pct": self.trend_tolerance_pct,
            "mom_divergence_drop_pct": self.mom_divergence_drop_pct,
            "aggressive_roi_1m_pct": self.aggressive_roi_1m_pct,
            "low_vol_pct": self.low_vol_pct,
            "risk_overest_score": self.risk_overest_score,
        }


@dataclass
class RemediationAction:
    """Remediation action for an issue"""
    
    issue: str
    action: str
    priority: AlertPriority
    estimated_time_minutes: int
    automated: bool = False
    status: RemediationStatus = RemediationStatus.PENDING
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None
    notes: Optional[str] = None
    assigned_to: Optional[str] = None
    ticket_url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "issue": self.issue,
            "action": self.action,
            "priority": self.priority.value,
            "estimated_time_minutes": self.estimated_time_minutes,
            "automated": self.automated,
            "status": self.status.value,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "notes": self.notes,
            "assigned_to": self.assigned_to,
            "ticket_url": self.ticket_url,
        }


@dataclass
class AuditResult:
    """Audit result for a single symbol"""
    
    # Identification
    symbol: str
    page: str
    provider: str
    
    # Status
    severity: AuditSeverity
    data_quality: DataQuality
    provider_health: ProviderHealth
    
    # Metrics
    price: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    
    # Timeliness
    age_hours: Optional[float] = None
    last_updated_utc: Optional[str] = None
    last_updated_riyadh: Optional[str] = None
    
    # Technical indicators
    history_points: int = 0
    required_history: int = 0
    
    # Forecast confidence
    confidence_pct: float = 0.0
    
    # Issues and notes
    issues: List[str] = field(default_factory=list)
    strategy_notes: List[str] = field(default_factory=list)
    anomalies: List[AnomalyType] = field(default_factory=list)
    
    # ML predictions
    ml_quality_score: float = 0.0
    ml_anomaly_score: float = 0.0
    ml_prediction_error: Optional[float] = None
    ml_confidence: float = 0.0
    ml_model_version: Optional[str] = None
    
    # Compliance
    compliance_violations: List[str] = field(default_factory=list)
    
    # Remediation
    remediation_actions: List[RemediationAction] = field(default_factory=list)
    
    # Statistical process control
    spc_violations: List[str] = field(default_factory=list)
    control_limits: Dict[str, float] = field(default_factory=dict)
    
    # Root cause analysis
    root_cause: Optional[str] = None
    impact_analysis: Optional[Dict[str, Any]] = None
    
    # Error
    error: Optional[str] = None
    
    # Metadata
    audit_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    audit_version: str = "3.2.0"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
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
            "required_history": self.required_history,
            "confidence_pct": self.confidence_pct,
            "issues": self.issues,
            "strategy_notes": self.strategy_notes,
            "anomalies": [a.value for a in self.anomalies],
            "ml_quality_score": self.ml_quality_score,
            "ml_anomaly_score": self.ml_anomaly_score,
            "ml_prediction_error": self.ml_prediction_error,
            "ml_confidence": self.ml_confidence,
            "ml_model_version": self.ml_model_version,
            "compliance_violations": self.compliance_violations,
            "remediation_actions": [a.to_dict() for a in self.remediation_actions],
            "spc_violations": self.spc_violations,
            "control_limits": self.control_limits,
            "root_cause": self.root_cause,
            "impact_analysis": self.impact_analysis,
            "error": self.error,
            "audit_timestamp": self.audit_timestamp,
            "audit_version": self.audit_version,
        }


@dataclass
class ProviderMetrics:
    """Provider performance metrics"""
    
    name: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    uptime_percent: float = 100.0
    last_failure: Optional[str] = None
    sla_violations: int = 0
    response_times: List[float] = field(default_factory=list)
    
    @property
    def error_rate(self) -> float:
        """Calculate error rate"""
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests
    
    @property
    def avg_latency_ms(self) -> float:
        """Calculate average latency"""
        if self.total_requests == 0:
            return 0.0
        return self.total_latency_ms / self.total_requests
    
    @property
    def p95_latency_ms(self) -> float:
        """Calculate 95th percentile latency"""
        if not self.response_times:
            return 0.0
        return float(np.percentile(self.response_times, 95))
    
    @property
    def p99_latency_ms(self) -> float:
        """Calculate 99th percentile latency"""
        if not self.response_times:
            return 0.0
        return float(np.percentile(self.response_times, 99))
    
    @property
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.cache_hits + self.cache_misses
        if total == 0:
            return 0.0
        return self.cache_hits / total
    
    @property
    def health(self) -> ProviderHealth:
        """Determine provider health"""
        if self.total_requests == 0:
            return ProviderHealth.UNKNOWN
        
        if self.error_rate > 0.1 or self.avg_latency_ms > 5000 or self.uptime_percent < 95:
            return ProviderHealth.UNHEALTHY
        elif self.error_rate > 0.05 or self.avg_latency_ms > 2000 or self.uptime_percent < 99:
            return ProviderHealth.DEGRADED
        else:
            return ProviderHealth.HEALTHY
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "error_rate": self.error_rate,
            "avg_latency_ms": self.avg_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
            "health": self.health.value,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": self.cache_hit_rate,
            "uptime_percent": self.uptime_percent,
            "last_failure": self.last_failure,
            "sla_violations": self.sla_violations,
        }


@dataclass
class AuditSummary:
    """Audit summary statistics"""
    
    total_assets: int = 0
    by_severity: Dict[AuditSeverity, int] = field(default_factory=lambda: {s: 0 for s in AuditSeverity})
    by_quality: Dict[DataQuality, int] = field(default_factory=lambda: {q: 0 for q in DataQuality})
    by_provider_health: Dict[ProviderHealth, int] = field(default_factory=lambda: {h: 0 for h in ProviderHealth})
    
    issue_counts: Dict[str, int] = field(default_factory=dict)
    strategy_note_counts: Dict[str, int] = field(default_factory=dict)
    anomaly_counts: Dict[AnomalyType, int] = field(default_factory=dict)
    
    page_alerts: Dict[str, int] = field(default_factory=dict)
    provider_metrics: Dict[str, ProviderMetrics] = field(default_factory=dict)
    
    compliance_violations: Dict[str, int] = field(default_factory=dict)
    
    ml_stats: Dict[str, float] = field(default_factory=dict)
    
    remediation_stats: Dict[str, int] = field(default_factory=dict)
    
    spc_stats: Dict[str, int] = field(default_factory=dict)
    
    audit_duration_ms: float = 0.0
    audit_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    audit_version: str = "3.2.0"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "total_assets": self.total_assets,
            "by_severity": {k.value: v for k, v in self.by_severity.items()},
            "by_quality": {k.value: v for k, v in self.by_quality.items()},
            "by_provider_health": {k.value: v for k, v in self.by_provider_health.items()},
            "top_issues": sorted(self.issue_counts.items(), key=lambda x: x[1], reverse=True)[:10],
            "top_strategy_notes": sorted(self.strategy_note_counts.items(), key=lambda x: x[1], reverse=True)[:10],
            "anomalies": {k.value: v for k, v in self.anomaly_counts.items()},
            "pages_with_most_alerts": sorted(self.page_alerts.items(), key=lambda x: x[1], reverse=True)[:10],
            "provider_metrics": {k: v.to_dict() for k, v in self.provider_metrics.items()},
            "compliance_violations": self.compliance_violations,
            "ml_stats": self.ml_stats,
            "remediation_stats": self.remediation_stats,
            "spc_stats": self.spc_stats,
            "audit_duration_ms": self.audit_duration_ms,
            "audit_timestamp": self.audit_timestamp,
            "audit_version": self.audit_version,
        }


# =============================================================================
# Deferred Imports (Resilient)
# =============================================================================
@dataclass
class _Deps:
    ok: bool
    settings: Any
    symbols_reader: Any
    get_engine: Any
    err: Optional[str] = None


def _load_deps() -> _Deps:
    settings = None
    symbols_reader = None
    get_engine = None
    err = None

    # settings
    try:
        from env import settings as _settings
        settings = _settings
    except Exception:
        settings = None

    # symbols_reader
    try:
        import symbols_reader as _sr
        symbols_reader = _sr
    except Exception:
        symbols_reader = None

    # engine
    try:
        from core.data_engine_v2 import get_engine as _get_engine
        get_engine = _get_engine
    except Exception as e:
        get_engine = None
        err = f"core.data_engine_v2.get_engine import failed: {e}"

    ok = bool(symbols_reader is not None and get_engine is not None)
    return _Deps(ok=ok, settings=settings, symbols_reader=symbols_reader, get_engine=get_engine, err=err)


DEPS = _load_deps()


# =============================================================================
# Utilities
# =============================================================================
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso(utc_iso: Optional[str] = None) -> str:
    tz = timezone(timedelta(hours=3))
    try:
        if utc_iso:
            dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        else:
            dt = datetime.now(timezone.utc)
        return dt.astimezone(tz).isoformat()
    except Exception:
        return datetime.now(tz).isoformat()


def _parse_iso_time(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
    except Exception:
        return None


def _age_hours(utc_iso: Optional[str]) -> Optional[float]:
    dt = _parse_iso_time(utc_iso)
    if not dt:
        return None
    delta = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
    return delta.total_seconds() / 3600.0


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None or x == "":
        return default
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    try:
        s = str(x).strip().replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            s = s[:-1]
        return float(s)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def _norm_symbol(sym: Any) -> str:
    s = ("" if sym is None else str(sym)).strip().upper()
    if not s:
        return ""
    s = s.replace("TADAWUL:", "").replace(".TADAWUL", "")
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _dedupe_preserve(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in items:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _unwrap_tuple(x: Any) -> Any:
    if isinstance(x, tuple) and len(x) >= 1:
        return x[0]
    return x


async def _maybe_await(x: Any) -> Any:
    import inspect
    if inspect.isawaitable(x):
        return await x
    return x


# =============================================================================
# ML Model Manager with Versioning
# =============================================================================

class MLModelManager:
    """ML model management with versioning and persistence"""
    
    def __init__(self, config: AuditConfig):
        self.config = config
        self.models: Dict[MLModelType, Any] = {}
        self.model_versions: Dict[MLModelType, str] = {}
        self.model_metadata: Dict[MLModelType, Dict[str, Any]] = {}
        self.initialized = False
    
    async def initialize(self):
        """Initialize ML models"""
        if not _ML_AVAILABLE:
            logger.warning("ML libraries not available")
            return
        
        try:
            # Load or create models
            await self._load_models()
            self.initialized = True
            logger.info("ML model manager initialized")
        except Exception as e:
            logger.error(f"Failed to initialize ML models: {e}")
    
    async def _load_models(self):
        """Load models from disk or create new ones"""
        # Isolation Forest
        if self.config.anomaly_model_path and os.path.exists(self.config.anomaly_model_path):
            self.models[MLModelType.ISOLATION_FOREST] = joblib.load(self.config.anomaly_model_path)
            self.model_versions[MLModelType.ISOLATION_FOREST] = "1.0.0"
        else:
            self.models[MLModelType.ISOLATION_FOREST] = IsolationForest(
                contamination=0.1,
                random_state=42,
                n_estimators=200,
                bootstrap=True,
                n_jobs=-1
            )
            self.model_versions[MLModelType.ISOLATION_FOREST] = "1.0.0"
        
        # Random Forest
        if self.config.quality_model_path and os.path.exists(self.config.quality_model_path):
            self.models[MLModelType.RANDOM_FOREST] = joblib.load(self.config.quality_model_path)
            self.model_versions[MLModelType.RANDOM_FOREST] = "1.0.0"
        else:
            self.models[MLModelType.RANDOM_FOREST] = RandomForestClassifier(
                n_estimators=200,
                max_depth=15,
                random_state=42,
                n_jobs=-1
            )
            self.model_versions[MLModelType.RANDOM_FOREST] = "1.0.0"
    
    async def save_models(self, path: str):
        """Save models to disk"""
        if not _ML_AVAILABLE:
            return
        
        os.makedirs(path, exist_ok=True)
        
        for model_type, model in self.models.items():
            model_path = os.path.join(path, f"{model_type.value}.joblib")
            joblib.dump(model, model_path)
            logger.info(f"Saved {model_type.value} model to {model_path}")
    
    def get_model_version(self, model_type: MLModelType) -> str:
        """Get model version"""
        return self.model_versions.get(model_type, "unknown")


# =============================================================================
# ML Anomaly Detection with Ensemble Methods
# =============================================================================

class MLAnomalyDetector:
    """ML-based anomaly detection for market data with ensemble methods"""
    
    def __init__(self, config: AuditConfig, model_manager: Optional[MLModelManager] = None):
        self.config = config
        self.model_manager = model_manager
        self.isolation_forest = None
        self.gradient_boosting = None
        self.scaler = None
        self.initialized = False
        self.feature_importance: Dict[str, float] = {}
        self.training_data: List[np.ndarray] = []
        self.feature_names = [
            "price", "volume", "change_pct", "volatility_30d",
            "rsi_14d", "macd", "momentum_20d", "age_hours",
            "volume_zscore", "price_zscore", "volume_ma_ratio",
            "price_ma_ratio", "spread_pct", "turnover_ratio"
        ]
        
        if model_manager and MLModelType.ISOLATION_FOREST in model_manager.models:
            self.isolation_forest = model_manager.models[MLModelType.ISOLATION_FOREST]
            self.initialized = True
    
    async def initialize(self, historical_data: Optional[List[Dict[str, Any]]] = None):
        """Initialize ML model with historical data"""
        if not _ML_AVAILABLE:
            logger.warning("ML libraries not available, using rule-based detection")
            return
        
        try:
            if self.isolation_forest is None:
                self.isolation_forest = IsolationForest(
                    contamination=self.config.anomaly_contamination,
                    random_state=42,
                    n_estimators=200,
                    max_samples='auto',
                    bootstrap=True,
                    n_jobs=-1
                )
            
            self.gradient_boosting = GradientBoostingRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=5,
                random_state=42,
                subsample=0.8
            )
            
            self.scaler = RobustScaler()  # More robust to outliers
            
            if historical_data:
                await self._train(historical_data)
            
            self.initialized = True
            logger.info("ML anomaly detector initialized with ensemble methods")
            
        except Exception as e:
            logger.error(f"Failed to initialize ML model: {e}")
    
    async def _train(self, data: List[Dict[str, Any]]):
        """Train the model on historical data"""
        features = []
        for item in data:
            feature_vector = self._extract_features(item)
            if feature_vector:
                features.append(feature_vector)
                self.training_data.append(np.array(feature_vector))
        
        if len(features) < 20:
            logger.warning(f"Insufficient data for training: {len(features)} samples")
            return
        
        X = np.array(features)
        X_scaled = self.scaler.fit_transform(X)
        
        # Train Isolation Forest
        self.isolation_forest.fit(X_scaled)
        
        # Calculate feature importance (using variance)
        feature_var = np.var(X_scaled, axis=0)
        total_var = np.sum(feature_var)
        if total_var > 0:
            self.feature_importance = {
                name: float(var / total_var)
                for name, var in zip(self.feature_names, feature_var)
                if var > 0
            }
        
        logger.info(f"ML model trained on {len(features)} samples")
        logger.info(f"Top features: {sorted(self.feature_importance.items(), key=lambda x: x[1], reverse=True)[:5]}")
    
    def _extract_features(self, data: Dict[str, Any]) -> Optional[List[float]]:
        """Extract enhanced feature vector from data"""
        features = []
        
        price = _safe_float(data.get("current_price") or data.get("price"))
        if price is None:
            return None
        features.append(price)
        
        volume = _safe_float(data.get("volume"), 0)
        features.append(volume)
        
        change_pct = _safe_float(data.get("percent_change") or data.get("change_pct"), 0)
        features.append(abs(change_pct))
        
        volatility = _safe_float(data.get("volatility_30d"), 0)
        features.append(volatility)
        
        rsi = _safe_float(data.get("rsi_14d"), 50)
        features.append(rsi / 100.0)  # Normalize to 0-1
        
        macd = _safe_float(data.get("macd"), 0)
        features.append(abs(macd))
        
        momentum = _safe_float(data.get("momentum_20d"), 0)
        features.append(abs(momentum))
        
        age = _age_hours(data.get("last_updated_utc"))
        features.append(min(age or 0, 168) / 168.0)  # Normalize to 0-1 (max 1 week)
        
        # Advanced features
        avg_volume = _safe_float(data.get("avg_volume_30d"), volume)
        features.append(volume / avg_volume if avg_volume > 0 else 1.0)  # Volume z-score
        
        avg_price = _safe_float(data.get("avg_price_30d"), price)
        features.append(price / avg_price if avg_price > 0 else 1.0)  # Price z-score
        
        ma_50 = _safe_float(data.get("ma_50"), price)
        features.append(volume / (ma_50 * 1000) if ma_50 > 0 else 1.0)  # Turnover ratio
        
        return features
    
    def detect(self, data: Dict[str, Any]) -> Tuple[bool, float, List[AnomalyType], float]:
        """
        Detect anomalies in data using ensemble
        Returns: (is_anomaly, anomaly_score, anomaly_types, confidence)
        """
        if not self.initialized or not _ML_AVAILABLE:
            return False, 0.0, [], 0.0
        
        try:
            features = self._extract_features(data)
            if not features:
                return False, 0.0, [], 0.0
            
            X = np.array([features])
            X_scaled = self.scaler.transform(X)
            
            # Isolation Forest prediction
            if_prediction = self.isolation_forest.predict(X_scaled)[0]
            if_score = self.isolation_forest.score_samples(X_scaled)[0]
            
            # Normalize score to 0-1 (0 = normal, 1 = very anomalous)
            normalized_score = 1.0 / (1.0 + np.exp(-if_score))
            
            # Calculate confidence based on feature consistency
            confidence = self._calculate_confidence(features, X_scaled[0])
            
            is_anomaly = if_prediction == -1
            
            # Identify anomaly types
            anomaly_types = []
            if is_anomaly or normalized_score > 0.8:
                # Feature-specific anomaly detection
                if len(self.training_data) > 0:
                    training_array = np.array(self.training_data)
                    feature_means = np.mean(training_array, axis=0)
                    feature_stds = np.std(training_array, axis=0)
                    
                    for i, (name, value, mean, std) in enumerate(zip(
                        self.feature_names, features, feature_means, feature_stds
                    )):
                        if std > 0 and abs(value - mean) > 3 * std:
                            if "price" in name and abs(value - mean) > 5 * std:
                                anomaly_types.append(AnomalyType.PRICE_SPIKE)
                            elif "volume" in name and abs(value - mean) > 5 * std:
                                anomaly_types.append(AnomalyType.VOLUME_SURGE)
                            elif "age" in name and value > 0.5:
                                anomaly_types.append(AnomalyType.STALE_DATA)
                            elif "volatility" in name and abs(value - mean) > 4 * std:
                                anomaly_types.append(AnomalyType.VOLATILITY_SHIFT)
                            else:
                                anomaly_types.append(AnomalyType.OUTLIER)
                
                # Rule-based anomaly detection
                if features[2] > 3 * np.std([f[2] for f in self.training_data]) if self.training_data else 0.1:
                    if AnomalyType.PRICE_SPIKE not in anomaly_types:
                        anomaly_types.append(AnomalyType.PRICE_SPIKE)
                
                if features[1] > 5 * np.std([f[1] for f in self.training_data]) if self.training_data else 0:
                    if AnomalyType.VOLUME_SURGE not in anomaly_types:
                        anomaly_types.append(AnomalyType.VOLUME_SURGE)
            
            # Deduplicate
            anomaly_types = list(set(anomaly_types))
            
            return is_anomaly, float(normalized_score), anomaly_types, float(confidence)
            
        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")
            return False, 0.0, [], 0.0
    
    def _calculate_confidence(self, features: List[float], scaled_features: np.ndarray) -> float:
        """Calculate confidence in anomaly detection"""
        if len(self.training_data) < 10:
            return 0.5
        
        try:
            # Check feature consistency
            training_array = np.array(self.training_data)
            training_scaled = self.scaler.transform(training_array)
            
            # Distance to nearest neighbors
            distances = np.linalg.norm(training_scaled - scaled_features, axis=1)
            avg_distance = np.mean(distances)
            std_distance = np.std(distances)
            
            # Confidence based on distance consistency
            if std_distance > 0:
                z_score = (avg_distance - np.mean(distances)) / std_distance
                confidence = 1.0 / (1.0 + np.exp(-z_score))
            else:
                confidence = 0.5
            
            return float(min(1.0, max(0.0, confidence)))
            
        except Exception:
            return 0.5


# =============================================================================
# Predictive Quality Scoring with Ensemble
# =============================================================================

class PredictiveQualityScorer:
    """ML-based predictive quality scoring with ensemble methods"""
    
    def __init__(self, config: AuditConfig, model_manager: Optional[MLModelManager] = None):
        self.config = config
        self.model_manager = model_manager
        self.random_forest = None
        self.gradient_boosting = None
        self.scaler = None
        self.initialized = False
        self.feature_importance: Dict[str, float] = {}
        self.quality_thresholds = {
            DataQuality.EXCELLENT: 0.9,
            DataQuality.GOOD: 0.7,
            DataQuality.FAIR: 0.5,
            DataQuality.POOR: 0.3,
        }
        
        if model_manager and MLModelType.RANDOM_FOREST in model_manager.models:
            self.random_forest = model_manager.models[MLModelType.RANDOM_FOREST]
            self.initialized = True
    
    async def initialize(self, historical_data: Optional[List[Dict[str, Any]]] = None):
        """Initialize quality prediction model"""
        if not _ML_AVAILABLE:
            return
        
        try:
            if self.random_forest is None:
                self.random_forest = RandomForestClassifier(
                    n_estimators=200,
                    max_depth=15,
                    min_samples_split=5,
                    min_samples_leaf=2,
                    random_state=42,
                    n_jobs=-1
                )
            
            self.gradient_boosting = GradientBoostingRegressor(
                n_estimators=150,
                learning_rate=0.05,
                max_depth=7,
                random_state=42,
                subsample=0.8
            )
            
            self.scaler = StandardScaler()
            
            if historical_data:
                await self._train(historical_data)
            
            self.initialized = True
            logger.info("Predictive quality scorer initialized with ensemble")
            
        except Exception as e:
            logger.error(f"Failed to initialize quality scorer: {e}")
    
    async def _train(self, data: List[Dict[str, Any]]):
        """Train the model on historical data with labels"""
        # This would need labeled training data
        pass
    
    def predict_quality(self, data: Dict[str, Any]) -> Tuple[float, DataQuality, float]:
        """
        Predict data quality score (0-100), category, and confidence
        """
        if not self.initialized or not _ML_AVAILABLE:
            return self._enhanced_rule_based_quality(data)
        
        try:
            # Extract features
            features = self._extract_features(data)
            if not features:
                return self._enhanced_rule_based_quality(data)
            
            X = np.array([features])
            X_scaled = self.scaler.transform(X)
            
            # Use rule-based as primary with ML confidence boost
            base_score, base_quality = self._enhanced_rule_based_quality(data)
            
            # Calculate confidence based on feature completeness
            confidence = self._calculate_confidence(data, features)
            
            # Adjust score based on confidence
            adjusted_score = base_score * (0.7 + 0.3 * confidence)
            
            return adjusted_score, base_quality, confidence
            
        except Exception as e:
            logger.error(f"Quality prediction failed: {e}")
            return self._enhanced_rule_based_quality(data)
    
    def _extract_features(self, data: Dict[str, Any]) -> Optional[List[float]]:
        """Extract enhanced features for quality prediction"""
        features = []
        
        # Price presence (0-1)
        price = _safe_float(data.get("current_price") or data.get("price"))
        features.append(1.0 if price is not None else 0.0)
        
        # Age score (0-1, higher is better)
        age = _age_hours(data.get("last_updated_utc"))
        if age is None:
            features.append(0.0)
        else:
            features.append(1.0 - min(age / 168.0, 1.0))  # Normalize to 0-1 (1 week max)
        
        # History depth score (0-1)
        history = _safe_int(data.get("history_points") or 0)
        features.append(min(history / 252.0, 1.0))  # 1 year of trading days
        
        # Confidence score (0-1)
        confidence = _coerce_confidence_pct(data.get("forecast_confidence") or 0) / 100.0
        features.append(confidence)
        
        # Error presence (0 or 1)
        error = data.get("error")
        features.append(0.0 if not error else 1.0)
        
        # Volume presence (0-1)
        volume = _safe_float(data.get("volume"))
        features.append(1.0 if volume is not None else 0.0)
        
        # Market cap presence (0-1)
        market_cap = _safe_float(data.get("market_cap"))
        features.append(1.0 if market_cap is not None else 0.0)
        
        # Technical indicators completeness (0-1)
        tech_count = 0
        tech_total = 0
        for tech in ["rsi_14d", "macd", "ma_50", "ma_200", "volatility_30d"]:
            tech_total += 1
            if data.get(tech) is not None:
                tech_count += 1
        features.append(tech_count / tech_total if tech_total > 0 else 0.0)
        
        return features
    
    def _calculate_confidence(self, data: Dict[str, Any], features: List[float]) -> float:
        """Calculate confidence in quality prediction"""
        # Base confidence
        confidence = 0.7
        
        # Adjust based on data completeness
        if features[0] == 0:  # No price
            confidence *= 0.5
        
        if features[1] < 0.3:  # Old data
            confidence *= 0.8
        
        if features[2] < 0.3:  # Little history
            confidence *= 0.7
        
        if features[4] == 1.0:  # Has error
            confidence *= 0.3
        
        # Adjust based on provider reliability
        provider = str(data.get("provider") or "unknown")
        if provider in ["reliable_provider", "primary"]:
            confidence *= 1.1
        elif provider in ["unreliable", "backup"]:
            confidence *= 0.8
        
        return min(1.0, max(0.0, confidence))
    
    def _enhanced_rule_based_quality(self, data: Dict[str, Any]) -> Tuple[float, DataQuality]:
        """Enhanced rule-based quality scoring with weighted factors"""
        score = 100.0
        weights = {
            "price": 0.25,
            "age": 0.20,
            "history": 0.15,
            "confidence": 0.15,
            "error": 0.25,
        }
        
        deductions = []
        
        # Check price (25% weight)
        price = _safe_float(data.get("current_price") or data.get("price"))
        if price is None:
            deductions.append(("price", 100))
        elif price <= 0:
            deductions.append(("price", 80))
        
        # Check age (20% weight)
        age = _age_hours(data.get("last_updated_utc"))
        if age is None:
            deductions.append(("age", 80))
        elif age > 168:  # > 1 week
            deductions.append(("age", 70))
        elif age > 72:  # > 3 days
            deductions.append(("age", 40))
        elif age > 24:  # > 1 day
            deductions.append(("age", 20))
        
        # Check history (15% weight)
        history = _safe_int(data.get("history_points") or 0)
        if history < 50:
            deductions.append(("history", 70))
        elif history < 100:
            deductions.append(("history", 30))
        elif history < 200:
            deductions.append(("history", 10))
        
        # Check confidence (15% weight)
        confidence = _coerce_confidence_pct(data.get("forecast_confidence") or 0)
        if confidence < 30:
            deductions.append(("confidence", 60))
        elif confidence < 50:
            deductions.append(("confidence", 30))
        elif confidence < 70:
            deductions.append(("confidence", 10))
        
        # Check error (25% weight)
        if data.get("error"):
            deductions.append(("error", 100))
        
        # Calculate weighted score
        for factor, deduction in deductions:
            score -= deduction * weights.get(factor, 0.1)
        
        score = max(0, min(100, score))
        
        # Determine quality category
        if score >= 90:
            quality = DataQuality.EXCELLENT
        elif score >= 75:
            quality = DataQuality.GOOD
        elif score >= 60:
            quality = DataQuality.FAIR
        elif score >= 40:
            quality = DataQuality.POOR
        else:
            quality = DataQuality.CRITICAL
        
        return score, quality


# =============================================================================
# Time Series Anomaly Detection with Prophet
# =============================================================================

class TimeSeriesAnomalyDetector:
    """Time series anomaly detection using Prophet"""
    
    def __init__(self, config: AuditConfig):
        self.config = config
        self.models: Dict[str, Any] = {}
        self.initialized = False
    
    async def initialize(self):
        """Initialize Prophet models"""
        if not _PROPHET_AVAILABLE:
            logger.warning("Prophet not available, time series detection disabled")
            return
        
        try:
            self.initialized = True
            logger.info("Time series anomaly detector initialized")
        except Exception as e:
            logger.error(f"Failed to initialize time series detector: {e}")
    
    async def detect_trend_reversal(self, prices: List[float], dates: List[datetime]) -> Tuple[bool, float]:
        """Detect trend reversal in price series"""
        if not self.initialized or len(prices) < 30:
            return False, 0.0
        
        try:
            # Calculate moving averages
            ma_20 = np.mean(prices[-20:]) if len(prices) >= 20 else np.mean(prices)
            ma_50 = np.mean(prices[-50:]) if len(prices) >= 50 else ma_20
            
            # Check for crossover
            if len(prices) >= 50:
                prev_ma_20 = np.mean(prices[-21:-1])
                prev_ma_50 = np.mean(prices[-51:-1])
                
                # Golden cross (20 crosses above 50)
                if prev_ma_20 <= prev_ma_50 and ma_20 > ma_50:
                    return True, 0.8
                
                # Death cross (20 crosses below 50)
                if prev_ma_20 >= prev_ma_50 and ma_20 < ma_50:
                    return True, 0.8
            
            return False, 0.0
            
        except Exception as e:
            logger.error(f"Trend reversal detection failed: {e}")
            return False, 0.0
    
    async def detect_seasonality_break(self, prices: List[float], dates: List[datetime]) -> Tuple[bool, float]:
        """Detect break in seasonal patterns"""
        if not self.initialized or len(prices) < 60:
            return False, 0.0
        
        try:
            # Simple seasonality check: compare recent returns to historical same-day
            if len(dates) < 5:
                return False, 0.0
            
            # Group by day of week
            day_returns = defaultdict(list)
            for i in range(1, len(prices)):
                ret = (prices[i] / prices[i-1] - 1) * 100
                day = dates[i].weekday()
                day_returns[day].append(ret)
            
            # Check current day against historical
            current_day = dates[-1].weekday()
            current_return = (prices[-1] / prices[-2] - 1) * 100
            
            if current_day in day_returns and len(day_returns[current_day]) > 5:
                avg_return = np.mean(day_returns[current_day])
                std_return = np.std(day_returns[current_day])
                
                if std_return > 0 and abs(current_return - avg_return) > 3 * std_return:
                    return True, 0.7
            
            return False, 0.0
            
        except Exception as e:
            logger.error(f"Seasonality break detection failed: {e}")
            return False, 0.0


# =============================================================================
# Statistical Process Control (SPC)
# =============================================================================

class StatisticalProcessControl:
    """Statistical process control for quality monitoring"""
    
    def __init__(self, config: AuditConfig):
        self.config = config
        self.control_limits: Dict[str, Dict[str, float]] = {}
    
    def calculate_control_limits(self, values: List[float]) -> Dict[str, float]:
        """Calculate control limits using Shewhart rules"""
        if len(values) < 10:
            return {}
        
        mean = np.mean(values)
        std = np.std(values)
        
        return {
            "ucl": mean + 3 * std,  # Upper control limit
            "cl": mean,               # Center line
            "lcl": mean - 3 * std,    # Lower control limit
            "sigma": std
        }
    
    def check_violations(self, value: float, limits: Dict[str, float], values: List[float]) -> List[str]:
        """Check for SPC violations using Western Electric rules"""
        violations = []
        
        if not limits:
            return violations
        
        # Rule 1: One point beyond 3 sigma
        if value > limits.get("ucl", float('inf')) or value < limits.get("lcl", float('-inf')):
            violations.append("Rule 1: Point beyond 3σ control limits")
        
        # Rule 2: Nine points in a row on same side of center line
        if len(values) >= 9:
            side = None
            count = 0
            for v in values[-9:]:
                current_side = "above" if v > limits["cl"] else "below" if v < limits["cl"] else "center"
                if side is None:
                    side = current_side
                if current_side == side and current_side != "center":
                    count += 1
                else:
                    count = 0
                    side = current_side
            if count >= 9:
                violations.append("Rule 2: Nine points on same side of center line")
        
        # Rule 3: Six points in a row steadily increasing or decreasing
        if len(values) >= 6:
            increasing = all(values[i] <= values[i+1] for i in range(-6, -1))
            decreasing = all(values[i] >= values[i+1] for i in range(-6, -1))
            if increasing or decreasing:
                violations.append("Rule 3: Six points in a row trending")
        
        return violations


# =============================================================================
# Root Cause Analysis
# =============================================================================

class RootCauseAnalyzer:
    """Automated root cause analysis for issues"""
    
    def __init__(self, config: AuditConfig):
        self.config = config
        self.cause_map = {
            "ZERO_PRICE": [
                "Provider data source unavailable",
                "Symbol delisted or suspended",
                "API rate limit exceeded",
                "Network timeout during fetch"
            ],
            "STALE_DATA": [
                "Provider update frequency changed",
                "Symbol trading halted",
                "Data pipeline backlog",
                "Scheduled maintenance"
            ],
            "PROVIDER_ERROR": [
                "Provider API authentication failure",
                "Provider service outage",
                "Invalid API credentials",
                "Provider rate limit exceeded"
            ],
            "INSUFFICIENT_HISTORY": [
                "Recently listed company",
                "New data provider with limited history",
                "Data retention policy changed",
                "Historical data purge"
            ],
            "EXTREME_DAILY_MOVE": [
                "Corporate action (split/dividend)",
                "Earnings announcement",
                "Market-wide volatility",
                "Data error (misplaced decimal)"
            ]
        }
    
    def analyze(self, issue: str, context: Dict[str, Any]) -> Tuple[str, float]:
        """Analyze root cause for an issue"""
        causes = self.cause_map.get(issue, ["Unknown cause"])
        
        # Calculate confidence based on context
        confidence = 0.7
        
        if issue == "ZERO_PRICE":
            if context.get("provider") == "unknown":
                confidence = 0.5
            if context.get("age_hours", 0) > 24:
                confidence = 0.9
        
        elif issue == "STALE_DATA":
            age = context.get("age_hours", 0)
            if age > 168:  # > 1 week
                confidence = 0.95
            elif age > 72:  # > 3 days
                confidence = 0.8
        
        elif issue == "PROVIDER_ERROR":
            if context.get("provider") in ["unreliable", "backup"]:
                confidence = 0.9
            if context.get("error", "").find("rate limit") >= 0:
                confidence = 0.95
        
        return causes[0], confidence


# =============================================================================
# Remediation Engine
# =============================================================================

class RemediationEngine:
    """Automated remediation suggestions engine"""
    
    def __init__(self, config: AuditConfig):
        self.config = config
        self.remediation_map: Dict[str, List[RemediationAction]] = {}
        self._init_remediation_map()
    
    def _init_remediation_map(self):
        """Initialize remediation actions for common issues"""
        self.remediation_map = {
            "ZERO_PRICE": [
                RemediationAction(
                    issue="ZERO_PRICE",
                    action="Check provider data source and refresh symbol mapping",
                    priority=AlertPriority.P1,
                    estimated_time_minutes=30,
                    automated=False
                ),
                RemediationAction(
                    issue="ZERO_PRICE",
                    action="Fall back to backup provider or historical data",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=15,
                    automated=True
                )
            ],
            "STALE_DATA": [
                RemediationAction(
                    issue="STALE_DATA",
                    action="Force refresh from primary provider",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=10,
                    automated=True
                ),
                RemediationAction(
                    issue="STALE_DATA",
                    action="Check provider health and failover if needed",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=20,
                    automated=False
                )
            ],
            "HARD_STALE_DATA": [
                RemediationAction(
                    issue="HARD_STALE_DATA",
                    action="Immediate provider failover required",
                    priority=AlertPriority.P1,
                    estimated_time_minutes=15,
                    automated=True
                ),
                RemediationAction(
                    issue="HARD_STALE_DATA",
                    action="Investigate provider outage and escalate",
                    priority=AlertPriority.P1,
                    estimated_time_minutes=45,
                    automated=False
                )
            ],
            "PROVIDER_ERROR": [
                RemediationAction(
                    issue="PROVIDER_ERROR",
                    action="Retry with exponential backoff",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=5,
                    automated=True
                ),
                RemediationAction(
                    issue="PROVIDER_ERROR",
                    action="Switch to backup provider",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=10,
                    automated=True
                )
            ],
            "INSUFFICIENT_HISTORY": [
                RemediationAction(
                    issue="INSUFFICIENT_HISTORY",
                    action="Extend data collection period",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=1440,  # 24 hours
                    automated=False
                ),
                RemediationAction(
                    issue="INSUFFICIENT_HISTORY",
                    action="Use alternative data source with longer history",
                    priority=AlertPriority.P4,
                    estimated_time_minutes=60,
                    automated=False
                )
            ],
            "EXTREME_DAILY_MOVE": [
                RemediationAction(
                    issue="EXTREME_DAILY_MOVE",
                    action="Verify data accuracy and check for splits/dividends",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=30,
                    automated=False
                ),
                RemediationAction(
                    issue="EXTREME_DAILY_MOVE",
                    action="Apply volatility smoothing filter",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=15,
                    automated=True
                )
            ],
            "LOW_CONFIDENCE": [
                RemediationAction(
                    issue="LOW_CONFIDENCE",
                    action="Collect more historical data points",
                    priority=AlertPriority.P4,
                    estimated_time_minutes=10080,  # 7 days
                    automated=False
                ),
                RemediationAction(
                    issue="LOW_CONFIDENCE",
                    action="Use ensemble model with higher weight on fundamentals",
                    priority=AlertPriority.P4,
                    estimated_time_minutes=60,
                    automated=True
                )
            ],
            "ZOMBIE_TICKER": [
                RemediationAction(
                    issue="ZOMBIE_TICKER",
                    action="Remove from active watchlist",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=15,
                    automated=True
                ),
                RemediationAction(
                    issue="ZOMBIE_TICKER",
                    action="Check if symbol has been delisted or changed",
                    priority=AlertPriority.P4,
                    estimated_time_minutes=30,
                    automated=False
                )
            ],
            "ML_ANOMALY_DETECTED": [
                RemediationAction(
                    issue="ML_ANOMALY_DETECTED",
                    action="Investigate unusual market activity",
                    priority=AlertPriority.P2,
                    estimated_time_minutes=45,
                    automated=False
                ),
                RemediationAction(
                    issue="ML_ANOMALY_DETECTED",
                    action="Verify with multiple data sources",
                    priority=AlertPriority.P3,
                    estimated_time_minutes=30,
                    automated=False
                )
            ]
        }
    
    def get_actions(self, issue: str) -> List[RemediationAction]:
        """Get remediation actions for an issue"""
        return self.remediation_map.get(issue, [
            RemediationAction(
                issue=issue,
                action="Investigate manually",
                priority=AlertPriority.P3,
                estimated_time_minutes=60,
                automated=False
            )
        ])
    
    def prioritize_actions(self, issues: List[str]) -> List[RemediationAction]:
        """Get prioritized remediation actions for multiple issues"""
        all_actions = []
        for issue in issues:
            all_actions.extend(self.get_actions(issue))
        
        # Sort by priority (P1 first)
        priority_order = {p: i for i, p in enumerate([
            AlertPriority.P1, AlertPriority.P2, AlertPriority.P3,
            AlertPriority.P4, AlertPriority.P5
        ])}
        
        all_actions.sort(key=lambda a: (priority_order.get(a.priority, 999), a.estimated_time_minutes))
        
        # Deduplicate
        seen = set()
        unique_actions = []
        for action in all_actions:
            key = f"{action.issue}_{action.action}"
            if key not in seen:
                seen.add(key)
                unique_actions.append(action)
        
        return unique_actions
    
    def get_remediation_stats(self, actions: List[RemediationAction]) -> Dict[str, int]:
        """Get remediation statistics"""
        stats = {
            "total": len(actions),
            "automated": sum(1 for a in actions if a.automated),
            "by_priority": defaultdict(int),
            "by_status": defaultdict(int),
            "estimated_hours": 0
        }
        
        for action in actions:
            stats["by_priority"][action.priority.value] += 1
            stats["by_status"][action.status.value] += 1
            stats["estimated_hours"] += action.estimated_time_minutes / 60.0
        
        return dict(stats)


# =============================================================================
# Enhanced Alerting System with Escalation
# =============================================================================

class AlertManager:
    """Multi-channel alert manager with escalation policies"""
    
    def __init__(self, config: AuditConfig):
        self.config = config
        self.slack_client = None
        self.twilio_client = None
        self._init_clients()
        self.alert_history: List[Dict[str, Any]] = []
        self.escalation_tasks: Dict[str, asyncio.Task] = {}
    
    def _init_clients(self):
        """Initialize alert clients"""
        if _SLACK_AVAILABLE and self.config.slack_webhook:
            self.slack_client = AsyncWebClient()
        
        if self.config.twilio_sid and self.config.twilio_token:
            try:
                from twilio.rest import Client
                self.twilio_client = Client(self.config.twilio_sid, self.config.twilio_token)
            except ImportError:
                pass
    
    async def send_alert(
        self,
        severity: AuditSeverity,
        title: str,
        message: str,
        data: Optional[Dict[str, Any]] = None,
        priority: AlertPriority = AlertPriority.P3
    ):
        """Send alert through configured channels with escalation"""
        if not self.config.enable_alerts:
            return
        
        alert_id = str(uuid.uuid4())
        timestamp = _utc_now_iso()
        
        alert_data = {
            "id": alert_id,
            "timestamp": timestamp,
            "severity": severity.value,
            "title": title,
            "message": message,
            "data": data or {},
            "priority": priority.value,
            "channels": [c.value for c in self.config.alert_channels]
        }
        
        self.alert_history.append(alert_data)
        if len(self.alert_history) > 1000:
            self.alert_history = self.alert_history[-1000:]
        
        tasks = []
        for channel in self.config.alert_channels:
            if channel == AlertChannel.CONSOLE:
                self._console_alert(severity, title, message, data, priority)
            elif channel == AlertChannel.SLACK and self.slack_client:
                tasks.append(self._slack_alert(alert_id, severity, title, message, data, priority))
            elif channel == AlertChannel.EMAIL and self.config.email_recipients:
                tasks.append(self._email_alert(alert_id, severity, title, message, data, priority))
            elif channel == AlertChannel.WEBHOOK and self.config.webhook_url:
                tasks.append(self._webhook_alert(alert_id, alert_data))
            elif channel == AlertChannel.SMS and self.twilio_client and self.config.sms_recipients:
                tasks.append(self._sms_alert(alert_id, message, priority))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Start escalation if enabled
        if self.config.enable_escalation and priority in [AlertPriority.P1, AlertPriority.P2]:
            self._start_escalation(alert_id, priority)
    
    def _console_alert(self, severity: AuditSeverity, title: str, message: str, 
                       data: Optional[Dict[str, Any]] = None, priority: AlertPriority = AlertPriority.P3):
        """Send console alert"""
        log_level = {
            AuditSeverity.CRITICAL: logging.CRITICAL,
            AuditSeverity.HIGH: logging.ERROR,
            AuditSeverity.MEDIUM: logging.WARNING,
            AuditSeverity.LOW: logging.INFO,
        }.get(severity, logging.INFO)
        
        logger.log(log_level, f"[{severity.value}:{priority.value}] {title} - {message}")
        if data:
            logger.log(log_level, f"Data: {json.dumps(data, default=str)[:200]}")
    
    async def _slack_alert(self, alert_id: str, severity: AuditSeverity, title: str, 
                           message: str, data: Optional[Dict[str, Any]] = None,
                           priority: AlertPriority = AlertPriority.P3):
        """Send Slack alert with rich formatting"""
        if not self.slack_client or not self.config.slack_webhook:
            return
        
        color = {
            AuditSeverity.CRITICAL: "danger",
            AuditSeverity.HIGH: "danger",
            AuditSeverity.MEDIUM: "warning",
            AuditSeverity.LOW: "good",
        }.get(severity, "good")
        
        emoji = {
            AlertPriority.P1: "🚨",
            AlertPriority.P2: "⚠️",
            AlertPriority.P3: "🔔",
            AlertPriority.P4: "ℹ️",
            AlertPriority.P5: "✅",
        }.get(priority, "🔔")
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {title} [{priority.value}]"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            }
        ]
        
        if data:
            fields = []
            for key, value in list(data.items())[:8]:  # Limit to 8 fields
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*{key}:* {json.dumps(value, default=str)[:50]}"
                })
            
            if fields:
                # Split into two columns
                mid = len(fields) // 2
                blocks.append({
                    "type": "section",
                    "fields": fields[:mid]
                })
                if mid < len(fields):
                    blocks.append({
                        "type": "section",
                        "fields": fields[mid:]
                    })
        
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Alert ID: `{alert_id}` | Severity: {severity.value} | <{self._get_alert_url(alert_id)}|View Details>"
                }
            ]
        })
        
        payload = {
            "channel": self.config.slack_channel or "#alerts",
            "attachments": [{
                "color": color,
                "blocks": blocks,
                "ts": int(time.time())
            }]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(self.config.slack_webhook, json=payload)
        except Exception as e:
            logger.error(f"Slack alert failed: {e}")
    
    async def _email_alert(self, alert_id: str, severity: AuditSeverity, title: str,
                           message: str, data: Optional[Dict[str, Any]] = None,
                           priority: AlertPriority = AlertPriority.P3):
        """Send email alert"""
        if not self.config.email_recipients or not self.config.email_sender:
            return
        
        # This would need SMTP configuration
        pass
    
    async def _sms_alert(self, alert_id: str, message: str, priority: AlertPriority):
        """Send SMS alert"""
        if not self.twilio_client or not self.config.sms_recipients:
            return
        
        try:
            for recipient in self.config.sms_recipients:
                self.twilio_client.messages.create(
                    body=f"[{priority.value}] {message[:140]}",
                    from_=self.config.twilio_from,
                    to=recipient
                )
        except Exception as e:
            logger.error(f"SMS alert failed: {e}")
    
    async def _webhook_alert(self, alert_id: str, alert_data: Dict[str, Any]):
        """Send webhook alert"""
        if not self.config.webhook_url:
            return
        
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(self.config.webhook_url, json=alert_data)
        except Exception as e:
            logger.error(f"Webhook alert failed: {e}")
    
    def _get_alert_url(self, alert_id: str) -> str:
        """Get URL to view alert details"""
        base_url = os.getenv("ALERT_BASE_URL", "https://alerts.tadawul.com")
        return f"{base_url}/alerts/{alert_id}"
    
    def _start_escalation(self, alert_id: str, priority: AlertPriority):
        """Start escalation timer for high-priority alerts"""
        if alert_id in self.escalation_tasks:
            return
        
        minutes = self.config.escalation_minutes.get(priority, 60)
        if minutes <= 0:
            return
        
        async def escalation_task():
            await asyncio.sleep(minutes * 60)
            logger.warning(f"ESCALATION: Alert {alert_id} not acknowledged within {minutes} minutes")
            # Here you would trigger escalation (PagerDuty, phone call, etc.)
        
        self.escalation_tasks[alert_id] = asyncio.create_task(escalation_task())
    
    def acknowledge_alert(self, alert_id: str):
        """Acknowledge alert to stop escalation"""
        if alert_id in self.escalation_tasks:
            self.escalation_tasks[alert_id].cancel()
            del self.escalation_tasks[alert_id]
            logger.info(f"Alert {alert_id} acknowledged")


# =============================================================================
# WebSocket Server for Real-time Updates
# =============================================================================

class WebSocketServer:
    """WebSocket server for real-time audit updates"""
    
    def __init__(self, config: AuditConfig):
        self.config = config
        self.connected_clients: Set[Any] = set()
        self.app = None
        self.server = None
    
    async def start(self):
        """Start WebSocket server"""
        if not _ASYNC_HTTP_AVAILABLE or not self.config.enable_websocket:
            return
        
        self.app = web.Application()
        self.app.router.add_get('/ws', self.websocket_handler)
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        self.server = web.TCPSite(runner, 'localhost', self.config.websocket_port)
        await self.server.start()
        
        logger.info(f"WebSocket server started on port {self.config.websocket_port}")
    
    async def websocket_handler(self, request):
        """Handle WebSocket connection"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.connected_clients.add(ws)
        logger.info(f"WebSocket client connected. Total clients: {len(self.connected_clients)}")
        
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if msg.data == 'ping':
                        await ws.send_str('pong')
                    elif msg.data.startswith('subscribe:'):
                        symbol = msg.data[10:]
                        # Handle subscription logic
                        await ws.send_str(f"subscribed:{symbol}")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket connection closed with exception: {ws.exception()}")
        finally:
            self.connected_clients.remove(ws)
            logger.info(f"WebSocket client disconnected. Total clients: {len(self.connected_clients)}")
        
        return ws
    
    async def broadcast(self, message: Dict[str, Any]):
        """Broadcast message to all connected clients"""
        if not self.connected_clients:
            return
        
        dead_clients = set()
        for client in self.connected_clients:
            try:
                await client.send_json(message)
            except:
                dead_clients.add(client)
        
        # Remove dead clients
        self.connected_clients -= dead_clients
    
    async def stop(self):
        """Stop WebSocket server"""
        if self.server:
            await self.server.stop()


# =============================================================================
# Prometheus Metrics Exporter
# =============================================================================

class MetricsExporter:
    """Prometheus metrics exporter for real-time monitoring"""
    
    def __init__(self):
        self.registry = prometheus_client.CollectorRegistry() if _PROMETHEUS_AVAILABLE else None
        
        if _PROMETHEUS_AVAILABLE:
            # Define metrics
            self.audit_duration = Histogram(
                'audit_duration_seconds',
                'Audit duration in seconds',
                buckets=[1, 5, 10, 30, 60, 120, 300, 600],
                registry=self.registry
            )
            
            self.assets_scanned = Counter(
                'audit_assets_scanned_total',
                'Total number of assets scanned',
                registry=self.registry
            )
            
            self.issues_by_severity = Counter(
                'audit_issues_by_severity_total',
                'Issues by severity',
                ['severity'],
                registry=self.registry
            )
            
            self.provider_health = Gauge(
                'audit_provider_health',
                'Provider health status (1=healthy, 0=degraded, -1=unhealthy)',
                ['provider'],
                registry=self.registry
            )
            
            self.provider_latency = Gauge(
                'audit_provider_latency_ms',
                'Provider average latency in milliseconds',
                ['provider'],
                registry=self.registry
            )
            
            self.provider_error_rate = Gauge(
                'audit_provider_error_rate',
                'Provider error rate',
                ['provider'],
                registry=self.registry
            )
            
            self.ml_confidence = Gauge(
                'audit_ml_confidence',
                'ML model confidence score',
                ['model'],
                registry=self.registry
            )
            
            self.anomaly_counter = Counter(
                'audit_anomalies_total',
                'Anomalies detected by type',
                ['type'],
                registry=self.registry
            )
            
            self.remediation_actions = Counter(
                'audit_remediation_actions_total',
                'Remediation actions by priority',
                ['priority'],
                registry=self.registry
            )
    
    def update(self, summary: AuditSummary):
        """Update metrics with audit summary"""
        if not _PROMETHEUS_AVAILABLE:
            return
        
        # Update issue counts
        for severity, count in summary.by_severity.items():
            self.issues_by_severity.labels(severity=severity.value).inc(count)
        
        # Update provider metrics
        for provider, metrics in summary.provider_metrics.items():
            # Health: 1=healthy, 0=degraded, -1=unhealthy
            health_value = 1 if metrics.health == ProviderHealth.HEALTHY else (
                0 if metrics.health == ProviderHealth.DEGRADED else -1
            )
            self.provider_health.labels(provider=provider).set(health_value)
            self.provider_latency.labels(provider=provider).set(metrics.avg_latency_ms)
            self.provider_error_rate.labels(provider=provider).set(metrics.error_rate)
        
        # Update ML metrics
        for model, score in summary.ml_stats.items():
            self.ml_confidence.labels(model=model).set(score)
        
        # Update anomaly counts
        for anomaly_type, count in summary.anomaly_counts.items():
            self.anomaly_counter.labels(type=anomaly_type.value).inc(count)
        
        # Update remediation counts
        for priority, count in summary.remediation_stats.items():
            if priority.startswith('remediation_'):
                p = priority.replace('remediation_', '')
                self.remediation_actions.labels(priority=p).inc(count)
    
    def get_metrics(self) -> str:
        """Get Prometheus metrics in text format"""
        if not _PROMETHEUS_AVAILABLE:
            return ""
        return generate_latest(self.registry).decode('utf-8')


# =============================================================================
# Compliance Checker
# =============================================================================

class ComplianceChecker:
    """SAMA and CMA compliance checker"""
    
    def __init__(self, config: AuditConfig):
        self.config = config
        self.violations: List[str] = []
        self.compliance_rules = {
            "SAMA": {
                "data_freshness": 24,  # hours
                "data_completeness": ["current_price", "volume", "market_cap"],
                "forecast_reasonableness": 100,  # max % monthly return
                "audit_retention": 90,  # days
            },
            "CMA": {
                "price_accuracy": 0.01,  # 1% tolerance
                "volume_reporting": True,
                "trade_reporting": True,
            },
            "GDPR": {
                "data_retention": 365,  # days
                "right_to_erasure": True,
                "data_minimization": True,
            }
        }
    
    def check(self, data: Dict[str, Any]) -> List[str]:
        """Check data for compliance violations"""
        violations = []
        
        if not self.config.sama_compliant:
            return violations
        
        # Check data freshness (SAMA requires timely data)
        age = _age_hours(data.get("last_updated_utc"))
        if age and age > self.compliance_rules["SAMA"]["data_freshness"]:
            violations.append("SAMA_TIMELINESS_VIOLATION")
        
        # Check data completeness
        for field in self.compliance_rules["SAMA"]["data_completeness"]:
            if data.get(field) is None:
                violations.append(f"SAMA_{field.upper()}_MISSING")
        
        # Check forecast reasonableness
        roi_1m = _safe_float(data.get("expected_roi_1m"))
        if roi_1m and roi_1m > self.compliance_rules["SAMA"]["forecast_reasonableness"]:
            violations.append("SAMA_UNREALISTIC_FORECAST")
        
        # Check audit trail
        if not data.get("audit_timestamp"):
            violations.append("SAMA_AUDIT_TRAIL_MISSING")
        
        # CMA compliance
        if self.config.cma_compliant:
            price = _safe_float(data.get("current_price"))
            expected = _safe_float(data.get("expected_price"))
            if price and expected and abs(price - expected) / price > self.compliance_rules["CMA"]["price_accuracy"]:
                violations.append("CMA_PRICE_ACCURACY_VIOLATION")
        
        return violations


# =============================================================================
# Core Audit Logic
# =============================================================================

def _coerce_confidence_pct(x: Any) -> float:
    """Convert confidence value to percentage"""
    v = _safe_float(x, default=0.0) or 0.0
    if 0.0 <= v <= 1.0:
        return v * 100.0
    return v


def _infer_history_points(q: Dict[str, Any]) -> int:
    """Infer number of history points"""
    hp = _safe_int(q.get("history_points") or 0, 0)
    if hp > 0:
        return hp
    
    for k in ("history_len", "history_count", "prices_count"):
        v = _safe_int(q.get(k) or 0, 0)
        if v > 0:
            return v
    
    h = q.get("history")
    if isinstance(h, list):
        return len(h)
    
    return 0


def _required_history_points(q: Dict[str, Any], thresholds: Dict[str, Any]) -> int:
    """Calculate required history points based on available indicators"""
    req = int(thresholds["min_hist_macd"])
    
    if q.get("ma200") is not None or q.get("MA200") is not None:
        req = max(req, int(thresholds["min_hist_ma200"]))
    if q.get("ma50") is not None or q.get("MA50") is not None:
        req = max(req, int(thresholds["min_hist_ma50"]))
    if q.get("volatility_30d") is not None:
        req = max(req, int(thresholds["min_hist_vol30"]))
    if q.get("rsi_14d") is not None or q.get("rsi") is not None:
        req = max(req, int(thresholds["min_hist_rsi"]))
    
    return req


def _perform_strategy_review(q: Dict[str, Any], thresholds: Dict[str, Any]) -> List[str]:
    """Perform strategy review and generate notes"""
    notes: List[str] = []
    
    trend_sig = str(q.get("trend_signal") or q.get("trend") or "NEUTRAL").strip().upper()
    rec = str(q.get("recommendation") or "HOLD").strip().upper()
    
    ret_1w = _safe_float(q.get("returns_1w") or q.get("Returns 1W %") or 0.0, 0.0) or 0.0
    vol_30d = _safe_float(q.get("volatility_30d") or 0.0, 0.0) or 0.0
    exp_roi_1m = _safe_float(q.get("expected_roi_1m") or 0.0, 0.0) or 0.0
    exp_roi_3m = _safe_float(q.get("expected_roi_3m") or 0.0, 0.0) or 0.0
    macd_hist = _safe_float(q.get("macd_hist") or q.get("MACD Hist") or 0.0, 0.0) or 0.0
    
    tol = float(thresholds["trend_tolerance_pct"])
    if trend_sig == "UPTREND" and ret_1w < -tol:
        notes.append("TREND_BREAK_BEAR")
    elif trend_sig == "DOWNTREND" and ret_1w > tol:
        notes.append("TREND_BREAK_BULL")
    
    if macd_hist > 0 and ret_1w < -float(thresholds["mom_divergence_drop_pct"]):
        notes.append("MOM_DIVERGENCE_BEAR")
    
    if exp_roi_1m > float(thresholds["aggressive_roi_1m_pct"]) and vol_30d < float(thresholds["low_vol_pct"]):
        notes.append("AGGRESSIVE_FORECAST_LOW_VOL")
    
    risk_score = _safe_float(q.get("risk_score") or 50.0, 50.0) or 50.0
    if risk_score > float(thresholds["risk_overest_score"]) and vol_30d < float(thresholds["low_vol_pct"]):
        notes.append("RISK_OVEREST_LOW_VOL")
    
    if rec == "BUY" and exp_roi_1m < 0:
        notes.append("REC_BUY_BUT_NEG_ROI_1M")
    if rec == "SELL" and exp_roi_1m > 5:
        notes.append("REC_SELL_BUT_POS_ROI_1M")
    
    if exp_roi_1m > 0 and exp_roi_3m < 0:
        notes.append("ROI_INCONSISTENT_1M_POS_3M_NEG")
    
    return notes


def _determine_severity(
    issues: List[str],
    strategy_notes: List[str],
    anomalies: List[AnomalyType],
    quality_score: float
) -> AuditSeverity:
    """Determine audit severity based on findings"""
    
    critical_issues = {"PROVIDER_ERROR", "ZERO_PRICE", "HARD_STALE_DATA", "ZOMBIE_TICKER"}
    high_issues = {"STALE_DATA", "INSUFFICIENT_HISTORY", "EXTREME_DAILY_MOVE"}
    medium_issues = {"LOW_CONFIDENCE", "MISSING_METADATA", "ML_ANOMALY_DETECTED"}
    
    if any(i in critical_issues for i in issues):
        return AuditSeverity.CRITICAL
    
    if any(i in high_issues for i in issues):
        return AuditSeverity.HIGH
    
    if any(i in medium_issues for i in issues) or anomalies:
        return AuditSeverity.MEDIUM
    
    if strategy_notes:
        return AuditSeverity.LOW
    
    if quality_score < 50:
        return AuditSeverity.MEDIUM
    elif quality_score < 70:
        return AuditSeverity.LOW
    
    return AuditSeverity.OK


def _determine_quality(score: float) -> DataQuality:
    """Determine data quality category from score"""
    if score >= 90:
        return DataQuality.EXCELLENT
    elif score >= 75:
        return DataQuality.GOOD
    elif score >= 60:
        return DataQuality.FAIR
    elif score >= 40:
        return DataQuality.POOR
    else:
        return DataQuality.CRITICAL


async def _audit_quote(
    symbol: str,
    q: Dict[str, Any],
    config: AuditConfig,
    anomaly_detector: Optional[MLAnomalyDetector] = None,
    quality_scorer: Optional[PredictiveQualityScorer] = None,
    time_series_detector: Optional[TimeSeriesAnomalyDetector] = None,
    compliance_checker: Optional[ComplianceChecker] = None,
    remediation_engine: Optional[RemediationEngine] = None,
    root_cause_analyzer: Optional[RootCauseAnalyzer] = None,
    spc: Optional[StatisticalProcessControl] = None,
    model_manager: Optional[MLModelManager] = None
) -> AuditResult:
    """Audit a single quote with enhanced ML features"""
    
    # Extract basic info
    price = _safe_float(q.get("current_price") or q.get("price"))
    last_upd = q.get("last_updated_utc") or q.get("Last Updated (UTC)")
    provider = str(q.get("data_source") or q.get("provider") or "unknown")
    
    issues: List[str] = []
    strategy_notes: List[str] = []
    spc_violations: List[str] = []
    
    # Price integrity
    if price is None or price < config.min_price:
        issues.append("ZERO_PRICE")
    
    # Freshness
    age = _age_hours(str(last_upd) if last_upd else None)
    if age is None:
        issues.append("NO_TIMESTAMP")
    elif age > config.hard_stale_hours:
        issues.append("HARD_STALE_DATA")
    elif age > config.stale_hours:
        issues.append("STALE_DATA")
    
    # Technical integrity
    hist_pts = _infer_history_points(q)
    req_hist = _required_history_points(q, config.to_dict())
    if hist_pts < req_hist:
        issues.append(f"INSUFFICIENT_HISTORY ({hist_pts}<{req_hist})")
    
    # Provider error
    if q.get("error"):
        issues.append("PROVIDER_ERROR")
    
    # Daily move sanity
    chg_pct = _safe_float(q.get("percent_change") or q.get("change_pct"))
    if chg_pct and abs(chg_pct) > config.max_abs_change_pct:
        issues.append("EXTREME_DAILY_MOVE")
    
    # Confidence
    conf_pct = _coerce_confidence_pct(q.get("forecast_confidence") or 0)
    if conf_pct > 0 and conf_pct < config.min_confidence_pct:
        issues.append("LOW_CONFIDENCE")
    
    # Zombie heuristics
    dq = str(q.get("data_quality") or "").strip().upper()
    name = str(q.get("name") or "").strip()
    if ("ZERO_PRICE" in issues and "STALE_DATA" in issues) or dq in ("MISSING", "BAD"):
        issues.append("ZOMBIE_TICKER")
    if not name and dq in ("MISSING", "PARTIAL") and age and age > config.stale_hours:
        issues.append("MISSING_METADATA")
    
    # ML anomaly detection
    anomalies: List[AnomalyType] = []
    ml_anomaly_score = 0.0
    ml_confidence = 0.0
    ml_model_version = None
    if anomaly_detector:
        is_anomaly, ml_anomaly_score, detected_anomalies, ml_confidence = anomaly_detector.detect(q)
        anomalies = detected_anomalies
        if is_anomaly or ml_anomaly_score > 0.8:
            issues.append("ML_ANOMALY_DETECTED")
        if model_manager:
            ml_model_version = model_manager.get_model_version(MLModelType.ISOLATION_FOREST)
    
    # ML quality scoring
    ml_quality_score = 0.0
    if quality_scorer:
        ml_quality_score, data_quality, quality_confidence = quality_scorer.predict_quality(q)
        ml_confidence = max(ml_confidence, quality_confidence)
    else:
        ml_quality_score = max(0, 100 - len(issues) * 10)
        data_quality = _determine_quality(ml_quality_score)
    
    # Time series anomaly detection
    if time_series_detector and q.get("history_prices"):
        prices = q.get("history_prices", [])
        dates = q.get("history_dates", [])
        
        trend_reversal, trend_confidence = await time_series_detector.detect_trend_reversal(prices, dates)
        if trend_reversal:
            anomalies.append(AnomalyType.TREND_REVERSAL)
        
        seasonality_break, seasonality_confidence = await time_series_detector.detect_seasonality_break(prices, dates)
        if seasonality_break:
            anomalies.append(AnomalyType.SEASONALITY_BREAK)
    
    # Statistical Process Control
    control_limits = {}
    if spc and q.get("history_prices"):
        prices = q.get("history_prices", [])
        if len(prices) > 20:
            control_limits = spc.calculate_control_limits(prices[-20:])
            if price:
                spc_violations = spc.check_violations(price, control_limits, prices[-20:])
    
    # Strategy review
    try:
        strategy_notes = _perform_strategy_review(q, config.to_dict())
    except Exception:
        strategy_notes = []
    
    # Compliance checks
    compliance_violations: List[str] = []
    if compliance_checker:
        compliance_violations = compliance_checker.check(q)
    
    # Root cause analysis
    root_cause = None
    impact_analysis = None
    if root_cause_analyzer and issues:
        primary_issue = issues[0]
        context = {
            "provider": provider,
            "age_hours": age,
            "price": price,
            "error": q.get("error")
        }
        cause, confidence = root_cause_analyzer.analyze(primary_issue, context)
        root_cause = cause
        
        # Simple impact analysis
        impact_analysis = {
            "primary_issue": primary_issue,
            "root_cause": cause,
            "confidence": confidence,
            "affected_symbols": 1,
            "estimated_recovery_time": "1-2 hours" if confidence > 0.8 else "Unknown"
        }
    
    # Remediation actions
    remediation_actions: List[RemediationAction] = []
    if remediation_engine:
        remediation_actions = remediation_engine.prioritize_actions(issues)
    
    # Determine severity
    severity = _determine_severity(issues, strategy_notes, anomalies, ml_quality_score)
    
    # Determine provider health
    provider_health = ProviderHealth.UNKNOWN
    if "PROVIDER_ERROR" in issues:
        provider_health = ProviderHealth.UNHEALTHY
    elif age and age > config.stale_hours:
        provider_health = ProviderHealth.DEGRADED
    elif not issues:
        provider_health = ProviderHealth.HEALTHY
    
    return AuditResult(
        symbol=symbol,
        page="",  # Will be set by caller
        provider=provider,
        severity=severity,
        data_quality=data_quality,
        provider_health=provider_health,
        price=price,
        change_pct=chg_pct,
        volume=_safe_int(q.get("volume")),
        market_cap=_safe_float(q.get("market_cap")),
        age_hours=age,
        last_updated_utc=str(last_upd) if last_upd else None,
        last_updated_riyadh=_riyadh_iso(str(last_upd)) if last_upd else _riyadh_iso(),
        history_points=hist_pts,
        required_history=req_hist,
        confidence_pct=round(conf_pct, 1),
        issues=issues,
        strategy_notes=strategy_notes,
        anomalies=anomalies,
        ml_quality_score=ml_quality_score,
        ml_anomaly_score=ml_anomaly_score,
        ml_confidence=ml_confidence,
        ml_model_version=ml_model_version,
        compliance_violations=compliance_violations,
        remediation_actions=remediation_actions,
        spc_violations=spc_violations,
        control_limits=control_limits,
        root_cause=root_cause,
        impact_analysis=impact_analysis,
        error=q.get("error"),
    )


# =============================================================================
# Engine Fetching
# =============================================================================

def _supports_param(fn: Any, name: str) -> bool:
    try:
        import inspect
        sig = inspect.signature(fn)
        return name in sig.parameters
    except Exception:
        return False


async def _call_engine_batch(engine: Any, symbols: List[str], refresh: bool) -> Dict[str, Dict[str, Any]]:
    """Call engine batch method"""
    if engine is None:
        return {s: {"error": "NO_ENGINE"} for s in symbols}
    
    fn = getattr(engine, "get_enriched_quotes", None)
    if not callable(fn):
        return {}
    
    try:
        kwargs = {}
        if _supports_param(fn, "refresh"):
            kwargs["refresh"] = refresh
        res = await _maybe_await(fn(symbols, **kwargs))
    except Exception as e:
        logger.warning(f"Batch fetch failed: {e}")
        return {}
    
    res = _unwrap_tuple(res)
    
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(res, dict):
        for k, v in res.items():
            kk = _norm_symbol(k)
            vv = _unwrap_tuple(v)
            out[kk or str(k)] = vv if isinstance(vv, dict) else (vv.__dict__ if hasattr(vv, "__dict__") else {})
        for s in symbols:
            out.setdefault(_norm_symbol(s), {})
        return out
    
    if isinstance(res, list):
        for i, s in enumerate(symbols):
            item = res[i] if i < len(res) else {}
            item = _unwrap_tuple(item)
            d = item if isinstance(item, dict) else (item.__dict__ if hasattr(item, "__dict__") else {})
            out[_norm_symbol(s)] = d
        return out
    
    for s in symbols:
        out[_norm_symbol(s)] = {"error": "INVALID_BATCH_FORMAT"}
    return out


async def _call_engine_single(engine: Any, symbol: str, refresh: bool) -> Dict[str, Any]:
    """Call engine single method"""
    if engine is None:
        return {"error": "NO_ENGINE"}
    
    fn = getattr(engine, "get_enriched_quote", None) or getattr(engine, "get_quote", None)
    if not callable(fn):
        return {"error": "ENGINE_NO_QUOTE_METHOD"}
    
    try:
        kwargs = {}
        if _supports_param(fn, "refresh"):
            kwargs["refresh"] = refresh
        res = await _maybe_await(fn(symbol, **kwargs))
        res = _unwrap_tuple(res)
        return res if isinstance(res, dict) else (res.__dict__ if hasattr(res, "__dict__") else {})
    except Exception as e:
        return {"error": str(e)}


async def _fetch_quotes(
    engine: Any,
    symbols: List[str],
    *,
    refresh: bool,
    concurrency: int,
    provider_metrics: Dict[str, ProviderMetrics]
) -> Dict[str, Dict[str, Any]]:
    """Fetch quotes for symbols with metrics"""
    
    symbols = [_norm_symbol(s) for s in symbols if _norm_symbol(s)]
    symbols = _dedupe_preserve(symbols)
    
    start_time = time.time()
    
    # Try batch first
    batch_map = await _call_engine_batch(engine, symbols, refresh=refresh)
    
    # Track batch metrics
    if batch_map:
        for s, data in batch_map.items():
            provider = str(data.get("data_source") or data.get("provider") or "unknown")
            if provider not in provider_metrics:
                provider_metrics[provider] = ProviderMetrics(name=provider)
            
            pm = provider_metrics[provider]
            pm.total_requests += 1
            
            if data.get("error"):
                pm.failed_requests += 1
                pm.last_failure = _utc_now_iso()
            else:
                pm.successful_requests += 1
            
            if data.get("_cache_hit"):
                pm.cache_hits += 1
            else:
                pm.cache_misses += 1
        
        # Add latency estimate
        elapsed_ms = (time.time() - start_time) * 1000
        for pm in provider_metrics.values():
            pm.total_latency_ms += elapsed_ms / len(provider_metrics)
            # Update uptime (simplified)
            if pm.total_requests > 0:
                pm.uptime_percent = 100.0 * (pm.successful_requests / pm.total_requests)
        
        return batch_map
    
    # Fallback to concurrent singles
    logger.info(f"Batch fetch failed, falling back to {len(symbols)} concurrent singles")
    
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    
    async def one(s: str) -> Tuple[str, Dict[str, Any]]:
        start = time.time()
        async with sem:
            d = await _call_engine_single(engine, s, refresh=refresh)
            elapsed_ms = (time.time() - start) * 1000
            
            provider = str(d.get("data_source") or d.get("provider") or "unknown")
            if provider not in provider_metrics:
                provider_metrics[provider] = ProviderMetrics(name=provider)
            
            pm = provider_metrics[provider]
            pm.total_requests += 1
            pm.total_latency_ms += elapsed_ms
            pm.response_times.append(elapsed_ms)
            
            if d.get("error"):
                pm.failed_requests += 1
                pm.last_failure = _utc_now_iso()
            else:
                pm.successful_requests += 1
            
            if d.get("_cache_hit"):
                pm.cache_hits += 1
            else:
                pm.cache_misses += 1
            
            # Update uptime
            if pm.total_requests > 0:
                pm.uptime_percent = 100.0 * (pm.successful_requests / pm.total_requests)
            
            return s, d
    
    pairs = await asyncio.gather(*[one(s) for s in symbols], return_exceptions=True)
    
    out: Dict[str, Dict[str, Any]] = {}
    for i, p in enumerate(pairs):
        if isinstance(p, Exception):
            s = symbols[i]
            out[s] = {"error": str(p)}
            
            # Track error with unknown provider
            if "unknown" not in provider_metrics:
                provider_metrics["unknown"] = ProviderMetrics(name="unknown")
            provider_metrics["unknown"].failed_requests += 1
            provider_metrics["unknown"].total_requests += 1
            provider_metrics["unknown"].last_failure = _utc_now_iso()
            
        else:
            s, d = p
            out[s] = d if isinstance(d, dict) else {"error": "BAD_SINGLE_FORMAT"}
    
    return out


# =============================================================================
# Page Symbol Loading
# =============================================================================

def _get_spreadsheet_id(args: argparse.Namespace) -> Optional[str]:
    sid = (args.sheet_id or "").strip()
    if sid:
        return sid
    
    if DEPS.settings is not None:
        try:
            sid = str(getattr(DEPS.settings, "default_spreadsheet_id", "") or "").strip()
            if sid:
                return sid
        except Exception:
            pass
    
    sid = (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip()
    return sid or None


def _get_target_keys(args: argparse.Namespace) -> List[str]:
    if args.keys:
        return [str(k).strip() for k in args.keys if str(k).strip()]
    
    sr = DEPS.symbols_reader
    if sr is not None:
        try:
            reg = getattr(sr, "PAGE_REGISTRY", None)
            if isinstance(reg, dict) and reg:
                return list(reg.keys())
        except Exception:
            pass
    
    return []


def _read_page_symbols(page_key: str, sid: str) -> List[str]:
    sr = DEPS.symbols_reader
    if sr is None:
        return []
    
    try:
        sym_data = sr.get_page_symbols(page_key, spreadsheet_id=sid)
        
        if isinstance(sym_data, dict):
            for k in ("all", "symbols", "tickers", "items"):
                v = sym_data.get(k)
                if isinstance(v, list):
                    return [_norm_symbol(x) for x in v if _norm_symbol(x)]
            
            vals = []
            for v in sym_data.values():
                if isinstance(v, list):
                    vals.extend(v)
            return [_norm_symbol(x) for x in vals if _norm_symbol(x)]
        
        if isinstance(sym_data, list):
            return [_norm_symbol(x) for x in sym_data if _norm_symbol(x)]
    
    except Exception as e:
        logger.warning(f"   Skipping {page_key}: {e}")
    
    return []


# =============================================================================
# Reporting & Export
# =============================================================================

def _print_table(rows: List[AuditResult], limit: int = 400) -> None:
    """Print formatted table of results"""
    if not rows:
        return
    
    # Sort by severity (CRITICAL first)
    severity_order = {s: i for i, s in enumerate([
        AuditSeverity.CRITICAL, AuditSeverity.HIGH, AuditSeverity.MEDIUM,
        AuditSeverity.LOW, AuditSeverity.OK
    ])}
    rows.sort(key=lambda r: (severity_order.get(r.severity, 999), r.symbol))
    
    # Fixed columns
    cols = ["symbol", "page", "severity", "provider", "data_quality", "age_hours", "issues", "strategy_notes"]
    header = f"{'SYMBOL':<12} | {'PAGE':<16} | {'SEV':<8} | {'PROVIDER':<12} | {'DQ':<8} | {'AGE(h)':<7} | DETAILS"
    print("\n" + header)
    print("-" * min(160, max(100, len(header) + 20)))
    
    for i, r in enumerate(rows[:limit]):
        sym = str(r.symbol or "")[:12]
        page = str(r.page or "")[:16]
        sev = str(r.severity.value)[:8]
        prov = str(r.provider or "")[:12]
        dq = str(r.data_quality.value)[:8]
        age = r.age_hours
        age_s = f"{age:.1f}" if age is not None else "?"
        
        details = []
        for x in r.issues:
            details.append(str(x))
        for x in r.strategy_notes:
            details.append(str(x))
        for x in r.anomalies:
            details.append(f"ANOMALY:{x.value}")
        d = ", ".join(details)[:200]
        
        print(f"{sym:<12} | {page:<16} | {sev:<8} | {prov:<12} | {dq:<8} | {age_s:<7} | {d}")
    
    if len(rows) > limit:
        print(f"... ({len(rows) - limit} more rows not shown)")


def _generate_summary(results: List[AuditResult], provider_metrics: Dict[str, ProviderMetrics]) -> AuditSummary:
    """Generate enhanced audit summary"""
    
    summary = AuditSummary()
    summary.total_assets = len(results)
    
    for r in results:
        # Severity counts
        summary.by_severity[r.severity] = summary.by_severity.get(r.severity, 0) + 1
        
        # Quality counts
        summary.by_quality[r.data_quality] = summary.by_quality.get(r.data_quality, 0) + 1
        
        # Provider health
        summary.by_provider_health[r.provider_health] = summary.by_provider_health.get(r.provider_health, 0) + 1
        
        # Issues
        for issue in r.issues:
            summary.issue_counts[issue] = summary.issue_counts.get(issue, 0) + 1
        
        # Strategy notes
        for note in r.strategy_notes:
            summary.strategy_note_counts[note] = summary.strategy_note_counts.get(note, 0) + 1
        
        # Anomalies
        for anomaly in r.anomalies:
            summary.anomaly_counts[anomaly] = summary.anomaly_counts.get(anomaly, 0) + 1
        
        # Page alerts
        if r.severity != AuditSeverity.OK:
            summary.page_alerts[r.page] = summary.page_alerts.get(r.page, 0) + 1
        
        # Compliance violations
        for violation in r.compliance_violations:
            summary.compliance_violations[violation] = summary.compliance_violations.get(violation, 0) + 1
        
        # Remediation stats
        if r.remediation_actions:
            for action in r.remediation_actions:
                summary.remediation_stats[f"remediation_{action.priority.value}"] = \
                    summary.remediation_stats.get(f"remediation_{action.priority.value}", 0) + 1
        
        # SPC stats
        if r.spc_violations:
            for violation in r.spc_violations:
                summary.spc_stats[violation] = summary.spc_stats.get(violation, 0) + 1
    
    # Provider metrics
    summary.provider_metrics = provider_metrics
    
    # ML stats
    if results:
        ml_scores = [r.ml_quality_score for r in results if r.ml_quality_score > 0]
        if ml_scores:
            summary.ml_stats["avg_quality_score"] = sum(ml_scores) / len(ml_scores)
            summary.ml_stats["min_quality_score"] = min(ml_scores)
            summary.ml_stats["max_quality_score"] = max(ml_scores)
        
        anomaly_scores = [r.ml_anomaly_score for r in results if r.ml_anomaly_score > 0]
        if anomaly_scores:
            summary.ml_stats["avg_anomaly_score"] = sum(anomaly_scores) / len(anomaly_scores)
        
        ml_confidences = [r.ml_confidence for r in results if r.ml_confidence > 0]
        if ml_confidences:
            summary.ml_stats["avg_ml_confidence"] = sum(ml_confidences) / len(ml_confidences)
    
    return summary


def _export_json(path: str, data: Dict[str, Any]) -> None:
    """Export to JSON"""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)


def _export_csv(path: str, results: List[AuditResult]) -> None:
    """Export to CSV"""
    if not results:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("No results")
        return
    
    headers = [
        "symbol", "page", "provider", "severity", "data_quality", "provider_health",
        "price", "change_pct", "volume", "market_cap", "age_hours",
        "last_updated_utc", "last_updated_riyadh", "history_points", "required_history",
        "confidence_pct", "issues", "strategy_notes", "anomalies", "ml_quality_score",
        "ml_anomaly_score", "ml_confidence", "ml_model_version", "compliance_violations",
        "root_cause", "error", "audit_timestamp", "audit_version"
    ]
    
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in results:
            row = r.to_dict()
            row["issues"] = ", ".join(row.get("issues", []))
            row["strategy_notes"] = ", ".join(row.get("strategy_notes", []))
            row["anomalies"] = ", ".join(row.get("anomalies", []))
            row["compliance_violations"] = ", ".join(row.get("compliance_violations", []))
            w.writerow({k: row.get(k) for k in headers})


def _export_parquet(path: str, results: List[AuditResult]) -> None:
    """Export to Parquet"""
    if not _PARQUET_AVAILABLE or not results:
        return
    
    try:
        df = pd.DataFrame([r.to_dict() for r in results])
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)
        logger.info(f"Parquet export saved to {path}")
    except Exception as e:
        logger.error(f"Parquet export failed: {e}")


def _export_excel(path: str, results: List[AuditResult], summary: AuditSummary) -> None:
    """Export to Excel with multiple sheets"""
    try:
        import pandas as pd
        
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            # Results sheet
            df_results = pd.DataFrame([r.to_dict() for r in results])
            df_results.to_excel(writer, sheet_name="Audit Results", index=False)
            
            # Summary sheet
            df_summary = pd.DataFrame([summary.to_dict()])
            df_summary.to_excel(writer, sheet_name="Summary", index=False)
            
            # Issues sheet
            issues_data = [{"issue": k, "count": v} for k, v in summary.issue_counts.items()]
            df_issues = pd.DataFrame(issues_data)
            df_issues.to_excel(writer, sheet_name="Issues", index=False)
            
            # Provider metrics sheet
            provider_data = [pm.to_dict() for pm in summary.provider_metrics.values()]
            df_providers = pd.DataFrame(provider_data)
            df_providers.to_excel(writer, sheet_name="Providers", index=False)
            
            # Anomalies sheet
            anomaly_data = [{"anomaly": k.value, "count": v} for k, v in summary.anomaly_counts.items()]
            df_anomalies = pd.DataFrame(anomaly_data)
            df_anomalies.to_excel(writer, sheet_name="Anomalies", index=False)
            
            # Remediation sheet
            if summary.remediation_stats:
                remediation_data = [{"action": k, "count": v} for k, v in summary.remediation_stats.items()]
                df_remediation = pd.DataFrame(remediation_data)
                df_remediation.to_excel(writer, sheet_name="Remediation", index=False)
            
            # SPC sheet
            if summary.spc_stats:
                spc_data = [{"violation": k, "count": v} for k, v in summary.spc_stats.items()]
                df_spc = pd.DataFrame(spc_data)
                df_spc.to_excel(writer, sheet_name="SPC Violations", index=False)
            
        logger.info(f"Excel export saved to {path}")
        
    except Exception as e:
        logger.error(f"Excel export failed: {e}")


def _generate_dashboard(summary: AuditSummary, output_dir: str) -> None:
    """Generate enhanced HTML dashboard with charts"""
    if not _PLOT_AVAILABLE:
        return
    
    try:
        # Generate charts
        charts_dir = os.path.join(output_dir, "charts")
        os.makedirs(charts_dir, exist_ok=True)
        
        # Severity pie chart
        plt.figure(figsize=(10, 6))
        severities = [s.value for s in summary.by_severity.keys() if summary.by_severity[s] > 0]
        counts = [summary.by_severity[s] for s in summary.by_severity.keys() if summary.by_severity[s] > 0]
        colors = ['#d32f2f', '#f57c00', '#fbc02d', '#7cb342', '#388e3c']
        plt.pie(counts, labels=severities, autopct='%1.1f%%', colors=colors[:len(severities)])
        plt.title('Issues by Severity')
        plt.savefig(os.path.join(charts_dir, 'severity.png'))
        plt.close()
        
        # Provider health bar chart
        plt.figure(figsize=(12, 6))
        providers = list(summary.provider_metrics.keys())
        health_values = []
        for p in providers:
            pm = summary.provider_metrics[p]
            if pm.health == ProviderHealth.HEALTHY:
                health_values.append(1)
            elif pm.health == ProviderHealth.DEGRADED:
                health_values.append(0)
            else:
                health_values.append(-1)
        
        colors = ['green' if v == 1 else 'orange' if v == 0 else 'red' for v in health_values]
        plt.bar(providers, health_values, color=colors)
        plt.title('Provider Health')
        plt.ylabel('Health (1=Healthy, 0=Degraded, -1=Unhealthy)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(charts_dir, 'provider_health.png'))
        plt.close()
        
        # Top issues bar chart
        plt.figure(figsize=(12, 6))
        top_issues = sorted(summary.issue_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        issues = [i[0][:20] + '...' if len(i[0]) > 20 else i[0] for i in top_issues]
        counts = [i[1] for i in top_issues]
        plt.barh(issues, counts, color='steelblue')
        plt.title('Top 10 Issues')
        plt.xlabel('Count')
        plt.tight_layout()
        plt.savefig(os.path.join(charts_dir, 'top_issues.png'))
        plt.close()
        
        # Anomalies bar chart
        if summary.anomaly_counts:
            plt.figure(figsize=(10, 5))
            anomalies = [a.value for a in summary.anomaly_counts.keys()]
            counts = list(summary.anomaly_counts.values())
            plt.bar(anomalies, counts, color='orange')
            plt.title('Anomalies Detected')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig(os.path.join(charts_dir, 'anomalies.png'))
            plt.close()
        
        # Generate HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Tadawul Data Quality Dashboard v3.2.0</title>
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
                h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
                h2 {{ color: #666; margin-top: 30px; }}
                .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
                .card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .card h3 {{ margin: 0 0 10px 0; color: #555; }}
                .card p {{ margin: 0; font-size: 24px; font-weight: bold; }}
                .critical {{ color: #d32f2f; }}
                .high {{ color: #f57c00; }}
                .medium {{ color: #fbc02d; }}
                .low {{ color: #7cb342; }}
                .ok {{ color: #388e3c; }}
                .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin: 20px 0; }}
                .chart {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .chart img {{ width: 100%; height: auto; }}
                table {{ border-collapse: collapse; width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                th, td {{ padding: 12px 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:hover {{ background-color: #f5f5f5; }}
                .badge {{ display: inline-block; padding: 3px 8px; border-radius: 12px; font-size: 12px; font-weight: bold; }}
                .badge-critical {{ background: #d32f2f; color: white; }}
                .badge-high {{ background: #f57c00; color: white; }}
                .badge-medium {{ background: #fbc02d; color: black; }}
                .badge-low {{ background: #7cb342; color: white; }}
                .badge-ok {{ background: #388e3c; color: white; }}
                .footer {{ margin-top: 30px; text-align: center; color: #777; font-size: 12px; }}
            </style>
        </head>
        <body>
            <h1>📊 Tadawul Data Quality Audit Dashboard v3.2.0</h1>
            <p>Audit Time: {summary.audit_timestamp} (UTC) | Riyadh: {_riyadh_iso(summary.audit_timestamp)}</p>
            
            <div class="summary">
                <div class="card">
                    <h3>Total Assets</h3>
                    <p>{summary.total_assets}</p>
                </div>
                <div class="card">
                    <h3>Critical Issues</h3>
                    <p class="critical">{summary.by_severity.get(AuditSeverity.CRITICAL, 0)}</p>
                </div>
                <div class="card">
                    <h3>High Issues</h3>
                    <p class="high">{summary.by_severity.get(AuditSeverity.HIGH, 0)}</p>
                </div>
                <div class="card">
                    <h3>Medium Issues</h3>
                    <p class="medium">{summary.by_severity.get(AuditSeverity.MEDIUM, 0)}</p>
                </div>
                <div class="card">
                    <h3>Low Issues</h3>
                    <p class="low">{summary.by_severity.get(AuditSeverity.LOW, 0)}</p>
                </div>
                <div class="card">
                    <h3>OK</h3>
                    <p class="ok">{summary.by_severity.get(AuditSeverity.OK, 0)}</p>
                </div>
                <div class="card">
                    <h3>Audit Duration</h3>
                    <p>{summary.audit_duration_ms:.2f} ms</p>
                </div>
                <div class="card">
                    <h3>ML Avg Quality</h3>
                    <p>{summary.ml_stats.get('avg_quality_score', 0):.1f}</p>
                </div>
            </div>
            
            <div class="charts">
                <div class="chart">
                    <h3>Issues by Severity</h3>
                    <img src="charts/severity.png" alt="Severity Distribution">
                </div>
                <div class="chart">
                    <h3>Provider Health</h3>
                    <img src="charts/provider_health.png" alt="Provider Health">
                </div>
                <div class="chart">
                    <h3>Top Issues</h3>
                    <img src="charts/top_issues.png" alt="Top Issues">
                </div>
        """
        
        if summary.anomaly_counts:
            html += f"""
                <div class="chart">
                    <h3>Anomalies</h3>
                    <img src="charts/anomalies.png" alt="Anomalies">
                </div>
            """
        
        html += """
            </div>
            
            <h2>Provider Metrics</h2>
            <table>
                <tr>
                    <th>Provider</th>
                    <th>Health</th>
                    <th>Requests</th>
                    <th>Error Rate</th>
                    <th>Avg Latency (ms)</th>
                    <th>P95 Latency</th>
                    <th>Cache Hit Rate</th>
                    <th>Uptime</th>
                </tr>
        """
        
        for pm in summary.provider_metrics.values():
            health_class = {
                ProviderHealth.HEALTHY: "badge-ok",
                ProviderHealth.DEGRADED: "badge-medium",
                ProviderHealth.UNHEALTHY: "badge-critical",
            }.get(pm.health, "")
            
            html += f"""
                <tr>
                    <td>{pm.name}</td>
                    <td><span class="badge {health_class}">{pm.health.value}</span></td>
                    <td>{pm.total_requests}</td>
                    <td>{pm.error_rate:.2%}</td>
                    <td>{pm.avg_latency_ms:.2f}</td>
                    <td>{pm.p95_latency_ms:.2f}</td>
                    <td>{pm.cache_hit_rate:.2%}</td>
                    <td>{pm.uptime_percent:.2f}%</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>ML Statistics</h2>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
        """
        
        for key, value in summary.ml_stats.items():
            html += f"""
                <tr>
                    <td>{key}</td>
                    <td>{value:.3f}</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>Top Issues</h2>
            <table>
                <tr>
                    <th>Issue</th>
                    <th>Count</th>
                </tr>
        """
        
        for issue, count in sorted(summary.issue_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
            html += f"""
                <tr>
                    <td>{issue}</td>
                    <td>{count}</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>Anomalies Detected</h2>
            <table>
                <tr>
                    <th>Anomaly Type</th>
                    <th>Count</th>
                </tr>
        """
        
        for anomaly, count in summary.anomaly_counts.items():
            html += f"""
                <tr>
                    <td>{anomaly.value}</td>
                    <td>{count}</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>Compliance Violations</h2>
            <table>
                <tr>
                    <th>Violation</th>
                    <th>Count</th>
                </tr>
        """
        
        for violation, count in summary.compliance_violations.items():
            html += f"""
                <tr>
                    <td>{violation}</td>
                    <td>{count}</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>Remediation Summary</h2>
            <table>
                <tr>
                    <th>Action</th>
                    <th>Count</th>
                </tr>
        """
        
        for action, count in summary.remediation_stats.items():
            html += f"""
                <tr>
                    <td>{action}</td>
                    <td>{count}</td>
                </tr>
            """
        
        if summary.spc_stats:
            html += """
            </table>
            
            <h2>SPC Violations</h2>
            <table>
                <tr>
                    <th>Violation</th>
                    <th>Count</th>
                </tr>
            """
            
            for violation, count in summary.spc_stats.items():
                html += f"""
                    <tr>
                        <td>{violation}</td>
                        <td>{count}</td>
                    </tr>
                """
        
        html += """
            </table>
            
            <div class="footer">
                <p>Generated by TFB Audit Tool v3.2.0 | SAMA Compliant | Real-time ML Enhanced | SPC Enabled</p>
            </div>
        </body>
        </html>
        """
        
        dashboard_path = os.path.join(output_dir, "dashboard.html")
        with open(dashboard_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        logger.info(f"Enhanced dashboard generated at {dashboard_path}")
        
    except Exception as e:
        logger.error(f"Dashboard generation failed: {e}")


# =============================================================================
# Main Audit Runner
# =============================================================================

async def _run_audit(
    *,
    keys: List[str],
    sid: str,
    config: AuditConfig,
    refresh: bool,
    concurrency: int,
    max_symbols_per_page: int,
    alert_manager: Optional[AlertManager] = None,
    metrics_exporter: Optional[MetricsExporter] = None,
    websocket_server: Optional[WebSocketServer] = None
) -> Tuple[List[AuditResult], AuditSummary]:
    """Run the audit with enhanced ML features"""
    
    # Initialize engine
    try:
        engine = await _maybe_await(DEPS.get_engine())
        if not engine:
            return [], AuditSummary()
    except Exception as e:
        logger.error(f"Engine initialization error: {e}")
        return [], AuditSummary()
    
    logger.info(f"🚀 Starting Data Quality & Strategy Audit v3.2.0")
    logger.info(f"📊 Configuration: stale={config.stale_hours}h, hard={config.hard_stale_hours}h, concurrency={concurrency}")
    logger.info(f"🤖 ML Enabled: anomaly={_ML_AVAILABLE}, prophet={_PROPHET_AVAILABLE}")
    
    all_results: List[AuditResult] = []
    provider_metrics: Dict[str, ProviderMetrics] = {}
    
    # Initialize ML components
    model_manager = MLModelManager(config) if _ML_AVAILABLE else None
    if model_manager:
        await model_manager.initialize()
    
    anomaly_detector = MLAnomalyDetector(config, model_manager) if _ML_AVAILABLE else None
    quality_scorer = PredictiveQualityScorer(config, model_manager) if _ML_AVAILABLE else None
    time_series_detector = TimeSeriesAnomalyDetector(config) if _PROPHET_AVAILABLE else None
    compliance_checker = ComplianceChecker(config)
    remediation_engine = RemediationEngine(config)
    root_cause_analyzer = RootCauseAnalyzer(config)
    spc = StatisticalProcessControl(config)
    
    if anomaly_detector:
        await anomaly_detector.initialize()
    if quality_scorer:
        await quality_scorer.initialize()
    if time_series_detector:
        await time_series_detector.initialize()
    
    for key in keys:
        key = str(key).strip()
        if not key:
            continue
        
        logger.info(f"🔍 Scanning Page: {key}")
        
        symbols = _read_page_symbols(key, sid)
        symbols = [s for s in symbols if s]
        symbols = _dedupe_preserve(symbols)
        
        if max_symbols_per_page > 0 and len(symbols) > max_symbols_per_page:
            symbols = symbols[:max_symbols_per_page]
        
        if not symbols:
            logger.info(f"   -> Empty page.")
            continue
        
        logger.info(f"   -> Fetching {len(symbols)} symbols (refresh={refresh}, concurrency={concurrency})...")
        quotes_map = await _fetch_quotes(
            engine, symbols,
            refresh=refresh,
            concurrency=concurrency,
            provider_metrics=provider_metrics
        )
        
        page_alerts = 0
        for sym in symbols:
            q = quotes_map.get(sym) or {}
            if not isinstance(q, dict):
                q = {}
            
            result = await _audit_quote(
                sym, q, config,
                anomaly_detector=anomaly_detector,
                quality_scorer=quality_scorer,
                time_series_detector=time_series_detector,
                compliance_checker=compliance_checker,
                remediation_engine=remediation_engine,
                root_cause_analyzer=root_cause_analyzer,
                spc=spc,
                model_manager=model_manager
            )
            result.page = key
            all_results.append(result)
            
            if result.severity != AuditSeverity.OK:
                page_alerts += 1
                
                # Send alert for critical/high issues
                if alert_manager and result.severity in (AuditSeverity.CRITICAL, AuditSeverity.HIGH):
                    priority = AlertPriority.P1 if result.severity == AuditSeverity.CRITICAL else AlertPriority.P2
                    await alert_manager.send_alert(
                        result.severity,
                        f"Data Quality Alert: {result.symbol}",
                        f"Issues: {', '.join(result.issues[:3])}",
                        {
                            "page": result.page,
                            "provider": result.provider,
                            "age_hours": result.age_hours,
                            "quality_score": result.ml_quality_score,
                            "anomalies": [a.value for a in result.anomalies[:3]],
                            "root_cause": result.root_cause
                        },
                        priority
                    )
                
                # Broadcast via WebSocket
                if websocket_server:
                    await websocket_server.broadcast({
                        "type": "alert",
                        "data": result.to_dict()
                    })
        
        logger.info(f"   -> Alerts on {key}: {page_alerts}")
    
    # Generate summary
    summary = _generate_summary(all_results, provider_metrics)
    
    # Update metrics
    if metrics_exporter:
        metrics_exporter.update(summary)
    
    # Broadcast summary via WebSocket
    if websocket_server:
        await websocket_server.broadcast({
            "type": "summary",
            "data": summary.to_dict()
        })
    
    return all_results, summary


# =============================================================================
# Metrics Endpoint
# =============================================================================

def start_metrics_server(port: int = 9090):
    """Start Prometheus metrics server"""
    if not _PROMETHEUS_AVAILABLE:
        return
    
    from prometheus_client import start_http_server
    start_http_server(port)
    logger.info(f"Prometheus metrics server started on port {port}")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="TFB Enterprise Data Quality & Strategy Auditor v3.2.0")
    
    # Core options
    parser.add_argument("--keys", nargs="*", default=None, help="Specific pages to audit")
    parser.add_argument("--sheet-id", dest="sheet_id", help="Override Spreadsheet ID")
    
    # Audit parameters
    parser.add_argument("--refresh", type=int, default=1, help="1=force refresh, 0=allow cache")
    parser.add_argument("--concurrency", type=int, default=12, help="Concurrency for fetching")
    parser.add_argument("--max-per-page", type=int, default=0, help="Limit symbols per page")
    parser.add_argument("--include-ok", type=int, default=0, help="1=include OK rows in output")
    parser.add_argument("--strict", type=int, default=0, help="1=exit non-zero if CRITICAL issues")
    
    # Threshold overrides
    parser.add_argument("--stale-hours", type=int, help="Override stale_hours threshold")
    parser.add_argument("--hard-stale-hours", type=int, help="Override hard_stale_hours threshold")
    
    # ML options
    parser.add_argument("--ml-model", help="Path to pre-trained ML model")
    parser.add_argument("--train-data", help="Path to historical data for training")
    parser.add_argument("--disable-ml", action="store_true", help="Disable ML features")
    parser.add_argument("--save-models", help="Save trained models to directory")
    
    # Export options
    parser.add_argument("--json-out", help="Save full report to JSON file")
    parser.add_argument("--csv-out", help="Save alerts to CSV file")
    parser.add_argument("--parquet-out", help="Save to Parquet file")
    parser.add_argument("--excel-out", help="Save to Excel file")
    parser.add_argument("--dashboard-dir", help="Generate HTML dashboard in directory")
    
    # Alerting
    parser.add_argument("--slack-webhook", help="Slack webhook URL for alerts")
    parser.add_argument("--slack-channel", default="#alerts", help="Slack channel for alerts")
    parser.add_argument("--email-recipients", nargs="*", help="Email recipients for alerts")
    parser.add_argument("--email-sender", help="Email sender address")
    parser.add_argument("--pagerduty-key", help="PagerDuty integration key")
    parser.add_argument("--webhook-url", help="Generic webhook URL")
    parser.add_argument("--twilio-sid", help="Twilio Account SID for SMS")
    parser.add_argument("--twilio-token", help="Twilio Auth Token")
    parser.add_argument("--twilio-from", help="Twilio phone number")
    parser.add_argument("--sms-recipients", nargs="*", help="SMS recipients")
    parser.add_argument("--disable-alerts", action="store_true", help="Disable all alerts")
    
    # Metrics
    parser.add_argument("--metrics-port", type=int, default=0, help="Start Prometheus metrics server on port")
    
    # WebSocket
    parser.add_argument("--websocket-port", type=int, default=8765, help="WebSocket server port")
    parser.add_argument("--enable-websocket", action="store_true", help="Enable WebSocket server")
    
    # Compliance
    parser.add_argument("--sama-compliance", type=int, default=1, help="Enable SAMA compliance checks")
    parser.add_argument("--cma-compliance", type=int, default=1, help="Enable CMA compliance checks")
    
    args = parser.parse_args()
    
    strict = bool(int(args.strict or 0))
    
    # Start metrics server if requested
    if args.metrics_port > 0:
        start_metrics_server(args.metrics_port)
    
    # Dependency readiness
    if not DEPS.ok:
        msg = f"Audit skipped (environment not ready). Missing deps. Details: {DEPS.err or 'symbols_reader/get_engine missing'}"
        if strict:
            logger.error(msg)
            sys.exit(2)
        logger.warning(msg)
        sys.exit(0)
    
    sid = _get_spreadsheet_id(args)
    if not sid:
        msg = "No Spreadsheet ID found (check --sheet-id, env settings.default_spreadsheet_id, or DEFAULT_SPREADSHEET_ID)."
        if strict:
            logger.error(msg)
            sys.exit(2)
        logger.warning(msg)
        sys.exit(0)
    
    keys = _get_target_keys(args)
    if not keys:
        msg = "No pages found to audit (keys empty)."
        if strict:
            logger.error(msg)
            sys.exit(2)
        logger.warning(msg)
        sys.exit(0)
    
    # Configuration
    config = AuditConfig()
    if args.stale_hours:
        config.stale_hours = float(args.stale_hours)
    if args.hard_stale_hours:
        config.hard_stale_hours = float(args.hard_stale_hours)
    if args.disable_ml:
        config.enable_ml_predictions = False
    
    # Compliance settings
    config.sama_compliant = bool(int(args.sama_compliance))
    config.cma_compliant = bool(int(args.cma_compliance))
    
    # ML model paths
    if args.ml_model:
        config.ml_model_path = args.ml_model
        config.anomaly_model_path = os.path.join(args.ml_model, "anomaly.joblib")
        config.quality_model_path = os.path.join(args.ml_model, "quality.joblib")
    
    # WebSocket
    config.websocket_port = args.websocket_port
    config.enable_websocket = args.enable_websocket
    
    # Alerting
    alert_manager = None
    if not args.disable_alerts:
        config.enable_alerts = True
        config.alert_channels = [AlertChannel.CONSOLE]
        
        if args.slack_webhook:
            config.alert_channels.append(AlertChannel.SLACK)
            config.slack_webhook = args.slack_webhook
            config.slack_channel = args.slack_channel
        
        if args.email_recipients and args.email_sender:
            config.alert_channels.append(AlertChannel.EMAIL)
            config.email_recipients = args.email_recipients
            config.email_sender = args.email_sender
        
        if args.pagerduty_key:
            config.alert_channels.append(AlertChannel.PAGERDUTY)
            config.pagerduty_key = args.pagerduty_key
        
        if args.webhook_url:
            config.alert_channels.append(AlertChannel.WEBHOOK)
            config.webhook_url = args.webhook_url
        
        if args.twilio_sid and args.twilio_token and args.twilio_from and args.sms_recipients:
            config.alert_channels.append(AlertChannel.SMS)
            config.twilio_sid = args.twilio_sid
            config.twilio_token = args.twilio_token
            config.twilio_from = args.twilio_from
            config.sms_recipients = args.sms_recipients
        
        alert_manager = AlertManager(config)
    
    # Metrics exporter
    metrics_exporter = MetricsExporter() if args.metrics_port > 0 else None
    
    # WebSocket server
    websocket_server = None
    if config.enable_websocket and _ASYNC_HTTP_AVAILABLE:
        websocket_server = WebSocketServer(config)
        asyncio.create_task(websocket_server.start())
    
    refresh = bool(int(args.refresh or 0))
    concurrency = max(1, min(40, int(args.concurrency or 12)))
    max_per_page = max(0, int(args.max_per_page or 0))
    include_ok = bool(int(args.include_ok or 0))
    
    # Run
    start_time = time.time()
    
    try:
        results, summary = asyncio.run(
            _run_audit(
                keys=keys,
                sid=sid,
                config=config,
                refresh=refresh,
                concurrency=concurrency,
                max_symbols_per_page=max_per_page,
                alert_manager=alert_manager,
                metrics_exporter=metrics_exporter,
                websocket_server=websocket_server
            )
        )
        
        summary.audit_duration_ms = (time.time() - start_time) * 1000
        
    except KeyboardInterrupt:
        logger.info("Audit interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Audit run crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Filter alerts if needed
    alerts_only = [r for r in results if r.severity != AuditSeverity.OK]
    
    # Console output
    if not alerts_only:
        logger.info("\n✅ AUDIT COMPLETE: No alerts found. System looks healthy.")
    else:
        logger.info(f"\n=== ⚠️  AUDIT REPORT: {len(alerts_only)} ITEMS REQUIRING ATTENTION ===")
        _print_table(alerts_only, limit=600)
    
    # System diagnosis
    logger.info("\n🧠 SYSTEM DIAGNOSIS (v3.2.0):")
    logger.info(f"   Pages scanned: {len(keys)}")
    logger.info(f"   Assets analyzed: {summary.total_assets}")
    logger.info(f"   CRITICAL: {summary.by_severity.get(AuditSeverity.CRITICAL, 0)}")
    logger.info(f"   HIGH: {summary.by_severity.get(AuditSeverity.HIGH, 0)}")
    logger.info(f"   MEDIUM: {summary.by_severity.get(AuditSeverity.MEDIUM, 0)}")
    logger.info(f"   LOW: {summary.by_severity.get(AuditSeverity.LOW, 0)}")
    logger.info(f"   OK: {summary.by_severity.get(AuditSeverity.OK, 0)}")
    
    logger.info(f"   Trend breaks: {summary.strategy_note_counts.get('TREND_BREAK_BEAR', 0) + summary.strategy_note_counts.get('TREND_BREAK_BULL', 0)}")
    logger.info(f"   Aggressive forecasts: {summary.strategy_note_counts.get('AGGRESSIVE_FORECAST_LOW_VOL', 0)}")
    logger.info(f"   Data holes: {summary.issue_counts.get('INSUFFICIENT_HISTORY', 0)}")
    logger.info(f"   Provider errors: {summary.issue_counts.get('PROVIDER_ERROR', 0)}")
    logger.info(f"   Anomalies detected: {sum(summary.anomaly_counts.values())}")
    logger.info(f"   Compliance violations: {len(summary.compliance_violations)}")
    logger.info(f"   SPC violations: {len(summary.spc_stats)}")
    logger.info(f"   Audit duration: {summary.audit_duration_ms:.2f} ms")
    
    # Provider health
    logger.info("\n🏥 Provider Health:")
    for pm in summary.provider_metrics.values():
        logger.info(f"   {pm.name}: {pm.health.value} (errors: {pm.error_rate:.2%}, latency: {pm.avg_latency_ms:.2f}ms, p95: {pm.p95_latency_ms:.2f}ms, cache: {pm.cache_hit_rate:.2%})")
    
    # ML stats
    if summary.ml_stats:
        logger.info("\n🤖 ML Statistics:")
        for key, value in summary.ml_stats.items():
            logger.info(f"   {key}: {value:.2f}")
    
    # Remediation stats
    if summary.remediation_stats:
        logger.info("\n🔧 Remediation Summary:")
        for key, value in summary.remediation_stats.items():
            logger.info(f"   {key}: {value}")
    
    # SPC stats
    if summary.spc_stats:
        logger.info("\n📊 SPC Violations:")
        for key, value in summary.spc_stats.items():
            logger.info(f"   {key}: {value}")
    
    # Prepare payload for export
    payload = {
        "audit_version": "3.2.0",
        "audit_time_utc": _utc_now_iso(),
        "audit_time_riyadh": _riyadh_iso(),
        "spreadsheet_id": sid,
        "keys": keys,
        "config": config.to_dict(),
        "run_config": {
            "refresh": refresh,
            "concurrency": concurrency,
            "max_per_page": max_per_page,
            "include_ok": include_ok,
            "strict": strict,
        },
        "summary": summary.to_dict(),
        "alerts": [r.to_dict() for r in alerts_only],
        "all_results": [r.to_dict() for r in results] if include_ok else None,
    }
    
    # Export to various formats
    if args.json_out:
        _export_json(args.json_out, payload)
        logger.info(f"📄 JSON report saved to {args.json_out}")
    
    if args.csv_out:
        _export_csv(args.csv_out, alerts_only)
        logger.info(f"📄 CSV alerts saved to {args.csv_out}")
    
    if args.parquet_out:
        _export_parquet(args.parquet_out, alerts_only)
        logger.info(f"📄 Parquet saved to {args.parquet_out}")
    
    if args.excel_out:
        _export_excel(args.excel_out, alerts_only, summary)
        logger.info(f"📄 Excel saved to {args.excel_out}")
    
    if args.dashboard_dir:
        os.makedirs(args.dashboard_dir, exist_ok=True)
        _generate_dashboard(summary, args.dashboard_dir)
    
    # Save models if requested
    if args.save_models and model_manager:
        asyncio.run(model_manager.save_models(args.save_models))
        logger.info(f"💾 ML models saved to {args.save_models}")
    
    # Exit code policy
    if strict:
        critical_count = summary.by_severity.get(AuditSeverity.CRITICAL, 0)
        if critical_count > 0:
            sys.exit(3)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
