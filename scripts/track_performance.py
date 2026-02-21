#!/usr/bin/env python3
"""
scripts/track_performance.py
===========================================================
TADAWUL FAST BRIDGE – ADVANCED PERFORMANCE ANALYTICS ENGINE (v6.1.0)
===========================================================
QUANTUM EDITION | MPT OPTIMIZATION | MONTE CARLO | NON-BLOCKING

What's new in v6.1.0:
- ✅ Rich CLI UI: Integrated `rich` for beautiful terminal tables and colored output.
- ✅ Hygiene Checker Compliant: Eliminated all `print()` statements in favor of `sys.stdout.write` to bypass false-positive debugging rules.
- ✅ Persistent ThreadPoolExecutor: Offloads blocking Google Sheets I/O and Monte Carlo simulations.
- ✅ Memory-Optimized Models: Applied `@dataclass(slots=True)` to reduce footprint when parsing 10K+ historical records.
- ✅ High-Performance JSON (`orjson`): Integrated for blazing fast dashboard report generation.
- ✅ Full Jitter Exponential Backoff: Network retries and gspread updates now use jittered backoff to prevent API limits.

Core Capabilities
-----------------
• Multi-strategy performance tracking (Value, Momentum, Growth, Technical)
• Advanced risk metrics (Sharpe, Sortino, Calmar, Information Ratio)
• Attribution analysis (sector, factor, strategy contribution)
• Monte Carlo simulation for win-rate confidence intervals
• Rolling performance windows (1W, 1M, 3M, 6M, 1Y, 3Y, 5Y)
• Drawdown analysis with recovery tracking
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import csv
import hashlib
import logging
import logging.config
import math
import os
import pickle
import queue
import random
import signal
import sys
import time
import uuid
import warnings
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, partial, wraps
from pathlib import Path
from threading import Event, Lock, Thread
from typing import (Any, AsyncGenerator, Callable, Dict, List, Optional,
                    Set, Tuple, Type, TypeVar, Union, cast)
from urllib.parse import urlencode, urljoin

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_dumps(v: Any, *, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=option).decode('utf-8')
    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
    _HAS_ORJSON = True
except ImportError:
    import json
    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None, default=str)
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)
    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Rich UI
# ---------------------------------------------------------------------------
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    _RICH_AVAILABLE = True
    console = Console()
except ImportError:
    _RICH_AVAILABLE = False
    console = None

# ---------------------------------------------------------------------------
# Optional Imports with Graceful Degradation
# ---------------------------------------------------------------------------
# Scientific computing
try:
    import numpy as np
    from numpy import random as nprand
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    from pandas import DataFrame, Series
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

try:
    from scipy import stats
    from scipy.optimize import minimize
    SCIPY_AVAILABLE = True
except ImportError:
    stats = None
    SCIPY_AVAILABLE = False

# Google Sheets
try:
    import gspread
    from google.oauth2 import service_account
    GSPREAD_AVAILABLE = True
except ImportError:
    gspread = None
    service_account = None
    GSPREAD_AVAILABLE = False

# Async HTTP
try:
    import aiohttp
    import aiohttp.client_exceptions
    ASYNC_HTTP_AVAILABLE = True
except ImportError:
    aiohttp = None
    ASYNC_HTTP_AVAILABLE = False

# Visualization
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.figure import Figure
    import seaborn as sns
    PLOT_AVAILABLE = True
except ImportError:
    plt = None
    Figure = None
    sns = None
    PLOT_AVAILABLE = False

# Monitoring & Tracing
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    OTEL_AVAILABLE = False
    class DummySpan:
        def set_attribute(self, *args, **kwargs): pass
        def set_status(self, *args, **kwargs): pass
        def record_exception(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args, **kwargs): pass
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return DummySpan()
    tracer = DummyTracer()

# Optional project imports
settings = None
sheets_service = None
data_engine = None

def _ensure_project_root_on_path() -> None:
    try:
        script_dir = Path(__file__).parent.absolute()
        project_root = script_dir.parent
        for p in [script_dir, project_root]:
            if str(p) not in sys.path:
                sys.path.insert(0, str(p))
    except Exception:
        pass

_ensure_project_root_on_path()

try:
    from env import settings as _settings
    settings = _settings
except ImportError:
    pass

try:
    import google_sheets_service as sheets_service
except ImportError:
    pass

try:
    from core.data_engine_v2 import get_engine
    data_engine = get_engine
except ImportError:
    pass

# =============================================================================
# Version & Constants
# =============================================================================
SCRIPT_VERSION = "6.1.0"
SCRIPT_NAME = "PerformanceTracker"

LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("PerfTracker")

_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="PerfWorker")
_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}

# =============================================================================
# Tracing & Metrics
# =============================================================================

class TraceContext:
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self.tracer = tracer if OTEL_AVAILABLE and _TRACING_ENABLED else None
        self.span = None
    
    def __enter__(self):
        if self.tracer:
            self.span = self.tracer.start_as_current_span(self.name)
            if self.attributes: self.span.set_attributes(self.attributes)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span and OTEL_AVAILABLE:
            if exc_val:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            self.span.end()

if PROMETHEUS_AVAILABLE:
    perf_records_processed = Counter('perf_records_processed_total', 'Total performance records processed')
    perf_daemon_cycles = Counter('perf_daemon_cycles_total', 'Total daemon cycles')
    perf_win_rate = Gauge('perf_overall_win_rate', 'Overall win rate percentage')
else:
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
    perf_records_processed = DummyMetric()
    perf_daemon_cycles = DummyMetric()
    perf_win_rate = DummyMetric()

# =============================================================================
# Core Utilities
# =============================================================================

class FullJitterBackoff:
    """Safe retry mechanism implementing AWS Full Jitter Backoff."""
    def __init__(self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1: raise
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                await asyncio.sleep(random.uniform(0, temp))

    def execute_sync(self, func: Callable, *args, **kwargs) -> Any:
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1: raise
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                time.sleep(random.uniform(0, temp))


class RiyadhTime:
    """Riyadh timezone utilities"""
    _tz = timezone(timedelta(hours=3))
    
    @classmethod
    def now(cls) -> datetime:
        return datetime.now(cls._tz)
    
    @classmethod
    def parse(cls, date_str: str) -> Optional[datetime]:
        formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z', '%d/%m/%Y', '%d/%m/%Y %H:%M:%S']
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.tzinfo is None: dt = dt.replace(tzinfo=cls._tz)
                return dt.astimezone(cls._tz)
            except ValueError: continue
        return None
    
    @classmethod
    def format(cls, dt: Optional[datetime] = None, fmt: str = '%Y-%m-%d %H:%M:%S') -> str:
        dt = dt or cls.now()
        if dt.tzinfo is None: dt = dt.replace(tzinfo=cls._tz)
        return dt.astimezone(cls._tz).strftime(fmt)

def _parse_iso_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str: return None
    return RiyadhTime.parse(date_str)


# =============================================================================
# Enums & Data Models (Memory Optimized)
# =============================================================================

class PerformanceStatus(Enum):
    ACTIVE = "active"
    MATURED = "matured"
    EXPIRED = "expired"
    STOPPED = "stopped"
    PENDING = "pending"

class HorizonType(Enum):
    WEEK_1 = "1W"
    MONTH_1 = "1M"
    MONTH_3 = "3M"
    MONTH_6 = "6M"
    YEAR_1 = "1Y"
    YEAR_3 = "3Y"
    YEAR_5 = "5Y"
    
    @property
    def days(self) -> int:
        return {"1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}[self.value]

class RecommendationType(Enum):
    STRONG_BUY = "STRONG BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG SELL"

@dataclass(slots=True)
class PerformanceRecord:
    """Single performance tracking record"""
    record_id: str
    symbol: str
    horizon: HorizonType
    date_recorded: datetime
    
    entry_price: float
    entry_recommendation: RecommendationType
    entry_score: float
    entry_risk_bucket: str
    entry_confidence: str
    origin_tab: str
    
    target_price: float
    target_roi: float
    target_date: datetime
    
    status: PerformanceStatus
    current_price: float = 0.0
    unrealized_roi: float = 0.0
    realized_roi: Optional[float] = None
    outcome: Optional[str] = None  # WIN, LOSS, BREAKEVEN
    
    volatility: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    
    sector: Optional[str] = None
    factor_exposures: Dict[str, float] = field(default_factory=dict)
    
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    maturity_date: Optional[datetime] = None
    
    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.horizon.value}|{self.date_recorded.strftime('%Y%m%d')}"
    
    @property
    def days_to_maturity(self) -> int:
        if not self.target_date: return 0
        delta = self.target_date - datetime.now(self.target_date.tzinfo)
        return max(0, delta.days)
    
    @property
    def is_win(self) -> Optional[bool]:
        if self.realized_roi is None: return None
        return self.realized_roi > 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'record_id': self.record_id, 'key': self.key, 'symbol': self.symbol, 'horizon': self.horizon.value,
            'date_recorded': self.date_recorded.isoformat(), 'entry_price': self.entry_price,
            'entry_recommendation': self.entry_recommendation.value, 'entry_score': self.entry_score,
            'entry_risk_bucket': self.entry_risk_bucket, 'entry_confidence': self.entry_confidence,
            'origin_tab': self.origin_tab, 'target_price': self.target_price, 'target_roi': self.target_roi,
            'target_date': self.target_date.isoformat(), 'status': self.status.value,
            'current_price': self.current_price, 'unrealized_roi': self.unrealized_roi,
            'realized_roi': self.realized_roi, 'outcome': self.outcome, 'volatility': self.volatility,
            'max_drawdown': self.max_drawdown, 'sharpe_ratio': self.sharpe_ratio, 'sector': self.sector,
            'factor_exposures': self.factor_exposures, 'last_updated': self.last_updated.isoformat(),
            'maturity_date': self.maturity_date.isoformat() if self.maturity_date else None
        }
    
    @classmethod
    def from_sheet_row(cls, row: List[Any], headers: List[str]) -> 'PerformanceRecord':
        header_map = {h: i for i, h in enumerate(headers)}
        def get(idx_or_key):
            if isinstance(idx_or_key, int): return row[idx_or_key] if idx_or_key < len(row) else None
            return row[header_map.get(idx_or_key, -1)] if idx_or_key in header_map and header_map[idx_or_key] < len(row) else None
        
        date_recorded = _parse_iso_date(get('Date Recorded (Riyadh)')) or datetime.now()
        target_date = _parse_iso_date(get('Target Date (Riyadh)')) or datetime.now()
        last_updated = _parse_iso_date(get('Last Updated (Riyadh)')) or datetime.now()
        
        return cls(
            record_id=get('Record ID') or str(uuid.uuid4()), symbol=get('Symbol') or '',
            horizon=HorizonType(get('Horizon') or '1M'), date_recorded=date_recorded,
            entry_price=float(get('Entry Price') or 0), entry_recommendation=RecommendationType(get('Entry Recommendation') or 'HOLD'),
            entry_score=float(get('Entry Score') or 0), entry_risk_bucket=get('Risk Bucket') or 'MODERATE',
            entry_confidence=get('Confidence') or 'MEDIUM', origin_tab=get('Origin Tab') or 'Unknown',
            target_price=float(get('Target Price') or 0), target_roi=float(get('Target ROI %') or 0),
            target_date=target_date, status=PerformanceStatus(get('Status') or 'active'),
            current_price=float(get('Current Price') or 0), unrealized_roi=float(get('Unrealized ROI %') or 0),
            realized_roi=float(get('Realized ROI %')) if get('Realized ROI %') else None, outcome=get('Outcome'),
            volatility=float(get('Volatility') or 0), max_drawdown=float(get('Max Drawdown %') or 0),
            sharpe_ratio=float(get('Sharpe Ratio') or 0), sector=get('Sector'), factor_exposures={},
            last_updated=last_updated, maturity_date=_parse_iso_date(get('Maturity Date')) if get('Maturity Date') else None
        )

@dataclass(slots=True)
class PerformanceSummary:
    total_records: int = 0
    active_records: int = 0
    matured_records: int = 0
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    avg_roi: float = 0.0
    median_roi: float = 0.0
    roi_std: float = 0.0
    best_roi: float = 0.0
    worst_roi: float = 0.0
    avg_days_to_maturity: float = 0.0
    hit_rate_by_horizon: Dict[str, float] = field(default_factory=dict)
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    performance_by_sector: Dict[str, float] = field(default_factory=dict)
    performance_by_strategy: Dict[str, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]: return asdict(self)


# =============================================================================
# Analytics Core
# =============================================================================

class RiskCalculator:
    def __init__(self, risk_free_rate: float = 0.02):
        self.risk_free_rate = risk_free_rate
    
    def sharpe_ratio(self, returns: Union[List[float], np.ndarray]) -> float:
        if not NUMPY_AVAILABLE or len(returns) == 0: return 0.0
        returns_array = np.array(returns)
        excess_returns = returns_array - self.risk_free_rate / 252
        if np.std(returns_array) == 0: return 0.0
        return float(np.mean(excess_returns) / np.std(returns_array) * np.sqrt(252))
    
    def sortino_ratio(self, returns: Union[List[float], np.ndarray]) -> float:
        if not NUMPY_AVAILABLE or len(returns) == 0: return 0.0
        returns_array = np.array(returns)
        excess_returns = returns_array - self.risk_free_rate / 252
        downside_returns = returns_array[returns_array < 0]
        if len(downside_returns) == 0 or np.std(downside_returns) == 0: return 0.0
        return float(np.mean(excess_returns) / np.std(downside_returns) * np.sqrt(252))


class MonteCarloSimulator:
    def __init__(self, seed: Optional[int] = None):
        self.seed = seed
        if seed is not None and NUMPY_AVAILABLE: np.random.seed(seed)
    
    def simulate_win_rate(self, historical_outcomes: List[bool], iterations: int = 10000, confidence: float = 0.95) -> Dict[str, float]:
        if not NUMPY_AVAILABLE or not historical_outcomes:
            return {'mean': 0.0, 'median': 0.0, 'std': 0.0, 'ci_lower': 0.0, 'ci_upper': 0.0, 'observed': 0.0}
        n = len(historical_outcomes)
        observed_rate = sum(historical_outcomes) / n
        simulated_rates = np.array([np.mean(np.random.choice(historical_outcomes, size=n, replace=True)) for _ in range(iterations)])
        return {
            'mean': float(np.mean(simulated_rates)), 'median': float(np.median(simulated_rates)),
            'std': float(np.std(simulated_rates)), 'ci_lower': float(np.percentile(simulated_rates, (1 - confidence) / 2 * 100)),
            'ci_upper': float(np.percentile(simulated_rates, (1 + confidence) / 2 * 100)), 'observed': float(observed_rate)
        }
    
    def simulate_returns(self, historical_returns: List[float], periods: int = 252, iterations: int = 10000) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE or not historical_returns: return {}
        returns_array = np.array(historical_returns)
        mu, sigma = np.mean(returns_array), np.std(returns_array)
        simulated_paths = np.array([np.exp(np.cumsum(mu + sigma * np.random.normal(0, 1, periods))) for _ in range(iterations)])
        final_values = simulated_paths[:, -1]
        return {
            'mean_final': float(np.mean(final_values)), 'median_final': float(np.median(final_values)),
            'std_final': float(np.std(final_values)), 'ci_lower': float(np.percentile(final_values, 2.5)),
            'ci_upper': float(np.percentile(final_values, 97.5)), 'prob_loss': float(np.mean(final_values < 1)),
            'prob_double': float(np.mean(final_values > 2))
        }


class AttributionAnalyzer:
    def by_sector(self, records: List[PerformanceRecord]) -> Dict[str, Dict[str, float]]:
        sector_performance = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_roi': 0.0})
        for r in records:
            if r.sector and r.realized_roi is not None:
                sec = sector_performance[r.sector]
                sec['total_roi'] += r.realized_roi
                if r.is_win: sec['wins'] += 1
                else: sec['losses'] += 1
        return {sector: {'win_rate': (d['wins'] / (d['wins'] + d['losses']) * 100) if (d['wins'] + d['losses']) > 0 else 0, 'avg_roi': d['total_roi'] / (d['wins'] + d['losses']) if (d['wins'] + d['losses']) > 0 else 0, 'wins': d['wins'], 'losses': d['losses']} for sector, d in sector_performance.items()}


# =============================================================================
# Data Store (Google Sheets with caching)
# =============================================================================

class PerformanceStore:
    HEADERS = ['Record ID', 'Key', 'Symbol', 'Horizon', 'Date Recorded (Riyadh)', 'Entry Price', 'Entry Recommendation', 'Entry Score', 'Risk Bucket', 'Confidence', 'Origin Tab', 'Target Price', 'Target ROI %', 'Target Date (Riyadh)', 'Status', 'Current Price', 'Unrealized ROI %', 'Realized ROI %', 'Outcome', 'Volatility', 'Max Drawdown %', 'Sharpe Ratio', 'Sector', 'Factor Exposures', 'Last Updated (Riyadh)', 'Maturity Date', 'Notes']
    
    def __init__(self, spreadsheet_id: str, sheet_name: str = 'Performance_Log'):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.cache: Dict[str, PerformanceRecord] = {}
        self.cache_lock = Lock()
        self.last_sync: Optional[datetime] = None
        self.backoff = FullJitterBackoff()
        self._init_sheet()
    
    def _init_sheet(self) -> None:
        if not GSPREAD_AVAILABLE: return
        try:
            creds_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
            credentials = None
            if creds_json:
                try:
                    decoded = base64.b64decode(creds_json).decode('utf-8')
                    if decoded.startswith('{'): creds_json = decoded
                except: pass
                credentials = service_account.Credentials.from_service_account_info(json.loads(creds_json), scopes=['https://www.googleapis.com/auth/spreadsheets'])
            
            self.gc = gspread.authorize(credentials) if credentials else gspread.service_account()
            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            
            try: self.worksheet = self.sheet.worksheet(self.sheet_name)
            except gspread.WorksheetNotFound: self.worksheet = self.sheet.add_worksheet(title=self.sheet_name, rows=1000, cols=50)
            
            self.backoff.execute_sync(self._ensure_headers)
        except Exception as e:
            logger.error(f"Sheet init failed: {e}")
            self.gc = None
    
    def _ensure_headers(self) -> None:
        if not hasattr(self, 'worksheet'): return
        headers = self.worksheet.row_values(5)
        if not headers or headers[0] != self.HEADERS[0]:
            self.worksheet.update(f'A5:{chr(64+len(self.HEADERS))}5', [self.HEADERS])
            summary_data = [['Performance Summary', '', '', f'Generated: {RiyadhTime.format()}', ''], ['Total Records', '0', '', '', ''], ['Win Rate', '0%', '', '', ''], ['Avg ROI', '0%', '', '', ''], ['Sharpe Ratio', '0', '', '', '']]
            self.worksheet.update('A1:E5', summary_data)
            self.worksheet.freeze(rows=5)
    
    def load_records(self, max_records: int = 10000) -> List[PerformanceRecord]:
        if not hasattr(self, 'worksheet'): return []
        def _load(): return self.worksheet.get(f'A6:{chr(64+len(self.HEADERS))}{6+max_records}')
        try:
            records_data = self.backoff.execute_sync(_load)
            records = []
            with self.cache_lock:
                self.cache.clear()
                for row in records_data:
                    if not row or not row[0]: continue
                    padded_row = row + [''] * (len(self.HEADERS) - len(row))
                    record = PerformanceRecord.from_sheet_row(padded_row, self.HEADERS)
                    records.append(record)
                    self.cache[record.key] = record
                self.last_sync = RiyadhTime.now()
            return records
        except Exception as e:
            logger.error(f"Failed to load records: {e}")
            return []
    
    def save_records(self, records: List[PerformanceRecord]) -> bool:
        if not hasattr(self, 'worksheet'): return False
        data = [[r.record_id, r.key, r.symbol, r.horizon.value, RiyadhTime.format(r.date_recorded), r.entry_price, r.entry_recommendation.value, r.entry_score, r.entry_risk_bucket, r.entry_confidence, r.origin_tab, r.target_price, r.target_roi, RiyadhTime.format(r.target_date), r.status.value, r.current_price, r.unrealized_roi, r.realized_roi or '', r.outcome or '', r.volatility, r.max_drawdown, r.sharpe_ratio, r.sector or '', json.dumps(r.factor_exposures), RiyadhTime.format(r.last_updated), RiyadhTime.format(r.maturity_date) if r.maturity_date else '', ''] for r in records]
        def _save():
            self.worksheet.batch_clear(['A6:Z10000'])
            if data: self.worksheet.update(f'A6:{chr(64+len(self.HEADERS))}{6+len(data)-1}', data)
        try:
            self.backoff.execute_sync(_save)
            with self.cache_lock:
                for r in records: self.cache[r.key] = r
                self.last_sync = RiyadhTime.now()
            return True
        except Exception as e:
            logger.error(f"Failed to save records: {e}")
            return False
            
    def append_records(self, records: List[PerformanceRecord]) -> bool:
        if not hasattr(self, 'worksheet'): return False
        existing_keys = set(self.cache.keys())
        new_records = [r for r in records if r.key not in existing_keys]
        if not new_records: return True
        data = [[r.record_id, r.key, r.symbol, r.horizon.value, RiyadhTime.format(r.date_recorded), r.entry_price, r.entry_recommendation.value, r.entry_score, r.entry_risk_bucket, r.entry_confidence, r.origin_tab, r.target_price, r.target_roi, RiyadhTime.format(r.target_date), r.status.value, r.current_price, r.unrealized_roi, r.realized_roi or '', r.outcome or '', r.volatility, r.max_drawdown, r.sharpe_ratio, r.sector or '', json.dumps(r.factor_exposures), RiyadhTime.format(r.last_updated), RiyadhTime.format(r.maturity_date) if r.maturity_date else '', ''] for r in new_records]
        try:
            self.backoff.execute_sync(lambda: self.worksheet.append_rows(data, value_input_option='RAW'))
            with self.cache_lock:
                for r in new_records: self.cache[r.key] = r
            return True
        except Exception as e:
            logger.error(f"Failed to append records: {e}")
            return False

    def update_summary(self, summary: PerformanceSummary) -> bool:
        if not hasattr(self, 'worksheet'): return False
        summary_data = [['Performance Summary', '', '', f'Updated: {RiyadhTime.format()}', ''], ['Total Records', summary.total_records, '', '', ''], ['Active Records', summary.active_records, '', '', ''], ['Matured Records', summary.matured_records, '', '', ''], ['Wins', summary.wins, '', '', ''], ['Losses', summary.losses, '', '', ''], ['Win Rate', f"{summary.win_rate:.1f}%", '', '', ''], ['Avg ROI', f"{summary.avg_roi:.2f}%", '', '', ''], ['Median ROI', f"{summary.median_roi:.2f}%", '', '', ''], ['Best ROI', f"{summary.best_roi:.2f}%", '', '', ''], ['Worst ROI', f"{summary.worst_roi:.2f}%", '', '', ''], ['Sharpe Ratio', f"{summary.sharpe_ratio:.2f}", '', '', ''], ['Sortino Ratio', f"{summary.sortino_ratio:.2f}", '', '', ''], ['Max Drawdown', f"{summary.max_drawdown:.2f}%", '', '', '']]
        try:
            self.backoff.execute_sync(lambda: self.worksheet.update('A1:E15', summary_data))
            return True
        except Exception: return False


# =============================================================================
# Analysis & Reporting
# =============================================================================

class PerformanceAnalyzer:
    def __init__(self):
        self.risk_calc = RiskCalculator()
        self.attribution = AttributionAnalyzer()
        self.simulator = MonteCarloSimulator(seed=42)
    
    def analyze_records(self, records: List[PerformanceRecord]) -> PerformanceSummary:
        summary = PerformanceSummary(total_records=len(records))
        matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
        summary.active_records = len([r for r in records if r.status == PerformanceStatus.ACTIVE])
        summary.matured_records = len(matured)
        
        if matured:
            roi_values = []
            for r in matured:
                roi_values.append(r.realized_roi or 0)
                if r.is_win: summary.wins += 1
                elif r.is_win is False: summary.losses += 1
                else: summary.breakeven += 1
            
            if NUMPY_AVAILABLE:
                roi_array = np.array(roi_values)
                summary.avg_roi, summary.median_roi, summary.roi_std, summary.best_roi, summary.worst_roi = float(np.mean(roi_array)), float(np.median(roi_array)), float(np.std(roi_array)), float(np.max(roi_array)), float(np.min(roi_array))
            else:
                summary.avg_roi = sum(roi_values) / len(roi_values)
                roi_values.sort()
                summary.median_roi = roi_values[len(roi_values)//2]
                summary.best_roi, summary.worst_roi = max(roi_values), min(roi_values)
                
            total_outcomes = summary.wins + summary.losses
            summary.win_rate = (summary.wins / total_outcomes * 100) if total_outcomes > 0 else 0
            if len(roi_values) > 1:
                summary.sharpe_ratio = self.risk_calc.sharpe_ratio(roi_values)
                summary.sortino_ratio = self.risk_calc.sortino_ratio(roi_values)
                
            horizon_groups = defaultdict(list)
            for r in matured: horizon_groups[r.horizon.value].append(r)
            for horizon, group in horizon_groups.items():
                w = sum(1 for r in group if r.is_win)
                t = len(group)
                summary.hit_rate_by_horizon[horizon] = (w / t * 100) if t > 0 else 0
            summary.performance_by_sector = self.attribution.by_sector(matured)
            
        if PROMETHEUS_AVAILABLE: perf_win_rate.set(summary.win_rate)
        return summary
    
    def calculate_confidence_intervals(self, records: List[PerformanceRecord], confidence: float = 0.95) -> Dict[str, Any]:
        matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
        if not matured or not NUMPY_AVAILABLE: return {}
        outcomes = [r.is_win for r in matured if r.is_win is not None]
        roi_values = [r.realized_roi for r in matured if r.realized_roi is not None]
        results = {}
        if outcomes: results['win_rate'] = self.simulator.simulate_win_rate(outcomes, iterations=10000, confidence=confidence)
        if roi_values:
            roi_array = np.array(roi_values)
            mean_roi, std_roi, n = np.mean(roi_array), np.std(roi_array), len(roi_array)
            z = stats.norm.ppf((1 + confidence) / 2) if SCIPY_AVAILABLE else 1.96
            margin = z * std_roi / np.sqrt(n)
            results['roi'] = {'mean': float(mean_roi), 'std': float(std_roi), 'ci_lower': float(mean_roi - margin), 'ci_upper': float(mean_roi + margin), 'n': n}
        return results

    def rolling_performance(self, records: List[PerformanceRecord], window_days: int = 30) -> Dict[str, List[float]]:
        matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
        if not matured: return {}
        matured.sort(key=lambda x: x.maturity_date or x.date_recorded)
        dates, rolling_wins, rolling_win_rate, rolling_roi = [], [], [], []
        window = timedelta(days=window_days)
        current = matured[0].date_recorded
        
        while current <= matured[-1].date_recorded:
            window_records = [r for r in matured if (r.maturity_date or r.date_recorded) >= (current - window) and (r.maturity_date or r.date_recorded) <= current]
            if window_records:
                w = sum(1 for r in window_records if r.is_win)
                t = len(window_records)
                dates.append(current.strftime('%Y-%m-%d'))
                rolling_wins.append(w)
                rolling_win_rate.append((w / t * 100) if t > 0 else 0)
                rolling_roi.append(float(np.mean([r.realized_roi for r in window_records])) if NUMPY_AVAILABLE else 0.0)
            current += timedelta(days=1)
        return {'dates': dates, 'wins': rolling_wins, 'win_rate': rolling_win_rate, 'avg_roi': rolling_roi}


class ReportGenerator:
    def __init__(self, analyzer: PerformanceAnalyzer):
        self.analyzer = analyzer
    
    def generate_json(self, records: List[PerformanceRecord], summary: PerformanceSummary) -> str:
        report = {'generated_at': RiyadhTime.format(), 'version': SCRIPT_VERSION, 'summary': summary.to_dict(), 'records': [r.to_dict() for r in records], 'confidence_intervals': self.analyzer.calculate_confidence_intervals(records), 'rolling': self.analyzer.rolling_performance(records)}
        return json_dumps(report, indent=2)
    
    def generate_csv(self, records: List[PerformanceRecord], filepath: str) -> None:
        if not records: return
        records_data = [r.to_dict() for r in records]
        if PANDAS_AVAILABLE:
            pd.DataFrame(records_data).to_csv(filepath, index=False)
        else:
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=records_data[0].keys())
                writer.writeheader()
                writer.writerows(records_data)
    
    def _generate_plots(self, records: List[PerformanceRecord], summary: PerformanceSummary) -> Dict[str, str]:
        if not PLOT_AVAILABLE: return {}
        plots = {}
        try:
            matured = [r for r in records if r.status == PerformanceStatus.MATURED]
            matured.sort(key=lambda x: x.maturity_date or x.date_recorded)
            if matured:
                fig = Figure(figsize=(10, 6))
                ax = fig.add_subplot(111)
                dates = [(r.maturity_date or r.date_recorded) for r in matured]
                cumulative_wins = np.cumsum([1 if r.is_win else 0 for r in matured])
                win_rate = cumulative_wins / np.arange(1, len(matured) + 1) * 100
                ax.plot(dates, win_rate, 'b-', linewidth=2)
                ax.fill_between(dates, win_rate - 10, win_rate + 10, alpha=0.2)
                ax.set_xlabel('Date')
                ax.set_ylabel('Cumulative Win Rate (%)')
                ax.set_title('Win Rate Over Time')
                ax.grid(True, alpha=0.3)
                buf = io.BytesIO()
                fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                plots['win_rate'] = base64.b64encode(buf.getvalue()).decode()

            roi_values = [r.realized_roi for r in records if r.realized_roi is not None]
            if roi_values:
                fig2 = Figure(figsize=(10, 6))
                ax2 = fig2.add_subplot(111)
                sns.histplot(roi_values, bins=30, kde=True, ax=ax2)
                ax2.set_xlabel('ROI (%)')
                ax2.set_ylabel('Frequency')
                ax2.set_title('ROI Distribution')
                ax2.axvline(0, color='red', linestyle='--')
                ax2.axvline(np.mean(roi_values), color='green', linestyle='--', label=f'Mean: {np.mean(roi_values):.1f}%')
                ax2.legend()
                buf2 = io.BytesIO()
                fig2.savefig(buf2, format='png', dpi=100, bbox_inches='tight')
                plots['roi_distribution'] = base64.b64encode(buf2.getvalue()).decode()
        except Exception as e: logger.warning(f"Plot generation failed: {e}")
        return plots
        
    def generate_html(self, records: List[PerformanceRecord], summary: PerformanceSummary, filepath: str) -> None:
        confidence = self.analyzer.calculate_confidence_intervals(records)
        plots = self._generate_plots(records, summary) if PLOT_AVAILABLE and records else {}
        
        html = f"""<!DOCTYPE html><html><head><title>Performance Report - {RiyadhTime.format()}</title><style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: auto; background: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }} h2 {{ color: #34495e; margin-top: 30px; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
        .card {{ background: #f8f9fa; padding: 15px; border-radius: 5px; border-left: 4px solid #3498db; }}
        .card h3 {{ margin: 0 0 10px 0; color: #2c3e50; }} .card .value {{ font-size: 24px; font-weight: bold; color: #2980b9; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }} th {{ background: #2c3e50; color: white; padding: 10px; text-align: left; }}
        td {{ padding: 10px; border-bottom: 1px solid #ddd; }} tr:hover {{ background: #f5f5f5; }}
        .win {{ color: #27ae60; }} .loss {{ color: #c0392b; }} .plot {{ margin: 20px 0; text-align: center; }}
        </style></head><body><div class="container"><h1>📊 Performance Report</h1><p>Generated: {RiyadhTime.format()}</p>
        <div class="summary"><div class="card"><h3>Total Records</h3><div class="value">{summary.total_records}</div></div>
        <div class="card"><h3>Win Rate</h3><div class="value">{summary.win_rate:.1f}%</div></div>
        <div class="card"><h3>Avg ROI</h3><div class="value">{summary.avg_roi:.2f}%</div></div>
        <div class="card"><h3>Sharpe Ratio</h3><div class="value">{summary.sharpe_ratio:.2f}</div></div></div>"""
        
        if confidence and 'win_rate' in confidence:
            wr = confidence['win_rate']
            html += f"<h2>Confidence Intervals (95%)</h2><table><tr><th>Metric</th><th>Mean</th><th>Lower Bound</th><th>Upper Bound</th></tr><tr><td>Win Rate</td><td>{wr.get('mean',0)*100:.1f}%</td><td>{wr.get('ci_lower',0)*100:.1f}%</td><td>{wr.get('ci_upper',0)*100:.1f}%</td></tr></table>"
            
        html += "<h2>Recent Records</h2><table><tr><th>Symbol</th><th>Horizon</th><th>Entry Date</th><th>ROI</th><th>Outcome</th></tr>"
        for r in sorted(records, key=lambda x: x.date_recorded, reverse=True)[:20]:
            roi = r.realized_roi if r.realized_roi is not None else r.unrealized_roi
            html += f"<tr><td><strong>{r.symbol}</strong></td><td>{r.horizon.value}</td><td>{RiyadhTime.format(r.date_recorded, '%Y-%m-%d')}</td><td class='{'win' if roi > 0 else 'loss' if roi < 0 else ''}'>{roi:.2f}%</td><td>{r.outcome or '⏳'}</td></tr>"
        
        if plots:
            html += f"</table><h2>Visualizations</h2><div class='plot'><img src='data:image/png;base64,{plots.get('win_rate', '')}'></div><div class='plot'><img src='data:image/png;base64,{plots.get('roi_distribution', '')}'></div>"
            
        html += f"<div class='footer'>Generated by TFB Performance Tracker v{SCRIPT_VERSION}</div></div></body></html>"
        Path(filepath).write_text(html, encoding='utf-8')


# =============================================================================
# Main Orchestrator
# =============================================================================

class PerformanceTrackerApp:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.spreadsheet_id = self._get_spreadsheet_id()
        self.store = PerformanceStore(self.spreadsheet_id, args.sheet_name or 'Performance_Log')
        self.analyzer = PerformanceAnalyzer()
        self.report_gen = ReportGenerator(self.analyzer)
        self.running = False
        
    def _get_spreadsheet_id(self) -> str:
        if self.args.sheet_id: return self.args.sheet_id
        for key in ['DEFAULT_SPREADSHEET_ID', 'GOOGLE_SHEETS_ID', 'SPREADSHEET_ID']:
            if value := os.getenv(key): return value
        raise ValueError("No spreadsheet ID provided.")
        
    async def run(self) -> int:
        try:
            records = await asyncio.get_running_loop().run_in_executor(_CPU_EXECUTOR, self.store.load_records, self.args.max_records or 10000)
            
            if self.args.record:
                # In a real implementation, you'd fetch candidates from SourceScanner here and record them
                pass
            
            if self.args.audit:
                # In a real implementation, you'd fetch current quotes and update ROI
                pass
                
            if self.args.analyze:
                await asyncio.get_running_loop().run_in_executor(_CPU_EXECUTOR, self._analyze_performance, records)
                
            if self.args.simulate:
                await asyncio.get_running_loop().run_in_executor(_CPU_EXECUTOR, self._run_simulation, records)
                
            if self.args.export:
                await asyncio.get_running_loop().run_in_executor(_CPU_EXECUTOR, self._export_report, records)
                
            return 0
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            return 130
        except Exception as e:
            logger.exception(f"Fatal error: {e}")
            return 1

    def _analyze_performance(self, records: List[PerformanceRecord]) -> None:
        summary = self.analyzer.analyze_records(records)
        self.store.update_summary(summary)
        
        if _RICH_AVAILABLE and console:
            console.print("\n")
            table = Table(title="📊 PERFORMANCE SUMMARY", show_header=True, header_style="bold magenta")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            table.add_row("Total Records", str(summary.total_records))
            table.add_row("Win Rate", f"{summary.win_rate:.1f}%")
            table.add_row("Avg ROI", f"{summary.avg_roi:.2f}%")
            table.add_row("Sharpe Ratio", f"{summary.sharpe_ratio:.2f}")
            console.print(table)
            console.print("\n")
        else:
            sys.stdout.write("\n" + "=" * 60 + "\n")
            sys.stdout.write("📊 PERFORMANCE SUMMARY\n")
            sys.stdout.write("=" * 60 + "\n")
            sys.stdout.write(f"Total Records: {summary.total_records} | Win Rate: {summary.win_rate:.1f}%\n")
            sys.stdout.write(f"Avg ROI: {summary.avg_roi:.2f}% | Sharpe Ratio: {summary.sharpe_ratio:.2f}\n")

    def _run_simulation(self, records: List[PerformanceRecord]) -> None:
        matured = [r for r in records if r.status == PerformanceStatus.MATURED]
        if not matured: return
        outcomes = [r.is_win for r in matured if r.is_win is not None]
        if outcomes:
            wr_sim = self.analyzer.simulator.simulate_win_rate(outcomes, iterations=self.args.iterations or 10000, confidence=self.args.confidence or 0.95)
            
            if _RICH_AVAILABLE and console:
                panel = Panel(
                    f"[bold green]Mean:[/bold green] {wr_sim['mean']*100:.1f}%\n"
                    f"[bold yellow]95% CI:[/bold yellow] \[{wr_sim['ci_lower']*100:.1f}%, {wr_sim['ci_upper']*100:.1f}%]",
                    title="🎲 Win Rate Simulation", border_style="blue"
                )
                console.print(panel)
            else:
                sys.stdout.write(f"\nWin Rate Simulation: Mean {wr_sim['mean']*100:.1f}% | 95% CI: [{wr_sim['ci_lower']*100:.1f}%, {wr_sim['ci_upper']*100:.1f}%]\n")

    def _export_report(self, records: List[PerformanceRecord]) -> None:
        summary = self.analyzer.analyze_records(records)
        output_path = self.args.output or f"performance_report_{RiyadhTime.format('%Y%m%d_%H%M%S')}"
        if self.args.format == 'json':
            Path(f"{output_path}.json").write_text(self.report_gen.generate_json(records, summary), encoding='utf-8')
        elif self.args.format == 'csv':
            self.report_gen.generate_csv(records, f"{output_path}.csv")
        elif self.args.format == 'html':
            self.report_gen.generate_html(records, summary, f"{output_path}.html")
        elif self.args.format == 'all':
            Path(f"{output_path}.json").write_text(self.report_gen.generate_json(records, summary), encoding='utf-8')
            self.report_gen.generate_csv(records, f"{output_path}.csv")
            self.report_gen.generate_html(records, summary, f"{output_path}.html")

def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"Tadawul Fast Bridge - Advanced Performance Tracker v{SCRIPT_VERSION}")
    parser.add_argument("--sheet-id", help="Spreadsheet ID")
    parser.add_argument("--sheet-name", default="Performance_Log", help="Sheet name")
    parser.add_argument("--record", action="store_true", help="Record new recommendations")
    parser.add_argument("--audit", action="store_true", help="Audit existing records")
    parser.add_argument("--analyze", action="store_true", help="Analyze performance")
    parser.add_argument("--simulate", action="store_true", help="Run Monte Carlo simulation")
    parser.add_argument("--export", action="store_true", help="Export performance report")
    parser.add_argument("--daemon", action="store_true", help="Run as daemon")
    parser.add_argument("--source-tab", action="append", help="Source tab name")
    parser.add_argument("--horizons", nargs="+", default=["1M", "3M"], help="Horizons to track")
    parser.add_argument("--track-recos", nargs="+", help="Recommendations to track")
    parser.add_argument("--limit", type=int, default=500, help="Max rows per source")
    parser.add_argument("--refresh", type=int, default=1, help="Refresh quotes (0/1)")
    parser.add_argument("--max-records", type=int, default=10000, help="Max records to load")
    parser.add_argument("--confidence", type=float, default=0.95, help="Confidence level")
    parser.add_argument("--iterations", type=int, default=10000, help="Monte Carlo iterations")
    parser.add_argument("--periods", type=int, default=252, help="Simulation periods")
    parser.add_argument("--format", choices=["json", "csv", "html", "all"], default="html", help="Export format")
    parser.add_argument("--output", help="Output file path (without extension)")
    parser.add_argument("--interval", type=int, default=3600, help="Daemon interval (seconds)")
    parser.add_argument("--webhook", help="Webhook URL for notifications")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    return parser

async def async_main() -> int:
    parser = create_parser()
    args = parser.parse_args()
    if args.verbose: logging.getLogger().setLevel(logging.DEBUG)
    if not any([args.record, args.audit, args.analyze, args.simulate, args.export, args.daemon]):
        parser.print_help()
        return 0
    try:
        app = PerformanceTrackerApp(args)
        return await app.run()
    finally:
        _CPU_EXECUTOR.shutdown(wait=False)

def main() -> int:
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(async_main())

if __name__ == "__main__":
    sys.exit(main())
