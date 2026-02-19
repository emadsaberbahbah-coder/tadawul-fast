#!/usr/bin/env python3
"""
track_performance.py
===========================================================
TADAWUL FAST BRIDGE – ADVANCED PERFORMANCE ANALYTICS ENGINE (v3.5.0)
===========================================================
Enterprise-Grade Portfolio Tracking, Risk Analytics, and Performance Attribution

Core Capabilities
-----------------
• Multi-strategy performance tracking (Value, Momentum, Growth, Technical)
• Advanced risk metrics (Sharpe, Sortino, Calmar, Information Ratio)
• Attribution analysis (sector, factor, strategy contribution)
• Monte Carlo simulation for win-rate confidence intervals
• Rolling performance windows (1W, 1M, 3M, 6M, 1Y, 3Y, 5Y)
• Benchmark-relative performance (vs. TASI, MSCI, custom)
• Drawdown analysis with recovery tracking
• Portfolio optimization recommendations
• Export to multiple formats (JSON, CSV, Parquet, HTML report)

Exit Codes
----------
0: Success
1: Configuration error
2: Data fetch error
3: Sheet write error
4: Validation error
5: Resource limit exceeded

Usage Examples
--------------
# Record new signals
python scripts/track_performance.py --record --source Market_Scan --horizons 1M 3M 12M

# Comprehensive audit with risk metrics
python scripts/track_performance.py --audit --risk-metrics --benchmark TASI

# Performance attribution
python scripts/track_performance.py --attribution --by-sector --by-factor

# Monte Carlo simulation for win rates
python scripts/track_performance.py --simulate --iterations 10000 --confidence 0.95

# Export performance report
python scripts/track_performance.py --export --format html --output perf_report.html

# Real-time monitoring daemon
python scripts/track_performance.py --daemon --interval 300 --webhook https://...
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import hashlib
import json
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
from concurrent.futures import ThreadPoolExecutor
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
from urllib.request import Request, urlopen

# =============================================================================
# Version & Core Configuration
# =============================================================================
SCRIPT_VERSION = "3.5.0"
SCRIPT_NAME = "PerformanceTracker"
MIN_PYTHON = (3, 8)

if sys.version_info < MIN_PYTHON:
    sys.exit(f"❌ Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required")

# =============================================================================
# Path Setup
# =============================================================================
def _ensure_project_root_on_path() -> None:
    try:
        script_dir = Path(__file__).parent.absolute()
        project_root = script_dir.parent
        
        for path in [script_dir, project_root]:
            if str(path) not in sys.path:
                sys.path.insert(0, str(path))
    except Exception:
        pass

_ensure_project_root_on_path()

# =============================================================================
# Optional Imports with Graceful Degradation
# =============================================================================
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
    from google.auth.transport.requests import Request
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
    import seaborn as sns
    PLOT_AVAILABLE = True
except ImportError:
    plt = None
    sns = None
    PLOT_AVAILABLE = False

# Optional project imports
settings = None
sheets_service = None
data_engine = None

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
# Logging Configuration
# =============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("PerfTracker")

# =============================================================================
# Enums & Advanced Types
# =============================================================================
class PerformanceStatus(Enum):
    """Performance tracking status"""
    ACTIVE = "active"
    MATURED = "matured"
    EXPIRED = "expired"
    STOPPED = "stopped"
    PENDING = "pending"

class HorizonType(Enum):
    """Tracking horizons"""
    WEEK_1 = "1W"
    MONTH_1 = "1M"
    MONTH_3 = "3M"
    MONTH_6 = "6M"
    YEAR_1 = "1Y"
    YEAR_3 = "3Y"
    YEAR_5 = "5Y"
    
    @property
    def days(self) -> int:
        """Get days for horizon"""
        return {
            "1W": 7,
            "1M": 30,
            "3M": 90,
            "6M": 180,
            "1Y": 365,
            "3Y": 1095,
            "5Y": 1825
        }[self.value]

class RiskMetric(Enum):
    """Risk metrics for analysis"""
    SHARPE = "sharpe"
    SORTINO = "sortino"
    CALMAR = "calmar"
    INFORMATION = "information"
    BETA = "beta"
    ALPHA = "alpha"
    VOLATILITY = "volatility"
    VAR = "value_at_risk"
    CVAR = "conditional_var"
    MAX_DRAWDOWN = "max_drawdown"

class AttributionType(Enum):
    """Performance attribution types"""
    SECTOR = "sector"
    FACTOR = "factor"
    STRATEGY = "strategy"
    MARKET = "market"
    CURRENCY = "currency"
    TIMING = "timing"

class RecommendationType(Enum):
    """Recommendation types"""
    STRONG_BUY = "STRONG BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG SELL"
    
    @property
    def score(self) -> int:
        """Get numeric score"""
        return {
            "STRONG BUY": 5,
            "BUY": 4,
            "HOLD": 3,
            "SELL": 2,
            "STRONG SELL": 1
        }[self.value]

# =============================================================================
# Data Models
# =============================================================================
@dataclass
class PerformanceRecord:
    """Single performance tracking record"""
    # Core identifiers
    record_id: str
    symbol: str
    horizon: HorizonType
    date_recorded: datetime
    
    # Entry data
    entry_price: float
    entry_recommendation: RecommendationType
    entry_score: float
    entry_risk_bucket: str
    entry_confidence: str
    origin_tab: str
    
    # Target data
    target_price: float
    target_roi: float
    target_date: datetime
    
    # Current status
    status: PerformanceStatus
    current_price: float = 0.0
    unrealized_roi: float = 0.0
    realized_roi: Optional[float] = None
    outcome: Optional[str] = None  # WIN, LOSS, BREAKEVEN
    
    # Risk metrics
    volatility: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    
    # Attribution
    sector: Optional[str] = None
    factor_exposures: Dict[str, float] = field(default_factory=dict)
    
    # Timestamps
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    maturity_date: Optional[datetime] = None
    
    @property
    def key(self) -> str:
        """Unique record key"""
        return f"{self.symbol}|{self.horizon.value}|{self.date_recorded.strftime('%Y%m%d')}"
    
    @property
    def days_to_maturity(self) -> int:
        """Days until maturity"""
        if not self.target_date:
            return 0
        delta = self.target_date - datetime.now(self.target_date.tzinfo)
        return max(0, delta.days)
    
    @property
    def is_win(self) -> Optional[bool]:
        """Check if record is a win"""
        if self.realized_roi is None:
            return None
        return self.realized_roi > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'record_id': self.record_id,
            'key': self.key,
            'symbol': self.symbol,
            'horizon': self.horizon.value,
            'date_recorded': self.date_recorded.isoformat(),
            'entry_price': self.entry_price,
            'entry_recommendation': self.entry_recommendation.value,
            'entry_score': self.entry_score,
            'entry_risk_bucket': self.entry_risk_bucket,
            'entry_confidence': self.entry_confidence,
            'origin_tab': self.origin_tab,
            'target_price': self.target_price,
            'target_roi': self.target_roi,
            'target_date': self.target_date.isoformat(),
            'status': self.status.value,
            'current_price': self.current_price,
            'unrealized_roi': self.unrealized_roi,
            'realized_roi': self.realized_roi,
            'outcome': self.outcome,
            'volatility': self.volatility,
            'max_drawdown': self.max_drawdown,
            'sharpe_ratio': self.sharpe_ratio,
            'sector': self.sector,
            'factor_exposures': self.factor_exposures,
            'last_updated': self.last_updated.isoformat(),
            'maturity_date': self.maturity_date.isoformat() if self.maturity_date else None
        }
    
    @classmethod
    def from_sheet_row(cls, row: List[Any], headers: List[str]) -> 'PerformanceRecord':
        """Create record from sheet row"""
        # Map headers to indices
        header_map = {h: i for i, h in enumerate(headers)}
        
        def get(idx_or_key):
            if isinstance(idx_or_key, int):
                return row[idx_or_key] if idx_or_key < len(row) else None
            return row[header_map.get(idx_or_key, -1)] if idx_or_key in header_map else None
        
        # Parse dates
        date_recorded = _parse_iso_date(get('Date Recorded (Riyadh)')) or datetime.now()
        target_date = _parse_iso_date(get('Target Date (Riyadh)')) or datetime.now()
        last_updated = _parse_iso_date(get('Last Updated (Riyadh)')) or datetime.now()
        
        return cls(
            record_id=get('Record ID') or str(uuid.uuid4()),
            symbol=get('Symbol') or '',
            horizon=HorizonType(get('Horizon') or '1M'),
            date_recorded=date_recorded,
            entry_price=float(get('Entry Price') or 0),
            entry_recommendation=RecommendationType(get('Entry Recommendation') or 'HOLD'),
            entry_score=float(get('Entry Score') or 0),
            entry_risk_bucket=get('Risk Bucket') or 'MODERATE',
            entry_confidence=get('Confidence') or 'MEDIUM',
            origin_tab=get('Origin Tab') or 'Unknown',
            target_price=float(get('Target Price') or 0),
            target_roi=float(get('Target ROI %') or 0),
            target_date=target_date,
            status=PerformanceStatus(get('Status') or 'active'),
            current_price=float(get('Current Price') or 0),
            unrealized_roi=float(get('Unrealized ROI %') or 0),
            realized_roi=float(get('Realized ROI %')) if get('Realized ROI %') else None,
            outcome=get('Outcome'),
            volatility=float(get('Volatility') or 0),
            max_drawdown=float(get('Max Drawdown %') or 0),
            sharpe_ratio=float(get('Sharpe Ratio') or 0),
            sector=get('Sector'),
            factor_exposures={},  # Would need to parse JSON
            last_updated=last_updated,
            maturity_date=_parse_iso_date(get('Maturity Date')) if get('Maturity Date') else None
        )

@dataclass
class PerformanceSummary:
    """Aggregated performance summary"""
    total_records: int = 0
    active_records: int = 0
    matured_records: int = 0
    
    # Win/Loss metrics
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    
    # ROI metrics
    avg_roi: float = 0.0
    median_roi: float = 0.0
    roi_std: float = 0.0
    best_roi: float = 0.0
    worst_roi: float = 0.0
    
    # Time-based
    avg_days_to_maturity: float = 0.0
    hit_rate_by_horizon: Dict[str, float] = field(default_factory=dict)
    
    # Risk metrics
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    
    # Attribution
    performance_by_sector: Dict[str, float] = field(default_factory=dict)
    performance_by_strategy: Dict[str, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

@dataclass
class RiskMetrics:
    """Advanced risk metrics"""
    # Return metrics
    total_return: float
    annualized_return: float
    excess_return: float
    
    # Volatility metrics
    volatility: float
    downside_volatility: float
    semi_volatility: float
    
    # Risk ratios
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    information_ratio: float
    
    # Drawdown metrics
    max_drawdown: float
    avg_drawdown: float
    max_drawdown_duration: int
    ulcer_index: float
    
    # Value at Risk
    var_95: float
    var_99: float
    cvar_95: float
    expected_shortfall: float
    
    # Market metrics
    beta: float
    alpha: float
    r_squared: float
    tracking_error: float
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary"""
        return {k: v for k, v in asdict(self).items() if isinstance(v, (int, float))}

# =============================================================================
# Riyadh Timezone Helpers
# =============================================================================
class RiyadhTime:
    """Riyadh timezone utilities"""
    
    _tz = timezone(timedelta(hours=3))
    
    @classmethod
    def now(cls) -> datetime:
        """Get current Riyadh time"""
        return datetime.now(cls._tz)
    
    @classmethod
    def today(cls) -> datetime:
        """Get today's date at midnight"""
        return cls.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    @classmethod
    def parse(cls, date_str: str) -> Optional[datetime]:
        """Parse date string to Riyadh time"""
        formats = [
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S%z',
            '%d/%m/%Y',
            '%d/%m/%Y %H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=cls._tz)
                return dt.astimezone(cls._tz)
            except ValueError:
                continue
        return None
    
    @classmethod
    def format(cls, dt: Optional[datetime] = None, fmt: str = '%Y-%m-%d %H:%M:%S') -> str:
        """Format datetime"""
        dt = dt or cls.now()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=cls._tz)
        return dt.astimezone(cls._tz).strftime(fmt)

def _parse_iso_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO date string"""
    if not date_str:
        return None
    return RiyadhTime.parse(date_str)

# =============================================================================
# Advanced Risk Calculator
# =============================================================================
class RiskCalculator:
    """Advanced risk metrics calculator"""
    
    def __init__(self, risk_free_rate: float = 0.02):
        self.risk_free_rate = risk_free_rate
    
    def calculate_returns(self, prices: List[float]) -> np.ndarray:
        """Calculate returns from prices"""
        if not NUMPY_AVAILABLE:
            return np.array([])
        
        prices_array = np.array(prices)
        returns = np.diff(prices_array) / prices_array[:-1]
        return returns
    
    def sharpe_ratio(self, returns: Union[List[float], np.ndarray]) -> float:
        """Calculate Sharpe ratio"""
        if not NUMPY_AVAILABLE or len(returns) == 0:
            return 0.0
        
        returns_array = np.array(returns)
        excess_returns = returns_array - self.risk_free_rate / 252
        if np.std(returns_array) == 0:
            return 0.0
        
        return float(np.mean(excess_returns) / np.std(returns_array) * np.sqrt(252))
    
    def sortino_ratio(self, returns: Union[List[float], np.ndarray]) -> float:
        """Calculate Sortino ratio"""
        if not NUMPY_AVAILABLE or len(returns) == 0:
            return 0.0
        
        returns_array = np.array(returns)
        excess_returns = returns_array - self.risk_free_rate / 252
        downside_returns = returns_array[returns_array < 0]
        
        if len(downside_returns) == 0 or np.std(downside_returns) == 0:
            return 0.0
        
        return float(np.mean(excess_returns) / np.std(downside_returns) * np.sqrt(252))
    
    def calmar_ratio(self, returns: Union[List[float], np.ndarray], 
                     max_drawdown: float) -> float:
        """Calculate Calmar ratio"""
        if not NUMPY_AVAILABLE or len(returns) == 0 or max_drawdown == 0:
            return 0.0
        
        annualized_return = np.mean(returns) * 252
        return float(annualized_return / abs(max_drawdown))
    
    def max_drawdown(self, prices: Union[List[float], np.ndarray]) -> Dict[str, float]:
        """Calculate maximum drawdown and related metrics"""
        if not NUMPY_AVAILABLE or len(prices) == 0:
            return {'max_drawdown': 0.0, 'duration': 0, 'recovery': 0}
        
        prices_array = np.array(prices)
        running_max = np.maximum.accumulate(prices_array)
        drawdown = (prices_array - running_max) / running_max
        
        max_drawdown = np.min(drawdown)
        max_drawdown_idx = np.argmin(drawdown)
        
        # Calculate duration
        peak_idx = np.argmax(prices_array[:max_drawdown_idx + 1])
        recovery_idx = max_drawdown_idx + 1
        while recovery_idx < len(prices_array) and prices_array[recovery_idx] < prices_array[peak_idx]:
            recovery_idx += 1
        
        duration = max_drawdown_idx - peak_idx
        recovery_time = recovery_idx - max_drawdown_idx if recovery_idx < len(prices_array) else -1
        
        return {
            'max_drawdown': float(max_drawdown),
            'duration': int(duration),
            'recovery_time': int(recovery_time) if recovery_time > 0 else -1
        }
    
    def value_at_risk(self, returns: Union[List[float], np.ndarray], 
                     confidence: float = 0.95) -> Dict[str, float]:
        """Calculate Value at Risk (VaR) and Conditional VaR"""
        if not NUMPY_AVAILABLE or len(returns) == 0:
            return {'var': 0.0, 'cvar': 0.0}
        
        returns_array = np.array(returns)
        var = np.percentile(returns_array, (1 - confidence) * 100)
        cvar = np.mean(returns_array[returns_array <= var])
        
        return {'var': float(var), 'cvar': float(cvar)}
    
    def beta_alpha(self, returns: Union[List[float], np.ndarray],
                   benchmark_returns: Union[List[float], np.ndarray]) -> Dict[str, float]:
        """Calculate beta and alpha relative to benchmark"""
        if not SCIPY_AVAILABLE or len(returns) == 0 or len(benchmark_returns) == 0:
            return {'beta': 1.0, 'alpha': 0.0, 'r_squared': 0.0}
        
        returns_array = np.array(returns)
        benchmark_array = np.array(benchmark_returns[:len(returns)])
        
        # Linear regression
        slope, intercept, r_value, _, _ = stats.linregress(benchmark_array, returns_array)
        
        return {
            'beta': float(slope),
            'alpha': float(intercept * 252),  # Annualized
            'r_squared': float(r_value ** 2)
        }
    
    def all_metrics(self, prices: List[float], 
                   benchmark_prices: Optional[List[float]] = None) -> RiskMetrics:
        """Calculate all risk metrics"""
        if not NUMPY_AVAILABLE or len(prices) < 2:
            return RiskMetrics(
                total_return=0.0, annualized_return=0.0, excess_return=0.0,
                volatility=0.0, downside_volatility=0.0, semi_volatility=0.0,
                sharpe_ratio=0.0, sortino_ratio=0.0, calmar_ratio=0.0,
                information_ratio=0.0, max_drawdown=0.0, avg_drawdown=0.0,
                max_drawdown_duration=0, ulcer_index=0.0,
                var_95=0.0, var_99=0.0, cvar_95=0.0, expected_shortfall=0.0,
                beta=1.0, alpha=0.0, r_squared=0.0, tracking_error=0.0
            )
        
        returns = self.calculate_returns(prices)
        
        # Basic metrics
        total_return = (prices[-1] / prices[0]) - 1
        annualized_return = (1 + total_return) ** (252 / len(returns)) - 1
        
        # Volatility metrics
        volatility = np.std(returns) * np.sqrt(252)
        downside_returns = returns[returns < 0]
        downside_volatility = np.std(downside_returns) * np.sqrt(252) if len(downside_returns) > 0 else 0
        
        # Drawdown metrics
        dd_metrics = self.max_drawdown(prices)
        
        # VaR metrics
        var_95 = self.value_at_risk(returns, 0.95)
        var_99 = self.value_at_risk(returns, 0.99)
        
        # Risk ratios
        sharpe = self.sharpe_ratio(returns)
        sortino = self.sortino_ratio(returns)
        calmar = self.calmar_ratio(returns, dd_metrics['max_drawdown'])
        
        # Benchmark comparison
        beta = 1.0
        alpha = 0.0
        r_squared = 0.0
        tracking_error = 0.0
        information_ratio = 0.0
        
        if benchmark_prices is not None and len(benchmark_prices) >= len(prices):
            bench_returns = self.calculate_returns(benchmark_prices[:len(prices)])
            ba = self.beta_alpha(returns, bench_returns)
            beta = ba['beta']
            alpha = ba['alpha']
            r_squared = ba['r_squared']
            
            tracking_error = np.std(returns - bench_returns) * np.sqrt(252)
            excess_returns = annualized_return - (np.mean(bench_returns) * 252)
            information_ratio = excess_returns / tracking_error if tracking_error > 0 else 0
        
        return RiskMetrics(
            total_return=float(total_return),
            annualized_return=float(annualized_return),
            excess_return=float(annualized_return - self.risk_free_rate),
            volatility=float(volatility),
            downside_volatility=float(downside_volatility),
            semi_volatility=float(np.std(returns[returns < np.mean(returns)]) * np.sqrt(252)),
            sharpe_ratio=float(sharpe),
            sortino_ratio=float(sortino),
            calmar_ratio=float(calmar),
            information_ratio=float(information_ratio),
            max_drawdown=float(dd_metrics['max_drawdown']),
            avg_drawdown=0.0,  # Would need rolling calculation
            max_drawdown_duration=int(dd_metrics['duration']),
            ulcer_index=0.0,  # Would need rolling calculation
            var_95=float(var_95['var']),
            var_99=float(var_99['var']),
            cvar_95=float(var_95['cvar']),
            expected_shortfall=float(var_99['cvar']),
            beta=float(beta),
            alpha=float(alpha),
            r_squared=float(r_squared),
            tracking_error=float(tracking_error)
        )

# =============================================================================
# Monte Carlo Simulator
# =============================================================================
class MonteCarloSimulator:
    """Monte Carlo simulation for win rates and performance"""
    
    def __init__(self, seed: Optional[int] = None):
        self.seed = seed
        if seed is not None and NUMPY_AVAILABLE:
            np.random.seed(seed)
    
    def simulate_win_rate(self, historical_outcomes: List[bool], 
                          iterations: int = 10000,
                          confidence: float = 0.95) -> Dict[str, float]:
        """Simulate win rate distribution"""
        if not NUMPY_AVAILABLE or not historical_outcomes:
            return {
                'mean': 0.0,
                'median': 0.0,
                'std': 0.0,
                'ci_lower': 0.0,
                'ci_upper': 0.0
            }
        
        n = len(historical_outcomes)
        wins = sum(historical_outcomes)
        observed_rate = wins / n
        
        # Bootstrap sampling
        simulated_rates = []
        for _ in range(iterations):
            sample = np.random.choice(historical_outcomes, size=n, replace=True)
            simulated_rates.append(np.mean(sample))
        
        simulated_rates = np.array(simulated_rates)
        ci_lower = np.percentile(simulated_rates, (1 - confidence) / 2 * 100)
        ci_upper = np.percentile(simulated_rates, (1 + confidence) / 2 * 100)
        
        return {
            'mean': float(np.mean(simulated_rates)),
            'median': float(np.median(simulated_rates)),
            'std': float(np.std(simulated_rates)),
            'ci_lower': float(ci_lower),
            'ci_upper': float(ci_upper),
            'observed': float(observed_rate)
        }
    
    def simulate_returns(self, historical_returns: List[float],
                         periods: int = 252,
                         iterations: int = 10000) -> Dict[str, Any]:
        """Simulate future returns distribution"""
        if not NUMPY_AVAILABLE or not historical_returns:
            return {}
        
        returns_array = np.array(historical_returns)
        mu = np.mean(returns_array)
        sigma = np.std(returns_array)
        
        # Geometric Brownian Motion simulation
        simulated_paths = []
        for _ in range(iterations):
            eps = np.random.normal(0, 1, periods)
            path_returns = mu + sigma * eps
            simulated_paths.append(np.exp(np.cumsum(path_returns)))
        
        simulated_paths = np.array(simulated_paths)
        
        # Calculate statistics
        final_values = simulated_paths[:, -1]
        
        return {
            'mean_final': float(np.mean(final_values)),
            'median_final': float(np.median(final_values)),
            'std_final': float(np.std(final_values)),
            'ci_lower': float(np.percentile(final_values, 2.5)),
            'ci_upper': float(np.percentile(final_values, 97.5)),
            'prob_loss': float(np.mean(final_values < 1)),
            'prob_double': float(np.mean(final_values > 2))
        }

# =============================================================================
# Performance Attribution
# =============================================================================
class AttributionAnalyzer:
    """Performance attribution analysis"""
    
    def __init__(self):
        self.factors = {
            'value': ['P/E', 'P/B', 'EV/EBITDA'],
            'momentum': ['RSI', 'MACD', 'Price Momentum'],
            'quality': ['ROE', 'ROA', 'Profit Margins'],
            'growth': ['Revenue Growth', 'EPS Growth'],
            'size': ['Market Cap'],
            'volatility': ['Beta', 'Volatility']
        }
    
    def by_sector(self, records: List[PerformanceRecord]) -> Dict[str, Dict[str, float]]:
        """Attribution by sector"""
        sector_performance = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_roi': 0.0})
        
        for r in records:
            if r.sector and r.realized_roi is not None:
                sec = sector_performance[r.sector]
                sec['total_roi'] += r.realized_roi
                if r.is_win:
                    sec['wins'] += 1
                else:
                    sec['losses'] += 1
        
        results = {}
        for sector, data in sector_performance.items():
            total = data['wins'] + data['losses']
            results[sector] = {
                'win_rate': (data['wins'] / total * 100) if total > 0 else 0,
                'avg_roi': data['total_roi'] / total if total > 0 else 0,
                'wins': data['wins'],
                'losses': data['losses']
            }
        
        return results
    
    def by_horizon(self, records: List[PerformanceRecord]) -> Dict[str, Dict[str, float]]:
        """Attribution by horizon"""
        horizon_performance = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_roi': 0.0})
        
        for r in records:
            if r.realized_roi is not None:
                h = horizon_performance[r.horizon.value]
                h['total_roi'] += r.realized_roi
                if r.is_win:
                    h['wins'] += 1
                else:
                    h['losses'] += 1
        
        results = {}
        for horizon, data in horizon_performance.items():
            total = data['wins'] + data['losses']
            results[horizon] = {
                'win_rate': (data['wins'] / total * 100) if total > 0 else 0,
                'avg_roi': data['total_roi'] / total if total > 0 else 0,
                'wins': data['wins'],
                'losses': data['losses']
            }
        
        return results
    
    def by_recommendation(self, records: List[PerformanceRecord]) -> Dict[str, Dict[str, float]]:
        """Attribution by recommendation type"""
        rec_performance = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_roi': 0.0})
        
        for r in records:
            if r.realized_roi is not None:
                rec = rec_performance[r.entry_recommendation.value]
                rec['total_roi'] += r.realized_roi
                if r.is_win:
                    rec['wins'] += 1
                else:
                    rec['losses'] += 1
        
        results = {}
        for rec, data in rec_performance.items():
            total = data['wins'] + data['losses']
            results[rec] = {
                'win_rate': (data['wins'] / total * 100) if total > 0 else 0,
                'avg_roi': data['total_roi'] / total if total > 0 else 0,
                'wins': data['wins'],
                'losses': data['losses']
            }
        
        return results

# =============================================================================
# Data Store (Google Sheets with caching)
# =============================================================================
class PerformanceStore:
    """Performance data store with caching and retry logic"""
    
    # Headers for Performance_Log sheet
    HEADERS = [
        'Record ID',
        'Key',
        'Symbol',
        'Horizon',
        'Date Recorded (Riyadh)',
        'Entry Price',
        'Entry Recommendation',
        'Entry Score',
        'Risk Bucket',
        'Confidence',
        'Origin Tab',
        'Target Price',
        'Target ROI %',
        'Target Date (Riyadh)',
        'Status',
        'Current Price',
        'Unrealized ROI %',
        'Realized ROI %',
        'Outcome',
        'Volatility',
        'Max Drawdown %',
        'Sharpe Ratio',
        'Sector',
        'Factor Exposures',
        'Last Updated (Riyadh)',
        'Maturity Date',
        'Notes'
    ]
    
    # Summary block at top (A1:E5)
    SUMMARY_HEADERS = [
        'Metric', 'Value', 'vs Benchmark', 'Status', 'Last Updated'
    ]
    
    def __init__(self, spreadsheet_id: str, sheet_name: str = 'Performance_Log'):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.cache: Dict[str, PerformanceRecord] = {}
        self.cache_lock = Lock()
        self.last_sync: Optional[datetime] = None
        
        # Initialize connection
        self._init_sheet()
    
    def _init_sheet(self) -> None:
        """Initialize sheet connection and ensure headers"""
        if not GSPREAD_AVAILABLE:
            logger.warning("gspread not available, using fallback mode")
            return
        
        try:
            # Try to get credentials from env
            creds_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
            if creds_json:
                # Handle base64 encoded credentials
                try:
                    decoded = base64.b64decode(creds_json).decode('utf-8')
                    if decoded.startswith('{'):
                        creds_json = decoded
                except:
                    pass
                
                creds_dict = json.loads(creds_json)
                credentials = service_account.Credentials.from_service_account_info(
                    creds_dict,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
            else:
                # Try default credentials
                credentials = None
            
            self.gc = gspread.authorize(credentials) if credentials else gspread.service_account()
            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            
            # Get or create worksheet
            try:
                self.worksheet = self.sheet.worksheet(self.sheet_name)
            except gspread.WorksheetNotFound:
                self.worksheet = self.sheet.add_worksheet(title=self.sheet_name, rows=1000, cols=50)
            
            # Ensure headers
            self._ensure_headers()
            
        except Exception as e:
            logger.error(f"Sheet initialization failed: {e}")
            if os.getenv('STRICT') == '1':
                raise
            self.gc = None
    
    def _ensure_headers(self) -> None:
        """Ensure headers exist at row 5"""
        if not hasattr(self, 'worksheet'):
            return
        
        try:
            # Check if headers exist at row 5
            headers = self.worksheet.row_values(5)
            if not headers or headers[0] != self.HEADERS[0]:
                # Write headers
                self.worksheet.update(f'A5:{chr(64+len(self.HEADERS))}5', [self.HEADERS])
                
                # Add summary block at top
                summary_data = [
                    ['Performance Summary', '', '', f'Generated: {RiyadhTime.format()}', ''],
                    ['Total Records', '0', '', '', ''],
                    ['Win Rate', '0%', '', '', ''],
                    ['Avg ROI', '0%', '', '', ''],
                    ['Sharpe Ratio', '0', '', '', '']
                ]
                self.worksheet.update('A1:E5', summary_data)
                
                # Freeze headers and apply formatting
                self.worksheet.freeze(rows=5)
                
        except Exception as e:
            logger.warning(f"Header initialization failed: {e}")
    
    def load_records(self, max_records: int = 10000) -> List[PerformanceRecord]:
        """Load records from sheet"""
        if not hasattr(self, 'worksheet'):
            return []
        
        try:
            # Get all records from A6 downwards
            records_data = self.worksheet.get(f'A6:{chr(64+len(self.HEADERS))}{6+max_records}')
            
            records = []
            with self.cache_lock:
                self.cache.clear()
                
                for row in records_data:
                    if not row or not row[0]:  # Skip empty rows
                        continue
                    
                    # Pad row to match headers length
                    padded_row = row + [''] * (len(self.HEADERS) - len(row))
                    
                    # Convert to PerformanceRecord
                    record = PerformanceRecord.from_sheet_row(padded_row, self.HEADERS)
                    records.append(record)
                    self.cache[record.key] = record
                
                self.last_sync = RiyadhTime.now()
            
            logger.info(f"Loaded {len(records)} performance records")
            return records
            
        except Exception as e:
            logger.error(f"Failed to load records: {e}")
            return []
    
    def save_records(self, records: List[PerformanceRecord]) -> bool:
        """Save records to sheet"""
        if not hasattr(self, 'worksheet'):
            return False
        
        try:
            # Prepare data for writing
            data = []
            for r in records:
                row = [
                    r.record_id,
                    r.key,
                    r.symbol,
                    r.horizon.value,
                    RiyadhTime.format(r.date_recorded),
                    r.entry_price,
                    r.entry_recommendation.value,
                    r.entry_score,
                    r.entry_risk_bucket,
                    r.entry_confidence,
                    r.origin_tab,
                    r.target_price,
                    r.target_roi,
                    RiyadhTime.format(r.target_date),
                    r.status.value,
                    r.current_price,
                    r.unrealized_roi,
                    r.realized_roi or '',
                    r.outcome or '',
                    r.volatility,
                    r.max_drawdown,
                    r.sharpe_ratio,
                    r.sector or '',
                    json.dumps(r.factor_exposures),
                    RiyadhTime.format(r.last_updated),
                    RiyadhTime.format(r.maturity_date) if r.maturity_date else '',
                    ''  # Notes
                ]
                data.append(row)
            
            # Clear existing data and write new
            self.worksheet.batch_clear(['A6:Z10000'])
            if data:
                self.worksheet.update(f'A6:{chr(64+len(self.HEADERS))}{6+len(data)-1}', data)
            
            # Update cache
            with self.cache_lock:
                for r in records:
                    self.cache[r.key] = r
                self.last_sync = RiyadhTime.now()
            
            logger.info(f"Saved {len(records)} performance records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save records: {e}")
            return False
    
    def append_records(self, records: List[PerformanceRecord]) -> bool:
        """Append new records"""
        if not hasattr(self, 'worksheet'):
            return False
        
        try:
            # Check for duplicates
            existing_keys = set(self.cache.keys())
            new_records = [r for r in records if r.key not in existing_keys]
            
            if not new_records:
                logger.info("No new records to append")
                return True
            
            # Prepare data
            data = []
            for r in new_records:
                row = [
                    r.record_id,
                    r.key,
                    r.symbol,
                    r.horizon.value,
                    RiyadhTime.format(r.date_recorded),
                    r.entry_price,
                    r.entry_recommendation.value,
                    r.entry_score,
                    r.entry_risk_bucket,
                    r.entry_confidence,
                    r.origin_tab,
                    r.target_price,
                    r.target_roi,
                    RiyadhTime.format(r.target_date),
                    r.status.value,
                    r.current_price,
                    r.unrealized_roi,
                    r.realized_roi or '',
                    r.outcome or '',
                    r.volatility,
                    r.max_drawdown,
                    r.sharpe_ratio,
                    r.sector or '',
                    json.dumps(r.factor_exposures),
                    RiyadhTime.format(r.last_updated),
                    RiyadhTime.format(r.maturity_date) if r.maturity_date else '',
                    ''  # Notes
                ]
                data.append(row)
            
            # Append to sheet
            self.worksheet.append_rows(data, value_input_option='RAW')
            
            # Update cache
            with self.cache_lock:
                for r in new_records:
                    self.cache[r.key] = r
            
            logger.info(f"Appended {len(new_records)} new records")
            return True
            
        except Exception as e:
            logger.error(f"Failed to append records: {e}")
            return False
    
    def update_summary(self, summary: PerformanceSummary) -> bool:
        """Update summary block"""
        if not hasattr(self, 'worksheet'):
            return False
        
        try:
            summary_data = [
                ['Performance Summary', '', '', f'Updated: {RiyadhTime.format()}', ''],
                ['Total Records', summary.total_records, '', '', ''],
                ['Active Records', summary.active_records, '', '', ''],
                ['Matured Records', summary.matured_records, '', '', ''],
                ['Wins', summary.wins, '', '', ''],
                ['Losses', summary.losses, '', '', ''],
                ['Win Rate', f"{summary.win_rate:.1f}%", '', '', ''],
                ['Avg ROI', f"{summary.avg_roi:.2f}%", '', '', ''],
                ['Median ROI', f"{summary.median_roi:.2f}%", '', '', ''],
                ['Best ROI', f"{summary.best_roi:.2f}%", '', '', ''],
                ['Worst ROI', f"{summary.worst_roi:.2f}%", '', '', ''],
                ['Sharpe Ratio', f"{summary.sharpe_ratio:.2f}", '', '', ''],
                ['Sortino Ratio', f"{summary.sortino_ratio:.2f}", '', '', ''],
                ['Max Drawdown', f"{summary.max_drawdown:.2f}%", '', '', '']
            ]
            
            self.worksheet.update('A1:E15', summary_data)
            return True
            
        except Exception as e:
            logger.error(f"Failed to update summary: {e}")
            return False

# =============================================================================
# Quote Fetcher (Async with fallbacks)
# =============================================================================
class QuoteFetcher:
    """Async quote fetcher with multiple backends"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Tuple[float, datetime]] = {}
        self.cache_duration = 300  # 5 minutes
        self._lock = asyncio.Lock()
    
    async def __aenter__(self):
        if ASYNC_HTTP_AVAILABLE:
            self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def fetch_quotes(self, symbols: List[str], 
                          refresh: bool = False) -> Dict[str, Dict[str, Any]]:
        """Fetch quotes for symbols"""
        if not symbols:
            return {}
        
        # Filter cached symbols
        to_fetch = []
        result = {}
        
        async with self._lock:
            now = datetime.now()
            for sym in symbols:
                if not refresh and sym in self.cache:
                    price, ts = self.cache[sym]
                    if (now - ts).total_seconds() < self.cache_duration:
                        result[sym] = {'current_price': price}
                        continue
                to_fetch.append(sym)
        
        if not to_fetch:
            return result
        
        # Try data engine first
        if data_engine:
            engine_result = await self._fetch_from_engine(to_fetch, refresh)
            result.update(engine_result)
            to_fetch = [s for s in to_fetch if s not in engine_result]
        
        # Try HTTP backend
        if to_fetch:
            http_result = await self._fetch_from_http(to_fetch)
            result.update(http_result)
        
        # Update cache
        async with self._lock:
            for sym, data in result.items():
                if 'current_price' in data:
                    self.cache[sym] = (data['current_price'], datetime.now())
        
        return result
    
    async def _fetch_from_engine(self, symbols: List[str], 
                                refresh: bool) -> Dict[str, Dict[str, Any]]:
        """Fetch from data engine"""
        try:
            engine = await data_engine()
            result = {}
            
            for sym in symbols:
                try:
                    if hasattr(engine, 'get_enriched_quote'):
                        quote = await engine.get_enriched_quote(sym, refresh=refresh)
                    else:
                        quote = await engine.get_quote(sym, refresh=refresh)
                    
                    if quote:
                        if isinstance(quote, tuple):
                            quote = quote[0]
                        
                        price = None
                        if isinstance(quote, dict):
                            price = quote.get('current_price') or quote.get('price')
                        elif hasattr(quote, 'current_price'):
                            price = quote.current_price
                        
                        if price:
                            result[sym] = {'current_price': float(price)}
                            
                except Exception as e:
                    logger.debug(f"Engine fetch failed for {sym}: {e}")
            
            return result
            
        except Exception as e:
            logger.warning(f"Data engine error: {e}")
            return {}
    
    async def _fetch_from_http(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch from HTTP backend"""
        if not ASYNC_HTTP_AVAILABLE or not self.session:
            return {}
        
        try:
            base_url = os.getenv('BACKEND_BASE_URL', 'http://localhost:8000').rstrip('/')
            token = os.getenv('APP_TOKEN', '')
            
            # Batch fetch
            params = [('tickers', sym) for sym in symbols]
            url = f"{base_url}/v1/enriched/quotes?{urlencode(params, doseq=True)}"
            
            headers = {'User-Agent': f'PerfTracker/{SCRIPT_VERSION}'}
            if token:
                headers['X-APP-TOKEN'] = token
            
            async with self.session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get('items') or data.get('results') or []
                    
                    result = {}
                    for item in items:
                        if isinstance(item, dict):
                            sym = item.get('symbol') or item.get('ticker')
                            price = item.get('current_price') or item.get('price')
                            if sym and price:
                                result[sym] = {'current_price': float(price)}
                    
                    return result
                else:
                    logger.warning(f"HTTP fetch returned {resp.status}")
                    return {}
                    
        except Exception as e:
            logger.warning(f"HTTP fetch failed: {e}")
            return {}

# =============================================================================
# Source Scanner (reads recommendations from source tabs)
# =============================================================================
class SourceScanner:
    """Scans source tabs for recommendations"""
    
    # Common header mappings
    HEADER_MAPPINGS = {
        'symbol': ['symbol', 'ticker', 'code'],
        'recommendation': ['recommendation', 'rec', 'signal', 'action'],
        'price': ['price', 'current price', 'last price'],
        'score': ['score', 'advisor score', 'overall score', 'opportunity score'],
        'risk': ['risk', 'risk bucket', 'risk level'],
        'confidence': ['confidence', 'confidence bucket'],
        'sector': ['sector', 'industry'],
        'roi_1w': ['roi 1w', 'expected roi 1w', '1w return'],
        'roi_1m': ['roi 1m', 'expected roi 1m', '1m return'],
        'roi_3m': ['roi 3m', 'expected roi 3m', '3m return'],
        'roi_6m': ['roi 6m', 'expected roi 6m', '6m return'],
        'roi_1y': ['roi 1y', 'expected roi 1y', '1y return'],
        'fp_1w': ['fp 1w', 'forecast 1w', 'target 1w'],
        'fp_1m': ['fp 1m', 'forecast 1m', 'target 1m'],
        'fp_3m': ['fp 3m', 'forecast 3m', 'target 3m'],
        'fp_6m': ['fp 6m', 'forecast 6m', 'target 6m'],
        'fp_1y': ['fp 1y', 'forecast 1y', 'target 1y'],
    }
    
    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.sheets_service = sheets_service
    
    def scan_tab(self, tab_name: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Scan a single tab for recommendations"""
        try:
            # Read headers (row 5)
            header_range = f"'{tab_name}'!A5:ZZ5"
            headers_raw = sheets_service.read_range(self.spreadsheet_id, header_range)
            
            if not headers_raw or not headers_raw[0]:
                logger.warning(f"No headers found in {tab_name}")
                return []
            
            headers = [str(h).strip().lower() for h in headers_raw[0]]
            
            # Build header map
            header_map = {}
            for i, h in enumerate(headers):
                if h:
                    header_map[h] = i
            
            # Find column indices
            col_indices = {}
            for key, aliases in self.HEADER_MAPPINGS.items():
                for alias in aliases:
                    if alias in header_map:
                        col_indices[key] = header_map[alias]
                        break
            
            if 'symbol' not in col_indices:
                logger.warning(f"Symbol column not found in {tab_name}")
                return []
            
            # Read data (row 6 onwards)
            data_range = f"'{tab_name}'!A6:{chr(64+len(headers))}{6+limit}"
            data = sheets_service.read_range(self.spreadsheet_id, data_range)
            
            if not data:
                return []
            
            # Extract recommendations
            recommendations = []
            for row in data:
                if not row or len(row) <= col_indices['symbol']:
                    continue
                
                symbol = str(row[col_indices['symbol']]).strip().upper()
                if not symbol or symbol in ['SYMBOL', 'TICKER', '']:
                    continue
                
                rec = {}
                for key, idx in col_indices.items():
                    if idx < len(row):
                        value = row[idx]
                        if key in ['roi_1w', 'roi_1m', 'roi_3m', 'roi_6m', 'roi_1y']:
                            rec[key] = self._parse_percent(value)
                        elif key in ['fp_1w', 'fp_1m', 'fp_3m', 'fp_6m', 'fp_1y']:
                            rec[key] = self._parse_float(value)
                        else:
                            rec[key] = str(value).strip() if value else ''
                
                if 'recommendation' in rec:
                    rec['recommendation'] = rec['recommendation'].upper().replace('_', ' ')
                
                rec['origin_tab'] = tab_name
                recommendations.append(rec)
            
            logger.info(f"Found {len(recommendations)} recommendations in {tab_name}")
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to scan {tab_name}: {e}")
            return []
    
    def _parse_float(self, value: Any) -> float:
        """Parse float value"""
        try:
            if value is None:
                return 0.0
            s = str(value).strip().replace(',', '')
            if not s:
                return 0.0
            return float(s)
        except:
            return 0.0
    
    def _parse_percent(self, value: Any) -> float:
        """Parse percentage value"""
        try:
            if value is None:
                return 0.0
            s = str(value).strip().replace('%', '').replace(',', '')
            if not s:
                return 0.0
            f = float(s)
            # Convert decimal to percent if needed
            if abs(f) <= 1.5 and f != 0:
                return f * 100.0
            return f
        except:
            return 0.0

# =============================================================================
# Performance Analyzer
# =============================================================================
class PerformanceAnalyzer:
    """Advanced performance analysis engine"""
    
    def __init__(self):
        self.risk_calc = RiskCalculator()
        self.attribution = AttributionAnalyzer()
        self.simulator = MonteCarloSimulator(seed=42)
    
    def analyze_records(self, records: List[PerformanceRecord]) -> PerformanceSummary:
        """Analyze performance records"""
        summary = PerformanceSummary()
        
        # Filter matured records
        matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
        active = [r for r in records if r.status == PerformanceStatus.ACTIVE]
        
        summary.total_records = len(records)
        summary.active_records = len(active)
        summary.matured_records = len(matured)
        
        if matured:
            # Win/Loss metrics
            roi_values = []
            for r in matured:
                roi_values.append(r.realized_roi or 0)
                if r.is_win:
                    summary.wins += 1
                elif r.is_win is False:
                    summary.losses += 1
                else:
                    summary.breakeven += 1
            
            # ROI statistics
            if NUMPY_AVAILABLE:
                roi_array = np.array(roi_values)
                summary.avg_roi = float(np.mean(roi_array))
                summary.median_roi = float(np.median(roi_array))
                summary.roi_std = float(np.std(roi_array))
                summary.best_roi = float(np.max(roi_array))
                summary.worst_roi = float(np.min(roi_array))
            else:
                summary.avg_roi = sum(roi_values) / len(roi_values)
                roi_values.sort()
                summary.median_roi = roi_values[len(roi_values)//2]
                summary.best_roi = max(roi_values)
                summary.worst_roi = min(roi_values)
            
            # Win rate
            total_outcomes = summary.wins + summary.losses
            summary.win_rate = (summary.wins / total_outcomes * 100) if total_outcomes > 0 else 0
            
            # Risk metrics
            if len(roi_values) > 1:
                # For risk metrics, we'd need price history
                # Using ROI as returns proxy
                summary.sharpe_ratio = self.risk_calc.sharpe_ratio(roi_values)
                summary.sortino_ratio = self.risk_calc.sortino_ratio(roi_values)
            
            # Hit rate by horizon
            horizon_groups = defaultdict(list)
            for r in matured:
                horizon_groups[r.horizon.value].append(r)
            
            for horizon, group in horizon_groups.items():
                wins = sum(1 for r in group if r.is_win)
                total = len(group)
                summary.hit_rate_by_horizon[horizon] = (wins / total * 100) if total > 0 else 0
            
            # Attribution
            summary.performance_by_sector = self.attribution.by_sector(matured)
            summary.performance_by_strategy = {}  # Would need strategy mapping
        
        return summary
    
    def calculate_confidence_intervals(self, records: List[PerformanceRecord],
                                      confidence: float = 0.95) -> Dict[str, Any]:
        """Calculate confidence intervals for key metrics"""
        matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
        
        if not matured or not NUMPY_AVAILABLE:
            return {}
        
        outcomes = [r.is_win for r in matured if r.is_win is not None]
        roi_values = [r.realized_roi for r in matured if r.realized_roi is not None]
        
        results = {}
        
        # Win rate confidence
        if outcomes:
            results['win_rate'] = self.simulator.simulate_win_rate(
                outcomes, iterations=10000, confidence=confidence
            )
        
        # ROI confidence
        if roi_values and NUMPY_AVAILABLE:
            roi_array = np.array(roi_values)
            mean_roi = np.mean(roi_array)
            std_roi = np.std(roi_array)
            n = len(roi_array)
            
            # Normal confidence interval
            if SCIPY_AVAILABLE:
                z = stats.norm.ppf((1 + confidence) / 2)
            else:
                z = 1.96  # Approx for 95% confidence
            
            margin = z * std_roi / np.sqrt(n)
            
            results['roi'] = {
                'mean': float(mean_roi),
                'std': float(std_roi),
                'ci_lower': float(mean_roi - margin),
                'ci_upper': float(mean_roi + margin),
                'n': n
            }
        
        return results
    
    def rolling_performance(self, records: List[PerformanceRecord],
                          window_days: int = 30) -> Dict[str, List[float]]:
        """Calculate rolling performance metrics"""
        matured = [r for r in records if r.status == PerformanceStatus.MATURED and r.realized_roi is not None]
        
        if not matured:
            return {}
        
        # Sort by date
        matured.sort(key=lambda x: x.maturity_date or x.date_recorded)
        
        dates = []
        rolling_wins = []
        rolling_win_rate = []
        rolling_roi = []
        
        window = timedelta(days=window_days)
        current = matured[0].date_recorded
        
        while current <= matured[-1].date_recorded:
            window_start = current - window
            window_records = [r for r in matured 
                            if (r.maturity_date or r.date_recorded) >= window_start
                            and (r.maturity_date or r.date_recorded) <= current]
            
            if window_records:
                wins = sum(1 for r in window_records if r.is_win)
                total = len(window_records)
                roi = np.mean([r.realized_roi for r in window_records]) if NUMPY_AVAILABLE else 0
                
                dates.append(current.strftime('%Y-%m-%d'))
                rolling_wins.append(wins)
                rolling_win_rate.append((wins / total * 100) if total > 0 else 0)
                rolling_roi.append(float(roi))
            
            current += timedelta(days=1)
        
        return {
            'dates': dates,
            'wins': rolling_wins,
            'win_rate': rolling_win_rate,
            'avg_roi': rolling_roi
        }

# =============================================================================
# Report Generator
# =============================================================================
class ReportGenerator:
    """Generate performance reports in various formats"""
    
    def __init__(self, analyzer: PerformanceAnalyzer):
        self.analyzer = analyzer
    
    def generate_json(self, records: List[PerformanceRecord], 
                     summary: PerformanceSummary) -> str:
        """Generate JSON report"""
        report = {
            'generated_at': RiyadhTime.format(),
            'version': SCRIPT_VERSION,
            'summary': summary.to_dict(),
            'records': [r.to_dict() for r in records],
            'confidence_intervals': self.analyzer.calculate_confidence_intervals(records),
            'rolling': self.analyzer.rolling_performance(records)
        }
        return json.dumps(report, indent=2, default=str)
    
    def generate_csv(self, records: List[PerformanceRecord], 
                    filepath: str) -> None:
        """Generate CSV report"""
        if not records:
            return
        
        # Convert to dicts
        records_data = [r.to_dict() for r in records]
        
        if PANDAS_AVAILABLE:
            # Use pandas for better CSV handling
            df = pd.DataFrame(records_data)
            df.to_csv(filepath, index=False)
        else:
            # Fallback to csv module
            if not records_data:
                return
            
            fieldnames = records_data[0].keys()
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records_data)
        
        logger.info(f"CSV report saved to {filepath}")
    
    def generate_html(self, records: List[PerformanceRecord], 
                     summary: PerformanceSummary,
                     filepath: str) -> None:
        """Generate HTML report with visualizations"""
        
        # Calculate additional metrics
        confidence = self.analyzer.calculate_confidence_intervals(records)
        rolling = self.analyzer.rolling_performance(records)
        
        # Generate base64 plots if matplotlib available
        plots = {}
        if PLOT_AVAILABLE and records:
            plots = self._generate_plots(records, summary)
        
        # Build HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Performance Report - {RiyadhTime.format()}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: auto; background: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
                h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
                .card {{ background: #f8f9fa; padding: 15px; border-radius: 5px; border-left: 4px solid #3498db; }}
                .card h3 {{ margin: 0 0 10px 0; color: #2c3e50; }}
                .card .value {{ font-size: 24px; font-weight: bold; color: #2980b9; }}
                .card .label {{ color: #7f8c8d; font-size: 12px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th {{ background: #2c3e50; color: white; padding: 10px; text-align: left; }}
                td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
                tr:hover {{ background: #f5f5f5; }}
                .win {{ color: #27ae60; }}
                .loss {{ color: #c0392b; }}
                .plot {{ margin: 20px 0; text-align: center; }}
                .plot img {{ max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 5px; }}
                .footer {{ margin-top: 30px; padding-top: 10px; border-top: 1px solid #ddd; color: #7f8c8d; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Performance Report</h1>
                <p>Generated: {RiyadhTime.format()}</p>
                
                <div class="summary">
                    <div class="card">
                        <h3>Total Records</h3>
                        <div class="value">{summary.total_records}</div>
                        <div class="label">Active: {summary.active_records} | Matured: {summary.matured_records}</div>
                    </div>
                    <div class="card">
                        <h3>Win Rate</h3>
                        <div class="value">{summary.win_rate:.1f}%</div>
                        <div class="label">Wins: {summary.wins} | Losses: {summary.losses}</div>
                    </div>
                    <div class="card">
                        <h3>Avg ROI</h3>
                        <div class="value">{summary.avg_roi:.2f}%</div>
                        <div class="label">Best: {summary.best_roi:.2f}% | Worst: {summary.worst_roi:.2f}%</div>
                    </div>
                    <div class="card">
                        <h3>Sharpe Ratio</h3>
                        <div class="value">{summary.sharpe_ratio:.2f}</div>
                        <div class="label">Sortino: {summary.sortino_ratio:.2f}</div>
                    </div>
                </div>
                
                <h2>Confidence Intervals (95%)</h2>
                <table>
                    <tr>
                        <th>Metric</th>
                        <th>Mean</th>
                        <th>Lower Bound</th>
                        <th>Upper Bound</th>
                        <th>Std Dev</th>
                    </tr>
        """
        
        if confidence:
            if 'win_rate' in confidence:
                wr = confidence['win_rate']
                html += f"""
                    <tr>
                        <td>Win Rate</td>
                        <td>{wr.get('mean', 0)*100:.1f}%</td>
                        <td>{wr.get('ci_lower', 0)*100:.1f}%</td>
                        <td>{wr.get('ci_upper', 0)*100:.1f}%</td>
                        <td>{wr.get('std', 0)*100:.1f}%</td>
                    </tr>
                """
            if 'roi' in confidence:
                roi = confidence['roi']
                html += f"""
                    <tr>
                        <td>ROI</td>
                        <td>{roi.get('mean', 0):.2f}%</td>
                        <td>{roi.get('ci_lower', 0):.2f}%</td>
                        <td>{roi.get('ci_upper', 0):.2f}%</td>
                        <td>{roi.get('std', 0):.2f}%</td>
                    </tr>
                """
        
        html += """
                </table>
                
                <h2>Performance by Horizon</h2>
                <table>
                    <tr>
                        <th>Horizon</th>
                        <th>Win Rate</th>
                        <th>Wins</th>
                        <th>Losses</th>
                        <th>Avg ROI</th>
                    </tr>
        """
        
        for horizon, rate in summary.hit_rate_by_horizon.items():
            html += f"""
                    <tr>
                        <td>{horizon}</td>
                        <td>{rate:.1f}%</td>
                        <td>-</td>
                        <td>-</td>
                        <td>-</td>
                    </tr>
            """
        
        html += """
                </table>
                
                <h2>Performance by Sector</h2>
                <table>
                    <tr>
                        <th>Sector</th>
                        <th>Win Rate</th>
                        <th>Wins</th>
                        <th>Losses</th>
                        <th>Avg ROI</th>
                    </tr>
        """
        
        for sector, data in summary.performance_by_sector.items():
            html += f"""
                    <tr>
                        <td>{sector}</td>
                        <td>{data.get('win_rate', 0):.1f}%</td>
                        <td>{data.get('wins', 0)}</td>
                        <td>{data.get('losses', 0)}</td>
                        <td>{data.get('avg_roi', 0):.2f}%</td>
                    </tr>
            """
        
        html += """
                </table>
                
                <h2>Recent Records</h2>
                <table>
                    <tr>
                        <th>Symbol</th>
                        <th>Horizon</th>
                        <th>Entry Date</th>
                        <th>Entry Price</th>
                        <th>Current Price</th>
                        <th>ROI</th>
                        <th>Outcome</th>
                        <th>Status</th>
                    </tr>
        """
        
        # Show last 20 records
        for r in sorted(records, key=lambda x: x.date_recorded, reverse=True)[:20]:
            roi = r.realized_roi if r.realized_roi is not None else r.unrealized_roi
            roi_class = 'win' if roi > 0 else 'loss' if roi < 0 else ''
            outcome_symbol = '✅' if r.outcome == 'WIN' else '❌' if r.outcome == 'LOSS' else '⏳'
            
            html += f"""
                    <tr>
                        <td><strong>{r.symbol}</strong></td>
                        <td>{r.horizon.value}</td>
                        <td>{RiyadhTime.format(r.date_recorded, '%Y-%m-%d')}</td>
                        <td>{r.entry_price:.4f}</td>
                        <td>{r.current_price:.4f}</td>
                        <td class="{roi_class}">{roi:.2f}%</td>
                        <td>{outcome_symbol}</td>
                        <td>{r.status.value}</td>
                    </tr>
            """
        
        # Add plots if available
        if plots:
            html += f"""
                    <h2>Performance Visualizations</h2>
                    <div class="plot">
                        <img src="data:image/png;base64,{plots.get('win_rate', '')}" alt="Win Rate Over Time">
                    </div>
                    <div class="plot">
                        <img src="data:image/png;base64,{plots.get('roi_distribution', '')}" alt="ROI Distribution">
                    </div>
            """
        
        html += f"""
                <div class="footer">
                    Generated by TFB Performance Tracker v{SCRIPT_VERSION} | Riyadh Time
                </div>
            </div>
        </body>
        </html>
        """
        
        # Write to file
        Path(filepath).write_text(html, encoding='utf-8')
        logger.info(f"HTML report saved to {filepath}")
    
    def _generate_plots(self, records: List[PerformanceRecord], 
                       summary: PerformanceSummary) -> Dict[str, str]:
        """Generate base64 encoded plots"""
        if not PLOT_AVAILABLE:
            return {}
        
        plots = {}
        
        try:
            # Win rate over time
            plt.figure(figsize=(10, 6))
            
            matured = [r for r in records if r.status == PerformanceStatus.MATURED]
            matured.sort(key=lambda x: x.maturity_date or x.date_recorded)
            
            if matured:
                dates = [(r.maturity_date or r.date_recorded) for r in matured]
                cumulative_wins = np.cumsum([1 if r.is_win else 0 for r in matured])
                cumulative_total = np.arange(1, len(matured) + 1)
                win_rate = cumulative_wins / cumulative_total * 100
                
                plt.plot(dates, win_rate, 'b-', linewidth=2)
                plt.fill_between(dates, win_rate - 10, win_rate + 10, alpha=0.2)
                plt.xlabel('Date')
                plt.ylabel('Cumulative Win Rate (%)')
                plt.title('Win Rate Over Time')
                plt.grid(True, alpha=0.3)
                
                # Save to base64
                buf = io.BytesIO()
                plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                buf.seek(0)
                plots['win_rate'] = base64.b64encode(buf.getvalue()).decode()
                plt.close()
            
            # ROI distribution
            plt.figure(figsize=(10, 6))
            
            roi_values = [r.realized_roi for r in records if r.realized_roi is not None]
            if roi_values:
                sns.histplot(roi_values, bins=30, kde=True)
                plt.xlabel('ROI (%)')
                plt.ylabel('Frequency')
                plt.title('ROI Distribution')
                plt.axvline(0, color='red', linestyle='--')
                plt.axvline(np.mean(roi_values), color='green', linestyle='--', label=f'Mean: {np.mean(roi_values):.1f}%')
                plt.legend()
                
                buf = io.BytesIO()
                plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                buf.seek(0)
                plots['roi_distribution'] = base64.b64encode(buf.getvalue()).decode()
                plt.close()
            
        except Exception as e:
            logger.warning(f"Plot generation failed: {e}")
        
        return plots

# =============================================================================
# Main Application
# =============================================================================
class PerformanceTrackerApp:
    """Main application class"""
    
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.spreadsheet_id = self._get_spreadsheet_id()
        self.store = PerformanceStore(self.spreadsheet_id, args.sheet_name or 'Performance_Log')
        self.scanner = SourceScanner(self.spreadsheet_id)
        self.analyzer = PerformanceAnalyzer()
        self.report_gen = ReportGenerator(self.analyzer)
        self.running = False
        self.daemon_thread: Optional[Thread] = None
        
    def _get_spreadsheet_id(self) -> str:
        """Get spreadsheet ID from args or env"""
        if self.args.sheet_id:
            return self.args.sheet_id
        
        # Try environment variables
        for key in ['DEFAULT_SPREADSHEET_ID', 'GOOGLE_SHEETS_ID', 'SPREADSHEET_ID']:
            value = os.getenv(key)
            if value:
                return value
        
        raise ValueError("No spreadsheet ID provided. Use --sheet-id or set DEFAULT_SPREADSHEET_ID")
    
    async def run(self) -> int:
        """Run the application"""
        try:
            # Load existing records
            records = self.store.load_records(max_records=self.args.max_records or 10000)
            
            if self.args.record:
                await self._record_recommendations(records)
            
            if self.args.audit:
                await self._audit_records(records)
            
            if self.args.analyze:
                self._analyze_performance(records)
            
            if self.args.simulate:
                self._run_simulation(records)
            
            if self.args.export:
                self._export_report(records)
            
            if self.args.daemon:
                await self._run_daemon()
            
            return 0
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            return 130
        except Exception as e:
            logger.exception(f"Fatal error: {e}")
            return 1
    
    async def _record_recommendations(self, existing_records: List[PerformanceRecord]) -> None:
        """Record new recommendations"""
        source_tabs = self.args.source_tab or ['Market_Scan', 'Market_Leaders']
        horizons = [HorizonType(h.upper()) for h in (self.args.horizons or ['1M', '3M'])]
        track_recos = set(self.args.track_recos or ['BUY', 'STRONG BUY'])
        
        all_candidates = []
        
        for tab in source_tabs:
            candidates = self.scanner.scan_tab(tab, limit=self.args.limit or 500)
            all_candidates.extend(candidates)
        
        # Filter by recommendation
        filtered = [c for c in all_candidates 
                   if c.get('recommendation', '').upper() in track_recos]
        
        # Create records
        new_records = []
        date_recorded = RiyadhTime.now()
        
        for cand in filtered:
            for horizon in horizons:
                # Check if already exists
                key = f"{cand['symbol']}|{horizon.value}|{date_recorded.strftime('%Y%m%d')}"
                if key in self.store.cache:
                    continue
                
                # Calculate target date and price
                target_date = date_recorded + timedelta(days=horizon.days)
                
                # Get ROI and forecast price for horizon
                roi_key = f"roi_{horizon.value.lower()}"
                fp_key = f"fp_{horizon.value.lower()}"
                
                target_roi = cand.get(roi_key, 0)
                target_price = cand.get(fp_key, 0)
                
                # If target price not available, calculate from ROI
                if target_price == 0 and cand.get('price', 0) > 0 and target_roi != 0:
                    target_price = cand['price'] * (1 + target_roi / 100)
                
                record = PerformanceRecord(
                    record_id=str(uuid.uuid4()),
                    symbol=cand['symbol'],
                    horizon=horizon,
                    date_recorded=date_recorded,
                    entry_price=cand.get('price', 0),
                    entry_recommendation=RecommendationType(cand.get('recommendation', 'HOLD')),
                    entry_score=cand.get('score', 0),
                    entry_risk_bucket=cand.get('risk', 'MODERATE'),
                    entry_confidence=cand.get('confidence', 'MEDIUM'),
                    origin_tab=cand.get('origin_tab', 'Unknown'),
                    target_price=target_price,
                    target_roi=target_roi,
                    target_date=target_date,
                    status=PerformanceStatus.ACTIVE,
                    sector=cand.get('sector')
                )
                
                new_records.append(record)
        
        if new_records:
            self.store.append_records(new_records)
            logger.info(f"✅ Recorded {len(new_records)} new recommendations")
        else:
            logger.info("No new recommendations to record")
    
    async def _audit_records(self, records: List[PerformanceRecord]) -> None:
        """Audit existing records"""
        if not records:
            logger.info("No records to audit")
            return
        
        # Get active records that need price updates
        active = [r for r in records if r.status == PerformanceStatus.ACTIVE]
        
        if not active:
            logger.info("No active records to audit")
            return
        
        # Fetch current prices
        symbols = list(set(r.symbol for r in active))
        
        async with QuoteFetcher() as fetcher:
            quotes = await fetcher.fetch_quotes(symbols, refresh=self.args.refresh)
        
        # Update records
        now = RiyadhTime.now()
        updated_count = 0
        matured_count = 0
        
        for r in active:
            if r.symbol in quotes:
                quote = quotes[r.symbol]
                if 'current_price' in quote:
                    r.current_price = quote['current_price']
                    r.unrealized_roi = ((r.current_price / r.entry_price) - 1) * 100
                    r.last_updated = now
                    updated_count += 1
            
            # Check maturity
            if r.target_date and now >= r.target_date:
                r.status = PerformanceStatus.MATURED
                r.maturity_date = now
                r.realized_roi = r.unrealized_roi
                r.outcome = 'WIN' if r.realized_roi > 0 else 'LOSS' if r.realized_roi < 0 else 'BREAKEVEN'
                matured_count += 1
        
        # Save updates
        if updated_count > 0:
            self.store.save_records(records)
            logger.info(f"✅ Updated {updated_count} records ({matured_count} matured)")
    
    def _analyze_performance(self, records: List[PerformanceRecord]) -> None:
        """Analyze performance"""
        summary = self.analyzer.analyze_records(records)
        
        # Update summary in sheet
        self.store.update_summary(summary)
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 PERFORMANCE SUMMARY")
        print("=" * 60)
        print(f"Total Records: {summary.total_records}")
        print(f"Active: {summary.active_records} | Matured: {summary.matured_records}")
        print(f"Wins: {summary.wins} | Losses: {summary.losses} | Win Rate: {summary.win_rate:.1f}%")
        print(f"Avg ROI: {summary.avg_roi:.2f}% | Median ROI: {summary.median_roi:.2f}%")
        print(f"Best ROI: {summary.best_roi:.2f}% | Worst ROI: {summary.worst_roi:.2f}%")
        print(f"Sharpe Ratio: {summary.sharpe_ratio:.2f} | Sortino: {summary.sortino_ratio:.2f}")
        print("=" * 60)
        
        # Confidence intervals
        if self.args.confidence:
            ci = self.analyzer.calculate_confidence_intervals(records, self.args.confidence)
            if ci:
                print("\n📈 CONFIDENCE INTERVALS")
                print("-" * 40)
                for metric, values in ci.items():
                    print(f"{metric.upper()}:")
                    print(f"  Mean: {values.get('mean', 0):.2f}")
                    print(f"  95% CI: [{values.get('ci_lower', 0):.2f}, {values.get('ci_upper', 0):.2f}]")
    
    def _run_simulation(self, records: List[PerformanceRecord]) -> None:
        """Run Monte Carlo simulation"""
        matured = [r for r in records if r.status == PerformanceStatus.MATURED]
        
        if not matured:
            logger.info("No matured records for simulation")
            return
        
        outcomes = [r.is_win for r in matured if r.is_win is not None]
        roi_values = [r.realized_roi for r in matured if r.realized_roi is not None]
        
        print("\n" + "=" * 60)
        print("🎲 MONTE CARLO SIMULATION")
        print("=" * 60)
        
        if outcomes:
            wr_sim = self.analyzer.simulator.simulate_win_rate(
                outcomes, 
                iterations=self.args.iterations or 10000,
                confidence=self.args.confidence or 0.95
            )
            
            print(f"\nWin Rate Simulation ({self.args.iterations or 10000} iterations):")
            print(f"  Mean: {wr_sim['mean']*100:.1f}%")
            print(f"  Median: {wr_sim['median']*100:.1f}%")
            print(f"  Std: {wr_sim['std']*100:.1f}%")
            print(f"  95% CI: [{wr_sim['ci_lower']*100:.1f}%, {wr_sim['ci_upper']*100:.1f}%]")
            print(f"  Observed: {wr_sim['observed']*100:.1f}%")
        
        if roi_values:
            roi_sim = self.analyzer.simulator.simulate_returns(
                roi_values,
                periods=self.args.periods or 252,
                iterations=self.args.iterations or 10000
            )
            
            print(f"\nReturns Simulation (1 year):")
            print(f"  Mean Final Value: {roi_sim.get('mean_final', 0):.2f}")
            print(f"  Median Final: {roi_sim.get('median_final', 0):.2f}")
            print(f"  95% CI: [{roi_sim.get('ci_lower', 0):.2f}, {roi_sim.get('ci_upper', 0):.2f}]")
            print(f"  Probability of Loss: {roi_sim.get('prob_loss', 0)*100:.1f}%")
            print(f"  Probability of Doubling: {roi_sim.get('prob_double', 0)*100:.1f}%")
    
    def _export_report(self, records: List[PerformanceRecord]) -> None:
        """Export performance report"""
        summary = self.analyzer.analyze_records(records)
        output_path = self.args.output or f"performance_report_{RiyadhTime.format('%Y%m%d_%H%M%S')}"
        
        if self.args.format == 'json':
            json_str = self.report_gen.generate_json(records, summary)
            path = f"{output_path}.json"
            Path(path).write_text(json_str, encoding='utf-8')
            logger.info(f"JSON report saved to {path}")
            
        elif self.args.format == 'csv':
            path = f"{output_path}.csv"
            self.report_gen.generate_csv(records, path)
            
        elif self.args.format == 'html':
            path = f"{output_path}.html"
            self.report_gen.generate_html(records, summary, path)
            
        elif self.args.format == 'all':
            # Generate all formats
            json_path = f"{output_path}.json"
            Path(json_path).write_text(self.report_gen.generate_json(records, summary), encoding='utf-8')
            
            csv_path = f"{output_path}.csv"
            self.report_gen.generate_csv(records, csv_path)
            
            html_path = f"{output_path}.html"
            self.report_gen.generate_html(records, summary, html_path)
            
            logger.info(f"Reports saved to {output_path}.*")
    
    async def _run_daemon(self) -> None:
        """Run as daemon with periodic updates"""
        self.running = True
        
        def signal_handler(sig, frame):
            logger.info("Shutdown signal received")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info(f"Starting daemon mode (interval: {self.args.interval}s)")
        
        while self.running:
            try:
                # Load records
                records = self.store.load_records(max_records=self.args.max_records)
                
                # Audit records
                await self._audit_records(records)
                
                # Analyze and update summary
                summary = self.analyzer.analyze_records(records)
                self.store.update_summary(summary)
                
                # Optional webhook notification
                if self.args.webhook:
                    await self._send_webhook(summary)
                
                logger.info(f"Daemon cycle complete. Next in {self.args.interval}s")
                
                # Wait for next cycle or shutdown
                for _ in range(self.args.interval):
                    if not self.running:
                        break
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Daemon cycle failed: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    async def _send_webhook(self, summary: PerformanceSummary) -> None:
        """Send webhook notification"""
        if not self.args.webhook or not ASYNC_HTTP_AVAILABLE:
            return
        
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    'text': f"📊 Performance Update - {RiyadhTime.format()}",
                    'attachments': [{
                        'fields': [
                            {'title': 'Win Rate', 'value': f"{summary.win_rate:.1f}%", 'short': True},
                            {'title': 'Avg ROI', 'value': f"{summary.avg_roi:.2f}%", 'short': True},
                            {'title': 'Sharpe', 'value': f"{summary.sharpe_ratio:.2f}", 'short': True},
                            {'title': 'Records', 'value': str(summary.total_records), 'short': True}
                        ],
                        'color': 'good' if summary.win_rate > 50 else 'warning'
                    }]
                }
                
                await session.post(self.args.webhook, json=payload)
                
        except Exception as e:
            logger.warning(f"Webhook notification failed: {e}")

# =============================================================================
# CLI Entry Point
# =============================================================================
def create_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        description=f"Tadawul Fast Bridge - Advanced Performance Tracker v{SCRIPT_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Core options
    parser.add_argument("--sheet-id", help="Spreadsheet ID")
    parser.add_argument("--sheet-name", default="Performance_Log", help="Sheet name")
    
    # Action modes
    parser.add_argument("--record", action="store_true", help="Record new recommendations")
    parser.add_argument("--audit", action="store_true", help="Audit existing records")
    parser.add_argument("--analyze", action="store_true", help="Analyze performance")
    parser.add_argument("--simulate", action="store_true", help="Run Monte Carlo simulation")
    parser.add_argument("--export", action="store_true", help="Export performance report")
    parser.add_argument("--daemon", action="store_true", help="Run as daemon")
    
    # Recording options
    parser.add_argument("--source-tab", action="append", help="Source tab name")
    parser.add_argument("--horizons", nargs="+", default=["1M", "3M"], help="Horizons to track")
    parser.add_argument("--track-recos", nargs="+", help="Recommendations to track")
    parser.add_argument("--limit", type=int, default=500, help="Max rows per source")
    
    # Audit options
    parser.add_argument("--refresh", type=int, default=1, help="Refresh quotes (0/1)")
    parser.add_argument("--max-records", type=int, default=10000, help="Max records to load")
    
    # Analysis options
    parser.add_argument("--confidence", type=float, default=0.95, help="Confidence level")
    parser.add_argument("--iterations", type=int, default=10000, help="Monte Carlo iterations")
    parser.add_argument("--periods", type=int, default=252, help="Simulation periods")
    
    # Export options
    parser.add_argument("--format", choices=["json", "csv", "html", "all"], default="html", help="Export format")
    parser.add_argument("--output", help="Output file path (without extension)")
    
    # Daemon options
    parser.add_argument("--interval", type=int, default=3600, help="Daemon interval (seconds)")
    parser.add_argument("--webhook", help="Webhook URL for notifications")
    
    # Other
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    return parser

async def async_main() -> int:
    """Async main entry point"""
    parser = create_parser()
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not any([args.record, args.audit, args.analyze, args.simulate, args.export, args.daemon]):
        parser.print_help()
        return 0
    
    try:
        app = PerformanceTrackerApp(args)
        return await app.run()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return 1

def main() -> int:
    """Main entry point"""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    return asyncio.run(async_main())

if __name__ == "__main__":
    sys.exit(main())
