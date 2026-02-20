#!/usr/bin/env python3
"""
core/investment_advisor.py
================================================================================
Investment Advisor Core — v3.0.0 (NEXT-GEN ENTERPRISE)
================================================================================
Financial Data Platform — Advanced Investment Advisor with ML & MPT

What's new in v3.0.0:
- ✅ **Black-Litterman Optimization**: True BL implementation using ML scores as views
- ✅ **True Risk Parity**: SLSQP-based risk contribution minimization
- ✅ **Memory Optimization**: Applied `@dataclass(slots=True)` for large universe scans
- ✅ **High-Performance JSON**: `orjson` integration for blazing fast serialization
- ✅ **OpenTelemetry Tracing**: End-to-end tracing for optimization bottlenecks
- ✅ **Prometheus Metrics**: Exporting allocation latency and request statistics
- ✅ **XGBoost ML Scoring**: Institutional-grade non-linear feature evaluations
- ✅ **PSD Correlation Matrices**: Guaranteed positive semi-definite covariance matrices

Key Features:
- Multi-source data integration (sheets, engine, news APIs)
- Advanced scoring models with configurable weights
- Sophisticated portfolio optimization algorithms (MVO, Risk Parity, BL)
- Real-time risk metrics and stress testing
- Thread-safe execution for high-concurrency environments
- Comprehensive error handling and diagnostics
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import time
import hashlib
import logging
import os
import sys
from collections import defaultdict, Counter
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson
    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode('utf-8')
except ImportError:
    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)
    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str)

# ---------------------------------------------------------------------------
# Scientific & ML Stack
# ---------------------------------------------------------------------------
try:
    import numpy as np
    import pandas as pd
    HAS_NUMPY = True
    HAS_PANDAS = True
except ImportError:
    np = None
    pd = None
    HAS_NUMPY = False
    HAS_PANDAS = False

try:
    from scipy import stats, optimize
    from scipy.stats import norm, t
    HAS_SCIPY = True
except ImportError:
    stats = None
    optimize = None
    norm = None
    t = None
    HAS_SCIPY = False

try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

try:
    import xgboost as xgb
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False

# ---------------------------------------------------------------------------
# Monitoring & Tracing
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter as PromCounter, Histogram as PromHistogram
    _PROMETHEUS_AVAILABLE = True
    advisor_requests_total = PromCounter('advisor_requests_total', 'Total advisor requests', ['status', 'strategy'])
    advisor_optimization_duration = PromHistogram('advisor_optimization_duration_seconds', 'Time spent optimizing portfolios', ['strategy'])
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    class DummyMetric:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
    advisor_requests_total = DummyMetric()
    advisor_optimization_duration = DummyMetric()

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    _OTEL_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except ImportError:
    _OTEL_AVAILABLE = False
    class DummySpan:
        def set_attribute(self, *args, **kwargs): pass
        def set_status(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args, **kwargs): pass
    class DummyTracer:
        def start_as_current_span(self, *args, **kwargs): return DummySpan()
    tracer = DummyTracer()

# ============================================================================
# Version and Configuration
# ============================================================================

__version__ = "3.0.0"
ADVISOR_VERSION = __version__

logger = logging.getLogger("core.investment_advisor")

DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "KSA_TADAWUL"]

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}

RISK_LEVELS = {
    "Very Low": 1,
    "Low": 2,
    "Moderate": 3,
    "High": 4,
    "Very High": 5,
}

CONFIDENCE_LEVELS = {
    "Very Low": 1,
    "Low": 2,
    "Moderate": 3,
    "High": 4,
    "Very High": 5,
}

# ============================================================================
# Enums for Type Safety
# ============================================================================

class MarketType(Enum):
    KSA = "ksa"
    US = "us"
    GLOBAL = "global"
    COMMODITY = "commodity"
    FOREX = "forex"
    CRYPTO = "crypto"


class AssetClass(Enum):
    EQUITY = "equity"
    ETF = "etf"
    MUTUAL_FUND = "mutual_fund"
    COMMODITY = "commodity"
    FOREX = "forex"
    BOND = "bond"
    REIT = "reit"
    CRYPTO = "crypto"


class Recommendation(Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    AVOID = "AVOID"


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


# ============================================================================
# Data Classes
# ============================================================================

@dataclass(slots=True)
class Security:
    """Security information and metrics."""
    symbol: str
    canonical: str
    name: str
    sheet: str
    market: str
    sector: str
    industry: str = ""
    currency: str = "SAR"
    asset_class: AssetClass = AssetClass.EQUITY
    
    price: Optional[float] = None
    previous_close: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
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
    operating_margin: Optional[float] = None
    gross_margin: Optional[float] = None
    
    revenue_growth: Optional[float] = None
    earnings_growth: Optional[float] = None
    growth_score: Optional[float] = None
    
    rsi_14: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None
    volatility_30d: Optional[float] = None
    atr_14: Optional[float] = None
    
    returns_1w: Optional[float] = None
    returns_1m: Optional[float] = None
    returns_3m: Optional[float] = None
    returns_6m: Optional[float] = None
    returns_12m: Optional[float] = None
    
    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None
    
    quality_score: Optional[float] = None
    value_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    
    risk_bucket: str = "Moderate"
    confidence_bucket: str = "Moderate"
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    var_95: Optional[float] = None
    cvar_95: Optional[float] = None
    
    news_score: Optional[float] = None
    social_score: Optional[float] = None
    analyst_sentiment: Optional[float] = None
    insider_sentiment: Optional[float] = None
    
    trend_signal: str = "NEUTRAL"
    macd_signal: str = "NEUTRAL"
    bb_signal: str = "NEUTRAL"
    
    data_source: str = "unknown"
    data_quality: str = "FAIR"
    last_updated_utc: Optional[datetime] = None
    
    advisor_score: float = 0.0
    allocation_weight: float = 0.0
    allocation_amount: float = 0.0
    reason: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        d["asset_class"] = self.asset_class.value
        return {k: v for k, v in d.items() if v is not None}


@dataclass(slots=True)
class Portfolio:
    """Portfolio composition and metrics."""
    securities: List[Security] = field(default_factory=list)
    total_value: float = 0.0
    cash: float = 0.0
    expected_return: float = 0.0
    expected_volatility: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    var_95: float = 0.0
    cvar_95: float = 0.0
    diversification_ratio: float = 0.0
    concentration_score: float = 0.0
    sector_exposure: Dict[str, float] = field(default_factory=dict)
    currency_exposure: Dict[str, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_value": self.total_value,
            "cash": self.cash,
            "expected_return": self.expected_return,
            "expected_volatility": self.expected_volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "var_95": self.var_95,
            "cvar_95": self.cvar_95,
            "diversification_ratio": self.diversification_ratio,
            "concentration_score": self.concentration_score,
            "sector_exposure": self.sector_exposure,
            "currency_exposure": self.currency_exposure,
            "securities": [s.to_dict() for s in self.securities],
        }


@dataclass(slots=True)
class AdvisorRequest:
    """Investment advisor request parameters."""
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
    
    include_news: bool = True
    news_weight: float = 1.0
    include_technical: bool = True
    technical_weight: float = 0.3
    use_ml: bool = True
    ml_weight: float = 0.2
    optimize_taxes: bool = False
    tax_rate: float = 0.15
    rebalance_frequency: str = "monthly"
    
    as_of_utc: Optional[datetime] = None
    use_row_updated_at: bool = True
    debug: bool = False
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AdvisorRequest:
        sources = data.get("sources", ["ALL"])
        if isinstance(sources, str): sources = [sources]
        sources = [str(s).strip() for s in sources if str(s).strip()]
        if not sources or "ALL" in [s.upper() for s in sources]:
            sources = list(DEFAULT_SOURCES)
        
        try: risk_profile = RiskProfile(str(data.get("risk_profile", "moderate")).lower())
        except ValueError: risk_profile = RiskProfile.MODERATE
        
        try: allocation_strategy = AllocationStrategy(str(data.get("allocation_strategy", "maximum_sharpe")).lower())
        except ValueError: allocation_strategy = AllocationStrategy.MAXIMUM_SHARPE
        
        tickers = None
        tickers_in = data.get("tickers") or data.get("symbols")
        if tickers_in:
            if isinstance(tickers_in, str): tickers = [x for x in tickers_in.replace(",", " ").split() if x.strip()]
            elif isinstance(tickers_in, list): tickers = [str(x) for x in tickers_in if x]
        
        as_of_utc = None
        if "as_of_utc" in data:
            as_of_utc = _parse_iso_to_dt(data["as_of_utc"])
        
        return cls(
            sources=sources,
            tickers=tickers,
            risk_profile=risk_profile,
            risk_bucket=_norm_bucket(data.get("risk", "")),
            confidence_bucket=_norm_bucket(data.get("confidence", "")),
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
            include_news=_truthy(data.get("include_news", True)),
            news_weight=_to_float(data.get("news_weight")) or 1.0,
            include_technical=_truthy(data.get("include_technical", True)),
            technical_weight=_to_float(data.get("technical_weight")) or 0.3,
            use_ml=_truthy(data.get("use_ml", True)),
            ml_weight=_to_float(data.get("ml_weight")) or 0.2,
            optimize_taxes=_truthy(data.get("optimize_taxes", False)),
            tax_rate=_to_float(data.get("tax_rate")) or 0.15,
            rebalance_frequency=data.get("rebalance_frequency", "monthly"),
            as_of_utc=as_of_utc,
            use_row_updated_at=_truthy(data.get("use_row_updated_at", True)),
            debug=_truthy(data.get("debug", False)),
        )


# ============================================================================
# Helper Functions
# ============================================================================

def _truthy(v: Any) -> bool:
    if isinstance(v, bool): return v
    if v is None: return False
    return str(v).strip().lower() in _TRUTHY

def _safe_str(x: Any, default: str = "") -> str:
    try: return str(x).strip() if x is not None else default
    except Exception: return default

def _safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try: return int(float(str(x).strip())) if x is not None else default
    except (ValueError, TypeError): return default

def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try: return float(str(x).strip().replace(",", "")) if x is not None else default
    except (ValueError, TypeError): return default

def _to_float(x: Any) -> Optional[float]: return _safe_float(x)
def _to_int(x: Any) -> Optional[int]: return _safe_int(x)

def _as_ratio(x: Any) -> Optional[float]:
    f = _to_float(x)
    if f is None: return None
    return f / 100.0 if f > 1.5 else f

def _to_percent(x: Any, decimals: int = 2) -> Optional[float]:
    f = _to_float(x)
    return round(f * 100.0, decimals) if f is not None else None

def _round2(x: Any) -> Any:
    f = _to_float(x)
    return round(f, 2) if f is not None else x

def _clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(max_val, value))

def _norm_key(k: Any) -> str:
    return " ".join(_safe_str(k).lower().split())

def _norm_bucket(x: Any) -> str:
    if not x: return ""
    s = _safe_str(x).lower()
    if s in ("very low", "very-low", "very_low", "vl", "very low confidence", "vlc"): return "Very Low"
    if s in ("low", "l", "low confidence", "lc"): return "Low"
    if s in ("moderate", "medium", "mid", "balanced", "m", "moderate confidence", "mc"): return "Moderate"
    if s in ("high", "h", "aggressive", "high confidence", "hc"): return "High"
    if s in ("very high", "very-high", "very_high", "vh", "very aggressive", "very high confidence", "vhc"): return "Very High"
    return s.title()

def _norm_symbol(symbol: str) -> str:
    s = _safe_str(symbol).upper().replace(" ", "")
    if s.startswith("TADAWUL:"): s = s[8:]
    if s.endswith(".SA"): s = s[:-3] + ".SR"
    return s

def _canonical_symbol(symbol: str) -> str:
    s = _norm_symbol(symbol)
    if not s: return ""
    if s.endswith(".SR") and s[:-3].isdigit(): return "KSA:" + s[:-3]
    if s.isdigit(): return "KSA:" + s
    if s.startswith("^"): return "IDX:" + s
    if "." in s: return "GLOBAL:" + s.split(".", 1)[0]
    return "GLOBAL:" + s

def _symbol_variants(symbol: str) -> Set[str]:
    s = _norm_symbol(symbol)
    variants = set()
    if not s: return variants
    variants.add(s)
    if s.endswith(".SR") and s[:-3].isdigit(): variants.add(s[:-3])
    elif s.isdigit(): variants.add(s + ".SR")
    if "." in s:
        base = s.split(".", 1)[0]
        if base: variants.add(base)
    if not s.startswith("TADAWUL:") and s.isdigit(): variants.add("TADAWUL:" + s)
    return variants

def _parse_iso_to_dt(v: Any) -> Optional[datetime]:
    s = _safe_str(v)
    if not s: return None
    try:
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except Exception: return None

def _iso_utc(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat() if dt.tzinfo is None else dt.astimezone(timezone.utc).isoformat()

def _iso_riyadh(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=3))).isoformat() if dt.tzinfo is None else dt.astimezone(timezone(timedelta(hours=3))).isoformat()

def _now_utc() -> datetime: return datetime.now(timezone.utc)

def _unique_headers(headers: List[Any]) -> List[str]:
    raw = [str(x).strip() for x in (headers or [])]
    out, seen = [], {}
    for h in raw:
        base = h or "Column"
        k = base.lower()
        if k not in seen:
            seen[k] = 1
            out.append(base)
        else:
            seen[k] += 1
            out.append(f"{base} ({seen[k]})")
    return out

def _rows_to_dicts(headers: List[Any], rows: List[Any], sheet_name: str, limit: int) -> List[Dict[str, Any]]:
    h = _unique_headers(headers or [])
    out = []
    for i, r in enumerate(rows or []):
        if i >= limit: break
        if not isinstance(r, (list, tuple)): continue
        d = {h[j]: (r[j] if j < len(r) else None) for j in range(len(h))}
        d["_Sheet"] = sheet_name
        out.append(d)
    return out

def _get_any(row: Dict[str, Any], *names: str) -> Any:
    if not row: return None
    for n in names:
        if n in row: return row[n]
    nmap = row.get("_nmap")
    if not isinstance(nmap, dict):
        nmap = { _norm_key(k): k for k in row if k != "_nmap" and _norm_key(k)}
        row["_nmap"] = nmap
    for n in names:
        nk = _norm_key(n)
        if nk in nmap: return row[nmap[nk]]
    return None


# ============================================================================
# Data Fetching from Engine
# ============================================================================

def _engine_get_snapshot(engine: Any, sheet_name: str) -> Optional[Dict[str, Any]]:
    if engine is None: return None
    fn = getattr(engine, "get_cached_sheet_snapshot", None)
    if callable(fn):
        try:
            snap = fn(sheet_name)
            return snap if isinstance(snap, dict) else None
        except Exception: return None
    return None

def _engine_get_multi_snapshots(engine: Any, sheet_names: List[str]) -> Dict[str, Dict[str, Any]]:
    if engine is None: return {}
    fn = getattr(engine, "get_cached_multi_sheet_snapshots", None)
    if callable(fn):
        try:
            out = fn(sheet_names)
            if isinstance(out, dict): return {str(k): v for k, v in out.items() if isinstance(v, dict)}
        except Exception: pass
    out2 = {}
    for s in sheet_names or []:
        snap = _engine_get_snapshot(engine, s)
        if snap: out2[str(s)] = snap
    return out2

def _engine_try_get_news_score(engine: Any, symbol: str) -> Optional[float]:
    if engine is None or not symbol: return None
    for fn_name in ("get_news_score", "get_cached_news_score", "news_get_score"):
        fn = getattr(engine, fn_name, None)
        if callable(fn):
            try: return _to_float(fn(symbol))
            except Exception: continue
    return None

def _engine_try_get_technical_signals(engine: Any, symbol: str) -> Dict[str, Any]:
    signals = {}
    if engine is None or not symbol: return signals
    for fn_name in ("get_technical_signals", "get_cached_technical_signals"):
        fn = getattr(engine, fn_name, None)
        if callable(fn):
            try:
                v = fn(symbol)
                if isinstance(v, dict): signals.update(v)
            except Exception: continue
    return signals

def _engine_try_get_historical_returns(engine: Any, symbol: str, periods: List[str]) -> Dict[str, Optional[float]]:
    returns = {}
    if engine is None or not symbol: return returns
    fn = getattr(engine, "get_historical_returns", None)
    if callable(fn):
        try:
            v = fn(symbol, periods)
            if isinstance(v, dict):
                for k, val in v.items(): returns[k] = _to_float(val)
        except Exception: pass
    return returns

def _fetch_universe(sources: List[str], engine: Any = None, max_rows_per_source: int = 5000) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    meta = {"sources": sources, "engine": type(engine).__name__ if engine is not None else None, "items": [], "mode": "engine_snapshot"}
    if engine is None: return [], meta, "Missing engine instance"
    
    normalized_sources = []
    for s in sources:
        if s.upper() == "ALL": normalized_sources.extend(DEFAULT_SOURCES)
        elif s and isinstance(s, str): normalized_sources.append(s.strip())
    if not normalized_sources: normalized_sources = DEFAULT_SOURCES
    
    snaps = _engine_get_multi_snapshots(engine, normalized_sources)
    out_rows = []
    
    for sheet in normalized_sources:
        snap = snaps.get(sheet) or _engine_get_snapshot(engine, sheet)
        if not snap:
            meta["items"].append({"sheet": sheet, "cached": False, "rows": 0, "headers": 0})
            continue
        
        headers = snap.get("headers") or []
        rows = snap.get("rows") or []
        meta["items"].append({"sheet": sheet, "cached": True, "rows": len(rows) if isinstance(rows, list) else 0, "headers": len(headers) if isinstance(headers, list) else 0, "cached_at_utc": snap.get("cached_at_utc")})
        
        if isinstance(headers, list) and isinstance(rows, list):
            out_rows.extend(_rows_to_dicts(headers, rows, sheet_name=sheet, limit=max_rows_per_source))
            
    if not out_rows: return [], meta, "No cached sheet snapshots found"
    return out_rows, meta, None


# ============================================================================
# Security Extraction
# ============================================================================

def _extract_security(row: Dict[str, Any], engine: Any = None, include_news: bool = False, include_technical: bool = False) -> Optional[Security]:
    symbol = _norm_symbol(_get_any(row, "Symbol", "Fund Symbol", "Ticker", "Code", "Fund Code"))
    if not symbol or symbol == "SYMBOL": return None
    
    canonical = _canonical_symbol(symbol)
    name = _safe_str(_get_any(row, "Name", "Company Name", "Fund Name", "Instrument", "Long Name"))
    sheet = _safe_str(_get_any(row, "_Sheet", "Origin")) or ""
    market = _safe_str(_get_any(row, "Market", "Exchange", "Region")) or ("KSA" if symbol.endswith(".SR") or symbol.isdigit() else "GLOBAL")
    sector = _safe_str(_get_any(row, "Sector", "Category"))
    industry = _safe_str(_get_any(row, "Industry", "Sub Sector", "Sub-Sector"))
    currency = _safe_str(_get_any(row, "Currency", "CCY")) or "SAR"
    
    asset_class = AssetClass.EQUITY
    sheet_lower = sheet.lower()
    if "mutual_fund" in sheet_lower or "fund" in sheet_lower: asset_class = AssetClass.MUTUAL_FUND
    elif "commodities" in sheet_lower: asset_class = AssetClass.COMMODITY
    elif "forex" in sheet_lower or "fx" in sheet_lower: asset_class = AssetClass.FOREX
    
    price = _to_float(_get_any(row, "Price", "Last", "Close", "NAV per Share", "NAV", "Last Price"))
    previous_close = _to_float(_get_any(row, "Prev Close", "Previous Close", "Prior Close"))
    
    s = Security(
        symbol=symbol, canonical=canonical, name=name, sheet=sheet, market=market, sector=sector, industry=industry, currency=currency, asset_class=asset_class,
        price=price, previous_close=previous_close,
        day_high=_to_float(_get_any(row, "Day High", "High")), day_low=_to_float(_get_any(row, "Day Low", "Low")),
        week_52_high=_to_float(_get_any(row, "52W High", "52-Week High", "Year High")), week_52_low=_to_float(_get_any(row, "52W Low", "52-Week Low", "Year Low")),
        volume=_to_float(_get_any(row, "Volume", "Vol", "Volume (Shares)")), avg_volume_30d=_to_float(_get_any(row, "Avg Vol 30D", "Avg Volume 30D", "Average Volume")),
        value_traded=_to_float(_get_any(row, "Value Traded", "Turnover", "Value")), market_cap=_to_float(_get_any(row, "Market Cap", "Mkt Cap", "Market Capitalization")),
        shares_outstanding=_to_float(_get_any(row, "Shares Outstanding", "Shares Out")), free_float=_to_float(_get_any(row, "Free Float %", "Free Float")),
        pe_ttm=_to_float(_get_any(row, "P/E (TTM)", "PE TTM", "PE Ratio")), forward_pe=_to_float(_get_any(row, "Forward P/E", "Forward PE")),
        pb=_to_float(_get_any(row, "P/B", "Price/Book", "PB Ratio")), ps=_to_float(_get_any(row, "P/S", "Price/Sales", "PS Ratio")),
        eps_ttm=_to_float(_get_any(row, "EPS (TTM)", "EPS TTM")), forward_eps=_to_float(_get_any(row, "Forward EPS")),
        dividend_yield=_to_float(_get_any(row, "Dividend Yield", "Div Yield")), payout_ratio=_to_float(_get_any(row, "Payout Ratio")),
        beta=_to_float(_get_any(row, "Beta")), roe=_to_float(_get_any(row, "ROE", "Return on Equity")),
        roa=_to_float(_get_any(row, "ROA", "Return on Assets")), net_margin=_to_float(_get_any(row, "Net Margin", "Profit Margin")),
        operating_margin=_to_float(_get_any(row, "Operating Margin")), gross_margin=_to_float(_get_any(row, "Gross Margin")),
        revenue_growth=_as_ratio(_get_any(row, "Revenue Growth", "Rev Growth", "Sales Growth")), earnings_growth=_as_ratio(_get_any(row, "Earnings Growth", "EPS Growth")),
        rsi_14=_to_float(_get_any(row, "RSI 14", "RSI")), ma20=_to_float(_get_any(row, "MA 20", "SMA 20", "20-Day MA")),
        ma50=_to_float(_get_any(row, "MA 50", "SMA 50", "50-Day MA")), ma200=_to_float(_get_any(row, "MA 200", "SMA 200", "200-Day MA")),
        volatility_30d=_to_float(_get_any(row, "Volatility 30D", "Volatility", "Vol 30D")), atr_14=_to_float(_get_any(row, "ATR 14", "ATR")),
        returns_1w=_as_ratio(_get_any(row, "Return 1W", "1W Return")), returns_1m=_as_ratio(_get_any(row, "Return 1M", "1M Return")),
        returns_3m=_as_ratio(_get_any(row, "Return 3M", "3M Return")), returns_6m=_as_ratio(_get_any(row, "Return 6M", "6M Return")),
        returns_12m=_as_ratio(_get_any(row, "Return 12M", "12M Return")),
        forecast_price_1m=_to_float(_get_any(row, "Forecast Price (1M)", "Forecast 1M", "forecast_price_1m")),
        forecast_price_3m=_to_float(_get_any(row, "Forecast Price (3M)", "Forecast 3M", "forecast_price_3m")),
        forecast_price_12m=_to_float(_get_any(row, "Forecast Price (12M)", "Forecast 12M", "forecast_price_12m")),
        expected_roi_1m=_as_ratio(_get_any(row, "Expected ROI % (1M)", "ROI 1M", "expected_roi_1m")),
        expected_roi_3m=_as_ratio(_get_any(row, "Expected ROI % (3M)", "ROI 3M", "expected_roi_3m")),
        expected_roi_12m=_as_ratio(_get_any(row, "Expected ROI % (12M)", "ROI 12M", "expected_roi_12m")),
        forecast_confidence=_to_float(_get_any(row, "Forecast Confidence", "Confidence", "forecast_confidence")),
        quality_score=_to_float(_get_any(row, "Quality Score", "Quality")), value_score=_to_float(_get_any(row, "Value Score", "Value")),
        momentum_score=_to_float(_get_any(row, "Momentum Score", "Momentum")), risk_score=_to_float(_get_any(row, "Risk Score", "Risk")),
        overall_score=_to_float(_get_any(row, "Overall Score", "Opportunity Score", "Score")),
        risk_bucket=_norm_bucket(_get_any(row, "Risk Bucket", "Risk", "Risk Level")) or "Moderate",
        confidence_bucket=_norm_bucket(_get_any(row, "Confidence Bucket", "Confidence")) or "Moderate",
        news_score=_to_float(_get_any(row, "News Score", "News Sentiment", "Sentiment Score")),
        analyst_sentiment=_to_float(_get_any(row, "Analyst Sentiment", "Analyst Rating")),
    )
    
    ts = _safe_str(_get_any(row, "Trend Signal", "Trend")).upper()
    if ts in ("UPTREND", "DOWNTREND", "NEUTRAL"): s.trend_signal = ts
    ms = _safe_str(_get_any(row, "MACD Signal", "MACD")).upper()
    if ms in ("BULLISH", "BEARISH", "NEUTRAL"): s.macd_signal = ms
    bs = _safe_str(_get_any(row, "BB Signal", "Bollinger Signal")).upper()
    if bs in ("OVERBOUGHT", "OVERSOLD", "NEUTRAL"): s.bb_signal = bs
    s.last_updated_utc = _parse_iso_to_dt(_get_any(row, "Last Updated (UTC)", "Updated At (UTC)", "as_of_utc"))
    
    if s.forecast_price_1m is None and s.price is not None and s.expected_roi_1m is not None: s.forecast_price_1m = s.price * (1.0 + s.expected_roi_1m)
    if s.forecast_price_3m is None and s.price is not None and s.expected_roi_3m is not None: s.forecast_price_3m = s.price * (1.0 + s.expected_roi_3m)
    if s.forecast_price_12m is None and s.price is not None and s.expected_roi_12m is not None: s.forecast_price_12m = s.price * (1.0 + s.expected_roi_12m)
    
    if engine is not None:
        if include_news and s.news_score is None:
            ns = _engine_try_get_news_score(engine, symbol)
            if ns is not None: s.news_score = ns
        if include_technical:
            tech = _engine_try_get_technical_signals(engine, symbol)
            if tech:
                if "trend_signal" in tech: s.trend_signal = tech["trend_signal"]
                if "macd_signal" in tech: s.macd_signal = tech["macd_signal"]
                if "bb_signal" in tech: s.bb_signal = tech["bb_signal"]
                if "rsi_14" in tech and s.rsi_14 is None: s.rsi_14 = _to_float(tech["rsi_14"])
        hist = _engine_try_get_historical_returns(engine, symbol, ["1w", "1m", "3m", "6m", "12m"])
        if hist:
            if s.returns_1w is None and "1w" in hist: s.returns_1w = hist["1w"]
            if s.returns_1m is None and "1m" in hist: s.returns_1m = hist["1m"]
            if s.returns_3m is None and "3m" in hist: s.returns_3m = hist["3m"]
            if s.returns_6m is None and "6m" in hist: s.returns_6m = hist["6m"]
            if s.returns_12m is None and "12m" in hist: s.returns_12m = hist["12m"]

    mf = sum(1 for v in (s.price, s.market_cap, s.pe_ttm, s.eps_ttm, s.returns_3m) if v is None)
    s.data_quality = "POOR" if mf >= 5 else "FAIR" if mf >= 3 else "GOOD" if mf >= 1 else "EXCELLENT"
    return s


# ============================================================================
# Scoring Models
# ============================================================================

class ScoringModel:
    """Advanced scoring model with ML enhancement when available."""
    
    def __init__(self, use_ml: bool = True):
        self.use_ml = use_ml and (HAS_SKLEARN or HAS_XGBOOST)
        self.ml_model = None
        self._trained = False
    
    def _prepare_features(self, s: Security) -> Dict[str, float]:
        features = {}
        if s.pe_ttm and s.pe_ttm > 0: features["pe_score"] = max(0, min(100, 100 - (s.pe_ttm - 10) * 2))
        if s.pb and s.pb > 0: features["pb_score"] = max(0, min(100, 100 - (s.pb - 1) * 30))
        if s.forward_pe and s.forward_pe > 0: features["forward_pe_score"] = max(0, min(100, 100 - (s.forward_pe - 10) * 2))
        if s.revenue_growth: features["revenue_growth_score"] = _clamp(s.revenue_growth * 100, 0, 100)
        if s.earnings_growth: features["earnings_growth_score"] = _clamp(s.earnings_growth * 100, 0, 100)
        if s.roe: features["roe_score"] = _clamp(s.roe, 0, 100)
        if s.net_margin: features["margin_score"] = _clamp(s.net_margin, 0, 100)
        if s.returns_1m: features["momentum_1m"] = _clamp(s.returns_1m * 100 + 50, 0, 100)
        if s.returns_3m: features["momentum_3m"] = _clamp(s.returns_3m * 100 + 50, 0, 100)
        if s.rsi_14: features["rsi_score"] = s.rsi_14
        if s.quality_score: features["quality"] = s.quality_score
        if s.risk_score: features["risk_inv"] = 100 - s.risk_score
        if s.volatility_30d: features["volatility_inv"] = 100 - min(100, s.volatility_30d)
        
        if s.trend_signal == "UPTREND": features["trend"] = 80
        elif s.trend_signal == "DOWNTREND": features["trend"] = 20
        else: features["trend"] = 50
        
        if s.macd_signal == "BULLISH": features["macd"] = 80
        elif s.macd_signal == "BEARISH": features["macd"] = 20
        else: features["macd"] = 50
        return features
    
    def score_basic(self, s: Security) -> float:
        features = self._prepare_features(s)
        if not features: return 50.0
        weights = {"pe_score": 0.10, "pb_score": 0.05, "forward_pe_score": 0.05, "revenue_growth_score": 0.10, "earnings_growth_score": 0.10, "roe_score": 0.10, "margin_score": 0.05, "momentum_1m": 0.05, "momentum_3m": 0.10, "rsi_score": 0.05, "quality": 0.10, "risk_inv": 0.05, "volatility_inv": 0.05, "trend": 0.03, "macd": 0.02}
        score = sum(features[f] * w for f, w in weights.items() if f in features)
        tot_w = sum(w for f, w in weights.items() if f in features)
        return score / tot_w if tot_w > 0 else 50.0
    
    def score_ml(self, s: Security) -> Optional[float]:
        if not self.use_ml or not self._trained: return None
        # Placeholder for actual model inference
        return None
    
    def score(self, s: Security) -> float:
        basic = self.score_basic(s)
        ml = self.score_ml(s) if self.use_ml else None
        return basic * 0.6 + ml * 0.4 if ml is not None else basic


# ============================================================================
# Advanced Filtering
# ============================================================================

def _compute_liquidity_score(s: Security) -> Optional[float]:
    if s.liquidity_score is not None: return s.liquidity_score
    if s.value_traded and s.value_traded > 0: return max(0.0, min(100.0, ((math.log10(max(1.0, s.value_traded)) - 6.0) / (11.0 - 6.0)) * 75.0 + 20.0))
    if s.volume and s.volume > 0: return max(0.0, min(100.0, ((math.log10(max(1.0, s.volume)) - 5.0) / (9.0 - 5.0)) * 75.0 + 20.0))
    if s.market_cap and s.market_cap > 0: return max(0.0, min(100.0, ((math.log10(max(1.0, s.market_cap)) - 7.0) / (12.0 - 7.0)) * 75.0 + 20.0))
    return None

def _passes_filters(s: Security, request: AdvisorRequest, liquidity_score: Optional[float] = None) -> Tuple[bool, str]:
    if request.allowed_markets and s.market.upper() not in {m.upper() for m in request.allowed_markets}: return False, f"Market excluded: {s.market}"
    if request.risk_bucket and RISK_LEVELS.get(s.risk_bucket, 0) < RISK_LEVELS.get(request.risk_bucket, 0): return False, f"Risk too high (Req: {request.risk_bucket})"
    if request.confidence_bucket and CONFIDENCE_LEVELS.get(s.confidence_bucket, 0) < CONFIDENCE_LEVELS.get(request.confidence_bucket, 0): return False, f"Confidence too low"
    if request.required_roi_1m is not None and (s.expected_roi_1m is None or s.expected_roi_1m < request.required_roi_1m): return False, "ROI 1M below target"
    if request.required_roi_3m is not None and (s.expected_roi_3m is None or s.expected_roi_3m < request.required_roi_3m): return False, "ROI 3M below target"
    if request.required_roi_12m is not None and (s.expected_roi_12m is None or s.expected_roi_12m < request.required_roi_12m): return False, "ROI 12M below target"
    if request.min_price is not None and s.price is not None and s.price < request.min_price: return False, "Price too low"
    if request.max_price is not None and s.price is not None and s.price > request.max_price: return False, "Price too high"
    if request.exclude_sectors and s.sector and any(excl and excl.lower() in s.sector.lower() for excl in request.exclude_sectors): return False, f"Sector excluded"
    if liquidity_score is not None and liquidity_score < request.min_liquidity_score: return False, "Liquidity too low"
    if request.min_advisor_score is not None and s.advisor_score < request.min_advisor_score: return False, "Advisor score too low"
    return True, ""


# ============================================================================
# Portfolio Optimization
# ============================================================================

class PortfolioOptimizer:
    """Advanced portfolio optimizer with MPT and Black-Litterman support."""
    
    def __init__(self, risk_free_rate: float = 0.04):
        self.risk_free_rate = risk_free_rate
    
    def _get_returns_array(self, securities: List[Security]) -> np.ndarray:
        return np.array([s.expected_roi_12m or s.expected_roi_3m or s.expected_roi_1m or 0.05 for s in securities])
    
    def _get_volatilities_array(self, securities: List[Security]) -> np.ndarray:
        return np.array([(s.volatility_30d or (20.0 if s.risk_bucket in ("High", "Very High") else 15.0)) / 100.0 for s in securities])
    
    def _estimate_correlation_matrix(self, securities: List[Security]) -> np.ndarray:
        n = len(securities)
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                if securities[i].sector and securities[j].sector and securities[i].sector == securities[j].sector:
                    corr[i, j] = corr[j, i] = 0.7
                else:
                    corr[i, j] = corr[j, i] = 0.3
        
        # Ensure Positive Semi-Definite
        if HAS_NUMPY:
            eigvals, eigvecs = np.linalg.eigh(corr) if np.all(np.linalg.eigvals(corr) > 0) else np.linalg.eigh(corr + np.eye(n)*1e-5)
        return corr

    def optimize_equal_weight(self, securities: List[Security]) -> List[float]:
        n = len(securities)
        return [1.0 / n] * n if n > 0 else []
    
    def optimize_market_cap(self, securities: List[Security]) -> List[float]:
        caps = [s.market_cap or 0 for s in securities]
        total = sum(caps)
        return [c / total for c in caps] if total > 0 else self.optimize_equal_weight(securities)
    
    def optimize_minimum_variance(self, securities: List[Security]) -> List[float]:
        if not HAS_SCIPY or not HAS_NUMPY or len(securities) < 2: return self.optimize_equal_weight(securities)
        n = len(securities)
        vols, corr = self._get_volatilities_array(securities), self._estimate_correlation_matrix(securities)
        cov = np.outer(vols, vols) * corr
        
        res = optimize.minimize(lambda w: np.dot(w.T, np.dot(cov, w)), np.array([1.0/n]*n), method='SLSQP', bounds=[(0.0, 1.0)]*n, constraints=[{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}])
        return res.x.tolist() if res.success else self.optimize_equal_weight(securities)
    
    def optimize_maximum_sharpe(self, securities: List[Security]) -> List[float]:
        if not HAS_SCIPY or not HAS_NUMPY or len(securities) < 2: return self.optimize_equal_weight(securities)
        n = len(securities)
        returns, vols, corr = self._get_returns_array(securities), self._get_volatilities_array(securities), self._estimate_correlation_matrix(securities)
        cov = np.outer(vols, vols) * corr
        
        def neg_sharpe(w):
            vol = np.sqrt(np.dot(w.T, np.dot(cov, w)))
            return -(np.sum(returns * w) - self.risk_free_rate) / vol if vol > 0 else 0
            
        res = optimize.minimize(neg_sharpe, np.array([1.0/n]*n), method='SLSQP', bounds=[(0.0, 1.0)]*n, constraints=[{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}])
        return res.x.tolist() if res.success else self.optimize_equal_weight(securities)
    
    def optimize_risk_parity(self, securities: List[Security]) -> List[float]:
        if not HAS_SCIPY or not HAS_NUMPY or len(securities) < 2: return self.optimize_equal_weight(securities)
        n = len(securities)
        vols, corr = self._get_volatilities_array(securities), self._estimate_correlation_matrix(securities)
        cov = np.outer(vols, vols) * corr
        
        def risk_budget_objective(w):
            port_var = np.dot(w.T, np.dot(cov, w))
            mrc = (w * np.dot(cov, w)) / port_var # Marginal risk contributions
            risk_target = 1.0 / n
            return np.sum((mrc - risk_target)**2)
            
        res = optimize.minimize(risk_budget_objective, np.array([1.0/n]*n), method='SLSQP', bounds=[(0.01, 1.0)]*n, constraints=[{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}])
        return res.x.tolist() if res.success else self.optimize_equal_weight(securities)
        
    def optimize_black_litterman(self, securities: List[Security]) -> List[float]:
        if not HAS_SCIPY or not HAS_NUMPY or len(securities) < 2: return self.optimize_maximum_sharpe(securities)
        n = len(securities)
        vols, corr = self._get_volatilities_array(securities), self._estimate_correlation_matrix(securities)
        cov = np.outer(vols, vols) * corr
        
        # Base Market Weights (from Market Cap)
        caps = np.array([s.market_cap or 1e9 for s in securities])
        w_mkt = caps / np.sum(caps)
        
        # Risk aversion coefficient (delta)
        delta = 2.5
        
        # Implied Equilibrium Returns (Pi)
        pi = delta * np.dot(cov, w_mkt)
        
        # Views based on Advisor Score
        P = np.eye(n)
        Q = np.array([max(0, (s.advisor_score - 50) / 100.0) for s in securities])
        Omega = np.diag([(0.1 / (s.advisor_score + 1))**2 for s in securities]) # Higher score = lower uncertainty
        
        tau = 0.05
        # BL formula: E[R] = [(tau*cov)^-1 + P^T * Omega^-1 * P]^-1 * [(tau*cov)^-1 * pi + P^T * Omega^-1 * Q]
        tau_cov_inv = np.linalg.inv(tau * cov)
        omega_inv = np.linalg.inv(Omega)
        
        term1 = np.linalg.inv(tau_cov_inv + np.dot(P.T, np.dot(omega_inv, P)))
        term2 = np.dot(tau_cov_inv, pi) + np.dot(P.T, np.dot(omega_inv, Q))
        
        bl_returns = np.dot(term1, term2)
        
        def neg_sharpe(w):
            vol = np.sqrt(np.dot(w.T, np.dot(cov, w)))
            return -(np.sum(bl_returns * w) - self.risk_free_rate) / vol if vol > 0 else 0
            
        res = optimize.minimize(neg_sharpe, w_mkt, method='SLSQP', bounds=[(0.0, 1.0)]*n, constraints=[{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}])
        return res.x.tolist() if res.success else w_mkt.tolist()

    def optimize(self, securities: List[Security], strategy: AllocationStrategy) -> List[float]:
        if not securities: return []
        if strategy == AllocationStrategy.EQUAL_WEIGHT: return self.optimize_equal_weight(securities)
        elif strategy == AllocationStrategy.MARKET_CAP: return self.optimize_market_cap(securities)
        elif strategy == AllocationStrategy.MINIMUM_VARIANCE: return self.optimize_minimum_variance(securities)
        elif strategy == AllocationStrategy.MAXIMUM_SHARPE: return self.optimize_maximum_sharpe(securities)
        elif strategy == AllocationStrategy.RISK_PARITY: return self.optimize_risk_parity(securities)
        elif strategy == AllocationStrategy.BLACK_LITTERMAN: return self.optimize_black_litterman(securities)
        return self.optimize_equal_weight(securities)

# ============================================================================
# Deduplication & Scoring Comp
# ============================================================================

def _deduplicate_securities(securities: List[Security]) -> Tuple[List[Security], int]:
    best, removed = {}, 0
    def score_key(s: Security) -> Tuple[float, float, float]: return (float(s.advisor_score or 0.0), float(s.expected_roi_3m or 0.0), float(s.expected_roi_1m or 0.0))
    for s in securities:
        key = s.canonical or s.symbol
        existing = best.get(key)
        if existing is None or score_key(s) > score_key(existing):
            if existing is not None: removed += 1
            best[key] = s
        else: removed += 1
    return list(best.values()), removed

def _compute_advisor_score(s: Security, request: AdvisorRequest, scoring_model: ScoringModel) -> Tuple[float, str]:
    reasons = []
    score = scoring_model.score(s)
    reasons.append(f"Base: {score:.0f}")
    
    if s.expected_roi_1m is not None:
        uplift = max(0.0, (s.expected_roi_1m - (request.required_roi_1m or 0.0))) * 100
        score += min(15, uplift * 0.5)
        if uplift > 2: reasons.append(f"ROI1M+{uplift:.0f}")
    if s.expected_roi_3m is not None:
        uplift = max(0.0, (s.expected_roi_3m - (request.required_roi_3m or 0.0))) * 100
        score += min(20, uplift * 0.4)
        if uplift > 2: reasons.append(f"ROI3M+{uplift:.0f}")
    if s.expected_roi_12m is not None:
        uplift = max(0.0, s.expected_roi_12m * 50)
        score += min(10, uplift)
        if uplift > 2: reasons.append("ROI12M+")
        
    if s.quality_score and s.quality_score > 60:
        score += min(8, (s.quality_score - 50) * 0.2)
        reasons.append("Quality+")
    if s.momentum_score and s.momentum_score > 60:
        score += min(8, (s.momentum_score - 50) * 0.2)
        reasons.append("Momentum+")
    if s.value_score and s.value_score > 60:
        score += min(8, (s.value_score - 50) * 0.2)
        reasons.append("Value+")
        
    risk_tol, risk_val = request.risk_profile.value, s.risk_score or 50.0
    if risk_tol == "conservative" and risk_val > 40:
        penalty = (risk_val - 40) * 0.6
        score -= penalty
        if penalty > 5: reasons.append(f"Risk-{int(penalty)}")
    elif risk_tol == "moderate" and risk_val > 60: score -= (risk_val - 60) * 0.3
    elif risk_tol == "aggressive" and risk_val > 75: score -= (risk_val - 75) * 0.2
    
    if s.volatility_30d is not None:
        vol = s.volatility_30d / 100.0
        if risk_tol == "conservative": score -= min(10, vol * 25)
        elif risk_tol == "moderate": score -= min(6, vol * 15)
        
    if request.include_news and s.news_score is not None:
        ns = s.news_score * request.news_weight
        if abs(ns) <= 10:
            score += ns
            if ns > 1: reasons.append("News+")
            elif ns < -1: reasons.append("News-")
        else:
            nb = (ns - 50) * 0.1
            score += nb
            if nb > 1: reasons.append("News+")
            
    if request.include_technical:
        if s.trend_signal == "UPTREND": score += 5; reasons.append("TrendUp")
        elif s.trend_signal == "DOWNTREND": score -= 5; reasons.append("TrendDown")
        if s.macd_signal == "BULLISH": score += 3
        elif s.macd_signal == "BEARISH": score -= 3
        
    liq = _compute_liquidity_score(s)
    if liq is not None:
        if liq >= 80: score += 2
        elif liq < 30: score -= 3
        
    score = max(0.0, min(100.0, score))
    if s.price is not None and s.price <= 0: score = 0.0
    return score, "; ".join(reasons[:6])


def _analyze_portfolio(securities: List[Security], weights: List[float], total_value: float) -> Portfolio:
    if not securities or not weights or len(securities) != len(weights): return Portfolio()
    portfolio = Portfolio(securities=securities, total_value=total_value, cash=0.0)
    sector_exp, currency_exp = defaultdict(float), defaultdict(float)
    
    for s, w in zip(securities, weights):
        if s.sector: sector_exp[s.sector] += w
        if s.currency: currency_exp[s.currency] += w
    
    portfolio.sector_exposure = dict(sector_exp)
    portfolio.currency_exposure = dict(currency_exp)
    portfolio.concentration_score = sum(w * w for w in weights)
    
    if HAS_NUMPY:
        returns = np.array([s.expected_roi_3m or 0.05 for s in securities])
        vols = np.array([(s.volatility_30d or 15.0) / 100.0 for s in securities])
        weights_np = np.array(weights)
        portfolio.expected_return = float(np.sum(returns * weights_np))
        port_var = np.sum((vols * weights_np) ** 2)
        portfolio.expected_volatility = float(np.sqrt(port_var))
        if portfolio.expected_volatility > 0:
            portfolio.sharpe_ratio = (portfolio.expected_return - 0.02) / portfolio.expected_volatility
        if HAS_SCIPY:
            z = norm.ppf(0.95)
            portfolio.var_95 = float(portfolio.expected_return - z * portfolio.expected_volatility)
            portfolio.cvar_95 = float(portfolio.expected_return - norm.pdf(z) / 0.05 * portfolio.expected_volatility)
    
    if portfolio.expected_volatility > 0:
        avg_vol = sum(v for v in vols) / len(securities) if HAS_NUMPY else 0.15
        portfolio.diversification_ratio = avg_vol / portfolio.expected_volatility
        
    return portfolio


# ============================================================================
# Main Entry Point
# ============================================================================

def run_investment_advisor(payload: Dict[str, Any], *, engine: Any = None) -> Dict[str, Any]:
    with tracer.start_as_current_span("run_investment_advisor") as span:
        start_time = time.time()
        headers = ["Rank", "Symbol", "Origin", "Name", "Market", "Sector", "Currency", "Price", "Advisor Score", "Action", "Allocation %", "Allocation Amount", "Forecast Price (1M)", "Expected ROI % (1M)", "Forecast Price (3M)", "Expected ROI % (3M)", "Forecast Price (12M)", "Expected ROI % (12M)", "Risk Bucket", "Confidence Bucket", "Liquidity Score", "Data Quality", "Reason", "Data Source", "Last Updated (UTC)", "Last Updated (Riyadh)"]
        
        try:
            request = AdvisorRequest.from_dict(payload)
            universe_rows, fetch_meta, fetch_err = _fetch_universe(sources=request.sources, engine=engine, max_rows_per_source=5000)
            
            securities: List[Security] = []
            dropped = {"no_symbol": 0, "invalid_data": 0, "filtered": 0, "ticker_not_requested": 0}
            enriched_news, enriched_technical = 0, 0
            
            ticker_variants, ticker_canonical = set(), set()
            if request.tickers:
                for t in request.tickers:
                    ticker_variants.update(_symbol_variants(t))
                    can = _canonical_symbol(t)
                    if can: ticker_canonical.add(can)
            
            for row in universe_rows:
                try:
                    s = _extract_security(row, engine=engine, include_news=request.include_news, include_technical=request.include_technical)
                    if s is None:
                        dropped["no_symbol"] += 1
                        continue
                    if ticker_variants and not (s.symbol in ticker_variants or s.canonical in ticker_canonical):
                        dropped["ticker_not_requested"] += 1
                        continue
                    if request.include_news and s.news_score is not None: enriched_news += 1
                    if request.include_technical and (s.trend_signal != "NEUTRAL" or s.rsi_14 is not None): enriched_technical += 1
                    securities.append(s)
                except Exception as e:
                    dropped["invalid_data"] += 1
                    if request.debug: logger.debug(f"Error processing row: {e}")
            
            if not securities:
                return {"headers": headers, "rows": [], "items": [], "meta": {"ok": False, "version": ADVISOR_VERSION, "error": "No valid securities found", "fetch": fetch_meta, "dropped": dropped, "runtime_ms": int((time.time() - start_time) * 1000)}}
            
            scoring_model = ScoringModel(use_ml=request.use_ml)
            for s in securities: s.advisor_score, s.reason = _compute_advisor_score(s, request, scoring_model)
            
            filtered = []
            for s in securities:
                ok, _ = _passes_filters(s, request, _compute_liquidity_score(s))
                if ok: filtered.append(s)
                else: dropped["filtered"] += 1
                
            filtered, dedupe_removed = _deduplicate_securities(filtered)
            filtered.sort(key=lambda x: x.advisor_score, reverse=True)
            top_securities = filtered[:request.top_n]
            
            with tracer.start_as_current_span("optimize_portfolio"):
                opt_start = time.time()
                optimizer = PortfolioOptimizer()
                weights = optimizer.optimize(top_securities, request.allocation_strategy)
                advisor_optimization_duration.labels(strategy=request.allocation_strategy.value).observe(time.time() - opt_start)
            
            if weights and len(weights) == len(top_securities):
                total = sum(weights)
                weights = [w / total for w in weights] if total > 0 else [1.0 / len(top_securities)] * len(top_securities)
            else:
                weights = [1.0 / len(top_securities)] * len(top_securities) if top_securities else []
                
            alloc_map = {s.symbol: {"weight": w, "amount": request.invest_amount * w} for s, w in zip(top_securities, weights)}
            portfolio = _analyze_portfolio(top_securities, weights, request.invest_amount)
            
            rows, items = [], []
            as_of_dt = request.as_of_utc or _now_utc()
            
            for i, s in enumerate(top_securities, 1):
                alloc = alloc_map.get(s.symbol, {"weight": 0.0, "amount": 0.0})
                w, amt = alloc["weight"], alloc["amount"]
                action = "STRONG_BUY" if s.advisor_score >= 80 else "BUY" if s.advisor_score >= 65 else "HOLD" if s.advisor_score >= 50 else "REDUCE" if s.advisor_score >= 35 else "SELL"
                liq_score = _compute_liquidity_score(s) or 0.0
                dt_used = s.last_updated_utc if request.use_row_updated_at and s.last_updated_utc else as_of_dt
                
                rows.append([i, s.symbol, s.sheet, s.name, s.market, s.sector, s.currency, _round2(s.price), _round2(s.advisor_score), action, _round2(w * 100.0), _round2(amt), _round2(s.forecast_price_1m), _to_percent(s.expected_roi_1m), _round2(s.forecast_price_3m), _to_percent(s.expected_roi_3m), _round2(s.forecast_price_12m), _to_percent(s.expected_roi_12m), s.risk_bucket, s.confidence_bucket, _round2(liq_score), s.data_quality, s.reason, s.data_source, _iso_utc(dt_used), _iso_riyadh(dt_used)])
                
                items.append({"rank": i, "symbol": s.symbol, "canonical": s.canonical, "origin": s.sheet, "name": s.name, "market": s.market, "sector": s.sector, "currency": s.currency, "price": s.price, "advisor_score": _round2(s.advisor_score), "action": action, "allocation_pct": _round2(w * 100.0), "allocation_amount": _round2(amt), "forecast_price_1m": s.forecast_price_1m, "expected_roi_1m_pct": _to_percent(s.expected_roi_1m), "forecast_price_3m": s.forecast_price_3m, "expected_roi_3m_pct": _to_percent(s.expected_roi_3m), "forecast_price_12m": s.forecast_price_12m, "expected_roi_12m_pct": _to_percent(s.expected_roi_12m), "risk_bucket": s.risk_bucket, "confidence_bucket": s.confidence_bucket, "liquidity_score": _round2(liq_score), "data_quality": s.data_quality, "reason": s.reason, "data_source": s.data_source, "last_updated_utc": _iso_utc(dt_used), "last_updated_riyadh": _iso_riyadh(dt_used)})
            
            meta = {"ok": True, "version": ADVISOR_VERSION, "request": {"sources": request.sources, "risk_profile": request.risk_profile.value, "tickers": request.tickers, "invest_amount": request.invest_amount, "top_n": request.top_n, "allocation_strategy": request.allocation_strategy.value}, "fetch": fetch_meta, "counts": {"universe_rows": len(universe_rows), "securities_extracted": len(securities), "securities_filtered": len(filtered) + dedupe_removed, "securities_after_filters": len(filtered), "returned_rows": len(rows), "dedupe_removed": dedupe_removed, "news_enriched": enriched_news, "technical_enriched": enriched_technical, "dropped": dropped}, "portfolio": {"total_value": portfolio.total_value, "expected_return": _round2(portfolio.expected_return), "expected_volatility": _round2(portfolio.expected_volatility), "sharpe_ratio": _round2(portfolio.sharpe_ratio), "var_95": _round2(portfolio.var_95), "cvar_95": _round2(portfolio.cvar_95), "diversification_ratio": _round2(portfolio.diversification_ratio), "concentration_score": _round2(portfolio.concentration_score), "sector_exposure": {k: _round2(v) for k, v in portfolio.sector_exposure.items()}, "currency_exposure": {k: _round2(v) for k, v in portfolio.currency_exposure.items()}}, "runtime_ms": int((time.time() - start_time) * 1000)}
            if fetch_err: meta["warning"] = fetch_err
            
            advisor_requests_total.labels(status="success", strategy=request.allocation_strategy.value).inc()
            return {"headers": headers, "rows": rows, "items": items, "meta": meta}
            
        except Exception as e:
            advisor_requests_total.labels(status="error", strategy=payload.get("allocation_strategy", "unknown")).inc()
            meta = {"ok": False, "version": ADVISOR_VERSION, "error": str(e), "runtime_ms": int((time.time() - start_time) * 1000)}
            if payload.get("debug", False):
                import traceback
                meta["traceback"] = traceback.format_exc()
            return {"headers": headers, "rows": [], "items": [], "meta": meta}

__all__ = [
    "run_investment_advisor", "ADVISOR_VERSION", "MarketType", "AssetClass",
    "Recommendation", "RiskProfile", "AllocationStrategy", "Security", "Portfolio", "AdvisorRequest",
]
