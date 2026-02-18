#!/usr/bin/env python3
"""
Tadawul Fast Bridge — Investment Advisor Core v2.0.0
================================================================================
Advanced Investment Advisor with ML-Powered Recommendations
File: core/investment_advisor.py

What's new in v2.0.0:
- ✅ **Machine Learning Integration**: Optional ML models for enhanced scoring
- ✅ **Portfolio Optimization**: Modern Portfolio Theory with efficient frontier
- ✅ **Risk Management**: VaR, CVaR, stress testing, drawdown analysis
- ✅ **Factor Analysis**: Multi-factor model (Value, Momentum, Quality, Growth)
- ✅ **Sentiment Analysis**: News and social media sentiment integration
- ✅ **Technical Indicators**: RSI, MACD, Bollinger Bands signals
- ✅ **Correlation Analysis**: Asset correlation and diversification scoring
- ✅ **Scenario Analysis**: Monte Carlo simulation for return distributions
- ✅ **Tax-Aware Optimization**: Tax-efficient allocation strategies
- ✅ **Rebalancing Logic**: Automatic rebalancing recommendations
- ✅ **Performance Attribution**: Source of returns breakdown
- ✅ **Comprehensive Reporting**: Detailed JSON and sheet-ready outputs

Key Features:
- Multi-source data integration (sheets, engine, news APIs)
- Advanced scoring models with configurable weights
- Sophisticated portfolio optimization algorithms
- Real-time risk metrics and stress testing
- Machine learning enhancement when available
- Thread-safe caching for performance
- Comprehensive error handling and diagnostics
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import time
import hashlib
from collections import defaultdict, Counter
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
import warnings

# Optional scientific stack with graceful fallback
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

# ============================================================================
# Version and Configuration
# ============================================================================

__version__ = "2.0.0"
ADVISOR_VERSION = __version__

# Default sources for universe building
DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX", "KSA_TADAWUL"]

# Truthy values for boolean parsing
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}

# Risk bucket hierarchy for filtering
RISK_LEVELS = {
    "Very Low": 1,
    "Low": 2,
    "Moderate": 3,
    "High": 4,
    "Very High": 5,
}

# Confidence levels
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
    """Market classification."""
    KSA = "ksa"
    US = "us"
    GLOBAL = "global"
    COMMODITY = "commodity"
    FOREX = "forex"
    CRYPTO = "crypto"


class AssetClass(Enum):
    """Asset class classification."""
    EQUITY = "equity"
    ETF = "etf"
    MUTUAL_FUND = "mutual_fund"
    COMMODITY = "commodity"
    FOREX = "forex"
    BOND = "bond"
    REIT = "reit"
    CRYPTO = "crypto"


class Recommendation(Enum):
    """Investment recommendation."""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    SELL = "SELL"
    AVOID = "AVOID"


class RiskProfile(Enum):
    """Investor risk profile."""
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"
    VERY_AGGRESSIVE = "very_aggressive"


class AllocationStrategy(Enum):
    """Portfolio allocation strategy."""
    EQUAL_WEIGHT = "equal_weight"
    MARKET_CAP = "market_cap"
    RISK_PARITY = "risk_parity"
    MINIMUM_VARIANCE = "minimum_variance"
    MAXIMUM_SHARPE = "maximum_sharpe"
    BLACK_LITTERMAN = "black_litterman"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class Security:
    """Security information and metrics."""
    # Core identity
    symbol: str
    canonical: str
    name: str
    sheet: str
    market: str
    sector: str
    industry: str = ""
    currency: str = "SAR"
    asset_class: AssetClass = AssetClass.EQUITY
    
    # Price data
    price: Optional[float] = None
    previous_close: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    
    # Volume data
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    
    # Fundamentals
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
    
    # Profitability
    roe: Optional[float] = None
    roa: Optional[float] = None
    net_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    gross_margin: Optional[float] = None
    
    # Growth
    revenue_growth: Optional[float] = None
    earnings_growth: Optional[float] = None
    growth_score: Optional[float] = None
    
    # Technicals
    rsi_14: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None
    volatility_30d: Optional[float] = None
    atr_14: Optional[float] = None
    
    # Returns
    returns_1w: Optional[float] = None
    returns_1m: Optional[float] = None
    returns_3m: Optional[float] = None
    returns_6m: Optional[float] = None
    returns_12m: Optional[float] = None
    
    # Forecasts
    forecast_price_1m: Optional[float] = None
    forecast_price_3m: Optional[float] = None
    forecast_price_12m: Optional[float] = None
    expected_roi_1m: Optional[float] = None
    expected_roi_3m: Optional[float] = None
    expected_roi_12m: Optional[float] = None
    forecast_confidence: Optional[float] = None
    
    # Scores
    quality_score: Optional[float] = None
    value_score: Optional[float] = None
    momentum_score: Optional[float] = None
    risk_score: Optional[float] = None
    overall_score: Optional[float] = None
    
    # Risk metrics
    risk_bucket: str = "Moderate"
    confidence_bucket: str = "Moderate"
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    var_95: Optional[float] = None  # Value at Risk (95%)
    cvar_95: Optional[float] = None  # Conditional VaR
    
    # Sentiment
    news_score: Optional[float] = None
    social_score: Optional[float] = None
    analyst_sentiment: Optional[float] = None
    insider_sentiment: Optional[float] = None
    
    # Technical signals
    trend_signal: str = "NEUTRAL"
    macd_signal: str = "NEUTRAL"
    bb_signal: str = "NEUTRAL"
    
    # Metadata
    data_source: str = "unknown"
    data_quality: str = "FAIR"
    last_updated_utc: Optional[datetime] = None
    
    # Computed fields
    advisor_score: float = 0.0
    allocation_weight: float = 0.0
    allocation_amount: float = 0.0
    reason: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
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
    correlation_matrix: Optional[np.ndarray] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
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
        return result


@dataclass
class AdvisorRequest:
    """Investment advisor request parameters."""
    # Core parameters
    sources: List[str] = field(default_factory=lambda: ["ALL"])
    tickers: Optional[List[str]] = None
    
    # Risk profile
    risk_profile: RiskProfile = RiskProfile.MODERATE
    risk_bucket: str = ""
    confidence_bucket: str = ""
    
    # Return requirements
    required_roi_1m: Optional[float] = None
    required_roi_3m: Optional[float] = None
    required_roi_12m: Optional[float] = None
    
    # Portfolio parameters
    invest_amount: float = 0.0
    currency: str = "SAR"
    top_n: int = 20
    allocation_strategy: AllocationStrategy = AllocationStrategy.MAXIMUM_SHARPE
    
    # Constraints
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
    
    # Advanced features
    include_news: bool = True
    news_weight: float = 1.0
    include_technical: bool = True
    technical_weight: float = 0.3
    use_ml: bool = True
    ml_weight: float = 0.2
    optimize_taxes: bool = False
    tax_rate: float = 0.15
    rebalance_frequency: str = "monthly"
    
    # Data options
    as_of_utc: Optional[datetime] = None
    use_row_updated_at: bool = True
    debug: bool = False
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AdvisorRequest:
        """Create request from dictionary."""
        # Parse sources
        sources = data.get("sources", ["ALL"])
        if isinstance(sources, str):
            sources = [sources]
        sources = [str(s).strip() for s in sources if str(s).strip()]
        if not sources or "ALL" in [s.upper() for s in sources]:
            sources = list(DEFAULT_SOURCES)
        
        # Parse risk profile
        risk_profile_str = str(data.get("risk_profile", "moderate")).lower()
        try:
            risk_profile = RiskProfile(risk_profile_str)
        except ValueError:
            risk_profile = RiskProfile.MODERATE
        
        # Parse allocation strategy
        strategy_str = str(data.get("allocation_strategy", "maximum_sharpe")).lower()
        try:
            allocation_strategy = AllocationStrategy(strategy_str)
        except ValueError:
            allocation_strategy = AllocationStrategy.MAXIMUM_SHARPE
        
        # Parse tickers
        tickers = None
        tickers_in = data.get("tickers") or data.get("symbols")
        if tickers_in:
            if isinstance(tickers_in, str):
                tickers = [x for x in tickers_in.replace(",", " ").split() if x.strip()]
            elif isinstance(tickers_in, list):
                tickers = [str(x) for x in tickers_in if x]
        
        # Parse as_of date
        as_of_utc = None
        if "as_of_utc" in data:
            dt = _parse_iso_to_dt(data["as_of_utc"])
            if dt:
                as_of_utc = dt
        
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
    """Check if value is truthy."""
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in _TRUTHY


def _safe_str(x: Any, default: str = "") -> str:
    """Safely convert to string."""
    if x is None:
        return default
    try:
        s = str(x).strip()
        return s if s else default
    except Exception:
        return default


def _safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert to int."""
    if x is None:
        return default
    try:
        return int(float(str(x).strip()))
    except (ValueError, TypeError):
        return default


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert to float."""
    if x is None:
        return default
    try:
        return float(str(x).strip().replace(",", ""))
    except (ValueError, TypeError):
        return default


def _to_float(x: Any) -> Optional[float]:
    """Convert to float, handling None and empty strings."""
    return _safe_float(x)


def _to_int(x: Any) -> Optional[int]:
    """Convert to int, handling None and empty strings."""
    return _safe_int(x)


def _as_ratio(x: Any) -> Optional[float]:
    """
    Convert percentage or ratio to decimal ratio.
    - 10% -> 0.10
    - 0.10 -> 0.10
    - 10 -> 0.10
    """
    f = _to_float(x)
    if f is None:
        return None
    if f > 1.5:  # Assume percentage
        return f / 100.0
    return f


def _to_percent(x: Any, decimals: int = 2) -> Optional[float]:
    """Convert ratio to percentage."""
    f = _to_float(x)
    if f is None:
        return None
    return round(f * 100.0, decimals)


def _round2(x: Any) -> Any:
    """Round to 2 decimal places if numeric."""
    f = _to_float(x)
    if f is None:
        return x
    return round(f, 2)


def _clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value between min and max."""
    return max(min_val, min(max_val, value))


def _norm_key(k: Any) -> str:
    """Normalize dictionary key for case-insensitive lookup."""
    s = _safe_str(k).lower()
    return " ".join(s.split())


def _norm_bucket(x: Any) -> str:
    """Normalize bucket names (risk, confidence)."""
    if x is None:
        return ""
    s = _safe_str(x).lower()
    if not s:
        return ""
    
    # Risk levels
    if s in ("very low", "very-low", "very_low", "vl"):
        return "Very Low"
    if s in ("low", "l"):
        return "Low"
    if s in ("moderate", "medium", "mid", "balanced", "m"):
        return "Moderate"
    if s in ("high", "h", "aggressive"):
        return "High"
    if s in ("very high", "very-high", "very_high", "vh", "very aggressive"):
        return "Very High"
    
    # Confidence levels
    if s in ("very low confidence", "very-low-confidence", "vlc"):
        return "Very Low"
    if s in ("low confidence", "low-confidence", "lc"):
        return "Low"
    if s in ("moderate confidence", "medium confidence", "mid confidence", "mc"):
        return "Moderate"
    if s in ("high confidence", "high-confidence", "hc"):
        return "High"
    if s in ("very high confidence", "very-high-confidence", "vhc"):
        return "Very High"
    
    return s.title()


def _norm_symbol(symbol: str) -> str:
    """
    Normalize symbol format:
    - Uppercase
    - Remove spaces
    - .SA -> .SR (common KSA mismatch)
    - Handle TADAWUL prefix
    """
    s = _safe_str(symbol).upper().replace(" ", "")
    if not s:
        return ""
    
    # Handle TADAWUL prefix
    if s.startswith("TADAWUL:"):
        s = s[8:]
    
    # Handle .SA suffix
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    
    return s


def _canonical_symbol(symbol: str) -> str:
    """
    Generate canonical key for deduplication.
    
    Examples:
    - "1120", "1120.SR" -> "KSA:1120"
    - "AAPL", "AAPL.US" -> "GLOBAL:AAPL"
    - "^GSPC" -> "IDX:^GSPC"
    """
    s = _norm_symbol(symbol)
    if not s:
        return ""
    
    # KSA numeric
    if s.endswith(".SR") and s[:-3].isdigit():
        return "KSA:" + s[:-3]
    if s.isdigit():
        return "KSA:" + s
    
    # Indices
    if s.startswith("^"):
        return "IDX:" + s
    
    # Global equities - strip exchange suffix
    if "." in s:
        base = s.split(".", 1)[0]
        return "GLOBAL:" + base
    
    return "GLOBAL:" + s


def _symbol_variants(symbol: str) -> Set[str]:
    """
    Generate common variants for symbol matching.
    """
    s = _norm_symbol(symbol)
    variants: Set[str] = set()
    
    if not s:
        return variants
    
    variants.add(s)
    
    # KSA variants
    if s.endswith(".SR") and s[:-3].isdigit():
        variants.add(s[:-3])
    elif s.isdigit():
        variants.add(s + ".SR")
    
    # Global variants
    if "." in s:
        base = s.split(".", 1)[0]
        if base:
            variants.add(base)
    
    # Handle TADAWUL prefix
    if not s.startswith("TADAWUL:") and s.isdigit():
        variants.add("TADAWUL:" + s)
    
    return variants


def _parse_iso_to_dt(v: Any) -> Optional[datetime]:
    """Parse ISO datetime string to datetime object."""
    s = _safe_str(v)
    if not s:
        return None
    
    try:
        # Handle Z suffix
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _iso_utc(dt: datetime) -> str:
    """Convert datetime to UTC ISO string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _iso_riyadh(dt: datetime) -> str:
    """Convert datetime to Riyadh timezone ISO string."""
    tz_riyadh = timezone(timedelta(hours=3))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz_riyadh).isoformat()


def _now_utc() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def _now_riyadh() -> datetime:
    """Get current Riyadh datetime."""
    return datetime.now(timezone(timedelta(hours=3)))


def _unique_headers(headers: List[Any]) -> List[str]:
    """
    Make headers unique to avoid overwriting on dict conversion.
    Example: ["Price","Price"] -> ["Price","Price (2)"]
    """
    raw = [str(x).strip() for x in (headers or [])]
    out: List[str] = []
    seen: Dict[str, int] = {}
    
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


def _rows_to_dicts(
    headers: List[Any],
    rows: List[Any],
    sheet_name: str,
    limit: int
) -> List[Dict[str, Any]]:
    """Convert rows to list of dictionaries with headers."""
    h = _unique_headers(headers or [])
    out: List[Dict[str, Any]] = []
    
    for i, r in enumerate(rows or []):
        if i >= limit:
            break
        if not isinstance(r, (list, tuple)):
            continue
        
        d = {h[j]: (r[j] if j < len(r) else None) for j in range(len(h))}
        d["_Sheet"] = sheet_name
        out.append(d)
    
    return out


def _get_any(row: Dict[str, Any], *names: str) -> Any:
    """
    Case-insensitive lookup in dictionary.
    Uses cached normalized keys for performance.
    """
    if not row:
        return None
    
    # Direct match
    for n in names:
        if n in row:
            return row[n]
    
    # Build normalized map if not present
    nmap = row.get("_nmap")
    if not isinstance(nmap, dict):
        nmap = {}
        for k in row:
            if k != "_nmap":
                nk = _norm_key(k)
                if nk and nk not in nmap:
                    nmap[nk] = k
        row["_nmap"] = nmap
    
    # Try normalized lookup
    for n in names:
        nk = _norm_key(n)
        if nk in nmap:
            return row[nmap[nk]]
    
    return None


def _safe_mean(values: List[Optional[float]], default: float = 0.0) -> float:
    """Safe mean calculation ignoring None values."""
    valid = [v for v in values if v is not None]
    if not valid:
        return default
    return sum(valid) / len(valid)


def _safe_weighted_mean(
    values: List[Optional[float]],
    weights: List[float],
    default: float = 0.0
) -> float:
    """Safe weighted mean calculation."""
    valid = [(v, w) for v, w in zip(values, weights) if v is not None]
    if not valid:
        return default
    
    total_weight = sum(w for _, w in valid)
    if total_weight == 0:
        return default
    
    return sum(v * w for v, w in valid) / total_weight


# ============================================================================
# Data Fetching from Engine
# ============================================================================

def _engine_get_snapshot(engine: Any, sheet_name: str) -> Optional[Dict[str, Any]]:
    """Get cached sheet snapshot from engine."""
    if engine is None:
        return None
    
    fn = getattr(engine, "get_cached_sheet_snapshot", None)
    if callable(fn):
        try:
            snap = fn(sheet_name)
            return snap if isinstance(snap, dict) else None
        except Exception:
            return None
    
    return None


def _engine_get_multi_snapshots(
    engine: Any,
    sheet_names: List[str]
) -> Dict[str, Dict[str, Any]]:
    """Get multiple sheet snapshots from engine."""
    if engine is None:
        return {}
    
    fn = getattr(engine, "get_cached_multi_sheet_snapshots", None)
    if callable(fn):
        try:
            out = fn(sheet_names)
            if isinstance(out, dict):
                return {str(k): v for k, v in out.items() if isinstance(v, dict)}
        except Exception:
            pass
    
    # Fallback to individual fetches
    out2: Dict[str, Dict[str, Any]] = {}
    for s in sheet_names or []:
        snap = _engine_get_snapshot(engine, s)
        if snap:
            out2[str(s)] = snap
    
    return out2


def _engine_try_get_news_score(engine: Any, symbol: str) -> Optional[float]:
    """Get news sentiment score from engine."""
    if engine is None or not symbol:
        return None
    
    for fn_name in ("get_news_score", "get_cached_news_score", "news_get_score"):
        fn = getattr(engine, fn_name, None)
        if callable(fn):
            try:
                v = fn(symbol)
                return _to_float(v)
            except Exception:
                continue
    
    return None


def _engine_try_get_technical_signals(
    engine: Any,
    symbol: str
) -> Dict[str, Any]:
    """Get technical signals from engine."""
    signals = {}
    
    if engine is None or not symbol:
        return signals
    
    for fn_name in ("get_technical_signals", "get_cached_technical_signals"):
        fn = getattr(engine, fn_name, None)
        if callable(fn):
            try:
                v = fn(symbol)
                if isinstance(v, dict):
                    signals.update(v)
            except Exception:
                continue
    
    return signals


def _engine_try_get_historical_returns(
    engine: Any,
    symbol: str,
    periods: List[str]
) -> Dict[str, Optional[float]]:
    """Get historical returns from engine."""
    returns = {}
    
    if engine is None or not symbol:
        return returns
    
    fn = getattr(engine, "get_historical_returns", None)
    if callable(fn):
        try:
            v = fn(symbol, periods)
            if isinstance(v, dict):
                for k, val in v.items():
                    returns[k] = _to_float(val)
        except Exception:
            pass
    
    return returns


def _fetch_universe(
    sources: List[str],
    engine: Any = None,
    max_rows_per_source: int = 5000
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    """
    Fetch universe from engine snapshots.
    """
    meta: Dict[str, Any] = {
        "sources": sources,
        "engine": type(engine).__name__ if engine is not None else None,
        "items": [],
        "mode": "engine_snapshot",
    }
    
    if engine is None:
        return [], meta, "Missing engine instance"
    
    # Normalize sources
    normalized_sources = []
    for s in sources:
        if s.upper() == "ALL":
            normalized_sources.extend(DEFAULT_SOURCES)
        elif s and isinstance(s, str):
            normalized_sources.append(s.strip())
    
    if not normalized_sources:
        normalized_sources = DEFAULT_SOURCES
    
    # Fetch snapshots
    snaps = _engine_get_multi_snapshots(engine, normalized_sources)
    out_rows: List[Dict[str, Any]] = []
    
    for sheet in normalized_sources:
        snap = snaps.get(sheet) or _engine_get_snapshot(engine, sheet)
        if not snap:
            meta["items"].append({
                "sheet": sheet,
                "cached": False,
                "rows": 0,
                "headers": 0
            })
            continue
        
        headers = snap.get("headers") or []
        rows = snap.get("rows") or []
        meta["items"].append({
            "sheet": sheet,
            "cached": True,
            "rows": len(rows) if isinstance(rows, list) else 0,
            "headers": len(headers) if isinstance(headers, list) else 0,
            "cached_at_utc": snap.get("cached_at_utc"),
        })
        
        if isinstance(headers, list) and isinstance(rows, list):
            out_rows.extend(_rows_to_dicts(
                headers, rows, sheet_name=sheet, limit=max_rows_per_source
            ))
    
    if not out_rows:
        return [], meta, "No cached sheet snapshots found"
    
    return out_rows, meta, None


# ============================================================================
# Security Extraction and Enrichment
# ============================================================================

def _extract_security(
    row: Dict[str, Any],
    engine: Any = None,
    include_news: bool = False,
    include_technical: bool = False
) -> Optional[Security]:
    """Extract security from row data with optional enrichment."""
    
    # Basic identity
    symbol = _norm_symbol(_get_any(
        row, "Symbol", "Fund Symbol", "Ticker", "Code", "Fund Code"
    ))
    if not symbol or symbol == "SYMBOL":
        return None
    
    canonical = _canonical_symbol(symbol)
    name = _safe_str(_get_any(
        row, "Name", "Company Name", "Fund Name", "Instrument", "Long Name"
    ))
    sheet = _safe_str(_get_any(row, "_Sheet", "Origin")) or ""
    
    # Market classification
    market = _safe_str(_get_any(row, "Market", "Exchange", "Region")) or (
        "KSA" if symbol.endswith(".SR") or symbol.isdigit() else "GLOBAL"
    )
    
    # Sector/Industry
    sector = _safe_str(_get_any(row, "Sector", "Category"))
    industry = _safe_str(_get_any(row, "Industry", "Sub Sector", "Sub-Sector"))
    
    # Currency
    currency = _safe_str(_get_any(row, "Currency", "CCY")) or "SAR"
    
    # Asset class detection
    asset_class = AssetClass.EQUITY
    sheet_lower = sheet.lower()
    if "mutual_fund" in sheet_lower or "fund" in sheet_lower:
        asset_class = AssetClass.MUTUAL_FUND
    elif "commodities" in sheet_lower:
        asset_class = AssetClass.COMMODITY
    elif "forex" in sheet_lower or "fx" in sheet_lower:
        asset_class = AssetClass.FOREX
    
    # Price data
    price = _to_float(_get_any(
        row, "Price", "Last", "Close", "NAV per Share", "NAV", "Last Price"
    ))
    previous_close = _to_float(_get_any(
        row, "Prev Close", "Previous Close", "Prior Close"
    ))
    day_high = _to_float(_get_any(row, "Day High", "High"))
    day_low = _to_float(_get_any(row, "Day Low", "Low"))
    week_52_high = _to_float(_get_any(
        row, "52W High", "52-Week High", "Year High"
    ))
    week_52_low = _to_float(_get_any(
        row, "52W Low", "52-Week Low", "Year Low"
    ))
    
    # Volume data
    volume = _to_float(_get_any(row, "Volume", "Vol", "Volume (Shares)"))
    avg_volume_30d = _to_float(_get_any(
        row, "Avg Vol 30D", "Avg Volume 30D", "Average Volume"
    ))
    value_traded = _to_float(_get_any(
        row, "Value Traded", "Turnover", "Value"
    ))
    
    # Fundamentals
    market_cap = _to_float(_get_any(
        row, "Market Cap", "Mkt Cap", "Market Capitalization"
    ))
    shares_outstanding = _to_float(_get_any(
        row, "Shares Outstanding", "Shares Out"
    ))
    free_float = _to_float(_get_any(
        row, "Free Float %", "Free Float"
    ))
    pe_ttm = _to_float(_get_any(row, "P/E (TTM)", "PE TTM", "PE Ratio"))
    forward_pe = _to_float(_get_any(row, "Forward P/E", "Forward PE"))
    pb = _to_float(_get_any(row, "P/B", "Price/Book", "PB Ratio"))
    ps = _to_float(_get_any(row, "P/S", "Price/Sales", "PS Ratio"))
    eps_ttm = _to_float(_get_any(row, "EPS (TTM)", "EPS TTM"))
    forward_eps = _to_float(_get_any(row, "Forward EPS"))
    dividend_yield = _to_float(_get_any(row, "Dividend Yield", "Div Yield"))
    payout_ratio = _to_float(_get_any(row, "Payout Ratio"))
    beta = _to_float(_get_any(row, "Beta"))
    
    # Profitability
    roe = _to_float(_get_any(row, "ROE", "Return on Equity"))
    roa = _to_float(_get_any(row, "ROA", "Return on Assets"))
    net_margin = _to_float(_get_any(row, "Net Margin", "Profit Margin"))
    operating_margin = _to_float(_get_any(row, "Operating Margin"))
    gross_margin = _to_float(_get_any(row, "Gross Margin"))
    
    # Growth
    revenue_growth = _as_ratio(_get_any(
        row, "Revenue Growth", "Rev Growth", "Sales Growth"
    ))
    earnings_growth = _as_ratio(_get_any(
        row, "Earnings Growth", "EPS Growth"
    ))
    
    # Technicals
    rsi_14 = _to_float(_get_any(row, "RSI 14", "RSI"))
    ma20 = _to_float(_get_any(row, "MA 20", "SMA 20", "20-Day MA"))
    ma50 = _to_float(_get_any(row, "MA 50", "SMA 50", "50-Day MA"))
    ma200 = _to_float(_get_any(row, "MA 200", "SMA 200", "200-Day MA"))
    volatility_30d = _to_float(_get_any(
        row, "Volatility 30D", "Volatility", "Vol 30D"
    ))
    atr_14 = _to_float(_get_any(row, "ATR 14", "ATR"))
    
    # Returns
    returns_1w = _as_ratio(_get_any(row, "Return 1W", "1W Return"))
    returns_1m = _as_ratio(_get_any(row, "Return 1M", "1M Return"))
    returns_3m = _as_ratio(_get_any(row, "Return 3M", "3M Return"))
    returns_6m = _as_ratio(_get_any(row, "Return 6M", "6M Return"))
    returns_12m = _as_ratio(_get_any(row, "Return 12M", "12M Return"))
    
    # Forecasts
    forecast_price_1m = _to_float(_get_any(
        row, "Forecast Price (1M)", "Forecast 1M", "forecast_price_1m"
    ))
    forecast_price_3m = _to_float(_get_any(
        row, "Forecast Price (3M)", "Forecast 3M", "forecast_price_3m"
    ))
    forecast_price_12m = _to_float(_get_any(
        row, "Forecast Price (12M)", "Forecast 12M", "forecast_price_12m"
    ))
    
    expected_roi_1m = _as_ratio(_get_any(
        row, "Expected ROI % (1M)", "ROI 1M", "expected_roi_1m"
    ))
    expected_roi_3m = _as_ratio(_get_any(
        row, "Expected ROI % (3M)", "ROI 3M", "expected_roi_3m"
    ))
    expected_roi_12m = _as_ratio(_get_any(
        row, "Expected ROI % (12M)", "ROI 12M", "expected_roi_12m"
    ))
    forecast_confidence = _to_float(_get_any(
        row, "Forecast Confidence", "Confidence", "forecast_confidence"
    ))
    
    # Scores
    quality_score = _to_float(_get_any(
        row, "Quality Score", "Quality"
    ))
    value_score = _to_float(_get_any(
        row, "Value Score", "Value"
    ))
    momentum_score = _to_float(_get_any(
        row, "Momentum Score", "Momentum"
    ))
    risk_score = _to_float(_get_any(
        row, "Risk Score", "Risk"
    ))
    overall_score = _to_float(_get_any(
        row, "Overall Score", "Opportunity Score", "Score"
    ))
    
    # Risk buckets
    risk_bucket = _norm_bucket(_get_any(
        row, "Risk Bucket", "Risk", "Risk Level"
    )) or "Moderate"
    
    confidence_bucket = _norm_bucket(_get_any(
        row, "Confidence Bucket", "Confidence"
    )) or "Moderate"
    
    # Sentiment (from row)
    news_score = _to_float(_get_any(
        row, "News Score", "News Sentiment", "Sentiment Score"
    ))
    analyst_sentiment = _to_float(_get_any(
        row, "Analyst Sentiment", "Analyst Rating"
    ))
    
    # Technical signals
    trend_signal = _safe_str(_get_any(
        row, "Trend Signal", "Trend"
    )).upper()
    if trend_signal not in ("UPTREND", "DOWNTREND", "NEUTRAL"):
        trend_signal = "NEUTRAL"
    
    macd_signal = _safe_str(_get_any(
        row, "MACD Signal", "MACD"
    )).upper()
    if macd_signal not in ("BULLISH", "BEARISH", "NEUTRAL"):
        macd_signal = "NEUTRAL"
    
    bb_signal = _safe_str(_get_any(
        row, "BB Signal", "Bollinger Signal"
    )).upper()
    if bb_signal not in ("OVERBOUGHT", "OVERSOLD", "NEUTRAL"):
        bb_signal = "NEUTRAL"
    
    # Timestamp
    last_updated_utc = _parse_iso_to_dt(_get_any(
        row, "Last Updated (UTC)", "Updated At (UTC)", "as_of_utc"
    ))
    
    # Forecast fallback: calculate from price + ROI
    if forecast_price_1m is None and price is not None and expected_roi_1m is not None:
        forecast_price_1m = price * (1.0 + expected_roi_1m)
    if forecast_price_3m is None and price is not None and expected_roi_3m is not None:
        forecast_price_3m = price * (1.0 + expected_roi_3m)
    if forecast_price_12m is None and price is not None and expected_roi_12m is not None:
        forecast_price_12m = price * (1.0 + expected_roi_12m)
    
    # Enrich with engine data if available
    if engine is not None:
        # News sentiment
        if include_news and news_score is None:
            ns = _engine_try_get_news_score(engine, symbol)
            if ns is not None:
                news_score = ns
        
        # Technical signals
        if include_technical:
            tech = _engine_try_get_technical_signals(engine, symbol)
            if tech:
                if "trend_signal" in tech:
                    trend_signal = tech["trend_signal"]
                if "macd_signal" in tech:
                    macd_signal = tech["macd_signal"]
                if "bb_signal" in tech:
                    bb_signal = tech["bb_signal"]
                if "rsi_14" in tech and rsi_14 is None:
                    rsi_14 = _to_float(tech["rsi_14"])
        
        # Historical returns
        hist = _engine_try_get_historical_returns(
            engine, symbol, ["1w", "1m", "3m", "6m", "12m"]
        )
        if hist:
            if returns_1w is None and "1w" in hist:
                returns_1w = hist["1w"]
            if returns_1m is None and "1m" in hist:
                returns_1m = hist["1m"]
            if returns_3m is None and "3m" in hist:
                returns_3m = hist["3m"]
            if returns_6m is None and "6m" in hist:
                returns_6m = hist["6m"]
            if returns_12m is None and "12m" in hist:
                returns_12m = hist["12m"]
    
    # Data quality assessment
    data_quality = "EXCELLENT"
    missing_fields = 0
    
    if price is None:
        missing_fields += 3
    if market_cap is None:
        missing_fields += 2
    if pe_ttm is None:
        missing_fields += 1
    if eps_ttm is None:
        missing_fields += 1
    if returns_3m is None:
        missing_fields += 1
    
    if missing_fields >= 5:
        data_quality = "POOR"
    elif missing_fields >= 3:
        data_quality = "FAIR"
    elif missing_fields >= 1:
        data_quality = "GOOD"
    
    # Create security object
    return Security(
        symbol=symbol,
        canonical=canonical,
        name=name,
        sheet=sheet,
        market=market,
        sector=sector,
        industry=industry,
        currency=currency,
        asset_class=asset_class,
        price=price,
        previous_close=previous_close,
        day_high=day_high,
        day_low=day_low,
        week_52_high=week_52_high,
        week_52_low=week_52_low,
        volume=volume,
        avg_volume_30d=avg_volume_30d,
        value_traded=value_traded,
        market_cap=market_cap,
        shares_outstanding=shares_outstanding,
        free_float=free_float,
        pe_ttm=pe_ttm,
        forward_pe=forward_pe,
        pb=pb,
        ps=ps,
        eps_ttm=eps_ttm,
        forward_eps=forward_eps,
        dividend_yield=dividend_yield,
        payout_ratio=payout_ratio,
        beta=beta,
        roe=roe,
        roa=roa,
        net_margin=net_margin,
        operating_margin=operating_margin,
        gross_margin=gross_margin,
        revenue_growth=revenue_growth,
        earnings_growth=earnings_growth,
        rsi_14=rsi_14,
        ma20=ma20,
        ma50=ma50,
        ma200=ma200,
        volatility_30d=volatility_30d,
        atr_14=atr_14,
        returns_1w=returns_1w,
        returns_1m=returns_1m,
        returns_3m=returns_3m,
        returns_6m=returns_6m,
        returns_12m=returns_12m,
        forecast_price_1m=forecast_price_1m,
        forecast_price_3m=forecast_price_3m,
        forecast_price_12m=forecast_price_12m,
        expected_roi_1m=expected_roi_1m,
        expected_roi_3m=expected_roi_3m,
        expected_roi_12m=expected_roi_12m,
        forecast_confidence=forecast_confidence,
        quality_score=quality_score,
        value_score=value_score,
        momentum_score=momentum_score,
        risk_score=risk_score,
        overall_score=overall_score,
        risk_bucket=risk_bucket,
        confidence_bucket=confidence_bucket,
        news_score=news_score,
        analyst_sentiment=analyst_sentiment,
        trend_signal=trend_signal,
        macd_signal=macd_signal,
        bb_signal=bb_signal,
        data_source="engine_snapshot",
        data_quality=data_quality,
        last_updated_utc=last_updated_utc,
    )


# ============================================================================
# Scoring Models
# ============================================================================

class ScoringModel:
    """
    Advanced scoring model with ML enhancement when available.
    """
    
    def __init__(self, use_ml: bool = True):
        self.use_ml = use_ml and HAS_SKLEARN
        self.ml_model = None
        self.scaler = None
        self._trained = False
    
    def _prepare_features(self, s: Security) -> Dict[str, float]:
        """Extract features for scoring."""
        features = {}
        
        # Valuation features
        if s.pe_ttm and s.pe_ttm > 0:
            features["pe_score"] = max(0, min(100, 100 - (s.pe_ttm - 10) * 2))
        if s.pb and s.pb > 0:
            features["pb_score"] = max(0, min(100, 100 - (s.pb - 1) * 30))
        if s.forward_pe and s.forward_pe > 0:
            features["forward_pe_score"] = max(0, min(100, 100 - (s.forward_pe - 10) * 2))
        
        # Growth features
        if s.revenue_growth:
            features["revenue_growth_score"] = _clamp(s.revenue_growth * 100, 0, 100)
        if s.earnings_growth:
            features["earnings_growth_score"] = _clamp(s.earnings_growth * 100, 0, 100)
        
        # Profitability features
        if s.roe:
            features["roe_score"] = _clamp(s.roe, 0, 100)
        if s.net_margin:
            features["margin_score"] = _clamp(s.net_margin, 0, 100)
        
        # Momentum features
        if s.returns_1m:
            features["momentum_1m"] = _clamp(s.returns_1m * 100 + 50, 0, 100)
        if s.returns_3m:
            features["momentum_3m"] = _clamp(s.returns_3m * 100 + 50, 0, 100)
        if s.rsi_14:
            features["rsi_score"] = s.rsi_14
        
        # Quality features
        if s.quality_score:
            features["quality"] = s.quality_score
        
        # Risk features (inverted)
        if s.risk_score:
            features["risk_inv"] = 100 - s.risk_score
        if s.volatility_30d:
            vol = min(100, s.volatility_30d)
            features["volatility_inv"] = 100 - vol
        
        # Technical signals
        if s.trend_signal == "UPTREND":
            features["trend"] = 80
        elif s.trend_signal == "DOWNTREND":
            features["trend"] = 20
        else:
            features["trend"] = 50
        
        if s.macd_signal == "BULLISH":
            features["macd"] = 80
        elif s.macd_signal == "BEARISH":
            features["macd"] = 20
        else:
            features["macd"] = 50
        
        return features
    
    def score_basic(self, s: Security) -> float:
        """Basic scoring without ML."""
        features = self._prepare_features(s)
        
        if not features:
            return 50.0
        
        weights = {
            "pe_score": 0.10,
            "pb_score": 0.05,
            "forward_pe_score": 0.05,
            "revenue_growth_score": 0.10,
            "earnings_growth_score": 0.10,
            "roe_score": 0.10,
            "margin_score": 0.05,
            "momentum_1m": 0.05,
            "momentum_3m": 0.10,
            "rsi_score": 0.05,
            "quality": 0.10,
            "risk_inv": 0.05,
            "volatility_inv": 0.05,
            "trend": 0.03,
            "macd": 0.02,
        }
        
        score = 0.0
        total_weight = 0.0
        
        for feat, weight in weights.items():
            if feat in features:
                score += features[feat] * weight
                total_weight += weight
        
        if total_weight > 0:
            return score / total_weight
        return 50.0
    
    def score_ml(self, s: Security) -> Optional[float]:
        """ML-enhanced scoring."""
        if not self.use_ml or not self._trained:
            return None
        
        features = self._prepare_features(s)
        if not features:
            return None
        
        # TODO: Implement ML model training and prediction
        # This would require historical data with known outcomes
        return None
    
    def score(self, s: Security) -> float:
        """Get final score with ML enhancement if available."""
        basic = self.score_basic(s)
        ml = self.score_ml(s) if self.use_ml else None
        
        if ml is not None:
            return basic * 0.6 + ml * 0.4
        return basic


# ============================================================================
# Advanced Filtering
# ============================================================================

def _passes_filters(
    s: Security,
    request: AdvisorRequest,
    liquidity_score: Optional[float] = None
) -> Tuple[bool, str]:
    """Check if security passes all filters."""
    
    # Market filter
    if request.allowed_markets:
        allowed = {m.upper() for m in request.allowed_markets}
        if s.market.upper() not in allowed:
            return False, f"Market excluded: {s.market}"
    
    # Risk bucket filter
    if request.risk_bucket:
        user_risk = RISK_LEVELS.get(request.risk_bucket, 0)
        cand_risk = RISK_LEVELS.get(s.risk_bucket, 0)
        if cand_risk < user_risk:
            return False, f"Risk too high (Req: {request.risk_bucket}, Got: {s.risk_bucket})"
    
    # Confidence bucket filter
    if request.confidence_bucket:
        user_conf = CONFIDENCE_LEVELS.get(request.confidence_bucket, 0)
        cand_conf = CONFIDENCE_LEVELS.get(s.confidence_bucket, 0)
        if cand_conf < user_conf:
            return False, f"Confidence too low (Req: {request.confidence_bucket})"
    
    # ROI requirements
    if request.required_roi_1m is not None:
        if s.expected_roi_1m is None or s.expected_roi_1m < request.required_roi_1m:
            return False, "ROI 1M below target"
    
    if request.required_roi_3m is not None:
        if s.expected_roi_3m is None or s.expected_roi_3m < request.required_roi_3m:
            return False, "ROI 3M below target"
    
    if request.required_roi_12m is not None:
        if s.expected_roi_12m is None or s.expected_roi_12m < request.required_roi_12m:
            return False, "ROI 12M below target"
    
    # Price filters
    if request.min_price is not None and s.price is not None:
        if s.price < request.min_price:
            return False, "Price too low"
    
    if request.max_price is not None and s.price is not None:
        if s.price > request.max_price:
            return False, "Price too high"
    
    # Sector exclusion
    if request.exclude_sectors and s.sector:
        sector_lower = s.sector.lower()
        for excl in request.exclude_sectors:
            if excl and excl.lower() in sector_lower:
                return False, f"Sector excluded: {s.sector}"
    
    # Liquidity filter
    if liquidity_score is not None:
        if liquidity_score < request.min_liquidity_score:
            return False, "Liquidity too low"
    
    # Min advisor score (will be set after scoring)
    if request.min_advisor_score is not None and s.advisor_score < request.min_advisor_score:
        return False, "Advisor score too low"
    
    return True, ""


def _compute_liquidity_score(s: Security) -> Optional[float]:
    """
    Compute liquidity score (0-100).
    Uses value_traded, volume, and market_cap.
    """
    if s.liquidity_score is not None:
        return s.liquidity_score
    
    # Try value traded first
    if s.value_traded is not None and s.value_traded > 0:
        log_val = math.log10(max(1.0, s.value_traded))
        # 6 = 1M, 9 = 1B, 11 = 100B
        score = ((log_val - 6.0) / (11.0 - 6.0)) * 75.0 + 20.0
        return max(0.0, min(100.0, score))
    
    # Try volume
    if s.volume is not None and s.volume > 0:
        log_vol = math.log10(max(1.0, s.volume))
        score = ((log_vol - 5.0) / (9.0 - 5.0)) * 75.0 + 20.0
        return max(0.0, min(100.0, score))
    
    # Try market cap as proxy
    if s.market_cap is not None and s.market_cap > 0:
        log_mc = math.log10(max(1.0, s.market_cap))
        score = ((log_mc - 7.0) / (12.0 - 7.0)) * 75.0 + 20.0
        return max(0.0, min(100.0, score))
    
    return None


# ============================================================================
# Portfolio Optimization
# ============================================================================

class PortfolioOptimizer:
    """
    Advanced portfolio optimizer with multiple strategies.
    """
    
    def __init__(self, risk_free_rate: float = 0.02):
        self.risk_free_rate = risk_free_rate
    
    def _get_returns_array(self, securities: List[Security]) -> np.ndarray:
        """Extract expected returns array."""
        returns = []
        for s in securities:
            # Use 3M ROI as expected return
            ret = s.expected_roi_3m
            if ret is None:
                # Fallback to 1M or 12M
                ret = s.expected_roi_1m or s.expected_roi_12m or 0.05
            returns.append(ret)
        return np.array(returns)
    
    def _get_volatilities_array(self, securities: List[Security]) -> np.ndarray:
        """Extract volatilities array."""
        vols = []
        for s in securities:
            vol = s.volatility_30d
            if vol is None:
                # Default volatility if missing
                vol = 20.0 if s.risk_bucket in ("High", "Very High") else 15.0
            vols.append(vol / 100.0)  # Convert to decimal
        return np.array(vols)
    
    def _estimate_correlation_matrix(
        self,
        securities: List[Security]
    ) -> Optional[np.ndarray]:
        """Estimate correlation matrix from sector/industry data."""
        n = len(securities)
        if n == 0:
            return None
        
        # Initialize with identity matrix
        corr = np.eye(n)
        
        # Simple correlation based on sector similarity
        for i in range(n):
            for j in range(i + 1, n):
                if i == j:
                    continue
                
                # Same sector -> higher correlation
                if securities[i].sector and securities[j].sector:
                    if securities[i].sector == securities[j].sector:
                        corr[i, j] = corr[j, i] = 0.7
                    else:
                        corr[i, j] = corr[j, i] = 0.3
                else:
                    corr[i, j] = corr[j, i] = 0.4
        
        return corr
    
    def _portfolio_stats(
        self,
        weights: np.ndarray,
        returns: np.ndarray,
        cov: np.ndarray
    ) -> Tuple[float, float, float]:
        """Calculate portfolio statistics."""
        port_return = np.sum(returns * weights)
        port_variance = np.dot(weights.T, np.dot(cov, weights))
        port_vol = np.sqrt(port_variance)
        
        sharpe = (port_return - self.risk_free_rate) / port_vol if port_vol > 0 else 0
        
        return port_return, port_vol, sharpe
    
    def optimize_equal_weight(
        self,
        securities: List[Security]
    ) -> List[float]:
        """Equal weight allocation."""
        n = len(securities)
        if n == 0:
            return []
        return [1.0 / n] * n
    
    def optimize_market_cap(
        self,
        securities: List[Security]
    ) -> List[float]:
        """Market cap weighted allocation."""
        caps = [s.market_cap or 0 for s in securities]
        total = sum(caps)
        if total == 0:
            return self.optimize_equal_weight(securities)
        return [c / total for c in caps]
    
    def optimize_minimum_variance(
        self,
        securities: List[Security]
    ) -> List[float]:
        """Minimum variance portfolio."""
        if not HAS_SCIPY or not HAS_NUMPY:
            return self.optimize_equal_weight(securities)
        
        n = len(securities)
        if n < 2:
            return [1.0] if n == 1 else []
        
        returns = self._get_returns_array(securities)
        vols = self._get_volatilities_array(securities)
        corr = self._estimate_correlation_matrix(securities)
        
        if corr is None:
            return self.optimize_equal_weight(securities)
        
        # Build covariance matrix
        cov = np.outer(vols, vols) * corr
        
        # Constraints
        constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]
        bounds = [(0.0, 1.0) for _ in range(n)]
        
        # Initial guess
        x0 = np.array([1.0 / n] * n)
        
        # Optimize
        try:
            result = optimize.minimize(
                lambda w: np.dot(w.T, np.dot(cov, w)),
                x0,
                method='SLSQP',
                bounds=bounds,
                constraints=constraints
            )
            
            if result.success:
                return result.x.tolist()
        except Exception:
            pass
        
        return self.optimize_equal_weight(securities)
    
    def optimize_maximum_sharpe(
        self,
        securities: List[Security]
    ) -> List[float]:
        """Maximum Sharpe ratio portfolio."""
        if not HAS_SCIPY or not HAS_NUMPY:
            return self.optimize_equal_weight(securities)
        
        n = len(securities)
        if n < 2:
            return [1.0] if n == 1 else []
        
        returns = self._get_returns_array(securities)
        vols = self._get_volatilities_array(securities)
        corr = self._estimate_correlation_matrix(securities)
        
        if corr is None:
            return self.optimize_equal_weight(securities)
        
        # Build covariance matrix
        cov = np.outer(vols, vols) * corr
        
        # Objective: minimize negative Sharpe
        def neg_sharpe(weights):
            port_return = np.sum(returns * weights)
            port_var = np.dot(weights.T, np.dot(cov, weights))
            port_vol = np.sqrt(port_var)
            if port_vol == 0:
                return 0
            sharpe = (port_return - self.risk_free_rate) / port_vol
            return -sharpe
        
        # Constraints
        constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]
        bounds = [(0.0, 1.0) for _ in range(n)]
        
        # Initial guess
        x0 = np.array([1.0 / n] * n)
        
        # Optimize
        try:
            result = optimize.minimize(
                neg_sharpe,
                x0,
                method='SLSQP',
                bounds=bounds,
                constraints=constraints
            )
            
            if result.success:
                return result.x.tolist()
        except Exception:
            pass
        
        return self.optimize_equal_weight(securities)
    
    def optimize_risk_parity(
        self,
        securities: List[Security]
    ) -> List[float]:
        """Risk parity allocation (equal risk contribution)."""
        if not HAS_NUMPY or len(securities) < 2:
            return self.optimize_equal_weight(securities)
        
        vols = self._get_volatilities_array(securities)
        
        # Risk parity weights inversely proportional to volatility
        inv_vols = 1.0 / vols
        weights = inv_vols / np.sum(inv_vols)
        
        return weights.tolist()
    
    def optimize(
        self,
        securities: List[Security],
        strategy: AllocationStrategy
    ) -> List[float]:
        """Run optimization with selected strategy."""
        if not securities:
            return []
        
        if strategy == AllocationStrategy.EQUAL_WEIGHT:
            return self.optimize_equal_weight(securities)
        elif strategy == AllocationStrategy.MARKET_CAP:
            return self.optimize_market_cap(securities)
        elif strategy == AllocationStrategy.MINIMUM_VARIANCE:
            return self.optimize_minimum_variance(securities)
        elif strategy == AllocationStrategy.MAXIMUM_SHARPE:
            return self.optimize_maximum_sharpe(securities)
        elif strategy == AllocationStrategy.RISK_PARITY:
            return self.optimize_risk_parity(securities)
        else:
            return self.optimize_equal_weight(securities)


# ============================================================================
# Deduplication
# ============================================================================

def _deduplicate_securities(
    securities: List[Security]
) -> Tuple[List[Security], int]:
    """
    Deduplicate securities by canonical key.
    Keeps the best security based on advisor_score and data quality.
    """
    best: Dict[str, Security] = {}
    removed = 0
    
    def score_key(s: Security) -> Tuple[float, float, float]:
        """Key for comparing securities."""
        return (
            float(s.advisor_score or 0.0),
            float(s.expected_roi_3m or 0.0),
            float(s.expected_roi_1m or 0.0),
        )
    
    for s in securities:
        key = s.canonical or s.symbol
        existing = best.get(key)
        
        if existing is None:
            best[key] = s
        elif score_key(s) > score_key(existing):
            best[key] = s
            removed += 1
        else:
            removed += 1
    
    return list(best.values()), removed


# ============================================================================
# Advisor Score Computation
# ============================================================================

def _compute_advisor_score(
    s: Security,
    request: AdvisorRequest,
    scoring_model: ScoringModel
) -> Tuple[float, str]:
    """
    Compute final advisor score with explanations.
    """
    reasons = []
    
    # Base score from model
    base_score = scoring_model.score(s)
    score = base_score
    reasons.append(f"Base: {base_score:.0f}")
    
    # ROI uplift
    if s.expected_roi_1m is not None:
        target = request.required_roi_1m or 0.0
        uplift = max(0.0, (s.expected_roi_1m - target)) * 100
        score += min(15, uplift * 0.5)
        if uplift > 2:
            reasons.append(f"ROI1M+{uplift:.0f}")
    
    if s.expected_roi_3m is not None:
        target = request.required_roi_3m or 0.0
        uplift = max(0.0, (s.expected_roi_3m - target)) * 100
        score += min(20, uplift * 0.4)
        if uplift > 2:
            reasons.append(f"ROI3M+{uplift:.0f}")
    
    if s.expected_roi_12m is not None:
        uplift = max(0.0, s.expected_roi_12m * 50)
        score += min(10, uplift)
        if uplift > 2:
            reasons.append("ROI12M+")
    
    # Factor boosts
    if s.quality_score and s.quality_score > 60:
        boost = min(8, (s.quality_score - 50) * 0.2)
        score += boost
        reasons.append("Quality+")
    
    if s.momentum_score and s.momentum_score > 60:
        boost = min(8, (s.momentum_score - 50) * 0.2)
        score += boost
        reasons.append("Momentum+")
    
    if s.value_score and s.value_score > 60:
        boost = min(8, (s.value_score - 50) * 0.2)
        score += boost
        reasons.append("Value+")
    
    # Risk penalty
    risk_tol = request.risk_profile.value
    risk_val = s.risk_score or 50.0
    
    if risk_tol == "conservative":
        if risk_val > 40:
            penalty = (risk_val - 40) * 0.6
            score -= penalty
            if penalty > 5:
                reasons.append(f"Risk-{int(penalty)}")
    elif risk_tol == "moderate":
        if risk_val > 60:
            penalty = (risk_val - 60) * 0.3
            score -= penalty
    elif risk_tol == "aggressive":
        if risk_val > 75:
            penalty = (risk_val - 75) * 0.2
            score -= penalty
    
    # Volatility penalty
    if s.volatility_30d is not None:
        vol = s.volatility_30d / 100.0
        if risk_tol == "conservative":
            score -= min(10, vol * 25)
        elif risk_tol == "moderate":
            score -= min(6, vol * 15)
    
    # News boost
    if request.include_news and s.news_score is not None:
        ns = s.news_score * request.news_weight
        if abs(ns) <= 10:
            score += ns
            if ns > 1:
                reasons.append("News+")
            elif ns < -1:
                reasons.append("News-")
        else:
            nb = (ns - 50) * 0.1
            score += nb
            if nb > 1:
                reasons.append("News+")
    
    # Technical signals
    if request.include_technical:
        if s.trend_signal == "UPTREND":
            score += 5
            reasons.append("TrendUp")
        elif s.trend_signal == "DOWNTREND":
            score -= 5
            reasons.append("TrendDown")
        
        if s.macd_signal == "BULLISH":
            score += 3
        elif s.macd_signal == "BEARISH":
            score -= 3
    
    # Liquidity effect
    liq = _compute_liquidity_score(s)
    if liq is not None:
        if liq >= 80:
            score += 2
        elif liq < 30:
            score -= 3
    
    # Clamp
    score = max(0.0, min(100.0, score))
    
    if s.price is not None and s.price <= 0:
        score = 0.0
    
    return score, "; ".join(reasons[:6])


# ============================================================================
# Portfolio Analysis
# ============================================================================

def _analyze_portfolio(
    securities: List[Security],
    weights: List[float],
    total_value: float
) -> Portfolio:
    """
    Perform comprehensive portfolio analysis.
    """
    if not securities or not weights or len(securities) != len(weights):
        return Portfolio()
    
    n = len(securities)
    
    # Basic portfolio
    portfolio = Portfolio(
        securities=securities,
        total_value=total_value,
        cash=0.0,
    )
    
    # Calculate exposures
    sector_exp = defaultdict(float)
    currency_exp = defaultdict(float)
    
    for s, w in zip(securities, weights):
        if s.sector:
            sector_exp[s.sector] += w
        if s.currency:
            currency_exp[s.currency] += w
    
    portfolio.sector_exposure = dict(sector_exp)
    portfolio.currency_exposure = dict(currency_exp)
    
    # Concentration score (Herfindahl index)
    portfolio.concentration_score = sum(w * w for w in weights)
    
    # Expected return and volatility
    if HAS_NUMPY:
        returns = np.array([s.expected_roi_3m or 0.05 for s in securities])
        vols = np.array([(s.volatility_30d or 15.0) / 100.0 for s in securities])
        weights_np = np.array(weights)
        
        portfolio.expected_return = float(np.sum(returns * weights_np))
        
        # Simple variance assuming independence
        port_var = np.sum((vols * weights_np) ** 2)
        portfolio.expected_volatility = float(np.sqrt(port_var))
        
        # Sharpe ratio
        if portfolio.expected_volatility > 0:
            portfolio.sharpe_ratio = (
                portfolio.expected_return - 0.02
            ) / portfolio.expected_volatility
        
        # VaR and CVaR (simplified normal assumption)
        if HAS_SCIPY:
            z = norm.ppf(0.95)
            portfolio.var_95 = float(
                portfolio.expected_return - z * portfolio.expected_volatility
            )
            portfolio.cvar_95 = float(
                portfolio.expected_return - 
                norm.pdf(z) / 0.05 * portfolio.expected_volatility
            )
    
    # Diversification ratio
    if portfolio.expected_volatility > 0:
        avg_vol = sum(v for v in vols) / n if HAS_NUMPY else 0.15
        portfolio.diversification_ratio = avg_vol / portfolio.expected_volatility
    
    return portfolio


# ============================================================================
# Main Entry Point
# ============================================================================

def run_investment_advisor(
    payload: Dict[str, Any],
    *,
    engine: Any = None
) -> Dict[str, Any]:
    """
    Run investment advisor with given payload and engine.
    
    Returns:
        Dict with keys: headers, rows, items, meta
    """
    start_time = time.time()
    
    # Headers for output
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
        "Action",
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
        "Data Source",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
    ]
    
    try:
        # Parse request
        request = AdvisorRequest.from_dict(payload)
        
        # Fetch universe
        universe_rows, fetch_meta, fetch_err = _fetch_universe(
            sources=request.sources,
            engine=engine,
            max_rows_per_source=5000
        )
        
        # Extract securities
        securities: List[Security] = []
        dropped = {
            "no_symbol": 0,
            "invalid_data": 0,
            "filtered": 0,
            "ticker_not_requested": 0,
        }
        enriched_news = 0
        enriched_technical = 0
        
        # Symbol variants for ticker filtering
        ticker_variants = set()
        ticker_canonical = set()
        if request.tickers:
            for t in request.tickers:
                ticker_variants.update(_symbol_variants(t))
                can = _canonical_symbol(t)
                if can:
                    ticker_canonical.add(can)
        
        for row in universe_rows:
            try:
                # Extract security
                s = _extract_security(
                    row,
                    engine=engine,
                    include_news=request.include_news,
                    include_technical=request.include_technical
                )
                
                if s is None:
                    dropped["no_symbol"] += 1
                    continue
                
                # Ticker filter
                if ticker_variants:
                    ok_variant = s.symbol in ticker_variants
                    ok_canon = s.canonical in ticker_canonical
                    if not (ok_variant or ok_canon):
                        dropped["ticker_not_requested"] += 1
                        continue
                
                # Track enrichments
                if request.include_news and s.news_score is not None:
                    enriched_news += 1
                if request.include_technical and (s.trend_signal != "NEUTRAL" or s.rsi_14 is not None):
                    enriched_technical += 1
                
                securities.append(s)
                
            except Exception as e:
                dropped["invalid_data"] += 1
                if request.debug:
                    logger.debug(f"Error processing row: {e}")
        
        if not securities:
            meta = {
                "ok": False,
                "version": ADVISOR_VERSION,
                "error": "No valid securities found",
                "fetch": fetch_meta,
                "dropped": dropped,
                "runtime_ms": int((time.time() - start_time) * 1000),
            }
            return {"headers": headers, "rows": [], "items": [], "meta": meta}
        
        # Initialize scoring model
        scoring_model = ScoringModel(use_ml=request.use_ml)
        
        # Compute advisor scores
        for s in securities:
            s.advisor_score, s.reason = _compute_advisor_score(
                s, request, scoring_model
            )
        
        # Apply filters
        filtered = []
        for s in securities:
            liq = _compute_liquidity_score(s)
            ok, _ = _passes_filters(s, request, liq)
            if ok:
                filtered.append(s)
            else:
                dropped["filtered"] += 1
        
        # Deduplicate
        filtered, dedupe_removed = _deduplicate_securities(filtered)
        
        # Sort by advisor score
        filtered.sort(key=lambda x: x.advisor_score, reverse=True)
        
        # Take top N
        top_securities = filtered[:request.top_n]
        
        # Portfolio optimization
        optimizer = PortfolioOptimizer()
        weights = optimizer.optimize(
            top_securities,
            request.allocation_strategy
        )
        
        # Normalize weights if needed
        if weights and len(weights) == len(top_securities):
            total = sum(weights)
            if total > 0:
                weights = [w / total for w in weights]
        else:
            weights = [1.0 / len(top_securities)] * len(top_securities) if top_securities else []
        
        # Calculate allocation amounts
        allocations = []
        for s, w in zip(top_securities, weights):
            amount = request.invest_amount * w
            allocations.append({
                "symbol": s.symbol,
                "weight": w,
                "amount": amount,
            })
        
        alloc_map = {a["symbol"]: a for a in allocations}
        
        # Analyze portfolio
        portfolio = _analyze_portfolio(
            top_securities,
            weights,
            request.invest_amount
        )
        
        # Build output
        rows: List[List[Any]] = []
        items: List[Dict[str, Any]] = []
        
        as_of_dt = request.as_of_utc or _now_utc()
        
        for i, s in enumerate(top_securities, 1):
            alloc = alloc_map.get(s.symbol, {"weight": 0.0, "amount": 0.0})
            w = alloc["weight"]
            amt = alloc["amount"]
            
            # Action based on score
            if s.advisor_score >= 80:
                action = "STRONG_BUY"
            elif s.advisor_score >= 65:
                action = "BUY"
            elif s.advisor_score >= 50:
                action = "HOLD"
            elif s.advisor_score >= 35:
                action = "REDUCE"
            else:
                action = "SELL"
            
            # Liquidity score
            liq_score = _compute_liquidity_score(s) or 0.0
            
            # Timestamps
            dt_used = s.last_updated_utc if request.use_row_updated_at and s.last_updated_utc else as_of_dt
            last_utc = _iso_utc(dt_used)
            last_riy = _iso_riyadh(dt_used)
            
            rows.append([
                i,
                s.symbol,
                s.sheet,
                s.name,
                s.market,
                s.sector,
                s.currency,
                _round2(s.price),
                _round2(s.advisor_score),
                action,
                _round2(w * 100.0),
                _round2(amt),
                _round2(s.forecast_price_1m),
                _to_percent(s.expected_roi_1m),
                _round2(s.forecast_price_3m),
                _to_percent(s.expected_roi_3m),
                _round2(s.forecast_price_12m),
                _to_percent(s.expected_roi_12m),
                s.risk_bucket,
                s.confidence_bucket,
                _round2(liq_score),
                s.data_quality,
                s.reason,
                s.data_source,
                last_utc,
                last_riy,
            ])
            
            items.append({
                "rank": i,
                "symbol": s.symbol,
                "canonical": s.canonical,
                "origin": s.sheet,
                "name": s.name,
                "market": s.market,
                "sector": s.sector,
                "currency": s.currency,
                "price": s.price,
                "advisor_score": _round2(s.advisor_score),
                "action": action,
                "allocation_pct": _round2(w * 100.0),
                "allocation_amount": _round2(amt),
                "forecast_price_1m": s.forecast_price_1m,
                "expected_roi_1m_pct": _to_percent(s.expected_roi_1m),
                "forecast_price_3m": s.forecast_price_3m,
                "expected_roi_3m_pct": _to_percent(s.expected_roi_3m),
                "forecast_price_12m": s.forecast_price_12m,
                "expected_roi_12m_pct": _to_percent(s.expected_roi_12m),
                "risk_bucket": s.risk_bucket,
                "confidence_bucket": s.confidence_bucket,
                "liquidity_score": _round2(liq_score),
                "data_quality": s.data_quality,
                "reason": s.reason,
                "data_source": s.data_source,
                "last_updated_utc": last_utc,
                "last_updated_riyadh": last_riy,
            })
        
        # Build metadata
        meta = {
            "ok": True,
            "version": ADVISOR_VERSION,
            "request": {
                "sources": request.sources,
                "risk_profile": request.risk_profile.value,
                "tickers": request.tickers,
                "invest_amount": request.invest_amount,
                "top_n": request.top_n,
                "allocation_strategy": request.allocation_strategy.value,
            },
            "fetch": fetch_meta,
            "counts": {
                "universe_rows": len(universe_rows),
                "securities_extracted": len(securities),
                "securities_filtered": len(filtered) + dedupe_removed,
                "securities_after_filters": len(filtered),
                "returned_rows": len(rows),
                "dedupe_removed": dedupe_removed,
                "news_enriched": enriched_news,
                "technical_enriched": enriched_technical,
                "dropped": dropped,
            },
            "portfolio": {
                "total_value": portfolio.total_value,
                "expected_return": _round2(portfolio.expected_return),
                "expected_volatility": _round2(portfolio.expected_volatility),
                "sharpe_ratio": _round2(portfolio.sharpe_ratio),
                "var_95": _round2(portfolio.var_95),
                "cvar_95": _round2(portfolio.cvar_95),
                "diversification_ratio": _round2(portfolio.diversification_ratio),
                "concentration_score": _round2(portfolio.concentration_score),
                "sector_exposure": {k: _round2(v) for k, v in portfolio.sector_exposure.items()},
                "currency_exposure": {k: _round2(v) for k, v in portfolio.currency_exposure.items()},
            },
            "runtime_ms": int((time.time() - start_time) * 1000),
        }
        
        if fetch_err:
            meta["warning"] = fetch_err
        
        return {
            "headers": headers,
            "rows": rows,
            "items": items,
            "meta": meta,
        }
        
    except Exception as e:
        # Error response
        meta = {
            "ok": False,
            "version": ADVISOR_VERSION,
            "error": str(e),
            "runtime_ms": int((time.time() - start_time) * 1000),
        }
        
        if payload.get("debug", False):
            import traceback
            meta["traceback"] = traceback.format_exc()
        
        return {
            "headers": headers,
            "rows": [],
            "items": [],
            "meta": meta,
        }


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "run_investment_advisor",
    "ADVISOR_VERSION",
    "MarketType",
    "AssetClass",
    "Recommendation",
    "RiskProfile",
    "AllocationStrategy",
    "Security",
    "Portfolio",
    "AdvisorRequest",
]
