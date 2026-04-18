#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 — GLOBAL-FIRST ORCHESTRATOR — v5.51.1
================================================================================

This revision is a clean, import-safe replacement focused on route compatibility,
schema-first sheet rows, and stable exports.

Key improvements versus the uploaded script:
- fixes the scoring fallback recursion bug
- restores the missing module-level compatibility exports referenced in __all__
- preserves page-aware provider routing (EODHD-first for configured non-KSA pages)
- preserves schema-first envelopes for sheet rows and special pages
- keeps import-time behavior lightweight and failure-tolerant
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import re
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from pydantic import BaseModel, ConfigDict
except Exception:  # pragma: no cover
    class BaseModel:  # type: ignore
        def __init__(self, **data: Any) -> None:
            self.__dict__.update(data)

        def model_dump(self, mode: str = "python") -> Dict[str, Any]:
            return dict(self.__dict__)

        def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            return dict(self.__dict__)

    def ConfigDict(**kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return dict(kwargs)


ROOT_DIR = Path(__file__).resolve().parents[0]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__version__ = "5.51.1"
VERSION = __version__

logger = logging.getLogger("core.data_engine_v2")
logger.addHandler(logging.NullHandler())


# =============================================================================
# Compatibility / diagnostics placeholders
# =============================================================================
class _NoopMetrics:
    def set(self, *args: Any, **kwargs: Any) -> None:
        return

    def inc(self, *args: Any, **kwargs: Any) -> None:
        return

    def observe(self, *args: Any, **kwargs: Any) -> None:
        return


_METRICS = _NoopMetrics()
_PERF_METRICS: List[Dict[str, Any]] = []


def get_perf_metrics() -> List[Dict[str, Any]]:
    return list(_PERF_METRICS)


def get_perf_stats() -> Dict[str, Any]:
    if not _PERF_METRICS:
        return {"count": 0}
    durs = [float(x.get("duration_ms", 0.0)) for x in _PERF_METRICS]
    return {
        "count": len(durs),
        "avg_duration_ms": round(sum(durs) / len(durs), 3),
        "max_duration_ms": round(max(durs), 3),
    }


def reset_perf_metrics() -> None:
    _PERF_METRICS.clear()


class MetricsRegistry:
    pass


class DistributedCache:
    pass


class DynamicCircuitBreaker:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


class TokenBucket:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


# =============================================================================
# Minimal domain models
# =============================================================================
class QuoteQuality(str, Enum):
    GOOD = "good"
    FAIR = "fair"
    MISSING = "missing"


class DataSource(str, Enum):
    ENGINE_V2 = "engine_v2"
    EXTERNAL_ROWS = "external_rows"
    SNAPSHOT = "snapshot"
    FALLBACK = "fallback"


class UnifiedQuote(BaseModel):
    model_config = ConfigDict(extra="allow")


class StubUnifiedQuote(BaseModel):
    model_config = ConfigDict(extra="allow")

    def finalize(self) -> "StubUnifiedQuote":
        d = _model_to_dict(self)
        if d.get("current_price") is None and d.get("price") is not None:
            d["current_price"] = d.get("price")
        if d.get("price") is None and d.get("current_price") is not None:
            d["price"] = d.get("current_price")
        if d.get("price_change") is None and d.get("change") is not None:
            d["price_change"] = d.get("change")
        if d.get("change") is None and d.get("price_change") is not None:
            d["change"] = d.get("price_change")
        if d.get("percent_change") is None and d.get("change_pct") is not None:
            d["percent_change"] = d.get("change_pct")
        if d.get("change_pct") is None and d.get("percent_change") is not None:
            d["change_pct"] = d.get("percent_change")
        if d.get("percent_change") is None and d.get("current_price") is not None and d.get("previous_close") not in (None, 0):
            try:
                d["percent_change"] = float(d["current_price"]) / float(d["previous_close"]) - 1.0
                d["change_pct"] = d["percent_change"]
            except Exception:
                pass
        d.setdefault("last_updated_utc", _now_utc_iso())
        d.setdefault("last_updated_riyadh", _now_riyadh_iso())
        d.setdefault("data_quality", QuoteQuality.MISSING.value)
        return StubUnifiedQuote(**d)


@dataclass
class SymbolInfo:
    requested: str
    normalized: str
    is_ksa: bool


@dataclass
class BatchProgress:
    total: int
    completed: int = 0
    succeeded: int = 0
    failed: int = 0
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    errors: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def completion_pct(self) -> float:
        return round((self.completed / self.total) * 100.0, 2) if self.total else 0.0


@dataclass
class PerfMetrics:
    name: str
    duration_ms: float
    success: bool
    meta: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Canonical contracts
# =============================================================================
INSTRUMENT_CANONICAL_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    "price_change_5d",
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "volume_ratio",
    "beta_5y", "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio",
    "revenue_ttm", "revenue_growth_yoy", "gross_margin", "operating_margin",
    "profit_margin", "debt_to_equity", "free_cash_flow_ttm",
    "roe", "roa",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    "rsi_signal", "technical_score", "day_range_position", "atr_14",
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
    "intrinsic_value", "valuation_score", "upside_pct",
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score",
    "analyst_rating", "target_price", "upside_downside_pct",
    "recommendation", "signal", "trend_1m", "trend_3m", "trend_12m",
    "short_term_signal", "recommendation_reason", "invest_period_label", "horizon_days",
    "rank_overall",
    "position_qty", "avg_cost", "position_cost", "position_value", "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
]

INSTRUMENT_CANONICAL_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Change %", "52W Position %",
    "5D Change %",
    "Volume", "Avg Vol 10D", "Avg Vol 30D", "Market Cap", "Float Shares", "Volume Ratio",
    "Beta (5Y)", "P/E (TTM)", "P/E (Fwd)", "EPS (TTM)", "Div Yield %", "Payout Ratio %",
    "Revenue TTM", "Rev Growth YoY %", "Gross Margin %", "Op Margin %",
    "Net Margin %", "D/E Ratio", "FCF (TTM)",
    "ROE %", "ROA %",
    "RSI (14)", "Volatility 30D %", "Volatility 90D %", "Max DD 1Y %",
    "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    "RSI Signal", "Tech Score", "Day Range Pos %", "ATR 14",
    "P/B", "P/S", "EV/EBITDA", "PEG Ratio",
    "Intrinsic Value", "Valuation Score", "Upside %",
    "Price Tgt 1M", "Price Tgt 3M", "Price Tgt 12M",
    "ROI 1M %", "ROI 3M %", "ROI 12M %",
    "AI Confidence", "Confidence Score", "Confidence",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score",
    "Analyst Rating", "Target Price", "Upside/Downside %",
    "Recommendation", "Signal", "Trend 1M", "Trend 3M", "Trend 12M",
    "ST Signal", "Reason", "Horizon", "Horizon Days",
    "Rank (Overall)",
    "Qty", "Avg Cost", "Position Cost", "Position Value", "Unrealized P/L", "Unrealized P/L %",
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
]

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = ("top10_rank", "selection_reason", "criteria_snapshot")
TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top 10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

INSIGHTS_KEYS: List[str] = ["section", "item", "symbol", "metric", "value", "signal", "priority", "notes", "as_of_riyadh"]
INSIGHTS_HEADERS: List[str] = ["Section", "Item", "Symbol", "Metric", "Value", "Signal", "Priority", "Notes", "Last Updated (Riyadh)"]

DATA_DICTIONARY_KEYS: List[str] = ["sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes"]
DATA_DICTIONARY_HEADERS: List[str] = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]

_GM_EXTRA_KEYS = [
    "region", "market_status", "price_usd",
    "sector_pe_avg", "vs_sector_pe_pct", "sector_ytd_pct",
    "sector_signal", "sector_rank", "sector_vs_msci_pct",
    "vs_sp500_ytd", "vs_msci_world_ytd", "wall_st_target",
    "upside_to_target_pct", "analyst_consensus", "country_risk",
    "ma_50d", "ma_200d", "ema_signal", "macd_signal",
]
_GM_EXTRA_HEADERS = [
    "Region", "Market Status", "Price (USD)",
    "Sector P/E Avg", "vs Sector P/E %", "Sector YTD %",
    "Sector Signal", "Sector Rank", "Sector vs MSCI %",
    "vs S&P 500 YTD %", "vs MSCI World %", "Wall St Target",
    "Upside to Tgt %", "Analyst Consensus", "Country Risk",
    "50D MA", "200D MA", "EMA Signal", "MACD Signal",
]

_CF_REMOVE_KEYS: Set[str] = {
    "pe_ttm", "pe_forward", "eps_ttm", "peg_ratio", "revenue_ttm", "payout_ratio",
    "gross_margin", "operating_margin", "profit_margin", "debt_to_equity", "free_cash_flow_ttm", "roe", "roa",
}
_CF_ADD_KEYS = ["commodity_type", "contract_expiry", "spot_price", "usd_correlation", "seasonal_signal", "carry_rate"]
_CF_ADD_HEADERS = ["Asset Type", "Contract Expiry", "Spot Price", "USD Correlation", "Seasonal Signal", "Carry Rate %"]

_MF_REMOVE_KEYS: Set[str] = {
    "pe_ttm", "pe_forward", "eps_ttm", "peg_ratio", "revenue_ttm", "revenue_growth_yoy",
    "gross_margin", "operating_margin", "profit_margin", "debt_to_equity", "free_cash_flow_ttm", "roe", "roa",
}
_MF_ADD_KEYS = [
    "fund_type", "benchmark_name", "holdings_count", "aum", "expense_ratio", "nav",
    "nav_premium_pct", "distribution_yield", "ytd_return", "return_1y", "return_3y_ann", "return_5y_ann",
    "tracking_error", "alpha_1y",
]
_MF_ADD_HEADERS = [
    "Fund Type", "Benchmark", "Holdings Count", "AUM (B USD)", "Expense Ratio %", "NAV",
    "NAV Prem/Disc %", "Distribution Yield %", "YTD Return %", "1Y Return %", "3Y Return (Ann) %",
    "5Y Return (Ann) %", "Tracking Error %", "Alpha (1Y)",
]

_MP_ADD_KEYS = [
    "portfolio_weight_pct", "target_weight_pct", "weight_deviation", "rebalance_signal",
    "stop_loss", "take_profit", "distance_to_sl_pct", "distance_to_tp_pct",
    "days_held", "annual_dividend_income", "beta_contribution",
]
_MP_ADD_HEADERS = [
    "Weight %", "Target Weight %", "Weight Deviation %", "Rebal Signal", "Stop Loss", "Take Profit",
    "Dist to SL %", "Dist to TP %", "Days Held", "Ann Div Income", "Beta Contribution",
]

_T10_EXTRA_KEYS = [
    "top10_rank", "selection_reason", "criteria_snapshot", "entry_price",
    "stop_loss_suggested", "take_profit_suggested", "risk_reward_ratio",
]
_T10_EXTRA_HEADERS = [
    "Top 10 Rank", "Selection Reason", "Criteria Snapshot", "Entry Price",
    "Stop Loss (AI)", "Take Profit (AI)", "Risk/Reward",
]


def _build_page_keys(base_keys: List[str], base_headers: List[str], remove: Set[str], add_keys: List[str], add_headers: List[str]) -> Tuple[List[str], List[str]]:
    keys: List[str] = []
    headers: List[str] = []
    for k, h in zip(base_keys, base_headers):
        if k not in remove:
            keys.append(k)
            headers.append(h)
    keys.extend(add_keys)
    headers.extend(add_headers)
    return keys, headers


_GM_KEYS, _GM_HEADERS = _build_page_keys(INSTRUMENT_CANONICAL_KEYS, INSTRUMENT_CANONICAL_HEADERS, set(), _GM_EXTRA_KEYS, _GM_EXTRA_HEADERS)
_CF_KEYS, _CF_HEADERS = _build_page_keys(INSTRUMENT_CANONICAL_KEYS, INSTRUMENT_CANONICAL_HEADERS, _CF_REMOVE_KEYS, _CF_ADD_KEYS, _CF_ADD_HEADERS)
_MF_KEYS, _MF_HEADERS = _build_page_keys(INSTRUMENT_CANONICAL_KEYS, INSTRUMENT_CANONICAL_HEADERS, _MF_REMOVE_KEYS, _MF_ADD_KEYS, _MF_ADD_HEADERS)
_MP_KEYS, _MP_HEADERS = _build_page_keys(INSTRUMENT_CANONICAL_KEYS, INSTRUMENT_CANONICAL_HEADERS, set(), _MP_ADD_KEYS, _MP_ADD_HEADERS)
_T10_KEYS = list(INSTRUMENT_CANONICAL_KEYS) + _T10_EXTRA_KEYS
_T10_HEADERS = list(INSTRUMENT_CANONICAL_HEADERS) + _T10_EXTRA_HEADERS

STATIC_CANONICAL_SHEET_CONTRACTS: Dict[str, Dict[str, List[str]]] = {
    "Market_Leaders": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Global_Markets": {"headers": _GM_HEADERS, "keys": _GM_KEYS},
    "Commodities_FX": {"headers": _CF_HEADERS, "keys": _CF_KEYS},
    "Mutual_Funds": {"headers": _MF_HEADERS, "keys": _MF_KEYS},
    "My_Portfolio": {"headers": _MP_HEADERS, "keys": _MP_KEYS},
    "My_Investments": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Top_10_Investments": {"headers": _T10_HEADERS, "keys": _T10_KEYS},
    "Insights_Analysis": {"headers": INSIGHTS_HEADERS, "keys": INSIGHTS_KEYS},
    "Data_Dictionary": {"headers": DATA_DICTIONARY_HEADERS, "keys": DATA_DICTIONARY_KEYS},
}

INSTRUMENT_SHEETS: Set[str] = {
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio", "My_Investments", "Top_10_Investments",
}
SPECIAL_SHEETS: Set[str] = {"Insights_Analysis", "Data_Dictionary"}
TOP10_ENGINE_DEFAULT_PAGES: List[str] = ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio", "My_Investments"]

EMERGENCY_PAGE_SYMBOLS: Dict[str, List[str]] = {
    "Market_Leaders": ["2222.SR", "1120.SR", "2010.SR", "7010.SR", "AAPL", "MSFT", "NVDA", "GOOGL"],
    "Global_Markets": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO"],
    "Commodities_FX": ["GC=F", "BZ=F", "SI=F", "EURUSD=X", "GBPUSD=X", "JPY=X", "SAR=X", "CL=F"],
    "Mutual_Funds": ["SPY", "QQQ", "VTI", "VOO", "IWM"],
    "My_Portfolio": ["2222.SR", "AAPL", "MSFT"],
    "My_Investments": ["2222.SR", "AAPL", "MSFT"],
    "Top_10_Investments": ["2222.SR", "1120.SR", "AAPL", "MSFT", "NVDA"],
}

PAGE_SYMBOL_ENV_KEYS: Dict[str, str] = {
    "Market_Leaders": "MARKET_LEADERS_SYMBOLS",
    "Global_Markets": "GLOBAL_MARKETS_SYMBOLS",
    "Commodities_FX": "COMMODITIES_FX_SYMBOLS",
    "Mutual_Funds": "MUTUAL_FUNDS_SYMBOLS",
    "My_Portfolio": "MY_PORTFOLIO_SYMBOLS",
    "My_Investments": "MY_INVESTMENTS_SYMBOLS",
    "Top_10_Investments": "TOP10_FALLBACK_SYMBOLS",
}

DEFAULT_PROVIDERS = ["eodhd", "yahoo", "finnhub"]
DEFAULT_KSA_PROVIDERS = ["tadawul", "argaam", "yahoo"]
DEFAULT_GLOBAL_PROVIDERS = ["eodhd", "yahoo", "finnhub"]
NON_KSA_EODHD_PRIMARY_PAGES = {"Global_Markets", "Commodities_FX", "Mutual_Funds"}
PROVIDER_PRIORITIES = {"tadawul": 10, "argaam": 20, "eodhd": 30, "yahoo": 40, "finnhub": 50, "yahoo_chart": 60}


# =============================================================================
# Generic helpers
# =============================================================================
def _safe_str(x: Any, default: str = "") -> str:
    if x is None:
        return default
    try:
        s = str(x).strip()
        return s if s else default
    except Exception:
        return default


def _norm_key(x: Any) -> str:
    s = _safe_str(x).lower()
    if not s:
        return ""
    s = s.replace("-", "_").replace("/", "_").replace("&", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"__+", "_", s).strip("_")
    return s


def _norm_key_loose(x: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _safe_str(x).lower())


def _safe_bool(x: Any, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    s = _safe_str(x).lower()
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


def _safe_int(x: Any, default: int = 0, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        v = int(float(x))
    except Exception:
        v = int(default)
    if lo is not None:
        v = max(lo, v)
    if hi is not None:
        v = min(hi, v)
    return v


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _as_pct_points(x: Any) -> Optional[float]:
    v = _as_float(x)
    if v is None:
        return None
    return v * 100.0 if abs(v) <= 1.5 else v


def _dedupe_keep_order(items: Sequence[Any]) -> List[Any]:
    out: List[Any] = []
    seen: Set[Any] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _complete_schema_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    raw_headers = list(headers or [])
    raw_keys = list(keys or [])
    max_len = max(len(raw_headers), len(raw_keys))
    hdrs: List[str] = []
    ks: List[str] = []
    for i in range(max_len):
        h = _safe_str(raw_headers[i]) if i < len(raw_headers) else ""
        k = _safe_str(raw_keys[i]) if i < len(raw_keys) else ""
        if not h and not k:
            continue
        if h and not k:
            k = _norm_key(h)
        elif k and not h:
            h = k.replace("_", " ").title()
        if h and k:
            hdrs.append(h)
            ks.append(k)
    return hdrs, ks


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    for field in TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(TOP10_REQUIRED_HEADERS[field])
    return _complete_schema_contract(hdrs, ks)


def _usable_contract(headers: Sequence[str], keys: Sequence[str], sheet_name: str = "") -> bool:
    if not headers or not keys or len(headers) != len(keys):
        return False
    canon = _canonicalize_sheet_name(sheet_name)
    keyset = set(keys)
    if canon in INSTRUMENT_SHEETS - {"Top_10_Investments"}:
        return bool({"symbol", "ticker", "requested_symbol"} & keyset) and bool({"current_price", "price", "name"} & keyset)
    if canon == "Top_10_Investments":
        return bool({"symbol", "ticker", "requested_symbol"} & keyset) and set(TOP10_REQUIRED_FIELDS).issubset(keyset)
    if canon == "Insights_Analysis":
        return {"section", "item", "metric", "value"}.issubset(keyset)
    if canon == "Data_Dictionary":
        return {"sheet", "header", "key"}.issubset(keyset)
    return True


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, Decimal):
        return _as_float(value)
    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if is_dataclass(value):
        try:
            return {str(k): _json_safe(v) for k, v in asdict(value).items()}
        except Exception:
            return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return _json_safe(value.dict())
    except Exception:
        pass
    try:
        return str(value)
    except Exception:
        return None


def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, Mapping):
        try:
            return dict(obj)
        except Exception:
            return {}
    try:
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            d = obj.model_dump(mode="python")
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            d = obj.dict()
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    try:
        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass
    return {"result": obj}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo("Asia/Riyadh")).isoformat()
    except Exception:
        pass
    return _now_utc_iso()


def _safe_env(name: str, default: str = "") -> str:
    return _safe_str(os.getenv(name), default)


def _get_env_bool(name: str, default: bool = False) -> bool:
    return _safe_bool(os.getenv(name), default)


def _get_env_int(name: str, default: int) -> int:
    return _safe_int(os.getenv(name), default)


def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)


def _get_env_list(name: str, default: Sequence[str]) -> List[str]:
    raw = _safe_env(name, "")
    if not raw:
        return [str(x).lower() for x in default]
    return [p.strip().lower() for p in re.split(r"[,;|\s]+", raw) if p.strip()]


def _page_catalog_candidates() -> List[Any]:
    modules: List[Any] = []
    for mod_path in ("core.sheets.page_catalog", "sheets.page_catalog"):
        try:
            modules.append(import_module(mod_path))
        except Exception:
            continue
    return modules


def _page_catalog_canonical_name(name: str) -> str:
    raw = _safe_str(name)
    if not raw:
        return ""
    for mod in _page_catalog_candidates():
        for fn_name in ("canonicalize_page_name", "normalize_page_name", "get_canonical_page_name", "canonical_page_name"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                for args, kwargs in (((raw,), {}), ((), {"page": raw}), ((), {"name": raw}), ((), {"sheet": raw})):
                    try:
                        val = fn(*args, **kwargs)
                    except TypeError:
                        continue
                    except Exception:
                        continue
                    txt = _safe_str(val)
                    if txt:
                        return txt
        for attr_name in ("PAGE_ALIASES", "SHEET_ALIASES", "ALIASES", "PAGE_NAME_ALIASES"):
            mapping = getattr(mod, attr_name, None)
            if isinstance(mapping, dict):
                for cand in (raw, raw.replace(" ", "_"), raw.replace("-", "_"), _norm_key(raw), _norm_key_loose(raw)):
                    for key, val in mapping.items():
                        if cand in {_safe_str(key), _norm_key(_safe_str(key)), _norm_key_loose(_safe_str(key))}:
                            txt = _safe_str(val)
                            if txt:
                                return txt
    return ""


def _canonicalize_sheet_name(name: str) -> str:
    raw = _safe_str(name)
    if not raw:
        return ""
    candidates = [raw, raw.replace(" ", "_"), raw.replace("-", "_"), _norm_key(raw)]
    known = {k: k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_norm = {_norm_key(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_loose = {_norm_key_loose(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    for cand in candidates:
        if cand in known:
            return known[cand]
        nk = _norm_key(cand)
        if nk in by_norm:
            return by_norm[nk]
        nkl = _norm_key_loose(cand)
        if nkl in by_loose:
            return by_loose[nkl]
    page_catalog_name = _page_catalog_canonical_name(raw)
    if page_catalog_name:
        pc_candidates = [page_catalog_name, page_catalog_name.replace(" ", "_"), _norm_key(page_catalog_name), _norm_key_loose(page_catalog_name)]
        for cand in pc_candidates:
            if cand in known:
                return known[cand]
            if _norm_key(cand) in by_norm:
                return by_norm[_norm_key(cand)]
            if _norm_key_loose(cand) in by_loose:
                return by_loose[_norm_key_loose(cand)]
        return page_catalog_name.replace(" ", "_")
    return raw.replace(" ", "_")


def _sheet_lookup_candidates(sheet: str) -> List[str]:
    s = _canonicalize_sheet_name(sheet)
    vals = [s, s.replace("_", " "), s.lower(), _norm_key(s), _norm_key_loose(s)]
    return [v for v in _dedupe_keep_order(vals) if _safe_str(v)]


def _looks_like_symbol_token(x: Any) -> bool:
    s = _safe_str(x)
    if not s or len(s) > 24:
        return False
    return bool(re.match(r"^[A-Z0-9.=\-:^/]{1,24}$", s) or re.match(r"^[0-9]{4}(\.SR)?$", s))


def normalize_symbol(symbol: str) -> str:
    return _safe_str(symbol).upper()


def get_symbol_info(symbol: str) -> Dict[str, Any]:
    s = normalize_symbol(symbol)
    return {"requested": _safe_str(symbol), "normalized": s, "is_ksa": s.endswith(".SR") or re.match(r"^[0-9]{4}$", s) is not None}


def _split_symbols(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for v in value:
            out.extend(_split_symbols(v))
        return out
    s = _safe_str(value)
    if not s:
        return []
    return [p.strip() for p in re.split(r"[,;|\s]+", s) if p.strip()]


def _normalize_symbol_list(symbols: Iterable[Any], limit: int = 5000) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in symbols:
        s = normalize_symbol(_safe_str(item))
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= limit:
            break
    return out


def _extract_requested_symbols_from_body(body: Optional[Dict[str, Any]], limit: int = 5000) -> List[str]:
    if not isinstance(body, dict):
        return []
    raw: List[str] = []
    for key in (
        "symbols", "tickers", "selected_symbols", "direct_symbols", "codes", "watchlist", "portfolio_symbols", "symbol", "ticker", "code", "requested_symbol",
    ):
        raw.extend(_split_symbols(body.get(key)))
    criteria = body.get("criteria")
    if isinstance(criteria, dict):
        for key in ("symbols", "tickers", "selected_symbols", "direct_symbols", "codes", "symbol", "ticker", "code"):
            raw.extend(_split_symbols(criteria.get(key)))
    return _normalize_symbol_list(raw, limit=limit)


def _merge_route_body_dicts(*parts: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for part in parts:
        if part is None:
            continue
        if isinstance(part, Mapping):
            for k, v in part.items():
                key = _safe_str(k)
                if key:
                    merged[key] = v
            continue
        try:
            if hasattr(part, "multi_items") and callable(getattr(part, "multi_items")):
                for k, v in part.multi_items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
                continue
        except Exception:
            pass
        try:
            if hasattr(part, "items") and callable(getattr(part, "items")):
                for k, v in part.items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
                continue
        except Exception:
            pass
        try:
            d = _model_to_dict(part)
            if isinstance(d, dict):
                for k, v in d.items():
                    key = _safe_str(k)
                    if key:
                        merged[key] = v
        except Exception:
            continue
    return merged


def _extract_request_route_parts(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    out: Dict[str, Any] = {}
    for attr in ("query_params", "path_params"):
        try:
            part = getattr(request, attr, None)
        except Exception:
            part = None
        if part is not None:
            out.update(_merge_route_body_dicts(part))
    try:
        state = getattr(request, "state", None)
        if state is not None:
            for attr in ("payload", "body", "json", "data", "params"):
                val = getattr(state, attr, None)
                if isinstance(val, Mapping):
                    out.update(_merge_route_body_dicts(val))
    except Exception:
        pass
    return out


def _normalize_route_call_inputs(
    *,
    page: Optional[str] = None,
    sheet: Optional[str] = None,
    sheet_name: Optional[str] = None,
    limit: int = 2000,
    offset: int = 0,
    mode: str = "",
    body: Optional[Dict[str, Any]] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> Tuple[str, int, int, str, Dict[str, Any], Dict[str, Any]]:
    extras = dict(extras or {})
    request_parts = _extract_request_route_parts(extras.get("request"))
    merged_body = _merge_route_body_dicts(
        request_parts,
        extras.get("params"), extras.get("query"), extras.get("query_params"),
        extras.get("payload"), extras.get("data"), extras.get("json"), extras.get("body"),
        body, extras,
    )
    target_raw = (
        page or sheet or sheet_name or _safe_str(merged_body.get("page")) or _safe_str(merged_body.get("sheet")) or
        _safe_str(merged_body.get("sheet_name")) or _safe_str(merged_body.get("page_name")) or _safe_str(merged_body.get("name")) or
        _safe_str(merged_body.get("tab")) or _safe_str(merged_body.get("worksheet")) or _safe_str(merged_body.get("sheetName")) or
        _safe_str(merged_body.get("pageName")) or _safe_str(merged_body.get("worksheet_name")) or "Market_Leaders"
    )
    effective_limit = _safe_int(merged_body.get("limit", limit), default=limit, lo=1, hi=5000)
    effective_offset = _safe_int(merged_body.get("offset", offset), default=offset, lo=0)
    effective_mode = _safe_str(merged_body.get("mode") or mode)
    passthrough = {k: v for k, v in merged_body.items() if k not in {"request", "params", "query", "query_params", "payload", "data", "json", "body"}}
    return _canonicalize_sheet_name(target_raw) or "Market_Leaders", effective_limit, effective_offset, effective_mode, passthrough, request_parts


# =============================================================================
# Schema registry helpers
# =============================================================================
try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY as _RAW_SCHEMA_REGISTRY  # type: ignore
    from core.sheets.schema_registry import get_sheet_spec as _RAW_GET_SHEET_SPEC  # type: ignore
except Exception:
    _RAW_SCHEMA_REGISTRY = {}
    _RAW_GET_SHEET_SPEC = None

SCHEMA_REGISTRY = _RAW_SCHEMA_REGISTRY if isinstance(_RAW_SCHEMA_REGISTRY, dict) else {}


def _schema_columns_from_any(spec: Any) -> List[Any]:
    if spec is None:
        return []
    if isinstance(spec, dict) and len(spec) == 1 and "columns" not in spec and "fields" not in spec:
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict) and ("columns" in first_val or "fields" in first_val):
            spec = first_val
    cols = getattr(spec, "columns", None)
    if isinstance(cols, list) and cols:
        return cols
    fields = getattr(spec, "fields", None)
    if isinstance(fields, list) and fields:
        return fields
    if isinstance(spec, Mapping):
        cols2 = spec.get("columns") or spec.get("fields")
        if isinstance(cols2, list) and cols2:
            return cols2
    return []


def _schema_keys_headers_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    cols = _schema_columns_from_any(spec)
    headers: List[str] = []
    keys: List[str] = []
    for c in cols:
        if isinstance(c, Mapping):
            h = _safe_str(c.get("header") or c.get("display_header") or c.get("label") or c.get("title"))
            k = _safe_str(c.get("key") or c.get("field") or c.get("name") or c.get("id"))
        else:
            h = _safe_str(getattr(c, "header", getattr(c, "display_header", getattr(c, "label", getattr(c, "title", None)))))
            k = _safe_str(getattr(c, "key", getattr(c, "field", getattr(c, "name", getattr(c, "id", None)))))
        if h or k:
            headers.append(h or k.replace("_", " ").title())
            keys.append(k or _norm_key(h))
    if not headers and not keys and isinstance(spec, Mapping):
        h2 = spec.get("headers") or spec.get("display_headers")
        k2 = spec.get("keys") or spec.get("fields")
        if isinstance(h2, list):
            headers = [_safe_str(x) for x in h2 if _safe_str(x)]
        if isinstance(k2, list):
            keys = [_safe_str(x) for x in k2 if _safe_str(x)]
    return _complete_schema_contract(headers, keys)


def get_sheet_spec(sheet: str) -> Any:
    canon = _canonicalize_sheet_name(sheet)
    if callable(_RAW_GET_SHEET_SPEC):
        for cand in _dedupe_keep_order([canon, canon.replace("_", " "), _norm_key(canon), sheet]):
            try:
                spec = _RAW_GET_SHEET_SPEC(cand)  # type: ignore[misc]
                if spec is not None:
                    return spec
            except Exception:
                continue
    if isinstance(SCHEMA_REGISTRY, dict):
        for cand in [canon, canon.replace("_", " "), _norm_key(canon), _norm_key_loose(canon)]:
            if cand in SCHEMA_REGISTRY:
                return SCHEMA_REGISTRY[cand]
        by_norm = {_norm_key(k): v for k, v in SCHEMA_REGISTRY.items()}
        by_loose = {_norm_key_loose(k): v for k, v in SCHEMA_REGISTRY.items()}
        if _norm_key(canon) in by_norm:
            return by_norm[_norm_key(canon)]
        if _norm_key_loose(canon) in by_loose:
            return by_loose[_norm_key_loose(canon)]
    static_contract = STATIC_CANONICAL_SHEET_CONTRACTS.get(canon)
    if static_contract:
        return dict(static_contract)
    raise KeyError(f"Unknown sheet spec: {sheet}")


def _schema_for_sheet(sheet: str) -> Tuple[Any, List[str], List[str], str]:
    canon = _canonicalize_sheet_name(sheet)
    try:
        spec = get_sheet_spec(canon)
        h, k = _schema_keys_headers_from_spec(spec)
        if canon == "Top_10_Investments":
            h, k = _ensure_top10_contract(h, k)
        if _usable_contract(h, k, canon):
            return spec, h, k, "schema_registry"
    except Exception:
        pass
    if canon in STATIC_CANONICAL_SHEET_CONTRACTS:
        c = STATIC_CANONICAL_SHEET_CONTRACTS[canon]
        h, k = _complete_schema_contract(c["headers"], c["keys"])
        if canon == "Top_10_Investments":
            h, k = _ensure_top10_contract(h, k)
        return dict(c), h, k, "static_canonical_contract"
    return None, [], [], "missing"


# =============================================================================
# Provider helpers
# =============================================================================
class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._tasks: Dict[str, asyncio.Task[Any]] = {}

    async def execute(self, key: str, factory: Any) -> Any:
        async with self._lock:
            task = self._tasks.get(key)
            if task is None:
                task = asyncio.create_task(factory())
                self._tasks[key] = task
        try:
            return await task
        finally:
            async with self._lock:
                if self._tasks.get(key) is task:
                    self._tasks.pop(key, None)


class MultiLevelCache:
    def __init__(self, name: str, l1_ttl: int = 60, max_l1_size: int = 5000) -> None:
        self.name = name
        self.l1_ttl = max(1, int(l1_ttl))
        self.max_l1_size = max(1, int(max_l1_size))
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._lock = asyncio.Lock()

    def _key(self, **kwargs: Any) -> str:
        items = sorted((str(k), _safe_str(v)) for k, v in kwargs.items())
        return "|".join([self.name] + [f"{k}={v}" for k, v in items])

    async def get(self, **kwargs: Any) -> Any:
        key = self._key(**kwargs)
        async with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at < time.time():
                self._data.pop(key, None)
                return None
            return value

    async def set(self, value: Any, **kwargs: Any) -> None:
        key = self._key(**kwargs)
        async with self._lock:
            if len(self._data) >= self.max_l1_size:
                oldest_key = next(iter(self._data.keys()), None)
                if oldest_key:
                    self._data.pop(oldest_key, None)
            self._data[key] = (time.time() + self.l1_ttl, value)


class ProviderRegistry:
    def __init__(self) -> None:
        self._stats: Dict[str, Dict[str, Any]] = {}

    async def get_provider(self, provider: str) -> Tuple[Optional[Any], Any]:
        module = None
        candidates = [
            f"core.providers.{provider}",
            f"providers.{provider}",
            f"core.providers.{provider}_provider",
            f"providers.{provider}_provider",
        ]
        for mod_path in candidates:
            try:
                module = import_module(mod_path)
                break
            except Exception:
                continue
        stats = type("ProviderStats", (), {"is_circuit_open": False, "last_import_error": "" if module is not None else "provider module missing"})()
        return module, stats

    async def record_success(self, provider: str, latency_ms: float) -> None:
        stat = self._stats.setdefault(provider, {"success": 0, "failure": 0, "last_error": "", "latency_ms": 0.0})
        stat["success"] += 1
        stat["latency_ms"] = round(float(latency_ms or 0.0), 2)

    async def record_failure(self, provider: str, error: str) -> None:
        stat = self._stats.setdefault(provider, {"success": 0, "failure": 0, "last_error": "", "latency_ms": 0.0})
        stat["failure"] += 1
        stat["last_error"] = _safe_str(error)

    async def get_stats(self) -> Dict[str, Any]:
        return {k: dict(v) for k, v in self._stats.items()}


def _pick_provider_callable(module: Any, provider: str) -> Optional[Any]:
    for name in ("get_quote", "fetch_quote", "fetch_enriched_quote", "get_enriched_quote", "quote"):
        fn = getattr(module, name, None)
        if callable(fn):
            return fn
    for attr in (provider, f"{provider}_quote", "client", "service"):
        obj = getattr(module, attr, None)
        if obj is not None:
            for name in ("get_quote", "fetch_quote", "get_enriched_quote"):
                fn = getattr(obj, name, None)
                if callable(fn):
                    return fn
    return None


# =============================================================================
# Row normalization helpers
# =============================================================================
_NULL_STRINGS: Set[str] = {"", "null", "none", "n/a", "na", "nan", "-", "--"}

_CANONICAL_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "code", "requested_symbol", "regularMarketSymbol"),
    "name": ("name", "shortName", "longName", "displayName", "companyName", "fundName", "description"),
    "asset_class": ("asset_class", "assetClass", "quoteType", "assetType", "instrumentType", "securityType", "type"),
    "exchange": ("exchange", "exchangeName", "fullExchangeName", "market", "marketName", "mic", "exchangeCode"),
    "currency": ("currency", "financialCurrency", "reportingCurrency", "quoteCurrency", "baseCurrency"),
    "country": ("country", "countryName", "country_code", "countryCode", "localeCountry"),
    "sector": ("sector", "sectorDisp", "gicsSector", "sectorName"),
    "industry": ("industry", "industryDisp", "gicsIndustry", "category", "industryName"),
    "current_price": ("current_price", "currentPrice", "price", "last", "lastPrice", "latestPrice", "regularMarketPrice", "nav", "close"),
    "previous_close": ("previous_close", "previousClose", "regularMarketPreviousClose", "prevClose", "priorClose"),
    "open_price": ("open_price", "day_open", "dayOpen", "open", "openPrice", "regularMarketOpen"),
    "day_high": ("day_high", "high", "dayHigh", "regularMarketDayHigh"),
    "day_low": ("day_low", "low", "dayLow", "regularMarketDayLow"),
    "week_52_high": ("week_52_high", "52WeekHigh", "fiftyTwoWeekHigh", "yearHigh"),
    "week_52_low": ("week_52_low", "52WeekLow", "fiftyTwoWeekLow", "yearLow"),
    "price_change": ("price_change", "change", "priceChange", "regularMarketChange"),
    "percent_change": ("percent_change", "changePercent", "percentChange", "regularMarketChangePercent", "pctChange", "change_pct"),
    "volume": ("volume", "regularMarketVolume", "sharesTraded", "tradeVolume", "vol"),
    "avg_volume_10d": ("avg_volume_10d", "avg_vol_10d", "averageVolume10days", "avgVolume10Day"),
    "avg_volume_30d": ("avg_volume_30d", "avg_vol_30d", "averageVolume", "averageDailyVolume3Month", "avgVolume3Month"),
    "market_cap": ("market_cap", "marketCap", "marketCapitalization", "capitalization"),
    "float_shares": ("float_shares", "floatShares", "sharesFloat", "sharesOutstanding"),
    "beta_5y": ("beta_5y", "beta", "beta5Y"),
    "pe_ttm": ("pe_ttm", "trailingPE", "peRatio", "priceEarningsTTM", "pe"),
    "pe_forward": ("pe_forward", "forward_pe", "forwardPE", "forwardPe"),
    "eps_ttm": ("eps_ttm", "trailingEps", "eps", "earningsPerShare"),
    "dividend_yield": ("dividend_yield", "dividendYield", "trailingAnnualDividendYield", "distributionYield"),
    "payout_ratio": ("payout_ratio", "payoutRatio"),
    "revenue_ttm": ("revenue_ttm", "totalRevenue", "revenueTTM", "revenue"),
    "revenue_growth_yoy": ("revenue_growth_yoy", "revenueGrowth", "revenueGrowthYoY"),
    "gross_margin": ("gross_margin", "grossMargins", "grossMargin"),
    "operating_margin": ("operating_margin", "operatingMargins", "operatingMargin"),
    "profit_margin": ("profit_margin", "profitMargins", "profitMargin", "netMargin"),
    "debt_to_equity": ("debt_to_equity", "d_e_ratio", "debtToEquity", "deRatio"),
    "free_cash_flow_ttm": ("free_cash_flow_ttm", "fcf_ttm", "freeCashflow", "freeCashFlow", "fcf"),
    "roe": ("roe", "returnOnEquity", "ROE"),
    "roa": ("roa", "returnOnAssets", "ROA"),
    "rsi_14": ("rsi_14", "rsi", "rsi14"),
    "pb_ratio": ("pb_ratio", "priceToBook", "pb"),
    "ps_ratio": ("ps_ratio", "priceToSalesTrailing12Months", "ps"),
    "ev_ebitda": ("ev_ebitda", "enterpriseToEbitda", "evToEbitda"),
    "peg_ratio": ("peg_ratio", "peg", "pegRatio"),
    "intrinsic_value": ("intrinsic_value", "fairValue", "dcf", "dcfValue"),
    "target_price": ("target_price", "targetPrice", "targetMeanPrice"),
    "data_provider": ("data_provider", "provider", "source", "dataProvider"),
    "last_updated_utc": ("last_updated_utc", "lastUpdated", "updatedAt", "timestamp", "asOf"),
    "warnings": ("warnings", "warning", "messages", "errors"),
}

_COMMODITY_SYMBOL_HINTS = ("GC=F", "SI=F", "BZ=F", "CL=F", "NG=F", "HG=F")
_ETF_SYMBOL_HINTS = ("SPY", "QQQ", "VTI", "VOO", "IWM", "DIA", "IVV", "EFA", "EEM", "ARKK")
_COMMODITY_DISPLAY_NAMES = {"GC=F": "Gold Futures", "SI=F": "Silver Futures", "BZ=F": "Brent Crude Futures", "CL=F": "WTI Crude Futures", "NG=F": "Natural Gas Futures", "HG=F": "Copper Futures"}
_ETF_DISPLAY_NAMES = {"SPY": "SPDR S&P 500 ETF", "QQQ": "Invesco QQQ Trust", "VTI": "Vanguard Total Stock Market ETF", "VOO": "Vanguard S&P 500 ETF", "IWM": "iShares Russell 2000 ETF", "DIA": "SPDR Dow Jones Industrial Average ETF", "IVV": "iShares Core S&P 500 ETF", "EFA": "iShares MSCI EAFE ETF", "EEM": "iShares MSCI Emerging Markets ETF", "ARKK": "ARK Innovation ETF"}
_COMMODITY_INDUSTRY_HINTS = {"GC=F": "Precious Metals", "SI=F": "Precious Metals", "HG=F": "Industrial Metals", "BZ=F": "Energy", "CL=F": "Energy", "NG=F": "Energy"}


def _is_blank_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in _NULL_STRINGS
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _to_scalar(value: Any) -> Any:
    if isinstance(value, (list, tuple, set)):
        seq = [v for v in value if not _is_blank_value(v)]
        if not seq:
            return None
        if all(not isinstance(v, (dict, list, tuple, set)) for v in seq):
            return seq[0] if len(seq) == 1 else "; ".join(_safe_str(v) for v in seq if _safe_str(v))
        return None
    return value


def _flatten_scalar_fields(obj: Any, out: Optional[Dict[str, Any]] = None, prefix: str = "", depth: int = 0, max_depth: int = 4) -> Dict[str, Any]:
    if out is None:
        out = {}
    if depth > max_depth or obj is None:
        return out
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            key = _safe_str(k)
            if not key:
                continue
            full = f"{prefix}.{key}" if prefix else key
            if isinstance(v, Mapping):
                _flatten_scalar_fields(v, out=out, prefix=full, depth=depth + 1, max_depth=max_depth)
                continue
            if isinstance(v, (list, tuple, set)) and v and isinstance(next(iter(v)), Mapping):
                continue
            scalar = _to_scalar(v)
            if scalar is None:
                continue
            out.setdefault(key, scalar)
            out.setdefault(full, scalar)
    return out


def _lookup_alias_value(src: Mapping[str, Any], flat: Mapping[str, Any], alias: str) -> Any:
    if not alias:
        return None
    candidates = [alias, alias.lower(), _norm_key(alias), _norm_key_loose(alias), alias.replace("_", " "), alias.replace("_", "-")]
    src_ci = {str(k).strip().lower(): v for k, v in src.items()}
    src_loose = {_norm_key_loose(k): v for k, v in src.items()}
    flat_ci = {str(k).strip().lower(): v for k, v in flat.items()}
    flat_loose = {_norm_key_loose(k): v for k, v in flat.items()}
    for cand in candidates:
        if cand in src and not _is_blank_value(src.get(cand)):
            return src.get(cand)
        if cand in flat and not _is_blank_value(flat.get(cand)):
            return flat.get(cand)
        low = cand.lower()
        if low in src_ci and not _is_blank_value(src_ci.get(low)):
            return src_ci.get(low)
        if low in flat_ci and not _is_blank_value(flat_ci.get(low)):
            return flat_ci.get(low)
        loose = _norm_key_loose(cand)
        if loose in src_loose and not _is_blank_value(src_loose.get(loose)):
            return src_loose.get(loose)
        if loose in flat_loose and not _is_blank_value(flat_loose.get(loose)):
            return flat_loose.get(loose)
    return None


def _infer_asset_class_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Equity"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F") or s in _COMMODITY_SYMBOL_HINTS:
        return "Commodity"
    if s in _ETF_SYMBOL_HINTS:
        return "ETF"
    return "Equity"


def _infer_exchange_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Tadawul"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F"):
        return "Futures"
    return "NASDAQ/NYSE"


def _infer_currency_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "SAR"
    if s.endswith("=X"):
        pair = s[:-2]
        return pair[-3:] if len(pair) >= 6 else (pair or "FX")
    return "USD"


def _infer_country_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Saudi Arabia"
    if s.endswith("=X") or s.endswith("=F"):
        return "Global"
    return "USA"


def _infer_sector_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith("=X"):
        return "Currencies"
    if s.endswith("=F") or s in _COMMODITY_SYMBOL_HINTS:
        return "Commodities"
    if s in _ETF_SYMBOL_HINTS:
        return "Broad Market"
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Saudi Market"
    return ""


def _infer_industry_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s in _COMMODITY_INDUSTRY_HINTS:
        return _COMMODITY_INDUSTRY_HINTS[s]
    if s.endswith("=X"):
        return "Foreign Exchange"
    if s.endswith("=F"):
        return "Commodity Futures"
    if s in _ETF_SYMBOL_HINTS:
        return "ETF"
    if s.endswith(".SR") or re.match(r"^[0-9]{4}$", s):
        return "Listed Equities"
    return ""


def _infer_display_name_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if s in _COMMODITY_DISPLAY_NAMES:
        return _COMMODITY_DISPLAY_NAMES[s]
    if s in _ETF_DISPLAY_NAMES:
        return _ETF_DISPLAY_NAMES[s]
    if s.endswith("=X"):
        pair = s[:-2]
        return f"{pair[:3]}/{pair[3:6]}" if len(pair) >= 6 else (f"{pair} FX" if pair else s)
    if s.endswith("=F"):
        return _safe_str(s.replace("=F", "")).strip() or s
    return s


def _apply_symbol_context_defaults(row: Dict[str, Any], symbol: str = "", page: str = "") -> Dict[str, Any]:
    out = dict(row or {})
    sym = normalize_symbol(symbol or _safe_str(out.get("symbol") or out.get("ticker") or out.get("requested_symbol")))
    if not sym:
        return out
    page = _canonicalize_sheet_name(page) if page else ""
    out.setdefault("symbol", sym)
    out.setdefault("requested_symbol", sym)
    out.setdefault("symbol_normalized", sym)
    out.setdefault("name", _infer_display_name_from_symbol(sym) or sym)
    out.setdefault("asset_class", _infer_asset_class_from_symbol(sym))
    out.setdefault("exchange", _infer_exchange_from_symbol(sym))
    out.setdefault("currency", _infer_currency_from_symbol(sym))
    out.setdefault("country", _infer_country_from_symbol(sym))
    if page == "Commodities_FX" or sym.endswith("=F") or sym.endswith("=X"):
        out.setdefault("sector", _infer_sector_from_symbol(sym))
        out.setdefault("industry", _infer_industry_from_symbol(sym))
        out.setdefault("invest_period_label", "1Y")
        out.setdefault("horizon_days", 365)
    return out


def _canonicalize_provider_row(row: Dict[str, Any], requested_symbol: str = "", normalized_symbol: str = "", provider: str = "") -> Dict[str, Any]:
    src = dict(row or {})
    flat = _flatten_scalar_fields(src)
    symbol = normalized_symbol or normalize_symbol(_safe_str(_lookup_alias_value(src, flat, "symbol") or requested_symbol))
    out: Dict[str, Any] = {"symbol": symbol or requested_symbol, "symbol_normalized": symbol or requested_symbol, "requested_symbol": requested_symbol or symbol}
    for field, aliases in _CANONICAL_FIELD_ALIASES.items():
        for alias in (field,) + tuple(aliases):
            val = _lookup_alias_value(src, flat, alias)
            if not _is_blank_value(val):
                out[field] = _json_safe(_to_scalar(val))
                break
    out = _apply_symbol_context_defaults(out, symbol=symbol)
    if provider and not out.get("data_provider"):
        out["data_provider"] = provider
    out.setdefault("last_updated_utc", _now_utc_iso())
    out.setdefault("last_updated_riyadh", _now_riyadh_iso())
    price = _as_float(out.get("current_price"))
    prev = _as_float(out.get("previous_close"))
    if out.get("price_change") is None and price is not None and prev is not None:
        out["price_change"] = price - prev
    if out.get("percent_change") is None and price is not None and prev not in (None, 0):
        out["percent_change"] = ((price - prev) / prev) * 100.0
    return out


def _normalize_to_schema_keys(keys: Sequence[str], headers: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    src = _canonicalize_provider_row(dict(row or {}), requested_symbol=_safe_str((row or {}).get("requested_symbol")), normalized_symbol=normalize_symbol(_safe_str((row or {}).get("symbol") or (row or {}).get("ticker"))), provider=_safe_str((row or {}).get("data_provider") or (row or {}).get("provider")))
    flat = _flatten_scalar_fields(src)
    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers) else key
        aliases = [key, header, _norm_key(key), _norm_key(header), key.lower(), header.lower(), key.replace("_", " ")]
        aliases.extend(_CANONICAL_FIELD_ALIASES.get(key, ()))
        val = None
        found = False
        for alias in aliases:
            val = _lookup_alias_value(src, flat, alias)
            if not _is_blank_value(val):
                found = True
                break
        out[key] = _json_safe(_to_scalar(val)) if found else None
    return out


def _strict_project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe(row.get(k)) for k in keys}


def _strict_project_row_display(headers: Sequence[str], keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers or []) else key
        out[header] = _json_safe(row.get(key))
    return out


def _rows_display_objects_from_rows(rows: List[Dict[str, Any]], headers: List[str], keys: List[str]) -> List[Dict[str, Any]]:
    return [_strict_project_row_display(headers, keys, row) for row in rows or []]


def _rows_matrix_from_rows(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows or []]


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    if out is None:
        return []
    if isinstance(out, list):
        if not out:
            return []
        if isinstance(out[0], dict):
            return [dict(r) for r in out if isinstance(r, dict)]
        return []
    if isinstance(out, dict):
        for key in ("row_objects", "records", "items", "data", "quotes"):
            val = out.get(key)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return [dict(r) for r in val if isinstance(r, dict)]
        rows_matrix = out.get("rows_matrix") or out.get("rows")
        cols = out.get("keys") or out.get("headers") or []
        if isinstance(rows_matrix, list) and isinstance(cols, list) and cols:
            keys = [_safe_str(c) for c in cols if _safe_str(c)]
            res: List[Dict[str, Any]] = []
            for row in rows_matrix:
                if isinstance(row, (list, tuple)):
                    res.append({k: row[i] if i < len(row) else None for i, k in enumerate(keys)})
            return res
        if {"sheet", "header", "key"}.issubset(set(out.keys())) or {"symbol", "ticker", "requested_symbol"} & set(out.keys()):
            return [dict(out)]
    return []


def _extract_row_symbol(row: Dict[str, Any]) -> str:
    for k in ("symbol", "ticker", "code", "requested_symbol", "Symbol", "Ticker", "Code"):
        v = row.get(k)
        if v:
            return _safe_str(v)
    return ""


def _extract_symbols_from_rows(rows: Sequence[Dict[str, Any]], limit: int = 5000) -> List[str]:
    raw: List[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        sym = _extract_row_symbol(row)
        if sym:
            raw.append(sym)
    return _normalize_symbol_list(raw, limit=limit)


def _merge_missing_fields(base_row: Dict[str, Any], template_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base_row or {})
    if not isinstance(template_row, dict):
        return out
    for k, v in template_row.items():
        if out.get(k) in (None, "", [], {}) and v not in (None, "", [], {}):
            out[k] = _json_safe(v)
    return out


def _derive_new_columns(row: Dict[str, Any], page: str = "") -> None:
    if row.get("volume_ratio") in (None, ""):
        vol = _as_float(row.get("volume"))
        avg_vol = _as_float(row.get("avg_volume_10d")) or _as_float(row.get("avg_volume_30d"))
        if vol is not None and avg_vol not in (None, 0):
            row["volume_ratio"] = round(vol / avg_vol, 4)
    if row.get("day_range_position") in (None, ""):
        price = _as_float(row.get("current_price"))
        low = _as_float(row.get("day_low"))
        high = _as_float(row.get("day_high"))
        if price is not None and low is not None and high is not None and high > low:
            row["day_range_position"] = round(_clamp((price - low) / (high - low), 0.0, 1.0), 4)
    if row.get("upside_pct") in (None, ""):
        price = _as_float(row.get("current_price"))
        intrinsic = _as_float(row.get("intrinsic_value")) or _as_float(row.get("target_price"))
        if price not in (None, 0) and intrinsic is not None:
            row["upside_pct"] = round((intrinsic - price) / price, 4)
    if row.get("rsi_signal") in (None, ""):
        rsi = _as_float(row.get("rsi_14"))
        if rsi is not None:
            row["rsi_signal"] = "Oversold" if rsi < 30 else "Overbought" if rsi > 70 else "Neutral"


def _compute_scores_fallback(row: Dict[str, Any]) -> None:
    price = _as_float(row.get("current_price"))
    pe = _as_float(row.get("pe_ttm"))
    pb = _as_float(row.get("pb_ratio"))
    rev_growth = _as_pct_points(row.get("revenue_growth_yoy")) or 0.0
    margin = _as_pct_points(row.get("profit_margin")) or 0.0
    div_yield = _as_pct_points(row.get("dividend_yield")) or 0.0
    if row.get("value_score") is None:
        score = 55.0
        if pe is not None and pe > 0:
            score += max(0.0, 22.0 - min(pe, 22.0))
        if pb is not None and pb > 0:
            score += max(0.0, 10.0 - min(pb * 2.0, 10.0))
        score += min(div_yield, 10.0)
        row["value_score"] = round(_clamp(score, 0.0, 100.0), 2)
    if row.get("quality_score") is None:
        row["quality_score"] = round(_clamp(45.0 + margin * 0.7, 0.0, 100.0), 2)
    if row.get("growth_score") is None:
        row["growth_score"] = round(_clamp(50.0 + _clamp(rev_growth, -25.0, 35.0), 0.0, 100.0), 2)
    if row.get("momentum_score") is None:
        pct = _as_pct_points(row.get("percent_change")) or 0.0
        row["momentum_score"] = round(_clamp(50.0 + pct, 0.0, 100.0), 2)
    if row.get("valuation_score") is None:
        intrinsic = _as_float(row.get("intrinsic_value")) or _as_float(row.get("target_price"))
        score = 50.0
        if intrinsic is not None and price not in (None, 0):
            score += _clamp(((intrinsic - price) / price) * 100.0, -20.0, 25.0)
        row["valuation_score"] = round(_clamp(score, 0.0, 100.0), 2)
    if row.get("overall_score") is None:
        vals = [
            _as_float(row.get("value_score")), _as_float(row.get("quality_score")), _as_float(row.get("growth_score")),
            _as_float(row.get("momentum_score")), _as_float(row.get("valuation_score")),
        ]
        vals = [v for v in vals if v is not None]
        row["overall_score"] = round(sum(vals) / len(vals), 2) if vals else 50.0
    if row.get("risk_score") is None:
        beta = _as_float(row.get("beta_5y")) or 1.0
        row["risk_score"] = round(_clamp(35.0 + beta * 10.0, 0.0, 100.0), 2)
    if row.get("risk_bucket") is None:
        rs = _as_float(row.get("risk_score")) or 50.0
        row["risk_bucket"] = "LOW" if rs < 40 else "MODERATE" if rs < 70 else "HIGH"
    if row.get("forecast_confidence") is None:
        row["forecast_confidence"] = 0.55
    if row.get("confidence_score") is None:
        fc = _as_float(row.get("forecast_confidence")) or 0.55
        row["confidence_score"] = round(fc * 100.0 if fc <= 1.5 else fc, 2)
    if row.get("confidence_bucket") is None:
        cs = _as_float(row.get("confidence_score")) or 55.0
        row["confidence_bucket"] = "HIGH" if cs >= 75 else "MODERATE" if cs >= 55 else "LOW"
    if row.get("opportunity_score") is None:
        base = _as_float(row.get("overall_score")) or 50.0
        conf = _as_float(row.get("confidence_score")) or 55.0
        risk = _as_float(row.get("risk_score")) or 50.0
        row["opportunity_score"] = round(_clamp(base + (conf - 50.0) * 0.2 - (risk - 50.0) * 0.25, 0.0, 100.0), 2)
    if price is not None:
        row.setdefault("forecast_price_1m", round(price * 1.01, 4))
        row.setdefault("forecast_price_3m", round(price * 1.03, 4))
        row.setdefault("forecast_price_12m", round(price * 1.08, 4))
        if row.get("expected_roi_1m") is None:
            row["expected_roi_1m"] = round((row["forecast_price_1m"] - price) / price, 6)
        if row.get("expected_roi_3m") is None:
            row["expected_roi_3m"] = round((row["forecast_price_3m"] - price) / price, 6)
        if row.get("expected_roi_12m") is None:
            row["expected_roi_12m"] = round((row["forecast_price_12m"] - price) / price, 6)


_scoring_mod: Any = None
_scoring_mod_tried = False


def _get_scoring_mod() -> Any:
    global _scoring_mod, _scoring_mod_tried
    if _scoring_mod_tried:
        return _scoring_mod
    _scoring_mod_tried = True
    for mod_path in ("core.scoring", "scoring"):
        try:
            mod = import_module(mod_path)
            if callable(getattr(mod, "compute_scores", None)):
                _scoring_mod = mod
                return _scoring_mod
        except Exception:
            continue
    _scoring_mod = None
    return None


def _try_scoring_module(row: Dict[str, Any], settings: Any = None) -> None:
    mod = _get_scoring_mod()
    if mod is not None:
        try:
            patch = mod.compute_scores(row, settings=settings)
            if isinstance(patch, dict):
                for k, v in patch.items():
                    if v is not None and row.get(k) in (None, "", [], {}):
                        row[k] = v
                return
        except Exception:
            pass
    _compute_scores_fallback(row)


def _compute_recommendation(row: Dict[str, Any]) -> None:
    if row.get("recommendation"):
        return
    overall = _as_float(row.get("overall_score")) or 50.0
    conf = _as_float(row.get("confidence_score")) or 55.0
    risk = _as_float(row.get("risk_score")) or 50.0
    tech = _as_float(row.get("technical_score"))
    if overall >= 78 and conf >= 70 and risk <= 45:
        rec = "STRONG_BUY"
    elif overall >= 72 and conf >= 65 and risk <= 60:
        rec = "BUY"
    elif overall >= 60 and conf >= 55:
        rec = "BUY"
    elif tech is not None and tech >= 65 and conf >= 55:
        rec = "BUY"
    elif overall <= 35 or risk >= 85:
        rec = "REDUCE"
    else:
        rec = "HOLD"
    row["recommendation"] = rec
    row.setdefault("recommendation_reason", f"overall={round(overall,1)} conf={round(conf,1)} risk={round(risk,1)}")


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    scored: List[Tuple[int, float]] = []
    for i, row in enumerate(rows):
        score = _as_float(row.get("overall_score"))
        if score is None:
            score = _as_float(row.get("opportunity_score"))
        if score is not None:
            scored.append((i, score))
    scored.sort(key=lambda t: t[1], reverse=True)
    for rank, (idx, _) in enumerate(scored, start=1):
        rows[idx]["rank_overall"] = rank


def _apply_page_row_backfill(sheet: str, row: Dict[str, Any]) -> Dict[str, Any]:
    target = _canonicalize_sheet_name(sheet)
    out = _apply_symbol_context_defaults(dict(row or {}), page=target)
    out.setdefault("last_updated_utc", _now_utc_iso())
    out.setdefault("last_updated_riyadh", _now_riyadh_iso())
    out.setdefault("invest_period_label", "1Y")
    out.setdefault("horizon_days", 365)
    _derive_new_columns(out, page=target)
    return out


# =============================================================================
# Engine
# =============================================================================
class _EngineSymbolsReaderProxy:
    def __init__(self, engine: "DataEngineV5") -> None:
        self._engine = engine

    async def get_symbols_for_sheet(self, sheet: str, limit: int = 5000) -> List[str]:
        return await self._engine.get_sheet_symbols(sheet, limit=limit)

    async def get_symbols_for_page(self, page: str, limit: int = 5000) -> List[str]:
        return await self._engine.get_page_symbols(page, limit=limit)

    async def list_symbols_for_page(self, page: str, limit: int = 5000) -> List[str]:
        return await self._engine.list_symbols_for_page(page, limit=limit)


class DataEngineV5:
    def __init__(self, settings: Any = None) -> None:
        self.settings = settings
        self.version = __version__
        self.primary_provider = _safe_str(getattr(settings, "primary_provider", None), _safe_env("PRIMARY_PROVIDER", "eodhd")).lower() or "eodhd"
        self.enabled_providers = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)
        self.non_ksa_primary_provider = _safe_str(getattr(settings, "non_ksa_primary_provider", None), _safe_env("NON_KSA_PRIMARY_PROVIDER", "eodhd")).lower() or "eodhd"
        configured_non_ksa_pages = [_canonicalize_sheet_name(p) for p in _get_env_list("NON_KSA_PRIMARY_PAGES", list(NON_KSA_EODHD_PRIMARY_PAGES)) if _safe_str(p)]
        self.page_primary_providers = {page: self.non_ksa_primary_provider for page in configured_non_ksa_pages if page}
        self.max_concurrency = _get_env_int("DATA_ENGINE_MAX_CONCURRENCY", 25)
        self.request_timeout = _get_env_float("DATA_ENGINE_TIMEOUT_SECONDS", 20.0)
        self.ksa_disallow_eodhd = _get_env_bool("KSA_DISALLOW_EODHD", True)
        self.schema_strict_sheet_rows = _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)
        self.top10_force_full_schema = _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True)
        self.rows_hydrate_external = _get_env_bool("ROWS_HYDRATE_EXTERNAL_READER", True)
        self._sem = asyncio.Semaphore(max(1, self.max_concurrency))
        self._singleflight = SingleFlight()
        self._registry = ProviderRegistry()
        self._cache = MultiLevelCache("data_engine", l1_ttl=_get_env_int("CACHE_L1_TTL", 60), max_l1_size=_get_env_int("CACHE_L1_MAX", 5000))
        self._symbols_cache = MultiLevelCache("sheet_symbols", l1_ttl=_get_env_int("SHEET_SYMBOLS_L1_TTL", 300), max_l1_size=_get_env_int("SHEET_SYMBOLS_L1_MAX", 256))
        self._sheet_snapshots: Dict[str, Dict[str, Any]] = {}
        self._sheet_symbol_resolution_meta: Dict[str, Dict[str, Any]] = {}
        self._symbols_reader_source = ""
        self._rows_reader_source = ""
        self.flags = {
            "computations_enabled": True,
            "forecasting_enabled": True,
            "scoring_enabled": True,
        }
        self.symbols_reader = _EngineSymbolsReaderProxy(self)

    async def aclose(self) -> None:
        return

    async def health(self) -> Dict[str, Any]:
        return await self.get_health()

    async def get_health(self) -> Dict[str, Any]:
        return {
            "status": "healthy",
            "version": self.version,
            "timestamp_utc": _now_utc_iso(),
            "providers": list(self.enabled_providers),
            "provider_stats": await self._registry.get_stats(),
        }

    async def health_check(self) -> Dict[str, Any]:
        return await self.get_health()

    async def get_stats(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "enabled_providers": list(self.enabled_providers),
            "ksa_providers": list(self.ksa_providers),
            "global_providers": list(self.global_providers),
            "non_ksa_primary_provider": self.non_ksa_primary_provider,
            "page_primary_providers": dict(self.page_primary_providers),
            "provider_stats": await self._registry.get_stats(),
            "schema_strict_sheet_rows": bool(self.schema_strict_sheet_rows),
            "top10_force_full_schema": bool(self.top10_force_full_schema),
            "rows_hydrate_external": bool(self.rows_hydrate_external),
            "snapshot_sheets": sorted(self._sheet_snapshots.keys()),
            "sheet_symbol_resolution_meta": dict(self._sheet_symbol_resolution_meta),
        }

    def _store_sheet_snapshot(self, sheet: str, payload: Dict[str, Any]) -> None:
        target = _canonicalize_sheet_name(sheet)
        if target and isinstance(payload, dict):
            self._sheet_snapshots[target] = dict(payload)

    def get_cached_sheet_snapshot(self, sheet: Optional[str] = None, page: Optional[str] = None, sheet_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet or page or sheet_name or "")
        if not target:
            return None
        snap = self._sheet_snapshots.get(target)
        return dict(snap) if isinstance(snap, dict) else None

    def _set_sheet_symbols_meta(self, sheet: str, source: str, count: int, note: Optional[str] = None) -> None:
        target = _canonicalize_sheet_name(sheet)
        if not target:
            return
        self._sheet_symbol_resolution_meta[target] = {
            "sheet": target,
            "source": source,
            "count": int(count or 0),
            "note": note or "",
            "timestamp_utc": _now_utc_iso(),
        }

    def _get_sheet_symbols_meta(self, sheet: str) -> Dict[str, Any]:
        target = _canonicalize_sheet_name(sheet)
        meta = self._sheet_symbol_resolution_meta.get(target)
        return dict(meta) if isinstance(meta, dict) else {}

    def _finalize_payload(self, *, sheet: str, headers: List[str], keys: List[str], row_objects: List[Dict[str, Any]], include_matrix: bool, status: str = "success", meta: Optional[Dict[str, Any]] = None, error: Optional[str] = None) -> Dict[str, Any]:
        headers, keys = _complete_schema_contract(headers, keys)
        dict_rows = [_strict_project_row(keys, r) for r in row_objects or []]
        display_row_objects = _rows_display_objects_from_rows(dict_rows, headers, keys)
        matrix_rows = _rows_matrix_from_rows(dict_rows, keys) if include_matrix else []
        payload = {
            "status": status,
            "sheet": sheet,
            "page": sheet,
            "sheet_name": sheet,
            "headers": headers,
            "display_headers": headers,
            "keys": keys,
            "fields": keys,
            "columns": keys,
            "rows": matrix_rows,
            "rows_matrix": matrix_rows,
            "row_objects": dict_rows,
            "items": dict_rows,
            "records": dict_rows,
            "data": dict_rows,
            "quotes": dict_rows,
            "display_row_objects": display_row_objects,
            "count": len(dict_rows),
            "meta": dict(meta or {}),
            "version": self.version,
        }
        if error is not None:
            payload["error"] = error
        return _json_safe(payload)

    def _providers_for(self, symbol: str, page: str = "") -> List[str]:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))
        page_ctx = _canonicalize_sheet_name(page) if page else ""
        def _provider_allowed(provider: str) -> bool:
            provider = _safe_str(provider).lower()
            if not provider or provider not in self.enabled_providers:
                return False
            if is_ksa_sym and self.ksa_disallow_eodhd and provider == "eodhd":
                return False
            return True
        providers = [p for p in (self.ksa_providers if is_ksa_sym else self.global_providers) if _provider_allowed(p)]
        primary = self.non_ksa_primary_provider if (not is_ksa_sym and page_ctx in self.page_primary_providers) else self.primary_provider
        if primary and _provider_allowed(primary):
            providers = [p for p in providers if p != primary]
            providers.insert(0, primary)
        seen: Set[str] = set()
        out: List[str] = []
        for p in providers:
            p2 = _safe_str(p).lower()
            if p2 and p2 not in seen:
                seen.add(p2)
                out.append(p2)
        return out

    async def _fetch_patch(self, provider: str, symbol: str) -> Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.time()
        async with self._sem:
            module, stats = await self._registry.get_provider(provider)
            if getattr(stats, "is_circuit_open", False):
                return provider, None, 0.0, "circuit_open"
            if module is None:
                err = getattr(stats, "last_import_error", "provider module missing")
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000.0, err
            fn = _pick_provider_callable(module, provider)
            if fn is None:
                err = f"no callable fetch function for provider '{provider}'"
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000.0, err
            variants = [((symbol,), {}), ((), {"symbol": symbol}), ((), {"ticker": symbol}), ((symbol,), {"settings": self.settings}), ((), {"symbol": symbol, "settings": self.settings})]
            result = None
            collected_errs: List[str] = []
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(self.request_timeout):
                        if inspect.iscoroutinefunction(fn):
                            result = await fn(*args, **kwargs)
                        else:
                            result = await asyncio.to_thread(fn, *args, **kwargs)
                    break
                except TypeError:
                    continue
                except Exception as exc:
                    collected_errs.append(f"{type(exc).__name__}: {str(exc)[:120]}")
                    continue
            latency = (time.time() - start) * 1000.0
            patch = _model_to_dict(result)
            if patch and isinstance(patch, dict):
                await self._registry.record_success(provider, latency)
                return provider, patch, latency, None
            err = " | ".join(collected_errs) if collected_errs else "non_dict_or_empty"
            await self._registry.record_failure(provider, err)
            return provider, None, latency, err

    def _merge(self, requested_symbol: str, norm: str, patches: List[Tuple[str, Dict[str, Any], float]]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {
            "symbol": norm,
            "symbol_normalized": norm,
            "requested_symbol": requested_symbol,
            "last_updated_utc": _now_utc_iso(),
            "last_updated_riyadh": _now_riyadh_iso(),
            "data_sources": [],
            "provider_latency": {},
        }
        normalized_patches: List[Tuple[str, Dict[str, Any], float]] = []
        for prov, patch, latency in patches:
            canonical = _canonicalize_provider_row(patch, requested_symbol=requested_symbol, normalized_symbol=norm, provider=prov)
            normalized_patches.append((prov, canonical, latency))
        sorted_patches = sorted(normalized_patches, key=lambda item: (PROVIDER_PRIORITIES.get(item[0], 999), -sum(1 for v in item[1].values() if v not in (None, "", [], {}))))
        for prov, patch, latency in sorted_patches:
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(float(latency or 0.0), 2)
            for k, v in patch.items():
                if k in {"symbol", "symbol_normalized", "requested_symbol"} or v is None:
                    continue
                if k not in merged or merged.get(k) in (None, "", [], {}):
                    merged[k] = v
        return _canonicalize_provider_row(merged, requested_symbol=requested_symbol, normalized_symbol=norm, provider=_safe_str((merged.get("data_sources") or [""])[0] if isinstance(merged.get("data_sources"), list) else ""))

    def _data_quality(self, row: Dict[str, Any]) -> str:
        if _as_float(row.get("current_price")) is None:
            return QuoteQuality.MISSING.value
        return QuoteQuality.GOOD.value if any(row.get(k) is not None for k in ("overall_score", "forecast_price_3m", "pb_ratio")) else QuoteQuality.FAIR.value

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True, *, page: str = "", sheet: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> UnifiedQuote:
        norm = normalize_symbol(symbol)
        if not norm:
            return UnifiedQuote(symbol=_safe_str(symbol), requested_symbol=_safe_str(symbol), data_quality=QuoteQuality.MISSING.value, error="Invalid symbol", last_updated_utc=_now_utc_iso(), last_updated_riyadh=_now_riyadh_iso())
        page_ctx = _canonicalize_sheet_name(page or sheet or _safe_str((body or {}).get("page") or (body or {}).get("sheet")))
        cache_key = f"{norm}|{page_ctx or 'default'}"
        if use_cache:
            cached = await self._cache.get(symbol=cache_key)
            if isinstance(cached, dict) and cached:
                return UnifiedQuote(**cached)
        providers = self._providers_for(norm, page=page_ctx)
        patches_ok: List[Tuple[str, Dict[str, Any], float]] = []
        if providers:
            gathered = await asyncio.gather(*[self._fetch_patch(p, norm) for p in providers[:4]], return_exceptions=True)
            for item in gathered:
                if isinstance(item, tuple) and len(item) == 4:
                    provider, patch, latency, _err = item
                    if patch:
                        patches_ok.append((provider, patch, latency))
        if patches_ok:
            row = self._merge(symbol, norm, patches_ok)
        else:
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": _safe_str(symbol),
                "name": _infer_display_name_from_symbol(norm) or norm,
                "current_price": None,
                "warnings": "No live provider data available",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
                "data_sources": [],
                "provider_latency": {},
            }
        row = _apply_page_row_backfill(page_ctx or "Market_Leaders", row)
        _try_scoring_module(row)
        _compute_recommendation(row)
        row["data_quality"] = self._data_quality(row)
        if not row.get("data_provider"):
            row["data_provider"] = _safe_str((row.get("data_sources") or [""])[0] if isinstance(row.get("data_sources"), list) else "")
        q = UnifiedQuote(**row)
        if use_cache:
            await self._cache.set(_model_to_dict(q), symbol=cache_key)
        return q

    async def get_enriched_quote(self, symbol: str, use_cache: bool = True, *, schema: Any = None, page: str = "", sheet: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> UnifiedQuote:
        key = f"quote:{normalize_symbol(symbol)}:{_canonicalize_sheet_name(page or sheet or '')}:{'cache' if use_cache else 'live'}"
        q = await self._singleflight.execute(key, lambda: self._get_enriched_quote_impl(symbol, use_cache, page=page, sheet=sheet, body=body, **kwargs))
        if schema is None:
            return q
        row = _model_to_dict(q)
        if isinstance(schema, str):
            _spec, hdrs, keys, _src = _schema_for_sheet(schema)
            projected = _normalize_to_schema_keys(keys, hdrs, row)
            return UnifiedQuote(**projected)
        return q

    async def get_enriched_quote_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None, page: str = "", sheet: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
        q = await self.get_enriched_quote(symbol, use_cache=use_cache, schema=schema, page=page, sheet=sheet, body=body, **kwargs)
        return _model_to_dict(q)

    async def get_enriched_quotes(self, symbols: List[str], *, schema: Any = None, page: str = "", sheet: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> List[UnifiedQuote]:
        if not symbols:
            return []
        batch = max(1, min(500, _get_env_int("QUOTE_BATCH_SIZE", 25)))
        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i:i + batch]
            out.extend(await asyncio.gather(*[self.get_enriched_quote(s, schema=schema, page=page, sheet=sheet, body=body, **kwargs) for s in part]))
        return out

    async def get_enriched_quotes_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None, page: str = "", sheet: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        norm_syms = _normalize_symbol_list(symbols, limit=len(symbols) + 10)
        quotes = await asyncio.gather(*[self.get_enriched_quote_dict(s, schema=schema, page=page, sheet=sheet, body=body, **kwargs) for s in norm_syms])
        for req_sym, qd in zip(norm_syms, quotes):
            out[req_sym] = qd
            norm = _safe_str(qd.get("symbol_normalized") or qd.get("symbol"))
            if norm:
                out[norm] = qd
        return out

    get_quote = get_enriched_quote
    get_quotes = get_enriched_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_enriched_quotes
    get_quotes_batch = get_enriched_quotes_batch
    get_analysis_quotes_batch = get_enriched_quotes_batch
    quotes_batch = get_enriched_quotes_batch
    get_quote_dict = get_enriched_quote_dict

    async def _build_data_dictionary_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for sheet_name, contract in STATIC_CANONICAL_SHEET_CONTRACTS.items():
            headers, keys = _complete_schema_contract(contract["headers"], contract["keys"])
            for idx, (header, key) in enumerate(zip(headers, keys), start=1):
                group = "Canonical"
                if key in TOP10_REQUIRED_FIELDS:
                    group = "Top10"
                elif sheet_name == "Insights_Analysis":
                    group = "Insights"
                elif sheet_name == "Data_Dictionary":
                    group = "Metadata"
                elif idx <= 8:
                    group = "Identity"
                elif idx <= 18:
                    group = "Price"
                elif idx <= 24:
                    group = "Liquidity"
                elif idx <= 36:
                    group = "Fundamentals"
                elif idx <= 44:
                    group = "Risk"
                elif idx <= 50:
                    group = "Valuation"
                elif idx <= 69:
                    group = "Forecast & Scoring"
                else:
                    group = "Portfolio & Provenance"
                rows.append({
                    "sheet": sheet_name,
                    "group": group,
                    "header": header,
                    "key": key,
                    "dtype": "string",
                    "fmt": "",
                    "required": idx <= 3,
                    "source": "static_contract",
                    "notes": "schema contract",
                })
        return rows

    async def _build_insights_rows_fallback(self, body: Optional[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        body = dict(body or {})
        symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 2, 10))
        if not symbols:
            for page_name in TOP10_ENGINE_DEFAULT_PAGES:
                symbols.extend(await self.get_sheet_symbols(page_name, limit=max(limit * 2, 10), body=body))
        symbols = _normalize_symbol_list(symbols, limit=max(limit * 3, 30))
        if not symbols:
            symbols = list(EMERGENCY_PAGE_SYMBOLS.get("Market_Leaders", [])[: max(limit, 6)])
        quotes = await self.get_enriched_quotes(symbols, schema=None, page="Insights_Analysis", body=body)
        rows = [_model_to_dict(q) for q in quotes]
        rows = [r for r in rows if isinstance(r, dict)]
        rows.sort(key=lambda r: (_as_float(r.get("opportunity_score")) or _as_float(r.get("overall_score")) or 0.0, _as_float(r.get("confidence_score")) or 0.0), reverse=True)
        out: List[Dict[str, Any]] = []
        out.append({"section": "Market Summary", "item": "Universe", "symbol": None, "metric": "Symbols Analyzed", "value": len(rows), "signal": None, "priority": "High", "notes": "engine fallback summary", "as_of_riyadh": _now_riyadh_iso()})
        for idx, d in enumerate(rows[: max(3, min(7, limit))], start=1):
            out.append({"section": "Top Ideas", "item": d.get("name") or d.get("symbol"), "symbol": d.get("symbol"), "metric": "Recommendation", "value": d.get("recommendation"), "signal": d.get("recommendation"), "priority": "High" if idx <= 3 else "Medium", "notes": d.get("recommendation_reason"), "as_of_riyadh": _now_riyadh_iso()})
        return out[:limit]

    async def _build_top10_rows_fallback(self, headers: Sequence[str], keys: Sequence[str], body: Optional[Dict[str, Any]], limit: int, mode: str = "") -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        body = dict(body or {})
        criteria = dict(body.get("criteria") or {}) if isinstance(body.get("criteria"), dict) else {}
        out_headers, out_keys = _ensure_top10_contract(headers, keys)
        top_n = max(1, min(int(criteria.get("top_n") or body.get("top_n") or 10), max(1, limit)))
        requested_pages = list(TOP10_ENGINE_DEFAULT_PAGES)
        requested_symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 10, 200))
        for page_name in requested_pages:
            if len(requested_symbols) >= max(limit * 10, 200):
                break
            requested_symbols.extend(await self.get_sheet_symbols(page_name, limit=max(limit * 2, 25), body=body))
        requested_symbols = _normalize_symbol_list(requested_symbols or EMERGENCY_PAGE_SYMBOLS.get("Top_10_Investments", []), limit=max(limit * 10, 200))
        quotes = await self.get_enriched_quotes(requested_symbols, schema=None, page="Top_10_Investments", body=body)
        rows = []
        for q in quotes:
            row = _model_to_dict(q)
            row = _apply_page_row_backfill("Top_10_Investments", row)
            _try_scoring_module(row)
            _compute_recommendation(row)
            rows.append(row)
        _apply_rank_overall(rows)
        rows.sort(key=lambda r: (_as_float(r.get("opportunity_score")) or -1e9, _as_float(r.get("overall_score")) or -1e9, _as_float(r.get("confidence_score")) or -1e9), reverse=True)
        selected: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        snapshot = {"top_n": top_n, "pages_selected": requested_pages, "direct_symbols": requested_symbols}
        criteria_snapshot = json.dumps(_json_safe(snapshot), ensure_ascii=False, separators=(",", ":"))
        for row in rows:
            sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("requested_symbol")))
            if not sym or sym in seen:
                continue
            seen.add(sym)
            row["top10_rank"] = len(selected) + 1
            row["selection_reason"] = row.get("selection_reason") or f"Selected by fallback ranking: overall={row.get('overall_score')} confidence={row.get('confidence_score')}"
            row["criteria_snapshot"] = row.get("criteria_snapshot") or criteria_snapshot
            selected.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)))
            if len(selected) >= top_n:
                break
        return out_headers, out_keys, selected

    async def get_sheet_symbols(self, sheet: Optional[str] = None, *, sheet_name: Optional[str] = None, page: Optional[str] = None, limit: int = 5000, body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(page=page, sheet=sheet, sheet_name=sheet_name, limit=limit, offset=0, mode="", body=body, extras=kwargs)
        if target_sheet in SPECIAL_SHEETS:
            self._set_sheet_symbols_meta(target_sheet, "special_sheet", 0)
            return []
        from_body = _extract_requested_symbols_from_body(normalized_body, limit=effective_limit)
        if from_body:
            self._set_sheet_symbols_meta(target_sheet, "body_symbols", len(from_body))
            return from_body
        cached = await self._symbols_cache.get(sheet=target_sheet, limit=effective_limit)
        if isinstance(cached, list) and cached:
            syms = _normalize_symbol_list(cached, limit=effective_limit)
            self._set_sheet_symbols_meta(target_sheet, "symbols_cache", len(syms))
            return syms
        specific = PAGE_SYMBOL_ENV_KEYS.get(target_sheet)
        if specific:
            raw = os.getenv(specific, "") or ""
            if raw.strip():
                syms = _normalize_symbol_list(_split_symbols(raw), limit=effective_limit)
                self._set_sheet_symbols_meta(target_sheet, "env", len(syms))
                await self._symbols_cache.set(syms, sheet=target_sheet, limit=effective_limit)
                return syms
        snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
        snap_rows = _coerce_rows_list(snap)
        if snap_rows:
            syms = _extract_symbols_from_rows(snap_rows, limit=effective_limit)
            if syms:
                self._set_sheet_symbols_meta(target_sheet, "snapshot_rows", len(syms))
                await self._symbols_cache.set(syms, sheet=target_sheet, limit=effective_limit)
                return syms
        emergency = EMERGENCY_PAGE_SYMBOLS.get(target_sheet) or []
        syms = _normalize_symbol_list(emergency, limit=effective_limit)
        self._set_sheet_symbols_meta(target_sheet, "emergency_page_symbols", len(syms), note="last_resort_fallback")
        await self._symbols_cache.set(syms, sheet=target_sheet, limit=effective_limit)
        return syms

    async def get_page_symbols(self, page: Optional[str] = None, *, sheet: Optional[str] = None, sheet_name: Optional[str] = None, limit: int = 5000, body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> List[str]:
        return await self.get_sheet_symbols(sheet=sheet, sheet_name=sheet_name, page=page, limit=limit, body=body, **kwargs)

    async def list_symbols_for_page(self, page: str, *, limit: int = 5000, body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> List[str]:
        return await self.get_page_symbols(page=page, limit=limit, body=body, **kwargs)

    async def get_page_rows(self, page: Optional[str] = None, *, sheet: Optional[str] = None, sheet_name: Optional[str] = None, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(page or sheet or sheet_name, limit=limit, offset=offset, mode=mode, body=body, page=page, sheet=sheet, sheet_name=sheet_name, **kwargs)

    async def get_sheet(self, sheet_name: Optional[str] = None, *, sheet: Optional[str] = None, page: Optional[str] = None, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(sheet_name or sheet or page, limit=limit, offset=offset, mode=mode, body=body, page=page, sheet=sheet, sheet_name=sheet_name, **kwargs)

    async def get_sheet_contract(self, sheet: str) -> Dict[str, Any]:
        _spec, headers, keys, source = _schema_for_sheet(sheet)
        return {"sheet": _canonicalize_sheet_name(sheet), "headers": headers, "keys": keys, "source": source}

    async def get_page_contract(self, page: str) -> Dict[str, Any]:
        return await self.get_sheet_contract(page)

    async def get_sheet_schema(self, sheet: str) -> Dict[str, Any]:
        return await self.get_sheet_contract(sheet)

    async def get_page_schema(self, page: str) -> Dict[str, Any]:
        return await self.get_sheet_contract(page)

    async def get_sheet_rows(self, sheet: Optional[str] = None, *, sheet_name: Optional[str] = None, page: Optional[str] = None, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
        started = time.time()
        target_sheet, limit, offset, mode, body, request_parts = _normalize_route_call_inputs(page=page, sheet=sheet, sheet_name=sheet_name, limit=limit, offset=offset, mode=mode, body=body, extras=kwargs)
        include_matrix = _safe_bool(body.get("include_matrix"), True)
        spec, headers, keys, schema_src = _schema_for_sheet(target_sheet)
        headers, keys = _complete_schema_contract(headers, keys)
        if target_sheet == "Top_10_Investments" and self.top10_force_full_schema:
            headers, keys = _ensure_top10_contract(headers, keys)
        contract_level = "canonical" if _usable_contract(headers, keys, target_sheet) else "partial"
        base_meta = {
            "schema_source": schema_src,
            "contract_level": contract_level,
            "strict_requested": bool(self.schema_strict_sheet_rows),
            "strict_enforced": False,
            "target_sheet_known": target_sheet in STATIC_CANONICAL_SHEET_CONTRACTS,
            "route_input_keys": sorted([str(k) for k in body.keys()]) if isinstance(body, dict) else [],
            "request_input_keys": sorted([str(k) for k in request_parts.keys()]) if isinstance(request_parts, dict) else [],
        }
        if _safe_bool(body.get("schema_only"), False) or _safe_bool(body.get("headers_only"), False):
            payload = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, row_objects=[], include_matrix=include_matrix, status="success", meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode, "built_from": "schema_only_fast_path"})
            _PERF_METRICS.append({"name": "get_sheet_rows", "duration_ms": round((time.time() - started) * 1000.0, 3), "success": True, "sheet": target_sheet})
            return payload
        if target_sheet == "Data_Dictionary":
            rows = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in await self._build_data_dictionary_rows()]
            payload_full = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, row_objects=rows, include_matrix=include_matrix, status="success", meta={**base_meta, "rows": len(rows), "limit": limit, "offset": offset, "mode": mode, "built_from": "internal_data_dictionary"})
            self._store_sheet_snapshot(target_sheet, payload_full)
            out = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, row_objects=rows[offset:offset + limit], include_matrix=include_matrix, status="success", meta={**payload_full.get("meta", {}), "rows": len(rows[offset:offset + limit])})
            _PERF_METRICS.append({"name": "get_sheet_rows", "duration_ms": round((time.time() - started) * 1000.0, 3), "success": True, "sheet": target_sheet})
            return out
        if target_sheet == "Insights_Analysis":
            rows = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in await self._build_insights_rows_fallback(body, limit=max(limit + offset, 10))]
            payload_full = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, row_objects=rows, include_matrix=include_matrix, status="success" if rows else "warn", meta={**base_meta, "rows": len(rows), "limit": limit, "offset": offset, "mode": mode, "built_from": "engine_insights_fallback"})
            self._store_sheet_snapshot(target_sheet, payload_full)
            out = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, row_objects=rows[offset:offset + limit], include_matrix=include_matrix, status=payload_full.get("status", "success"), meta={**payload_full.get("meta", {}), "rows": len(rows[offset:offset + limit])})
            _PERF_METRICS.append({"name": "get_sheet_rows", "duration_ms": round((time.time() - started) * 1000.0, 3), "success": True, "sheet": target_sheet})
            return out
        if target_sheet == "Top_10_Investments":
            out_headers, out_keys, rows = await self._build_top10_rows_fallback(headers, keys, body, limit=max(limit + offset, 10), mode=mode)
            payload_full = self._finalize_payload(sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows, include_matrix=include_matrix, status="success" if rows else "warn", meta={**base_meta, "rows": len(rows), "limit": limit, "offset": offset, "mode": mode, "built_from": "top10_fallback_ranker"})
            self._store_sheet_snapshot(target_sheet, payload_full)
            out = self._finalize_payload(sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows[offset:offset + limit], include_matrix=include_matrix, status=payload_full.get("status", "warn"), meta={**payload_full.get("meta", {}), "rows": len(rows[offset:offset + limit])})
            _PERF_METRICS.append({"name": "get_sheet_rows", "duration_ms": round((time.time() - started) * 1000.0, 3), "success": True, "sheet": target_sheet})
            return out
        requested_symbols = _extract_requested_symbols_from_body(body, limit=limit + offset)
        built_from = "body_symbols" if requested_symbols else "auto_sheet_symbols"
        if not requested_symbols and target_sheet in INSTRUMENT_SHEETS:
            requested_symbols = await self.get_sheet_symbols(target_sheet, limit=limit + offset, body=body)
        rows_full: List[Dict[str, Any]] = []
        if requested_symbols:
            quotes = await self.get_enriched_quotes(requested_symbols, schema=None, page=target_sheet, body=body)
            for q in quotes:
                row = _model_to_dict(q)
                row = _apply_page_row_backfill(target_sheet, row)
                _try_scoring_module(row)
                _compute_recommendation(row)
                rows_full.append(_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, row)))
            _apply_rank_overall(rows_full)
        payload_full = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_full, include_matrix=include_matrix, status="success" if rows_full or target_sheet in STATIC_CANONICAL_SHEET_CONTRACTS else "warn", meta={**base_meta, "rows": len(rows_full), "limit": limit, "offset": offset, "mode": mode, "built_from": built_from, "resolved_symbols_count": len(requested_symbols), "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)})
        if rows_full:
            self._store_sheet_snapshot(target_sheet, payload_full)
        out = self._finalize_payload(sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_full[offset:offset + limit], include_matrix=include_matrix, status=payload_full.get("status", "success"), meta={**payload_full.get("meta", {}), "rows": len(rows_full[offset:offset + limit])})
        _PERF_METRICS.append({"name": "get_sheet_rows", "duration_ms": round((time.time() - started) * 1000.0, 3), "success": True, "sheet": target_sheet})
        return out

    # compatibility aliases
    async def execute_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def run_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_analysis_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def run_analysis_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def get_rows_for_sheet(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def get_rows_for_page(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_page_rows(*args, **kwargs)

    async def get_analysis_rows_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None) -> Dict[str, Dict[str, Any]]:
        return await self.get_enriched_quotes_batch(symbols, mode=mode, schema=schema)

    async def get_analysis_row_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> Dict[str, Any]:
        return await self.get_enriched_quote_dict(symbol, use_cache=use_cache, schema=schema)


# =============================================================================
# Module-level helpers / wrappers
# =============================================================================
def normalize_row_to_schema(sheet: str, row: Dict[str, Any], keep_extras: bool = False) -> Dict[str, Any]:
    target = _canonicalize_sheet_name(sheet) or sheet or "Market_Leaders"
    _spec, headers, keys, _src = _schema_for_sheet(target)
    if target == "Top_10_Investments":
        headers, keys = _ensure_top10_contract(headers, keys)
    normalized = _normalize_to_schema_keys(keys, headers, dict(row or {}))
    normalized = _apply_page_row_backfill(target, normalized)
    if keep_extras and isinstance(row, dict):
        for k, v in row.items():
            if k not in normalized:
                normalized[k] = _json_safe(v)
    return normalized


_ENGINE_INSTANCE: Optional[DataEngineV5] = None
ENGINE: Optional[DataEngineV5] = None
engine: Optional[DataEngineV5] = None
_ENGINE: Optional[DataEngineV5] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine() -> DataEngineV5:
    global _ENGINE_INSTANCE, ENGINE, engine, _ENGINE
    if _ENGINE_INSTANCE is None:
        async with _ENGINE_LOCK:
            if _ENGINE_INSTANCE is None:
                _ENGINE_INSTANCE = DataEngineV5()
    ENGINE = _ENGINE_INSTANCE
    engine = _ENGINE_INSTANCE
    _ENGINE = _ENGINE_INSTANCE
    return _ENGINE_INSTANCE


async def close_engine() -> None:
    global _ENGINE_INSTANCE, ENGINE, engine, _ENGINE
    if _ENGINE_INSTANCE is not None:
        await _ENGINE_INSTANCE.aclose()
    _ENGINE_INSTANCE = None
    ENGINE = None
    engine = None
    _ENGINE = None


def get_engine_if_ready() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def peek_engine() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def get_cache() -> Any:
    return getattr(_ENGINE_INSTANCE, "_cache", None)


def _run_coro_sync(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
        raise RuntimeError("Synchronous engine helper cannot run inside an active event loop")
    except RuntimeError as exc:
        if "active event loop" in str(exc):
            raise
    return asyncio.run(coro)


def get_engine_sync() -> DataEngineV5:
    return _run_coro_sync(get_engine())


async def get_quote(symbol: str, use_cache: bool = True, **kwargs: Any) -> Any:
    eng = await get_engine()
    return await eng.get_enriched_quote(symbol, use_cache=use_cache, **kwargs)


async def get_quotes(symbols: List[str], use_cache: bool = True, **kwargs: Any) -> List[Any]:
    eng = await get_engine()
    return await eng.get_enriched_quotes(symbols, **kwargs)


async def get_enriched_quote(symbol: str, use_cache: bool = True, **kwargs: Any) -> Any:
    return await get_quote(symbol, use_cache=use_cache, **kwargs)


async def get_enriched_quotes(symbols: List[str], use_cache: bool = True, **kwargs: Any) -> List[Any]:
    return await get_quotes(symbols, use_cache=use_cache, **kwargs)


async def get_sheet_rows(sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
    eng = await get_engine()
    return await eng.get_sheet_rows(sheet, limit=limit, offset=offset, mode=mode, body=body, **kwargs)


def get_sheet_rows_sync(sheet: str, limit: int = 2000, offset: int = 0, mode: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
    return _run_coro_sync(get_sheet_rows(sheet, limit=limit, offset=offset, mode=mode, body=body, **kwargs))


async def process_batch(symbols: List[str], use_cache: bool = True, progress_callback: Optional[Any] = None, batch_size: int = 25, delay_seconds: float = 0.0, max_retries: int = 0, **kwargs: Any) -> List[Any]:
    clean = _normalize_symbol_list(symbols, limit=len(symbols) + 10)
    progress = BatchProgress(total=len(clean))
    out: List[Any] = []
    for i in range(0, len(clean), max(1, batch_size)):
        part = clean[i:i + max(1, batch_size)]
        try:
            results = await get_enriched_quotes(part, use_cache=use_cache, **kwargs)
            out.extend(results)
            progress.completed += len(part)
            progress.succeeded += len(part)
        except Exception as exc:
            for sym in part:
                out.append(StubUnifiedQuote(symbol=sym, error=str(exc)).finalize())
                progress.completed += 1
                progress.failed += 1
                progress.errors.append((sym, str(exc)))
        if progress_callback is not None:
            try:
                progress_callback(progress)
            except Exception:
                pass
        if i + batch_size < len(clean) and delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
    return out


async def health_check() -> Dict[str, Any]:
    health: Dict[str, Any] = {
        "status": "healthy",
        "version": __version__,
        "timestamp": _now_utc_iso(),
        "checks": {},
        "warnings": [],
        "errors": [],
    }
    try:
        eng = await get_engine()
        health["checks"]["engine"] = "ready"
        health["checks"]["cache"] = bool(get_cache() is not None)
        health["checks"]["schema_available"] = True
        try:
            sr = await get_sheet_rows("Data_Dictionary", limit=1, offset=0, mode="", body={"include_matrix": False})
            health["checks"]["sheet_rows_test"] = "passed" if isinstance(sr, dict) else "failed"
        except Exception as exc:
            health["checks"]["sheet_rows_test"] = "failed"
            health["warnings"].append(f"sheet_rows_test error: {exc}")
        try:
            q = await get_enriched_quote("AAPL", use_cache=False)
            qd = _model_to_dict(q)
            health["checks"]["quote_test"] = "passed" if qd is not None else "failed"
        except Exception as exc:
            health["checks"]["quote_test"] = "failed"
            health["warnings"].append(f"quote_test error: {exc}")
    except Exception as exc:
        health["status"] = "unhealthy"
        health["errors"].append(f"Health check failed: {exc}")
    return health


@asynccontextmanager
async def engine_context() -> AsyncGenerator[Any, None]:
    eng = await get_engine()
    try:
        yield eng
    finally:
        await close_engine()


class EngineSession:
    def __enter__(self) -> Any:
        self._engine = get_engine_sync()
        return self._engine

    def __exit__(self, *args: Any) -> None:
        _run_coro_sync(close_engine())


def get_engine_meta() -> Dict[str, Any]:
    inst = _ENGINE_INSTANCE
    return {
        "version": __version__,
        "engine_ready": inst is not None,
        "engine_class": type(inst).__name__ if inst is not None else None,
        "primary_provider": getattr(inst, "primary_provider", None),
        "enabled_providers": list(getattr(inst, "enabled_providers", [])) if inst is not None else _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS),
        "ksa_providers": list(getattr(inst, "ksa_providers", [])) if inst is not None else _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS),
        "global_providers": list(getattr(inst, "global_providers", [])) if inst is not None else _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS),
        "schema_strict_sheet_rows": bool(getattr(inst, "schema_strict_sheet_rows", _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True))) if inst is not None else _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True),
        "top10_force_full_schema": bool(getattr(inst, "top10_force_full_schema", _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True))) if inst is not None else _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True),
        "perf_stats": get_perf_stats(),
    }


DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5
DataEngine = DataEngineV5

__all__ = [
    "DataEngineV5", "DataEngineV4", "DataEngineV3", "DataEngineV2", "DataEngine",
    "ENGINE", "engine", "_ENGINE",
    "get_engine", "get_engine_if_ready", "peek_engine", "close_engine", "get_cache",
    "QuoteQuality", "DataSource", "UnifiedQuote", "StubUnifiedQuote", "SymbolInfo", "BatchProgress", "PerfMetrics",
    "normalize_symbol", "get_symbol_info", "get_engine_sync",
    "get_quote", "get_quotes", "get_enriched_quote", "get_enriched_quotes",
    "get_sheet_rows", "get_sheet_rows_sync", "process_batch", "health_check",
    "engine_context", "EngineSession", "DistributedCache", "DynamicCircuitBreaker", "TokenBucket",
    "get_perf_metrics", "get_perf_stats", "reset_perf_metrics", "get_engine_meta", "MetricsRegistry", "_METRICS",
    "normalize_row_to_schema",
]

try:
    _METRICS.set("active_requests", 0)
except Exception:
    pass
