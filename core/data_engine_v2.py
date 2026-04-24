#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 — v6.1.2 (SCHEMA-ALIGNED / REGISTRY-FIRST / CANONICAL)
================================================================================
Tadawul Fast Bridge (TFB)

v6.1.2 — Respect scoring.py's intentional None for valuation_score
------------------------------------------------------------------
Paired with scoring.py v4.1.2.

scoring.py v4.1.2 changed `compute_valuation_score` to return None
(instead of a misleading near-zero number) when no forward-looking
signal is available — intrinsic_value/target_price/forecast_price_*/
expected_roi_* all missing from the provider row. Downstream, the
scorer's `compute_scores` already handles None components gracefully
by skipping them and renormalizing weights across the remaining
components.

Problem this created in v6.1.1 (without this fix):
    v6.1.1 listed `valuation_score` in `_SCORING_CRITICAL_FIELDS`.
    After scoring.py v4.1.2 the scorer legitimately returns None
    for that field on sparse-data rows — which triggered v6.1.1's
    completeness fallback. The fallback then wrote:
      * valuation_score = 50.0  (phantom neutral placeholder)
      * value_score     = ~55   (PE-only guess)
      * forecast_price_1m = price × 1.01
      * forecast_price_3m = price × 1.03
      * forecast_price_12m = price × 1.08
      * expected_roi_1m/3m/12m = 1%, 3%, 8%  (arbitrary)
    i.e. it subverted the scoring.py v4.1.2 change and polluted
    the sheet with phantom forecasts on any row where the provider
    hadn't returned forward valuation data.

Fix (one-line constant change):
    Remove `valuation_score` from `_SCORING_CRITICAL_FIELDS`. The
    scorer now has a legitimate reason to return None for this
    field, and the fallback must not override that signal. The
    fallback is still triggered when the scorer genuinely failed
    (detected via overall_score / recommendation / confidence_score /
    risk_score being None) — those remaining four critical fields
    are enough to catch a real scorer failure, because scoring.py
    always populates them when it runs successfully.

Net effect: rows with sparse provider data (like AAPL today:
intrinsic/target/forecast/ROI all null from providers) now display
blank Valuation Score / Value Score / Forecast Price / Expected ROI
cells instead of misleading phantoms. Overall Score and
Recommendation remain correct and are driven by the other
scoring components the scorer COULD compute.

No other behaviour changes. All v6.1.1 / v6.1.0 guarantees preserved.

v6.1.1 — Scoring completeness fix (preserved)
---------------------------------------------
Root cause of the "scoring columns partially blank" sheet symptom:

    v6.1.0's `_try_scoring_module` returned early as soon as the scorer
    returned a dict, even when critical fields (overall_score,
    confidence_score, recommendation) came back as None. That caused
    rows to show Risk Score / Valuation Score populated but Overall
    Score / Recommendation blank.

Fix (surgical — one function + one constant):

  * Added `_SCORING_CRITICAL_FIELDS`: the fields that MUST be populated
    for the sheet to look healthy. v6.1.2 trimmed this to four fields
    (overall_score, confidence_score, recommendation, risk_score) —
    see the v6.1.2 note above for why valuation_score was removed.
  * `_try_scoring_module` now merges the scorer's non-None values as
    before, then runs the non-destructive `_compute_scores_fallback`
    when any critical field is still None.
  * The fallback itself is unchanged — each branch gates on
    `row.get(field) is None` so it cannot overwrite good values the
    scorer produced.

Net effect: rows that previously landed with partial scoring now get
topped up. Rows that score cleanly are unaffected.

No other behaviour changes in that release. All v6.1.0 alignment
guarantees (schema_registry-first, 80-column instrument contract,
83-column Top_10, 7-column Insights, 9-column Dictionary) are
preserved exactly.

v6.1.0 — Alignment rewrite (preserved)
--------------------------------------
- core.sheets.schema_registry      → single source of truth for columns
- core.sheets.page_catalog         → single source of truth for page names
- core.symbols_reader              → single source of truth for sheet symbols
- core.scoring                     → single source of truth for scoring

Key guarantees from v6.1.0 (unchanged):
1. Schema contracts are derived from schema_registry at import time
2. Top_10_Investments is exactly 80 + 3 = 83 columns
3. Insights_Analysis is exactly 7 columns
4. Data_Dictionary stays at 9 columns
5. My_Investments aliases to My_Portfolio
6. Sheet name canonicalization delegates to page_catalog
7. Symbol resolution delegates to core.symbols_reader
8. Scoring patches are always merged
9. Provider callable discovery covers *_patch variants
10. Public API surface is preserved
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
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

# ---------------------------------------------------------------------------
# Path Setup
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

__version__ = "6.1.2"
VERSION = __version__

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Environment / conversion helpers
# ---------------------------------------------------------------------------


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        s = str(value).strip()
        return s if s else default
    except Exception:
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    s = _safe_str(value).lower()
    if s in {"1", "true", "yes", "y", "on", "t"}:
        return True
    if s in {"0", "false", "no", "n", "off", "f"}:
        return False
    return default


def _safe_int(value: Any, default: int = 0, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        v = int(float(value))
    except Exception:
        v = default
    if lo is not None:
        v = max(lo, v)
    if hi is not None:
        v = min(hi, v)
    return v


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _get_env_bool(name: str, default: bool = False) -> bool:
    return _safe_bool(os.getenv(name), default)


def _get_env_int(name: str, default: int) -> int:
    return _safe_int(os.getenv(name), default)


def _get_env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _get_env_list(name: str, default: Sequence[str]) -> List[str]:
    raw = _safe_str(os.getenv(name), "")
    if not raw:
        return [str(x).lower() for x in default]
    return [p.strip().lower() for p in re.split(r"[,;|\s]+", raw) if p.strip()]


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

try:
    from zoneinfo import ZoneInfo

    _HAS_ZONEINFO = True
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore
    _HAS_ZONEINFO = False


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    try:
        if _HAS_ZONEINFO and ZoneInfo is not None:
            return datetime.now(ZoneInfo("Asia/Riyadh")).isoformat()
    except Exception:
        pass
    return _now_utc_iso()


# ---------------------------------------------------------------------------
# String normalization helpers
# ---------------------------------------------------------------------------


def _norm_key(value: Any) -> str:
    s = _safe_str(value).lower()
    s = s.replace("-", "_").replace("/", "_").replace("&", "_")
    s = re.sub(r"\s+", "_", s)
    return re.sub(r"__+", "_", s).strip("_")


def _norm_key_loose(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _safe_str(value).lower())


def _deduplicate_keep_order(items: Sequence[Any]) -> List[Any]:
    seen: Set[Any] = set()
    result: List[Any] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, Decimal):
        return _safe_float(value)
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
        if hasattr(value, "model_dump") and callable(value.model_dump):
            return _json_safe(value.model_dump(mode="python"))
    except Exception:
        pass
    try:
        if hasattr(value, "dict") and callable(value.dict):
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
        if hasattr(obj, "model_dump") and callable(obj.model_dump):
            d = obj.model_dump(mode="python")
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    try:
        if hasattr(obj, "dict") and callable(obj.dict):
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


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class QuoteQuality(str, Enum):
    GOOD = "good"
    FAIR = "fair"
    MISSING = "missing"


class DataSource(str, Enum):
    ENGINE_V2 = "engine_v2"
    EXTERNAL_ROWS = "external_rows"
    SNAPSHOT = "snapshot"
    FALLBACK = "fallback"


# ---------------------------------------------------------------------------
# Pydantic compatibility
# ---------------------------------------------------------------------------

try:
    from pydantic import BaseModel, ConfigDict

    _PYDANTIC_AVAILABLE = True
except ImportError:  # pragma: no cover

    class BaseModel:  # type: ignore
        def __init__(self, **data: Any) -> None:
            self.__dict__.update(data)

        def model_dump(self, mode: str = "python") -> Dict[str, Any]:
            return dict(self.__dict__)

        def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            return dict(self.__dict__)

    def ConfigDict(**kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return dict(kwargs)

    _PYDANTIC_AVAILABLE = False


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class UnifiedQuote(BaseModel):
    """Unified quote model (schema-tolerant)."""

    model_config = ConfigDict(extra="allow")


class StubUnifiedQuote(BaseModel):
    """Stub quote model for fallback."""

    model_config = ConfigDict(extra="allow")

    def finalize(self) -> "StubUnifiedQuote":
        d = _model_to_dict(self)

        if d.get("current_price") is None and d.get("price") is not None:
            d["current_price"] = d["price"]
        if d.get("price") is None and d.get("current_price") is not None:
            d["price"] = d["current_price"]
        if d.get("price_change") is None and d.get("change") is not None:
            d["price_change"] = d["change"]
        if d.get("change") is None and d.get("price_change") is not None:
            d["change"] = d["price_change"]
        if d.get("percent_change") is None and d.get("change_pct") is not None:
            d["percent_change"] = d["change_pct"]
        if d.get("change_pct") is None and d.get("percent_change") is not None:
            d["change_pct"] = d["percent_change"]

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


# ---------------------------------------------------------------------------
# Metrics (no-op placeholders kept for public API compat)
# ---------------------------------------------------------------------------


class _NoopMetrics:
    def set(self, *args: Any, **kwargs: Any) -> None:
        pass

    def inc(self, *args: Any, **kwargs: Any) -> None:
        pass

    def observe(self, *args: Any, **kwargs: Any) -> None:
        pass


_METRICS = _NoopMetrics()
_PERF_METRICS: List[Dict[str, Any]] = []


def get_perf_metrics() -> List[Dict[str, Any]]:
    return list(_PERF_METRICS)


def get_perf_stats() -> Dict[str, Any]:
    if not _PERF_METRICS:
        return {"count": 0}
    durations: List[float] = []
    for x in _PERF_METRICS:
        try:
            d = float(x.get("duration_ms", 0.0))
        except Exception:
            continue
        if not (math.isnan(d) or math.isinf(d)):
            durations.append(d)
    if not durations:
        return {"count": 0}
    return {
        "count": len(durations),
        "avg_duration_ms": round(sum(durations) / len(durations), 3),
        "max_duration_ms": round(max(durations), 3),
    }


def reset_perf_metrics() -> None:
    _PERF_METRICS.clear()


class MetricsRegistry:
    """Metrics registry stub (public API compat)."""

    pass


class DistributedCache:
    """Distributed cache stub (public API compat)."""

    pass


class DynamicCircuitBreaker:
    """Dynamic circuit breaker stub (public API compat)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


class TokenBucket:
    """Token bucket stub (public API compat)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


# ===========================================================================
# CANONICAL SCHEMA (aligned with core.sheets.schema_registry v2.2.0)
# ===========================================================================
#
# schema_registry is the authoritative source for sheet contracts. We import
# from it when available; otherwise we use a hardcoded fallback that matches
# the registry *exactly*. Never diverge these lists from schema_registry.py.
# ---------------------------------------------------------------------------

# Canonical 80 instrument columns — aligned with
# core.sheets.schema_registry._canonical_instrument_columns()
_FALLBACK_INSTRUMENT_KEYS: List[str] = [
    # Identity (8)
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    # Price (10) -> 18
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    # Liquidity (6) -> 24
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    # Fundamentals (12) -> 36
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio", "revenue_ttm",
    "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "free_cash_flow_ttm",
    # Risk (8) -> 44
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    # Valuation (6) -> 50
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value", "valuation_score",
    # Forecast (9) -> 59
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    # Scores (7) -> 66
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    # Recommendation (4) -> 70
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    # Portfolio (6) -> 76
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    # Provenance (4) -> 80
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
]

_FALLBACK_INSTRUMENT_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Percent Change", "52W Position %",
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio",
    "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin", "Operating Margin",
    "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)",
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y",
    "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Valuation Score",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    "Position Qty", "Avg Cost", "Position Cost", "Position Value",
    "Unrealized P/L", "Unrealized P/L %",
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
]

# Top10 extras (3) — appended to canonical 80 → 83
_FALLBACK_TOP10_EXTRA_KEYS: List[str] = ["top10_rank", "selection_reason", "criteria_snapshot"]
_FALLBACK_TOP10_EXTRA_HEADERS: List[str] = ["Top10 Rank", "Selection Reason", "Criteria Snapshot"]

# Insights (7)
_FALLBACK_INSIGHTS_KEYS: List[str] = [
    "section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh",
]
_FALLBACK_INSIGHTS_HEADERS: List[str] = [
    "Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)",
]

# Data dictionary (9)
_FALLBACK_DICT_KEYS: List[str] = [
    "sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes",
]
_FALLBACK_DICT_HEADERS: List[str] = [
    "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes",
]

# Required field sets for validation
TOP10_REQUIRED_FIELDS: Tuple[str, ...] = ("top10_rank", "selection_reason", "criteria_snapshot")
TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

# ---------------------------------------------------------------------------
# v6.1.2 — Scoring completeness fields (updated from v6.1.1)
# ---------------------------------------------------------------------------
# These are the fields that MUST be populated for a row to look healthy in
# the sheet. If the scorer returns a dict but leaves any of these as None,
# we run the fallback to top them up.
#
# v6.1.2 note: `valuation_score` was REMOVED from this list. scoring.py
# v4.1.2 legitimately returns None for valuation_score when no
# forward-looking signal is available — we must not override that with
# a phantom fallback value. The remaining four fields are still enough
# to detect real scorer failures because scoring.py always populates
# them (overall_score/recommendation default to 50.0/"HOLD" even when
# inputs are insufficient; confidence_score is derived from data
# quality which is always computable; risk_score only returns None
# when zero risk signals are available, which is rare).
_SCORING_CRITICAL_FIELDS: Tuple[str, ...] = (
    "overall_score",
    "confidence_score",
    "recommendation",
    "risk_score",
)

# Public exports (derived at module load — see _load_canonical_from_registry below)
INSTRUMENT_CANONICAL_KEYS: List[str] = list(_FALLBACK_INSTRUMENT_KEYS)
INSTRUMENT_CANONICAL_HEADERS: List[str] = list(_FALLBACK_INSTRUMENT_HEADERS)
INSIGHTS_KEYS: List[str] = list(_FALLBACK_INSIGHTS_KEYS)
INSIGHTS_HEADERS: List[str] = list(_FALLBACK_INSIGHTS_HEADERS)
DATA_DICTIONARY_KEYS: List[str] = list(_FALLBACK_DICT_KEYS)
DATA_DICTIONARY_HEADERS: List[str] = list(_FALLBACK_DICT_HEADERS)


# ---------------------------------------------------------------------------
# Schema registry integration (authoritative import)
# ---------------------------------------------------------------------------

try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY as _RAW_SCHEMA_REGISTRY  # type: ignore
except Exception:  # pragma: no cover
    _RAW_SCHEMA_REGISTRY = None

try:
    from core.sheets.schema_registry import get_sheet_spec as _RAW_GET_SHEET_SPEC  # type: ignore
except Exception:  # pragma: no cover
    _RAW_GET_SHEET_SPEC = None

try:
    from core.sheets.schema_registry import normalize_sheet_name as _RAW_NORMALIZE_SHEET  # type: ignore
except Exception:  # pragma: no cover
    _RAW_NORMALIZE_SHEET = None

SCHEMA_REGISTRY: Dict[str, Any] = (
    dict(_RAW_SCHEMA_REGISTRY) if isinstance(_RAW_SCHEMA_REGISTRY, dict) else {}
)


def _schema_columns_from_any(spec: Any) -> List[Any]:
    """Extract ordered column specs from a SheetSpec or dict-like spec."""
    if spec is None:
        return []

    if isinstance(spec, dict) and len(spec) == 1 and "columns" not in spec and "fields" not in spec:
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict) and ("columns" in first_val or "fields" in first_val):
            spec = first_val

    cols = getattr(spec, "columns", None)
    if isinstance(cols, (list, tuple)) and cols:
        return list(cols)

    if isinstance(spec, Mapping):
        cols2 = spec.get("columns") or spec.get("fields")
        if isinstance(cols2, (list, tuple)) and cols2:
            return list(cols2)

    return []


def _schema_keys_headers_from_spec(spec: Any) -> Tuple[List[str], List[str]]:
    """Extract headers+keys from a SheetSpec/dict, preserving order."""
    cols = _schema_columns_from_any(spec)
    headers: List[str] = []
    keys: List[str] = []

    for col in cols:
        if isinstance(col, Mapping):
            h = _safe_str(col.get("header") or col.get("display_header") or col.get("label") or col.get("title"))
            k = _safe_str(col.get("key") or col.get("field") or col.get("name") or col.get("id"))
        else:
            h = _safe_str(getattr(col, "header",
                                  getattr(col, "display_header",
                                          getattr(col, "label",
                                                  getattr(col, "title", None)))))
            k = _safe_str(getattr(col, "key",
                                  getattr(col, "field",
                                          getattr(col, "name",
                                                  getattr(col, "id", None)))))
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


def _load_canonical_from_registry() -> None:
    """
    Pull canonical contracts from schema_registry into module-level lists.
    Falls back to the hardcoded defaults if the registry is unavailable.
    """
    global INSTRUMENT_CANONICAL_KEYS, INSTRUMENT_CANONICAL_HEADERS
    global INSIGHTS_KEYS, INSIGHTS_HEADERS
    global DATA_DICTIONARY_KEYS, DATA_DICTIONARY_HEADERS

    if not SCHEMA_REGISTRY:
        return

    # Market_Leaders is the canonical instrument contract in the registry.
    try:
        ml_spec = SCHEMA_REGISTRY.get("Market_Leaders")
        if ml_spec is not None:
            hdrs, keys = _schema_keys_headers_from_spec(ml_spec)
            if hdrs and keys and len(hdrs) == len(keys):
                INSTRUMENT_CANONICAL_HEADERS = list(hdrs)
                INSTRUMENT_CANONICAL_KEYS = list(keys)
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to load Market_Leaders schema from registry: %s", exc)

    try:
        ins_spec = SCHEMA_REGISTRY.get("Insights_Analysis")
        if ins_spec is not None:
            hdrs, keys = _schema_keys_headers_from_spec(ins_spec)
            if hdrs and keys and len(hdrs) == len(keys):
                INSIGHTS_HEADERS = list(hdrs)
                INSIGHTS_KEYS = list(keys)
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to load Insights_Analysis schema from registry: %s", exc)

    try:
        dd_spec = SCHEMA_REGISTRY.get("Data_Dictionary")
        if dd_spec is not None:
            hdrs, keys = _schema_keys_headers_from_spec(dd_spec)
            if hdrs and keys and len(hdrs) == len(keys):
                DATA_DICTIONARY_HEADERS = list(hdrs)
                DATA_DICTIONARY_KEYS = list(keys)
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to load Data_Dictionary schema from registry: %s", exc)


# ---------------------------------------------------------------------------
# Schema contract helpers
# ---------------------------------------------------------------------------


def _complete_schema_contract(
    headers: Sequence[str],
    keys: Sequence[str],
) -> Tuple[List[str], List[str]]:
    """Complete schema contract so headers and keys are aligned and non-empty."""
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


def _ensure_top10_contract(
    headers: Sequence[str],
    keys: Sequence[str],
) -> Tuple[List[str], List[str]]:
    """Ensure Top_10 contract has the 3 required trailing fields."""
    hdrs = list(headers or [])
    ks = list(keys or [])

    for f in TOP10_REQUIRED_FIELDS:
        if f not in ks:
            ks.append(f)
            hdrs.append(TOP10_REQUIRED_HEADERS[f])

    return _complete_schema_contract(hdrs, ks)


# Initialize canonical exports from registry (if available)
_load_canonical_from_registry()


# ---------------------------------------------------------------------------
# Static canonical sheet contracts (built from registry-aligned lists)
# ---------------------------------------------------------------------------

# Top_10 canonical = canonical 80 + 3 extras = 83
_TOP10_KEYS = list(INSTRUMENT_CANONICAL_KEYS) + list(_FALLBACK_TOP10_EXTRA_KEYS)
_TOP10_HEADERS = list(INSTRUMENT_CANONICAL_HEADERS) + list(_FALLBACK_TOP10_EXTRA_HEADERS)

STATIC_CANONICAL_SHEET_CONTRACTS: Dict[str, Dict[str, List[str]]] = {
    # All instrument sheets share the canonical 80-column contract
    "Market_Leaders":     {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Global_Markets":     {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Commodities_FX":     {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Mutual_Funds":       {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "My_Portfolio":       {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    # My_Investments is not in the registry; alias it to My_Portfolio's shape
    "My_Investments":     {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    # Top_10 = 80 + 3 = 83
    "Top_10_Investments": {"headers": _TOP10_HEADERS, "keys": _TOP10_KEYS},
    # Special sheets
    "Insights_Analysis":  {"headers": list(INSIGHTS_HEADERS), "keys": list(INSIGHTS_KEYS)},
    "Data_Dictionary":    {"headers": list(DATA_DICTIONARY_HEADERS), "keys": list(DATA_DICTIONARY_KEYS)},
}

# Canonical instrument pages (input universes)
INSTRUMENT_SHEETS: Set[str] = {
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds",
    "My_Portfolio", "My_Investments", "Top_10_Investments",
}

# Special/output pages
SPECIAL_SHEETS: Set[str] = {"Insights_Analysis", "Data_Dictionary"}

# Default feed pages for Top_10 selection (matches page_catalog.TOP10_FEED_PAGES_DEFAULT)
TOP10_ENGINE_DEFAULT_PAGES: List[str] = [
    "Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds", "My_Portfolio",
]

# Emergency fallback symbols per page (used only if all other sources fail)
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

# Provider defaults
DEFAULT_PROVIDERS = ["eodhd", "yahoo", "finnhub"]
DEFAULT_KSA_PROVIDERS = ["tadawul", "argaam", "yahoo"]
DEFAULT_GLOBAL_PROVIDERS = ["eodhd", "yahoo", "finnhub"]

NON_KSA_EODHD_PRIMARY_PAGES = {"Global_Markets", "Commodities_FX", "Mutual_Funds"}

PROVIDER_PRIORITIES: Dict[str, int] = {
    "tadawul": 10,
    "argaam": 20,
    "eodhd": 30,
    "yahoo": 40,
    "yahoo_fundamentals": 45,
    "finnhub": 50,
    "yahoo_chart": 60,
}


# ---------------------------------------------------------------------------
# Page catalog integration (canonical page resolution)
# ---------------------------------------------------------------------------

_PAGE_CATALOG_MOD: Any = None
_PAGE_CATALOG_TRIED = False


def _page_catalog_module() -> Any:
    global _PAGE_CATALOG_MOD, _PAGE_CATALOG_TRIED
    if _PAGE_CATALOG_TRIED:
        return _PAGE_CATALOG_MOD
    _PAGE_CATALOG_TRIED = True
    for mod_path in ("core.sheets.page_catalog", "sheets.page_catalog"):
        try:
            _PAGE_CATALOG_MOD = import_module(mod_path)
            return _PAGE_CATALOG_MOD
        except ImportError:
            continue
        except Exception:  # pragma: no cover
            continue
    return None


def _page_catalog_canonical_name(name: str) -> str:
    """Resolve a name through page_catalog. Returns '' if unknown or unavailable."""
    raw = _safe_str(name)
    if not raw:
        return ""

    mod = _page_catalog_module()
    if mod is None:
        return ""

    for fn_name in (
        "normalize_page_name",
        "canonicalize_page",
        "resolve_page",
        "get_canonical_page_name",
    ):
        fn = getattr(mod, fn_name, None)
        if not callable(fn):
            continue
        try:
            val = fn(raw)
        except Exception:
            continue
        txt = _safe_str(val)
        if txt:
            return txt

    return ""


def _canonicalize_sheet_name(name: str) -> str:
    """
    Canonicalize a sheet/page name.

    Resolution order:
    1. Registry's normalize_sheet_name() (if importable)
    2. Exact / normalized match against STATIC_CANONICAL_SHEET_CONTRACTS
    3. page_catalog.normalize_page_name()
    4. Fallback: underscore-normalized raw string
    """
    raw = _safe_str(name)
    if not raw:
        return ""

    if callable(_RAW_NORMALIZE_SHEET):
        try:
            canonical = _safe_str(_RAW_NORMALIZE_SHEET(raw))
            if canonical:
                return canonical
        except Exception:
            pass

    known = {k: k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_norm = {_norm_key(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}
    by_loose = {_norm_key_loose(k): k for k in STATIC_CANONICAL_SHEET_CONTRACTS.keys()}

    for cand in (
        raw,
        raw.replace(" ", "_"),
        raw.replace("-", "_"),
        _norm_key(raw),
        _norm_key_loose(raw),
    ):
        if cand in known:
            return known[cand]
        if _norm_key(cand) in by_norm:
            return by_norm[_norm_key(cand)]
        if _norm_key_loose(cand) in by_loose:
            return by_loose[_norm_key_loose(cand)]

    pc = _page_catalog_canonical_name(raw)
    if pc:
        if pc in STATIC_CANONICAL_SHEET_CONTRACTS:
            return pc
        return pc.replace(" ", "_")

    return raw.replace(" ", "_")


# ---------------------------------------------------------------------------
# Sheet spec lookup (registry-first)
# ---------------------------------------------------------------------------


def get_sheet_spec(sheet: str) -> Any:
    """Get sheet spec from registry (with static fallback)."""
    canon = _canonicalize_sheet_name(sheet)

    if callable(_RAW_GET_SHEET_SPEC):
        for cand in _deduplicate_keep_order([canon, canon.replace("_", " "), _norm_key(canon), sheet]):
            try:
                spec = _RAW_GET_SHEET_SPEC(cand)
                if spec is not None:
                    return spec
            except Exception:
                continue

    if SCHEMA_REGISTRY:
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
    """Return (spec, headers, keys, source) for a sheet, using registry first."""
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


def _usable_contract(
    headers: Sequence[str],
    keys: Sequence[str],
    sheet_name: str = "",
) -> bool:
    """Check that a contract has the minimum fields expected for its sheet type."""
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


# ---------------------------------------------------------------------------
# Symbol helpers
# ---------------------------------------------------------------------------


def normalize_symbol(symbol: str) -> str:
    s = _safe_str(symbol).upper()
    if re.match(r"^[0-9]{4}$", s):
        return f"{s}.SR"
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
        if re.match(r"^[0-9]{4}$", s):
            s = f"{s}.SR"
    return s


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


def get_symbol_info(symbol: str) -> Dict[str, Any]:
    s = normalize_symbol(symbol)
    return {
        "requested": _safe_str(symbol),
        "normalized": s,
        "is_ksa": s.endswith(".SR") or bool(re.match(r"^[0-9]{4}$", s)),
    }


def _split_symbols(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for v in value:
            out.extend(_split_symbols(v))
        return out
    s = _safe_str(value)
    return [p.strip() for p in re.split(r"[,;|\s]+", s) if p.strip()] if s else []


def _extract_requested_symbols_from_body(body: Optional[Dict[str, Any]], limit: int = 5000) -> List[str]:
    if not isinstance(body, dict):
        return []

    raw: List[str] = []
    for key in (
        "symbols", "tickers", "selected_symbols", "selected_tickers", "direct_symbols",
        "codes", "watchlist", "portfolio_symbols", "symbol", "ticker", "code", "requested_symbol",
    ):
        raw.extend(_split_symbols(body.get(key)))

    criteria = body.get("criteria")
    if isinstance(criteria, dict):
        for key in ("symbols", "tickers", "selected_symbols", "direct_symbols", "codes", "symbol", "ticker", "code"):
            raw.extend(_split_symbols(criteria.get(key)))

    return _normalize_symbol_list(raw, limit=limit)


# ---------------------------------------------------------------------------
# Route input normalization
# ---------------------------------------------------------------------------


def _merge_route_body_dicts(*parts: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for part in parts:
        if part is None:
            continue
        if isinstance(part, Mapping):
            for k, v in part.items():
                merged[_safe_str(k)] = v
            continue
        try:
            if hasattr(part, "items") and callable(part.items):
                for k, v in part.items():
                    merged[_safe_str(k)] = v
        except Exception:
            continue
    return {k: v for k, v in merged.items() if k}


def _extract_request_route_parts(request: Any) -> Dict[str, Any]:
    if request is None:
        return {}
    out: Dict[str, Any] = {}
    for attr in ("query_params", "path_params"):
        try:
            part = getattr(request, attr, None)
            if part is not None:
                out.update(_merge_route_body_dicts(part))
        except Exception:
            pass
    return out


def _normalize_route_call_inputs(
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
        extras.get("params"),
        extras.get("query"),
        extras.get("query_params"),
        extras.get("payload"),
        extras.get("data"),
        extras.get("json"),
        extras.get("body"),
        body,
        extras,
    )

    target_raw = (
        page or sheet or sheet_name
        or merged_body.get("page") or merged_body.get("sheet")
        or merged_body.get("sheet_name") or merged_body.get("name") or merged_body.get("tab")
        or "Market_Leaders"
    )
    effective_limit = _safe_int(merged_body.get("limit", limit), default=limit, lo=1, hi=5000)
    effective_offset = _safe_int(merged_body.get("offset", offset), default=offset, lo=0)
    effective_mode = _safe_str(merged_body.get("mode") or mode)

    passthrough = {
        k: v for k, v in merged_body.items()
        if k not in {"request", "params", "query", "query_params", "payload", "data", "json", "body"}
    }

    return (
        _canonicalize_sheet_name(_safe_str(target_raw)) or "Market_Leaders",
        effective_limit,
        effective_offset,
        effective_mode,
        passthrough,
        request_parts,
    )


# ---------------------------------------------------------------------------
# Field aliases and canonicalization
# ---------------------------------------------------------------------------

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
    "current_price": ("current_price", "currentPrice", "price", "last", "lastPrice", "latestPrice",
                      "regularMarketPrice", "nav", "close"),
    "previous_close": ("previous_close", "previousClose", "regularMarketPreviousClose", "prevClose", "priorClose"),
    "open_price": ("open_price", "day_open", "dayOpen", "open", "openPrice", "regularMarketOpen"),
    "day_high": ("day_high", "high", "dayHigh", "regularMarketDayHigh"),
    "day_low": ("day_low", "low", "dayLow", "regularMarketDayLow"),
    "week_52_high": ("week_52_high", "52WeekHigh", "fiftyTwoWeekHigh", "yearHigh"),
    "week_52_low": ("week_52_low", "52WeekLow", "fiftyTwoWeekLow", "yearLow"),
    "price_change": ("price_change", "change", "priceChange", "regularMarketChange"),
    "percent_change": ("percent_change", "changePercent", "percentChange", "regularMarketChangePercent",
                       "pctChange", "change_pct"),
    "volume": ("volume", "regularMarketVolume", "sharesTraded", "tradeVolume", "vol"),
    "avg_volume_10d": ("avg_volume_10d", "avg_vol_10d", "averageVolume10days", "avgVolume10Day"),
    "avg_volume_30d": ("avg_volume_30d", "avg_vol_30d", "averageVolume", "averageDailyVolume3Month",
                       "avgVolume3Month"),
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

# Commodity / ETF hints
_COMMODITY_SYMBOL_HINTS = ("GC=F", "SI=F", "BZ=F", "CL=F", "NG=F", "HG=F")
_ETF_SYMBOL_HINTS = ("SPY", "QQQ", "VTI", "VOO", "IWM", "DIA", "IVV", "EFA", "EEM", "ARKK")
_COMMODITY_DISPLAY_NAMES = {
    "GC=F": "Gold Futures",
    "SI=F": "Silver Futures",
    "BZ=F": "Brent Crude Futures",
    "CL=F": "WTI Crude Futures",
    "NG=F": "Natural Gas Futures",
    "HG=F": "Copper Futures",
}
_ETF_DISPLAY_NAMES = {
    "SPY": "SPDR S&P 500 ETF",
    "QQQ": "Invesco QQQ Trust",
    "VTI": "Vanguard Total Stock Market ETF",
    "VOO": "Vanguard S&P 500 ETF",
    "IWM": "iShares Russell 2000 ETF",
    "DIA": "SPDR Dow Jones Industrial Average ETF",
    "IVV": "iShares Core S&P 500 ETF",
    "EFA": "iShares MSCI EAFE ETF",
    "EEM": "iShares MSCI Emerging Markets ETF",
    "ARKK": "ARK Innovation ETF",
}
_COMMODITY_INDUSTRY_HINTS = {
    "GC=F": "Precious Metals",
    "SI=F": "Precious Metals",
    "HG=F": "Industrial Metals",
    "BZ=F": "Energy",
    "CL=F": "Energy",
    "NG=F": "Energy",
}


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


def _flatten_scalar_fields(
    obj: Any,
    out: Optional[Dict[str, Any]] = None,
    prefix: str = "",
    depth: int = 0,
    max_depth: int = 4,
) -> Dict[str, Any]:
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


def _lookup_alias_value(
    src: Mapping[str, Any],
    flat: Mapping[str, Any],
    alias: str,
) -> Any:
    if not alias:
        return None

    candidates = [
        alias,
        alias.lower(),
        _norm_key(alias),
        _norm_key_loose(alias),
        alias.replace("_", " "),
        alias.replace("_", "-"),
    ]

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


# ---------------------------------------------------------------------------
# Symbol context inference
# ---------------------------------------------------------------------------


def _infer_asset_class_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if not s:
        return ""
    if s.endswith(".SR"):
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
    if s.endswith(".SR"):
        return "Tadawul"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F"):
        return "Futures"
    return "NASDAQ/NYSE"


def _infer_currency_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR"):
        return "SAR"
    if s.endswith("=X"):
        pair = s[:-2]
        return pair[-3:] if len(pair) >= 6 else (pair or "FX")
    return "USD"


def _infer_country_from_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    if s.endswith(".SR"):
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
    if s.endswith(".SR"):
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
    if s.endswith(".SR"):
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


def _apply_symbol_context_defaults(
    row: Dict[str, Any],
    symbol: str = "",
    page: str = "",
) -> Dict[str, Any]:
    out = dict(row or {})
    sym = normalize_symbol(
        symbol or _safe_str(out.get("symbol") or out.get("ticker") or out.get("requested_symbol"))
    )
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


def _canonicalize_provider_row(
    row: Dict[str, Any],
    requested_symbol: str = "",
    normalized_symbol: str = "",
    provider: str = "",
) -> Dict[str, Any]:
    src = dict(row or {})
    flat = _flatten_scalar_fields(src)

    symbol = normalized_symbol or normalize_symbol(
        _safe_str(_lookup_alias_value(src, flat, "symbol") or requested_symbol)
    )

    out: Dict[str, Any] = {
        "symbol": symbol or requested_symbol,
        "symbol_normalized": symbol or requested_symbol,
        "requested_symbol": requested_symbol or symbol,
    }

    for field_name, aliases in _CANONICAL_FIELD_ALIASES.items():
        for alias in (field_name,) + tuple(aliases):
            val = _lookup_alias_value(src, flat, alias)
            if not _is_blank_value(val):
                out[field_name] = _json_safe(_to_scalar(val))
                break

    out = _apply_symbol_context_defaults(out, symbol=symbol)

    if provider and not out.get("data_provider"):
        out["data_provider"] = provider

    out.setdefault("last_updated_utc", _now_utc_iso())
    out.setdefault("last_updated_riyadh", _now_riyadh_iso())

    # Derived fields
    price = _safe_float(out.get("current_price"))
    prev = _safe_float(out.get("previous_close"))

    if out.get("price_change") is None and price is not None and prev is not None:
        out["price_change"] = price - prev
    if out.get("percent_change") is None and price is not None and prev not in (None, 0):
        out["percent_change"] = ((price - prev) / prev) * 100.0

    return out


def _normalize_to_schema_keys(
    keys: Sequence[str],
    headers: Sequence[str],
    row: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Normalize a row to a schema's keys. Preserves unknown scalar fields
    (e.g. section/item for Insights_Analysis, sheet/group for
    Data_Dictionary, top10_rank/selection_reason/criteria_snapshot for
    Top_10_Investments) by searching both the canonicalized provider view
    *and* the original row.
    """
    original = dict(row or {})

    src = _canonicalize_provider_row(
        original,
        requested_symbol=_safe_str(original.get("requested_symbol")),
        normalized_symbol=normalize_symbol(
            _safe_str(original.get("symbol") or original.get("ticker"))
        ),
        provider=_safe_str(original.get("data_provider") or original.get("provider")),
    )

    # Combine original and canonicalized views. Canonical values win when both
    # are present and non-blank (they are cleaner / derived), but unknown
    # fields from the original survive.
    combined: Dict[str, Any] = dict(original)
    for k, v in src.items():
        if not _is_blank_value(v):
            combined[k] = v

    flat = _flatten_scalar_fields(combined)
    out: Dict[str, Any] = {}

    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers) else key
        aliases = [
            key, header, _norm_key(key), _norm_key(header),
            key.lower(), header.lower(), key.replace("_", " "),
        ]
        aliases.extend(_CANONICAL_FIELD_ALIASES.get(key, ()))

        val = None
        found = False

        for alias in aliases:
            val = _lookup_alias_value(combined, flat, alias)
            if not _is_blank_value(val):
                found = True
                break

        out[key] = _json_safe(_to_scalar(val)) if found else None

    return out


# ---------------------------------------------------------------------------
# Row projection helpers
# ---------------------------------------------------------------------------


def _strict_project_row(keys: Sequence[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe(row.get(k)) for k in keys}


def _strict_project_row_display(
    headers: Sequence[str],
    keys: Sequence[str],
    row: Dict[str, Any],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for idx, key in enumerate(keys or []):
        header = headers[idx] if idx < len(headers or []) else key
        out[header] = _json_safe(row.get(key))
    return out


def _rows_display_objects_from_rows(
    rows: List[Dict[str, Any]],
    headers: List[str],
    keys: List[str],
) -> List[Dict[str, Any]]:
    return [_strict_project_row_display(headers, keys, row) for row in rows or []]


def _rows_matrix_from_rows(
    rows: List[Dict[str, Any]],
    keys: List[str],
) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows or []]


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    if out is None:
        return []
    if isinstance(out, list):
        return [dict(r) for r in out if isinstance(r, dict)]
    if isinstance(out, dict):
        for key in ("row_objects", "records", "items", "data", "quotes", "display_row_objects"):
            val = out.get(key)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return [dict(r) for r in val if isinstance(r, dict)]
    return []


def _extract_row_symbol(row: Dict[str, Any]) -> str:
    for key in ("symbol", "ticker", "code", "requested_symbol", "Symbol", "Ticker", "Code"):
        v = row.get(key)
        if v:
            return _safe_str(v)
    return ""


def _extract_symbols_from_rows(rows: Sequence[Dict[str, Any]], limit: int = 5000) -> List[str]:
    raw: List[str] = []
    for row in rows or []:
        if isinstance(row, dict):
            sym = _extract_row_symbol(row)
            if sym:
                raw.append(sym)
    return _normalize_symbol_list(raw, limit=limit)


def _merge_missing_fields(
    base_row: Dict[str, Any],
    template_row: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    out = dict(base_row or {})
    if isinstance(template_row, dict):
        for k, v in template_row.items():
            if out.get(k) in (None, "", [], {}) and v not in (None, "", [], {}):
                out[k] = _json_safe(v)
    return out


def _row_signal_count(row: Dict[str, Any]) -> int:
    score = 0
    for key in (
        "symbol", "name", "current_price", "market_cap", "overall_score",
        "recommendation", "target_price", "intrinsic_value",
    ):
        if row.get(key) not in (None, "", [], {}):
            score += 1
    score += sum(1 for v in row.values() if v not in (None, "", [], {}))
    return score


# ---------------------------------------------------------------------------
# Scoring and recommendation helpers (delegate to core.scoring)
# ---------------------------------------------------------------------------

_SCORING_MOD: Any = None
_SCORING_MOD_TRIED = False


def _get_scoring_mod() -> Any:
    global _SCORING_MOD, _SCORING_MOD_TRIED
    if _SCORING_MOD_TRIED:
        return _SCORING_MOD
    _SCORING_MOD_TRIED = True
    for mod_path in ("core.scoring", "scoring", "core.scoring_engine", "scoring_engine"):
        try:
            mod = import_module(mod_path)
            if callable(getattr(mod, "compute_scores", None)):
                _SCORING_MOD = mod
                return _SCORING_MOD
        except ImportError:
            continue
        except Exception:  # pragma: no cover
            continue
    _SCORING_MOD = None
    return None


def _derive_new_columns(row: Dict[str, Any], page: str = "") -> None:
    """Fill derived columns when not already provided."""
    if row.get("volume_ratio") in (None, ""):
        vol = _safe_float(row.get("volume"))
        avg_vol = _safe_float(row.get("avg_volume_10d")) or _safe_float(row.get("avg_volume_30d"))
        if vol is not None and avg_vol not in (None, 0):
            row["volume_ratio"] = round(vol / avg_vol, 4)

    if row.get("day_range_position") in (None, ""):
        price = _safe_float(row.get("current_price"))
        low = _safe_float(row.get("day_low"))
        high = _safe_float(row.get("day_high"))
        if price is not None and low is not None and high is not None and high > low:
            row["day_range_position"] = round(_clamp((price - low) / (high - low), 0.0, 1.0), 4)

    if row.get("upside_pct") in (None, ""):
        price = _safe_float(row.get("current_price"))
        intrinsic = _safe_float(row.get("intrinsic_value")) or _safe_float(row.get("target_price"))
        if price not in (None, 0) and intrinsic is not None:
            row["upside_pct"] = round((intrinsic - price) / price, 4)


def _compute_scores_fallback(row: Dict[str, Any]) -> None:
    """Local fallback scoring — non-destructive (only fills None fields)."""
    price = _safe_float(row.get("current_price"))
    pe = _safe_float(row.get("pe_ttm"))
    pb = _safe_float(row.get("pb_ratio"))
    rev_growth = _safe_float(row.get("revenue_growth_yoy")) or 0.0
    margin = _safe_float(row.get("profit_margin")) or 0.0
    div_yield = _safe_float(row.get("dividend_yield")) or 0.0

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
        pct = _safe_float(row.get("percent_change")) or 0.0
        row["momentum_score"] = round(_clamp(50.0 + pct, 0.0, 100.0), 2)

    if row.get("valuation_score") is None:
        intrinsic = _safe_float(row.get("intrinsic_value")) or _safe_float(row.get("target_price"))
        score = 50.0
        if intrinsic is not None and price not in (None, 0):
            score += _clamp(((intrinsic - price) / price) * 100.0, -20.0, 25.0)
        row["valuation_score"] = round(_clamp(score, 0.0, 100.0), 2)

    if row.get("overall_score") is None:
        vals = [
            _safe_float(row.get("value_score")),
            _safe_float(row.get("quality_score")),
            _safe_float(row.get("growth_score")),
            _safe_float(row.get("momentum_score")),
            _safe_float(row.get("valuation_score")),
        ]
        vals = [v for v in vals if v is not None]
        row["overall_score"] = round(sum(vals) / len(vals), 2) if vals else 50.0

    if row.get("risk_score") is None:
        beta = _safe_float(row.get("beta_5y")) or 1.0
        row["risk_score"] = round(_clamp(35.0 + beta * 10.0, 0.0, 100.0), 2)

    if row.get("risk_bucket") is None:
        rs = _safe_float(row.get("risk_score")) or 50.0
        row["risk_bucket"] = "LOW" if rs < 40 else "MODERATE" if rs < 70 else "HIGH"

    if row.get("forecast_confidence") is None:
        row["forecast_confidence"] = 0.55

    if row.get("confidence_score") is None:
        fc = _safe_float(row.get("forecast_confidence")) or 0.55
        row["confidence_score"] = round(fc * 100.0 if fc <= 1.5 else fc, 2)

    if row.get("confidence_bucket") is None:
        cs = _safe_float(row.get("confidence_score")) or 55.0
        row["confidence_bucket"] = "HIGH" if cs >= 75 else "MODERATE" if cs >= 55 else "LOW"

    if row.get("opportunity_score") is None:
        base = _safe_float(row.get("overall_score")) or 50.0
        conf = _safe_float(row.get("confidence_score")) or 55.0
        risk = _safe_float(row.get("risk_score")) or 50.0
        row["opportunity_score"] = round(
            _clamp(base + (conf - 50.0) * 0.2 - (risk - 50.0) * 0.25, 0.0, 100.0), 2
        )

    if price is not None:
        if row.get("forecast_price_1m") is None:
            row["forecast_price_1m"] = round(price * 1.01, 4)
        if row.get("forecast_price_3m") is None:
            row["forecast_price_3m"] = round(price * 1.03, 4)
        if row.get("forecast_price_12m") is None:
            row["forecast_price_12m"] = round(price * 1.08, 4)

        if row.get("expected_roi_1m") is None:
            row["expected_roi_1m"] = round((row["forecast_price_1m"] - price) / price, 6)
        if row.get("expected_roi_3m") is None:
            row["expected_roi_3m"] = round((row["forecast_price_3m"] - price) / price, 6)
        if row.get("expected_roi_12m") is None:
            row["expected_roi_12m"] = round((row["forecast_price_12m"] - price) / price, 6)


def _try_scoring_module(row: Dict[str, Any], settings: Any = None) -> None:
    """
    Apply core.scoring patch to the row, always merging; top up with the
    fallback if any critical scoring fields are still empty afterwards.

    v6.1.2 — Respect scoring.py v4.1.2's intentional None for valuation:
    -------------------------------------------------------------------
    `_SCORING_CRITICAL_FIELDS` no longer includes `valuation_score`.
    scoring.py v4.1.2 returns None for valuation_score when no
    forward-looking signal is available, and we must honor that
    decision rather than override it with a phantom fallback value.
    The remaining four critical fields still catch real scorer
    failures. See the module docstring for details.

    v6.1.1 — Root-cause fix for "scoring columns partially blank":
    ------------------------------------------------------------------
    Previously this function returned as soon as the scorer returned a
    dict, even if the dict contained None for overall_score, confidence_score
    or recommendation. That caused the sheet to show partially-filled
    scoring columns (e.g. Risk Score and Valuation Score populated but
    Overall Score, Confidence Score, and Recommendation blank).

    Now: merge whatever the scorer provides (still skipping None values
    so the scorer cannot erase existing good data), then run the
    fallback to fill any critical field that is still None. The fallback
    itself is non-destructive — each branch checks `row.get(k) is None`
    before writing.

    Critical fields topped up: see `_SCORING_CRITICAL_FIELDS`.

    IMPORTANT (preserved from v6.1.0): merges every non-None key from
    the scorer into the row, regardless of whether the row already held
    a value. This preserves scorer-owned provenance fields such as
    scoring_updated_utc, scoring_updated_riyadh, scoring_errors,
    overall_score_raw, overall_penalty_factor, expected_return_*,
    expected_price_*.
    """
    scorer_ran = False

    mod = _get_scoring_mod()
    if mod is not None:
        try:
            patch = mod.compute_scores(row, settings=settings)
        except TypeError:
            try:
                patch = mod.compute_scores(row)
            except Exception:
                patch = None
        except Exception:
            patch = None

        if isinstance(patch, dict):
            scorer_ran = True
            for k, v in patch.items():
                if v is None:
                    continue
                row[k] = v

    # v6.1.1/v6.1.2: Top up with fallback if scorer didn't run OR left any
    # critical field None. The fallback is non-destructive — it only
    # writes to fields that are currently None. valuation_score was
    # removed from the critical fields in v6.1.2 (see module docstring).
    if (not scorer_ran) or any(row.get(field) is None for field in _SCORING_CRITICAL_FIELDS):
        _compute_scores_fallback(row)


def _compute_recommendation(row: Dict[str, Any]) -> None:
    """
    Compute a recommendation if the scorer didn't provide one.
    core.scoring already returns a recommendation; this is only a safety net.
    """
    if row.get("recommendation"):
        return

    overall = _safe_float(row.get("overall_score")) or 50.0
    conf = _safe_float(row.get("confidence_score")) or 55.0
    risk = _safe_float(row.get("risk_score")) or 50.0

    if overall >= 78 and conf >= 70 and risk <= 45:
        rec = "STRONG_BUY"
    elif overall >= 72 and conf >= 65 and risk <= 60:
        rec = "BUY"
    elif overall >= 60 and conf >= 55:
        rec = "BUY"
    elif overall <= 35 or risk >= 85:
        rec = "REDUCE"
    else:
        rec = "HOLD"

    row["recommendation"] = rec
    row.setdefault(
        "recommendation_reason",
        f"overall={round(overall, 1)} conf={round(conf, 1)} risk={round(risk, 1)}",
    )


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    """Rank rows by overall_score (desc). Delegates to core.scoring when present."""
    mod = _get_scoring_mod()
    if mod is not None:
        ranker = getattr(mod, "assign_rank_overall", None) or getattr(mod, "rank_rows_by_overall", None)
        if callable(ranker):
            try:
                ranker(rows)
                return
            except Exception:
                pass

    # Fallback: simple sort by overall_score / opportunity_score
    scored: List[Tuple[int, float]] = []
    for i, row in enumerate(rows):
        score = _safe_float(row.get("overall_score"))
        if score is None:
            score = _safe_float(row.get("opportunity_score"))
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


# ---------------------------------------------------------------------------
# Provider registry and fetching
# ---------------------------------------------------------------------------

_PROVIDER_CALLABLE_NAMES: Tuple[str, ...] = (
    # Full-quote forms
    "get_enriched_quote", "fetch_enriched_quote",
    "get_quote", "fetch_quote",
    "quote",
    # Patch forms (returned by most shims for partial enrichment)
    "fetch_enriched_quote_patch", "get_enriched_quote_patch",
    "fetch_quote_patch", "get_quote_patch",
    # Misc aliases
    "get_row", "fetch_row",
)


def _pick_provider_callable(module: Any, provider: str) -> Optional[Any]:
    """Find a callable on the provider module for fetching a single symbol quote."""
    for name in _PROVIDER_CALLABLE_NAMES:
        fn = getattr(module, name, None)
        if callable(fn):
            return fn

    for attr in (provider, f"{provider}_quote", f"{provider}_provider", "client", "service"):
        obj = getattr(module, attr, None)
        if obj is None:
            continue
        for name in _PROVIDER_CALLABLE_NAMES:
            fn = getattr(obj, name, None)
            if callable(fn):
                return fn
    return None


class ProviderRegistry:
    """Provider registry: loads provider modules lazily and tracks stats."""

    def __init__(self) -> None:
        self._stats: Dict[str, Dict[str, Any]] = {}

    async def get_provider(self, provider: str) -> Tuple[Optional[Any], Any]:
        module = None
        for mod_path in (
            f"core.providers.{provider}",
            f"providers.{provider}",
            f"core.providers.{provider}_provider",
            f"providers.{provider}_provider",
            f"core.{provider}_provider",
            f"{provider}_provider",
        ):
            try:
                module = import_module(mod_path)
                break
            except ImportError:
                continue
            except Exception:  # pragma: no cover
                continue

        stats = type(
            "ProviderStats",
            (),
            {
                "is_circuit_open": False,
                "last_import_error": "" if module is not None else "provider module missing",
            },
        )()
        return module, stats

    async def record_success(self, provider: str, latency_ms: float) -> None:
        stat = self._stats.setdefault(
            provider,
            {"success": 0, "failure": 0, "last_error": "", "latency_ms": 0.0},
        )
        stat["success"] += 1
        stat["latency_ms"] = round(float(latency_ms or 0.0), 2)

    async def record_failure(self, provider: str, error: str) -> None:
        stat = self._stats.setdefault(
            provider,
            {"success": 0, "failure": 0, "last_error": "", "latency_ms": 0.0},
        )
        stat["failure"] += 1
        stat["last_error"] = _safe_str(error)

    async def get_stats(self) -> Dict[str, Any]:
        return {k: dict(v) for k, v in self._stats.items()}


# ---------------------------------------------------------------------------
# Async primitives
# ---------------------------------------------------------------------------


class SingleFlight:
    """Deduplicate concurrent requests with the same key."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._tasks: Dict[str, asyncio.Task[Any]] = {}

    async def execute(self, key: str, factory: Callable[[], Any]) -> Any:
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
    """Single-level in-memory TTL cache (public API compat name retained)."""

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


# ---------------------------------------------------------------------------
# Symbols reader integration
# ---------------------------------------------------------------------------

_SYMBOLS_READER_MOD: Any = None
_SYMBOLS_READER_TRIED = False


def _symbols_reader_module() -> Any:
    global _SYMBOLS_READER_MOD, _SYMBOLS_READER_TRIED
    if _SYMBOLS_READER_TRIED:
        return _SYMBOLS_READER_MOD
    _SYMBOLS_READER_TRIED = True
    for mod_path in ("core.symbols_reader", "symbols_reader"):
        try:
            _SYMBOLS_READER_MOD = import_module(mod_path)
            return _SYMBOLS_READER_MOD
        except ImportError:
            continue
        except Exception:  # pragma: no cover
            continue
    return None


async def _call_reader_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a reader function that may be sync or async."""
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    return await asyncio.to_thread(fn, *args, **kwargs)


class _EngineSymbolsReaderProxy:
    """
    Thin proxy exposed as engine.symbols_reader.

    Delegates to core.symbols_reader when available, otherwise routes back
    through the engine's own resolution logic.
    """

    def __init__(self, engine: "DataEngineV5") -> None:
        self._engine = engine

    async def get_symbols_for_sheet(self, sheet: str, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await self._engine.get_sheet_symbols(sheet=sheet, limit=limit, **kwargs)

    async def get_symbols_for_page(self, page: str, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await self._engine.get_page_symbols(page=page, limit=limit, **kwargs)

    async def list_symbols_for_page(self, page: str, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await self._engine.list_symbols_for_page(page=page, limit=limit, **kwargs)

    async def get_sheet_symbols(self, sheet: str, limit: int = 5000, **kwargs: Any) -> List[str]:
        return await self._engine.get_sheet_symbols(sheet=sheet, limit=limit, **kwargs)


# ---------------------------------------------------------------------------
# DataEngineV5 implementation
# ---------------------------------------------------------------------------


class DataEngineV5:
    """Data Engine V5 — primary data access layer (schema-aligned)."""

    def __init__(self, settings: Any = None) -> None:
        self.settings = settings
        self.version = __version__

        # Provider configuration
        self.primary_provider = (
            _safe_str(
                getattr(settings, "primary_provider", None),
                _safe_str(os.getenv("PRIMARY_PROVIDER"), "eodhd"),
            ).lower()
            or "eodhd"
        )

        self.enabled_providers = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)

        self.non_ksa_primary_provider = (
            _safe_str(
                getattr(settings, "non_ksa_primary_provider", None),
                _safe_str(os.getenv("NON_KSA_PRIMARY_PROVIDER"), "eodhd"),
            ).lower()
            or "eodhd"
        )

        configured_non_ksa_pages = [
            _canonicalize_sheet_name(p)
            for p in _get_env_list("NON_KSA_PRIMARY_PAGES", list(NON_KSA_EODHD_PRIMARY_PAGES))
            if _safe_str(p)
        ]
        self.page_primary_providers = {
            page: self.non_ksa_primary_provider for page in configured_non_ksa_pages if page
        }

        # Performance configuration
        self.max_concurrency = _get_env_int("DATA_ENGINE_MAX_CONCURRENCY", 25)
        self.request_timeout = _get_env_float("DATA_ENGINE_TIMEOUT_SECONDS", 12.0)
        self.sheet_rows_time_budget = _get_env_float("SHEET_ROWS_TIME_BUDGET_SECONDS", 12.0)
        self.sheet_rows_hydration_cap = _get_env_int("SHEET_ROWS_HYDRATION_CAP", 30)
        self.sheet_rows_symbol_buffer = _get_env_int("SHEET_ROWS_SYMBOL_BUFFER", 8)

        # Feature flags
        self.ksa_disallow_eodhd = _get_env_bool("KSA_DISALLOW_EODHD", True)
        self.schema_strict_sheet_rows = _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)
        self.top10_force_full_schema = _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True)
        self.rows_hydrate_external = _get_env_bool("ROWS_HYDRATE_EXTERNAL_READER", True)

        # Async primitives
        self._sem = asyncio.Semaphore(max(1, self.max_concurrency))
        self._singleflight = SingleFlight()
        self._registry = ProviderRegistry()
        self._cache = MultiLevelCache(
            "data_engine",
            l1_ttl=_get_env_int("CACHE_L1_TTL", 60),
            max_l1_size=_get_env_int("CACHE_L1_MAX", 5000),
        )
        self._symbols_cache = MultiLevelCache(
            "sheet_symbols",
            l1_ttl=_get_env_int("SHEET_SYMBOLS_L1_TTL", 300),
            max_l1_size=_get_env_int("SHEET_SYMBOLS_L1_MAX", 256),
        )

        # State
        self._sheet_snapshots: Dict[str, Dict[str, Any]] = {}
        self._sheet_symbol_resolution_meta: Dict[str, Dict[str, Any]] = {}
        self._rows_reader_source = ""
        self._rows_reader_obj: Any = None
        self._rows_reader_ready = False
        self._rows_reader_lock = asyncio.Lock()

        self._symbols_reader_obj: Any = None
        self._symbols_reader_source = ""
        self._symbols_reader_ready = False
        self._symbols_reader_lock = asyncio.Lock()

        self.symbols_reader = _EngineSymbolsReaderProxy(self)

    # -----------------------------------------------------------------------
    # Lifecycle + health
    # -----------------------------------------------------------------------

    async def aclose(self) -> None:
        return

    async def get_health(self) -> Dict[str, Any]:
        return {
            "status": "healthy",
            "version": self.version,
            "timestamp_utc": _now_utc_iso(),
            "providers": list(self.enabled_providers),
            "provider_stats": await self._registry.get_stats(),
            "rows_reader_source": self._rows_reader_source or "",
            "symbols_reader_source": self._symbols_reader_source or "",
            "scoring_module": _safe_str(getattr(_get_scoring_mod(), "__name__", "")),
            "schema_source": "schema_registry" if SCHEMA_REGISTRY else "fallback",
            "scoring_critical_fields": list(_SCORING_CRITICAL_FIELDS),
        }

    async def health(self) -> Dict[str, Any]:
        return await self.get_health()

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
            "rows_reader_source": self._rows_reader_source or "",
            "symbols_reader_source": self._symbols_reader_source or "",
            "schema_source": "schema_registry" if SCHEMA_REGISTRY else "fallback",
            "scoring_critical_fields": list(_SCORING_CRITICAL_FIELDS),
        }

    # -----------------------------------------------------------------------
    # Snapshot helpers
    # -----------------------------------------------------------------------

    def _store_sheet_snapshot(self, sheet: str, payload: Dict[str, Any]) -> None:
        target = _canonicalize_sheet_name(sheet)
        if target and isinstance(payload, dict):
            self._sheet_snapshots[target] = dict(payload)

    def get_cached_sheet_snapshot(
        self,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet or page or sheet_name or "")
        snap = self._sheet_snapshots.get(target)
        return dict(snap) if isinstance(snap, dict) else None

    def _set_sheet_symbols_meta(self, sheet: str, source: str, count: int, note: Optional[str] = None) -> None:
        target = _canonicalize_sheet_name(sheet)
        if target:
            self._sheet_symbol_resolution_meta[target] = {
                "sheet": target,
                "source": source,
                "count": int(count or 0),
                "note": note or "",
                "timestamp_utc": _now_utc_iso(),
            }

    def _get_sheet_symbols_meta(self, sheet: str) -> Dict[str, Any]:
        return dict(self._sheet_symbol_resolution_meta.get(_canonicalize_sheet_name(sheet), {}))

    def _get_best_cached_snapshot_row_for_symbol(
        self,
        symbol: str,
        prefer_sheet: str = "",
    ) -> Optional[Dict[str, Any]]:
        sym = normalize_symbol(symbol)
        preferred = _canonicalize_sheet_name(prefer_sheet) if prefer_sheet else ""

        best_row: Optional[Dict[str, Any]] = None
        best_score = -1

        for sheet_name, snap in self._sheet_snapshots.items():
            rows = _coerce_rows_list(snap)
            for row in rows:
                if normalize_symbol(_extract_row_symbol(row)) != sym:
                    continue
                score = _row_signal_count(row)
                if preferred and _canonicalize_sheet_name(sheet_name) == preferred:
                    score += 1000
                if score > best_score:
                    best_score = score
                    best_row = dict(row)

        return best_row

    # -----------------------------------------------------------------------
    # Payload finalization
    # -----------------------------------------------------------------------

    def _finalize_payload(
        self,
        sheet: str,
        headers: List[str],
        keys: List[str],
        row_objects: List[Dict[str, Any]],
        include_matrix: bool,
        status: str = "success",
        meta: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
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

    # -----------------------------------------------------------------------
    # External rows reader (Google Sheets)
    # -----------------------------------------------------------------------

    async def _init_rows_reader(self) -> Tuple[Any, str]:
        if self._rows_reader_ready:
            return self._rows_reader_obj, self._rows_reader_source

        async with self._rows_reader_lock:
            if self._rows_reader_ready:
                return self._rows_reader_obj, self._rows_reader_source

            obj = None
            source = ""

            for mod_path in (
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
                "core.symbols_reader",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
            ):
                try:
                    mod = import_module(mod_path)
                except ImportError:
                    continue
                except Exception:  # pragma: no cover
                    continue

                if any(
                    callable(getattr(mod, nm, None))
                    for nm in (
                        "get_rows_for_sheet", "read_rows_for_sheet",
                        "get_sheet_rows", "fetch_sheet_rows",
                        "sheet_rows", "get_rows",
                    )
                ):
                    obj = mod
                    source = mod_path
                    break

                for attr_name in ("service", "reader", "rows_reader", "google_sheets_service"):
                    candidate = getattr(mod, attr_name, None)
                    if candidate is not None:
                        obj = candidate
                        source = f"{mod_path}.{attr_name}"
                        break
                if obj is not None:
                    break

            self._rows_reader_obj = obj
            self._rows_reader_source = source
            self._rows_reader_ready = True

            return obj, source

    async def _call_rows_reader(
        self,
        obj: Any,
        sheet: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        if obj is None:
            return []

        timeout_s = _get_env_float("ROWS_READER_TIMEOUT_SECONDS", 6.0)

        for name in (
            "get_rows_for_sheet", "read_rows_for_sheet",
            "get_sheet_rows", "fetch_sheet_rows",
            "sheet_rows", "get_rows",
        ):
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue

            for args, kwargs in (
                ((), {"sheet": sheet, "limit": limit}),
                ((), {"sheet_name": sheet, "limit": limit}),
                ((), {"page": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ):
                try:
                    async with asyncio.timeout(timeout_s):
                        result = await _call_reader_async(fn, *args, **kwargs)
                    rows = _coerce_rows_list(result)
                    if rows:
                        return rows[:limit]
                except TypeError:
                    continue
                except Exception:
                    continue

        return []

    async def _get_rows_from_external_reader(self, sheet: str, limit: int) -> List[Dict[str, Any]]:
        obj, _ = await self._init_rows_reader()
        return [] if obj is None else await self._call_rows_reader(obj, sheet, limit)

    # -----------------------------------------------------------------------
    # Symbols reader (core.symbols_reader delegation)
    # -----------------------------------------------------------------------

    async def _init_symbols_reader(self) -> Tuple[Any, str]:
        if self._symbols_reader_ready:
            return self._symbols_reader_obj, self._symbols_reader_source

        async with self._symbols_reader_lock:
            if self._symbols_reader_ready:
                return self._symbols_reader_obj, self._symbols_reader_source

            obj: Any = None
            source = ""

            mod = _symbols_reader_module()
            if mod is not None:
                # Prefer the factory/class from the module
                factory = (
                    getattr(mod, "get_reader", None)
                    or getattr(mod, "create_reader", None)
                    or getattr(mod, "build_reader", None)
                    or getattr(mod, "SymbolsReader", None)
                )
                if callable(factory):
                    try:
                        obj = factory(settings=self.settings)
                        source = f"{mod.__name__}.{factory.__name__}"
                    except TypeError:
                        try:
                            obj = factory()
                            source = f"{mod.__name__}.{getattr(factory, '__name__', 'factory')}"
                        except Exception as exc:  # pragma: no cover
                            logger.debug("symbols_reader factory failed: %s", exc)
                            obj = None

                # Fall back to module-level functions
                if obj is None:
                    obj = mod
                    source = mod.__name__

            self._symbols_reader_obj = obj
            self._symbols_reader_source = source
            self._symbols_reader_ready = True

            return obj, source

    async def _call_symbols_reader(
        self,
        sheet: str,
        limit: int,
    ) -> List[str]:
        obj, _ = await self._init_symbols_reader()
        if obj is None:
            return []

        timeout_s = _get_env_float("SYMBOLS_READER_TIMEOUT_SECONDS", 6.0)

        # Try typical method names in order of specificity
        for name in (
            "get_symbols_for_sheet",
            "read_symbols_for_sheet",
            "get_sheet_symbols",
            "get_symbols_for_page",
            "list_symbols_for_page",
            "get_page_symbols",
            "get_symbols",
            "list_symbols",
        ):
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue

            for kwargs in (
                {"sheet": sheet, "limit": limit, "settings": self.settings},
                {"sheet": sheet, "limit": limit},
                {"page": sheet, "limit": limit, "settings": self.settings},
                {"page": sheet, "limit": limit},
                {"sheet_name": sheet, "limit": limit},
            ):
                try:
                    async with asyncio.timeout(timeout_s):
                        result = await _call_reader_async(fn, **kwargs)
                except TypeError:
                    continue
                except Exception:
                    continue

                # Accept list[str] / list[dict] / (list, meta) tuple
                if isinstance(result, tuple) and result:
                    result = result[0]
                if isinstance(result, list):
                    if result and isinstance(result[0], dict):
                        syms = _extract_symbols_from_rows(result, limit=limit)
                    else:
                        syms = _normalize_symbol_list(result, limit=limit)
                    if syms:
                        return syms

            # Try positional form as a last resort for this name
            try:
                async with asyncio.timeout(timeout_s):
                    result = await _call_reader_async(fn, sheet, limit)
                if isinstance(result, tuple) and result:
                    result = result[0]
                if isinstance(result, list):
                    if result and isinstance(result[0], dict):
                        syms = _extract_symbols_from_rows(result, limit=limit)
                    else:
                        syms = _normalize_symbol_list(result, limit=limit)
                    if syms:
                        return syms
            except Exception:
                continue

        return []

    # -----------------------------------------------------------------------
    # Provider selection and fetching
    # -----------------------------------------------------------------------

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

        primary = (
            self.non_ksa_primary_provider
            if (not is_ksa_sym and page_ctx in self.page_primary_providers)
            else self.primary_provider
        )

        if primary and _provider_allowed(primary):
            providers = [p for p in providers if p != primary]
            providers.insert(0, primary)

        return _deduplicate_keep_order([p for p in providers if _safe_str(p)])

    async def _fetch_patch(
        self,
        provider: str,
        symbol: str,
    ) -> Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]:
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

            result = None
            errors: List[str] = []

            for args, kwargs in (
                ((symbol,), {}),
                ((), {"symbol": symbol}),
                ((), {"ticker": symbol}),
                ((symbol,), {"settings": self.settings}),
                ((), {"symbol": symbol, "settings": self.settings}),
            ):
                try:
                    async with asyncio.timeout(self.request_timeout):
                        result = await _call_reader_async(fn, *args, **kwargs)
                    break
                except TypeError:
                    continue
                except Exception as exc:
                    errors.append(f"{type(exc).__name__}: {str(exc)[:120]}")

            latency = (time.time() - start) * 1000.0
            patch = _model_to_dict(result)

            if patch:
                await self._registry.record_success(provider, latency)
                return provider, patch, latency, None

            err = " | ".join(errors) if errors else "non_dict_or_empty"
            await self._registry.record_failure(provider, err)
            return provider, None, latency, err

    def _merge(
        self,
        requested_symbol: str,
        norm: str,
        patches: List[Tuple[str, Dict[str, Any], float]],
    ) -> Dict[str, Any]:
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
            normalized_patches.append(
                (
                    prov,
                    _canonicalize_provider_row(
                        patch,
                        requested_symbol=requested_symbol,
                        normalized_symbol=norm,
                        provider=prov,
                    ),
                    latency,
                )
            )

        for prov, patch, latency in sorted(
            normalized_patches,
            key=lambda item: (PROVIDER_PRIORITIES.get(item[0], 999), -_row_signal_count(item[1])),
        ):
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(float(latency or 0.0), 2)

            for k, v in patch.items():
                if k in {"symbol", "symbol_normalized", "requested_symbol"} or v is None:
                    continue
                if k not in merged or merged.get(k) in (None, "", [], {}):
                    merged[k] = v

        return _canonicalize_provider_row(
            merged,
            requested_symbol=requested_symbol,
            normalized_symbol=norm,
            provider=_safe_str(
                (merged.get("data_sources") or [""])[0]
                if isinstance(merged.get("data_sources"), list)
                else ""
            ),
        )

    def _data_quality(self, row: Dict[str, Any]) -> str:
        if _safe_float(row.get("current_price")) is None:
            return QuoteQuality.MISSING.value
        return (
            QuoteQuality.GOOD.value
            if any(row.get(k) is not None for k in ("overall_score", "forecast_price_3m", "pb_ratio"))
            else QuoteQuality.FAIR.value
        )

    # -----------------------------------------------------------------------
    # Quote fetching
    # -----------------------------------------------------------------------

    async def _get_enriched_quote_impl(
        self,
        symbol: str,
        use_cache: bool = True,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> UnifiedQuote:
        norm = normalize_symbol(symbol)
        if not norm:
            return UnifiedQuote(
                symbol=_safe_str(symbol),
                requested_symbol=_safe_str(symbol),
                data_quality=QuoteQuality.MISSING.value,
                error="Invalid symbol",
                last_updated_utc=_now_utc_iso(),
                last_updated_riyadh=_now_riyadh_iso(),
            )

        page_ctx = _canonicalize_sheet_name(
            page or sheet or _safe_str((body or {}).get("page") or (body or {}).get("sheet"))
        )
        cache_key = f"{norm}|{page_ctx or 'default'}"

        if use_cache:
            cached = await self._cache.get(symbol=cache_key)
            if isinstance(cached, dict) and cached:
                return UnifiedQuote(**cached)

        providers = self._providers_for(norm, page=page_ctx)
        patches_ok: List[Tuple[str, Dict[str, Any], float]] = []

        if providers:
            gathered = await asyncio.gather(
                *[self._fetch_patch(p, norm) for p in providers[:4]],
                return_exceptions=True,
            )
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

        cached_row = self._get_best_cached_snapshot_row_for_symbol(norm, prefer_sheet=page_ctx)
        if cached_row:
            row = _merge_missing_fields(row, cached_row)

        row = _apply_page_row_backfill(page_ctx or "Market_Leaders", row)
        _try_scoring_module(row, settings=self.settings)
        _compute_recommendation(row)
        row["data_quality"] = self._data_quality(row)

        if not row.get("data_provider"):
            row["data_provider"] = _safe_str(
                (row.get("data_sources") or [""])[0]
                if isinstance(row.get("data_sources"), list)
                else ""
            )

        quote = UnifiedQuote(**row)

        if use_cache:
            await self._cache.set(_model_to_dict(quote), symbol=cache_key)

        return quote

    async def get_enriched_quote(
        self,
        symbol: str,
        use_cache: bool = True,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> UnifiedQuote:
        key = (
            f"quote:{normalize_symbol(symbol)}"
            f":{_canonicalize_sheet_name(page or sheet or '')}"
            f":{'cache' if use_cache else 'live'}"
        )
        quote = await self._singleflight.execute(
            key,
            lambda: self._get_enriched_quote_impl(
                symbol, use_cache, page=page, sheet=sheet, body=body, **kwargs
            ),
        )

        if schema is None:
            return quote

        row = _model_to_dict(quote)
        if isinstance(schema, str):
            _spec, hdrs, keys, _src = _schema_for_sheet(schema)
            return UnifiedQuote(**_normalize_to_schema_keys(keys, hdrs, row))

        return quote

    async def get_enriched_quote_dict(
        self,
        symbol: str,
        use_cache: bool = True,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return _model_to_dict(
            await self.get_enriched_quote(
                symbol, use_cache=use_cache, schema=schema,
                page=page, sheet=sheet, body=body, **kwargs,
            )
        )

    async def get_enriched_quotes(
        self,
        symbols: List[str],
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
        **kwargs: Any,
    ) -> List[UnifiedQuote]:
        if not symbols:
            return []

        batch = max(1, min(100, _get_env_int("QUOTE_BATCH_SIZE", 20)))
        results: List[UnifiedQuote] = []

        for i in range(0, len(symbols), batch):
            batch_symbols = symbols[i:i + batch]
            batch_results = await asyncio.gather(
                *[
                    self.get_enriched_quote(
                        s, use_cache=use_cache, schema=schema,
                        page=page, sheet=sheet, body=body, **kwargs,
                    )
                    for s in batch_symbols
                ]
            )
            results.extend(batch_results)

        return results

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
        **kwargs: Any,
    ) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        norm_symbols = _normalize_symbol_list(symbols, limit=len(symbols) + 10)

        quotes = await asyncio.gather(
            *[
                self.get_enriched_quote_dict(
                    s, use_cache=use_cache, schema=schema,
                    page=page, sheet=sheet, body=body, **kwargs,
                )
                for s in norm_symbols
            ]
        )

        for req_sym, qd in zip(norm_symbols, quotes):
            result[req_sym] = qd
            norm = _safe_str(qd.get("symbol_normalized") or qd.get("symbol"))
            if norm:
                result[norm] = qd

        return result

    # Aliases for compatibility
    get_quote = get_enriched_quote
    get_quotes = get_enriched_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_enriched_quotes
    get_quotes_batch = get_enriched_quotes_batch
    get_analysis_quotes_batch = get_enriched_quotes_batch
    quotes_batch = get_enriched_quotes_batch
    get_quote_dict = get_enriched_quote_dict

    # -----------------------------------------------------------------------
    # Row builders for special sheets
    # -----------------------------------------------------------------------

    async def _build_data_dictionary_rows(self) -> List[Dict[str, Any]]:
        """Build Data_Dictionary rows from schema_registry (or static fallback)."""
        rows: List[Dict[str, Any]] = []

        # If registry is available, use its actual column metadata
        if SCHEMA_REGISTRY:
            for sheet_name, spec in SCHEMA_REGISTRY.items():
                cols = _schema_columns_from_any(spec)
                for col in cols:
                    if isinstance(col, Mapping):
                        entry = {
                            "sheet": sheet_name,
                            "group": _safe_str(col.get("group")),
                            "header": _safe_str(col.get("header")),
                            "key": _safe_str(col.get("key")),
                            "dtype": _safe_str(col.get("dtype"), "str"),
                            "fmt": _safe_str(col.get("fmt"), "text"),
                            "required": bool(col.get("required", False)),
                            "source": _safe_str(col.get("source")),
                            "notes": _safe_str(col.get("notes")),
                        }
                    else:
                        entry = {
                            "sheet": sheet_name,
                            "group": _safe_str(getattr(col, "group", "")),
                            "header": _safe_str(getattr(col, "header", "")),
                            "key": _safe_str(getattr(col, "key", "")),
                            "dtype": _safe_str(getattr(col, "dtype", "str"), "str"),
                            "fmt": _safe_str(getattr(col, "fmt", "text"), "text"),
                            "required": bool(getattr(col, "required", False)),
                            "source": _safe_str(getattr(col, "source", "")),
                            "notes": _safe_str(getattr(col, "notes", "")),
                        }
                    if entry["header"] and entry["key"]:
                        rows.append(entry)
            if rows:
                return rows

        # Fallback: derive from STATIC_CANONICAL_SHEET_CONTRACTS with coarse groups
        for sheet_name, contract in STATIC_CANONICAL_SHEET_CONTRACTS.items():
            headers, keys = _complete_schema_contract(contract["headers"], contract["keys"])
            for idx, (header, key) in enumerate(zip(headers, keys), start=1):
                if sheet_name == "Insights_Analysis":
                    group = "Insights" if key != "last_updated_riyadh" else "Provenance"
                elif sheet_name == "Data_Dictionary":
                    group = "Dictionary"
                elif key in TOP10_REQUIRED_FIELDS:
                    group = "Top10"
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
                elif idx <= 59:
                    group = "Forecast"
                elif idx <= 66:
                    group = "Scores"
                elif idx <= 70:
                    group = "Recommendation"
                elif idx <= 76:
                    group = "Portfolio"
                else:
                    group = "Provenance"

                rows.append({
                    "sheet": sheet_name,
                    "group": group,
                    "header": header,
                    "key": key,
                    "dtype": "str",
                    "fmt": "text",
                    "required": key == "symbol",
                    "source": "static_contract",
                    "notes": "",
                })
        return rows

    async def _build_insights_rows_fallback(
        self,
        body: Optional[Dict[str, Any]],
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Build Insights_Analysis fallback rows (7-column schema)."""
        body = dict(body or {})
        symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 2, 10))

        if not symbols:
            for page_name in TOP10_ENGINE_DEFAULT_PAGES:
                symbols.extend(await self.get_sheet_symbols(page_name, limit=max(limit * 2, 10), body=body))

        symbols = _normalize_symbol_list(symbols, limit=max(limit * 3, 30))

        if not symbols:
            symbols = list(EMERGENCY_PAGE_SYMBOLS.get("Market_Leaders", [])[:max(limit, 6)])

        quotes = await self.get_enriched_quotes(
            symbols[:max(6, limit)],
            schema=None,
            page="Insights_Analysis",
            body=body,
        )

        rows = [_model_to_dict(q) for q in quotes if isinstance(_model_to_dict(q), dict)]
        rows.sort(
            key=lambda r: (
                _safe_float(r.get("opportunity_score")) or _safe_float(r.get("overall_score")) or 0.0,
                _safe_float(r.get("confidence_score")) or 0.0,
            ),
            reverse=True,
        )

        result: List[Dict[str, Any]] = [
            {
                "section": "Market Summary",
                "item": "Universe",
                "symbol": None,
                "metric": "Symbols Analyzed",
                "value": len(rows),
                "notes": "engine fallback summary",
                "last_updated_riyadh": _now_riyadh_iso(),
            }
        ]

        for idx, d in enumerate(rows[:max(3, min(7, limit))], start=1):
            result.append({
                "section": "Top Ideas",
                "item": d.get("name") or d.get("symbol"),
                "symbol": d.get("symbol"),
                "metric": "Recommendation",
                "value": d.get("recommendation"),
                "notes": d.get("recommendation_reason"),
                "last_updated_riyadh": _now_riyadh_iso(),
            })

        return result[:limit]

    async def _build_top10_rows_fallback(
        self,
        headers: Sequence[str],
        keys: Sequence[str],
        body: Optional[Dict[str, Any]],
        limit: int,
        mode: str = "",
    ) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        """Build Top_10_Investments fallback rows (83-column schema)."""
        body = dict(body or {})
        criteria = dict(body.get("criteria") or {}) if isinstance(body.get("criteria"), dict) else {}

        out_headers, out_keys = _ensure_top10_contract(headers, keys)

        top_n = max(1, min(int(criteria.get("top_n") or body.get("top_n") or 10), max(1, limit)))
        requested_pages = list(TOP10_ENGINE_DEFAULT_PAGES)
        requested_symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 10, 200))

        for page_name in requested_pages:
            if len(requested_symbols) >= max(limit * 10, 200):
                break
            requested_symbols.extend(
                await self.get_sheet_symbols(page_name, limit=max(limit * 2, 25), body=body)
            )

        requested_symbols = _normalize_symbol_list(
            requested_symbols or EMERGENCY_PAGE_SYMBOLS.get("Top_10_Investments", []),
            limit=max(limit * 10, 200),
        )

        quotes = await self.get_enriched_quotes(
            requested_symbols[:max(top_n * 3, 30)],
            schema=None,
            page="Top_10_Investments",
            body=body,
        )

        rows: List[Dict[str, Any]] = []
        for q in quotes:
            row = _apply_page_row_backfill("Top_10_Investments", _model_to_dict(q))
            _try_scoring_module(row, settings=self.settings)
            _compute_recommendation(row)
            rows.append(row)

        _apply_rank_overall(rows)
        rows.sort(
            key=lambda r: (
                _safe_float(r.get("opportunity_score")) or -1e9,
                _safe_float(r.get("overall_score")) or -1e9,
                _safe_float(r.get("confidence_score")) or -1e9,
            ),
            reverse=True,
        )

        selected: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        snapshot = {
            "top_n": top_n,
            "pages_selected": requested_pages,
            "direct_symbols": requested_symbols,
        }
        criteria_snapshot = json.dumps(_json_safe(snapshot), ensure_ascii=False, separators=(",", ":"))

        for row in rows:
            sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("requested_symbol")))
            if not sym or sym in seen:
                continue
            seen.add(sym)

            row["top10_rank"] = len(selected) + 1
            row["selection_reason"] = (
                row.get("selection_reason")
                or f"Selected by fallback ranking: overall={row.get('overall_score')} confidence={row.get('confidence_score')}"
            )
            row["criteria_snapshot"] = row.get("criteria_snapshot") or criteria_snapshot

            selected.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)))

            if len(selected) >= top_n:
                break

        return out_headers, out_keys, selected

    # -----------------------------------------------------------------------
    # Symbols per sheet/page
    # -----------------------------------------------------------------------

    async def get_sheet_symbols(
        self,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = (
            _normalize_route_call_inputs(
                page=page, sheet=sheet, sheet_name=sheet_name,
                limit=limit, offset=0, mode="", body=body, extras=kwargs,
            )
        )

        if target_sheet in SPECIAL_SHEETS:
            self._set_sheet_symbols_meta(target_sheet, "special_sheet", 0)
            return []

        # 1) Body-provided symbols
        from_body = _extract_requested_symbols_from_body(normalized_body, limit=effective_limit)
        if from_body:
            self._set_sheet_symbols_meta(target_sheet, "body_symbols", len(from_body))
            return from_body

        # 2) Cached symbols
        cached = await self._symbols_cache.get(sheet=target_sheet, limit=effective_limit)
        if isinstance(cached, list) and cached:
            syms = _normalize_symbol_list(cached, limit=effective_limit)
            self._set_sheet_symbols_meta(target_sheet, "symbols_cache", len(syms))
            return syms

        # 3) core.symbols_reader (authoritative)
        try:
            reader_syms = await self._call_symbols_reader(target_sheet, effective_limit)
        except Exception as exc:  # pragma: no cover
            logger.debug("symbols_reader delegation failed for %s: %s", target_sheet, exc)
            reader_syms = []
        if reader_syms:
            self._set_sheet_symbols_meta(
                target_sheet,
                f"symbols_reader:{self._symbols_reader_source or 'reader'}",
                len(reader_syms),
            )
            await self._symbols_cache.set(reader_syms, sheet=target_sheet, limit=effective_limit)
            return reader_syms

        # 4) External rows reader (Google Sheets service)
        ext_rows = await self._get_rows_from_external_reader(
            target_sheet, limit=min(max(25, effective_limit), 250)
        )
        if ext_rows:
            syms = _extract_symbols_from_rows(ext_rows, limit=effective_limit)
            if syms:
                self._set_sheet_symbols_meta(
                    target_sheet,
                    f"external_rows:{self._rows_reader_source or 'reader'}",
                    len(syms),
                )
                await self._symbols_cache.set(syms, sheet=target_sheet, limit=effective_limit)
                return syms

        # 5) Environment variables
        specific = PAGE_SYMBOL_ENV_KEYS.get(target_sheet)
        if specific:
            raw = os.getenv(specific, "") or ""
            if raw.strip():
                syms = _normalize_symbol_list(_split_symbols(raw), limit=effective_limit)
                self._set_sheet_symbols_meta(target_sheet, "env", len(syms))
                await self._symbols_cache.set(syms, sheet=target_sheet, limit=effective_limit)
                return syms

        # 6) Snapshot rows
        snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
        snap_rows = _coerce_rows_list(snap)
        if snap_rows:
            syms = _extract_symbols_from_rows(snap_rows, limit=effective_limit)
            if syms:
                self._set_sheet_symbols_meta(target_sheet, "snapshot_rows", len(syms))
                await self._symbols_cache.set(syms, sheet=target_sheet, limit=effective_limit)
                return syms

        # 7) Emergency fallback
        emergency = EMERGENCY_PAGE_SYMBOLS.get(target_sheet) or []
        syms = _normalize_symbol_list(emergency, limit=effective_limit)
        self._set_sheet_symbols_meta(
            target_sheet, "emergency_page_symbols", len(syms), note="last_resort_fallback"
        )
        await self._symbols_cache.set(syms, sheet=target_sheet, limit=effective_limit)
        return syms

    async def get_page_symbols(
        self,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        return await self.get_sheet_symbols(
            sheet=sheet, sheet_name=sheet_name, page=page, limit=limit, body=body, **kwargs,
        )

    async def list_symbols_for_page(
        self,
        page: str,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        return await self.get_page_symbols(page=page, limit=limit, body=body, **kwargs)

    # -----------------------------------------------------------------------
    # Sheet rows (main entrypoint)
    # -----------------------------------------------------------------------

    async def get_page_rows(
        self,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return await self.get_sheet_rows(
            page or sheet or sheet_name,
            limit=limit, offset=offset, mode=mode, body=body,
            page=page, sheet=sheet, sheet_name=sheet_name, **kwargs,
        )

    async def get_sheet(
        self,
        sheet_name: Optional[str] = None,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        return await self.get_sheet_rows(
            sheet_name or sheet or page,
            limit=limit, offset=offset, mode=mode, body=body,
            page=page, sheet=sheet, sheet_name=sheet_name, **kwargs,
        )

    async def get_sheet_contract(self, sheet: str) -> Dict[str, Any]:
        _spec, headers, keys, source = _schema_for_sheet(sheet)
        return {
            "sheet": _canonicalize_sheet_name(sheet),
            "headers": headers,
            "keys": keys,
            "source": source,
        }

    async def get_page_contract(self, page: str) -> Dict[str, Any]:
        return await self.get_sheet_contract(page)

    async def get_sheet_schema(self, sheet: str) -> Dict[str, Any]:
        return await self.get_sheet_contract(sheet)

    async def get_page_schema(self, page: str) -> Dict[str, Any]:
        return await self.get_sheet_contract(page)

    def _needed_hydration_count(self, limit: int, offset: int, page: str) -> int:
        base = max(limit + offset + self.sheet_rows_symbol_buffer, min(10, self.sheet_rows_hydration_cap))
        if page == "Top_10_Investments":
            base = max(base, 25)
        return max(1, min(base, self.sheet_rows_hydration_cap))

    def _build_symbol_scaffold_row(self, page: str, symbol: str, warning: str = "") -> Dict[str, Any]:
        row = {
            "symbol": normalize_symbol(symbol),
            "requested_symbol": normalize_symbol(symbol),
            "name": _infer_display_name_from_symbol(symbol) or normalize_symbol(symbol),
            "warnings": warning or "Built from symbol context fallback",
            "data_provider": DataSource.FALLBACK.value,
        }
        row = _apply_page_row_backfill(page, row)
        _try_scoring_module(row, settings=self.settings)
        _compute_recommendation(row)
        return row

    async def _safe_quote_map(
        self,
        symbols: List[str],
        page: str,
        body: Dict[str, Any],
        max_needed: int,
    ) -> Tuple[Dict[str, Dict[str, Any]], str]:
        take = _normalize_symbol_list(symbols, limit=max_needed)
        if not take:
            return {}, "no_symbols"

        try:
            async with asyncio.timeout(self.sheet_rows_time_budget):
                quotes = await self.get_enriched_quotes(take, schema=None, page=page, body=body)

            result: Dict[str, Dict[str, Any]] = {}
            for q in quotes:
                qd = _model_to_dict(q)
                sym = normalize_symbol(_safe_str(qd.get("symbol") or qd.get("requested_symbol")))
                if sym:
                    result[sym] = qd

            return result, "quote_hydration"
        except Exception as exc:
            logger.warning("quote hydration degraded for %s: %s", page, exc)
            return {}, f"quote_hydration_error:{type(exc).__name__}"

    def _prepare_external_rows_map(
        self,
        sheet: str,
        headers: List[str],
        keys: List[str],
        ext_rows: List[Dict[str, Any]],
        needed: int,
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str], List[Dict[str, Any]]]:
        by_symbol: Dict[str, Dict[str, Any]] = {}
        ordered_symbols: List[str] = []
        unsymbolled: List[Dict[str, Any]] = []

        for raw in ext_rows[:max(needed * 3, needed)]:
            sym = normalize_symbol(_extract_row_symbol(raw))
            norm_row = _normalize_to_schema_keys(keys, headers, raw)
            norm_row = _apply_page_row_backfill(sheet, norm_row)

            if sym:
                if sym not in by_symbol:
                    by_symbol[sym] = norm_row
                    ordered_symbols.append(sym)
                else:
                    by_symbol[sym] = _merge_missing_fields(by_symbol[sym], norm_row)
            else:
                unsymbolled.append(norm_row)

        return by_symbol, ordered_symbols, unsymbolled

    async def get_sheet_rows(
        self,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        started = time.time()

        target_sheet, limit, offset, mode, body, request_parts = _normalize_route_call_inputs(
            page=page, sheet=sheet, sheet_name=sheet_name,
            limit=limit, offset=offset, mode=mode, body=body, extras=kwargs,
        )

        include_matrix = _safe_bool(body.get("include_matrix"), True)
        _spec, headers, keys, schema_src = _schema_for_sheet(target_sheet)
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
            "request_input_keys": (
                sorted([str(k) for k in request_parts.keys()]) if isinstance(request_parts, dict) else []
            ),
            "rows_reader_source": self._rows_reader_source or "",
            "symbols_reader_source": self._symbols_reader_source or "",
        }

        # Fast path for schema-only requests
        if _safe_bool(body.get("schema_only"), False) or _safe_bool(body.get("headers_only"), False):
            payload = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                include_matrix=include_matrix, status="success",
                meta={
                    **base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                    "built_from": "schema_only_fast_path",
                },
            )
            _PERF_METRICS.append({
                "name": "get_sheet_rows",
                "duration_ms": round((time.time() - started) * 1000.0, 3),
                "success": True,
                "sheet": target_sheet,
            })
            return payload

        # Data_Dictionary
        if target_sheet == "Data_Dictionary":
            rows = [
                _strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r))
                for r in await self._build_data_dictionary_rows()
            ]
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows,
                include_matrix=include_matrix, status="success",
                meta={
                    **base_meta, "rows": len(rows), "limit": limit, "offset": offset, "mode": mode,
                    "built_from": "internal_data_dictionary",
                },
            )
            self._store_sheet_snapshot(target_sheet, payload_full)

            out = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys,
                row_objects=rows[offset:offset + limit], include_matrix=include_matrix,
                status="success",
                meta={**payload_full.get("meta", {}), "rows": len(rows[offset:offset + limit])},
            )
            _PERF_METRICS.append({
                "name": "get_sheet_rows",
                "duration_ms": round((time.time() - started) * 1000.0, 3),
                "success": True,
                "sheet": target_sheet,
            })
            return out

        # Insights_Analysis
        if target_sheet == "Insights_Analysis":
            rows = [
                _strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r))
                for r in await self._build_insights_rows_fallback(body, limit=max(limit + offset, 10))
            ]
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows,
                include_matrix=include_matrix, status="success" if rows else "warn",
                meta={
                    **base_meta, "rows": len(rows), "limit": limit, "offset": offset, "mode": mode,
                    "built_from": "engine_insights_fallback",
                },
            )
            self._store_sheet_snapshot(target_sheet, payload_full)

            out = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys,
                row_objects=rows[offset:offset + limit], include_matrix=include_matrix,
                status=payload_full.get("status", "success"),
                meta={**payload_full.get("meta", {}), "rows": len(rows[offset:offset + limit])},
            )
            _PERF_METRICS.append({
                "name": "get_sheet_rows",
                "duration_ms": round((time.time() - started) * 1000.0, 3),
                "success": True,
                "sheet": target_sheet,
            })
            return out

        # Top_10_Investments
        if target_sheet == "Top_10_Investments":
            out_headers, out_keys, rows = await self._build_top10_rows_fallback(
                headers, keys, body, limit=max(limit + offset, 10), mode=mode,
            )
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows,
                include_matrix=include_matrix, status="success" if rows else "warn",
                meta={
                    **base_meta, "rows": len(rows), "limit": limit, "offset": offset, "mode": mode,
                    "built_from": "top10_fallback_ranker",
                },
            )
            self._store_sheet_snapshot(target_sheet, payload_full)

            out = self._finalize_payload(
                sheet=target_sheet, headers=out_headers, keys=out_keys,
                row_objects=rows[offset:offset + limit], include_matrix=include_matrix,
                status=payload_full.get("status", "warn"),
                meta={**payload_full.get("meta", {}), "rows": len(rows[offset:offset + limit])},
            )
            _PERF_METRICS.append({
                "name": "get_sheet_rows",
                "duration_ms": round((time.time() - started) * 1000.0, 3),
                "success": True,
                "sheet": target_sheet,
            })
            return out

        # Instrument sheets
        needed = self._needed_hydration_count(limit, offset, target_sheet)
        requested_symbols = _extract_requested_symbols_from_body(body, limit=max(needed, limit + offset))
        built_from = "body_symbols" if requested_symbols else "auto_sheet_symbols"

        ext_rows: List[Dict[str, Any]] = []
        if self.rows_hydrate_external and target_sheet in INSTRUMENT_SHEETS:
            try:
                ext_rows = await self._get_rows_from_external_reader(target_sheet, limit=max(needed * 2, 20))
            except Exception:
                ext_rows = []

        ext_row_map: Dict[str, Dict[str, Any]] = {}
        ext_order: List[str] = []
        ext_unsymbolled: List[Dict[str, Any]] = []

        if ext_rows:
            ext_row_map, ext_order, ext_unsymbolled = self._prepare_external_rows_map(
                target_sheet, headers, keys, ext_rows, needed
            )
            if ext_order and not requested_symbols:
                requested_symbols = _normalize_symbol_list(
                    ext_order, limit=max(needed, limit + offset)
                )
                built_from = f"external_rows:{self._rows_reader_source or 'reader'}"

        if not requested_symbols:
            requested_symbols = await self.get_sheet_symbols(
                target_sheet, limit=max(needed, 20), body=body
            )
            if requested_symbols:
                meta = self._get_sheet_symbols_meta(target_sheet)
                built_from = _safe_str(meta.get("source")) or "sheet_symbols"

        if not requested_symbols and ext_unsymbolled:
            rows_full = [_strict_project_row(keys, row) for row in ext_unsymbolled]
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_full,
                include_matrix=include_matrix, status="partial" if rows_full else "warn",
                meta={
                    **base_meta, "rows": len(rows_full), "limit": limit, "offset": offset, "mode": mode,
                    "built_from": "external_rows_no_symbols",
                    "resolved_symbols_count": 0,
                    "external_rows_count": len(ext_rows),
                },
                error=None if rows_full else "No usable rows built",
            )
            if rows_full:
                self._store_sheet_snapshot(target_sheet, payload_full)

            out = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys,
                row_objects=rows_full[offset:offset + limit], include_matrix=include_matrix,
                status=payload_full.get("status", "partial"),
                meta={**payload_full.get("meta", {}), "rows": len(rows_full[offset:offset + limit])},
                error=payload_full.get("error"),
            )
            _PERF_METRICS.append({
                "name": "get_sheet_rows",
                "duration_ms": round((time.time() - started) * 1000.0, 3),
                "success": bool(rows_full),
                "sheet": target_sheet,
            })
            return out

        if not requested_symbols:
            requested_symbols = _normalize_symbol_list(
                EMERGENCY_PAGE_SYMBOLS.get(target_sheet, []), limit=max(needed, 5)
            )
            built_from = "emergency_page_symbols"

        quotes_by_symbol, hydration_source = await self._safe_quote_map(
            requested_symbols, target_sheet, body, needed
        )

        rows_full: List[Dict[str, Any]] = []
        ordered_symbols = _normalize_symbol_list(
            list(requested_symbols) + ext_order,
            limit=max(needed, len(requested_symbols) + len(ext_order)),
        )

        for sym in ordered_symbols[:needed]:
            base_row = ext_row_map.get(sym) or {}
            snapshot_row = self._get_best_cached_snapshot_row_for_symbol(sym, prefer_sheet=target_sheet) or {}
            quote_row = quotes_by_symbol.get(sym) or {}

            row = _normalize_to_schema_keys(keys, headers, base_row or {"symbol": sym, "requested_symbol": sym})
            row = _merge_missing_fields(
                row, _normalize_to_schema_keys(keys, headers, snapshot_row) if snapshot_row else None
            )
            row = _merge_missing_fields(
                row, _normalize_to_schema_keys(keys, headers, quote_row) if quote_row else None
            )
            row = _apply_page_row_backfill(target_sheet, row)

            if not quote_row and not base_row and snapshot_row:
                row["warnings"] = _safe_str(row.get("warnings")) or "Built from cached snapshot"
                row.setdefault("data_provider", DataSource.SNAPSHOT.value)
            elif not quote_row and base_row:
                row["warnings"] = (
                    _safe_str(row.get("warnings"))
                    or "Built from external rows with partial live hydration"
                )
                row.setdefault(
                    "data_provider",
                    _safe_str(row.get("data_provider")) or DataSource.EXTERNAL_ROWS.value,
                )
            elif not quote_row and not base_row and not snapshot_row:
                row = _merge_missing_fields(
                    row,
                    self._build_symbol_scaffold_row(
                        target_sheet, sym, warning="Built from symbol context fallback"
                    ),
                )

            _try_scoring_module(row, settings=self.settings)
            _compute_recommendation(row)
            rows_full.append(_strict_project_row(keys, row))

        if not rows_full and ext_unsymbolled:
            rows_full = [_strict_project_row(keys, row) for row in ext_unsymbolled[:needed]]

        if rows_full:
            _apply_rank_overall(rows_full)

        status_out = "success" if rows_full else "partial"
        error_out = None if rows_full else "No usable rows built"

        payload_full = self._finalize_payload(
            sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_full,
            include_matrix=include_matrix, status=status_out,
            meta={
                **base_meta, "rows": len(rows_full), "limit": limit, "offset": offset, "mode": mode,
                "built_from": built_from,
                "hydration_source": hydration_source,
                "resolved_symbols_count": len(requested_symbols),
                "hydration_target_count": needed,
                "external_rows_count": len(ext_rows),
                "snapshot_rows_available": bool(self.get_cached_sheet_snapshot(sheet=target_sheet)),
                "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet),
            },
            error=error_out,
        )

        if rows_full:
            self._store_sheet_snapshot(target_sheet, payload_full)

        out = self._finalize_payload(
            sheet=target_sheet, headers=headers, keys=keys,
            row_objects=rows_full[offset:offset + limit], include_matrix=include_matrix,
            status=payload_full.get("status", status_out),
            meta={**payload_full.get("meta", {}), "rows": len(rows_full[offset:offset + limit])},
            error=payload_full.get("error"),
        )

        _PERF_METRICS.append({
            "name": "get_sheet_rows",
            "duration_ms": round((time.time() - started) * 1000.0, 3),
            "success": bool(rows_full),
            "sheet": target_sheet,
        })
        return out

    # -----------------------------------------------------------------------
    # Compatibility aliases
    # -----------------------------------------------------------------------

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

    async def get_analysis_rows_batch(
        self, symbols: List[str], mode: str = "", schema: Any = None,
    ) -> Dict[str, Dict[str, Any]]:
        return await self.get_enriched_quotes_batch(symbols, mode=mode, schema=schema)

    async def get_analysis_row_dict(
        self, symbol: str, use_cache: bool = True, schema: Any = None,
    ) -> Dict[str, Any]:
        return await self.get_enriched_quote_dict(symbol, use_cache=use_cache, schema=schema)


# ---------------------------------------------------------------------------
# Public module-level functions
# ---------------------------------------------------------------------------


def normalize_row_to_schema(
    sheet: str,
    row: Dict[str, Any],
    keep_extras: bool = False,
) -> Dict[str, Any]:
    """Normalize a row to a sheet's schema keys."""
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


# ---------------------------------------------------------------------------
# Engine singleton (per-loop safe)
# ---------------------------------------------------------------------------

_ENGINE_INSTANCE: Optional[DataEngineV5] = None
_ENGINE_LOCK: Optional[asyncio.Lock] = None


def _get_engine_lock() -> asyncio.Lock:
    """
    Return a lock bound to the current running loop.

    Creating an asyncio.Lock() at import time binds it to whatever loop
    happens to exist then; reusing it across different loops (e.g.
    pytest-asyncio per-test loops, Starlette's lifespan loop) raises
    RuntimeError on some Python versions.
    """
    global _ENGINE_LOCK
    if _ENGINE_LOCK is None:
        _ENGINE_LOCK = asyncio.Lock()
    return _ENGINE_LOCK


async def get_engine() -> DataEngineV5:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE is None:
        async with _get_engine_lock():
            if _ENGINE_INSTANCE is None:
                _ENGINE_INSTANCE = DataEngineV5()
    return _ENGINE_INSTANCE


async def close_engine() -> None:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE is not None:
        try:
            await _ENGINE_INSTANCE.aclose()
        except Exception:
            pass
    _ENGINE_INSTANCE = None


def get_engine_if_ready() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def peek_engine() -> Optional[DataEngineV5]:
    return _ENGINE_INSTANCE


def get_cache() -> Any:
    return getattr(_ENGINE_INSTANCE, "_cache", None)


def _run_coro_sync(coro: Any) -> Any:
    """Run coroutine synchronously. Raises if called inside an active loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("Synchronous engine helper cannot run inside an active event loop")


def get_engine_sync() -> DataEngineV5:
    return _run_coro_sync(get_engine())


async def get_quote(symbol: str, use_cache: bool = True, **kwargs: Any) -> Any:
    engine = await get_engine()
    return await engine.get_enriched_quote(symbol, use_cache=use_cache, **kwargs)


async def get_quotes(symbols: List[str], use_cache: bool = True, **kwargs: Any) -> List[Any]:
    engine = await get_engine()
    return await engine.get_enriched_quotes(symbols, use_cache=use_cache, **kwargs)


async def get_enriched_quote(symbol: str, use_cache: bool = True, **kwargs: Any) -> Any:
    return await get_quote(symbol, use_cache=use_cache, **kwargs)


async def get_enriched_quotes(symbols: List[str], use_cache: bool = True, **kwargs: Any) -> List[Any]:
    return await get_quotes(symbols, use_cache=use_cache, **kwargs)


async def get_sheet_rows(
    sheet: str,
    limit: int = 2000,
    offset: int = 0,
    mode: str = "",
    body: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    engine = await get_engine()
    return await engine.get_sheet_rows(
        sheet, limit=limit, offset=offset, mode=mode, body=body, **kwargs,
    )


def get_sheet_rows_sync(
    sheet: str,
    limit: int = 2000,
    offset: int = 0,
    mode: str = "",
    body: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    return _run_coro_sync(
        get_sheet_rows(sheet, limit=limit, offset=offset, mode=mode, body=body, **kwargs)
    )


async def process_batch(
    symbols: List[str],
    use_cache: bool = True,
    progress_callback: Optional[Callable[[BatchProgress], None]] = None,
    batch_size: int = 25,
    delay_seconds: float = 0.0,
    max_retries: int = 0,
    **kwargs: Any,
) -> List[Any]:
    clean = _normalize_symbol_list(symbols, limit=len(symbols) + 10)
    progress = BatchProgress(total=len(clean))
    results: List[Any] = []

    for i in range(0, len(clean), max(1, batch_size)):
        batch = clean[i:i + max(1, batch_size)]

        try:
            batch_results = await get_enriched_quotes(batch, use_cache=use_cache, **kwargs)
            results.extend(batch_results)
            progress.completed += len(batch)
            progress.succeeded += len(batch)
        except Exception as exc:
            for sym in batch:
                results.append(StubUnifiedQuote(symbol=sym, error=str(exc)).finalize())
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

    return results


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
        await get_engine()
        health["checks"]["engine"] = "ready"
        health["checks"]["cache"] = bool(get_cache() is not None)
        health["checks"]["schema_source"] = "schema_registry" if SCHEMA_REGISTRY else "fallback"
        health["checks"]["scoring_module"] = _safe_str(
            getattr(_get_scoring_mod(), "__name__", "")
        ) or "fallback"
        health["checks"]["scoring_critical_fields"] = list(_SCORING_CRITICAL_FIELDS)

        try:
            sr = await get_sheet_rows(
                "Data_Dictionary", limit=1, offset=0, mode="",
                body={"include_matrix": False},
            )
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
async def engine_context() -> AsyncGenerator[DataEngineV5, None]:
    engine = await get_engine()
    try:
        yield engine
    finally:
        await close_engine()


class EngineSession:
    """Engine session for synchronous usage."""

    def __enter__(self) -> DataEngineV5:
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
        "enabled_providers": (
            list(getattr(inst, "enabled_providers", []))
            if inst is not None
            else _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        ),
        "ksa_providers": (
            list(getattr(inst, "ksa_providers", []))
            if inst is not None
            else _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        ),
        "global_providers": (
            list(getattr(inst, "global_providers", []))
            if inst is not None
            else _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)
        ),
        "schema_strict_sheet_rows": (
            bool(getattr(inst, "schema_strict_sheet_rows",
                          _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)))
            if inst is not None
            else _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)
        ),
        "top10_force_full_schema": (
            bool(getattr(inst, "top10_force_full_schema",
                          _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True)))
            if inst is not None
            else _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True)
        ),
        "sheet_rows_time_budget": (
            float(getattr(inst, "sheet_rows_time_budget",
                           _get_env_float("SHEET_ROWS_TIME_BUDGET_SECONDS", 12.0)))
            if inst is not None
            else _get_env_float("SHEET_ROWS_TIME_BUDGET_SECONDS", 12.0)
        ),
        "sheet_rows_hydration_cap": (
            int(getattr(inst, "sheet_rows_hydration_cap",
                         _get_env_int("SHEET_ROWS_HYDRATION_CAP", 30)))
            if inst is not None
            else _get_env_int("SHEET_ROWS_HYDRATION_CAP", 30)
        ),
        "schema_source": "schema_registry" if SCHEMA_REGISTRY else "fallback",
        "scoring_critical_fields": list(_SCORING_CRITICAL_FIELDS),
        "perf_stats": get_perf_stats(),
        "rows_reader_source": getattr(inst, "_rows_reader_source", "") if inst is not None else "",
        "symbols_reader_source": getattr(inst, "_symbols_reader_source", "") if inst is not None else "",
    }


# ---------------------------------------------------------------------------
# Backward compatibility aliases + exports
# ---------------------------------------------------------------------------

DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5
DataEngine = DataEngineV5

# Global instances (legacy module-level names — filled by first engine build)
ENGINE: Optional[DataEngineV5] = None
engine: Optional[DataEngineV5] = None
_ENGINE: Optional[DataEngineV5] = None

__all__ = [
    # Engine classes
    "DataEngineV5", "DataEngineV4", "DataEngineV3", "DataEngineV2", "DataEngine",
    "ENGINE", "engine", "_ENGINE",
    # Engine lifecycle
    "get_engine", "get_engine_if_ready", "peek_engine", "close_engine", "get_cache",
    "get_engine_sync", "get_engine_meta",
    # Types
    "QuoteQuality", "DataSource", "UnifiedQuote", "StubUnifiedQuote",
    "SymbolInfo", "BatchProgress", "PerfMetrics",
    # Symbol helpers
    "normalize_symbol", "get_symbol_info",
    # Quotes + rows
    "get_quote", "get_quotes", "get_enriched_quote", "get_enriched_quotes",
    "get_sheet_rows", "get_sheet_rows_sync", "process_batch", "health_check",
    # Contexts
    "engine_context", "EngineSession",
    # Stubs (public API compat)
    "DistributedCache", "DynamicCircuitBreaker", "TokenBucket",
    # Metrics
    "get_perf_metrics", "get_perf_stats", "reset_perf_metrics",
    "MetricsRegistry", "_METRICS",
    # Schema helpers
    "normalize_row_to_schema", "get_sheet_spec",
    # Schema constants
    "INSTRUMENT_CANONICAL_KEYS", "INSTRUMENT_CANONICAL_HEADERS",
    "INSIGHTS_KEYS", "INSIGHTS_HEADERS",
    "DATA_DICTIONARY_KEYS", "DATA_DICTIONARY_HEADERS",
    "TOP10_REQUIRED_FIELDS", "TOP10_REQUIRED_HEADERS",
    "STATIC_CANONICAL_SHEET_CONTRACTS",
    "INSTRUMENT_SHEETS", "SPECIAL_SHEETS",
    "TOP10_ENGINE_DEFAULT_PAGES",
    "EMERGENCY_PAGE_SYMBOLS", "PAGE_SYMBOL_ENV_KEYS",
    "DEFAULT_PROVIDERS", "DEFAULT_KSA_PROVIDERS", "DEFAULT_GLOBAL_PROVIDERS",
    "PROVIDER_PRIORITIES",
    # Version
    "__version__", "VERSION",
]

# Initialize metrics (no-op with the stub registry)
try:
    _METRICS.set("active_requests", 0)
except Exception:
    pass
