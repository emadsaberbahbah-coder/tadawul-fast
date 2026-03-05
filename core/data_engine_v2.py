#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 — GLOBAL-FIRST ORCHESTRATOR — v5.8.0 (PHASE D — ADVANCED FIELDS)
================================================================================

WHY v5.8.0 (Phase D: eliminate missing advanced fields across Global + KSA)

This revision makes the engine the *actual* source of truth for advanced fields:
- ✅ Risk stats computed from history (when not provided):
    volatility_90d, max_drawdown_1y, var_95_1d, sharpe_1y
- ✅ Valuation fields mapped + (best-effort) computed:
    pb_ratio, ps_ratio, peg_ratio, intrinsic_value, valuation_score
- ✅ Score fields + buckets ensured (even if upstream providers are partial):
    value_score, quality_score, momentum_score, growth_score,
    overall_score, opportunity_score, rank_overall,
    confidence_bucket, risk_bucket
- ✅ KSA classification guaranteed baseline:
    country, sector, industry (country forced to SAU for KSA symbols)

Provider routing constraint (your rule):
- ✅ GLOBAL: EODHD is primary (subscription) + Yahoo/Finnhub as fallback
- ✅ KSA: Tadawul (price) + Argaam (classification/profile enrichment) + Yahoo chart (history/risk)

Other hardening kept:
- ✅ SPECIAL DISPATCH map in sheet-rows so Insights_Analysis / Top_10_Investments / Data_Dictionary
  never fall back to the 80-col instrument schema.
- ✅ Schema-first enforcement for headers/keys/rows.
- ✅ Startup-safe: no network IO at import time; heavy imports are lazy inside calls.
- ✅ JSON-safe + best-effort compatibility with multiple repo variants.

================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import pickle
import re
import sys
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

# ---------------------------------------------------------------------------
# Ensure repo root is importable (fixes "No module named 'providers'")
# core/data_engine_v2.py -> core/ -> ROOT
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__version__ = "5.8.0"

logger = logging.getLogger("core.data_engine_v2")
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Fast JSON (orjson optional)
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj, default=str).decode("utf-8")

except Exception:
    import json  # type: ignore

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, default=str, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Schema Registry (Phase 1)
# ---------------------------------------------------------------------------
try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY, get_sheet_spec  # type: ignore

    _SCHEMA_AVAILABLE = True
except Exception:
    SCHEMA_REGISTRY = {}  # type: ignore
    _SCHEMA_AVAILABLE = False

    def get_sheet_spec(_: str) -> Any:  # type: ignore
        raise KeyError("schema_registry not available")


# ---------------------------------------------------------------------------
# Canonical engine keys (fallback union, even if schema isn't importable yet)
# ---------------------------------------------------------------------------
DEFAULT_ENGINE_KEYS: List[str] = [
    # identity
    "symbol",
    "symbol_normalized",
    "requested_symbol",
    "name",
    "exchange",
    "currency",
    "asset_class",
    "country",
    "sector",
    "industry",
    # prices / liquidity
    "current_price",
    "price",  # alias
    "previous_close",
    "open_price",
    "day_high",
    "day_low",
    "week_52_high",
    "week_52_low",
    "volume",
    "avg_volume_10d",
    "avg_volume_30d",
    "market_cap",
    "float_shares",
    "beta_5y",
    "price_change",
    "percent_change",
    "week_52_position_pct",
    "change",      # alias
    "change_pct",  # alias
    # fundamentals
    "pe_ttm",
    "pe_forward",
    "eps_ttm",
    "dividend_yield",
    "payout_ratio",
    "revenue_ttm",
    "revenue_growth_yoy",
    "gross_margin",
    "operating_margin",
    "profit_margin",
    "debt_to_equity",
    "free_cash_flow_ttm",
    # risk stats
    "rsi_14",
    "volatility_30d",
    "volatility_90d",
    "max_drawdown_1y",
    "var_95_1d",
    "sharpe_1y",
    "risk_score",
    "risk_bucket",
    # valuation
    "pb_ratio",
    "ps_ratio",
    "ev_ebitda",
    "peg_ratio",
    "intrinsic_value",
    "valuation_score",
    # forecast / roi
    "forecast_price_1m",
    "forecast_price_3m",
    "forecast_price_12m",
    "expected_roi_1m",
    "expected_roi_3m",
    "expected_roi_12m",
    "forecast_confidence",
    "confidence_score",
    "confidence_bucket",
    # scores
    "value_score",
    "quality_score",
    "momentum_score",
    "growth_score",
    "overall_score",
    "opportunity_score",
    "rank_overall",
    # recommendation
    "recommendation",
    "recommendation_reason",
    "horizon_days",
    "invest_period_label",
    # portfolio
    "position_qty",
    "avg_cost",
    "position_cost",
    "position_value",
    "unrealized_pl",
    "unrealized_pl_pct",
    # meta / provenance
    "data_provider",
    "data_quality",
    "error",
    "warning",
    "warnings",
    "info",
    "data_sources",
    "provider_latency",
    "last_updated_utc",
    "last_updated_riyadh",
]


def _build_union_schema_keys() -> List[str]:
    """
    Union of keys across all sheets in SCHEMA_REGISTRY, preserving stable order.
    Also appends DEFAULT_ENGINE_KEYS as a safety net.

    NOTE: This is ONLY a fallback for non-schema scenarios.
    """
    keys: List[str] = []
    seen = set()

    if isinstance(SCHEMA_REGISTRY, dict) and SCHEMA_REGISTRY:
        for _, spec in SCHEMA_REGISTRY.items():
            cols = getattr(spec, "columns", None) or []
            for c in cols:
                k = getattr(c, "key", None)
                if not k:
                    continue
                k = str(k).strip()
                if k and k not in seen:
                    seen.add(k)
                    keys.append(k)

    for k in DEFAULT_ENGINE_KEYS:
        if k not in seen:
            seen.add(k)
            keys.append(k)

    return keys


_SCHEMA_UNION_KEYS: List[str] = _build_union_schema_keys()


def normalize_row_to_schema(schema: Any, rowdict: Dict[str, Any], *, keep_extras: bool = True) -> Dict[str, Any]:
    """
    Normalize a row dict to a schema, guaranteeing all keys exist.

    `schema` can be:
      - sheet name (str) -> uses schema_registry
      - sheet spec object (has .columns with .key)
      - list/tuple of keys
      - None -> uses union schema across all sheets (+ DEFAULT_ENGINE_KEYS)

    Returns: dict with ALL schema keys, missing => None.
    """
    keys: List[str] = []

    if schema is None:
        keys = list(_SCHEMA_UNION_KEYS)

    elif isinstance(schema, str):
        try:
            spec = get_sheet_spec(schema)
            keys = [str(getattr(c, "key", "")).strip() for c in (getattr(spec, "columns", None) or [])]
            keys = [k for k in keys if k]
        except Exception:
            keys = list(_SCHEMA_UNION_KEYS)

    elif isinstance(schema, (list, tuple)):
        keys = [str(k).strip() for k in schema if str(k).strip()]

    else:
        cols = getattr(schema, "columns", None)
        if cols:
            keys = [str(getattr(c, "key", "")).strip() for c in cols if str(getattr(c, "key", "")).strip()]
        else:
            keys = list(_SCHEMA_UNION_KEYS)

    raw = rowdict or {}
    out: Dict[str, Any] = {k: raw.get(k, None) for k in keys}

    if keep_extras:
        for k, v in raw.items():
            if k not in out:
                out[k] = v

    return out


# ---------------------------------------------------------------------------
# Pydantic / dataclass safe detection
# ---------------------------------------------------------------------------
def _model_to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
        try:
            return obj.model_dump(mode="python")
        except Exception:
            try:
                return obj.model_dump()
            except Exception:
                return {}
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        try:
            return obj.dict()
        except Exception:
            return {}
    if hasattr(obj, "__dict__"):
        try:
            return dict(obj.__dict__)
        except Exception:
            pass
    return {"result": obj}


# ---------------------------------------------------------------------------
# UnifiedQuote import (fallback includes Phase D fields)
# ---------------------------------------------------------------------------
try:
    from core.schemas import UnifiedQuote  # type: ignore

    SCHEMAS_AVAILABLE = True
except Exception:
    SCHEMAS_AVAILABLE = False
    try:
        from pydantic import BaseModel, Field  # type: ignore
    except Exception:  # pragma: no cover
        BaseModel = object  # type: ignore

        def Field(default=None, **kwargs):  # type: ignore
            return default

    class UnifiedQuote(BaseModel):  # type: ignore
        # identity
        symbol: str = Field(default="")
        symbol_normalized: Optional[str] = None
        requested_symbol: Optional[str] = None
        name: Optional[str] = None
        exchange: Optional[str] = None
        currency: Optional[str] = None
        asset_class: Optional[str] = None
        country: Optional[str] = None
        sector: Optional[str] = None
        industry: Optional[str] = None

        # price/liquidity
        current_price: Optional[float] = None
        price: Optional[float] = None
        previous_close: Optional[float] = None
        open_price: Optional[float] = None
        day_high: Optional[float] = None
        day_low: Optional[float] = None
        week_52_high: Optional[float] = None
        week_52_low: Optional[float] = None
        volume: Optional[float] = None
        avg_volume_10d: Optional[float] = None
        avg_volume_30d: Optional[float] = None
        market_cap: Optional[float] = None
        float_shares: Optional[float] = None
        beta_5y: Optional[float] = None
        price_change: Optional[float] = None
        percent_change: Optional[float] = None
        week_52_position_pct: Optional[float] = None
        change: Optional[float] = None
        change_pct: Optional[float] = None

        # fundamentals
        pe_ttm: Optional[float] = None
        pe_forward: Optional[float] = None
        eps_ttm: Optional[float] = None
        dividend_yield: Optional[float] = None
        payout_ratio: Optional[float] = None
        revenue_ttm: Optional[float] = None
        revenue_growth_yoy: Optional[float] = None
        gross_margin: Optional[float] = None
        operating_margin: Optional[float] = None
        profit_margin: Optional[float] = None
        debt_to_equity: Optional[float] = None
        free_cash_flow_ttm: Optional[float] = None

        # risk stats
        rsi_14: Optional[float] = None
        volatility_30d: Optional[float] = None
        volatility_90d: Optional[float] = None
        max_drawdown_1y: Optional[float] = None
        var_95_1d: Optional[float] = None
        sharpe_1y: Optional[float] = None
        risk_score: Optional[float] = None
        risk_bucket: Optional[str] = None

        # valuation
        pb_ratio: Optional[float] = None
        ps_ratio: Optional[float] = None
        ev_ebitda: Optional[float] = None
        peg_ratio: Optional[float] = None
        intrinsic_value: Optional[float] = None
        valuation_score: Optional[float] = None

        # forecast/roi
        forecast_price_1m: Optional[float] = None
        forecast_price_3m: Optional[float] = None
        forecast_price_12m: Optional[float] = None
        expected_roi_1m: Optional[float] = None
        expected_roi_3m: Optional[float] = None
        expected_roi_12m: Optional[float] = None
        forecast_confidence: Optional[float] = None
        confidence_score: Optional[float] = None
        confidence_bucket: Optional[str] = None

        # scores
        value_score: Optional[float] = None
        quality_score: Optional[float] = None
        momentum_score: Optional[float] = None
        growth_score: Optional[float] = None
        overall_score: Optional[float] = None
        opportunity_score: Optional[float] = None
        rank_overall: Optional[int] = None

        # recommendation
        recommendation: Optional[str] = None
        recommendation_reason: Optional[str] = None
        horizon_days: Optional[int] = None
        invest_period_label: Optional[str] = None

        # portfolio
        position_qty: Optional[float] = None
        avg_cost: Optional[float] = None
        position_cost: Optional[float] = None
        position_value: Optional[float] = None
        unrealized_pl: Optional[float] = None
        unrealized_pl_pct: Optional[float] = None

        # meta
        data_provider: Optional[str] = None
        data_quality: str = "MISSING"
        error: Optional[str] = None
        warning: Optional[str] = None
        warnings: Optional[str] = None
        info: Optional[Any] = None
        data_sources: Optional[List[str]] = None
        provider_latency: Optional[Dict[str, float]] = None
        last_updated_utc: Optional[str] = None
        last_updated_riyadh: Optional[str] = None

        class Config:
            extra = "allow"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class QuoteQuality(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    MISSING = "MISSING"


class DataSource(str, Enum):
    CACHE = "cache"
    PRIMARY = "primary"
    FALLBACK = "fallback"
    ENRICHMENT = "enrichment"


# ---------------------------------------------------------------------------
# Symbol normalization (supports BOTH layouts)
# - symbols/normalize.py
# - core/symbols/normalize.py
# ---------------------------------------------------------------------------
def _fallback_is_ksa(s: str) -> bool:
    u = (s or "").strip().upper()
    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()
    if u.endswith(".SR"):
        code = u[:-3].strip()
        return code.isdigit() and 3 <= len(code) <= 6
    return u.isdigit() and 3 <= len(u) <= 6


def _fallback_normalize_symbol(s: str) -> str:
    u = (s or "").strip().upper()
    if not u:
        return ""
    if u.startswith("TADAWUL:"):
        u = u.split(":", 1)[1].strip()
    if _fallback_is_ksa(u):
        if u.endswith(".SR"):
            code = u[:-3].strip()
            return f"{code}.SR"
        if u.isdigit():
            return f"{u}.SR"
    return u


def _fallback_to_yahoo_symbol(s: str) -> str:
    u = _fallback_normalize_symbol(s)
    if _fallback_is_ksa(u):
        return u if u.endswith(".SR") else f"{u}.SR"
    return u


try:
    from symbols.normalize import normalize_symbol as normalize_symbol  # type: ignore
    from symbols.normalize import is_ksa as is_ksa  # type: ignore

    try:
        from symbols.normalize import to_yahoo_symbol as to_yahoo_symbol  # type: ignore
    except Exception:
        to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore

except Exception:
    try:
        from core.symbols.normalize import normalize_symbol as normalize_symbol  # type: ignore
        from core.symbols.normalize import is_ksa as is_ksa  # type: ignore

        try:
            from core.symbols.normalize import to_yahoo_symbol as to_yahoo_symbol  # type: ignore
        except Exception:
            to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore

    except Exception:
        normalize_symbol = _fallback_normalize_symbol  # type: ignore
        is_ksa = _fallback_is_ksa  # type: ignore
        to_yahoo_symbol = _fallback_to_yahoo_symbol  # type: ignore


def get_symbol_info(symbol: str) -> Dict[str, Any]:
    norm = normalize_symbol(symbol) if callable(normalize_symbol) else _fallback_normalize_symbol(symbol)
    ksa = bool(is_ksa(norm)) if callable(is_ksa) else _fallback_is_ksa(norm)
    return {"raw": symbol, "normalized": norm, "market": "KSA" if ksa else "GLOBAL", "is_ksa": ksa}


# ---------------------------------------------------------------------------
# Settings loader (optional)
# ---------------------------------------------------------------------------
def _try_get_settings() -> Any:
    try:
        from core.config import get_settings_cached  # type: ignore

        return get_settings_cached()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------
def _get_env_list(key: str, default: str) -> List[str]:
    raw = os.getenv(key, default) or default
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


def _get_env_int(key: str, default: int) -> int:
    try:
        return int(float(os.getenv(key, str(default))))
    except Exception:
        return default


def _get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _get_env_str(key: str, default: str = "") -> str:
    return (os.getenv(key, default) or default).strip()


def _get_env_bool(key: str, default: bool = False) -> bool:
    raw = (os.getenv(key) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on", "t"}


def _feature_flags(settings: Any) -> Dict[str, bool]:
    def _get(name: str, env: str, default: bool) -> bool:
        if settings is not None and hasattr(settings, name):
            try:
                return bool(getattr(settings, name))
            except Exception:
                return default
        return _get_env_bool(env, default)

    # Phase D: risk stats + scoring must be on by default
    return {
        "computations_enabled": _get("computations_enabled", "COMPUTATIONS_ENABLED", True),
        "fundamentals_enabled": _get("fundamentals_enabled", "FUNDAMENTALS_ENABLED", True),
        "technicals_enabled": _get("technicals_enabled", "TECHNICALS_ENABLED", True),
        "forecasting_enabled": _get("forecasting_enabled", "FORECASTING_ENABLED", True),
        "scoring_enabled": _get("scoring_enabled", "SCORING_ENABLED", True),
        "risk_stats_enabled": _get("risk_stats_enabled", "RISK_STATS_ENABLED", True),
        "valuation_enabled": _get("valuation_enabled", "VALUATION_ENABLED", True),
    }


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(timezone(timedelta(hours=3))).isoformat()


# ---------------------------------------------------------------------------
# Numeric helpers for pct fields (store as fraction whenever possible)
# ---------------------------------------------------------------------------
def _as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _as_pct_fraction(x: Any) -> Optional[float]:
    """
    Normalize percent-like inputs to FRACTION (0.12 == 12%).
    Accepts:
      12.0    -> 0.12
      "12%"   -> 0.12
      0.12    -> 0.12
      "0.12"  -> 0.12
    """
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        if s.endswith("%"):
            v = _as_float(s[:-1].strip())
            return (v / 100.0) if v is not None else None
        v = _as_float(s.replace(",", ""))
        if v is None:
            return None
        # heuristic: values > 1.5 are likely percent
        return (v / 100.0) if abs(v) > 1.5 else v

    v = _as_float(x)
    if v is None:
        return None
    return (v / 100.0) if abs(v) > 1.5 else v


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# ---------------------------------------------------------------------------
# Patch helpers + key aliasing (Phase D extended)
# ---------------------------------------------------------------------------
def _clean_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (patch or {}).items() if v is not None and v != ""}


_PATCH_ALIASES: Dict[str, str] = {
    # price
    "last_price": "current_price",
    "last": "current_price",
    "price": "current_price",
    "prev_close": "previous_close",
    "close": "previous_close",
    "open": "open_price",
    "high": "day_high",
    "low": "day_low",
    "52w_high": "week_52_high",
    "high_52w": "week_52_high",
    "52w_low": "week_52_low",
    "low_52w": "week_52_low",
    "change": "price_change",
    "price_diff": "price_change",
    "pct_change": "percent_change",
    "percent_change": "percent_change",
    "change_percent": "percent_change",
    "change_pct": "percent_change",

    # identity/classification
    "company_name": "name",
    "long_name": "name",
    "instrument_name": "name",
    "mic": "exchange",
    "market": "exchange",
    "ccy": "currency",
    "country_name": "country",
    "sector_name": "sector",
    "industry_name": "industry",
    "gics_sector": "sector",
    "gics_industry": "industry",

    # valuation ratios
    "pb": "pb_ratio",
    "p_b": "pb_ratio",
    "price_to_book": "pb_ratio",
    "price_to_book_ratio": "pb_ratio",
    "ps": "ps_ratio",
    "p_s": "ps_ratio",
    "price_to_sales": "ps_ratio",
    "price_to_sales_ratio": "ps_ratio",
    "peg": "peg_ratio",
    "peg": "peg_ratio",
    "peg_ratio": "peg_ratio",
    "intrinsic": "intrinsic_value",
    "intrinsic_val": "intrinsic_value",
    "intrinsic_price": "intrinsic_value",
    "ev_to_ebitda": "ev_ebitda",

    # margins/growth
    "rev_ttm": "revenue_ttm",
    "revenue": "revenue_ttm",
    "revenue_growth": "revenue_growth_yoy",
    "revenue_growth_pct": "revenue_growth_yoy",
    "gross_margin_pct": "gross_margin",
    "operating_margin_pct": "operating_margin",
    "profit_margin_pct": "profit_margin",

    # risk fields
    "volatility90d": "volatility_90d",
    "volatility_90": "volatility_90d",
    "max_drawdown": "max_drawdown_1y",
    "drawdown_1y": "max_drawdown_1y",
    "var95_1d": "var_95_1d",
    "sharpe": "sharpe_1y",
    "sharpe_ratio": "sharpe_1y",

    # scores/buckets
    "conf_score": "confidence_score",
    "confidence": "forecast_confidence",
    "confidence_bucket": "confidence_bucket",
    "risk_bucket": "risk_bucket",
}


def _normalize_patch_keys(patch: Dict[str, Any]) -> Dict[str, Any]:
    if not patch:
        return {}
    out = dict(patch)

    for src, dst in _PATCH_ALIASES.items():
        if src in out and (dst not in out or out.get(dst) in (None, "")):
            out[dst] = out.get(src)

    # ensure main aliases are present
    if "current_price" in out and ("price" not in out or out.get("price") in (None, "")):
        out["price"] = out.get("current_price")
    if "price" in out and ("current_price" not in out or out.get("current_price") in (None, "")):
        out["current_price"] = out.get("price")

    if "price_change" in out and ("change" not in out or out.get("change") in (None, "")):
        out["change"] = out.get("price_change")
    if "percent_change" in out and ("change_pct" not in out or out.get("change_pct") in (None, "")):
        out["change_pct"] = out.get("percent_change")

    # normalize pct-like fields to fraction where reasonable
    for k in (
        "percent_change",
        "change_pct",
        "dividend_yield",
        "payout_ratio",
        "revenue_growth_yoy",
        "gross_margin",
        "operating_margin",
        "profit_margin",
        "volatility_30d",
        "volatility_90d",
        "max_drawdown_1y",
        "var_95_1d",
        "expected_roi_1m",
        "expected_roi_3m",
        "expected_roi_12m",
    ):
        if k in out and out.get(k) is not None:
            pv = _as_pct_fraction(out.get(k))
            if pv is not None:
                out[k] = pv

    # confidence normalization
    for k in ("forecast_confidence", "confidence_score"):
        if k in out and out.get(k) is not None:
            v = _as_float(out.get(k))
            if v is not None:
                out[k] = (v / 100.0) if v > 1.5 else v

    return out


def _is_useful_patch(p: Dict[str, Any]) -> bool:
    if not isinstance(p, dict) or not p:
        return False
    if _as_float(p.get("current_price")) is not None:
        return True
    if (str(p.get("name") or "")).strip():
        return True
    # classification can also be useful for KSA completeness
    if (str(p.get("sector") or "")).strip() or (str(p.get("industry") or "")).strip():
        return True
    return False


# ============================================================================
# Provider configuration (GLOBAL-first + KSA constraints)
# ============================================================================
DEFAULT_PROVIDERS = "tadawul,argaam,yahoo_chart,yahoo_fundamentals,finnhub,eodhd"
DEFAULT_KSA_PROVIDERS = "tadawul,argaam,yahoo_chart"  # KSA: Tadawul + Argaam + Yahoo history
DEFAULT_GLOBAL_PROVIDERS = "eodhd,yahoo_fundamentals,yahoo_chart,finnhub"  # GLOBAL: EODHD primary

PROVIDER_PRIORITIES = {
    "eodhd": 10,
    "tadawul": 20,
    "argaam": 30,
    "yahoo_fundamentals": 40,
    "yahoo_chart": 50,
    "finnhub": 60,
}

PROVIDER_MODULE_CANDIDATES: Dict[str, List[str]] = {
    "tadawul": ["providers.tadawul_provider", "core.providers.tadawul_provider"],
    "argaam": ["providers.argaam_provider", "core.providers.argaam_provider"],
    "yahoo_chart": ["providers.yahoo_chart_provider", "core.providers.yahoo_chart_provider"],
    "yahoo_fundamentals": ["providers.yahoo_fundamentals_provider", "core.providers.yahoo_fundamentals_provider"],
    "finnhub": ["providers.finnhub_provider", "core.providers.finnhub_provider"],
    "eodhd": ["providers.eodhd_provider", "core.providers.eodhd_provider"],
}

PROVIDER_FUNCTIONS: Dict[str, List[str]] = {
    "tadawul": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "argaam": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "yahoo_chart": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "yahoo_fundamentals": ["fetch_fundamentals_patch", "fetch_enriched_quote_patch", "fetch_patch"],
    "finnhub": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
    "eodhd": ["fetch_enriched_quote_patch", "fetch_quote_patch", "fetch_patch"],
}

# For risk stats (history). We do best-effort function probing.
HISTORY_FUNCTIONS: Dict[str, List[str]] = {
    "eodhd": ["fetch_history", "fetch_price_history", "fetch_daily_history", "fetch_chart_history", "get_history"],
    "yahoo_chart": ["fetch_history", "fetch_price_history", "fetch_chart_history", "get_history", "fetch_chart"],
    "tadawul": ["fetch_history", "fetch_price_history", "fetch_chart_history", "get_history"],
    "argaam": ["fetch_history", "fetch_price_history", "fetch_chart_history", "get_history"],
    "finnhub": ["fetch_history", "fetch_price_history", "fetch_daily_history", "get_history"],
}


def _import_provider(provider_name: str) -> Tuple[Optional[Any], Optional[str]]:
    last_err: Optional[str] = None
    for module_path in PROVIDER_MODULE_CANDIDATES.get(provider_name, []):
        try:
            return import_module(module_path), None
        except Exception as e:
            last_err = f"{module_path}: {e!r}"
    return None, last_err or "no candidates"


def _pick_provider_callable(module: Any, provider_name: str) -> Optional[Callable]:
    for fn_name in PROVIDER_FUNCTIONS.get(provider_name, ["fetch_enriched_quote_patch"]):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            return fn
    return None


async def _call_maybe_async(fn: Callable, *args, **kwargs) -> Any:
    out = fn(*args, **kwargs)
    if asyncio.iscoroutine(out) or asyncio.isfuture(out) or hasattr(out, "__await__"):
        return await out
    return out


# ============================================================================
# Provider Stats / Registry
# ============================================================================
@dataclass(slots=True)
class ProviderStats:
    name: str
    success_count: int = 0
    failure_count: int = 0
    total_latency_ms: float = 0.0
    consecutive_failures: int = 0
    circuit_open_until: Optional[datetime] = None
    last_error: Optional[str] = None
    last_import_error: Optional[str] = None
    last_import_attempt_utc: float = 0.0

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.success_count if self.success_count > 0 else 0.0

    @property
    def success_rate(self) -> float:
        t = self.success_count + self.failure_count
        return self.success_count / t if t > 0 else 1.0

    @property
    def is_circuit_open(self) -> bool:
        if not self.circuit_open_until:
            return False
        return datetime.now(timezone.utc) < self.circuit_open_until

    def record_success(self, latency_ms: float) -> None:
        self.success_count += 1
        self.total_latency_ms += float(latency_ms or 0.0)
        self.consecutive_failures = 0
        self.circuit_open_until = None
        self.last_error = None

    def record_failure(self, err: str) -> None:
        self.failure_count += 1
        self.consecutive_failures += 1
        self.last_error = err
        threshold = _get_env_int("PROVIDER_CIRCUIT_BREAKER_THRESHOLD", 5)
        cooldown = _get_env_int("PROVIDER_CIRCUIT_BREAKER_COOLDOWN", 60)
        if self.consecutive_failures >= threshold:
            self.circuit_open_until = datetime.now(timezone.utc) + timedelta(seconds=cooldown)


class ProviderRegistry:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._providers: Dict[str, Tuple[Optional[Any], ProviderStats]] = {}

    async def get_provider(self, name: str) -> Tuple[Optional[Any], ProviderStats]:
        retry_sec = _get_env_int("PROVIDER_IMPORT_RETRY_SEC", 60)
        now = time.time()

        async with self._lock:
            if name not in self._providers:
                module, import_err = _import_provider(name)
                stats = ProviderStats(name=name, last_import_error=import_err, last_import_attempt_utc=now)
                self._providers[name] = (module, stats)
                return self._providers[name]

            module, stats = self._providers[name]

            if module is None and stats.last_import_error and (now - stats.last_import_attempt_utc) >= retry_sec:
                module2, import_err2 = _import_provider(name)
                stats.last_import_error = import_err2
                stats.last_import_attempt_utc = now
                self._providers[name] = (module2, stats)
                module = module2

            return module, stats

    async def record_success(self, name: str, latency_ms: float) -> None:
        async with self._lock:
            if name in self._providers:
                self._providers[name][1].record_success(latency_ms)

    async def record_failure(self, name: str, err: str) -> None:
        async with self._lock:
            if name in self._providers:
                self._providers[name][1].record_failure(err)

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            out: Dict[str, Any] = {}
            for name, (_, s) in self._providers.items():
                out[name] = {
                    "success": s.success_count,
                    "failure": s.failure_count,
                    "avg_latency_ms": round(s.avg_latency_ms, 2),
                    "success_rate": round(s.success_rate, 3),
                    "circuit_open": s.is_circuit_open,
                    "last_error": s.last_error,
                    "last_import_error": s.last_import_error,
                }
            return out


# ============================================================================
# Cache (L1 memory + disk)
# ============================================================================
class MultiLevelCache:
    def __init__(self, name: str, l1_ttl: int = 60, l3_ttl: int = 3600, max_l1_size: int = 5000):
        self.name = name
        self.l1_ttl = max(1, int(l1_ttl))
        self.l3_ttl = max(1, int(l3_ttl))
        self.max_l1_size = max(128, int(max_l1_size))
        self._l1: Dict[str, Tuple[Any, float]] = {}
        self._l1_access: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._dir = os.path.join("/tmp", f"cache_{name}")
        os.makedirs(self._dir, exist_ok=True)

    def _key(self, **kwargs) -> str:
        payload = json_dumps(kwargs)
        h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        return f"{self.name}:{h}"

    def _compress(self, data: Any) -> bytes:
        try:
            return zlib.compress(pickle.dumps(data), level=6)
        except Exception:
            return pickle.dumps(data)

    def _decompress(self, data: bytes) -> Any:
        try:
            return pickle.loads(zlib.decompress(data))
        except Exception:
            try:
                return pickle.loads(data)
            except Exception:
                return None

    async def get(self, **kwargs) -> Optional[Any]:
        key = self._key(**kwargs)
        now = time.time()

        async with self._lock:
            item = self._l1.get(key)
            if item:
                val, exp = item
                if now < exp:
                    self._l1_access[key] = now
                    return val
                self._l1.pop(key, None)
                self._l1_access.pop(key, None)

        disk_path = os.path.join(self._dir, key)
        if os.path.exists(disk_path):
            try:
                if (time.time() - os.path.getmtime(disk_path)) <= self.l3_ttl:
                    with open(disk_path, "rb") as f:
                        raw = f.read()
                    val = self._decompress(raw)
                    if val is not None:
                        async with self._lock:
                            if len(self._l1) >= self.max_l1_size and self._l1_access:
                                oldest = min(self._l1_access.items(), key=lambda x: x[1])[0]
                                self._l1.pop(oldest, None)
                                self._l1_access.pop(oldest, None)
                            self._l1[key] = (val, now + self.l1_ttl)
                            self._l1_access[key] = now
                    return val
            except Exception:
                pass
        return None

    async def set(self, value: Any, **kwargs) -> None:
        key = self._key(**kwargs)
        now = time.time()

        async with self._lock:
            if len(self._l1) >= self.max_l1_size and self._l1_access:
                oldest = min(self._l1_access.items(), key=lambda x: x[1])[0]
                self._l1.pop(oldest, None)
                self._l1_access.pop(oldest, None)
            self._l1[key] = (value, now + self.l1_ttl)
            self._l1_access[key] = now

        try:
            with open(os.path.join(self._dir, key), "wb") as f:
                f.write(self._compress(value))
        except Exception:
            pass


# ============================================================================
# SingleFlight (no await inside lock)
# ============================================================================
class SingleFlight:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._calls: Dict[str, asyncio.Future] = {}

    async def execute(self, key: str, coro_func: Callable[[], Any]) -> Any:
        async with self._lock:
            fut = self._calls.get(key)
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._calls[key] = fut
                owner = True
            else:
                owner = False

        if not owner:
            return await fut  # type: ignore

        try:
            res = await coro_func()
            if not fut.done():
                fut.set_result(res)
            return res
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise
        finally:
            async with self._lock:
                self._calls.pop(key, None)


# ============================================================================
# Optional Scoring / Forecasting adapters (LAZY import; best-effort)
# ============================================================================
_SCORING_MOD: Optional[Any] = None
_FORECAST_MOD: Optional[Any] = None
_ANALYTICS_IMPORT_LOCK = asyncio.Lock()


def _try_import_any(paths: Sequence[str]) -> Optional[Any]:
    for p in paths:
        try:
            return import_module(p)
        except Exception:
            continue
    return None


async def _get_scoring_module() -> Optional[Any]:
    global _SCORING_MOD
    if _SCORING_MOD is not None:
        return _SCORING_MOD
    async with _ANALYTICS_IMPORT_LOCK:
        if _SCORING_MOD is None:
            _SCORING_MOD = _try_import_any(["core.scoring", "core.analysis.scoring"])
    return _SCORING_MOD


async def _get_forecast_module() -> Optional[Any]:
    global _FORECAST_MOD
    if _FORECAST_MOD is not None:
        return _FORECAST_MOD
    async with _ANALYTICS_IMPORT_LOCK:
        if _FORECAST_MOD is None:
            _FORECAST_MOD = _try_import_any(["core.forecasting", "core.analysis.forecasting"])
    return _FORECAST_MOD


def _fn_accepts_settings(fn: Callable) -> bool:
    try:
        co = getattr(fn, "__code__", None)
        varnames = getattr(co, "co_varnames", ()) if co is not None else ()
        return "settings" in set(varnames or ())
    except Exception:
        return False


async def _maybe_apply_scoring_module(row: Dict[str, Any], settings: Any) -> Dict[str, Any]:
    mod = await _get_scoring_module()
    if mod is None:
        return row

    for fn_name in ("compute_scores", "score_row", "score_quote"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            try:
                r = fn(row, settings=settings) if _fn_accepts_settings(fn) else fn(row)
                if asyncio.iscoroutine(r) or asyncio.isfuture(r) or hasattr(r, "__await__"):
                    r = await r
                if isinstance(r, dict):
                    for k, v in r.items():
                        if v is not None:
                            row[k] = v
            except Exception:
                pass
            break
    return row


async def _maybe_apply_forecast(row: Dict[str, Any], settings: Any) -> Dict[str, Any]:
    mod = await _get_forecast_module()
    if mod is None:
        return row

    for fn_name in ("compute_forecast", "forecast_row", "forecast_quote"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            try:
                r = fn(row, settings=settings) if _fn_accepts_settings(fn) else fn(row)
                if asyncio.iscoroutine(r) or asyncio.isfuture(r) or hasattr(r, "__await__"):
                    r = await r
                if isinstance(r, dict):
                    for k, v in r.items():
                        if v is not None:
                            row[k] = v
            except Exception:
                pass
            break
    return row


# ============================================================================
# Phase D computations (risk stats / valuation / scoring / buckets / recos)
# ============================================================================
def _compute_price_derivatives(row: Dict[str, Any]) -> None:
    cp = _as_float(row.get("current_price"))
    pc = _as_float(row.get("previous_close"))
    if cp is not None and pc is not None:
        if row.get("price_change") is None:
            row["price_change"] = cp - pc
        if row.get("percent_change") is None and pc != 0:
            row["percent_change"] = (cp - pc) / pc
    # week_52_position_pct
    hi = _as_float(row.get("week_52_high"))
    lo = _as_float(row.get("week_52_low"))
    if cp is not None and hi is not None and lo is not None and hi != lo and row.get("week_52_position_pct") is None:
        row["week_52_position_pct"] = _clamp((cp - lo) / (hi - lo), 0.0, 1.0)

    # keep aliases
    if row.get("price") is None and cp is not None:
        row["price"] = cp
    if row.get("current_price") is None and row.get("price") is not None:
        row["current_price"] = row.get("price")
    if row.get("change") is None and row.get("price_change") is not None:
        row["change"] = row.get("price_change")
    if row.get("change_pct") is None and row.get("percent_change") is not None:
        row["change_pct"] = row.get("percent_change")


def _compute_portfolio_derivatives(row: Dict[str, Any]) -> None:
    qty = _as_float(row.get("position_qty"))
    avg_cost = _as_float(row.get("avg_cost"))
    cp = _as_float(row.get("current_price"))
    if qty is None or avg_cost is None:
        return
    cost = qty * avg_cost
    if row.get("position_cost") is None:
        row["position_cost"] = cost
    if cp is not None:
        val = qty * cp
        if row.get("position_value") is None:
            row["position_value"] = val
        pl = val - cost
        if row.get("unrealized_pl") is None:
            row["unrealized_pl"] = pl
        if row.get("unrealized_pl_pct") is None and cost != 0:
            row["unrealized_pl_pct"] = pl / cost


def _extract_prices_from_history(history: Any) -> List[float]:
    """
    Accepts history formats:
      - list of dicts with close/adj_close/price
      - list of (date, close) tuples
      - list of floats
    Returns list of close prices in chronological order.
    """
    if not history or not isinstance(history, list):
        return []
    out: List[float] = []
    for item in history:
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            f = _as_float(item)
            if f is not None:
                out.append(f)
            continue
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            f = _as_float(item[1])
            if f is not None:
                out.append(f)
            continue
        if isinstance(item, dict):
            for k in ("adj_close", "close", "price", "c"):
                if k in item:
                    f = _as_float(item.get(k))
                    if f is not None:
                        out.append(f)
                        break
    return out


def _pct_std(xs: List[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = sum(xs) / float(len(xs))
    var = sum((x - m) ** 2 for x in xs) / float(len(xs) - 1)
    if var < 0:
        return None
    return math.sqrt(var)


def _compute_rsi_from_prices(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        ch = prices[-i] - prices[-i - 1]
        if ch >= 0:
            gains += ch
        else:
            losses += abs(ch)
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return float(_clamp(rsi, 0.0, 100.0))


def _compute_risk_stats_from_prices(prices: List[float]) -> Dict[str, Any]:
    """
    Computes:
      - volatility_30d, volatility_90d (annualized, as fraction)
      - max_drawdown_1y (fraction, negative or positive? we store as positive drawdown magnitude)
      - var_95_1d (fraction loss magnitude)
      - sharpe_1y (float)
    """
    if len(prices) < 20:
        return {}

    # returns (simple)
    rets: List[float] = []
    for i in range(1, len(prices)):
        p0 = prices[i - 1]
        p1 = prices[i]
        if p0 and p0 > 0:
            rets.append((p1 / p0) - 1.0)

    out: Dict[str, Any] = {}
    if len(rets) >= 30:
        std30 = _pct_std(rets[-30:])
        if std30 is not None:
            out["volatility_30d"] = std30 * math.sqrt(252.0)
    if len(rets) >= 90:
        std90 = _pct_std(rets[-90:])
        if std90 is not None:
            out["volatility_90d"] = std90 * math.sqrt(252.0)

    # max drawdown over last ~252 trading days
    window = prices[-252:] if len(prices) >= 252 else prices[:]
    peak = None
    max_dd = 0.0
    for p in window:
        if peak is None or p > peak:
            peak = p
        if peak and peak > 0:
            dd = (p / peak) - 1.0
            if dd < max_dd:
                max_dd = dd
    # store magnitude as positive fraction (e.g., 0.25 == -25% drawdown)
    out["max_drawdown_1y"] = abs(max_dd) if max_dd < 0 else 0.0

    # VaR 95% 1D based on historical returns (5th percentile loss magnitude)
    if len(rets) >= 30:
        sorted_rets = sorted(rets)
        idx = max(0, int(0.05 * (len(sorted_rets) - 1)))
        q05 = sorted_rets[idx]
        out["var_95_1d"] = abs(q05) if q05 < 0 else 0.0

    # Sharpe 1Y (risk-free ~0)
    if len(rets) >= 60:
        m = sum(rets[-252:]) / float(len(rets[-252:])) if len(rets) >= 252 else (sum(rets) / float(len(rets)))
        s = _pct_std(rets[-252:]) if len(rets) >= 252 else _pct_std(rets)
        if s is not None and s > 0:
            out["sharpe_1y"] = (m * 252.0) / (s * math.sqrt(252.0))

    return out


def _score_from_risk_stats(row: Dict[str, Any]) -> Optional[float]:
    """
    Risk score (0-100): higher means riskier.
    Uses volatility_90d + max_drawdown_1y + var_95_1d (best-effort).
    """
    vol = _as_pct_fraction(row.get("volatility_90d"))
    dd = _as_pct_fraction(row.get("max_drawdown_1y"))
    var = _as_pct_fraction(row.get("var_95_1d"))

    # if provider stored as fraction, ok; if stored as percent, normalized above
    if vol is None and dd is None and var is None:
        return None

    # normalize each component into 0..100 contribution
    def to100(x: Optional[float], scale: float) -> float:
        if x is None:
            return 0.0
        return _clamp((x / scale) * 100.0, 0.0, 100.0)

    # vol scale: 60% annualized is "high"
    a = to100(vol, 0.60)
    # drawdown scale: 50% is "very high"
    b = to100(dd, 0.50)
    # var scale: 6% daily loss is "high"
    c = to100(var, 0.06)

    # weighted
    score = 0.45 * a + 0.35 * b + 0.20 * c
    return float(_clamp(score, 0.0, 100.0))


def _bucket_risk(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    s = float(score)
    if s <= 35:
        return "Low"
    if s <= 65:
        return "Moderate"
    return "High"


def _bucket_confidence(score01: Optional[float]) -> Optional[str]:
    if score01 is None:
        return None
    s = float(score01)
    if s >= 0.75:
        return "High"
    if s >= 0.50:
        return "Medium"
    return "Low"


def _compute_valuation_score(row: Dict[str, Any]) -> Optional[float]:
    """
    Valuation score (0-100): higher means more attractive valuation.
    Uses pe_ttm, pb_ratio, ps_ratio, peg_ratio (best-effort).
    Lower multiples => higher score.
    """
    # helpers: invert ratio into 0..100 with a soft cap
    def inv_score(x: Optional[float], cap: float) -> Optional[float]:
        if x is None:
            return None
        if x <= 0:
            return None
        # 1/x scaled, cap at cap
        s = (cap / x) * 100.0
        return float(_clamp(s, 0.0, 100.0))

    pe = _as_float(row.get("pe_ttm"))
    pb = _as_float(row.get("pb_ratio"))
    ps = _as_float(row.get("ps_ratio"))
    peg = _as_float(row.get("peg_ratio"))

    parts: List[float] = []
    w: List[float] = []

    s1 = inv_score(pe, cap=25.0)
    if s1 is not None:
        parts.append(s1)
        w.append(0.35)

    s2 = inv_score(pb, cap=3.5)
    if s2 is not None:
        parts.append(s2)
        w.append(0.25)

    s3 = inv_score(ps, cap=6.0)
    if s3 is not None:
        parts.append(s3)
        w.append(0.20)

    s4 = inv_score(peg, cap=2.0)
    if s4 is not None:
        parts.append(s4)
        w.append(0.20)

    if not parts:
        return None

    ww = sum(w) if w else 1.0
    return float(_clamp(sum(p * wi for p, wi in zip(parts, w)) / ww, 0.0, 100.0))


def _compute_scores_fallback(row: Dict[str, Any]) -> None:
    """
    Ensure all score fields exist (0..100 where applicable), even if core.scoring didn’t fill them.
    This prevents "blank score columns" across pages.

    - value_score: from valuation_score or valuation ratios
    - growth_score: from revenue_growth_yoy
    - momentum_score: from percent_change + rsi_14 + week_52_position_pct
    - quality_score: from margins + debt_to_equity
    - overall_score: weighted average of above, penalized by risk_score
    - opportunity_score: overall * confidence * (1 - risk)
    """
    # confidence_score: normalize to 0..100 if possible
    conf01 = _as_float(row.get("forecast_confidence"))
    if conf01 is None:
        conf01 = _as_float(row.get("confidence_score"))
    if conf01 is not None:
        conf01 = (conf01 / 100.0) if conf01 > 1.5 else conf01
        conf01 = _clamp(conf01, 0.0, 1.0)
    if row.get("confidence_score") is None and conf01 is not None:
        row["confidence_score"] = conf01  # keep 0..1 internally; schema can format
    if row.get("confidence_bucket") is None and conf01 is not None:
        row["confidence_bucket"] = _bucket_confidence(conf01)

    # risk_score
    if row.get("risk_score") is None:
        rs = _score_from_risk_stats(row)
        if rs is not None:
            row["risk_score"] = rs
    if row.get("risk_bucket") is None and row.get("risk_score") is not None:
        row["risk_bucket"] = _bucket_risk(_as_float(row.get("risk_score")))

    # valuation_score
    if row.get("valuation_score") is None:
        vs = _compute_valuation_score(row)
        if vs is not None:
            row["valuation_score"] = vs

    # value_score
    if row.get("value_score") is None:
        if row.get("valuation_score") is not None:
            row["value_score"] = _clamp(float(row["valuation_score"]), 0.0, 100.0)

    # growth_score
    if row.get("growth_score") is None:
        g = _as_pct_fraction(row.get("revenue_growth_yoy"))
        if g is not None:
            # map -30%..+30% into 0..100
            row["growth_score"] = _clamp(((g + 0.30) / 0.60) * 100.0, 0.0, 100.0)

    # momentum_score
    if row.get("momentum_score") is None:
        pc = _as_pct_fraction(row.get("percent_change"))
        rsi = _as_float(row.get("rsi_14"))
        pos = _as_pct_fraction(row.get("week_52_position_pct"))
        parts = []
        weights = []
        if pc is not None:
            # -10%..+10% -> 0..100
            parts.append(_clamp(((pc + 0.10) / 0.20) * 100.0, 0.0, 100.0))
            weights.append(0.40)
        if rsi is not None:
            parts.append(_clamp(rsi, 0.0, 100.0))
            weights.append(0.35)
        if pos is not None:
            parts.append(_clamp(pos * 100.0, 0.0, 100.0))
            weights.append(0.25)
        if parts:
            wsum = sum(weights) if weights else 1.0
            row["momentum_score"] = sum(p * w for p, w in zip(parts, weights)) / wsum

    # quality_score
    if row.get("quality_score") is None:
        gm = _as_pct_fraction(row.get("gross_margin"))
        om = _as_pct_fraction(row.get("operating_margin"))
        pm = _as_pct_fraction(row.get("profit_margin"))
        de = _as_float(row.get("debt_to_equity"))
        parts = []
        weights = []
        if gm is not None:
            parts.append(_clamp((gm / 0.60) * 100.0, 0.0, 100.0))
            weights.append(0.25)
        if om is not None:
            parts.append(_clamp((om / 0.35) * 100.0, 0.0, 100.0))
            weights.append(0.30)
        if pm is not None:
            parts.append(_clamp((pm / 0.30) * 100.0, 0.0, 100.0))
            weights.append(0.25)
        if de is not None and de >= 0:
            # lower D/E is better. cap at 3.0
            parts.append(_clamp((1.0 - min(de, 3.0) / 3.0) * 100.0, 0.0, 100.0))
            weights.append(0.20)
        if parts:
            wsum = sum(weights) if weights else 1.0
            row["quality_score"] = sum(p * w for p, w in zip(parts, weights)) / wsum

    # overall_score
    if row.get("overall_score") is None:
        vals = []
        w = []
        for k, ww in (("value_score", 0.30), ("quality_score", 0.25), ("momentum_score", 0.25), ("growth_score", 0.20)):
            v = _as_float(row.get(k))
            if v is not None:
                vals.append(_clamp(v, 0.0, 100.0))
                w.append(ww)
        if vals:
            base = sum(v * ww for v, ww in zip(vals, w)) / (sum(w) if w else 1.0)
            # penalize by risk
            risk = _as_float(row.get("risk_score"))
            if risk is not None:
                base = base * (1.0 - _clamp(risk / 100.0, 0.0, 0.65))
            row["overall_score"] = _clamp(base, 0.0, 100.0)

    # opportunity_score
    if row.get("opportunity_score") is None:
        ov = _as_float(row.get("overall_score"))
        risk = _as_float(row.get("risk_score"))
        conf = conf01
        if ov is not None:
            rr = _clamp((risk / 100.0), 0.0, 1.0) if risk is not None else 0.5
            cc = _clamp(conf, 0.0, 1.0) if conf is not None else 0.5
            row["opportunity_score"] = _clamp(ov * cc * (1.0 - rr), 0.0, 100.0)


def _compute_recommendation(row: Dict[str, Any]) -> None:
    """
    Light deterministic recommendation (to avoid blank column).
    If you have a dedicated advisor engine, it can overwrite later.
    """
    if row.get("recommendation"):
        return

    ov = _as_float(row.get("overall_score"))
    risk = _as_float(row.get("risk_score"))
    conf = _as_float(row.get("forecast_confidence"))
    if conf is None:
        conf = _as_float(row.get("confidence_score"))
    if conf is not None:
        conf = (conf / 100.0) if conf > 1.5 else conf
        conf = _clamp(conf, 0.0, 1.0)

    if ov is None:
        row["recommendation"] = "HOLD"
        row["recommendation_reason"] = "Insufficient score data"
        return

    risk_v = risk if risk is not None else 60.0
    conf_v = conf if conf is not None else 0.55

    if ov >= 78 and conf_v >= 0.70 and risk_v <= 55:
        row["recommendation"] = "BUY"
        row["recommendation_reason"] = "High score + strong confidence + controlled risk"
    elif ov >= 65 and risk_v <= 70:
        row["recommendation"] = "HOLD"
        row["recommendation_reason"] = "Moderate score / acceptable risk"
    elif ov >= 50:
        row["recommendation"] = "REDUCE"
        row["recommendation_reason"] = "Weak score or elevated risk"
    else:
        row["recommendation"] = "SELL"
        row["recommendation_reason"] = "Very weak score"


def _ensure_ksa_classification(row: Dict[str, Any], is_ksa_sym: bool) -> None:
    if not is_ksa_sym:
        return
    if not row.get("country"):
        row["country"] = "SAU"
    # don’t overwrite sector/industry if already present (providers fill)
    # but if they exist as blanks, normalize to None
    for k in ("sector", "industry"):
        v = row.get(k)
        if isinstance(v, str) and not v.strip():
            row[k] = None


def _compute_intrinsic_value_fallback(row: Dict[str, Any]) -> None:
    """
    Best-effort intrinsic value to reduce blanks WITHOUT inventing data.
    Only computes when inputs exist.
    """
    if row.get("intrinsic_value") is not None:
        return
    # If we have eps and a reasonable forward PE, approximate intrinsic as eps * pe_forward
    eps = _as_float(row.get("eps_ttm"))
    pef = _as_float(row.get("pe_forward"))
    if eps is not None and pef is not None and eps > 0 and 0 < pef < 60:
        row["intrinsic_value"] = eps * pef


def _ensure_required_advanced_fields(row: Dict[str, Any]) -> None:
    """
    Ensure *presence* (not necessarily non-null) of required advanced fields,
    so schema projection doesn’t look like “missing columns”.
    """
    required_keys = [
        # valuation
        "pb_ratio", "ps_ratio", "peg_ratio", "intrinsic_value", "valuation_score",
        # risk stats
        "var_95_1d", "sharpe_1y", "max_drawdown_1y", "volatility_90d",
        # scores
        "value_score", "quality_score", "momentum_score", "growth_score",
        "overall_score", "opportunity_score", "rank_overall",
        # buckets
        "confidence_bucket", "risk_bucket",
        # classification
        "country", "sector", "industry",
    ]
    for k in required_keys:
        if k not in row:
            row[k] = None


# ============================================================================
# History fetching for risk stats (best-effort; provider-dependent)
# ============================================================================
async def _fetch_history_from_provider(registry: ProviderRegistry, provider: str, symbol: str, timeout_s: float) -> Optional[Any]:
    module, stats = await registry.get_provider(provider)
    if stats.is_circuit_open:
        return None
    if module is None:
        return None

    # choose history function
    fn = None
    for nm in HISTORY_FUNCTIONS.get(provider, []):
        cand = getattr(module, nm, None)
        if callable(cand):
            fn = cand
            break
    if fn is None:
        return None

    try:
        async with asyncio.timeout(timeout_s):
            out = await _call_maybe_async(fn, symbol)
        return out
    except Exception:
        return None


# ============================================================================
# Sheet schema helpers (CRITICAL for correctness)
# ============================================================================
def _canonicalize_sheet_name(sheet: str) -> str:
    s = (sheet or "").strip()
    if not s:
        return s

    try:
        from core.sheets.page_catalog import resolve_page  # type: ignore

        out = resolve_page(s)
        if isinstance(out, str) and out.strip():
            return out.strip()
    except Exception:
        pass

    try:
        from core.sheets.page_catalog import canonicalize_page  # type: ignore

        out = canonicalize_page(s)
        if isinstance(out, str) and out.strip():
            return out.strip()
    except Exception:
        pass

    return s.replace(" ", "_")


def _schema_for_sheet(sheet: str) -> Tuple[Optional[Any], List[str], List[str], str]:
    """
    Returns (spec, headers, keys, source)
    """
    if not sheet:
        return None, [], [], "none"
    try:
        spec = get_sheet_spec(sheet)
        cols = getattr(spec, "columns", None) or []
        headers = [str(getattr(c, "header", "")).strip() for c in cols]
        keys = [str(getattr(c, "key", "")).strip() for c in cols]
        # drop blank
        headers = [h for h in headers if h]
        keys = [k for k in keys if k]
        if headers and keys and len(headers) == len(keys):
            return spec, headers, keys, "schema_registry.get_sheet_spec"
        return spec, headers, keys, "schema_registry.partial"
    except Exception:
        return None, [], [], "none"


def _strict_project_row(keys: List[str], row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: row.get(k, None) for k in keys}


def _rows_matrix(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[r.get(k) for k in keys] for r in rows]


def _normalize_to_schema_keys(schema_keys: Sequence[str], schema_headers: Sequence[str], raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes a raw row dict to schema KEYS.
    Supports raw using either schema keys OR schema headers.
    Missing -> None
    """
    raw = raw or {}
    raw_ci = {str(k).strip().lower(): v for k, v in raw.items()}

    header_by_key: Dict[str, str] = {}
    for k, h in zip(schema_keys, schema_headers):
        header_by_key[str(k)] = str(h)

    out: Dict[str, Any] = {}
    for k in schema_keys:
        ks = str(k)
        v = None

        if ks in raw:
            v = raw.get(ks)
        else:
            v = raw_ci.get(ks.lower())

        if v is None:
            h = header_by_key.get(ks, "")
            if h:
                if h in raw:
                    v = raw.get(h)
                else:
                    v = raw_ci.get(h.strip().lower())

        out[ks] = v

    return out


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    """
    Coerce builder output to list[dict].
    Accepts:
      - list[dict]
      - dict envelope: rows/data/items/records
      - single dict row
    """
    if out is None:
        return []
    if isinstance(out, list):
        return [r for r in out if isinstance(r, dict)]
    if isinstance(out, dict):
        r2 = out.get("rows") or out.get("data") or out.get("items") or out.get("records")
        if isinstance(r2, list):
            return [r for r in r2 if isinstance(r, dict)]
        return [out]
    return []


def _list_sheet_names_best_effort() -> List[str]:
    if isinstance(SCHEMA_REGISTRY, dict) and SCHEMA_REGISTRY:
        try:
            return [str(k) for k in SCHEMA_REGISTRY.keys()]
        except Exception:
            pass
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore

        return [str(x) for x in CANONICAL_PAGES]
    except Exception:
        return []


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    """
    Compute rank_overall within the returned rowset (sheet-level).
    Uses overall_score then opportunity_score.
    """
    scored: List[Tuple[int, float]] = []
    for i, r in enumerate(rows):
        ov = _as_float(r.get("overall_score"))
        if ov is None:
            ov = _as_float(r.get("opportunity_score"))
        if ov is None:
            continue
        scored.append((i, ov))
    scored.sort(key=lambda t: t[1], reverse=True)
    rank = 1
    for idx, _ in scored:
        rows[idx]["rank_overall"] = rank
        rank += 1


# ============================================================================
# DataEngine V5 (v5.8.0)
# ============================================================================
class DataEngineV5:
    def __init__(self, settings: Any = None):
        self.settings = settings if settings is not None else _try_get_settings()
        self.flags = _feature_flags(self.settings)
        self.version = __version__

        if self.settings is not None:
            try:
                enabled = [str(x).lower() for x in (getattr(self.settings, "enabled_providers", None) or [])]
            except Exception:
                enabled = []
            try:
                ksa_list = [str(x).lower() for x in (getattr(self.settings, "ksa_providers", None) or [])]
            except Exception:
                ksa_list = []
            try:
                primary = str(getattr(self.settings, "primary_provider", "eodhd") or "eodhd").lower()
            except Exception:
                primary = "eodhd"
        else:
            enabled = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
            ksa_list = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
            primary = _get_env_str("PRIMARY_PROVIDER", "eodhd").lower()

        self.primary_provider = primary or "eodhd"
        self.enabled_providers = enabled or _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = ksa_list or _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)

        self.max_concurrency = _get_env_int("DATA_ENGINE_MAX_CONCURRENCY", 25)
        self.request_timeout = _get_env_float("DATA_ENGINE_TIMEOUT_SECONDS", 20.0)

        # IMPORTANT: EODHD not usable for KSA (your constraint) => default TRUE
        self.ksa_disallow_eodhd = _get_env_bool("KSA_DISALLOW_EODHD", True)

        # strict sheet-rows prevents silent “default schema” collapses
        self.schema_strict_sheet_rows = _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)

        self._sem = asyncio.Semaphore(max(1, self.max_concurrency))
        self._singleflight = SingleFlight()
        self._registry = ProviderRegistry()
        self._cache = MultiLevelCache(
            name="data_engine",
            l1_ttl=_get_env_int("CACHE_L1_TTL", 60),
            l3_ttl=_get_env_int("CACHE_L3_TTL", 3600),
            max_l1_size=_get_env_int("CACHE_L1_MAX", 5000),
        )

        logger.info(
            "DataEngineV5 v%s initialized | primary=%s | enabled=%s | ksa=%s | global=%s | flags=%s | schema=%s | strict_sheet_rows=%s | ksa_disallow_eodhd=%s",
            self.version,
            self.primary_provider,
            len(self.enabled_providers),
            len(self.ksa_providers),
            len(self.global_providers),
            self.flags,
            "available" if _SCHEMA_AVAILABLE else "missing",
            self.schema_strict_sheet_rows,
            self.ksa_disallow_eodhd,
        )

    async def aclose(self) -> None:
        return

    # -----------------------------
    # Provider ordering (GLOBAL-first)
    # -----------------------------
    def _providers_for(self, symbol: str) -> List[str]:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))

        base = self.ksa_providers if is_ksa_sym else self.global_providers
        providers = [p for p in base if p in self.enabled_providers]

        if is_ksa_sym and self.ksa_disallow_eodhd:
            providers = [p for p in providers if p != "eodhd"]

        # ensure primary is head when allowed
        if self.primary_provider and (self.primary_provider in self.enabled_providers):
            if self.primary_provider in providers:
                providers = [p for p in providers if p != self.primary_provider]
                providers.insert(0, self.primary_provider)
            else:
                # only inject primary if it is allowed for this market
                if (not is_ksa_sym) or (self.primary_provider != "eodhd") or (not self.ksa_disallow_eodhd):
                    providers.insert(0, self.primary_provider)

        # de-dupe
        seen: set = set()
        providers = [p for p in providers if not (p in seen or seen.add(p))]

        # stable sort tail by priority
        def pr(p: str) -> int:
            return PROVIDER_PRIORITIES.get(p, 999)

        if providers:
            head = providers[0]
            tail = sorted(providers[1:], key=pr)
            return [head] + tail

        return providers

    def _provider_symbol(self, provider: str, symbol: str) -> str:
        if provider.startswith("yahoo"):
            try:
                return to_yahoo_symbol(symbol)  # type: ignore
            except Exception:
                return symbol
        return symbol

    async def _fetch_patch(self, provider: str, symbol: str) -> Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.time()

        async with self._sem:
            module, stats = await self._registry.get_provider(provider)

            if stats.is_circuit_open:
                return provider, None, 0.0, "circuit_open"

            if module is None:
                err = stats.last_import_error or "provider module missing"
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000, err

            fn = _pick_provider_callable(module, provider)
            if fn is None:
                err = f"no callable fetch function for provider '{provider}'"
                await self._registry.record_failure(provider, err)
                return provider, None, (time.time() - start) * 1000, err

            provider_symbol = self._provider_symbol(provider, symbol)

            try:
                async with asyncio.timeout(self.request_timeout):
                    res = await _call_maybe_async(fn, provider_symbol)

                latency = (time.time() - start) * 1000

                if isinstance(res, dict) and res:
                    patch = _normalize_patch_keys(_clean_patch(res))
                    if _is_useful_patch(patch):
                        await self._registry.record_success(provider, latency)
                        return provider, patch, latency, None

                    err = str(res.get("error") or "empty_result")
                    await self._registry.record_failure(provider, err)
                    return provider, None, latency, err

                err = "non_dict_or_empty"
                await self._registry.record_failure(provider, err)
                return provider, None, latency, err

            except TimeoutError:
                latency = (time.time() - start) * 1000
                err = "timeout"
                await self._registry.record_failure(provider, err)
                return provider, None, latency, err

            except Exception as e:
                latency = (time.time() - start) * 1000
                await self._registry.record_failure(provider, repr(e))
                return provider, None, latency, repr(e)

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

        protected = {"symbol", "symbol_normalized", "requested_symbol"}

        for prov, patch, latency in patches:
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(float(latency or 0.0), 2)

            for k, v in patch.items():
                if k in protected or v is None:
                    continue
                # fill-if-missing policy (enrichment-friendly)
                if k not in merged or merged.get(k) in (None, "", []):
                    merged[k] = v

        # keep aliases consistent
        if merged.get("current_price") is None and merged.get("price") is not None:
            merged["current_price"] = merged.get("price")
        if merged.get("price") is None and merged.get("current_price") is not None:
            merged["price"] = merged.get("current_price")

        if merged.get("price_change") is None and merged.get("change") is not None:
            merged["price_change"] = merged.get("change")
        if merged.get("change") is None and merged.get("price_change") is not None:
            merged["change"] = merged.get("price_change")

        if merged.get("percent_change") is None and merged.get("change_pct") is not None:
            merged["percent_change"] = merged.get("change_pct")
        if merged.get("change_pct") is None and merged.get("percent_change") is not None:
            merged["change_pct"] = merged.get("percent_change")

        return merged

    def _data_quality(self, row: Dict[str, Any]) -> str:
        cp = row.get("current_price")
        if _as_float(cp) is None:
            return QuoteQuality.MISSING.value
        # if we have key advanced signals, upgrade quality
        if any(row.get(k) is not None for k in ("overall_score", "forecast_price_3m", "pb_ratio", "volatility_90d")):
            return QuoteQuality.GOOD.value
        return QuoteQuality.FAIR.value

    async def _maybe_compute_risk_stats(self, row: Dict[str, Any], symbol_norm: str, is_ksa_sym: bool, providers_used: List[str]) -> None:
        """
        Compute missing risk stats using history, best-effort.
        Preference:
          - GLOBAL: eodhd history, fallback yahoo_chart
          - KSA: yahoo_chart history (since EODHD disallowed), fallback others
        """
        if not self.flags.get("risk_stats_enabled", True):
            return

        need = any(row.get(k) is None for k in ("volatility_90d", "max_drawdown_1y", "var_95_1d", "sharpe_1y"))
        if not need:
            # still compute RSI if missing and we have prices below
            return

        # pick provider order for history
        if is_ksa_sym:
            history_providers = ["yahoo_chart", "tadawul", "argaam"]
            history_providers = [p for p in history_providers if p in self.enabled_providers]
        else:
            history_providers = ["eodhd", "yahoo_chart", "finnhub"]
            history_providers = [p for p in history_providers if p in self.enabled_providers]

        # try providers; convert symbol per provider when needed
        hist_obj = None
        for p in history_providers:
            sym_p = self._provider_symbol(p, symbol_norm)
            hist_obj = await _fetch_history_from_provider(self._registry, p, sym_p, timeout_s=self.request_timeout)
            if hist_obj:
                break

        # some providers may embed history already
        if not hist_obj:
            hist_obj = row.get("history") or row.get("prices") or row.get("price_history")

        prices = _extract_prices_from_history(hist_obj)
        if len(prices) < 30:
            return

        stats = _compute_risk_stats_from_prices(prices)
        for k, v in stats.items():
            if row.get(k) is None and v is not None:
                # pct-like values stored as fraction
                if k in ("volatility_30d", "volatility_90d", "max_drawdown_1y", "var_95_1d"):
                    row[k] = _clamp(float(v), 0.0, 5.0)
                else:
                    row[k] = v

        # RSI if missing
        if row.get("rsi_14") is None:
            rsi = _compute_rsi_from_prices(prices, period=14)
            if rsi is not None:
                row["rsi_14"] = rsi

    # -----------------------------
    # Public Quote API
    # -----------------------------
    async def get_enriched_quote(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> UnifiedQuote:
        return await self._singleflight.execute(
            f"quote:{symbol}",
            lambda: self._get_enriched_quote_impl(symbol, use_cache, schema=schema),
        )

    async def get_enriched_quote_dict(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> Dict[str, Any]:
        q = await self.get_enriched_quote(symbol, use_cache=use_cache, schema=schema)
        return _model_to_dict(q)

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True, *, schema: Any = None) -> UnifiedQuote:
        info = get_symbol_info(symbol)
        norm = info.get("normalized") or ""
        is_ksa_sym = bool(info.get("is_ksa"))

        if not norm:
            row = {
                "symbol": symbol,
                "symbol_normalized": None,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "Invalid symbol",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            _ensure_required_advanced_fields(row)
            row = normalize_row_to_schema(schema, row) if schema is not None else normalize_row_to_schema(None, row)
            return UnifiedQuote(**row)  # type: ignore

        if use_cache:
            cached = await self._cache.get(symbol=norm)
            if cached:
                try:
                    if isinstance(cached, dict):
                        return UnifiedQuote(**cached)  # type: ignore
                    if isinstance(cached, UnifiedQuote):
                        return cached  # type: ignore
                except Exception:
                    pass

        providers = self._providers_for(norm)
        if not providers:
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "No providers available",
                "data_sources": [],
                "provider_latency": {},
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            _ensure_required_advanced_fields(row)
            row = normalize_row_to_schema(schema, row) if schema is not None else normalize_row_to_schema(None, row)
            return UnifiedQuote(**row)  # type: ignore

        top_n = _get_env_int("PROVIDER_TOP_N", 4)
        top = providers[: max(1, int(top_n))]

        gathered = await asyncio.gather(*[self._fetch_patch(p, norm) for p in top], return_exceptions=True)

        results: List[Tuple[str, Optional[Dict[str, Any]], float, Optional[str]]] = []
        for r in gathered:
            if isinstance(r, tuple) and len(r) == 4:
                results.append(r)

        patches_ok: List[Tuple[str, Dict[str, Any], float]] = [(p, patch, lat) for (p, patch, lat, _) in results if patch]

        if not patches_ok:
            stats = await self._registry.get_stats()
            err_detail = {
                "requested": symbol,
                "normalized": norm,
                "attempted_providers": top,
                "provider_stats": {k: stats.get(k) for k in top},
                "errors": [{"provider": p, "error": err, "latency_ms": round(lat, 2)} for (p, _, lat, err) in results],
            }
            row = {
                "symbol": norm,
                "symbol_normalized": norm,
                "requested_symbol": symbol,
                "data_quality": QuoteQuality.MISSING.value,
                "error": "No data available",
                "info": err_detail,
                "data_sources": [],
                "provider_latency": {},
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            _ensure_required_advanced_fields(row)
            row = normalize_row_to_schema(schema, row) if schema is not None else normalize_row_to_schema(None, row)
            q = UnifiedQuote(**row)  # type: ignore
            if use_cache:
                await self._cache.set(_model_to_dict(q), symbol=norm)
            return q

        row = self._merge(symbol, norm, patches_ok)

        # Phase D: always compute basic derivatives + KSA classification baseline
        _compute_price_derivatives(row)
        _compute_portfolio_derivatives(row)
        _ensure_ksa_classification(row, is_ksa_sym)

        # Phase D: valuation mapping + intrinsic fallback
        if self.flags.get("valuation_enabled", True):
            _compute_intrinsic_value_fallback(row)
            if row.get("valuation_score") is None:
                vs = _compute_valuation_score(row)
                if vs is not None:
                    row["valuation_score"] = vs

        # Phase D: risk stats (history) if missing
        await self._maybe_compute_risk_stats(row, norm, is_ksa_sym, providers_used=[p for p, _, _ in patches_ok])

        # Phase D: forecasting/scoring modules (if available)
        if self.flags.get("computations_enabled", True):
            if self.flags.get("forecasting_enabled", True):
                row = await _maybe_apply_forecast(row, self.settings)

            # scoring module first (if exists)
            if self.flags.get("scoring_enabled", True):
                row = await _maybe_apply_scoring_module(row, self.settings)

        # Phase D: ensure scores/buckets exist (fallback)
        _compute_scores_fallback(row)
        _compute_recommendation(row)

        # Ensure all required advanced fields exist (even if None)
        _ensure_required_advanced_fields(row)

        row["data_quality"] = self._data_quality(row)
        row["data_provider"] = (row.get("data_provider") or (row.get("data_sources")[0] if isinstance(row.get("data_sources"), list) and row["data_sources"] else "") or "")

        # Normalize to schema
        row = normalize_row_to_schema(schema, row) if schema is not None else normalize_row_to_schema(None, row)

        q = UnifiedQuote(**row)  # type: ignore
        if use_cache:
            await self._cache.set(_model_to_dict(q), symbol=norm)
        return q

    async def get_enriched_quotes(self, symbols: List[str], *, schema: Any = None) -> List[UnifiedQuote]:
        if not symbols:
            return []
        batch = _get_env_int("QUOTE_BATCH_SIZE", 25)
        try:
            if self.settings is not None and getattr(self.settings, "quote_batch_size", None):
                batch = int(getattr(self.settings, "quote_batch_size"))
        except Exception:
            pass
        batch = max(1, min(500, int(batch)))
        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i : i + batch]
            out.extend(await asyncio.gather(*[self.get_enriched_quote(s, schema=schema) for s in part]))
        return out

    async def get_enriched_quotes_batch(self, symbols: List[str], mode: str = "", *, schema: Any = None) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not symbols:
            return out
        quotes = await asyncio.gather(*[self.get_enriched_quote_dict(s, schema=schema) for s in symbols])
        for s, qd in zip(symbols, quotes):
            out[s] = qd
        return out

    # Backwards-compatible aliases
    get_quote = get_enriched_quote
    get_quotes = get_enriched_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_enriched_quotes
    get_quotes_batch = get_enriched_quotes_batch

    # -----------------------------
    # ✅ Schema-correct Sheet Rows API (SPECIAL DISPATCH + STRICT)
    # -----------------------------
    async def get_sheet_rows(
        self,
        *,
        sheet: str,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Engine-native sheet rows builder that NEVER silently falls back to DEFAULT schema
        when schema_registry knows the requested sheet.
        """
        body = body or {}
        limit = max(1, min(5000, int(limit or 2000)))
        offset = max(0, int(offset or 0))

        include_matrix = True
        try:
            v = body.get("include_matrix")
            if isinstance(v, bool):
                include_matrix = v
            elif isinstance(v, (int, float)):
                include_matrix = bool(int(v))
            elif isinstance(v, str):
                include_matrix = v.strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            include_matrix = True

        raw_sheet = (sheet or "").strip()
        sheet2 = _canonicalize_sheet_name(raw_sheet)

        spec, headers, keys, schema_src = _schema_for_sheet(sheet2)

        if (not headers or not keys) and self.schema_strict_sheet_rows and _SCHEMA_AVAILABLE:
            return {
                "status": "error",
                "sheet": sheet2,
                "page": sheet2,
                "headers": [],
                "keys": [],
                "rows": [],
                "rows_matrix": [] if include_matrix else None,
                "error": f"Unknown sheet or schema missing for '{sheet2}'",
                "meta": {"schema_source": schema_src, "strict": True, "known_sheets": _list_sheet_names_best_effort()},
                "version": self.version,
            }

        # -----------------------------
        # ✅ SPECIAL DISPATCH (explicit)
        # -----------------------------
        if sheet2 == "Data_Dictionary":
            rows: List[Dict[str, Any]] = []
            dd_note = None
            try:
                from core.sheets.data_dictionary import build_data_dictionary_rows as _dd  # type: ignore

                raw_rows = _dd(include_meta_sheet=True)
                rows = [_normalize_to_schema_keys(keys, headers, r) for r in _coerce_rows_list(raw_rows)]
                dd_note = "core.sheets.data_dictionary.build_data_dictionary_rows"
            except Exception:
                if not headers or not keys:
                    headers = ["Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes"]
                    keys = headers[:]
                    schema_src = "fallback:standard_data_dictionary"
                # simple deterministic fallback
                rows = []
                for sn in _list_sheet_names_best_effort():
                    sp, _, _, _ = _schema_for_sheet(sn)
                    cols = getattr(sp, "columns", None) if sp else None
                    if not cols:
                        continue
                    for c in cols:
                        rr = {
                            "sheet": sn,
                            "group": str(getattr(c, "group", "")),
                            "header": str(getattr(c, "header", "")),
                            "key": str(getattr(c, "key", "")),
                            "dtype": str(getattr(c, "dtype", "")),
                            "fmt": str(getattr(c, "fmt", "")),
                            "required": bool(getattr(c, "required", False)),
                            "source": str(getattr(c, "source", "")),
                            "notes": str(getattr(c, "notes", "")),
                        }
                        rows.append(_normalize_to_schema_keys(keys, headers, rr))
                dd_note = "fallback:internal"

            rows = rows[offset : offset + limit]
            proj = [_strict_project_row(keys, r) for r in rows]
            return {
                "status": "success",
                "sheet": "Data_Dictionary",
                "page": "Data_Dictionary",
                "headers": headers,
                "keys": keys,
                "rows": proj,
                "rows_matrix": _rows_matrix(proj, keys) if include_matrix else None,
                "meta": {"schema_source": schema_src, "builder": dd_note, "rows": len(proj), "limit": limit, "offset": offset},
                "version": self.version,
            }

        if sheet2 == "Insights_Analysis":
            # Phase B: always use real builder
            try:
                from core.analysis.insights_builder import build_insights_analysis_rows  # type: ignore

                crit = body.get("criteria") if isinstance(body.get("criteria"), dict) else None
                universes = body.get("universes") if isinstance(body.get("universes"), dict) else None
                symbols = body.get("symbols") if isinstance(body.get("symbols"), list) else None

                payload = await build_insights_analysis_rows(
                    engine=self,
                    criteria=crit,
                    universes=universes,
                    symbols=symbols,
                    mode=mode or "",
                )
                rows0 = _coerce_rows_list(payload)
            except Exception as e:
                rows0 = [{
                    "section": "System",
                    "item": "Insights Builder Error",
                    "symbol": "",
                    "metric": "error",
                    "value": type(e).__name__,
                    "notes": str(e),
                    "last_updated_riyadh": _now_riyadh_iso(),
                }]

            rows_norm = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
            rows_norm = rows_norm[offset : offset + limit]
            return {
                "status": "success",
                "sheet": sheet2,
                "page": sheet2,
                "headers": headers,
                "keys": keys,
                "rows": rows_norm,
                "rows_matrix": _rows_matrix(rows_norm, keys) if include_matrix else None,
                "meta": {"schema_source": schema_src, "builder": "core.analysis.insights_builder", "rows": len(rows_norm), "limit": limit, "offset": offset, "mode": mode},
                "version": self.version,
            }

        if sheet2 == "Top_10_Investments":
            # Phase C: one source of truth -> core.analysis.top10_selector
            try:
                from core.analysis.top10_selector import build_top10_rows  # type: ignore

                criteria = body.get("criteria") if isinstance(body.get("criteria"), dict) else dict(body or {})
                payload = await build_top10_rows(engine=self, criteria=criteria, limit=int(body.get("limit") or body.get("top_n") or 10), mode=mode or "")
                rows0 = payload.get("rows") if isinstance(payload, dict) else payload
                rows0 = _coerce_rows_list(rows0)
            except Exception as e:
                rows0 = [{
                    "symbol": "",
                    "name": "",
                    "top10_rank": None,
                    "selection_reason": "",
                    "criteria_snapshot": "",
                    "warnings": f"Top10 builder error: {type(e).__name__}: {e}",
                    "last_updated_riyadh": _now_riyadh_iso(),
                }]

            rows_norm = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
            rows_norm = rows_norm[offset : offset + limit]
            return {
                "status": "success",
                "sheet": sheet2,
                "page": sheet2,
                "headers": headers,
                "keys": keys,
                "rows": rows_norm,
                "rows_matrix": _rows_matrix(rows_norm, keys) if include_matrix else None,
                "meta": {"schema_source": schema_src, "builder": "core.analysis.top10_selector", "rows": len(rows_norm), "limit": limit, "offset": offset, "mode": mode},
                "version": self.version,
            }

        # -----------------------------
        # Instrument tables: build rows from provided symbols (live quotes),
        # ensuring advanced fields are present and rank_overall is computed.
        # -----------------------------
        symbols = body.get("symbols") or body.get("tickers") or []
        if isinstance(symbols, str):
            symbols = [s.strip() for s in symbols.replace(",", " ").split() if s.strip()]
        if not isinstance(symbols, list):
            symbols = []
        symbols = [str(s).strip() for s in symbols if str(s).strip()]

        rows: List[Dict[str, Any]] = []
        if symbols:
            quotes = await self.get_enriched_quotes(symbols, schema=sheet2 if sheet2 else None)
            for q in quotes:
                d = _model_to_dict(q)
                d = _normalize_to_schema_keys(keys, headers, d) if keys else d
                rows.append(_strict_project_row(keys, d) if keys else d)

        # Apply rank_overall per sheet result
        if rows and ("rank_overall" in (keys or [])):
            _apply_rank_overall(rows)

        rows = rows[offset : offset + limit] if rows else rows
        out_headers = headers[:] if headers else (keys[:] if keys else [])
        out_keys = keys[:] if keys else (headers[:] if headers else [])

        return {
            "status": "success",
            "sheet": sheet2,
            "page": sheet2,
            "headers": out_headers,
            "keys": out_keys,
            "rows": rows,
            "rows_matrix": _rows_matrix(rows, out_keys) if (include_matrix and out_keys) else None,
            "meta": {"schema_source": schema_src, "rows": len(rows), "limit": limit, "offset": offset, "mode": mode, "built_from": "live_quotes" if symbols else "empty"},
            "version": self.version,
        }

    # Common aliases used by routers
    async def sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_sheet_rows(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    # Health helpers used by routers
    async def health(self) -> Dict[str, Any]:
        return {"status": "ok", "version": self.version, "schema_available": bool(_SCHEMA_AVAILABLE)}

    async def get_health(self) -> Dict[str, Any]:
        return await self.health()

    async def health_check(self) -> Dict[str, Any]:
        return await self.health()

    async def get_stats(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "primary_provider": self.primary_provider,
            "enabled_providers": self.enabled_providers,
            "ksa_providers": self.ksa_providers,
            "global_providers": self.global_providers,
            "ksa_disallow_eodhd": self.ksa_disallow_eodhd,
            "flags": dict(self.flags),
            "provider_stats": await self._registry.get_stats(),
            "schema_available": bool(_SCHEMA_AVAILABLE),
            "schema_strict_sheet_rows": bool(self.schema_strict_sheet_rows),
        }


# ============================================================================
# Singleton exports
# ============================================================================
_ENGINE_INSTANCE: Optional[DataEngineV5] = None
_ENGINE_LOCK = asyncio.Lock()


async def get_engine() -> DataEngineV5:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE is None:
        async with _ENGINE_LOCK:
            if _ENGINE_INSTANCE is None:
                _ENGINE_INSTANCE = DataEngineV5()
    return _ENGINE_INSTANCE


async def close_engine() -> None:
    global _ENGINE_INSTANCE
    if _ENGINE_INSTANCE:
        await _ENGINE_INSTANCE.aclose()
        _ENGINE_INSTANCE = None


def get_cache() -> Any:
    global _ENGINE_INSTANCE
    return getattr(_ENGINE_INSTANCE, "_cache", None)


# Backwards names
DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5
DataEngine = DataEngineV5

__all__ = [
    "DataEngineV5",
    "DataEngineV4",
    "DataEngineV3",
    "DataEngineV2",
    "DataEngine",
    "get_engine",
    "close_engine",
    "get_cache",
    "QuoteQuality",
    "DataSource",
    "__version__",
    # Phase-required export
    "normalize_row_to_schema",
]
