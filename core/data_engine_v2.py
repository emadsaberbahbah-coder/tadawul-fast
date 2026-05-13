#!/usr/bin/env python3
# core/data_engine_v2.py
"""
================================================================================
Data Engine V2 — GLOBAL-FIRST ORCHESTRATOR — v5.67.0
================================================================================

WHY v5.67.0 — UNIT-CONTRACT ALIGNMENT (May 13, 2026)
-----------------------------------------------------
v5.66.0 closed the candlestick gap but four percent-unit fields were still
emitted as POINTS while 04_Format.gs v2.7.0 classifies them as FRACTION in
`_KNOWN_FRACTION_PERCENT_COLUMNS_`. The formatter applies "0.00%" (which
multiplies by 100 for display) on top of the engine's existing ×100,
producing a 100× display error on:
    - percent_change             (47/57 rows affected per May 2026 audit)
    - upside_pct                 (52/57 rows)
    - max_drawdown_1y            (57/57 rows; sign also lost via abs())
    - var_95_1d                  (53/57 rows)

v5.67.0 switches these four emit sites from points to fraction:
    - `_canonicalize_provider_row` percent_change branch (PATCH 6)
    - `_sanitize_percent_change` PHASE-BB                (PATCH 7)
    - `_compute_history_patch_from_rows`                 (PATCH 8)
        max_drawdown_1y keeps SIGN (negative); ×100 removed on all three
    - `_compute_intrinsic_and_upside` PHASE-DD           (PATCH 9)

The four downstream consumers that compare these fields against
points-form thresholds are updated to either use `_as_pct_points()`
(which gracefully converts fraction→points based on magnitude) or
`abs()` (so signed drawdowns still contribute to risk_score):
    - `_derive_views`                  (PATCH 10a)
    - `_classify_recommendation_8tier` (PATCH 10b)
    - `_build_top_factors_and_risks`   (PATCH 10c, upside + drawdown)
    - `_compute_scores_fallback`       (PATCH 10d, abs() for dd/var95)

v5.67.0 also adds a comprehensive `_SUFFIX_TO_LOCALE` map (46 entries
covering .HK, .L, .CO, .NS, .SA, .TO, .XETRA, .DE, .PA, .AS, .T, .KS,
.SI, .SS, .SZ, .AX, etc.) and an override block in
`_canonicalize_provider_row` that fixes the 51-row foreign-suffix audit
finding: pre-v5.67.0, `_infer_exchange_from_symbol` returned NASDAQ/NYSE
for every non-Saudi/non-FX/non-Futures ticker, so .HK / .L / .NS rows
all displayed as US listings even when their suffix clearly identified a
foreign exchange. The three inference helpers now consult the suffix
map first, and a defensive override in `_canonicalize_provider_row`
fixes rows where a provider sent Country=USA / Exchange=NASDAQ for
a foreign-suffix ticker (PATCH 4 + PATCH 5).

[PRESERVED] All v5.66.0 PHASE-JJ + v5.65.0 PHASE-II + v5.64.0 FIX-EE/FF/GG
+ v5.63.0 PHASE-AA/BB/CC/DD + v5.62.0 PHASE-Z + v5.47.2 baseline logic intact.

================================================================================

WHY v5.66.0 — PHASE-JJ CANDLESTICK PATTERN DETECTION (May 13, 2026)
--------------------------------------------------------------------
v5.65.0 left the 5 candlestick_* columns showing identical placeholder
values for every symbol ("No clear pattern / NEUTRAL / 0 / 0.00 / None").

v5.66.0 embeds the `core.candlesticks` v1.0.0 pattern detection module
directly into the engine and wires it into the history-row processing
path. This is "free" — it piggybacks on the OHLC bars the history
fallback already fetches, so no extra network calls.

Patterns detected (oldest -> newest bars, defensive on malformed input):
  Single-bar : Doji, Hammer, Inverted Hammer, Shooting Star,
               Hanging Man, Bullish/Bearish Marubozu
  Two-bar    : Bullish Engulfing, Bearish Engulfing
  Three-bar  : Morning Star, Evening Star

Trend context (used to disambiguate Hammer/Hanging Man and
Inverted Hammer/Shooting Star — same geometry, opposite meaning):
A 10-bar percentage change on closes preceding the candle being
evaluated, with a 1% threshold for UP/DOWN/FLAT.

Output fields written to the row:
  candlestick_pattern         "Candle Pattern"       str
  candlestick_signal          "Candle Signal"        str (BULLISH|BEARISH|NEUTRAL|DOJI)
  candlestick_strength        "Candle Strength"      str (STRONG|MODERATE|WEAK)
  candlestick_confidence      "Candle Confidence"    float (0-100)
  candlestick_patterns_recent "Recent Patterns (5D)" str (" | "-separated)

Integration: detection runs inside `_compute_history_patch_from_rows()`,
which is called from both the main history fallback path AND the
yahoo_chart_provider path. Detection happens for free whenever OHLC bars
are processed.

[PRESERVED] All v5.65.0 + v5.64.0 + v5.63.0 + v5.62.0 logic intact.

================================================================================

WHY v5.65.0 — PHASE-II FORECAST QUALITY UPGRADE (May 13, 2026)
---------------------------------------------------------------
v5.64.0 still inherited the v5.47.2 hardcoded forecasts: every symbol got
the same Expected ROI (0.33% / 3% / 8%) and Forecast Confidence (0.55,
"MODERATE"). This made the forecast columns useless for differentiating
symbols.

v5.65.0 adds PHASE-II `_phase_ii_quality_forecast()` which generates
per-symbol forecasts from the symbol's own data:

  Forecast Prices (1M / 3M / 12M):
    - Blended from four weighted components:
      (a) Mean reversion to intrinsic value     (40% weight)
      (b) Trend extrapolation from momentum     (30% weight)
      (c) Composite quality/value/growth signal (20% weight)
      (d) Overall score baseline                (10% weight)
    - RSI adjustment: overbought pulls back, oversold rebounds
    - 12M cap: +/- 30% (MODERATE setting)
    - 3M = 35% of 12M move; 1M = 12% of 12M move (sub-linear)

  Expected ROI (1M / 3M / 12M):
    - Derived from forecast prices: (forecast - current) / current
    - Each horizon differs per symbol

  Forecast Confidence (STRICT scoring):
    - Baseline 0.50
    - +up to 0.20 for data completeness (10 fundamentals/risk fields)
    - +up to 0.10 for score agreement (low variance across sub-scores)
    - -up to 0.15 for HIGH risk / elevated volatility
    - -0.05 if forecast magnitude exceeds 25% (uncertainty bigger than signal)
    - Clamped to [0.30, 0.85]: only well-supported symbols reach HIGH (>=0.70)
    - Bucket: LOW (<0.50) / MODERATE (0.50-0.70) / HIGH (>=0.70)

[PRESERVED] All v5.64.0 + v5.63.0 + v5.62.0 logic intact.

================================================================================

WHY v5.64.0 — THREE-FIX PRODUCTION FOLLOWUP (May 13, 2026)
-----------------------------------------------------------
v5.63.0 post-deploy testing showed three remaining issues:

  FIX-EE.  INSTRUMENT_CANONICAL_KEYS / HEADERS expansion
           v5.47.2 baseline schema was missing 17 v5.50.0->v5.61.0 fields:
           upside_pct, fundamental_view, technical_view, risk_view,
           value_view, sector_relative_score, conviction_score,
           top_factors, top_risks, position_size_hint,
           recommendation_detailed, recommendation_priority, and 5
           candlestick_* fields. Engine projection through canonical
           keys dropped these even when Phase 4 set them on the row.
           v5.64.0 adds them to INSTRUMENT_CANONICAL_KEYS and
           INSTRUMENT_CANONICAL_HEADERS in matching positions.

  FIX-FF.  percent_change x100 double-multiplication
           `_canonicalize_provider_row` line 2369 multiplied percent_change
           by 100 when value <= 1.5, assuming it was a fraction. This
           re-ran AT SCHEMA PROJECTION TIME on already-sanitized values,
           turning 0.7243 (correct percent points) into 72.43 (wrong).
           v5.64.0 adds two guards: (a) skip x100 when the warnings
           string indicates Phase 2 already sanitized, and (b) cross-check
           against price/previous_close arithmetic to detect whether the
           stored value is already in percent points.

  FIX-GG.  Phase 4 execution order
           PHASE-DD ran BEFORE `_compute_scores_fallback`, so
           forecast_price_12m was None when `_compute_intrinsic_and_upside`
           tried to blend it into the intrinsic value. Only the PE-based
           component contributed, giving AAPL intrinsic=206.25 instead of
           the blended 262.31. v5.64.0 moves PHASE-DD to AFTER
           `_compute_scores_fallback` + `_compute_recommendation`.

[PRESERVED] All v5.63.0 PHASE-AA/BB/CC/DD logic remains. All v5.62.0
PHASE-Z Yahoo enrichment remains. v5.47.2 baseline intact.

================================================================================

WHY v5.63.0 — FOUR-PHASE PRODUCTION FIX (May 13, 2026)
-------------------------------------------------------
Production refresh of Market_Leaders sheet revealed four distinct issues
after the v5.62.0 PHASE-Z deploy:

  PHASE-AA. Yahoo `.US` suffix stripping
            Yahoo doesn't recognize EODHD-style ".US" suffix. CIM.US,
            NTR.US, LW.US etc. fell through Yahoo enrichment and got
            only "recommendation" filled. Now `_yahoo_symbol_for()`
            strips ".US" and remaps ".XETRA" -> ".DE", ".LSE" -> ".L",
            ".PAR" -> ".PA", ".MIL" -> ".MI", and similar EODHD->Yahoo
            suffix translations.

  PHASE-BB. percent_change sanity guard
            Production showed bizarre values like "-744.05%" and
            "9,678.78%" for daily percent changes. Root cause was
            unit mismatch between fraction and percent points. New
            helpers `_sanitize_percent_change`, `_sanitize_price_change`,
            `_sanitize_week_52_position_pct` recompute from
            current_price/previous_close, clamp to ±50% daily, and tag
            warnings on overrides.

  PHASE-CC. International exchange rescue
            MTX.XETRA and similar non-US exchange symbols returned
            all-null rows because EODHD lacked coverage and the
            early-return guard in `_apply_yahoo_enrichment_pass`
            blocked Yahoo from being called. PHASE-CC removes that
            guard and adds current_price, previous_close, day_high,
            day_low, volume, open_price to `_YAHOO_CHART_FIELDS` so
            Yahoo can rescue the entire row.

  PHASE-DD. Restored v5.50.0 -> v5.61.0 derived columns
            The v5.47.2 baseline used for PHASE-Z lacks the
            v5.50.0-v5.61.0 features (Decision Matrix, intrinsic value,
            views, top factors/risks, conviction score). PHASE-DD
            restores best-effort reproductions of:
              - `_compute_intrinsic_and_upside`  (v5.58.0)
              - `_synthesize_market_cap_if_zero` (v5.55.0)
              - `_derive_views`                  (v5.56.0)
              - `_classify_recommendation_8tier` (v5.50.0)
              - `_build_top_factors_and_risks`   (v5.56.0)
            so Intrinsic Value, Upside %, Fundamental/Technical/Risk/
            Value View, Recommendation Detail, Top Factors, Top Risks,
            Conviction Score, Position Size Hint, Sector-Adj Score
            columns repopulate.

[PRESERVED] All v5.62.0 PHASE-Z (Yahoo enrichment pass) functionality
remains intact. v5.47.2 baseline structure remains intact.

================================================================================

WHY v5.47.2 (preserved baseline)
-----------
- FIX: makes provider priority page-aware so non-KSA pages like
       Global_Markets, Commodities_FX, and Mutual_Funds prefer EODHD first
       while KSA pages keep their protected local-first routing.
- (etc.)

Design goals
------------
- Never fail import because an optional module is missing.
- Never return an empty schema for a known page.
- Prefer live or external rows when available.
- Preserve schema-first contracts for route stability.
- Keep payloads JSON-safe and route-tolerant.
================================================================================
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

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

    def ConfigDict(**kwargs: Any) -> Dict[str, Any]:  # type: ignore
        return dict(kwargs)

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__version__ = "5.67.0"

logger = logging.getLogger("core.data_engine_v2")
logger.addHandler(logging.NullHandler())


# =============================================================================
# v5.62.0 PHASE-Z — Yahoo enrichment field maps and helpers
# =============================================================================

_YAHOO_FUNDAMENTAL_FIELDS: Tuple[str, ...] = (
    "industry", "sector", "currency", "country", "name",
    "market_cap", "float_shares", "shares_outstanding",
    "pe_ttm", "pe_forward", "eps_ttm", "eps_forward",
    "dividend_yield", "payout_ratio", "beta_5y",
    "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "revenue_ttm", "revenue_growth_yoy",
    "free_cash_flow_ttm", "roe", "roa", "earnings_growth_yoy",
    "pb_ratio", "ps_ratio", "peg_ratio", "ev_ebitda",
    "target_mean_price", "target_high_price", "target_low_price",
    "analyst_count", "recommendation",
)

_YAHOO_CHART_FIELDS: Tuple[str, ...] = (
    "rsi_14", "volatility_30d", "volatility_90d",
    "max_drawdown_1y", "var_95_1d", "sharpe_1y",
    "week_52_high", "week_52_low", "week_52_position_pct",
    "avg_volume_10d", "avg_volume_30d",
    "candlestick_pattern", "candlestick_signal",
    "candlestick_strength", "candlestick_confidence",
    "candlestick_patterns_recent",
    "current_price", "previous_close", "open_price",
    "day_high", "day_low", "volume",
)

_YAHOO_UNKNOWN_STRINGS: Set[str] = {
    "", "unknown", "unclassified", "n/a", "na", "none", "null",
    "nan", "-", "--", "not available",
}

_YAHOO_ENRICHMENT_LAST_PASS: Dict[str, Any] = {
    "ts": 0.0,
    "symbol": "",
    "fundamentals_called": False,
    "chart_called": False,
    "fundamentals_filled_fields": [],
    "chart_filled_fields": [],
}


def _yahoo_enrichment_enabled() -> bool:
    raw = (os.getenv("ENGINE_YAHOO_ENRICHMENT_ENABLED") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "f"}:
        return False
    return True


def _yahoo_enrich_on_missing_industry() -> bool:
    raw = (os.getenv("ENGINE_YAHOO_ENRICH_ON_MISSING_INDUSTRY") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "f"}:
        return False
    return True


def _yahoo_enrich_on_missing_risk_metrics() -> bool:
    raw = (os.getenv("ENGINE_YAHOO_ENRICH_ON_MISSING_RISK_METRICS") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off", "f"}:
        return False
    return True


def _is_missing_or_unknown_field(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        s = v.strip().lower()
        if s in _YAHOO_UNKNOWN_STRINGS:
            return True
        return False
    if isinstance(v, (list, tuple, set, dict)):
        return len(v) == 0
    return False


def _row_needs_yahoo_enrichment(row: Dict[str, Any]) -> Tuple[bool, bool]:
    if not isinstance(row, dict):
        return False, False

    needs_fund = False
    needs_chart = False

    if _yahoo_enrich_on_missing_industry():
        needs_fund = any(
            _is_missing_or_unknown_field(row.get(k))
            for k in _YAHOO_FUNDAMENTAL_FIELDS
        )

    if _yahoo_enrich_on_missing_risk_metrics():
        needs_chart = any(
            _is_missing_or_unknown_field(row.get(k))
            for k in _YAHOO_CHART_FIELDS
        )

    return needs_fund, needs_chart


def _filter_patch_to_missing_fields(
    row: Dict[str, Any],
    patch: Dict[str, Any],
    candidate_fields: Sequence[str],
) -> Tuple[Dict[str, Any], List[str]]:
    if not isinstance(patch, dict) or not patch:
        return {}, []

    filtered: Dict[str, Any] = {}
    filled: List[str] = []

    candidates: Set[str] = set(candidate_fields)
    for k, v in patch.items():
        if k not in candidates:
            continue
        if _is_missing_or_unknown_field(v):
            continue
        if not _is_missing_or_unknown_field(row.get(k)):
            continue
        filtered[k] = v
        filled.append(k)

    return filtered, filled


def _import_yahoo_provider_module(module_basename: str) -> Optional[Any]:
    candidates = (
        "core.providers." + module_basename,
        "providers." + module_basename,
    )
    for path in candidates:
        try:
            return import_module(path)
        except Exception:
            continue
    return None


# v5.63.0 PHASE-AA — Yahoo symbol normalization
_YAHOO_STRIP_SUFFIXES: Tuple[str, ...] = (
    ".US",
    ".us",
    ".USA",
    ".usa",
)

_YAHOO_SUFFIX_REMAP: Dict[str, str] = {
    ".XETRA": ".DE",
    ".XETR": ".DE",
    ".LSE": ".L",
    ".PAR": ".PA",
    ".AMS": ".AS",
    ".MIL": ".MI",
    ".MAD": ".MC",
    ".BRU": ".BR",
    ".STO": ".ST",
    ".HEL": ".HE",
    ".OSL": ".OL",
    ".CPH": ".CO",
    ".VIE": ".VI",
    ".WAR": ".WA",
    ".SWX": ".SW",
    ".SAU": ".SR",
    ".TADAWUL": ".SR",
    ".KSE": ".SR",
}


def _yahoo_symbol_for(symbol: str) -> str:
    if not isinstance(symbol, str):
        return ""
    s = symbol.strip()
    if not s:
        return ""

    for suf in _YAHOO_STRIP_SUFFIXES:
        if s.endswith(suf):
            return s[: -len(suf)]

    last_dot = s.rfind(".")
    if last_dot > 0:
        head, tail = s[:last_dot], s[last_dot:]
        tail_upper = tail.upper()
        if tail_upper in _YAHOO_SUFFIX_REMAP:
            return head + _YAHOO_SUFFIX_REMAP[tail_upper]

    return s


# v5.63.0 PHASE-BB — percent_change sanity guards
_PERCENT_CHANGE_DAILY_MAX_ABS: float = 50.0  # +/- 50% daily cap (points)
_WEEK_52_POSITION_MAX: float = 100.0


def _sanitize_percent_change(row: Dict[str, Any]) -> None:
    """
    v5.63.0 PHASE-BB / v5.67.0: Recompute and sanity-check percent_change
    in FRACTION form (was POINTS pre-v5.67.0). Aligns with the engine's
    v5.67.0 emit contract and the 04_Format.gs v2.7.0 FRACTION expectation.
    """
    if not isinstance(row, dict):
        return

    try:
        raw = row.get("percent_change")
        raw_f = float(raw) if raw is not None and raw != "" else None
    except (TypeError, ValueError):
        raw_f = None

    try:
        cp = row.get("current_price")
        pc = row.get("previous_close")
        cp_f = float(cp) if cp is not None and cp != "" else None
        pc_f = float(pc) if pc is not None and pc != "" else None
    except (TypeError, ValueError):
        cp_f = pc_f = None

    # v5.67.0: recomputed value is in FRACTION form (no ×100)
    recomputed: Optional[float] = None
    if cp_f is not None and pc_f is not None and pc_f != 0.0:
        recomputed = (cp_f - pc_f) / pc_f

    chosen = recomputed if recomputed is not None else raw_f
    if chosen is None:
        return

    # v5.67.0: if raw looks like POINTS (|raw| > 1.5) but recomputed fits
    # the fraction sanity range, trust the recomputed fraction.
    if raw_f is not None and abs(raw_f) > 1.5 and recomputed is not None:
        if abs(recomputed) <= _PERCENT_CHANGE_DAILY_MAX_ABS_FRACTION:
            row["percent_change"] = round(recomputed, 8)
            _append_yahoo_warning_tag(row, "percent_change_recomputed")
            return

    # Cap: daily fraction beyond 0.50 (50%) is almost certainly garbage
    if abs(chosen) > _PERCENT_CHANGE_DAILY_MAX_ABS_FRACTION:
        if recomputed is not None and abs(recomputed) <= _PERCENT_CHANGE_DAILY_MAX_ABS_FRACTION:
            row["percent_change"] = round(recomputed, 8)
            _append_yahoo_warning_tag(row, "percent_change_clamped_from_provider")
        else:
            row["percent_change"] = None
            _append_yahoo_warning_tag(row, "percent_change_suspect_dropped")
        return

    # Prefer recomputed if it diverges meaningfully (>1 percentage point)
    if recomputed is not None and raw_f is not None:
        if abs(recomputed - chosen) > 0.01:
            row["percent_change"] = round(recomputed, 8)
            _append_yahoo_warning_tag(row, "percent_change_recomputed")
            return

    row["percent_change"] = round(chosen, 8)


def _sanitize_week_52_position_pct(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    try:
        cp = row.get("current_price")
        hi = row.get("week_52_high")
        lo = row.get("week_52_low")
        cp_f = float(cp) if cp is not None and cp != "" else None
        hi_f = float(hi) if hi is not None and hi != "" else None
        lo_f = float(lo) if lo is not None and lo != "" else None
    except (TypeError, ValueError):
        return

    if cp_f is None or hi_f is None or lo_f is None:
        return
    if hi_f <= lo_f:
        return
    pct = ((cp_f - lo_f) / (hi_f - lo_f)) * 100.0
    pct = max(0.0, min(_WEEK_52_POSITION_MAX, pct))
    row["week_52_position_pct"] = round(pct, 6)


def _sanitize_price_change(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return
    try:
        cp = row.get("current_price")
        pc = row.get("previous_close")
        cp_f = float(cp) if cp is not None and cp != "" else None
        pc_f = float(pc) if pc is not None and pc != "" else None
    except (TypeError, ValueError):
        return
    if cp_f is None or pc_f is None:
        return
    row["price_change"] = round(cp_f - pc_f, 6)


def _apply_phase_bb_sanity(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    _sanitize_price_change(row)
    _sanitize_percent_change(row)
    _sanitize_week_52_position_pct(row)
    return row


# =============================================================================
# v5.63.0 PHASE-DD — Restored derived/synthesized columns
# =============================================================================

_INTRINSIC_UPSIDE_MIN_PCT: float = -90.0
_INTRINSIC_UPSIDE_MAX_PCT: float = 200.0


def _compute_intrinsic_and_upside(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return
    if row.get("intrinsic_value") is not None and row.get("upside_pct") is not None:
        return

    cp = _as_float(row.get("current_price"))
    if cp is None or cp <= 0:
        return

    eps = _as_float(row.get("eps_ttm"))
    forecast_12m = _as_float(row.get("forecast_price_12m"))
    pb = _as_float(row.get("pb_ratio"))
    pe_ttm = _as_float(row.get("pe_ttm"))
    sector = _safe_str(row.get("sector")).lower()

    sector_pe_map = {
        "technology": 25.0,
        "consumer electronics": 22.0,
        "communication services": 18.0,
        "financial services": 13.0,
        "healthcare": 22.0,
        "consumer defensive": 22.0,
        "consumer cyclical": 18.0,
        "industrials": 19.0,
        "energy": 12.0,
        "utilities": 17.0,
        "real estate": 25.0,
        "basic materials": 15.0,
    }
    fair_pe = sector_pe_map.get(sector, 18.0)

    candidates: List[float] = []
    weights: List[float] = []

    if eps is not None and eps > 0:
        pe_fair = eps * fair_pe
        if pe_fair > 0:
            candidates.append(pe_fair)
            weights.append(0.4)

    if forecast_12m is not None and forecast_12m > 0:
        candidates.append(forecast_12m)
        weights.append(0.4)

    if pb is not None and pb > 0 and pb < 20:
        book_value = cp / pb
        if book_value > 0:
            candidates.append(book_value * 1.5)
            weights.append(0.2)

    if not candidates:
        return

    total_w = sum(weights)
    if total_w <= 0:
        return
    intrinsic = sum(c * w for c, w in zip(candidates, weights)) / total_w

    # v5.67.0: emit upside_pct as FRACTION (was POINTS). The cap constants
    # _INTRINSIC_UPSIDE_MIN_PCT / MAX_PCT remain in points form for
    # readability, so divide by 100 to get equivalent fraction bounds.
    upside_fraction = (intrinsic - cp) / cp
    upside_fraction = max(_INTRINSIC_UPSIDE_MIN_PCT / 100.0,
                          min(_INTRINSIC_UPSIDE_MAX_PCT / 100.0, upside_fraction))

    row["intrinsic_value"] = round(intrinsic, 4)
    row["upside_pct"] = round(upside_fraction, 6)


def _synthesize_market_cap_if_zero(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return
    mc = _as_float(row.get("market_cap"))
    if mc is not None and mc > 0:
        return

    cp = _as_float(row.get("current_price"))
    shares = _as_float(row.get("float_shares"))
    if shares is None:
        shares = _as_float(row.get("shares_outstanding"))

    if cp is not None and cp > 0 and shares is not None and shares > 0:
        row["market_cap"] = round(cp * shares, 2)


def _derive_views(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    quality = _as_float(row.get("quality_score"))
    growth = _as_float(row.get("growth_score"))
    if quality is not None and growth is not None:
        combined = (quality + growth) / 2.0
        if combined >= 70:
            fv = "STRONG"
        elif combined >= 55:
            fv = "POSITIVE"
        elif combined >= 40:
            fv = "NEUTRAL"
        else:
            fv = "WEAK"
        row["fundamental_view"] = row.get("fundamental_view") or fv

    momentum = _as_float(row.get("momentum_score"))
    rsi = _as_float(row.get("rsi_14"))
    if momentum is not None:
        if rsi is not None and rsi > 70:
            tv = "OVERBOUGHT"
        elif rsi is not None and rsi < 30:
            tv = "OVERSOLD"
        elif momentum >= 70:
            tv = "BULLISH"
        elif momentum >= 50:
            tv = "POSITIVE"
        elif momentum >= 30:
            tv = "NEUTRAL"
        else:
            tv = "BEARISH"
        row["technical_view"] = row.get("technical_view") or tv

    risk_bucket = _safe_str(row.get("risk_bucket")).upper()
    if risk_bucket:
        rv_map = {"LOW": "LOW", "MODERATE": "MODERATE", "HIGH": "HIGH"}
        row["risk_view"] = row.get("risk_view") or rv_map.get(risk_bucket, risk_bucket)

    # v5.67.0: upside_pct is now stored as FRACTION. _as_pct_points
    # gracefully handles both fraction and points input.
    upside_points = _as_pct_points(row.get("upside_pct"))
    if upside_points is not None:
        if upside_points >= 30:
            vv = "CHEAP"
        elif upside_points >= 10:
            vv = "FAIR"
        elif upside_points >= -10:
            vv = "FULL"
        else:
            vv = "EXPENSIVE"
        row["value_view"] = row.get("value_view") or vv


def _classify_recommendation_8tier(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    overall = _as_float(row.get("overall_score"))
    # v5.67.0: upside_pct is now stored as FRACTION. _as_pct_points
    # gracefully handles both fraction and points input.
    upside_points = _as_pct_points(row.get("upside_pct"))
    risk_bucket = _safe_str(row.get("risk_bucket")).upper()
    value_view = _safe_str(row.get("value_view")).upper()
    technical = _safe_str(row.get("technical_view")).upper()
    fundamental = _safe_str(row.get("fundamental_view")).upper()

    if overall is None:
        return

    if overall >= 80 and upside_points is not None and upside_points >= 25 and risk_bucket != "HIGH":
        rec = "STRONG_BUY"
        priority = 1
    elif overall >= 70 and upside_points is not None and upside_points >= 15:
        rec = "BUY"
        priority = 2
    elif overall >= 60 and (value_view in ("CHEAP", "FAIR") or technical == "BULLISH"):
        rec = "ACCUMULATE"
        priority = 3
    elif overall >= 40 or (overall is not None and 35 <= overall < 60):
        rec = "HOLD"
        priority = 4
    elif overall >= 30 and value_view == "EXPENSIVE":
        rec = "REDUCE"
        priority = 5
    elif overall >= 20:
        rec = "SELL"
        priority = 6
    elif overall < 20 and risk_bucket == "HIGH":
        rec = "STRONG_SELL"
        priority = 7
    else:
        rec = "AVOID"
        priority = 8

    if not row.get("recommendation"):
        row["recommendation"] = rec
    if not row.get("recommendation_priority"):
        row["recommendation_priority"] = priority
    if not row.get("recommendation_detailed"):
        row["recommendation_detailed"] = rec

    fv = fundamental or "NEUTRAL"
    tv = technical or "NEUTRAL"
    rv = risk_bucket or "MODERATE"
    vv = value_view or "FAIR"
    actual_rec = row.get("recommendation", rec)
    if not row.get("recommendation_reason"):
        row["recommendation_reason"] = (
            f"P{priority} [{actual_rec}]: "
            f"Fund {fv} | Tech {tv} | Risk {rv} | Val {vv}"
        )


def _build_top_factors_and_risks(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    factors: List[str] = []
    risks: List[str] = []

    # v5.67.0: upside_pct is now stored as FRACTION. _as_pct_points
    # gracefully handles both fraction and points input.
    upside_points = _as_pct_points(row.get("upside_pct"))
    if upside_points is not None and upside_points >= 15:
        factors.append("Attractive valuation")
    momentum = _as_float(row.get("momentum_score"))
    if momentum is not None and momentum >= 70:
        factors.append("Positive momentum")
    quality = _as_float(row.get("quality_score"))
    if quality is not None and quality >= 70:
        factors.append("Strong fundamentals")
    growth = _as_float(row.get("growth_score"))
    if growth is not None and growth >= 70:
        factors.append("Solid growth")
    div_yield = _as_float(row.get("dividend_yield"))
    if div_yield is not None and div_yield >= 0.03:
        factors.append("Dividend income")
    rgyoy = _as_float(row.get("revenue_growth_yoy"))
    if rgyoy is not None and rgyoy >= 0.10:
        factors.append("Revenue growth")

    if not factors:
        factors.append("Limited positive signals")

    risk_bucket = _safe_str(row.get("risk_bucket")).upper()
    if risk_bucket == "HIGH":
        risks.append("High volatility")
    vol = _as_float(row.get("volatility_30d"))
    if vol is not None and vol >= 0.40:
        risks.append("Elevated volatility")
    # v5.67.0: max_drawdown_1y is now stored as SIGNED FRACTION (e.g. -0.338).
    # _as_pct_points converts to signed points (-33.8). abs() handles the sign.
    dd_points = _as_pct_points(row.get("max_drawdown_1y"))
    if dd_points is not None and abs(dd_points) >= 30:
        risks.append("Recent drawdown")
    rsi = _as_float(row.get("rsi_14"))
    if rsi is not None and rsi > 75:
        risks.append("Overbought (RSI)")
    elif rsi is not None and rsi < 25:
        risks.append("Oversold (RSI)")
    de = _as_float(row.get("debt_to_equity"))
    if de is not None and de >= 100:
        risks.append("High leverage")
    pe = _as_float(row.get("pe_ttm"))
    if pe is not None and pe >= 50:
        risks.append("Expensive valuation")

    if not risks:
        risks.append("Limited downside signals")

    conviction = 50.0 + (len(factors) - len(risks)) * 8.0
    conviction = max(0.0, min(100.0, conviction))

    overall = _as_float(row.get("overall_score"))
    if overall is not None:
        sector_adj = overall + (5.0 if "Strong fundamentals" in factors else 0.0) - (5.0 if risk_bucket == "HIGH" else 0.0)
        sector_adj = max(0.0, min(100.0, sector_adj))
        if not row.get("sector_relative_score"):
            row["sector_relative_score"] = round(sector_adj, 2)

    if not row.get("top_factors"):
        row["top_factors"] = "; ".join(factors[:3])
    if not row.get("top_risks"):
        row["top_risks"] = "; ".join(risks[:3])
    if not row.get("conviction_score"):
        row["conviction_score"] = round(conviction, 2)

    rec = _safe_str(row.get("recommendation")).upper()
    if rec in ("STRONG_BUY",):
        psh = "Core position"
    elif rec in ("BUY", "ACCUMULATE"):
        psh = "Standard position"
    elif rec == "HOLD":
        psh = "Maintain or trim"
    else:
        psh = "Avoid / reduce"
    if not row.get("position_size_hint"):
        row["position_size_hint"] = psh


# =============================================================================
# v5.66.0 PHASE-JJ — Candlestick pattern detection
# =============================================================================

_CS_SIGNAL_BULLISH = "BULLISH"
_CS_SIGNAL_BEARISH = "BEARISH"
_CS_SIGNAL_NEUTRAL = "NEUTRAL"
_CS_SIGNAL_DOJI = "DOJI"

_CS_STRENGTH_STRONG = "STRONG"
_CS_STRENGTH_MODERATE = "MODERATE"
_CS_STRENGTH_WEAK = "WEAK"

_CS_P_DOJI = "Doji"
_CS_P_HAMMER = "Hammer"
_CS_P_INVERTED_HAMMER = "Inverted Hammer"
_CS_P_SHOOTING_STAR = "Shooting Star"
_CS_P_HANGING_MAN = "Hanging Man"
_CS_P_MARUBOZU_BULL = "Bullish Marubozu"
_CS_P_MARUBOZU_BEAR = "Bearish Marubozu"
_CS_P_BULL_ENGULFING = "Bullish Engulfing"
_CS_P_BEAR_ENGULFING = "Bearish Engulfing"
_CS_P_MORNING_STAR = "Morning Star"
_CS_P_EVENING_STAR = "Evening Star"

_CS_PATTERN_SIGNAL: Dict[str, str] = {
    _CS_P_DOJI: _CS_SIGNAL_DOJI,
    _CS_P_HAMMER: _CS_SIGNAL_BULLISH,
    _CS_P_INVERTED_HAMMER: _CS_SIGNAL_BULLISH,
    _CS_P_SHOOTING_STAR: _CS_SIGNAL_BEARISH,
    _CS_P_HANGING_MAN: _CS_SIGNAL_BEARISH,
    _CS_P_MARUBOZU_BULL: _CS_SIGNAL_BULLISH,
    _CS_P_MARUBOZU_BEAR: _CS_SIGNAL_BEARISH,
    _CS_P_BULL_ENGULFING: _CS_SIGNAL_BULLISH,
    _CS_P_BEAR_ENGULFING: _CS_SIGNAL_BEARISH,
    _CS_P_MORNING_STAR: _CS_SIGNAL_BULLISH,
    _CS_P_EVENING_STAR: _CS_SIGNAL_BEARISH,
}

_CS_DOJI_BODY_RATIO_MAX = 0.10
_CS_MARUBOZU_BODY_RATIO_MIN = 0.95
_CS_HAMMER_SHADOW_MULTIPLIER = 2.0
_CS_HAMMER_OPP_SHADOW_RATIO_MAX = 0.30
_CS_SMALL_BODY_RATIO_MAX = 0.30
_CS_LONG_BODY_RATIO_MIN = 0.55
_CS_TREND_LOOKBACK = 10
_CS_TREND_THRESHOLD = 0.01
_CS_RECENT_LOOKBACK = 5


def _cs_coerce_bar(row: Any) -> Optional[Dict[str, float]]:
    if not isinstance(row, dict):
        return None
    o = _as_float(row.get("open") if row.get("open") is not None else row.get("o"))
    h = _as_float(row.get("high") if row.get("high") is not None else row.get("h"))
    low = _as_float(row.get("low") if row.get("low") is not None else row.get("l"))
    c = _as_float(
        row.get("close")
        if row.get("close") is not None
        else (
            row.get("adjusted_close")
            if row.get("adjusted_close") is not None
            else (row.get("adjclose") if row.get("adjclose") is not None else row.get("c"))
        )
    )
    if c is None:
        return None
    if o is None:
        o = c
    if h is None:
        h = max(o, c)
    if low is None:
        low = min(o, c)
    if h < low:
        h, low = low, h
    v = _as_float(row.get("volume") if row.get("volume") is not None else row.get("v")) or 0.0
    return {"open": o, "high": h, "low": low, "close": c, "volume": v}


def _cs_coerce_bars(rows: Any) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    if not rows:
        return out
    try:
        iterable = list(rows)
    except Exception:
        return out
    for row in iterable:
        bar = _cs_coerce_bar(row)
        if bar is not None:
            out.append(bar)
    return out


def _cs_bar_geom(bar: Dict[str, float]) -> Dict[str, float]:
    o = bar["open"]; h = bar["high"]; low = bar["low"]; c = bar["close"]
    rng = max(h - low, 1e-12)
    body = abs(c - o)
    upper_shadow = max(h - max(o, c), 0.0)
    lower_shadow = max(min(o, c) - low, 0.0)
    return {
        "open": o, "high": h, "low": low, "close": c, "range": rng,
        "body": body, "body_ratio": body / rng,
        "upper_shadow": upper_shadow, "lower_shadow": lower_shadow,
        "upper_shadow_ratio": upper_shadow / rng,
        "lower_shadow_ratio": lower_shadow / rng,
        "is_bullish": c > o, "is_bearish": c < o,
        "midpoint": (o + c) / 2.0,
    }


def _cs_trend_at(bars: List[Dict[str, float]], idx: int) -> str:
    start = idx - _CS_TREND_LOOKBACK
    if start < 0:
        return "FLAT"
    window = bars[start:idx]
    if len(window) < 3:
        return "FLAT"
    first = window[0]["close"]
    last = window[-1]["close"]
    if first <= 0:
        return "FLAT"
    pct = (last - first) / first
    if pct >= _CS_TREND_THRESHOLD:
        return "UP"
    if pct <= -_CS_TREND_THRESHOLD:
        return "DOWN"
    return "FLAT"


def _cs_detect_marubozu(g: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g["body_ratio"] < _CS_MARUBOZU_BODY_RATIO_MIN:
        return None
    confidence = 50.0 + (g["body_ratio"] - _CS_MARUBOZU_BODY_RATIO_MIN) * 1000.0
    confidence = max(50.0, min(100.0, confidence))
    if g["is_bullish"]:
        return _CS_P_MARUBOZU_BULL, confidence
    if g["is_bearish"]:
        return _CS_P_MARUBOZU_BEAR, confidence
    return None


def _cs_detect_doji(g: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g["body_ratio"] >= _CS_DOJI_BODY_RATIO_MAX:
        return None
    raw = 100.0 * (1.0 - g["body_ratio"] / _CS_DOJI_BODY_RATIO_MAX)
    confidence = max(40.0, min(95.0, raw))
    return _CS_P_DOJI, confidence


def _cs_has_hammer_shape(g: Dict[str, float]) -> bool:
    if g["body"] <= 0:
        return False
    if g["lower_shadow"] < _CS_HAMMER_SHADOW_MULTIPLIER * g["body"]:
        return False
    if g["upper_shadow_ratio"] > _CS_HAMMER_OPP_SHADOW_RATIO_MAX:
        return False
    return True


def _cs_has_inverted_hammer_shape(g: Dict[str, float]) -> bool:
    if g["body"] <= 0:
        return False
    if g["upper_shadow"] < _CS_HAMMER_SHADOW_MULTIPLIER * g["body"]:
        return False
    if g["lower_shadow_ratio"] > _CS_HAMMER_OPP_SHADOW_RATIO_MAX:
        return False
    return True


def _cs_detect_hammer_family(g: Dict[str, float], trend: str) -> Optional[Tuple[str, float]]:
    if _cs_has_hammer_shape(g):
        ratio = g["lower_shadow"] / max(g["body"], 1e-9)
        base_conf = min(85.0, 40.0 + ratio * 8.0)
        if trend == "DOWN":
            return _CS_P_HAMMER, base_conf
        if trend == "UP":
            return _CS_P_HANGING_MAN, base_conf * 0.85
        return _CS_P_HAMMER, base_conf * 0.65
    if _cs_has_inverted_hammer_shape(g):
        ratio = g["upper_shadow"] / max(g["body"], 1e-9)
        base_conf = min(80.0, 35.0 + ratio * 8.0)
        if trend == "DOWN":
            return _CS_P_INVERTED_HAMMER, base_conf
        if trend == "UP":
            return _CS_P_SHOOTING_STAR, base_conf
        return _CS_P_INVERTED_HAMMER, base_conf * 0.65
    return None


def _cs_detect_engulfing(g_prev: Dict[str, float], g_curr: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g_prev["body_ratio"] < _CS_DOJI_BODY_RATIO_MAX:
        return None
    if g_curr["body_ratio"] < _CS_DOJI_BODY_RATIO_MAX:
        return None
    prev_open = g_prev["open"]; prev_close = g_prev["close"]
    curr_open = g_curr["open"]; curr_close = g_curr["close"]
    if g_prev["is_bearish"] and g_curr["is_bullish"]:
        if curr_open <= prev_close and curr_close >= prev_open:
            engulf_factor = g_curr["body"] / max(g_prev["body"], 1e-9)
            confidence = min(90.0, 50.0 + engulf_factor * 10.0)
            return _CS_P_BULL_ENGULFING, confidence
    if g_prev["is_bullish"] and g_curr["is_bearish"]:
        if curr_open >= prev_close and curr_close <= prev_open:
            engulf_factor = g_curr["body"] / max(g_prev["body"], 1e-9)
            confidence = min(90.0, 50.0 + engulf_factor * 10.0)
            return _CS_P_BEAR_ENGULFING, confidence
    return None


def _cs_detect_star(g_first: Dict[str, float], g_star: Dict[str, float], g_third: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if g_star["body_ratio"] >= _CS_SMALL_BODY_RATIO_MAX:
        return None
    if g_first["body_ratio"] < _CS_LONG_BODY_RATIO_MIN:
        return None
    if g_third["body_ratio"] < _CS_LONG_BODY_RATIO_MIN:
        return None
    if g_first["is_bearish"] and g_third["is_bullish"]:
        if g_third["close"] > g_first["midpoint"]:
            penetration = (g_third["close"] - g_first["midpoint"]) / max(g_first["body"], 1e-9)
            confidence = min(95.0, 60.0 + penetration * 30.0)
            return _CS_P_MORNING_STAR, confidence
    if g_first["is_bullish"] and g_third["is_bearish"]:
        if g_third["close"] < g_first["midpoint"]:
            penetration = (g_first["midpoint"] - g_third["close"]) / max(g_first["body"], 1e-9)
            confidence = min(95.0, 60.0 + penetration * 30.0)
            return _CS_P_EVENING_STAR, confidence
    return None


def _cs_detect_at_index(bars: List[Dict[str, float]], idx: int) -> Optional[Tuple[str, float]]:
    if idx < 0 or idx >= len(bars):
        return None
    trend = _cs_trend_at(bars, idx)
    g_curr = _cs_bar_geom(bars[idx])
    if idx >= 2:
        g_first = _cs_bar_geom(bars[idx - 2])
        g_star = _cs_bar_geom(bars[idx - 1])
        star = _cs_detect_star(g_first, g_star, g_curr)
        if star is not None:
            return star
    if idx >= 1:
        g_prev = _cs_bar_geom(bars[idx - 1])
        eng = _cs_detect_engulfing(g_prev, g_curr)
        if eng is not None:
            return eng
    marubozu = _cs_detect_marubozu(g_curr)
    if marubozu is not None:
        return marubozu
    hammer = _cs_detect_hammer_family(g_curr, trend)
    if hammer is not None:
        return hammer
    doji = _cs_detect_doji(g_curr)
    if doji is not None:
        return doji
    return None


def _cs_confidence_to_strength(confidence: float) -> str:
    if confidence >= 75.0:
        return _CS_STRENGTH_STRONG
    if confidence >= 55.0:
        return _CS_STRENGTH_MODERATE
    return _CS_STRENGTH_WEAK


def detect_candlestick_patterns(rows: Any) -> Dict[str, Any]:
    empty = {
        "candlestick_pattern": "",
        "candlestick_signal": _CS_SIGNAL_NEUTRAL,
        "candlestick_strength": "",
        "candlestick_confidence": 0.0,
        "candlestick_patterns_recent": "",
    }
    bars = _cs_coerce_bars(rows)
    if not bars:
        return empty
    last_idx = len(bars) - 1
    latest = _cs_detect_at_index(bars, last_idx)
    if latest is None:
        out = dict(empty)
    else:
        pattern, confidence = latest
        out = {
            "candlestick_pattern": pattern,
            "candlestick_signal": _CS_PATTERN_SIGNAL.get(pattern, _CS_SIGNAL_NEUTRAL),
            "candlestick_strength": _cs_confidence_to_strength(confidence),
            "candlestick_confidence": round(float(confidence), 2),
            "candlestick_patterns_recent": "",
        }
    recent_patterns: List[str] = []
    start = max(0, last_idx - _CS_RECENT_LOOKBACK + 1)
    for i in range(start, last_idx + 1):
        det = _cs_detect_at_index(bars, i)
        if det is None:
            continue
        name, _ = det
        recent_patterns.append(name)
    out["candlestick_patterns_recent"] = " | ".join(recent_patterns)
    return out


def _apply_phase_dd_enhancements(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    _synthesize_market_cap_if_zero(row)
    _compute_intrinsic_and_upside(row)
    _derive_views(row)
    _classify_recommendation_8tier(row)
    _build_top_factors_and_risks(row)
    _phase_ii_quality_forecast(row)
    return row


# =============================================================================
# v5.65.0 PHASE-II — Quality forecast generator
# =============================================================================

_PHASE_II_MAX_12M_ABS_RETURN: float = 0.30
_PHASE_II_MIN_12M_ABS_RETURN: float = -0.30
_PHASE_II_VOL_BAND_FACTOR: float = 0.5
_PHASE_II_CONF_MIN: float = 0.30
_PHASE_II_CONF_MAX: float = 0.85


def _phase_ii_quality_forecast(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return

    cp = _as_float(row.get("current_price"))
    if cp is None or cp <= 0:
        return

    intrinsic = _as_float(row.get("intrinsic_value"))
    momentum = _as_float(row.get("momentum_score"))
    quality = _as_float(row.get("quality_score"))
    value = _as_float(row.get("value_score"))
    growth = _as_float(row.get("growth_score"))
    overall = _as_float(row.get("overall_score"))
    vol_30d = _as_float(row.get("volatility_30d"))
    vol_90d = _as_float(row.get("volatility_90d"))
    rsi = _as_float(row.get("rsi_14"))
    risk_bucket = _safe_str(row.get("risk_bucket")).upper()

    components: List[Tuple[float, float]] = []

    if intrinsic is not None and intrinsic > 0:
        reversion_return = (intrinsic - cp) / cp
        reversion_return *= 0.6
        components.append((reversion_return, 0.40))

    if momentum is not None:
        trend_return = ((momentum - 50.0) / 50.0) * 0.15
        components.append((trend_return, 0.30))

    fundamentals_signals: List[float] = []
    for sub in (quality, value, growth):
        if sub is not None:
            fundamentals_signals.append((sub - 50.0) / 50.0)
    if fundamentals_signals:
        avg_fund = sum(fundamentals_signals) / len(fundamentals_signals)
        fund_return = avg_fund * 0.10
        components.append((fund_return, 0.20))

    if overall is not None:
        baseline_return = ((overall - 50.0) / 50.0) * 0.08
        components.append((baseline_return, 0.10))

    if not components:
        return

    total_weight = sum(w for _, w in components)
    if total_weight <= 0:
        return
    expected_12m_return = sum(r * w for r, w in components) / total_weight

    if rsi is not None:
        if rsi > 75:
            expected_12m_return -= 0.03
        elif rsi > 70:
            expected_12m_return -= 0.015
        elif rsi < 25:
            expected_12m_return += 0.03
        elif rsi < 30:
            expected_12m_return += 0.015

    expected_12m_return = max(_PHASE_II_MIN_12M_ABS_RETURN,
                              min(_PHASE_II_MAX_12M_ABS_RETURN, expected_12m_return))

    expected_3m_return = expected_12m_return * 0.35
    expected_1m_return = expected_12m_return * 0.12

    forecast_12m = cp * (1.0 + expected_12m_return)
    forecast_3m = cp * (1.0 + expected_3m_return)
    forecast_1m = cp * (1.0 + expected_1m_return)

    row["forecast_price_1m"] = round(forecast_1m, 4)
    row["forecast_price_3m"] = round(forecast_3m, 4)
    row["forecast_price_12m"] = round(forecast_12m, 4)
    row["expected_roi_1m"] = round(expected_1m_return, 6)
    row["expected_roi_3m"] = round(expected_3m_return, 6)
    row["expected_roi_12m"] = round(expected_12m_return, 6)

    conf = 0.50

    completeness_fields = [
        "pe_ttm", "pb_ratio", "eps_ttm", "dividend_yield",
        "revenue_growth_yoy", "gross_margin", "operating_margin",
        "rsi_14", "volatility_30d", "max_drawdown_1y",
    ]
    present = sum(1 for f in completeness_fields if _as_float(row.get(f)) is not None)
    completeness_ratio = present / len(completeness_fields)
    conf += completeness_ratio * 0.20

    sub_scores = [s for s in (quality, value, growth, momentum) if s is not None]
    if len(sub_scores) >= 3:
        mean_s = sum(sub_scores) / len(sub_scores)
        variance = sum((s - mean_s) ** 2 for s in sub_scores) / len(sub_scores)
        agreement = max(0.0, 1.0 - (variance / 800.0))
        conf += agreement * 0.10

    if risk_bucket == "HIGH":
        conf -= 0.10
    if vol_90d is not None and vol_90d > 0.50:
        conf -= 0.05
    elif vol_30d is not None and vol_30d > 0.45:
        conf -= 0.03

    if abs(expected_12m_return) > 0.25:
        conf -= 0.05

    conf = max(_PHASE_II_CONF_MIN, min(_PHASE_II_CONF_MAX, conf))

    row["forecast_confidence"] = round(conf, 4)
    row["confidence_score"] = round(conf * 100.0, 2)
    if conf >= 0.70:
        row["confidence_bucket"] = "HIGH"
    elif conf >= 0.50:
        row["confidence_bucket"] = "MODERATE"
    else:
        row["confidence_bucket"] = "LOW"


def _pick_yahoo_callable(mod: Any, *names: str) -> Optional[Any]:
    if mod is None:
        return None
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


def _append_yahoo_warning_tag(row: Dict[str, Any], tag: str) -> None:
    if not tag:
        return
    existing = row.get("warnings")
    if isinstance(existing, list):
        if tag not in existing:
            existing.append(tag)
        return
    s = str(existing or "")
    if tag in s:
        return
    row["warnings"] = (s + "; " + tag) if s else tag


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


# =============================================================================
# Canonical page contracts
# =============================================================================
INSTRUMENT_CANONICAL_KEYS: List[str] = [
    "symbol", "name", "asset_class", "exchange", "currency", "country", "sector", "industry",
    "current_price", "previous_close", "open_price", "day_high", "day_low",
    "week_52_high", "week_52_low", "price_change", "percent_change", "week_52_position_pct",
    "volume", "avg_volume_10d", "avg_volume_30d", "market_cap", "float_shares", "beta_5y",
    "pe_ttm", "pe_forward", "eps_ttm", "dividend_yield", "payout_ratio",
    "revenue_ttm", "revenue_growth_yoy", "gross_margin", "operating_margin", "profit_margin",
    "debt_to_equity", "free_cash_flow_ttm",
    "rsi_14", "volatility_30d", "volatility_90d", "max_drawdown_1y",
    "var_95_1d", "sharpe_1y", "risk_score", "risk_bucket",
    "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio", "intrinsic_value",
    "upside_pct", "valuation_score",
    "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
    "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
    "forecast_confidence", "confidence_score", "confidence_bucket",
    "value_score", "quality_score", "momentum_score", "growth_score",
    "overall_score", "opportunity_score", "rank_overall",
    "fundamental_view", "technical_view", "risk_view", "value_view",
    "recommendation", "recommendation_reason", "horizon_days", "invest_period_label",
    "position_qty", "avg_cost", "position_cost", "position_value",
    "unrealized_pl", "unrealized_pl_pct",
    "data_provider", "last_updated_utc", "last_updated_riyadh", "warnings",
    "sector_relative_score", "conviction_score", "top_factors", "top_risks",
    "position_size_hint", "recommendation_detailed", "recommendation_priority",
    "candlestick_pattern", "candlestick_signal", "candlestick_strength",
    "candlestick_confidence", "candlestick_patterns_recent",
]

INSTRUMENT_CANONICAL_HEADERS: List[str] = [
    "Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry",
    "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Percent Change", "52W Position %",
    "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)",
    "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio",
    "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin", "Operating Margin",
    "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)",
    "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y",
    "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket",
    "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Forecast Confidence", "Confidence Score", "Confidence Bucket",
    "Value Score", "Quality Score", "Momentum Score", "Growth Score",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    "Fundamental View", "Technical View", "Risk View", "Value View",
    "Recommendation", "Recommendation Reason", "Horizon Days", "Invest Period Label",
    "Position Qty", "Avg Cost", "Position Cost", "Position Value",
    "Unrealized P/L", "Unrealized P/L %",
    "Data Provider", "Last Updated (UTC)", "Last Updated (Riyadh)", "Warnings",
    "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks",
    "Position Size Hint", "Recommendation Detail", "Reco Priority",
    "Candle Pattern", "Candle Signal", "Candle Strength",
    "Candle Confidence", "Recent Patterns (5D)",
]

TOP10_REQUIRED_FIELDS: Tuple[str, ...] = (
    "top10_rank",
    "selection_reason",
    "criteria_snapshot",
)
TOP10_REQUIRED_HEADERS: Dict[str, str] = {
    "top10_rank": "Top10 Rank",
    "selection_reason": "Selection Reason",
    "criteria_snapshot": "Criteria Snapshot",
}

INSIGHTS_HEADERS: List[str] = [
    "Section", "Item", "Metric", "Value", "Notes", "Source", "Sort Order",
]
INSIGHTS_KEYS: List[str] = [
    "section", "item", "metric", "value", "notes", "source", "sort_order",
]

DATA_DICTIONARY_HEADERS: List[str] = [
    "Sheet", "Group", "Header", "Key", "DType", "Format", "Required", "Source", "Notes",
]
DATA_DICTIONARY_KEYS: List[str] = [
    "sheet", "group", "header", "key", "dtype", "fmt", "required", "source", "notes",
]

STATIC_CANONICAL_SHEET_CONTRACTS: Dict[str, Dict[str, List[str]]] = {
    "Market_Leaders": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Global_Markets": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Commodities_FX": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Mutual_Funds": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "My_Portfolio": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "My_Investments": {"headers": list(INSTRUMENT_CANONICAL_HEADERS), "keys": list(INSTRUMENT_CANONICAL_KEYS)},
    "Top_10_Investments": {
        "headers": list(INSTRUMENT_CANONICAL_HEADERS) + [TOP10_REQUIRED_HEADERS[k] for k in TOP10_REQUIRED_FIELDS],
        "keys": list(INSTRUMENT_CANONICAL_KEYS) + list(TOP10_REQUIRED_FIELDS),
    },
    "Insights_Analysis": {"headers": list(INSIGHTS_HEADERS), "keys": list(INSIGHTS_KEYS)},
    "Data_Dictionary": {"headers": list(DATA_DICTIONARY_HEADERS), "keys": list(DATA_DICTIONARY_KEYS)},
}

INSTRUMENT_SHEETS: Set[str] = {
    "Market_Leaders", "Global_Markets", "Commodities_FX",
    "Mutual_Funds", "My_Portfolio", "My_Investments", "Top_10_Investments",
}
SPECIAL_SHEETS: Set[str] = {"Insights_Analysis", "Data_Dictionary"}

TOP10_ENGINE_DEFAULT_PAGES: List[str] = [
    "Market_Leaders", "Global_Markets", "Commodities_FX",
    "Mutual_Funds", "My_Portfolio", "My_Investments",
]

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
PAGE_PRIMARY_PROVIDER_DEFAULTS = {page: "eodhd" for page in NON_KSA_EODHD_PRIMARY_PAGES}
PROVIDER_PRIORITIES = {
    "tadawul": 10,
    "argaam": 20,
    "eodhd": 30,
    "yahoo": 40,
    "finnhub": 50,
    "yahoo_chart": 60,
}


# =============================================================================
# Small helpers
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


def _as_pct_fraction(x: Any) -> Optional[float]:
    v = _as_float(x)
    if v is None:
        return None
    if abs(v) > 1.5:
        return v / 100.0
    return v


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
                    text = _safe_str(val)
                    if text:
                        return text

        for attr_name in ("PAGE_ALIASES", "SHEET_ALIASES", "ALIASES", "PAGE_NAME_ALIASES"):
            mapping = getattr(mod, attr_name, None)
            if isinstance(mapping, dict):
                for cand in (raw, raw.replace(" ", "_"), raw.replace("-", "_"), _norm_key(raw), _norm_key_loose(raw)):
                    for key, val in mapping.items():
                        if cand in {_safe_str(key), _norm_key(_safe_str(key)), _norm_key_loose(_safe_str(key))}:
                            text = _safe_str(val)
                            if text:
                                return text
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
    if not s:
        return False
    if len(s) > 24:
        return False
    if re.match(r"^[A-Z0-9.=\-:^/]{1,24}$", s):
        return True
    if re.match(r"^[0-9]{4}(\.SR)?$", s):
        return True
    return False


def normalize_symbol(symbol: str) -> str:
    return _safe_str(symbol).upper()


def get_symbol_info(symbol: str) -> Dict[str, Any]:
    s = normalize_symbol(symbol)
    return {
        "requested": _safe_str(symbol),
        "normalized": s,
        "is_ksa": s.endswith(".SR") or re.match(r"^[0-9]{4}$", s) is not None,
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
    if not s:
        return []
    parts = re.split(r"[,;|\s]+", s)
    return [p.strip() for p in parts if p.strip()]


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


def _extract_nested_dict(payload: Dict[str, Any], key: str) -> Dict[str, Any]:
    val = payload.get(key)
    return dict(val) if isinstance(val, dict) else {}


def _extract_requested_symbols_from_body(body: Optional[Dict[str, Any]], limit: int = 5000) -> List[str]:
    if not isinstance(body, dict):
        return []
    raw: List[str] = []
    for key in (
        "symbols", "tickers", "selected_symbols", "direct_symbols", "codes",
        "watchlist", "portfolio_symbols", "symbol", "ticker", "code", "requested_symbol",
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
                if not key:
                    continue
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
            if isinstance(d, dict) and d:
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
        page
        or sheet
        or sheet_name
        or _safe_str(merged_body.get("page"))
        or _safe_str(merged_body.get("sheet"))
        or _safe_str(merged_body.get("sheet_name"))
        or _safe_str(merged_body.get("page_name"))
        or _safe_str(merged_body.get("name"))
        or _safe_str(merged_body.get("tab"))
        or _safe_str(merged_body.get("worksheet"))
        or _safe_str(merged_body.get("sheetName"))
        or _safe_str(merged_body.get("pageName"))
        or _safe_str(merged_body.get("worksheet_name"))
        or "Market_Leaders"
    )

    effective_limit = _safe_int(
        merged_body.get("limit", limit),
        default=limit,
        lo=1,
        hi=5000,
    )
    if effective_limit <= 0:
        effective_limit = max(1, min(5000, int(limit or 2000)))

    effective_offset = _safe_int(merged_body.get("offset", offset), default=offset, lo=0)
    effective_mode = _safe_str(merged_body.get("mode") or mode)

    passthrough = {
        k: v for k, v in merged_body.items()
        if k not in {"request", "params", "query", "query_params", "payload", "data", "json", "body"}
    }
    return _canonicalize_sheet_name(target_raw) or "Market_Leaders", effective_limit, effective_offset, effective_mode, passthrough, request_parts


def _extract_top10_pages_from_body(body: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(body, dict):
        return []
    raw: List[str] = []
    for key in ("pages_selected", "pages", "source_pages"):
        val = body.get(key)
        if isinstance(val, (list, tuple, set)):
            raw.extend([_canonicalize_sheet_name(_safe_str(v)) for v in val if _safe_str(v)])
    criteria = body.get("criteria")
    if isinstance(criteria, dict):
        val = criteria.get("pages_selected") or criteria.get("pages")
        if isinstance(val, (list, tuple, set)):
            raw.extend([_canonicalize_sheet_name(_safe_str(v)) for v in val if _safe_str(v)])
    return [p for p in _dedupe_keep_order(raw) if p]


def _normalize_top10_body_for_engine(body: Optional[Dict[str, Any]], limit: int) -> Tuple[Dict[str, Any], List[str]]:
    out = dict(body or {})
    warnings: List[str] = []
    criteria = dict(out.get("criteria") or {}) if isinstance(out.get("criteria"), dict) else {}
    if not criteria:
        criteria = {}
    if not criteria.get("top_n"):
        criteria["top_n"] = max(1, min(limit, 50))
    out["criteria"] = criteria
    out.setdefault("top_n", criteria.get("top_n"))
    return out, warnings


def _is_schema_only_body(body: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(body, dict):
        return False
    return _safe_bool(body.get("schema_only"), False) or _safe_bool(body.get("headers_only"), False)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo("Asia/Riyadh")).isoformat()
    except Exception:
        pass
    return datetime.now(timezone.utc).isoformat()


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

        if not h and k:
            h = k.replace("_", " ").title()
        if h and not k:
            k = _norm_key(h)

        if h and k:
            hdrs.append(h)
            ks.append(k)

    return hdrs, ks


def _usable_contract(headers: Sequence[str], keys: Sequence[str], sheet_name: str = "") -> bool:
    if not headers or not keys:
        return False
    if len(headers) != len(keys) or len(headers) == 0:
        return False
    canon = _canonicalize_sheet_name(sheet_name)
    keyset = set(keys)
    if canon in INSTRUMENT_SHEETS - {"Top_10_Investments"}:
        if not ({"symbol", "ticker", "requested_symbol"} & keyset):
            return False
        if not ({"current_price", "price", "name"} & keyset):
            return False
    if canon == "Top_10_Investments":
        if not ({"symbol", "ticker", "requested_symbol"} & keyset):
            return False
        if not set(TOP10_REQUIRED_FIELDS).issubset(keyset):
            return False
    if canon == "Insights_Analysis":
        if not ({"section", "item", "metric", "value"} <= keyset):
            return False
    if canon == "Data_Dictionary":
        if not {"sheet", "header", "key"}.issubset(keyset):
            return False
    return True


def _ensure_top10_contract(headers: Sequence[str], keys: Sequence[str]) -> Tuple[List[str], List[str]]:
    hdrs = list(headers or [])
    ks = list(keys or [])
    for field in TOP10_REQUIRED_FIELDS:
        if field not in ks:
            ks.append(field)
            hdrs.append(TOP10_REQUIRED_HEADERS[field])
    return _complete_schema_contract(hdrs, ks)


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, Decimal):
        return _as_float(value)
    if isinstance(value, (datetime, date, dt_time)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
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
        if hasattr(obj, "__dict__"):
            d = getattr(obj, "__dict__", None)
            if isinstance(d, dict):
                return dict(d)
    except Exception:
        pass
    return {"result": obj}


def _looks_like_explicit_row_dict(d: Any) -> bool:
    if not isinstance(d, dict) or not d:
        return False
    keyset = {str(k) for k in d.keys()}
    if keyset & {"symbol", "ticker", "code", "requested_symbol"}:
        return True
    if {"sheet", "header", "key"}.issubset(keyset):
        return True
    if {"top10_rank", "selection_reason"}.issubset(keyset):
        return True
    if keyset & {"section", "item", "recommendation", "overall_score"}:
        return True
    return False


def _rows_from_matrix_payload(matrix: Any, cols: Sequence[Any]) -> List[Dict[str, Any]]:
    keys = [_safe_str(c) for c in cols if _safe_str(c)]
    out: List[Dict[str, Any]] = []
    for row in matrix or []:
        if not isinstance(row, (list, tuple)):
            continue
        d: Dict[str, Any] = {}
        for i, k in enumerate(keys):
            d[k] = row[i] if i < len(row) else None
        out.append(d)
    return out


def _coerce_rows_list(out: Any) -> List[Dict[str, Any]]:
    if out is None:
        return []

    if isinstance(out, list):
        if not out:
            return []
        if isinstance(out[0], dict):
            return [dict(r) for r in out if isinstance(r, dict)]
        if isinstance(out[0], (list, tuple)):
            return []
        return [_model_to_dict(r) for r in out if _model_to_dict(r)]

    if isinstance(out, dict):
        maybe_symbol_map = True
        rows_from_map: List[Dict[str, Any]] = []
        symbol_like_keys = 0
        if out:
            for k, v in out.items():
                if not isinstance(v, dict):
                    maybe_symbol_map = False
                    break
                if not _looks_like_symbol_token(k):
                    maybe_symbol_map = False
                    break
                symbol_like_keys += 1
                row = dict(v)
                if not row.get("symbol"):
                    row["symbol"] = _safe_str(k)
                rows_from_map.append(row)
        if maybe_symbol_map and symbol_like_keys > 0 and rows_from_map:
            return rows_from_map

        for key in ("row_objects", "records", "items", "data", "quotes", "rows"):
            val = out.get(key)
            if isinstance(val, list):
                if val and isinstance(val[0], dict):
                    return [dict(r) for r in val if isinstance(r, dict)]
                if val and isinstance(val[0], (list, tuple)):
                    cols = out.get("keys") or out.get("headers") or out.get("columns") or []
                    if isinstance(cols, list) and cols:
                        return _rows_from_matrix_payload(val, cols)
            if isinstance(val, dict):
                nested_rows = _coerce_rows_list(val)
                if nested_rows:
                    return nested_rows

        rows_matrix = out.get("rows_matrix") or out.get("matrix")
        if isinstance(rows_matrix, list):
            cols = out.get("keys") or out.get("headers") or out.get("columns") or []
            if isinstance(cols, list) and cols:
                return _rows_from_matrix_payload(rows_matrix, cols)

        if _looks_like_explicit_row_dict(out):
            return [dict(out)]

        for key in ("payload", "result", "response", "output"):
            nested = out.get(key)
            nested_rows = _coerce_rows_list(nested)
            if nested_rows:
                return nested_rows

        return []

    d = _model_to_dict(out)
    return [d] if _looks_like_explicit_row_dict(d) else []


def _extract_symbols_from_rows(rows: Sequence[Dict[str, Any]], limit: int = 5000) -> List[str]:
    raw: List[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for key in ("symbol", "ticker", "code", "requested_symbol", "Symbol", "Ticker", "Code"):
            v = row.get(key)
            if v:
                raw.append(str(v).strip())
                break
    return _normalize_symbol_list(raw, limit=limit)


_NULL_STRINGS: Set[str] = {"", "null", "none", "n/a", "na", "nan", "-", "--"}

_CANONICAL_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("symbol", "ticker", "code", "requested_symbol", "regularMarketSymbol"),
    "name": ("name", "shortName", "longName", "displayName", "companyName", "fundName", "description"),
    "asset_class": ("asset_class", "assetClass", "quoteType", "assetType", "instrumentType", "securityType", "type"),
    "exchange": ("exchange", "exchangeName", "fullExchangeName", "market", "marketName", "mic", "exchangeCode"),
    "currency": ("currency", "financialCurrency", "reportingCurrency", "quoteCurrency", "baseCurrency"),
    "country": ("country", "countryName", "country_code", "countryCode", "localeCountry"),
    "sector": ("sector", "sectorDisp", "gicsSector", "industryGroup", "sectorName", "gics_sector", "Sector", "General.Sector"),
    "industry": ("industry", "industryDisp", "gicsIndustry", "category", "industryName", "Industry", "General.Industry", "industry_group"),
    "current_price": ("current_price", "currentPrice", "price", "last", "lastPrice", "latestPrice", "regularMarketPrice", "nav", "close", "adjusted_close", "adjclose", "closePrice", "last_trade_price", "regular_market_price", "price_close"),
    "previous_close": ("previous_close", "previousClose", "regularMarketPreviousClose", "prevClose", "priorClose", "close_yesterday", "previous_close_price"),
    "open_price": ("open_price", "day_open", "dayOpen", "open", "openPrice", "regularMarketOpen", "open_price_day", "dailyOpen", "sessionOpen"),
    "day_high": ("day_high", "high", "dayHigh", "regularMarketDayHigh", "sessionHigh", "highPrice", "intradayHigh", "dailyHigh"),
    "day_low": ("day_low", "low", "dayLow", "regularMarketDayLow", "sessionLow", "lowPrice", "intradayLow", "dailyLow"),
    "week_52_high": ("week_52_high", "52WeekHigh", "fiftyTwoWeekHigh", "yearHigh", "week52High"),
    "week_52_low": ("week_52_low", "52WeekLow", "fiftyTwoWeekLow", "yearLow", "week52Low"),
    "price_change": ("price_change", "change", "priceChange", "regularMarketChange", "netChange"),
    "percent_change": ("percent_change", "changePercent", "percentChange", "regularMarketChangePercent", "pctChange", "change_pct"),
    "volume": ("volume", "regularMarketVolume", "sharesTraded", "tradeVolume", "Volume", "vol", "trade_count_volume"),
    "avg_volume_10d": ("avg_volume_10d", "avg_vol_10d", "averageVolume10days", "avgVolume10Day", "avgVol10d", "averageVolume10Day", "avg_volume_10_day"),
    "avg_volume_30d": ("avg_volume_30d", "avg_vol_30d", "averageVolume", "averageDailyVolume3Month", "avgVolume3Month", "avgVol30d", "averageVolume30Day", "avg_volume_30_day"),
    "market_cap": ("market_cap", "marketCap", "marketCapitalization", "MarketCapitalization", "capitalization", "Capitalization", "market_capitalization"),
    "float_shares": ("float_shares", "floatShares", "sharesFloat", "FloatShares", "SharesFloat", "sharesOutstanding", "SharesOutstanding"),
    "beta_5y": ("beta_5y", "beta", "beta5Y", "Beta", "beta5Year"),
    "pe_ttm": ("pe_ttm", "trailingPE", "peRatio", "priceEarningsTTM", "pe", "PERatio", "PriceEarningsTTM", "peTTM"),
    "pe_forward": ("pe_forward", "forward_pe", "forwardPE", "forwardPe", "ForwardPE", "ForwardPERatio", "forwardPERatio"),
    "eps_ttm": ("eps_ttm", "trailingEps", "eps", "earningsPerShare", "epsTTM", "EarningsShare", "epsTtm", "DilutedEPSTTM"),
    "dividend_yield": ("dividend_yield", "dividendYield", "trailingAnnualDividendYield", "distributionYield", "DividendYield", "forwardAnnualDividendYield", "Yield"),
    "payout_ratio": ("payout_ratio", "payoutRatio", "PayoutRatio", "payout", "PayoutRatioTTM"),
    "revenue_ttm": ("revenue_ttm", "totalRevenue", "revenueTTM", "revenue", "RevenueTTM", "TotalRevenueTTM", "Revenue", "SalesTTM"),
    "revenue_growth_yoy": ("revenue_growth_yoy", "revenueGrowth", "revenueGrowthYoY", "revenue_yoy_growth", "RevenueGrowthYOY", "QuarterlyRevenueGrowthYOY", "revenueGrowthYoy"),
    "gross_margin": ("gross_margin", "grossMargins", "grossMargin", "GrossMargin", "GrossProfitMargin", "grossMarginTTM"),
    "operating_margin": ("operating_margin", "operatingMargins", "operatingMargin", "OperatingMargin", "OperatingMarginTTM", "operatingMarginTTM"),
    "profit_margin": ("profit_margin", "profitMargins", "profitMargin", "netMargin", "ProfitMargin", "NetProfitMargin", "profitMarginTTM"),
    "debt_to_equity": ("debt_to_equity", "d_e_ratio", "debtToEquity", "deRatio", "DebtToEquity", "TotalDebtEquity"),
    "free_cash_flow_ttm": ("free_cash_flow_ttm", "fcf_ttm", "freeCashflow", "freeCashFlow", "fcf", "FreeCashFlow", "FreeCashFlowTTM"),
    "rsi_14": ("rsi_14", "rsi", "rsi14"),
    "volatility_30d": ("volatility_30d", "volatility30d", "vol30d"),
    "volatility_90d": ("volatility_90d", "volatility90d", "vol90d"),
    "max_drawdown_1y": ("max_drawdown_1y", "maxDrawdown1y", "drawdown1y"),
    "var_95_1d": ("var_95_1d", "var95_1d", "valueAtRisk95_1d"),
    "sharpe_1y": ("sharpe_1y", "sharpe1y", "sharpeRatio"),
    "risk_score": ("risk_score",),
    "risk_bucket": ("risk_bucket",),
    "pb_ratio": ("pb_ratio", "priceToBook", "pb"),
    "ps_ratio": ("ps_ratio", "priceToSalesTrailing12Months", "ps"),
    "ev_ebitda": ("ev_ebitda", "enterpriseToEbitda", "evToEbitda"),
    "peg_ratio": ("peg_ratio", "peg", "pegRatio"),
    "intrinsic_value": ("intrinsic_value", "fairValue", "dcf", "dcfValue", "intrinsicValue"),
    "upside_pct": ("upside_pct", "upsidePct", "upside_percent", "upsidePercent", "upside", "potentialUpside"),
    "valuation_score": ("valuation_score",),
    "forecast_price_1m": ("forecast_price_1m", "targetPrice1m", "priceTarget1m"),
    "forecast_price_3m": ("forecast_price_3m", "targetPrice3m", "priceTarget3m", "targetPrice", "targetMeanPrice"),
    "forecast_price_12m": ("forecast_price_12m", "targetPrice12m", "priceTarget12m", "targetMedianPrice", "targetHighPrice"),
    "expected_roi_1m": ("expected_roi_1m", "expectedReturn1m", "roi1m"),
    "expected_roi_3m": ("expected_roi_3m", "expectedReturn3m", "roi3m"),
    "expected_roi_12m": ("expected_roi_12m", "expectedReturn12m", "roi12m"),
    "forecast_confidence": ("forecast_confidence", "confidence", "confidencePct", "modelConfidence"),
    "confidence_score": ("confidence_score", "modelConfidenceScore"),
    "confidence_bucket": ("confidence_bucket",),
    "value_score": ("value_score",),
    "quality_score": ("quality_score",),
    "momentum_score": ("momentum_score",),
    "growth_score": ("growth_score",),
    "overall_score": ("overall_score", "score", "compositeScore"),
    "opportunity_score": ("opportunity_score",),
    "rank_overall": ("rank_overall", "rank", "overallRank"),
    "fundamental_view": ("fundamental_view", "fundamentalView", "fundamental_rating"),
    "technical_view": ("technical_view", "technicalView", "technical_rating"),
    "risk_view": ("risk_view", "riskView", "risk_rating", "risk_label"),
    "value_view": ("value_view", "valueView", "value_rating", "valuation_label"),
    "recommendation": ("recommendation", "rating", "action", "reco", "consensus"),
    "recommendation_reason": ("recommendation_reason", "reason", "summary", "thesis", "analysis"),
    "horizon_days": ("horizon_days", "horizon", "days"),
    "invest_period_label": ("invest_period_label", "periodLabel", "horizonLabel"),
    "position_qty": ("position_qty", "positionQty", "qty", "quantity", "shares", "holdingQty"),
    "avg_cost": ("avg_cost", "avgCost", "averageCost", "costBasisPerShare"),
    "position_cost": ("position_cost", "positionCost", "costBasis", "totalCost"),
    "position_value": ("position_value", "marketValue", "positionValue", "holdingValue"),
    "unrealized_pl": ("unrealized_pl", "unrealizedPnL", "unrealizedPL", "profitLoss"),
    "unrealized_pl_pct": ("unrealized_pl_pct", "unrealizedPnLPct", "unrealizedPLPct"),
    "data_provider": ("data_provider", "provider", "source", "dataProvider"),
    "last_updated_utc": ("last_updated_utc", "lastUpdated", "updatedAt", "timestamp", "asOf"),
    "last_updated_riyadh": ("last_updated_riyadh",),
    "warnings": ("warnings", "warning", "messages", "errors"),
    "sector_relative_score": ("sector_relative_score", "sectorAdjustedScore", "sectorAdjScore", "sector_adj_score", "sectorRelativeScore"),
    "conviction_score": ("conviction_score", "convictionScore", "conviction"),
    "top_factors": ("top_factors", "topFactors", "positives", "factors"),
    "top_risks": ("top_risks", "topRisks", "negatives", "risks"),
    "position_size_hint": ("position_size_hint", "positionSizeHint", "sizingHint", "sizing"),
    "recommendation_detailed": ("recommendation_detailed", "recommendationDetailed", "recommendationDetail", "reco_detail", "detailed_recommendation"),
    "recommendation_priority": ("recommendation_priority", "recoPriority", "priority", "reco_priority"),
    "candlestick_pattern": ("candlestick_pattern", "candlePattern", "candlestickPattern", "pattern"),
    "candlestick_signal": ("candlestick_signal", "candleSignal", "candlestickSignal", "patternSignal"),
    "candlestick_strength": ("candlestick_strength", "candleStrength", "candlestickStrength", "patternStrength"),
    "candlestick_confidence": ("candlestick_confidence", "candleConfidence", "candlestickConfidence", "patternConfidence"),
    "candlestick_patterns_recent": ("candlestick_patterns_recent", "recentPatterns", "candlestickPatternsRecent", "patterns5d", "patterns_recent"),
}

_COMMODITY_SYMBOL_HINTS: Tuple[str, ...] = ("GC=F", "SI=F", "BZ=F", "CL=F", "NG=F", "HG=F")
_ETF_SYMBOL_HINTS: Tuple[str, ...] = ("SPY", "QQQ", "VTI", "VOO", "IWM", "DIA", "IVV", "EFA", "EEM", "ARKK")
_ETF_DISPLAY_NAMES: Dict[str, str] = {
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
_COMMODITY_DISPLAY_NAMES: Dict[str, str] = {
    "GC=F": "Gold Futures",
    "SI=F": "Silver Futures",
    "BZ=F": "Brent Crude Futures",
    "CL=F": "WTI Crude Futures",
    "NG=F": "Natural Gas Futures",
    "HG=F": "Copper Futures",
}
_COMMODITY_INDUSTRY_HINTS: Dict[str, str] = {
    "GC=F": "Precious Metals",
    "SI=F": "Precious Metals",
    "HG=F": "Industrial Metals",
    "BZ=F": "Energy",
    "CL=F": "Energy",
    "NG=F": "Energy",
}


# =============================================================================
# v5.67.0 — Suffix → locale map (international exchange/currency/country)
# =============================================================================
# Pre-v5.67.0, the three `_infer_*_from_symbol` helpers defaulted every
# non-Saudi/non-FX/non-Futures ticker to NASDAQ/USD/USA. The May 2026 audit
# showed 51/57 rows with .HK / .L / .NS / .SA / .TO / .XETRA / .DE tickers
# all stamped as US listings. This map fixes that. Format:
# {suffix: (exchange_display, currency_code, country)}
#
# Note: .SA is Brazil (Yahoo convention); Saudi Arabia uses .SR. GBp (pence)
# is intentionally lowercase 'p' — LSE quotes most equities in pence, not pounds.
_SUFFIX_TO_LOCALE: Dict[str, Tuple[str, str, str]] = {
    ".HK":    ("HKEX", "HKD", "Hong Kong"),
    ".L":     ("LSE", "GBp", "United Kingdom"),
    ".LON":   ("LSE", "GBp", "United Kingdom"),
    ".CO":    ("Copenhagen", "DKK", "Denmark"),
    ".NS":    ("NSE", "INR", "India"),
    ".BO":    ("BSE", "INR", "India"),
    ".SA":    ("B3", "BRL", "Brazil"),
    ".SR":    ("Tadawul", "SAR", "Saudi Arabia"),
    ".SAU":   ("Tadawul", "SAR", "Saudi Arabia"),
    ".TADAWUL": ("Tadawul", "SAR", "Saudi Arabia"),
    ".KSE":   ("Tadawul", "SAR", "Saudi Arabia"),
    ".TO":    ("TSX", "CAD", "Canada"),
    ".V":     ("TSX Venture", "CAD", "Canada"),
    ".CN":    ("CSE", "CAD", "Canada"),
    ".NE":    ("NEO Exchange", "CAD", "Canada"),
    ".XETRA": ("XETRA", "EUR", "Germany"),
    ".XETR":  ("XETRA", "EUR", "Germany"),
    ".DE":    ("XETRA", "EUR", "Germany"),
    ".F":     ("Frankfurt", "EUR", "Germany"),
    ".HM":    ("Hamburg", "EUR", "Germany"),
    ".MU":    ("Munich", "EUR", "Germany"),
    ".PA":    ("Euronext Paris", "EUR", "France"),
    ".PAR":   ("Euronext Paris", "EUR", "France"),
    ".AS":    ("Euronext Amsterdam", "EUR", "Netherlands"),
    ".AMS":   ("Euronext Amsterdam", "EUR", "Netherlands"),
    ".MI":    ("Borsa Italiana", "EUR", "Italy"),
    ".MIL":   ("Borsa Italiana", "EUR", "Italy"),
    ".MC":    ("BME", "EUR", "Spain"),
    ".MAD":   ("BME", "EUR", "Spain"),
    ".BR":    ("Euronext Brussels", "EUR", "Belgium"),
    ".BRU":   ("Euronext Brussels", "EUR", "Belgium"),
    ".LS":    ("Euronext Lisbon", "EUR", "Portugal"),
    ".HE":    ("Helsinki", "EUR", "Finland"),
    ".HEL":   ("Helsinki", "EUR", "Finland"),
    ".IR":    ("Euronext Dublin", "EUR", "Ireland"),
    ".ST":    ("Stockholm", "SEK", "Sweden"),
    ".STO":   ("Stockholm", "SEK", "Sweden"),
    ".OL":    ("Oslo", "NOK", "Norway"),
    ".OSL":   ("Oslo", "NOK", "Norway"),
    ".SW":    ("SIX", "CHF", "Switzerland"),
    ".SWX":   ("SIX", "CHF", "Switzerland"),
    ".VI":    ("Vienna", "EUR", "Austria"),
    ".VIE":   ("Vienna", "EUR", "Austria"),
    ".WA":    ("Warsaw", "PLN", "Poland"),
    ".WAR":   ("Warsaw", "PLN", "Poland"),
    ".AX":    ("ASX", "AUD", "Australia"),
    ".NZ":    ("NZX", "NZD", "New Zealand"),
    ".T":     ("TSE", "JPY", "Japan"),
    ".TYO":   ("TSE", "JPY", "Japan"),
    ".KS":    ("KRX", "KRW", "South Korea"),
    ".KQ":    ("KOSDAQ", "KRW", "South Korea"),
    ".SI":    ("SGX", "SGD", "Singapore"),
    ".KL":    ("Bursa Malaysia", "MYR", "Malaysia"),
    ".BK":    ("SET", "THB", "Thailand"),
    ".JK":    ("IDX", "IDR", "Indonesia"),
    ".SS":    ("Shanghai", "CNY", "China"),
    ".SZ":    ("Shenzhen", "CNY", "China"),
    ".TW":    ("TWSE", "TWD", "Taiwan"),
    ".TWO":   ("TPEx", "TWD", "Taiwan"),
    ".MX":    ("BMV", "MXN", "Mexico"),
    ".BA":    ("BCBA", "ARS", "Argentina"),
    ".JO":    ("JSE", "ZAR", "South Africa"),
    ".US":    ("NASDAQ/NYSE", "USD", "USA"),
}

# v5.67.0: tokens that indicate the displayed `country` field is a stale
# US default and is eligible to be overwritten by suffix derivation.
_US_COUNTRY_TOKENS: Set[str] = {
    "", "USA", "US", "U.S.", "U.S.A.", "UNITED STATES", "UNITED STATES OF AMERICA",
}

# v5.67.0: percent_change is now stored as a FRACTION. Daily moves above
# 0.50 (50%) are extremely rare and almost always indicate a unit error.
_PERCENT_CHANGE_DAILY_MAX_ABS_FRACTION: float = 0.50


def _suffix_locale_for(symbol: str) -> Optional[Tuple[str, str, str]]:
    """
    v5.67.0: Look up (exchange, currency, country) by symbol suffix.
    Returns None if no recognized suffix. Longest-match wins (e.g. .TADAWUL
    beats .T) so multi-character suffixes are not shadowed by single-character
    prefixes.
    """
    s = normalize_symbol(symbol)
    if not s or "." not in s:
        return None
    best_suffix: Optional[str] = None
    s_upper = s.upper()
    for suffix in _SUFFIX_TO_LOCALE:
        if s_upper.endswith(suffix):
            if best_suffix is None or len(suffix) > len(best_suffix):
                best_suffix = suffix
    if best_suffix is None:
        return None
    return _SUFFIX_TO_LOCALE[best_suffix]


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
            if len(seq) == 1:
                return seq[0]
            return "; ".join(_safe_str(v) for v in seq if _safe_str(v))
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
        lower = cand.lower()
        if lower in src_ci and not _is_blank_value(src_ci.get(lower)):
            return src_ci.get(lower)
        if lower in flat_ci and not _is_blank_value(flat_ci.get(lower)):
            return flat_ci.get(lower)
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
    if s in {"SPY", "QQQ", "VTI", "VOO", "IWM", "DIA"}:
        return "ETF"
    return "Equity"


def _infer_exchange_from_symbol(symbol: str) -> str:
    """v5.67.0: consult _SUFFIX_TO_LOCALE first to avoid mislabeling foreign tickers."""
    s = normalize_symbol(symbol)
    if not s:
        return ""
    locale = _suffix_locale_for(s)
    if locale is not None:
        return locale[0]
    if re.match(r"^[0-9]{4}$", s):
        return "Tadawul"
    if s.endswith("=X"):
        return "FX"
    if s.endswith("=F"):
        return "Futures"
    return "NASDAQ/NYSE"


def _infer_currency_from_symbol(symbol: str) -> str:
    """v5.67.0: consult _SUFFIX_TO_LOCALE first."""
    s = normalize_symbol(symbol)
    if not s:
        return ""
    locale = _suffix_locale_for(s)
    if locale is not None:
        return locale[1]
    if re.match(r"^[0-9]{4}$", s):
        return "SAR"
    if s.endswith("=X"):
        pair = s[:-2]
        if len(pair) >= 6:
            return pair[-3:]
        if pair:
            return pair
        return "FX"
    if s.endswith("=F"):
        return "USD"
    return "USD"


def _infer_country_from_symbol(symbol: str) -> str:
    """v5.67.0: consult _SUFFIX_TO_LOCALE first."""
    s = normalize_symbol(symbol)
    if not s:
        return ""
    locale = _suffix_locale_for(s)
    if locale is not None:
        return locale[2]
    if re.match(r"^[0-9]{4}$", s):
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
        if len(pair) >= 6:
            return f"{pair[:3]}/{pair[3:6]}"
        return f"{pair} FX" if pair else s
    if s.endswith("=F"):
        return _safe_str(s.replace("=F", "")).strip() or s
    return s


def _apply_symbol_context_defaults(row: Dict[str, Any], symbol: str = "", page: str = "") -> Dict[str, Any]:
    out = dict(row or {})
    sym = normalize_symbol(symbol or _safe_str(out.get("symbol") or out.get("ticker") or out.get("requested_symbol")))
    if not sym:
        return out

    page = _canonicalize_sheet_name(page) if page else ""

    if not out.get("symbol"):
        out["symbol"] = sym
    if not out.get("requested_symbol"):
        out["requested_symbol"] = sym
    if not out.get("symbol_normalized"):
        out["symbol_normalized"] = sym

    if page == "Commodities_FX" or sym.endswith("=F") or sym.endswith("=X"):
        out.setdefault("asset_class", _infer_asset_class_from_symbol(sym))
        out.setdefault("exchange", _infer_exchange_from_symbol(sym))
        out.setdefault("currency", _infer_currency_from_symbol(sym))
        out.setdefault("country", _infer_country_from_symbol(sym))
        out.setdefault("sector", _infer_sector_from_symbol(sym))
        out.setdefault("industry", _infer_industry_from_symbol(sym))

        current_name = _safe_str(out.get("name"))
        inferred_name = _infer_display_name_from_symbol(sym)
        if not current_name or current_name == sym:
            out["name"] = inferred_name or sym

        if sym.endswith("=X"):
            out.setdefault("market_cap", None)
            out.setdefault("float_shares", None)
            out.setdefault("beta_5y", None)
        if sym.endswith("=F"):
            out.setdefault("market_cap", None)
            out.setdefault("float_shares", None)

        if out.get("invest_period_label") in (None, ""):
            out["invest_period_label"] = "1Y"
        if out.get("horizon_days") in (None, ""):
            out["horizon_days"] = 365

    return out


def _coerce_datetime_like(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        try:
            return value.isoformat()
        except Exception:
            return _safe_str(value)
    return _safe_str(value) or None


def _canonicalize_provider_row(row: Dict[str, Any], requested_symbol: str = "", normalized_symbol: str = "", provider: str = "") -> Dict[str, Any]:
    src = dict(row or {})
    flat = _flatten_scalar_fields(src)
    symbol = normalized_symbol or normalize_symbol(_safe_str(_lookup_alias_value(src, flat, "symbol") or requested_symbol))
    out: Dict[str, Any] = {
        "symbol": symbol or requested_symbol,
        "symbol_normalized": symbol or requested_symbol,
        "requested_symbol": requested_symbol or symbol,
    }
    for field, aliases in _CANONICAL_FIELD_ALIASES.items():
        for alias in (field,) + tuple(aliases):
            val = _lookup_alias_value(src, flat, alias)
            if not _is_blank_value(val):
                out[field] = _json_safe(_to_scalar(val))
                break

    inferred_symbol = out.get("symbol") or normalized_symbol or requested_symbol
    inferred_name = _infer_display_name_from_symbol(inferred_symbol)
    if not out.get("name") or _safe_str(out.get("name")) == _safe_str(inferred_symbol):
        out["name"] = inferred_name or out.get("symbol") or normalized_symbol or requested_symbol
    if not out.get("asset_class"):
        out["asset_class"] = _infer_asset_class_from_symbol(inferred_symbol)
    if not out.get("exchange"):
        out["exchange"] = _infer_exchange_from_symbol(inferred_symbol)
    if not out.get("currency"):
        out["currency"] = _infer_currency_from_symbol(inferred_symbol)
    if not out.get("country"):
        out["country"] = _infer_country_from_symbol(inferred_symbol)
    if not out.get("sector"):
        out["sector"] = _infer_sector_from_symbol(inferred_symbol)
    if not out.get("industry"):
        out["industry"] = _infer_industry_from_symbol(inferred_symbol)

    # v5.67.0: Repair stale US defaults on foreign-suffix tickers. Pre-v5.67.0,
    # when EODHD/Finnhub returned a partial row with Country=USA / Exchange=NASDAQ
    # for a .HK / .L / .NS ticker, the `if not out.get(...)` checks above
    # respected the bad value because the field WAS populated (just with the
    # wrong content). This block detects that case and overrides.
    locale = _suffix_locale_for(inferred_symbol)
    if locale is not None and locale[2] != "USA":
        derived_exch, derived_curr, derived_country = locale
        if _safe_str(out.get("country")).upper() in _US_COUNTRY_TOKENS:
            out["country"] = derived_country
        current_exch_upper = _safe_str(out.get("exchange")).upper()
        if (not current_exch_upper) or "NASDAQ" in current_exch_upper or "NYSE" in current_exch_upper:
            out["exchange"] = derived_exch
        current_curr_upper = _safe_str(out.get("currency")).upper()
        if (not current_curr_upper) or current_curr_upper == "USD":
            out["currency"] = derived_curr

    if provider and not out.get("data_provider"):
        out["data_provider"] = provider

    if not out.get("last_updated_utc"):
        out["last_updated_utc"] = _coerce_datetime_like(_lookup_alias_value(src, flat, "last_updated_utc")) or _now_utc_iso()
    if not out.get("last_updated_riyadh"):
        out["last_updated_riyadh"] = _now_riyadh_iso()

    warnings = out.get("warnings")
    if isinstance(warnings, (list, tuple, set)):
        out["warnings"] = "; ".join(_safe_str(v) for v in warnings if _safe_str(v))

    price = _as_float(out.get("current_price")) or _as_float(out.get("price"))
    prev = _as_float(out.get("previous_close"))
    change = _as_float(out.get("price_change"))
    pct = _as_float(out.get("percent_change"))
    if price is None:
        price = _as_float(out.get("close"))
        if price is not None:
            out["current_price"] = price
    if prev is None and price is not None and change is not None:
        prev = price - change
        out["previous_close"] = prev
    if change is None and price is not None and prev is not None:
        change = price - prev
        out["price_change"] = round(change, 6)

    # v5.67.0: emit percent_change as FRACTION (was POINTS pre-v5.67.0).
    # 04_Format.gs v2.7.0 expects FRACTION via `_KNOWN_FRACTION_PERCENT_COLUMNS_`
    # and applies "0.00%" format which multiplies by 100 for display. Pre-v5.67.0
    # the engine stored POINTS, causing the formatter to display 100× too large.
    if pct is None and price is not None and prev not in (None, 0):
        # No incoming value — compute as fraction directly
        pct = (price - prev) / prev
        out["percent_change"] = round(pct, 8)
    elif pct is not None:
        # Provider sent a value. Determine whether it's already a fraction or
        # in points form, using a ground-truth comparison when prices are available.
        if price is not None and prev not in (None, 0):
            true_fraction = (price - prev) / prev
            err_as_fraction = abs(pct - true_fraction)
            err_as_points = abs(pct - true_fraction * 100.0)
            if err_as_points < err_as_fraction:
                # stored value is in points form; convert to fraction
                out["percent_change"] = round(pct / 100.0, 8)
            else:
                # stored value is already in fraction form
                out["percent_change"] = round(pct, 8)
        else:
            # No price/prev for ground truth — use magnitude heuristic
            if abs(pct) > 1.5:
                out["percent_change"] = round(pct / 100.0, 8)
            else:
                out["percent_change"] = round(pct, 8)

    high52 = _as_float(out.get("week_52_high"))
    low52 = _as_float(out.get("week_52_low"))
    if price is not None and high52 is not None and low52 is not None and high52 > low52 and out.get("week_52_position_pct") is None:
        out["week_52_position_pct"] = round(((price - low52) / (high52 - low52)) * 100.0, 6)

    qty = _as_float(out.get("position_qty"))
    avg_cost = _as_float(out.get("avg_cost"))
    if qty is not None and price is not None and out.get("position_value") is None:
        out["position_value"] = round(qty * price, 6)
    if qty is not None and avg_cost is not None and out.get("position_cost") is None:
        out["position_cost"] = round(qty * avg_cost, 6)
    pos_val = _as_float(out.get("position_value"))
    pos_cost = _as_float(out.get("position_cost"))
    if pos_val is not None and pos_cost is not None and out.get("unrealized_pl") is None:
        out["unrealized_pl"] = round(pos_val - pos_cost, 6)
    upl = _as_float(out.get("unrealized_pl"))
    if upl is not None and pos_cost not in (None, 0) and out.get("unrealized_pl_pct") is None:
        out["unrealized_pl_pct"] = round((upl / pos_cost) * 100.0, 6)

    out = _apply_symbol_context_defaults(out, symbol=inferred_symbol)
    if _as_float(out.get("current_price")) is not None and _safe_str(out.get("warnings")).lower() == "no live provider data available":
        out["warnings"] = "Recovered from history/chart fallback"

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


def _apply_page_row_backfill(sheet: str, row: Dict[str, Any]) -> Dict[str, Any]:
    target = _canonicalize_sheet_name(sheet)
    out = _apply_symbol_context_defaults(dict(row or {}), page=target)
    sym = normalize_symbol(_safe_str(out.get("symbol") or out.get("requested_symbol")))

    if out.get("invest_period_label") in (None, ""):
        out["invest_period_label"] = "1Y"
    if out.get("horizon_days") in (None, ""):
        out["horizon_days"] = 365

    if out.get("data_provider") in (None, ""):
        sources = out.get("data_sources")
        if isinstance(sources, list) and sources:
            out["data_provider"] = _safe_str(sources[0])

    conf = _as_float(out.get("confidence_score"))
    if conf is None:
        conf_fraction = _as_float(out.get("forecast_confidence"))
        if conf_fraction is not None:
            conf = conf_fraction * 100.0 if conf_fraction <= 1.5 else conf_fraction
            out.setdefault("confidence_score", round(_clamp(conf, 0.0, 100.0), 2))
    if conf is not None and out.get("confidence_bucket") in (None, ""):
        out["confidence_bucket"] = "HIGH" if conf >= 75 else "MODERATE" if conf >= 55 else "LOW"

    if target == "Commodities_FX" or sym.endswith("=F") or sym.endswith("=X"):
        out.setdefault("data_provider", _safe_str(out.get("data_provider"), "history_or_fallback"))
        if out.get("forecast_confidence") in (None, ""):
            out["forecast_confidence"] = 0.55
        if out.get("confidence_score") in (None, ""):
            out["confidence_score"] = 55.0
        if out.get("forecast_confidence") not in (None, "") and out.get("confidence_bucket") in (None, ""):
            conf = _as_float(out.get("confidence_score")) or ((_as_float(out.get("forecast_confidence")) or 0.55) * 100.0)
            out["confidence_bucket"] = "HIGH" if conf >= 75 else "MODERATE" if conf >= 55 else "LOW"
        if out.get("warnings") in (None, "") and _as_float(out.get("current_price")) is None:
            out["warnings"] = "Live quote sparse; chart/history fallback unavailable"

    if target == "Mutual_Funds":
        if out.get("asset_class") in (None, ""):
            out["asset_class"] = "Fund"
        if out.get("sector") in (None, ""):
            out["sector"] = "Diversified"
        if out.get("industry") in (None, ""):
            out["industry"] = "Mutual Funds"
        if out.get("country") in (None, ""):
            out["country"] = _infer_country_from_symbol(sym)
        if out.get("exchange") in (None, ""):
            out["exchange"] = _infer_exchange_from_symbol(sym)
        if out.get("currency") in (None, ""):
            out["currency"] = _infer_currency_from_symbol(sym)
        if out.get("invest_period_label") in (None, ""):
            out["invest_period_label"] = "1Y"
        if out.get("horizon_days") in (None, ""):
            out["horizon_days"] = 365

    if target in {"Global_Markets", "Market_Leaders", "My_Portfolio", "Top_10_Investments"}:
        asset_class = _safe_str(out.get("asset_class"))
        if sym in _ETF_SYMBOL_HINTS or asset_class.upper() == "ETF":
            out.setdefault("asset_class", "ETF")
            out.setdefault("sector", "Broad Market")
            out.setdefault("industry", "ETF")
            inferred_name = _infer_display_name_from_symbol(sym)
            if inferred_name and (_safe_str(out.get("name")) in ("", sym)):
                out["name"] = inferred_name

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
    return [_strict_project_row_display(headers, keys, row) for row in (rows or [])]


def _merge_missing_fields(base_row: Dict[str, Any], template_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(base_row or {})
    if not isinstance(template_row, dict):
        return out
    for k, v in template_row.items():
        if out.get(k) in (None, "", [], {}) and v not in (None, "", [], {}):
            out[k] = _json_safe(v)
    return out


def _rows_matrix_from_rows(rows: List[Dict[str, Any]], keys: List[str]) -> List[List[Any]]:
    return [[_json_safe(row.get(k)) for k in keys] for row in rows or []]


def _compute_scores_fallback(row: Dict[str, Any]) -> None:
    price = _as_float(row.get("current_price")) or _as_float(row.get("price"))
    pe = _as_float(row.get("pe_ttm"))
    pb = _as_float(row.get("pb_ratio"))
    ps = _as_float(row.get("ps_ratio"))
    ev_ebitda = _as_float(row.get("ev_ebitda"))
    intrinsic = _as_float(row.get("intrinsic_value"))
    beta = _as_float(row.get("beta_5y"))
    debt_to_equity = _as_float(row.get("debt_to_equity"))

    div_yield_pct = _as_pct_points(row.get("dividend_yield")) or 0.0
    gross_margin_pct = _as_pct_points(row.get("gross_margin")) or 0.0
    operating_margin_pct = _as_pct_points(row.get("operating_margin")) or 0.0
    profit_margin_pct = _as_pct_points(row.get("profit_margin")) or 0.0
    revenue_growth_pct = _as_pct_points(row.get("revenue_growth_yoy")) or 0.0

    seed_roi_1m = _as_pct_points(row.get("expected_roi_1m"))
    seed_roi_3m = _as_pct_points(row.get("expected_roi_3m"))
    seed_roi_12m = _as_pct_points(row.get("expected_roi_12m"))
    seed_best_roi = next((v for v in (seed_roi_3m, seed_roi_12m, seed_roi_1m) if v is not None), 0.0)

    if row.get("value_score") is None:
        value_score = 55.0
        if pe is not None and pe > 0:
            value_score += max(0.0, 22.0 - min(pe, 22.0))
        if pb is not None and pb > 0:
            value_score += max(0.0, 12.0 - min(pb * 3.0, 12.0))
        if ps is not None and ps > 0:
            value_score += max(0.0, 10.0 - min(ps * 2.0, 10.0))
        value_score += min(max(div_yield_pct, 0.0), 12.0)
        row["value_score"] = round(_clamp(float(value_score), 0.0, 100.0), 2)

    if row.get("valuation_score") is None:
        valuation_score = 50.0
        if intrinsic is not None and price not in (None, 0):
            upside_pct = ((intrinsic - price) / price) * 100.0
            valuation_score += _clamp(upside_pct, -20.0, 25.0)
        if ev_ebitda is not None and ev_ebitda > 0:
            valuation_score += max(0.0, 12.0 - min(ev_ebitda, 12.0))
        if pe is not None and pe > 0:
            valuation_score += max(0.0, 15.0 - min(pe, 15.0))
        row["valuation_score"] = round(_clamp(float(valuation_score), 0.0, 100.0), 2)

    if row.get("quality_score") is None:
        quality_score = 45.0
        quality_score += min(max(gross_margin_pct, 0.0), 20.0) * 0.6
        quality_score += min(max(operating_margin_pct, 0.0), 18.0) * 0.7
        quality_score += min(max(profit_margin_pct, 0.0), 15.0) * 0.7
        if debt_to_equity is not None:
            quality_score += max(0.0, 15.0 - min(max(debt_to_equity, 0.0), 15.0))
        row["quality_score"] = round(_clamp(float(quality_score), 0.0, 100.0), 2)

    if row.get("momentum_score") is None:
        pct = _as_pct_points(row.get("percent_change"))
        if pct is None:
            pct = _as_pct_points(row.get("change_pct"))
        if pct is None:
            pct = 0.0
        row["momentum_score"] = round(_clamp(50.0 + pct, 0.0, 100.0), 2)

    if row.get("growth_score") is None:
        growth_score = 50.0 + _clamp(revenue_growth_pct, -25.0, 35.0)
        eps = _as_float(row.get("eps_ttm"))
        if eps is not None and eps > 0:
            growth_score += 3.0
        row["growth_score"] = round(_clamp(float(growth_score), 0.0, 100.0), 2)

    conf = _as_float(row.get("forecast_confidence"))
    if conf is None:
        conf = _as_float(row.get("confidence_score"))
    if conf is None:
        conf = 0.55
    if conf > 1.5:
        conf = conf / 100.0
    row.setdefault("forecast_confidence", round(_clamp(conf, 0.0, 1.0), 4))
    row.setdefault("confidence_score", round(_clamp(conf * 100.0, 0.0, 100.0), 2))

    if row.get("risk_score") is None:
        vol = _as_pct_points(row.get("volatility_90d"))
        # v5.67.0: max_drawdown_1y now stored as SIGNED FRACTION (e.g. -0.338).
        # _as_pct_points converts to signed points (-33.8). Use abs() so risk
        # contribution doesn't zero out via max(x, 0) on negative values.
        drawdown = _as_pct_points(row.get("max_drawdown_1y"))
        var95 = _as_pct_points(row.get("var_95_1d"))
        risk_score = 30.0
        if vol is not None:
            risk_score += min(max(vol, 0.0), 35.0)
        if drawdown is not None:
            risk_score += min(abs(drawdown), 20.0) * 0.6
        if var95 is not None:
            risk_score += min(abs(var95), 12.0)
        if beta is not None:
            risk_score += min(max(beta * 8.0, 0.0), 15.0)
        row["risk_score"] = round(_clamp(float(risk_score), 0.0, 100.0), 2)

    if row.get("overall_score") is None:
        vals = [
            _as_float(row.get("value_score")),
            _as_float(row.get("valuation_score")),
            _as_float(row.get("quality_score")),
            _as_float(row.get("momentum_score")),
            _as_float(row.get("growth_score")),
        ]
        vals2 = [v for v in vals if v is not None]
        overall = sum(vals2) / len(vals2) if vals2 else 50.0
        row["overall_score"] = round(_clamp(float(overall), 0.0, 100.0), 2)

    if price is not None and row.get("forecast_price_1m") is None:
        drift = max(0.5, min(4.0, seed_best_roi if seed_best_roi else 1.0))
        row["forecast_price_1m"] = round(price * (1.0 + drift / 300.0), 4)
    if price is not None and row.get("forecast_price_3m") is None:
        drift = max(1.0, min(8.0, seed_best_roi if seed_best_roi else 3.0))
        row["forecast_price_3m"] = round(price * (1.0 + drift / 100.0), 4)
    if price is not None and row.get("forecast_price_12m") is None:
        drift = max(3.0, min(18.0, (seed_roi_12m if seed_roi_12m is not None else seed_best_roi) or 8.0))
        row["forecast_price_12m"] = round(price * (1.0 + drift / 100.0), 4)

    if price is not None and row.get("expected_roi_1m") is None:
        fp1 = _as_float(row.get("forecast_price_1m"))
        if fp1 is not None and price:
            row["expected_roi_1m"] = round((fp1 - price) / price, 6)
    if price is not None and row.get("expected_roi_3m") is None:
        fp3 = _as_float(row.get("forecast_price_3m"))
        if fp3 is not None and price:
            row["expected_roi_3m"] = round((fp3 - price) / price, 6)
    if price is not None and row.get("expected_roi_12m") is None:
        fp12 = _as_float(row.get("forecast_price_12m"))
        if fp12 is not None and price:
            row["expected_roi_12m"] = round((fp12 - price) / price, 6)

    final_roi_1m = _as_pct_points(row.get("expected_roi_1m"))
    final_roi_3m = _as_pct_points(row.get("expected_roi_3m"))
    final_roi_12m = _as_pct_points(row.get("expected_roi_12m"))
    final_best_roi = next((v for v in (final_roi_3m, final_roi_12m, final_roi_1m) if v is not None), 0.0)

    if row.get("opportunity_score") is None:
        base = _as_float(row.get("overall_score")) or 50.0
        confidence_boost = ((_as_float(row.get("confidence_score")) or 50.0) - 50.0) * 0.20
        risk_penalty = ((_as_float(row.get("risk_score")) or 50.0) - 50.0) * 0.25
        roi_boost = _clamp(final_best_roi, -25.0, 35.0) * 0.35
        row["opportunity_score"] = round(_clamp(base + confidence_boost + roi_boost - risk_penalty, 0.0, 100.0), 2)

    if not row.get("risk_bucket"):
        rs = _as_float(row.get("risk_score")) or 50.0
        row["risk_bucket"] = "LOW" if rs < 40 else "MODERATE" if rs < 70 else "HIGH"

    if not row.get("confidence_bucket"):
        cs = _as_float(row.get("confidence_score")) or 55.0
        row["confidence_bucket"] = "HIGH" if cs >= 75 else "MODERATE" if cs >= 55 else "LOW"


def _compute_recommendation(row: Dict[str, Any]) -> None:
    if row.get("recommendation"):
        return
    overall = _as_float(row.get("overall_score")) or 50.0
    conf = _as_float(row.get("confidence_score")) or 55.0
    risk = _as_float(row.get("risk_score")) or 50.0
    if overall >= 75 and conf >= 65 and risk <= 60:
        rec = "BUY"
    elif overall >= 60 and conf >= 55:
        rec = "ACCUMULATE"
    elif overall <= 35 or risk >= 85:
        rec = "REDUCE"
    else:
        rec = "HOLD"
    row["recommendation"] = rec
    row.setdefault(
        "recommendation_reason",
        f"overall={round(overall,1)} confidence={round(conf,1)} risk={round(risk,1)}",
    )


def _apply_rank_overall(rows: List[Dict[str, Any]]) -> None:
    scored: List[Tuple[int, float]] = []
    for i, row in enumerate(rows):
        score = _as_float(row.get("overall_score"))
        if score is None:
            score = _as_float(row.get("opportunity_score"))
        if score is None:
            continue
        scored.append((i, score))
    scored.sort(key=lambda t: t[1], reverse=True)
    for rank, (idx, _) in enumerate(scored, start=1):
        rows[idx]["rank_overall"] = rank


def _top10_selection_reason(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key, label in (
        ("overall_score", "overall"),
        ("opportunity_score", "opportunity"),
        ("confidence_score", "confidence"),
        ("risk_score", "risk"),
    ):
        val = _as_float(row.get(key))
        if val is None:
            continue
        suffix = "%" if key == "confidence_score" else ""
        parts.append(f"{label}={round(val, 1)}{suffix}")
    return "Selected by fallback ranking" if not parts else ("Selected by fallback ranking: " + ", ".join(parts))


def _top10_criteria_snapshot(criteria: Dict[str, Any]) -> str:
    keep = {
        "top_n": criteria.get("top_n"),
        "pages_selected": criteria.get("pages_selected"),
        "horizon_days": criteria.get("horizon_days") or criteria.get("invest_period_days"),
        "risk_level": criteria.get("risk_level"),
        "min_expected_roi": criteria.get("min_expected_roi"),
        "confidence_level": criteria.get("confidence_level"),
        "direct_symbols": criteria.get("direct_symbols") or criteria.get("symbols"),
    }
    keep = {k: v for k, v in keep.items() if v not in (None, "", [], {})}
    try:
        import json
        return json.dumps(_json_safe(keep), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


def _feature_flags(settings: Any) -> Dict[str, bool]:
    return {
        "computations_enabled": _safe_bool(getattr(settings, "computations_enabled", True), True),
        "forecasting_enabled": _safe_bool(getattr(settings, "forecasting_enabled", True), True),
        "scoring_enabled": _safe_bool(getattr(settings, "scoring_enabled", True), True),
    }


def _try_get_settings() -> Any:
    for mod_path in ("config", "core.config", "env"):
        try:
            mod = import_module(mod_path)
        except Exception:
            continue
        for fn_name in ("get_settings_cached", "get_settings"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    return fn()
                except Exception:
                    continue
    return None


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)

    result = await asyncio.to_thread(fn, *args, **kwargs)
    return await result if inspect.isawaitable(result) else result


# =============================================================================
# Schema registry helpers
# =============================================================================
try:
    from core.sheets.schema_registry import SCHEMA_REGISTRY as _RAW_SCHEMA_REGISTRY  # type: ignore
    from core.sheets.schema_registry import get_sheet_spec as _RAW_GET_SHEET_SPEC  # type: ignore
    _SCHEMA_AVAILABLE = True
except Exception:
    _RAW_SCHEMA_REGISTRY = {}
    _RAW_GET_SHEET_SPEC = None
    _SCHEMA_AVAILABLE = False

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
    if isinstance(spec, dict) and len(spec) == 1 and not any(k in spec for k in ("columns", "fields", "headers", "keys", "display_headers")):
        first_val = list(spec.values())[0]
        if isinstance(first_val, dict):
            spec = first_val
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


def _registry_sheet_lookup(sheet: str) -> Any:
    if not SCHEMA_REGISTRY:
        return None
    candidates = [sheet, sheet.replace(" ", "_"), sheet.replace("_", " "), _norm_key(sheet), _norm_key_loose(sheet)]
    by_norm = {_norm_key(k): v for k, v in SCHEMA_REGISTRY.items()}
    by_loose = {_norm_key_loose(k): v for k, v in SCHEMA_REGISTRY.items()}
    for cand in candidates:
        if cand in SCHEMA_REGISTRY:
            return SCHEMA_REGISTRY.get(cand)
        nk = _norm_key(cand)
        if nk in by_norm:
            return by_norm[nk]
        nkl = _norm_key_loose(cand)
        if nkl in by_loose:
            return by_loose[nkl]
    return None


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
    spec = _registry_sheet_lookup(canon)
    if spec is not None:
        return spec
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
        return dict(c), h, k, "static_canonical_contract_fallback"

    return None, [], [], "missing"


def _list_sheet_names_best_effort() -> List[str]:
    names = list(STATIC_CANONICAL_SHEET_CONTRACTS.keys())
    try:
        from core.sheets.page_catalog import CANONICAL_PAGES  # type: ignore
        for item in list(CANONICAL_PAGES or []):
            s = _canonicalize_sheet_name(_safe_str(item))
            if s and s not in names:
                names.append(s)
    except Exception:
        pass
    if isinstance(SCHEMA_REGISTRY, dict):
        for k in SCHEMA_REGISTRY.keys():
            s = _canonicalize_sheet_name(_safe_str(k))
            if s and s not in names:
                names.append(s)
    return names


def _build_union_schema_keys() -> List[str]:
    keys: List[str] = []
    seen: Set[str] = set()
    for contract in STATIC_CANONICAL_SHEET_CONTRACTS.values():
        for key in contract.get("keys", []):
            k = _safe_str(key)
            if k and k not in seen:
                seen.add(k)
                keys.append(k)
    if isinstance(SCHEMA_REGISTRY, dict):
        for _, spec in SCHEMA_REGISTRY.items():
            try:
                _, spec_keys = _schema_keys_headers_from_spec(spec)
                for key in spec_keys:
                    k = _safe_str(key)
                    if k and k not in seen:
                        seen.add(k)
                        keys.append(k)
            except Exception:
                continue
    for field in TOP10_REQUIRED_FIELDS:
        if field not in seen:
            seen.add(field)
            keys.append(field)
    return keys


_SCHEMA_UNION_KEYS: List[str] = _build_union_schema_keys()


# =============================================================================
# Async utilities
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
        stats = type(
            "ProviderStats",
            (),
            {"is_circuit_open": False, "last_import_error": "" if module is not None else "provider module missing"},
        )()
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
# Engine symbols proxy
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


# =============================================================================
# DataEngineV5
# =============================================================================
class DataEngineV5:
    def __init__(self, settings: Any = None) -> None:
        self.settings = settings if settings is not None else _try_get_settings()
        self.flags = _feature_flags(self.settings)
        self.version = __version__

        self.primary_provider = (
            _safe_str(getattr(self.settings, "primary_provider", "eodhd") if self.settings is not None else _safe_env("PRIMARY_PROVIDER", "eodhd")).lower()
            or "eodhd"
        )
        self.enabled_providers = _get_env_list("ENABLED_PROVIDERS", DEFAULT_PROVIDERS)
        self.ksa_providers = _get_env_list("KSA_PROVIDERS", DEFAULT_KSA_PROVIDERS)
        self.global_providers = _get_env_list("GLOBAL_PROVIDERS", DEFAULT_GLOBAL_PROVIDERS)
        self.non_ksa_primary_provider = (
            _safe_str(
                getattr(self.settings, "non_ksa_primary_provider", None) if self.settings is not None else None,
                _safe_env("NON_KSA_PRIMARY_PROVIDER", "eodhd"),
            ).lower()
            or "eodhd"
        )
        configured_non_ksa_pages = [
            _canonicalize_sheet_name(p)
            for p in _get_env_list("NON_KSA_PRIMARY_PAGES", list(NON_KSA_EODHD_PRIMARY_PAGES))
            if _safe_str(p)
        ]
        self.page_primary_providers = {
            page: self.non_ksa_primary_provider
            for page in configured_non_ksa_pages
            if page
        }
        self.history_fallback_providers = _get_env_list(
            "HISTORY_FALLBACK_PROVIDERS",
            ["yahoo_chart", "yahoo", "eodhd", "finnhub", "tadawul", "argaam"],
        )
        self.max_concurrency = _get_env_int("DATA_ENGINE_MAX_CONCURRENCY", 25)
        self.request_timeout = _get_env_float("DATA_ENGINE_TIMEOUT_SECONDS", 20.0)
        self.ksa_disallow_eodhd = _get_env_bool("KSA_DISALLOW_EODHD", True)

        self.schema_strict_sheet_rows = _get_env_bool("SCHEMA_STRICT_SHEET_ROWS", True)
        self.top10_force_full_schema = _get_env_bool("TOP10_FORCE_FULL_SCHEMA", True)
        self.rows_hydrate_external = _get_env_bool("ROWS_HYDRATE_EXTERNAL_READER", True)

        self._sem = asyncio.Semaphore(max(1, self.max_concurrency))
        self._singleflight = SingleFlight()
        self._registry = ProviderRegistry()
        self._cache = MultiLevelCache(
            name="data_engine",
            l1_ttl=_get_env_int("CACHE_L1_TTL", 60),
            max_l1_size=_get_env_int("CACHE_L1_MAX", 5000),
        )
        self._symbols_cache = MultiLevelCache(
            name="sheet_symbols",
            l1_ttl=_get_env_int("SHEET_SYMBOLS_L1_TTL", 300),
            max_l1_size=_get_env_int("SHEET_SYMBOLS_L1_MAX", 256),
        )

        self._symbols_reader_lock = asyncio.Lock()
        self._symbols_reader_ready = False
        self._symbols_reader_obj: Any = None
        self._symbols_reader_source = ""

        self._rows_reader_lock = asyncio.Lock()
        self._rows_reader_ready = False
        self._rows_reader_obj: Any = None
        self._rows_reader_source = ""

        self._sheet_snapshots: Dict[str, Dict[str, Any]] = {}
        self._sheet_symbol_resolution_meta: Dict[str, Dict[str, Any]] = {}

        self.symbols_reader = _EngineSymbolsReaderProxy(self)

    async def aclose(self) -> None:
        return

    # ------------------------------------------------------------------
    # compatibility aliases
    # ------------------------------------------------------------------
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

    def get_page_snapshot(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(*args, **kwargs)

    # ------------------------------------------------------------------
    # snapshot helpers
    # ------------------------------------------------------------------
    def _store_sheet_snapshot(self, sheet: str, payload: Dict[str, Any]) -> None:
        target = _canonicalize_sheet_name(sheet)
        if not target or not isinstance(payload, dict):
            return
        self._sheet_snapshots[target] = dict(payload)

    def get_cached_sheet_snapshot(
        self,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet or page or sheet_name or "")
        if not target:
            return None
        snap = self._sheet_snapshots.get(target)
        return dict(snap) if isinstance(snap, dict) else None

    def get_sheet_snapshot(
        self,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(sheet=sheet, page=page, sheet_name=sheet_name)

    def get_cached_sheet_rows(
        self,
        sheet_name: Optional[str] = None,
        sheet: Optional[str] = None,
        page: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.get_cached_sheet_snapshot(sheet=sheet, page=page, sheet_name=sheet_name)

    # ------------------------------------------------------------------
    # symbol resolution meta
    # ------------------------------------------------------------------
    def _set_sheet_symbols_meta(self, sheet: str, source: str, count: int, note: Optional[str] = None) -> None:
        target = _canonicalize_sheet_name(sheet)
        if not target:
            return
        self._sheet_symbol_resolution_meta[target] = {
            "sheet": target,
            "source": source or "",
            "count": int(count or 0),
            "note": note or "",
            "timestamp_utc": _now_utc_iso(),
        }

    def _get_sheet_symbols_meta(self, sheet: str) -> Dict[str, Any]:
        target = _canonicalize_sheet_name(sheet)
        meta = self._sheet_symbol_resolution_meta.get(target)
        return dict(meta) if isinstance(meta, dict) else {}

    @staticmethod
    def _extract_row_symbol(row: Dict[str, Any]) -> str:
        if not isinstance(row, dict):
            return ""
        for k in ("symbol", "ticker", "code", "requested_symbol", "Symbol", "Ticker", "Code"):
            v = row.get(k)
            if v:
                return _safe_str(v)
        return ""

    def _get_cached_snapshot_symbol_map(self, sheet: str) -> Dict[str, Dict[str, Any]]:
        target = _canonicalize_sheet_name(sheet)
        snap = self.get_cached_sheet_snapshot(sheet=target)
        rows = _coerce_rows_list(snap)
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = normalize_symbol(self._extract_row_symbol(row))
            if sym and sym not in out:
                out[sym] = dict(row)
        return out

    @staticmethod
    def _non_empty_field_count(row: Optional[Dict[str, Any]]) -> int:
        if not isinstance(row, dict):
            return 0
        return sum(1 for v in row.values() if v not in (None, "", [], {}))

    def _get_best_cached_snapshot_row_for_symbol(self, symbol: str, prefer_sheet: str = "") -> Optional[Dict[str, Any]]:
        sym = normalize_symbol(symbol)
        if not sym:
            return None
        preferred = _canonicalize_sheet_name(prefer_sheet) if prefer_sheet else ""
        best_row: Optional[Dict[str, Any]] = None
        best_score: float = -1.0
        for sheet_name, snap in self._sheet_snapshots.items():
            rows = _coerce_rows_list(snap)
            if not rows:
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_sym = normalize_symbol(self._extract_row_symbol(row))
                if row_sym != sym:
                    continue
                score = float(self._non_empty_field_count(row))
                if preferred and _canonicalize_sheet_name(sheet_name) == preferred:
                    score += 1000.0
                if score > best_score:
                    best_score = score
                    best_row = dict(row)
        return best_row

    # ------------------------------------------------------------------
    # final payload
    # ------------------------------------------------------------------
    def _finalize_payload(
        self,
        *,
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
        dict_rows = [_strict_project_row(keys, r) for r in (row_objects or [])]
        display_row_objects = _rows_display_objects_from_rows(dict_rows, headers, keys)
        matrix_rows = _rows_matrix_from_rows(dict_rows, keys) if include_matrix else []

        payload = {
            "status": status,
            "sheet": sheet,
            "page": sheet,
            "sheet_name": sheet,
            "headers": headers,
            "display_headers": headers,
            "sheet_headers": headers,
            "column_headers": headers,
            "keys": keys,
            "columns": keys,
            "fields": keys,
            "rows": matrix_rows,
            "rows_matrix": matrix_rows,
            "row_objects": dict_rows,
            "items": dict_rows,
            "records": dict_rows,
            "data": dict_rows,
            "quotes": dict_rows,
            "display_row_objects": display_row_objects,
            "display_items": display_row_objects,
            "display_records": display_row_objects,
            "rows_dict_display": display_row_objects,
            "count": len(dict_rows),
            "meta": dict(meta or {}),
            "version": self.version,
        }
        if error is not None:
            payload["error"] = error
        return _json_safe(payload)

    # ------------------------------------------------------------------
    # rows reader discovery
    # ------------------------------------------------------------------
    async def _init_rows_reader(self) -> Tuple[Any, str]:
        if self._rows_reader_ready:
            return self._rows_reader_obj, self._rows_reader_source

        async with self._rows_reader_lock:
            if self._rows_reader_ready:
                return self._rows_reader_obj, self._rows_reader_source

            obj: Any = None
            source = ""
            for mod_path in (
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
            ):
                try:
                    mod = import_module(mod_path)
                except Exception:
                    continue

                if any(
                    callable(getattr(mod, nm, None))
                    for nm in ("get_rows_for_sheet", "read_rows_for_sheet", "get_sheet_rows", "fetch_sheet_rows", "sheet_rows", "get_rows")
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

    async def _call_rows_reader(self, obj: Any, sheet: str, limit: int) -> List[Dict[str, Any]]:
        if obj is None:
            return []

        for name in ("get_rows_for_sheet", "read_rows_for_sheet", "get_sheet_rows", "fetch_sheet_rows", "sheet_rows", "get_rows"):
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue

            variants = [
                ((), {"sheet": sheet, "limit": limit}),
                ((), {"sheet_name": sheet, "limit": limit}),
                ((), {"page": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ]
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(_get_env_float("ROWS_READER_TIMEOUT_SECONDS", 20.0)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    rows = _coerce_rows_list(result)
                    if rows:
                        return rows[:limit]
                except TypeError:
                    continue
                except Exception:
                    continue

        return []

    async def _get_rows_from_external_reader(self, sheet: str, limit: int) -> List[Dict[str, Any]]:
        obj, _src = await self._init_rows_reader()
        if obj is None:
            return []
        return await self._call_rows_reader(obj, sheet, limit)

    # ------------------------------------------------------------------
    # symbols reader discovery
    # ------------------------------------------------------------------
    async def _init_symbols_reader(self) -> Tuple[Any, str]:
        if self._symbols_reader_ready:
            return self._symbols_reader_obj, self._symbols_reader_source

        async with self._symbols_reader_lock:
            if self._symbols_reader_ready:
                return self._symbols_reader_obj, self._symbols_reader_source

            obj: Any = None
            source = ""
            for mod_path in (
                "symbols_reader",
                "core.symbols_reader",
                "integrations.symbols_reader",
                "core.integrations.symbols_reader",
                "integrations.google_sheets_service",
                "core.integrations.google_sheets_service",
                "google_sheets_service",
                "core.google_sheets_service",
            ):
                try:
                    mod = import_module(mod_path)
                except Exception:
                    continue

                if any(
                    callable(getattr(mod, nm, None))
                    for nm in (
                        "get_symbols_for_sheet",
                        "read_symbols_for_sheet",
                        "get_sheet_symbols",
                        "get_symbols",
                        "list_symbols_for_page",
                        "get_symbols_for_page",
                        "read_symbols",
                        "load_symbols",
                        "read_sheet_symbols",
                    )
                ):
                    obj = mod
                    source = mod_path
                    break

                for attr_name in ("symbols_reader", "reader", "symbol_reader", "sheet_reader", "service"):
                    candidate = getattr(mod, attr_name, None)
                    if candidate is not None:
                        obj = candidate
                        source = f"{mod_path}.{attr_name}"
                        break

                if obj is not None:
                    break

            self._symbols_reader_obj = obj
            self._symbols_reader_source = source
            self._symbols_reader_ready = True
            return obj, source

    async def _call_symbols_reader(self, obj: Any, sheet: str, limit: int) -> List[str]:
        if obj is None:
            return []

        if isinstance(obj, dict):
            for key in _sheet_lookup_candidates(sheet):
                vals = obj.get(key)
                syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                if syms:
                    return syms

        for name in (
            "get_symbols_for_sheet",
            "read_symbols_for_sheet",
            "get_sheet_symbols",
            "get_symbols_for_page",
            "list_symbols_for_page",
            "get_symbols",
            "list_symbols",
            "read_symbols",
            "load_symbols",
            "read_sheet_symbols",
        ):
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue

            variants = [
                ((), {"sheet": sheet, "limit": limit}),
                ((), {"sheet_name": sheet, "limit": limit}),
                ((), {"page": sheet, "limit": limit}),
                ((sheet,), {"limit": limit}),
                ((sheet,), {}),
            ]
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(_get_env_float("SHEET_SYMBOLS_TIMEOUT_SECONDS", 15.0)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    syms = _normalize_symbol_list(_split_symbols(result), limit=limit)
                    if not syms and isinstance(result, (dict, list)):
                        syms = _extract_symbols_from_rows(_coerce_rows_list(result), limit=limit)
                    if syms:
                        return syms
                except TypeError:
                    continue
                except Exception:
                    continue

        return []

    async def _get_symbols_from_env(self, sheet: str, limit: int) -> List[str]:
        env_candidates: List[str] = []
        specific = PAGE_SYMBOL_ENV_KEYS.get(sheet)
        if specific:
            env_candidates.append(specific)

        for cand in _sheet_lookup_candidates(sheet):
            token = re.sub(r"[^A-Za-z0-9]+", "_", cand).strip("_").upper()
            if token:
                env_candidates.extend([f"{token}_SYMBOLS", f"{token}_TICKERS", f"{token}_CODES"])

        env_candidates.extend(["TOP10_FALLBACK_SYMBOLS", "DEFAULT_PAGE_SYMBOLS", "DEFAULT_SYMBOLS"])

        seen: Set[str] = set()
        for env_key in env_candidates:
            if not env_key or env_key in seen:
                continue
            seen.add(env_key)
            raw = os.getenv(env_key, "") or ""
            if raw.strip():
                syms = _normalize_symbol_list(_split_symbols(raw), limit=limit)
                if syms:
                    return syms
        return []

    async def _get_symbols_from_settings(self, sheet: str, limit: int) -> List[str]:
        if self.settings is None:
            return []
        candidates = _sheet_lookup_candidates(sheet)

        for attr_name in (
            f"{sheet.lower()}_symbols",
            f"{sheet.lower()}_tickers",
            f"{sheet.lower()}_codes",
            "default_symbols",
            "page_symbols",
            "sheet_symbols",
        ):
            try:
                raw = getattr(self.settings, attr_name, None)
            except Exception:
                raw = None

            if isinstance(raw, dict):
                for cand in candidates:
                    vals = raw.get(cand)
                    syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                    if syms:
                        return syms
            elif raw:
                syms = _normalize_symbol_list(_split_symbols(raw), limit=limit)
                if syms:
                    return syms

        return []

    async def _get_symbols_from_page_catalog(self, sheet: str, limit: int) -> List[str]:
        candidates = _sheet_lookup_candidates(sheet)

        for mod_path in ("core.sheets.page_catalog", "sheets.page_catalog"):
            try:
                mod = import_module(mod_path)
            except Exception:
                continue

            for attr_name in ("PAGE_SYMBOLS", "SHEET_SYMBOLS", "DEFAULT_PAGE_SYMBOLS", "PAGE_DEFAULT_SYMBOLS"):
                mapping = getattr(mod, attr_name, None)
                if isinstance(mapping, dict):
                    for cand in candidates:
                        vals = mapping.get(cand)
                        syms = _normalize_symbol_list(_split_symbols(vals), limit=limit)
                        if syms:
                            return syms

            for fn_name in ("get_default_symbols", "get_page_symbols", "get_symbols_for_page"):
                fn = getattr(mod, fn_name, None)
                if not callable(fn):
                    continue
                for args, kwargs in [
                    ((sheet,), {"limit": limit}),
                    ((sheet,), {}),
                    ((), {"page": sheet, "limit": limit}),
                    ((), {"sheet": sheet, "limit": limit}),
                ]:
                    try:
                        result = await _call_maybe_async(fn, *args, **kwargs)
                        syms = _normalize_symbol_list(_split_symbols(result), limit=limit)
                        if syms:
                            return syms
                    except TypeError:
                        continue
                    except Exception:
                        continue

        return []

    async def _get_symbols_for_sheet_impl(self, sheet: str, limit: int = 5000, body: Optional[Dict[str, Any]] = None) -> List[str]:
        target = _canonicalize_sheet_name(sheet)
        if target in SPECIAL_SHEETS:
            self._set_sheet_symbols_meta(target, "special_sheet", 0)
            return []

        limit = max(1, min(5000, int(limit or 5000)))
        from_body = _extract_requested_symbols_from_body(body, limit=limit)
        if from_body:
            self._set_sheet_symbols_meta(target, "body_symbols", len(from_body))
            return from_body

        cached = await self._symbols_cache.get(sheet=target, limit=limit)
        if isinstance(cached, list) and cached:
            syms = _normalize_symbol_list(cached, limit=limit)
            self._set_sheet_symbols_meta(target, "symbols_cache", len(syms))
            return syms

        obj, src = await self._init_symbols_reader()
        if obj is not None:
            syms = await self._call_symbols_reader(obj, target, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(target, f"symbols_reader:{src or 'unknown'}", len(syms))
                await self._symbols_cache.set(syms, sheet=target, limit=limit)
                return syms

        syms = await self._get_symbols_from_page_catalog(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "page_catalog", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms

        syms = await self._get_symbols_from_env(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "env", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms

        syms = await self._get_symbols_from_settings(target, limit=limit)
        if syms:
            self._set_sheet_symbols_meta(target, "settings", len(syms))
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms

        snap = self.get_cached_sheet_snapshot(sheet=target)
        snap_rows = _coerce_rows_list(snap)
        if snap_rows:
            syms = _extract_symbols_from_rows(snap_rows, limit=limit)
            if syms:
                self._set_sheet_symbols_meta(target, "snapshot_rows", len(syms))
                await self._symbols_cache.set(syms, sheet=target, limit=limit)
                return syms

        emergency = EMERGENCY_PAGE_SYMBOLS.get(target) or []
        if emergency:
            syms = _normalize_symbol_list(emergency, limit=limit)
            self._set_sheet_symbols_meta(target, "emergency_page_symbols", len(syms), note="last_resort_fallback")
            await self._symbols_cache.set(syms, sheet=target, limit=limit)
            return syms

        self._set_sheet_symbols_meta(target, "none", 0, note=(src or "no_source"))
        return []

    async def get_sheet_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def get_page_symbols(
        self,
        page: Optional[str] = None,
        *,
        sheet: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def list_symbols_for_page(self, page: str, *, limit: int = 5000, body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def list_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    async def get_symbols(
        self,
        sheet: Optional[str] = None,
        *,
        page: Optional[str] = None,
        sheet_name: Optional[str] = None,
        limit: int = 5000,
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[str]:
        target_sheet, effective_limit, _offset, _mode, normalized_body, _request_parts = _normalize_route_call_inputs(
            page=page,
            sheet=sheet,
            sheet_name=sheet_name,
            limit=limit,
            offset=0,
            mode="",
            body=body,
            extras=kwargs,
        )
        return await self._get_symbols_for_sheet_impl(target_sheet, limit=effective_limit, body=normalized_body)

    # ------------------------------------------------------------------
    # quote context / provider preference helpers
    # ------------------------------------------------------------------
    def _resolve_quote_page_context(
        self,
        *,
        page: Optional[str] = None,
        sheet: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        schema: Any = None,
        extras: Optional[Dict[str, Any]] = None,
    ) -> str:
        merged = _merge_route_body_dicts(body, extras or {})
        target_raw = (
            page
            or sheet
            or _safe_str(merged.get("page"))
            or _safe_str(merged.get("sheet"))
            or _safe_str(merged.get("sheet_name"))
            or _safe_str(merged.get("page_name"))
            or _safe_str(merged.get("name"))
            or _safe_str(merged.get("tab"))
            or _safe_str(merged.get("worksheet"))
        )
        if not target_raw and isinstance(schema, str):
            target_raw = schema
        return _canonicalize_sheet_name(target_raw) if target_raw else ""

    def _page_primary_provider_for(self, symbol: str, page: str = "") -> str:
        info = get_symbol_info(symbol)
        is_ksa_sym = bool(info.get("is_ksa"))
        page_ctx = _canonicalize_sheet_name(page) if page else ""
        if (
            not is_ksa_sym
            and page_ctx
            and page_ctx in self.page_primary_providers
        ):
            candidate = _safe_str(self.page_primary_providers.get(page_ctx), self.non_ksa_primary_provider).lower()
            if candidate:
                return candidate
        return self.primary_provider

    def _provider_profile_key(self, symbol: str, page: str = "") -> str:
        info = get_symbol_info(symbol)
        page_ctx = _canonicalize_sheet_name(page) if page else ""
        primary = self._page_primary_provider_for(symbol, page_ctx)
        market = "ksa" if bool(info.get("is_ksa")) else "global"
        return f"{market}|{page_ctx or 'default'}|{primary or 'none'}"

    # ------------------------------------------------------------------
    # quote APIs
    # ------------------------------------------------------------------
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

            call_variants = [
                ((symbol,), {}),
                ((), {"symbol": symbol}),
                ((), {"ticker": symbol}),
                ((), {"requested_symbol": symbol}),
                ((symbol,), {"settings": self.settings}),
                ((), {"symbol": symbol, "settings": self.settings}),
            ]

            result = None
            collected_errs: List[str] = []
            for args, kwargs in call_variants:
                try:
                    async with asyncio.timeout(self.request_timeout):
                        result = await _call_maybe_async(fn, *args, **kwargs)
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

        primary_provider = self._page_primary_provider_for(symbol, page_ctx)
        if primary_provider and _provider_allowed(primary_provider):
            if primary_provider in providers:
                providers = [p for p in providers if p != primary_provider]
            providers.insert(0, primary_provider)

        if (not is_ksa_sym) and page_ctx and page_ctx in self.page_primary_providers and _provider_allowed("eodhd"):
            providers = [p for p in providers if p != "eodhd"]
            providers.insert(0, "eodhd")

        seen: Set[str] = set()
        out: List[str] = []
        for p in providers:
            p2 = _safe_str(p).lower()
            if not p2 or p2 in seen:
                continue
            seen.add(p2)
            out.append(p2)
        return out

    def _rows_from_parallel_series(
        self,
        timestamps: Sequence[Any],
        opens: Optional[Sequence[Any]] = None,
        highs: Optional[Sequence[Any]] = None,
        lows: Optional[Sequence[Any]] = None,
        closes: Optional[Sequence[Any]] = None,
        volumes: Optional[Sequence[Any]] = None,
        adjcloses: Optional[Sequence[Any]] = None,
    ) -> List[Dict[str, Any]]:
        ts_list = list(timestamps or [])
        if not ts_list:
            return []

        rows: List[Dict[str, Any]] = []
        for idx, ts in enumerate(ts_list):
            row = {
                "timestamp": ts,
                "open": opens[idx] if opens is not None and idx < len(opens) else None,
                "high": highs[idx] if highs is not None and idx < len(highs) else None,
                "low": lows[idx] if lows is not None and idx < len(lows) else None,
                "close": closes[idx] if closes is not None and idx < len(closes) else None,
                "volume": volumes[idx] if volumes is not None and idx < len(volumes) else None,
                "adjclose": adjcloses[idx] if adjcloses is not None and idx < len(adjcloses) else None,
            }
            if any(v is not None for k, v in row.items() if k != "timestamp"):
                rows.append(row)
        return rows

    def _coerce_history_rows(self, result: Any) -> List[Dict[str, Any]]:
        if result is None:
            return []
        if hasattr(result, "to_dict") and callable(getattr(result, "to_dict")):
            try:
                rows = result.to_dict("records")
                if isinstance(rows, list):
                    return [dict(r) for r in rows if isinstance(r, dict)]
            except Exception:
                pass
        if isinstance(result, list):
            out: List[Dict[str, Any]] = []
            for item in result:
                if isinstance(item, dict):
                    if {"open", "high", "low", "close"} & set(item.keys()) and not isinstance(item.get("open"), list):
                        out.append(dict(item))
                    else:
                        nested = self._coerce_history_rows(item)
                        if nested:
                            out.extend(nested)
                        else:
                            out.append(dict(item))
                elif isinstance(item, (list, tuple)) and len(item) >= 5:
                    out.append({
                        "timestamp": item[0],
                        "open": item[1],
                        "high": item[2],
                        "low": item[3],
                        "close": item[4],
                        "volume": item[5] if len(item) > 5 else None,
                    })
            return out
        if isinstance(result, dict):
            if isinstance(result.get("chart"), Mapping):
                chart = result.get("chart") or {}
                nested = self._coerce_history_rows(chart.get("result"))
                if nested:
                    return nested

            if isinstance(result.get("result"), list):
                for item in result.get("result") or []:
                    nested = self._coerce_history_rows(item)
                    if nested:
                        return nested

            timestamps = result.get("timestamp") or result.get("timestamps") or result.get("time")
            if isinstance(timestamps, list) and timestamps:
                indicators = result.get("indicators") if isinstance(result.get("indicators"), Mapping) else {}
                quote = None
                if isinstance(indicators.get("quote"), list) and indicators.get("quote"):
                    quote = indicators.get("quote")[0]
                adj = None
                if isinstance(indicators.get("adjclose"), list) and indicators.get("adjclose"):
                    adj = indicators.get("adjclose")[0]
                if isinstance(quote, Mapping):
                    rows = self._rows_from_parallel_series(
                        timestamps=timestamps,
                        opens=list(quote.get("open") or []),
                        highs=list(quote.get("high") or []),
                        lows=list(quote.get("low") or []),
                        closes=list(quote.get("close") or []),
                        volumes=list(quote.get("volume") or []),
                        adjcloses=list(adj.get("adjclose") or []) if isinstance(adj, Mapping) else None,
                    )
                    if rows:
                        return rows

            if any(isinstance(result.get(k), list) for k in ("close", "open", "high", "low", "volume")):
                ts = result.get("timestamp") or list(range(len(result.get("close") or result.get("price") or [])))
                rows = self._rows_from_parallel_series(
                    timestamps=list(ts or []),
                    opens=list(result.get("open") or []),
                    highs=list(result.get("high") or []),
                    lows=list(result.get("low") or []),
                    closes=list(result.get("close") or result.get("adjclose") or result.get("price") or result.get("value") or []),
                    volumes=list(result.get("volume") or []),
                    adjcloses=list(result.get("adjclose") or []),
                )
                if rows:
                    return rows

            for key in ("Time Series (Daily)", "time_series", "series"):
                series = result.get(key)
                if isinstance(series, Mapping):
                    rows: List[Dict[str, Any]] = []
                    for ts, entry in series.items():
                        if not isinstance(entry, Mapping):
                            continue
                        rows.append({
                            "timestamp": ts,
                            "open": entry.get("1. open") or entry.get("open"),
                            "high": entry.get("2. high") or entry.get("high"),
                            "low": entry.get("3. low") or entry.get("low"),
                            "close": entry.get("4. close") or entry.get("close"),
                            "volume": entry.get("5. volume") or entry.get("volume"),
                        })
                    if rows:
                        rows.sort(key=lambda r: _safe_str(r.get("timestamp")))
                        return rows

            for key in ("history", "bars", "candles", "prices", "data", "items", "rows", "chart"):
                if key in result:
                    nested = self._coerce_history_rows(result.get(key))
                    if nested:
                        return nested
            if {"open", "high", "low", "close"} & set(result.keys()):
                return [dict(result)]
        return []

    def _safe_mean(self, values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    def _safe_std(self, values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = self._safe_mean(values)
        var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return math.sqrt(max(var, 0.0))

    def _quantile(self, values: List[float], q: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        pos = max(0.0, min(1.0, q)) * (len(ordered) - 1)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return ordered[lo]
        frac = pos - lo
        return ordered[lo] + (ordered[hi] - ordered[lo]) * frac

    def _compute_history_patch_from_rows(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        closes: List[float] = []
        highs: List[float] = []
        lows: List[float] = []
        volumes: List[float] = []
        opens: List[float] = []
        for row in rows or []:
            close = _as_float(row.get("close") or row.get("adjclose") or row.get("price") or row.get("value"))
            high = _as_float(row.get("high") or row.get("day_high"))
            low = _as_float(row.get("low") or row.get("day_low"))
            vol = _as_float(row.get("volume"))
            opn = _as_float(row.get("open") or row.get("open_price"))
            if close is not None:
                closes.append(close)
            if high is not None:
                highs.append(high)
            if low is not None:
                lows.append(low)
            if vol is not None:
                volumes.append(vol)
            if opn is not None:
                opens.append(opn)
        # v5.66.0 PHASE-JJ: Detect candlestick patterns from raw OHLC bars.
        candle_fields: Dict[str, Any] = {}
        try:
            candle_fields = detect_candlestick_patterns(rows or [])
        except Exception as cs_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-JJ candlestick detection failed: %s: %s",
                __version__, cs_err.__class__.__name__, cs_err,
            )
        if len(closes) < 2:
            return candle_fields if candle_fields else {}
        returns = []
        for prev, cur in zip(closes[:-1], closes[1:]):
            if prev not in (None, 0):
                returns.append((cur / prev) - 1.0)
        if not returns:
            return {}
        recent14 = closes[-15:]
        gains: List[float] = []
        losses: List[float] = []
        for prev, cur in zip(recent14[:-1], recent14[1:]):
            delta = cur - prev
            if delta >= 0:
                gains.append(delta)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(delta))
        avg_gain = self._safe_mean(gains)
        avg_loss = self._safe_mean(losses)
        rsi = None
        if gains and losses:
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
        last30 = returns[-30:] if len(returns) >= 30 else returns
        last90 = returns[-90:] if len(returns) >= 90 else returns
        vol30 = self._safe_std(last30) * math.sqrt(252.0) if last30 else None
        vol90 = self._safe_std(last90) * math.sqrt(252.0) if last90 else None
        mean_daily = self._safe_mean(last90)
        std_daily = self._safe_std(last90)
        sharpe = (mean_daily / std_daily) * math.sqrt(252.0) if std_daily not in (None, 0.0) else None
        var95 = abs(self._quantile(last90, 0.05)) if last90 else None
        peak = closes[0]
        max_dd = 0.0
        for price in closes:
            peak = max(peak, price)
            if peak > 0:
                dd = (price / peak) - 1.0
                max_dd = min(max_dd, dd)
        # v5.67.0 PATCH 8: emit max_drawdown_1y / var_95_1d / percent_change as
        # FRACTION (was POINTS pre-v5.67.0). max_drawdown_1y keeps its SIGN
        # (negative) — abs() removed so the formatter "0.00%" + signed-fraction
        # displays correctly (e.g. -33.8% for max_dd = -0.338).
        patch: Dict[str, Any] = {
            "current_price": closes[-1],
            "previous_close": closes[-2],
            "open_price": opens[-1] if opens else None,
            "day_high": highs[-1] if highs else None,
            "day_low": lows[-1] if lows else None,
            "week_52_high": max(highs) if highs else max(closes),
            "week_52_low": min(lows) if lows else min(closes),
            "avg_volume_10d": self._safe_mean(volumes[-10:]) if volumes else None,
            "avg_volume_30d": self._safe_mean(volumes[-30:]) if volumes else None,
            "volatility_30d": vol30,
            "volatility_90d": vol90,
            "max_drawdown_1y": max_dd,
            "var_95_1d": var95 if var95 is not None else None,
            "sharpe_1y": sharpe,
            "rsi_14": rsi,
            "price_change": closes[-1] - closes[-2],
            "percent_change": ((closes[-1] - closes[-2]) / closes[-2]) if closes[-2] not in (None, 0) else None,
            "volume": volumes[-1] if volumes else None,
        }
        if patch.get("current_price") is not None and patch.get("week_52_high") is not None and patch.get("week_52_low") is not None:
            hi = _as_float(patch.get("week_52_high"))
            lo = _as_float(patch.get("week_52_low"))
            cp = _as_float(patch.get("current_price"))
            if hi is not None and lo is not None and cp is not None and hi > lo:
                patch["week_52_position_pct"] = ((cp - lo) / (hi - lo)) * 100.0
        result_patch = {k: v for k, v in patch.items() if v is not None}
        if candle_fields:
            for k, v in candle_fields.items():
                if v not in (None, "", 0.0):
                    result_patch[k] = v
                elif k not in result_patch:
                    result_patch[k] = v
        return result_patch

    async def _fetch_history_patch(self, provider: str, symbol: str) -> Dict[str, Any]:
        module, _stats = await self._registry.get_provider(provider)
        if module is None:
            return {}
        callables = []
        for name in (
            "get_history", "fetch_history", "get_price_history", "fetch_price_history", "history",
            "get_chart", "fetch_chart", "get_chart_history", "fetch_chart_history",
            "get_historical_data", "fetch_historical_data", "get_history_rows", "fetch_history_rows",
            "get_timeseries", "fetch_timeseries", "get_series", "fetch_series", "get_ohlcv", "fetch_ohlcv",
        ):
            fn = getattr(module, name, None)
            if callable(fn):
                callables.append(fn)
        if not callables:
            return {}
        variants = [
            ((symbol,), {"period": "1y", "interval": "1d"}),
            ((symbol,), {"range": "1y", "interval": "1d"}),
            ((symbol,), {"lookback": "1y", "interval": "1d"}),
            ((), {"symbol": symbol, "period": "1y", "interval": "1d"}),
            ((), {"ticker": symbol, "period": "1y", "interval": "1d"}),
            ((), {"code": symbol, "period": "1y", "interval": "1d"}),
            ((), {"symbol": symbol, "range": "1y", "interval": "1d"}),
            ((), {"ticker": symbol, "range": "1y", "interval": "1d"}),
            ((symbol,), {}),
        ]
        for fn in callables:
            for args, kwargs in variants:
                try:
                    async with asyncio.timeout(max(5.0, self.request_timeout)):
                        result = await _call_maybe_async(fn, *args, **kwargs)
                    rows = self._coerce_history_rows(result)
                    patch = self._compute_history_patch_from_rows(rows)
                    if patch:
                        patch["data_provider"] = provider
                        return patch
                except TypeError:
                    continue
                except Exception:
                    continue
        return {}

    async def _get_history_patch_best_effort(self, symbol: str, providers: Sequence[str], page: str = "") -> Dict[str, Any]:
        candidates: List[str] = []
        for provider in list(providers or []):
            if provider and provider not in candidates:
                candidates.append(provider)

        page_ctx = _canonicalize_sheet_name(page) if page else ""
        preferred_history = list(self.history_fallback_providers or [])
        primary_provider = self._page_primary_provider_for(symbol, page_ctx)

        if (not get_symbol_info(symbol).get("is_ksa")) and page_ctx and page_ctx in self.page_primary_providers:
            preferred_history = [primary_provider, "yahoo_chart", "yahoo", "eodhd", "finnhub"] + preferred_history
        elif symbol.endswith("=F") or symbol.endswith("=X"):
            preferred_history = ["yahoo_chart", "yahoo", "eodhd", "finnhub"] + preferred_history

        for provider in preferred_history:
            provider = _safe_str(provider).lower()
            if provider and provider not in candidates and (provider in self.enabled_providers or provider in preferred_history):
                candidates.append(provider)

        for provider in candidates:
            patch = await self._fetch_history_patch(provider, symbol)
            if patch:
                return patch
        return {}

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
        normalized_patches: List[Tuple[str, Dict[str, Any], float]] = []
        for prov, patch, latency in patches:
            canonical = _canonicalize_provider_row(patch, requested_symbol=requested_symbol, normalized_symbol=norm, provider=prov)
            normalized_patches.append((prov, canonical, latency))

        sorted_patches = sorted(
            normalized_patches,
            key=lambda item: (
                PROVIDER_PRIORITIES.get(item[0], 999),
                -sum(1 for v in item[1].values() if v not in (None, "", [], {})),
            ),
        )

        for prov, patch, latency in sorted_patches:
            merged["data_sources"].append(prov)
            merged["provider_latency"][prov] = round(float(latency or 0.0), 2)
            for k, v in patch.items():
                if k in protected or v is None:
                    continue
                if k not in merged or merged.get(k) in (None, "", [], {}):
                    merged[k] = v

        merged = _canonicalize_provider_row(merged, requested_symbol=requested_symbol, normalized_symbol=norm, provider=_safe_str((merged.get("data_sources") or [""])[0] if isinstance(merged.get("data_sources"), list) else ""))
        return merged

    def _data_quality(self, row: Dict[str, Any]) -> str:
        if _as_float(row.get("current_price")) is None:
            return QuoteQuality.MISSING.value
        return QuoteQuality.GOOD.value if any(row.get(k) is not None for k in ("overall_score", "forecast_price_3m", "pb_ratio")) else QuoteQuality.FAIR.value

    # =================================================================
    # v5.62.0 PHASE-Z — Yahoo enrichment pass
    # =================================================================
    async def _fetch_yahoo_fundamentals_patch(self, symbol: str) -> Optional[Dict[str, Any]]:
        mod = _import_yahoo_provider_module("yahoo_fundamentals_provider")
        if mod is None:
            logger.debug("[engine_v2 v%s] PHASE-Z fundamentals: module not importable", __version__)
            return None

        yahoo_symbol = _yahoo_symbol_for(symbol)
        if not yahoo_symbol:
            return None

        fn = _pick_yahoo_callable(
            mod,
            "get_quote_patch", "fetch_fundamentals_patch", "fetch_enriched_quote_patch",
            "fetch_quote", "get_quote", "quote", "enriched_quote",
        )
        if fn is None:
            logger.debug("[engine_v2 v%s] PHASE-Z fundamentals: no compatible callable", __version__)
            return None

        try:
            if inspect.iscoroutinefunction(fn):
                patch = await fn(yahoo_symbol)
            else:
                patch = await asyncio.to_thread(fn, yahoo_symbol)
                if inspect.isawaitable(patch):
                    patch = await patch
        except Exception as exc:
            logger.debug(
                "[engine_v2 v%s] PHASE-Z fundamentals call failed for %s (yahoo=%s): %s: %s",
                __version__, symbol, yahoo_symbol, exc.__class__.__name__, exc,
            )
            return None

        if patch is None:
            return None
        if isinstance(patch, dict):
            return dict(patch)
        try:
            if hasattr(patch, "model_dump"):
                d = patch.model_dump(mode="python")
                if isinstance(d, dict):
                    return d
        except Exception:
            pass
        return None

    async def _fetch_yahoo_chart_patch(self, symbol: str) -> Optional[Dict[str, Any]]:
        mod = _import_yahoo_provider_module("yahoo_chart_provider")
        if mod is None:
            logger.debug("[engine_v2 v%s] PHASE-Z chart: module not importable", __version__)
            return None

        yahoo_symbol = _yahoo_symbol_for(symbol)
        if not yahoo_symbol:
            return None

        fn = _pick_yahoo_callable(
            mod,
            "get_quote_patch", "fetch_chart_patch", "fetch_quote", "get_quote", "quote",
        )
        patch: Optional[Dict[str, Any]] = None
        if fn is not None:
            try:
                if inspect.iscoroutinefunction(fn):
                    res = await fn(yahoo_symbol)
                else:
                    res = await asyncio.to_thread(fn, yahoo_symbol)
                    if inspect.isawaitable(res):
                        res = await res
                if res is not None:
                    if isinstance(res, dict):
                        patch = dict(res)
                    elif hasattr(res, "model_dump"):
                        try:
                            d = res.model_dump(mode="python")
                            if isinstance(d, dict):
                                patch = d
                        except Exception:
                            patch = None
            except Exception as exc:
                logger.debug(
                    "[engine_v2 v%s] PHASE-Z chart patch call failed for %s (yahoo=%s): %s: %s",
                    __version__, symbol, yahoo_symbol, exc.__class__.__name__, exc,
                )

        history_needs = (
            patch is None
            or not isinstance(patch, dict)
            or any(_is_missing_or_unknown_field(patch.get(k)) for k in
                   ("rsi_14", "volatility_30d", "volatility_90d",
                    "max_drawdown_1y", "var_95_1d", "sharpe_1y"))
        )
        if history_needs:
            hist_fn = _pick_yahoo_callable(
                mod,
                "get_history_rows", "fetch_history_rows", "get_rows", "get_chart_rows", "fetch_chart_rows",
            )
            rows: List[Dict[str, Any]] = []
            if hist_fn is not None:
                for kwargs in (
                    {"symbol": yahoo_symbol, "interval": "1d", "range": "1y"},
                    {"symbol": yahoo_symbol, "period": "1y"},
                    {"symbol": yahoo_symbol},
                ):
                    try:
                        if inspect.iscoroutinefunction(hist_fn):
                            raw = await hist_fn(**kwargs)
                        else:
                            raw = await asyncio.to_thread(lambda kw=kwargs: hist_fn(**kw))
                            if inspect.isawaitable(raw):
                                raw = await raw
                    except TypeError:
                        continue
                    except Exception as exc:
                        logger.debug(
                            "[engine_v2 v%s] PHASE-Z chart history call failed for %s (yahoo=%s): %s: %s",
                            __version__, symbol, yahoo_symbol, exc.__class__.__name__, exc,
                        )
                        break
                    if isinstance(raw, list) and raw:
                        rows = [r for r in raw if isinstance(r, dict)]
                        if rows:
                            break

            if rows:
                hist_patch = self._compute_history_patch_from_rows(rows)
                if hist_patch:
                    if patch is None:
                        patch = {}
                    for k, v in hist_patch.items():
                        if k not in patch or _is_missing_or_unknown_field(patch.get(k)):
                            patch[k] = v

        return patch if patch else None

    async def _apply_yahoo_enrichment_pass(
        self,
        row: Dict[str, Any],
        normalized: str,
        requested: str,
    ) -> Dict[str, Any]:
        if not _yahoo_enrichment_enabled():
            return row

        needs_fund, needs_chart = _row_needs_yahoo_enrichment(row)
        if not needs_fund and not needs_chart:
            return row

        diag: Dict[str, Any] = {
            "ts": time.time(),
            "symbol": normalized,
            "fundamentals_called": False,
            "chart_called": False,
            "fundamentals_filled_fields": [],
            "chart_filled_fields": [],
        }

        if needs_fund:
            diag["fundamentals_called"] = True
            fund_patch = await self._fetch_yahoo_fundamentals_patch(normalized)
            if fund_patch:
                filtered, filled = _filter_patch_to_missing_fields(
                    row, fund_patch, _YAHOO_FUNDAMENTAL_FIELDS,
                )
                if filled:
                    for k, v in filtered.items():
                        if _is_missing_or_unknown_field(row.get(k)):
                            row[k] = v
                    diag["fundamentals_filled_fields"] = filled
                    sources = row.get("data_sources")
                    if isinstance(sources, list):
                        if "yahoo_fundamentals" not in sources:
                            sources.append("yahoo_fundamentals")
                    else:
                        row["data_sources"] = ["yahoo_fundamentals"]
                    _append_yahoo_warning_tag(
                        row,
                        "yahoo_fundamentals_enrichment_applied:"
                        + ",".join(filled[:8])
                        + ("..." if len(filled) > 8 else ""),
                    )

        if needs_chart:
            diag["chart_called"] = True
            chart_patch = await self._fetch_yahoo_chart_patch(normalized)
            if chart_patch:
                filtered, filled = _filter_patch_to_missing_fields(
                    row, chart_patch, _YAHOO_CHART_FIELDS,
                )
                if filled:
                    for k, v in filtered.items():
                        if _is_missing_or_unknown_field(row.get(k)):
                            row[k] = v
                    diag["chart_filled_fields"] = filled
                    sources = row.get("data_sources")
                    if isinstance(sources, list):
                        if "yahoo_chart" not in sources:
                            sources.append("yahoo_chart")
                    else:
                        row["data_sources"] = ["yahoo_chart"]
                    _append_yahoo_warning_tag(
                        row,
                        "yahoo_chart_enrichment_applied:"
                        + ",".join(filled[:8])
                        + ("..." if len(filled) > 8 else ""),
                    )

        try:
            _YAHOO_ENRICHMENT_LAST_PASS.update(diag)
        except Exception:
            pass

        if diag["fundamentals_filled_fields"] or diag["chart_filled_fields"]:
            logger.info(
                "[engine_v2 v%s] PHASE-Z enrichment for %s: fund=%d chart=%d",
                __version__, normalized,
                len(diag["fundamentals_filled_fields"]),
                len(diag["chart_filled_fields"]),
            )

        return row

    async def _get_enriched_quote_impl(self, symbol: str, use_cache: bool = True, *, page: str = "", sheet: str = "", body: Optional[Dict[str, Any]] = None, **kwargs: Any) -> UnifiedQuote:
        info = get_symbol_info(symbol)
        norm = _safe_str(info.get("normalized"))

        if not norm:
            row = {
                "symbol": _safe_str(symbol),
                "symbol_normalized": None,
                "requested_symbol": _safe_str(symbol),
                "data_quality": QuoteQuality.MISSING.value,
                "error": "Invalid symbol",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }
            row = _apply_symbol_context_defaults(row, symbol=_safe_str(symbol))
            _compute_scores_fallback(row)
            _compute_recommendation(row)
            return UnifiedQuote(**row)

        page_context = self._resolve_quote_page_context(page=page, sheet=sheet, body=body, extras=kwargs)
        provider_profile = self._provider_profile_key(norm, page_context)

        if use_cache:
            cached = await self._cache.get(symbol=norm, provider_profile=provider_profile)
            if isinstance(cached, dict) and cached:
                return UnifiedQuote(**cached)

        providers = self._providers_for(norm, page=page_context)
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
                "data_sources": [],
                "provider_latency": {},
                "warnings": "No live provider data available",
                "last_updated_utc": _now_utc_iso(),
                "last_updated_riyadh": _now_riyadh_iso(),
            }

        row = _apply_symbol_context_defaults(row, symbol=norm)

        cached_best_row = self._get_best_cached_snapshot_row_for_symbol(norm, prefer_sheet=page_context)
        if cached_best_row:
            row = _merge_missing_fields(row, cached_best_row)
            row = _apply_page_row_backfill(page_context or sheet or "", row)

        missing_history_fields = [
            "current_price", "previous_close", "day_high", "day_low",
            "week_52_high", "week_52_low", "avg_volume_10d", "avg_volume_30d",
            "volatility_30d", "volatility_90d", "max_drawdown_1y",
            "var_95_1d", "sharpe_1y", "rsi_14",
        ]
        if any(row.get(k) in (None, "", [], {}) for k in missing_history_fields):
            hist_patch = await self._get_history_patch_best_effort(norm, providers, page=page_context)
            if hist_patch:
                row = self._merge(symbol, norm, patches_ok + [(hist_patch.get("data_provider") or "history", hist_patch, 0.0)])
                row = _apply_symbol_context_defaults(row, symbol=norm)

        if cached_best_row:
            row = _merge_missing_fields(row, cached_best_row)
            row = _apply_page_row_backfill(page_context or sheet or "", row)

        try:
            row = await self._apply_yahoo_enrichment_pass(row, norm, _safe_str(symbol))
        except Exception as enrich_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-Z enrichment pass failed for %s: %s: %s",
                __version__, norm, enrich_err.__class__.__name__, enrich_err,
            )

        try:
            row = _apply_phase_bb_sanity(row)
        except Exception as bb_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-BB sanity guard failed for %s: %s: %s",
                __version__, norm, bb_err.__class__.__name__, bb_err,
            )

        if _as_float(row.get("current_price")) is not None and _safe_str(row.get("warnings")).lower() == "no live provider data available":
            row["warnings"] = "Recovered from history/chart fallback"
        elif _as_float(row.get("current_price")) is None and not row.get("warnings"):
            row["warnings"] = "No live quote payload and no usable history fallback"

        _compute_scores_fallback(row)
        _compute_recommendation(row)

        try:
            row = _apply_phase_dd_enhancements(row)
        except Exception as dd_err:
            logger.debug(
                "[engine_v2 v%s] PHASE-DD enhancement failed for %s: %s: %s",
                __version__, norm, dd_err.__class__.__name__, dd_err,
            )

        row["data_quality"] = self._data_quality(row)
        row["data_provider"] = row.get("data_provider") or ((row.get("data_sources") or [""])[0] if isinstance(row.get("data_sources"), list) else "")
        q = UnifiedQuote(**row)

        if use_cache:
            await self._cache.set(_model_to_dict(q), symbol=norm, provider_profile=provider_profile)

        return q

    async def get_enriched_quote(
        self,
        symbol: str,
        use_cache: bool = True,
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> UnifiedQuote:
        page_context = self._resolve_quote_page_context(page=page, sheet=sheet, body=body, schema=schema, extras=kwargs)
        provider_profile = self._provider_profile_key(normalize_symbol(symbol), page_context)
        key = f"quote:{normalize_symbol(symbol)}:{provider_profile}:{'cache' if use_cache else 'live'}"
        raw_q = await self._singleflight.execute(
            key,
            lambda: self._get_enriched_quote_impl(symbol, use_cache, page=page_context, body=body, schema=schema, **kwargs),
        )
        if schema is None:
            return raw_q
        row = _model_to_dict(raw_q)
        if isinstance(schema, str):
            _spec, hdrs, keys, _src = _schema_for_sheet(_safe_str(schema))
            projected = _normalize_to_schema_keys(keys, hdrs, row)
            return UnifiedQuote(**projected)
        return raw_q

    async def get_enriched_quote_dict(
        self,
        symbol: str,
        use_cache: bool = True,
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        q = await self.get_enriched_quote(symbol, use_cache=use_cache, schema=schema, page=page, sheet=sheet, body=body, **kwargs)
        return _model_to_dict(q)

    async def get_enriched_quotes(
        self,
        symbols: List[str],
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[UnifiedQuote]:
        if not symbols:
            return []
        batch = max(1, min(500, _get_env_int("QUOTE_BATCH_SIZE", 25)))
        out: List[UnifiedQuote] = []
        for i in range(0, len(symbols), batch):
            part = symbols[i:i + batch]
            out.extend(
                await asyncio.gather(*[
                    self.get_enriched_quote(s, schema=schema, page=page, sheet=sheet, body=body, **kwargs)
                    for s in part
                ])
            )
        return out

    async def get_enriched_quotes_batch(
        self,
        symbols: List[str],
        mode: str = "",
        *,
        schema: Any = None,
        page: str = "",
        sheet: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        norm_syms = _normalize_symbol_list(symbols, limit=len(symbols) + 10)
        quotes = await asyncio.gather(*[self.get_enriched_quote_dict(s, schema=schema, page=page, sheet=sheet, body=body, **kwargs) for s in norm_syms])
        for req_sym, qd in zip(norm_syms, quotes):
            out[req_sym] = qd
            norm = _safe_str(qd.get("symbol_normalized") or qd.get("symbol"))
            if norm:
                out[norm] = qd
        for req in symbols:
            req2 = _safe_str(req)
            if req2 and req2 not in out:
                norm = normalize_symbol(req2)
                if norm in out:
                    out[req2] = out[norm]
        return out

    get_quote = get_enriched_quote
    get_quotes = get_enriched_quotes
    fetch_quote = get_enriched_quote
    fetch_quotes = get_enriched_quotes
    get_quotes_batch = get_enriched_quotes_batch
    get_analysis_quotes_batch = get_enriched_quotes_batch
    quotes_batch = get_enriched_quotes_batch
    get_quote_dict = get_enriched_quote_dict

    # ------------------------------------------------------------------
    # builder fallbacks
    # ------------------------------------------------------------------
    async def _build_data_dictionary_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for sheet_name in _list_sheet_names_best_effort():
            spec, headers, keys, source = _schema_for_sheet(sheet_name)
            columns = _schema_columns_from_any(spec)
            for idx, (header, key) in enumerate(zip(headers, keys), start=1):
                col_meta = columns[idx - 1] if idx - 1 < len(columns) else {}
                if not isinstance(col_meta, Mapping):
                    col_meta = _model_to_dict(col_meta)
                dtype = _safe_str(col_meta.get("dtype") or col_meta.get("type") or col_meta.get("data_type") or "string")
                fmt = _safe_str(col_meta.get("format") or col_meta.get("fmt") or col_meta.get("number_format") or "")
                required = bool(col_meta.get("required")) if "required" in col_meta else idx <= 3
                source_hint = _safe_str(col_meta.get("source") or source)
                notes = _safe_str(col_meta.get("notes") or col_meta.get("description") or "static/registry contract")

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

                rows.append(
                    {
                        "sheet": sheet_name,
                        "group": group,
                        "header": header,
                        "key": key,
                        "dtype": dtype,
                        "fmt": fmt,
                        "required": required,
                        "source": source_hint,
                        "notes": notes,
                    }
                )
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
        quote_rows = [_model_to_dict(q) for q in quotes]
        quote_rows = [r for r in quote_rows if isinstance(r, dict)]
        quote_rows.sort(
            key=lambda r: (
                _as_float(r.get("opportunity_score")) or _as_float(r.get("overall_score")) or 0.0,
                _as_float(r.get("confidence_score")) or 0.0,
            ),
            reverse=True,
        )

        def _avg(values: List[Optional[float]]) -> Optional[float]:
            nums = [v for v in values if v is not None]
            return round(sum(nums) / len(nums), 4) if nums else None

        total = len(quote_rows)
        avg_overall = _avg([_as_float(r.get("overall_score")) for r in quote_rows])
        avg_roi_3m = _avg([_as_float(r.get("expected_roi_3m")) for r in quote_rows])

        risk_counts = {"LOW": 0, "MODERATE": 0, "HIGH": 0}
        for row in quote_rows:
            bucket = _safe_str(row.get("risk_bucket")).upper()
            if bucket in risk_counts:
                risk_counts[bucket] += 1

        rows: List[Dict[str, Any]] = []
        rows.append({"section": "Market Summary", "item": "Universe", "metric": "Symbols Analyzed",
                     "value": total, "notes": f"fallback summary from {len(symbols)} requested symbols",
                     "source": "engine_fallback", "sort_order": 1})
        rows.append({"section": "Market Summary", "item": "Universe", "metric": "Average Overall Score",
                     "value": avg_overall, "notes": "mean overall score across analyzed instruments",
                     "source": "engine_fallback", "sort_order": 2})
        rows.append({"section": "Market Summary", "item": "Universe", "metric": "Average Expected ROI 3M",
                     "value": avg_roi_3m, "notes": "fractional ROI where available",
                     "source": "engine_fallback", "sort_order": 3})

        sort_order = 10
        for bucket in ("LOW", "MODERATE", "HIGH"):
            rows.append({"section": "Risk Distribution", "item": bucket, "metric": "Count",
                         "value": risk_counts[bucket], "notes": "fallback risk bucket summary",
                         "source": "engine_fallback", "sort_order": sort_order})
            sort_order += 1

        top_quotes = quote_rows[: max(3, min(7, limit))]
        for idx, d in enumerate(top_quotes, start=1):
            rows.append({"section": "Top Ideas", "item": d.get("symbol"), "metric": "Recommendation",
                         "value": d.get("recommendation"),
                         "notes": d.get("recommendation_reason") or f"overall={d.get('overall_score')} opportunity={d.get('opportunity_score')}",
                         "source": "engine_fallback", "sort_order": 100 + idx})
            rows.append({"section": "Top Ideas", "item": d.get("symbol"), "metric": "Expected ROI 3M",
                         "value": d.get("expected_roi_3m"),
                         "notes": f"confidence={d.get('confidence_score')} risk={d.get('risk_bucket')}",
                         "source": "engine_fallback", "sort_order": 120 + idx})
            if _as_float(d.get("position_value")) is not None or _as_float(d.get("unrealized_pl")) is not None:
                rows.append({"section": "Portfolio Signals", "item": d.get("symbol"),
                             "metric": "Unrealized P/L", "value": d.get("unrealized_pl"),
                             "notes": f"value={d.get('position_value')} cost={d.get('position_cost')}",
                             "source": "engine_fallback", "sort_order": 140 + idx})

        return rows[:limit]

    def _top10_sort_key(self, row: Dict[str, Any]) -> Tuple[float, ...]:
        return (
            _as_float(row.get("opportunity_score")) or float("-inf"),
            _as_float(row.get("overall_score")) or float("-inf"),
            _as_float(row.get("confidence_score")) or float("-inf"),
            _as_float(row.get("expected_roi_3m")) or float("-inf"),
            _as_float(row.get("expected_roi_12m")) or float("-inf"),
            _as_float(row.get("value_score")) or float("-inf"),
            _as_float(row.get("quality_score")) or float("-inf"),
            _as_float(row.get("momentum_score")) or float("-inf"),
            _as_float(row.get("growth_score")) or float("-inf"),
            _as_float(row.get("current_price")) or float("-inf"),
        )

    async def _build_top10_rows_fallback(
        self,
        headers: Sequence[str],
        keys: Sequence[str],
        body: Optional[Dict[str, Any]],
        limit: int,
        mode: str = "",
    ) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        body = dict(body or {})
        criteria = dict(body.get("criteria") or {}) if isinstance(body.get("criteria"), dict) else {}
        out_headers, out_keys = _ensure_top10_contract(headers, keys)

        top_n = max(1, min(int(criteria.get("top_n") or body.get("top_n") or 10), max(1, limit)))
        requested_pages = _extract_top10_pages_from_body(body) or list(TOP10_ENGINE_DEFAULT_PAGES)
        requested_symbols = _extract_requested_symbols_from_body(body, limit=max(limit * 10, 200))

        for page_name in requested_pages:
            if len(requested_symbols) >= max(limit * 10, 200):
                break
            syms = await self.get_sheet_symbols(page_name, limit=max(limit * 2, 25), body=body)
            if syms:
                requested_symbols.extend(syms)

        if not requested_symbols:
            requested_symbols = list(EMERGENCY_PAGE_SYMBOLS.get("Top_10_Investments") or [])

        requested_symbols = _normalize_symbol_list(requested_symbols, limit=max(limit * 10, 200))
        if not requested_symbols:
            return out_headers, out_keys, []

        quotes = await self.get_enriched_quotes(requested_symbols, schema=None, page="Top_10_Investments", body=body)
        rows: List[Dict[str, Any]] = []
        for q in quotes:
            row = _model_to_dict(q)
            row = _apply_page_row_backfill("Top_10_Investments", row)
            _compute_scores_fallback(row)
            _compute_recommendation(row)
            rows.append(row)

        if not rows:
            return out_headers, out_keys, []

        _apply_rank_overall(rows)
        rows.sort(key=self._top10_sort_key, reverse=True)

        criteria_snapshot = _top10_criteria_snapshot({
            **criteria,
            "pages_selected": requested_pages,
            "direct_symbols": requested_symbols,
            "top_n": top_n,
        })

        selected: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        for row in rows:
            sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("ticker") or row.get("requested_symbol")))
            dedupe_key = sym or _safe_str(row.get("name")) or f"row_{len(selected)+1}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            row["top10_rank"] = len(selected) + 1
            row["selection_reason"] = row.get("selection_reason") or _top10_selection_reason(row)
            row["criteria_snapshot"] = row.get("criteria_snapshot") or criteria_snapshot
            projected = _normalize_to_schema_keys(out_keys, out_headers, row)
            projected["top10_rank"] = row["top10_rank"]
            projected["selection_reason"] = row["selection_reason"]
            projected["criteria_snapshot"] = row["criteria_snapshot"]
            selected.append(_strict_project_row(out_keys, projected))
            if len(selected) >= top_n:
                break

        return out_headers, out_keys, selected

    # ------------------------------------------------------------------
    # main sheet/page APIs
    # ------------------------------------------------------------------
    async def get_page_rows(
        self,
        page: Optional[str] = None,
        *,
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
        *,
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

    async def get_sheet_rows(
        self,
        sheet: Optional[str] = None,
        *,
        sheet_name: Optional[str] = None,
        page: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
        mode: str = "",
        body: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        target_sheet, limit, offset, mode, body, request_parts = _normalize_route_call_inputs(
            page=page, sheet=sheet, sheet_name=sheet_name,
            limit=limit, offset=offset, mode=mode, body=body, extras=kwargs,
        )
        include_matrix = _safe_bool(body.get("include_matrix"), True)

        spec, headers, keys, schema_src = _schema_for_sheet(target_sheet)
        headers, keys = _complete_schema_contract(headers, keys)

        if target_sheet == "Top_10_Investments" and self.top10_force_full_schema:
            static_contract = STATIC_CANONICAL_SHEET_CONTRACTS.get("Top_10_Investments", {})
            static_headers, static_keys = _complete_schema_contract(static_contract.get("headers", []), static_contract.get("keys", []))
            if len(static_keys) >= len(keys):
                headers, keys = _ensure_top10_contract(static_headers, static_keys)
                schema_src = f"{schema_src}|top10_force_full_schema"

        target_sheet_known = target_sheet in INSTRUMENT_SHEETS or target_sheet in SPECIAL_SHEETS or bool(spec)
        strict_req = bool(self.schema_strict_sheet_rows)
        contract_level = "canonical" if _usable_contract(headers, keys, target_sheet) else "partial"
        recovered_from: Optional[str] = None

        if target_sheet == "Data_Dictionary":
            if _is_schema_only_body(body):
                return self._finalize_payload(
                    sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                    include_matrix=include_matrix, status="success",
                    meta={"schema_source": schema_src, "contract_level": contract_level,
                          "strict_requested": strict_req, "strict_enforced": False,
                          "target_sheet_known": True, "builder": "schema_only_fast_path",
                          "rows": 0, "limit": limit, "offset": offset, "mode": mode},
                )
            rows_all = await self._build_data_dictionary_rows()
            rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows_all]
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_proj,
                include_matrix=include_matrix, status="success",
                meta={"schema_source": schema_src, "contract_level": contract_level,
                      "strict_requested": strict_req, "strict_enforced": False,
                      "target_sheet_known": True, "builder": "engine.internal_data_dictionary",
                      "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode},
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_page,
                include_matrix=include_matrix, status="success",
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        if target_sheet == "Insights_Analysis":
            if _is_schema_only_body(body):
                return self._finalize_payload(
                    sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                    include_matrix=include_matrix, status="success",
                    meta={"schema_source": schema_src, "contract_level": contract_level,
                          "strict_requested": strict_req, "strict_enforced": False,
                          "target_sheet_known": True, "builder": "schema_only_fast_path",
                          "rows": 0, "limit": limit, "offset": offset, "mode": mode},
                )

            rows0: List[Dict[str, Any]] = []
            builder_name = "core.analysis.insights_builder"
            try:
                from core.analysis.insights_builder import build_insights_analysis_rows  # type: ignore
                crit = body.get("criteria") if isinstance(body.get("criteria"), dict) else None
                universes = body.get("universes") if isinstance(body.get("universes"), dict) else None
                symbols = body.get("symbols") if isinstance(body.get("symbols"), list) else None
                payload = await build_insights_analysis_rows(
                    engine=self, criteria=crit, universes=universes,
                    symbols=symbols, mode=mode or "",
                )
                rows0 = _coerce_rows_list(payload)
            except Exception as exc:
                builder_name = f"fallback:insights_builder_failed:{type(exc).__name__}"
                rows0 = []

            if not rows0:
                rows0 = await self._build_insights_rows_fallback(body, limit=max(limit + offset, 10))
                builder_name = "fallback:engine_insights_rows"

            rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_proj,
                include_matrix=include_matrix,
                status="success" if rows_proj else "warn",
                meta={"schema_source": schema_src, "contract_level": contract_level,
                      "strict_requested": strict_req, "strict_enforced": False,
                      "target_sheet_known": True, "builder": builder_name,
                      "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode},
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_page,
                include_matrix=include_matrix, status=payload_full.get("status", "success"),
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        if target_sheet == "Top_10_Investments":
            top10_body, route_warnings = _normalize_top10_body_for_engine(body, limit=max(1, min(limit, 50)))
            if _is_schema_only_body(top10_body):
                headers, keys = _ensure_top10_contract(headers, keys)
                return self._finalize_payload(
                    sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                    include_matrix=include_matrix, status="success",
                    meta={"schema_source": schema_src, "contract_level": contract_level,
                          "strict_requested": strict_req, "strict_enforced": False,
                          "target_sheet_known": True, "builder": "schema_only_fast_path",
                          "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                          "warnings": route_warnings},
                )

            rows_proj: List[Dict[str, Any]] = []
            builder_used = "core.analysis.top10_selector"
            status_out = "success"

            try:
                from core.analysis.top10_selector import build_top10_rows  # type: ignore
                criteria = top10_body.get("criteria") if isinstance(top10_body.get("criteria"), dict) else None
                payload = await build_top10_rows(
                    engine=self, settings=self.settings,
                    criteria=criteria, body=dict(top10_body or {}),
                    limit=int(top10_body.get("limit") or top10_body.get("top_n") or min(limit, 10) or 10),
                    mode=mode or "",
                )
                rows0 = _coerce_rows_list(payload)
                if rows0:
                    rows_proj = [_strict_project_row(keys, _normalize_to_schema_keys(keys, headers, r)) for r in rows0]
                status_out = _safe_str(payload.get("status"), "success") if isinstance(payload, dict) else "success"
            except Exception as exc:
                builder_used = f"fallback:top10_selector_failed:{type(exc).__name__}"
                status_out = "warn"

            if not rows_proj:
                headers, keys, rows_proj = await self._build_top10_rows_fallback(headers, keys, top10_body, limit=max(limit + offset, 10), mode=mode)
                builder_used = "fallback:live_ranker"
                if rows_proj:
                    status_out = "warn"

            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_proj,
                include_matrix=include_matrix,
                status=status_out if rows_proj else "warn",
                meta={"schema_source": schema_src, "contract_level": contract_level,
                      "strict_requested": strict_req, "strict_enforced": False,
                      "target_sheet_known": True, "builder": builder_used,
                      "rows": len(rows_proj), "limit": limit, "offset": offset, "mode": mode,
                      "warnings": route_warnings},
            )
            if rows_proj:
                self._store_sheet_snapshot(target_sheet, payload_full)

            rows_page = rows_proj[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=rows_page,
                include_matrix=include_matrix, status=payload_full.get("status", "warn"),
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        if contract_level != "canonical":
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
            recovered = False
            if isinstance(cached_snap, dict):
                c_headers = cached_snap.get("headers") or cached_snap.get("display_headers")
                c_keys = cached_snap.get("keys") or cached_snap.get("fields")
                if c_headers or c_keys:
                    ch, ck = _complete_schema_contract(c_headers or [], c_keys or [])
                    if _usable_contract(ch, ck, target_sheet):
                        headers, keys = ch, ck
                        schema_src = "recovered_from_cache_contract"
                        contract_level = "recovered"
                        recovered_from = "cache_contract"
                        recovered = True

            if not recovered and target_sheet in STATIC_CANONICAL_SHEET_CONTRACTS:
                c = STATIC_CANONICAL_SHEET_CONTRACTS[target_sheet]
                headers, keys = _complete_schema_contract(c["headers"], c["keys"])
                schema_src = "static_canonical_contract_recovery"
                contract_level = "recovered"
                recovered_from = "static_contract"
                recovered = True

            if not recovered and target_sheet in INSTRUMENT_SHEETS:
                headers, keys = list(INSTRUMENT_CANONICAL_HEADERS), list(INSTRUMENT_CANONICAL_KEYS)
                schema_src = "fallback_union"
                contract_level = "union_fallback"
                recovered_from = "union_fallback"

        final_status = "success"
        schema_warning: Optional[str] = None
        if contract_level == "union_fallback" and target_sheet_known:
            final_status = "warn"
            schema_warning = "canonical_schema_unusable_used_union_schema"
        elif not target_sheet_known and not strict_req:
            final_status = "warn"
            schema_warning = "unknown_sheet_non_strict_mode"

        base_meta = {
            "schema_source": schema_src, "contract_level": contract_level,
            "strict_requested": strict_req, "strict_enforced": False,
            "target_sheet_known": target_sheet_known,
            "route_input_keys": sorted([str(k) for k in body.keys()]) if isinstance(body, dict) else [],
            "request_input_keys": sorted([str(k) for k in request_parts.keys()]) if isinstance(request_parts, dict) else [],
        }
        if recovered_from:
            base_meta["recovered_from"] = recovered_from
        if schema_warning:
            base_meta["schema_warning"] = schema_warning

        if _is_schema_only_body(body):
            return self._finalize_payload(
                sheet=target_sheet, headers=headers, keys=keys, row_objects=[],
                include_matrix=include_matrix, status=final_status,
                meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                      "built_from": "schema_only_fast_path"},
            )

        requested_symbols = _extract_requested_symbols_from_body(body, limit=limit + offset)
        built_from = "body_symbols" if requested_symbols else "live_quotes"
        if requested_symbols:
            self._set_sheet_symbols_meta(target_sheet, "body_symbols", len(requested_symbols))
        if not requested_symbols and target_sheet in INSTRUMENT_SHEETS:
            requested_symbols = await self.get_sheet_symbols(target_sheet, limit=limit + offset, body=body)
            built_from = self._get_sheet_symbols_meta(target_sheet).get("source") or ("auto_sheet_symbols" if requested_symbols else "empty")

        out_headers = list(headers)
        out_keys = list(keys)

        if target_sheet in INSTRUMENT_SHEETS:
            ext_rows = await self._get_rows_from_external_reader(target_sheet, limit + offset)
            if ext_rows:
                enriched_rows: List[Dict[str, Any]] = []
                symbols = _extract_symbols_from_rows(ext_rows, limit=limit + offset)
                quote_map: Dict[str, Dict[str, Any]] = {}

                if self.rows_hydrate_external and symbols:
                    for q in await self.get_enriched_quotes(symbols, schema=None, page=target_sheet, body=body):
                        d = _model_to_dict(q)
                        sym = normalize_symbol(_safe_str(d.get("symbol")))
                        if sym:
                            quote_map[sym] = d

                snapshot_map = self._get_cached_snapshot_symbol_map(target_sheet)

                for row in ext_rows:
                    merged = dict(row)
                    sym = normalize_symbol(self._extract_row_symbol(row))
                    if sym and sym in snapshot_map:
                        merged = _merge_missing_fields(merged, snapshot_map[sym])
                    best_snapshot_row = self._get_best_cached_snapshot_row_for_symbol(sym, prefer_sheet=target_sheet) if sym else None
                    if best_snapshot_row:
                        merged = _merge_missing_fields(merged, best_snapshot_row)
                    if sym and sym in quote_map:
                        merged = _merge_missing_fields(merged, quote_map[sym])
                    merged = _apply_page_row_backfill(target_sheet, merged)
                    _compute_scores_fallback(merged)
                    _compute_recommendation(merged)
                    enriched_rows.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, merged)))

                _apply_rank_overall(enriched_rows)

                payload_full = self._finalize_payload(
                    sheet=target_sheet, headers=out_headers, keys=out_keys,
                    row_objects=enriched_rows, include_matrix=include_matrix, status=final_status,
                    meta={**base_meta, "rows": len(enriched_rows), "limit": limit, "offset": offset, "mode": mode,
                          "built_from": "external_rows_reader",
                          "rows_reader_source": self._rows_reader_source,
                          "symbols_reader_source": self._symbols_reader_source,
                          "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)},
                )
                self._store_sheet_snapshot(target_sheet, payload_full)
                rows_page = enriched_rows[offset : offset + limit]
                return self._finalize_payload(
                    sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows_page,
                    include_matrix=include_matrix, status=final_status,
                    meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
                )

        if not requested_symbols:
            cached_snap = self.get_cached_sheet_snapshot(sheet=target_sheet)
            cached_rows = _coerce_rows_list(cached_snap)
            if cached_rows:
                proj_rows = [_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)) for row in cached_rows]
                rows_page = proj_rows[offset : offset + limit]
                return self._finalize_payload(
                    sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows_page,
                    include_matrix=include_matrix, status=final_status,
                    meta={**base_meta, "rows": len(rows_page), "limit": limit, "offset": offset, "mode": mode,
                          "built_from": "cached_snapshot",
                          "symbols_reader_source": self._symbols_reader_source,
                          "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)},
                )

        rows_full: List[Dict[str, Any]] = []
        if requested_symbols:
            snapshot_map = self._get_cached_snapshot_symbol_map(target_sheet)
            quotes = await self.get_enriched_quotes(requested_symbols, schema=None, page=target_sheet, body=body)
            for q in quotes:
                row = _model_to_dict(q)
                sym = normalize_symbol(_safe_str(row.get("symbol") or row.get("requested_symbol")))
                if sym and sym in snapshot_map:
                    row = _merge_missing_fields(row, snapshot_map[sym])
                best_snapshot_row = self._get_best_cached_snapshot_row_for_symbol(sym, prefer_sheet=target_sheet) if sym else None
                if best_snapshot_row:
                    row = _merge_missing_fields(row, best_snapshot_row)
                row = _apply_page_row_backfill(target_sheet, row)
                _compute_scores_fallback(row)
                _compute_recommendation(row)
                rows_full.append(_strict_project_row(out_keys, _normalize_to_schema_keys(out_keys, out_headers, row)))
            _apply_rank_overall(rows_full)

        if rows_full:
            payload_full = self._finalize_payload(
                sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows_full,
                include_matrix=include_matrix, status=final_status,
                meta={**base_meta, "rows": len(rows_full), "limit": limit, "offset": offset, "mode": mode,
                      "built_from": built_from,
                      "resolved_symbols_count": len(requested_symbols),
                      "symbols_reader_source": self._symbols_reader_source,
                      "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)},
            )
            self._store_sheet_snapshot(target_sheet, payload_full)
            rows_page = rows_full[offset : offset + limit]
            return self._finalize_payload(
                sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=rows_page,
                include_matrix=include_matrix, status=final_status,
                meta={**payload_full.get("meta", {}), "rows": len(rows_page)},
            )

        return self._finalize_payload(
            sheet=target_sheet, headers=out_headers, keys=out_keys, row_objects=[],
            include_matrix=include_matrix, status=final_status,
            meta={**base_meta, "rows": 0, "limit": limit, "offset": offset, "mode": mode,
                  "built_from": built_from, "resolved_symbols_count": len(requested_symbols),
                  "symbols_reader_source": self._symbols_reader_source,
                  "symbol_resolution_meta": self._get_sheet_symbols_meta(target_sheet)},
        )

    async def sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    async def build_sheet_rows(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self.get_sheet_rows(*args, **kwargs)

    def get_sheet_contract(self, sheet: str) -> Dict[str, Any]:
        target = _canonicalize_sheet_name(sheet) or sheet or "Market_Leaders"
        _spec, headers, keys, source = _schema_for_sheet(target)
        if target == "Top_10_Investments":
            headers, keys = _ensure_top10_contract(headers, keys)
        return {
            "sheet": target, "page": target, "sheet_name": target,
            "headers": headers, "display_headers": headers,
            "keys": keys, "fields": keys, "source": source, "count": len(keys),
        }

    def get_page_contract(self, page: str) -> Dict[str, Any]:
        return self.get_sheet_contract(page)

    def get_sheet_schema(self, sheet: str) -> Dict[str, Any]:
        return self.get_sheet_contract(sheet)

    def get_page_schema(self, page: str) -> Dict[str, Any]:
        return self.get_sheet_contract(page)

    def get_headers_for_sheet(self, sheet: str) -> List[str]:
        return list(self.get_sheet_contract(sheet).get("headers") or [])

    def get_keys_for_sheet(self, sheet: str) -> List[str]:
        return list(self.get_sheet_contract(sheet).get("keys") or [])

    # ------------------------------------------------------------------
    # health / stats
    # ------------------------------------------------------------------
    async def health(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "version": self.version,
            "schema_available": True,
            "static_contract_sheets": sorted(list(STATIC_CANONICAL_SHEET_CONTRACTS.keys())),
            "snapshot_sheets": len(self._sheet_snapshots),
            "rows_reader_source": self._rows_reader_source,
            "symbols_reader_source": self._symbols_reader_source,
            "yahoo_enrichment_pass": {
                "enabled": _yahoo_enrichment_enabled(),
                "enrich_on_missing_industry": _yahoo_enrich_on_missing_industry(),
                "enrich_on_missing_risk_metrics": _yahoo_enrich_on_missing_risk_metrics(),
                "fundamental_fields_chased": list(_YAHOO_FUNDAMENTAL_FIELDS),
                "chart_fields_chased": list(_YAHOO_CHART_FIELDS),
                "last_pass": dict(_YAHOO_ENRICHMENT_LAST_PASS),
            },
        }

    async def get_health(self) -> Dict[str, Any]:
        return await self.health()

    async def health_check(self) -> Dict[str, Any]:
        return await self.health()

    async def get_stats(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "primary_provider": self.primary_provider,
            "enabled_providers": list(self.enabled_providers),
            "ksa_providers": list(self.ksa_providers),
            "global_providers": list(self.global_providers),
            "non_ksa_primary_provider": self.non_ksa_primary_provider,
            "page_primary_providers": dict(self.page_primary_providers),
            "history_fallback_providers": list(self.history_fallback_providers),
            "ksa_disallow_eodhd": bool(self.ksa_disallow_eodhd),
            "flags": dict(self.flags),
            "provider_stats": await self._registry.get_stats(),
            "schema_available": True,
            "schema_strict_sheet_rows": bool(self.schema_strict_sheet_rows),
            "top10_force_full_schema": bool(self.top10_force_full_schema),
            "rows_hydrate_external": bool(self.rows_hydrate_external),
            "symbols_reader_source": self._symbols_reader_source,
            "rows_reader_source": self._rows_reader_source,
            "snapshot_sheets": sorted(list(self._sheet_snapshots.keys())),
            "sheet_symbol_resolution_meta": dict(self._sheet_symbol_resolution_meta),
        }


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


DataEngineV4 = DataEngineV5
DataEngineV3 = DataEngineV5
DataEngineV2 = DataEngineV5
DataEngine = DataEngineV5

__all__ = [
    "DataEngineV5", "DataEngineV4", "DataEngineV3", "DataEngineV2", "DataEngine",
    "ENGINE", "engine", "_ENGINE",
    "get_engine", "get_engine_if_ready", "peek_engine", "close_engine", "get_cache",
    "QuoteQuality", "DataSource", "UnifiedQuote",
    "__version__",
    "STATIC_CANONICAL_SHEET_CONTRACTS",
    "get_sheet_spec", "normalize_row_to_schema",
]
