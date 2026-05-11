#!/usr/bin/env python3
"""
core/analysis/criteria_model.py
================================================================================
Advisor Criteria Model -- v3.1.0
(CROSS-STACK CONVICTION + DATA-QUALITY CRITERIA, V2-ONLY, SCENARIO-TABLE)
================================================================================
Tadawul Fast Bridge (TFB)

Purpose
-------
Single validated source-of-truth for advisor criteria embedded in the
Insights_Analysis top block (key/value rows) and shared by
Top_10_Investments.

================================================================================
v3.1.0 Changes (from v3.0.0)  --  CROSS-STACK CONVICTION + DATA-QUALITY
================================================================================

Aligns criteria_model with the May 2026 cross-stack revisions:

  - core.scoring v5.2.5 produces `conviction_score` per row (alongside
    top_factors, top_risks, position_size_hint).
  - core.reco_normalize v7.2.0 introduces env-tunable conviction floors
    (RECO_STRONG_BUY_CONVICTION_FLOOR default 60, RECO_BUY_CONVICTION_FLOOR
    default 45) used by the view-aware classification logic to downgrade
    STRONG_BUY -> BUY and BUY -> HOLD when conviction falls below the
    floor.
  - core.data_engine_v2 v5.60.0 emits 5 engine-dropped valuation tags
    (intrinsic_unit_mismatch_suspected, upside_synthesis_suspect,
    engine_52w_high_unit_mismatch_dropped, engine_52w_low_unit_mismatch_dropped,
    engine_52w_high_low_inverted) and 4 forecast-unavailable tags + bool
    flag (forecast_unavailable, forecast_unavailable_no_source,
    forecast_cleared_consistency_sweep, forecast_skipped_unavailable),
    plus preserves provider `last_error_class` (Phase Q).
  - core.analysis.top10_selector v4.11.0 applies data-quality penalties.
  - core.analysis.insights_builder v7.0.0 surfaces all of these in the
    Top Picks + NEW Data Quality Alerts section.

All v3.1.0 changes are ADDITIVE WITH DEFAULTS -- no field removals, no
validator changes for existing fields, no breaking API changes. Existing
callers see identical behaviour.

  A. NEW field `min_conviction_score` (float, 0-100, default 0.0 = no
     filter). Lets callers express "I only want high-conviction picks"
     without importing reco_normalize's floor mechanics directly. The
     validator handles fraction shape (0.7 -> 70.0) consistent with
     max_risk_score's preserved behaviour.

  B. NEW exclusion bool fields for upstream data quality issues:
       exclude_engine_dropped_valuation -- drop rows where engine cleared
                                            intrinsic_value upstream
       exclude_forecast_unavailable     -- drop rows without forecast
       exclude_provider_errors          -- drop rows where last_error_class
                                            is non-empty
     All default False (opt-in filtering; preserves v3.0.0 semantics).

  C. _SCENARIO_PRESETS extended with conviction floors + data quality
     exclusions per scenario level:
       Conservative: min_conviction=70, exclude all 3 data-quality flags
       Moderate:     min_conviction=50, exclude only forecast_unavailable
       Aggressive:   min_conviction=30, no exclusions (high opportunity)
     Defaults chosen to align with reco_normalize v7.2.0's view-aware
     conviction ladder (60/45 floors for STRONG_BUY/BUY downgrades).

  D. `ScenarioSpec` (frozen dataclass) gained four new fields with
     defaults:
       min_conviction          : float = 0.0
       exclude_engine_drops    : bool = False
       exclude_forecast_unavail: bool = False
       exclude_provider_errors : bool = False
     Notes string updated to mention conviction floor and exclusion
     policy when set. Existing callers that read only label / signal /
     notes are unaffected.

  E. NEW env-tunable conviction floor helpers (mirror reco_normalize
     v7.2.0):
       get_strong_buy_conviction_floor()
           -- reads RECO_STRONG_BUY_CONVICTION_FLOOR (default 60.0)
       get_buy_conviction_floor()
           -- reads RECO_BUY_CONVICTION_FLOOR (default 45.0)
     These let callers (e.g. UI components, dashboards, audit reports)
     read the SAME floor reco_normalize uses without importing
     reco_normalize directly. Single source of truth for the env var
     names + defaults.

  F. NEW `__version__ = CRITERIA_MODEL_VERSION` alias (TFB module
     convention used by scoring v5.2.5, reco_normalize v7.2.0,
     insights_builder v7.0.0, scoring_engine v3.4.2, top10_selector
     v4.11.0).

  G. KV map factory recognises new field labels via fuzzy aliases:
       min_conviction_score: "min conviction", "conviction floor",
                             "minimum conviction"
       exclude_engine_dropped_valuation: "exclude engine drops"
       exclude_forecast_unavailable: "exclude forecast unavailable",
                                     "exclude forecast na"
       exclude_provider_errors: "exclude provider errors"

  H. __all__ augmented with __version__, get_strong_buy_conviction_floor,
     get_buy_conviction_floor.

  I. Version bump 3.0.0 -> 3.1.0.

Wiring note (out-of-scope for v3.1.0): top10_selector._passes_filters
and insights_builder section builders still need a follow-up patch to
actively consume the new fields. v3.1.0 is the data-model foundation
that makes them AVAILABLE.

================================================================================
v3.0.0 Changes (preserved)  --  BUG-FIX, V2-ONLY, SCENARIO-TABLE
================================================================================

BREAKING (v3.0.0):
  - Pydantic v1 is no longer supported. Requires pydantic>=2.0.

Bug fixes (v3.0.0):
  - max_risk_score=0 no longer gets replaced with the default 60.0.
  - SignalMapper rule registration is now thread-safe.
  - `top10_enabled`-as-integer no longer silently overrides an explicit
    `top_n` value from the same payload.
  - `validate_assignment=True` removed from ConfigDict.

Cleanup (v3.0.0):
  - Removed meaningless `ClassVar[...]` annotations.
  - Removed dead constant HORIZON_LABELS.
  - Merged `_normalize_pages` (free fn) and `PageHelper` (class).
  - Replaced three hardcoded blocks in `to_scenario_variants` with a
    single table (_SCENARIO_PRESETS).
  - Exception classes now actually get raised when `strict=True`.

================================================================================
Design Principles (unchanged)
================================================================================

- No startup network I/O
- Safe to import on Render
- Lazy page-catalog loading
- Fail gracefully with defaults (unless strict=True)
- Log warnings, don't crash
================================================================================
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pydantic v2 (required)
# ---------------------------------------------------------------------------

try:
    from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
except ImportError as _exc:  # pragma: no cover
    raise ImportError(
        "core.analysis.criteria_model requires pydantic>=2.0. "
        "Install with: pip install 'pydantic>=2.0,<3.0'"
    ) from _exc

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CRITERIA_MODEL_VERSION = "3.1.0"
# v3.1.0 Phase F: TFB module-version convention alias.
__version__ = CRITERIA_MODEL_VERSION

# Valid signal values for Insights_Analysis Signal column
SIGNAL_VALUES: FrozenSet[str] = frozenset({
    "UP", "DOWN", "NEUTRAL",
    "HIGH", "MODERATE", "LOW",
    "OK", "WARN", "ALERT",
    "STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL",
})

# Scenario labels for risk scenarios section
SCENARIO_LABELS: Tuple[str, ...] = ("Conservative", "Moderate", "Aggressive")


# ---------------------------------------------------------------------------
# v3.1.0 Phase E — Env-tunable conviction floor helpers
# ---------------------------------------------------------------------------
#
# Mirror constants here so callers (UI components, dashboards, audit
# reports, top10_selector when it advances) can read the SAME floor
# reco_normalize uses without importing reco_normalize directly. Single
# source of truth for the env var names + defaults.

def _env_float(name: str, default: float) -> float:
    """Read float env var defensively; falls back to default on any error."""
    try:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            return default
        value = float(raw)
        # NaN and infinity are not valid floors.
        if value != value or value in (float("inf"), float("-inf")):
            return default
        return value
    except (ValueError, TypeError):
        return default


def get_strong_buy_conviction_floor() -> float:
    """
    Return the canonical STRONG_BUY conviction floor.

    Below this value, reco_normalize v7.2.0 view-aware classification
    will downgrade STRONG_BUY -> BUY (or further down depending on the
    rule chain). Mirrors reco_normalize's env knob so callers can read
    the floor without importing reco_normalize.

    Env: RECO_STRONG_BUY_CONVICTION_FLOOR (default 60.0)
    """
    return _env_float("RECO_STRONG_BUY_CONVICTION_FLOOR", 60.0)


def get_buy_conviction_floor() -> float:
    """
    Return the canonical BUY conviction floor.

    Below this value, reco_normalize v7.2.0 view-aware classification
    will downgrade BUY -> HOLD. Mirrors reco_normalize's env knob.

    Env: RECO_BUY_CONVICTION_FLOOR (default 45.0)
    """
    return _env_float("RECO_BUY_CONVICTION_FLOOR", 45.0)


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class CriteriaModelError(Exception):
    """Base exception for criteria model errors."""


class CriteriaParseError(CriteriaModelError):
    """Raised when parsing criteria from malformed input (with strict=True)."""


class CriteriaValidationError(CriteriaModelError):
    """Raised when criteria validation fails (with strict=True)."""


# ---------------------------------------------------------------------------
# Helper Functions (Pure, Testable)
# ---------------------------------------------------------------------------

def _to_string(value: Any) -> str:
    """Safely convert any value to string, handling None."""
    if value is None:
        return ""
    if isinstance(value, (int, float, bool)):
        return str(value)
    try:
        return str(value).strip()
    except Exception:
        return ""


def _is_blank(value: Any) -> bool:
    """Check if value is None, empty string, or whitespace-only."""
    if value is None:
        return True
    s = _to_string(value)
    return not s or s.isspace()


def _to_bool(value: Any, default: bool = False) -> bool:
    """Convert various representations to boolean."""
    if isinstance(value, bool):
        return value
    s = _to_string(value).lower()
    if not s:
        return default
    truthy = {"1", "true", "yes", "y", "on", "t", "enabled", "enable", "active"}
    falsy = {"0", "false", "no", "n", "off", "f", "disabled", "disable", "inactive"}
    if s in truthy:
        return True
    if s in falsy:
        return False
    return default


def _to_int(value: Any, default: int = 0) -> int:
    """Convert value to int safely."""
    if isinstance(value, bool):
        return default
    try:
        s = _to_string(value).replace(",", "").replace(" ", "")
        if not s:
            return default
        return int(float(s))  # handles "90.0"
    except (ValueError, TypeError):
        return default


def _to_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """
    Convert value to float safely.

    Returns `default` (which may be None) for unparseable input. Handles
    percentage strings like "10%" by dividing by 100.
    """
    if value is None or isinstance(value, bool):
        return default
    try:
        s = _to_string(value).replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            return float(s[:-1].strip()) / 100.0
        return float(s)
    except (ValueError, TypeError):
        return default


def _as_ratio(value: Any, default: float = 0.0) -> float:
    """
    Convert any percent-like value to a ratio (0.12 = 12%).

    Accepts: 0.12, 12, "12%", "0.12". Values with abs > 1.5 are treated
    as percentages and divided by 100.
    """
    f = _to_float(value, None)
    if f is None:
        return default
    if abs(f) > 1.5:
        return f / 100.0
    return f


def _clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp a value between min and max."""
    return max(min_val, min(value, max_val))


# Explicit bucket canonicalization -- avoids .title() quirks for multi-word labels.
_BUCKET_CANONICAL: Mapping[str, str] = {
    "very low": "Very Low",
    "low": "Low",
    "moderate": "Moderate",
    "high": "High",
    "very high": "Very High",
}

_BUCKET_ALIASES: Mapping[str, str] = {
    "vl": "very low",
    "l": "low",
    "m": "moderate",
    "mid": "moderate",
    "med": "moderate",
    "medium": "moderate",
    "balanced": "moderate",
    "h": "high",
    "vh": "very high",
    "aggressive": "high",
    "conservative": "low",
}


def _normalize_bucket(value: Any) -> str:
    """
    Normalize bucket labels to canonical form.

    Examples:
        "VL" -> "Very Low"
        "very low risk" -> "Very Low"
        "mod" -> "Moderate"
        "high risk" -> "High"
    """
    s = _to_string(value).strip().lower()
    if not s:
        return ""

    for suffix in (" risk", " confidence", " bucket"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]

    for sep in ("_", "-", "|", ","):
        s = s.replace(sep, " ")

    s = " ".join(s.split())  # collapse whitespace

    # Resolve alias (if any), then canonicalize
    resolved = _BUCKET_ALIASES.get(s, s)
    return _BUCKET_CANONICAL.get(resolved, resolved.title())


# ---------------------------------------------------------------------------
# Enums for Type Safety
# ---------------------------------------------------------------------------

class RiskLevel(str, Enum):
    """Standardized risk level buckets."""
    VERY_LOW = "Very Low"
    LOW = "Low"
    MODERATE = "Moderate"
    HIGH = "High"
    VERY_HIGH = "Very High"

    @classmethod
    def from_string(cls, value: Any) -> "RiskLevel":
        """Parse risk level from string with fuzzy matching; defaults to MODERATE."""
        normalized = _normalize_bucket(value)
        for member in cls:
            if member.value.lower() == normalized.lower():
                return member
        return cls.MODERATE


class ConfidenceLevel(str, Enum):
    """Standardized confidence level buckets."""
    VERY_LOW = "Very Low"
    LOW = "Low"
    MODERATE = "Moderate"
    HIGH = "High"
    VERY_HIGH = "Very High"

    @classmethod
    def from_string(cls, value: Any) -> "ConfidenceLevel":
        """Parse confidence level from string with fuzzy matching; defaults to HIGH."""
        normalized = _normalize_bucket(value)
        for member in cls:
            if member.value.lower() == normalized.lower():
                return member
        return cls.HIGH


class Horizon(str, Enum):
    """Investment horizon labels."""
    MONTH_1 = "1M"
    MONTH_3 = "3M"
    MONTH_12 = "12M"

    @classmethod
    def from_days(cls, days: int) -> "Horizon":
        """Convert days to horizon enum (defensive: clamps negatives to 1)."""
        days = max(1, int(days) if days else 1)
        if days <= 45:
            return cls.MONTH_1
        if days <= 120:
            return cls.MONTH_3
        return cls.MONTH_12

    @property
    def expected_roi_key(self) -> str:
        return _HORIZON_ROI_KEYS[self.value]

    @property
    def forecast_price_key(self) -> str:
        return _HORIZON_PRICE_KEYS[self.value]


_HORIZON_ROI_KEYS: Mapping[str, str] = {
    "1M": "expected_roi_1m",
    "3M": "expected_roi_3m",
    "12M": "expected_roi_12m",
}

_HORIZON_PRICE_KEYS: Mapping[str, str] = {
    "1M": "forecast_price_1m",
    "3M": "forecast_price_3m",
    "12M": "forecast_price_12m",
}


# ---------------------------------------------------------------------------
# Page Catalog Helper (consolidated: single lazy path)
# ---------------------------------------------------------------------------

class PageHelper:
    """Lazy-loading helper for page catalog operations."""

    _has_catalog: Optional[bool] = None
    _normalize_fn: Optional[Callable[..., str]] = None
    _canonical_pages: Optional[Set[str]] = None

    @classmethod
    def _ensure_loaded(cls) -> None:
        if cls._has_catalog is not None:
            return
        try:
            from core.sheets.page_catalog import (  # type: ignore
                CANONICAL_PAGES,
                normalize_page_name,
            )
            cls._normalize_fn = normalize_page_name
            cls._canonical_pages = set(CANONICAL_PAGES)
            cls._has_catalog = True
        except ImportError:
            cls._has_catalog = False

    @classmethod
    def normalize_page(cls, page_name: str, allow_output_pages: bool = True) -> str:
        cls._ensure_loaded()
        if cls._has_catalog and cls._normalize_fn:
            try:
                return cls._normalize_fn(page_name, allow_output_pages=allow_output_pages)
            except Exception as exc:
                logger.debug("Failed to normalize page name %r: %s", page_name, exc)
        return page_name

    @classmethod
    def get_canonical_pages(cls) -> Set[str]:
        cls._ensure_loaded()
        return set(cls._canonical_pages) if cls._canonical_pages else set()

    @classmethod
    def normalize_many(cls, pages: Sequence[str]) -> List[str]:
        """Normalize + dedupe a sequence of page names, preserving order."""
        out: List[str] = []
        seen: Set[str] = set()
        for raw in pages:
            name = _to_string(raw)
            if not name:
                continue
            normalized = cls.normalize_page(name, allow_output_pages=True) or name
            if normalized not in seen:
                seen.add(normalized)
                out.append(normalized)
        return out


def _split_pages(value: Any) -> List[str]:
    """Split page selection string into list of page names."""
    if value is None:
        return []
    if isinstance(value, list):
        return [p for p in (_to_string(i) for i in value) if p]
    s = _to_string(value)
    if not s:
        return []
    if s.strip().upper() == "ALL":
        return ["ALL"]
    return [p.strip() for p in re.split(r"[,;|\s]+", s) if p.strip()]


def _resolve_pages(raw: Sequence[str]) -> List[str]:
    """Resolve parsed pages -- expand ALL, normalize via catalog, dedupe."""
    if not raw:
        return []
    if any(_to_string(p).upper() == "ALL" for p in raw):
        canonical = PageHelper.get_canonical_pages()
        if canonical:
            return sorted(p for p in canonical if p != "Data_Dictionary")
        return ["ALL"]
    return PageHelper.normalize_many(raw)


# ---------------------------------------------------------------------------
# Signal Mapping (module-level constants -- thread-safe by construction)
# ---------------------------------------------------------------------------

_RECO_MAP: Mapping[str, str] = {
    "STRONG_BUY": "STRONG_BUY",
    "BUY": "BUY",
    "HOLD": "HOLD",
    "NEUTRAL": "NEUTRAL",
    "REDUCE": "REDUCE",
    "SELL": "SELL",
    "ACCUMULATE": "BUY",
    "AVOID": "SELL",
}

_BUCKET_SIGNAL_MAP: Mapping[str, str] = {
    "HIGH": "HIGH", "H": "HIGH", "VERY_HIGH": "HIGH",
    "MODERATE": "MODERATE", "M": "MODERATE", "MEDIUM": "MODERATE",
    "LOW": "LOW", "L": "LOW", "VERY_LOW": "LOW",
}


def _numeric_signal(value: float, metric: str, th_high: float, th_low: float) -> str:
    """Convert a numeric value to a signal based on metric semantics."""
    metric_l = metric.lower()

    # Risk scores: higher = worse
    if "risk" in metric_l:
        if value >= th_high:
            return "HIGH"
        if value >= th_low:
            return "MODERATE"
        return "LOW"

    # ROI / returns: positive = UP, negative = DOWN
    if any(k in metric_l for k in ("roi", "return", "change", "pl", "p/l")):
        if value > 0.02:
            return "UP"
        if value < -0.02:
            return "DOWN"
        return "NEUTRAL"

    # Scores / confidence / quality / momentum / conviction: higher = better
    if any(k in metric_l for k in ("score", "confidence", "quality", "momentum", "conviction")):
        if value >= th_high:
            return "HIGH"
        if value >= th_low:
            return "MODERATE"
        return "LOW"

    # Generic numeric
    if value >= th_high:
        return "HIGH"
    if value < th_low:
        return "LOW"
    return "MODERATE"


def signal_for_value(
    value: Any,
    *,
    metric: str = "",
    threshold_high: float = 70.0,
    threshold_low: float = 30.0,
) -> str:
    """
    Map a value to the Signal column vocabulary.

    Resolution order:
      1. Already-valid signal token (passthrough)
      2. Recommendation alias (BUY / ACCUMULATE / AVOID / ...)
      3. Bucket alias (HIGH / MEDIUM / LOW / VL / VH / ...)
      4. Numeric rule (depends on `metric` for semantics)

    Examples:
        >>> signal_for_value(85, metric="confidence")
        'HIGH'
        >>> signal_for_value(-0.05, metric="return")
        'DOWN'
        >>> signal_for_value("BUY")
        'BUY'
        >>> signal_for_value(75, metric="conviction_score")  # v3.1.0
        'HIGH'
    """
    # 1. Exact signal passthrough
    s = _to_string(value).upper().replace(" ", "_").replace("-", "_")
    if s in SIGNAL_VALUES:
        return s

    # 2. Recommendation mapping
    if s in _RECO_MAP:
        return _RECO_MAP[s]

    # 3. Bucket mapping
    if s in _BUCKET_SIGNAL_MAP:
        return _BUCKET_SIGNAL_MAP[s]

    # 4. Numeric rule
    f = _to_float(value, None)
    if f is not None:
        result = _numeric_signal(f, metric, threshold_high, threshold_low)
        if result in SIGNAL_VALUES:
            return result

    return ""


class SignalMapper:
    """Backward-compatible class wrapper around `signal_for_value`."""

    @staticmethod
    def map(
        value: Any,
        metric: str = "",
        threshold_high: float = 70.0,
        threshold_low: float = 30.0,
    ) -> str:
        return signal_for_value(
            value,
            metric=metric,
            threshold_high=threshold_high,
            threshold_low=threshold_low,
        )


# ---------------------------------------------------------------------------
# AdvisorCriteria Model (Pydantic v2)
# ---------------------------------------------------------------------------

class AdvisorCriteria(BaseModel):
    """
    Advisor criteria read from the Insights_Analysis top block.

    All ratio fields (required_return_pct, min_expected_roi_pct,
    min_ai_confidence) are stored as fractions (0.12 = 12%).
    max_risk_score and min_conviction_score are stored on a 0-100 scale.

    Examples:
        >>> criteria = AdvisorCriteria(
        ...     risk_level="Moderate",
        ...     invest_period_days=90,
        ...     required_return_pct=0.10,
        ...     min_conviction_score=60.0,             # v3.1.0
        ...     exclude_forecast_unavailable=True,     # v3.1.0
        ... )
        >>> criteria.horizon
        '3M'
        >>> criteria.includes_page("Market_Leaders")
        True
    """

    # --- Buckets ---
    risk_level: str = Field(
        default="Moderate",
        description="Risk tolerance bucket: Very Low / Low / Moderate / High / Very High",
    )
    confidence_level: str = Field(
        default="High",
        description="Required confidence bucket: Very Low / Low / Moderate / High / Very High",
    )

    # --- Period (always DAYS) ---
    invest_period_days: int = Field(
        default=90,
        ge=1,
        le=3650,
        description="Investment period in days. Mapped to 1M/3M/12M horizons.",
    )

    # --- Return thresholds (fractions) ---
    required_return_pct: float = Field(
        default=0.10,
        ge=-1.0,
        le=10.0,
        description="Required minimum return (fraction: 0.10 = 10%)",
    )
    min_expected_roi_pct: float = Field(
        default=0.0,
        ge=-1.0,
        le=10.0,
        description="Minimum expected ROI filter (fraction)",
    )
    min_ai_confidence: float = Field(
        default=0.60,
        ge=0.0,
        le=1.0,
        description="Minimum AI forecast confidence (0-1 fraction)",
    )

    # --- Risk threshold (score 0-100) ---
    max_risk_score: float = Field(
        default=60.0,
        ge=0.0,
        le=100.0,
        description="Maximum allowed risk score (0-100)",
    )

    # --- v3.1.0 NEW: Conviction floor (score 0-100) ---
    min_conviction_score: float = Field(
        default=0.0,
        ge=0.0,
        le=100.0,
        description=(
            "Minimum conviction score filter (0-100, default 0 = no filter). "
            "Aligns with reco_normalize v7.2.0 conviction floors (60/45 for "
            "STRONG_BUY/BUY downgrades) and scoring v5.2.5 conviction_score "
            "row field. Set to 60 to mirror reco_normalize STRONG_BUY floor."
        ),
    )

    # --- Capital ---
    amount: float = Field(
        default=0.0,
        ge=0.0,
        description="Investment capital amount (currency units)",
    )

    # --- Page selection ---
    pages_selected: List[str] = Field(
        default_factory=lambda: ["ALL"],
        description="Pages to include. 'ALL' = all canonical pages.",
    )

    # --- Top N ---
    top_n: int = Field(
        default=10,
        ge=1,
        le=200,
        description="Top-N selection count",
    )
    top10_enabled: bool = Field(
        default=True,
        description="Enable Top_10_Investments page generation",
    )

    # --- Section flags ---
    include_market_summary: bool = Field(default=True)
    include_risk_scenarios: bool = Field(default=True)
    include_top_opportunities: bool = Field(default=True)
    include_portfolio_health: bool = Field(default=True)

    # --- v3.1.0 NEW: Data quality exclusion flags ---
    # All default False (preserves v3.0.0 semantics; opt-in filtering).
    # Map to engine v5.60.0 / scoring v5.2.5 / insights_builder v7.0.0
    # tag detection logic.
    exclude_engine_dropped_valuation: bool = Field(
        default=False,
        description=(
            "Drop rows where the engine cleared intrinsic_value or "
            "upside_pct upstream (5 tags from data_engine_v2 v5.60.0 "
            "Phase H/I/P: intrinsic_unit_mismatch_suspected, "
            "upside_synthesis_suspect, engine_52w_*_dropped, "
            "engine_52w_high_low_inverted)."
        ),
    )
    exclude_forecast_unavailable: bool = Field(
        default=False,
        description=(
            "Drop rows where forecast synthesis was skipped (4 tags from "
            "data_engine_v2 v5.60.0 Phase B: forecast_unavailable, "
            "forecast_unavailable_no_source, forecast_cleared_consistency_sweep, "
            "forecast_skipped_unavailable; OR forecast_unavailable bool flag)."
        ),
    )
    exclude_provider_errors: bool = Field(
        default=False,
        description=(
            "Drop rows where the provider's last_error_class is non-empty "
            "(preserved by data_engine_v2 v5.60.0 Phase Q from eodhd "
            "v4.7.3, yahoo_fundamentals v6.1.0, yahoo_chart v8.2.0, "
            "enriched_quote v4.3.0). Null-string filter applies "
            "('None'/'null'/'nan'/'n/a' counted as no error)."
        ),
    )

    # --- Provenance ---
    source_page: str = Field(default="Insights_Analysis")
    source_block: str = Field(default="top_block")
    version: str = Field(default=CRITERIA_MODEL_VERSION)

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "examples": [
                {
                    "risk_level": "Moderate",
                    "invest_period_days": 90,
                    "required_return_pct": 0.10,
                    "max_risk_score": 60.0,
                    "min_conviction_score": 60.0,
                    "exclude_forecast_unavailable": True,
                }
            ]
        },
    )

    # -----------------------------------------------------------------------
    # Validators (mode="before" -- normalize raw input to Pydantic-safe types)
    # -----------------------------------------------------------------------

    @field_validator("risk_level", "confidence_level", mode="before")
    @classmethod
    def _validate_bucket(cls, v: Any) -> str:
        return _normalize_bucket(v) or "Moderate"

    @field_validator("invest_period_days", mode="before")
    @classmethod
    def _validate_days(cls, v: Any) -> int:
        return max(1, _to_int(v, 90))

    @field_validator("required_return_pct", "min_expected_roi_pct", mode="before")
    @classmethod
    def _validate_ratio(cls, v: Any) -> float:
        return _as_ratio(v, 0.0)

    @field_validator("min_ai_confidence", mode="before")
    @classmethod
    def _validate_confidence(cls, v: Any) -> float:
        return _clamp(_as_ratio(v, 0.60), 0.0, 1.0)

    @field_validator("max_risk_score", mode="before")
    @classmethod
    def _validate_risk(cls, v: Any) -> float:
        # Preserve legitimate zero values (fixed from v2.0.0's "or 60.0" bug).
        parsed = _to_float(v, None)
        f = 60.0 if parsed is None else parsed
        # If value is between 0-1, assume it's a fraction and convert to 0-100.
        if 0.0 <= f <= 1.0 and f != 0.0:
            f = f * 100.0
        return _clamp(f, 0.0, 100.0)

    @field_validator("min_conviction_score", mode="before")
    @classmethod
    def _validate_conviction(cls, v: Any) -> float:
        """
        v3.1.0: parse conviction floor with fraction-shape detection.

        Accepts:
          - None / "" / unparseable -> 0.0 (no filter, preserves
            v3.0.0 semantics for rows that don't include this field).
          - 0.7  -> 70.0 (treated as fraction since 0 < value <= 1).
          - 70   -> 70.0 (treated as percent points).
          - "60%" -> 60.0 (via _to_float).
        Clamps to [0.0, 100.0].
        """
        parsed = _to_float(v, None)
        if parsed is None:
            return 0.0
        # Mirror max_risk_score's fraction->percent shape detection
        # (but preserve a legitimate zero, which means "no filter").
        if 0.0 < parsed <= 1.0:
            parsed = parsed * 100.0
        return _clamp(parsed, 0.0, 100.0)

    @field_validator("amount", mode="before")
    @classmethod
    def _validate_amount(cls, v: Any) -> float:
        parsed = _to_float(v, None)
        return max(0.0, 0.0 if parsed is None else parsed)

    @field_validator("pages_selected", mode="before")
    @classmethod
    def _validate_pages(cls, v: Any) -> List[str]:
        pages = _split_pages(v)
        resolved = _resolve_pages(pages) if pages else []
        return resolved or ["ALL"]

    @field_validator("top_n", mode="before")
    @classmethod
    def _validate_top_n(cls, v: Any) -> int:
        return _clamp(_to_int(v, 10), 1, 200)  # type: ignore[return-value]

    @field_validator(
        "top10_enabled",
        "include_market_summary",
        "include_risk_scenarios",
        "include_top_opportunities",
        "include_portfolio_health",
        mode="before",
    )
    @classmethod
    def _validate_bool_flag(cls, v: Any) -> bool:
        return _to_bool(v, True)

    @field_validator(
        "exclude_engine_dropped_valuation",
        "exclude_forecast_unavailable",
        "exclude_provider_errors",
        mode="before",
    )
    @classmethod
    def _validate_exclusion_flag(cls, v: Any) -> bool:
        """
        v3.1.0: Exclusion flags default False (opt-in filtering).

        Distinct from `_validate_bool_flag` which defaults to True for
        section flags (where the default-on behaviour is desirable).
        Exclusion flags default off so v3.0.0 callers see identical
        behaviour.
        """
        return _to_bool(v, False)

    @model_validator(mode="after")
    def _finalize(self) -> "AdvisorCriteria":
        if not self.risk_level:
            self.risk_level = "Moderate"
        if not self.confidence_level:
            self.confidence_level = "High"
        if not self.pages_selected:
            self.pages_selected = ["ALL"]
        return self

    # -----------------------------------------------------------------------
    # Backward Compatibility Aliases
    # -----------------------------------------------------------------------

    @property
    def investment_period_days(self) -> int:
        return self.invest_period_days

    @property
    def required_return(self) -> float:
        return self.required_return_pct

    @property
    def min_roi(self) -> float:
        return self.min_expected_roi_pct

    # -----------------------------------------------------------------------
    # Derived Properties
    # -----------------------------------------------------------------------

    @property
    def horizon_enum(self) -> Horizon:
        return Horizon.from_days(self.invest_period_days)

    @property
    def horizon(self) -> str:
        return self.horizon_enum.value

    @property
    def expected_roi_key(self) -> str:
        return self.horizon_enum.expected_roi_key

    @property
    def forecast_price_key(self) -> str:
        return self.horizon_enum.forecast_price_key

    # -----------------------------------------------------------------------
    # Public Methods
    # -----------------------------------------------------------------------

    def includes_page(self, page_name: str) -> bool:
        """Return True if `page_name` is covered by the current selection."""
        if not page_name:
            return False
        if any(p.upper() == "ALL" for p in self.pages_selected):
            return True
        normalized = PageHelper.normalize_page(page_name, allow_output_pages=True)
        return normalized in set(self.pages_selected)

    def to_public_dict(self) -> Dict[str, Any]:
        """Convert to a JSON-friendly dict including derived fields and aliases."""
        d = self.model_dump(mode="python")
        d["horizon"] = self.horizon
        d["expected_roi_key"] = self.expected_roi_key
        d["forecast_price_key"] = self.forecast_price_key
        # Backward compatibility aliases
        d["investment_period_days"] = self.invest_period_days
        d["required_return"] = self.required_return_pct
        d["min_roi"] = self.min_expected_roi_pct
        return d

    def to_scenario_variants(self) -> List["AdvisorCriteria"]:
        """
        Generate three scenario variants: Conservative, Moderate, Aggressive.

        The Moderate variant inherits user-supplied thresholds as minimums
        (so a user who asked for required_return=15% gets >=15% in Moderate).
        Conservative and Aggressive use fixed presets.

        v3.1.0: variants now also carry min_conviction_score + 3 data
        quality exclusion flags from _SCENARIO_PRESETS (see preset table
        for per-scenario defaults).
        """
        base_days = self.invest_period_days
        base_pages = list(self.pages_selected)
        base_top_n = self.top_n

        # Merge-compute Moderate-specific fields from user inputs (preserve quirk:
        # max_risk_score == 0 in user input means "use default 60").
        user_max_risk = self.max_risk_score if self.max_risk_score > 0 else 60.0
        moderate_risk = min(user_max_risk, 60.0)

        overrides_by_label: Dict[str, Dict[str, Any]] = {
            "Conservative": {},
            "Moderate": {
                "required_return_pct": max(self.required_return_pct, _SCENARIO_PRESETS["Moderate"]["required_return_pct"]),
                "min_expected_roi_pct": max(self.min_expected_roi_pct, _SCENARIO_PRESETS["Moderate"]["min_expected_roi_pct"]),
                "max_risk_score": moderate_risk,
            },
            "Aggressive": {},
        }

        variants: List[AdvisorCriteria] = []
        for label in SCENARIO_LABELS:
            preset = {**_SCENARIO_PRESETS[label], **overrides_by_label[label]}
            variants.append(AdvisorCriteria(
                risk_level=preset["risk_level"],
                confidence_level=preset["confidence_level"],
                invest_period_days=base_days,
                required_return_pct=preset["required_return_pct"],
                min_expected_roi_pct=preset["min_expected_roi_pct"],
                min_ai_confidence=preset["min_ai_confidence"],
                max_risk_score=preset["max_risk_score"],
                # v3.1.0: NEW conviction + exclusion fields (defaults preserved
                # when scenario preset doesn't override).
                min_conviction_score=preset.get("min_conviction_score", 0.0),
                exclude_engine_dropped_valuation=preset.get(
                    "exclude_engine_dropped_valuation", False),
                exclude_forecast_unavailable=preset.get(
                    "exclude_forecast_unavailable", False),
                exclude_provider_errors=preset.get(
                    "exclude_provider_errors", False),
                amount=self.amount,
                pages_selected=base_pages,
                top_n=base_top_n,
                top10_enabled=self.top10_enabled,
                include_market_summary=False,
                include_risk_scenarios=False,
                include_top_opportunities=True,
                include_portfolio_health=False,
                source_page=self.source_page,
                source_block=f"scenario_{label.lower()}",
            ))
        return variants

    def scenario_label(self) -> str:
        """Infer scenario label from source_block or risk_level."""
        sb = _to_string(self.source_block).lower()
        for label in SCENARIO_LABELS:
            if label.lower() in sb:
                return label

        rl = _to_string(self.risk_level).lower()
        if rl in ("low", "very low"):
            return "Conservative"
        if rl in ("high", "very high"):
            return "Aggressive"
        if rl == "moderate":
            return "Moderate"
        return "Custom"

    def scenario_signal(self) -> str:
        """Return the Signal column value for this scenario."""
        return _SCENARIO_SIGNAL_MAP.get(self.scenario_label(), "NEUTRAL")

    # -----------------------------------------------------------------------
    # Factory Methods
    # -----------------------------------------------------------------------

    @classmethod
    def from_kv_map(
        cls,
        kv: Mapping[str, Any],
        *,
        source_page: str = "Insights_Analysis",
        source_block: str = "top_block",
        strict: bool = False,
    ) -> "AdvisorCriteria":
        """
        Build from a key/value dict tolerant to label variations.

        Args:
            kv: Dictionary of key/value pairs (sheet row data, API payload, etc.)
            source_page: Source page name for provenance
            source_block: Source block name for provenance
            strict: If True, raise `CriteriaValidationError` on parse failure.
                    If False (default), log a warning and return a defaults-only
                    instance.

        Examples:
            >>> kv = {"Risk Level": "High", "Period (Days)": 180,
            ...       "Min Conviction": 60}
            >>> criteria = AdvisorCriteria.from_kv_map(kv)
            >>> criteria.min_conviction_score
            60.0
        """
        kv = dict(kv or {})

        # Pre-build normalized key -> original_key lookup (done once)
        normalized_keys: Dict[str, str] = {}
        for key in kv.keys():
            norm = _norm_key(key)
            if norm:
                normalized_keys[norm] = key

        def pick(*candidate_names: str) -> Any:
            for name in candidate_names:
                norm = _norm_key(name)
                if norm in normalized_keys:
                    return kv.get(normalized_keys[norm])
            return None

        payload: Dict[str, Any] = {
            "risk_level": pick("risk_level", "risk level", "risk", "risk bucket"),
            "confidence_level": pick(
                "confidence_level", "confidence level", "confidence", "confidence bucket",
            ),
            "invest_period_days": pick(
                "invest_period_days", "investment_period_days",
                "investment period (days)", "invest period (days)",
                "period (days)", "period days", "period",
            ),
            "required_return_pct": pick(
                "required_return_pct", "required_return",
                "required return %", "required return", "required roi",
            ),
            "min_expected_roi_pct": pick(
                "min_expected_roi_pct", "min_roi",
                "min expected roi %", "min expected roi",
                "minimum roi", "minimum expected roi",
            ),
            "max_risk_score": pick(
                "max_risk_score", "max risk", "max risk score",
                "maximum risk", "max risk (score)",
            ),
            "min_ai_confidence": pick(
                "min_ai_confidence", "min ai confidence",
                "min confidence", "ai min confidence",
            ),
            # v3.1.0 Phase G: conviction + exclusion field aliases
            "min_conviction_score": pick(
                "min_conviction_score", "min conviction", "min_conviction",
                "conviction floor", "minimum conviction", "min conviction score",
                "conviction min", "conviction threshold",
            ),
            "exclude_engine_dropped_valuation": pick(
                "exclude_engine_dropped_valuation",
                "exclude engine drops", "exclude_engine_drops",
                "exclude engine drop", "drop engine flagged",
            ),
            "exclude_forecast_unavailable": pick(
                "exclude_forecast_unavailable",
                "exclude forecast unavailable", "exclude_forecast_na",
                "exclude forecast na", "drop unforecastable",
            ),
            "exclude_provider_errors": pick(
                "exclude_provider_errors",
                "exclude provider errors", "exclude_provider_errs",
                "exclude provider error", "drop provider errors",
            ),
            "amount": pick(
                "amount", "invest amount", "investment amount",
                "capital", "portfolio amount",
            ),
            "pages_selected": pick(
                "pages_selected", "pages", "pages selected",
                "tabs", "tabs selected", "selected pages",
            ),
            "top_n": pick("top_n", "top n", "top", "top count"),
            "top10_enabled": pick(
                "top10_enabled", "top10 enabled", "top 10 enabled",
                "enable top10", "top10",
            ),
            "include_market_summary": pick("include_market_summary", "market summary"),
            "include_risk_scenarios": pick("include_risk_scenarios", "risk scenarios"),
            "include_top_opportunities": pick("include_top_opportunities", "top opportunities"),
            "include_portfolio_health": pick("include_portfolio_health", "portfolio health"),
            "source_page": source_page,
            "source_block": source_block,
            "version": CRITERIA_MODEL_VERSION,
        }

        # Special case: top10_enabled given as an integer (e.g. "Top 10 = 15")
        # means "enable Top 10 AND use 15 as top_n" -- but only if top_n wasn't
        # explicitly provided elsewhere. (Fixed from v2.0.0 which silently
        # overrode an explicit top_n.)
        t10 = payload.get("top10_enabled")
        if isinstance(t10, (int, float)) and not isinstance(t10, bool):
            if int(t10) > 0:
                payload["top10_enabled"] = True
                if payload.get("top_n") is None:
                    payload["top_n"] = int(t10)
            else:
                payload["top10_enabled"] = False

        # Drop None so field defaults apply
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            return cls.model_validate(payload)
        except Exception as exc:
            if strict:
                raise CriteriaValidationError(
                    f"Failed to parse criteria from KV map: {exc}"
                ) from exc
            logger.warning("Failed to parse criteria from KV map: %s", exc)
            return cls(source_page=source_page, source_block=source_block)

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[Sequence[Any]],
        *,
        key_col: int = 0,
        value_col: int = 1,
        source_page: str = "Insights_Analysis",
        source_block: str = "top_block",
        max_rows: int = 50,
        strict: bool = False,
    ) -> "AdvisorCriteria":
        """
        Parse criteria from a 2D matrix (e.g., Google Sheets rows).

        Expected format:
            rows = [
                ["Risk Level", "Moderate"],
                ["Investment Period (Days)", 90],
                ["Required Return %", "10%"],
                ["Min Conviction", 60],         # v3.1.0
                ...
            ]
        """
        kv: Dict[str, Any] = {}
        min_cols = max(key_col, value_col)
        for i, row in enumerate(rows or []):
            if i >= max_rows:
                break
            if not isinstance(row, (list, tuple)) or len(row) <= min_cols:
                continue
            key = _to_string(row[key_col])
            value = row[value_col]
            if _is_blank(key) and _is_blank(value):
                continue
            if key:
                kv[key] = value

        return cls.from_kv_map(
            kv,
            source_page=source_page,
            source_block=source_block,
            strict=strict,
        )

    @classmethod
    def from_schema_defaults(cls, *, source_page: str = "Insights_Analysis") -> "AdvisorCriteria":
        """Build using default values from schema_registry, falling back to hardcoded defaults."""
        try:
            from core.sheets.schema_registry import _insights_criteria_fields  # type: ignore
        except ImportError:
            try:
                from schema_registry import _insights_criteria_fields  # type: ignore
            except ImportError:
                logger.debug("Schema registry not available; using hardcoded defaults.")
                return cls(source_page=source_page, source_block="schema_defaults")

        kv: Dict[str, Any] = {}
        for cf in _insights_criteria_fields():
            if cf.default not in ("", None):
                kv[cf.key] = cf.default
        return cls.from_kv_map(kv, source_page=source_page, source_block="schema_defaults")


def _norm_key(key: Any) -> str:
    """Normalize a KV key for fuzzy matching: collapse whitespace, lowercase."""
    return re.sub(r"\s+", " ", _to_string(key).strip().lower())


# ---------------------------------------------------------------------------
# Scenario Preset Table
# ---------------------------------------------------------------------------
# Replaces three 20-line hardcoded blocks in v2.0.0's `to_scenario_variants`.
# Tune scenario knobs by editing this table in one place.
#
# v3.1.0: extended with min_conviction_score + 3 data-quality exclusion
# flags per scenario. Conservative wants high-conviction picks with clean
# data; Aggressive accepts wider data tolerance for opportunity capture.

_SCENARIO_PRESETS: Dict[str, Dict[str, Any]] = {
    "Conservative": {
        "risk_level": "Low",
        "confidence_level": "High",
        "required_return_pct": 0.05,
        "min_expected_roi_pct": 0.03,
        "min_ai_confidence": 0.70,
        "max_risk_score": 40.0,
        # v3.1.0: Conservative wants high-conviction picks + clean data
        # only. min_conviction=70 aligns with reco_normalize v7.2.0
        # STRONG_BUY floor (60) plus a 10-point safety margin.
        "min_conviction_score": 70.0,
        "exclude_engine_dropped_valuation": True,
        "exclude_forecast_unavailable": True,
        "exclude_provider_errors": True,
    },
    "Moderate": {
        "risk_level": "Moderate",
        "confidence_level": "Moderate",
        "required_return_pct": 0.10,
        "min_expected_roi_pct": 0.07,
        "min_ai_confidence": 0.60,
        "max_risk_score": 60.0,
        # v3.1.0: Moderate aligns with reco_normalize v7.2.0 BUY floor
        # (45) plus a small margin. Only excludes forecast_unavailable
        # since rows without forecasts can't contribute ROI signal anyway.
        "min_conviction_score": 50.0,
        "exclude_engine_dropped_valuation": False,
        "exclude_forecast_unavailable": True,
        "exclude_provider_errors": False,
    },
    "Aggressive": {
        "risk_level": "High",
        "confidence_level": "Low",
        "required_return_pct": 0.20,
        "min_expected_roi_pct": 0.15,
        "min_ai_confidence": 0.45,
        "max_risk_score": 80.0,
        # v3.1.0: Aggressive accepts lower conviction (30 = HOLD-tier
        # downgrades from BUY) and no data-quality exclusions to maximize
        # the candidate pool. Operators see upstream caveats in the
        # Insights_Analysis Data Quality Alerts section regardless.
        "min_conviction_score": 30.0,
        "exclude_engine_dropped_valuation": False,
        "exclude_forecast_unavailable": False,
        "exclude_provider_errors": False,
    },
}

_SCENARIO_SIGNAL_MAP: Mapping[str, str] = {
    "Conservative": "LOW",
    "Moderate": "MODERATE",
    "Aggressive": "HIGH",
}


# ---------------------------------------------------------------------------
# Scenario Specification (Immutable)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScenarioSpec:
    """
    Immutable descriptor for a single risk scenario row in Insights_Analysis.

    v3.1.0: gained four new fields with defaults:
        min_conviction          : 0-100 conviction floor for this scenario
        exclude_engine_drops    : drop engine-cleared valuation rows
        exclude_forecast_unavail: drop forecast-unavailable rows
        exclude_provider_errors : drop rows with last_error_class set
    All default values preserve v3.0.0 semantics for existing callers
    (insights_builder v7.0.0's Risk Scenarios section reads only label /
    signal / notes; the new fields surface through the notes string and
    via direct attribute access for callers that need the policy).
    """
    label: str
    signal: str
    max_risk: float
    min_roi: float
    required_return: float
    min_confidence: float
    horizon: str
    # v3.1.0 NEW: conviction floor + 3 data quality exclusion flags
    min_conviction: float = 0.0
    exclude_engine_drops: bool = False
    exclude_forecast_unavail: bool = False
    exclude_provider_errors: bool = False
    notes: str = field(default="")

    def __post_init__(self) -> None:
        if self.label not in SCENARIO_LABELS:
            raise ValueError(f"Invalid scenario label: {self.label!r}")
        if self.signal not in SIGNAL_VALUES:
            raise ValueError(f"Invalid signal value: {self.signal!r}")
        if not 0 <= self.max_risk <= 100:
            raise ValueError(f"max_risk must be between 0 and 100: {self.max_risk}")
        if not 0 <= self.min_roi <= 10:
            raise ValueError(f"min_roi must be between 0 and 10: {self.min_roi}")
        if not 0 <= self.min_confidence <= 1:
            raise ValueError(f"min_confidence must be between 0 and 1: {self.min_confidence}")
        # v3.1.0: validate new field
        if not 0 <= self.min_conviction <= 100:
            raise ValueError(
                f"min_conviction must be between 0 and 100: {self.min_conviction}"
            )


def build_scenario_specs(criteria: AdvisorCriteria) -> List[ScenarioSpec]:
    """
    Convert AdvisorCriteria scenarios to typed ScenarioSpec instances.

    v3.1.0: the notes string now also surfaces the conviction floor and
    any data-quality exclusion policy active for the scenario.
    """
    specs: List[ScenarioSpec] = []
    for variant in criteria.to_scenario_variants():
        # Build the exclusion-policy suffix (only when at least one flag is on)
        exclude_parts: List[str] = []
        if variant.exclude_engine_dropped_valuation:
            exclude_parts.append("engine-drops")
        if variant.exclude_forecast_unavailable:
            exclude_parts.append("forecast-na")
        if variant.exclude_provider_errors:
            exclude_parts.append("provider-errs")
        exclude_suffix = f" | Exclude: {', '.join(exclude_parts)}" if exclude_parts else ""

        notes = (
            f"Risk ceiling: {variant.max_risk_score:.0f} | "
            f"Min ROI: {variant.min_expected_roi_pct * 100:.1f}% | "
            f"Min Confidence: {variant.min_ai_confidence * 100:.0f}% | "
            f"Min Conviction: {variant.min_conviction_score:.0f} | "
            f"Horizon: {variant.horizon}"
            f"{exclude_suffix}"
        )

        specs.append(ScenarioSpec(
            label=variant.scenario_label(),
            signal=variant.scenario_signal(),
            max_risk=variant.max_risk_score,
            min_roi=variant.min_expected_roi_pct,
            required_return=variant.required_return_pct,
            min_confidence=variant.min_ai_confidence,
            horizon=variant.horizon,
            # v3.1.0: new fields
            min_conviction=variant.min_conviction_score,
            exclude_engine_drops=variant.exclude_engine_dropped_valuation,
            exclude_forecast_unavail=variant.exclude_forecast_unavailable,
            exclude_provider_errors=variant.exclude_provider_errors,
            notes=notes,
        ))
    return specs


# ---------------------------------------------------------------------------
# Horizon Helpers (Public API)
# ---------------------------------------------------------------------------

def map_days_to_horizon(days: int) -> str:
    """Convert days to horizon label (1M / 3M / 12M)."""
    return Horizon.from_days(days).value


def horizon_to_expected_roi_key(horizon: str) -> str:
    """Get expected ROI field key for a horizon; defaults to expected_roi_12m."""
    return _HORIZON_ROI_KEYS.get(_to_string(horizon).upper(), "expected_roi_12m")


def horizon_to_forecast_price_key(horizon: str) -> str:
    """Get forecast price field key for a horizon; defaults to forecast_price_12m."""
    return _HORIZON_PRICE_KEYS.get(_to_string(horizon).upper(), "forecast_price_12m")


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------

__all__ = [
    # Version
    "CRITERIA_MODEL_VERSION",
    # v3.1.0 Phase F: __version__ alias (TFB module convention)
    "__version__",
    # Constants
    "SCENARIO_LABELS",
    "SIGNAL_VALUES",
    # Enums
    "RiskLevel",
    "ConfidenceLevel",
    "Horizon",
    # Main Model
    "AdvisorCriteria",
    # Scenario Support
    "ScenarioSpec",
    "build_scenario_specs",
    # Signal Helpers
    "signal_for_value",
    "SignalMapper",
    # Horizon Helpers
    "map_days_to_horizon",
    "horizon_to_expected_roi_key",
    "horizon_to_forecast_price_key",
    # v3.1.0 Phase E: env-tunable conviction floor helpers
    "get_strong_buy_conviction_floor",
    "get_buy_conviction_floor",
    # Page Helper (exposed for tests / callers that need canonical pages)
    "PageHelper",
    # Exceptions
    "CriteriaModelError",
    "CriteriaParseError",
    "CriteriaValidationError",
]
