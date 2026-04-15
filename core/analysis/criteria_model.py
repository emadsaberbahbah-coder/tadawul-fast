#!/usr/bin/env python3
"""
core/analysis/criteria_model.py
================================================================================
Advisor Criteria Model -- v1.1.0 (SCHEMA-ALIGNED / SCENARIO-AWARE)
================================================================================
Tadawul Fast Bridge (TFB)

Purpose
-------
Single validated source-of-truth for advisor criteria embedded in the
Insights_Analysis top block (key/value rows) and shared by Top_10_Investments.

v1.1.0 changes vs v1.0.0
--------------------------
FIX: Field names now match schema_registry v3.0.0 criteria_fields exactly:
  investment_period_days -> invest_period_days  (was mismatched)
  required_return        -> required_return_pct (was mismatched)
  min_roi                -> min_expected_roi_pct (was mismatched)
  Backward-compat properties kept for code that still uses old names.

FIX: Defaults aligned with schema_registry._insights_criteria_fields():
  max_risk_score default: 100.0 -> 60.0  (registry default is 60)
  min_ai_confidence default: 0.0 -> 0.60 (registry default is 0.60)

ENH: 4 section include flags for the new insights_builder.py 4-section layout:
  include_market_summary, include_risk_scenarios,
  include_top_opportunities, include_portfolio_health.

ENH: to_scenario_variants() -- generates Conservative/Moderate/Aggressive
  variants used by insights_builder.py Risk Scenarios section.

ENH: signal_for_value() -- maps a numeric/text value to the Signal column
  vocabulary (UP/DOWN/NEUTRAL/HIGH/MODERATE/LOW/OK/WARN/ALERT).

ENH: from_schema_defaults() -- builds an AdvisorCriteria directly from
  schema_registry._insights_criteria_fields() default values. Useful for
  initializing the top block of a fresh Insights_Analysis sheet.

No startup network I/O. Safe to import on Render.
================================================================================
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Pydantic (v2 preferred, v1 fallback)
# ---------------------------------------------------------------------------
try:
    from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator  # type: ignore
    _PYDANTIC_V2 = True
except Exception:
    from pydantic import BaseModel, Field  # type: ignore
    _PYDANTIC_V2 = False

    def field_validator(*args, **kwargs):  # type: ignore
        def _dec(fn):
            return fn
        return _dec

    def model_validator(*args, **kwargs):  # type: ignore
        def _dec(fn):
            return fn
        return _dec


CRITERIA_MODEL_VERSION = "1.1.0"

# ---------------------------------------------------------------------------
# Optional page catalog
# ---------------------------------------------------------------------------
try:
    from core.sheets.page_catalog import normalize_page_name, CANONICAL_PAGES  # type: ignore
    _HAS_PAGE_CATALOG = True
except Exception:
    _HAS_PAGE_CATALOG = False
    normalize_page_name = None  # type: ignore
    CANONICAL_PAGES = set()  # type: ignore

# ---------------------------------------------------------------------------
# Signal vocabulary (aligned with Insights_Analysis schema v3.0.0 Signal col)
# ---------------------------------------------------------------------------
#: Valid values for the Signal column in Insights_Analysis
SIGNAL_VALUES = frozenset({
    "UP", "DOWN", "NEUTRAL",
    "HIGH", "MODERATE", "LOW",
    "OK", "WARN", "ALERT",
    "STRONG_BUY", "BUY", "HOLD", "REDUCE", "SELL",
})

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY  = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _s(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _is_blank(x: Any) -> bool:
    return _s(x) == ""


def _to_bool(x: Any, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    s = _s(x).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _to_int(x: Any, default: int) -> int:
    if isinstance(x, bool):
        return default
    try:
        s = _s(x).replace(",", "")
        return int(float(s)) if s else default
    except Exception:
        return default


def _to_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return default
    try:
        s = _s(x).replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            return float(s[:-1].strip()) / 100.0
        return float(s)
    except Exception:
        return default


def _as_ratio(x: Any, default: float = 0.0) -> float:
    """
    Normalizes any percent-like value to a fraction (0.12 = 12%).
    Accepts: 0.12 | 12 | "12%" | "0.12"
    Values > 1.5 are treated as percent points and divided by 100.
    """
    f = _to_float(x, None)
    if f is None:
        return float(default)
    if abs(f) > 1.5:
        return float(f / 100.0)
    return float(f)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _norm_bucket(s: str) -> str:
    """Normalizes bucket labels to Very Low/Low/Moderate/High/Very High."""
    x = " ".join((_s(s) or "").strip().lower().replace("_", " ").replace("-", " ").split())
    if not x:
        return ""
    if x in ("vl", "very low", "very low risk", "very low confidence"):
        return "Very Low"
    if x in ("l", "low", "low risk", "low confidence"):
        return "Low"
    if x in ("m", "mid", "medium", "moderate", "balanced", "moderate risk", "moderate confidence"):
        return "Moderate"
    if x in ("h", "high", "high risk", "high confidence", "aggressive"):
        return "High"
    if x in ("vh", "very high", "very high risk", "very high confidence", "very aggressive"):
        return "Very High"
    return x.title()


# ---------------------------------------------------------------------------
# Horizon helpers (project rule: period always stored in DAYS)
# ---------------------------------------------------------------------------

def map_days_to_horizon(days: int) -> str:
    """
    Maps investment period days to a forecast horizon label.
      <= 45  -> 1M
      <= 120 -> 3M
      else   -> 12M
    """
    d = max(1, int(days))
    if d <= 45:
        return "1M"
    if d <= 120:
        return "3M"
    return "12M"


def horizon_to_expected_roi_key(h: str) -> str:
    h = _s(h).upper()
    if h == "1M":
        return "expected_roi_1m"
    if h == "3M":
        return "expected_roi_3m"
    return "expected_roi_12m"


def horizon_to_forecast_price_key(h: str) -> str:
    h = _s(h).upper()
    if h == "1M":
        return "forecast_price_1m"
    if h == "3M":
        return "forecast_price_3m"
    return "forecast_price_12m"


# ---------------------------------------------------------------------------
# Signal helpers (Insights_Analysis Signal column)
# ---------------------------------------------------------------------------

def signal_for_value(
    value: Any,
    *,
    metric: str = "",
    threshold_high: float = 70.0,
    threshold_low: float = 30.0,
) -> str:
    """
    Maps a numeric or text value to the Signal column vocabulary.

    Used by insights_builder.py when building the Market Summary and
    Risk Scenarios sections.

    Returns one of: UP, DOWN, NEUTRAL, HIGH, MODERATE, LOW, OK, WARN, ALERT,
                    STRONG_BUY, BUY, HOLD, REDUCE, SELL, or "" (unknown).
    """
    # Text passthrough if already a valid signal
    sv = _s(value).upper().replace(" ", "_").replace("-", "_")
    if sv in SIGNAL_VALUES:
        return sv

    # Recommendation passthrough
    reco_map = {
        "STRONG_BUY": "STRONG_BUY",
        "BUY": "BUY",
        "HOLD": "HOLD",
        "NEUTRAL": "NEUTRAL",
        "REDUCE": "REDUCE",
        "SELL": "SELL",
        "ACCUMULATE": "BUY",
        "AVOID": "SELL",
    }
    if sv in reco_map:
        return reco_map[sv]

    # Bucket passthrough
    bucket_map = {
        "HIGH": "HIGH", "H": "HIGH",
        "MODERATE": "MODERATE", "M": "MODERATE", "MEDIUM": "MODERATE",
        "LOW": "LOW", "L": "LOW",
        "VERY_HIGH": "HIGH", "VERY_LOW": "LOW",
    }
    if sv in bucket_map:
        return bucket_map[sv]

    # Numeric score/value
    f = _to_float(value, None)
    if f is not None:
        metric_l = (_s(metric) or "").lower()

        # Scores 0-100: map to HIGH/MODERATE/LOW
        if any(k in metric_l for k in ("score", "confidence", "quality", "value", "momentum", "risk")):
            if "risk" in metric_l:
                # Risk: higher = worse
                if f >= threshold_high:
                    return "HIGH"
                if f >= threshold_low:
                    return "MODERATE"
                return "LOW"
            # Others: higher = better
            if f >= threshold_high:
                return "HIGH"
            if f >= threshold_low:
                return "MODERATE"
            return "LOW"

        # ROI/return: positive = UP, negative = DOWN
        if any(k in metric_l for k in ("roi", "return", "change", "percent_change", "pl", "p/l")):
            if f > 0.02:
                return "UP"
            if f < -0.02:
                return "DOWN"
            return "NEUTRAL"

        # Generic: compare to threshold
        if f >= threshold_high:
            return "HIGH"
        if f < threshold_low:
            return "LOW"
        return "MODERATE"

    return ""


# ---------------------------------------------------------------------------
# Page list helpers
# ---------------------------------------------------------------------------

def _split_pages(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [p for p in (_s(i) for i in x) if p]
    s = _s(x)
    if not s:
        return []
    if s.strip().upper() == "ALL":
        return ["ALL"]
    return [p.strip() for p in re.split(r"[,;|]+", s) if p.strip()]


def _normalize_pages(pages: Sequence[str]) -> List[str]:
    raw = [p for p in (_s(x) for x in pages) if p]
    if not raw:
        return []
    if any(p.upper() == "ALL" for p in raw):
        if _HAS_PAGE_CATALOG and CANONICAL_PAGES:
            return sorted([p for p in CANONICAL_PAGES if p != "Data_Dictionary"])
        return ["ALL"]
    out: List[str] = []
    seen: set = set()
    for p in raw:
        key = p.strip()
        if not key:
            continue
        if _HAS_PAGE_CATALOG and callable(normalize_page_name):
            try:
                key = normalize_page_name(key, allow_output_pages=True)
            except Exception:
                pass
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return out


# ---------------------------------------------------------------------------
# AdvisorCriteria model
# ---------------------------------------------------------------------------

class AdvisorCriteria(BaseModel):
    """
    Advisor criteria read from the Insights_Analysis top block.

    Field naming aligned with schema_registry v3.0.0 criteria_fields:
      invest_period_days     (was investment_period_days in v1.0.0)
      required_return_pct    (was required_return in v1.0.0)
      min_expected_roi_pct   (was min_roi in v1.0.0)

    Backward-compat properties:
      .investment_period_days  -> alias for .invest_period_days
      .required_return         -> alias for .required_return_pct
      .min_roi                 -> alias for .min_expected_roi_pct

    All ratio fields (required_return_pct, min_expected_roi_pct,
    min_ai_confidence) are stored as fractions (0.12 = 12%).
    max_risk_score is stored as 0-100.
    """

    # --- Buckets ---
    risk_level: str = Field(
        default="Moderate",
        description="Risk tolerance bucket: Very Low / Low / Moderate / High / Very High.",
    )
    confidence_level: str = Field(
        default="High",
        description="Required confidence bucket: Very Low / Low / Moderate / High / Very High.",
    )

    # --- Period (always DAYS) ---
    # FIX v1.1.0: renamed from investment_period_days -> invest_period_days
    # to match schema_registry.CriteriaField key exactly.
    invest_period_days: int = Field(
        default=90,
        ge=1,
        le=3650,
        description="Investment period in DAYS. Mapped to 1M/3M/12M horizons.",
    )

    # --- Return thresholds (fractions) ---
    # FIX v1.1.0: renamed from required_return -> required_return_pct
    required_return_pct: float = Field(
        default=0.10,
        ge=-1.0,
        le=10.0,
        description="Required minimum return (fraction: 0.10 = 10%).",
    )
    # FIX v1.1.0: renamed from min_roi -> min_expected_roi_pct
    min_expected_roi_pct: float = Field(
        default=0.0,
        ge=-1.0,
        le=10.0,
        description="Minimum expected ROI filter (fraction: 0.10 = 10%).",
    )
    min_ai_confidence: float = Field(
        default=0.60,
        ge=0.0,
        le=1.0,
        description="Minimum AI forecast confidence (0-1 fraction). Default 0.60.",
    )

    # --- Risk threshold (score 0-100) ---
    max_risk_score: float = Field(
        default=60.0,
        ge=0.0,
        le=100.0,
        description="Max allowed risk score (0-100). Default 60 = moderate ceiling.",
    )

    # --- Capital ---
    amount: float = Field(
        default=0.0,
        ge=0.0,
        description="Investment capital amount (currency units). Optional.",
    )

    # --- Page selection ---
    pages_selected: List[str] = Field(
        default_factory=lambda: ["ALL"],
        description="Pages to include in Top-10 selection and insights. 'ALL' = all canonical pages.",
    )

    # --- Top 10 ---
    top_n: int = Field(
        default=10,
        ge=1,
        le=200,
        description="Top-N selection count (used by Insights + Top_10_Investments).",
    )
    top10_enabled: bool = Field(
        default=True,
        description="If True, Top_10_Investments page uses this criteria set.",
    )

    # --- Insights section flags (new in v1.1.0) ---
    # Controls which of the 4 executive sections are generated by insights_builder.
    include_market_summary: bool = Field(
        default=True,
        description="Generate Market Summary section in Insights_Analysis.",
    )
    include_risk_scenarios: bool = Field(
        default=True,
        description="Generate Risk Scenarios section (3 proposals) in Insights_Analysis.",
    )
    include_top_opportunities: bool = Field(
        default=True,
        description="Generate Top Opportunities section (Top 10) in Insights_Analysis.",
    )
    include_portfolio_health: bool = Field(
        default=True,
        description="Generate Portfolio Health section (P/L summary) in Insights_Analysis.",
    )

    # --- Provenance ---
    source_page: str = Field(default="Insights_Analysis")
    source_block: str = Field(default="top_block")
    version: str = Field(default=CRITERIA_MODEL_VERSION)

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", validate_assignment=True)

    # -----------------------------------------------------------------------
    # Validators
    # -----------------------------------------------------------------------

    @field_validator("risk_level", "confidence_level", mode="before")
    @classmethod
    def _v_bucket(cls, v: Any) -> str:
        return _norm_bucket(_s(v)) or "Moderate"

    @field_validator("invest_period_days", mode="before")
    @classmethod
    def _v_days(cls, v: Any) -> int:
        return max(1, _to_int(v, 90))

    @field_validator("required_return_pct", "min_expected_roi_pct", mode="before")
    @classmethod
    def _v_ratio(cls, v: Any) -> float:
        return _as_ratio(v, 0.0)

    @field_validator("min_ai_confidence", mode="before")
    @classmethod
    def _v_conf(cls, v: Any) -> float:
        return float(_clamp(_as_ratio(v, 0.60), 0.0, 1.0))

    @field_validator("max_risk_score", mode="before")
    @classmethod
    def _v_risk(cls, v: Any) -> float:
        f = _to_float(v, 60.0) or 60.0
        # if user accidentally passed fraction (0.6 meaning 60), convert
        if 0.0 <= f <= 1.0:
            f = f * 100.0
        return float(_clamp(f, 0.0, 100.0))

    @field_validator("amount", mode="before")
    @classmethod
    def _v_amount(cls, v: Any) -> float:
        return float(max(0.0, _to_float(v, 0.0) or 0.0))

    @field_validator("pages_selected", mode="before")
    @classmethod
    def _v_pages(cls, v: Any) -> List[str]:
        pages = _split_pages(v)
        return _normalize_pages(pages) if pages else ["ALL"]

    @field_validator("top_n", mode="before")
    @classmethod
    def _v_top_n(cls, v: Any) -> int:
        return max(1, min(200, _to_int(v, 10)))

    @field_validator(
        "top10_enabled", "include_market_summary", "include_risk_scenarios",
        "include_top_opportunities", "include_portfolio_health",
        mode="before",
    )
    @classmethod
    def _v_bool(cls, v: Any) -> bool:
        return _to_bool(v, True)

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
    # Backward-compat properties (v1.0.0 field names)
    # -----------------------------------------------------------------------

    @property
    def investment_period_days(self) -> int:
        """Backward-compat alias for invest_period_days."""
        return self.invest_period_days

    @property
    def required_return(self) -> float:
        """Backward-compat alias for required_return_pct."""
        return self.required_return_pct

    @property
    def min_roi(self) -> float:
        """Backward-compat alias for min_expected_roi_pct."""
        return self.min_expected_roi_pct

    # -----------------------------------------------------------------------
    # Derived helpers
    # -----------------------------------------------------------------------

    @property
    def horizon(self) -> str:
        return map_days_to_horizon(self.invest_period_days)

    @property
    def expected_roi_key(self) -> str:
        return horizon_to_expected_roi_key(self.horizon)

    @property
    def forecast_price_key(self) -> str:
        return horizon_to_forecast_price_key(self.horizon)

    def includes_page(self, page_name: str) -> bool:
        if not page_name:
            return False
        if any(p.upper() == "ALL" for p in self.pages_selected):
            return True
        p = _s(page_name)
        if _HAS_PAGE_CATALOG and callable(normalize_page_name):
            try:
                p = normalize_page_name(p, allow_output_pages=True)
            except Exception:
                pass
        return p in set(self.pages_selected)

    def to_public_dict(self) -> Dict[str, Any]:
        """JSON-friendly dict with all keys matching schema_registry criteria fields."""
        if _PYDANTIC_V2:
            d = self.model_dump(mode="python")
        else:
            d = self.dict()  # type: ignore
        d["horizon"] = self.horizon
        d["expected_roi_key"] = self.expected_roi_key
        d["forecast_price_key"] = self.forecast_price_key
        # Include schema-aligned key aliases for downstream callers
        d["investment_period_days"] = self.invest_period_days   # compat
        d["required_return"] = self.required_return_pct          # compat
        d["min_roi"] = self.min_expected_roi_pct                 # compat
        return d

    # -----------------------------------------------------------------------
    # Scenario generation (new in v1.1.0)
    # -----------------------------------------------------------------------

    def to_scenario_variants(self) -> List["AdvisorCriteria"]:
        """
        Returns 3 scenario variants: Conservative, Moderate, Aggressive.

        Used by insights_builder.py to populate the Risk Scenarios section.
        Each variant has the same pages_selected and horizon as the base criteria
        but different risk/confidence/return thresholds.

        The base criteria's risk_level determines the center point:
          - Conservative: always max_risk_score <= 40, required_return = 0.05
          - Moderate:     uses base values as-is (or recentered to 60)
          - Aggressive:   max_risk_score <= 80, required_return = 0.20

        Returns a list of 3 AdvisorCriteria instances in order:
          [0] Conservative, [1] Moderate, [2] Aggressive
        """
        base_days   = self.invest_period_days
        base_pages  = list(self.pages_selected)
        base_top_n  = self.top_n

        # Conservative scenario
        conservative = AdvisorCriteria(
            risk_level="Low",
            confidence_level="High",
            invest_period_days=base_days,
            required_return_pct=0.05,
            min_expected_roi_pct=0.03,
            min_ai_confidence=0.70,
            max_risk_score=40.0,
            amount=self.amount,
            pages_selected=base_pages,
            top_n=base_top_n,
            top10_enabled=self.top10_enabled,
            include_market_summary=False,
            include_risk_scenarios=False,
            include_top_opportunities=True,
            include_portfolio_health=False,
            source_page=self.source_page,
            source_block="scenario_conservative",
        )

        # Moderate scenario (centered on base or default)
        moderate_risk = min(self.max_risk_score, 60.0) if self.max_risk_score > 0 else 60.0
        moderate = AdvisorCriteria(
            risk_level="Moderate",
            confidence_level="Moderate",
            invest_period_days=base_days,
            required_return_pct=max(self.required_return_pct, 0.10),
            min_expected_roi_pct=max(self.min_expected_roi_pct, 0.07),
            min_ai_confidence=0.60,
            max_risk_score=moderate_risk,
            amount=self.amount,
            pages_selected=base_pages,
            top_n=base_top_n,
            top10_enabled=self.top10_enabled,
            include_market_summary=False,
            include_risk_scenarios=False,
            include_top_opportunities=True,
            include_portfolio_health=False,
            source_page=self.source_page,
            source_block="scenario_moderate",
        )

        # Aggressive scenario
        aggressive = AdvisorCriteria(
            risk_level="High",
            confidence_level="Low",
            invest_period_days=base_days,
            required_return_pct=0.20,
            min_expected_roi_pct=0.15,
            min_ai_confidence=0.45,
            max_risk_score=80.0,
            amount=self.amount,
            pages_selected=base_pages,
            top_n=base_top_n,
            top10_enabled=self.top10_enabled,
            include_market_summary=False,
            include_risk_scenarios=False,
            include_top_opportunities=True,
            include_portfolio_health=False,
            source_page=self.source_page,
            source_block="scenario_aggressive",
        )

        return [conservative, moderate, aggressive]

    def scenario_label(self) -> str:
        """
        Infer scenario label from source_block or risk_level.
        Returns: 'Conservative' | 'Moderate' | 'Aggressive' | 'Custom'
        """
        sb = _s(self.source_block).lower()
        if "conservative" in sb:
            return "Conservative"
        if "aggressive" in sb:
            return "Aggressive"
        if "moderate" in sb:
            return "Moderate"
        rl = _s(self.risk_level).lower()
        if rl in ("low", "very low"):
            return "Conservative"
        if rl == "high" or "very high" in rl:
            return "Aggressive"
        if rl == "moderate":
            return "Moderate"
        return "Custom"

    def scenario_signal(self) -> str:
        """
        Returns the Signal column value for this scenario.
        Conservative -> LOW, Moderate -> MODERATE, Aggressive -> HIGH.
        """
        label = self.scenario_label()
        return {"Conservative": "LOW", "Moderate": "MODERATE", "Aggressive": "HIGH"}.get(label, "NEUTRAL")

    # -----------------------------------------------------------------------
    # Parsing helpers
    # -----------------------------------------------------------------------

    @classmethod
    def from_kv_map(
        cls,
        kv: Dict[str, Any],
        *,
        source_page: str = "Insights_Analysis",
        source_block: str = "top_block",
    ) -> "AdvisorCriteria":
        """
        Build from a key/value dict tolerant to common sheet label variations.

        Supports all schema_registry criteria field keys directly, plus
        human-readable synonyms from the Insights_Analysis top block.
        """
        kv = kv or {}
        nmap = {re.sub(r"\s+", " ", _s(k).strip().lower()): k for k in kv.keys()}

        def pick(*names: str) -> Any:
            for nm in names:
                key = re.sub(r"\s+", " ", _s(nm).strip().lower())
                if key in nmap:
                    return kv.get(nmap[key])
            return None

        payload: Dict[str, Any] = {
            # --- Schema-aligned keys (exact match first) ---
            "risk_level": pick(
                "risk_level", "risk level", "risk", "risk bucket", "risk level (bucket)",
            ),
            "confidence_level": pick(
                "confidence_level", "confidence level", "confidence", "confidence bucket",
            ),
            # FIX v1.1.0: accept both new (invest_period_days) and old name
            "invest_period_days": pick(
                "invest_period_days",
                "investment_period_days",
                "investment period (days)",
                "invest period (days)",
                "investment period days",
                "invest period days",
                "invest period",
                "investment period",
                "period (days)",
                "period days",
                "period",
            ),
            # FIX v1.1.0: accept both new (required_return_pct) and old name
            "required_return_pct": pick(
                "required_return_pct",
                "required_return",
                "required return %",
                "required return",
                "required roi",
                "required roi %",
            ),
            # FIX v1.1.0: accept both new (min_expected_roi_pct) and old name
            "min_expected_roi_pct": pick(
                "min_expected_roi_pct",
                "min_roi",
                "min expected roi %",
                "min expected roi",
                "minimum roi",
                "minimum expected roi",
                "min roi %",
            ),
            "max_risk_score": pick(
                "max_risk_score",
                "max risk",
                "max risk score",
                "maximum risk",
                "max risk (score)",
            ),
            "min_ai_confidence": pick(
                "min_ai_confidence",
                "min ai confidence",
                "min confidence",
                "ai min confidence",
                "minimum ai confidence",
                "minimum confidence",
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
            # Section flags (new v1.1.0)
            "include_market_summary": pick(
                "include_market_summary", "market summary", "include market summary",
            ),
            "include_risk_scenarios": pick(
                "include_risk_scenarios", "risk scenarios", "include risk scenarios",
            ),
            "include_top_opportunities": pick(
                "include_top_opportunities", "top opportunities", "include top opportunities",
            ),
            "include_portfolio_health": pick(
                "include_portfolio_health", "portfolio health", "include portfolio health",
            ),
            "source_page": source_page,
            "source_block": source_block,
            "version": CRITERIA_MODEL_VERSION,
        }

        # If top10_enabled is an integer (e.g. 10), treat as enabling + set top_n
        t10 = payload.get("top10_enabled")
        if isinstance(t10, (int, float)) and not isinstance(t10, bool):
            if int(t10) > 0:
                payload["top10_enabled"] = True
                payload["top_n"] = int(t10)

        # Remove None values so defaults kick in from the model
        payload = {k: v for k, v in payload.items() if v is not None}

        if _PYDANTIC_V2:
            return cls.model_validate(payload)
        return cls.parse_obj(payload)  # type: ignore

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
    ) -> "AdvisorCriteria":
        """
        Parse criteria from a 2D matrix (e.g. Google Sheets row data).

        Expects rows like:
          [["Risk Level", "Moderate"],
           ["Investment Period (Days)", 90],
           ["Required Return %", "10%"], ...]
        """
        kv: Dict[str, Any] = {}
        for i, r in enumerate(rows or []):
            if i >= max_rows:
                break
            if not isinstance(r, (list, tuple)) or len(r) <= max(key_col, value_col):
                continue
            k = _s(r[key_col])
            v = r[value_col]
            if _is_blank(k) and _is_blank(v):
                continue
            if k:
                kv[k] = v
        return cls.from_kv_map(kv, source_page=source_page, source_block=source_block)

    @classmethod
    def from_schema_defaults(cls, *, source_page: str = "Insights_Analysis") -> "AdvisorCriteria":
        """
        Build an AdvisorCriteria using the default values from
        schema_registry._insights_criteria_fields(). Useful for initializing
        a fresh Insights_Analysis sheet or resetting criteria to defaults.
        """
        try:
            from core.sheets.schema_registry import _insights_criteria_fields  # type: ignore
        except ImportError:
            try:
                from schema_registry import _insights_criteria_fields  # type: ignore
            except ImportError:
                return cls(source_page=source_page, source_block="schema_defaults")

        kv: Dict[str, Any] = {}
        for cf in _insights_criteria_fields():
            if cf.default not in ("", None):
                kv[cf.key] = cf.default
        return cls.from_kv_map(kv, source_page=source_page, source_block="schema_defaults")


# ---------------------------------------------------------------------------
# Scenario label constant (for insights_builder sections)
# ---------------------------------------------------------------------------

SCENARIO_LABELS: Tuple[str, ...] = ("Conservative", "Moderate", "Aggressive")


@dataclass(frozen=True)
class ScenarioSpec:
    """
    Lightweight descriptor for a single risk scenario row in Insights_Analysis.
    Built by AdvisorCriteria.to_scenario_variants() and consumed by
    insights_builder._build_risk_scenario_rows().
    """
    label: str           # Conservative | Moderate | Aggressive
    signal: str          # LOW | MODERATE | HIGH
    max_risk: float      # 0-100
    min_roi: float       # fraction
    required_return: float  # fraction
    min_confidence: float   # 0-1
    horizon: str         # 1M | 3M | 12M
    notes: str = ""


def build_scenario_specs(criteria: AdvisorCriteria) -> List[ScenarioSpec]:
    """
    Converts AdvisorCriteria.to_scenario_variants() into typed ScenarioSpec
    instances for use by insights_builder without importing criteria_model
    in the builder itself (avoids circular dep).
    """
    variants = criteria.to_scenario_variants()
    return [
        ScenarioSpec(
            label=v.scenario_label(),
            signal=v.scenario_signal(),
            max_risk=v.max_risk_score,
            min_roi=v.min_expected_roi_pct,
            required_return=v.required_return_pct,
            min_confidence=v.min_ai_confidence,
            horizon=v.horizon,
            notes=(
                f"Risk ceiling: {v.max_risk_score:.0f} | "
                f"Min ROI: {v.min_expected_roi_pct * 100:.1f}% | "
                f"Min Confidence: {v.min_ai_confidence * 100:.0f}% | "
                f"Horizon: {v.horizon}"
            ),
        )
        for v in variants
    ]


__all__ = [
    "CRITERIA_MODEL_VERSION",
    "SCENARIO_LABELS",
    "SIGNAL_VALUES",
    "AdvisorCriteria",
    "ScenarioSpec",
    "build_scenario_specs",
    "map_days_to_horizon",
    "horizon_to_expected_roi_key",
    "horizon_to_forecast_price_key",
    "signal_for_value",
]
