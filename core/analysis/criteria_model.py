#!/usr/bin/env python3
"""
core/analysis/criteria_model.py
================================================================================
Advisor Criteria Model (Insights_Analysis Embedded) — v1.0.0 (PHASE 5)
================================================================================

Purpose
- Single validated source-of-truth for "Advisor Criteria" now embedded in Insights_Analysis.
- Designed to be fed by a "top block" key/value section inside the Insights_Analysis sheet.

Key requirements (Phase 5)
- ✅ Pydantic model with validated defaults
- ✅ Supports flexible inputs (strings, %, decimals, ints)
- ✅ Investment period is ALWAYS treated as DAYS, and mapped to forecast horizons (1M/3M/12M)
- ✅ Pages selected are normalized via page_catalog when available
- ✅ Adds Top10 selector fields so Insights + Top10 can share identical criteria

No startup network I/O. Safe to import on Render.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Pydantic (v2 preferred, v1 fallback)
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator  # type: ignore

    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore

    _PYDANTIC_V2 = False

    def field_validator(*args, **kwargs):  # type: ignore
        def _decorator(fn):  # type: ignore
            return fn

        return _decorator

    def model_validator(*args, **kwargs):  # type: ignore
        def _decorator(fn):  # type: ignore
            return fn

        return _decorator


CRITERIA_MODEL_VERSION = "1.0.0"

# -----------------------------------------------------------------------------
# Optional imports (safe)
# -----------------------------------------------------------------------------
try:
    from core.sheets.page_catalog import normalize_page_name, CANONICAL_PAGES  # type: ignore

    _HAS_PAGE_CATALOG = True
except Exception:
    _HAS_PAGE_CATALOG = False
    normalize_page_name = None  # type: ignore
    CANONICAL_PAGES = set()  # type: ignore


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


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
        if isinstance(x, (int,)):
            return int(x)
        s = _s(x).replace(",", "")
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def _to_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return default
    try:
        if isinstance(x, (int, float)):
            return float(x)
        s = _s(x).replace(",", "")
        if not s:
            return default
        # handle percent like "12%"
        if s.endswith("%"):
            return float(s[:-1].strip()) / 100.0
        return float(s)
    except Exception:
        return default


def _as_ratio(x: Any, default: float = 0.0) -> float:
    """
    Accepts:
      - 0.12  (already fraction)
      - 12    (percent points -> 0.12)
      - "12%" -> 0.12
    """
    f = _to_float(x, None)
    if f is None:
        return float(default)
    # treat > 1.5 as percent points
    if abs(f) > 1.5:
        return float(f / 100.0)
    return float(f)


def _clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _norm_bucket(s: str) -> str:
    """
    Normalizes common labels to a consistent set for UI and filtering.
    Output set (recommended):
      Very Low, Low, Moderate, High, Very High
    """
    x = " ".join((s or "").strip().lower().replace("_", " ").replace("-", " ").split())
    if not x:
        return ""
    if x in ("vl", "verylow", "very low", "very low risk", "very low confidence"):
        return "Very Low"
    if x in ("l", "low", "low risk", "low confidence"):
        return "Low"
    if x in ("m", "mid", "medium", "moderate", "balanced", "moderate risk", "moderate confidence"):
        return "Moderate"
    if x in ("h", "high", "high risk", "high confidence", "aggressive"):
        return "High"
    if x in ("vh", "veryhigh", "very high", "very high risk", "very high confidence", "very aggressive"):
        return "Very High"
    # fallback title-case
    return x.title()


def map_days_to_horizon(days: int) -> str:
    """
    Project rule:
      Investment Period is stored in DAYS and mapped to horizons (1M / 3M / 12M).
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


def _split_pages(x: Any) -> List[str]:
    """
    Accepts:
      - list[str]
      - "Market_Leaders, Global Markets, Mutual_Funds"
      - "ALL"
    """
    if x is None:
        return []
    if isinstance(x, list):
        return [p for p in (_s(i) for i in x) if p]
    s = _s(x)
    if not s:
        return []
    if s.strip().upper() == "ALL":
        return ["ALL"]
    # split by comma or semicolon or pipe
    parts = re.split(r"[,\;\|]+", s)
    return [p.strip() for p in parts if p.strip()]


def _normalize_pages(pages: Sequence[str]) -> List[str]:
    raw = [p for p in (_s(x) for x in pages) if p]
    if not raw:
        return []
    if any(p.upper() == "ALL" for p in raw):
        # canonical default scope for criteria selection:
        # include all canonical pages except internal schema pages
        if _HAS_PAGE_CATALOG and CANONICAL_PAGES:
            # keep stable ordering by sorting
            return sorted([p for p in CANONICAL_PAGES if p not in ("Data_Dictionary",)])
        return ["ALL"]

    out: List[str] = []
    seen = set()
    for p in raw:
        key = p.strip()
        if not key:
            continue
        if _HAS_PAGE_CATALOG and callable(normalize_page_name):
            try:
                key = normalize_page_name(key, allow_output_pages=True)
            except Exception:
                # keep user's value, but still dedup
                key = p.strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return out


# -----------------------------------------------------------------------------
# Criteria Model
# -----------------------------------------------------------------------------
class AdvisorCriteria(BaseModel):
    """
    Advisor Criteria read from Insights_Analysis top block.

    Ratios:
      - required_return, min_roi, min_ai_confidence are FRACTIONS (0.12 = 12%)
      - max_risk_score is 0..100
    """

    # Buckets (string for compatibility across sheets/apps script)
    risk_level: str = Field(default="Moderate", description="Risk bucket: Very Low/Low/Moderate/High/Very High.")
    confidence_level: str = Field(default="High", description="Confidence bucket: Very Low..Very High.")

    # Period always in DAYS
    investment_period_days: int = Field(default=90, ge=1, le=3650, description="Investment period in DAYS.")

    # Returns / thresholds (fractions)
    required_return: float = Field(default=0.0, ge=-1.0, le=10.0, description="Required return (fraction).")
    min_roi: float = Field(default=0.0, ge=-1.0, le=10.0, description="Minimum ROI threshold (fraction).")
    min_ai_confidence: float = Field(default=0.0, ge=0.0, le=1.0, description="Minimum AI confidence (0..1).")

    # Risk threshold (score 0..100)
    max_risk_score: float = Field(default=100.0, ge=0.0, le=100.0, description="Max allowed risk score (0..100).")

    # Money
    invest_amount: float = Field(default=0.0, ge=0.0, description="Investment amount (currency units).")

    # Page selection
    pages_selected: List[str] = Field(default_factory=lambda: ["ALL"], description="Selected pages or ALL.")

    # Top10 selector / shared behavior
    top_n: int = Field(default=10, ge=1, le=200, description="Top-N selection used by Insights + Top10.")
    top10_enabled: bool = Field(default=True, description="If true, Top_10_Investments page uses this criteria.")

    # Optional sheet hints / diagnostics
    source_page: str = Field(default="Insights_Analysis", description="Where the criteria was read from.")
    source_block: str = Field(default="top_block", description="Which block / region in the page.")
    version: str = Field(default=CRITERIA_MODEL_VERSION, description="Criteria model version.")

    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore", validate_assignment=True)

    # -----------------------
    # Validators (robust)
    # -----------------------
    @field_validator("risk_level", "confidence_level", mode="before")
    @classmethod
    def _v_bucket(cls, v: Any) -> str:
        s = _s(v)
        if not s:
            return ""
        return _norm_bucket(s)

    @field_validator("investment_period_days", mode="before")
    @classmethod
    def _v_days(cls, v: Any) -> int:
        d = _to_int(v, 90)
        return max(1, d)

    @field_validator("required_return", "min_roi", mode="before")
    @classmethod
    def _v_ratio(cls, v: Any) -> float:
        return _as_ratio(v, 0.0)

    @field_validator("min_ai_confidence", mode="before")
    @classmethod
    def _v_conf_ratio(cls, v: Any) -> float:
        r = _as_ratio(v, 0.0)
        # treat 75 (meaning 75%) as 0.75
        return _clamp(r, 0.0, 1.0)

    @field_validator("max_risk_score", mode="before")
    @classmethod
    def _v_max_risk(cls, v: Any) -> float:
        f = _to_float(v, 100.0)
        if f is None:
            f = 100.0
        # if user passed fraction (0.7) by mistake, interpret as 70
        if 0.0 <= f <= 1.0:
            f = f * 100.0
        return float(_clamp(float(f), 0.0, 100.0))

    @field_validator("invest_amount", mode="before")
    @classmethod
    def _v_amount(cls, v: Any) -> float:
        f = _to_float(v, 0.0)
        if f is None:
            f = 0.0
        return float(max(0.0, f))

    @field_validator("pages_selected", mode="before")
    @classmethod
    def _v_pages(cls, v: Any) -> List[str]:
        pages = _split_pages(v)
        return _normalize_pages(pages) if pages else ["ALL"]

    @field_validator("top_n", mode="before")
    @classmethod
    def _v_top_n(cls, v: Any) -> int:
        n = _to_int(v, 10)
        return max(1, min(200, n))

    @field_validator("top10_enabled", mode="before")
    @classmethod
    def _v_top10(cls, v: Any) -> bool:
        return _to_bool(v, True)

    @model_validator(mode="after")
    def _finalize(self) -> "AdvisorCriteria":
        # defaults if blank
        if not self.risk_level:
            self.risk_level = "Moderate"
        if not self.confidence_level:
            self.confidence_level = "High"
        if not self.pages_selected:
            self.pages_selected = ["ALL"]
        return self

    # -----------------------
    # Derived helpers
    # -----------------------
    @property
    def horizon(self) -> str:
        return map_days_to_horizon(self.investment_period_days)

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
        """
        JSON-friendly, stable keys for endpoints like GET /v1/analysis/criteria.
        """
        if _PYDANTIC_V2:
            d = self.model_dump(mode="python")
        else:
            d = self.dict()  # type: ignore
        d["horizon"] = self.horizon
        d["expected_roi_key"] = self.expected_roi_key
        d["forecast_price_key"] = self.forecast_price_key
        return d

    # -----------------------
    # Parsing helpers for Insights_Analysis top block
    # -----------------------
    @classmethod
    def from_kv_map(
        cls,
        kv: Dict[str, Any],
        *,
        source_page: str = "Insights_Analysis",
        source_block: str = "top_block",
    ) -> "AdvisorCriteria":
        """
        Build criteria from a dict-like key/value map, tolerant to common sheet labels.

        Expected keys (examples / synonyms):
          Risk Level, Risk, Risk Bucket
          Confidence Level, Confidence, AI Confidence
          Invest Period, Investment Period (Days), Period Days
          Required Return, Required ROI
          Amount, Invest Amount
          Pages, Pages Selected, Tabs
          Min ROI, Minimum ROI
          Max Risk, Max Risk Score
          Min AI Confidence, Min Confidence, AI Min Confidence
          Top N, Top10, Top 10 Enabled
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
            "risk_level": pick("risk level", "risk", "risk bucket", "risk level (bucket)"),
            "confidence_level": pick("confidence level", "confidence", "confidence bucket", "ai confidence level"),
            "investment_period_days": pick(
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
            "required_return": pick("required return", "required roi", "required return %", "required roi %"),
            "min_roi": pick("min roi", "minimum roi", "min expected roi", "min required roi"),
            "max_risk_score": pick("max risk", "max risk score", "maximum risk", "max risk (score)"),
            "min_ai_confidence": pick("min ai confidence", "min confidence", "ai min confidence", "minimum ai confidence"),
            "invest_amount": pick("amount", "invest amount", "investment amount", "capital", "portfolio amount"),
            "pages_selected": pick("pages", "pages selected", "tabs", "tabs selected", "selected pages"),
            "top_n": pick("top n", "top_n", "top", "top10", "top 10", "top count"),
            "top10_enabled": pick("top10 enabled", "top 10 enabled", "enable top10", "top10", "top 10"),
            "source_page": source_page,
            "source_block": source_block,
            "version": CRITERIA_MODEL_VERSION,
        }

        # If top10_enabled provided as a number (10), treat it as enabling + set top_n.
        t10 = payload.get("top10_enabled")
        if isinstance(t10, (int, float)) and not isinstance(t10, bool):
            if int(t10) > 0:
                payload["top10_enabled"] = True
                payload["top_n"] = int(t10)

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
        Convenience parser if the Insights_Analysis top block was read as rows matrix.

        rows is expected to look like:
          [
            ["Risk Level", "Moderate"],
            ["Confidence Level", "High"],
            ["Investment Period (Days)", 90],
            ...
          ]
        """
        kv: Dict[str, Any] = {}
        for i, r in enumerate(rows or []):
            if i >= max_rows:
                break
            if not isinstance(r, (list, tuple)) or len(r) <= max(key_col, value_col):
                continue
            k = _s(r[key_col])
            if not k:
                continue
            v = r[value_col]
            # stop if we hit an empty row sentinel (optional)
            if _is_blank(k) and _is_blank(v):
                continue
            kv[k] = v
        return cls.from_kv_map(kv, source_page=source_page, source_block=source_block)


__all__ = [
    "AdvisorCriteria",
    "map_days_to_horizon",
    "horizon_to_expected_roi_key",
    "horizon_to_forecast_price_key",
    "CRITERIA_MODEL_VERSION",
]
