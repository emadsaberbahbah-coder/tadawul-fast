# schemas/advisor.py
"""
Tadawul Fast Bridge — Advisor Schemas (Request/Response Contract)
Version: 0.2.0

Purpose
- Validate inbound criteria from Google Sheets / GAS
- Enforce stable, Sheets-safe outbound structure
- Prevent breaking changes over time

Design principles
- Inputs are forgiving (strings, percent, ratio); normalized to stable internal types
- Unknown fields are ignored (Sheets often sends extra columns)
- Response is stable: headers + rows + meta (always present)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

TT_ADVISOR_SCHEMA_VERSION = "0.2.0"


# -----------------------------------------------------------------------------
# Enums / Literals (keep aligned with your Sheets dropdowns)
# -----------------------------------------------------------------------------
RiskBucket = Literal["", "Low", "Moderate", "High", "Very High"]
ConfidenceBucket = Literal["", "Low", "Moderate", "High"]

AdvisorSource = Literal[
    "ALL",
    "Market_Leaders",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _to_float_safe(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        if not s:
            return default
        try:
            return float(s)
        except Exception:
            return default
    return default


def _roi_to_ratio(v: Any) -> float:
    """
    Normalize ROI to ratio:
      0.10 -> 0.10
      10 -> 0.10
      "10%" -> 0.10
      "0.10" -> 0.10
    """
    if v is None:
        return 0.0

    if isinstance(v, str):
        s = v.strip().replace(",", "")
        s = s.replace("%", "")
        if s == "":
            return 0.0
        try:
            v = float(s)
        except Exception:
            return 0.0

    if isinstance(v, (int, float)):
        f = float(v)
        # if user typed 5 or 10 (percent), convert to 0.05 / 0.10
        if f > 1.5:
            return f / 100.0
        # already ratio
        return f

    return 0.0


def _normalize_sources(v: Any) -> List[str]:
    """
    Accept:
    - ["ALL"]
    - "ALL"
    - "Market_Leaders,Global_Markets"
    - ["Market_Leaders", "Global_Markets"]
    Returns a clean list; defaults to ["ALL"] if empty.
    """
    if v is None:
        return ["ALL"]

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return ["ALL"]
        if s.upper() == "ALL":
            return ["ALL"]
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or ["ALL"]

    if isinstance(v, list):
        parts: List[str] = []
        for it in v:
            if it is None:
                continue
            if isinstance(it, str):
                s = it.strip()
                if s:
                    parts.append(s)
        if not parts:
            return ["ALL"]
        if any(p.upper() == "ALL" for p in parts):
            return ["ALL"]
        return parts

    return ["ALL"]


# -----------------------------------------------------------------------------
# Request
# -----------------------------------------------------------------------------
class AdvisorRequest(BaseModel):
    """
    Criteria sent by GAS.

    Notes:
    - ROI inputs accept:
        - ratio: 0.10 means 10%
        - percent: 10 means 10%
        - string: "10%" supported
    """

    sources: List[AdvisorSource] = Field(
        default_factory=lambda: ["ALL"],
        description="Which sheets/pages to include. Use ['ALL'] for all supported pages.",
    )

    risk: RiskBucket = Field(default="Moderate", description="Risk bucket filter from dropdown")
    confidence: ConfidenceBucket = Field(default="High", description="Confidence bucket filter from dropdown")

    required_roi_1m: float = Field(
        default=0.05,
        ge=0.0,
        le=5.0,  # ratio upper bound; enforced after normalization
        description="Required ROI for 1M. Accepts ratio (0.05) or percent (5). Normalized to ratio.",
    )
    required_roi_3m: float = Field(
        default=0.10,
        ge=0.0,
        le=10.0,  # ratio upper bound; enforced after normalization
        description="Required ROI for 3M. Accepts ratio (0.10) or percent (10). Normalized to ratio.",
    )

    top_n: int = Field(default=10, ge=1, le=200, description="Number of symbols to return")
    invest_amount: float = Field(default=10000.0, ge=0.0, le=1e12, description="Total amount to allocate")

    currency: str = Field(default="SAR", min_length=1, max_length=8)

    include_news: bool = Field(default=True, description="If true, enrich scoring with news intelligence")
    as_of_utc: Optional[str] = Field(default=None, description="Optional UTC ISO timestamp override")

    # Optional future knobs (safe to ignore)
    min_price: Optional[float] = Field(default=None, ge=0.0)
    max_price: Optional[float] = Field(default=None, ge=0.0)

    model_config = {"extra": "ignore"}  # IMPORTANT: ignore unknown fields from Sheets to avoid breaking

    # -------------------------
    # Validators (Pydantic v2)
    # -------------------------
    @field_validator("sources", mode="before")
    @classmethod
    def _v_sources(cls, v: Any) -> Any:
        return _normalize_sources(v)

    @field_validator("required_roi_1m", mode="before")
    @classmethod
    def _v_roi_1m(cls, v: Any) -> float:
        return _roi_to_ratio(v)

    @field_validator("required_roi_3m", mode="before")
    @classmethod
    def _v_roi_3m(cls, v: Any) -> float:
        return _roi_to_ratio(v)

    @field_validator("currency", mode="before")
    @classmethod
    def _v_currency(cls, v: Any) -> str:
        s = str(v or "SAR").strip().upper()
        return s if s else "SAR"

    @field_validator("invest_amount", mode="before")
    @classmethod
    def _v_invest_amount(cls, v: Any) -> float:
        return _to_float_safe(v, 10000.0)

    @model_validator(mode="after")
    def _v_price_range(self) -> "AdvisorRequest":
        # Swap min/max if user reversed them
        if self.min_price is not None and self.max_price is not None and self.max_price < self.min_price:
            self.min_price, self.max_price = self.max_price, self.min_price
        # If sources contains invalid entries, Pydantic will raise; keep stability by ensuring ["ALL"] if empty
        if not self.sources:
            self.sources = ["ALL"]  # type: ignore
        return self


# -----------------------------------------------------------------------------
# Response
# -----------------------------------------------------------------------------
class AdvisorResponse(BaseModel):
    """
    Stable output for Sheets writing.

    - status: "ok" or "error" (preferred, consistent with other routes)
    - headers: list of strings (must be non-empty for successful run)
    - rows: list of arrays aligned with headers
    - meta: run metadata, diagnostics, criteria echo
    - error: optional error message when status="error"
    """

    status: Literal["ok", "error"] = Field(default="ok")
    schema_version: str = Field(default=TT_ADVISOR_SCHEMA_VERSION)

    headers: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = Field(default=None)

    model_config = {"extra": "ignore"}

    @model_validator(mode="after")
    def _v_response_consistency(self) -> "AdvisorResponse":
        # If error exists, force status=error
        if self.error and self.status != "error":
            self.status = "error"  # type: ignore

        # If status=error, headers/rows can be empty; if ok, keep them as lists
        if self.headers is None:
            self.headers = []  # type: ignore
        if self.rows is None:
            self.rows = []  # type: ignore
        if self.meta is None:
            self.meta = {}  # type: ignore
        return self


__all__ = [
    "TT_ADVISOR_SCHEMA_VERSION",
    "RiskBucket",
    "ConfidenceBucket",
    "AdvisorSource",
    "AdvisorRequest",
    "AdvisorResponse",
]
