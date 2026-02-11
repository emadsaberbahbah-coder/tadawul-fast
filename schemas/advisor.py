# schemas/advisor.py
"""
Tadawul Fast Bridge — Advisor Schemas (Request/Response Contract)
Version: 0.3.0

Key Fixes in v0.3.0
- Accepts payload.tickers / payload.symbols (list OR comma string) without breaking Pydantic validation.
- Keeps response Sheets-safe even when no opportunities are found:
  -> headers are ALWAYS non-empty for status="ok"
  -> rows may be empty (0 items), but schema stays renderable in Google Sheets.
- Still ignores unknown fields (Sheets often sends extras).

Design principles
- Inputs are forgiving (strings, percent, ratio); normalized to stable internal types
- Unknown fields are ignored
- Response is stable: headers + rows + meta (always present)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

TT_ADVISOR_SCHEMA_VERSION = "0.3.0"

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
# Stable default headers (Sheets-safe even when result set is empty)
# IMPORTANT: backend may return richer "items" payload; but the stable contract
# for Sheets writing is headers+rows.
# -----------------------------------------------------------------------------
TT_ADVISOR_DEFAULT_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Market",
    "Currency",
    "Price",
    "Advisor Score",
    "Action",
    "Allocation %",
    "Allocation Amount",
    "Expected ROI % (1M)",
    "Expected ROI % (3M)",
    "Risk Bucket",
    "Confidence Bucket",
    "Reason (Explain)",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
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


def _normalize_tickers(v: Any) -> List[str]:
    """
    Accept:
    - ["1120.SR", "AAPL.US"]
    - "1120.SR,AAPL.US"
    - "1120.SR"
    - None -> []
    Returns uppercase, trimmed, de-duplicated (stable order).
    """
    if v is None:
        return []

    items: List[str] = []

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        parts = [p.strip() for p in s.split(",") if p.strip()]
        items = parts

    elif isinstance(v, list):
        for it in v:
            if it is None:
                continue
            s = str(it).strip()
            if s:
                items.append(s)

    else:
        s = str(v).strip()
        if s:
            items = [s]

    # normalize + dedupe preserving order
    seen = set()
    out: List[str] = []
    for t in items:
        u = t.strip().upper()
        if not u or u == "SYMBOL":
            continue
        if u.startswith("#"):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


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
    - tickers/symbols: can be list OR comma-separated string
    """

    # NEW (compat with GAS + existing backend routes)
    tickers: List[str] = Field(default_factory=list, description="Universe tickers list (optional).")
    symbols: List[str] = Field(default_factory=list, description="Alias for tickers (optional).")

    sources: List[AdvisorSource] = Field(
        default_factory=lambda: ["ALL"],
        description="Which sheets/pages to include. Use ['ALL'] for all supported pages.",
    )

    risk: RiskBucket = Field(default="Moderate", description="Risk bucket filter from dropdown")
    confidence: ConfidenceBucket = Field(default="High", description="Confidence bucket filter from dropdown")

    required_roi_1m: float = Field(
        default=0.05,
        ge=0.0,
        le=5.0,
        description="Required ROI for 1M. Accepts ratio (0.05) or percent (5). Normalized to ratio.",
    )
    required_roi_3m: float = Field(
        default=0.10,
        ge=0.0,
        le=10.0,
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

    model_config = {"extra": "ignore"}  # IMPORTANT: ignore unknown fields from Sheets

    # -------------------------
    # Validators (Pydantic v2)
    # -------------------------
    @field_validator("sources", mode="before")
    @classmethod
    def _v_sources(cls, v: Any) -> Any:
        return _normalize_sources(v)

    @field_validator("tickers", mode="before")
    @classmethod
    def _v_tickers(cls, v: Any) -> Any:
        return _normalize_tickers(v)

    @field_validator("symbols", mode="before")
    @classmethod
    def _v_symbols(cls, v: Any) -> Any:
        return _normalize_tickers(v)

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
    def _v_post(self) -> "AdvisorRequest":
        # Swap min/max if user reversed them
        if self.min_price is not None and self.max_price is not None and self.max_price < self.min_price:
            self.min_price, self.max_price = self.max_price, self.min_price

        # If sources somehow empty, keep stable default
        if not self.sources:
            self.sources = ["ALL"]  # type: ignore

        # Merge symbols into tickers if tickers is empty or partial
        # (backend routes may use either name)
        if self.symbols:
            if not self.tickers:
                self.tickers = list(self.symbols)
            else:
                # union preserve order
                seen = set(self.tickers)
                for s in self.symbols:
                    if s not in seen:
                        self.tickers.append(s)
                        seen.add(s)

        return self


# -----------------------------------------------------------------------------
# Response
# -----------------------------------------------------------------------------
class AdvisorResponse(BaseModel):
    """
    Stable output for Sheets writing.

    - status: "ok" or "error"
    - headers: list of strings (must be non-empty for status="ok")
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
        # Force status=error when error exists
        if self.error and self.status != "error":
            self.status = "error"  # type: ignore

        # Ensure containers exist
        if self.headers is None:
            self.headers = []  # type: ignore
        if self.rows is None:
            self.rows = []  # type: ignore
        if self.meta is None:
            self.meta = {}  # type: ignore

        # CRITICAL FIX:
        # If status="ok" but no opportunities found, we STILL return stable headers
        # so Google Sheets can render the report without "0 columns" errors.
        if self.status == "ok" and (not self.headers or len(self.headers) == 0):
            self.headers = list(TT_ADVISOR_DEFAULT_HEADERS)

        # If rows exist, normalize row widths to match headers
        if self.headers and self.rows:
            w = len(self.headers)
            normalized: List[List[Any]] = []
            for r in self.rows:
                rr = list(r) if isinstance(r, list) else []
                if len(rr) < w:
                    rr.extend([""] * (w - len(rr)))
                elif len(rr) > w:
                    rr = rr[:w]
                normalized.append(rr)
            self.rows = normalized

        return self


__all__ = [
    "TT_ADVISOR_SCHEMA_VERSION",
    "TT_ADVISOR_DEFAULT_HEADERS",
    "RiskBucket",
    "ConfidenceBucket",
    "AdvisorSource",
    "AdvisorRequest",
    "AdvisorResponse",
]
