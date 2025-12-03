"""
core/data_engine.py
----------------------------------------------------------------------
UNIFIED DATA & ANALYSIS ENGINE – v1.1
Author: Emad Bahbah (with GPT-5.1 Thinking)

GOAL
- Avoid missing data by combining multiple providers (KSA + Global).
- Normalize all quote & fundamental data into a single structure.
- Provide data quality flags, missing field diagnostics, and
  a simple opportunity score + risk flag + notes.

INTEGRATION
- Backend: Tadawul Stock Analysis API v3.5.x (FastAPI).
- Can be used by routes like: /v1/enriched-quote or /v1/quote-advanced.
- Frontend: Google Sheets (Apps Script) and any UI.

USAGE EXAMPLE
----------------------------------------------------------------------
from core.data_engine import get_enriched_quote

quote = await get_enriched_quote("MSFT")
print(quote.model_dump())
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional, List, Literal, Tuple

from pydantic import BaseModel, Field

# ======================================================================
# 1) UNIFIED MODELS
# ======================================================================

DataQualityLevel = Literal["EXCELLENT", "GOOD", "FAIR", "POOR", "MISSING"]
MarketRegion = Literal["KSA", "GLOBAL", "UNKNOWN"]


class QuoteSourceInfo(BaseModel):
    """
    Meta-information for one provider used in the final merged result.
    """
    provider: str
    timestamp: datetime
    fields: List[str] = Field(default_factory=list)


class UnifiedQuote(BaseModel):
    """
    MASTER STRUCTURE for all quote/fundamental responses.

    IMPORTANT:
    - This should be the "standard" shape consumed by:
        * Google Sheets
        * React UI
        * DeepSeek / advisor logic
    - All external providers must be normalized to these fields.
    """

    # Identity
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    market_region: MarketRegion = "UNKNOWN"

    # Intraday price snapshot
    price: Optional[float] = None
    prev_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None

    # Derived price info
    change: Optional[float] = None
    change_pct: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None

    # Fundamentals (extend as needed)
    market_cap: Optional[float] = None
    eps_ttm: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    debt_to_equity: Optional[float] = None
    profit_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    revenue_growth_yoy: Optional[float] = None
    net_income_growth_yoy: Optional[float] = None

    # Meta
    last_updated_utc: Optional[datetime] = None
    data_quality: DataQualityLevel = "MISSING"
    data_gaps: List[str] = Field(default_factory=list)
    sources: List[QuoteSourceInfo] = Field(default_factory=list)

    # Analysis (basic v1)
    opportunity_score: Optional[float] = None
    risk_flag: Optional[str] = None
    notes: Optional[str] = None


# ======================================================================
# 2) PROVIDER ADAPTERS (STUBS – to be wired to real APIs)
# ======================================================================

async def fetch_from_eodhd(symbol: str) -> Dict:
    """
    EODHD (or other primary data provider) adaptor.

    TODO:
    - Replace this MOCK implementation with a real HTTP call
      using your existing HTTP client (httpx/aiohttp).
    - Normalize the API response keys to UnifiedQuote fields.

    NOTE:
    - Keeping a small MSFT mock so that you can test the engine
      even before wiring real APIs.
    """
    sym = symbol.upper()

    if sym == "MSFT":
        now = datetime.now(timezone.utc)
        return {
            "symbol": "MSFT",
            "name": "Microsoft Corporation",
            "exchange": "NASDAQ",
            "currency": "USD",
            "price": 420.0,
            "prev_close": 415.0,
            "open": 416.0,
            "high": 421.5,
            "low": 414.2,
            "volume": 18_000_000,
            "fifty_two_week_high": 430.0,
            "fifty_two_week_low": 310.0,
            "market_cap": 3.1e12,
            "eps_ttm": 11.9,
            "pe_ttm": 35.3,
            "pb": 12.0,
            "dividend_yield": 0.007,  # 0.7%
            "roe": 0.38,
            "roa": 0.18,
            "debt_to_equity": 0.55,
            "profit_margin": 0.35,
            "operating_margin": 0.42,
            "revenue_growth_yoy": 0.13,
            "net_income_growth_yoy": 0.16,
            "last_updated_utc": now,
            "__source__": QuoteSourceInfo(
                provider="EODHD",
                timestamp=now,
                fields=[
                    "price", "prev_close", "open", "high", "low",
                    "volume", "market_cap", "eps_ttm", "pe_ttm",
                    "pb", "roe", "profit_margin",
                    "revenue_growth_yoy", "net_income_growth_yoy",
                ],
            ),
        }

    # DEFAULT: empty dict = no data from this provider
    return {}


async def fetch_from_fmp(symbol: str) -> Dict:
    """
    FinancialModelingPrep (FMP) adaptor (fundamentals / backup).

    TODO:
    - If you still use FMP for /profile or fundamentals, call it here.
    - Normalize all keys to UnifiedQuote fields and return a dict.

    For now, returns {} (no data).
    """
    return {}


async def fetch_from_tadawul_bridge(symbol: str) -> Dict:
    """
    INTERNAL TADAWUL BRIDGE ADAPTOR – KSA .SR TICKERS

    IDEA:
    - Call your own /v1/quote endpoint (or new /v1/ksa-quote) here
      to reuse the existing multi-provider logic (finnhub, alpha_vantage).
    - Map fields to UnifiedQuote structure.

    EXAMPLE SHAPE TO RETURN:
    {
        "symbol": "1120.SR",
        "name": "Al Rajhi Bank",
        "exchange": "TADAWUL",
        "currency": "SAR",
        "price": 93.5,
        ...
        "__source__": QuoteSourceInfo(...)
    }

    For now, returns {} (no data).
    """
    return {}


# Provider registry in PRIORITY ORDER
PROVIDERS: List[Tuple[str, callable]] = [
    ("EODHD", fetch_from_eodhd),
    ("FMP", fetch_from_fmp),
    ("TADAWUL_BRIDGE", fetch_from_tadawul_bridge),
    # later: ("DEEPSEEK_AI", fetch_from_deepseek_enriched), etc.
]


# ======================================================================
# 3) MERGING & DATA COMPLETENESS LOGIC
# ======================================================================

def _merge_dicts(primary: Dict, secondary: Dict) -> Dict:
    """
    Merge two provider dicts:
    - Keep existing values in primary.
    - Fill missing (None) values from secondary.
    - Ignore internal keys starting with "__".
    """
    merged = dict(primary)
    for key, value in secondary.items():
        if key.startswith("__"):
            continue
        if merged.get(key) is None and value is not None:
            merged[key] = value
    return merged


def _infer_market_region(symbol: str, exchange: Optional[str]) -> MarketRegion:
    """
    Simple mapping KSA vs GLOBAL vs UNKNOWN.
    """
    sym = symbol.upper()
    if sym.endswith(".SR") or (exchange and exchange.upper() in {"TADAWUL", "NOMU"}):
        return "KSA"
    if exchange and exchange.upper() in {"NASDAQ", "NYSE", "AMEX", "LSE", "XETRA"}:
        return "GLOBAL"
    return "UNKNOWN"


def _calculate_change_fields(data: Dict) -> None:
    """
    Compute absolute and percentage change if possible.
    """
    price = data.get("price")
    prev_close = data.get("prev_close")

    if price is None or prev_close is None:
        return

    if prev_close == 0:
        return

    data["change"] = price - prev_close
    data["change_pct"] = (price - prev_close) / prev_close * 100.0


def _assess_data_quality(data: Dict) -> Tuple[DataQualityLevel, List[str]]:
    """
    Assign a simple quality score based on coverage of key fields.
    """
    required_price_fields = ["price", "prev_close", "volume"]
    important_fundamentals = ["market_cap", "eps_ttm", "pe_ttm", "pb"]

    missing: List[str] = []

    for f in required_price_fields:
        if data.get(f) is None:
            missing.append(f)

    for f in important_fundamentals:
        if data.get(f) is None:
            missing.append(f)

    total_fields = len(required_price_fields) + len(important_fundamentals)
    missing_count = len(missing)

    if missing_count == total_fields:
        # Almost nothing available
        return "MISSING", missing

    coverage = 1.0 - (missing_count / float(total_fields))

    if coverage > 0.80:
        return "EXCELLENT", missing
    if coverage > 0.60:
        return "GOOD", missing
    if coverage > 0.40:
        return "FAIR", missing
    return "POOR", missing


# ======================================================================
# 4) ANALYSIS LAYER (BASIC v1 – CAN BE EXTENDED)
# ======================================================================

def _compute_opportunity_score(data: Dict) -> Optional[float]:
    """
    VERY SIMPLE SCORING MODEL (0–100)
    - Valuation: PE vs ROE
    - Profitability: Profit margin
    - Growth: Revenue & Net income YoY

    You can later replace this with your DeepSeek model output.
    """
    pe = data.get("pe_ttm")
    roe = data.get("roe")
    pm = data.get("profit_margin")
    rev_g = data.get("revenue_growth_yoy")
    ni_g = data.get("net_income_growth_yoy")

    if all(v is None for v in [pe, roe, pm, rev_g, ni_g]):
        return None

    score = 50.0  # neutral baseline

    # Valuation vs profitability
    if pe is not None and roe is not None and pe > 0:
        # ROE in percentage to compare
        pe_roe_ratio = pe / max(roe * 100.0, 0.1)
        if pe_roe_ratio < 0.5:
            score += 15
        elif pe_roe_ratio < 1.0:
            score += 8
        elif pe_roe_ratio > 2.0:
            score -= 10

    # Profit margin
    if pm is not None:
        pm_pct = pm * 100.0
        if pm_pct > 25:
            score += 10
        elif pm_pct > 15:
            score += 5
        elif pm_pct < 5:
            score -= 5

    # Growth: revenue + net income
    growth_scores: List[float] = []
    for g in (rev_g, ni_g):
        if g is None:
            continue
        g_pct = g * 100.0
        if g_pct > 20:
            growth_scores.append(10)
        elif g_pct > 10:
            growth_scores.append(6)
        elif g_pct > 0:
            growth_scores.append(3)
        elif g_pct < -5:
            growth_scores.append(-5)

    if growth_scores:
        score += sum(growth_scores) / len(growth_scores)

    # Clamp to [0, 100]
    return max(0.0, min(100.0, score))


def _derive_risk_flag(data: Dict, opportunity_score: Optional[float]) -> Optional[str]:
    """
    Simple labels based on opportunity + some risk metrics.
    """
    if opportunity_score is None:
        return None

    pe = data.get("pe_ttm")
    debt_equity = data.get("debt_to_equity")
    rev_g = data.get("revenue_growth_yoy")

    if opportunity_score >= 70:
        if debt_equity is not None and debt_equity > 1.5:
            return "LEVERAGED OPPORTUNITY"
        if rev_g is not None and rev_g < 0:
            return "TURNAROUND PLAY"
        return "STRONG OPPORTUNITY"

    if opportunity_score <= 35:
        if pe is not None and pe > 40:
            return "OVERVALUED / SPECULATIVE"
        if rev_g is not None and rev_g < 0:
            return "DECLINING BUSINESS"
        return "WEAK OPPORTUNITY"

    return "NEUTRAL"


def _build_notes(
    data: Dict,
    opportunity_score: Optional[float],
    risk_flag: Optional[str],
) -> Optional[str]:
    """
    Build a human-readable comment, suitable for:
    - Sheets (Insights_Analysis)
    - Advisor UI
    """
    pieces: List[str] = []

    if risk_flag:
        pieces.append(f"Risk flag: {risk_flag}.")
    if opportunity_score is not None:
        pieces.append(f"Composite opportunity score: {opportunity_score:.1f}/100.")

    pe = data.get("pe_ttm")
    roe = data.get("roe")
    pm = data.get("profit_margin")
    rev_g = data.get("revenue_growth_yoy")
    ni_g = data.get("net_income_growth_yoy")

    if pe is not None:
        pieces.append(f"P/E (TTM): {pe:.1f}.")
    if roe is not None:
        pieces.append(f"ROE: {roe * 100:.1f}%.")
    if pm is not None:
        pieces.append(f"Profit margin: {pm * 100:.1f}%.")
    if rev_g is not None:
        pieces.append(f"Revenue YoY growth: {rev_g * 100:.1f}%.")
    if ni_g is not None:
        pieces.append(f"Net income YoY growth: {ni_g * 100:.1f}%.")

    return " ".join(pieces) if pieces else None


# ======================================================================
# 5) PUBLIC ENTRYPOINT
# ======================================================================

async def get_enriched_quote(symbol: str) -> UnifiedQuote:
    """
    MAIN PUBLIC FUNCTION
    ------------------------------------------------------------------
    - Calls all registered providers concurrently.
    - Merges their data into a single normalized dict.
    - Computes:
        * market_region
        * change & change_pct
        * data_quality + data_gaps
        * opportunity_score + risk_flag + notes
    - Returns a UnifiedQuote Pydantic model.
    """

    symbol = symbol.strip()
    if not symbol:
        raise ValueError("Symbol cannot be empty.")

    # 1) Call all providers in parallel
    tasks: Dict[str, asyncio.Task] = {
        name: asyncio.create_task(func(symbol))
        for name, func in PROVIDERS
    }

    raw_results: Dict[str, Dict] = {}
    sources: List[QuoteSourceInfo] = []

    for name, task in tasks.items():
        try:
            data = await task
        except Exception:
            # In production you can log this with structlog
            continue

        if not data:
            continue

        raw_results[name] = data

        src = data.get("__source__")
        if isinstance(src, QuoteSourceInfo):
            sources.append(src)
        else:
            # fallback source info
            sources.append(
                QuoteSourceInfo(
                    provider=name,
                    timestamp=datetime.now(timezone.utc),
                    fields=[k for k in data.keys() if not k.startswith("__")],
                )
            )

    # 2) If NOTHING from any provider, return a "missing" quote
    if not raw_results:
        return UnifiedQuote(
            symbol=symbol,
            last_updated_utc=datetime.now(timezone.utc),
            data_quality="MISSING",
            data_gaps=[
                "No data from any provider (EODHD/FMP/TADAWUL_BRIDGE).",
            ],
            sources=[],
        )

    # 3) Merge provider data respecting PRIORITY order
    merged: Dict = {}
    for provider_name, _func in PROVIDERS:
        provider_data = raw_results.get(provider_name)
        if not provider_data:
            continue

        if not merged:
            # first non-empty result becomes base
            merged = {
                k: v
                for k, v in provider_data.items()
                if not k.startswith("__")
            }
        else:
            merged = _merge_dicts(merged, provider_data)

    # 4) Derived fields & housekeeping
    _calculate_change_fields(merged)

    merged["market_region"] = _infer_market_region(
        merged.get("symbol") or symbol,
        merged.get("exchange"),
    )

    if merged.get("last_updated_utc") is None:
        merged["last_updated_utc"] = datetime.now(timezone.utc)

    dq, gaps = _assess_data_quality(merged)

    # 5) Analysis
    opp_score = _compute_opportunity_score(merged)
    risk_flag = _derive_risk_flag(merged, opp_score)
    notes = _build_notes(merged, opp_score, risk_flag)

    # 6) Build UnifiedQuote model
    quote = UnifiedQuote(
        **{
            **merged,
            "symbol": merged.get("symbol") or symbol,
            "data_quality": dq,
            "data_gaps": gaps,
            "opportunity_score": opp_score,
            "risk_flag": risk_flag,
            "notes": notes,
            "sources": sources,
        }
    )

    return quote
