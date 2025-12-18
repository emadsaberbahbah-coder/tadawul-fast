# core/enriched_quote.py
"""
core/enriched_quote.py
===========================================================
EnrichedQuote Mapper + Sheets Row Builder – v2.5.0 (KSA-SCHEMA ALIGNED)

Why this update?
- Your core/schemas.py defines KSA_TADAWUL_HEADERS_52 with fields like:
  ROI (3M) %, Expected ROI (1M) %, Financial Health Score, Target Price (12M)...
- Older EnrichedQuote had extra="ignore" and lacked these fields,
  so they were silently dropped -> Sheets rows looked missing/wrong.

Design
- Defensive: never throws during row build.
- Header-driven: supports many English + Arabic header conventions.
- Sheets-safe values: None / NaN / inf -> "" (blank cell)
- Conservative ratio->percent conversion for ratio-like fundamentals only.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from core.data_engine_v2 import UnifiedQuote

ENRICHED_VERSION = "2.5.0"
RIYADH_TZ = timezone(timedelta(hours=3))


# =============================================================================
# Time helpers
# =============================================================================
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _now_riyadh_iso() -> str:
    return datetime.now(RIYADH_TZ).isoformat()


# =============================================================================
# Numeric helpers
# =============================================================================
def _is_bad_number(x: Any) -> bool:
    try:
        if isinstance(x, (int, float)):
            f = float(x)
            return math.isnan(f) or math.isinf(f)
    except Exception:
        return True
    return False

def _clean_value(x: Any) -> Any:
    if x is None:
        return ""
    if _is_bad_number(x):
        return ""
    return x

def _ratio_to_percent(v: Any) -> Optional[float]:
    """
    Conservative conversion for ratio-like fundamentals:
    - If provider returns 0.15 => 15.0
    - If provider returns 15.0 => 15.0
    """
    if v is None:
        return None
    try:
        f = float(v)
        if _is_bad_number(f):
            return None
        return f * 100.0 if -1.0 <= f <= 1.0 else f
    except Exception:
        return None

def _safe_upside(current: Any, target: Any) -> Optional[float]:
    try:
        c = float(current) if current is not None else None
        t = float(target) if target is not None else None
        if c is None or t is None or c == 0:
            return None
        if _is_bad_number(c) or _is_bad_number(t):
            return None
        return (t / c - 1.0) * 100.0
    except Exception:
        return None


# =============================================================================
# Header normalization
# =============================================================================
def _contains_arabic(s: str) -> bool:
    return any("\u0600" <= ch <= "\u06FF" for ch in (s or ""))

def _norm_header_en(h: str) -> str:
    """
    Normalize header labels to a compact key:
    - lowercase
    - remove non-alphanumeric
    Example: "Last Updated (Riyadh)" -> "lastupdatedriyadh"
    """
    s = (h or "").strip().lower()
    s = s.replace("٪", "%")
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def _snake_from_header(h: str) -> str:
    raw = (h or "").strip().lower()
    raw = re.sub(r"[\(\)\[\]]", " ", raw)
    raw = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return raw


# =============================================================================
# Arabic header map (best-effort)
# =============================================================================
_AR_HEADER_MAP: Dict[str, str] = {
    # Identity
    "الرمز": "symbol",
    "رمز": "symbol",
    "الشركة": "name",
    "اسم الشركة": "name",
    "الاسم": "name",
    "القطاع": "sector",
    "النشاط": "industry",
    "القطاع الفرعي": "sub_sector",
    "السوق": "market",
    "العملة": "currency",
    "تاريخ الادراج": "listing_date",
    "تاريخ الإدراج": "listing_date",

    # Prices
    "اخر سعر": "current_price",
    "آخر سعر": "current_price",
    "السعر": "current_price",
    "الاغلاق السابق": "previous_close",
    "الإغلاق السابق": "previous_close",
    "الافتتاح": "open",
    "الأعلى": "day_high",
    "الادنى": "day_low",
    "الأدنى": "day_low",
    "اعلى 52 اسبوع": "high_52w",
    "أعلى 52 أسبوع": "high_52w",
    "ادنى 52 اسبوع": "low_52w",
    "أدنى 52 أسبوع": "low_52w",
    "التغير": "price_change",
    "نسبة التغير": "percent_change",
    "نسبة التغير%": "percent_change",

    # Volume / value
    "حجم التداول": "volume",
    "قيمة التداول": "value_traded",
    "القيمة السوقية": "market_cap",

    # Ratios
    "مكرر الارباح": "pe_ttm",
    "ربحية السهم": "eps_ttm",
    "عائد التوزيع": "dividend_yield",
    "العائد على حقوق الملكية": "roe",
    "العائد على الأصول": "roa",
    "هامش الربح": "net_margin",
    "مديونية/حقوق": "debt_to_equity",
    "المديونية/حقوق": "debt_to_equity",

    # Meta
    "مصدر البيانات": "data_source",
    "جودة البيانات": "data_quality",
    "آخر تحديث utc": "last_updated_utc",
    "آخر تحديث": "last_updated_riyadh",
    "خطأ": "error",
    "الخطأ": "error",
}

def _ar_key(h: str) -> Optional[str]:
    if not h:
        return None
    s = (h or "").strip()
    if not s:
        return None
    s2 = s.replace("٪", "%")
    s2 = re.sub(r"\s+", " ", s2).strip()
    if s2 in _AR_HEADER_MAP:
        return _AR_HEADER_MAP[s2]
    s3 = s2.replace(":", "").replace("-", "").strip()
    if s3 in _AR_HEADER_MAP:
        return _AR_HEADER_MAP[s3]
    s4 = s3.replace(" ", "")
    for k, v in _AR_HEADER_MAP.items():
        if k.replace(" ", "") == s4:
            return v
    return None


# =============================================================================
# Model: EnrichedQuote
# =============================================================================
class EnrichedQuote(BaseModel):
    """
    Stable “public” quote shape for API + Google Sheets.
    Field names intentionally match UnifiedQuote (engine output).

    NOTE: This model explicitly includes KSA_TADAWUL_HEADERS_52 fields so
    they are not dropped by extra="ignore".
    """
    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
        validate_assignment=True,
        extra="ignore",
    )

    # Identity
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sub_sector: Optional[str] = None
    market: str = "UNKNOWN"
    currency: Optional[str] = None
    listing_date: Optional[str] = None

    # Shares / Float / Cap
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    market_cap: Optional[float] = None
    free_float_market_cap: Optional[float] = None

    # Prices
    current_price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    position_52w_percent: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None

    # Volume / Liquidity
    volume: Optional[float] = None
    avg_volume_30d: Optional[float] = None
    value_traded: Optional[float] = None
    turnover_percent: Optional[float] = None
    liquidity_score: Optional[float] = None

    # Fundamentals
    eps_ttm: Optional[float] = None
    forward_eps: Optional[float] = None
    pe_ttm: Optional[float] = None
    forward_pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    ev_ebitda: Optional[float] = None
    dividend_yield: Optional[float] = None
    dividend_rate: Optional[float] = None
    payout_ratio: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    net_margin: Optional[float] = None
    ebitda_margin: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None
    beta: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None

    # Technicals
    rsi_14: Optional[float] = None
    volatility_30d: Optional[float] = None
    macd: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None

    # Valuation / Targets
    fair_value: Optional[float] = None
    target_price: Optional[float] = None
    upside_percent: Optional[float] = None
    valuation_label: Optional[str] = None
    analyst_rating: Optional[str] = None

    # =========================
    # KSA schema extra fields
    # =========================
    roi_3m_percent: Optional[float] = None
    roi_12m_percent: Optional[float] = None

    revenue_growth_yoy_percent: Optional[float] = None

    financial_health_score: Optional[float] = None
    financial_health_rank_sector: Optional[float] = None

    target_price_12m: Optional[float] = None
    expected_roi_1m_percent: Optional[float] = None
    expected_roi_3m_percent: Optional[float] = None
    expected_roi_12m_percent: Optional[float] = None
    expected_price_growth_12m_percent: Optional[float] = None

    # Scores / Recommendation
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    momentum_score: Optional[float] = None
    opportunity_score: Optional[float] = None
    overall_score: Optional[float] = None
    recommendation: Optional[str] = None
    confidence: Optional[float] = None
    risk_score: Optional[float] = None

    # Meta
    data_source: str = "none"
    data_quality: str = "MISSING"
    last_updated_utc: str = ""
    last_updated_riyadh: str = ""
    error: Optional[str] = None

    # -------------------------------------------------------------------------
    @classmethod
    def from_unified(cls, q: Any) -> "EnrichedQuote":
        if q is None:
            return cls(
                symbol="UNKNOWN",
                data_quality="MISSING",
                error="Empty quote input",
                last_updated_utc=_now_utc_iso(),
                last_updated_riyadh=_now_riyadh_iso(),
            )

        if isinstance(q, EnrichedQuote):
            if not q.last_updated_utc:
                q.last_updated_utc = _now_utc_iso()
            if not q.last_updated_riyadh:
                q.last_updated_riyadh = _now_riyadh_iso()
            if not q.symbol:
                q.symbol = "UNKNOWN"
            return q

        data: Dict[str, Any] = {}
        try:
            if isinstance(q, UnifiedQuote):
                data = q.model_dump(exclude_none=False)
            elif isinstance(q, dict):
                data = dict(q)
            elif hasattr(q, "model_dump"):
                data = q.model_dump(exclude_none=False)  # type: ignore
            elif hasattr(q, "__dict__"):
                data = dict(getattr(q, "__dict__", {}) or {})
        except Exception:
            data = {}

        sym = (data.get("symbol") or data.get("ticker") or data.get("Symbol") or "").strip()
        if not sym and hasattr(q, "symbol"):
            try:
                sym = str(getattr(q, "symbol") or "").strip()
            except Exception:
                sym = ""
        if not sym:
            sym = "UNKNOWN"

        obj = cls(**{**data, "symbol": sym})

        if not obj.last_updated_utc:
            obj.last_updated_utc = _now_utc_iso()
        if not obj.last_updated_riyadh:
            obj.last_updated_riyadh = _now_riyadh_iso()

        if obj.upside_percent is None:
            tgt = obj.target_price if obj.target_price is not None else obj.fair_value
            obj.upside_percent = _safe_upside(obj.current_price, tgt)

        return obj

    # -------------------------------------------------------------------------
    def to_row(self, headers: List[str]) -> List[Any]:
        try:
            d = self.model_dump(exclude_none=False)
        except Exception:
            d = dict(getattr(self, "__dict__", {}) or {})

        def g(*keys: str) -> Any:
            for k in keys:
                if k in d:
                    return d.get(k)
            return None

        def pct_field(key: str) -> Any:
            return _ratio_to_percent(g(key))

        # Canonical field getters
        field_getters: Dict[str, Callable[[], Any]] = {
            # Identity
            "symbol": lambda: g("symbol"),
            "name": lambda: g("name"),
            "sector": lambda: g("sector"),
            "industry": lambda: g("industry"),
            "sub_sector": lambda: g("sub_sector"),
            "market": lambda: g("market"),
            "currency": lambda: g("currency"),
            "listing_date": lambda: g("listing_date"),

            # Prices
            "current_price": lambda: g("current_price"),
            "previous_close": lambda: g("previous_close"),
            "price_change": lambda: g("price_change"),
            "percent_change": lambda: g("percent_change"),
            "open": lambda: g("open"),
            "day_high": lambda: g("day_high"),
            "day_low": lambda: g("day_low"),
            "high_52w": lambda: g("high_52w"),
            "low_52w": lambda: g("low_52w"),
            "position_52w_percent": lambda: g("position_52w_percent"),

            # Volume / cap
            "volume": lambda: g("volume"),
            "avg_volume_30d": lambda: g("avg_volume_30d"),
            "value_traded": lambda: g("value_traded"),
            "turnover_percent": lambda: g("turnover_percent"),
            "shares_outstanding": lambda: g("shares_outstanding"),
            "free_float": lambda: g("free_float"),
            "market_cap": lambda: g("market_cap"),
            "free_float_market_cap": lambda: g("free_float_market_cap"),
            "liquidity_score": lambda: g("liquidity_score"),

            # Fundamentals
            "eps_ttm": lambda: g("eps_ttm"),
            "forward_eps": lambda: g("forward_eps"),
            "pe_ttm": lambda: g("pe_ttm"),
            "forward_pe": lambda: g("forward_pe"),
            "pb": lambda: g("pb"),
            "ps": lambda: g("ps"),
            "ev_ebitda": lambda: g("ev_ebitda"),
            "dividend_yield": lambda: pct_field("dividend_yield"),
            "dividend_rate": lambda: g("dividend_rate"),
            "payout_ratio": lambda: pct_field("payout_ratio"),
            "roe": lambda: pct_field("roe"),
            "roa": lambda: pct_field("roa"),
            "net_margin": lambda: pct_field("net_margin"),
            "ebitda_margin": lambda: pct_field("ebitda_margin"),
            "revenue_growth": lambda: pct_field("revenue_growth"),
            "net_income_growth": lambda: pct_field("net_income_growth"),
            "beta": lambda: g("beta"),
            "debt_to_equity": lambda: g("debt_to_equity"),
            "current_ratio": lambda: g("current_ratio"),
            "quick_ratio": lambda: g("quick_ratio"),

            # Technicals
            "volatility_30d": lambda: g("volatility_30d"),
            "rsi_14": lambda: g("rsi_14"),
            "macd": lambda: g("macd"),
            "ma20": lambda: g("ma20"),
            "ma50": lambda: g("ma50"),

            # Valuation / targets
            "fair_value": lambda: g("fair_value"),
            "target_price": lambda: g("target_price"),
            "upside_percent": lambda: g("upside_percent")
            if g("upside_percent") is not None
            else _safe_upside(g("current_price"), g("target_price") or g("fair_value")),
            "valuation_label": lambda: g("valuation_label"),
            "analyst_rating": lambda: g("analyst_rating"),

            # KSA extras (already percent fields by spec)
            "roi_3m_percent": lambda: g("roi_3m_percent"),
            "roi_12m_percent": lambda: g("roi_12m_percent"),
            "revenue_growth_yoy_percent": lambda: g("revenue_growth_yoy_percent"),
            "financial_health_score": lambda: g("financial_health_score"),
            "financial_health_rank_sector": lambda: g("financial_health_rank_sector"),
            "target_price_12m": lambda: g("target_price_12m"),
            "expected_roi_1m_percent": lambda: g("expected_roi_1m_percent"),
            "expected_roi_3m_percent": lambda: g("expected_roi_3m_percent"),
            "expected_roi_12m_percent": lambda: g("expected_roi_12m_percent"),
            "expected_price_growth_12m_percent": lambda: g("expected_price_growth_12m_percent"),

            # Scores / reco
            "value_score": lambda: g("value_score"),
            "quality_score": lambda: g("quality_score"),
            "momentum_score": lambda: g("momentum_score"),
            "opportunity_score": lambda: g("opportunity_score"),
            "risk_score": lambda: g("risk_score"),
            "overall_score": lambda: g("overall_score"),
            "recommendation": lambda: g("recommendation"),
            "confidence": lambda: g("confidence"),

            # Meta
            "data_source": lambda: g("data_source"),
            "data_quality": lambda: g("data_quality"),
            "last_updated_utc": lambda: g("last_updated_utc") or _now_utc_iso(),
            "last_updated_riyadh": lambda: g("last_updated_riyadh") or _now_riyadh_iso(),
            "error": lambda: g("error"),
        }

        # English aliases -> internal keys (normalized form)
        en_alias: Dict[str, str] = {
            # Identity
            "symbol": "symbol",
            "ticker": "symbol",
            "companyname": "name",
            "name": "name",
            "sector": "sector",
            "subsector": "sub_sector",
            "subsectorname": "sub_sector",
            "industry": "industry",
            "market": "market",
            "currency": "currency",
            "listingdate": "listing_date",

            # Prices
            "lastprice": "current_price",
            "currentprice": "current_price",
            "previousclose": "previous_close",
            "pricechange": "price_change",
            "percentchange": "percent_change",
            "open": "open",
            "dayhigh": "day_high",
            "daylow": "day_low",
            "52whigh": "high_52w",
            "52wlow": "low_52w",
            "52wposition%": "position_52w_percent",
            "52wpositionpercent": "position_52w_percent",
            "52wposition": "position_52w_percent",

            # Volume / cap
            "volume": "volume",
            "avgvolume30d": "avg_volume_30d",
            "avgvolume(30d)": "avg_volume_30d",
            "valuetraded": "value_traded",
            "turnover%": "turnover_percent",
            "turnoverpercent": "turnover_percent",
            "sharesoutstanding": "shares_outstanding",
            "freefloat%": "free_float",
            "marketcap": "market_cap",
            "freefloatmarketcap": "free_float_market_cap",
            "liquidityscore": "liquidity_score",

            # Fundamentals
            "eps(ttm)": "eps_ttm",
            "epsttm": "eps_ttm",
            "forwardeps": "forward_eps",
            "p/e(ttm)": "pe_ttm",
            "pettm": "pe_ttm",
            "forwardp/e": "forward_pe",
            "forwardpe": "forward_pe",
            "p/b": "pb",
            "pb": "pb",
            "p/s": "ps",
            "ps": "ps",
            "ev/ebitda": "ev_ebitda",
            "evebitda": "ev_ebitda",
            "dividendyield%": "dividend_yield",
            "dividendyield": "dividend_yield",
            "dividendrate": "dividend_rate",
            "payoutratio%": "payout_ratio",
            "payoutratio": "payout_ratio",
            "roe%": "roe",
            "roe": "roe",
            "roa%": "roa",
            "roa": "roa",
            "netmargin%": "net_margin",
            "netmargin": "net_margin",
            "ebitdamargin%": "ebitda_margin",
            "revenuegrowth%": "revenue_growth",
            "netincomegrowth%": "net_income_growth",
            "debt/equity": "debt_to_equity",
            "debtequity": "debt_to_equity",

            # Technicals
            "volatility(30d)": "volatility_30d",
            "volatility30d": "volatility_30d",
            "rsi(14)": "rsi_14",
            "rsi14": "rsi_14",

            # Valuation / Targets
            "fairvalue": "fair_value",
            "upside%": "upside_percent",
            "upsidepercent": "upside_percent",
            "valuationlabel": "valuation_label",
            "analystrating": "analyst_rating",

            # KSA schema fields
            "roi(3m)%": "roi_3m_percent",
            "roi3m%": "roi_3m_percent",
            "roi3m": "roi_3m_percent",

            "roi(12m)%": "roi_12m_percent",
            "roi12m%": "roi_12m_percent",
            "roi12m": "roi_12m_percent",

            "revenuegrowth(yoy)%": "revenue_growth_yoy_percent",
            "revenuegrowthyoy%": "revenue_growth_yoy_percent",
            "revenuegrowthyoy": "revenue_growth_yoy_percent",

            "financialhealthscore": "financial_health_score",
            "financialhealthrank(sector)": "financial_health_rank_sector",
            "financialhealthranksector": "financial_health_rank_sector",

            "targetprice(12m)": "target_price_12m",
            "targetprice12m": "target_price_12m",

            "expectedroi(1m)%": "expected_roi_1m_percent",
            "expectedroi1m%": "expected_roi_1m_percent",
            "expectedroi1m": "expected_roi_1m_percent",

            "expectedroi(3m)%": "expected_roi_3m_percent",
            "expectedroi3m%": "expected_roi_3m_percent",
            "expectedroi3m": "expected_roi_3m_percent",

            "expectedroi(12m)%": "expected_roi_12m_percent",
            "expectedroi12m%": "expected_roi_12m_percent",
            "expectedroi12m": "expected_roi_12m_percent",

            "expectedpricegrowth(12m)%": "expected_price_growth_12m_percent",
            "expectedpricegrowth12m%": "expected_price_growth_12m_percent",
            "expectedpricegrowth12m": "expected_price_growth_12m_percent",

            # Scores / reco
            "valuescore": "value_score",
            "qualityscore": "quality_score",
            "momentumscore": "momentum_score",
            "opportunityscore": "opportunity_score",
            "riskscore": "risk_score",
            "overallscore": "overall_score",
            "recommendation": "recommendation",
            "confidence": "confidence",

            # Meta
            "datasource": "data_source",
            "dataquality": "data_quality",
            "lastupdated(utc)": "last_updated_utc",
            "lastupdatedutc": "last_updated_utc",
            "lastupdated(riyadh)": "last_updated_riyadh",
            "lastupdatedriyadh": "last_updated_riyadh",
            "error": "error",
        }

        row: List[Any] = []
        for h in (headers or []):
            try:
                raw_h = (h or "").strip()
                if _contains_arabic(raw_h):
                    ak = _ar_key(raw_h)
                    if ak and ak in field_getters:
                        row.append(_clean_value(field_getters[ak]()))
                        continue

                k = _norm_header_en(raw_h)

                # extra shims
                if k == "52wposition":
                    k = "52wpositionpercent"
                if k == "turnover":
                    k = "turnoverpercent"
                if k == "lastupdatedlocal":
                    k = "lastupdatedriyadh"

                internal = en_alias.get(k)
                if internal and internal in field_getters:
                    row.append(_clean_value(field_getters[internal]()))
                    continue

                snake = _snake_from_header(raw_h)
                if snake and snake in field_getters:
                    row.append(_clean_value(field_getters[snake]()))
                    continue

                row.append(_clean_value(d.get(snake, "")) if snake else "")
            except Exception:
                row.append("")

        return row


__all__ = ["EnrichedQuote", "ENRICHED_VERSION"]
