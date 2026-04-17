#!/usr/bin/env python3
# core/sheets/schema_registry.py
"""
===============================================================================
Schema Registry — v3.4.0  (PER-PAGE SCHEMAS / WORLD-CLASS COLUMNS)
===============================================================================

Purpose
- Central, schema-first registry for all supported Google Sheets / API pages.
- Provides canonical page names, field definitions, display headers, required
  keys, row projection, validation, and data-dictionary generation.
- Designed to stay lightweight and import-safe for Render / FastAPI startup.
  No network I/O, no file I/O, no heavy runtime initialization.

v3.4.0 changes vs v3.0.0
--------------------------
PER-PAGE SCHEMAS  —  each of the 8 pages now has its own dedicated field set
and kind string matching the TFB Page Layout Proposal v1 column design.

  instrument_table       → Market_Leaders  (99 cols)
    New: price_change_5d, volume_ratio, roe, roa, rsi_signal, technical_score,
         day_range_position, atr_14, upside_pct, short_term_signal,
         recommendation_reason, confidence_bucket, invest_period_label,
         position_qty, avg_cost, position_cost, position_value,
         unrealized_pl, unrealized_pl_pct

  global_markets_table   → Global_Markets (112 cols)
    Adds all Market_Leaders new fields PLUS:
         region, market_status, price_usd,
         sector_pe_avg, vs_sector_pe_pct, sector_ytd_pct, sector_signal,
         sector_rank, sector_vs_msci_pct,
         vs_sp500_ytd, vs_msci_world_ytd, wall_st_target,
         upside_to_target_pct, analyst_consensus, country_risk,
         ma_50d, ma_200d, ema_signal, macd_signal

  commodities_table      → Commodities_FX (85 cols)
    Removes equity-only fields (P/E, EPS, margins, revenue etc.)
    Adds commodity-specific: commodity_type, contract_expiry, spot_price,
         usd_correlation, seasonal_signal, carry_rate
    Adds shared new: price_change_5d, volume_ratio, rsi_signal,
         technical_score, day_range_position, atr_14, upside_pct,
         short_term_signal, recommendation_reason, confidence_bucket,
         invest_period_label

  mutual_funds_table     → Mutual_Funds  (93 cols)
    Removes equity-only fundamentals (P/E, EPS, margins etc.)
    Adds fund-specific: fund_type, benchmark_name, holdings_count,
         aum, expense_ratio, nav, nav_premium_pct, distribution_yield,
         ytd_return, return_1y, return_3y_ann, return_5y_ann,
         tracking_error, alpha_1y
    Adds shared new: price_change_5d, volume_ratio, rsi_signal,
         technical_score, day_range_position, atr_14, upside_pct,
         short_term_signal, recommendation_reason, confidence_bucket,
         invest_period_label

  portfolio_table        → My_Portfolio  (110 cols)
    All 80 base fields PLUS all shared new fields PLUS full portfolio suite:
         position_qty, avg_cost, position_cost, position_value,
         unrealized_pl, unrealized_pl_pct, portfolio_weight_pct,
         target_weight_pct, weight_deviation, rebalance_signal,
         stop_loss, take_profit, distance_to_sl_pct, distance_to_tp_pct,
         days_held, annual_dividend_income, beta_contribution

  insights_analysis      → Insights_Analysis (9 cols)
    New: signal, priority (was 7 cols)

  top10_output           → Top_10_Investments (106 cols)
    Market_Leaders (99) + top10 extras (3) + trade setup (4):
         top10_rank, selection_reason, criteria_snapshot,
         entry_price, stop_loss_suggested, take_profit_suggested,
         risk_reward_ratio

  data_dictionary        → Data_Dictionary (9 cols — unchanged)

PAGE_ORDER  —  fixed: My_Portfolio moved from position 2 to position 5,
matching the canonical order used by page_catalog.py v3.4.0.

CANONICAL_SHEETS  —  new tuple consumed by page_catalog.py v3.4.0.

BACKWARD COMPATIBILITY  —  all public function signatures unchanged.
INSTRUMENT_FIELDS → alias for MARKET_LEADERS_FIELDS.
TOP10_EXTRA_FIELDS, INSIGHTS_FIELDS aliases preserved.
New helpers: list_sheets(), get_page_kind().
===============================================================================
"""

from dataclasses import dataclass, field
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


_VERSION = "3.4.0"

# Canonical page order — must match page_catalog.py CANONICAL_SHEETS
PAGE_ORDER: List[str] = [
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
    "My_Portfolio",
    "Insights_Analysis",
    "Top_10_Investments",
    "Data_Dictionary",
]

# Explicit tuple for page_catalog.py v3.4.0 CANONICAL_SHEETS
CANONICAL_SHEETS: Tuple[str, ...] = tuple(PAGE_ORDER)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _norm(value: Any) -> str:
    """Normalize page names / keys / headers into a comparison-safe token."""
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _assemble(*groups: Tuple["FieldSpec", ...]) -> Tuple["FieldSpec", ...]:
    """Concatenate field tuples, deduplicating by key (first occurrence wins)."""
    seen: set = set()
    out: List[FieldSpec] = []
    for grp in groups:
        for fs in grp:
            if fs.key not in seen:
                seen.add(fs.key)
                out.append(fs)
    return tuple(out)


# ─────────────────────────────────────────────────────────────────────────────
# Core data structures  (identical to v3.0.0 — no breaking changes)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FieldSpec:
    key: str
    header: str
    data_type: str = "string"
    required: bool = False
    description: str = ""
    group: str = ""
    format_hint: str = ""
    aliases: Tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "header": self.header,
            "data_type": self.data_type,
            "required": self.required,
            "description": self.description,
            "group": self.group,
            "format_hint": self.format_hint,
            "aliases": list(self.aliases),
        }

    def normalized_names(self) -> List[str]:
        names = [self.key, self.header, *self.aliases]
        return _dedupe_keep_order(_norm(x) for x in names if str(x or "").strip())


@dataclass(frozen=True)
class SheetSchema:
    page: str
    fields: Tuple[FieldSpec, ...]
    description: str = ""
    kind: str = "table"
    aliases: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def keys(self) -> List[str]:
        return [f.key for f in self.fields]

    @property
    def display_headers(self) -> List[str]:
        return [f.header for f in self.fields]

    @property
    def required_keys(self) -> List[str]:
        return [f.key for f in self.fields if f.required]

    @property
    def field_map(self) -> Dict[str, FieldSpec]:
        return {f.key: f for f in self.fields}

    @property
    def column_count(self) -> int:
        return len(self.fields)

    @property
    def all_page_aliases(self) -> List[str]:
        return _dedupe_keep_order([
            self.page,
            _norm(self.page),
            self.page.replace("_", " "),
            *self.aliases,
        ])

    def alias_lookup(self) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for fs in self.fields:
            for candidate in fs.normalized_names():
                mapping[candidate] = fs.key
        return mapping

    def as_sheet_spec(self, include_fields: bool = True) -> Dict[str, Any]:
        spec = {
            "page": self.page,
            "sheet_name": self.page,
            "description": self.description,
            "kind": self.kind,
            "column_count": self.column_count,
            "keys": self.keys,
            "headers": self.display_headers,
            "display_headers": self.display_headers,
            "required_keys": self.required_keys,
            "aliases": list(self.aliases),
        }
        if include_fields:
            spec["fields"] = [f.as_dict() for f in self.fields]
        return spec


def _f(
    key: str,
    header: str,
    data_type: str = "string",
    *,
    required: bool = False,
    description: str = "",
    group: str = "",
    format_hint: str = "",
    aliases: Sequence[str] = (),
) -> FieldSpec:
    return FieldSpec(
        key=key, header=header, data_type=data_type,
        required=required, description=description,
        group=group, format_hint=format_hint, aliases=tuple(aliases),
    )


def _schema(
    page: str,
    fields: Sequence[FieldSpec],
    *,
    aliases: Sequence[str] = (),
    description: str = "",
    kind: str = "table",
) -> SheetSchema:
    return SheetSchema(
        page=page, fields=tuple(fields),
        aliases=tuple(aliases), description=description, kind=kind,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SHARED FIELD BUILDING BLOCKS
# Assemble per-page schemas from these reusable tuples.
# ─────────────────────────────────────────────────────────────────────────────

# ── Identity ──────────────────────────────────────────────────────────────────
_IDENTITY: Tuple[FieldSpec, ...] = (
    _f("symbol",       "Symbol",     required=True, group="Identity",
       description="Ticker / trading symbol.", aliases=("ticker",)),
    _f("name",         "Name",       required=True, group="Identity",
       description="Instrument or fund name."),
    _f("asset_class",  "Asset Class",               group="Identity",
       description="Equity, ETF, commodity, FX, fund, etc."),
    _f("exchange",     "Exchange",                  group="Identity",
       description="Primary exchange or market."),
    _f("currency",     "Currency",                  group="Identity",
       description="Trading currency."),
    _f("country",      "Country",                   group="Identity",
       description="Country / market classification."),
    _f("sector",       "Sector",                    group="Identity",
       description="GICS sector classification."),
    _f("industry",     "Industry",                  group="Identity",
       description="GICS industry classification."),
)

# ── Price (base) ──────────────────────────────────────────────────────────────
_PRICE_BASE: Tuple[FieldSpec, ...] = (
    _f("current_price",    "Current Price",  "number", required=True,
       group="Price", format_hint="0.00", aliases=("price", "last_price"),
       description="Latest traded / computed price."),
    _f("previous_close",   "Previous Close", "number",
       group="Price", format_hint="0.00",
       description="Prior session close."),
    _f("open",             "Open",           "number",
       group="Price", format_hint="0.00",
       description="Current session open.", aliases=("open_price",)),
    _f("day_high",         "Day High",       "number",
       group="Price", format_hint="0.00"),
    _f("day_low",          "Day Low",        "number",
       group="Price", format_hint="0.00"),
    _f("week_52_high",     "52W High",       "number",
       group="Price", format_hint="0.00", aliases=("52w_high", "year_high")),
    _f("week_52_low",      "52W Low",        "number",
       group="Price", format_hint="0.00", aliases=("52w_low", "year_low")),
    _f("price_change",     "Price Change",   "number",
       group="Price", format_hint="0.00"),
    _f("percent_change",   "Change %",       "percent",
       group="Price", format_hint="0.00%",
       description="Price movement percentage."),
    _f("position_52w_pct", "52W Position %", "percent",
       group="Price", format_hint="0.00%",
       description="Position between 52W high and low.",
       aliases=("52w_position_pct", "week_52_position_pct")),
)

# ── Price (new additions) ─────────────────────────────────────────────────────
_PRICE_NEW: Tuple[FieldSpec, ...] = (
    _f("price_change_5d",  "5D Change %",    "percent",
       group="Price", format_hint="0.00%",
       description="5-day price return — short-term momentum signal."),
)

# ── Volume / Size ─────────────────────────────────────────────────────────────
_VOLUME: Tuple[FieldSpec, ...] = (
    _f("volume",           "Volume",         "number",
       group="Liquidity", format_hint="#,##0",
       description="Current or latest volume."),
    _f("avg_volume_10d",   "Avg Vol 10D",    "number",
       group="Liquidity", format_hint="#,##0"),
    _f("avg_volume_30d",   "Avg Vol 30D",    "number",
       group="Liquidity", format_hint="#,##0"),
    _f("market_cap",       "Market Cap",     "number",
       group="Liquidity", format_hint="#,##0"),
    _f("float_shares",     "Float Shares",   "number",
       group="Liquidity", format_hint="#,##0"),
    _f("volume_ratio",     "Volume Ratio",   "number",
       group="Liquidity", format_hint="0.00",
       description="volume / avg_volume_10d. >1.5 = unusual activity surge."),
)

# ── Fundamentals — equity ─────────────────────────────────────────────────────
_FUNDAMENTALS_EQUITY: Tuple[FieldSpec, ...] = (
    _f("beta_5y",               "Beta (5Y)",          "number",
       group="Fundamentals", format_hint="0.00"),
    _f("pe_ttm",                "P/E (TTM)",          "number",
       group="Fundamentals", format_hint="0.00",
       description="Trailing price-to-earnings."),
    _f("pe_forward",            "P/E (Fwd)",          "number",
       group="Fundamentals", format_hint="0.00",
       description="Forward price-to-earnings."),
    _f("eps_ttm",               "EPS (TTM)",          "number",
       group="Fundamentals", format_hint="0.00"),
    _f("dividend_yield",        "Div Yield %",        "percent",
       group="Fundamentals", format_hint="0.00%"),
    _f("payout_ratio",          "Payout Ratio %",     "percent",
       group="Fundamentals", format_hint="0.00%"),
    _f("revenue_ttm",           "Revenue TTM",        "number",
       group="Fundamentals", format_hint="#,##0"),
    _f("revenue_growth_yoy",    "Rev Growth YoY %",   "percent",
       group="Fundamentals", format_hint="0.00%"),
    _f("gross_margin",          "Gross Margin %",     "percent",
       group="Fundamentals", format_hint="0.00%"),
    _f("operating_margin",      "Op Margin %",        "percent",
       group="Fundamentals", format_hint="0.00%"),
    _f("profit_margin",         "Net Margin %",       "percent",
       group="Fundamentals", format_hint="0.00%"),
    _f("debt_to_equity",        "D/E Ratio",          "number",
       group="Fundamentals", format_hint="0.00"),
    _f("free_cash_flow_ttm",    "FCF (TTM)",          "number",
       group="Fundamentals", format_hint="#,##0"),
)

# ── Fundamentals — quality (new) ──────────────────────────────────────────────
_FUNDAMENTALS_QUALITY: Tuple[FieldSpec, ...] = (
    _f("roe", "ROE %",  "percent",
       group="Fundamentals", format_hint="0.00%",
       description="Return on equity = net_income / shareholders_equity. "
                   "Sourced from yahoo_fundamentals or EODHD."),
    _f("roa", "ROA %",  "percent",
       group="Fundamentals", format_hint="0.00%",
       description="Return on assets = net_income / total_assets."),
)

# ── Fundamentals — minimal (non-equity pages) ─────────────────────────────────
_FUNDAMENTALS_MINIMAL: Tuple[FieldSpec, ...] = (
    _f("beta_5y",        "Beta (5Y)",   "number",
       group="Fundamentals", format_hint="0.00"),
    _f("dividend_yield", "Div Yield %", "percent",
       group="Fundamentals", format_hint="0.00%",
       description="Distribution / dividend yield where applicable."),
)

# ── Risk ──────────────────────────────────────────────────────────────────────
_RISK: Tuple[FieldSpec, ...] = (
    _f("rsi_14",        "RSI (14)",       "number",
       group="Risk", format_hint="0.00",
       description="14-period RSI."),
    _f("volatility_30d","Volatility 30D %","percent",
       group="Risk", format_hint="0.00%"),
    _f("volatility_90d","Volatility 90D %","percent",
       group="Risk", format_hint="0.00%"),
    _f("max_drawdown_1y","Max DD 1Y %",   "percent",
       group="Risk", format_hint="0.00%"),
    _f("var_95_1d",     "VaR 95% (1D)",  "percent",
       group="Risk", format_hint="0.00%"),
    _f("sharpe_1y",     "Sharpe (1Y)",   "number",
       group="Risk", format_hint="0.00"),
    _f("risk_score",    "Risk Score",    "number",
       group="Risk", format_hint="0.00"),
    _f("risk_bucket",   "Risk Bucket",
       group="Risk",
       description="Low / Moderate / High."),
)

# ── Technical signals (new) ───────────────────────────────────────────────────
_TECHNICALS_NEW: Tuple[FieldSpec, ...] = (
    _f("rsi_signal",       "RSI Signal",     group="Technicals",
       description="Oversold / Neutral / Overbought derived from rsi_14."),
    _f("technical_score",  "Tech Score",     "number",
       group="Technicals", format_hint="0.00",
       description="Composite short-term technical signal 0-100. "
                   "Formula: 0.40×RSI_zone + 0.30×volume_ratio_score + "
                   "0.30×day_range_position."),
    _f("day_range_position","Day Range Pos %","percent",
       group="Technicals", format_hint="0.00%",
       description="(current_price − day_low) / (day_high − day_low). "
                   "Near 0 = bottom of range (buy zone), near 1 = top."),
    _f("atr_14",           "ATR 14",         "number",
       group="Technicals", format_hint="0.00",
       description="14-period Average True Range. Used for stop-loss sizing. "
                   "Sourced from TwelveData."),
)

# ── EODHD moving average technicals (Global_Markets only) ────────────────────
_EODHD_TECHNICALS: Tuple[FieldSpec, ...] = (
    _f("ma_50d",    "50D MA",      "number",
       group="EODHD Technicals", format_hint="0.00",
       description="50-day simple moving average from EODHD."),
    _f("ma_200d",   "200D MA",     "number",
       group="EODHD Technicals", format_hint="0.00",
       description="200-day simple moving average from EODHD."),
    _f("ema_signal","EMA Signal",
       group="EODHD Technicals",
       description="Above / Below — price position relative to 50D EMA."),
    _f("macd_signal","MACD Signal",
       group="EODHD Technicals",
       description="Bullish / Neutral / Bearish from MACD(12,26,9) crossover."),
)

# ── Valuation ─────────────────────────────────────────────────────────────────
_VALUATION: Tuple[FieldSpec, ...] = (
    _f("pb",             "P/B",           "number",
       group="Valuation", format_hint="0.00",
       aliases=("pb_ratio",)),
    _f("ps",             "P/S",           "number",
       group="Valuation", format_hint="0.00",
       aliases=("ps_ratio",)),
    _f("ev_to_ebitda",   "EV/EBITDA",     "number",
       group="Valuation", format_hint="0.00",
       aliases=("ev_ebitda",)),
    _f("peg",            "PEG Ratio",     "number",
       group="Valuation", format_hint="0.00",
       aliases=("peg_ratio",)),
    _f("intrinsic_value","Intrinsic Value","number",
       group="Valuation", format_hint="0.00",
       description="Model-derived intrinsic / fair value estimate."),
    _f("valuation_score","Valuation Score","number",
       group="Valuation", format_hint="0.00"),
    _f("upside_pct",     "Upside %",      "percent",
       group="Valuation", format_hint="0.00%",
       description="(intrinsic_value − current_price) / current_price. "
                   "Positive = trading below intrinsic value."),
)

# ── Forecast ──────────────────────────────────────────────────────────────────
_FORECAST: Tuple[FieldSpec, ...] = (
    _f("forecast_price_1m", "Price Tgt 1M",  "number",
       group="Forecast", format_hint="0.00"),
    _f("forecast_price_3m", "Price Tgt 3M",  "number",
       group="Forecast", format_hint="0.00"),
    _f("forecast_price_12m","Price Tgt 12M", "number",
       group="Forecast", format_hint="0.00"),
    _f("expected_roi_1m",   "ROI 1M %",      "percent",
       group="Forecast", format_hint="0.00%"),
    _f("expected_roi_3m",   "ROI 3M %",      "percent",
       group="Forecast", format_hint="0.00%"),
    _f("expected_roi_12m",  "ROI 12M %",     "percent",
       group="Forecast", format_hint="0.00%"),
    _f("forecast_confidence","AI Confidence", "number",
       group="Forecast", format_hint="0.00"),
    _f("confidence",        "Confidence Score","number",
       group="Forecast", format_hint="0.00"),
)

# ── Scoring ───────────────────────────────────────────────────────────────────
_SCORING: Tuple[FieldSpec, ...] = (
    _f("opportunity_score","Opportunity Score","number",
       group="Scoring", format_hint="0.00"),
    _f("overall_score",    "Overall Score",    "number",
       group="Scoring", format_hint="0.00"),
    _f("quality_score",    "Quality Score",    "number",
       group="Scoring", format_hint="0.00"),
    _f("growth_score",     "Growth Score",     "number",
       group="Scoring", format_hint="0.00"),
    _f("value_score",      "Value Score",      "number",
       group="Scoring", format_hint="0.00"),
    _f("momentum_score",   "Momentum Score",   "number",
       group="Scoring", format_hint="0.00"),
)

# ── Decision / Recommendation ─────────────────────────────────────────────────
_DECISION: Tuple[FieldSpec, ...] = (
    _f("analyst_rating",      "Analyst Rating",
       group="Decision",
       description="Consensus analyst rating."),
    _f("target_price",        "Target Price",      "number",
       group="Decision", format_hint="0.00"),
    _f("upside_downside_pct", "Upside/Downside %", "percent",
       group="Decision", format_hint="0.00%"),
    _f("recommendation",      "Recommendation",
       group="Decision",
       description="STRONG_BUY / BUY / HOLD / REDUCE / SELL.",
       aliases=("recommend",)),
    _f("signal",              "Signal",
       group="Decision"),
    _f("trend_1m",            "Trend 1M",
       group="Decision"),
    _f("trend_3m",            "Trend 3M",
       group="Decision"),
    _f("trend_12m",           "Trend 12M",
       group="Decision"),
)

# ── New recommendation fields ─────────────────────────────────────────────────
_RECOMMENDATION_NEW: Tuple[FieldSpec, ...] = (
    _f("short_term_signal",   "ST Signal",
       group="Decision",
       description="Day / week direction: BUY / HOLD / SELL. "
                   "Based on technical_score thresholds."),
    _f("recommendation_reason","Reason",
       group="Decision",
       description="One-line explanation for the recommendation."),
    _f("confidence_bucket",   "Confidence",
       group="Decision",
       description="High / Medium / Low confidence bucket."),
    _f("invest_period_label", "Horizon",
       group="Decision",
       description="1D / 1W / 1M / 3M / 12M investment horizon label."),
)

# ── Provenance ────────────────────────────────────────────────────────────────
_PROVENANCE: Tuple[FieldSpec, ...] = (
    _f("provider_primary",  "Provider",            group="Provenance",
       aliases=("provider", "data_provider"),
       description="Primary data provider for this row."),
    _f("provider_secondary","Provider Secondary",  group="Provenance"),
    _f("data_quality_score","Data Quality Score",  "number",
       group="Provenance", format_hint="0.00",
       description="Row-level data completeness / quality score."),
    _f("as_of_utc",         "As Of (UTC)",        "datetime", group="Provenance"),
    _f("as_of_riyadh",      "As Of (Riyadh)",     "datetime", group="Provenance",
       aliases=("updated_at_riyadh",)),
    _f("updated_at_utc",    "Updated At (UTC)",   "datetime", group="Provenance"),
    _f("updated_at_riyadh", "Updated At (Riyadh)","datetime", group="Provenance"),
    _f("row_source",        "Row Source",          group="Provenance",
       description="Source pipeline / builder used for the row."),
)

# ── Portfolio — basic P&L ─────────────────────────────────────────────────────
_PORTFOLIO_BASIC: Tuple[FieldSpec, ...] = (
    _f("position_qty",    "Qty",             "number",
       group="Portfolio", format_hint="0.00",
       description="Position quantity."),
    _f("avg_cost",        "Avg Cost",        "number",
       group="Portfolio", format_hint="0.00",
       description="Average cost per unit."),
    _f("position_cost",   "Position Cost",   "number",
       group="Portfolio", format_hint="0.00",
       description="position_qty × avg_cost."),
    _f("position_value",  "Position Value",  "number",
       group="Portfolio", format_hint="0.00",
       description="position_qty × current_price."),
    _f("unrealized_pl",   "Unrealized P/L",  "number",
       group="Portfolio", format_hint="0.00",
       description="position_value − position_cost."),
    _f("unrealized_pl_pct","Unrealized P/L %","percent",
       group="Portfolio", format_hint="0.00%"),
)

# ── Portfolio — management (My_Portfolio only) ────────────────────────────────
_PORTFOLIO_ADVANCED: Tuple[FieldSpec, ...] = (
    _f("portfolio_weight_pct", "Weight %",          "percent",
       group="Portfolio Management", format_hint="0.00%",
       description="position_value / total_portfolio_value."),
    _f("target_weight_pct",    "Target Weight %",   "percent",
       group="Portfolio Management", format_hint="0.00%",
       description="User-defined target allocation percentage."),
    _f("weight_deviation",     "Weight Deviation %","percent",
       group="Portfolio Management", format_hint="0.00%",
       description="actual_weight − target_weight. Triggers rebalance signal."),
    _f("rebalance_signal",     "Rebal Signal",
       group="Portfolio Management",
       description="Add / On-Target / Trim based on weight deviation."),
    _f("stop_loss",            "Stop Loss",         "number",
       group="Risk Management", format_hint="0.00",
       description="User-defined stop loss price (set in Google Sheet)."),
    _f("take_profit",          "Take Profit",       "number",
       group="Risk Management", format_hint="0.00",
       description="User-defined take profit price (set in Google Sheet)."),
    _f("distance_to_sl_pct",   "Dist to SL %",      "percent",
       group="Risk Management", format_hint="0.00%",
       description="(current_price − stop_loss) / current_price."),
    _f("distance_to_tp_pct",   "Dist to TP %",      "percent",
       group="Risk Management", format_hint="0.00%",
       description="(take_profit − current_price) / current_price."),
    _f("days_held",            "Days Held",         "number",
       group="Portfolio Management", format_hint="0",
       description="Days since first purchase. Affects capital gains tax."),
    _f("annual_dividend_income","Ann Div Income",   "number",
       group="Portfolio Management", format_hint="0.00",
       description="position_qty × current_price × dividend_yield."),
    _f("beta_contribution",    "Beta Contribution", "number",
       group="Risk Management", format_hint="0.00",
       description="portfolio_weight_pct × beta_5y. Portfolio risk attribution."),
)

# ── Global context (Global_Markets only) ─────────────────────────────────────
_GLOBAL_CONTEXT: Tuple[FieldSpec, ...] = (
    _f("region",        "Region",        group="Global Context",
       description="Americas / Europe / Asia-Pacific / MENA / Global EM."),
    _f("market_status", "Market Status", group="Global Context",
       description="Open / Closed / Pre-Market based on exchange timezone."),
    _f("price_usd",     "Price (USD)",   "number",
       group="Global Context", format_hint="0.00",
       description="Price converted to USD for cross-border comparison."),
)

# ── Sector analysis (Global_Markets only) ────────────────────────────────────
_SECTOR_ANALYSIS: Tuple[FieldSpec, ...] = (
    _f("sector_pe_avg",    "Sector P/E Avg",  "number",
       group="Sector Analysis", format_hint="0.00",
       description="Average P/E of the stock's own GICS sector. "
                   "Sourced from EODHD sector endpoint."),
    _f("vs_sector_pe_pct", "vs Sector P/E %", "percent",
       group="Sector Analysis", format_hint="0.00%",
       description="(stock_PE − sector_PE) / sector_PE × 100. "
                   "Negative = trading at discount (opportunity). "
                   "Positive = premium (justify with quality)."),
    _f("sector_ytd_pct",   "Sector YTD %",    "percent",
       group="Sector Analysis", format_hint="0.00%",
       description="Sector year-to-date return. Indicates sector money flow."),
    _f("sector_signal",    "Sector Signal",
       group="Sector Analysis",
       description="Bullish / Neutral / Bearish based on sector momentum."),
    _f("sector_rank",      "Sector Rank",     "number",
       group="Sector Analysis", format_hint="0",
       description="Stock rank within its sector universe. 1 = top performer."),
    _f("sector_vs_msci_pct","Sector vs MSCI %","percent",
       group="Sector Analysis", format_hint="0.00%",
       description="Sector YTD vs MSCI World. Positive = sector outperforming."),
)

# ── Comparative performance (Global_Markets only) ─────────────────────────────
_COMPARATIVE: Tuple[FieldSpec, ...] = (
    _f("vs_sp500_ytd",        "vs S&P 500 YTD %",   "percent",
       group="Comparative", format_hint="0.00%",
       description="YTD return relative to S&P 500."),
    _f("vs_msci_world_ytd",   "vs MSCI World %",    "percent",
       group="Comparative", format_hint="0.00%",
       description="YTD return relative to MSCI World index."),
    _f("wall_st_target",      "Wall St Target",     "number",
       group="Comparative", format_hint="0.00",
       description="Analyst consensus 12-month price target. EODHD Highlights."),
    _f("upside_to_target_pct","Upside to Tgt %",    "percent",
       group="Comparative", format_hint="0.00%",
       description="(wall_st_target − current_price) / current_price."),
    _f("analyst_consensus",   "Analyst Consensus",
       group="Comparative",
       description="Buy / Neutral / Sell consensus from analyst community."),
    _f("country_risk",        "Country Risk",
       group="Comparative",
       description="Investment Grade / Speculative Grade / Mixed."),
)

# ── Commodity-specific (Commodities_FX only) ──────────────────────────────────
_COMMODITY_SPECIFIC: Tuple[FieldSpec, ...] = (
    _f("commodity_type",  "Asset Type",       group="Asset Info",
       description="Metal / Energy / FX Pair / Agriculture / Crypto."),
    _f("contract_expiry", "Contract Expiry",  group="Asset Info",
       description="Futures contract expiry (e.g. Jun-2026). "
                   "Spot instruments = Spot."),
    _f("spot_price",      "Spot Price",  "number",
       group="Asset Info", format_hint="0.00",
       description="Underlying spot price where applicable."),
    _f("usd_correlation", "USD Correlation",  "number",
       group="Commodity Signals", format_hint="0.00",
       description="90-day correlation with DXY (USD index). "
                   "Negative = inverse USD relationship (e.g. Gold)."),
    _f("seasonal_signal", "Seasonal Signal",
       group="Commodity Signals",
       description="Historical seasonal pattern for current month: "
                   "Bullish / Bearish / Neutral."),
    _f("carry_rate",      "Carry Rate %",     "percent",
       group="Commodity Signals", format_hint="0.00%",
       description="Interest rate differential for FX pairs. "
                   "Positive = long-carry trade favourable."),
)

# ── Fund-specific (Mutual_Funds only) ─────────────────────────────────────────
_FUND_INFO: Tuple[FieldSpec, ...] = (
    _f("fund_type",       "Fund Type",
       group="Fund Info",
       description="ETF / Open-End / Closed-End / REIT."),
    _f("benchmark_name",  "Benchmark",
       group="Fund Info",
       description="Index the fund tracks (e.g. S&P 500, NASDAQ-100)."),
    _f("holdings_count",  "Holdings Count",   "number",
       group="Fund Info", format_hint="0",
       description="Number of holdings in the fund."),
)

_FUND_PRICING: Tuple[FieldSpec, ...] = (
    _f("aum",                "AUM (B USD)",        "number",
       group="Fund Pricing", format_hint="0.00",
       description="Assets under management in USD billions. yahoo_fundamentals."),
    _f("expense_ratio",      "Expense Ratio %",    "percent",
       group="Fund Pricing", format_hint="0.000%",
       description="Annual expense ratio. <0.10% = very low, >1% = high."),
    _f("nav",                "NAV",                "number",
       group="Fund Pricing", format_hint="0.00",
       description="Net asset value per share."),
    _f("nav_premium_pct",    "NAV Prem/Disc %",    "percent",
       group="Fund Pricing", format_hint="0.00%",
       description="(price − nav) / nav. "
                   "Negative = discount (buy signal for closed-end funds)."),
    _f("distribution_yield", "Distribution Yield %","percent",
       group="Fund Pricing", format_hint="0.00%"),
)

_FUND_PERFORMANCE: Tuple[FieldSpec, ...] = (
    _f("ytd_return",      "YTD Return %",      "percent",
       group="Fund Performance", format_hint="0.00%"),
    _f("return_1y",       "1Y Return %",       "percent",
       group="Fund Performance", format_hint="0.00%"),
    _f("return_3y_ann",   "3Y Return (Ann) %", "percent",
       group="Fund Performance", format_hint="0.00%",
       description="3-year annualized total return."),
    _f("return_5y_ann",   "5Y Return (Ann) %", "percent",
       group="Fund Performance", format_hint="0.00%",
       description="5-year annualized total return."),
    _f("tracking_error",  "Tracking Error %",  "percent",
       group="Fund Performance", format_hint="0.00%",
       description="How closely the ETF tracks its benchmark index."),
    _f("alpha_1y",        "Alpha (1Y)",        "number",
       group="Fund Performance", format_hint="0.00",
       description="Excess return vs benchmark over 1 year. "
                   "Positive = manager / structure adds value."),
)

# ── Top10 extras ──────────────────────────────────────────────────────────────
_TOP10_EXTRAS: Tuple[FieldSpec, ...] = (
    _f("top10_rank",      "Top 10 Rank", "number", required=True,
       group="Top10", format_hint="0",
       description="Rank within the Top 10 selection (1 = best)."),
    _f("selection_reason","Selection Reason", required=True,
       group="Top10",
       description="Why this instrument entered the Top 10."),
    _f("criteria_snapshot","Criteria Snapshot",
       group="Top10",
       description="Compact criteria / score snapshot at selection time."),
)

# ── Trade setup (Top_10_Investments only) ─────────────────────────────────────
_TRADE_SETUP: Tuple[FieldSpec, ...] = (
    _f("entry_price",           "Entry Price",      "number",
       group="Trade Setup", format_hint="0.00",
       description="AI-suggested entry price based on technical support zone."),
    _f("stop_loss_suggested",   "Stop Loss (AI)",   "number",
       group="Trade Setup", format_hint="0.00",
       description="AI stop loss = entry − (atr_14 × 1.5). "
                   "Limits downside to ~1.5 ATR."),
    _f("take_profit_suggested", "Take Profit (AI)", "number",
       group="Trade Setup", format_hint="0.00",
       description="AI take profit = entry + (upside_pct × entry). "
                   "Uses intrinsic value gap as target."),
    _f("risk_reward_ratio",     "Risk/Reward",      "number",
       group="Trade Setup", format_hint="0.00",
       description="(take_profit − entry) / (entry − stop_loss). "
                   "Only include picks where R/R ≥ 1.5."),
)


# ─────────────────────────────────────────────────────────────────────────────
# PER-PAGE FIELD SETS
# ─────────────────────────────────────────────────────────────────────────────

# ── Market_Leaders (99 cols) ─────────────────────────────────────────────────
# Full equity schema: all base 80 fields + new technical/quality/recommendation
# fields + portfolio_basic (light P&L for watchlist view).
MARKET_LEADERS_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_EQUITY, _FUNDAMENTALS_QUALITY,
    _RISK, _TECHNICALS_NEW,
    _VALUATION,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PORTFOLIO_BASIC,
    _PROVENANCE,
)

# ── Global_Markets (112 cols) ────────────────────────────────────────────────
# International equity: adds global context + sector analysis + comparative
# performance + EODHD technicals. Keeps full equity fundamentals (indices and
# ETFs still expose aggregated P/E, revenue etc from EODHD).
GLOBAL_MARKETS_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _GLOBAL_CONTEXT,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_EQUITY, _FUNDAMENTALS_QUALITY,
    _RISK, _TECHNICALS_NEW, _EODHD_TECHNICALS,
    _VALUATION,
    _SECTOR_ANALYSIS,
    _COMPARATIVE,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PROVENANCE,
)

# ── Commodities_FX (85 cols) ─────────────────────────────────────────────────
# Commodity / FX schema: removes equity-only fundamentals (P/E, EPS, margins,
# revenue, payout etc. have zero meaning for Gold, Oil, EUR/USD).
# Adds commodity-specific signals: USD correlation, seasonal, carry rate.
COMMODITIES_FX_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _COMMODITY_SPECIFIC,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_MINIMAL,   # beta_5y + div_yield only
    _RISK, _TECHNICALS_NEW,
    _VALUATION,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PROVENANCE,
)

# ── Mutual_Funds (93 cols) ───────────────────────────────────────────────────
# Fund schema: removes equity-only fundamentals (P/E, EPS, revenue, margins
# are aggregated numbers that add little value for fund selection decisions).
# Adds fund-specific data: AUM, expense ratio, NAV premium, multi-period
# returns, tracking error, alpha.
MUTUAL_FUNDS_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _FUND_INFO, _FUND_PRICING, _FUND_PERFORMANCE,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_MINIMAL,   # beta_5y + div_yield only
    _RISK, _TECHNICALS_NEW,
    _VALUATION,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PROVENANCE,
)

# ── My_Portfolio (110 cols) ──────────────────────────────────────────────────
# Full portfolio management schema: all equity fields + complete portfolio
# tracking suite (stop/target, weight allocation, rebalancing, risk attribution).
MY_PORTFOLIO_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    _IDENTITY,
    _PRICE_BASE, _PRICE_NEW,
    _VOLUME,
    _FUNDAMENTALS_EQUITY, _FUNDAMENTALS_QUALITY,
    _RISK, _TECHNICALS_NEW,
    _VALUATION,
    _FORECAST, _SCORING,
    _DECISION, _RECOMMENDATION_NEW,
    _PORTFOLIO_BASIC, _PORTFOLIO_ADVANCED,
    _PROVENANCE,
)

# ── Insights_Analysis (9 cols) ───────────────────────────────────────────────
# AI intelligence dashboard — curated insight rows.
# v3.4.0: adds signal (BUY/HOLD/SELL/ALERT/INFO) and priority (High/Med/Low).
INSIGHTS_ANALYSIS_FIELDS: Tuple[FieldSpec, ...] = (
    _f("section",       "Section",   required=True, group="Insight",
       description="Market Summary / Top Picks / Risk Alerts / "
                   "Short-Term Opportunities / Portfolio KPIs / Macro Signals."),
    _f("metric",        "Metric",    required=True, group="Insight",
       description="Metric or KPI being reported."),
    _f("value",         "Value",                    group="Insight",
       description="Metric value (Apps Script friendly string)."),
    _f("unit",          "Unit",                     group="Insight"),
    _f("direction",     "Direction",                group="Insight",
       description="Positive / Negative / Neutral."),
    _f("signal",        "Signal",                   group="Insight",
       description="BUY / SELL / HOLD / ALERT / INFO — at-a-glance action."),
    _f("priority",      "Priority",                 group="Insight",
       description="High / Medium / Low — what to act on first."),
    _f("commentary",    "Commentary",               group="Insight",
       description="Narrative explanation / supporting data."),
    _f("updated_at",    "Updated At", "datetime",   group="Provenance"),
)

# ── Top_10_Investments (106 cols) ────────────────────────────────────────────
# Full Market_Leaders schema + Top10 rank/reason/criteria + trade setup
# (entry price, AI stop loss, AI take profit, risk/reward ratio).
TOP10_INVESTMENTS_FIELDS: Tuple[FieldSpec, ...] = _assemble(
    MARKET_LEADERS_FIELDS,
    _TOP10_EXTRAS,
    _TRADE_SETUP,
)

# ── Data_Dictionary (9 cols — unchanged) ─────────────────────────────────────
DATA_DICTIONARY_FIELDS: Tuple[FieldSpec, ...] = (
    _f("page",           "Page",           required=True, group="Dictionary",
       description="Page / sheet name."),
    _f("field_key",      "Field Key",      required=True, group="Dictionary",
       description="Canonical field key used in code and API."),
    _f("display_header", "Display Header", required=True, group="Dictionary",
       description="User-facing column header shown in Google Sheets."),
    _f("data_type",      "Data Type",      required=True, group="Dictionary",
       description="Expected data type: string / number / percent / datetime / boolean."),
    _f("group",          "Group",                                 group="Dictionary",
       description="Logical column grouping."),
    _f("required",       "Required",                              group="Dictionary",
       description="Whether the field is required for the page to function."),
    _f("description",    "Description",                           group="Dictionary",
       description="Field description, business meaning, and derivation notes."),
    _f("format_hint",    "Format Hint",                           group="Dictionary",
       description="Suggested Google Sheets format (e.g. 0.00%, #,##0)."),
    _f("aliases",        "Aliases",                               group="Dictionary",
       description="Alternative accepted input key names for this field."),
)

# ─────────────────────────────────────────────────────────────────────────────
# BACKWARD COMPATIBILITY ALIASES
# ─────────────────────────────────────────────────────────────────────────────

# v3.0.0 callers that import INSTRUMENT_FIELDS get Market_Leaders fields
INSTRUMENT_FIELDS: Tuple[FieldSpec, ...] = MARKET_LEADERS_FIELDS

# v3.0.0 callers that import INSIGHTS_FIELDS
INSIGHTS_FIELDS: Tuple[FieldSpec, ...] = INSIGHTS_ANALYSIS_FIELDS

# v3.0.0 callers that import TOP10_EXTRA_FIELDS
TOP10_EXTRA_FIELDS: Tuple[FieldSpec, ...] = _TOP10_EXTRAS


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_REGISTRY: Dict[str, SheetSchema] = {
    "Market_Leaders": _schema(
        "Market_Leaders",
        MARKET_LEADERS_FIELDS,
        kind="instrument_table",
        aliases=(
            "market leaders", "marketleaders", "market_leaders",
            "ksa", "ksa_shares", "local", "local_shares",
            "saudi_shares", "tadawul", "saudi leaders", "tasi leaders",
        ),
        description="Primary Saudi & global equity leaders — "
                    "full AI scoring + short-term signals. (99 cols)",
    ),
    "Global_Markets": _schema(
        "Global_Markets",
        GLOBAL_MARKETS_FIELDS,
        kind="global_markets_table",
        aliases=(
            "global markets", "global_markets", "global",
            "international_markets", "world_markets", "international",
        ),
        description="International equity universe — EODHD-enhanced + "
                    "sector analysis + comparative performance. (112 cols)",
    ),
    "Commodities_FX": _schema(
        "Commodities_FX",
        COMMODITIES_FX_FIELDS,
        kind="commodities_table",
        aliases=(
            "commodities fx", "commodities_fx", "commodities",
            "fx", "commodities_and_fx", "commodities & fx",
            "metals", "gold", "oil",
        ),
        description="Commodities & FX pairs — commodity signals, "
                    "carry rate, USD correlation. (85 cols)",
    ),
    "Mutual_Funds": _schema(
        "Mutual_Funds",
        MUTUAL_FUNDS_FIELDS,
        kind="mutual_funds_table",
        aliases=(
            "mutual funds", "mutual_funds", "funds",
            "mutualfunds", "etfs", "etf",
            "index funds", "investment funds",
        ),
        description="ETFs & mutual funds — AUM, expense ratio, "
                    "NAV premium, multi-period returns. (93 cols)",
    ),
    "My_Portfolio": _schema(
        "My_Portfolio",
        MY_PORTFOLIO_FIELDS,
        kind="portfolio_table",
        aliases=(
            "my portfolio", "my_portfolio", "portfolio",
            "my investments", "my_investments", "investments",
            "holdings", "positions",
        ),
        description="User portfolio — P&L, stop/target, "
                    "weight allocation, rebalancing signals. (110 cols)",
    ),
    "Insights_Analysis": _schema(
        "Insights_Analysis",
        INSIGHTS_ANALYSIS_FIELDS,
        kind="insights_analysis",
        aliases=(
            "insights analysis", "insights_analysis", "insights",
            "analysis", "analysis_insights",
            "ai insights", "market insights",
        ),
        description="AI market intelligence — top picks, risk alerts, "
                    "portfolio KPIs, macro signals. (9 cols)",
    ),
    "Top_10_Investments": _schema(
        "Top_10_Investments",
        TOP10_INVESTMENTS_FIELDS,
        kind="top10_output",
        aliases=(
            "top 10 investments", "top_10_investments",
            "top10", "top_10", "top10_investments",
            "top ten investments", "best picks", "top picks",
        ),
        description="AI-selected top 10 — ranked with entry price, "
                    "stop loss, take profit, R/R ratio. (106 cols)",
    ),
    "Data_Dictionary": _schema(
        "Data_Dictionary",
        DATA_DICTIONARY_FIELDS,
        kind="data_dictionary",
        aliases=(
            "data dictionary", "data_dictionary", "dictionary",
            "schema_dictionary", "column reference", "schema reference",
        ),
        description="Schema reference — all column definitions, "
                    "sources and formats. (9 cols)",
    ),
}

# ─────────────────────────────────────────────────────────────────────────────
# Page aliases index
# ─────────────────────────────────────────────────────────────────────────────

PAGE_ALIASES: Dict[str, str] = {}
for _canonical, _schema_obj in SCHEMA_REGISTRY.items():
    for _alias in _schema_obj.all_page_aliases:
        PAGE_ALIASES[_norm(_alias)] = _canonical

# Extra hand-written aliases for common variations
for _alias, _target in [
    ("My_Investments",     "My_Portfolio"),
    ("my_investments",     "My_Portfolio"),
    ("Market Leaders",     "Market_Leaders"),
    ("Global Markets",     "Global_Markets"),
    ("Commodities FX",     "Commodities_FX"),
    ("Mutual Funds",       "Mutual_Funds"),
    ("My Portfolio",       "My_Portfolio"),
    ("Top 10",             "Top_10_Investments"),
    ("Data Dictionary",    "Data_Dictionary"),
]:
    PAGE_ALIASES[_norm(_alias)] = _target

SUPPORTED_PAGES: List[str] = [p for p in PAGE_ORDER if p in SCHEMA_REGISTRY]


# ─────────────────────────────────────────────────────────────────────────────
# Public API  (all signatures identical to v3.0.0)
# ─────────────────────────────────────────────────────────────────────────────

def get_version() -> str:
    return _VERSION


def get_supported_pages() -> List[str]:
    return list(SUPPORTED_PAGES)


def list_pages() -> List[str]:
    return get_supported_pages()


def list_sheets() -> List[str]:
    """Alias consumed by page_catalog.py v3.4.0."""
    return get_supported_pages()


def has_page(page: Optional[str]) -> bool:
    return normalize_page_name(page) in SCHEMA_REGISTRY


def page_exists(page: Optional[str]) -> bool:
    return has_page(page)


def normalize_page_name(page: Optional[str]) -> str:
    canonical = PAGE_ALIASES.get(_norm(page))
    if canonical:
        return canonical
    raw = str(page or "").strip()
    return raw if raw in SCHEMA_REGISTRY else ""


def resolve_page_name(page: Optional[str]) -> str:
    return normalize_page_name(page)


def get_schema(page: Optional[str]) -> SheetSchema:
    canonical = normalize_page_name(page)
    if not canonical or canonical not in SCHEMA_REGISTRY:
        supported = ", ".join(get_supported_pages())
        raise KeyError(f"Unknown page: {page!r}. Supported pages: {supported}")
    return SCHEMA_REGISTRY[canonical]


def get_page_schema(page: Optional[str]) -> SheetSchema:
    return get_schema(page)


def get_sheet_schema(page: Optional[str]) -> SheetSchema:
    return get_schema(page)


def get_keys(page: Optional[str]) -> List[str]:
    return get_schema(page).keys


def get_headers(page: Optional[str]) -> List[str]:
    return get_schema(page).display_headers


def get_display_headers(page: Optional[str]) -> List[str]:
    return get_schema(page).display_headers


def get_required_keys(page: Optional[str]) -> List[str]:
    return get_schema(page).required_keys


def get_field_specs(page: Optional[str]) -> List[Dict[str, Any]]:
    return [f.as_dict() for f in get_schema(page).fields]


def get_column_count(page: Optional[str]) -> int:
    return get_schema(page).column_count


def get_sheet_spec(page: Optional[str], include_fields: bool = True) -> Dict[str, Any]:
    return get_schema(page).as_sheet_spec(include_fields=include_fields)


def get_sheet_specs(include_fields: bool = True) -> Dict[str, Dict[str, Any]]:
    return {p: get_sheet_spec(p, include_fields=include_fields) for p in get_supported_pages()}


def get_pages_payload(include_fields: bool = False) -> Dict[str, Any]:
    return {
        "ok": True,
        "version": get_version(),
        "count": len(get_supported_pages()),
        "pages": [
            get_sheet_spec(p, include_fields=include_fields)
            for p in get_supported_pages()
        ],
    }


def get_page_kind(page: Optional[str]) -> str:
    """Return the kind string for a page. Returns '' if unknown.
    New in v3.4.0 — consumed by page_catalog.py v3.4.0."""
    canonical = normalize_page_name(page)
    if canonical and canonical in SCHEMA_REGISTRY:
        return SCHEMA_REGISTRY[canonical].kind
    return ""


def is_instrument_page(page: Optional[str]) -> bool:
    canonical = normalize_page_name(page)
    return canonical in {
        "Market_Leaders", "Global_Markets", "Commodities_FX",
        "Mutual_Funds", "My_Portfolio", "Top_10_Investments",
    }


def is_special_page(page: Optional[str]) -> bool:
    return not is_instrument_page(page)


def _row_to_dict_by_position(schema: SheetSchema, row: Sequence[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    row_list = list(row)
    for idx, key in enumerate(schema.keys):
        out[key] = row_list[idx] if idx < len(row_list) else None
    return out


def _row_to_dict_by_name(schema: SheetSchema, row: Mapping[str, Any]) -> Dict[str, Any]:
    alias_map = schema.alias_lookup()
    normalized_incoming: Dict[str, Any] = {}
    for incoming_key, value in row.items():
        normalized_incoming[_norm(incoming_key)] = value

    out: Dict[str, Any] = {}
    for fs in schema.fields:
        matched = None
        for candidate in fs.normalized_names():
            if candidate in normalized_incoming:
                matched = normalized_incoming[candidate]
                break
        out[fs.key] = matched
    return out


def project_row_to_schema(
    page: Optional[str],
    row: Any,
    *,
    include_unknown: bool = False,
    preserve_input_on_unknown: bool = False,
) -> Dict[str, Any]:
    schema = get_schema(page)
    if isinstance(row, Mapping):
        projected = _row_to_dict_by_name(schema, row)
        if include_unknown:
            alias_map = schema.alias_lookup()
            unknown = {str(k): v for k, v in row.items() if _norm(k) not in alias_map}
            if unknown:
                projected["_unknown"] = dict(unknown) if preserve_input_on_unknown else unknown
        return projected
    if isinstance(row, (list, tuple)):
        return _row_to_dict_by_position(schema, row)
    return {key: None for key in schema.keys}


def project_rows_to_schema(
    page: Optional[str],
    rows: Iterable[Any],
    *,
    include_unknown: bool = False,
) -> List[Dict[str, Any]]:
    return [project_row_to_schema(page, row, include_unknown=include_unknown) for row in (rows or [])]


def project_row(page: Optional[str], row: Any, **kwargs: Any) -> Dict[str, Any]:
    return project_row_to_schema(page, row, **kwargs)


def project_rows(page: Optional[str], rows: Iterable[Any], **kwargs: Any) -> List[Dict[str, Any]]:
    return project_rows_to_schema(page, rows, **kwargs)


def rows_to_matrix(page: Optional[str], rows: Iterable[Any]) -> List[List[Any]]:
    schema = get_schema(page)
    projected = project_rows_to_schema(page, rows)
    return [[row.get(key) for key in schema.keys] for row in projected]


def validate_row_against_schema(
    page: Optional[str],
    row: Any,
    *,
    strict: bool = False,
) -> Dict[str, Any]:
    schema = get_schema(page)
    projected = project_row_to_schema(page, row, include_unknown=True)
    missing = [k for k in schema.required_keys if projected.get(k) in (None, "", [], {}, ())]
    unexpected: List[str] = []
    if strict and isinstance(row, Mapping):
        alias_map = schema.alias_lookup()
        unexpected = [str(k) for k in row.keys() if _norm(k) not in alias_map]
    filled = sum(1 for k in schema.keys if projected.get(k) not in (None, "", [], {}, ()))
    return {
        "ok": not missing and not unexpected,
        "page": schema.page,
        "expected_columns": schema.column_count,
        "filled_columns": filled,
        "completeness_ratio_pct": round((filled / max(1, schema.column_count)) * 100, 2),
        "missing_required_keys": missing,
        "unexpected_keys": unexpected,
        "projected_row": {k: projected.get(k) for k in schema.keys},
    }


def validate_row(page: Optional[str], row: Any, *, strict: bool = False) -> Dict[str, Any]:
    return validate_row_against_schema(page, row, strict=strict)


def build_data_dictionary_rows(page: Optional[str] = None) -> List[Dict[str, Any]]:
    pages = [normalize_page_name(page)] if page else get_supported_pages()
    pages = [p for p in pages if p]
    rows: List[Dict[str, Any]] = []
    for page_name in pages:
        schema = get_schema(page_name)
        for fs in schema.fields:
            rows.append({
                "page": schema.page,
                "field_key": fs.key,
                "display_header": fs.header,
                "data_type": fs.data_type,
                "group": fs.group,
                "required": fs.required,
                "description": fs.description,
                "format_hint": fs.format_hint,
                "aliases": ", ".join(fs.aliases),
            })
    return rows


def get_data_dictionary_rows(page: Optional[str] = None) -> List[Dict[str, Any]]:
    return build_data_dictionary_rows(page=page)


def get_data_dictionary_matrix(page: Optional[str] = None) -> List[List[Any]]:
    rows = build_data_dictionary_rows(page=page)
    schema = get_schema("Data_Dictionary")
    return [[row.get(k) for k in schema.keys] for row in rows]


def infer_page_from_headers(headers: Sequence[str]) -> str:
    normalized_headers = {_norm(h) for h in (headers or []) if str(h or "").strip()}
    best_page = ""
    best_score = -1
    for page_name in get_supported_pages():
        schema = get_schema(page_name)
        key_pool = {_norm(x) for x in schema.keys + schema.display_headers}
        score = len(normalized_headers & key_pool)
        if score > best_score:
            best_page = page_name
            best_score = score
    return best_page


def infer_page_from_rows(rows: Sequence[Any]) -> str:
    if not rows:
        return ""
    first_row = rows[0]
    if isinstance(first_row, Mapping):
        return infer_page_from_headers(list(first_row.keys()))
    return ""


def registry_summary() -> Dict[str, Any]:
    return {
        "version": get_version(),
        "page_count": len(get_supported_pages()),
        "pages": {
            page: {
                "column_count": get_column_count(page),
                "required_keys": get_required_keys(page),
                "kind": get_schema(page).kind,
            }
            for page in get_supported_pages()
        },
    }


__all__ = [
    "CANONICAL_SHEETS",
    "DATA_DICTIONARY_FIELDS",
    "FieldSpec",
    "INSTRUMENT_FIELDS",         # backward compat → MARKET_LEADERS_FIELDS
    "INSIGHTS_FIELDS",           # backward compat → INSIGHTS_ANALYSIS_FIELDS
    "MARKET_LEADERS_FIELDS",
    "GLOBAL_MARKETS_FIELDS",
    "COMMODITIES_FX_FIELDS",
    "MUTUAL_FUNDS_FIELDS",
    "MY_PORTFOLIO_FIELDS",
    "INSIGHTS_ANALYSIS_FIELDS",
    "TOP10_INVESTMENTS_FIELDS",
    "PAGE_ALIASES",
    "PAGE_ORDER",
    "SCHEMA_REGISTRY",
    "SUPPORTED_PAGES",
    "SheetSchema",
    "TOP10_EXTRA_FIELDS",        # backward compat
    "build_data_dictionary_rows",
    "get_column_count",
    "get_data_dictionary_matrix",
    "get_data_dictionary_rows",
    "get_display_headers",
    "get_field_specs",
    "get_headers",
    "get_keys",
    "get_page_kind",
    "get_page_schema",
    "get_pages_payload",
    "get_required_keys",
    "get_schema",
    "get_sheet_schema",
    "get_sheet_spec",
    "get_sheet_specs",
    "get_supported_pages",
    "get_version",
    "has_page",
    "infer_page_from_headers",
    "infer_page_from_rows",
    "is_instrument_page",
    "is_special_page",
    "list_pages",
    "list_sheets",
    "normalize_page_name",
    "page_exists",
    "project_row",
    "project_row_to_schema",
    "project_rows",
    "project_rows_to_schema",
    "registry_summary",
    "resolve_page_name",
    "rows_to_matrix",
    "validate_row",
    "validate_row_against_schema",
]
