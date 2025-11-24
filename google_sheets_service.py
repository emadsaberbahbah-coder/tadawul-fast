# google_sheets_service.py
# =============================================================================
# Google Sheets Service - Ultimate Investment Dashboard (10 Pages)
# =============================================================================
from __future__ import annotations

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

import gspread
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# =============================================================================
# Optional Data Models (mainly for future write/aux functions)
# =============================================================================


class StockData(BaseModel):
    symbol: str
    company_name: str
    price: float
    change: float
    change_percent: float
    volume: int
    market_cap: Optional[float] = None
    sector: Optional[str] = None
    timestamp: datetime


class FinancialData(BaseModel):
    symbol: str
    revenue: Optional[float] = None
    net_income: Optional[float] = None
    eps: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    timestamp: datetime


class TechnicalIndicator(BaseModel):
    symbol: str
    rsi: Optional[float] = None
    macd: Optional[float] = None
    moving_avg_50: Optional[float] = None
    moving_avg_200: Optional[float] = None
    timestamp: datetime


class SheetUpdate(BaseModel):
    sheet_name: str
    range: str
    values: List[List[Any]]
    timestamp: datetime


# =============================================================================
# Google Sheets Service
# =============================================================================


class GoogleSheetsService:
    """
    Ultimate Investment Dashboard – 10-page Google Sheets integration.

    ENVIRONMENT VARIABLES (MUST BE SET):
      - GOOGLE_SERVICE_ACCOUNT_JSON : Full JSON for service account (string).
      - SPREADSHEET_ID              : Ultimate Investment Dashboard Sheet ID.

    MAIN TABS / PAGES (BY NAME IN GOOGLE SHEETS):
      1) "KSA Tadawul Market"
      2) "Global Market Stock"
      3) "Mutual Fund"
      4) "My Portfolio Investment"
      5) "Commodities & FX"
      6) "Advanced Analysis & Advice"
      7) "Economic Calendar"
      8) "Investment Income Statement YTD"
      9) "Investment Advisor Assumptions" (inputs + AI output table)
    """

    SHEET_NAMES: Dict[str, str] = {
        "KSA_TADAWUL": "KSA Tadawul Market",
        "GLOBAL_MARKET": "Global Market Stock",
        "MUTUAL_FUND": "Mutual Fund",
        "MY_PORTFOLIO": "My Portfolio Investment",
        "COMMODITIES_FX": "Commodities & FX",
        "ADVANCED_ANALYSIS": "Advanced Analysis & Advice",
        "ECONOMIC_CALENDAR": "Economic Calendar",
        "INCOME_STATEMENT": "Investment Income Statement YTD",
        "INVESTMENT_ADVISOR": "Investment Advisor Assumptions",
    }

    def __init__(self, spreadsheet_id: Optional[str] = None) -> None:
        self.client: Optional[gspread.Client] = None
        self.spreadsheet: Optional[gspread.Spreadsheet] = None
        self.worksheets: Dict[str, gspread.Worksheet] = {}
        self.initialized: bool = False
        self.spreadsheet_id: Optional[str] = spreadsheet_id or os.getenv("SPREADSHEET_ID")

    # -------------------------------------------------------------------------
    # Initialization
    # -------------------------------------------------------------------------
    def initialize(self, credentials_json: Optional[str] = None) -> bool:
        """
        Initialize Google Sheets client using the service account JSON.

        - credentials_json:
            * If None, uses GOOGLE_SERVICE_ACCOUNT_JSON env.
            * Can be a JSON string or a dict.
        """
        if self.initialized:
            return True

        try:
            if credentials_json is None:
                credentials_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

            if not credentials_json:
                logger.error("❌ GOOGLE_SERVICE_ACCOUNT_JSON env var is missing or empty")
                return False

            if isinstance(credentials_json, str):
                credentials_dict = json.loads(credentials_json)
            else:
                credentials_dict = credentials_json

            scopes = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]

            credentials = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
            self.client = gspread.authorize(credentials)

            if not self.spreadsheet_id:
                logger.error("❌ SPREADSHEET_ID env var is missing")
                return False

            self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            self._cache_worksheets()

            self.initialized = True
            logger.info("✅ Google Sheets service initialized (spreadsheet_id=%s)", self.spreadsheet_id)
            return True

        except Exception as e:
            logger.error("❌ Failed to initialize Google Sheets service: %s", e, exc_info=True)
            return False

    def _cache_worksheets(self) -> None:
        """Cache all worksheet handles for faster access."""
        if not self.spreadsheet:
            logger.error("❌ Cannot cache worksheets – spreadsheet is not set")
            return

        try:
            worksheets = self.spreadsheet.worksheets()
            self.worksheets = {ws.title: ws for ws in worksheets}
            logger.info("✅ Cached %d worksheets: %s", len(self.worksheets), list(self.worksheets.keys()))
        except Exception as e:
            logger.error("❌ Failed to cache worksheets: %s", e, exc_info=True)

    # -------------------------------------------------------------------------
    # Worksheet Access Helpers
    # -------------------------------------------------------------------------
    def get_worksheet(self, sheet_name: str):
        """Get worksheet by tab name (with caching)."""
        if not self.initialized:
            if not self.initialize():
                return None

        if sheet_name in self.worksheets:
            return self.worksheets[sheet_name]

        try:
            if not self.spreadsheet:
                logger.error("❌ Spreadsheet not initialized when accessing '%s'", sheet_name)
                return None

            ws = self.spreadsheet.worksheet(sheet_name)
            self.worksheets[sheet_name] = ws
            return ws
        except Exception as e:
            logger.error("❌ Worksheet '%s' not found: %s", sheet_name, e, exc_info=True)
            return None

    def get_sheet_by_key(self, key: str):
        """Get worksheet using a logical key from SHEET_NAMES."""
        sheet_name = self.SHEET_NAMES.get(key)
        if not sheet_name:
            logger.error("❌ Unknown sheet key: %s", key)
            return None
        return self.get_worksheet(sheet_name)

    # =========================================================================
    # 1) KSA Tadawul Market – Identity, Price, Fundamentals, Technical, Forecasts, AI
    # =========================================================================
    def read_ksa_tadawul_market(self) -> List[Dict[str, Any]]:
        """
        Read rows from 'KSA Tadawul Market' sheet.

        EXPECTED COLUMNS (header row):
          - Ticker           (Input, grey)
          - Custom Tag       (Input, grey)
          - Company Name
          - Sector
          - Trading Market
          - Last Price, Day High, Day Low, Previous Close
          - Change Value, Change %
          - Volume, Value Traded, Market Cap
          - P/E, P/B, Dividend Yield, EPS, ROE
          - Trend Direction
          - Support Level, Resistance Level
          - Momentum Score, Volatility Est
          - Expected ROI 1M, 3M, 12M
          - Risk Level
          - Confidence Score
          - Data Quality
          - Composite Score
          - Rank
          - Recommendation
        """
        try:
            ws = self.get_sheet_by_key("KSA_TADAWUL")
            if not ws:
                return []

            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                ticker = str(row.get("Ticker", "")).strip()
                if not ticker:
                    continue

                entry = {
                    "ticker": ticker,
                    "custom_tag": str(row.get("Custom Tag", "")).strip(),
                    "company_name": str(row.get("Company Name", "")).strip(),
                    "sector": str(row.get("Sector", "")).strip(),
                    "trading_market": str(row.get("Trading Market", "")).strip(),
                    "last_price": self._safe_float(row.get("Last Price")),
                    "day_high": self._safe_float(row.get("Day High")),
                    "day_low": self._safe_float(row.get("Day Low")),
                    "previous_close": self._safe_float(row.get("Previous Close")),
                    "change_value": self._safe_float(row.get("Change Value")),
                    "change_pct": self._safe_float(row.get("Change %")),
                    "volume": self._safe_int(row.get("Volume")),
                    "value_traded": self._safe_float(row.get("Value Traded")),
                    "market_cap": self._safe_float(row.get("Market Cap")),
                    "pe": self._safe_float(row.get("P/E")),
                    "pb": self._safe_float(row.get("P/B")),
                    "dividend_yield": self._safe_float(row.get("Dividend Yield")),
                    "eps": self._safe_float(row.get("EPS")),
                    "roe": self._safe_float(row.get("ROE")),
                    "trend_direction": str(row.get("Trend Direction", "")).strip(),
                    "support_level": self._safe_float(row.get("Support Level")),
                    "resistance_level": self._safe_float(row.get("Resistance Level")),
                    "momentum_score": self._safe_float(row.get("Momentum Score")),
                    "volatility_est": self._safe_float(row.get("Volatility Est")),
                    "expected_roi_1m": self._safe_float(row.get("Expected ROI 1M")),
                    "expected_roi_3m": self._safe_float(row.get("Expected ROI 3M")),
                    "expected_roi_12m": self._safe_float(row.get("Expected ROI 12M")),
                    "risk_level": str(row.get("Risk Level", "")).strip(),
                    "confidence_score": self._safe_float(row.get("Confidence Score")),
                    "data_quality": str(row.get("Data Quality", "")).strip(),
                    "composite_score": self._safe_float(row.get("Composite Score")),
                    "rank": self._safe_int(row.get("Rank")),
                    "recommendation": str(row.get("Recommendation", "")).strip(),
                }
                results.append(entry)

            logger.info("📈 Read %d KSA Tadawul rows", len(results))
            return results

        except Exception as e:
            logger.error("❌ Failed to read KSA Tadawul Market: %s", e, exc_info=True)
            return []

    # =========================================================================
    # 2) Global Market Stock – Same idea with extra fields
    # =========================================================================
    def read_global_market_stock(self) -> List[Dict[str, Any]]:
        """
        Read rows from 'Global Market Stock' sheet.

        EXPECTED EXTRA COLUMNS vs KSA:
          - Country
          - Currency
          - Exchange
          - 52W High, 52W Low
          - Pre-market, After-hours (optional)
        """
        try:
            ws = self.get_sheet_by_key("GLOBAL_MARKET")
            if not ws:
                return []

            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                ticker = str(row.get("Ticker") or row.get("Symbol") or "").strip()
                if not ticker:
                    continue

                entry = {
                    "ticker": ticker,
                    "custom_tag": str(row.get("Custom Tag", "")).strip(),
                    "company_name": str(row.get("Company Name", "")).strip(),
                    "country": str(row.get("Country", "")).strip(),
                    "sector": str(row.get("Sector", "")).strip(),
                    "currency": str(row.get("Currency", "")).strip(),
                    "exchange": str(row.get("Exchange", "")).strip(),
                    "last_price": self._safe_float(row.get("Last Price")),
                    "day_high": self._safe_float(row.get("Day High")),
                    "day_low": self._safe_float(row.get("Day Low")),
                    "previous_close": self._safe_float(row.get("Previous Close")),
                    "change_value": self._safe_float(row.get("Change Value")),
                    "change_pct": self._safe_float(row.get("Change %")),
                    "volume": self._safe_int(row.get("Volume")),
                    "value_traded": self._safe_float(row.get("Value Traded")),
                    "market_cap": self._safe_float(row.get("Market Cap")),
                    "high_52w": self._safe_float(row.get("52W High")),
                    "low_52w": self._safe_float(row.get("52W Low")),
                    "premarket_price": self._safe_float(row.get("Pre-market Price")),
                    "after_hours_price": self._safe_float(row.get("After-hours Price")),
                    "pe": self._safe_float(row.get("P/E")),
                    "pb": self._safe_float(row.get("P/B")),
                    "dividend_yield": self._safe_float(row.get("Dividend Yield")),
                    "eps": self._safe_float(row.get("EPS")),
                    "roe": self._safe_float(row.get("ROE")),
                    "trend_direction": str(row.get("Trend Direction", "")).strip(),
                    "support_level": self._safe_float(row.get("Support Level")),
                    "resistance_level": self._safe_float(row.get("Resistance Level")),
                    "momentum_score": self._safe_float(row.get("Momentum Score")),
                    "volatility_est": self._safe_float(row.get("Volatility Est")),
                    "expected_roi_1m": self._safe_float(row.get("Expected ROI 1M")),
                    "expected_roi_3m": self._safe_float(row.get("Expected ROI 3M")),
                    "expected_roi_12m": self._safe_float(row.get("Expected ROI 12M")),
                    "risk_level": str(row.get("Risk Level", "")).strip(),
                    "confidence_score": self._safe_float(row.get("Confidence Score")),
                    "data_quality": str(row.get("Data Quality", "")).strip(),
                    "composite_score": self._safe_float(row.get("Composite Score")),
                    "rank": self._safe_int(row.get("Rank")),
                    "recommendation": str(row.get("Recommendation", "")).strip(),
                }
                results.append(entry)

            logger.info("🌍 Read %d Global Market rows", len(results))
            return results

        except Exception as e:
            logger.error("❌ Failed to read Global Market Stock: %s", e, exc_info=True)
            return []

    # =========================================================================
    # 3) Mutual Fund
    # =========================================================================
    def read_mutual_funds(self) -> List[Dict[str, Any]]:
        """
        Read rows from 'Mutual Fund' sheet.

        EXPECTED COLUMNS:
          - Fund Name, Fund Code, Category
          - NAV, NAV Change %
          - YTD Return %, 1Y Return %, 3Y Return %
          - Sharpe Ratio, Expense Ratio, Fund Size
          - Risk Level, Manager Comments, Recommendation
        """
        try:
            ws = self.get_sheet_by_key("MUTUAL_FUND")
            if not ws:
                return []

            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                name = str(row.get("Fund Name", "")).strip()
                if not name:
                    continue

                entry = {
                    "fund_name": name,
                    "fund_code": str(row.get("Fund Code", "")).strip(),
                    "category": str(row.get("Category", "")).strip(),
                    "nav": self._safe_float(row.get("NAV")),
                    "nav_change_pct": self._safe_float(row.get("NAV Change %")),
                    "ytd_return_pct": self._safe_float(row.get("YTD Return %")),
                    "one_year_return_pct": self._safe_float(row.get("1Y Return %")),
                    "three_year_return_pct": self._safe_float(row.get("3Y Return %")),
                    "sharpe_ratio": self._safe_float(row.get("Sharpe Ratio")),
                    "expense_ratio": self._safe_float(row.get("Expense Ratio")),
                    "fund_size": self._safe_float(row.get("Fund Size")),
                    "risk_level": str(row.get("Risk Level", "")).strip(),
                    "manager_comments": str(row.get("Manager Comments", "")).strip(),
                    "recommendation": str(row.get("Recommendation", "")).strip(),
                }
                results.append(entry)

            logger.info("💼 Read %d mutual funds", len(results))
            return results

        except Exception as e:
            logger.error("❌ Failed to read Mutual Fund sheet: %s", e, exc_info=True)
            return []

    # =========================================================================
    # 4) My Portfolio Investment
    # =========================================================================
    def read_my_portfolio_investment(self) -> List[Dict[str, Any]]:
        """
        Read rows from 'My Portfolio Investment' sheet.

        EXPECTED COLUMNS:
          - Ticker, Quantity, Buy Price, Today's Price
          - Cost Value, Market Value
          - Unrealized P/L, Realized P/L, Total Return %
          - Weight % of Portfolio
          - Risk Level, Confidence Score
          - Target Price
          - Investment Horizon (Days)
        """
        try:
            ws = self.get_sheet_by_key("MY_PORTFOLIO")
            if not ws:
                return []

            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                ticker = str(row.get("Ticker") or row.get("Symbol") or "").strip()
                if not ticker:
                    continue

                entry = {
                    "ticker": ticker,
                    "quantity": self._safe_float(row.get("Quantity")),
                    "buy_price": self._safe_float(row.get("Buy Price")),
                    "today_price": self._safe_float(row.get("Today's Price")),
                    "cost_value": self._safe_float(row.get("Cost Value")),
                    "market_value": self._safe_float(row.get("Market Value")),
                    "unrealized_pl": self._safe_float(row.get("Unrealized P/L")),
                    "realized_pl": self._safe_float(row.get("Realized P/L")),
                    "total_return_pct": self._safe_float(row.get("Total Return %")),
                    "weight_pct": self._safe_float(row.get("Weight % of Portfolio")),
                    "risk_level": str(row.get("Risk Level", "")).strip(),
                    "confidence_score": self._safe_float(row.get("Confidence Score")),
                    "target_price": self._safe_float(row.get("Target Price")),
                    "investment_horizon_days": self._safe_int(row.get("Investment Horizon (Days)")),
                }
                results.append(entry)

            logger.info("📊 Read %d portfolio positions", len(results))
            return results

        except Exception as e:
            logger.error("❌ Failed to read My Portfolio Investment: %s", e, exc_info=True)
            return []

    # =========================================================================
    # 5) Commodities & FX
    # =========================================================================
    def read_commodities_fx(self) -> List[Dict[str, Any]]:
        """
        Read rows from 'Commodities & FX' sheet.

        EXPECTED COLUMNS:
          - Asset, Category (Commodity / FX / Crypto)
          - Last Price, Day Change %
          - 30D Volatility
          - Trend
          - Support Level, Resistance Level
          - Forecast ROI 1M, 3M
          - Economic Sensitivity
          - Recommendation
        """
        try:
            ws = self.get_sheet_by_key("COMMODITIES_FX")
            if not ws:
                return []

            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                asset = str(row.get("Asset", "")).strip()
                if not asset:
                    continue

                entry = {
                    "asset": asset,
                    "category": str(row.get("Category", "")).strip(),
                    "last_price": self._safe_float(row.get("Last Price")),
                    "day_change_pct": self._safe_float(row.get("Day Change %")),
                    "volatility_30d": self._safe_float(row.get("30D Volatility")),
                    "trend": str(row.get("Trend", "")).strip(),
                    "support_level": self._safe_float(row.get("Support Level")),
                    "resistance_level": self._safe_float(row.get("Resistance Level")),
                    "forecast_roi_1m": self._safe_float(row.get("Forecast ROI 1M")),
                    "forecast_roi_3m": self._safe_float(row.get("Forecast ROI 3M")),
                    "economic_sensitivity": str(row.get("Economic Sensitivity", "")).strip(),
                    "recommendation": str(row.get("Recommendation", "")).strip(),
                }
                results.append(entry)

            logger.info("💱 Read %d Commodities/FX rows", len(results))
            return results

        except Exception as e:
            logger.error("❌ Failed to read Commodities & FX: %s", e, exc_info=True)
            return []

    # =========================================================================
    # 6) Advanced Analysis & Advice
    # =========================================================================
    def read_advanced_analysis_advice(self) -> List[Dict[str, Any]]:
        """
        Read rows from 'Advanced Analysis & Advice' sheet.

        This page is complex (Top 7 KSA, Top 7 Global, Top 7 Funds,
        risk buckets, portfolio KPIs, market summary).
        For now, we return all records as-is and let the API layer
        interpret sections by 'Section' / 'Bucket' / 'Type' columns.
        """
        try:
            ws = self.get_sheet_by_key("ADVANCED_ANALYSIS")
            if not ws:
                return []

            rows = ws.get_all_records()
            logger.info("🔍 Read %d Advanced Analysis rows", len(rows))
            return rows

        except Exception as e:
            logger.error("❌ Failed to read Advanced Analysis & Advice: %s", e, exc_info=True)
            return []

    # =========================================================================
    # 7) Economic Calendar
    # =========================================================================
    def read_economic_calendar(self) -> List[Dict[str, Any]]:
        """
        Read rows from 'Economic Calendar' sheet.

        EXPECTED COLUMNS:
          - Date
          - Country
          - Event
          - Actual
          - Forecast
          - Previous
          - Impact Level
          - Market Relevance
        """
        try:
            ws = self.get_sheet_by_key("ECONOMIC_CALENDAR")
            if not ws:
                return []

            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                date_raw = str(row.get("Date", "")).strip()
                if not date_raw:
                    continue

                entry = {
                    "date": date_raw,  # keep as string; API can parse if needed
                    "country": str(row.get("Country", "")).strip(),
                    "event": str(row.get("Event", "")).strip(),
                    "actual": str(row.get("Actual", "")).strip(),
                    "forecast": str(row.get("Forecast", "")).strip(),
                    "previous": str(row.get("Previous", "")).strip(),
                    "impact_level": str(row.get("Impact Level", "")).strip(),
                    "market_relevance": str(row.get("Market Relevance", "")).strip(),
                }
                results.append(entry)

            logger.info("📅 Read %d Economic Calendar events", len(results))
            return results

        except Exception as e:
            logger.error("❌ Failed to read Economic Calendar: %s", e, exc_info=True)
            return []

    # =========================================================================
    # 8) Investment Income Statement YTD
    # =========================================================================
    def read_investment_income_statement_ytd(self) -> List[Dict[str, Any]]:
        """
        Read rows from 'Investment Income Statement YTD' sheet.

        EXPECTED COLUMNS (per period row, e.g. monthly or quarterly):
          - Period
          - Beginning Portfolio Value
          - Contributions
          - Withdrawals
          - Realized Gains
          - Unrealized Gains
          - Dividends / Coupons
          - Total Return
          - ROI %
          - YTD vs Last Year Comparison (optional columns)
        """
        try:
            ws = self.get_sheet_by_key("INCOME_STATEMENT")
            if not ws:
                return []

            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                period = str(row.get("Period", "")).strip()
                if not period:
                    continue

                entry = {
                    "period": period,
                    "begin_value": self._safe_float(row.get("Beginning Portfolio Value")),
                    "contributions": self._safe_float(row.get("Contributions")),
                    "withdrawals": self._safe_float(row.get("Withdrawals")),
                    "realized_gains": self._safe_float(row.get("Realized Gains")),
                    "unrealized_gains": self._safe_float(row.get("Unrealized Gains")),
                    "dividends_coupons": self._safe_float(row.get("Dividends / Coupons")),
                    "total_return": self._safe_float(row.get("Total Return")),
                    "roi_pct": self._safe_float(row.get("ROI %")),
                    "ytd_vs_last_year": self._safe_float(row.get("YTD vs Last Year %")),
                }
                results.append(entry)

            logger.info("📘 Read %d Investment Income Statement rows", len(results))
            return results

        except Exception as e:
            logger.error("❌ Failed to read Investment Income Statement YTD: %s", e, exc_info=True)
            return []

    # =========================================================================
    # 9) Investment Advisor Assumptions – Inputs (DAYS) + AI Ideas Table
    # =========================================================================
    def read_investment_advisor_assumptions(self) -> List[Dict[str, Any]]:
        """
        Read assumptions from 'Investment Advisor Assumptions' sheet.

        INPUT BLOCK (grey) – expected columns:
          - Investment Amount
          - Target ROI %
          - Max Risk Level
          - Investment Period (Days)   ← STRICTLY IN DAYS
          - Preferred Sectors
          - Excluded Sectors
          - Market Preference (KSA / Global / Funds / Mixed)

        The same sheet may also contain the AI recommendations table; those
        rows can be distinguished by a 'Type' or 'Section' column, or by
        leaving input columns blank. Here we only read rows that look like
        input scenarios.
        """
        try:
            ws = self.get_sheet_by_key("INVESTMENT_ADVISOR")
            if not ws:
                return []

            rows = ws.get_all_records()
            results: List[Dict[str, Any]] = []

            for row in rows:
                # Use 'Investment Amount' as a marker for input rows
                if row.get("Investment Amount") in (None, "", 0):
                    continue

                period_days = self._safe_int(row.get("Investment Period (Days)", 0))

                entry = {
                    "investment_amount": self._safe_float(row.get("Investment Amount")),
                    "target_roi_pct": self._safe_float(row.get("Target ROI")),
                    "max_risk_level": str(row.get("Max Risk Level", "")).strip(),
                    "investment_period_days": period_days,  # STRICTLY days
                    "preferred_sectors": str(row.get("Preferred Sectors", "")).strip(),
                    "excluded_sectors": str(row.get("Excluded Sectors", "")).strip(),
                    "market_preference": str(row.get("Market Preference", "")).strip(),
                }
                results.append(entry)

            logger.info("🎯 Read %d Investment Advisor scenarios", len(results))
            return results

        except Exception as e:
            logger.error("❌ Failed to read Investment Advisor Assumptions: %s", e, exc_info=True)
            return []

    # =========================================================================
    # Utility Methods
    # =========================================================================
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert to float."""
        if value is None or value == "":
            return None
        try:
            if isinstance(value, str):
                value = value.replace(",", "").replace('"', "")
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value) -> int:
        """Safely convert to int."""
        if value is None or value == "":
            return 0
        try:
            if isinstance(value, str):
                value = value.replace(",", "").replace('"', "")
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    def get_sheet_info(self) -> Dict[str, Any]:
        """
        Get metadata about the spreadsheet and all cached sheets.
        """
        try:
            if not self.initialized:
                if not self.initialize():
                    return {}

            if not self.spreadsheet:
                return {}

            info: Dict[str, Any] = {
                "spreadsheet_title": self.spreadsheet.title,
                "spreadsheet_id": self.spreadsheet.id,
                "sheets": [],
                "last_updated": datetime.now(),
            }

            for name, ws in self.worksheets.items():
                info["sheets"].append(
                    {
                        "name": name,
                        "row_count": ws.row_count,
                        "col_count": ws.col_count,
                    }
                )

            return info

        except Exception as e:
            logger.error("❌ Failed to get sheet info: %s", e, exc_info=True)
            return {}

    def create_backup_sheet(self) -> bool:
        """
        Create a backup sheet by duplicating the first tab.
        """
        try:
            if not self.initialized:
                if not self.initialize():
                    return False

            if not self.spreadsheet:
                return False

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"Backup_{ts}"

            source = self.spreadsheet.sheet1
            self.spreadsheet.duplicate_sheet(source.id, new_sheet_name=backup_name)

            logger.info("✅ Created backup sheet: %s", backup_name)
            return True

        except Exception as e:
            logger.error("❌ Failed to create backup sheet: %s", e, exc_info=True)
            return False


# =============================================================================
# Singleton Instance
# =============================================================================

google_sheets_service = GoogleSheetsService()
