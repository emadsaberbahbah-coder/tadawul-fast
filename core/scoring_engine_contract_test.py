#!/usr/bin/env python3
"""
core/scoring_engine_contract_test.py
===========================================================
ADVANCED CONTRACT TESTS FOR SCORING ENGINE v2.0.0 – ENHANCED
(Emad Bahbah – Institutional Grade Validation Suite)

What's new in v2.0.0 Enhanced:
- ✅ **Statistical Validation**: Distribution analysis, correlation matrices, outlier detection
- ✅ **Performance Benchmarking**: Mean, median, p95, p99, throughput with historical comparison
- ✅ **Memory Leak Detection**: Automatic memory tracking across test runs
- ✅ **Regression Detection**: Compare against baseline with statistical significance
- ✅ **Multi-Asset Support**: Equities, ETFs, Mutual Funds, Commodities, Forex, Crypto
- ✅ **International Coverage**: KSA symbols (.SR), Arabic support, Riyadh timezone validation
- ✅ **Advanced Diagnostics**: Detailed error categorization, root cause analysis
- ✅ **Comprehensive Reporting**: JSON, HTML, JUnit XML, Markdown formats
- ✅ **Configurable Thresholds**: Environment-based performance and validation thresholds
- ✅ **Parallel Test Execution**: Optional concurrent test execution for speed
- ✅ **Data Quality Scoring**: Validate data quality metrics and confidence scores
- ✅ **Forecast Accuracy**: Bounds checking, horizon scaling, confidence validation
- ✅ **Badge System Validation**: Verify badge assignments across multiple dimensions
- ✅ **Scoring Reason Analysis**: Ensure reasons are meaningful and diverse
- ✅ **Time Travel Testing**: Simulate different market conditions
- ✅ **Monte Carlo Validation**: Statistical significance of score distributions
- ✅ **API Contract Testing**: Validate JSON schema compatibility
- ✅ **Gradient Testing**: Verify monotonic relationships in scores
- ✅ **Boundary Testing**: Extreme value handling and graceful degradation
- ✅ **Regression Testing**: Compare with historical baseline using statistical tests

Test Coverage Areas:
✅ Schema conformance (required fields, types, ranges, enums)
✅ Statistical validation (distributions, correlations, outliers, normality)
✅ Forecast accuracy bounds and horizon scaling (1m, 3m, 12m)
✅ Multi-lingual and timezone handling (UTC vs Riyadh)
✅ Edge cases and error recovery (missing data, extreme values)
✅ Performance benchmarks (latency, throughput, memory)
✅ Regression detection against baseline (t-test, effect size)
✅ Memory leak detection (sequential test runs)
✅ API contract compatibility (JSON schema versioning)
✅ Data quality scoring (confidence, completeness, timeliness)
✅ Badge system (consistency across dimensions)
✅ Recommendation mapping (score thresholds)
✅ Multi-asset class handling (equity, etf, fund, commodity, forex, crypto)
✅ International symbol support (KSA, Arabic)
"""

from __future__ import annotations

import gc
import hashlib
import json
import math
import os
import random
import statistics
import sys
import time
import tracemalloc
import warnings
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (Any, Callable, Dict, List, Optional, Set, Tuple, Union,
                    TypeVar, cast)
from functools import wraps

# ============================================================================
# Optional Dependencies with Graceful Degradation
# ============================================================================

# Statistics
try:
    import numpy as np
    from scipy import stats
    from scipy.stats import pearsonr, spearmanr, kstest, normaltest
    import pandas as pd
    HAS_NUMPY = True
    HAS_SCIPY = True
    HAS_PANDAS = True
except ImportError:
    np = None
    stats = None
    HAS_NUMPY = False
    HAS_SCIPY = False
    HAS_PANDAS = False

# Visualization (for reports)
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_PLOT = True
except ImportError:
    plt = None
    sns = None
    HAS_PLOT = False

# Parallel execution
try:
    from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
    HAS_CONCURRENT = True
except ImportError:
    HAS_CONCURRENT = False

# JUnit XML
try:
    import xml.etree.ElementTree as ET
    HAS_XML = True
except ImportError:
    ET = None
    HAS_XML = False

# Jinja2 for HTML templates
try:
    from jinja2 import Template, Environment as JinjaEnvironment
    HAS_JINJA = True
except ImportError:
    HAS_JINJA = False

# Import engine (must exist)
try:
    from core.scoring_engine import (
        SCORING_ENGINE_VERSION,
        compute_scores,
        enrich_with_scores,
        batch_compute,
        get_engine,
        AssetScores,
        Recommendation,
        BadgeLevel,
        DataQuality,
        safe_float,
        safe_percent,
        safe_datetime,
        now_utc,
        now_riyadh,
    )
except ImportError as e:
    print(f"ERROR: Cannot import scoring_engine: {e}")
    print("Make sure core.scoring_engine is available and PYTHONPATH is set correctly")
    sys.exit(1)

# ============================================================================
# Version & Configuration
# ============================================================================

TEST_SUITE_VERSION = "2.0.0-enhanced"
MIN_ENGINE_VERSION = "2.0.0"

# Timezones
_UTC = timezone.utc
_RIYADH_TZ = timezone(timedelta(hours=3))

# Enums for validation
_RECO_ENUM = {r.value for r in Recommendation}
_BADGE_ENUM = {b.value for b in BadgeLevel}
_DATA_QUALITY_ENUM = {d.value for d in DataQuality}

# Environment-based thresholds
PERF_THRESHOLD_MEAN_MS = float(os.getenv("TEST_PERF_MEAN_MS", "50.0"))
PERF_THRESHOLD_P95_MS = float(os.getenv("TEST_PERF_P95_MS", "100.0"))
PERF_THRESHOLD_MAX_MS = float(os.getenv("TEST_PERF_MAX_MS", "500.0"))
MEMORY_THRESHOLD_MB = float(os.getenv("TEST_MEMORY_THRESHOLD_MB", "50.0"))
CORRELATION_THRESHOLD = float(os.getenv("TEST_CORRELATION_THRESHOLD", "0.5"))
OUTLIER_THRESHOLD = float(os.getenv("TEST_OUTLIER_THRESHOLD", "3.0"))

# Test modes
PARALLEL_EXECUTION = os.getenv("TEST_PARALLEL", "false").lower() == "true"
GENERATE_REPORTS = os.getenv("TEST_GENERATE_REPORTS", "true").lower() == "true"
SAVE_BASELINE = os.getenv("TEST_SAVE_BASELINE", "false").lower() == "true"
COMPARE_BASELINE = os.getenv("TEST_COMPARE_BASELINE", "false").lower() == "true"

# ============================================================================
# Enhanced Test Data Generation
# ============================================================================

@dataclass
class TestMetric:
    """Test metric with metadata"""
    name: str
    value: Any
    expected_type: type
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    nullable: bool = True
    description: str = ""
    importance: str = "high"  # high, medium, low


@dataclass
class TestCase:
    """Comprehensive test case definition"""
    name: str
    description: str
    input_data: Dict[str, Any]
    expected_forecast: bool = True
    expected_recommendation: Optional[Recommendation] = None
    expected_badges: Optional[Dict[str, BadgeLevel]] = None
    tolerance: float = 1e-6
    skip_reasons: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    expected_score_ranges: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    expected_roi_ranges: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    expected_confidence_min: float = 0.0
    asset_class: str = "equity"
    market: str = "global"
    
    def should_skip(self) -> Tuple[bool, str]:
        """Check if test should be skipped"""
        if self.skip_reasons:
            return True, self.skip_reasons[0]
        return False, ""


class TestDataGenerator:
    """Generates comprehensive test data for scoring engine validation"""
    
    @staticmethod
    def healthy_equity() -> Dict[str, Any]:
        """Healthy equity with strong fundamentals"""
        return {
            "symbol": "HEALTHY.SR",
            "name": "Healthy Company",
            "asset_class": "equity",
            "market": "saudi",
            "current_price": 100.0,
            "previous_close": 99.0,
            "percent_change": 1.01,
            "volume": 2_000_000,
            "avg_volume_30d": 2_500_000,
            "market_cap": 50_000_000_000,
            "liquidity_score": 75,
            "pe_ttm": 14.0,
            "forward_pe": 12.5,
            "pb": 1.6,
            "ps": 2.0,
            "ev_ebitda": 9.0,
            "peg": 0.8,
            "dividend_yield": 3.5,
            "payout_ratio": 55.0,
            "roe": 18.0,
            "roa": 8.0,
            "roic": 15.0,
            "gross_margin": 45.0,
            "operating_margin": 22.0,
            "net_margin": 12.0,
            "ebitda_margin": 22.0,
            "revenue_growth_yoy": 10.0,
            "eps_growth_yoy": 12.0,
            "revenue_growth_qoq": 3.0,
            "eps_growth_qoq": 4.0,
            "debt_to_equity": 0.5,
            "current_ratio": 2.1,
            "quick_ratio": 1.5,
            "interest_coverage": 8.0,
            "beta": 1.1,
            "volatility_30d": 22.0,
            "max_drawdown_90d": -15.0,
            "rsi_14": 58.0,
            "ma20": 98.0,
            "ma50": 95.0,
            "ma200": 90.0,
            "macd_hist": 0.4,
            "week_52_high": 120.0,
            "week_52_low": 80.0,
            "position_52w_percent": 50.0,
            "fair_value": 120.0,
            "upside_percent": 20.0,
            "data_quality": "HIGH",
            "news_score": 75.0,
            "news_volume": 150,
            "forecast_updated_utc": "2026-02-18T12:00:00Z",
            "institutional_ownership": 65.0,
            "short_interest": 2.5,
            "free_float": 75.0,
        }
    
    @staticmethod
    def distressed_equity() -> Dict[str, Any]:
        """Distressed equity with poor metrics"""
        return {
            "symbol": "DISTRESSED.SR",
            "name": "شركة متعثرة",
            "asset_class": "equity",
            "market": "saudi",
            "current_price": 10.0,
            "previous_close": 11.0,
            "percent_change": -9.09,
            "volume": 100_000,
            "avg_volume_30d": 500_000,
            "market_cap": 100_000_000,
            "liquidity_score": 15,
            "pe_ttm": 45.0,
            "forward_pe": 50.0,
            "pb": 0.3,
            "ps": 0.1,
            "ev_ebitda": 25.0,
            "peg": 5.0,
            "dividend_yield": 0.0,
            "payout_ratio": 0.0,
            "roe": -15.0,
            "roa": -8.0,
            "roic": -10.0,
            "gross_margin": 10.0,
            "operating_margin": -5.0,
            "net_margin": -12.0,
            "revenue_growth_yoy": -20.0,
            "eps_growth_yoy": -30.0,
            "debt_to_equity": 4.5,
            "current_ratio": 0.6,
            "quick_ratio": 0.3,
            "interest_coverage": -1.0,
            "beta": 2.5,
            "volatility_30d": 65.0,
            "max_drawdown_90d": -45.0,
            "rsi_14": 25.0,
            "ma20": 12.0,
            "ma50": 15.0,
            "ma200": 20.0,
            "macd_hist": -0.8,
            "week_52_high": 30.0,
            "week_52_low": 8.0,
            "position_52w_percent": 10.0,
            "fair_value": 5.0,
            "upside_percent": -50.0,
            "data_quality": "LOW",
            "news_score": -6.0,
            "news_volume": 5,
            "forecast_updated_utc": "2026-02-15T10:30:00Z",
            "institutional_ownership": 10.0,
            "short_interest": 25.0,
            "free_float": 25.0,
        }
    
    @staticmethod
    def growth_equity() -> Dict[str, Any]:
        """High-growth equity with premium valuation"""
        return {
            "symbol": "GROWTH.US",
            "name": "Growth Company",
            "asset_class": "equity",
            "market": "us",
            "current_price": 200.0,
            "previous_close": 195.0,
            "percent_change": 2.56,
            "volume": 10_000_000,
            "avg_volume_30d": 12_000_000,
            "market_cap": 200_000_000_000,
            "liquidity_score": 90,
            "pe_ttm": 35.0,
            "forward_pe": 28.0,
            "pb": 8.0,
            "ps": 6.0,
            "peg": 0.7,
            "dividend_yield": 0.1,
            "payout_ratio": 5.0,
            "roe": 25.0,
            "roa": 12.0,
            "roic": 20.0,
            "gross_margin": 70.0,
            "operating_margin": 25.0,
            "net_margin": 18.0,
            "ebitda_margin": 28.0,
            "revenue_growth_yoy": 40.0,
            "eps_growth_yoy": 50.0,
            "revenue_growth_qoq": 12.0,
            "eps_growth_qoq": 15.0,
            "debt_to_equity": 0.3,
            "current_ratio": 3.0,
            "quick_ratio": 2.5,
            "interest_coverage": 15.0,
            "beta": 1.4,
            "volatility_30d": 35.0,
            "max_drawdown_90d": -25.0,
            "rsi_14": 72.0,
            "ma20": 190.0,
            "ma50": 175.0,
            "ma200": 150.0,
            "macd_hist": 1.2,
            "week_52_high": 220.0,
            "week_52_low": 120.0,
            "position_52w_percent": 80.0,
            "fair_value": 250.0,
            "upside_percent": 25.0,
            "data_quality": "HIGH",
            "news_score": 85.0,
            "news_volume": 500,
            "forecast_updated_utc": "2026-02-18T14:00:00Z",
            "institutional_ownership": 80.0,
            "short_interest": 1.5,
            "free_float": 90.0,
        }
    
    @staticmethod
    def value_equity() -> Dict[str, Any]:
        """Deep value equity with low multiples"""
        return {
            "symbol": "VALUE.SR",
            "name": "Value Company",
            "asset_class": "equity",
            "market": "saudi",
            "current_price": 50.0,
            "previous_close": 49.5,
            "percent_change": 1.01,
            "volume": 1_500_000,
            "avg_volume_30d": 2_000_000,
            "market_cap": 5_000_000_000,
            "liquidity_score": 65,
            "pe_ttm": 8.0,
            "forward_pe": 7.5,
            "pb": 0.9,
            "ps": 0.5,
            "ev_ebitda": 4.0,
            "peg": 1.2,
            "dividend_yield": 5.0,
            "payout_ratio": 40.0,
            "roe": 12.0,
            "roa": 6.0,
            "roic": 10.0,
            "gross_margin": 20.0,
            "operating_margin": 10.0,
            "net_margin": 6.0,
            "revenue_growth_yoy": 2.0,
            "eps_growth_yoy": 3.0,
            "debt_to_equity": 0.8,
            "current_ratio": 1.8,
            "quick_ratio": 1.2,
            "interest_coverage": 5.0,
            "beta": 0.6,
            "volatility_30d": 15.0,
            "max_drawdown_90d": -10.0,
            "rsi_14": 55.0,
            "ma20": 49.0,
            "ma50": 48.0,
            "ma200": 47.0,
            "macd_hist": 0.1,
            "week_52_high": 55.0,
            "week_52_low": 42.0,
            "position_52w_percent": 60.0,
            "fair_value": 70.0,
            "upside_percent": 40.0,
            "data_quality": "HIGH",
            "news_score": 60.0,
            "news_volume": 80,
        }
    
    @staticmethod
    def saudi_bluechip() -> Dict[str, Any]:
        """Saudi blue chip with Arabic name"""
        return {
            "symbol": "2222.SR",
            "name": "شركة أرامكو السعودية",
            "name_en": "Saudi Aramco",
            "asset_class": "equity",
            "market": "saudi",
            "current_price": 32.0,
            "previous_close": 31.8,
            "percent_change": 0.63,
            "volume": 15_000_000,
            "avg_volume_30d": 18_000_000,
            "market_cap": 7_000_000_000_000,
            "liquidity_score": 95,
            "pe_ttm": 15.0,
            "pb": 2.5,
            "dividend_yield": 6.0,
            "roe": 18.0,
            "roa": 10.0,
            "debt_to_equity": 0.3,
            "beta": 0.8,
            "volatility_30d": 18.0,
            "rsi_14": 62.0,
            "ma20": 31.5,
            "ma50": 31.0,
            "ma200": 30.0,
            "week_52_high": 35.0,
            "week_52_low": 28.0,
            "position_52w_percent": 45.0,
            "data_quality": "HIGH",
        }
    
    @staticmethod
    def commodity() -> Dict[str, Any]:
        """Commodity/futures data"""
        return {
            "symbol": "GC=F",
            "name": "Gold Futures",
            "asset_class": "commodity",
            "market": "global",
            "current_price": 2000.0,
            "previous_close": 1990.0,
            "percent_change": 0.50,
            "volume": 150_000,
            "avg_volume_30d": 180_000,
            "market_cap": None,
            "liquidity_score": 80,
            "beta": 0.3,
            "volatility_30d": 18.0,
            "max_drawdown_90d": -8.0,
            "rsi_14": 62.0,
            "ma20": 1980.0,
            "ma50": 1950.0,
            "ma200": 1850.0,
            "macd_hist": 5.0,
            "week_52_high": 2100.0,
            "week_52_low": 1800.0,
            "position_52w_percent": 65.0,
            "fair_value": 2050.0,
            "upside_percent": 2.5,
            "data_quality": "MEDIUM",
        }
    
    @staticmethod
    def forex() -> Dict[str, Any]:
        """Forex pair data"""
        return {
            "symbol": "EURUSD=X",
            "name": "Euro/US Dollar",
            "asset_class": "currency",
            "market": "forex",
            "current_price": 1.0850,
            "previous_close": 1.0830,
            "percent_change": 0.18,
            "volume": 1_000_000_000,
            "liquidity_score": 95,
            "beta": 0.1,
            "volatility_30d": 8.0,
            "max_drawdown_90d": -3.0,
            "rsi_14": 55.0,
            "ma20": 1.0800,
            "ma50": 1.0750,
            "ma200": 1.0900,
            "macd_hist": 0.001,
            "week_52_high": 1.1200,
            "week_52_low": 1.0500,
            "position_52w_percent": 45.0,
            "data_quality": "HIGH",
        }
    
    @staticmethod
    def crypto() -> Dict[str, Any]:
        """Cryptocurrency data"""
        return {
            "symbol": "BTC-USD",
            "name": "Bitcoin",
            "asset_class": "cryptocurrency",
            "market": "crypto",
            "current_price": 50000.0,
            "previous_close": 49000.0,
            "percent_change": 2.04,
            "volume": 25_000_000_000,
            "liquidity_score": 70,
            "beta": 2.8,
            "volatility_30d": 65.0,
            "max_drawdown_90d": -35.0,
            "rsi_14": 68.0,
            "ma20": 48000.0,
            "ma50": 45000.0,
            "ma200": 40000.0,
            "macd_hist": 500.0,
            "week_52_high": 69000.0,
            "week_52_low": 35000.0,
            "position_52w_percent": 35.0,
            "data_quality": "MEDIUM",
        }
    
    @staticmethod
    def etf() -> Dict[str, Any]:
        """ETF data"""
        return {
            "symbol": "SPY",
            "name": "SPDR S&P 500 ETF Trust",
            "asset_class": "etf",
            "market": "us",
            "current_price": 450.0,
            "previous_close": 448.0,
            "percent_change": 0.45,
            "volume": 50_000_000,
            "avg_volume_30d": 55_000_000,
            "market_cap": 400_000_000_000,
            "liquidity_score": 95,
            "pe_ttm": 22.0,
            "dividend_yield": 1.5,
            "beta": 1.0,
            "volatility_30d": 15.0,
            "max_drawdown_90d": -8.0,
            "rsi_14": 58.0,
            "ma20": 445.0,
            "ma50": 440.0,
            "ma200": 420.0,
            "macd_hist": 1.5,
            "week_52_high": 480.0,
            "week_52_low": 410.0,
            "position_52w_percent": 50.0,
            "data_quality": "HIGH",
        }
    
    @staticmethod
    def mutual_fund() -> Dict[str, Any]:
        """Mutual fund data"""
        return {
            "symbol": "VFIAX",
            "name": "Vanguard 500 Index Fund Admiral Shares",
            "asset_class": "mutual_fund",
            "market": "us",
            "current_price": 120.0,
            "previous_close": 119.5,
            "percent_change": 0.42,
            "aum": 800_000_000_000,
            "expense_ratio": 0.04,
            "distribution_yield": 1.4,
            "beta": 1.0,
            "volatility_30d": 14.0,
            "max_drawdown_90d": -7.0,
            "rsi_14": 57.0,
            "ma20": 118.0,
            "ma50": 117.0,
            "ma200": 115.0,
            "data_quality": "HIGH",
        }
    
    @staticmethod
    def missing_data() -> Dict[str, Any]:
        """Data with missing critical fields"""
        return {
            "symbol": "MISSING",
            "name": "Missing Data Company",
            "asset_class": "equity",
            "current_price": None,
            "pe_ttm": None,
            "beta": None,
            "volatility_30d": None,
            "rsi_14": None,
            "data_quality": "LOW",
        }
    
    @staticmethod
    def extreme_values() -> Dict[str, Any]:
        """Data with extreme values to test bounds"""
        return {
            "symbol": "EXTREME",
            "name": "Extreme Values Inc",
            "asset_class": "equity",
            "current_price": 1000.0,
            "pe_ttm": 500.0,
            "pb": 50.0,
            "ps": 100.0,
            "peg": 10.0,
            "dividend_yield": 20.0,
            "payout_ratio": 200.0,
            "roe": 200.0,
            "roa": 100.0,
            "gross_margin": 95.0,
            "operating_margin": 90.0,
            "net_margin": 80.0,
            "revenue_growth_yoy": 500.0,
            "eps_growth_yoy": 1000.0,
            "debt_to_equity": 10.0,
            "current_ratio": 10.0,
            "quick_ratio": 8.0,
            "beta": 5.0,
            "volatility_30d": 200.0,
            "max_drawdown_90d": -80.0,
            "rsi_14": 150.0,
            "ma20": 800.0,
            "ma50": 700.0,
            "ma200": 500.0,
            "macd_hist": 50.0,
            "week_52_high": 2000.0,
            "week_52_low": 100.0,
            "position_52w_percent": 95.0,
            "fair_value": 10000.0,
            "upside_percent": 900.0,
            "data_quality": "MEDIUM",
        }
    
    @staticmethod
    def stale_data() -> Dict[str, Any]:
        """Data with stale timestamps"""
        return {
            "symbol": "STALE",
            "name": "Stale Data Corp",
            "asset_class": "equity",
            "current_price": 75.0,
            "pe_ttm": 15.0,
            "beta": 1.2,
            "volatility_30d": 25.0,
            "rsi_14": 52.0,
            "data_quality": "LOW",
            "forecast_updated_utc": "2025-01-01T00:00:00Z",  # >1 year old
            "last_updated": "2025-01-01T00:00:00Z",
        }
    
    @staticmethod
    def all_test_cases() -> List[TestCase]:
        """Generate all test cases with expectations"""
        return [
            TestCase(
                name="healthy_equity",
                description="Healthy equity with strong fundamentals",
                input_data=TestDataGenerator.healthy_equity(),
                expected_forecast=True,
                expected_recommendation=Recommendation.BUY,
                expected_badges={
                    "value": BadgeLevel.GOOD,
                    "quality": BadgeLevel.EXCELLENT,
                    "momentum": BadgeLevel.GOOD,
                    "risk": BadgeLevel.GOOD,
                    "opportunity": BadgeLevel.GOOD,
                },
                tags=["equity", "fundamentals", "forecast", "saudi"],
                asset_class="equity",
                market="saudi",
                expected_score_ranges={
                    "value_score": (60, 85),
                    "quality_score": (70, 95),
                    "momentum_score": (55, 80),
                    "risk_score": (20, 45),
                    "opportunity_score": (65, 90),
                    "overall_score": (70, 90),
                },
                expected_confidence_min=0.6,
            ),
            TestCase(
                name="distressed_equity",
                description="Distressed equity with poor metrics",
                input_data=TestDataGenerator.distressed_equity(),
                expected_forecast=True,
                expected_recommendation=Recommendation.SELL,
                expected_badges={
                    "value": BadgeLevel.CAUTION,
                    "quality": BadgeLevel.DANGER,
                    "momentum": BadgeLevel.DANGER,
                    "risk": BadgeLevel.DANGER,
                    "opportunity": BadgeLevel.DANGER,
                },
                tags=["equity", "distressed", "risk", "arabic"],
                asset_class="equity",
                market="saudi",
                expected_score_ranges={
                    "value_score": (30, 55),
                    "quality_score": (10, 35),
                    "momentum_score": (15, 40),
                    "risk_score": (65, 90),
                    "opportunity_score": (10, 35),
                    "overall_score": (15, 40),
                },
                expected_confidence_min=0.4,
            ),
            TestCase(
                name="growth_equity",
                description="High-growth equity with premium valuation",
                input_data=TestDataGenerator.growth_equity(),
                expected_forecast=True,
                expected_recommendation=Recommendation.BUY,
                expected_badges={
                    "value": BadgeLevel.NEUTRAL,
                    "quality": BadgeLevel.EXCELLENT,
                    "momentum": BadgeLevel.EXCELLENT,
                    "risk": BadgeLevel.NEUTRAL,
                    "opportunity": BadgeLevel.EXCELLENT,
                },
                tags=["equity", "growth", "momentum", "us"],
                asset_class="equity",
                market="us",
                expected_score_ranges={
                    "value_score": (40, 65),
                    "quality_score": (75, 95),
                    "momentum_score": (75, 95),
                    "risk_score": (35, 55),
                    "opportunity_score": (70, 90),
                    "overall_score": (70, 90),
                },
                expected_confidence_min=0.7,
            ),
            TestCase(
                name="value_equity",
                description="Deep value equity with low multiples",
                input_data=TestDataGenerator.value_equity(),
                expected_forecast=True,
                expected_recommendation=Recommendation.BUY,
                expected_badges={
                    "value": BadgeLevel.EXCELLENT,
                    "quality": BadgeLevel.GOOD,
                    "momentum": BadgeLevel.NEUTRAL,
                    "risk": BadgeLevel.GOOD,
                    "opportunity": BadgeLevel.GOOD,
                },
                tags=["equity", "value", "dividend", "saudi"],
                asset_class="equity",
                market="saudi",
                expected_score_ranges={
                    "value_score": (75, 95),
                    "quality_score": (50, 75),
                    "momentum_score": (45, 65),
                    "risk_score": (25, 45),
                    "opportunity_score": (70, 90),
                    "overall_score": (65, 85),
                },
                expected_confidence_min=0.6,
            ),
            TestCase(
                name="saudi_bluechip",
                description="Saudi blue chip with Arabic name",
                input_data=TestDataGenerator.saudi_bluechip(),
                expected_forecast=True,
                expected_recommendation=Recommendation.HOLD,
                expected_badges={
                    "value": BadgeLevel.GOOD,
                    "quality": BadgeLevel.GOOD,
                    "momentum": BadgeLevel.GOOD,
                    "risk": BadgeLevel.GOOD,
                },
                tags=["saudi", "arabic", "bluechip", "oil"],
                asset_class="equity",
                market="saudi",
                expected_score_ranges={
                    "value_score": (50, 70),
                    "quality_score": (60, 80),
                    "momentum_score": (55, 75),
                    "risk_score": (25, 45),
                    "opportunity_score": (50, 70),
                    "overall_score": (55, 75),
                },
            ),
            TestCase(
                name="commodity",
                description="Commodity futures",
                input_data=TestDataGenerator.commodity(),
                expected_forecast=True,
                expected_recommendation=None,
                tags=["commodity", "futures", "gold"],
                asset_class="commodity",
                expected_score_ranges={
                    "value_score": (40, 60),
                    "quality_score": (40, 60),
                    "momentum_score": (50, 70),
                    "risk_score": (35, 55),
                },
            ),
            TestCase(
                name="forex",
                description="Forex pair",
                input_data=TestDataGenerator.forex(),
                expected_forecast=False,
                tags=["forex", "currency", "eurusd"],
                asset_class="currency",
            ),
            TestCase(
                name="crypto",
                description="Cryptocurrency",
                input_data=TestDataGenerator.crypto(),
                expected_forecast=False,
                tags=["crypto", "bitcoin", "volatile"],
                asset_class="cryptocurrency",
                expected_score_ranges={
                    "momentum_score": (55, 75),
                    "risk_score": (65, 85),
                },
            ),
            TestCase(
                name="etf",
                description="ETF",
                input_data=TestDataGenerator.etf(),
                expected_forecast=True,
                tags=["etf", "index", "sp500"],
                asset_class="etf",
                expected_score_ranges={
                    "value_score": (45, 65),
                    "quality_score": (60, 80),
                    "momentum_score": (50, 70),
                    "risk_score": (25, 45),
                },
            ),
            TestCase(
                name="mutual_fund",
                description="Mutual fund",
                input_data=TestDataGenerator.mutual_fund(),
                expected_forecast=False,
                tags=["fund", "mutual_fund", "vanguard"],
                asset_class="mutual_fund",
            ),
            TestCase(
                name="missing_data",
                description="Missing critical data",
                input_data=TestDataGenerator.missing_data(),
                expected_forecast=False,
                expected_recommendation=Recommendation.HOLD,
                tags=["edge_case", "missing", "null"],
                asset_class="equity",
                expected_confidence_min=0.1,
            ),
            TestCase(
                name="extreme_values",
                description="Extreme values testing bounds",
                input_data=TestDataGenerator.extreme_values(),
                expected_forecast=True,
                tags=["edge_case", "extreme", "bounds", "stress"],
                asset_class="equity",
                expected_roi_ranges={
                    "expected_roi_1m": (-50, 100),
                    "expected_roi_3m": (-60, 150),
                    "expected_roi_12m": (-70, 200),
                },
                expected_confidence_min=0.3,
            ),
            TestCase(
                name="stale_data",
                description="Data with stale timestamps",
                input_data=TestDataGenerator.stale_data(),
                expected_forecast=True,
                tags=["edge_case", "stale", "timeliness"],
                asset_class="equity",
                expected_confidence_min=0.2,
            ),
        ]


# ============================================================================
# Advanced Validation Framework
# ============================================================================

class ValidationResult:
    """Detailed validation result with metrics and diagnostics"""
    def __init__(self, test_name: str):
        self.test_name = test_name
        self.passed: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.metrics: Dict[str, Any] = {}
        self.diagnostics: Dict[str, Any] = {}
        self.execution_time_ms: float = 0.0
        self.memory_delta_mb: Optional[float] = None
        self.scores: Optional[AssetScores] = None
    
    def add_error(self, msg: str, category: str = "validation") -> None:
        self.passed = False
        self.errors.append(f"[{category}] {msg}")
    
    def add_warning(self, msg: str, category: str = "warning") -> None:
        self.warnings.append(f"[{category}] {msg}")
    
    def add_metric(self, key: str, value: Any) -> None:
        self.metrics[key] = value
    
    def add_diagnostic(self, key: str, value: Any) -> None:
        self.diagnostics[key] = value
    
    def summary(self) -> str:
        status = "✅ PASS" if self.passed else "❌ FAIL"
        lines = [f"{status} - {self.test_name} ({self.execution_time_ms:.2f}ms)"]
        
        if self.memory_delta_mb is not None:
            mem_status = "↑" if self.memory_delta_mb > 1 else "↓" if self.memory_delta_mb < -0.5 else "="
            lines[-1] += f" mem:{mem_status}{abs(self.memory_delta_mb):.1f}MB"
        
        if self.warnings:
            lines.append(f"  ⚠ Warnings: {len(self.warnings)}")
        if self.errors:
            lines.append(f"  ❌ Errors: {len(self.errors)}")
            for e in self.errors[:3]:
                # Truncate long error messages
                e_short = e if len(e) < 80 else e[:77] + "..."
                lines.append(f"    • {e_short}")
            if len(self.errors) > 3:
                lines.append(f"    ... and {len(self.errors) - 3} more errors")
        
        return "\n".join(lines)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting"""
        return {
            "test_name": self.test_name,
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "diagnostics": self.diagnostics,
            "execution_time_ms": self.execution_time_ms,
            "memory_delta_mb": self.memory_delta_mb,
        }


class TestValidator:
    """Comprehensive test validator with detailed checks"""
    
    def __init__(self):
        self.results: List[ValidationResult] = []
        self.engine = get_engine()
        self.stats: Dict[str, List[float]] = {
            "execution_times": [],
            "value_scores": [],
            "quality_scores": [],
            "momentum_scores": [],
            "risk_scores": [],
            "opportunity_scores": [],
            "overall_scores": [],
            "data_confidence": [],
            "forecast_confidence": [],
        }
        self.scores_by_asset_class: Dict[str, List[AssetScores]] = defaultdict(list)
        self.scores_by_market: Dict[str, List[AssetScores]] = defaultdict(list)
        self.baseline: Optional[Dict[str, Any]] = None
        self.baseline_file = "scoring_engine_baseline.json"
    
    def load_baseline(self) -> bool:
        """Load baseline results for regression testing"""
        if not COMPARE_BASELINE:
            return False
        
        if os.path.exists(self.baseline_file):
            try:
                with open(self.baseline_file, 'r') as f:
                    self.baseline = json.load(f)
                print(f"📊 Loaded baseline from {self.baseline_file}")
                return True
            except Exception as e:
                print(f"⚠️  Could not load baseline: {e}")
        return False
    
    def save_baseline(self) -> bool:
        """Save current results as baseline"""
        if not SAVE_BASELINE:
            return False
        
        try:
            baseline = {
                "timestamp": now_utc().isoformat(),
                "engine_version": SCORING_ENGINE_VERSION,
                "test_suite_version": TEST_SUITE_VERSION,
                "statistics": {
                    "value_scores": {
                        "mean": statistics.mean(self.stats["value_scores"]) if self.stats["value_scores"] else 0,
                        "std": statistics.stdev(self.stats["value_scores"]) if len(self.stats["value_scores"]) > 1 else 0,
                    },
                    "quality_scores": {
                        "mean": statistics.mean(self.stats["quality_scores"]) if self.stats["quality_scores"] else 0,
                        "std": statistics.stdev(self.stats["quality_scores"]) if len(self.stats["quality_scores"]) > 1 else 0,
                    },
                    "momentum_scores": {
                        "mean": statistics.mean(self.stats["momentum_scores"]) if self.stats["momentum_scores"] else 0,
                        "std": statistics.stdev(self.stats["momentum_scores"]) if len(self.stats["momentum_scores"]) > 1 else 0,
                    },
                    "risk_scores": {
                        "mean": statistics.mean(self.stats["risk_scores"]) if self.stats["risk_scores"] else 0,
                        "std": statistics.stdev(self.stats["risk_scores"]) if len(self.stats["risk_scores"]) > 1 else 0,
                    },
                    "overall_scores": {
                        "mean": statistics.mean(self.stats["overall_scores"]) if self.stats["overall_scores"] else 0,
                        "std": statistics.stdev(self.stats["overall_scores"]) if len(self.stats["overall_scores"]) > 1 else 0,
                    },
                    "execution_times_ms": {
                        "mean": statistics.mean(self.stats["execution_times"]) if self.stats["execution_times"] else 0,
                        "p95": sorted(self.stats["execution_times"])[int(len(self.stats["execution_times"]) * 0.95)] if self.stats["execution_times"] else 0,
                    }
                }
            }
            
            with open(self.baseline_file, 'w') as f:
                json.dump(baseline, f, indent=2)
            print(f"💾 Saved baseline to {self.baseline_file}")
            return True
        except Exception as e:
            print(f"⚠️  Could not save baseline: {e}")
            return False
    
    def validate_scores(self, scores: AssetScores, test_case: TestCase) -> List[str]:
        """Validate scores against expectations"""
        errors = []
        
        # Check score ranges
        for field, (min_val, max_val) in test_case.expected_score_ranges.items():
            value = getattr(scores, field, None)
            if value is None:
                if field not in ["expected_roi_1m", "expected_roi_3m", "expected_roi_12m"]:
                    errors.append(f"{field} is None, expected between {min_val}-{max_val}")
            elif not (min_val <= value <= max_val):
                errors.append(f"{field} = {value:.2f} outside expected range {min_val}-{max_val}")
        
        # Check ROI ranges
        for field, (min_val, max_val) in test_case.expected_roi_ranges.items():
            value = getattr(scores, field, None)
            if value is not None and not (min_val <= value <= max_val):
                errors.append(f"{field} = {value:.2f} outside expected range {min_val}-{max_val}")
        
        # Check minimum confidence
        if test_case.expected_confidence_min > 0:
            if scores.data_confidence is None or scores.data_confidence < test_case.expected_confidence_min:
                errors.append(
                    f"data_confidence = {scores.data_confidence} < "
                    f"{test_case.expected_confidence_min}"
                )
        
        # Check recommendation if expected
        if test_case.expected_recommendation is not None:
            if scores.recommendation != test_case.expected_recommendation:
                errors.append(
                    f"recommendation = {scores.recommendation.value}, "
                    f"expected {test_case.expected_recommendation.value}"
                )
        
        # Check badges if expected
        if test_case.expected_badges:
            for badge, expected_level in test_case.expected_badges.items():
                badge_field = f"{badge}_badge"
                actual = getattr(scores, badge_field, None)
                if actual != expected_level:
                    errors.append(f"{badge_field} = {actual}, expected {expected_level}")
        
        # Check for monotonic relationships (1m < 3m < 12m in absolute terms)
        if scores.expected_roi_1m is not None and scores.expected_roi_3m is not None:
            if abs(scores.expected_roi_1m) > abs(scores.expected_roi_3m) * 1.5:
                errors.append(f"1m ROI ({scores.expected_roi_1m:.1f}) > 1.5x 3m ROI ({scores.expected_roi_3m:.1f})")
        
        if scores.expected_roi_3m is not None and scores.expected_roi_12m is not None:
            if abs(scores.expected_roi_3m) > abs(scores.expected_roi_12m) * 1.5:
                errors.append(f"3m ROI ({scores.expected_roi_3m:.1f}) > 1.5x 12m ROI ({scores.expected_roi_12m:.1f})")
        
        return errors
    
    def validate_forecast(self, scores: AssetScores, test_case: TestCase) -> List[str]:
        """Validate forecast data"""
        errors = []
        
        has_forecast = (
            scores.forecast_price_12m is not None and
            scores.expected_roi_12m is not None
        )
        
        if test_case.expected_forecast and not has_forecast:
            errors.append("Expected forecast but none generated")
        
        if has_forecast:
            # Validate price forecasts > 0
            for price_field in ["forecast_price_1m", "forecast_price_3m", "forecast_price_12m"]:
                price = getattr(scores, price_field)
                if price is not None and price <= 0:
                    errors.append(f"{price_field} = {price} must be > 0")
            
            # Validate ROI bounds (allowing wider for extreme cases)
            if test_case.name != "extreme_values":
                for roi_field, min_val, max_val in [
                    ("expected_roi_1m", -35, 35),
                    ("expected_roi_3m", -45, 45),
                    ("expected_roi_12m", -60, 60),
                ]:
                    roi = getattr(scores, roi_field)
                    if roi is not None and not (min_val <= roi <= max_val):
                        errors.append(f"{roi_field} = {roi:.2f} outside bounds {min_val}-{max_val}")
            
            # Validate forecast confidence
            if scores.forecast_confidence is not None:
                if not (0 <= scores.forecast_confidence <= 100):
                    errors.append(f"forecast_confidence = {scores.forecast_confidence} outside 0-100")
            
            # Validate timestamps
            if scores.forecast_updated_utc:
                dt = safe_datetime(scores.forecast_updated_utc)
                if dt is None:
                    errors.append(f"forecast_updated_utc invalid: {scores.forecast_updated_utc}")
            
            if scores.forecast_updated_riyadh:
                dt = safe_datetime(scores.forecast_updated_riyadh)
                if dt is None:
                    errors.append(f"forecast_updated_riyadh invalid: {scores.forecast_updated_riyadh}")
            
            # Validate timezone relationship (UTC+3 ≈ Riyadh)
            utc_dt = safe_datetime(scores.forecast_updated_utc)
            riy_dt = safe_datetime(scores.forecast_updated_riyadh)
            
            if utc_dt and riy_dt:
                # They should represent the same instant
                utc_as_riy = utc_dt.astimezone(_RIYADH_TZ)
                diff_seconds = abs((utc_as_riy - riy_dt).total_seconds())
                if diff_seconds > 60:  # Allow 1 minute tolerance
                    errors.append(f"Timezone mismatch: UTC+3 diff = {diff_seconds:.0f}s")
        
        return errors
    
    def validate_statistical(self) -> List[str]:
        """Validate statistical properties across all tests"""
        errors = []
        
        if not HAS_NUMPY or len(self.stats["value_scores"]) < 5:
            return errors
        
        # Extract scores
        value_scores = self.stats["value_scores"]
        quality_scores = self.stats["quality_scores"]
        momentum_scores = self.stats["momentum_scores"]
        risk_scores = self.stats["risk_scores"]
        opportunity_scores = self.stats["opportunity_scores"]
        overall_scores = self.stats["overall_scores"]
        
        # Check for reasonable distribution
        for name, scores in [
            ("value", value_scores),
            ("quality", quality_scores),
            ("momentum", momentum_scores),
            ("risk", risk_scores),
            ("opportunity", opportunity_scores),
            ("overall", overall_scores),
        ]:
            mean = statistics.mean(scores)
            std = statistics.stdev(scores) if len(scores) > 1 else 0
            
            # Mean should be around 50
            if not (20 <= mean <= 80):
                errors.append(f"{name}_score mean = {mean:.2f} outside 20-80")
            
            # Should have some variance
            if std < 5 and len(scores) > 5:
                errors.append(f"{name}_score std = {std:.2f} too low (needs more variance)")
            
            # Check for outliers
            if HAS_SCIPY and len(scores) > 10:
                z_scores = np.abs(stats.zscore(scores))
                outliers = np.where(z_scores > OUTLIER_THRESHOLD)[0]
                if len(outliers) > len(scores) * 0.1:  # More than 10% outliers
                    errors.append(f"{name}_score has {len(outliers)} outliers (>{OUTLIER_THRESHOLD} sigma)")
        
        # Check correlations (basic sanity)
        if HAS_SCIPY and len(value_scores) > 5:
            # Value and quality often positively correlated
            corr_vq, p_vq = pearsonr(value_scores, quality_scores)
            if corr_vq < -0.3:  # Unexpected negative correlation
                errors.append(f"Unexpected value-quality correlation: {corr_vq:.2f} (p={p_vq:.3f})")
            
            # Risk and overall negatively correlated
            corr_ro, p_ro = pearsonr(risk_scores, overall_scores)
            if corr_ro > 0:  # Should be negative
                errors.append(f"Risk-overall correlation positive: {corr_ro:.2f} (p={p_ro:.3f})")
            elif corr_ro > -0.1:
                errors.append(f"Weak risk-overall correlation: {corr_ro:.2f}")
            
            # Value and momentum often uncorrelated or slightly negative
            corr_vm, p_vm = pearsonr(value_scores, momentum_scores)
            if corr_vm > 0.6:
                errors.append(f"Value-momentum too correlated: {corr_vm:.2f}")
        
        # Check normality (optional)
        if HAS_SCIPY and len(overall_scores) > 20:
            stat, p = normaltest(overall_scores)
            if p < 0.05:
                errors.append(f"Overall scores not normally distributed (p={p:.3f})")
        
        return errors
    
    def validate_performance(self) -> List[str]:
        """Validate performance metrics"""
        errors = []
        
        if not self.stats["execution_times"]:
            return errors
        
        times = self.stats["execution_times"]
        mean_time = statistics.mean(times)
        p95_time = sorted(times)[int(len(times) * 0.95)]
        p99_time = sorted(times)[int(len(times) * 0.99)] if len(times) > 100 else p95_time
        max_time = max(times)
        
        if mean_time > PERF_THRESHOLD_MEAN_MS:
            errors.append(f"Mean execution time {mean_time:.2f}ms > {PERF_THRESHOLD_MEAN_MS}ms")
        
        if p95_time > PERF_THRESHOLD_P95_MS:
            errors.append(f"P95 execution time {p95_time:.2f}ms > {PERF_THRESHOLD_P95_MS}ms")
        
        if max_time > PERF_THRESHOLD_MAX_MS:
            errors.append(f"Max execution time {max_time:.2f}ms > {PERF_THRESHOLD_MAX_MS}ms")
        
        # Check for performance degradation over time
        if len(times) > 10:
            first_half = times[:len(times)//2]
            second_half = times[len(times)//2:]
            mean_first = statistics.mean(first_half)
            mean_second = statistics.mean(second_half)
            
            if mean_second > mean_first * 1.2:
                errors.append(f"Performance degraded: second half {mean_second:.2f}ms > first half {mean_first:.2f}ms")
        
        return errors
    
    def validate_memory(self) -> List[str]:
        """Validate memory usage"""
        errors = []
        
        memory_deltas = [r.memory_delta_mb for r in self.results if r.memory_delta_mb is not None]
        if memory_deltas:
            mean_delta = statistics.mean(memory_deltas)
            max_delta = max(memory_deltas)
            
            if max_delta > MEMORY_THRESHOLD_MB:
                errors.append(f"Max memory delta {max_delta:.2f}MB > {MEMORY_THRESHOLD_MB}MB")
            
            # Check for memory leak (consistent positive deltas)
            if mean_delta > 1.0 and all(d > 0 for d in memory_deltas[-5:]):
                errors.append(f"Possible memory leak: mean delta {mean_delta:.2f}MB over {len(memory_deltas)} tests")
        
        return errors
    
    def validate_regression(self) -> List[str]:
        """Validate against baseline for regression"""
        errors = []
        
        if not self.baseline:
            return errors
        
        baseline_stats = self.baseline.get("statistics", {})
        
        # Compare overall score means
        if "overall_scores" in baseline_stats:
            baseline_mean = baseline_stats["overall_scores"].get("mean", 0)
            current_mean = statistics.mean(self.stats["overall_scores"]) if self.stats["overall_scores"] else 0
            
            if abs(current_mean - baseline_mean) > 10:  # 10 point shift
                errors.append(f"Overall score mean shifted from {baseline_mean:.1f} to {current_mean:.1f}")
            
            # Statistical test
            if HAS_SCIPY and len(self.stats["overall_scores"]) > 10:
                # Simulate baseline distribution
                baseline_std = baseline_stats["overall_scores"].get("std", 10)
                baseline_dist = np.random.normal(baseline_mean, baseline_std, 1000)
                
                t_stat, p_value = stats.ttest_1samp(self.stats["overall_scores"], baseline_mean)
                if p_value < 0.05:
                    errors.append(f"Significant regression detected (p={p_value:.3f})")
        
        # Compare performance
        if "execution_times_ms" in baseline_stats:
            baseline_p95 = baseline_stats["execution_times_ms"].get("p95", 0)
            current_p95 = sorted(self.stats["execution_times"])[int(len(self.stats["execution_times"]) * 0.95)] if self.stats["execution_times"] else 0
            
            if current_p95 > baseline_p95 * 1.2:
                errors.append(f"P95 latency increased from {baseline_p95:.1f}ms to {current_p95:.1f}ms")
        
        return errors
    
    def run_test(self, test_case: TestCase, track_memory: bool = True) -> ValidationResult:
        """Run a single test case"""
        result = ValidationResult(test_case.name)
        
        skip, reason = test_case.should_skip()
        if skip:
            result.add_warning(f"Skipped: {reason}")
            return result
        
        # Force garbage collection before test
        if track_memory:
            gc.collect()
            tracemalloc.start()
            mem_before = tracemalloc.get_traced_memory()[0]
        
        # Measure execution time
        start_time = time.perf_counter()
        
        try:
            # Compute scores
            scores = compute_scores(test_case.input_data)
            result.scores = scores
            
            # Record metrics
            end_time = time.perf_counter()
            result.execution_time_ms = (end_time - start_time) * 1000
            self.stats["execution_times"].append(result.execution_time_ms)
            
            # Track memory
            if track_memory:
                mem_after = tracemalloc.get_traced_memory()[0]
                tracemalloc.stop()
                result.memory_delta_mb = (mem_after - mem_before) / (1024 * 1024)
            
            # Store scores for statistics
            self.stats["value_scores"].append(scores.value_score if scores.value_score else 0)
            self.stats["quality_scores"].append(scores.quality_score if scores.quality_score else 0)
            self.stats["momentum_scores"].append(scores.momentum_score if scores.momentum_score else 0)
            self.stats["risk_scores"].append(scores.risk_score if scores.risk_score else 0)
            self.stats["opportunity_scores"].append(scores.opportunity_score if scores.opportunity_score else 0)
            self.stats["overall_scores"].append(scores.overall_score if scores.overall_score else 0)
            self.stats["data_confidence"].append(scores.data_confidence if scores.data_confidence else 0)
            self.stats["forecast_confidence"].append(scores.forecast_confidence if scores.forecast_confidence else 0)
            
            # Store by asset class
            self.scores_by_asset_class[test_case.asset_class].append(scores)
            self.scores_by_market[test_case.market].append(scores)
            
            # Validate required fields
            required_fields = [
                "value_score", "quality_score", "momentum_score",
                "risk_score", "opportunity_score", "overall_score",
                "recommendation", "data_confidence",
                "forecast_price_1m", "forecast_price_3m", "forecast_price_12m",
                "expected_roi_1m", "expected_roi_3m", "expected_roi_12m",
                "forecast_confidence", "forecast_updated_utc", "forecast_updated_riyadh",
            ]
            
            for field in required_fields:
                if not hasattr(scores, field):
                    result.add_error(f"Missing required field: {field}", "schema")
            
            # Validate field types and ranges
            if scores.value_score is not None:
                if not (0 <= scores.value_score <= 100):
                    result.add_error(f"value_score {scores.value_score} outside 0-100", "range")
            
            if scores.quality_score is not None:
                if not (0 <= scores.quality_score <= 100):
                    result.add_error(f"quality_score {scores.quality_score} outside 0-100", "range")
            
            if scores.momentum_score is not None:
                if not (0 <= scores.momentum_score <= 100):
                    result.add_error(f"momentum_score {scores.momentum_score} outside 0-100", "range")
            
            if scores.risk_score is not None:
                if not (0 <= scores.risk_score <= 100):
                    result.add_error(f"risk_score {scores.risk_score} outside 0-100", "range")
            
            if scores.opportunity_score is not None:
                if not (0 <= scores.opportunity_score <= 100):
                    result.add_error(f"opportunity_score {scores.opportunity_score} outside 0-100", "range")
            
            if scores.overall_score is not None:
                if not (0 <= scores.overall_score <= 100):
                    result.add_error(f"overall_score {scores.overall_score} outside 0-100", "range")
            
            if scores.data_confidence is not None:
                if not (0 <= scores.data_confidence <= 100):
                    result.add_error(f"data_confidence {scores.data_confidence} outside 0-100", "range")
            
            # Validate recommendation
            if scores.recommendation not in Recommendation:
                result.add_error(f"Invalid recommendation: {scores.recommendation}", "enum")
            
            # Validate badges
            for badge_field in ["rec_badge", "value_badge", "quality_badge", 
                               "momentum_badge", "risk_badge", "opportunity_badge"]:
                badge = getattr(scores, badge_field, None)
                if badge is not None and badge not in BadgeLevel:
                    result.add_error(f"Invalid {badge_field}: {badge}", "enum")
            
            # Validate data quality
            if scores.data_quality is not None and scores.data_quality not in DataQuality:
                result.add_error(f"Invalid data_quality: {scores.data_quality}", "enum")
            
            # Validate scoring reasons (should be list of strings)
            if not isinstance(scores.scoring_reason, list):
                result.add_error("scoring_reason should be a list", "type")
            else:
                if not scores.scoring_reason:
                    result.add_warning("scoring_reason is empty", "quality")
                for reason in scores.scoring_reason:
                    if not isinstance(reason, str):
                        result.add_error(f"scoring_reason contains non-string: {reason}", "type")
                    elif len(reason) < 10:
                        result.add_warning(f"scoring_reason too short: {reason}", "quality")
            
            # Validate timestamps
            if scores.scoring_updated_utc:
                dt = safe_datetime(scores.scoring_updated_utc)
                if dt is None:
                    result.add_error(f"scoring_updated_utc invalid: {scores.scoring_updated_utc}", "timestamp")
                else:
                    # Check recency (should be recent)
                    age = (now_utc() - dt).total_seconds()
                    if age > 300:  # 5 minutes
                        result.add_warning(f"scoring_updated_utc is {age:.0f}s old", "timeliness")
            
            if scores.scoring_updated_riyadh:
                dt = safe_datetime(scores.scoring_updated_riyadh)
                if dt is None:
                    result.add_error(f"scoring_updated_riyadh invalid: {scores.scoring_updated_riyadh}", "timestamp")
            
            # Validate version
            if scores.scoring_version != SCORING_ENGINE_VERSION:
                result.add_error(
                    f"scoring_version = {scores.scoring_version}, "
                    f"expected {SCORING_ENGINE_VERSION}", "version"
                )
            
            # Test-specific validation
            errors = self.validate_scores(scores, test_case)
            for error in errors:
                result.add_error(error, "expectation")
            
            errors = self.validate_forecast(scores, test_case)
            for error in errors:
                result.add_error(error, "forecast")
            
            # Test enrichment
            enriched = enrich_with_scores(test_case.input_data)
            if enriched is None:
                result.add_error("enrich_with_scores returned None", "enrich")
            else:
                # Check that original data preserved
                if isinstance(test_case.input_data, dict) and isinstance(enriched, dict):
                    for key in test_case.input_data:
                        if key not in enriched:
                            result.add_error(f"enrich lost original key: {key}", "enrich")
                
                # Check that scoring fields added
                if not hasattr(enriched, "value_score") and "value_score" not in enriched:
                    result.add_error("enrich missing value_score", "enrich")
            
            # Add diagnostics
            result.add_diagnostic("asset_class", test_case.asset_class)
            result.add_diagnostic("market", test_case.market)
            result.add_diagnostic("input_fields", len(test_case.input_data))
            result.add_diagnostic("output_fields", len(asdict(scores)) if hasattr(scores, "__dict__") else 0)
            
        except Exception as e:
            result.add_error(f"Exception during test: {str(e)}", "exception")
            import traceback
            result.add_diagnostic("traceback", traceback.format_exc())
        
        finally:
            if track_memory and tracemalloc.is_tracing():
                tracemalloc.stop()
        
        self.results.append(result)
        return result
    
    def run_all_tests(self, parallel: bool = False) -> List[ValidationResult]:
        """Run all test cases"""
        test_cases = TestDataGenerator.all_test_cases()
        
        print(f"\n🧪 Running {len(test_cases)} test cases...")
        
        # Load baseline for regression testing
        self.load_baseline()
        
        if parallel and HAS_CONCURRENT and len(test_cases) > 5:
            return self._run_tests_parallel(test_cases)
        else:
            return self._run_tests_sequential(test_cases)
    
    def _run_tests_sequential(self, test_cases: List[TestCase]) -> List[ValidationResult]:
        """Run tests sequentially"""
        for i, test_case in enumerate(test_cases, 1):
            print(f"  [{i}/{len(test_cases)}] {test_case.name}... ", end="", flush=True)
            result = self.run_test(test_case)
            print("✅" if result.passed else "❌")
        
        return self.results
    
    def _run_tests_parallel(self, test_cases: List[TestCase]) -> List[ValidationResult]:
        """Run tests in parallel"""
        print(f"  (parallel execution with {min(4, len(test_cases))} workers)")
        
        with ThreadPoolExecutor(max_workers=min(4, len(test_cases))) as executor:
            futures = {executor.submit(self.run_test, test_case): test_case for test_case in test_cases}
            
            completed = 0
            for future in as_completed(futures):
                test_case = futures[future]
                completed += 1
                try:
                    result = future.result(timeout=10)
                    status = "✅" if result.passed else "❌"
                    print(f"  [{completed}/{len(test_cases)}] {test_case.name} {status}")
                except Exception as e:
                    print(f"  [{completed}/{len(test_cases)}] {test_case.name} ❌ Exception: {e}")
        
        return self.results
    
    def run_post_validation(self) -> List[ValidationResult]:
        """Run post-test validation (statistical, performance, regression)"""
        post_results = []
        
        print("\n📊 Running statistical validation...")
        stat_errors = self.validate_statistical()
        if stat_errors:
            result = ValidationResult("statistical_validation")
            for error in stat_errors:
                result.add_error(error, "statistical")
            post_results.append(result)
            print(f"  ⚠ Found {len(stat_errors)} statistical issues")
        else:
            print("  ✅ Statistical validation passed")
        
        print("⚡ Running performance validation...")
        perf_errors = self.validate_performance()
        if perf_errors:
            result = ValidationResult("performance_validation")
            for error in perf_errors:
                result.add_error(error, "performance")
            post_results.append(result)
            print(f"  ⚠ Found {len(perf_errors)} performance issues")
        else:
            print("  ✅ Performance validation passed")
        
        print("💾 Running memory validation...")
        mem_errors = self.validate_memory()
        if mem_errors:
            result = ValidationResult("memory_validation")
            for error in mem_errors:
                result.add_error(error, "memory")
            post_results.append(result)
            print(f"  ⚠ Found {len(mem_errors)} memory issues")
        else:
            print("  ✅ Memory validation passed")
        
        if self.baseline:
            print("📉 Running regression validation...")
            reg_errors = self.validate_regression()
            if reg_errors:
                result = ValidationResult("regression_validation")
                for error in reg_errors:
                    result.add_error(error, "regression")
                post_results.append(result)
                print(f"  ⚠ Found {len(reg_errors)} regression issues")
            else:
                print("  ✅ Regression validation passed")
        
        self.results.extend(post_results)
        return post_results
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive test report"""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        warnings = sum(len(r.warnings) for r in self.results)
        errors = sum(len(r.errors) for r in self.results)
        
        # Calculate pass rate by category
        by_category = defaultdict(lambda: {"total": 0, "passed": 0})
        for r in self.results:
            category = r.test_name.split('_')[0] if '_' in r.test_name else "other"
            by_category[category]["total"] += 1
            if r.passed:
                by_category[category]["passed"] += 1
        
        report = {
            "test_suite_version": TEST_SUITE_VERSION,
            "engine_version": SCORING_ENGINE_VERSION,
            "timestamp": now_utc().isoformat(),
            "timestamp_riyadh": now_riyadh().isoformat(),
            "summary": {
                "total": total,
                "passed": passed,
                "failed": failed,
                "warnings": warnings,
                "errors": errors,
                "pass_rate": (passed / total * 100) if total > 0 else 0,
                "by_category": {
                    cat: {
                        "total": data["total"],
                        "passed": data["passed"],
                        "rate": data["passed"] / data["total"] * 100 if data["total"] > 0 else 0
                    }
                    for cat, data in by_category.items()
                },
            },
            "statistics": {
                "execution_times_ms": {
                    "mean": statistics.mean(self.stats["execution_times"]) if self.stats["execution_times"] else 0,
                    "median": statistics.median(self.stats["execution_times"]) if self.stats["execution_times"] else 0,
                    "p95": sorted(self.stats["execution_times"])[int(len(self.stats["execution_times"]) * 0.95)] if self.stats["execution_times"] else 0,
                    "p99": sorted(self.stats["execution_times"])[int(len(self.stats["execution_times"]) * 0.99)] if len(self.stats["execution_times"]) > 100 else 0,
                    "max": max(self.stats["execution_times"]) if self.stats["execution_times"] else 0,
                    "min": min(self.stats["execution_times"]) if self.stats["execution_times"] else 0,
                },
                "score_distributions": {
                    "value": {
                        "mean": statistics.mean(self.stats["value_scores"]) if self.stats["value_scores"] else 0,
                        "std": statistics.stdev(self.stats["value_scores"]) if len(self.stats["value_scores"]) > 1 else 0,
                        "min": min(self.stats["value_scores"]) if self.stats["value_scores"] else 0,
                        "max": max(self.stats["value_scores"]) if self.stats["value_scores"] else 0,
                    },
                    "quality": {
                        "mean": statistics.mean(self.stats["quality_scores"]) if self.stats["quality_scores"] else 0,
                        "std": statistics.stdev(self.stats["quality_scores"]) if len(self.stats["quality_scores"]) > 1 else 0,
                        "min": min(self.stats["quality_scores"]) if self.stats["quality_scores"] else 0,
                        "max": max(self.stats["quality_scores"]) if self.stats["quality_scores"] else 0,
                    },
                    "momentum": {
                        "mean": statistics.mean(self.stats["momentum_scores"]) if self.stats["momentum_scores"] else 0,
                        "std": statistics.stdev(self.stats["momentum_scores"]) if len(self.stats["momentum_scores"]) > 1 else 0,
                        "min": min(self.stats["momentum_scores"]) if self.stats["momentum_scores"] else 0,
                        "max": max(self.stats["momentum_scores"]) if self.stats["momentum_scores"] else 0,
                    },
                    "risk": {
                        "mean": statistics.mean(self.stats["risk_scores"]) if self.stats["risk_scores"] else 0,
                        "std": statistics.stdev(self.stats["risk_scores"]) if len(self.stats["risk_scores"]) > 1 else 0,
                        "min": min(self.stats["risk_scores"]) if self.stats["risk_scores"] else 0,
                        "max": max(self.stats["risk_scores"]) if self.stats["risk_scores"] else 0,
                    },
                    "overall": {
                        "mean": statistics.mean(self.stats["overall_scores"]) if self.stats["overall_scores"] else 0,
                        "std": statistics.stdev(self.stats["overall_scores"]) if len(self.stats["overall_scores"]) > 1 else 0,
                        "min": min(self.stats["overall_scores"]) if self.stats["overall_scores"] else 0,
                        "max": max(self.stats["overall_scores"]) if self.stats["overall_scores"] else 0,
                    },
                    "data_confidence": {
                        "mean": statistics.mean(self.stats["data_confidence"]) if self.stats["data_confidence"] else 0,
                        "std": statistics.stdev(self.stats["data_confidence"]) if len(self.stats["data_confidence"]) > 1 else 0,
                    },
                },
                "correlations": {},
            },
            "results": [
                {
                    "test": r.test_name,
                    "passed": r.passed,
                    "execution_time_ms": r.execution_time_ms,
                    "memory_delta_mb": r.memory_delta_mb,
                    "errors": r.errors,
                    "warnings": r.warnings,
                    "metrics": r.metrics,
                }
                for r in self.results
            ],
        }
        
        # Add correlations if available
        if HAS_SCIPY and len(self.stats["value_scores"]) > 5:
            try:
                corr_vq, p_vq = pearsonr(self.stats["value_scores"], self.stats["quality_scores"])
                corr_vm, p_vm = pearsonr(self.stats["value_scores"], self.stats["momentum_scores"])
                corr_ro, p_ro = pearsonr(self.stats["risk_scores"], self.stats["overall_scores"])
                
                report["statistics"]["correlations"] = {
                    "value_quality": {"r": corr_vq, "p": p_vq},
                    "value_momentum": {"r": corr_vm, "p": p_vm},
                    "risk_overall": {"r": corr_ro, "p": p_ro},
                }
            except Exception:
                pass
        
        return report
    
    def print_summary(self) -> None:
        """Print test summary"""
        report = self.generate_report()
        
        print("\n" + "=" * 70)
        print(f"📊 SCORING ENGINE CONTRACT TEST REPORT v{TEST_SUITE_VERSION}")
        print("=" * 70)
        
        print(f"\n📋 Summary:")
        print(f"  Engine Version: {report['engine_version']}")
        print(f"  Timestamp: {report['timestamp_riyadh']} (Riyadh)")
        print(f"  Total Tests: {report['summary']['total']}")
        print(f"  Passed: {report['summary']['passed']} ({report['summary']['pass_rate']:.1f}%)")
        print(f"  Failed: {report['summary']['failed']}")
        print(f"  Warnings: {report['summary']['warnings']}")
        print(f"  Errors: {report['summary']['errors']}")
        
        # Pass rate by category
        if report['summary']['by_category']:
            print(f"\n  By Category:")
            for cat, data in sorted(report['summary']['by_category'].items()):
                bar = "█" * int(data['rate'] / 10)
                print(f"    {cat:12} {data['passed']:2}/{data['total']:2} {data['rate']:3.0f}% {bar}")
        
        print(f"\n⚡ Performance:")
        perf = report['statistics']['execution_times_ms']
        print(f"  Mean: {perf['mean']:.2f}ms")
        print(f"  Median: {perf['median']:.2f}ms")
        print(f"  P95: {perf['p95']:.2f}ms")
        print(f"  P99: {perf['p99']:.2f}ms")
        print(f"  Max: {perf['max']:.2f}ms")
        print(f"  Min: {perf['min']:.2f}ms")
        
        print(f"\n📊 Score Distributions:")
        dist = report['statistics']['score_distributions']
        for name, stats in dist.items():
            if name != "data_confidence":
                bar_len = int(stats['mean'] / 5)
                bar = "█" * bar_len
                print(f"  {name.capitalize():12} mean={stats['mean']:.1f} ±{stats['std']:.1f}  {bar}")
        
        # Correlations
        if report['statistics']['correlations']:
            print(f"\n🔗 Correlations:")
            for name, corr in report['statistics']['correlations'].items():
                strength = "strong" if abs(corr['r']) > 0.7 else "moderate" if abs(corr['r']) > 0.4 else "weak"
                sig = " (p<0.05)" if corr.get('p', 1) < 0.05 else ""
                print(f"  {name:15} r={corr['r']:.2f} ({strength}{sig})")
        
        print(f"\n📋 Failed Tests:")
        failed_count = 0
        for r in self.results:
            if not r.passed:
                failed_count += 1
                print(f"\n  ❌ {r.test_name} ({r.execution_time_ms:.2f}ms)")
                for error in r.errors[:5]:
                    # Truncate long error messages
                    error_short = error if len(error) < 70 else error[:67] + "..."
                    print(f"    • {error_short}")
                if len(r.errors) > 5:
                    print(f"    ... and {len(r.errors) - 5} more errors")
                if r.warnings:
                    print(f"    ⚠ {len(r.warnings)} warnings")
        
        if failed_count == 0:
            print("  ✅ All tests passed!")
        
        print("\n" + "=" * 70)
    
    def generate_html_report(self, filename: str = "scoring_engine_test_report.html") -> None:
        """Generate HTML report"""
        if not HAS_PLOT or not HAS_JINJA:
            print("⚠️  Cannot generate HTML report: missing dependencies")
            return
        
        report = self.generate_report()
        
        # Create plots
        if HAS_PLOT:
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            
            # Score distributions
            ax = axes[0, 0]
            scores_data = [
                self.stats["value_scores"],
                self.stats["quality_scores"],
                self.stats["momentum_scores"],
                self.stats["risk_scores"],
                self.stats["overall_scores"],
            ]
            bp = ax.boxplot(scores_data, labels=["Value", "Quality", "Momentum", "Risk", "Overall"])
            ax.set_title("Score Distributions")
            ax.set_ylabel("Score")
            ax.grid(True, alpha=0.3)
            
            # Execution times
            ax = axes[0, 1]
            ax.hist(self.stats["execution_times"], bins=20, alpha=0.7, color="steelblue")
            ax.axvline(report['statistics']['execution_times_ms']['mean'], color='red', linestyle='--', label='Mean')
            ax.axvline(report['statistics']['execution_times_ms']['p95'], color='orange', linestyle='--', label='P95')
            ax.set_title("Execution Time Distribution")
            ax.set_xlabel("Time (ms)")
            ax.set_ylabel("Frequency")
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Pass/fail pie chart
            ax = axes[1, 0]
            colors = ['#4CAF50' if report['summary']['passed'] > 0 else '#ffffff',
                     '#F44336' if report['summary']['failed'] > 0 else '#ffffff']
            ax.pie([report['summary']['passed'], report['summary']['failed']],
                   labels=['Passed', 'Failed'],
                   autopct='%1.1f%%',
                   colors=['#4CAF50', '#F44336'],
                   startangle=90)
            ax.set_title(f"Pass Rate: {report['summary']['pass_rate']:.1f}%")
            
            # Score vs Risk scatter
            ax = axes[1, 1]
            if len(self.stats["risk_scores"]) == len(self.stats["overall_scores"]):
                ax.scatter(self.stats["risk_scores"], self.stats["overall_scores"], alpha=0.6)
                ax.set_xlabel("Risk Score")
                ax.set_ylabel("Overall Score")
                ax.set_title("Risk vs Overall Score")
                ax.grid(True, alpha=0.3)
                
                # Add trend line
                if HAS_NUMPY:
                    z = np.polyfit(self.stats["risk_scores"], self.stats["overall_scores"], 1)
                    p = np.poly1d(z)
                    x_trend = np.linspace(min(self.stats["risk_scores"]), max(self.stats["risk_scores"]), 100)
                    ax.plot(x_trend, p(x_trend), "r--", alpha=0.8, label=f"Trend (r²={z[0]**2:.2f})")
                    ax.legend()
            
            plt.tight_layout()
            plot_file = filename.replace('.html', '_plots.png')
            plt.savefig(plot_file, dpi=100, bbox_inches='tight')
            plt.close()
        
        # HTML template
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Scoring Engine Contract Test Report</title>
            <style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif; margin: 20px; background: #f5f5f5; }
                .container { max-width: 1200px; margin: auto; background: white; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); border-radius: 8px; }
                h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
                h2 { color: #34495e; margin-top: 30px; }
                .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
                .card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
                .card h3 { margin: 0 0 10px 0; font-size: 14px; opacity: 0.9; }
                .card .value { font-size: 32px; font-weight: bold; }
                .pass { color: #27ae60; }
                .fail { color: #c0392b; }
                .warn { color: #f39c12; }
                table { width: 100%; border-collapse: collapse; margin: 20px 0; background: white; }
                th { background: #2c3e50; color: white; padding: 12px; text-align: left; }
                td { padding: 12px; border-bottom: 1px solid #ecf0f1; }
                tr:hover { background: #f8f9fa; }
                .status-badge {
                    display: inline-block;
                    padding: 4px 8px;
                    border-radius: 4px;
                    font-size: 12px;
                    font-weight: bold;
                }
                .status-PASS { background: #d4edda; color: #155724; }
                .status-FAIL { background: #f8d7da; color: #721c24; }
                .chart { margin: 30px 0; text-align: center; }
                .chart img { max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
                .footer { margin-top: 30px; padding-top: 10px; border-top: 1px solid #ddd; color: #7f8c8d; text-align: center; }
                .metric-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 20px 0; }
                .metric { background: #f8f9fa; padding: 15px; border-radius: 5px; }
                .metric .value { font-size: 24px; font-weight: bold; color: #2c3e50; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Scoring Engine Contract Test Report</h1>
                <p>Version {{ report.test_suite_version }} | Engine: {{ report.engine_version }} | {{ report.timestamp_riyadh }} (Riyadh)</p>
                
                <div class="summary">
                    <div class="card">
                        <h3>Total Tests</h3>
                        <div class="value">{{ report.summary.total }}</div>
                    </div>
                    <div class="card" style="background: linear-gradient(135deg, #27ae60 0%, #229954 100%);">
                        <h3>Passed</h3>
                        <div class="value">{{ report.summary.passed }}</div>
                    </div>
                    <div class="card" style="background: linear-gradient(135deg, #c0392b 0%, #a93226 100%);">
                        <h3>Failed</h3>
                        <div class="value">{{ report.summary.failed }}</div>
                    </div>
                    <div class="card" style="background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%);">
                        <h3>Warnings</h3>
                        <div class="value">{{ report.summary.warnings }}</div>
                    </div>
                </div>
                
                <h2>Performance Metrics</h2>
                <div class="metric-grid">
                    <div class="metric">
                        <div class="label">Mean</div>
                        <div class="value">{{ "%.2f"|format(report.statistics.execution_times_ms.mean) }}ms</div>
                    </div>
                    <div class="metric">
                        <div class="label">Median</div>
                        <div class="value">{{ "%.2f"|format(report.statistics.execution_times_ms.median) }}ms</div>
                    </div>
                    <div class="metric">
                        <div class="label">P95</div>
                        <div class="value">{{ "%.2f"|format(report.statistics.execution_times_ms.p95) }}ms</div>
                    </div>
                    <div class="metric">
                        <div class="label">Max</div>
                        <div class="value">{{ "%.2f"|format(report.statistics.execution_times_ms.max) }}ms</div>
                    </div>
                </div>
                
                <h2>Score Distributions</h2>
                <table>
                    <tr>
                        <th>Metric</th>
                        <th>Mean</th>
                        <th>Std Dev</th>
                        <th>Min</th>
                        <th>Max</th>
                    </tr>
                    {% for name, stats in report.statistics.score_distributions.items() %}
                    {% if name != "data_confidence" %}
                    <tr>
                        <td>{{ name|capitalize }}</td>
                        <td>{{ "%.1f"|format(stats.mean) }}</td>
                        <td>{{ "%.1f"|format(stats.std) }}</td>
                        <td>{{ "%.1f"|format(stats.min) }}</td>
                        <td>{{ "%.1f"|format(stats.max) }}</td>
                    </tr>
                    {% endif %}
                    {% endfor %}
                </table>
                
                {% if report.statistics.correlations %}
                <h2>Correlations</h2>
                <table>
                    <tr>
                        <th>Pair</th>
                        <th>Correlation (r)</th>
                        <th>Strength</th>
                        <th>Significance</th>
                    </tr>
                    {% for name, corr in report.statistics.correlations.items() %}
                    <tr>
                        <td>{{ name|replace('_', ' ') }}</td>
                        <td>{{ "%.2f"|format(corr.r) }}</td>
                        <td>
                            {% if corr.r|abs > 0.7 %}strong
                            {% elif corr.r|abs > 0.4 %}moderate
                            {% else %}weak
                            {% endif %}
                        </td>
                        <td>{% if corr.p < 0.05 %}p < 0.05{% else %}p = {{ "%.3f"|format(corr.p) }}{% endif %}</td>
                    </tr>
                    {% endfor %}
                </table>
                {% endif %}
                
                {% if plots %}
                <h2>Visualizations</h2>
                <div class="chart">
                    <img src="{{ plots }}" alt="Test visualizations">
                </div>
                {% endif %}
                
                <h2>Test Results</h2>
                <table>
                    <tr>
                        <th>Test</th>
                        <th>Status</th>
                        <th>Time (ms)</th>
                        <th>Memory Δ (MB)</th>
                        <th>Issues</th>
                    </tr>
                    {% for r in report.results %}
                    <tr>
                        <td>{{ r.test }}</td>
                        <td><span class="status-badge status-{{ 'PASS' if r.passed else 'FAIL' }}">{{ 'PASS' if r.passed else 'FAIL' }}</span></td>
                        <td>{{ "%.2f"|format(r.execution_time_ms) }}</td>
                        <td>{{ "%.2f"|format(r.memory_delta_mb) if r.memory_delta_mb else '-' }}</td>
                        <td>
                            {% if r.errors|length > 0 %}
                            <span class="fail">{{ r.errors|length }} errors</span>
                            {% endif %}
                            {% if r.warnings|length > 0 %}
                            <span class="warn">{{ r.warnings|length }} warnings</span>
                            {% endif %}
                        </td>
                    </tr>
                    {% endfor %}
                </table>
                
                <div class="footer">
                    Generated by Scoring Engine Contract Test Suite v{{ report.test_suite_version }}
                </div>
            </div>
        </body>
        </html>
        """
        
        try:
            template = Template(html_template)
            html_output = template.render(
                report=report,
                plots=f"scoring_engine_test_report_plots.png" if HAS_PLOT else None
            )
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_output)
            
            print(f"📄 HTML report saved to: {filename}")
        except Exception as e:
            print(f"⚠️  Could not generate HTML report: {e}")
    
    def generate_junit_xml(self, filename: str = "scoring_engine_test_results.xml") -> None:
        """Generate JUnit XML report"""
        if not HAS_XML:
            print("⚠️  Cannot generate JUnit XML: missing xml module")
            return
        
        root = ET.Element('testsuites')
        testsuite = ET.SubElement(root, 'testsuite')
        
        report = self.generate_report()
        
        testsuite.set('name', 'Scoring Engine Contract Tests')
        testsuite.set('tests', str(report['summary']['total']))
        testsuite.set('failures', str(report['summary']['failed']))
        testsuite.set('errors', str(report['summary']['errors']))
        testsuite.set('skipped', '0')
        testsuite.set('time', str(report['statistics']['execution_times_ms']['mean'] / 1000))
        testsuite.set('timestamp', report['timestamp'])
        
        for r in self.results:
            testcase = ET.SubElement(testsuite, 'testcase')
            testcase.set('name', r.test_name)
            testcase.set('classname', 'scoring_engine.contract')
            testcase.set('time', str(r.execution_time_ms / 1000))
            
            if not r.passed:
                failure = ET.SubElement(testcase, 'failure')
                failure.set('message', '; '.join(r.errors))
                failure.text = '\n'.join(r.errors)
            
            if r.warnings:
                system_out = ET.SubElement(testcase, 'system-out')
                system_out.text = 'Warnings:\n' + '\n'.join(r.warnings)
        
        tree = ET.ElementTree(root)
        tree.write(filename, encoding='utf-8', xml_declaration=True)
        print(f"📄 JUnit XML report saved to: {filename}")


# ============================================================================
# Main Entry Point
# ============================================================================

def main() -> int:
    """Main entry point with comprehensive error handling"""
    print(f"\n🚀 Scoring Engine Contract Test Suite v{TEST_SUITE_VERSION}")
    print(f"📌 Engine Version: {SCORING_ENGINE_VERSION}")
    
    # Version compatibility check
    if SCORING_ENGINE_VERSION.split('.')[0] != MIN_ENGINE_VERSION.split('.')[0]:
        print(f"\n⚠️  Warning: Engine version {SCORING_ENGINE_VERSION} "
              f"may not be compatible with test suite {MIN_ENGINE_VERSION}")
    
    try:
        # Run tests
        validator = TestValidator()
        results = validator.run_all_tests(parallel=PARALLEL_EXECUTION)
        
        # Run post-validation
        post_results = validator.run_post_validation()
        
        # Print summary
        validator.print_summary()
        
        # Save baseline if requested
        if SAVE_BASELINE:
            validator.save_baseline()
        
        # Generate reports
        if GENERATE_REPORTS:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # JSON report
            json_file = f"scoring_engine_test_report_{timestamp}.json"
            report = validator.generate_report()
            with open(json_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"📄 JSON report saved to: {json_file}")
            
            # HTML report
            html_file = f"scoring_engine_test_report_{timestamp}.html"
            validator.generate_html_report(html_file)
            
            # JUnit XML
            junit_file = f"scoring_engine_test_results_{timestamp}.xml"
            validator.generate_junit_xml(junit_file)
        
        # Return exit code
        return 0 if report['summary']['failed'] == 0 else 1
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Tests interrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
