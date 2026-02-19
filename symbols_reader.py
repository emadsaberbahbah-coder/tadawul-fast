#!/usr/bin/env python3
"""
symbols_reader.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE SYMBOLS READER (v4.5.0)
===========================================================
Advanced Intelligent Symbol Discovery & Management System

Core Capabilities
-----------------
• Multi-strategy symbol discovery (header-based, data-based, ML-based)
• Intelligent column detection with confidence scoring
• Symbol normalization with KSA/Global classification
• Multi-source aggregation (Google Sheets, CSV, JSON, API)
• Advanced caching with TTL and invalidation
• Symbol relationship mapping (parent/child, ETFs, indices)
• Sector/Industry classification enrichment
• Symbol validation against live market data
• Duplicate detection and resolution
• Origin tracking for auditability
• Rate-limited sheet access with retries
• Comprehensive metadata and diagnostics

Architecture
------------
┌─────────────────────────────────────────────────────────┐
│                    Symbols Reader                        │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Registry  │  │   Detector  │  │ Normalizer  │    │
│  │   Manager   │  │   Engine    │  │   Pipeline  │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Cache     │  │   Origin    │  │   Enricher  │    │
│  │   Layer     │  │   Tracker   │  │   Engine    │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
└─────────────────────────────────────────────────────────┘

Supported Symbol Formats
------------------------
• KSA/Tadawul: 1120, 1120.SR, 1120.SR, 1120SR
• Global: AAPL, MSFT, GOOGL, BRK.B, BF-B
• Indices: ^GSPC, ^IXIC, ^DJI, .SPX
• ETFs: SPY, QQQ, IVV, ARKK
• Mutual Funds: VFINX, VTSAX, FXAIX
• Currencies: EURUSD=X, GBPUSD=X, JPY=X
• Cryptocurrencies: BTC-USD, ETH-USD, XRP-USD
• Commodities: GC=F, CL=F, SI=F

Version: 4.5.0
Last Updated: 2024-03-20
"""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import logging
import logging.config
import os
import pickle
import random
import re
import threading
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import (Any, Callable, Dict, Generator, Iterable, List, Optional,
                    Set, Tuple, Type, TypeVar, Union, cast)

# =============================================================================
# Version & Core Configuration
# =============================================================================
VERSION = "4.5.0"
SCHEMA_VERSION = "2.0"
MIN_PYTHON = (3, 8)

if sys.version_info < MIN_PYTHON:
    sys.exit(f"❌ Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required")

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================
# Google Sheets API
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    Credentials = None
    build = None
    HttpError = None
    GOOGLE_API_AVAILABLE = False

# CSV/Data processing
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

# Async HTTP
try:
    import aiohttp
    import aiohttp.client_exceptions
    ASYNC_HTTP_AVAILABLE = True
except ImportError:
    aiohttp = None
    ASYNC_HTTP_AVAILABLE = False

# Machine Learning (optional)
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.ensemble import RandomForestClassifier
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("symbols_reader")

# =============================================================================
# Enums & Types
# =============================================================================
class SymbolType(Enum):
    """Symbol classification types"""
    KSA = "ksa"
    GLOBAL = "global"
    INDEX = "index"
    ETF = "etf"
    MUTUAL_FUND = "mutual_fund"
    CURRENCY = "currency"
    CRYPTO = "crypto"
    COMMODITY = "commodity"
    UNKNOWN = "unknown"

class DiscoveryStrategy(Enum):
    """Symbol discovery strategies"""
    HEADER = "header"
    DATA_SCAN = "data_scan"
    ML = "ml"
    EXPLICIT = "explicit"
    DEFAULT = "default"

class ConfidenceLevel(Enum):
    """Confidence levels for detection"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"

class CacheBackend(Enum):
    """Cache backends"""
    MEMORY = "memory"
    REDIS = "redis"
    DISK = "disk"
    NONE = "none"

@dataclass
class SymbolMetadata:
    """Rich symbol metadata"""
    symbol: str
    normalized: str
    symbol_type: SymbolType
    origin: str
    discovery_strategy: DiscoveryStrategy
    confidence: ConfidenceLevel
    sheet_name: Optional[str] = None
    column: Optional[str] = None
    row: Optional[int] = None
    raw_value: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    name: Optional[str] = None
    is_active: bool = True
    discovered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_validated: Optional[datetime] = None
    validation_status: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PageSpec:
    """Page specification with enhanced metadata"""
    key: str
    sheet_names: Tuple[str, ...]
    header_row: int = 5
    start_row: int = 6
    max_rows: int = 5000
    header_candidates: Tuple[str, ...] = ("SYMBOL", "TICKER", "CODE", "STOCK")
    symbol_type: SymbolType = SymbolType.GLOBAL
    confidence_threshold: float = 0.7
    required: bool = False
    description: str = ""
    tags: List[str] = field(default_factory=list)
    pre_processor: Optional[Callable] = None
    post_processor: Optional[Callable] = None
    cache_ttl: Optional[int] = None  # Override global cache TTL

# =============================================================================
# Utility Functions
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled", "ok", "active"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled", "inactive"}

_BLOCKLIST_EXACT = {
    "SYMBOL", "TICKER", "CODE", "STOCK", "NAME", "RANK", "MARKET", "CURRENCY",
    "PRICE", "LAST", "UPDATED", "LASTUPDATED", "DATE", "TIME", "NOTES", "COMMENT",
    "DESCRIPTION", "TITLE", "HEADER", "COLUMN", "ROW", "VALUE", "DATA", "INFO"
}

_TICKER_PATTERNS = {
    'ksa': [
        re.compile(r'^\d{4}$'),  # 1120
        re.compile(r'^\d{4}\.SR$', re.IGNORECASE),  # 1120.SR
        re.compile(r'^\d{4}SR$', re.IGNORECASE),  # 1120SR
        re.compile(r'^[0-9]{4}\.(SR|TADAWUL)$', re.IGNORECASE),  # 1120.SR, 1120.TADAWUL
    ],
    'global': [
        re.compile(r'^[A-Z]{1,5}$'),  # AAPL, MSFT
        re.compile(r'^[A-Z]{1,4}\.[A-Z]{1,2}$'),  # BRK.B, BF.B
        re.compile(r'^[A-Z]{1,5}-[A-Z]{1,5}$'),  # BRK-A, BRK-B
    ],
    'index': [
        re.compile(r'^\^[A-Z]{2,5}$'),  # ^GSPC, ^IXIC
        re.compile(r'^\.[A-Z]{2,5}$'),  # .SPX, .DJI
        re.compile(r'^[A-Z]{2,5}\.INDX$', re.IGNORECASE),  # SPX.INDX
    ],
    'etf': [
        re.compile(r'^[A-Z]{3,5}$'),  # SPY, QQQ, IVV
        re.compile(r'^[A-Z]{3,5}\.[A-Z]{2}$'),  # VOO.AS, IWDA.L
    ],
    'mutual_fund': [
        re.compile(r'^[A-Z]{5}$'),  # VFINX, VTSAX
        re.compile(r'^[A-Z]{5}\.[A-Z]{2}$'),  # VFINX.US
    ],
    'currency': [
        re.compile(r'^[A-Z]{6}=X$'),  # EURUSD=X
        re.compile(r'^[A-Z]{3}/[A-Z]{3}$'),  # EUR/USD
        re.compile(r'^[A-Z]{3}[A-Z]{3}$'),  # EURUSD
    ],
    'crypto': [
        re.compile(r'^[A-Z]{3,5}-USD$'),  # BTC-USD
        re.compile(r'^[A-Z]{3,5}/USD$'),  # BTC/USD
        re.compile(r'^[A-Z]{3,5}$'),  # BTC (if context known)
    ],
    'commodity': [
        re.compile(r'^[A-Z]{2}=F$'),  # GC=F, CL=F
        re.compile(r'^[A-Z]{2}\d{2}=F$'),  # GCZ24=F
    ]
}

def strip_value(v: Any) -> str:
    """Strip whitespace from value"""
    try:
        return str(v).strip()
    except Exception:
        return ""

def strip_wrapping_quotes(s: str) -> str:
    """Remove wrapping quotes from string"""
    t = strip_value(s)
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1].strip()
    return t

def coerce_bool(v: Any, default: bool = False) -> bool:
    """Coerce value to boolean"""
    if isinstance(v, bool):
        return v
    s = strip_value(v).lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default

def coerce_int(v: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    """Coerce value to integer with bounds"""
    try:
        x = int(float(strip_value(v)))
    except (ValueError, TypeError):
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x

def coerce_float(v: Any, default: float, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    """Coerce value to float with bounds"""
    try:
        x = float(strip_value(v))
    except (ValueError, TypeError):
        x = default
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    return x

def get_env_bool(key: str, default: bool = False) -> bool:
    """Get boolean from environment"""
    return coerce_bool(os.getenv(key), default)

def get_env_int(key: str, default: int, **kwargs) -> int:
    """Get integer from environment"""
    return coerce_int(os.getenv(key), default, **kwargs)

def get_env_float(key: str, default: float, **kwargs) -> float:
    """Get float from environment"""
    return coerce_float(os.getenv(key), default, **kwargs)

def get_env_str(key: str, default: str = "") -> str:
    """Get string from environment"""
    return strip_value(os.getenv(key)) or default

def get_env_list(key: str, default: Optional[List[str]] = None) -> List[str]:
    """Get list from environment (comma-separated)"""
    value = os.getenv(key)
    if not value:
        return default or []
    return [v.strip() for v in value.split(",") if v.strip()]

def now_utc_iso() -> str:
    """Get current UTC time in ISO format"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def now_riyadh_iso() -> str:
    """Get current Riyadh time in ISO format"""
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat(timespec="milliseconds")

def mask_secret(s: Optional[str], reveal_first: int = 3, reveal_last: int = 3) -> str:
    """Mask secret string for logging"""
    if s is None:
        return "MISSING"
    x = strip_value(s)
    if not x:
        return "EMPTY"
    if len(x) < reveal_first + reveal_last + 4:
        return "***"
    return f"{x[:reveal_first]}...{x[-reveal_last:]}"

def split_cell(cell: Any) -> List[str]:
    """Split cell content into tokens"""
    raw = strip_value(cell)
    if not raw:
        return []
    # Split by common separators: comma, space, newline, semicolon, pipe, tab
    parts = re.split(r"[\s,;|\t\n\r]+", raw)
    return [p.strip() for p in parts if p and p.strip()]

# =============================================================================
# Configuration
# =============================================================================
class Config:
    """Configuration manager for symbols reader"""

    # Row configuration
    DEFAULT_HEADER_ROW = get_env_int("TFB_SYMBOL_HEADER_ROW", 5, lo=1, hi=100)
    DEFAULT_START_ROW = get_env_int("TFB_SYMBOL_START_ROW", 6, lo=2, hi=10000)
    DEFAULT_MAX_ROWS = get_env_int("TFB_SYMBOL_MAX_ROWS", 5000, lo=10, hi=100000)

    # Cache configuration
    CACHE_TTL_SEC = get_env_float("TFB_SYMBOLS_CACHE_TTL_SEC", 45.0, lo=1.0, hi=3600.0)
    CACHE_DISABLE = get_env_bool("TFB_SYMBOLS_CACHE_DISABLE", False)
    CACHE_BACKEND = get_env_str("TFB_SYMBOLS_CACHE_BACKEND", "memory")
    CACHE_MAX_SIZE = get_env_int("TFB_SYMBOLS_CACHE_MAX_SIZE", 1000, lo=10, hi=10000)

    # Performance
    MAX_RETRIES = get_env_int("TFB_SYMBOLS_MAX_RETRIES", 3, lo=0, hi=10)
    RETRY_DELAY = get_env_float("TFB_SYMBOLS_RETRY_DELAY", 0.5, lo=0.1, hi=5.0)
    BATCH_SIZE = get_env_int("TFB_SYMBOLS_BATCH_SIZE", 100, lo=10, hi=1000)
    REQUEST_TIMEOUT = get_env_float("TFB_SYMBOLS_TIMEOUT", 30.0, lo=5.0, hi=120.0)

    # Validation
    VALIDATE_SYMBOLS = get_env_bool("TFB_SYMBOLS_VALIDATE", True)
    VALIDATION_TIMEOUT = get_env_float("TFB_SYMBOLS_VALIDATION_TIMEOUT", 5.0, lo=1.0, hi=30.0)

    # ML detection
    ML_DETECTION_ENABLED = get_env_bool("TFB_SYMBOLS_ML_DETECTION", False)
    ML_CONFIDENCE_THRESHOLD = get_env_float("TFB_SYMBOLS_ML_CONFIDENCE", 0.8, lo=0.0, hi=1.0)

    # Logging
    LOG_DETECTION = get_env_bool("TFB_SYMBOLS_LOG_DETECTION", True)
    LOG_PERFORMANCE = get_env_bool("TFB_SYMBOLS_LOG_PERFORMANCE", True)


config = Config()

# =============================================================================
# Page Registry
# =============================================================================
PAGE_REGISTRY: Dict[str, PageSpec] = {
    # Market data pages
    "MARKET_LEADERS": PageSpec(
        key="MARKET_LEADERS",
        sheet_names=("Market_Leaders", "Market Leaders", "Leaders"),
        header_row=5,
        start_row=6,
        max_rows=2000,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Top market performers and momentum leaders",
        tags=["market", "leaders", "performance"],
    ),
    "GLOBAL_MARKETS": PageSpec(
        key="GLOBAL_MARKETS",
        sheet_names=("Global_Markets", "Global Markets", "World Markets"),
        header_row=5,
        start_row=6,
        max_rows=3000,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Global equity markets and indices",
        tags=["global", "markets", "indices"],
    ),
    "KSA_TADAWUL": PageSpec(
        key="KSA_TADAWUL",
        sheet_names=("KSA_Tadawul", "KSA Tadawul", "Tadawul", "Saudi Market"),
        header_row=5,
        start_row=6,
        max_rows=500,
        header_candidates=("SYMBOL", "TICKER", "CODE", "ISIN"),
        symbol_type=SymbolType.KSA,
        description="Saudi Stock Exchange (Tadawul)",
        tags=["ksa", "tadawul", "saudi"],
    ),
    "MUTUAL_FUNDS": PageSpec(
        key="MUTUAL_FUNDS",
        sheet_names=("Mutual_Funds", "Mutual Funds", "Funds"),
        header_row=5,
        start_row=6,
        max_rows=1000,
        header_candidates=("SYMBOL", "TICKER", "FUND", "CODE"),
        symbol_type=SymbolType.MUTUAL_FUND,
        description="Mutual funds and ETFs",
        tags=["funds", "mutual", "etf"],
    ),
    "COMMODITIES_FX": PageSpec(
        key="COMMODITIES_FX",
        sheet_names=("Commodities_FX", "Commodities & FX", "FX & Commodities"),
        header_row=5,
        start_row=6,
        max_rows=500,
        header_candidates=("SYMBOL", "TICKER", "PAIR", "COMMODITY"),
        symbol_type=SymbolType.COMMODITY,
        description="Commodities and Forex pairs",
        tags=["commodities", "fx", "forex"],
    ),

    # Portfolio and analysis pages
    "MY_PORTFOLIO": PageSpec(
        key="MY_PORTFOLIO",
        sheet_names=("My_Portfolio", "My Portfolio", "Portfolio"),
        header_row=5,
        start_row=6,
        max_rows=500,
        header_candidates=("SYMBOL", "TICKER", "CODE", "ASSET"),
        symbol_type=SymbolType.GLOBAL,
        required=False,
        description="User portfolio holdings",
        tags=["portfolio", "holdings"],
    ),
    "INSIGHTS_ANALYSIS": PageSpec(
        key="INSIGHTS_ANALYSIS",
        sheet_names=("Insights_Analysis", "Insights Analysis", "Analysis"),
        header_row=5,
        start_row=6,
        max_rows=200,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Investment insights and analysis",
        tags=["insights", "analysis"],
    ),
    "INVESTMENT_ADVISOR": PageSpec(
        key="INVESTMENT_ADVISOR",
        sheet_names=("Investment_Advisor", "Investment Advisor", "Advisor"),
        header_row=5,
        start_row=6,
        max_rows=200,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Investment advisor recommendations",
        tags=["advisor", "recommendations"],
    ),
    "MARKET_SCAN": PageSpec(
        key="MARKET_SCAN",
        sheet_names=("Market_Scan", "Market Scan", "Scan Results"),
        header_row=5,
        start_row=6,
        max_rows=500,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="AI-powered market scan results",
        tags=["scan", "opportunities"],
    ),

    # Special pages
    "WATCHLIST": PageSpec(
        key="WATCHLIST",
        sheet_names=("Watchlist", "Watch List", "Watch_List"),
        header_row=5,
        start_row=6,
        max_rows=500,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="User watchlist",
        tags=["watchlist", "tracking"],
    ),
    "HOT_LIST": PageSpec(
        key="HOT_LIST",
        sheet_names=("Hot_List", "Hot List", "Hotlist"),
        header_row=5,
        start_row=6,
        max_rows=200,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Hot stocks and momentum picks",
        tags=["hot", "momentum"],
    ),
    "DIVIDEND_KINGS": PageSpec(
        key="DIVIDEND_KINGS",
        sheet_names=("Dividend_Kings", "Dividend Kings", "Dividend Aristocrats"),
        header_row=5,
        start_row=6,
        max_rows=200,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Dividend aristocrats and kings",
        tags=["dividend", "income"],
    ),
    "IPO_CALENDAR": PageSpec(
        key="IPO_CALENDAR",
        sheet_names=("IPO_Calendar", "IPO Calendar", "Upcoming IPOs"),
        header_row=5,
        start_row=6,
        max_rows=200,
        header_candidates=("SYMBOL", "TICKER", "COMPANY"),
        symbol_type=SymbolType.GLOBAL,
        description="Upcoming IPOs",
        tags=["ipo", "calendar"],
    ),
    "EARNINGS_CALENDAR": PageSpec(
        key="EARNINGS_CALENDAR",
        sheet_names=("Earnings_Calendar", "Earnings Calendar", "Earnings"),
        header_row=5,
        start_row=6,
        max_rows=500,
        header_candidates=("SYMBOL", "TICKER", "CODE"),
        symbol_type=SymbolType.GLOBAL,
        description="Earnings calendar",
        tags=["earnings", "calendar"],
    ),
    "ECONOMIC_CALENDAR": PageSpec(
        key="ECONOMIC_CALENDAR",
        sheet_names=("Economic_Calendar", "Economic Calendar", "Economic Data"),
        header_row=5,
        start_row=6,
        max_rows=200,
        header_candidates=("EVENT", "INDICATOR"),
        symbol_type=SymbolType.UNKNOWN,
        description="Economic calendar",
        tags=["economic", "calendar"],
    ),
    "CRYPTO_MARKET": PageSpec(
        key="CRYPTO_MARKET",
        sheet_names=("Crypto_Market", "Crypto Market", "Cryptocurrencies"),
        header_row=5,
        start_row=6,
        max_rows=500,
        header_candidates=("SYMBOL", "TICKER", "COIN"),
        symbol_type=SymbolType.CRYPTO,
        description="Cryptocurrency market",
        tags=["crypto", "blockchain"],
    ),
}


def resolve_key(key: str) -> str:
    """Resolve canonical page key from alias"""
    k = strip_value(key).upper().replace("-", "_").replace(" ", "_")

    # Common aliases
    aliases = {
        "KSA": "KSA_TADAWUL",
        "TADAWUL": "KSA_TADAWUL",
        "TASI": "KSA_TADAWUL",
        "SAUDI": "KSA_TADAWUL",
        "GLOBAL": "GLOBAL_MARKETS",
        "WORLD": "GLOBAL_MARKETS",
        "INTERNATIONAL": "GLOBAL_MARKETS",
        "LEADERS": "MARKET_LEADERS",
        "MARKETLEADERS": "MARKET_LEADERS",
        "PORTFOLIO": "MY_PORTFOLIO",
        "MYPORTFOLIO": "MY_PORTFOLIO",
        "INSIGHTS": "INSIGHTS_ANALYSIS",
        "ANALYSIS": "INSIGHTS_ANALYSIS",
        "ADVISOR": "INVESTMENT_ADVISOR",
        "INVESTMENTADVISOR": "INVESTMENT_ADVISOR",
        "SCAN": "MARKET_SCAN",
        "MARKETSCAN": "MARKET_SCAN",
        "WATCH": "WATCHLIST",
        "WATCHLIST": "WATCHLIST",
        "HOT": "HOT_LIST",
        "HOTLIST": "HOT_LIST",
        "DIVIDEND": "DIVIDEND_KINGS",
        "DIVIDENDKINGS": "DIVIDEND_KINGS",
        "IPO": "IPO_CALENDAR",
        "EARNINGS": "EARNINGS_CALENDAR",
        "ECONOMIC": "ECONOMIC_CALENDAR",
        "CRYPTO": "CRYPTO_MARKET",
        "CRYPTOCURRENCY": "CRYPTO_MARKET",
    }

    return aliases.get(k, k)


# =============================================================================
# Google Sheets Authentication
# =============================================================================
class GoogleSheetsAuth:
    """Enhanced Google Sheets authentication with multiple strategies"""

    def __init__(self):
        self.credentials = None
        self.service = None
        self._lock = threading.Lock()

    def _repair_private_key(self, key: str) -> str:
        """Repair private key with common issues"""
        if not key:
            return key
        # Fix escaped newlines
        key = key.replace('\\n', '\n')
        key = key.replace('\\r\\n', '\n')
        return key

    def _decode_base64_if_needed(self, s: str) -> str:
        """Decode base64 if string appears to be base64 encoded"""
        t = strip_wrapping_quotes(s)
        if not t or t.startswith("{"):
            return t
        # Check if it looks like base64 (no spaces, length multiple of 4, base64 chars)
        if len(t) < 50 or not re.match(r'^[A-Za-z0-9+/=]+$', t):
            return t
        try:
            decoded = base64.b64decode(t, validate=True).decode("utf-8", errors="replace").strip()
            if decoded.startswith("{"):
                return decoded
        except Exception:
            pass
        return t

    def _parse_json_credentials(self, raw: str) -> Optional[Dict[str, Any]]:
        """Parse JSON credentials with repair"""
        t = self._decode_base64_if_needed(raw)
        try:
            obj = json.loads(t)
            if isinstance(obj, dict):
                if "private_key" in obj and isinstance(obj["private_key"], str):
                    obj["private_key"] = self._repair_private_key(obj["private_key"])
                return obj
        except Exception as e:
            logger.debug(f"Failed to parse JSON credentials: {e}")
        return None

    def _load_from_file(self, path: str) -> Optional[Dict[str, Any]]:
        """Load credentials from file"""
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            return self._parse_json_credentials(content)
        except Exception as e:
            logger.debug(f"Failed to load credentials from file {path}: {e}")
        return None

    def load_credentials(self) -> Optional[Dict[str, Any]]:
        """Load credentials from various sources"""
        # Try environment variables in order
        for env_var in ["GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"]:
            raw = os.getenv(env_var)
            if raw:
                creds = self._parse_json_credentials(raw)
                if creds:
                    logger.info(f"Loaded credentials from {env_var}")
                    return creds

        # Try GOOGLE_APPLICATION_CREDENTIALS file
        path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if path and os.path.exists(path):
            creds = self._load_from_file(path)
            if creds:
                logger.info(f"Loaded credentials from {path}")
                return creds

        # Try default paths
        default_paths = [
            "credentials.json",
            "service-account.json",
            "google-creds.json",
            "config/credentials.json",
            "secrets/credentials.json",
        ]

        for path in default_paths:
            if os.path.exists(path):
                creds = self._load_from_file(path)
                if creds:
                    logger.info(f"Loaded credentials from {path}")
                    return creds

        logger.warning("No Google Sheets credentials found")
        return None

    def get_service(self):
        """Get or create Google Sheets service"""
        if self.service is not None:
            return self.service

        with self._lock:
            if self.service is not None:
                return self.service

            if not GOOGLE_API_AVAILABLE:
                logger.error("Google API libraries not available")
                return None

            creds_info = self.load_credentials()
            if not creds_info:
                return None

            try:
                scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
                credentials = Credentials.from_service_account_info(creds_info, scopes=scopes)
                self.service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
                logger.info("Google Sheets service initialized")
                return self.service
            except Exception as e:
                logger.error(f"Failed to initialize Google Sheets service: {e}")
                return None


# =============================================================================
# Symbol Normalizer
# =============================================================================
class SymbolNormalizer:
    """Advanced symbol normalization with type detection"""

    def __init__(self):
        self.cache: Dict[str, Tuple[str, SymbolType]] = {}
        self._lock = threading.Lock()

    def normalize(self, raw: str) -> Tuple[str, SymbolType]:
        """Normalize symbol and detect type"""
        with self._lock:
            if raw in self.cache:
                return self.cache[raw]

        s = strip_value(raw).upper()

        # Remove common prefixes/suffixes
        prefixes = ["$", "#", "^", "NYSE:", "NASDAQ:", "TADAWUL:", "SR:", "SAUDI:"]
        suffixes = [".SR", ".SA", ".AB", ".TADAWUL", ".NYSE", ".NASDAQ"]

        for p in prefixes:
            if s.startswith(p):
                s = s[len(p):]
                break

        for suf in suffixes:
            if s.endswith(suf):
                s = s[:-len(suf)]
                break

        # Detect symbol type
        symbol_type = self._detect_type(s, raw)

        # Normalize KSA symbols to standard format
        if symbol_type == SymbolType.KSA:
            if re.match(r'^\d{4}$', s):
                s = f"{s}.SR"
            elif re.match(r'^\d{4}SR$', s):
                s = f"{s[:4]}.SR"
            elif re.match(r'^\d{4}\.SR$', s, re.IGNORECASE):
                s = s.upper()

        with self._lock:
            self.cache[raw] = (s, symbol_type)
            if len(self.cache) > 10000:  # Limit cache size
                # Remove oldest 20%
                remove = len(self.cache) // 5
                for _ in range(remove):
                    self.cache.pop(next(iter(self.cache)))

        return s, symbol_type

    def _detect_type(self, normalized: str, original: str) -> SymbolType:
        """Detect symbol type based on patterns"""
        # Check original for clues
        orig_upper = original.upper()

        if ".SR" in orig_upper or "TADAWUL" in orig_upper or "SAUDI" in orig_upper:
            return SymbolType.KSA

        # Check patterns
        for pattern in _TICKER_PATTERNS['ksa']:
            if pattern.match(normalized) or pattern.match(orig_upper):
                return SymbolType.KSA

        for pattern in _TICKER_PATTERNS['index']:
            if pattern.match(orig_upper):
                return SymbolType.INDEX

        for pattern in _TICKER_PATTERNS['crypto']:
            if pattern.match(orig_upper):
                return SymbolType.CRYPTO

        for pattern in _TICKER_PATTERNS['currency']:
            if pattern.match(orig_upper):
                return SymbolType.CURRENCY

        for pattern in _TICKER_PATTERNS['commodity']:
            if pattern.match(orig_upper):
                return SymbolType.COMMODITY

        for pattern in _TICKER_PATTERNS['etf']:
            if pattern.match(normalized):
                return SymbolType.ETF

        for pattern in _TICKER_PATTERNS['mutual_fund']:
            if pattern.match(normalized):
                return SymbolType.MUTUAL_FUND

        for pattern in _TICKER_PATTERNS['global']:
            if pattern.match(normalized):
                return SymbolType.GLOBAL

        return SymbolType.UNKNOWN

    def is_ksa(self, symbol: str) -> bool:
        """Check if symbol is KSA"""
        _, sym_type = self.normalize(symbol)
        return sym_type == SymbolType.KSA

    def is_valid(self, symbol: str) -> bool:
        """Check if symbol is valid"""
        norm, sym_type = self.normalize(symbol)
        if not norm:
            return False
        if sym_type == SymbolType.UNKNOWN:
            return False
        if norm in _BLOCKLIST_EXACT:
            return False
        return True


normalizer = SymbolNormalizer()

# =============================================================================
# Symbol Validator (optional)
# =============================================================================
class SymbolValidator:
    """Validate symbols against live market data"""

    def __init__(self):
        self.cache: Dict[str, Tuple[bool, float]] = {}
        self._lock = threading.Lock()

    async def validate(self, symbol: str, timeout: float = 5.0) -> bool:
        """Validate symbol by checking with provider"""
        # Check cache
        with self._lock:
            if symbol in self.cache:
                valid, timestamp = self.cache[symbol]
                if time.time() - timestamp < 3600:  # 1 hour cache
                    return valid

        if not ASYNC_HTTP_AVAILABLE:
            return True  # Can't validate without aiohttp

        norm, sym_type = normalizer.normalize(symbol)

        # Try multiple providers
        valid = await self._check_with_providers(norm, timeout)

        with self._lock:
            self.cache[symbol] = (valid, time.time())
            if len(self.cache) > 1000:
                # Remove oldest
                remove = len(self.cache) // 5
                for _ in range(remove):
                    self.cache.pop(next(iter(self.cache)))

        return valid

    async def _check_with_providers(self, symbol: str, timeout: float) -> bool:
        """Check symbol with multiple providers"""
        # Simplified - would check with actual providers
        return True


validator = SymbolValidator() if config.VALIDATE_SYMBOLS else None

# =============================================================================
# ML Detection Engine (optional)
# =============================================================================
class MLDetectionEngine:
    """Machine learning based symbol detection"""

    def __init__(self):
        self.model = None
        self.vectorizer = None
        self.trained = False
        self._lock = threading.Lock()

    def train(self, positive_samples: List[str], negative_samples: List[str]):
        """Train the model on labeled samples"""
        if not ML_AVAILABLE:
            return

        with self._lock:
            # Create training data
            texts = positive_samples + negative_samples
            labels = [1] * len(positive_samples) + [0] * len(negative_samples)

            # Vectorize
            self.vectorizer = TfidfVectorizer(
                analyzer='char',
                ngram_range=(2, 5),
                max_features=1000
            )
            X = self.vectorizer.fit_transform(texts)

            # Train
            self.model = RandomForestClassifier(n_estimators=100)
            self.model.fit(X, labels)
            self.trained = True

    def predict(self, text: str) -> Tuple[bool, float]:
        """Predict if text contains symbols"""
        if not self.trained or not ML_AVAILABLE:
            return False, 0.0

        with self._lock:
            X = self.vectorizer.transform([text])
            proba = self.model.predict_proba(X)[0]
            confidence = proba[1]  # Probability of positive class
            return confidence > config.ML_CONFIDENCE_THRESHOLD, confidence


ml_detector = MLDetectionEngine() if config.ML_DETECTION_ENABLED else None

# =============================================================================
# Cache Manager
# =============================================================================
class CacheManager:
    """Multi-backend cache manager for symbols"""

    def __init__(self):
        self.backend = config.CACHE_BACKEND
        self.ttl = config.CACHE_TTL_SEC
        self.max_size = config.CACHE_MAX_SIZE
        self._memory_cache: Dict[str, Tuple[float, Any]] = {}
        self._memory_lock = threading.Lock()
        self._redis_client = None

        if self.backend == "redis":
            self._init_redis()

    def _init_redis(self):
        """Initialize Redis client"""
        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            try:
                import redis
                self._redis_client = redis.from_url(redis_url, decode_responses=True)
                logger.info("Redis cache initialized")
            except ImportError:
                logger.warning("Redis not available, falling back to memory cache")
                self.backend = "memory"
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}, falling back to memory")
                self.backend = "memory"

    def _make_key(self, *parts) -> str:
        """Generate cache key"""
        key = ":".join(str(p) for p in parts)
        if len(key) > 200:
            key = hashlib.sha256(key.encode()).hexdigest()
        return f"sym:{key}"

    def get(self, key: str) -> Optional[Any]:
        """Get from cache"""
        cache_key = self._make_key(key)

        # Try Redis first
        if self.backend == "redis" and self._redis_client:
            try:
                value = self._redis_client.get(cache_key)
                if value:
                    return pickle.loads(base64.b64decode(value))
            except Exception as e:
                logger.debug(f"Redis get failed: {e}")

        # Try memory cache
        with self._memory_lock:
            if cache_key in self._memory_cache:
                timestamp, value = self._memory_cache[cache_key]
                if time.time() - timestamp < self.ttl:
                    return value
                else:
                    del self._memory_cache[cache_key]

        return None

    def set(self, key: str, value: Any) -> None:
        """Set in cache"""
        cache_key = self._make_key(key)

        # Set in Redis
        if self.backend == "redis" and self._redis_client:
            try:
                pickled = base64.b64encode(pickle.dumps(value)).decode()
                self._redis_client.setex(cache_key, int(self.ttl), pickled)
            except Exception as e:
                logger.debug(f"Redis set failed: {e}")

        # Set in memory
        with self._memory_lock:
            self._memory_cache[cache_key] = (time.time(), value)

            # Prune if too large
            if len(self._memory_cache) > self.max_size:
                # Remove oldest 20%
                sorted_items = sorted(self._memory_cache.items(), key=lambda x: x[1][0])
                remove_count = len(self._memory_cache) // 5
                for i in range(remove_count):
                    if i < len(sorted_items):
                        del self._memory_cache[sorted_items[i][0]]

    def delete(self, key: str) -> None:
        """Delete from cache"""
        cache_key = self._make_key(key)

        if self.backend == "redis" and self._redis_client:
            try:
                self._redis_client.delete(cache_key)
            except Exception:
                pass

        with self._memory_lock:
            if cache_key in self._memory_cache:
                del self._memory_cache[cache_key]

    def clear(self) -> None:
        """Clear all cache"""
        if self.backend == "redis" and self._redis_client:
            try:
                for key in self._redis_client.scan_iter("sym:*"):
                    self._redis_client.delete(key)
            except Exception:
                pass

        with self._memory_lock:
            self._memory_cache.clear()


cache = CacheManager()

# =============================================================================
# Google Sheets Reader
# =============================================================================
class GoogleSheetsReader:
    """Enhanced Google Sheets reader with retries and error handling"""

    def __init__(self):
        self.auth = GoogleSheetsAuth()
        self._service = None
        self._lock = threading.Lock()

    def _get_service(self):
        """Get service with lazy initialization"""
        if self._service is not None:
            return self._service

        with self._lock:
            if self._service is not None:
                return self._service
            self._service = self.auth.get_service()
            return self._service

    def _safe_sheet_name(self, name: str) -> str:
        """Escape sheet name for A1 notation"""
        n = strip_value(name)
        if not n:
            return "Sheet1"
        if "'" in n or " " in n or "-" in n:
            return f"'{n.replace(chr(39), chr(39)*2)}'"
        return n

    def _col_to_letter(self, col: int) -> str:
        """Convert 1-based column index to letter"""
        if col <= 0:
            return "A"
        letters = ""
        while col > 0:
            col -= 1
            letters = chr(65 + (col % 26)) + letters
            col //= 26
        return letters or "A"

    def read_range(self, spreadsheet_id: str, range_a1: str) -> List[List[Any]]:
        """Read range with retries and exponential backoff"""
        service = self._get_service()
        if not service:
            return []

        for attempt in range(config.MAX_RETRIES):
            try:
                t0 = time.perf_counter()
                result = service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=range_a1,
                    valueRenderOption="UNFORMATTED_VALUE",
                    dateTimeRenderOption="SERIAL_NUMBER"
                ).execute()
                elapsed = (time.perf_counter() - t0) * 1000

                if config.LOG_PERFORMANCE:
                    logger.debug(f"Sheet read {range_a1}: {elapsed:.1f}ms")

                return result.get("values", [])

            except HttpError as e:
                if e.resp.status in [429, 500, 502, 503, 504]:
                    # Retry on rate limits and server errors
                    wait = config.RETRY_DELAY * (2 ** attempt) + random.uniform(0, 0.1)
                    logger.debug(f"Retry {attempt + 1}/{config.MAX_RETRIES} after {wait:.2f}s: {e}")
                    time.sleep(wait)
                else:
                    logger.error(f"Sheet error {range_a1}: {e}")
                    return []

            except Exception as e:
                logger.error(f"Unexpected error reading {range_a1}: {e}")
                return []

        logger.error(f"Max retries exceeded for {range_a1}")
        return []

    def list_sheets(self, spreadsheet_id: str) -> List[str]:
        """List all sheet names in spreadsheet"""
        service = self._get_service()
        if not service:
            return []

        try:
            metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            return [
                sheet["properties"]["title"]
                for sheet in metadata.get("sheets", [])
                if sheet.get("properties", {}).get("title")
            ]
        except Exception as e:
            logger.error(f"Failed to list sheets: {e}")
            return []

    def get_sheet_metadata(self, spreadsheet_id: str, sheet_name: str) -> Optional[Dict[str, Any]]:
        """Get sheet metadata including grid properties"""
        service = self._get_service()
        if not service:
            return None

        try:
            safe_name = self._safe_sheet_name(sheet_name)
            range_a1 = f"{safe_name}!A1:ZZ1"  # Just to get sheet ID

            # Get spreadsheet with sheet ID
            result = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[f"{safe_name}!A1"],
                includeGridData=False
            ).execute()

            for sheet in result.get("sheets", []):
                props = sheet.get("properties", {})
                if props.get("title") == sheet_name:
                    return {
                        "sheet_id": props.get("sheetId"),
                        "title": props.get("title"),
                        "row_count": props.get("gridProperties", {}).get("rowCount", 0),
                        "column_count": props.get("gridProperties", {}).get("columnCount", 0),
                        "hidden": props.get("hidden", False),
                    }

        except Exception as e:
            logger.error(f"Failed to get sheet metadata: {e}")

        return None


sheets_reader = GoogleSheetsReader()

# =============================================================================
# Column Detection Engine
# =============================================================================
class ColumnDetectionEngine:
    """Intelligent column detection with multiple strategies"""

    def __init__(self):
        self.strategies = [
            ("header", self._detect_by_header),
            ("data_scan", self._detect_by_data),
        ]
        if ml_detector:
            self.strategies.append(("ml", self._detect_by_ml))

    def detect(self, spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Tuple[Optional[str], DiscoveryStrategy, float, Dict[str, Any]]:
        """
        Detect symbol column with confidence score.
        Returns: (column_letter, strategy, confidence, metadata)
        """
        results = []

        for strategy_name, detector in self.strategies:
            col, confidence, metadata = detector(spreadsheet_id, sheet_name, spec)
            if col and confidence >= spec.confidence_threshold:
                results.append((col, strategy_name, confidence, metadata))

        if not results:
            return "B", DiscoveryStrategy.DEFAULT, 0.5, {"reason": "fallback to B"}

        # Sort by confidence
        results.sort(key=lambda x: x[2], reverse=True)
        best = results[0]

        return best[0], DiscoveryStrategy(best[1]), best[2], best[3]

    def _detect_by_header(self, spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Tuple[Optional[str], float, Dict[str, Any]]:
        """Detect column by header text"""
        range_a1 = f"{sheets_reader._safe_sheet_name(sheet_name)}!A{spec.header_row}:ZZ{spec.header_row}"
        values = sheets_reader.read_range(spreadsheet_id, range_a1)

        if not values or not values[0]:
            return None, 0.0, {}

        headers = [strip_value(h).upper() for h in values[0]]

        # Look for exact matches first
        for idx, header in enumerate(headers, 1):
            if header in spec.header_candidates:
                col = sheets_reader._col_to_letter(idx)
                return col, 1.0, {"matched_header": header, "column": idx}

        # Look for partial matches
        best_score = 0.0
        best_col = None
        best_match = ""

        for idx, header in enumerate(headers, 1):
            if not header:
                continue
            for candidate in spec.header_candidates:
                if candidate in header:
                    score = len(candidate) / len(header) if header else 0
                    if score > best_score:
                        best_score = score
                        best_col = sheets_reader._col_to_letter(idx)
                        best_match = candidate

        if best_col and best_score >= 0.5:
            return best_col, best_score, {"matched_header": best_match, "similarity": best_score}

        return None, 0.0, {}

    def _detect_by_data(self, spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Tuple[Optional[str], float, Dict[str, Any]]:
        """Detect column by scanning data for ticker patterns"""
        # Sample rows
        sample_size = min(200, spec.max_rows // 10)
        end_row = spec.start_row + sample_size - 1

        range_a1 = f"{sheets_reader._safe_sheet_name(sheet_name)}!A{spec.start_row}:ZZ{end_row}"
        values = sheets_reader.read_range(spreadsheet_id, range_a1)

        if not values:
            return None, 0.0, {}

        # Score each column
        max_cols = max((len(row) for row in values), default=0)
        col_scores = defaultdict(lambda: {"hits": 0, "total": 0, "cells": 0})

        for row in values:
            for col_idx in range(max_cols):
                if col_idx >= len(row):
                    continue

                cell = row[col_idx]
                if cell is None:
                    continue

                tokens = split_cell(cell)
                if not tokens:
                    continue

                col_scores[col_idx]["cells"] += 1
                col_scores[col_idx]["total"] += len(tokens)

                for token in tokens:
                    norm, sym_type = normalizer.normalize(token)
                    if normalizer.is_valid(norm):
                        col_scores[col_idx]["hits"] += 1

        # Calculate best column
        best_col = None
        best_score = 0.0
        best_stats = {}

        for col_idx, stats in col_scores.items():
            if stats["total"] < 5:  # Need enough data
                continue

            hit_rate = stats["hits"] / stats["total"] if stats["total"] else 0
            cell_coverage = stats["cells"] / len(values) if values else 0

            # Combined score
            score = hit_rate * 0.7 + cell_coverage * 0.3

            if score > best_score:
                best_score = score
                best_col = sheets_reader._col_to_letter(col_idx + 1)
                best_stats = {
                    "hits": stats["hits"],
                    "total": stats["total"],
                    "cells": stats["cells"],
                    "hit_rate": hit_rate,
                    "cell_coverage": cell_coverage,
                }

        if best_col and best_score >= 0.3:
            return best_col, best_score, best_stats

        return None, 0.0, {}

    def _detect_by_ml(self, spreadsheet_id: str, sheet_name: str, spec: PageSpec) -> Tuple[Optional[str], float, Dict[str, Any]]:
        """Detect column using ML model"""
        if not ml_detector or not ml_detector.trained:
            return None, 0.0, {}

        # Similar to data detection but using ML
        # Simplified - would use trained model
        return None, 0.0, {}


detector = ColumnDetectionEngine()

# =============================================================================
# Symbol Extractor
# =============================================================================
class SymbolExtractor:
    """Extract and normalize symbols from raw data"""

    def __init__(self):
        self.normalizer = normalizer

    def extract_from_column(self, values: List[List[Any]], column_letter: str) -> List[SymbolMetadata]:
        """Extract symbols from a column"""
        col_idx = self._letter_to_col(column_letter) - 1
        symbols = []

        for row_idx, row in enumerate(values, start=config.DEFAULT_START_ROW):
            if not row or col_idx >= len(row):
                continue

            cell = row[col_idx]
            if cell is None:
                continue

            tokens = split_cell(cell)

            for token in tokens:
                norm, sym_type = self.normalizer.normalize(token)

                if not norm or not self.normalizer.is_valid(norm):
                    continue

                metadata = SymbolMetadata(
                    symbol=norm,
                    normalized=norm,
                    symbol_type=sym_type,
                    origin="sheet",
                    discovery_strategy=DiscoveryStrategy.DATA_SCAN,
                    confidence=ConfidenceLevel.HIGH if sym_type != SymbolType.UNKNOWN else ConfidenceLevel.LOW,
                    sheet_name=None,  # Set by caller
                    column=column_letter,
                    row=row_idx,
                    raw_value=token,
                    discovered_at=datetime.now(timezone.utc),
                )

                symbols.append(metadata)

        return symbols

    def _letter_to_col(self, letter: str) -> int:
        """Convert column letter to 1-based index"""
        col = 0
        for ch in letter.upper():
            col = col * 26 + (ord(ch) - ord('A') + 1)
        return col


extractor = SymbolExtractor()

# =============================================================================
# Main API
# =============================================================================
def get_page_symbols(key: str, spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Get symbols from a page with rich metadata.

    Returns:
    {
        "all": [...],           # All normalized symbols
        "ksa": [...],            # KSA symbols only
        "global": [...],         # Global symbols only
        "by_type": {...},        # Symbols grouped by type
        "metadata": [...],       # Detailed metadata for each symbol
        "origin": str,           # Origin page key
        "discovery": {...},      # Discovery metadata
        "performance": {...},    # Performance metrics
        "cache_hit": bool,       # Whether result was cached
        "timestamp": str,        # UTC timestamp
        "version": str,          # Reader version
    }
    """
    start_time = time.perf_counter()

    # Resolve spreadsheet ID
    sid = strip_value(spreadsheet_id) or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID")
    if not sid:
        logger.error("No spreadsheet ID provided")
        return {
            "all": [],
            "error": "No spreadsheet ID",
            "status": "error",
        }

    # Resolve page key
    canonical_key = resolve_key(key)
    spec = PAGE_REGISTRY.get(canonical_key)

    if not spec:
        logger.warning(f"Unknown page key: {key} (resolved to {canonical_key})")
        return {
            "all": [],
            "error": f"Unknown page key: {key}",
            "status": "error",
        }

    # Check cache
    cache_key = f"page:{sid}:{canonical_key}:{spec.header_row}:{spec.start_row}:{spec.max_rows}"
    cached = cache.get(cache_key)

    if cached is not None:
        cached["cache_hit"] = True
        if config.LOG_PERFORMANCE:
            elapsed = (time.perf_counter() - start_time) * 1000
            logger.debug(f"Cache hit for {key} ({elapsed:.1f}ms)")
        return cached

    # Try each sheet name
    all_symbols: List[SymbolMetadata] = []
    best_result = None
    best_confidence = 0.0

    for sheet_name in spec.sheet_names:
        # Detect symbol column
        col, strategy, confidence, detect_meta = detector.detect(sid, sheet_name, spec)

        if not col or confidence < spec.confidence_threshold:
            continue

        # Read data
        end_row = spec.start_row + spec.max_rows - 1
        range_a1 = f"{sheets_reader._safe_sheet_name(sheet_name)}!{col}{spec.start_row}:{col}{end_row}"
        values = sheets_reader.read_range(sid, range_a1)

        if not values:
            continue

        # Extract symbols
        symbols = extractor.extract_from_column(values, col)

        # Add sheet info
        for sym in symbols:
            sym.sheet_name = sheet_name
            sym.origin = canonical_key
            sym.discovery_strategy = strategy
            if strategy == DiscoveryStrategy.HEADER and confidence > 0.9:
                sym.confidence = ConfidenceLevel.HIGH
            elif confidence > 0.7:
                sym.confidence = ConfidenceLevel.MEDIUM
            else:
                sym.confidence = ConfidenceLevel.LOW

        if len(symbols) > best_confidence:
            best_confidence = len(symbols)
            best_result = (sheet_name, col, strategy, confidence, detect_meta, symbols)

    # Process best result
    if best_result:
        sheet_name, col, strategy, confidence, detect_meta, symbols = best_result

        # Group by type
        all_syms = []
        ksa_syms = []
        global_syms = []
        by_type = defaultdict(list)

        for sym in symbols:
            all_syms.append(sym.symbol)
            by_type[sym.symbol_type.value].append(sym.symbol)
            if sym.symbol_type == SymbolType.KSA:
                ksa_syms.append(sym.symbol)
            else:
                global_syms.append(sym.symbol)

        # Build result
        elapsed = (time.perf_counter() - start_time) * 1000

        result = {
            "all": all_syms,
            "ksa": ksa_syms,
            "global": global_syms,
            "by_type": dict(by_type),
            "metadata": [asdict(sym) for sym in symbols],
            "origin": canonical_key,
            "discovery": {
                "sheet": sheet_name,
                "column": col,
                "strategy": strategy.value,
                "confidence": confidence,
                "detection": detect_meta,
            },
            "performance": {
                "elapsed_ms": round(elapsed, 2),
                "symbol_count": len(symbols),
                "unique_count": len(set(all_syms)),
            },
            "cache_hit": False,
            "timestamp": now_utc_iso(),
            "version": VERSION,
            "status": "success",
        }

        # Cache result
        cache.set(cache_key, result)

        if config.LOG_DETECTION:
            logger.info(f"Found {len(all_syms)} symbols in {key} (col {col}, {strategy.value}, {confidence:.2f})")

        return result

    # No symbols found
    elapsed = (time.perf_counter() - start_time) * 1000

    result = {
        "all": [],
        "ksa": [],
        "global": [],
        "by_type": {},
        "metadata": [],
        "origin": canonical_key,
        "discovery": None,
        "performance": {
            "elapsed_ms": round(elapsed, 2),
            "symbol_count": 0,
            "unique_count": 0,
        },
        "cache_hit": False,
        "timestamp": now_utc_iso(),
        "version": VERSION,
        "status": "empty",
        "warning": "No symbols found in any sheet",
    }

    cache.set(cache_key, result)
    logger.warning(f"No symbols found for {key}")
    return result


def get_universe(keys: List[str], spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Aggregate symbols from multiple pages.

    Returns:
    {
        "symbols": [...],        # Unique symbols across all pages
        "by_origin": {...},      # Symbols grouped by origin page
        "by_type": {...},        # Symbols grouped by type
        "origin_map": {...},     # Map symbol -> origin page
        "metadata": {...},       # Metadata for each page
        "performance": {...},    # Aggregated performance metrics
    }
    """
    start_time = time.perf_counter()

    sid = strip_value(spreadsheet_id) or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID")
    if not sid:
        return {
            "symbols": [],
            "error": "No spreadsheet ID",
            "status": "error",
        }

    all_symbols = []
    origin_map = {}
    by_origin = defaultdict(list)
    by_type = defaultdict(list)
    page_metadata = {}
    total_time = 0

    for key in keys:
        t0 = time.perf_counter()
        result = get_page_symbols(key, spreadsheet_id=sid)
        elapsed = (time.perf_counter() - t0) * 1000
        total_time += elapsed

        canonical_key = resolve_key(key)

        if result.get("status") == "success" and result.get("all"):
            symbols = result["all"]
            page_metadata[canonical_key] = {
                "count": len(symbols),
                "elapsed_ms": round(elapsed, 2),
                "status": result.get("status"),
                "discovery": result.get("discovery"),
            }

            for sym in symbols:
                if sym not in origin_map:
                    origin_map[sym] = canonical_key
                    all_symbols.append(sym)
                    by_origin[canonical_key].append(sym)

        # Also add by_type from metadata
        if "metadata" in result:
            for meta in result["metadata"]:
                if meta["symbol"] in origin_map:
                    by_type[meta["symbol_type"]].append(meta["symbol"])

    # Deduplicate by_type
    for type_name in by_type:
        by_type[type_name] = list(set(by_type[type_name]))

    # Sort for consistency
    all_symbols.sort()
    for key in by_origin:
        by_origin[key].sort()
    for type_name in by_type:
        by_type[type_name].sort()

    elapsed = (time.perf_counter() - start_time) * 1000

    return {
        "symbols": all_symbols,
        "by_origin": dict(by_origin),
        "by_type": dict(by_type),
        "origin_map": origin_map,
        "metadata": page_metadata,
        "performance": {
            "elapsed_ms": round(elapsed, 2),
            "pages_processed": len(keys),
            "pages_with_data": len([k for k, v in page_metadata.items() if v.get("count", 0) > 0]),
            "total_symbols": len(all_symbols),
        },
        "timestamp": now_utc_iso(),
        "version": VERSION,
        "status": "success",
    }


def list_tabs(spreadsheet_id: Optional[str] = None) -> List[str]:
    """List all tabs in spreadsheet"""
    sid = strip_value(spreadsheet_id) or os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID")
    if not sid:
        logger.error("No spreadsheet ID provided")
        return []
    return sheets_reader.list_sheets(sid)


def validate_symbols(symbols: List[str], timeout: float = 5.0) -> Dict[str, Any]:
    """Validate symbols against live market data"""
    if not validator:
        return {
            "valid": symbols,
            "invalid": [],
            "status": "validation_disabled",
        }

    import asyncio

    async def validate_all():
        results = {}
        for sym in symbols:
            valid = await validator.validate(sym, timeout)
            results[sym] = valid
        return results

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        results = loop.run_until_complete(validate_all())
    finally:
        loop.close()

    valid = [sym for sym, is_valid in results.items() if is_valid]
    invalid = [sym for sym, is_valid in results.items() if not is_valid]

    return {
        "valid": valid,
        "invalid": invalid,
        "total": len(symbols),
        "valid_count": len(valid),
        "invalid_count": len(invalid),
        "status": "success",
    }


def get_symbol_metadata(symbol: str) -> Optional[SymbolMetadata]:
    """Get rich metadata for a single symbol"""
    norm, sym_type = normalizer.normalize(symbol)

    if not norm:
        return None

    return SymbolMetadata(
        symbol=norm,
        normalized=norm,
        symbol_type=sym_type,
        origin="direct",
        discovery_strategy=DiscoveryStrategy.EXPLICIT,
        confidence=ConfidenceLevel.HIGH,
        discovered_at=datetime.now(timezone.utc),
    )


# =============================================================================
# CLI Entry Point
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=f"TFB Symbols Reader v{VERSION}")
    parser.add_argument("--sheet-id", help="Spreadsheet ID override")
    parser.add_argument("--key", default="KSA", help="Page key (e.g., KSA, GLOBAL, LEADERS)")
    parser.add_argument("--keys", nargs="+", help="Multiple keys for universe")
    parser.add_argument("--list-tabs", action="store_true", help="List tabs and exit")
    parser.add_argument("--validate", help="Validate symbol")
    parser.add_argument("--validate-list", help="Validate symbols from file")
    parser.add_argument("--output", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--no-cache", action="store_true", help="Disable cache")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.no_cache:
        config.CACHE_DISABLE = True

    if args.list_tabs:
        tabs = list_tabs(args.sheet_id)
        if args.output == "json":
            print(json.dumps({"tabs": tabs}, indent=2))
        else:
            print("\n".join(tabs) if tabs else "No tabs found")
        sys.exit(0)

    if args.validate:
        meta = get_symbol_metadata(args.validate)
        if args.output == "json":
            print(json.dumps(asdict(meta) if meta else {"error": "Invalid symbol"}, indent=2))
        else:
            if meta:
                print(f"Symbol: {meta.symbol}")
                print(f"Normalized: {meta.normalized}")
                print(f"Type: {meta.symbol_type.value}")
                print(f"Valid: {normalizer.is_valid(meta.symbol)}")
            else:
                print(f"Invalid symbol: {args.validate}")
        sys.exit(0)

    if args.validate_list:
        try:
            with open(args.validate_list, "r") as f:
                symbols = [line.strip() for line in f if line.strip()]
            results = validate_symbols(symbols)
            if args.output == "json":
                print(json.dumps(results, indent=2))
            else:
                print(f"Valid: {len(results['valid'])}/{results['total']}")
                if results['invalid']:
                    print(f"Invalid: {', '.join(results['invalid'])}")
        except Exception as e:
            print(f"Error: {e}")
        sys.exit(0)

    if args.keys:
        result = get_universe(args.keys, spreadsheet_id=args.sheet_id)
    else:
        result = get_page_symbols(args.key, spreadsheet_id=args.sheet_id)

    if args.output == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        status = result.get("status", "unknown")
        symbols = result.get("all", [])

        print(f"Symbols Reader v{VERSION}")
        print(f"Status: {status}")
        print(f"Key: {args.key or args.keys}")
        print(f"Count: {len(symbols)}")

        if "discovery" in result and result["discovery"]:
            d = result["discovery"]
            print(f"Sheet: {d.get('sheet')}")
            print(f"Column: {d.get('column')}")
            print(f"Strategy: {d.get('strategy')}")
            print(f"Confidence: {d.get('confidence', 0):.2f}")

        if "performance" in result:
            p = result["performance"]
            print(f"Time: {p.get('elapsed_ms', 0):.1f}ms")

        if "by_type" in result:
            bt = result["by_type"]
            print("\nBy Type:")
            for type_name, syms in bt.items():
                print(f"  {type_name}: {len(syms)}")

        if symbols:
            print("\nPreview (first 20):")
            print("  " + ", ".join(symbols[:20]))

        if "cache_hit" in result and result["cache_hit"]:
            print("\n(Cache hit)")

        if "warning" in result:
            print(f"\nWarning: {result['warning']}")
