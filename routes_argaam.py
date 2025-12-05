# =============================================================================
# Argaam Integration Routes - Enhanced Production Version (Lightweight)
# Version: 4.1.0
# Integration: Tadawul Stock Analysis API
# Description:
#   Professional Arabic/English stock data scraping with caching,
#   simplified dependencies (no prometheus_client, cachetools, structlog)
# =============================================================================

import asyncio
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Literal, Tuple

from urllib.parse import quote

# Core Framework
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    status,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ConfigDict
from pydantic.functional_validators import field_validator

# HTTP Clients
import httpx
from httpx import AsyncClient, Timeout, Limits

# Web Scraping
from bs4 import BeautifulSoup

# =============================================================================
# Logging
# =============================================================================

logger = logging.getLogger("argaam")
if not logger.handlers:
    # Basic config if not already configured by main app
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

# =============================================================================
# Configuration Models
# =============================================================================


class ArgaamEnvironment(str, Enum):
    """Argaam environment modes."""

    PRODUCTION = "production"
    DEVELOPMENT = "development"
    TESTING = "testing"


class ArgaamSettings(BaseModel):
    """Argaam-specific configuration."""

    # Service Configuration
    enabled: bool = Field(True, description="Enable Argaam integration")
    base_url: str = Field("https://www.argaam.com", description="Argaam base URL")
    timeout: int = Field(30, description="HTTP request timeout in seconds")
    max_retries: int = Field(3, description="Maximum retry attempts")
    rate_limit_delay: float = Field(0.15, description="Delay between requests")

    # Cache Configuration
    cache_enabled: bool = Field(True, description="Enable caching")
    cache_ttl: int = Field(600, description="Cache TTL in seconds")
    cache_maxsize: int = Field(1000, description="Maximum cache size")

    # Rate Limiting
    requests_per_minute: int = Field(60, description="Max requests per minute")
    concurrent_requests: int = Field(10, description="Max concurrent requests")

    # HTML Parsing
    html_parsers: List[str] = Field(
        default=["html.parser"],
        description="HTML parsers in order of preference",
    )

    # User Agent
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        description="User agent for requests",
    )

    # Ticker Normalization
    ticker_pattern: str = Field(
        r"(\d{1,4})", description="Regex for ticker normalization"
    )

    model_config = ConfigDict(
        env_prefix="ARGAAM_",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        if v < 5:
            return 5
        if v > 120:
            return 120
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        if v < 0:
            return 0
        if v > 5:
            return 5
        return v


# =============================================================================
# Lightweight Metrics (no prometheus_client)
# =============================================================================


class ArgaamMetrics:
    """In-memory counters for Argaam integration."""

    def __init__(self) -> None:
        # Request metrics
        self.requests_total: Dict[Tuple[str, str], int] = {}
        self.request_durations: Dict[str, List[float]] = {}

        # Cache metrics
        self.cache_hits_total: int = 0
        self.cache_misses_total: int = 0

        # Data quality counters
        self.data_quality_counts: Dict[str, int] = {}

        # Error metrics
        self.errors_total: Dict[str, int] = {}

    # --- Requests ---

    def record_request(self, endpoint: str, status: str, duration: float) -> None:
        self.requests_total[(endpoint, status)] = (
            self.requests_total.get((endpoint, status), 0) + 1
        )
        self.request_durations.setdefault(endpoint, []).append(duration)

    # --- Cache ---

    def record_cache_hit(self) -> None:
        self.cache_hits_total += 1

    def record_cache_miss(self) -> None:
        self.cache_misses_total += 1

    # --- Data Quality ---

    def record_data_quality(self, quality: str) -> None:
        self.data_quality_counts[quality] = self.data_quality_counts.get(quality, 0) + 1

    # --- Errors ---

    def record_error(self, error_type: str) -> None:
        self.errors_total[error_type] = self.errors_total.get(error_type, 0) + 1

    # --- Snapshot ---

    def snapshot(self) -> Dict[str, Any]:
        durations_summary = {}
        for endpoint, durations in self.request_durations.items():
            if durations:
                durations_summary[endpoint] = {
                    "count": len(durations),
                    "avg": sum(durations) / len(durations),
                    "min": min(durations),
                    "max": max(durations),
                }

        total_cache_requests = self.cache_hits_total + self.cache_misses_total
        hit_ratio = (
            self.cache_hits_total / total_cache_requests
            if total_cache_requests > 0
            else 0.0
        )

        return {
            "requests_total": dict(
                {f"{ep}|{st}": v for (ep, st), v in self.requests_total.items()}
            ),
            "request_durations": durations_summary,
            "cache": {
                "hits": self.cache_hits_total,
                "misses": self.cache_misses_total,
                "hit_ratio": round(hit_ratio, 3),
            },
            "data_quality": self.data_quality_counts,
            "errors_total": self.errors_total,
        }


# =============================================================================
# Data Models
# =============================================================================


class TickerNormalization(BaseModel):
    """Ticker normalization model."""

    original: str
    normalized: str
    confidence: float = Field(ge=0.0, le=1.0)


class ArgaamCompanySnapshot(BaseModel):
    """Company snapshot from Argaam."""

    symbol: str
    name_ar: Optional[str] = None
    name_en: Optional[str] = None
    sector_ar: Optional[str] = None
    sector_en: Optional[str] = None
    industry_ar: Optional[str] = None
    industry_en: Optional[str] = None

    # Market Data
    price: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[int] = None

    # Fundamentals
    eps: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    beta: Optional[float] = None
    high_52_week: Optional[float] = None
    low_52_week: Optional[float] = None

    # Technical
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    open_price: Optional[float] = None
    previous_close: Optional[float] = None

    # Metadata
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data_quality: str = Field(default="unknown", pattern="^(unknown|low|medium|high)$")
    provider: str = "argaam"
    url: Optional[str] = None

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class ArgaamQuoteResponse(BaseModel):
    """Argaam quote response."""

    success: bool
    symbol: str
    data: Optional[ArgaamCompanySnapshot] = None
    error: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)
    processing_time: float
    cache_hit: bool = False
    retry_count: int = 0


class ArgaamBatchRequest(BaseModel):
    """Batch request for multiple tickers."""

    tickers: List[str] = Field(..., min_items=1, max_items=50)
    cache_ttl: Optional[int] = None
    priority: Literal["low", "normal", "high"] = "normal"
    fallback_enabled: bool = True
    html_parsing: bool = True

    @field_validator("tickers")
    @classmethod
    def validate_tickers(cls, v: List[str]) -> List[str]:
        if len(v) > 50:
            raise ValueError("Maximum 50 tickers per request")
        return [t.strip() for t in v if t.strip()]


class ArgaamBatchResponse(BaseModel):
    """Batch response for multiple tickers."""

    request_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processed: int
    successful: int
    failed: int
    results: Dict[str, ArgaamQuoteResponse]
    metadata: Dict[str, Any]

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class ArgaamHealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
    timestamp: datetime
    uptime: float
    cache: Dict[str, Any]
    metrics: Dict[str, Any]
    configuration: Dict[str, Any]


class ArgaamCacheInfo(BaseModel):
    """Cache information."""

    size: int
    max_size: int
    hits: int
    misses: int
    hit_ratio: float
    evictions: int
    ttl: int
    memory_usage_mb: float


# =============================================================================
# Router Definition
# =============================================================================

router = APIRouter(
    prefix="/api/v1/argaam",
    tags=["Argaam Integration"],
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Forbidden"},
        429: {"description": "Too Many Requests"},
        500: {"description": "Internal Server Error"},
    },
)

# =============================================================================
# Internal TTL Cache (no cachetools)
# =============================================================================


class ArgaamTTLCache:
    """Simple TTL cache with stats."""

    def __init__(self, maxsize: int, ttl: int) -> None:
        self.store: Dict[str, Tuple[float, Any]] = {}
        self.maxsize = maxsize
        self.ttl = ttl
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def _now(self) -> float:
        return time.time()

    def get(self, key: str) -> Optional[Any]:
        now = self._now()
        entry = self.store.get(key)
        if not entry:
            self.misses += 1
            return None

        expires_at, value = entry
        if expires_at < now:
            # expired
            self.misses += 1
            self.store.pop(key, None)
            return None

        self.hits += 1
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        if len(self.store) >= self.maxsize and key not in self.store:
            # simple eviction: remove first key
            try:
                first_key = next(iter(self.store.keys()))
                self.store.pop(first_key, None)
                self.evictions += 1
            except StopIteration:
                pass
        expires_at = self._now() + (ttl or self.ttl)
        self.store[key] = (expires_at, value)

    def clear(self) -> None:
        self.store.clear()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def info(self) -> ArgaamCacheInfo:
        # Approximate memory usage
        memory_usage = 0
        for k, v in self.store.items():
            memory_usage += sys.getsizeof(k) + sys.getsizeof(v)

        memory_mb = memory_usage / (1024 * 1024)
        total_requests = self.hits + self.misses
        hit_ratio = self.hits / total_requests if total_requests > 0 else 0.0

        return ArgaamCacheInfo(
            size=len(self.store),
            max_size=self.maxsize,
            hits=self.hits,
            misses=self.misses,
            hit_ratio=round(hit_ratio, 3),
            evictions=self.evictions,
            ttl=self.ttl,
            memory_usage_mb=round(memory_mb, 2),
        )


# =============================================================================
# Core Services
# =============================================================================


class TickerNormalizer:
    """Normalize ticker symbols for Argaam."""

    def __init__(self, settings: ArgaamSettings):
        self.settings = settings
        self.pattern = re.compile(settings.ticker_pattern)

        # Common aliases mapping
        self.aliases: Dict[str, str] = {
            # English aliases
            "aramco": "2222",
            "stc": "7010",
            "alrajhi": "1120",
            "sabic": "2010",
            "riyadbank": "1010",
            "alinma": "1150",
            "alahli": "1180",
            "samba": "1090",
            "anb": "1060",
            "mobily": "7020",
            "petrochem": "2001",
            "maaden": "1211",
            "yanbu": "2290",
            "dallah": "4300",
            # Arabic aliases
            "أرامكو": "2222",
            "الاتصالات": "7010",
            "الراجحي": "1120",
            "سابك": "2010",
            "البنك الأهلي": "1180",
            "سامبا": "1090",
            "معادن": "1211",
            "ينبع": "2290",
            "دلة": "4300",
        }

        # Sector mapping
        self.sector_symbols: Dict[str, List[str]] = {
            "banking": [
                "1010",
                "1020",
                "1030",
                "1040",
                "1050",
                "1060",
                "1070",
                "1080",
                "1090",
                "1110",
                "1120",
                "1130",
                "1140",
                "1150",
                "1160",
                "1180",
                "1190",
            ],
            "petrochemicals": [
                "2001",
                "2002",
                "2010",
                "2020",
                "2030",
                "2050",
                "2060",
                "2080",
                "2090",
                "2100",
                "2110",
                "2120",
                "2140",
                "2150",
                "2160",
                "2170",
                "2180",
            ],
            "telecom": ["7010", "7020", "7030"],
            "cement": [
                "3001",
                "3002",
                "3003",
                "3004",
                "3005",
                "3007",
                "3008",
                "3010",
                "3020",
                "3030",
                "3040",
                "3050",
                "3060",
                "3080",
                "3090",
            ],
            "retail": [
                "4001",
                "4002",
                "4003",
                "4004",
                "4005",
                "4006",
                "4007",
                "4008",
                "4010",
                "4012",
                "4013",
                "4014",
            ],
            "insurance": [
                "8010",
                "8012",
                "8020",
                "8030",
                "8040",
                "8050",
                "8070",
                "8080",
                "8090",
                "8100",
                "8110",
                "8120",
                "8130",
                "8140",
                "8150",
                "8160",
                "8180",
                "8190",
            ],
            "energy": ["2222", "2380", "2381", "4003"],
            "real_estate": [
                "4030",
                "4031",
                "4040",
                "4050",
                "4070",
                "4080",
                "4090",
                "4100",
                "4110",
                "4130",
                "4140",
                "4150",
                "4160",
                "4170",
                "4180",
                "4190",
            ],
        }

    def normalize(self, ticker: str) -> TickerNormalization:
        original = ticker.strip()
        if not original:
            return TickerNormalization(original=original, normalized="", confidence=0.0)

        ticker_upper = original.upper()
        ticker_upper = (
            ticker_upper.replace(".SR", "")
            .replace(".SA", "")
            .replace("TASI:", "")
            .replace("SASE:", "")
        )

        alias_key = ticker_upper.lower()
        if alias_key in self.aliases:
            normalized = self.aliases[alias_key]
            confidence = 0.95
        else:
            match = self.pattern.search(ticker_upper)
            if match:
                normalized = match.group(1).zfill(4)
                confidence = 0.9
            else:
                normalized = ticker_upper
                confidence = 0.5

        if re.match(r"^\d{4}$", normalized):
            confidence = min(confidence + 0.05, 1.0)

        return TickerNormalization(
            original=original, normalized=normalized, confidence=confidence
        )

    def get_sector(self, normalized_ticker: str) -> Optional[str]:
        for sector, symbols in self.sector_symbols.items():
            if normalized_ticker in symbols:
                return sector
        return None


class ArgaamHTMLParser:
    """Parse HTML content from Argaam."""

    def __init__(self, settings: ArgaamSettings):
        self.settings = settings

        self.arabic_to_english = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

        self.field_patterns = {
            "price": [
                r"السعر\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"Price\s*[:=]?\s*([\d,\.]+)",
                r"آخر سعر\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"Last Price\s*[:=]?\s*([\d,\.]+)",
            ],
            "change": [
                r"التغيير\s*[:=]?\s*([\d,٠-٩\.\+\-]+)",
                r"Change\s*[:=]?\s*([\d,\.\+\-]+)",
            ],
            "change_percent": [
                r"التغيير\s*\(%\)\s*[:=]?\s*([\d,٠-٩\.\+\-]+)",
                r"Change\s*\(%\)\s*[:=]?\s*([\d,\.\+\-]+)",
                r"النسبة\s*[:=]?\s*([\d,٠-٩\.\+\-]+)%",
            ],
            "volume": [
                r"الكمية\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"Volume\s*[:=]?\s*([\d,\.]+)",
                r"حجم التداول\s*[:=]?\s*([\d,٠-٩\.]+)",
            ],
            "market_cap": [
                r"القيمة السوقية\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"Market Cap\s*[:=]?\s*([\d,\.]+)",
                r"رأس المال السوقي\s*[:=]?\s*([\d,٠-٩\.]+)",
            ],
            "eps": [
                r"ربحية السهم\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"EPS\s*[:=]?\s*([\d,\.]+)",
                r"ربحية السهم الواحد\s*[:=]?\s*([\d,٠-٩\.]+)",
            ],
            "pe_ratio": [
                r"مكرر الربحية\s*[:=]?\s*([\d,٠-٩\.]+)",
                r"P/E\s*[:=]?\s*([\d,\.]+)",
                r"مكرر الأرباح\s*[:=]?\s*([\d,٠-٩\.]+)",
            ],
            "dividend_yield": [
                r"العائد\s*[:=]?\s*([\d,٠-٩\.]+)%",
                r"Dividend Yield\s*[:=]?\s*([\d,\.]+)%",
                r"عائد توزيعات\s*[:=]?\s*([\d,٠-٩\.]+)%",
            ],
        }

    def parse_number(self, text: str) -> Optional[float]:
        if not text:
            return None

        text = text.translate(self.arabic_to_english)
        text = text.replace(",", "").replace(" ", "")

        multipliers = {
            "k": 1e3,
            "m": 1e6,
            "b": 1e9,
            "t": 1e12,
            "%": 0.01,
        }

        match = re.match(r"([\+\-]?\d+(?:\.\d+)?)([kmbt%]?)", text, re.IGNORECASE)
        if match:
            value = float(match.group(1))
            suffix = match.group(2).lower()
            if suffix in multipliers:
                value *= multipliers[suffix]
            return value

        return None

    def extract_with_patterns(self, text: str, patterns: List[str]) -> Optional[float]:
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = self.parse_number(match.group(1))
                if value is not None:
                    return value
        return None

    def parse_html(self, html: str, symbol: str) -> ArgaamCompanySnapshot:
        snapshot = ArgaamCompanySnapshot(symbol=symbol)

        if not html:
            snapshot.data_quality = "low"
            return snapshot

        soup = None
        for parser_name in self.settings.html_parsers:
            try:
                soup = BeautifulSoup(html, parser_name)
                break
            except Exception as e:
                logger.warning("HTML parser failed", extra={"parser": parser_name, "error": str(e)})
                continue

        if not soup:
            snapshot.data_quality = "low"
            return snapshot

        all_text = soup.get_text(" ", strip=True)

        name_selectors = [
            "h1",
            ".company-name",
            ".stock-name",
            ".symbol-name",
            '[class*="company"]',
            '[class*="symbol"]',
            '[class*="stock"]',
            "title",
        ]

        for selector in name_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text and len(text) > 2:
                    if re.search(r"[\u0600-\u06FF]", text):
                        snapshot.name_ar = text
                    else:
                        snapshot.name_en = text
                    break

        fields_found = 0

        for field, patterns in self.field_patterns.items():
            value = self.extract_with_patterns(all_text, patterns)
            if value is not None:
                setattr(snapshot, field, value)
                fields_found += 1

        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    value_text = cells[1].get_text(strip=True)

                    if any(keyword in label.lower() for keyword in ["price", "سعر"]):
                        value = self.parse_number(value_text)
                        if value and snapshot.price is None:
                            snapshot.price = value
                            fields_found += 1

                    elif any(
                        keyword in label.lower()
                        for keyword in ["volume", "كمية", "تداول"]
                    ):
                        value = self.parse_number(value_text)
                        if value and snapshot.volume is None:
                            snapshot.volume = int(value)
                            fields_found += 1

        if fields_found >= 8:
            snapshot.data_quality = "high"
        elif fields_found >= 5:
            snapshot.data_quality = "medium"
        elif fields_found >= 2:
            snapshot.data_quality = "low"
        else:
            snapshot.data_quality = "unknown"

        logger.info(
            "Argaam HTML parsed",
            extra={
                "symbol": symbol,
                "fields_found": fields_found,
                "quality": snapshot.data_quality,
            },
        )

        return snapshot


class ArgaamHTTPClient:
    """HTTP client for Argaam with simple rate limiting and retry."""

    def __init__(self, settings: ArgaamSettings, metrics: ArgaamMetrics):
        self.settings = settings
        self.metrics = metrics

        self.semaphore = asyncio.Semaphore(settings.concurrent_requests)
        self.last_request_time = 0.0

        self.timeout = Timeout(settings.timeout)
        self.limits = Limits(max_connections=100, max_keepalive_connections=50)

        self.headers = {
            "User-Agent": settings.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
        }

        self.client: Optional[AsyncClient] = None

    async def __aenter__(self) -> "ArgaamHTTPClient":
        self.client = AsyncClient(
            timeout=self.timeout,
            limits=self.limits,
            headers=self.headers,
            follow_redirects=True,
            http2=True,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.client:
            await self.client.aclose()

    async def fetch_html(self, url: str) -> Optional[str]:
        """Fetch HTML with manual retry and rate limiting."""
        if not self.client:
            self.client = AsyncClient(
                timeout=self.timeout,
                limits=self.limits,
                headers=self.headers,
                follow_redirects=True,
            )

        attempts = max(1, self.settings.max_retries)
        last_error: Optional[Exception] = None

        for attempt in range(attempts):
            async with self.semaphore:
                now = time.time()
                elapsed = now - self.last_request_time
                if elapsed < self.settings.rate_limit_delay:
                    await asyncio.sleep(self.settings.rate_limit_delay - elapsed)

                start = time.time()
                try:
                    response = await self.client.get(url)
                    duration = time.time() - start
                    self.last_request_time = time.time()

                    if response.status_code == 200:
                        content = response.text
                        if content and len(content) > 1000:
                            self.metrics.record_request("fetch_html", "success", duration)
                            logger.debug(
                                "Argaam HTML fetched",
                                extra={
                                    "url": url,
                                    "size": len(content),
                                    "duration": duration,
                                },
                            )
                            return content
                        else:
                            logger.warning(
                                "Argaam HTML insufficient content",
                                extra={"url": url, "size": len(content)},
                            )
                            self.metrics.record_request(
                                "fetch_html", "insufficient", duration
                            )
                    else:
                        logger.warning(
                            "Argaam HTTP error",
                            extra={"url": url, "status": response.status_code},
                        )
                        self.metrics.record_request(
                            "fetch_html", f"error_{response.status_code}", duration
                        )

                    last_error = HTTPException(
                        status_code=response.status_code,
                        detail=f"HTTP error {response.status_code}",
                    )
                except Exception as e:
                    duration = time.time() - start
                    last_error = e
                    logger.error(
                        "Argaam fetch error",
                        extra={"url": url, "error": str(e)},
                    )
                    self.metrics.record_request("fetch_html", "exception", duration)
                    self.metrics.record_error(type(e).__name__)

            # backoff between attempts
            await asyncio.sleep(0.5 * (attempt + 1))

        if last_error:
            raise last_error

        return None


class ArgaamURLDiscoverer:
    """Discover Argaam URLs for symbols."""

    def __init__(self, settings: ArgaamSettings):
        self.settings = settings

        self.patterns = [
            # Arabic patterns
            "{base}/ar/company/{symbol}",
            "{base}/ar/company/financials/{symbol}",
            "{base}/ar/tadawul/company?symbol={symbol}",
            "{base}/ar/tadawul/{symbol}",
            "{base}/ar/{symbol}",
            # English patterns
            "{base}/en/company/{symbol}",
            "{base}/en/company/financials/{symbol}",
            "{base}/en/tadawul/company?symbol={symbol}",
            "{base}/en/tadawul/{symbol}",
            "{base}/en/{symbol}",
            # Common
            "{base}/company/{symbol}",
            "{base}/tadawul/{symbol}",
            "{base}/stock/{symbol}",
            "{base}/symbol/{symbol}",
        ]

        self.successful_patterns: Dict[str, str] = {}

    async def discover_url(
        self, http_client: ArgaamHTTPClient, symbol: str
    ) -> Optional[str]:
        if symbol in self.successful_patterns:
            pattern = self.successful_patterns[symbol]
            url = pattern.format(base=self.settings.base_url, symbol=quote(symbol))
            logger.debug(
                "Argaam URL from cache", extra={"symbol": symbol, "pattern": pattern}
            )
            return url

        for pattern in self.patterns:
            url = pattern.format(base=self.settings.base_url, symbol=quote(symbol))
            try:
                content = await http_client.fetch_html(url)
                if content and len(content) > 2000:
                    self.successful_patterns[symbol] = pattern
                    logger.info(
                        "Argaam URL discovered",
                        extra={"symbol": symbol, "url": url},
                    )
                    return url
            except Exception as e:
                logger.debug(
                    "Argaam URL pattern failed",
                    extra={"pattern": pattern, "error": str(e)},
                )
                continue

        logger.warning("No Argaam URL discovered", extra={"symbol": symbol})
        return None


class ArgaamCacheManager:
    """Cache manager for Argaam data."""

    def __init__(self, settings: ArgaamSettings, metrics: ArgaamMetrics):
        self.settings = settings
        self.metrics = metrics
        self.cache = ArgaamTTLCache(
            maxsize=settings.cache_maxsize, ttl=settings.cache_ttl
        )

    def _key(self, symbol: str) -> str:
        return f"argaam:{symbol}"

    async def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.settings.cache_enabled:
            return None

        key = self._key(symbol)
        data = self.cache.get(key)
        if data is not None:
            self.metrics.record_cache_hit()
            logger.debug("Argaam cache hit", extra={"symbol": symbol})
            return data
        else:
            self.metrics.record_cache_miss()
            logger.debug("Argaam cache miss", extra={"symbol": symbol})
            return None

    async def set(self, symbol: str, data: Dict[str, Any], ttl: Optional[int] = None):
        if not self.settings.cache_enabled:
            return
        key = self._key(symbol)
        cache_data = {
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ttl": ttl or self.settings.cache_ttl,
        }
        self.cache.set(key, cache_data, ttl=ttl)

    async def clear(self) -> None:
        self.cache.clear()
        logger.info("Argaam cache cleared")

    async def get_info(self) -> ArgaamCacheInfo:
        return self.cache.info()


# =============================================================================
# Main Service
# =============================================================================


class ArgaamService:
    """Main Argaam service."""

    def __init__(self, settings: ArgaamSettings):
        self.settings = settings
        self.metrics = ArgaamMetrics()

        self.ticker_normalizer = TickerNormalizer(settings)
        self.html_parser = ArgaamHTMLParser(settings)
        self.cache_manager = ArgaamCacheManager(settings, self.metrics)
        self.url_discoverer = ArgaamURLDiscoverer(settings)

        self.start_time = time.time()
        self.total_requests = 0
        self.total_symbols = 0
        self.successful_fetches = 0
        self.failed_fetches = 0

    async def initialize(self) -> None:
        logger.info(
            "ArgaamService initialized",
            extra={
                "base_url": self.settings.base_url,
                "cache_enabled": self.settings.cache_enabled,
                "cache_ttl": self.settings.cache_ttl,
            },
        )

    async def get_company_data(
        self, symbol: str, use_cache: bool = True
    ) -> ArgaamQuoteResponse:
        start = time.time()
        retry_count = 0

        normalization = self.ticker_normalizer.normalize(symbol)
        normalized_symbol = normalization.normalized

        if not normalized_symbol:
            processing_time = time.time() - start
            return ArgaamQuoteResponse(
                success=False,
                symbol=symbol,
                error=f"Invalid ticker symbol: {symbol}",
                processing_time=processing_time,
                retry_count=retry_count,
            )

        cache_hit = False
        if use_cache and self.settings.cache_enabled:
            cached = await self.cache_manager.get(normalized_symbol)
            if cached:
                processing_time = time.time() - start
                self.total_requests += 1
                self.total_symbols += 1

                snapshot = ArgaamCompanySnapshot(**cached["data"])

                logger.info(
                    "Argaam cache hit",
                    extra={
                        "symbol": symbol,
                        "normalized": normalized_symbol,
                        "processing_time": processing_time,
                    },
                )

                return ArgaamQuoteResponse(
                    success=True,
                    symbol=normalized_symbol,
                    data=snapshot,
                    processing_time=processing_time,
                    cache_hit=True,
                    retry_count=retry_count,
                )

        try:
            async with ArgaamHTTPClient(self.settings, self.metrics) as http_client:
                url = await self.url_discoverer.discover_url(
                    http_client, normalized_symbol
                )
                if not url:
                    processing_time = time.time() - start
                    self.failed_fetches += 1
                    self.total_requests += 1
                    self.total_symbols += 1
                    return ArgaamQuoteResponse(
                        success=False,
                        symbol=normalized_symbol,
                        error="Company URL not found on Argaam",
                        processing_time=processing_time,
                        retry_count=retry_count,
                    )

                html = await http_client.fetch_html(url)
                if not html:
                    processing_time = time.time() - start
                    self.failed_fetches += 1
                    self.total_requests += 1
                    self.total_symbols += 1
                    return ArgaamQuoteResponse(
                        success=False,
                        symbol=normalized_symbol,
                        error="Failed to fetch data from Argaam",
                        processing_time=processing_time,
                        retry_count=retry_count,
                    )

                snapshot = self.html_parser.parse_html(html, normalized_symbol)
                snapshot.url = url

                self.total_requests += 1
                self.total_symbols += 1
                self.successful_fetches += 1

                if self.settings.cache_enabled:
                    await self.cache_manager.set(
                        normalized_symbol,
                        snapshot.model_dump(),
                        ttl=self.settings.cache_ttl,
                    )

                processing_time = time.time() - start
                self.metrics.record_data_quality(snapshot.data_quality)

                logger.info(
                    "Argaam data fetched",
                    extra={
                        "symbol": symbol,
                        "normalized": normalized_symbol,
                        "quality": snapshot.data_quality,
                        "processing_time": processing_time,
                        "cache_hit": cache_hit,
                    },
                )

                return ArgaamQuoteResponse(
                    success=True,
                    symbol=normalized_symbol,
                    data=snapshot,
                    processing_time=processing_time,
                    cache_hit=False,
                    retry_count=retry_count,
                )

        except Exception as e:
            processing_time = time.time() - start
            self.failed_fetches += 1
            self.total_requests += 1
            self.total_symbols += 1
            self.metrics.record_error("service_exception")

            logger.error(
                "Argaam service error",
                extra={
                    "symbol": symbol,
                    "normalized": normalized_symbol,
                    "error": str(e),
                    "processing_time": processing_time,
                },
            )

            return ArgaamQuoteResponse(
                success=False,
                symbol=normalized_symbol,
                error=f"Service error: {str(e)}",
                processing_time=processing_time,
                retry_count=retry_count,
            )

    async def get_batch_data(
        self, tickers: List[str], cache_ttl: Optional[int] = None
    ) -> ArgaamBatchResponse:
        start_time = time.time()
        request_id = f"argaam_batch_{int(start_time)}_{abs(hash(str(tickers)))}"

        original_ttl = self.settings.cache_ttl
        if cache_ttl:
            self.settings.cache_ttl = cache_ttl

        try:
            semaphore = asyncio.Semaphore(5)

            async def process_ticker(ticker: str) -> Tuple[str, ArgaamQuoteResponse]:
                async with semaphore:
                    result = await self.get_company_data(ticker)
                    return ticker, result

            tasks = [process_ticker(t) for t in tickers]
            results_raw = await asyncio.gather(*tasks, return_exceptions=True)

            processed: Dict[str, ArgaamQuoteResponse] = {}
            successful = 0
            failed = 0

            for i, result in enumerate(results_raw):
                ticker = tickers[i]
                if isinstance(result, Exception):
                    processed[ticker] = ArgaamQuoteResponse(
                        success=False,
                        symbol=ticker,
                        error=f"Processing error: {str(result)}",
                        processing_time=0.0,
                        retry_count=0,
                    )
                    failed += 1
                else:
                    t_key, quote_resp = result
                    processed[t_key] = quote_resp
                    if quote_resp.success:
                        successful += 1
                    else:
                        failed += 1

            processing_time = time.time() - start_time
            cache_info = await self.cache_manager.get_info()

            if cache_ttl:
                self.settings.cache_ttl = original_ttl

            logger.info(
                "Argaam batch completed",
                extra={
                    "request_id": request_id,
                    "total": len(tickers),
                    "successful": successful,
                    "failed": failed,
                    "processing_time": processing_time,
                },
            )

            return ArgaamBatchResponse(
                request_id=request_id,
                processed=len(tickers),
                successful=successful,
                failed=failed,
                results=processed,
                metadata={
                    "cache": cache_info.model_dump(),
                    "processing_time": processing_time,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "metrics": self.metrics.snapshot(),
                },
            )

        except Exception as e:
            processing_time = time.time() - start_time
            if cache_ttl:
                self.settings.cache_ttl = original_ttl

            logger.error(
                "Argaam batch failed",
                extra={
                    "request_id": request_id,
                    "error": str(e),
                    "processing_time": processing_time,
                },
            )

            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Batch processing failed: {str(e)}",
            )

    async def get_health(self) -> ArgaamHealthResponse:
        uptime = time.time() - self.start_time
        cache_info = await self.cache_manager.get_info()

        metrics_data = {
            "total_requests": self.total_requests,
            "total_symbols": self.total_symbols,
            "successful_fetches": self.successful_fetches,
            "failed_fetches": self.failed_fetches,
            "uptime_hours": round(uptime / 3600, 2),
            "metrics_snapshot": self.metrics.snapshot(),
        }

        config_data = self.settings.model_dump()

        if self.failed_fetches > 0 and self.successful_fetches == 0:
            status = "unhealthy"
        elif self.failed_fetches > self.successful_fetches:
            status = "degraded"
        else:
            status = "healthy"

        return ArgaamHealthResponse(
            status=status,
            version="4.1.0",
            timestamp=datetime.now(timezone.utc),
            uptime=uptime,
            cache=cache_info.model_dump(),
            metrics=metrics_data,
            configuration=config_data,
        )


# =============================================================================
# Application State Management
# =============================================================================


class ArgaamAppState:
    """Application state manager for Argaam."""

    def __init__(self):
        self.settings = ArgaamSettings()
        self.service: Optional[ArgaamService] = None
        self.initialized = False

    async def initialize(self) -> None:
        if self.initialized:
            return
        self.service = ArgaamService(self.settings)
        await self.service.initialize()
        self.initialized = True

    async def shutdown(self) -> None:
        self.initialized = False
        self.service = None


argaam_app_state = ArgaamAppState()


async def get_argaam_service() -> ArgaamService:
    if not argaam_app_state.initialized:
        await argaam_app_state.initialize()
    if not argaam_app_state.service:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Argaam service not initialized",
        )
    return argaam_app_state.service


# =============================================================================
# Route Handlers
# =============================================================================


@router.on_event("startup")
async def startup_event():
    await argaam_app_state.initialize()
    logger.info(
        "Argaam routes initialized",
        extra={
            "version": "4.1.0",
            "base_url": argaam_app_state.settings.base_url,
            "cache_enabled": argaam_app_state.settings.cache_enabled,
        },
    )


@router.on_event("shutdown")
async def shutdown_event():
    await argaam_app_state.shutdown()
    logger.info("Argaam routes shutdown complete")


@router.get("/health", response_model=ArgaamHealthResponse, summary="Health Check")
async def health_check(service: ArgaamService = Depends(get_argaam_service)):
    return await service.get_health()


@router.post("/quotes", response_model=ArgaamBatchResponse, summary="Batch Quotes")
async def get_quotes(
    request: ArgaamBatchRequest, service: ArgaamService = Depends(get_argaam_service)
):
    return await service.get_batch_data(tickers=request.tickers, cache_ttl=request.cache_ttl)


@router.get("/quotes/{symbol}", response_model=ArgaamQuoteResponse, summary="Single Quote")
async def get_quote(
    symbol: str,
    use_cache: bool = True,
    service: ArgaamService = Depends(get_argaam_service),
):
    return await service.get_company_data(symbol, use_cache)


@router.get("/cache/info", response_model=ArgaamCacheInfo, summary="Cache Information")
async def get_cache_info(service: ArgaamService = Depends(get_argaam_service)):
    return await service.cache_manager.get_info()


@router.delete("/cache/clear", summary="Clear Cache")
async def clear_cache(service: ArgaamService = Depends(get_argaam_service)):
    await service.cache_manager.clear()
    return {
        "status": "success",
        "message": "Cache cleared successfully",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/normalize/{ticker}", summary="Normalize Ticker")
async def normalize_ticker(
    ticker: str, service: ArgaamService = Depends(get_argaam_service)
):
    normalization = service.ticker_normalizer.normalize(ticker)
    sector = service.ticker_normalizer.get_sector(normalization.normalized)
    return {
        "original": normalization.original,
        "normalized": normalization.normalized,
        "confidence": normalization.confidence,
        "sector": sector,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/test/{ticker}", summary="Test Endpoint")
async def test_ticker(
    ticker: str, service: ArgaamService = Depends(get_argaam_service)
):
    result = await service.get_company_data(ticker, use_cache=False)
    return {
        "test": True,
        "ticker": ticker,
        "result": result.model_dump(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "configuration": {
            "base_url": service.settings.base_url,
            "cache_enabled": service.settings.cache_enabled,
            "cache_ttl": service.settings.cache_ttl,
            "user_agent": service.settings.user_agent[:80] + "...",
        },
    }


@router.get("/stats", summary="Statistics")
async def get_stats(service: ArgaamService = Depends(get_argaam_service)):
    uptime = time.time() - service.start_time
    cache_info = await service.cache_manager.get_info()
    total_symbols = max(service.total_symbols, 1)
    success_rate = service.successful_fetches / total_symbols

    return {
        "uptime_seconds": round(uptime, 2),
        "uptime_hours": round(uptime / 3600, 2),
        "total_requests": service.total_requests,
        "total_symbols": service.total_symbols,
        "successful_fetches": service.successful_fetches,
        "failed_fetches": service.failed_fetches,
        "success_rate": round(success_rate, 3),
        "cache_info": cache_info.model_dump(),
        "metrics": service.metrics.snapshot(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/", summary="Root Endpoint")
async def root():
    return {
        "name": "Argaam Integration API",
        "version": "4.1.0",
        "description": "Professional Arabic/English stock data scraping from Argaam",
        "status": "operational",
        "endpoints": {
            "health": "/api/v1/argaam/health",
            "quotes": "/api/v1/argaam/quotes",
            "single_quote": "/api/v1/argaam/quotes/{symbol}",
            "cache_info": "/api/v1/argaam/cache/info",
            "normalize": "/api/v1/argaam/normalize/{ticker}",
            "stats": "/api/v1/argaam/stats",
            "test": "/api/v1/argaam/test/{ticker}",
        },
        "features": {
            "ticker_normalization": True,
            "arabic_english_support": True,
            "html_parsing": True,
            "caching": True,
            "rate_limiting": True,
            "batch_processing": True,
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
        },
    }


# =============================================================================
# Error Handlers (router-level)
# =============================================================================


@router.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.warning(
        "Argaam HTTP exception",
        extra={
            "status_code": exc.status_code,
            "detail": exc.detail,
            "path": request.url.path,
            "client_ip": request.client.host if request.client else None,
        },
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "code": exc.status_code,
            "message": exc.detail,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "path": request.url.path,
        },
    )


@router.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(
        "Argaam unhandled exception",
        extra={
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "path": request.url.path,
            "client_ip": request.client.host if request.client else None,
        },
    )
    if argaam_app_state.service:
        argaam_app_state.service.metrics.record_error("unhandled_exception")

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": True,
            "code": 500,
            "message": "Internal server error",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": request.headers.get("X-Request-ID", "unknown"),
        },
    )


# =============================================================================
# Export Router
# =============================================================================


def get_argaam_router() -> APIRouter:
    """Get the Argaam router with initialized state."""
    return router


# =============================================================================
# Direct Execution (for local testing)
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI

    test_app = FastAPI(title="Argaam Test API", version="4.1.0")
    test_app.include_router(router)
    uvicorn.run(test_app, host="127.0.0.1", port=8001)
