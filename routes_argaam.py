# =============================================================================
# Argaam Integration Routes - Ultimate Investment Dashboard (v3.2.0)
# Enhanced with Resilience, Monitoring & Advanced Error Handling
# Aligned with main.py / symbols_reader.py caching & config patterns
# =============================================================================
from __future__ import annotations

import asyncio
import os
import re
import time
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote
from datetime import datetime

import httpx
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel, Field
from pydantic.functional_validators import field_validator  # pydantic v2 style
import cachetools
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# -------------------------------------------------------------------------
# Optional Parser (BeautifulSoup) with Enhanced Fallback Strategy
# -------------------------------------------------------------------------
HAS_BS = False
BeautifulSoup = None
HTML_PARSERS = ["lxml", "html.parser", "html5lib"]

try:
    from bs4 import BeautifulSoup  # type: ignore
    HAS_BS = True
    logger.info("✅ BeautifulSoup available - enhanced parsing enabled for Argaam")
except ImportError as e:
    logger.warning("⚠️ BeautifulSoup not available for Argaam routes: %s. Basic parsing only.", e)

# -------------------------------------------------------------------------
# Router
# -------------------------------------------------------------------------
router = APIRouter(prefix="/v1/argaam", tags=["Argaam"])

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------
class ArgaamConfig:
    def __init__(self) -> None:
        # User agent
        self.USER_AGENT = os.getenv(
            "ARGAAM_USER_AGENT",
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
        ).strip()

        # Tokens & security flags (aligned with main.py style)
        self.APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
        self.BACKUP_APP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "").strip()
        # If REQUIRE_AUTH not set, default true for production safety
        self.REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "true").lower() == "true"

        # Base Argaam URL
        self.BASE_URL = os.getenv("ARGAAM_BASE_URL", "https://www.argaam.com")

        # Timeouts & retry / cache config
        self.TIMEOUT = int(os.getenv("ARGAAM_TIMEOUT", "30"))
        self.MAX_RETRIES = int(os.getenv("ARGAAM_MAX_RETRIES", "3"))

        # Cache TTL resolution:
        # 1) ARGAAM_CACHE_TTL / ARGAAM_CACHE_TTL_SECONDS
        # 2) CACHE_DEFAULT_TTL / CACHE_DEFAULT_TTL_SECONDS
        # 3) default 600s
        ttl_env = (
            os.getenv("ARGAAM_CACHE_TTL")
            or os.getenv("ARGAAM_CACHE_TTL_SECONDS")
            or os.getenv("CACHE_DEFAULT_TTL")
            or os.getenv("CACHE_DEFAULT_TTL_SECONDS")
        )
        try:
            ttl_val = int(ttl_env) if ttl_env is not None else 600
        except (TypeError, ValueError):
            ttl_val = 600
        self.CACHE_TTL = max(60, ttl_val)

        self.CACHE_MAXSIZE = int(os.getenv("ARGAAM_CACHE_MAXSIZE", "1000"))
        self.RATE_LIMIT_DELAY = float(os.getenv("ARGAAM_RATE_LIMIT_DELAY", "0.15"))

        self.HEADERS = {
            "User-Agent": self.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        }

        self.validate_config()

    def validate_config(self) -> None:
        """Validate critical configuration parameters."""
        if self.REQUIRE_AUTH and not (self.APP_TOKEN or self.BACKUP_APP_TOKEN):
            logger.warning("⚠️ Argaam: Authentication required but no tokens configured (APP_TOKEN/BACKUP_APP_TOKEN)")

        if self.TIMEOUT < 5:
            logger.warning("⚠️ Argaam: Timeout too low (%ss), increasing to minimum 5 seconds", self.TIMEOUT)
            self.TIMEOUT = 5

        if self.MAX_RETRIES > 5:
            logger.warning("⚠️ Argaam: Max retries too high (%s), capping at 5", self.MAX_RETRIES)
            self.MAX_RETRIES = 5

        logger.info(
            "✅ ArgaamConfig loaded: timeout=%ss, retries=%s, cache_ttl=%ss, require_auth=%s",
            self.TIMEOUT,
            self.MAX_RETRIES,
            self.CACHE_TTL,
            self.REQUIRE_AUTH,
        )


config = ArgaamConfig()

# -------------------------------------------------------------------------
# Enhanced TTL Cache with LRU Eviction
# -------------------------------------------------------------------------
class TTLCache:
    def __init__(self, maxsize: int = 1000, ttl: int = 600) -> None:
        self._cache = cachetools.TTLCache(maxsize=maxsize, ttl=ttl)
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def get(self, key: str) -> Optional[Any]:
        try:
            value = self._cache[key]
            self._hits += 1
            return value
        except KeyError:
            self._misses += 1
            return None

    def set(self, key: str, value: Any) -> None:
        try:
            old_size = len(self._cache)
            self._cache[key] = value
            # cachetools.TTLCache evicts on insert when full
            if len(self._cache) < old_size:
                self._evictions += 1
        except Exception as e:
            logger.warning("⚠️ Argaam cache set failed for key %s: %s", key, e)

    def clear(self) -> None:
        self._cache.clear()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def info(self) -> Dict[str, Any]:
        total_requests = self._hits + self._misses
        hit_ratio = self._hits / total_requests if total_requests > 0 else 0
        return {
            "size": len(self._cache),
            "max_size": self._cache.maxsize,
            "hits": self._hits,
            "misses": self._misses,
            "hit_ratio": round(hit_ratio, 3),
            "evictions": self._evictions,
            "ttl": self._cache.ttl,
        }


_CACHE = TTLCache(maxsize=config.CACHE_MAXSIZE, ttl=config.CACHE_TTL)

# -------------------------------------------------------------------------
# Async HTTP Client with Connection Pooling
# -------------------------------------------------------------------------
class ArgaamHTTPClient:
    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None
        self._last_used: Optional[float] = None
        self._request_count: int = 0

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._should_recreate_client():
            await self._close_client()
            self._client = httpx.AsyncClient(
                headers=config.HEADERS,
                timeout=config.TIMEOUT,
                follow_redirects=True,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
                http2=True,
            )
            self._last_used = time.time()
            self._request_count = 0
            logger.info("✅ Created new HTTP client for Argaam")

        self._last_used = time.time()
        return self._client

    def _should_recreate_client(self) -> bool:
        if self._last_used is None:
            return True
        # Recreate client every 30 minutes or after 1000 requests
        time_elapsed = time.time() - self._last_used
        return time_elapsed > 1800 or self._request_count > 1000

    async def _close_client(self) -> None:
        if self._client:
            try:
                await self._client.aclose()
                logger.info("✅ Closed Argaam HTTP client")
            except Exception as e:
                logger.warning("⚠️ Error closing Argaam HTTP client: %s", e)
            finally:
                self._client = None

    def increment_request_count(self) -> None:
        self._request_count += 1


http_client = ArgaamHTTPClient()

# -------------------------------------------------------------------------
# Authentication with basic rate limiting (per IP)
# -------------------------------------------------------------------------
@dataclass
class FailedAttempt:
    count: int
    first_ts: float


class AuthManager:
    def __init__(self) -> None:
        self._failed_attempts: Dict[str, FailedAttempt] = {}
        self._max_failed_attempts = 5
        self._lockout_duration = 300  # 5 minutes

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request (Render / proxy aware)."""
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _is_ip_locked(self, ip: str) -> bool:
        """Check if IP is temporarily locked due to failed attempts."""
        attempt = self._failed_attempts.get(ip)
        if not attempt:
            return False

        if attempt.count >= self._max_failed_attempts:
            lockout_time = attempt.first_ts + self._lockout_duration
            if time.time() < lockout_time:
                return True
            # lockout expired → reset
            del self._failed_attempts[ip]
        return False

    def _record_failed_attempt(self, ip: str) -> None:
        """Record failed authentication attempt."""
        now = time.time()
        attempt = self._failed_attempts.get(ip)
        if not attempt:
            self._failed_attempts[ip] = FailedAttempt(count=1, first_ts=now)
        else:
            # Reset window if last failure long ago (> 1 hour)
            if now - attempt.first_ts > 3600:
                self._failed_attempts[ip] = FailedAttempt(count=1, first_ts=now)
            else:
                attempt.count += 1
                self._failed_attempts[ip] = attempt

    def validate_auth(self, request: Request) -> None:
        """Authentication validation using APP_TOKEN/BACKUP_APP_TOKEN."""
        if not config.REQUIRE_AUTH:
            return

        client_ip = self._get_client_ip(request)

        # Check IP lockout
        if self._is_ip_locked(client_ip):
            logger.warning("🚫 IP %s temporarily locked due to failed attempts", client_ip)
            raise HTTPException(
                status_code=429,
                detail="Too many failed authentication attempts. Please try again later.",
            )

        # Extract and validate token
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            self._record_failed_attempt(client_ip)
            raise HTTPException(status_code=401, detail="Missing Bearer token")

        token = auth_header.split(" ", 1)[1].strip()
        allowed_tokens = {t for t in (config.APP_TOKEN, config.BACKUP_APP_TOKEN) if t}

        if token not in allowed_tokens:
            self._record_failed_attempt(client_ip)
            logger.warning("🚫 Invalid token attempt from IP %s", client_ip)
            raise HTTPException(status_code=403, detail="Invalid or expired token")

        # Reset failed attempts on successful auth
        if client_ip in self._failed_attempts:
            del self._failed_attempts[client_ip]

        logger.debug("✅ Argaam auth successful for IP %s", client_ip)


auth_manager = AuthManager()

# -------------------------------------------------------------------------
# Ticker Normalization
# -------------------------------------------------------------------------
class TickerNormalizer:
    def __init__(self) -> None:
        self._code_re = re.compile(r"(\d{1,4})")
        self._common_aliases = {
            "stc": "7010",
            "aramco": "2222",
            "alrajhi": "1120",
            "sabic": "2010",
            "riyadbank": "1010",
            "alinnma": "1150",
            "alahli": "1180",
            "samba": "1090",
            "anb": "1060",
            "mobily": "7020",
            "petrochem": "2001",
            "maaden": "1211",
        }

    def normalize(self, code: str) -> str:
        """Enhanced ticker normalization with alias support."""
        if not code:
            return ""

        # Convert to uppercase and remove common suffixes
        code = code.upper().replace(".SR", "").replace("TASI:", "").strip()

        # Check for common aliases
        if code.lower() in self._common_aliases:
            normalized = self._common_aliases[code.lower()]
            logger.debug("🔤 Normalized alias %s -> %s", code, normalized)
            return normalized

        # Extract numeric part
        match = self._code_re.search(code)
        if match:
            normalized = match.group(1).zfill(4)  # Pad with leading zeros
            logger.debug("🔤 Normalized %s -> %s", code, normalized)
            return normalized

        logger.warning("⚠️ Could not normalize ticker: %s", code)
        return code


ticker_normalizer = TickerNormalizer()

# -------------------------------------------------------------------------
# HTML Fetcher with Retry Logic
# -------------------------------------------------------------------------
class HTMLFetcher:
    def __init__(self) -> None:
        self._session_semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

    @retry(
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPError)),
    )
    async def fetch(self, url: str) -> Optional[str]:
        """Enhanced HTML fetching with retry logic and rate limiting."""
        async with self._session_semaphore:
            client = await http_client.get_client()
            http_client.increment_request_count()

            try:
                logger.info("🌐 Argaam: Fetching URL: %s", url)
                start_time = time.time()
                response = await client.get(url)
                response_time = time.time() - start_time

                if response.status_code == 200:
                    content = response.text
                    content_length = len(content)

                    if content_length > 1000:  # Basic content validation
                        logger.info(
                            "✅ Argaam: Fetched %d bytes from %s in %.2fs",
                            content_length,
                            url,
                            response_time,
                        )
                        return content
                    else:
                        logger.warning(
                            "⚠️ Argaam: Content too short from %s: %d bytes",
                            url,
                            content_length,
                        )
                        return None
                elif response.status_code == 404:
                    logger.info("❌ Argaam: URL not found: %s", url)
                    return None
                elif response.status_code == 403:
                    logger.warning("🚫 Argaam: Access forbidden: %s", url)
                    raise httpx.HTTPStatusError(
                        "403 Forbidden", request=response.request, response=response
                    )
                elif response.status_code == 429:
                    logger.warning("🚫 Argaam: Rate limited: %s", url)
                    await asyncio.sleep(5)  # Longer delay for rate limits
                    raise httpx.HTTPStatusError(
                        "429 Rate Limited", request=response.request, response=response
                    )
                else:
                    logger.warning("⚠️ Argaam: HTTP %s from %s", response.status_code, url)
                    response.raise_for_status()

            except httpx.TimeoutException:
                logger.error("⏰ Argaam: Timeout fetching %s", url)
                raise
            except httpx.HTTPError as e:
                logger.error("🌐 Argaam: HTTP error fetching %s: %s", url, e)
                raise
            except Exception as e:
                logger.error("💥 Argaam: Unexpected error fetching %s: %s", url, e)
                return None
            finally:
                # Soft rate limiting between requests
                await asyncio.sleep(config.RATE_LIMIT_DELAY)

        return None


html_fetcher = HTMLFetcher()

# -------------------------------------------------------------------------
# Data Parser
# -------------------------------------------------------------------------
class DataParser:
    def __init__(self) -> None:
        self._number_pattern = re.compile(r"-?\d+[,٠-٩]*(?:\.\d+)?\s*[%kKmMbBtT]?")
        self._arabic_to_english = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

        self._field_mappings: Dict[str, List[str]] = {
            "marketCap": ["Market Cap", "القيمة السوقية", "رأس المال السوقي"],
            "sharesOutstanding": ["Shares Outstanding", "الأسهم القائمة", "عدد الأسهم"],
            "eps": ["EPS", "ربحية السهم", "ربحية السهم الواحد"],
            "pe": ["P/E", "مكرر الربحية", "مكرر الأرباح"],
            "beta": ["Beta", "بيتا", "معامل بيتا"],
            "dividendYield": ["Dividend Yield", "عائد توزيعات", "العائد"],
            "volume": ["Volume", "الكمية", "حجم التداول"],
            "dayHigh": ["High", "أعلى سعر", "السعر الأعلى"],
            "dayLow": ["Low", "أدنى سعر", "السعر الأدنى"],
            "price": ["Last Price", "السعر", "آخر سعر", "الإغلاق"],
        }

    def normalize_number(self, text: Optional[str]) -> Optional[float]:
        """Enhanced number normalization with Arabic numeral support."""
        if not text:
            return None

        text = str(text).strip()
        if not text:
            return None

        # Convert Arabic numerals to English
        text = text.translate(self._arabic_to_english)
        text = text.replace(",", "").replace(" ", "").strip()

        # Multiplier mapping
        multipliers = {
            "k": 1e3,
            "m": 1e6,
            "b": 1e9,
            "t": 1e12,
            "%": 0.01,
            "٪": 0.01,
        }

        match = re.match(r"(-?\d+(?:\.\d+)?)([kKmMbBtT%٪]?)", text)
        if match:
            value = float(match.group(1))
            suffix = match.group(2).lower()
            if suffix in multipliers:
                value *= multipliers[suffix]
            return value

        return None

    def extract_field_value(self, soup, field_name: str) -> Optional[float]:
        """Extract field value using multiple search strategies."""
        if not HAS_BS or not soup:
            return None

        labels = self._field_mappings.get(field_name, [])
        if not labels:
            return None

        pattern = re.compile("|".join(re.escape(label) for label in labels), re.IGNORECASE)

        for element in soup.find_all(string=pattern):
            try:
                container = element.parent
                if container:
                    text_areas = [
                        container.get_text(" ", strip=True),
                        container.parent.get_text(" ", strip=True)
                        if container.parent
                        else "",
                    ]

                    for text in text_areas:
                        numbers = self._number_pattern.findall(text)
                        for num_str in numbers:
                            value = self.normalize_number(num_str)
                            if value is not None:
                                logger.debug("📊 Argaam: Found %s: %s in '%s'", field_name, value, text)
                                return value
            except Exception as e:
                logger.debug("🔍 Argaam: Error extracting %s: %s", field_name, e)
                continue

        return None

    def parse_company_snapshot(self, html: str) -> Dict[str, Any]:
        """Enhanced company data parsing with multiple fallback strategies."""
        result: Dict[str, Any] = {
            "name": None,
            "sector": None,
            "industry": None,
            "price": None,
            "dayHigh": None,
            "dayLow": None,
            "volume": None,
            "marketCap": None,
            "sharesOutstanding": None,
            "dividendYield": None,
            "eps": None,
            "pe": None,
            "beta": None,
            "lastUpdated": datetime.utcnow().isoformat(),
            "dataQuality": "low",  # low, medium, high, none, error
        }

        if not HAS_BS or not html:
            result["dataQuality"] = "none"
            logger.warning("⚠️ Argaam: No HTML or BeautifulSoup - returning empty data snapshot")
            return result

        try:
            soup = None
            for parser in HTML_PARSERS:
                try:
                    soup = BeautifulSoup(html, parser)  # type: ignore
                    logger.debug("✅ Argaam: Successfully parsed HTML with %s", parser)
                    break
                except Exception as e:
                    logger.debug("⚠️ Argaam: Parser %s failed: %s", parser, e)
                    continue

            if not soup:
                logger.warning("❌ Argaam: No HTML parser succeeded")
                return result

            # Extract company name
            name_selectors = ["h1", ".company-name", ".stock-name", "title"]
            for selector in name_selectors:
                element = soup.select_one(selector)
                if element:
                    result["name"] = element.get_text(" ", strip=True)
                    logger.debug("🏢 Argaam: Found company name: %s", result["name"])
                    break

            # Extract financial fields
            data_points_found = 0
            for field in self._field_mappings.keys():
                value = self.extract_field_value(soup, field)
                if value is not None:
                    result[field] = value
                    data_points_found += 1

            # Assess data quality
            if data_points_found >= 5:
                result["dataQuality"] = "high"
            elif data_points_found >= 3:
                result["dataQuality"] = "medium"
            else:
                result["dataQuality"] = "low"

            logger.info(
                "📈 Argaam: Parsed %d data points with %s quality",
                data_points_found,
                result["dataQuality"],
            )

        except Exception as e:
            logger.error("💥 Argaam: Error parsing HTML: %s", e)
            result["dataQuality"] = "error"

        return result


data_parser = DataParser()

# -------------------------------------------------------------------------
# URL Discoverer
# -------------------------------------------------------------------------
class URLDiscoverer:
    def __init__(self) -> None:
        self._url_templates = [
            # High priority - English pages
            "{base}/en/company/{code}",
            "{base}/en/company/financials/{code}",
            "{base}/en/tadawul/company?symbol={code}",
            # Medium priority - Arabic pages
            "{base}/ar/company/{code}",
            "{base}/ar/company/financials/{code}",
            "{base}/ar/tadawul/company?symbol={code}",
            # Fallback - Search pages
            "{base}/en/search?q={code}",
            "{base}/ar/search?q={code}",
        ]
        self._successful_patterns: Dict[str, str] = {}

    async def discover_company_url(self, code: str) -> Optional[str]:
        """Discover company URL with pattern learning."""
        code_pattern = code[:2] + "XX"  # Group by first two digits
        if code_pattern in self._successful_patterns:
            template = self._successful_patterns[code_pattern]
            url = template.format(base=config.BASE_URL, code=quote(code))
            logger.debug("🔄 Argaam: Trying cached pattern for %s: %s", code, template)
            content = await html_fetcher.fetch(url)
            if content and len(content) > 2000:
                logger.info("✅ Argaam: Found URL using cached pattern: %s", url)
                return url

        for template in self._url_templates:
            url = template.format(base=config.BASE_URL, code=quote(code))
            logger.debug("🔄 Argaam: Trying template for %s: %s", code, template)
            content = await html_fetcher.fetch(url)

            if content and len(content) > 2000:
                self._successful_patterns[code_pattern] = template
                logger.info("✅ Argaam: Discovered URL for %s: %s", code, url)
                return url

        logger.warning("❌ Argaam: No valid URL discovered for %s", code)
        return None


url_discoverer = URLDiscoverer()

# -------------------------------------------------------------------------
# Data Fetcher with Cache Integration
# -------------------------------------------------------------------------
async def fetch_company_data(code: str, ttl: Optional[int] = None) -> Dict[str, Any]:
    """
    Enhanced company data fetching with cache integration.

    NOTE: TTL parameter is accepted for future extension; current cache TTL
    is controlled globally by ARGAAM_CACHE_TTL / CACHE_DEFAULT_TTL.
    """
    if ttl is None:
        ttl = config.CACHE_TTL

    cache_key = f"argaam::{code}"

    # Cache first
    cached_data = _CACHE.get(cache_key)
    if cached_data:
        logger.info("💾 Argaam: Cache hit for %s", code)
        return cached_data

    logger.info("🔄 Argaam: Cache miss for %s, fetching fresh data", code)

    # Discover and fetch data
    url = await url_discoverer.discover_company_url(code)
    if not url:
        result = {"error": "Company not found", "lastUpdated": datetime.utcnow().isoformat()}
        _CACHE.set(cache_key, result)
        return result

    html = await html_fetcher.fetch(url)
    if not html:
        result = {"error": "Failed to fetch data", "lastUpdated": datetime.utcnow().isoformat()}
        _CACHE.set(cache_key, result)
        return result

    result = data_parser.parse_company_snapshot(html)
    _CACHE.set(cache_key, result)

    logger.info("✅ Argaam: Successfully fetched and cached data for %s", code)
    return result

# -------------------------------------------------------------------------
# Pydantic Models
# -------------------------------------------------------------------------
class ArgaamRequest(BaseModel):
    tickers: List[str] = Field(..., min_items=1, max_items=50)
    cache_ttl: int = Field(default=config.CACHE_TTL, ge=60, le=3600)
    priority: str = Field(default="normal", pattern="^(low|normal|high)$")

    @field_validator("tickers")
    @classmethod
    def validate_tickers(cls, v: List[str]) -> List[str]:
        if len(v) > 50:
            raise ValueError("Maximum 50 tickers allowed per request")
        return v


class ArgaamQuoteResponse(BaseModel):
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    price: Optional[float] = Field(None, ge=0)
    dayHigh: Optional[float] = Field(None, ge=0)
    dayLow: Optional[float] = Field(None, ge=0)
    volume: Optional[float] = Field(None, ge=0)
    marketCap: Optional[float] = Field(None, ge=0)
    sharesOutstanding: Optional[float] = Field(None, ge=0)
    dividendYield: Optional[float] = Field(None, ge=0, le=100)
    eps: Optional[float] = None
    pe: Optional[float] = Field(None, ge=0)
    beta: Optional[float] = None
    lastUpdated: Optional[str] = None
    dataQuality: Optional[str] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    bs4_available: bool
    cache: Dict[str, Any]
    timestamp: float
    uptime: float
    requests_served: int

# -------------------------------------------------------------------------
# Background Tasks & Monitoring
# -------------------------------------------------------------------------
class PerformanceMonitor:
    def __init__(self) -> None:
        self.start_time = time.time()
        self.requests_served = 0
        self.errors_count = 0

    def record_request(self) -> None:
        self.requests_served += 1

    def record_error(self) -> None:
        self.errors_count += 1

    def get_stats(self) -> Dict[str, Any]:
        uptime = time.time() - self.start_time
        error_rate = self.errors_count / max(self.requests_served, 1)
        requests_per_hour = self.requests_served / (uptime / 3600) if uptime > 0 else 0

        return {
            "uptime": round(uptime, 2),
            "requests_served": self.requests_served,
            "errors_count": self.errors_count,
            "error_rate": round(error_rate, 4),
            "requests_per_hour": round(requests_per_hour, 2),
        }


performance_monitor = PerformanceMonitor()


async def background_cache_cleanup() -> None:
    """Background task to periodically log cache stats."""
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        cache_info = _CACHE.info()
        stats = performance_monitor.get_stats()
        logger.info("📊 Argaam Performance Stats - Cache: %s, Requests: %s", cache_info, stats)

# -------------------------------------------------------------------------
# Lifecycle Hooks
# -------------------------------------------------------------------------
@router.on_event("startup")
async def startup_event() -> None:
    """Initialize background tasks on startup."""
    try:
        asyncio.create_task(background_cache_cleanup())
        logger.info("🚀 Argaam routes started with enhanced features (v3.2.0)")
    except Exception as e:
        logger.error("💥 Argaam: Failed to start background cache cleanup task: %s", e)


@router.on_event("shutdown")
async def shutdown_event() -> None:
    """Cleanup on shutdown."""
    await http_client._close_client()
    logger.info("🛑 Argaam routes shutdown complete")

# -------------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------------
@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> Dict[str, Any]:
    """Enhanced health check endpoint."""
    auth_manager.validate_auth(request)

    stats = performance_monitor.get_stats()
    return {
        "status": "healthy",
        "bs4_available": HAS_BS,
        "cache": _CACHE.info(),
        "timestamp": time.time(),
        "uptime": stats["uptime"],
        "requests_served": stats["requests_served"],
    }


@router.post("/quotes", response_model=Dict[str, Any])
async def get_quotes(
    request: ArgaamRequest,
    http_request: Request,
    background_tasks: BackgroundTasks,
) -> Dict[str, Any]:
    """Enhanced quotes endpoint with background processing."""
    auth_manager.validate_auth(http_request)
    performance_monitor.record_request()

    if not HAS_BS:
        logger.warning("⚠️ Argaam: BeautifulSoup not available - limited functionality")

    # Normalize tickers
    normalized_tickers = [ticker_normalizer.normalize(t) for t in request.tickers]
    unique_tickers = list(dict.fromkeys(normalized_tickers))

    logger.info("📥 Argaam: Processing %d unique tickers: %s", len(unique_tickers), unique_tickers)

    tasks: List[asyncio.Task] = []
    for code in unique_tickers:
        task = asyncio.create_task(fetch_company_data(code, ttl=request.cache_ttl))
        tasks.append(task)

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("✅ Argaam: Successfully gathered data for %d tickers", len(tasks))
    except Exception as e:
        logger.error("💥 Argaam: Error gathering company data: %s", e)
        performance_monitor.record_error()
        raise HTTPException(
            status_code=500,
            detail="Internal server error during Argaam data gathering",
        )

    processed_results: Dict[str, Any] = {}
    successful_count = 0
    error_count = 0

    for code, result in zip(unique_tickers, results):
        if isinstance(result, Exception):
            logger.error("❌ Argaam: Error fetching data for %s: %s", code, result)
            processed_results[code] = {
                "error": f"Data fetch failed: {str(result)}",
                "lastUpdated": datetime.utcnow().isoformat(),
            }
            performance_monitor.record_error()
            error_count += 1
        else:
            processed_results[code] = ArgaamQuoteResponse(**result).dict()
            if not result.get("error"):
                successful_count += 1
            else:
                error_count += 1

    response_data = {
        "ok": True,
        "data": processed_results,
        "metadata": {
            "requested": len(request.tickers),
            "processed": len(unique_tickers),
            "successful": successful_count,
            "errors": error_count,
            "cache_ttl": request.cache_ttl,
            "cache": _CACHE.info(),
            "performance": performance_monitor.get_stats(),
            "timestamp": datetime.utcnow().isoformat(),
        },
    }

    logger.info("📤 Argaam: Returning data: %d successful, %d errors", successful_count, error_count)
    return response_data


@router.get("/quotes", response_model=Dict[str, Any])
async def get_quotes_endpoint(
    tickers: str,
    cache_ttl: int = config.CACHE_TTL,
    request: Request = None,
) -> Dict[str, Any]:
    """
    GET endpoint for quotes (convenience wrapper around POST /quotes).

    Example:
      /v1/argaam/quotes?tickers=2222,1120,2010
    """
    if request is None:
        # Should not happen under FastAPI, but keep defensive
        raise HTTPException(status_code=500, detail="Request context missing")

    auth_manager.validate_auth(request)
    ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        raise HTTPException(status_code=400, detail="No tickers provided")

    argaam_request = ArgaamRequest(tickers=ticker_list, cache_ttl=cache_ttl)
    # We create a dummy BackgroundTasks instance for compatibility
    return await get_quotes(argaam_request, request, BackgroundTasks())


@router.get("/cache/info")
async def get_cache_info(request: Request) -> Dict[str, Any]:
    """Enhanced cache information endpoint."""
    auth_manager.validate_auth(request)
    cache_info = _CACHE.info()
    logger.info("📊 Argaam: Cache info requested: %s", cache_info)
    return cache_info


@router.delete("/cache/clear")
async def clear_cache(request: Request) -> Dict[str, Any]:
    """Clear cache endpoint."""
    auth_manager.validate_auth(request)
    previous_info = _CACHE.info()
    _CACHE.clear()
    logger.info("🗑️ Argaam: Cache cleared successfully")
    return {
        "cleared": True,
        "previous": previous_info,
        "message": "Cache cleared successfully",
    }


@router.get("/test/{ticker}")
async def test_ticker(ticker: str, request: Request) -> Dict[str, Any]:
    """Enhanced test endpoint for debugging."""
    auth_manager.validate_auth(request)

    normalized = ticker_normalizer.normalize(ticker)
    data = await fetch_company_data(normalized)

    return {
        "ticker": ticker,
        "normalized": normalized,
        "data": data,
        "bs4_available": HAS_BS,
        "available_parsers": HTML_PARSERS if HAS_BS else [],
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/stats")
async def get_stats(request: Request) -> Dict[str, Any]:
    """Get performance statistics."""
    auth_manager.validate_auth(request)
    stats = performance_monitor.get_stats()
    logger.info("📈 Argaam: Stats requested: %s", stats)
    return stats


@router.get("/")
async def root() -> Dict[str, Any]:
    """Root endpoint with API information."""
    return {
        "ok": True,
        "message": "Argaam Integration API",
        "version": "3.2.0",
        "status": "operational",
        "base_url": config.BASE_URL,
        "endpoints": {
            "health": "/v1/argaam/health",
            "quotes": "/v1/argaam/quotes",
            "cache_info": "/v1/argaam/cache/info",
            "stats": "/v1/argaam/stats",
            "test": "/v1/argaam/test/{ticker}",
        },
        "features": {
            "beautifulsoup_available": HAS_BS,
            "authentication_required": config.REQUIRE_AUTH,
            "caching_enabled": True,
            "rate_limiting": True,
        },
    }
