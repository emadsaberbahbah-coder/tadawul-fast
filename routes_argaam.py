# =============================================================================
# Argaam Integration Routes - Ultimate Investment Dashboard (v3.3.0)
# Tadawul Fast Bridge / Stock Market Hub
#
# - Async httpx client with connection pooling
# - Auth compatible with APP_TOKEN / BACKUP_APP_TOKEN + REQUIRE_AUTH
#   (Bearer, raw Authorization, or ?token= supported)
# - TTL cache (LRU) for scraped data
# - BeautifulSoup-based HTML parsing with Arabic + English numeric handling
# - Consistent logging & metadata for Render deployment
# =============================================================================
from __future__ import annotations

import asyncio
import os
import re
import time
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import quote
from datetime import datetime

import httpx
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel, Field, validator
import cachetools
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logger = logging.getLogger("argaam_routes")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Optional BeautifulSoup parser
# -----------------------------------------------------------------------------
HAS_BS = False
BeautifulSoup = None
HTML_PARSERS = ["lxml", "html.parser", "html5lib"]

try:
    from bs4 import BeautifulSoup  # type: ignore

    HAS_BS = True
    logger.info("✅ BeautifulSoup available - enhanced Argaam parsing enabled")
except ImportError as e:
    logger.warning(f"⚠️ BeautifulSoup not available for Argaam: {e}. Basic behaviour only.")

# -----------------------------------------------------------------------------
# Router
# -----------------------------------------------------------------------------
router = APIRouter(prefix="/v1/argaam", tags=["Argaam"])

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
class ArgaamConfig:
    def __init__(self) -> None:
        # User-Agent: can be overridden via env
        self.USER_AGENT = os.getenv(
            "ARGAAM_USER_AGENT",
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
        ).strip()

        # Auth – keep consistent with main API style
        self.APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
        self.BACKUP_APP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "").strip()
        self.REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "true").lower() == "true"

        # Base Argaam URL and behavior
        self.BASE_URL = os.getenv("ARGAAM_BASE_URL", "https://www.argaam.com").rstrip("/")
        self.TIMEOUT = int(os.getenv("ARGAAM_TIMEOUT", "30"))
        self.MAX_RETRIES = int(os.getenv("ARGAAM_MAX_RETRIES", "3"))
        self.CACHE_TTL = int(os.getenv("ARGAAM_CACHE_TTL", "600"))
        self.CACHE_MAXSIZE = int(os.getenv("ARGAAM_CACHE_MAXSIZE", "1000"))
        self.RATE_LIMIT_DELAY = float(os.getenv("ARGAAM_RATE_LIMIT_DELAY", "0.15"))

        self.HEADERS = {
            "User-Agent": self.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5,ar;q=0.4",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        }

        self.validate_config()

    def validate_config(self) -> None:
        if self.REQUIRE_AUTH and not (self.APP_TOKEN or self.BACKUP_APP_TOKEN):
            logger.warning("⚠️ REQUIRE_AUTH=true but APP_TOKEN / BACKUP_APP_TOKEN not configured")

        if self.TIMEOUT < 5:
            logger.warning("⚠️ Argaam TIMEOUT too low (<5s); using 5s minimum")
            self.TIMEOUT = 5

        if self.MAX_RETRIES > 5:
            logger.warning("⚠️ Argaam MAX_RETRIES too high; capping at 5")
            self.MAX_RETRIES = 5

        logger.info(
            "✅ ArgaamConfig loaded: timeout=%ss, retries=%s, cache_ttl=%ss, require_auth=%s",
            self.TIMEOUT,
            self.MAX_RETRIES,
            self.CACHE_TTL,
            self.REQUIRE_AUTH,
        )


config = ArgaamConfig()

# -----------------------------------------------------------------------------
# TTL Cache (LRU)
# -----------------------------------------------------------------------------
class TTLCache:
    """
    Lightweight wrapper around cachetools.TTLCache with stats.
    """

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
        """
        Set key and estimate evictions:
        - If cache is full and key is new, TTLCache will evict 1 LRU item.
        """
        try:
            existed = key in self._cache
            old_size = len(self._cache)
            at_capacity = old_size >= self._cache.maxsize

            self._cache[key] = value

            if at_capacity and not existed and len(self._cache) == self._cache.maxsize:
                # We replaced some older entry
                self._evictions += 1
        except Exception as e:
            logger.warning(f"⚠️ Argaam cache set failed for key={key}: {e}")

    def clear(self) -> None:
        self._cache.clear()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def info(self) -> Dict[str, Any]:
        total_requests = self._hits + self._misses
        hit_ratio = self._hits / total_requests if total_requests > 0 else 0.0
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

# -----------------------------------------------------------------------------
# Async HTTP client (httpx) – shared, pooled
# -----------------------------------------------------------------------------
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
            logger.info("✅ Created new Argaam HTTP client")

        self._last_used = time.time()
        return self._client

    def _should_recreate_client(self) -> bool:
        if self._last_used is None:
            return True
        elapsed = time.time() - self._last_used
        return elapsed > 1800 or self._request_count > 1000  # 30 min or 1000 reqs

    async def _close_client(self) -> None:
        if self._client:
            try:
                await self._client.aclose()
                logger.info("✅ Closed Argaam HTTP client")
            except Exception as e:
                logger.warning(f"⚠️ Error closing Argaam HTTP client: {e}")
            finally:
                self._client = None

    def increment_request_count(self) -> None:
        self._request_count += 1


http_client = ArgaamHTTPClient()

# -----------------------------------------------------------------------------
# Authentication manager – consistent token logic
# -----------------------------------------------------------------------------
class AuthManager:
    """
    Auth aligned to main.py style:

    Accepts any of:
      - Authorization: Bearer <token>
      - Authorization: <token>
      - ?token=<token>

    Uses APP_TOKEN / BACKUP_APP_TOKEN and REQUIRE_AUTH from env.
    """

    def __init__(self) -> None:
        # Dict[ip] = [failures_count, first_failure_timestamp]
        self._failed_attempts: Dict[str, List[float]] = {}
        self._max_failed_attempts = 5
        self._lockout_duration = 300  # 5 minutes

    def _get_client_ip(self, request: Request) -> str:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _is_ip_locked(self, ip: str) -> bool:
        if ip in self._failed_attempts:
            failures, first_failure = self._failed_attempts[ip]
            if failures >= self._max_failed_attempts:
                lockout_time = first_failure + self._lockout_duration
                if time.time() < lockout_time:
                    return True
                else:
                    # Reset after lockout period
                    del self._failed_attempts[ip]
        return False

    def _record_failed_attempt(self, ip: str) -> None:
        now = time.time()
        if ip not in self._failed_attempts:
            self._failed_attempts[ip] = [1, now]
        else:
            failures, first_failure = self._failed_attempts[ip]
            # Reset window if >1h since first failure
            if now - first_failure > 3600:
                self._failed_attempts[ip] = [1, now]
            else:
                self._failed_attempts[ip] = [failures + 1, first_failure]

    def _extract_token(self, request: Request) -> Optional[str]:
        """
        Extract token from:
          1) Authorization: Bearer <token>
          2) Authorization: <token>
          3) ?token=<token>
        """
        # Query param
        token = request.query_params.get("token")
        if token:
            return token.strip()

        # Authorization header
        auth_header = request.headers.get("Authorization", "").strip()
        if not auth_header:
            return None

        if auth_header.lower().startswith("bearer "):
            return auth_header.split(" ", 1)[1].strip()

        # Raw token header (no Bearer prefix)
        return auth_header

    def validate_auth(self, request: Request) -> None:
        if not config.REQUIRE_AUTH:
            return

        client_ip = self._get_client_ip(request)

        # Rate-limit repeated failures
        if self._is_ip_locked(client_ip):
            logger.warning(f"🚫 IP {client_ip} temporarily locked (too many failed attempts)")
            raise HTTPException(
                status_code=429,
                detail="Too many failed attempts. Please try again later.",
            )

        allowed_tokens = {t for t in (config.APP_TOKEN, config.BACKUP_APP_TOKEN) if t}
        if not allowed_tokens:
            logger.error("🚨 No APP_TOKEN / BACKUP_APP_TOKEN configured on server")
            raise HTTPException(
                status_code=500,
                detail="Server authentication configuration is missing tokens",
            )

        token = self._extract_token(request)
        if not token:
            self._record_failed_attempt(client_ip)
            raise HTTPException(status_code=401, detail="Missing authentication token")

        if token not in allowed_tokens:
            self._record_failed_attempt(client_ip)
            logger.warning(f"🚫 Invalid token from IP={client_ip}")
            raise HTTPException(status_code=403, detail="Invalid or expired token")

        if client_ip in self._failed_attempts:
            del self._failed_attempts[client_ip]

        logger.debug(f"✅ Authentication successful for IP={client_ip}")


auth_manager = AuthManager()

# -----------------------------------------------------------------------------
# Ticker normalization
# -----------------------------------------------------------------------------
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
        if not code:
            return ""

        raw = code.strip()
        code_u = raw.upper().replace(".SR", "").replace("TASI:", "").strip()

        # Check aliases (case-insensitive)
        alias_key = code_u.lower()
        if alias_key in self._common_aliases:
            normalized = self._common_aliases[alias_key]
            logger.debug(f"🔤 Argaam alias normalized {raw} -> {normalized}")
            return normalized

        # Extract numeric component
        match = self._code_re.search(code_u)
        if match:
            normalized = match.group(1).zfill(4)
            logger.debug(f"🔤 Argaam numeric normalized {raw} -> {normalized}")
            return normalized

        logger.warning(f"⚠️ Could not normalize ticker for Argaam: {raw}")
        return raw


ticker_normalizer = TickerNormalizer()

# -----------------------------------------------------------------------------
# HTML fetcher with retry
# -----------------------------------------------------------------------------
class HTMLFetcher:
    def __init__(self) -> None:
        self._session_semaphore = asyncio.Semaphore(10)

    @retry(
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPError)),
    )
    async def fetch(self, url: str) -> Optional[str]:
        async with self._session_semaphore:
            client = await http_client.get_client()
            http_client.increment_request_count()

            try:
                logger.info(f"🌐 Argaam fetch URL: {url}")
                start = time.time()
                response = await client.get(url)
                elapsed = time.time() - start

                status = response.status_code
                if status == 200:
                    content = response.text
                    length = len(content)
                    if length > 1000:
                        logger.info(
                            "✅ Argaam fetched %d bytes from %s in %.2fs",
                            length,
                            url,
                            elapsed,
                        )
                        return content
                    logger.warning(
                        "⚠️ Argaam content too short (%d bytes) from %s", length, url
                    )
                    return None
                elif status == 404:
                    logger.info("❌ Argaam URL not found: %s", url)
                    return None
                elif status == 403:
                    logger.warning("🚫 Argaam access forbidden: %s", url)
                    raise httpx.HTTPStatusError(
                        "403 Forbidden", request=response.request, response=response
                    )
                elif status == 429:
                    logger.warning("🚫 Argaam rate limited: %s", url)
                    await asyncio.sleep(5)
                    raise httpx.HTTPStatusError(
                        "429 Too Many Requests", request=response.request, response=response
                    )
                else:
                    logger.warning("⚠️ Argaam HTTP %s from %s", status, url)
                    response.raise_for_status()
            except httpx.TimeoutException:
                logger.error("⏰ Argaam timeout for %s", url)
                raise
            except httpx.HTTPError as e:
                logger.error("🌐 Argaam HTTP error for %s: %s", url, e)
                raise
            except Exception as e:
                logger.error("💥 Argaam unexpected error for %s: %s", url, e)
                return None
            finally:
                await asyncio.sleep(config.RATE_LIMIT_DELAY)

        return None


html_fetcher = HTMLFetcher()

# -----------------------------------------------------------------------------
# Data parser – numeric extraction, Arabic/English
# -----------------------------------------------------------------------------
class DataParser:
    def __init__(self) -> None:
        self._number_pattern = re.compile(r"-?\d+[,٠-٩]*(?:\.\d+)?\s*[%kKmMbBtT٪]?")
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
        if not text:
            return None

        s = str(text).strip()
        if not s:
            return None

        # Convert Arabic digits
        s = s.translate(self._arabic_to_english)
        s = s.replace(",", "").replace(" ", "").strip()

        multipliers = {
            "k": 1e3,
            "m": 1e6,
            "b": 1e9,
            "t": 1e12,
            "%": 0.01,
            "٪": 0.01,
        }

        m = re.match(r"(-?\d+(?:\.\d+)?)([kKmMbBtT%٪]?)", s)
        if m:
            value = float(m.group(1))
            suffix = m.group(2).lower()
            if suffix in multipliers:
                value *= multipliers[suffix]
            return value

        return None

    def extract_field_value(self, soup, field_name: str) -> Optional[float]:
        if not HAS_BS or not soup:
            return None

        labels = self._field_mappings.get(field_name, [])
        if not labels:
            return None

        pattern = re.compile("|".join(re.escape(label) for label in labels), re.IGNORECASE)

        for element in soup.find_all(string=pattern):
            try:
                container = element.parent
                if not container:
                    continue

                text_areas = [
                    container.get_text(" ", strip=True),
                    container.parent.get_text(" ", strip=True) if container.parent else "",
                ]

                for text in text_areas:
                    numbers = self._number_pattern.findall(text)
                    for num_str in numbers:
                        value = self.normalize_number(num_str)
                        if value is not None:
                            logger.debug("📊 Argaam %s=%s found in '%s'", field_name, value, text)
                            return value
            except Exception as e:
                logger.debug("🔍 Error extracting %s from Argaam HTML: %s", field_name, e)
                continue

        return None

    def parse_company_snapshot(self, html: str) -> Dict[str, Any]:
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
            "lastUpdated": datetime.utcnow().isoformat() + "Z",
            "dataQuality": "low",  # low, medium, high, none, error
        }

        if not HAS_BS or not html:
            result["dataQuality"] = "none"
            logger.warning("⚠️ No HTML or BeautifulSoup – returning minimal Argaam data")
            return result

        try:
            soup = None
            for parser in HTML_PARSERS:
                try:
                    soup = BeautifulSoup(html, parser)  # type: ignore
                    logger.debug("✅ Argaam HTML parsed with %s", parser)
                    break
                except Exception as e:
                    logger.debug("⚠️ Argaam parser %s failed: %s", parser, e)
                    continue

            if not soup:
                logger.warning("❌ No Argaam HTML parser succeeded")
                result["dataQuality"] = "error"
                return result

            # Name discovery
            name_selectors = ["h1", ".company-name", ".stock-name", "title"]
            for selector in name_selectors:
                el = soup.select_one(selector)
                if el:
                    result["name"] = el.get_text(" ", strip=True)
                    logger.debug("🏢 Argaam company name: %s", result["name"])
                    break

            # Extract numeric / KPI fields
            data_points_found = 0
            for field in self._field_mappings.keys():
                value = self.extract_field_value(soup, field)
                if value is not None:
                    result[field] = value
                    data_points_found += 1

            if data_points_found >= 5:
                result["dataQuality"] = "high"
            elif data_points_found >= 3:
                result["dataQuality"] = "medium"
            else:
                result["dataQuality"] = "low"

            logger.info(
                "📈 Argaam parsed %d data points, quality=%s",
                data_points_found,
                result["dataQuality"],
            )
        except Exception as e:
            logger.error("💥 Argaam HTML parsing error: %s", e)
            result["dataQuality"] = "error"

        return result


data_parser = DataParser()

# -----------------------------------------------------------------------------
# URL discoverer – pattern learning
# -----------------------------------------------------------------------------
class URLDiscoverer:
    def __init__(self) -> None:
        self._url_templates = [
            # English
            "{base}/en/company/{code}",
            "{base}/en/company/financials/{code}",
            "{base}/en/tadawul/company?symbol={code}",
            # Arabic
            "{base}/ar/company/{code}",
            "{base}/ar/company/financials/{code}",
            "{base}/ar/tadawul/company?symbol={code}",
            # Search
            "{base}/en/search?q={code}",
            "{base}/ar/search?q={code}",
        ]
        # key: code_pattern (e.g. "22XX") -> template
        self._successful_patterns: Dict[str, str] = {}

    async def discover_company_url(self, code: str) -> Optional[str]:
        code_pattern = code[:2] + "XX"
        if code_pattern in self._successful_patterns:
            template = self._successful_patterns[code_pattern]
            url = template.format(base=config.BASE_URL, code=quote(code))
            logger.debug("🔄 Argaam cached pattern for %s: %s", code, template)
            content = await html_fetcher.fetch(url)
            if content and len(content) > 2000:
                logger.info("✅ Argaam URL via cached pattern: %s", url)
                return url

        for template in self._url_templates:
            url = template.format(base=config.BASE_URL, code=quote(code))
            logger.debug("🔄 Argaam trying template %s for code=%s", template, code)
            content = await html_fetcher.fetch(url)
            if content and len(content) > 2000:
                self._successful_patterns[code_pattern] = template
                logger.info("✅ Argaam discovered URL for %s: %s", code, url)
                return url

        logger.warning("❌ No valid Argaam URL discovered for code=%s", code)
        return None


url_discoverer = URLDiscoverer()

# -----------------------------------------------------------------------------
# High-level data fetch + cache
# -----------------------------------------------------------------------------
async def fetch_company_data(code: str, ttl: Optional[int] = None) -> Dict[str, Any]:
    """
    Fetch company snapshot from Argaam with cache integration.
    TTL parameter exists for future extension; effective TTL is config.CACHE_TTL.
    """
    if ttl is None:
        ttl = config.CACHE_TTL

    cache_key = f"argaam::{code}"
    cached = _CACHE.get(cache_key)
    if cached:
        logger.info("💾 Argaam cache hit for %s", code)
        return cached

    logger.info("🔄 Argaam cache miss for %s – fetching from web", code)

    url = await url_discoverer.discover_company_url(code)
    if not url:
        result = {
            "error": "Company not found",
            "lastUpdated": datetime.utcnow().isoformat() + "Z",
            "dataQuality": "none",
        }
        _CACHE.set(cache_key, result)
        return result

    html = await html_fetcher.fetch(url)
    if not html:
        result = {
            "error": "Failed to fetch data",
            "lastUpdated": datetime.utcnow().isoformat() + "Z",
            "dataQuality": "none",
        }
        _CACHE.set(cache_key, result)
        return result

    result = data_parser.parse_company_snapshot(html)
    _CACHE.set(cache_key, result)
    logger.info("✅ Argaam data fetched & cached for %s", code)
    return result

# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------
class ArgaamRequest(BaseModel):
    tickers: List[str] = Field(..., min_items=1, max_items=50)
    cache_ttl: int = Field(default=config.CACHE_TTL, ge=60, le=3600)
    priority: str = Field(default="normal", regex="^(low|normal|high)$")

    @validator("tickers")
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


class ArgaamHealthResponse(BaseModel):
    status: str
    bs4_available: bool
    cache: Dict[str, Any]
    timestamp: float
    uptime: float
    requests_served: int


# -----------------------------------------------------------------------------
# Performance monitor
# -----------------------------------------------------------------------------
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
        rph = self.requests_served / (uptime / 3600) if uptime > 0 else 0.0

        return {
            "uptime": round(uptime, 2),
            "requests_served": self.requests_served,
            "errors_count": self.errors_count,
            "error_rate": round(error_rate, 4),
            "requests_per_hour": round(rph, 2),
        }


performance_monitor = PerformanceMonitor()


async def background_cache_cleanup() -> None:
    while True:
        await asyncio.sleep(300)  # 5 minutes
        cache_info = _CACHE.info()
        stats = performance_monitor.get_stats()
        logger.info("📊 Argaam Cache/Perf – cache=%s, stats=%s", cache_info, stats)

# -----------------------------------------------------------------------------
# Lifecycle events
# -----------------------------------------------------------------------------
@router.on_event("startup")
async def startup_event() -> None:
    try:
        asyncio.create_task(background_cache_cleanup())
        logger.info("🚀 Argaam routes started (v3.3.0)")
    except Exception as e:
        logger.error("💥 Failed to start Argaam background task: %s", e)


@router.on_event("shutdown")
async def shutdown_event() -> None:
    await http_client._close_client()
    logger.info("🛑 Argaam routes shutdown complete")

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/health", response_model=ArgaamHealthResponse)
async def health_check(request: Request) -> Dict[str, Any]:
    """Health check (secured if REQUIRE_AUTH=true)."""
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
    """
    POST endpoint for Argaam quotes – main entry for backend integrations.

    - Secured by APP_TOKEN / BACKUP_APP_TOKEN if REQUIRE_AUTH=true
    - Normalizes tickers (e.g., "2222.SR", "Aramco", "2222") to 4-digit codes
    - Uses HTML scraping + BeautifulSoup for basic snapshot fundamentals
    """
    auth_manager.validate_auth(http_request)
    performance_monitor.record_request()

    if not HAS_BS:
        logger.warning("⚠️ BeautifulSoup not available – Argaam functionality is limited")

    normalized_tickers = [ticker_normalizer.normalize(t) for t in request.tickers]
    unique_tickers = list(dict.fromkeys(normalized_tickers))

    logger.info("📥 Argaam processing %d unique tickers: %s", len(unique_tickers), unique_tickers)

    tasks: List[asyncio.Task] = []
    for code in unique_tickers:
        task = asyncio.create_task(fetch_company_data(code, ttl=request.cache_ttl))
        tasks.append(task)

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("✅ Argaam gathered data for %d tickers", len(tasks))
    except Exception as e:
        logger.error("💥 Argaam data gathering error: %s", e)
        performance_monitor.record_error()
        raise HTTPException(status_code=500, detail="Internal server error during data gathering")

    processed_results: Dict[str, Any] = {}
    successful_count = 0
    error_count = 0

    for code, result in zip(unique_tickers, results):
        if isinstance(result, Exception):
            logger.error("❌ Argaam error for %s: %s", code, result)
            processed_results[code] = {
                "error": f"Data fetch failed: {str(result)}",
                "lastUpdated": datetime.utcnow().isoformat() + "Z",
                "dataQuality": "error",
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
        "data": processed_results,
        "metadata": {
            "requested": len(request.tickers),
            "processed": len(unique_tickers),
            "successful": successful_count,
            "errors": error_count,
            "cache_ttl": request.cache_ttl,
            "cache": _CACHE.info(),
            "performance": performance_monitor.get_stats(),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        },
    }

    logger.info("📤 Argaam response – %d successful, %d errors", successful_count, error_count)
    return response_data


@router.get("/quotes", response_model=Dict[str, Any])
async def get_quotes_endpoint(
    tickers: str,
    cache_ttl: int = config.CACHE_TTL,
    request: Request = None,
) -> Dict[str, Any]:
    """
    GET convenience endpoint for quotes (wrapper around POST /quotes).

    Example:
        /v1/argaam/quotes?tickers=2222,1120,2010&token=YOUR_APP_TOKEN
    """
    if request is None:
        raise HTTPException(status_code=500, detail="Request context not available")

    ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        raise HTTPException(status_code=400, detail="No tickers provided")

    argaam_request = ArgaamRequest(tickers=ticker_list, cache_ttl=cache_ttl)
    return await get_quotes(argaam_request, request, BackgroundTasks())


@router.get("/cache/info")
async def get_cache_info(request: Request) -> Dict[str, Any]:
    """Return cache statistics."""
    auth_manager.validate_auth(request)
    cache_info = _CACHE.info()
    logger.info("📊 Argaam cache info requested: %s", cache_info)
    return cache_info


@router.delete("/cache/clear")
async def clear_cache(request: Request) -> Dict[str, Any]:
    """Clear Argaam cache."""
    auth_manager.validate_auth(request)
    previous = _CACHE.info()
    _CACHE.clear()
    logger.info("🗑️ Argaam cache cleared")
    return {
        "cleared": True,
        "previous": previous,
        "message": "Cache cleared successfully",
    }


@router.get("/test/{ticker}")
async def test_ticker(ticker: str, request: Request) -> Dict[str, Any]:
    """
    Debug endpoint to test a single ticker end-to-end.

    - Normalizes ticker
    - Fetches + parses Argaam snapshot
    - Returns raw snapshot + metadata
    """
    auth_manager.validate_auth(request)
    normalized = ticker_normalizer.normalize(ticker)
    data = await fetch_company_data(normalized)

    return {
        "ticker": ticker,
        "normalized": normalized,
        "data": data,
        "bs4_available": HAS_BS,
        "available_parsers": HTML_PARSERS if HAS_BS else [],
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@router.get("/stats")
async def get_stats(request: Request) -> Dict[str, Any]:
    """Performance statistics for Argaam routes."""
    auth_manager.validate_auth(request)
    stats = performance_monitor.get_stats()
    logger.info("📈 Argaam stats requested: %s", stats)
    return stats


@router.get("/")
async def root() -> Dict[str, Any]:
    """Root meta endpoint for Argaam integration."""
    return {
        "message": "Argaam Integration API",
        "version": "3.3.0",
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
