# =============================================================================
# Argaam Integration Routes - Ultimate Investment Dashboard (v2.3.0)
# Enhanced with Resilience, Monitoring & Advanced Error Handling
# =============================================================================
from __future__ import annotations

import asyncio
import os
import re
import time
import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
from datetime import datetime, timedelta

import httpx
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel, Field, validator
import cachetools
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import backoff

# -----------------------------------------------------------------------------
# Enhanced Logging Setup
# -----------------------------------------------------------------------------
logger = logging.getLogger("argaam_routes")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Optional Parser (BeautifulSoup) with Fallback Strategy
# -----------------------------------------------------------------------------
HAS_BS = False
BeautifulSoup = None
HTML_PARSERS = ['lxml', 'html.parser', 'html5lib']

try:
    from bs4 import BeautifulSoup
    HAS_BS = True
    logger.info("BeautifulSoup available - enhanced parsing enabled")
except ImportError as e:
    logger.warning(f"BeautifulSoup not available: {e}. Basic parsing only.")

# -----------------------------------------------------------------------------
# Router
# -----------------------------------------------------------------------------
router = APIRouter(prefix="/v1/argaam", tags=["Argaam"])

# -----------------------------------------------------------------------------
# Enhanced Configuration with Validation
# -----------------------------------------------------------------------------
class ArgaamConfig:
    def __init__(self):
        self.USER_AGENT = os.getenv(
            "ARGAAM_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        ).strip()
        
        self.APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
        self.BACKUP_APP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "").strip()
        self.REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "true").lower() == "true"
        
        self.BASE_URL = os.getenv("ARGAAM_BASE_URL", "https://www.argaam.com")
        self.TIMEOUT = int(os.getenv("ARGAAM_TIMEOUT", "30"))
        self.MAX_RETRIES = int(os.getenv("ARGAAM_MAX_RETRIES", "3"))
        self.CACHE_TTL = int(os.getenv("ARGAAM_CACHE_TTL", "600"))
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
    
    def validate_config(self):
        """Validate critical configuration parameters"""
        if self.REQUIRE_AUTH and not (self.APP_TOKEN or self.BACKUP_APP_TOKEN):
            logger.warning("Authentication required but no tokens configured")
        
        if self.TIMEOUT < 5:
            logger.warning("Timeout too low, increasing to minimum 5 seconds")
            self.TIMEOUT = 5
            
        if self.MAX_RETRIES > 5:
            logger.warning("Max retries too high, capping at 5")
            self.MAX_RETRIES = 5

config = ArgaamConfig()

# -----------------------------------------------------------------------------
# Enhanced TTL Cache with LRU Eviction
# -----------------------------------------------------------------------------
class TTLCache:
    def __init__(self, maxsize: int = 1000, ttl: int = 600):
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
            if len(self._cache) < old_size:
                self._evictions += 1
        except Exception as e:
            logger.warning(f"Cache set failed for key {key}: {e}")
    
    def clear(self) -> None:
        self._cache.clear()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
    
    def info(self) -> Dict[str, Any]:
        return {
            "size": len(self._cache),
            "max_size": self._cache.maxsize,
            "hits": self._hits,
            "misses": self._misses,
            "hit_ratio": self._hits / (self._hits + self._misses) if (self._hits + self._misses) > 0 else 0,
            "evictions": self._evictions,
            "ttl": self._cache.ttl,
        }

# Initialize enhanced cache
_CACHE = TTLCache(maxsize=config.CACHE_MAXSIZE, ttl=config.CACHE_TTL)

# -----------------------------------------------------------------------------
# Enhanced Async HTTP Client with Connection Pooling
# -----------------------------------------------------------------------------
class ArgaamHTTPClient:
    def __init__(self):
        self._client = None
        self._last_used = None
        self._request_count = 0
    
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
            logger.info("Created new HTTP client")
        
        self._last_used = time.time()
        return self._client
    
    def _should_recreate_client(self) -> bool:
        if self._last_used is None:
            return True
        # Recreate client every 30 minutes or after 1000 requests
        time_elapsed = time.time() - self._last_used
        return time_elapsed > 1800 or self._request_count > 1000
    
    async def _close_client(self):
        if self._client:
            try:
                await self._client.aclose()
                logger.info("Closed HTTP client")
            except Exception as e:
                logger.warning(f"Error closing HTTP client: {e}")
            finally:
                self._client = None
    
    def increment_request_count(self):
        self._request_count += 1

http_client = ArgaamHTTPClient()

# -----------------------------------------------------------------------------
# Enhanced Authentication with Rate Limiting
# -----------------------------------------------------------------------------
class AuthManager:
    def __init__(self):
        self._failed_attempts = {}
        self._max_failed_attempts = 5
        self._lockout_duration = 300  # 5 minutes
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request"""
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"
    
    def _is_ip_locked(self, ip: str) -> bool:
        """Check if IP is temporarily locked due to failed attempts"""
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
    
    def _record_failed_attempt(self, ip: str):
        """Record failed authentication attempt"""
        now = time.time()
        if ip not in self._failed_attempts:
            self._failed_attempts[ip] = [1, now]
        else:
            self._failed_attempts[ip][0] += 1
            # Reset if last failure was long ago
            if now - self._failed_attempts[ip][1] > 3600:  # 1 hour
                self._failed_attempts[ip] = [1, now]
    
    def validate_auth(self, request: Request) -> None:
        """Enhanced authentication validation"""
        if not config.REQUIRE_AUTH:
            return

        client_ip = self._get_client_ip(request)
        
        # Check IP lockout
        if self._is_ip_locked(client_ip):
            logger.warning(f"IP {client_ip} temporarily locked due to failed attempts")
            raise HTTPException(
                status_code=429, 
                detail="Too many failed attempts. Please try again later."
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
            logger.warning(f"Invalid token attempt from IP {client_ip}")
            raise HTTPException(status_code=403, detail="Invalid or expired token")
        
        # Reset failed attempts on successful auth
        if client_ip in self._failed_attempts:
            del self._failed_attempts[client_ip]

auth_manager = AuthManager()

# -----------------------------------------------------------------------------
# Enhanced Ticker Normalization
# -----------------------------------------------------------------------------
class TickerNormalizer:
    def __init__(self):
        self._code_re = re.compile(r"(\d{1,4})")
        self._common_aliases = {
            'stc': '7010', 'aramco': '2222', 'alrajhi': '1120',
            'sabic': '2010', 'riyadbank': '1010', 'alinnma': '1150'
        }
    
    def normalize(self, code: str) -> str:
        """Enhanced ticker normalization with alias support"""
        if not code:
            return ""
        
        # Convert to uppercase and remove common suffixes
        code = code.upper().replace(".SR", "").replace("TASI:", "").strip()
        
        # Check for common aliases
        if code in self._common_aliases:
            return self._common_aliases[code]
        
        # Extract numeric part
        match = self._code_re.search(code)
        if match:
            normalized = match.group(1).zfill(4)  # Pad with leading zeros
            logger.debug(f"Normalized {code} -> {normalized}")
            return normalized
        
        return code

ticker_normalizer = TickerNormalizer()

# -----------------------------------------------------------------------------
# Enhanced HTML Fetcher with Retry Logic
# -----------------------------------------------------------------------------
class HTMLFetcher:
    def __init__(self):
        self._session_semaphore = asyncio.Semaphore(10)  # Limit concurrent requests
    
    @retry(
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError))
    )
    async def fetch(self, url: str) -> Optional[str]:
        """Enhanced HTML fetching with retry logic and rate limiting"""
        async with self._session_semaphore:
            client = await http_client.get_client()
            http_client.increment_request_count()
            
            try:
                logger.info(f"Fetching URL: {url}")
                response = await client.get(url)
                
                if response.status_code == 200:
                    content = response.text
                    if len(content) > 1000:  # Basic content validation
                        logger.info(f"Successfully fetched {len(content)} bytes from {url}")
                        return content
                    else:
                        logger.warning(f"Content too short from {url}: {len(content)} bytes")
                        return None
                elif response.status_code == 404:
                    logger.info(f"URL not found: {url}")
                    return None
                elif response.status_code == 403:
                    logger.warning(f"Access forbidden: {url}")
                    raise httpx.HTTPStatusError("403 Forbidden", request=response.request, response=response)
                else:
                    logger.warning(f"HTTP {response.status_code} from {url}")
                    response.raise_for_status()
                    
            except httpx.TimeoutException:
                logger.error(f"Timeout fetching {url}")
                raise
            except httpx.NetworkError as e:
                logger.error(f"Network error fetching {url}: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}")
                return None
            finally:
                # Rate limiting
                await asyncio.sleep(config.RATE_LIMIT_DELAY)
        
        return None

html_fetcher = HTMLFetcher()

# -----------------------------------------------------------------------------
# Enhanced Data Parser with Multiple Strategies
# -----------------------------------------------------------------------------
class DataParser:
    def __init__(self):
        self._number_pattern = re.compile(r"-?\d+[,٠-٩]*(?:\.\d+)?\s*[%kKmMbBtT]?")
        self._arabic_to_english = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
        
        self._field_mappings = {
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
        """Enhanced number normalization with Arabic numeral support"""
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
            "k": 1e3, "m": 1e6, "b": 1e9, "t": 1e12,
            "%": 0.01, "٪": 0.01,
        }
        
        # Match numbers with optional multipliers
        match = re.match(r"(-?\d+(?:\.\d+)?)([kKmMbBtT%٪]?)", text)
        if match:
            value = float(match.group(1))
            suffix = match.group(2).lower()
            if suffix in multipliers:
                value *= multipliers[suffix]
            return value
        
        return None
    
    def extract_field_value(self, soup, field_name: str) -> Optional[float]:
        """Extract field value using multiple search strategies"""
        if not HAS_BS or not soup:
            return None
            
        labels = self._field_mappings.get(field_name, [])
        if not labels:
            return None
        
        # Strategy 1: Direct text search
        pattern = re.compile("|".join(re.escape(label) for label in labels), re.IGNORECASE)
        
        for element in soup.find_all(string=pattern):
            try:
                # Look for numbers in the same container
                container = element.parent
                if container:
                    # Check siblings and parent
                    text_areas = [
                        container.get_text(" ", strip=True),
                        container.parent.get_text(" ", strip=True) if container.parent else "",
                    ]
                    
                    for text in text_areas:
                        numbers = self._number_pattern.findall(text)
                        for num_str in numbers:
                            value = self.normalize_number(num_str)
                            if value is not None:
                                logger.debug(f"Found {field_name}: {value} in '{text}'")
                                return value
            except Exception as e:
                logger.debug(f"Error extracting {field_name}: {e}")
                continue
        
        return None
    
    def parse_company_snapshot(self, html: str) -> Dict[str, Any]:
        """Enhanced company data parsing with multiple fallback strategies"""
        result = {
            "name": None, "sector": None, "industry": None,
            "price": None, "dayHigh": None, "dayLow": None,
            "volume": None, "marketCap": None,
            "sharesOutstanding": None, "dividendYield": None,
            "eps": None, "pe": None, "beta": None,
            "lastUpdated": datetime.utcnow().isoformat(),
            "dataQuality": "low"  # Can be low, medium, high
        }
        
        if not HAS_BS or not html:
            result["dataQuality"] = "none"
            return result
        
        try:
            # Try multiple parsers for better compatibility
            for parser in HTML_PARSERS:
                try:
                    soup = BeautifulSoup(html, parser)
                    break
                except Exception:
                    continue
            else:
                logger.warning("No HTML parser succeeded")
                return result
            
            # Extract company name
            name_selectors = ["h1", ".company-name", ".stock-name", "title"]
            for selector in name_selectors:
                element = soup.select_one(selector)
                if element:
                    result["name"] = element.get_text(" ", strip=True)
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
                
            logger.info(f"Parsed {data_points_found} data points with {result['dataQuality']} quality")
            
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            result["dataQuality"] = "error"
        
        return result

data_parser = DataParser()

# -----------------------------------------------------------------------------
# Enhanced URL Discovery with Priority Queue
# -----------------------------------------------------------------------------
class URLDiscoverer:
    def __init__(self):
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
        self._successful_patterns = {}  # Track which patterns work for which codes
    
    async def discover_company_url(self, code: str) -> Optional[str]:
        """Discover company URL with pattern learning"""
        # Check if we have a known good pattern for this code pattern
        code_pattern = code[:2] + "XX"  # Group by first two digits
        if code_pattern in self._successful_patterns:
            template = self._successful_patterns[code_pattern]
            url = template.format(base=config.BASE_URL, code=quote(code))
            content = await html_fetcher.fetch(url)
            if content and len(content) > 2000:
                return url
        
        # Try all templates
        for template in self._url_templates:
            url = template.format(base=config.BASE_URL, code=quote(code))
            content = await html_fetcher.fetch(url)
            
            if content and len(content) > 2000:
                # Remember successful pattern
                self._successful_patterns[code_pattern] = template
                logger.info(f"Discovered URL for {code}: {url}")
                return url
        
        logger.warning(f"No valid URL discovered for {code}")
        return None

url_discoverer = URLDiscoverer()

# -----------------------------------------------------------------------------
# Enhanced Data Fetcher with Cache Integration
# -----------------------------------------------------------------------------
async def fetch_company_data(code: str, ttl: int = None) -> Dict[str, Any]:
    """Enhanced company data fetching with cache integration"""
    if ttl is None:
        ttl = config.CACHE_TTL
    
    cache_key = f"argaam::{code}"
    
    # Check cache first
    cached_data = _CACHE.get(cache_key)
    if cached_data:
        logger.info(f"Cache hit for {code}")
        return cached_data
    
    logger.info(f"Cache miss for {code}, fetching fresh data")
    
    # Discover and fetch data
    url = await url_discoverer.discover_company_url(code)
    if not url:
        # Cache negative result for shorter period
        result = {"error": "Company not found", "lastUpdated": datetime.utcnow().isoformat()}
        _CACHE.set(cache_key, result)
        return result
    
    html = await html_fetcher.fetch(url)
    if not html:
        result = {"error": "Failed to fetch data", "lastUpdated": datetime.utcnow().isoformat()}
        _CACHE.set(cache_key, result)
        return result
    
    # Parse and cache result
    result = data_parser.parse_company_snapshot(html)
    _CACHE.set(cache_key, result)
    
    return result

# -----------------------------------------------------------------------------
# Enhanced Pydantic Models
# -----------------------------------------------------------------------------
class ArgaamRequest(BaseModel):
    tickers: List[str] = Field(..., min_items=1, max_items=50)
    cache_ttl: int = Field(default=config.CACHE_TTL, ge=60, le=3600)
    priority: str = Field(default="normal", regex="^(low|normal|high)$")
    
    @validator('tickers')
    def validate_tickers(cls, v):
        if len(v) > 50:
            raise ValueError('Maximum 50 tickers allowed per request')
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

# -----------------------------------------------------------------------------
# Background Tasks & Monitoring
# -----------------------------------------------------------------------------
class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.requests_served = 0
        self.errors_count = 0
        self.last_reset = self.start_time
    
    def record_request(self):
        self.requests_served += 1
    
    def record_error(self):
        self.errors_count += 1
    
    def get_stats(self) -> Dict[str, Any]:
        uptime = time.time() - self.start_time
        return {
            "uptime": uptime,
            "requests_served": self.requests_served,
            "errors_count": self.errors_count,
            "error_rate": self.errors_count / max(self.requests_served, 1),
            "requests_per_hour": self.requests_served / (uptime / 3600),
        }

performance_monitor = PerformanceMonitor()

async def background_cache_cleanup():
    """Background task to periodically log cache stats"""
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        cache_info = _CACHE.info()
        logger.info(f"Cache stats: {cache_info}")

# -----------------------------------------------------------------------------
# Enhanced Routes
# -----------------------------------------------------------------------------
@router.on_event("startup")
async def startup_event():
    """Initialize background tasks on startup"""
    asyncio.create_task(background_cache_cleanup())
    logger.info("Argaam routes started with enhanced features")

@router.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await http_client._close_client()
    logger.info("Argaam routes shutdown complete")

@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request):
    """Enhanced health check endpoint"""
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
    background_tasks: BackgroundTasks
):
    """Enhanced quotes endpoint with background processing"""
    auth_manager.validate_auth(http_request)
    performance_monitor.record_request()
    
    if not HAS_BS:
        raise HTTPException(
            status_code=503,
            detail="Enhanced parsing unavailable. Install 'beautifulsoup4' and 'lxml'.",
        )
    
    # Normalize tickers
    normalized_tickers = [ticker_normalizer.normalize(t) for t in request.tickers]
    unique_tickers = list(dict.fromkeys(normalized_tickers))  # Remove duplicates preserving order
    
    logger.info(f"Processing {len(unique_tickers)} unique tickers: {unique_tickers}")
    
    # Fetch data concurrently
    tasks = [
        fetch_company_data(code, ttl=request.cache_ttl) 
        for code in unique_tickers
    ]
    
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Error gathering company data: {e}")
        performance_monitor.record_error()
        raise HTTPException(status_code=500, detail="Internal server error during data gathering")
    
    # Process results
    processed_results = {}
    for i, (code, result) in enumerate(zip(unique_tickers, results)):
        if isinstance(result, Exception):
            logger.error(f"Error fetching data for {code}: {result}")
            processed_results[code] = {
                "error": f"Data fetch failed: {str(result)}",
                "lastUpdated": datetime.utcnow().isoformat()
            }
            performance_monitor.record_error()
        else:
            processed_results[code] = ArgaamQuoteResponse(**result).dict()
    
    response_data = {
        "data": processed_results,
        "metadata": {
            "requested": len(request.tickers),
            "processed": len(unique_tickers),
            "cache_ttl": request.cache_ttl,
            "cache": _CACHE.info(),
            "performance": performance_monitor.get_stats(),
        },
    }
    
    return response_data

@router.get("/quotes", response_model=Dict[str, Any])
async def get_quotes_endpoint(
    tickers: str,
    cache_ttl: int = config.CACHE_TTL,
    request: Request = None
):
    """GET endpoint for quotes"""
    ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
    argaam_request = ArgaamRequest(tickers=ticker_list, cache_ttl=cache_ttl)
    return await get_quotes(argaam_request, request, BackgroundTasks())

@router.get("/cache/info")
async def get_cache_info(request: Request):
    """Enhanced cache information endpoint"""
    auth_manager.validate_auth(request)
    return _CACHE.info()

@router.delete("/cache/clear")
async def clear_cache(request: Request):
    """Clear cache endpoint"""
    auth_manager.validate_auth(request)
    previous_info = _CACHE.info()
    _CACHE.clear()
    return {
        "cleared": True, 
        "previous": previous_info,
        "message": "Cache cleared successfully"
    }

@router.get("/test/{ticker}")
async def test_ticker(ticker: str, request: Request):
    """Enhanced test endpoint for debugging"""
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
async def get_stats(request: Request):
    """Get performance statistics"""
    auth_manager.validate_auth(request)
    return performance_monitor.get_stats()
