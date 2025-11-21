# routes_argaam.py
from __future__ import annotations

import asyncio
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

# BeautifulSoup is optional; endpoints will respond with a clear error if missing
try:
    from bs4 import BeautifulSoup  # type: ignore
    HAS_BEAUTIFULSOUP = True
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore
    HAS_BEAUTIFULSOUP = False

# ---------------------------------------------------------------------------
# Router (mounted under /v41/argaam by main.py)
# ---------------------------------------------------------------------------
router = APIRouter(prefix="/v41/argaam", tags=["argaam"])

# ---------------------------------------------------------------------------
# Config (env-driven; mirrors main.py semantics)
# ---------------------------------------------------------------------------
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
).strip()

FASTAPI_TOKEN = os.getenv("FASTAPI_TOKEN", "").strip()
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
REQUIRE_AUTH = bool(FASTAPI_TOKEN or APP_TOKEN)

BASE_HEADERS = {
    "User-Agent": USER_AGENT,
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# URL candidates we try (English first, then Arabic)
ARGAAM_CANDIDATES = [
    "https://www.argaam.com/en/company/{code}",
    "https://www.argaam.com/en/company/financials/{code}",
    "https://www.argaam.com/en/tadawul/company?symbol={code}",
    "https://www.argaam.com/ar/company/{code}",
    "https://www.argaam.com/ar/company/financials/{code}",
    "https://www.argaam.com/ar/tadawul/company?symbol={code}",
]

# ---------------------------------------------------------------------------
# Auth helper (same behavior as main.py)
# ---------------------------------------------------------------------------
def _check_auth(request: Request) -> None:
    if not REQUIRE_AUTH:
        return
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = auth.split(" ", 1)[1].strip()
    allowed = {t for t in (FASTAPI_TOKEN, APP_TOKEN) if t}
    if token not in allowed:
        raise HTTPException(status_code=403, detail="invalid token")

# ---------------------------------------------------------------------------
# In-memory TTL cache: key -> (expires_epoch, data)
# ---------------------------------------------------------------------------
_CACHE: Dict[str, Tuple[float, Any]] = {}


def _cache_get(key: str) -> Optional[Any]:
    ent = _CACHE.get(key)
    if not ent:
        return None
    exp, data = ent
    if exp < time.time():
        _CACHE.pop(key, None)
        return None
    return data


def _cache_put(key: str, data: Any, ttl_sec: int = 600) -> None:
    if ttl_sec <= 0:
        return
    _CACHE[key] = (time.time() + ttl_sec, data)


def _cache_clear() -> None:
    """Clear all cached data."""
    _CACHE.clear()


def _cache_info() -> Dict[str, Any]:
    """Get cache information."""
    now = time.time()
    valid_entries = {k: v for k, v in _CACHE.items() if v[0] > now}
    expired_entries = {k: v for k, v in _CACHE.items() if v[0] <= now}
    
    # Clean up expired entries
    for key in expired_entries:
        _CACHE.pop(key, None)
    
    return {
        "total_entries": len(valid_entries),
        "expired_entries": len(expired_entries),
        "keys": list(valid_entries.keys())
    }

# ---------------------------------------------------------------------------
# Shared async HTTP client (gracefully closed via close_argaam_http_client)
# ---------------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None


async def get_client() -> httpx.AsyncClient:
    """Get or create the HTTP client."""
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers=BASE_HEADERS,
            follow_redirects=True,
        )
    return _CLIENT


async def close_argaam_http_client() -> None:
    """Called from main.py on shutdown to close this module's client."""
    global _CLIENT
    if _CLIENT is not None:
        try:
            await _CLIENT.aclose()
        except Exception as e:
            print(f"Error closing Argaam HTTP client: {e}")
        finally:
            _CLIENT = None

# ---------------------------------------------------------------------------
# Async helpers
# ---------------------------------------------------------------------------
async def _sleep_backoff(i: int) -> None:
    await asyncio.sleep(0.1 * (2 ** i))


async def _fetch_html(url: str, retries: int = 3, timeout: float = 30.0) -> Optional[str]:
    """Fetch HTML content with retries and timeout."""
    client = await get_client()
    
    for i in range(retries + 1):
        try:
            response = await client.get(url, timeout=timeout)
            if response.status_code == 200 and response.text and len(response.text) > 1000:
                return response.text
            elif response.status_code == 404:
                return None  # Company not found
        except httpx.TimeoutException:
            if i == retries:
                return None
        except Exception as e:
            if i == retries:
                print(f"Failed to fetch {url}: {e}")
                return None
        await _sleep_backoff(i)
    return None

# ---------------------------------------------------------------------------
# Helpers: normalization & parsing
# ---------------------------------------------------------------------------
_CODE_RE = re.compile(r"(\d{4})")


def _norm_tasi_code(s: str) -> str:
    """Extract 4-digit TASI code from strings like '1120' or '1120.SR'."""
    if not s:
        return ""
    
    # Remove .SR suffix if present
    s = s.upper().replace('.SR', '')
    
    # Extract 4-digit code
    m = _CODE_RE.search(s)
    return m.group(1) if m else s


def _num_like(s: Optional[str]) -> Optional[float]:
    """
    Parse numbers from messy text (Arabic digits, %, and k/m/b/bn/t suffixes).
    Returns float or None.
    """
    if s is None:
        return None
    if isinstance(s, (int, float)):
        try:
            return float(s)
        except Exception:
            return None

    t = str(s).strip()
    if not t:
        return None

    # Arabic-Indic digits → Latin
    t = t.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))

    # Remove thousands separators and spaces
    t = t.replace(",", "").replace(" ", "").strip()

    # Handle negative numbers
    is_negative = False
    if t.startswith('-'):
        is_negative = True
        t = t[1:]

    # Flexible pattern: number with optional suffix or %
    m = re.search(r"(\d+(?:\.\d+)?)([%kKmMbBtTn]?)$", t)
    if not m:
        try:
            return float(t) * (-1 if is_negative else 1)
        except Exception:
            return None

    core = m.group(1)
    suf = m.group(2).lower()

    try:
        val = float(core)
    except Exception:
        return None

    # Apply suffix multiplier
    multipliers = {
        'k': 1e3, 'm': 1e6, 'b': 1e9, 'bn': 1e9, 't': 1e12, 'n': 1e9
    }
    if suf in multipliers:
        val *= multipliers[suf]
    elif suf == '%':
        val /= 100.0

    return val * (-1 if is_negative else 1)


def _extract_after_label(soup: "BeautifulSoup", labels: List[str]) -> Optional[float]:
    """Find a label (English/Arabic) and return the nearest numeric-like value."""
    if not soup or not HAS_BEAUTIFULSOUP:
        return None
        
    pattern = re.compile("|".join([re.escape(label) for label in labels]), flags=re.IGNORECASE)

    for element in soup.find_all(string=pattern):
        try:
            # Look in the parent container
            parent = element.parent
            if parent:
                # Get all text from parent and siblings
                container = parent.parent if parent.parent else parent
                text_content = container.get_text(" ", strip=True)
                
                # Find all number-like patterns in the text
                numbers = re.findall(r"-?\d+[,٠-٩]*(?:\.\d+)?\s*[%kKmMbBtT]?", text_content)
                for num_str in numbers:
                    value = _num_like(num_str)
                    if value is not None:
                        return value
                        
        except Exception:
            continue
            
    return None


def _parse_argaam_snapshot(html: str) -> Dict[str, Any]:
    """Parse Argaam HTML to extract company data."""
    if not HAS_BEAUTIFULSOUP or not html:
        return {}
        
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return {}

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
    }

    # Extract company name
    try:
        h1 = soup.find("h1")
        if h1:
            result["name"] = h1.get_text(" ", strip=True)
    except Exception:
        pass

    # Define labels for data extraction (English + Arabic)
    labels_config = {
        "marketCap": ["Market Cap", "القيمة السوقية"],
        "sharesOutstanding": ["Shares Outstanding", "الأسهم القائمة", "الاسهم القائمة"],
        "eps": ["EPS", "ربحية السهم", "Earnings Per Share"],
        "pe": ["P/E", "مكرر الربحية", "Price to Earnings"],
        "beta": ["Beta", "بيتا"],
        "dividendYield": ["Dividend Yield", "عائد التوزيع", "Dividend"],
        "volume": ["Volume", "الكمية", "حجم التداول"],
        "dayHigh": ["High", "أعلى", "Today High"],
        "dayLow": ["Low", "أدنى", "Today Low"],
        "price": ["Last", "السعر", "Last Trade", "آخر صفقة", "آخر سعر", "Price"],
    }

    # Extract values for each field
    for field, field_labels in labels_config.items():
        result[field] = _extract_after_label(soup, field_labels)

    # Additional fallback for price
    if result.get("price") is None:
        # Try data attributes
        price_element = soup.select_one("[data-last], [data-last-price], [data-price]")
        if price_element:
            for attr in ["data-last", "data-last-price", "data-price"]:
                price_value = price_element.get(attr)
                if price_value:
                    parsed_price = _num_like(price_value)
                    if parsed_price is not None:
                        result["price"] = parsed_price
                        break

    return result


async def _guess_argaam_url(code: str) -> Optional[str]:
    """Try multiple URL patterns until one returns plausible HTML."""
    for template in ARGAAM_CANDIDATES:
        url = template.format(code=quote(code))
        html = await _fetch_html(url, retries=1, timeout=15.0)
        if html and len(html) > 2000:
            return url
    return None


async def _fetch_company(
    code: str, url_override: Optional[str] = None, ttl: int = 600
) -> Dict[str, Any]:
    """Fetch and parse a single company's snapshot from Argaam (with caching)."""
    cache_key = f"argaam::{code}"
    cached_data = _cache_get(cache_key)
    if cached_data is not None:
        return cached_data

    # Try to find the right URL
    url = url_override or (await _guess_argaam_url(code))
    if not url:
        _cache_put(cache_key, {}, ttl_sec=300)  # Cache failure for 5 minutes
        return {}

    # Fetch HTML
    html = await _fetch_html(url)
    if not html:
        _cache_put(cache_key, {}, ttl_sec=300)  # Cache failure for 5 minutes
        return {}

    # Parse data
    data = _parse_argaam_snapshot(html)
    _cache_put(cache_key, data, ttl_sec=ttl)
    
    return data

# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class ArgaamReq(BaseModel):
    tickers: List[str]
    urls: Optional[Dict[str, str]] = None  # optional per-ticker override URL map
    cache_ttl: int = 600                   # seconds


class ArgaamQuoteResponse(BaseModel):
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    price: Optional[float] = None
    dayHigh: Optional[float] = None
    dayLow: Optional[float] = None
    volume: Optional[float] = None
    marketCap: Optional[float] = None
    sharesOutstanding: Optional[float] = None
    dividendYield: Optional[float] = None
    eps: Optional[float] = None
    pe: Optional[float] = None
    beta: Optional[float] = None

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.get("/health")
async def argaam_health(request: Request):
    """Health check for Argaam integration."""
    _check_auth(request)
    
    return {
        "status": "healthy",
        "beautifulsoup_available": HAS_BEAUTIFULSOUP,
        "http_client_ready": _CLIENT is not None,
        "cache_info": _cache_info(),
        "timestamp": time.time()
    }


@router.post("/quotes")
async def argaam_quotes(req: ArgaamReq, request: Request):
    """
    POST: /v41/argaam/quotes
    Body: { "tickers": ["1120","2010"], "urls": {"1120":"..."}, "cache_ttl": 600 }
    """
    _check_auth(request)

    if not req.tickers:
        raise HTTPException(status_code=400, detail="tickers array is required")

    if not HAS_BEAUTIFULSOUP:
        raise HTTPException(
            status_code=503,
            detail="Argaam parser not available. Install 'beautifulsoup4' in your environment.",
        )

    # Normalize ticker codes
    normalized_tickers = [_norm_tasi_code(ticker) for ticker in req.tickers]
    
    # Prepare results
    results: Dict[str, Any] = {}
    to_fetch: List[Tuple[str, Optional[str]]] = []

    # Check cache first
    for ticker, normalized in zip(req.tickers, normalized_tickers):
        cache_key = f"argaam::{normalized}"
        cached = _cache_get(cache_key)
        
        if cached is not None:
            results[normalized] = ArgaamQuoteResponse(**cached).dict()
        else:
            url_override = (req.urls or {}).get(ticker) or (req.urls or {}).get(normalized)
            to_fetch.append((normalized, url_override))

    # Fetch missing data concurrently
    if to_fetch:
        tasks = [
            _fetch_company(code, url_override=url, ttl=req.cache_ttl) 
            for code, url in to_fetch
        ]
        fetched_data = await asyncio.gather(*tasks, return_exceptions=True)
        
        for (code, _), data in zip(to_fetch, fetched_data):
            if isinstance(data, Exception):
                results[code] = {"error": f"Fetch error: {str(data)}"}
            else:
                results[code] = ArgaamQuoteResponse(**data).dict()

    return {
        "data": results,
        "metadata": {
            "total_requested": len(req.tickers),
            "returned": len(results),
            "from_cache": len(results) - len(to_fetch),
            "newly_fetched": len(to_fetch),
            "cache_ttl": req.cache_ttl
        }
    }


@router.get("/quotes")
async def argaam_quotes_get(
    request: Request,
    tickers: str,
    cache_ttl: int = 600,
):
    """
    GET wrapper:
      /v41/argaam/quotes?tickers=1120,2010&cache_ttl=600
    """
    _check_auth(request)

    if not tickers:
        raise HTTPException(status_code=400, detail="tickers query param is required")

    tickers_list = [t.strip() for t in tickers.split(",") if t.strip()]
    payload = ArgaamReq(tickers=tickers_list, urls=None, cache_ttl=cache_ttl)
    return await argaam_quotes(payload, request)


@router.get("/cache/info")
async def argaam_cache_info(request: Request):
    """Get cache information."""
    _check_auth(request)
    return _cache_info()


@router.delete("/cache/clear")
async def argaam_cache_clear(request: Request):
    """Clear the Argaam cache."""
    _check_auth(request)
    cache_info_before = _cache_info()
    _cache_clear()
    return {
        "cleared": True,
        "cleared_entries": cache_info_before["total_entries"],
        "previous_cache_info": cache_info_before
    }


@router.get("/test/{ticker}")
async def argaam_test_ticker(ticker: str, request: Request):
    """Test endpoint for a specific ticker."""
    _check_auth(request)
    
    normalized = _norm_tasi_code(ticker)
    data = await _fetch_company(normalized)
    
    return {
        "ticker": ticker,
        "normalized": normalized,
        "data": data,
        "beautifulsoup_available": HAS_BEAUTIFULSOUP
    }
