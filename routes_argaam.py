# =============================================================================
# Argaam Integration Routes - Ultimate Investment Dashboard (v2.2.0)
# =============================================================================
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

# -----------------------------------------------------------------------------
# Optional Parser (BeautifulSoup)
# -----------------------------------------------------------------------------
try:
    from bs4 import BeautifulSoup  # type: ignore
    HAS_BS = True
except Exception:
    BeautifulSoup = None  # type: ignore
    HAS_BS = False

# -----------------------------------------------------------------------------
# Router
# -----------------------------------------------------------------------------
router = APIRouter(prefix="/v1/argaam", tags=["Argaam"])

# -----------------------------------------------------------------------------
# Environment / Config
# -----------------------------------------------------------------------------
USER_AGENT = os.getenv(
    "ARGAAM_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
).strip()

APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
BACKUP_APP_TOKEN = os.getenv("BACKUP_APP_TOKEN", "").strip()
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "true").lower() == "true"

ARGAAM_BASE_URL = os.getenv("ARGAAM_BASE_URL", "https://www.argaam.com")
ARGAAM_TIMEOUT = int(os.getenv("ARGAAM_TIMEOUT", "30"))
ARGAAM_MAX_RETRIES = int(os.getenv("ARGAAM_MAX_RETRIES", "3"))
ARGAAM_CACHE_TTL = int(os.getenv("ARGAAM_CACHE_TTL", "600"))

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# -----------------------------------------------------------------------------
# Authentication
# -----------------------------------------------------------------------------
def _check_auth(request: Request) -> None:
    """Validate Bearer Token using APP_TOKEN / BACKUP_APP_TOKEN."""
    if not REQUIRE_AUTH:
        return

    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth.split(" ", 1)[1].strip()
    allowed = {t for t in (APP_TOKEN, BACKUP_APP_TOKEN) if t}

    if token not in allowed:
        raise HTTPException(status_code=403, detail="Invalid or expired token")

# -----------------------------------------------------------------------------
# In-Memory TTL Cache
# -----------------------------------------------------------------------------
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

def _cache_put(key: str, data: Any, ttl: int) -> None:
    _CACHE[key] = (time.time() + ttl, data)

def _cache_info() -> Dict[str, Any]:
    """Return cache summary and auto-clean expired entries."""
    now = time.time()
    keys = list(_CACHE.keys())
    expired = 0
    for k in keys:
        if _CACHE[k][0] < now:
            _CACHE.pop(k, None)
            expired += 1
    return {
        "active": len(_CACHE),
        "expired_removed": expired,
        "keys": list(_CACHE.keys()),
    }

def _cache_clear():
    _CACHE.clear()

# -----------------------------------------------------------------------------
# Shared Async HTTP Client
# -----------------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None

async def get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.AsyncClient(
            headers=HEADERS,
            timeout=ARGAAM_TIMEOUT,
            follow_redirects=True,
        )
    return _CLIENT

async def close_argaam_http_client():
    global _CLIENT
    if _CLIENT:
        try:
            await _CLIENT.aclose()
        except Exception:
            pass
        _CLIENT = None

# -----------------------------------------------------------------------------
# Helper: Normalize TASI Ticker (1120.SR → 1120)
# -----------------------------------------------------------------------------
_CODE_RE = re.compile(r"(\d{4})")

def _norm_tasi_code(code: str) -> str:
    if not code:
        return ""
    c = code.upper().replace(".SR", "")
    m = _CODE_RE.search(c)
    return m.group(1) if m else c

# -----------------------------------------------------------------------------
# Fetcher
# -----------------------------------------------------------------------------
async def _fetch_html(url: str) -> Optional[str]:
    client = await get_client()
    for attempt in range(ARGAAM_MAX_RETRIES):
        try:
            r = await client.get(url)
            if r.status_code == 200 and len(r.text) > 1000:
                return r.text
            if r.status_code == 404:
                return None
        except Exception:
            if attempt == ARGAAM_MAX_RETRIES - 1:
                return None
        await asyncio.sleep(0.15 * (attempt + 1))
    return None

# -----------------------------------------------------------------------------
# Parsing Helpers
# -----------------------------------------------------------------------------
def _num_like(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None

    t = t.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))
    t = t.replace(",", "").replace(" ", "").strip()

    mul = {
        "k": 1e3, "m": 1e6, "b": 1e9, "t": 1e12,
        "%": 0.01,
    }

    m = re.match(r"(-?\d+(\.\d+)?)([kKmMbBtT%]?)", t)
    if not m:
        return None

    val = float(m.group(1))
    suf = m.group(3).lower()
    if suf in mul:
        val *= mul[suf]
    return val

def _extract_label(soup, labels: List[str]) -> Optional[float]:
    if not HAS_BS:
        return None
    patt = re.compile("|".join(re.escape(l) for l in labels), re.IGNORECASE)
    for el in soup.find_all(string=patt):
        try:
            container = el.parent.parent if el.parent else el
            txt = container.get_text(" ", strip=True)
            nums = re.findall(r"-?\d+[,٠-٩]*(?:\.\d+)?\s*[%kKmMbBtT]?", txt)
            for n in nums:
                val = _num_like(n)
                if val is not None:
                    return val
        except Exception:
            continue
    return None

def _parse_snapshot(html: str) -> Dict[str, Any]:
    if not HAS_BS or not html:
        return {}
    soup = BeautifulSoup(html, "html.parser")

    result = {
        "name": None, "sector": None, "industry": None,
        "price": None, "dayHigh": None, "dayLow": None,
        "volume": None, "marketCap": None,
        "sharesOutstanding": None, "dividendYield": None,
        "eps": None, "pe": None, "beta": None,
    }

    h1 = soup.find("h1")
    if h1:
        result["name"] = h1.get_text(" ", strip=True)

    fields = {
        "marketCap": ["Market Cap", "القيمة السوقية"],
        "sharesOutstanding": ["Shares Outstanding", "الأسهم القائمة"],
        "eps": ["EPS", "ربحية السهم"],
        "pe": ["P/E", "مكرر الربحية"],
        "beta": ["Beta", "بيتا"],
        "dividendYield": ["Dividend", "عائد"],
        "volume": ["Volume", "الكمية"],
        "dayHigh": ["High", "أعلى"],
        "dayLow": ["Low", "أدنى"],
        "price": ["Last", "السعر", "آخر"],
    }

    for key, labels in fields.items():
        result[key] = _extract_label(soup, labels)

    return result

# -----------------------------------------------------------------------------
# URL handling
# -----------------------------------------------------------------------------
CANDIDATES = [
    "{base}/en/company/{c}",
    "{base}/en/company/financials/{c}",
    "{base}/en/tadawul/company?symbol={c}",
    "{base}/ar/company/{c}",
    "{base}/ar/company/financials/{c}",
    "{base}/ar/tadawul/company?symbol={c}",
]

async def _guess_url(code: str) -> Optional[str]:
    for template in CANDIDATES:
        url = template.format(base=ARGAAM_BASE_URL, c=quote(code))
        html = await _fetch_html(url)
        if html and len(html) > 2000:
            return url
    return None

async def _fetch_company(code: str, ttl: int) -> Dict[str, Any]:
    key = f"argaam::{code}"
    cached = _cache_get(key)
    if cached:
        return cached

    url = await _guess_url(code)
    if not url:
        _cache_put(key, {}, 300)
        return {}

    html = await _fetch_html(url)
    if not html:
        _cache_put(key, {}, 300)
        return {}

    data = _parse_snapshot(html)
    _cache_put(key, data, ttl)
    return data

# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------
class ArgaamReq(BaseModel):
    tickers: List[str]
    cache_ttl: int = ARGAAM_CACHE_TTL

class ArgaamQuoteResponse(BaseModel):
    name: Optional[str]
    sector: Optional[str]
    industry: Optional[str]
    price: Optional[float]
    dayHigh: Optional[float]
    dayLow: Optional[float]
    volume: Optional[float]
    marketCap: Optional[float]
    sharesOutstanding: Optional[float]
    dividendYield: Optional[float]
    eps: Optional[float]
    pe: Optional[float]
    beta: Optional[float]

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@router.get("/health")
async def health(request: Request):
    _check_auth(request)
    return {
        "status": "healthy",
        "bs4_available": HAS_BS,
        "cache": _cache_info(),
        "timestamp": time.time(),
    }

@router.post("/quotes")
async def quotes(req: ArgaamReq, request: Request):
    _check_auth(request)

    if not HAS_BS:
        raise HTTPException(
            status_code=503,
            detail="BeautifulSoup not available. Install 'beautifulsoup4'.",
        )

    tickers = [_norm_tasi_code(t) for t in req.tickers]
    results: Dict[str, Any] = {}

    tasks = [ _fetch_company(code, ttl=req.cache_ttl) for code in tickers ]
    fetched = await asyncio.gather(*tasks)

    for code, data in zip(tickers, fetched):
        results[code] = ArgaamQuoteResponse(**data).dict()

    return {
        "data": results,
        "metadata": {
            "requested": len(tickers),
            "cache_ttl": req.cache_ttl,
            "cache": _cache_info(),
        },
    }

@router.get("/quotes")
async def quotes_get(tickers: str, cache_ttl: int = ARGAAM_CACHE_TTL, request: Request = None):
    _check_auth(request)
    arr = [t.strip() for t in tickers.split(",") if t.strip()]
    req = ArgaamReq(tickers=arr, cache_ttl=cache_ttl)
    return await quotes(req, request)

@router.get("/cache/info")
async def cache_info(request: Request):
    _check_auth(request)
    return _cache_info()

@router.delete("/cache/clear")
async def cache_clear(request: Request):
    _check_auth(request)
    before = _cache_info()
    _cache_clear()
    return {"cleared": True, "previous": before}

@router.get("/test/{ticker}")
async def test_ticker(ticker: str, request: Request):
    _check_auth(request)
    code = _norm_tasi_code(ticker)
    data = await _fetch_company(code, ttl=ARGAAM_CACHE_TTL)
    return {
        "ticker": ticker,
        "normalized": code,
        "data": data,
        "bs4_available": HAS_BS,
    }
