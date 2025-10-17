# routes_argaam.py
from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

# BeautifulSoup is optional; endpoints will respond with a clear error if missing
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore

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

# ---------------------------------------------------------------------------
# Shared async HTTP client (gracefully closed via close_argaam_http_client)
# ---------------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None


def _client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
            headers=BASE_HEADERS,
        )
    return _CLIENT


async def close_argaam_http_client() -> None:
    """Called from main.py on shutdown to close this module's client."""
    global _CLIENT
    if _CLIENT is not None:
        try:
            await _CLIENT.aclose()
        finally:
            _CLIENT = None

# ---------------------------------------------------------------------------
# Async helpers
# ---------------------------------------------------------------------------
async def _sleep_backoff(i: int) -> None:
    import asyncio
    await asyncio.sleep(0.12 * (2 ** i))


async def _fetch_html(url: str, retries: int = 2, timeout: float = 15.0) -> Optional[str]:
    for i in range(retries + 1):
        try:
            r = await _client().get(url, timeout=timeout)
            if r.status_code == 200 and r.text and len(r.text) > 1000:
                return r.text
        except Exception:
            pass
        await _sleep_backoff(i)
    return None

# ---------------------------------------------------------------------------
# Helpers: normalization & parsing
# ---------------------------------------------------------------------------
_CODE_RE = re.compile(r"(\d{4})")


def _norm_tasi_code(s: str) -> str:
    """Extract 4-digit TASI code from strings like '1120' or '1120.SR'."""
    m = _CODE_RE.search(s or "")
    return m.group(1) if m else (s or "").upper()


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

    # Remove thousands separators (commas to spaces for flexible matching)
    t = t.replace(",", " ").strip()

    # Flexible pattern: number + optional suffix or %
    m = re.search(r"(-?\d+(?:\s?\d{3})*(?:\.\d+)?)(\s*[%kKmMbBtT]n?)?", t)
    if not m:
        try:
            return float(t.replace(" ", ""))
        except Exception:
            return None

    core = m.group(1).replace(" ", "")
    suf = (m.group(2) or "").strip().lower()

    try:
        val = float(core)
    except Exception:
        return None

    if suf == "%":
        return val / 100.0

    mult = {"k": 1e3, "m": 1e6, "b": 1e9, "bn": 1e9, "t": 1e12}
    if suf in mult:
        val *= mult[suf]
    return val


def _extract_after_label(soup: "BeautifulSoup", labels: List[str]) -> Optional[float]:
    """Find a label (English/Arabic) and return the nearest numeric-like value."""
    if not soup:
        return None
    pat = re.compile("|".join([re.escape(x) for x in labels]), flags=re.I)

    for node in soup.find_all(string=pat):
        try:
            parent = node.parent
            if parent:
                # Forward siblings
                for s in parent.next_siblings:
                    if hasattr(s, "get_text"):
                        v = _num_like(s.get_text(" ", strip=True))
                        if v is not None:
                            return v
                # Backward siblings
                for s in parent.previous_siblings:
                    if hasattr(s, "get_text"):
                        v = _num_like(s.get_text(" ", strip=True))
                        if v is not None:
                            return v

            # Scan the whole row (e.g., table rows)
            row = parent.parent if parent else None
            if row:
                txt = row.get_text(" ", strip=True)
                nums = re.findall(r"[-\d٠-٩\.,%]+", txt)
                for cand in reversed(nums):
                    v = _num_like(cand)
                    if v is not None:
                        return v
        except Exception:
            continue
    return None


def _parse_argaam_snapshot(html: str) -> Dict[str, Any]:
    if BeautifulSoup is None:
        return {}
    soup = BeautifulSoup(html, "html.parser")

    # Company name (best effort)
    name = None
    try:
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(" ", strip=True)
    except Exception:
        pass

    # Labels we try to read (English + Arabic)
    labels = {
        "marketCap": ["Market Cap", "القيمة السوقية"],
        "sharesOutstanding": ["Shares Outstanding", "الأسهم القائمة", "الاسهم القائمة"],
        "eps": ["EPS (TTM)", "ربحية السهم (TTM)", "ربحية السهم"],
        "pe": ["P/E (TTM)", "مكرر الربحية"],
        "beta": ["Beta", "بيتا"],
        "dividendYield": ["Dividend Yield", "عائد التوزيع"],
        "volume": ["Volume", "الكمية", "حجم التداول"],
        "dayHigh": ["High", "أعلى"],
        "dayLow": ["Low", "أدنى"],
        "price": ["Last", "السعر", "Last Trade", "آخر صفقة", "آخر سعر"],
    }

    out: Dict[str, Any] = {"name": name, "sector": None, "industry": None}
    for k, labs in labels.items():
        out[k] = _extract_after_label(soup, labs)

    # Fallback: some pages expose live price in data attributes
    if out.get("price") is None:
        live = soup.select_one("[data-last], [data-last-price]")
        if live:
            cand = live.get("data-last") or live.get("data-last-price")
            out["price"] = _num_like(cand)

    return out


async def _guess_argaam_url(code: str) -> Optional[str]:
    """Try multiple URL patterns until one returns plausible HTML."""
    from urllib.parse import quote
    for tpl in ARGAAM_CANDIDATES:
        url = tpl.format(code=quote(code))
        html = await _fetch_html(url, retries=2, timeout=15.0)
        if html and len(html) > 2000:
            return url
    return None


async def _fetch_company(
    code: str, url_override: Optional[str] = None, ttl: int = 600
) -> Dict[str, Any]:
    """Fetch and parse a single company's snapshot from Argaam (with caching)."""
    key = f"argaam::{code}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    url = url_override or (await _guess_argaam_url(code))
    if not url:
        _cache_put(key, {}, ttl_sec=120)
        return {}

    html = await _fetch_html(url, retries=2, timeout=15.0)
    if not html:
        _cache_put(key, {}, ttl_sec=120)
        return {}

    data = _parse_argaam_snapshot(html) if html else {}
    _cache_put(key, data, ttl_sec=ttl)
    return data

# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class ArgaamReq(BaseModel):
    tickers: List[str]
    urls: Optional[Dict[str, str]] = None  # optional per-ticker override URL map
    cache_ttl: int = 600                   # seconds

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.post("/quotes")
async def argaam_quotes(req: ArgaamReq, request: Request):
    """
    POST: /v41/argaam/quotes
    Body: { "tickers": ["1120","2010"], "urls": {"1120":"..."}, "cache_ttl": 600 }
    """
    _check_auth(request)

    if not req.tickers:
        raise HTTPException(status_code=400, detail="tickers array is required")

    if BeautifulSoup is None:
        raise HTTPException(
            status_code=503,
            detail="Argaam parser not available. Install 'beautifulsoup4' in your environment.",
        )

    out: Dict[str, Any] = {}

    # Serve cached first; fetch the rest concurrently
    to_fetch: List[tuple[str, Optional[str]]] = []
    for raw in req.tickers:
        code = _norm_tasi_code(raw or "")
        cached = _cache_get(f"argaam::{code}")
        if cached is not None:
            out[code] = {
                "name": cached.get("name"),
                "sector": cached.get("sector"),
                "industry": cached.get("industry"),
                "price": cached.get("price"),
                "dayHigh": cached.get("dayHigh"),
                "dayLow": cached.get("dayLow"),
                "volume": cached.get("volume"),
                "marketCap": cached.get("marketCap"),
                "sharesOutstanding": cached.get("sharesOutstanding"),
                "dividendYield": cached.get("dividendYield"),
                "eps": cached.get("eps"),
                "pe": cached.get("pe"),
                "beta": cached.get("beta"),
            }
        else:
            to_fetch.append((code, (req.urls or {}).get(code)))

    if to_fetch:
        import asyncio
        tasks = [
            _fetch_company(code, url_override=url, ttl=req.cache_ttl) for code, url in to_fetch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        for (code, _), data in zip(to_fetch, results):
            out[code] = {
                "name": data.get("name"),
                "sector": data.get("sector"),
                "industry": data.get("industry"),
                "price": data.get("price"),
                "dayHigh": data.get("dayHigh"),
                "dayLow": data.get("dayLow"),
                "volume": data.get("volume"),
                "marketCap": data.get("marketCap"),
                "sharesOutstanding": data.get("sharesOutstanding"),
                "dividendYield": data.get("dividendYield"),
                "eps": data.get("eps"),
                "pe": data.get("pe"),
                "beta": data.get("beta"),
            }

    return {"data": out}


@router.get("/quotes")
async def argaam_quotes_get(
    request: Request,
    tickers: Optional[str] = None,  # comma-separated, e.g., "1120,2010"
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
