# routes_argaam.py
from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# BeautifulSoup is optional; if it's missing, the endpoint will return a clear error
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # type: ignore

# ---------------------------------------------------------------------------
# Router (v41 path so it won't clash with legacy v33 route in main.py)
# ---------------------------------------------------------------------------
router = APIRouter(prefix="/v41/argaam", tags=["argaam"])

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

# ---------------------------------------------------------------------------
# Simple in-memory TTL cache: key -> (expires_epoch, data)
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
# Shared async HTTP client (closed by close_argaam_http_client)
# ---------------------------------------------------------------------------
_CLIENT: Optional[httpx.AsyncClient] = None


def _client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.AsyncClient(timeout=httpx.Timeout(15.0), headers=HEADERS)
    return _CLIENT


async def close_argaam_http_client() -> None:
    """Allow main.py to gracefully close this module's HTTP client."""
    global _CLIENT
    if _CLIENT is not None:
        await _CLIENT.aclose()
        _CLIENT = None


# ---------------------------------------------------------------------------
# Helpers: normalization & parsing
# ---------------------------------------------------------------------------
_CODE_RE = re.compile(r"(\d{4})")


def _norm_tasi_code(s: str) -> str:
    """Extract 4-digit TASI code from strings like '1120' or '1120.SR'."""
    m = _CODE_RE.search(s or "")
    return m.group(1) if m else (s or "").upper()


def _num_like(s: Optional[str]) -> Optional[float]:
    """Parse numbers from messy text (handles Arabic digits, %, and k/m/b/bn/t)."""
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

    # Remove thousands separators (commas become spaces for flexible matching)
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
    """Find a label (English or Arabic) and return the nearest numeric-like value."""
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

    # Labels we try to read (English + Arabic). Arabic strings are site labels.
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
    """Try a few URL patterns until one returns a plausible HTML page."""
    candidates = [
        f"https://www.argaam.com/en/company/{code}",
        f"https://www.argaam.com/en/company/financials/{code}",
        f"https://www.argaam.com/en/tadawul/company?symbol={code}",
    ]
    client = _client()
    for url in candidates:
        try:
            r = await client.get(url)
            if r.status_code == 200 and r.text and len(r.text) > 2000:
                return url
        except Exception:
            pass
    return None


async def _fetch_company(code: str, url_override: Optional[str] = None, ttl: int = 600) -> Dict[str, Any]:
    """Fetch and parse a single company's snapshot from Argaam (with caching)."""
    key = f"argaam::{code}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    url = url_override or (await _guess_argaam_url(code))
    if not url:
        _cache_put(key, {}, ttl_sec=120)
        return {}

    try:
        r = await _client().get(url)
        if r.status_code != 200 or not r.text:
            _cache_put(key, {}, ttl_sec=120)
            return {}
        data = _parse_argaam_snapshot(r.text)
        _cache_put(key, data, ttl_sec=ttl)
        return data
    except Exception:
        _cache_put(key, {}, ttl_sec=120)
        return {}


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
async def argaam_quotes(req: ArgaamReq):
    if not req.tickers:
        raise HTTPException(status_code=400, detail="tickers array is required")

    if BeautifulSoup is None:
        raise HTTPException(
            status_code=503,
            detail="Argaam parser not available. Install 'beautifulsoup4' in your environment.",
        )

    out: Dict[str, Any] = {}
    for raw in req.tickers:
        code = _norm_tasi_code(raw or "")
        url = (req.urls or {}).get(code)
        info = await _fetch_company(code, url_override=url, ttl=req.cache_ttl)

        # Normalize output field names
        out[code] = {
            "name": info.get("name"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "price": info.get("price"),
            "dayHigh": info.get("dayHigh"),
            "dayLow": info.get("dayLow"),
            "volume": info.get("volume"),
            "marketCap": info.get("marketCap"),
            "sharesOutstanding": info.get("sharesOutstanding"),
            "dividendYield": info.get("dividendYield"),
            "eps": info.get("eps"),
            "pe": info.get("pe"),
            "beta": info.get("beta"),
        }

    return {"data": out}
