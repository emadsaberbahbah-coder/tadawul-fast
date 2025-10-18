# routes_argaam.py
from __future__ import annotations

import json
import os
import re
import time
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from cachetools import TTLCache
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import httpx

# ---------------------------------------------------------------------
# Router (support both v41 and legacy v33 via optional mount in main.py)
# ---------------------------------------------------------------------
router = APIRouter(prefix="/v41/argaam", tags=["argaam"])

# ---------------------------------------------------------------------
# Config / HTTP client
# ---------------------------------------------------------------------
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8,ar;q=0.7",
}

HTTP_TIMEOUT = float(os.getenv("ARGAAM_HTTP_TIMEOUT", "15"))
RETRY_ATTEMPTS = int(os.getenv("ARGAAM_RETRY_ATTEMPTS", "2"))
CACHE_TTL_SEC = int(os.getenv("ARGAAM_CACHE_TTL", "600"))
CACHE_NEG_TTL_SEC = int(os.getenv("ARGAAM_NEG_TTL", "120"))
CACHE_SIZE = int(os.getenv("ARGAAM_CACHE_SIZE", "2048"))

# Single shared sync client (httpx is happy in uvicorn workers)
_client = httpx.Client(follow_redirects=True, headers=COMMON_HEADERS, timeout=HTTP_TIMEOUT)

# Positive & negative caches (separate TTLs)
_cache_ok: TTLCache[str, dict] = TTLCache(maxsize=CACHE_SIZE, ttl=CACHE_TTL_SEC)
_cache_neg: TTLCache[str, dict] = TTLCache(maxsize=CACHE_SIZE // 4, ttl=CACHE_NEG_TTL_SEC)

# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------
class ArgaamReq(BaseModel):
    tickers: List[str] = Field(..., description="List of Tadawul tickers, e.g., '1120' or '1120.SR'")
    urls: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional map of { '1120': 'https://...' } to override the detected company page per ticker",
    )

# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------
_AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_SUFFIX_MULT = {"k": 1e3, "m": 1e6, "b": 1e9, "bn": 1e9, "t": 1e12}


def _norm_ticker(raw: str) -> str:
    """1120.SR -> 1120 ; keep 4 digits if present, else uppercase raw."""
    if not raw:
        return ""
    m = re.search(r"(\d{4})", str(raw))
    return m.group(1) if m else str(raw).strip().upper()


def _num(text) -> Optional[float]:
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return float(text)

    s = str(text).strip().replace(",", "")
    s = s.translate(_AR_DIGITS)

    # percentage
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return None

    # value with suffix (k/m/b/bn/t)
    m = re.match(r"^(-?\d+(?:\.\d+)?)([kmbt]|bn)?$", s, flags=re.I)
    if m:
        val = float(m.group(1))
        suf = (m.group(2) or "").lower()
        return val * _SUFFIX_MULT.get(suf, 1.0)

    try:
        return float(s)
    except Exception:
        return None


def _extract_text_after_label(soup: BeautifulSoup, labels: List[str]) -> Optional[str]:
    """
    Find a label (English/Arabic) and return the first numeric-ish token to the right
    within the same container row/card.
    """
    joined = "|".join([re.escape(lbl) for lbl in labels])
    for el in soup.find_all(string=re.compile(joined, flags=re.I)):
        try:
            parent = el.parent or el.find_parent()
            if not parent:
                continue
            # Probe siblings and nearby elements
            check_nodes = list(parent.next_siblings)
            # Also try parent's parent siblings (some pages nest label/value in spans)
            if parent.parent:
                check_nodes.extend(list(parent.parent.next_siblings))

            for node in check_nodes:
                if hasattr(node, "get_text"):
                    txt = node.get_text(" ", strip=True)
                    mt = re.search(r"[-\d٠-٩\.,%]+(?:\.\d+)?%?", txt)
                    if mt:
                        return mt.group(0)
        except Exception:
            continue
    return None


_LABELS = {
    "marketCap": ["Market Cap", "القيمة السوقية"],
    "sharesOutstanding": ["Shares Outstanding", "الأسهم القائمة"],
    "eps": ["EPS (TTM)", "ربحية السهم (TTM)", "ربحية السهم"],
    "pe": ["P/E (TTM)", "مكرر الربحية"],
    "beta": ["Beta", "بيتا"],
    "dividendYield": ["Dividend Yield", "عائد التوزيع"],
    "volume": ["Volume", "الكمية", "حجم التداول"],
    "dayHigh": ["High", "أعلى"],
    "dayLow": ["Low", "أدنى"],
    "price": ["Last", "السعر", "Last Trade", "آخر صفقة"],
}


def _parse_company_snapshot(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # Name: try H1, or og:title
    name = None
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(" ", strip=True)
    if not name:
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            name = og["content"].strip()

    out = {
        "name": name,
        "sector": None,   # Not always present on these pages; left for future enhancement
        "industry": None, # "
    }

    # scrape by labels
    for key, labs in _LABELS.items():
        vtxt = _extract_text_after_label(soup, labs)
        if vtxt is not None:
            out[key] = _num(vtxt)

    # Fallback for live price in data attributes
    if out.get("price") is None:
        live = soup.select_one("[data-last], [data-last-price]")
        if live:
            cand = live.get("data-last") or live.get("data-last-price")
            out["price"] = _num(cand)

    return out


def _candidate_urls(tasi_code: str) -> List[str]:
    # Try English first then Arabic paths
    return [
        f"https://www.argaam.com/en/company/{tasi_code}",
        f"https://www.argaam.com/en/company/financials/{tasi_code}",
        f"https://www.argaam.com/en/tadawul/company?symbol={tasi_code}",
        f"https://www.argaam.com/ar/company/{tasi_code}",
        f"https://www.argaam.com/ar/company/financials/{tasi_code}",
        f"https://www.argaam.com/ar/tadawul/company?symbol={tasi_code}",
    ]


def _guess_company_url(tasi_code: str) -> Optional[str]:
    for url in _candidate_urls(tasi_code):
        try:
            r = _client.get(url)
            if r.status_code == 200 and len(r.text) > 2000:
                return url
        except Exception:
            continue
    return None


def _cache_get(code: str) -> Optional[dict]:
    if code in _cache_ok:
        return _cache_ok[code]
    if code in _cache_neg:
        return {}  # negative cached
    return None


def _cache_put_ok(code: str, data: dict) -> None:
    _cache_ok[code] = data


def _cache_put_neg(code: str) -> None:
    _cache_neg[code] = {}


def _fetch_company(code: str, url_override: Optional[str] = None) -> dict:
    """
    Fetch and parse a single Argaam company page.
    Uses positive & negative caches with distinct TTLs.
    """
    cached = _cache_get(code)
    if cached is not None:
        return cached

    url = url_override or _guess_company_url(code)
    if not url:
        _cache_put_neg(code)
        return {}

    # Minimal retry loop (httpx already follows redirects)
    last_exc: Optional[Exception] = None
    for _ in range(max(1, RETRY_ATTEMPTS)):
        try:
            res = _client.get(url)
            if res.status_code != 200 or len(res.text) < 500:
                last_exc = HTTPException(status_code=502, detail=f"argaam {code} bad response")
                continue
            data = _parse_company_snapshot(res.text)
            _cache_put_ok(code, data)
            return data
        except Exception as e:
            last_exc = e
            continue

    _cache_put_neg(code)
    return {}  # do not raise; keep API stable


def _normalize_output(code: str, info: dict) -> dict:
    # Normalize to stable field names used by Apps Script
    return {
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

# ---------------------------------------------------------------------
# POST (primary) + GET (fallback) endpoints
# ---------------------------------------------------------------------
@router.post("/quotes")
def argaam_quotes_post(req: ArgaamReq):
    if not req.tickers:
        raise HTTPException(status_code=400, detail="tickers array required")

    out: Dict[str, dict] = {}
    for raw in req.tickers:
        code = _norm_ticker(raw)
        if not code:
            continue
        override = (req.urls or {}).get(code)
        info = _fetch_company(code, url_override=override)
        out[code] = _normalize_output(code, info)

    return {"ok": True, "data": out}


@router.get("/quotes")
def argaam_quotes_get(
    tickers: str = Query(..., description="Comma-separated tickers, e.g. 1120,2222,7010.SR"),
    urls: Optional[str] = Query(
        default=None,
        description='Optional JSON map: {"1120":"https://..."}',
    ),
):
    """
    GET variant for Apps Script GET fallback.
    Example:
      /v41/argaam/quotes?tickers=1120,2222&urls={"1120":"https://..."}
    """
    try:
        tick_list = [t.strip() for t in tickers.split(",") if t.strip()]
    except Exception:
        raise HTTPException(status_code=400, detail="invalid tickers query param")

    url_map: Dict[str, str] = {}
    if urls:
        try:
            url_map = json.loads(urls)
            if not isinstance(url_map, dict):
                url_map = {}
        except Exception:
            # Ignore bad JSON; proceed without overrides
            url_map = {}

    out: Dict[str, dict] = {}
    for raw in tick_list:
        code = _norm_ticker(raw)
        if not code:
            continue
        info = _fetch_company(code, url_override=url_map.get(code))
        out[code] = _normalize_output(code, info)

    return {"ok": True, "data": out}


# ---------------------------------------------------------------------
# Optional: legacy mount helper (if you want /v33/argaam compatibility)
# In your main.py, you can do:
#   from routes_argaam import router as argaam_router, legacy_router as argaam_legacy_router
#   app.include_router(argaam_router)
#   app.include_router(argaam_legacy_router)
# ---------------------------------------------------------------------
legacy_router = APIRouter(prefix="/v33/argaam", tags=["argaam (legacy)"])

@legacy_router.post("/quotes")
def argaam_quotes_post_legacy(req: ArgaamReq):
    return argaam_quotes_post(req)

@legacy_router.get("/quotes")
def argaam_quotes_get_legacy(
    tickers: str = Query(...),
    urls: Optional[str] = Query(default=None),
):
    return argaam_quotes_get(tickers=tickers, urls=urls)
