from __future__ import annotations

import os
import re
import time
import random
import json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

# Optional: BeautifulSoup (Argaam parsing will be skipped if not installed)
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # type: ignore

# =============================================================================
# Configuration (environment-driven)
# =============================================================================
SERVICE_NAME = "tadawul-fast"
VERSION = "v41"

FASTAPI_TOKEN = os.getenv("FASTAPI_TOKEN", "").strip()
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()  # backward-compat
REQUIRE_AUTH = bool(FASTAPI_TOKEN or APP_TOKEN)

FASTAPI_BASE = os.getenv("FASTAPI_BASE", "").strip()
RENDER_DEPLOY_HOOK = os.getenv("RENDER_DEPLOY_HOOK", "").strip()

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
).strip()

# Yahoo Finance endpoints
YQ_BASES: List[str] = [
    "https://query1.finance.yahoo.com/v7/finance/quote?symbols=",
    "https://query2.finance.yahoo.com/v7/finance/quote?symbols=",
]
YC_BASES: List[str] = [
    "https://query1.finance.yahoo.com/v8/finance/chart/",
    "https://query2.finance.yahoo.com/v8/finance/chart/",
]

# Saudi Exchange fund API
TAD_BASE: str = "https://www.saudiexchange.sa/api/chart/trading-data/mutual-funds/"

# Argaam URL candidates (English first, then Arabic fallbacks)
ARGAAM_CANDIDATES = [
    "https://www.argaam.com/en/company/{code}",
    "https://www.argaam.com/en/company/financials/{code}",
    "https://www.argaam.com/en/tadawul/company?symbol={code}",
    "https://www.argaam.com/ar/company/{code}",
    "https://www.argaam.com/ar/company/financials/{code}",
    "https://www.argaam.com/ar/tadawul/company?symbol={code}",
]

# Yahoo tuning knobs
QUOTE_BATCH_SIZE = int(os.getenv("QUOTE_BATCH_SIZE", "20"))           # safer than 50/100
YAHOO_MAX_URL_LEN = int(os.getenv("YAHOO_MAX_URL_LEN", "1800"))
YAHOO_MAX_RETRY_401 = int(os.getenv("YAHOO_MAX_RETRY_401", "3"))
YAHOO_MIN_BACKOFF_MS = int(os.getenv("YAHOO_MIN_BACKOFF_MS", "600"))
YAHOO_MAX_BACKOFF_MS = int(os.getenv("YAHOO_MAX_BACKOFF_MS", "2200"))

DEFAULT_QUOTE_TTL = int(os.getenv("CACHE_DURATION_QUOTE_SEC", "60"))
DEFAULT_CHART_TTL = int(os.getenv("CACHE_DURATION_HISTORY_SEC", "900"))

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15.0"))

# =============================================================================
# In-memory TTL cache  (key -> (expires_epoch, data))
# =============================================================================
_CACHE: Dict[str, Tuple[float, Any]] = {}


def _cache_get(key: str) -> Optional[Any]:
    v = _CACHE.get(key)
    if not v:
        return None
    exp, data = v
    if exp < time.time():
        _CACHE.pop(key, None)
        return None
    return data


def _cache_put(key: str, data: Any, ttl_sec: int) -> None:
    if ttl_sec <= 0:
        return
    _CACHE[key] = (time.time() + ttl_sec, data)


# =============================================================================
# Shared async HTTP client
# =============================================================================
_CLIENT: Optional[httpx.AsyncClient] = None


def get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(REQUEST_TIMEOUT),
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            },
            follow_redirects=True,
        )
    return _CLIENT


# =============================================================================
# Async helpers
# =============================================================================
async def _sleep_ms(ms: int) -> None:
    import asyncio
    await asyncio.sleep(max(0, ms) / 1000.0)


def _backoff_ms(attempt: int) -> int:
    # randomized backoff within configured bounds, grows slightly with attempt
    base = min(YAHOO_MAX_BACKOFF_MS, YAHOO_MIN_BACKOFF_MS * (1 + attempt))
    return random.randint(YAHOO_MIN_BACKOFF_MS, base)


async def _yahoo_get_json(url: str, add_referer_first: bool = True) -> Optional[dict]:
    """
    Try with Referer header, then without. Return JSON or None.
    """
    client = get_client()
    headers_ref = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.8",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "Referer": "https://finance.yahoo.com",
    }
    headers_no_ref = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.8",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    headers_list = [headers_ref, headers_no_ref] if add_referer_first else [headers_no_ref, headers_ref]

    for headers in headers_list:
        try:
            r = await client.get(url, headers=headers)
            if 200 <= r.status_code < 300:
                return r.json()
        except Exception:
            pass
    return None


# =============================================================================
# Auth
# =============================================================================
def _check_auth(request: Request) -> None:
    """Enforce Bearer auth only if a token is configured."""
    if not REQUIRE_AUTH:
        return
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = auth.split(" ", 1)[1].strip()
    allowed = {t for t in (FASTAPI_TOKEN, APP_TOKEN) if t}
    if token not in allowed:
        raise HTTPException(status_code=403, detail="invalid token")


# =============================================================================
# Symbol normalization (map TASI 4-digit codes -> Yahoo .SR)
# =============================================================================
_CODE_RE = re.compile(r"(\d{4})$")


def _to_yahoo_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    m = _CODE_RE.search(s)
    if m:
        return f"{m.group(1)}.SR"
    if s.endswith(".TASI"):
        return s[:-5] + ".SR"
    return s


# =============================================================================
# FastAPI app + CORS
# =============================================================================
app = FastAPI(title=SERVICE_NAME, version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict if you like
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# Meta / health
# =============================================================================
@app.get("/")
def root():
    return {
        "ok": True,
        "name": SERVICE_NAME,
        "version": VERSION,
        "base": FASTAPI_BASE or None,
        "endpoints": [
            "/health",
            "/healthz",
            "/v41/quotes (POST, GET)",
            "/v41/charts (POST, GET)",
            "/v33/quotes (POST) [legacy]",
            "/v33/charts (POST) [legacy]",
            "/v33/fund/{code} (GET)",
            "/v41/argaam/quotes (POST, GET)",
            "/v41/deploy (POST; protected)",
        ],
        "auth_required": REQUIRE_AUTH,
    }


@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}


@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": int(time.time())}


# =============================================================================
# QUOTES helper
# =============================================================================
def _normalize_symbols(raw: List[str]) -> List[str]:
    return [str(s).strip().upper() for s in raw if str(s).strip()]


def _parse_query_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _shrink_to_url_limit(host_prefix: str, items: List[str]) -> List[str]:
    """
    Ensure host_prefix + ",".join(items) stays under YAHOO_MAX_URL_LEN by shrinking items.
    """
    if not items:
        return items
    joined = ",".join(items)
    url = host_prefix + quote(joined, safe="")
    if len(url) <= YAHOO_MAX_URL_LEN:
        return items
    # shrink
    lo, hi = 1, len(items)
    best = [items[0]]
    while lo <= hi:
        mid = (lo + hi) // 2
        joined = ",".join(items[:mid])
        url = host_prefix + quote(joined, safe="")
        if len(url) <= YAHOO_MAX_URL_LEN:
            best = items[:mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return best


async def _fetch_quotes_batch(batch: List[str]) -> Dict[str, Any]:
    """
    Fetch one batch across hosts with referer toggling & backoff.
    Returns map keyed by Yahoo symbols.
    """
    out: Dict[str, Any] = {}
    attempts = 0
    while attempts <= YAHOO_MAX_RETRY_401:
        for host in YQ_BASES:
            shrink = _shrink_to_url_limit(host, batch)
            if not shrink:
                return out
            url = host + quote(",".join(shrink), safe="")
            # try with referer then without
            for add_ref in (True, False):
                j = await _yahoo_get_json(url, add_referer_first=add_ref)
                if j and isinstance(j, dict):
                    lst = (j.get("quoteResponse", {}) or {}).get("result") or []
                    for r in lst:
                        s = str(r.get("symbol") or "").upper()
                        if not s:
                            continue
                        out[s] = r
                    return out
        # backoff then retry whole batch
        attempts += 1
        await _sleep_ms(_backoff_ms(attempts))
    return out


def _build_quote_payload(raw_symbols: List[str], cache_ttl: int) -> Dict[str, Any]:
    y_symbols: List[str] = []
    back_map: Dict[str, str] = {}
    for s in raw_symbols:
        y = _to_yahoo_symbol(s)
        y_symbols.append(y)
        back_map[y] = s  # map back to input symbol

    out: Dict[str, Any] = {}
    errors: List[str] = []

    tasks: List[Tuple[str, List[str]]] = []  # (cache_key, batch)
    metas: List[Tuple[str, List[str]]] = []

    for i in range(0, len(y_symbols), QUOTE_BATCH_SIZE):
        batch = y_symbols[i : i + QUOTE_BATCH_SIZE]
        key = "yq::" + ",".join(batch)
        cached = _cache_get(key)
        if cached is not None:
            out.update(cached)  # cached pack already mapped by orig symbols
            continue
        tasks.append((key, batch))
        metas.append((key, batch))

    return {
        "y_symbols": y_symbols,
        "back_map": back_map,
        "tasks": tasks,
        "metas": metas,
        "out": out,
        "errors": errors,
        "cache_ttl": cache_ttl,
    }


async def _resolve_quotes(payload: Dict[str, Any]) -> Dict[str, Any]:
    tasks: List[Tuple[str, List[str]]] = payload["tasks"]
    metas = payload["metas"]
    out = payload["out"]
    errors = payload["errors"]
    back_map = payload["back_map"]
    cache_ttl = payload["cache_ttl"]

    # fetch batches sequentially (safer re: rate limits)
    for key, batch in tasks:
        raw_map = await _fetch_quotes_batch(batch)
        pack: Dict[str, Any] = {}
        if not raw_map:
            errors.append(f"Yahoo quote fetch failed for {batch}")
        for ysym, q in raw_map.items():
            orig = back_map.get(ysym, ysym)

            price = q.get("regularMarketPrice")
            eps = q.get("epsTrailingTwelveMonths")
            pe = q.get("trailingPE")
            if pe is None and price is not None and eps not in (None, 0):
                try:
                    pe = price / eps if eps else None
                except Exception:
                    pe = None

            mcap = q.get("marketCap")
            shares = q.get("sharesOutstanding")
            if mcap is None and price is not None and shares not in (None, 0):
                mcap = price * shares
            if shares is None and price not in (None, 0) and mcap is not None:
                try:
                    shares = mcap / price
                except Exception:
                    shares = None

            dy = q.get("dividendYield")
            if dy is None:
                rate = q.get("trailingAnnualDividendRate")
                if rate is not None and price not in (None, 0):
                    try:
                        dy = rate / price
                    except Exception:
                        dy = None

            # Minimal + extended fields (so Apps Script can read either)
            pack[orig] = {
                "symbol": q.get("symbol"),
                "price": price,
                "regularMarketPrice": price,
                "regularMarketDayHigh": q.get("regularMarketDayHigh"),
                "regularMarketDayLow": q.get("regularMarketDayLow"),
                "regularMarketVolume": q.get("regularMarketVolume"),
                "marketCap": mcap,
                "sharesOutstanding": shares,
                "dividendYield": dy,
                "lastDividendValue": q.get("lastDividendValue"),
                "dividendDate": q.get("dividendDate"),
                "eps": eps,
                "epsTrailingTwelveMonths": eps,
                "pe": pe,
                "trailingPE": pe,
                "beta": q.get("beta"),
                "beta3Year": q.get("beta3Year"),
                "beta5YearMonthly": q.get("beta5YearMonthly"),
                "shortName": q.get("shortName"),
                "longName": q.get("longName"),
                "name": q.get("longName") or q.get("shortName"),
                "sector": q.get("sector"),
                "industry": q.get("industry"),
                "industryDisp": q.get("industryDisp"),
                "fiftyTwoWeekHigh": q.get("fiftyTwoWeekHigh"),
                "fiftyTwoWeekLow": q.get("fiftyTwoWeekLow"),
                "recommendationKey": q.get("recommendationKey"),
                "recommendationMean": q.get("recommendationMean"),
                "marketCapIntraDay": q.get("marketCapIntraDay"),
                "dayHigh": q.get("regularMarketDayHigh"),
                "dayLow": q.get("regularMarketDayLow"),
                "volume": q.get("regularMarketVolume"),
            }

        _cache_put(key, pack, cache_ttl)
        out.update(pack)

    return out


async def _quotes_response(symbols: List[str], cache_ttl: int) -> Dict[str, Any]:
    if not symbols:
        return {"data": {}}
    payload = _build_quote_payload(symbols, cache_ttl)
    out = await _resolve_quotes(payload)
    resp = {"data": out}
    errors = payload["errors"]
    if errors:
        resp["error"] = "; ".join(errors)
    return resp


# =============================================================================
# CHARTS helper
# =============================================================================
async def _fetch_chart_one(yahoo_symbol: str, p1: int, p2: int) -> List[Dict[str, Any]]:
    """
    Try both hosts and both header modes with backoff.
    """
    params = f"?period1={p1}&period2={p2}&interval=1d&events=history&includeAdjustedClose=true"

    attempts = 0
    while attempts <= YAHOO_MAX_RETRY_401:
        for base in YC_BASES:
            url = f"{base}{quote(yahoo_symbol, safe='')}{params}"
            # with referer then without
            for add_ref in (True, False):
                j = await _yahoo_get_json(url, add_ref)
                try:
                    result = (((j or {}).get("chart") or {}).get("result") or [None])[0]
                    if not result:
                        continue
                    ts = result.get("timestamp") or []
                    q = ((result.get("indicators") or {}).get("quote") or [None])[0] or {}
                    series: List[Dict[str, Any]] = []
                    for i, t in enumerate(ts):
                        series.append(
                            {
                                "date": int(t) * 1000,
                                "timestamp": int(t),
                                "close": (q.get("close") or [None])[i],
                                "high": (q.get("high") or [None])[i],
                                "low": (q.get("low") or [None])[i],
                                "volume": (q.get("volume") or [None])[i],
                            }
                        )
                    return series
                except Exception:
                    # try next header/host
                    pass
        attempts += 1
        await _sleep_ms(_backoff_ms(attempts))
    return []


async def _charts_response(
    symbols_in: List[str], p1: int, p2: int, cache_ttl: int
) -> Dict[str, Any]:
    if not symbols_in or not p1 or not p2:
        return {"data": {}}

    out: Dict[str, Any] = {}
    errors: List[str] = []

    # sequential (safer under rate limits)
    for s in symbols_in:
        orig = s
        ysym = _to_yahoo_symbol(s)
        key = f"yc::{ysym}::{p1}::{p2}"
        cached = _cache_get(key)
        if cached is not None:
            out[orig] = cached
            continue

        series = await _fetch_chart_one(ysym, p1, p2)
        if not series:
            errors.append(f"chart fetch failed for {orig}")
        _cache_put(key, series, cache_ttl)
        out[orig] = series

    resp = {"data": out}
    if errors:
        resp["error"] = "; ".join(errors)
    return resp


# =============================================================================
# QUOTES — v33 (legacy) and v41 (preferred)
# =============================================================================
@app.post("/v33/quotes")
async def quotes(request: Request):
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    symbols = _normalize_symbols(body.get("symbols") or [])
    cache_ttl = int(body.get("cache_ttl") or DEFAULT_QUOTE_TTL)
    return await _quotes_response(symbols, cache_ttl)


@app.post("/v41/quotes")
async def quotes_v41(request: Request):
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    symbols = _normalize_symbols(body.get("symbols") or [])
    cache_ttl = int(body.get("cache_ttl") or DEFAULT_QUOTE_TTL)
    return await _quotes_response(symbols, cache_ttl)


@app.get("/v41/quotes")
async def quotes_v41_get(request: Request, symbols: Optional[str] = None, cache_ttl: int = DEFAULT_QUOTE_TTL):
    _check_auth(request)
    parsed = _parse_query_list(symbols)
    return await _quotes_response(_normalize_symbols(parsed), int(cache_ttl or DEFAULT_QUOTE_TTL))


# =============================================================================
# CHARTS — v33 (legacy) and v41 (preferred)
# =============================================================================
@app.post("/v33/charts")
async def charts(request: Request):
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    symbols_in = _normalize_symbols(body.get("symbols") or [])
    p1 = int(body.get("period1") or 0)
    p2 = int(body.get("period2") or 0)
    cache_ttl = int(body.get("cache_ttl") or DEFAULT_CHART_TTL)
    return await _charts_response(symbols_in, p1, p2, cache_ttl)


@app.post("/v41/charts")
async def charts_v41(request: Request):
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    symbols_in = _normalize_symbols(body.get("symbols") or [])
    p1 = int(body.get("period1") or 0)
    p2 = int(body.get("period2") or 0)
    cache_ttl = int(body.get("cache_ttl") or DEFAULT_CHART_TTL)
    return await _charts_response(symbols_in, p1, p2, cache_ttl)


@app.get("/v41/charts")
async def charts_v41_get(
    request: Request,
    symbols: Optional[str] = None,
    period1: Optional[int] = None,
    period2: Optional[int] = None,
    cache_ttl: int = DEFAULT_CHART_TTL,
):
    _check_auth(request)
    parsed = _normalize_symbols(_parse_query_list(symbols))
    p1 = int(period1 or 0)
    p2 = int(period2 or 0)
    return await _charts_response(parsed, p1, p2, int(cache_ttl or DEFAULT_CHART_TTL))


# =============================================================================
# TADAWUL FUND (numeric code) — GET /v33/fund/{code}
# =============================================================================
@app.get("/v33/fund/{code}")
async def fund(request: Request, code: str):
    _check_auth(request)
    key = f"tad::{code}"
    cached = _cache_get(key)
    if cached is not None:
        return {"data": cached}

    url = f"{TAD_BASE.rstrip('/')}/{quote(code)}"
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []

    j = await _yahoo_get_json(url, add_referer_first=False)  # simple GET
    if j is None:
        errors.append("fetch error")

    try:
        arr = []
        if isinstance(j, dict):
            for k in ("data", "prices", "series", "values"):
                if k in j and isinstance(j[k], list):
                    arr = j[k]
                    break
        if isinstance(arr, list):
            for d in arr:
                ts = d.get("t") or d.get("ts") or d.get("time") or d.get("date")
                c = (
                    d.get("c")
                    if d.get("c") is not None
                    else d.get("close")
                    if d.get("close") is not None
                    else d.get("value")
                )
                if ts is not None and c is not None:
                    try:
                        ts_num = int(ts)
                    except Exception:
                        ts_num = int(time.time())
                    ts_ms = ts_num if len(str(ts_num)) > 10 else ts_num * 1000
                    rows.append(
                        {
                            "date": int(ts_ms),
                            "close": float(c),
                            "high": float(d.get("h")) if d.get("h") is not None else None,
                            "low": float(d.get("l")) if d.get("l") is not None else None,
                        }
                    )
        rows.sort(key=lambda x: x["date"])
    except Exception as e:
        errors.append(f"parse error: {e}")
        rows = []

    _cache_put(key, rows, 6 * 3600)
    resp = {"data": rows}
    if errors:
        resp["error"] = "; ".join(errors)
    return resp


# =============================================================================
# A R G A A M  (optional; requires beautifulsoup4)
# =============================================================================
def _num_like(s: Optional[str]) -> Optional[float]:
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
    # normalize thousands
    t = t.replace(",", " ")
    m = re.search(r"(-?\d+(\s?\d{3})*(\.\d+)?)(\s*[%kKmMbBtT]n?)?", t)
    if not m:
        try:
            return float(t.replace(" ", ""))
        except Exception:
            return None
    core = m.group(1).replace(" ", "")
    suf = (m.group(4) or "").strip().lower()
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
    if not soup:
        return None
    pat = re.compile("|".join([re.escape(x) for x in labels]), flags=re.I)
    for node in soup.find_all(string=pat):
        try:
            parent = node.parent
            if parent:
                for s in parent.next_siblings:
                    if hasattr(s, "get_text"):
                        v = _num_like(s.get_text(" ", strip=True))
                        if v is not None:
                            return v
                for s in parent.previous_siblings:
                    if hasattr(s, "get_text"):
                        v = _num_like(s.get_text(" ", strip=True))
                        if v is not None:
                            return v
            row = parent.parent if parent else None
            if row:
                txt = row.get_text(" ", strip=True)
                nums = re.findall(r"[-\d٠-٩\.,%]+", txt)
                for cand in nums[::-1]:
                    v = _num_like(cand)
                    if v is not None:
                        return v
        except Exception:
            continue
    return None


async def _guess_argaam_url(code: str) -> Optional[str]:
    for tpl in ARGAAM_CANDIDATES:
        url = tpl.format(code=quote(code))
        html = await _fetch_html(url, retries=2, timeout=15.0)
        if html and len(html) > 2000:
            return url
    return None


def _parse_argaam_snapshot(html: str) -> Dict[str, Any]:
    if BeautifulSoup is None:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    name = None
    try:
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(" ", strip=True)
    except Exception:
        pass

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

    if out.get("price") is None:
        live = soup.select_one("[data-last], [data-last-price]")
        if live:
            cand = live.get("data-last") or live.get("data-last-price")
            out["price"] = _num_like(cand)
    return out


def _norm_tasi_code(s: str) -> str:
    m = re.search(r"(\d{4})", s or "")
    return m.group(1) if m else (s or "").upper()


@app.post("/v41/argaam/quotes")
async def argaam_quotes(request: Request):
    """
    Body: { "tickers": ["1120","2010"], "urls": {"1120":"..."}, "cache_ttl": 600 }
    """
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    tickers_in: List[str] = [str(s) for s in (body.get("tickers") or [])]
    tickers = [_norm_tasi_code(s) for s in tickers_in if str(s).strip()]
    url_map: Dict[str, str] = {_norm_tasi_code(k): v for k, v in (body.get("urls") or {}).items()}
    cache_ttl = int(body.get("cache_ttl") or 600)

    if not tickers:
        return {"data": {}}

    if BeautifulSoup is None:
        raise HTTPException(
            status_code=503,
            detail="Argaam parser not available. Install 'beautifulsoup4' in your environment.",
        )

    out: Dict[str, Any] = {}
    to_fetch: List[Tuple[str, Optional[str]]] = []
    for code in tickers:
        key = f"argaam::{code}"
        cached = _cache_get(key)
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
            to_fetch.append((code, url_map.get(code)))

    if to_fetch:
        import asyncio

        async def _fetch_one(code: str, override: Optional[str]) -> Tuple[str, Dict[str, Any]]:
            url = override or (await _guess_argaam_url(code))
            html = await _fetch_html(url, retries=2) if url else None
            data = _parse_argaam_snapshot(html) if html else {}
            _cache_put(f"argaam::{code}", data, cache_ttl)
            return code, data

        tasks = [_fetch_one(code, url) for code, url in to_fetch]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        for code, data in results:
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


@app.get("/v41/argaam/quotes")
async def argaam_quotes_get(
    request: Request,
    tickers: Optional[str] = None,  # comma-separated "1120,2010"
    cache_ttl: int = 600,
):
    """GET wrapper: /v41/argaam/quotes?tickers=1120,2010&cache_ttl=600"""
    _check_auth(request)
    if not tickers:
        raise HTTPException(status_code=400, detail="tickers query param is required")

    tickers_list = [t.strip() for t in tickers.split(",") if t.strip()]
    body = {"tickers": tickers_list, "cache_ttl": cache_ttl}
    request._body = body  # type: ignore
    return await argaam_quotes(request)


# =============================================================================
# Protected Deploy Trigger (optional)
# =============================================================================
@app.post("/v41/deploy")
async def trigger_deploy(request: Request):
    """
    Protected endpoint to trigger Render deploy via RENDER_DEPLOY_HOOK.
    Requires valid Bearer token. No-op if hook not configured.
    """
    _check_auth(request)
    hook = RENDER_DEPLOY_HOOK
    if not hook:
        return {"ok": False, "error": "RENDER_DEPLOY_HOOK is not set"}
    try:
        r = await get_client().get(hook, timeout=20.0)
        return {"ok": r.status_code in (200, 201, 202), "status": r.status_code}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"deploy hook error: {e}")


# =============================================================================
# Shutdown hook (close shared client)
# =============================================================================
@app.on_event("shutdown")
async def _shutdown():
    global _CLIENT
    if _CLIENT is not None:
        try:
            await _CLIENT.aclose()
        finally:
            _CLIENT = None
