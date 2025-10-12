import os, time, json
from typing import List, Dict, Any
from urllib.parse import quote
import asyncio

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import httpx

# -------------------------------------------------------------------
# ENV & CONSTANTS
# -------------------------------------------------------------------
APP_TOKEN = os.getenv("APP_TOKEN", "change-me")
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

YQ_BASES = [
    "https://query1.finance.yahoo.com/v7/finance/quote?symbols=",
    "https://query2.finance.yahoo.com/v7/finance/quote?symbols="
]
YC_BASE  = "https://query1.finance.yahoo.com/v8/finance/chart/"  # we’ll URL-encode the symbol
TAD_BASE = "https://www.saudiexchange.sa/api/chart/trading-data/mutual-funds/"

# -------------------------------------------------------------------
# simple in-memory TTL cache
# -------------------------------------------------------------------
_cache: Dict[str, Any] = {}  # key -> (expires_epoch, data)

def _cache_get(key: str):
    v = _cache.get(key)
    if not v:
        return None
    exp, data = v
    if exp < time.time():
        _cache.pop(key, None)
        return None
    return data

def _cache_put(key: str, data: Any, ttl_sec: int):
    _cache[key] = (time.time() + ttl_sec, data)

# -------------------------------------------------------------------
# HTTP helpers
# -------------------------------------------------------------------
async def _fetch_json(client: httpx.AsyncClient, url: str, retries=2, timeout=12):
    last_exc = None
    for i in range(retries + 1):
        try:
            r = await client.get(
                url,
                headers={"User-Agent": USER_AGENT, "Pragma": "no-cache", "Cache-Control": "no-cache"},
                timeout=timeout
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            # small exponential backoff
            await asyncio.sleep(0.12 * (2 ** i))
    raise RuntimeError(f"GET failed: {url} -> {last_exc}")

# -------------------------------------------------------------------
# FastAPI app + auth
# -------------------------------------------------------------------
app = FastAPI()

@app.get("/")
def root():
    return {
        "ok": True,
        "endpoints": ["/health", "POST /v33/quotes", "POST /v33/charts", "GET /v33/fund/{code}"]
    }

def _check_auth(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = auth.split(" ", 1)[1].strip()
    if token != APP_TOKEN:
        raise HTTPException(status_code=403, detail="invalid token")

@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

# -------------------------------------------------------------------
# helpers
# -------------------------------------------------------------------
def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

# -------------------------------------------------------------------
# QUOTES (Yahoo) — POST /v33/quotes
# body: { "symbols": [...], "cache_ttl": 60 }
# returns: { "data": { "AAPL": {...}, ... } }
# -------------------------------------------------------------------
@app.post("/v33/quotes")
async def quotes(request: Request):
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    symbols: List[str] = [str(s).upper().strip() for s in (body.get("symbols") or []) if str(s).strip()]
    cache_ttl = int(body.get("cache_ttl") or 60)
    if not symbols:
        return {"data": {}}

    out: Dict[str, Any] = {}
    chunk = 50
    base_idx = 0

    async with httpx.AsyncClient() as client:
        tasks = []
        meta = []

        # prepare batches (use cache if present)
        for i in range(0, len(symbols), chunk):
            batch = symbols[i:i+chunk]
            key = "yq::" + ",".join(batch)
            cached = _cache_get(key)
            if cached is not None:
                out.update(cached)
                continue

            base = YQ_BASES[base_idx % len(YQ_BASES)]
            base_idx += 1
            # FIX: build URL safely (no httpx.QueryParams.encode_component)
            url = base + ",".join(quote(s) for s in batch)
            tasks.append(_fetch_json(client, url))
            meta.append((key, batch))

        # gather network results
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for (key, batch), res in zip(meta, results):
                pack = {}
                if isinstance(res, Exception):
                    # on failure, just skip this batch (don’t blow up 500)
                    continue
                arr = (((res or {}).get("quoteResponse") or {}).get("result")) or []
                for q in arr:
                    t = str(q.get("symbol", "")).upper()
                    if not t:
                        continue
                    price = _safe_float(q.get("regularMarketPrice"))
                    eps = _safe_float(q.get("epsTrailingTwelveMonths"))
                    pe  = _safe_float(q.get("trailingPE"))
                    if pe is None and price is not None and eps not in (None, 0):
                        try:
                            pe = price / eps if eps else None
                        except Exception:
                            pe = None

                    # market cap / shares fallbacks
                    mcap = _safe_float(q.get("marketCap"))
                    shares = _safe_float(q.get("sharesOutstanding"))
                    if mcap is None and price is not None and shares not in (None, 0):
                        mcap = price * shares
                    if shares is None and price not in (None, 0) and mcap is not None:
                        try:
                            shares = mcap / price
                        except Exception:
                            shares = None

                    # dividend yield fallback
                    dy = _safe_float(q.get("trailingAnnualDividendYield"))
                    if dy is None:
                        rate = _safe_float(q.get("trailingAnnualDividendRate"))
                        if rate is not None and price not in (None, 0):
                            try:
                                dy = rate / price
                            except Exception:
                                dy = None

                    pack[t] = {
                        "price": price,
                        "chgPct": (_safe_float(q.get("regularMarketChangePercent")) or 0)/100
                                  if q.get("regularMarketChangePercent") is not None else None,
                        "name": q.get("longName") or q.get("shortName"),
                        "sector": q.get("sector"),
                        "industry": q.get("industry"),
                        "dayHigh": _safe_float(q.get("regularMarketDayHigh")),
                        "dayLow":  _safe_float(q.get("regularMarketDayLow")),
                        "volume":  _safe_float(q.get("regularMarketVolume")),
                        "marketCap": mcap,
                        "sharesOutstanding": shares,
                        "dividendYield": dy,
                        "eps": eps,
                        "pe": pe,
                        "beta": _safe_float(q.get("beta")),
                    }
                _cache_put(key, pack, cache_ttl)
                out.update(pack)

    return {"data": out}

# -------------------------------------------------------------------
# CHART (Yahoo) — POST /v33/charts
# body: { "symbols": [...], "period1": 1690000000, "period2": 1720000000 }
# returns: { "data": { "AAPL": [ {date, open, high, low, close, volume}, ... ] } }
# -------------------------------------------------------------------
@app.post("/v33/charts")
async def charts(request: Request):
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}

    symbols: List[str] = [str(s).upper().strip() for s in (body.get("symbols") or []) if str(s).strip()]
    p1 = int(body.get("period1") or 0)
    p2 = int(body.get("period2") or 0)
    cache_ttl = int(body.get("cache_ttl") or 900)
    if not symbols or not p1 or not p2:
        return {"data": {}}

    out: Dict[str, Any] = {}

    async with httpx.AsyncClient() as client:
        tasks, meta = [], []
        for s in symbols:
            key = f"yc::{s}::{p1}::{p2}"
            cached = _cache_get(key)
            if cached is not None:
                out[s] = cached
                continue
            # FIX: safe URL for symbol
            url = f"{YC_BASE}{quote(s)}?period1={p1}&period2={p2}&interval=1d&events=history%7Cdiv"
            tasks.append(_fetch_json(client, url))
            meta.append((key, s))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for (key, s), j in zip(meta, results):
                hist = []
                if isinstance(j, Exception):
                    _cache_put(key, hist, cache_ttl)
                    out[s] = hist
                    continue
                try:
                    result = (((j or {}).get("chart") or {}).get("result") or [None])[0]
                    if result and result.get("timestamp"):
                        ts = result["timestamp"]
                        q  = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
                        for i in range(len(ts)):
                            c = (q.get("close") or [None])[i]
                            if c is None:
                                continue
                            row = {
                                "date": int(ts[i]) * 1000,  # ms epoch for GAS
                                "open": (q.get("open") or [None])[i],
                                "high": (q.get("high") or [None])[i],
                                "low":  (q.get("low")  or [None])[i],
                                "close": c,
                                "volume": (q.get("volume") or [None])[i],
                            }
                            hist.append(row)
                except Exception:
                    hist = []
                _cache_put(key, hist, cache_ttl)
                out[s] = hist

    return {"data": out}

# -------------------------------------------------------------------
# TADAWUL FUND (numeric code) — GET /v33/fund/{code}
# returns: { "data": [ {date(ms), close, high?, low?}, ... ] }
# -------------------------------------------------------------------
@app.get("/v33/fund/{code}")
async def fund(request: Request, code: str):
    _check_auth(request)

    key = f"tad::{code}"
    cached = _cache_get(key)
    if cached is not None:
        return {"data": cached}

    url = f"{TAD_BASE.rstrip('/')}/{quote(code)}"
    async with httpx.AsyncClient() as client:
        j = await _fetch_json(client, url)

    rows = []
    arr = (j.get("data") or j.get("prices") or j.get("series") or j.get("values") or [])
    if isinstance(arr, list):
        for d in arr:
            ts = d.get("t") or d.get("ts") or d.get("time") or d.get("date")
            c  = d.get("c") if d.get("c") is not None else d.get("close") if d.get("close") is not None else d.get("value")
            if ts is not None and c is not None:
                try:
                    ts_i = int(ts)
                    ts_ms = ts_i if len(str(ts_i)) > 10 else ts_i * 1000
                except Exception:
                    ts_ms = int(time.time() * 1000)
                rows.append({
                    "date": ts_ms,
                    "close": float(c),
                    "high": float(d.get("h")) if d.get("h") is not None else None,
                    "low":  float(d.get("l")) if d.get("l") is not None else None
                })

    rows.sort(key=lambda x: x["date"])
    _cache_put(key, rows, 6*3600)
    return {"data": rows}
