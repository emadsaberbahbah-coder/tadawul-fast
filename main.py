# main.py
import os, time
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import asyncio

APP_TOKEN = os.getenv("APP_TOKEN", "change-me")
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

YQ_BASES = [
    "https://query1.finance.yahoo.com/v7/finance/quote?symbols=",
    "https://query2.finance.yahoo.com/v7/finance/quote?symbols=",
]
YC_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
TAD_BASE = "https://www.saudiexchange.sa/api/chart/trading-data/mutual-funds/"

# -------------------------------------------------------------------
# in-memory TTL cache
# -------------------------------------------------------------------
_cache: Dict[str, Tuple[float, Any]] = {}

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
    _cache[key] = (time.time() + max(0, int(ttl_sec)), data)

# -------------------------------------------------------------------
# HTTP helpers
# -------------------------------------------------------------------
async def _sleep(s: float):
    await asyncio.sleep(s)

async def _fetch_json(client: httpx.AsyncClient, url: str, retries: int = 2, timeout: float = 12.0):
    """
    GET with retries. Never closes the shared client.
    On retries, use one-off clients so parallel tasks aren’t broken.
    """
    last_exc: Optional[Exception] = None

    try:
        r = await client.get(
            url,
            headers={"User-Agent": USER_AGENT, "Pragma": "no-cache", "Cache-Control": "no-cache"},
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        last_exc = e

    delay = 0.2
    for _ in range(retries):
        await _sleep(delay)
        delay = min(delay * 2, 2.0)
        try:
            async with httpx.AsyncClient(http2=True, timeout=timeout) as oneoff:
                r = await oneoff.get(
                    url,
                    headers={"User-Agent": USER_AGENT, "Pragma": "no-cache", "Cache-Control": "no-cache"},
                )
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_exc = e
            continue

    # Don’t raise here; we let callers handle gracefully.
    print(f"[WARN] GET failed after retries: {url} -> {last_exc}")
    return None

# -------------------------------------------------------------------
# FastAPI app + CORS + auth
# -------------------------------------------------------------------
app = FastAPI(title="tadawul-fast", version="v1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def _check_auth(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = auth.split(" ", 1)[1].strip()
    if token != APP_TOKEN:
        raise HTTPException(status_code=403, detail="invalid token")

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "tadawul-fast",
        "endpoints": ["/health", "/v33/quotes (POST)", "/v33/charts (POST)", "/v33/fund/{code} (GET)"],
    }

@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

# -------------------------------------------------------------------
# QUOTES — POST /v33/quotes
# body: { "symbols": [...], "cache_ttl": 60 }
# Always returns 200 with {data:{}} even on upstream errors.
# -------------------------------------------------------------------
@app.post("/v33/quotes")
async def quotes(request: Request):
    try:
        _check_auth(request)
        body = await request.json()
        symbols: List[str] = [str(s).strip().upper() for s in (body.get("symbols") or []) if str(s).strip()]
        cache_ttl = int(body.get("cache_ttl") or 60)
        if not symbols:
            return {"data": {}}

        out: Dict[str, Any] = {}
        chunk = 50
        base_idx = 0

        async with httpx.AsyncClient(http2=True, timeout=12.0) as client:
            tasks = []
            meta = []

            for i in range(0, len(symbols), chunk):
                batch = symbols[i:i + chunk]
                key = "yq::" + ",".join(batch)
                cached = _cache_get(key)
                if cached is not None:
                    out.update(cached)
                    continue

                base = YQ_BASES[base_idx % len(YQ_BASES)]
                base_idx += 1
                url = base + ",".join([quote(s, safe="") for s in batch])

                tasks.append(_fetch_json(client, url))
                meta.append((key, batch))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for (key, batch), res in zip(meta, results):
                    if isinstance(res, Exception) or res is None:
                        print(f"[WARN] quotes: fetch failed for {batch}: {res}")
                        _cache_put(key, {}, cache_ttl)
                        continue

                    arr = (((res or {}).get("quoteResponse") or {}).get("result")) or []
                    pack: Dict[str, Any] = {}

                    for q in arr:
                        t = str(q.get("symbol", "")).upper()
                        if not t:
                            continue

                        price = q.get("regularMarketPrice")
                        eps = q.get("epsTrailingTwelveMonths")
                        pe = q.get("trailingPE")

                        if pe is None and price is not None and eps not in (None, 0):
                            try:
                                pe = (price / eps) if eps else None
                            except Exception:
                                pe = None

                        mcap = q.get("marketCap")
                        shares = q.get("sharesOutstanding")
                        if mcap is None and (price is not None) and shares not in (None, 0):
                            mcap = price * shares
                        if shares is None and (price not in (None, 0)) and (mcap is not None):
                            try:
                                shares = mcap / price if price else None
                            except Exception:
                                shares = None

                        dy = q.get("trailingAnnualDividendYield")
                        if dy is None:
                            rate = q.get("trailingAnnualDividendRate")
                            if rate is not None and price not in (None, 0):
                                try:
                                    dy = rate / price
                                except Exception:
                                    dy = None

                        pack[t] = {
                            "price": price,
                            "chgPct": (q.get("regularMarketChangePercent") or 0) / 100
                            if q.get("regularMarketChangePercent") is not None
                            else None,
                            "name": q.get("longName") or q.get("shortName"),
                            "sector": q.get("sector"),
                            "industry": q.get("industry"),
                            "dayHigh": q.get("regularMarketDayHigh"),
                            "dayLow": q.get("regularMarketDayLow"),
                            "volume": q.get("regularMarketVolume"),
                            "marketCap": mcap,
                            "sharesOutstanding": shares,
                            "dividendYield": dy,
                            "eps": eps,
                            "pe": pe,
                            "beta": q.get("beta"),
                        }

                    _cache_put(key, pack, cache_ttl)
                    out.update(pack)

        return {"data": out}
    except HTTPException:
        # propagate auth errors as-is
        raise
    except Exception as e:
        # Never 500 — log and return empty
        print(f"[ERROR] /v33/quotes crashed: {e}")
        return {"data": {}}

# -------------------------------------------------------------------
# CHARTS — POST /v33/charts
# body: { "symbols": [...], "period1": <sec>, "period2": <sec> }
# -------------------------------------------------------------------
@app.post("/v33/charts")
async def charts(request: Request):
    try:
        _check_auth(request)
        body = await request.json()
        symbols: List[str] = [str(s).strip().upper() for s in (body.get("symbols") or []) if str(s).strip()]
        p1 = int(body.get("period1") or 0)
        p2 = int(body.get("period2") or 0)
        cache_ttl = int(body.get("cache_ttl") or 900)
        if not symbols or not p1 or not p2:
            return {"data": {}}

        out: Dict[str, Any] = {}
        async with httpx.AsyncClient(http2=True, timeout=12.0) as client:
            tasks, meta = [], []
            for s in symbols:
                key = f"yc::{s}::{p1}::{p2}"
                cached = _cache_get(key)
                if cached is not None:
                    out[s] = cached
                    continue

                url = f"{YC_BASE}{quote(s, safe='')}" \
                      f"?period1={p1}&period2={p2}&interval=1d&events=history%7Cdiv"

                tasks.append(_fetch_json(client, url))
                meta.append((key, s))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for (key, s), j in zip(meta, results):
                    if isinstance(j, Exception) or j is None:
                        print(f"[WARN] charts: fetch failed for {s}: {j}")
                        _cache_put(key, [], cache_ttl)
                        out[s] = []
                        continue

                    hist = []
                    try:
                        result = (((j or {}).get("chart") or {}).get("result") or [None])[0]
                        if result and result.get("timestamp"):
                            ts = result["timestamp"]
                            q = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
                            for i in range(len(ts)):
                                c = (q.get("close") or [None])[i]
                                if c is None:
                                    continue
                                row = {
                                    "date": int(ts[i]) * 1000,
                                    "open": (q.get("open") or [None])[i],
                                    "high": (q.get("high") or [None])[i],
                                    "low": (q.get("low") or [None])[i],
                                    "close": c,
                                    "volume": (q.get("volume") or [None])[i],
                                }
                                hist.append(row)
                    except Exception as ex:
                        print(f"[WARN] charts: parse failed for {s}: {ex}")
                        hist = []
                    _cache_put(key, hist, cache_ttl)
                    out[s] = hist

        return {"data": out}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] /v33/charts crashed: {e}")
        return {"data": {}}

# -------------------------------------------------------------------
# TADAWUL FUND — GET /v33/fund/{code}
# -------------------------------------------------------------------
@app.get("/v33/fund/{code}")
async def fund(request: Request, code: str):
    try:
        _check_auth(request)
        key = f"tad::{code}"
        cached = _cache_get(key)
        if cached is not None:
            return {"data": cached}

        url = f"{TAD_BASE.rstrip('/')}/{quote(code, safe='')}"
        async with httpx.AsyncClient(http2=True, timeout=12.0) as client:
            j = await _fetch_json(client, url)

        rows = []
        if isinstance(j, dict):
            arr = (j.get("data") or j.get("prices") or j.get("series") or j.get("values") or [])
            if isinstance(arr, list):
                for d in arr:
                    ts = d.get("t") or d.get("ts") or d.get("time") or d.get("date")
                    c = d.get("c") if d.get("c") is not None else d.get("close") if d.get("close") is not None else d.get("value")
                    if ts is not None and c is not None:
                        try:
                            ts_int = int(ts)
                            ts_ms = ts_int if len(str(abs(ts_int))) > 10 else ts_int * 1000
                        except Exception:
                            ts_ms = int(time.time() * 1000)
                        rows.append({
                            "date": ts_ms,
                            "close": float(c),
                            "high": float(d.get("h")) if d.get("h") is not None else None,
                            "low": float(d.get("l")) if d.get("l") is not None else None
                        })
        rows.sort(key=lambda x: x["date"])
        _cache_put(key, rows, 6 * 3600)
        return {"data": rows}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] /v33/fund crashed: {e}")
        return {"data": []}
