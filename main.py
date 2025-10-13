# main.py
import os, time, re
from typing import List, Dict, Any, Optional
from urllib.parse import quote

from fastapi import FastAPI, Request, HTTPException
import httpx

# ---- Try to import BeautifulSoup; if missing, keep running without Argaam parsing
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # type: ignore

# -------------------------------------------------------------------
# Config (env-driven)
# -------------------------------------------------------------------
APP_TOKEN  = os.getenv("APP_TOKEN", "change-me")  # MUST match your Render env var
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
)

YQ_BASES: List[str] = [
    "https://query1.finance.yahoo.com/v7/finance/quote?symbols=",
    "https://query2.finance.yahoo.com/v7/finance/quote?symbols=",
]
YC_BASE: str  = "https://query1.finance.yahoo.com/v8/finance/chart/"
TAD_BASE: str = "https://www.saudiexchange.sa/api/chart/trading-data/mutual-funds/"

# Argaam page candidates
ARGAAM_CANDIDATES = [
    "https://www.argaam.com/en/company/{code}",
    "https://www.argaam.com/en/company/financials/{code}",
    "https://www.argaam.com/en/tadawul/company?symbol={code}",
]

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
# tiny async helpers
# -------------------------------------------------------------------
async def asyncio_sleep(s: float):
    import asyncio
    await asyncio.sleep(s)

async def _fetch_json(client: httpx.AsyncClient, url: str, retries: int = 2, timeout: int = 12) -> Optional[dict]:
    for i in range(retries + 1):
        try:
            r = await client.get(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                    "Pragma": "no-cache",
                    "Cache-Control": "no-cache",
                },
                timeout=timeout,
            )
            r.raise_for_status()
            return r.json()
        except Exception:
            await asyncio_sleep(0.12 * (2 ** i))
    return None

async def _fetch_html(client: httpx.AsyncClient, url: str, retries: int = 2, timeout: int = 15) -> Optional[str]:
    for i in range(retries + 1):
        try:
            r = await client.get(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Pragma": "no-cache",
                    "Cache-Control": "no-cache",
                },
                timeout=timeout,
            )
            if r.status_code == 200 and r.text and len(r.text) > 1000:
                return r.text
        except Exception:
            pass
        await asyncio_sleep(0.12 * (2 ** i))
    return None

async def gather_all(tasks):
    import asyncio
    if not tasks:
        return []
    return await asyncio.gather(*tasks, return_exceptions=False)

# -------------------------------------------------------------------
# FastAPI app + auth
# -------------------------------------------------------------------
app = FastAPI(title="tadawul-fast", version="v33")

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
        "name": "tadawul-fast",
        "version": "v33",
        "endpoints": [
            "/health",
            "/healthz",
            "/v33/quotes (POST)",
            "/v33/charts (POST)",
            "/v33/fund/{code} (GET)",
            "/v33/argaam/quotes (POST)",
        ],
    }

@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": int(time.time())}

# -------------------------------------------------------------------
# QUOTES (Yahoo) — POST /v33/quotes
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
    errors: List[str] = []
    chunk = 50
    idx = 0

    try:
        async with httpx.AsyncClient() as client:
            tasks = []
            meta = []
            for i in range(0, len(symbols), chunk):
                batch = symbols[i : i + chunk]
                key = "yq::" + ",".join(batch)
                cached = _cache_get(key)
                if cached is not None:
                    out.update(cached)
                    continue

                base = YQ_BASES[idx % len(YQ_BASES)]
                idx += 1
                url = base + ",".join(quote(s) for s in batch)
                tasks.append(_fetch_json(client, url))
                meta.append((key, batch))

            if tasks:
                results = await gather_all(tasks)
                for (key, batch), res in zip(meta, results):
                    if not res:
                        errors.append(f"Yahoo quote fetch failed for {batch}")
                        continue

                    arr = (((res or {}).get("quoteResponse") or {}).get("result")) or []
                    pack: Dict[str, Any] = {}
                    for q in arr:
                        t = str(q.get("symbol", "")).upper()
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
    except Exception as e:
        errors.append(f"internal error: {e}")

    resp = {"data": out}
    if errors:
        resp["error"] = "; ".join(errors)
    return resp

# -------------------------------------------------------------------
# CHART (Yahoo) — POST /v33/charts
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
    errors: List[str] = []

    try:
        async with httpx.AsyncClient() as client:
            tasks, meta = [], []
            for s in symbols:
                key = f"yc::{s}::{p1}::{p2}"
                cached = _cache_get(key)
                if cached is not None:
                    out[s] = cached
                    continue
                url = f"{YC_BASE}{quote(s)}?period1={p1}&period2={p2}&interval=1d&events=history%7Cdiv"
                tasks.append(_fetch_json(client, url))
                meta.append((key, s))

            if tasks:
                results = await gather_all(tasks)
                for (key, s), j in zip(meta, results):
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
                    except Exception:
                        errors.append(f"Yahoo chart parse failed for {s}")
                        hist = []
                    _cache_put(key, hist, cache_ttl)
                    out[s] = hist
    except Exception as e:
        errors.append(f"internal error: {e}")

    resp = {"data": out}
    if errors:
        resp["error"] = "; ".join(errors)
    return resp

# -------------------------------------------------------------------
# TADAWUL FUND (numeric code) — GET /v33/fund/{code}
# -------------------------------------------------------------------
@app.get("/v33/fund/{code}")
async def fund(request: Request, code: str):
    _check_auth(request)
    key = f"tad::{code}"
    cached = _cache_get(key)
    if cached is not None:
        return {"data": cached}

    url = f"{TAD_BASE.rstrip('/')}/{quote(code)}"

    rows = []
    errors: List[str] = []

    try:
        async with httpx.AsyncClient() as client:
            j = await _fetch_json(client, url)
    except Exception as e:
        j = None
        errors.append(f"fetch error: {e}")

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

# ===================================================================
#                          A R G A A M  (optional)
# ===================================================================

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
    t = t.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))
    t = t.replace(",", " ")
    m = re.search(r"(-?\d+(\s?\d{3})*(\.\d+)?)(\s*[%kKmMbBtT]n?)?", t)
    if not m:
        t2 = t.replace(" ", "")
        try:
            return float(t2)
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
    mult = {"k":1e3, "m":1e6, "b":1e9, "bn":1e9, "t":1e12}
    for k in list(mult.keys()):
        mult[k.upper()] = mult[k]
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

async def _guess_argaam_url(client: httpx.AsyncClient, code: str) -> Optional[str]:
    for tpl in ARGAAM_CANDIDATES:
        url = tpl.format(code=quote(code))
        html = await _fetch_html(client, url, retries=1)
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

async def _fetch_argaam_company(client: httpx.AsyncClient, code: str, url_override: Optional[str] = None, ttl: int = 600) -> Dict[str, Any]:
    cache_key = f"argaam::{code}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    url = url_override or (await _guess_argaam_url(client, code))
    if not url:
        _cache_put(cache_key, {}, 120)
        return {}
    html = await _fetch_html(client, url, retries=2)
    if not html:
        _cache_put(cache_key, {}, 120)
        return {}
    data = _parse_argaam_snapshot(html)
    _cache_put(cache_key, data, ttl)
    return data

def _norm_tasi_code(s: str) -> str:
    m = re.search(r"(\d{4})", s or "")
    return m.group(1) if m else (s or "").upper()

# ---- Argaam endpoint (returns 200 with error if bs4 not installed)
@app.post("/v33/argaam/quotes")
async def argaam_quotes(request: Request):
    _check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    tickers: List[str] = [_norm_tasi_code(str(s)) for s in (body.get("tickers") or []) if str(s).strip()]
    url_map: Dict[str, str] = {_norm_tasi_code(k): v for k, v in (body.get("urls") or {}).items()}
    cache_ttl = int(body.get("cache_ttl") or 600)

    if not tickers:
        return {"data": {}}

    if BeautifulSoup is None:
        return {"data": {}, "error": "Argaam parser unavailable. Install beautifulsoup4 or add it to requirements.txt."}

    out: Dict[str, Any] = {}
    errors: List[str] = []
    try:
        async with httpx.AsyncClient() as client:
            tasks, meta = [], []
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
                    continue
                tasks.append(_fetch_argaam_company(client, code, url_override=url_map.get(code), ttl=cache_ttl))
                meta.append(code)

            if tasks:
                results = await gather_all(tasks)
                for code, data in zip(meta, results):
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
    except Exception as e:
        errors.append(f"internal error: {e}")

    resp = {"data": out}
    if errors:
        resp["error"] = "; ".join(errors)
    return resp
