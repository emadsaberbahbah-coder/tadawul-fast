# routes_argaam.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import re, time, math
import requests
from bs4 import BeautifulSoup

router = APIRouter(prefix="/v33/argaam", tags=["argaam"])

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT, "Pragma": "no-cache", "Cache-Control": "no-cache"}

# Simple in-memory TTL cache
_CACHE = {}  # key -> (expires_ts, data)


class ArgaamReq(BaseModel):
    tickers: list[str]
    urls: dict[str, str] | None = None  # optional per-ticker override


def _ttl_get(key):
    now = time.time()
    ent = _CACHE.get(key)
    if not ent: return None
    exp, data = ent
    if now > exp:
        _CACHE.pop(key, None)
        return None
    return data


def _ttl_put(key, data, ttl=600):
    _CACHE[key] = (time.time() + ttl, data)


def _num(x):
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip().replace(",", "")
    # handle Arabic digits
    ar_map = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    s = s.translate(ar_map)
    # handle percentages
    if s.endswith("%"):
        try: return float(s[:-1]) / 100.0
        except: return None
    # handle common suffixes
    m = re.match(r"^(-?\d+(\.\d+)?)([MBTK]|B|bn|m|k)?$", s, flags=re.I)
    if m:
        val = float(m.group(1))
        suf = (m.group(3) or "").lower()
        mult = {"k":1e3, "m":1e6, "b":1e9, "bn":1e9, "t":1e12}.get(suf, 1.0)
        return val * mult
    try: return float(s)
    except: return None


def _extract_text_after_label(soup, labels):
    # try English & Arabic labels
    joined = "|".join([re.escape(lbl) for lbl in labels])
    # Find label anywhere and get the nearest number-like text in the same row/box
    for el in soup.find_all(text=re.compile(joined, flags=re.I)):
        # look rightwards / parent row cells
        try:
            parent = el.find_parent()
            if not parent: continue
            # check siblings
            sibs = list(parent.next_siblings)
            for s in sibs:
                if hasattr(s, "get_text"):
                    txt = s.get_text(" ", strip=True)
                    # first numeric-ish token
                    mt = re.search(r"[-\d٠-٩\.,%]+", txt)
                    if mt:
                        return mt.group(0)
        except:
            pass
    return None


def _parse_company_snapshot(html):
    soup = BeautifulSoup(html, "html.parser")

    # Try to collect basic name/sector/industry if available
    name = None
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(" ", strip=True)

    # Common labels to try (English & Arabic)
    labels = {
        "marketCap": ["Market Cap", "القيمة السوقية"],
        "sharesOutstanding": ["Shares Outstanding", "الأسهم القائمة"],
        "eps": ["EPS (TTM)", "ربحية السهم (TTM)", "ربحية السهم"],
        "pe": ["P/E (TTM)", "مكرر الربحية"],
        "beta": ["Beta", "بيتا"],
        "dividendYield": ["Dividend Yield", "عائد التوزيع"],
        "volume": ["Volume", "الكمية", "حجم التداول"],
        "dayHigh": ["High", "أعلى"],
        "dayLow": ["Low", "أدنى"],
        "price": ["Last", "السعر", "Last Trade", "آخر صفقة"]
    }

    out = {"name": name, "sector": None, "industry": None}

    # scrape by labels
    for key, labs in labels.items():
        vtxt = _extract_text_after_label(soup, labs)
        if vtxt is not None:
            out[key] = _num(vtxt)

    # Fallback: sometimes live price sits in a special tag with data attributes
    if out.get("price") is None:
        live = soup.select_one("[data-last], [data-last-price]")
        if live:
            cand = live.get("data-last") or live.get("data-last-price")
            out["price"] = _num(cand)

    return out


def _guess_company_url(tasi_code: str) -> str | None:
    # Heuristic (English pages). Adjust if your deployment is Arabic-only.
    # We try a couple of common patterns:
    candidates = [
        f"https://www.argaam.com/en/company/{tasi_code}",
        f"https://www.argaam.com/en/company/financials/{tasi_code}",
        f"https://www.argaam.com/en/tadawul/company?symbol={tasi_code}",
    ]
    for url in candidates:
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code == 200 and len(r.text) > 2000:
                return url
        except:
            pass
    return None


def _fetch_company(tasi_code: str, url_override: str | None = None) -> dict:
    # caching
    cache_key = f"argaam:{tasi_code}"
    cached = _ttl_get(cache_key)
    if cached is not None:
        return cached

    url = url_override or _guess_company_url(tasi_code)
    if not url:
        _ttl_put(cache_key, {}, ttl=120)
        return {}

    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code != 200:
            _ttl_put(cache_key, {}, ttl=120)
            return {}
        data = _parse_company_snapshot(res.text)
        _ttl_put(cache_key, data, ttl=600)
        return data
    except:
        _ttl_put(cache_key, {}, ttl=120)
        return {}


@router.post("/quotes")
def argaam_quotes(req: ArgaamReq):
    if not req.tickers:
        raise HTTPException(status_code=400, detail="tickers array required")

    out = {}
    for raw in req.tickers:
        # Normalize 1120.SR -> 1120
        m = re.search(r"(\d{4})", raw or "")
        code = m.group(1) if m else (raw or "").upper()
        url = (req.urls or {}).get(code)
        info = _fetch_company(code, url_override=url)

        # normalize fields to the exact names your Apps Script expects
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
            "beta": info.get("beta")
        }

    return {"ok": True, "data": out}
