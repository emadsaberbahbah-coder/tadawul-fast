# core/data_engine.py
"""
core/data_engine.py
===========================================================
LEGACY KSA ENGINE (PROD SAFE) – v3.0.0

Purpose
-------
This module exists because core.data_engine_v2 delegates KSA symbols here first.
If this file is missing/weak, KSA returns MISSING.

Design goals
------------
- KSA-safe: never uses EODHD/FMP/Finnhub for .SR.
- Avoid yfinance (Yahoo often 401/blocked on servers).
- Best-effort multi-source:
    1) Tadawul JSON endpoints (preferred if reachable)
    2) Argaam tables scrape (fallback)

Exports
-------
- async get_enriched_quote(symbol) -> dict
- async get_enriched_quotes(symbols) -> list[dict]
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import httpx

# Optional HTML parsing
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None


logger = logging.getLogger("core.data_engine")

ENGINE_VERSION = "3.0.0"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "on", "y", "t"}


# ----------------------------
# Helpers
# ----------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if s.isdigit():
        return f"{s}.SR"
    if s.endswith(".SR") or s.endswith(".US") or "." in s:
        return s
    return f"{s}.US"


def is_ksa_symbol(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    return s.endswith(".SR") or s.isdigit()


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        s = str(val).strip()
        if not s or s in {"-", "—", "N/A", "NA", "null", "None"}:
            return None

        s = s.translate(_ARABIC_DIGITS)
        s = s.replace("٬", ",").replace("٫", ".")
        s = s.replace("SAR", "").replace("ريال", "").strip()
        s = s.replace("%", "").strip()
        s = s.replace(",", "")

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        return float(s)
    except Exception:
        return None


def _env_bool(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUTHY


def _timeout() -> float:
    try:
        v = float(os.getenv("HTTP_TIMEOUT_SEC", "20").strip())
        return max(8.0, min(40.0, v))
    except Exception:
        return 20.0


def _headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    h = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar-SA,ar;q=0.9,en-US,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
    }
    if extra:
        h.update(extra)
    return h


async def _http_get_text(client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None, timeout_sec: float = 12.0) -> Optional[str]:
    for attempt in range(3):
        try:
            r = await client.get(url, params=params, timeout=timeout_sec)
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                if attempt < 2:
                    await asyncio.sleep(0.35 + random.random() * 0.45)
                    continue
                return None
            if r.status_code >= 400:
                return None
            return r.text
        except Exception:
            if attempt < 2:
                await asyncio.sleep(0.35 + random.random() * 0.45)
                continue
            return None
    return None


async def _http_get_json(client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None, timeout_sec: float = 12.0) -> Optional[Union[Dict[str, Any], List[Any]]]:
    txt = await _http_get_text(client, url, params=params, timeout_sec=timeout_sec)
    if not txt:
        return None
    try:
        return json.loads(txt)
    except Exception:
        return None


# ----------------------------
# Argaam fallback (scrape)
# ----------------------------
ARGAAM_PRICES_URLS = [
    "https://www.argaam.com/ar/company/companies-prices/3",
    "https://www.argaam.com/en/company/companies-prices/3",
]
ARGAAM_VOLUME_URLS = [
    "https://www.argaam.com/ar/company/companies-volume/14",
    "https://www.argaam.com/en/company/companies-volume/14",
]


def _parse_argaam_table(html: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not BeautifulSoup or not html:
        return out

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table")
    table = None
    best_rows = 0
    for t in tables:
        rs = t.find_all("tr")
        if len(rs) > best_rows:
            best_rows = len(rs)
            table = t
    if not table:
        return out

    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        txt = [c.get_text(" ", strip=True) for c in cols]

        code = None
        code_idx = None
        for i in range(min(4, len(txt))):
            cand = (txt[i] or "").strip().translate(_ARABIC_DIGITS)
            if re.match(r"^\d{3,6}$", cand):
                code = cand
                code_idx = i
                break
        if not code or code_idx is None:
            continue

        name = txt[code_idx + 1].strip() if code_idx + 1 < len(txt) else None

        # Heuristic parse: after code+name, first numbers are price/change/%; later may contain volume/value
        nums = [_safe_float(x) for x in txt[code_idx + 2:]]
        price = nums[0] if len(nums) > 0 else None
        change = nums[1] if len(nums) > 1 else None
        chg_p = nums[2] if len(nums) > 2 else None

        # volume/value: pick the biggest as volume-like, and the second-biggest as traded value-like
        vol = None
        val = None
        tail = [v for v in nums if isinstance(v, (int, float)) and v is not None and v > 0]
        if tail:
            tail_sorted = sorted(tail, reverse=True)
            vol = tail_sorted[0]
            if len(tail_sorted) > 1:
                val = tail_sorted[1]

            # If val < vol and price exists, it likely swapped; try infer value
            if price and vol and val and val < vol:
                val = price * vol

        out[code] = {
            "name": _safe_str(name),
            "price": price,
            "change": change,
            "change_percent": chg_p,
            "volume": vol,
            "value_traded": val,
        }

    return out


async def _argaam_snapshot(client: httpx.AsyncClient) -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    # stronger headers: some hosts behave better with referer
    async def get_first_ok(urls: Sequence[str]) -> Optional[str]:
        tasks = []
        for u in urls:
            tasks.append(_http_get_text(client, u, timeout_sec=14.0))
        res = await asyncio.gather(*tasks, return_exceptions=True)
        for r in res:
            if isinstance(r, str) and len(r) > 800:
                low = r.lower()
                if "access denied" in low or "cloudflare" in low:
                    continue
                return r
        for r in res:
            if isinstance(r, str) and r.strip():
                return r
        return None

    prices_html = await get_first_ok(ARGAAM_PRICES_URLS)
    if prices_html:
        part = _parse_argaam_table(prices_html)
        for k, v in part.items():
            merged.setdefault(k, {}).update(v)

    vol_html = await get_first_ok(ARGAAM_VOLUME_URLS)
    if vol_html:
        part = _parse_argaam_table(vol_html)
        for k, v in part.items():
            merged.setdefault(k, {}).update(v)

    return merged


# ----------------------------
# Tadawul best-effort (JSON)
# ----------------------------
# NOTE: Tadawul changes endpoints occasionally. We keep multiple candidates.
TADAWUL_ENDPOINTS = [
    # Common “market watch” JSON (candidate patterns)
    "https://www.tadawul.com.sa/wps/portal/tadawul/markets/equities/market-watch/market-watch",
    "https://www.tadawul.com.sa/wps/portal/tadawul/markets/equities/company-profile/company-profile",
]

# Some Tadawul pages embed data in script tags. We'll try to extract symbol numeric code matches.
def _extract_first_json_like(text: str) -> Optional[dict]:
    if not text:
        return None
    # Try to find a JSON object in scripts (very defensive)
    m = re.search(r"(\{.*\})", text, re.DOTALL)
    if not m:
        return None
    blob = m.group(1).strip()
    # trim absurdly large to avoid heavy parse
    blob = blob[:300000]
    try:
        return json.loads(blob)
    except Exception:
        return None


async def _tadawul_best_effort(client: httpx.AsyncClient, code: str) -> Dict[str, Any]:
    """
    Returns dict with some of:
      current_price, previous_close, open, day_high, day_low, volume, value_traded, name, sector, market_cap
    Best-effort only.
    """
    # Strategy A: try endpoints as HTML and look for code + nearby numbers
    for url in TADAWUL_ENDPOINTS:
        html = await _http_get_text(client, url, timeout_sec=14.0)
        if not html or len(html) < 800:
            continue

        # If JSON is embedded, try parse
        j = _extract_first_json_like(html)
        if isinstance(j, dict) and j:
            # try common keys
            flat = json.dumps(j)
            if code in flat:
                # can't guarantee schema; keep fallback to regex extraction below
                pass

        # Regex extraction near code
        txt = re.sub(r"\s+", " ", html)
        if code not in txt:
            continue

        # Try capture price patterns near the code
        # These are heuristic and may not hit; still worth trying.
        price = None
        prev = None
        vol = None

        # last price: pick first float after code within 300 chars
        m1 = re.search(rf"{re.escape(code)}(.{{0,300}}?)(\d+\.\d+|\d+)", txt)
        if m1:
            price = _safe_float(m1.group(2))

        # previous close: look for "Previous Close" labels
        m2 = re.search(r"(Previous Close|الإغلاق السابق).{0,50}?(\d+\.\d+|\d+)", txt, re.IGNORECASE)
        if m2:
            prev = _safe_float(m2.group(2))

        # volume label
        m3 = re.search(r"(Volume|حجم التداول).{0,50}?(\d[\d,\.]+)", txt, re.IGNORECASE)
        if m3:
            vol = _safe_float(m3.group(2))

        if price is not None or prev is not None or vol is not None:
            return {
                "current_price": price,
                "previous_close": prev,
                "volume": vol,
                "data_source": "tadawul_best_effort",
            }

    return {}


# ----------------------------
# Public API
# ----------------------------
async def get_enriched_quote(symbol: str) -> Dict[str, Any]:
    sym = normalize_symbol(symbol)
    if not sym:
        return {"symbol": str(symbol or ""), "market": "KSA", "currency": "SAR", "error": "Empty symbol"}

    if not is_ksa_symbol(sym):
        # legacy module is only meant for KSA; return a clean error
        return {"symbol": sym, "market": "GLOBAL", "currency": "USD", "error": "Legacy KSA engine only"}

    code = sym.split(".", 1)[0].strip()

    timeout = _timeout()
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=min(10.0, timeout)),
        follow_redirects=True,
        headers=_headers({"Referer": "https://www.tadawul.com.sa/"}),
    ) as client:
        # 1) Tadawul best-effort
        td = await _tadawul_best_effort(client, code)
        if isinstance(td, dict) and td and (td.get("current_price") is not None or td.get("previous_close") is not None):
            return {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "current_price": _safe_float(td.get("current_price")),
                "previous_close": _safe_float(td.get("previous_close")),
                "open": _safe_float(td.get("open")),
                "day_high": _safe_float(td.get("day_high")),
                "day_low": _safe_float(td.get("day_low")),
                "volume": _safe_float(td.get("volume")),
                "value_traded": _safe_float(td.get("value_traded")),
                "market_cap": _safe_float(td.get("market_cap")),
                "name": _safe_str(td.get("name")),
                "sector": _safe_str(td.get("sector")),
                "industry": _safe_str(td.get("industry")),
                "sub_sector": _safe_str(td.get("sub_sector")),
                "data_source": td.get("data_source") or "tadawul_best_effort",
                "last_updated_utc": _now_utc_iso(),
            }

        # 2) Argaam snapshot fallback
        snap = await _argaam_snapshot(client)
        row = snap.get(code) if isinstance(snap, dict) else None
        if row:
            return {
                "symbol": sym,
                "market": "KSA",
                "currency": "SAR",
                "name": _safe_str(row.get("name")),
                "current_price": _safe_float(row.get("price")),
                "price_change": _safe_float(row.get("change")),
                "percent_change": _safe_float(row.get("change_percent")),
                "volume": _safe_float(row.get("volume")),
                "value_traded": _safe_float(row.get("value_traded")),
                "data_source": "argaam_snapshot_legacy",
                "last_updated_utc": _now_utc_iso(),
            }

    return {
        "symbol": sym,
        "market": "KSA",
        "currency": "SAR",
        "error": "No KSA provider data (tadawul+argaam failed)",
        "data_source": "none",
        "last_updated_utc": _now_utc_iso(),
    }


async def get_enriched_quotes(symbols: Sequence[str]) -> List[Dict[str, Any]]:
    items = [s for s in (symbols or []) if s and str(s).strip()]
    if not items:
        return []

    # Concurrency limit to protect Render
    sem = asyncio.Semaphore(int(os.getenv("LEGACY_BATCH_CONCURRENCY", "8") or "8"))

    async def one(s: str) -> Dict[str, Any]:
        async with sem:
            try:
                return await get_enriched_quote(s)
            except Exception as exc:
                return {
                    "symbol": normalize_symbol(s) or str(s),
                    "market": "KSA",
                    "currency": "SAR",
                    "error": f"Legacy fetch error: {exc}",
                    "data_source": "none",
                    "last_updated_utc": _now_utc_iso(),
                }

    return await asyncio.gather(*(one(s) for s in items))


__all__ = ["get_enriched_quote", "get_enriched_quotes", "normalize_symbol", "is_ksa_symbol", "ENGINE_VERSION"]
