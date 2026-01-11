# routes/routes_argaam.py  (FULL REPLACEMENT) — v3.7.1
"""
routes/routes_argaam.py
===============================================================
Argaam Router (delegate) — v3.7.1
(PROD SAFE + Auth-Compat + Sheet-Rows + Schema-Aligned)

✅ v3.7.1 improvements (over v3.7.0)
- ✅ Await-safe everywhere (inspect.isawaitable instead of asyncio.iscoroutine)
- ✅ Supports per-endpoint custom headers (optional):
    • ARGAAM_HEADERS_JSON
    • ARGAAM_HEADERS_QUOTE_JSON
    • ARGAAM_HEADERS_PROFILE_JSON
- ✅ Same v3.7.0 guarantees retained:
    • Normalizes percent_change (ratio→% when needed)
    • Computes value_traded = price * volume
    • Ensures last_updated_utc + last_updated_riyadh always present (when possible)
    • Sets data_quality based on presence of current_price (OK / MISSING)
    • Schema sheet-rows fills rank + origin consistently
    • Token via X-APP-TOKEN / Authorization: Bearer / optional ?token (if ALLOW_QUERY_TOKEN=1)
    • Always returns HTTP 200 with {status, data_quality, error?} for client simplicity

Expected env vars
- APP_TOKEN / BACKUP_APP_TOKEN (optional; if none set, auth is disabled)
- ARGAAM_QUOTE_URL (optional; if missing, endpoint mode will report "not configured")
- ARGAAM_PROFILE_URL (optional)
- ARGAAM_HEADERS_JSON (optional; JSON dict for headers/cookies)
- ARGAAM_HEADERS_QUOTE_JSON (optional; JSON dict override for quote endpoint calls)
- ARGAAM_HEADERS_PROFILE_JSON (optional; JSON dict override for profile endpoint calls)
- HTTP_TIMEOUT_SEC (default 25)
- HTTP_RETRY_ATTEMPTS (default 3)
- ARGAAM_QUOTE_TTL_SEC (default 20)
- ARGAAM_META_TTL_SEC (default 21600)
- DEBUG_ERRORS=1 (optional; include debug details on failures)
- ALLOW_QUERY_TOKEN=1 (optional; allow ?token=... for quick manual testing)
- ARGAAM_SHEET_MODE=schema|compact (optional default override)
- ARGAAM_SHEET_MAX (default 500)
- ARGAAM_SHEET_CONCURRENCY (default 12)
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from cachetools import TTLCache
from fastapi import APIRouter, Header, Query, Request
from pydantic import BaseModel

logger = logging.getLogger("routes.routes_argaam")

ROUTE_VERSION = "3.7.1"
router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_SHEET_MAX = 500
DEFAULT_CONCURRENCY = 12

_KSA_TZ = timezone(timedelta(hours=3))
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _riyadh_iso(dt: Optional[datetime] = None) -> str:
    d = dt or datetime.now(timezone.utc)
    return d.astimezone(_KSA_TZ).isoformat()


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        ss = str(s).strip()
        if ss.endswith("Z"):
            ss = ss[:-1] + "+00:00"
        return datetime.fromisoformat(ss)
    except Exception:
        return None


def _utc_to_riyadh_iso(utc_iso: Optional[str]) -> Optional[str]:
    d = _parse_iso(utc_iso)
    if not d:
        return None
    try:
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(_KSA_TZ).isoformat()
    except Exception:
        return None


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
        s = s.replace("%", "").replace(",", "").replace("+", "").strip()

        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].strip()

        m = re.match(r"^(-?\d+(\.\d+)?)([KMB])$", s, re.IGNORECASE)
        mult = 1.0
        if m:
            num = m.group(1)
            suf = m.group(3).upper()
            mult = 1_000.0 if suf == "K" else 1_000_000.0 if suf == "M" else 1_000_000_000.0
            s = num

        f = float(s) * mult
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def _normalize_ksa_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip().upper()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    if s.isdigit() or re.match(r"^\d{3,6}$", s):
        return f"{s}.SR"
    return s.replace(" ", "")


def _ksa_code(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s.endswith(".SR"):
        s = s[:-3]
    if "." in s:
        s = s.split(".", 1)[0]
    return s


def _format_url(template: str, symbol: str) -> str:
    code = _ksa_code(symbol)
    return (template or "").replace("{code}", code).replace("{symbol}", symbol)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# =============================================================================
# Auth helpers (APP_TOKEN / BACKUP_APP_TOKEN)
# =============================================================================
def _extract_token(
    x_app_token: Optional[str],
    authorization: Optional[str],
    token_qs: Optional[str],
) -> Optional[str]:
    t = (x_app_token or "").strip()
    if t:
        return t

    auth = (authorization or "").strip()
    if auth:
        low = auth.lower()
        if low.startswith("bearer "):
            return auth.split(" ", 1)[1].strip() or None
        return auth.strip() or None

    # optional: allow query token only if explicitly enabled
    if _truthy(os.getenv("ALLOW_QUERY_TOKEN", "0")):
        tq = (token_qs or "").strip()
        if tq:
            return tq

    return None


def _auth_ok(provided_token: Optional[str]) -> bool:
    required = (os.getenv("APP_TOKEN") or "").strip()
    backup = (os.getenv("BACKUP_APP_TOKEN") or "").strip()

    if not required and not backup:
        return True  # auth disabled

    pt = (provided_token or "").strip()
    if not pt:
        return False

    return (required and pt == required) or (backup and pt == backup)


# =============================================================================
# Optional endpoint config
# =============================================================================
def _headers_from_env(*keys: str) -> Dict[str, str]:
    """
    Merges ARGAAM_HEADERS_JSON with an optional per-endpoint override.
    Later keys override earlier keys.
    """
    hdrs: Dict[str, str] = {}

    def load_env(k: str) -> None:
        raw = (os.getenv(k) or "").strip()
        if not raw:
            return
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for kk, vv in obj.items():
                    hdrs[str(kk)] = str(vv)
        except Exception:
            return

    load_env("ARGAAM_HEADERS_JSON")
    for k in keys:
        if k:
            load_env(k)
    return hdrs


ARGAAM_QUOTE_URL = _safe_str(os.getenv("ARGAAM_QUOTE_URL"))
ARGAAM_PROFILE_URL = _safe_str(os.getenv("ARGAAM_PROFILE_URL"))

HTTP_TIMEOUT_SEC = float(_safe_float(os.getenv("HTTP_TIMEOUT_SEC")) or DEFAULT_TIMEOUT_SEC)
HTTP_RETRY_ATTEMPTS = int(_safe_float(os.getenv("HTTP_RETRY_ATTEMPTS")) or DEFAULT_RETRY_ATTEMPTS)

_quote_cache = TTLCache(
    maxsize=4000,
    ttl=max(5.0, float(_safe_float(os.getenv("ARGAAM_QUOTE_TTL_SEC")) or 20.0)),
)

_meta_cache = TTLCache(
    maxsize=2500,
    ttl=max(120.0, float(_safe_float(os.getenv("ARGAAM_META_TTL_SEC")) or 21600.0)),
)

_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                timeout = httpx.Timeout(HTTP_TIMEOUT_SEC, connect=min(10.0, HTTP_TIMEOUT_SEC))
                _client = httpx.AsyncClient(
                    timeout=timeout,
                    follow_redirects=True,
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.8,ar;q=0.6",
                    },
                    limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
                )
    return _client


# Optional: close client on shutdown (best-effort)
try:

    @router.on_event("shutdown")  # type: ignore[attr-defined]
    async def _shutdown() -> None:
        global _client
        c = _client
        _client = None
        try:
            if c is not None:
                await c.aclose()
        except Exception:
            pass

except Exception:
    pass


async def _fetch_url(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[Optional[Union[Dict[str, Any], List[Any]]], Optional[str], int]:
    """
    Returns (json_or_list_or_none, raw_text_or_none, status_code)
    """
    c = await _get_client()

    for attempt in range(max(1, HTTP_RETRY_ATTEMPTS)):
        try:
            r = await c.get(url, headers=headers)
            sc = int(r.status_code)

            if sc == 429 or 500 <= sc < 600:
                if attempt < (HTTP_RETRY_ATTEMPTS - 1):
                    ra = r.headers.get("Retry-After")
                    if ra and ra.strip().isdigit():
                        await asyncio.sleep(min(2.0, float(ra.strip())))
                    else:
                        await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                    continue
                return None, None, sc

            if sc >= 400:
                return None, (r.text or "").strip() or None, sc

            try:
                return r.json(), None, sc
            except Exception:
                txt = (r.text or "").strip()
                if txt.startswith("{") or txt.startswith("["):
                    try:
                        return json.loads(txt), None, sc
                    except Exception:
                        return None, txt or None, sc
                return None, txt or None, sc

        except Exception as exc:
            if attempt < (HTTP_RETRY_ATTEMPTS - 1):
                await asyncio.sleep(0.25 * (2**attempt) + random.random() * 0.35)
                continue
            return None, str(exc), 0

    return None, None, 0


def _coerce_dict(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


def _pick_num(obj: Any, *keys: str) -> Optional[float]:
    if not isinstance(obj, (dict, list)):
        return None

    keyset = {k.lower() for k in keys if k}
    seen = set()

    def walk(x: Any, depth: int) -> Any:
        if x is None or depth < 0:
            return None
        xid = id(x)
        if xid in seen:
            return None
        seen.add(xid)

        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).strip().lower() in keyset:
                    return v
            for v in x.values():
                r = walk(v, depth - 1)
                if r is not None:
                    return r
        elif isinstance(x, list):
            for it in x:
                r = walk(it, depth - 1)
                if r is not None:
                    return r
        return None

    return _safe_float(walk(obj, 6))


def _pick_str(obj: Any, *keys: str) -> Optional[str]:
    if not isinstance(obj, (dict, list)):
        return None

    keyset = {k.lower() for k in keys if k}
    seen = set()

    def walk(x: Any, depth: int) -> Any:
        if x is None or depth < 0:
            return None
        xid = id(x)
        if xid in seen:
            return None
        seen.add(xid)

        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).strip().lower() in keyset:
                    return v
            for v in x.values():
                r = walk(v, depth - 1)
                if r is not None:
                    return r
        elif isinstance(x, list):
            for it in x:
                r = walk(it, depth - 1)
                if r is not None:
                    return r
        return None

    return _safe_str(walk(obj, 6))


def _compute_change(price: Optional[float], prev: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if price is None or prev in (None, 0):
        return None, None
    try:
        ch = float(price) - float(prev)
        pct = (ch / float(prev)) * 100.0
        if math.isnan(pct) or math.isinf(pct):
            return ch, None
        return ch, pct
    except Exception:
        return None, None


def _normalize_pct(pct: Any) -> Optional[float]:
    """
    Accepts either:
    - percent value (e.g. 1.25 means 1.25%)
    - ratio value (e.g. 0.0125 means 1.25%)
    Returns percent.
    """
    f = _safe_float(pct)
    if f is None:
        return None
    try:
        if -1.2 <= f <= 1.2:
            return f * 100.0
        return f
    except Exception:
        return f


def _compute_value_traded(price: Any, volume: Any) -> Optional[float]:
    p = _safe_float(price)
    v = _safe_float(volume)
    if p is None or v is None:
        return None
    try:
        return round(p * v, 6)
    except Exception:
        return None


# =============================================================================
# Engine/provider integration (best-effort)
# =============================================================================
async def _try_engine_argaam(request: Request, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    eng = getattr(getattr(request.app, "state", None), "engine", None)
    if eng is None:
        return {}, "no engine"

    for meth in (
        "get_argaam_quote_patch",
        "fetch_argaam_quote_patch",
        "argaam_quote_patch",
        "get_argaam_patch",
    ):
        fn = getattr(eng, meth, None)
        if callable(fn):
            try:
                res = fn(symbol)
                res = await _maybe_await(res)
                if isinstance(res, tuple) and len(res) == 2:
                    patch, err = res
                    return (patch or {}), err
                if isinstance(res, dict):
                    return res, None
            except Exception as exc:
                return {}, f"engine.{meth} failed: {exc.__class__.__name__}: {exc}"

    return {}, "engine has no argaam method"


async def _try_provider_module(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    try:
        import importlib

        m = importlib.import_module("core.providers.argaam_provider")
        fn = getattr(m, "fetch_quote_patch", None)
        if callable(fn):
            res = fn(symbol)
            res = await _maybe_await(res)
            if isinstance(res, tuple) and len(res) == 2:
                patch, err = res
                return (patch or {}), err
            if isinstance(res, dict):
                return res, None
        return {}, "core.providers.argaam_provider has no fetch_quote_patch"
    except Exception:
        return {}, "argaam provider module not available"


async def _try_profile_name(symbol: str) -> Optional[str]:
    """
    Optional best-effort profile fetch for company name (cached).
    """
    if not ARGAAM_PROFILE_URL:
        return None

    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return None

    ck = f"m::{sym}"
    hit = _meta_cache.get(ck)
    if isinstance(hit, dict):
        nm = _safe_str(hit.get("name"))
        if nm:
            return nm

    url = _format_url(ARGAAM_PROFILE_URL, sym)
    hdrs = _headers_from_env("ARGAAM_HEADERS_PROFILE_JSON") or None
    data, raw, sc = await _fetch_url(url, headers=hdrs)
    if sc >= 400:
        return None

    name: Optional[str] = None
    if isinstance(data, (dict, list)):
        root = _coerce_dict(data)
        name = _pick_str(root, "name", "company", "company_name", "companyName", "shortName", "longName")
    elif raw:
        m = re.search(r"<title>\s*(.*?)\s*</title>", raw, re.IGNORECASE)
        if m:
            name = _safe_str(m.group(1))

    if name:
        _meta_cache[ck] = {"name": name, "ts": _utc_iso()}
    return name


async def _try_configured_endpoints(symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return {}, "empty symbol"

    if not ARGAAM_QUOTE_URL:
        return {}, "not configured (ARGAAM_QUOTE_URL not set)"

    ck = f"q::{sym}"
    hit = _quote_cache.get(ck)
    if isinstance(hit, dict) and hit:
        return dict(hit), None

    url = _format_url(ARGAAM_QUOTE_URL, sym)
    hdrs = _headers_from_env("ARGAAM_HEADERS_QUOTE_JSON") or None

    t0 = time.perf_counter()
    data, raw, sc = await _fetch_url(url, headers=hdrs)
    dt_ms = int((time.perf_counter() - t0) * 1000)

    if isinstance(data, (dict, list)):
        root = _coerce_dict(data)

        price = _pick_num(root, "price", "last", "last_price", "close", "current", "c", "regularMarketPrice")
        prev = _pick_num(root, "previous_close", "previousClose", "pc", "prevClose")
        vol = _pick_num(root, "volume", "vol", "tradedVolume")
        mcap = _pick_num(root, "market_cap", "marketCap", "marketcap")

        name = _pick_str(root, "name", "company", "company_name", "companyName", "shortName", "longName")
        if not name:
            name = await _try_profile_name(sym)

        ch, pct = _compute_change(price, prev)

        now_utc = _utc_iso()
        patch = {
            "origin": "KSA_TADAWUL",
            "market": "KSA",
            "currency": "SAR",
            "name": name,
            "current_price": price,
            "previous_close": prev,
            "price_change": ch,
            "percent_change": pct,
            "volume": vol,
            "market_cap": mcap,
            "value_traded": _compute_value_traded(price, vol),
            "data_source": "argaam",
            "last_updated_utc": now_utc,
            "last_updated_riyadh": _utc_to_riyadh_iso(now_utc),
        }

        if patch.get("current_price") is not None:
            _quote_cache[ck] = dict(patch)
            return patch, None

        return {}, f"argaam endpoint json had no price ({dt_ms}ms)"

    return {}, f"argaam endpoint non-json ({sc}) ({dt_ms}ms): {(raw or '')[:220]}"


def _base_ok(symbol: str) -> Dict[str, Any]:
    now = _utc_iso()
    return {
        "status": "ok",
        "symbol": _normalize_ksa_symbol(symbol),
        "origin": "KSA_TADAWUL",
        "market": "KSA",
        "currency": "SAR",
        "data_source": "argaam",
        "data_quality": "OK",
        "error": "",
        "route_version": ROUTE_VERSION,
        "time_utc": now,
        "time_riyadh": _utc_to_riyadh_iso(now) or _riyadh_iso(),
        "last_updated_utc": now,
        "last_updated_riyadh": _utc_to_riyadh_iso(now) or _riyadh_iso(),
    }


def _base_err(symbol: str, err: str, quality: str = "MISSING") -> Dict[str, Any]:
    now = _utc_iso()
    return {
        "status": "error",
        "symbol": _normalize_ksa_symbol(symbol),
        "origin": "KSA_TADAWUL",
        "market": "KSA",
        "currency": "SAR",
        "data_source": "argaam",
        "data_quality": quality,
        "error": err or "unknown error",
        "route_version": ROUTE_VERSION,
        "time_utc": now,
        "time_riyadh": _utc_to_riyadh_iso(now) or _riyadh_iso(),
        "last_updated_utc": now,
        "last_updated_riyadh": _utc_to_riyadh_iso(now) or _riyadh_iso(),
    }


def _debug_enabled(debug: int) -> bool:
    return _truthy(os.getenv("DEBUG_ERRORS", "0")) or bool(int(debug or 0))


def _finalize_quote(sym: str, out: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensures:
    - percent_change in % (not ratio)
    - value_traded computed
    - last_updated_utc/riyadh present
    - data_quality consistent with current_price
    """
    try:
        out["symbol"] = _normalize_ksa_symbol(sym) or out.get("symbol") or sym
        out.setdefault("origin", "KSA_TADAWUL")
        out.setdefault("market", "KSA")
        out.setdefault("currency", "SAR")
        out.setdefault("data_source", "argaam")

        # timestamps
        if not out.get("last_updated_utc"):
            out["last_updated_utc"] = out.get("time_utc") or _utc_iso()
        if not out.get("last_updated_riyadh"):
            out["last_updated_riyadh"] = (
                _utc_to_riyadh_iso(str(out.get("last_updated_utc")))
                or out.get("time_riyadh")
                or _riyadh_iso()
            )

        # normalize percent_change
        if out.get("percent_change") is not None:
            out["percent_change"] = _normalize_pct(out.get("percent_change"))

        # compute change if missing
        if out.get("price_change") is None or out.get("percent_change") is None:
            ch, pct = _compute_change(_safe_float(out.get("current_price")), _safe_float(out.get("previous_close")))
            if out.get("price_change") is None:
                out["price_change"] = ch
            if out.get("percent_change") is None:
                out["percent_change"] = pct

        # compute value_traded
        if out.get("value_traded") is None:
            out["value_traded"] = _compute_value_traded(out.get("current_price"), out.get("volume"))

        # quality
        if _safe_float(out.get("current_price")) is None:
            out["status"] = "error"
            out["data_quality"] = "MISSING"
            if not str(out.get("error") or "").strip():
                out["error"] = "missing price"
        else:
            out.setdefault("status", "ok")
            out["data_quality"] = out.get("data_quality") or "OK"
            out["error"] = str(out.get("error") or "")

        return out
    except Exception:
        # never raise
        return out


# =============================================================================
# Request models
# =============================================================================
class SheetRowsIn(BaseModel):
    tickers: Optional[List[str]] = None
    symbols: Optional[List[str]] = None
    sheet_name: Optional[str] = None


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    eng = getattr(getattr(request.app, "state", None), "engine", None)
    auth_required = bool((os.getenv("APP_TOKEN") or "").strip() or (os.getenv("BACKUP_APP_TOKEN") or "").strip())
    return {
        "status": "ok",
        "module": "routes.routes_argaam",
        "route_version": ROUTE_VERSION,
        "engine_available": bool(eng is not None),
        "engine_type": (eng.__class__.__name__ if eng is not None else ""),
        "auth_required": auth_required,
        "ARGAAM_QUOTE_URL_set": bool(ARGAAM_QUOTE_URL),
        "ARGAAM_PROFILE_URL_set": bool(ARGAAM_PROFILE_URL),
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
    }


async def _get_quote_payload(
    request: Request,
    symbol: str,
    *,
    debug: int = 0,
) -> Dict[str, Any]:
    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return _base_err(symbol, "empty symbol")

    dbg = _debug_enabled(debug)
    debug_info: Dict[str, Any] = {}

    # Priority: engine -> provider -> configured endpoint
    patch, err = await _try_engine_argaam(request, sym)
    if patch:
        out = _base_ok(sym)
        out.update({k: v for k, v in patch.items() if v is not None})
        if out.get("last_updated_utc") and not out.get("last_updated_riyadh"):
            out["last_updated_riyadh"] = _utc_to_riyadh_iso(str(out.get("last_updated_utc")))
        return _finalize_quote(sym, out)
    debug_info["engine"] = err

    patch, err = await _try_provider_module(sym)
    if patch:
        out = _base_ok(sym)
        out.update({k: v for k, v in patch.items() if v is not None})
        if out.get("last_updated_utc") and not out.get("last_updated_riyadh"):
            out["last_updated_riyadh"] = _utc_to_riyadh_iso(str(out.get("last_updated_utc")))
        return _finalize_quote(sym, out)
    debug_info["provider"] = err

    patch, err = await _try_configured_endpoints(sym)
    if patch:
        out = _base_ok(sym)
        out.update({k: v for k, v in patch.items() if v is not None})
        if out.get("last_updated_utc") and not out.get("last_updated_riyadh"):
            out["last_updated_riyadh"] = _utc_to_riyadh_iso(str(out.get("last_updated_utc")))
        return _finalize_quote(sym, out)
    debug_info["endpoint"] = err

    msg = "Argaam quote unavailable."
    if not ARGAAM_QUOTE_URL:
        msg = "Argaam not configured (ARGAAM_QUOTE_URL not set)."

    out = _base_err(sym, msg, quality="MISSING")
    if dbg:
        out["debug"] = debug_info
        out["hint"] = "Set ARGAAM_QUOTE_URL (and optional ARGAAM_HEADERS_JSON) OR add core.providers.argaam_provider."
    return _finalize_quote(sym, out)


@router.get("/quote")
async def quote(
    request: Request,
    symbol: str = Query(..., description="KSA symbol (e.g., 1120.SR or 1120)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None, description="(optional) token via query if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(default=0, description="1 to include debug details (or set DEBUG_ERRORS=1)"),
) -> Dict[str, Any]:
    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return _base_err(symbol, "empty symbol")

    provided = _extract_token(x_app_token, authorization, token)
    if not _auth_ok(provided):
        return _base_err(sym, "Unauthorized: invalid or missing token (X-APP-TOKEN or Authorization: Bearer)", quality="BLOCKED")

    return await _get_quote_payload(request, sym, debug=debug)


@router.get("/quotes")
async def quotes(
    request: Request,
    symbols: str = Query(..., description="Comma-separated KSA symbols e.g. 1120.SR,2222.SR"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None),
    debug: int = Query(default=0),
) -> Dict[str, Any]:
    provided = _extract_token(x_app_token, authorization, token)
    if not _auth_ok(provided):
        return {"status": "error", "error": "Unauthorized: invalid or missing token", "items": [], "route_version": ROUTE_VERSION}

    arr = [s.strip() for s in (symbols or "").split(",") if s.strip()]
    arr2: List[str] = []
    seen = set()
    for x in arr:
        ns = _normalize_ksa_symbol(x)
        if not ns:
            continue
        if ns in seen:
            continue
        seen.add(ns)
        arr2.append(ns)
    arr2 = arr2[:50]

    concurrency = int(_safe_float(os.getenv("ARGAAM_SHEET_CONCURRENCY")) or DEFAULT_CONCURRENCY)
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(sym: str) -> Dict[str, Any]:
        async with sem:
            return await _get_quote_payload(request, sym, debug=debug)

    items = await asyncio.gather(*[one(s) for s in arr2])
    return {"status": "ok", "count": len(items), "items": items, "route_version": ROUTE_VERSION, "time_utc": _utc_iso()}


# --------------------------------------------------------------------------
# Sheet Rows
# --------------------------------------------------------------------------
SHEET_HEADERS_COMPACT: List[str] = [
    "Symbol",
    "Name",
    "Market",
    "Currency",
    "Current Price",
    "Previous Close",
    "Change",
    "Change %",
    "Volume",
    "Market Cap",
    "Value Traded",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Error",
]


def _to_sheet_row_compact(q: Dict[str, Any]) -> List[Any]:
    def g(*keys: str) -> Any:
        for k in keys:
            if k in q:
                return q.get(k)
        return None

    return [
        g("symbol"),
        g("name"),
        g("market"),
        g("currency"),
        g("current_price", "price"),
        g("previous_close"),
        g("price_change", "change"),
        g("percent_change"),
        g("volume"),
        g("market_cap"),
        g("value_traded"),
        g("data_source"),
        g("data_quality"),
        g("last_updated_utc", "time_utc"),
        g("last_updated_riyadh", "time_riyadh"),
        g("error"),
    ]


def _sheet_mode(payload: SheetRowsIn, mode: Optional[str]) -> str:
    # explicit mode first
    m = (mode or "").strip().lower()
    if m in {"schema", "compact"}:
        return m

    # env default override
    em = (os.getenv("ARGAAM_SHEET_MODE") or "").strip().lower()
    if em in {"schema", "compact"}:
        return em

    # default decision
    return "schema" if (payload.sheet_name or "").strip() else "compact"


def _build_schema_rows(
    items: List[Dict[str, Any]],
    *,
    sheet_name: str,
) -> Tuple[List[str], List[List[Any]]]:
    """
    Returns (headers, rows) aligned to core.schemas.get_headers_for_sheet(sheet_name).
    Missing fields are returned as None (safe for Sheets).
    """
    try:
        from core.schemas import get_headers_for_sheet, header_field_candidates  # type: ignore

        headers = get_headers_for_sheet(sheet_name)
        rows: List[List[Any]] = []

        origin_label = "KSA_TADAWUL"

        for idx, it in enumerate(items, start=1):
            q = dict(it or {})
            q.setdefault("rank", idx)
            q.setdefault("origin", origin_label)

            # ensure timestamps in both zones when possible
            if q.get("last_updated_utc") and not q.get("last_updated_riyadh"):
                q["last_updated_riyadh"] = _utc_to_riyadh_iso(str(q.get("last_updated_utc")))

            # ensure percent_change normalization
            if q.get("percent_change") is not None:
                q["percent_change"] = _normalize_pct(q.get("percent_change"))

            # ensure value_traded
            if q.get("value_traded") is None:
                q["value_traded"] = _compute_value_traded(q.get("current_price"), q.get("volume"))

            row: List[Any] = []
            for h in headers:
                val = None
                for f in header_field_candidates(h):
                    if f in q and q.get(f) is not None:
                        val = q.get(f)
                        break
                row.append(val)
            rows.append(row)

        return headers, rows
    except Exception as exc:
        logger.warning("schema row builder failed; fallback to compact: %s", exc)
        return SHEET_HEADERS_COMPACT, [_to_sheet_row_compact(it) for it in items]


@router.post("/sheet-rows")
async def sheet_rows(
    request: Request,
    payload: SheetRowsIn,
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None),
    debug: int = Query(default=0),
    mode: Optional[str] = Query(default=None, description="schema|compact (optional)"),
) -> Dict[str, Any]:
    provided = _extract_token(x_app_token, authorization, token)
    eff_mode = _sheet_mode(payload, mode)

    if not _auth_ok(provided):
        return {
            "status": "error",
            "mode": eff_mode,
            "headers": SHEET_HEADERS_COMPACT,
            "rows": [],
            "count": 0,
            "error": "Unauthorized: invalid or missing token",
            "route_version": ROUTE_VERSION,
            "time_utc": _utc_iso(),
        }

    syms = (payload.tickers or []) + (payload.symbols or [])
    normed: List[str] = []
    seen = set()
    for s in syms:
        ns = _normalize_ksa_symbol(str(s or ""))
        if not ns:
            continue
        if ns in seen:
            continue
        seen.add(ns)
        normed.append(ns)

    max_n = int(_safe_float(os.getenv("ARGAAM_SHEET_MAX")) or DEFAULT_SHEET_MAX)
    normed = normed[: max(1, max_n)]

    if not normed:
        return {
            "status": "ok",
            "mode": eff_mode,
            "headers": SHEET_HEADERS_COMPACT,
            "rows": [],
            "count": 0,
            "route_version": ROUTE_VERSION,
            "time_utc": _utc_iso(),
            "error": "",
        }

    # concurrency-limited fetch
    concurrency = int(_safe_float(os.getenv("ARGAAM_SHEET_CONCURRENCY")) or DEFAULT_CONCURRENCY)
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(sym: str) -> Dict[str, Any]:
        async with sem:
            return await _get_quote_payload(request, sym, debug=debug)

    items: List[Dict[str, Any]] = await asyncio.gather(*[one(s) for s in normed])

    # clearer top-level message when not configured
    top_err = ""
    if not ARGAAM_QUOTE_URL:
        top_err = "Argaam not configured (ARGAAM_QUOTE_URL not set)."

    if eff_mode == "schema":
        sheet_name = (payload.sheet_name or "KSA_Tadawul").strip()
        headers, rows = _build_schema_rows(items, sheet_name=sheet_name)
    else:
        headers = SHEET_HEADERS_COMPACT
        rows = [_to_sheet_row_compact(it) for it in items]

    return {
        "status": "ok",
        "mode": eff_mode,
        "headers": headers,
        "rows": rows,
        "count": len(rows),
        "route_version": ROUTE_VERSION,
        "time_utc": _utc_iso(),
        "error": top_err,
    }


__all__ = ["router"]
