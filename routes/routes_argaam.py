# routes/routes_argaam.py  (FULL REPLACEMENT)
"""
routes/routes_argaam.py
===============================================================
Argaam Router (delegate) — v3.4.0 (PROD SAFE + Auth-Compat + Cleaner Errors)

Improvements in v3.4.0
- ✅ Accepts token via X-APP-TOKEN OR Authorization: Bearer <token>
- ✅ Cleaner "not configured" messaging when ARGAAM_QUOTE_URL is missing
- ✅ Keeps PROD-safe import behavior (no network calls at import time)
- ✅ Best-effort: engine -> provider module -> configured endpoint

Behavior
- Always returns HTTP 200 with {status, symbol, data_source, data_quality, error?}
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from cachetools import TTLCache
from fastapi import APIRouter, Header, Query, Request

logger = logging.getLogger("routes.routes_argaam")

ROUTE_VERSION = "3.4.0"
router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SEC = 25.0
DEFAULT_RETRY_ATTEMPTS = 3

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    if s.isdigit() or re.match(r"^\d{3,6}$", s):
        return f"{s}.SR"
    return s


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


# ---------------------------
# Auth helpers (APP_TOKEN / BACKUP_APP_TOKEN)
# ---------------------------
def _extract_token(x_app_token: Optional[str], authorization: Optional[str]) -> Optional[str]:
    t = (x_app_token or "").strip()
    if t:
        return t

    auth = (authorization or "").strip()
    if not auth:
        return None

    # Bearer <token>
    low = auth.lower()
    if low.startswith("bearer "):
        return auth.split(" ", 1)[1].strip() or None

    # Raw token (rare but allow)
    return auth or None


def _auth_ok(provided_token: Optional[str]) -> bool:
    required = (os.getenv("APP_TOKEN") or "").strip()
    backup = (os.getenv("BACKUP_APP_TOKEN") or "").strip()

    if not required and not backup:
        return True  # auth disabled

    pt = (provided_token or "").strip()
    if not pt:
        return False

    return (required and pt == required) or (backup and pt == backup)


# ---------------------------
# Optional endpoint config
# ---------------------------
def _headers_from_env() -> Dict[str, str]:
    hdrs: Dict[str, str] = {}
    raw = (os.getenv("ARGAAM_HEADERS_JSON") or "").strip()
    if raw:
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                hdrs.update({str(k): str(v) for k, v in obj.items()})
        except Exception:
            pass
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


async def _fetch_url(url: str) -> Tuple[Optional[Union[Dict[str, Any], List[Any]]], Optional[str], int]:
    """
    Returns (json_or_list_or_none, raw_text_or_none, status_code)
    """
    headers = _headers_from_env() or None
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

    return _safe_float(walk(obj, 5))


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

    return _safe_str(walk(obj, 5))


# ---------------------------
# Engine/provider integration (best-effort)
# ---------------------------
async def _try_engine_argaam(request: Request, symbol: str) -> Tuple[Dict[str, Any], Optional[str]]:
    eng = getattr(request.app.state, "engine", None)
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
                if asyncio.iscoroutine(res):
                    res = await res
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
            if asyncio.iscoroutine(res):
                res = await res
            if isinstance(res, tuple) and len(res) == 2:
                patch, err = res
                return (patch or {}), err
            if isinstance(res, dict):
                return res, None
        return {}, "core.providers.argaam_provider has no fetch_quote_patch"
    except Exception:
        return {}, "argaam provider module not available"


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
    t0 = time.perf_counter()
    data, raw, sc = await _fetch_url(url)
    dt_ms = int((time.perf_counter() - t0) * 1000)

    if isinstance(data, (dict, list)):
        root = _coerce_dict(data)

        price = _pick_num(root, "price", "last", "last_price", "close", "current", "c", "regularMarketPrice")
        prev = _pick_num(root, "previous_close", "previousClose", "pc", "prevClose")
        vol = _pick_num(root, "volume", "vol", "tradedVolume")

        name = _pick_str(root, "name", "company", "company_name", "companyName", "shortName", "longName")

        patch = {
            "market": "KSA",
            "currency": "SAR",
            "name": name,
            "current_price": price,
            "previous_close": prev,
            "volume": vol,
            "data_source": "argaam",
            "last_updated_utc": _utc_iso(),
        }

        if patch.get("current_price") is not None:
            _quote_cache[ck] = dict(patch)
            return patch, None

        return {}, f"argaam endpoint json had no price ({dt_ms}ms)"

    return {}, f"argaam endpoint non-json ({sc}) ({dt_ms}ms): {(raw or '')[:220]}"


def _base_ok(symbol: str) -> Dict[str, Any]:
    return {
        "status": "ok",
        "symbol": _normalize_ksa_symbol(symbol),
        "market": "KSA",
        "currency": "SAR",
        "data_source": "argaam",
        "data_quality": "OK",
        "error": "",
        "route_version": ROUTE_VERSION,
        "time_utc": _utc_iso(),
    }


def _base_err(symbol: str, err: str, quality: str = "MISSING") -> Dict[str, Any]:
    return {
        "status": "error",
        "symbol": _normalize_ksa_symbol(symbol),
        "market": "KSA",
        "currency": "SAR",
        "data_source": "argaam",
        "data_quality": quality,
        "error": err or "unknown error",
        "route_version": ROUTE_VERSION,
        "time_utc": _utc_iso(),
    }


@router.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    eng = getattr(request.app.state, "engine", None)
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
    }


@router.get("/quote")
async def quote(
    request: Request,
    symbol: str = Query(..., description="KSA symbol (e.g., 1120.SR or 1120)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    debug: int = Query(default=0, description="1 to include debug details"),
) -> Dict[str, Any]:
    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return _base_err(symbol, "empty symbol")

    token = _extract_token(x_app_token, authorization)
    if not _auth_ok(token):
        return _base_err(sym, "Unauthorized: invalid or missing token (X-APP-TOKEN or Authorization: Bearer)", quality="BLOCKED")

    debug_info: Dict[str, Any] = {}

    # Priority: engine -> provider -> configured endpoint
    patch, err = await _try_engine_argaam(request, sym)
    if patch:
        out = _base_ok(sym)
        out.update({k: v for k, v in patch.items() if v is not None})
        return out
    debug_info["engine"] = err

    patch, err = await _try_provider_module(sym)
    if patch:
        out = _base_ok(sym)
        out.update({k: v for k, v in patch.items() if v is not None})
        return out
    debug_info["provider"] = err

    patch, err = await _try_configured_endpoints(sym)
    if patch:
        out = _base_ok(sym)
        out.update({k: v for k, v in patch.items() if v is not None})
        return out
    debug_info["endpoint"] = err

    # final
    msg = "Argaam quote unavailable."
    if not ARGAAM_QUOTE_URL:
        msg = "Argaam not configured (ARGAAM_QUOTE_URL not set)."

    out = _base_err(sym, msg, quality="MISSING")
    if int(debug or 0) == 1:
        out["debug"] = debug_info
        out["hint"] = "Set ARGAAM_QUOTE_URL (and optional ARGAAM_HEADERS_JSON) OR add core.providers.argaam_provider."
    return out


@router.get("/quotes")
async def quotes(
    request: Request,
    symbols: str = Query(..., description="Comma-separated KSA symbols e.g. 1120.SR,2222.SR"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    debug: int = Query(default=0),
) -> Dict[str, Any]:
    token = _extract_token(x_app_token, authorization)
    if not _auth_ok(token):
        return {"status": "error", "error": "Unauthorized: invalid or missing token", "items": []}

    arr = [s.strip() for s in (symbols or "").split(",") if s.strip()]
    arr = arr[:50]

    items: List[Dict[str, Any]] = []
    for s in arr:
        items.append(
            await quote(
                request,
                symbol=s,
                x_app_token=x_app_token,
                authorization=authorization,
                debug=debug,
            )
        )

    return {"status": "ok", "count": len(items), "items": items, "route_version": ROUTE_VERSION}


__all__ = ["router"]
