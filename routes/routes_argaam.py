# routes/routes_argaam.py
"""
routes/routes_argaam.py
===============================================================
Argaam Router (delegate) — FULL REPLACEMENT — v4.4.0
(PROD SAFE + CONFIG-AWARE AUTH + SCORING + SCHEMA MAPPING + RIYADH TIME)

What this route does
- ✅ Fetches KSA quotes from Argaam (URL template via env)
- ✅ Normalizes symbols (core.symbols.normalize > fallback)
- ✅ Optional scoring (core.scoring_engine.enrich_with_scores)
- ✅ Schema-aware mapping (core.enriched_quote.EnrichedQuote + core.schemas.get_headers_for_sheet)
- ✅ GAS-safe outputs: stable headers + rectangular rows
- ✅ Robust batching: mixed success/failure never crashes
- ✅ Config-aware auth header name (routes.config.AUTH_HEADER_NAME / settings.auth_header_name)
- ✅ Open mode if no tokens configured (routes.config.allowed_tokens or env tokens empty)

Endpoints
- GET  /v1/argaam/health
- GET  /v1/argaam/quote?symbol=1120.SR
- GET  /v1/argaam/quotes?symbols=1120.SR,2222.SR
- POST /v1/argaam/sheet-rows   (returns {status, headers, rows, count, meta})

Required env
- ARGAAM_QUOTE_URL : URL template with {code} or {symbol}
  Examples:
    https://api.example.com/argaam/quote/{code}
    https://api.example.com/argaam/quote?symbol={symbol}

Optional env
- APP_TOKEN / BACKUP_APP_TOKEN / TFB_APP_TOKEN
- ALLOW_QUERY_TOKEN=1
- ARGAAM_TIMEOUT_SEC=25
- ARGAAM_RETRY_ATTEMPTS=3
- ARGAAM_CONCURRENCY=12
- ARGAAM_SHEET_MAX=500
- ARGAAM_CACHE_TTL_SEC=20
- ARGAAM_CACHE_MAX=4000
- ARGAAM_USER_AGENT=...
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Header, Query, Request
from fastapi.responses import JSONResponse

# Pydantic v2 preferred, v1 fallback
try:
    from pydantic import BaseModel, ConfigDict, Field  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    from pydantic import BaseModel, Field  # type: ignore
    ConfigDict = None  # type: ignore
    _PYDANTIC_V2 = False

logger = logging.getLogger("routes.routes_argaam")

ROUTE_VERSION = "4.4.0"
router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


# =============================================================================
# Safe dependency imports (never crash app import)
# =============================================================================
@lru_cache(maxsize=1)
def _httpx():
    try:
        import httpx  # type: ignore
        return httpx
    except Exception:
        return None


class _TinyTTLCache:
    """
    Minimal TTL cache fallback if cachetools is unavailable.
    """
    def __init__(self, maxsize: int, ttl: float):
        self.maxsize = max(10, int(maxsize))
        self.ttl = max(1.0, float(ttl))
        self._d: Dict[str, Tuple[float, Any]] = {}

    def get(self, k: str) -> Any:
        now = time.time()
        it = self._d.get(k)
        if not it:
            return None
        exp, val = it
        if now > exp:
            self._d.pop(k, None)
            return None
        return val

    def set(self, k: str, v: Any) -> None:
        now = time.time()
        # simple eviction if over maxsize
        if len(self._d) >= self.maxsize:
            # drop a random key (fast + good enough)
            try:
                self._d.pop(next(iter(self._d.keys())), None)
            except Exception:
                self._d.clear()
        self._d[k] = (now + self.ttl, v)


@lru_cache(maxsize=1)
def _cache_impl():
    try:
        from cachetools import TTLCache  # type: ignore
        return TTLCache
    except Exception:
        return None


# =============================================================================
# Config/auth integration (routes.config preferred)
# =============================================================================
@lru_cache(maxsize=1)
def _routes_config_mod():
    try:
        import routes.config as rc  # type: ignore
        return rc
    except Exception:
        return None


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _auth_header_name() -> str:
    rc = _routes_config_mod()
    name = None
    if rc is not None:
        name = getattr(rc, "AUTH_HEADER_NAME", None)
        if not name:
            try:
                gs = getattr(rc, "get_settings", None)
                if callable(gs):
                    s = gs()
                    name = getattr(s, "auth_header_name", None)
            except Exception:
                name = None
    return str(name or "X-APP-TOKEN")


def _allowed_tokens_env() -> List[str]:
    toks: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            toks.append(v)
    # de-dupe preserve order
    out: List[str] = []
    seen = set()
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _extract_token(request: Request, query_token: Optional[str]) -> Optional[str]:
    # 1) dynamic header name, then X-APP-TOKEN fallback
    hdr = _auth_header_name()
    t = (request.headers.get(hdr) or request.headers.get("X-APP-TOKEN") or "").strip()
    if t:
        return t

    # 2) Authorization: Bearer
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        bt = auth.split(" ", 1)[1].strip()
        if bt:
            return bt

    # 3) Query token if enabled
    if _truthy(os.getenv("ALLOW_QUERY_TOKEN", "")):
        qt = (query_token or "").strip()
        if qt:
            return qt

    return None


def _auth_ok(request: Request, query_token: Optional[str]) -> bool:
    """
    Uses routes.config if available; else env-only.
    Open mode if no tokens configured.
    """
    provided = _extract_token(request, query_token)
    rc = _routes_config_mod()

    # Prefer config helpers if present
    if rc is not None:
        fn = getattr(rc, "auth_ok_request", None)
        if callable(fn):
            try:
                x_app = request.headers.get(_auth_header_name()) or request.headers.get("X-APP-TOKEN")
                authorization = request.headers.get("Authorization")
                return bool(fn(x_app_token=x_app, authorization=authorization, query_token=query_token))
            except Exception:
                pass

        fn2 = getattr(rc, "auth_ok", None)
        if callable(fn2):
            try:
                return bool(fn2(provided))
            except Exception:
                pass

        fn3 = getattr(rc, "allowed_tokens", None)
        if callable(fn3):
            try:
                allowed = fn3() or []
                if not allowed:
                    return True
                allowed_set = set([str(x).strip() for x in allowed if str(x).strip()])
                return bool(provided and provided.strip() in allowed_set)
            except Exception:
                pass

    # Fallback env-only
    allowed = _allowed_tokens_env()
    if not allowed:
        return True
    return bool(provided and provided.strip() in set(allowed))


# =============================================================================
# Lazy imports (core integrations)
# =============================================================================
@lru_cache(maxsize=1)
def _get_ksa_normalizer():
    try:
        from core.symbols.normalize import normalize_ksa_symbol  # type: ignore
        return normalize_ksa_symbol
    except Exception:
        return None


@lru_cache(maxsize=1)
def _get_scoring_enricher():
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore
        return enrich_with_scores
    except Exception:
        return None


@lru_cache(maxsize=1)
def _get_enriched_quote_class():
    try:
        from core.enriched_quote import EnrichedQuote  # type: ignore
        return EnrichedQuote
    except Exception:
        return None


@lru_cache(maxsize=1)
def _get_headers_helper():
    try:
        from core.schemas import get_headers_for_sheet, DEFAULT_HEADERS_59  # type: ignore
        return get_headers_for_sheet, DEFAULT_HEADERS_59
    except Exception:
        return None, None


# =============================================================================
# Env config
# =============================================================================
def _safe_int(x: Any, default: int) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def _safe_float(x: Any, default: float) -> float:
    try:
        return float(str(x).strip())
    except Exception:
        return default


def _cfg() -> Dict[str, Any]:
    return {
        "timeout_sec": max(5.0, min(90.0, _safe_float(os.getenv("ARGAAM_TIMEOUT_SEC"), 25.0))),
        "retry_attempts": max(0, min(8, _safe_int(os.getenv("ARGAAM_RETRY_ATTEMPTS"), 3))),
        "concurrency": max(1, min(40, _safe_int(os.getenv("ARGAAM_CONCURRENCY"), 12))),
        "sheet_max": max(10, min(3000, _safe_int(os.getenv("ARGAAM_SHEET_MAX"), 500))),
        "cache_ttl": max(1.0, min(600.0, _safe_float(os.getenv("ARGAAM_CACHE_TTL_SEC"), 20.0))),
        "cache_max": max(100, min(20000, _safe_int(os.getenv("ARGAAM_CACHE_MAX"), 4000))),
        "user_agent": (os.getenv("ARGAAM_USER_AGENT") or "").strip(),
        "quote_url": (os.getenv("ARGAAM_QUOTE_URL") or "").strip(),
    }


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# =============================================================================
# Time helpers
# =============================================================================
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso(utc_iso: Optional[str] = None) -> str:
    tz = timezone(timedelta(hours=3))
    try:
        if utc_iso:
            dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        else:
            dt = datetime.now(timezone.utc)
        return dt.astimezone(tz).isoformat()
    except Exception:
        return datetime.now(tz).isoformat()


# =============================================================================
# Normalization helpers
# =============================================================================
def _fallback_normalize_ksa(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip().upper()
    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")].strip()
    # numeric code => .SR
    if s.isdigit():
        return f"{s}.SR"
    # allow already .SR
    if s.endswith(".SR"):
        return s
    # accept plain code "1120" etc
    if re.fullmatch(r"\d{3,6}", s):
        return f"{s}.SR"
    return ""


def _normalize_ksa_symbol(symbol: str) -> str:
    norm_fn = _get_ksa_normalizer()
    if callable(norm_fn):
        try:
            out = norm_fn(symbol)
            return (out or "").strip().upper()
        except Exception:
            pass
    return _fallback_normalize_ksa(symbol)


def _parse_symbols(raw: Any) -> List[str]:
    """
    Accepts list/tuple OR string with comma/space separators.
    Returns normalized .SR symbols (dedup preserving order).
    """
    if raw is None:
        return []

    parts: List[str] = []
    if isinstance(raw, (list, tuple)):
        for it in raw:
            s = str(it or "").strip()
            if s:
                s = s.replace(",", " ")
                parts.extend([p.strip() for p in s.split() if p.strip()])
    else:
        s = str(raw or "").strip()
        if s:
            s = s.replace(",", " ")
            parts = [p.strip() for p in s.split() if p.strip()]

    out: List[str] = []
    seen = set()
    for p in parts:
        sym = _normalize_ksa_symbol(p)
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def _format_url(template: str, symbol: str) -> str:
    code = symbol.split(".")[0] if "." in symbol else symbol
    return (template or "").replace("{code}", code).replace("{symbol}", symbol)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# =============================================================================
# Cache
# =============================================================================
_cfg0 = _cfg()
_TTLCache = _cache_impl()

if _TTLCache:
    _quote_cache = _TTLCache(maxsize=_cfg0["cache_max"], ttl=_cfg0["cache_ttl"])  # type: ignore[misc]
else:
    _quote_cache = _TinyTTLCache(maxsize=_cfg0["cache_max"], ttl=_cfg0["cache_ttl"])


def _cache_get(k: str) -> Any:
    try:
        if hasattr(_quote_cache, "get"):
            return _quote_cache.get(k)  # type: ignore[attr-defined]
        return None
    except Exception:
        return None


def _cache_set(k: str, v: Any) -> None:
    try:
        if hasattr(_quote_cache, "__setitem__"):
            _quote_cache[k] = v  # type: ignore[index]
            return
    except Exception:
        pass
    try:
        if hasattr(_quote_cache, "set"):
            _quote_cache.set(k, v)  # type: ignore[attr-defined]
    except Exception:
        pass


# =============================================================================
# Fetch + mapping
# =============================================================================
def _coerce_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    try:
        s = str(x).strip().replace(",", "")
        if s.endswith("%"):
            s = s[:-1]
        return float(s)
    except Exception:
        return None


def _pick(d: Dict[str, Any], keys: Sequence[str]) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None


def _extract_data_container(payload: Any) -> Dict[str, Any]:
    """
    Argaam payloads vary. We support:
    - dict directly
    - {data:{...}}
    - {result:{...}}
    """
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), dict):
            return payload["data"]
        if isinstance(payload.get("result"), dict):
            return payload["result"]
        return payload
    return {}


def _unified_from_argaam(symbol: str, raw: Any, *, source_url: str) -> Dict[str, Any]:
    """
    Convert Argaam response into UnifiedQuote-like dict.
    Keep keys compatible with core.enriched_quote mapping.
    """
    now_utc = _utc_iso()
    d0 = _extract_data_container(raw)

    # Price + changes
    last = _pick(d0, ["last", "price", "close", "lastPrice", "tradedPrice"])
    prev = _pick(d0, ["previous_close", "prev_close", "previousClose", "prevClose"])
    chg = _pick(d0, ["change", "price_change", "delta", "chg"])
    chg_pct = _pick(d0, ["change_pct", "percentage_change", "chgPct", "percent_change", "pctChange"])

    out: Dict[str, Any] = {
        "status": "ok",
        "symbol": symbol,
        "symbol_normalized": symbol,
        "market": "KSA",
        "currency": "SAR",
        "current_price": _coerce_float(last),
        "previous_close": _coerce_float(prev),
        "price_change": _coerce_float(chg),
        "percent_change": _coerce_float(chg_pct),
        "volume": _coerce_float(_pick(d0, ["volume", "vol", "tradedVolume"])),

        # Descriptors
        "name": _pick(d0, ["companyName", "nameAr", "nameEn", "name"]),
        "sector": _pick(d0, ["sectorName", "sector", "sector_name"]),
        "sub_sector": _pick(d0, ["subSector", "sub_sector"]),

        # Provenance
        "data_source": "argaam",
        "engine_source": "argaam_direct",
        "provider_url": source_url,
        "data_quality": "OK" if _coerce_float(last) is not None else "PARTIAL",
        "last_updated_utc": now_utc,
        "last_updated_riyadh": _riyadh_iso(now_utc),
    }

    # Scoring (best-effort; never fail)
    enricher = _get_scoring_enricher()
    if callable(enricher):
        try:
            out = enricher(out)  # may mutate or return new
        except Exception:
            pass

    # Recommendation normalization if present
    try:
        reco = str(out.get("recommendation") or "HOLD").upper()
        if reco not in ("BUY", "HOLD", "REDUCE", "SELL"):
            out["recommendation"] = "HOLD"
        else:
            out["recommendation"] = reco
    except Exception:
        out["recommendation"] = "HOLD"

    return out


async def _fetch_json(url: str, *, timeout_sec: float, retry_attempts: int, user_agent: str) -> Tuple[Optional[Any], Optional[str]]:
    httpx = _httpx()
    if httpx is None:
        return None, "httpx is not installed on server"

    headers = {"User-Agent": user_agent or DEFAULT_USER_AGENT, "Accept": "application/json,*/*"}
    # retry with exponential backoff + jitter
    last_err = None
    for attempt in range(0, max(1, retry_attempts + 1)):
        try:
            async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
                r = await client.get(url, headers=headers)
                if r.status_code != 200:
                    last_err = f"HTTP {r.status_code}"
                else:
                    # Try JSON; if not JSON, return text error
                    try:
                        return r.json(), None
                    except Exception:
                        txt = (r.text or "").strip()
                        return None, "Non-JSON response from Argaam"
        except Exception as e:
            last_err = str(e)

        # backoff before next attempt
        if attempt < retry_attempts:
            base = 0.35 * (2 ** attempt)
            jitter = random.uniform(0.0, 0.25)
            await asyncio.sleep(min(3.0, base + jitter))

    return None, last_err or "Unknown fetch error"


async def _get_quote(symbol: str, *, refresh: bool, debug: int = 0) -> Dict[str, Any]:
    cfg = _cfg()
    sym = _normalize_ksa_symbol(symbol)
    if not sym:
        return {
            "status": "error",
            "symbol": (symbol or "").strip(),
            "error": "Invalid KSA symbol",
            "data_source": "argaam",
            "data_quality": "MISSING",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }

    tpl = cfg["quote_url"]
    if not tpl:
        return {
            "status": "error",
            "symbol": sym,
            "error": "ARGAAM_QUOTE_URL is not configured",
            "data_source": "argaam",
            "data_quality": "MISSING",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }

    ck = f"argaam::{sym}"
    if not refresh:
        hit = _cache_get(ck)
        if isinstance(hit, dict):
            return hit

    url = _format_url(tpl, sym)
    raw, err = await _fetch_json(
        url,
        timeout_sec=cfg["timeout_sec"],
        retry_attempts=cfg["retry_attempts"],
        user_agent=cfg["user_agent"] or DEFAULT_USER_AGENT,
    )

    if raw is None:
        out = {
            "status": "error",
            "symbol": sym,
            "error": err or "No data",
            "provider_url": url,
            "data_source": "argaam",
            "data_quality": "MISSING",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }
        if debug:
            out["debug"] = {"url": url}
        _cache_set(ck, out)
        return out

    out = _unified_from_argaam(sym, raw, source_url=url)
    _cache_set(ck, out)
    return out


# =============================================================================
# Sheet mapping helpers
# =============================================================================
def _rectangularize(headers: List[Any], rows: List[Any]) -> Tuple[List[str], List[List[Any]]]:
    h = [str(x).strip() for x in (headers or []) if str(x).strip()]
    if not h:
        # Safe minimal fallback
        h = ["Symbol", "Price", "Change %", "Data Quality", "Error", "Last Updated (UTC)", "Last Updated (Riyadh)"]

    out_rows: List[List[Any]] = []
    w = len(h)
    for r in (rows or []):
        rr = list(r) if isinstance(r, list) else []
        if len(rr) < w:
            rr.extend([""] * (w - len(rr)))
        elif len(rr) > w:
            rr = rr[:w]
        out_rows.append(rr)

    return h, out_rows


def _headers_for_sheet(sheet_name: Optional[str]) -> List[str]:
    get_headers, default_59 = _get_headers_helper()
    # 1) sheet-specific headers if possible
    if sheet_name and callable(get_headers):
        try:
            h = get_headers(sheet_name)
            if isinstance(h, list) and len(h) > 0:
                return [str(x).strip() for x in h if str(x).strip()]
        except Exception:
            pass
    # 2) fallback: 59-col schema if present
    if isinstance(default_59, (list, tuple)) and len(default_59) > 0:
        return [str(x).strip() for x in list(default_59) if str(x).strip()]
    # 3) minimal fallback
    return ["Symbol", "Name", "Price", "Change %", "Volume", "Recommendation", "Overall Score", "Data Quality", "Error", "Last Updated (UTC)", "Last Updated (Riyadh)"]


def _map_to_row(item: Dict[str, Any], headers: List[str]) -> List[Any]:
    EnrichedQuote = _get_enriched_quote_class()
    if EnrichedQuote and isinstance(item, dict) and item.get("status") == "ok":
        try:
            eq = EnrichedQuote.from_unified(item)
            row = eq.to_row(headers)
            if isinstance(row, list):
                return row
        except Exception:
            pass

    # Basic fallback mapping for error or if EnrichedQuote missing
    hlow = [h.lower() for h in headers]
    row = [""] * len(headers)

    def put(col_name_sub: str, val: Any):
        try:
            idx = next((i for i, hh in enumerate(hlow) if col_name_sub in hh), None)
            if idx is not None:
                row[idx] = val
        except Exception:
            pass

    put("symbol", item.get("symbol") or item.get("symbol_normalized") or "")
    put("name", item.get("name") or "")
    put("price", item.get("current_price") or "")
    put("change %", item.get("percent_change") or "")
    put("volume", item.get("volume") or "")
    put("recommendation", item.get("recommendation") or "HOLD")
    put("overall", item.get("overall_score") or "")
    put("data quality", item.get("data_quality") or "MISSING")
    put("error", item.get("error") or "")
    put("last updated (utc)", item.get("last_updated_utc") or _utc_iso())
    put("last updated (riyadh)", item.get("last_updated_riyadh") or _riyadh_iso(item.get("last_updated_utc")))

    return row


# =============================================================================
# Models
# =============================================================================
class _ExtraIgnore(BaseModel):
    if _PYDANTIC_V2:
        model_config = ConfigDict(extra="ignore")
    else:
        class Config:  # type: ignore
            extra = "ignore"


class SheetRowsIn(_ExtraIgnore):
    tickers: Optional[List[str]] = Field(default=None)
    symbols: Optional[List[str]] = Field(default=None)
    sheet_name: Optional[str] = Field(default=None)
    refresh: Optional[int] = Field(default=0)
    debug: Optional[int] = Field(default=0)


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health() -> Dict[str, Any]:
    cfg = _cfg()
    open_mode = True
    rc = _routes_config_mod()
    try:
        if rc is not None and callable(getattr(rc, "allowed_tokens", None)):
            open_mode = len(rc.allowed_tokens() or []) == 0  # type: ignore[attr-defined]
        else:
            open_mode = len(_allowed_tokens_env()) == 0
    except Exception:
        open_mode = len(_allowed_tokens_env()) == 0

    return {
        "status": "ok",
        "module": "routes.routes_argaam",
        "version": ROUTE_VERSION,
        "configured": bool(cfg["quote_url"]),
        "auth_header": _auth_header_name(),
        "open_mode": open_mode,
        "timeout_sec": cfg["timeout_sec"],
        "retry_attempts": cfg["retry_attempts"],
        "concurrency": cfg["concurrency"],
        "cache_ttl_sec": cfg["cache_ttl"],
        "cache_max": cfg["cache_max"],
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
    }


@router.get("/quote")
async def quote(
    request: Request,
    symbol: str = Query(..., description="KSA symbol, e.g. 1120.SR or 1120"),
    refresh: int = Query(0, description="refresh=1 bypasses cache"),
    token: Optional[str] = Query(None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 adds small debug info"),
) -> Union[Dict[str, Any], JSONResponse]:
    if not _auth_ok(request, token):
        return JSONResponse({"status": "error", "error": "Unauthorized"}, status_code=401)

    item = await _get_quote(symbol, refresh=bool(refresh), debug=int(debug or 0))
    # Keep HTTP 200 for GAS safety unless explicit unauthorized
    return item


@router.get("/quotes")
async def quotes(
    request: Request,
    symbols: Optional[str] = Query(None, description="Comma/space list: 1120.SR,2222.SR"),
    tickers: Optional[str] = Query(None, description="Alias of symbols"),
    refresh: int = Query(0, description="refresh=1 bypasses cache"),
    token: Optional[str] = Query(None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 adds small debug info"),
) -> Union[Dict[str, Any], JSONResponse]:
    if not _auth_ok(request, token):
        return JSONResponse({"status": "error", "error": "Unauthorized"}, status_code=401)

    syms = _parse_symbols(symbols or tickers or "")
    if not syms:
        return {"status": "error", "error": "No symbols provided", "items": [], "count": 0, "version": ROUTE_VERSION}

    cfg = _cfg()
    syms = syms[: cfg["sheet_max"]]

    sem = asyncio.Semaphore(cfg["concurrency"])

    async def one(s: str) -> Dict[str, Any]:
        async with sem:
            return await _get_quote(s, refresh=bool(refresh), debug=int(debug or 0))

    items = await asyncio.gather(*[one(s) for s in syms], return_exceptions=True)

    out_items: List[Dict[str, Any]] = []
    for i, it in enumerate(items):
        if isinstance(it, Exception):
            out_items.append(
                {
                    "status": "error",
                    "symbol": syms[i],
                    "error": str(it),
                    "data_source": "argaam",
                    "data_quality": "MISSING",
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
        else:
            out_items.append(it)

    return {
        "status": "ok",
        "count": len(out_items),
        "items": out_items,
        "version": ROUTE_VERSION,
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
    }


@router.post("/sheet-rows")
async def sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
) -> Dict[str, Any]:
    """
    GAS-oriented endpoint: returns schema-aware grid rows.

    Request JSON (examples):
      {"tickers":["1120.SR","2222.SR"], "sheet_name":"Market_Leaders"}
      {"symbols":["1120","2222"], "sheet_name":"Market_Leaders", "refresh":1}

    Response:
      { status, headers, rows, count, meta }
    """
    if not _auth_ok(request, token):
        # GAS safe: always include headers/rows even on auth error
        headers = _headers_for_sheet(body.get("sheet_name"))
        headers, rows = _rectangularize(headers, [])
        return {
            "status": "error",
            "error": "Unauthorized",
            "headers": headers,
            "rows": rows,
            "count": 0,
            "meta": {"version": ROUTE_VERSION, "auth_header": _auth_header_name(), "time_riyadh": _riyadh_iso()},
        }

    # Parse input robustly (list OR string)
    sheet_name = body.get("sheet_name") or body.get("sheet") or body.get("tab") or None
    refresh = int(body.get("refresh") or 0)
    debug = int(body.get("debug") or 0)

    syms = _parse_symbols(body.get("tickers") or body.get("symbols") or [])
    cfg = _cfg()
    syms = syms[: cfg["sheet_max"]]

    headers = _headers_for_sheet(sheet_name)
    if not syms:
        headers, rows0 = _rectangularize(headers, [])
        return {
            "status": "skipped",
            "headers": headers,
            "rows": rows0,
            "count": 0,
            "meta": {"version": ROUTE_VERSION, "reason": "No symbols", "time_riyadh": _riyadh_iso()},
        }

    sem = asyncio.Semaphore(cfg["concurrency"])

    async def one(s: str) -> Dict[str, Any]:
        async with sem:
            return await _get_quote(s, refresh=bool(refresh), debug=debug)

    raw_items = await asyncio.gather(*[one(s) for s in syms], return_exceptions=True)

    items: List[Dict[str, Any]] = []
    errors = 0
    for i, it in enumerate(raw_items):
        if isinstance(it, Exception):
            errors += 1
            items.append(
                {
                    "status": "error",
                    "symbol": syms[i],
                    "error": str(it),
                    "data_source": "argaam",
                    "data_quality": "MISSING",
                    "last_updated_utc": _utc_iso(),
                    "last_updated_riyadh": _riyadh_iso(),
                }
            )
        else:
            items.append(it)

    rows = []
    for it in items:
        try:
            rows.append(_map_to_row(it, headers))
        except Exception as e:
            errors += 1
            # Absolute safety: never break row count
            rows.append(_map_to_row({"status": "error", "symbol": it.get("symbol"), "error": str(e), "data_quality": "MISSING"}, headers))

    headers, rows = _rectangularize(headers, rows)

    ok_count = sum(1 for it in items if isinstance(it, dict) and it.get("status") == "ok")
    meta = {
        "version": ROUTE_VERSION,
        "sheet_name": sheet_name,
        "requested": len(syms),
        "ok": ok_count,
        "errors": errors,
        "refresh": bool(refresh),
        "configured": bool(cfg["quote_url"]),
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
    }

    return {"status": "ok" if ok_count > 0 else "error", "headers": headers, "rows": rows, "count": len(rows), "meta": meta}


__all__ = ["router"]
