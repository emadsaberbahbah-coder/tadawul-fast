# routes/routes_argaam.py
"""
routes/routes_argaam.py
===============================================================
Argaam Router (delegate) — FULL REPLACEMENT — v4.6.0
(PROD SAFE + CONFIG-AWARE AUTH + SCORING + SCHEMA MAPPING + RIYADH TIME + ADVANCED META REPORT)

What this route does
- ✅ Fetches KSA quotes from Argaam (URL template via env)
- ✅ Normalizes symbols (core.symbols.normalize.normalize_ksa_symbol > fallback)
- ✅ Optional scoring (core.scoring_engine.enrich_with_scores)
- ✅ Schema-aware mapping (core.enriched_quote.EnrichedQuote + core.schemas.get_headers_for_sheet)
- ✅ GAS-safe outputs: stable headers + rectangular rows
- ✅ Robust batching: mixed success/failure never crashes
- ✅ Config-aware auth header name (routes.config.AUTH_HEADER_NAME / settings.auth_header_name)
- ✅ Open mode if no tokens configured (routes.config.allowed_tokens or env tokens empty)
- ✅ Advanced meta report: cache hits/misses, timings, configured flags, per-item status summary

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
- AUTH_HEADER_NAME=... (optional override)
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

from fastapi import APIRouter, Body, Query, Request

logger = logging.getLogger("routes.routes_argaam")

ROUTE_VERSION = "4.6.0"
router = APIRouter(prefix="/v1/argaam", tags=["KSA / Argaam"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_RIYADH_TZ = timezone(timedelta(hours=3))


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
    """Minimal TTL cache fallback if cachetools is unavailable."""

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
        if len(self._d) >= self.maxsize:
            # Fast eviction (good enough)
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


def _allowed_tokens_env() -> List[str]:
    toks: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            toks.append(v)
    out: List[str] = []
    seen = set()
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _open_mode() -> bool:
    rc = _routes_config_mod()
    try:
        if rc is not None and callable(getattr(rc, "allowed_tokens", None)):
            return len(rc.allowed_tokens() or []) == 0  # type: ignore[attr-defined]
    except Exception:
        pass
    return len(_allowed_tokens_env()) == 0


def _auth_header_name() -> str:
    # env override wins (handy for hotfix without code change)
    env_name = (os.getenv("AUTH_HEADER_NAME") or "").strip()
    if env_name:
        return env_name

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
    return str(name or "X-APP-TOKEN").strip() or "X-APP-TOKEN"


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
                allowed_set = {str(x).strip() for x in allowed if str(x).strip()}
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
    try:
        if utc_iso:
            dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        else:
            dt = datetime.now(timezone.utc)
        return dt.astimezone(_RIYADH_TZ).isoformat()
    except Exception:
        return datetime.now(_RIYADH_TZ).isoformat()


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
    if s.endswith(".SR"):
        return s
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


def _parse_symbols_like(items: Sequence[Any]) -> List[str]:
    """
    Accepts list elements that might contain comma/space separated strings.
    Dedup (preserve order).
    """
    out: List[str] = []
    seen = set()
    for it in items or []:
        raw = str(it or "").strip()
        if not raw:
            continue
        parts = re.split(r"[\s,]+", raw)
        for p in parts:
            if not p.strip():
                continue
            sym = _normalize_ksa_symbol(p)
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
    return out


def _symbols_from_query(symbols: Optional[List[str]], tickers: Optional[List[str]]) -> List[str]:
    if symbols:
        return _parse_symbols_like(symbols)
    if tickers:
        return _parse_symbols_like(tickers)
    return []


def _symbols_from_body(body: Dict[str, Any]) -> List[str]:
    raw = body.get("symbols")
    if raw is None:
        raw = body.get("tickers")
    if isinstance(raw, str):
        return _parse_symbols_like([raw])
    if isinstance(raw, list):
        return _parse_symbols_like(raw)
    return []


def _format_url(template: str, symbol: str) -> str:
    code = symbol.split(".")[0] if "." in symbol else symbol
    return (template or "").replace("{code}", code).replace("{symbol}", symbol)


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
    # cachetools TTLCache supports __setitem__
    try:
        if hasattr(_quote_cache, "__setitem__"):
            _quote_cache[k] = v  # type: ignore[index]
            return
    except Exception:
        pass
    # Tiny cache uses .set()
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
    - {quote:{...}}
    """
    if isinstance(payload, dict):
        for k in ("data", "result", "quote"):
            if isinstance(payload.get(k), dict):
                return payload[k]
        return payload
    return {}


def _unified_from_argaam(symbol: str, raw: Any, *, source_url: str, cache_hit: bool) -> Dict[str, Any]:
    """
    Convert Argaam response into UnifiedQuote-like dict.
    Keep keys compatible with core.enriched_quote mapping.
    """
    now_utc = _utc_iso()
    d0 = _extract_data_container(raw)

    # Common Argaam-ish keys
    last = _pick(d0, ["last", "price", "close", "lastPrice", "tradedPrice"])
    prev = _pick(d0, ["previous_close", "prev_close", "previousClose", "prevClose"])
    chg = _pick(d0, ["change", "price_change", "delta", "chg"])
    chg_pct = _pick(d0, ["change_pct", "percentage_change", "chgPct", "percent_change", "pctChange"])
    vol = _pick(d0, ["volume", "vol", "tradedVolume"])
    mcap = _pick(d0, ["market_cap", "marketCap", "mcap"])
    vtraded = _pick(d0, ["value_traded", "valueTraded", "tradedValue"])

    out: Dict[str, Any] = {
        "status": "ok",
        "requested_symbol": symbol,
        "symbol": symbol,
        "symbol_normalized": symbol,
        "market": "KSA",
        "currency": "SAR",
        "current_price": _coerce_float(last),
        "previous_close": _coerce_float(prev),
        "price_change": _coerce_float(chg),
        "percent_change": _coerce_float(chg_pct),
        "volume": _coerce_float(vol),
        "market_cap": _coerce_float(mcap),
        "value_traded": _coerce_float(vtraded),
        "name": _pick(d0, ["companyName", "nameAr", "nameEn", "name"]),
        "sector": _pick(d0, ["sectorName", "sector", "sector_name"]),
        "sub_sector": _pick(d0, ["subSector", "sub_sector"]),
        "data_source": "argaam",
        "engine_source": "argaam_direct",
        "provider_url": source_url,
        "data_quality": "OK" if _coerce_float(last) is not None else "PARTIAL",
        "last_updated_utc": now_utc,
        "last_updated_riyadh": _riyadh_iso(now_utc),
        "_cache_hit": bool(cache_hit),
    }

    # Scoring (best-effort; never fail)
    enricher = _get_scoring_enricher()
    if callable(enricher):
        try:
            out = enricher(out) or out
        except Exception:
            pass

    # Recommendation normalization if present
    try:
        reco = str(out.get("recommendation") or "HOLD").upper()
        out["recommendation"] = reco if reco in ("BUY", "HOLD", "REDUCE", "SELL") else "HOLD"
    except Exception:
        out["recommendation"] = "HOLD"

    return out


async def _fetch_json(
    url: str,
    *,
    timeout_sec: float,
    retry_attempts: int,
    user_agent: str,
) -> Tuple[Optional[Any], Optional[str]]:
    httpx = _httpx()
    if httpx is None:
        return None, "httpx is not installed on server"

    headers = {"User-Agent": user_agent or DEFAULT_USER_AGENT, "Accept": "application/json,*/*"}
    last_err = None

    # Create ONE client for all retries (faster + fewer sockets)
    try:
        async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
            for attempt in range(0, max(1, retry_attempts + 1)):
                try:
                    r = await client.get(url, headers=headers)
                    if r.status_code != 200:
                        last_err = f"HTTP {r.status_code}"
                    else:
                        try:
                            return r.json(), None
                        except Exception:
                            return None, "Non-JSON response from Argaam"

                except Exception as e:
                    last_err = str(e)

                if attempt < retry_attempts:
                    base = 0.35 * (2**attempt)
                    jitter = random.uniform(0.0, 0.25)
                    await asyncio.sleep(min(3.0, base + jitter))
    except Exception as e:
        return None, str(e)

    return None, last_err or "Unknown fetch error"


async def _get_quote(symbol: str, *, refresh: bool, debug: int = 0) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Returns (quote_item, diag)
    diag includes cache_hit/cache_key/url/error
    """
    cfg = _cfg()
    sym = _normalize_ksa_symbol(symbol)

    diag: Dict[str, Any] = {"requested": (symbol or "").strip(), "normalized": sym, "cache_hit": False}

    if not sym:
        out = {
            "status": "error",
            "requested_symbol": (symbol or "").strip(),
            "symbol": (symbol or "").strip(),
            "error": "Invalid KSA symbol",
            "data_source": "argaam",
            "data_quality": "MISSING",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }
        return out, diag

    tpl = cfg["quote_url"]
    if not tpl:
        out = {
            "status": "error",
            "requested_symbol": sym,
            "symbol": sym,
            "error": "ARGAAM_QUOTE_URL is not configured",
            "data_source": "argaam",
            "data_quality": "MISSING",
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
        }
        return out, diag

    ck = f"argaam::{sym}"
    diag["cache_key"] = ck

    if not refresh:
        hit = _cache_get(ck)
        if isinstance(hit, dict):
            diag["cache_hit"] = True
            # Ensure we never lose requested_symbol
            hit.setdefault("requested_symbol", sym)
            hit.setdefault("symbol", sym)
            hit.setdefault("_cache_hit", True)
            return hit, diag

    url = _format_url(tpl, sym)
    diag["url"] = url

    raw, err = await _fetch_json(
        url,
        timeout_sec=cfg["timeout_sec"],
        retry_attempts=cfg["retry_attempts"],
        user_agent=cfg["user_agent"] or DEFAULT_USER_AGENT,
    )

    if raw is None:
        out = {
            "status": "error",
            "requested_symbol": sym,
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
        return out, diag

    out = _unified_from_argaam(sym, raw, source_url=url, cache_hit=False)
    _cache_set(ck, out)
    return out, diag


# =============================================================================
# Sheet mapping helpers
# =============================================================================
def _rectangularize(headers: List[Any], rows: List[Any]) -> Tuple[List[str], List[List[Any]]]:
    h = [str(x).strip() for x in (headers or []) if str(x).strip()]
    if not h:
        h = ["Symbol", "Name", "Price", "Change %", "Volume", "Recommendation", "Overall Score", "Data Quality", "Error", "Last Updated (UTC)", "Last Updated (Riyadh)"]

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
            if isinstance(h, list) and h:
                return [str(x).strip() for x in h if str(x).strip()]
        except Exception:
            pass

    # 2) fallback: 59-col schema if present
    if isinstance(default_59, (list, tuple)) and len(default_59) >= 10:
        return [str(x).strip() for x in list(default_59) if str(x).strip()]

    # 3) minimal fallback
    return ["Symbol", "Name", "Price", "Change %", "Volume", "Recommendation", "Overall Score", "Data Quality", "Error", "Last Updated (UTC)", "Last Updated (Riyadh)"]


def _map_to_row(item: Dict[str, Any], headers: List[str]) -> List[Any]:
    EnrichedQuote = _get_enriched_quote_class()
    if EnrichedQuote and isinstance(item, dict):
        try:
            eq = EnrichedQuote.from_unified(item)  # type: ignore
            row = eq.to_row(headers)  # type: ignore
            if isinstance(row, list):
                return row
        except Exception:
            pass

    # Basic fallback mapping (works for both ok/error)
    hlow = [h.lower() for h in headers]
    row = [""] * len(headers)

    def put(sub: str, val: Any):
        try:
            idx = next((i for i, hh in enumerate(hlow) if sub in hh), None)
            if idx is not None:
                row[idx] = val
        except Exception:
            pass

    put("symbol", item.get("symbol") or item.get("symbol_normalized") or item.get("requested_symbol") or "")
    put("name", item.get("name") or "")
    put("price", item.get("current_price") or "")
    put("change %", item.get("percent_change") or "")
    put("volume", item.get("volume") or "")
    put("recommendation", item.get("recommendation") or "HOLD")
    put("overall", item.get("overall_score") or item.get("advisor_score") or "")
    put("data quality", item.get("data_quality") or "MISSING")
    put("error", item.get("error") or "")
    put("last updated (utc)", item.get("last_updated_utc") or _utc_iso())
    put("last updated (riyadh)", item.get("last_updated_riyadh") or _riyadh_iso(item.get("last_updated_utc")))

    return row


def _envelope(status: str, *, headers: List[str], rows: List[List[Any]], error: Optional[str], meta: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "status": status,
        "headers": headers,
        "rows": rows,
        "count": len(rows),
        "version": ROUTE_VERSION,
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
        "meta": meta or {},
    }
    if error is not None:
        out["error"] = error
    if extra and isinstance(extra, dict):
        out.update(extra)
    return out


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def health() -> Dict[str, Any]:
    cfg = _cfg()

    # Mask settings (if config provides)
    rc = _routes_config_mod()
    config_mask = None
    try:
        if rc is not None and callable(getattr(rc, "mask_settings_dict", None)):
            config_mask = rc.mask_settings_dict()  # type: ignore[attr-defined]
    except Exception:
        config_mask = None

    return {
        "status": "ok",
        "module": "routes.routes_argaam",
        "version": ROUTE_VERSION,
        "configured": bool(cfg["quote_url"]),
        "quote_url_has_placeholder": ("{code}" in (cfg["quote_url"] or "") or "{symbol}" in (cfg["quote_url"] or "")),
        "auth_header": _auth_header_name(),
        "open_mode": _open_mode(),
        "timeout_sec": cfg["timeout_sec"],
        "retry_attempts": cfg["retry_attempts"],
        "concurrency": cfg["concurrency"],
        "cache_ttl_sec": cfg["cache_ttl"],
        "cache_max": cfg["cache_max"],
        "cache_type": type(_quote_cache).__name__,
        "config_mask": config_mask,
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
) -> Dict[str, Any]:
    # GAS-safe: HTTP 200 always
    if not _auth_ok(request, token):
        return {
            "status": "error",
            "requested_symbol": (symbol or "").strip(),
            "symbol": (symbol or "").strip(),
            "error": "Unauthorized",
            "version": ROUTE_VERSION,
            "time_utc": _utc_iso(),
            "time_riyadh": _riyadh_iso(),
        }

    item, diag = await _get_quote(symbol, refresh=bool(refresh), debug=int(debug or 0))
    if int(debug or 0):
        item.setdefault("debug", {})
        if isinstance(item["debug"], dict):
            item["debug"]["diag"] = diag
            item["debug"]["route_version"] = ROUTE_VERSION
    item["version"] = ROUTE_VERSION
    return item


@router.get("/quotes")
async def quotes(
    request: Request,
    symbols: Optional[List[str]] = Query(None, description="Repeated or CSV/space list"),
    tickers: Optional[List[str]] = Query(None, description="Alias of symbols"),
    refresh: int = Query(0, description="refresh=1 bypasses cache"),
    token: Optional[str] = Query(None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 adds small debug info"),
) -> Dict[str, Any]:
    # GAS-safe: HTTP 200 always
    if not _auth_ok(request, token):
        return {"status": "error", "error": "Unauthorized", "items": [], "count": 0, "version": ROUTE_VERSION, "time_utc": _utc_iso(), "time_riyadh": _riyadh_iso()}

    syms = _symbols_from_query(symbols, tickers)
    if not syms:
        return {"status": "error", "error": "No symbols provided", "items": [], "count": 0, "version": ROUTE_VERSION, "time_utc": _utc_iso(), "time_riyadh": _riyadh_iso()}

    cfg = _cfg()
    syms = syms[: cfg["sheet_max"]]

    sem = asyncio.Semaphore(cfg["concurrency"])
    started = time.time()

    cache_hits = 0
    cache_misses = 0

    async def one(s: str) -> Dict[str, Any]:
        nonlocal cache_hits, cache_misses
        async with sem:
            item, diag = await _get_quote(s, refresh=bool(refresh), debug=int(debug or 0))
            if diag.get("cache_hit"):
                cache_hits += 1
            else:
                cache_misses += 1
            if int(debug or 0):
                item.setdefault("debug", {})
                if isinstance(item["debug"], dict):
                    item["debug"]["diag"] = diag
            return item

    results = await asyncio.gather(*[one(s) for s in syms], return_exceptions=True)

    items: List[Dict[str, Any]] = []
    errors = 0
    for i, it in enumerate(results):
        if isinstance(it, Exception):
            errors += 1
            items.append(
                {
                    "status": "error",
                    "requested_symbol": syms[i],
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

    ok_count = sum(1 for it in items if isinstance(it, dict) and it.get("status") == "ok")
    processing_ms = round((time.time() - started) * 1000, 2)

    return {
        "status": "ok" if ok_count > 0 else "error",
        "count": len(items),
        "items": items,
        "version": ROUTE_VERSION,
        "meta": {
            "route_version": ROUTE_VERSION,
            "requested": len(syms),
            "ok": ok_count,
            "errors": errors,
            "refresh": bool(refresh),
            "configured": bool(cfg["quote_url"]),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "processing_time_ms": processing_ms,
            "time_utc": _utc_iso(),
            "time_riyadh": _riyadh_iso(),
        },
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
      {"symbols":"1120,2222", "sheetName":"Market_Leaders", "refresh":1}

    Response:
      { status, headers, rows, count, meta }
    """
    started = time.time()

    sheet_name = str(body.get("sheet_name") or body.get("sheetName") or body.get("sheet") or body.get("tab") or "").strip() or None
    refresh = _truthy(body.get("refresh")) or str(body.get("refresh") or "").strip() == "1"
    debug = _truthy(body.get("debug")) or str(body.get("debug") or "").strip() == "1"

    headers = _headers_for_sheet(sheet_name)
    if not headers:
        headers = ["Symbol", "Error"]

    # Auth error but still return stable grid contract
    if not _auth_ok(request, token):
        h, r = _rectangularize(headers, [])
        return _envelope(
            "error",
            headers=h,
            rows=r,
            error="Unauthorized",
            meta={"route_version": ROUTE_VERSION, "auth_header": _auth_header_name(), "sheet_name": sheet_name, "time_riyadh": _riyadh_iso()},
        )

    # Resolve symbols (payload supports list OR string)
    syms = _symbols_from_body(body)
    cfg = _cfg()
    syms = syms[: cfg["sheet_max"]]

    if not syms:
        h, r0 = _rectangularize(headers, [])
        return _envelope(
            "skipped",
            headers=h,
            rows=r0,
            error=None,
            meta={"route_version": ROUTE_VERSION, "reason": "No symbols", "sheet_name": sheet_name, "time_riyadh": _riyadh_iso()},
        )

    sem = asyncio.Semaphore(cfg["concurrency"])
    cache_hits = 0
    cache_misses = 0

    async def one(s: str) -> Dict[str, Any]:
        nonlocal cache_hits, cache_misses
        async with sem:
            item, diag = await _get_quote(s, refresh=bool(refresh), debug=int(debug))
            if diag.get("cache_hit"):
                cache_hits += 1
            else:
                cache_misses += 1
            if debug:
                item.setdefault("debug", {})
                if isinstance(item["debug"], dict):
                    item["debug"]["diag"] = diag
            return item

    raw_items = await asyncio.gather(*[one(s) for s in syms], return_exceptions=True)

    items: List[Dict[str, Any]] = []
    errors = 0
    for i, it in enumerate(raw_items):
        if isinstance(it, Exception):
            errors += 1
            items.append(
                {
                    "status": "error",
                    "requested_symbol": syms[i],
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

    rows: List[List[Any]] = []
    for it in items:
        try:
            rows.append(_map_to_row(it, headers))
        except Exception as e:
            errors += 1
            rows.append(_map_to_row({"status": "error", "symbol": it.get("symbol"), "error": str(e), "data_quality": "MISSING"}, headers))

    h2, r2 = _rectangularize(headers, rows)
    ok_count = sum(1 for it in items if isinstance(it, dict) and it.get("status") == "ok")
    processing_ms = round((time.time() - started) * 1000, 2)

    meta = {
        "route_version": ROUTE_VERSION,
        "sheet_name": sheet_name,
        "requested": len(syms),
        "ok": ok_count,
        "errors": errors,
        "refresh": bool(refresh),
        "configured": bool(cfg["quote_url"]),
        "quote_url_has_placeholder": ("{code}" in (cfg["quote_url"] or "") or "{symbol}" in (cfg["quote_url"] or "")),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "headers_len": len(h2),
        "rows_len": len(r2),
        "processing_time_ms": processing_ms,
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
    }

    status = "ok" if ok_count > 0 else "error"
    err_text = None if status == "ok" else "No successful rows returned"

    return _envelope(status, headers=h2, rows=r2, error=err_text, meta=meta)


__all__ = ["router"]
