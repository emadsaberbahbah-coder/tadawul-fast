# routes/enriched_quote.py
"""
routes/enriched_quote.py
------------------------------------------------------------
Enriched Quote Router — PROD SAFE (v6.6.0)
(SHEETS-ROWS + ROUTES.CONFIG AUTH + SCORING + NORMALIZATION + RIYADH TIME + TIMEOUTS)

What this router provides
- GET  /v1/enriched/health
- GET  /v1/enriched/quote
- GET  /v1/enriched/quotes
- POST /v1/enriched/sheet-rows        ✅ (Google Sheets grid endpoint)

Design rules
- Never hard-crash on missing optional modules (schemas/scoring/normalizer/engine).
- Deterministic outputs for Google Sheets: NEVER return empty headers.
- Auth supports:
    - Configurable header (AUTH_HEADER_NAME) + X-APP-TOKEN fallback
    - Authorization: Bearer <token>
    - Optional ?token=... when ALLOW_QUERY_TOKEN=1
- Handles symbols input shapes:
    - repeated params: ?symbols=AAPL&symbols=MSFT
    - csv/space strings: ?symbols=AAPL,MSFT 1120.SR
    - payload: {"symbols":[...], "tickers":[...]} for sheet-rows

Alignment
- Canonical ROI keys: expected_roi_1m / expected_roi_3m / expected_roi_12m
- Canonical forecast price keys: forecast_price_1m / forecast_price_3m / forecast_price_12m
- Aliases mirrored for legacy columns:
    expected_return_* , expected_price_* , target_price_*

Environment (safe defaults)
- ENRICHED_BATCH_SIZE (default 80)
- ENRICHED_CONCURRENCY (default 6)
- ENRICHED_MAX_SYMBOLS (default 3000)
- ENRICHED_TIMEOUT_SEC (default 45)
- ALLOW_QUERY_TOKEN=1 (optional, only used if tokens are set)
- APP_TOKEN / BACKUP_APP_TOKEN / TFB_APP_TOKEN (if any set -> auth enforced)

Notes
- No heavy work at import-time. Engines/providers resolved lazily.
- HTTP 200 always (Sheets safe), with {status,...} payload.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Query, Request

logger = logging.getLogger("routes.enriched_quote")

ENRICHED_ROUTE_VERSION = "6.6.0"
router = APIRouter(prefix="/v1/enriched", tags=["enriched"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_HKEY_RE = re.compile(r"[^a-z0-9]+")
_RIYADH_TZ = timezone(timedelta(hours=3))


# =============================================================================
# Small helpers
# =============================================================================
def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso(utc_iso: Optional[str] = None) -> str:
    try:
        if utc_iso:
            s = str(utc_iso).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.now(timezone.utc)
        return dt.astimezone(_RIYADH_TZ).isoformat()
    except Exception:
        return datetime.now(_RIYADH_TZ).isoformat()


def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    return _riyadh_iso(utc_iso) if utc_iso else ""


def _hkey(name: Any) -> str:
    s = str(name or "").strip().lower()
    if not s:
        return ""
    return _HKEY_RE.sub("", s)


def _safe_int(x: Any, default: int) -> int:
    try:
        s = str(x).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        s = str(x).strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _cfg() -> Dict[str, Any]:
    # Keep defaults conservative for Render
    batch_size = _safe_int(os.getenv("ENRICHED_BATCH_SIZE", "80"), 80)
    concurrency = _safe_int(os.getenv("ENRICHED_CONCURRENCY", "6"), 6)
    max_symbols = _safe_int(os.getenv("ENRICHED_MAX_SYMBOLS", "3000"), 3000)
    timeout_sec = float(os.getenv("ENRICHED_TIMEOUT_SEC", "45") or "45")

    batch_size = max(10, min(400, batch_size))
    concurrency = max(1, min(30, concurrency))
    max_symbols = max(50, min(10000, max_symbols))
    timeout_sec = max(5.0, min(180.0, timeout_sec))

    return {
        "batch_size": batch_size,
        "concurrency": concurrency,
        "max_symbols": max_symbols,
        "timeout_sec": timeout_sec,
    }


# =============================================================================
# Safe/Lazy imports
# =============================================================================
@lru_cache(maxsize=1)
def _get_scoring_enricher():
    try:
        from core.scoring_engine import enrich_with_scores  # type: ignore

        return enrich_with_scores
    except Exception:
        return None


@lru_cache(maxsize=1)
def _get_normalizer():
    """
    Returns (normalize_symbol, is_ksa) or (None, None)
    """
    try:
        from core.symbols.normalize import normalize_symbol, is_ksa  # type: ignore

        return normalize_symbol, is_ksa
    except Exception:
        return None, None


@lru_cache(maxsize=1)
def _get_schemas():
    try:
        import core.schemas as schemas  # type: ignore

        return schemas
    except Exception:
        return None


@lru_cache(maxsize=1)
def _get_enriched_quote_model():
    try:
        from core.enriched_quote import EnrichedQuote  # type: ignore

        return EnrichedQuote
    except Exception:
        return None


@lru_cache(maxsize=1)
def _get_routes_config():
    """
    Prefer centralized auth/config shim if present.
    """
    try:
        from routes.config import (  # type: ignore
            AUTH_HEADER_NAME,
            auth_ok_request,
            is_open_mode,
            mask_settings_dict,
        )

        return {
            "AUTH_HEADER_NAME": AUTH_HEADER_NAME,
            "auth_ok_request": auth_ok_request,
            "is_open_mode": is_open_mode,
            "mask_settings_dict": mask_settings_dict,
        }
    except Exception:
        return None


# =============================================================================
# Normalization
# =============================================================================
def _fallback_normalize(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip().upper()
    if s.endswith(".TADAWUL"):
        s = s.replace(".TADAWUL", "")
    # keep indices / fx as-is
    if any(ch in s for ch in ("^", "=", "/")):
        return s
    if s.isdigit():
        return f"{s}.SR"
    # normalize Saudi .SA => .SR
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    return s


def _normalize_symbol(sym: str) -> str:
    norm_fn, _ = _get_normalizer()
    if callable(norm_fn):
        try:
            out = norm_fn(sym)
            out = (str(out or "").strip().upper()) if out else ""
            return out or _fallback_normalize(sym)
        except Exception:
            return _fallback_normalize(sym)
    return _fallback_normalize(sym)


def _is_ksa_symbol(sym: str) -> bool:
    _, is_ksa_fn = _get_normalizer()
    if callable(is_ksa_fn):
        try:
            return bool(is_ksa_fn(sym))
        except Exception:
            pass
    u = (sym or "").upper().strip()
    return u.endswith(".SR") or u.isdigit()


def _market_hint_for(sym: str) -> str:
    u = (sym or "").upper()
    if "^" in u or "=" in u or "/" in u:
        return "INDEXFX"
    if _is_ksa_symbol(u):
        return "KSA"
    return "GLOBAL"


def _parse_symbols_like(items: Sequence[Any]) -> List[str]:
    """
    Accepts list elements that might contain comma/space separated strings.
    Returns normalized, de-duped, order-preserving list.
    """
    out: List[str] = []
    seen = set()

    for it in items or []:
        raw = str(it or "").strip()
        if not raw:
            continue
        parts = re.split(r"[\s,;|]+", raw)
        for p in parts:
            p = p.strip()
            if not p:
                continue
            n = _normalize_symbol(p)
            if not n or n in seen:
                continue
            seen.add(n)
            out.append(n)

    return out


def _symbols_from_query_params(symbols: Optional[List[str]], tickers: Optional[List[str]]) -> List[str]:
    # prefer symbols then tickers
    if symbols:
        return _parse_symbols_like(symbols)
    if tickers:
        return _parse_symbols_like(tickers)
    return []


def _symbols_from_payload(body: Dict[str, Any]) -> List[str]:
    raw = body.get("symbols")
    if raw is None:
        raw = body.get("tickers")
    if raw is None:
        raw = body.get("all_tickers") or body.get("ksa_tickers") or body.get("global_tickers")

    if isinstance(raw, str):
        return _parse_symbols_like([raw])
    if isinstance(raw, list):
        return _parse_symbols_like(raw)
    return []


# =============================================================================
# Auth (centralized via routes.config when available)
# =============================================================================
def _extract_tokens_for_auth(request: Request) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (header_token_value, authorization_header, query_token_value)
    """
    cfg = _get_routes_config()
    hdr_name = (str(cfg.get("AUTH_HEADER_NAME")).strip() if cfg and cfg.get("AUTH_HEADER_NAME") else "") or "X-APP-TOKEN"

    header_token = request.headers.get(hdr_name) or request.headers.get("X-APP-TOKEN")
    authorization = request.headers.get("Authorization")
    query_token = request.query_params.get("token")

    return header_token, authorization, query_token


def _auth_ok_request(request: Request) -> bool:
    cfg = _get_routes_config()
    header_token, authorization, query_token = _extract_tokens_for_auth(request)

    # If centralized helper exists, use it
    if cfg and callable(cfg.get("auth_ok_request")):
        try:
            return bool(
                cfg["auth_ok_request"](
                    x_app_token=header_token,
                    authorization=authorization,
                    query_token=query_token,
                )
            )
        except Exception:
            pass

    # Local fallback
    valid = set()
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
        if v:
            valid.add(v)

    # OPEN MODE
    if not valid:
        return True

    if header_token and header_token.strip() in valid:
        return True

    # Bearer extraction locally
    bearer_token = None
    if authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            bearer_token = parts[1].strip()
    if bearer_token and bearer_token in valid:
        return True

    if query_token and _truthy(os.getenv("ALLOW_QUERY_TOKEN")) and query_token.strip() in valid:
        return True

    return False


# =============================================================================
# Engine access
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _get_engine(request: Request) -> Any:
    # 1) App state
    try:
        st = getattr(request.app, "state", None)
        if st and getattr(st, "engine", None):
            return st.engine
    except Exception:
        pass

    # 2) Singleton fallback
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        return await _maybe_await(get_engine())
    except Exception:
        return None


async def _call_engine_batch(eng: Any, symbols: List[str], refresh: bool, timeout_sec: float) -> Tuple[Any, str]:
    if not eng:
        return None, "none"

    fn = getattr(eng, "get_enriched_quotes", None)
    if callable(fn):
        try:
            coro = fn(symbols, refresh=refresh)
        except TypeError:
            coro = fn(symbols)
        try:
            return await asyncio.wait_for(_maybe_await(coro), timeout=timeout_sec), "batch_enriched"
        except asyncio.TimeoutError:
            return None, "batch_enriched_timeout"
        except Exception:
            return None, "batch_enriched_failed"

    fn2 = getattr(eng, "get_quotes", None)
    if callable(fn2):
        try:
            coro = fn2(symbols, refresh=refresh)
        except TypeError:
            coro = fn2(symbols)
        try:
            return await asyncio.wait_for(_maybe_await(coro), timeout=timeout_sec), "batch_standard"
        except asyncio.TimeoutError:
            return None, "batch_standard_timeout"
        except Exception:
            return None, "batch_standard_failed"

    return None, "none"


async def _call_engine_single(eng: Any, symbol: str, refresh: bool, timeout_sec: float) -> Tuple[Any, str]:
    if not eng:
        return None, "none"

    fn = getattr(eng, "get_enriched_quote", None)
    if callable(fn):
        try:
            coro = fn(symbol, refresh=refresh)
        except TypeError:
            coro = fn(symbol)
        try:
            return await asyncio.wait_for(_maybe_await(coro), timeout=timeout_sec), "single_enriched"
        except asyncio.TimeoutError:
            return None, "single_enriched_timeout"
        except Exception:
            return None, "single_enriched_failed"

    fn2 = getattr(eng, "get_quote", None)
    if callable(fn2):
        try:
            coro = fn2(symbol, refresh=refresh)
        except TypeError:
            coro = fn2(symbol)
        try:
            return await asyncio.wait_for(_maybe_await(coro), timeout=timeout_sec), "single_standard"
        except asyncio.TimeoutError:
            return None, "single_standard_timeout"
        except Exception:
            return None, "single_standard_failed"

    return None, "none"


# =============================================================================
# Enrichment / shaping
# =============================================================================
def _apply_roi_aliases(item: Dict[str, Any]) -> None:
    """
    Ensure canonical ROI/forecast keys exist, and mirror legacy aliases.
    Canonical:
      expected_roi_1m/3m/12m
      forecast_price_1m/3m/12m
    Aliases mirrored:
      expected_return_*, expected_price_*, target_price_*
    """
    for h in ("1m", "3m", "12m"):
        canon_roi = f"expected_roi_{h}"
        legacy_roi = f"expected_return_{h}"

        v = item.get(canon_roi)
        if v is None:
            v = item.get(legacy_roi)

        vf = _safe_float(v)
        if vf is not None:
            item[canon_roi] = vf
            item[legacy_roi] = vf

        canon_p = f"forecast_price_{h}"
        legacy_p = f"expected_price_{h}"
        legacy_tp = f"target_price_{h}"

        pv = item.get(canon_p)
        if pv is None:
            pv = item.get(legacy_p)
        if pv is None:
            pv = item.get(legacy_tp)

        pf = _safe_float(pv)
        if pf is not None:
            item[canon_p] = pf
            item[legacy_p] = pf
            item[legacy_tp] = pf


def _coerce_common_fields(item: Dict[str, Any]) -> None:
    """
    Fill common convenience keys without breaking existing provider keys.
    """
    if item.get("price") is None:
        item["price"] = item.get("current_price")
    if item.get("current_price") is None:
        item["current_price"] = item.get("price")

    if item.get("percent_change") is None:
        item["percent_change"] = item.get("change_pct")
    if item.get("change_pct") is None:
        item["change_pct"] = item.get("percent_change")


def _enrich_item(raw: Any, requested_symbol: str, *, engine_source: str = "") -> Dict[str, Any]:
    """
    Normalize one item into a dict with stable keys + timestamps.
    """
    if isinstance(raw, tuple) and len(raw) == 2:
        raw = raw[0]

    if raw is None:
        item: Dict[str, Any] = {"error": "No Data"}
    elif isinstance(raw, dict):
        item = dict(raw)
    else:
        # best-effort object -> dict
        item = dict(getattr(raw, "__dict__", {}) or {})
        if not item:
            item = {"raw": str(raw)}

    req = (requested_symbol or "").strip().upper()
    item.setdefault("requested_symbol", req)

    sym = str(item.get("symbol") or item.get("ticker") or req).strip().upper()
    item["symbol"] = sym or req
    item.setdefault("symbol_normalized", item["symbol"])

    _coerce_common_fields(item)

    # Data quality (basic)
    if not item.get("data_quality"):
        px = item.get("current_price") or item.get("price")
        item["data_quality"] = "OK" if _safe_float(px) is not None else "MISSING"

    # Scoring engine (best effort)
    enricher = _get_scoring_enricher()
    if callable(enricher):
        try:
            item = enricher(item) or item
        except Exception as e:
            item.setdefault("scoring_error", str(e))

    # Recommendation clamp
    reco = str(item.get("recommendation") or "HOLD").upper()
    if reco not in ("BUY", "HOLD", "REDUCE", "SELL"):
        reco = "HOLD"
    item["recommendation"] = reco

    # Timestamps (authoritative route-level injection)
    utc = item.get("last_updated_utc") or _now_utc_iso()
    item["last_updated_utc"] = str(utc)
    item["last_updated_riyadh"] = _to_riyadh_iso(str(utc))

    # Forecast timestamps (keep provider utc if present; default to last_updated)
    f_utc = item.get("forecast_updated_utc") or item.get("forecast_updated") or utc
    item["forecast_updated_utc"] = str(f_utc)
    item["forecast_updated_riyadh"] = item.get("forecast_updated_riyadh") or _to_riyadh_iso(str(f_utc))

    # ROI/forecast alignment
    _apply_roi_aliases(item)

    # Engine provenance
    if engine_source:
        item.setdefault("engine_source", engine_source)

    return item


def _shape_batch_result(requested: List[str], res: Any, engine_source: str) -> Dict[str, Dict[str, Any]]:
    """
    Align batch results to requested symbols:
    - list: order-based
    - dict: key-based (case-insensitive + normalized)
    """
    out: Dict[str, Dict[str, Any]] = {}

    if isinstance(res, list):
        for i, req in enumerate(requested):
            raw = res[i] if i < len(res) else None
            out[req] = _enrich_item(raw, req, engine_source=engine_source)
        return out

    if isinstance(res, dict):
        lookup: Dict[str, Any] = {}
        for k, v in res.items():
            kk = str(k or "").strip().upper()
            if not kk:
                continue
            lookup[kk] = v
            lookup[_normalize_symbol(kk)] = v

        for req in requested:
            raw = lookup.get(req) or lookup.get(_normalize_symbol(req))
            out[req] = _enrich_item(raw, req, engine_source=engine_source)
        return out

    for req in requested:
        out[req] = _enrich_item({"error": "Invalid batch format"}, req, engine_source=engine_source)
    return out


async def _fetch_quotes_map(engine: Any, symbols: List[str], *, refresh: bool) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (map, meta). Ensures every requested symbol has a row.
    Chunked + concurrency-limited + timeout-protected.
    """
    cfg = _cfg()
    start = time.time()

    symbols = (symbols or [])[: cfg["max_symbols"]]
    if not symbols:
        return {}, {"mode": "empty", "route_version": ENRICHED_ROUTE_VERSION}

    # Market hint grouping (optional metadata only)
    groups: Dict[str, List[str]] = {}
    for s in symbols:
        groups.setdefault(_market_hint_for(s), []).append(s)

    batch = int(cfg["batch_size"])
    timeout_sec = float(cfg["timeout_sec"])
    sem = asyncio.Semaphore(int(cfg["concurrency"]))

    final_map: Dict[str, Dict[str, Any]] = {}

    # Build chunk tasks across all groups (concurrency applies per chunk)
    chunk_tasks: List[Tuple[str, List[str]]] = []
    for hint, syms in groups.items():
        for i in range(0, len(syms), batch):
            chunk_tasks.append((hint, syms[i : i + batch]))

    async def process_chunk(hint: str, ch: List[str]) -> None:
        async with sem:
            res, src = await _call_engine_batch(engine, ch, refresh, timeout_sec)
            if res is not None:
                final_map.update(_shape_batch_result(ch, res, src))
                return

            # per-symbol fallback (still within semaphore)
            for one in ch:
                raw, ssrc = await _call_engine_single(engine, one, refresh, timeout_sec)
                if raw is None:
                    final_map[one] = _enrich_item({"error": "No Data", "market_hint": hint}, one, engine_source=ssrc or src or "single_fallback")
                else:
                    final_map[one] = _enrich_item(raw, one, engine_source=ssrc or src or "single_fallback")

    await asyncio.gather(*[process_chunk(hint, ch) for hint, ch in chunk_tasks])

    # Ensure all requested symbols exist
    for s in symbols:
        if s not in final_map:
            final_map[s] = _enrich_item({"error": "Missing from engine response"}, s, engine_source="missing")

    ms = round((time.time() - start) * 1000, 2)
    meta = {
        "batch_size": cfg["batch_size"],
        "concurrency": cfg["concurrency"],
        "max_symbols": cfg["max_symbols"],
        "timeout_sec": cfg["timeout_sec"],
        "refresh": bool(refresh),
        "processing_time_ms": ms,
        "groups": {k: len(v) for k, v in groups.items()},
        "chunks": len(chunk_tasks),
    }
    return final_map, meta


# =============================================================================
# Sheets mapping helpers
# =============================================================================
def _fallback_headers() -> List[str]:
    return [
        "Symbol",
        "Name",
        "Market",
        "Currency",
        "Price",
        "Prev Close",
        "Change",
        "Change %",
        "Volume",
        "Market Cap",
        "Overall Score",
        "Risk Score",
        "Recommendation",
        "Expected ROI % (1M)",
        "Forecast Price (1M)",
        "Expected ROI % (3M)",
        "Forecast Price (3M)",
        "Expected ROI % (12M)",
        "Forecast Price (12M)",
        "Forecast Confidence",
        "Data Quality",
        "Error",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
        "Forecast Updated (UTC)",
        "Forecast Updated (Riyadh)",
    ]


def _headers_for_sheet(sheet_name: str) -> List[str]:
    schemas = _get_schemas()
    if schemas:
        fn = getattr(schemas, "get_headers_for_sheet", None)
        if callable(fn):
            try:
                h = fn(sheet_name)
                if isinstance(h, list) and h:
                    out = [str(x).strip() for x in h if str(x).strip()]
                    if out:
                        return out
            except Exception:
                pass

        # fallback to known default lists if present
        for attr in ("DEFAULT_HEADERS_59", "DEFAULT_HEADERS"):
            hh = getattr(schemas, attr, None)
            if isinstance(hh, (list, tuple)) and len(hh) >= 10:
                out2 = [str(x).strip() for x in hh if str(x).strip()]
                if out2:
                    return out2

    return _fallback_headers()


def _dict_to_row_by_headers(d: Dict[str, Any], headers: List[str]) -> List[Any]:
    """
    Best-effort mapping by normalized header key.
    """
    km = {_hkey(k): v for k, v in (d or {}).items()}

    # common aliases for friendly headers
    alias: Dict[str, str] = {
        "price": "current_price",
        "prevclose": "prev_close",
        "change%": "percent_change",
        "forecastconfidence": "forecast_confidence",
        "dataquality": "data_quality",
        "lastupdatedutc": "last_updated_utc",
        "lastupdatedriyadh": "last_updated_riyadh",
        "forecastupdatedutc": "forecast_updated_utc",
        "forecastupdatedriyadh": "forecast_updated_riyadh",
    }
    alias2 = {_hkey(k): _hkey(v) for k, v in alias.items()}

    row: List[Any] = []
    for h in headers:
        hk = _hkey(h)

        # Expected ROI % (1M/3M/12M)
        if "expectedroi" in hk and ("1m" in hk or "3m" in hk or "12m" in hk):
            if "1m" in hk:
                row.append(km.get("expectedroi1m", km.get("expectedreturn1m")))
                continue
            if "3m" in hk:
                row.append(km.get("expectedroi3m", km.get("expectedreturn3m")))
                continue
            if "12m" in hk:
                row.append(km.get("expectedroi12m", km.get("expectedreturn12m")))
                continue

        # Forecast Price (1M/3M/12M)
        if "forecastprice" in hk and ("1m" in hk or "3m" in hk or "12m" in hk):
            if "1m" in hk:
                row.append(km.get("forecastprice1m", km.get("expectedprice1m", km.get("targetprice1m"))))
                continue
            if "3m" in hk:
                row.append(km.get("forecastprice3m", km.get("expectedprice3m", km.get("targetprice3m"))))
                continue
            if "12m" in hk:
                row.append(km.get("forecastprice12m", km.get("expectedprice12m", km.get("targetprice12m"))))
                continue

        # Direct key
        if hk in km:
            row.append(km.get(hk))
            continue

        # Alias
        ak = alias2.get(hk)
        if ak and ak in km:
            row.append(km.get(ak))
            continue

        row.append(None)

    return row


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def enriched_health(request: Request):
    eng = await _get_engine(request)
    cfg = _cfg()
    rc = _get_routes_config()

    open_mode = None
    auth_header = None
    config_mask = None
    try:
        if rc and callable(rc.get("is_open_mode")):
            open_mode = bool(rc["is_open_mode"]())
        auth_header = str(rc.get("AUTH_HEADER_NAME")) if rc and rc.get("AUTH_HEADER_NAME") else "X-APP-TOKEN"
        config_mask = rc["mask_settings_dict"]() if rc and callable(rc.get("mask_settings_dict")) else None
    except Exception:
        pass

    return {
        "status": "ok",
        "module": "routes.enriched_quote",
        "version": ENRICHED_ROUTE_VERSION,
        "engine": type(eng).__name__ if eng else "none",
        "time_utc": _now_utc_iso(),
        "time_riyadh": _riyadh_iso(),
        "open_mode": open_mode,
        "auth_header": auth_header or "X-APP-TOKEN",
        "cfg": cfg,
        "config_mask": config_mask,
    }


@router.get("/quote")
async def get_single_quote(
    request: Request,
    symbol: str = Query(..., description="Symbol (e.g., 1120.SR)"),
    refresh: int = Query(0, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    # HTTP 200 always (Sheets/client safe)
    if not _auth_ok_request(request):
        return {"status": "error", "error": "Unauthorized", "version": ENRICHED_ROUTE_VERSION}

    eng = await _get_engine(request)
    if not eng:
        return {"status": "error", "error": "Engine Unavailable", "version": ENRICHED_ROUTE_VERSION}

    norm_sym = _normalize_symbol(symbol)
    if not norm_sym:
        return {"status": "error", "error": "Invalid Symbol", "version": ENRICHED_ROUTE_VERSION}

    cfg = _cfg()
    try:
        raw, src = await _call_engine_single(eng, norm_sym, bool(refresh), float(cfg["timeout_sec"]))
        item = _enrich_item(raw, norm_sym, engine_source=src or "single")
        item["status"] = "ok"
        item["version"] = ENRICHED_ROUTE_VERSION
        if debug:
            item["debug"] = {"engine_source": src, "refresh": bool(refresh), "timeout_sec": cfg["timeout_sec"]}
        return item
    except Exception as e:
        out = {"status": "error", "error": str(e), "symbol": norm_sym, "version": ENRICHED_ROUTE_VERSION}
        if debug:
            out["debug"] = True
        return out


@router.get("/quotes")
async def get_batch_quotes(
    request: Request,
    symbols: Optional[List[str]] = Query(None, description="Repeated or CSV/space symbols"),
    tickers: Optional[List[str]] = Query(None, description="Alias of symbols"),
    refresh: int = Query(0, ge=0, le=1),
):
    # HTTP 200 always (Sheets/client safe)
    if not _auth_ok_request(request):
        return {"status": "error", "error": "Unauthorized", "items": [], "version": ENRICHED_ROUTE_VERSION}

    target_list = _symbols_from_query_params(symbols, tickers)
    if not target_list:
        return {"status": "error", "error": "No symbols provided", "items": [], "version": ENRICHED_ROUTE_VERSION}

    eng = await _get_engine(request)
    if not eng:
        return {"status": "error", "error": "Engine Unavailable", "items": [], "version": ENRICHED_ROUTE_VERSION}

    quotes_map, meta = await _fetch_quotes_map(eng, target_list, refresh=bool(refresh))

    items: List[Dict[str, Any]] = []
    for s in target_list:
        items.append(quotes_map.get(s) or _enrich_item({"error": "Missing row"}, s, engine_source="missing"))

    return {
        "status": "ok",
        "count": len(items),
        "items": items,
        "meta": {
            **(meta or {}),
            "route_version": ENRICHED_ROUTE_VERSION,
            "time_utc": _now_utc_iso(),
            "time_riyadh": _riyadh_iso(),
        },
        "version": ENRICHED_ROUTE_VERSION,
    }


@router.post("/sheet-rows")
async def enriched_sheet_rows(
    request: Request,
    body: Dict[str, Any] = Body(...),
):
    """
    Google Sheets Grid Endpoint
    - Input payload expected (flexible):
        {
          "symbols": [...], "tickers":[...],
          "sheet_name": "Market_Leaders",
          "sheetName": "Market_Leaders",
          "refresh": 0/1
        }

    - Output (Sheets-safe):
        { status, headers, rows, error?, meta, version }
    """
    # Sheets-safe: HTTP 200 always
    if not _auth_ok_request(request):
        return {
            "status": "error",
            "error": "Unauthorized",
            "headers": ["Symbol", "Error"],
            "rows": [],
            "version": ENRICHED_ROUTE_VERSION,
            "meta": {"time_utc": _now_utc_iso(), "time_riyadh": _riyadh_iso()},
        }

    symbols_list = _symbols_from_payload(body)
    if not symbols_list:
        return {
            "status": "skipped",
            "error": "No symbols provided",
            "headers": ["Symbol", "Error"],
            "rows": [],
            "version": ENRICHED_ROUTE_VERSION,
            "meta": {"time_utc": _now_utc_iso(), "time_riyadh": _riyadh_iso()},
        }

    sheet_name = str(body.get("sheet_name") or body.get("sheetName") or body.get("sheet") or "Enriched").strip() or "Enriched"
    refresh = _truthy(body.get("refresh")) or str(body.get("refresh") or "").strip() == "1"

    headers = _headers_for_sheet(sheet_name)
    if not isinstance(headers, list) or len(headers) == 0:
        headers = ["Symbol", "Error"]

    eng = await _get_engine(request)
    if not eng:
        # Return placeholders so Sheets can still write rows
        err_idx = next((i for i, h in enumerate(headers) if _hkey(h) == "error"), None)
        sym_idx = next((i for i, h in enumerate(headers) if _hkey(h) in ("symbol", "ticker")), 0)
        rows: List[List[Any]] = []
        for s in symbols_list:
            r = [None] * len(headers)
            r[sym_idx] = s
            if err_idx is not None:
                r[err_idx] = "Engine Unavailable"
            rows.append(r)
        return {
            "status": "error",
            "error": "Engine Unavailable",
            "headers": headers,
            "rows": rows,
            "version": ENRICHED_ROUTE_VERSION,
            "meta": {"time_utc": _now_utc_iso(), "time_riyadh": _riyadh_iso(), "sheet": sheet_name},
        }

    quotes_map, meta = await _fetch_quotes_map(eng, symbols_list, refresh=bool(refresh))

    EnrichedQuote = _get_enriched_quote_model()
    rows: List[List[Any]] = []

    for s in symbols_list:
        item = quotes_map.get(s) or _enrich_item({"error": "Missing row"}, s, engine_source="missing")

        # Preferred mapping: EnrichedQuote schema mapper
        if EnrichedQuote and isinstance(item, dict):
            try:
                eq = EnrichedQuote.from_unified(item)  # type: ignore
                row = eq.to_row(headers)               # type: ignore
                row = list(row)
                if len(row) < len(headers):
                    row.extend([None] * (len(headers) - len(row)))
                elif len(row) > len(headers):
                    row = row[: len(headers)]
                rows.append(row)
                continue
            except Exception as e:
                item = dict(item)
                item["error"] = item.get("error") or f"Mapping error: {e}"

        # Fallback mapping by header name
        row2 = _dict_to_row_by_headers(item, headers)
        if len(row2) < len(headers):
            row2.extend([None] * (len(headers) - len(row2)))
        elif len(row2) > len(headers):
            row2 = row2[: len(headers)]
        rows.append(row2)

    return {
        "status": "success",
        "headers": headers,
        "rows": rows,
        "version": ENRICHED_ROUTE_VERSION,
        "meta": {
            **(meta or {}),
            "route_version": ENRICHED_ROUTE_VERSION,
            "sheet": sheet_name,
            "symbols_count": len(symbols_list),
            "time_utc": _now_utc_iso(),
            "time_riyadh": _riyadh_iso(),
        },
    }


__all__ = ["router"]
