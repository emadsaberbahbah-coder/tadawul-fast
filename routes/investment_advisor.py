# routes/investment_advisor.py
"""
routes/investment_advisor.py
------------------------------------------------------------
Tadawul Fast Bridge — Investment Advisor Routes (GOOGLE SHEETS SAFE)
FULL REPLACEMENT — v2.2.0 (Advanced + Config-Aware + Diagnostics + Gains)

Endpoints
- GET  /v1/advisor/health
- GET  /v1/advisor/options
- POST /v1/advisor/recommendations
- POST /v1/advisor/run                 (alias)

Primary goals
- ✅ PROD SAFE: never crash on missing optional modules (config/core/engine).
- ✅ Sheets safe: headers NEVER empty on ok/error; rows are rectangular.
- ✅ Auth: open if no tokens configured; supports dynamic header name (routes.config.AUTH_HEADER_NAME)
        + Authorization: Bearer <token>
        + optional ?token=... when ALLOW_QUERY_TOKEN=1
- ✅ Payload hardening: accepts many shapes (GAS, JSON, strings, percent-ish).
- ✅ Diagnostics: snapshot cache hit stats by source + filtering summaries.
- ✅ Optional post-processing: compute Expected Gain/Loss (1M/3M) if possible.

Canonical alignment
- required_roi_1m / required_roi_3m accepted as percent or ratio; sent to core as ratio (0..1).
- expected_roi_1m/3m/12m and forecast_price_1m/3m/12m are preserved if returned by core.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from fastapi import APIRouter, Body, Query, Request

logger = logging.getLogger("routes.investment_advisor")

ADVISOR_ROUTE_VERSION = "2.2.0"
router = APIRouter(prefix="/v1/advisor", tags=["investment_advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}

# Canonical/default pages (must match GAS tab names)
DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

# Sheets-safe default headers (minimum stable contract)
# (Core may return its own headers; we only fallback to these.)
TT_ADVISOR_DEFAULT_HEADERS: List[str] = [
    "Rank",
    "Symbol",
    "Origin",
    "Name",
    "Market",
    "Currency",
    "Price",
    "Advisor Score",
    "Action",
    "Allocation %",
    "Allocation Amount",
    "Expected ROI % (1M)",
    "Forecast Price (1M)",
    "Expected ROI % (3M)",
    "Forecast Price (3M)",
    "Expected ROI % (12M)",
    "Forecast Price (12M)",
    "Risk Bucket",
    "Confidence Bucket",
    "Reason (Explain)",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
]

# Columns we can compute if missing (we add them at the end, never reorder existing headers)
GAIN_COL_1M = "Expected Gain/Loss 1M"
GAIN_COL_3M = "Expected Gain/Loss 3M"


# =============================================================================
# Time helpers
# =============================================================================
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso(utc_iso: Optional[str] = None) -> str:
    tz = timezone(timedelta(hours=3))
    try:
        if utc_iso:
            s = str(utc_iso).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.now(timezone.utc)
        return dt.astimezone(tz).isoformat()
    except Exception:
        return datetime.now(tz).isoformat()


# =============================================================================
# Config/Auth integration (routes.config preferred)
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
    # de-dupe preserve order
    out: List[str] = []
    seen = set()
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _auth_header_name() -> str:
    rc = _routes_config_mod()
    name = None
    if rc is not None:
        name = getattr(rc, "AUTH_HEADER_NAME", None)
        # Some configs expose settings with auth_header_name
        if not name:
            try:
                gs = getattr(rc, "get_settings", None)
                if callable(gs):
                    s = gs()
                    name = getattr(s, "auth_header_name", None)
            except Exception:
                name = None
    return str(name or "X-APP-TOKEN")


def _extract_token_from_request(request: Request, query_token: Optional[str]) -> Optional[str]:
    # 1) Header (dynamic) + X-APP-TOKEN fallback
    hdr = _auth_header_name()
    t = (request.headers.get(hdr) or request.headers.get("X-APP-TOKEN") or "").strip()
    if t:
        return t

    # 2) Bearer
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        bt = auth.split(" ", 1)[1].strip()
        if bt:
            return bt

    # 3) Query token only if enabled
    if _truthy(os.getenv("ALLOW_QUERY_TOKEN", "")):
        qt = (query_token or "").strip()
        if qt:
            return qt

    return None


def _auth_ok_request(request: Request, query_token: Optional[str]) -> bool:
    """
    Uses routes.config if available; else env-only.
    Open mode if no tokens configured.
    """
    rc = _routes_config_mod()
    provided = _extract_token_from_request(request, query_token)

    # Preferred: config helper
    if rc is not None:
        # If config provides auth_ok_request(x_app_token, authorization, query_token)
        fn = getattr(rc, "auth_ok_request", None)
        if callable(fn):
            try:
                # Provide both header + bearer in the same call (config decides priority)
                x_app = request.headers.get(_auth_header_name()) or request.headers.get("X-APP-TOKEN")
                authorization = request.headers.get("Authorization")
                return bool(fn(x_app_token=x_app, authorization=authorization, query_token=query_token))
            except Exception:
                pass

        # Else if config provides auth_ok(x_app_token)
        fn2 = getattr(rc, "auth_ok", None)
        if callable(fn2):
            try:
                # best-effort: pass the token we extracted
                return bool(fn2(provided))
            except Exception:
                pass

        # Else if config provides allowed_tokens()
        fn3 = getattr(rc, "allowed_tokens", None)
        if callable(fn3):
            try:
                allowed = fn3() or []
                if not allowed:
                    return True
                return bool(provided and provided.strip() in set([str(x).strip() for x in allowed if str(x).strip()]))
            except Exception:
                pass

    # Fallback: env-only
    allowed = _allowed_tokens_env()
    if not allowed:
        return True
    return bool(provided and provided.strip() in set(allowed))


# =============================================================================
# Engine resolution + snapshot probes
# =============================================================================
async def _resolve_engine(request: Request) -> Any:
    # 1) app.state.engine
    try:
        st = getattr(request.app, "state", None)
        eng = getattr(st, "engine", None) if st else None
        if eng:
            return eng
    except Exception:
        pass

    # 2) singleton (best-effort)
    try:
        from core.data_engine_v2 import get_engine  # type: ignore
        eng2 = get_engine()
        # handle async or sync
        if hasattr(eng2, "__await__"):
            return await eng2  # type: ignore[misc]
        return eng2
    except Exception:
        return None


def _probe_engine_snapshots(engine: Any, sources: List[str]) -> Tuple[Dict[str, bool], int]:
    """
    Returns (hits_map, hit_count)
    - Works with engines exposing get_cached_sheet_snapshot(name)
    - Tries minor normalization for common sheet keys
    """
    hits: Dict[str, bool] = {}
    if not sources:
        return hits, 0

    getter = getattr(engine, "get_cached_sheet_snapshot", None) if engine else None
    if not callable(getter):
        for s in sources:
            hits[s] = False
        return hits, 0

    # Try common variants
    variants = {
        "Market_Leaders": ["Market_Leaders", "MARKET_LEADERS"],
        "Global_Markets": ["Global_Markets", "GLOBAL_MARKETS"],
        "Mutual_Funds": ["Mutual_Funds", "MUTUAL_FUNDS"],
        "Commodities_FX": ["Commodities_FX", "COMMODITIES_FX"],
    }

    hit_count = 0
    for s in sources:
        ok = False
        cand = variants.get(s, [s, str(s).upper(), str(s).lower()])
        for c in cand:
            try:
                snap = getter(c)
                ok = bool(isinstance(snap, dict) and isinstance(snap.get("rows"), list))
            except Exception:
                ok = False
            if ok:
                break
        hits[s] = ok
        hit_count += 1 if ok else 0

    return hits, hit_count


# =============================================================================
# Normalizers (payload hardening)
# =============================================================================
def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        s = str(v).strip().replace(",", "")
        if s == "":
            return None
        if s.endswith("%"):
            s = s[:-1]
        return float(s)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    try:
        return int(float(str(v).strip()))
    except Exception:
        return None


def _clamp_int(v: Any, default: int, lo: int, hi: int) -> int:
    x = _safe_int(v)
    if x is None:
        x = default
    return max(lo, min(hi, x))


def _normalize_sources(v: Any) -> List[str]:
    if v is None:
        return list(DEFAULT_SOURCES)

    if isinstance(v, str):
        s = v.strip()
        if not s or s.upper() == "ALL":
            return list(DEFAULT_SOURCES)
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or list(DEFAULT_SOURCES)

    if isinstance(v, list):
        parts = [str(x).strip() for x in v if str(x or "").strip()]
        if len(parts) == 1 and parts[0].upper() == "ALL":
            return list(DEFAULT_SOURCES)
        return parts or list(DEFAULT_SOURCES)

    return list(DEFAULT_SOURCES)


def _normalize_tickers(v: Any) -> List[str]:
    if v is None:
        return []

    # Accept list or string; string can be csv OR space-separated
    items: List[str] = []
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        s = s.replace(",", " ")
        items = [p.strip() for p in s.split() if p.strip()]
    elif isinstance(v, list):
        for it in v:
            s = str(it or "").strip()
            if s:
                s = s.replace(",", " ")
                items.extend([p.strip() for p in s.split() if p.strip()])
    else:
        s = str(v).strip()
        if s:
            items = [s]

    seen = set()
    out: List[str] = []
    for t in items:
        u = t.strip().upper()
        if not u or u == "SYMBOL" or u == "TICKER":
            continue
        if u.startswith("#"):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _normalize_list(v: Any) -> Optional[List[str]]:
    if v is None:
        return None
    if isinstance(v, list):
        out = [str(x).strip() for x in v if str(x or "").strip()]
        return out or None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or None
    return None


def _percent_to_ratio(v: Any, *, default_ratio: float) -> float:
    """
    Accepts:
      0.10 -> 0.10
      10   -> 0.10 (assumed percent)
      "10%" -> 0.10
      "0.1" -> 0.10
    """
    x = _safe_float(v)
    if x is None:
        return default_ratio
    # if appears like percent
    if x > 1.0:
        return float(x) / 100.0
    # ratio
    if x < 0.0:
        return default_ratio
    return float(x)


def _normalize_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces payload keys expected by core.investment_advisor.run_investment_advisor
    """
    raw = dict(body or {})

    sources = _normalize_sources(raw.get("sources") or raw.get("data_sources") or raw.get("pages"))
    risk = str(raw.get("risk") or raw.get("risk_bucket") or "Moderate").strip() or "Moderate"
    confidence = str(raw.get("confidence") or raw.get("confidence_bucket") or "High").strip() or "High"

    tickers = _normalize_tickers(raw.get("tickers") or raw.get("symbols") or raw.get("universe") or "")

    amount = raw.get("amount")
    if amount is None:
        amount = raw.get("invest_amount")
    if amount is None:
        amount = raw.get("amount_to_invest")
    invest_amount = _safe_float(amount) or 0.0

    top_n = raw.get("top_n")
    if top_n is None:
        top_n = raw.get("max_items")
    if top_n is None:
        top_n = raw.get("number_of_stocks")
    top_n_i = _clamp_int(top_n, default=10, lo=1, hi=500)

    include_news = raw.get("include_news")
    if include_news is None:
        include_news = raw.get("includeNews")
    include_news_b = True if include_news is None else (_truthy(include_news) if isinstance(include_news, str) else bool(include_news))

    # required ROI: accept many aliases, send to core as ratio
    req_1m_ratio = _percent_to_ratio(
        raw.get("required_roi_1m") or raw.get("required_return_1m") or raw.get("target_roi_1m"),
        default_ratio=0.05,
    )
    req_3m_ratio = _percent_to_ratio(
        raw.get("required_roi_3m") or raw.get("required_return_3m") or raw.get("target_roi_3m"),
        default_ratio=0.10,
    )

    currency = str(raw.get("currency") or "SAR").strip().upper() or "SAR"

    # Filters (pass-through if present)
    min_price = _safe_float(raw.get("min_price") or raw.get("minPrice"))
    max_price = _safe_float(raw.get("max_price") or raw.get("maxPrice"))
    exclude_sectors = _normalize_list(raw.get("exclude_sectors") or raw.get("excludeSectors") or raw.get("exclude_sectors_csv"))
    liquidity_min = _safe_float(raw.get("liquidity_min") or raw.get("min_liquidity"))
    max_position_pct = _safe_float(raw.get("max_position_pct"))
    min_position_pct = _safe_float(raw.get("min_position_pct"))

    as_of_utc = raw.get("as_of_utc") or raw.get("asOfUtc") or None

    payload: Dict[str, Any] = {
        "sources": sources,
        "risk": risk,
        "confidence": confidence,
        "required_roi_1m": req_1m_ratio,
        "required_roi_3m": req_3m_ratio,
        "top_n": top_n_i,
        "invest_amount": float(invest_amount or 0.0),
        "include_news": bool(include_news_b),
        "currency": currency,
        "as_of_utc": as_of_utc,
        "min_price": min_price,
        "max_price": max_price,
        "exclude_sectors": exclude_sectors,
        "liquidity_min": liquidity_min,
        "max_position_pct": max_position_pct,
        "min_position_pct": min_position_pct,
        # tickers override (restrict universe)
        "tickers": tickers if tickers else None,
        # diagnostics (safe extras)
        "_diag_tickers_count": len(tickers),
        "_diag_tickers_sample": tickers[:50],
    }
    return payload


# =============================================================================
# Response safety + computed gains
# =============================================================================
def _rectangularize(headers: List[Any], rows: List[Any]) -> Tuple[List[str], List[List[Any]]]:
    h = [str(x).strip() for x in (headers or []) if str(x).strip()]
    if not h:
        h = list(TT_ADVISOR_DEFAULT_HEADERS)

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


def _find_col(headers: List[str], needles: Sequence[str]) -> Optional[int]:
    # loose match
    hlow = [str(x or "").strip().lower() for x in headers]
    for i, hh in enumerate(hlow):
        for n in needles:
            if n in hh:
                return i
    return None


def _parse_percent_cell(x: Any) -> Optional[float]:
    """
    Returns ratio (0..1) if possible.
    Accepts:
      10, "10", "10%", "0.1"
    """
    v = _safe_float(x)
    if v is None:
        return None
    if v > 1.0:
        return v / 100.0
    if v < 0.0:
        return None
    return v


def _maybe_add_gain_columns(headers: List[str], rows: List[List[Any]], *, currency: str) -> Tuple[List[str], List[List[Any]], Dict[str, Any]]:
    """
    If we can identify Allocation Amount + ROI columns, compute gains.
    Adds columns at END only (does not reorder).
    """
    meta: Dict[str, Any] = {"gains_added": False}

    if not headers or not rows:
        return headers, rows, meta

    # Detect allocation amount
    idx_alloc = _find_col(headers, ["allocation amount", "allocated amount"])
    if idx_alloc is None:
        return headers, rows, meta

    # Detect ROI columns
    idx_roi_1m = _find_col(headers, ["expected roi % (1m)", "expected roi (1m)", "expected return (1m)", "expected roi 1m"])
    idx_roi_3m = _find_col(headers, ["expected roi % (3m)", "expected roi (3m)", "expected return (3m)", "expected roi 3m"])

    if idx_roi_1m is None and idx_roi_3m is None:
        return headers, rows, meta

    # Only add if not already present
    has_1m = any(str(h).strip().lower() == GAIN_COL_1M.lower() for h in headers)
    has_3m = any(str(h).strip().lower() == GAIN_COL_3M.lower() for h in headers)

    new_headers = list(headers)
    add_1m = not has_1m
    add_3m = not has_3m

    if not add_1m and not add_3m:
        return headers, rows, meta

    if add_1m:
        new_headers.append(f"{GAIN_COL_1M} ({currency})")
    if add_3m:
        new_headers.append(f"{GAIN_COL_3M} ({currency})")

    total_alloc = 0.0
    total_gain_1m = 0.0
    total_gain_3m = 0.0

    new_rows: List[List[Any]] = []
    for r in rows:
        rr = list(r)
        alloc = _safe_float(rr[idx_alloc]) or 0.0
        total_alloc += alloc

        g1 = ""
        g3 = ""
        if idx_roi_1m is not None:
            roi1 = _parse_percent_cell(rr[idx_roi_1m])
            if roi1 is not None:
                g1v = alloc * roi1
                g1 = round(g1v, 2)
                total_gain_1m += float(g1v)
        if idx_roi_3m is not None:
            roi3 = _parse_percent_cell(rr[idx_roi_3m])
            if roi3 is not None:
                g3v = alloc * roi3
                g3 = round(g3v, 2)
                total_gain_3m += float(g3v)

        if add_1m:
            rr.append(g1)
        if add_3m:
            rr.append(g3)

        new_rows.append(rr)

    meta.update(
        {
            "gains_added": True,
            "total_allocated": round(total_alloc, 2),
            "total_expected_gain_1m": round(total_gain_1m, 2),
            "total_expected_gain_3m": round(total_gain_3m, 2),
        }
    )
    return new_headers, new_rows, meta


def _envelope(status: str, *, headers: List[str], rows: List[List[Any]], error: Optional[str], meta: Dict[str, Any]) -> Dict[str, Any]:
    # Always include route/version/timestamps
    base = {
        "status": status,
        "headers": headers,
        "rows": rows,
        "error": error,
        "version": ADVISOR_ROUTE_VERSION,
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
        "meta": meta or {},
    }
    # never emit None keys that break some GAS JSON parsers
    if base["error"] is None:
        base.pop("error", None)
    return base


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health")
@router.get("/ping")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = await _resolve_engine(request)
    engine_name = type(engine).__name__ if engine is not None else "none"
    engine_version = None
    if engine is not None:
        engine_version = getattr(engine, "ENGINE_VERSION", None) or getattr(engine, "engine_version", None)

    open_mode = len(_allowed_tokens_env()) == 0
    rc = _routes_config_mod()
    try:
        if rc is not None and callable(getattr(rc, "allowed_tokens", None)):
            open_mode = len(rc.allowed_tokens() or []) == 0  # type: ignore[attr-defined]
    except Exception:
        pass

    return _envelope(
        "ok",
        headers=["key", "value"],
        rows=[
            ["module", "routes.investment_advisor"],
            ["route_version", ADVISOR_ROUTE_VERSION],
            ["engine", engine_name],
            ["engine_version", str(engine_version or "")],
            ["auth_header", _auth_header_name()],
            ["open_mode", str(open_mode)],
            ["default_sources", ",".join(DEFAULT_SOURCES)],
            ["default_headers_len", str(len(TT_ADVISOR_DEFAULT_HEADERS))],
            ["time_utc", _utc_iso()],
            ["time_riyadh", _riyadh_iso()],
        ],
        error=None,
        meta={},
    )


@router.get("/options")
async def advisor_options() -> Dict[str, Any]:
    """
    Handy for building dropdowns / UI on GAS side.
    """
    return {
        "status": "ok",
        "version": ADVISOR_ROUTE_VERSION,
        "auth_header": _auth_header_name(),
        "defaults": {
            "sources": list(DEFAULT_SOURCES),
            "risk": ["Low", "Moderate", "High"],
            "confidence": ["Low", "Medium", "High"],
            "currency": ["SAR", "USD", "EUR"],
        },
        "notes": {
            "required_roi_fields": "Accepts percent or ratio. Examples: 10, '10%', 0.1",
            "tickers": "Accepts list OR string with comma/space separators",
        },
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
    }


async def _run_core(request: Request, payload: Dict[str, Any], *, debug: int = 0) -> Dict[str, Any]:
    # Lazy import (prod safe)
    try:
        from core.investment_advisor import run_investment_advisor  # type: ignore
    except Exception as exc:
        logger.exception("Advisor core import failed: %s", exc)
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        return _envelope(
            "error",
            headers=h,
            rows=r,
            error=f"Advisor core missing: {exc}",
            meta={"route_version": ADVISOR_ROUTE_VERSION},
        )

    engine = await _resolve_engine(request)

    # Snapshot diagnostics (helps explain 0 rows issues)
    sources = payload.get("sources") or []
    cache_hits, cache_hit_count = _probe_engine_snapshots(engine, sources if isinstance(sources, list) else [])

    # Execute
    started = time.time()
    try:
        result = run_investment_advisor(payload, engine=engine)  # type: ignore
        if not isinstance(result, dict):
            h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
            return _envelope(
                "error",
                headers=h,
                rows=r,
                error="Advisor core returned non-dict result",
                meta={"route_version": ADVISOR_ROUTE_VERSION},
            )

        headers = result.get("headers") if isinstance(result.get("headers"), list) else []
        rows = result.get("rows") if isinstance(result.get("rows"), list) else []
        meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}

        # Always rectangular + never empty headers
        headers2, rows2 = _rectangularize(headers, rows)

        # Optional gains computation (non-breaking: add columns at end only)
        currency = str(payload.get("currency") or "SAR").strip().upper() or "SAR"
        headers3, rows3, gains_meta = _maybe_add_gain_columns(headers2, rows2, currency=currency)

        processing_ms = round((time.time() - started) * 1000, 2)

        # Route-level diagnostics meta
        meta_out = dict(meta or {})
        meta_out.update(
            {
                "route_version": ADVISOR_ROUTE_VERSION,
                "processing_time_ms": processing_ms,
                "engine_present": bool(engine is not None),
                "engine_type": type(engine).__name__ if engine is not None else "none",
                "sheet_cache_hits": cache_hits,
                "sheet_cache_hit_count": cache_hit_count,
                "diag_tickers_count": payload.get("_diag_tickers_count", 0),
                "diag_tickers_sample": payload.get("_diag_tickers_sample", []),
                "sources": payload.get("sources") or [],
                "risk": payload.get("risk"),
                "confidence": payload.get("confidence"),
                "required_roi_1m_ratio": payload.get("required_roi_1m"),
                "required_roi_3m_ratio": payload.get("required_roi_3m"),
                "invest_amount": payload.get("invest_amount"),
                "filters": {
                    "min_price": payload.get("min_price"),
                    "max_price": payload.get("max_price"),
                    "exclude_sectors": payload.get("exclude_sectors"),
                    "liquidity_min": payload.get("liquidity_min"),
                    "max_position_pct": payload.get("max_position_pct"),
                    "min_position_pct": payload.get("min_position_pct"),
                },
                "last_updated_riyadh": _riyadh_iso(),
            }
        )
        meta_out.update(gains_meta or {})

        # Determine status from core meta if present
        status = "ok"
        if isinstance(meta, dict) and meta.get("ok") is False:
            status = "error"

        # If error status but no rows, keep headers stable
        return _envelope(status, headers=headers3, rows=rows3, error=(meta.get("error") if status == "error" else None), meta=meta_out)

    except Exception as exc:
        logger.exception("Advisor core execution failed: %s", exc)
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        meta_out = {
            "route_version": ADVISOR_ROUTE_VERSION,
            "engine_present": bool(engine is not None),
            "engine_type": type(engine).__name__ if engine is not None else "none",
            "sheet_cache_hits": cache_hits,
            "sheet_cache_hit_count": cache_hit_count,
        }
        if int(debug or 0):
            meta_out["debug_error"] = str(exc)
        return _envelope("error", headers=h, rows=r, error=f"Advisor execution failed: {exc}", meta=meta_out)


@router.post("/recommendations")
async def advisor_recommendations(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 adds extra diagnostics"),
) -> Dict[str, Any]:
    # Sheets-safe: HTTP 200 always
    if not _auth_ok_request(request, token):
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        return _envelope(
            "error",
            headers=h,
            rows=r,
            error="Unauthorized: invalid or missing token.",
            meta={"route_version": ADVISOR_ROUTE_VERSION, "auth_header": _auth_header_name()},
        )

    payload = _normalize_payload(body or {})

    # Log minimal summary (no secrets)
    try:
        logger.info(
            "[Advisor] /recommendations sources=%s top_n=%s invest=%s tickers=%s",
            ",".join(payload.get("sources") or []),
            payload.get("top_n"),
            payload.get("invest_amount"),
            payload.get("_diag_tickers_count"),
        )
    except Exception:
        pass

    return await _run_core(request, payload, debug=debug)


@router.post("/run")
async def advisor_run(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 adds extra diagnostics"),
) -> Dict[str, Any]:
    """
    Alias endpoint for compatibility: POST /v1/advisor/run
    """
    # Same behavior as /recommendations
    if not _auth_ok_request(request, token):
        h, r = _rectangularize(list(TT_ADVISOR_DEFAULT_HEADERS), [])
        return _envelope(
            "error",
            headers=h,
            rows=r,
            error="Unauthorized: invalid or missing token.",
            meta={"route_version": ADVISOR_ROUTE_VERSION, "auth_header": _auth_header_name()},
        )

    payload = _normalize_payload(body or {})

    try:
        logger.info(
            "[Advisor] /run sources=%s top_n=%s invest=%s tickers=%s",
            ",".join(payload.get("sources") or []),
            payload.get("top_n"),
            payload.get("invest_amount"),
            payload.get("_diag_tickers_count"),
        )
    except Exception:
        pass

    return await _run_core(request, payload, debug=debug)


__all__ = ["router"]
