# routes/advisor.py
"""
routes/advisor.py
------------------------------------------------------------
Tadawul Fast Bridge — Advisor Routes (v2.9.0)
(ENGINE-AWARE + AUTH-GUARDED + GAS-SAFE + MULTI-SHAPE INPUT + DETERMINISTIC OUTPUT + TIMEOUTS + ALLOCATION)

What this route guarantees (Google Sheets safe)
- ✅ Never returns empty headers (always TT_ADVISOR_DEFAULT_HEADERS at minimum).
- ✅ Never crashes the whole request because of one bad ticker (robust parsing + placeholders).
- ✅ Accepts tickers/symbols in MANY shapes:
    - List[str]
    - String "AAPL,MSFT 1120.SR"
    - Dict payload keys: {tickers,symbols,all_tickers,ksa_tickers,global_tickers}
    - Query param tickers="AAPL,MSFT 1120.SR" (optional)
- ✅ Uses app.state.engine when present; otherwise best-effort core engine resolve.
- ✅ Auth guard supports:
    - X-APP-TOKEN
    - Authorization: Bearer <token>
    - ?token=... only if ALLOW_QUERY_TOKEN=1
- ✅ Riyadh timestamps injected at route-level into meta (ZoneInfo preferred).
- ✅ /run alias endpoint kept for backward compatibility.
- ✅ Stable output: rows are padded/truncated to headers length.
- ✅ Optional: enforces required ROI filters at route-level if core doesn't.
- ✅ Optional: safe allocation calculation if core doesn't provide it.

Contract:
POST /v1/advisor/recommendations
POST /v1/advisor/run   (alias)

Response (AdvisorResponse):
{ status, headers, rows, meta, error? }

Notes:
- Lightweight imports at module import-time. Heavy deps are lazy.
"""

from __future__ import annotations

import inspect
import logging
import os
import time
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, Query, Request

# ---- Safe schema import (preferred) -----------------------------------------
try:
    from schemas.advisor import (  # type: ignore
        AdvisorRequest,
        AdvisorResponse,
        TT_ADVISOR_DEFAULT_HEADERS,
    )

    _SCHEMAS_OK = True
except Exception:  # pragma: no cover
    _SCHEMAS_OK = False
    # Absolute fallback: keep route alive even if schema module missing.
    try:
        from pydantic import BaseModel, ConfigDict, Field  # type: ignore

        _PYDANTIC_V2 = True
    except Exception:  # pragma: no cover
        from pydantic import BaseModel, Field  # type: ignore

        ConfigDict = None  # type: ignore
        _PYDANTIC_V2 = False

    class _ExtraIgnore(BaseModel):
        if _PYDANTIC_V2:
            model_config = ConfigDict(extra="ignore")
        else:

            class Config:  # type: ignore
                extra = "ignore"

    class AdvisorRequest(_ExtraIgnore):  # type: ignore
        sources: Optional[Any] = None
        risk: Optional[str] = None
        confidence: Optional[str] = None
        required_roi_1m: Optional[float] = None
        required_roi_3m: Optional[float] = None
        top_n: Optional[int] = 50
        invest_amount: Optional[float] = None
        currency: Optional[str] = None
        include_news: Optional[bool] = True
        as_of_utc: Optional[str] = None
        min_price: Optional[float] = None
        max_price: Optional[float] = None
        tickers: Optional[Any] = None
        symbols: Optional[Any] = None
        # optional filters
        exclude_sectors: Optional[Any] = None
        liquidity_min: Optional[float] = None
        max_position_pct: Optional[float] = None
        min_position_pct: Optional[float] = None

    class AdvisorResponse(_ExtraIgnore):  # type: ignore
        status: str = "error"
        error: Optional[str] = None
        headers: List[str] = Field(default_factory=list)
        rows: List[List[Any]] = Field(default_factory=list)
        meta: Dict[str, Any] = Field(default_factory=dict)

    TT_ADVISOR_DEFAULT_HEADERS = [
        "Rank",
        "Symbol",
        "Name",
        "Source Sheet",
        "Price",
        "Risk Bucket",
        "Confidence Bucket",
        "Advisor Score",
        "Expected ROI % (1M)",
        "Forecast Price (1M)",
        "Expected ROI % (3M)",
        "Forecast Price (3M)",
        "Weight",
        "Allocated Amount (SAR)",
        "Expected Gain/Loss 1M (SAR)",
        "Expected Gain/Loss 3M (SAR)",
        "Reason (Explain)",
    ]

logger = logging.getLogger("routes.advisor")

router = APIRouter(prefix="/v1/advisor", tags=["advisor"])
ADVISOR_ROUTE_VERSION = "2.9.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enable", "enabled"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disable", "disabled"}


# =============================================================================
# Env helpers
# =============================================================================
def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, "")
    if not v:
        return default
    vv = v.lower()
    if vv in _TRUTHY:
        return True
    if vv in _FALSY:
        return False
    return default


def _env_int(name: str, default: int, *, lo: int = -10**9, hi: int = 10**9) -> int:
    v = _env_str(name, "")
    if not v:
        return int(default)
    try:
        x = int(float(v))
        return max(lo, min(hi, x))
    except Exception:
        return int(default)


def _env_float(name: str, default: float, *, lo: float = -1e18, hi: float = 1e18) -> float:
    v = _env_str(name, "")
    if not v:
        return float(default)
    try:
        x = float(v)
        return max(lo, min(hi, x))
    except Exception:
        return float(default)


_ALLOW_QUERY_TOKEN = _env_bool("ALLOW_QUERY_TOKEN", False)
_ROUTE_TIMEOUT_SEC = _env_float("ADVISOR_ROUTE_TIMEOUT_SEC", 75.0, lo=6.0, hi=240.0)  # soft
_DEFAULT_TOP_N = _env_int("ADVISOR_DEFAULT_TOP_N", 50, lo=1, hi=500)
_MAX_TOP_N = _env_int("ADVISOR_MAX_TOP_N", 200, lo=10, hi=1000)
_ENFORCE_ROUTE_FILTERS = _env_bool("ADVISOR_ENFORCE_ROUTE_FILTERS", True)


# =============================================================================
# Time / Meta (Riyadh via ZoneInfo preferred)
# =============================================================================
@lru_cache(maxsize=1)
def _riyadh_tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("Asia/Riyadh")
    except Exception:
        # fallback
        from datetime import timedelta

        return timezone(timedelta(hours=3))


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _riyadh_iso() -> str:
    return datetime.now(_riyadh_tz()).isoformat(timespec="seconds")


def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso:
        return ""
    try:
        dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_riyadh_tz()).isoformat(timespec="seconds")
    except Exception:
        return ""


# =============================================================================
# Auth
# =============================================================================
@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    toks: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = _env_str(k, "")
        if v:
            toks.append(v)
    # stable de-dupe
    out: List[str] = []
    seen = set()
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _extract_bearer(authorization: Optional[str]) -> str:
    a = (authorization or "").strip()
    if not a:
        return ""
    parts = a.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return ""


def _auth_ok(x_app_token: Optional[str], authorization: Optional[str], query_token: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True  # open-mode

    tok = (x_app_token or "").strip()
    if tok and tok in allowed:
        return True

    btok = _extract_bearer(authorization)
    if btok and btok in allowed:
        return True

    if _ALLOW_QUERY_TOKEN:
        qtok = (query_token or "").strip()
        if qtok and qtok in allowed:
            return True

    return False


# =============================================================================
# Symbol Normalization
# =============================================================================
def _fallback_norm_symbol(x: Any) -> str:
    s = ("" if x is None else str(x)).strip().upper()
    if not s:
        return ""
    s = s.replace(" ", "")
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")].strip()
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


@lru_cache(maxsize=1)
def _symbol_normalizer():
    try:
        from core.symbols.normalize import normalize_symbol  # type: ignore

        return normalize_symbol
    except Exception:
        return None


def _norm_symbol(x: Any) -> str:
    fn = _symbol_normalizer()
    if callable(fn):
        try:
            out = fn(x)
            out = (out or "").strip().upper()
            return out or _fallback_norm_symbol(x)
        except Exception:
            return _fallback_norm_symbol(x)
    return _fallback_norm_symbol(x)


def _split_string_tickers(s: str) -> List[str]:
    raw = (s or "").replace(",", " ").replace("|", " ").replace(";", " ")
    return [p.strip() for p in raw.split() if p.strip()]


def _dedupe_preserve(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in items or []:
        s = _norm_symbol(x)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _extract_tickers_from_any(obj: Any) -> List[str]:
    """
    Extract tickers from many shapes:
    - list[str]
    - str
    - dict keys: tickers/symbols/all_tickers/ksa_tickers/global_tickers
    - pydantic-like objects (model_dump/dict)
    """
    if obj is None:
        return []

    if isinstance(obj, str):
        return _dedupe_preserve(_split_string_tickers(obj))

    if isinstance(obj, (list, tuple)):
        return _dedupe_preserve([str(x) for x in obj if str(x or "").strip()])

    if isinstance(obj, dict):
        acc: List[str] = []
        for k in ("tickers", "symbols", "all_tickers", "ksa_tickers", "global_tickers"):
            v = obj.get(k)
            if isinstance(v, str):
                acc.extend(_split_string_tickers(v))
            elif isinstance(v, (list, tuple)):
                acc.extend([str(x) for x in v if str(x or "").strip()])
        return _dedupe_preserve(acc)

    # pydantic v2/v1 object
    try:
        d = None
        md = getattr(obj, "model_dump", None)
        if callable(md):
            d = md()
        if d is None:
            dd = getattr(obj, "dict", None)
            if callable(dd):
                d = dd()
        if isinstance(d, dict):
            return _extract_tickers_from_any(d)
    except Exception:
        pass

    return []


def _normalize_tickers(req: AdvisorRequest, raw_body: Optional[Dict[str, Any]] = None, q_tickers: str = "") -> List[str]:
    """
    Supports:
      - req.tickers / req.symbols
      - raw JSON body keys
      - query param tickers="..."
    """
    tickers: List[str] = []

    raw = None
    if getattr(req, "tickers", None) not in (None, "", [], ()):
        raw = getattr(req, "tickers", None)
    elif getattr(req, "symbols", None) not in (None, "", [], ()):
        raw = getattr(req, "symbols", None)

    tickers.extend(_extract_tickers_from_any(raw))

    if raw_body:
        tickers.extend(_extract_tickers_from_any(raw_body))

    if q_tickers and str(q_tickers).strip():
        tickers.extend(_extract_tickers_from_any(str(q_tickers)))

    return _dedupe_preserve(tickers)


# =============================================================================
# Engine Resolve
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _resolve_engine(request: Request) -> Optional[Any]:
    try:
        st = getattr(request.app, "state", None)
        if st is not None:
            eng = getattr(st, "engine", None)
            if eng is not None:
                return eng
    except Exception:
        pass

    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        return await _maybe_await(get_engine())
    except Exception:
        return None


# =============================================================================
# Output Safety
# =============================================================================
def _safe_headers(h: Any) -> List[str]:
    if isinstance(h, list) and h:
        out = [str(x).strip() for x in h if str(x).strip()]
        if out:
            return out
    return list(TT_ADVISOR_DEFAULT_HEADERS)


def _pad_row(row: List[Any], n: int) -> List[Any]:
    if len(row) == n:
        return row
    if len(row) < n:
        return row + [None] * (n - len(row))
    return row[:n]


def _safe_rows(r: Any, headers_len: int) -> List[List[Any]]:
    if not isinstance(r, list):
        return []
    out: List[List[Any]] = []
    for row in r:
        if isinstance(row, (list, tuple)):
            out.append(_pad_row(list(row), headers_len))
        elif isinstance(row, dict):
            out.append(_pad_row([str(row)], headers_len))
        else:
            out.append(_pad_row([row], headers_len))
    return out


def _status_from_core(result: Dict[str, Any]) -> str:
    st = str(result.get("status") or "").strip().lower()
    if st in ("success", "ok", "200"):
        return "success"
    if st in ("partial", "warning", "warnings"):
        return "partial"
    if st in ("error", "fail", "failed"):
        return "error"
    meta = result.get("meta")
    if isinstance(meta, dict) and meta.get("ok") is False:
        return "error"
    return "success"


def _idx(headers: List[str], name: str) -> int:
    """
    Case-insensitive header index lookup with fallback -1.
    """
    target = (name or "").strip().lower()
    if not target:
        return -1
    for i, h in enumerate(headers):
        if str(h or "").strip().lower() == target:
            return i
    return -1


def _maybe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _maybe_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(float(x))
    except Exception:
        return None


# =============================================================================
# Optional route-level filters + allocation stabilization
# =============================================================================
def _apply_route_filters(
    headers: List[str],
    rows: List[List[Any]],
    *,
    required_roi_1m: Optional[float],
    required_roi_3m: Optional[float],
    top_n: int,
) -> Tuple[List[List[Any]], Dict[str, Any]]:
    """
    Filters rows if ROI columns exist. If columns absent, does nothing.
    Returns (rows, stats).
    """
    stats: Dict[str, Any] = {"filtered": False, "before": len(rows), "after": len(rows)}

    if not rows:
        return rows, stats

    i_roi1 = _idx(headers, "Expected ROI % (1M)")
    i_roi3 = _idx(headers, "Expected ROI % (3M)")
    i_score = _idx(headers, "Advisor Score")

    if i_roi1 < 0 and i_roi3 < 0:
        return rows, stats

    def keep(row: List[Any]) -> bool:
        ok = True
        if required_roi_1m is not None and i_roi1 >= 0:
            v = _maybe_float(row[i_roi1]) if i_roi1 < len(row) else None
            if v is not None:
                ok = ok and (v >= float(required_roi_1m))
        if required_roi_3m is not None and i_roi3 >= 0:
            v = _maybe_float(row[i_roi3]) if i_roi3 < len(row) else None
            if v is not None:
                ok = ok and (v >= float(required_roi_3m))
        return ok

    filtered = [r for r in rows if keep(r)]

    # stable sort by Advisor Score desc if present
    if i_score >= 0:
        def score_key(r: List[Any]) -> float:
            v = _maybe_float(r[i_score]) if i_score < len(r) else None
            return -float(v) if v is not None else 0.0

        filtered.sort(key=score_key)

    filtered = filtered[: max(1, int(top_n or len(filtered)))]

    stats.update({"filtered": True, "after": len(filtered)})
    return filtered, stats


def _ensure_allocation_math(headers: List[str], rows: List[List[Any]], invest_amount: Optional[float]) -> Dict[str, Any]:
    """
    If core didn't compute allocated amount / expected gains but columns exist, compute best-effort.
    Returns stats.
    """
    stats: Dict[str, Any] = {"allocation_applied": False}

    if not rows or invest_amount is None:
        return stats

    invest = float(invest_amount)

    i_w = _idx(headers, "Weight")
    i_alloc = _idx(headers, "Allocated Amount (SAR)")
    i_roi1 = _idx(headers, "Expected ROI % (1M)")
    i_roi3 = _idx(headers, "Expected ROI % (3M)")
    i_gain1 = _idx(headers, "Expected Gain/Loss 1M (SAR)")
    i_gain3 = _idx(headers, "Expected Gain/Loss 3M (SAR)")

    if i_w < 0 or i_alloc < 0:
        return stats  # cannot allocate

    # if weights missing, derive by equal weights across rows
    weights: List[float] = []
    for r in rows:
        w = _maybe_float(r[i_w]) if i_w < len(r) else None
        weights.append(w if (w is not None and w > 0) else 0.0)

    if sum(weights) <= 0:
        # equal weights
        n = max(1, len(rows))
        weights = [1.0 / n] * n
        for r in rows:
            if i_w < len(r):
                r[i_w] = 1.0 / n

    # normalize weights
    s = sum(weights)
    if s <= 0:
        return stats
    weights = [w / s for w in weights]

    for idx_r, r in enumerate(rows):
        w = weights[idx_r]
        alloc = invest * w
        if i_alloc < len(r) and (r[i_alloc] in (None, "", 0, 0.0)):
            r[i_alloc] = round(alloc, 2)

        # gains from ROI if columns exist and empty
        if i_gain1 >= 0 and i_roi1 >= 0 and i_gain1 < len(r) and i_roi1 < len(r):
            if r[i_gain1] in (None, "", 0, 0.0):
                roi1 = _maybe_float(r[i_roi1])
                if roi1 is not None:
                    r[i_gain1] = round(alloc * (roi1 / 100.0), 2)

        if i_gain3 >= 0 and i_roi3 >= 0 and i_gain3 < len(r) and i_roi3 < len(r):
            if r[i_gain3] in (None, "", 0, 0.0):
                roi3 = _maybe_float(r[i_roi3])
                if roi3 is not None:
                    r[i_gain3] = round(alloc * (roi3 / 100.0), 2)

    stats["allocation_applied"] = True
    return stats


# =============================================================================
# Core Advisor Invocation (lazy + flexible)
# =============================================================================
async def _call_core_advisor(core_payload: Dict[str, Any], engine: Optional[Any]) -> Dict[str, Any]:
    """
    Try multiple core entrypoints (keeps backward compatibility).
    """
    # preferred
    try:
        from core.investment_advisor import run_investment_advisor  # type: ignore

        out = run_investment_advisor(core_payload, engine=engine)
        out = await _maybe_await(out)
        if isinstance(out, dict):
            return out
    except Exception:
        pass

    # alternate naming
    try:
        from core.advisor_engine import run_investment_advisor  # type: ignore

        out = run_investment_advisor(core_payload, engine=engine)
        out = await _maybe_await(out)
        if isinstance(out, dict):
            return out
    except Exception:
        pass

    # last resort
    raise RuntimeError("Advisor service module missing or returned invalid output.")


# =============================================================================
# Routes
# =============================================================================
@router.get("/health")
async def advisor_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "version": ADVISOR_ROUTE_VERSION,
        "time_utc": _utc_iso(),
        "time_riyadh": _riyadh_iso(),
        "schemas_ok": bool(_SCHEMAS_OK),
        "open_mode": (len(_allowed_tokens()) == 0),
        "allow_query_token": bool(_ALLOW_QUERY_TOKEN),
        "route_timeout_sec": _ROUTE_TIMEOUT_SEC,
    }


@router.post("/recommendations", response_model=AdvisorResponse)
async def advisor_recommendations(
    req: AdvisorRequest,
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    tickers_q: str = Query(default="", description="Optional tickers override: 'AAPL,MSFT 1120.SR'"),
    token: Optional[str] = Query(default=None, description="Optional token (only if ALLOW_QUERY_TOKEN=1)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> AdvisorResponse:
    """
    Main advisor endpoint used by Google Sheets.
    """
    t0 = time.perf_counter()

    # Auth
    if not _auth_ok(x_app_token, authorization, token):
        return AdvisorResponse(
            status="error",
            error="Unauthorized",
            headers=list(TT_ADVISOR_DEFAULT_HEADERS),
            rows=[],
            meta={
                "ok": False,
                "route_version": ADVISOR_ROUTE_VERSION,
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
            },
        )

    # Normalize tickers override
    tickers = _normalize_tickers(req, raw_body=body, q_tickers=tickers_q)

    # Resolve engine
    engine = await _resolve_engine(request)

    # Build payload
    top_n = getattr(req, "top_n", None)
    top_n = int(top_n) if top_n is not None else _DEFAULT_TOP_N
    top_n = max(1, min(_MAX_TOP_N, top_n))

    core_payload: Dict[str, Any] = {
        "sources": getattr(req, "sources", None),
        "risk": getattr(req, "risk", None),
        "confidence": getattr(req, "confidence", None),
        "required_roi_1m": getattr(req, "required_roi_1m", None),
        "required_roi_3m": getattr(req, "required_roi_3m", None),
        "top_n": top_n,
        "invest_amount": getattr(req, "invest_amount", None),
        "currency": getattr(req, "currency", None),
        "include_news": getattr(req, "include_news", None),
        "as_of_utc": getattr(req, "as_of_utc", None),
        "min_price": getattr(req, "min_price", None),
        "max_price": getattr(req, "max_price", None),
        "exclude_sectors": getattr(req, "exclude_sectors", None),
        "liquidity_min": getattr(req, "liquidity_min", None),
        "max_position_pct": getattr(req, "max_position_pct", None),
        "min_position_pct": getattr(req, "min_position_pct", None),
    }
    if tickers:
        core_payload["tickers"] = tickers

    # Call core with soft timeout
    try:
        remaining = _ROUTE_TIMEOUT_SEC
        result = await _maybe_await(
            asyncio_wait_for_if_available(_call_core_advisor(core_payload, engine), timeout=remaining)  # type: ignore
        )
        if not isinstance(result, dict):
            raise RuntimeError("Advisor returned invalid result type (expected dict).")
    except Exception as e:
        logger.exception("Advisor execution failed.")
        return AdvisorResponse(
            status="error",
            error=f"Advisor execution failed: {str(e)}",
            headers=list(TT_ADVISOR_DEFAULT_HEADERS),
            rows=[],
            meta={
                "ok": False,
                "route_version": ADVISOR_ROUTE_VERSION,
                "engine_status": "injected" if engine else "fallback",
                "processing_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
                "tickers_override_count": len(tickers),
                "tickers_override_mode": bool(tickers),
                "top_n": top_n,
            },
        )

    # Stabilize output
    headers = _safe_headers(result.get("headers"))
    rows = _safe_rows(result.get("rows"), headers_len=len(headers))
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}

    # Optional route-level filters (if enabled)
    filter_stats: Dict[str, Any] = {}
    if _ENFORCE_ROUTE_FILTERS:
        rows, filter_stats = _apply_route_filters(
            headers,
            rows,
            required_roi_1m=getattr(req, "required_roi_1m", None),
            required_roi_3m=getattr(req, "required_roi_3m", None),
            top_n=top_n,
        )

    # Optional allocation stabilization
    alloc_stats = _ensure_allocation_math(headers, rows, getattr(req, "invest_amount", None))

    # Meta override (route-authoritative)
    processing_time_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    meta.update(
        {
            "route_version": ADVISOR_ROUTE_VERSION,
            "advisor_route_version": ADVISOR_ROUTE_VERSION,
            "engine_status": "injected" if engine else "fallback",
            "processing_time_ms": processing_time_ms,
            "last_updated_utc": _utc_iso(),
            "last_updated_riyadh": _riyadh_iso(),
            "tickers_override_count": len(tickers),
            "tickers_override_mode": bool(tickers),
            "opportunities_found": len(rows),
            "headers_count": len(headers),
            "top_n": top_n,
            "filters": filter_stats,
            "allocation": alloc_stats,
        }
    )

    status = _status_from_core(result)
    # If we filtered everything out, return partial (not error)
    if status == "success" and filter_stats.get("filtered") and len(rows) == 0:
        status = "partial"
        meta["warning"] = "No rows met route-level filters."

    meta.setdefault("ok", status != "error")

    err = None
    if status == "error":
        err = str(result.get("error") or meta.get("error") or "Advisor failed")

    return AdvisorResponse(status=status, headers=headers, rows=rows, meta=meta, error=err)


@router.post("/run", response_model=AdvisorResponse)
async def advisor_run(
    req: AdvisorRequest,
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    tickers_q: str = Query(default="", description="Optional tickers override: 'AAPL,MSFT 1120.SR'"),
    token: Optional[str] = Query(default=None, description="Optional token (only if ALLOW_QUERY_TOKEN=1)"),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> AdvisorResponse:
    """
    Alias endpoint for compatibility with older GAS / backend calls.
    """
    return await advisor_recommendations(
        req=req,
        request=request,
        body=body,
        tickers_q=tickers_q,
        token=token,
        x_app_token=x_app_token,
        authorization=authorization,
    )


# =============================================================================
# Small helper: asyncio.wait_for without importing asyncio at top-level of call site
# =============================================================================
async def asyncio_wait_for_if_available(coro, timeout: float):
    """
    Use asyncio.wait_for if available; keep compatibility if environment stubs differ.
    """
    import asyncio as _asyncio

    return await _asyncio.wait_for(coro, timeout=timeout)


__all__ = ["router"]
