# routes/advisor.py
"""
routes/advisor.py
------------------------------------------------------------
Tadawul Fast Bridge — Advisor Routes (v2.4.0)
(ENGINE-AWARE + AUTH-GUARDED + GAS-SAFE + MULTI-SHAPE INPUT + STABLE OUTPUT)

What this route guarantees (Google Sheets safe)
- ✅ Never returns empty headers (always TT_ADVISOR_DEFAULT_HEADERS at minimum).
- ✅ Never crashes the whole request because of one bad ticker (robust parsing).
- ✅ Accepts tickers/symbols in MANY shapes:
    - List[str]
    - String "AAPL,MSFT 1120.SR"
    - Dict payloads where tickers live in {tickers,symbols,all_tickers,ksa_tickers,global_tickers}
    - Query param tickers="AAPL,MSFT 1120.SR" (optional)
- ✅ Uses app.state.engine when present; otherwise best-effort core engine resolve.
- ✅ Auth guard supports:
    - X-APP-TOKEN
    - Authorization: Bearer <token>
    - ?token=... only if ALLOW_QUERY_TOKEN=1
- ✅ Riyadh timestamps are injected at route-level into meta.
- ✅ /run alias endpoint kept for backward compatibility.

Contract:
POST /v1/advisor/recommendations
POST /v1/advisor/run   (alias)

Response (AdvisorResponse):
{ status, headers, rows, meta, error? }

Notes:
- This file keeps imports lightweight and avoids expensive initialization at import-time.
"""

from __future__ import annotations

import inspect
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, Body, Header, Query, Request

# ---- Safe schema import (preferred) -----------------------------------------
try:
    from schemas.advisor import (  # type: ignore
        AdvisorRequest,
        AdvisorResponse,
        TT_ADVISOR_DEFAULT_HEADERS,
    )
except Exception:  # pragma: no cover
    # Absolute fallback: keep route alive even if schema module missing.
    # We define permissive pydantic models (extra ignore) to keep the app running.
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
        # minimal compatibility fields
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
ADVISOR_ROUTE_VERSION = "2.4.0"

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_RIYADH_TZ = timezone(timedelta(hours=3))


# =============================================================================
# Time / Meta
# =============================================================================
def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _riyadh_iso() -> str:
    return datetime.now(_RIYADH_TZ).isoformat()


def _to_riyadh_iso(utc_iso: Optional[str]) -> str:
    if not utc_iso:
        return ""
    try:
        dt = datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
        return dt.astimezone(_RIYADH_TZ).isoformat()
    except Exception:
        return ""


# =============================================================================
# Auth
# =============================================================================
def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in _TRUTHY


_ALLOW_QUERY_TOKEN = _env_bool("ALLOW_QUERY_TOKEN", False)


def _allowed_tokens() -> List[str]:
    toks: List[str] = []
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN", "TFB_APP_TOKEN"):
        v = (os.getenv(k) or "").strip()
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
    # common KSA aliases
    if s.startswith("TADAWUL:"):
        s = s.split(":", 1)[1].strip()
    if s.endswith(".TADAWUL"):
        s = s[: -len(".TADAWUL")].strip()
    if s.endswith(".SA"):
        s = s[:-3] + ".SR"
    # numeric KSA
    if s.isdigit() and 3 <= len(s) <= 6:
        return f"{s}.SR"
    return s


def _norm_symbol(x: Any) -> str:
    # Try project normalizer, otherwise fallback
    try:
        from core.symbols.normalize import normalize_symbol  # type: ignore

        out = normalize_symbol(x)
        out = (out or "").strip().upper()
        return out or _fallback_norm_symbol(x)
    except Exception:
        return _fallback_norm_symbol(x)


def _split_string_tickers(s: str) -> List[str]:
    # supports "AAPL,MSFT 1120.SR" and "AAPL|MSFT" etc.
    raw = (s or "").replace(",", " ").replace("|", " ").replace(";", " ")
    parts = [p.strip() for p in raw.split() if p.strip()]
    return parts


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
    Extract tickers from many possible shapes:
    - list[str]
    - str
    - dict with keys: tickers/symbols/all_tickers/ksa_tickers/global_tickers
    """
    if obj is None:
        return []

    # string
    if isinstance(obj, str):
        return _dedupe_preserve(_split_string_tickers(obj))

    # list/tuple
    if isinstance(obj, (list, tuple)):
        return _dedupe_preserve([str(x) for x in obj if str(x or "").strip()])

    # dict
    if isinstance(obj, dict):
        acc: List[str] = []
        for k in ("tickers", "symbols", "all_tickers", "ksa_tickers", "global_tickers"):
            v = obj.get(k)
            if isinstance(v, str):
                acc.extend(_split_string_tickers(v))
            elif isinstance(v, (list, tuple)):
                acc.extend([str(x) for x in v if str(x or "").strip()])
        return _dedupe_preserve(acc)

    # pydantic / objects
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
      - req.tickers / req.symbols (list or str)
      - raw JSON body keys (tickers/symbols/all_tickers/ksa_tickers/global_tickers)
      - query param tickers="..."
    """
    tickers: List[str] = []

    # 1) from request object fields
    raw = None
    if getattr(req, "tickers", None) not in (None, "", [], ()):
        raw = getattr(req, "tickers", None)
    elif getattr(req, "symbols", None) not in (None, "", [], ()):
        raw = getattr(req, "symbols", None)

    tickers.extend(_extract_tickers_from_any(raw))

    # 2) from raw body (if present)
    if raw_body:
        tickers.extend(_extract_tickers_from_any(raw_body))

    # 3) from query param
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
    # preferred: app.state.engine
    try:
        st = getattr(request.app, "state", None)
        if st is not None and hasattr(st, "engine"):
            eng = getattr(st, "engine", None)
            if eng is not None:
                return eng
    except Exception:
        pass

    # fallback: core.data_engine_v2.get_engine() if available
    try:
        from core.data_engine_v2 import get_engine  # type: ignore

        return await _maybe_await(get_engine())
    except Exception:
        return None


# =============================================================================
# Output Safety
# =============================================================================
def _safe_headers(h: Any) -> List[str]:
    if isinstance(h, list) and len(h) > 0:
        out = [str(x).strip() for x in h if str(x).strip()]
        if out:
            return out
    return list(TT_ADVISOR_DEFAULT_HEADERS)


def _safe_rows(r: Any) -> List[List[Any]]:
    if isinstance(r, list):
        out: List[List[Any]] = []
        for row in r:
            if isinstance(row, (list, tuple)):
                out.append(list(row))
            elif isinstance(row, dict):
                # keep dict rows as a single cell JSON-like (avoid crash)
                out.append([str(row)])
            else:
                out.append([row])
        return out
    return []


def _status_from_core(result: Dict[str, Any]) -> str:
    """
    Normalize status from core output into {success|partial|error}.
    """
    st = str(result.get("status") or "").strip().lower()
    if st in ("success", "ok", "200"):
        return "success"
    if st in ("partial", "warning", "warnings"):
        return "partial"
    if st in ("error", "fail", "failed"):
        return "error"
    # if meta says ok=false
    meta = result.get("meta")
    if isinstance(meta, dict) and meta.get("ok") is False:
        return "error"
    return "success"


# =============================================================================
# Routes
# =============================================================================
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
    start_time = time.time()

    # ---- Auth ---------------------------------------------------------------
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

    # ---- Normalize tickers override ----------------------------------------
    tickers = _normalize_tickers(req, raw_body=body, q_tickers=tickers_q)

    # ---- Resolve engine (shared state) -------------------------------------
    engine = await _resolve_engine(request)

    # ---- Build payload for core advisor ------------------------------------
    core_payload: Dict[str, Any] = {
        "sources": getattr(req, "sources", None),
        "risk": getattr(req, "risk", None),
        "confidence": getattr(req, "confidence", None),
        "required_roi_1m": getattr(req, "required_roi_1m", None),
        "required_roi_3m": getattr(req, "required_roi_3m", None),
        "top_n": getattr(req, "top_n", None),
        "invest_amount": getattr(req, "invest_amount", None),
        "currency": getattr(req, "currency", None),
        "include_news": getattr(req, "include_news", None),
        "as_of_utc": getattr(req, "as_of_utc", None),
        "min_price": getattr(req, "min_price", None),
        "max_price": getattr(req, "max_price", None),
        # optional constraints (schema may have them)
        "exclude_sectors": getattr(req, "exclude_sectors", None),
        "liquidity_min": getattr(req, "liquidity_min", None),
        "max_position_pct": getattr(req, "max_position_pct", None),
        "min_position_pct": getattr(req, "min_position_pct", None),
    }

    # tickers override restrict universe
    if tickers:
        core_payload["tickers"] = tickers

    # ---- Run core advisor (resilient) --------------------------------------
    try:
        from core.investment_advisor import run_investment_advisor  # type: ignore
    except Exception:
        logger.exception("core.investment_advisor import failed.")
        return AdvisorResponse(
            status="error",
            error="Advisor service module missing on server.",
            headers=list(TT_ADVISOR_DEFAULT_HEADERS),
            rows=[],
            meta={
                "ok": False,
                "route_version": ADVISOR_ROUTE_VERSION,
                "engine_status": "injected" if engine else "missing",
                "processing_time_ms": round((time.time() - start_time) * 1000, 2),
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
                "tickers_override_count": len(tickers),
                "tickers_override_mode": bool(tickers),
            },
        )

    try:
        result = run_investment_advisor(core_payload, engine=engine)
        result = await _maybe_await(result)  # support async core impl
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
                "processing_time_ms": round((time.time() - start_time) * 1000, 2),
                "last_updated_utc": _utc_iso(),
                "last_updated_riyadh": _riyadh_iso(),
                "tickers_override_count": len(tickers),
                "tickers_override_mode": bool(tickers),
            },
        )

    # ---- Output stabilization ----------------------------------------------
    headers = _safe_headers(result.get("headers"))
    rows = _safe_rows(result.get("rows"))
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}

    processing_time_ms = round((time.time() - start_time) * 1000, 2)

    # Route-level meta overrides (authoritative)
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
        }
    )

    status = _status_from_core(result)
    meta.setdefault("ok", status != "error")

    # Keep error string stable
    err = None
    if status == "error":
        err = str(result.get("error") or meta.get("error") or "Advisor failed")

    return AdvisorResponse(
        status=status,
        headers=headers,
        rows=rows,
        meta=meta,
        error=err,
    )


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


__all__ = ["router"]
