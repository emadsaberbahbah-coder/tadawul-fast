# routes/investment_advisor.py
"""
Tadawul Fast Bridge — Investment Advisor Routes (GOOGLE SHEETS SAFE)
Full Replacement — v1.4.0

Why this revision (vs your v1.3.2 text)
- ✅ FIX #1 (most important): map GAS payload -> advisor core correctly
  GAS sends: amount / max_items / required_roi_1m (percent) / required_roi_3m (percent)
  Your core expects: invest_amount / top_n / required_roi_1m (ratio) / required_roi_3m (ratio)
  This mismatch is why you got: tickers_count=1262 but Universe Scan Size = 0 candidates.
- ✅ FIX #2: call the revised core: core.investment_advisor.run_investment_advisor(..., engine=app.state.engine)
  so it can actually read cached sheet rows from the ENGINE instance.
- ✅ Still keeps your endpoints and token guard.
- ✅ Always returns Sheets-safe headers (never empty when status="ok").
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Header, Query, Request

logger = logging.getLogger("routes.investment_advisor")

ADVISOR_ROUTE_VERSION = "1.4.0"
router = APIRouter(prefix="/v1/advisor", tags=["investment_advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

# Sheets-safe default headers (for GAS write safety)
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
    "Expected ROI % (3M)",
    "Risk Bucket",
    "Confidence Bucket",
    "Reason (Explain)",
    "Data Source",
    "Data Quality",
    "Last Updated (UTC)",
]


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------
# Token guard (open if no token is set)
# ---------------------------------------------------------------------
def _read_token_from_auth(authorization: Optional[str]) -> Optional[str]:
    auth = (authorization or "").strip()
    if not auth:
        return None
    low = auth.lower()
    if low.startswith("bearer "):
        t = auth.split(" ", 1)[1].strip()
        return t or None
    return auth or None


@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    """
    If none set => open mode.
    """
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


def _extract_token(
    x_app_token: Optional[str],
    authorization: Optional[str],
    token_qs: Optional[str],
) -> Optional[str]:
    t = (x_app_token or "").strip()
    if t:
        return t

    t2 = _read_token_from_auth(authorization)
    if t2:
        return t2

    if _truthy(os.getenv("ALLOW_QUERY_TOKEN", "0")):
        tq = (token_qs or "").strip()
        if tq:
            return tq

    return None


def _auth_ok(provided: Optional[str]) -> bool:
    allowed = _allowed_tokens()
    if not allowed:
        return True  # open mode
    pt = (provided or "").strip()
    return bool(pt and pt in allowed)


def _envelope_ok(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    out.setdefault("status", "ok")
    out.setdefault("route_version", ADVISOR_ROUTE_VERSION)
    out.setdefault("time_utc", _utc_iso())
    out.setdefault("meta", {})
    return out


def _envelope_error(msg: str, *, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {"status": "error", "error": msg}
    if extra:
        out.update(extra)
    out.setdefault("route_version", ADVISOR_ROUTE_VERSION)
    out.setdefault("time_utc", _utc_iso())
    out.setdefault("meta", {})
    return out


# ---------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------
def _parse_percentish(v: Any, *, default: Optional[float] = None) -> Optional[float]:
    """
    Returns percent (0..100), or default if cannot parse.
    Accepts:
      - 0.05 (ratio) => 5.0
      - 5 => 5.0
      - "5%" => 5.0
      - "0.05" => 5.0 (treated as ratio if <= 1)
    """
    if v is None or v == "":
        return default
    try:
        if isinstance(v, str):
            s = v.strip().replace(" ", "")
            if not s:
                return default
            if s.endswith("%"):
                s = s[:-1]
            x = float(s)
        else:
            x = float(v)
    except Exception:
        return default

    if 0 <= x <= 1:
        return x * 100.0
    return x


def _percent_to_ratio(p: Any, *, default_ratio: float) -> float:
    """
    Accept percent-ish and return ratio.
    """
    pct = _parse_percentish(p, default=default_ratio * 100.0)
    try:
        return float(pct or 0.0) / 100.0
    except Exception:
        return default_ratio


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
        parts = [str(x).strip() for x in v if str(x).strip()]
        if len(parts) == 1 and parts[0].upper() == "ALL":
            return list(DEFAULT_SOURCES)
        return parts or list(DEFAULT_SOURCES)
    return list(DEFAULT_SOURCES)


def _normalize_tickers(v: Any) -> List[str]:
    """
    Accept:
    - ["1120.SR", "AAPL.US"]
    - "1120.SR,AAPL.US"
    - "1120.SR"
    - None -> []
    Returns uppercase, trimmed, de-duplicated (stable order).
    """
    if v is None:
        return []

    items: List[str] = []
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        items = [p.strip() for p in s.split(",") if p.strip()]
    elif isinstance(v, list):
        for it in v:
            if it is None:
                continue
            s = str(it).strip()
            if s:
                items.append(s)
    else:
        s = str(v).strip()
        if s:
            items = [s]

    seen = set()
    out: List[str] = []
    for t in items:
        u = t.strip().upper()
        if not u or u == "SYMBOL":
            continue
        if u.startswith("#"):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _normalize_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces:
      - advisor_core_payload: keys that core.investment_advisor.run_investment_advisor expects
      - also keeps original extras in meta/logs if needed
    """
    raw = dict(body or {})

    sources = _normalize_sources(raw.get("sources"))

    risk = raw.get("risk") or "Moderate"
    confidence = raw.get("confidence") or "High"

    # carry tickers for diagnostics only (core currently scans cached sheets)
    tickers = _normalize_tickers(raw.get("tickers") or raw.get("symbols"))

    # GAS sends amount OR invest_amount
    amount = raw.get("amount")
    if amount is None:
        amount = raw.get("invest_amount")
    try:
        amount_f = float(amount or 0.0)
    except Exception:
        amount_f = 0.0

    # GAS sends max_items OR top_n OR max_positions
    max_items = raw.get("max_items")
    if max_items is None:
        max_items = raw.get("top_n")
    if max_items is None:
        max_items = raw.get("max_positions")
    try:
        max_items_i = int(max_items or 10)
    except Exception:
        max_items_i = 10

    # include_news
    include_news = raw.get("include_news")
    if include_news is None:
        include_news = raw.get("includeNews")
    include_news_b = bool(_truthy(include_news)) if isinstance(include_news, str) else bool(include_news if include_news is not None else True)

    # REQUIRED ROI:
    # routes receives percent-ish; core expects ratio
    req_1m_ratio = _percent_to_ratio(raw.get("required_roi_1m") or raw.get("target_roi_1m"), default_ratio=0.05)
    req_3m_ratio = _percent_to_ratio(raw.get("required_roi_3m") or raw.get("target_roi_3m"), default_ratio=0.10)

    currency = str(raw.get("currency") or "SAR").strip().upper() or "SAR"

    advisor_core_payload: Dict[str, Any] = {
        "sources": sources,
        "risk": risk,
        "confidence": confidence,
        "required_roi_1m": req_1m_ratio,
        "required_roi_3m": req_3m_ratio,
        "top_n": max_items_i,
        "invest_amount": amount_f,
        "include_news": include_news_b,
        "currency": currency,
    }

    # keep some diagnostics
    advisor_core_payload["_diag_tickers_count"] = len(tickers)
    advisor_core_payload["_diag_tickers"] = tickers[:50]  # avoid huge
    return advisor_core_payload


# ---------------------------------------------------------------------
# Table conversion + safety enforcement
# ---------------------------------------------------------------------
def _ensure_ok_headers(resp: Dict[str, Any]) -> Dict[str, Any]:
    status = str(resp.get("status") or "ok").lower()
    headers = resp.get("headers")
    rows = resp.get("rows")

    if not isinstance(headers, list):
        headers = []
    if not isinstance(rows, list):
        rows = []

    if status == "ok" and len(headers) == 0:
        headers = list(TT_ADVISOR_DEFAULT_HEADERS)

    if headers and rows:
        w = len(headers)
        fixed_rows: List[List[Any]] = []
        for r in rows:
            rr = list(r) if isinstance(r, list) else []
            if len(rr) < w:
                rr.extend([""] * (w - len(rr)))
            elif len(rr) > w:
                rr = rr[:w]
            fixed_rows.append(rr)
        rows = fixed_rows

    resp["headers"] = headers
    resp["rows"] = rows
    return resp


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@router.get("/health")
@router.get("/ping")
async def advisor_health(request: Request) -> Dict[str, Any]:
    engine = None
    try:
        st = getattr(request.app, "state", None)
        engine = getattr(st, "engine", None) if st else None
    except Exception:
        engine = None

    engine_name = type(engine).__name__ if engine is not None else "none"
    engine_version = None
    if engine is not None:
        engine_version = getattr(engine, "ENGINE_VERSION", None) or getattr(engine, "engine_version", None)

    return _envelope_ok(
        {
            "module": "routes.investment_advisor",
            "engine": engine_name,
            "engine_version": engine_version,
            "auth": "open" if not _allowed_tokens() else "token",
            "default_headers_len": len(TT_ADVISOR_DEFAULT_HEADERS),
        }
    )


def _auth_guard_or_envelope(
    x_app_token: Optional[str],
    authorization: Optional[str],
    token_qs: Optional[str],
) -> Optional[Dict[str, Any]]:
    provided = _extract_token(x_app_token, authorization, token_qs)
    if not _auth_ok(provided):
        return _envelope_error(
            "Unauthorized: invalid or missing token.",
            extra={"headers": list(TT_ADVISOR_DEFAULT_HEADERS), "rows": [], "items": []},
        )
    return None


async def _run_core(request: Request, payload: Dict[str, Any], *, debug: int = 0) -> Dict[str, Any]:
    """
    Calls core.investment_advisor.run_investment_advisor(..., engine=app.state.engine)
    """
    # Lazy import (prod safe)
    try:
        from core.investment_advisor import run_investment_advisor  # type: ignore
    except Exception as exc:
        logger.exception("Advisor core import failed: %s", exc)
        return _ensure_ok_headers(
            _envelope_error(
                f"Advisor core missing: {exc}",
                extra={"headers": list(TT_ADVISOR_DEFAULT_HEADERS), "rows": [], "items": []},
            )
        )

    engine = None
    try:
        st = getattr(request.app, "state", None)
        engine = getattr(st, "engine", None) if st else None
    except Exception:
        engine = None

    try:
        result = run_investment_advisor(payload, engine=engine)  # type: ignore
        if not isinstance(result, dict):
            return _ensure_ok_headers(
                _envelope_error(
                    "Advisor core returned non-dict result",
                    extra={"headers": list(TT_ADVISOR_DEFAULT_HEADERS), "rows": [], "items": []},
                )
            )

        headers = result.get("headers") if isinstance(result.get("headers"), list) else []
        rows = result.get("rows") if isinstance(result.get("rows"), list) else []
        meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}

        out = _envelope_ok(
            {
                "headers": headers,
                "rows": rows,
                "items": [],  # core returns table
                "count": len(rows or []),
                "meta": meta,
                "engine_version": meta.get("engine_version") or None,
            }
        )

        # add route diagnostics
        try:
            out_meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
            out_meta.update(
                {
                    "route_version": ADVISOR_ROUTE_VERSION,
                    "diag_tickers_count": payload.get("_diag_tickers_count", 0),
                }
            )
            out["meta"] = out_meta
        except Exception:
            pass

        return _ensure_ok_headers(out)

    except Exception as exc:
        logger.exception("Advisor core execution failed: %s", exc)
        extra: Dict[str, Any] = {"headers": list(TT_ADVISOR_DEFAULT_HEADERS), "rows": [], "items": []}
        if int(debug or 0):
            extra["trace"] = str(exc)
        return _ensure_ok_headers(_envelope_error(str(exc), extra=extra))


@router.post("/recommendations")
async def advisor_recommendations(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 includes a short error trace if available"),
) -> Dict[str, Any]:
    denied = _auth_guard_or_envelope(x_app_token, authorization, token)
    if denied:
        return denied

    payload = _normalize_payload(body or {})

    try:
        logger.info(
            "[Advisor] /recommendations sources=%s top_n=%s amount=%s diag_tickers=%s",
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
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 includes a short error trace if available"),
) -> Dict[str, Any]:
    """
    Alias endpoint used by some GAS clients: POST /v1/advisor/run
    """
    denied = _auth_guard_or_envelope(x_app_token, authorization, token)
    if denied:
        return denied

    payload = _normalize_payload(body or {})

    try:
        logger.info(
            "[Advisor] /run sources=%s top_n=%s amount=%s diag_tickers=%s",
            ",".join(payload.get("sources") or []),
            payload.get("top_n"),
            payload.get("invest_amount"),
            payload.get("_diag_tickers_count"),
        )
    except Exception:
        pass

    return await _run_core(request, payload, debug=debug)


__all__ = ["router"]
