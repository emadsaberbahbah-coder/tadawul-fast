# routes/investment_advisor.py
"""
Investment Advisor Routes (v1.2.0) — PROD SAFE / GOOGLE SHEETS FRIENDLY

What this fixes vs v1.1.0
- ✅ Adds the missing endpoint your GAS client calls: POST /v1/advisor/run
    (aliases to /v1/advisor/recommendations)
- ✅ Keeps existing endpoints:
    - GET  /v1/advisor/health
    - GET  /v1/advisor/ping
    - POST /v1/advisor/recommendations
- ✅ More tolerant payload normalization to match your Apps Script (55_AI_Advisor.gs):
    GAS sends: invest_amount, top_n, include_news, required_roi_1m/3m sometimes as "3%" strings
    Backend expects: amount/max_items etc -> we map safely.
- ✅ Always returns HTTP 200 JSON envelope {status,...} even on errors.
- ✅ Token guard aligned with enriched_quote:
    - X-APP-TOKEN
    - Authorization: Bearer <token> (or raw)
    - optional ?token if ALLOW_QUERY_TOKEN=1
- ✅ Lazy-import advisor engine so missing modules don't crash startup
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Header, Query, Request

logger = logging.getLogger("routes.investment_advisor")

ADVISOR_ROUTE_VERSION = "1.2.0"
router = APIRouter(prefix="/v1/advisor", tags=["investment_advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]


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
    Keep in sync with platform env.
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
    return out


def _envelope_error(msg: str, *, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {"status": "error", "error": msg}
    if extra:
        out.update(extra)
    out.setdefault("route_version", ADVISOR_ROUTE_VERSION)
    out.setdefault("time_utc", _utc_iso())
    return out


# ---------------------------------------------------------------------
# Payload helpers (normalize GAS -> engine)
# ---------------------------------------------------------------------
def _parse_percentish(v: Any, *, default: Optional[float] = None) -> Optional[float]:
    """
    Accepts:
      - 0.05 (ratio) => returns 5.0 if <= 1.0
      - 5 or 5.0 => returns 5.0
      - "5%" => returns 5.0
      - "0.05" => returns 5.0 (treated as ratio if <=1)
    Returns percent (0..100), or default if cannot parse.
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

    # If ratio-like
    if 0 <= x <= 1:
        return x * 100.0
    return x


def _normalize_sources(v: Any) -> List[str]:
    if v is None:
        return list(DEFAULT_SOURCES)
    if isinstance(v, str):
        s = v.strip()
        if not s or s.upper() == "ALL":
            return list(DEFAULT_SOURCES)
        # comma-separated string
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return parts or list(DEFAULT_SOURCES)
    if isinstance(v, list):
        parts = [str(x).strip() for x in v if str(x).strip()]
        if len(parts) == 1 and parts[0].upper() == "ALL":
            return list(DEFAULT_SOURCES)
        return parts or list(DEFAULT_SOURCES)
    return list(DEFAULT_SOURCES)


def _normalize_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces a stable engine-friendly payload while staying backward compatible.
    Supports both:
      - GAS keys: invest_amount, top_n, include_news
      - Engine keys: amount, max_items, max_positions
    """
    out = dict(body or {})

    # sources
    out["sources"] = _normalize_sources(out.get("sources"))

    # risk/confidence as strings
    if out.get("risk") is None:
        out["risk"] = "Moderate"
    if out.get("confidence") is None:
        out["confidence"] = "High"

    # amount
    if out.get("amount") is None:
        out["amount"] = out.get("invest_amount")
    try:
        out["amount"] = float(out.get("amount") or 0)
    except Exception:
        out["amount"] = 0.0

    # max_items / max_positions
    if out.get("max_items") is None:
        out["max_items"] = out.get("top_n")
    if out.get("max_items") is None:
        out["max_items"] = out.get("max_positions")
    try:
        out["max_items"] = int(out.get("max_items") or 10)
    except Exception:
        out["max_items"] = 10

    # include_news
    if out.get("include_news") is None:
        out["include_news"] = out.get("includeNews")
    out["include_news"] = bool(_truthy(out["include_news"])) if isinstance(out.get("include_news"), str) else bool(out.get("include_news"))

    # ROI (percent)
    # GAS sends strings like "3%" and "10%"
    out["required_roi_1m"] = _parse_percentish(out.get("required_roi_1m"), default=_parse_percentish(out.get("target_roi_1m"), default=5.0))
    out["required_roi_3m"] = _parse_percentish(out.get("required_roi_3m"), default=_parse_percentish(out.get("target_roi_3m"), default=10.0))

    return out


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
        }
    )


async def _run_engine(request: Request, payload: Dict[str, Any], *, debug: int = 0) -> Dict[str, Any]:
    # Lazy import of advisor engine (prod safe)
    try:
        from core.investment_advisor_engine import build_recommendations  # type: ignore
    except Exception as exc:
        logger.exception("Advisor engine import failed: %s", exc)
        return _envelope_error(f"Advisor engine missing: {exc}", extra={"headers": [], "rows": [], "items": []})

    try:
        result = await build_recommendations(request=request, payload=payload)  # type: ignore
        if not isinstance(result, dict):
            return _envelope_error("Advisor engine returned non-dict result", extra={"headers": [], "rows": [], "items": []})

        # Common shapes supported:
        # - {items:[...], meta:{...}}
        # - {headers:[...], rows:[...], meta:{...}}  (preferred for Google Sheets write)
        result.setdefault("items", [])
        result.setdefault("headers", [])
        result.setdefault("rows", [])
        result.setdefault("meta", {})
        return _envelope_ok(result)
    except Exception as exc:
        logger.exception("Advisor engine execution failed: %s", exc)
        extra: Dict[str, Any] = {"headers": [], "rows": [], "items": []}
        if int(debug or 0):
            extra["trace"] = str(exc)
        return _envelope_error(str(exc), extra=extra)


def _auth_guard_or_envelope(
    x_app_token: Optional[str],
    authorization: Optional[str],
    token_qs: Optional[str],
) -> Optional[Dict[str, Any]]:
    provided = _extract_token(x_app_token, authorization, token_qs)
    if not _auth_ok(provided):
        return _envelope_error("Unauthorized: invalid or missing token.", extra={"headers": [], "rows": [], "items": []})
    return None


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
    return await _run_engine(request, payload, debug=debug)


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
    ✅ This matches your Google Apps Script client:
      - GAS calls: POST /v1/advisor/run
    It behaves the same as /recommendations.
    """
    denied = _auth_guard_or_envelope(x_app_token, authorization, token)
    if denied:
        return denied

    payload = _normalize_payload(body or {})
    return await _run_engine(request, payload, debug=debug)


__all__ = ["router"]
