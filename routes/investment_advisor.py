# routes/investment_advisor.py
"""
Investment Advisor Routes (v1.1.0) — PROD SAFE / GOOGLE SHEETS FRIENDLY

Fixes
- Ensures optional router import path works: routes.investment_advisor (module)
- Always returns HTTP 200 JSON envelope: {status,...}
- Token guard aligned with enriched_quote:
    - X-APP-TOKEN
    - Authorization: Bearer <token> (or raw)
    - optional ?token if ALLOW_QUERY_TOKEN=1
- Lazy-import advisor engine so missing modules don't crash startup

Endpoints
- GET  /v1/advisor/health
- GET  /v1/advisor/ping
- POST /v1/advisor/recommendations
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Header, Query, Request

logger = logging.getLogger("routes.investment_advisor")

ADVISOR_ROUTE_VERSION = "1.1.0"
router = APIRouter(prefix="/v1/advisor", tags=["investment_advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}


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
# Defaults
# ---------------------------------------------------------------------
DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]


@router.get("/health")
@router.get("/ping")
async def advisor_health(request: Request) -> Dict[str, Any]:
    # Engine status (best effort)
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


@router.post("/recommendations")
async def advisor_recommendations(
    request: Request,
    body: Dict[str, Any] = Body(default_factory=dict),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    token: Optional[str] = Query(default=None, description="(optional) query token if ALLOW_QUERY_TOKEN=1"),
    debug: int = Query(0, description="debug=1 includes a short error trace if available"),
) -> Dict[str, Any]:
    """
    Sheets-friendly: always returns HTTP 200 JSON.
    Expected body:
      {
        "sources": ["Market_Leaders", ...] OR "ALL",
        "risk": "Low|Moderate|High" (optional),
        "confidence": "Low|Moderate|High" (optional),
        "required_roi_1m": 5 (percent),
        "required_roi_3m": 15 (percent),
        "max_items": 10,
        "amount": 10000
      }
    """
    provided = _extract_token(x_app_token, authorization, token)
    if not _auth_ok(provided):
        return _envelope_error("Unauthorized: invalid or missing token.", extra={"items": []})

    # Soft-normalize sources if caller uses "ALL"
    try:
        src = body.get("sources")
        if isinstance(src, str) and src.strip().upper() == "ALL":
            body["sources"] = list(DEFAULT_SOURCES)
        elif src is None:
            body["sources"] = list(DEFAULT_SOURCES)
    except Exception:
        pass

    # Lazy import of advisor engine (prod safe)
    try:
        from core.investment_advisor_engine import build_recommendations  # type: ignore
    except Exception as exc:
        logger.exception("Advisor engine import failed: %s", exc)
        return _envelope_error(f"Advisor engine missing: {exc}", extra={"items": []})

    try:
        result = await build_recommendations(request=request, payload=body)  # type: ignore

        if not isinstance(result, dict):
            return _envelope_error("Advisor engine returned non-dict result", extra={"items": []})

        # Ensure stable envelope
        result.setdefault("items", [])
        result = _envelope_ok(result)
        return result

    except Exception as exc:
        logger.exception("Advisor recommendations failed: %s", exc)
        extra: Dict[str, Any] = {"items": []}
        if int(debug or 0):
            extra["trace"] = str(exc)
        return _envelope_error(str(exc), extra=extra)


__all__ = ["router"]
