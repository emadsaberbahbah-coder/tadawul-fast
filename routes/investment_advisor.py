# tadawul-fast/routes/investment_advisor.py
"""
Tadawul Fast Bridge — Investment Advisor Routes (GOOGLE SHEETS SAFE)
Full Replacement — v1.3.2

Fixes in v1.3.2
- ✅ NEVER returns empty headers when status="ok"
  - If engine returns {count:0, items:[], headers:[]} -> we inject default headers and rows=[]
- ✅ Supports payload.tickers / payload.symbols (list OR comma string)
- ✅ Supports both engine output modes:
  1) items[] (dicts)  -> converted to headers+rows
  2) headers+rows     -> used as-is, but headers are enforced non-empty for ok
- ✅ Keeps endpoints:
  - GET  /v1/advisor/health
  - GET  /v1/advisor/ping
  - POST /v1/advisor/recommendations
  - POST /v1/advisor/run  (alias)
- ✅ Token guard (same behavior as your v1.2.0)
- ✅ PROD SAFE: lazy engine import, never crashes startup

NOTE
- Your Render log shows this file is the one mounted:
  Mounted router: investment_advisor (routes.investment_advisor.router) router_prefix=/v1/advisor
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Header, Query, Request

logger = logging.getLogger("routes.investment_advisor")

ADVISOR_ROUTE_VERSION = "1.3.2"
router = APIRouter(prefix="/v1/advisor", tags=["investment_advisor"])

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
DEFAULT_SOURCES = ["Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX"]

# Sheets-safe default headers (must match your schemas/advisor.py v0.3.0 default headers)
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
    Produces a stable engine-friendly payload while staying backward compatible.
    Also carries tickers/symbols through.
    """
    out = dict(body or {})

    # sources
    out["sources"] = _normalize_sources(out.get("sources"))

    # risk/confidence
    out["risk"] = (out.get("risk") or "Moderate")
    out["confidence"] = (out.get("confidence") or "High")

    # tickers/symbols universe
    out["tickers"] = _normalize_tickers(out.get("tickers"))
    out["symbols"] = _normalize_tickers(out.get("symbols"))

    # amount
    if out.get("invest_amount") is not None and out.get("amount") is None:
        out["amount"] = out.get("invest_amount")
    try:
        out["amount"] = float(out.get("amount") or 0)
    except Exception:
        out["amount"] = 0.0

    # top_n / max_items
    if out.get("top_n") is not None and out.get("max_items") is None:
        out["max_items"] = out.get("top_n")
    if out.get("max_positions") is not None and out.get("max_items") is None:
        out["max_items"] = out.get("max_positions")
    try:
        out["max_items"] = int(out.get("max_items") or 10)
    except Exception:
        out["max_items"] = 10

    # include_news
    if out.get("include_news") is None:
        out["include_news"] = out.get("includeNews")
    out["include_news"] = bool(_truthy(out["include_news"])) if isinstance(out.get("include_news"), str) else bool(out.get("include_news"))

    # ROI (percent, for engine compatibility; your schema may convert to ratios elsewhere)
    out["required_roi_1m"] = _parse_percentish(out.get("required_roi_1m"), default=_parse_percentish(out.get("target_roi_1m"), default=5.0))
    out["required_roi_3m"] = _parse_percentish(out.get("required_roi_3m"), default=_parse_percentish(out.get("target_roi_3m"), default=10.0))

    # currency
    cur = str(out.get("currency") or "SAR").strip().upper()
    out["currency"] = cur or "SAR"

    return out


# ---------------------------------------------------------------------
# Table conversion + safety enforcement
# ---------------------------------------------------------------------
def _items_to_table(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Convert engine items into Sheets table with stable headers.
    """
    headers = list(TT_ADVISOR_DEFAULT_HEADERS)
    rows: List[List[Any]] = []

    for i, it in enumerate(items or [], start=1):
        rows.append(
            [
                i,
                it.get("symbol", "") or it.get("ticker", "") or "",
                it.get("origin", "") or "",
                it.get("name", "") or "",
                it.get("market", "") or "",
                it.get("currency", "") or "",
                it.get("price", "") or it.get("current_price", "") or "",
                it.get("advisor_score", "") or it.get("score", "") or "",
                it.get("action", "") or it.get("recommendation", "") or "",
                it.get("allocation_pct", "") or it.get("allocation_percent", "") or "",
                it.get("allocation_amount", "") or it.get("amount", "") or "",
                it.get("expected_roi_1m_pct", "") or it.get("roi_1m_pct", "") or "",
                it.get("expected_roi_3m_pct", "") or it.get("roi_3m_pct", "") or "",
                it.get("risk_bucket", "") or it.get("risk", "") or "",
                it.get("confidence_bucket", "") or it.get("confidence", "") or "",
                it.get("reason", "") or it.get("explain", "") or "",
                it.get("data_source", "") or it.get("source", "") or "",
                it.get("data_quality", "") or it.get("quality", "") or "",
                it.get("last_updated_utc", "") or it.get("updated_at_utc", "") or "",
            ]
        )

    return {"headers": headers, "rows": rows}


def _ensure_ok_headers(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    CRITICAL:
    If status is ok but headers empty -> set default headers and keep rows (or empty).
    Also normalize row widths when rows exist.
    """
    status = str(resp.get("status") or "ok").lower()
    headers = resp.get("headers")
    rows = resp.get("rows")

    if not isinstance(headers, list):
        headers = []
    if not isinstance(rows, list):
        rows = []

    if status == "ok" and len(headers) == 0:
        headers = list(TT_ADVISOR_DEFAULT_HEADERS)

    # normalize row widths
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


async def _run_engine(request: Request, payload: Dict[str, Any], *, debug: int = 0) -> Dict[str, Any]:
    # Lazy import (prod safe)
    try:
        from core.investment_advisor_engine import build_recommendations  # type: ignore
    except Exception as exc:
        logger.exception("Advisor engine import failed: %s", exc)
        return _ensure_ok_headers(
            _envelope_error(
                f"Advisor engine missing: {exc}",
                extra={"headers": list(TT_ADVISOR_DEFAULT_HEADERS), "rows": [], "items": []},
            )
        )

    try:
        result = await build_recommendations(request=request, payload=payload)  # type: ignore
        if not isinstance(result, dict):
            return _ensure_ok_headers(
                _envelope_error(
                    "Advisor engine returned non-dict result",
                    extra={"headers": list(TT_ADVISOR_DEFAULT_HEADERS), "rows": [], "items": []},
                )
            )

        # normalize shape
        result.setdefault("meta", {})
        result.setdefault("items", [])
        result.setdefault("headers", [])
        result.setdefault("rows", [])

        # If engine returned items (recommended path), convert to table when headers empty
        items = result.get("items") if isinstance(result.get("items"), list) else []
        headers = result.get("headers") if isinstance(result.get("headers"), list) else []
        rows = result.get("rows") if isinstance(result.get("rows"), list) else []

        if items and (not headers or len(headers) == 0):
            table = _items_to_table(items)  # stable headers+rows
            headers = table["headers"]
            rows = table["rows"]

        # If count==0 and headers still empty => fixed later by _ensure_ok_headers
        out = _envelope_ok(
            {
                **result,
                "headers": headers,
                "rows": rows,
                "engine_version": result.get("engine_version") or result.get("engine") or None,
            }
        )

        # Always ensure headers not empty for ok
        out = _ensure_ok_headers(out)

        # Add safe diagnostics
        try:
            meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
            meta.update(
                {
                    "route_version": ADVISOR_ROUTE_VERSION,
                    "tickers_count": len(payload.get("tickers") or payload.get("symbols") or []),
                }
            )
            out["meta"] = meta
        except Exception:
            pass

        return out

    except Exception as exc:
        logger.exception("Advisor engine execution failed: %s", exc)
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

    # Helpful server log
    try:
        logger.info("[Advisor] /recommendations tickers=%s symbols=%s sources=%s",
                    len(payload.get("tickers") or []),
                    len(payload.get("symbols") or []),
                    ",".join(payload.get("sources") or []))
    except Exception:
        pass

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
    Alias endpoint used by some GAS clients: POST /v1/advisor/run
    """
    denied = _auth_guard_or_envelope(x_app_token, authorization, token)
    if denied:
        return denied

    payload = _normalize_payload(body or {})

    try:
        logger.info("[Advisor] /run tickers=%s symbols=%s sources=%s",
                    len(payload.get("tickers") or []),
                    len(payload.get("symbols") or []),
                    ",".join(payload.get("sources") or []))
    except Exception:
        pass

    return await _run_engine(request, payload, debug=debug)


__all__ = ["router"]
