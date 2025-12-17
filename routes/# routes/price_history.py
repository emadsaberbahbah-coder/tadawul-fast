# routes/price_history.py
"""
routes/price_history.py
===========================================================
Price History Routes (Snapshot + Backfill) — v1.0.0 (PROD SAFE)

Purpose
- Expose stored history for ROI calculations by horizon (days).
- Provide snapshot endpoints (single + batch) used by Insights/Analysis.
- Provide backfill endpoint (best-effort) to populate local history store.

Endpoints
- GET  /v1/history/health
- GET  /v1/history/series?symbol=1120.SR&limit=400
- GET  /v1/history/snapshot?symbol=1120.SR&horizons=7,30,90,365
- POST /v1/history/snapshot
    body: {tickers:[...], symbols:[...], horizons:[7,30,90], sheet_name?: "..."}
- POST /v1/history/backfill
    body: {tickers:[...], symbols:[...], days?:365, interval?: "1d", mode?: "best_effort"}

Notes
- Uses X-APP-TOKEN auth (same pattern as other routes). If no tokens are configured, endpoints are OPEN.
- Integrates with core.price_history_store (NEW). If the store module is missing, routes return a clear error.

Design goals
- Never crash callers. Always returns JSON with status + errors.
- Works even if engine cannot fetch historic candles: backfill falls back to recording "current quote" only.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from core.config import get_settings
from core.data_engine_v2 import DataEngine, UnifiedQuote, normalize_symbol, is_ksa_symbol  # type: ignore

# Optional history store (NEW)
try:
    from core.price_history_store import get_price_history_store  # type: ignore
except Exception:  # pragma: no cover
    get_price_history_store = None  # type: ignore

logger = logging.getLogger("routes.price_history")

ROUTE_VERSION = "1.0.0"
router = APIRouter(prefix="/v1/history", tags=["history"])


# =============================================================================
# Request models
# =============================================================================
class SnapshotRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    sheet_name: Optional[str] = None
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)  # alias support
    horizons: List[int] = Field(default_factory=list)  # days


class BackfillRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tickers: List[str] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)  # alias support
    days: int = 365
    interval: str = "1d"
    mode: str = "best_effort"  # best_effort | strict


# =============================================================================
# Auth (X-APP-TOKEN) — same style as enriched routes
# =============================================================================
@lru_cache(maxsize=1)
def _allowed_tokens() -> List[str]:
    tokens: List[str] = []
    try:
        s = get_settings()
        for attr in ("app_token", "backup_app_token", "APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = getattr(s, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

    # env.py exports if present
    try:
        import env as env_mod  # type: ignore
        for attr in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
            v = getattr(env_mod, attr, None)
            if isinstance(v, str) and v.strip():
                tokens.append(v.strip())
    except Exception:
        pass

    # env vars last resort
    for k in ("APP_TOKEN", "BACKUP_APP_TOKEN"):
        v = os.getenv(k)
        if v and v.strip():
            tokens.append(v.strip())

    # de-dup preserve order
    out: List[str] = []
    seen = set()
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)

    if not out:
        logger.warning("[history] No APP_TOKEN configured -> endpoints are OPEN (no auth).")
    return out


def _require_token(x_app_token: Optional[str]) -> None:
    allowed = _allowed_tokens()
    if not allowed:
        return  # open mode
    if not x_app_token or x_app_token.strip() not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid or missing X-APP-TOKEN).")


# =============================================================================
# Engine resolution (prefer app.state.engine; else singleton)
# =============================================================================
_ENGINE: Optional[DataEngine] = None
_ENGINE_LOCK = asyncio.Lock()


def _get_app_engine(request: Request) -> Optional[DataEngine]:
    try:
        st = getattr(getattr(request, "app", None), "state", None)
        if not st:
            return None
        for attr in ("engine", "data_engine", "data_engine_v2"):
            eng = getattr(st, attr, None)
            if isinstance(eng, DataEngine):
                return eng
        return None
    except Exception:
        return None


async def _get_singleton_engine() -> Optional[DataEngine]:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    async with _ENGINE_LOCK:
        if _ENGINE is None:
            try:
                _ENGINE = DataEngine()
                logger.info("[history] DataEngine initialized (routes singleton).")
            except Exception as exc:
                logger.exception("[history] Failed to init DataEngine: %s", exc)
                _ENGINE = None
    return _ENGINE


async def _resolve_engine(request: Request) -> Optional[DataEngine]:
    eng = _get_app_engine(request)
    if eng is not None:
        return eng
    return await _get_singleton_engine()


# =============================================================================
# Helpers
# =============================================================================
def _clean_symbols(symbols: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in symbols or []:
        if x is None:
            continue
        s = normalize_symbol(str(x).strip())
        if not s:
            continue
        su = s.upper()
        if su in seen:
            continue
        seen.add(su)
        out.append(su)
    return out


def _as_int_list(csv_or_list: Any, default: Optional[List[int]] = None) -> List[int]:
    if isinstance(csv_or_list, list):
        xs = []
        for v in csv_or_list:
            try:
                n = int(v)
                if n > 0:
                    xs.append(n)
            except Exception:
                continue
        return xs or (default or [])
    if isinstance(csv_or_list, str):
        xs = []
        for part in csv_or_list.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                n = int(part)
                if n > 0:
                    xs.append(n)
            except Exception:
                continue
        return xs or (default or [])
    return default or []


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)
    try:
        s = str(x).strip()
        if not s:
            return None
        # Accept ISO; allow trailing Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


@dataclass
class _Point:
    ts: datetime
    price: float


def _coerce_points(series: Any) -> List[_Point]:
    """
    Accept series in multiple shapes:
      - [{"ts":"...","price":123.4}, ...]
      - [{"time":"...","close":123.4}, ...]
      - [[ "2025-01-01T00:00:00Z", 123.4], ...]
    """
    out: List[_Point] = []
    if not series:
        return out

    if isinstance(series, list):
        for it in series:
            try:
                if isinstance(it, dict):
                    dt = _parse_dt(it.get("ts") or it.get("time") or it.get("date"))
                    px = it.get("price")
                    if px is None:
                        px = it.get("close") or it.get("last") or it.get("value")
                    if dt is None or px is None:
                        continue
                    out.append(_Point(ts=dt, price=float(px)))
                elif isinstance(it, (list, tuple)) and len(it) >= 2:
                    dt = _parse_dt(it[0])
                    if dt is None:
                        continue
                    out.append(_Point(ts=dt, price=float(it[1])))
            except Exception:
                continue

    out.sort(key=lambda p: p.ts)
    return out


def _find_price_at_or_before(points: List[_Point], target_ts: datetime) -> Optional[float]:
    if not points:
        return None
    # binary-ish scan from end (fast for recent targets)
    for p in reversed(points):
        if p.ts <= target_ts:
            return p.price
    return None


def _compute_rois(points: List[_Point], horizons_days: List[int]) -> Dict[str, Any]:
    """
    ROI = (last / past - 1) * 100
    """
    rois: Dict[str, Any] = {}
    if not points:
        for h in horizons_days:
            rois[f"{h}d"] = None
        return rois

    last = points[-1]
    for h in horizons_days:
        key = f"{h}d"
        past_ts = last.ts - timedelta(days=int(h))
        past_px = _find_price_at_or_before(points, past_ts)
        if past_px is None or past_px == 0:
            rois[key] = None
        else:
            rois[key] = round(((last.price / past_px) - 1.0) * 100.0, 4)
    return rois


def _store_required() -> Any:
    if not get_price_history_store:
        raise RuntimeError("price_history_store is not available (core/price_history_store.py missing).")
    return get_price_history_store()


async def _engine_try_backfill_history(engine: DataEngine, symbol: str, days: int, interval: str) -> Optional[List[Dict[str, Any]]]:
    """
    Best-effort: if engine exposes a history/candles method, use it.
    Otherwise return None.
    """
    # method candidates (you can implement any of them in DataEngine later)
    cands = ["get_price_history", "get_history", "get_candles", "get_ohlc"]
    for name in cands:
        fn = getattr(engine, name, None)
        if not fn:
            continue
        try:
            # support various signatures
            try:
                data = await fn(symbol, days=days, interval=interval)  # type: ignore
            except TypeError:
                try:
                    data = await fn(symbol, days=days)  # type: ignore
                except TypeError:
                    data = await fn(symbol)  # type: ignore
            if isinstance(data, list):
                return data  # list of dicts/points
        except Exception:
            continue
    return None


async def _engine_get_quote(engine: DataEngine, sym: str) -> UnifiedQuote:
    if hasattr(engine, "get_quote"):
        return await engine.get_quote(sym)  # type: ignore
    if hasattr(engine, "get_enriched_quote"):
        return await engine.get_enriched_quote(sym)  # type: ignore
    raise RuntimeError("Engine has no get_quote/get_enriched_quote")


# =============================================================================
# Endpoints
# =============================================================================
@router.get("/health", tags=["system"])
async def history_health(
    request: Request,
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)

    eng = await _resolve_engine(request)
    store_ok = bool(get_price_history_store)
    path = os.getenv("PRICE_HISTORY_PATH", "") if store_ok else ""
    ttl = os.getenv("PRICE_HISTORY_TTL_SEC", "") if store_ok else ""

    return {
        "status": "ok",
        "module": "routes.price_history",
        "version": ROUTE_VERSION,
        "engine": "DataEngineV2" if eng else None,
        "history_store": {"available": store_ok, "persist_path": path or None, "ttl_sec": ttl or None},
        "auth": "open" if not _allowed_tokens() else "token",
    }


@router.get("/series")
async def history_series(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g., 1120.SR, AAPL.US)."),
    limit: int = Query(default=400, ge=1, le=5000),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)
    t0 = time.perf_counter()

    sym = normalize_symbol((symbol or "").strip())
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")

    try:
        store = _store_required()
    except Exception as e:
        return {"status": "error", "symbol": sym, "error": str(e), "series": [], "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)}}

    try:
        series = store.get_series(sym, limit=limit)  # type: ignore
        # keep as returned (list of dicts) for flexibility
        if not isinstance(series, list):
            series = []
        return {
            "status": "success",
            "symbol": sym,
            "count": len(series),
            "series": series,
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }
    except Exception as e:
        return {"status": "error", "symbol": sym, "error": str(e), "series": [], "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)}}


@router.get("/snapshot")
async def history_snapshot_single(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g., 1120.SR, AAPL.US)."),
    horizons: str = Query(default="7,30,90,365", description="Comma-separated horizon days."),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)
    t0 = time.perf_counter()

    sym = normalize_symbol((symbol or "").strip())
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")

    hz = _as_int_list(horizons, default=[7, 30, 90, 365])

    try:
        store = _store_required()
    except Exception as e:
        return {
            "status": "error",
            "symbol": sym,
            "horizons": hz,
            "error": str(e),
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }

    try:
        # Prefer store-native snapshot if exists
        if hasattr(store, "snapshot_one"):
            snap = store.snapshot_one(sym, horizons_days=hz)  # type: ignore
            if isinstance(snap, dict):
                snap.setdefault("symbol", sym)
                snap.setdefault("horizons", hz)
                snap.setdefault("status", "success")
                snap["_meta"] = {"elapsed_ms": int((time.perf_counter() - t0) * 1000)}
                return snap

        # Fallback: compute from series
        series = store.get_series(sym, limit=5000)  # type: ignore
        pts = _coerce_points(series)
        rois = _compute_rois(pts, hz)
        last_price = pts[-1].price if pts else None
        as_of = pts[-1].ts.isoformat() if pts else None

        return {
            "status": "success",
            "symbol": sym,
            "horizons": hz,
            "as_of": as_of,
            "last_price": last_price,
            "points": len(pts),
            "roi_percent": rois,
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }

    except Exception as e:
        return {
            "status": "error",
            "symbol": sym,
            "horizons": hz,
            "error": str(e),
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }


@router.post("/snapshot")
async def history_snapshot_batch(
    request: Request,
    req: Any = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    _require_token(x_app_token)
    t0 = time.perf_counter()

    try:
        parsed = SnapshotRequest.model_validate(req)  # type: ignore
    except Exception:
        try:
            parsed = SnapshotRequest.parse_obj(req)  # type: ignore
        except Exception:
            parsed = SnapshotRequest()

    symbols_in = list(getattr(parsed, "tickers", []) or []) + list(getattr(parsed, "symbols", []) or [])
    symbols = _clean_symbols(symbols_in)
    hz = list(getattr(parsed, "horizons", []) or []) or [7, 30, 90, 365]

    if not symbols:
        return {
            "status": "skipped",
            "error": "No tickers provided",
            "count": 0,
            "symbols": [],
            "horizons": hz,
            "rows": [],
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }

    try:
        store = _store_required()
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "count": len(symbols),
            "symbols": symbols,
            "horizons": hz,
            "rows": [],
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }

    rows: List[Dict[str, Any]] = []
    status = "success"
    err_msg: Optional[str] = None

    for sym in symbols:
        try:
            if hasattr(store, "snapshot_one"):
                snap = store.snapshot_one(sym, horizons_days=hz)  # type: ignore
                if isinstance(snap, dict):
                    snap.setdefault("symbol", sym)
                    rows.append(snap)
                    continue

            series = store.get_series(sym, limit=5000)  # type: ignore
            pts = _coerce_points(series)
            rois = _compute_rois(pts, hz)
            last_price = pts[-1].price if pts else None
            as_of = pts[-1].ts.isoformat() if pts else None

            rows.append(
                {
                    "symbol": sym,
                    "as_of": as_of,
                    "last_price": last_price,
                    "points": len(pts),
                    "roi_percent": rois,
                }
            )
        except Exception as e:
            status = "partial"
            err_msg = (err_msg + " | " if err_msg else "") + f"{sym}: {e}"
            rows.append({"symbol": sym, "as_of": None, "last_price": None, "points": 0, "roi_percent": {f"{h}d": None for h in hz}, "error": str(e)})

    return {
        "status": status,
        "error": err_msg,
        "count": len(symbols),
        "symbols": symbols,
        "horizons": hz,
        "rows": rows,
        "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
    }


@router.post("/backfill")
async def history_backfill(
    request: Request,
    req: Any = Body(...),
    x_app_token: Optional[str] = Header(default=None, alias="X-APP-TOKEN"),
) -> Dict[str, Any]:
    """
    Backfill (best-effort):
    - If engine supports history candles, store them.
    - Else, fallback to recording the current quote (single point).
    """
    _require_token(x_app_token)
    t0 = time.perf_counter()

    try:
        parsed = BackfillRequest.model_validate(req)  # type: ignore
    except Exception:
        try:
            parsed = BackfillRequest.parse_obj(req)  # type: ignore
        except Exception:
            parsed = BackfillRequest()

    symbols_in = list(getattr(parsed, "tickers", []) or []) + list(getattr(parsed, "symbols", []) or [])
    symbols = _clean_symbols(symbols_in)
    days = int(getattr(parsed, "days", 365) or 365)
    interval = str(getattr(parsed, "interval", "1d") or "1d").strip()
    mode = str(getattr(parsed, "mode", "best_effort") or "best_effort").strip().lower()

    if not symbols:
        return {
            "status": "skipped",
            "error": "No tickers provided",
            "count": 0,
            "symbols": [],
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }

    try:
        store = _store_required()
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "count": len(symbols),
            "symbols": symbols,
            "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
        }

    eng = await _resolve_engine(request)

    status = "success"
    errors: List[str] = []
    details: List[Dict[str, Any]] = []

    for sym in symbols:
        ok = False
        used = "none"
        try:
            if eng is not None:
                hist = await _engine_try_backfill_history(eng, sym, days=days, interval=interval)
                if isinstance(hist, list) and hist:
                    if hasattr(store, "ingest_history"):
                        ok = bool(store.ingest_history(sym, hist, interval=interval))  # type: ignore
                        used = "engine_history->ingest_history"
                    else:
                        # fallback ingest: record each point if store supports record_point
                        if hasattr(store, "record_point"):
                            cnt = 0
                            for it in hist:
                                try:
                                    if isinstance(it, dict):
                                        dt = _parse_dt(it.get("ts") or it.get("time") or it.get("date"))
                                        px = it.get("price")
                                        if px is None:
                                            px = it.get("close") or it.get("last") or it.get("value")
                                        if dt is None or px is None:
                                            continue
                                        if store.record_point(sym, float(px), dt):  # type: ignore
                                            cnt += 1
                                    elif isinstance(it, (list, tuple)) and len(it) >= 2:
                                        dt = _parse_dt(it[0])
                                        if dt is None:
                                            continue
                                        if store.record_point(sym, float(it[1]), dt):  # type: ignore
                                            cnt += 1
                                except Exception:
                                    continue
                            ok = cnt > 0
                            used = "engine_history->record_point"
                        else:
                            # can't ingest raw history without store support
                            ok = False
                            used = "engine_history(no_ingest_support)"

            if not ok:
                # fallback: record current quote only
                if eng is None:
                    raise RuntimeError("Engine unavailable for fallback quote.")
                uq = await _engine_get_quote(eng, sym)
                uq = uq.finalize()
                if getattr(uq, "current_price", None) is None:
                    raise RuntimeError("Quote returned no current_price.")
                if hasattr(store, "record_quote"):
                    ok = bool(store.record_quote(uq))  # type: ignore
                    used = "record_quote"
                elif hasattr(store, "record_point"):
                    ok = bool(store.record_point(sym, float(uq.current_price), _utcnow()))  # type: ignore
                    used = "record_point"
                else:
                    raise RuntimeError("History store has no record_quote/record_point methods.")

            details.append({"symbol": sym, "ok": bool(ok), "method": used, "days": days, "interval": interval})

        except Exception as e:
            status = "partial" if mode != "strict" else "error"
            errors.append(f"{sym}: {e}")
            details.append({"symbol": sym, "ok": False, "method": used, "error": str(e), "days": days, "interval": interval})
            if mode == "strict":
                break

    if status == "error" and mode != "strict":
        status = "partial"

    return {
        "status": status,
        "error": " | ".join(errors) if errors else None,
        "count": len(symbols),
        "symbols": symbols,
        "days": days,
        "interval": interval,
        "mode": mode,
        "details": details,
        "_meta": {"elapsed_ms": int((time.perf_counter() - t0) * 1000)},
    }


__all__ = ["router"]
