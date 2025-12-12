"""
main.py
===========================================================
Tadawul Fast Bridge - Main Application
Version: 4.5.1 (Boot-safe + Router Lazy-Load)

Key improvement vs prior:
- NEVER hard-crashes on router import errors.
- Starts FAST (passes Render health check), then loads routers in background.
- If routers fail to import, app still stays up for /health, /v1/status, legacy endpoints.

FastAPI backend for:
    • Enriched Quotes       (/v1/enriched*)
    • AI Analysis           (/v1/analysis*)
    • Advanced Analysis     (/v1/advanced*)  [optional]
    • KSA / Argaam Gateway  (/v1/argaam*)
    • Legacy Quotes         (/v1/quote, /v1/legacy/sheet-rows)

Notes
-----
- This file NEVER calls external market providers directly.
- KSA (.SR) routing should remain KSA-safe (no direct EODHD for .SR).
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

# ------------------------------------------------------------
# Configuration import (env.py) with safe, non-crashing fallback
# ------------------------------------------------------------

try:  # pragma: no cover
    import env as _env_mod  # type: ignore
except Exception:
    _env_mod = None  # type: ignore


@dataclass
class _SettingsFallback:
    """Minimal fallback if env.py.settings is not available."""

    app_env: str = os.getenv("APP_ENV", "production")
    default_spreadsheet_id: Optional[str] = os.getenv("DEFAULT_SPREADSHEET_ID", None)
    app_name: str = os.getenv("APP_NAME", "Tadawul Fast Bridge")
    app_version: str = os.getenv("APP_VERSION", "4.5.1")


# Prefer Settings instance from env.py; otherwise fallback instance
settings = getattr(_env_mod, "settings", _SettingsFallback())


def _get_env_attr(name: str, default: str = "") -> str:
    if _env_mod is not None and hasattr(_env_mod, name):
        value = getattr(_env_mod, name)
        if isinstance(value, str):
            return value
        try:
            return str(value)
        except Exception:
            return default
    return os.getenv(name, default)


def _get_bool(name: str, default: bool = False) -> bool:
    if _env_mod is not None and hasattr(_env_mod, name):
        val = getattr(_env_mod, name)
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in {"1", "true", "yes", "on", "y"}

    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


# ------------------------------------------------------------
# Core app identity & tokens
# ------------------------------------------------------------

APP_NAME: str = getattr(settings, "app_name", _get_env_attr("APP_NAME", "tadawul-fast"))
APP_VERSION: str = getattr(
    settings, "app_version", _get_env_attr("APP_VERSION", "4.5.1")
)

APP_TOKEN: str = _get_env_attr("APP_TOKEN", "")
BACKUP_APP_TOKEN: str = _get_env_attr("BACKUP_APP_TOKEN", "")
BACKEND_BASE_URL: str = _get_env_attr("BACKEND_BASE_URL", "")

ENABLE_CORS_ALL_ORIGINS: bool = _get_bool("ENABLE_CORS_ALL_ORIGINS", True)

GOOGLE_SHEETS_CREDENTIALS_RAW: str = getattr(
    _env_mod,
    "GOOGLE_SHEETS_CREDENTIALS_RAW",
    os.getenv("GOOGLE_SHEETS_CREDENTIALS", ""),
)

GOOGLE_APPS_SCRIPT_BACKUP_URL: str = _get_env_attr("GOOGLE_APPS_SCRIPT_BACKUP_URL", "")
ARGAAM_GATEWAY_URL: str = _get_env_attr("ARGAAM_GATEWAY_URL", "")

_HAS_TOKEN_CONFIGURED = bool(APP_TOKEN or BACKUP_APP_TOKEN)
HAS_SECURE_TOKEN: bool = _get_bool("HAS_SECURE_TOKEN", _HAS_TOKEN_CONFIGURED)

try:
    DEFAULT_SPREADSHEET_ID: Optional[str] = getattr(settings, "default_spreadsheet_id", None)
except Exception:
    DEFAULT_SPREADSHEET_ID = None


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

logger = logging.getLogger("tadawul_fast_bridge")
if not logger.handlers:
    level_name = _get_env_attr("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

START_TIME = time.time()


# ------------------------------------------------------------
# Provider meta helper (for /v1/status)
# ------------------------------------------------------------

def _get_providers_meta() -> Dict[str, Any]:
    enabled: Optional[List[str]] = None
    primary: Optional[str] = None

    try:
        if hasattr(settings, "enabled_providers"):
            maybe_list = getattr(settings, "enabled_providers", None)
            if isinstance(maybe_list, (list, tuple)):
                enabled = list(maybe_list) or None

        if enabled is None:
            raw = os.getenv("ENABLED_PROVIDERS")
            if raw:
                enabled = [p.strip() for p in raw.split(",") if p.strip()]

        if hasattr(settings, "primary_or_default_provider"):
            primary = getattr(settings, "primary_or_default_provider", None)
        if not primary and _env_mod is not None and hasattr(_env_mod, "PRIMARY_PROVIDER"):
            primary = getattr(_env_mod, "PRIMARY_PROVIDER")
        if not primary:
            primary = os.getenv("PRIMARY_PROVIDER")
    except Exception:
        enabled = None
        primary = None

    return {"enabled": enabled, "primary": primary}


# ------------------------------------------------------------
# Rate limiting (SlowAPI) – proxy-aware remote address
# ------------------------------------------------------------

def _client_ip(request: Request) -> str:
    """
    Prefer X-Forwarded-For when behind Render proxy/CDN.
    Falls back to starlette request.client.host.
    """
    xff = request.headers.get("x-forwarded-for") or request.headers.get("X-Forwarded-For")
    if xff:
        # first IP is the original client
        ip = xff.split(",")[0].strip()
        if ip:
            return ip
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


limiter = Limiter(key_func=_client_ip, headers_enabled=True)


# ------------------------------------------------------------
# Auth – APP_TOKEN / BACKUP_APP_TOKEN
# ------------------------------------------------------------

auth_scheme = HTTPBearer(auto_error=False)


async def require_app_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(auth_scheme),
) -> Optional[str]:
    """
    Require a valid APP token only if token auth is enabled.

    Accepts:
      - Authorization: Bearer <token>
      - X-APP-TOKEN: <token>
      - ?token=<token>
    """
    if not HAS_SECURE_TOKEN:
        return None

    token: Optional[str] = None

    if credentials and credentials.scheme.lower() == "bearer":
        token = (credentials.credentials or "").strip()

    if not token:
        header_token = request.headers.get("X-APP-TOKEN") or request.headers.get("x-app-token")
        if header_token:
            token = header_token.strip()

    if not token:
        query_token = request.query_params.get("token")
        if query_token:
            token = query_token.strip()

    if token and token in {APP_TOKEN, BACKUP_APP_TOKEN}:
        return token

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API token")


# ------------------------------------------------------------
# FastAPI app
# ------------------------------------------------------------

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description="Tadawul Fast Bridge – Unified KSA + Global data + Google Sheets integration.",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS: keep wide-open for Sheets, but avoid invalid '*' + credentials combination
if ENABLE_CORS_ALL_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://docs.google.com", "https://script.google.com"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.add_middleware(GZipMiddleware, minimum_size=1000)


# ------------------------------------------------------------
# Router lazy-load (prevents deploy timeout due to import errors)
# ------------------------------------------------------------

ROUTER_STATE: Dict[str, Any] = {
    "loading": False,
    "loaded": False,
    "error": None,
    "started_at_utc": None,
    "finished_at_utc": None,
    "routers": {
        "enriched": {"available": False, "error": None},
        "ai_analysis": {"available": False, "error": None},
        "advanced_analysis": {"available": False, "error": None},
        "ksa_argaam": {"available": False, "error": None},
    },
}

_router_lock = asyncio.Lock()


async def _import_module_threaded(module_path: str):
    return await asyncio.to_thread(importlib.import_module, module_path)


async def _load_routers_once() -> None:
    async with _router_lock:
        if ROUTER_STATE["loaded"] or ROUTER_STATE["loading"]:
            return
        ROUTER_STATE["loading"] = True
        ROUTER_STATE["started_at_utc"] = datetime.now(timezone.utc).isoformat()

    try:
        # Small delay so /health can respond immediately after boot
        await asyncio.sleep(1.0)

        # 1) Enriched quotes router
        try:
            enriched_mod = await _import_module_threaded("routes.enriched_quote")
            app.include_router(enriched_mod.router, dependencies=[Depends(require_app_token)])
            ROUTER_STATE["routers"]["enriched"]["available"] = True
        except Exception as e:
            logger.exception("Failed to import routes.enriched_quote")
            ROUTER_STATE["routers"]["enriched"]["error"] = str(e)

        # 2) AI analysis router
        try:
            ai_mod = await _import_module_threaded("routes.ai_analysis")
            app.include_router(ai_mod.router, dependencies=[Depends(require_app_token)])
            ROUTER_STATE["routers"]["ai_analysis"]["available"] = True
        except Exception as e:
            logger.exception("Failed to import routes.ai_analysis")
            ROUTER_STATE["routers"]["ai_analysis"]["error"] = str(e)

        # 3) Advanced analysis router (optional)
        try:
            adv_mod = await _import_module_threaded("routes.advanced_analysis")
            app.include_router(adv_mod.router, dependencies=[Depends(require_app_token)])
            ROUTER_STATE["routers"]["advanced_analysis"]["available"] = True
        except Exception as e:
            # optional; do not treat as fatal
            ROUTER_STATE["routers"]["advanced_analysis"]["error"] = str(e)

        # 4) KSA / Argaam gateway router (optional but expected)
        try:
            argaam_mod = await _import_module_threaded("routes_argaam")
            app.include_router(argaam_mod.router, dependencies=[Depends(require_app_token)])
            ROUTER_STATE["routers"]["ksa_argaam"]["available"] = True
        except Exception as e:
            ROUTER_STATE["routers"]["ksa_argaam"]["error"] = str(e)

        # Mark loaded if at least core routers are available
        core_ok = (
            ROUTER_STATE["routers"]["enriched"]["available"]
            and ROUTER_STATE["routers"]["ai_analysis"]["available"]
        )
        ROUTER_STATE["loaded"] = bool(core_ok)

        if core_ok:
            logger.info("✅ Core routers loaded (enriched + ai_analysis).")
        else:
            logger.warning("⚠️ Routers not fully loaded. Service stays up for debugging (/v1/status).")

    except Exception as e:
        ROUTER_STATE["error"] = str(e)
        logger.exception("Router loader failed unexpectedly")
    finally:
        ROUTER_STATE["loading"] = False
        ROUTER_STATE["finished_at_utc"] = datetime.now(timezone.utc).isoformat()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("🚀 Starting %s (env=%s, version=%s)", APP_NAME, getattr(settings, "app_env", "production"), APP_VERSION)
    # Start router import in background (non-blocking boot)
    asyncio.create_task(_load_routers_once())
    yield
    uptime = time.time() - START_TIME
    logger.info("🛑 Shutting down after %.1f seconds", uptime)


# attach lifespan after defining it
app.router.lifespan_context = lifespan


# ------------------------------------------------------------
# Root / Health / Status
# ------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root() -> str:
    providers_meta = _get_providers_meta()
    providers_html = (
        f"<code>{', '.join(providers_meta['enabled'])}</code>"
        if providers_meta.get("enabled")
        else "<em>not configured</em>"
    )
    primary_provider_html = (
        f"<code>{providers_meta.get('primary')}</code>"
        if providers_meta.get("primary")
        else "<em>auto</em>"
    )

    def _badge(ok: bool) -> str:
        return "✅" if ok else "❌"

    r = ROUTER_STATE["routers"]

    return f"""
    <html>
      <head><title>{APP_NAME}</title></head>
      <body style="font-family:system-ui;background:#0b1020;color:#edf2f7;padding:20px;">
        <h1>{APP_NAME}</h1>
        <p>Environment: <strong>{getattr(settings, 'app_env', 'production')}</strong></p>
        <p>Version: <strong>{APP_VERSION}</strong></p>
        <p>Base URL: <code>{BACKEND_BASE_URL}</code></p>
        <p>Default Spreadsheet: <code>{DEFAULT_SPREADSHEET_ID or "not configured"}</code></p>
        <p>Providers (global): {providers_html} &nbsp;|&nbsp; Primary: {primary_provider_html}</p>

        <h2>Router Status</h2>
        <ul>
          <li>Enriched: {_badge(bool(r['enriched']['available']))}</li>
          <li>AI Analysis: {_badge(bool(r['ai_analysis']['available']))}</li>
          <li>Advanced: {_badge(bool(r['advanced_analysis']['available']))}</li>
          <li>KSA Argaam: {_badge(bool(r['ksa_argaam']['available']))}</li>
        </ul>

        <h2>Quick Links</h2>
        <ul>
          <li><code>/health</code></li>
          <li><code>/v1/status</code></li>
          <li><code>/v1/router-state</code></li>
          <li><code>/v1/quote?tickers=AAPL</code> (legacy)</li>
          <li><code>/v1/quote?tickers=1120.SR</code> (legacy)</li>
        </ul>

        <p style="margin-top:24px;font-size:0.9rem;color:#a0aec0;">
          If routers are still loading or failed, check <code>/v1/status</code> and Render logs.
        </p>
      </body>
    </html>
    """


@app.get("/health")
async def basic_health() -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    uptime_seconds = time.time() - START_TIME
    return {
        "status": "ok",
        "app": APP_NAME,
        "version": APP_VERSION,
        "env": getattr(settings, "app_env", "production"),
        "time_utc": now.isoformat(),
        "uptime_seconds": uptime_seconds,
        "routers_loaded": bool(ROUTER_STATE["loaded"]),
        "routers_loading": bool(ROUTER_STATE["loading"]),
    }


@app.get("/v1/router-state")
async def router_state() -> Dict[str, Any]:
    return ROUTER_STATE


@app.get("/v1/status")
async def status_endpoint(request: Request) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    uptime_seconds = time.time() - START_TIME

    services = {
        "google_sheets": bool(GOOGLE_SHEETS_CREDENTIALS_RAW),
        "google_apps_script": bool(GOOGLE_APPS_SCRIPT_BACKUP_URL),
    }

    auth = {
        "has_secure_token": HAS_SECURE_TOKEN,
        "token_in_query": bool(request.query_params.get("token")),
        "token_in_header": bool(
            request.headers.get("Authorization")
            or request.headers.get("X-APP-TOKEN")
            or request.headers.get("x-app-token")
        ),
    }

    sheets_meta = {
        "default_spreadsheet_id": DEFAULT_SPREADSHEET_ID,
        "has_default_spreadsheet": bool(DEFAULT_SPREADSHEET_ID),
    }

    providers_meta = _get_providers_meta()

    sheet_endpoints = {
        "legacy": "/v1/legacy/sheet-rows",
        "enriched": "/v1/enriched/sheet-rows" if ROUTER_STATE["routers"]["enriched"]["available"] else None,
        "ai_analysis": "/v1/analysis/sheet-rows" if ROUTER_STATE["routers"]["ai_analysis"]["available"] else None,
        "advanced_analysis": "/v1/advanced/sheet-rows" if ROUTER_STATE["routers"]["advanced_analysis"]["available"] else None,
        "ksa_argaam": "/v1/argaam/sheet-rows" if ROUTER_STATE["routers"]["ksa_argaam"]["available"] else None,
    }

    return {
        "status": "operational",
        "version": APP_VERSION,
        "environment": getattr(settings, "app_env", "production"),
        "uptime_seconds": uptime_seconds,
        "timestamp": now.isoformat(),
        "backend_base_url": BACKEND_BASE_URL,
        "services": services,
        "auth": auth,
        "sheets": sheets_meta,
        "providers": providers_meta,
        "router_state": ROUTER_STATE,
        "sheet_endpoints": sheet_endpoints,
        "ksa_argaam_gateway": {
            "configured": bool(ARGAAM_GATEWAY_URL),
            "gateway_url_prefix": (ARGAAM_GATEWAY_URL or "")[:120] or None,
            "router_available": bool(ROUTER_STATE["routers"]["ksa_argaam"]["available"]),
        },
        "notes": [
            "If deploy previously timed out, this build-safe main.py keeps /health alive even if routers fail.",
            "Fix router import errors by checking Render logs + the router_state errors above.",
        ],
    }


# ------------------------------------------------------------
# Legacy endpoints (keep always available; import heavy module lazily)
# ------------------------------------------------------------

class LegacyQuoteSheetRequest(BaseModel):
    tickers: List[str] = Field(default_factory=list, description="Symbols list, e.g. ['1120.SR','AAPL']")


class LegacyQuoteSheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any]


@app.get("/v1/quote")
@limiter.limit("120/minute")
async def legacy_quote_endpoint(
    request: Request,
    tickers: str = Query(..., description="Comma-separated symbols, e.g. AAPL,MSFT,1120.SR"),
    _token: Optional[str] = Depends(require_app_token),
) -> JSONResponse:
    symbols = [t.strip() for t in tickers.split(",") if t.strip()]
    # Lazy import (prevents startup crash if legacy_service has issues)
    from legacy_service import get_legacy_quotes  # type: ignore
    payload = await get_legacy_quotes(symbols)
    return JSONResponse(content=payload)


@app.post("/v1/legacy/sheet-rows", response_model=LegacyQuoteSheetResponse)
@limiter.limit("60/minute")
async def legacy_sheet_rows_endpoint(
    request: Request,
    body: LegacyQuoteSheetRequest,
    _token: Optional[str] = Depends(require_app_token),
) -> LegacyQuoteSheetResponse:
    from legacy_service import build_legacy_sheet_payload  # type: ignore
    payload = await build_legacy_sheet_payload(body.tickers or [])
    return LegacyQuoteSheetResponse(
        headers=payload.get("headers", []),
        rows=payload.get("rows", []),
        meta=payload.get("meta", {}),
    )


# ------------------------------------------------------------
# Error handlers
# ------------------------------------------------------------

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    # If a router endpoint is hit while routers are still loading, return 503 instead of 404
    if exc.status_code == 404:
        path = request.url.path or ""
        if path.startswith(("/v1/enriched", "/v1/analysis", "/v1/advanced", "/v1/argaam")):
            if ROUTER_STATE["loading"]:
                return JSONResponse(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    content={
                        "error": True,
                        "status_code": 503,
                        "detail": "Service warming up (routers loading). Retry in a few seconds.",
                    },
                )
            if not ROUTER_STATE["loaded"]:
                return JSONResponse(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    content={
                        "error": True,
                        "status_code": 503,
                        "detail": "Routers not available (import failed). Check /v1/router-state and logs.",
                        "router_state": ROUTER_STATE,
                    },
                )

    logger.warning("HTTPException (%s) at %s: %s", exc.status_code, request.url.path, exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": True, "status_code": exc.status_code, "detail": exc.detail},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception at %s", request.url.path)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": True, "status_code": 500, "detail": "Internal server error – see backend logs."},
    )


__all__ = ["app"]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )
