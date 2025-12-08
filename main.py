"""
main.py
===========================================================
Tadawul Fast Bridge - Main Application
Version: 4.2.x (Unified Engine + Google Sheets + KSA-safe)

FastAPI backend for:
    • Enriched Quotes       (/v1/enriched)
    • AI Analysis           (/v1/analysis)
    • Advanced Analysis     (/v1/advanced)  [optional – only if router imports]
    • KSA / Argaam Gateway  (/v1/argaam)
    • Legacy Quotes         (/v1/quote, /v1/legacy/sheet-rows)

Integrated with:
    • core.data_engine / core.data_engine_v2 (multi-provider engine):
        - KSA via Tadawul/Argaam gateway and v1 delegate (NO EODHD for .SR)
        - Global via FMP + optional EODHD + optional Finnhub
    • env.py (all config & tokens, providers, Sheets meta, etc.)
    • Google Sheets / Apps Script flows
      (9 pages: KSA_Tadawul, Global_Markets, Mutual_Funds,
       Commodities_FX, My_Portfolio_Investment, Insights_Analysis,
       Investment_Advisor, Economic_Calendar, Investment_Income_Statement)

Notes
-----
- This file NEVER calls external market providers directly.
  All market data flows through the unified data engine and/or dedicated KSA gateway.
- KSA (.SR) routing is handled by the unified engine and KSA router,
  using non-EODHD KSA providers only.
"""

from __future__ import annotations

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
from slowapi.util import get_remote_address

# ------------------------------------------------------------
# Configuration import (env.py) with safe, non-crashing fallback
# ------------------------------------------------------------

try:
    import env as _env_mod  # type: ignore
except Exception:  # pragma: no cover - defensive fallback
    _env_mod = None  # type: ignore


@dataclass
class _SettingsFallback:
    """
    Minimal fallback if env.py.settings is not available.
    Keeps the app bootable for local / emergency scenarios.
    """

    app_env: str = os.getenv("APP_ENV", "production")
    default_spreadsheet_id: Optional[str] = os.getenv("DEFAULT_SPREADSHEET_ID", None)
    app_name: str = os.getenv("APP_NAME", "Tadawul Fast Bridge")
    app_version: str = os.getenv("APP_VERSION", "4.2.0")


# Prefer Settings instance from env.py; otherwise use fallback dataclass instance
settings = getattr(_env_mod, "settings", _SettingsFallback())


def _get_env_attr(name: str, default: str = "") -> str:
    """
    Helper to read config from env.py if available, otherwise from OS env vars.
    Returns a string representation.
    """
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
    """
    Helper to read a boolean from env.py or environment variables.
    Accepts bool or typical truthy strings.
    """
    # Prefer env.py attribute if present
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


# Core app identity & tokens
APP_NAME: str = getattr(
    settings, "app_name", _get_env_attr("APP_NAME", "tadawul-fast-bridge")
)
APP_VERSION: str = getattr(
    settings, "app_version", _get_env_attr("APP_VERSION", "4.2.0")
)
APP_TOKEN: str = _get_env_attr("APP_TOKEN", "")
BACKUP_APP_TOKEN: str = _get_env_attr("BACKUP_APP_TOKEN", "")
BACKEND_BASE_URL: str = _get_env_attr("BACKEND_BASE_URL", "")

ENABLE_CORS_ALL_ORIGINS: bool = _get_bool("ENABLE_CORS_ALL_ORIGINS", True)

# Google integration flags for /v1/status
GOOGLE_SHEETS_CREDENTIALS_RAW: str = getattr(
    _env_mod,
    "GOOGLE_SHEETS_CREDENTIALS_RAW",
    os.getenv("GOOGLE_SHEETS_CREDENTIALS", ""),
)

GOOGLE_APPS_SCRIPT_BACKUP_URL: str = _get_env_attr(
    "GOOGLE_APPS_SCRIPT_BACKUP_URL", ""
)

# KSA / Argaam gateway visibility in /v1/status
ARGAAM_GATEWAY_URL: str = _get_env_attr("ARGAAM_GATEWAY_URL", "")

# HAS_SECURE_TOKEN:
# - If env.py / env vars define HAS_SECURE_TOKEN, respect that (parsed via _get_bool).
# - Otherwise derive from presence of APP_TOKEN / BACKUP_APP_TOKEN.
_HAS_TOKEN_CONFIGURED = bool(APP_TOKEN or BACKUP_APP_TOKEN)
HAS_SECURE_TOKEN: bool = _get_bool("HAS_SECURE_TOKEN", _HAS_TOKEN_CONFIGURED)


# Optional provider information (for /v1/status)
def _get_providers_meta() -> Dict[str, Any]:
    """
    Summarize provider configuration for diagnostics.
    Prefer env.settings if available, otherwise raw env vars.
    """
    enabled: Optional[List[str]] = None
    primary: Optional[str] = None

    try:
        # Try settings.enabled_providers (if defined in env.py)
        if hasattr(settings, "enabled_providers"):
            maybe_list = getattr(settings, "enabled_providers", None)
            if isinstance(maybe_list, (list, tuple)):
                enabled = list(maybe_list) or None

        # Fallback: ENABLED_PROVIDERS env var
        if enabled is None:
            raw = os.getenv("ENABLED_PROVIDERS")
            if raw:
                enabled = [p.strip() for p in raw.split(",") if p.strip()]

        # Primary provider: settings or env
        if hasattr(settings, "primary_or_default_provider"):
            primary = getattr(settings, "primary_or_default_provider", None)
        if not primary and _env_mod is not None and hasattr(_env_mod, "PRIMARY_PROVIDER"):
            primary = getattr(_env_mod, "PRIMARY_PROVIDER")
        if not primary:
            primary = os.getenv("PRIMARY_PROVIDER")
    except Exception:  # pragma: no cover - extremely defensive
        enabled = None
        primary = None

    return {
        "enabled": enabled,
        "primary": primary,
    }


# ------------------------------------------------------------
# Routers & legacy services
# ------------------------------------------------------------

# Core routers:
# - routes/enriched_quote.py
# - routes/ai_analysis.py
from routes import enriched_quote, ai_analysis  # type: ignore

# advanced_analysis is optional – if not present, we only disable /v1/advanced*
try:  # pragma: no cover - optional router import
    from routes import advanced_analysis  # type: ignore

    _ADVANCED_ANALYSIS_AVAILABLE = True
except Exception as _advanced_exc:  # pragma: no cover - defensive
    logging.error(
        "advanced_analysis router could not be imported: %s. "
        "The /v1/advanced* endpoints will be disabled for this deploy.",
        _advanced_exc,
    )
    advanced_analysis = None  # type: ignore
    _ADVANCED_ANALYSIS_AVAILABLE = False

# KSA / Argaam gateway routes (v1/argaam/*) – .SR only, NO EODHD
try:  # pragma: no cover - optional, but expected in your project
    import routes_argaam  # type: ignore

    _ARGAAM_AVAILABLE = True
except Exception as _argaam_exc:  # pragma: no cover - defensive
    logging.error(
        "routes_argaam router could not be imported: %s. "
        "The /v1/argaam* endpoints will be disabled for this deploy.",
        _argaam_exc,
    )
    routes_argaam = None  # type: ignore
    _ARGAAM_AVAILABLE = False

# Legacy service abstraction (global + KSA via unified engine)
from legacy_service import build_legacy_sheet_payload, get_legacy_quotes

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

logger = logging.getLogger("tadawul_fast_bridge")
if not logger.handlers:
    # If env.LOG_LEVEL is defined, prefer it; otherwise INFO
    log_level_name = _get_env_attr("LOG_LEVEL", "INFO").upper()
    try:
        log_level = getattr(logging, log_level_name, logging.INFO)
    except Exception:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

START_TIME = time.time()

# ------------------------------------------------------------
# Derived / optional values from env.settings
# ------------------------------------------------------------

try:
    DEFAULT_SPREADSHEET_ID: Optional[str] = getattr(
        settings, "default_spreadsheet_id", None
    )
except Exception:  # very defensive
    DEFAULT_SPREADSHEET_ID = None

# ------------------------------------------------------------
# Rate limiting (SlowAPI) – light defaults
# ------------------------------------------------------------

limiter = Limiter(key_func=get_remote_address, headers_enabled=True)

# ------------------------------------------------------------
# Auth – APP_TOKEN / BACKUP_APP_TOKEN
# ------------------------------------------------------------

auth_scheme = HTTPBearer(auto_error=False)


async def require_app_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(auth_scheme),
) -> Optional[str]:
    """
    Require a valid APP token *only if* a token is configured.

    Accepts:
      - Authorization: Bearer <token>
      - X-APP-TOKEN: <token>
      - ?token=<token>

    If no APP_TOKEN/BACKUP_APP_TOKEN is configured, this is a no-op (dev mode).
    """
    if not HAS_SECURE_TOKEN:
        # No token configured -> no enforcement (development mode / local tests)
        return None

    token: Optional[str] = None

    # 1) Authorization: Bearer <token>
    if credentials and credentials.scheme.lower() == "bearer":
        token = (credentials.credentials or "").strip()

    # 2) X-APP-TOKEN header (for Google Apps Script / PowerShell, etc.)
    if not token:
        header_token = request.headers.get("X-APP-TOKEN") or request.headers.get(
            "x-app-token"
        )
        if header_token:
            token = header_token.strip()

    # 3) ?token=<token> query param (legacy tests)
    if not token:
        query_token = request.query_params.get("token")
        if query_token:
            token = query_token.strip()

    if token and token in {APP_TOKEN, BACKUP_APP_TOKEN}:
        return token

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API token",
    )


# ------------------------------------------------------------
# Lifespan (startup / shutdown)
# ------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "🚀 Starting %s (env=%s, version=%s)",
        APP_NAME,
        getattr(settings, "app_env", "production"),
        APP_VERSION,
    )
    yield
    uptime = time.time() - START_TIME
    logger.info("🛑 Shutting down after %.1f seconds", uptime)


# ------------------------------------------------------------
# FastAPI app
# ------------------------------------------------------------

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description=(
        "Tadawul Fast Bridge – Unified KSA + Global data engine "
        "with 9-page Google Sheets investment dashboard integration."
    ),
    lifespan=lifespan,
)

# Attach rate limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Middleware: CORS
if ENABLE_CORS_ALL_ORIGINS:
    # Open CORS – convenient for Google Sheets, Apps Script, and local tools
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    # Stricter mode – tuned for Google Sheets / Apps Script
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://docs.google.com",
            "https://script.google.com",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Middleware: GZip compression
app.add_middleware(GZipMiddleware, minimum_size=1000)

# ------------------------------------------------------------
# Include routers (protected by token if configured)
# ------------------------------------------------------------

# Unified Enriched Quotes (for both KSA + Global, using engine routing)
app.include_router(
    enriched_quote.router,
    dependencies=[Depends(require_app_token)],
)

# AI-based analysis (scores + recommendation)
app.include_router(
    ai_analysis.router,
    dependencies=[Depends(require_app_token)],
)

# Advanced analysis / risk engine (extra KPIs, risk buckets, etc.) – optional
if _ADVANCED_ANALYSIS_AVAILABLE and advanced_analysis is not None:
    app.include_router(
        advanced_analysis.router,  # type: ignore[attr-defined]
        dependencies=[Depends(require_app_token)],
    )
else:
    logger.warning(
        "Advanced analysis router is not available – "
        "skipping /v1/advanced* endpoints for this deploy."
    )

# KSA / Argaam gateway routes (v1/argaam/*) – .SR only, NO EODHD from here
if _ARGAAM_AVAILABLE and routes_argaam is not None:
    app.include_router(
        routes_argaam.router,
        dependencies=[Depends(require_app_token)],
    )
else:
    logger.warning(
        "KSA / Argaam router is not available – "
        "skipping /v1/argaam* endpoints for this deploy."
    )

# ------------------------------------------------------------
# Root / Health / Status
# ------------------------------------------------------------


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root() -> str:
    """
    Simple HTML landing page for quick checks.
    Helpful when testing from a browser or Render "Open app" button.
    """
    default_sheet_html = (
        f"<code>{DEFAULT_SPREADSHEET_ID}</code>"
        if DEFAULT_SPREADSHEET_ID
        else "<em>not configured</em>"
    )
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

    return f"""
    <html>
      <head><title>{APP_NAME}</title></head>
      <body style="font-family:system-ui;background:#0b1020;color:#edf2f7;padding:20px;">
        <h1>{APP_NAME}</h1>
        <p>Environment: <strong>{getattr(settings, 'app_env', 'production')}</strong></p>
        <p>Version: <strong>{APP_VERSION}</strong></p>
        <p>Base URL: <code>{BACKEND_BASE_URL}</code></p>
        <p>Default Spreadsheet (9-page dashboard): {default_sheet_html}</p>
        <p>Providers (global): {providers_html} &nbsp;|&nbsp; Primary: {primary_provider_html}</p>

        <h2>Quick Links</h2>
        <ul>
          <li><code>/health</code></li>
          <li><code>/v1/status</code></li>
          <li><code>/v1/quote?tickers=AAPL</code> (legacy)</li>
          <li><code>/v1/enriched/health</code></li>
          <li><code>/v1/analysis/health</code></li>
          <li><code>/v1/advanced/ping</code> (if enabled)</li>
          <li><code>/v1/argaam/health</code> (KSA / Argaam gateway)</li>
        </ul>

        <h3>Auth usage (for Sheets / scripts)</h3>
        <p style="font-size:0.9rem;color:#a0aec0;">
          Send your app token as either:<br/>
          • <code>Authorization: Bearer &lt;APP_TOKEN&gt;</code><br/>
          • <code>X-APP-TOKEN: &lt;APP_TOKEN&gt;</code><br/>
          • or <code>?token=&lt;APP_TOKEN&gt;</code> on legacy endpoints.<br/>
        </p>

        <p style="margin-top:24px;font-size:0.9rem;color:#a0aec0;">
          KSA (.SR) tickers are handled via Tadawul/Argaam gateway only.<br/>
          Global (non-.SR) tickers are handled via the unified engine using the
          configured global providers (FMP, EODHD, Finnhub, etc.).<br/>
          Google Sheets 9-page dashboard should use the <code>* /sheet-rows</code> endpoints
          exposed by each router (see <code>/v1/status</code> for a complete list).
        </p>
      </body>
    </html>
    """


@app.get("/health")
async def basic_health() -> Dict[str, Any]:
    """
    Very lightweight health endpoint (no external calls).
    Suitable for uptime checks and Render health probes.
    """
    now = datetime.now(timezone.utc)
    uptime_seconds = time.time() - START_TIME
    return {
        "status": "ok",
        "app": APP_NAME,
        "version": APP_VERSION,
        "env": getattr(settings, "app_env", "production"),
        "time_utc": now.isoformat(),
        "uptime_seconds": uptime_seconds,
    }


@app.get("/v1/status")
async def status_endpoint(request: Request) -> Dict[str, Any]:
    """
    Detailed status used by PowerShell / integration tests and Sheets hooks.

    Example:
        GET /v1/status
        GET /v1/status?token=...
    """
    now = datetime.now(timezone.utc)
    uptime_seconds = time.time() - START_TIME

    # Simple service flags (no heavy external calls here)
    services = {
        "google_sheets": bool(GOOGLE_SHEETS_CREDENTIALS_RAW)
        or (
            _env_mod is not None
            and hasattr(_env_mod, "GOOGLE_SHEETS_CREDENTIALS")
        ),
        "google_apps_script": bool(GOOGLE_APPS_SCRIPT_BACKUP_URL),
        "cache": False,  # placeholder for future cache (Redis/Memory/etc.)
    }

    # Whether tokens are configured + how the request came in
    auth = {
        "has_secure_token": HAS_SECURE_TOKEN,
        "token_in_query": bool(request.query_params.get("token")),
        "token_in_header": bool(
            request.headers.get("Authorization")
            or request.headers.get("X-APP-TOKEN")
            or request.headers.get("x-app-token")
        ),
    }

    # Sheets-related info for the 9-page dashboard
    sheets_meta = {
        "default_spreadsheet_id": DEFAULT_SPREADSHEET_ID,
        "has_default_spreadsheet": bool(DEFAULT_SPREADSHEET_ID),
    }

    # For Apps Script / Google Sheets integration – single source of truth
    sheet_endpoints = {
        "enriched": "/v1/enriched/sheet-rows",
        "ai_analysis": "/v1/analysis/sheet-rows",
        "advanced_analysis": (
            "/v1/advanced/sheet-rows" if _ADVANCED_ANALYSIS_AVAILABLE else None
        ),
        "legacy": "/v1/legacy/sheet-rows",
        "ksa_argaam": "/v1/argaam/sheet-rows" if _ARGAAM_AVAILABLE else None,
    }

    providers_meta = _get_providers_meta()

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
        "sheet_endpoints": sheet_endpoints,
        "providers": providers_meta,
        "advanced_analysis_enabled": _ADVANCED_ANALYSIS_AVAILABLE,
        "ksa_argaam_gateway": {
            "configured": bool(ARGAAM_GATEWAY_URL),
            "gateway_url_prefix": (ARGAAM_GATEWAY_URL or "")[:80] or None,
            "router_available": _ARGAAM_AVAILABLE,
        },
        "notes": [
            "KSA (.SR) tickers are handled by the unified data engine using Tadawul/Argaam providers.",
            "No direct EODHD calls are made for KSA inside this main application.",
            "Global (non-.SR) tickers use FMP + optional EODHD + optional Finnhub via core.data_engine / core.data_engine_v2.",
            "Google Sheets 9-page dashboard should use the /sheet-rows endpoints listed in 'sheet_endpoints'.",
        ],
    }


# ------------------------------------------------------------
# Legacy endpoints (v1/quote + v1/legacy/sheet-rows)
# ------------------------------------------------------------


class LegacyQuoteSheetRequest(BaseModel):
    tickers: List[str] = Field(
        default_factory=list,
        description="List of symbols, e.g. ['1120.SR','1180.SR','AAPL']",
    )


class LegacyQuoteSheetResponse(BaseModel):
    headers: List[str]
    rows: List[List[Any]]
    meta: Dict[str, Any]


@app.get("/v1/quote")
@limiter.limit("120/minute")
async def legacy_quote_endpoint(
    request: Request,
    tickers: str = Query(
        ...,
        description="Comma-separated symbols, e.g. AAPL,MSFT,1120.SR",
    ),
    _token: Optional[str] = Depends(require_app_token),
) -> JSONResponse:
    """
    Legacy quote endpoint (used by early PowerShell tests and ad-hoc checks).

    Example:
        GET /v1/quote?tickers=AAPL
        GET /v1/quote?tickers=AAPL,MSFT,1120.SR
    """
    symbols = [t.strip() for t in tickers.split(",") if t.strip()]
    payload = await get_legacy_quotes(symbols)
    return JSONResponse(content=payload)


@app.post("/v1/legacy/sheet-rows", response_model=LegacyQuoteSheetResponse)
@limiter.limit("60/minute")
async def legacy_sheet_rows_endpoint(
    request: Request,
    body: LegacyQuoteSheetRequest,
    _token: Optional[str] = Depends(require_app_token),
) -> LegacyQuoteSheetResponse:
    """
    Google Sheets–friendly legacy data:

        {
          "headers": [...],
          "rows": [[...], ...],
          "meta": {...}
        }

    Used by:
        - Google Apps Script (UrlFetchApp)
        - google_sheets_service (legacy layout for any page if needed)
    """
    payload = await build_legacy_sheet_payload(body.tickers or [])
    return LegacyQuoteSheetResponse(
        headers=payload.get("headers", []),
        rows=payload.get("rows", []),
        meta=payload.get("meta", {}),
    )


# ------------------------------------------------------------
# Global error handlers
# ------------------------------------------------------------


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """
    Standardize HTTPException responses.
    """
    logger.warning(
        "HTTPException (%s) at %s: %s",
        exc.status_code,
        request.url.path,
        exc.detail,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "status_code": exc.status_code,
            "detail": exc.detail,
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """
    Catch-all for uncaught exceptions – ensures we do not leak stack traces
    in production while still logging them server-side.
    """
    logger.exception("Unhandled exception at %s", request.url.path)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": True,
            "status_code": 500,
            "detail": "Internal server error – see backend logs.",
        },
    )
