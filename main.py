"""
main.py
===========================================================
Tadawul Fast Bridge - Main Application
Version: 4.0.x (Unified Engine + Google Sheets + KSA-safe)

- FastAPI backend for:
    • Enriched Quotes (v1/enriched)
    • AI Analysis (v1/analysis)
    • Advanced Analysis & Risk (v1/advanced)
    • KSA / Argaam Gateway (v1/argaam)
    • Legacy Quotes (v1/quote, v1/legacy/sheet-rows)

- Integrated with:
    • core.data_engine (multi-provider engine:
        - KSA via Tadawul/Argaam gateway (NO EODHD for .SR)
        - Global via EODHD + FMP)
    • env.py (all config & tokens, Argaam, Sheets, defaults)
    • Google Sheets / Apps Script flows
      (9 pages: KSA_Tadawul, Global_Markets, Mutual_Funds,
       Commodities_FX, My_Portfolio, Insights_Analysis, etc.)

- IMPORTANT:
    • This file NEVER calls EODHD directly.
    • KSA (.SR) routing is handled by the unified engine and
      KSA router using non-EODHD KSA providers.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from env import (
    APP_NAME,
    APP_VERSION,
    APP_TOKEN,
    BACKUP_APP_TOKEN,
    BACKEND_BASE_URL,
    ENABLE_CORS_ALL_ORIGINS,
    HAS_SECURE_TOKEN,
    GOOGLE_SHEETS_CREDENTIALS_RAW,
    GOOGLE_APPS_SCRIPT_BACKUP_URL,
    DEFAULT_SPREADSHEET_ID,
    settings,
)

from routes import enriched_quote, ai_analysis, advanced_analysis, routes_argaam
from legacy_service import get_legacy_quotes, build_legacy_sheet_payload

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

logger = logging.getLogger("tadawul_fast_bridge")
if not logger.handlers:
    # Basic config if not configured by host
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

START_TIME = time.time()

# ------------------------------------------------------------
# Rate limiting (SlowAPI) – light default
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

    If no APP_TOKEN/BACKUP_APP_TOKEN is configured, this is a no-op.
    """
    if not HAS_SECURE_TOKEN:
        # No token configured -> do not enforce
        return None

    token: Optional[str] = None

    # 1) Authorization: Bearer
    if credentials and credentials.scheme.lower() == "bearer":
        token = (credentials.credentials or "").strip()

    # 2) X-APP-TOKEN header
    if not token:
        header_token = request.headers.get("X-APP-TOKEN") or request.headers.get(
            "x-app-token"
        )
        if header_token:
            token = header_token.strip()

    # 3) ?token= query param (for old tests)
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
        settings.app_env,
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
        "Tadawul Fast Bridge – Unified KSA + Global data engine with Google Sheets "
        "integration (9-page investment dashboard)."
    ),
    lifespan=lifespan,
)

# Attach limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Middleware: CORS
if ENABLE_CORS_ALL_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    # Restrict to Google Docs/Script if you want stricter CORS in future
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://docs.google.com", "https://script.google.com"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Middleware: GZip compression
app.add_middleware(GZipMiddleware, minimum_size=1000)


# ------------------------------------------------------------
# Include routers (protected by token if configured)
# ------------------------------------------------------------

# Unified Enriched Quotes
app.include_router(
    enriched_quote.router,
    dependencies=[Depends(require_app_token)],
)

# AI-based analysis
app.include_router(
    ai_analysis.router,
    dependencies=[Depends(require_app_token)],
)

# Advanced analysis / risk engine
app.include_router(
    advanced_analysis.router,
    dependencies=[Depends(require_app_token)],
)

# KSA / Argaam gateway routes (v1/argaam/*) – .SR only, NO EODHD
app.include_router(
    routes_argaam.router,
    dependencies=[Depends(require_app_token)],
)


# ------------------------------------------------------------
# Root / Health / Status
# ------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def root() -> str:
    """
    Simple HTML landing page for quick checks.
    """
    default_sheet_html = (
        f"<code>{DEFAULT_SPREADSHEET_ID}</code>"
        if DEFAULT_SPREADSHEET_ID
        else "<em>not configured</em>"
    )

    return f"""
    <html>
      <head><title>{APP_NAME}</title></head>
      <body style="font-family:system-ui;background:#0b1020;color:#edf2f7;padding:20px;">
        <h1>{APP_NAME}</h1>
        <p>Environment: <strong>{settings.app_env}</strong></p>
        <p>Version: <strong>{APP_VERSION}</strong></p>
        <p>Base URL: <code>{BACKEND_BASE_URL}</code></p>
        <p>Default Spreadsheet (9-page dashboard): {default_sheet_html}</p>
        <h2>Quick Links</h2>
        <ul>
          <li><code>/health</code></li>
          <li><code>/v1/status</code></li>
          <li><code>/v1/quote?tickers=AAPL</code> (legacy)</li>
          <li><code>/v1/enriched/health</code></li>
          <li><code>/v1/analysis/health</code></li>
          <li><code>/v1/advanced/health</code></li>
          <li><code>/v1/argaam/health</code> (KSA / Argaam gateway)</li>
        </ul>
        <p style="margin-top:24px;font-size:0.9rem;color:#a0aec0;">
          KSA (.SR) tickers are handled via Tadawul/Argaam gateway only.
          Global (non-.SR) tickers are handled via EODHD + FMP.
        </p>
      </body>
    </html>
    """


@app.get("/health")
async def basic_health() -> Dict[str, Any]:
    """
    Very lightweight health endpoint (no external calls).
    """
    now = datetime.now(timezone.utc)
    uptime_seconds = time.time() - START_TIME
    return {
        "status": "ok",
        "app": APP_NAME,
        "version": APP_VERSION,
        "env": settings.app_env,
        "time_utc": now.isoformat(),
        "uptime_seconds": uptime_seconds,
    }


@app.get("/v1/status")
async def status_endpoint(request: Request) -> Dict[str, Any]:
    """
    Detailed status used by your PowerShell / integration tests.

    Example:
        GET /v1/status
        GET /v1/status?token=...
    """
    now = datetime.now(timezone.utc)
    uptime_seconds = time.time() - START_TIME

    # Simple service flags (no heavy external calls here)
    services = {
        "google_sheets": bool(GOOGLE_SHEETS_CREDENTIALS_RAW),
        "google_apps_script": bool(GOOGLE_APPS_SCRIPT_BACKUP_URL),
        "cache": False,  # placeholder for future cache
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

    return {
        "status": "operational",
        "version": APP_VERSION,
        "environment": settings.app_env,
        "uptime_seconds": uptime_seconds,
        "timestamp": now.isoformat(),
        "backend_base_url": BACKEND_BASE_URL,
        "services": services,
        "auth": auth,
        "sheets": sheets_meta,
        "notes": [
            "KSA (.SR) tickers are handled by the unified data engine using Tadawul/Argaam providers.",
            "No direct EODHD calls are made for KSA inside this main application.",
            "Global (non-.SR) tickers use EODHD + FMP via core.data_engine.",
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
    Legacy quote endpoint (used by your early PowerShell scripts).

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

    Can be used by:
        - Google Apps Script (UrlFetchApp)
        - google_sheets_service (if you want the legacy layout)
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
