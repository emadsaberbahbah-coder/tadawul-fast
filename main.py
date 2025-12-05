#!/usr/bin/env python3
"""
Tadawul Fast Bridge - Core API v4.1.0

Clean, provider-driven version focused on:
- /v1/status
- /v1/quote
- /debug/eodhd/{symbol}

Key ideas:
- Read ENABLED_PROVIDERS and PRIMARY_PROVIDER from env
- Fully implement EODHD as a provider
- Always record providers_used in /v1/quote
- Support auth via ?token=... or Bearer token
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logger = logging.getLogger("main")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)


# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

class Settings(BaseSettings):
    # Service info
    service_name: str = Field("Tadawul Fast Bridge", alias="SERVICE_NAME")
    service_version: str = Field("4.1.0", alias="SERVICE_VERSION")
    environment: str = Field("production", alias="ENVIRONMENT")

    # Auth
    app_token: Optional[str] = Field(None, alias="APP_TOKEN")
    backup_app_token: Optional[str] = Field(None, alias="BACKUP_APP_TOKEN")
    require_auth: bool = Field(True, alias="REQUIRE_AUTH")

    # Providers
    enabled_providers_raw: str = Field(
        "eodhd",
        alias="ENABLED_PROVIDERS",
        description="Comma-separated list: eodhd,finnhub,alpha_vantage,marketstack,twelvedata,fmp",
    )
    primary_provider: str = Field(
        "eodhd",
        alias="PRIMARY_PROVIDER",
        description="Primary provider key, e.g. eodhd",
    )

    # EODHD
    eodhd_api_key: Optional[str] = Field(None, alias="EODHD_API_KEY")
    eodhd_base_url: str = Field("https://eodhd.com/api", alias="EODHD_BASE_URL")

    # HTTP
    http_timeout: float = Field(10.0, alias="HTTP_TIMEOUT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

# Normalize enabled providers
ENABLED_PROVIDERS: List[str] = [
    p.strip().lower()
    for p in settings.enabled_providers_raw.split(",")
    if p.strip()
]

logger.info("Service Name: %s", settings.service_name)
logger.info("Service Version: %s", settings.service_version)
logger.info("Environment: %s", settings.environment)
logger.info("Enabled providers: %s", ENABLED_PROVIDERS)
logger.info("Primary provider: %s", settings.primary_provider)


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------

class Quote(BaseModel):
    ticker: str
    symbol: str
    exchange: Optional[str] = None
    currency: Optional[str] = None
    name: Optional[str] = None
    sector: Optional[str] = None

    price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None

    volume: Optional[int] = None
    avg_volume: Optional[int] = None

    change: Optional[float] = None
    change_percent: Optional[float] = None

    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None

    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    ev_ebitda: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    debt_equity: Optional[float] = None

    rsi: Optional[float] = None
    macd: Optional[float] = None
    moving_avg_20: Optional[float] = None
    moving_avg_50: Optional[float] = None
    volatility: Optional[float] = None

    status: str = "ok"
    quality: str = "unknown"
    provider: Optional[str] = None
    source: Optional[str] = None

    cached: bool = False
    cache_ttl: Optional[int] = None

    message: Optional[str] = None

    last_updated: Optional[datetime] = None
    timestamp: Optional[datetime] = None

    raw_data: Optional[Dict[str, Any]] = None


class QuoteResponse(BaseModel):
    quotes: List[Quote]
    timestamp: datetime
    request_id: str
    success_count: int
    error_count: int
    cache_hits: int
    cache_misses: int
    execution_time: float
    providers_used: List[str]
    status: str
    message: Optional[str] = None


class StatusResponse(BaseModel):
    status: str
    version: str
    environment: str
    timestamp: datetime
    uptime_seconds: float
    services: Dict[str, Any]


# -----------------------------------------------------------------------------
# Provider Interface + EODHD Implementation
# -----------------------------------------------------------------------------

class ProviderError(Exception):
    pass


class BaseProvider:
    name: str

    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        raise NotImplementedError


class EodhdProvider(BaseProvider):
    """
    EODHD real-time provider using /real-time/{code}?api_token=...
    """

    def __init__(self, api_key: str, base_url: str, client: httpx.AsyncClient):
        self.name = "eodhd"
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.client = client

    def _normalize_symbol(self, ticker: str) -> str:
        """
        EODHD needs an exchange suffix.
        - If ticker already has '.', assume full code (e.g., 2222.SR, MSFT.US)
        - If no '.', assume US stock and append '.US'
        """
        t = ticker.strip().upper()
        if "." in t:
            return t
        # basic heuristic: treat as US
        return f"{t}.US"

    async def get_quote(self, ticker: str) -> Dict[str, Any]:
        if not self.api_key:
            raise ProviderError("EODHD_API_KEY is not configured")

        code = self._normalize_symbol(ticker)
        url = f"{self.base_url}/real-time/{code}"
        params = {
            "api_token": self.api_key,
            "fmt": "json",
        }

        try:
            resp = await self.client.get(url, params=params)
        except Exception as exc:
            raise ProviderError(f"EODHD request error: {exc}") from exc

        if resp.status_code != 200:
            raise ProviderError(f"EODHD HTTP {resp.status_code}: {resp.text}")

        try:
            data = resp.json()
        except Exception as exc:
            raise ProviderError(f"EODHD JSON parse error: {exc}") from exc

        # EODHD real-time sample:
        # {"code":"AAPL.US","timestamp":..., "open":..., "high":..., ...}
        if not data or "code" not in data:
            raise ProviderError(f"EODHD returned invalid payload: {data}")

        return data


# -----------------------------------------------------------------------------
# QuoteService
# -----------------------------------------------------------------------------

class QuoteService:
    def __init__(self, settings: Settings):
        self.settings = settings
        timeout = httpx.Timeout(settings.http_timeout)
        self.http_client = httpx.AsyncClient(timeout=timeout)

        # Build providers registry
        self.providers: Dict[str, BaseProvider] = {}

        if "eodhd" in ENABLED_PROVIDERS:
            if not settings.eodhd_api_key:
                logger.warning("EODHD enabled but EODHD_API_KEY is missing.")
            else:
                self.providers["eodhd"] = EodhdProvider(
                    api_key=settings.eodhd_api_key,
                    base_url=settings.eodhd_base_url,
                    client=self.http_client,
                )
                logger.info("EODHD provider initialized")

        # Stubs for other providers (you can implement later)
        for stub_name in ["alpha_vantage", "finnhub", "marketstack", "twelvedata", "fmp"]:
            if stub_name in ENABLED_PROVIDERS and stub_name not in self.providers:
                logger.warning(
                    "Provider '%s' is listed in ENABLED_PROVIDERS but not implemented yet.",
                    stub_name,
                )

    async def close(self):
        await self.http_client.aclose()

    def _build_provider_order(self) -> List[str]:
        """
        Build provider order with PRIMARY_PROVIDER first, then the rest.
        """
        providers = [p for p in ENABLED_PROVIDERS if p in self.providers]
        primary = self.settings.primary_provider.lower()

        ordered: List[str] = []
        if primary in providers:
            ordered.append(primary)
        for p in providers:
            if p not in ordered:
                ordered.append(p)
        return ordered

    def _infer_currency(self, ticker: str) -> str:
        t = ticker.upper()
        if t.endswith(".SR"):
            return "SAR"
        return "USD"

    async def get_quote_for_symbol(self, ticker: str) -> Tuple[Quote, List[str]]:
        """
        Try providers in order, return Quote and list of providers tried.
        """
        start = time.perf_counter()
        providers_tried: List[str] = []

        base_quote = Quote(
            ticker=ticker,
            symbol=ticker,
            currency=self._infer_currency(ticker),
            status="error",
            message="Failed to fetch quote",
            last_updated=datetime.now(timezone.utc),
            timestamp=datetime.now(timezone.utc),
        )

        order = self._build_provider_order()
        if not order:
            base_quote.message = "No providers configured"
            return base_quote, providers_tried

        for provider_name in order:
            provider = self.providers.get(provider_name)
            providers_tried.append(provider_name)

            if provider is None:
                continue

            try:
                raw = await provider.get_quote(ticker)
            except ProviderError as exc:
                logger.warning(
                    "Provider %s failed for %s: %s", provider_name, ticker, exc
                )
                continue
            except Exception as exc:
                logger.error(
                    "Unexpected error in provider %s for %s: %s",
                    provider_name,
                    ticker,
                    exc,
                )
                continue

            # Map EODHD payload
            if provider_name == "eodhd":
                quote = self._map_eodhd_quote(ticker, raw)
                quote.provider = "eodhd"
                quote.source = "eodhd-realtime"
                quote.status = "ok"
                quote.message = None
                quote.raw_data = raw
                quote.timestamp = datetime.now(timezone.utc)
                return quote, providers_tried

            # Fallback generic mapping if implemented later
            quote = base_quote.copy()
            quote.provider = provider_name
            quote.raw_data = raw
            quote.status = "ok"
            quote.message = None
            quote.timestamp = datetime.now(timezone.utc)
            return quote, providers_tried

        # None succeeded
        duration = time.perf_counter() - start
        logger.warning("All providers failed for %s in %.3fs", ticker, duration)
        base_quote.timestamp = datetime.now(timezone.utc)
        return base_quote, providers_tried

    def _map_eodhd_quote(self, ticker: str, data: Dict[str, Any]) -> Quote:
        """
        Map EODHD real-time JSON to Quote model.
        Example data:
        {
          "code":"AAPL.US",
          "timestamp":1764958860,
          "gmtoffset":0,
          "open":280.54,
          "high":281.14,
          "low":278.05,
          "close":278.97,
          "volume":18388595,
          "previousClose":280.7,
          "change":-1.73,
          "change_p":-0.6163
        }
        """
        code = data.get("code", ticker)
        price = data.get("close")
        prev_close = data.get("previousClose")
        change = data.get("change")
        change_p = data.get("change_p")

        dt = None
        ts = data.get("timestamp")
        if isinstance(ts, (int, float)):
            try:
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                dt = None

        q = Quote(
            ticker=ticker,
            symbol=ticker,
            exchange=None,  # you can improve this later if needed
            currency=self._infer_currency(ticker),
            name=None,
            sector=None,
            price=price,
            previous_close=prev_close,
            open=data.get("open"),
            high=data.get("high"),
            low=data.get("low"),
            volume=data.get("volume"),
            change=change,
            change_percent=change_p,
            status="ok",
            quality="realtime",
            last_updated=dt or datetime.now(timezone.utc),
            timestamp=dt or datetime.now(timezone.utc),
        )
        return q


# Global service
quote_service = QuoteService(settings=settings)

# -----------------------------------------------------------------------------
# Auth helpers
# -----------------------------------------------------------------------------

def verify_token(request: Request, token_param: Optional[str]) -> None:
    """
    Check ?token=... OR Authorization: Bearer ...
    Only enforces if REQUIRE_AUTH is True.
    """
    if not settings.require_auth:
        return

    valid_tokens = {t for t in [settings.app_token, settings.backup_app_token] if t}
    if not valid_tokens:
        logger.warning("require_auth=True but no APP_TOKEN/BACKUP_APP_TOKEN set")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication not configured",
        )

    # 1) Query param
    if token_param and token_param in valid_tokens:
        return

    # 2) Header
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        candidate = auth_header.split(" ", 1)[1].strip()
        if candidate in valid_tokens:
            return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing authentication token",
    )


# -----------------------------------------------------------------------------
# FastAPI app
# -----------------------------------------------------------------------------

app = FastAPI(
    title=settings.service_name,
    version=settings.service_version,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS (relaxed; you can tighten later)
cors_origins_raw = os.getenv("CORS_ORIGINS", "*")
cors_origins = [o.strip() for o in cors_origins_raw.split(",")] if cors_origins_raw else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup():
    logger.info("🚀 Starting %s v%s", settings.service_name, settings.service_version)
    logger.info("🌍 Environment: %s", settings.environment)
    logger.info("🔐 Authentication: %s", "Enabled" if settings.require_auth else "Disabled")
    logger.info("📈 Providers enabled: %s (primary: %s)", ENABLED_PROVIDERS, settings.primary_provider)


@app.on_event("shutdown")
async def on_shutdown():
    await quote_service.close()
    logger.info("🛑 Application shutdown complete.")


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


@app.get("/v1/status", response_model=StatusResponse)
async def status_endpoint():
    uptime_seconds = 0.0  # If you track start time, you can compute real uptime
    now = datetime.now(timezone.utc)
    services = {
        "providers": {
            "enabled": ENABLED_PROVIDERS,
            "primary": settings.primary_provider,
            "eodhd_configured": bool(settings.eodhd_api_key),
        }
    }
    return StatusResponse(
        status="operational",
        version=settings.service_version,
        environment=settings.environment,
        timestamp=now,
        uptime_seconds=uptime_seconds,
        services=services,
    )


@app.get("/v1/quote", response_model=QuoteResponse)
async def v1_quote(
    request: Request,
    tickers: str = Query(..., description="Comma-separated list of tickers, e.g. 2250.SR,AAPL"),
    token: Optional[str] = Query(None, description="App token (alternative to Bearer header)"),
):
    """
    Main quote endpoint used by Google Sheets / Apps Script.

    Example:
      /v1/quote?tickers=2250.SR,AAPL&token=...
    """
    verify_token(request, token)

    symbols = [t.strip() for t in tickers.split(",") if t.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="No tickers provided")

    start = time.perf_counter()
    quotes: List[Quote] = []
    providers_used_set: set[str] = set()
    cache_hits = 0   # not implemented yet
    cache_misses = 0 # not implemented yet

    for sym in symbols:
        quote, tried = await quote_service.get_quote_for_symbol(sym)
        quotes.append(quote)
        for p in tried:
            providers_used_set.add(p)

    success_count = sum(1 for q in quotes if q.status == "ok")
    error_count = len(quotes) - success_count
    duration = time.perf_counter() - start

    overall_status = "ok" if success_count > 0 and error_count == 0 else "partial"
    if success_count == 0:
        overall_status = "error"

    message = f"Retrieved {success_count} of {len(quotes)} quotes"

    response = QuoteResponse(
        quotes=quotes,
        timestamp=datetime.now(timezone.utc),
        request_id=os.urandom(16).hex(),
        success_count=success_count,
        error_count=error_count,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        execution_time=duration,
        providers_used=sorted(list(providers_used_set)),
        status=overall_status,
        message=message,
    )
    logger.info(
        "v1/quote: %s -> %s (success=%d, error=%d, providers=%s, duration=%.3fs)",
        tickers,
        overall_status,
        success_count,
        error_count,
        response.providers_used,
        duration,
    )
    return response


@app.get("/debug/eodhd/{symbol}")
async def debug_eodhd(
    request: Request,
    symbol: str,
    token: Optional[str] = Query(None, description="App token (alternative to Bearer header)"),
):
    """
    Direct EODHD test from inside the Fast Bridge container.

    Example:
      /debug/eodhd/AAPL.US?token=xxxx
      /debug/eodhd/2250.SR?token=xxxx
    """
    verify_token(request, token)

    provider = quote_service.providers.get("eodhd")
    if not provider or not isinstance(provider, EodhdProvider):
        raise HTTPException(
            status_code=500,
            detail="EODHD provider is not configured (check ENABLED_PROVIDERS and EODHD_API_KEY)",
        )

    try:
        raw = await provider.get_quote(symbol)
    except ProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    except Exception as exc:
        logger.exception("Unexpected error in debug_eodhd for %s: %s", symbol, exc)
        raise HTTPException(status_code=500, detail="Unexpected error in debug_eodhd")

    return {
        "symbol": symbol,
        "normalized_symbol": provider._normalize_symbol(symbol),
        "base_url": settings.eodhd_base_url,
        "status": "ok",
        "raw": raw,
    }
