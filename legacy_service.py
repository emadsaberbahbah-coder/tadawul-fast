#!/usr/bin/env python3

# =============================================================================
# Tadawul Fast Bridge - Legacy Stock Microservice (Enhanced Edition)
# Version: 4.1.1
#
# - Cleaned for Render deployment
# - No broken docstrings
# - No local sheet IDs (secure from env only)
# - Full Google Sheets support
# - TTL cache, health endpoints, CSV export
# =============================================================================

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import gspread
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from google.oauth2.service_account import Credentials
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address


# =============================================================================
# Settings
# =============================================================================

class Settings:
    """Application configuration loaded from environment variables."""

    def __init__(self) -> None:
        self.APP_NAME = os.getenv("SERVICE_NAME", "Tadawul Fast Bridge - Legacy")
        self.VERSION = os.getenv("SERVICE_VERSION", "4.1.1")
        self.ENVIRONMENT = os.getenv("ENVIRONMENT", "production")

        # API sources
        self.ARGAAM_API_KEY = os.getenv("ARGAAM_API_KEY", "")
        self.ARGAAM_BASE_URL = os.getenv("ARGAAM_BASE_URL", "https://api.argaam.com")
        self.TADAWUL_BASE_URL = os.getenv("TADAWUL_BASE_URL", "https://www.tadawul.com.sa")

        # Auth token
        self.APP_TOKEN = os.getenv("APP_TOKEN", "")

        # Google Sheets
        self.GOOGLE_CREDENTIALS_JSON = (
            os.getenv("GOOGLE_SHEETS_CREDENTIALS")
            or os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
            or os.getenv("GOOGLE_CREDENTIALS", "")
        )

        # Cache configuration
        self.CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))
        self.CACHE_MAXSIZE = int(os.getenv("CACHE_MAXSIZE", "1000"))

        # Logging
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


settings = Settings()


# =============================================================================
# Logging Setup
# =============================================================================

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("tadawul-legacy")


# =============================================================================
# Models
# =============================================================================

class StockSymbol(BaseModel):
    symbol: str
    name_ar: Optional[str] = None
    name_en: Optional[str] = None
    sector: Optional[str] = None
    market_cap: Optional[float] = None
    last_price: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None
    timestamp: Optional[str] = None


class MarketData(BaseModel):
    timestamp: str
    market_index: Optional[float] = None
    market_change: Optional[float] = None
    market_change_percent: Optional[float] = None
    turnover: Optional[float] = None
    volume: Optional[int] = None
    advancers: Optional[int] = None
    decliners: Optional[int] = None
    unchanged: Optional[int] = None


# =============================================================================
# Cache
# =============================================================================

class DataCache:
    """Memory TTL cache with stats."""

    def __init__(self, maxsize: int, ttl: int):
        self._store: Dict[str, Tuple[float, Any]] = {}
        self.maxsize = maxsize
        self.ttl = ttl
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        item = self._store.get(key)

        if not item:
            self.misses += 1
            return None

        expires, value = item
        if expires < now:
            self.misses += 1
            self._store.pop(key, None)
            return None

        self.hits += 1
        return value

    def set(self, key: str, value: Any):
        if len(self._store) >= self.maxsize:
            self._store.pop(next(iter(self._store)), None)
        self._store[key] = (time.time() + self.ttl, value)

    def clear(self):
        self._store.clear()
        self.hits = self.misses = 0

    def stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total else 0
        return {
            "size": len(self._store),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round(hit_rate, 1),
        }


cache = DataCache(settings.CACHE_MAXSIZE, settings.CACHE_TTL)


# =============================================================================
# Google Sheets Service
# =============================================================================

class GoogleSheetsService:
    def __init__(self):
        json_str = settings.GOOGLE_CREDENTIALS_JSON
        if not json_str:
            logger.warning("Google Sheets credentials not found.")
            self.client = None
            return

        try:
            creds = Credentials.from_service_account_info(json.loads(json_str))
            self.client = gspread.authorize(creds)
        except Exception as e:
            logger.error(f"Failed to init Google Sheets: {e}")
            self.client = None

    async def update_sheet(self, spreadsheet_id: str, sheet_name: str, data):
        if not self.client:
            raise HTTPException(503, "Google Sheets disabled")

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_update, spreadsheet_id, sheet_name, data)

    def _sync_update(self, spreadsheet_id, sheet_name, data):
        ss = self.client.open_by_key(spreadsheet_id)
        try:
            ws = ss.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(sheet_name, rows=len(data) + 10, cols=len(data[0]) + 5)

        ws.clear()
        ws.update(data)
        return {"rows": len(data)}


# =============================================================================
# HTTP Client
# =============================================================================

class HTTPClient:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def fetch_json(self, url, params=None):
        try:
            async with self.session.get(url, params=params) as r:
                if r.status != 200:
                    raise HTTPException(r.status, f"HTTP {r.status}")
                return await r.json()
        except Exception as e:
            raise HTTPException(503, f"Request failed: {str(e)}")


# =============================================================================
# Stock Data Service
# =============================================================================

class StockDataService:
    def __init__(self):
        self.http = HTTPClient()
        self.sheets = GoogleSheetsService()

    async def get_symbol(self, symbol: str, cache_enabled=True):
        key = f"symbol:{symbol}"

        if cache_enabled:
            cached = cache.get(key)
            if cached:
                return cached

        try:
            if settings.ARGAAM_API_KEY:
                async with self.http as client:
                    raw = await client.fetch_json(
                        f"{settings.ARGAAM_BASE_URL}/v1.0/symbols/{symbol}",
                        params={"apikey": settings.ARGAAM_API_KEY},
                    )
            else:
                raw = self._mock(symbol)

        except:
            raw = self._mock(symbol)

        normalized = self._normalize(raw, symbol)
        cache.set(key, normalized)
        return normalized

    def _mock(self, symbol):
        return {
            "arabicName": f"شركة {symbol}",
            "englishName": f"Company {symbol}",
            "sector": random.choice(["Banks", "Materials", "Telecom"]),
            "marketCap": random.uniform(1e7, 5e9),
            "lastPrice": random.uniform(10, 300),
            "change": random.uniform(-10, 10),
            "changePercent": random.uniform(-3, 3),
            "volume": random.randint(10_000, 5_000_000),
        }

    def _normalize(self, raw, symbol):
        return {
            "symbol": symbol,
            "name_ar": raw.get("arabicName"),
            "name_en": raw.get("englishName"),
            "sector": raw.get("sector"),
            "market_cap": raw.get("marketCap"),
            "last_price": raw.get("lastPrice"),
            "change": raw.get("change"),
            "change_percent": raw.get("changePercent"),
            "volume": raw.get("volume"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# =============================================================================
# FastAPI App
# =============================================================================

limiter = Limiter(key_func=get_remote_address)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.service = StockDataService()
    app.state.start = time.time()
    app.state.requests = 0
    app.state.total_time = 0
    yield
    cache.clear()


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware)


# =============================================================================
# Middleware: performance tracking
# =============================================================================

@app.middleware("http")
async def track(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start

    request.app.state.requests += 1
    request.app.state.total_time += duration

    response.headers["X-Process-Time"] = str(duration)
    return response


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/")
def root():
    return {"service": settings.APP_NAME, "version": settings.VERSION}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/api/v1/health")
def health_detailed(request: Request):
    rt = request.app.state
    avg = (rt.total_time / rt.requests * 1000) if rt.requests > 0 else 0
    return {
        "status": "healthy",
        "uptime": int(time.time() - rt.start),
        "avg_ms": round(avg, 2),
        "cache": cache.stats(),
        "env": settings.ENVIRONMENT,
    }


@app.get("/api/v1/symbols/{symbol}")
async def symbol(symbol: str, request: Request):
    svc: StockDataService = request.app.state.service
    data = await svc.get_symbol(symbol)
    return data


@app.get("/api/v1/symbols")
async def multiple(symbols: str, request: Request):
    svc: StockDataService = request.app.state.service
    sym_list = [s.strip() for s in symbols.split(",")]
    return [await svc.get_symbol(s) for s in sym_list]


@app.get("/api/v1/export/csv/{symbol}")
async def export_csv(symbol: str, request: Request):
    svc: StockDataService = request.app.state.service
    data = await svc.get_symbol(symbol)

    buf = io.StringIO()
    writer = csv.writer(buf)

    cols = ["symbol", "name_en", "last_price", "change", "change_percent", "volume", "timestamp"]
    writer.writerow(cols)
    writer.writerow([data.get(c, "") for c in cols])

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={symbol}.csv"},
    )


@app.post("/api/v1/export/to-sheets")
async def export_to_sheets(
    spreadsheet_id: str,
    sheet_name: str = "StockData",
    symbols: str = "2220,2010,1180",
    request: Request = None,
):
    svc: StockDataService = request.app.state.service
    sheet_data = [["Symbol", "Price", "Change", "Change%", "Volume", "Timestamp"]]

    for sym in [s.strip() for s in symbols.split(",")]:
        d = await svc.get_symbol(sym)
        sheet_data.append([
            d["symbol"], d["last_price"], d["change"], d["change_percent"],
            d["volume"], d["timestamp"]
        ])

    result = await svc.sheets.update_sheet(spreadsheet_id, sheet_name, sheet_data)
    return {"status": "ok", "details": result}


# =============================================================================
# Run (local only)
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("legacy_service:app", host="0.0.0.0", port=8000, reload=True)
