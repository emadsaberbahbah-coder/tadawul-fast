# main.py — FULL REPLACEMENT — v5.3.5
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE + FAST BOOT) — v5.3.5

What's New in v5.3.5:
- ✅ Full alignment with DataEngineV2 (v2.8.5) and Hardened Providers.
- ✅ Lifecycle: Ensures get_engine() singleton is initialized on boot.
- ✅ Lifecycle: Ensures engine.aclose() is called on shutdown for session cleanup.
- ✅ Fast Boot: Binds port immediately while routers/engine load in background.
- ✅ Auth-Aware: Sets up environment aliases for X-APP-TOKEN consistency.

Entrypoint for Render/uvicorn:  main:app
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import sys
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse, PlainTextResponse, Response

# ---------------------------------------------------------------------
# Path safety & Constants
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
APP_ENTRY_VERSION = "5.3.5"

# ---------------------------------------------------------------------
# Environment & Settings Resolution
# ---------------------------------------------------------------------

def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY

def _clamp_str(s: Any, max_len: int = 2000) -> str:
    txt = (str(s) if s is not None else "").strip()
    if not txt: return ""
    if len(txt) <= max_len: return txt
    return txt[:max_len-12] + " ...TRUNC..."

def _apply_runtime_env_aliases():
    """Ensure consistency between various naming conventions for API keys/tokens."""
    # Auth Tokens
    token = (os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN") or "").strip()
    if token:
        os.environ.setdefault("APP_TOKEN", token)
        os.environ.setdefault("TFB_APP_TOKEN", token)
    
    # Provider Keys
    eodhd = (os.getenv("EODHD_API_KEY") or os.getenv("EODHD_API_TOKEN") or "").strip()
    if eodhd: os.environ.setdefault("EODHD_API_KEY", eodhd)
    
    finnhub = (os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_API_TOKEN") or "").strip()
    if finnhub: os.environ.setdefault("FINNHUB_API_KEY", finnhub)

# ---------------------------------------------------------------------
# Router & Engine Management
# ---------------------------------------------------------------------

def _mount_router(app_: FastAPI, name: str, candidates: List[str]) -> Dict[str, Any]:
    """Import and include a router from the first available module candidate."""
    report = {"name": name, "mounted": False, "loaded_from": None}
    mod = None
    for path in candidates:
        try:
            mod = import_module(path)
            report["loaded_from"] = path
            break
        except Exception: continue

    if mod:
        router_obj = getattr(mod, "router", None) or (getattr(mod, "get_router")() if hasattr(mod, "get_router") else None)
        if router_obj:
            try:
                app_.include_router(router_obj)
                report["mounted"] = True
                logging.getLogger("main").info(f"Mounted router: {name} ({path})")
            except Exception as e:
                report["error"] = str(e)
    return report

async def _init_engine_singleton(app_: FastAPI):
    """Initializes the Unified Data Engine and its hardened sessions."""
    try:
        from core.data_engine_v2 import get_engine
        eng = await get_engine()
        app_.state.engine = eng
        app_.state.engine_ready = True
        logging.getLogger("main").info(f"Data Engine v{getattr(eng, 'ENGINE_VERSION', 'unknown')} initialized.")
    except Exception as e:
        app_.state.engine_ready = False
        app_.state.engine_error = _clamp_str(traceback.format_exc())
        logging.getLogger("main").error(f"Failed to initialize Data Engine: {e}")

async def _background_boot(app_: FastAPI, router_plan: List[Tuple[str, List[str]]], required: List[str]):
    """Boot process run in background to allow immediate port binding on Render."""
    try:
        # 1. Initialize Engine (Hardened Sessions)
        await _init_engine_singleton(app_)
        
        # 2. Mount Routers
        reports = []
        for name, candidates in router_plan:
            reports.append(_mount_router(app_, name, candidates))
        
        app_.state.mount_report = reports
        mounted_names = {r["name"] for r in reports if r["mounted"]}
        app_.state.routers_ready = all(req in mounted_names for req in required)
        app_.state.boot_completed = True
    except Exception as e:
        app_.state.boot_error = str(e)
        logging.getLogger("main").error(f"Background boot encountered error: {e}")

# ---------------------------------------------------------------------
# App Factory
# ---------------------------------------------------------------------

def create_app() -> FastAPI:
    _apply_runtime_env_aliases()
    
    # Configure Logging
    log_format = os.getenv("LOG_FORMAT", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger("main")

    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        # State Initialization
        app_.state.boot_completed = False
        app_.state.routers_ready = False
        app_.state.engine_ready = False
        app_.state.start_time = datetime.now(timezone.utc).isoformat()
        
        # Define Router Plan
        router_plan = [
            ("enriched_quote", ["routes.enriched_quote", "core.enriched_quote"]),
            ("ai_analysis", ["routes.ai_analysis"]),
            ("advanced_analysis", ["routes.advanced_analysis"]),
            ("routes_argaam", ["routes.routes_argaam", "routes_argaam"]),
            ("legacy_service", ["core.legacy_service", "legacy_service"]),
        ]
        required = ["enriched_quote"]

        # Background Boot (Render Port binding logic)
        app_.state.boot_task = asyncio.create_task(_background_boot(app_, router_plan, required))
        
        logger.info(f"🚀 Tadawul Fast Bridge v{APP_ENTRY_VERSION} starting...")
        yield
        
        # Shutdown: Graceful cleanup
        if hasattr(app_.state, "engine"):
            await app_.state.engine.aclose()
            logger.info("Data Engine connections closed.")

    app = FastAPI(
        title="Tadawul Fast Bridge",
        version=APP_ENTRY_VERSION,
        lifespan=lifespan
    )

    # Middlewares
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -----------------------------------------------------------------
    # System Routes
    # -----------------------------------------------------------------
    
    @app.get("/", include_in_schema=False)
    async def root():
        return {
            "status": "ok",
            "app": "Tadawul Fast Bridge",
            "version": app.version,
            "boot_completed": getattr(app.state, "boot_completed", False)
        }

    @app.get("/readyz", tags=["system"])
    async def readyz():
        """Health check for load balancers/orchestrators."""
        return {
            "status": "ok" if getattr(app.state, "boot_completed", False) else "booting",
            "engine_ready": getattr(app.state, "engine_ready", False),
            "routers_ready": getattr(app.state, "routers_ready", False),
            "boot_error": getattr(app.state, "boot_error", None)
        }

    @app.get("/health", tags=["system"])
    async def full_health():
        """Detailed diagnostic health check."""
        eng = getattr(app.state, "engine", None)
        return {
            "status": "ok",
            "entry_version": APP_ENTRY_VERSION,
            "engine_version": getattr(eng, "ENGINE_VERSION", "none") if eng else "none",
            "uptime_start": getattr(app.state, "start_time", None),
            "mounted_routers": [r["name"] for r in getattr(app.state, "mount_report", []) if r["mounted"]],
            "failed_routers": [r["name"] for r in getattr(app.state, "mount_report", []) if not r["mounted"]]
        }

    return app

# Entrypoint for uvicorn
app = create_app()
