# main.py — FULL REPLACEMENT — v5.3.6
"""
main.py
------------------------------------------------------------
Tadawul Fast Bridge – FastAPI Entry Point (PROD SAFE + FAST BOOT) — v5.3.6

Fixes:
- ✅ Resolves ValueError: Invalid format 'detailed' for '%' style.
- ✅ Keywords like 'detailed', 'simple', 'json' are now correctly mapped to format strings.
- ✅ Aligned with DataEngineV2 (v2.8.5) and Hardened Providers.
- ✅ Fast Boot: Binds port immediately while engine/routers load in background.

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
APP_ENTRY_VERSION = "5.3.6"

# ---------------------------------------------------------------------
# Logging & Environment Helpers
# ---------------------------------------------------------------------

def _resolve_log_format() -> str:
    """
    Map LOG_FORMAT keywords to valid Python logging format strings.
    Prevents ValueError: Invalid format 'detailed' for '%' style.
    """
    raw = str(os.getenv("LOG_FORMAT", "")).strip()
    low = raw.lower()
    
    # If the user provided a raw Python format string, use it
    if "%(" in raw:
        return raw
    
    # Keyword Mapping
    if low in {"detailed", "full", "verbose"}:
        return "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    if low in {"simple", "compact"}:
        return "%(levelname)s | %(message)s"
    if low == "json":
        return '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}'
    
    # Default fallback
    return "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in _TRUTHY

def _clamp_str(s: Any, max_len: int = 2000) -> str:
    txt = (str(s) if s is not None else "").strip()
    if not txt: return ""
    if len(txt) <= max_len: return txt
    return txt[:max_len-12] + " ...TRUNC..."

def _apply_runtime_env_aliases():
    """Ensure consistency between naming conventions for tokens."""
    token = (os.getenv("APP_TOKEN") or os.getenv("TFB_APP_TOKEN") or "").strip()
    if token:
        os.environ.setdefault("APP_TOKEN", token)
        os.environ.setdefault("TFB_APP_TOKEN", token)

# ---------------------------------------------------------------------
# Router & Engine Management
# ---------------------------------------------------------------------

def _mount_router(app_: FastAPI, name: str, candidates: List[str]) -> Dict[str, Any]:
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
        logging.getLogger("main").info("Data Engine initialized.")
    except Exception as e:
        app_.state.engine_ready = False
        app_.state.engine_error = _clamp_str(traceback.format_exc())
        logging.getLogger("main").error(f"Failed to initialize Data Engine: {e}")

async def _background_boot(app_: FastAPI, router_plan: List[Tuple[str, List[str]]], required: List[str]):
    try:
        await _init_engine_singleton(app_)
        reports = []
        for name, candidates in router_plan:
            reports.append(_mount_router(app_, name, candidates))
        
        app_.state.mount_report = reports
        mounted_names = {r["name"] for r in reports if r["mounted"]}
        app_.state.routers_ready = all(req in mounted_names for req in required)
        app_.state.boot_completed = True
    except Exception as e:
        app_.state.boot_error = str(e)
        logging.getLogger("main").error(f"Background boot error: {e}")

# ---------------------------------------------------------------------
# App Factory
# ---------------------------------------------------------------------

def create_app() -> FastAPI:
    _apply_runtime_env_aliases()
    
    # Fix: Resolve the keyword 'detailed' to a valid format string
    log_format = _resolve_log_format()
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger("main")

    @asynccontextmanager
    async def lifespan(app_: FastAPI):
        app_.state.boot_completed = False
        app_.state.routers_ready = False
        app_.state.engine_ready = False
        app_.state.start_time = datetime.now(timezone.utc).isoformat()
        
        router_plan = [
            ("enriched_quote", ["routes.enriched_quote", "core.enriched_quote"]),
            ("ai_analysis", ["routes.ai_analysis"]),
            ("advanced_analysis", ["routes.advanced_analysis"]),
            ("routes_argaam", ["routes.routes_argaam", "routes_argaam"]),
            ("legacy_service", ["core.legacy_service", "legacy_service"]),
        ]
        required = ["enriched_quote"]

        app_.state.boot_task = asyncio.create_task(_background_boot(app_, router_plan, required))
        
        logger.info(f"🚀 Tadawul Fast Bridge v{APP_ENTRY_VERSION} starting...")
        yield
        
        if hasattr(app_.state, "engine"):
            await app_.state.engine.aclose()
            logger.info("Data Engine connections closed.")

    app = FastAPI(title="Tadawul Fast Bridge", version=APP_ENTRY_VERSION, lifespan=lifespan)

    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

    @app.get("/", include_in_schema=False)
    async def root():
        return {"status": "ok", "app": "Tadawul Fast Bridge", "boot_completed": getattr(app.state, "boot_completed", False)}

    @app.get("/readyz", tags=["system"])
    async def readyz():
        return {
            "status": "ok" if getattr(app.state, "boot_completed", False) else "booting",
            "engine_ready": getattr(app.state, "engine_ready", False),
            "routers_ready": getattr(app.state, "routers_ready", False)
        }

    return app

app = create_app()
