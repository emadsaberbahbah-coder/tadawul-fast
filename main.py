from __future__ import annotations

import os
import logging
from typing import Optional, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings

log = logging.getLogger("tfb.main")


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_list(name: str, default: Optional[List[str]] = None) -> List[str]:
    v = os.getenv(name, "")
    if not v.strip():
        return default or []
    return [x.strip() for x in v.split(",") if x.strip()]


def _safe_include_router(app: FastAPI, import_path: str, alias: str) -> None:
    """
    Import router lazily and include it.
    Never crash app startup if an optional router fails to import.
    """
    try:
        module_path, obj_name = import_path.rsplit(":", 1)
        mod = __import__(module_path, fromlist=[obj_name])
        router = getattr(mod, obj_name)
        app.include_router(router)
        log.info("✅ Router loaded: %s (%s)", alias, import_path)
    except Exception as e:
        log.error("⚠️ Router NOT loaded: %s (%s). Reason: %s", alias, import_path, e)


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version=settings.version,
        docs_url="/docs",
        redoc_url=None,
        openapi_url="/openapi.json",
    )

    # -----------------------------
    # Root handlers (Render-friendly)
    # -----------------------------
    @app.get("/", include_in_schema=False)
    def root():
        return {
            "status": "ok",
            "app": settings.app_name,
            "version": settings.version,
            "docs": "/docs",
            "openapi": "/openapi.json",
            "health": "/health",
        }

    @app.head("/", include_in_schema=False)
    def root_head():
        # Some platforms send HEAD / to check liveness
        return None

    # -----------------------------
    # CORS (configurable)
    # -----------------------------
    allow_origins = _env_list("CORS_ALLOW_ORIGINS", default=["*"])  # e.g. "https://site.com,https://x.com"
    allow_credentials = _env_bool("CORS_ALLOW_CREDENTIALS", default=False)

    # If you use "*" you should keep allow_credentials=False (browser rule)
    if "*" in allow_origins:
        allow_credentials = False

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -----------------------------
    # Routers (safe include)
    # -----------------------------
    # Core Phase-1 routers
    _safe_include_router(app, "routes.health:router", "health")
    _safe_include_router(app, "routes.config_routes:router", "config")
    _safe_include_router(app, "routes.pages_routes:router", "pages")
    _safe_include_router(app, "routes.validate_routes:router", "validate")

    # Domains
    enable_ksa = _env_bool("ENABLE_KSA_ROUTES", default=True)
    if enable_ksa:
        _safe_include_router(app, "domain.ksa.router:router", "ksa")
    else:
        log.info("ℹ️ KSA routes disabled (ENABLE_KSA_ROUTES=0)")

    _safe_include_router(app, "domain.global_mkt.router:router", "global")

    return app


app = create_app()
