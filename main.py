from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from routes.health import router as health_router
from routes.config_routes import router as config_router
from domain.ksa.router import router as ksa_router
from domain.global_mkt.router import router as global_router
from storage.database import init_db


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version=settings.version,
    )

    # Helpful for Apps Script / browser tooling; safe for server-to-server too
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(health_router)
    app.include_router(config_router)
    app.include_router(ksa_router)
    app.include_router(global_router)

    @app.on_event("startup")
    async def _startup() -> None:
        # Creates table if DATABASE_URL is configured (no-op otherwise)
        await init_db()

    return app


app = create_app()
