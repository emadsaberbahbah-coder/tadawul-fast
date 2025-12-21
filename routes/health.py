from __future__ import annotations

import os
from datetime import datetime, timezone
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) and v.strip() else default


@router.get("/", include_in_schema=False)
def root():
    # Render sends HEAD/GET to "/" sometimes for checks; keep it 200 always
    return {
        "status": "ok",
        "app": _env("SERVICE_NAME", "Tadawul Fast Bridge"),
        "version": _env("SERVICE_VERSION", _env("APP_VERSION", "5.0.0-phase1")),
        "docs": "/docs",
        "openapi": "/openapi.json",
        "health": "/health",
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }


@router.head("/", include_in_schema=False)
def root_head():
    return Response(status_code=200)


@router.get("/health")
def health():
    return {
        "status": "ok",
        "app": _env("SERVICE_NAME", "Tadawul Fast Bridge"),
        "version": _env("SERVICE_VERSION", _env("APP_VERSION", "5.0.0-phase1")),
        "env": _env("ENVIRONMENT", _env("ENV", "production")),
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }
