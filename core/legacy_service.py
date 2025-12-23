"""
core/legacy_service.py
Compatibility shim for legacy_service router.
"""

from __future__ import annotations

from fastapi import APIRouter

try:
    from legacy_service import router as router  # type: ignore
except Exception:
    try:
        from routes.legacy_service import router as router  # type: ignore
    except Exception:
        router = APIRouter()
