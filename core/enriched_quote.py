"""
core/enriched_quote.py
Compatibility shim.

Some modules import:
  from core.enriched_quote import router

We re-export the router from routes.enriched_quote when available.
"""

from __future__ import annotations

from fastapi import APIRouter

try:
    from routes.enriched_quote import router as router  # type: ignore
except Exception:
    router = APIRouter()
