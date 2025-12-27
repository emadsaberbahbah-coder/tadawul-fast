# routes/enriched_quote.py  (FULL REPLACEMENT)
"""
routes/enriched_quote.py
------------------------------------------------------------
Shim router (PROD SAFE) — v2.3.4

This file re-exports the canonical router from core.enriched_quote to avoid:
- duplicated endpoints
- schema drift
- inconsistent batch behavior
"""

from __future__ import annotations

from core.enriched_quote import router as router  # noqa: F401

__all__ = ["router"]
