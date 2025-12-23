# routes/__init__.py
"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) – v1.1.0

Goals
- Keep imports zero/optional to avoid cold-start failures on Render.
- Do NOT import routers here (router mounts happen in main.py).
- Provide lightweight helpers for debugging and safe discovery.

Why
- Any import side-effect here can break `import routes.*` and prevent router mounting.
"""

from __future__ import annotations

from typing import List


ROUTES_PACKAGE_VERSION = "1.1.0"


def get_routes_version() -> str:
    return ROUTES_PACKAGE_VERSION


def get_expected_router_modules() -> List[str]:
    """
    Only a reference list (does not import).
    Helps debug missing modules on Render logs.
    """
    return [
        "routes.enriched_quote",
        "routes.ai_analysis",
        "routes.advanced_analysis",
        "routes.legacy_service",
    ]


__all__ = ["ROUTES_PACKAGE_VERSION", "get_routes_version", "get_expected_router_modules"]
