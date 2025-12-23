# routes/__init__.py
"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) – v1.3.0 (Hardened)

Design rules
- ZERO heavy imports here (no FastAPI, no routers, no app state).
- No side effects (no network, no env validation, no file IO).
- Safe helpers for:
    • version reporting
    • expected module discovery
    • optional availability checks (without importing routers directly)
    • debug snapshot (Render logs friendly)

Router mounting MUST remain in main.py (or your app factory).
"""

from __future__ import annotations

import importlib.util
import os
from typing import Dict, List, Optional, Tuple

ROUTES_PACKAGE_VERSION = "1.3.0"


# -----------------------------------------------------------------------------
# Public helpers
# -----------------------------------------------------------------------------

def get_routes_version() -> str:
    return ROUTES_PACKAGE_VERSION


def get_expected_router_modules() -> List[str]:
    """
    Reference list only (no imports).
    Update this list when adding/removing route modules.
    """
    return [
        # Core stable routes
        "routes.enriched_quote",
        "routes.ai_analysis",
        "routes.advanced_analysis",
        "routes.legacy_service",
        # Optional / KSA gateway
        "routes_argaam",
        # Optional shims
        "routes.config",
    ]


def module_exists(module_path: str) -> bool:
    """
    Check module availability WITHOUT importing it.
    Uses importlib.util.find_spec which is safe and side-effect free.
    """
    if not module_path or not isinstance(module_path, str):
        return False
    try:
        return importlib.util.find_spec(module_path) is not None
    except Exception:
        return False


def get_available_router_modules(expected: Optional[List[str]] = None) -> List[str]:
    """
    Returns the subset of expected modules that appear importable
    (does NOT import them).
    """
    exp = expected or get_expected_router_modules()
    out: List[str] = []
    for m in exp:
        if module_exists(m):
            out.append(m)
    return out


def get_missing_router_modules(expected: Optional[List[str]] = None) -> List[str]:
    """
    Returns the subset of expected modules that appear missing/unimportable
    (does NOT import them).
    """
    exp = expected or get_expected_router_modules()
    out: List[str] = []
    for m in exp:
        if not module_exists(m):
            out.append(m)
    return out


def get_routes_debug_snapshot() -> Dict[str, object]:
    """
    Extremely lightweight debug snapshot for logs.
    Safe to call during startup.

    NOTE: This does not validate env vars or import routers.
    """
    expected = get_expected_router_modules()
    available = get_available_router_modules(expected)
    missing = [m for m in expected if m not in available]

    # Minimal environment hints (do NOT leak secrets)
    # We only report whether a token is set.
    app_token_set = bool((os.getenv("APP_TOKEN") or "").strip())
    backup_token_set = bool((os.getenv("BACKUP_APP_TOKEN") or "").strip())

    return {
        "routes_pkg_version": ROUTES_PACKAGE_VERSION,
        "expected_count": len(expected),
        "available_count": len(available),
        "missing_count": len(missing),
        "available": available,
        "missing": missing,
        "env_hints": {
            "app_token_set": app_token_set,
            "backup_app_token_set": backup_token_set,
        },
    }


__all__ = [
    "ROUTES_PACKAGE_VERSION",
    "get_routes_version",
    "get_expected_router_modules",
    "module_exists",
    "get_available_router_modules",
    "get_missing_router_modules",
    "get_routes_debug_snapshot",
]
