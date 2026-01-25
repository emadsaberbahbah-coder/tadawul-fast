# routes/__init__.py
"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) – v1.7.0 (Hardened++)

Design rules
- ZERO heavy imports here (no FastAPI, no routers, no app state).
- No side effects (no network, no env validation, no file IO).
- Safe helpers for:
  • version reporting
  • expected module discovery
  • optional availability checks (without importing routers directly)
  • debug snapshot (Render logs friendly)

Router mounting MUST remain in main.py (or your app factory).

v1.7.0 changes
- ✅ Adds optional "safe router discovery" helper (still no imports):
    get_router_discovery()
  Provides a structured view of which preferred module path exists per router.
- ✅ Keeps expected list but adds grouping (core/kSA/optional) for clarity.
- ✅ Adds SAFE environment hint flags (no secret leakage), including:
    ALLOW_QUERY_TOKEN, DEBUG_ERRORS, LOG_LEVEL, APP_ENV
- ✅ Provides "recommended_imports" field to make main.py mounting deterministic.
"""

from __future__ import annotations

import importlib.util
import os
from typing import Dict, List, Optional, Tuple

ROUTES_PACKAGE_VERSION = "1.7.0"


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
        # Core stable routes (within package)
        "routes.enriched_quote",
        "routes.enriched",  # optional alternate naming
        "routes.ai_analysis",
        "routes.advanced_analysis",
        "routes.legacy_service",

        # KSA gateway (main router inside package)
        "routes.routes_argaam",

        # KSA gateway (repo-root shim; keep for legacy deployments)
        "routes_argaam",

        # Optional shims / misc
        "routes.config",
    ]


def get_expected_router_groups() -> Dict[str, List[str]]:
    """
    Grouped reference list only (no imports).
    This is used for nicer debug output and deterministic mounting guidance.
    """
    return {
        "core": [
            "routes.enriched_quote",
            "routes.enriched",
            "routes.ai_analysis",
            "routes.advanced_analysis",
            "routes.legacy_service",
        ],
        "ksa": [
            "routes.routes_argaam",
            "routes_argaam",
        ],
        "optional": [
            "routes.config",
        ],
    }


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


# -----------------------------------------------------------------------------
# Deterministic router discovery (still NO imports)
# -----------------------------------------------------------------------------
def _first_existing(candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if module_exists(c):
            return c
    return None


def get_router_discovery() -> Dict[str, Dict[str, object]]:
    """
    Returns a structured view of where each router SHOULD be imported from
    (without importing).

    Example output keys:
      - enriched: preferred module path is routes.enriched_quote (or routes.enriched)
      - argaam: preferred module path is routes.routes_argaam (or routes_argaam)
    """
    discovery: Dict[str, Dict[str, object]] = {}

    # Enriched router can be in two common places
    enriched_candidates = ["routes.enriched_quote", "routes.enriched"]
    enriched_selected = _first_existing(enriched_candidates)
    discovery["enriched"] = {
        "candidates": enriched_candidates,
        "selected": enriched_selected,
        "exists": bool(enriched_selected),
    }

    # AI analysis routes
    ai_candidates = ["routes.ai_analysis"]
    ai_selected = _first_existing(ai_candidates)
    discovery["ai_analysis"] = {
        "candidates": ai_candidates,
        "selected": ai_selected,
        "exists": bool(ai_selected),
    }

    # Advanced analysis routes
    adv_candidates = ["routes.advanced_analysis"]
    adv_selected = _first_existing(adv_candidates)
    discovery["advanced_analysis"] = {
        "candidates": adv_candidates,
        "selected": adv_selected,
        "exists": bool(adv_selected),
    }

    # Legacy service routes
    legacy_candidates = ["routes.legacy_service"]
    legacy_selected = _first_existing(legacy_candidates)
    discovery["legacy_service"] = {
        "candidates": legacy_candidates,
        "selected": legacy_selected,
        "exists": bool(legacy_selected),
    }

    # Argaam router (package + repo-root shim)
    argaam_candidates = ["routes.routes_argaam", "routes_argaam"]
    argaam_selected = _first_existing(argaam_candidates)
    discovery["argaam"] = {
        "candidates": argaam_candidates,
        "selected": argaam_selected,
        "exists": bool(argaam_selected),
    }

    # Optional config router/module
    config_candidates = ["routes.config"]
    config_selected = _first_existing(config_candidates)
    discovery["config"] = {
        "candidates": config_candidates,
        "selected": config_selected,
        "exists": bool(config_selected),
    }

    return discovery


def get_recommended_imports() -> List[Tuple[str, str]]:
    """
    Returns a deterministic list of recommended module imports for main.py
    in the form: [(router_name, module_path), ...]
    WITHOUT importing anything.

    main.py can use these to decide which router modules to import/mount.
    """
    d = get_router_discovery()
    out: List[Tuple[str, str]] = []
    for key in ("enriched", "ai_analysis", "advanced_analysis", "legacy_service", "argaam", "config"):
        sel = d.get(key, {}).get("selected")
        if isinstance(sel, str) and sel:
            out.append((key, sel))
    return out


# -----------------------------------------------------------------------------
# Debug snapshot (Render logs friendly)
# -----------------------------------------------------------------------------
def _env_flag(name: str) -> bool:
    return bool((os.getenv(name) or "").strip())


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
    app_token_set = _env_flag("APP_TOKEN")
    backup_token_set = _env_flag("BACKUP_APP_TOKEN")

    # Extra safe hints (still no leakage)
    allow_query_token = (os.getenv("ALLOW_QUERY_TOKEN") or "").strip().lower() in ("1", "true", "yes", "on")
    debug_errors = (os.getenv("DEBUG_ERRORS") or "").strip().lower() in ("1", "true", "yes", "on")
    log_level = (os.getenv("LOG_LEVEL") or "").strip().lower() or ""
    app_env = (os.getenv("APP_ENV") or "").strip().lower() or ""

    # Optional: detect which Argaam module(s) exist
    argaam_layout = {
        "routes.routes_argaam": module_exists("routes.routes_argaam"),
        "routes_argaam": module_exists("routes_argaam"),
    }

    # Optional: detect which enriched module(s) exist
    enriched_layout = {
        "routes.enriched_quote": module_exists("routes.enriched_quote"),
        "routes.enriched": module_exists("routes.enriched"),
    }

    discovery = get_router_discovery()
    recommended_imports = get_recommended_imports()

    return {
        "routes_pkg_version": ROUTES_PACKAGE_VERSION,
        "expected_count": len(expected),
        "available_count": len(available),
        "missing_count": len(missing),
        "available": available,
        "missing": missing,
        "groups": get_expected_router_groups(),
        "router_discovery": discovery,
        "recommended_imports": recommended_imports,
        "argaam_layout": argaam_layout,
        "enriched_layout": enriched_layout,
        "env_hints": {
            "app_token_set": app_token_set,
            "backup_app_token_set": backup_token_set,
            "allow_query_token": allow_query_token,
            "debug_errors": debug_errors,
            "log_level": log_level,
            "app_env": app_env,
        },
    }


__all__ = [
    "ROUTES_PACKAGE_VERSION",
    "get_routes_version",
    "get_expected_router_modules",
    "get_expected_router_groups",
    "module_exists",
    "get_available_router_modules",
    "get_missing_router_modules",
    "get_router_discovery",
    "get_recommended_imports",
    "get_routes_debug_snapshot",
]
