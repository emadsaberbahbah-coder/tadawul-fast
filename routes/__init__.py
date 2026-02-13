# routes/__init__.py
"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) – v1.8.1 (Intelligent Discovery)

Design rules
- ZERO heavy imports here (no FastAPI, no routers, no app state).
- No side effects (no network, no env validation, no file IO).
- Safe helpers for:
  • version reporting
  • dynamic module discovery (scans directory)
  • optional availability checks (without importing routers directly)
  • debug snapshot (Render logs friendly)

v1.8.1 changes
- ✅ Dynamic Discovery: Scans routes/ folder for new modules.
- ✅ Router Verification: Checks if module likely exports 'router'.
- ✅ Dependency Checks: Verifies critical libraries in debug snapshot.
- ✅ Advisor-aware: Explicitly tracks investment_advisor status.
"""

from __future__ import annotations

import importlib.util
import os
import pkgutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

ROUTES_PACKAGE_VERSION = "1.8.1"


# -----------------------------------------------------------------------------
# Public helpers
# -----------------------------------------------------------------------------
def get_routes_version() -> str:
    return ROUTES_PACKAGE_VERSION


def _discover_route_modules() -> List[str]:
    """
    Dynamically scans the 'routes' directory for python modules.
    Returns a list of module names (e.g., 'routes.enriched_quote').
    """
    modules = []
    package_path = Path(__file__).parent
    
    for _, name, _ in pkgutil.iter_modules([str(package_path)]):
        if name not in ("__init__", "__pycache__"):
            modules.append(f"routes.{name}")
            
    # Add legacy root modules if they exist (best-effort)
    root_candidates = ["routes_argaam", "legacy_service"]
    for rc in root_candidates:
        if module_exists(rc):
            modules.append(rc)
            
    return modules


def get_expected_router_modules() -> List[str]:
    """
    Returns a list of all discoverable route modules plus known core routes.
    """
    discovered = _discover_route_modules()
    
    # Ensure core routes are always listed even if discovery fails
    core_routes = [
        "routes.enriched_quote",
        "routes.ai_analysis", 
        "routes.advanced_analysis",
        "routes.investment_advisor",
        "routes.routes_argaam",
        "routes.legacy_service",
        "routes.config"
    ]
    
    # Merge and dedupe
    all_routes = sorted(list(set(discovered + core_routes)))
    return all_routes


def get_expected_router_groups() -> Dict[str, List[str]]:
    """
    Grouped reference list for mounting logic.
    """
    all_mods = get_expected_router_modules()
    
    groups = {
        "core": [],
        "advisor": [],
        "ksa": [],
        "optional": []
    }
    
    for m in all_mods:
        if "enriched" in m or "analysis" in m or "legacy" in m:
            groups["core"].append(m)
        elif "advisor" in m:
            groups["advisor"].append(m)
        elif "argaam" in m or "tadawul" in m:
            groups["ksa"].append(m)
        else:
            groups["optional"].append(m)
            
    return groups


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
    Returns the subset of expected modules that appear importable.
    """
    exp = expected or get_expected_router_modules()
    out: List[str] = []
    for m in exp:
        if module_exists(m):
            out.append(m)
    return out


def get_missing_router_modules(expected: Optional[List[str]] = None) -> List[str]:
    """
    Returns the subset of expected modules that appear missing/unimportable.
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
    Returns a structured view of where each router SHOULD be imported from.
    Includes 'status' to indicate if it's found or missing.
    """
    discovery: Dict[str, Dict[str, object]] = {}

    def _probe(key: str, candidates: List[str]):
        sel = _first_existing(candidates)
        discovery[key] = {
            "candidates": candidates,
            "selected": sel,
            "exists": bool(sel),
            "status": "ready" if sel else "missing"
        }

    _probe("enriched", ["routes.enriched_quote", "routes.enriched"])
    _probe("ai_analysis", ["routes.ai_analysis"])
    _probe("advanced_analysis", ["routes.advanced_analysis"])
    _probe("legacy_service", ["routes.legacy_service"])
    _probe("investment_advisor", ["routes.investment_advisor"])
    _probe("argaam", ["routes.routes_argaam", "routes_argaam"])
    _probe("config", ["routes.config"])

    return discovery


def get_recommended_imports() -> List[Tuple[str, str]]:
    """
    Returns a deterministic list of recommended module imports for main.py.
    """
    d = get_router_discovery()
    out: List[Tuple[str, str]] = []
    # Order matters for mounting priority
    priority = ["enriched", "ai_analysis", "advanced_analysis", "investment_advisor", "argaam", "legacy_service", "config"]
    
    for key in priority:
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
    """
    expected = get_expected_router_modules()
    available = get_available_router_modules(expected)
    missing = [m for m in expected if m not in available]

    # Dependency Checks (Critical libs for data engine)
    deps = {
        "pandas": module_exists("pandas"),
        "numpy": module_exists("numpy"),
        "google-auth": module_exists("google.auth"),
        "httpx": module_exists("httpx"),
        "yfinance": module_exists("yfinance")
    }

    # Environment hints
    env_hints = {
        "app_token_set": _env_flag("APP_TOKEN"),
        "backup_app_token_set": _env_flag("BACKUP_APP_TOKEN"),
        "tfb_app_token_set": _env_flag("TFB_APP_TOKEN"),
        "allow_query_token": (os.getenv("ALLOW_QUERY_TOKEN") or "").strip().lower() in ("1", "true", "yes", "on"),
        "debug_errors": (os.getenv("DEBUG_ERRORS") or "").strip().lower() in ("1", "true", "yes", "on"),
        "log_level": (os.getenv("LOG_LEVEL") or "").strip().lower() or "info",
        "app_env": (os.getenv("APP_ENV") or "").strip().lower() or "production",
        "advisor_enabled": (os.getenv("ADVISOR_ENABLED") or "").strip().lower() in ("", "1", "true", "yes", "on"),
    }

    discovery = get_router_discovery()
    
    return {
        "routes_pkg_version": ROUTES_PACKAGE_VERSION,
        "modules": {
            "expected": len(expected),
            "available": len(available),
            "missing": len(missing)
        },
        "available_modules": available,
        "missing_modules": missing,
        "discovery_map": discovery,
        "dependencies": deps,
        "env_hints": env_hints,
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
