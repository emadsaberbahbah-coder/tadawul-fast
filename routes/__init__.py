"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) – v1.9.0 (Financial Leader Edition)

Design rules:
- ZERO heavy imports here (no FastAPI, no routers, no app state).
- No side effects (no network, no env validation, no file IO).
- Safe helpers for version reporting and dynamic module discovery.
- Aligned with core/symbols/normalize.py v1.2.0 standards.

v1.9.0 Changes:
- ✅ **Advanced Mapping**: Probes for both 'advisor' and 'investment_advisor' variants.
- ✅ **Dependency Auditing**: Verifies presence of 'zoneinfo' and 'yfinance'.
- ✅ **Mount Priority**: Optimized deterministic order for main.py integration.
- ✅ **Environment Hints**: Tracks Riyadh localization and Auth Header status.
"""

from __future__ import annotations

import importlib.util
import os
import pkgutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

ROUTES_PACKAGE_VERSION = "1.9.0"


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
            
    # Add legacy root modules if they exist (best-effort compatibility)
    root_candidates = ["routes_argaam", "legacy_service", "advisor", "advanced_analysis"]
    for rc in root_candidates:
        if module_exists(rc):
            modules.append(rc)
            
    return modules


def get_expected_router_modules() -> List[str]:
    """
    Returns a list of all discoverable route modules plus known core routes.
    """
    discovered = _discover_route_modules()
    
    # Explicit list of canonical routes to ensure they are tracked
    core_routes = [
        "routes.enriched_quote",
        "routes.ai_analysis", 
        "routes.advanced_analysis",
        "routes.advisor",
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
    Grouped reference list for mounting logic in main.py.
    """
    all_mods = get_expected_router_modules()
    
    groups = {
        "core": [],
        "advisor": [],
        "ksa": [],
        "system": []
    }
    
    for m in all_mods:
        m_low = m.lower()
        if "enriched" in m_low or "analysis" in m_low or "legacy" in m_low:
            groups["core"].append(m)
        elif "advisor" in m_low:
            groups["advisor"].append(m)
        elif "argaam" in m_low or "tadawul" in m_low:
            groups["ksa"].append(m)
        elif "config" in m_low or "health" in m_low:
            groups["system"].append(m)
            
    return groups


def module_exists(module_path: str) -> bool:
    """
    Check module availability WITHOUT importing it.
    Uses importlib.util.find_spec which is safe and side-effect free.
    """
    if not module_path or not isinstance(module_path, str):
        return False
    try:
        # Standard lookup
        spec = importlib.util.find_spec(module_path)
        return spec is not None
    except Exception:
        return False


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
    This logic handles repo-refactoring (e.g. moving investment_advisor to advisor).
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

    # Probing patterns based on the most robust project structure
    _probe("enriched", ["routes.enriched_quote", "routes.enriched"])
    _probe("ai_analysis", ["routes.ai_analysis", "routes.ai"])
    _probe("advanced_analysis", ["routes.advanced_analysis"])
    _probe("advisor", ["routes.advisor", "routes.investment_advisor", "routes.advisor_engine"])
    _probe("argaam", ["routes.routes_argaam", "routes_argaam"])
    _probe("legacy", ["routes.legacy_service", "legacy_service"])
    _probe("config", ["routes.config"])

    return discovery


def get_recommended_imports() -> List[Tuple[str, str]]:
    """
    Returns a deterministic list of recommended module imports for main.py.
    Used for mounting order: Security -> Core -> KSA -> Optional.
    """
    d = get_router_discovery()
    out: List[Tuple[str, str]] = []
    
    # Execution/Mount Order: Enriched Quote first (primary), then Analysis, then Advisor
    priority = ["enriched", "ai_analysis", "advanced_analysis", "advisor", "argaam", "legacy", "config"]
    
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


def get_available_router_modules(expected: Optional[List[str]] = None) -> List[str]:
    exp = expected or get_expected_router_modules()
    return [m for m in exp if module_exists(m)]


def get_routes_debug_snapshot() -> Dict[str, object]:
    """
    Extremely lightweight debug snapshot for logs.
    Safe to call during startup without triggering network/FastAPI.
    """
    expected = get_expected_router_modules()
    available = get_available_router_modules(expected)
    missing = [m for m in expected if m not in available]

    # Dependency Checks (Critical libs for the modern data engine)
    deps = {
        "pandas": module_exists("pandas"),
        "numpy": module_exists("numpy"),
        "zoneinfo": module_exists("zoneinfo"), # Critical for Riyadh Time
        "httpx": module_exists("httpx"),
        "yfinance": module_exists("yfinance") # Essential for Fundamentals
    }

    # Environment hints
    env_hints = {
        "app_token_set": _env_flag("APP_TOKEN"),
        "riyadh_time_enforced": _env_bool_safe("ENFORCE_RIYADH_TIME", True),
        "debug_mode": _env_bool_safe("DEBUG_ERRORS", False),
        "log_level": (os.getenv("LOG_LEVEL") or "info").lower(),
        "app_env": (os.getenv("APP_ENV") or "production").lower(),
        "advisor_active": _env_bool_safe("ADVISOR_ENABLED", True),
    }

    return {
        "routes_pkg_version": ROUTES_PACKAGE_VERSION,
        "summary": {
            "expected_count": len(expected),
            "available_count": len(available),
            "missing_count": len(missing)
        },
        "available": available,
        "discovery": get_router_discovery(),
        "system_deps": deps,
        "env_hints": env_hints,
    }


def _env_bool_safe(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v: return default
    return v in ("1", "true", "yes", "on", "t")


__all__ = [
    "ROUTES_PACKAGE_VERSION",
    "get_routes_version",
    "get_expected_router_modules",
    "get_expected_router_groups",
    "module_exists",
    "get_available_router_modules",
    "get_router_discovery",
    "get_recommended_imports",
    "get_routes_debug_snapshot",
]
