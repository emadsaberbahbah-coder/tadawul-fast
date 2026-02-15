"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) — v2.2.0 (TFB Hardened)

Design rules (kept)
- ZERO heavy imports here (no FastAPI, no routers, no app state)
- No side effects (no network, no env validation, no file IO)
- Safe helpers for version reporting and dynamic module discovery
- Deterministic mount order helpers for main.py

v2.2.0 upgrades
- ✅ Stronger discovery: handles package + legacy root modules safely
- ✅ Deterministic ordering: stable sort with explicit priority + grouping
- ✅ Better module_exists: avoids surprises, supports "routes.xxx" and "xxx"
- ✅ Dependency audit: optional, configurable, no heavy imports
- ✅ Env hints: stricter parsing, includes auth transport + timezone intent
- ✅ Render-friendly snapshots: compact + consistent keys for logging
"""

from __future__ import annotations

import importlib.util
import os
import pkgutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROUTES_PACKAGE_VERSION = "2.2.0"

# ---------------------------------------------------------------------
# Small utils (no heavy work)
# ---------------------------------------------------------------------
_TRUTHY = {"1", "true", "yes", "y", "on", "t"}
_FALSY = {"0", "false", "no", "n", "off", "f"}


def get_routes_version() -> str:
    return ROUTES_PACKAGE_VERSION


def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, "")
    if not v:
        return default
    vv = v.lower()
    if vv in _TRUTHY:
        return True
    if vv in _FALSY:
        return False
    return default


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        s = str(x or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# ---------------------------------------------------------------------
# Existence checks (NO imports)
# ---------------------------------------------------------------------
def module_exists(module_path: str) -> bool:
    """
    Check module availability WITHOUT importing it.
    Safe + side-effect free: uses importlib.util.find_spec.

    Supports:
    - "routes.enriched_quote"
    - "enriched_quote" (legacy root module)
    """
    if not module_path or not isinstance(module_path, str):
        return False
    mp = module_path.strip()
    if not mp:
        return False
    try:
        return importlib.util.find_spec(mp) is not None
    except Exception:
        return False


def _first_existing(candidates: Sequence[str]) -> Optional[str]:
    for c in candidates or []:
        if module_exists(c):
            return c
    return None


# ---------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------
def _routes_dir() -> Path:
    return Path(__file__).parent


def _discover_route_modules_in_package() -> List[str]:
    """
    Scan the 'routes' package directory for python modules.
    Returns: ["routes.xxx", ...] (no imports performed).
    """
    modules: List[str] = []
    package_path = _routes_dir()
    for _, name, _ in pkgutil.iter_modules([str(package_path)]):
        if name in ("__init__", "__pycache__"):
            continue
        modules.append(f"routes.{name}")
    return modules


def _legacy_root_candidates() -> List[str]:
    """
    Best-effort legacy modules that might live at repo root or older layout.
    These are NOT imported; only checked via find_spec.
    """
    return [
        # legacy names observed in refactors
        "routes_argaam",
        "legacy_service",
        "advisor",
        "advanced_analysis",
        "ai_analysis",
        "enriched_quote",
        "config",
        # alt advisor naming variants
        "investment_advisor",
        "advisor_engine",
        "advisor_router",
    ]


def get_expected_router_modules() -> List[str]:
    """
    Returns a deduped list of:
    - discovered package modules (routes.*)
    - known canonical modules (routes.*)
    - legacy root candidates (best-effort)
    """
    discovered = _discover_route_modules_in_package()

    canonical = [
        "routes.enriched_quote",
        "routes.ai_analysis",
        "routes.advanced_analysis",
        "routes.advisor",
        "routes.investment_advisor",
        "routes.routes_argaam",
        "routes.legacy_service",
        "routes.config",
    ]

    legacy = _legacy_root_candidates()

    # Keep a stable ordering: discovered first, then canonical, then legacy.
    # Dedupe while preserving order.
    return _dedupe_keep_order(discovered + canonical + legacy)


def get_available_router_modules(expected: Optional[Sequence[str]] = None) -> List[str]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    return [m for m in exp if module_exists(m)]


# ---------------------------------------------------------------------
# Grouping helpers (for main.py mounting logic)
# ---------------------------------------------------------------------
def get_expected_router_groups() -> Dict[str, List[str]]:
    """
    Group modules by intent to help main.py mount in a clean order.
    """
    mods = get_expected_router_modules()

    groups: Dict[str, List[str]] = {
        "security": [],
        "core": [],
        "advisor": [],
        "ksa": [],
        "system": [],
        "other": [],
    }

    for m in mods:
        ml = m.lower()

        # system/config/health-ish
        if any(x in ml for x in ("config", "health", "system", "status")):
            groups["system"].append(m)
            continue

        # advisor (includes investment_advisor variants)
        if "advisor" in ml:
            groups["advisor"].append(m)
            continue

        # KSA-focused (argaam/tadawul)
        if any(x in ml for x in ("argaam", "tadawul", "ksa")):
            groups["ksa"].append(m)
            continue

        # core analytics/quotes/legacy
        if any(x in ml for x in ("enriched", "analysis", "legacy", "quote")):
            groups["core"].append(m)
            continue

        groups["other"].append(m)

    # Dedupe within each group (keep order)
    for k in list(groups.keys()):
        groups[k] = _dedupe_keep_order(groups[k])

    return groups


# ---------------------------------------------------------------------
# Deterministic router discovery map
# ---------------------------------------------------------------------
def get_router_discovery() -> Dict[str, Dict[str, object]]:
    """
    Structured view of where each router SHOULD be imported from.
    Handles refactors (package route vs legacy root).
    No imports performed.
    """
    discovery: Dict[str, Dict[str, object]] = {}

    def _probe(key: str, candidates: Sequence[str]) -> None:
        sel = _first_existing(candidates)
        discovery[key] = {
            "candidates": list(candidates),
            "selected": sel,
            "exists": bool(sel),
            "status": "ready" if sel else "missing",
        }

    # Priority candidates: package module first, then older alternates, then root
    _probe("enriched", ["routes.enriched_quote", "routes.enriched", "enriched_quote"])
    _probe("ai_analysis", ["routes.ai_analysis", "routes.ai", "ai_analysis"])
    _probe("advanced_analysis", ["routes.advanced_analysis", "advanced_analysis"])
    _probe(
        "advisor",
        [
            "routes.advisor",
            "routes.investment_advisor",
            "routes.advisor_engine",
            "investment_advisor",
            "advisor_engine",
            "advisor",
        ],
    )
    _probe("argaam", ["routes.routes_argaam", "routes.argaam", "routes_argaam"])
    _probe("legacy", ["routes.legacy_service", "legacy_service"])
    _probe("config", ["routes.config", "config"])

    return discovery


def get_recommended_imports() -> List[Tuple[str, str]]:
    """
    Deterministic list of recommended imports for main.py mounting.
    Return: [(key, module_path), ...]
    """
    d = get_router_discovery()
    out: List[Tuple[str, str]] = []

    # Mount order: System/Config -> Enriched -> Analysis -> Advisor -> KSA -> Legacy
    priority = ["config", "enriched", "ai_analysis", "advanced_analysis", "advisor", "argaam", "legacy"]

    for key in priority:
        sel = d.get(key, {}).get("selected")
        if isinstance(sel, str) and sel:
            out.append((key, sel))

    return out


# ---------------------------------------------------------------------
# Dependency audit (optional)
# ---------------------------------------------------------------------
def get_dependency_audit(extra: Optional[Sequence[str]] = None) -> Dict[str, bool]:
    """
    Lightweight "presence" check only (find_spec). No imports.
    """
    base = [
        "pandas",
        "numpy",
        "httpx",
        "zoneinfo",  # timezone support (py>=3.9)
        "yfinance",
    ]
    if extra:
        base.extend(list(extra))
    base = _dedupe_keep_order(base)
    return {name: module_exists(name) for name in base}


# ---------------------------------------------------------------------
# Debug snapshot (Render logs friendly)
# ---------------------------------------------------------------------
def get_routes_debug_snapshot() -> Dict[str, object]:
    """
    Extremely lightweight debug snapshot for logs.
    Safe to call during startup without triggering FastAPI/router imports.
    """
    expected = get_expected_router_modules()
    available = get_available_router_modules(expected)
    missing = [m for m in expected if m not in available]

    env_hints = {
        "app_env": (_env_str("APP_ENV", "production") or "production").lower(),
        "log_level": (_env_str("LOG_LEVEL", "info") or "info").lower(),
        "debug_mode": _env_bool("DEBUG_ERRORS", False),
        "advisor_enabled": _env_bool("ADVISOR_ENABLED", True),
        "enforce_riyadh_time": _env_bool("ENFORCE_RIYADH_TIME", True),
        # auth hints
        "app_token_set": bool(_env_str("APP_TOKEN", "")),
        "auth_header_expected": _env_bool("EXPECT_AUTH_HEADER", True),
        "token_transport": (_env_str("TOKEN_TRANSPORT", "") or _env_str("APPS_SCRIPT_TOKEN_TRANSPORT", "header,query")).lower(),
        # timezone intent (string only; no tz import)
        "tz": _env_str("TZ", "") or _env_str("TIMEZONE", "") or "Asia/Riyadh",
    }

    return {
        "routes_pkg_version": ROUTES_PACKAGE_VERSION,
        "summary": {
            "expected_count": len(expected),
            "available_count": len(available),
            "missing_count": len(missing),
        },
        "recommended_imports": get_recommended_imports(),
        "available": available,
        "missing": missing[:50],  # cap to keep logs short
        "discovery": get_router_discovery(),
        "groups": get_expected_router_groups(),
        "deps": get_dependency_audit(),
        "env_hints": env_hints,
    }


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
    "get_dependency_audit",
]
