"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) — v2.4.0 (TFB Hardened+)

Design rules (kept)
- ZERO heavy imports here (no FastAPI, no routers, no app state)
- No side effects (no network, no env validation, no file IO)
- Safe helpers for version reporting and dynamic module discovery
- Deterministic mount order helpers for main.py

v2.4.0 upgrades
- ✅ Cached module_exists (find_spec) for faster startup / repeated calls
- ✅ Smarter discovery: uses package __path__ if available (namespace-friendly)
- ✅ Include/Exclude filters via env (ROUTES_INCLUDE / ROUTES_EXCLUDE) with glob patterns
- ✅ Better grouping: explicit "security" routing + refined heuristics
- ✅ Mount plan: structured, deterministic, with reasons (still no imports)
- ✅ Debug snapshot: includes filters + mount plan + resolver results (Render-friendly)
"""

from __future__ import annotations

import fnmatch
import importlib.util
import os
import pkgutil
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROUTES_PACKAGE_VERSION = "2.4.0"

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


def _env_csv(name: str, default: str = "") -> List[str]:
    raw = _env_str(name, default)
    if not raw:
        return []
    parts = [p.strip() for p in re.split(r"[,\n;]+", raw) if p.strip()]
    return parts


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


def _normalize_module_path(module_path: str) -> str:
    return str(module_path or "").strip()


def _looks_like_glob(p: str) -> bool:
    return any(ch in p for ch in ("*", "?", "["))


def _match_any(name: str, patterns: Sequence[str]) -> bool:
    if not patterns:
        return False
    for p in patterns:
        pp = (p or "").strip()
        if not pp:
            continue
        if _looks_like_glob(pp):
            if fnmatch.fnmatchcase(name, pp):
                return True
        else:
            if name == pp:
                return True
    return False


# ---------------------------------------------------------------------
# Existence checks (NO imports)
# ---------------------------------------------------------------------
@lru_cache(maxsize=2048)
def module_exists(module_path: str) -> bool:
    """
    Check module availability WITHOUT importing it.
    Safe + side-effect free: uses importlib.util.find_spec.

    Supports:
    - "routes.enriched_quote"
    - "enriched_quote" (legacy root module)
    """
    mp = _normalize_module_path(module_path)
    if not mp:
        return False
    try:
        return importlib.util.find_spec(mp) is not None
    except Exception:
        return False


def module_exists_any(candidates: Sequence[str]) -> bool:
    for c in candidates or []:
        if module_exists(c):
            return True
    return False


def _first_existing(candidates: Sequence[str]) -> Optional[str]:
    for c in candidates or []:
        if module_exists(c):
            return c
    return None


# ---------------------------------------------------------------------
# Discovery (NO imports of route modules)
# ---------------------------------------------------------------------
def _routes_dir() -> Path:
    return Path(__file__).parent


def _iter_package_module_names() -> List[str]:
    """
    Returns bare module names inside this package (no imports).
    Uses __path__ if present (namespace-friendly), else filesystem path.
    """
    modules: List[str] = []

    # Prefer package __path__ (handles namespace packages)
    pkg_paths = None
    try:
        pkg_paths = list(globals().get("__path__", []))  # type: ignore[arg-type]
    except Exception:
        pkg_paths = None

    if pkg_paths:
        for _, name, _ in pkgutil.iter_modules(pkg_paths):
            modules.append(name)
        return modules

    # Fallback to directory path (normal packages)
    package_path = _routes_dir()
    for _, name, _ in pkgutil.iter_modules([str(package_path)]):
        modules.append(name)
    return modules


def _discover_route_modules_in_package() -> List[str]:
    """
    Scan the 'routes' package directory for python modules.
    Returns: ["routes.xxx", ...] (no imports performed).
    Applies safe filtering (skips __init__/__pycache__/private modules).
    """
    out: List[str] = []
    for name in _iter_package_module_names():
        if name in ("__init__", "__pycache__"):
            continue
        if name.startswith("_"):
            # private helpers are not routers by convention
            continue
        out.append(f"routes.{name}")
    return out


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


def _filters() -> Dict[str, List[str]]:
    """
    Optional include/exclude patterns for discovered/expected routes.
    Patterns support:
      - exact module names
      - glob patterns (e.g., routes.*analysis*, routes.enriched_*)
    """
    include = _env_csv("ROUTES_INCLUDE", "")
    exclude = _env_csv("ROUTES_EXCLUDE", "")
    return {"include": include, "exclude": exclude}


def _apply_filters(mods: Sequence[str]) -> List[str]:
    flt = _filters()
    inc = flt["include"]
    exc = flt["exclude"]

    src = list(mods or [])

    # If include list is provided, keep only matches
    if inc:
        src = [m for m in src if _match_any(m, inc)]

    # Exclude always applies
    if exc:
        src = [m for m in src if not _match_any(m, exc)]

    return _dedupe_keep_order(src)


def get_expected_router_modules() -> List[str]:
    """
    Returns a deduped list of:
    - discovered package modules (routes.*)
    - known canonical modules (routes.*)
    - legacy root candidates (best-effort)

    Then applies optional env filters (ROUTES_INCLUDE / ROUTES_EXCLUDE).
    """
    discovered = _discover_route_modules_in_package()

    canonical = [
        "routes.config",
        "routes.enriched_quote",
        "routes.ai_analysis",
        "routes.advanced_analysis",
        "routes.advisor",
        "routes.investment_advisor",
        "routes.routes_argaam",
        "routes.legacy_service",
        # common alternates
        "routes.system",
        "routes.health",
        "routes.status",
    ]

    legacy = _legacy_root_candidates()

    # Keep a stable ordering: discovered first, then canonical, then legacy.
    merged = _dedupe_keep_order(discovered + canonical + legacy)
    return _apply_filters(merged)


def get_available_router_modules(expected: Optional[Sequence[str]] = None) -> List[str]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    return [m for m in exp if module_exists(m)]


# ---------------------------------------------------------------------
# Grouping helpers (for main.py mounting logic)
# ---------------------------------------------------------------------
def _group_of(module_path: str) -> str:
    """
    Classify module into a group for deterministic mounting.
    """
    ml = (module_path or "").lower()

    # Security/auth/middleware-ish
    if any(x in ml for x in ("auth", "security", "middleware", "rate_limit", "ratelimit", "token")):
        return "security"

    # System/config/health/status
    if any(x in ml for x in ("config", "health", "system", "status", "ping", "meta", "version")):
        return "system"

    # Advisor (includes investment_advisor variants)
    if "advisor" in ml:
        return "advisor"

    # KSA-focused (argaam/tadawul)
    if any(x in ml for x in ("argaam", "tadawul", "ksa", "saudi", "sr")):
        return "ksa"

    # Core analytics/quotes/legacy
    if any(x in ml for x in ("enriched", "analysis", "quote", "legacy", "signals", "insights")):
        return "core"

    return "other"


def get_expected_router_groups() -> Dict[str, List[str]]:
    """
    Group modules by intent to help main.py mount in a clean order.
    """
    mods = get_expected_router_modules()

    groups: Dict[str, List[str]] = {
        "security": [],
        "system": [],
        "core": [],
        "advisor": [],
        "ksa": [],
        "other": [],
    }

    for m in mods:
        g = _group_of(m)
        groups[g].append(m)

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
    _probe("security", ["routes.security", "routes.auth", "auth", "security"])
    _probe("config", ["routes.config", "config", "routes.system"])
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

    return discovery


def get_recommended_imports() -> List[Tuple[str, str]]:
    """
    Deterministic list of recommended imports for main.py mounting.
    Return: [(key, module_path), ...]
    """
    d = get_router_discovery()
    out: List[Tuple[str, str]] = []

    # Mount order: Security -> System/Config -> Enriched -> Analysis -> Advisor -> KSA -> Legacy
    priority = ["security", "config", "enriched", "ai_analysis", "advanced_analysis", "advisor", "argaam", "legacy"]

    for key in priority:
        sel = d.get(key, {}).get("selected")
        if isinstance(sel, str) and sel:
            out.append((key, sel))

    return out


def get_mount_plan(expected: Optional[Sequence[str]] = None) -> List[Dict[str, object]]:
    """
    Deterministic plan of what to mount and why.
    No imports performed; only find_spec checks.
    """
    exp = list(expected) if expected is not None else get_expected_router_modules()
    avail = set(get_available_router_modules(exp))

    # Group priority (deterministic)
    group_priority = {"security": 0, "system": 1, "core": 2, "advisor": 3, "ksa": 4, "other": 9}

    plan: List[Dict[str, object]] = []
    for m in exp:
        g = _group_of(m)
        exists = m in avail
        plan.append(
            {
                "module": m,
                "group": g,
                "group_priority": group_priority.get(g, 9),
                "exists": exists,
                "reason": "module available" if exists else "missing (find_spec returned None)",
            }
        )

    # Stable sort: group priority then module path
    plan.sort(key=lambda x: (int(x.get("group_priority", 9)), str(x.get("module", ""))))
    return plan


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
    flt = _filters()

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
        "allow_query_token": _env_bool("ALLOW_QUERY_TOKEN", False) or _env_bool("ENRICHED_ALLOW_QUERY_TOKEN", False),
        # timezone intent (string only; no tz import)
        "tz": _env_str("TZ", "") or _env_str("TIMEZONE", "") or "Asia/Riyadh",
        # filters
        "routes_include": flt["include"],
        "routes_exclude": flt["exclude"],
    }

    return {
        "routes_pkg_version": ROUTES_PACKAGE_VERSION,
        "summary": {
            "expected_count": len(expected),
            "available_count": len(available),
            "missing_count": len(missing),
        },
        "recommended_imports": get_recommended_imports(),
        "mount_plan": get_mount_plan(expected),
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
    "module_exists_any",
    "get_available_router_modules",
    "get_router_discovery",
    "get_recommended_imports",
    "get_mount_plan",
    "get_routes_debug_snapshot",
    "get_dependency_audit",
]
