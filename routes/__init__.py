#!/usr/bin/env python3
# routes/__init__.py
"""
================================================================================
TADAWUL FAST BRIDGE — ROUTER DISCOVERY & MOUNT LOGIC (v3.0.0)
================================================================================
PROD-SAFE • IMPORT-SAFE • RENDER-SAFE • DETERMINISTIC • DUPLICATE-AWARE

Why this revision
-----------------
- ✅ FIX: Uses your REAL repo router module names
- ✅ FIX: Deterministic mount order aligned to your current route structure
- ✅ FIX: Avoids accidental duplicate router inclusion
- ✅ FIX: Exposes richer diagnostics for mounted/missing/import/mount/duplicate cases
- ✅ FIX: Supports main.py compatibility aliases:
      - mount_all_routers(app)  [preferred]
      - mount_all(app)
      - mount_routers(app)
      - mount_routes(app)
      - get_expected_router_modules()
      - get_mount_plan()

Main design goals
-----------------
1) Mount each logical router once only
2) Prefer actual repo files over guessed aliases
3) Keep startup safe (no network / provider calls at import-time)
4) Make debugging route exposure easy
5) Avoid silent OpenAPI gaps caused by skipped imports or duplicate inclusion

Optional env controls
---------------------
- ROUTES_STRICT_IMPORT=1      -> raise on mount/import/required errors
- ROUTES_INCLUDE="a,b,c"      -> only include these exact module names
- ROUTES_EXCLUDE="a,b,c"      -> exclude these exact module names
- ROUTES_LOG_PLAN=1           -> log resolved plan before mounting
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger("routes")

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}

_SPEC_CACHE: Dict[str, bool] = {}


# ======================================================================================
# Env helpers
# ======================================================================================
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env_str(name, "")
    if raw == "":
        return bool(default)
    s = raw.strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return bool(default)


def _parse_csv(value: str) -> List[str]:
    s = (value or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _err_to_str(e: BaseException, limit: int = 1200) -> str:
    try:
        s = f"{type(e).__name__}: {e}"
    except Exception:
        s = "UnknownError"
    return s if len(s) <= limit else (s[:limit] + "...(truncated)")


# ======================================================================================
# Cheap module existence checks
# ======================================================================================
def _module_exists(module_name: str) -> bool:
    hit = _SPEC_CACHE.get(module_name)
    if hit is not None:
        return bool(hit)
    try:
        spec = importlib.util.find_spec(module_name)
        ok = spec is not None
    except Exception:
        ok = False
    _SPEC_CACHE[module_name] = ok
    return ok


# ======================================================================================
# Router catalog
# ======================================================================================
@dataclass(frozen=True)
class RouterSpec:
    """
    One logical router slot with candidate module names.
    The first importable candidate wins.
    """
    key: str
    candidates: Tuple[str, ...]
    required: bool = False


def _default_catalog() -> List[RouterSpec]:
    """
    Real repo-aligned mount order.

    Order rationale:
    1) config / simple shared routes
    2) quote endpoints
    3) analysis endpoints
    4) advisor endpoints
    5) sheet-row endpoints
    6) schema/meta endpoints
    7) special selectors / extras
    """
    return [
        RouterSpec(
            key="config",
            candidates=(
                "routes.config",
            ),
            required=False,
        ),
        RouterSpec(
            key="enriched_quote",
            candidates=(
                "routes.enriched_quote",
            ),
            required=False,
        ),
        RouterSpec(
            key="advanced_analysis",
            candidates=(
                "routes.advanced_analysis",
            ),
            required=False,
        ),
        RouterSpec(
            key="ai_analysis",
            candidates=(
                "routes.ai_analysis",
            ),
            required=False,
        ),
        RouterSpec(
            key="advisor",
            candidates=(
                "routes.advisor",
            ),
            required=False,
        ),
        RouterSpec(
            key="investment_advisor",
            candidates=(
                "routes.investment_advisor",
            ),
            required=False,
        ),
        RouterSpec(
            key="analysis_sheet_rows",
            candidates=(
                "routes.analysis_sheet_rows",
            ),
            required=False,
        ),
        RouterSpec(
            key="advanced_sheet_rows",
            candidates=(
                "routes.advanced_sheet_rows",
            ),
            required=False,
        ),
        RouterSpec(
            key="data_dictionary",
            candidates=(
                "routes.data_dictionary",
            ),
            required=False,
        ),
        RouterSpec(
            key="top10_investments",
            candidates=(
                "routes.top10_investments",
            ),
            required=False,
        ),
        RouterSpec(
            key="routes_argaam",
            candidates=(
                "routes.routes_argaam",
            ),
            required=False,
        ),
    ]


def _resolve_catalog(catalog: Sequence[RouterSpec]) -> Tuple[List[str], Dict[str, str], List[str]]:
    """
    Returns:
      - plan: resolved module names in order
      - resolved_map: logical key -> chosen module
      - missing_required_keys: required logical keys with no importable candidate
    """
    plan: List[str] = []
    resolved_map: Dict[str, str] = {}
    missing_required: List[str] = []

    for spec in catalog:
        chosen: Optional[str] = None
        for mod_name in spec.candidates:
            if _module_exists(mod_name):
                chosen = mod_name
                break

        if chosen:
            plan.append(chosen)
            resolved_map[spec.key] = chosen
        elif spec.required:
            missing_required.append(spec.key)

    # de-duplicate preserve order
    seen: Set[str] = set()
    unique_plan: List[str] = []
    for mod_name in plan:
        if mod_name not in seen:
            seen.add(mod_name)
            unique_plan.append(mod_name)

    return unique_plan, resolved_map, missing_required


# ======================================================================================
# Public plan helpers
# ======================================================================================
def get_expected_router_modules() -> List[str]:
    """
    Flat list of all candidate router modules in the catalog.
    """
    modules: List[str] = []
    seen: Set[str] = set()

    for spec in _default_catalog():
        for mod_name in spec.candidates:
            if mod_name not in seen:
                seen.add(mod_name)
                modules.append(mod_name)

    return modules


def get_mount_plan() -> List[str]:
    """
    Resolved router plan after include/exclude filters.
    """
    plan, _, _ = _resolve_catalog(_default_catalog())

    include = set(_parse_csv(_env_str("ROUTES_INCLUDE", "")))
    exclude = set(_parse_csv(_env_str("ROUTES_EXCLUDE", "")))

    if include:
        plan = [m for m in plan if m in include]
    if exclude:
        plan = [m for m in plan if m not in exclude]

    return plan


# ======================================================================================
# Import / mount helpers
# ======================================================================================
def _import_module(module_name: str) -> Tuple[Optional[Any], Optional[BaseException]]:
    try:
        return importlib.import_module(module_name), None
    except Exception as e:
        return None, e


def _router_signature(router: Any) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
    """
    Produce a stable-ish signature for duplicate detection.
    Uses prefix + (path, methods) tuples if available.
    """
    prefix = str(getattr(router, "prefix", "") or "")
    routes = getattr(router, "routes", []) or []

    items: List[Tuple[str, str]] = []
    for r in routes:
        path = str(getattr(r, "path", "") or "")
        methods = getattr(r, "methods", None) or set()
        if not isinstance(methods, (set, list, tuple)):
            methods = set()
        meth = ",".join(sorted(str(m) for m in methods))
        items.append((path, meth))

    return prefix, tuple(items)


def _app_route_signature_set(app: Any) -> Set[Tuple[str, str]]:
    """
    Existing app route signatures as (path, sorted_methods_csv)
    """
    sigs: Set[Tuple[str, str]] = set()
    for r in getattr(app, "routes", []) or []:
        path = str(getattr(r, "path", "") or "")
        methods = getattr(r, "methods", None) or set()
        if not isinstance(methods, (set, list, tuple)):
            methods = set()
        meth = ",".join(sorted(str(m) for m in methods))
        sigs.add((path, meth))
    return sigs


def _router_would_duplicate_existing(app: Any, router: Any) -> bool:
    """
    Conservative duplicate check:
    if every route signature in router already exists on app, treat as duplicate.
    """
    router_routes = getattr(router, "routes", []) or []
    if not router_routes:
        return False

    app_sigs = _app_route_signature_set(app)
    router_sigs: Set[Tuple[str, str]] = set()

    for r in router_routes:
        path = str(getattr(r, "path", "") or "")
        methods = getattr(r, "methods", None) or set()
        if not isinstance(methods, (set, list, tuple)):
            methods = set()
        meth = ",".join(sorted(str(m) for m in methods))
        router_sigs.add((path, meth))

    return bool(router_sigs) and router_sigs.issubset(app_sigs)


def _mount_one(app: Any, module_name: str) -> Tuple[bool, Optional[str], str]:
    """
    Mount strategies:
    - module.router -> app.include_router(router)
    - module.mount(app) -> custom mount

    Returns:
      (success, error_message, mode)
      mode in {"router", "mount_fn", "duplicate_skip", "no_router"}
    """
    mod, exc = _import_module(module_name)
    if mod is None:
        return False, _err_to_str(exc or Exception("import failed")), "import_error"

    router = getattr(mod, "router", None)
    mount_fn = getattr(mod, "mount", None)

    try:
        if router is not None:
            if _router_would_duplicate_existing(app, router):
                return True, None, "duplicate_skip"
            app.include_router(router)
            return True, None, "router"

        if callable(mount_fn):
            # We cannot cheaply pre-detect duplicates for custom mount functions,
            # so just call it and trust the module.
            mount_fn(app)
            return True, None, "mount_fn"

        return False, "No `router` attribute and no `mount(app)` function found", "no_router"

    except Exception as e:
        return False, _err_to_str(e), "mount_error"


# ======================================================================================
# Main mount entrypoint
# ======================================================================================
def mount_all_routers(app: Any) -> Dict[str, Any]:
    """
    Mount all routers on the provided FastAPI app and return a diagnostic snapshot.

    Never raises unless ROUTES_STRICT_IMPORT=1.
    """
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)
    log_plan = _env_bool("ROUTES_LOG_PLAN", False)

    catalog = _default_catalog()
    plan, resolved_map, missing_required = _resolve_catalog(catalog)

    include = set(_parse_csv(_env_str("ROUTES_INCLUDE", "")))
    exclude = set(_parse_csv(_env_str("ROUTES_EXCLUDE", "")))

    if include:
        plan = [m for m in plan if m in include]
    if exclude:
        plan = [m for m in plan if m not in exclude]

    if log_plan:
        logger.info("Resolved router plan: %s", ", ".join(plan) if plan else "(empty)")

    mounted: List[str] = []
    duplicate_skips: List[str] = []
    missing: List[str] = []
    import_errors: Dict[str, str] = {}
    mount_errors: Dict[str, str] = {}
    no_router: Dict[str, str] = {}
    mount_modes: Dict[str, str] = {}

    for module_name in plan:
        if not _module_exists(module_name):
            missing.append(module_name)
            continue

        ok, err, mode = _mount_one(app, module_name)

        if ok:
            if mode == "duplicate_skip":
                duplicate_skips.append(module_name)
            else:
                mounted.append(module_name)
            mount_modes[module_name] = mode
            continue

        if mode == "import_error" or (err and "modulenotfounderror" in err.lower()):
            import_errors[module_name] = err or "import failed"
        elif mode == "no_router":
            no_router[module_name] = err or "no router/mount"
        else:
            mount_errors[module_name] = err or "unknown mount error"

        if strict:
            raise RuntimeError(f"Router mount failed for {module_name}: {err}")

    snap = {
        "mounted": mounted,
        "duplicate_skips": duplicate_skips,
        "missing": missing,
        "import_errors": import_errors,
        "mount_errors": mount_errors,
        "no_router": no_router,
        "strict": strict,
        "strategy": "routes.mount_all_routers",
        "resolved_map": resolved_map,
        "missing_required_keys": missing_required,
        "plan": plan,
        "mount_modes": mount_modes,
        "expected_router_modules": get_expected_router_modules(),
        "openapi_route_count_after_mount": len(getattr(app, "routes", []) or []),
    }

    failed_count = len(import_errors) + len(mount_errors) + len(no_router)

    logger.info(
        "Routes mount summary: mounted=%s duplicate_skips=%s missing=%s failed=%s strict=%s",
        len(mounted),
        len(duplicate_skips),
        len(missing),
        failed_count,
        strict,
    )

    if missing_required:
        logger.warning(
            "Missing REQUIRED router keys (none of their candidates exist): %s",
            ", ".join(missing_required),
        )
        if strict:
            raise RuntimeError(f"Missing required routers: {', '.join(missing_required)}")

    if missing:
        logger.warning("Missing router modules: %s", ", ".join(missing))
    if import_errors:
        logger.warning("Router import errors: %s", json.dumps(import_errors, ensure_ascii=False))
    if mount_errors:
        logger.warning("Router mount errors: %s", json.dumps(mount_errors, ensure_ascii=False))
    if no_router:
        logger.warning("Routers with no router/mount: %s", json.dumps(no_router, ensure_ascii=False))
    if duplicate_skips:
        logger.info("Duplicate router skips: %s", ", ".join(duplicate_skips))

    # Best effort: expose snapshot on app.state if available
    try:
        if hasattr(app, "state"):
            app.state.routes_snapshot = snap
    except Exception:
        pass

    return snap


# ======================================================================================
# Backward-compatible aliases
# ======================================================================================
def mount_all(app: Any) -> Dict[str, Any]:
    return mount_all_routers(app)


def mount_routers(app: Any) -> Dict[str, Any]:
    return mount_all_routers(app)


def mount_routes(app: Any) -> Dict[str, Any]:
    return mount_all_routers(app)


__all__ = [
    "get_expected_router_modules",
    "get_mount_plan",
    "mount_all_routers",
    "mount_all",
    "mount_routers",
    "mount_routes",
]
