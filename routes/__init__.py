#!/usr/bin/env python3
# routes/__init__.py
"""
================================================================================
TADAWUL FAST BRIDGE — ROUTER DISCOVERY & MOUNT LOGIC (v3.2.0)
================================================================================
PROD-SAFE • IMPORT-SAFE • RENDER-SAFE • DETERMINISTIC • DUPLICATE-AWARE
MAIN-PY-COMPATIBLE • DIAGNOSTIC-RICH • REPO-ALIGNED • PRIORITY-SAFE

Why this revision
-----------------
- ✅ FIX: Reorders mount priority so authoritative schema router loads BEFORE
        generic sheet-row helpers and can win duplicate path ownership
- ✅ FIX: Keeps your real repo router module names and preferred mount order
- ✅ FIX: Supports include/exclude by logical key OR exact module name
- ✅ FIX: Avoids duplicate inclusion by route-signature comparison
- ✅ FIX: Supports module exports in any of these forms:
      - router
      - get_router()
      - build_router()
      - create_router()
      - mount(app)
- ✅ FIX: Exposes richer snapshot fields for debugging route exposure problems
- ✅ FIX: Adds duplicate-details and overlap path diagnostics
- ✅ FIX: Best-effort safe behavior with optional strict mode
- ✅ COMPAT: Preserves aliases expected by main.py:
      - mount_all_routers(app)  [preferred]
      - mount_all(app)
      - mount_routers(app)
      - mount_routes(app)
      - get_expected_router_modules()
      - get_mount_plan()

Optional env controls
---------------------
- ROUTES_STRICT_IMPORT=1      -> raise on mount/import/required errors
- ROUTES_INCLUDE="a,b,c"      -> include logical keys and/or module names only
- ROUTES_EXCLUDE="a,b,c"      -> exclude logical keys and/or module names
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


def _err_to_str(e: BaseException, limit: int = 2000) -> str:
    try:
        s = f"{type(e).__name__}: {e}"
    except Exception:
        s = "UnknownError"
    return s if len(s) <= limit else (s[:limit] + "...(truncated)")


def _norm_name(value: str) -> str:
    return str(value or "").strip().lower()


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

    IMPORTANT:
    - data_dictionary MUST mount early so /v1/schema/* endpoints are owned by the
      authoritative schema router before generic sheet/helper routers are loaded.
    - enriched_quote should also mount early because it is a primary sheet-rows family.
    - generic helper routers come later and should lose duplicates safely.
    """
    return [
        RouterSpec(
            key="config",
            candidates=("routes.config",),
            required=False,
        ),
        RouterSpec(
            key="enriched_quote",
            candidates=("routes.enriched_quote",),
            required=False,
        ),
        RouterSpec(
            key="data_dictionary",
            candidates=("routes.data_dictionary",),
            required=False,
        ),
        RouterSpec(
            key="investment_advisor",
            candidates=("routes.investment_advisor",),
            required=False,
        ),
        RouterSpec(
            key="advanced_analysis",
            candidates=("routes.advanced_analysis",),
            required=False,
        ),
        RouterSpec(
            key="ai_analysis",
            candidates=("routes.ai_analysis",),
            required=False,
        ),
        RouterSpec(
            key="advisor",
            candidates=("routes.advisor",),
            required=False,
        ),
        RouterSpec(
            key="analysis_sheet_rows",
            candidates=("routes.analysis_sheet_rows",),
            required=False,
        ),
        RouterSpec(
            key="advanced_sheet_rows",
            candidates=("routes.advanced_sheet_rows",),
            required=False,
        ),
        RouterSpec(
            key="top10_investments",
            candidates=("routes.top10_investments",),
            required=False,
        ),
        RouterSpec(
            key="routes_argaam",
            candidates=("routes.routes_argaam",),
            required=False,
        ),
    ]


def _resolve_catalog(
    catalog: Sequence[RouterSpec],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Returns:
      - resolved entries:
          [
            {
              "key": logical_key,
              "module": chosen_module_or_None,
              "required": bool,
              "all_candidates": [...],
              "existing_candidates": [...],
            }
          ]
      - missing_required_keys
    """
    entries: List[Dict[str, Any]] = []
    missing_required: List[str] = []

    for spec in catalog:
        existing = [m for m in spec.candidates if _module_exists(m)]
        chosen = existing[0] if existing else None

        if spec.required and not chosen:
            missing_required.append(spec.key)

        entries.append(
            {
                "key": spec.key,
                "module": chosen,
                "required": spec.required,
                "all_candidates": list(spec.candidates),
                "existing_candidates": existing,
            }
        )

    return entries, missing_required


# ======================================================================================
# Public plan helpers
# ======================================================================================
def get_expected_router_modules() -> List[str]:
    modules: List[str] = []
    seen: Set[str] = set()

    for spec in _default_catalog():
        for mod_name in spec.candidates:
            if mod_name not in seen:
                seen.add(mod_name)
                modules.append(mod_name)

    return modules


def _filter_resolved_entries(entries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    include_raw = _parse_csv(_env_str("ROUTES_INCLUDE", ""))
    exclude_raw = _parse_csv(_env_str("ROUTES_EXCLUDE", ""))

    include = {_norm_name(x) for x in include_raw}
    exclude = {_norm_name(x) for x in exclude_raw}

    filtered: List[Dict[str, Any]] = []
    for entry in entries:
        key = _norm_name(str(entry.get("key") or ""))
        module = _norm_name(str(entry.get("module") or ""))

        if include:
            if key not in include and module not in include:
                continue

        if exclude:
            if key in exclude or module in exclude:
                continue

        filtered.append(dict(entry))

    return filtered


def get_mount_plan() -> List[str]:
    resolved, _missing_required = _resolve_catalog(_default_catalog())
    resolved = _filter_resolved_entries(resolved)
    plan = [str(x["module"]) for x in resolved if x.get("module")]
    seen: Set[str] = set()
    out: List[str] = []
    for m in plan:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out


# ======================================================================================
# Import / router extraction helpers
# ======================================================================================
def _import_module(module_name: str) -> Tuple[Optional[Any], Optional[BaseException]]:
    try:
        return importlib.import_module(module_name), None
    except Exception as e:
        return None, e


def _get_router_from_module(mod: Any) -> Tuple[Optional[Any], str]:
    """
    Supported patterns:
    - module.router
    - module.get_router()
    - module.build_router()
    - module.create_router()
    """
    router = getattr(mod, "router", None)
    if router is not None:
        return router, "router_attr"

    for fn_name in ("get_router", "build_router", "create_router"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            try:
                out = fn()
                if out is not None:
                    return out, fn_name
            except Exception:
                continue

    return None, "none"


def _router_signature(router: Any) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
    """
    Stable-ish router fingerprint:
    (prefix, ((path, methods_csv), ...))
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
    sigs: Set[Tuple[str, str]] = set()
    for r in getattr(app, "routes", []) or []:
        path = str(getattr(r, "path", "") or "")
        methods = getattr(r, "methods", None) or set()
        if not isinstance(methods, (set, list, tuple)):
            methods = set()
        meth = ",".join(sorted(str(m) for m in methods))
        sigs.add((path, meth))
    return sigs


def _router_route_signature_set(router: Any) -> Set[Tuple[str, str]]:
    sigs: Set[Tuple[str, str]] = set()
    for r in getattr(router, "routes", []) or []:
        path = str(getattr(r, "path", "") or "")
        methods = getattr(r, "methods", None) or set()
        if not isinstance(methods, (set, list, tuple)):
            methods = set()
        meth = ",".join(sorted(str(m) for m in methods))
        sigs.add((path, meth))
    return sigs


def _router_would_duplicate_existing(app: Any, router: Any) -> bool:
    router_sigs = _router_route_signature_set(router)
    if not router_sigs:
        return False
    app_sigs = _app_route_signature_set(app)
    return router_sigs.issubset(app_sigs)


def _route_overlap_with_app(app: Any, router: Any) -> List[Tuple[str, str]]:
    router_sigs = _router_route_signature_set(router)
    if not router_sigs:
        return []
    app_sigs = _app_route_signature_set(app)
    overlap = sorted(router_sigs.intersection(app_sigs))
    return overlap


def _get_or_init_snapshot_store(app: Any) -> Dict[str, Any]:
    snap = {
        "mounted_modules": [],
        "duplicate_skips": [],
        "duplicate_details": {},
        "missing_modules": [],
        "import_errors": {},
        "mount_errors": {},
        "no_router": {},
        "mount_modes": {},
        "module_to_key": {},
        "router_signatures": {},
    }
    try:
        if hasattr(app, "state"):
            existing = getattr(app.state, "routes_snapshot", None)
            if isinstance(existing, dict):
                for k, v in existing.items():
                    snap[k] = v
    except Exception:
        pass
    return snap


# ======================================================================================
# Mount helpers
# ======================================================================================
def _mount_one(
    app: Any,
    module_name: str,
) -> Tuple[bool, Optional[str], str, Optional[Dict[str, Any]]]:
    """
    Mount strategies:
    - module.router -> app.include_router(router)
    - module.get_router()/build_router()/create_router()
    - module.mount(app)

    Returns:
      (success, error_message, mode, details)

    mode in:
      - "router_attr"
      - "get_router"
      - "build_router"
      - "create_router"
      - "mount_fn"
      - "duplicate_skip"
      - "import_error"
      - "no_router"
      - "mount_error"
    """
    mod, exc = _import_module(module_name)
    if mod is None:
        return False, _err_to_str(exc or Exception("import failed")), "import_error", None

    router, source = _get_router_from_module(mod)
    mount_fn = getattr(mod, "mount", None)

    try:
        if router is not None:
            if _router_would_duplicate_existing(app, router):
                overlap = _route_overlap_with_app(app, router)
                return True, None, "duplicate_skip", {
                    "overlap_count": len(overlap),
                    "overlap_paths": overlap[:50],
                    "router_prefix": str(getattr(router, "prefix", "") or ""),
                    "router_route_count": len(getattr(router, "routes", []) or []),
                }
            app.include_router(router)
            return True, None, source, {
                "router_prefix": str(getattr(router, "prefix", "") or ""),
                "router_route_count": len(getattr(router, "routes", []) or []),
            }

        if callable(mount_fn):
            before = _app_route_signature_set(app)
            mount_fn(app)
            after = _app_route_signature_set(app)
            added = sorted(after - before)
            return True, None, "mount_fn", {
                "added_count": len(added),
                "added_paths": added[:50],
            }

        return False, "No router export and no mount(app) function found", "no_router", None

    except Exception as e:
        return False, _err_to_str(e), "mount_error", None


# ======================================================================================
# Main mount entrypoint
# ======================================================================================
def mount_all_routers(app: Any) -> Dict[str, Any]:
    """
    Mount all routers on the provided FastAPI app and return a diagnostic snapshot.

    Does not raise unless ROUTES_STRICT_IMPORT=1.
    """
    strict = _env_bool("ROUTES_STRICT_IMPORT", False)
    log_plan = _env_bool("ROUTES_LOG_PLAN", False)

    catalog = _default_catalog()
    resolved_entries, missing_required = _resolve_catalog(catalog)
    resolved_entries = _filter_resolved_entries(resolved_entries)

    if log_plan:
        logger.info(
            "Resolved router plan: %s",
            ", ".join(
                f"{x['key']}=>{x['module']}" for x in resolved_entries if x.get("module")
            ) or "(empty)",
        )

    snap = _get_or_init_snapshot_store(app)

    mounted: List[str] = []
    duplicate_skips: List[str] = []
    duplicate_details: Dict[str, Any] = {}
    missing: List[str] = []
    import_errors: Dict[str, str] = {}
    mount_errors: Dict[str, str] = {}
    no_router: Dict[str, str] = {}
    mount_modes: Dict[str, str] = {}
    module_to_key: Dict[str, str] = {}
    resolved_map: Dict[str, str] = {}

    plan_modules: List[str] = []

    for entry in resolved_entries:
        key = str(entry.get("key") or "")
        module_name = entry.get("module")
        if module_name:
            resolved_map[key] = str(module_name)
            plan_modules.append(str(module_name))

    seen_plan: Set[str] = set()
    plan_modules_unique: List[str] = []
    for m in plan_modules:
        if m not in seen_plan:
            seen_plan.add(m)
            plan_modules_unique.append(m)

    for entry in resolved_entries:
        key = str(entry.get("key") or "")
        module_name = entry.get("module")

        if not module_name:
            all_candidates = entry.get("all_candidates") or []
            existing_candidates = entry.get("existing_candidates") or []
            if all_candidates and not existing_candidates:
                continue
            continue

        module_name = str(module_name)
        module_to_key[module_name] = key

        if not _module_exists(module_name):
            missing.append(module_name)
            continue

        ok, err, mode, details = _mount_one(app, module_name)

        if ok:
            if mode == "duplicate_skip":
                duplicate_skips.append(module_name)
                if details:
                    duplicate_details[module_name] = details
            else:
                mounted.append(module_name)

            mount_modes[module_name] = mode

            try:
                mod = importlib.import_module(module_name)
                router, _source = _get_router_from_module(mod)
                if router is not None:
                    snap.setdefault("router_signatures", {})[module_name] = {
                        "signature": _router_signature(router),
                        "route_count": len(getattr(router, "routes", []) or []),
                        "prefix": str(getattr(router, "prefix", "") or ""),
                    }
            except Exception:
                pass

            continue

        if mode == "import_error" or (err and "modulenotfounderror" in err.lower()):
            import_errors[module_name] = err or "import failed"
        elif mode == "no_router":
            no_router[module_name] = err or "no router/mount"
        else:
            mount_errors[module_name] = err or "unknown mount error"

        if strict:
            raise RuntimeError(f"Router mount failed for {module_name}: {err}")

    failed_count = len(import_errors) + len(mount_errors) + len(no_router)

    snapshot = {
        "mounted": mounted,
        "mounted_count": len(mounted),
        "duplicate_skips": duplicate_skips,
        "duplicate_skips_count": len(duplicate_skips),
        "duplicate_details": duplicate_details,
        "missing": missing,
        "missing_count": len(missing),
        "import_errors": import_errors,
        "mount_errors": mount_errors,
        "no_router": no_router,
        "failed_count": failed_count,
        "strict": strict,
        "strategy": "routes.mount_all_routers",
        "resolved_map": resolved_map,
        "module_to_key": module_to_key,
        "missing_required_keys": missing_required,
        "plan": plan_modules_unique,
        "plan_count": len(plan_modules_unique),
        "mount_modes": mount_modes,
        "expected_router_modules": get_expected_router_modules(),
        "expected_router_modules_count": len(get_expected_router_modules()),
        "catalog_keys": [spec.key for spec in catalog],
        "resolved_entries": resolved_entries,
        "openapi_route_count_after_mount": len(getattr(app, "routes", []) or []),
    }

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
        try:
            logger.info("Duplicate router details: %s", json.dumps(duplicate_details, ensure_ascii=False, default=str))
        except Exception:
            pass

    try:
        if hasattr(app, "state"):
            app.state.routes_snapshot = snapshot
            app.state.routes_mount_result = snapshot
            app.state.routes_strategy = "routes.mount_all_routers"
    except Exception:
        pass

    return snapshot


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
