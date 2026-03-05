#!/usr/bin/env python3
"""
routes/__init__.py
================================================================================
TADAWUL FAST BRIDGE — ROUTER DISCOVERY & MOUNT LOGIC (v2.7.2)
================================================================================
PROD-SAFE • IMPORT-SAFE • RENDER-SAFE • DETERMINISTIC • DIAGNOSTIC-RICH

Why this revision (fixes your current symptom: OpenAPI paths=0, /quotes=404)
- ✅ Provides a deterministic, schema-aligned mount plan (no silent “skip all”)
- ✅ Resolves router modules by trying candidate aliases (first importable wins)
- ✅ Exposes clear diagnostics (missing vs import errors vs mount errors vs no_router)
- ✅ Offers multiple entrypoints for main.py compatibility:
    - mount_all_routers(app)  [preferred]
    - mount_all(app) / mount_routers(app) / mount_routes(app)  [aliases]
    - get_expected_router_modules()
    - get_mount_plan()

Design rules
- No network calls at import-time.
- No heavy imports at import-time.
- Uses importlib.util.find_spec() to check existence cheaply before import.
- Never crashes startup unless ROUTES_STRICT_IMPORT=1.

Env controls (optional)
- ROUTES_STRICT_IMPORT=1        -> raise on mount/import errors
- ROUTES_INCLUDE="a,b,c"        -> only consider these module names (after resolution)
- ROUTES_EXCLUDE="a,b,c"        -> exclude these module names
- ROUTES_LOG_PLAN=1             -> logs resolved plan at mount time
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("routes")

_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


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


# ---------------------------------------------------------------------
# Cheap module existence checks (cached)
# ---------------------------------------------------------------------
_SPEC_CACHE: Dict[str, bool] = {}


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


def _err_to_str(e: BaseException, limit: int = 900) -> str:
    try:
        s = f"{type(e).__name__}: {e}"
    except Exception:
        s = "UnknownError"
    return s if len(s) <= limit else (s[:limit] + "...(truncated)")


# ---------------------------------------------------------------------
# Router catalog (schema-aligned)
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class RouterSpec:
    """
    A logical router "slot" with multiple candidate module names (aliases).
    We resolve to the first candidate that exists.
    """
    key: str
    candidates: Tuple[str, ...]
    required: bool = False


def _default_catalog() -> List[RouterSpec]:
    """
    Canonical mount order (high-level):
    1) config / auth / shared dependencies
    2) quotes/enriched quote endpoints
    3) analysis endpoints (advanced + ai)
    4) advisor endpoints
    5) sheets / sheet-rows endpoints
    6) special meta builders: data_dictionary, top_10
    """
    return [
        RouterSpec(
            key="config",
            candidates=(
                "routes.config",
                "routes.settings",
                "routes.core_config",
            ),
            required=False,
        ),
        RouterSpec(
            key="enriched_quote",
            candidates=(
                "routes.enriched_quote",
                "routes.quotes",
                "routes.quote",
                "routes.market_quotes",
            ),
            required=False,
        ),
        RouterSpec(
            key="advanced_analysis",
            candidates=(
                "routes.advanced_analysis",
                "routes.analysis",
                "routes.advanced",
            ),
            required=False,
        ),
        RouterSpec(
            key="ai_analysis",
            candidates=(
                "routes.ai_analysis",
                "routes.ai",
                "routes.llm_analysis",
            ),
            required=False,
        ),
        RouterSpec(
            key="advisor",
            candidates=(
                "routes.investment_advisor",
                "routes.advisor",
                "routes.recommendations",
            ),
            required=False,
        ),
        RouterSpec(
            key="sheets",
            candidates=(
                "routes.sheets",
                "routes.sheet_rows",
                "routes.sheetrows",
                "routes.sheet_rows_v2",
                "routes.dashboard",
            ),
            required=False,
        ),
        RouterSpec(
            key="data_dictionary",
            candidates=(
                "routes.data_dictionary",
                "routes.dictionary",
                "routes.schema_dictionary",
            ),
            required=False,
        ),
        RouterSpec(
            key="top_10_investments",
            candidates=(
                "routes.top_10_investments",
                "routes.top10",
                "routes.top_investments",
            ),
            required=False,
        ),
    ]


def _resolve_catalog(catalog: List[RouterSpec]) -> Tuple[List[str], Dict[str, str], List[str]]:
    """
    Returns:
      - plan: resolved module names in order
      - resolved_map: key -> module_name (only for found keys)
      - missing_required_keys
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
        else:
            if spec.required:
                missing_required.append(spec.key)

    # Deduplicate while preserving order (in case multiple keys resolve to same module)
    seen = set()
    unique_plan: List[str] = []
    for m in plan:
        if m not in seen:
            seen.add(m)
            unique_plan.append(m)

    return unique_plan, resolved_map, missing_required


# ---------------------------------------------------------------------
# Public helpers expected by main.py
# ---------------------------------------------------------------------
def get_expected_router_modules() -> List[str]:
    """
    Returns a flat list of all candidate modules (used for diagnostics elsewhere).
    """
    mods: List[str] = []
    for spec in _default_catalog():
        mods.extend(list(spec.candidates))
    # dedupe preserve order
    seen = set()
    out: List[str] = []
    for m in mods:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out


def get_mount_plan() -> List[str]:
    """
    Returns the resolved mount plan (first importable candidate per RouterSpec key),
    filtered by ROUTES_INCLUDE / ROUTES_EXCLUDE if set.
    """
    catalog = _default_catalog()
    plan, _, _ = _resolve_catalog(catalog)

    include = set(_parse_csv(_env_str("ROUTES_INCLUDE", "")))
    exclude = set(_parse_csv(_env_str("ROUTES_EXCLUDE", "")))

    if include:
        plan = [m for m in plan if m in include]
    if exclude:
        plan = [m for m in plan if m not in exclude]

    return plan


# ---------------------------------------------------------------------
# Mount execution
# ---------------------------------------------------------------------
def _import_module(module_name: str) -> Tuple[Optional[Any], Optional[BaseException]]:
    try:
        mod = importlib.import_module(module_name)
        return mod, None
    except Exception as e:
        return None, e


def _mount_one(app: Any, module_name: str) -> Tuple[bool, Optional[str]]:
    """
    Mount strategies:
    - module.router -> app.include_router(router)
    - module.mount(app) -> custom mount
    Returns (success, error_message_if_any)
    """
    mod, exc = _import_module(module_name)
    if mod is None:
        return False, _err_to_str(exc or Exception("import failed"))

    router = getattr(mod, "router", None)
    mount_fn = getattr(mod, "mount", None)

    try:
        if router is not None:
            app.include_router(router)
            return True, None
        if callable(mount_fn):
            mount_fn(app)
            return True, None
        return False, "No `router` attribute and no `mount(app)` function found"
    except Exception as e:
        return False, _err_to_str(e)


def mount_all_routers(app: Any) -> Dict[str, Any]:
    """
    Mount all routers on the provided FastAPI app, returning a diagnostic snapshot dict.
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
    missing: List[str] = []
    import_errors: Dict[str, str] = {}
    mount_errors: Dict[str, str] = {}
    no_router: Dict[str, str] = {}

    for module_name in plan:
        # Existence check
        if not _module_exists(module_name):
            missing.append(module_name)
            continue

        ok, err = _mount_one(app, module_name)
        if ok:
            mounted.append(module_name)
            continue

        # differentiate: import error vs mount error vs no_router (best effort)
        if err and ("import" in err.lower() or "modulenotfounderror" in err.lower()):
            import_errors[module_name] = err
        elif err and ("no `router`" in err.lower() or "no router" in err.lower()):
            no_router[module_name] = err
        else:
            mount_errors[module_name] = err or "unknown error"

        if strict:
            raise RuntimeError(f"Router mount failed for {module_name}: {err}")

    snap = {
        "mounted": mounted,
        "missing": missing,
        "import_errors": import_errors,
        "mount_errors": mount_errors,
        "no_router": no_router,
        "strict": strict,
        "strategy": "routes.mount_all_routers",
        "resolved_map": resolved_map,
        "missing_required_keys": missing_required,
        "plan": plan,
    }

    failed_count = len(import_errors) + len(mount_errors) + len(no_router)
    logger.info(
        "Routes mount summary: mounted=%s missing=%s failed=%s strict=%s",
        len(mounted),
        len(missing),
        failed_count,
        strict,
    )

    if missing_required:
        logger.warning("Missing REQUIRED router keys (none of their candidates exist): %s", ", ".join(missing_required))
        if strict:
            raise RuntimeError(f"Missing required routers: {', '.join(missing_required)}")

    if missing:
        logger.warning("Missing router modules (plan resolved but spec now missing): %s", ", ".join(missing))
    if import_errors:
        logger.warning("Router import errors: %s", json.dumps(import_errors, ensure_ascii=False))
    if mount_errors:
        logger.warning("Router mount errors: %s", json.dumps(mount_errors, ensure_ascii=False))
    if no_router:
        logger.warning("Routers with no router/mount: %s", json.dumps(no_router, ensure_ascii=False))

    return snap


# Backwards-compatible aliases (main.py tries these names)
def mount_all(app: Any) -> Dict[str, Any]:
    return mount_all_routers(app)


def mount_routers(app: Any) -> Dict[str, Any]:
    return mount_all_routers(app)


def mount_routes(app: Any) -> Dict[str, Any]:
    return mount_all_routers(app)
