#!/usr/bin/env python3
# routes/__init__.py
"""
================================================================================
TADAWUL FAST BRIDGE — ROUTER DISCOVERY & MOUNT LOGIC (v3.6.0)
================================================================================
MAIN-PY-ALIGNED • IMPORT-SAFE • RENDER-SAFE • DETERMINISTIC • DUPLICATE-AWARE
CONTROLLED-OWNER SAFE • ROUTE-LEVEL FILTER SAFE • PREFIX-OWNER ENFORCED
PACKAGE-MOUNT COMPATIBLE • CONFLICT-ROLLBACK SAFE • SNAPSHOT-RICH

Why this revision
-----------------
- ✅ ALIGN: canonical owner map stays aligned with the controlled runtime policy
        used by main.py.
- ✅ FIX: mount(app) additions are now filtered for disallowed AND duplicate routes
        at route level, instead of only rolling back the all-duplicate case.
- ✅ FIX: snapshot now records partial duplicate skips and count fields explicitly.
- ✅ FIX: route-level append diagnostics track the exact route objects added,
        improving overlap and route-count reporting.
- ✅ FIX: package-level policy blocks now also fence root/advanced sheet-row
        reclaim attempts more consistently for data-dictionary style modules.
- ✅ FIX: route/debug snapshots include route_signature_count_after_mount to align
        better with main.py diagnostics.

Optional env controls
---------------------
- ROUTES_STRICT_IMPORT=1              -> raise on mount/import/required errors
- ROUTES_INCLUDE="a,b,c"              -> include logical keys and/or module names only
- ROUTES_EXCLUDE="a,b,c"              -> exclude logical keys and/or module names
- ROUTES_LOG_PLAN=1                   -> log resolved plan before mounting
- ROUTES_PROTECTED_PREFIXES="a,b,c"   -> protected path prefixes
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

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


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in items:
        s = str(item or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _err_to_str(e: BaseException, limit: int = 2000) -> str:
    try:
        s = f"{type(e).__name__}: {e}"
    except Exception:
        s = "UnknownError"
    return s if len(s) <= limit else (s[:limit] + "...(truncated)")


def _norm_name(value: str) -> str:
    return str(value or "").strip().lower()


def _protected_prefixes() -> List[str]:
    raw = _parse_csv(
        _env_str(
            "ROUTES_PROTECTED_PREFIXES",
            "/v1/advisor,/v1/advanced,/v1/analysis,/v1/schema,/schema,/v1/enriched,/v1/enriched_quote,/v1/enriched-quote,/quote,/quotes,/sheet-rows",
        )
    )
    return _dedupe_keep_order(raw)


def _canonical_owner_map() -> Dict[str, str]:
    """
    Canonical logical owner per protected prefix.
    The value is the logical RouterSpec.key, not the module name.
    """
    return {
        "/v1/advisor": "advisor",
        "/v1/advanced": "investment_advisor",
        "/v1/analysis": "analysis_sheet_rows",
        "/v1/schema": "advanced_analysis",
        "/schema": "advanced_analysis",
        "/v1/enriched": "enriched_quote",
        "/v1/enriched_quote": "enriched_quote",
        "/v1/enriched-quote": "enriched_quote",
        "/quote": "enriched_quote",
        "/quotes": "enriched_quote",
        "/sheet-rows": "advanced_analysis",
    }


def _module_route_policies() -> Dict[str, Dict[str, Sequence[str]]]:
    return {
        "routes.data_dictionary": {
            "block_prefixes": ("/v1/schema", "/schema"),
            "block_exact": ("/sheet-rows", "/v1/advanced/sheet-rows"),
        },
        "routes.analysis_sheet_rows": {
            "allow_prefixes": ("/v1/analysis",),
        },
        "routes.advisor": {
            "allow_prefixes": ("/v1/advisor",),
        },
        "routes.investment_advisor": {
            "allow_prefixes": ("/v1/advanced", "/v1/investment_advisor", "/v1/investment-advisor"),
            "block_prefixes": ("/v1/advisor",),
        },
        "routes.advanced_analysis": {
            "allow_prefixes": ("/v1/schema", "/schema"),
            "allow_exact": (
                "/sheet-rows",
                "/v1/advanced/insights-analysis",
                "/v1/advanced/top10-investments",
                "/v1/advanced/insights-criteria",
            ),
        },
        "routes.enriched_quote": {
            "allow_prefixes": ("/v1/enriched", "/v1/enriched_quote", "/v1/enriched-quote"),
            "allow_exact": ("/quote", "/quotes"),
            "block_exact": ("/sheet-rows",),
        },
    }


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
    priority: int = 100


def _default_catalog() -> List[RouterSpec]:
    """
    Package-level mount order aligned to the controlled runtime mount plan in main.py.
    """
    return [
        RouterSpec("config", ("routes.config",), required=False, priority=10),
        RouterSpec("data_dictionary", ("routes.data_dictionary",), required=False, priority=20),
        RouterSpec("analysis_sheet_rows", ("routes.analysis_sheet_rows",), required=False, priority=30),
        RouterSpec("advanced_analysis", ("routes.advanced_analysis",), required=False, priority=40),
        RouterSpec("ai_analysis", ("routes.ai_analysis",), required=False, priority=50),
        RouterSpec("advisor", ("routes.advisor",), required=False, priority=60),
        RouterSpec("investment_advisor", ("routes.investment_advisor",), required=False, priority=70),
        RouterSpec("enriched_quote", ("routes.enriched_quote",), required=False, priority=80),
        RouterSpec("routes_argaam", ("routes.routes_argaam",), required=False, priority=90),
    ]


def _resolve_catalog(catalog: Sequence[RouterSpec]) -> Tuple[List[Dict[str, Any]], List[str]]:
    entries: List[Dict[str, Any]] = []
    missing_required: List[str] = []

    ordered_specs = sorted(catalog, key=lambda s: (int(s.priority), s.key))

    for spec in ordered_specs:
        existing = [m for m in spec.candidates if _module_exists(m)]
        chosen = existing[0] if existing else None

        if spec.required and not chosen:
            missing_required.append(spec.key)

        entries.append(
            {
                "key": spec.key,
                "module": chosen,
                "required": spec.required,
                "priority": int(spec.priority),
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

    for spec in sorted(_default_catalog(), key=lambda s: (int(s.priority), s.key)):
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

        if include and key not in include and module not in include:
            continue
        if exclude and (key in exclude or module in exclude):
            continue

        filtered.append(dict(entry))

    filtered.sort(key=lambda x: (int(x.get("priority", 100)), str(x.get("key") or "")))
    return filtered


def get_mount_plan() -> List[str]:
    resolved, _missing_required = _resolve_catalog(_default_catalog())
    resolved = _filter_resolved_entries(resolved)
    return _dedupe_keep_order([str(x["module"]) for x in resolved if x.get("module")])


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


def _route_signature_set_from_routes(routes: Sequence[Any]) -> Set[Tuple[str, str]]:
    sigs: Set[Tuple[str, str]] = set()
    for r in routes or []:
        path = str(getattr(r, "path", "") or "")
        methods = getattr(r, "methods", None) or set()
        if not isinstance(methods, (set, list, tuple)):
            methods = set()
        meth = ",".join(sorted(str(m) for m in methods))
        sigs.add((path, meth))
    return sigs


def _route_signature_pairs_from_route(route: Any) -> Set[Tuple[str, str]]:
    path = str(getattr(route, "path", "") or "")
    methods = getattr(route, "methods", None) or set()
    if not isinstance(methods, (set, list, tuple)):
        methods = set()
    meth = ",".join(sorted(str(m) for m in methods))
    if not path:
        return set()
    return {(path, meth)}


def _route_paths_from_routes(routes: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for r in routes or []:
        path = str(getattr(r, "path", "") or "")
        if path and path not in seen:
            seen.add(path)
            out.append(path)
    return out


def _app_route_signature_set(app: Any) -> Set[Tuple[str, str]]:
    return _route_signature_set_from_routes(getattr(app, "routes", []) or [])


def _route_signature_count(app: Any) -> int:
    return len(_app_route_signature_set(app))


def _router_route_paths(router: Any) -> List[str]:
    return _route_paths_from_routes(getattr(router, "routes", []) or [])


def _invalidate_openapi_cache(app: Any) -> None:
    try:
        if hasattr(app, "openapi_schema"):
            app.openapi_schema = None
    except Exception:
        pass


def _is_protected_path(path: str, protected_prefixes: Sequence[str]) -> bool:
    p = str(path or "")
    return any(p.startswith(prefix) for prefix in protected_prefixes)


def _claimed_protected_prefixes_from_paths(
    paths: Sequence[str],
    protected_prefixes: Sequence[str],
) -> List[str]:
    claimed: List[str] = []
    seen: Set[str] = set()

    for path in paths:
        p = str(path or "")
        for prefix in protected_prefixes:
            if p.startswith(prefix) and prefix not in seen:
                seen.add(prefix)
                claimed.append(prefix)

    return claimed


def _overlap_details(
    existing_sigs: Set[Tuple[str, str]],
    candidate_sigs: Set[Tuple[str, str]],
    protected_prefixes: Sequence[str],
) -> Dict[str, Any]:
    overlap = existing_sigs.intersection(candidate_sigs)
    protected_overlap = sorted(
        [(path, meth) for (path, meth) in overlap if _is_protected_path(path, protected_prefixes)]
    )
    return {
        "overlap_count": len(overlap),
        "protected_overlap_count": len(protected_overlap),
        "protected_overlap": protected_overlap[:100],
    }


def _canonical_owner_for_prefix(prefix: str, owner_map: Dict[str, str]) -> Optional[str]:
    return owner_map.get(str(prefix or "").strip())


def _owner_conflicts_for_claimed_prefixes(
    *,
    module_key: str,
    module_name: str,
    claimed_prefixes: Sequence[str],
    prefix_owners: Dict[str, str],
    module_to_key: Dict[str, str],
    canonical_owner_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    conflicts: List[Dict[str, Any]] = []

    for prefix in claimed_prefixes:
        canonical_owner_key = _canonical_owner_for_prefix(prefix, canonical_owner_map)
        if canonical_owner_key and canonical_owner_key != module_key:
            conflicts.append(
                {
                    "prefix": prefix,
                    "reason": "canonical_owner_mismatch",
                    "candidate_module": module_name,
                    "candidate_key": module_key,
                    "canonical_owner_key": canonical_owner_key,
                    "existing_owner_module": prefix_owners.get(prefix),
                    "existing_owner_key": module_to_key.get(prefix_owners.get(prefix, ""), ""),
                }
            )
            continue

        existing_owner_module = prefix_owners.get(prefix)
        existing_owner_key = module_to_key.get(existing_owner_module, "") if existing_owner_module else ""
        if existing_owner_module and existing_owner_module != module_name:
            conflicts.append(
                {
                    "prefix": prefix,
                    "reason": "prefix_already_owned",
                    "candidate_module": module_name,
                    "candidate_key": module_key,
                    "existing_owner_module": existing_owner_module,
                    "existing_owner_key": existing_owner_key,
                    "canonical_owner_key": canonical_owner_key or existing_owner_key or module_key,
                }
            )

    return conflicts


def _path_allowed_for_module(module_name: str, path: str) -> bool:
    policy = _module_route_policies().get(module_name)
    if not policy:
        return True

    allow_prefixes = tuple(policy.get("allow_prefixes", ()) or ())
    allow_exact = tuple(policy.get("allow_exact", ()) or ())
    block_prefixes = tuple(policy.get("block_prefixes", ()) or ())
    block_exact = tuple(policy.get("block_exact", ()) or ())

    if path in block_exact:
        return False
    if any(path.startswith(prefix) for prefix in block_prefixes):
        return False

    if allow_exact or allow_prefixes:
        if path in allow_exact:
            return True
        if any(path.startswith(prefix) for prefix in allow_prefixes):
            return True
        return False

    return True


def _get_or_init_snapshot_store(app: Any) -> Dict[str, Any]:
    snap = {
        "mounted_modules": [],
        "duplicate_skips": [],
        "partial_duplicate_skips": [],
        "conflict_skips": [],
        "policy_skips": [],
        "missing_modules": [],
        "import_errors": {},
        "mount_errors": {},
        "no_router": {},
        "mount_modes": {},
        "module_to_key": {},
        "router_signatures": {},
        "priority_map": {},
        "overlap_details": {},
        "protected_prefixes": _protected_prefixes(),
        "canonical_owner_map": _canonical_owner_map(),
        "prefix_owners": {},
        "claimed_prefixes": {},
        "owner_conflicts": {},
        "policy_filtered_routes": {},
        "route_signature_count_after_mount": 0,
    }
    try:
        if hasattr(app, "state"):
            existing = getattr(app.state, "routes_snapshot", None)
            if isinstance(existing, dict):
                snap.update(existing)
    except Exception:
        pass
    return snap


def _remove_added_routes(app: Any, added_routes: Sequence[Any]) -> None:
    if not added_routes:
        return

    added_ids = {id(r) for r in added_routes}

    try:
        if hasattr(app, "router") and hasattr(app.router, "routes"):
            app.router.routes = [r for r in list(app.router.routes) if id(r) not in added_ids]
    except Exception:
        pass

    try:
        if hasattr(app, "routes"):
            app.routes = [r for r in list(app.routes) if id(r) not in added_ids]
    except Exception:
        pass

    _invalidate_openapi_cache(app)


def _append_router_routes_filtered(app: Any, router_obj: Any, module_name: str) -> Dict[str, Any]:
    existing = _app_route_signature_set(app)

    added = 0
    duplicate_skips = 0
    partial_duplicate_skips = 0
    filtered_out = 0
    filtered_paths: List[str] = []
    added_paths: List[str] = []
    added_routes: List[Any] = []

    for route in list(getattr(router_obj, "routes", []) or []):
        path = str(getattr(route, "path", "") or "")
        if not _path_allowed_for_module(module_name, path):
            filtered_out += 1
            if path:
                filtered_paths.append(path)
            continue

        sigs = _route_signature_pairs_from_route(route)
        if not sigs:
            continue

        overlap = sigs & existing
        if overlap == sigs:
            duplicate_skips += 1
            continue

        if overlap:
            partial_duplicate_skips += 1
            logger.warning(
                "Skipped partial-duplicate route while mounting %s: path=%s overlap=%s",
                module_name,
                path,
                sorted(list(overlap)),
            )
            continue

        app.router.routes.append(route)
        existing.update(sigs)
        added += 1
        added_routes.append(route)
        if path:
            added_paths.append(path)

    _invalidate_openapi_cache(app)
    return {
        "added": added,
        "duplicate_skips": duplicate_skips,
        "partial_duplicate_skips": partial_duplicate_skips,
        "filtered_out": filtered_out,
        "filtered_paths": _dedupe_keep_order(filtered_paths),
        "added_paths": _dedupe_keep_order(added_paths),
        "added_routes": added_routes,
    }


def _split_added_routes_by_duplicate(
    existing_sigs: Set[Tuple[str, str]],
    added_routes: Sequence[Any],
) -> Tuple[List[Any], List[Any], List[Any]]:
    kept: List[Any] = []
    duplicate_only: List[Any] = []
    partial_duplicate: List[Any] = []
    seen_existing = set(existing_sigs)

    for route in added_routes or []:
        sigs = _route_signature_pairs_from_route(route)
        if not sigs:
            kept.append(route)
            continue

        overlap = sigs & seen_existing
        if overlap == sigs:
            duplicate_only.append(route)
            continue
        if overlap:
            partial_duplicate.append(route)
            continue

        kept.append(route)
        seen_existing.update(sigs)

    return kept, duplicate_only, partial_duplicate


# ======================================================================================
# Mount helpers
# ======================================================================================
def _mount_one(
    app: Any,
    module_name: str,
    *,
    module_key: str,
    protected_prefixes: Sequence[str],
    prefix_owners: Dict[str, str],
    module_to_key: Dict[str, str],
    canonical_owner_map: Dict[str, str],
) -> Tuple[bool, Optional[str], str, Dict[str, Any]]:
    """
    Mount strategies:
    - module.router -> filtered route append
    - module.get_router()/build_router()/create_router() -> filtered route append
    - module.mount(app) -> route-level diff/rollback filtering

    Returns:
      (success, error_message, mode, details)
    """
    mod, exc = _import_module(module_name)
    if mod is None:
        return False, _err_to_str(exc or Exception("import failed")), "import_error", {}

    router, source = _get_router_from_module(mod)
    mount_fn = getattr(mod, "mount", None)

    try:
        if router is not None:
            router_paths = _router_route_paths(router)
            allowed_paths = [p for p in router_paths if _path_allowed_for_module(module_name, p)]
            claimed_prefixes = _claimed_protected_prefixes_from_paths(allowed_paths, protected_prefixes)

            owner_conflicts = _owner_conflicts_for_claimed_prefixes(
                module_key=module_key,
                module_name=module_name,
                claimed_prefixes=claimed_prefixes,
                prefix_owners=prefix_owners,
                module_to_key=module_to_key,
                canonical_owner_map=canonical_owner_map,
            )
            if owner_conflicts:
                return True, None, "conflict_skip", {
                    "reason": "protected_prefix_owner_conflict",
                    "route_count": len(getattr(router, "routes", []) or []),
                    "claimed_prefixes": claimed_prefixes,
                    "owner_conflicts": owner_conflicts,
                    "policy_filtered_routes": [],
                    "duplicate_skips": 0,
                    "partial_duplicate_skips": 0,
                    "filtered_out": 0,
                }

            existing_before = _app_route_signature_set(app)
            append_stats = _append_router_routes_filtered(app, router, module_name)
            added_routes = list(append_stats.get("added_routes", []) or [])
            added_sigs = _route_signature_set_from_routes(added_routes)

            details = {
                "route_count": len(getattr(router, "routes", []) or []),
                "prefix": str(getattr(router, "prefix", "") or ""),
                "claimed_prefixes": claimed_prefixes,
                "policy_filtered_routes": list(append_stats.get("filtered_paths", []) or []),
                "added_count": int(append_stats.get("added", 0) or 0),
                "duplicate_skips": int(append_stats.get("duplicate_skips", 0) or 0),
                "partial_duplicate_skips": int(append_stats.get("partial_duplicate_skips", 0) or 0),
                "filtered_out": int(append_stats.get("filtered_out", 0) or 0),
                **_overlap_details(existing_before, added_sigs, protected_prefixes),
            }

            if details["added_count"] <= 0:
                if details["filtered_out"] > 0:
                    return True, None, "policy_skip", details
                if details["partial_duplicate_skips"] > 0:
                    return True, None, "partial_duplicate_skip", details
                return True, None, "duplicate_skip", details

            return True, None, source, details

        if callable(mount_fn):
            before_routes = list(getattr(app, "routes", []) or [])
            before_ids = {id(r) for r in before_routes}
            before_sigs = _route_signature_set_from_routes(before_routes)

            mount_fn(app)

            after_routes = list(getattr(app, "routes", []) or [])
            added_routes = [r for r in after_routes if id(r) not in before_ids]
            if not added_routes:
                return True, None, "mount_fn", {
                    "route_count": 0,
                    "claimed_prefixes": [],
                    "policy_filtered_routes": [],
                    "duplicate_skips": 0,
                    "partial_duplicate_skips": 0,
                    "filtered_out": 0,
                }

            disallowed_routes = [
                r for r in added_routes if not _path_allowed_for_module(module_name, str(getattr(r, "path", "") or ""))
            ]
            if disallowed_routes:
                _remove_added_routes(app, disallowed_routes)

            remaining_routes = [r for r in added_routes if id(r) not in {id(x) for x in disallowed_routes}]
            kept_routes, duplicate_routes, partial_duplicate_routes = _split_added_routes_by_duplicate(before_sigs, remaining_routes)

            if duplicate_routes:
                _remove_added_routes(app, duplicate_routes)
            if partial_duplicate_routes:
                _remove_added_routes(app, partial_duplicate_routes)

            kept_paths = _route_paths_from_routes(kept_routes)
            claimed_prefixes = _claimed_protected_prefixes_from_paths(kept_paths, protected_prefixes)

            owner_conflicts = _owner_conflicts_for_claimed_prefixes(
                module_key=module_key,
                module_name=module_name,
                claimed_prefixes=claimed_prefixes,
                prefix_owners=prefix_owners,
                module_to_key=module_to_key,
                canonical_owner_map=canonical_owner_map,
            )
            if owner_conflicts:
                _remove_added_routes(app, kept_routes)
                return True, None, "conflict_skip", {
                    "reason": "mount_fn_protected_prefix_owner_conflict",
                    "route_count": len(kept_routes),
                    "claimed_prefixes": claimed_prefixes,
                    "policy_filtered_routes": _route_paths_from_routes(disallowed_routes),
                    "owner_conflicts": owner_conflicts,
                    "duplicate_skips": len(duplicate_routes),
                    "partial_duplicate_skips": len(partial_duplicate_routes),
                    "filtered_out": len(disallowed_routes),
                }

            kept_sigs = _route_signature_set_from_routes(kept_routes)
            details = {
                "route_count": len(kept_sigs),
                "claimed_prefixes": claimed_prefixes,
                "policy_filtered_routes": _route_paths_from_routes(disallowed_routes),
                "duplicate_skips": len(duplicate_routes),
                "partial_duplicate_skips": len(partial_duplicate_routes),
                "filtered_out": len(disallowed_routes),
                **_overlap_details(before_sigs, kept_sigs, protected_prefixes),
            }

            if not kept_routes:
                if details["filtered_out"] > 0:
                    return True, None, "policy_skip", details
                if details["partial_duplicate_skips"] > 0:
                    return True, None, "partial_duplicate_skip", details
                if details["duplicate_skips"] > 0:
                    return True, None, "duplicate_skip", details
                return True, None, "mount_fn", details

            return True, None, "mount_fn", details

        return False, "No router export and no mount(app) function found", "no_router", {}

    except Exception as e:
        return False, _err_to_str(e), "mount_error", {}


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
    protected_prefixes = _protected_prefixes()
    canonical_owner_map = _canonical_owner_map()

    catalog = _default_catalog()
    resolved_entries, missing_required = _resolve_catalog(catalog)
    resolved_entries = _filter_resolved_entries(resolved_entries)

    if log_plan:
        logger.info(
            "Resolved router plan: %s",
            ", ".join(
                f"{x['priority']}:{x['key']}=>{x['module']}"
                for x in resolved_entries if x.get("module")
            ) or "(empty)",
        )

    snap = _get_or_init_snapshot_store(app)

    mounted: List[str] = []
    duplicate_skips: List[str] = []
    partial_duplicate_skips: List[str] = []
    conflict_skips: List[str] = []
    policy_skips: List[str] = []
    missing: List[str] = []
    import_errors: Dict[str, str] = {}
    mount_errors: Dict[str, str] = {}
    no_router: Dict[str, str] = {}
    mount_modes: Dict[str, str] = {}
    module_to_key: Dict[str, str] = {}
    resolved_map: Dict[str, str] = {}
    priority_map: Dict[str, int] = {}
    overlap_details: Dict[str, Any] = {}
    claimed_prefixes_by_module: Dict[str, List[str]] = {}
    owner_conflicts: Dict[str, Any] = {}
    policy_filtered_routes: Dict[str, List[str]] = {}

    plan_modules: List[str] = []

    for entry in resolved_entries:
        key = str(entry.get("key") or "")
        module_name = entry.get("module")
        priority = int(entry.get("priority", 100))
        if module_name:
            resolved_map[key] = str(module_name)
            priority_map[str(module_name)] = priority
            plan_modules.append(str(module_name))

    plan_modules_unique = _dedupe_keep_order(plan_modules)
    prefix_owners: Dict[str, str] = dict(snap.get("prefix_owners", {}) or {})

    for entry in resolved_entries:
        key = str(entry.get("key") or "")
        module_name = entry.get("module")
        priority = int(entry.get("priority", 100))

        if not module_name:
            all_candidates = entry.get("all_candidates") or []
            existing_candidates = entry.get("existing_candidates") or []
            if all_candidates and not existing_candidates:
                continue
            continue

        module_name = str(module_name)
        module_to_key[module_name] = key
        priority_map[module_name] = priority

        if not _module_exists(module_name):
            missing.append(module_name)
            continue

        ok, err, mode, details = _mount_one(
            app,
            module_name,
            module_key=key,
            protected_prefixes=protected_prefixes,
            prefix_owners=prefix_owners,
            module_to_key=module_to_key,
            canonical_owner_map=canonical_owner_map,
        )

        if details:
            overlap_details[module_name] = details
            claimed_prefixes_by_module[module_name] = list(details.get("claimed_prefixes", []) or [])
            policy_filtered_routes[module_name] = list(details.get("policy_filtered_routes", []) or [])
            if details.get("owner_conflicts"):
                owner_conflicts[module_name] = details.get("owner_conflicts")

        if ok:
            if mode == "duplicate_skip":
                duplicate_skips.append(module_name)
            elif mode == "partial_duplicate_skip":
                partial_duplicate_skips.append(module_name)
            elif mode == "conflict_skip":
                conflict_skips.append(module_name)
            elif mode == "policy_skip":
                policy_skips.append(module_name)
            else:
                mounted.append(module_name)
                for prefix in claimed_prefixes_by_module.get(module_name, []):
                    prefix_owners[prefix] = module_name

            mount_modes[module_name] = mode

            try:
                mod = importlib.import_module(module_name)
                router, _source = _get_router_from_module(mod)
                if router is not None:
                    snap.setdefault("router_signatures", {})[module_name] = {
                        "signature": _router_signature(router),
                        "route_count": len(getattr(router, "routes", []) or []),
                        "prefix": str(getattr(router, "prefix", "") or ""),
                        "priority": priority,
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
        "partial_duplicate_skips": partial_duplicate_skips,
        "partial_duplicate_skips_count": len(partial_duplicate_skips),
        "conflict_skips": conflict_skips,
        "conflict_skips_count": len(conflict_skips),
        "policy_skips": policy_skips,
        "policy_skips_count": len(policy_skips),
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
        "priority_map": priority_map,
        "missing_required_keys": missing_required,
        "plan": plan_modules_unique,
        "plan_count": len(plan_modules_unique),
        "mount_modes": mount_modes,
        "expected_router_modules": get_expected_router_modules(),
        "expected_router_modules_count": len(get_expected_router_modules()),
        "catalog_keys": [spec.key for spec in sorted(catalog, key=lambda s: (int(s.priority), s.key))],
        "resolved_entries": resolved_entries,
        "openapi_route_count_after_mount": len(getattr(app, "routes", []) or []),
        "route_signature_count_after_mount": _route_signature_count(app),
        "protected_prefixes": protected_prefixes,
        "canonical_owner_map": canonical_owner_map,
        "prefix_owners": prefix_owners,
        "claimed_prefixes": claimed_prefixes_by_module,
        "owner_conflicts": owner_conflicts,
        "policy_filtered_routes": policy_filtered_routes,
        "overlap_details": overlap_details,
    }

    logger.info(
        "Routes mount summary: mounted=%s duplicate_skips=%s partial_duplicate_skips=%s conflict_skips=%s policy_skips=%s missing=%s failed=%s strict=%s",
        len(mounted),
        len(duplicate_skips),
        len(partial_duplicate_skips),
        len(conflict_skips),
        len(policy_skips),
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
    if partial_duplicate_skips:
        logger.info("Partial duplicate router skips: %s", ", ".join(partial_duplicate_skips))
    if conflict_skips:
        logger.info("Conflict router skips: %s", ", ".join(conflict_skips))
    if policy_skips:
        logger.info("Policy router skips: %s", ", ".join(policy_skips))
    if owner_conflicts:
        logger.info("Owner conflicts: %s", json.dumps(owner_conflicts, ensure_ascii=False))

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
