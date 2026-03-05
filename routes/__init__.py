#!/usr/bin/env python3
"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) — v3.0.0 (PHASE-8+ SCHEMA-FIRST / ROUTE-COVERAGE GUARDED)

Goal (your wiring concern):
- Ensure the routers that *actually serve* schema-driven pages are mounted (not skipped / not shadowed)
- Prevent “fallback to 80 columns” by ensuring sheet-rows owners are present
- Prefer wrapper routers when they truly provide the required endpoints; otherwise auto-fallback to the next candidate

Key behavior upgrades vs v2.9.0:
- ✅ Family mount is now “capability-aware”: we only pick a family winner if it actually registers
  the required routes (e.g., POST */sheet-rows). Otherwise we try the next candidate.
- ✅ Duplicate handling is now “shadow-safe”:
    - If a router would add ZERO new routes (only duplicates), we skip it (shadowed)
    - If it adds any new routes, we mount it and record duplicates as warnings (no silent skipping)
- ✅ Adds route-coverage checks after mount and records them in the mount report.
- ✅ Schema preflight remains mount-time only (no network calls, no heavy imports at module import time).

Env overrides (optional)
- ROUTES_EXPECTED="routes.config,routes.enriched_quote,..."
- ROUTES_AUTO_DISCOVER=1                   (scan package via pkgutil; off by default)
- ROUTES_STRICT=1                          (raise on failures for required modules)
- ROUTES_REQUIRED="routes.config,..."       (comma list; only used when ROUTES_STRICT=1)
- ROUTES_IMPORT_TIMEOUT_SEC=5              (async import timeout)
- ROUTES_CB_THRESHOLD=3                    (import circuit breaker threshold)
- ROUTES_CB_RESET_TIMEOUT_SEC=60           (circuit breaker reset timeout)
- ROUTES_SKIP_DUPLICATES=1                 (default ON; but shadow-safe)
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import os
import pkgutil
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache, wraps
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

ROUTES_PACKAGE_VERSION = "3.0.0"

# =============================================================================
# Small env helpers (no deps)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def coerce_bool(val: Any, default: bool = False) -> bool:
    if isinstance(val, bool):
        return val
    if val is None:
        return default
    s = str(val).strip().lower()
    if not s:
        return default
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return default


def _parse_env_list(name: str) -> List[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# Enums and Types
# =============================================================================
class ModuleStatus(Enum):
    AVAILABLE = "available"
    MISSING = "missing"
    ERROR = "error"
    LOADED = "loaded"
    FAILED = "failed"
    SKIPPED = "skipped"


class ErrorCategory(Enum):
    NOT_FOUND = "not_found"
    IMPORT_ERROR = "import_error"
    ROUTER_MISSING = "router_missing"
    INVALID = "invalid"
    TIMEOUT = "timeout"
    DUPLICATE = "duplicate"
    SHADOWED = "shadowed"
    COVERAGE = "coverage"
    UNKNOWN = "unknown"


class ModuleGroup(Enum):
    SECURITY = "security"
    SYSTEM = "system"
    CORE = "core"
    SCHEMA = "schema"
    ANALYSIS = "analysis"
    ADVISOR = "advisor"
    KSA = "ksa"
    OTHER = "other"

    @property
    def priority(self) -> int:
        priorities = {
            "security": 0,
            "system": 1,
            "core": 2,
            "schema": 3,
            "analysis": 4,
            "advisor": 5,
            "ksa": 6,
            "other": 9,
        }
        return priorities.get(self.value, 9)


@dataclass(slots=True)
class ModuleInfo:
    name: str
    group: ModuleGroup
    status: ModuleStatus = ModuleStatus.MISSING
    error: Optional[str] = None
    error_category: Optional[ErrorCategory] = None
    load_time_ms: Optional[float] = None
    last_checked_utc: Optional[str] = None

    router_attr: str = "router"
    mount_attr: str = "mount"
    router_found: bool = False
    mount_found: bool = False

    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RouterSpec:
    module: str
    group: ModuleGroup
    required: bool = False


# =============================================================================
# Lightweight metrics (no deps)
# =============================================================================
class RoutesMetrics:
    def __init__(self):
        self._start = time.time()
        self._timings_ms: List[float] = []
        self._exists_checks: Dict[str, int] = {}
        self._import_attempts: Dict[str, int] = {}
        self._errors: Dict[str, int] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    def record_timing(self, ms: float) -> None:
        self._timings_ms.append(float(ms))

    def record_exists_check(self, module: str, hit_cache: bool) -> None:
        self._exists_checks[module] = self._exists_checks.get(module, 0) + 1
        if hit_cache:
            self._cache_hits += 1
        else:
            self._cache_misses += 1

    def record_import(self, module: str) -> None:
        self._import_attempts[module] = self._import_attempts.get(module, 0) + 1

    def record_error(self, category: Union[str, ErrorCategory]) -> None:
        key = category.value if isinstance(category, ErrorCategory) else str(category)
        self._errors[key] = self._errors.get(key, 0) + 1

    def get_stats(self) -> Dict[str, Any]:
        total = len(self._timings_ms)
        avg = (sum(self._timings_ms) / total) if total else 0.0
        denom = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / denom) if denom else 0.0
        return {
            "uptime_seconds": time.time() - self._start,
            "timings_count": total,
            "avg_timing_ms": avg,
            "exists_checks_unique": len(self._exists_checks),
            "exists_checks_total": sum(self._exists_checks.values()),
            "import_attempts_unique": len(self._import_attempts),
            "import_attempts_total": sum(self._import_attempts.values()),
            "cache_hit_rate": hit_rate,
            "errors_by_category": dict(self._errors),
        }


_metrics = RoutesMetrics()


def get_metrics() -> RoutesMetrics:
    return _metrics


def timed(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            _metrics.record_timing((time.perf_counter() - start) * 1000.0)

    return wrapper


def async_timed(func: Callable[..., Awaitable]) -> Callable[..., Awaitable]:
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return await func(*args, **kwargs)
        finally:
            _metrics.record_timing((time.perf_counter() - start) * 1000.0)

    return wrapper


# =============================================================================
# Circuit breaker (IMPORT failures only)
# =============================================================================
class CircuitBreaker:
    """
    Protects repeated import failures.
    Missing spec is NOT a failure; only exceptions during import/router extraction are failures.
    """

    def __init__(self, failure_threshold: int = 3, reset_timeout_sec: float = 60.0):
        self.failure_threshold = max(1, int(failure_threshold))
        self.reset_timeout_sec = max(5.0, float(reset_timeout_sec))
        self._failures: Dict[str, int] = {}
        self._last_failure: Dict[str, float] = {}
        self._open: Set[str] = set()

    def record_failure(self, module: str) -> None:
        now = time.time()
        self._failures[module] = self._failures.get(module, 0) + 1
        self._last_failure[module] = now
        if self._failures[module] >= self.failure_threshold:
            self._open.add(module)

    def record_success(self, module: str) -> None:
        self._failures.pop(module, None)
        self._last_failure.pop(module, None)
        self._open.discard(module)

    def is_open(self, module: str) -> bool:
        if module not in self._open:
            return False
        last = self._last_failure.get(module)
        if last and (time.time() - last) > self.reset_timeout_sec:
            self._open.discard(module)
            self._failures.pop(module, None)
            self._last_failure.pop(module, None)
            return False
        return True

    @contextmanager
    def guard(self, module: str):
        if self.is_open(module):
            raise RuntimeError(f"circuit_open:{module}")
        try:
            yield
            self.record_success(module)
        except Exception:
            self.record_failure(module)
            raise

    def snapshot(self) -> Dict[str, Any]:
        return {
            "open": sorted(self._open),
            "failure_counts": dict(self._failures),
            "threshold": self.failure_threshold,
            "reset_timeout_sec": self.reset_timeout_sec,
        }


_circuit_breaker = CircuitBreaker(
    failure_threshold=int(os.getenv("ROUTES_CB_THRESHOLD", "3") or "3"),
    reset_timeout_sec=float(os.getenv("ROUTES_CB_RESET_TIMEOUT_SEC", "60") or "60"),
)


# =============================================================================
# Normalization + existence checks
# =============================================================================
def _normalize_module_name(module_path: str) -> str:
    if not module_path:
        return ""
    s = str(module_path).strip()
    if not s:
        return ""
    s = s.replace("\\", "/")
    if s.endswith(".py"):
        s = s[:-3]
    if s.endswith("/__init__"):
        s = s[:-9]
    s = s.replace("/", ".")
    return s.strip(".")


@lru_cache(maxsize=4096)
def _find_spec_cached(module_name: str) -> Tuple[bool, Optional[str]]:
    mn = _normalize_module_name(module_name)
    if not mn:
        return False, "empty module name"
    try:
        spec = importlib.util.find_spec(mn)
        if spec is None:
            return False, "spec not found"
        if spec.origin is None and not spec.has_location and spec.submodule_search_locations is None:
            return False, "no location and not namespace package"
        return True, None
    except ModuleNotFoundError as e:
        return False, f"module not found: {e}"
    except ValueError as e:
        return False, f"invalid module name: {e}"
    except Exception as e:
        _metrics.record_error(ErrorCategory.UNKNOWN)
        return False, f"unexpected error: {e}"


@timed
def module_exists(module_path: str, *, use_cache: bool = True) -> bool:
    mn = _normalize_module_name(module_path)
    if not mn:
        _metrics.record_error(ErrorCategory.INVALID)
        return False

    if use_cache:
        ok, _ = _find_spec_cached(mn)
        _metrics.record_exists_check(mn, hit_cache=True)
        return bool(ok)

    try:
        spec = importlib.util.find_spec(mn)
        _metrics.record_exists_check(mn, hit_cache=False)
        return spec is not None
    except Exception:
        _metrics.record_error(ErrorCategory.UNKNOWN)
        _metrics.record_exists_check(mn, hit_cache=False)
        return False


@async_timed
async def module_exists_async(module_path: str, *, timeout: float = 5.0, use_cache: bool = True) -> bool:
    try:
        return await asyncio.wait_for(asyncio.to_thread(module_exists, module_path, use_cache=use_cache), timeout=timeout)
    except asyncio.TimeoutError:
        _metrics.record_error(ErrorCategory.TIMEOUT)
        return False
    except Exception:
        _metrics.record_error(ErrorCategory.UNKNOWN)
        return False


# =============================================================================
# Grouping (Phase-8 aligned)
# =============================================================================
def _group_of(module_name: str) -> ModuleGroup:
    ml = (module_name or "").lower()

    if any(k in ml for k in ("auth", "security", "middleware", "rate", "jwt", "token", "cors")):
        return ModuleGroup.SECURITY

    if any(k in ml for k in ("health", "status", "ping", "meta", "version", "ready", "live")):
        return ModuleGroup.SYSTEM

    if any(k in ml for k in ("data_dictionary", "schema", "registry", "dictionary")):
        return ModuleGroup.SCHEMA

    if any(k in ml for k in ("advanced_analysis", "advanced_sheet_rows", "ai_analysis", "insights", "analysis_sheet_rows", "analysis")):
        return ModuleGroup.ANALYSIS

    if any(k in ml for k in ("investment_advisor", "advisor")):
        return ModuleGroup.ADVISOR

    if any(k in ml for k in ("argaam", "tadawul", "routes_argaam", "ksa", "saudi")):
        return ModuleGroup.KSA

    if any(k in ml for k in ("enriched", "quote", "quotes", "market", "engine")):
        return ModuleGroup.CORE

    return ModuleGroup.OTHER


# =============================================================================
# Defaults: expected routers (schema-first)
# =============================================================================
def get_routes_version() -> str:
    return ROUTES_PACKAGE_VERSION


def get_expected_router_modules() -> List[str]:
    """
    Default expected router modules.
    Override via ROUTES_EXPECTED.

    IMPORTANT:
    - Include likely sheet-rows owners to avoid “not mounted / 80 fallback” issues.
    - Preference families will choose winners, but will fall back if the winner
      doesn't actually provide required endpoints.
    """
    override = _parse_env_list("ROUTES_EXPECTED")
    if override:
        return [_normalize_module_name(x) for x in override if _normalize_module_name(x)]

    # Core + optional
    expected = [
        "routes.config",
        "routes.enriched_quote",
        "routes.routes_argaam",
        # Common “sheet rows” owners (names vary across repos)
        "routes.sheet_rows",
        "routes.sheets",
        "routes.sheet_data",
        "routes.sheet_rows_v2",
        # Your schema-first routers
        "routes.analysis_sheet_rows",
        "routes.advanced_sheet_rows",
        # Wrapper routers (only win if they really provide coverage)
        "routes.ai_analysis",
        "routes.insights_analysis",
        "routes.advanced_analysis",
        # Top10 + advisor
        "routes.top10_investments",
        "routes.investment_advisor",
        "routes.advisor",
    ]
    return expected


def _auto_discover_router_modules() -> List[str]:
    """
    Optional discovery by scanning the 'routes' package.
    Safe: lists module names only; no imports.
    Enabled with ROUTES_AUTO_DISCOVER=1.
    """
    if not coerce_bool(os.getenv("ROUTES_AUTO_DISCOVER", ""), False):
        return []
    try:
        import routes as _routes_pkg  # type: ignore

        discovered: List[str] = []
        for m in pkgutil.iter_modules(_routes_pkg.__path__, prefix="routes."):
            name = getattr(m, "name", "")
            if not name:
                continue
            if name.split(".")[-1].startswith("_"):
                continue
            discovered.append(name)
        return sorted(set(discovered))
    except Exception:
        _metrics.record_error(ErrorCategory.UNKNOWN)
        return []


# =============================================================================
# Preference families (avoid duplicates but ensure coverage)
# =============================================================================
_ANALYSIS_FAMILY = [
    "routes.ai_analysis",
    "routes.insights_analysis",
    "routes.analysis_sheet_rows",
    "routes.sheet_rows",
    "routes.sheets",
    "routes.sheet_data",
]
_ADVANCED_FAMILY = ["routes.advanced_analysis", "routes.advanced_sheet_rows"]
_ADVISOR_FAMILY = ["routes.investment_advisor", "routes.advisor"]


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for x in items:
        nx = _normalize_module_name(x)
        if not nx or nx in seen:
            continue
        seen.add(nx)
        out.append(nx)
    return out


def _required_set_from_env() -> Set[str]:
    req = set(_parse_env_list("ROUTES_REQUIRED"))
    return {_normalize_module_name(x) for x in req if _normalize_module_name(x)}


# =============================================================================
# Import + extraction
# =============================================================================
def _classify_import_error(e: Exception) -> ErrorCategory:
    if isinstance(e, ModuleNotFoundError):
        return ErrorCategory.NOT_FOUND
    if isinstance(e, ImportError):
        return ErrorCategory.IMPORT_ERROR
    return ErrorCategory.UNKNOWN


def _err_str(e: BaseException, limit: int = 900) -> str:
    try:
        s = f"{type(e).__name__}: {e}"
    except Exception:
        s = "UnknownError"
    return s if len(s) <= limit else (s[:limit] + "...(truncated)")


def load_mount_target(
    module: str,
    *,
    router_attr: str = "router",
    mount_attr: str = "mount",
) -> Tuple[Optional[Any], Optional[Callable[[Any], None]], ModuleInfo]:
    m = _normalize_module_name(module)
    info = ModuleInfo(name=m, group=_group_of(m), status=ModuleStatus.MISSING, router_attr=router_attr, mount_attr=mount_attr)
    info.last_checked_utc = _utc_iso()

    ok, err = _find_spec_cached(m)
    _metrics.record_exists_check(m, hit_cache=True)
    if not ok:
        info.status = ModuleStatus.MISSING
        info.error = err
        info.error_category = ErrorCategory.NOT_FOUND
        return None, None, info

    if _circuit_breaker.is_open(m):
        info.status = ModuleStatus.FAILED
        info.error = "circuit_open(import_failures)"
        info.error_category = ErrorCategory.IMPORT_ERROR
        _metrics.record_error(ErrorCategory.IMPORT_ERROR)
        return None, None, info

    start = time.perf_counter()
    try:
        with _circuit_breaker.guard(m):
            _metrics.record_import(m)
            mod = importlib.import_module(m)

            router = getattr(mod, router_attr, None)
            mount_fn = getattr(mod, mount_attr, None)

            info.router_found = router is not None
            info.mount_found = callable(mount_fn)

            if router is None and not callable(mount_fn):
                info.status = ModuleStatus.FAILED
                info.error = f"missing router/mount (expected `{router_attr}` or `{mount_attr}(app)`)"
                info.error_category = ErrorCategory.ROUTER_MISSING
                _metrics.record_error(ErrorCategory.ROUTER_MISSING)
                return None, None, info

            info.status = ModuleStatus.LOADED
            return router, mount_fn if callable(mount_fn) else None, info

    except Exception as e:
        info.status = ModuleStatus.FAILED
        info.error = _err_str(e)
        info.error_category = _classify_import_error(e)
        _metrics.record_error(info.error_category)
        return None, None, info
    finally:
        info.load_time_ms = (time.perf_counter() - start) * 1000.0


async def load_mount_target_async(
    module: str,
    *,
    router_attr: str = "router",
    mount_attr: str = "mount",
    timeout: Optional[float] = None,
) -> Tuple[Optional[Any], Optional[Callable[[Any], None]], ModuleInfo]:
    t = float(timeout if timeout is not None else float(os.getenv("ROUTES_IMPORT_TIMEOUT_SEC", "5") or "5"))
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(load_mount_target, module, router_attr=router_attr, mount_attr=mount_attr),
            timeout=t,
        )
    except asyncio.TimeoutError:
        info = ModuleInfo(
            name=_normalize_module_name(module),
            group=_group_of(module),
            status=ModuleStatus.FAILED,
            error="import timeout",
            error_category=ErrorCategory.TIMEOUT,
            router_attr=router_attr,
            mount_attr=mount_attr,
            router_found=False,
            mount_found=False,
            last_checked_utc=_utc_iso(),
        )
        _metrics.record_error(ErrorCategory.TIMEOUT)
        return None, None, info


# =============================================================================
# Route key extraction + capability checks
# =============================================================================
def _collect_route_keys(app: Any) -> Set[Tuple[str, str]]:
    keys: Set[Tuple[str, str]] = set()
    router = getattr(app, "router", None)
    routes = getattr(router, "routes", None) if router is not None else None
    if not isinstance(routes, list):
        return keys
    for r in routes:
        path = getattr(r, "path", None)
        methods = getattr(r, "methods", None)
        if not path or not methods:
            continue
        for m in methods:
            keys.add((str(m).upper(), str(path)))
    return keys


def _router_keys(router: Any) -> Set[Tuple[str, str]]:
    keys: Set[Tuple[str, str]] = set()
    prefix = getattr(router, "prefix", "") or ""
    routes = getattr(router, "routes", None)
    if not isinstance(routes, list):
        return keys
    for r in routes:
        path = getattr(r, "path", "") or ""
        methods = getattr(r, "methods", None) or set()
        full = f"{prefix}{path}"
        for m in methods:
            keys.add((str(m).upper(), str(full)))
    return keys


def _router_has_sheet_rows_post(router: Any) -> bool:
    """
    Capability check: does this router provide any POST endpoint ending with /sheet-rows ?
    (We keep this intentionally broad to support prefix variations.)
    """
    for method, path in _router_keys(router):
        if method == "POST" and str(path).endswith("/sheet-rows"):
            return True
    return False


def _app_has_sheet_rows_post(app: Any) -> bool:
    for method, path in _collect_route_keys(app):
        if method == "POST" and str(path).endswith("/sheet-rows"):
            return True
    return False


# =============================================================================
# Schema-first preflight (mount-time only)
# =============================================================================
def _schema_preflight() -> Dict[str, Any]:
    """
    Best-effort import+validate schema registry at mount time.
    Never raises (caller decides strictness).
    """
    out: Dict[str, Any] = {"ok": False, "schema_version": None, "sheets_count": None, "digest": None, "error": None}
    try:
        from core.sheets import schema_registry as sr  # type: ignore

        out["schema_version"] = getattr(sr, "SCHEMA_VERSION", None)
        reg = getattr(sr, "SCHEMA_REGISTRY", None)
        out["sheets_count"] = len(reg) if isinstance(reg, dict) else None

        validate = getattr(sr, "validate_schema_registry", None)
        if callable(validate):
            validate()

        digest_fn = getattr(sr, "schema_registry_digest", None)
        if callable(digest_fn):
            out["digest"] = digest_fn()

        out["ok"] = True
        return out
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
        return out


# =============================================================================
# Family-aware mount planner
# =============================================================================
def _merge_expected_and_discovered(expected: Optional[Sequence[str]] = None) -> List[str]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    auto = _auto_discover_router_modules()
    merged = _dedupe_keep_order(list(exp) + list(auto))
    return merged


def _non_family_modules(mods: Sequence[str]) -> List[str]:
    famset = set(_ANALYSIS_FAMILY) | set(_ADVANCED_FAMILY) | set(_ADVISOR_FAMILY)
    return [m for m in mods if m not in famset]


def _existing_modules(mods: Sequence[str]) -> List[str]:
    return [m for m in mods if module_exists(m)]


def _sorted_by_group(mods: Sequence[str]) -> List[str]:
    # stable ordering: group priority then name
    return sorted(mods, key=lambda m: (_group_of(m).priority, m))


def _family_candidates(family: Sequence[str], mods_universe: Sequence[str]) -> List[str]:
    """
    Returns candidates in preference order, but only if present in merged universe.
    """
    u = set(mods_universe)
    return [m for m in family if m in u]


# =============================================================================
# Mount orchestrator
# =============================================================================
def mount_routers(app: Any, *, expected: Optional[Sequence[str]] = None, strict: Optional[bool] = None) -> Dict[str, Any]:
    strict_mode = coerce_bool(os.getenv("ROUTES_STRICT", ""), False) if strict is None else bool(strict)
    skip_dups = coerce_bool(os.getenv("ROUTES_SKIP_DUPLICATES", "1"), True)
    required = _required_set_from_env()

    universe = _merge_expected_and_discovered(expected)
    existing = _existing_modules(universe)

    report: Dict[str, Any] = {
        "mounted": [],
        "family_winners": {},
        "skipped_shadowed": {},
        "duplicates": {},
        "missing": [],
        "failed": [],
        "strict": strict_mode,
        "version": ROUTES_PACKAGE_VERSION,
        "timestamp_utc": _utc_iso(),
        "schema": _schema_preflight(),
        "coverage": {
            "has_sheet_rows_post": False,
            "missing_required_endpoints": [],
        },
        "preference_families": {
            "analysis_family": list(_ANALYSIS_FAMILY),
            "advanced_family": list(_ADVANCED_FAMILY),
            "advisor_family": list(_ADVISOR_FAMILY),
        },
        "universe": {
            "total_considered": len(universe),
            "total_existing": len(existing),
            "existing_modules": list(existing),
        },
    }

    if strict_mode and not report["schema"].get("ok"):
        raise RuntimeError(f"Schema preflight failed: {report['schema'].get('error')}")

    existing_keys = _collect_route_keys(app)

    def _mount_router_or_fn(module_name: str, router: Any, mount_fn: Optional[Callable[[Any], None]]) -> Tuple[bool, str]:
        """
        Shadow-safe mounting:
        - If router adds ZERO new routes (only duplicates), skip as shadowed
        - If router adds any new routes, mount and record duplicates (warn)
        """
        nonlocal existing_keys

        # Prefer router include if available
        if router is not None and hasattr(app, "include_router"):
            rkeys = _router_keys(router)
            dups = sorted([f"{m} {p}" for (m, p) in (rkeys & existing_keys)])
            uniq = rkeys - existing_keys

            if skip_dups and (not uniq) and dups:
                report["skipped_shadowed"][module_name] = dups[:50]
                _metrics.record_error(ErrorCategory.SHADOWED)
                return False, "shadowed_duplicates_only"

            try:
                app.include_router(router)
                # refresh keys
                try:
                    existing_keys |= rkeys
                except Exception:
                    existing_keys = _collect_route_keys(app)

                if dups:
                    report["duplicates"][module_name] = dups[:200]
                    _metrics.record_error(ErrorCategory.DUPLICATE)

                return True, "mounted_router"
            except Exception as e:
                return False, f"include_router_error:{_err_str(e)}"

        # Otherwise, try mount(app)
        if callable(mount_fn):
            try:
                before = _collect_route_keys(app)
                mount_fn(app)
                after = _collect_route_keys(app)
                added = after - before
                if not added:
                    # allow, but flag (mount did nothing)
                    report["skipped_shadowed"][module_name] = ["mount_fn added no routes"]
                    return False, "mount_no_routes"
                existing_keys = after
                return True, "mounted_mount_fn"
            except Exception as e:
                return False, f"mount_fn_error:{_err_str(e)}"

        return False, "no_mount_target"

    def _handle_missing_or_failed(info: ModuleInfo, module_name: str, is_required: bool) -> None:
        if info.status == ModuleStatus.MISSING:
            report["missing"].append(info)
            if strict_mode and is_required:
                raise RuntimeError(f"Required router missing: {module_name} ({info.error})")
        else:
            report["failed"].append(info)
            if strict_mode and is_required:
                raise RuntimeError(f"Required router failed: {module_name} ({info.error})")

    # -------------------------------------------------------------------------
    # 1) Mount NON-FAMILY modules first (security/system/core/schema etc.)
    # -------------------------------------------------------------------------
    nonfam = _sorted_by_group(_non_family_modules(existing))
    for m in nonfam:
        is_required = (m in required)
        router, mount_fn, info = load_mount_target(m)
        if router is None and mount_fn is None:
            _handle_missing_or_failed(info, m, is_required)
            continue

        ok, why = _mount_router_or_fn(m, router, mount_fn)
        if ok:
            report["mounted"].append(m)
        else:
            info.status = ModuleStatus.SKIPPED if why.startswith("shadowed") or why.startswith("mount_") else ModuleStatus.FAILED
            info.error = why
            info.error_category = ErrorCategory.SHADOWED if "shadowed" in why else ErrorCategory.UNKNOWN
            report["failed"].append(info)
            if strict_mode and is_required:
                raise RuntimeError(f"Required router could not be mounted: {m} ({why})")

    # -------------------------------------------------------------------------
    # 2) Mount FAMILY winners (capability-aware)
    # -------------------------------------------------------------------------
    def _mount_family(name: str, family: Sequence[str], require_sheet_rows: bool = False) -> None:
        candidates = _family_candidates(family, existing)
        for cand in candidates:
            is_required = (cand in required)
            router, mount_fn, info = load_mount_target(cand)
            if router is None and mount_fn is None:
                _handle_missing_or_failed(info, cand, is_required)
                continue

            # capability check when possible (router object)
            if require_sheet_rows and router is not None:
                if not _router_has_sheet_rows_post(router):
                    info.status = ModuleStatus.SKIPPED
                    info.error = "capability_miss(no POST */sheet-rows)"
                    info.error_category = ErrorCategory.COVERAGE
                    report["failed"].append(info)
                    _metrics.record_error(ErrorCategory.COVERAGE)
                    continue

            ok, why = _mount_router_or_fn(cand, router, mount_fn)
            if ok:
                report["mounted"].append(cand)
                report["family_winners"][name] = cand
                return

            # Not mounted -> try next candidate
            info.status = ModuleStatus.SKIPPED if "shadowed" in why else ModuleStatus.FAILED
            info.error = why
            info.error_category = ErrorCategory.SHADOWED if "shadowed" in why else ErrorCategory.UNKNOWN
            report["failed"].append(info)
            if strict_mode and is_required:
                raise RuntimeError(f"Required family router failed to mount: {cand} ({why})")

        # No winner
        report["family_winners"][name] = None
        if strict_mode:
            # only hard-fail if any of the family modules are marked required
            fam_required = [m for m in candidates if m in required]
            if fam_required:
                raise RuntimeError(f"Required family '{name}' has no mountable winner. Required: {fam_required}")

    # Analysis + Advanced must provide sheet-rows
    _mount_family("analysis", _ANALYSIS_FAMILY, require_sheet_rows=True)
    _mount_family("advanced", _ADVANCED_FAMILY, require_sheet_rows=True)
    _mount_family("advisor", _ADVISOR_FAMILY, require_sheet_rows=False)

    # -------------------------------------------------------------------------
    # 3) Coverage check + last-chance fallback for sheet-rows
    # -------------------------------------------------------------------------
    report["coverage"]["has_sheet_rows_post"] = _app_has_sheet_rows_post(app)
    if not report["coverage"]["has_sheet_rows_post"]:
        # last-chance: try any known sheet-rows modules not yet mounted (best-effort)
        fallbacks = _dedupe_keep_order(
            [
                "routes.analysis_sheet_rows",
                "routes.advanced_sheet_rows",
                "routes.sheet_rows",
                "routes.sheets",
                "routes.sheet_data",
            ]
        )
        for fb in fallbacks:
            if fb in report["mounted"]:
                continue
            if not module_exists(fb):
                continue
            router, mount_fn, info = load_mount_target(fb)
            if router is None and mount_fn is None:
                report["failed"].append(info)
                continue
            ok, why = _mount_router_or_fn(fb, router, mount_fn)
            if ok:
                report["mounted"].append(fb)
                report["coverage"]["has_sheet_rows_post"] = _app_has_sheet_rows_post(app)
                if report["coverage"]["has_sheet_rows_post"]:
                    report["coverage"]["recovered_by"] = fb
                    break
            else:
                info.status = ModuleStatus.SKIPPED if "shadowed" in why else ModuleStatus.FAILED
                info.error = why
                info.error_category = ErrorCategory.SHADOWED if "shadowed" in why else ErrorCategory.UNKNOWN
                report["failed"].append(info)

    if not report["coverage"]["has_sheet_rows_post"]:
        report["coverage"]["missing_required_endpoints"].append("POST */sheet-rows")
        _metrics.record_error(ErrorCategory.COVERAGE)
        if strict_mode:
            raise RuntimeError("Route coverage failure: missing POST */sheet-rows (no sheet rows router mounted)")

    return report


# =============================================================================
# Debug / Audit
# =============================================================================
def get_dependency_audit(expected: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    universe = _merge_expected_and_discovered(expected)
    discovery = {m: module_exists(m) for m in universe}
    available = [m for m, ok in discovery.items() if ok]
    missing = [m for m, ok in discovery.items() if not ok]
    status = "healthy" if available else "critical"
    return {
        "status": status,
        "total_expected": len(universe),
        "total_available": len(available),
        "total_missing": len(missing),
        "available_modules": sorted(available),
        "missing_modules": sorted(missing),
        "package_version": ROUTES_PACKAGE_VERSION,
        "auto_discover_enabled": coerce_bool(os.getenv("ROUTES_AUTO_DISCOVER", ""), False),
    }


def clear_module_cache() -> None:
    _find_spec_cached.cache_clear()


def get_cache_info() -> Dict[str, Any]:
    ci = _find_spec_cached.cache_info()
    denom = ci.hits + ci.misses
    return {
        "hits": ci.hits,
        "misses": ci.misses,
        "maxsize": ci.maxsize,
        "currsize": ci.currsize,
        "hit_rate": (ci.hits / denom) if denom else 0.0,
    }


def get_routes_debug_snapshot(expected: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    universe = _merge_expected_and_discovered(expected)
    existing = [m for m in universe if module_exists(m)]
    return {
        "version": ROUTES_PACKAGE_VERSION,
        "timestamp_utc": _utc_iso(),
        "schema": _schema_preflight(),
        "universe": {"total_considered": len(universe), "total_existing": len(existing), "existing_modules": existing},
        "audit": get_dependency_audit(expected),
        "metrics": get_metrics().get_stats(),
        "cache_info": get_cache_info(),
        "circuit_breaker": _circuit_breaker.snapshot(),
        "python": {"version": sys.version, "platform": sys.platform},
        "preference_families": {
            "analysis_family": list(_ANALYSIS_FAMILY),
            "advanced_family": list(_ADVANCED_FAMILY),
            "advisor_family": list(_ADVISOR_FAMILY),
        },
    }


# =============================================================================
# Exports
# =============================================================================
__all__ = [
    "ROUTES_PACKAGE_VERSION",
    "get_routes_version",
    "ModuleStatus",
    "ErrorCategory",
    "ModuleGroup",
    "ModuleInfo",
    "RouterSpec",
    "module_exists",
    "module_exists_async",
    "get_expected_router_modules",
    "load_mount_target",
    "load_mount_target_async",
    "mount_routers",
    "get_dependency_audit",
    "get_routes_debug_snapshot",
    "clear_module_cache",
    "get_cache_info",
    "get_metrics",
    "CircuitBreaker",
]
