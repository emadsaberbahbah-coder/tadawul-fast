#!/usr/bin/env python3
"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) — v2.8.0 (PHASE-8 ALIGNED)

Design goals
- ✅ Render/Prod-safe: zero network, zero heavy imports at module import-time
- ✅ Deterministic mount order (Phase-8 wiring, schema-first)
- ✅ Fast existence checks (find_spec) with LRU caching + async helpers
- ✅ Circuit breaker for IMPORT failures (not for mere "missing spec")
- ✅ Preference families (mount ONLY one router per family):
    - Analysis sheet-rows: ai_analysis > insights_analysis > analysis_sheet_rows
    - Advisor: investment_advisor > advisor
- ✅ Graceful degradation: missing/broken routers don't crash unless strict enabled
- ✅ Optional duplicate-route guard (prevents FastAPI crash on duplicate path+method)
- ✅ Rich debug snapshot for main.py /_debug/routes

Environment overrides (optional)
- ROUTES_EXPECTED="routes.config,routes.enriched_quote,..."
- ROUTES_AUTO_DISCOVER=1                   (scan package via pkgutil; off by default)
- ROUTES_STRICT=1                          (raise on failures for required modules)
- ROUTES_REQUIRED="routes.config,..."       (comma list; only used when ROUTES_STRICT=1)
- ROUTES_IMPORT_TIMEOUT_SEC=5              (async import timeout)
- ROUTES_CB_THRESHOLD=3                    (import circuit breaker threshold)
- ROUTES_CB_RESET_TIMEOUT_SEC=60           (circuit breaker reset timeout)
- ROUTES_SKIP_DUPLICATES=1                 (skip modules introducing duplicate routes)
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import os
import pkgutil
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache, wraps
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

ROUTES_PACKAGE_VERSION = "2.8.0"

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
    AVAILABLE = "available"  # spec exists (importable in principle)
    MISSING = "missing"  # spec not found
    ERROR = "error"  # unexpected error during check
    LOADED = "loaded"  # imported + router/mount extracted
    FAILED = "failed"  # import failed or router/mount missing


class ErrorCategory(Enum):
    NOT_FOUND = "not_found"
    IMPORT_ERROR = "import_error"
    ROUTER_MISSING = "router_missing"
    INVALID = "invalid"
    TIMEOUT = "timeout"
    DUPLICATE = "duplicate"
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

    # extraction
    router_attr: str = "router"
    mount_attr: str = "mount"
    router_found: bool = False
    mount_found: bool = False

    # optional info
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DiscoveryResult:
    module: str
    found: bool
    candidates_tried: List[str]
    time_taken_ms: float
    error: Optional[str] = None
    error_category: Optional[ErrorCategory] = None


@dataclass(slots=True)
class RouterSpec:
    """
    Minimal contract for mounting.
    - module: python module path
    - required: if strict mode enabled, failures raise for required only
    """
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

    if any(k in ml for k in ("advanced_analysis", "ai_analysis", "insights", "analysis_sheet_rows", "analysis")):
        return ModuleGroup.ANALYSIS

    if any(k in ml for k in ("investment_advisor", "advisor")):
        return ModuleGroup.ADVISOR

    if any(k in ml for k in ("argaam", "tadawul", "routes_argaam", "ksa", "saudi")):
        return ModuleGroup.KSA

    if any(k in ml for k in ("enriched", "quote", "quotes", "market", "engine")):
        return ModuleGroup.CORE

    return ModuleGroup.OTHER


# =============================================================================
# Defaults: Phase-8 expected routers (single-view)
# =============================================================================
def get_routes_version() -> str:
    return ROUTES_PACKAGE_VERSION


def get_expected_router_modules() -> List[str]:
    """
    Default expected router modules (Phase-8 wiring).
    Override via ROUTES_EXPECTED.
    """
    override = _parse_env_list("ROUTES_EXPECTED")
    if override:
        return [_normalize_module_name(x) for x in override if _normalize_module_name(x)]

    # Phase-8 single-view plan (ordered)
    return [
        # Core/config
        "routes.config",
        # Data sources
        "routes.enriched_quote",
        "routes.routes_argaam",
        # Schema-driven
        "routes.data_dictionary",
        # Analysis (preference family below will keep only one, but we keep all as candidates)
        "routes.ai_analysis",
        "routes.insights_analysis",
        "routes.analysis_sheet_rows",
        # Advanced
        "routes.advanced_analysis",
        # Top10
        "routes.top10_investments",
        # Advisor (prefer investment_advisor to avoid /v1/advisor duplicates)
        "routes.investment_advisor",
        "routes.advisor",
    ]


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
# Preference families (avoid duplicates)
# =============================================================================
_ANALYSIS_FAMILY = ["routes.ai_analysis", "routes.insights_analysis", "routes.analysis_sheet_rows"]
_ADVISOR_FAMILY = ["routes.investment_advisor", "routes.advisor"]


def _apply_preference_families(mods: List[str]) -> List[str]:
    """
    Keeps only the first available module in each preference family.
    Assumes mods are already ordered by preference.
    """
    s = [_normalize_module_name(x) for x in mods if _normalize_module_name(x)]
    out: List[str] = []
    seen: Set[str] = set()

    def is_in_family(m: str, fam: List[str]) -> bool:
        return m in fam

    # choose winners by first appearance
    family_winner: Dict[str, str] = {}
    for fam in (_ANALYSIS_FAMILY, _ADVISOR_FAMILY):
        winner = None
        for m in s:
            if m in fam and m not in seen:
                winner = m
                break
        if winner:
            family_winner["|".join(fam)] = winner

    for m in s:
        if m in seen:
            continue

        # analysis family
        if is_in_family(m, _ANALYSIS_FAMILY):
            if m != family_winner.get("|".join(_ANALYSIS_FAMILY)):
                continue

        # advisor family
        if is_in_family(m, _ADVISOR_FAMILY):
            if m != family_winner.get("|".join(_ADVISOR_FAMILY)):
                continue

        out.append(m)
        seen.add(m)

    return out


# =============================================================================
# Discovery / mount plan
# =============================================================================
@timed
def get_router_discovery(expected: Optional[Sequence[str]] = None) -> Dict[str, bool]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    auto = _auto_discover_router_modules()
    merged = list(dict.fromkeys([_normalize_module_name(x) for x in (exp + auto) if _normalize_module_name(x)]))
    merged = _apply_preference_families(merged)
    return {m: module_exists(m) for m in merged}


@timed
def get_available_router_modules(expected: Optional[Sequence[str]] = None) -> List[str]:
    disc = get_router_discovery(expected)
    return [m for m, ok in disc.items() if ok]


@timed
def get_mount_plan(expected: Optional[Sequence[str]] = None) -> List[str]:
    """
    Returns modules that exist, ordered and preference-filtered.
    """
    exp = list(expected) if expected is not None else get_expected_router_modules()
    auto = _auto_discover_router_modules()

    merged = list(dict.fromkeys([_normalize_module_name(x) for x in (exp + auto) if _normalize_module_name(x)]))
    merged = _apply_preference_families(merged)

    # keep only existing
    existing = [m for m in merged if module_exists(m)]

    # stable ordering by group priority but preserve within-group order of "merged"
    # (important for Phase-8 wiring)
    idx = {m: i for i, m in enumerate(merged)}
    existing.sort(key=lambda m: (_group_of(m).priority, idx.get(m, 10_000), m))
    return existing


@timed
def get_expected_router_groups() -> Dict[str, List[str]]:
    groups = defaultdict(list)
    for mod in get_expected_router_modules():
        groups[_group_of(mod).value].append(mod)
    return {k: v[:] for k, v in groups.items()}


def _required_set_from_env() -> Set[str]:
    req = set(_parse_env_list("ROUTES_REQUIRED"))
    return {_normalize_module_name(x) for x in req if _normalize_module_name(x)}


@timed
def get_mount_plan_detailed(expected: Optional[Sequence[str]] = None) -> List[ModuleInfo]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    plan = get_mount_plan(exp)
    required = _required_set_from_env()

    out: List[ModuleInfo] = []
    now = _utc_iso()

    for m in plan:
        ok, err = _find_spec_cached(m)
        info = ModuleInfo(
            name=m,
            group=_group_of(m),
            status=ModuleStatus.AVAILABLE if ok else ModuleStatus.MISSING,
            error=None if ok else err,
            error_category=None if ok else ErrorCategory.NOT_FOUND,
            last_checked_utc=now,
            metrics={"required": bool(m in required)},
        )
        out.append(info)

    return out


def build_router_specs(expected: Optional[Sequence[str]] = None) -> List[RouterSpec]:
    required = _required_set_from_env()
    mods = get_mount_plan(expected)
    specs = [RouterSpec(module=m, group=_group_of(m), required=(m in required)) for m in mods]
    specs.sort(key=lambda x: (x.group.priority, x.module))
    return specs


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


def load_mount_target(module: str, *, router_attr: str = "router", mount_attr: str = "mount") -> Tuple[Optional[Any], Optional[Callable[[Any], None]], ModuleInfo]:
    """
    Imports module and extracts either:
      - router object (preferred) OR
      - mount(app) callable
    Returns: (router, mount_fn, info)
    """
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
# Duplicate-route guard (optional)
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


# =============================================================================
# Mount orchestrator (optional usage)
# =============================================================================
def mount_routers(app: Any, *, expected: Optional[Sequence[str]] = None, strict: Optional[bool] = None) -> Dict[str, Any]:
    """
    Mount routers into a FastAPI app (or compatible include_router()).

    Returns:
      {
        mounted: [module,...],
        skipped_duplicates: {module:[...],...},
        missing: [ModuleInfo,...],
        failed: [ModuleInfo,...],
        strict: bool
      }
    """
    strict_mode = coerce_bool(os.getenv("ROUTES_STRICT", ""), False) if strict is None else bool(strict)
    skip_dups = coerce_bool(os.getenv("ROUTES_SKIP_DUPLICATES", "1"), True)
    required = _required_set_from_env()

    report: Dict[str, Any] = {
        "mounted": [],
        "skipped_duplicates": {},
        "missing": [],
        "failed": [],
        "strict": strict_mode,
        "version": ROUTES_PACKAGE_VERSION,
        "timestamp_utc": _utc_iso(),
    }

    existing_keys = _collect_route_keys(app)

    for spec in build_router_specs(expected):
        router, mount_fn, info = load_mount_target(spec.module)

        if router is None and mount_fn is None:
            if info.status == ModuleStatus.MISSING:
                report["missing"].append(info)
                if strict_mode and (spec.required or spec.module in required):
                    raise RuntimeError(f"Required router missing: {spec.module} ({info.error})")
            else:
                report["failed"].append(info)
                if strict_mode and (spec.required or spec.module in required):
                    raise RuntimeError(f"Required router failed: {spec.module} ({info.error})")
            continue

        # Duplicate guard if router object exists
        if router is not None and skip_dups:
            try:
                keys = _router_keys(router)
                dups = sorted([f"{m} {p}" for (m, p) in (keys & existing_keys)])
                if dups:
                    info.status = ModuleStatus.FAILED
                    info.error = "duplicate routes"
                    info.error_category = ErrorCategory.DUPLICATE
                    report["skipped_duplicates"][spec.module] = dups
                    _metrics.record_error(ErrorCategory.DUPLICATE)
                    if strict_mode and (spec.required or spec.module in required):
                        raise RuntimeError(f"Duplicate routes for required module: {spec.module} ({dups[:5]})")
                    continue
            except Exception:
                # if we can't compute keys, proceed (best-effort)
                pass

        try:
            if router is not None and hasattr(app, "include_router"):
                app.include_router(router)
                if router is not None and skip_dups:
                    try:
                        existing_keys |= _router_keys(router)
                    except Exception:
                        pass
                report["mounted"].append(spec.module)
                continue

            if callable(mount_fn):
                mount_fn(app)
                existing_keys = _collect_route_keys(app)
                report["mounted"].append(spec.module)
                continue

            info.status = ModuleStatus.FAILED
            info.error = "no usable mount target"
            info.error_category = ErrorCategory.ROUTER_MISSING
            report["failed"].append(info)
            if strict_mode and (spec.required or spec.module in required):
                raise RuntimeError(f"Required router has no mount target: {spec.module}")

        except Exception as e:
            info.status = ModuleStatus.FAILED
            info.error = _err_str(e)
            info.error_category = ErrorCategory.UNKNOWN
            report["failed"].append(info)
            _metrics.record_error(ErrorCategory.UNKNOWN)
            if strict_mode and (spec.required or spec.module in required):
                raise

    return report


# =============================================================================
# Debug / Audit
# =============================================================================
def get_dependency_audit(expected: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    auto = _auto_discover_router_modules()
    merged = list(dict.fromkeys([_normalize_module_name(x) for x in (exp + auto) if _normalize_module_name(x)]))
    merged = _apply_preference_families(merged)

    discovery = {m: module_exists(m) for m in merged}
    available = [m for m, ok in discovery.items() if ok]
    missing = [m for m, ok in discovery.items() if not ok]

    status = "healthy" if available else "critical"
    return {
        "status": status,
        "total_expected": len(merged),
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


def get_routes_debug_snapshot() -> Dict[str, Any]:
    return {
        "version": ROUTES_PACKAGE_VERSION,
        "timestamp_utc": _utc_iso(),
        "expected": get_expected_router_modules(),
        "mount_plan": get_mount_plan(),
        "audit": get_dependency_audit(),
    }


def get_routes_debug_snapshot_enhanced() -> Dict[str, Any]:
    base = get_routes_debug_snapshot()
    base.update(
        {
            "metrics": get_metrics().get_stats(),
            "cache_info": get_cache_info(),
            "circuit_breaker": _circuit_breaker.snapshot(),
            "python": {"version": sys.version, "platform": sys.platform},
            "preference_families": {
                "analysis_family": _ANALYSIS_FAMILY,
                "advisor_family": _ADVISOR_FAMILY,
            },
        }
    )
    return base


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
    "DiscoveryResult",
    "RouterSpec",
    # existence checks
    "module_exists",
    "module_exists_async",
    # discovery / mount plan
    "get_expected_router_modules",
    "get_expected_router_groups",
    "get_available_router_modules",
    "get_router_discovery",
    "get_mount_plan",
    "get_mount_plan_detailed",
    "build_router_specs",
    # import/mount
    "load_mount_target",
    "load_mount_target_async",
    "mount_routers",
    # audit/debug
    "get_dependency_audit",
    "get_routes_debug_snapshot",
    "get_routes_debug_snapshot_enhanced",
    "clear_module_cache",
    "get_cache_info",
    "get_metrics",
    "CircuitBreaker",
]
