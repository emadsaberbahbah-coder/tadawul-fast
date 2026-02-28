#!/usr/bin/env python3
"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) — v2.6.0 (TFB Aligned)

Design goals
- ✅ Render/Prod-safe: zero network, zero heavy imports at module import-time
- ✅ Deterministic mount order via groups + priorities
- ✅ Fast existence checks (find_spec) with caching + async helpers
- ✅ Circuit breaker for IMPORT failures (not for mere "missing")
- ✅ Dynamic router discovery (optional) + static expected list (default, repo-aligned)
- ✅ Graceful degradation: missing/broken routers don't crash unless strict mode enabled
- ✅ Rich debug snapshot for health endpoints / logs

TFB alignment (default expected routers)
- routes.config
- routes.enriched_quote
- routes.advanced_analysis
- routes.ai_analysis
- routes.advisor
- routes.investment_advisor
- routes.routes_argaam

Environment overrides (optional)
- ROUTES_EXPECTED="routes.config,routes.enriched_quote,..."
- ROUTES_AUTO_DISCOVER=1             (scan package via pkgutil; off by default)
- ROUTES_STRICT=1                    (raise on import failures for "required" modules)
- ROUTES_REQUIRED="routes.config,..." (comma list; only used when ROUTES_STRICT=1)
- ROUTES_IMPORT_TIMEOUT_SEC=5        (async import timeout)
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

ROUTES_PACKAGE_VERSION = "2.6.0"

# =============================================================================
# Enums and Types
# =============================================================================
class ModuleStatus(Enum):
    AVAILABLE = "available"   # spec exists (importable in principle)
    MISSING = "missing"       # spec not found
    ERROR = "error"           # unexpected error during check
    LOADED = "loaded"         # imported + router extracted
    FAILED = "failed"         # import failed or router attr missing


class ErrorCategory(Enum):
    NOT_FOUND = "not_found"
    IMPORT_ERROR = "import_error"
    ROUTER_MISSING = "router_missing"
    INVALID = "invalid"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ModuleGroup(Enum):
    SECURITY = "security"
    SYSTEM = "system"
    CORE = "core"
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
            "analysis": 3,
            "advisor": 4,
            "ksa": 5,
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
    router_found: bool = False
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
    - router_attr: attribute to fetch (default "router")
    - required: if strict mode is enabled, failures raise for required routers only
    """
    module: str
    group: ModuleGroup
    router_attr: str = "router"
    required: bool = False


# =============================================================================
# Metrics (lightweight, no deps)
# =============================================================================
class RoutesMetrics:
    def __init__(self):
        self._start = time.time()
        self._discovery_ms: List[float] = []
        self._exists_checks: Dict[str, int] = {}
        self._import_attempts: Dict[str, int] = {}
        self._errors: Dict[str, int] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    def record_discovery(self, ms: float) -> None:
        self._discovery_ms.append(float(ms))

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
        total = len(self._discovery_ms)
        avg = (sum(self._discovery_ms) / total) if total else 0.0
        denom = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / denom) if denom else 0.0
        return {
            "uptime_seconds": time.time() - self._start,
            "discoveries": total,
            "avg_discovery_time_ms": avg,
            "exists_checks_unique": len(self._exists_checks),
            "exists_checks_total": sum(self._exists_checks.values()),
            "import_attempts_unique": len(self._import_attempts),
            "import_attempts_total": sum(self._import_attempts.values()),
            "cache_hit_rate": hit_rate,
            "errors_by_category": dict(self._errors),
        }

    def reset(self) -> None:
        self.__init__()


_metrics = RoutesMetrics()


def get_metrics() -> RoutesMetrics:
    return _metrics


# =============================================================================
# Timing decorators
# =============================================================================
def timed(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            _metrics.record_discovery((time.perf_counter() - start) * 1000.0)
    return wrapper


def async_timed(func: Callable[..., Awaitable]) -> Callable[..., Awaitable]:
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return await func(*args, **kwargs)
        finally:
            _metrics.record_discovery((time.perf_counter() - start) * 1000.0)
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
            # auto reset
            self._open.discard(module)
            self._failures.pop(module, None)
            self._last_failure.pop(module, None)
            return False
        return True

    @contextmanager
    def guard(self, module: str):
        if self.is_open(module):
            raise RuntimeError(f"Circuit breaker open for module: {module}")
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
# Existence checks
# =============================================================================
def _normalize_module_name(module_path: str) -> str:
    if not module_path:
        return ""
    s = str(module_path).strip()
    if not s:
        return ""
    # allow "routes/foo.py" or "routes\\foo.py"
    s = s.replace("\\", "/")
    if s.endswith(".py"):
        s = s[:-3]
    if s.endswith("/__init__"):
        s = s[:-9]
    s = s.replace("/", ".")
    return s.strip(".")


@lru_cache(maxsize=4096)
def _find_spec_cached(module_name: str) -> Tuple[bool, Optional[str]]:
    """
    Returns (exists, error_message). Never raises.
    """
    mn = _normalize_module_name(module_name)
    if not mn:
        return False, "empty module name"
    try:
        spec = importlib.util.find_spec(mn)
        if spec is None:
            return False, "spec not found"
        # namespace pkg is OK
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

    # no-cache path: do not clear global cache (avoid perf cliff)
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


@timed
def module_exists_any(candidates: Sequence[str]) -> Tuple[bool, Optional[str]]:
    for c in candidates or []:
        if module_exists(c):
            return True, c
    return False, None


@async_timed
async def module_exists_any_async(candidates: Sequence[str], *, timeout: float = 5.0) -> Tuple[bool, Optional[str]]:
    if not candidates:
        return False, None
    tasks = [module_exists_async(c, timeout=timeout) for c in candidates]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for c, r in zip(candidates, results):
        if isinstance(r, bool) and r:
            return True, c
    return False, None


# =============================================================================
# Grouping (TFB-aligned)
# =============================================================================
def _group_of(module_name: str) -> ModuleGroup:
    ml = (module_name or "").lower()

    if any(k in ml for k in ("auth", "security", "middleware", "rate", "jwt", "token", "cors")):
        return ModuleGroup.SECURITY

    if any(k in ml for k in ("health", "status", "ping", "meta", "version", "config", "ready", "live")):
        return ModuleGroup.SYSTEM

    if any(k in ml for k in ("advanced_analysis", "ai_analysis", "analysis")):
        return ModuleGroup.ANALYSIS

    if any(k in ml for k in ("advisor", "investment_advisor", "adviser")):
        return ModuleGroup.ADVISOR

    if any(k in ml for k in ("argaam", "tadawul", "routes_argaam", "ksa", "saudi")):
        return ModuleGroup.KSA

    if any(k in ml for k in ("enriched", "quote", "quotes", "market", "engine")):
        return ModuleGroup.CORE

    return ModuleGroup.OTHER


# =============================================================================
# Discovery (expected list + optional auto-discover)
# =============================================================================
def _parse_env_list(name: str) -> List[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def get_routes_version() -> str:
    return ROUTES_PACKAGE_VERSION


def get_expected_router_modules() -> List[str]:
    """
    Default: repo-aligned list (based on your project structure).
    Override via ROUTES_EXPECTED.
    """
    override = _parse_env_list("ROUTES_EXPECTED")
    if override:
        return [_normalize_module_name(x) for x in override if _normalize_module_name(x)]

    # TFB aligned defaults (do NOT include non-existent "routes.health" unless you have it)
    return [
        "routes.config",
        "routes.enriched_quote",
        "routes.advanced_analysis",
        "routes.ai_analysis",
        "routes.advisor",
        "routes.investment_advisor",
        "routes.routes_argaam",
    ]


def get_expected_router_groups() -> Dict[str, List[str]]:
    groups = defaultdict(list)
    for mod in get_expected_router_modules():
        groups[_group_of(mod).value].append(mod)
    # stable ordering
    return {k: sorted(v) for k, v in groups.items()}


def _auto_discover_router_modules() -> List[str]:
    """
    Optional discovery by scanning the 'routes' package.
    Safe: only lists module names; no imports.
    Enabled with ROUTES_AUTO_DISCOVER=1.
    """
    auto = coerce_bool(os.getenv("ROUTES_AUTO_DISCOVER", ""), False)
    if not auto:
        return []
    try:
        # this is our own package; should exist
        import routes as _routes_pkg  # type: ignore

        discovered: List[str] = []
        for m in pkgutil.iter_modules(_routes_pkg.__path__, prefix="routes."):
            name = getattr(m, "name", "")
            if not name:
                continue
            # ignore private modules
            if name.split(".")[-1].startswith("_"):
                continue
            discovered.append(name)
        return sorted(set(discovered))
    except Exception:
        _metrics.record_error(ErrorCategory.UNKNOWN)
        return []


def get_router_discovery(expected: Optional[Sequence[str]] = None) -> Dict[str, bool]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    exp2 = [_normalize_module_name(x) for x in exp if _normalize_module_name(x)]
    return {m: module_exists(m) for m in exp2}


def get_available_router_modules(expected: Optional[Sequence[str]] = None) -> List[str]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    exp2 = [_normalize_module_name(x) for x in exp if _normalize_module_name(x)]
    return [m for m in exp2 if module_exists(m)]


# =============================================================================
# Batch checking
# =============================================================================
@timed
def check_modules_batch(modules: Sequence[str]) -> Dict[str, DiscoveryResult]:
    results: Dict[str, DiscoveryResult] = {}
    for module in modules or []:
        m = _normalize_module_name(module)
        start = time.perf_counter()
        ok = module_exists(m)
        elapsed = (time.perf_counter() - start) * 1000.0
        results[m] = DiscoveryResult(module=m, found=ok, candidates_tried=[m], time_taken_ms=elapsed)
    return results


@async_timed
async def check_modules_batch_async(modules: Sequence[str], *, timeout: float = 5.0) -> Dict[str, DiscoveryResult]:
    mods = [_normalize_module_name(m) for m in (modules or []) if _normalize_module_name(m)]
    if not mods:
        return {}
    async def one(m: str) -> DiscoveryResult:
        start = time.perf_counter()
        ok = await module_exists_async(m, timeout=timeout)
        elapsed = (time.perf_counter() - start) * 1000.0
        return DiscoveryResult(module=m, found=ok, candidates_tried=[m], time_taken_ms=elapsed)
    res_list = await asyncio.gather(*[one(m) for m in mods], return_exceptions=True)
    out: Dict[str, DiscoveryResult] = {}
    for m, r in zip(mods, res_list):
        if isinstance(r, DiscoveryResult):
            out[m] = r
        else:
            out[m] = DiscoveryResult(module=m, found=False, candidates_tried=[m], time_taken_ms=0.0, error=str(r), error_category=ErrorCategory.UNKNOWN)
    return out


# =============================================================================
# Mount plan (detailed + simple)
# =============================================================================
def _required_set_from_env() -> Set[str]:
    req = set(_parse_env_list("ROUTES_REQUIRED"))
    return {_normalize_module_name(x) for x in req if _normalize_module_name(x)}


def get_mount_plan_detailed(expected: Optional[Sequence[str]] = None) -> List[ModuleInfo]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    auto = _auto_discover_router_modules()
    merged = list(dict.fromkeys([_normalize_module_name(x) for x in (exp + auto) if _normalize_module_name(x)]))

    required = _required_set_from_env()
    batch = check_modules_batch(merged)

    plan: List[ModuleInfo] = []
    now = datetime.now(timezone.utc).isoformat()

    for module in merged:
        result = batch.get(module)
        found = bool(result.found) if result else False
        group = _group_of(module)

        info = ModuleInfo(
            name=module,
            group=group,
            status=ModuleStatus.AVAILABLE if found else ModuleStatus.MISSING,
            last_checked_utc=now,
            router_attr="router",
            router_found=False,
            metrics={"check_time_ms": float(result.time_taken_ms) if result else 0.0, "required": bool(module in required)},
        )
        plan.append(info)

    plan.sort(key=lambda x: (x.group.priority, x.name))
    return plan


def get_mount_plan(expected: Optional[Sequence[str]] = None) -> List[str]:
    detailed = get_mount_plan_detailed(expected)
    return [m.name for m in detailed if m.status == ModuleStatus.AVAILABLE]


# =============================================================================
# Safe import + router extraction
# =============================================================================
def _classify_import_error(e: Exception) -> ErrorCategory:
    if isinstance(e, ModuleNotFoundError):
        return ErrorCategory.NOT_FOUND
    if isinstance(e, ImportError):
        return ErrorCategory.IMPORT_ERROR
    return ErrorCategory.UNKNOWN


def load_router(module: str, *, router_attr: str = "router") -> Tuple[Optional[Any], ModuleInfo]:
    """
    Import module and extract router attr.
    Never imports at package import-time; called by app boot.
    """
    m = _normalize_module_name(module)
    info = ModuleInfo(name=m, group=_group_of(m), status=ModuleStatus.MISSING, router_attr=router_attr, router_found=False)
    info.last_checked_utc = datetime.now(timezone.utc).isoformat()

    # existence first
    ok, err = _find_spec_cached(m)
    _metrics.record_exists_check(m, hit_cache=True)
    if not ok:
        info.status = ModuleStatus.MISSING
        info.error = err
        info.error_category = ErrorCategory.NOT_FOUND
        return None, info

    # circuit breaker for imports
    if _circuit_breaker.is_open(m):
        info.status = ModuleStatus.FAILED
        info.error = "Circuit breaker open (import failures)"
        info.error_category = ErrorCategory.IMPORT_ERROR
        _metrics.record_error(ErrorCategory.IMPORT_ERROR)
        return None, info

    start = time.perf_counter()
    try:
        with _circuit_breaker.guard(m):
            _metrics.record_import(m)
            mod = importlib.import_module(m)
            router = getattr(mod, router_attr, None)
            if router is None:
                info.status = ModuleStatus.FAILED
                info.error = f"Router attribute '{router_attr}' not found"
                info.error_category = ErrorCategory.ROUTER_MISSING
                _metrics.record_error(ErrorCategory.ROUTER_MISSING)
                return None, info

            info.status = ModuleStatus.LOADED
            info.router_found = True
            return router, info
    except Exception as e:
        info.status = ModuleStatus.FAILED
        info.error = str(e)
        info.error_category = _classify_import_error(e)
        _metrics.record_error(info.error_category)
        return None, info
    finally:
        info.load_time_ms = (time.perf_counter() - start) * 1000.0


async def load_router_async(module: str, *, router_attr: str = "router", timeout: Optional[float] = None) -> Tuple[Optional[Any], ModuleInfo]:
    t = float(timeout if timeout is not None else float(os.getenv("ROUTES_IMPORT_TIMEOUT_SEC", "5") or "5"))
    try:
        return await asyncio.wait_for(asyncio.to_thread(load_router, module, router_attr=router_attr), timeout=t)
    except asyncio.TimeoutError:
        info = ModuleInfo(
            name=_normalize_module_name(module),
            group=_group_of(module),
            status=ModuleStatus.FAILED,
            error="import timeout",
            error_category=ErrorCategory.TIMEOUT,
            router_attr=router_attr,
            router_found=False,
            last_checked_utc=datetime.now(timezone.utc).isoformat(),
        )
        _metrics.record_error(ErrorCategory.TIMEOUT)
        return None, info


def build_router_specs(expected: Optional[Sequence[str]] = None) -> List[RouterSpec]:
    """
    Convert discovery plan into RouterSpec list.
    Required modules are controlled by ROUTES_REQUIRED (only used with ROUTES_STRICT=1).
    """
    required = _required_set_from_env()
    mods = get_mount_plan(expected)
    specs = []
    for m in mods:
        specs.append(RouterSpec(module=m, group=_group_of(m), router_attr="router", required=(m in required)))
    specs.sort(key=lambda x: (x.group.priority, x.module))
    return specs


def mount_routers(app: Any, *, expected: Optional[Sequence[str]] = None, strict: Optional[bool] = None) -> Dict[str, Any]:
    """
    Mount routers into a FastAPI app (or compatible object exposing include_router()).

    Returns a report dict:
      {mounted: [...], failed: [...], missing: [...], strict: bool}
    """
    strict_mode = coerce_bool(os.getenv("ROUTES_STRICT", ""), False) if strict is None else bool(strict)
    required = _required_set_from_env()

    report = {"mounted": [], "failed": [], "missing": [], "strict": strict_mode}

    for spec in build_router_specs(expected):
        router, info = load_router(spec.module, router_attr=spec.router_attr)
        if router is not None:
            try:
                app.include_router(router)  # FastAPI style
                report["mounted"].append(spec.module)
            except Exception as e:
                info.status = ModuleStatus.FAILED
                info.error = f"include_router failed: {e}"
                info.error_category = ErrorCategory.UNKNOWN
                report["failed"].append(info)
                if strict_mode and (spec.required or spec.module in required):
                    raise
        else:
            # distinguish missing vs failed
            if info.status == ModuleStatus.MISSING:
                report["missing"].append(info)
                if strict_mode and (spec.required or spec.module in required):
                    raise RuntimeError(f"Required router missing: {spec.module} ({info.error})")
            else:
                report["failed"].append(info)
                if strict_mode and (spec.required or spec.module in required):
                    raise RuntimeError(f"Required router failed: {spec.module} ({info.error})")

    return report


# =============================================================================
# Debug / Audit
# =============================================================================
def get_dependency_audit(expected: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    exp = list(expected) if expected is not None else get_expected_router_modules()
    auto = _auto_discover_router_modules()
    merged = list(dict.fromkeys([_normalize_module_name(x) for x in (exp + auto) if _normalize_module_name(x)]))

    discovery = get_router_discovery(merged)
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
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "expected_groups": get_expected_router_groups(),
        "discovery": get_router_discovery(),
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
    "module_exists_any",
    "module_exists_any_async",
    "check_modules_batch",
    "check_modules_batch_async",
    # discovery/mount plan
    "get_expected_router_modules",
    "get_expected_router_groups",
    "get_available_router_modules",
    "get_router_discovery",
    "get_mount_plan",
    "get_mount_plan_detailed",
    "build_router_specs",
    # import/mount
    "load_router",
    "load_router_async",
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
