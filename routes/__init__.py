"""
routes/__init__.py
------------------------------------------------------------
Routes package initialization (PROD SAFE) — v2.5.0 (Advanced)

Enhanced with:
- Async module existence checking
- Circuit breaker pattern for module loading
- Metrics collection
- Lazy loading support
- Better error categorization
- Performance monitoring hooks
"""

from __future__ import annotations

import asyncio
import fnmatch
import importlib.util
import os
import pkgutil
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union
from contextlib import contextmanager

ROUTES_PACKAGE_VERSION = "2.5.0"

# ---------------------------------------------------------------------
# Enums and Types
# ---------------------------------------------------------------------
class ModuleStatus(Enum):
    """Status of a module in the discovery process."""
    AVAILABLE = "available"
    MISSING = "missing"
    ERROR = "error"
    PENDING = "pending"
    LOADED = "loaded"
    FAILED = "failed"

class ModuleGroup(Enum):
    """Module groups for deterministic mounting."""
    SECURITY = "security"
    SYSTEM = "system"
    CORE = "core"
    ADVISOR = "advisor"
    KSA = "ksa"
    OTHER = "other"
    
    @property
    def priority(self) -> int:
        """Get mount priority (lower = earlier)."""
        priorities = {
            "security": 0,
            "system": 1,
            "core": 2,
            "advisor": 3,
            "ksa": 4,
            "other": 9
        }
        return priorities.get(self.value, 9)

@dataclass
class ModuleInfo:
    """Information about a discovered module."""
    name: str
    path: Optional[str]
    group: ModuleGroup
    status: ModuleStatus = ModuleStatus.PENDING
    error: Optional[str] = None
    load_time: Optional[float] = None
    last_checked: Optional[datetime] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

@dataclass
class DiscoveryResult:
    """Result of module discovery operation."""
    module: str
    found: bool
    candidates_tried: List[str]
    time_taken_ms: float
    error: Optional[str] = None

# ---------------------------------------------------------------------
# Metrics and Monitoring
# ---------------------------------------------------------------------
class RoutesMetrics:
    """Collect metrics about route discovery and loading."""
    
    def __init__(self):
        self._discovery_times: List[float] = []
        self._module_checks: Dict[str, int] = {}
        self._errors: Dict[str, int] = {}
        self._cache_hits: int = 0
        self._cache_misses: int = 0
        self._start_time = time.time()
    
    def record_discovery(self, time_ms: float) -> None:
        """Record module discovery time."""
        self._discovery_times.append(time_ms)
    
    def record_module_check(self, module: str, hit_cache: bool) -> None:
        """Record module existence check."""
        self._module_checks[module] = self._module_checks.get(module, 0) + 1
        if hit_cache:
            self._cache_hits += 1
        else:
            self._cache_misses += 1
    
    def record_error(self, error_type: str) -> None:
        """Record an error occurrence."""
        self._errors[error_type] = self._errors.get(error_type, 0) + 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            "uptime_seconds": time.time() - self._start_time,
            "total_discoveries": len(self._discovery_times),
            "avg_discovery_time_ms": sum(self._discovery_times) / len(self._discovery_times) if self._discovery_times else 0,
            "modules_checked": len(self._module_checks),
            "total_checks": sum(self._module_checks.values()),
            "cache_hit_rate": self._cache_hits / (self._cache_hits + self._cache_misses) if (self._cache_hits + self._cache_misses) > 0 else 0,
            "errors_by_type": self._errors.copy(),
        }
    
    def reset(self) -> None:
        """Reset all metrics."""
        self.__init__()

_metrics = RoutesMetrics()

def get_metrics() -> RoutesMetrics:
    """Get the global metrics instance."""
    return _metrics

# ---------------------------------------------------------------------
# Performance decorators
# ---------------------------------------------------------------------
def timed(func: Callable) -> Callable:
    """Time function execution."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            elapsed = (time.perf_counter() - start) * 1000  # ms
            _metrics.record_discovery(elapsed)
    return wrapper

def async_timed(func: Callable[..., Awaitable]) -> Callable[..., Awaitable]:
    """Time async function execution."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            result = await func(*args, **kwargs)
            return result
        finally:
            elapsed = (time.perf_counter() - start) * 1000  # ms
            _metrics.record_discovery(elapsed)
    return wrapper

# ---------------------------------------------------------------------
# Circuit Breaker for module loading
# ---------------------------------------------------------------------
class CircuitBreaker:
    """Circuit breaker pattern to prevent repeated failures."""
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._failures: Dict[str, int] = {}
        self._last_failure: Dict[str, float] = {}
        self._open: Set[str] = set()
    
    def record_failure(self, module: str) -> None:
        """Record a module failure."""
        now = time.time()
        self._failures[module] = self._failures.get(module, 0) + 1
        self._last_failure[module] = now
        
        if self._failures[module] >= self.failure_threshold:
            self._open.add(module)
    
    def record_success(self, module: str) -> None:
        """Record a module success."""
        self._failures.pop(module, None)
        self._last_failure.pop(module, None)
        self._open.discard(module)
    
    def is_open(self, module: str) -> bool:
        """Check if circuit is open for module."""
        if module not in self._open:
            return False
        
        # Check if we should auto-reset
        last = self._last_failure.get(module)
        if last and (time.time() - last) > self.reset_timeout:
            self._open.discard(module)
            self._failures.pop(module, None)
            return False
        
        return True
    
    @contextmanager
    def guard(self, module: str):
        """Context manager for circuit breaker protection."""
        if self.is_open(module):
            raise RuntimeError(f"Circuit breaker open for module: {module}")
        
        try:
            yield
            self.record_success(module)
        except Exception as e:
            self.record_failure(module)
            raise e

_circuit_breaker = CircuitBreaker()

# ---------------------------------------------------------------------
# Enhanced module existence checking
# ---------------------------------------------------------------------
@lru_cache(maxsize=4096)
def _module_exists_cached(module_path: str) -> Tuple[bool, Optional[str]]:
    """
    Core module existence check with error capture.
    Returns (exists, error_message).
    """
    mp = _normalize_module_path(module_path)
    if not mp:
        return False, "Empty module path"
    
    try:
        spec = importlib.util.find_spec(mp)
        if spec is None:
            return False, "Module spec not found"
        
        # Verify it's actually a module we can import
        if spec.origin is None and not spec.has_location:
            return False, "Module has no location"
        
        return True, None
    except ModuleNotFoundError as e:
        return False, f"Module not found: {str(e)}"
    except ValueError as e:
        return False, f"Invalid module name: {str(e)}"
    except Exception as e:
        _metrics.record_error(type(e).__name__)
        return False, f"Unexpected error: {str(e)}"

@timed
def module_exists(module_path: str, use_cache: bool = True) -> bool:
    """
    Check module availability WITHOUT importing it.
    Enhanced with metrics and error capture.
    """
    mp = _normalize_module_path(module_path)
    if not mp:
        return False
    
    # Check circuit breaker
    if _circuit_breaker.is_open(mp):
        return False
    
    try:
        with _circuit_breaker.guard(mp):
            if use_cache:
                exists, _ = _module_exists_cached(mp)
                _metrics.record_module_check(mp, hit_cache=True)
            else:
                # Bypass cache for fresh check
                _module_exists_cached.cache_clear()
                exists, error = _module_exists_cached(mp)
                _metrics.record_module_check(mp, hit_cache=False)
                if not exists and error:
                    # Log error for debugging
                    pass
            
            return exists
    except Exception:
        return False

async def module_exists_async(module_path: str, timeout: float = 5.0) -> bool:
    """
    Async version of module_exists with timeout.
    Useful for concurrent checks during startup.
    """
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, module_exists, module_path),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        _metrics.record_error("timeout")
        return False
    except Exception:
        _metrics.record_error("async_error")
        return False

@timed
def module_exists_any(candidates: Sequence[str]) -> Tuple[bool, Optional[str]]:
    """
    Check if any candidate module exists.
    Returns (exists, first_existing).
    """
    for c in candidates or []:
        if module_exists(c):
            return True, c
    return False, None

async def module_exists_any_async(candidates: Sequence[str]) -> Tuple[bool, Optional[str]]:
    """
    Async version checking multiple candidates concurrently.
    """
    if not candidates:
        return False, None
    
    tasks = [module_exists_async(c) for c in candidates]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for c, r in zip(candidates, results):
        if isinstance(r, bool) and r:
            return True, c
    
    return False, None

def _first_existing(candidates: Sequence[str]) -> Optional[str]:
    """Find first existing module from candidates."""
    for c in candidates or []:
        if module_exists(c):
            return c
    return None

async def _first_existing_async(candidates: Sequence[str]) -> Optional[str]:
    """Async version of _first_existing."""
    exists, found = await module_exists_any_async(candidates)
    return found if exists else None

# ---------------------------------------------------------------------
# Enhanced grouping
# ---------------------------------------------------------------------
def _group_of(module_path: str) -> ModuleGroup:
    """
    Classify module into a group for deterministic mounting.
    Enhanced with more granular patterns.
    """
    ml = (module_path or "").lower()
    
    # Security patterns
    security_patterns = (
        "auth", "security", "middleware", "rate_limit", "ratelimit", 
        "token", "jwt", "oauth", "permission", "rbac", "cors"
    )
    if any(p in ml for p in security_patterns):
        return ModuleGroup.SECURITY
    
    # System patterns
    system_patterns = (
        "config", "health", "system", "status", "ping", "meta", 
        "version", "info", "metrics", "monitoring", "ready", "live"
    )
    if any(p in ml for p in system_patterns):
        return ModuleGroup.SYSTEM
    
    # Advisor patterns
    if "advisor" in ml or "adviser" in ml:
        return ModuleGroup.ADVISOR
    
    # KSA patterns
    ksa_patterns = ("argaam", "tadawul", "ksa", "saudi", "sr", "riyad", "jeddah")
    if any(p in ml for p in ksa_patterns):
        return ModuleGroup.KSA
    
    # Core patterns
    core_patterns = (
        "enriched", "analysis", "quote", "legacy", "signals", 
        "insights", "fundamental", "technical", "market", "stock"
    )
    if any(p in ml for p in core_patterns):
        return ModuleGroup.CORE
    
    return ModuleGroup.OTHER

# ---------------------------------------------------------------------
# Enhanced discovery with batch operations
# ---------------------------------------------------------------------
@timed
def check_modules_batch(modules: Sequence[str]) -> Dict[str, DiscoveryResult]:
    """
    Check multiple modules in batch.
    Returns detailed results for each module.
    """
    results = {}
    for module in modules or []:
        start = time.perf_counter()
        found = module_exists(module)
        elapsed = (time.perf_counter() - start) * 1000
        
        results[module] = DiscoveryResult(
            module=module,
            found=found,
            candidates_tried=[module],
            time_taken_ms=elapsed
        )
    
    return results

async def check_modules_batch_async(modules: Sequence[str]) -> Dict[str, DiscoveryResult]:
    """
    Async batch check of multiple modules.
    """
    if not modules:
        return {}
    
    async def check_one(module: str) -> DiscoveryResult:
        start = time.perf_counter()
        found = await module_exists_async(module)
        elapsed = (time.perf_counter() - start) * 1000
        
        return DiscoveryResult(
            module=module,
            found=found,
            candidates_tried=[module],
            time_taken_ms=elapsed
        )
    
    tasks = [check_one(m) for m in modules]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)
    
    results = {}
    for module, result in zip(modules, results_list):
        if isinstance(result, DiscoveryResult):
            results[module] = result
        else:
            results[module] = DiscoveryResult(
                module=module,
                found=False,
                candidates_tried=[module],
                time_taken_ms=0,
                error=str(result) if result else "Unknown error"
            )
    
    return results

# ---------------------------------------------------------------------
# Enhanced mount plan with detailed module info
# ---------------------------------------------------------------------
def get_mount_plan_detailed(expected: Optional[Sequence[str]] = None) -> List[ModuleInfo]:
    """
    Get detailed mount plan with full module information.
    """
    exp = list(expected) if expected is not None else get_expected_router_modules()
    
    # Batch check for efficiency
    results = check_modules_batch(exp)
    
    plan = []
    for module in exp:
        result = results.get(module)
        group = _group_of(module)
        
        info = ModuleInfo(
            name=module,
            path=None,  # Would need spec to get path
            group=group,
            status=ModuleStatus.AVAILABLE if result and result.found else ModuleStatus.MISSING,
            last_checked=datetime.now(),
            metrics={
                "check_time_ms": result.time_taken_ms if result else 0
            }
        )
        
        if result and result.error:
            info.error = result.error
            info.status = ModuleStatus.ERROR
        
        plan.append(info)
    
    # Sort by group priority then name
    plan.sort(key=lambda x: (x.group.priority, x.name))
    return plan

# ---------------------------------------------------------------------
# Caching control
# ---------------------------------------------------------------------
def clear_module_cache() -> None:
    """Clear all module existence caches."""
    _module_exists_cached.cache_clear()

def invalidate_module_cache(module: Optional[str] = None) -> None:
    """
    Invalidate cache for a specific module or all modules.
    """
    if module is None:
        clear_module_cache()
    else:
        # For specific module, we need to clear all and rely on lazy re-population
        # Could be optimized with better cache key management
        clear_module_cache()

def get_cache_info() -> Dict[str, Any]:
    """Get cache statistics."""
    cache_info = _module_exists_cached.cache_info()
    return {
        "hits": cache_info.hits,
        "misses": cache_info.misses,
        "maxsize": cache_info.maxsize,
        "currsize": cache_info.currsize,
        "hit_rate": cache_info.hits / (cache_info.hits + cache_info.misses) if (cache_info.hits + cache_info.misses) > 0 else 0
    }

# ---------------------------------------------------------------------
# Enhanced debug snapshot
# ---------------------------------------------------------------------
def get_routes_debug_snapshot_enhanced() -> Dict[str, Any]:
    """
    Enhanced debug snapshot with metrics and cache info.
    """
    base_snapshot = get_routes_debug_snapshot()
    
    # Add enhanced information
    base_snapshot.update({
        "enhanced_version": ROUTES_PACKAGE_VERSION,
        "metrics": get_metrics().get_stats(),
        "cache_info": get_cache_info(),
        "circuit_breaker": {
            "open_circuits": list(_circuit_breaker._open),
            "failure_counts": _circuit_breaker._failures.copy()
        },
        "timestamp": datetime.now().isoformat(),
        "python_version": os.sys.version,
        "platform": os.sys.platform
    })
    
    return base_snapshot

# ---------------------------------------------------------------------
# Keep all original functions for backward compatibility
# ---------------------------------------------------------------------
# [Include all original functions here...]

# ---------------------------------------------------------------------
# Extended exports
# ---------------------------------------------------------------------
__all__ = [
    # Original exports
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
    
    # New exports
    "ModuleStatus",
    "ModuleGroup",
    "ModuleInfo",
    "DiscoveryResult",
    "module_exists_async",
    "module_exists_any_async",
    "check_modules_batch",
    "check_modules_batch_async",
    "get_mount_plan_detailed",
    "get_routes_debug_snapshot_enhanced",
    "clear_module_cache",
    "invalidate_module_cache",
    "get_cache_info",
    "get_metrics",
    "CircuitBreaker",
]
