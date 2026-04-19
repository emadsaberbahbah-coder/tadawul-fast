#!/usr/bin/env python3
# core/analysis/__init__.py
"""
================================================================================
core.analysis -- Analysis Package Initializer v3.0.0
================================================================================
Tadawul Fast Bridge (TFB)

This package contains the analysis and selection engines:

  insights_builder.py    -- Insights_Analysis page builder (executive layout)
                            Sections: Market Summary, Risk Scenarios,
                            Top Opportunities, Portfolio Health.
                            Schema: 10 columns (Section/Category/Item/Symbol/
                                                Metric/Value/Signal/Score/
                                                Notes/Last Updated)

  top10_selector.py      -- Top_10_Investments selector engine
                            Criteria-driven composite scoring and ranking.
                            Schema: 83 columns (80 canonical + 3 Top10 extras)

Public API (all lazy-loaded on first access):

  # Insights Builder (core.analysis.insights_builder)
  build_insights_analysis_rows(engine, criteria=None, universes=None, ...) -> List[Dict]
  get_insights_schema() -> Tuple[List[str], List[str], str]
  build_criteria_rows(engine, criteria) -> List[Dict[str, Any]]

  # Top 10 Selector (core.analysis.top10_selector)
  build_top10_rows(engine, symbols=None, criteria=None, ...) -> List[Dict]

  # Version constants
  INSIGHTS_BUILDER_VERSION -> str
  TOP10_SELECTOR_VERSION   -> str
  __version__              -> str ("3.0.0")

  # Introspection
  get_available_engines()  -> List[str]
  is_engine_available(engine_name) -> bool
  get_package_metadata()   -> Dict[str, Any]

Design:
  - Import-safe: submodules load on first access, not at `import core.analysis`.
  - PEP 562 module __getattr__/__dir__ for natural lazy resolution -- the real
    callable is returned (not a proxy), preserving __name__, __doc__, signature,
    inspect compatibility, and IDE autocomplete.
  - Once a lazy name is resolved it is cached in module globals, so subsequent
    accesses bypass __getattr__ entirely (zero overhead).
  - Thread safety is delegated to CPython's import lock and functools.lru_cache.
  - No network I/O, no heavy imports, no runtime exceptions at package import.
================================================================================
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
import warnings
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Type-checker-only imports
# ---------------------------------------------------------------------------
# These imports are erased at runtime. Their purpose is to let Pylance / mypy /
# pyright see the real signatures of our lazy-loaded names so autocomplete and
# type checking work exactly as if the functions were imported eagerly.
if TYPE_CHECKING:
    from core.analysis.insights_builder import (  # noqa: F401
        build_criteria_rows,
        build_insights_analysis_rows,
        get_insights_schema,
    )
    from core.analysis.top10_selector import build_top10_rows  # noqa: F401


# ---------------------------------------------------------------------------
# Package metadata
# ---------------------------------------------------------------------------

__version__ = "3.0.0"

__all__ = [
    # Insights Builder API
    "build_insights_analysis_rows",
    "get_insights_schema",
    "build_criteria_rows",
    # Top 10 Selector API
    "build_top10_rows",
    # Version constants
    "INSIGHTS_BUILDER_VERSION",
    "TOP10_SELECTOR_VERSION",
    "__version__",
    # Introspection helpers
    "get_available_engines",
    "is_engine_available",
    "get_package_metadata",
]

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lazy attribute routing tables
# ---------------------------------------------------------------------------
# Map: public_name -> (submodule_dotted_path, attribute_name_in_submodule)
# Adding a new lazy export = one line in the appropriate table.

_LAZY_CALLABLES: Dict[str, Tuple[str, str]] = {
    "build_insights_analysis_rows": (
        "core.analysis.insights_builder", "build_insights_analysis_rows",
    ),
    "get_insights_schema": (
        "core.analysis.insights_builder", "get_insights_schema",
    ),
    "build_criteria_rows": (
        "core.analysis.insights_builder", "build_criteria_rows",
    ),
    "build_top10_rows": (
        "core.analysis.top10_selector", "build_top10_rows",
    ),
}

_LAZY_VERSIONS: Dict[str, Tuple[str, str]] = {
    "INSIGHTS_BUILDER_VERSION": (
        "core.analysis.insights_builder", "INSIGHTS_BUILDER_VERSION",
    ),
    "TOP10_SELECTOR_VERSION": (
        "core.analysis.top10_selector", "TOP10_SELECTOR_VERSION",
    ),
}

_ENGINE_MODULES: Dict[str, str] = {
    "insights_builder": "core.analysis.insights_builder",
    "top10_selector":   "core.analysis.top10_selector",
}


# ---------------------------------------------------------------------------
# Core resolvers (memoized, thread-safe via lru_cache)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=None)
def _resolve_attribute(module_path: str, attr_name: str) -> Any:
    """
    Import `module_path` and return its `attr_name`.

    Raises ImportError if the module cannot be imported, or AttributeError if
    the module is loaded but does not expose `attr_name`. Successful lookups
    are cached for the lifetime of the process.
    """
    module = importlib.import_module(module_path)
    try:
        return getattr(module, attr_name)
    except AttributeError as exc:
        raise AttributeError(
            f"module {module_path!r} has no attribute {attr_name!r}"
        ) from exc


@lru_cache(maxsize=None)
def _resolve_version(module_path: str, attr_name: str) -> str:
    """
    Return a version string from a submodule, degrading to 'unknown' on any
    failure. Never raises. Cached after first lookup.
    """
    try:
        return str(_resolve_attribute(module_path, attr_name))
    except (ImportError, AttributeError) as exc:
        _log.debug("Could not load %s from %s: %s", attr_name, module_path, exc)
        return "unknown"
    except Exception as exc:  # noqa: BLE001 -- defensive: never let version lookup crash callers
        _log.warning(
            "Unexpected error loading %s from %s: %s", attr_name, module_path, exc
        )
        return "unknown"


# ---------------------------------------------------------------------------
# PEP 562 module protocol: __getattr__ + __dir__
# ---------------------------------------------------------------------------

def __getattr__(name: str) -> Any:
    """
    Lazily resolve package-level attributes on first access.

    After resolution, the value is written to module globals so subsequent
    accesses bypass this hook entirely. This is the standard CPython behaviour
    for PEP 562 modules and gives us zero-overhead lookup after warm-up.
    """
    if name in _LAZY_CALLABLES:
        module_path, attr_name = _LAZY_CALLABLES[name]
        value = _resolve_attribute(module_path, attr_name)
        globals()[name] = value
        return value

    if name in _LAZY_VERSIONS:
        module_path, attr_name = _LAZY_VERSIONS[name]
        value = _resolve_version(module_path, attr_name)
        globals()[name] = value
        return value

    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}. "
        f"Available: {', '.join(sorted(__all__))}"
    )


def __dir__() -> List[str]:
    """Expose lazy names to `dir()` and IDE introspection."""
    return sorted(set(__all__) | set(globals()))


# ---------------------------------------------------------------------------
# Introspection helpers (eager -- cheap, no heavy imports)
# ---------------------------------------------------------------------------

def is_engine_available(engine_name: str) -> bool:
    """
    Return True if the named engine module can be located without importing it.

    Args:
        engine_name: 'insights_builder' or 'top10_selector'.

    Returns:
        True if importlib.util.find_spec locates the module, False otherwise.
    """
    module_path = _ENGINE_MODULES.get(engine_name)
    if module_path is None:
        return False
    try:
        return importlib.util.find_spec(module_path) is not None
    except (ImportError, ValueError):
        return False


def get_available_engines() -> List[str]:
    """Return the names of engines that are importable in this environment."""
    return [name for name in _ENGINE_MODULES if is_engine_available(name)]


def get_package_metadata() -> Dict[str, Any]:
    """
    Return a snapshot of package version info and engine availability.

    Note: triggers version resolution for both submodules (cached afterwards).
    """
    return {
        "package_version": __version__,
        "insights_builder_version": _resolve_version(
            *_LAZY_VERSIONS["INSIGHTS_BUILDER_VERSION"]
        ),
        "top10_selector_version": _resolve_version(
            *_LAZY_VERSIONS["TOP10_SELECTOR_VERSION"]
        ),
        "available_engines": get_available_engines(),
        "public_api": list(__all__),
    }


# ---------------------------------------------------------------------------
# Optional import-time sanity check (non-fatal, silenced under test/docs)
# ---------------------------------------------------------------------------

def _verify_submodule_specs() -> None:
    """
    Emit an ImportWarning if any declared submodule cannot be located.

    This is purely diagnostic -- it never raises, and lazy access still works
    for any modules that ARE present. Skipped entirely under pytest and Sphinx
    to avoid noise in test output and generated docs.
    """
    missing: List[str] = []
    for module_path in _ENGINE_MODULES.values():
        try:
            if importlib.util.find_spec(module_path) is None:
                missing.append(module_path)
        except (ImportError, ValueError):
            missing.append(module_path)

    if missing:
        warnings.warn(
            f"core.analysis: submodules not importable: {missing}. "
            f"Lazy access to their APIs will raise ImportError on first use.",
            ImportWarning,
            stacklevel=2,
        )


def _should_skip_verification() -> bool:
    """Skip the sanity check under test runners and doc builders."""
    return (
        getattr(sys, "_called_from_test", False)
        or "sphinx" in sys.modules
        or "pytest" in sys.modules
    )


if not _should_skip_verification():
    try:
        _verify_submodule_specs()
    except Exception as _exc:  # noqa: BLE001 -- defensive: never break package import
        _log.debug("submodule verification skipped: %s", _exc)
