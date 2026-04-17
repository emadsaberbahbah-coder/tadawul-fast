#!/usr/bin/env python3
# core/analysis/__init__.py
"""
================================================================================
core.analysis -- Analysis Package Initializer v1.1.0
================================================================================
Tadawul Fast Bridge (TFB)

This package contains the analysis and selection engines:

  insights_builder.py    -- Insights_Analysis page builder (executive layout)
                            4 sections: Market Summary, Risk Scenarios,
                            Top Opportunities, Portfolio Health.
                            Schema: 10 columns (Section/Category/Item/Symbol/
                                                Metric/Value/Signal/Score/
                                                Notes/Last Updated)

  top10_selector.py      -- Top_10_Investments selector engine
                            Criteria-driven composite scoring and ranking.
                            Schema: 83 columns (80 canonical + 3 Top10 extras)

Public API (lazy-imported to keep startup safe):

  from core.analysis import build_insights_analysis_rows
  from core.analysis import build_top10_rows
  from core.analysis import get_insights_schema
  from core.analysis import INSIGHTS_BUILDER_VERSION
  from core.analysis import TOP10_SELECTOR_VERSION

Design rules:
  - Import-safe: no network calls, no heavy I/O at import time.
  - Lazy imports: submodule code only executes when a function is called.
  - Never raises at import time.
================================================================================
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

__version__ = "1.1.0"
__all__ = [
    # insights_builder public API
    "build_insights_analysis_rows",
    "get_insights_schema",
    "build_criteria_rows",
    "INSIGHTS_BUILDER_VERSION",
    # top10_selector public API
    "build_top10_rows",
    "TOP10_SELECTOR_VERSION",
    # package version
    "__version__",
]

# ---------------------------------------------------------------------------
# Lazy loader helper
# ---------------------------------------------------------------------------

def _lazy(module_path: str, attr: str) -> Any:
    """
    Returns a lazy proxy that imports `attr` from `module_path` on first call.
    Keeps import time fast and prevents circular import issues.
    """
    _cache: Dict[str, Any] = {}

    def _get() -> Any:
        if "value" not in _cache:
            import importlib
            mod = importlib.import_module(module_path)
            _cache["value"] = getattr(mod, attr)
        return _cache["value"]

    return _get


# ---------------------------------------------------------------------------
# insights_builder lazy proxies
# ---------------------------------------------------------------------------

def build_insights_analysis_rows(*args: Any, **kwargs: Any) -> Any:
    """
    Build the Insights_Analysis page rows.

    Returns a dict with keys: status, page, headers, keys, rows,
    row_objects, rows_matrix, meta.

    Sections generated (v2.0.0):
      1. Market Summary    -- trend signals per universe/page
      2. Risk Scenarios    -- 3 proposals (Conservative/Moderate/Aggressive)
      3. Top Opportunities -- Top 10 shortlist with ROI and confidence
      4. Portfolio Health  -- P/L summary from My_Portfolio

    Args:
      engine:    data engine instance (optional but recommended)
      criteria:  dict of advisor criteria (risk_level, invest_period_days, etc.)
      universes: dict of {section_name: [symbols]} to override auto-universe
      symbols:   flat list of symbols (alternative to universes)
      mode:      fetch mode string
    """
    import importlib
    mod = importlib.import_module("core.analysis.insights_builder")
    return mod.build_insights_analysis_rows(*args, **kwargs)


def get_insights_schema() -> Tuple[List[str], List[str], str]:
    """
    Return (headers, keys, source_marker) for the Insights_Analysis schema.
    Safe to call at import time -- no engine access.
    """
    import importlib
    mod = importlib.import_module("core.analysis.insights_builder")
    return mod.get_insights_schema()


def build_criteria_rows(*args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    """
    Build the criteria block rows for the top of Insights_Analysis.
    """
    import importlib
    mod = importlib.import_module("core.analysis.insights_builder")
    return mod.build_criteria_rows(*args, **kwargs)


def build_top10_rows(*args: Any, **kwargs: Any) -> Any:
    """
    Build Top_10_Investments page rows.

    Returns a dict with keys: status, headers, keys, rows, meta.
    Supports both sync and async call patterns.
    """
    import importlib
    mod = importlib.import_module("core.analysis.top10_selector")
    return mod.build_top10_rows(*args, **kwargs)


# ---------------------------------------------------------------------------
# Version constants (resolved lazily at first access)
# ---------------------------------------------------------------------------

def _get_insights_version() -> str:
    try:
        import importlib
        mod = importlib.import_module("core.analysis.insights_builder")
        return str(getattr(mod, "INSIGHTS_BUILDER_VERSION", "unknown"))
    except Exception:
        return "unknown"


def _get_top10_version() -> str:
    try:
        import importlib
        mod = importlib.import_module("core.analysis.top10_selector")
        return str(getattr(mod, "TOP10_SELECTOR_VERSION", "unknown"))
    except Exception:
        return "unknown"


class _LazyVersionDescriptor:
    """Descriptor that fetches the version string on first access."""
    def __init__(self, getter: Callable[[], str]):
        self._getter = getter
        self._value: Optional[str] = None

    def __get__(self, obj: Any, objtype: Any = None) -> str:
        if self._value is None:
            self._value = self._getter()
        return self._value


class _Versions:
    """Namespace for package version strings -- resolved lazily."""
    insights_builder = _LazyVersionDescriptor(_get_insights_version)
    top10_selector   = _LazyVersionDescriptor(_get_top10_version)


versions = _Versions()

# Module-level version aliases (resolve on access via module __getattr__)
_INSIGHTS_VERSION_CACHE: Optional[str] = None
_TOP10_VERSION_CACHE:    Optional[str] = None


def __getattr__(name: str) -> Any:
    """
    Module-level __getattr__ for lazy version constant resolution.
    Allows:
        from core.analysis import INSIGHTS_BUILDER_VERSION
        from core.analysis import TOP10_SELECTOR_VERSION
    without importing the submodules at package load time.
    """
    global _INSIGHTS_VERSION_CACHE, _TOP10_VERSION_CACHE

    if name == "INSIGHTS_BUILDER_VERSION":
        if _INSIGHTS_VERSION_CACHE is None:
            _INSIGHTS_VERSION_CACHE = _get_insights_version()
        return _INSIGHTS_VERSION_CACHE

    if name == "TOP10_SELECTOR_VERSION":
        if _TOP10_VERSION_CACHE is None:
            _TOP10_VERSION_CACHE = _get_top10_version()
        return _TOP10_VERSION_CACHE

    raise AttributeError(f"module 'core.analysis' has no attribute {name!r}")
