#!/usr/bin/env python3
# providers/__init__.py
"""
================================================================================
providers package initializer — v1.0.3 (PRODUCTION SAFE)
================================================================================

Purpose
-------
- Makes `providers` a valid Python package so imports like:
    import providers.argaam_provider
  work correctly in Render/production.

Design
------
- Intentionally minimal (no heavy imports, no network calls).
- Safe to import at startup (prevents boot failures).
- Optional: exposes a small helper to list available provider modules.

v1.0.3 Fixes (vs v1.0.2)
------------------------
- get_provider_info return type: Dict[str, any] -> Dict[str, Any]
  (`any` was the builtin function, not `typing.Any`).
- Replace hardcoded `name[10:]` with `str.removeprefix("providers.")` so
  the prefix length is not duplicated as a magic number.
- __getattr__ now raises AttributeError when a name listed in __all__ is
  missing from globals, instead of silently returning None.
- get_provider_info handles whitespace-only / empty module_name with a clear
  error message instead of falling through to a confusing "Unknown provider: ".
- Public API, __all__, and signatures preserved.

Notes
-----
- Keep this file lightweight. Do NOT import provider modules here (circular/import-time
  failures are common and will break FastAPI boot).
"""

from __future__ import annotations

from importlib.util import find_spec
from typing import Any, Dict, List, Optional

__all__ = [
    "is_provider_available",
    "available_providers",
    "get_provider_info",
    "PROVIDER_MODULES",
    "__version__",
]
__version__ = "1.0.3"

# Provider module names used by your repo (add more if you create new providers)
PROVIDER_MODULES: List[str] = [
    "argaam_provider",
    "tadawul_provider",
    "yahoo_chart_provider",
    "yahoo_fundamentals_provider",
    "finnhub_provider",
    "eodhd_provider",
    "alpha_vantage_provider",
    "marketstack_provider",
]

# Provider metadata for documentation
_PROVIDER_METADATA: Dict[str, Dict[str, str]] = {
    "argaam_provider": {
        "name": "Argaam",
        "source": "Saudi financial news and data",
        "region": "KSA",
    },
    "tadawul_provider": {
        "name": "Tadawul",
        "source": "Saudi Stock Exchange",
        "region": "KSA",
    },
    "yahoo_chart_provider": {
        "name": "Yahoo Finance Charts",
        "source": "Yahoo Finance",
        "region": "Global",
    },
    "yahoo_fundamentals_provider": {
        "name": "Yahoo Finance Fundamentals",
        "source": "Yahoo Finance",
        "region": "Global",
    },
    "finnhub_provider": {
        "name": "Finnhub",
        "source": "Finnhub.io API",
        "region": "Global",
    },
    "eodhd_provider": {
        "name": "EOD Historical Data",
        "source": "EODHD API",
        "region": "Global",
    },
    "alpha_vantage_provider": {
        "name": "Alpha Vantage",
        "source": "Alpha Vantage API",
        "region": "Global",
    },
    "marketstack_provider": {
        "name": "Marketstack",
        "source": "Marketstack API",
        "region": "Global",
    },
}


def _strip_pkg_prefix(name: str) -> str:
    """Strip the 'providers.' prefix if present. Centralized so the prefix
    length isn't duplicated as a magic number."""
    return (name or "").strip().removeprefix("providers.")


def is_provider_available(module_name: str) -> bool:
    """
    Return True if a provider module appears importable without actually importing it.

    Example:
        is_provider_available("argaam_provider")            # checks providers.argaam_provider
        is_provider_available("providers.argaam_provider")  # also works

    Args:
        module_name: Name of the provider module (with or without 'providers.' prefix)

    Returns:
        True if the module spec exists and is importable
    """
    name = _strip_pkg_prefix(module_name)
    if not name:
        return False

    return find_spec(f"providers.{name}") is not None


def available_providers() -> Dict[str, bool]:
    """
    Return a dict {provider_module: available_bool} for all known providers.
    Lightweight discovery helper — performs no imports.

    Returns:
        Dictionary mapping provider names to availability status
    """
    return {m: is_provider_available(m) for m in PROVIDER_MODULES}


def get_provider_info(module_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Get detailed information about providers.

    Args:
        module_name: Optional specific provider module name

    Returns:
        Dictionary with provider information including availability and metadata
    """
    if module_name:
        normalized = _strip_pkg_prefix(module_name)

        if not normalized:
            # Whitespace-only or just the bare "providers." prefix.
            return {
                "module": module_name,
                "available": False,
                "error": "Empty or whitespace-only provider name",
            }

        if normalized not in PROVIDER_MODULES:
            return {
                "module": normalized,
                "available": False,
                "error": f"Unknown provider: {normalized}",
            }

        return {
            "module": normalized,
            "available": is_provider_available(normalized),
            "metadata": _PROVIDER_METADATA.get(normalized, {}),
        }

    # Return all providers info
    providers_info: Dict[str, Dict[str, Any]] = {}
    for provider in PROVIDER_MODULES:
        providers_info[provider] = {
            "available": is_provider_available(provider),
            "metadata": _PROVIDER_METADATA.get(provider, {}),
        }

    return {
        "total_providers": len(PROVIDER_MODULES),
        "available_count": sum(1 for p in PROVIDER_MODULES if is_provider_available(p)),
        "providers": providers_info,
        "version": __version__,
    }


# -----------------------------------------------------------------------------
# Module exports and metadata
# -----------------------------------------------------------------------------

def __dir__() -> List[str]:
    """Custom dir for better IDE support"""
    return list(__all__)


def __getattr__(name: str) -> Any:
    """Custom getattr for better error messages.

    Only invoked when normal attribute lookup fails. If the requested name is
    listed in __all__ but is genuinely missing from globals (shouldn't happen
    in practice), raise AttributeError instead of silently returning None.
    """
    if name in __all__ and name in globals():
        return globals()[name]
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


# -----------------------------------------------------------------------------
# CLI self-test (run: python -m providers)
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    print("=== Providers Package Info ===")
    print(f"Version: {__version__}")
    print(f"Total providers: {len(PROVIDER_MODULES)}")
    print(f"Available: {available_providers()}")
    print("\n=== Detailed Info ===")
    print(json.dumps(get_provider_info(), indent=2, default=str))
