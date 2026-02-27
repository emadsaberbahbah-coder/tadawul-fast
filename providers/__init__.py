# providers/__init__.py
"""
================================================================================
providers package initializer — v1.0.1 (PRODUCTION SAFE)
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

Notes
-----
- Keep this file lightweight. Do NOT import provider modules here (circular/import-time
  failures are common and will break FastAPI boot).
"""

from __future__ import annotations

from importlib.util import find_spec
from typing import Dict, List

__all__ = ["is_provider_available", "available_providers"]

__version__ = "1.0.1"

# Provider module names used by your repo (add more if you create new providers)
_KNOWN_PROVIDERS: List[str] = [
    "argaam_provider",
    "tadawul_provider",
    "yahoo_chart_provider",
    "yahoo_fundamentals_provider",
    "finnhub_provider",
    "eodhd_provider",
]


def is_provider_available(module_name: str) -> bool:
    """
    Return True if a provider module appears importable without actually importing it.

    Example:
        is_provider_available("argaam_provider") -> checks providers.argaam_provider
    """
    name = (module_name or "").strip()
    if not name:
        return False
    if name.startswith("providers."):
        fq = name
    else:
        fq = f"providers.{name}"
    return find_spec(fq) is not None


def available_providers() -> Dict[str, bool]:
    """
    Returns a dict {provider_module: available_bool} for known providers.
    This is a lightweight discovery helper (no imports).
    """
    return {m: is_provider_available(m) for m in _KNOWN_PROVIDERS}
