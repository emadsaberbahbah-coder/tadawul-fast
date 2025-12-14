# core/config.py
"""
core/config.py
------------------------------------------------------------
Compatibility shim (stable).

Many modules import settings via:
  from core.config import Settings, get_settings

The single source of truth is the repo-root `config.py`.
This file re-exports Settings/get_settings so old imports keep working.

Rules:
- Prefer root config.py always.
- Never crash the app if something goes wrong: provide a minimal fallback.
"""

from __future__ import annotations

from typing import Any, Optional


try:
    # ✅ Preferred: repo root config.py
    from config import Settings, get_settings  # type: ignore

except Exception:  # pragma: no cover
    # ------------------------------------------------------------
    # Defensive fallback: define minimal Settings + get_settings
    # (keeps app booting even if import graph is broken)
    # ------------------------------------------------------------
    class Settings:  # type: ignore
        def __init__(self, **kwargs: Any):
            for k, v in kwargs.items():
                setattr(self, k, v)

        # minimal fields commonly used across modules
        app_name: str = "Tadawul Fast Bridge"
        env: str = "production"
        version: str = "4.6.0"
        log_level: str = "info"

        enabled_providers: list[str] = ["eodhd", "finnhub"]
        enabled_ksa_providers: list[str] = ["tadawul", "argaam"]

        # secrets (None by default)
        app_token: Optional[str] = None

    _CACHED: Optional[Settings] = None

    def get_settings() -> Settings:  # type: ignore
        global _CACHED
        if _CACHED is None:
            _CACHED = Settings()
        return _CACHED


__all__ = ["Settings", "get_settings"]

