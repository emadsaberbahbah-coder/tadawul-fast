# core/config.py
"""
Compatibility shim.

Many modules import settings from `core.config`.
Your real settings file is at repo root: `config.py`.

This file re-exports Settings/get_settings so all old imports keep working.
"""

from __future__ import annotations

try:
    # Preferred source (repo root)
    from config import Settings, get_settings  # type: ignore
except Exception:
    # Fallback if root config isn't importable for any reason
    from env import Settings, get_settings  # type: ignore

__all__ = ["Settings", "get_settings"]
