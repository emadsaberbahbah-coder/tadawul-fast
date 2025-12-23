# routes/config.py
"""
routes/config.py
------------------------------------------------------------
Compatibility shim for older route modules that do:
  from routes.config import Settings, get_settings

We re-export from core.config (preferred), falling back to root config.
"""

from __future__ import annotations

try:
    from core.config import Settings, get_settings  # type: ignore
except Exception:  # pragma: no cover
    from config import Settings, get_settings  # type: ignore

__all__ = ["Settings", "get_settings"]
