#!/usr/bin/env python3
# core/utils/__init__.py
"""
core.utils — Small shared utilities (v1.0.0)
 
    - compat  : cross-module compatibility helpers (type coercion,
                safe attribute access, import-safe fallbacks)
 
This __init__ is intentionally empty of runtime imports. Consumers
import directly:
 
    from core.utils.compat import ...
"""
 
from __future__ import annotations
 
__version__ = "1.0.0"
__all__: list[str] = []
