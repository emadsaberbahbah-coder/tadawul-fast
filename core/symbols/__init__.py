#!/usr/bin/env python3
# core/symbols/__init__.py
"""
core.symbols — Symbol normalization helpers (v1.0.0)

    - normalize  : KSA/global symbol canonicalization (e.g. "2222"
                   -> "2222.SR", "TADAWUL:2010" -> "2010.SR")

This __init__ is intentionally empty of runtime imports. Consumers
import directly:

    from core.symbols.normalize import normalize_symbol
"""

from __future__ import annotations

__version__ = "1.0.0"
__all__: list[str] = []
