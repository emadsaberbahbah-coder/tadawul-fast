#!/usr/bin/env python3
# core/symbols/__init__.py
"""
core.symbols — Symbol normalization helpers (v1.1.0)

    - normalize  : KSA/global symbol canonicalization (e.g. "2222"
                   -> "2222.SR", "TADAWUL:2010" -> "2010.SR")
    - runtime truth patch: provider aliases and deterministic venue metadata
      for legacy Abu Dhabi, Philippine and Oman symbols, plus invalid .SR
      blocking. The patch never creates a price, score or recommendation.

Consumers continue to import directly:

    from core.symbols.normalize import normalize_symbol

The runtime patch is fail-open at import time, but its own tests and CI verify
that it installs in the supported application environment.
"""

from __future__ import annotations

import logging

__version__ = "1.1.0"
__all__: list[str] = []

_log = logging.getLogger(__name__)

try:
    from core.symbols.runtime_truth_patch import install_runtime_truth_patch

    install_runtime_truth_patch()
except Exception as exc:  # pragma: no cover - startup resilience boundary
    _log.warning(
        "core.symbols runtime truth patch unavailable (%s: %s)",
        exc.__class__.__name__,
        exc,
    )
