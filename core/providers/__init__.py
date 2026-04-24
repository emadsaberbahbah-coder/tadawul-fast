#!/usr/bin/env python3
# core/providers/__init__.py
"""
core.providers — Market data provider modules (v1.0.0)

This package contains the provider shims used by core.data_engine_v2 to
fetch quotes and fundamentals for Saudi (KSA) and global instruments.

Exposed provider modules:
    - argaam_provider              (KSA news + fundamentals)
    - tadawul_provider             (KSA exchange primary)
    - eodhd_provider               (global primary)
    - finnhub_provider             (global backup)
    - yahoo_chart_provider         (global charts + history)
    - yahoo_fundamentals_provider  (global fundamentals)

This __init__ is intentionally empty of runtime imports. Each provider
module is loaded lazily by the engine's ProviderRegistry when first
needed, so boot stays fast and a broken provider never blocks startup.

Note: a separate root-level `providers/` package (with its own
__init__.py) is legacy / not used by the engine. The canonical provider
path is `core.providers.<name>`.
"""

from __future__ import annotations

__version__ = "1.0.0"
__all__: list[str] = []
