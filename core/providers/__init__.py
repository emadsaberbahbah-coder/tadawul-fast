#!/usr/bin/env python3
# core/providers/__init__.py
"""
core.providers — Market data provider modules (v1.1.0)

This package contains the provider shims used by core.data_engine_v2 to fetch
quotes and fundamentals for Saudi (KSA) and global instruments.

Exposed provider modules:
    - argaam_provider              (KSA news + fundamentals)
    - tadawul_provider             (KSA exchange primary)
    - eodhd_provider               (global primary)
    - finnhub_provider             (global backup)
    - yahoo_chart_provider         (global charts + history)
    - yahoo_fundamentals_provider  (global fundamentals)

The EODHD module remains network-idle at import. A small runtime wrapper is
installed so the first provider-level HTTP 402 opens a bounded local circuit;
subsequent symbols receive explicit unavailable evidence instead of repeating
the same plan-restricted network call. No price, score or recommendation is
created by the wrapper.

Note: a separate root-level ``providers/`` package is legacy. The canonical
provider path is ``core.providers.<name>``.
"""

from __future__ import annotations

import logging

__version__ = "1.1.0"
__all__: list[str] = []

_log = logging.getLogger(__name__)

try:
    from core.providers.eodhd_http402_circuit import (
        install_eodhd_http402_circuit,
    )

    install_eodhd_http402_circuit()
except Exception as exc:  # pragma: no cover - startup resilience boundary
    _log.warning(
        "EODHD HTTP 402 circuit unavailable (%s: %s)",
        exc.__class__.__name__,
        exc,
    )
