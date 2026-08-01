#!/usr/bin/env python3
# core/providers/__init__.py
"""
core.providers — Market data provider modules (v1.3.0)

This package contains the provider shims used by core.data_engine_v2 to fetch
quotes and fundamentals for Saudi (KSA) and global instruments.

Exposed provider modules:
    - argaam_provider              (KSA news + fundamentals)
    - tadawul_provider             (KSA exchange primary)
    - eodhd_provider               (global primary)
    - finnhub_provider             (global backup)
    - yahoo_chart_provider         (global charts + history)
    - yahoo_fundamentals_provider  (global fundamentals)

Two network-idle runtime safeguards are installed here:

1. The deterministic market-identity truth guard is armed immediately when
   possible. If ``identity_guard`` is still partially initialized through the
   production import chain, one bounded daemon worker retries after imports
   settle. This avoids a circular-import race without blocking startup.
2. The first provider-level EODHD HTTP 402 opens a bounded local circuit;
   subsequent symbols receive explicit unavailable evidence instead of
   repeating the same plan-restricted network call.

Neither safeguard creates a price, score, rank, forecast or recommendation.

Note: a separate root-level ``providers/`` package is legacy. The canonical
provider path is ``core.providers.<name>``.
"""

from __future__ import annotations

import logging

__version__ = "1.3.0"
__all__: list[str] = []

_log = logging.getLogger(__name__)

try:
    from core.providers.market_truth_activation import (
        ACTIVATION_VERSION as _MARKET_TRUTH_ACTIVATION_VERSION,
        arm_identity_guard_truth_patch,
    )
    from core.symbols.runtime_truth_patch import (
        PATCH_VERSION as _MARKET_TRUTH_VERSION,
    )

    _market_truth_ready = arm_identity_guard_truth_patch()
    if _market_truth_ready:
        _log.info(
            "Market identity truth patch v%s armed immediately "
            "(activation=%s)",
            _MARKET_TRUTH_VERSION,
            _MARKET_TRUTH_ACTIVATION_VERSION,
        )
    else:
        _log.info(
            "Market identity truth patch v%s queued for bounded deferred "
            "activation (activation=%s)",
            _MARKET_TRUTH_VERSION,
            _MARKET_TRUTH_ACTIVATION_VERSION,
        )
except Exception as exc:  # pragma: no cover - startup resilience boundary
    _log.warning(
        "Market identity truth activation unavailable (%s: %s)",
        exc.__class__.__name__,
        exc,
    )

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
