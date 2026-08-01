#!/usr/bin/env python3
"""EODHD HTTP 402 plan/entitlement circuit breaker.

CG-1 / CIRCUIT v1.0.0 — WHY (2026-08-01 live zero-write diagnostic)
-------------------------------------------------------------------------------
ROOT CAUSE: EODHD uses HTTP 402 for plan or endpoint-entitlement rejection.
The provider special-cases 401/403/429, but main v4.15.0 lets 402 fall through
its generic ``sc >= 400`` branch. Every symbol therefore repeats a request that
is already known to be unavailable at provider/account scope.

LIVE EVIDENCE: the 2026-08-01 run showed all four US holdings carrying
``fetch_failed:HTTP 402; provider_unhealthy:eodhd`` while the request path kept
trying EODHD. This is a failure-path defect, not a symbol-specific miss.

FIX: after the first confirmed 402, open a process-local, async-safe circuit for
a bounded TTL. Calls made while open return an explicit unavailable error and
perform no network request. They never return a price, zero, score, rank or
recommendation. The normal provider chain remains free to try other providers.

BLAST RADIUS: EODHD failure handling only. Healthy responses and the existing
401/403/429/5xx paths are not changed by this module.

KILL-SWITCH / REVERSIBILITY: ``TFB_EODHD_HTTP402_CIRCUIT=0|false|no|off``
disables the circuit. With it disabled, the native provider keeps its v4.15.0
HTTP 402 behavior. The only accepted boolean vocabulary is the project-wide
``{1,true,yes,on}`` / ``{0,false,no,off}`` contract.

WINDOW STATUS: WINDOW-SAFE — failure-path infrastructure only; no recommendation
or healthy-row behavior changes.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Optional, Tuple

CIRCUIT_VERSION = "1.0.0"
__version__ = CIRCUIT_VERSION

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}

_LOCK: Optional[asyncio.Lock] = None
_LOCK_LOOP: Optional[asyncio.AbstractEventLoop] = None
_OPEN_UNTIL = 0.0
_LAST_REASON = ""
_ACTUAL_402_COUNT = 0
_SHORT_CIRCUIT_COUNT = 0


def circuit_enabled() -> bool:
    """Return the fail-safe circuit flag using the canonical bool vocabulary."""
    raw = (os.getenv("TFB_EODHD_HTTP402_CIRCUIT") or "1").strip().lower()
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return True


def circuit_ttl_seconds() -> float:
    """Return the bounded process-local open interval in seconds."""
    raw = (os.getenv("TFB_EODHD_HTTP402_CIRCUIT_TTL_SEC") or "1800").strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = 1800.0
    return max(60.0, min(21600.0, value))


def is_http402_error(error: Any) -> bool:
    """Recognize network and short-circuit forms of the entitlement failure."""
    text = str(error or "").strip().casefold()
    return any(
        token in text
        for token in (
            "http 402",
            "http_402",
            "plan_or_entitlement",
            "plan_restricted",
            "payment_required",
        )
    )


def _get_lock() -> asyncio.Lock:
    global _LOCK, _LOCK_LOOP
    loop = asyncio.get_running_loop()
    if _LOCK is None or _LOCK_LOOP is not loop:
        _LOCK = asyncio.Lock()
        _LOCK_LOOP = loop
    return _LOCK


async def before_request() -> Tuple[bool, Optional[str]]:
    """Return whether an EODHD network request may proceed."""
    global _OPEN_UNTIL, _LAST_REASON, _SHORT_CIRCUIT_COUNT
    if not circuit_enabled():
        return True, None

    now = time.monotonic()
    async with _get_lock():
        if _OPEN_UNTIL <= now:
            if _OPEN_UNTIL > 0.0:
                _OPEN_UNTIL = 0.0
                _LAST_REASON = ""
            return True, None

        _SHORT_CIRCUIT_COUNT += 1
        remaining = max(0, int(round(_OPEN_UNTIL - now)))
        return (
            False,
            "provider_circuit_open:eodhd:plan_or_entitlement:"
            f"retry_after_sec={remaining}",
        )


async def record_http402(reason: Any) -> bool:
    """Open or extend the circuit after one confirmed HTTP 402 response."""
    global _OPEN_UNTIL, _LAST_REASON, _ACTUAL_402_COUNT
    if not circuit_enabled() or not is_http402_error(reason):
        return False

    async with _get_lock():
        _OPEN_UNTIL = max(
            _OPEN_UNTIL,
            time.monotonic() + circuit_ttl_seconds(),
        )
        _LAST_REASON = str(reason or "HTTP 402 plan_or_entitlement")
        _ACTUAL_402_COUNT += 1
    return True


def circuit_snapshot() -> dict[str, Any]:
    """Return a read-only operational snapshot without network activity."""
    now = time.monotonic()
    return {
        "version": CIRCUIT_VERSION,
        "enabled": circuit_enabled(),
        "ttl_sec": circuit_ttl_seconds(),
        "open": circuit_enabled() and now < _OPEN_UNTIL,
        "remaining_sec": max(0, int(round(_OPEN_UNTIL - now))),
        "last_reason": _LAST_REASON,
        "actual_http402_count": _ACTUAL_402_COUNT,
        "short_circuit_count": _SHORT_CIRCUIT_COUNT,
    }


def _reset_for_tests() -> None:
    """Reset process-local state; test-only and intentionally not exported."""
    global _LOCK, _LOCK_LOOP, _OPEN_UNTIL, _LAST_REASON
    global _ACTUAL_402_COUNT, _SHORT_CIRCUIT_COUNT
    _LOCK = None
    _LOCK_LOOP = None
    _OPEN_UNTIL = 0.0
    _LAST_REASON = ""
    _ACTUAL_402_COUNT = 0
    _SHORT_CIRCUIT_COUNT = 0


__all__ = [
    "CIRCUIT_VERSION",
    "circuit_enabled",
    "circuit_ttl_seconds",
    "is_http402_error",
    "before_request",
    "record_http402",
    "circuit_snapshot",
]
