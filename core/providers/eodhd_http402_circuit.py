#!/usr/bin/env python3
"""Immediate EODHD HTTP 402 circuit breaker.

EODHD ``HTTP 402`` is a plan/entitlement failure, not a symbol-specific miss.
The provider already emits ``provider_unhealthy:eodhd``, but the live diagnostic
showed the same network failure repeated across dozens of rows before the
engine-level registry could stop every path.

This process-local wrapper opens after the first observed 402 and short-circuits
subsequent EODHD network calls for a bounded TTL. Short-circuited calls return an
explicit unavailable error; they never return a price, zero, score or synthetic
recommendation. Other providers remain free to serve the symbol.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Awaitable, Callable, Optional, Tuple

CIRCUIT_VERSION = "1.0.0"
__version__ = CIRCUIT_VERSION

_INSTALLED = False
_LOCK: Optional[asyncio.Lock] = None
_LOCK_LOOP: Optional[asyncio.AbstractEventLoop] = None
_OPEN_UNTIL = 0.0
_LAST_REASON = ""
_ACTUAL_402_COUNT = 0
_SHORT_CIRCUIT_COUNT = 0

_TRUTHY = {"1", "true", "yes", "y", "on", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "disabled", "disable"}


def _enabled() -> bool:
    raw = (os.getenv("TFB_EODHD_HTTP402_CIRCUIT") or "1").strip().lower()
    if raw in _FALSY:
        return False
    if raw in _TRUTHY:
        return True
    return True


def _ttl_seconds() -> float:
    raw = (os.getenv("TFB_EODHD_HTTP402_CIRCUIT_TTL_SEC") or "1800").strip()
    try:
        value = float(raw)
    except Exception:
        value = 1800.0
    return max(60.0, min(21600.0, value))


def _is_http402(error: Any) -> bool:
    text = str(error or "").strip().casefold()
    return (
        "http 402" in text
        or "http_402" in text
        or "payment_required" in text
    )


def _get_lock() -> asyncio.Lock:
    global _LOCK, _LOCK_LOOP
    loop = asyncio.get_running_loop()
    if _LOCK is None or _LOCK_LOOP is not loop:
        _LOCK = asyncio.Lock()
        _LOCK_LOOP = loop
    return _LOCK


async def _snapshot_open() -> tuple[bool, float, str]:
    now = time.monotonic()
    async with _get_lock():
        remaining = max(0.0, _OPEN_UNTIL - now)
        return remaining > 0.0, remaining, _LAST_REASON


async def _open_circuit(reason: str) -> None:
    global _OPEN_UNTIL, _LAST_REASON, _ACTUAL_402_COUNT
    async with _get_lock():
        _OPEN_UNTIL = max(_OPEN_UNTIL, time.monotonic() + _ttl_seconds())
        _LAST_REASON = str(reason or "HTTP 402")
        _ACTUAL_402_COUNT += 1


async def _record_short_circuit() -> None:
    global _SHORT_CIRCUIT_COUNT
    async with _get_lock():
        _SHORT_CIRCUIT_COUNT += 1


async def call_with_http402_circuit(
    operation: Callable[[], Awaitable[Tuple[Any, Optional[str]]]],
) -> Tuple[Any, Optional[str]]:
    """Run one provider operation under the bounded 402 circuit."""
    if not _enabled():
        return await operation()

    is_open, remaining, _reason = await _snapshot_open()
    if is_open:
        await _record_short_circuit()
        return (
            None,
            "provider_circuit_open:eodhd:plan_restricted:"
            f"retry_after_sec={int(round(remaining))}",
        )

    data, error = await operation()
    if _is_http402(error):
        await _open_circuit(str(error or "HTTP 402"))
    return data, error


def circuit_snapshot() -> dict[str, Any]:
    """Synchronous operational snapshot; no network and no mutation."""
    return {
        "version": CIRCUIT_VERSION,
        "enabled": _enabled(),
        "ttl_sec": _ttl_seconds(),
        "open": time.monotonic() < _OPEN_UNTIL,
        "remaining_sec": max(0, int(round(_OPEN_UNTIL - time.monotonic()))),
        "last_reason": _LAST_REASON,
        "actual_http402_count": _ACTUAL_402_COUNT,
        "short_circuit_count": _SHORT_CIRCUIT_COUNT,
    }


def _reset_for_tests() -> None:
    global _OPEN_UNTIL, _LAST_REASON, _ACTUAL_402_COUNT, _SHORT_CIRCUIT_COUNT
    _OPEN_UNTIL = 0.0
    _LAST_REASON = ""
    _ACTUAL_402_COUNT = 0
    _SHORT_CIRCUIT_COUNT = 0


def install_eodhd_http402_circuit() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    from core.providers import eodhd_provider

    client_class = eodhd_provider.EODHDClient
    if getattr(client_class, "_TFB_HTTP402_CIRCUIT_INSTALLED", False):
        _INSTALLED = True
        return

    original = client_class._request_json

    async def wrapped_request_json(self: Any, *args: Any, **kwargs: Any):
        async def operation():
            return await original(self, *args, **kwargs)

        return await call_with_http402_circuit(operation)

    client_class._request_json = wrapped_request_json
    client_class._TFB_HTTP402_CIRCUIT_ORIGINAL = original
    client_class._TFB_HTTP402_CIRCUIT_INSTALLED = True
    eodhd_provider.get_http402_circuit_snapshot = circuit_snapshot
    _INSTALLED = True


__all__ = [
    "CIRCUIT_VERSION",
    "call_with_http402_circuit",
    "circuit_snapshot",
    "install_eodhd_http402_circuit",
]
