#!/usr/bin/env python3
"""Bounded activation for the market-identity and issuer-truth guards.

``core.providers`` can be imported while ``core.analysis.identity_guard`` is
still being initialized through a circular-but-valid import chain. Calling the
patches synchronously at that moment sees a partially initialized module and
cannot wrap ``guard_sheet_rows``.

This module closes that production-only race with a small daemon worker. The
worker performs no network I/O and retries only idempotent in-process patches.
It is bounded by attempt count and delay, and it never creates prices, scores,
ranks, forecasts, recommendations, or workbook data.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict, Optional

ACTIVATION_VERSION = "1.1.0"
__version__ = ACTIVATION_VERSION

_log = logging.getLogger(__name__)
_lock = threading.RLock()
_thread: Optional[threading.Thread] = None
_armed = False
_attempts_used = 0
_last_error = ""


def _ensure_once() -> bool:
    from core.symbols.runtime_truth_patch import (
        ensure_identity_guard_truth_patch,
    )

    if not ensure_identity_guard_truth_patch():
        return False

    from core.providers.urgent_issuer_firewall import (
        ensure_urgent_issuer_firewall,
    )

    return bool(ensure_urgent_issuer_firewall())


def _run_bounded(
    ensure_fn: Callable[[], bool],
    *,
    attempts: int = 80,
    delay_sec: float = 0.05,
    sleeper: Callable[[float], None] = time.sleep,
) -> tuple[bool, int, str]:
    """Run a bounded retry loop and return ``(armed, attempts_used, error)``."""
    total = max(1, int(attempts))
    delay = max(0.0, float(delay_sec))
    last_error = ""

    for attempt in range(1, total + 1):
        try:
            if ensure_fn():
                return True, attempt, ""
        except Exception as exc:  # pragma: no cover - defensive runtime edge
            last_error = f"{exc.__class__.__name__}: {exc}"

        if attempt < total and delay:
            sleeper(delay)

    return False, total, last_error


def _deferred_worker(*, attempts: int, delay_sec: float) -> None:
    global _armed, _attempts_used, _last_error

    armed, used, error = _run_bounded(
        _ensure_once,
        attempts=attempts,
        delay_sec=delay_sec,
    )
    with _lock:
        _armed = bool(armed)
        _attempts_used = int(used)
        _last_error = str(error or "")

    if armed:
        _log.info(
            "Market identity truth + urgent issuer firewall armed after "
            "deferred import retry (attempt=%s)",
            used,
        )
    else:
        _log.error(
            "Market identity truth + urgent issuer firewall did not arm after "
            "%s bounded attempts%s",
            used,
            f" ({error})" if error else "",
        )


def arm_identity_guard_truth_patch(
    *,
    attempts: int = 80,
    delay_sec: float = 0.05,
) -> bool:
    """Arm immediately when possible; otherwise start one bounded daemon retry."""
    global _armed, _attempts_used, _last_error, _thread

    with _lock:
        if _armed:
            return True

    try:
        if _ensure_once():
            with _lock:
                _armed = True
                _attempts_used = max(1, _attempts_used)
                _last_error = ""
            return True
    except Exception as exc:  # pragma: no cover - defensive runtime edge
        with _lock:
            _last_error = f"{exc.__class__.__name__}: {exc}"

    with _lock:
        if _armed:
            return True
        if _thread is not None and _thread.is_alive():
            return False

        _thread = threading.Thread(
            target=_deferred_worker,
            kwargs={
                "attempts": max(1, int(attempts)),
                "delay_sec": max(0.0, float(delay_sec)),
            },
            name="tfb-market-truth-activation",
            daemon=True,
        )
        _thread.start()
        return False


def activation_snapshot() -> Dict[str, object]:
    issuer_armed = False
    issuer_version = ""
    try:
        from core.analysis import identity_guard

        issuer_armed = bool(
            getattr(
                identity_guard,
                "_TFB_URGENT_ISSUER_FIREWALL_PATCHED",
                False,
            )
        )
        issuer_version = str(
            getattr(
                identity_guard,
                "_TFB_URGENT_ISSUER_FIREWALL_VERSION",
                "",
            )
            or ""
        )
    except Exception:
        pass

    with _lock:
        return {
            "version": ACTIVATION_VERSION,
            "armed": bool(_armed),
            "attempts_used": int(_attempts_used),
            "last_error": str(_last_error),
            "thread_alive": bool(_thread is not None and _thread.is_alive()),
            "urgent_issuer_firewall_armed": issuer_armed,
            "urgent_issuer_firewall_version": issuer_version,
        }


__all__ = [
    "ACTIVATION_VERSION",
    "arm_identity_guard_truth_patch",
    "activation_snapshot",
    "_run_bounded",
]
