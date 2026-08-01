#!/usr/bin/env python3
"""Fail-closed KEEP-LAST-GOOD identity gate v1.3.1.

The runtime issuer firewall can correctly turn a mismatched provider response
into a symbol-only, decision-blocked stub. The sync runner's KEEP-LAST-GOOD
stage executes later and may replace that safe stub with the old workbook row.
When the old row is itself the poisoned predecessor, the cache revives the
wrong issuer and prevents convergence.

This module patches only ``_keep_last_good_rows`` after the sync runner finishes
importing. Every proposed last-good substitution is checked by the active
critical Symbol-to-Issuer / venue policy. A failed candidate is rejected and
the incoming blocked stub is retained. No price, name, score, rank, forecast,
recommendation, or workbook value is created here, and this module performs no
network or workbook I/O of its own.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import Any, Sequence

PATCH_VERSION = "1.3.1"
PATCH_TAG = "[KLG-IDENTITY v1.3.1]"

_log = logging.getLogger(__name__)
_LOCK = threading.Lock()
_STARTED = False
_PATCHED_MODULE_IDS: set[int] = set()


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _symbol(value: Any) -> str:
    return _text(value).upper()


def _key(value: Any) -> str:
    return "".join(ch for ch in _text(value).casefold() if ch.isalnum())


def _find(headers: Sequence[Any], aliases: Sequence[str]) -> int:
    wanted = {_key(alias) for alias in aliases}
    for index, header in enumerate(headers or []):
        if _key(header) in wanted:
            return index
    return -1


def _canonical(sync_module: Any, value: Any) -> str:
    canonicalize = getattr(sync_module, "canonicalize_symbol", None)
    if callable(canonicalize):
        try:
            return _symbol(canonicalize(value))
        except Exception:
            pass
    return _symbol(value)


def _copy_rows(rows: Any) -> list[list[Any]]:
    return [list(row) for row in (rows or []) if isinstance(row, (list, tuple))]


def _append_suspects(sync_module: Any, symbols: Sequence[str]) -> None:
    target = getattr(sync_module, "_LAST_KLG_ID_SUSPECTS", None)
    if not isinstance(target, list):
        return
    for symbol in symbols:
        if symbol and symbol not in target:
            target.append(symbol)


def _patch_sync_module(sync_module: Any) -> bool:
    """Patch one loaded run_dashboard_sync module, idempotently."""
    module_id = id(sync_module)
    with _LOCK:
        if module_id in _PATCHED_MODULE_IDS:
            return True

        original = getattr(sync_module, "_keep_last_good_rows", None)
        guard = getattr(sync_module, "quarantine_critical_rows", None)
        if not callable(original) or not callable(guard):
            return False
        if getattr(original, "_TFB_KLG_CRITICAL_GATE_PATCHED", False):
            _PATCHED_MODULE_IDS.add(module_id)
            return True

        def keep_last_good_rows(*args: Any, **kwargs: Any):
            headers = kwargs.get("headers")
            incoming_rows = kwargs.get("rows_matrix")
            if headers is None and len(args) > 3:
                headers = args[3]
            if incoming_rows is None and len(args) > 4:
                incoming_rows = args[4]

            before = _copy_rows(incoming_rows)
            output = original(*args, **kwargs)
            try:
                rows, swapped = output
            except Exception:
                return output

            if not headers or not rows or not swapped or not before:
                return output

            sym_i = _find(headers, ("Symbol", "Ticker", "Code"))
            if sym_i < 0:
                return output

            swapped_set = {_canonical(sync_module, item) for item in swapped}
            refused: list[str] = []
            checked_rows = _copy_rows(rows)

            for index, row in enumerate(checked_rows):
                if index >= len(before) or sym_i >= len(row):
                    continue
                symbol = _canonical(sync_module, row[sym_i])
                if not symbol or symbol not in swapped_set:
                    continue

                candidate = [list(row)]
                failed = False
                try:
                    _guarded, failures = guard(list(headers), candidate)
                    failed = bool(failures)
                except Exception as exc:
                    # A safety check that cannot execute may not certify stale
                    # data. Retain the fresh blocked stub instead.
                    failed = True
                    logger = getattr(sync_module, "logger", _log)
                    logger.error(
                        "%s identity check failed for %s; refusing stale substitution (%s: %s)",
                        PATCH_TAG,
                        symbol,
                        exc.__class__.__name__,
                        exc,
                    )

                if not failed:
                    continue
                rows[index] = list(before[index])
                if symbol not in refused:
                    refused.append(symbol)

            if not refused:
                return rows, list(swapped)

            _append_suspects(sync_module, refused)
            refused_set = set(refused)
            accepted = [
                item
                for item in swapped
                if _canonical(sync_module, item) not in refused_set
            ]
            logger = getattr(sync_module, "logger", _log)
            logger.error(
                "%s refused %d poisoned last-good substitution(s): %s — incoming blocked stubs retained",
                PATCH_TAG,
                len(refused),
                ", ".join(refused[:20]) + ("…" if len(refused) > 20 else ""),
            )
            return rows, accepted

        keep_last_good_rows._TFB_KLG_CRITICAL_GATE_PATCHED = True
        keep_last_good_rows._TFB_ORIGINAL = original
        sync_module._keep_last_good_rows = keep_last_good_rows
        _PATCHED_MODULE_IDS.add(module_id)
        return True


def _candidate_sync_modules() -> list[Any]:
    modules: list[Any] = []
    for name in ("scripts.run_dashboard_sync", "run_dashboard_sync", "__main__"):
        module = sys.modules.get(name)
        if module is None or module in modules:
            continue
        if name == "__main__":
            path = _text(getattr(module, "__file__", "")).replace("\\", "/")
            if not path.endswith("/run_dashboard_sync.py"):
                continue
        modules.append(module)
    return modules


def ensure_installed() -> bool:
    installed = False
    for sync_module in _candidate_sync_modules():
        try:
            installed = _patch_sync_module(sync_module) or installed
        except Exception as exc:  # pragma: no cover - startup resilience edge
            _log.warning("%s install skipped: %s", PATCH_TAG, exc)
    return installed


def _worker() -> None:
    for _ in range(1000):
        if ensure_installed():
            _log.info("%s poisoned last-good refusal gate armed", PATCH_TAG)
            return
        time.sleep(0.01)
    _log.error("%s did not arm after bounded import retries", PATCH_TAG)


def start_deferred_install() -> None:
    global _STARTED
    if os.getenv("TFB_SYNC_KLG_CRITICAL_GATE", "1").strip().lower() in {
        "0",
        "false",
        "off",
        "no",
    }:
        return
    with _LOCK:
        if _STARTED:
            return
        _STARTED = True
    threading.Thread(
        target=_worker,
        name="tfb-klg-identity-v1.3.1",
        daemon=True,
    ).start()


__all__ = [
    "PATCH_VERSION",
    "PATCH_TAG",
    "ensure_installed",
    "start_deferred_install",
    "_patch_sync_module",
]
