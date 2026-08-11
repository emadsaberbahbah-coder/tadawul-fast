#!/usr/bin/env python3
"""Preserve explicit missing-response stubs through the identity guard.

The sync-integrity patch deliberately creates one decision-blocked, fact-empty
row when a requested symbol receives no verified backend response.  The legacy
identity guard predates that contract and drops every incoming symbol-only
shell as ``pre_existing_blank_shell``.  When the two protections are composed,
the explicit stub can therefore disappear and the published page can become
shorter than the requested universe.

This patch is deliberately narrow:
* it preserves only stubs carrying explicit unavailable/missing-response proof;
* it never fills Name, Current Price, forecasts, scores, ranks or recommendations;
* it keeps the existing BLOCKED / DO_NOT_INVEST state unchanged;
* unexplained legacy blank shells continue to be dropped by the original guard;
* genuine duplicate handling remains owned by the original guard.

No network calls. No workbook writes.
"""
from __future__ import annotations

import logging
import sys
import threading
import time
from typing import Any, Callable, Mapping, Sequence

PATCH_VERSION = "1.0.0"
PATCH_TAG = "[EXPLICIT-STUB-PRESERVE v1.0.0]"
MISSING_RESPONSE_PREFIX = "missing_response_row:explicit_stub:"

_log = logging.getLogger(__name__)
_LOCK = threading.Lock()
_STARTED = False
_PATCHED_GUARD_IDS: set[int] = set()


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _get(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return ""


def _symbol(row: Mapping[str, Any]) -> str:
    return _text(_get(row, "Symbol", "symbol", "Ticker", "ticker", "Code", "code")).upper()


def is_explicit_missing_stub(row: Mapping[str, Any]) -> bool:
    """True only for an intentionally published, decision-blocked missing row."""
    if not isinstance(row, Mapping) or not _symbol(row):
        return False

    name = _text(_get(row, "Name", "name"))
    price = _text(_get(row, "Current Price", "current_price", "price"))
    if name or price:
        return False

    provider = _text(_get(row, "Data Provider", "data_provider", "Provider", "provider")).casefold()
    row_source = _text(_get(row, "Row Source", "row_source", "Source", "source")).casefold()
    warnings = _text(_get(row, "Warnings", "warnings", "Warning", "warning")).casefold()
    block = _text(_get(row, "Block Reason", "block_reason", "Blocked Reason")).casefold()
    investability = _text(_get(row, "Investability Status", "investability_status", "Investability")).upper()
    action = _text(_get(row, "Final Action", "final_action")).upper()

    explicit_provenance = (
        provider == "unavailable"
        or row_source == "explicit_missing_response_stub"
        or MISSING_RESPONSE_PREFIX in warnings
    )
    fail_closed = (
        investability == "BLOCKED"
        or action == "DO_NOT_INVEST"
        or "missing verified provider response" in block
    )
    return explicit_provenance and fail_closed


def build_guard_wrapper(original_guard: Callable[..., Any], identity_guard_module: Any) -> Callable[..., Any]:
    """Wrap an existing guard without changing its handling of normal rows."""
    if getattr(original_guard, "_TFB_EXPLICIT_STUB_PRESERVED", False):
        return original_guard

    def guard_sheet_rows(
        rows: Sequence[Mapping[str, Any]],
        sheet: str = "",
        *,
        run_dedup: bool = True,
    ) -> Any:
        source_rows = list(rows or [])
        explicit_by_symbol: dict[str, dict[str, Any]] = {}
        input_order: list[str] = []
        seen_order: set[str] = set()

        for raw in source_rows:
            if not isinstance(raw, Mapping):
                continue
            sym = _symbol(raw)
            if sym and sym not in seen_order:
                seen_order.add(sym)
                input_order.append(sym)
            if sym and is_explicit_missing_stub(raw) and sym not in explicit_by_symbol:
                explicit_by_symbol[sym] = dict(raw)

        plan = original_guard(source_rows, sheet=sheet, run_dedup=run_dedup)
        if not explicit_by_symbol:
            return plan

        current_rows = list(getattr(plan, "rows", []) or [])
        present = {
            _symbol(row)
            for row in current_rows
            if isinstance(row, Mapping) and _symbol(row)
        }
        restored: list[str] = []
        for sym, stub in explicit_by_symbol.items():
            if sym not in present:
                current_rows.append(stub)
                present.add(sym)
                restored.append(sym)

        # The original guard reports an intentional DROP_ROW finding for every
        # blank shell.  Once an explicit stub is retained that finding is false
        # evidence, so remove only that exact legacy finding for retained stubs.
        findings = list(getattr(plan, "findings", []) or [])
        filtered = []
        for finding in findings:
            finding_symbol = _text(getattr(finding, "symbol", "")).upper()
            reason = _text(getattr(finding, "reason", ""))
            action = _text(getattr(finding, "action", ""))
            if (
                finding_symbol in explicit_by_symbol
                and reason == "pre_existing_blank_shell"
                and action == getattr(getattr(identity_guard_module, "Action", object), "DROP_ROW", "DROP_ROW")
            ):
                continue
            filtered.append(finding)

        order_index = {sym: idx for idx, sym in enumerate(input_order)}
        indexed = list(enumerate(current_rows))
        indexed.sort(
            key=lambda pair: (
                order_index.get(_symbol(pair[1]) if isinstance(pair[1], Mapping) else "", len(order_index)),
                pair[0],
            )
        )
        plan.rows = [row for _, row in indexed]
        plan.findings = filtered

        if restored:
            _log.warning(
                "%s preserved %d explicit missing-response stub(s) on %s: %s",
                PATCH_TAG,
                len(restored),
                sheet or "<unknown>",
                ", ".join(restored[:15]) + ("…" if len(restored) > 15 else ""),
            )
        return plan

    guard_sheet_rows._TFB_EXPLICIT_STUB_PRESERVED = True
    guard_sheet_rows._TFB_ORIGINAL = original_guard
    return guard_sheet_rows


def ensure_installed() -> bool:
    """Patch the module guard and any already-imported sync-local reference."""
    identity_guard = sys.modules.get("core.analysis.identity_guard")
    if identity_guard is None:
        try:
            from core.analysis import identity_guard as imported_guard
        except Exception:
            return False
        identity_guard = imported_guard

    current = getattr(identity_guard, "guard_sheet_rows", None)
    if not callable(current):
        return False
    if getattr(current, "_TFB_EXPLICIT_STUB_PRESERVED", False):
        return True

    guard_id = id(current)
    with _LOCK:
        if guard_id in _PATCHED_GUARD_IDS:
            return True
        wrapped = build_guard_wrapper(current, identity_guard)
        identity_guard.guard_sheet_rows = wrapped
        for name in ("scripts.run_dashboard_sync", "run_dashboard_sync", "__main__"):
            module = sys.modules.get(name)
            if module is None:
                continue
            if getattr(module, "guard_sheet_rows", None) is current:
                module.guard_sheet_rows = wrapped
        _PATCHED_GUARD_IDS.add(guard_id)
    _log.info("%s explicit missing-response row preservation armed", PATCH_TAG)
    return True


def _worker() -> None:
    for _ in range(1000):
        if ensure_installed():
            return
        time.sleep(0.01)


def start_deferred_install() -> None:
    global _STARTED
    with _LOCK:
        if _STARTED:
            return
        _STARTED = True
    threading.Thread(
        target=_worker,
        name="tfb-explicit-stub-preserve-v1.0.0",
        daemon=True,
    ).start()


__all__ = [
    "PATCH_VERSION",
    "PATCH_TAG",
    "MISSING_RESPONSE_PREFIX",
    "is_explicit_missing_stub",
    "build_guard_wrapper",
    "ensure_installed",
    "start_deferred_install",
]
