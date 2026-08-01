#!/usr/bin/env python3
"""Late-stage sync integrity patch v1.3.0.

This module closes two live gaps without changing prices, scoring, ranking,
portfolio arithmetic, or trading logic:

* a backend omission becomes an explicit unavailable row, in the exact request
  order, rather than disappearing from the page universe;
* deterministic venue metadata is re-applied after persistence and
  KEEP-LAST-GOOD, which can otherwise reintroduce stale NASDAQ/USD labels for
  Abu Dhabi, Philippine, and Oman symbols.

The module performs no network calls and writes no workbook cells. It patches
only the in-process Python sync functions after ``run_dashboard_sync`` finishes
importing. All missing facts remain blank and decision-blocked.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import Any, Mapping, MutableSequence, Sequence

PATCH_VERSION = "1.3.0"
MISSING_RESPONSE_TAG = "missing_response_row:explicit_stub:v1.3.0"
SYNC_PATCH_TAG = "[SYNC-INTEGRITY v1.3.0]"

_log = logging.getLogger(__name__)
_LOCK = threading.Lock()
_STARTED = False
_PATCHED_SYNC_IDS: set[int] = set()
_PATCHED_CRITICAL_IDS: set[int] = set()

_MARKET_TRUTH: Mapping[str, tuple[str, str, str, str]] = {
    ".AB": ("ADX", "AED", "United Arab Emirates", "legacy_ab_to_ad_adx"),
    ".AD": ("ADX", "AED", "United Arab Emirates", "abu_dhabi"),
    ".ADX": ("ADX", "AED", "United Arab Emirates", "abu_dhabi"),
    ".PS": ("PSE", "PHP", "Philippines", "philippines"),
    ".PSE": ("PSE", "PHP", "Philippines", "philippines"),
    ".OM": ("MSX", "OMR", "Oman", "oman"),
}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _key(value: Any) -> str:
    return "".join(ch for ch in _text(value).casefold() if ch.isalnum())


def _symbol(value: Any) -> str:
    return _text(value).upper()


def _find(headers: Sequence[Any], aliases: Sequence[str]) -> int:
    wanted = {_key(alias) for alias in aliases}
    for index, header in enumerate(headers or []):
        if _key(header) in wanted:
            return index
    return -1


def _set(row: list[Any], index: int, value: Any) -> None:
    if index < 0:
        return
    if index >= len(row):
        row.extend([""] * (index + 1 - len(row)))
    row[index] = value


def _append(row: list[Any], index: int, marker: str) -> None:
    if index < 0:
        return
    if index >= len(row):
        row.extend([""] * (index + 1 - len(row)))
    parts = [part.strip() for part in _text(row[index]).split(";") if part.strip()]
    if marker and marker not in parts:
        parts.append(marker)
    row[index] = "; ".join(parts)


def _truth(symbol: str) -> tuple[str, str, str, str] | None:
    upper = _symbol(symbol)
    for suffix in sorted(_MARKET_TRUTH, key=len, reverse=True):
        if upper.endswith(suffix):
            return _MARKET_TRUTH[suffix]
    return None


def _valid_sr(symbol: str) -> bool:
    upper = _symbol(symbol)
    if not upper.endswith(".SR"):
        return True
    stem = upper[:-3]
    return stem.isdigit() and 3 <= len(stem) <= 6


def apply_market_truth(
    headers: Sequence[Any],
    rows: MutableSequence[list[Any]],
) -> tuple[MutableSequence[list[Any]], list[str]]:
    """Apply deterministic venue identity after last-good substitution.

    Only venue fields and explicit decision-block fields are touched. Names,
    prices, scores, ranks, forecasts, and recommendations are never filled.
    """
    corrected: list[str] = []
    if not headers or rows is None:
        return rows, corrected

    sym_i = _find(headers, ("Symbol", "Ticker", "Code"))
    exch_i = _find(headers, ("Exchange", "Market", "Exchange Code"))
    ccy_i = _find(headers, ("Currency", "Currency Code"))
    country_i = _find(headers, ("Country", "Country Name"))
    warn_i = _find(headers, ("Warnings", "Warning"))
    block_i = _find(headers, ("Block Reason", "Blocked Reason"))
    invest_i = _find(headers, ("Investability Status", "Investability"))
    action_i = _find(headers, ("Final Action",))
    if sym_i < 0:
        return rows, corrected

    for row in rows:
        if not isinstance(row, list) or sym_i >= len(row):
            continue
        symbol = _symbol(row[sym_i])
        if not symbol:
            continue

        if symbol.endswith(".SR") and not _valid_sr(symbol):
            _set(row, exch_i, "")
            _set(row, ccy_i, "")
            _set(row, country_i, "")
            _set(row, invest_i, "BLOCKED")
            _set(row, action_i, "DO_NOT_INVEST")
            _append(row, block_i, "Invalid .SR symbol shape: Tadawul identifiers must be numeric")
            _append(row, warn_i, "invalid_symbol_shape:non_numeric_sr")
            corrected.append(symbol)
            continue

        truth = _truth(symbol)
        if truth is None:
            continue
        expected_exchange, expected_currency, expected_country, source = truth
        current_exchange = _text(row[exch_i]) if 0 <= exch_i < len(row) else ""
        current_currency = _text(row[ccy_i]).upper() if 0 <= ccy_i < len(row) else ""
        current_country = _text(row[country_i]) if 0 <= country_i < len(row) else ""

        conflicts: list[str] = []
        if current_exchange and _key(current_exchange) != _key(expected_exchange):
            conflicts.append("exchange")
        if current_currency and current_currency != expected_currency:
            conflicts.append("currency")
        if current_country and _key(current_country) != _key(expected_country):
            conflicts.append("country")

        _set(row, exch_i, expected_exchange)
        _set(row, ccy_i, expected_currency)
        _set(row, country_i, expected_country)
        if symbol.endswith(".AB"):
            _append(row, warn_i, "legacy_symbol_alias:.AB->Yahoo.AD/EODHD.ADX")
        if conflicts:
            _set(row, invest_i, "BLOCKED")
            _set(row, action_i, "DO_NOT_INVEST")
            _append(row, block_i, "Market metadata conflicted with the symbol venue")
            _append(
                row,
                warn_i,
                f"market_metadata_conflict_corrected:{source}:{','.join(conflicts)}",
            )
        elif not (current_exchange and current_currency and current_country):
            _append(row, warn_i, f"market_metadata_filled:{source}")

        if conflicts or not (
            _key(current_exchange) == _key(expected_exchange)
            and current_currency == expected_currency
            and _key(current_country) == _key(expected_country)
        ):
            corrected.append(symbol)

    return rows, corrected


def _indices(headers: Sequence[Any]) -> dict[str, int]:
    return {
        "symbol": _find(headers, ("Symbol", "Ticker", "Code")),
        "provider": _find(headers, ("Data Provider", "Provider", "Data Source")),
        "warnings": _find(headers, ("Warnings", "Warning")),
        "block": _find(headers, ("Block Reason", "Blocked Reason")),
        "investability": _find(headers, ("Investability Status", "Investability")),
        "action": _find(headers, ("Final Action",)),
        "row_source": _find(headers, ("Row Source", "Source")),
    }


def missing_response_stub(headers: Sequence[Any], symbol: str) -> list[Any]:
    """Build a symbol-preserving row with every unknown fact left blank."""
    row: list[Any] = ["" for _ in headers]
    idx = _indices(headers)
    _set(row, idx["symbol"], _symbol(symbol))
    _set(row, idx["provider"], "unavailable")
    _set(row, idx["investability"], "BLOCKED")
    _set(row, idx["action"], "DO_NOT_INVEST")
    _set(row, idx["row_source"], "explicit_missing_response_stub")
    _append(row, idx["warnings"], MISSING_RESPONSE_TAG)
    _append(row, idx["block"], "Missing verified provider response")
    apply_market_truth(headers, [row])
    return row


def complete_response_rows(
    sync_module: Any,
    headers: Sequence[Any],
    rows: Sequence[Sequence[Any]],
    requested_symbols: Sequence[Any],
) -> tuple[list[list[Any]], list[str]]:
    """Produce exactly one row per requested symbol, in exact request order."""
    canonicalize = getattr(sync_module, "canonicalize_symbol", _symbol)
    requested: list[str] = []
    requested_set: set[str] = set()
    for raw in requested_symbols or []:
        symbol = _symbol(canonicalize(raw))
        if symbol and symbol not in requested_set:
            requested_set.add(symbol)
            requested.append(symbol)

    existing = [list(row) for row in (rows or []) if isinstance(row, (list, tuple))]
    if not headers or not requested:
        return existing, []
    sym_i = _find(headers, ("Symbol", "Ticker", "Code"))
    if sym_i < 0:
        return existing, []

    builder = getattr(sync_module, "_build_request_symbol_index", None)
    resolver = getattr(sync_module, "_resolve_requested_symbol", None)
    request_index = builder(requested) if callable(builder) else None
    by_symbol: dict[str, list[Any]] = {}

    for row in existing:
        raw_symbol = row[sym_i] if sym_i < len(row) else ""
        resolved = ""
        if callable(resolver) and request_index is not None:
            try:
                resolved = _symbol(resolver(raw_symbol, request_index=request_index))
            except Exception:
                resolved = ""
        if not resolved:
            candidate = _symbol(canonicalize(raw_symbol))
            if candidate in requested_set:
                resolved = candidate
        if resolved and resolved not in by_symbol:
            _set(row, sym_i, resolved)
            by_symbol[resolved] = row

    completed: list[list[Any]] = []
    missing: list[str] = []
    for symbol in requested:
        row = by_symbol.get(symbol)
        if row is None:
            row = missing_response_stub(headers, symbol)
            missing.append(symbol)
        completed.append(row)
    return completed, missing


def _patch_critical_module(critical_module: Any, sync_module: Any | None = None) -> bool:
    module_id = id(critical_module)
    with _LOCK:
        if module_id in _PATCHED_CRITICAL_IDS:
            wrapper = getattr(critical_module, "quarantine_critical_rows", None)
            if sync_module is not None and callable(wrapper):
                sync_module.quarantine_critical_rows = wrapper
            return True
        original = getattr(critical_module, "quarantine_critical_rows", None)
        if not callable(original):
            return False
        if getattr(original, "_TFB_MARKET_TRUTH_PATCHED", False):
            _PATCHED_CRITICAL_IDS.add(module_id)
            if sync_module is not None:
                sync_module.quarantine_critical_rows = original
            return True

        def quarantine_critical_rows(headers: Sequence[Any], rows: MutableSequence[list[Any]]):
            apply_market_truth(headers, rows)
            return original(headers, rows)

        quarantine_critical_rows._TFB_MARKET_TRUTH_PATCHED = True
        quarantine_critical_rows._TFB_ORIGINAL = original
        critical_module.quarantine_critical_rows = quarantine_critical_rows
        if sync_module is not None:
            sync_module.quarantine_critical_rows = quarantine_critical_rows
        _PATCHED_CRITICAL_IDS.add(module_id)
        return True


def _patch_sync_module(sync_module: Any) -> bool:
    module_id = id(sync_module)
    with _LOCK:
        if module_id in _PATCHED_SYNC_IDS:
            return True
        original = getattr(sync_module, "_fetch_market_rows_batched", None)
        if not callable(original):
            return False
        if getattr(original, "_TFB_RESPONSE_COMPLETENESS_PATCHED", False):
            _PATCHED_SYNC_IDS.add(module_id)
            return True

        async def fetch_market_rows_batched(*args: Any, **kwargs: Any):
            output = await original(*args, **kwargs)
            try:
                headers, rows, endpoint, error = output
            except Exception:
                return output
            symbols = kwargs.get("symbols")
            if symbols is None and len(args) > 2:
                symbols = args[2]
            completed, missing = complete_response_rows(
                sync_module,
                list(headers or []),
                list(rows or []),
                list(symbols or []),
            )
            if missing:
                logger = getattr(sync_module, "logger", _log)
                logger.warning(
                    "%s converted %d omitted backend row(s) into explicit unavailable stubs: %s",
                    SYNC_PATCH_TAG,
                    len(missing),
                    ", ".join(missing[:15]) + ("…" if len(missing) > 15 else ""),
                )
            return headers, completed, endpoint, error

        fetch_market_rows_batched._TFB_RESPONSE_COMPLETENESS_PATCHED = True
        fetch_market_rows_batched._TFB_ORIGINAL = original
        sync_module._fetch_market_rows_batched = fetch_market_rows_batched
        _PATCHED_SYNC_IDS.add(module_id)
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
    """Install both patches once the parent sync module is fully initialized."""
    critical = sys.modules.get("scripts.critical_symbol_identity") or sys.modules.get(
        "critical_symbol_identity"
    )
    installed = False
    for sync_module in _candidate_sync_modules():
        try:
            sync_ok = _patch_sync_module(sync_module)
            critical_ok = (
                _patch_critical_module(critical, sync_module)
                if critical is not None
                else False
            )
            installed = (sync_ok and critical_ok) or installed
        except Exception as exc:  # pragma: no cover - startup resilience boundary
            _log.warning("%s install skipped: %s", SYNC_PATCH_TAG, exc)
    return installed


def _worker() -> None:
    for _ in range(1000):
        if ensure_installed():
            _log.info("%s response completeness + post-KLG market truth armed", SYNC_PATCH_TAG)
            return
        time.sleep(0.01)


def start_deferred_install() -> None:
    global _STARTED
    if os.getenv("TFB_SYNC_RESPONSE_COMPLETENESS", "1").strip().lower() in {
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
        name="tfb-sync-integrity-v1.3.0",
        daemon=True,
    ).start()


__all__ = [
    "PATCH_VERSION",
    "MISSING_RESPONSE_TAG",
    "SYNC_PATCH_TAG",
    "apply_market_truth",
    "missing_response_stub",
    "complete_response_rows",
    "ensure_installed",
    "start_deferred_install",
]
