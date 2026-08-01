#!/usr/bin/env python3
"""Runtime market-identity truth patch.

The live 1,000-symbol diagnostic proved two separate integrity failures:

* legacy Abu Dhabi ``.AB`` requests were not converted to Yahoo's ``.AD``;
* data-free rows for Abu Dhabi, the Philippines and Oman could be labelled
  ``NASDAQ/NYSE`` / ``USD`` by the engine's generic suffix fallback.

This module fixes only deterministic instrument identity metadata. It never
creates a price, score, rank, forecast or recommendation. Contradictory venue
metadata is corrected *before* the existing identity guard runs, explicitly
blocked, and disclosed in ``warnings``. Pre-correction prevents the guard's
25% blast-radius refusal from turning a broad deterministic metadata defect
into an unguarded page.

The normalizer can be imported while :mod:`core.analysis.identity_guard` is
still only partially initialized. Therefore normalizer installation and guard
installation are intentionally separate: the former installs immediately and
the latter can be retried safely after provider initialization. This closes the
production import-order race without weakening the fail-open startup boundary.
"""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple

PATCH_VERSION = "1.0.2"
__version__ = PATCH_VERSION

_INSTALLED = False

# Deterministic venue facts. These are instrument identifiers, not market
# observations. The symbol itself remains unchanged on the sheet.
_MARKET_TRUTH: Dict[str, Tuple[str, str, str, str]] = {
    ".AB": ("ADX", "AED", "United Arab Emirates", "legacy_ab_to_ad_adx"),
    ".AD": ("ADX", "AED", "United Arab Emirates", "abu_dhabi"),
    ".ADX": ("ADX", "AED", "United Arab Emirates", "abu_dhabi"),
    ".PS": ("PSE", "PHP", "Philippines", "philippines"),
    ".PSE": ("PSE", "PHP", "Philippines", "philippines"),
    ".OM": ("MSX", "OMR", "Oman", "oman"),
}

_VALID_SR = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _get(row: Mapping[str, Any], canonical: str, display: str) -> Any:
    if canonical in row:
        return row.get(canonical)
    return row.get(display)


def _set(row: MutableMapping[str, Any], canonical: str, display: str, value: Any) -> None:
    row[canonical] = value
    if display in row:
        row[display] = value


def _append_warning(row: MutableMapping[str, Any], marker: str) -> None:
    current = _text(_get(row, "warnings", "Warnings"))
    parts = [part.strip() for part in current.split(";") if part.strip()]
    if marker not in parts:
        parts.append(marker)
    _set(row, "warnings", "Warnings", "; ".join(parts))


def _append_block_reason(row: MutableMapping[str, Any], reason: str) -> None:
    current = _text(_get(row, "block_reason", "Block Reason"))
    if reason.casefold() in current.casefold():
        return
    combined = f"{current}; {reason}" if current else reason
    _set(row, "block_reason", "Block Reason", combined)


def _suffix_truth(symbol: str) -> Optional[Tuple[str, str, str, str]]:
    upper = _text(symbol).upper()
    for suffix in sorted(_MARKET_TRUTH, key=len, reverse=True):
        if upper.endswith(suffix):
            return _MARKET_TRUTH[suffix]
    return None


def _correct_market_metadata(row: MutableMapping[str, Any]) -> bool:
    """Apply deterministic venue facts and return True when the row changed."""
    symbol = _text(_get(row, "symbol", "Symbol")).upper()
    if not symbol:
        return False

    # A Tadawul ticker is numeric. ``ELET3.SR`` is not a Saudi security and may
    # not inherit SAR/Tadawul merely because it ends with .SR. Keep the symbol
    # visible for operator repair, clear false venue facts and block decisions.
    if symbol.endswith(".SR") and not _VALID_SR.fullmatch(symbol):
        _set(row, "exchange", "Exchange", "")
        _set(row, "currency", "Currency", "")
        _set(row, "country", "Country", "")
        _set(row, "investability_status", "Investability Status", "BLOCKED")
        _set(row, "final_action", "Final Action", "DO_NOT_INVEST")
        _append_block_reason(
            row,
            "Invalid .SR symbol shape: Tadawul identifiers must be numeric",
        )
        _append_warning(row, "invalid_symbol_shape:non_numeric_sr")
        return True

    truth = _suffix_truth(symbol)
    if truth is None:
        return False

    expected_exchange, expected_currency, expected_country, source = truth
    current_exchange = _text(_get(row, "exchange", "Exchange"))
    current_currency = _text(_get(row, "currency", "Currency")).upper()
    current_country = _text(_get(row, "country", "Country"))

    conflicts = []
    if current_exchange and current_exchange.casefold() != expected_exchange.casefold():
        conflicts.append("exchange")
    if current_currency and current_currency != expected_currency:
        conflicts.append("currency")
    if current_country and current_country.casefold() != expected_country.casefold():
        conflicts.append("country")

    _set(row, "exchange", "Exchange", expected_exchange)
    _set(row, "currency", "Currency", expected_currency)
    _set(row, "country", "Country", expected_country)

    if symbol.endswith(".AB"):
        _append_warning(row, "legacy_symbol_alias:.AB->Yahoo.AD/EODHD.ADX")

    if conflicts:
        _set(row, "investability_status", "Investability Status", "BLOCKED")
        _set(row, "final_action", "Final Action", "DO_NOT_INVEST")
        _append_block_reason(
            row,
            "Market metadata conflicted with the symbol venue",
        )
        _append_warning(
            row,
            "market_metadata_conflict_corrected:"
            + source
            + ":"
            + ",".join(conflicts),
        )
    elif not (current_exchange and current_currency and current_country):
        _append_warning(row, f"market_metadata_filled:{source}")

    return bool(conflicts) or not (
        current_exchange == expected_exchange
        and current_currency == expected_currency
        and current_country == expected_country
    )


def _patch_normalize_module() -> None:
    from core.symbols import normalize as normalizer

    # Extend deterministic metadata support without changing the canonical
    # sheet symbol. ``.AB`` and ``.PS`` already exist in this branch; ``.OM``
    # is added here because it was live in the universe but absent from the
    # normalizer's exchange tables.
    normalizer.EXCHANGE_SUFFIXES.setdefault(".OM", "OM")
    normalizer.CURRENCY_BY_COUNTRY.setdefault("OM", "OMR")
    normalizer.EXCHANGE_DISPLAY_NAMES.setdefault("OM", "MSX")
    normalizer.COUNTRY_DISPLAY_NAMES.setdefault("OM", "Oman")

    if getattr(normalizer, "_TFB_RUNTIME_TRUTH_PATCHED", False):
        return

    original_to_yahoo = normalizer.to_yahoo_symbol
    original_metadata = normalizer.infer_symbol_metadata

    @lru_cache(maxsize=20000)
    def to_yahoo_symbol(symbol: str) -> str:
        canonical = normalizer.normalize_symbol(symbol)
        if canonical.upper().endswith(".AB"):
            return canonical[:-3] + ".AD"
        return original_to_yahoo(symbol)

    def infer_symbol_metadata(symbol: str) -> Dict[str, Optional[str]]:
        result = dict(original_metadata(symbol))
        canonical = normalizer.normalize_symbol(symbol).upper()
        if canonical.endswith(".SR") and not _VALID_SR.fullmatch(canonical):
            result.update(
                {
                    "symbol_normalized": canonical,
                    "exchange": None,
                    "exchange_code": None,
                    "currency": None,
                    "country": None,
                    "country_code": None,
                    "mic": None,
                    "market_type": normalizer.MarketType.GLOBAL.value,
                    "inferred_from": "runtime_truth_patch:invalid_sr_shape",
                }
            )
            return result

        truth = _suffix_truth(canonical)
        if truth is not None:
            exchange, currency, country, source = truth
            country_code = (
                "AE"
                if canonical.endswith((".AB", ".AD", ".ADX"))
                else "PH"
                if canonical.endswith((".PS", ".PSE"))
                else "OM"
            )
            market_type = (
                normalizer.MarketType.AE.value
                if country_code == "AE"
                else normalizer.MarketType.PH.value
                if country_code == "PH"
                else normalizer.MarketType.GLOBAL.value
            )
            result.update(
                {
                    "symbol_normalized": canonical,
                    "exchange": exchange,
                    "exchange_code": country_code,
                    "currency": currency,
                    "country": country,
                    "country_code": country_code,
                    "market_type": market_type,
                    "inferred_from": f"runtime_truth_patch:{source}",
                }
            )
        return result

    normalizer.to_yahoo_symbol = to_yahoo_symbol
    normalizer.infer_symbol_metadata = infer_symbol_metadata
    normalizer._TFB_RUNTIME_TRUTH_PATCHED = True


def _patch_identity_guard() -> bool:
    """Patch the fully initialized identity guard; return False when too early."""
    from core.analysis import identity_guard

    original_guard = getattr(identity_guard, "guard_sheet_rows", None)
    if not callable(original_guard):
        # Production can reach this function while identity_guard is still
        # importing through symbol_dedup -> core.symbols.normalize. Defer rather
        # than converting a harmless import order into a startup failure.
        return False

    identity_guard.SUFFIX_CURRENCY.update(
        {
            "AB": "AED",
            "AD": "AED",
            "ADX": "AED",
            "PS": "PHP",
            "PSE": "PHP",
            "OM": "OMR",
        }
    )

    if getattr(identity_guard, "_TFB_MARKET_METADATA_TRUTH_PATCHED", False):
        return True

    def guard_sheet_rows(
        rows: Any,
        sheet: str = "",
        *,
        run_dedup: bool = True,
    ) -> Any:
        prepared = []
        invalid_symbols = []
        for raw in rows or []:
            if not isinstance(raw, Mapping):
                prepared.append(raw)
                continue
            row = dict(raw)
            symbol = _text(_get(row, "symbol", "Symbol")).upper()
            _correct_market_metadata(row)
            if symbol.endswith(".SR") and not _VALID_SR.fullmatch(symbol):
                invalid_symbols.append(symbol)
            prepared.append(row)

        # Run the existing identity and duplicate controls on metadata that is
        # already internally coherent. The explicit block/warning remains on
        # every row that arrived with a conflict.
        plan = original_guard(prepared, sheet=sheet, run_dedup=run_dedup)

        corrected = []
        for raw in plan.rows:
            if not isinstance(raw, Mapping):
                corrected.append(raw)
                continue
            row = dict(raw)
            _correct_market_metadata(row)
            corrected.append(row)
        plan.rows = corrected

        for symbol in invalid_symbols:
            try:
                plan.findings.append(
                    identity_guard.IdentityFinding(
                        action=identity_guard.Action.QUARANTINE_FIELDS,
                        symbol=symbol,
                        sheet=sheet,
                        reason="invalid_symbol_shape",
                        detail="non-numeric .SR identifier blocked",
                    )
                )
            except Exception:
                pass
        return plan

    identity_guard.guard_sheet_rows = guard_sheet_rows
    identity_guard._TFB_MARKET_METADATA_TRUTH_PATCHED = True
    return True


def ensure_identity_guard_truth_patch() -> bool:
    """Retry the guard patch after imports settle and report its armed state."""
    _patch_normalize_module()
    return _patch_identity_guard()


def install_runtime_truth_patch() -> None:
    """Install normalizer truth immediately and guard truth when available."""
    global _INSTALLED
    if not _INSTALLED:
        _patch_normalize_module()
        _INSTALLED = True
    # Do not fail package import when identity_guard is only partially loaded.
    # core.providers.__init__ retries this after provider initialization.
    _patch_identity_guard()


__all__ = [
    "PATCH_VERSION",
    "install_runtime_truth_patch",
    "ensure_identity_guard_truth_patch",
    "_correct_market_metadata",
]
