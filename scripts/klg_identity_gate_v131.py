#!/usr/bin/env python3
"""Fail-closed KEEP-LAST-GOOD identity gate v1.3.1.

The deployed backend issuer firewall can turn a mismatched provider response
into a symbol-only, decision-blocked stub. The sync runner's KEEP-LAST-GOOD
stage executes later and may replace that safe stub with the old workbook row.
When the old row is itself the poisoned predecessor, the cache revives the
wrong issuer and prevents convergence.

This module wraps only ``_keep_last_good_rows`` after the sync runner finishes
importing. Every proposed last-good substitution is checked against a small,
auditable registry derived from the live 2026-08-01 contamination evidence and
against deterministic venue rules. A failed candidate is rejected and the
incoming blocked stub is retained. No price, name, score, rank, forecast,
recommendation, or workbook value is created here, and this module performs no
network or workbook I/O of its own.
"""
from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from typing import Any, Mapping, Sequence

PATCH_VERSION = "1.3.1"
PATCH_TAG = "[KLG-IDENTITY v1.3.1]"

_log = logging.getLogger(__name__)
_LOCK = threading.Lock()
_STARTED = False
_PATCHED_MODULE_IDS: set[int] = set()

# Exact audited identities. These are safety assertions for accepting a cached
# predecessor, not an attempt to build the full instrument master in this patch.
EXPECTED_ISSUER_TOKENS: Mapping[str, tuple[str, ...]] = {
    "AC.PS": ("ayala corporation",),
    "SCC.PS": ("semirara mining",),
    "BDO.PS": ("bdo unibank",),
    "SMPH.PS": ("sm prime",),
    "SM.PS": ("sm investments",),
    "JFC.PS": ("jollibee foods",),
    "AREIT.PS": ("areit",),
    "MONDE.PS": ("monde nissin",),
    "SMC.PS": ("san miguel corporation",),
    "ICT.PS": ("international container terminal", "ictsi"),
    "URC.PS": ("universal robina",),
    "BPI.PS": ("bank of the philippine islands",),
    "TEL.PS": ("pldt",),
    "GTCAP.PS": ("gt capital",),
    "TAQA.AB": ("abu dhabi national energy",),
    "ALPHADHABI.AB": ("alpha dhabi",),
    "FAB.AB": ("first abu dhabi bank",),
    "FERTIGLOBE.AB": ("fertiglobe",),
    "ADNOCGAS.AB": ("adnoc gas",),
    "ADPORTS.AB": ("ad ports", "abu dhabi ports"),
    "ALDAR.AB": ("aldar properties",),
    "IHC.AB": ("international holding company",),
    "ADNOCLS.AB": ("adnoc logistics", "adnoc l&s"),
    "EAND.AB": ("emirates telecommunications", "etisalat by e&"),
    "PRESIGHT.AB": ("presight",),
    "ADNOCDIST.AB": ("adnoc distribution", "abu dhabi national oil company for distribution"),
    "BOROUGE.AB": ("borouge",),
    "ADIB.AB": ("abu dhabi islamic bank",),
    "OQGN.OM": ("oq gas networks",),
    # Lifecycle aliases remain unsafe as cached rows unless their issuer is the
    # exact current company. This gate does not rewrite symbols; it only refuses
    # a wrong predecessor.
    "BK.US": ("bank of new york mellon", "bny mellon"),
    "BNY.US": ("bank of new york mellon", "bny mellon"),
    "NZYM-B.CO": ("novonesis", "novozymes"),
    "NSIS-B.CO": ("novonesis", "novozymes"),
}

VENUE_RULES: tuple[tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...]], ...] = (
    (".AB", ("adx", "abu dhabi securities", "abu dhabi"), ("aed",), ("united arab emirates", "uae")),
    (".PS", ("pse", "philippine stock exchange"), ("php",), ("philippines",)),
    (".OM", ("msx", "muscat stock exchange", "muscat"), ("omr",), ("oman",)),
)
_VALID_SR = re.compile(r"^\d{3,6}\.SR$", re.IGNORECASE)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _symbol(value: Any) -> str:
    return _text(value).upper()


def _norm(value: Any) -> str:
    return " ".join(_text(value).casefold().split())


def _key(value: Any) -> str:
    return "".join(ch for ch in _text(value).casefold() if ch.isalnum())


def _find(headers: Sequence[Any], aliases: Sequence[str]) -> int:
    wanted = {_key(alias) for alias in aliases}
    for index, header in enumerate(headers or []):
        if _key(header) in wanted:
            return index
    return -1


def _cell(row: Sequence[Any], index: int) -> Any:
    return row[index] if 0 <= index < len(row) else ""


def _positive(value: Any) -> bool:
    try:
        return float(str(value).replace(",", "").strip()) > 0
    except Exception:
        return False


def _contains_any(value: Any, tokens: Sequence[str]) -> bool:
    text = _norm(value)
    return bool(text) and any(token in text for token in tokens)


def _venue_rule(symbol: str):
    for suffix, exchanges, currencies, countries in VENUE_RULES:
        if symbol.endswith(suffix):
            return suffix, exchanges, currencies, countries
    return None


def candidate_identity_failure(headers: Sequence[Any], row: Sequence[Any]) -> str:
    """Return a refusal reason for an unsafe last-good candidate, else blank."""
    sym_i = _find(headers, ("Symbol", "Ticker", "Code"))
    if sym_i < 0:
        return "symbol column missing"
    symbol = _symbol(_cell(row, sym_i))
    if not symbol:
        return "symbol missing"

    name_i = _find(headers, ("Name", "Company Name", "Instrument Name", "Short Name"))
    exchange_i = _find(headers, ("Exchange", "Market", "Exchange Code"))
    currency_i = _find(headers, ("Currency", "Currency Code"))
    country_i = _find(headers, ("Country", "Country Name"))
    price_i = _find(headers, ("Current Price", "Price", "Last Price"))

    if symbol.endswith(".SR") and not _VALID_SR.fullmatch(symbol):
        return "invalid Saudi symbol format"

    issuer_tokens = EXPECTED_ISSUER_TOKENS.get(symbol)
    if issuer_tokens:
        name = _cell(row, name_i)
        if not _contains_any(name, issuer_tokens):
            return "issuer name mismatch"
        # A predecessor accepted as last-good must carry an actual positive
        # current price; otherwise the incoming explicit stub is more truthful.
        if price_i >= 0 and not _positive(_cell(row, price_i)):
            return "current price missing or invalid"

    venue = _venue_rule(symbol)
    if venue is None:
        return ""
    _suffix, exchanges, currencies, countries = venue
    exchange = _cell(row, exchange_i)
    currency = _cell(row, currency_i)
    country = _cell(row, country_i)

    # For exact audited instruments, venue metadata is required. For other
    # symbols on the same venue, only an explicit contradiction is refused.
    require = issuer_tokens is not None
    if (require or _text(exchange)) and not _contains_any(exchange, exchanges):
        return "exchange mismatch"
    if (require or _text(currency)) and not _contains_any(currency, currencies):
        return "currency mismatch"
    if (require or _text(country)) and not _contains_any(country, countries):
        return "country mismatch"
    return ""


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
        if not callable(original):
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

                try:
                    reason = candidate_identity_failure(list(headers), row)
                except Exception as exc:
                    reason = f"identity check unavailable: {exc.__class__.__name__}"

                if not reason:
                    continue
                rows[index] = list(before[index])
                if symbol not in refused:
                    refused.append(symbol)
                logger = getattr(sync_module, "logger", _log)
                logger.error(
                    "%s refusing stale substitution for %s: %s",
                    PATCH_TAG,
                    symbol,
                    reason,
                )

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
    "EXPECTED_ISSUER_TOKENS",
    "candidate_identity_failure",
    "ensure_installed",
    "start_deferred_install",
    "_patch_sync_module",
]
