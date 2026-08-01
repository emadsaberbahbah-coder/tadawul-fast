#!/usr/bin/env python3
"""Critical symbol identity policy for the production market sync.

The sheet is the market-universe source. A poisoned row can therefore become a
permanent request and KEEP-LAST-GOOD can preserve it indefinitely. This module
contains the small, auditable set of symbol lifecycle and identity rules needed
for known collision cases. It performs no network calls and has no investment
or scoring logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, MutableSequence, Sequence

POLICY_VERSION = "1.0.0"
CRITICAL_IDENTITY_TAG = "identity_quarantined:critical_registry:v1.0.0"

# Provider-safe canonical identifiers. EODHD uses the .US exchange suffix and
# BRK-B.US for Berkshire Class B. Fiserv moved from FI to FISV in November 2025.
CANONICAL_SYMBOLS: Mapping[str, str] = {
    "BK": "BK.US",
    "BRK-B": "BRK-B.US",
    "BRK.B": "BRK-B.US",
    "FI": "FISV.US",
    "FI.US": "FISV.US",
    "FISV": "FISV.US",
}

# These identifiers must not remain in the active refresh universe.
# 3001.SR and 8270.SR are merger/delisting cases. 4328.SR has no verified active
# Saudi Exchange issuer mapping and is treated as unsupported until an official
# listing can be evidenced.
INACTIVE_SYMBOLS: Mapping[str, str] = {
    "3001.SR": "delisted: Hail Cement acquired by Qassim Cement",
    "8270.SR": "inactive: Buruj merger into MEDGULF and trading suspension pending delisting",
    "4328.SR": "unsupported: no verified active Saudi Exchange issuer mapping",
}


@dataclass(frozen=True)
class IdentityRule:
    accepted_name_tokens: tuple[str, ...]
    currency_tokens: tuple[str, ...] = ("usd",)
    country_tokens: tuple[str, ...] = ("usa", "united states")
    exchange_tokens: tuple[str, ...] = ()


CRITICAL_IDENTITIES: Mapping[str, IdentityRule] = {
    "BK.US": IdentityRule(
        accepted_name_tokens=("bank of new york mellon", "bny mellon"),
        exchange_tokens=("nyse",),
    ),
    "BRK-B.US": IdentityRule(
        accepted_name_tokens=("berkshire hathaway",),
        exchange_tokens=("nyse",),
    ),
    "FISV.US": IdentityRule(
        accepted_name_tokens=("fiserv",),
        exchange_tokens=("nasdaq",),
    ),
}

CRITICAL_FETCH_SYMBOLS = frozenset(CRITICAL_IDENTITIES)


@dataclass(frozen=True)
class UniverseChange:
    source_symbol: str
    action: str
    target_symbol: str = ""
    reason: str = ""


@dataclass(frozen=True)
class IdentityFailure:
    symbol: str
    reason: str
    seen_name: str = ""


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def canonicalize_symbol(value: Any) -> str:
    symbol = normalize_symbol(value)
    return CANONICAL_SYMBOLS.get(symbol, symbol)


def sanitize_active_universe(symbols: Iterable[Any]) -> tuple[list[str], list[UniverseChange]]:
    """Remove inactive identifiers, canonicalize collision-prone US tickers,
    and de-duplicate stably.

    The returned list is the only list that should be sent to providers and to
    persistence verification. Removing a retired symbol here prevents the old
    poisoned row from being made immortal by the persistence layer.
    """
    clean: list[str] = []
    changes: list[UniverseChange] = []
    seen: set[str] = set()

    for raw in symbols:
        source = normalize_symbol(raw)
        if not source:
            continue
        if source in INACTIVE_SYMBOLS:
            changes.append(
                UniverseChange(
                    source_symbol=source,
                    action="removed",
                    reason=INACTIVE_SYMBOLS[source],
                )
            )
            continue

        target = canonicalize_symbol(source)
        if target != source:
            changes.append(
                UniverseChange(
                    source_symbol=source,
                    action="canonicalized",
                    target_symbol=target,
                    reason="provider-safe current identifier",
                )
            )

        if target in seen:
            changes.append(
                UniverseChange(
                    source_symbol=source,
                    action="deduplicated",
                    target_symbol=target,
                    reason="canonical symbol already present",
                )
            )
            continue
        seen.add(target)
        clean.append(target)

    return clean, changes


def build_isolated_batches(symbols: Sequence[Any], batch_size: int) -> list[list[str]]:
    """Put every critical identifier in its own provider request.

    Critical requests run first so a page time budget cannot starve the repair.
    Non-critical symbols retain their relative order and normal batch size.
    """
    size = max(1, int(batch_size))
    normalized = [normalize_symbol(s) for s in symbols if normalize_symbol(s)]
    critical = [[s] for s in normalized if s in CRITICAL_FETCH_SYMBOLS]
    normal = [s for s in normalized if s not in CRITICAL_FETCH_SYMBOLS]
    return critical + [normal[i : i + size] for i in range(0, len(normal), size)]


def _norm_cell(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _find_column(headers: Sequence[Any], aliases: Sequence[str]) -> int:
    wanted = {"".join(ch for ch in alias.casefold() if ch.isalnum()) for alias in aliases}
    for index, header in enumerate(headers):
        norm = "".join(ch for ch in str(header or "").casefold() if ch.isalnum())
        if norm in wanted:
            return index
    return -1


def _optional_field_matches(value: Any, accepted: Sequence[str]) -> bool:
    if not accepted:
        return True
    text = _norm_cell(value)
    if not text:
        return True
    return any(token in text for token in accepted)


def quarantine_critical_rows(
    headers: Sequence[Any],
    rows: MutableSequence[list[Any]],
) -> tuple[MutableSequence[list[Any]], list[IdentityFailure]]:
    """Fail closed on a known critical Symbol->Issuer mismatch.

    A failing row is converted to a symbol-only stub with a visible warning. The
    caller must also mark the page result failed after the write; writing the
    stub purges an already-poisoned predecessor while the failed result prevents
    a false-green refresh verdict.
    """
    failures: list[IdentityFailure] = []
    if not headers or rows is None:
        return rows, failures

    sym_i = _find_column(headers, ("Symbol", "Ticker", "Code"))
    name_i = _find_column(headers, ("Name", "Company Name", "Instrument Name", "Short Name"))
    currency_i = _find_column(headers, ("Currency", "Currency Code"))
    country_i = _find_column(headers, ("Country", "Country Name"))
    exchange_i = _find_column(headers, ("Exchange", "Market", "Exchange Code"))
    warning_i = _find_column(headers, ("Warnings", "Warning"))
    if sym_i < 0:
        return rows, failures

    for row_index, row in enumerate(list(rows)):
        if not isinstance(row, list) or sym_i >= len(row):
            continue
        # Provider responses are not guaranteed to echo the current request
        # spelling. Resolve aliases here (rather than relying on the batched
        # fetcher) so the same rule is selected on every call path.
        symbol = canonicalize_symbol(row[sym_i])
        rule = CRITICAL_IDENTITIES.get(symbol)
        if rule is None:
            continue
        row[sym_i] = symbol

        name = row[name_i] if 0 <= name_i < len(row) else ""
        name_text = _norm_cell(name)
        reason = ""
        if not name_text:
            reason = "blank instrument name"
        elif not any(token in name_text for token in rule.accepted_name_tokens):
            reason = "issuer name mismatch"
        elif currency_i >= 0 and not _optional_field_matches(
            row[currency_i] if currency_i < len(row) else "", rule.currency_tokens
        ):
            reason = "currency mismatch"
        elif country_i >= 0 and not _optional_field_matches(
            row[country_i] if country_i < len(row) else "", rule.country_tokens
        ):
            reason = "country mismatch"
        elif exchange_i >= 0 and not _optional_field_matches(
            row[exchange_i] if exchange_i < len(row) else "", rule.exchange_tokens
        ):
            reason = "exchange mismatch"

        if not reason:
            continue

        blanked = ["" for _ in row]
        blanked[sym_i] = row[sym_i]
        if 0 <= warning_i < len(blanked):
            blanked[warning_i] = CRITICAL_IDENTITY_TAG
        rows[row_index] = blanked
        failures.append(
            IdentityFailure(symbol=symbol, reason=reason, seen_name=str(name or "")[:100])
        )

    return rows, failures


def validate_fresh_critical_rows(
    headers: Sequence[Any],
    rows: MutableSequence[list[Any]],
    requested_symbols: Iterable[Any],
) -> tuple[MutableSequence[list[Any]], list[IdentityFailure]]:
    """Validate current-run proof for every requested critical identifier.

    This must run directly after response membership filtering, before any
    persistence or KEEP-LAST-GOOD operation can add a predecessor row. A
    valid predecessor protects stored data, but is deliberately not evidence
    that the provider returned the right instrument in this run.
    """
    requested = {
        canonicalize_symbol(symbol)
        for symbol in requested_symbols
        if canonicalize_symbol(symbol) in CRITICAL_FETCH_SYMBOLS
    }
    rows, failures = quarantine_critical_rows(headers, rows)
    failed = {failure.symbol for failure in failures}

    sym_i = _find_column(headers, ("Symbol", "Ticker", "Code"))
    returned: set[str] = set()
    if sym_i >= 0:
        for row in rows:
            if not isinstance(row, list) or sym_i >= len(row):
                continue
            symbol = canonicalize_symbol(row[sym_i])
            if symbol in requested:
                row[sym_i] = symbol
                returned.add(symbol)

    for symbol in sorted(requested - returned - failed):
        failures.append(IdentityFailure(symbol=symbol, reason="missing fresh response row"))
    return rows, failures


def fail_result_on_identity(result: Any, failures: Sequence[IdentityFailure]) -> Any:
    """Ensure an unrecoverable critical quarantine can never report success."""
    if not failures:
        return result
    symbols = ", ".join(f.symbol for f in failures)
    result.status = "failed"
    result.rows_failed = max(int(getattr(result, "rows_failed", 0) or 0), len(failures))
    result.error = f"Critical symbol identity mismatch: {symbols}"
    return result
