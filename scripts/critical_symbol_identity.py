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

POLICY_VERSION = "1.3.0"
CRITICAL_IDENTITY_TAG = "identity_quarantined:critical_registry:v1.3.0"

# Provider-safe canonical identifiers. BNY changed its common-stock ticker
# from BK to BNY effective 2026-05-21; stale BK spellings are lifecycle aliases,
# not active provider identities. EODHD uses the .US exchange suffix and BRK-B.US
# for Berkshire Class B. Fiserv moved from FI to FISV in November 2025. Novozymes
# changed name/ticker to Novonesis / NSIS-B.CO.
CANONICAL_SYMBOLS: Mapping[str, str] = {
    "BK": "BNY.US",
    "BK.US": "BNY.US",
    "BNY": "BNY.US",
    "BRK-B": "BRK-B.US",
    "BRK.B": "BRK-B.US",
    "FI": "FISV.US",
    "FI.US": "FISV.US",
    "FISV": "FISV.US",
    "NZYM-B.CO": "NSIS-B.CO",
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


@dataclass(frozen=True)
class VenueRule:
    suffix: str
    exchange_tokens: tuple[str, ...]
    currency_tokens: tuple[str, ...]
    country_tokens: tuple[str, ...]
    numeric_base: bool = False


CRITICAL_IDENTITIES: Mapping[str, IdentityRule] = {
    "BNY.US": IdentityRule(
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
    "NSIS-B.CO": IdentityRule(
        accepted_name_tokens=("novonesis", "novozymes"),
        currency_tokens=("dkk",),
        country_tokens=("denmark",),
        exchange_tokens=("copenhagen", "nasdaq"),
    ),
}

# Urgent, evidence-backed identity subset for the rows that were visibly
# contaminated in the live workbook on 2026-08-01. These tokens are deliberately
# issuer-specific and are checked after canonical symbol resolution. The list is
# a safety firewall, not a complete instrument master; unknown symbols continue to
# be governed by the venue rules below.
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
}

VENUE_RULES: tuple[VenueRule, ...] = (
    VenueRule(
        suffix=".AB",
        exchange_tokens=("adx", "abu dhabi securities", "abu dhabi"),
        currency_tokens=("aed",),
        country_tokens=("united arab emirates", "uae"),
    ),
    VenueRule(
        suffix=".PS",
        exchange_tokens=("pse", "philippine stock exchange"),
        currency_tokens=("php",),
        country_tokens=("philippines",),
    ),
    VenueRule(
        suffix=".OM",
        exchange_tokens=("msx", "muscat stock exchange", "muscat"),
        currency_tokens=("omr",),
        country_tokens=("oman",),
    ),
    VenueRule(
        suffix=".SR",
        exchange_tokens=("tadawul", "saudi exchange", "sau", "xsau"),
        currency_tokens=("sar",),
        country_tokens=("saudi arabia", "kingdom of saudi arabia", "ksa"),
        numeric_base=True,
    ),
)

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
    """Remove inactive identifiers, canonicalize collision-prone tickers,
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


def _venue_rule_for(symbol: str) -> VenueRule | None:
    for rule in VENUE_RULES:
        if symbol.endswith(rule.suffix):
            return rule
    return None


def _identity_reason(
    symbol: str,
    name: Any,
    exchange: Any,
    currency: Any,
    country: Any,
) -> str:
    name_text = _norm_cell(name)
    critical = CRITICAL_IDENTITIES.get(symbol)
    if critical is not None:
        if not name_text:
            return "blank instrument name"
        if not any(token in name_text for token in critical.accepted_name_tokens):
            return "issuer name mismatch"
        if not _optional_field_matches(currency, critical.currency_tokens):
            return "currency mismatch"
        if not _optional_field_matches(country, critical.country_tokens):
            return "country mismatch"
        if not _optional_field_matches(exchange, critical.exchange_tokens):
            return "exchange mismatch"

    issuer_tokens = EXPECTED_ISSUER_TOKENS.get(symbol)
    if issuer_tokens:
        if not name_text:
            return "blank instrument name"
        if not any(token in name_text for token in issuer_tokens):
            return "issuer name mismatch"

    venue = _venue_rule_for(symbol)
    if venue is None:
        return ""
    base = symbol[: -len(venue.suffix)]
    if venue.numeric_base and not base.isdigit():
        return "invalid Saudi symbol format"
    if not _optional_field_matches(exchange, venue.exchange_tokens):
        return "exchange mismatch"
    if not _optional_field_matches(currency, venue.currency_tokens):
        return "currency mismatch"
    if not _optional_field_matches(country, venue.country_tokens):
        return "country mismatch"
    return ""


def quarantine_critical_rows(
    headers: Sequence[Any],
    rows: MutableSequence[list[Any]],
) -> tuple[MutableSequence[list[Any]], list[IdentityFailure]]:
    """Fail closed on critical issuer or venue-identity mismatches.

    A failing row is converted to a symbol-only stub with a visible warning. The
    caller must also mark the page result failed after the write; writing the
    stub purges an already-poisoned predecessor while the failed result prevents
    a false-green refresh verdict. Missing optional venue metadata is not
    invented; only an explicit conflict or an exact issuer mismatch is stripped.
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
        symbol = canonicalize_symbol(row[sym_i])
        row[sym_i] = symbol
        name = row[name_i] if 0 <= name_i < len(row) else ""
        exchange = row[exchange_i] if 0 <= exchange_i < len(row) else ""
        currency = row[currency_i] if 0 <= currency_i < len(row) else ""
        country = row[country_i] if 0 <= country_i < len(row) else ""
        reason = _identity_reason(symbol, name, exchange, currency, country)
        if not reason:
            continue

        blanked = ["" for _ in row]
        blanked[sym_i] = symbol
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
    """Validate current-run proof for every isolated critical identifier.

    This must run directly after response membership filtering, before any
    persistence or KEEP-LAST-GOOD operation can add a predecessor row. A valid
    predecessor protects stored data, but is deliberately not evidence that the
    provider returned the right instrument in this run.
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
