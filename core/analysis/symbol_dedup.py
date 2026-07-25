#!/usr/bin/env python3
# core/analysis/symbol_dedup.py
"""
================================================================================
Symbol Identity De-duplication — v1.0.0
================================================================================
Collapses duplicate security rows created by inconsistent ticker suffixes
(NTES vs NTES.US) WITHOUT destroying distinct issuers that merely share a
ticker root (ALV.US = Autoliv vs ALV.DE = Allianz).

WHY THIS MODULE EXISTS
----------------------
`core/analysis/top10_selector.py` de-duplicates by keying its `candidates`
dict on `_normalize_symbol(...)`. That does not collapse suffix variants,
because `core.symbols.normalize.normalize_symbol()` only forces a default
exchange suffix when NORMALIZE_DEFAULT_EQUITY_EXCHANGE_SUFFIX is set — and it
is unset by default. So:

    normalize_symbol("NTES")     -> "NTES"        }  two dict keys
    normalize_symbol("NTES.US")  -> "NTES.US"     }  two candidate rows

Observed consequence (2026-07-22): NetEase was scored twice in the same run at
121.11 and 129.55 — 6.97% apart — with Overall Score 63.39 vs 64.04 and
Forecast Reliability 71.5 vs 76.5. The stale copy carried an ACCUMULATE.

Keying on the base symbol alone is NOT the fix — it silently merges different
companies. Measured on the live universe, 113 base-ticker collisions were
distinct issuers (1211.SR Maaden vs 1211.HK BYD; AAL.L Anglo American vs
AAL.US American Airlines; 7203.T Toyota vs 7203.SR Elm).

THE IDENTITY KEY  (all three components must agree)
--------------------------------------------------
    (base_symbol, normalised_company_name, currency)

Price is deliberately NOT part of the key. Twin rows refresh on different
days, so their prices legitimately differ — only 49 of ~305 collision groups
had matching prices, so a price-equality test misses ~84% of real duplicates.
Price is carried as a *diagnostic*: a wide spread means one copy is stale.

CLASSIFICATION
--------------
    DUPLICATE            same identity key            -> keep freshest, drop rest
    CROSS_LISTING        same name, different ccy      -> keep both, tag as one exposure
    DISTINCT_ISSUER      same base, different name     -> never touched
    QUARANTINED_SHELL    symbol only, no name/price    -> drop (needs re-fetch)
    SUSPECT_QUOTE        fallback price displayed      -> keep, flag for review

Pure stdlib. No pandas, no I/O, no network. Safe to import at startup.

Typical wiring in core/analysis/top10_selector.py:

    from core.analysis.symbol_dedup import dedupe_symbol_rows

    result = dedupe_symbol_rows(pool_rows, sheet=page_name)
    pool_rows = result.rows
    meta["deduped_candidate_count"] = result.removed_count
    meta["dedup_warnings"] = result.warning_summary()
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

SYMBOL_DEDUP_VERSION = "1.0.0"
__version__ = SYMBOL_DEDUP_VERSION

__all__ = [
    # enums / result types
    "DedupVerdict",
    "SecurityIdentity",
    "DedupFinding",
    "DedupResult",
    # main entry points
    "dedupe_symbol_rows",
    "drop_quarantined_shells",
    "resolve_identity",
    # helpers (exported for tests + callers that need the key)
    "identity_key",
    "normalise_company_name",
    "base_symbol_of",
    "is_quarantined_shell",
    "SYMBOL_DEDUP_VERSION",
    "__version__",
]

# =============================================================================
# Optional dependency on core.symbols.normalize
# =============================================================================
# Mirrors the guarded-import pattern used in integrations/google_sheets_service.py
# so this module stays importable in isolation (tests, scripts, cold start).

_HAVE_CORE_NORMALIZE = False
try:  # pragma: no cover - exercised by import environment
    from core.symbols.normalize import (  # type: ignore
        extract_base_symbol as _core_extract_base_symbol,
        normalize_symbol as _core_normalize_symbol,
    )

    _HAVE_CORE_NORMALIZE = True
except Exception:  # pragma: no cover
    try:
        from symbols.normalize import (  # type: ignore
            extract_base_symbol as _core_extract_base_symbol,
            normalize_symbol as _core_normalize_symbol,
        )

        _HAVE_CORE_NORMALIZE = True
    except Exception:
        _core_extract_base_symbol = None  # type: ignore
        _core_normalize_symbol = None  # type: ignore

# Fallback suffix set, used only when core.symbols.normalize is unavailable.
# Keep in sync with EXCHANGE_SUFFIXES in core/symbols/normalize.py.
_FALLBACK_EXCHANGE_SUFFIXES: Set[str] = {
    "US", "NYSE", "N", "NASDAQ", "OQ", "NM", "NG", "TO", "V", "CNQ", "MX",
    "SA", "BA", "L", "LSE", "LN", "PA", "FP", "DE", "F", "BE", "DU", "HM",
    "SW", "VX", "AS", "BR", "MC", "MI", "IM", "CO", "ST", "OL", "HE", "WA",
    "PR", "BU", "AT", "VI", "IR", "DUB", "ZA", "JSE", "TA", "TASE", "SAU",
    "SR", "TADAWUL", "AE", "DFM", "ADX", "QA", "QE", "KW", "KSE", "EG",
    "EGX", "T", "TYO", "HK", "HKG", "SS", "SHG", "SZ", "SHE", "NS", "NSE",
    "BO", "BSE", "KS", "KQ", "KOSDAQ", "TW", "TWO", "SI", "SGX", "KL",
    "KLSE", "JK", "IDX", "SET", "BK", "VN", "HOSE", "PS", "PSE", "AU", "AX",
    "ASX", "NZ", "NZSE",
}

# =============================================================================
# Field aliasing
# =============================================================================
# Rows reach this module either as sheet rows (Title Case headers) or as engine
# rows (snake_case keys). Resolve both, same spirit as _HEADER_ALIAS_MAP in
# integrations/google_sheets_service.py.

_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "symbol": ("Symbol", "symbol", "ticker", "requested_symbol", "requestedSymbol"),
    "name": ("Name", "name", "company_name", "companyName", "long_name"),
    "currency": ("Currency", "currency", "ccy"),
    "price": ("Current Price", "current_price", "currentPrice", "price", "last_price"),
    "updated": (
        "Last Updated (UTC)", "last_updated_utc", "lastUpdatedUtc",
        "Last Updated (Riyadh)", "last_updated_riyadh", "updated_at", "as_of",
    ),
    "warnings": ("Warnings", "warnings", "warning", "flags"),
}

_QUARANTINE_MARKERS = ("identity_quarantined",)
_FALLBACK_QUOTE_MARKERS = ("quote_current_price_missing",)

# Legal-form tokens dropped when comparing company names.
_NAME_NOISE: Set[str] = {
    "INC", "INCORPORATED", "CORP", "CORPORATION", "COMPANY", "CO", "COS",
    "LTD", "LIMITED", "PLC", "LLC", "LP", "LLP", "AG", "NV", "BV", "SE",
    "SA", "SAB", "SAA", "SPA", "AB", "AS", "ASA", "OYJ", "KK", "KGAA",
    "GMBH", "HOLDING", "HOLDINGS", "GROUP", "GRP", "THE", "AND",
    "SGPS", "CV", "DE", "CIA", "COMPANHIA", "SAS", "PT", "TBK", "BHD",
    "PJSC", "JSC", "OAO", "PAO", "ADR", "GDR", "CLASS", "SHARES", "SHS",
    "COMMON", "STOCK", "ORD", "ORDINARY", "REG", "REGISTERED", "TRUST",
    "FUND", "ETF",
}

# Price spread above which twins are reported as stale rather than merely dup.
STALE_TWIN_SPREAD = 0.02

_NON_ALNUM_RE = re.compile(r"[^A-Z0-9 ]+")
_WS_RE = re.compile(r"\s+")


# =============================================================================
# Small value helpers
# =============================================================================


def _get(row: Mapping[str, Any], logical: str) -> Any:
    """Read a logical field from a row regardless of header casing."""
    for candidate in _FIELD_ALIASES.get(logical, ()):
        if candidate in row:
            value = row[candidate]
            if value is not None and str(value).strip() != "":
                return value
    return None


def _as_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number == number else None  # reject NaN


def _as_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = _as_text(value)
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    for parse in (
        lambda t: datetime.fromisoformat(t),
        lambda t: datetime.strptime(t, "%Y-%m-%d %H:%M:%S"),
        lambda t: datetime.strptime(t, "%Y-%m-%d"),
    ):
        try:
            parsed = parse(text)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    return None


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    """Order-preserving unique. Matches the helper name used across core/."""
    seen: Set[str] = set()
    out: List[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


# =============================================================================
# Identity resolution
# =============================================================================


def base_symbol_of(symbol: Any) -> str:
    """
    Ticker with a REAL exchange suffix removed. Share classes survive
    (BRK.B stays BRK.B) because ".B" is not an exchange.

    Delegates to core.symbols.normalize.extract_base_symbol when available so
    there is one suffix table in the codebase, not two.
    """
    text = _as_text(symbol).upper()
    if not text:
        return ""
    if _HAVE_CORE_NORMALIZE and _core_extract_base_symbol is not None:
        try:
            resolved = _as_text(_core_extract_base_symbol(text)).upper()
            if resolved:
                return resolved
        except Exception:
            pass
    if "." not in text:
        return text
    root, _, tail = text.rpartition(".")
    if root and tail in _FALLBACK_EXCHANGE_SUFFIXES:
        # Keep KSA canonical: 2222.SR must not collapse to 2222.
        return text if tail == "SR" else root
    return text


def normalise_company_name(name: Any) -> str:
    """Company name -> comparison key. Empty string when unusable."""
    text = _as_text(name).upper()
    if not text:
        return ""
    text = _NON_ALNUM_RE.sub(" ", text)
    tokens = [t for t in _WS_RE.split(text) if t and t not in _NAME_NOISE]
    return "".join(tokens)


@dataclass(frozen=True)
class SecurityIdentity:
    """Everything needed to decide whether two rows are the same security."""

    symbol: str
    base: str
    name: str
    name_key: str
    currency: str
    price: Optional[float]
    updated: Optional[datetime]
    warnings: str
    populated_fields: int

    @property
    def is_shell(self) -> bool:
        """Identity-quarantined: a symbol and nothing usable behind it."""
        return self.name_key == "" and self.price is None

    @property
    def is_quarantined(self) -> bool:
        low = self.warnings.lower()
        return any(marker in low for marker in _QUARANTINE_MARKERS)

    @property
    def has_fallback_quote(self) -> bool:
        low = self.warnings.lower()
        return any(marker in low for marker in _FALLBACK_QUOTE_MARKERS)

    @property
    def key(self) -> Tuple[str, str, str]:
        return (self.base, self.name_key, self.currency)

    def freshness_rank(self) -> Tuple[float, int, int]:
        """Higher is better: newest first, then most complete, then priced."""
        stamp = self.updated.timestamp() if self.updated else float("-inf")
        return (stamp, self.populated_fields, 1 if self.price is not None else 0)


def resolve_identity(row: Mapping[str, Any]) -> SecurityIdentity:
    """Build a SecurityIdentity from a sheet row or an engine row."""
    symbol = _as_text(_get(row, "symbol")).upper()
    name = _as_text(_get(row, "name"))
    populated = sum(
        1 for value in row.values() if value is not None and str(value).strip() != ""
    )
    return SecurityIdentity(
        symbol=symbol,
        base=base_symbol_of(symbol),
        name=name,
        name_key=normalise_company_name(name),
        currency=_as_text(_get(row, "currency")).upper(),
        price=_as_float(_get(row, "price")),
        updated=_as_datetime(_get(row, "updated")),
        warnings=_as_text(_get(row, "warnings")),
        populated_fields=populated,
    )


def identity_key(row: Mapping[str, Any]) -> Tuple[str, str, str]:
    """The de-duplication key: (base symbol, normalised name, currency)."""
    return resolve_identity(row).key


def is_quarantined_shell(row: Mapping[str, Any]) -> bool:
    """True when the row carries a symbol but its data was blanked upstream."""
    return resolve_identity(row).is_shell


# =============================================================================
# Findings
# =============================================================================


class DedupVerdict:
    """Verdict tags. Plain strings so they survive JSON logging unchanged."""

    DUPLICATE = "DUPLICATE_REMOVED"
    CROSS_LISTING = "CROSS_LISTING_KEPT"
    DISTINCT_ISSUER = "DISTINCT_ISSUER_KEPT"
    SHELL_WITH_TWIN = "QUARANTINED_SHELL_DROPPED"
    SHELL_ORPHAN = "QUARANTINED_SHELL_ORPHAN_DROPPED"
    SUSPECT_QUOTE = "SUSPECT_QUOTE_KEPT"


@dataclass
class DedupFinding:
    """One decision, shaped for _Run_Log / Warnings surfacing."""

    verdict: str
    sheet: str = ""
    base: str = ""
    company: str = ""
    currency: str = ""
    kept: str = ""
    dropped: List[str] = field(default_factory=list)
    symbols: List[str] = field(default_factory=list)
    prices: List[Optional[float]] = field(default_factory=list)
    price_spread: Optional[float] = None
    note: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "verdict": self.verdict,
            "sheet": self.sheet,
            "base": self.base,
            "company": self.company,
            "currency": self.currency,
            "kept": self.kept,
            "dropped": list(self.dropped),
            "symbols": list(self.symbols),
            "prices": list(self.prices),
            "price_spread": self.price_spread,
            "note": self.note,
        }


@dataclass
class DedupResult:
    """Cleaned rows plus a full audit trail."""

    rows: List[Dict[str, Any]] = field(default_factory=list)
    findings: List[DedupFinding] = field(default_factory=list)
    input_count: int = 0

    @property
    def output_count(self) -> int:
        return len(self.rows)

    @property
    def removed_count(self) -> int:
        return self.input_count - self.output_count

    def by_verdict(self, verdict: str) -> List[DedupFinding]:
        return [f for f in self.findings if f.verdict == verdict]

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for finding in self.findings:
            tally[finding.verdict] = tally.get(finding.verdict, 0) + 1
        return tally

    def warning_summary(self) -> str:
        """One-line summary suitable for the sheet Status cell."""
        tally = self.counts()
        if not tally:
            return f"dedup v{SYMBOL_DEDUP_VERSION}: no duplicates"
        parts = [f"{key}={value}" for key, value in sorted(tally.items())]
        return (
            f"dedup v{SYMBOL_DEDUP_VERSION}: "
            f"{self.removed_count} row(s) removed of {self.input_count} | "
            + " ".join(parts)
        )

    def orphan_symbols(self) -> List[str]:
        """Symbols whose data was destroyed upstream and needs a re-fetch."""
        out: List[str] = []
        for finding in self.by_verdict(DedupVerdict.SHELL_ORPHAN):
            out.extend(finding.dropped)
        return _dedupe_keep_order(out)


# =============================================================================
# Core algorithm
# =============================================================================


def _spread(prices: Sequence[Optional[float]]) -> Optional[float]:
    valid = [p for p in prices if p is not None and p > 0]
    if len(valid) < 2:
        return None
    high = max(valid)
    return (high - min(valid)) / high if high else None


def dedupe_symbol_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    sheet: str = "",
    drop_shells: bool = True,
    flag_suspect_quotes: bool = True,
) -> DedupResult:
    """
    Remove duplicate security rows, preserving distinct issuers.

    Args:
        rows: sheet rows or engine rows; Title Case and snake_case both work.
        sheet: page name, recorded on each finding for logging.
        drop_shells: remove identity-quarantined rows whose data was blanked.
                     Set False to keep them visible while debugging upstream.
        flag_suspect_quotes: emit a finding for rows displaying a fallback
                     price (live quote failed). Never removes the row.

    Returns:
        DedupResult with .rows (cleaned, input order preserved) and .findings.
    """
    result = DedupResult(input_count=len(rows))
    if not rows:
        return result

    identities = [resolve_identity(row) for row in rows]
    dropped: Set[int] = set()

    # --- pass 1: exact identity duplicates --------------------------------
    groups: Dict[Tuple[str, str, str], List[int]] = {}
    for index, identity in enumerate(identities):
        if identity.name_key and identity.base:
            groups.setdefault(identity.key, []).append(index)

    for (base, _name_key, currency), members in groups.items():
        if len(members) < 2:
            continue
        ordered = sorted(members, key=lambda i: identities[i].freshness_rank(), reverse=True)
        keeper, losers = ordered[0], ordered[1:]
        dropped.update(losers)
        prices = [identities[i].price for i in ordered]
        spread = _spread(prices)
        stale = spread is not None and spread > STALE_TWIN_SPREAD
        result.findings.append(
            DedupFinding(
                verdict=DedupVerdict.DUPLICATE,
                sheet=sheet,
                base=base,
                company=identities[keeper].name,
                currency=currency,
                kept=identities[keeper].symbol,
                dropped=[identities[i].symbol for i in losers],
                symbols=[identities[i].symbol for i in ordered],
                prices=prices,
                price_spread=round(spread, 4) if spread is not None else None,
                note=(
                    f"stale twin: prices disagree by {spread:.2%}; "
                    "an exact-price match would have missed this"
                    if stale
                    else "prices agree"
                ),
            )
        )

    # --- pass 2: classify remaining base-ticker collisions ----------------
    by_base: Dict[str, List[int]] = {}
    for index, identity in enumerate(identities):
        if identity.base and index not in dropped:
            by_base.setdefault(identity.base, []).append(index)

    for base, members in by_base.items():
        if len(members) < 2:
            continue
        named = [i for i in members if identities[i].name_key]
        if len({identities[i].name_key for i in named}) > 1:
            result.findings.append(
                DedupFinding(
                    verdict=DedupVerdict.DISTINCT_ISSUER,
                    sheet=sheet,
                    base=base,
                    symbols=[identities[i].symbol for i in members],
                    company=" | ".join(identities[i].name for i in named),
                    prices=[identities[i].price for i in members],
                    note="different issuers sharing a ticker root; not merged",
                )
            )
        elif named and len({identities[i].currency for i in named if identities[i].currency}) > 1:
            result.findings.append(
                DedupFinding(
                    verdict=DedupVerdict.CROSS_LISTING,
                    sheet=sheet,
                    base=base,
                    company=identities[named[0]].name,
                    currency=",".join(
                        _dedupe_keep_order(identities[i].currency for i in members)
                    ),
                    symbols=[identities[i].symbol for i in members],
                    prices=[identities[i].price for i in members],
                    note="same issuer, different venue/currency; kept, but treat "
                    "as ONE exposure for position and sector caps",
                )
            )

    # --- pass 3: identity-quarantined shells ------------------------------
    if drop_shells:
        for base, members in by_base.items():
            shells = [i for i in members if identities[i].is_shell and i not in dropped]
            if not shells:
                continue
            alive = [
                i
                for i in members
                if i not in dropped
                and not identities[i].is_shell
                and identities[i].price is not None
            ]
            dropped.update(shells)
            has_twin = bool(alive)
            result.findings.append(
                DedupFinding(
                    verdict=(
                        DedupVerdict.SHELL_WITH_TWIN if has_twin else DedupVerdict.SHELL_ORPHAN
                    ),
                    sheet=sheet,
                    base=base,
                    dropped=[identities[i].symbol for i in shells],
                    symbols=[identities[i].symbol for i in alive],
                    note=(
                        "blanked duplicate; a populated twin remains"
                        if has_twin
                        else "ORPHAN: row was blanked upstream but has no populated "
                        "twin — data was destroyed, symbol needs re-fetch"
                    ),
                )
            )
        # shells with no base at all (symbol unparseable) still go
        for index, identity in enumerate(identities):
            if index in dropped or not identity.is_shell:
                continue
            dropped.add(index)
            result.findings.append(
                DedupFinding(
                    verdict=DedupVerdict.SHELL_ORPHAN,
                    sheet=sheet,
                    base=identity.base,
                    dropped=[identity.symbol],
                    note="ORPHAN: blanked row with no populated twin; needs re-fetch",
                )
            )

    # --- pass 4: fallback-price rows (kept, flagged) -----------------------
    if flag_suspect_quotes:
        for index, identity in enumerate(identities):
            if index in dropped:
                continue
            if identity.has_fallback_quote and identity.price is not None:
                result.findings.append(
                    DedupFinding(
                        verdict=DedupVerdict.SUSPECT_QUOTE,
                        sheet=sheet,
                        base=identity.base,
                        company=identity.name,
                        currency=identity.currency,
                        symbols=[identity.symbol],
                        prices=[identity.price],
                        note="live quote failed; displayed price is a fallback — "
                        "verify identity and price before trading",
                    )
                )

    result.rows = [dict(row) for index, row in enumerate(rows) if index not in dropped]
    return result


def drop_quarantined_shells(
    rows: Sequence[Mapping[str, Any]], *, sheet: str = ""
) -> DedupResult:
    """
    Remove only the identity-quarantined empty rows, leaving duplicates alone.

    Use this when you want the blank-row fix without changing selection
    behaviour — it is the narrower, lower-risk half of dedupe_symbol_rows().
    """
    return dedupe_symbol_rows(
        rows, sheet=sheet, drop_shells=True, flag_suspect_quotes=False
    )
