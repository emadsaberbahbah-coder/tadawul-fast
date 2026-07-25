#!/usr/bin/env python3
# core/analysis/identity_guard.py
"""
================================================================================
Identity Guard — v1.0.0   (replaces the destructive ID-FIREWALL dedup stage)
================================================================================
Fixes the defect that blanked 608 rows across Global_Markets, Market_Leaders and
Commodities_FX between 2026-07-20 and 2026-07-25.

WHAT ACTUALLY WENT WRONG  (measured, not inferred)
--------------------------------------------------
It was never a de-duplication problem. It was an identity LEAK that the
de-duplication rule then amplified into data loss.

Step 1 — the leak. When a symbol's primary quote fails, the enrichment fallback
resolves the wrong record and copies its name and price onto the requesting
symbol. Evidence from the 2026-07-25 export, 127 owner/borrower pairs:

    owner (healthy quote)          borrower (quote_current_price_missing)
    ACU.US   "Acme United"  45.90 USD    7205.T    same name,  47.00 "JPY"
    AAOI.US  "Applied Opto" 110.52 USD   KOZAA.IS  same name, 102.41 "TRY"
    BXMT.US  "Blackstone M"  16.70 USD   BBGI.L    same name,  17.08 "GBX"
    ALPHA.AT "Alpha Bank"     4.05 EUR   GS$D.US   same name,   3.85 "USD"

52.8% of borrowed prices land within 5% of the real owner's price, against
1.85% expected if names were assigned at random — a 29x enrichment. 79 of the
127 pairs cross a currency boundary, so the borrowed number is not a quote at
all: 7205.T priced at "47.00 JPY" is Acme's USD price wearing a JPY label.
The fallback is matching on price proximity while ignoring symbol and currency.

Step 2 — the amplifier. The firewall grouped rows by company NAME ALONE, then
quarantined EVERY member of each group. Because the names were already
corrupted, healthy securities were grouped with contaminated ones and destroyed
alongside them. Of 45 groups it acted on, all 138 member rows were blanked --
at minimum one legitimate security per group was lost needlessly. Name-only
grouping also swept up things that merely share an issuer name:

    share classes      LBTYA.US / LBTYB.US / LBTYK.US   (3 distinct securities)
    preferred series   BEPH.US / BEPI.US / BEPJ.US, AFGB..AFGE.US
    cross-listings     0005.HK / HSBA.L / HSBC          (3 currencies)
    corrupted names    BRK-B + FI + GT.US all "Goodyear"

Step 3 — the destruction. `dedup_mode` flipped observe -> quarantine on
2026-07-20 between 01:01:34 and 07:27:43, and the quarantine BLANKS a row in
place rather than dropping it, so the sheet keeps a shell that looks like a
provider outage.

WHAT THIS MODULE CHANGES
------------------------
1. Detects the leak directly, from the sheet alone, before any dedup runs.
2. Never groups on name alone -- requires (base symbol, name, currency).
3. Never blanks a whole row, and never removes every member of a group.
4. Excludes contaminated rows from ranking instead of silently emptying them.

Three actions, and nothing else:

    TRUST             row is usable
    QUARANTINE_FIELDS clear only the contaminated identity/price fields,
                      keep the symbol + an explicit reason, mark for re-fetch
    DROP_ROW          remove entirely (genuine duplicate only)

KNOWN LIMITATION
----------------
The borrowed-name signature needs the row that legitimately owns the name to be
present in the same sheet. When it is not, a borrowed price that sits inside the
currency band slips through -- BK at 979,000 "USD" is the worked example: it
clears the USD ceiling (which must stay above BRK-A's ~738,000) and its true
owner 012450.KS lives on another page. That case is not solvable from the sheet
alone. The real fix belongs in the provider layer: reject any record whose
returned symbol differs from the symbol that was requested. This module reduces
the blast radius; it does not remove the need for that change.

Pure stdlib. No I/O, no network. Safe to import at startup.

    from core.analysis.identity_guard import guard_sheet_rows

    plan = guard_sheet_rows(rows, sheet=page_name)
    rows = plan.apply()                 # returns corrected rows
    meta["identity_guard"] = plan.summary()
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from core.analysis.symbol_dedup import (
    DedupVerdict,
    SecurityIdentity,
    base_symbol_of,
    dedupe_symbol_rows,
    normalise_company_name,
    resolve_identity,
)

IDENTITY_GUARD_VERSION = "1.0.0"
__version__ = IDENTITY_GUARD_VERSION

__all__ = [
    "Action",
    "IdentityFinding",
    "GuardPlan",
    "guard_sheet_rows",
    "expected_currency_for",
    "currency_is_consistent",
    "price_is_plausible",
    "price_band_applies",
    "row_asset_class",
    "IDENTITY_GUARD_VERSION",
    "__version__",
]

# ---------------------------------------------------------------------------
# Optional reuse of core.symbols.normalize
# ---------------------------------------------------------------------------
try:  # pragma: no cover
    from core.symbols.normalize import get_currency_from_symbol as _core_ccy  # type: ignore
except Exception:  # pragma: no cover
    try:
        from symbols.normalize import get_currency_from_symbol as _core_ccy  # type: ignore
    except Exception:
        _core_ccy = None  # type: ignore

# Exchange suffix -> the currency that venue quotes in. Only unambiguous
# venues are listed; anything absent is treated as "cannot verify".
SUFFIX_CURRENCY: Dict[str, str] = {
    "US": "USD", "SR": "SAR", "L": "GBX", "TO": "CAD", "V": "CAD",
    "PA": "EUR", "DE": "EUR", "F": "EUR", "MI": "EUR", "AS": "EUR",
    "BR": "EUR", "LS": "EUR", "MC": "EUR", "VI": "EUR", "HE": "EUR",
    "IR": "EUR", "AT": "EUR", "SW": "CHF", "ST": "SEK", "CO": "DKK",
    "OL": "NOK", "WA": "PLN", "T": "JPY", "HK": "HKD", "SS": "CNY",
    "SZ": "CNY", "KS": "KRW", "KQ": "KRW", "TW": "TWD", "TWO": "TWD",
    "SI": "SGD", "KL": "MYR", "JK": "IDR", "BK": "THB", "NS": "INR",
    "BO": "INR", "AX": "AUD", "NZ": "NZD", "SA": "BRL", "MX": "MXN",
    "IS": "TRY", "TA": "ILS", "JO": "ZAR", "QA": "QAR", "KW": "KWD",
    "AE": "AED", "EG": "EGP", "MA": "MAD", "VN": "VND", "PH": "PHP",
}

# Currencies that are minor units of another (price magnitudes differ x100).
SUBUNIT_OF = {"GBX": "GBP", "ZAC": "ZAR", "ILA": "ILS"}

# Plausible listed-price range per currency, as (low, high).
#
# Deliberately loose -- these are absurdity bounds, not validation. The point is
# to catch a price that came from a different currency entirely, e.g. BK quoted
# at 979,000 "USD" (a KRW number) or a Tokyo line at 47 "JPY" (a USD number).
# Anything inside the band is left alone.
PRICE_BAND: Dict[str, Tuple[float, float]] = {
    "USD": (0.0001, 2_000_000.0),   # BRK-A trades ~738k
    "EUR": (0.0001, 100_000.0),
    "GBP": (0.01, 100_000.0),
    "CHF": (0.01, 100_000.0),
    "CAD": (0.001, 100_000.0),
    "AUD": (0.001, 100_000.0),
    "SAR": (0.05, 100_000.0),
    "GBX": (0.5, 5_000_000.0),
    "JPY": (10.0, 10_000_000.0),
    "KRW": (100.0, 50_000_000.0),
    "IDR": (10.0, 50_000_000.0),
    "VND": (100.0, 50_000_000.0),
    "HKD": (0.01, 1_000_000.0),
    "TWD": (0.5, 1_000_000.0),
    "INR": (0.5, 1_000_000.0),
    "TRY": (0.05, 1_000_000.0),
}

_ASSET_CLASS_KEYS = ("Asset Class", "asset_class", "assetClass")


def row_asset_class(row: Mapping[str, Any]) -> str:
    for key in _ASSET_CLASS_KEYS:
        if key in row and row[key] is not None and str(row[key]).strip():
            return str(row[key]).strip()
    return ""


_QUOTE_FAILED_MARKERS = ("quote_current_price_missing",)
_NAME_BORROWED_MARKERS = ("name_from_chart_meta",)


class Action:
    TRUST = "TRUST"
    QUARANTINE_FIELDS = "QUARANTINE_FIELDS"
    DROP_ROW = "DROP_ROW"


class Reason:
    BORROWED_IDENTITY = "borrowed_identity"
    CURRENCY_MISMATCH = "currency_inconsistent_with_venue"
    PRICE_IMPLAUSIBLE = "price_magnitude_wrong_for_currency"
    QUOTE_FAILED_UNVERIFIABLE = "quote_failed_identity_unverifiable"
    DUPLICATE = "duplicate_of_fresher_row"


# Fields cleared by QUARANTINE_FIELDS. Symbol, Warnings and Block Reason are
# deliberately preserved so the row remains re-fetchable and self-explaining.
CONTAMINATED_FIELDS: Tuple[str, ...] = (
    "Name", "Current Price", "Previous Close", "Open", "Day High", "Day Low",
    "52W High", "52W Low", "Price Change", "Percent Change", "52W Position %",
    "Market Cap", "Float Shares", "P/E (TTM)", "P/E (Forward)", "EPS (TTM)",
    "Intrinsic Value", "Upside %", "Valuation Score",
    "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M",
    "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M",
    "Overall Score", "Opportunity Score", "Rank (Overall)",
    "Recommendation", "Recommendation Detail", "Recommendation Reason",
    "Target Price", "Upside/Downside %", "Analyst Rating",
)


# ---------------------------------------------------------------------------
# currency / venue consistency
# ---------------------------------------------------------------------------


def expected_currency_for(symbol: Any) -> Optional[str]:
    """Currency the symbol's venue quotes in, or None when unverifiable."""
    text = str(symbol or "").strip().upper()
    if not text or "." not in text:
        return None  # bare ticker: venue unknown, cannot verify
    suffix = text.rpartition(".")[2]
    expected = SUFFIX_CURRENCY.get(suffix)
    if expected:
        return expected
    if _core_ccy is not None:
        try:
            resolved = _core_ccy(text)
            return str(resolved).upper() if resolved else None
        except Exception:
            return None
    return None


# Instruments whose price legitimately sits outside equity bands and which the
# magnitude check must therefore never judge: crypto trades at 1e-8, FX crosses
# at 0.005, index levels run to six figures.
_CRYPTO_RE = re.compile(r"-(USD|USDT|EUR|BTC|ETH)$", re.IGNORECASE)
_FX_RE = re.compile(r"(=X$|^[A-Z]{6}$|^[A-Z]{3}/[A-Z]{3}$)", re.IGNORECASE)
_INDEX_RE = re.compile(r"^\^|\.INDX$", re.IGNORECASE)

_NON_EQUITY_ASSET_CLASSES = {
    "CRYPTO", "CRYPTOCURRENCY", "FOREX", "FX", "FX_INSTRUMENT", "CURRENCY",
    "INDEX", "COMMODITY", "COMMODITY_ETP", "FUTURE", "OPTION", "WARRANT",
}


def price_band_applies(symbol: Any, asset_class: Any = None) -> bool:
    """
    Whether the magnitude band is meaningful for this instrument.

    False for crypto, FX and indices -- SHIB-USD at 4.1e-06 and JPYCHF=X at
    0.005 are correct prices, and quarantining them would destroy good data to
    chase a bug that lives in equities.
    """
    if str(asset_class or "").strip().upper().replace(" ", "_") in _NON_EQUITY_ASSET_CLASSES:
        return False
    text = str(symbol or "").strip().upper()
    if not text:
        return False
    return not (_CRYPTO_RE.search(text) or _FX_RE.search(text) or _INDEX_RE.search(text))


def price_is_plausible(
    currency: Any, price: Optional[float], *, symbol: Any = None, asset_class: Any = None
) -> Optional[bool]:
    """
    True / False / None (unverifiable — no band, or not an equity).

    Independent of any name comparison, which matters: the currency label is
    itself derived from the ticker suffix (`quote_currency_from_suffix`), so it
    is always self-consistent and can never contradict the venue. Price
    magnitude is the one signal in the row that the suffix cannot fake.
    """
    if price is None or price <= 0:
        return None
    if symbol is not None and not price_band_applies(symbol, asset_class):
        return None
    band = PRICE_BAND.get(str(currency or "").strip().upper())
    if not band:
        return None
    low, high = band
    return low <= price <= high


def currency_is_consistent(symbol: Any, currency: Any) -> Optional[bool]:
    """
    True / False / None (unverifiable).

    A .T symbol reporting USD, or a .US symbol reporting KRW, is the signature
    of a borrowed record -- the price came from a different venue entirely.
    """
    expected = expected_currency_for(symbol)
    actual = str(currency or "").strip().upper()
    if not expected or not actual:
        return None
    if expected == actual:
        return True
    # GBP/GBX and friends are the same venue, different unit.
    if SUBUNIT_OF.get(expected) == actual or SUBUNIT_OF.get(actual) == expected:
        return True
    return False


# ---------------------------------------------------------------------------
# findings + plan
# ---------------------------------------------------------------------------


@dataclass
class IdentityFinding:
    action: str
    symbol: str
    sheet: str = ""
    reason: str = ""
    detail: str = ""
    borrowed_from: str = ""
    row_index: int = -1

    def as_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "symbol": self.symbol,
            "sheet": self.sheet,
            "reason": self.reason,
            "detail": self.detail,
            "borrowed_from": self.borrowed_from,
        }


@dataclass
class GuardPlan:
    rows: List[Dict[str, Any]] = field(default_factory=list)
    findings: List[IdentityFinding] = field(default_factory=list)
    input_count: int = 0
    sheet: str = ""

    def by_action(self, action: str) -> List[IdentityFinding]:
        return [f for f in self.findings if f.action == action]

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for finding in self.findings:
            tally[finding.action] = tally.get(finding.action, 0) + 1
        return tally

    def refetch_symbols(self) -> List[str]:
        """Symbols whose identity could not be trusted; queue these."""
        seen: Set[str] = set()
        out: List[str] = []
        for finding in self.by_action(Action.QUARANTINE_FIELDS):
            if finding.symbol not in seen:
                seen.add(finding.symbol)
                out.append(finding.symbol)
        return out

    def apply(self) -> List[Dict[str, Any]]:
        """The corrected rows. Already applied by guard_sheet_rows()."""
        return self.rows

    def summary(self) -> str:
        tally = self.counts()
        if not tally:
            return f"identity_guard v{IDENTITY_GUARD_VERSION}: clean"
        parts = [f"{k}={v}" for k, v in sorted(tally.items())]
        return (
            f"identity_guard v{IDENTITY_GUARD_VERSION}: "
            f"{self.input_count}->{len(self.rows)} rows | " + " ".join(parts)
        )


# ---------------------------------------------------------------------------
# the guard
# ---------------------------------------------------------------------------


def _quote_failed(identity: SecurityIdentity) -> bool:
    low = identity.warnings.lower()
    return any(marker in low for marker in _QUOTE_FAILED_MARKERS)


def _name_from_fallback(identity: SecurityIdentity) -> bool:
    low = identity.warnings.lower()
    return any(marker in low for marker in _NAME_BORROWED_MARKERS)


def guard_sheet_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    sheet: str = "",
    run_dedup: bool = True,
) -> GuardPlan:
    """
    Verify row identities, then de-duplicate safely.

    Ordering matters: identity verification runs FIRST, so contaminated names
    can never become input to the duplicate grouping. That single ordering
    change is what stops one bad name from destroying a healthy security.

    Args:
        rows:  sheet rows or engine rows.
        sheet: page name, recorded on findings.
        run_dedup: also collapse genuine duplicates via symbol_dedup.

    Returns:
        GuardPlan. `.rows` is the corrected row list.
    """
    plan = GuardPlan(input_count=len(rows), sheet=sheet)
    if not rows:
        return plan

    # --- stage 0: discard shells that arrived already blank -----------------
    # These are rows a previous destructive run emptied. Their data is gone and
    # they carry no reason, so they cannot be distinguished from an outage.
    # Dropping them here also keeps them from being confused with the rows THIS
    # run quarantines, which must survive as re-fetchable.
    incoming: List[Dict[str, Any]] = []
    for row in rows:
        identity = resolve_identity(row)
        if identity.symbol and identity.is_shell:
            plan.findings.append(
                IdentityFinding(
                    action=Action.DROP_ROW,
                    symbol=identity.symbol,
                    sheet=sheet,
                    reason="pre_existing_blank_shell",
                    detail="row arrived with no name and no price; emptied by an "
                    "earlier run, needs re-fetch",
                )
            )
            continue
        incoming.append(dict(row))

    working = incoming
    identities = [resolve_identity(row) for row in working]

    # Owners: rows with a healthy quote, indexed by normalised name. A name is
    # only "owned" when the row that carries it did NOT fail its quote.
    owners: Dict[str, List[int]] = {}
    for index, identity in enumerate(identities):
        if identity.name_key and identity.price is not None and not _quote_failed(identity):
            owners.setdefault(identity.name_key, []).append(index)

    contaminated: Set[int] = set()

    for index, identity in enumerate(identities):
        if not identity.symbol or identity.is_shell:
            continue

        ccy_ok = currency_is_consistent(identity.symbol, identity.currency)
        failed = _quote_failed(identity)

        # --- signature 1: currency contradicts the venue --------------------
        # Strongest signal, and independent of any name comparison. A .T row
        # quoting USD did not come from Tokyo.
        if ccy_ok is False:
            contaminated.add(index)
            plan.findings.append(
                IdentityFinding(
                    action=Action.QUARANTINE_FIELDS,
                    symbol=identity.symbol,
                    sheet=sheet,
                    reason=Reason.CURRENCY_MISMATCH,
                    detail=(
                        f"symbol implies {expected_currency_for(identity.symbol)} "
                        f"but row reports {identity.currency or 'blank'}"
                        + (" (quote also failed)" if failed else "")
                    ),
                    row_index=index,
                )
            )
            continue

        # --- signature 2: price magnitude belongs to another currency -------
        # Magnitude alone is NOT sufficient evidence to clear a row: BRK-A.US
        # legitimately trades at 738,500 USD. It only becomes conclusive when
        # the quote also failed, and that pairing separates cleanly on the live
        # data -- BRK-A.US has a healthy quote, while every borrowed price
        # (NUVA.US 144,700 "USD" = Posco's KRW, 4485.T 8.21 "JPY" = Suzano's
        # USD) carries quote_current_price_missing.
        asset_class = row_asset_class(working[index])
        if failed and price_is_plausible(
            identity.currency, identity.price,
            symbol=identity.symbol, asset_class=asset_class,
        ) is False:
            band = PRICE_BAND.get(identity.currency.upper(), (0.0, 0.0))
            contaminated.add(index)
            plan.findings.append(
                IdentityFinding(
                    action=Action.QUARANTINE_FIELDS,
                    symbol=identity.symbol,
                    sheet=sheet,
                    reason=Reason.PRICE_IMPLAUSIBLE,
                    detail=(
                        f"{identity.price:,.4g} is outside the plausible "
                        f"{identity.currency} range {band[0]:g}-{band[1]:,.0f}; "
                        "the number came from another currency"
                        + (" (quote also failed)" if failed else "")
                    ),
                    row_index=index,
                )
            )
            continue

        # --- signature 3: failed quote wearing another row's name -----------
        if failed and identity.name_key:
            healthy = [i for i in owners.get(identity.name_key, []) if i != index]
            if healthy:
                source = identities[healthy[0]]
                gap = ""
                if identity.price is not None and source.price:
                    gap = f", price within {abs(identity.price - source.price) / source.price:.1%}"
                contaminated.add(index)
                plan.findings.append(
                    IdentityFinding(
                        action=Action.QUARANTINE_FIELDS,
                        symbol=identity.symbol,
                        sheet=sheet,
                        reason=Reason.BORROWED_IDENTITY,
                        detail=(
                            f'quote failed, yet row carries "{identity.name}" which '
                            f"belongs to {source.symbol}{gap}"
                        ),
                        borrowed_from=source.symbol,
                        row_index=index,
                    )
                )
                continue

        # --- signature 4: failed quote, name from a fallback source ---------
        if failed and _name_from_fallback(identity):
            contaminated.add(index)
            plan.findings.append(
                IdentityFinding(
                    action=Action.QUARANTINE_FIELDS,
                    symbol=identity.symbol,
                    sheet=sheet,
                    reason=Reason.QUOTE_FAILED_UNVERIFIABLE,
                    detail="quote failed and name came from chart metadata, "
                    "so neither identity nor price is symbol-keyed",
                    row_index=index,
                )
            )

    # --- clear only the contaminated fields, never the whole row ------------
    for index in contaminated:
        row = working[index]
        for key in CONTAMINATED_FIELDS:
            if key in row:
                row[key] = None
        existing = str(row.get("Warnings") or "").strip()
        stamp = f"identity_guard_v{IDENTITY_GUARD_VERSION}:fields_quarantined"
        row["Warnings"] = f"{existing}; {stamp}" if existing else stamp
        for key in ("Block Reason", "block_reason"):
            if key in row:
                row[key] = "Identity unverified — re-fetch required"
                break
        for key in ("Investability Status", "investability_status"):
            if key in row:
                row[key] = "BLOCKED"
                break

    plan.rows = working

    # --- dedup runs LAST, on verified names only ---------------------------
    if run_dedup:
        # drop_shells=False: rows this run just field-quarantined must SURVIVE
        # as re-fetchable stubs. Pre-existing shells were already removed in
        # stage 0, so there is nothing left for the shell rule to clean up.
        result = dedupe_symbol_rows(
            plan.rows, sheet=sheet, drop_shells=False, flag_suspect_quotes=False
        )
        dropped_symbols: Set[str] = set()
        for finding in result.by_verdict(DedupVerdict.DUPLICATE):
            dropped_symbols.update(finding.dropped)
        for symbol in sorted(dropped_symbols):
            plan.findings.append(
                IdentityFinding(
                    action=Action.DROP_ROW,
                    symbol=symbol,
                    sheet=sheet,
                    reason=Reason.DUPLICATE,
                    detail="collapsed into the fresher row for the same "
                    "(base symbol, name, currency)",
                )
            )
        plan.rows = result.rows

    # --- invariant: this module must never empty a sheet -------------------
    _assert_no_mass_destruction(plan)
    return plan


def _assert_no_mass_destruction(plan: GuardPlan) -> None:
    """
    Guardrail the old firewall lacked.

    The 2026-07-20 incident blanked 9.5% of Market_Leaders in a single run with
    no alarm raised. Any run touching more than a quarter of a sheet is a bug in
    this module, not a data problem -- fail loudly instead of writing it.
    """
    if plan.input_count < 20:
        return
    touched = len(plan.by_action(Action.QUARANTINE_FIELDS)) + len(
        plan.by_action(Action.DROP_ROW)
    )
    share = touched / plan.input_count
    if share > 0.25:
        raise RuntimeError(
            f"identity_guard refused to write: it would alter {touched} of "
            f"{plan.input_count} rows ({share:.1%}) on sheet "
            f"'{plan.sheet or '?'}'. That is a guard defect, not a data "
            "problem. Investigate before overriding."
        )
