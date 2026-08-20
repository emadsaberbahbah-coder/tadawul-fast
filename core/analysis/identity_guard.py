#!/usr/bin/env python3
# core/analysis/identity_guard.py
"""
================================================================================
Identity Guard — v1.2.0   (replaces the destructive ID-FIREWALL dedup stage)
================================================================================

v1.2.0 — TOTAL-WIPE BLIND SPOT CLOSED (2026-08-20 production log)
------------------------------------------------------------------
EVIDENCE. Render production, 2026-08-21T00:37 Riyadh, engine v5.130.3,
TWICE in the same minute on consecutive /v1/analysis/sheet-rows batches:

    [engine_v2 v5.130.3] identity_guard v1.1.1: 10->0 rows | DROP_ROW=10
    [v5.130.3 RANK]  total=0 scored=0 skipped_no_score=0
    [v5.130.3 TRUST] total=0 low=0 med=0 high=0

Ten rows in, ZERO rows out, HTTP 200, no warning, no exception. The
ranker received an empty set and reported success. Twelve hours earlier
the same route logged `RANK total=10 scored=9`.

ROOT CAUSE — the safety net could not see it. _assert_no_mass_destruction
exists precisely to stop this (written after the 2026-07-20 incident that
blanked 9.5% of Market_Leaders unnoticed), but its first statement is:

    if plan.input_count < 20:
        return

A batch of 10 returns BEFORE the destruction check runs. So the one case
the guard most needs to catch — total annihilation of a small batch — is
the one case it is structurally blind to. 10 of 10 (100%) is a far
stronger defect signal than 26 of 100 (26%), yet only the latter raises.
The threshold was written as a PERCENTAGE test and small batches were
excluded to avoid false alarms on tiny samples; nobody asked what a 100%
share on a tiny sample means. It means the same thing at any size.

WHAT THIS VERSION DOES — and deliberately does NOT do:
  * ADDS a total-wipe detector that fires at ANY input_count >= 1 when a
    non-empty input yields an empty output. It emits ONE structured
    WARNING naming every dropped symbol and its reason, so the next
    occurrence is self-diagnosing from the log alone. It does NOT raise.
  * DOES NOT change the existing >25% rule for input_count >= 20. That
    predicate, its threshold and its RuntimeError are byte-identical.
  * DOES NOT raise on total wipes by default. RATIONALE: this fix is
    written from ONE production sample. I have not yet traced whether
    small all-shell batches are a routine, legitimate outcome (a page of
    genuinely delisted symbols would produce exactly this). Converting an
    unmeasured frequency into a hard failure would turn an observability
    gap into an outage — the same mistake in the opposite direction. The
    log line is the measurement; the escalation waits for it.
  * OFFERS the escalation as an opt-in ENV, TFB_IDENTITY_WIPE_RAISE,
    DEFAULT OFF, so once two clean days of logs establish the frequency
    the operator can arm fail-closed without another code change.
    ARMED SEMANTICS AT THE CALL SITE (verified in data_engine_v2): the
    engine wraps guard_sheet_rows in `except RuntimeError` and RETURNS
    THE ORIGINAL ROWS with an ERROR log. So arming does NOT produce an
    HTTP failure — it converts "empty batch, silently" into "original
    rows pass through unguarded, loudly". The shells stay visible
    downstream instead of vanishing, which is the fail-open-with-evidence
    behaviour the panel needs.
  * REVIEW FIX (E1/E2): total wipes at input_count >= 20 keep the
    v1.1.1 RuntimeError byte-identically (the new branch only adds the
    report line before it). The first draft accidentally swallowed that
    raise; the differential shape that would have caught it (all-shell
    >= 20) is now harness case S5.7/S6.all_shell_25.

CONTEXT THE SAME LOG SUPPLIES. All ten rows were dropped as
`pre_existing_blank_shell` — "arrived with no name and no price". The
same minute shows ~80 provider failures across ten symbols Yahoo reports
as delisted (ERJ, P10, SCVL, SEMR, ALFAA.MX, AUO, GES, 6641.T, APLS,
NVEI.TO), each retried over 5d/1mo/3mo/1y/2y plus quoteSummary. Dead
symbols -> blank shells -> total wipe. That upstream universe question is
NOT addressed here: this module's job is to notice, loudly, that it just
emptied a batch. Silence was the defect.

DEFAULT-OFF GUARANTEE: with TFB_IDENTITY_WIPE_RAISE unset, the only
observable difference from v1.1.1 is one additional WARNING log line on
a run that would previously have been silent. No row, no action, no
verdict, no return value changes.

v1.1.1 — FLAG-COMBINATION HOLE CLOSED (Codex review, PR #118)
-------------------------------------------------------------
v1.1.0 made the two gates fully independent. That admitted a combination
I had not reasoned about: TFB_IDENTITY_QUARANTINE_KEYS=1 with
TFB_SURFACE_ACTION_PRECEDENCE=0 writes investability_status=BLOCKED and
clears `recommendation`, but leaves final_action=INVEST untouched -- the
exact contradiction this module exists to remove, and STRICTLY WORSE than
v1.0.0: with the recommendation blanked, nothing on the row explains the
INVEST any more.

The planned rollout order (precedence first, quarantine after 2026-08-16)
happens to avoid it, but a correctness invariant must not depend on the
operator arming flags in a particular sequence.

FIX: quarantining a row now forces the action whenever EITHER gate is on.
Rejected alternative -- forcing DO_NOT_INVEST unconditionally -- would drop
the gate and break the default-OFF byte-identity guarantee the S-1 window
requires. Both gates unset still yields v1.0.0 behaviour exactly.

v1.1.0 WHY-BLOCK — THE QUARANTINE NEVER QUARANTINED ANYTHING
------------------------------------------------------------
Root cause, measured on the live 2026-08-05 Global_Markets export (6,646 rows):

    identity-unverified rows ................  82
    ...carrying the quarantine stamp ........   0   <- should have been 82
    ...still carrying Name ..................  82   <- should have been 0
    ...still carrying Recommendation ........  82   <- should have been 0
    ...still carrying Expected ROI 12M ......  82   <- should have been 0
    ...still carrying Current Price .........  74   <- should have been 0
    ...still carrying Rank (Overall) ........  72   <- should have been 0
    quarantine stamps anywhere in the file ..   0

CONTAMINATED_FIELDS holds PHYSICAL SHEET HEADERS ("Name", "Current Price",
"Rank (Overall)"). Rows reaching this module on the engine sheet-rows path
(data_engine_v2._bb2_apply_identity_guard, called at the get_sheet_rows
boundary AFTER _project_rows_with_trust_carry) have already been projected
onto the canonical snake_case contract -- the code immediately after that
call site writes r["investability_status"] / r["final_action"] /
r["block_reason"]. So `if key in row` was False for all 34 fields on every
production row and the quarantine cleared NOTHING, while
row["Warnings"] = stamp created an orphan Title-Case key that the 115-key
projection then stripped -- destroying the audit trail that would have
exposed the no-op.

The two writes that DID land are the two that already carried a snake_case
fallback -- ("Block Reason", "block_reason") and ("Investability Status",
"investability_status"). That asymmetry is the whole reason the defect read
as a cosmetic mismatch instead of a dead guard: the verdict was published,
the enforcement was not.

Why 47 CI tests did not catch it: every fixture in tests/test_identity_guard.py
builds Title-Case rows, so the suite has only ever exercised a key casing
production does not use. New snake_case cases are added rather than replacing
the Title-Case ones -- both callers must work.

A THIRD defect, independent of casing: `final_action` was never referenced
at all, so a row demoted to investability_status=BLOCKED kept whatever action
the engine gate had already assigned. Live evidence, same export:
BSANTANDER.SN, VCB.VN, VPB.VN, MBB.VN all published
Investability Status=BLOCKED alongside Final Action=INVEST.

FIXES (all additive, zero removals, all env-gated DEFAULT OFF):

  TFB_IDENTITY_QUARANTINE_KEYS (default OFF)
      Clears every spelling a contaminated field is known by
      (CONTAMINATED_FIELD_ALIASES, keys taken from
      core/sheets/schema_registry.py -- NOT inferred), and routes the
      warning stamp through the same case-tolerant writer so the audit
      trail survives projection. MATERIAL: arming this blanks name/price/
      forecast/recommendation/rank on rows the guard already judged
      contaminated, which changes what the decision surface shows.
      Arm AFTER the S-1 certification window closes (2026-08-16).

  TFB_SURFACE_ACTION_PRECEDENCE (default OFF)
      A quarantined row is BLOCKED, so its action is forced to
      DO_NOT_INVEST. DISPLAY-ONLY in the current wiring: the Top_10
      selector filters on Investability Status, not Final Action -- the
      2026-08-05 candidate audit shows all four leak rows already rejected
      with "First Fail = Investability: BLOCKED". Safe to arm immediately.

With both unset this module is behaviourally identical to v1.0.0. The only
output delta is the version substring inside the quarantine stamp itself
(identity_guard_v1.1.0:fields_quarantined), which is deliberate provenance:
the stamp records which guard version acted. On the production snake_case
path that stamp is written to an orphan key and stripped, so the served
rows are byte-identical.

NOT FIXED HERE, and deliberately not folded in: the 66 Global_Markets rows
whose price sits outside their own 52-week range. Only 1 of those 66 is in
the identity-unverified set, so this defect does not explain them and
claiming otherwise would be a false close.

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

import os
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

IDENTITY_GUARD_VERSION = "1.2.0"
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
    "CONTAMINATED_FIELD_ALIASES",
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
# v1.1.0 -- case-tolerant field resolution
# ---------------------------------------------------------------------------
# CONTAMINATED_FIELDS above holds the PHYSICAL SHEET HEADERS and is preserved
# verbatim (it remains the single ordered source of truth for WHICH fields are
# contaminated). This table adds, per header, EVERY key that field is known by
# on the paths that reach this module. The snake_case names are taken from
# core/sheets/schema_registry.py -- they are read from the registry, not
# guessed. Extra provider/engine aliases are included where the engine is known
# to write them, because leaving an alias populated would let a later stage
# resurrect the contaminated value: _apply_investability_gate (v5.79.5 Fix Q)
# explicitly backfills current_price from the `price` alias, so clearing only
# "Current Price"/"current_price" would be undone one stage later.
CONTAMINATED_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "Name": ("Name", "name", "company_name", "companyName", "long_name"),
    "Current Price": ("Current Price", "current_price", "currentPrice", "price", "last_price"),
    "Previous Close": ("Previous Close", "previous_close", "previousClose"),
    "Open": ("Open", "open_price", "open", "day_open"),
    "Day High": ("Day High", "day_high", "high"),
    "Day Low": ("Day Low", "day_low", "low"),
    "52W High": ("52W High", "week_52_high", "fifty_two_week_high"),
    "52W Low": ("52W Low", "week_52_low", "fifty_two_week_low"),
    "Price Change": ("Price Change", "price_change", "change"),
    "Percent Change": ("Percent Change", "percent_change", "change_percent"),
    "52W Position %": ("52W Position %", "week_52_position_pct"),
    "Market Cap": ("Market Cap", "market_cap", "marketCap"),
    "Float Shares": ("Float Shares", "float_shares"),
    "P/E (TTM)": ("P/E (TTM)", "pe_ttm"),
    "P/E (Forward)": ("P/E (Forward)", "pe_forward"),
    "EPS (TTM)": ("EPS (TTM)", "eps_ttm"),
    "Intrinsic Value": ("Intrinsic Value", "intrinsic_value"),
    "Upside %": ("Upside %", "upside_pct"),
    "Valuation Score": ("Valuation Score", "valuation_score"),
    "Forecast Price 1M": ("Forecast Price 1M", "forecast_price_1m"),
    "Forecast Price 3M": ("Forecast Price 3M", "forecast_price_3m"),
    "Forecast Price 12M": ("Forecast Price 12M", "forecast_price_12m"),
    "Expected ROI 1M": ("Expected ROI 1M", "expected_roi_1m"),
    "Expected ROI 3M": ("Expected ROI 3M", "expected_roi_3m"),
    "Expected ROI 12M": ("Expected ROI 12M", "expected_roi_12m"),
    "Overall Score": ("Overall Score", "overall_score"),
    "Opportunity Score": ("Opportunity Score", "opportunity_score"),
    "Rank (Overall)": ("Rank (Overall)", "rank_overall"),
    "Recommendation": ("Recommendation", "recommendation"),
    "Recommendation Detail": ("Recommendation Detail", "recommendation_detailed"),
    "Recommendation Reason": ("Recommendation Reason", "recommendation_reason"),
    "Target Price": ("Target Price", "target_price", "analyst_target_price"),
    "Upside/Downside %": ("Upside/Downside %", "upside_downside_pct"),
    "Analyst Rating": ("Analyst Rating", "analyst_rating", "analyst_recommendation"),
}

# Warning / action fields, in the order a writer should try them.
_WARNING_KEYS: Tuple[str, ...] = ("Warnings", "warnings", "warning", "flags")
_ACTION_KEYS: Tuple[str, ...] = ("Final Action", "final_action")

# Key created when a row carries no warnings field at all. snake_case, because
# that is the canonical 115-key contract this module sits inside; a Title-Case
# key would be stripped at projection (the v1.0.0 defect).
_WARNING_FALLBACK_KEY = "warnings"


def _env_flag_on(name: str, default: str = "0") -> bool:
    """True when the env flag is set to an affirmative value. Default OFF."""
    return (os.getenv(name) or default).strip().lower() in ("1", "true", "yes", "on")


def _quarantine_keys_enabled() -> bool:
    """v1.1.0 case-tolerant quarantine. DEFAULT OFF -> exact v1.0.0 behaviour.

    MATERIAL when armed: it blanks the display/decision fields on rows the guard
    judged contaminated. Arm only after the S-1 window closes (2026-08-16).
    """
    return _env_flag_on("TFB_IDENTITY_QUARANTINE_KEYS", "0")


def _action_precedence_enabled() -> bool:
    """BLOCKED => Final Action DO_NOT_INVEST. DEFAULT OFF.

    Display-only in the current wiring (the Top_10 selector gates on
    Investability Status), so this is safe to arm inside the S-1 window.
    """
    return _env_flag_on("TFB_SURFACE_ACTION_PRECEDENCE", "0")


def _field_spellings(header: str) -> Tuple[str, ...]:
    """Every key `header` may appear under. Falls back to the header itself."""
    return CONTAMINATED_FIELD_ALIASES.get(header, (header,))


def _clear_contaminated_field(row: Dict[str, Any], header: str) -> bool:
    """Blank EVERY present spelling of `header`. Never creates a key.

    Unlike a first-match write, this clears all aliases: leaving one populated
    is exactly how a cleared value gets resurrected downstream.
    """
    cleared = False
    for key in _field_spellings(header):
        if key in row:
            row[key] = None
            cleared = True
    return cleared


def _set_first_present(
    row: Dict[str, Any], candidates: Sequence[str], value: Any
) -> bool:
    """Write `value` to the first candidate key present. Never creates a key.

    Mirrors the v1.0.0 idiom already used for Block Reason / Investability
    Status: a strict schema contract means inventing a key is worse than
    skipping the write.
    """
    for key in candidates:
        if key in row:
            row[key] = value
            return True
    return False


def _append_guard_warning(row: Dict[str, Any], stamp: str) -> None:
    """Append `stamp` to whichever warnings field the row carries.

    Idempotent: a stamp already present is not duplicated, so a second guard
    pass over the same rows is byte-identical.
    """
    for key in _WARNING_KEYS:
        if key in row:
            existing = str(row.get(key) or "").strip()
            if stamp in existing:
                return
            row[key] = f"{existing}; {stamp}" if existing else stamp
            return
    row[_WARNING_FALLBACK_KEY] = stamp


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
    quarantine_keys_on = _quarantine_keys_enabled()   # v1.1.0
    action_precedence_on = _action_precedence_enabled()  # v1.1.0
    stamp = f"identity_guard_v{IDENTITY_GUARD_VERSION}:fields_quarantined"
    for index in contaminated:
        row = working[index]
        if quarantine_keys_on:
            # v1.1.0: clear every spelling, so a snake_case projected row is
            # actually quarantined instead of silently skipped.
            for header in CONTAMINATED_FIELDS:
                _clear_contaminated_field(row, header)
        else:
            # v1.0.0 path, preserved verbatim.
            for key in CONTAMINATED_FIELDS:
                if key in row:
                    row[key] = None
        if quarantine_keys_on:
            # v1.1.0: stamp the field the row actually carries, so the audit
            # trail survives the 115-key projection.
            _append_guard_warning(row, stamp)
        else:
            # v1.0.0 path, preserved verbatim.
            existing = str(row.get("Warnings") or "").strip()
            row["Warnings"] = f"{existing}; {stamp}" if existing else stamp
        for key in ("Block Reason", "block_reason"):
            if key in row:
                row[key] = "Identity unverified — re-fetch required"
                break
        for key in ("Investability Status", "investability_status"):
            if key in row:
                row[key] = "BLOCKED"
                break
        # v1.1.0: a BLOCKED row must never publish an INVEST action.
        # Never creates the key -- a row without an action field is a
        # caller that does not carry one, not a row to invent one for.
        #
        # v1.1.1 (Codex review, PR #118): fires when EITHER gate is armed.
        # Quarantine-armed-alone used to publish BLOCKED + INVEST with the
        # recommendation already blanked -- worse than doing nothing. The
        # action is now tied to the BLOCK itself, not to one flag, so no
        # arming order can produce an incoherent row.
        if action_precedence_on or quarantine_keys_on:
            _set_first_present(row, _ACTION_KEYS, "DO_NOT_INVEST")

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


def _wipe_raise_enabled() -> bool:
    """v1.2.0: escalate a TOTAL wipe from WARNING to RuntimeError.
    DEFAULT OFF -- unset keeps v1.1.1 behaviour except for one log line.
    Arm only after the log has established that total wipes are not a
    routine, legitimate outcome for small all-shell batches."""
    return (os.getenv("TFB_IDENTITY_WIPE_RAISE") or "0").strip().lower() in (
        "1", "true", "yes", "on")


def _wipe_report(plan: GuardPlan) -> str:
    """v1.2.0: one self-diagnosing line. Names every dropped symbol with its
    reason, so the NEXT occurrence needs no code archaeology to interpret."""
    tally: Dict[str, int] = {}
    for finding in plan.findings:
        key = str(getattr(finding, "reason", "") or "unspecified")
        tally[key] = tally.get(key, 0) + 1
    reasons = " ".join(f"{k}={v}" for k, v in sorted(tally.items()))
    symbols = []
    seen: Set[str] = set()
    for finding in plan.findings:
        sym = str(getattr(finding, "symbol", "") or "")
        if sym and sym not in seen:
            seen.add(sym)
            symbols.append(sym)
    shown = ", ".join(symbols[:20]) + ("…" if len(symbols) > 20 else "")
    return (
        f"identity_guard v{IDENTITY_GUARD_VERSION} TOTAL WIPE: "
        f"{plan.input_count}->0 rows on sheet '{plan.sheet or '?'}' | "
        f"reasons: {reasons or 'none recorded'} | symbols: {shown or 'none'}"
    )


def _assert_no_mass_destruction(plan: GuardPlan) -> None:
    """
    Guardrail the old firewall lacked.

    The 2026-07-20 incident blanked 9.5% of Market_Leaders in a single run with
    no alarm raised. Any run touching more than a quarter of a sheet is a bug in
    this module, not a data problem -- fail loudly instead of writing it.

    v1.2.0: the percentage rule below skips input_count < 20, which made a
    100%-destruction event on a 10-row batch INVISIBLE (production, twice,
    2026-08-21T00:37). The total-wipe check runs FIRST and at any size.
    """
    # --- v1.2.0 TOTAL-WIPE DETECTOR ------------------------------------
    # Review fix (self-review E1/E2, 2026-08-21): the first draft returned
    # here for EVERY total wipe, which SWALLOWED the RuntimeError the
    # existing >25% rule has always raised for input_count >= 20 — i.e. it
    # weakened the old protection for exactly its strongest case (25
    # shells -> 0). Order is therefore: report FIRST (any size, so >=20
    # wipes gain the same self-diagnosing line), then for input < 20 the
    # new warn/opt-in-raise path, and for input >= 20 FALL THROUGH so the
    # v1.1.1 percentage rule raises byte-identically.
    if plan.input_count >= 1 and not plan.rows:
        _report = _wipe_report(plan)
        try:
            print("::warning::" + _report)
        except Exception:
            pass
        if plan.input_count < 20:
            if _wipe_raise_enabled():
                raise RuntimeError(
                    _report + " | TFB_IDENTITY_WIPE_RAISE is armed: "
                    "refusing to return an empty batch."
                )
            return
        # input_count >= 20: fall through to the unchanged v1.1.1 rule,
        # which raises (share = 100% > 25%). Engine call-site catches
        # RuntimeError and passes the ORIGINAL rows through with an ERROR
        # log — raise here means "refuse the wipe", not a 500.

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
