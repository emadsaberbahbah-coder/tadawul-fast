#!/usr/bin/env python3
"""Exact issuer firewall for the audited high-risk market-symbol subset.

The generic identity guard detects broad cross-symbol leakage, but it cannot
prove an issuer mismatch when the legitimate owner is absent from the same
response page.  This module adds a small, evidence-backed Symbol->Issuer
registry for the rows observed contaminated in the live workbook on
2026-08-01.

It wraps the already-installed market-truth identity guard.  A known issuer
mismatch is converted to an explicit decision-blocked stub: symbol and
deterministic venue/provenance fields survive, while unverified facts,
scores, forecasts, ranks and recommendations become unknown.

No network call is made.  No price, score, rank, forecast or recommendation is
created.
"""
from __future__ import annotations

import re
from typing import Any, Mapping, MutableMapping

PATCH_VERSION = "1.0.0"
__version__ = PATCH_VERSION

WARNING_TAG = f"identity_quarantined:urgent_issuer_registry:v{PATCH_VERSION}"
FINDING_REASON = "known_issuer_mismatch"

# Evidence-backed subset from the live Market_Leaders contamination observed on
# 2026-08-01.  This is intentionally not presented as a complete instrument
# master.  Unknown symbols remain governed by the generic identity guard.
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
    "ADNOCDIST.AB": (
        "adnoc distribution",
        "abu dhabi national oil company for distribution",
    ),
    "BOROUGE.AB": ("borouge",),
    "ADIB.AB": ("abu dhabi islamic bank",),
    "OQGN.OM": ("oq gas networks",),
}

# Only these fields survive a known issuer mismatch.  All other values may
# belong to the wrong security and are cleared.  Normalisation makes this work
# for both canonical snake_case engine rows and display-header Sheet rows.
_SAFE_FIELD_KEYS = {
    "symbol",
    "ticker",
    "code",
    "assetclass",
    "exchange",
    "market",
    "exchangecode",
    "currency",
    "currencycode",
    "country",
    "countryname",
    "dataprovider",
    "provider",
    "datasource",
    "providersecondary",
    "lastupdatedutc",
    "lastupdatedriyadh",
    "rowupdatedutc",
    "rowsource",
    "warnings",
    "warning",
    "investabilitystatus",
    "finalaction",
    "blockreason",
}

_INSTALLED = False


def _norm_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _text(value: Any) -> str:
    return "" if value is None else " ".join(str(value).strip().split())


def _get(row: Mapping[str, Any], canonical: str, display: str) -> Any:
    if canonical in row:
        return row.get(canonical)
    return row.get(display)


def _set(
    row: MutableMapping[str, Any],
    canonical: str,
    display: str,
    value: Any,
) -> None:
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


def _issuer_mismatch(row: Mapping[str, Any]) -> tuple[str, str] | None:
    symbol = _text(_get(row, "symbol", "Symbol")).upper()
    tokens = EXPECTED_ISSUER_TOKENS.get(symbol)
    if not tokens:
        return None

    seen_name = _text(_get(row, "name", "Name"))
    normalized = seen_name.casefold()
    if normalized and any(token in normalized for token in tokens):
        return None
    return symbol, seen_name


def _quarantine_known_mismatch(
    row: MutableMapping[str, Any],
    *,
    symbol: str,
    seen_name: str,
) -> None:
    for key in list(row):
        if _norm_key(key) not in _SAFE_FIELD_KEYS:
            row[key] = None

    _set(row, "symbol", "Symbol", symbol)
    _set(row, "investability_status", "Investability Status", "BLOCKED")
    _set(row, "final_action", "Final Action", "DO_NOT_INVEST")
    _append_block_reason(
        row,
        "Known Symbol/Issuer mismatch — verified re-fetch required",
    )
    _append_warning(row, WARNING_TAG)
    if seen_name:
        _append_warning(
            row,
            "issuer_mismatch_seen_name:" + seen_name[:80].replace(";", ","),
        )


def ensure_urgent_issuer_firewall() -> bool:
    """Install the wrapper after ``identity_guard`` is fully initialized."""
    global _INSTALLED

    from core.analysis import identity_guard

    if getattr(identity_guard, "_TFB_URGENT_ISSUER_FIREWALL_PATCHED", False):
        _INSTALLED = True
        return True

    original_guard = getattr(identity_guard, "guard_sheet_rows", None)
    if not callable(original_guard):
        return False

    def guard_sheet_rows(
        rows: Any,
        sheet: str = "",
        *,
        run_dedup: bool = True,
    ) -> Any:
        plan = original_guard(rows, sheet=sheet, run_dedup=run_dedup)
        corrected = []
        mismatches: list[tuple[str, str]] = []

        for raw in plan.rows:
            if not isinstance(raw, Mapping):
                corrected.append(raw)
                continue
            row = dict(raw)
            mismatch = _issuer_mismatch(row)
            if mismatch is not None:
                symbol, seen_name = mismatch
                _quarantine_known_mismatch(
                    row,
                    symbol=symbol,
                    seen_name=seen_name,
                )
                mismatches.append((symbol, seen_name))
            corrected.append(row)

        plan.rows = corrected
        existing = {
            (str(getattr(item, "symbol", "")), str(getattr(item, "reason", "")))
            for item in getattr(plan, "findings", [])
        }
        for symbol, seen_name in mismatches:
            key = (symbol, FINDING_REASON)
            if key in existing:
                continue
            try:
                plan.findings.append(
                    identity_guard.IdentityFinding(
                        action=identity_guard.Action.QUARANTINE_FIELDS,
                        symbol=symbol,
                        sheet=sheet,
                        reason=FINDING_REASON,
                        detail=(
                            "audited Symbol->Issuer registry rejected "
                            f"{seen_name!r}; all unverified facts cleared"
                        ),
                    )
                )
                existing.add(key)
            except Exception:
                pass
        return plan

    identity_guard.guard_sheet_rows = guard_sheet_rows
    identity_guard._TFB_URGENT_ISSUER_FIREWALL_PATCHED = True
    identity_guard._TFB_URGENT_ISSUER_FIREWALL_VERSION = PATCH_VERSION
    _INSTALLED = True
    return True


__all__ = [
    "PATCH_VERSION",
    "WARNING_TAG",
    "FINDING_REASON",
    "EXPECTED_ISSUER_TOKENS",
    "ensure_urgent_issuer_firewall",
]
