"""core/sectors.py -- canonical sector-taxonomy normalization (single source of truth).

__version__ = "1.0.0"   (TFB Track A / A2 -- sector taxonomy unification)

WHY THIS MODULE EXISTS
----------------------
TFB ingests sector labels from TWO vocabularies that coexist across the universe:

  * GICS  -- emitted for KSA (.SR) names by
             ``data_engine_v2._ksa_sector_for_symbol`` (the ``_KSA_SYMBOL_SECTOR``
             map): "Financials", "Consumer Staples", "Consumer Discretionary",
             "Health Care", "Information Technology", "Materials", ...
  * Yahoo -- emitted for global / US names by the Yahoo fundamentals provider as
             the raw provider string: "Financial Services", "Consumer Defensive",
             "Consumer Cyclical", "Healthcare", "Technology", "Basic Materials", ...

Only SIX sector spellings differ between the two taxonomies; every other sector
name is byte-identical across both. Because the per-sector diversification caps
in BOTH decision tabs -- opportunity_builder's Top_10 funding/diversification cap
and top10_selector's W-5 cap -- key on the sector STRING, the same economic
sector written two ways becomes two cap buckets. A Saudi bank ("Financials") and
a US bank ("Financial Services") then each occupy their OWN bucket and the
concentration cap is silently dodged (the H2 finding).

This module is the ONE canonical map. GICS is the canonical target -- it matches
the ``_KSA_SYMBOL_SECTOR`` vocabulary that already governs every KSA row, and the
v5.99.1 Fix AM financials detector. opportunity_builder historically carried a
private copy of this map (added v1.0.13); as of A2 it imports from HERE, and new
consumers (top10_selector, and any future tab) import from HERE too -- so the map
CANNOT drift between files. Cross-file taxonomy drift is the exact class of bug
this fix is meant to eliminate, so the map must live in exactly one place.

The six divergences below are GICS-canonical and EXHAUSTIVE: they are verified
against the two 11-sector taxonomies. The full GICS sector set is enumerated in
``GICS_SECTORS`` for callers that want to assert a value already canon.

NON-NEGOTIABLES honoured here
-----------------------------
* Pure functions, no side effects, no environment reads. The on/off GATE is owned
  by each CALLER (e.g. ``TFB_OPP_SECTOR_NORMALIZE`` / ``TFB_TOP10_SECTOR_NORMALIZE``)
  so activation stays a deliberate, verifiable operator step -- this module never
  decides for itself whether normalization is on.
* Unknown / "" / any already-GICS or unrecognized string passes through unchanged,
  so the function is safe to call on ANY sector value without guarding the caller.
* Dependency-free: imports nothing from core, so it can never create an import
  cycle and is always importable standalone.
"""

from __future__ import annotations

from typing import Dict, FrozenSet

__version__ = "1.0.0"

# The SIX Yahoo-provider sector spellings that differ from GICS -> their GICS form.
# Exhaustive across the two 11-sector taxonomies; every other Yahoo sector string
# already equals its GICS counterpart and therefore needs no entry here.
YAHOO_TO_GICS_SECTOR: Dict[str, str] = {
    "Basic Materials": "Materials",
    "Healthcare": "Health Care",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Technology": "Information Technology",
    "Financial Services": "Financials",
}

# The canonical (GICS) 11-sector set. Provided for callers that want to validate
# that a normalized value landed on a real GICS sector (an unmapped long-tail
# label intentionally does NOT, and is treated as a data gap by the caller).
GICS_SECTORS: FrozenSet[str] = frozenset({
    "Energy",
    "Materials",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Health Care",
    "Financials",
    "Information Technology",
    "Communication Services",
    "Utilities",
    "Real Estate",
})


def normalize_sector(s) -> str:
    """Translate a Yahoo-provider sector spelling to its GICS canonical form.

    Already-GICS strings, "Unknown", "" and any unrecognized string pass through
    unchanged (leading/trailing whitespace is trimmed). This is a PURE function:
    no env reads, no side effects -- the caller owns the on/off gate and any
    audit logging of which values were remapped.
    """
    t = (s or "").strip()
    return YAHOO_TO_GICS_SECTOR.get(t, t)


def is_gics_sector(s) -> bool:
    """True iff ``s`` (trimmed) is one of the 11 canonical GICS sectors."""
    return (s or "").strip() in GICS_SECTORS


__all__ = [
    "__version__",
    "YAHOO_TO_GICS_SECTOR",
    "GICS_SECTORS",
    "normalize_sector",
    "is_gics_sector",
]
