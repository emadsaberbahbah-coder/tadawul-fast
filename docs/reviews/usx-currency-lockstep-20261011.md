# WP-04: optional USD-cent quote lane

The export has thirteen futures rows whose Yahoo quote currency is USX
(USD cents). Repository parent-rate fallback previously knew no USX parent.
The optional `TFB_OPP_FX_USX_SUBUNIT` lane now recognizes USD as its parent
and divides the major-unit FX rate by 100. It reads the flag at call time;
unset, blank, `0`, or other unrecognized values retain prior behavior.
Arming accepts `1`, `true`, `yes`, and `on` (case and whitespace tolerant).

The existing resolver already accepts an exact supplied Sheet rate such as
`{"USX": 0.0375}` when the flag is off. That immediate operator/data fix
remains supported. Off means missing only when neither an exact supplied
USX row nor another pre-existing currency resolution exists; USD-only maps
and static USD do not establish USX FX until armed.

In the armed lane, the USD peg check runs before division. An exact supplied
USX rate is multiplied by 100 only for comparison with the major-unit peg;
a valid exact rate is returned unchanged. A rejected parent or raw subunit
rate becomes `0.0375` SAR per cent, tagged `static(peg-guard)/100`.
The candidate's explicit row FX override undergoes the same parent-unit peg
check; it cannot bypass rate sanity. Lowercase `usx` inputs honor a provided
canonical `USX` key in the armed lane, without folding case-sensitive GBp.
For example, the export's 479.75-cent corn quote values at 17.990625 SAR,
never 1,799.0625 SAR. Existing GBp/GBX and other nonpegged subunit paths
retain their original floating-point operations even when this lane is on.

USX support is coordinated across opportunity FX, identity/venue checks,
surface currency validation, the share-witnessed engine market-cap
normalizer, workbook market-cap audit, and the native holdings/cash
guards. The provider explicitly preserves USX as USX: relabeling the quote
USD would lose the price unit. Engine market-cap normalization additionally
requires its existing `TFB_SUBUNIT_MARKET_CAP_NORMALIZE` flag and a share
witness; already-major caps stay unchanged, and no-share rows are tagged
without division. Native reconciliation rejects USX when armed rather than
certifying cents as a native cash denomination. Sync's existing
`TFB_PF_MINOR_UNIT_CCY_GUARD` must also be armed to reject a ledger that
explicitly declares the minor token; a USX quote against a USD ledger
continues to fail the existing currency mismatch regardless of either flag.

Identity and surface warning stamps retain their established rule versions
(1.3.0 and 1.4.1). Release version metadata advances, but default-off row
annotations retain prior bytes. The identity module also exposes WP-05's
pure adapter to the shared crypto name screen; it does not add a new
identity-guard decision or mutation.

Versions to pin: opportunity builder 1.26.0, identity guard 1.3.1,
portfolio reconciliation 1.0.1, surface invariants 1.4.2, EODHD provider
4.18.3, workbook auditor 1.2.1. Engine `_SAI_REQUIRED_VERSION` is 1.4.2.
The parent integration owns engine/sync versions and deployment-manifest
edits; this change does not write to the workbook, Render, or GitHub.

Offline validation covers provided/static USX conversion, exact subunit
rate precedence, valid/invalid peg inputs, unchanged GBp paths, original
off-mode Sheet support, watchlist preservation, all portfolio flag
combinations, surface quarantine and fetch-failure demotions, native cash
certification, provider unit retention, workbook audit, and share-witnessed
engine cap normalization/idempotence. The new tests accompany the existing
opportunity, identity, reconciliation, surface and provider regressions.
The targeted eight-file regression run passed 183 tests; undefined-name
and unused-import lint passed for all USX-owned Python files.
An independent comparison with HEAD passed 490 default-off resolver,
identity, surface, provider and certification cases; only intentional
release labels were excluded. Review regressions additionally prove row FX
cannot bypass the armed peg check and lowercase/mixed USX cannot bypass
the holdings guard, market-cap normalizer or audit.

The operator still owns the live Sheet FX row and its Apps Script seed.
These futures remain watchlist contracts; currency support does not prove
broker execution, reconciliation evidence, quote freshness or eligibility.
