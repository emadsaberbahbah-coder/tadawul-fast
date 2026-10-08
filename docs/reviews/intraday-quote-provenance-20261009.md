# Intraday quote publication contract — 9 October 2026 (Riyadh)

`scripts/intraday_quote_refresh.py` 1.0.2 keeps the targeted quote-refresh
workflow while disclosing that its existing model row was carried forward.
Changing a price must not retain contradictory ROI displays or certify the
old model as a successful current acquisition.

An incoming row needs a positive finite price, explicit successful acquisition
receipt, a recognized provider, precise source quote time, and native currency
agreement with the target row. Failed, preserved, stale, unverified, ambiguous,
or conflicting duplicate evidence is refused. Raw source quote-clock aliases
must agree exactly with the typed quote time. Acquisition time and source quote
time remain separate. Existing opportunity-builder quote freshness rules are
used: recent quotes can pass without calendar dependencies; an older closing
quote needs the existing venue/session proof. Calendar unavailability defers
that quote. No currency conversion is inferred; ambiguous `GBp` is refused.

The allowed destination pages are `Market_Leaders`, `Global_Markets`,
`Commodities_FX`, and `Mutual_Funds`; defaults remain the first two. A patch
changes only the existing price cell, every existing UTC/Riyadh retrieval stamp,
one unambiguous Warnings cell, and conflicting derived return cells identified
by `core.sheet_presentation.present_instrument_row`. Returns are explicitly
blanked using RAW `[[""]]`; model prices, margins, scores, recommendations,
Data Provider, identities, manual inputs and row membership are retained.
Warnings record the actual quote provider/time/currency and
`acquisition_status:preserved`. The shared classifier therefore withholds
full-model acquisition success until a new complete engine row replaces it.
The operational `_Run_Log` append remains separate from market-row membership.

All data/proof reads use `UNFORMATTED_VALUE`. Target timestamp aliases must
normalize to the same UTC whole second. This accommodates the canonical
producer's separately sampled UTC and Riyadh clocks (observed 20-microsecond
jitter); the maximum actual timestamp is retained as the overwrite bound.
Different whole seconds are refused. This allowance does not relax exact source
quote-clock agreement. Both retrieval and known source quote time must advance.

Every row's complete cell patch stays within one batch. Immediately before
each batch, the script re-reads raw values and verifies the complete header and
row fingerprint, then rebuilds the plan against the same immutable quote
evidence. Concurrent identity, manual, price, model, stamp or header changes
abandon that row. Only successful RPCs count as written. Google Sheets offers
no compare-and-swap, so a writer changing a row after this read remains a narrow
race; the script does not claim transactional isolation. Rechecking a complete
page per small batch also adds read overhead to a large destination page.

Offline regression coverage exercises actual `main`, HTTP envelope parsing and
fake Sheets transport, including the registry's exact 115-column schema and
warning-only receipts after projection. It checks invalid incoming proof,
duplicate conflicts in either order, source/target clocks, native units,
membership and financial-page refusal, RAW return blanks, full-row races,
multi-batch rechecks and truthful write counts. The existing learning guards
now use witnessed fixtures without skipping their original refusal paths.
The standalone `--selftest` also uses successful source receipts.
