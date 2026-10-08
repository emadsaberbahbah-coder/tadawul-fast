# Final market Sheets presentation transport

The direct daily-sync writer could restore legacy last-good rows after API presentation and publish their old margin/return/horizon cells. The SDK writer also sent unknown display values as JSON null. Google Values updates skip null cells, so an outgoing unknown did not remove the prior bad display quantity.

Sync 6.64.14 and SDK service 6.2.2 now use the shared sheet-presentation 1.0.1 transport helper for exactly Market_Leaders, Global_Markets, Mutual_Funds and Commodities_FX with the canonical 115-column matrix. The direct runner applies it after all restoration and the preserved-acquisition marker, before any clear/hold/write. The direct writer applies it after its existing fill guard; the SDK writer applies it immediately before both actual Values update transports.

The shared wire helper works on copies. Unknown margin, derived return/upside and horizon-label cells become explicit empty strings, so Values updates clear the prior cell. Proven fractional margin receipts survive repeated writes; a legacy margin diagnostic alone cannot authorize a conversion. A 365-day primary horizon displays 1Y, and incoherent forecast/return pairs withdraw the supplied display return. Prices, currencies, scores, acquisition times and preserved/failed proof remain unchanged. API JSON still represents unknowns as None. No rollout mode is activated, and no cash, portfolio, history or generic financial-tab transport is changed.

Market headers and row widths are validated before rectangularization can hide missing or extra cells. Invalid canonical market matrices fail before clear or update; there is no fallback that writes the raw malformed payload. Accepted tuple rows are normalized only on the market path, preserving other pages' prior writer contract.

All evidence is synthetic and offline. The immutable baseline is main a356ed8576f45944f6f0812e8bc37ae641e5d47f. Before the repair, actual direct SDK writes retained an unproven 0.9 margin, and the actual runner restored and published that quantity through KEEP-LAST-GOOD. The SDK null output also retained a seeded prior value under actual Google Values null-skip semantics.

Verification on the frozen candidate:

- 36 new tests exercise actual direct and SDK update/batchUpdate payloads, seeded null-skip/empty-clear behavior, all four pages, margin receipt idempotence, exact canonical shape, accepted tuples, actual KEEP-LAST-GOOD and late PV2 restoration, the composed runner-to-real-writer path, and guard failure before any clear/hold/update.
- Pinned Python 3.11: 134 passed across the new suite and existing presentation contracts.
- True lean Python 3.11: 469 passed across these suites and existing acquisition, portfolio-label, Insights publication, fetch-failure truth and 6.64.13 clock regressions. Two existing test-return warnings remain.
- `git diff --check` passed. The existing SDK fixtures now use real canonical market schemas; their fraction, numeric-string, idempotence and unrelated preservation assertions remain meaningful.

The independent reviewer checked both publisher paths and the seeded Values transport. Combined release CI, exact deployment/module readback and a controlled post-release market refresh remain required. These tests do not attest broker balances, live supplier accuracy, a bound Apps Script install, or repaired historical rows before that refresh.
