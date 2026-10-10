# Universe and pipeline investigation — 11 October 2026

The available workbook is an older evidence source than the handoff's claimed
10 October snapshot. Its SHA-256 is
`9c8b34812eee7d5554a3586f1f86a232e79159f24e228a4eac070e2436441ef4`;
the last `_Run_Log` timestamp is 6 October 2026 at 23:46:43 Riyadh. The market
pages and status rows predominantly represent 6 October, with older retained
rows. It cannot validate the handoff's 8–10 October health, acquisition,
scheduler or recovery claims. Measurements were made offline with openpyxl
in read-only/data-only mode; the aggregate evidence is preserved in
`docs/evidence/pipeline-export-investigation-20261011.json`.

WP-09: recurring identity-firewall strips are real in this available history.
The 14,730 run-log rows include 771 ID-FIREWALL events. Global_Markets shows:

| Riyadh date | Events | Reported strips per event |
| --- | --- | --- |
| 30 September | 3 | 120, 53, 108 |
| 1 October | 3 | 72–181 |
| 2 October | 3 | 134–151 |
| 3 October | 3 | 72–112 |
| 4 October | 3 | 5–19 |
| 5 October | 2 | 16–17 |
| 6 October | 3 | 112–136 |

This does not support a simple assertion that 30 September was healthy or
every October leg stripped 72–181 rows. Details JSON caps some symbol lists;
the available October arrays name at least 216 distinct symbols and list
RAY.US in eight events. Those are lower bounds, not the handoff's complete
337-symbol/23-of-32 population. The current available Global_Markets grid
contains only one `identity_quarantined:v6.24.0` row, while the final
6 October firewall event reported 129 outgoing strips. Final row population
cannot reconstruct a pre-firewall batch or the effects of last-good recovery.

The code explains the quarantine predicate, not the upstream cause:
`_row_identity_firewall` strips a testable positive-price/positive-EPS/positive-P/E row
whose Price/EPS-implied P/E exceeds its relative tolerance, except the
recognized minor-unit ratio band. It also merges fabricated-placeholder
strips into the same outgoing count. Guard/KLG code runs before publication.
This export contains no `/health` observations or recovery events and no
pre-firewall row payloads. A backend/provider correlation or narrower
flag-gated corrective predicate cannot be justified from these inputs.
Keep the identity guard armed and never derive a retirement list from its
quarantined shells. To resolve the cause, obtain the same-run raw batch
payloads, backend release/provider-health receipts and post-KLG/pre-firewall
rows, binding each by run ID and quote/acquisition timestamps.

The three provider-routing examples also require date-specific interpretation:

| Instrument | Available 6 October row | Current repository contract |
| --- | --- | --- |
| `DX=F` | blank price, EODHD, `fetch_failed:HTTP 422` | `core/provider_capabilities.py` sends Yahoo-native futures to the configured Yahoo path only; an omitted Yahoo provider is not silently enabled. |
| `XAUUSD=X` | blank price, EODHD, quote identity fields missing | Yahoo's configurable skip-list explicitly documents earlier junk XAUUSD prices; supported routing is not proof that a genuine quote currently exists. |
| `SPLG.US` | blank price/name, EODHD, `quote_current_price_missing` | Ordinary US-symbol provider order is preserved. Fresh single-symbol supplier identity/availability evidence is required before proposing a replacement symbol. |

All three carry `fund_cache:skip_page`. That receipt only skips the engine's
fundamentals fallback for the configured page; it neither skips the quote
request nor certifies quote availability. The current capability-routing
regressions pass offline. The older DX=F row does not prove the current
Yahoo-native futures contract is deployed or failing today. No speculative
provider alias, replacement ticker or live request was introduced.

WP-10: this export includes 232 log rows mentioning EODHD-QUOTA, of which
201 contain structured normal quota samples; all structured plan limits are
400,000. October samples report zero new HTTP 402 rows. The maximum observed
counter by day ranges from 91,674 to 118,631 for 1–6 October. These are
observed lower bounds from a shared account counter, not complete daily
totals or page-specific billed costs. Quota samples are not per-endpoint
telemetry: overlapping backend use, cache hits, retries and fundamentals
request-unit weighting prevent attribution of the handoff's Mutual_Funds
versus Global_Markets costs.

The code provides a concrete possible amplifier: EODHD's primary enriched
provider fetches quote plus optional fundamentals/history, and fundamentals
attempts debit ten request units; engine `fund_cache:skip_page` applies later
to the fallback seam and does not suppress the provider's initial fetch.
Whether this explains Mutual_Funds' higher measured cost depends on the
actual route entry point, active provider flags, cache-hit/miss telemetry and
retry counts. Capture those with the same run ID before changing a provider
policy; do not equate an account-counter delta with calls per row.

The final available Global_Markets `_Status` row reports SUCCESS with 6,609
rows and 95.8% fresh coverage, not the handoff's 96.7%. Recovery is decided
from source artifacts and exact batch/freshness evidence rather than this
status text. `audit_sync_outcome.py` can reject a short budget, an unrecovered
batch, unknown exact coverage or a failing PAGE-VERDICT even when a rounded
coverage value exceeds 95%. `run_inline_page_recovery.py` then requires both
runner exit zero and an accepted artifact audit. This workbook includes
neither the rejected run's sync_execution logs/PAGE-VERDICTs nor its
sync-recovery-plan/inline-recovery-summary files. Those artifacts are the
minimum evidence needed to identify an unnecessary replay or its failure;
no replay defect is established here. Existing planner/inline-recovery
regressions are included in the targeted verification.

Dispatcher arming, retirement content, Mutual_Funds page scope, confidence
attractor treatment, EODHD daily target and runs per day remain operator or
owner decisions. The new default-off workflow mappings make the existing
controls reachable; they do not authorize arming or establish current live
provider/workbook health.
