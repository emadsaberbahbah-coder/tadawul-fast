# Decision integrity update — 24 October 2026

Baseline: `70a88fa1a5dffda60925614c44293d3ecda08bfd` (main and the observed
production health response before this revision). Branch:
`revision/2026-10-24-decision-integrity`.

## Changes and acceptance conditions

| Component | Defect and correction | Acceptance |
| --- | --- | --- |
| Validator 1.3.2 | Default 1,500-row scope missed larger page tails. Read up to 20,000 with the existing over-read sentinel; fail incomplete or logical-only source coverage. | Actual 6,611-row tail checked; exact-bound complete page passes; overflow remains UNKNOWN/FAIL. |
| Workbook audit 1.0.4 | Count==1500 heuristic missed other caps and rejected a complete 1,500-row page. Reconcile actual census against one passing scope receipt. | Missing/duplicate receipts and arbitrary partial scopes fail. |
| Export audit 1.2.1 | v2 badge could confuse technical feed readiness with executable tickets. Verify board badge, output and count coherence. Reconcile the separate engine/PF reserve bases. | Held/zero board cannot claim EXECUTABLE; valid different budgets pass display arithmetic only. |
| Engine 5.157.3 | Configured sukuk inherited a generic Saudi equity industry. Use the existing fixed-income policy for the fallback and final repair. | Specific provider industry and financial values preserved; generic label fixed; idempotent classification. |
| Tracker 6.48.0 | Ambiguous panel metrics and repeated matured keys. Use unique valid matured outcomes consistently in summaries, intervals, plots and advisory calibration. | Explicit directional/price labels and denominators, exclusions visible, immutable historical rows retained. |
| Target creation / S1 | Legacy target-unit inference could certify a self-consistent synthetic price tuple. Default prospective canonical targets on; enforce canonical forecast provenance for criterion 4. | Proven canonical positive path passes unchanged sample/band; insufficient, duplicate, ambiguous or unproven evidence stays pending/quarantined. |
| Calibration report 1.0.1 | Long-price-positive outcomes were labelled as generic wins. Clarify price-positive returns and their difference from direction-aware call correctness. | Report wording identifies the metric and exclusions. |

No ranking, risk limit, S1 sample threshold, broker order, historical signal
target or account balance is rewritten by this patch. Strict provenance can
reduce the eligible calibration sample; that is an intended correction.

## Same-snapshot outcome comparison

These are read-only measurements of the same uploaded 24 October snapshot,
not proof of future returns or a current live release.

| Measure | Baseline | Candidate |
| --- | ---: | ---: |
| Matured records used | 11,800 | 11,773 unique valid |
| Duplicate matured records excluded | 0 | 27 |
| Directional wins / losses | 1,849 / 1,527 | 1,849 / 1,527 |
| Directional correctness | 54.76896% | 54.76896% |
| Price-positive rate after deduplication | Not separately labelled | 46.25692% |
| Mean long-price ROI | 0.0239967% | 0.02001795% |
| S1 criterion 4 with enforcement | PASS, n=6,521 inferred | PENDING, n=0 proven |

The earlier 54.8% and sign-only rate are different metrics, not contradictory
measurements. A SELL followed by a falling price may be directionally correct
while its long-price ROI is negative. HOLD and breakeven exclusions are shown.
Overlapping horizons remain correlated and these are not net trade P&L.

## Validation evidence

Baseline-negative regressions reproduce missing behavior on `70a88fa`:
source-scope battery 7 failed / 2 passed; performance integrity 10 failed /
1 passed; v2 display audit 36 failed / 1 passed; two actual sukuk fallback
paths fail their new assertion. Existing fixtures are preserved unless their
old expectation explicitly depended on the replaced behavior; positive
canonical and negative provenance cases are both retained.

Independent raw-diff review identified and resolved a CI dependency placement
issue and a false PASS for conflicting v2 badge/output/count states. No
outstanding finding remained in the changed code. Full-suite and exact-head
CI results will be recorded with the final commit/PR evidence.

## Live presentation repair and remaining prerequisites

The existing native workbook received five narrow connector updates, verified
by exact-cell readback: Top10 v2 K1 derives `BOARD: <output>` from A3; A2 shows
the observed executable count; Portfolio v2 I5 names the engine budget basis;
A52 identifies its PF cash-floor basis; the latest conflicting cash-note cell
received an audit note while its historical value/formula remained unchanged.
No account amount, receipt, quote, S1 result or funding input was changed.
Google's native browser render required sign-in; layout verification used
the returned cell formatting (existing wrap, font and alignment retained).

These v2 display cells are generated. A later bound-source rebuild may
overwrite this presentation repair. Persistent v2 source/configuration and
chart fixes require the complete authentic bound Apps Script file inventory,
restorable backup, selective owning-file change and unchanged-file readback.
The partial five-file repository mirror does not contain that implementation
or the cash writer. Do not replace the bound project with this mirror.

Current settled cash, account scope, reservations, FX and timestamps are
still required for financial reconciliation. A read-only connector returned
empty positions/orders but supplied no current cash or account evidence;
it cannot refresh or certify the financial snapshot.

The Render connector has no selected workspace. Its only listed workspace
is `My Workspace`; deployment/configuration operations through that connector
require the owner to identify the workspace. No secret environment values
are copied into this evidence. `DP2_ENGINE_ENV` remains unverified/unapplied
until its actual bound owner and effective non-secret configuration are read.

Rollback is a reviewed revert of this revision, preserving later unrelated
work. Prospective canonical target creation additionally accepts the existing
repository value `TRACK_CANONICAL_TARGETS=0`; do not restore a favorable legacy
calibration PASS as evidence. Recommendations remain withheld until current
source, account, stability and S1 acceptance criteria are independently met.
