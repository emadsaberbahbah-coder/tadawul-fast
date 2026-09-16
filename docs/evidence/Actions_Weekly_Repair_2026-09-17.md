# Weekly Actions repair — 17 September 2026

## Scope and evidence window
Owner-authorized review of `emadsaberbahbah-coder/tadawul-fast`.
Week begins Sunday 13 September 2026 00:00 Asia/Riyadh (12 September 21:00 UTC).
The initial inventory observed 535 runs and 37 pre-existing failures across six workflow families.
Repair-branch development runs are separate and must not be counted as pre-existing failures.
Inventory: Actions Health Review run **35154662488**.
Base production commit: `95a8ea4e7a803de9b7387e4035680c8cdca69d3a`.
Repair PR: **#515**.

## Failure triage
| Workflow | Original failed runs | Representative run | Diagnosis / disposition |
|---|---:|---:|---|
| Decision Surface Freshness | 15 | 35153768961 | Display-format timestamp loss repaired at read boundary. Real source row-count and mixed-snapshot findings retained. |
| Full Refresh Coverage | 15 | 35153769065 | Audit test contradicted the shipped Riyadh contract; fixed with equivalence/precision/age tests. Real missing names and row-count deficits retained. |
| Provider Target Coverage Alert | 4 | 35091988613 | Cached reference contained `pages: {}`. Failed scheduled audit could write that invalid empty state. Equal healthy observations did not renew verification time. Both code defects repaired. Explicit healthy initialization still required. |
| Tadawul Dashboard Advanced Sync | 1 | 34721785739 | Global refresh exhausted its recovery and an unresolved BNY.US identity stopped the run. Later production runs recovered. Identity quarantine not bypassed. |
| Sync Outcome Audit | 1 | 34727251647 | Correctly reported the upstream failed sync. Not an independent defect. |
| Investment Policy CI | 1 | 34748624107 | GitHub reported failure to acquire a runner after five attempts; no test steps started. Later policy runs passed. |

All current workflow definitions were inspected with the repository safety audit. At the reviewed state it reported zero errors and 28 maintenance warnings; these warnings are not proof that otherwise successful workflows failed. Manual-only workflows with no runs were not dispatched merely to inflate coverage. In particular, no unverified production recovery, grid resize, or workbook mutation was performed.

## Implemented repairs
1. `scripts/workflow_audit_support.py`: read underlying Sheets values with `UNFORMATTED_VALUE` and `SERIAL_NUMBER`; retain the existing retry wrapper. Do not change the normal production reader. Integer serial dates remain date-only and cannot pass an intraday precision test.
2. `scripts/audit_full_refresh_coverage.py`: use that read-only audit resolver, also inherited by the other audit modules.
3. `scripts/audit_provider_target_coverage.py` v1.2.0: CONTROL/FAIL observations cannot create an empty reference; stale references remain available for diagnosis; distinguish empty, stale, future and policy-mismatched state; record healthy equal-observation verification time separately from reference generation time. No automatic downward baseline ratchet.
4. `provider_target_coverage.yml`: cache only eligible written references, include run attempt in the cache key, and test relevant changes without production credentials.
5. Coverage/freshness workflow path filters include their shared audit dependency and regression tests.
6. `tests/test_full_refresh_coverage.py`: correct the obsolete UTC assertion and explicitly test timezone equivalence, date-only precision, serial fractional days, and half-hour freshness.
7. Permanent read-only weekly inventory and audit regression workflow. Main-only post-merge verification preserves actual failed outcomes and freezes provider baseline writes. Temporary patch applicator/source-export steps removed.

## Validation before merge
The exact patch was applied against pinned original blob SHAs in GitHub run **35155854385**:
- Provider core self-test: **16/16 passed**.
- Decision freshness core self-test: **6/6 passed**.
- Combined unit tests: **26/26 passed** (8 full-refresh, 4 decision, 14 new lifecycle/reader tests).
- Repository workflow safety audit: **0 errors**.

That preparation run failed only when its restricted GITHUB_TOKEN tried to publish workflow files. The tested commit was subsequently published through the existing authorized GitHub connector; runner permissions were not elevated. Final PR checks, not this preparation run's overall status, are the merge criterion. Final CI and post-merge live results belong in the PR discussion.

## Remaining operational acceptance criteria
These are not waived by successful unit tests:
- Market_Leaders observed **255** against approved audit minimum **1,025**; Mutual_Funds **2,474** against **4,496**. Restore the intended source universe or supply an independently approved universe contract before changing floors. Do not use observed counts as their own acceptance threshold.
- Global_Markets name coverage **98.17%** vs **99%**; My_Portfolio **85.71%** vs **100%** in representative run 35153769065. Resolve missing instrument identity/name data upstream; do not substitute ticker strings merely to satisfy a name test.
- Top-10 was earlier than the completed Global_Markets source and asserted a full universe despite incomplete source counts. The production decision cockpits are owned by Apps Script, not written by the GitHub sync. A same-epoch, post-source-refresh read-back is needed.
- Provider-target reference has no accepted page entries. Initialize explicitly only after a verified healthy observation; no silent scheduled bootstrap or invented historical baseline is included.

No recommendations, allocation rules, stop levels, production trades, Render deployments, or business-sheet values were changed by this repair.

## Rollback
Revert the squash merge of PR #515. Retain original failure artifacts; do not rewrite failed history as successful. Old-run re-runs use the original event SHA, so acceptance must be demonstrated on the repaired commit.
