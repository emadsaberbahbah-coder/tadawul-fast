# GitHub Actions failed-run census — 24 October 2026

Repository: `emadsaberbahbah-coder/tadawul-fast`. Review baseline: `561db60`.
The fixed observation window is **17 October 00:00 UTC through 24 October
08:05:38 UTC** (24 October 11:05:38 Riyadh). The complete paginated inventory
contains 850 `main` run records. All 80 failed run records were inspected,
including the job list and decoded log of every one of their 89 failed jobs.
No access or pagination gaps remained. See the [sanitized per-run evidence](../evidence/github-run-census-20261024.json).

This is a bounded census of the latest attempt represented by each run record.
It does not claim inspection of every historical attempt or every branch.
Original failures are retained; successful reruns do not rewrite history.
Several independent audits observe the same source/consumer condition, so
failed-job totals are not independent defect counts.

## Priorities and observed causes

| Workflow | Failed runs | Observed cause | Priority / disposition |
| --- | ---: | --- | --- |
| `daily_sync.yml` | 22 | Every final-publication audit rejected the decision surface. Seven had coverage exit 0, eleven coverage exit 2, four coverage exit 1; decision audit exit 2 in all 22. Eight additional per-leg validator failures found a missing/stuck native Top10 grid after successful source writes. One core acquisition failed with backend 401 responses. | P0: synchronize native decision consumers with completed source publication and verify receipts. Preserve final veto. Check current auth/bulk-render repairs on genuine source executions. |
| `decision_surface_freshness.yml` | 22 | Pool reconciliation unproven in 22/22; source clocks ahead of Portfolio and Top10 in 21/22 each; stale Portfolio in 11; bad Top10 status in seven; missing/incorrect pool counts in some older runs. | P0: real consumer freshness/cohort issue, not YAML syntax. Keep rejection until native source/readback is proven. |
| `full_refresh_coverage.yml` | 15 | Eleven runs failed the unchanged 99% Name floor. Four had no audit FAIL but strict warnings for two, seven, eight or ten missing/invalid Global timestamps. | P1: repair current authoritative names/timestamps. Latest nine coverage audit runs passed; do not erase older findings or lower floors. |
| `provider_target_coverage.yml` | 7 | Every scheduled run has `CH_BASELINE_EMPTY` and missing accepted Market_Leaders/Global_Markets reference measurements. | P1: establish an explicit valid baseline after healthy measured acquisition. No automatic acceptance from an empty cache. |
| `news_archive.yml` | 5 | Selected feed acquisition completed and archive append was acknowledged in all five; adaptive discovery and identity coverage remained incomplete. One also reported incomplete source reading. | P1: fix/disclose discovery coverage while preserving the distinction from successful queried feeds. |
| `release_decision_feed.yml` | 4 | One superseded cohort, one still-running source/final audit, one genuine decision rejection, one Sheet-read timeout. No release/promotion in these runs. | P1: align timing; safely retry reads where appropriate. Preserve no-promotion semantics. |
| `name_label_renewal.yml` | 3 | Check-mode expiry alerts: first 122/122 bindings within 48h; later four/122 and four/124. No renewal was attempted by those check jobs. | P1: verify identity evidence and renew through reviewed publication. A valid warning is not a Python crash. |
| `workbook_backup.yml` | 1 | Version 1.5.0 resumable upload failed with an SSL EOF. | P2: later main has 1.5.1 resumable recovery and seven successful backups in the window. Verify stored objects; no new backup code defect established here. |
| `workflow_repair_tests.yml` | 1 | Offline regression passed; read-only post-merge live checks rejected full refresh, decision surfaces and provider baseline, each exit 2 with report present. | P0: actual live conditions remain unaccepted. Keep genuine failures visible; inspect retained artifacts. |

Freshness failure-code counts are **per run**, and can overlap:
`T10_POOL_RECONCILIATION_UNPROVEN` 22; `PF_OLDER_THAN_SOURCE` 21;
`T10_OLDER_THAN_SOURCE` 21; `PF_RUN_STALE` 11; `T10_STATUS_NOT_OK` seven;
`T10_POOL_APPROVED_COUNT` nine; `T10_POOL_COUNT_MISSING` three;
`FALSE_FULL_UNIVERSE_CLAIM` five.

## Do not count contract-only runs as acquisition success

The 48 `daily_sync.yml` records split into **25 successful push-triggered
contract-only runs**, **21 scheduled acquisition runs** (20 failures and one
success), and **two failed manual acquisition runs**. Therefore the latest
attempt result for actual acquisition/publication execution is **1/23**,
not the misleading 26/48 obtained by mixing contract checks with source work.
The successful actual run is [39871834683, attempt 2](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39871834683).
Its earlier attempt is not added to this latest-record denominator.

The most recent merge-triggered success, run 40076497411, checks contracts;
its source matrix jobs are skipped. Likewise the 163 skipped records in each
of the freshness and coverage workflow families are not failed acquisitions.

This census made no external write, acquisition replay, schedule/configuration
change, baseline acceptance, email or trade. Raw logs, credential-bearing
upload URLs, account figures and private source identifiers are excluded from
the committed evidence. Root-cause fixes and their subsequent acceptance are
separate revisions and must not be inferred from this classification.

## All-branch supplement

The expanded, complete created-at inventory covers **1,793 runs on all
branches**, using 20 paginated requests for the same fixed time window.
The later metadata snapshot contains:

| Scope | Runs | Success | Skipped | Cancelled | Failure |
| --- | ---: | ---: | ---: | ---: | ---: |
| main | 850 | 279 | 491 | 0 | 80 |
| other branches | 943 | 875 | 0 | 50 | 18 |
| total | 1,793 | 1,154 | 491 | 50 | 98 |

The earlier main-only read had 278 successes and one still-running CI; that
same run subsequently completed successfully. Its original frozen evidence
is preserved rather than silently rewritten. The created-at membership is
unchanged. Counts use the latest attempt represented by each record.

Every one of the 18 additional failed branch runs was reviewed, including
all **51 failed-job logs**, with zero access gaps. The combined failed-log
coverage is therefore **98 runs / 140 failed jobs**. The
[all-branch supplement](../evidence/all-branch-run-census-20261024.json)
retains their exact IDs, SHAs, failed steps, test identifiers and follow-up
head-check evidence without raw logs.

| Branch family | Failed runs | Finding and current disposition |
| --- | ---: | --- |
| `claude/new-session-twoifp` | 14 | Repeated stale version manifests and a backtest Run_Log warning-row assertion; six later runs also put a FastAPI-dependent Portfolio test into the lean lane without FastAPI. No matching PR was found by head branch. These are historical branch-only candidate failures; deployment/merge lineage is not inferred. The two branch-only test files are absent from main. |
| `fix/github-workflow-review-20261018` | 2 | Same old head `d019962` failed concurrency-lease and supersede-before-writer ordering assertions in push/PR CI. Later PR #795 head `34922d2` passes its observed CI verdict and whole-suite checks. It remains open/unmerged, and two current required status contexts were not present on that head; this is not merge clearance. |
| `claude/stoic-dirac-iggjv8` | 1 | Old head `f54e3c8` failed default-off research-diversify equality. Later open PR #801 head `28b3150` passes the required contexts and whole suite. Passing a later candidate does not establish live deployment. |
| `claude/p0-sync-truth-20261021` | 1 | Old head `b3aee34` passed 268 pytest cases then failed two standalone status-stamp warning expectations (S7.14/S7.15). PR #810 later merged head `50c1bf4`, whose required contexts and whole suite passed. |

The downstream CI-verdict failures repeat their failed prerequisite tests;
they are not extra independent defects. None of these 18 branch runs is a
production acquisition execution. No failed run was deleted or relabelled,
and the 50 cancelled branch runs are recorded separately from failures.

A detached, unchanged `561db60` worktree was used to recheck the shared
historical contracts: version manifests, publication lease, writer
supersede order and research default-off equivalence — **six passed**.
The unmerged branch-only backtest/Portfolio test files were not recreated
or claimed as passing. No source/workflow change or external mutation was
performed for this supplement.
