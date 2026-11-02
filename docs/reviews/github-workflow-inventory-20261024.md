# GitHub workflow inventory — 24 October 2026

Baseline: `561db60c74c1d56d866d4b72756f8ef9a9df57d5` (merged #827).
Scope: all **38 workflow YAML files** and `.github/dependabot.yml`, including
parsed triggers, schedules, jobs, conditions, permissions, concurrency,
timeouts, action references and commands. Run evidence: **850/850** unique
`main`-branch run records from 17 October through **24 October 08:05:38 UTC
(11:05:38 Riyadh)**. All nine metadata pages were retrieved. This is not all
historical runs or a claim that every historical job log was inspected.

Run outcomes in that fixed metadata capture: **278 success, 80 failure,
491 skipped, one in progress**. Failure metadata identifies nine workflow
families. Root-cause log/artifact inspection is recorded by the owning repair
workstreams; this table does not manufacture causes from a red status.

## Interpretation that changes the repair priority

- Of **23 operational Daily Sync schedule/dispatch runs, 22 failed and one
  succeeded**. Its other 25 successes are push-only validation events. The
  green push after #827 does not prove a fresh acquisition-to-decision cycle.
- The three downstream audits contain **491 skipped runs** in total.
  Workflow-run events from CI/push executions are intentionally excluded by
  their operational event gates. These skips are not 491 failed updates.
- A successful audit report generator or an observe/off-mode job may report
  blocked recommendations, no eligible model, or no write. Such a green job
  establishes software execution only. Preserve semantic acceptance fields.
- No runs in this window is expected for manual-only recovery/canary tools;
  it is not proof that they are broken. No dormant workflow is activated by
  this revision.

Priority: P0 = data/decision publication; P1 = evidence, controls and data
quality; P2 = research, reporting and maintenance; P3 = optional diagnostics.
Latest run links below refer to the latest non-skipped event in the captured
window, except Daily Sync, where the actual scheduled acquisition is shown.
Counts are `success/failure/skipped/in-progress`; a dash means no captured run.

| File | Priority | Counts | Latest executed evidence | Disposition / next check |
| --- | --- | --- | --- | --- |
| `acceptance_check.yml` | P1 | 7/0/0/0 | [39950956890](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39950956890), success | Non-strict measured report by default; inspect criterion results, not job colour. |
| `actions_health_review.yml` | P1 | — | No run; manual only | Confirmed 1,000-result cap defect; repaired with bounded all-branch window splitting and incomplete receipts. |
| `backtest_harness.yml` | P2 | 1/0/0/0 | [39144347392](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39144347392), success | Research output; inspect eligible hypotheses/sample before using results. |
| `backtest_signals.yml` | P2 | 1/0/0/0 | [39934226088](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39934226088), success | Read-only evidence; keep cohort and overlapping-horizon limitations explicit. |
| `board_scorecard.yml` | P1 | 6/0/0/0 | [40047018760](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40047018760), success | Default observe; a scorecard is not an executable release. |
| `calendar_sync.yml` | P1 | 7/0/0/0 | [39936050950](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39936050950), success | Scheduled writer; preserve manual dry-run boundary. Job uses default platform timeout; add a measured bound in a later isolated revision. |
| `ci.yml` | P1 | 24/0/0/1 | [40076497441](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40076497441), running at capture | Required summary waits on compile/lean/contracts/engine; whole-suite/framework drift are separately advisory. |
| `core_repair_readback.yml` | P1 | — | Manual only | Needed for exact deployed SHA/feature readback; absence is unverified execution, not failure. |
| `daily_brief.yml` | P2 | 20/0/0/0 | [39863245276](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39863245276), success | Mail/preview distinction must remain; no sending enabled here. Default platform job timeout is a future boundedness item. |
| `daily_sync.yml` | P0 | 26/22/0/0 | [40038342135](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40038342135), scheduled failure | Highest priority: trace actual failed validation/publication steps; push-only CI success does not repair the source cycle. |
| `decision_surface_freshness.yml` | P0 | 2/22/163/0 | [40045012373](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40045012373), failure | Real source/decision freshness failures remain red; 163 non-operational triggers skipped intentionally. |
| `dependency_typing_review.yml` | P2 | 1/0/0/0 | [39281145470](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39281145470), success | Both jobs advisory; green workflow does not certify zero advisories/type errors. |
| `digest.yml` | P2 | 21/0/0/0 | [39998942098](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39998942098), success | Existing email workflow; no replay or recipient change. Inspect decision withholding inside report. |
| `full_refresh_coverage.yml` | P0 | 9/15/163/0 | [40045012297](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40045012297), success | Latest coverage is healthier than earlier 15 failures; keep source membership/price/name checks and history. |
| `grid_capacity_repair.yml` | P1 | — | Manual only | Potential workbook resize mutation; retain preview/commit contract and production writer lease. |
| `intraday_refresh.yml` | P1 | 10/0/0/0 | [39815715080](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39815715080), success | Witnessed partial quote refresh; not a full-universe refresh. Tight eight-minute bound retained. |
| `investment_policy_ci.yml` | P1 | — | No main run in capture | Path/PR-triggered protection; PR checks exist outside this main-branch capture. No false failure inferred. |
| `investment_policy_shadow.yml` | P1 | 8/0/0/0 | [40073434451](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40073434451), success | Shadow evidence; no production promotion armed. |
| `manual_refresh_priority.yml` | P1 | — | Contract tool; no run | Verify shared writer-priority semantics when that component changes; no reason to activate it routinely. |
| `name_label_renewal.yml` | P1 | 1/3/0/0 | [39931436275](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39931436275), failure | Owner workstream inspects verification errors. Never extend review expiry without re-verifying public evidence. |
| `news_archive.yml` | P1 | 6/5/0/0 | [40066451707](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40066451707), failure | News workstream owns acquisition/archive failure; retain evidence for append UNKNOWN and provider failures. |
| `page_refresh_recovery.yml` | P1 | — | Manual only | Recovery writes under shared production lease; preserve approved source cohort/quotas and avoid blind replay. |
| `pit_snapshot.yml` | P1 | 7/0/0/0 | [40037559653](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40037559653), success | Green capture is not assurance of slot-day uniqueness; related pending changes need their own acceptance. |
| `provider_target_coverage.yml` | P1 | 0/7/0/0 | [39948573365](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39948573365), failure | Seven repeated control failures; inspect genuine baseline/provenance/freshness before accepting any new baseline. |
| `release_decision_feed.yml` | P0 | 24/4/0/0 | [40064848594](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40064848594), success | Inspect accepted/released/reason/expiry. Success may be an intentional withheld attempt or sweep. |
| `repair_stores.yml` | P1 | — | Manual only | Repair store mutation needs actual compatible archive/schema; preserve backed-up migration and dry-run boundaries. |
| `repository_workflow_audit.yml` | P1 | 21/0/0/0 | [40076497456](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40076497456), success | Fixed two false ACTIVE warnings: variable-backed overrides are runtime-unverified notices. Static output does not certify runtime flags. |
| `runner_image_canary.yml` | P3 | — | Manual only | Keep production Ubuntu pin until canary evidence exists; no image/schedule changed. |
| `shadow_board.yml` | P1 | 14/0/0/0 | [40001976771](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40001976771), success | Shadow writer/evaluation; inspect record publication semantics independently. |
| `shadow_scorer.yml` | P1 | 7/0/0/0 | [40010289934](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40010289934), success | S1 acceptance remains separate. Missing workflow concurrency is a later isolation/risk review item, not an observed failed run. |
| `sync_outcome_audit.yml` | P0 | 22/0/165/0 | [40045012272](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40045012272), success | Audits uploaded source page verdicts; success alone does not equal final publication acceptance. 165 skips are event gating. |
| `tests.yml` | P1 | 3/0/0/0 | [39670190500](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39670190500), success | Foundation tests pass. Explicit least-privilege permissions, timeout and cancellation group are future isolated hardening items. |
| `thesis_exit_backtest.yml` | P2 | — | Manual only | Read-only optional backtest; no available result is not a failure. |
| `track_performance.yml` | P1 | 14/0/0/0 | [40012156458](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40012156458), success | Recording, auditing and calibration are separate; scheduled maturity audit remains opt-in. New #827 provenance controls await an actual scheduled readback. |
| `universe_maintenance.yml` | P1 | 7/0/0/0 | [40020863351](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40020863351), success | Off/observe/enforce plus overlap guard can yield intentional no work. Verify mode/report before claiming new symbols. |
| `weekly_brief.yml` | P2 | 1/0/0/0 | [39112873810](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39112873810), success | Existing mail/state behavior retained. Missing concurrency is a future duplicate-dispatch review item. |
| `workbook_backup.yml` | P1 | 7/1/0/0 | [40051111265](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40051111265), success | Earlier failure followed by successful runs; stored object/readability/restore and optional DQ outcomes remain distinct. |
| `workflow_repair_tests.yml` | P0 | 0/1/0/0 | [40076497421](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40076497421), failure | Regression tests pass but live controls fail after merge. Root workstream owns reports; never suppress genuine control failures. |

`.github/dependabot.yml` is the 39th reviewed configuration file, not an
executable workflow. Weekly GitHub Actions and monthly grouped pip updates
have bounded open-PR limits; dependency upgrades remain separately tested.
One dynamic Dependabot updater run [39181398539](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39181398539)
succeeded in the 850 records. It is not an additional repository YAML file.

## Implemented observability repairs

1. `audit_repository_workflows.py`: a nonempty YAML expression is not proof
   that `TFB_SYNC_FORCE_REFETCH_SYMBOLS` is active. A blank fallback or direct
   variable mapping now emits `RUNTIME_OVERRIDE_UNVERIFIED` NOTICE; literal
   overrides and nonempty source defaults remain warnings. Empty literals
   are clean. GitHub annotation syntax is `warning`/`notice`/`error`.
2. `actions_health_review.yml` and `github_workflow_run_window.js`: the
   default Sunday all-branch window actually contained **1,678** records at
   this cutoff, so the old single filtered search and explicit 1,000 guard
   could not complete. The new read-only collector recursively splits a
   fixed window, preserves all branches, verifies each leaf's pagination
   census and the complete child union against the original root count,
   deduplicates inclusive boundaries by run ID, and caps requests,
   unique runs, window length and split depth. Incomplete/failed/dense reads
   throw with an incomplete coverage receipt. No partial result is PASS.

GitHub documents the filtered 1,000-result search cap and 100-row page maximum
in its [workflow-runs REST reference](https://docs.github.com/en/rest/actions/workflow-runs#list-workflow-runs-for-a-repository).
The collector fixes metadata coverage, not expired historical logs. Original
run outcomes remain intact. A created-time cutoff does not freeze subsequent
status changes of runs that were still in flight.

Validation: scanner-related suites **91 passed, 10 subtests passed**; new
collector battery **16 Node cases passed** (multiple adverse subcases),
including a complete 1,678-row simulation, inclusive boundaries, unknown
transport/page/census, dense-second and every budget limit. No live all-branch
replay, schedule activation, email or provider/data mutation was performed.
The earlier scanner against baseline reports a false ACTIVE warning on the
actual blank-default mapping; the candidate reports runtime-unknown NOTICE.
The actual baseline health workflow retains its explicit >=1,000 throw;
the candidate's same-scope 1,678-row fixture completes without truncation.
The independent root/child mismatch probe (1,301 declared versus 1,300
observed) previously returned COMPLETE and now fails explicitly. Exact
fractional-boundary filtering occurs only after the full query census and
retains its original time scope.
