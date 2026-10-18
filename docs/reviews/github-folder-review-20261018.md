# Review of `.github` workflows and their execution paths

Review date: 18 October 2026. Repository: `emadsaberbahbah-coder/tadawul-fast`.

Review candidate and final CI receipts: [PR795](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/795).

Reviewed base: [`4c9e2cbf98a22ecb3decc6894398b3dc74157592`](https://github.com/emadsaberbahbah-coder/tadawul-fast/commit/4c9e2cbf98a22ecb3decc6894398b3dc74157592), including the source-read relay merged in PR793. The repair candidate preserves that change.

## Outcome and scope

The review covers **all 36 workflows and `.github/dependabot.yml`**, their shell entrypoints, directly invoked application scripts, and the checked-in web launcher. It also examines **1,025 distinct workflow runs**, including 800 main-branch runs reaching back through 9 October, and reads exact failed jobs and logs for the recent failure families.

The current main workflow definitions parse and their action versions are valid. The important defects are execution and evidence defects: ignored manual targets, misleading success after failed I/O, positional schema drift, missing backend authentication, and shell input interpolation. The candidate consolidates previously reviewed workflow repairs with newly reproduced fixes, adds blocking regression coverage, and keeps current data-quality thresholds and financial policy intact.

This is a code-review candidate. It does not constitute a live workbook repair or a production release. No live writer or repair workflow, provider refresh, email delivery, merge, or Render deployment was started by this review.

## Confirmed repairs in this candidate

| Priority | Defect and effect | Prepared correction |
|---|---|---|
| High | Daily Sync primary legs honored manual backend/workbook overrides, while inline recovery used repository defaults. Final auditing handled `null`/`undefined` differently from the writer. | Resolve the same inputs and defaults before recovery credentials or writes; resolve final-audit scope with the same sentinel rules. Reject malformed identifiers and environment-file injection. |
| High | Follow-up coverage/freshness workflows associated a manual source run with a scan of the default workbook. They also treated `health_only` and `single_key` as full refreshes. | Every primary leg records its resolved target before writing. Follow-ups require all three matching source receipts, bind repository/workflow/ref/SHA/run/attempt, and then audit that exact workbook. Non-full modes produce explicit skipped-mode evidence before credentials are configured. Failed artifact downloads cannot proceed to the resolver. |
| High | Historical recovery silently used current default targets and could replay pages already repaired inline. Its writer controls differed from the normal writer. | Require the original backend and workbook as explicit inputs; read primary and inline-recovery artifacts; retain the scheduled writer's existing sanity, quota, identity, currency, retirement and hold controls without changing their configured defaults. |
| High | Free-form manual inputs were interpolated into shell programs. Apostrophes in legitimate JSON could break a command; shell metacharacters could change execution. | Pass inputs through step environment variables and quoted arguments/arrays. Tests execute the real wrappers with inert argument recorders and verify child failures propagate. |
| High | Store repair treated worksheet lookup failures as absent tabs, continued a junk purge after a failed ledger read, counted refused deletions as completed, and always returned success. | Preserve partial reports, stop the affected purge on missing evidence, count completed writes, and return nonzero for setup, phase, safety-ceiling and verdict-persistence failures. The existing dry-run diagnostic `_Run_Log` append remains documented. |
| High | The weekly brief read zero-based index16 as `Gen2 Eligible` after the producer inserted `ROI Src` there. Valid rows could be blocked and stored state could contain provenance instead of eligibility. | Bind gate, snapshot and memo fields to the producer's named `OUT_HEADER` columns. Tests use real producer rows and an inserted upstream column. Scoring and eligibility policy are unchanged. |
| High | A digest could interpret a failed final-feed read as `UNKNOWN` and continue after a passing page manifest. Partial SMTP acceptance could also look successful. | Stop failed final-feed reads before picks, rendering or SMTP; require complete delivery acknowledgement. Successful legacy-empty reads and readable vetoes retain existing behavior. |
| High | Intraday requests omitted authentication even though the live backend's global authentication mode is enabled. Earlier error swallowing hid unsuccessful requests. | Reuse the existing backend-token contract through request headers and workflow environment mapping; preserve explicit failure receipts and the row-level quote/write guards. |
| High | Scorer, tracker and backtest publication acknowledgements could be ignored. Several successful jobs therefore did not prove that their outputs persisted. | Propagate failed required writes and log receipts. Retain partial progress and existing research/observe-mode policy. |
| High | Archive, PIT, holdings and daily-brief readers could turn failed schema/receipt reads into usable empty state. Invalid backup responses could enter storage and retention. | Distinguish failed reads from valid empty data; verify required schema/header publication; validate XLSX contents before storage/retention; retain distinct backup dates correctly. |
| Medium | Outcome/recovery evidence could reuse older success after a later attempted recovery lacked a terminal result. | Require complete latest-attempt evidence and retain bounded acquisition diagnostics. Missing execution proof remains a failure. |
| Medium | Backtest history requests could collapse to an unintended provider default; signal backtests omitted the producer's canonical `Risk Bucket` field; London calendar request symbols used the wrong provider suffix. | Preserve requested history depth, accept the canonical risk column with legacy compatibility, and normalize London provider requests. |
| Medium | A ref-qualified production concurrency group allowed a branch recovery/grid run to overlap the main schedule on the same workbook. | Share one non-cancelling group across refs for Daily Sync, historical Page Refresh Recovery and Grid Capacity Repair. CI retains its own cancellable groups. This is not a global workbook transaction. |
| Medium | The launcher accepted invalid port/cap inputs, omitted backlog, and raised file limits in a subshell. The dashboard build ignored dependency-check failures and used a broken option-like grep pattern. | Use the reviewed executable launcher and checked-in build wrapper. Enforce dependency checks and bounded launch arguments; document paired build/start commands. Dashboard configuration is a separate pending action. |
| Medium | A full-suite gate-equivalence test compared calls against a moving wall clock, so unchanged quote evidence could differ by 0.1 minute and intermittently fail CI. | Give both compared calls one scoped clock; preserve complete payload equality, quote-age fields and financial assertions. |
| Low | The repository audit reported an optional variable mapping as proof of a live force-refetch override. | Warn about actual literal overrides; do not infer configured values from expression plumbing. |

## Complete folder inventory

“Reviewed” means the definition, referenced commands and applicable failure/guard semantics were inspected. It does not mean every manual option was executed against production.

| File in `.github` | Review disposition |
|---|---|
| `dependabot.yml` | Valid pip/Actions update configuration, schedules, groups and limits. Reviewed; no change required. |
| `workflows/acceptance_check.yml` | Fix manual argument boundary. Existing optional strict acceptance policy retained. |
| `workflows/actions_health_review.yml` | Reviewed inventory/API pagination and time-window behavior; bounded-result refusal is intentional. |
| `workflows/backtest_harness.yml` | Reviewed workbook-writing entrypoint; reuse registry acknowledgement/history-window fixes. |
| `workflows/backtest_signals.yml` | Fix manual argument boundary and canonical risk-column handling. |
| `workflows/board_scorecard.yml` | Reviewed selftest/live/observe routing. Observe success is not proof of published measurements. |
| `workflows/calendar_sync.yml` | Reviewed invocation; reuse London request-symbol repair. |
| `workflows/ci.yml` | Add checksum-verified Actionlint and required regression enrollment. Keep four blocking jobs and two advisory lanes. |
| `workflows/core_repair_readback.yml` | Reviewed exact deployed-SHA and bounded authenticated readback checks. Historical mismatch no longer describes current Render. |
| `workflows/daily_brief.yml` | Reviewed send/preview/PDF routing; reuse required receipt-read failure handling. |
| `workflows/daily_sync.yml` | Fix target consistency, record source target receipts, retain publication guards and recovery evidence, coordinate protected writers across refs. |
| `workflows/decision_surface_freshness.yml` | Bind follow-up audit to validated source target/mode. Current live freshness failures remain real data/evidence blockers. |
| `workflows/dependency_typing_review.yml` | Reviewed deliberately advisory dependency/type jobs. Success does not certify absence of dependency advisories. |
| `workflows/digest.yml` | Reviewed invocation; fix required final-feed reads and complete SMTP acknowledgement in the called script. |
| `workflows/full_refresh_coverage.yml` | Bind follow-up audit to validated source target/mode. Preserve coverage, names and acquisition floors. |
| `workflows/grid_capacity_repair.yml` | Fix input handling and cross-ref coordination; preserve backup/confirmation/apply gates. |
| `workflows/intraday_refresh.yml` | Fix backend authentication and manual-input boundary; preserve quote evidence and write rechecks. |
| `workflows/investment_policy_ci.yml` | Reviewed stdlib tests and synthetic smoke contract. Policy unchanged. |
| `workflows/investment_policy_shadow.yml` | Reviewed analysis plus always-running output-contract gate. A failed analysis is not automatically accepted. |
| `workflows/manual_refresh_priority.yml` | Reviewed Apps Script source, inert deployment contract and test entrypoints. |
| `workflows/news_archive.yml` | Reviewed CLI/universe/dry-run routing; reuse schema, idempotency-read and provider-result failure handling. |
| `workflows/page_refresh_recovery.yml` | Fix original-target requirement, source recovery evidence and writer-control parity. |
| `workflows/pit_snapshot.yml` | Reviewed preview/write routing; reuse verified archive-schema/header handling. |
| `workflows/provider_target_coverage.yml` | Reviewed baseline cache lifecycle and exit semantics. Empty baseline intentionally fails until explicitly initialized after a healthy sync. |
| `workflows/release_decision_feed.yml` | Reviewed credential scope, source provenance, compare-and-set and final release vetoes. No weakening. |
| `workflows/repair_stores.yml` | Fix called tool's failure semantics. Apply remains maintenance-sensitive because other writers use separate coordination. |
| `workflows/repository_workflow_audit.yml` | Reviewed unfiltered PR trigger, compiler, tests and scanner; correct false override warning in called script. |
| `workflows/runner_image_canary.yml` | Reviewed manual bounded canary, OS checks and representative tests. No runner migration performed. |
| `workflows/shadow_board.yml` | Reviewed control/entrypoint routing; reuse required holdings-read failure handling. |
| `workflows/shadow_scorer.yml` | Fix manual input boundary; reuse publication-failure propagation. |
| `workflows/sync_outcome_audit.yml` | Reviewed source/artifact handling; require complete latest recovery-attempt evidence. |
| `workflows/tests.yml` | Reviewed foundations targets and overlap with universal CI. No required tests removed. |
| `workflows/thesis_exit_backtest.yml` | Fix free-form score/day arguments. Existing advisory research behavior retained. |
| `workflows/track_performance.yml` | Reviewed record/manual-audit controls and activation receipts; reuse persistence/history repairs. |
| `workflows/weekly_brief.yml` | Fix manual input boundary and called script's producer-schema binding. |
| `workflows/workbook_backup.yml` | Reviewed export/restore/DQ modes; reuse validated backup bytes and distinct-date retention. |
| `workflows/workflow_repair_tests.yml` | Reviewed offline tests and main-only live guards; preserve nonzero results for actual data failures. |

## Run history: initial evidence and final production refresh

Current main CI [39115943771](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39115943771) passed all seven jobs. Its full suite reported **5,144 passed, 33 skipped, 274 subtests passed**. The current-main Daily Sync push run [39115943790](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39115943790) was CI-only: its live writers and publication jobs were skipped. It does not establish a successful live refresh.

The following table preserves the initial review evidence. The later **17:00 UTC production update** immediately after it supersedes the morning data percentages and blocker counts.

| Workflow/run | Observed result | Correct response |
|---|---|---|
| Daily Sync [39113252914](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39113252914) | Primary legs and inline recovery succeeded; final publication reported `[2,2]`, coverage/freshness blockers, blocker published and feed not promoted. Source SHA was `11ef134`, before PR793. | Preserve the veto and repair the live source/decision evidence. |
| Full coverage [39117737637](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39117737637) | Global_Markets accepted-name coverage **98.0784%** is below **99%**: 127 of 6,609 rows lack accepted names. Fresh acquisition **96.5502%** exceeds the 95% floor. | Obtain exact-code authoritative identity evidence and publish verified names; do not lower the name floor or invent replacements. |
| Decision freshness [39117737642](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39117737642) | Top10 predates all four source clocks; claimed fund membership and native reconciliation/exclusion receipts are incomplete. Seven findings. | Rebuild the native decision output after source completion, with matching source membership and required individual receipts. Re-stamping old output is insufficient. |
| Provider target coverage [39026626201](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39026626201) | Restored last-good cache has no accepted Market_Leaders/Global_Markets baseline. | After verified healthy sync, explicitly bootstrap the reviewed baseline, then verify subsequent normal cache reuse. |
| Historical outcome [38891292785](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38891292785) | Acquisition **94.2957%**, below 95%; no timeout or exhausted quota was established. | Investigate missing acquisition if repeated. The latest [39117737628](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39117737628) already passes. |
| Historical deployed readback [38655521187](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38655521187) | Healthy HTTP response at that time but deployed SHA differed from expected SHA. | Preserve the SHA check. Today's control-plane and runtime SHA match current main. |
| Historical Daily Brief [38167875419](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38167875419) | `NoneType.__format__` in BOOK rendering. | Already repaired on current main using null-safe formatting. Preserve the fix; do not replay the old revision. |
| Separate development-branch CI [39048479442](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39048479442) | Stale manifest pins, JSON-format assertion and a full-framework import in a lean test lane. | Repair that branch if retained. Its failures are not failures of current main and are not resolved by importing its unrelated changes. |

### Latest production update: 17:00 UTC

The scheduled source run [39142793753](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39142793753), started at 15:57 UTC, ran current main `4c9e2cb` and completed all three source legs and inline recovery successfully. Its final publisher returned audit codes **`[1,2]`**, with **only `decision_failed`** in the blocker list, `blocker_published: true` and `feed_promoted: false`. These results predate this candidate's publication and are not effects of the proposed repairs.

The latest [coverage audit](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39147518253/job/117734274828) reported **zero failures and one warning**. Global_Markets has **6,613 unique rows**, **6,584 fresh acquisitions (99.5615%)**, and **99.7429% accepted-name coverage**, above the 99% floor. The earlier accepted-name blocker has cleared. Its remaining warning is **10 blank, unparseable or date-only timestamps**. The standalone coverage workflow intentionally returns nonzero for warning exit 1; that red result does not mean the old name-floor failure persists.

The latest [decision audit](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39147518255/job/117734274609) reported **eight failures**: Portfolio_Decision is older than My_Portfolio; Top10 is older than each of the four source pages; it declares 2,469 funds against the approved 2,474; individual native producer receipts for cross-page exclusions are missing; and its full-universe claim remains unproven. The count difference alone does not establish five missing source rows: physical source membership and valid deduplication must be reconciled explicitly. Fresh native decision generation and corresponding receipts remain required.

The latest [provider coverage run](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39122743513/job/117661484074) still reports `CH_BASELINE_EMPTY` and missing accepted baselines for Market_Leaders and Global_Markets. The latest [Sync Outcome audit](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39147518250) passes. These later results establish no additional code defect: the remaining work concerns timestamp evidence, decision generation/reconciliation and explicit baseline initialization.

## Target-receipt behavior and compatibility

Each normal primary matrix leg records `sync_target.json` in its own existing source artifact. It contains non-secret target and source identity fields, not workbook rows or a success claim. URLs carrying user information, query strings or fragments are rejected before recording. The artifact does not contain credentials.

Automatic follow-up audits require the three distinct expected groups and identical backend/workbook/mode, as well as exact repository, workflow path, main branch, commit, run ID and run attempt. Missing, malformed, mixed-target, mixed-attempt or historical receipts fail closed. A failed download cannot be treated as verified evidence merely because files were partially extracted. Full-sync coverage/decision rules remain unchanged after target resolution.

`health_only` and `single_key` produce a specific skipped-mode artifact. A direct manual audit remains a deliberate scan of the current default workbook and makes no source-run identity claim. Old runs without these receipts can be checked using that standalone audit; they cannot acquire a fabricated source-bound receipt retroactively.

A partial GitHub rerun can leave older successful-leg artifacts beside a new attempt. That mixed attempt is intentionally rejected. Rerun all three primary legs together to obtain coherent receipts. GitHub's current artifact client supports same-name artifacts on reruns, and download-artifact v8 selects the newest artifact per name; no overwrite change is needed.

## Deployment review

The Render service `tadawul-fast-bridge` reported live deployment `dep-dba9qbs9v7es73fn5d90`, completed **2026-10-18 10:21:15 UTC**, on `4c9e2cbf98a22ecb3decc6894398b3dc74157592`. A fresh public `/health` readback reported that same SHA, `ready: true`, `engine_ready: true`, zero failed route mounts, no effective failed modules and no readiness reasons. `/readyz` returned HTTP200. Global authentication is enabled. These checks establish deployment alignment and reported readiness, not the accuracy of workbook rows.

A final control-plane read at **17:24:08 UTC** confirmed the same latest deployment and SHA. Direct HTTP probes at **17:24:37–38 UTC** returned HTTP200 from both `/health` and `/readyz`, with `ready`, `engine_ready` and `global_auth_enforcement` all true. No deployment changed during the review.

Automatic deployment is off. The live service still uses a dashboard-managed inline build and start command; there is no repository `render.yaml`. Consequently, neither this candidate nor a future merge automatically changes the reviewed dashboard build configuration.

After the code is accepted, use the paired settings documented in Procfile:

```text
Build: bash scripts/build_web.sh
Start: bash -lc 'set -euo pipefail; source .venv/bin/activate; exec ./scripts/start_web.sh'
```

If `TFB_BUILD_VENV` selects a different environment, activate that same environment in Start. The build script enforces dependency compatibility and compilation; the launcher keeps one foreground Gunicorn process, validates port and bounded options, passes backlog, and preserves the raised soft file limit through `exec`.

HTTP readiness configuration should be reviewed separately. The observed HTTP health-check path is empty, so Render uses its default TCP check. Merely setting `/readyz` does not make this application's response strict: its existing `TFB_READYZ_STRICT` and dependency requirement settings govern that behavior. No readiness or authentication policy was changed here. See the current primary [Render deployment](https://render.com/docs/deploys) and [health-check](https://render.com/docs/health-checks) references.

## Coordination and remaining operational work

The concurrency correction covers the three workflows already designed to coordinate long production writes: Daily Sync, historical Page Refresh Recovery, and Grid Capacity Repair. Their group now represents the shared workbook across branches.

Store repair, intraday updates, independent performance tracking and external Apps Script/backend writers are not all participants in that group. The existing Redis/control-sheet mechanisms do not prove exclusive workbook-wide ownership. Structural store repair therefore still requires a reviewed maintenance window with the overlapping writers coordinated and a backup available. Joining only one group would not close that risk. The release workflow's in-flight writer inventory also needs to be revisited with any later maintenance coordination change.

Current GitHub supports `concurrency.queue: max`, which permits more pending runs than the default single-pending behavior, but it cannot be paired with `cancel-in-progress: true`. Adopting it across high-frequency writers requires a scheduling/backlog design and separation from cancellable CI, rather than an untested global queue change. See [GitHub's concurrency reference](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).

Prioritize the remaining work in this order:

1. Accept the reviewed code candidate after its checks pass; preserve the source relay and existing financial gates.
2. Apply the paired Render build/start settings and deploy that exact accepted commit; verify runtime SHA, readiness and relevant authenticated workflow calls.
3. Resolve the ten timestamp warnings using actual source evidence and regenerate both portfolio and Top10 decision outputs against the completed source cohort, including native reconciliation receipts; rerun read-only coverage/freshness audits. Name coverage already passes the required floor in the final production snapshot; continue improving remaining identities without inventing replacements.
4. Initialize the provider baseline only after a healthy refresh and verify ordinary scheduled cache reuse.
5. Coordinate structural repair and high-frequency writers before any maintenance apply run; retain backup and readback evidence.
6. Handle the separately reviewed financial/portfolio changes in their own remaining PRs. Do not blindly merge overlapping repair stacks twice.

## Integration provenance

The candidate reuses workflow-reliability implementation/tests from [PR785](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/785), plus narrowly selected target, argument and startup/build fixes from [PR771](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/771). Selected base files were verified byte-for-byte before applying those patches. Existing repair review documents were not copied wholesale, and unrelated PR771 financial/acceptance changes were excluded.

New work adds the store failure repair, named weekly schema binding, digest final-feed guard, complete input-boundary coverage, source-target receipt/consumer binding, and intraday authentication. CI enrollment and deployment manifest versions are synchronized with the candidate. The required verdict still depends on compile, lean-unit, contract and heavy; whole-suite and framework-drift remain advisory under the repository's existing explicit policy.

This candidate overlaps PR785 by design. Merge a reviewed integration path once, then reconcile the remaining open PR stack against the resulting main. No existing PR was closed, merged, or force-updated during this work.

## Validation record

Static checks pass on the candidate: all 36 workflows through checksum-verified Actionlint 1.7.12, Python compilation, Ruff's syntax/undefined-name rule set, repository workflow audit (zero errors and zero warnings), and diff whitespace checks. A production-matching Python 3.11.17 environment installed the exact requirements and CI test-only dependencies successfully; its dependency check confirmed 145 compatible packages.

The final Python 3.11 targeted runs reported **148 passed and 90 subtests passed** across source-receipt, input-boundary, store-repair, weekly/digest, startup/build, recovery and manifest suites, plus **112 passed** across new intraday authentication/dispatch tests and the existing intraday contract suite. These are 260 distinct targeted tests; they are not the complete repository suite.

Behavioral tests cover actual shell wrappers with inert child processes; in-memory Sheets errors and partial writes; producer-generated weekly rows; SMTP acknowledgements and required final-feed reads; request headers; real receipt CLI and workflow binding; fake Gunicorn startup; and fake-pip build failure propagation. New regressions were also run against the original implementations where applicable, reproducing the claimed defects before the repairs.

The local broad suite encountered a runtime wakeup limitation independently reproduced using only stdlib asyncio and ThreadPoolExecutor, without pytest or repository imports. No production workaround, timeout change, assertion weakening or skip was introduced. Local split-suite counts are not represented as a complete full-suite pass. GitHub's actual Python3.11 workflow run is the decisive integration check; its exact receipt will accompany this review candidate.

The [first candidate GitHub run](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39149084606) completed the full suite without the local runtime limitation: 5,605 tests passed and two existing structural expectations failed. Those tests still required a ref-qualified production group and no local receipt step between credentials and the writer-start veto. Their reviewed updates now require one shared production group across all tested refs, preserve separate cancellable CI groups, and allow only the exact local receipt command/environment before the veto. Negative mutations reject missing, duplicated, conditional or altered receipts, ignored failures and extra intervening commands. No production gate or test selection was relaxed. The PR records the subsequent CI result for the final proposed revision.

The corrected structural revision passed the complete [PR suite](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39150212621) with **5,608 tests and 382 subtests passed, 33 skipped**. Its parallel [push suite](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39150208823) exposed a pre-existing time-dependent equivalence test: two calls crossed a quote-age rounding boundary, yielding `age_min` 3.1 versus 3.2 for otherwise identical outputs. The review therefore also repairs the comparison fixture with one controlled clock. Freshness and full-payload equality assertions remain present; production age calculations and financial policy are unchanged. The final revision's actual PR and push results are recorded together in PR795, including the advisory full-suite job's real outcome.
