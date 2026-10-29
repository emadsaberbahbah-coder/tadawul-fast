# Commit Sheet - release_decision_feed v1.3.0: a cockpit caught mid-render settles instead of failing - 2026-10-29

**Scope:** readiness priority 1 (owner brief, 28 Oct): the publication cycle must not lose a verified release to a transient state. Gate `TFB_RELEASE_RENDER_SETTLE` (repository variable), **default off = v1.2.0 byte-identical apart from `script_version`** (R11 proves it against the untouched 729a316e module). No acceptance rule, threshold, gate, write grammar or exit code changes; the settle only re-runs the SAME strict audits after a bounded wait. (1.2.1 is claimed by open PR #831; workflow 1.1.1 likewise.)

**Files:**
| File | Version | Change |
|---|---|---|
| `scripts/release_decision_feed.py` | 1.2.0 -> 1.3.0 | step 6b: `settle_candidates()` (PURE), at most 2 x 120 s settles, each followed by a fresh in-flight probe and the same strict audits; one `fin.run` call site kept (`strict_audits`, case_n22) |
| `.github/workflows/release_decision_feed.yml` | 1.1.0 -> 1.2.0 | release STEP env maps `TFB_RELEASE_RENDER_SETTLE` (default `'0'`); job env untouched (final-job parity) |
| `tests/test_release_render_settle.py` | NEW | 12 cases (R1-R12), script and pytest |
| `tests/test_release_auto_source.py` | a7 | exact `1.2.0` pins -> a version floor and a k==k manifest pin |
| `.github/workflows/ci.yml` | 1.1.55 -> 1.1.58 | the new harness in the blocking lean lane (1.1.56 / 1.1.57 claimed by #875 / #876) |
| `scripts/verify_deployment.py` | pin | `release_decision_feed 1.3.0` |

## S1 - Evidence
- Both cockpits write `Last run <now> | status: running...` into B2 as the first write of every render: `16_Decision_Top10.gs` `statusCell.setValue(dt10StatusLine_('running\u2026', ''))`, `10_My_Portfolio.gs` `statusCell.setValue(pdStatusLine_('running\u2026', ''))`. The full line with the receipt returns only when the render ends.
- The strict decision audit reading B2 mid-render reports `T10_STATUS_NOT_OK` ("Embedded status is RUNNING, not OK."), `T10_SOURCE_CONSUMPTION_UNPROVEN`, four `T10_POOL_COUNT_MISSING` and `T10_POOL_RECONCILIATION_UNPROVEN`.
- Armed, a release run that re-verifies a LIVE release of the same run then writes `NOT_ACTIONABLE(final_publication) | run=N | ... | decision_failed | release=<this run>`, and `classify_prior` refuses every later release of N (`release_window_consumed`) until the next sync. Reproduced by the independent review of #876 on this file's own fixtures, and pinned as R1 here.
- Renders happen right when re-verifications happen: the decision chain re-renders Top_10 after every promotion (v1.0.1), the 4-hourly wrapper and the 08:07 trigger render Top_10, and the IBKR capture watcher renders Portfolio_Decision (08:40 and, from 29 Oct, 17:40 captures). Every release slot after a promotion re-verifies the live release.

## S2 - Change (gate on only; mode=release only)
- `settle_candidates(result)`: the cockpits whose strict finding is exactly `T10_STATUS_NOT_OK` / `PF_STATUS_NOT_OK` on its own surface with the message `Embedded status is RUNNING, not OK.`, and only when the coverage audit passed (`audit_exit_codes[0] == 0`). Any other state (ERROR, unknown, a stale board, a missing receipt with status OK, a failed coverage audit) settles nothing.
- When the strict audits fail and a candidate exists: wait 120 s; re-run the in-flight probe (busy or probe fault -> exit 1 `deferred`, zero writes; a live release stays live and the next slot re-verifies); re-run the SAME strict audits (fresh reads) and judge that result. At most 2 settles; a cockpit still mid-render after them is judged exactly as today (fail closed).
- `out["render_settle"]` (attempt, cockpits, wait, the first attempt's reasons) appears only when a settle happened; `out["reasons"]` then carries the final attempt's reasons. The printed line and the workflow summary keys are unchanged.
- Bounded: at most 4 extra minutes inside the 30-minute job.

## S3 - Tests
| Case | Proves |
|---|---|
| R1 | golden negative on the unchanged (gate-off) path: the live release re-verified mid-render is withdrawn with `release=777001` and its window is consumed |
| R2 | gate on, same race: one 120 s settle, re-audit passes, `already_released`, zero writes, the live value intact, 2 probes |
| R3 | Portfolio_Decision mid-render, and both cockpits mid-render |
| R4 | still mid-render after two settles: withdrawn exactly as today (fail closed) |
| R5 | a first release caught mid-render is released after the settle; gate off: a plain failure |
| R6 | 6 other failures (coverage failed, ERROR, `running_x`, missing receipt, stale board, no status): no settle, result identical with the gate on or off |
| R7 | a writer or a probe fault during the settle: `deferred`, zero writes, the live release kept |
| R8 | a strict-audit fault on the re-run: `fatal`, zero writes |
| R9 | `settle_candidates` exactness; the message and the running lines are read from the audit and the two `.gs` sources |
| R10 | gate vocabulary; constants 120 s x 2 |
| R11 | off-path identity: 1.3.0 with the gate off (and on, where nothing is mid-render) returns the same result and writes as the untouched 1.2.0 module (`git show 729a316e`), wall-clock stamps excluded; SKIP on a shallow checkout |
| R12 | the workflow maps the gate in the release STEP env only; job env and final-job env untouched; summary keys unchanged; time bound; ASCII |

Runs: `python tests/test_release_render_settle.py` -> `PASS 12/12`; neighbouring suites (release feed incl. case_n22 and case_n16, auto source, manifest pins, runner pin, repository workflow audit, CI verdict, final decision publication + workflow, decision-surface freshness, flag mappings, workflow-run window, activation workflows): **549 passed, 13 skipped**. `audit_repository_workflows.py` errors=0 warnings=0; `compileall` and ruff E9/F63/F7/F82 clean.

## S4 - Owner steps (after merge)
1. No Render deploy (GitHub Actions only), no Apps Script change.
2. Repository variable `TFB_RELEASE_RENDER_SETTLE` = `1` (one arming; it changes release behaviour only when a cockpit is caught mid-render).
3. Read-back: the next release run that meets a render shows `render_settle` in its artifact and ends `released` / `already_released` instead of `not_accepted`; `_Status` `TFB Decision Feed` keeps its EXECUTABLE value.

## S5 - Rollback
Unset `TFB_RELEASE_RENDER_SETTLE`, or revert the PR.

## Known limits / deliberate cuts
- A render longer than ~4 minutes still fails the attempt (fail closed); Top_10 renders take about 2 minutes.
- Only the cockpit's own RUNNING declaration settles. A half-written board that already restored its status line is judged as today.
- Open PR #831 edits the same file (steps 1/4/5, schedule-only deferral); the two changes touch different steps and rebase by merging the WHY blocks and taking the higher version.

## Correction record
| Defect | Root cause | Change | Tests | Deployed/installed version | Effective mode | Live acceptance | Remaining dependency |
|---|---|---|---|---|---|---|---|
| A live, verified release is withdrawn for good when any release run re-verifies it while a cockpit renders (`release_window_consumed`); a first release mid-render fails | The strict audit reads B2 while the cockpit's own first render write says `status: running...`; the release treats that transient state as a verdict | `release_decision_feed` 1.3.0: up to 2 x 120 s settles, each with a fresh probe and the same strict audits, only for the cockpit's own RUNNING finding with coverage passing | 12 cases (R1 golden negative, R11 identity vs 729a316e); neighbours 549 passed / 13 skipped | Merged: pending. GitHub Actions only | Off (`TFB_RELEASE_RENDER_SETTLE` unset) | Pending: a release run that meets a render settles and keeps / makes the release | Owner merge + set `TFB_RELEASE_RENDER_SETTLE=1` |

Not a licensed financial adviser; the owner decides and executes every trade. This change places no broker order and changes no executable count.
