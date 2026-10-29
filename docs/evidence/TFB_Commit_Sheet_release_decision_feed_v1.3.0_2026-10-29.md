# Commit Sheet - release_decision_feed v1.3.0: a cockpit caught mid-render settles instead of failing - 2026-10-29

**Scope:** readiness priority 1 (owner brief, 28 Oct): the publication cycle must not lose a verified release to a transient state. Gate `TFB_RELEASE_RENDER_SETTLE` (repository variable), **default off = v1.2.0 byte-identical apart from `script_version`** (R11 proves it against the untouched 729a316e module). No acceptance rule, threshold, gate, write grammar or exit code changes; the settle only re-runs the SAME strict audits after a bounded wait. (1.2.1 is claimed by open PR #831; workflow 1.1.1 likewise.)

**Files:**
| File | Version | Change |
|---|---|---|
| `scripts/release_decision_feed.py` | 1.2.0 -> 1.3.0 | step 6b: `settle_candidates()` (PURE), ONE 150 s settle followed by a fresh in-flight probe and the same strict audits, only when the render explains every finding; one `fin.run` call site kept (`strict_audits`, case_n22) |
| `.github/workflows/release_decision_feed.yml` | 1.1.0 -> 1.2.0 | release STEP env maps `TFB_RELEASE_RENDER_SETTLE` (default `'0'`); job env untouched (final-job parity); the summary prints a `render_settle` row only when a settle happened |
| `tests/test_release_render_settle.py` | NEW | 14 cases (R1-R14), script and pytest |
| `tests/test_release_auto_source.py` | a7 | exact `1.2.0` pins -> a version floor and a k==k manifest pin |
| `.github/workflows/ci.yml` | 1.1.55 -> 1.1.58 | the new harness in the blocking lean lane (1.1.56 / 1.1.57 claimed by #875 / #876) |
| `scripts/verify_deployment.py` | pin | `release_decision_feed 1.3.0` |

## S1 - Evidence
- Both cockpits write `Last run <now> | status: running...` into B2 as the first write of every render: `16_Decision_Top10.gs` `statusCell.setValue(dt10StatusLine_('running\u2026', ''))`, `10_My_Portfolio.gs` `statusCell.setValue(pdStatusLine_('running\u2026', ''))`. The full line with the receipt returns only when the render ends.
- The strict decision audit reading B2 mid-render reports `T10_STATUS_NOT_OK` ("Embedded status is RUNNING, not OK."), `T10_SOURCE_CONSUMPTION_UNPROVEN`, four `T10_POOL_COUNT_MISSING` and `T10_POOL_RECONCILIATION_UNPROVEN`.
- Armed, a release run that re-verifies a LIVE release of the same run then writes `NOT_ACTIONABLE(final_publication) | run=N | ... | decision_failed | release=<this run>`, and `classify_prior` refuses every later release of N (`release_window_consumed`) until the next sync. Reproduced by the independent review of #876 on this file's own fixtures, and pinned as R1 here.
- Renders happen right when re-verifications happen: the decision chain re-renders Top_10 after every promotion (v1.0.1), the 4-hourly wrapper and the 08:07 trigger render Top_10, and the IBKR capture watcher renders Portfolio_Decision (08:40 and, from 29 Oct, 17:40 captures). Every release slot after a promotion re-verifies the live release.

## S2 - Change (gate on only; mode=release only)
- `settle_candidates(result)`: the cockpits whose strict finding is exactly `T10_STATUS_NOT_OK` / `PF_STATUS_NOT_OK` on its own surface with the message `Embedded status is RUNNING, not OK.` - returned only when the coverage audit passed AND **every** decision finding is one the running line itself produces on a running cockpit (Top_10: `STATUS_NOT_OK`, `SOURCE_CONSUMPTION_UNPROVEN`, `POOL_COUNT_MISSING`, `POOL_RECONCILIATION_UNPROVEN`; Portfolio_Decision: `STATUS_NOT_OK`, `SOURCE_CONSUMPTION_UNPROVEN`; measured on the release fixtures). Any other finding - ERROR, unknown, a stale board, a missing receipt with status OK, a failed or stale Portfolio_Decision next to a running Top_10, a cohort, membership or structure finding, a failed coverage audit - settles nothing, so the attempt is judged exactly as 1.2.0.
- When the strict audits fail and the render explains every finding: wait 150 s (once); re-run the in-flight probe (busy or probe fault -> exit 1 `deferred`, zero writes; a live release that failed ONLY on the render stays live and the next slot re-verifies); re-run the SAME strict audits (fresh reads, the clock read again) and judge that result. A cockpit still mid-render is judged exactly as today (fail closed).
- `out["render_settle"]` (attempt, cockpits, wait, the first attempt's reasons) appears only when a settle happened; `out["reasons"]` then carries the final attempt's reasons. The printed line is unchanged; the workflow summary gains a `render_settle` row only when that key exists (R12 proves the summary output for a result without it equals the 1.1.0 step's).
- Bounded: one 150 s wait plus one strict audit inside the 30-minute job.

## S3 - Tests
| Case | Proves |
|---|---|
| R1 | golden negative on the unchanged (gate-off) path: the live release re-verified mid-render is withdrawn with `release=777001` and its window is consumed |
| R2 | gate on, same race: one 150 s settle, re-audit passes, `already_released`, zero writes, the live value intact, 2 probes |
| R3 | Portfolio_Decision mid-render, and both cockpits mid-render |
| R4 | still mid-render after the settle: withdrawn exactly as today (fail closed) |
| R5 | a first release caught mid-render is released after the settle; gate off: a plain failure |
| R6 | 9 other failures, 3 of them NEXT TO a running Top_10 (Portfolio_Decision failed / stale / without its receipt), each also with a writer queued after step 5: no settle, result and writes identical with the gate on or off (the review's kept-release scenario now withdraws exactly as 1.2.0) |
| R7 | a writer or a probe fault during the settle: `deferred`, zero writes, the live release kept |
| R8 | a strict-audit fault on the re-run: `fatal`, zero writes |
| R9 | `settle_candidates` exactness incl. 8 unexplained companions (stale, failed status, run stale, false full-universe claim, structure, a PF finding while PF is not running, a wrong surface, a non-dict); the message and the running lines are read from the audit and the two `.gs` sources |
| R10 | gate vocabulary; constants 150 s x 1 |
| R11 | off-path identity: 1.3.0 with the gate off (and on, where nothing is mid-render) returns the same result and writes as the untouched 1.2.0 module (`git show 729a316e`), wall-clock stamps excluded; SKIP on a shallow checkout |
| R12 | the workflow maps the gate in the release STEP env only; job env and final-job env untouched; summary output identical to 1.1.0 without a settle (executed); time bound; ASCII |
| R13 | the clock moves during the settle: the re-audit is judged at the new time (My_Portfolio crosses 8 h -> `PF_SOURCE_STALE`; first release: no write; live release: withdrawn as 1.2.0 would) |
| R14 | compare-and-set after a settle: a feed changed during the wait -> `refused`, `compare_and_set_failed`, zero writes |

Runs: `python tests/test_release_render_settle.py` -> `PASS 14/14`; neighbouring suites (release feed incl. case_n22 and case_n16, auto source, manifest pins, runner pin, repository workflow audit, CI verdict, final decision publication + workflow, decision-surface freshness, flag mappings, workflow-run window, activation workflows): **551 passed, 13 skipped**. `audit_repository_workflows.py` errors=0 warnings=0; `compileall` and ruff E9/F63/F7/F82 clean.

## S4 - Owner steps (after merge)
1. No Render deploy (GitHub Actions only), no Apps Script change.
2. Repository variable `TFB_RELEASE_RENDER_SETTLE` = `1` (one arming; it changes release behaviour only when a cockpit is caught mid-render).
3. Read-back: the next release run that meets a render shows `render_settle` in its artifact and ends `released` / `already_released` instead of `not_accepted`; `_Status` `TFB Decision Feed` keeps its EXECUTABLE value.

## S5 - Rollback
Unset `TFB_RELEASE_RENDER_SETTLE`, or revert the PR.

Independent review (a separate agent): 4 findings on the first cut, fixed in this same unreleased 1.3.0 - (1) the settle started on ANY running finding, so a live release that also carried a real failure was KEPT when a writer started or the probe failed during the wait (reproduced) -> every finding must now be explained by the render (R6); (2) each settle extends the time a newly set KILL is not seen by this run (TFB_DECISION_RELEASE and the KILL are read at run start) -> one settle instead of two; (3) a stuck `running` board costs every release run the wait plus one audit -> bounded and stated; (4) the summary never showed a settle -> a conditional row. Missing tests (mixed failures, a moving clock, compare-and-set after a settle) -> R6, R13, R14.

## Known limits / deliberate cuts
- A render with more than ~150 s still to run when the audit reads it fails the attempt (fail closed); Top_10 renders take about 2 minutes.
- Kill latency: a KILL variable set during the settle is not seen by this run (read at start); the documented kill also dispatches `mode=kill`, which runs right after this run. One settle bounds the extra exposure to about three minutes.
- A board left at `running` by an Apps Script timeout costs each release run 150 s plus one strict audit before it fails closed as today.
- Only the cockpit's own RUNNING declaration settles. A half-written board that already restored its status line is judged as today.
- Open PR #831 edits the same file (steps 1/4/5, schedule-only deferral); the two changes touch different steps and rebase by merging the WHY blocks and taking the higher version.

## Correction record
| Defect | Root cause | Change | Tests | Deployed/installed version | Effective mode | Live acceptance | Remaining dependency |
|---|---|---|---|---|---|---|---|
| A live, verified release is withdrawn for good when any release run re-verifies it while a cockpit renders (`release_window_consumed`); a first release mid-render fails | The strict audit reads B2 while the cockpit's own first render write says `status: running...`; the release treats that transient state as a verdict | `release_decision_feed` 1.3.0: one 150 s settle with a fresh probe and the same strict audits, only when the cockpit's own RUNNING line explains every finding and coverage passed | 14 cases (R1 golden negative, R11 identity vs 729a316e, R6 mixed failures, R13 moving clock, R14 compare-and-set); independent review, 4 findings fixed | Merged: pending. GitHub Actions only | Off (`TFB_RELEASE_RENDER_SETTLE` unset) | Pending: a release run that meets a render settles and keeps / makes the release | Owner merge + set `TFB_RELEASE_RENDER_SETTLE=1` |

Not a licensed financial adviser; the owner decides and executes every trade. This change places no broker order and changes no executable count.
