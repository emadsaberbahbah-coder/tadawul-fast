# Commit Sheet - await_cockpit_receipts v1.0.0 + release_after_sync.yml v1.0.0: release as soon as the cockpits consumed a completed sync - 2026-10-29

**Scope:** readiness priority 1 (owner brief, 28 Oct 14:47 Riyadh): "final decision after sources settle, bounded recovery tied to source progress". Timing only: the existing release (`release_decision_feed.yml` / `.py`) is byte-identical and stays the only code that can write EXECUTABLE. Gate `TFB_RELEASE_AFTER_SYNC` (repository variable), **default off = the new job is skipped**. No threshold, floor, audit, gate, sync leg, Apps Script file or Render setting is touched.

**Files:**
| File | Version | Change |
|---|---|---|
| `.github/workflows/release_after_sync.yml` | NEW 1.0.0 | `workflow_run: completed` of the Advanced Sync; job runs only for a `main` run whose event was `schedule`/`workflow_dispatch` and conclusion `success`/`failure`, with the gate armed; `permissions: contents read, actions write`; private JOB-level group `tfb-release-after-sync` |
| `scripts/await_cockpit_receipts.py` | NEW 1.0.0 | provenance (the release's own `check_provenance`), then a read-only bounded poll; one `workflow_dispatch` of the release on `main` when both receipts name the run and no writer or release run is active |
| `tests/test_await_cockpit_receipts.py` | NEW | 17 cases (W1-W17), script (`PASS k/k`) and pytest |
| `.github/workflows/ci.yml` | 1.1.55 -> 1.1.57 | the new harness in the blocking lean lane (1.1.56 is claimed by open PR #875) |
| `scripts/verify_deployment.py` | pin | `await_cockpit_receipts 1.0.0` |

## S1 - Evidence
- The release has no trigger tied to source progress: six fixed crons (`45 0,10,17,20`, `30 5`, `45 12` UTC) or a manual dispatch.
- 28 Oct: GitHub delivered this repository's crons 1-6 h late (sync slot 15:17 -> 21:22 Riyadh; release slots 15:45 -> 19:50, 20:45 -> 21:29 and 00:54).
- Sync `40744567865` (schedule) completed **19:31:05Z** (final job 19:29:27-19:30:58Z, conclusion `failure` = the designed pre-render veto). Its cohort passed every strict audit; it was released by a manual dispatch at **21:07:54Z** (`40769338335`, `released`, EXECUTABLE until 05:24 Riyadh). About 96 of its 480 minutes passed with the board showing FEED NOT ACTIONABLE for an accepted cohort.
- `26_Decision_Chain` (5-min ticks) renders Portfolio_Decision once the My_Portfolio publication is ready and Top_10 once `TFB Feed Global_Markets` and `TFB Decision Feed` both carry `run=N`; the strict audit then needs exactly one `source readiness VERIFIED (run N)` in cell B2 of each cockpit.

## S2 - Change
- **Trigger** (`release_after_sync.yml`): `workflow_run` of the Advanced Sync (the trigger names `daily_sync.yml`'s own `name:`; W14 pins it). Job condition (W16 evaluates all 1,080 combinations): `head_branch == 'main'` AND event `schedule`/`workflow_dispatch` (PR/push validation runs of the sync never qualify) AND conclusion `success`/`failure` AND `vars.TFB_RELEASE_AFTER_SYNC` in 1/true/yes/on. The concurrency group is JOB-level, so the skipped runs never enter it.
- **Wait** (`await_cockpit_receipts.py`), read-only:
  1. Provenance with `release_decision_feed.check_provenance` (completed production `daily_sync` on main, final job executed). Not releasable -> stop. Transport faults: 2 retries, the 3rd exits 3.
  2. Every 90 s, at most `TFB_RELEASE_AFTER_SYNC_WAIT_MIN` (default 45, refused above 55): ONE `batchGet` of `'_Status'!L1:M60`, `Portfolio_Decision!A1:B3`, `Top_10_Investments!A1:B3`. Parsing reuses the audit's `_consumed_source_run` / `parse_surface_status` on B2 and the release's `publication_slot` / `discover_source` / `_single_run` / `reads_executable` on the key block.
  3. **Stop, no dispatch** (feed evidence only): the Global_Markets feed or the decision feed naming another run (newer = superseded, older = this run's veto never landed); the feed missing or unreadable; the feed already EXECUTABLE for the run.
  4. **Ready** (both cockpits verified) -> GitHub probe, at most once per 270 s: the release's `github_inflight` (production writers) and every `release_decision_feed.yml` run not yet completed. Either busy -> keep waiting, never dispatch. Both clear -> `POST /repos/{repo}/actions/workflows/release_decision_feed.yml/dispatches` `{"ref":"main","inputs":{"source_run_id":"<N>","mode":"release"}}` (204).
  5. **Dispatch faults:** a 4xx fails at once. A transport fault or 5xx is never re-POSTed blindly (the POST is not idempotent): a later ready poll re-probes first, so a run the first POST did create is seen as active. A second failure exits 3.
  6. **Deadline**: `timeout`, no dispatch; the six slots still apply.
- **Why a dispatch, not an in-process release:** the dispatched run reads `TFB_DECISION_RELEASE` and the sticky `TFB_DECISION_RELEASE_KILL` when it starts (after the wait), runs in the release's own group `tfb-decision-release`, and keeps "this workflow is its only production entry". The wait holds no lock, so it never delays a kill, a sweep or a slot.
- **Why the release-run check:** a dispatch queued behind a running release can (a) cancel a PENDING kill (GitHub cancels the pending member of a group) and (b) re-verify a live release while the decision chain re-renders Top_10, whose B2 reads `status: running...` during the render; the strict re-verification then fails and the release script withdraws the live release for good (`release_window_consumed`). Reproduced on the release's own fixtures by the independent review.
- **Evidence** (`release-after-sync-<run>` artifact + step summary): outcome, reasons, polls, probes, waited_s, `first_seen_s` (pf, t10, ready) to calibrate the bound, dispatch attempts, provenance summary, last verdict (run tokens, declaration kind, parsed stamp/state, busy counts). Never status-line text (W11; the repository is public).

## S3 - Tests
| Case | Proves |
|---|---|
| W1 | gate off (unset, 0, off, observe, enabled ...): exit 0 with zero provenance, probe, read or dispatch calls |
| W2 | invalid source / this run / missing sheet / wait 0, 56, junk / poll 29, 601: exit 3, zero I/O |
| W3 | not_main, PR event, not completed, not daily_sync, final job missing: stop, no read; provenance faults bounded |
| W4 | ready on poll 1: one probe, one dispatch; the ranges and B2 are the strict audit's own (read from `run_live` / `audit_surfaces` source) |
| W5 | receipts landing over four polls (PF first, a WITHHELD Top_10 in between): GitHub untouched until the ready poll; first_seen pf 90 s, t10 270 s |
| W6 | 10 feed-evidence stops + a cohort that moves mid-wait: no probe, no dispatch |
| W7 | 8 receipt shapes the strict audit rejects: bounded timeout, no dispatch, one GitHub call (provenance); 31 polls at the defaults |
| W8 | read faults (one retried, three fail), three layout faults, probe faults (three fail on the 270 s cadence; one is retried), listing rules of `github_release_active` |
| W9 | 403/404/contract faults fail once; 502 re-POSTs only after a fresh probe (>= 270 s later); a 502 that did create the run is seen as active, never re-POSTed, and the stop is `already_executable`; two transport faults fail |
| W10 | the POST: URL, method, JSON body, headers, https only, token and repo required, 200/204 only |
| W11 | evidence and the printed line carry no status-line text, amounts, feed text or busy-run names |
| W12 | the waiter's readiness equals the REAL `audit_surfaces` consumption findings on 12 receipt shapes x 2 cockpits (a naive regex parser fails this case) |
| W13 | CLI: JSON written for gate-off, refusals and fatal; parse errors exit 3; env wait bound |
| W14 | workflow contract: trigger, permissions, JOB-level unique private group, single job, env defaults, inputs via env only, credentials/cleanup/install/setup steps equal to the release workflow's; the release workflow has no workflow_run trigger and does not mention the new script |
| W15 | the dispatch matches the release's `workflow_dispatch` inputs, its main-only check and its own source-ID regex (read from the step); manifest pin; script and test ASCII-only |
| W16 | the job condition over branch x event x conclusion x gate word (1,080 combinations) |
| W17 | a busy writer or an active release run never ends the attempt and never gets a dispatch queued behind it; probes back off to 270 s; a 45-min busy window costs 11 probes (77 GitHub calls), then `ready_but_blocked` (a version without the release-run probe fails this case) |

Runs: `python tests/test_await_cockpit_receipts.py` -> `PASS 17/17`; pytest 17 passed (lean venv: numpy, pytest, PyYAML). Neighbouring suites (release feed + auto source, manifest pins, runner pin, repository workflow audit, CI verdict, flag mappings, workflow-run window, final-publication + activation workflows, tracker and news workflows, name identity, approved universe, decision-surface freshness, final decision publication): **664 passed, 13 skipped** (the skips are the existing env-gated golden-negative legs). `audit_repository_workflows.py`: errors=0 warnings=0. `compileall` clean; ruff E9/F63/F7/F82 clean.

Golden negative: not applicable (new path, no behaviour change on any existing surface). The off-path identity is structural: the release workflow and script are untouched and their own suites (`case_n16` pins the release workflow's triggers, group, job list and env) pass unchanged.

Independent review (a separate agent that had not seen the work): 5 findings, all addressed in this same unreleased v1.0.0 - (1) a dispatch queued behind an active release could withdraw a live release during a re-render -> release-run probe; (2) it could cancel a pending kill -> same probe; (3) workflow-level concurrency was entered by skipped PR/push runs -> JOB-level group; (4) a busy recovery / grid repair / health_only run ended the attempt for good -> busy now only blocks the dispatch; (5) 6 GitHub calls per poll -> probes only on ready polls, at most every 270 s. Test-adequacy notes (self-referential parser check, vacuous regex check) -> W12 runs the real audit, W15 reads the release's own regex.

## S4 - Owner steps (after merge)
1. GitHub -> Settings -> Secrets and variables -> Actions -> Variables -> New repository variable: `TFB_RELEASE_AFTER_SYNC` = `1`. Optional: `TFB_RELEASE_AFTER_SYNC_WAIT_MIN` (default 45, at most 55).
2. No Render deploy, no Apps Script change, no new secret.
3. Read-back after the next production sync completes:
   - Actions -> **Release After Sync (P0-1)**: summary `outcome dispatched`, `first_seen_s` for both cockpits.
   - Actions -> **Release Decision Feed (P0-1)**: a `workflow_dispatch` run by `github-actions[bot]` with `source_run_id` = that sync -> `released` (or `already_released`).
   - `_Status` `TFB Decision Feed` = `EXECUTABLE | run=<that sync> | ...`; Top_10 shows it after the chain's release re-render.
4. `TFB_SYNC_FW2_STALE_PE=observe` (#875) is log-only, so both variables can be set before the same sync without confounding either evidence run.

## S5 - Rollback
Unset `TFB_RELEASE_AFTER_SYNC` (the job is skipped), or revert the PR. A live release is retracted the usual way (`TFB_DECISION_RELEASE_KILL=1` + dispatch `mode=kill`); this path never retracts or promotes by itself.

## Known limits / deliberate cuts
- **Pre-existing, not fixed here (next dependency):** any release run that re-verifies a LIVE release while a cockpit renders (B2 `status: running...`) withdraws it for good (`release_window_consumed`). A late scheduled slot or the 4-hourly Top_10 wrapper can still meet that race after any promotion, including one this path started. The fix belongs in `release_decision_feed.py` (a transient render state during re-verification of a live release should defer, not downgrade) and is a separate, gated PR; open PR #831 also edits that file.
- Residual kill window: a kill dispatched in the seconds between the last probe and the POST can still be cancelled by the queued dispatch; the sticky `TFB_DECISION_RELEASE_KILL=1` (step 1 of the documented kill) makes that queued run take the kill branch, so the retraction is delayed by one run, not lost.
- Only Advanced Sync completions start an attempt. `page_refresh_recovery.yml` and `grid_capacity_repair.yml` are dispatch-only; while they run the attempt waits, after them the slots apply.
- A WITHHELD receipt is not an early stop (the chain re-renders when `_Status` changes), so such a cohort waits to the deadline.
- A sync finishing just before a Top_10 quiet window (16:55-17:25, 07:55-08:25 Riyadh) needs about 35-40 min; the 45-min default covers it, a shorter variable would not.
- The wait occupies one runner for up to ~50 min per sync (public repository: no minute cost).
- `workflow_run` fires only for workflow files on the default branch, so nothing runs before the merge; renaming the Advanced Sync would silently stop the trigger (W14 pins the name).

## Correction record
| Defect | Root cause | Change | Tests | Deployed/installed version | Effective mode | Live acceptance | Remaining dependency |
|---|---|---|---|---|---|---|---|
| An audit-passing, consumed cohort waits for the next late slot or an operator before EXECUTABLE (28 Oct: sync `40744567865` done 19:31Z, released by hand 21:07Z) | The release has no trigger tied to source progress: six fixed crons (1-6 h late on 28 Oct) or a manual dispatch | `release_after_sync.yml` + `await_cockpit_receipts.py` 1.0.0: on sync completion, read-only bounded wait for both receipts naming the run, no dispatch while a writer or release run is active, then one dispatch of the unchanged release | 17 cases (W1-W17, incl. zero-I/O gate-off, parity with the real audit, 1,080-case job-condition matrix, busy/back-off budget); independent review, 5 findings fixed; workflow audit 0/0 | Merged: pending. GitHub Actions only (no Render deploy) | Off (`TFB_RELEASE_AFTER_SYNC` unset: job skipped) | Pending: after arming, the next production sync dispatches its own release within ~5-20 min of completing; the release ends `released`/`already_released` | Owner merge + set `TFB_RELEASE_AFTER_SYNC=1`; one evidence sync. Next: release re-verification must defer during a cockpit render |

Not a licensed financial adviser; the owner decides and executes every trade. This change places no broker order and changes no executable count.
