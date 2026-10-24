# TFB Commit Sheet - release_decision_feed v1.2.1 [SCHEDULED SLOT DEFERRAL]

| Item | Value |
| --- | --- |
| Date | 2026-10-24 (daily improvement routine; the 2026-10-23 firing arrived a day late and is folded in) |
| Branch | `claude/daily-2026-10-24` |
| Base | main `83283c61` (#829); `scripts/release_decision_feed.py` is v1.2.0 there |
| Files | `scripts/release_decision_feed.py` 1.2.0 -> 1.2.1; `.github/workflows/release_decision_feed.yml` v1.1.1 (one step-env line); `scripts/verify_deployment.py` (manifest pin only); `tests/test_release_auto_source.py` (fake `execute` takes `num_retries`, version floor, A8 guard); `tests/test_release_inflight_defer.py` (new) |
| Gate | `TFB_RELEASE_INFLIGHT_DEFER`, armed by 1/true/yes/on (the module's `_ARMED`), read from repository variable `vars.TFB_RELEASE_INFLIGHT_DEFER` |
| Scope | Only a scheduled (`GITHUB_EVENT_NAME=schedule`) `--source-run-id auto` attempt in `mode=release` |
| Default | Off. Byte-identical to v1.2.0 on the result JSON (except `script_version`), the reads, the probes and the writes |

## Why

The v1.2.0 commit sheet's Known limits says "a slot that lands inside a sync defers and retries next slot". It does not.

In `run()`, two steps refuse with exit 3 before the in-flight probe at step 5b ever runs:

- the prior classification (step 4);
- the provenance check (step 5).

The one key-block read also has no transport retry.

The 10:xx UTC release slot fires about 4-5 h late (cron `30 5 * * *`). That lands inside the delayed morning daily_sync (cron `17 4`, which starts at about 08:56-09:28Z and runs 57-71 min). The workflow turns exits 1 and 2 green on a schedule, but not exit 3. So the collisions show up as red runs that look the same as the refusals that matter.

`docs/reviews/github-run-census-20261024.md` (#828) lists this workflow with: "P1: align timing; safely retry reads where appropriate. Preserve no-promotion semantics."

## Evidence (job logs and run timings, read 2026-10-24)

| Release run | When (UTC) | Exit / reasons | What was running |
| --- | --- | --- | --- |
| 40083081692 | 2026-10-24 ~09:43 | 3 refused `superseded_by_run` | daily_sync 40079965055, 08:56:07-09:53:30 |
| 39938800176 | 2026-10-23 10:23 | 3 refused `superseded_by_run` | daily_sync 39931603565, 09:24-10:35 |
| 39763157033 | 2026-10-22 10:27 | 3 refused `source_not_completed`, `final_job_not_executed` | its own source, daily_sync 39755653571, completed 10:28:54 |
| 39584712312 | 2026-10-21 10:28 | 3 layout_fault, `TimeoutError: The read operation timed out` on the bare key read | daily_sync 39576807720, 09:25-10:34 |

These are 4 of the 5 red release runs since 19 Oct. The fifth, 39245386428, is a dispatched genuine rejection, and it stays red under this change.

All four wrote nothing (`blocker_published=false`, `feed_promoted=false`). The next slot re-reads everything, so no recommendation or outcome number was wrong. The cost is signal: a daily false red on the armed promotion path.

## What changed

`scripts/release_decision_feed.py`:

| Site | Change |
| --- | --- |
| docstring | `VERSION 1.2.1` WHY block above the 1.2.0 block (evidence, per-site change, rollback); EXIT CODES gains a line for the new exit-1 case. Every prior block is verbatim |
| constants | `SCRIPT_VERSION = "1.2.1"`; `INFLIGHT_DEFER_ENV`, `KEY_READ_RETRIES = 2`, `_INFLIGHT_PROVENANCE` |
| `inflight_defer_enabled()` (new) | Reads the gate with `_ARMED`; default off |
| `_defer_for_run(out, probe, run_id)` (new) | Calls the unchanged `_probe_inflight`. Returns True only when the probe succeeds AND lists `run_id` as busy, and records `out["inflight_defer"] = {run, busy, probe_error}` |
| `run()` gate | `slot_defer` = gate armed AND `mode == "release"` AND source is `auto` AND `GITHUB_EVENT_NAME == "schedule"` |
| step 1, the key read | With `slot_defer`, `request.execute(num_retries=2)`; otherwise `request.execute()` exactly as before. The real googleapiclient `HttpRequest` retries socket timeouts, connection resets, 429 and 5xx (it is a read, so it is idempotent; no write is retried) |
| step 4 | A `superseded_by_run` refusal whose prior is EXACTLY the supersede grammar (`SUPERSEDE_VALUE_RE`) defers only when the superseding run (`by=`) is busy. Result: exit 1, outcome `deferred`, reasons `[superseded_by_run, production_run_in_flight]`, `deferred=True`, `in_flight` = the probe list |
| step 5 | A provenance refusal defers only when its reasons include `source_not_completed`, are a subset of `{source_not_completed, final_job_not_executed}`, and SOURCE is busy. Result: exit 1 with the provenance reasons plus `production_run_in_flight` |

These keep exit 3 with the gate armed:

- a probe error;
- a busy list that does not contain that run;
- a non-exact supersede value;
- any other refusal reason;
- `final_job_not_executed` alone (the source has completed);
- `not_main` or `event_not_production`;
- a dispatched run;
- an explicit numeric source.

`sweep` and `kill` are untouched, and their read gets no retry.

Other files:

- `.github/workflows/release_decision_feed.yml` v1.1.1: the "Release or retract the decision feed" STEP env gets `TFB_RELEASE_INFLIGHT_DEFER: ${{ vars.TFB_RELEASE_INFLIGHT_DEFER || '0' }}`. It goes in the step env, not the job env, because `case_n16` requires the job env to equal the final job's env plus the two release gates.
- `scripts/verify_deployment.py`: pin 1.2.0 -> 1.2.1. `SCRIPT_VERSION` is unchanged, and the flag is not registered in FLAGS, so the render env contract is unchanged.

## Measured results (offline, venv Python 3.11)

**Harness** `tests/test_release_inflight_defer.py`: PASS 8/8 as a script and 8 passed under pytest. With no git history (`GIT_DIR=/nonexistent`, as on CI's depth-1 checkout): 7 passed, 1 skipped. N2 skips, and I5 checks the delivered module against the recorded `OFF_GOLDEN` digests.

**Golden negative.** The base is `git show 83283c61:scripts/release_decision_feed.py`, run as the delivered module. Result: 6 failed, 1 passed (I5, the off identity), 1 skipped (N2).
- On I1 (superseded, superseding sync busy) and I2 (source still running) the base returns exit 3 refused.
- On I3 the base dies with exit 3 layout_fault after one HTTP call.

**Off identity (I5).** Covers 13 fixtures, the 10 non-armed spellings plus unset, and an armed gate on a dispatched attempt. Each is compared with the base on:
- the full normalised result JSON;
- the read call (`execute()` with no `num_retries`);
- the provenance calls, the probe call count (0 on refusals) and the writes.

All equal. The `OFF_GOLDEN` digests are proven against the base where it is present.

**Real client (I3).** A `googleapiclient` service built from the bundled `sheets.v4.json`, with a transport that raises `TimeoutError` once:
- armed: retried (2 HTTP calls), and the run continues to the v1.2.0 provenance step;
- off: fails exit 3 layout_fault after 1 call;
- three timeouts while armed: still fails closed.

**Mutations** (`-x` per mutation): 16 of 16 caught. The mutations were:
- always defer at step 4;
- no exact grammar check;
- defer on a probe error;
- a busy list that ignores the run id;
- step 5 without the `source_not_completed` requirement;
- step 5 without the subset check;
- step 5 deferring on any busy run;
- ignore the event;
- ignore auto;
- ignore the mode;
- retry when off;
- zero retries;
- a loose vocabulary;
- drop the original reason;
- no `deferred` flag;
- step 5 deferral removed.

**Repo checks:**
- `python -m compileall -q main.py core routes scripts tests`: clean.
- release, verifier-pin, env-contract, flag-registry and workflow-flag-mapping suites: 96 passed, 11 skipped.
- workflow audit tests: 226 passed.
- `scripts/audit_repository_workflows.py`: errors=0 warnings=0.
- `tests/test_matrix_validation_scope.py`: 36 passed.
- The ci.yml lean list: see the PR body.

## Harness cases

| Case | Proves |
| --- | --- |
| I1 | armed + schedule + exact supersede value + superseding sync busy: exit 1 `[superseded_by_run, production_run_in_flight]`; zero writes, no provenance, no audit, one probe; the read used `num_retries=2` |
| I2 | armed + schedule + `[source_not_completed, final_job_not_executed]` + SOURCE busy: exit 1 with both reasons plus `production_run_in_flight`; zero writes, no audit |
| I3 | the REAL googleapiclient request: one timeout is retried when armed and fails closed when off; three timeouts fail closed when armed |
| I4 | 10 negatives keep exit 3 refused with no `production_run_in_flight` (dispatched, no event, explicit source, nothing busy, an unrelated run busy, probe error, loose supersede value, `final_job_not_executed` alone, `not_main`, SOURCE not busy); a probe error is recorded; sweep and kill unchanged, with no read retry |
| I5 | off == the v1.2.0 base on 13 fixtures x 11 gate values, plus armed-on-dispatch == base; `OFF_GOLDEN` digests where the base is missing |
| I6 | armed vocabulary 1/true/yes/on (any case, padded) only |
| N1 | step-level mapping, job env without the gate; version floor `>= (1, 2, 1)`; verifier pin; WHY order |
| N2 | added lines ASCII; removed non-blank lines are exactly the version line and the two lines of the old key-read expression; no function removed; exactly `inflight_defer_enabled` and `_defer_for_run` added |

`tests/test_release_auto_source.py` changes:
- its fake `_Req.execute` now takes `num_retries`, like the real client;
- the exact 1.2.0 pins became a floor plus the verifier pin;
- the autouse env fixture clears `TFB_RELEASE_INFLIGHT_DEFER` and `GITHUB_EVENT_NAME`;
- **A8** is a blocking-lane guard: off refuses exactly as v1.2.0; armed on a schedule with the superseding sync in flight defers with zero writes.

That file runs in the blocking lean lane (ci.yml). The new harness runs in the advisory whole-suite job.

## Operator steps (owner) - nothing is armed by this PR

1. After merge, set the repository variable `TFB_RELEASE_INFLIGHT_DEFER=1`. Each scheduled release that lands inside the sync that superseded or produced its cohort then ends green with the warning "Scheduled decision feed attempt: no release this slot (exit 1)". The artifact carries `inflight_defer` and `in_flight`.
2. Read the next few 10:xx UTC slots. They should be deferred (green) on days the morning sync is still running, and released or refused exactly as before otherwise.

## Known limits

- A source stuck queued, or `in_progress` for hours (for example waiting on an environment approval), now defers green on every slot instead of going red. Deferral needs two API reads that agree (provenance says not completed, and the probe lists that exact run), so it adds no new class of hidden failure. The probe already deferred green whenever any production sync was in flight.
- A race in which the source completes between the provenance read and the probe gives exit 3 (red), as today. No write happens either way.
- The armed read retry can lengthen a stalled read to about 3 x 60 s plus up to about 6 s of backoff, well inside the 30-minute job timeout.
- The cron collision itself is untouched: the release schedule and daily_sync's crons are pinned by `case_n16` and by the dispatcher work in open #781.

## Rollback

Delete the repository variable `TFB_RELEASE_INFLIGHT_DEFER` (or set it to anything outside 1/true/yes/on); no redeploy is needed. Or `git revert`.
