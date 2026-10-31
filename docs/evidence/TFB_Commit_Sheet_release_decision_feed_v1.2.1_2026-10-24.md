# TFB Commit Sheet - release_decision_feed v1.2.1 [SCHEDULED SLOT DEFERRAL]

Renumbered 2026-10-31 (merge of main #873-#881): main #877 shipped release_decision_feed **1.3.0** (render settle, `TFB_RELEASE_RENDER_SETTLE`, default off) and its workflow 1.2.0, reserving 1.2.1 / 1.1.1 for this PR. The merged module carries 1.3.0's code, so this build is **1.3.1** (workflow **1.2.1**), with its WHY blocks above main's, which are verbatim. The two gates touch different steps: this one steps 1, 4 and 5 (before any audit), the render settle step 6b (after the strict audits); with both off the module is v1.3.0. `verify_deployment` pins 1.3.1. `tests/test_release_inflight_defer.py` now takes main's v1.3.0 (`05b51ebe`) as its off base: OFF_GOLDEN is unchanged, PASS 8/8 (N2 now runs against 05b51ebe), and the golden negative with v1.3.0 delivered fails 6 cases as before. `tests/test_release_render_settle.py` (#877) passes unchanged. No default changed. The figures below are as measured on 2026-10-24 against v1.2.0.

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

The release slot on cron `30 5 * * *` fires about 4-5 h late (about 09:40-10:30Z). That lands inside the delayed morning daily_sync (cron `17 4`, which starts at about 08:56-09:28Z and runs 57-71 min). The workflow turns exits 1 and 2 green on a schedule, but not exit 3. So the collisions show up as red runs that look the same as the refusals that matter.

`docs/reviews/github-run-census-20261024.md` (#828) lists this workflow with: "P1: align timing; safely retry reads where appropriate. Preserve no-promotion semantics."

## Evidence (job logs and run timings, read 2026-10-24)

| Release run | Created (UTC) | Exit / reasons | What was running |
| --- | --- | --- | --- |
| 40083081692 | 2026-10-24 09:43:28 | 3 refused `superseded_by_run` | daily_sync 40079965055, 08:56:07-09:53:30 |
| 39938800176 | 2026-10-23 10:23:50 | 3 refused `superseded_by_run` | daily_sync 39931603565, 09:24-10:35 |
| 39763157033 | 2026-10-22 10:26:51 | 3 refused `source_not_completed`, `final_job_not_executed` | its own source, daily_sync 39755653571, completed 10:28:54 |
| 39584712312 | 2026-10-21 10:27:24 | 3 layout_fault, `TimeoutError: The read operation timed out` on the bare key read (a 61 s read) | overlapped daily_sync 39576807720, 09:25-10:34 |

As of 2026-10-24 14:30Z there are 6 red release runs since 19 Oct:

- 3 are refusals inside a running sync (the first three rows); the step-4 and step-5 deferrals cover them;
- 1 is a Sheets read timeout that overlapped a sync (the last row); the read retry covers it;
- 2 are dispatched genuine rejections, exit 2 `not_accepted`: 39245386428 (19 Oct) and 40102546721 (24 Oct 14:27Z, dispatched by the owner). They stay red under this change.

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
| step 4 | A `superseded_by_run` refusal whose prior is EXACTLY the supersede grammar (`SUPERSEDE_VALUE_RE`) defers only when SOURCE is older than the superseding run (`int(source) < int(by)`) AND the superseding run is busy. Result: exit 1, outcome `deferred`, reasons `[superseded_by_run, production_run_in_flight]`, `deferred=True`, `in_flight` = the probe list |
| step 5 | A provenance refusal defers only when its reasons include `source_not_completed`, are a subset of `{source_not_completed, final_job_not_executed}`, and SOURCE is busy. Result: exit 1 with the provenance reasons plus `production_run_in_flight` |

These keep exit 3 with the gate armed:

- a probe error;
- a busy list that does not contain that run;
- a non-exact supersede value;
- a re-release, or a superseding run that is not newer than SOURCE (`source >= by`): the cell keeps its supersede value, so the next slot would not read a newer cohort;
- any other refusal reason;
- `final_job_not_executed` alone (the source has completed);
- `not_main` or `event_not_production`;
- a dispatched run;
- an explicit numeric source.

`mode=sweep` and `mode=kill` are untouched, and their read gets no retry. The sticky `TFB_DECISION_RELEASE_KILL` variable and the in-release sweep run inside `mode=release`, after the key read. On a scheduled auto slot their read therefore gets the retry, and nothing else changes (harness I4 pins this).

Other files:

- `.github/workflows/release_decision_feed.yml` v1.1.1: the "Release or retract the decision feed" STEP env gets `TFB_RELEASE_INFLIGHT_DEFER: ${{ vars.TFB_RELEASE_INFLIGHT_DEFER || '0' }}`. It goes in the step env, not the job env, because `case_n16` requires the job env to equal the final job's env plus the two release gates.
- `scripts/verify_deployment.py`: pin 1.2.0 -> 1.2.1. `SCRIPT_VERSION` is unchanged, and the flag is not registered in FLAGS, so the render env contract is unchanged.

## Measured results (offline, venv Python 3.11)

**Harness** `tests/test_release_inflight_defer.py`: PASS 8/8 as a script and 8 passed under pytest. With no git history (`GIT_DIR=/nonexistent`, as on CI's depth-1 checkout): 7 passed, 1 skipped. N2 skips, and I5 checks every delivered run, including armed-on-dispatch, against the recorded `OFF_GOLDEN` digests.

**Golden negative.** The base is `git show 83283c61:scripts/release_decision_feed.py`, run as the delivered module. Result: 6 failed, 1 passed (I5, the off identity), 1 skipped (N2).
- On I1 (superseded, superseding sync busy) and I2 (source still running) the base returns exit 3 refused.
- On I3 the base dies with exit 3 layout_fault after one HTTP call.

**Off identity (I5).** Covers 15 fixtures, the 9 non-armed spellings plus unset, and an armed gate on a dispatched attempt. Each is compared with the base on:
- the full normalised result JSON;
- the read call (`execute()` with no `num_retries`);
- the provenance calls, the probe call count (0 on refusals) and the writes.

All equal. The `OFF_GOLDEN` digests are proven against the base where it is present.

**Real client (I3).** A `googleapiclient` service built from the bundled `sheets.v4.json`, with a transport that raises `TimeoutError` once:
- armed: retried (2 HTTP calls), and the run continues to the v1.2.0 provenance step;
- off: fails exit 3 layout_fault after 1 call;
- three timeouts while armed: still fails closed.

**Mutations** (`-x` per mutation): 17 of 17 caught. The mutations were:
- always defer at step 4;
- no exact grammar check;
- no `source < by` check;
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
- release, verifier-pin, env-contract, flag-registry, workflow-flag-mapping and matrix-validation suites: 133 passed, 11 skipped.
- workflow audit tests: 226 passed.
- `scripts/audit_repository_workflows.py`: errors=0 warnings=0.
- the three release suites in both orders in one process: 62 passed, 10 skipped.
- The ci.yml lean list: see the PR body.

## Harness cases

| Case | Proves |
| --- | --- |
| I1 | armed + schedule + exact supersede value (SOURCE older than the superseding run) + superseding sync busy: exit 1 `[superseded_by_run, production_run_in_flight]`; zero writes, no provenance, no audit, one probe; the read used `num_retries=2`; the same with `TFB_DECISION_RELEASE=1` (production) |
| I2 | armed + schedule + `[source_not_completed, final_job_not_executed]` + SOURCE busy: exit 1 with both reasons plus `production_run_in_flight`; zero writes, no audit; also with `TFB_DECISION_RELEASE=1` |
| I3 | the REAL googleapiclient request: one timeout is retried when armed and fails closed when off; three timeouts fail closed when armed |
| I4 | 12 negatives keep exit 3 refused with no `production_run_in_flight`, dry-run and with `TFB_DECISION_RELEASE=1`. The cases: dispatched, no event, explicit source, nothing busy, an unrelated run busy, probe error, re-release (`source == by`), superseding run older than SOURCE, loose supersede value, `final_job_not_executed` alone, `not_main`, SOURCE not busy. The two supersede-order cases make no probe; a probe error is recorded. `mode=sweep` and `mode=kill` read without retry. The sticky KILL variable on a scheduled release reads with the retry and is otherwise equal to gate-off |
| I5 | off == the v1.2.0 base on 15 fixtures x 10 gate values (unset plus 9 spellings), plus armed-on-dispatch; every run is checked against the `OFF_GOLDEN` digests, which are proven against the base when it is present |
| I6 | armed vocabulary 1/true/yes/on (any case, padded) only |
| N1 | step-level mapping, job env without the gate; version floor `>= (1, 2, 1)`; verifier pin; WHY order |
| N2 | added lines ASCII; removed non-blank lines are exactly the version line and the two lines of the old key-read expression; no function removed; exactly `inflight_defer_enabled` and `_defer_for_run` added |

`tests/test_release_auto_source.py` changes:
- its fake `_Req.execute` now takes `num_retries`, like the real client;
- the exact 1.2.0 pins became a floor plus the verifier pin;
- the autouse env fixture clears `TFB_RELEASE_INFLIGHT_DEFER` and `GITHUB_EVENT_NAME`;
- **A8** is a blocking-lane guard: off refuses exactly as v1.2.0; armed on a schedule with the superseding sync in flight defers with zero writes; a re-release stays refused.
- **A9** is the blocking-lane guard for step 5: SOURCE still running and busy defers; `not_main`, `final_job_not_executed` alone and SOURCE not busy stay exit 3.

That file runs in the blocking lean lane (ci.yml). The new harness runs in the advisory whole-suite job.

## Adversarial review (before the PR)

A four-lens review workflow ran: masking, off identity, harness and CI, and evidence against the Actions API. Each non-nit finding went to an independent refuter, and every confirmed finding is addressed here.

| Finding | Disposition |
| --- | --- |
| Step 4 also deferred the 'never re-release' branch of `superseded_by_run` (`source >= by`) while the superseding run was busy: a temporary false green over a genuine refusal (no write) | **Fixed.** It now defers only when `int(source) < int(by)`. Two new negatives, `re_release` and `older_superseder`, are in I4 (no probe), A8 and `OFF_GOLDEN`, plus a `no source<by` mutation |
| The 'two API reads that agree' argument covered step 5 only | Known limits now separates step 4 (the cell plus one probe read) from step 5 (two reads) and names a superseding writer stuck `waiting` |
| The read retry also reaches the sticky-KILL and in-release sweep paths | Kept, because it is a beneficial read retry. The documentation is corrected, and I4 pins it (equal to gate-off apart from the retry) |
| The red-run count went stale (owner dispatch 40102546721 at 14:27Z) | Updated to 6, with 2 dispatched genuine rejections. The 21 Oct timeout is described as an overlap that the read retry fixes |
| I5 counts were off by one; armed-on-dispatch was unchecked without the base copy; the read request itself was not compared | Counts are corrected. Every I5 run now checks `OFF_GOLDEN`, including armed-on-dispatch, and the digests cover the get request, probe and provenance calls and the writes |
| Step 5 guards were only in the advisory lane; no case used `TFB_DECISION_RELEASE=1` | **A9** covers step 5 in the blocking lane. I1, I2 and I4 also run with `TFB_DECISION_RELEASE=1` |
| Harness hygiene | Removed the dead `@@` guards; `_load` restores `sys.path`; I3 uses `importorskip` for googleapiclient and httplib2; the script-mode digest covers outcomes; the A8/A9 docstring is updated |
| Erratum: 3,956 counts every pending record | Reworded. The unpriced population is named separately (359 symbols, 0 priced by the fallback) |

The off-identity lens also ran its own independent probe, with zero differences against the base:

- 884,700 off comparisons over 88,470 scenarios;
- 277,572 armed comparisons where the gate does not apply.

These figures were measured on the pre-fix build. The fix only narrows the armed step-4 path.

## Operator steps (owner) - nothing is armed by this PR

1. After merge, set the repository variable `TFB_RELEASE_INFLIGHT_DEFER=1`. Each scheduled release that lands inside the sync that superseded or produced its cohort then ends green with the warning "Scheduled decision feed attempt: no release this slot (exit 1)". The artifact carries `inflight_defer` and `in_flight`.
2. Read the next few 05:30-cron slots (they fire about 09:40-10:30Z). They should be deferred (green) on days the morning sync is still running, and released or refused exactly as before otherwise.

## Known limits

- **Step 5 rests on two API reads that agree:** provenance says not completed, and the probe lists that exact run. A source stuck queued, or `in_progress` for hours (for example waiting on an environment approval), now defers green on every slot instead of going red. The v1.2.0 probe already deferred green whenever any production sync was in flight, so this is not a new class.
- **Step 4 rests on the cell plus one probe read:** the exact supersede value names a superseding run newer than SOURCE, and the probe lists that run as busy. A superseding writer stuck `waiting` or `queued` (including a `page_refresh_recovery.yml` dispatch, which supersedes but produces no new daily_sync cohort) defers green on every slot until it completes. When it completes, the red returns.
- A race in which the source completes between the provenance read and the probe gives exit 3 (red), as today. No write happens either way.
- The armed read retry can lengthen a stalled read to about 3 x 60 s plus up to about 6 s of backoff, well inside the 30-minute job timeout.
- The cron collision itself is untouched: the release schedule is pinned by `case_n16`, and daily_sync's cron is the subject of open #781.

## Rollback

Delete the repository variable `TFB_RELEASE_INFLIGHT_DEFER` (or set it to anything outside 1/true/yes/on); no redeploy is needed. Or `git revert`.
