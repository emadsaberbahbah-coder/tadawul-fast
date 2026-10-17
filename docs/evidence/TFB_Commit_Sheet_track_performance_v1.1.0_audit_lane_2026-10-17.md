# TFB Commit Sheet — track_performance.yml v1.1.0: scheduled maturity audit lane (D-L1) + daily_sync mirror guard + tests

Date: 2026-10-17 (Saturday) · Lane: GitHub Actions (two workflow files) + tests · Brief: Codex Brief 2026-10-17 v2 §2 item **L1** (P0) · Decision: **D-L1** (pending with Emad — prepare the PR, merge on his word) · Register: IR-0xx "Scheduled maturity audit disabled by #759"

## S0/S1 — Base pin
Base HEAD `11ef1345` (main, 2026-10-15 "#776 10_My_Portfolio v1.5.13").

| File | Base | Delivered | Change |
|---|---|---|---|
| `.github/workflows/track_performance.yml` | sha256 `6f493abb4b4369dae83b40f1f4b3fd04085b93e2b1b71790bf83e33a8746bf19` (470 ln; last touch `48925d5` 2026-10-14 #762) | sha256 `4d13ce81e993ef1b660d6e5896c79b026235fc472e771f248cb14d97f94dacec` (592 ln) | **+129 / −7** (9 anchored edits, each `count == 1`): v1.1.0 header; third cron; job `timeout-minutes` expression; 🚦 step env (4 controls) + wait-then-fail-closed audit branch (the curl/jq read moved into `sync_inflight()` — the 7 removed lines are that block, re-emitted inside the function verbatim); run-step `AUDIT_LANE` env + lane resolution + `$RECORD_FLAG`; `Lane` row in the summary |
| `.github/workflows/daily_sync.yml` | sha256 `00f6d95803170f8b7b63ed6bd214a049c18d47a2d8640cfb9453d2bb6b680956` (2,273 ln; last touch `278f148` 2026-10-14 #763) | sha256 `7ec43279968bf51c6c77e615adb216e7f6197f0e20ee526b30d0e8fe02633618` (2,306 ln) | **+33 / −0**: dated lead comment, `GH_TOKEN` in the tracker step env, mirror guard inside the tracker step's run block (before the `# Visibility (2026-06-18)` comment) |
| `tests/test_tracker_record_only_workflows.py` | sha256 `b5b28a082d43accfcb378c263947fbe6a020cbc49a4bef9707285e4e2cc0e6f9` (80 ln) | sha256 `8e0267aa6312614c4b6aad833e900fa4b300bca81e9579404e36830ecc46a38f` (108 ln) | **+37 / −9**: the record-only contract becomes the explicit three-way contract (manual / granted schedule / everything else) |
| `tests/test_tracker_audit_lane.py` | — | sha256 `97c6c59498931d8a08c009a924dd636d8eea8f910d2263b559f2de6e3738ff37` (313 ln) | NEW, 17 tests, offline (stub `curl`, real `jq`) |
| `.github/workflows/ci.yml` | — | sha256 `b57b8b89c2e186ad34eaad8707d870ed5242faaa21286d93f387ded7ed853ae2` (881 ln) | **+1 / −0**: the new test file in the lean pytest list, after `tests/test_track_flags_mapped.py` |

## S2 — Why
PR #759 (merged 2026-10-12 16:23 Riyadh, `track_performance.yml` v1.0.1) made every scheduled tracker run record-only (`TRACK_AUDIT=0`, `TRACK_PUBLISH_CALIBRATION=0`, `TFB_SIGNAL_TRENDS_TAB=0`, no `--audit`); `--audit` is reachable only through `workflow_dispatch run_audit=true`. Measured on the 17-Oct export: last maturation 2026-10-10 14:55; **2,176 active Performance_Log records past their target date and unscored** (1W 490 · 2W 462 · 1M 479 · 3M 745); `_S1_Calibration` as_of 2026-10-11 23:00:18; S-1 criterion 4 PENDING. The comment #759 replaced had warned: "without it picks never resolve to win/loss, so calibration never gets decided outcomes". The #759 *intent* — no reserialization race between the 14:47 clock and daily_sync's tracker step — is kept intact; the audit moves to a dedicated lane with a real single-writer check.

## S3 — Design
**Lane** — third cron `47 20 * * *` (23:47 Riyadh, after the US close). Both existing slots (`47 11`, `47 15`) are byte-for-byte record-only as v1.0.1 left them.

**Single-writer proof (the brief's requirement: check the sync's in-flight state, not the cron).** daily_sync's slots are `17 4,12,20 UTC`; the 20Z slot has fired as late as 23:00Z and one full sync took 4h37m (23:17Z→03:54Z), so a "skip if in flight" lane would skip most nights. The 🚦 step therefore **waits**: on the audit cron it polls the Actions API for queued/running/waiting/pending Daily Sync runs every `AUDIT_POLL_SECONDS` (120) for up to `AUDIT_WAIT_MINUTES` (120) and grants `audit=true` only on a zero reading. It **fails closed**: an unreadable sync state or an expired deadline skips the run (no audit, no record) with a `::warning::` — auditing blind *is* the #759 race (load → audit → `save_records` rewrites every row). The record lanes keep their v1.0.2 fail-open skip semantics exactly. Job `timeout-minutes` becomes `${{ github.event.schedule == '47 20 * * *' && 180 || 45 }}` so the wait budget fits; record slots keep 45.

**Mirror guard (the other direction).** A sync that starts *during* the audit would reach its tracker step (an append) inside the audit's load→save window. daily_sync's tracker step now reads `track_performance.yml` runs with `status=in_progress` and skips its record when any exist (notice in the log). Fail-open (unreadable → record as before): the clock side fails closed, so one blind side is the accepted residual; only `in_progress` counts because a *queued* clock run will itself wait for the sync. Offline harnesses have no `GITHUB_API_URL` and take the recording path unchanged.

**Run step.** `AUDIT_LANE: ${{ steps.overlap.outputs.audit }}`; the lane is taken only when `GITHUB_EVENT_NAME == schedule` **and** `AUDIT_LANE == true` (both re-checked). It exports `TRACK_AUDIT=1` and `TRACK_PUBLISH_CALIBRATION=1` explicitly (an inherited default must never decide a maturation) and runs `--audit --analyze --export`. `TRACKER_LANE` (`record-only` / `manual-audit` / `scheduled-audit`) is echoed, written to `GITHUB_ENV` and shown as a `Lane` row in the job summary.

### Deviations from the brief, stated
1. **No `--record` on the audit lane** (brief: `--record --audit --analyze --export`). The slot sits 13 minutes before the Riyadh day boundary (21:00Z) and the file's own measurements put cron drift at 3–7 h, so a `--record` there would almost always be keyed to the *next* Riyadh day from a pre-market board and pre-empt that day's 14:47 snapshot (keys are `symbol|horizon|YYYYMMDD`, first writer wins). Recording stays with the two record slots and daily_sync's tracker step. The receipt (`workflow_activation_receipt.py tracker`) does not require a record line; `run_once` audits without `--record` (records are loaded first, independent of `args.record`).
2. **Concurrency group kept as `tfb-performance-clock`** (brief: `tfb-tracker`). Same semantics (private to this workflow, `cancel-in-progress: false`); the file's 2026-10-06 note explains why the group must never be shared with daily_sync (a queued run cancels the pending one). Renaming would be cosmetic.

## S4 — Audits
| Check | Result |
|---|---|
| Anchored edits | 9 (clock) + 3 (sync) + 1 (ci) + 2 (test) with `count == 1` asserted; `git diff --numstat` 129/7 · 33/0 · 1/0 · 37/9 |
| YAML parse (`yaml.safe_load`) of the three workflows | PASS |
| `bash -n` on the three edited run blocks (🚦, run step, daily tracker step) | PASS ×3 |
| `python -m pytest -q` on `test_tracker_audit_lane.py` + `test_tracker_record_only_workflows.py` + `test_track_flags_mapped.py` + `test_workflow_flag_mappings.py` + `test_workflow_activation_observe.py` + `test_activation_rollout_evidence_workflows.py` + `test_scheduled_portfolio_refresh.py` | **331 passed** (baseline before the change: 113 passed on the four pre-existing files) |
| ENV parity (`test_tracker_lanes_have_parity_for_every_shared_effective_env_key`, `test_tracker_env_parity_for_dedup_flags`) | PASS — `GH_TOKEN` / `AUDIT_LANE` are not shared keys |
| ruff `E9,F63,F7,F82,F401,F841` on the two test files | PASS |
| Golden negative (14:47 / 18:47): `TRACK_PUBLISH_CALIBRATION=0`, `--record` present, `Lane: record-only` | pinned by `test_record_slots_still_write_no_calibration_row` (4 grant spellings) and the 9 legacy rows of the record-only test |
| Fail-closed paths | unreadable → `skip=true audit=false`, exactly one API read; deadline → `skip=true audit=false`; both pinned |
| Grant only on `schedule` + `47 20 * * *` | `workflow_dispatch`/`push`/empty event with the audit cron string → never granted (pinned) |

## S5 — Delivery
`.github/workflows/track_performance.yml` · `.github/workflows/daily_sync.yml` · `.github/workflows/ci.yml` · `tests/test_tracker_record_only_workflows.py` · `tests/test_tracker_audit_lane.py` (NEW) · this sheet (NEW). No application code, no ENV, no Script Property, no workbook cell.

## S6 — Arming / read-back
This change is **live on merge** (a schedule, not a flag) — which is why it needs Emad's word (D-L1): it reverses the #759 schedule contract. Nothing else in the file changes behaviour without that cron.
1. Merge. First `47 20 * * *` fire: job summary shows `Lane: scheduled-audit` (or `skipped (see the 🚦 step)` with the fail-closed warning naming the reason).
2. Read-back after the first audited run: Performance_Log overdue-active count 2,176 → ≈ 0 (matured/expired); `_S1_Calibration!A2` as_of advances; `_Status` "TFB Calibration" as_of advances; `S1_Gate` criterion 4 leaves PENDING; a `[PERF-VERDICT v6.45.0]` line in `_Run_Log` carries the audit tokens. Golden negative: the next 14:47 run's summary says `Lane: record-only` and writes no calibration row.
3. daily_sync read-back: any leg that overlaps a clock run logs `::notice:: … D-L1 mirror guard` and records nothing; Performance_Log duplicate matured keys do not grow (27 today).
4. Rollback: delete the `47 20 * * *` cron line → v1.0.2 behaviour exactly (the lane code is unreachable without that schedule). The daily_sync guard is inert without a running clock.

## Known limits
- The wait budget (120 min) is a judgement: a sync that holds the lease past 01:47 Riyadh skips that night's audit; the next night covers it. Read `AUDIT_WAIT_MINUTES` back against the measured sync durations after a week.
- The Actions API read sees daily_sync runs of every trigger (push-triggered CI-only runs included); those are minutes long and only delay the grant.
- `TRACK_RISK_BLANK` / `TRACK_DEDUP_MATURED` still default off in both lanes (Codex L7 / existing sheets): the audit lane will write 0.0 placeholders in the three risk columns until those are armed — unchanged from the manual audit path.
