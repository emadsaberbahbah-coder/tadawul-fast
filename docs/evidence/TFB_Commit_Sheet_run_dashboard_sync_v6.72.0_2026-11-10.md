# Commit sheet: run_dashboard_sync 6.72.0 [STAMP-REPAIR] (2026-11-10)

## Why

Run [42621566901](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/42621566901) (scheduled, 9 Nov 2026, 15:59 Riyadh):

- Every data job succeeded. Global_Markets wrote 6,601 rows (`[PAGE-VERDICT] status=success rows_written=6601`).
- Its `_Status` stamp failed. All 4 v6.70.2 attempts timed out reading `_Status!A:A` between 17:01 and 17:05 Riyadh, while the workbook was busy with Apps Script triggers. The log line: "stamp FAILED for Global_Markets after 4 attempt(s) ... The stamp was NOT written".
- `_Status` therefore kept the morning run 42556077749. Every Top_10 render after that was WITHHELD: "Global_Markets source run 42556077749 differs from cohort 42621566901". At 23:11 the Mutual_Funds pool shortfall was still unexplained for the same reason.
- No render wrote a receipt. "Verify final decision publication" waited 1,751 s and then failed (`decision_failed`), so the feed was never promoted.
- Reads of the same workbook worked again from about 17:11 (the recover job, then the 30 polls of the verify job).
- The same failure happened on 20 Oct (run 39513589733) before v6.70.2 added the in-place retries, and on 27 Oct (run 40556442682) before daily_sync 1.0.7 raised the budget to 300 s. In-place retries alone do not outlast a sustained burst of load.

## What

`scripts/run_dashboard_sync.py` 6.71.0 -> 6.72.0. It is additive: no function is removed, and 4 helpers are added.

- **Queue.** When `_stamp_page_status` gives up on a transient error, it queues the exact payload it would have written (same Last Updated time, status, message and run ID) and prints a `::notice::`. The v6.70.2 "stamp FAILED" warning text is unchanged.
- **Repair pass.** `_repair_failed_stamps` runs once at the end of `main_async`, after the stale-skip escalation and before the upstream verdict.
  - It waits 20, 40, 60, 90 and then 120 s between attempts, within `TFB_SYNC_STAMP_REPAIR_BUDGET_S` (default 480, clamped 60..900).
  - Each attempt re-reads column A and updates the page's row, as the in-place stamp does. A non-transient error stops that page at once.
  - Outcome: `::notice:: ... stamp written (row N)` or `::warning:: ... still NOT written`.
  - It never raises, and the exit code is unchanged.
- **Gate.** `TFB_SYNC_STAMP_REPAIR`: `0` (default; no queue, no call, no sleep, byte-identical) | `1`.
- **Workflow.** `daily_sync.yml` 1.0.12 maps both variables in both page-writing jobs, defaulting to `'0'` and `'480'`. The job timeout of 115 min leaves room for an 8-minute repair on top of the measured 65-minute Global_Markets leg.
- **Pins.** `verify_deployment.py` pins 6.72.0. The version pins in three existing suites moved. The fw2 A1 function-set check names the four new helpers.
- **CI.** `ci.yml` 1.1.74 runs the new suite in the blocking lean lane.

## Tests

`tests/test_sync_stamp_repair.py`: 23 cases (R1–R10). It reuses the Google API double from `test_sync_status_stamp_retry.py` and runs the real module functions.

- **Default off.** The gate is inert when off.
- **Queueing.** An exhausted stamp queues its exact payload. A non-transient stamp error is not queued.
- **Repair.** The pass writes that payload after waits of 20 s and 40 s. The budget ends the pass with nothing written. A non-transient repair error stops at once.
- **Queue handling.** The queue empties, and a page keeps one entry.
- **Settings.** Clamps and gate values are checked.
- **Placement and pins.** The call order in `main_async` and the version pins are checked.

Results:

- **Local, three identical runs:** 189 passed. The two failures are pre-existing on main and come from base-diff checks against old workflow commits; CI skips them on its shallow checkout. They are `test_daily_sync_stamp_budget::test_b4` and `test_fw2_stale_pe::test_w1b`.
- **The other 21 sync and workflow suites:** 609 passed. The flag registry, activation and matrix-scope suites: 155 passed.
- **AST:** removed 0; added `_repair_failed_stamps`, `_stamp_repair_budget_s`, `_stamp_repair_enabled`, `_stamp_repair_enqueue`.
- **Text checks:** no smart quotes. The only non-ASCII added line is the title, as in every version.

## Arming (owner)

1. Merge this PR.
2. Set the repository variable `TFB_SYNC_STAMP_REPAIR=1` at <https://github.com/emadsaberbahbah-coder/tadawul-fast/settings/variables/actions>.
3. Read back on the next sync. Either no stamp fails, or the Global_Markets job log shows `[STAMP-REPAIR v6.72.0] Global_Markets _Status stamp written`.

**Rollback:** set the variable to `0`, or delete it.

## Files (SHA-256, first 16)

- `scripts/run_dashboard_sync.py` `936b0dfeb06f09e0` (base 6.71.0 `0f1d4411de86e42a`)
- `tests/test_sync_stamp_repair.py` `821051d01c8e1af8`
- `.github/workflows/daily_sync.yml` `1c957bdcb49a8105`
