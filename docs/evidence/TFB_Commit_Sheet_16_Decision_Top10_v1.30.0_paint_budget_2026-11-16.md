# Commit sheet - 16_Decision_Top10.gs v1.30.0 [PAINT-BUDGET] + 31_Final_Decision_Column.gs v1.2.0 - 2026-11-16

**Owner request.** Mon 16 Nov 2026, 11:23 Riyadh: "Top_10_Investments, what about this page as well, we need to review and improve or it's ok?"

**Evidence.** `_Run_Log`, 16 Nov:
- 10:14:15 `ABORTED_PREV`: "previous cockpit run started 2026-11-16T05:44:25.662Z ... last phase reached: r_kpi at t+331.9s".
  - This is the 08:44 render, which follows the 08:41 IBKR capture.
  - `DT10_CLEAR_LATE` has been on since 9 Nov. So this render cleared the board right before the paint, and Google stopped it about 28 s later, inside the paint. Google's limit is 360 s.
  - Top_10_Investments then stayed empty below the KPI strip until a render the owner started by hand.
  - The same 08:44 render was also stopped on 9 and 10 Nov.
- Finished renders the same day took 110-168 s in total (TIMING rows, 07:22-10:16), and 98-301 s on 15 Nov.
  - They reach the paint at 95-290 s.
  - The paint itself takes 10-30 s.

**Why 300 s.** There were 125 finished renders with a TIMING row between 9 Nov 12:13 and 16 Nov 10:16:
- They reached the paint (`r_guard`) at 124 s (median). The latest were 246, 262 and 307 s; only one (9 Nov 14:00) was past 300 s, and it finished at 341 s.
- The paint plus logs took 18 s (median), with a maximum of 67 s. One outlier took 179 s.

So a 300 s budget would have skipped one finished render in a week, which 31 then re-runs. It would have kept the board in the 16 Nov 08:44 render, which reached the paint at 331.9 s.

**Root cause.** The clear runs whatever the time left. A render that reaches the paint at 330 s has 30 s left. That is too little to paint, but the board is cleared anyway.

**Change in 16 (default ON, one knob).**
- Script Property `DT10_PAINT_BUDGET_S`:
  - absent, blank or junk: 300;
  - an integer: that many seconds, clamped to 120..345;
  - `off`, `0`, `false` or `no`: the v1.29.0 render. Apart from one property read, the result is identical.
- When the budget is on:
  - The refresh reads the B2 status line before writing `running...`.
  - The board clear always runs late (as with `DT10_CLEAR_LATE=on`).
  - Right before that clear, the render measures how long this execution has run (`dt10ExecElapsedS_`):
    - normally from `DT10_EXEC_T0_`, which is set by global code at the start of the execution, so time spent by the morning job or the decision chain counts too;
    - from the refresh start when that clock is not plausible.
- Past the budget, nothing is cleared or painted:
  - B2 gets back its previous line, plus ` | SKIPPED render <time> (out of time: t+<s>s, paint budget <s>s) - the board below is the previous render, unchanged; nothing was cleared`. Only one SKIPPED tail is kept. A missing or `running` line becomes `Last run <now> | status: SKIPPED`.
  - One `_Run_Log` WARN `PAINT_SKIPPED` row is written, and the P-195 marker is cleared.
  - Nothing else is written: no selection, exit or shadow log, no `_Status` row, no last-success memory.
- Readers stay correct, because the kept line still describes the board on screen:
  - The Daily Plan and Decision Pages v2 read that board's own `status: ok` and its time.
  - The decision chain and the feed-release receipts see no new stamp, just as after a stopped render, so they retry.
- `dt10SelfTest` gains two lines: the knob and a pure core battery.

**Change in 31 (1.2.0).**
- `fdcStuck_` reads the SKIPPED tail as a stopped run, keyed `skipped <time>`.
- `fdcRerunDue_` re-runs it on the next 10-minute tick, without the 20-minute wait. As in 1.1.0, this happens once per run, only 07:00-23:00 Riyadh, and `FDC_RERUN=off` turns it off.
- New: at most 4 re-runs per Riyadh day (`FDC_RERUN_DAY` = `YYYY-MM-DD|n`). The count is added only once the trigger exists.
- `fdcStatus` shows the day's count.

**Diff.**
- 16:
  - 11 anchored hunks. +5 functions (`dt10PaintBudgetCore_`, `dt10PaintBudgetS_`, `dt10ExecElapsedS_`, `dt10SkipLineCore_`, `dt10PaintSkipped_`), +5 vars, 0 removed. Census 285 / 170.
  - ES5; no new non-ASCII character; every prior WHY kept.
  - SHA-256 goes from v1.29.0 `062b32e9` (origin/main 5fc749f) to v1.30.0 `f75fa071`.
- 31: 1.1.0 to 1.2.0, SHA-256 `e53688f5`.

**Tests.**
- `tests/test_dt10_v1300_paint_budget.js` (+ `.py`): P0-P8 PASS 9/9 x3, digest `244c703936e2b664`. It runs the REAL refresh and the REAL render in the v1.29.0 harness shims.
  - **P0:** base oracle and census.
  - **P1:** golden negative. Off, in 5 spellings, with the timer or the late clear on and the execution start at 0 or 340 s back: deep-equals the base, reads included.
  - **P2:** default on, inside the budget: the base board with the late clear, every write in order, plus one property read and one B2 read.
  - **P3:** past the budget:
    - nothing is written from row 15 down;
    - the writes are a prefix of the painted run's;
    - B2 is kept with one SKIPPED tail;
    - one PAINT_SKIPPED row; marker cleared; no memory and no selection log;
    - a run at exactly 300 s, a 345 s budget, an implausible start and off all still paint.
  - **P4:** the render alone. Past the budget, the zones and merges are untouched.
  - **P5:** pure cores. **P6:** self-test. **P7:** lint.
  - **P8:** 8 mutants are each caught.
- `tests/test_fdc_v100_final_decision_column.js`: 10/10 x3. New S10 covers the skip re-run, once-only, the day cap, the stopped-run cap, failure not counted, hours, and `fdcStatus`.
- History: `tests/js/dt10_cash_utc_history.js` gains `v1290Source`, with SHA `062b32e9` pinned; `v1280Source` chains through it. The v1.29.0 harness now reads the rebuilt v1.29.0 file and still passes 7/7 with digest `95c247c3908db02a`, unchanged.
- Pins moved to 1.30.0 in 14 harnesses. Census 285 / 170. Three self-test line counts gain the two lines.
- All cockpit harnesses and `tests/js/*.test.js` pass.
- `ci.yml` 1.1.80 adds the harness to the blocking lean lane.

**After merge (owner).**
1. Paste `16_Decision_Top10.gs` (1.30.0) over the current file.
2. Paste `31_Final_Decision_Column.gs` (1.2.0) over the current file. There is no need to run fdcInstallTrigger again.
3. Optional: run `dt10SelfTest`. It should read "paint budget (DT10_PAINT_BUDGET_S): 300 s" and "paint budget core: ok".

There is nothing to arm: the budget is on by default.

**Rollback.** `DT10_PAINT_BUDGET_S=off` (no re-paste), or re-paste v1.29.0.
