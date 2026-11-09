# Commit sheet - 16_Decision_Top10.gs v1.28.0 [RENDER-TIMING] + [CLEAR-LATE] - 2026-11-09

**Why.** The cockpit render now runs into the Apps Script 6-minute limit.
- `_Run_Log`, 4-9 Nov: ABORTED_PREV rows (a previous run left no completion) 2, 9, 4, 3, 2 and 3 a day, plus 0-2 decision-chain NOT_FINISHED renders a day. Finished renders took 140-344 s (daily medians 211-275 s).
- Mon 9 Nov:
  - The 08:44 evidence follow-up was stopped at 361 s. It was meant to use the fresh 08:41 IBKR capture.
  - The 09:12 Decision Pages v2 build was stopped too.
  - The 09:20 render finished in 326 s, but with a capture 39 minutes old. Top_10_Investments therefore read QUALIFIED_UNFUNDED / HELD all morning.
  - The 10:14 follow-up was stopped at the limit after the zone clear. Top_10_Investments showed the control panel and the KPI strip, with nothing below row 16 until the next render.
- Render log timing for the 10:14 run, from the backend's POST completions: main POST answered at t+128 s and the allocation replay at t+284 s. The replay took 156 s, against 81 s in the 09:20 run. The backend CPU was idle in that window, so the extra time was spent in Apps Script.
- A finished run logs only five lines, which give coarse phases. A stopped run leaves nothing. The fix has to be aimed, so this version measures first. It also makes a stopped render harmless to the screen.

**Change (two knobs, both default off).**
- **`DT10_RENDER_TIMING` = `on`** (any case, spaces trimmed). Off means absent, anything else, or a PropertiesService fault. When off, `DT10_PH_` stays null and every `dt10PhaseMark_` is a no-op. Armed:
  - (a) `dt10PhaseMark_` at 35 points streams `console.log [DT10 v1.28.0] t+123.4s pool +40.2s` at once. The points are setup, gates, inputs, funding, src, pool, ready, post, attach, verdict1, stab, verdict2, alloc, final, then inside the render r_clear, r_verdict, r_final, r_live, r_earn, r_postearn, r_guard, r_kpi, r_selected, r_qualified, r_nearmiss, r_alerts, r_gaps, r_cands, r_end, then render, notes, sellog, exitlog, shadow, status.
  - (b) Once this run owns the P-195 marker, the marker JSON gains `ph` (last phase) and `at` (seconds). It is rewritten at most every 5 s.
    - The next run's ABORTED_PREV row then ends `; last phase reached: alloc at t+251.4s`, and its details carry `trail`.
    - A marker without `ph` (an unarmed or older run) gives the v1.27.0 row byte for byte.
    - The other readers of `DT10_RUN_MARKER` read only `started` and ignore extra keys: `dt10RunMarkerCore_` (also used by 26_Decision_Chain), `porRenderBusyCore_` (28_Plan_Of_Record) and `dp2RpBusyCore_` (27_Decision_Pages_v2). The added `ph`/`at` keys are safe for all of them.
  - (c) A finished render logs `phases 326s: setup 3.1 | gates 1.2 | ...` and writes one `_Run_Log` row: INFO, TIMING, the same line, with details `{version, total_ms, marks}`.
- **`DT10_CLEAR_LATE` = `on`** (the same spellings; a fault reads off).
  - The refresh attaches `payload._dt10_clear_late = 1`.
  - `dt10RenderPayload_` then clears the board zones (`dt10ClearZones_`, the v1.27.0 clear lifted out unchanged) after the pre-paint steps instead of before them. Those steps are the verdict recheck, finalize, live fill, earnings and post-earnings tags, and the ticket guard.
  - With the timer armed too, there is one extra mark, `r_clear_late`.
  - A render stopped inside those steps leaves the previous board on screen. That board keeps its own banner and verdict age, and the Status line still says running.
  - A finished render paints the same board.
  - Those steps read only other sheets and the payload: `_Status`, the source pages, `Calendar_Events`, and the dividend and corporate-event sheets. They never read the board zones, so none of their results change.
  - Off (absent or anything else): no payload key, and the v1.27.0 order.
- With both knobs off, the render is v1.27.0 byte for byte, apart from one extra property read per knob at start. The status line, the stability state and every decision value are untouched either way.

**Diff.**
- 38 anchored hunks.
- Functions: +7 (`dt10RenderTimingOn_`, `dt10ClearLateOn_`, `dt10PhaseStart_`, `dt10PhaseMark_`, `dt10PhaseLine_`, `dt10PhaseTrailNote_`, `dt10ClearZones_`).
- Vars: +4 (`DT10_RENDER_TIMING_PROP`, `DT10_RENDER_TIMING_MARKER_MS`, `DT10_PH_`, `DT10_CLEAR_LATE_PROP`). Nothing removed.
- ES5 / ASCII. `dt10SelfTest` gains 2 lines (the first one names both knobs), zero FAIL.
- Base: v1.27.0, sha256 `e2db7ddf87c919066988d9c28bf8e155278b72fa3f357107c2b8b890e4bb4482` (origin/main `55f53b7`).
- New: v1.28.0, 708,152 bytes.
  - Repo file sha256: `03588d67a7ed94ce5a197f0c56cf321f84aaaed6f52baca14e2d813a3163301e`.
  - Apps Script editor copy (no trailing newline) sha256: `7d2a77046df46fa144dff34bb32b1953a65d26ae35b89143d7b05d0bca32a81e`.

**Harness.** `tests/test_dt10_v1280_render_timing.js` (+ `.py`) gives PASS 9/9, x3 identical, digest `8c196eb33e6fe635`. It runs the REAL `refreshDecisionTop10` in the full-workbook shim of the v1.20.0 harness, and the REAL `dt10RenderPayload_` on a cell / merge model of the board.
- R1, GOLDEN NEGATIVE / kill: either knob absent, off, blank or junk, or a fault, deep-equals the v1.27.0 base. That covers B2, every sheet write, `_Run_Log`, `_Selection_Log`, Logger, POST bodies, property writes and the properties after the run. There is no console line, and exactly one extra read per knob.
- R2, timer armed: the same board, writes and properties as off, plus the 35 phase lines in order, one Logger line and one TIMING row. The total equals the sum of the phases. With a ticking clock, marker writes are at least 5 s apart.
- R3: the trail in the next ABORTED_PREV row. An unphased marker gives the base row.
- R4: pure functions.
- R5: self-test, both knobs, off / on / fault.
- R6: lint.
- R7: eight mutants are each caught.
- R8, CLEAR-LATE:
  - The key rides only for on, " ON " and On.
  - The refresh paints the same board, with every sheet write in the same order.
  - A render stopped in the ticket-guard step keeps the previous rows and merges, with zero clears before the stop. Off, the same stop leaves an empty zone, the same as the base.
  - A finished armed render still wipes a stale far row and merge, and equals the off board.
  - With both knobs on, the marks gain `r_clear_late` right after `r_guard`.

**Also in this change.**
- **The v1.27.0 harness's clock is frozen** (the same fix as PR #939). `tests/test_dt10_v1270_corp_guard.js` read the real clock through `new Date()`, so its "6d ago" expectation became "7d" on 9 Nov. That turned the blocking lean lane red for every PR.
- **ci.yml 1.1.71.**
  - The new harness runs in the lean lane.
  - The advisory whole-suite job gets 25 minutes instead of 15. pytest alone now takes 11.3 min on a fast runner and about 14.5 min on a slow one. Three main pushes, and PR #939's run, were cancelled at 15 min.

**History and pins.** `v1270Source` (SHA pinned) now heads the chain, and the v1.27.0 harness reads the rebuilt v1.27.0.
- Pins moved to 1.28.0 in 14 harnesses.
- Census: functions 272 -> 279, variables 160 -> 164. Self-test line counts +2. The v1.21.0 report filter drops `render timing` lines.
- Local results: all 47 node harnesses pass, and 254 pytest tests pass (12 skipped) across the 25 cockpit-related Python files.

**Arm (after merge and paste).**
1. In zz_Property_Tool, run `tfbSetAnyProperty` with `DT10_CLEAR_LATE` = `on`, then again with `DT10_RENDER_TIMING` = `on`.
2. Run `dt10SelfTest`. Expect `render timing (DT10_RENDER_TIMING): on (...); clear late (DT10_CLEAR_LATE): on (...)` and `render timing core: ok`.
3. After the next render, read the `_Run_Log` TIMING row. After a stopped render, the next ABORTED_PREV row names the last phase reached, and the board above it is the previous one, not an empty zone.

**Rollback.** Delete either property.
