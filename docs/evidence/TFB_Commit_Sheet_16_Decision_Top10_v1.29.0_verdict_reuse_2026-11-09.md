# Commit sheet - 16_Decision_Top10.gs v1.29.0 [VERDICT-REUSE] - 2026-11-09

**Evidence.** The first armed `DT10_RENDER_TIMING` line came from the 12:09 Riyadh evidence follow-up render on 9 Nov (Apps Script execution log of `pdIbkrEvidenceFollowUp`):

```
phases 205.6s: setup 1.2 | gates 2.5 | inputs 1.8 | funding 0.1 | src 9.5 | pool 29.6 | ready 26 | post 38 |
attach 0.3 | verdict1 23.5 | stab 1.5 | verdict2 22.9 | alloc 1.2 | final 0 | r_clear 0 | r_verdict 22.2 | ...
r_cands 11.4 | ... | status 3.3
```

The source readiness (`dt10SourceReadiness_`) runs five times per render: src, ready, verdict1, verdict2 and r_verdict, about 104 s of 205.6 s. It covers the approved-scope Symbol reads and digests of the four pool pages, plus `_Status`. Two of the five runs decide nothing:

| Check | Seconds | What it feeds |
|---|---|---|
| `ready` (before the POST) | 26.0 | Only the Logger line before the POST. The status line is rebuilt from the pre-paint recheck's readiness. |
| `verdict2` (after stability) | 22.9 | It repeats verdict 1 across the stability step (1.5 s), which reads neither the feed nor the source pages. The pre-paint recheck (`r_verdict`) and a second `dt10FinalizeBoard_` follow anyway. |

**Change (one knob, default off).**
- Script Property `DT10_VERDICT_REUSE` = `on` (any case, spaces trimmed).
- Absent, anything else, or a property fault means off. Off is v1.28.0 byte for byte, apart from one extra property read per refresh.
- When armed:
  - **(a)** The pre-POST readiness is not computed. The POST Logger line ends `source readiness checked after the POST (DT10_VERDICT_REUSE)`.
  - **(b)** Verdict 1 stands for verdict 2.
- The three checks that decide something stay:
  - verdict 1 (it freezes stability when sources are blocked);
  - the pre-paint recheck (it re-finalizes the board before any value is painted);
  - the selection-log recheck (it withholds logged tickets).
- With the timer armed, the 35 marks keep their names; `ready` and `verdict2` read about 0.
- `dt10SelfTest`: the render-timing line names the knob (line count unchanged).

**Expected saving.** About 49 s per render (26.0 + 22.9 s at 12:09), so 205.6 s becomes about 157 s. Contended renders save more, because each Sheets call is slower.

**Diff.**
- 10 anchored hunks.
- +1 function (`dt10VerdictReuseOn_`), +1 var (`DT10_VERDICT_REUSE_PROP`), 0 removed. Census 280 / 165.
- ES5; no new non-ASCII character; every prior WHY kept.
- SHA-256: v1.28.0 `03588d67` (origin/main 772a497) to v1.29.0 `062b32e9`.

**Tests.**
- `tests/test_dt10_v1290_verdict_reuse.js` (+ `.py`): V0-V6 PASS 7/7 x3, digest `95c247c3908db02a`. It runs on the full-workbook shim of the v1.28.0 harness (the REAL refresh).
  - **V0:** base oracle; SHA pinned; census; the hunks reverse to the base.
  - **V1:** golden negative. 9 property sets plus a fault: writes, logs, console, POST bodies, state and verdict/readiness call counts equal the base.
  - **V2:** armed. Only the POST Logger line differs; one board verdict and one direct readiness fewer; marks unchanged.
  - **V3:** the sources go blocked during stability. Off and armed both end WITHHELD with the same board, banner, KPIs and seats.
  - **V4:** self-test line. **V5:** lint.
  - **V6:** 7 mutants caught.
- History: `tests/js/dt10_cash_utc_history.js` gains `v1280Source` (SHA `03588d67` pinned); `v1270Source` chains through it. The v1.28.0 harness reads the rebuilt v1.28.0 file (digest `8c196eb33e6fe635`, unchanged).
- Pins moved to 1.29.0 in 14 harnesses: version literals, Logger-line versions, version folds and oracle regexes. Census pins are 280 / 165, and inventories add the two names.
- All 22 cockpit harnesses, the DP2 harnesses and `tests/js/*.test.js` pass.
- `ci.yml` 1.1.73 adds the harness to the blocking lean lane.

**Arm (after merge).**
1. Paste the file into Apps Script.
2. Set `DT10_VERDICT_REUSE=on` (zz_Property_Tool).
3. The next render's TIMING row should read `ready` ≈ 0 and `verdict2` ≈ 0.

**Rollback.** Delete the property.
