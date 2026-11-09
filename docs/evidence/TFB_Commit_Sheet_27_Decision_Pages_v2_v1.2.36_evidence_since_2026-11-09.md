# Commit sheet - 27_Decision_Pages_v2.gs v1.2.36 [EVIDENCE-SINCE] - 2026-11-09

**Request.** Owner, Mon 9 Nov 2026 11:44 Riyadh: *"MODEL EVIDENCE 48.70% INVESTABLE hit rate, matured records ... I need to improve it between 60% -55% at least ... check what is preventing that and let's correct it."*

**Problem.** The engine fix is data_engine_v2 5.160.0 [EVIDENCE-COHORT] (PR #941). It sets an overall-score floor of 70 and stops calling Tadawul names with forecast confidence of 0.75 or more INVESTABLE. On the same matured records it scores 58.8%, against 48.7% today. The tile, however, pools every matured Performance_Log WIN/LOSS record since June (1,268 INVESTABLE records), so new records will take months to move it. The owner could not see whether the change works.

**Change (one gate, default off).**
- Script Property `DP2_EVIDENCE_SINCE` = `YYYY-MM-DD` or `YYYY-MM-DD HH:MM`, in Riyadh time.
  - It is compared with each record's `Date Recorded (Riyadh)`; the header `Date Recorded` is also read.
  - Anything else reads off. Off is the v1.2.35 pages byte for byte.
  - The property is read once per execution, and a fault reads off.
- **Top_10_Investments_v2, Model evidence tile.** The tile adds a figure for the matured INVESTABLE records made at or after that time. The all-time value is unchanged. Three forms:
  - ` · since <when>: <rate> (n <records>)`;
  - ` · since <when>: none matured yet`;
  - ` · since <when>: n/a (no Date Recorded column)`.
- **MODEL EVIDENCE chart.** It adds the bar `INVESTABLE since <when> (n N)` directly under the INVESTABLE bar once one such record has matured. Every other bar is unchanged.
- **dp2SelfTest.** It adds one line while the gate is on: `evidence since (DP2_EVIDENCE_SINCE): <when> · N matured INVESTABLE records since[, hit rate X] · core ok`. The line FAILs when Performance_Log has no Date Recorded column.
- **Unchanged:** Portfolio_Decision_v2, and every gate, ticket, sizing, alert, order and broker call.

**Diff.**
- 9 anchored hunks.
- Functions: +5 (`dp2EvidenceSinceParse_`, `dp2EvidenceSince_`, `dp2RecordedKey_`, `dp2EvidenceSinceTxt_`, `dp2EvidenceSinceCoreOk_`).
- Vars: +3 (`DP2_EVIDENCE_SINCE_PROP`, `DP2_ES_READ_`, `DP2_ES_`).
- Nothing removed. ES5/ASCII in every hunk; every prior WHY kept verbatim.
- SHA-256: v1.2.35 `dc88cfab` (origin/main 772a497) to v1.2.36 `487c0778`.

**Tests.**
- `tests/test_dp2_v1236_evidence_since.js` (+ `.py` wrapper): P0-P8 PASS 9/9 x2, digest `cf34eae77d81d1ae`.
  - **P0:** oracle on the v1.2.35 file rebuilt from `tests/js/dp2_edits_v1236.json` (SHA pinned); inventory checked.
  - **P1, golden negative:** 18 off values and a property fault, on two boards plus a dated log, give pages equal to v1.2.35. Armed, Portfolio_Decision_v2 alone is unchanged.
  - **P2:** with no Date Recorded column, only the tile text differs.
  - **P3, dated log** (a fixture of 30 records plus 11):
    - the tile shows 66.7% (n 6) beside the unchanged all-time 59.3%, and the bar sits under INVESTABLE;
    - covered cases: the exact boundary counts, one second before does not, a date-typed cell is read in Riyadh time, a date-only cell, the `T` separator, the header fallback, "none matured yet", and a date-only property counting the whole day.
  - **P4:** pure functions. **P5:** gate reads. **P6:** self-test. **P7:** lint.
  - **P8:** 9 mutants caught (boundary, any label, UTC date, calendar check, bar order, wins counted, text, date-only key, header fallback).
- `tests/js/dp2_history.js` adds `v1235Source` (the v1.2.36 hunks reversed, SHA pinned); `v1234Source` chains through it.
  - The v1.2.35 harness reads the rebuilt v1.2.35 file.
  - The v1.2.31-v1.2.35 harnesses pass unchanged: 11/11, 10/10, 10/10, 11/11 and 9/9.

**Arm (after install).**
1. Paste the file into Apps Script.
2. Set `DP2_EVIDENCE_SINCE` to the time the engine rule went live, e.g. `2026-11-09 13:00`.
3. The next Top_10_Investments_v2 build shows the tile line. The first 1W records mature about a week later.

**Rollback.** Delete the property, or Project History -> 1.2.35.
