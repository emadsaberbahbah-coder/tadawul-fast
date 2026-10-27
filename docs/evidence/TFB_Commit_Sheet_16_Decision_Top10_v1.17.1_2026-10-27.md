# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.17.1 [SHADOW-MOMENTUM] + `scripts/score_selection_log.py` v1.2.1 [CHALLENGER INPUTS] - 2026-10-27

**Scope:** program register **A2** (project doc `TFB_Self_Correcting_Recommendation_Program_2026-10-27.md`), a fix found on the first shadow day. The cockpit now sends the engine's `Momentum Score` column - the challenger's second pre-registered key - while the shadow rank log is armed; the nightly scorecard does not count a day logged without it.
**Files:** `apps_script/16_Decision_Top10.gs` (1.17.0 -> 1.17.1; 507,569 -> 513,214 chars; eight anchored hunks; zero removals), `tests/test_dt10_shadow_momentum.js` (new, 315 checks), `tests/js/dt10_edits_v1171.json` + `tests/js/dt10_cash_utc_history.js` (v1.17.1 hunks reversible, v1.17.0 base SHA-256 pinned), version / census pins moved in nine harnesses, `scripts/score_selection_log.py` (1.2.0 -> 1.2.1; zero function or constant removals), `tests/test_board_scorecard_p182.py` (P15 + pins), `.github/workflows/ci.yml` 1.1.44, this sheet.

## S1 - WHY (measured 27 Oct, first shadow day)
- Render, every opportunity call after the arming deploy (`f60cff37`, 09:13 Riyadh): `[RANK-CHALLENGER v1.28.0] mode=shadow pool=26 below_cap=24 at_cap=2 below_floor=0 overlap_top5=0 momentum=0/26 ...` - the challenger never received a momentum value.
- `_Shadow_Rank_Log`, first event 10:48:41 Riyadh (30 rows: 10 CHAMPION, 10 CHALLENGER, 10 BOARD): the Momentum column is blank on every row, and the challenger top 10 is the champion's ranks 26 -> 17 in order - smaller engine ROI first inside one reliability band (Rel 75-77). That is not the order the promotion rule was registered for (floor -> reliability band -> **sector-relative momentum** -> target revision -> smaller ROI).
- Cause: the pool contract `DT10_POOL_FIELDS` never sent `Momentum Score`; opportunity_builder v1.28.0 reads `momentum_score` (alias `momentumscore`) from the pool row. The column is filled on the pages: Global_Markets 6,605 of 6,611 rows (column BK, 0.19-88.09, mean 39.6); for the INVEST names e.g. OPBK 57.03, LIEN 21.58, AER 69.04. Same defect class as v1.6.1 / v1.8.1 / v1.11.12 - an input starved by the pool contract.

## S2 - WHAT
`16_Decision_Top10.gs` v1.17.1
- `var DT10_SHADOW_POOL_FIELDS = [{ send: 'Momentum Score', match: ['momentumscore'] }]`.
- `dt10PoolFieldsWithShadow_(fields, armed)` (PURE): `fields` itself when not armed (the same array object), else a copy plus the shadow fields (a send already present is not added twice); never mutates its input.
- `dt10PoolFieldsActive_()` (memoized, no argument - the projection every header map and pool row uses): `dt10PoolFieldsWithShadow_(<v1.17.0 list>, DT10_SHADOW_RANK_LOG is on/all)`. The pure `dt10PoolFieldsActive_(true|false)` forms are unchanged.
- Off (property absent, blank, junk, `off`, or a service fault): the v1.17.0 list object -> header maps, pool rows and the POST body byte for byte. On: each pool row gains `Momentum Score` (the sheet's number, as is); blank / placeholder cells send nothing; a page without the column sends nothing.
- `dt10SelfTest`: `property memo core` counts the armed field; new `shadow pool fields core: ok (Momentum Score sent only while the shadow log is armed) | sent now: yes|no`.
- Backend: no change. Under `TFB_OPP_RANK_MODE=legacy` the builder never reads the field; under `shadow` only the challenger's second key reads it. Live order, gates, verdicts, sizing and funding are unchanged either way.

`score_selection_log.py` v1.2.1 (inside `TFB_BOARD_SCORECARD_CHALLENGER=on`, already armed)
- `shadow_days_without_inputs(values)`: the logged UTC days whose FIRST event carries no Momentum value on any row (CHAMPION, CHALLENGER or BOARD). A grid without the column, a missing tab or an empty grid -> none.
- `shadow_events(values, skip_days=...)` drops those days for both orders (paired days stay paired); `challenger_verdict(..., excluded=n)` / `challenger_compare(..., excluded=n)` name them in the reason (`n logged day(s) excluded - no momentum input`; NO_DATA then reads `no trading day with the challenger's inputs in _Shadow_Rank_Log`); stdout prints the days; the details JSON gains `excluded_days`. With no day excluded every v1.2.0 output is byte for byte (P15).
- Amendment to the program doc section 4 made before any +5 outcome existed: a day counts only when the challenger ran with its pre-registered inputs.

## S3 - Gate / arming
| Item | Before | After | Effect |
|---|---|---|---|
| `DT10_SHADOW_RANK_LOG` (Script Property, already `on`) | log only | log + `Momentum Score` on the pool row | the challenger gets its second key on the next render after the paste |
| `TFB_BOARD_SCORECARD_CHALLENGER` (workflow, already `on`) | counts every logged day | counts days with the challenger's inputs | 27 Oct is excluded (and printed) |

No new property, variable or Render change.

## S4 - Tests
- `node tests/test_dt10_shadow_momentum.js` -> **ALL PASS (315 checks)**: M1 kill identity (absent / off / blank / junk / `1` / `onn` / service fault / W52 legacy flag: the v1.17.0 list object, header maps and POST rows byte-identical to the base); M2 armed (`on`, `all`, ` On `, `ALL`: Momentum Score appended last, mapped to its column, the sheet's number sent as is incl. 0, blank / em dash / `n/a` -> no key, every other key equal to the base row, a page without the column byte-identical; legacy flag + armed); M3 golden negative (the rebuilt v1.17.0, armed, never sends it - the 27 Oct `momentum=0/26`); M4 memo (one projection object and one read of each property per execution; a reset reads again); M5 pure helper (identity, no mutation, no duplicate); M6 the builder alias `momentum_score <- momentumscore` read from `core/analysis/opportunity_builder.py`, attached only off legacy; M7 the two self-test lines off / on / all and the projection untouched; M8 version lockstep, the WHY newest-first and ASCII, the v1.17.0 WHY verbatim, ES5/ASCII lint of the added lines, census 217 -> 218 functions and 116 -> 117 vars with exactly the designed names, eight hunks whose reversal is SHA-256 `cefeedeb...`.
- Every native harness on the delivered file: `test_dt10_shadow_rank_log` 86 (S9 now checks the rebuilt v1.17.0 file, SHA-pinned), `test_dt10_confirm_basis` 8/8 x3, `test_dt10_pending_grace` 8/8 x3, `test_dt10_funding_evidence` 103, `decision_input_truth` 20 + 3 skips (self-test now requires `shadow pool fields core: ok`), `dt10_feed_cohort_binding` 21/21, `dt10_bulk_format` 9/9, `gas_ledger_fx_failed_state` 11/11, `decision_source_readiness`, `decision_board_funding`, `decision_chain` 28/28, `portfolio_reconciliation_inputs`, `apps_script_syntax`, `dt10_calendar_completeness` 25/25, `p142`, `p145`, `cash_source`. Python tests that read the GAS file: 170 passed, 10 skipped.
- `python scripts/score_selection_log.py --selftest` -> **SELFTEST 25/25** (#25 challenger inputs). `tests/test_board_scorecard_p182.py` -> **149 PASS / 0 FAIL**: P15 a momentum-less first day -> 24 days / 120 episodes per order re-derived independently, the verdict reason and the stdout line name the day; one Momentum value on any row (a BOARD row) keeps the day; the live write path equals the TSV path, `_Run_Log` `| challenger: PROMOTE_ELIGIBLE (days 24, scored 120/120)`, details `excluded_days`; no exclusion -> no key; every day blank -> NO_DATA that says why; golden negative: the untouched v1.2.0 (`f60cff37`) counts the day (25 / 125); with no day excluded the P / C / U / K / I variants' stdout and three artifacts equal v1.2.0 byte for byte; the v1.2.0 base self-test is 24/24.
- Workflow / CI regressions: 310 passed, 4 skipped; `scripts/audit_repository_workflows.py` errors 0, warnings 0.
- Delivered SHA-256 `9541f4ec39224dd06fa85353e7dd6714ea5d054f5805251078a5205156a7d5a1`.

## S5 - Rollback
- GAS: `DT10_SHADOW_RANK_LOG` off stops the send (and the log) with no re-paste; or re-paste v1.17.0 (`cefeedeb...`).
- Scorecard: revert to v1.2.0 (the excluded day counts again).

## S6 - Known limits
- 27 Oct stays in `_Shadow_Rank_Log` as logged (nothing is rewritten); it is excluded from the comparison and printed as such.
- `Momentum Score` is the engine's short-horizon blend (RSI, 1D/5D change, 52W position, volume ratio) as the pages carry it; its quality is the engine's (6 of 6,611 Global_Markets rows blank on 27 Oct).
- The target-revision key stays inert until register item A8 publishes a revision field.

## S7 - Operator steps
1. Merge.
2. Save `apps_script/16_Decision_Top10.gs` v1.17.1 in the bound project (Claude stages it in the editor with the SHA-256 checked; the Save click is the owner's), then run Top_10_Investments -> Self-Test: expect `[DT10 v1.17.1] self-test`, `shadow pool fields core: ok ... | sent now: yes`, zero FAIL.
3. Before 03:00 Riyadh so the first render of the next UTC day carries momentum. Verify: Render log `momentum=N/26` with N > 0, and the Momentum column filled in that day's `_Shadow_Rank_Log` rows.

Not a licensed financial adviser; the owner decides and executes every trade.
