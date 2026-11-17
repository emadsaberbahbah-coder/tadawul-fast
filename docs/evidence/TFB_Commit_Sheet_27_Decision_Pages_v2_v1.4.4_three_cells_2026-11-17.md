# Commit Sheet - `apps_script/27_Decision_Pages_v2.gs` v1.4.4 [PLAN-ACCEPT / TOTAL-RETURN / EVIDENCE-RECENT] - 2026-11-17

**Scope:** owner, 17 Nov 09:44 Riyadh: "be sure that correct information is provided ... avoid misleading and inconsistent recommendation delivery". Three cells of the v2 pages.

**Files:**
- `apps_script/27_Decision_Pages_v2.gs` 1.4.3 -> 1.4.4: 11 hunks, +6 functions, 0 removed; every added line ES5 and ASCII.
- `tests/test_dp2_v1440_three_cells.js` (new; S0-S6 x3, `PASS 7/7`, 11 mutants) and `tests/test_dp2_v1440_three_cells.py` (pytest wrapper).
- `tests/js/dp2_edits_v1440.json` (new, the 11 hunks); `tests/js/dp2_history.js` gains `v1430Source` (SHA-256 `58a3abca...` pinned) and the older chain passes through it.
- `tests/test_dp2_v1430_stop_of_record.js` reads the rebuilt v1.4.3 file.
- This sheet.

## S0 - Evidence (17 Nov 2026, read-only)
- One holding's row read `FINAL: HOLD` and the Today Action Plan asked for nothing, while the `Dyn stop` cell told the owner to raise a broker stop that sat a few cents under the stop of record. The plan accepts a broker stop inside its own tolerance; this page compared to half a cent.
- The same row showed the lot's price loss in `Unreal.` although the lot had been paid a large cash dividend that the ledger records.
- The Model evidence tile is an all-time average. It fell every day because the records maturing now score below the average, and the tile could not show that.

## S1 - WHAT
1. **Plan accept.** `dp2FinalHoldsStop_` (pure): the final row is a HOLD whose words hold no `raise`, `place` or `verify`. For such a row, a working broker stop under the stop of record reads `broker <x> - accepted by the plan (<p>% under the stop of record)`. A raise, place or verify row, a stop on part of the position and a missing stop read as in 1.4.3.
2. **Total return.** `dp2TotalReturnTxt_` (pure): a lot whose ledger row carries `Dividends Recv` adds ` . total return <p>% with dividends <amount> <ccy> (ledger)` to its name cell: the ledger's `Total Return SAR` over the lot's cost. `Unreal.` is unchanged.
3. **Evidence recent.** The tile adds ` . targets of the last <N> days: <rate> (n <records>)` for the INVESTABLE records whose `Target Date (Riyadh)` lies in the window. Script Property `DP2_EVIDENCE_RECENT_DAYS`: absent or unreadable = 30; 1 to 365; `0` / `off` / `false` / `no` = the 1.4.3 tile. One property read per execution.
4. `dp2SelfTest` gains one line (`v1.4.4 cores ...`).

## S2 - What does not change
- With `DP2_EVIDENCE_RECENT_DAYS` = off, no lot with dividends and no broker stop under the stop of record, every cell of both pages is the 1.4.3 one (harness S1).
- No gate, ticket, sizing, alert, order or broker call reads any of it.

## S3 - Tests (invented figures only)
- `node tests/test_dp2_v1440_three_cells.js` x3: digest `f9f32d75eccf92a0` each time, **`PASS 7/7`**, 11 mutants caught.
- Every Node harness that reads the script or the history helper: all pass (the older ones on rebuilt files).
- `pytest` on the v2 wrappers: 80 passed.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S4 - Deploy and rollback
- **Deploy:** paste `apps_script/27_Decision_Pages_v2.gs` (1.4.4). No property is needed.
- **Verify:** `dp2SelfTest` shows `PASS - v1.4.4 cores: ... (DP2_EVIDENCE_RECENT_DAYS: 30 d)`.
- **Rollback:** `DP2_EVIDENCE_RECENT_DAYS` = `off` for the tile; re-paste v1.4.3 for the rest.
