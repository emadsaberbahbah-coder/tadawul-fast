# Commit Sheet - `apps_script/27_Decision_Pages_v2.gs` v1.4.6 [RETURN-COL] - 2026-11-19

**Scope:** owner, Thu 19 Nov 10:04 Riyadh: "still in some pages I see that SUPN.US showing high loss but the impact of the dividends received is not effective ... check and solve that now and in the future as well". The holdings table of Portfolio_Decision_v2.

**Files:**
- `apps_script/27_Decision_Pages_v2.gs` 1.4.5 -> 1.4.6: 6 hunks, +5 functions, 0 removed; every added line ES5 and ASCII.
- `tests/test_dp2_v1460_return_col.js` (new; R0-R4 x3, `PASS 5/5`, 8 mutants) and `tests/test_dp2_v1460_return_col.py` (pytest wrapper).
- `tests/js/dp2_edits_v1460.json` (new, the 6 hunks); `tests/js/dp2_history.js` gains `v1450Source` (SHA-256 `dc15d4c4...` pinned) and the older chain passes through it.
- `tests/test_dp2_v1450_div_net.js` reads the rebuilt v1.4.5 file.
- This sheet.

## S0 - Evidence (19 Nov 2026, the live page)
- The SUPN.US row's 'Unreal.' cell read -18.3%, the price change since the purchase. The lot was paid a special dividend of 8.13 a share on 3 Nov; the owner kept it less the 30% tax.
- The ledger's own figure for the lot (_Portfolio_CostBasis 'Total Return SAR' over the cost) was -2.2%. 1.4.4 printed it only in small text at the end of the name cell.

## S1 - WHAT
1. `dp2LotReturn_` (pure): the return cell is 'Total Return SAR' over the lot's cost in SAR when the ledger lot carries dividends received; otherwise the 1.4.5 price-only figure, the same expression as before.
2. `dp2ReturnNameTxt_` (pure): for such a lot the name cell ends ' . with dividends <amount> <ccy> (ledger) . price only <p>%'; otherwise the 1.4.4 text.
3. The column is headed 'Return'. A fixed-income row already showed its return with coupons in that column.
4. Script Property `DP2_RETURN_WITH_DIV`: `0` / `off` / `false` / `no` = the 1.4.5 table; anything else, absent or unreadable = on.
5. `dp2SelfTest` gains one line (`v1.4.6 core ...`).

Every future dividend, and any tax recorded against it in `_Dividend_Log`, reaches the cell through the ledger with no further change.

## S2 - What does not change
- With `DP2_RETURN_WITH_DIV` = off, both pages are the 1.4.5 build (harness R1, five spellings).
- On, with no dividend lot, only the column heading differs (R1). With a dividend lot, only the heading, that lot's name cell and its return cell differ (R2).
- The Top 10 page does not change. The 'Unrealized' tile, the exit engine, the stops and every decision are untouched; no gate, ticket, sizing, order or broker call reads the cell.

## S3 - Tests (invented figures only)
- `node tests/test_dp2_v1460_return_col.js` x3: digest `638457f875a6e9c0` each time, **`PASS 5/5`**, 8 mutants caught.
- `node tests/test_dp2_v1450_div_net.js`: `PASS 6/6` on the rebuilt v1.4.5 file; `node tests/test_dp2_v1440_three_cells.js`: `PASS 7/7`.
- `pytest tests/test_dp2_*.py`: 15 passed (3 min 30 s).
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S4 - Deploy and rollback
- **Deploy:** paste `apps_script/27_Decision_Pages_v2.gs` (1.4.6). No property is needed.
- **Verify:** `dp2SelfTest` shows `PASS - v1.4.6 core: the return cell counts dividends received (DP2_RETURN_WITH_DIV: on)`; the SUPN.US row's Return cell reads about -2.2%.
- **Rollback:** `DP2_RETURN_WITH_DIV` = `off`.
