# Commit Sheet - `apps_script/27_Decision_Pages_v2.gs` v1.4.5 [DIV-NET] - 2026-11-17

**Scope:** owner, 17 Nov 10:33 Riyadh, asked whether the entry should count a dividend net of the tax withheld: "go with the best option". The Exit Plan does so from `exit_plan` 1.4.0 (PR #1013). This sheet brings the exit engine cell of Portfolio_Decision_v2 to the same rule and the same record.

**Files:**
- `apps_script/27_Decision_Pages_v2.gs` 1.4.4 -> 1.4.5: 8 hunks, +6 functions, 0 removed; every added line ES5 and ASCII. SHA-256 `04b9f513...`.
- `tests/test_dp2_v1450_div_net.js` (new; S0-S5 x3, `PASS 6/6`, 14 mutants) and `tests/test_dp2_v1450_div_net.py` (pytest wrapper).
- `tests/js/dp2_edits_v1450.json` (new, the 8 hunks); `tests/js/dp2_history.js` gains `v1440Source` (SHA-256 `988c1f53...` pinned) and the older chain passes through it.
- `tests/test_dp2_v1440_three_cells.js` reads the rebuilt v1.4.4 file.
- This sheet.

## S0 - Evidence (17 Nov 2026)
- The exit engine cell lowers a lot's entry by the whole dividend, as the price history does on the ex-date.
- A Saudi resident does not receive the whole dividend of a US company: tax is withheld. After a large special dividend the cell counted the gain and the break-even from a price the owner's cash never reached, while the name cell (1.4.4) showed the ledger's real return beside it.
- The portfolio ledger's dividend log (`_Dividend_Log`) is the record: a dividend is a positive Amount, the tax recorded for it a negative Amount on the same symbol and ex-date.

## S1 - WHAT
1. `dp2DivTaxFromValues_` (pure): the log values -> the kept share per symbol and ex-date (sum of all amounts over the sum of the positive ones). Listed only from 50% up to 99.5%: no tax row, a typing error or a reversal lists nothing.
2. `dp2DivTax_`: reads the tab once per Portfolio_Decision_v2 build; the gate off, an absent tab or any failure gives nothing.
3. `dp2ExitDivsNet_` (pure): scales a dividend only when the lot was paid it (ex-date after the purchase) and the log lists a kept share for an ex-date within `DP2_EXIT_DIV_NEAR_D` days. Nothing to scale returns the input itself.
4. The engine cell reads `dividend-adjusted (<gross> ex <date>, net of <p>% tax (dividend log))` (`dp2ExitDivTxt_`, pure).
5. Script Property `DP2_DIV_NET`: `0` / `off` / `false` / `no` = the 1.4.4 cells; anything else, absent or unreadable = on.
6. `dp2SelfTest` gains one line (`v1.4.5 core ...`).

Invented example: a lot bought at 50 is paid 10 and 30% is withheld. The whole dividend puts the entry at 40; the cash basis is 43. On the harness shape the 1.4.4 cell read "+3.0R at the high ... BE locked"; the 1.4.5 cell reads "+0.8R at the high".

## S2 - What does not change
- With `DP2_DIV_NET` = off, with no dividend log, with a log that holds no tax row, or with a tax row of a symbol that is not held, every cell of both pages is the 1.4.4 one (harness S1).
- With a tax row, only the stop, engine and signal cells of that holding change on Portfolio_Decision_v2 (harness S2). The Top 10 page does not read the log.
- A dividend with no tax row stays gross: nothing is guessed from the listing.
- Analysis only, as before: the FINAL line follows the Today Action Plan. No gate, ticket, sizing, order or broker call reads any of it.

## S3 - Tests (invented figures only)
- `node tests/test_dp2_v1450_div_net.js` x3: digest `a79e3f6810e6e119` each time, **`PASS 6/6`**, 14 mutants caught.
- `node tests/test_dp2_v1440_three_cells.js`: digest `f9f32d75eccf92a0`, `PASS 7/7` on the rebuilt v1.4.4 file (the same digest as before this change).
- Every Node harness that reads the script or the history helper passes (the older ones on rebuilt files).
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S4 - Deploy and rollback
- **Deploy:** paste `apps_script/27_Decision_Pages_v2.gs` (1.4.5). No property is needed.
- **Verify:** `dp2SelfTest` shows `PASS - v1.4.5 core: a dividend net of the tax recorded for it (DP2_DIV_NET: on)`.
- **Rollback:** `DP2_DIV_NET` = `off`.
