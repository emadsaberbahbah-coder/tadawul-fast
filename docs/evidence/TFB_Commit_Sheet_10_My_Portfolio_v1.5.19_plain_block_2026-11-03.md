# Commit Sheet - `apps_script/10_My_Portfolio.gs` v1.5.19 [PLAIN-BLOCK] - 2026-11-03

**Scope:** gaps A2 (ticker changes) and C4 (plain BLOCK reasons) of `claude/TFB_Gap_Analysis_and_Build_Order_2026-11-03.md`. Owner, 3 Nov 12:14 Riyadh: "still my portfolio showing Block".
- **Gate:** a new Script Property `PD_PLAIN_REASONS`, **default OFF**.
- **When off:** absent, blank or any other word, every cell, row, request and line is byte-identical to v1.5.18, apart from one property read per rendered refresh.
- **Display only:** nothing is sent, weighted, funded or actioned differently.

**Files:**
- `apps_script/10_My_Portfolio.gs` 1.5.18 -> 1.5.19: 5,827 -> 6,075 lines; 8 hunks; 4 base lines replaced (the two version lines, the render call and the renderer signature); every added line ASCII.
- `tests/test_pd_v1519_plain_block.js` (new; P0-P8 x2, `PASS 9/9`, mutants 10/10) and `tests/test_pd_v1519_plain_block.py` (pytest wrapper).
- `tests/js/pd_edits_v1519.json` (new, the 8 hunks).
- Pins moved in three harnesses:
  - `tests/test_my_portfolio_v1518_cash_snapshot.js`: the version token is read from the file; the one PD_PLAIN_REASONS read is counted apart; T0 and the T9 lint check the rebuilt v1.5.18 file (SHA-256 `62b15eaa...`).
  - `tests/js/gas_ledger_fx_failed_state.test.js`: version pin.
  - `tests/test_pf_allcash_ok.py`: version pin, the 7 new functions, +248 lines.
- This sheet.

## S0 - Evidence (3 Nov 2026, read-only)
- IBKR moved the INDV shares to the new contract SUPN (Indivior-Supernus merger) while the ledger still said INDV.US.
- `pdIbkrPlan_` linked 4 of 5 rows. B2 read `... linked 4/5; IBKR not on page: SUPN ...`, and every row read `BLOCKED; RECONCILIATION_REQUIRED: custody_unknown, unrepresented_positions`.
- After the ledger rename, the rows read `RECONCILIATION_REQUIRED: holding_quote_unverified`: the price feed still priced the old SUPN.US listing.
- The page never said in words that a ticker had changed or what to do.

## S1 - WHAT
1. **Ticker-change hint.** `pdIbkrDrift_` (pure) runs inside `pdIbkrApply_` on a valid plan.
   - It pairs a page row that links to no IBKR position with a positive position that no row represents, **one to one by an equal share count**. Any second candidate on either side means no pair.
   - The result is stored on the apply result as a **non-enumerable** `drift`: the result's keys and every serialisation stay the v1.5.18 ones, and it is never sent.
2. **Reason codes.** `pdPlainCodes_` (pure) collects the reason codes of the payload's BLOCK rows: Advisor Note `RECONCILIATION_REQUIRED: a, b` plus `detail.reconciliation_status`, with `matched` excluded.
3. **Clauses.** `pdPlainClause_` (pure) maps each code to a plain clause via `PD_PLAIN_TEXT`, which covers the `core/portfolio_reconciliation` statuses and reasons and the `portfolio_actions` certification reasons. `stale_*`, `future_*` and the cash codes have generic clauses.
4. **The line.** `pdPlainLine_` (pure) returns `''` without a BLOCK row; otherwise `WHY BLOCKED (n of m holdings): <clause> (<code>); ... NEXT: <one step>.`. The step follows a fixed priority:
   - a ticker-change pair: `IBKR lists SUPN where My_Portfolio has INDV.US (same 95 shares) -- likely a ticker change (merger or rename): put the new ticker on that row in _Portfolio_CostBasis, then refresh`;
   - then a share-count difference;
   - then an unmatched row or position;
   - then a holding without a current price ("wait for a fresh price ...; funding stays withheld until then");
   - then a missing or stale capture ("refresh within 14 minutes of the next IBKR capture").
5. **Placement.** `pdWritePlainLine_` writes the line merged across the page and wrapped, in bold gold text, between the ACTIONS section title (row 13) and its header row. Everything below moves down one row.
6. `pdSelfTest` gains one line, `plain block reasons (PD_PLAIN_REASONS): on | core: ok`, only while the gate is on. `pdPlainCoreOk_` runs the 3 Nov shapes.

**Reader safety:** the line never contains `action` or `symbol`. `run_daily_brief._find_header` matches those words as substrings; `tfb_acceptance`, `tfb_export_audit` and `21_v_portfolio_decision.sql` match cells. All four still find the real header (P6). A mutant that puts the words in a clause is killed.

## S2 - Tests (node v22, Python 3.13)
- `node tests/test_pd_v1519_plain_block.js`: `run 1/2: 9/9`, digest `21c227acd4a51478` x2, **mutants killed 10/10**, **`PASS 9/9`**.
  - **P1** golden negative: 6 scenarios (ticker-change shape, stale-price shape, observe, IBKR off, a clean board, HTTP 500) x 9 gate values. The REAL `refreshPortfolioDecision` matches the base on B2, the POST body, every write, Logger, `_Run_Log` and triggers. Exactly one PD_PLAIN_REASONS read per rendered refresh (none on an HTTP error). The apply keys equal the base's.
  - **P2** the exact line at row 14. Every other write equals the base with rows from 14 shifted by one; B2, the POST body and Logger are unchanged.
  - **P3** the stale-price line.
  - **P4** no line on a clean board or an HTTP error; with IBKR off the reason line has no ticker hint.
  - **P5** pure pairing: twins, two candidates, different counts, zero shares, an invalid or malformed plan. Also parsing, clauses, the NEXT priority, ASCII, and no header words in any clause or step.
  - **P6** the four readers' header rules.
  - **P7** the self-test.
  - **P8** lint, inventory (7 functions + 2 vars, 0 removed), the four replaced lines, 8 hunks reversing to `62b15eaa...`, and the WHY block.
- Golden negative: the v1.5.18 file run as the delivered file gives `FAIL 0/9`.
- Every portfolio node harness passes on the delivered file: `test_my_portfolio_v1518_cash_snapshot` 10/10 (mutants 5/5), `pf_allcash_ok` 7/7, `pf_ibkr_evidence` 14/14, `pf_source_receipt` 8/8, `gas_ledger_fx_failed_state` 11/11, `test_pd_post_earn_inputs` 62, `test_pd_evidence_publish` 59, `decision_chain` 36/36, `test_por_v100` 51/51.
- `pytest` on the wrappers and readers (`test_pd_v1519_plain_block`, `test_my_portfolio_v1518_cash_snapshot`, `test_bq_views`, `test_pf_add_guards_d1`, `test_pf_allcash_ok`, `test_pf_ibkr_evidence`, `test_pf_source_receipt`, `test_gas_ledger_fx_failed_state`, `test_release_render_settle`): **171 passed, 2 skipped**.
- Delivered SHA-256 `ddc9389b8a1c18c0921c64bf64820c34d9f3d896ff436bf30cf2e1a41ea380a7` (6,075 lines, 263,796 bytes, UTF-16 length 263,602).

## S3 - Operator steps (the repo file is a mirror; nothing changes until the paste)
1. Merge.
2. Paste the full `apps_script/10_My_Portfolio.gs` v1.5.19 into Apps Script and Save.
3. Script Properties: `PD_PLAIN_REASONS` = `on`.
4. Run `pdSelfTest`. Expect `plain block reasons (PD_PLAIN_REASONS): on | core: ok`.
5. On the next BLOCK board, Portfolio_Decision row 14 reads `WHY BLOCKED (...): ... NEXT: ...`.
6. Rollback: delete `PD_PLAIN_REASONS`.

## S4 - Known limits
- The ticker hint needs a valid IBKR capture (observe or enforce). With IBKR off, the line gives the reasons only.
- A rename that also changes the share count (a merger ratio other than 1:1) is not paired. The line then says to make the rows match IBKR.
- The backend reports the stale-price reason as a count, not per holding, so the line cannot name which holding's price is stale. B2 and My_Portfolio's Last Updated column still show it.
- Nothing is renamed automatically: the owner edits `_Portfolio_CostBasis`. Linking holdings by IBKR contract id is the longer-term fix (gap A2, later).

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
