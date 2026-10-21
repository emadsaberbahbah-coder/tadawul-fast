# Top-10 v1.13.11 — bulk render formatting under the 6-minute limit

## What happened on 21 October

The first decision-feed release (run `39672207020`, source cohort `39642923547`) was accepted at 23:41 Riyadh. The Top-10 refresh that followed was the automatic 4-hourly refresh (`tfbAutoRefreshTrigger_`, started 23:44:58). Its execution log:

| Riyadh | Step |
|---|---|
| 23:45:14 | holdings → gate |
| 23:46:19 | POST `/sheet-rows/opportunity-candidates` (9,788 pool rows) |
| 23:47:27 | stability (`7 grace, 2 ft-carried`) |
| 23:48:37 | earnings tags |
| 23:48:42 | `upstream verdict: EXECUTABLE; final board: HELD` |
| **23:50:59** | **`Exceeded maximum execution time`** |

B2 stayed `Last run 23:45:09 | status: running…`. The scheduled release check at 23:55 then failed:
- the strict decision audit read `T10_STATUS_NOT_OK`, and found no source receipt and no pool counts;
- it withdrew the release: `NOT_ACTIONABLE(final_publication) | run=39642923547 | 2026-10-21 23:56:09+03:00 | decision_failed | release=39674377433`.

The board would have been `HELD` anyway (0 executable tickets, 2 fast-track seats unsized, 7 grace-held).

The 19:40 automatic run was killed inside the cockpit the same way. Completed Top-10 runs on 21 Oct took 127–232 s. The executions log shows 46 failed or timed-out executions in 7 days. Many are menu wrappers that time out after the cockpit has already finished.

## Cause addressed here

The render painted formats cell by cell:
- one `setBackground` per zebra row (`dt10WriteTable_`);
- `setBackground` + `setFontColor` + `setFontWeight` per coloured cell (`dt10ColorVerdicts_`, `dt10ColorQualified_`, `dt10ColorStability_`).

The 500-row CANDIDATES grid alone needed about 1,750 range calls, and each call is slower while other scripts write the workbook.

On a cell-model sheet, a full render with 9 seats, 3 qualified, 10 near-miss, 4 alerts and 500 audit rows took:

| Version | Range calls |
|---|---|
| v1.13.10 | 2,023 |
| v1.13.11 | 195 |

`setBackground` calls fell from 850 to 20.

## Change

- **`dt10WriteTable_`:** reads the body backgrounds once, sets the zebra rows and writes them back once.
- **The three colourers:** build one token per row and call the new `dt10BulkPaint_`. It reads backgrounds, font colours and font weights once, sets `tok.bg` / `tok.fg` / `bold` on the token rows and writes each attribute once. Nothing is read or written when no row has a token.
- **Rows the old loops left alone** keep their current format: it is read back and written unchanged.
- **Unchanged:** values, verdicts, withholding, funding, stability clocks, receipts, the status line and the hidden-tab path. The hidden tab benefits through the same `dt10ColorVerdicts_`.
- **Version:** 1.13.10 → 1.13.11. The header and `DT10_VERSION` move in lockstep, and the WHY block is added (ASCII, ES5).
- **Function census:** 194 → 195 (+`dt10BulkPaint_`). Variable census is unchanged at 104.

## Tests

- **New `tests/js/dt10_bulk_format.test.js` (9/9).**
  - *Oracle:* the reviewed v1.13.10 file. It is rebuilt from the delivered source by reversing only the enumerated v1.13.11 edits and must match SHA-256 `914592a3a103815e9d26ecc6b485233623c21cec5382aea68ef5c2b5b1cb32d8`.
  - *Equivalence:* both versions run in separate vm contexts on a cell-model sheet. The final state must be identical: values, backgrounds, font colours, weights, sizes, wrap, styles, number formats, merges and borders.
  - *Cases:* all four helpers across row counts, column counts, token sets and pre-painted cells. The real `dt10RenderPayload_` runs for an EXECUTABLE and a NOT_ACTIONABLE feed over a dirty zone, comparing more than 12,000 painted cells.
  - *Call counts:* bounded, and flat in rows.
  - *Five deliberate mutants:* wrong weight, zebra parity, null-row reset, a lost BLOCK token and a swapped stability token. All five fail the harness; the pristine file passes.
- **`tests/js/dt10_cash_utc_history.js`:**
  - reverses the seven v1.13.11 edits first, then v1.13.10 and v1.13.9;
  - the v1.13.8 digest `9d11832a…` still matches;
  - new export `v11310Source`.
- **Version pins updated:** `decision_input_truth`, `dt10_feed_cohort_binding`, `decision_source_readiness`, `gas_ledger_fx_failed_state`.
- **`decision_input_truth`:**
  - The range stub gains `getBackgrounds`, `getFontColors` and `getFontWeights`, returning GAS defaults.
  - T6 now checks that the hidden-tab verdict colouring is one bulk write of the 500-row column, with every verdict row painted and no per-cell colouring left.
  - T4's "> 1000 calls" proxy becomes "100–400 calls". The footer and grid-header checks still prove the render completed.
- **CI v1.1.30:** runs the new harness in the lean lane's native Apps Script step.

**Local runs (Node 22):**
- All 15 Apps Script harnesses pass: `tests/js/*.test.js` and `tests/test_dt10_*.js`.
- `tests/test_repository_workflow_audit.py`, `tests/test_workflow_audit_repairs.py`, `tests/test_approved_scope_native_binding.py`, `tests/test_daily_brief_blocked_inputs.py` and `tests/test_gas_ledger_fx_failed_state.py`: 68 passed, 3 skipped.
- Strict ES5 parse (acorn, `ecmaVersion: 5`) passes.

Delivered GAS SHA-256: `268b85adafa64b9d8e026646f5227f266541a7fd6a178352fb3d62a3c290b979`.

## Limits (stated)

- **A hard Apps Script timeout still cannot be caught.** v1.13.10's FAILED path covers throws only. This change cuts the render's share of the 360 s budget, but it does not make a killed run impossible.
- **The heaviest path is still the funded one.** It adds a second backend POST (`dt10ReallocateBoard_` allocate replay). Measure it on the first funded run.
- **Not touched:**
  - the pool read (52–65 s), which reads four pages fully;
  - the three source-readiness re-reads;
  - the menu wrappers that keep working after the cockpit finishes (live-only `01_Menu.gs` / `13_AutoRefresh.gs`).

## Bound-project installation and readback

1. Back up the live `16_Decision_Top10.gs` (v1.13.9 live on 21 Oct) from the editor.
2. Replace only `16_Decision_Top10` with this complete file. Verify the editor copy's SHA-256 against the value above, then save. Install `10_My_Portfolio.gs` v1.5.14 from the same main commit in the same session (pending since #810).
3. Run `dt10SelfTest`: version 1.13.11, zero FAIL.
4. Run one `refreshDecisionTop10` outside the `:23–:30` `buildDecisionPagesV2` window. Read the execution log:
   - `done status=ok in N s`;
   - compare the `upstream verdict` → `done` interval with 46–58 s on v1.13.9.
5. Read B2: one `pool_reconciliation_v1` token and one `source readiness VERIFIED (run N)` receipt.

Rollback: re-paste the backed-up file. No property, funding state or evidence changes with the install or the rollback.
