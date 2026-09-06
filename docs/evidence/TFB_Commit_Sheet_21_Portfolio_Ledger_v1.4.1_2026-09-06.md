# Commit sheet — 21_Portfolio_Ledger.gs v1.4.1 (build #4, 2026-09-06)

| Item | Value |
|---|---|
| Target | Apps Script project of the production workbook → file `21_Portfolio_Ledger.gs` (paste the full delivered text over v1.4.0). Repo copy of this sheet: `docs/evidence/TFB_Commit_Sheet_21_Portfolio_Ledger_v1.4.1_2026-09-06.md` |
| Base pinned | v1.4.0 as pasted 2026-09-06 (1,573 lines, blob `69e9c646`); base re-verified under node: all module fixtures pass and today's SHG row reproduces (2,650.3 SAR at 3.8090) |
| New | v1.4.1 — 1,759 lines, blob `598a83c4` (+189 / 3 replaced). Functions 48 → 53 (added `plFxPegOn_`, `plFxGuard_`, `plClearInfoFlag_`, `plRefinalizeCore_`, `plRefinalizeRow`); removed 0 |
| Runtime | ES5 only (lint clean: no let/const/arrow/template/class); smart-quote scan CLEAN; never-throws contract kept |
| Kill-switch | Script property `PL_FX_PEG_GUARD='off'` ⇒ v1.4.0 FX path byte-identically (computed cells N…W identical, proven below). The fee-zero note and `plRefinalizeRow` are additive. |
| ENV / repo | None. No Render, no GitHub Variable, no workflow. |

## Evidence
`_Portfolio_CostBasis` row 35 (SHG.US, closed 2026-09-03 at 84.34) froze with FX→SAR **3.8090** and Realized **2,650**. The frozen rate is the row's own FX cell, written during the Active phase from the `_Lists_Config` USD cell of that moment (1.5 % outside the SAMA band; corrected to 3.7528 by Run 2 on 09-05). Sell Fees = 0, so proceeds are gross (5,903.80) while IBKR realized **+693.32 USD** net of a 2.48 commission. Broker-true realized ≈ **2,602 SAR**; ledger overstated ≈ 48 SAR.

## What changes (anchored edits, count==1 each: 12)
1. `PL_VERSION` 1.4.0 → 1.4.1 + changelog block; header entry points + kill-switch list.
2. Constants: `PL_PROP_FX_PEG_GUARD`, `PL_FX_PEG_BAND {USD [3.74,3.77]}`, `PL_FX_PEG_STATIC {USD 3.75}` (mirror of `opportunity_builder` v1.19.3 `_fx_peg_guard`), `PL_INFO_FEES_FLAG`.
3. `plFxPegOn_()`, `plFxGuard_(ccy, rate, on)` → `{rate, guarded, table}`; never throws.
4. Active branch: table rate guarded; replacement counted per currency.
5. Finalize branch: an out-of-band FX **cell is not reused**; the table rate is guarded too; status names `from→to`.
6. Finalize with Sell Fees = 0 writes an informational `ⓘ` note (the ⚠/⛔ cleaner ignores it; `plClearInfoFlag_` removes it on re-finalize with fees > 0) + status bit `sell fees 0 at finalize: N`.
7. `plRefinalizeRow()` (menu-callable / editor-runnable): validates ONE Inactive row with sell fields, clears FX→SAR + Realized, runs the refresh → the row re-finalizes once with the guarded table rate and current Sell Fees / frozen dividends.
8. `plSelfTest` gains the peg-guard fixture line and the new property in the props line.

Not changed: freeze-once semantics, feeder/prune, dividends, contradiction guard, input normalizer, layout, formats.

## Proofs
| Step | Result |
|---|---|
| Compile (node vm), ES5 lint, smart quotes | OK / CLEAN / CLEAN |
| Base transcription probe (module fixtures + SHG arithmetic) | 10/10 |
| Real-refresh harness ×3 — in-memory workbook, `refreshPortfolioLedger` and `plRefinalizeCore_` executed for real, v1.4.1 vs pinned v1.4.0 | **21/21 PASS ×3**, identical output |

Harness scenarios: (A) table USD 3.8090 → active row guarded to 3.75, MV = 30×187.64×3.75; frozen SHG row untouched; re-finalize row nets fees ((5,903.80−2.48−5,208)×3.75); SGD lot untouched (no band) with `ⓘ` flag; status `fx peg-guard: USD 3.809→3.75 | sell fees 0 at finalize: 1`. (B) table 3.7528 → active 3.7528; stale cell 3.809 rejected → 3.7528, Realized **2,601.89**; exactly one guard entry in the status. (C) `PL_FX_PEG_GUARD=off` → every computed cell N…W identical to v1.4.0 on the same workbook, including the bad 3.809 freeze; status identical apart from the additive fee bit. (D) `plRefinalizeCore_` refuses invalid/Active/pending rows; clears FX + Realized on the frozen SHG row; next refresh re-finalizes at 3.7528 (2,611.40 with fees 0 + `ⓘ`); after Sell Fees 2.48 + re-finalize → 2,601.89 and the `ⓘ` note is removed.

## Operator procedure (after paste)
1. Run `plSelfTest` → report shows `[PL v1.4.1]` and `fx peg guard: OK (USD 3.8090→3.75, 3.7528 kept, SGD untouched, off=pass)`.
2. `_Portfolio_CostBasis` row **35** (SHG.US): enter **Sell Fees = 2.48** (IBKR commission). Optional: row 37 (HCI.US) Buy Price → **191.49** to match the broker average (191.4863).
3. Run `plRefinalizeRow` → prompt: `35` → the refresh re-finalizes SHG. Read-back: FX→SAR **3.7528**, Sell Proceeds **5,901.32**, Realized **≈2,602 SAR**, status line contains `fx peg-guard: USD 3.809→3.7528`, no `ⓘ` note on row 35.
4. Menu wiring for `plRefinalizeRow` belongs to `01_Menu.gs` (separate file); until then run it from the script editor.

## Read-back to log
Row 35 FX 3.7528 / Realized ≈2,602 (was 3.8090 / 2,650); `_Portfolio_CostBasis` status line with the peg-guard bit; `plSelfTest` version line.
