# TFB Commit Sheet — B3 (cockpit half): hidden 90-day price sparkline column

**Date:** 2026-10-17 · **Plan item:** Decision Pages v2 Execution Plan §2 B3 (Codex) · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `c0efbfa`

## 1. Scope and what is in the repo

The plan's B3 names three files. Only one of them is repo-managed; the other two are not in `apps_script/` and must be patched in the IDE by whoever holds them:

| B3 part | File | In repo? | Status |
|---|---|---|---|
| sparkline per holding | `apps_script/10_My_Portfolio.gs` | yes | **delivered here** (v1.5.15 → v1.5.16) |
| number-format map (integers `#,##0`, money `#,##0`, percents `0.0"%"`, dates `yyyy-mm-dd`) | `04_Format.gs` v2.19.0 | no | not deliverable from this branch; the anchors can only be read in the IDE |
| `_Data_Audit` heading must not start with `=` | `99_Data_Audit.gs` L1313 | no | same |

The plan also asked for a "backend-history fallback for LSE/HK". `routes/` carries no price-history endpoint (grep `history|candles|ohlc` returns nothing), and GOOGLEFINANCE serves `LON:` and `HKG:` tickers directly, so the fallback is not needed: the mapper sends `HSBA.L → LON:HSBA` and `0005.HK → HKG:0005`.

## 2. Base pin

| Item | Value |
|---|---|
| File | `apps_script/10_My_Portfolio.gs` |
| Before | v1.5.15 · sha256 `344103f69eb8…` (branch `c0efbfa`) |
| After | **v1.5.16** · 4,828 lines · sha256 `bb186c2a2b92…` |
| Gate | Script Property `PD_SPARKLINE = on` (default off → byte-identical render; golden `pf_ibkr_evidence` 14/14 unchanged) |

## 3. What changes (property on)

- ACTIONS table gains one more column `Spark 90d` after Advisor Note and, when `PD_SWITCH_ADVISORY_COL=on`, after `Switch (adv)`.
- Each cell is `=IFERROR(SPARKLINE(GOOGLEFINANCE("<ticker>","close",TODAY()-91,TODAY()),{"charttype","line";"linewidth",1;"color","#2a78d6"}),"")`, written through `setValues` (a string starting with `=` is a formula in Apps Script).
- The column is **hidden** (`sheet.hideColumns`) so today's layout is untouched; the v2 renderer (plan A3) unhides it where it wants the chart.
- `PD_ACTION_HEADERS` is untouched, so `_Portfolio_Action_Log` keeps its 26 → 32 width contract.
- Ticker mapping (`pdSparklineTicker_`, pure): `.US` → bare, `.L` → `LON:`, `.HK` → `HKG:`, `.SR` → `TADAWUL:`, `.T` → `TYO:`, `.PA` → `EPA:`, `.IS` → `IST:`, `.SI` → `SGX:` and 16 more in `PD_SPARK_EXCHANGES`; bare symbols pass through; an unknown suffix (`BRK.B`), blank or unsafe token yields an empty cell, never a wrong instrument.

## 4. Protocol evidence

- **Anchored edits:** 6 (`scratchpad/plan_v2/apply_b3.py`), every anchor `count == 1`; one comment line reworded afterwards for the ES5 guard (`...` in a comment).
- **Zero-removal proof:** removed lines = `Version:` header line, `PD_VERSION`, and the C4 wrap line `sheet.getRange(aT.firstDataRow, aHeaders.length, …)` → `aHeaders.length - (spCol ? 1 : 0)` (identical when the property is off). Functions added 3 (`pdSparklineColOn_`, `pdSparklineTicker_`, `pdSparklineFormula_`), vars 5; removed 0.
- **ES5:** every added line passes the guard (harness B4; `pf_uncertified_context` V2 over `git diff HEAD`).
- **Harness:** `node tests/js/pf_sparkline_col.test.js` 6/6; `pf_switch_advisory_col` 4/4 and `pf_uncertified_context` 7/7 (version pins moved to 1.5.16); `pf_ibkr_evidence` 14/14 golden; `pf_source_receipt` 8/8; `apps_script_syntax` 4 compiled; `ci.yml` JS step += `pf_sparkline_col`.
- **No ENV, Script Property or cell changed by this commit.**

## 5. Read-back after paste (Emad)

1. Paste v1.5.16 (or the later v1.5.17 that also carries B2), run `pdSelfTest` → line `sparkline col (PD_SPARKLINE): off (default) | core: ok`.
2. Set Script Property `PD_SPARKLINE = on`, run Refresh Portfolio Decision: the ACTIONS header row ends with a hidden column `Spark 90d`; unhide it once to see the 90-day lines (US, LON, HKG, TADAWUL tickers render; any blank cell means the symbol's suffix is not mapped and is listed in the status of the pure mapper).
3. Rollback: set the property to `off` (next refresh writes no formula and the hidden column stays empty) or re-paste v1.5.15.
