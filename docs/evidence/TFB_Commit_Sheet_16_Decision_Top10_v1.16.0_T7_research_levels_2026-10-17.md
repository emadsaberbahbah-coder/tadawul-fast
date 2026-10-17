# TFB Commit Sheet — 16_Decision_Top10 v1.16.0: research levels when sizing is withheld (T7)

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §3 T7 (P1) · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `fb918c6`

## 1. Problem and delivery

On a withheld board every level reads "—": `dt10FinalizeBoard_` blanks entry/stop/TP1/TP2 on every non-executable seat and the W1A-4a withhold pass blanks the rendered row again, so the operator cannot review the plan he is asked to trust next week.

Gate **`DT10_RESEARCH_LEVELS = on`** (Script Property; default off → page byte-identical to v1.15.0):

| Step | Function | What |
|---|---|---|
| capture | `dt10ResearchLevelsCapture_` | right before the refresh's finalize, snapshots entry zone / stop / TP1 / TP2 / R/R / price on every seat that still carries a plan; holds (grace, fast-track, P-145, earnings, seat-cap) are never captured (no plan today by doctrine) |
| decide | `dt10ResearchLevelsCtx_` | rows to show = captured, unsized seats, **only when the board OUTPUT is not EXECUTABLE** (WITHHELD / HELD / QUALIFIED_UNFUNDED); reference size = 5 % of `meta.cash_floor.nav_sar` (NAV) or, with the cash floor unarmed, 5 % of `kpis.deployable_sar` labelled "NAV unknown" |
| render | `dt10ResearchLevelsRow_` | after the withhold pass, restores Entry Zone, Stop, TP1, TP2 on the rendered row; Ticket SAR, Shares, Gain 12M, Funds From stay "—"; Advisor Note opens with `RESEARCH LEVELS — NOT EXECUTABLE — reference size only [R/R(TP2) 2.5; ref 5% = 1,160 SAR (11 sh) -> at risk 110 SAR (10%)]` followed by the withholding note |
| watermark | title + `dt10ResearchLevelsPaint_` | SELECTED title gains ` \| RESEARCH LEVELS xN (NOT EXECUTABLE — reference size only, 5%-NAV ref 1,160 SAR)`; the four level cells are painted italic grey |

The view is applied to the **rendered row arrays only**: payload tickets, KPIs, the withheld-truth KPI pass and `_Selection_Log` rows are untouched (R5 asserts the ticket JSON before/after the view).

## 2. Acceptance

- **Byte-identical page when off:** R5 runs the same board with the property absent → ctx rows empty, rendered rows deep-equal to the baseline; `dt10SelTitle` suffix absent; paint skipped.
- **EXECUTABLE days unchanged when on:** `dt10ResearchLevelsCtx_` returns no rows when `dt10OutputStatus_(payload) === 'EXECUTABLE'` (R3, R5).
- **Withheld board:** R4/R5 show the levels back at board indexes 9/12/13/14, Ticket SAR/Shares/Gain/Funds From still withheld, R/R and at-risk at the 5 %-NAV reference (NAV 23,200 → 1,160 SAR → 11 shares at 100 SAR with a 90 stop → 110 SAR at risk, 10 %).

## 3. Decisions (flag for Emad)

- **NAV source.** The cockpit payload has no NAV field; `meta.cash_floor.nav_sar` exists only when `TFB_T10_CASH_FLOOR_PCT` is armed (observe/enforce). Until then the reference falls back to 5 % of deployable and says "NAV unknown". Arming the cash-floor observe is the clean fix (ENV, §8-class).
- **Scope = board output ≠ EXECUTABLE.** A research seat on an EXECUTABLE board (unfunded by budget) keeps the v1.15.0 dashes, so executable days are byte-identical as the brief requires.
- **Watermark in a grid** = title suffix + note prefix + italic-grey level cells (Sheets has no overlay).
- Mirror of `TFB_PF_RESEARCH_VIEW` on the portfolio side is C4's read-back harness, not this commit.

## 4. Protocol evidence

- **Anchored edits:** 9 (`scratchpad/edit_t7_gas.py`), every anchor `count == 1`; reversal layer `tests/js/dt10_v1160_history.js` generated from the same list; chain v1.16.0 → v1.15.0 → v1.14.0 → v1.13.9 → pinned v1.13.8 digest (V2).
- **Zero-removal proof:** removed lines = the header `Version:` line and `DT10_VERSION` (both rewritten); every other edit is a pure insertion in front of an existing anchor. Census 205 → 212 functions (7 added, 0 removed), vars 105 (unchanged).
- **ES5:** every added line passes the V3 guard.
- **Dark by default:** Script Property absent → off; no ENV, Script Property or cell changed by this PR.
- **Harness:** `node tests/js/dt10_v1160_research_levels.test.js` 9/9; the eight other GAS harnesses pass (v1150/v1140 harnesses now run on the reverted source; version/census/self-test pins moved to 1.16.0 / 212 / 105); `dt10SelfTest` gains `research levels core: ok | DT10_RESEARCH_LEVELS=off (default)`; `ci.yml` JS step += the new harness.

## 5. Read-back after the paste (Claude/Emad)

1. Paste v1.16.0 (after v1.15.0); `dt10SelfTest` → `research levels core: ok | DT10_RESEARCH_LEVELS=off (default)`; the page renders exactly as v1.15.0.
2. Set `DT10_RESEARCH_LEVELS = on` after one observe cycle (§8): on the next withheld/held board the SELECTED title carries `RESEARCH LEVELS xN`, the level columns fill in italic grey, Ticket SAR / Shares / Funds From stay "—", `_Selection_Log` rows are unchanged from the off state.
