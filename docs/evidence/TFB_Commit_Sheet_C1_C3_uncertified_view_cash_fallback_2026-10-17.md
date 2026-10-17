# TFB Commit Sheet — C1 / C3: HOLD-uncertified, not BLOCK-with-zeros; ledger cash fallback

**Date:** 2026-10-17 · **Brief items:** Codex Brief v2 §4 C1 (P1) + C3 · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `ed9792c`

## 1. Problem and delivery

17-Oct 09:37 Riyadh: the only IBKR capture was 24 h 57 m old (limit 14 m) → evidence NOT SENT → Portfolio_Decision rendered BLOCK ×3 "RECONCILIATION_REQUIRED: missing_evidence", panel cash 0.0, every KPI 0, "No valued sectors". The ledger still knew MV, weights and P&L. 16-Oct with a 2-minute capture: 3/3 HOLD, certified cash 53,769.26.

| Half | Gate | What |
|---|---|---|
| **Backend** `core/analysis/portfolio_actions.py` v1.16.0 → **v1.17.0** | `TFB_PF_UNCERTIFIED_VIEW = on` (Render ENV, default off) | `_uncertified_view_portfolio` on the not-funding-eligible branch (supersedes the research view when both are set). Per row the research-view tier applies: matched / custody-not-proven rows show MV, cost, P&L, weight (MV ÷ (holdings + cash)), sector weight and the stop/TP ladder and carry the **rule verdict as the action**: HOLD / TRIM / EXIT read as the action with "(uncertified)" in the reason; ADD → HOLD (new money withheld, `capped_from ADD`); BLOCK stays BLOCK; contradicting evidence keeps the legacy blank BLOCK row. Sizing withheld on every row: Δ SAR 0, Δ shares 0, proceeds 0, funds_from None, `detail.execution_ready False`, `detail.uncertified True`, `detail.uncertified_view {tier, verdict, fx_rejected, capture_age_min}`. v1.16.0 scrubs unchanged (cost-basis BLOCK → cost/P&L None; rejected holding FX → seven SAR fields blank). **C3:** cash = panel cell when > 0, else `body.cash_snapshot.balance_sar` (the `_Cash_Snapshot` ledger balance), tagged `(uncertified)` in `meta.uncertified_view.cash_basis`, never certified: deployable / adds_funded / unallocated stay 0. KPIs (F3/F7): holdings, portfolio value, cash, cash %, cost, P&L, action_counts by shown action; `sector_summary` rebuilt. Capture age from `body.capture_context` on every reason ("capture 1497m old, limit 14m"); absent → "capture age unknown". Any exception → off output + `meta.uncertified_view.status = error`. |
| **Route** `routes/advanced_analysis.py` | — | `capture_context` and `cash_snapshot` forwarded from the body into `upstream_meta` (popped by the builder before `meta.upstream` is written). |
| **Cockpit** `apps_script/10_My_Portfolio.gs` v1.5.13 → **v1.5.14** | Script Property `PD_UNCERTIFIED_CONTEXT = on` (default off) | In enforce mode, when the capture is NOT fresh: `body.capture_context` (pure `pdUncertifiedContext_`: captured_at, age_min, limit_min 14, reason) and `body.cash_snapshot` from the `_Cash_Snapshot` sheet's last record (pure `pdCashSnapshotParse_`: 'Balance SAR' header scan, date column; `pdCashSnapshotRead_` never throws). ACTIONS table renders `detail.uncertified` rows as "HOLD (uncertified)" (`pdActionLabel_`). Status line gains ` | uncertified ctx sent (age 1497m; cash snapshot 53769.26 SAR)`. `pdSelfTest` gains the property state + `core: ok`. |

## 2. Acceptance

- **17-Oct fixture** (`tests/test_pf_uncertified_view_c1.py::test_17oct_rows_read_hold_uncertified_with_mv_weights_and_zero_deltas`): three US rows (ledger MV 11,858 / 9,664 / 7,942 SAR), no packet, context age 1497 m, panel cash 0, snapshot 53,769.26 → **3 × HOLD**, MV and weights populated, Δ shares 0, KPIs populated (cash 53,769; portfolio = holdings + cash), sector summary 3 rows, alert `portfolio_inputs_unverified` kept, `cash_basis = "_Cash_Snapshot 2026-10-16 (uncertified)"`.
- **16-Oct fixture** (`test_16oct_fresh_packet_is_the_certified_build_regardless_of_the_gate`): fresh packet → the certified build, byte-identical with the gate on or off (the view never runs when funding is eligible).
- **Off:** the v1.16.0 dispatch verbatim; the view function is never entered (`test_off_is_the_v1160_dispatch_and_never_enters_the_view`).
- **Cockpit off:** `tests/js/pf_ibkr_evidence.test.js` 14/14 — the off-mode golden (request body, render, status line) stands byte-identical.

## 3. Decisions (flag for Emad)

- **Action = the rule verdict, labelled uncertified.** TRIM/EXIT by rule are shown as the action with Δ 0 (the brief: "the rule-based legs … with an UNCERTIFIED tag"); ADD is never named (HOLD + "new-money verdict withheld").
- **Weights are display weights** (MV ÷ (holdings + uncertified cash)); `decide_action` still runs with weights None (no cap trim on an unverified denominator, the F3 convention).
- **No new alert:** the `portfolio_inputs_unverified` alert is kept as the brief asks.
- **Two gates, two lanes:** the Render ENV arms the view; the Script Property arms the context. Either alone is inert (ENV on without context → "capture age unknown", cash basis panel/none; property on without ENV → the backend pops and ignores the fields).
- **USD rows and the certified path:** the fresh-packet acceptance runs on SAR rows because the certified path needs an FX proof whose packet schema this fixture does not fabricate; the uncertified view itself is exercised on USD rows at the 3.75 peg.

## 4. Protocol evidence

- **Anchored edits:** backend 4 (`scratchpad/edit_c1_pa.py`), route 1, GAS 8 (`scratchpad/edit_c1_gas.py`); every anchor `count == 1`.
- **Zero-removal proof:** backend removed lines = the header `Version:` line and `PORTFOLIO_ACTIONS_VERSION` (rewritten); GAS removed lines = `Version:`, `PD_VERSION` and the first cell of `pdActionToRow_` (`pdCell_(a.action)` → `pdActionLabel_(a)`, which returns exactly `pdCell_(a.action)` unless the backend set `detail.uncertified`). No function, branch or message removed. Functions added: backend 3, GAS 5.
- **ES5:** every added GAS line passes the guard (`pf_uncertified_context.test.js` V2 over `git diff HEAD`).
- **Dark by default:** both gates default off; no ENV, Script Property or cell changed by this PR.
- **Harness:** pytest `tests/test_pf_uncertified_view_c1.py` 18/18; `test_pf_research_view_p06`, `test_pf_ibkr_evidence`, `test_portfolio_actions`, `test_pf_source_receipt`, `test_portfolio_action_reconciliation`, `test_core_repair_readback` → 100 passed; `node tests/js/pf_uncertified_context.test.js` 7/7; `pf_ibkr_evidence` 14/14; `apps_script_syntax` 4 compiled; ruff clean; `ci.yml` lean list and JS step updated.

## 5. Arming order (§8, Emad)

1. Deploy the backend; paste 10_My_Portfolio v1.5.14; `pdSelfTest` → `uncertified ctx (PD_UNCERTIFIED_CONTEXT): off (default) | core: ok`.
2. Set `TFB_PF_UNCERTIFIED_VIEW=on` on Render **and** `PD_UNCERTIFIED_CONTEXT=on` in Script Properties after one observe cycle of the page.
3. Read-back on the next stale-capture morning: rows "HOLD (uncertified)" with MV/weights, Δ Shares 0, KPIs populated, status line `… | ibkr evidence NOT SENT: … | uncertified ctx sent (age Nm; cash snapshot X SAR)`; a fresh-capture morning unchanged.
