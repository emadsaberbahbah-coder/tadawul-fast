# TFB Commit Sheet — C4: surface the switch scan as an advisory column

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §4 C4 · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `08e4218`

## 1. Problem and delivery

`TFB_PF_SWITCH_SCAN=1` is live but its proposals reach only `meta.switch_scan` and the `[PF SWITCH]` log line (F15); Shadow_Board showed 24 pairs on 17-Oct (e.g. sell KRP → buy ATAT, Δ 23.98 % vs hurdle 1.2 %) that no holding row displayed. Root cause in code: `_run_switch_scan` stamps `switch_note` onto the holding dicts after the action rows were rendered, so the note never reaches a row.

| Half | Gate | What |
|---|---|---|
| **Backend** `core/analysis/portfolio_actions.py` v1.17.0 → **v1.18.0** | rides `TFB_PF_SWITCH_SCAN`; kill `TFB_PF_SWITCH_ADVISORY_LEGACY=1` | `_stamp_switch_advisory(action_rows, sw_meta)` right after the scan on the certified path: the best pair per holding (highest Δ over proposals + persist_pending; a confirmed pair wins a tie) → `row["switch_advisory"] = "switch: → ATAT Δ+24.0% (adv)"` (day-1 pair: `(adv, day 1/2)`) and `row["detail"]["switch_advisory"] = {buy, delta_pct, hurdle_pct, persist_day, precedence}` with the §4.7 note ("the rule verdict and the funded action stand; a switch is advisory only, never an order"). `advisor_note`, action, sizing, KPIs untouched; `meta.switch_scan.advisory_rows` added. |
| **Cockpit** `apps_script/10_My_Portfolio.gs` v1.5.14 → **v1.5.15** | Script Property `PD_SWITCH_ADVISORY_COL = on` (default off) | ACTIONS table gains a 27th column "Switch (adv)" after Advisor Note (`pdActionHeaders_`, pure; `pdSwitchAdvisoryCell_`, pure); the sheet gains the column when narrower; wrap on the new last column. `PD_ACTION_HEADERS` itself untouched → the Portfolio_Action_Log width contract (26 → 32) is unchanged. Off: headers, rows, formats and the off-mode golden byte-identical to v1.5.14. |

## 2. Read-back harness (the brief's "include TFB_PF_RESEARCH_VIEW in the same harness")

`tests/test_pf_switch_advisory_c4.py` runs the real module with the scan live under every portfolio gate combination:

| Path | TFB_PF_RESEARCH_VIEW | TFB_PF_UNCERTIFIED_VIEW | switch field |
|---|---|---|---|
| certified (fresh packet) | any | any | **present** on the best-pair holding; second scan flips `(adv, day 1/2)` → `(adv)` |
| protective (no packet) | off / observe / enforce | — | absent; `meta.research_view` present iff observe/enforce |
| protective (no packet) | — | on | absent; `meta.uncertified_view.mode = on` |
| stale candidates (> 6 h) | — | — | absent; `switch_scan.status = stale_candidates`, `advisory_rows 0` |
| scan off / legacy | — | — | absent; every other row field and KPI identical |

## 3. Decisions (flag for Emad)

- **Display-only, rides the existing scan gate.** No new ENV to arm the backend field (the scan is the decision already taken; the field is its read-back); the GAS column is property-gated because it changes the sheet layout.
- **Best pair = highest Δ among the scan's published proposals** (`advisor_switch_scan` keeps the top 3 pairs overall; holdings outside them show nothing, honestly).
- **The deployed verifier** (`scripts/verify_core_repair_readback.py`) is unchanged: its protective probe never runs the scan, so there is nothing for it to assert here; the harness above is the read-back.

## 4. Protocol evidence

- **Anchored edits:** backend 4 (`scratchpad/edit_c4_pa.py`), GAS 7 (`scratchpad/edit_c4_gas.py`); every anchor `count == 1`.
- **Zero-removal proof:** backend removed lines = `Version:` header and `PORTFOLIO_ACTIONS_VERSION` (rewritten); GAS removed lines = `Version:`, `PD_VERSION` and `pdWriteTable_(sheet, row, PD_ACTION_HEADERS, aRows,` → `pdWriteTable_(sheet, row, aHeaders, aRows,` where `aHeaders === PD_ACTION_HEADERS` when the property is off. Functions added: backend 3, GAS 3; removed 0.
- **ES5:** every added GAS line passes the guard (harness C3).
- **Dark by default:** GAS column off; backend field only under the live scan, removable by `TFB_PF_SWITCH_ADVISORY_LEGACY=1`. No ENV, Script Property or cell changed by this PR.
- **Harness:** pytest `tests/test_pf_switch_advisory_c4.py` 9/9 + `test_switch_scan_wiring`, `test_pf_uncertified_view_c1`, `test_pf_research_view_p06`, `test_portfolio_actions` green; `node tests/js/pf_switch_advisory_col.test.js` 4/4; `pf_uncertified_context` 7/7 (pins moved to 1.5.15); `pf_ibkr_evidence` 14/14 golden; `apps_script_syntax` 4 compiled; ruff clean; `ci.yml` lean list and JS step updated.

## 5. Read-back after deploy (Claude/Emad)

1. Deploy backend v1.18.0; on the next certified morning the response rows for holdings with a scan proposal carry `switch_advisory`; `meta.switch_scan.advisory_rows ≥ 1` when Shadow_Board shows pairs.
2. Paste 10_My_Portfolio v1.5.15; `pdSelfTest` → `switch advisory col (PD_SWITCH_ADVISORY_COL): off (default) | headers 26 | core: ok`; set the property `on` after one observe cycle → "Switch (adv)" column reads e.g. `switch: → ATAT Δ+24.0% (adv)` on KRP.
