# Commit sheet — compliance_gate v1.1.0 + shadow_board.yml (build #3, 2026-09-06)

| Item | Value |
|---|---|
| Files (exact repo paths) | `core/compliance_gate.py` · `.github/workflows/shadow_board.yml` · this sheet at `docs/evidence/TFB_Commit_Sheet_compliance_gate_v1.1.0_2026-09-06.md` |
| Base pinned | main `d97e9f80` · `compliance_gate.py` blob `193b4504` (v1.0.1) · `shadow_board.yml` blob `0c4cabf3` — raw fetch == HEAD tarball for both |
| New blobs (expected after commit) | `compliance_gate.py` **`4bcd7be7`** (v1.1.0, 496 → 568 lines: +76 / 4 replaced) · `shadow_board.yml` **`6f1dd42c`** (129 → 131 lines, +2) |
| Change class | P-79 root cause — the 2026-08-13 screening retirement completed on the shadow (challenger) path |
| ENV required | **GitHub repository Variable `TFB_COMPLIANCE_SCREEN_RETIRED=1`** (reported added). Read only by `shadow_board.yml`; Render never sets it. Empty/unset/`0` ⇒ OFF ⇒ v1.0.1 byte-identical for every consumer. |
| Deploy | Push to main. Render redeploys (module changed) — harmless: default OFF there, selector behaviour unchanged; fundamentals L2 armed. |
| Commit message | `compliance_gate v1.1.0: SCREEN_RETIRED status behind TFB_COMPLIANCE_SCREEN_RETIRED (P-79 root cause; 2026-08-13 retirement completed on the shadow path); shadow_board.yml maps the variable` |

## Mechanism (from source, 2026-09-06)
`run_shadow_board.evaluate_board` → `compliance_gate.evaluate`: eligible only if `shariah_status ∈ {AUTHORITY_PASS, MODEL_SCREEN_PASS}`. Non-Saudi names are absent from the 408-row authority index ⇒ `model_screen`: activity screen (insurance/banks ⇒ `MODEL_SCREEN_FAIL`) then `totalDebt/marketCap` from yfinance `get_info` (> 0.30 ⇒ `FAIL`; miss ⇒ `UNKNOWN`). Harness on today's 9-name board, retired OFF: `compliance_eligible 1, blocked {UNKNOWN: 6, MODEL_SCREEN_FAIL: 2}` — the challenger basket that has kept S-1 at 3/28.

## What changes (anchored edits, count==1 each)
1. Header history block + ENV doc; `__version__` 1.0.1 → 1.1.0.
2. `SCREEN_RETIRED` status; `INVEST_OK_STATUSES = {AUTHORITY_PASS, MODEL_SCREEN_PASS, SCREEN_RETIRED}`.
3. `screen_retired()` env helper (`TFB_COMPLIANCE_SCREEN_RETIRED`, default `0`).
4. `evaluate()`: when retired, the Shariah step (authority lookup + model screen, incl. the sukuk branch) is replaced by the `SCREEN_RETIRED` stamp, source `retired_2026-08-13`, reason `screen_retired_2026-08-13`. Tradability, Nomu venue block, instrument permissions and floor-vs-cap unchanged; blocked names keep their blocked status.
5. Selftest +8 checks (22/22): OFF ⇒ UNKNOWN not eligible; ON ⇒ US name eligible, activity-blocked sector no longer blocks, `.NS` still untradable, `9628.SR` still venue-blocked, HK floor still locks at 50K equity, sukuk eligible, OFF-again verdict == pre-arming verdict.
6. Workflow: `TFB_COMPLIANCE_SCREEN_RETIRED: ${{ vars.TFB_COMPLIANCE_SCREEN_RETIRED }}` under the existing job env.

Functions added 1 (`screen_retired`), removed 0. No consumer status set lists `SCREEN_RETIRED` as blocking (`run_shadow_scorer._BLOCKING`, `regret._COMPLIANCE_REFUSALS`, selector's `BROKER_UNTRADABLE` check) — verified by read; floor-locked names are keyed `FLOOR_LOCKED` by the board because the status is in `INVEST_OK_STATUSES`.

## Proofs
| Step | Result |
|---|---|
| py_compile / YAML parse | OK / OK (job env keys resolve) |
| AST zero-removal | removed 0, added 1 |
| Smart-quote scan | CLEAN |
| Module selftest (real) | **22/22** |
| Consumers, retired OFF | shadow_board selftest **20/20**, shadow_scorer selftest **87/87**, selector + builder tests **47/47** |
| Consumer harness ×3 (`evaluate_board` on today's board, real script) | **8/8 PASS ×3**, identical: OFF reproduces P-79 (1 eligible / 6 UNKNOWN / 2 FAIL); ON ⇒ 9/9 eligible, `blocked {}`, every row `SCREEN_RETIRED / retired_2026-08-13`, tradability + venue columns unchanged, cost model computed; `.NS` still `BROKER_UNTRADABLE`, HK still `FLOOR_LOCKED` at 50K; OFF-again identical |
| Known, by design | shadow_board's own `--selftest` asserts v1.0.1 Shariah fixtures and reads 16/20 if run **with** the variable set; the workflow does not run it and CI never sees repo Variables |

## Arming + read-back
- First affected run: the next scheduled `shadow_board.yml` run after this lands with the variable set (05:10Z / 14:10Z). Today's evidence runs are spent on L2; if you want the 14:10Z (17:10 Riyadh) run untouched, commit `shadow_board.yml` after it.
- PASS: `_Run_Log` `[SHADOW-BOARD v1.3.0] cands=N eligible=N blocked={}` on an allow-listed board (floor-locked names, if any, appear as `FLOOR_LOCKED`); `Shadow_Board` "Shariah Status" column reads `SCREEN_RETIRED`; the following `[S1-GATE v1.7.2]` line scores the day instead of `reason=no-challenger`.
- Kill: variable → `0` or delete.

## Post-commit verification (Claude)
HEAD re-fetched; blobs == `4bcd7be7` / `6f1dd42c`; tree diff vs `d97e9f80` shows only the two files + this sheet.
