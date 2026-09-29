# TFB Commit Sheet — scripts/run_shadow_board.py v1.5.0 [P-175 NON-TRADING FREEZE]

Date: 2026-09-29 (Tuesday) · Lane: GitHub/Python (shadow_board.yml) · Build #2 of the day · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `scripts/run_shadow_board.py` v1.4.0 at HEAD `045d5f9` (#617) |
| Base SHA-256 | `511d717d8892cd2b…` (943 lines) — identical to the 09-13 P-139 commit record |
| Workflow at HEAD | `.github/workflows/shadow_board.yml` sha `2f9aa1fd748d…` (cron `10 5 * * *` + `10 14 * * *` = 08:10 / 17:10 Riyadh daily; `TFB_BOARD_ENGINE_ROI: "enforce"` at L93; `dry_run` dispatch input) |
| Environment golden | base `--selftest` = 25/27 in this workspace (the two `fundamentals -> MODEL_SCREEN` checks need optional deps absent here); delivered must reproduce the same FAIL set |

## S2 — Root (pinned on `_Run_Log` S1-GATE lines)
Every EXCLUDED_INFRA day since the P-139 fix is challenger **turnover across non-trading days**, not stale prices: 09-27 (Sun) `new=[ADAM,GOOGL,ITRN] stale=[PINFRA,NVDA]`; 09-28 (Mon) `excluded_reason=fresh-floor chal fresh 1/5 new=[GOOGL,ITRN,PINE,PINFRA]`; same mechanism 09-20. The board rebuilds twice on Saturday and twice on Sunday from a cockpit that drifts on weekends; the scorer pairs a seat only with the last SCORED day (Friday), so any seat added after Friday 15:20Z is NEW on Monday. Scored days since 09-16: 8; excluded: the two post-weekend days. Counter 11/28 (+41 excluded).

## S3 — Change (6 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | v1.5.0 WHY block (evidence, fix, S-1 boundary note) + `SCRIPT_VERSION = "1.5.0"` |
| E2 | constants | `ENV_FREEZE = "TFB_BOARD_FREEZE_NONTRADING"`, `ENV_FREEZE_HOLIDAYS`, `ENV_FREEZE_TODAY` (harness clock hook), `FREEZE_TAG` |
| E3 | after `_now_riyadh` | 5 pure helpers: `_freeze_mode()` (explicit words only), `_freeze_holidays()` (comma/space ISO list, junk dropped), `_freeze_today()` (UTC run date; env override), `_is_nontrading_day(d, holidays) -> (bool, reason)` (Sat/Sun + list), `freeze_verdict(mode, today, holidays) -> {mode, date, nontrading, reason, skip, note}` |
| E4 | `main()` after the selftest branch | verdict computed first; note printed when non-empty; **enforce + non-trading → return 0 after ONE `_Run_Log` row (`FROZEN`, JSON `{version, freeze, date}`)** — no Top_10 read, no yfinance, no engine-ROI fetch, no board write, no Regime_History; `--dry-run` skips even that row |
| E5 | final `_Run_Log` append | details JSON built as `_rl_details` — `{"version"}` unchanged in off mode; observe on a non-trading day adds `"freeze_observe": "<reason>"` (countable read-back) |
| E6 | `_selftest` | +7 freeze checks (holiday parse, weekend/holiday detection, off/observe/enforce Sunday, enforce Monday, mode parser) |

DEFAULT OFF = byte-identical behaviour to v1.4.0 (only the version string differs). Observe = proceeds exactly as v1.4.0 and prints/logs "would freeze". Enforce = zero-write on Sat/Sun and listed holidays. Rollback = env off or `git revert`.

Deliberate scope cuts: the Monday-morning rebuild is untouched (it is a legitimate trading-day rebuild; its own turnover stays visible to the scorer — if it still breaches the floor, the full fix is the frozen weekly board as challenger from Sat 10-03, a separate spec addendum); no meta line is added to the board in observe mode (the scorer's shape guard reads the meta block — byte-safe by design); the scorer's own NON_TRADING/EXCLUDED classification is untouched.

S-1 boundary note (for the evidence register): board-INPUT cadence change of the P-139 class — gate criteria, §1 benchmark, fresh floor and counter byte-untouched; prior excluded days stand; counter continues from 11/28.

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `410a3dcd386005d9d5a99acff47e66a8e6f1147d48d01482de629d4869892873` (1,091 lines) |
| `py_compile` | PASS |
| AST | functions 29 → 34 (+5 listed above, **0 removed**) |
| Line audit | 2 base lines not verbatim = the version constant + the `json.dumps({"version": …})` line replaced by the `_rl_details` equivalent (same bytes in off mode); every WHY block carried |
| Non-ASCII | multiset identical to base (24 lines; 0 new); 0 smart quotes |
| Harness `tests/test_sb_freeze_nontrading_p175.py` (REAL delivered module + REAL base via `SB_BASE`; fakes only at the `_open_sheet` / yfinance / regime / engine-ROI / authority / switch-scan seams; `rows_to_records`, `evaluate_board`, `build_risk_block`, `write_board`, the freeze helpers run for real) | **34/34 PASS ×3, digest `0914908ce0b0` ×3** |
| F1 | pure battery: Sat/Sun, weekday, holiday parse (junk dropped), holiday detection, verdicts for off/observe/enforce on Sun 09-27 and enforce on Mon 09-28, mode parser (case/space; junk → off), clock hook |
| F2 | **off on a Sunday == base call-for-call** (11 sheet calls in the same order, board rectangle identical with the version cell masked; stdout identical) |
| F3 | observe on a Sunday: writes identical to off, "would freeze (observe): weekend:Sun" printed once, `_Run_Log` JSON `{"version":"1.5.0","freeze_observe":"weekend:Sun"}`; off carries no freeze key |
| F4 | enforce on a Sunday: no `update`/`clear`/`append_rows`, no Top_10 read; exactly one `_Run_Log` row `FROZEN` with `{"version":"1.5.0","freeze":"weekend:Sun","date":"2026-09-27"}`; the `[SHADOW-BOARD v…]` verdict line is not printed |
| F5 | enforce on Monday 09-28 == off on Monday (calls and stdout identical) |
| F6 | holiday list: 2026-11-26 frozen with `holiday:2026-11-26`; the same date without the list runs normally |
| F7 | `--dry-run` under enforce on a Sunday: zero sheet calls, note printed |
| F8 | delivered `--selftest` 32/34 with the 7 freeze checks PASS and the **same FAIL set as base** (environment golden 25/27 → 32/34) |

## S5 — Delivery
| File | Destination |
|---|---|
| `scripts/run_shadow_board.py` | repo (full file) |
| `tests/test_sb_freeze_nontrading_p175.py` | repo `tests/` (set `SB_BASE=<path to the v1.4.0 file>` for the dual-tree legs; without it the base legs are skipped) |
| `docs/evidence/TFB_Commit_Sheet_run_shadow_board_v1.5.0_2026-09-29.md` | repo `docs/evidence/` |

## S6 — Arming (GitHub lane; one ENV per evidence run)
1. Commit the three files; the next scheduled board (14:10Z today) runs v1.5.0 in **off** mode — read-back = `[SHADOW-BOARD v1.5.0]` in `_Run_Log` with `{"version":"1.5.0"}` and an unchanged board.
2. Arming #1: `TFB_BOARD_FREEZE_NONTRADING: "observe"` in `shadow_board.yml` beside `TFB_BOARD_ENGINE_ROI` (L93) — read-back = **Saturday 10-03 08:10 Riyadh** board prints "would freeze (observe): weekend:Sat" and the `_Run_Log` JSON carries `freeze_observe`; weekday runs show nothing (by design).
3. Arming #2 (separate sitting, same Saturday before 17:10 Riyadh, with the observe read-back in hand): `"enforce"` — read-back = the 17:10 Saturday and both Sunday runs log `FROZEN`, the tab keeps Friday's board, and Monday 10-05's scorer reports `new=` only for Monday-morning turnover.
4. Optional: `TFB_BOARD_FREEZE_HOLIDAYS: "2026-11-26,2026-12-25"` (US market holidays) in the same env block.
