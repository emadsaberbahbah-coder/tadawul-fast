# Commit Sheet — `scripts/track_performance.py` v6.49.0 [BOARD-COVERAGE] — 2026-10-25

**Scope:** the live decision board's seated symbols join the daily forecast record. ENV-gated, default OFF.
**Files:** `scripts/track_performance.py` (6.48.0 → 6.49.0), `.github/workflows/track_performance.yml` + `.github/workflows/daily_sync.yml` (one mirrored env line each, ENV PARITY), `scripts/verify_deployment.py` (pin), `.github/workflows/ci.yml` (1.1.36, harness in the lean lane), `tests/test_track_board_coverage.py` (new), this sheet.

## S1 — Defect (evidence, live workbook, 2026-10-25 09:40 Riyadh, gviz read-only)

| Fact | Value |
|---|---|
| `Signal_History` rows dated 2026-10-24 | 176 |
| … tagged `Origin Tab = Top_10_Investments` | 44 — `2222.SR, 2283.SR, BNY.US, 9961.HK, BRK-B.US, 7030.SR, 7020.SR, 1150.SR, …` (all WATCH/WATCHLIST) = the rows the backend's legacy `/sheet-rows page=Top_10_Investments` build returns |
| … tagged `Decision_Coverage` | 132 — ledger lots (`AER, CARE, CWBC, DDI, EPRT, HCI, KRP, NMM, PFS, RCI, VEL, 5023.SR, …`) |
| Live board 2026-10-24 (16_Decision_Top10 v1.14.0, 22:51 run) | `PHP.L, FRST.US, ZTO.US, CRC.US, CBAN.US, ORC.US, AX.US, PGC.US, UMBF.US, FBK.US` |
| Board names with a 2026-10-24 snapshot | **0 of 10** |
| Last `Signal_History` row for `ZTO.US` / `PHP.L` / `FRST.US` | August 2026 |

`run_once` feeds BOTH pipelines (Performance_Log records and Signal_History snapshots) from `get_top10_rows()` (the legacy backend page) plus the v6.16.0 forced coverage (ledger lots + `TFB_TRACK_PRIORITY_SYMBOLS`). The Apps Script board — the thing the owner acts on — is read only for the v6.33.0 CHAMPION stamp, never fetched. Hence the forecast-vs-outcome evidence (Brier 0.2872, hit rate, calibration) never sees the published recommendations; strategy requirement #2 ("daily recording for Top_10 + holdings") is met for holdings only.

## S2 — Change (additive; zero removals)

- `_board_symbols_enabled()` — `TFB_TRACK_BOARD_SYMBOLS` via `_env_bool`, **default False**.
- `_board_symbols_from_values(vals)` — PURE parser of the board's `A12:B130` window: header `Rank|Symbol`, stop at `ALL QUALIFIED` or the first blank row, SG-1 shape validation, dedupe, cap 15; **GRACE ghosts (`—` rank) are KEPT** and counted (a grace hold is a published recommendation under watch). The v6.33.0 champion read (`_read_board_selected_symbols`) is untouched and still excludes ghosts.
- `_load_board_symbols(spreadsheet_id)` — one gspread read with the stores' SA-credential pattern (mirror of `_load_costbasis_symbols`); fail-open `([], 0)`.
- `_augment_with_decision_symbols`: when on, board symbols join `priority` and ride as **extras** into `_force_plan` (never cut by `TFB_TRACK_FORCE_MAX`); fetched rows whose symbol is board-only get `origin = "Decision_Board"`, all others keep `"Decision_Coverage"` byte for byte; one `[v6.49.0 BOARD-COVERAGE] board=n new=k ghosts=g` INFO line.
- Workflows: `TFB_TRACK_BOARD_SYMBOLS: ${{ vars.TFB_TRACK_BOARD_SYMBOLS || 'off' }}` in both tracker lanes (ENV PARITY rule; `tests/test_track_flags_mapped.py` unaffected — it pins the three dedup/risk flags).

**Byte-identical when off:** no sheet read, no log line, `_force_plan` receives `sorted(set(extras))` which dedupes exactly as the function's own `dict.fromkeys` already did.

## S3 — Known limits
- Rows come from `TFB_TRACK_FORCE_ROWS_ENDPOINT` (`/v1/analysis/sheet-rows`, page `Market_Leaders`): the engine's per-symbol verdict shape, not the board's sized ticket; stability state, seat and sizing are not recorded (the board's `_Selection_Log` keeps those).
- Up to 15 extra forced fetches per run (edge-timeout budget: `TFB_TRACK_FORCE_MAX` still caps the closed-lot remainder, never the board).
- A board that is WITHHELD still has seated names and is recorded — intended: the recommendation existed even when sizing was withheld.

## S4 — Tests
`tests/test_track_board_coverage.py` — B1–B7: parser (seats + ghosts in order, zone stop, blank-row stop, junk, dedupe/cap), knob (default off; 1/on/true; 0/off/false), `_force_plan` with board extras never cut, byte-identity of the extras list when off, version floor ≥ 6.49.0 + inventory. **PASS 7/7** as a script; pytest: `tests/test_track_board_coverage.py tests/test_track_flags_mapped.py tests/test_track_force_coverage_p180.py tests/test_tracker_canonical_publications.py tests/test_tracker_hist_target_close.py tests/test_tracker_record_only_workflows.py tests/test_performance_summary_integrity.py tests/test_verify_*.py` → **111 passed, 4 skipped**. `py_compile` PASS. Golden negative: B7 fails on v6.48.0 (helpers absent).

## S5 — Operator steps
1. Squash-merge. Nothing changes until the Variable exists.
2. After one scheduled Performance Clock run post-merge (read-back: the `[v6.16.0 COVERAGE]` line unchanged), set the repository Variable **`TFB_TRACK_BOARD_SYMBOLS = 1`**.
3. Read-back on the next clock run: job log `[v6.49.0 BOARD-COVERAGE] board=n new=k ghosts=g`; `Signal_History` gains rows with `Origin Tab = Decision_Board` for the board names of that day.
4. Rollback: delete the Variable (or set `0`).

## S6 — Not done here (follow-ups in `TFB_Workbook_GAS_Review_2026-10-25.md`)
- Replacing the legacy `get_top10_rows()` page as the Performance_Log seed (a cohort-shape change; needs its own gate).
- Recording the board's sized ticket (entry zone, stop, TPs) — a `_Selection_Log` join, Tuesday.
