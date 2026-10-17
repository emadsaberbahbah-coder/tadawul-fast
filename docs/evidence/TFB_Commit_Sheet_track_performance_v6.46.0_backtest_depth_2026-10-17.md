# TFB Commit Sheet — track_performance.py v6.46.0: L2 hypothesis backtest depth (period translation, named cause, loud empty verdict)

Date: 2026-10-17 (Saturday) · Lane: GitHub (backtest_harness.yml runs this script weekly; Render never runs `--backtest`) · Brief: Codex Brief 2026-10-17 v2 §2 **L2** · Diagnosis: `TFB_L2_Diagnosis_hypothesis_backtest_no_bars_2026-10-17.md` (same commit)

## S0/S1 — Base pin
Base HEAD `11ef1345` (main). Branch `claude/new-session-twoifp`, on top of `fa42a55` (L1).

| File | Base | Delivered | Change |
|---|---|---|---|
| `scripts/track_performance.py` | sha256 `f58f2dafb2759b5dcaa6e0fcc50d8838f2f89422995f8902c198e34563ecdf5a` (10,929 ln, v6.45.0) | sha256 `fe690a00ca6ab7dbe330f5ec7d1f7494ad1cc7f9d49e82ba059aed3f9bec532b` (11,085 ln, v6.46.0) | **+162 / −6** (8 anchored edits, each `count == 1`); the 6 removed lines are the `min_window: int = 12` default (now `_BT_MIN_WINDOW`), the two-line `universe/max_hist` opener (re-emitted with two counters) and the three-line bare `fn(sym)` retry (re-emitted inside the translated path) |
| `tests/test_backtest_history_depth_l2.py` | — | sha256 `5366cd5205dd9d41865e8fc0802ed4edc2706f662b2d114f6dbec2df77956da9` (231 ln) | NEW, 39 tests, offline |
| `.github/workflows/ci.yml` | — | — | +1: the new test in the lean pytest list after `tests/test_tracker_audit_lane.py` |

## S2 — Why
See the diagnosis note. In one line: `fn(sym, days=730)` raises `TypeError` against `yahoo_chart.fetch_history(symbol, period, interval)`, the bare retry gets Yahoo's 1-month default (~22 bars), the provider loop breaks on that non-empty list, and `run_backtest`'s `len(bars) < 33` guard empties the universe before `max_hist` is touched — Universe 0, History Days 0, six silent `INSUFFICIENT_DATA` rows and a green run. Confirmed on the live job log of run 38161414138 (`range=1mo` on all 16 symbols).

## S3 — What
| # | Change | Default | Why this default |
|---|---|---|---|
| 1 | `_fetch_from`: on `TypeError`, call `fn(sym, period=_bt_days_to_period(days))` before the bare retry (730 → `"2y"`) | **ON**, kill-switch `TFB_BACKTEST_PERIOD_FROM_DAYS=0` → v6.45.0 behaviour byte-for-byte | the off state is an accidental fallback nobody configured; nothing outside this file consumes an ACCEPTED hypothesis (grep: registry writer + `core/regime.py` protocol text only); with 22 bars no verdict is reachable, so no decision can change — the off state is the defect (P-127/P-130 class) |
| 2 | `run_backtest`: count `fetched` symbols and the deepest list; when Universe Size is 0 the Notes cell reads `no usable history: 0/16 symbols with >= 33 bars (deepest 22); provider window too shallow -- 0 trigger events (< min 30); no verdict` | on (display honesty, v6.24.0 precedent) | a zero row must say why |
| 3 | `run_backtest_mode`: `[backtest] history depth: usable a/b (need >= N bars; deepest d, median m)` line; when **every** hypothesis has Universe 0 → `logger.warning` + stdout `[BACKTEST-EMPTY v6.46.0] …`, one WARNING row appended to `_Run_Log` (`Hypothesis_Registry · BACKTEST_EMPTY · cause · depth JSON`, fail-open), **exit 2** | on | "assert universe > 0" as a run verdict: the registry is still written with the cause in Notes, but the harness run turns red instead of green |
| 4 | `_BT_MIN_WINDOW = 12` constant; `run_backtest(min_window=_BT_MIN_WINDOW)` | — | the depth line and the guard use the same number |

Pure helpers added: `_bt_period_from_days_enabled`, `_bt_days_to_period`, `_bt_history_depth`, `_bt_empty_verdict`; method `_append_runlog_backtest_warning`. Removed 0.

## S4 — Audits
| Check | Result |
|---|---|
| Anchored edits | 8/8 `count == 1`; `ast.parse` PASS |
| `tests/test_backtest_history_depth_l2.py` (period table incl. 730→2y and the kill-switch spellings; fetch path reproduced with a Yahoo-shaped fake module that has no `days` kw — exactly one `(sym, "2y")` call per symbol, EODHD untouched, kill-switch → `(sym, "1mo")` and 22 bars, a `days`-accepting provider is called straight through; `run_backtest` 16×22 → Universe 0 / History 0 / named cause, 16×505 → Universe 16 / History 505; `{}` → legacy note; depth + verdict pure functions; `_Run_Log` row shape, no-sheet no-op and swallowed exception; version + changelog) | **39 passed** |
| Existing tracker/backtest suites: `test_tp_*`, `test_track*`, `test_tracker*`, `test_tfb_backtest_rank_evidence`, `test_performance_math_extraction`, `test_checkpoint_target_contract`, `test_outcome_price_evidence`, `test_learning_guards` | **352 passed, 1 skipped** (pre-existing skip) |
| ruff `E9,F63,F7,F82,F401,F841` on both files | PASS |
| Byte-identity under the kill-switch | `TFB_BACKTEST_PERIOD_FROM_DAYS=0` → the bare `fn(sym)` retry exactly as v6.45.0 (pinned); items 2–3 only change text/exit on a universe that is already empty |
| Harness reproduction | the fake provider reproduces the 11-Oct log (`1mo` → 22 bars → Universe 0) and the repaired path (`2y` → 505 bars → Universe 16) |

## S5 — Delivery
`scripts/track_performance.py` · `tests/test_backtest_history_depth_l2.py` (NEW) · `.github/workflows/ci.yml` (+1) · `docs/evidence/TFB_L2_Diagnosis_hypothesis_backtest_no_bars_2026-10-17.md` (NEW) · this sheet (NEW). No ENV, no Script Property, no workbook cell.

## S6 — Read-back (next backtest_harness run, Sunday 13:00Z slot or a manual dispatch)
1. Job log: `history fetched for 16/16 symbols` **and** `history depth: usable 16/16 (need >= 33 bars; deepest ~505, median ~505)`; Yahoo requests show `range=2y`.
2. Hypothesis_Registry: Universe Size > 0 and History Days ≥ 60 on the four `_PS_FIELDS` hypotheses (and the two STABILITY ones); trigger events counting; status becomes REJECTED / ACCEPTED / INSUFFICIENT_DATA **by evidence** (the DSR and non-overlap gates are unchanged).
3. Golden negative: with `TFB_BACKTEST_PERIOD_FROM_DAYS=0` the run goes red (exit 2), `_Run_Log` gains a `BACKTEST_EMPTY` WARNING row and every Notes cell starts with `no usable history:`.
4. Rollback: `TFB_BACKTEST_PERIOD_FROM_DAYS=0` (depth), or `git revert` (all).

## Known limits
- `.SR` depth depends on Yahoo's chart API honouring `range=2y` for Tadawul tickers; if a venue comes back shallow the depth line will show it and the cause text will name the deepest list — `TFB_BACKTEST_KSA_YF=1` (yfinance deep pull, already in the file) remains the second lever and is still unmapped in `backtest_harness.yml` (operator item, not changed here).
- Exit 2 makes the weekly harness red on an empty universe by design; `tfb_workbook_dq_audit.py` DQ-67 already fails on all-zero universes, so the two signals agree.
- `backtest_harness.yml` still uses `ubuntu-latest` and `checkout@v4` / `setup-python@v5` (not touched; noted in the diagnosis).
