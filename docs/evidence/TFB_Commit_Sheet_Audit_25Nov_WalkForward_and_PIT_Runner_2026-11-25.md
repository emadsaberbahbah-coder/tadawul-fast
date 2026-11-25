# Commit sheet — external audit of 25 Nov 2026: walk-forward validation and the PIT repair runner

## Why
The owner's external audit (TFB_Project_Audit_2026-11-25, overall 60/100) raised two repository items that
need no deploy:

1. **P1 validation.** `scripts/tfb_backtest.py::cv_brier` shuffled rows into five random folds. Cohorts from
   the same day and overlapping 1W/2W/1M windows sat on both sides of a fold, and quintile cuts were fitted on
   every row. The SEPARATES verdict could be earned with hindsight.
2. **P0 research.** `_PIT_Fundamentals` still holds 78 timestamp-shaped values in reliability/diagnostic
   columns and 11 non-instrument label rows from the July header migration. The repair planner
   (`scripts/repair_pit_legacy_rows.py`, 11 Oct) existed but had no runner, so it never ran.

## What changed
| File | Change |
|---|---|
| `scripts/tfb_backtest.py` 1.1.1 → 1.2.0 | Purged walk-forward Brier (`wf_brier`): k+1 date blocks, training rows only when their outcome was known `--embargo-days` (default 1) before the test block, quintile cuts refitted on training rows, baseline = training win rate on the same test rows. The verdict uses the walk-forward gain. The random K-fold stays in the report as a labelled legacy diagnostic. |
| `tests/test_tfb_backtest_walk_forward.py` (new, 8) | Entry-day label gains under the legacy K-fold (+0.05) but not walk-forward; purge; embargo; train-only quintile cuts; Target Date preferred; legacy flag/env; selftest. |
| `tests/test_tfb_backtest_rank_evidence.py` | Golden dict gains the walk-forward keys (undated rows → `insufficient_dated_rows`); version 1.2.0. All legacy values unchanged. |
| `.github/workflows/pit_legacy_repair.yml` 1.0.0 (new) | Dispatch only. `scan` (default) prints counts. `apply` backs up the tab first, then runs `--apply` (the script refuses anomalies, > 78 realigns, > 11 deletions, or a grid that changed between reads). |
| `scripts/backup_sheet_tab.py` 1.0.0 (new) | Copies a tab to a hidden `<prefix>YYYYMMDD` tab in the same workbook; never overwrites an existing backup. |
| `tests/test_pit_legacy_repair_runner.py` (new, 4) | Backup naming/hiding/no-overwrite; workflow is dispatch-only, scan default, backup before apply. |
| `.github/workflows/ci.yml` | Runs the two new test files. |

## Behaviour and rollback
- Backtest: `--cv legacy` or `TFB_BACKTEST_CV=legacy` restores the 1.1.1 verdict exactly.
- PIT runner: nothing runs until dispatched. Rollback of an apply = copy the backup tab back.
- No broker, ENV, Render or live-gate change. Logs print counts only.

## After merge
1. Dispatch `pit_legacy_repair.yml` with `scan`. Expected: `realigned=78 deleted=11 anomalies=0`.
2. If clean, dispatch with `apply`. Read back: `[TAB-BACKUP] status=created`, `[PIT-REPAIR] mode=APPLY`, and the
   export audit's `pit_timestamp_in_reliability` check turning PASS on the next export.
3. The next `backtest_signals.yml` run reports walk-forward verdicts; compare with the legacy line in the same report.
