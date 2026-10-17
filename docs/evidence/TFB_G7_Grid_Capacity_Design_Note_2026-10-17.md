# TFB G7 — Grid capacity: design note (Signal_History dedup + Performance_Log archive policy)

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §5 G7 ("design note first") · **Status:** design only — no cell, tab or workflow changed by this note.

## 1. Measured today

| Signal | Value | Source |
|---|---|---|
| Allocated cells | **6,415,719 / 10,000,000 (64.16 %)**, free 3,584,281 | `[CAPACITY-STATUS v6.53.0]`, daily_sync run 39018080003 (08:40Z, 17 Oct) |
| Growth | ≈ +0.5 pt/day (brief) ≈ 50,000 cells/day | brief measurement |
| Signal_History | 31,123 rows, 8,281 unique symbol-days, 22,842 duplicates (73 %), max 19 per key | `[v6.44.0 DEDUP-SIGNAL]` in the same run (mode=observe) |
| Performance_Log | 22,469 records, 11,791 matured, 32 columns | brief / L3 fold |
| Headroom at current growth | ≈ 71 days to the 10M wall (≈ 26 Dec 2026); the August repair (`repair_grid_capacity.py` v1.0.0) bought 3.78M cells by grid-row reduction on six tabs | arithmetic |

The two appenders that grow without bound are Signal_History (45 snapshots/day × 21 columns, written up to 3× per day because every daily_sync lane records) and Performance_Log (new records per audited symbol × horizon, 32 columns). Everything else is overwrite-in-place.

## 2. Lever 1 — Signal_History dedup (delivered dark in L5, v6.47.0)

- `TRACK_DEDUP_SIGNAL_HISTORY=enforce` makes the write a last-of-day upsert: one row per symbol per Riyadh day; new keys append; prior days are never rewritten. Today it would have **skipped 45/45 appends** (`would_skip=45/45 new=0`): roughly 30–45 rows/day (≈ 1,000 cells/day) of growth removed, and the 22,842 historical duplicates stop compounding.
- The historical backlog (22,842 rows ≈ 480k cells) is **not** removed by enforce (never rewrite history). A one-off, archive-first prune of the duplicate rows (keep the latest per key) is a separate, triple-gated tool run in the `repair_grid_capacity.py` style; it recovers ≈ 4.8 % of the grid. Recommendation: do it once, after the enforce read-back shows one row per key for a week.

## 3. Lever 2 — Performance_Log archive policy (new; proposal)

**Policy:** matured records older than **90 days** (by `Maturity Date`, status `matured`) move to a Drive CSV; the sheet keeps the active and recently matured records the cockpit and the scorecard read daily.

| Element | Proposal |
|---|---|
| Target folder | the operator's backup folder used by `backup_workbook.py` (same credentials, same retention discipline) |
| File | `ARCHIVE_Performance_Log_YYYY-MM.csv` — one append-only file per month of maturity; header = the 32 Performance_Log columns verbatim; a `_Run_Log` row `[PL-ARCHIVE vX] moved=N file=… sha256=…` on every run |
| Lane | the existing `track_performance.yml` audit lane (20:47Z) after the scorecard publish; gate `TRACK_PL_ARCHIVE = off (default) | observe | enforce`; observe writes the CSV and counts, enforce also deletes the archived sheet rows (bottom-up `delete_rows`, never `clear`) |
| Safety | archive-before-prune (CSV written and sha-logged before any deletion); row count on the sheet after = before − moved, else exit 2 and no deletion; `TFB_PL_ARCHIVE_MAX_ROWS` per run (default 2,000) so a bad filter cannot empty the tab; records with `realized_roi_source_valid=False` are archived too (they are history, not evidence) |
| Readers to adapt | `forecast_scorecard_rows` (L3) and `PerformanceAnalyzer.analyze` read only what is on the sheet: the Scorecard then covers the last 90 days of maturity by construction. The **long-run cohorts** must come from the archive: add `--scorecard-include-archive` reading the CSVs (one Drive list + N downloads, cached in the artifact). Decide which the operator wants on the strip: 90-day (recent skill) or all-time (sample size). Recommendation: publish both horizons of time as a `Window` column (`90d`, `all`). |
| Effect | 11,791 matured today → ≈ 9,000 older than 90 days ≈ 290k cells (2.9 % of the grid) now, and the daily growth of Performance_Log becomes bounded (≈ 90 days × new records/day). Combined with Lever 1 the grid growth falls from ≈ +0.5 pt/day to ≈ +0.1 pt/day. |

## 4. What this note does not recommend

- Lowering `TFB_SYNC_MAX_SYMBOLS_MARKET` or trimming market-page columns: the August review proved allocated rows, not columns, are the consumer, and the pages are already at their target grids.
- Deleting Signal_History history by hand or with `clear` (does not release allocation; destroys the L5 publication-identity backfill).

## 5. Decision asked of Emad (D-G7)

1. Approve the 90-day Performance_Log archive policy and the Drive folder (same as the backup folder, or a new `TFB_Archive` folder).
2. Approve the one-off Signal_History duplicate prune (archive-first) once enforce has run clean for a week.
3. Choose the Scorecard window the cockpit strip shows (90d, all-time, or both).

On a GO the implementation is one PR: `scripts/track_performance.py` archive step (dark), `tests/test_tp_pl_archive.py`, `track_performance.yml` env mapping (`TRACK_PL_ARCHIVE`, Actions Variable, default off), DQ-audit registration of the `_Run_Log` archive row.
