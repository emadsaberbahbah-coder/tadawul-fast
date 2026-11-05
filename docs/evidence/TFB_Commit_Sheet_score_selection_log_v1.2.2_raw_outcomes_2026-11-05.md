# Commit Sheet - `scripts/score_selection_log.py` v1.2.2 [RAW OUTCOMES] - 2026-11-05

**Scope:** item 3 of the 5 Nov brief "Fix the Google Sheets recommendation and outcome reports" (the win-rate discrepancy).
- **The defect:** the Looker board page showed a +5-session win rate of **51.92 % (243 / 468)**; the scorer's own SUMMARY row said **52.56 % (246 / 468)**. Same cohort, same denominator, three wins apart.
- **The cause:**
  - `summarize()` counts a win on the raw return (`r > 0`).
  - `detail_rows()` wrote every outcome cell through `_r2` (2 decimals).
  - `archive_to_bigquery.py` reads those cells UNFORMATTED (the stored, already rounded value), and `sql/bigquery/10_v_board_episodes.sql` classifies `ret_plus5_pct > 0`.
  - A return in (0, +0.005) % is a win in SUMMARY and `0.00` -> a loss in the view. Three such episodes explain 246 vs 243.
- **The change:** a new env gate `TFB_BOARD_SCORECARD_RAW`, **default OFF**, armed in `board_scorecard.yml`.
  - Gate on: the DETAIL outcome cells (+1/+5/+10/+20 %, To Exit %, To Now %, vs SPUS +5 pp, vs SPUS Now pp, and with the ranking gate vs Ref +5 pp / vs Ref Now pp) carry the raw value. Rounding stays a display choice.
  - Unchanged: SUMMARY, CHURN, RANKING, CHALLENGER, every input column (prices, Engine ROI, Rel, Conf), the tab layout and the column order.
  - Gate off (unset, `off`, junk): the v1.2.1 rows byte for byte.

**Files:**
- `scripts/score_selection_log.py` 1.2.1 -> 1.2.2: `RAW_ENV`, `_raw_enabled()`, `_rraw()`; `detail_rows(..., raw=None)`. Every added line is ASCII.
- `.github/workflows/board_scorecard.yml`: job env `TFB_BOARD_SCORECARD_RAW: "on"` (with a WHY comment).
- `tests/test_board_scorecard_p182.py`: new P16 section.

## Tests
- `python3 tests/test_board_scorecard_p182.py`: `RESULT 166 PASS / 0 FAIL`, digest `f3625220760002ed`, run twice, equal digests.
- **P16:**
  - gate off equals the 2-decimal rows; the accepted spellings arm it, junk does not;
  - gate on writes raw cells; the inputs are untouched; no `-0.0`;
  - on a fixture with five returns in (0, 0.005), the view rule (`> 0` on the written cell) counts **5 wins = the SUMMARY's 5**, while the old rounded cells give 1;
  - the workflow arms the gate; `SCRIPT_VERSION == "1.2.2"`.
- `pytest tests/test_board_scorecard_p182.py tests/test_repository_workflow_audit.py`: 12 passed, 10 subtests passed.

## Operator steps
1. Merge. Nothing else: the nightly `board_scorecard.yml` run rewrites `_Board_Scorecard` with raw outcome cells, and the next `bq_archive.yml` run carries them into `v_board_episodes`.
2. **Check (the morning after):** the Looker +5 win rate equals the SUMMARY row (same numerator and denominator).
3. **Rollback:** set the env line to `"off"` (or delete it); the next run writes the v1.2.1 rows.

## Known limits
- `_Board_Scorecard` shows the outcome cells at full precision (for example `0.0041237`). If wanted, give those DETAIL columns a `0.00` display format in the sheet; the stored values stay raw.
- Episodes archived before the first raw run keep their rounded cells until the scorer rewrites them (it recomputes every episode nightly, so the next snapshot replaces them).

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
