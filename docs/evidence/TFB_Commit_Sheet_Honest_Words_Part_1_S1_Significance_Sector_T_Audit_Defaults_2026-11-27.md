# TFB Commit Sheet — Honest words, part 1: a PASS and a PROVEN say what they rest on; the export audit drops its stale defaults

Date: 2026-11-27 (Friday) · Lanes: GitHub Actions (Shadow Scorer, Learning Watch, BigQuery views) and the offline export audit · Observe only: no criterion, verdict, gate, recommendation or order changes · No Render deploy

## Why
The full project review of 27 Nov (owner's approval at 14:06 Riyadh: "ok approved and start doing the need") found three pages that say more than their evidence, and an audit whose RED was partly its own stale defaults.

1. **S1_Gate** read PASS, "all six criteria met — Tranche 1 may be authorized", on 37 scored days with about 60 excluded. Criterion 3 asks only for a net alpha of 0 or more. None of the six criteria asks whether that alpha could be chance, and every page that copies the gate repeats the PASS.
2. **Learning_Watch** listed 15 PROVEN rows. All 15 were 'beats its market' cells of `v_outcome_sector`, a view with no test: calls overlap (a name is counted once per day) and the reading needed only +0.5 points and 55% ahead.
3. **tfb_export_audit** rated the owner's export RED with 12 FAILs. Three were the audit's defaults: row counts typed in on 3 Oct (the pages held exactly what the sync wrote), and lateness against a cron that `daily_sync.yml` dropped on 10 Nov.

## What

### `scripts/run_shadow_scorer.py` 1.9.7 → **1.9.8** [S1-SIGNIFICANCE], observe only
- `net_daily_returns()`: per basket, the day's move of the stored Cum Index on scored days from the evidence epoch. The cost drag is in the index, so these are the returns criterion 3 compounds.
- `significance()`: daily net alpha = challenger minus benchmark on the days both were scored; t = mean over its standard error (daily returns do not overlap, so no horizon adjustment). Reading: TOO_FEW_DAYS under 20 paired days; SIGNIFICANT at t of 2 or more (one-sided: an edge, not a difference); else NOT_SIGNIFICANT. Coverage is 'thin' when more days were excluded than scored. It is evidence only when SIGNIFICANT with coverage ok.
- `apply_significance()`: the six criteria, their statuses and the verdict are untouched. On a PASS that is not evidence, the gate's 'why' line ends with the reading in words, so every copy of the gate carries it. A verdict that is not PASS is not annotated.
- One token line `[S1-SIGNIFICANCE v1.9.8] ...` on the verdict, the S1_Gate meta cell and the _Run_Log JSON. Numbers only.
- **On the owner's Shadow_History of 26 Nov:** 37 paired scored days, mean +0.143 points a day, **t = +0.47**, coverage thin → NOT_SIGNIFICANT. The gate's next run will read: "all six criteria met — Tranche 1 may be authorized | significance (observe): daily net alpha t=+0.47 over 37 paired scored days, N excluded: not yet evidence of an edge; the six criteria do not test it".
- No enforce mode exists. Whether the gate should require the test is for the owner's 10-session review (8 Dec).
- `.github/workflows/shadow_scorer.yml` 1.0.4: one env line, `TFB_S1_SIGNIFICANCE` (default observe).

### `sql/bigquery/86_v_outcome_sector.sql` (views 1.34.0 → **1.35.0**) and `scripts/learning_watch.py` 1.2.0 → **1.2.1**
- The view gains `t_adj`: the mean over distinct recording dates (a date's calls averaged first) over its standard error, divided by SQRT of the horizon in days (7 / 14 / 30 / 91), as `v_forward_signal_gates` does. 'beats its market' and 'loses to its market' now also need `t_adj` of 2 or more either way. One new column, before `reading`.
- Learning Watch reads the new column (shown under t).
- **On the data of 27 Nov:** 2 of the 15 cells keep 'beats its market' (Other markets, Financials, 1W t 2.46 and 2W t 2.11); 13 read 'no clear edge' (t 0.22 to 1.87).

### `scripts/tfb_export_audit.py` 1.2.2 → **1.2.3**
- Row counts: with no `--expect` for a page, the expectation is the sync's own receipt (the Rows cell of the page's SUCCESS row in `_Status`). A page shorter than its receipt was cut after the write. `--expect Page=N` still wins; with neither there is no expectation. The CLI default is no longer the 3 Oct universe.
- Lateness: when `_Run_Log` holds the dispatcher's own accepted dispatches (`tfbDispatchDailySync`, `tfbDispatchDailySyncNow`, status OK), lateness is the time from each dispatch to the first row of the run it started (same bands: 15 minutes PASS, to 60 WARN, beyond FAIL). An accepted dispatch older than an hour with no run within 12 hours is a FAIL of its own. An export with no dispatch row is judged against `--cron` as before.
- **On the owner's export of 27 Nov:** 12 FAILs → 9. The four `.rows` checks pass against their receipts; lateness passes (worst 1 minute over 23 dispatches). The nine that remain are real findings.
- The selftest digest (`e4a1dd8650ad62f9`) and the pinned payload of the synthetic export are unchanged.

### Pins and CI
`scripts/verify_deployment.py`: run_shadow_scorer 1.9.8, learning_watch 1.2.1, apply_bq_views 1.35.0, tfb_export_audit 1.2.3. `ci.yml` 1.1.105: the two new test files join the blocking lists.

## Tests
- New `tests/test_s1_significance.py` (11): the net return from the index, the test and its floors, today's rows, a PASS that is not evidence, only a PASS is annotated, the real `evaluate_s1` gate keeps its six criteria, the token line, the mode reader, the wiring order in `main()`, and that no other script parses the 'why' text.
- `tests/test_s1_criteria_v2_p204.py` C8: the reading on the real 2026-10-05 Shadow_History rows (14 paired days → TOO_FEW_DAYS), and the REAL `main()` under observe (the default) against off: history rows, criteria, verdict and 'why' identical; the gate page, the verdict line and the _Run_Log JSON differ by the token only; a dry run prints it and writes nothing.
- New `tests/test_export_audit_v123.py` (9): receipts from SUCCESS rows only, a page held to its receipt, an explicit expectation wins, the dispatcher clock and its bands, a dispatch that started nothing, `check_runlog` on both paths.
- `tests/test_bq_views.py`: the view's new fragments, the body hash (moved to PENDING), a Python mirror of the reading and of t_adj. `tests/test_learning_watch.py`: the registry reads `t_adj`; no record without a t can put a row under PROVEN.
- Changed with a reason: version pins in seven S-1 and audit test files; `tests/test_workflow_flag_mappings.py` gains the new mapping.
- 85 related test files: 2,159 pass, 44 skipped. The changed files also pass with `GITHUB_ACTIONS=true`. Workflow audit: 0 errors, 0 warnings.
- 25 hand mutants, each failing a test: the 20-day floor, the coverage rule, the first row of a date, the epoch, annotating a non-PASS, annotating evidence, the default mode, gross instead of net return, a two-sided reading, evidence without coverage, the wiring in `main()`, the excluded count, unpaired days, the variance divisor; receipts ignoring the status, the receipt overriding `--expect`, the 2-minute window, the 12-hour and 60-minute limits, a refused dispatch counted, a lost dispatch ignored, the dispatcher path switched off; the t floor in the view, t over calls instead of dates, the registry column.

## After the merge
1. BigQuery views `--write` on main (Claude dispatches it), then `--verify`: `v_outcome_sector` must carry `t_adj`. Learning Watch reads the old view as unreadable until then; its next run is 12:35 Riyadh.
2. The next Shadow Scorer run writes the reading on S1_Gate. Read-back: the `[S1-SIGNIFICANCE` token in the run log and in the gate's meta cell.

## Owner
Merge when you have read it. Nothing to set.

## Rollback
- Scorer: repository Variable `TFB_S1_SIGNIFICANCE=off` (byte-identical to 1.9.7).
- View and Learning Watch: revert the two files and run views `--write`.
- Export audit: pass `--expect` and `--cron` explicitly, or revert to 1.2.2.

Not a licensed financial adviser; the owner decides and executes every trade.
