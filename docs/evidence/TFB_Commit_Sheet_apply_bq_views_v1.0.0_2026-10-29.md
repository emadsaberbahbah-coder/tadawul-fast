# Commit Sheet - apply_bq_views v1.0.0: typed BigQuery views for Looker Studio (C8 Phase 2) - 2026-10-29

**Scope:** owner request 29 Oct 10:12 Riyadh ("can we also build Google Looker Studio"). The nightly archive (`archive_to_bigquery` v1.0.0, C8 Phase 1, three good loads 27-29 Oct, ~84 k rows a day) keeps every learning tab as raw rows (`cells[]`, `record_json`). Looker Studio needs typed columns, one row per record and stated units. This change adds the SQL layer and the tool that applies or verifies it. **Read-only for the system:** views and functions only, no table is created, changed or deleted, nothing is written to the workbook, and the live system never reads BigQuery. No gate, threshold, decision path or executable count changes.

**Files:**
| File | Version | Change |
|---|---|---|
| `sql/bigquery/00..02_f_*.sql` | NEW | helper functions `f_cell` (cell under a header name; blank / dash -> NULL), `f_num` (number in a cell), `f_ts` (Riyadh time stamps, both writers' formats) |
| `sql/bigquery/10..31_v_*.sql` | NEW | ten views (below) |
| `scripts/apply_bq_views.py` | NEW 1.0.0 | `--dry-run` (default: static checks + plan), `--write` (create or replace in file order, then verify), `--verify` (INFORMATION_SCHEMA definitions vs the files, SHA-256) |
| `.github/workflows/bq_views.yml` | NEW 1.0.0 | manual dispatch only, `mode=verify` (default) or `write`; contents: read |
| `tests/test_bq_views.py` + `tests/fixtures/bq_archive_headers_2026-10-29.json` | NEW | 10 cases (V1-V10) |
| `.github/workflows/ci.yml` | 1.1.58 -> 1.1.59 | the harness in the blocking lean lane next to `test_bq_archive.py` |
| `scripts/verify_deployment.py` | pin | `apply_bq_views 1.0.0` |

## Views (dataset `tfb_archive`)
| View | Source | Grain | Units / notes |
|---|---|---|---|
| `v_board_episodes` | `_Board_Scorecard` EPISODES table, latest snapshot | one row per Top-10 seat episode (479 since 20 Jun) | returns in percent points to the k-th session close; `vs SPUS` / `vs Ref` in pp; win / beat flags |
| `v_performance_log` | `Performance_Log`, all snapshots | latest version per Record ID (25,593) | realized / unrealized ROI in pp; **`target_roi_pp` recomputed from Target / Entry price** because the sheet's `Target ROI %` mixes fractions (14,664 rows), percent points (9,445 undeclared + 1,444 declared) and 40 inconsistent rows; the raw cell stays as `target_roi_sheet` with `target_roi_sheet_unit` |
| `v_selection_log` | `_Selection_Log`, all snapshots | latest version per seat row (3,151 seats, 558 board runs) | board ROI in pp; `board_output` / `board_status` parsed from the run line |
| `v_signal_history` | `Signal_History`, all snapshots | latest version per Snapshot ID (33,910) | the sheet's `Key` is symbol|date and repeats within a day, so it is NOT the row key |
| `v_top10_board` | `Top_10_Investments` main table, every snapshot | one row per board row and night | seated rows carry `seat_rank`, grace / fast-track rows NULL; an honestly empty board has no rows |
| `v_top10_status` | `Top_10_Investments` status line | one row per night | status, output, source readiness, rows / seats / funded seats (includes empty nights) |
| `v_portfolio_decision` | `Portfolio_Decision` holdings table, every snapshot | one row per holding and night | SAR valuations; the table ends before the "excluded" table |
| `v_my_portfolio` | `My_Portfolio`, every snapshot | one row per holding and night | `Actual Weight %` in pp; the sheet's `Expected ROI *` are fractions (kept as `*_sheet`); `expected_roi_*_pp` recomputed from forecast price / current price |
| `v_run_log` | `_Run_Log`, all snapshots | each distinct row once (20,520 since 24 Aug) | `message_tag` / `message_version` from the leading `[TAG vX]`; error / warning flags |
| `v_archive_health` | the ten archive tables | rows and capture time per tab and day | the first thing the health page shows |

## S1 - Evidence (read from BigQuery, 29 Oct)
- Archive: 3 partitions x 10 tables: 83,295 / 83,732 / 84,846 rows, loads 23:10 (dispatch), 05:08, 05:20 Riyadh (the 23:05 UTC cron fires ~3 h late).
- Cockpit tabs (`Top_10_Investments`, `_Board_Scorecard`, `Performance_Log`, `Portfolio_Decision`) have no `record_json` (multi-zone layouts), so the views find each table's header row by its column names in every snapshot and read cells by header name (`f_cell`) - a column moving does not break a view, a renamed column reads NULL.
- Unit checks: board ROI, scorecard returns, Performance_Log realized ROI and My_Portfolio weights are percent points; Performance_Log `Target ROI %` is mixed (table above); My_Portfolio `Expected ROI` is a fraction (FRST forecast 15.40 vs 14.79 = 4.1 % = 0.041).
- Read-back after creation: every view returns rows and the counts match the sheet (Performance_Log `Total Records 25593`; scorecard `episodes 479`; Selection_Log 3,152 rows incl. header).

## S2 - Installed state
All 13 objects were created on 29 Oct (owner's BigQuery, via the BigQuery connector) from these exact files; INFORMATION_SCHEMA read back and hashed: **13/13 match** the file bodies (V4 pins the hashes; `v_performance_log` re-applied after the review fix below and read back again). After merge, `bq_views.yml mode=verify` proves the same from main with the service account.

## S3 - Tests
| Case | Proves |
|---|---|
| V1 | 13 objects in apply order (3 functions, then 10 views); static checks clean |
| V2 | every header a view reads (150+ names) exists in that tab's archived header row (2026-10-29 fixture) |
| V3 | the placeholder is fully replaced; bad project / dataset ids refused |
| V4 | object bodies hash to the INFORMATION_SCHEMA read-back (installed pins); body extraction for views and functions |
| V5 | refuses DELETE, a hard-coded project, a forward reference, two statements, a function after a view, a misnamed file |
| V6 | dry run creates nothing; `--write` / `--verify` without credentials exit 2; `--write` applies in order and verifies; a failing statement stops the run (exit 1); `--verify` reports a changed or missing object (exit 1) |
| V7 | the workflow is dispatch-only, contents: read, verify by default, runs the script in the chosen mode |
| V8 | no Sheets API in the script or the SQL; every object is a view or a function |
| V9 | `entry_stance` mirrors `track_performance._reco_trade_sign` read from its source (STRONG SELL / EXIT / TRIM bearish, HOLD neutral, anything else `unknown`); fails on the first cut's mapping |
| V10 | `--dry-run` stays offline without a key; with a key every statement goes to BigQuery as a dry run in order; a broken key exits 2 |

Runs: `python tests/test_bq_views.py` -> `PASS 10/10`; with the manifest pins, archive, workflow-audit and CI-verdict suites: 67 passed. `audit_repository_workflows.py` errors=0 warnings=0; ruff E9/F63/F7/F82/F401/F841 clean; compileall clean.

## S4 - Owner steps (after merge)
1. None required for the views (already installed). Optional proof from main: Actions -> **BigQuery Looker Views** -> Run workflow (mode `verify`).
2. Looker Studio: open the report link Claude sends and press Save; the data sources are these views.

## S5 - Rollback
Views are logical: `DROP VIEW` / `DROP FUNCTION` (or ignore them); revert the PR. The archive tables are untouched either way.

## Review
Codex on the first cut: 2 findings, both fixed in this PR - (1) `STRONG SELL` fell through to `neutral` in `entry_stance` (the producer serializes it and treats it as sell-family) -> the stance lists now mirror `_reco_trade_sign` exactly (V9); (2) `--dry-run` never created a session, so the documented BigQuery dry-run never ran with credentials -> a configured key now creates one (V10).

## Known limits / deliberate cuts
- Cost grows with history: the all-snapshot views scan every partition (today ~10-45 MB per query; about 1.3 GB at 90 days for `v_run_log`). Inside the 1 TB/month free tier at the current viewing rate; a nightly materialized table is the planned fix when it matters.
- `v_board_episodes` reads only the latest scorecard snapshot (the scorecard recomputes every episode nightly).
- S-1 tabs (`Shadow_History`, `S1_Gate`) and `_Status` are not archived yet, so the S-1 counter is not in v1.

Not a licensed financial adviser; the owner decides and executes every trade. This change places no broker order and changes no executable count.
