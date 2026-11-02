# Commit Sheet - `scripts/archive_to_bigquery.py` v1.0.0 + `.github/workflows/bq_archive.yml` + `ci.yml` 1.1.48 - 2026-10-27

**Scope:** program register **C8, Phase 1** - a read-only daily archive of the decision workbook's learning tabs in BigQuery. Owner decision 27 Oct 16:22 (BigQuery project created and shared); owner granted the sync's service account **BigQuery Data Editor + BigQuery Job User** on the project at 16:51.
**Files:** `scripts/archive_to_bigquery.py` (new), `.github/workflows/bq_archive.yml` (new), `tests/test_bq_archive.py` (new, 31 cases), `scripts/verify_deployment.py` (manifest pin 1.0.0), `.github/workflows/ci.yml` 1.1.48, this sheet.

## S1 - WHY
- Most learning surfaces are overwritten every day: the Top-10 board, `Calendar_Events`, the nightly `_Board_Scorecard`, `My_Portfolio`, `Portfolio_Decision`. The logs (`_Selection_Log`, `Signal_History`, `Performance_Log`, `_Run_Log`) live in a spreadsheet with a cell budget (34% of the 20M-cell cap used).
- The self-correcting program needs that history queryable over months: champion vs challenger, post-earnings tags against outcomes, scorecard drift. Looker Studio dashboards come after about four weeks of data.
- BigQuery project `project-2e3530e6-2311-4aa3-b36` ("My First Project"), dataset `tfb_archive` (US, created 27 Oct). Billing is enabled: a probe table carried no expiration, so tables are not deleted after 60 days.

## S2 - WHAT
- For each tab in `TABS` (or `--tabs`): one Sheets `values.get` (UNFORMATTED values, dates as the sheet's text). Each non-empty row becomes `{snapshot_date, captured_at, run_id, tab, row_num, cells[], record_json}` and is loaded into `tfb_archive.<table>$YYYYMMDD` with **WRITE_TRUNCATE**. A re-run the same day replaces only that day; every other day stays as it was.
  - `snapshot_date` = the UTC date of the capture.
  - `cells` = the row positionally and losslessly.
  - `record_json` = `{header: value}` (non-empty values only) for tabs with a header row, so SQL can use `JSON_VALUE(record_json, '$."Symbol"')`. Blank headers become `col_N`, duplicates `name_2`.
- Tables are created on first use, DAY-partitioned on `snapshot_date`. Every tab uses one fixed schema, so a column added to a tab never breaks a load.
- Tabs and header rows:

  | Header row | Tabs |
  |---|---|
  | 0 (multi-zone layout; `cells` only) | `Top_10_Investments`, `_Board_Scorecard`, `Portfolio_Decision`, `Performance_Log` (a summary block sits above / beside its table; its header row is set from the first archived copy) |
  | 1 | `_Selection_Log`, `_Shadow_Rank_Log`, `Calendar_Events`, `Signal_History`, `My_Portfolio`, `_Run_Log` |

- Caps: 250,000 rows and 400 columns per tab; rows over the cap are left out and counted in the log line.
- HTTP goes through google-auth's AuthorizedSession, with scopes `spreadsheets.readonly` and `bigquery`. BigQuery loads are resumable-upload load jobs, polled until DONE. No new library: google-auth and requests are pinned to the repo's own versions. The module imports neither, so `verify_deployment` and the lean tests can import it.
- **Read-only for the workbook:** nothing is written to it, and the live system never reads BigQuery.
- Exit codes: 0 all tabs done; 1 any tab failed (the others are still archived and named in the summary line); 2 configuration missing, or the dataset missing or not readable (e.g. 403 before the IAM grant).

## S3 - Gate / arming
| Item | Where | Value | Effect |
|---|---|---|---|
| `TFB_BQ_ARCHIVE` | script default | off | nothing read, nothing written |
| | `bq_archive.yml` | `${{ vars.TFB_BQ_ARCHIVE \|\| 'on' }}` | armed; kill with the repository Variable `TFB_BQ_ARCHIVE=off` |
| `TFB_BQ_PROJECT` / `TFB_BQ_DATASET` | `bq_archive.yml` | Variable or `project-2e3530e6-2311-4aa3-b36` / `tfb_archive` | target |
| schedule | `bq_archive.yml` | `5 23 * * *` (02:05 Riyadh) | daily; after the US close and the 22:23 UTC scorecard |
| dispatch | `bq_archive.yml` | `write` (default false), `tabs` | dry run unless `write=true` |

IAM is done (owner, 16:51). Merge is enough.

## S4 - Tests
- `tests/test_bq_archive.py` - **31 passed** (lean and full venvs). The REAL module runs against an offline fake of the Sheets, BigQuery and upload endpoints:
  - **A1:** gate off / blank / junk -> no request at all; missing configuration -> exit 2.
  - **A2:** dry run reads, checks the dataset, creates and loads nothing, and prints exact counts.
  - **A3:**
    - missing tables are created DAY-partitioned on `snapshot_date` with the fixed schema;
    - every load targets `table$20261027` with WRITE_TRUNCATE / CREATE_NEVER / NDJSON;
    - the uploaded rows are exact (blank row skipped, header row without a record, record_json mapping, blank and duplicate headers named, metadata);
    - a same-day re-run hits the same partitions and is polled until DONE;
    - `--date` and the full ten-tab list work.
  - **A4:** a tab the Sheets API rejects and a job with an errorResult -> those two fail, the third loads, exit 1. A missing dataset, or one the account cannot read (403), stops before any read (exit 2). An empty tab loads nothing.
  - **A5:** table names, cell text (integral floats, booleans, float noise), header names, the row and column caps, tab selection, NDJSON UTF-8, the import is side-effect free.
  - **A6:** the workflow's schedule, permissions, arming, defaults, credentials and dry-run default.
- Manifest pins + workflow regressions: 368 passed. `scripts/audit_repository_workflows.py`: errors 0, warnings 0.

## S5 - Rollback
Repository Variable `TFB_BQ_ARCHIVE=off` (the next run reads and writes nothing), or delete the workflow. Archived partitions stay in BigQuery; nothing in the workbook changes either way.

## S6 - Known limits / next
- Storage estimate: about 2M cells a day (Signal_History, Performance_Log and _Run_Log dominate), roughly 20-30 MB a day, about 7-10 GB a year. That is inside the 10 GiB free tier for the first year; beyond it, cents a month.
- Phase 2: typed views per tab (`v_selection_log`, `v_signal_history`, ...) flattening `record_json`, then Looker Studio learning dashboards after about four weeks of data.
- Market pages (Global_Markets about 6,600 x 120) are not archived in Phase 1.

## S7 - Operator steps
1. Merge.
2. Claude dispatches `🗄️ BigQuery Archive` with `write=true` once and reads the tables back. After that it runs nightly at 23:05 UTC.

Not a licensed financial adviser; the owner decides and executes every trade.
