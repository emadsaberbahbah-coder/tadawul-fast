# Commit Sheet - archive_market_series v1.0.0 + apply_bq_views v1.1.0: the global markets page (C8 Phase 2b) - 2026-10-29

**Scope:** owner request 29 Oct 11:29 Riyadh: the first page of the Looker Studio report must show how the world's exchanges move - the US, Europe, China and the Gulf (and more) - the trend over the year, the average return of each market, one market alone or several compared, sectors, and any period (one day, a week, a month, custom). The workbook holds instrument snapshots, not index histories, so this change adds a small nightly history of reference series in BigQuery and the SQL the page reads. **Read-only for the system:** nothing is written to the workbook, the live system never reads BigQuery, and no decision, gate, threshold or executable count changes.

**Stacked on #878** (apply_bq_views 1.0.0, merged 29 Oct as a3ccb9d1; main merged back into this branch, identical tree). Also carries the two fixes the Codex review of this PR found in #878's views (Review section).

**Files:**
| File | Version | Change |
|---|---|---|
| `scripts/archive_market_series.py` | NEW 1.0.0 | Yahoo chart API daily bars of 48 series -> `tfb_archive.market_series_daily`, one integer-range partition per `series_id`, WRITE_TRUNCATE per series; gate `TFB_BQ_MARKETS` |
| `.github/workflows/bq_markets.yml` | NEW 1.0.0 | nightly 22:35 UTC (writes); manual dispatch is a dry run unless `write=true`; `symbols` / `days` inputs; contents: read |
| `sql/bigquery/40_v_market_daily.sql` | NEW | daily returns per series |
| `sql/bigquery/41_f_market_window.sql`, `42_f_market_summary.sql` | NEW | table functions for the Looker date range |
| `scripts/apply_bq_views.py` | 1.0.0 -> 1.1.0 | accepts `CREATE OR REPLACE TABLE FUNCTION` (body = the query after the `AS` line, as `ROUTINES.routine_definition` keeps it); `market_series_daily` is a source table; scalar functions still before every view and table function; `CREATE TABLE` still refused |
| `tests/test_archive_market_series.py` + `tests/fixtures/bq_market_series_table_2026-10-29.json` | NEW | 11 cases (M1-M11) |
| `sql/bigquery/11_v_performance_log.sql`, `21_v_portfolio_decision.sql` | fixed (review) | learning flags score the CALL like track_performance's summary; the SENT IBKR note is recognized |
| `tests/test_bq_views.py` | extended | 16 objects, TVF body extraction, new refusals, V11 (market objects), V12 / V13 (review fixes) |
| `.github/workflows/ci.yml` | 1.1.59 -> 1.1.60 | the new harness in the blocking lean lane after `test_bq_views.py` |
| `.github/workflows/bq_views.yml` | comment only | names the new objects |
| `scripts/verify_deployment.py` | pins | `apply_bq_views 1.1.0`, `archive_market_series 1.0.0` |

## Series (permanent ids, pinned by M1)
| Kind | Series |
|---|---|
| Index (local currency, price return) | 1 S&P 500, 2 Nasdaq Composite, 3 Dow Jones; 4 Euro Stoxx 50, 5 FTSE 100, 6 DAX 40, 7 CAC 40; 8 Shanghai Composite, 9 Shenzhen Component, 10 CSI 300, 11 Hang Seng; 12 Nikkei 225; 13 TASI |
| Market ETF (USD, total return) | 20 SPY, 21 VGK (Europe), 22 MCHI (China), 23 EWJ (Japan), 24 KSA, 25 UAE, 26 QAT, 27 KWT, 28 ACWI (world), 29 EEM (emerging) |
| US sector ETF (USD) | 40-50: XLK, XLF, XLE, XLV, XLI, XLY, XLP, XLU, XLB, XLRE, XLC |
| World sector ETF (USD) | 70-80: IXN, IXG, IXC, IXJ, EXI, RXI, KXI, JXI, MXI, REET, IXP (same 11 sector names as the US set) |
| Commodity (USD, front month) | 60 Brent, 61 WTI, 62 Gold |

The Gulf beyond Saudi Arabia comes in through the USD ETFs (UAE, Qatar, Kuwait): Yahoo has no dependable daily index for those venues. The USD rows make every market comparable in one currency (SAR is pegged).

## SQL objects (dataset `tfb_archive`)
| Object | Grain | What it gives the page |
|---|---|---|
| `v_market_daily` | series x trading day | `return_pct`, `log_return_pct` (vs the previous day of the same series), up / down day, `ytd_return_pct`, week (Monday) / month / year; `return_price` = dividend-adjusted close when the series has one on every row, else the close |
| `f_market_window(start_date, end_date)` | series x day in the window | `cum_return_pct` and `index_100` from the window start (the last close BEFORE `start_date`, so one day = that day's move), `drawdown_pct` from the window high, last-day flag |
| `f_market_summary(start_date, end_date)` | one row per series | `period_return_pct`, trading / up / down days, `up_day_share_pct`, average / best / worst day, `volatility_ann_pct`, `max_drawdown_pct`, last close; region and kind averages are plain averages of these rows |

Looker reads the table functions through a custom query with date range parameters: `SELECT * FROM \`project.tfb_archive.f_market_window\`(PARSE_DATE('%Y%m%d', @DS_START_DATE), PARSE_DATE('%Y%m%d', @DS_END_DATE))`. Any grouping of daily rows compounds exactly with the calculated field `(POWER(2.718281828459045, SUM(log_return_pct) / 100) - 1) * 100` (weekly / monthly heat map).

## S1 - Evidence (29 Oct, via the BigQuery connector)
- **Table:** `market_series_daily` created empty with exactly the script's `SCHEMA` (column order, types, modes) and `RANGE_BUCKET(series_id, GENERATE_ARRAY(1, 1001, 1))`; `tables.get` saved as the fixture and `schema_problems(fixture) == []` (M6). The loader checks the live layout before any load and refuses a different one (exit 2).
- **Objects:** the three new objects created from these exact files; `INFORMATION_SCHEMA` read back: **16/16 objects match** the file bodies (V4 pins), including both table functions (`routine_type = TABLE FUNCTION`). The empty table answers `0 / 0 / 0` rows through the view and both functions.
- **Logic check** (read-only query, the three bodies over 11 inline rows): index 100 -> 102 -> 101 -> 105 -> 103 (30 Dec .. 6 Jan): daily returns +2.000 / -0.980 / +3.960 / -1.905 %; YTD from the 31 Dec close -0.980 / +2.941 / +0.980 %; window 1-6 Jan starts from 102 (31 Dec): cumulative -0.980 / +2.941 / +0.980 %, drawdown -0.980 / 0 / -1.905 %; summary period +0.9804 %, 3 days, 1 up / 2 down (33.33 %), best +3.9604, worst -1.9048, max drawdown -1.9048; a one-day window (5 Jan) returns that day's move +3.9604 %. An ETF with adjusted closes on every row uses them (+2.04 / +3.00 %); one adjusted close missing switches that series to plain closes (+10.0 / +9.09 %). A series that starts inside the window starts from its first close.

## S2 - Tests
| Case | Proves |
|---|---|
| M1 | 48 series, ids unique, permanent (pinned map) and in 1..1000; symbols / names unique; kinds / regions declared; sector ETFs name a sector; non-index series are USD |
| M2 | venue-local dates (gmtoffset), null / zero closes skipped, last bar of a repeated date wins, adjclose / volume, sorted; malformed payloads empty |
| M3 | the bars of a session still open at run time are left out; closed session or bad period keeps all |
| M4 | query1 then query2, 404 final, encoded symbol (`%5E`, `%3D`) and the window in the URL |
| M5 | exactly the SCHEMA columns, blanks as NULL, required fields set, NDJSON |
| M6 | the live layout passes; a changed mode / type / column / partitioning is refused; creation is range-partitioned |
| M7 | each series loads into `market_series_daily$<id>`, WRITE_TRUNCATE + CREATE_NEVER; a series failing once is retried and loaded; failing twice stays untouched, the others load, exit 1; a job errorResult counts |
| M8 | dry run: one dataset GET, no table call, no load |
| M9 | gate off -> no request; missing project / bad days / unknown symbol / missing dataset / no key / wrong layout -> exit 2 |
| M10 | the workflow: schedule, dry-run dispatch default, contents: read, armed gate with kill switch, pinned actions |
| M11 | no Sheets API, no DML, the only write is the per-series load |
| V1-V11 | (test_bq_views) 16 objects in order, TVF body extraction, `CREATE TABLE` / unknown table / function-after-TVF refused, a TVF after its view accepted, V11 market objects read only SCHEMA columns and earlier objects |
| V12 | `v_performance_log` scores the call from the 8 tiers' signs read out of `track_performance.py` (loader default HOLD, so HOLD / EXIT / TRIM neutral), `is_scored` = decided, `win_flag` only for decided, one record per Key; fails on the first cut |
| V13 | `pd_ibkr_evidence` labels all four notes `pdIbkrNote_` writes (read from `apps_script/10_My_Portfolio.gs`); the first cut's pattern never matched `SENT (` |

Runs (after the review fixes): `python tests/test_bq_views.py` -> PASS 13/13 (V12 / V13 / V4 fail on the first-cut SQL, proven); `pytest tests/test_archive_market_series.py tests/test_bq_views.py tests/test_bq_archive.py tests/test_verify_manifest_pins.py tests/test_ci_required_verdict.py tests/test_repository_workflow_audit.py tests/test_workflow_audit_repairs.py` -> 98 passed; `python tests/test_bq_views.py` -> PASS 11/11; `audit_repository_workflows.py` errors=0 warnings=0; ruff E9/F63/F7/F82/F401/F841 clean; compileall clean.

## Review (Codex on this PR, 2 x P2, both fixed here)
1. **`v_performance_log` scored the price, not the call.** The sheet's `Outcome` is a price-direction label (WIN = the price rose); `track_performance` keeps it as written and scores the CALL for its summary (`_record_outcome` -> `_directional_outcome`: sign of the 8-tier recommendation x realized ROI; HOLD neutral; win rate = wins / (wins + losses)). The first cut counted 8,532 matured HOLD rows as scored and every SELL whose price fell as a loss. Now: `call_outcome` (WIN / LOSS / BREAKEVEN / NULL), `is_scored` = decided, `win_flag` 1 / 0 only for decided calls, `in_outcome_cohort` = first valid matured record per Key; `outcome` stays the sheet's label. **Live parity, 29 Oct:** over `in_outcome_cohort` the view gives 12,286 records, 1,967 wins, 1,608 losses, 37 breakeven, 8,674 neutral, win rate 55.02 % - exactly the sheet's own panel (Matured Outcomes 12,286; Duplicate excluded 27; Directional Wins 1,967; Losses 1,608; breakeven 37; HOLD / neutral 8,674; Directional Win Rate 55.0 %, denominator 3,575).
2. **`pd_ibkr_evidence` never recognized `SENT`.** `pdIbkrNote_` writes `ibkr evidence SENT (capture ...)`; the first-cut pattern needed `:` or `|` right after the status. Now a CASE labels all four notes: SENT / NOT SENT (enforce), WOULD SEND / WOULD NOT SEND (observe). The archive so far holds only `NOT SENT` nights (27-29 Oct), unchanged.

Both views re-applied on 29 Oct through the BigQuery connector and read back: hashes match the files (V4 pins `c8f644b5...` and `98b6090d...`); 16/16 objects match.

## S3 - Owner steps (after merge)
None required. Claude dispatches **BigQuery Market Series** with `write=true` once (the first fill, about 36,000 rows), runs **BigQuery Looker Views** `verify`, and builds the global markets page as page 1 of the report (the existing learning page moves to page 2). From then on the nightly schedule refreshes every series. Kill switch: repository Variable `TFB_BQ_MARKETS=off`.

## S4 - Rollback
Revert the PR; `TFB_BQ_MARKETS=off` stops the job at once. The table and the three objects are independent of every other archive table and view (dropping them affects only the markets page).

## Known limits / deliberate cuts
- Yahoo is an unofficial, unauthenticated source (the same one the board scorecard uses nightly); a series it does not answer keeps yesterday's rows and turns the run red (exit 1) so it is seen. Personal, non-commercial use.
- Index levels are price returns in local currency; the USD ETFs are total returns. The page labels both, so a local-currency index is not compared against a USD ETF by accident.
- Commodity futures are front-month continuous series (roll days included).
- Local holidays mean different trading days per venue; window returns are always from each series' own last close before the window.
- No Saudi sector history yet (Yahoo carries no dependable Tadawul sector indices); the sector view covers US and world sectors.

Not a licensed financial adviser; the owner decides and executes every trade. This change places no broker order and changes no executable count.
