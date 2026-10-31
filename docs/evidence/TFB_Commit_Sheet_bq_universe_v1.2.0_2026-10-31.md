# Commit Sheet: BigQuery data for Looker pages 3-6 (archive_to_bigquery 1.1.0, apply_bq_views 1.2.0), 2026-10-31

**Scope:** the owner asked on 31 Oct 2026 for four more Looker pages:
- a World Top-25 ROI table that follows the page's selections;
- a My Portfolio page and a Top-10 page with the evidence that makes a decision more reliable;
- a deep-dive page for one symbol, typed as a symbol or a company name inside a chosen market.

This PR is the data layer those pages read. It is read-only:
- nothing is written to the workbook;
- no decision, gate, floor or executable count changes;
- no broker call is made.

The Looker pages themselves are built in the report after the merge.

## Changes
| Change | Why | What | Tests | Deployed/installed version | Effective mode | Live acceptance | Remaining dependency |
|---|---|---|---|---|---|---|---|
| Archive 7 more tabs | The market pages and the news tabs are overwritten by each sync (the news tabs roll over), so nothing keeps their history. | `scripts/archive_to_bigquery.py` 1.0.0 -> 1.1.0. The new TABS, all with header row 1: Market_Leaders, Global_Markets, Mutual_Funds and Commodities_FX (one 115-column layout), plus _News_Items, _News_Gate and _Econ_Events. The read timeout goes from 120 to 300 s (Global_Markets is about 800k cells). The contract is unchanged: one partition per day, WRITE_TRUNCATE. | A7 (new): checks the tab list and that a 115-column market row keeps every cell. The count test now expects 17 tables. | Not installed. The job runs from `main`, nightly at 23:05 UTC. | Default on (the existing workflow). | Dispatch `bq-archive` once with `write=true` and the 7 tabs. That creates the 7 tables with today's partition. | Owner merge. |
| Typed views | Looker needs typed columns with stated units. | `scripts/apply_bq_views.py` 1.1.0 -> 1.2.0 adds the 7 source tables and 13 objects (listed below). | V1 to V17 (V14 to V17 are new); V4 pins the 13 file hashes. | Not installed. | — | Dispatch `bq-views` with `--write`. `--verify` must then report the 13 new bodies equal. | The archive dispatch above must run first. |

## Objects (sql/bigquery)
| File | Kind | For | Notes |
|---|---|---|---|
| 03_f_col, 04_f_at | functions | every wide-tab view | One header lookup per snapshot, then a read by position. `f_at(c, f_col(h, name)) = f_cell(h, c, name)`, checked in V17. |
| 05_f_ts_utc | function | market and news time stamps | Reads a time as UTC; an offset in the text wins. |
| 50_v_universe | view | all pages | Every market-page row for each snapshot, with all 115 columns typed. Units are described below the table. |
| 51_v_universe_latest | view | Top-25, the symbol picker | The newest row of every symbol. `meets_floors` = reliability >= 70, DQ >= 80 and no block reason (the Top-10 panel's defaults). |
| 52_f_universe_return | table function | Top-25 "realized in period" | Realized return over the Looker date range, from the archived prices. `covers_window` says whether the archive reaches the start. |
| 53-56 v_news_items, v_econ_events, v_news_gate, v_calendar_events | views | the evidence pages and the symbol page | One row per news item, economic event and gate run. Calendar rows come per snapshot, with an `is_latest` flag. |
| 57_v_top10_insight | view | Top-10 page | The latest board plus the `mkt_*` evidence, events, 7-day news, past episodes and `caution_flags` (plain words). |
| 58_v_portfolio_insight | view | My Portfolio page | The latest holdings plus the latest Portfolio_Decision row, the `mkt_*` evidence, events, news and `caution_flags`. |
| 59_f_symbol_pick | table function | symbol page | Matches a symbol, a ticker, a company name, or every word of a name, inside an optional market (country, region, exchange or suffix). The best match is first; the top 20 are kept. |

**v_universe units:**
- **`*_pct` columns:** all in percent points. The registry's pct columns are fractions, so they are multiplied by 100.
- **Recomputed from prices:** change_pct, expected_roi_*_pct and the two upsides. The sheet's value is used only when a price is missing.
- **`market_code`:** the listing suffix.
- **`region`:** named as on the market-series page, so one control can filter both.
- **`market_country`:** the country of the listing, not of the company.

## Tests
**Suite totals:** 109 passed. The run covered:
- test_bq_views
- test_bq_archive
- test_archive_market_series
- test_verify_manifest_pins
- test_ci_required_verdict
- test_repository_workflow_audit
- test_workflow_audit_repairs

**Other checks:**
- `audit_repository_workflows.py`: errors=0, warnings=0.
- The CI ruff rules (E9, F63, F7, F82) pass.
- `apply_bq_views.py --dry-run`: 29 objects, and the static checks pass.

**V14:** every header a new view reads is one its writer declares:
- the four market pages, through `core/sheets/schema_registry.get_sheet_headers`;
- `news_macro_gate.ITEMS/GATE/ECON_HEADER`;
- `run_calendar_sync.HEADERS + LAST_REPORT_HEADERS`.

The tables they read are exactly the archive's new tabs.

**V15:** checks units and regions:
- Every one of the 17 registry pct columns is either multiplied by 100 or recomputed from prices.
- A 0-100 points column is never multiplied by 100.
- Every listing suffix on the live pages (31 Oct) has exactly one region and a listing country.

**V16:** checks the table-function signatures, the documented custom queries, and the match ranks 1-7.

**V17:**
- The evidence views read typed views only.
- The portfolio's bearish calls are v_performance_log's own list.
- The caution flags are plain-word labels.

**Golden negatives:** every mutant below fails the suite.
- a pct column read without the multiplication by 100;
- DQ multiplied by 100;
- a suffix dropped from a region;
- a misspelt header;
- the bearish list cut short;
- a raw table read in an evidence view;
- a tab removed from the archive;
- a picker limit of 200;
- a different forecast horizon (caught by the hash pin).

## Post-merge order (agent)
1. Dispatch `bq-archive` with `write=true` and `tabs=Market_Leaders,Global_Markets,Mutual_Funds,Commodities_FX,_News_Items,_News_Gate,_Econ_Events`. This is read-only for the workbook.
2. Dispatch `bq-views` in write mode, then in verify mode.
3. Check the data:
   - row counts per page;
   - region 'Other' share;
   - derived vs sheet ROI (sheet x 100);
   - the floors' pass rate;
   - partition pruning on `v_universe_latest`.
4. Build the four Looker pages.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
