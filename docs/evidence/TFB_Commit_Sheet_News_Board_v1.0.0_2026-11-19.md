# Commit Sheet - `scripts/news_board.py` v1.0.0, the News_Board page (news learning loop, phase B) - 2026-11-19

**Scope:** owner, Thu 19 Nov 2026. 13:06 Riyadh: "build the new news and it's effect in the file". 14:30, after merging #1030 and #1031: "done also move to the next stage". Phase A put the news themes, the self-correcting sector weights, the shadow list and the trial into BigQuery. Nothing of it was in the workbook.

**Files:**
- `scripts/news_board.py` (new, v1.0.0): reads the five news views and writes one page, `News_Board`.
- `.github/workflows/news_board.yml` (new, 1.0.0): 07:55 and 16:55 Riyadh, and by hand (dry run on request).
- `tests/test_news_board.py` (new): 14 tests, invented figures only.
- `scripts/verify_deployment.py`: `("news_board", "1.0.0", "news board")` pinned on arrival.
- `.github/workflows/ci.yml` 1.1.87 -> 1.1.88: `tests/test_news_board.py` in the blocking lean lane.

## S0 - Evidence (live, 19 Nov 2026, the script's own seven queries run as written)
- The news archive ends on 18 Nov with 14 themes; the one-off history fill of the same day loaded 2 Oct 2023 to 18 Nov 2026.
- A sector tilt is in force for 17 to 23 Nov (news week of 9 Nov) in both arms; the week before has its realized result.
- The shadow list of 19 Nov: in each arm the tilt would move 28 names out and 28 in.
- Record of the tilt before the rule was fixed, 71 weeks: favoured minus avoided +0.24 pp a week (t 0.79) on political events, +0.23 (t 0.91) with the news themes. Not better than chance.
- The trial view has no forward day yet (it starts with the list of 20 Nov).

## S1 - WHAT the page shows
0. **Verdict:** where the forward trial stands, in one or two lines, with a warning when the news archive is more than 3 days old or no tilt is in force.
1. **World news:** each theme's last full week against its own previous four weeks, how unusual that is against a year of its own weekly changes, and the running week so far. Themes are grouped as War, Politics, Economy, Technology, Markets.
2. **Sector tilt:** the eleven US sectors, favoured / neutral / avoided for both arms, the themes with a working weight, last week's tilt and what the sector then did; and the record of the tilt, past weeks apart from forward weeks.
3. **Holdings:** each holding of `My_Portfolio` beside its sector's tilt, and whether the shadow list would keep it. A holding outside the US is marked "indicative only": the tilt is measured on US sector funds.
4. **Shadow list:** the names the tilt would move out of and into the investable list on the latest list day, both arms side by side. It says it is not an order. When that day lies outside the window of the tilt shown in section 2 (a new tilt began today and the list is yesterday's), the page says so and claims no shadow action for a holding.
5. **Trial:** `v_trial_news_overlay` as it stands, and the promotion rule fixed on 19 Nov.

## S2 - What does not change
- No recommendation, score, list or order changes. The page is analysis; only the Today Action Plan gives orders.
- BigQuery is read with SELECT only, through the Evidence Board's reader (`bq_query`, which refuses anything else).
- The workbook: one read (`My_Portfolio!A1:B200`, symbols and names) and one write (its own tab). No other tab is touched.
- The holdings' symbols are not printed in the run log (the repository and its logs are public).
- A failed read, query or write ends the run with exit 2 and leaves the previous page in place. A Sheets quota answer is waited out (65 s, twice at most), for the read and for the one write.
- Off switch: repository variable `TFB_NEWS_BOARD=off`.

## S3 - Cost
A run reads about 1 GB in BigQuery (the sector view and the trial view are the large ones). Two runs a day on purpose: about 60 GB a month.

## S4 - Not in this version
- Tracking of named people, the calendar of political events, weights per market (Tadawul, Europe) and the company / sector / country / world level on each news item.
- The page has no colours or column widths yet (plain values, like the Evidence Board).
- The record of the tilt on the page counts weeks that have both a favoured and an avoided sector (71), so its figure differs a little from the final test of the same day (75 weeks, +0.15 and +0.17). Both read the same: not better than chance.

## S5 - Tests
- `pytest tests/test_news_board.py` run 3 times: 14 passed. The full runs go through the real BigQuery reader on a fake session that answers as the REST API does, and a fake workbook.
- With the Evidence Board, Sheets quota, manifest pins, news themes, news gate, CI verdict, Today Action Plan and Holding_Read suites: 258 passed. Workflow suites: 154 passed. `tests/test_bq_views.py`: PASS 37/37.
- Mutants: 57 of 57 caught (the reading thresholds and words, the week in force, the Riyadh date, stale and not-in-force warnings, the list day outside the tilt's window, forward against past, the symbol and date guards, the dry run, the off switch, the tab written, the quota wait, holdings kept out of the log); the file restored byte-identical after each.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S6 - Deploy and rollback
- **Deploy:** merge. Then a dry run of **News Board** by hand, then a real run; the tab `News_Board` is created by the first run. Nothing to paste.
- **Rollback:** repository variable `TFB_NEWS_BOARD=off` (the page stays as last written), or revert the commit and delete the tab.

## S7 - Review finding (Codex P2 on #1032, verified and fixed)
- **Finding:** the holdings reader kept a row only when its symbol had an exchange suffix, so a plain ticker (AAPL, QQQ) would have been dropped from section 3 without a word.
- **Verified:** true. `core/data_engine.py` lists plain tickers and `GC=F` among the default `My_Portfolio` symbols, and the engine keeps a symbol without a suffix as it is typed. The suffix test was there only to pass over a totals row.
- **Fix:** every row under the `Symbol` header is a holding when it reads as a symbol (plain tickers, futures such as `GC=F` and indices such as `^GSPC` included); only the labels TOTAL, TOTALS and SUBTOTAL are passed over. A plain ticker is looked up in the archive under its own name and under `TICKER.US`. Only names made of letters, digits, dots and dashes are sent to BigQuery; a future is shown with "sector not known to the archive".
