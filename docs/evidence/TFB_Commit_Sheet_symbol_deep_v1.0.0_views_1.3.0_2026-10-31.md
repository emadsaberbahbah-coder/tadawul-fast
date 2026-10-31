# Commit Sheet: symbol deep data (archive_symbol_deep 1.0.0, apply_bq_views 1.3.0), 2026-10-31

**Scope.** On 31 Oct 2026 at 22:12 the owner asked for:
- candle charts in My Portfolio;
- news in My Portfolio;
- an investor recommendation rating "like IBKR", built "from different institutions or make our own one";
- the deep data, analysis, news and candle charts for one selected symbol.

This PR is the data layer for that request. It is read-only for the workbook and the live system:
- no decision, gate, floor or executable count changes;
- no broker call.

The Looker pages are built after the merge.

## Changes
| Change | Why | What | Tests | Installed | Mode | Live acceptance | Dependency |
|---|---|---|---|---|---|---|---|
| NEW `scripts/archive_symbol_deep.py` 1.0.0 | No symbol has price history before 31 Oct 2026, a per-institution analyst count, or its own headlines. | Three loads, described below the table. | `tests/test_archive_symbol_deep.py` D1-D10 | Not installed (`main` only) | Nightly 00:20 UTC; dispatch is a dry run unless write=true | Dispatch write=true once. `symbol_bars_daily`, `symbol_analyst_daily` and `symbol_news` should then hold rows, and the log should show the guard line. | Owner merge |
| NEW `.github/workflows/bq_symbol_deep.yml` 1.0.0 | Runs the loader. | Schedule `20 0 * * *` (after the 23:05 UTC archive and EODHD's daily reset). Gates are on by default: `TFB_BQ_DEEP`, and `TFB_DEEP_EODHD` for the EODHD half. It uses the existing `EODHD_API_KEY` and archive service account. | D10; workflow audit errors=0 warnings=0 | - | - | - | - |
| `scripts/apply_bq_views.py` 1.2.0 -> 1.3.0 | Typed views for candles, analysts, news and the TFB rating. | `DEEP_TABLES` added to the source tables. Seven new views, two changed. | `tests/test_bq_views.py` V1-V18 (V18 new) | Not installed | - | Dispatch `bq-views` write, then verify. | The loader must run first (its tables must exist). |

**The three loads:**
1. **Bars.** Two years of daily OHLC from the Yahoo chart API, using the `archive_market_series` reader with its hourly fallback. This uses no EODHD quota.
   - It covers the deep set, at most 600 symbols: holdings, Top-10 names of the last 30 days, the 50 investable names with the highest expected 12M ROI, Market_Leaders, and any extras.
   - It replaces the whole table, and only when at least 80 % of the set answered.
2. **Analyst.** EODHD fundamentals with the AnalystRatings filter, for the core set of at most 120 symbols: holdings, board names, the top-25 ROI names, and extras.
3. **News.** EODHD news for the last 30 days, 20 items each, with sentiment, for the core set.

Loads 2 and 3 write one partition per day.

**The budget guard.** EODHD's daily limit is shared with the live sync (100,000 calls, exhausted once on 5 Jul 2026), so loads 2 and 3 are guarded:
- **Cost.** About 1,800 EODHD calls a night: 10 per fundamentals call plus 5 per news call, for about 120 symbols.
- **Check.** Before running, the job reads `/api/user`, which is free.
- **Skip.** It skips both loads, and so fails closed, when either holds:
  - usage so far plus the planned calls would pass 60 % of the daily limit;
  - the usage cannot be read.

## SQL objects (sql/bigquery)
| File | Object | What |
|---|---|---|
| 60 | v_symbol_candles | Daily candles, combining Yahoo bars with one nightly-snapshot candle per symbol. Nights identical to the previous one are dropped (weekends and holidays). Adds SMA 20/50/200 (NULL until full), change % and a 20-day average volume. |
| 61 | v_symbol_analyst | The latest consensus: the five counts and their shares. The score is recomputed from the counts (5 Strong Buy .. 1 Strong Sell), so its direction never depends on the provider's scale. Also the label, the target upside, and the change over about a month. |
| 62 | v_symbol_news | EODHD headlines (positive at polarity >= 0.25, negative at <= -0.25) plus the news gate's items, each with a `source_kind`. |
| 63 | v_symbol_rating | **The TFB rating** (described below the table). |
| 64-66 | v_dive_signals / v_dive_board / v_dive_events | The signal history, the board episodes and the newest calendar row, each with `symbol_label` so one picker filters every section. |
| 67, 68 (were 57, 58) | v_top10_insight / v_portfolio_insight | Both now carry the rating and its parts, the analyst consensus, and an "analyst consensus below Hold" caution. The portfolio's bearish-call flag now also matches the market pages' `STRONG_SELL` spelling. Before, "engine says STRONG SELL" never fired for it. |

**The TFB rating.** Six parts, each scored 0-100 and weighted over the parts that have data:

| Part | Weight | Source |
|---|---|---|
| Engine | 30 % | The engine's overall score |
| Analysts | 20 % | The analyst consensus, falling back to the sheet's rating |
| Target | 15 % | 50 + 2 x (upside - 10), held to 0..100 |
| Momentum | 10 % | The momentum score |
| News | 10 % | 50 + 50 x the 30-day average polarity |
| Risk | 15 % | 100 - risk |

- **Label and stars:** Very positive >= 75, Positive >= 60, Neutral >= 45, Negative >= 30, otherwise Very negative; 5..1 stars.
- **Confidence:** high, medium or low, from the data coverage and the floors.
- **Parts:** listed in plain words.
- It summarizes the evidence; it is not advice.

## Pre-merge checks
- **Live BigQuery run.** All new SQL ran against the real archived data, with sample rows standing in for the three new tables (31 Oct, 22:50 Riyadh):
  - 9,719 candles;
  - the AAPL consensus came out at 4.06, "Buy";
  - 123 news rows;
  - 9,737 symbols rated, for example 2222.SR 71.8 "Positive" with all six parts listed;
  - the insight joins resolved.
- **Read-back.** INFORMATION_SCHEMA hashes of the 13 objects from v1.2.0 (installed by bq-views run 41227078785) equal the reviewed files. They are now INSTALLED pins. The nine objects added or changed here are PENDING pins.
- **Tests.**
  - The suites ran 121 passed: views, archive, symbol deep, market series, manifest pins, CI verdict and the two workflow audits.
  - `audit_repository_workflows.py`: errors=0, warnings=0.
  - CI ruff rules: clean.
  - `apply_bq_views --dry-run`: 36 objects, static checks pass.
- **Golden negatives.** Each mutant below fails the suite:
  - in the loader:
    - the guard removed;
    - the 80 % rule removed;
    - the token not scrubbed;
    - the KS->KO map changed;
    - the guard set to fail open;
    - core-set duplicates allowed;
  - in the views:
    - rating weights summing to 1.05;
    - a moved label threshold;
    - an undeclared bars column;
    - the old STRONG_SELL test;
    - a dive view without `symbol_label`.

## Post-merge order (agent)
1. Dispatch `bq_symbol_deep` with write=true and check the log: set sizes, bars n/n, the guard line, and the analyst and news rows.
2. Dispatch `bq_views` with mode=write. The write includes the verify step.
3. Build the Looker sections:
   - My Portfolio: candles per holding, news, ratings.
   - Top 10: finish the page.
   - The symbol deep-dive page.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
