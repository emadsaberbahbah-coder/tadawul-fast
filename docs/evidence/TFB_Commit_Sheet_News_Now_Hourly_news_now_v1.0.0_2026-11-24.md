# Commit sheet: News Now, the hourly news and price reading (news_now 1.0.0)

Date: 2026-11-24. Owner ask (24 Nov, 10:10 and 10:20 Riyadh): the news analysis should act on the reading of a
stock's price now. PR #1071 put the nightly reading on News_Board section 11 (twice a day). This change reads the
company news and the price within the hour during the US session.

## What changes

| File | Version | Change |
|---|---|---|
| `scripts/news_now.py` | new, 1.0.0 | The hourly job: names, EODHD news, Yahoo price, state, log, page |
| `.github/workflows/news_now.yml` | new, 1.0.0 | Chained to the hourly Exit Plan; UTC hours 13-21, Monday to Friday; no cron |
| `sql/bigquery/128_v_symbol_news_class_now.sql` | new | File 97's rules over the hourly table (only name, source and description differ; tested) |
| `scripts/apply_bq_views.py` | 1.26.0 -> 1.27.0 | `NOW_TABLES` (symbol_news_intraday, news_now_log) declared as sources |
| `scripts/archive_symbol_deep.py` | 1.3.0 -> 1.3.1 | `ensure_table` takes an optional spec; `load` an optional schema and WRITE_APPEND. Defaults unchanged |
| `scripts/verify_deployment.py` | pins | news_now 1.0.0, apply_bq_views 1.27.0, archive_symbol_deep 1.3.1 |
| `.github/workflows/ci.yml` | 1.1.100 | `tests/test_news_now.py` after `tests/test_news_board.py` |

## Each run

1. **Names:** the holdings (a plain ticker is read as a US listing) and the Top 10 names of the last 3 board days;
   at most 40.
2. **News:** EODHD company news of the last 2 days, behind the nightly job's daily-budget guard (5 calls a name).
   - The rows go into `symbol_news_intraday`, replacing today's partition.
   - The load happens only when every call answered, so a failed call never drops earlier items of the day.
3. **Price so far:** Yahoo daily chart for each US name and SPY. The session is read as open, closed today, or
   not open yet. The previous close uses the adjusted basis.
4. **State:** the rules of `v_news_trend_daily` (122), unchanged, applied to the session so far.
   - Daily volatility: the 20 sessions before.
   - Reaction z: over sd x sqrt 2.
   - Trend before: the last close against its 50-session average and the close 21 sessions back.
   - Expected move and trend fit follow the same rules.
   - TFB's view and the agreement follow `v_news_understanding`.
5. **Output:**
   - One row per name and run is appended to `news_now_log`. This is the hourly record for later scoring.
   - The News_Now tab is rewritten as a whole.

## Safety

- Observe only: no recommendation, gate or order changes. The page's second line says so.
- Public run logs print counts only: no holding symbol, quantity or amount. The EODHD token never appears.
- Before view 128 is applied, the page shows the raw headline counts and says the headlines are not classified yet.
- BigQuery cost: about 0.5 GB a run (the class view and `v_universe_latest`), 9 runs a weekday.
- EODHD cost: about 200 calls a run, inside the shared guard (60% of the daily limit).

## After merge (my side, the established practice)

1. Dispatch `news_now.yml` once with write=true on main. Its first run creates the two tables.
2. Dispatch `bq_views` with mode=write on main to apply view 128. A view cannot be created before its table exists.

## Nothing for the owner to set

Defaults:

| Variable | Default | Effect |
|---|---|---|
| `TFB_NEWS_NOW` | on | `off` stops the job |
| `TFB_NEWS_NOW_EODHD` | on | `off` skips the news calls |
| `TFB_NEWS_NOW_CHAIN_HOURS` | "13 14 15 16 17 18 19 20 21" | `off` stops the chained runs |

## Rollback

Repository Variable `TFB_NEWS_NOW=off`, or revert the commit. The two tables stay as research data.

This is not investment advice.
