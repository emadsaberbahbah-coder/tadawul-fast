# Commit sheet: news and trend read together (apply_bq_views 1.25.0)

Date: 23 Nov 2026. Owner's ask (12:34 Riyadh): "deep understanding of information ... analysis news ... using data
and information and in same time use strategy and trend ... bring the update in the project with that enhancement".

## Why

- A headline alone carried no edge in TFB's record: `v_company_news_eval` reads NO EDGE, and the news trial
  (`v_news_trial_summary`) has no reading above |t| 1.51 at 5, 10 or 20 days.
- A price alone is a trend. Research reads the two together:
  - Chan (2003, JFE 70): a price move with company news tends to continue; a large move without news tends to reverse.
  - Tetlock (2011), Lopez-Lira and Tang (2023): news the price has not yet answered tends to drift its own way.
  - Good news that the price refuses says more than the news itself.
- TFB already holds every piece: company headlines from two feeds (`v_symbol_news_class`, `v_news_items`), daily bars
  (`symbol_bars_daily`), SPY (`market_series_daily`), TFB's own view (`v_universe_latest`) and the projection context
  (`v_projection_context`). This change joins them.

## What changes

| File | Version | Change |
|---|---|---|
| sql/bigquery/122_v_news_trend_daily.sql | NEW (views 1.25.0) | One row per followed US name and session from 1 Oct 2026: <br>- company news score and items from both feeds (a weekend item belongs to Monday); <br>- the price's answer against SPY over two sessions and its z against the stock's own volatility; <br>- the trend before (50-session average and 20-session change, prior closes only); <br>- one of ten states fixed from research before the data was read, its expected direction and its fit with the trend; <br>- the return against SPY over the next 1, 5, 10 and 20 sessions, measured from the close after the reaction window. |
| sql/bigquery/123_v_news_trend_learning.sql | NEW (views 1.25.0) | Each state's record by period, trend fit and horizon: <br>- events, symbols, dates, mean and base; <br>- the edge in the expected direction and the share that went that way; <br>- a t over dates (each date averaged first), divided by SQRT(h) for overlap; <br>- a fixed reading: 'no expectation', 'collecting', 'works as expected', 'opposite of expected', 'no clear edge'. |
| sql/bigquery/124_v_news_understanding.sql | NEW (views 1.25.0) | Per followed US name today: <br>- the latest state of the last 5 sessions and the last 3 headlines; <br>- the trend and the move research expects, with that state's record; <br>- TFB's own view and the agreement between them; <br>- the one-month range and the adverse market states; <br>- one reading line. |
| scripts/apply_bq_views.py | 1.24.0 -> 1.25.0 | The three files and the docstring. No file of 1.24.0 changes. |
| scripts/verify_deployment.py | pin | apply_bq_views 1.25.0. |
| tests/test_bq_views.py | +case_v62 | Order, kinds, pins and offsets, plus case_v62: <br>- the sources; <br>- no look-ahead (volatility and trend from prior sessions, the outcome from the close after the window); <br>- the ten states in their fixed order, and their expected directions; <br>- the overlap-adjusted t and the reading thresholds; <br>- the agreement with TFB's view; <br>- no clock and no FOLLOWING. |

Outside the repository, the strategist page's `price_drivers` tool changed (artifact version 1795427304-4e92):
- each stock gets `news_and_trend`;
- with `market` true it adds `news_events_now` (the latest states across followed names) and `news_states_record`.

The tool description stays under 1 KB (967 bytes), and the site test checks it.

## The states (fixed 23 Nov 2026, before reading the data)

| State | Rule | Expected against SPY |
|---|---|---|
| good news, price agreed | news score > 0, reaction z >= 1 | up |
| bad news, price agreed | news score < 0, reaction z <= -1 | down |
| good news, price fell | news score > 0, reaction z <= -1 | down |
| bad news, price rose | news score < 0, reaction z >= 1 | up |
| news, price quiet | signed news, \|reaction z\| < 1 | the way of the news |
| unsigned news, big move | news without a sign, \|reaction z\| >= 2 | the way of the move |
| routine news | news without a sign, \|reaction z\| < 2 | none |
| big rise, no news | no company news from the session before to the session after, day z >= 2.5 | down (reversal) |
| big fall, no news | the same, day z <= -2.5 | up (reversal) |
| quiet | everything else | none |

## What the history shows (rules written before it was read; not evidence for switching anything on)

Checked by running the three views' SQL on the archive on 23 Nov 2026.

Coverage:
- 64 US names with company news and bars;
- 37 sessions from 1 Oct 2026;
- next-5-session returns against SPY.

| State | Events | Dates | Edge (pp) | Went the expected way | t (adjusted) | Reading |
|---|---|---|---|---|---|---|
| news, price quiet | 88 | 23 | +1.11 | 69.3 % | 0.85 | no clear edge |
| good news, price agreed | 28 | 13 | +0.11 | 42.9 % | -0.08 | no clear edge |
| big rise, no news | 24 | 10 | +0.95 | 54.2 % | 0.70 | no clear edge |
| big fall, no news | 28 | 15 | -0.62 | 39.3 % | -0.75 | no clear edge |
| unsigned news, big move | 23 | 13 | -0.12 | 43.5 % | -0.11 | no clear edge |
| good news, price fell | 17 | 10 | -1.81 | 23.5 % | -0.91 | collecting |
| bad news, price agreed | 7 | 7 | -1.94 | 28.6 % | -1.00 | collecting |

The base is the mean over all covered sessions: +0.42 pp at 5 sessions.

The strongest reading is drift after news the price has not yet answered: +1.11 pp, and +1.76 pp when it goes with the
trend. It is not statistically clear on 23 dates. Everything stays observe-only.

## Rollout

1. Merge (owner).
2. Dispatch `bq_views` with mode=write on main, then confirm that verify matches.
3. The strategist page reads the views as soon as they exist. Until then its parts say "not readable yet".

Research only: no decision, gate or count reads these views. Not investment advice.

## Codex follow-up (two P2 findings on the first commit)

1. **SPY on the same adjusted basis as the stock.**
   - The stock used adjusted closes and SPY raw closes. A window across SPY's ex-dividend date would then count SPY's
     dividend drop as the stock beating the market.
   - SPY now reads COALESCE(adj_close, close), as `v_company_news_eval` does.
   - Re-run on the archive: the history table above is unchanged. No SPY ex-dividend date falls inside the scored
     windows from 1 Oct.
2. **Recommendation spellings.**
   - 'STRONG BUY' / 'STRONG SELL' (with a space, as `track_performance.py` writes them) fell through to "TFB holds".
   - Spaces and hyphens now read as underscores before the match.
   - A row with no recommendation gives no stance (NULL), so the agreement reads "TFB has no view today".
