# Commit sheet: price drivers (options, VIX curve, futures, short volume) - 22 Nov 2026

Owner's request (22 Nov 2026 19:53 Riyadh): "add how future options and other options affect the current price and
recommendations ... add this effect to our score and analysis, also check other factors that effect current price and
add them to our project in best way".

Owner's ground rules applied (21 Nov): everything new is observe-only, logged and scored; nothing controls a buy; the
broker stays read-only; no live gate changes; switching anything on waits for its record and the owner's review.

## What changes

| File | Version | What |
|---|---|---|
| scripts/archive_market_series.py | 1.3.0 -> 1.4.0 | 21 more daily series: ^VIX9D, ^VIX3M, ^VIX6M, ^VVIX, ^SKEW, ^MOVE; ES=F, NQ=F, YM=F, RTY=F; ^IRX, ^FVX, ^TNX, ^TYX, TLT; DX-Y.NYB; HYG, LQD; RSP, IWM, QQQ |
| scripts/archive_options_reading.py | NEW 1.0.0 | Nightly: Cboe's delayed option chain per name (held, rated, the largest US names to 900, 20 ETFs) and FINRA's daily short volume -> `options_daily` (one partition per trading day, WRITE_TRUNCATE). Logs counts only, never symbols. Gate `TFB_BQ_OPTIONS` (default on in the workflow) |
| .github/workflows/bq_options.yml | NEW | 23:30 UTC Mon-Fri; dispatch inputs write and max |
| sql/bigquery/115_v_options_reading.sql | views 1.23.0 | IV30, 5-day change, 1-year percentile (20 days needed), term ratio, implied move to the front expiry, event priced (front/90d >= 1.15, <= 45 days), 25-delta put skew, calls-minus-puts IV, put/call volume and OI, short volume and its z (10 earlier days needed), cross-sectional z-scores (ETFs out), options tilt -3..+3 |
| sql/bigquery/116_v_market_drivers.sql | views 1.23.0 | Per US day: VIX curve (VIX/VIX3M, VIX9D/VIX), VVIX, SKEW, MOVE, futures vs cash, 10y-3m curve, 10y 5-day change, dollar, credit (HYG/IEF), breadth (RSP/SPY), small caps (IWM/SPY), each with a state; z over the previous 252 days; SPY next 5 and 20 sessions as the outcome |
| sql/bigquery/117_v_market_driver_learning.sql | views 1.23.0 | Each state's record against all days over about three years (in-sample), is_now; 'too few days' under 50 |
| sql/bigquery/118_v_price_drivers.sql | views 1.23.0 | Every stock on TFB's pages: options reading, IV/HV, week vs its sector ETF, volume vs average, analysts' monthly change, days to earnings, market state, `driver_tilt_pp` and the observe-only `overall_with_drivers_shadow` |
| sql/bigquery/119_v_driver_learning.sql | views 1.23.0 | Each stock-level driver (and TFB's score, and the shadow score): daily Spearman rank IC and quintiles against the next week's excess return of US names of $2bn or more, from 31 Oct 2026; 'too few weeks' under 8 weeks |
| scripts/apply_bq_views.py | 1.22.0 -> 1.23.0 | Five views; `OPTIONS_TABLES = {"options_daily"}` joins SOURCE_TABLES |
| scripts/verify_deployment.py | pins | apply_bq_views 1.23.0, archive_market_series 1.4.0, NEW archive_options_reading 1.0.0 |
| tests | | test_archive_options_reading.py (8, real Cboe fixture), test_archive_market_series.py (pins 75 ids), test_bq_views.py (order, kinds, hashes, cases v58-v60) |
| .github/workflows/ci.yml | 1.1.96 | the options test in the lean lane |

## Checks

- BigQuery (read-only, with stubs for the tables not yet loaded): 115, 116, 117, 118 and 119 all run.
  - 118 on the live universe: SLS.US with a stub tilt of -1.4 and short z 2.3 -> driver_tilt -3.1, shadow 34.8 against
    overall 37.85; it also reads "beat its sector by 79.9 points this week".
  - 119 on the live universe (options stubbed empty): 16 days, 3 weeks, about 1,939 names a day; every driver reads
    'too few weeks'. First sign, not evidence: the past week against the market has IC -0.24 on 9 of 9 days
    (a weekly reversal, Q5 minus Q1 = -3.8 points).
- Cboe parsing on real chains: TSN iv30 25.6, implied move 5.7 %, skew 2.4; SUPN's thin, adjusted chain gives no
  derived readings (by design).
- pytest: test_bq_views 60 passed; test_archive_options_reading 8; test_archive_market_series 15; verify_manifest_pins 3.
  The full suite: two failures that also fail on main without this change
  (test_security_pins_libsmoke l17 tracecontext - the local library version; test_tracker_log_read_precision l2).
- Workflow audit: errors 0, warnings 0.

## After the owner merges (in order)

1. `bq_markets` with write - the 21 new series load (about 1,100 days each), so v_market_drivers fills.
2. `bq_options` with write - creates `options_daily` and loads the first day.
3. `bq_views` with write, then verify - the five views.

## The site (published 22 Nov 2026, version 1795368487-960b)

The strategist gets a 16th tool, `price_drivers`: TFB's row from v_price_drivers (when live), IBKR's live options
reading per US name (implied volatility and its percentile, expected move over 1 day / 1 week / 1 month, implied over
realised, put/call volume against usual), and with market true the market's forces, their record and each driver's
record. No new connector permission is needed (get_price_snapshot and BigQuery were already declared).

Not investment advice; the owner decides and executes every trade.
