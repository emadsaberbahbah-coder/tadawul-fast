# Commit Sheet: total return, benchmarks and regimes (apply_bq_views 1.7.0, archive_market_series 1.2.0), 2026-11-08

**Scope.** Owner, 8 Nov 2026 12:46-13:13 Riyadh (verbatim: "there is last week big loss happend how we can avoid that in the future"; "What we can add to our analysis and forecasting"; "ok let's do the requirment and implement it the project"). Items 7, 8 and 9 of the post-mortem plan (project doc `TFB_PostMortem_Week_2026-11-02_to_06.md`, section 4b): the learning loop scores dividend-adjusted returns, each forecast is judged against its market, and every board seat carries the market regime it was taken in. Research layer only: the live system never reads BigQuery; no decision, gate, count, ticket or broker action changes.

## Why
| Finding (measured 8 Nov) | Why it matters | Fix |
|---|---|---|
| `v_outcome_engine` scored raw closes. SUPN went ex an $8.13 special dividend on 3 Nov (22 % of the price): the owner's 2 Nov INDV forecast would have scored -26.0 %; the economic result is -4.7 %. | The calibration would learn a false -22 % loss and the ROI capture a false miss. | Total return: outcome adj_close restated in entry-day terms (outcome adj x base close / base adj); raw close when adj is missing. `engine_roi_raw_pp` and `dividend_adj_pp` disclose the difference. |
| INDV.US records had no bars after 2 Nov (the ticker became SUPN); SUPN.US was recorded at the stale pre-merger 41.92 on 3 Nov against the merged company's 28.15 (-37.6 %). | Forecasts of a renamed name never mature; a stale quote scores as a crash. | Alias table (INDV.US -> SUPN.US: the record keeps its symbol, its bars are the surviving series'; `bars_symbol` says which); `entry_price_mismatch` when the entry price is more than 35 % from the base bar's close - 226 of 25,715 records today (AAPL / MSFT / NVDA split history, Saudi splits, SUPN), never scored. |
| The engine's win rate is absolute: the 5 Nov -4.4 % day lowers every pick's score at once. | The ranking learns market direction, not selection. | `v_outcome_excess`: each engine-scored forecast beside its market benchmark (SPY / TASI / HSI / FTSE / Euro Stoxx / Nikkei; ACWI elsewhere), excess return and verdict at the target and on the ladder. |
| Nothing says what kind of day a seat was taken on; the archive had SPY but no fear gauge. | The F-8 drawdown brake cannot be decided on evidence. | `^VIX` archived (series 14, kind "Volatility index"); `v_market_regime` (one label per US session: crash_day / drawdown / stress / weak / calm), `v_seat_regime` (seat tagged with the regime at the last US close before it), `v_regime_outcomes` (outcomes per regime and currency group with sample sizes). |

## Changes
| Change | Tests | Installed |
|---|---|---|
| `sql/bigquery/70_v_outcome_engine.sql` 1.5.0 -> 1.6.0 (total return, alias, mismatch state; `bars_symbol`, `base_bar_date`, `target_close_adj`, `engine_roi_raw_pp`, `dividend_adj_pp`) | V24 (unchanged contract, new state named), V30 (the SUPN -4.84 % vs -26.16 %; a 2:1 split reads 0; missing adj falls back; alias relabels the new series and drops the old stub; 41.92 vs 28.15 is a mismatch; a 10 % band would refuse ordinary moves) | pending (85d51279) replaces a7f75e0528a3 |
| `sql/bigquery/73_v_outcome_coverage.sql` 1.5.0 -> 1.5.1 (`engine_entry_mismatch` per horizon and ALL) | V27 | pending (fba5c320) replaces 341bd1f3a44d |
| `sql/bigquery/76_v_outcome_excess.sql` (new) | V31 (benchmark by market, from the engine's base bar - never the recording day; a -3 % call in a -4.4 % market is a WIN; a short that fell less than the market is a LOSS; review P2: the benchmark scan reaches the target bar for every horizon - 6M / 1Y - and keeps the 98-day ladder window, as the engine's own scan does; the old 98-day cut returned NULL excess for every long-horizon forecast) | pending (6a085d68) |
| `sql/bigquery/77_v_market_regime.sql` (new) | V32 (label order fixed; the 50-day verdict needs 50 bars; -2.0 % is a crash day) | pending (422399a6) |
| `sql/bigquery/78_v_seat_regime.sql`, `79_v_regime_outcomes.sql` (new) | V33 (a Monday seat reads Friday, never its own close; USD and ALL groups; 'small sample' under 30 scored or 5 days) | pending (98603886, 3a4c2de8) |
| `scripts/archive_market_series.py` 1.1.1 -> 1.2.0 (series 14 `^VIX`, kind "Volatility index"); `tests/test_archive_market_series.py` (49 series, pinned id 14) | M1 | next `bq_market_series` write loads it |
| `scripts/apply_bq_views.py` 1.6.0 -> 1.7.0 (WHY header); `scripts/verify_deployment.py` pins (views 1.7.0, market series 1.2.0) | manifest pins | - |

## Pre-merge checks
- **Live BigQuery (read-only, 8 Nov, every new body run in full with the engine inlined).**
  - Engine: FRST.US recorded 30 Oct reads -1.66 % adjusted vs -2.33 % raw (the $0.10 ex-dividend of 6 Nov); INDV.US recorded 29 Oct, 1W target bar 5 Nov: -0.88 % adjusted vs -23.09 % raw; INDV.US recorded 2 Nov (the owner's entry day): unrealized -4.68 % adjusted vs -26.04 % raw, bars from SUPN.US; SUPN.US recorded 3 Nov at 41.92: entry_price_mismatch.
  - Mismatch census: 25,715 records with a bar in the week after recording; entry / base-close ratio outside 0.65-1.35 for 226 (AAPL, MSFT, NVDA, 2222.SR, 2382.SR, 7202.SR, SUPN.US, MNST.US and others); 25,475 inside 0.8-1.25.
  - Excess: every engine-scored record found its benchmark (US 3,066 / SPY; Tadawul 1,515 / TASI; Hong Kong 556 / HSI; Europe 89; Tokyo 75; London 53; 170 others / ACWI). Tadawul picks: absolute win 43 %, excess win 51 % (TASI itself fell 2.1 % over the same windows); Hong Kong picks +2.9 pp over HSI; US +0.08 pp over SPY.
  - Regime: since 20 Jun the archive has 74 calm, 23 weak and 1 crash day (5 Nov, -4.41 %); no drawdown / stress day yet (VIX not archived until the next market-series write). USD seats: calm 229 scored, +0.29 % at +5, 54 % win; weak 94 scored, -0.14 %, 47 %; crash_day 2 seats, none scored yet.
- **Tests.** test_bq_views 33/33 (29 + V30-V33); with test_verify_manifest_pins, test_bq_archive and test_archive_market_series 83 passed.
- **Mutants.** 13 SQL mutants, each killed by V24 / V27 / V30-V33 (pins aside): the raw close as engine_roi_pp, a 10 % mismatch band, the old symbol's own bars kept, raw last close, a raw 1M ladder step, the benchmark from the recording day, an inverted bearish verdict, SPY for every market, drawdown judged before crash_day, a 50-day verdict on 49 bars, the seat day's own close, 'small sample' without the day floor, the ALL group filtered to USD.

## Post-merge order (lead)
1. A fresh `bq_market_series` dispatch (mode=write) outside 10:00-15:00 Riyadh and outside a running sync loads `^VIX` (49/49 series).
2. A fresh `bq_views` dispatch with mode=write installs the seven pending bodies (and the two 1.6.0 ones if not yet installed); verify must read 47/47, then the pins move to INSTALLED.
3. Looker: the Learning section gains the excess table (v_outcome_excess by market) and a Regime table (v_regime_outcomes); the coverage table gains engine_entry_mismatch.
4. Read v_regime_outcomes before arming any drawdown brake; today it says 'small sample' for every regime but calm.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
