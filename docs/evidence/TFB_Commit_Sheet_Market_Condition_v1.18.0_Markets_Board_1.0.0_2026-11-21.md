# Commit sheet - a condition reading for every market, their order, and the forward test (21 Nov 2026)

Branch `feat/market-condition-readings`. Observe only: shown and logged, it controls no buy.

| File | Version |
|---|---|
| `scripts/archive_market_series.py` | 1.2.0 -> **1.3.0** (5 series) |
| `scripts/apply_bq_views.py` | 1.17.0 -> **1.18.0** (4 views; three-digit file numbers) |
| `sql/bigquery/99_v_market_condition.sql` | new |
| `sql/bigquery/100_v_market_rank.sql` | new |
| `sql/bigquery/101_v_market_trial.sql` | new |
| `sql/bigquery/102_v_market_trial_summary.sql` | new |
| `scripts/markets_board.py` | new, **1.0.0** |
| `.github/workflows/markets_board.yml` | new, **1.0.0** |
| `.github/workflows/ci.yml` | 1.1.92 -> **1.1.93** (lean lane: `tests/test_markets_board.py`) |
| `scripts/verify_deployment.py` | pins: apply_bq_views 1.18.0, archive_market_series 1.3.0, markets_board 1.0.0 |
| `tests/test_markets_board.py` | new (10 tests) |
| `tests/test_bq_views.py` | V42-V45, the pins, the order |
| `tests/test_archive_market_series.py` | M1: 54 series, the new ids |

## Why

The owner wrote on 21 Nov 2026 at 11:03 Riyadh. This was the build list after the voice call.

> "Start the build from the TFB Build Brief (21 Nov 2026) ... Build in this order. Everything starts in observe mode: shown and logged, but it controls no buys.
> 1. A condition reading for every market, with the same tests as v_market_regime ...
> 2. A daily ranking of the markets by trend and risk. The Daily Plan's hold then works market by market. It follows the market an asset tracks, not the currency it trades in. When every market is weak, cash stays cash.
> ...
> 6. Accuracy: log and score everything. Observe for two weeks (10 sessions), then review with the owner, and switch on only what proved itself."

In the call he gave his reasons:
- Global_Markets had only 23 names left to invest.
- Top 10 had funded nothing for weeks.
- Every market kept falling.

He asked whether Japan, China or Europe, or gold, oil, bonds, sukuk or REITs, were doing any better.

This sheet is items 1 and 2, and the scoring of item 6. The ready list and the switch check (items 3 and 4) are the next PR. News per market (item 5) comes after that.

## The markets

| market_id | Read on | A buy would use |
|---|---|---|
| us | SPY and the ^VIX, price closes, exactly as `v_market_regime` | SPY |
| sa | Tadawul All Share `^TASI.SR` | KSA |
| eu | STOXX Europe 600 `^STOXX` (new) | VGK |
| jp | Nikkei 225 `^N225` | EWJ |
| cn | Hang Seng `^HSI` | MCHI |
| gold | the gold price `GC=F` | GLD (new) |
| oil | Brent `BZ=F` | none: a reading only |
| bonds | IEF (new) | IEF |
| sukuk | SPSK (new) | SPSK |
| reits_us | VNQ (new) | VNQ |
| reits_sa | the REITs on Tadawul (an equal-weight index; see below) | the REITs themselves |

**The Saudi REITs.** The index is built from the REITs on Tadawul, codes 4330-4350, in `symbol_bars_daily`. Seventeen are in the deep set today; 4336 and 4350 are not. The codes were confirmed against the Tadawul REIT lists Argaam publishes. The index is the equal-weight mean of the daily total-return changes (adjusted closes).
- A day needs 10 REITs or more.
- A one-day change beyond Tadawul's 10% limit is a data fault and is left out. One REIT-day of 8,800 was such a fault.

**Closes.**
- Funds use the dividend-adjusted close. A fund that pays out drifts under its own price-only high: about 6% a year for the Saudi REITs, and about 3-4% for IEF and SPSK.
- Indices and futures use the price close.
- The US keeps SPY's price close, so the US reading is the live rule's reading.

## The rules

**The labels.** These are the five labels of `v_market_regime`; the first match wins. k is the market's normal yearly volatility divided by 16. The normal volatility is the standard deviation of its last 250 daily changes, annualised, and needs at least 120 changes. 16% is the S&P 500's normal level, on which the US limits were set. The US keeps k = 1.

| Label | Test |
|---|---|
| crash_day | r1 <= -2k |
| drawdown | r5 <= -3k, or under its 250-close high by 8k or more |
| stress | 20-close volatility >= 25k (the US also: VIX >= 25) |
| weak | under the 50-close average |
| calm | everything else |

**Columns of `v_market_condition`.**
- `waiting_for` lists every failed test with its limit.
- `trend_score` is the mean of the 20- and 60-close changes, each in units of the market's normal move over that span: normal volatility x sqrt(n/252).

**Columns of `v_market_rank`.**
- State: open = calm; hold = any other label. This is the Daily Plan's own US test: `TFB_PLAN_REGIME_BLOCK` defaults to `weak,stress,drawdown,crash_day`.
- Rank: the open markets come first, then the held ones, each group by trend score.
- The verdict reads `ALL ON HOLD - cash stays cash` when no market with a fund is open.
- On every calendar day each market carries its latest close, if that close is 6 days old or less.

**`v_market_trial`.**
- Decision days are the US sessions; every fund but the Saudi REITs trades in New York.
- Entry is the fund's first close **after** the day, so the day's own close is never used.
- Outcomes are taken at +5, +10 and +20 closes, beside SPY over the same span.
- The period is `forward` from Monday 23 Nov 2026 and `backtest` before it.

**`v_market_trial_summary`** is the scorecard. It counts the markets with a fund only. Its columns:
- open against held: mean, share that rose, and the worst 5%;
- the day's first open market against SPY, on the days it was not the US;
- SPY after the days with every market on hold;
- the rank correlation of the trend score with the outcome.

**No fitted limits.** No limit was fitted to the archive. The US limits are `v_market_regime`'s. Every other limit is the US limit scaled by k. The lookbacks (20 and 60 closes) were set before the first read.

## Evidence (read 21 Nov 2026)

**Tickers, confirmed against the data provider.**
- Dry run 44884228224 on the branch read `^STOXX` (753 bars) and GLD, IEF, SPSK and VNQ (755 bars each), 2023-11-17 to 2026-11-20.
- IBKR (read-only `search_contracts`) lists IEF, SPSK, GLD and VNQ, so each fund a buy would use trades at the broker.

**The load.**
- Run 44884452573 loaded the five series from the branch: 5/5 series, 3,773 bars, partitions 15 and 30-33 of `market_series_daily`. This was done so the views could be read with every market before the review.
- The nightly job on main keeps them after the merge.
- Nothing else in the table was touched.

**The US is exactly `v_market_regime`.** On all 755 US sessions of the archive (2023-11-17 to 2026-11-20) `v_market_condition` gives the same label, and its 1-close, 20-close-volatility and from-high figures match to 1e-9 (0 differences).

**The board on Fri 20 Nov 2026: ALL ON HOLD - cash stays cash.**

| Rank | Market | Condition | Waiting for |
|---|---|---|---|
| 1 | Bonds (IEF) | drawdown | 3.6% under its 250-close high (limit 2.6%) |
| 2 | Saudi Arabia | drawdown | 12.2% under its high (limit 6.2%); 1.3% under its 50-close average |
| 3 | US REITs | drawdown | 5-close change -4.1% (limit -2.7%); 7.5% under its high (limit 7.1%) |
| 4 | Oil (reading only) | weak | 11.2% under its 50-close average |
| 5 | Europe | crash_day | 1-close -2.0% (limit -1.6%); 5-close -5.8% (limit -2.4%) |
| 6 | Sukuk | drawdown | 2.8% under its high (limit 1.7%) |
| 7 | China / Hong Kong | drawdown | 5-close -5.2% (limit -3.5%); 19.9% under its high (limit 9.4%) |
| 8 | Japan | crash_day | 1-close -5.1% (limit -3.8%); 22.0% under its high (limit 15.0%) |
| 9 | Saudi REITs | drawdown | 12.5% under its high (limit 3.8%); 20-close volatility 15.5% (limit 12.0%) |
| 10 | Gold | drawdown | 30.7% under its high (limit 14.1%) |
| 11 | United States | crash_day | every test: VIX 35.9, 11.5% under its high, ... |

**The backtest** is the same rules read back on the archive: US sessions before 23 Nov 2026, the markets with a fund. Windows of consecutive days overlap, so read the means.

| Measure | 5 closes | 10 closes | 20 closes |
|---|---|---|---|
| Open markets: outcomes / mean | 3,400 / +0.11% | 3,390 / +0.28% | 3,374 / +0.70% |
| Held markets: outcomes / mean | 2,736 / +0.25% | 2,696 / +0.49% | 2,612 / +0.88% |
| Open minus held | -0.14 pp | -0.21 pp | -0.18 pp |
| Worst 5%: open / held | -3.10% / -3.67% | -3.89% / -4.55% | -5.65% / -6.08% |
| First open market other than the US against SPY | -0.37 pp (beat 46%) | -0.83 pp (40.9%) | -1.21 pp (36.6%) |
| Days with every market on hold / SPY after them | 26 / +0.92% | 26 / +1.71% | 24 / +3.73% |
| Rank correlation, trend score against the outcome | -0.004 | +0.011 | +0.041 |

**What the backtest says, plainly.**
- The label does flag the riskier markets: the held markets' worst 5% was deeper.
- But at 5-20 closes the held markets rose more on average (rebounds after falls), so **the hold has not earned its place on the archive**.
- The trend order carries **no edge**.
- Switching out of the US lost to SPY in 2024-2026.
- On the 26 days with every market on hold, SPY rose afterwards: cash missed the rebound.
- Gold is the one market where open did better than hold (+0.63% against +0.16% over 5 closes).
- SPY alone, after a not-calm close against a calm one: +0.69% against +0.34% before 20 Jun 2026, and +0.08% against -0.11% from 20 Jun.
- The live US rule rests on a different fact: the **picks** made in a weak market did worse (`v_regime_outcomes`, 18 Nov: -1.03% against +0.29% over 5 sessions). The index evidence does not contradict that. It says the market itself rebounded.
- This is why everything stays observe-only until the forward test and the owner's review.

## The forward test

**Dates.**
- It starts on Mon 23 Nov 2026.
- Ten US sessions: 23, 24, 25 and 27 Nov (26 Nov is a US holiday), 30 Nov, and 1, 2, 3, 4 and 7 Dec.
- On 7 Dec the 5-close outcomes of the first four sessions have matured. The 10-close ones mature around 21 Dec.

**The rules are frozen.**
- The four view bodies are pinned in `tests/test_bq_views.py` (PENDING until the write run, then INSTALLED).
- Changing any of them is a new trial, written down as such.

## Safety

- The live system never reads BigQuery. No decision, gate, count or broker action depends on these views or on the Markets_Board tab.
- The Daily Plan keeps its US rule. No variable is changed.
- The broker stays read-only.
- The board reads no tab and writes only Markets_Board.
- The run log prints counts and market labels only, nothing of the account.
- Rollback: repository Variable `TFB_MARKETS_BOARD=off`.

## Tests

The tests:
- `tests/test_markets_board.py`, 10 tests:
  - M1 the day, the verdict, the run on hold;
  - M2 a market's row and the missing market;
  - M3 the scorecard and its reading;
  - M4 the SQL;
  - M5 full runs through the real BigQuery reader on a fake session;
  - M6 dry run, off switch, failures and the quota wait;
  - M7 the workflow wiring and its gate run in bash.
- `tests/test_bq_views.py` V42-V45:
  - the US identity fragments against `v_market_regime` and the live rule's labels;
  - the fund lists of 100 and 101 against 99's market list;
  - entry strictly after the day;
  - the three-digit order.
- `tests/test_archive_market_series.py` M1.

The mutants, all killed by the tests:
- the calendar-gap check of the run on hold;
- one open market ending the run;
- the hold verdict;
- "days old";
- the 60-day history;
- the rank-correlation band;
- an archive failure starting the board;
- base-10 chain hours;
- the deep-data trigger;
- a calm oil price counted as open;
- the first open market ignoring the fund.

## After the merge

1. Dispatch `bq_views.yml` with mode `write` on main. Then dispatch it with mode `verify` and expect every view to verify. Move the four pins from PENDING to INSTALLED.
2. The board runs on its own:
   - after the nightly market series (about 06:30 Riyadh);
   - after the deep data (about 10:30 Riyadh);
   - after the Exit Plan in the UTC hours of `TFB_MARKETS_BOARD_CHAIN_HOURS` (default "5 13").
   The owner needs to set no Variable.

## Known limits

- Oil and gold are front-month futures. Brent also carries a few Sunday bars from Yahoo, so oil is a reading only.
- The REIT index has 17 of the 19 REITs. Its archive holds two years, so its readings start in mid-2025.
- The archive windows (1,100 days; two years for the deep data) bound the backtest to Nov 2024 - Nov 2026 for most markets. That was a strong US market with V-shaped recoveries.
- Asian, Saudi and European closes reach the views with the nightly archive, so the board shows the previous session's reading for those markets until the next morning.

Not investment advice; the owner decides and executes every trade.
