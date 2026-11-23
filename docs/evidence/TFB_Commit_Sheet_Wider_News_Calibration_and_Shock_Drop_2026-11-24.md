# Commit sheet: wider news, confidence calibration and the shock-drop shadow rule

Date: 24 Nov 2026. Builds: archive_symbol_deep 1.3.0 and apply_bq_views 1.26.0.

Owner's words (Riyadh time):
- 00:48: "Today NVDA increased around 30% ... did the new news system expect that?"
- 00:58: "a company like that, the first in the world, is not allowed to collapse ... if we expected that, the stock
  became attractive to buy ... the news of 100 companies, is that list fixed or changing?"
- 01:07: "check now we are able to achieve our target"
- 01:11: "ok, no objection"
- 01:18: "Ok let's start"

## Why

- **NVDA was invisible to the news system.** NVDA rose 31.3 % on 23 Nov when its board reinstated Jensen Huang. The
  news system had nothing on it:
  - the company-news core set is rebuilt each night from what TFB likes (the holdings, the board, the ROI leaders);
  - it does not follow what moves the market;
  - NVDA was a HOLD.
- **The hit-rate target is not reachable as measured today.** The Model Evidence tile's population is 2,389 scored
  INVESTABLE records:
  - 47.7 % rose, against the 70 % target;
  - 58.0 % beat their own market over the same dates (mean +1.16 pp);
  - the share that rose follows the market: about 57 % in the rising months, about 30 % in the falling ones.
- **TFB's confidence is inverted for Saudi calls.**

  | Saudi INVESTABLE calls | Records | Names | Rose | Beat TASI |
  |---|---|---|---|---|
  | HIGH confidence | 449 | 21 | 23.8 % | 36.5 % |
  | MEDIUM confidence | 171 | 13 | 53.8 % | 74.9 % |

- **NVDA fell 39 % in 13 sessions before the rebound.** It fell 20 % more after the President's remark of 16 Nov. The
  owner's idea (a large company is not allowed to collapse) is therefore measured as a shadow rule before it controls
  anything.

## What changes

| File | Version | Change |
|---|---|---|
| scripts/archive_symbol_deep.py | 1.2.0 -> 1.3.0 | With `TFB_DEEP_NEWS_WIDE` on (default), the set query adds two reasons from `v_universe_latest`. Each symbol must be equity on every page and never a Mutual_Funds row (SPY, IVV and VTI are labelled EQUITY there). <br>- 'movers': up to 30 listings among the 500 largest of their market whose last change was 5 % or more and 3 daily standard deviations or more, every market; <br>- 'mega': the 50 largest US equities. <br>They join the core set after the extra symbols, and the core cap becomes 200. About 80 more names cost about 1,200 EODHD calls a night (1.2 % of the limit), still behind the budget guard. Off: v1.2.0 byte for byte. |
| .github/workflows/bq_symbol_deep.yml | 1.3.0 | `TFB_DEEP_NEWS_WIDE: ${{ vars.TFB_DEEP_NEWS_WIDE \|\| 'on' }}`. |
| sql/bigquery/125_v_confidence_calibration.sql | NEW (views 1.26.0) | The tile's population, by market (SA, US, OTHER, ALL), confidence and month: <br>- the share that rose and the mean return; <br>- the market's own return over the same dates (SPY, ^TASI.SR, ACWI; the last close on or before each date); <br>- the mean excess and the share that beat the market; <br>- a fixed reading: HIGH in order, close to, or inverted against MEDIUM. |
| sql/bigquery/126_v_shock_drop_events.sql | NEW (views 1.26.0) | The shadow rule, fixed before reading: the 100 largest US equities with bars, a close 30 % or more under the highest close of the last 16 sessions. <br>- A: buy at the first close of the fall; <br>- B: buy at the first close above the previous high within 10 sessions; <br>- each scored against SPY over 5, 10 and 20 sessions. <br>Bars stored as NVDA or NVDA.US are one series. |
| sql/bigquery/127_v_shock_drop_learning.sql | NEW (views 1.26.0) | The rule's record by period, arm and horizon, with an overlap-adjusted t over dates. <br>The history is read as 'in-sample, not evidence' because it uses today's largest companies (survivorship). <br>Forward rows from 24 Nov 2026 are read after 10 events on 8 dates. |
| scripts/apply_bq_views.py | 1.25.1 -> 1.26.0 | The three files and the docstring. No file of 1.25.1 changes. |
| scripts/verify_deployment.py | pins | archive_symbol_deep 1.3.0; apply_bq_views 1.26.0. |
| tests/test_archive_symbol_deep.py | +D13 | The wide query; the gate; set order and cap; a run with the gate on and off. The older cases run with the gate off, so they prove v1.2.0 is unchanged. |
| tests/test_bq_views.py | +case_v63 | Order, kinds, pins and offsets. The sources; the benchmark dates (never after); the reading thresholds; the rule's windows (prior sessions only, outcome from the signal close); history never read as evidence. |

## Checked before the PR (BigQuery, 24 Nov)

- **The wide set query returns 50 mega names and 21 movers.**
  - Mega: AAPL, MSFT, GOOGL, NVDA, ... (SPY and IVV are out after the Mutual_Funds filter).
  - Movers: 3008.SR, JUP.L, VK.PA, INTU.US, ...
- **The calibration view reproduces the tile.**

  | Slice | Records | Rose | Beat market |
  |---|---|---|---|
  | ALL / ALL | 2,389 | 47.7 % | 58.0 % |
  | ALL / HIGH | 610 | 32.6 % | 40.8 % |
  | ALL / MEDIUM | 1,779 | 52.8 % | 63.9 % |
  | US / MEDIUM | 1,328 | 50.5 % | 61.7 % |

  - Saudi: HIGH vs MEDIUM reads 'inverted: HIGH below MEDIUM'.
- **The shock-drop events view finds 3 events today:**
  - ANET (A and B, March 2025);
  - NVDA (A, 19 Nov 2026 at 145.98, the fall -31.7 %).

  Only 29 of the 100 largest have bars today; the mega set adds the rest from tonight's run.

## Nothing decides on this

- The live system never reads BigQuery.
- No gate, count, page or broker action changes.
- The wider news set is data for the news-and-trend views (observe).
- The calibration and shock-drop views are research. Any change to confidence or a buy rule waits for the review and
  the owner's approval (ground rules, 21 Nov 2026).

## Rollback

- Wider news: the repository Variable `TFB_DEEP_NEWS_WIDE=off`.
- Views: the three views are additive; dropping them changes nothing else.

Not a licensed financial adviser; the owner decides and executes every trade.
