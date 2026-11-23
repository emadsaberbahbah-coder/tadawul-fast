# Commit sheet: projection context and its scorecard (views 1.24.0) - 23 Nov 2026

Owner's request (23 Nov 2026 10:03 Riyadh): "make more deep analysis for all project parts ... compare the results
between pages ... link the last updates that I ask: news, future options, financial derivatives, any other related
data and information that can support the projection accuracy ... search ... the way that you can make better
improvement".

Owner's ground rules (21 Nov): everything new is observe-only, logged and scored; nothing controls a buy.

## What the review measured (read-only, BigQuery, 23 Nov 07:00 UTC)

All figures below are in-sample. These months hold few market regimes, so they are a guide, not evidence.

- **Promise against result (2,569 scored INVESTABLE buy calls, 28 Jun - 13 Nov 2026):**
  - the correlation between promised and realised return is -0.04;
  - the mean promise was +2.62 % and the mean result -0.11 %;
  - at 3 months outside the US: hit 32.8 %, mean -5.12 % against +11.45 % promised.
- **The range:**
  - a one-sigma band from 20-session realised volatility held 59.2 % of the results (US: 56.7 %), against the 68 % it should hold;
  - the ranges are too narrow.
- **Adverse US market states at the call (US calls, 1,541):** the states counted are VIX 20 or more, VIX curve inverted, VVIX high, MOVE high, credit weakening and 10-year yield jumping.

| States | Calls | Days | Hit | Mean |
|---|---|---|---|---|
| 0 | 458 | 46 | 61.6 % | +2.51 % |
| 1 | 442 | 22 | 53.4 % | +0.35 % |
| 2 | 548 | 28 | 49.1 % | +0.22 % |
| 3 or more | 93 | 6 | 30.1 % | -2.27 % |

  - P1 (at most 1 state) against the other days: +2.25 pp, Welch t 2.88 (68 days against 34) before the overlap
    adjustment; 0.71 after it (see the Codex follow-up below).
- **Promise against own sigma (z):**
  - all markets: z 0.41-0.69 hit 59.1 % (+1.79 %), z above 0.69 hit 48.7 % (-0.71 %);
  - US alone: the larger 1-month promises did better (r = +0.22), and keeping only z <= 0.7 hit 48.1 % against 56.4 % at 1M (t -2.97);
  - so the 0.7-sigma cap is a test, not a finding.

## What changes

| File | Version | Change |
|---|---|---|
| sql/bigquery/120_v_projection_context.sql | NEW (views 1.24.0) | Per stock on TFB's pages: <br>- the 1M and 3M promise against its own sigma (Cboe IV30 when read in the last 6 days, else realised 30-day); <br>- z_1m and z_3m; <br>- the one-month one-sigma range; <br>- an observe-only shadow projection capped at 0.7 sigma; <br>- the adverse US states now, with their names; <br>- a reading line. |
| sql/bigquery/121_v_projection_scorecard.sql | NEW (views 1.24.0) | Five rules fixed on 23 Nov: B0, P1, P2, P3, P4. <br>- They run on the scored US calls, in-sample and forward from 24 Nov. <br>- No look-ahead: the market close and the volatility bar used are both from before the call. <br>- P2 is read on the within-day lift; P1 and P4 on the between-days lift (Welch); P3 on kept against left-out calls, clustered by call day. <br>- Every t is overlap-adjusted (divided by the square root of the mean horizon in sessions). <br>- A forward row is read only at the larger of 20 call days and two horizon lengths, and 40 calls. |
| scripts/apply_bq_views.py | 1.23.0 -> 1.24.0 | The two views. |
| scripts/verify_deployment.py | pin | apply_bq_views 1.24.0. |
| tests/test_bq_views.py | | Order, kinds, hashes, case v61, and the bars-table read in v18. |

## Checks

- **BigQuery:** both views ran on live data.
  - Today 5 of the 6 states are on: VIX 20 or more, VIX curve inverted, VVIX high, MOVE high and credit weakening.
  - The holdings' 1-month ranges are drawn from implied volatility, for example SUPN 23.50-33.72 on IV30 61.9 %.
- **pytest:** test_bq_views and manifest pins, 64 passed.

## After merge

Run `bq_views` with write, then verify.

## The site (published 23 Nov, version 1795418172-74b5)

`price_drivers` adds `projection` per stock and, with market true, `projection_record`. The tool description is 906 bytes, under the 1 KB limit.

Not investment advice; the owner decides and executes every trade.

## Codex follow-up (two P2 findings on the first commit)

1. **Overlapping return windows.** Calls on neighbouring days share most of their result window (a 1M call holds 21
   sessions), so treating every call day as independent inflated every t. Every t in the view is now divided by
   SQRT(hd), hd = the mean horizon in sessions of the row's calls: about days / hd call days count as independent,
   the same rule as the overlap-adjusted t of v_trial_c2 and v_trial_shadow_v2 (they count calendar days; this view
   counts sessions, as its call days are sessions). A forward row now waits for the larger of 20 call days and two
   horizon lengths.
2. **P3 is a rule of both kinds.** P3 = a market state AND a stock test, so judging it on timing alone ignored the
   calls it rejects on the days it keeps. It is now read on `keep_vs_out_t`: the mean of the calls it keeps minus the
   mean of the calls it leaves out, with the standard error clustered by call day (G / (G - 1) x the sum of each
   day's squared contribution), overlap-adjusted as above. The column is shown for every rule.

**In-sample after the fix (horizon ALL, US, 1,541 calls, mean horizon 16.5 sessions; not evidence):**

| Rule | Timing lift | t (adjusted) | Kept vs left out | t (adjusted) |
|---|---|---|---|---|
| P1 at most 1 adverse state | +2.25 pp | 0.71 | +1.59 pp | 0.62 |
| P2 promise within 0.7 sigma | within day -0.23 pp | -0.31 | -2.74 pp | -0.80 |
| P3 both | +2.32 pp | 0.75 | +1.06 pp | 0.42 |
| P4 3 or more adverse states | -2.84 pp | -0.66 | -3.26 pp | -0.83 |

None of the in-sample effects is clear once the overlap is counted. The direction stays as before (calm better,
stress worse), and the forward test from 24 Nov decides. Checked by running the view's query on the archive
(23 Nov 2026).

Site: the strategist's price-drivers tool also reads `keep_vs_out_pp` and `keep_vs_out_t`, and its reading note says
every t is overlap-adjusted (artifact version 1795418889-8c7b).
