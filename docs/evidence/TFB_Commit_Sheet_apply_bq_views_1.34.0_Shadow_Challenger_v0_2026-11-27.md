# TFB Commit Sheet — apply_bq_views 1.34.0 (shadow challenger v0, week plan package 5)

Date: 2026-11-27 (Friday) · Lane: BigQuery views (research only) · No tab, gate, score or order changes · No Render deploy

## Why
Week plan of 23 Nov, package 5. The 23 Nov deep audit asks for a challenger to the live score. It has to be built only from data TFB holds for two years, which means daily prices, and judged per market, net of cost, on dates it never saw. Exit test from the plan: *a look-ahead input is rejected; a scorecard per market is published; no decision reads any of it.*

## What
Four new views (`sql/bigquery/144`–`147`), research only:

| View | What it holds |
|---|---|
| `v_challenger_v0_features` | Per share and session, from that close and earlier ones only: 12-1 and 6-1 month momentum over 63-session volatility, the 63-session return (relative strength: ranked inside a market it orders names as their return above the market does), and minus the 126-session downside volatility. Market from the exchange suffix (US, SA, JP, HK, OTHER pooled), GICS sector from `v_symbol_sector`. |
| `v_challenger_v0_ranks` | The first session of each ISO week of each market with at least 20 names that have a 12-month history (tied to the calendar, so the rolling 760-day bar window never moves a date). Each input's percentile inside market and sector when at least 20 peers have it, else inside the market (at least 20), else neutral (missing shrinks to 0). The equal-weight score. The result over 21 sessions, **bought at the next close**, against the market's benchmark (SPY total return; ^TASI.SR, ^N225, ^HSI price only; ACWI for OTHER). |
| `v_challenger_v0_walkforward` | Each market's weights on date D: the mean rank correlation of each input on earlier dates whose **last result closed before D** (purged). Negatives go to 0. Equal weights for the first 12 trained dates; no view when no input is positive. Then the score, its quintile, a **shadow action** (buy / hold / avoid / no view) and a **shadow size** (share of the market's shadow buys by inverse volatility, capped at 10 %). |
| `v_challenger_v0_scorecard` | Per market and variant (walk-forward, equal fixed): IC, top fifth vs benchmark net of 0.50 points, top minus bottom fifth net of 1.00, each t over dates divided by √(21/5), and a reading fixed before the first read (THIN < 26 dates; REVERSED at IC t ≤ −2; CANDIDATE at IC t ≥ 2, top fifth net > 0 and spread t ≥ 2; else NO EDGE). |

`scripts/apply_bq_views.py` 1.33.0 → **1.34.0**. New `lookahead_problems()` runs inside `check_objects()`, so the workflow refuses to write a challenger view that could look ahead:
- no forward window frame and no clock in any of them;
- LEAD only in the label CTE, which must buy at the next close;
- no result column in the inputs view, the ranking CTEs, or a `dev_*` / `score_eq` expression;
- the weights train only on dates whose last result closed first (`i.exit_max < f.d`).

## The record (history, backtest; 21-session results; read on 27 Nov, formation on each week's first session)

| Market | Names | Variant | Dates | IC | IC t | Top fifth vs benchmark, net (pp) | Beat % | Top − bottom (pp, gross) | t | Reading |
|---|---|---|---|---|---|---|---|---|---|---|
| US | 496 | equal | 53 | 0.006 | 0.21 | +0.07 | 49 | +0.30 | 0.36 | NO EDGE |
| US | 501 | walk-forward | 37 | −0.007 | −0.15 | −1.22 | 22 | −0.22 | −0.20 | NO EDGE |
| SA | 253 | equal | 52 | 0.115 | 2.30 | −0.21 | 52 | +1.90 | 1.80 | NO EDGE |
| SA | 254 | walk-forward | 36 | 0.069 | 1.18 | +0.09 | 61 | +0.89 | 0.75 | NO EDGE |
| HK | 51 | equal | 52 | 0.134 | 2.21 | +1.93 | 69 | +2.47 | 1.72 | NO EDGE |
| HK | 51 | walk-forward | 35 | 0.244 | 3.06 | +2.34 | 77 | +4.45 | 2.64 | CANDIDATE (forward test) |
| JP | 59 | equal | 50 | −0.011 | −0.20 | −0.03 | 52 | −0.43 | −0.25 | NO EDGE |
| JP | 59 | walk-forward | 32 | −0.040 | −0.31 | −2.07 | 25 | −2.28 | −0.51 | NO EDGE |
| OTHER | 108 | equal | 54 | −0.056 | −1.46 | −1.06 | 54 | −1.29 | −1.38 | NO EDGE |
| OTHER | 106 | walk-forward | 38 | −0.087 | −1.54 | −1.65 | 26 | −1.95 | −1.32 | NO EDGE |

How to read it:
- **Tadawul ranks somewhat, but its top fifth does not beat the index.** The ranking has a cross-sectional edge (IC t 2.3, top minus bottom +1.9 points a month). But the top fifth trailed TASI after cost, so as a buy list it would not have paid.
- **The US has no edge.** The audit found the same, and our own drivers show the past week reversing in US names.
- **Hong Kong is the only CANDIDATE, and a weak one.** It rests on 51 names that are today's survivors, against a price-only index, over 35 overlapping dates (about 8 independent months). It goes to the forward test, nothing more.
- **Japan and the pooled OTHER group have no edge.** OTHER leans negative.
- **The readings are fragile.** Moving the formation day from every 5th session to each week's first session changed OTHER's walk-forward reading from REVERSED to NO EDGE and Tadawul's spread t from 2.3 to 1.8. Two years of prices cannot settle any of this.

Today's shadow book (formation on 23–24 Nov):

| Market | Shadow buys | Learned weights |
|---|---|---|
| US | 101 of 503 | 6-1 momentum 0.54, 12-1 0.34, downside 0.12 |
| SA | 51 of 254 | 12-1 0.21, 6-1 0.17, 63-day 0.25, downside 0.37 |
| HK | 11 of 51 | downside 0.53 |
| JP | 12 of 59 | all weight on calm downside |
| OTHER | none | no input positive |

## Changed after the first read (disclosed)
The first read showed the US top-minus-bottom at −7.9 points a month while its IC was +0.02. Three US names carry unadjusted reverse splits (QH, INHD, MBRX: a price ×20 to ×170 in one session), and their results swamped the quintile means.

The results now used for means (`excess_w_pp`) are held inside each date's 1st and 99th percentile. The ranks and the IC were not affected. Codex P2 on #1101 also moved the formation dates. They were every 5th session counted from the oldest retained bar, but `symbol_bars_daily` is reloaded nightly as a rolling 760-day window, so each reload would move every date. They are now each market's first session of each ISO week, tied to the calendar. The table above is read with that rule.

Two guards were also added after that read:
- an input counts on a date only with at least 20 ranked names (the pooled OTHER calendar starts before most of its names have a year of history, which gave NaN weights);
- a date whose score does not vary gives no IC.

**The reading rules were not changed.**

## Checked
- All four bodies compile and run on the live tables (about 0.6 GB a read).
- **Look-ahead, live:** with every bar after 15 Jun 2026 removed, 0 of 976 names change any input on 15 Jun.
- `tests/test_bq_views.py` + `tests/test_bq_archive.py`: 102 pass. New `case_v69` covers fragments and refs, and eight planted leaks, each refused by `lookahead_problems()` and by `check_objects()`:
  - LEAD in the inputs view;
  - a FOLLOWING frame;
  - a `dev_*` that reads the result;
  - a rank ordered by the result;
  - an entry at the formation close;
  - LEAD outside the label CTE;
  - weights trained on unclosed dates;
  - a clock.
- The four new hashes are pinned in PENDING.

## Limits
- **Survivors only.** The 1,062 names are today's list carried back, which flatters any long side.
- **Benchmarks.** Saudi, Japan and Hong Kong benchmarks are price only, which flatters the names by about the index yield. ACWI is in USD against local returns.
- **Adjusted closes** fold in later splits and dividends. The returns are right, but no point-in-time fundamentals exist.
- **One cost for every market:** 0.50 points a round trip.
- **The shadow book is recomputed on every read, not stored.** A revised bar can change a past book. Keeping each day's book in a table (a nightly append) is the next step, so the forward test judges the books as they were published.
- **History is not evidence.** Only the forward rows, from 27 Nov, can be; they are first read on 8 Dec and need months.

## After merge (Claude)
Dispatch the BigQuery views workflow on main with `write`, then `verify`.

## Owner
Merge when you have read it. Nothing to set. Nothing reads these views.

## Rollback
Delete the four files and write the views again (`write` leaves the old objects; drop them by hand if wanted).

Not a licensed financial adviser; the owner decides and executes every trade.
