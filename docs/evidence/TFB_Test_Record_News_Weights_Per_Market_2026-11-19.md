# Test record - news weights per market - 2026-11-19

**Question (owner, 19 Nov 2026):** should the weekly news themes tilt the plan between markets (Tadawul first), the way the sector tilt is being tried?

**Verdict: no. Not built.** No market tilt and no market overlay. The sector trial that started with the list of 20 Nov 2026 stays the only news trial.

## Method (the same clean timing as the sector test)
- News week: Monday to Sunday, UTC. The 14 weekly themes of `news_theme_daily` (GDELT events and GKG).
- Target: each market's return from the first trading day's close of week W+1 to that of week W+2, against the average of the markets.
- Weights for week W are fitted on pairs up to week W-2 only.
- Placebo: the same test with the news lagged 8 to 40 weeks. A real effect must beat every placebo.

## Results
| Test | Edge per week | t | Reading |
|---|---|---|---|
| Nine markets, political events | +0.395 pp | 2.05 | no placebo scored higher |
| Six liquid markets (US, Europe, China, Japan, Tadawul, emerging) | +0.11 pp | 0.48 | nothing |
| Tadawul index alone | - | 0.75 | nothing |
| Kuwait fund alone | - | 3.03 | carries the nine-market result |
| Qatar fund / Saudi fund / UAE fund / China | - | 1.86 / 1.52 / 1.19 / 1.38 | weak |

## Why it is not an edge
- The nine-market result comes from the thin New York-listed Gulf country funds. Their prices lag their home markets, so "news predicts next week" is partly "last week's home-market move reaches the fund late".
- Take those funds out and the edge is gone (t 0.48). The Tadawul index itself shows nothing (t 0.75).
- A rule built on it would trade the lag of a fund, not the news.

## What would reopen it
- The same test on home-market index closes for the Gulf markets, with at least 8 news weeks and t of 2.5 or more - the promotion rule already set for the sector trial.
