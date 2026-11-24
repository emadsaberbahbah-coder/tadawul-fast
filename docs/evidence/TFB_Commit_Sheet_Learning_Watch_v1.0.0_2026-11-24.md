# Commit sheet: Learning Watch (learning_watch 1.0.0)

Date: 2026-11-24. Owner, 13:07 Riyadh (in Arabic): how can BigQuery be used more, faster and more accurately, and
what do we need to develop? This is item 1 of the answer.

## Why

BigQuery holds 18 learning records open to pages (the shadow score trial is left out: no page-building script may read it while it is in shadow mode): every signal, trial and gate is scored against the market with a fixed
reading. None is seen until someone opens it, so a reading that turns "works" or "opposite" waits for a manual review.

## What changes

| File | Version | Change |
|---|---|---|
| `scripts/learning_watch.py` | new, 1.0.0 | Reads the 18 views and stores the day's readings; writes the Learning_Watch tab |
| `.github/workflows/learning_watch.yml` | new, 1.0.0 | Chained to the hourly Exit Plan at UTC 9 (12:35 Riyadh); no cron |
| `scripts/verify_deployment.py` | pin | learning_watch 1.0.0 |
| `.github/workflows/ci.yml` | 1.1.101 | `tests/test_learning_watch.py` after `tests/test_news_now.py` |

## Each run

1. **Read the views.** Each view in the registry is read with one SELECT of its key columns, reading, sample size,
   effect and t. A view that is missing or refused is listed, never fatal.
2. **Sort the readings.** Each reading goes into one of five groups by fixed words:

   | Group | Words |
   |---|---|
   | PROVEN | works, holds, beats, better, edge, ahead |
   | AGAINST | opposite, inverted, worse, fails, inverse, loses, not ahead |
   | COLLECTING | collecting, too few, thin, no edge yet |
   | NEUTRAL | no clear edge, not proven, in order, and so on |
   | IN-SAMPLE | history, backtest or "candidate" rows; never evidence |

   Every reading word the 18 views can produce is tested.
3. **Store and compare.** The day's rows go to `learning_watch_log` (one partition per Riyadh day; a rerun replaces it)
   and are compared with the last earlier day.
4. **Write the page.** One page, Learning_Watch:
   - the counts;
   - what is new or changed since the last day;
   - every forward PROVEN and AGAINST row;
   - each record's tally.

## Safety

- Observe only: a PROVEN row is a candidate for the owner's review, nothing more.
- The run log holds counts per record only.
- BigQuery: SELECT only from the views, plus one table this job creates.

## Cost

18 view reads a day, roughly 4-8 GB in all (several views cost 0.2-0.5 GB each). That is about 3% of the monthly
free tier per day.

## After merge (my side)

1. Dispatch `learning_watch.yml` once without write, to check every view reads.
2. Then dispatch it once with write=true.

## Rollback

Repository Variable `TFB_LEARNING_WATCH=off`, or revert the commit.

This is not investment advice.
