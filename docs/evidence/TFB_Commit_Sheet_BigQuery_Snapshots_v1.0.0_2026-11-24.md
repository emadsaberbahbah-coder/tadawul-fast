# Commit sheet: BigQuery snapshots (bq_snapshots 1.0.0, views 1.29.0, news_board 1.7.0, news_now 1.1.0)

Date: 2026-11-24. Owner, 13:07 Riyadh: BigQuery more, faster, more accurate. 13:53: "Let's go with next". This is
item 2 of the plan.

## Why

Three views are read many times a day, and each read recomputes them from the archive:

| View | Per read | Readers |
|---|---|---|
| `v_universe_latest` | 0.24 GB | News Now twice an hour (directly and through `v_symbol_news_class_now`) |
| `v_news_understanding` | 0.37 GB | Two reads per News Board run |
| `v_news_trend_learning` | 0.36 GB | News Board and Learning Watch |

Their sources change only when a collector loads, so each read between two loads recomputes the same answer.

## What changes

| File | Version | Change |
|---|---|---|
| `scripts/bq_snapshots.py` | new, 1.0.0 | Each view computed once into `snap_<name>` (query job, WRITE_TRUNCATE, clustered, with `snapshot_at`); `source_for()` returns the snapshot when it is under 30 hours old, else the view |
| `.github/workflows/bq_snapshots.yml` | new, 1.0.0 | Runs after each finished archive, deep-data, market-series or views run, failed ones included (they may have loaded part of their sources); no cron |
| `scripts/news_now.py` | 1.0.0 -> 1.1.0 | TFB's view (name and recommendation) from the fresh snapshot, else the view |
| `scripts/news_board.py` | 1.6.0 -> 1.7.0 | Sections 11 and 12 read the fresh snapshots, else the views |
| `sql/bigquery/128_v_symbol_news_class_now.sql` | views 1.29.0 | Names from `snap_universe_latest` (the rest of file 97's rules unchanged; tested) |
| `scripts/apply_bq_views.py` | 1.28.0 -> 1.29.0 | `SNAP_TABLES` declared as sources |
| `scripts/verify_deployment.py` | pins | all of the above |
| `.github/workflows/ci.yml` | 1.1.102 | `tests/test_bq_snapshots.py` |

## Accuracy

- A snapshot holds the view's rows as they were when it was taken.
- It is refreshed after every load of its sources, and after every views change.
- A reader never takes a snapshot older than 30 hours, so a missing or stale snapshot costs only the old price, never
  a different page.
- The one exception is view 128: it always reads the universe snapshot, because a view cannot fall back. There it only
  supplies company names, which stay the same between archives.

## Cost and speed

- **Refresh:** about 1 GB per refresh (the three views once), several refreshes a day.
- **News Now:** about 0.47 GB less per run (9 runs a weekday).
- **News Board:** about 1 GB less per run.
- **Speed:** reads take milliseconds instead of recomputing.
- **Net:** several GB a day saved, and the pages build faster.

## After merge (my side, in this order)

1. Dispatch `bq_snapshots.yml` with write=true. View 128 needs `snap_universe_latest` to exist before it is applied.
2. Dispatch `bq_views` with mode=write.

## Rollback

Repository Variable `TFB_BQ_SNAPSHOTS=off`. The snapshots then age past 30 hours and every reader goes back to the
views by itself, except view 128, which keeps the last universe snapshot until the commit is reverted.

This is not investment advice.
