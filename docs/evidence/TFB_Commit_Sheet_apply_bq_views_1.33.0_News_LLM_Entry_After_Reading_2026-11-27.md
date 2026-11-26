# TFB Commit Sheet — apply_bq_views 1.33.0 (v_news_llm_days enters after the model's reading)

Date: 2026-11-27 (Friday) · Lane: BigQuery views (research only) · No tab, gate or order changes · No Render deploy

## Why
Finding D1 of the outside engineering audit the owner uploaded on 26 Nov: `v_news_llm_days` (views 1.32.0) entered at the first close after the **news day**, but the language model reads the gate's 72-hour name window. In BigQuery, 90 of the first 108 name headlines were read one to three days after their news day, and 23 of 32 name-days had an entry close that came **before** the model read them. That is look-ahead: the scoreboard would credit the model with moves it had not yet seen.

## What
`sql/bigquery/141_v_news_llm_days.sql`:
- `day_scores` keeps `last_read_at` = the day's last reading, and `after_d` = the later of the news day and the New York date of that reading;
- a name's entry and SPY's entry are the first close after `after_d`, within 6 calendar days of it;
- `last_read_at` is a new output column.

Both readers (the word lists and the model) keep the same entry, so the duel between them stays fair. The word lists read each headline when the gate collects it, before the model does.

`scripts/apply_bq_views.py` 1.32.0 → **1.33.0**; `scripts/verify_deployment.py` pin. No other view changes.

## Checked in BigQuery
- The new body compiles and runs on the live tables (no entry yet: the bars end on 26 Nov).
- After the fix, no row has an entry on or before the New York date of its reading.

## Tests
- `tests/test_bq_views.py` + `tests/test_bq_archive.py`: 101 pass. The hash of `v_news_llm_days` is re-pinned (PENDING). `case_v68` checks the new entry rule and that the news day alone is no longer the anchor.
- Test files that read the deploy pins: 1415 pass, 35 skipped.

## After merge (Claude)
Dispatch the BigQuery views workflow on main with `write`, then `verify`.

## Owner
Nothing to set.

## Rollback
Revert the file to views 1.32.0 and write the views again.

Not a licensed financial adviser; the owner decides and executes every trade.
