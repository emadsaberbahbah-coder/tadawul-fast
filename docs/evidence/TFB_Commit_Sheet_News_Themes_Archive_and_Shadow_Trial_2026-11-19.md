# Commit Sheet - the news learning loop, phase A: theme archive, self-correcting weights, shadow list and trial - 2026-11-19

**Scope:** owner, Thu 19 Nov 2026. 12:15 and 12:31 Riyadh: world news (wars, politics, the economy, AI fear, elections, summits) must be classified, weighted by kind and by sector, tested on the past and able to correct itself. 13:06, verbatim: "Let's Start build the new news and it's effect in the file by the best way that you propse and before doing that make more final test then apply the changes by the best possible and effective way".

**Files:**
- `scripts/archive_news_themes.py` (new, v1.0.0) and `.github/workflows/bq_news_themes.yml` (new, v1.0.0).
- `sql/bigquery/91_v_news_theme_weekly.sql`, `92_v_news_sector_weights.sql`, `93_v_news_sector_tilt.sql`, `94_v_news_overlay_list.sql`, `95_v_trial_news_overlay.sql` (new).
- `scripts/apply_bq_views.py` 1.13.0 -> 1.14.0 (header, `NEWS_TABLES`); `scripts/verify_deployment.py` pins.
- `tests/test_archive_news_themes.py` (new, 11 tests); `tests/test_bq_views.py` (five PENDING hashes, order, kinds, `case_v37`).
- `.github/workflows/ci.yml` 1.1.86 -> 1.1.87 (the new test file in the lean lane).

## S0 - The final test (in-sample, NOT evidence)
Three years of GDELT (Oct 2023 - Nov 2026) against the 11 US sector ETFs minus SPY. Every week the weights are fitted on the past only; a week's news (Monday - Sunday UTC) is set against the week entered at the next first close. 75 scored weeks (May 2025 - Nov 2026).

| Arm | Favoured 3 minus avoided 3, points a week | t | Favoured above the sector average | Stale-news versions that scored higher |
|---|---|---|---|---|
| events (wars, US-China) | +0.15 | 0.53 | 53% of weeks | 5 of 9 |
| all (events + 10 document themes) | +0.17 | 0.69 | 56% of weeks | 3 of 9 |

- "Stale news" is the same method fed news 8 to 40 weeks old. It should not work; it scored as well as fresh news.
- The first cut of the test had read events by event date and prices Friday to Friday. That let weekend news and next-day reports into the week. The clean version above is the one that counts.
- On the 13 archived list days (31 Oct - 12 Nov), one week ahead: the events overlay was ahead of the list on 12 days; the all-themes overlay was behind it on 8. Three weekly decisions, two arms, opposite answers.

**Reading:** no news weight beats chance yet. So the news is built to learn and to be measured in the open, and it moves nothing.

## S1 - WHAT
1. **The archive.** `archive_news_themes.py` merges one row per UTC day and theme into `tfb_archive.news_theme_daily`, counted in the public GDELT tables:
   - events (by the day an event was added; events dated that day or the day before only): `iran_war`, `russia_war`, `us_china_coercion`, `us_china_tone`;
   - documents (by publication day): `fed_rates`, `trade_policy`, `uncertainty`, `election`, `ai_tech`, `ai_tone`, `market_tone`, `inflation`, `oil_price`, `armed_conflict`.
   Each row is a numerator and a denominator, so a week is an exact sum. One atomic MERGE per source; a day loaded twice is the same row.
2. **The weights (`v_news_sector_weights`).** Per sector and theme, the slope of next week's sector excess return on the theme's surprise, fitted on pairs up to two weeks back only. A weight is active only while its own earlier predictions correlate with what happened (26+ scored weeks, correlation above 0). A weight that stops working switches itself off, and on again if it recovers.
3. **The tilt (`v_news_sector_tilt`).** Two arms (`events`, `all`). The three best-scored sectors with a score above 0 are favoured; the three worst with a score under 0 are avoided. It applies from the Tuesday after the news week to the following Monday.
4. **The shadow list (`v_news_overlay_list`).** Investable names of an avoided sector leave; for each one, a WATCHLIST name rated HOLD from a favoured sector, LOW or MID volatility, comes in by overall score.
5. **The trial (`v_trial_news_overlay`).** The news version against the list itself, 1W and 2W. Evidence only from list days of 20 Nov 2026. Promotion rule fixed now: 20 or 28 changed days, 8 news weeks, and an overlap-adjusted t of 2.5 or more. Even then the owner decides.

## S2 - What does not change
- No page, score, pick, gate, ticket or order. The live system never reads BigQuery.
- Nothing is written to the workbook.

## S3 - Cost guard
- Every source is sized with a BigQuery dry run first and refused above `--max-gb` (default 5); the MERGE carries `maximumBytesBilled`.
- The daily window (3 days) reads under 1 GB. The one-off history fill (`--start 2023-10-02 --max-gb 300`) reads about 240 GB once.

## S4 - Tests
- `pytest tests/test_archive_news_themes.py` run 3 times: 11 passed. 15 of 15 mutants caught; the file restored byte-identical.
- `python3 tests/test_bq_views.py` run 3 times: **PASS 37/37**. 18 of 18 SQL mutants caught by `case_v37` (the hash pins catch them too).
- Related files together (views, both archive scripts, manifest pins, CI verdict, runner pins, trial rules): 174 passed, 13 skipped.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.
- **Live check, read-only, 19 Nov:** the five view bodies were run in BigQuery as one query, with the table replaced by the loader's own events SELECT over three years. They returned 162 weeks (153 usable), the tilt of each week, today's shadow list, and the trial rows. In-sample, events arm, 1W: list -0.42, news version -0.14, mean daily difference +0.45, overlap-adjusted t 0.69.

## S5 - Deploy (order matters: the views need the table)
1. Merge.
2. **BigQuery News Themes**, `write=true`, `start=2023-10-02`, `max_gb=300` (the history fill, once).
3. **BigQuery Looker Views**, `mode=write`, then `mode=verify`.
4. From then on the daily run at 01:20 UTC keeps the table current. Kill switch: repository Variable `TFB_BQ_NEWS_THEMES=off`.
