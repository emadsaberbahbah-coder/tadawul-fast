# Commit Sheet - `scripts/apply_bq_views.py` v1.13.0: the standing lost-opportunity comparison - 2026-11-19

**Scope:** owner, Thu 19 Nov 10:04 Riyadh, verbatim: "make better compsion between the ROI as the investable recommdadtion and the best avalaible in the market as actual result and what we are missed to be consider in the future as well". The 19 Nov review had to rebuild this comparison by hand. These views make it available every day.

**Files:**
- `sql/bigquery/88_v_forward_universe.sql` (new)
- `sql/bigquery/89_v_investable_vs_market.sql` (new)
- `sql/bigquery/90_v_missed_winners.sql` (new)
- `scripts/apply_bq_views.py`: 1.12.0 -> 1.13.0, header only.
- `scripts/verify_deployment.py`: pin moved to 1.13.0.
- `tests/test_bq_views.py`: three PENDING hashes, the order and kinds lists, and new `case_v36`.

## S1 - WHAT
1. **v_forward_universe.** Covers every liquid equity on the market pages (Global_Markets and Market_Leaders, a 30-day traded value of at least 1 million in local currency) for every archived day. Each row carries:
   - the forward 1W and 2W price return (to the first archived price from day +7 or +14, up to 6 days later);
   - that day's status, recommendation and block reason;
   - the sector, with the six provider spellings folded to GICS as `v_symbol_sector` does;
   - the volatility tercile;
   - the return rank and the market median.
2. **v_investable_vs_market.** One row per day and horizon for each group: MARKET, INVESTABLE, BUY_CALL, BEST_DECILE, SECTOR/x, VOL/band, INV_SECTOR/x and INV_VOL/band. Each row gives n, mean, median, the median versus the market median, the share above the market median, and the best name.
3. **v_missed_winners.** Each day's 20 best risers per horizon, each with the cause it was not a pick: picked, blocked (with the reason), rated SELL / STRONG_SELL / REDUCE / AVOID, rated buy but not investable, or watchlist.

## S2 - What does not change
These are views only, read by Looker and by the reviews. No table, page, score, pick, gate or order changes, and the live system never reads BigQuery.

## S3 - Tests and evidence
- `python3 tests/test_bq_views.py` run 3 times: **PASS 36/36**.
- 7 mutants caught: the 20-row cut, the BLOCKED branch, the decile line, the "above median" comparison, the horizon window, the outlier cap and one sector fold.
- `pytest tests/test_bq_views.py`: 43 passed together with the verifier tests.
- Workflow audit: errors=0 warnings=0.
- The same SQL, run read-only on 19 Nov over the archive (31 Oct - 19 Nov, in-sample):
  - at one week, the investable list's median was 0.18 points under the market median, with 48.5% of its names above it;
  - the low-volatility tercile was +0.37 and the high tercile -1.51;
  - the best tenth was +11.6;
  - none of the day's 20 best risers was ever a pick; 240 of 260 were rated SELL, STRONG_SELL, REDUCE or AVOID, with an average 30-day volatility of about 110%.

## S4 - Deploy
After merge, dispatch **BigQuery Looker Views** with `mode=write`, then `mode=verify`.

## S5 - Codex review on #1029 (both P2 verified and fixed)
1. **Group and market medians used different estimators.** The group median was APPROX_QUANTILES; the market median was PERCENTILE_CONT. Both are now PERCENTILE_CONT 0.5.
   - Verified on the archive: MARKET now reads 0.00 against itself on every day.
   - INVESTABLE now reads -0.18 (1W) and -0.03 (2W).
2. **BEST_DECILE could empty on a tie at the cut.** It is now the day's best CEIL(n_day / 10) by `ret_pos` (ties broken by symbol).
   - `v_forward_universe` gains `ret_pos` and `n_day`.
   - Two more mutants are caught.
