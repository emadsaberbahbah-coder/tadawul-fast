# Commit Sheet - `scripts/apply_bq_views.py` v1.22.0 and `sql/bigquery/114_v_learning_scorecard.sql` - 2026-11-22

**Scope:** owner, Sun 22 Nov 2026, 12:23 Riyadh: "check the system is getting learning day over day and how we can improve and accelerate the learning ... how system can learn from the previous loss". And 13:27: "build what you need or recommend ... go ahead".

**Files:**
- `sql/bigquery/114_v_learning_scorecard.sql` (new): one view, research only.
- `scripts/apply_bq_views.py` 1.21.0 -> 1.22.0: header note and version. No logic changes; no file of 1.21.0 changes.
- `scripts/verify_deployment.py`: pin moved to 1.22.0.
- `tests/test_bq_views.py`: pins moved (82 objects, 54 trailing views); `case_v57` / `test_v57` added.

## S0 - WHY
- The review of 22 Nov found three things about how the system learns:
  - Calls are scored every day (`v_outcome_engine`), but no rule is ever re-tested by itself. Every finding so far was a one-off query.
  - A rule needs about 20 call days before it can be judged. That clock cannot be shortened. What can change is how many rules run on it at once: until now, one at a time.
  - The losses were not studied as a set: which rule would have avoided them, and at what cost in missed winners.
- This view runs ten rule variants on the same clock, every day, against the same baseline.

## S1 - WHAT
- **The list of variants is fixed on 22 Nov 2026, before any forward data.** Changing a variant later means a new arm name; old rows keep their meaning.

| Arm | Rule | Compared with |
|---|---|---|
| B0 | every INVESTABLE buy | the baseline itself |
| E1 | the market is not on the armed hold list (weak, stress, drawdown, crash_day) | B0 |
| E2 | the hold list without 'weak' | B0 |
| E3 | without Tadawul | B0 |
| E4 | the selection gate (upside 15% or more, risk under 30) | B0 |
| E5 | both gates (E1 and E4) | B0 |
| X1, X2, X3 | a stop at 5%, 8%, 10% on daily closes | the same calls held to the horizon |
| S1 | the sell side: rated SELL, REDUCE or AVOID, counted as right when the price fell | buy-rated calls, same measure |

- **Columns:** period, horizon (each horizon and `ALL`), arm, calls, days, hit_pct, mean_pp, worst5_pp, base_calls, base_hit_pct, base_mean_pp, lift_pp, t_stat, first_day, last_day, reading.
- **Two periods, never mixed:** `in-sample, not evidence` (calls before 23 Nov 2026) and `forward from 2026-11-23`. The date is written into the view; the view reads no clock.
- **Lift:** the mean, over call days, of the arm's mean result minus the baseline's on the same day. Days are the unit because calls made on one day move together.
- **Reading (forward rows only):** `TOO FEW TO JUDGE` below 20 call days or 40 calls; then `BETTER THAN THE BASELINE` at t >= 2, `WORSE THAN THE BASELINE` at t <= -2, otherwise `NO CLEAR DIFFERENCE`. An in-sample row always reads `in-sample, not evidence`.

## S2 - No look-ahead
- The market label a call is judged on is the one of its market's last close **before** the day of the call (`c.date < o.recorded_date`). This is what the engine can read when it makes the call.
- The selection values (upside, risk) are the last signal recorded **on** the day of the call. A call with no signal that day is not counted as selected.
- A stop books exactly the stop (never better), when the worst daily close of the holding period reached it. Intraday lows are not in the archive, so a real stop would have been hit more often than this.

## S3 - Safety
- Research only. The view is read by people and by the site; the live system never reads BigQuery. It changes no gate, no recommendation and no order.
- It is installed only by a `bq_views.yml` run with mode=write, after merge.
- Cost: one read of the view bills about 0.62 GB (measured on 22 Nov 2026).

## S4 - First reading (in-sample, all horizons, run on the final file on 22 Nov 2026)
| Arm | Calls | Days | Hit % | Mean % | Worst 5% | Lift | t |
|---|---|---|---|---|---|---|---|
| B0 every INVESTABLE buy | 2,569 | 120 | 50.4 | -0.11 | -14.59 | | |
| E1 not on the hold list | 1,206 | 87 | 53.6 | +0.69 | -12.62 | +0.88 | 3.07 |
| E2 hold list without weak | 1,769 | 104 | 53.3 | +0.69 | -13.39 | +0.81 | 3.22 |
| E3 without Tadawul | 1,938 | 106 | 55.9 | +0.96 | -12.28 | +1.09 | 4.12 |
| E4 selection gate | 761 | 61 | 61.8 | +2.17 | -5.81 | +2.33 | 8.32 |
| E5 both gates | 378 | 48 | 69.0 | +2.76 | -4.01 | +2.52 | 6.18 |
| X1 stop at 5% | 2,569 | 120 | 45.3 | +0.43 | -5.00 | +0.64 | 3.30 |
| X2 stop at 8% | 2,569 | 120 | 48.4 | +0.21 | -8.00 | +0.34 | 1.95 |
| X3 stop at 10% | 2,569 | 120 | 49.3 | +0.12 | -10.00 | +0.19 | 1.18 |
| S1 sell side | 2,092 | 116 | 58.7 | +0.98 | -13.25 | +0.16 | 0.51 |

- These rows are in-sample: the gates were chosen on the same calls. They are a starting point, not proof. The forward rows start empty on 23 Nov.
- S1: 58.7% of sell-rated calls fell, against 47.5% of the 5,185 buy-rated calls. The daily lift is small (+0.16, t 0.51), so the sell rating is not yet shown to add return.
- The stop rows cover closes only; see S2.

## S5 - Checks
- `python -m pytest -q tests/test_bq_views.py tests/test_verify_manifest_pins.py`: 60 passed.
- `ruff check --select E9,F63,F7,F82`: clean on the three changed Python files.
- `apply_bq_views.check_objects`: no finding on 82 objects.
- The view's body was run against BigQuery from the committed file (table above).

## S6 - After merge
1. Dispatch `bq_views.yml` with mode=write to install the view.
2. The site's Learning page reads it and shows the forward rows as they fill.
3. When the forward rows fill (horizons in the archive: 1W, 2W, 1M, 3M; measured on 22 Nov 2026, the latest scored 1W day was 13 Nov, nine days back):
   - first forward rows: about 2 Dec 2026 (1W calls of 23 Nov);
   - first reading at 20 scored call days, 1W, wide arms (B0, E1-E3, X1-X3, S1): the last days of December 2026;
   - narrow arms (E4, E5) had calls on about half of the days in-sample, so they need roughly twice as long;
   - 2W, 1M and 3M follow by their own horizon.
