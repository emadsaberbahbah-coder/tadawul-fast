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
| X1, X2, X3 | a stop at 5%, 8%, 10% under the entry, hit when a daily low reaches it | the same calls held to the horizon |
| S1 | the sell side: rated SELL, REDUCE or AVOID, counted as right when the price fell | buy-rated calls, same measure |

- **Columns:** period, horizon (each horizon and `ALL`), arm, calls, days, hit_pct, mean_pp, worst5_pp, base_calls, base_hit_pct, base_mean_pp, lift_pp, t_stat, first_day, last_day, reading.
- **Two periods, never mixed:** `in-sample, not evidence` (calls before 23 Nov 2026) and `forward from 2026-11-23`. The date is written into the view; the view reads no clock.
- **Lift:** the mean, over call days, of the arm's mean result minus the baseline's on the same day. Days are the unit because calls made on one day move together.
- **Reading (forward rows only):** `TOO FEW TO JUDGE` below 20 call days or 40 calls; then `BETTER THAN THE BASELINE` at t >= 2, `WORSE THAN THE BASELINE` at t <= -2, otherwise `NO CLEAR DIFFERENCE`. An in-sample row always reads `in-sample, not evidence`.

## S2 - No look-ahead
- The market label a call is judged on is the one of its market's last close **before** the day of the call (`c.date < o.recorded_date`). This is what the engine can read when it makes the call.
- The selection values (upside, risk) come from the call's **own** signal snapshot: the newest snapshot of the symbol on the call's day that is not later than 60 seconds after the call.
  - Both days are Riyadh days, as the calls' `recorded_date` is.
  - Why 60 seconds: the engine writes a call and, a second or two later, its signal snapshot. Measured on 22 Nov 2026 on the 2,569 INVESTABLE buys: every one has a snapshot within 12 seconds, and for 2,291 the snapshot follows the call. A strict "not later than the call" would drop those 2,291.
  - A later run of the same day is never read. A call with no such snapshot is not counted as selected.
- A stop books exactly the stop, when the lowest daily **low** after the recording day, up to the target bar, reached it (`v_outcome_engine.mae_pct`). This is how a resting stop order behaves. It counts no gap below the stop and no commission, so it is an estimate.

## S3 - Safety
- Research only. The view is read by people and by the site; the live system never reads BigQuery. It changes no gate, no recommendation and no order.
- It is installed only by a `bq_views.yml` run with mode=write, after merge.
- Cost: one read of the view bills about 0.62 GB (measured on 22 Nov 2026).
- The worst 5% is exact (the result at rank floor(5% of (n - 1))), so two reads of the same rows agree.

## S4 - First reading (in-sample, all horizons, run on the final file on 22 Nov 2026)
| Arm | Calls | Days | Hit % | Mean % | Worst 5% | Lift | t |
|---|---|---|---|---|---|---|---|
| B0 every INVESTABLE buy | 2,569 | 120 | 50.4 | -0.11 | -14.57 | | |
| E1 not on the hold list | 1,206 | 87 | 53.6 | +0.69 | -12.62 | +0.88 | 3.07 |
| E2 hold list without weak | 1,769 | 104 | 53.3 | +0.69 | -13.28 | +0.81 | 3.22 |
| E3 without Tadawul | 1,938 | 106 | 55.9 | +0.96 | -12.26 | +1.09 | 4.12 |
| E4 selection gate | 755 | 64 | 62.5 | +2.28 | -5.22 | +2.54 | 8.80 |
| E5 both gates | 381 | 51 | 69.6 | +2.84 | -3.82 | +2.71 | 6.52 |
| X1 stop at 5% | 2,569 | 120 | 45.3 | +0.43 | -5.00 | +0.64 | 3.30 |
| X2 stop at 8% | 2,569 | 120 | 48.4 | +0.21 | -8.00 | +0.34 | 1.95 |
| X3 stop at 10% | 2,569 | 120 | 49.3 | +0.12 | -10.00 | +0.19 | 1.18 |
| S1 sell side | 2,092 | 116 | 58.7 | +0.98 | -13.16 | +0.16 | 0.51 |

- These rows are in-sample: the gates were chosen on the same calls. They are a starting point, not proof. The forward rows start empty on 23 Nov.
- S1: 58.7% of sell-rated calls fell, against 47.5% of the 5,185 buy-rated calls. The daily lift is small (+0.16, t 0.51), so the sell rating is not yet shown to add return.
- One-week horizon alone: B0 51.4% on 858 calls; E5 70.9% on 151.

## S5 - Checks
- `python -m pytest -q tests/test_bq_views.py tests/test_verify_manifest_pins.py`: 60 passed.
- `ruff check --select E9,F63,F7,F82`: clean on the three changed Python files.
- `apply_bq_views.check_objects`: no finding on 82 objects.
- The view's body was run against BigQuery from the committed file (table above).
- `record_id` is one row per call: B0 counts 2,569 before and after the join to the snapshot.

## S6 - After merge
1. Dispatch `bq_views.yml` with mode=write to install the view.
2. The site's Learning page reads it and shows the forward rows as they fill.
3. When the forward rows fill (horizons in the archive: 1W, 2W, 1M, 3M; measured on 22 Nov 2026, the latest scored 1W day was 13 Nov, nine days back):
   - first forward rows: about 2 Dec 2026 (1W calls of 23 Nov);
   - first reading at 20 scored call days, 1W, wide arms (B0, E1-E3, X1-X3, S1): the last days of December 2026;
   - narrow arms (E4, E5) had calls on about half of the days in-sample, so they need roughly twice as long;
   - 2W, 1M and 3M follow by their own horizon.

## S7 - Review findings on the first commit (Codex, 22 Nov) and what changed
All three were checked against the code and the data, and all three were right.

| Finding | Check | Change |
|---|---|---|
| The stop arms were described as daily closes; `mae_pct` is built from daily lows | `70_v_outcome_engine.sql`: `win_low` takes `a.tb.low` | Wording corrected everywhere (view, sheet, test, site, report). The SQL of the arms is unchanged: a low-touch stop is the realistic one |
| Signals were dated in UTC, calls in Riyadh time | 5,360 of 39,762 signals fall on a different day in UTC | `DATE(recorded_at, 'Asia/Riyadh')` |
| The day's last snapshot could post-date the call | 2,203 of the 2,569 INVESTABLE buys have several snapshots that day | The call's own snapshot: the newest not later than 60 seconds after the call (see S2) |

Effect on the first reading: E4 761 calls, 61.8% became 755 calls, 62.5%; E5 378 calls, 69.0% became 381 calls, 69.6%. No other arm moved, except the worst 5% figures, which are now exact.
