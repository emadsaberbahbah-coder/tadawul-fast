# TFB Commit Sheet — exit_plan 1.8.0 [NEWS-SHADOW] (the stop without the news, recorded; audit C1)

Date: 2026-11-27 (Friday) · Lane: GitHub Actions (Exit Plan) · Observe only: no rule, page, mail or action changes · No Render deploy

## Why
Finding C1 of the outside engineering audit the owner uploaded on 26 Nov. A negative name item of severity H or M and information grade 1–3 tightens the trail from 3×ATR to 2×ATR. Grade 3 means few or unconfirmed sources, and the rule has never been measured: it may sell holdings on news that did not matter. Changing a live stop rule needs the owner's decision, so this release only **records** what the stop would have been without the news, so the two can be compared.

## What
`scripts/exit_plan.py` 1.7.1 → **1.8.0**.
- `exit_levels()` computes `stop_no_news`. It uses the same entry, ATR, highest close, breakeven and initial stop, the same broker floor and the same frozen-offer stop, with the negative-news reason taken out. Any other tightening reason (forecast below the close, close under the 20-day average after +2R) still tightens it. It follows its own history (the highest recorded for the lot, never down) and is never above the stop of record.
- The `_Exit_Plan` ledger gets a 20th column, **Stop Without News**. A 1.7.x ledger (19 columns) is read as before, and its rows' own Stop stands in for the shadow (`prior_row` → `StopNN` / `StopNNNorm`).
- The run log gives counts only: lots tightened by negative news, and how many of them have a stop above the stop without the news.
- **Unchanged:** the stop of record, the actions, the Exit_Plan page, the mail, Risk Watch and My_Portfolio. They read the ledger by exact header (`Stop`), and the new column is the last one.

## Tests
- `tests/test_exit_plan.py` + `tests/test_exit_plan_offers.py`: 64 pass, also with `GITHUB_ACTIONS=true`.
- New X30 tests:
  - with no news the shadow equals the stop;
  - with news alone the shadow keeps the 3×ATR trail;
  - with news plus another reason the shadow tightens too;
  - the shadow's own history, never above the stop of record;
  - a 1.7.x row lends its Stop;
  - the broker floor applies to both;
  - the run writes the column and logs counts without symbols, and the page does not show it.
- Changed with a reason: the ledger shape assertions (19 → 20 columns) and the version pins.
- Five hand mutants each fail a test:
  - the shadow keeping the news;
  - ignoring its own history;
  - `prior_row` dropping it;
  - the ledger dropping it;
  - no broker floor.

## Owner
Merge when you have read it. Nothing to set. After a few weeks with news on holdings, compare the two stops on what the price did next, then decide whether grade 3 keeps tightening.

## Rollback
Revert to 1.7.1. The extra ledger column is ignored by 1.7.1 (it reads the first 19).

Not a licensed financial adviser; the owner decides and executes every trade.
