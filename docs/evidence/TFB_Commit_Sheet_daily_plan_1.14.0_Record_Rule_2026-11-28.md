# TFB Commit Sheet — daily_plan 1.14.0 [RECORD-RULE] (the record holds what lost, and puts the best third first)

Date: 2026-11-28 (Saturday) · Lane: GitHub Actions (🗓️ Daily Plan) · Gate: **a live buy rule changes on merge** (`TFB_PLAN_HONEST` defaults to `enforce` in the workflow) · No Render change

## Why
Owner, 28 Nov 2026 at 21:40 Riyadh, in Arabic: apply what was found, so the recommendations improve directly; he approves and executes.

What was found that day, on the scored buy calls (Forecast_Record, cuts fixed as constants on 28 Nov):

| US one-month calls | Calls | Delivered | Rose |
|---|---|---|---|
| Lowest third of promises | 153 | −1.28 % | 38 % |
| Middle third | 153 | +1.42 % | 54 % |
| Highest third | 153 | +3.26 % | 72 % |

Every third of the Saudi calls lost money. Until now the plan showed these numbers beside each name and bought the lowest third as readily as the highest.

## What
**`scripts/daily_plan.py` 1.14.0** — `TFB_PLAN_HONEST` gains a third word, `enforce`:
- **Hold:** a candidate whose one-month record reads "lost money" is not picked. It waits with its reason, for example "the record: US buy calls like it lost money over 1M (-1.3% on 153 calls)". The record is the name's own third, or its whole market when the third has too few calls.
- **Order:** among the others, the highest third of promises comes first, then the middle third, then a name the record cannot measure. The engine's rank (or the upside order) decides inside each.
- A kept pick still comes first, and a name under the entry level still comes last.
- A name the record cannot measure is never held by it. A name another gate already holds keeps that gate's reason.
- The run log counts the names held (no symbol).

**`.github/workflows/daily_plan.yml` 1.11.0** — `TFB_PLAN_HONEST: ${{ vars.TFB_PLAN_HONEST || 'enforce' }}`. Merging this arms the rule.

## Not changed
- `observe` and `off` are 1.13.0 to the word (3,000 random cases: 0 differences in picks, sizes, stops, waiting reasons and labels).
- Sizes, stops, targets, the cash floor, the sector limits, the market rule and every other gate.
- The labels on each name, and the upside arm (it goes through the same hold and order).

## Limits of the evidence
The thirds were cut on the same calls that show the difference, so this is a record, not a test on later calls. The forward rows (calls from 29 Nov) and the paper book judge it. The rule only removes and reorders candidates; it adds no position and no size.

## Evidence
- `tests/test_forecast_record.py`: `test_r1` (what lost is held, with its reason; every other gate's reason stands; observe holds nothing), `test_r2` (the highest third first; a kept pick before it), `test_r3` (off and observe pick as before; the log counts only; no record read means nothing held).
- Hand mutants: 9 of 10 killed; the tenth is a redundant guard (an unmeasured name has no reading).
- `tests/test_daily_plan.py` + `tests/test_forecast_record.py`: 99 passed. Related files (action plan, consistency check, markets board, workflow audit, manifest pins): passed. Workflow audit: 0 errors, 0 warnings.

## After the merge
Read the next cut's "honest enforce" line: names labelled, names held by the record.

## Rollback
Repository variable `TFB_PLAN_HONEST=observe` (the owner's own action), or `git revert`.
