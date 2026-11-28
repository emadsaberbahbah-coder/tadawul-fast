# TFB Commit Sheet — daily_plan 1.11.0 [RANKER] (the plan can rank by the analyst target upside; armed in observe)

Date: 2026-11-28 (Saturday) · Lane: GitHub Actions (🗓️ Daily Plan) · Gate: observe (workflow default) · No tab, column or ledger-layout change · No Render deploy

## Why
The owner asked on 28 Nov for a full review and a restructured engine "looking for better results". The review found that the plan ranks its candidates by the one input with no edge, and that the strongest input it already reads ranks nothing:

| Input (US, 1 month, v_signal_edge, signal days to 26 Oct) | IC | t (overlap-adjusted) | First half / second half |
|---|---|---|---|
| Analyst target upside | 0.30 | 2.59 | 0.31 / 0.30 |
| Shadow score v2 (0.6 upside + 0.4 engine forecast) | 0.29 | 2.30 | 0.28 / 0.30 |
| Engine 1-month forecast | 0.14 | 1.27 | 0.13 / 0.16 |
| Overall score (today's ranker) | −0.01 | −0.09 | 0.10 / −0.12 |

IC is the day's rank correlation with the next month's return, about 24 to 32 names a day. This is in-sample history, not a forward test. So the ranker is built as a switch, and the workflow arms observe, per the 21 Nov rule: "Everything starts in observe mode: shown and logged, but it controls no buys."

## What
`scripts/daily_plan.py` 1.10.3 → **1.11.0**. New knob `TFB_PLAN_RANKER` = `engine` (code default) | `observe` | `upside`; any other word reads `engine`. New knob `TFB_PLAN_RANKER_CCY` (default `USD`): the currencies whose candidates the upside order ranks.
1. **`analyst_upside(src)`** (pure): `(target / price − 1) × 100` from the source page, 4 places. This is the evidence rule's number and v_signal_edge's `target_upside`. It is None without a positive price and a positive target.
2. **`upside_order(cands, ccy)`** (pure): the candidates priced in `ccy` that carry an upside, highest first, ties by engine rank. The rest follow in engine order.
3. **`would_pick(order, n, max_per_sector)`** (pure): the first n of an order under the day's sector cap alone.
4. **`decide()`:**
   - `engine`: picks, sizes and labels are unchanged; the plan only gains a `ranker` summary.
   - `observe`: the picks are the engine's. Each candidate gets a label:
     - `[observe] analyst upside +x% - upside order n of m`;
     - or, when it has none, `[observe] no analyst upside (no target price on its source page)` / `[observe] no upside order (priced in X)`.
     - The names the upside switch would pick read `[observe] the upside ranker would pick this (before the cash and held-sector checks)`. The preview takes the switch's own order (a kept pick first, a name under the entry level last), then the day's sector cap (Codex P2 on #1112).
   - `upside`: the candidates are taken in the upside order. A name kept from the previous plan still comes first, and a low-data name still comes last. A pick reads `ranked by analyst upside +x% (engine rank n)` and a waiting name `next in line (upside order n; engine rank m)`.
   - Every gate before the order is unchanged: the regime gate, the forecast cap, the evidence rule, currency and funding, the earnings and ex-dividend windows, the entry rule, the sector caps and the cash checks.
5. **Run log, counts only:** `ranker observe: a of b candidate(s) carry an analyst upside; the upside order would pick c name(s), d of them among today's e pick(s)`. The line is not printed in `engine`, and no symbol is printed.

`.github/workflows/daily_plan.yml`: `TFB_PLAN_RANKER: ${{ vars.TFB_PLAN_RANKER || 'observe' }}` and `TFB_PLAN_RANKER_CCY: ${{ vars.TFB_PLAN_RANKER_CCY || 'USD' }}`. `scripts/verify_deployment.py`: the pin moves to 1.11.0. No ci.yml change: `tests/test_daily_plan.py` is already in its lanes.

## How it is judged
The forward evidence already exists and was fixed before any forward day:
- `v_trial_c2` (fixed 19 Nov; calls from 20 Nov): buy calls in the top half of their day by analyst target upside, against the champion;
- `v_trial_shadow_v2` (from 19 Nov);
- the target-upside IC in `v_signal_edge` on signal days after 19 Nov.

The observe labels show the owner each morning which names the upside order would buy, beside the engine's. Switching to `upside` is the owner's decision once those readings are in (1-week reading about 17 Dec, 2-week about 31 Dec). Rollback: `TFB_PLAN_RANKER=engine`.

## Tests
- `tests/test_daily_plan.py`: 73 pass. The eight new R tests:
  - R1: knob parsing (including `on` and blank read `engine`), the currency list, `analyst_upside` and its None cases, `upside_order` ties and the rest by rank, `would_pick` under the sector cap;
  - R2: engine is the default and adds no label;
  - R3: observe keeps the picks, quantities, entries, stops and allocations, labels every candidate, names the would-picks, and gives the `ranker` summary;
  - R4: upside reorders the fixture's picks (rank 3, upside +6 %, is replaced by rank 4, +28 %), labels the picks, and gives the waiting order;
  - R5: the kept pick still leads; a currency outside `TFB_PLAN_RANKER_CCY` is not ranked; the regime gate still holds US buys;
  - R6: the run-log line gives counts and no symbol, and is absent in `engine`;
  - R7: the workflow arms observe, and the version and header are checked;
  - R8: the observe preview equals the upside switch's picks with a kept pick and with names under the entry level.
- Changed with a reason: W1's version assertion moves to 1.11.0.
- Related suites pass: `test_today_action_plan`, `test_markets_board`, `test_verify_manifest_pins`, `test_bq_views`, and `test_trial_shadow_v2_rule`. That last guard holds that no live script names the shadow score, so the code header names `v_trial_c2` and `v_signal_edge` only. Workflow audit: errors 0, warnings 0.
- Twenty-one hand mutants: twenty fail a test. The one that lives computes the upside order in `engine` mode too, which changes nothing anyone can see (an equivalent mutant).

## Owner
Merge when ready; nothing to set. The workflow arms observe. Setting the repository variable `TFB_PLAN_RANKER=upside` is the live switch and stays your decision after the forward readings.

A day's plan is frozen once cut, so the first observe labels and the counts line come with the next day's cut after the merge. Claude reads that run and reports the line.
