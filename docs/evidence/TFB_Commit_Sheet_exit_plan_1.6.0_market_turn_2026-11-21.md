# Commit sheet - a holding whose market is not calm is flagged for sale (exit_plan 1.6.0, 21 Nov 2026)

Branch `feat/exit-market-turn`, against main after #1051 (merged 21 Nov 2026 19:23 UTC). **Observe by default: the Exit Plan shows the flag and logs it; no action, stop or target changes** until the repository variable `TFB_EXIT_MARKET_TURN=on`. The broker is never called.

| File | Version |
|---|---|
| `scripts/exit_plan.py` | 1.5.0 -> **1.6.0** (`[MARKET-TURN]`; the ledger gains the column Market) |
| `.github/workflows/exit_plan.yml` | 1.5.0 -> **1.6.0** (`TFB_EXIT_MARKET_TURN` observe, `TFB_EXIT_MARKET_TURN_LABELS`) |
| `scripts/verify_deployment.py` | pin exit_plan 1.6.0 |
| `tests/test_exit_plan.py` | X19 (7 tests); two pins to 1.6.0 |

No new test file, so `ci.yml` is unchanged.

## Why

The owner, 21 Nov 2026 22:45 Riyadh:

> "maybe in the stable phase most stocks profit, but in the crash phase I think the system had better say sell before the loss, and also wait for the better time to buy to take profit in the rise - or are there better plans"

and at 22:51, on the plan sent in reply: "consider my approval your choice of the best and most suitable decision for the project".

## The evidence

**Our own picks.** The matured INVESTABLE records (`v_outcome_excess`, bar-based return from the entry price), each with the label of its market at the last close before the record. The rule: sell at the next close after the first close, inside the holding window, on which the stock's market was not calm. Against holding to the target:

| Entered | Records | Rule exited | Won: hold -> rule | Mean: hold -> rule | Worst 5 %: hold -> rule |
|---|---:|---:|---|---|---|
| market calm | 1,166 | 841 | 52.2 % -> **54.1 %** | +0.49 % -> **+0.61 %** | -13.6 % -> **-7.8 %** |
| market not calm | 1,367 | 1,343 | 45.0 % -> **47.6 %** | -1.24 % -> **-0.27 %** | -16.1 % -> **-5.2 %** |

Selling only on drawdown / crash_day: for calm entries 52.2 % -> 52.7 %, mean +0.49 % -> +0.46 %, worst 5 % unchanged.

**The indices themselves** (daily, the label of each close deciding the next day):

| Market | Hold: return / worst drawdown | Out on drawdown or crash | In only when calm |
|---|---|---|---|
| US (Nov 2023 - Nov 2026) | +53.0 % / -19.0 % | +37.1 % / -14.0 % | +31.3 % / -8.7 % |
| Saudi (Jun 2024 - Nov 2026) | -13.9 % / -24.2 % | -9.3 % / -9.9 % | -6.8 % / -7.5 % |
| Europe | +19.2 % / -16.6 % | +12.7 % / -10.3 % | +1.5 % / -12.5 % |
| Japan | +44.5 % / -26.3 % | +33.9 % / -23.2 % | +7.9 % / -20.9 % |
| Hong Kong | +14.6 % / -20.0 % | -11.1 % / -24.0 % | -17.6 % / -29.2 % |

Reading: **the rule protects a holding; it is not a reason to sit in cash.** On the indices, staying out whenever the market is not calm halves the drawdown but gives up much of the gain (the rebounds come before the label turns calm again), except in Saudi Arabia's long decline. On our picks, which do worse than their market when it is not calm, selling at the first turn cut the worst losses by half or more and raised the mean. Re-entry is the engine's job: from 5.161.0 a new buy needs a calm market.

## What is built

- Each holding of the US (`.US`, a bare ticker, a share class), Saudi, European, Japanese or Hong Kong / China market carries the label of its market on the last completed session, read live by `core.market_condition` 1.0.0 (the rules of `99_v_market_condition`; checked against the view on 80 real closes, all equal). Commodities, currencies, indices, bonds, sukuk, crypto and futures carry none.
- When the label is in `TFB_EXIT_MARKET_TURN_LABELS` (default `weak,stress,drawdown,crash_day`):
  - **observe** (default): the note "market not calm (observe): the US market on a crash day as of 2026-11-20 (S&P 500 fund -3.6% on the day) - the market-turn rule would sell; nothing changes until TFB_EXIT_MARKET_TURN=on"; the summary line adds "market not calm: N holdings (observe)";
  - **on**: the action is SELL, with "the market turned: ... - sell (market-turn rule)" first in its reasons; the Today Action Plan copies it as "Sell all ... of ...";
  - **off**: nothing is read; the page is the 1.5.0 page.
- A lot with no close since its purchase waits for the first one, as the close rules do.
- The page's rule line states the market-turn rule and its mode.
- **Logged for scoring:** the ledger `_Exit_Plan` gains the column **Market** (`market:label:date`, e.g. `us:crash_day:2026-11-20`) on every lot and run, so the rule can be scored on the real book at the 8 Dec review. A 1.5.0 ledger (17 columns) is read as before and rewritten with the new column; its stops of record carry over. The two other readers of the ledger (Portfolio_Decision's `pdExitStopsFromValues_`, `consistency_check`) find their columns by name.

## Checked

- `tests/test_exit_plan.py` 44 passed (X19: the gate and its labels; which holdings carry a label; observe, on, off; a lot that has not closed; the live read and its faults; a 1.5.0 ledger read and carried with its stop; the workflow knobs).
- Every suite that imports or reads the Exit Plan (daily plan, today action plan, holding read, market regime, consistency check, plan archive, sheets quota, page chains, Portfolio_Decision's stop reader): 362 passed; the Node harness of the stop reader 8/8.
- `core.market_condition` imports in a bare virtual environment (the workflow installs only gspread, google-auth and requests).

## Arming (owner)

Nothing to do for observe: it starts with the first run after the merge. To make the flag the action: repository variable `TFB_EXIT_MARKET_TURN=on` - recommended only after the 8 Dec review, on the book's own record. Rollback: `TFB_EXIT_MARKET_TURN=off`.

**Today, as of the 20 Nov closes:** all seven holdings are US shares and the US closed on a crash day, so all seven would carry the flag.
