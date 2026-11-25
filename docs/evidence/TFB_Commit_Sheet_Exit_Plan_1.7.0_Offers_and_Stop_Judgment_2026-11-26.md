# TFB Commit Sheet — Exit Plan 1.7.0 [OFFER] [STOP-JUDGMENT], Today Action Plan 1.9.0, core/corporate_offers 1.0.0

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions page jobs (Python) · Gates: `TFB_EXIT_OFFERS` on, `TFB_EXIT_STOP_JUDGMENT` observe, `TFB_TAP_OFFERS` on · No Render deploy · No order is placed, changed or cancelled

## Why (owner, 26 Nov 2026, Riyadh)
- 01:07 — after a stop filled in the first seconds of a session that closed higher: is the fall temporary or lasting, should stops be adjusted or kept; the stop on the tender holding was removed on purpose ("a sell order is executed while a purchase offer stands at a fixed price, so we lose for no reason"); "the system must have reason, wisdom and flexibility".
- 01:15 — "the sale there will mostly be a manual decision, waiting for the best price before the final offer".
- 01:37 — "please do the required build now".

## What was wrong (the 00:42 run)
| Page | Reading on the tender holding | Why it was wrong |
|---|---|---|
| Exit_Plan | stop trailed to 1.8 % under the price; target above the offer price; "market not calm: the market-turn rule would sell" | a price pinned to an offer has a shrinking ATR, cannot pass the offer, and does not follow the market |
| Today_Action_Plan, Final_Decisions | "No stop order is working at IBKR: place one at … for all … shares" | the owner removed that stop on purpose; a sell order on tendered shares can open a short position |
| Holding_Read | no longer PINNED BY AN OFFER | it knows an offer only while the day's news carries it, so the 1.8.0 caution had gone |

## Change
| # | File | Edit |
|---|---|---|
| 1 | `core/corporate_offers.py` **1.0.0** (new) | the registry reader: `parse`, `load` (never raises), `standing`, `lapsed`, `close_before`, `reading` (gap, days left, implied completion), `label`. Stdlib only, pure apart from `load`. |
| 2 | `config/corporate_offers.json` (new) | schema `tfb.corporate-offers.v1`; one entry, the cash tender already named in `corporate_actions_confirmed.json`, `sale: manual`. Public offers, never holdings. |
| 3 | `scripts/exit_plan.py` **1.6.3 → 1.7.0** | **[OFFER]** a holding with a standing offer, sale manual: stop of record frozen at its value before this close, target = offer price, no RAISE STOP / TAKE PROFIT / price-rule SELL, no market-turn note; Why opens with the offer; REVIEW when the price doubts the offer (implied completion < 50 %, or > 8 % under it with no close before the offer), the market pays it (within 0.5 %), or 5 days or fewer remain. Sale rule: price rule kept, target capped, the offer named. A passed last day: "record its outcome". **[STOP-JUDGMENT]** (observe) the holding's 15-session move beside its market's own reading; a fall of 8 % is named with its market / on its own / mixed; the rule under test would wait only on a fall with the market inside a 5-point margin and above a 15 % hard limit. Ledger column 19 `Judgment`; the 18- and 17-column ledgers still read. |
| 4 | `scripts/today_action_plan.py` **1.8.2 → 1.9.0** | reads the same registry: a manual-sale offer holding gets no stop step and is left out of "confirm each holding's stop"; REVIEW is one decision step; a stop that is working on it is named, nothing asked; FINAL DECISION, order status and the section B note say why. Any registry offer carries the 1.8.0 tender caution. |
| 5 | `.github/workflows/exit_plan.yml` 1.7.0, `today_action_plan.yml` | the three gates as repository variables with their defaults |
| 6 | `scripts/verify_deployment.py`, `tests/test_verify_manifest_pins.py`, `.github/workflows/ci.yml` | pins `exit_plan 1.7.0`, `today_action_plan 1.9.0`, `core.corporate_offers 1.0.0`; the new test file in the lean lane |

The registry file is read by the live run only (no workbook passed in), as the IBKR capture is: the suites stay independent of the file's content.

## Evidence for the judgment (archive, 26 Nov; in-sample - context, not evidence)
US shares with daily bars, Oct 2024 – Nov 2026, the first close at −8 % over 15 sessions (one event per share in 20 sessions), then 20 sessions held; selling at the fall = 0. The SQL is `sql/bigquery/131_v_stop_cause_events.sql` of the views PR.

| Cause · verdict of the rule under test | Cases | Days | Higher after 20 sessions | Mean | Against the market | t by day |
|---|---|---|---|---|---|---|
| with the market · wait | 314 | 22 | 72.0 % | +5.8 % | −0.4 pts | 0.6 |
| on its own · stop stands | 1,535 | 343 | 52.3 % | +0.7 % | +0.4 pts | 1.2 |
| on its own · hard limit (−15 %) | 86 | 70 | 40.7 % | −9.6 % | −9.8 pts | −1.6 |

After a fall with the market the shares were higher in 72 % of cases, but no better than the market itself and on 22 days only: the gain was the market's rebound, and nothing here is statistically clear. So the judgment is a note and a ledger column; it changes no action. Its forward record (`v_stop_cause_learning`, read by Learning Watch) is reviewed with the owner before it may control anything (ground rules of 21 Nov: observe, log, score, review, then switch on only what proved itself).

## Tests
| Battery | Result |
|---|---|
| `tests/test_exit_plan_offers.py` (new, 13) | registry parse / days / reading / shipped file; frozen stop, capped target; the three REVIEW triggers and their limits; sale rule; gate off = 1.6.3 result; run() page, ledger, public log, frozen stop on the next close, a passed last day; judgment readings and limits; run() note and column, off, exempt asset; ledger shapes; workflow, pins, CI |
| `tests/test_today_action_plan.py` (+2: T20) | no stop step, the REVIEW step, an old RAISE STOP row, a working stop named, sale rule, outside the days / off / no registry = 1.8.2 |
| `tests/test_exit_plan.py` (46), `test_today_action_plan.py` (58), `test_consistency_check.py`, `test_plan_archive.py`, `test_holding_read.py`, `test_market_condition.py`, `test_verify_manifest_pins.py`, `test_ci_required_verdict.py` | pass (three assertions moved with the version and the 19th ledger column) |
| ruff E9, F63, F7, F82, F401, F841 · compileall | clean |

## After merge (no owner step)
The next hourly Exit Plan and Today Action Plan runs apply it. Read-back: Exit_Plan row of the tender holding reads HOLD with "cash offer …" first in Why, target = the offer price, summary "under a cash offer, manual sale: 1"; `_Exit_Plan` has column `Judgment`; Today_Action_Plan has no "place one at" step for it and Final_Decisions reads "Hold; cash offer … no stop order is asked for."

## Rollback
Repository variables: `TFB_EXIT_OFFERS=off` (the 1.6.3 reading for every holding), `TFB_EXIT_STOP_JUDGMENT=off` (no note, empty column), `TFB_TAP_OFFERS=off` (the 1.8.2 page). The 19th ledger column stays; it is text.

## Known limits
- Portfolio_Decision_v2 (Apps Script) still prints "no broker stop — place …" on that holding's stop line: it has its own check of `_IBKR_Orders`. It follows in the Apps Script change that also takes a sold holding out of NAV.
- The implied completion is a rough reading: it leaves out the time value of the wait, a raised bid and anything that changed in the company since the offer.
- The offer's day is the Riyadh calendar day; an offer that closes at 17:00 New York reads as passed from 00:00 Riyadh of the next day, one hour early. The broker's own cut-off is earlier than both.
- The stop judgment needs the market's own reading from Yahoo; when it is not read, the note and the column are empty for that run.
