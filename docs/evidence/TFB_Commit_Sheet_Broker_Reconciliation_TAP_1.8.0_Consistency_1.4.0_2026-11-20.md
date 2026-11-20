# Commit Sheet: the broker in the plan and in the control (today_action_plan 1.8.0, consistency_check 1.4.0, ibkr_capture 1.2.0), 2026-11-20

**Scope.**
- **Request.** Owner, 20 Nov 2026 22:40 Riyadh (in Arabic): "a full review of the file against the market ... the most important differences between the pages ... and if something is missing we can develop it".
- **Basis.** The review read the workbook of 22:41 against IBKR (read-only) and found three things.
  - A holding sold at the broker was still held on every page.
  - Pages gave a stop instruction for a holding under a cash tender offer.
  - The internal control could not see either.
- **Limits.** Display and control only. No score, pick, gate, level or order changes. The broker is read, never written.

## Why
| Finding (20 Nov, 22:22-22:41 run) | Why it matters | Fix |
|---|---|---|
| The GTC stop on NMM filled at the US open (17:30 Riyadh). The capture listed NMM at 0 shares with the day's P&L. `ibkr_capture.parse` dropped every zero row. | The Today Action Plan said "verify: absent from the broker capture". Final_Decisions read HOLD. Every page still counted the shares, while the cash already held the proceeds. | `ibkr_capture` 1.2.0 returns `closed_today`. The plan (1.8.0) reads such a holding as **SOLD AT IBKR**: one step to record the sale in the ledger, decision SELL with those words, control VERIFY, order status "SOLD at IBKR today (position 0)". |
| DDI is under a cash tender offer (Holding_Read: PINNED BY AN OFFER). Its stop was cancelled 37 minutes after the broker's tender notice. The plan said "place one at 15.22 for all 240 shares". | If the shares were tendered, a sell order on them can open a short position. | The plan reads Holding_Read. A holding pinned by an offer gets a conditional step: "if tendered, place no sell order; if not, place the stop". Every other sell-side step on it ends with the same caution. |
| Consistency_Check read "0 mismatch(es)". C5 compares the ledger with Portfolio_Decision, which reads the ledger. | The broker was never in the comparison. | **C13 broker:** the ledger's shares against the newest IBKR capture. |
| S1_Gate read PASS (20 Nov). The Evidence Board and the Today Action Plan showed FAIL (19 Nov). | Two pages showed an old verdict. | **C14 S-1 gate:** an older copy is STALE; the same day with another verdict is a MISMATCH. |
| `_IBKR_NAV_History` ends 17 Oct and `_IBKR_Account` is as of 17 Oct. | Portfolio_Decision_v2 showed the return since 11 Aug as −0.1% (broker −4.1%) and a month-old trade record. | **C15 broker tabs:** a tab older than 7 days is STALE. |

## Changes
- **`scripts/ibkr_capture.py` 1.1.0 → 1.2.0.**
  - New keys: `closed_today` (zero rows that carry a day's P&L) and `realized_usd`.
  - Every other key is unchanged; `positions` still holds only held shares.
- **`scripts/today_action_plan.py` 1.7.1 → 1.8.0.**
  - SOLD AT IBKR state: step, decision, control status and order status, plus section B shows SOLD.
  - New gate `TFB_TAP_SOLD_AT_BROKER` (on; off = 1.7.1).
  - Tender-offer caution from Holding_Read, which is an optional read.
  - New chip: SOLD.
- **`scripts/consistency_check.py` 1.3.0 → 1.4.0.**
  - New checks C13, C14 and C15 with their control rules.
  - New knobs `TFB_CONS_IBKR_MAX_AGE_H` (30) and `TFB_CONS_IBKR_TAB_DAYS` (7).
  - Reads four more tabs.
  - The capture is read through `ibkr_capture.latest`. When it is off or not configured, there is no C13 row.
  - C13 detail stays off the public log.
- **Workflows.**
  - `today_action_plan.yml` passes `TFB_TAP_SOLD_AT_BROKER`.
  - `consistency_check.yml` 1.4.0 passes the capture settings and the two new knobs.
- **`scripts/verify_deployment.py`.** Pins today_action_plan 1.8.0, consistency_check 1.4.0, ibkr_capture 1.2.0.

## Review fixes (Codex, PR 1039)
- **P1: a sell order still working on a closed position.** The capture can show a zero-share row and a working stop or limit on the same symbol. With no shares, that order can open a short.
  - The SOLD step now names every such order and comes first: "... but a sell order is still working on it (stop 63.20 for 10 shares). Cancel it at IBKR ... then record the sale ...".
  - The decision, control status and order status say the same.
- **P2: the gate's third verdict.** C14 reads NOT_DECIDABLE as well as PASS and FAIL. So a NOT_DECIDABLE gate against an older PASS copy is STALE, and two NOT_DECIDABLE pages MATCH.

## Tests
- **`test_ibkr_capture` C5.**
  - A zero row with a day's P&L reads as `closed_today`.
  - A zero row with no P&L, zero P&L or under half a cent does not.
  - A second row that still holds shares wins.
  - A 1.1.0 capture reads as before.
- **`test_today_action_plan` T19 (two tests).**
  - Sold at IBKR: the step's words, the SOLD chip, section B, the Final_Decisions row (decision, control, order) and no amount in the run log.
  - Absent with no P&L still reads VERIFY; `off` = 1.7.1; the workflow knob.
  - Tender offer: the conditional step and the caution on a raise and a sell.
  - Without Holding_Read, or with a stop working, the words are the 1.7.1 ones.
- **`test_consistency_check` K10 (four tests).**
  - C13: sold today, a different count, a position not in the ledger, a stale capture, an unreadable capture, a capture that is off or unset, and the sukuk skipped.
  - C14: STALE, MISMATCH, MATCH, INFO, and no row.
  - C15: STALE, MATCH, and no row.
  - The run reads the capture and the four tabs and keeps quantities off the log.
- **Results.** 194 tests pass in every suite that touches these modules. ruff (E9, F63, F7, F82) is clean, the workflow audit reads 0 errors and 0 warnings, and verify_deployment shows 0 drift.

## Post-merge
1. Nothing to paste: all three are Python pages run by their workflows.
2. Dispatch **Consistency Check** and **Today Action Plan** once, or wait for the next scheduled run.
3. **Expected on the 20 Nov workbook:**
   - C13 MISMATCH on NMM until the sale is recorded in the ledger;
   - C14 STALE until the Evidence Board is rebuilt;
   - C15 STALE until the broker tabs are refreshed.
4. **Rollback:** `TFB_TAP_SOLD_AT_BROKER=off`; `TFB_CONSISTENCY=off` stops the whole check.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
