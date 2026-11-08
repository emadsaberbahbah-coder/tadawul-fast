# Commit sheet - 27_Decision_Pages_v2.gs v1.2.35 [STOP-RULE] - 2026-11-08

**Decision.** Owner decision B6, 8 Nov 2026. The AskUserQuestion answer was "-8% + 45 sessions (Recommended)": one stop rule for every new ticket.

**Evidence.** A replay of 269 Top-10 entries, 2 Sep - 4 Nov (`tfb_archive.v_board_episodes` + `symbol_bars_daily`).

| Rule | Mean | Median | Win rate | Stopped | Stopped within 3 sessions |
|---|---|---|---|---|---|
| 2xATR fixed | -0.60% | -1.65% | not given | 42% | 10.4% |
| -8% hard + 45-session time stop | -0.60% | +0.06% (best) | 50.6% (best) | 23% | not given |

**Problem.** The cockpit's tickets already carry the rule: Render `TFB_OPP_STOP_VOL_MULT=0` since 8 Nov 11:00, so every ticket stop is 8% under the price. This page's exit engine still trailed 2xATR under the highest close. On the first new lot it would have advised `RAISE STOP` over the -8% working stop, which is exactly the early stop-out pattern the rule removes.

**Change (one gate, default off).**
- Script Property `DP2_STOP_RULE` = `fixed` or `fixed:YYYY-MM-DD`. The default start is 2026-11-08. Off is the v1.2.34 engine byte for byte.
- A lot entered on/after the start date replays a fixed stop `DP2X.FIXED_STOP_PCT` (8) % under its entry.
  - No trailing, no breakeven lock, no profit-protection exit.
  - It gets a time stop `DP2X.TIME_STOP_SESSIONS` (45) sessions after the entry bar.
  - The time stop is an exit episode that latches like a stop touch (`SELL - rule exit <date> (time stop, 45 sessions held, close X) not acted`). It can be acknowledged like any rule exit.
- Older lots keep the trailing rule.
- Top_10_Investments_v2 seats show the initial stop 8% under the price, so the risk per seat is 8% of its allocation.
- The holding texts, the e-mail lines and the engine cell name the fixed rule (`fixed stop -8%, time stop in N sessions`, `HOLD - fixed stop X, time stop in N sessions`).
- Review fix (Codex P2 on #931): the page's per-lot actions (place/raise stop, TP watch), the uncertified HOLD label, the holdings note, the D-stop note and the e-mail footer name the fixed rule; a fixed lot is never offered a trailing order (it read `trail amount 0.00 (null × ATR)`).
- `dp2SelfTest` adds one line while the gate is on.

**Diff.**
- 27 anchored hunks.
- Functions: +6 (`dp2StopRuleParse_`, `dp2StopRule_`, `dp2StopRuleFor_`, `dp2ExitFixed_`, `dp2StopTimeTxt_`, `dp2StopRuleCoreOk_`). Vars: +4 (`DP2_STOP_RULE_PROP`, `DP2X_SR_FROM_`, `DP2X_SR_READ_`, `DP2X_SR_`). Nothing removed.
- ES5. No new non-ASCII character in any hunk. Every prior WHY kept.
- Base: v1.2.34, sha256 `992fb7f77e0d4c4c59f1eb6fb42b7b45be41e1ffaa47957797222662246811c0`.
- New: v1.2.35, sha256 `dc88cfab7cc706b391f6288dcc44d8e9e0cf6479015b22a725f134c6fd694abf`, 3,975 lines.

**Harness.** `tests/test_dp2_v1235_stop_rule.js` (+ `.py`) gives PASS 9/9, x2 identical, digest `015c462f961b0770`.
- P1 GOLDEN NEGATIVE / KILL. Seven off-values and a property fault all give both pages equal to the v1.2.34 build on two boards.
- P2 armed, default start. The 26 Oct FRST lot keeps the trailing engine, and Portfolio_Decision_v2 equals v1.2.34. Funded Top_10_v2 seats risk 8% of allocation.
- P3 armed from 2026-10-01. The FRST lot replays the fixed rule (stop 13.98), while v1.2.34 trails 2xATR.
- P4 pure functions and the e-mail texts.
- P5 one property read per execution.
- P6 self-test line.
- P7 lint.
- P8 four mutants.

**History.** New file `tests/js/dp2_history.js` provides `v1234Source`. The v1.2.31-v1.2.34 harnesses read the rebuilt v1.2.34 file; each still passes (11/11, 10/10, 10/10, 11/11). 68 DP2-related Python tests pass.

**Arm (after merge and paste).**
1. Run `tfbSetAnyProperty` with `DP2_STOP_RULE` = `fixed`.
2. Run `dp2SelfTest`. Expect `stop rule (DP2_STOP_RULE): fixed from 2026-11-08 · core ok`.

**Rollback.** Delete the property.

Existing holdings (bought before 8 Nov) are untouched. Applying the rule to them is a separate owner decision: set a start date earlier than their buy dates.
