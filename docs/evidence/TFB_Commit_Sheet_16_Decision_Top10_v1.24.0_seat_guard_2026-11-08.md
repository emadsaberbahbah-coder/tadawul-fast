# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.24.0 [SEAT-GUARD] - 2026-11-08

(ASCII sheet: `--` stands for the em dash.)

**Scope:** items B3 and D1 of the 8 Nov morning review (`claude/TFB_Morning_Review_and_Build_Order_2026-11-08.md`), plus one display fix. Two behaviour changes, each behind its own Script Property, both default off (absent = the v1.23.1 board byte for byte through the core and `dt10ApplyStability_`, including the knobs object, note, audit and saved blob). No gate, floor, cap, score, verdict or sizing rule changes; Section 8 kept (every reason is read from the backend's audited row).

## Evidence (live `_Selection_Log`, 8 Nov)
- 03:53 render: `SPFI.US` entered `NEW (confirmed 3/3)`; the board also carried `WDP.BR` and `MRL.MC` in `GRACE (1/3 missed)`, `ZTO.US` grace (qualified-held), `MBIN.US` / `ACA.PA` `FAST-TRACK (day 1)`.
- 07:33 render: `EFC.US` and `PFS.US` confirmed and the note read `displaced SPFI.US/MBIN.US`. The v1.3.0 displacement compares the challenger with the LAST seat by smoothed score only; grace seats keep the high scores of the days they qualified, so the fresh qualifying seat went.
- 03:53 token `cooldown=10d held 3 ITRN.US(sold 3d ago) SNDA.US(sold 3d ago) ITRN.US(sold 3d ago)`: ITRN was seated AND in the raw list, so the raw loop and the membership pass each pushed an entry.
- 7 Nov review (B3): 94 hard exits in five weeks; under strict (v1.8.6 default) one render whose row reads `structural_block`, `DO_NOT_INVEST` or `BLOCKED` exits the member at once and deletes its window history.

## What changed
| Tag | Knob | Armed behaviour | Unarmed |
|---|---|---|---|
| HARD-EXIT-CONFIRM | `TFB_T10_HARD_EXIT_CONFIRM` = `on` | A member hard-out for a data-side reason (`structural_block`; strict `DO_NOT_INVEST` / `BLOCKED`) keeps its seat on the first day-key it is seen (`st.hx` = that day-key) and exits on a later day-key still hard-out; a clean render clears `hx`. Sold inside the cool-down and `SELL`/`AVOID`/`EXIT` verdicts stay immediate. The kept seat reads `GRACE (c/n missed) - hard-exit pending` with sizing suspended even when a raw ticket exists. Outage-paused members untouched. | v1.23.1 exits |
| SEAT-GUARD | `TFB_T10_SEAT_GUARD_DAYS` = 1..10 | A confirmed challenger on a full board takes the weakest NON-QUALIFYING seat (not raw, not qualified-within, or hard-exit pending; outage-paused excluded), else the weakest UNCONFIRMED fast-track seat -- both without the score test -- else the weakest confirmed seat past its first N day-keys with the v1.3.0 smoothed-score test. A confirmed seat in its first N day-keys and a seat entered on this run are never taken. | v1.23.1 displacement |
| COOLDOWN-NOTE-FIX | -- | A seated sold name that is also in the raw list is named once (`member: true`). | (fix; only the token and `audit.cooldown_held` change, only in that case) |

Note tokens only while armed: `, hxconfirm[ held k SYM ...]`, `, guard=Nd[ took SYM(nq|ft) ...]`; audit keys `hard_exit_held`, `seat_guard {days, took}` only while armed; blob key `hx` only while a member is pending.

## Files
- `apps_script/16_Decision_Top10.gs` 1.23.1 -> 1.24.0: 648,780 -> 668,509 bytes, 12,152 -> 12,480 lines, 19 anchored hunks, 286 added lines, zero removals; +2 functions (`dt10StabHardExitConfirm_`, `dt10StabSeatGuardDays_`), +3 vars (`DT10_T10_HARD_EXIT_CONFIRM_PROP`, `DT10_T10_SEAT_GUARD_PROP`, `DT10_T10_SEAT_GUARD_MAX`); census 262 / 154. `dt10SelfTest` 76 -> 80 lines (two property lines, two core batteries) and the reentry cooldown battery gains the dedupe case.
- `tests/test_dt10_v1240_seat_guard.js` (new; G0-G9 x3, `PASS 10/10`, digest `608d37e347d28922` x3 identical) + `.py` wrapper.
- `tests/js/dt10_edits_v1240.json` (19 hunks) and `tests/js/dt10_cash_utc_history.js` (`v1231Source`, v1.23.1 SHA-256 `8bb2a39c...` pinned; `v1230Source` now starts from it -- chain verified `v1231 8bb2a39c / v1230 ee0edd52 / v1220 584d08c8 / v1200 dcb22bb1`, history oracle unchanged).
- `tests/test_dt10_v1231_confirm_window_fix.js` reads the rebuilt v1.23.1 file, as the v1.23.0 / v1.22.0 / v1.21.1 harnesses read theirs.
- Version pins moved to 1.24.0 in fourteen harnesses; census pins (+2 functions / +3 vars) in `test_dt10_confirm_basis.js`, `test_dt10_pending_grace.js`, `tests/js/decision_input_truth.test.js`; self-test line counts (+4) in `test_dt10_v1190_hold_live_rule.js` and `test_dt10_v1200_ticket_guard.js`; `test_dt10_v1210_exdiv_guard.js` drops the four new lines before its v1.20.0 comparison; the rebuilt-oracle conditions and `nv` / `vAll` normalisers accept `24.0`.
- `.github/workflows/ci.yml` 1.1.64 -> 1.1.65: the new harness joins the native Apps Script step.

## Harness (`tests/test_dt10_v1240_seat_guard.js`)
- G0 oracle, lockstep, census, 19 hunks reverse to the pinned base.
- G1 golden negative on v1.23.1 (fresh confirmed seat displaced, non-qualifying seat kept) and the guard (non-qualifying seat taken; qualifying board with a fresh seat keeps every seat and the challenger waits 3/3; past the guard the score test; clamp 25 -> 10; fast-track first; an outage-paused seat is not taken without the score test).
- G2 golden negative on v1.23.1 (structural block and strict DO_NOT_INVEST exit at once) and the confirmation (kept one day-key with no sizing, same-day re-run kept, next blocked day-key exits, clean day clears hx, raw ticket suspended, DO_NOT_INVEST / BLOCKED held, SELL / AVOID / EXIT and a sold name immediate, outage pause untouched, disarming drops hx).
- G3 golden negative on v1.23.1 (sold seat in the raw list named twice) and the fix; everything else in the run equals the base.
- G4 KILL (knobs absent / 0 / false / junk; properties absent / off / blank / junk / faulted) deep-equal the base through the core and `dt10ApplyStability_`. G5 service reads. G6 parser. G7 embedded self-test. G8 lint. G9 four mutants caught.
- Whole cockpit set green on the v1.24.0 file: every `tests/test_dt10_*.js`, `tests/test_pd_*.js` and `tests/js/*.test.js`.

## Owner steps (arming)
1. Merge, then paste `16_Decision_Top10.gs` (1.24.0) into the Apps Script project (full-file replace) and run `dt10SelfTest`: expect `[DT10 v1.24.0] self-test`, `hard-exit confirm core: ok`, `seat guard core: ok`, zero FAIL.
2. Script Properties: `TFB_T10_SEAT_GUARD_DAYS = 3` and `TFB_T10_HARD_EXIT_CONFIRM = on` (together; `zz_Property_Tool` `tfbSetAnyProperty` works for both).
3. Read the next render's note: `, hxconfirm` and `, guard=3d` appear; a displacement now names its victim class (`took SYM(nq)` / `SYM(ft)`).
- Rollback: delete either property (no re-paste) or re-paste v1.23.1 (blobs with `hx` keep parsing there: its parser drops unknown keys).

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
