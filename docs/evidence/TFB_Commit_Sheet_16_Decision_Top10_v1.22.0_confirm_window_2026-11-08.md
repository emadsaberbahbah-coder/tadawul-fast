# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.22.0 [CONFIRM-WINDOW] - 2026-11-08

(ASCII sheet: `--` stands for the em dash, `<=` / `>=` for the comparison signs.)

**Scope:** item A1 of `claude/TFB_Full_Review_Health_Check_and_Improvement_Plan_2026-11-07.md` (owner, Sun 8 Nov 2026 00:37 Riyadh: "go with the best options and recommended ones"). The entry-confirmation clock of the Top-10 board counts the QUALIFIED day-keys inside a rolling window instead of demanding a consecutive run. One new Script Property, default OFF: with `TFB_T10_CONFIRM_WINDOW` absent, blank, `0`, `1`, junk or a PropertiesService fault, every render is the v1.21.1 render byte for byte (tickets, labels, state blob, audit, note, knobs object, Logger lines), apart from one property read. No gate, floor, cap, sizing rule or exit rule changes; three qualified days are still required before a seat is sized.

**Files:**
- `apps_script/16_Decision_Top10.gs` 1.21.1 -> 1.22.0: 615,673 -> 629,583 bytes, 11,588 -> 11,830 lines, 19 anchored hunks, 214 added lines; eight base lines replaced (the two version lines, the same-day branch head, the pending push, the two FAST-TRACK label lines, the core's doc line, the WHY anchor); zero removals.
- `tests/test_dt10_v1220_confirm_window.js` (new; 10 sections x3, `PASS 10/10`, digest `e0ef115f44add241` x3 identical).
- `tests/js/dt10_edits_v1220.json` (new, the 19 hunks as `[current, previous]`) and `tests/js/dt10_cash_utc_history.js` (`v1211Source`, the v1.21.1 base SHA-256 `d4afc834...` pinned; `v1210Source` now starts from it, so every older oracle still rebuilds its pinned file from the delivered one).
- `apps_script/zz_Property_Tool` 2.1 -> 2.2: `tfbPropsConfirmWindowOn` (sets the property to 5, names the pasted cockpit version, points at `dt10SelfTest`) and `tfbPropsConfirmWindowOff`.
- Version pins moved to 1.22.0 in twelve harnesses, as every prior cockpit release did: `test_dt10_confirm_basis` and `test_dt10_pending_grace` (T6 inventory +4 functions, +2 vars; census 257), `test_dt10_funding_evidence`, `test_dt10_post_earnings`, `test_dt10_shadow_momentum`, `test_dt10_shadow_rank_log`, `test_dt10_v1190_hold_live_rule` and `test_dt10_v1200_ticket_guard` (their rebuilt-v1.21.0 oracle condition now also matches 1.22.0; their self-test line counts take the two new lines), `test_dt10_v1210_exdiv_guard` (E9 drops the two new self-test lines before the v1.20.0 report comparison), `test_dt10_v1211_mf_scope` (reads the rebuilt v1.21.1 file, since the v1.22.0 diff has its own harness), `tests/js/dt10_bulk_format.test.js`, `tests/js/dt10_feed_cohort_binding.test.js`.
- `.github/workflows/ci.yml` 1.1.61 -> 1.1.62: the new harness joins the native Apps Script step.
- This sheet.

**Not touched:** the backend (`core/analysis/top10_selector.py` keeps its own v4.21.0 stability layer; the cockpit computes the live clocks locally in `dt10StabCore_`), `scripts/verify_deployment.py` (it pins no DT10 version), the live Apps Script project, the workbook. The repo file is a mirror: nothing changes until the owner pastes it, and the clock changes nothing until the property is set.

## S0 - Evidence (read-only, 7 Nov 2026)
- BigQuery `tfb_archive.v_board_episodes`, seats since 1 Oct: 319 episodes; 195 (61 %) lasted <= 1 day, 64 two days, 29 three days, 30 four to five days, 1 longer. 4 reached ACTIVE. Exits: 180 `DROPPED` (56 %), 94 `EXIT: hard`, 21 `EXIT: soft`, 12 `BOARD EMPTY`.
- Repeat seats 20 Oct - 7 Nov: BBOX.L 8 episodes (20 days on board), FBK.US 7, WDP.BR 7, ZTO.US 6, CRC.US 6 -- the same names seated, dropped and re-seated, the clock restarting each time.
- The 7 Nov 04:38 board: 1 ACTIVE (BBOX.L day 11), 6 FAST-TRACK reading `day 4, 1/3 confirmed` (MRL.MC, 2020.HK), `day 3, 1/3 confirmed` (WDP.BR, 1030.SR), 3 GRACE; output `QUALIFIED_UNFUNDED`, 0 tickets.
- `v_selection_log` 20 Oct - 7 Nov: executable renders on 3 of 19 days; 13 ticket rows.
- Mechanism, pinned on v1.21.1 `dt10StabCore_` (counter pass, day-advance): a symbol in `rawSet` or qualified inside the buffer gets `ci + 1`; every other day-key sets `ci = 0` (the v1.14.0 reset branch; the v1.15.0 pending grace pauses only a NON-member). A FAST-TRACK member that misses one day-key out of four therefore reads 1/3 on day 4.
- Replay of the archive on the seat-presence proxy (1 Oct - 7 Nov, 38 day-keys): the consecutive clock confirms 24 symbol-days / 20 symbols on 16 days (first render of each UTC day). "3 of the last 5 day-keys" confirms 72 / 39 / 31; with a later render of the same day allowed to count, 137 / 52 / 35. The proxy sees seats only, not names qualified at ranks 11-15, so the live effect of the buffer branch is not measured here.

## S1 - WHY
The owner's purpose for the 3-day rule (22 Oct) was to stop a stock reading INVEST, being bought, and dropping off the list the next day. The rule as built asks for three CONSECUTIVE qualified day-keys on a board whose ranking jitters daily at the cut-off, so the clock restarts more often than it completes. Counting three qualified day-keys inside the last five keeps the owner's requirement (three independent confirmations) and removes the demand that no noisy day fall between them. A later render of the same day-key may raise the day's bit because the first render after a sync is the noisiest read of the day; it may never lower it.

## S2 - WHAT (edit sites)
1. Header `Version:` line 4 and `var DT10_VERSION = '1.22.0'` in lockstep. One dated WHY / WHAT / GATES / ROLLBACK block is prepended; every prior WHY is kept verbatim.
2. New vars after `DT10_T10_PENDING_GRACE_MAX`: `DT10_T10_CONFIRM_WINDOW_PROP` (`TFB_T10_CONFIRM_WINDOW`) and `DT10_T10_CONFIRM_WINDOW_MAX` (30).
3. `dt10StabParseState_`: a `qh` array (0/1 bits, newest last) is kept per symbol when present and non-empty, newest 30 bits; absent in older blobs -> no key, so a blob the window never touched stays byte-identical.
4. New pure helpers before `dt10StabCore_`: `dt10StabConfirmWindowLen_(knobs)` (0 when the knob is absent / 0 / 1 / junk; else the knob raised to `confirm_days` and capped at 30), `dt10StabQhBits_(st, win)` (the entry's bits, or `ci` ones when the entry has no `qh` -- the migration of a run in progress -- trimmed to the window; never mutates), `dt10StabQhSum_(bits)`.
5. `dt10StabCore_`, counter pass, day-advance, not outage-paused: `cwPrev` (the count before the chain) and `cwQual` (in `rawSet`, or INVEST inside the rank buffer under `confirm=qualified`) are read before the v1.21.1 chain, which runs UNCHANGED (co, cp, the score point, pendingHeld). Then, only while the window is armed, today's bit joins the bits, the array is trimmed to the window and `ci` becomes its sum. A miss lowers the count by at most the bit that leaves the window; it never zeroes it.
6. Same day-key re-run: the score-point refresh is unchanged; while armed, a render that finds the symbol qualified raises today's bit to 1 and recomputes `ci`; a bit is never lowered; a blob with no window entry waits for the next day-advance.
7. Exits: a hard exit (`hardOut`) deletes `qh` and zeroes `ci` as before; soft (`co >= exit_days`), capacity and displacement exits keep `qh` and set `ci` to its sum, so a still-qualified name pushed out by a better one does not restart.
8. Labels: the persisted FAST-TRACK label and the `pending` entries show `min(ci, confirm_days)` while armed, so `4/3` never renders; entry, fast-track fill, displacement, grace and every other label are v1.21.1. The read-back token `, cwin=N` and `audit.confirm_window = N` appear only while armed.
9. `dt10StabConfirmWindow_` (new service read, the `dt10StabPendingGrace_` pattern): an integer >= 2 arms (above 30 clamps to 30); `''`, `0`, `1`, negative, decimal, junk, absent or a PropertiesService fault read as 0. `dt10ApplyStability_` sets `knobs.confirm_window` ONLY when armed, so an unarmed run publishes the v1.21.1 knobs object byte for byte.
10. `dt10SelfTest`: `confirm window (TFB_T10_CONFIRM_WINDOW): N (0 = v1.21.1 clocks; 2..30 arms)` and the pure battery `confirm window core: ok (...)` on the confirm-basis board.

Functions added (0 removed): `dt10StabConfirmWindow_`, `dt10StabConfirmWindowLen_`, `dt10StabQhBits_`, `dt10StabQhSum_`. The census goes 253 -> 257 top-level functions and 146 -> 148 vars. ES5 only; every added line is ASCII (the version header line keeps its pre-existing em dash); LF only.

## S3 - Gates / arming
| Script Property (new) | Default | Values | Effect |
|---|---|---|---|
| `TFB_T10_CONFIRM_WINDOW` | absent = consecutive clock (v1.21.1) | integer 2..30 (raised to `T10: Stability Confirm Days`, 3 today) | A seat confirms when `confirm_days` of the last N day-keys were qualified. Recommended first value: **5**. |

Arming (owner, GAS lane, no Render change): paste `16_Decision_Top10.gs` 1.22.0 and `zz_Property_Tool` 2.2 from `main`, Save, run `tfbPropsConfirmWindowOn`, then `dt10SelfTest` (expect `confirm window (TFB_T10_CONFIRM_WINDOW): 5 (...)` and `confirm window core: ok`). Read-back on the next day-advance render: the stability note carries `, cwin=5`; FAST-TRACK seats read `k/3 confirmed` with k = qualified days in the last five; the `_Selection_Log` stability column follows. Rollback: `tfbPropsConfirmWindowOff` (the consecutive clock returns at once; `qh` bits stay parsed and inert) or re-paste v1.21.1 (its parser drops `qh`).

## S4 - Harness (`tests/test_dt10_v1220_confirm_window.js`, PASS 10/10 x3)
- K0 base oracle = the pinned v1.21.1 file (SHA-256 `d4afc834...`); lockstep; both compile.
- K1 golden negative on v1.21.1: CHAL confirmed days 1-2, WATCH at rank 12 on day 3 -> `ci` 1,2,0,1, never seated; the base ignores the knob and writes no `qh`.
- K2 kill: knob absent / 0 / 1 / junk and property absent / `0` / `1` / fault deep-equal the base on ten fixtures x four days, through the pure core and through `dt10ApplyStability_` (note, selected, audit, saved blob, knobs; the new file reads the property once, the base never).
- K3 window 5: `ci` 1,2,2,3; day 4 `NEW (confirmed 3/3)` with the raw ticket's sizing, B displaced; the seeded consecutive 3 migrates to four ones; `cwin=5`; `audit.confirm_window`; `qh` in the blob; a pending raw challenger with five bits reads 3/3.
- K4 semantics: window 2 raised to 3 (2 on day 4, not seated); 99 clamps to 30; a same-day re-run raises the bit and `near 1 CHAL.US(3/3)`; a later re-run never lowers it; no entry waits for the day-advance; a structural block hard-exits B and deletes `qh`; soft, capacity and displacement exits keep `qh` and set `ci` to its sum; an outage-paused day pushes no bit; the `co` path equals the base.
- K5 service read table; the panel snapshot never carries the knob; the knobs object carries it only while armed.
- K6 state sanitizer: bits 1/0/'1'/2/'x' -> 1/0/1/0/0; newest 30 of 40; absent or empty -> no key; the v1.21.1 parser drops `qh` (rollback safe).
- K7 embedded `dt10SelfTest`: the two new lines, every base line present, zero FAIL; armed property line.
- K8 lint and inventory: added lines ES5 / ASCII; 4 functions + 2 vars added, 0 removed; 19 hunks reverse to the pinned base; WHY block newest-first and ASCII; LF only.
- K9 mutants: "bit always 1", "token leaks unarmed", "service read arms 1", "hard exit keeps qh" -- each caught.

Every earlier cockpit harness passes on the delivered file after its pin move: `test_dt10_confirm_basis` 8/8, `test_dt10_pending_grace` 8/8, `test_dt10_funding_evidence` 103 checks, `test_dt10_post_earnings` 686, `test_dt10_shadow_momentum` 365, `test_dt10_shadow_rank_log` 86, `test_dt10_v1190_hold_live_rule` 9/9, `test_dt10_v1200_ticket_guard` 12/12, `test_dt10_v1210_exdiv_guard` 12/12, `test_dt10_v1211_mf_scope` 7/7, `test_dt10_p142_containment`, `test_dt10_p145_grace_sizing`, `test_dt10_calendar_completeness`, `test_dt10_cash_source`; `tests/js/*.test.js` all pass (`apps_script_syntax` compiles 7 sources).

## S5 - What this does NOT do
- It does not seat a name on fewer than `confirm_days` qualified days, does not touch the exit clock, the rank buffer, the earnings / ex-dividend guards, funding evidence, sizing or any floor.
- It does not change the backend selector's own stability layer (not the live path).
- It does not measure the buffer branch's live contribution; the first armed week's `_Selection_Log` is the read-back.
