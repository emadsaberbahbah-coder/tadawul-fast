# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.23.1 [CONFIRM-WINDOW-FIX] - 2026-11-08

(ASCII sheet: `--` stands for the em dash.)

**Scope:** the two Codex P2 findings on PR #927 (review of commit `f82e7e7`, v1.22.0 [CONFIRM-WINDOW]), both verified real against the code and reproduced as golden negatives on the rebuilt v1.23.0 file before the fix was written. No gate, floor, cap, sizing or exit rule changes; no function or var is added or removed; the knob (`TFB_T10_CONFIRM_WINDOW`) and its default-off semantics are unchanged; a blob the window never touched stays byte-identical to v1.21.1 through every path.

**The findings and the fix:**

| # | Finding (Codex, P2) | Verified mechanism in v1.22.0 / v1.23.0 | Fix in v1.23.1 |
|---|---|---|---|
| 1 | Stale window history survives a disarmed period and re-arming prefers it over the current count. | The unarmed day-advance never touches `st.qh`; `ci` follows the consecutive clock; `dt10StabQhBits_` prefers `qh` over `ci` when both exist. Replay: `qh=[1,1]`, one unarmed WATCH day (`ci` 0), re-arm, one qualified day -> `ci` 3 and a sized `NEW (confirmed 3/3)` that displaces a member. | An entry that still carries `qh` on an UNARMED day-advance loses `qh` (and `qd`): a render day passed without the window running, so the bits no longer describe the last N render days. Re-arming migrates the consecutive count (the v1.22.0 rule). Same replay -> `ci` 1, no seat, the member keeps its seat. |
| 2 | After an outage pause on the day's first render, the same-day recovery branch treats yesterday's bit as today's. | P-168 pause skips the counter pass (no bit appended; `state.date` advances); the same-day branch raised the newest bit, which was yesterday's: a pending 2/3 never counted its third day (bit already 1), and a 0 yesterday was rewritten to 1. | `st.qd` = the day-key of the newest bit, written by every armed day-advance. A qualified same-day render whose `qd` is an earlier day APPENDS today's bit (window trimmed, `ci` = sum, `qd` = today); when `qd` is today -- or absent, for a 1.22.0/1.23.0 blob until its next day-advance -- the v1.22.0 raise-only rule stands. |

**Files:**
- `apps_script/16_Decision_Top10.gs` 1.23.0 -> 1.23.1: 642,581 -> 648780 bytes, 12,060 -> 12,152 lines, 9 anchored hunks, 102 added lines (the WHY block, the parser `qd` guard, the day-advance stamp / unarmed drop, the same-day append branch, the hard-exit `qd` drop, the self-test extension); zero removals; 0 functions / 0 vars added (census 260 / 151 unchanged).
- `tests/test_dt10_v1231_confirm_window_fix.js` (new; W0-W7 x3, `PASS 8/8`, digest `3ddc2a1b9213ee68` x3 identical) and `tests/test_dt10_v1231_confirm_window_fix.py`.
- `tests/js/dt10_edits_v1231.json` (the 9 hunks as `[current, previous]`) and `tests/js/dt10_cash_utc_history.js` (`v1230Source`, the v1.23.0 SHA-256 `ee0edd52...` pinned; `v1220Source` now starts from it -- chain verified `v1230 ee0edd52 / v1220 584d08c8 / v1211 d4afc834 / v1210 e153f4dc`).
- `tests/test_dt10_v1230_reentry_cooldown.js` reads the rebuilt v1.23.0 file (`history.v1230Source`), as `test_dt10_v1220_confirm_window.js` reads the rebuilt v1.22.0 and `test_dt10_v1211_mf_scope.js` the rebuilt v1.21.1: each release's harness keeps proving its own diff.
- Version pins moved to 1.23.1 in fifteen harnesses (`'1.23.1'`, header line 4, `[DT10 v1.23.1]`, the `nv` / `vAll` normalisers now accept `23.[01]`, the S1210 rebuilt-oracle condition accepts `3.1`); no inventory or self-test line count changes (none were needed: the self-test line count is unchanged).
- `apps_script/zz_Property_Tool` 2.3 -> 2.4: `tfbPropsConfirmWindowOff` names the new behaviour (the history is dropped on the first render day with the window off); the paste reminders point at 1.23.1.
- `.github/workflows/ci.yml` 1.1.63 -> 1.1.64: the new harness joins the native Apps Script step.
- This sheet.

## State blob
- `qd` (string `yyyy-mm-dd`) is written only beside `qh`, only by an armed day-advance or an armed same-day append; parsed only beside `qh` and only in that shape (junk, numbers and `qd` without `qh` are dropped).
- Hard exit deletes `qh` and `qd`; soft, capacity and displacement exits keep both (as v1.22.0 kept `qh`).
- A blob written by v1.22.0 / v1.23.0 (`qh`, no `qd`) is read unchanged; its same-day renders keep the raise-only rule until the next day-advance stamps `qd`, so the paste day cannot double-count.
- Rollback: re-paste v1.23.0 -- its parser drops the unknown `qd` key; nothing else in the blob changed shape.

## Harness (`tests/test_dt10_v1231_confirm_window_fix.js`, PASS 8/8 x3)
- W0 base oracle (SHA-256), lockstep, both compile, census unchanged, 9 hunks each unique and reversing to the pinned base.
- W1 finding 1: golden negative on v1.23.0 (`ci` 3, `NEW (confirmed 3/3)`, B displaced) and the fix (`ci` 1, no seat, B kept; the drop happens on the first unarmed day and holds through seven).
- W2 finding 2: golden negatives on v1.23.0 (2/3 stays 2/3; `[1,0]` rewritten to `[1,1]`) and the fix (`[1,1,1]`, `ci` 3, sized; `[1,0,1]`; window-2 trim; a non-qualified same-day render changes nothing).
- W3 KILL: unarmed replays deep-equal the base through the core and `dt10ApplyStability_`; armed replays without a disarmed day or a pause equal the base apart from `qd`; a 1.23.0 blob keeps the raise rule and gains `qd` on its next day-advance.
- W4 parser and exits. W5 embedded `dt10SelfTest` (extended line ok, zero FAIL, line count unchanged, base lacks the extension). W6 lint. W7 three mutants caught (stale history kept; late append never taken; `qd` not stamped).
- Whole cockpit set green on the v1.23.1 file (confirm_basis 8/8, pending_grace 8/8, funding 103, post_earnings 686, shadow 365 / 86, v1190 9/9, v1200 12/12, exdiv 12/12, mf_scope 7/7, v1220 10/10, v1230 9/9; `tests/js` all green).

## Owner steps
Unchanged from the v1.23.0 sheet, with 1.23.1 as the file to paste: merge #927 -> Render deploy of `main` -> paste `16_Decision_Top10.gs` (1.23.1) and `zz_Property_Tool` (2.4) -> `tfbPropsStabilityArmAll` -> `dt10SelfTest` (expect `[DT10 v1.23.1] self-test`, `confirm window core: ok (... v1.23.1: unarmed day drops the history (re-arm 1/3), outage pause + same-day recovery appends (3/3))`, `reentry cooldown core: ok`, zero FAIL).
