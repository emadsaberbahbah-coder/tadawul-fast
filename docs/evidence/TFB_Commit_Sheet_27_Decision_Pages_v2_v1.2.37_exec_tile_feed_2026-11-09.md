# TFB Commit Sheet - 27_Decision_Pages_v2 1.2.37 [EXEC-TILE-FEED]: the Executable tile follows the feed

| Item | Value |
| --- | --- |
| Date | 2026-11-09 (daily improvement routine) |
| Branch | `claude/daily-2026-11-09` |
| Base | origin/main `86edd72` (27_Decision_Pages_v2 1.2.36, SHA-256 `487c0778`; ci.yml 1.1.73) |
| Lane | Apps Script (paste the mirror file; nothing changes until the owner pastes it and sets the property). No Render deploy, no workflow change except the CI harness line |
| Gate | Script Property `DP2_EXEC_TILE_FEED` = 1 / on / true / yes; anything else, absent or a property fault = off = the 1.2.36 pages byte for byte (the footer version aside) |
| Priority | (a) a published decision surface that says "executable" when the decision feed no longer releases the tickets |
| Versions | 27_Decision_Pages_v2 1.2.37 (no open PR or branch claims it). ci.yml 1.1.80 (1.1.74-1.1.79 are claimed by open #922, #923, #909, #926, #938, #915) |

## S1 - Evidence

**The defect.** `buildTop10V2` counts the seats that carry a `Ticket SAR` and paints the Executable tile green whenever that count is above zero (line 3708 on 1.2.36). It never reads the feed view `fv`. The cockpit does not zero a ticket when its release expires (`DT10_UV_MAX_AGE_MIN` = 480 is checked only at render time), and a new sync withdraws the release (`superseded_by_run`) before the next render. So between an expiry, or the start of a sync, and the next cockpit render, the hourly Decision Pages v2 build publishes `Executable 6` in green under an EXPIRED or SYNC RUNNING chip.

**Already recorded as a follow-up.** `docs/evidence/TFB_Commit_Sheet_27_Decision_Pages_v2_v1.2.31_engine_tickets_2026-11-02.md` line 111 says: "the Executable tile still counts `Ticket SAR > 0` regardless of the current feed state (all pre-existing; candidates for a follow-up)". v1.2.31 already greyed the adjacent `Allocated (engine)` tile and prefixed it with the feed state when `!fv.live`. Nothing has closed this since.

**Mon 2 Nov.** Timeline from `docs/evidence/TFB_Commit_Sheet_28_Plan_Of_Record_v1.0.0_2026-11-03.md` S1:

| Riyadh | Event |
| --- | --- |
| 04:19 | release makes run 41363258387 EXECUTABLE until 10:42 |
| 08:49 | cockpit renders 6 EXECUTABLE tickets (68,995 SAR) |
| 10:42 | the release expires |
| 15:35 | the next sync starts |
| 16:40-16:47 | the owner places four limit buys |
| 17:32 | the cockpit re-renders WITHHELD |

That board is replayed offline on 1.2.36 at 11:00 Riyadh (the harness's fixture `tests/fixtures/dp2_board_2026-11-02.json`, frozen clock moved past 10:42):
- **Header chip:** `EXPIRED`.
- **Feed line:** "the release of run 41363258387 expired at 10:42 Riyadh (480-minute clock)".
- **Executable tile:** `6`, colour `#1a7f37` (green), sub "of 10 &middot; EXECUTABLE".
- **Allocated (engine) tile:** already grey, sub `feed EXPIRED - ...`.

The same holds under SYNC RUNNING (superseded) and NOT ACTIONABLE, and with `DP2_PREVIEW_LABEL` or `DP2_COHORT_LEGACY` on.

**How often.** Only when the last render carried engine tickets and the feed has since stopped being live.
- Releases last 6-7 h from issue, and each sync withdraws its predecessor for about an hour before the new render.
- So on a ticket day a few hourly builds land in a non-live state.
- Boards since 3 Nov carried 0 tickets (HELD / WITHHELD / QUALIFIED_UNFUNDED), so the next occurrence is the next ticket day.

**Readers.** The owner. The page is rebuilt hourly by `dp2InstallTrigger` and on the decision chain. Owner quotes about this page drive the 1.2.26, 1.2.31, 1.2.32, 1.2.34 and 1.2.36 WHY entries.

**Dedupe.**
- No open PR (186 checked, codex/* included) touches the tile. The only open PR touching this file is #803 (a stale 1.2.0 rotation).
- No branch pushed in the last 48 h holds a 1.2.37.
- Today's main merges (#939-#945) are on 16_Decision_Top10, the engine, DP2 1.2.36 and zz_Property_Tool.

**Picked over** (each checked against the code on `86edd72` and by an independent refuter):
- The MODEL EVIDENCE basis: arming it would not move the owner's 48.7% figure.
- The ticket-guard source probe: real, but the corporate-events tab is being written for the first time today and the earnings tab is healthy.
- A total-return observe log: real, but the same numbers are one BigQuery query away.
- Plan_Of_Record venue holidays: real and timely (26 Nov), but the visible page needs `POR_MODE=on`, which the repo does not show. That is the strongest next item.

## S2 - Change

**`apps_script/27_Decision_Pages_v2.gs` 1.2.36 -> 1.2.37.** Five anchored hunks, listed in `tests/js/dp2_edits_v1237.json`. ES5, and every added line is ASCII.

1. The header `Version:` line, plus the 1.2.37 WHY block prepended above 1.2.36. Every prior WHY is kept verbatim.
2. `var DP2_VERSION = '1.2.37';`, in lockstep with the header.
3. After the v1.2.32 `dp2PreviewLabelOn_` line:
   - `DP2_EXEC_TILE_FEED_PROP` and the memo vars `DP2_ETF_READ_` / `DP2_ETF_`;
   - `dp2ExecTileFeedOn_()`: read once per execution; a fault reads off through `dp2Prop_`;
   - the pure `dp2ExecTileFeedTile_(tile, executable, feedState, fv)`: returns `{label, value, sub: 'feed <fv.label> - not executable now - ' + sub, color: DP2C.grey}`. It returns the same object when `executable` is not above 0, the feed has no state, there is no view, or `fv.live === true`;
   - `dp2ExecTileFeedCoreOk_()`.
4. In `buildTop10V2`, after the v1.2.31 tile loop and before `dp2Tiles_`: `if (dp2ExecTileFeedOn_()) { ... tiles[tx] = dp2ExecTileFeedTile_(tiles[tx], executable, feed.state, fv); }`. The gate is read on every Top-10 build, live or not, which matches the earlier gates' "one read per execution" pin. `Portfolio_Decision_v2` never reads it.
5. In `dp2SelfTest`, after the v1.2.36 line: one line while armed, "executable tile follows the feed (DP2_EXEC_TILE_FEED): on &middot; core ok|FAIL (...)".

Inventory: functions +3, line-start vars +2 (`DP2_ETF_` shares a line), 0 removed.

**What stays.**
- The label `Executable` and the count. `scripts/tfb_export_audit.py` `check_top10_v2_presentation` finds the tile by the upper-cased `EXECUTABLE` label cell and reads a number under it.
- The header chip, every other tile, seats, sizing, alerts, charts and Portfolio_Decision_v2.
- No number, seat, gate, ticket, order or broker call changes.
- `DP2_COHORT_LEGACY` does not change the rule: the tile is the same tile in both modes, and the feed view is computed in both.

**Tests.**
- `tests/test_dp2_v1237_exec_tile_feed.js` (+ `.py`) is new. It runs in the blocking lean-unit "Native reconciliation and complete calendar inputs" step (ci.yml 1.1.80). It is the first 27_Decision_Pages_v2 harness in a blocking lane; the 1.2.31-1.2.36 ones run only through their `.py` wrappers in the advisory whole suite.
- `tests/js/dp2_history.js` gains `v1236Source` (v1.2.36 SHA-256 `487c0778...` pinned; the 1.2.37 hunks reversed), and `v1235Source` / `v1234Source` chain through it.
- `tests/test_dp2_v1236_evidence_since.js` reads the rebuilt v1.2.36 file, as #942 did for the v1.2.35 harness.

**Rollback:** unset `DP2_EXEC_TILE_FEED`, or paste the 1.2.36 file.

## S3 - Measured

**`tests/test_dp2_v1237_exec_tile_feed.js`: PASS 10/10**, run twice with identical digests, in about 53 s. It loads the REAL `.gs` in a vm (the v1.2.36 harness's GAS shim and 2 Nov board fixture, copied verbatim):

| Case | What it checks |
| --- | --- |
| P0 | the oracle (the 5 hunks reverse to the SHA-pinned v1.2.36 file), version lockstep, both compile, inventory +3 / +2 / 0 removed |
| P1 | GOLDEN NEGATIVE / KILL. The gate absent, blank, off words or near misses (`off 0 false no enable enabled onn y 2 on; 'yes please'`), on the live, expired, sync-running and not-actionable boards -> the v1.2.36 build (cells, formats, charts, calls, logs, mails, returns). Both pages with `DP2_COHORT_LEGACY`. A property fault (armed or not) reads off. Armed, Portfolio_Decision_v2 alone is unchanged |
| P2 | armed with nothing to correct (a live release, a board with no ticket, a research board under a live release, a feed with no `TFB Decision Feed` key), with and without `DP2_COHORT_LEGACY` -> the v1.2.36 pages. The live tile stays green, and a board with no ticket stays red |
| P3 | armed with the release not live (EXPIRED at 11:00, SYNC RUNNING, NOT ACTIONABLE; every on-spelling; `DP2_COHORT_LEGACY`). The page equals an EXPECTED page that the harness builds from the v1.2.36 source with only the Executable tile changed by a `dp2Tiles_` hook (grey, "feed &lt;state&gt; - not executable now - of 10 &middot; EXECUTABLE"). The count stays `6`, while v1.2.36 painted it green. The header chip reads EXPIRED |
| P4 | the export audit's view: the `EXECUTABLE` label cell and a numeric value under it, equal to v1.2.36 on every board |
| P5 | the pure rule: expired, sync running, no label, live, legacy `ACTIONABLE` live, no ticket (0, -1, null, undefined, NaN), no feed state, no view, the input not mutated; the core check |
| P6 | the gate: one read per Top-10 / both build, live or not. Portfolio_Decision_v2 alone never reads it. Memoised, and a fault reads off. 13 off and 8 on spellings |
| P7 | `dp2SelfTest`: off gives the v1.2.36 report; on adds one line, after the symbol-key line or after the v1.2.36 evidence-since line when that is on |
| P8 | lint: LF; ASCII and ES5 in every hunk; the WHY newest-first; every prior WHY line kept |
| P9 | 10 single-line mutants all caught: live ignored, no ticket ignored, no feed state ignored, stays green, sub unchanged, label renamed (it would break the export audit), default on, read only when not live, not memoised, wrong tile |

**Golden negative.** The untouched main copy given as both files (`node tests/test_dp2_v1237_exec_tile_feed.js base.gs base.gs`) gives 3 passed, 7 failed.
- P3 fails on the defect itself: the tile is still `#1a7f37` under EXPIRED.
- The 3 passes (P1, P2, P4) describe the base: off identity, the boards with nothing to correct, and the audit view.

**The older harnesses on the delivered file:** v1.2.31 11/11, v1.2.32 10/10, v1.2.33 10/10, v1.2.34 11/11, v1.2.35 9/9, v1.2.36 9/9.

LANES_RESULT

## S4 - Off identity

- **Gate absent, or any word but 1 / on / true / yes, or a property fault:** `dp2ExecTileFeedOn_()` is false and the tile array is untouched. P1 shows both pages byte for byte equal to v1.2.36 on every board, the footer / toast version aside.
- **Armed, but the release is live, nothing is executable, or the feed has no state:** the same object is returned, so the page is v1.2.36 (P2).
- **Armed and not live:** one tile's colour and sub change (P3), and nothing else.

## S5 - Review

REVIEW_RESULT

## S6 - Owner steps (proposed; nothing is armed here)

1. Merge.
2. Paste `apps_script/27_Decision_Pages_v2.gs` (1.2.37) over the 1.2.36 file in the Apps Script project, then Save.
3. Set the Script Property **`DP2_EXEC_TILE_FEED=on`**. The Script Properties UI hit its 50-property cap on 4 Nov, so use `zz_Property_Tool`: put the key and value in `TFB_PT_SET_KEY` / `TFB_PT_SET_VALUE` and run `tfbSetAnyProperty`.
4. Run `dp2SelfTest`. It should show "PASS - executable tile follows the feed (DP2_EXEC_TILE_FEED): on &middot; core ok".
5. On the next ticket day, after the release's `expires=` time, the Executable tile reads grey with `feed EXPIRED - not executable now - ...`.

**Rollback:** delete the property, or set it to off.

## S7 - Known limits and not done

- **Only the tile changes.** Without `DP2_PREVIEW_LABEL`, which no setup function arms, these stay unqualified: the funnel stage "Plans -> executable (N)", the SEATS title "N engine tickets", and the bold-green seat Alloc cells. The header chip and the feed line already say EXPIRED / SYNC RUNNING.
- **The tile still counts the cockpit's `Ticket SAR > 0`.** The count is a fact about the last render. The tile now says that the feed no longer releases it.
- **Not changed:** the cockpit (16_Decision_Top10) still does not re-render or zero tickets on expiry; the release window (480 minutes) and its timing.
- **Not touched:** `zz_Property_Tool` (the owner edited it today, #945). Arming goes through `tfbSetAnyProperty`.
- No workbook read or write and no provider call; everything is offline.
