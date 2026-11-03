# Commit Sheet - `apps_script/27_Decision_Pages_v2.gs` v1.2.32 [PREVIEW-LABEL] - 2026-11-03

(ASCII sheet: `--` stands for the em dash and `.` for the middle dot on the page.)

**Scope:** gap C1 of `claude/TFB_Gap_Analysis_and_Build_Order_2026-11-03.md`.
- **Owner, 3 Nov 12:14 Riyadh:** "my top 10 investment page 2 showing that there is cash allocated to symbols but the page at top showing red no execution".
- **The change:** a new Script Property `DP2_PREVIEW_LABEL`, **default OFF**. Absent, blank or any other word, the page is the v1.2.31 page byte for byte. `DP2_COHORT_LEGACY` keeps the v1.2.30 page whatever this property says.
- **Display only:** no number, seat, class, tile, chart, gate, ticket, order or broker call changes.

**Files:**
- `apps_script/27_Decision_Pages_v2.gs` 1.2.31 -> 1.2.32: 3,494 -> 3,526 lines; 11 hunks, 38 lines added; 6 base lines replaced (the two version lines and four rendering lines); every added line ASCII.
- `tests/test_dp2_v1232_preview_label.js` (new; P0-P9 x2, `PASS 10/10`) and `tests/test_dp2_v1232_preview_label.py` (pytest wrapper).
- `tests/js/dp2_edits_v1232.json` (new, the 11 hunks).
- `tests/test_dp2_v1231_engine_tickets.js`: pins moved. Its oracle chain and T10 lint now start from the v1.2.31 file rebuilt from the delivered one (SHA-256 `d789cf50...` pinned); `PASS 11/11`.
- This sheet.

## S0 - Evidence (3 Nov 2026, read-only)
- The 09:55 release of run 41566014627 was withdrawn at 10:08:41 (fixed for future runs by PR #904). The cockpit finished WITHHELD with Executable 0.
- Top_10_Investments_v2 then sized the active seats with the research water-fill. Each seat's `Alloc SAR` / `Buy qty` printed in **bold green** under the red `NOT ACTIONABLE` header.
- 1.2.26 greyed only the Allocated tile (`Sizing preview only`). The seat table, its SEATS line and the ALLOCATION block still read like tickets.

## S1 - WHAT
- `dp2PreviewLabelOn_()` reads the gate: `1` / `on` / `true` / `yes`, any case, trimmed. A fault reads off. It is read once per Top-10 build, never under `DP2_COHORT_LEGACY`.
- `dp2PreviewWhy_(executable, fv)` is pure. It returns `''` when the page can be acted on: an engine ticket on the board AND a live EXECUTABLE release (`dp2FeedView_` `live === true`). Otherwise it returns one of:
  - `nothing executable today`, plus ` . feed <label>` when the feed is not live;
  - `feed EXPIRED`;
  - `feed SYNC RUNNING`;
  - `feed NOT ACTIONABLE`.
- With the gate on and a reason:
  - The seat columns read `Preview SAR` / `Preview qty`.
  - A sized seat's two cells are grey italic, normal weight (not bold green), and its Flags start `preview . not a ticket`.
  - The SEATS line starts `PREVIEW ONLY -- not tickets (<why>).`
  - The ALLOCATION block title reads `SIZING PREVIEW (not tickets) -- research mirror of the engine's conviction sizing`, or `ENGINE TICKETS NOT LIVE (<why>) -- the engine's tickets, mirrored` on an engine board whose release is no longer live.

## S2 - Tests (node v22)
- `node tests/test_dp2_v1232_preview_label.js`: `run 1/2: 10/10`, digest `cb99b9ac9f160b56` x2, **`PASS 10/10`** (about 26 s).
  - **P1** golden negative: seven boards x seven gate values, a property fault, `DP2_COHORT_LEGACY` + the gate, and `Refresh both`. All equal the v1.2.31 build (every cell, format, merge, chart, call and return value).
  - **P2** the 3 Nov shape (research board, WITHHELD, feed NOT ACTIONABLE), on the strict, not-strict and held variants:
    - the labels, the SEATS prefix and the ALLOCATION title;
    - the seven sized seats grey italic with the flag first;
    - unsized seats unchanged and every number identical;
    - only the documented cells differ: the two header cells, the SEATS line, the ALLOCATION title, and the two cells plus Flags of each sized seat.
  - **P3** the live EXECUTABLE engine board (today 09:24) on four variants: the v1.2.31 page exactly.
  - **P4** an engine board on a NOT ACTIONABLE, SYNC RUNNING or EXPIRED (11:00) feed: `ENGINE TICKETS NOT LIVE (feed ..)`, with the engine tickets unchanged.
  - **P5** a research board on a live feed: `nothing executable today`.
  - **P6** the pure reason table, the gate spellings and one read per build.
  - **P7** Portfolio_Decision_v2 unchanged.
  - **P8** lint and inventory: 2 functions + 1 var added, 0 removed; the 6 replaced lines pinned; 11 hunks reversing to `d789cf50...`.
  - **P9** 9/9 mutants killed: the gate always on or never on, a live page labelled, legacy not respected, cells still green, the flag on unsized seats, the labels kept, the engine title not relabelled, and the feed suffix dropped.
- Golden negative: the v1.2.31 file run as the delivered file fails P0, P2, P4, P5, P6, P8 and P9 (`FAIL 3/10`). P1, P3 and P7 assert identity with v1.2.31 by design.
- `tests/test_dp2_v1231_engine_tickets.js`: `PASS 11/11`.
- `pytest tests/test_dp2_v1232_preview_label.py tests/test_dp2_v1231_engine_tickets.py tests/test_universe_maintenance.py`: 65 passed.
- Delivered SHA-256 `5ac92c367d86a344ed7beefff33d490d292b82fb68b4ede7fd0c0a9afb18a723` (3,526 lines, 324,917 bytes, UTF-16 length 323,667).

## S3 - Operator steps (the repo file is a mirror; nothing changes until the paste)
1. Merge.
2. Paste the full `apps_script/27_Decision_Pages_v2.gs` v1.2.32 into Apps Script and Save.
3. Script Properties: `DP2_PREVIEW_LABEL` = `on`.
4. On a not-actionable day, rebuild Top_10_Investments_v2 (Decision v2 menu). Expect `Preview SAR` / `Preview qty`, grey seat amounts and `PREVIEW ONLY -- not tickets (...)`. On a live EXECUTABLE board nothing changes.
5. Rollback: delete `DP2_PREVIEW_LABEL`, or Project History -> 1.2.31.

## S4 - Known limits
- Portfolio_Decision_v2 is not touched.
- The 1.2.26 Allocated tile keeps its own grey `Sizing preview only` rule.
- The research water-fill and every number are unchanged by design: the page still shows what the engine would size, now labelled as a preview.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
