# Commit Sheet - `apps_script/27_Decision_Pages_v2.gs` v1.2.34 [RENDER-PRIORITY] - 2026-11-05

**Scope:** item 2 of the 5 Nov brief "Fix the Google Sheets recommendation and outcome reports" (Top_10_Investments_v2), stacked on the render-priority work of PR #918 (26_Decision_Chain 1.0.5, 28_Plan_Of_Record 1.0.1). All three use the same switch.
- **The defect:** at 01:48 Riyadh on 5 Nov, `Top_10_Investments_v2` still read `Status: Last run 2026-11-05 00:03:55 | status: running...`. The board shown was that old snapshot. The page did not say how old it was, or that the render it copied had stopped.
- **The cause:**
  - The hourly `buildDecisionPagesV2` always built `Portfolio_Decision_v2` first. It skipped this page whenever that one alone used `RUN_SOFT_LIMIT_MS` (270 s). On 4 Nov that first page took about 329 s, so this page could stay behind for several hourly runs.
  - The snapshot it kept was taken while a cockpit render was running. Apps Script then stopped that render at its 6-minute limit, so the copied `running...` status never resolved.
  - The same hourly build ran alongside the decision chain's renders and the Plan of Record refresh. That contention stopped the Top_10 renders of sync run 41901407829.
- **The change:** Script Property `TFB_RENDER_PRIORITY`, **default OFF**. It is shared with 26_ 1.0.5 and 28_ 1.0.1.
  - **(a) Yield:** while a Top_10 / Portfolio_Decision render runs, `buildDecisionPagesV2` writes no page and returns `{yielded: true}`. A render counts as running when `DT10_RUN_MARKER` or the decision chain claim is younger than 7 minutes. The yield also writes one toast and one log line.
  - **(b) Oldest page first:**
    - The page built longest ago goes first. The timestamps live in Script Property `DP2_LAST_BUILT {pd, t10}` (epoch ms).
    - A page with no timestamp counts as oldest. With no timestamps at all, the v1.2.33 order applies.
    - The other page is built only inside `RUN_SOFT_LIMIT_MS`.
    - A page that throws still gets a timestamp, and the other page is still built. The toast names the failure and the first error is rethrown.
    - **Bars cache** (Codex P2 on this PR). Building Top_10 first must not starve a holding's exit replay:
      - a six-month fetch for a seat that is also a holding keeps the older cached bars of that symbol (`dp2RpKeepOlder_`, capped at the longer series; the fresh bars win from their first date);
      - a cached entry is no longer cut to `KEEP_BARS` (130) at every cache write (`dp2RpKeepLengths_`). v1.2.33 cut every entry read back from the sheet;
      - so, when the second page finds the download budget spent, its fallback still reaches the oldest entry.
  - **(c) Top_10_Investments_v2 shows its own state:**
    - The header adds `page built yyyy-MM-dd HH:mm`.
    - When the copied cockpit status reads `running`, the Status line turns amber and says one of two things:
      - `a cockpit render started HH:MM is in progress; until it finishes, read the seats below as the previous render's` (started under 7 minutes ago);
      - `the cockpit render started HH:MM did not finish (Apps Script stops an execution at 6 minutes; it wrote no receipt); read the seats below as the previous render's until the cockpit shows a finished run`.
  - **(d)** `dp2SelfTest` adds one line while the switch is on: `render priority (TFB_RENDER_PRIORITY): on · core ok`.
  - **Switch off:** one property read per execution and nothing else.
  - **Unchanged:** no gate, verdict, ticket, sizing, alert, order or broker call.

**Files:**
- `apps_script/27_Decision_Pages_v2.gs` 1.2.33 -> 1.2.34: 3,713 -> 3,885 lines, SHA-256 `992fb7f77e0d4c4c59f1eb6fb42b7b45be41e1ffaa47957797222662246811c0`. Every added line is ASCII and ES5.
  - Added: 11 functions (`dp2RpOnCore_`, `dp2RpOn_`, `dp2RpBusyCore_`, `dp2RpBusy_`, `dp2RpOrderCore_`, `dp2RpStamp_`, `dp2RpRunningNoteCore_`, `dp2RpBuildBoth_`, `dp2RpKeepOlder_`, `dp2RpKeepLengths_`, `dp2RpCoreOk_`) and 4 vars.
  - 4 base lines replaced (the Version line, `DP2_VERSION`, the cache-write line of `dp2LoadBars_`, the Status line). 0 removed.
- `tests/js/dp2_edits_v1234.json`: the 7 hunks. Reversing them gives the v1.2.33 file (SHA-256 pinned `dd5ee204`).
- `tests/test_dp2_v1234_render_priority.js` (new, 771 lines) and `tests/test_dp2_v1234_render_priority.py`.
- The 1.2.31, 1.2.32 and 1.2.33 harnesses now undo v1.2.34 first. Their behaviour still runs on the delivered file, and their golden negatives still compare it with their own base.

## Tests (node v22)
- **`node tests/test_dp2_v1234_render_priority.js`:** `PASS 11/11`. The battery runs twice with the identical digest `8cf0d1028697f3df` (19 s).
  - **P1 golden negative:**
    - Setup: the switch absent, blank, `off`, `0`, `enable`, `no`, or a property fault; five boards, two of them a cockpit `running` status; a live marker, a live claim and old timestamps.
    - Result: both pages and each page alone match v1.2.33 cell for cell, including formats, merges, charts, the call log, toasts, logs and return values. No timestamp is written.
  - **P2 yield:**
    - A marker of 2 min, a bare timestamp of 6 min, or a claim of 1 or 6.5 min: no page, `{yielded: true}`, one toast and one log line, no timestamp.
    - A marker of 7 or 8 min, a future or junk marker, a claim of 8 min, no claim, or junk state: both pages are built.
  - **P3 order:**
    - No timestamps: Portfolio_Decision_v2 first. Otherwise the older page first, and a page with no timestamp first.
    - Past the soft limit, only the first page is built and timestamped, and the toast names the skipped page.
    - A synthetic Top_10 fault: Portfolio_Decision_v2 is still built, both pages get timestamps, the toast says `FAILED`, and the error is rethrown.
  - **P3b bars cache:**
    - Switch on: a 130-bar seat fetch over a 300-bar cached holding keeps 300 bars (oldest kept, fresh tail exact), and another cached holding keeps its 300. With the budget spent, the holding's fallback reaches its entry and the exit replay is non-null.
    - Base: both are cut to 130 and the replay is null.
    - Pure cases: the cap, overlap and empty-series rules.
  - **P4 the page:**
    - The header shows the build time, and nothing else changes on a finished board.
    - Statuses started 4 min and 19 min earlier get the in-progress and did-not-finish notes (amber, height 30).
    - Every other cell of both pages matches the switch-off build.
  - **P5 pure helpers** (including the 7-minute boundaries and clock skew). **P6** the switch spellings, one read per execution. **P7** `dp2SelfTest` is the v1.2.33 report while off, plus one line while on.
  - **P8 lint and inventory:** ES5 and ASCII; 11 functions and 4 vars added; 4 replaced lines pinned; reversing the 7 hunks gives v1.2.33; WHY block present; every prior WHY kept.
  - **P9:** 23 of 23 mutants killed.
- **Earlier harnesses on the delivered file:** 1.2.31 `PASS 11/11`, 1.2.32 `PASS 10/10`, 1.2.33 `PASS 10/10`.
- **`pytest`:** the four dp2 wrappers, `tests/test_universe_maintenance.py` and the workflow audit: 78 passed, 10 subtests passed.

## Operator steps (the repo file is a mirror; nothing changes until the paste)
1. Merge.
2. Paste the full `apps_script/27_Decision_Pages_v2.gs` v1.2.34 into Apps Script and Save.
3. Run `tfbSetupRenderPriority` (zz_Property_Tool 2.1, PR #914). It sets the one shared switch and reports the live versions of 26_, 27_ and 28_.
4. Run `dp2SelfTest`. Expect `render priority (TFB_RENDER_PRIORITY): on · core ok`.
5. **Watch:**
   - the next hourly build's toast shows `... built in Ns - <older page> (n rows) · <other page> (n rows)`;
   - Top_10_Investments_v2's header ends with `page built <time>`.
6. **Rollback:** set `TFB_RENDER_PRIORITY` to `off`, which turns it off in all three files. To roll back this file only, use Project History -> 1.2.33. `DP2_LAST_BUILT` is harmless when off.

## Known limits
- With the switch on, the bars cache holds up to `KEEP_BARS_MAX` bars per symbol instead of 130, so the hidden `_DP2_Exit_Engine` tab is larger. A split between two fetches can join cached bars from before the split to adjusted fresh bars. The v1.2.33 exit-sanity anchor check then gives no signal.
- A yield skips that hourly run. With renders of about 4–6 minutes, a collision with the hourly trigger is uncommon, and the next hour builds both pages.
- The page is still a derived view. The authoritative decision surface is `Top_10_Investments`: it owns the receipts, the release and the tickets.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
