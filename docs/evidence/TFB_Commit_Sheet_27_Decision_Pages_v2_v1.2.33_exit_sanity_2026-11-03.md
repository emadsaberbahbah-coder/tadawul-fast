# Commit Sheet - `apps_script/27_Decision_Pages_v2.gs` v1.2.33 [EXIT-SANITY] - 2026-11-03

(ASCII sheet: `--` stands for the em dash and `.` for the middle dot on the page.)

**Scope:** found while checking the owner's deployment on 3 Nov ("all is done please check", 16:31 Riyadh). This is gap A1 part C (dividend-aware holdings) of `claude/TFB_Gap_Analysis_and_Build_Order_2026-11-03.md`.
- **The defect:** `dp2ExitStatus` at 16:15 read `SUPN.US . SELL NOW -- stop 38.32 hit . last 36.28 . dyn stop 38.32 . high 41.92`. Exit e-mails were switched on at 16:16, so the next v2 build could e-mail a SELL NOW for a holding that had not fallen.
- **The change:** a new Script Property `DP2_EXIT_SANITY`, **default OFF**. Absent, blank or any other word, the engine is the v1.2.32 engine byte for byte, apart from one property read per execution.
- **Scope of the change:** Portfolio_Decision_v2 only. Top_10_Investments_v2 is untouched. No number on the board, no gate, ticket, order or broker call changes.

**Files:**
- `apps_script/27_Decision_Pages_v2.gs` 1.2.32 -> 1.2.33: 3,526 -> 3,713 lines; 10 hunks, 190 lines added; 3 base lines replaced (the two version lines and the chart fetch's return, now `var outY = {..}` with the same fields); every added line ASCII.
- `tests/test_dp2_v1233_exit_sanity.js` (new; P0-P9 x2, `PASS 10/10`, mutants 15/15) and `tests/test_dp2_v1233_exit_sanity.py` (pytest wrapper).
- `tests/js/dp2_edits_v1233.json` (new, the 10 hunks).
- Pins moved in two harnesses. Behaviour still runs on the delivered file; each one's own oracle, lint and inventory start from the v1.2.32 file rebuilt from it (SHA-256 `5ac92c36...` pinned):
  - `tests/test_dp2_v1232_preview_label.js`: `PASS 10/10`, digest unchanged;
  - `tests/test_dp2_v1231_engine_tickets.js`: `PASS 11/11`, digest unchanged.
- This sheet.

## S0 - Evidence (3 Nov 2026, read-only)
- **IBKR (read-only):**
  - The 95 shares are contract 849142054, `SUPN -- SUPERNUS INC` (NASDAQ), average cost 35.45. The old Supernus Pharmaceuticals line is `SUPN.OLD` (106853221).
  - IBKR's history of the held contract closed 36.28 on 2 Nov.
  - Its corporate actions list a cash dividend of **8.13 USD, ex 3 Nov, paid 6 Nov**.
  - Pre-market on 3 Nov: 30.00.
- **`_DP2_Exit_State`, SUPN.US row:** entry 35.4541 on 2026-11-02, highest close 41.92, ATR14 1.80, SMA10 41.37, last 36.28, dyn stop 38.32, `STOP_HIT` since 12:49.
- **Cause:** the chart key `SUPN` still served the old line (2 Nov close 41.92), while the live last 36.28 is the held shares' price. The replay compared one instrument's trail with another instrument's price.
- **Feed (BigQuery `v_universe_latest`, 3 Nov):**
  - `SUPN.US` is Supernus Pharmaceuticals with a flat 41.92 bar (open = high = low = close).
  - `INDV.US` is 36.28, the held shares' 2 Nov close.
- **Second exposure:** even a correct history would have read the ex-dividend price (36.28 -> 30.00, the 8.13 special) as a stop hit.

## S1 - WHAT (gate on only)
1. **The lot's own prices: `dp2ExitAnchorMiss_` (pure).**
   - **The check:** the first bar on or after the lot's entry date (within 5 calendar days) must hold the entry price inside its low-high range widened by 8 %.
   - **What fails:** a history of another listing (a ticker change), another unit (GBp vs GBP) or a split the ledger does not carry.
   - **What the lot then shows:**
     - no exit signal and no alert: `HOLD -- exit engine n/a`, `ENGINE_NA` on `_DP2_Exit_State`;
     - the engine cell reads `exit engine n/a -- the price history does not match this lot: the 2026-11-02 bar (low .., high .., close 41.92) is +18.2% from the entry 35.45 (a ticker change, another listing, another unit or a split); check the symbol`;
     - one `CHECK <symbol> (<n> sh): ..` action (priority 1).
   - **Not judged:** no entry date, the entry after the last bar, or a first bar more than 5 days after the entry.
2. **A dividend is not a fall: `dp2ExitDivAdjust_` (pure).**
   - **The adjustment:** every bar before an ex-date is scaled by `1 - amount / the cum-dividend close`. The entry is scaled too when the lot was bought before the ex-date. The replay then runs on the adjusted series, so the stop stays in today's prices.
   - **The ex-day before its bar:** when the ex-date has arrived but its bar has not, the last close is the cum-dividend close and every bar is scaled.
   - **Skipped:** a dividend with no bar before it, a future ex-date, or one above 60 % of the close.
   - **Where the amounts come from:**
     - the chart's own events (`dp2YahooDivs_`; the request adds `events=div`);
     - for an ex-date the chart does not list yet, the `Dividend_Events` row of the same symbol (`dp2DivTab_` / `dp2DivTabFromValues_`, calendar_sync `TFB_CAL_DIVIDENDS`). That row counts only once it has arrived, only in the quote's own currency (a subunit quote never matches the upper-cased tab), and never within 3 days of a chart dividend (`dp2ExitDivsFor_`).
   - **On the page:** the engine cell ends `. dividend-adjusted (8.13 ex 2026-11-03)`.
3. **The gate:** `dp2ExitSanityOn_` reads `DP2_EXIT_SANITY` once per execution (`1` / `on` / `true` / `yes`, any case, trimmed; a fault reads off).
4. **Self-test:** `dp2SelfTest` adds `exit sanity (DP2_EXIT_SANITY): on . core ok (a history of another listing gives no signal; an ex-dividend gap is not a stop)` while the gate is on (`dp2ExitSanityCoreOk_`).

## S2 - Tests (node v22)
- **`node tests/test_dp2_v1233_exit_sanity.js`:** `run 1/2: 10/10`, digest `fe245011115ffc96` x2, **`PASS 10/10`** (about 13 s).
  - **P1 golden negative:** the gate absent / blank / `off` / `0` / `enable` / `no`, or a property fault, on the three SUPN shapes. Portfolio_Decision_v2 (every cell, format, call, e-mail, chart request and return value) equals the v1.2.32 build. Both pages equal it on three fixture boards. One gate read per execution.
  - **P2 the 3 Nov shape:** with the gate on, SUPN.US reads `HOLD -- exit engine n/a` with the exact engine text and one CHECK action. There is no SELL action and no e-mail naming SUPN.US. The FRST.US row is the v1.2.32 row. The base reads `SELL NOW -- stop .. hit` and e-mails it.
  - **P3 the ex-dividend day (chart event):**
    - SUPN.US is not a sell, and the engine cell names the adjustment;
    - the dynamic stop equals the adjusted replay of the delivered helpers;
    - the request carries `events=div`, which the base never asks for;
    - the base hits the stop.
  - **P4 the ex-day before its bar:** the `Dividend_Events` row adjusts. A row in GBP, a future ex-date or another symbol does not. A chart dividend plus a tab row one day apart adjusts once.
  - **P5 pure helpers:**
    - the 8 % band edges, x100 units both ways, a 2:1 split, and the not-judged cases;
    - the dividend factor, the entry rule (bought before vs on the ex-date), compounding, and the skip rules;
    - the tab parse (Date cells, junk, duplicates);
    - the merge rules, the chart events, and the self-test core.
  - **P6 the gate:** the spellings, a fault, and one read per execution.
  - **P7 `dp2SelfTest`:** the v1.2.32 report while off; one line, core ok, while on.
  - **P8 lint and inventory:** 10 functions + 8 vars added, 0 removed; the 3 replaced lines pinned; 10 hunks reversing to `5ac92c36...`; the WHY block; every prior WHY kept.
  - **P9:** 15/15 mutants killed:
    - the gate always on / never on / not memoised;
    - a miss still computed, no band, a wide band;
    - the entry not scaled, the ex-day before its bar ignored, a future ex-date merged;
    - the currency not checked, a double count;
    - events never requested;
    - no CHECK action, the adjustment not named;
    - the self-test line always shown.
- **Golden negative:** the v1.2.32 file run as the delivered file gives `FAIL 0/10`.
- **`pytest`:** `test_dp2_v1233_exit_sanity`, `test_dp2_v1232_preview_label`, `test_dp2_v1231_engine_tickets`: 3 passed; `test_universe_maintenance`: 63 passed.
- **Delivered file:** SHA-256 `dd5ee2041b065b5e12aed7c41ea505fd3b75b3219e29807d060ff4d95a394756` (3,713 lines, 339,376 bytes, UTF-16 length 338,126).

## S3 - Operator steps (the repo file is a mirror; nothing changes until the paste)
1. Merge.
2. Paste the full `apps_script/27_Decision_Pages_v2.gs` v1.2.33 into Apps Script and Save.
3. Script Properties: `DP2_EXIT_SANITY` = `on`.
4. Run `dp2SelfTest`. Expect `exit sanity (DP2_EXIT_SANITY): on . core ok (..)`.
5. Rebuild Portfolio_Decision_v2.
   - While the symbol charts the old line, SUPN.US reads `HOLD -- exit engine n/a` with a CHECK action.
   - Once the symbol charts the held shares, the 8.13 gap reads `dividend-adjusted (8.13 ex 2026-11-03)` instead of a stop hit.
6. Rollback: delete `DP2_EXIT_SANITY`, or Project History -> 1.2.32.

## S4 - Known limits
- **The anchor judges the entry day only.** A ticker that moves to another company after the entry is not caught. A live-last vs chart check was left out on purpose: on a crash day it would silence a real stop.
- **Dividend sources:** the chart's events, or the `Dividend_Events` tab (the next declared dividend only, same symbol, the quote's own currency). Cached bars carry neither events nor a quote currency, so they are not adjusted.
- **Adjusted lots:** the stops shown are in today's prices, i.e. the broker stop to place. The `_DP2_Exit_State` columns Initial stop / R / Highest close are on the adjusted scale.
- **Portfolio_Decision (v1)** still values SUPN.US from the feed's old line (41.92; +18.2 % on the page) until the feed or the ledger prices the held shares (gap A2). This version does not touch it.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
