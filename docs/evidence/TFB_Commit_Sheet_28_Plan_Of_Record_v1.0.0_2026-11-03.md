# Commit sheet — `apps_script/28_Plan_Of_Record.gs` v1.0.0: one locked plan per market session, tracked to execution (3 Nov 2026)

**Owner request (Tue 3 Nov 2026, 01:08 Riyadh, verbatim):** "good night , Please make full review for the project first check the google drive and make the full review and analysis today morning there was 6 symboles as quailifed for investing , but after that now no one is qualified , I have make decision for investing as per quilifed and allocation of cash now all change the program should show in consistance beahviour and more stable recommdadtions not like what I see changes is done frequant and no cleare way to invest , this is the time to make the project more stable , please go a head and build the required to make it more stable cleare and consistance and alinaged with all pages and requiremets"

## 1. Evidence: what happened to the six tickets on Mon 2 Nov
All read-only: `_Selection_Log`, `_Status`, `My_Portfolio`, `Portfolio_Decision`, Render application logs, the release runs' job summaries, IBKR positions and orders.

| Riyadh | Event | Source |
|---|---|---|
| 04:19 | Release 41371798113 makes run 41363258387 EXECUTABLE (TTL to 10:42) | `_Status` / release summary |
| 08:49 | Cockpit renders **6 EXECUTABLE tickets**: ITRN 62, DDI 244, CRC 41, GLPI 83, EFC 268, INDV 93 (68,995 SAR). All on NYSE/NASDAQ, which opens at 17:30 | `_Selection_Log` |
| 10:42 | Feed TTL expires, 6 h 48 min before the market that can fill the tickets opens | `_Status` |
| 15:35 | Scheduled sync 41445971593 starts (the 15:17 slot, late); finishes 17:07 with a red final job | GitHub |
| 16:40–16:47 | Owner places limit buys: ITRN 67, DDI 240, GLPI 79, INDV 95 (the v2 page's research allocation) with GTC stops at the v2 2×ATR levels (49.94 / 12.90 / 37.33; none on INDV) | IBKR orders |
| 17:30:23 | Builder pool: **26 qualified** (`[RANK-CHALLENGER v1.28.0] pool=26`) | Render log |
| 17:30–18:01 | Fills: GLPI 79 @ 38.85, INDV 95 @ 35.43, ITRN 67 @ 52.35, DDI 240 @ 13.42 | IBKR trades |
| 17:32 | Render WITHHELD (feed not yet released); CRC and OLP already slip to grace | `_Selection_Log` |
| 17:33 | Release 41463134866 (dispatched by the sync): run 41445971593 EXECUTABLE until 23:37, audits [0, 0] | release summary |
| 17:38:26 | Builder pool: **2 qualified** (`pool=2 … top5=0440.HK,0883.HK`) | Render log |
| 17:39 | Render HELD: 0 executable; ITRN / DDI / CRC / INDV grace; GLPI / EFC / OLP / 2020.HK `EXIT: hard`; 0440.HK new fast-track | `_Selection_Log` |
| 23:31 | Scheduled release re-verifies, decision audit exit 2 (`decision_findings_present`), withdraws: `NOT_ACTIONABLE(final_publication) … decision_failed` | release summary |
| 00:55 | Release refused: `release_window_consumed` | release summary |
| 01:21 | Portfolio_Decision: the four new holdings read **BLOCK** "RECONCILIATION_REQUIRED: missing_evidence" | sheet |
| 01:45 (3 Nov) | IBKR lists the INDV position as **SUPN** (contract 849142054, 95 sh @ 35.45) after the Supernus merger; still no stop | IBKR positions / orders |

**Why the pool fell from 26 to 2 in eight minutes:** the W-2 quote-freshness gate (`core/analysis/opportunity_builder.py` v1.4.0+, `TFB_TICKET_FRESHNESS_GATE`, default on) fails every quote older than 15 minutes while its venue trades ("STALE_PRICE intraday"). The workbook's US prices came from the 15:35 sync, before the 17:30 open. After the open every US name was deferred, and only the two Hong Kong names, whose market was closed, still qualified. The gate is correct protection against sizing a ticket from an old price intraday. The defect is that a plan built correctly on the last close disappears from every page the moment its market opens.

**The structural result:** no window existed on 2 Nov in which US tickets were both released and tradable. The morning release expired before the open, and the freshness gate emptied the afternoon release at the open. Tadawul has the same pattern at 10:00.

## 2. What this build does
A new file. No other file changes. It writes nothing that any audit, release check or receipt reads.

- **Reads:**
  - `_Selection_Log`;
  - `Top_10_Investments`: status line B2 and the ticket table, i.e. the board as rendered;
  - `My_Portfolio`: Position Qty, Avg Cost, Buy Date;
  - `_Status` L:M `TFB Decision Feed`;
  - the latest read-only IBKR capture, through `10_My_Portfolio` `pdIbkrLatestCapture_`.
- **Writes:**
  - `_Plan_Of_Record`: the hidden ledger, which is the plan of record itself, carried forward on every refresh;
  - `Plan_Of_Record`: the page;
  - one Script Property, `POR_HOLDINGS_SNAPSHOT`: the holdings this refresh saw, used as the baseline of the next lock.
- **Renders:** each refresh reads the cockpit log and the board.
  - The cockpit logs a render only when its seat signature changes (`dt10AppendSelectionLog_` with `DT10_SELECTION_LOG` unset). The signature leaves out shares, levels and the run.
  - So when the board's status line does not say `SelLog: +N`, the board counts as one more render, at its `Last run` time.
- **Lock rule, per venue session:**
  - A plan is the tickets of an EXECUTABLE render.
  - Its session is the first regular session of the ticket's venue whose close is after the render.
  - A newer EXECUTABLE render before that open replaces the plan (pre-open re-lock).
  - Renders issued while the market is open never change the plan.
  - Plans are carried in the ledger. They stay on record when their render leaves the log tail or the board, until 4 days after the close.
- **Status per ticket:**
  - EXECUTED: the holding grew by at least 90 % of the plan's shares since the lock. The baseline is the holdings snapshot of a refresh taken at or before the lock, at most 60 minutes earlier; with no such snapshot, the rule is a holding bought on or after the lock date.
  - PARTIAL, INVALIDATED (release kill switch), EXPIRED, NOT EXECUTABLE (feed validity), WAITING, OPEN.
- **IBKR protection:**
  - shows the working SELL stop covering the executed shares, against the plan stop, or `NO STOP AT IBKR`;
  - `POR_IBKR_ALIASES` maps a ticket whose ticker changed (for example `INDV.US=SUPN`).
- **Live board:** each ticket's latest cockpit state is shown beside the plan, so a re-ranking never reads as a change of plan.

What the page would have shown at 01:40 on 3 Nov (harness T3, the real board):

| Ticket | Status | IBKR protection | Live board |
|---|---|---|---|
| ITRN.US 62 | EXECUTED (67 sh @ 52.38) | stop 49.94 (plan 48.27) | GRACE (23:42) |
| DDI.US 244 | EXECUTED (240 sh @ 13.44) | stop 12.90 (plan 12.40) | GRACE (23:42) |
| CRC.US 41 | EXPIRED | — | GRACE (23:42) |
| GLPI.US 83 | EXECUTED (79 sh @ 38.88) | stop 37.33 (plan 35.21) | not on the board |
| EFC.US 268 | EXPIRED | — | not on the board |
| INDV.US 93 | EXECUTED (95 sh @ 35.45) | **NO STOP AT IBKR** | GRACE (23:42) |

## 3. Review fixes (Codex on #898, before the first paste)
| Finding | Fix | Proof |
|---|---|---|
| P1: with `POR_VALIDITY=feed`, an expired EXECUTABLE cell still read OPEN. The cell is not rewritten when the release expires | `porFeedOf_` applies the cockpit's own clock: `dt10UvParse_`, 480 min from the first stamp, 15 min skew, naive stamps read the same way. It also applies the release's `expires=` token. Whichever ends first decides. The reason reads "the release of this plan (run N) expired …" | T12: the morning release at 17:45 reads NOT EXECUTABLE ("expired 2026-11-02 12:19"). On 13 values, POR's live/age equals `dt10UvParse_` run from `16_Decision_Top10.gs` itself. Mutants: expiry ignored, token case ignored |
| P1: the lock-time quantity came from the first refresh after the lock, so a fill in between became the baseline (OPEN, and no missing-stop warning) | The baseline now comes only from `POR_HOLDINGS_SNAPSHOT` taken at or before the lock, at most 60 min earlier. It is carried per session and symbol. Otherwise the buy-date rule applies | T7: lock at 18:00 with 100 sh bought before the 18:10 refresh → EXECUTED, `NO STOP AT IBKR`. A snapshot taken after the lock, or 90 min before it, is never the baseline. Mutants: post-lock snapshot, unbounded gap |
| P2: `_Selection_Log` skips renders with an unchanged seat signature, so a later pre-open render with new shares or a new run never reached the lock rule | Board capture (above), plus the ledger carried as the record. A logged board is read only from the log. Forcing `DT10_SELECTION_LOG=always` was rejected: the scorecard, DQ-65, `v_selection_log` and the archive read every logged row | T10: an unlogged 16:50 render (run 41445971593, ITRN 70) re-locks the plan, and at 18:10 it is OPEN under the release of its own run. Without the record it reads NOT EXECUTABLE. T11 and T15 test the carry. Mutants: capture ignored, logged board read twice, ledger ignored, lock-order changes |

## 4. Gates (Script Properties; no re-paste to change)
- `POR_MODE` = `off` (default) | `observe` | `on`. Any other value is off.
  - off: nothing is read or written.
  - observe: the ledger and the snapshot are written.
  - on: the ledger, the snapshot and the page are written.
- `POR_VALIDITY` = `feed` (default) | `session`.
  - feed: an unexecuted ticket is OPEN only while the decision feed releases the plan's own run and that release has not expired.
  - session: the plan stays OPEN through its market session. It is built on the last close, which the freshness gate itself certifies while the market is closed. Entry stays bounded by the entry zone (limit orders), and the release kill switch still invalidates it.
  - **Arming `session` is an owner decision.** It lets a pre-open plan outlive the 8-hour feed TTL until its market closes.
- `POR_IBKR_ALIASES` = empty (default) | `INDV.US=SUPN, …`: the IBKR ticker of a renamed holding, used for the stop and limit match only.

## 5. Operator steps
1. Merge this PR.
2. Apps Script → **+** → Script → name `28_Plan_Of_Record` → paste the file from `main` (SHA-256 below) → Save.
3. Run `porSelfTest` once. It authorizes Drive for the capture reader; read the log lines.
4. Project Settings → Script Properties:
   - `POR_MODE` = `observe`;
   - optionally `POR_IBKR_ALIASES` = `INDV.US=SUPN`.
5. Run `porInstallTrigger` (every 15 minutes).
6. After one session, read `_Plan_Of_Record`; then set `POR_MODE` = `on`.
7. Decide `POR_VALIDITY`:
   - `feed` keeps today's semantics;
   - `session` keeps a pre-open plan executable through its market session.

**Rollback:** `POR_MODE=off` (or `porRemoveTrigger`), or delete the file. The two sheets and `POR_HOLDINGS_SNAPSHOT` can then be deleted by hand.

## 6. Verification
- `node tests/test_por_v100.js` → **PASS 51/51**. The harness runs the real file in a vm with a recording GAS shim. It covers:
  - T0–T6:
    - compile, ES5 / ASCII and the function inventory;
    - gates;
    - sessions: US DST and standard time, Tadawul Sun–Thu, LSE BST/GMT, HKEX;
    - the real 2 Nov board at 01:40 / 18:10 / 16:00 / 09:00 / 11:30 / 17:45;
    - pre-open re-lock and in-session freeze;
    - kill switch.
  - T7: the snapshot baseline.
  - T8: robustness — Date cells, including a workbook in Auckland (Date cells read in `getSpreadsheetTimeZone`), header drift fails closed, a board read fault, determinism.
  - T9: write isolation, including Script Properties; `porSelfTest` writes nothing.
  - T10–T15:
    - board capture;
    - the ledger as record;
    - feed expiry checked against the cockpit's reader;
    - aliases;
    - the contract with `16_Decision_Top10.gs`: `DT10_SELECTED_HEADERS`, B2, `Last run`, `output:`, `SelLog: +N`, `VERIFIED (run N)`, log headers and clock constants, all read from that file;
    - merge order.
  - 18 mutants killed.
  - Two runs with matching digests.
- The same harness fails on the reviewed commit `f3a0c4b7`, for the reviewed reasons (T12 "an EXECUTABLE cell past its clock: OPEN").
- `pytest tests/test_por_v100.py` → 2 passed. `node tests/js/apps_script_syntax.test.js` → 7 sources compiled.
- File: `apps_script/28_Plan_Of_Record.gs`, 1,205 lines, SHA-256 `6a53f7697351af2f02e42907e7876930c791883de3d4b0fb47b6c7ca6731ab7b`, 62 functions (prefix `por`), removed 0.

## 7. Known limits
- Exchange holidays and half days are not modelled: a ticket on a holiday expires at that day's close without a fill.
- Venues outside US, Tadawul, HKEX, LSE, TSE and the main EU markets follow the feed only.
- Fills appear after they reach My_Portfolio, which is fed from `_Portfolio_CostBasis`.
- Date cells are read in the workbook's own time zone (`getSpreadsheetTimeZone`), as `27_Decision_Pages_v2` does, so the cockpit's Riyadh text keeps its meaning whatever the workbook setting.
- IBKR matching is by base symbol (ITRN.US ↔ ITRN) or `POR_IBKR_ALIASES`.
- The board is read every 15 minutes. If the cockpit replaces an unlogged render within that window, POR never sees it: the plan stays on its previous lock, and feed validity shows NOT EXECUTABLE when the release has moved to a newer run.
- A pre-open render first seen after the open still takes the plan's place, because it was the plan at the open. `Replaced Lock` shows it.
- Each run takes a few seconds; 96 runs a day use roughly 5–8 minutes of the Apps Script daily trigger budget.

## 8. Not in this build (next, each its own gated change)
1. Top_10_Investments_v2 and Portfolio_Decision read `_Plan_Of_Record`. That gives one decision header on every page, so an executed ticket reads "executed — manage on Portfolio_Decision", not grace or BLOCK.
2. One stop rule across pages (owner decision): the engine plan stop (ITRN 48.27) or the v2 2×ATR stop (49.94). Today the owner's IBKR stops follow v2.
3. Forecast stability, in the backend (hysteresis or carry-forward). BigQuery `v_universe`, 31 Oct → 2 Nov, at an unchanged price:
   - 483–751 symbols a day moved their 3-month expected ROI by ≥ 2 points;
   - 187–326 flipped to or from the derived 10.5 % value;
   - 25–39 changed investability.
4. Venue-aware release: a pre-open release slot per market, so a plan locks on the last close before each open.
5. The reconciliation evidence supplier (D4), so Portfolio_Decision stops showing BLOCK on every new holding.
6. Identity after a ticker change: INDV.US is now SUPN at IBKR, while the universe still carries the old Supernus Pharmaceuticals SUPN.US row. The mapping should follow the IBKR contract id, not the ticker text.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
