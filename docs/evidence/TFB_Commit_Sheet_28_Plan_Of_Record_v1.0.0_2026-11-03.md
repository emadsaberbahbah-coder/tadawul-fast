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

**Why the pool fell from 26 to 2 in eight minutes:** the W-2 quote-freshness gate (`core/analysis/opportunity_builder.py` v1.4.0+, `TFB_TICKET_FRESHNESS_GATE`, default on) fails every quote older than 15 minutes while its venue trades ("STALE_PRICE intraday"). The workbook's US prices came from the 15:35 sync, before the 17:30 open. After the open every US name was deferred and only the two Hong Kong names, whose market was closed, still qualified. The gate is correct protection against sizing a ticket from an old price intraday. The defect is that a plan built correctly on the last close disappears from every page the moment its market opens.

**The structural result:** no window existed on 2 Nov in which US tickets were both released and tradable. The morning release expired before the open; the afternoon release was emptied by the freshness gate at the open. Tadawul has the same pattern at 10:00.

## 2. What this build does
A new file. No other file changes. It writes nothing that any audit, release check or receipt reads.

- **Reads:** `_Selection_Log`, `My_Portfolio` (Position Qty, Avg Cost, Buy Date), `_Status` L:M `TFB Decision Feed`, and the latest read-only IBKR capture through `10_My_Portfolio` `pdIbkrLatestCapture_`.
- **Writes:** only `_Plan_Of_Record` (hidden ledger, rebuilt each refresh from the last 4 days of renders) and `Plan_Of_Record` (the page).
- **Lock rule, per venue session:**
  - A plan is the tickets of an EXECUTABLE render.
  - Its session is the first regular session of the ticket's venue whose close is after the render.
  - A newer EXECUTABLE render before that open replaces the plan (pre-open re-lock).
  - Once the market is open the plan is frozen until the close.
- **Status per ticket:**
  - EXECUTED: the holding grew by at least 90 % of the plan's shares since the lock, or, with the lock-time quantity unknown, a holding bought on or after the lock date.
  - PARTIAL, INVALIDATED (release kill switch), EXPIRED, NOT EXECUTABLE (feed validity), WAITING, OPEN.
- **IBKR protection:** the working SELL stop covering the executed shares against the plan stop, or `NO STOP AT IBKR`.
- **Live board:** each ticket's latest cockpit state shown beside the plan, so a re-ranking never reads as a change of plan.

What the page would have shown at 01:40 on 3 Nov (harness T3, the real board):

| Ticket | Status | IBKR protection | Live board |
|---|---|---|---|
| ITRN.US 62 | EXECUTED (67 sh @ 52.38) | stop 49.94 (plan 48.27) | GRACE (23:42) |
| DDI.US 244 | EXECUTED (240 sh @ 13.44) | stop 12.90 (plan 12.40) | GRACE (23:42) |
| CRC.US 41 | EXPIRED | — | GRACE (23:42) |
| GLPI.US 83 | EXECUTED (79 sh @ 38.88) | stop 37.33 (plan 35.21) | not on the board |
| EFC.US 268 | EXPIRED | — | not on the board |
| INDV.US 93 | EXECUTED (95 sh @ 35.45) | **NO STOP AT IBKR** | GRACE (23:42) |

## 3. Gates (Script Properties; no re-paste to change)
- `POR_MODE` = `off` (default) | `observe` | `on`. Any other value is off.
  - off: nothing read or written.
  - observe: ledger only.
  - on: ledger and page.
- `POR_VALIDITY` = `feed` (default) | `session`.
  - feed: an unexecuted ticket is OPEN only while the decision feed releases the plan's own run. The release TTL keeps its meaning.
  - session: the plan stays OPEN through its market session. It is built on the last close, which the freshness gate itself certifies while the market is closed. Entry stays bounded by the entry zone (limit orders), and the release kill switch still invalidates it.
  - **Arming `session` is an owner decision**: it lets a pre-open plan outlive the 8-hour feed TTL until its market closes.

## 4. Operator steps
1. Merge this PR.
2. Apps Script → **+** → Script → name `28_Plan_Of_Record` → paste the file from `main` (SHA-256 below) → Save.
3. Run `porSelfTest` once. It authorizes Drive for the capture reader; read the log lines.
4. Project Settings → Script Properties: `POR_MODE` = `observe`.
5. Run `porInstallTrigger` (every 15 minutes).
6. After one session, read `_Plan_Of_Record`; then set `POR_MODE` = `on`.
7. Decide `POR_VALIDITY` (`feed` keeps today's semantics; `session` keeps a pre-open plan executable through its market session).

**Rollback:** `POR_MODE=off` (or `porRemoveTrigger`), or delete the file; the two sheets can then be deleted by hand.

## 5. Verification
- `node tests/test_por_v100.js` → **PASS 29/29**: compile, ES5 / ASCII, gates, sessions (US DST and standard time, Tadawul Sun–Thu, LSE BST/GMT, HKEX), the real 2 Nov board at 01:40 / 18:10 / 16:00 / 09:00 / 11:30 / 17:45, pre-open re-lock and in-session freeze, kill switch, lock-time quantity, Date cells, header drift (fail closed, no write), missing inputs, determinism, write isolation, triggers, eight mutants killed, two runs with matching digests.
- `pytest tests/test_por_v100.py` → 2 passed. `node tests/js/apps_script_syntax.test.js` → 7 sources compiled.
- File: `apps_script/28_Plan_Of_Record.gs`, 877 lines, SHA-256 `27f2aa0c296853656d9debd6529c7a11b4ffa2841af2f894ec788eb61f6f78aa`, 45 functions (prefix `por`), removed 0.

## 6. Known limits
- Exchange holidays and half days are not modelled: a ticket on a holiday expires at that day's close without a fill.
- Venues outside US, Tadawul, HKEX, LSE, TSE and the main EU markets follow the feed only.
- Fills appear after the next IBKR capture reaches My_Portfolio (hourly in the US session).
- IBKR matching is by base symbol (ITRN.US ↔ ITRN).
- Each run takes a few seconds; 96 runs a day use roughly 5–8 minutes of the Apps Script daily trigger budget.

## 7. Not in this build (next, each its own gated change)
1. Top_10_Investments_v2 and Portfolio_Decision read `_Plan_Of_Record`. That gives one decision header on every page, so an executed ticket reads "executed — manage on Portfolio_Decision", not grace or BLOCK.
2. One stop rule across pages (owner decision): the engine plan stop (ITRN 48.27) or the v2 2×ATR stop (49.94). Today the owner's IBKR stops follow v2.
3. Forecast stability: 483–751 symbols a day moved their 3-month expected ROI by ≥ 2 points at an unchanged price, 187–326 flipped to or from the derived 10.5 % value, and 25–39 changed investability with no price change (BigQuery `v_universe`, 31 Oct → 2 Nov). Hysteresis or carry-forward belongs in the backend.
4. Venue-aware release: a pre-open release slot per market, so a plan locks on the last close before each open.
5. The reconciliation evidence supplier (D4) so Portfolio_Decision stops showing BLOCK on every new holding.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
