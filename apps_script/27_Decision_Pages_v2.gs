/**
 * 27_Decision_Pages_v2.gs
 * Emad Bahbah Financial Advisor — Decision Pages v2 renderer
 * Version: 1.2.22 (2026-10-20)
 *   1.2.22: FULL-WORKBOOK REVIEW (20 Oct export). (a) Cash: the cockpit's 'Entered cash (SAR)' KPI is the engine's
 *          kpis.deployable_sar (cash + proceeds less the absolute reserve), so it is no longer read as the cash — the
 *          cash is the panel input 'Cash Available (SAR)' (the per-seat cap's budget base and NAV move with it), and
 *          without a paste or an alert the reserve the last build took is read from the difference (treated as
 *          binding). With DP2_ENGINE_ENV pasted the reserve is no longer taken twice. (b) IBKR: the daily read-only
 *          capture (10_My_Portfolio pdIbkrLatestCapture_) supersedes older _IBKR_Account / _IBKR_Orders tabs — cash,
 *          NAV, stock value, leverage and the working orders — so brackets and the cash cross-check use the morning
 *          read instead of a manual snapshot. (c) The coverage blocker names the failing page from a recount of blank
 *          Names (it said Global_Markets for every coverage failure; on 20 Oct it is Mutual_Funds 97.98 %), and the
 *          path to the first executable board names the owner steps (chain, cohort bind, capture watcher, S-1 c4).
 *          (d) A cockpit run still in progress ('status: running…') no longer hides the engine version: the last
 *          completed run's line on _Status is read instead.
 *   1.2.21: TWO REVIEW FINDINGS. (a) With DP2_ENGINE_ENV present, the cash floor comes from it (the current production
 *          configuration) — a last-sync cash_floor alert may predate an ENV change and is only compared (noted when it
 *          differs). (b) The capital call is the engine's: the first TFB_OPP_CAPITAL_CALL_TOPN funding plans (any state),
 *          their shortfalls summed, the names with a shortfall listed (dp2CapitalCall_); rotation_insufficient is noted
 *          once per run. (c) The Portfolio page's "Unfunded plans / capital call" action is computed once after the lot
 *          loop — it was chained after lot 0's rotation branch and never shown when lot 0 was a rotation candidate; it
 *          names an eligible holding that is too small (rotation insufficient) instead of saying none trails the plans.
 *          (d) The rotation basis follows the same rule as the cash floor: with DP2_ENGINE_ENV present the pasted
 *          TFB_OPP_ROTATION_BASIS decides and the last sync's Rotation Basis KPI is only compared (noted when it differs).
 *   1.2.20: TWO REVIEW FINDINGS (tilt parity, sector-cap denominator) and the class behind them. The production
 *          configuration comes from ONE Script Property, DP2_ENGINE_ENV — the TFB_OPP_* / TFB_T10_* variables of the
 *          Render service as KEY=VALUE lines (absent = unset = engine default; other lines ignored). dp2ProdConfig_
 *          parses it as the engine's readers and mirrors: sizing mode (cap | conviction), tilt, cap (panel wins),
 *          minimum ticket + venue floors, lot size + venue lots, entry-high sizing, sector cap % and basis (budget | pd |
 *          legacy), sector normalization, required ROI (panel), funding-plan switch, rotation basis / hurdles / cost /
 *          exclude / holding days / TP1 proximity, the synthetic-forecast tokens (degraded holdings never rotate) and
 *          the cash floor fallback. Switches are planned only with the paste present, no unmodelled setting off its
 *          default (floor guard, held-target, settled-only, canon market, price-check enforce) and an engine version
 *          new enough for the rules in use. The research view stays the conviction rule; the funding plans use
 *          production's mode. Replaces the DP2_ROTATION_* / DP2_CASH_FLOOR_* / DP2_MIN_TICKET_SAR / DP2_VENUE_FLOORS /
 *          DP2_MAX_WEIGHT_PCT / DP2_SIZING_TILT / DP2_ROTATION_BASIS properties of 1.2.14–1.2.19.
 *   1.2.19: TWO REVIEW FINDINGS. (a) The per-seat cap is production's max_weight_pct (Top_10 panel 'Max Weight %' →
 *          DP2_MAX_WEIGHT_PCT mirroring TFB_OPP_MAX_WEIGHT_PCT → 15) and the tilt TFB_OPP_SIZING_TILT (DP2_SIZING_TILT); switches
 *          are planned only when the minimum ticket AND the cap are mirrored. (b) Before a funding need is recorded, the
 *          seat's minimum ticket is tested against the post-action sector cap (the engine's replay guard) — SECTOR_CAP, no need.
 *   1.2.18: REVIEW FINDING. The minimum ticket is production's (dp2TicketPolicy_): Script Properties DP2_MIN_TICKET_SAR
 *          and DP2_VENUE_FLOORS mirror TFB_OPP_MIN_TICKET_SAR / TFB_OPP_VENUE_FLOORS; the water-fill releases on the operator
 *          floor, a seat below its effective (venue-raised) floor is deferred with that floor as the need (no need when it
 *          is 0), the remnant rule uses the operator floor. Unset → research floor for the sizing, rotation plans withheld.
 *   1.2.17: TWO REVIEW FINDINGS. (a) The rotation test uses PRODUCTION's parameters: hurdles, cost, minimum holding
 *          days, TP1 proximity and the exclude list come from Script Properties DP2_ROTATION_* mirroring the Render ENV
 *          (dp2RotParams_; engine defaults otherwise) and are carried on every plan and printed with their source.
 *          (b) A percent cash-floor alert that names a lower absolute reserve (engine v1.27.4) keeps max(absolute,
 *          pct × NAV) when the rule is re-applied to the current NAV; an older alert falls back to DP2_CASH_FLOOR_SAR.
 *   1.2.16: REVIEW FINDING. The episode of the last sell alert is read back as RAW TEXT (dp2EpisodeStr_) — a live key
 *          '<last bar> live' was cut to its date and re-sent the same alert on every build. dp2SellAlertDue_ /
 *          dp2EpisodeIsLater_ also treat that live break and the stop touch the next bar records as ONE episode.
 *   1.2.15: TWO REVIEW FINDINGS. (a) The cash reserve taken before sizing is the ENGINE's (dp2CashReserve_), never a
 *          hard-coded 10 %: inputs are the engine's cash_floor alert on the Top_10 sheet (ENFORCED → the floor, a percent
 *          re-applied to the current NAV; observe → none) → Script Properties DP2_CASH_FLOOR_SAR/_PCT/_MODE (bind only
 *          with MODE=enforce, as the engine) → none. The PF 10 % floor stays a LIMITS check. The Top_10 reader also
 *          finds the KPI row by its current 'Deployable (SAR)' heading and parses the ALERTS table.
 *          (b) STOP_HIT / SELL_NOW / EXIT_LATCHED form one SELL alert family keyed to the exit episode (state col 24):
 *          a code change about the same episode never resets the cooldown; a new episode alerts at once.
 *   1.2.14: THREE REVIEW FINDINGS. (a) The PRODUCTION rotation basis (Top_10 KPI 'Rotation Basis' → Script Property
 *          DP2_ROTATION_BASIS → '12m') gates the SWITCH actions and advisories; the other basis is a research note.
 *          (b) A rotation's surplus is recorded, never recycled to later needs in the same run (engine semantics).
 *          (c) Lots of one symbol are aggregated into one rotation holding (value summed, oldest entry), as the engine
 *          evaluates the My_Portfolio position.
 *   1.2.13: THREE REVIEW FINDINGS (several lots / several stop levels). (a) dp2BracketFor_ keeps every active stop
 *          level (stops[]) and reports the LOWEST as `stop`; a RAISE names the shares resting below the rule. (b) Rule-exit
 *          acknowledgements are keyed by LOT (symbol@entry); a bare symbol is honoured only for a single-lot symbol.
 *          (c) The position's active stop quantity is allocated to the lots oldest-first, so the uncovered shares are
 *          instructed once in total — each lot reports its own covered quantity (a lot with none → NO WORKING STOP for
 *          its shares).
 *   1.2.12: THREE REVIEW FINDINGS (several lots per symbol). (a) Stop coverage is judged on the POSITION — the sum of
 *          the symbol's active lots — against the active stop quantity. (b) The exit-alert state is keyed per LOT
 *          (symbol@entry#n, state column 23) so two lots of one symbol never overwrite each other's alert state.
 *          (c) On stale bars the rendered page carries the verify-only action, never a priority-0 SELL.
 *   1.2.11: THREE REVIEW FINDINGS. (a) Stop QUANTITY coverage: an active stop covering part of the lot is
 *          STOP_PARTIAL (advisory: place a stop for the uncovered shares), never full protection; dp2BracketFor_
 *          returns stopQty / tpQty. (b) The history must REACH THE ENTRY: dp2LoadBars_(symbols, sinceDate) widens
 *          the fetch range (6mo / 1y / 2y / 5y) and the bars kept to the oldest equity entry; dp2ExitCompute_ returns
 *          null when the first bar is after the entry (engine n/a with the reason); an approximated entry ATR is
 *          flagged. (c) Per-seat cap and the sector denominator use the engine's budget_base = portfolio + cash
 *          BEFORE the reserve (the reserve shrinks the sizing budget, not NAV).
 *   1.2.10: TWO REVIEW FINDINGS. (a) A stale-bar item in the advisory e-mail carries a VERIFY-ONLY action (never
 *          'Sell now') and a 'stale bars — verify' subject label. (b) dp2ExitLine_ is null-safe when a lot has no
 *          price history (ENGINE_NA rotation candidate) — one such item can no longer abort the whole e-mail.
 *   1.2.9: THREE REVIEW FINDINGS. (a) STALE bars never produce an actionable alert: the engine runs on the cockpit
 *          quote, a sell signal is downgraded to a flagged advisory ('STALE BARS … verify the quote'). (b) The
 *          allocation and the top-up honour the engine's post-action PORTFOLIO sector cap (PF: Max Sector %, held
 *          SAR per sector from the ledger) — a breaching seat is SECTOR_CAP (deferred, no need). (c) A script lock
 *          serialises the alert read → send → write transition and the bars-cache write (manual refresh vs hourly
 *          trigger): the loser skips alerts/state for that run, so no duplicate e-mail and no competing writes.
 *   1.2.8: TWO REVIEW FINDINGS. (a) An empty cohort with cash available and a per-seat cap below the minimum ticket
 *          is CAP_INFEASIBLE (deferred, no need) — never capital exhaustion (engine v1.27.3 does the same).
 *          (b) dp2RotationPick_ no longer drops a holding smaller than the minimum ticket outright: eligibility
 *          depends on the shortfall (R1) and a small holding exits in full under the remnant rule (R2), as the engine.
 *   1.2.7: FOUR REVIEW FINDINGS. (a) The replay records every rule-exit EPISODE (hits / moms); an acknowledgement
 *          covers the episodes up to its date and the earliest later episode latches again. (b) _DP2_Exit_State gains
 *          column 22 'Latched exit' and the reader returns the signal text, so Acknowledge rule exit records the real
 *          exit date (never today). (c) dp2Allocate_ runs the engine's lot-rounding top-up pass (conviction order,
 *          within the per-seat cap) before totals and funding plans. (d) The capital-call action sums the SEQUENTIAL
 *          shortfalls of the plan list (cash left counted once), not the independent per-need shorts.
 *   1.2.6: THREE REVIEW FINDINGS — the engine's funding-need semantics exactly. (a) A need is the MINIMUM TICKET and
 *          arises only for a capital-exhausted seat (whole cohort unfunded, or a sub-floor ticket from the cash left);
 *          a seat released while other seats kept a target is a deferral (RELEASED), never a rotation target.
 *          (Backend v1.27.2 in the same PR makes conviction mode treat a whole-cohort release as capital exhaustion
 *          too — before it recorded no need at all with zero cash.) (b) FUNDABLE NOW only when the cash left covers
 *          the whole need; a partial amount is a shortfall for the rotation / capital-call path. (c) The holding's
 *          plan TP1 for the 3% proximity guard comes from the cockpit row (TP1 SAR, else price × (1 + ROI % (TP1))),
 *          independent of broker-order freshness; the broker TP1 is the fallback.
 *   1.2.5: THREE REVIEW FINDINGS. (a) Rule exits LATCH: the first stop touch / profit-protection close since entry
 *          recorded by the replay keeps the lot on 'SELL — rule exit <date> not acted' (EXIT_LATCHED, alerting) until
 *          it is sold or the owner acknowledges it (menu: Acknowledge rule exit → Script Property DP2_EXIT_ACK); a
 *          recovery before the next build never silently reverts to HOLD. (b) The holding-side 3M ROI for the
 *          rotation is the plan-TP1 ROI (cockpit ladder, else working broker TP1), as the engine's plan3m basis —
 *          never the My_Portfolio model forecast, which is display-only. (c) Broker orders are KNOWN only from a
 *          dated snapshot within DP2.IBKR_TABS_STALE_H; a stale or undated snapshot = unknown (levels shown as
 *          UNVERIFIED, no stop advisories, no protection assumed).
 *   1.2.4: FIVE REVIEW FINDINGS. (a) A rotation sells only the SHORTFALL (cash left funds the rest). (b) Exit-signal
 *          precedence: RAISE STOP / broker-unknown / NO WORKING STOP outrank TP WATCH; the TP proximity is carried in
 *          the text (out.tpWatch). (c) Conviction weights use the engine's roi_pct = valuation (TP2) upside from
 *          TP2 SAR / Price SAR (2 × TP1 ROI as fallback), not the TP1 plan ROI. (d) Cohort caps (max seats / per
 *          sector / per market) come from the Top_10 panel (dp2CohortCaps_), DP2A only as fallback. (e) Max drawdown
 *          is the worst decline from a preceding (running) peak, dated.
 *   1.2.3: TWO MORE REVIEW FINDINGS. (a) A rotation must COVER the shortfall (dp2RotationPick_ gets the shortfall as
 *          minValue, as the engine's floor guard does); a smaller eligible holding is reported as rotation-insufficient
 *          and the seat stays a CAPITAL CALL — the page never advises a sale that cannot fund the advertised seat.
 *          (b) Exit-alert actions are specific to the signal code: a stop instruction only for RAISE_STOP /
 *          NO_BROKER_STOP; HOLD / HOLD_BE / TP_WATCH / broker-unknown never say 'raise' to a lower level ('No stop
 *          change — never lower a stop'); a rotation-only mail carries the switch action instead.
 *   1.2.2: TWO REVIEW FINDINGS. (a) ONE CASH BASIS: both pages and the shared allocation memo use dp2CashBasis_ —
 *          Top_10 'Entered cash' → _IBKR_Account cash (STALE-flagged) → _Cash_Snapshot; the memo is keyed on
 *          cash|fx so a different balance recomputes; each page discloses the other reading and flags a gap
 *          > DP2.CASH_DIFF_WARN_PCT. (b) ACTIVE ORDERS ONLY: dp2BracketFor_ takes broker TP / stop levels only
 *          from active-status orders (dp2OrderActive_); REPLACED / CANCELLED / FILLED rows keep lastTp / lastStop
 *          for the wording but never protect a lot; new 'inactive' state; 'partial' says which side is missing.
 *   1.2.1: NO SEEDING ON REFRESH (review finding). buildDecisionPagesV2 / buildPortfolioDecisionV2 no longer call
 *          dp2EnsureIbkrTabs_; the 17 Oct connector sample is written only by the explicit menu action Seed IBKR
 *          Tabs. Missing or empty _IBKR_Orders = broker orders UNKNOWN ('HOLD — broker orders unknown'), never
 *          'NO WORKING STOP'; the pages label the _IBKR_* snapshot's age and STALE (> 30 h) and say so when the
 *          tabs are missing (cash then comes from _Cash_Snapshot). Readers unchanged otherwise.
 *   1.2.0: MENU BAR + RESEARCH ALLOCATION + ROTATION. (a) dp2_addTopMenu(ui): a top-level '📊 Decision v2' menu
 *          (refresh either page or both, open the tabs, dynamic-exit controls, hourly trigger, self-test) — one
 *          guarded line in 01_Menu.gs onOpen (v2.20.3). (b) Top_10_Investments_v2 SEATS: the flat 5%-NAV reference
 *          ticket is replaced by a research allocation that mirrors the engine's conviction-weighted sizing
 *          (opportunity_builder v1.27.0, TFB_OPP_SIZING_MODE=conviction): weight = plan ROI × risk multiplier ×
 *          reliability, water-filled against the deployable cash (cash − 10% NAV floor) with a per-seat cap of 15% of
 *          (portfolio + deployable) and a 2,000 SAR minimum ticket; only FAST-TRACK plans are sized. New ALLOCATION
 *          block (deployable, allocated, expected 3M gain, risk at stops, EV/risk) and FUNDING PLANS & ROTATION block.
 *          (c) Rotation (switch) advisory on both pages — a mirror of the engine's funding plans (v1.18.0+): when cash
 *          does not cover a plan, the held equity with the lowest forward ROI that trails the opportunity by ≥ 8 pp
 *          (engine 12M, production rule; ≥ 5 pp on the proposed plan-3M basis) after 1.1% round-trip cost is proposed
 *          as TRIM/EXIT → fund the plan; held ≥ 7 d, not within 3% of TP1, sukuk excluded. Shown in ACTIONS TODAY,
 *          LIMITS, the holdings' Exit engine cell and the advisory e-mail ('SWITCH candidate'). Research only.
 *   1.1.0: DYNAMIC EXITS. (a) Top_10_Investments_v2 SEATS table gains the 3-month close sparkline, a Buy qty /
 *          Ticket SAR pair (engine ticket when published, else the 5% NAV reference), the initial dynamic stop
 *          (entry − 2 × ATR14) and the SAR at risk if that stop is hit. (b) Portfolio_Decision_v2 holdings gain a
 *          trailing dynamic stop (ratchet: never loosens; breakeven lock after +1R; 2 × ATR under the highest close,
 *          1.5 × ATR after +2R), a profit-protection exit (in profit, close < SMA10, ≥ 1 ATR given back from the
 *          high) and an Exit signal chip: SELL NOW / STOP NEAR / TP WATCH / RAISE STOP / HOLD. (c) E-mail alerts
 *          (MailApp, to the script owner; 20 h cooldown per symbol+signal) when a SELL NOW appears, and at most
 *          once a day for advisories (stop near / raise the broker stop / no working stop). (d) Price history from
 *          the Yahoo chart API via UrlFetchApp
 *          (Stooq fallback for US names, optional DP2_HISTORY_URL backend route), cached in _DP2_Exit_Engine;
 *          signal state in _DP2_Exit_State. (e) GOOGLEFINANCE sparkline keys mapped per venue (LON:, TADAWUL:…).
 *          Nothing here places, changes or cancels a broker order.
 *   1.0.2: Top 10 cash read made robust (KPI row in any column → panel 'Cash Available' → _IBKR_Account → _Cash_Snapshot),
 *          so NAV / deployable / 5% reference ticket are right; tile and key/value text kept literal (no percent auto-parse);
 *          sukuk tile wording; '—' for seats without an FX rate.
 *   1.0.1: dp2_addMenuItems is safe to run without an argument (shows a hint).
 *
 * PURPOSE
 * Renders the two redesigned decision pages — Portfolio_Decision_v2 and
 * Top_10_Investments_v2 — INSIDE the workbook, from data the workbook already
 * holds, plus four small IBKR tabs (_IBKR_Account, _IBKR_Orders, _IBKR_Trades,
 * _IBKR_NAV_History). The production cockpits (Portfolio_Decision,
 * Top_10_Investments) are READ, never written. Nothing here touches gates,
 * tickets, triggers or _Lists_Config. Default-off: nothing runs until an
 * operator (or the optional hourly trigger) calls buildDecisionPagesV2().
 *
 * SOURCES READ
 *   Top_10_Investments   status (B2), KPI row, seat rows, ALL QUALIFIED rows,
 *                        DATA GAPS rows, control-panel values
 *   Portfolio_Decision   status (B2) — evidence / receipt facts
 *   _Portfolio_CostBasis active lots (qty, cost, price, MV, days held), totals
 *   My_Portfolio         RSI, 52W position, expected ROI, scores, asset class
 *   _Cash_Snapshot       latest broker cash (fallback when _IBKR_Account absent)
 *   _Status              Global Key / Value pairs (Decision Feed, Calibration…)
 *   Global_Markets       'Investability Status' distribution
 *   Performance_Log      matured records → hit rate by label
 *   S1_Gate              verdict + criteria
 *   Shadow_Board         switch-scan candidates
 *   _IBKR_*              account, working orders, closed trades, TWR series
 *   Yahoo chart API      daily OHLC + last price per held / seated symbol (UrlFetchApp);
 *                        Stooq CSV fallback for US names; DP2_HISTORY_URL (Script
 *                        Property, optional) is tried first when set
 *
 * OUTPUT
 *   Portfolio_Decision_v2, Top_10_Investments_v2 (rebuilt wholesale on each
 *   run), _DP2_Charts_* (hidden chart data), _DP2_Exit_Engine (hidden OHLC
 *   cache), _DP2_Exit_State (hidden signal / alert state). Charts are
 *   EmbeddedCharts; cell visuals are SPARKLINE formulas. E-mail alerts via
 *   MailApp (first run after pasting asks for the send-mail permission).
 *
 * ENTRY POINTS
 *   buildDecisionPagesV2()   both pages        buildPortfolioDecisionV2()
 *   buildTop10V2()           dp2SeedIbkrTabs() (explicit menu action only: seeds the four
 *   IBKR tabs with the 17 Oct 2026 connector read when they are empty; a refresh never seeds)
 *   dp2InstallTrigger() / dp2RemoveTrigger()  hourly rebuild (+ exit alerts)
 *   dp2ExitStatus() / dp2ExitTestAlert() / dp2ExitAlertsToggle()
 *   dp2SelfTest()            dp2_addMenuItems(menu) (Today submenu, 01_Menu v2.20.2)
 *   dp2_addTopMenu(ui)       '📊 Decision v2' menu-bar entry (01_Menu v2.20.3 hook)
 *
 * SCRIPT PROPERTIES (optional)
 *   DP2_EXIT_ALERTS   on|off   (default on — alerts only go out from a build / the hourly trigger)
 *   DP2_EXIT_ALERT_TO e-mail   (default: the script owner)
 *   DP2_HISTORY_URL   template with {symbol}, e.g. https://…/v1/history?symbol={symbol}&days=130
 *   DP2_HISTORY_KEY   value sent as X-API-Key with DP2_HISTORY_URL
 *
 * CONVENTIONS: ES5 only; no onOpen here (01_Menu owns the menu).
 */
'use strict';
var DP2_VERSION = '1.2.22';
var DP2 = {
  SHEET_PD: 'Portfolio_Decision_v2',
  SHEET_T10: 'Top_10_Investments_v2',
  SHEET_CHARTS: '_DP2_Charts',
  SHEET_CHARTS_PD: '_DP2_Charts_PD',
  SHEET_CHARTS_T10: '_DP2_Charts_T10',
  SRC_T10: 'Top_10_Investments',
  SRC_PD: 'Portfolio_Decision',
  SRC_MP: 'My_Portfolio',
  SRC_LEDGER: '_Portfolio_CostBasis',
  SRC_CASH: '_Cash_Snapshot',
  SRC_STATUS: '_Status',
  SRC_PERF: 'Performance_Log',
  SRC_GM: 'Global_Markets',
  SRC_S1: 'S1_Gate',
  SRC_SHADOW: 'Shadow_Board',
  IB_ACCOUNT: '_IBKR_Account',
  IB_ORDERS: '_IBKR_Orders',
  IB_TRADES: '_IBKR_Trades',
  IB_NAV: '_IBKR_NAV_History',
  COLS: 16,
  COL_WIDTH: 86,
  ROW_HEIGHT: 21,
  FX_DEFAULT: 3.75,
  CASH_FLOOR_PCT: 0.10,
  REF_SIZE_PCT: 0.05,
  EVIDENCE_LIMIT_MIN: 14,
  IBKR_TABS_STALE_H: 30,   // _IBKR_* snapshot older than this is labelled STALE on the page (it is a manual read, not the Flex capture)
  CASH_DIFF_WARN_PCT: 5,   // v1.2.2: the other cash balance (IBKR snapshot / _Cash_Snapshot) differing from the basis by more than this is flagged
  TZ: 'Asia/Riyadh'
};
var DP2C = {
  navy: '#14202b', navyText: '#0b2a4a', surface: '#ffffff', page: '#f3f5f8', border: '#d5dde7',
  headerBg: '#e6edf5', zebra: '#f7f9fc', ink: '#14202b', ink2: '#4a5a6b', muted: '#898781',
  red: '#b42318', redBg: '#fde8e6', amber: '#8a5a00', amberBg: '#fff4e5', green: '#1a7f37', greenBg: '#e8f5ec',
  blue: '#2a78d6', orange: '#eb6834', seriesRed: '#e34948', grey: '#898781', gold: '#ffd166',
  ramp: ['#86b6ef', '#6da7ec', '#5598e7', '#3987e5', '#2a78d6', '#1c5cab']
};
/* Dynamic-exit rule parameters (long positions). Distances are in ATR14 units or in R (= initial risk per share). */
var DP2X = {
  SHEET_BARS: '_DP2_Exit_Engine',
  SHEET_STATE: '_DP2_Exit_State',
  ATR_N: 14,
  ATR_MULT_STOP: 2.0,        // initial stop = entry − 2 × ATR14; trail distance under the highest close
  TRAIL_MULT_TIGHT: 1.5,     // trail distance once the highest close is ≥ TIGHTEN_R × R above entry
  TIGHTEN_R: 2.0,
  PROFIT_LOCK_R: 1.0,        // once the highest close is ≥ 1R above entry the stop is at least breakeven + BREAKEVEN_ATR × ATR
  BREAKEVEN_ATR: 0.1,
  MOMENTUM_SMA: 10,          // profit protection: in profit, close < SMA10, ≥ MOM_GIVEBACK_ATR × ATR given back, move at the high ≥ MOM_MIN_R × R
  MOM_GIVEBACK_ATR: 1.0,
  MOM_MIN_R: 1.0,
  NEAR_STOP_PCT: 1.0,        // STOP NEAR when the last price is within 1% above the dynamic stop
  RAISE_MIN_PCT: 1.0,        // RAISE STOP when the dynamic stop is ≥ 1% above the working broker stop
  TP_WATCH_PCT: 1.5,
  RISK_WARN_PCT_NAV: 1.0,    // Top 10: SAR at risk on the initial stop above 1% NAV is flagged
  COOLDOWN_H: 20,            // e-mail cooldown per symbol + signal
  ALERT_RAISE: true,         // daily e-mail when the broker stop should be raised / is missing
  RANGE: '6mo',            // default fetch range; v1.2.11: dp2LoadBars_(symbols, sinceDate) widens it (1y / 2y / 5y) so the history reaches the oldest entry
  KEEP_BARS: 130,          // bars kept per symbol at the default range; widened ranges keep up to KEEP_BARS_MAX
  KEEP_BARS_MAX: 1300,
  ATR_WARMUP_BARS: 14,     // bars wanted BEFORE the entry so the initial ATR is the entry-date ATR
  FETCH_SLEEP_MS: 200,
  PROP_ALERTS: 'DP2_EXIT_ALERTS',
  PROP_ALERT_TO: 'DP2_EXIT_ALERT_TO',
  PROP_HISTORY_URL: 'DP2_HISTORY_URL',
  PROP_HISTORY_KEY: 'DP2_HISTORY_KEY',
  PROP_EXIT_ACK: 'DP2_EXIT_ACK',     // v1.2.5: JSON { symbol: 'yyyy-MM-dd' } — rule exits acknowledged (hold by decision)
  PROP_ENGINE_ENV: 'DP2_ENGINE_ENV'      // v1.2.20: the production configuration — the TFB_OPP_* / TFB_T10_* variables of the Render service as KEY=VALUE lines
};
/* Research allocation + rotation (v1.2.0) — a faithful mirror of the backend's conviction-weighted seat sizing
 * (opportunity_builder v1.27.0, TFB_OPP_SIZING_MODE=conviction) and its rotation funding plans (v1.18.0+):
 * weight = plan ROI % × risk multiplier × reliability factor (0.5..1), tilt 1; water-filled against the deployable
 * cash with a per-seat cap of MAX_WEIGHT_PCT × (portfolio value + deployable) and a minimum ticket; when cash runs
 * out, ONE rotation per run: the held equity with the lowest forward ROI that trails the opportunity by ≥ edge after
 * round-trip cost (held ≥ 7 d, not within 3% of its TP1, sukuk excluded). Research view only — the engine's tickets
 * are the executable ones. */
var DP2A = {
  MAX_WEIGHT_PCT: 15,            // TFB_OPP_MAX_WEIGHT_PCT engine DEFAULT — v1.2.20: the value in use comes from dp2ProdConfig_ (panel → DP2_ENGINE_ENV → 15)
  MIN_TICKET_SAR: 2000,          // research floor, used only while DP2_ENGINE_ENV is unset (v1.2.20: production = TFB_OPP_MIN_TICKET_SAR via dp2ProdConfig_)
  TILT: 1.0,                     // TFB_OPP_SIZING_TILT
  RISK_MULT: { Low: 1.0, Medium: 0.7, High: 0.4 }, RISK_DEFAULT: 0.7, REL_MIN: 0.5,
  REQUIRED_ROI_PCT: 12,          // T10: Required ROI % — fallback weight when a plan ROI is missing
  MAX_SELECTED: 10, MAX_PER_SECTOR: 2, MAX_PER_MARKET: 10,   // fallbacks only — v1.2.4 reads T10: Max Selected / Max Per Sector / Max Per Market from the Top_10 panel
  ROT_EDGE_PP_12M: 8,            // TFB_OPP_ROTATION_EDGE_PP (engine 12M basis — production) · the ROT_* values are the engine DEFAULTS; v1.2.20: production values from DP2_ENGINE_ENV
  ROT_EDGE_PP_3M: 5,             // TFB_OPP_ROTATION_EDGE_PP_3M (plan 3M basis — proposed, PR)
  ROT_COST_PCT: 1.1,             // TFB_OPP_ROTATION_COST_PCT — round-trip cost
  ROT_MIN_HELD_DAYS: 7,          // TFB_OPP_ROTATION_MIN_HELD_DAYS
  ROT_TP1_PROX_PCT: 3,           // TFB_OPP_ROTATION_TP1_PROXIMITY_PCT
  ROT_EXCLUDE: ['5023.SR']       // TFB_OPP_ROTATION_EXCLUDE (sukuk / outside execution scope)
};
/* GOOGLEFINANCE exchange prefixes by TFB venue suffix (sparklines only). Unknown venues fall back to the bare ticker inside IFERROR. */
var DP2_GF_EXCH_ = { L: 'LON', SR: 'TADAWUL', HK: 'HKG', T: 'TYO', PA: 'EPA', AS: 'AMS', BR: 'EBR', DE: 'ETR', MI: 'BIT', MC: 'BME', LS: 'ELI', VI: 'VIE', SW: 'SWX', TO: 'TSE', AX: 'ASX', SI: 'SGX', MX: 'BMV' };
var DP2X_MEMO_ = {};

/* ========================================================= seeds (17 Oct 2026) */
var DP2_SEED_ACCOUNT_ = [["as_of", "2026-10-17 18:15 Riyadh"], ["source", "IBKR read-only connector (manual read) — replace by Flex capture parsing (B2)"], ["nav_usd", 22187.54], ["cash_usd", 14338.47], ["stock_mv_usd", 7847.29], ["unrealized_usd", 423.5], ["leverage", 0.35], ["fx_usd_sar", 3.75], ["twr_inception_pct", 0.99], ["twr_mtd_pct", 1.33]];
var DP2_SEED_ORDERS_ = [["CWBC", "SELL", "LIMIT", 100, 29.6, "NEW", "2026-09-28 13:07Z"], ["CWBC", "SELL", "STOP", 100, 25.09, "NEW", "2026-09-28 13:07Z"], ["DDI", "SELL", "LIMIT", 229, 15.03, "NEW", "2026-09-28 13:05Z"], ["DDI", "SELL", "STOP", 229, 12.49, "NEW", "2026-09-28 13:05Z"], ["KRP", "SELL", "LIMIT", 130, 16.4, "REPLACED", "2026-09-28 12:37Z"], ["KRP", "SELL", "STOP", 130, 13.95, "REPLACED", "2026-09-28 12:37Z"]];
var DP2_SEED_TRADES_ = [["SHG", "Shinhan Financial", "USD", "limit TP", "24 Aug→3 Sep", 693.32], ["T82U", "Suntec REIT (SGD)", "SGD", "limit TP", "12→21 Aug", 82.9], ["PFLT", "PennantPark Floating", "USD", "limit TP", "6→10 Aug", 59.6], ["CTO", "CTO Realty", "USD", "market exit", "5→6 Aug", -6.28], ["PFS", "Provident Financial", "USD", "market exit", "28 Jul→8 Sep", -6.65], ["VEL", "Velocity Financial", "USD", "stop", "9→16 Sep", -29.3], ["ARCO", "Arcos Dorados", "USD", "market exit", "6→7 Aug", -76.37], ["AER", "AerCap", "USD", "stop", "28 Sep→8 Oct", -111.54], ["HCI", "HCI Group", "USD", "market exit", "3→8 Sep", -156.38], ["EPRT", "Essential Properties", "USD", "stop", "27 Aug→11 Sep", -168.08], ["UVV", "Universal Corp", "USD", "market exit", "6→7 Aug", -188.78], ["CARE", "Carter Bankshares", "USD", "stop", "10→29 Sep", -192.58], ["OTIS", "Otis Worldwide", "USD", "limit exit", "12 Aug→10 Sep", -208.63], ["YUM", "Yum! Brands", "USD", "stop", "12 Aug→1 Oct", -210.98], ["SBAC", "SBA Communications", "USD", "stop", "12 Aug→24 Sep", -289.33]];
var DP2_SEED_NAV_ = [["2026-08-11", 0.01088036], ["2026-08-12", 0.02029201], ["2026-08-13", 0.02308486], ["2026-08-14", 0.01874209], ["2026-08-17", 0.00663677], ["2026-08-18", 0.00334145], ["2026-08-19", 0.01117834], ["2026-08-20", 0.02145295], ["2026-08-21", 0.02582668], ["2026-08-24", 0.03766559], ["2026-08-25", 0.04315257], ["2026-08-26", 0.04409669], ["2026-08-27", 0.03875485], ["2026-08-28", 0.05143286], ["2026-08-31", 0.04598747], ["2026-09-01", 0.04530575], ["2026-09-02", 0.04699249], ["2026-09-03", 0.06199355], ["2026-09-04", 0.05327767], ["2026-09-07", 0.05327767], ["2026-09-08", 0.04764731], ["2026-09-09", 0.03799457], ["2026-09-10", 0.0321522], ["2026-09-11", 0.02788503], ["2026-09-14", 0.02978219], ["2026-09-15", 0.0251003], ["2026-09-16", 0.01477026], ["2026-09-17", 0.01339792], ["2026-09-18", 0.01505565], ["2026-09-21", 0.0170493], ["2026-09-22", 0.01422178], ["2026-09-23", 0.00605919], ["2026-09-24", 0.00422813], ["2026-09-25", 0.00859595], ["2026-09-28", 0.00163031], ["2026-09-29", -0.00218851], ["2026-09-30", -0.0033451], ["2026-10-01", -0.00291763], ["2026-10-02", 0.00093722], ["2026-10-05", 0.00100868], ["2026-10-06", 0.00441609], ["2026-10-07", 0.00176017], ["2026-10-08", 0.00245983], ["2026-10-09", 0.00212847], ["2026-10-12", 0.00551085], ["2026-10-13", 0.00582901], ["2026-10-14", 0.0045955], ["2026-10-15", 0.00732561], ["2026-10-16", 0.01002796], ["2026-10-17", 0.00991269]];
/* YUM daily bars, IBKR price history 20 Apr – 16 Oct 2026: self-test fixture for the dynamic-exit rule (24 sh @ 144.61 on 12 Aug; fixed stop 136.01). */
var DP2_FIXTURE_YUM_ = {"t":["2026-04-20","2026-04-21","2026-04-22","2026-04-23","2026-04-24","2026-04-27","2026-04-28","2026-04-29","2026-04-30","2026-05-01","2026-05-04","2026-05-05","2026-05-06","2026-05-07","2026-05-08","2026-05-11","2026-05-12","2026-05-13","2026-05-14","2026-05-15","2026-05-18","2026-05-19","2026-05-20","2026-05-21","2026-05-22","2026-05-26","2026-05-27","2026-05-28","2026-05-29","2026-06-01","2026-06-02","2026-06-03","2026-06-04","2026-06-05","2026-06-08","2026-06-09","2026-06-10","2026-06-11","2026-06-12","2026-06-15","2026-06-16","2026-06-17","2026-06-18","2026-06-22","2026-06-23","2026-06-24","2026-06-25","2026-06-26","2026-06-29","2026-06-30","2026-07-01","2026-07-02","2026-07-06","2026-07-07","2026-07-08","2026-07-09","2026-07-10","2026-07-13","2026-07-14","2026-07-15","2026-07-16","2026-07-17","2026-07-20","2026-07-21","2026-07-22","2026-07-23","2026-07-24","2026-07-27","2026-07-28","2026-07-29","2026-07-30","2026-07-31","2026-08-03","2026-08-04","2026-08-05","2026-08-06","2026-08-07","2026-08-10","2026-08-11","2026-08-12","2026-08-13","2026-08-14","2026-08-17","2026-08-18","2026-08-19","2026-08-20","2026-08-21","2026-08-24","2026-08-25","2026-08-26","2026-08-27","2026-08-28","2026-08-31","2026-09-01","2026-09-02","2026-09-03","2026-09-04","2026-09-08","2026-09-09","2026-09-10","2026-09-11","2026-09-14","2026-09-15","2026-09-16","2026-09-17","2026-09-18","2026-09-21","2026-09-22","2026-09-23","2026-09-24","2026-09-25","2026-09-28","2026-09-29","2026-09-30","2026-10-01","2026-10-02","2026-10-05","2026-10-06","2026-10-07","2026-10-08","2026-10-09","2026-10-12","2026-10-13","2026-10-14","2026-10-15","2026-10-16"],"o":[163.67,161.64,160.68,160.05,160,159,156.28,158.53,159.67,160.48,157.28,153.91,155.77,155.5,158.34,151.67,151.69,151.92,150.39,151.39,150.45,152.01,152.28,151.18,154.24,154.87,152.57,151.39,149.23,147.56,147.27,148.9,150,148.84,149.75,147.23,152.7,151.19,154.56,154.22,158.75,155.75,154.12,150.5,151.27,151.75,152.42,152.64,156.84,158.09,160.42,161.29,164.5,167.75,167.41,165.26,163.51,163.46,159.84,156.48,151.27,151,148.98,145.75,147.25,146.56,148.1,148.84,150.02,151.2,162,158.05,153.15,148.5,148.18,152.67,151.5,149.6,145.16,144.61,152,150.8,147.31,146.38,145.42,145.65,152.08,154.16,156.68,156.02,152.65,151.64,153.44,153.64,151.98,151.7,151.78,150.02,147.68,146.2,145.2,142.9,144.76,141.15,138.94,135.5,138.7,140.44,139.66,139.54,136.99,138.78,137.63,138.56,136.89,136.7,136.63,137.03,139.8,140.4,144,144.5,147.47,146.82,149.2,151.98],"h":[164.16,162.45,161.7,161.67,160.61,159.15,158.35,163.54,161.03,161.53,157.98,155.18,158,158.09,159.16,152.23,154.24,153.82,151.66,152.83,153.2,153.7,154.03,154.7,155.63,155.7,154.75,152.09,150.18,149.1,148.19,151.1,150.37,151.41,150.7,152.6,153.37,154.35,155.1,155.45,160.24,157.26,154.71,152,153.5,153.39,153.11,156.87,158.44,160.16,161.64,164.95,166.04,170.14,167.41,167.54,166.31,164.97,161.26,158.04,152.98,156.6,149,148.38,150.09,149.39,149.39,150.75,151.57,153.93,166.16,158.05,155.61,150.35,153.16,153.51,151.95,149.87,145.76,150.39,154.78,150.99,148.07,146.97,148.16,152.4,153.39,158.05,157.54,156.19,152.84,154.57,155,154.59,153,153.93,153.23,151.17,148.64,147.01,145.69,147.11,145,141.35,140.95,138.32,139.95,141.15,141.22,140.62,139.63,140.3,138.57,138.57,137.45,137.88,138.06,139.97,141.18,144.08,145.47,147.1,148.67,147.06,151.38,152.42],"l":[161.47,159.47,158.8,159.7,158.82,154.65,155.06,158.52,158.22,157.11,153.78,153.39,154.61,155.23,151.71,149.39,150.11,149.69,150.33,149.93,150.26,150.67,151,150.54,153.68,153.85,151.49,149.32,147.33,145.44,145.9,148.21,147.53,147.9,147.16,146.6,150.74,150.69,153.01,153.56,156.71,153.73,151.2,149.51,150.78,151.2,150.77,151.54,154.96,157.03,158.63,160.98,162.32,166.63,163.02,158.25,162.44,161.58,154.44,152.01,149.67,147.67,146.36,144.64,145.07,146.35,147.69,147.7,149.03,150.83,155.47,152.21,148,146.71,148.13,150.4,150.24,145.14,143,144.42,150.72,148.08,143.26,144.77,143.88,145.32,149.58,154.15,155.8,154.1,150.05,151.44,152.85,151.93,150.89,149.84,150.22,148.54,142.26,143.42,140.85,142.79,140.68,137.78,135.69,134.26,137.79,138.86,138.15,137.25,136.4,137.55,137.03,136.3,135.89,135.95,135.35,136.75,139.8,140,143.04,143.93,145.98,144.2,149.2,149.52],"c":[161.82,160.34,159.06,160.73,160.28,155.17,156.46,159.84,159.65,158.36,154.4,153.89,156.02,157.25,151.95,150.29,152.86,149.77,150.63,149.97,152.31,152.19,152.71,154.32,155.15,154.01,152.02,150.03,147.95,147.51,146.58,148.44,148.08,150.87,147.37,151.63,151.08,153.27,154.31,154.67,157.67,154.07,151.99,150.74,151.6,153.02,151.14,156.41,158.24,159.86,161.59,164.73,165.99,167.49,165.25,162.39,163.54,161.68,158.18,152.57,152.1,147.92,147.37,147.17,147.57,147.37,148.92,147.8,150.28,151.92,157,153.28,148.8,147.68,152.37,152.28,150.76,145.33,144.75,150.34,150.85,148.11,145.14,144.86,145.93,152.33,152.99,157.35,156.27,154.41,150.75,153.86,153.31,152.03,151.27,152.54,150.71,149.51,145.16,143.89,140.87,146.33,140.77,137.96,135.75,137.99,139.33,139.71,140.63,137.34,138.63,138.28,137.73,136.34,136.69,136.12,136.97,139.84,140.35,143,144.82,146.75,147.06,146.69,150.83,150.51]};


/* ========================================================= entry points */
function buildDecisionPagesV2() {
  var t0 = Date.now();
  var ss = SpreadsheetApp.getActive();
  DP2X_MEMO_ = {};
  // v1.2.1: a refresh never seeds the _IBKR_* tabs — missing broker inputs are reported as unknown (menu: Seed IBKR Tabs)
  var pd = buildPortfolioDecisionV2();
  var t10 = buildTop10V2();
  var msg = 'Decision Pages v2 built in ' + Math.round((Date.now() - t0) / 1000) + 's — ' + DP2.SHEET_PD + ' (' + pd.rows + ' rows) · ' + DP2.SHEET_T10 + ' (' + t10.rows + ' rows)';
  try { ss.toast(msg, 'Decision Pages v2 ' + DP2_VERSION, 8); } catch (e) {}
  return { ok: true, pd: pd, t10: t10, ms: Date.now() - t0 };
}

function dp2SeedIbkrTabs() {
  var ss = SpreadsheetApp.getActive();
  var seeded = dp2EnsureIbkrTabs_(ss);
  var msg = 'IBKR tabs: ' + seeded.join(', ');
  try { SpreadsheetApp.getUi().alert('Decision Pages v2', msg, SpreadsheetApp.getUi().ButtonSet.OK); } catch (e) { Logger.log(msg); }
  return seeded;
}

function dp2InstallTrigger() {
  dp2RemoveTrigger();
  ScriptApp.newTrigger('buildDecisionPagesV2').timeBased().everyHours(1).create();
  var msg = 'Hourly trigger installed: buildDecisionPagesV2 rebuilds both v2 pages every hour and sends the dynamic-exit alerts (' + (dp2ExitAlertsEnabled_() ? 'alerts ON → ' + (dp2ExitAlertTo_() || 'no recipient') : 'alerts OFF') + ').';
  try { SpreadsheetApp.getUi().alert('Decision Pages v2', msg, SpreadsheetApp.getUi().ButtonSet.OK); } catch (e) { Logger.log(msg); }
  return true;
}
function dp2RemoveTrigger() {
  var trs = ScriptApp.getProjectTriggers();
  var n = 0;
  for (var i = 0; i < trs.length; i++) {
    if (trs[i].getHandlerFunction() === 'buildDecisionPagesV2') { ScriptApp.deleteTrigger(trs[i]); n++; }
  }
  return n;
}

/* Hook for 01_Menu.gs (v2.20.2 Today submenu): if (typeof dp2_addMenuItems === 'function') dp2_addMenuItems(m);
 * Not a button: running it from the IDE without a menu argument shows a hint instead of throwing. */
function dp2_addMenuItems(menu) {
  if (!menu || typeof menu.addItem !== 'function') {
    var hint = 'dp2_addMenuItems(menu) is the hook 01_Menu.gs (v2.20.2 or later) calls while it builds the Today submenu; it is not meant to be run on its own.\n\n' +
      'To build the pages run buildDecisionPagesV2 (both), buildPortfolioDecisionV2 or buildTop10V2 — or, after pasting 01_Menu v2.20.2 and reloading the spreadsheet, use Today ▸ 📊 Build Decision Pages v2.';
    try { SpreadsheetApp.getUi().alert('Decision Pages v2', hint, SpreadsheetApp.getUi().ButtonSet.OK); } catch (e) { Logger.log(hint); }
    return null;
  }
  return menu
    .addItem('📊 Build Decision Pages v2 (both)', 'buildDecisionPagesV2')
    .addItem('📊 Build Portfolio_Decision_v2', 'buildPortfolioDecisionV2')
    .addItem('📊 Build Top_10_Investments_v2', 'buildTop10V2')
    .addItem('🌱 Seed IBKR Tabs (first run)', 'dp2SeedIbkrTabs')
    .addItem('🧭 Dynamic Exits — status', 'dp2ExitStatus')
    .addItem('✉️ Dynamic Exits — send test alert', 'dp2ExitTestAlert')
    .addItem('🔔 Dynamic Exits — alerts on/off', 'dp2ExitAlertsToggle')
    .addItem('🧪 Self-Test: Decision Pages v2', 'dp2SelfTest');
}

/* v1.2.0 — top-level menu bar entry. Hook for 01_Menu.gs (v2.20.3) onOpen:
 *   if (typeof dp2_addTopMenu === 'function') { try { dp2_addTopMenu(ui); } catch (e) {} }
 * Builds '📊 Decision v2' next to the other menus: refresh either page or both, open the tabs,
 * dynamic-exit controls, the hourly trigger and the self-test. */
function dp2_addTopMenu(ui) {
  ui = ui || SpreadsheetApp.getUi();
  var m = ui.createMenu('📊 Decision v2')
    .addItem('🔄 Refresh both v2 pages', 'buildDecisionPagesV2')
    .addItem('🔄 Refresh Portfolio_Decision_v2', 'buildPortfolioDecisionV2')
    .addItem('🔄 Refresh Top_10_Investments_v2', 'buildTop10V2')
    .addSeparator()
    .addItem('↗ Open Portfolio_Decision_v2', 'dp2OpenPD')
    .addItem('↗ Open Top_10_Investments_v2', 'dp2OpenT10')
    .addSeparator()
    .addItem('🧭 Dynamic Exits — status', 'dp2ExitStatus')
    .addItem('✉️ Dynamic Exits — send test alert', 'dp2ExitTestAlert')
    .addItem('🔔 Dynamic Exits — alerts on/off', 'dp2ExitAlertsToggle')
    .addItem('✅ Acknowledge rule exit (hold by decision)', 'dp2ExitAckLatched')
    .addSeparator()
    .addItem('⏱ Install hourly refresh + alerts', 'dp2InstallTrigger')
    .addItem('⏹ Remove hourly refresh', 'dp2RemoveTriggerUi')
    .addItem('🌱 Seed IBKR Tabs (first run)', 'dp2SeedIbkrTabs')
    .addItem('🧪 Self-Test: Decision Pages v2', 'dp2SelfTest');
  m.addToUi();
  return m;
}
function dp2OpenSheet_(name) {
  var ss = dp2Ss_();
  var sh = ss.getSheetByName(name);
  if (!sh) { try { SpreadsheetApp.getUi().alert('Decision Pages v2', name + ' does not exist yet — run 📊 Decision v2 ▸ Refresh first.', SpreadsheetApp.getUi().ButtonSet.OK); } catch (e) {} return false; }
  ss.setActiveSheet(sh);
  return true;
}
function dp2OpenPD() { return dp2OpenSheet_(DP2.SHEET_PD); }
function dp2OpenT10() { return dp2OpenSheet_(DP2.SHEET_T10); }
function dp2RemoveTriggerUi() {
  var n = dp2RemoveTrigger();
  var msg = n ? 'Removed ' + n + ' hourly trigger(s) for buildDecisionPagesV2. The pages stay; nothing refreshes or alerts until you install it again.' : 'No hourly trigger was installed.';
  try { SpreadsheetApp.getUi().alert('Decision Pages v2', msg, SpreadsheetApp.getUi().ButtonSet.OK); } catch (e) { Logger.log(msg); }
  return n;
}

/* ========================================================= generic helpers */
function dp2Ss_() { return SpreadsheetApp.getActive(); }
function dp2Sheet_(name, create) {
  var ss = dp2Ss_();
  var sh = ss.getSheetByName(name);
  if (!sh && create) sh = ss.insertSheet(name);
  return sh;
}
function dp2Num_(v) {
  if (typeof v === 'number') return isNaN(v) ? null : v;
  if (v === null || v === undefined) return null;
  var s = String(v).trim();
  if (!s || s === '—' || s === '-' || s === 'n/a') return null;
  var neg = /^\(.*\)$/.test(s) || /^−/.test(s);
  s = s.replace(/[,%\s()]/g, '').replace(/^−/, '-').replace(/SAR|USD|p$/g, '');
  var n = parseFloat(s);
  if (isNaN(n)) return null;
  return neg && n > 0 ? -n : n;
}
function dp2Str_(v) { return (v === null || v === undefined) ? '' : String(v); }
function dp2Fmt_(n, d) {
  if (n === null || n === undefined || isNaN(n)) return '—';
  var f = Number(n).toFixed(d === undefined ? 0 : d);
  var parts = f.split('.');
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  return parts.join('.');
}
function dp2Pct_(n, d) { return (n === null || n === undefined || isNaN(n)) ? '—' : ((n >= 0 ? '+' : '−') + Math.abs(n).toFixed(d === undefined ? 1 : d) + '%'); }
function dp2Signed_(n, d) { if (n === null || n === undefined || isNaN(n)) return '—'; return (n >= 0 ? '+' : '−') + dp2Fmt_(Math.abs(n), d === undefined ? 0 : d); }
function dp2PctPlain_(n, d) { return (n === null || n === undefined || isNaN(n)) ? '—' : (Number(n).toFixed(d === undefined ? 1 : d) + '%'); }
function dp2Today_() { return Utilities.formatDate(new Date(), DP2.TZ, 'yyyy-MM-dd HH:mm'); }
function dp2ColLetter_(c) { var s = ''; while (c > 0) { var m = (c - 1) % 26; s = String.fromCharCode(65 + m) + s; c = Math.floor((c - 1) / 26); } return s; }
function dp2HeaderMap_(row) {
  var map = {};
  for (var i = 0; i < row.length; i++) { var k = dp2Str_(row[i]).trim(); if (k && map[k] === undefined) map[k] = i; }
  return map;
}
function dp2FindRow_(vals, pred, from) {
  for (var r = (from || 0); r < vals.length; r++) { if (pred(vals[r], r)) return r; }
  return -1;
}
function dp2Days_(fromDate) {
  if (!fromDate) return null;
  var d = (fromDate instanceof Date) ? fromDate : new Date(fromDate);
  if (isNaN(d.getTime())) return null;
  return Math.max(0, Math.round((Date.now() - d.getTime()) / 86400000));
}

/* ========================================================= sheet canvas helpers */
function dp2ResetSheet_(sh) {
  var charts = sh.getCharts();
  for (var i = 0; i < charts.length; i++) sh.removeChart(charts[i]);
  var maxR = Math.max(sh.getMaxRows(), 1), maxC = Math.max(sh.getMaxColumns(), 1);
  sh.getRange(1, 1, maxR, maxC).breakApart();
  sh.clear();
  sh.clearConditionalFormatRules();
  if (sh.getFrozenRows() > 0) sh.setFrozenRows(0);
  if (sh.getMaxColumns() < DP2.COLS) sh.insertColumnsAfter(sh.getMaxColumns(), DP2.COLS - sh.getMaxColumns());
  if (sh.getMaxColumns() > DP2.COLS) sh.deleteColumns(DP2.COLS + 1, sh.getMaxColumns() - DP2.COLS);
  if (sh.getMaxRows() < 170) sh.insertRowsAfter(sh.getMaxRows(), 170 - sh.getMaxRows());
  for (var c = 1; c <= DP2.COLS; c++) sh.setColumnWidth(c, DP2.COL_WIDTH);
  sh.setHiddenGridlines(true);
  sh.getRange(1, 1, sh.getMaxRows(), DP2.COLS).setBackground(DP2C.page).setFontFamily('Arial').setFontSize(10).setFontColor(DP2C.ink).setVerticalAlignment('middle');
}
function dp2Range_(sh, row, c1, c2, rows) { return sh.getRange(row, c1, rows || 1, (c2 || c1) - c1 + 1); }
function dp2Box_(rng) {
  rng.setBackground(DP2C.surface);
  rng.setBorder(true, true, true, true, false, false, DP2C.border, SpreadsheetApp.BorderStyle.SOLID);
}
function dp2Header_(sh, row, title, chipText, chipKind, rightText) {
  var left = dp2Range_(sh, row, 1, 10).merge();
  left.setValue(title).setBackground(DP2C.navy).setFontColor('#ffffff').setFontWeight('bold').setFontSize(14).setHorizontalAlignment('left');
  var chip = dp2Range_(sh, row, 11, 12).merge();
  dp2Chip_(chip, chipText, chipKind, true);
  var right = dp2Range_(sh, row, 13, 16).merge();
  right.setValue(rightText || '').setBackground(DP2C.navy).setFontColor('#d5dde7').setFontSize(9).setHorizontalAlignment('right');
  sh.setRowHeight(row, 34);
}
function dp2Chip_(rng, text, kind, onNavy) {
  var bg, fg;
  if (kind === 'red') { bg = '#b42318'; fg = '#ffffff'; }
  else if (kind === 'amber') { bg = onNavy ? DP2C.gold : DP2C.amberBg; fg = onNavy ? DP2C.navy : DP2C.amber; }
  else if (kind === 'green') { bg = onNavy ? '#1a7f37' : DP2C.greenBg; fg = onNavy ? '#ffffff' : DP2C.green; }
  else if (kind === 'navy') { bg = DP2C.headerBg; fg = DP2C.navyText; }
  else { bg = '#eef0f3'; fg = DP2C.ink2; }
  rng.setValue(text).setBackground(bg).setFontColor(fg).setFontWeight('bold').setFontSize(10).setHorizontalAlignment('center').setVerticalAlignment('middle');
}
function dp2Line_(sh, row, text, opts) {
  opts = opts || {};
  var c1 = opts.c1 || 1, c2 = opts.c2 || DP2.COLS;
  var rng = dp2Range_(sh, row, c1, c2).merge();
  rng.setValue(text).setFontColor(opts.color || DP2C.ink2).setFontSize(opts.size || 9.5).setWrap(opts.wrap !== false).setHorizontalAlignment(opts.align || 'left').setVerticalAlignment('middle');
  if (opts.bold) rng.setFontWeight('bold');
  if (opts.bg) rng.setBackground(opts.bg);
  if (opts.height) sh.setRowHeight(row, opts.height);
  return rng;
}
function dp2Section_(sh, row, c1, c2, title, sub) {
  var rng = dp2Range_(sh, row, c1, c2).merge();
  rng.setValue(title).setFontWeight('bold').setFontSize(10).setFontColor(DP2C.navyText).setBackground(DP2C.surface).setHorizontalAlignment('left');
  if (sub) { var s = dp2Range_(sh, row + 1, c1, c2).merge(); s.setValue(sub).setFontSize(8.5).setFontColor(DP2C.ink2).setBackground(DP2C.surface).setWrap(true); sh.setRowHeight(row + 1, 30); }
  return row + (sub ? 2 : 1);
}
/* tiles: array of {label, value, sub, color}; each tile = 2 columns × 3 rows */
function dp2Tiles_(sh, row, tiles) {
  var c = 1;
  for (var i = 0; i < tiles.length && c + 1 <= DP2.COLS; i++) {
    var t = tiles[i];
    var box = dp2Range_(sh, row, c, c + 1, 3);
    dp2Box_(box);
    var l = dp2Range_(sh, row, c, c + 1).merge(); l.setNumberFormat('@').setValue(dp2Str_(t.label).toUpperCase()).setFontSize(8).setFontColor(DP2C.ink2).setHorizontalAlignment('left');
    var v = dp2Range_(sh, row + 1, c, c + 1).merge(); v.setNumberFormat('@').setValue(t.value).setFontSize(16).setFontWeight('bold').setFontColor(t.color || DP2C.ink).setHorizontalAlignment('left');
    var s = dp2Range_(sh, row + 2, c, c + 1).merge(); s.setNumberFormat('@').setValue(t.sub || '').setFontSize(8).setFontColor(DP2C.ink2).setWrap(true).setHorizontalAlignment('left');
    c += 2;
  }
  sh.setRowHeight(row, 18); sh.setRowHeight(row + 1, 28); sh.setRowHeight(row + 2, 30);
  return row + 3;
}
/* table: headers[], rows[][], opts {c1, widths (array of column spans), align: ['l','r',...], fmt: ['', '#,##0', ...], wrap: [bool], rowHeight} */
function dp2Table_(sh, row, headers, rows, opts) {
  opts = opts || {};
  var c1 = opts.c1 || 1;
  var spans = opts.spans || [];
  var cols = [];
  var c = c1;
  for (var i = 0; i < headers.length; i++) { var w = spans[i] || 1; cols.push([c, c + w - 1]); c += w; }
  var lastCol = c - 1;
  var hdr = dp2Range_(sh, row, c1, lastCol);
  hdr.setBackground(DP2C.headerBg).setFontColor(DP2C.navyText).setFontWeight('bold').setFontSize(9);
  for (i = 0; i < headers.length; i++) {
    var hr = dp2Range_(sh, row, cols[i][0], cols[i][1]);
    if (cols[i][1] > cols[i][0]) hr.merge();
    hr.setValue(headers[i]).setHorizontalAlignment((opts.align && opts.align[i] === 'r') ? 'right' : 'left').setWrap(true);
  }
  sh.setRowHeight(row, 30);
  for (var r = 0; r < rows.length; r++) {
    var rr = row + 1 + r;
    var band = dp2Range_(sh, rr, c1, lastCol);
    band.setBackground(r % 2 === 1 ? DP2C.zebra : DP2C.surface).setFontSize(9.5);
    for (i = 0; i < headers.length; i++) {
      var cell = dp2Range_(sh, rr, cols[i][0], cols[i][1]);
      if (cols[i][1] > cols[i][0]) cell.merge();
      var val = rows[r][i];
      if (typeof val === 'string' && val.charAt(0) === '=') cell.setFormula(val); else cell.setValue(val === undefined ? '' : val);
      cell.setHorizontalAlignment((opts.align && opts.align[i] === 'r') ? 'right' : ((opts.align && opts.align[i] === 'c') ? 'center' : 'left'));
      if (opts.fmt && opts.fmt[i]) cell.setNumberFormat(opts.fmt[i]);
      if (opts.wrap && opts.wrap[i]) cell.setWrap(true);
    }
    sh.setRowHeight(rr, opts.rowHeight || 24);
  }
  var all = dp2Range_(sh, row, c1, lastCol, rows.length + 1);
  all.setBorder(true, true, true, true, false, true, DP2C.border, SpreadsheetApp.BorderStyle.SOLID);
  return row + 1 + rows.length;
}
/* key/value block: pairs [[k, v, color?]] within columns c1..c2 (value in the last 2 columns) */
function dp2KV_(sh, row, c1, c2, pairs) {
  for (var i = 0; i < pairs.length; i++) {
    var rr = row + i;
    var band = dp2Range_(sh, rr, c1, c2); band.setBackground(DP2C.surface).setFontSize(9.5);
    var k = dp2Range_(sh, rr, c1, c2 - 3).merge(); k.setValue(pairs[i][0]).setFontColor(DP2C.ink2).setWrap(true);
    var v = dp2Range_(sh, rr, c2 - 2, c2).merge(); v.setNumberFormat('@').setValue(pairs[i][1]).setHorizontalAlignment('right').setFontFamily('Roboto Mono').setFontSize(9).setFontColor(pairs[i][2] || DP2C.ink);
    sh.setRowHeight(rr, 22);
  }
  dp2Range_(sh, row, c1, c2, pairs.length).setBorder(true, true, true, true, false, false, DP2C.border, SpreadsheetApp.BorderStyle.SOLID);
  return row + pairs.length;
}
/* chart data: each page owns a hidden data sheet (_DP2_Charts_PD / _DP2_Charts_T10), reset by its builder */
var DP2_CHART_ROW_ = 1;
var DP2_CHART_SHEET_ = null;
function dp2ChartsBegin_(name) {
  var sh = dp2Sheet_(name, true);
  sh.clear();
  DP2_CHART_SHEET_ = sh;
  DP2_CHART_ROW_ = 1;
  try { sh.hideSheet(); } catch (e) {}
  return sh;
}
function dp2ChartData_(title, header, rows) {
  var sh = DP2_CHART_SHEET_ || dp2ChartsBegin_(DP2.SHEET_CHARTS);
  sh.getRange(DP2_CHART_ROW_, 1).setValue('## ' + title);
  var start = DP2_CHART_ROW_ + 1;
  var data = [header].concat(rows);
  var width = header.length;
  for (var i = 0; i < data.length; i++) { while (data[i].length < width) data[i].push(''); }
  sh.getRange(start, 1, data.length, width).setValues(data);
  DP2_CHART_ROW_ = start + data.length + 1;
  return sh.getRange(start, 1, data.length, width);
}
function dp2Chart_(sh, type, range, row, col, opts) {
  opts = opts || {};
  var b = sh.newChart().addRange(range).setPosition(row, col, 0, 0)
    .setOption('width', opts.width || 640).setOption('height', opts.height || 250)
    .setOption('backgroundColor', DP2C.surface)
    .setOption('fontName', 'Arial')
    .setOption('titleTextStyle', { color: DP2C.navyText, fontSize: 11, bold: true })
    .setOption('legend', opts.legend || { position: 'none' })
    .setOption('chartArea', opts.chartArea || { left: 150, top: 30, width: '70%', height: '70%' });
  if (opts.title) b.setOption('title', opts.title);
  if (opts.colors) b.setOption('colors', opts.colors);
  if (opts.hAxis) b.setOption('hAxis', opts.hAxis);
  if (opts.vAxis) b.setOption('vAxis', opts.vAxis);
  if (opts.isStacked !== undefined) b.setOption('isStacked', opts.isStacked);
  if (opts.series) b.setOption('series', opts.series);
  if (opts.pointSize) b.setOption('pointSize', opts.pointSize);
  if (opts.useFirstColumnAsDomain !== undefined) b.setOption('useFirstColumnAsDomain', opts.useFirstColumnAsDomain);
  if (type === 'BAR') b.setChartType(Charts.ChartType.BAR);
  else if (type === 'COLUMN') b.setChartType(Charts.ChartType.COLUMN);
  else if (type === 'LINE') b.setChartType(Charts.ChartType.LINE);
  else if (type === 'SCATTER') b.setChartType(Charts.ChartType.SCATTER);
  else if (type === 'AREA') b.setChartType(Charts.ChartType.AREA);
  if (opts.lineWidth) b.setOption('lineWidth', opts.lineWidth);
  if (opts.bar) b.setOption('bar', opts.bar);
  if (opts.annotations) b.setOption('annotations', opts.annotations);
  sh.insertChart(b.build());
}

/* ========================================================= readers: workbook */
function dp2ReadStatusKV_() {
  var sh = dp2Sheet_(DP2.SRC_STATUS, false);
  var out = {};
  if (!sh) return out;
  var vals = sh.getDataRange().getValues();
  if (!vals.length) return out;
  var hdr = dp2HeaderMap_(vals[0]);
  var kc = hdr['Global Key'], vc = hdr['Value'];
  if (kc === undefined) return out;
  for (var r = 1; r < vals.length; r++) { var k = dp2Str_(vals[r][kc]).trim(); if (k) out[k] = dp2Str_(vals[r][vc]); }
  return out;
}
function dp2ParseFeed_(kv) {
  var s = kv['TFB Decision Feed'] || '';
  var m = /^([A-Z_]+)(?:\(([^)]*)\))?/.exec(s);
  var run = /run=(\d+)/.exec(s);
  var when = /\|\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})/.exec(s);
  var reasons = s.split('|').pop().trim();
  return { raw: s, state: m ? m[1] : '', blocker: m && m[2] ? m[2] : '', run: run ? run[1] : '', at: when ? when[1] : '', reasons: /failed|passed|ok/i.test(reasons) ? reasons : '' };
}
function dp2ReadTop10_() {
  var sh = dp2Sheet_(DP2.SRC_T10, false);
  var out = { ok: false, status: '', kpi: {}, panel: {}, seats: [], qualified: [], gaps: [], banner: '', alerts: {} };
  if (!sh) return out;
  var vals = sh.getDataRange().getValues();
  out.ok = true;
  // v1.2.15: the engine's ALERTS table (type · count · required action) — the cash_floor row states the reserve the engine took
  (function () {
    var inA = false;
    for (var ra = 0; ra < vals.length; ra++) {
      var c0 = dp2Str_(vals[ra][0]).trim();
      if (/^ALERTS/.test(c0)) { inA = true; continue; }
      if (!inA) continue;
      if (!c0) { if (Object.keys(out.alerts).length) break; else continue; }
      if (c0 === 'Type') continue;
      if (/^[a-z][a-z0-9_]*$/.test(c0)) out.alerts[c0] = { count: dp2Num_(vals[ra][1]), text: dp2Str_(vals[ra][2]) };
      else if (Object.keys(out.alerts).length) break;
    }
  })();
  out.status = dp2Str_(vals.length > 1 ? vals[1][1] : '');
  // control panel: scan the first 16 rows for label/value pairs
  for (var r = 0; r < Math.min(16, vals.length); r++) {
    for (var c = 0; c + 1 < vals[r].length; c++) {
      var lab = dp2Str_(vals[r][c]).trim();
      if (/^T10: |^Cash Available|^Pending Proceeds|^Pool /.test(lab)) out.panel[lab] = vals[r][c + 1];
    }
  }
  // KPI row: the label row that holds 'Entered cash' in any column; values sit in the next row
  var kr = dp2FindRow_(vals, function (row) { for (var c = 0; c < row.length; c++) { var cl = dp2Str_(row[c]).trim(); if (/^entered cash/i.test(cl) || /^deployable \(sar\)$/i.test(cl)) return true; } return false; });   // v1.2.15: current layout heads the KPI row with 'Deployable (SAR)'
  if (kr >= 0 && kr + 1 < vals.length) { for (var i = 0; i < vals[kr].length; i++) { var k = dp2Str_(vals[kr][i]).trim(); if (k) out.kpi[k] = vals[kr + 1][i]; } }
  // v1.2.22: the cockpit's first KPI slot ('Entered cash (SAR)', formerly 'Deployable (SAR)') carries the engine's
  // kpis.deployable_sar = cash + pending proceeds LESS the absolute reserve (_select_and_size). The operator's cash is
  // the panel input 'Cash Available (SAR)'; the KPI is kept as the engine's deployable (dp2CashReserve_ reads the
  // reserve the last build took from the difference).
  out.enteredCash = null; out.kpiDeployable = null; out.panelCash = null; out.panelProceeds = null;
  for (var kk in out.kpi) { if (out.kpi.hasOwnProperty(kk) && (/^entered cash/i.test(kk) || /^deployable \(sar\)$/i.test(kk))) { out.kpiDeployable = dp2Num_(out.kpi[kk]); } }
  for (kk in out.panel) { if (out.panel.hasOwnProperty(kk)) { if (/^cash available/i.test(kk)) out.panelCash = dp2Num_(out.panel[kk]); else if (/^pending proceeds/i.test(kk)) out.panelProceeds = dp2Num_(out.panel[kk]); } }
  out.enteredCash = out.panelCash !== null ? out.panelCash : out.kpiDeployable;
  // banner
  var br = dp2FindRow_(vals, function (row) { var s = dp2Str_(row[0]); return /FEED|SELECTED|EXECUTABLE/.test(s) && s.length > 20 && !/^Status/.test(s); }, kr >= 0 ? kr : 0);
  if (br >= 0) out.banner = dp2Str_(vals[br][0]);
  // seats: first header row with Rank/Symbol
  var sr = dp2FindRow_(vals, function (row) { return dp2Str_(row[0]).trim() === 'Rank' && dp2Str_(row[1]).trim() === 'Symbol'; });
  if (sr >= 0) {
    var h = dp2HeaderMap_(vals[sr]);
    for (r = sr + 1; r < vals.length; r++) {
      var sym = dp2Str_(vals[r][h['Symbol']]).trim();
      if (!sym) break;
      out.seats.push(dp2RowObj_(vals[r], h));
    }
  }
  // all qualified
  var qr = dp2FindRow_(vals, function (row) { return /^ALL QUALIFIED/.test(dp2Str_(row[0])); });
  if (qr >= 0 && qr + 1 < vals.length) {
    var hq = dp2HeaderMap_(vals[qr + 1]);
    for (r = qr + 2; r < vals.length; r++) {
      var qs = dp2Str_(vals[r][hq['Symbol']]).trim();
      if (!qs) break;
      out.qualified.push(dp2RowObj_(vals[r], hq));
    }
    var mq = /\((\d+)\)/.exec(dp2Str_(vals[qr][0]));
    out.qualifiedCount = mq ? parseInt(mq[1], 10) : out.qualified.length;
  }
  // data gaps
  var gr = dp2FindRow_(vals, function (row) { return /^DATA GAPS/.test(dp2Str_(row[0])); });
  if (gr >= 0 && gr + 1 < vals.length) {
    var mg = /(\d+) of (\d+) passed/.exec(dp2Str_(vals[gr][0]));
    out.gapsPassed = mg ? parseInt(mg[1], 10) : null;
    out.gapsTotal = mg ? parseInt(mg[2], 10) : null;
    var hg = dp2HeaderMap_(vals[gr + 1]);
    for (r = gr + 2; r < vals.length; r++) {
      var g = dp2Str_(vals[r][0]).trim();
      if (!g) break;
      out.gaps.push({ gate: g, failed: dp2Num_(vals[r][hg['Candidates Failed'] !== undefined ? hg['Candidates Failed'] : 1]), share: dp2Num_(vals[r][hg['Share %'] !== undefined ? hg['Share %'] : 2]), examples: dp2Str_(vals[r][3]) });
    }
  }
  // status parsing
  var st = out.status;
  var mpool = /sheets pool (\d+) rows/.exec(st); out.scanned = mpool ? parseInt(mpool[1], 10) : null;
  var mout = /output: ([A-Z_]+)/.exec(st); out.output = mout ? mout[1] : '';
  var mrun = /Last run (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/.exec(st); out.lastRun = mrun ? mrun[1] : '';
  var mstab = /stab\[[^\]]*\]: (\d+) grace, (\d+) ft-carried/.exec(st); out.grace = mstab ? parseInt(mstab[1], 10) : null; out.fastTrack = mstab ? parseInt(mstab[2], 10) : null;
  var mep = /epoch=([^|]+)/.exec(st); out.epoch = mep ? mep[1].trim() : '';
  var mearn = /earn ⚠(\d+)\/(\d+)/.exec(st); out.earnFlagged = mearn ? parseInt(mearn[1], 10) : null;
  var mdup = /\((\d+) duplicate symbols removed\)/.exec(st); out.duplicates = mdup ? parseInt(mdup[1], 10) : null;
  return out;
}
function dp2RowObj_(row, h) {
  var o = {};
  for (var k in h) if (h.hasOwnProperty(k)) o[k] = row[h[k]];
  return o;
}
function dp2ReadPortfolioDecision_() {
  var sh = dp2Sheet_(DP2.SRC_PD, false);
  var out = { ok: false, status: '', panel: {} };
  if (!sh) return out;
  var vals = sh.getDataRange().getValues();
  out.ok = true;
  out.status = dp2Str_(vals.length > 1 ? vals[1][1] : '');
  for (var r = 0; r < Math.min(10, vals.length); r++) {
    for (var c = 0; c + 1 < vals[r].length; c++) { var lab = dp2Str_(vals[r][c]).trim(); if (/^PF: /.test(lab)) out.panel[lab] = vals[r][c + 1]; }
  }
  var mrun = /source readiness ([A-Z_]+)(?: \(run (\d+)\))?/.exec(out.status); out.readiness = mrun ? mrun[1] : ''; out.receiptRun = mrun && mrun[2] ? mrun[2] : '';
  var mev = /ibkr evidence ([A-Z_ ]+?):\s*capture ([0-9:]+) is ([0-9hm ]+) old \(limit ([0-9]+)m\)/.exec(out.status);
  out.evidence = mev ? { state: mev[1].trim(), capture: mev[2], age: mev[3].trim(), limit: parseInt(mev[4], 10) } : null;
  var mev2 = /ibkr evidence ([A-Z_ ]+?)(?:;|\|)/.exec(out.status); if (!out.evidence && mev2) out.evidence = { state: mev2[1].trim(), capture: '', age: '', limit: DP2.EVIDENCE_LIMIT_MIN };
  var mfx = /USD ([0-9.]+)/.exec(out.status); out.fx = mfx ? parseFloat(mfx[1]) : null;
  var mlr = /Last run (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/.exec(out.status); out.lastRun = mlr ? mlr[1] : '';
  var mex = /excluded ([A-Z0-9., ]+?)(?: \||$)/.exec(out.status); out.excluded = mex ? mex[1].trim() : '';
  return out;
}
function dp2ReadLedger_() {
  var sh = dp2Sheet_(DP2.SRC_LEDGER, false);
  var out = { ok: false, lots: [], totals: {}, status: '' };
  if (!sh) return out;
  var vals = sh.getDataRange().getValues();
  out.ok = true;
  out.status = dp2Str_(vals.length > 1 ? vals[1][1] : '');
  var mt = /closed (-?[\d,.]+) · active (-?[\d,.]+) · lifetime (-?[\d,.]+)/.exec(out.status);
  if (mt) out.totals = { closed: dp2Num_(mt[1]), active: dp2Num_(mt[2]), lifetime: dp2Num_(mt[3]) };
  var mc = /(\d+) active · (\d+) closed/.exec(out.status); if (mc) { out.totals.activeCount = parseInt(mc[1], 10); out.totals.closedCount = parseInt(mc[2], 10); }
  var hr = dp2FindRow_(vals, function (row) { return dp2Str_(row[0]).trim() === 'Symbol' && dp2Str_(row[3]).trim() === 'Status'; });
  if (hr < 0) return out;
  var h = dp2HeaderMap_(vals[hr]);
  for (var r = hr + 1; r < vals.length; r++) {
    var sym = dp2Str_(vals[r][h['Symbol']]).trim();
    if (!sym) continue;
    if (dp2Str_(vals[r][h['Status']]).trim() !== 'Active') continue;
    var o = dp2RowObj_(vals[r], h);
    out.lots.push({
      symbol: sym, name: dp2Str_(o['Name']), ccy: dp2Str_(o['Ccy']), buyDate: o['Buy Date'], buyPrice: dp2Num_(o['Buy Price']), shares: dp2Num_(o['Shares']),
      costBasis: dp2Num_(o['Cost Basis']), price: dp2Num_(o['Current Price']), fx: dp2Num_(o['FX→SAR']), mvSar: dp2Num_(o['Market Value SAR']),
      unrealSar: dp2Num_(o['Unrealized G/L SAR']), dividends: dp2Num_(o['Dividends Recv']), totalReturnSar: dp2Num_(o['Total Return SAR']), returnPct: dp2Num_(o['Return %']),
      holdingDays: dp2Num_(o['Holding Days'])
    });
  }
  return out;
}
function dp2ReadMyPortfolio_() {
  var sh = dp2Sheet_(DP2.SRC_MP, false);
  var out = {};
  if (!sh) return out;
  var vals = sh.getDataRange().getValues();
  if (vals.length < 2) return out;
  var h = dp2HeaderMap_(vals[0]);
  for (var r = 1; r < vals.length; r++) {
    var sym = dp2Str_(vals[r][h['Symbol']]).trim();
    if (!sym) continue;
    out[sym] = {
      assetClass: dp2Str_(vals[r][h['Asset Class']]), sector: dp2Str_(vals[r][h['Sector']]), industry: dp2Str_(vals[r][h['Industry']]), exchange: dp2Str_(vals[r][h['Exchange']]),
      price: dp2Num_(vals[r][h['Current Price']]), w52hi: dp2Num_(vals[r][h['52W High']]), w52lo: dp2Num_(vals[r][h['52W Low']]), w52pos: dp2Num_(vals[r][h['52W Position %']]),
      rsi: dp2Num_(vals[r][h['RSI (14)']]), roi1m: dp2Num_(vals[r][h['Expected ROI 1M']]), roi3m: dp2Num_(vals[r][h['Expected ROI 3M']]), roi12m: dp2Num_(vals[r][h['Expected ROI 12M']]),
      overall: dp2Num_(vals[r][h['Overall Score']]), risk: dp2Str_(vals[r][h['Risk Bucket']]), reco: dp2Str_(vals[r][h['Recommendation']]), recoDetail: dp2Str_(vals[r][h['Recommendation Detail']]),
      qty: dp2Num_(vals[r][h['Position Qty']]), avgCost: dp2Num_(vals[r][h['Avg Cost']]), vol30: dp2Num_(vals[r][h['Volatility 30D']]),
      forecastSource: h['Forecast Source'] !== undefined ? dp2Str_(vals[r][h['Forecast Source']]) : '', warnings: h['Warnings'] !== undefined ? dp2Str_(vals[r][h['Warnings']]) : ''   // v1.2.20: the rotation floor guard's provenance inputs
    };
  }
  return out;
}
function dp2ReadCash_() {
  var sh = dp2Sheet_(DP2.SRC_CASH, false);
  if (!sh) return null;
  var vals = sh.getDataRange().getValues();
  if (vals.length < 2) return null;
  var h = dp2HeaderMap_(vals[0]);
  var bc = h['Balance SAR'] !== undefined ? h['Balance SAR'] : 4;
  var dc = h['Date'] !== undefined ? h['Date'] : 0;
  for (var r = vals.length - 1; r >= 1; r--) { var b = dp2Num_(vals[r][bc]); if (b !== null) return { balance: b, date: vals[r][dc] }; }
  return null;
}
/* v1.2.2 — ONE cash basis for both pages and the shared allocation memo (Codex P1): the Top_10 panel cash
   (v1.2.22: the panel input 'Cash Available (SAR)'; the KPI slot is the engine's deployable after its reserve and is
   used only when no panel cash is found) → the IBKR cash (v1.2.22: the daily read-only capture when it is newer than
   the _IBKR_* tabs) × fx (STALE-flagged past DP2.IBKR_TABS_STALE_H) → _Cash_Snapshot → none. The balances not chosen
   are kept so the pages can disclose a discrepancy. */
function dp2CashBasis_(t, ib, fx) {
  var out = { sar: null, source: 'n/a', basis: 'none', entered: null, ibkrSar: null, snapSar: null, snapDate: '', other: null, otherLabel: '', diffPct: null, warn: false };
  var cashUsd = dp2Num_(ib && ib.account ? ib.account['cash_usd'] : null);
  if (cashUsd !== null) out.ibkrSar = cashUsd * (fx || DP2.FX_DEFAULT);
  var snap = null; try { snap = dp2ReadCash_(); } catch (eC) { snap = null; }
  if (snap) { out.snapSar = dp2Num_(snap.balance); out.snapDate = dp2DateStr_(snap.date) || dp2Str_(snap.date); }
  out.entered = (t && t.enteredCash !== null && t.enteredCash !== undefined) ? dp2Num_(t.enteredCash) : null;
  out.kpiDeployable = (t && t.kpiDeployable !== undefined) ? t.kpiDeployable : null;   // v1.2.22
  if (out.entered !== null) { out.sar = out.entered; out.basis = 'top10'; out.source = (t && t.panelCash !== null && t.panelCash !== undefined) ? 'Top_10 panel (Cash Available)' : 'Top_10 KPI (the engine\'s deployable; no panel cash found)'; }
  else if (out.ibkrSar !== null) { out.sar = out.ibkrSar; out.basis = 'ibkr'; out.source = 'IBKR ' + dp2Str_(ib.account['as_of']) + (ib.stale ? ' — STALE snapshot' : ''); }
  else if (out.snapSar !== null) { out.sar = out.snapSar; out.basis = 'snapshot'; out.source = '_Cash_Snapshot ' + out.snapDate; }
  if (out.basis !== 'ibkr' && out.ibkrSar !== null) { out.other = out.ibkrSar; out.otherLabel = 'IBKR snapshot ' + dp2Str_(ib.account['as_of']) + (ib.ageH !== null ? ' (' + dp2Fmt_(ib.ageH, 0) + ' h old' + (ib.stale ? ', STALE' : '') + ')' : ''); }
  else if (out.snapSar !== null && out.basis !== 'snapshot') { out.other = out.snapSar; out.otherLabel = '_Cash_Snapshot ' + out.snapDate; }
  if (out.sar !== null && out.other !== null && out.sar > 0) { out.diffPct = (out.other - out.sar) / out.sar * 100; out.warn = Math.abs(out.diffPct) > DP2.CASH_DIFF_WARN_PCT; }
  return out;
}
/* One line for the pages: which balance the allocation uses and how far the other reading is from it. */
function dp2CashBasisText_(cb) {
  if (!cb || cb.sar === null) return 'no cash figure — enter it in the Top_10 panel';
  var t = dp2Fmt_(cb.sar) + ' SAR from ' + cb.source;
  if (cb.other !== null) t += ' · ' + cb.otherLabel + ' reads ' + dp2Fmt_(cb.other) + ' SAR (' + (cb.diffPct >= 0 ? '+' : '') + dp2Fmt_(cb.diffPct, 1) + '%)' + (cb.warn ? ' — RECONCILE before placing orders' : '');
  return t;
}
/* v1.2.22: the coverage blocker names the page that fails. The sync's Name floor (final publication D3) allows at
 * most 1 % blank Names per market page (Mutual_Funds 2,474 rows → 24); recounted here from the four source sheets
 * (Symbol and Name columns only). A blank Name is filled by the sync from the reviewed captions in
 * data/verified_name_labels.json, which expire after seven days unless the Verified Name Label Renewal workflow
 * renews them. */
var DP2_NAME_PAGES_ = ['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds'];
var DP2_NAME_COV_MEMO_ = null;
function dp2NameCoverage_() {
  if (DP2_NAME_COV_MEMO_) return DP2_NAME_COV_MEMO_;
  var res = [];
  for (var p = 0; p < DP2_NAME_PAGES_.length; p++) {
    var sh = dp2Sheet_(DP2_NAME_PAGES_[p], false);
    if (!sh || sh.getLastRow() < 2) continue;
    var hdr = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0], si = -1, ni = -1;
    for (var c = 0; c < hdr.length; c++) { var h = dp2Str_(hdr[c]).trim(); if (h === 'Symbol' && si < 0) si = c; else if (h === 'Name' && ni < 0) ni = c; }
    if (si < 0 || ni < 0) continue;
    var n = sh.getLastRow() - 1, sv = sh.getRange(2, si + 1, n, 1).getValues(), nv = sh.getRange(2, ni + 1, n, 1).getValues(), rows = 0, blank = 0;
    for (var r = 0; r < n; r++) { if (!dp2Str_(sv[r][0]).trim()) continue; rows++; if (!dp2Str_(nv[r][0]).trim()) blank++; }
    var allowed = Math.floor(rows * 0.01 + 1e-9);
    res.push({ page: DP2_NAME_PAGES_[p], rows: rows, blank: blank, allowed: allowed, pct: rows ? (rows - blank) / rows * 100 : null, fail: blank > allowed });
  }
  DP2_NAME_COV_MEMO_ = res;
  return res;
}
function dp2CoverageNote_() {
  var nc = [];
  try { nc = dp2NameCoverage_(); } catch (eN) { nc = []; }
  var bad = nc.filter(function (x) { return x.fail; });
  if (!bad.length) return 'a market page is below its coverage floor — see the sync\'s final publication';
  return bad.map(function (x) { return x.page + ' Name coverage ' + dp2Fmt_(x.pct, 2) + '% (' + x.blank + ' blank Names, ' + x.allowed + ' allowed)'; }).join('; ') + ' — below the 99% floor; renew the reviewed Name captions (workflow Verified Name Label Renewal)';
}
function dp2ReadGmInvestability_() {
  var sh = dp2Sheet_(DP2.SRC_GM, false);
  var out = { total: 0, counts: {} };
  if (!sh) return out;
  var last = sh.getLastColumn();
  var hdr = sh.getRange(1, 1, 1, last).getValues()[0];
  var h = dp2HeaderMap_(hdr);
  var ic = h['Investability Status'];
  if (ic === undefined) return out;
  var n = sh.getLastRow() - 1;
  if (n <= 0) return out;
  var col = sh.getRange(2, ic + 1, n, 1).getValues();
  for (var r = 0; r < col.length; r++) { var v = dp2Str_(col[r][0]).trim().toUpperCase(); if (!v) continue; out.total++; out.counts[v] = (out.counts[v] || 0) + 1; }
  return out;
}
function dp2ReadHitRates_() {
  var sh = dp2Sheet_(DP2.SRC_PERF, false);
  var out = { ok: false, labels: [] };
  if (!sh) return out;
  var vals = sh.getDataRange().getValues();
  var hr = dp2FindRow_(vals, function (row) { return dp2Str_(row[0]).trim() === 'Record ID'; });
  if (hr < 0) return out;
  var h = dp2HeaderMap_(vals[hr]);
  var cStatus = h['Status'], cOut = h['Outcome'], cInv = h['Entry Investability'], cReco = h['Entry Recommendation'], cHor = h['Horizon'];
  var agg = { 'INVESTABLE': [0, 0], 'WATCHLIST': [0, 0], 'ACCUMULATE / 1M': [0, 0], 'ALL': [0, 0] };
  for (var r = hr + 1; r < vals.length; r++) {
    if (dp2Str_(vals[r][cStatus]).toLowerCase() !== 'matured') continue;
    var o = dp2Str_(vals[r][cOut]).toUpperCase();
    if (o !== 'WIN' && o !== 'LOSS') continue;
    var win = o === 'WIN' ? 1 : 0;
    agg['ALL'][0] += win; agg['ALL'][1]++;
    var inv = dp2Str_(vals[r][cInv]).toUpperCase();
    if (agg[inv]) { agg[inv][0] += win; agg[inv][1]++; }
    if (/ACCUMULATE/.test(dp2Str_(vals[r][cReco]).toUpperCase()) && dp2Str_(vals[r][cHor]) === '1M') { agg['ACCUMULATE / 1M'][0] += win; agg['ACCUMULATE / 1M'][1]++; }
  }
  out.ok = true;
  var order = ['INVESTABLE', 'WATCHLIST', 'ACCUMULATE / 1M', 'ALL'];
  for (var i = 0; i < order.length; i++) { var a = agg[order[i]]; out.labels.push({ label: order[i], wins: a[0], n: a[1], rate: a[1] ? a[0] / a[1] : null }); }
  return out;
}
function dp2ReadS1_() {
  var sh = dp2Sheet_(DP2.SRC_S1, false);
  var out = { ok: false, verdict: '', criteria: [] };
  if (!sh) return out;
  var vals = sh.getDataRange().getValues();
  out.ok = true;
  var mv = /verdict: ([A-Z_]+)/.exec(dp2Str_(vals[0] ? vals[0].join(' ') : ''));
  out.verdict = mv ? mv[1] : '';
  var hr = dp2FindRow_(vals, function (row) { return dp2Str_(row[1]).trim() === 'Criterion'; });
  if (hr >= 0) for (var r = hr + 1; r < vals.length; r++) { if (!dp2Str_(vals[r][1])) break; out.criteria.push({ n: dp2Num_(vals[r][0]), name: dp2Str_(vals[r][1]), status: dp2Str_(vals[r][2]), detail: dp2Str_(vals[r][3]) }); }
  return out;
}
function dp2ReadShadowSwitch_() {
  var sh = dp2Sheet_(DP2.SRC_SHADOW, false);
  var out = [];
  if (!sh) return out;
  var vals = sh.getRange(1, 1, Math.min(12, sh.getLastRow()), Math.min(4, sh.getLastColumn())).getValues();
  for (var r = 0; r < vals.length; r++) {
    if (/^switch scan/.test(dp2Str_(vals[r][0]))) {
      try { var arr = JSON.parse(dp2Str_(vals[r][1])); for (var i = 0; i < arr.length; i++) out.push({ sell: arr[i].sell, buy: arr[i].buy, delta: dp2Num_(arr[i].delta_pct), hurdle: dp2Num_(arr[i].hurdle_pct) }); } catch (e) {}
    }
  }
  return out;
}

/* ========================================================= readers: IBKR tabs */
function dp2EnsureIbkrTabs_(ss) {
  var seeded = [];
  var specs = [
    [DP2.IB_ACCOUNT, ['key', 'value'], DP2_SEED_ACCOUNT_],
    [DP2.IB_ORDERS, ['symbol', 'side', 'type', 'qty', 'price', 'status', 'placed_utc'], DP2_SEED_ORDERS_],
    [DP2.IB_TRADES, ['symbol', 'name', 'ccy', 'exit_type', 'span', 'realized_usd'], DP2_SEED_TRADES_],
    [DP2.IB_NAV, ['date', 'twr_cum_since_inception'], DP2_SEED_NAV_]
  ];
  for (var i = 0; i < specs.length; i++) {
    var sh = ss.getSheetByName(specs[i][0]);
    var fresh = false;
    if (!sh) { sh = ss.insertSheet(specs[i][0]); fresh = true; }
    if (fresh || sh.getLastRow() === 0) {
      var data = [specs[i][1]].concat(specs[i][2]);
      sh.getRange(1, 1, data.length, specs[i][1].length).setValues(data);
      sh.getRange(1, 1, 1, specs[i][1].length).setFontWeight('bold').setBackground(DP2C.headerBg);
      sh.setFrozenRows(1);
      seeded.push(specs[i][0] + ' (seeded ' + specs[i][2].length + ' rows)');
    } else {
      seeded.push(specs[i][0] + ' (kept, ' + (sh.getLastRow() - 1) + ' rows)');
    }
  }
  return seeded;
}
function dp2IbkrAgeHours_(asOf) {
  // '2026-10-17 18:15 Riyadh' | '2026-10-17 18:15' | Date → hours since, or null
  if (asOf === null || asOf === undefined || asOf === '') return null;
  var ms = null;
  if (asOf instanceof Date) ms = asOf.getTime();
  else {
    var m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/.exec(dp2Str_(asOf).trim());
    if (m) ms = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4] - 3, +m[5]);   // Riyadh = UTC+3
  }
  return ms === null || isNaN(ms) ? null : Math.max(0, (Date.now() - ms) / 3600000);
}
function dp2ReadIbkr_() {
  var out = { account: {}, orders: [], trades: [], nav: [], tabs: {}, ordersKnown: false, ageH: null, stale: false };
  var a = dp2Sheet_(DP2.IB_ACCOUNT, false);
  out.tabs.account = !!(a && a.getLastRow() > 1);
  out.tabs.orders = !!(dp2Sheet_(DP2.IB_ORDERS, false) && dp2Sheet_(DP2.IB_ORDERS, false).getLastRow() > 1);
  out.tabs.trades = !!(dp2Sheet_(DP2.IB_TRADES, false) && dp2Sheet_(DP2.IB_TRADES, false).getLastRow() > 1);
  out.tabs.nav = !!(dp2Sheet_(DP2.IB_NAV, false) && dp2Sheet_(DP2.IB_NAV, false).getLastRow() > 1);
  if (a && a.getLastRow() > 1) { var av = a.getRange(2, 1, a.getLastRow() - 1, 2).getValues(); for (var i = 0; i < av.length; i++) { var k = dp2Str_(av[i][0]).trim(); if (k) out.account[k] = av[i][1]; } }
  var o = dp2Sheet_(DP2.IB_ORDERS, false);
  if (o && o.getLastRow() > 1) { var ov = o.getRange(2, 1, o.getLastRow() - 1, 7).getValues(); for (i = 0; i < ov.length; i++) { if (!dp2Str_(ov[i][0])) continue; out.orders.push({ symbol: dp2Str_(ov[i][0]).trim().toUpperCase(), side: dp2Str_(ov[i][1]), type: dp2Str_(ov[i][2]).toUpperCase(), qty: dp2Num_(ov[i][3]), price: dp2Num_(ov[i][4]), status: dp2Str_(ov[i][5]).toUpperCase(), placed: dp2Str_(ov[i][6]) }); } }
  var t = dp2Sheet_(DP2.IB_TRADES, false);
  if (t && t.getLastRow() > 1) { var tv = t.getRange(2, 1, t.getLastRow() - 1, 6).getValues(); for (i = 0; i < tv.length; i++) { if (!dp2Str_(tv[i][0])) continue; out.trades.push({ symbol: dp2Str_(tv[i][0]), name: dp2Str_(tv[i][1]), ccy: dp2Str_(tv[i][2]), exit: dp2Str_(tv[i][3]), span: dp2Str_(tv[i][4]), pnl: dp2Num_(tv[i][5]) }); } }
  var n = dp2Sheet_(DP2.IB_NAV, false);
  if (n && n.getLastRow() > 1) { var nv = n.getRange(2, 1, n.getLastRow() - 1, 2).getValues(); for (i = 0; i < nv.length; i++) { var d = nv[i][0]; var c = dp2Num_(nv[i][1]); if (d && c !== null) out.nav.push({ date: (d instanceof Date) ? Utilities.formatDate(d, DP2.TZ, 'yyyy-MM-dd') : dp2Str_(d), cps: c }); } }
  try { dp2IbkrOverlayCapture_(out); } catch (eCap) { out.captureNote = 'capture not read (' + dp2Str_(eCap && eCap.message || eCap).slice(0, 80) + ')'; }   // v1.2.22
  out.ageH = dp2IbkrAgeHours_(out.account['as_of']);
  out.stale = out.ageH === null ? out.tabs.account : out.ageH > DP2.IBKR_TABS_STALE_H;
  // v1.2.5 (review finding): broker orders are KNOWN only from a dated, fresh snapshot. An absent/empty orders tab, a
  // missing _IBKR_Account (no as_of) or a snapshot older than DP2.IBKR_TABS_STALE_H = unknown — cached NEW/SUBMITTED
  // rows are never read as live protection.
  out.ordersKnown = !!(out.tabs.orders && out.tabs.account && out.ageH !== null && !out.stale);
  out.unknownWhy = !out.tabs.orders ? '_IBKR_Orders tab missing or empty' : (!out.tabs.account || out.ageH === null ? '_IBKR_Account has no as_of for the orders snapshot' : (out.stale ? 'snapshot ' + dp2Fmt_(out.ageH, 0) + ' h old (limit ' + DP2.IBKR_TABS_STALE_H + ' h) — order statuses unverified' : ''));
  return out;
}
/* v1.2.22: the daily READ-ONLY IBKR capture (the scheduled task writes 'TFB_IBKR_Capture_*.json', schema
 * tfb.ibkr-capture/v1, to Drive; read through 10_My_Portfolio pdIbkrLatestCapture_, the same reader Portfolio_Decision
 * uses) supersedes _IBKR_Account / _IBKR_Orders when it is newer: cash (settled), NAV, stock value and unrealized from
 * the USD (else BASE) balance, gross leverage = stock value / NAV, and the WORKING orders as captured. Trades, the NAV
 * history and the TWR stay on the tabs. Read-only: nothing is written; without the reader or a capture the tabs are
 * used exactly as before. */
function dp2IbkrOverlayCapture_(out) {
  if (typeof pdIbkrLatestCapture_ !== 'function') { out.captureNote = 'capture reader not installed (10_My_Portfolio v1.5.12+)'; return false; }
  var cap = pdIbkrLatestCapture_(new Date().getTime());
  if (!cap || !cap.ok || !cap.json) { out.captureNote = 'no capture (' + dp2Str_(cap && cap.reason || 'unknown') + ')'; return false; }
  var j = cap.json;
  if (dp2Str_(j.schema) !== 'tfb.ibkr-capture/v1') { out.captureNote = 'capture schema not recognised'; return false; }
  var capMs = Date.parse(dp2Str_(j.captured_at));
  if (isNaN(capMs)) { out.captureNote = 'capture time unreadable'; return false; }
  var tabAge = dp2IbkrAgeHours_(out.account['as_of']);
  var tabMs = tabAge === null ? null : new Date().getTime() - tabAge * 3600000;
  if (tabMs !== null && tabMs >= capMs) { out.captureNote = 'the _IBKR_* tabs are newer than the capture'; return false; }
  var bals = (j.balances && j.balances.balances) || [], usd = null, base = null, i;
  for (i = 0; i < bals.length; i++) { var cc = dp2Str_(bals[i] && bals[i].currency).toUpperCase(); if (cc === 'USD') usd = bals[i]; else if (cc === 'BASE') base = bals[i]; }
  var b = usd || base;
  if (!b) { out.captureNote = 'capture has no USD balance'; return false; }
  var acct = {}, k;
  for (k in out.account) if (out.account.hasOwnProperty(k)) acct[k] = out.account[k];
  var nav = dp2Num_(b.net_liquidation_value), stk = dp2Num_(b.stock_market_value), settled = dp2Num_(b.settled_cash), cashB = dp2Num_(b.cash_balance);
  if (settled === null && cashB === null) { out.captureNote = 'capture balance has no cash'; return false; }
  acct['as_of'] = Utilities.formatDate(new Date(capMs), DP2.TZ, 'yyyy-MM-dd HH:mm') + ' Riyadh';
  acct['source'] = 'IBKR read-only capture ' + dp2Str_(cap.name);
  acct['cash_usd'] = settled !== null ? settled : cashB;
  if (nav !== null) acct['nav_usd'] = nav;
  if (stk !== null) acct['stock_mv_usd'] = stk;
  if (dp2Num_(b.unrealized_pnl) !== null) acct['unrealized_usd'] = dp2Num_(b.unrealized_pnl);
  if (nav && stk !== null) acct['leverage'] = Math.round(stk / nav * 100) / 100;
  var ords = (j.orders && j.orders.orders) || [], mapped = [];
  for (i = 0; i < ords.length; i++) {
    var o = ords[i] || {}, pdsc = dp2Str_(o.primary_description), sdsc = dp2Str_(o.secondary_description);
    var mo = /^(buy|sell)\s+[\d,\.]+\s+(\S+)/i.exec(pdsc);
    var typ = dp2Str_(o.order_type).toUpperCase().replace(/\s+/g, '_');
    if (typ === 'STP') typ = 'STOP'; else if (typ === 'LMT') typ = 'LIMIT'; else if (typ === 'STP_LMT') typ = 'STOP_LIMIT';
    var px = null;
    if (/STOP/.test(typ)) { var mstp = /STP\s+(?:LMT\s+)?([\d,]*\.?\d+)/i.exec(sdsc); px = mstp ? dp2Num_(mstp[1].replace(/,/g, '')) : null; }
    else px = dp2Num_(o.limit_price);
    mapped.push({ symbol: mo ? mo[2].toUpperCase() : '', side: dp2Str_(o.side).toUpperCase() || (mo ? mo[1].toUpperCase() : ''), type: typ, qty: dp2Num_(o.total_shares_qty), price: px, status: dp2Str_(o.order_status).toUpperCase(), placed: dp2Str_(o.order_time) });
  }
  out.account = acct; out.orders = mapped;
  out.tabs.account = true; out.tabs.orders = true;
  out.capture = { name: dp2Str_(cap.name), ms: capMs, positions: ((j.positions && j.positions.positions) || []).length, orders: mapped.length };
  out.captureNote = 'IBKR read-only capture ' + Utilities.formatDate(new Date(capMs), DP2.TZ, 'yyyy-MM-dd HH:mm') + ' Riyadh';
  return true;
}
/* v1.2.2 (Codex P1): broker TP / stop levels come ONLY from orders in an active status. Historical REPLACED,
   CANCELLED, FILLED or unknown-status rows never supply a level, so a replaced stop can no longer make an
   unprotected lot look protected; their last prices are kept separately (lastTp / lastStop) for the wording. */
function dp2OrderActive_(status) {
  var st = dp2Str_(status).toUpperCase();
  if (/CANCEL|REPLACED|INACTIVE|REJECT|EXPIRED/.test(st)) return false;
  if (st === 'FILLED') return false;
  return /NEW|SUBMITTED|PRESUBMITTED|PENDING_SUBMIT|WORKING|PARTIALLY/.test(st);
}
function dp2BracketFor_(orders, symbolKey) {
  var tp = null, stop = null, lastTp = null, lastStop = null, statuses = [], placed = '', active = 0, replaced = 0, stopQty = 0, tpQty = 0, stops = [];
  for (var i = 0; i < orders.length; i++) {
    var o = orders[i];
    if (o.symbol !== symbolKey || o.side.toUpperCase() !== 'SELL') continue;
    statuses.push(o.status);
    var isTp = (o.type === 'LIMIT' || o.type === 'LMT'), isStop = (o.type === 'STOP' || o.type === 'STP' || o.type === 'STOP_LIMIT' || o.type === 'STP LMT');
    if (isTp) lastTp = o.price;
    if (isStop) lastStop = o.price;
    if (/REPLACED/.test(dp2Str_(o.status).toUpperCase())) replaced++;
    if (!dp2OrderActive_(o.status)) continue;
    active++;
    if (isTp) { tp = o.price; tpQty += (o.qty || 0); }
    if (isStop) { stopQty += (o.qty || 0); stops.push({ price: o.price, qty: o.qty || 0 }); if (stop === null || (o.price !== null && o.price < stop)) stop = o.price; }   // v1.2.11/13: quantities per level; `stop` = the LOWEST active level (weakest protection)
    placed = o.placed || placed;
  }
  var state;
  if (statuses.length === 0) state = 'none';
  else if (tp !== null && stop !== null) state = 'working';
  else if (tp !== null || stop !== null) state = 'partial';
  else if (replaced > 0) state = 'replaced';
  else state = 'inactive';
  return { tp: tp, stop: stop, lastTp: lastTp, lastStop: lastStop, state: state, placed: placed, n: statuses.length, active: active, statuses: statuses, stopQty: stopQty, tpQty: tpQty, stops: stops };
}

/* ========================================================= dynamic exits (v1.1.0): price history */
function dp2YahooKey_(symbol) { return dp2Str_(symbol).trim().toUpperCase().replace(/\.US$/, ''); }
function dp2IsUs_(symbol) { var s = dp2Str_(symbol).trim().toUpperCase(); return /\.US$/.test(s) || !/\./.test(s); }
function dp2GfKey_(symbol) {
  var s = dp2Str_(symbol).trim().toUpperCase();
  var m = /^(.+)\.([A-Z]{1,3})$/.exec(s);
  if (!m) return s;
  if (m[2] === 'US') return m[1];
  var ex = DP2_GF_EXCH_[m[2]];
  return ex ? ex + ':' + m[1] : m[1];
}
function dp2SparkFormula_(symbol) {
  return '=IFERROR(SPARKLINE(GOOGLEFINANCE("' + dp2GfKey_(symbol) + '","price",TODAY()-91,TODAY(),"DAILY"),{"charttype","line";"color","' + DP2C.blue + '";"linewidth",1.5}),"n/a")';
}
function dp2DateStr_(v) {
  if (v === null || v === undefined || v === '') return '';
  if (v instanceof Date) return isNaN(v.getTime()) ? '' : Utilities.formatDate(v, DP2.TZ, 'yyyy-MM-dd');
  var s = dp2Str_(v).trim();
  var m = /^(\d{4}-\d{2}-\d{2})/.exec(s);
  if (m) return m[1];
  var d = new Date(s);
  return isNaN(d.getTime()) ? '' : Utilities.formatDate(d, DP2.TZ, 'yyyy-MM-dd');
}
function dp2DedupeBars_(bars) {
  var out = [], seen = {};
  for (var i = bars.length - 1; i >= 0; i--) { if (seen[bars[i].date]) continue; seen[bars[i].date] = true; out.unshift(bars[i]); }
  return out;
}
function dp2Prop_(name) { try { return PropertiesService.getScriptProperties().getProperty(name) || ''; } catch (e) { return ''; } }
/* Optional TFB-native route: DP2_HISTORY_URL template → JSON {bars:[{date,o,h,l,c}], last} or [[date,o,h,l,c],…] */
function dp2FetchBackend_(symbol, range) {
  var tpl = dp2Prop_(DP2X.PROP_HISTORY_URL);
  if (!tpl) return null;
  var url = tpl.replace('{symbol}', encodeURIComponent(symbol)).replace('{range}', encodeURIComponent(range || DP2X.RANGE));
  var opts = { muteHttpExceptions: true, followRedirects: true, headers: {} };
  var key = dp2Prop_(DP2X.PROP_HISTORY_KEY); if (key) opts.headers['X-API-Key'] = key;
  var resp = UrlFetchApp.fetch(url, opts);
  if (resp.getResponseCode() !== 200) throw new Error('backend HTTP ' + resp.getResponseCode());
  var j = JSON.parse(resp.getContentText());
  var arr = j && j.bars ? j.bars : j;
  var bars = [];
  for (var i = 0; i < (arr || []).length; i++) {
    var b = arr[i];
    var row = Array.isArray(b) ? { date: dp2DateStr_(b[0]), o: dp2Num_(b[1]), h: dp2Num_(b[2]), l: dp2Num_(b[3]), c: dp2Num_(b[4]) }
                               : { date: dp2DateStr_(b.date), o: dp2Num_(b.o !== undefined ? b.o : b.open), h: dp2Num_(b.h !== undefined ? b.h : b.high), l: dp2Num_(b.l !== undefined ? b.l : b.low), c: dp2Num_(b.c !== undefined ? b.c : b.close) };
    if (!row.date || row.c === null || row.h === null || row.l === null) continue;
    if (row.o === null) row.o = row.c;
    bars.push(row);
  }
  if (!bars.length) throw new Error('backend: no bars');
  return { bars: bars, last: j && j.last !== undefined ? dp2Num_(j.last) : null, ccy: (j && j.currency) || '', marketState: '', source: 'backend' };
}
/* v1.2.11: fetch range / bars-to-keep from the oldest entry date the caller needs (plus the ATR warm-up). */
function dp2RangeFor_(sinceDate) {
  var d = dp2DateStr_(sinceDate);
  if (!d) return { range: DP2X.RANGE, keep: DP2X.KEEP_BARS, days: null };
  var days = Math.round((Date.now() - new Date(d + 'T00:00:00Z').getTime()) / 86400000) + Math.round(DP2X.ATR_WARMUP_BARS * 1.5);
  if (days <= 150) return { range: '6mo', keep: DP2X.KEEP_BARS, days: days };
  if (days <= 330) return { range: '1y', keep: 270, days: days };
  if (days <= 700) return { range: '2y', keep: 530, days: days };
  return { range: '5y', keep: DP2X.KEEP_BARS_MAX, days: days };
}
function dp2FetchYahoo_(key, range) {
  var url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + encodeURIComponent(key) + '?interval=1d&range=' + (range || DP2X.RANGE) + '&includePrePost=false';
  var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true, followRedirects: true, headers: { 'User-Agent': 'Mozilla/5.0 (compatible; TFB-DecisionPages/' + DP2_VERSION + ')', 'Accept': 'application/json' } });
  var code = resp.getResponseCode();
  if (code !== 200) throw new Error('yahoo HTTP ' + code);
  var j = JSON.parse(resp.getContentText());
  var r = j && j.chart && j.chart.result && j.chart.result[0];
  if (!r || !r.timestamp) throw new Error('yahoo: ' + ((j && j.chart && j.chart.error && j.chart.error.description) || 'no result'));
  var q = (r.indicators && r.indicators.quote && r.indicators.quote[0]) || {};
  var tz = (r.meta && r.meta.exchangeTimezoneName) || 'America/New_York';
  var bars = [];
  for (var i = 0; i < r.timestamp.length; i++) {
    var c = q.close ? q.close[i] : null, h = q.high ? q.high[i] : null, l = q.low ? q.low[i] : null, o = q.open ? q.open[i] : null;
    if (c === null || c === undefined || h === null || h === undefined || l === null || l === undefined) continue;
    bars.push({ date: Utilities.formatDate(new Date(r.timestamp[i] * 1000), tz, 'yyyy-MM-dd'), o: (o === null || o === undefined) ? c : o, h: h, l: l, c: c });
  }
  if (!bars.length) throw new Error('yahoo: empty series');
  return { bars: bars, last: r.meta ? dp2Num_(r.meta.regularMarketPrice) : null, ccy: (r.meta && r.meta.currency) || '', marketState: (r.meta && r.meta.marketState) || '', source: 'yahoo' };
}
function dp2FetchStooq_(key, keep) {
  var url = 'https://stooq.com/q/d/l/?s=' + encodeURIComponent(key.toLowerCase() + '.us') + '&i=d';
  var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true, followRedirects: true });
  if (resp.getResponseCode() !== 200) throw new Error('stooq HTTP ' + resp.getResponseCode());
  var lines = resp.getContentText().split(/\r?\n/);
  if (!/^Date,Open,High,Low,Close/.test(lines[0] || '')) throw new Error('stooq: ' + (lines[0] || 'empty').slice(0, 60));
  var bars = [];
  for (var i = 1; i < lines.length; i++) {
    var p = lines[i].split(',');
    if (p.length < 5) continue;
    var o = parseFloat(p[1]), h = parseFloat(p[2]), l = parseFloat(p[3]), c = parseFloat(p[4]);
    if (isNaN(c) || isNaN(h) || isNaN(l)) continue;
    bars.push({ date: p[0], o: isNaN(o) ? c : o, h: h, l: l, c: c });
  }
  if (!bars.length) throw new Error('stooq: empty series');
  bars = bars.slice(-(keep || DP2X.KEEP_BARS));
  return { bars: bars, last: bars[bars.length - 1].c, ccy: 'USD', marketState: '', source: 'stooq' };
}
function dp2BarsCacheRead_() {
  var sh = dp2Sheet_(DP2X.SHEET_BARS, false);
  var out = {};
  if (!sh || sh.getLastRow() < 2) return out;
  var tz = dp2Ss_().getSpreadsheetTimeZone() || DP2.TZ;
  var vals = sh.getRange(2, 1, sh.getLastRow() - 1, 9).getValues();
  for (var i = 0; i < vals.length; i++) {
    var sym = dp2Str_(vals[i][0]).trim(); if (!sym) continue;
    var e = out[sym] || (out[sym] = { bars: [], source: '', fetched: '', key: '' });
    e.key = dp2Str_(vals[i][1]); e.source = dp2Str_(vals[i][7]); e.fetched = dp2Str_(vals[i][8]);
    var d = vals[i][2];
    var c = dp2Num_(vals[i][6]), h = dp2Num_(vals[i][4]), l = dp2Num_(vals[i][5]);
    if (c === null || h === null || l === null) continue;
    e.bars.push({ date: (d instanceof Date) ? Utilities.formatDate(d, tz, 'yyyy-MM-dd') : dp2Str_(d), o: dp2Num_(vals[i][3]) !== null ? dp2Num_(vals[i][3]) : c, h: h, l: l, c: c });
  }
  for (var s in out) if (out.hasOwnProperty(s)) out[s].bars = dp2DedupeBars_(out[s].bars.sort(function (a, b) { return a.date < b.date ? -1 : (a.date > b.date ? 1 : 0); }));
  return out;
}
function dp2BarsCacheWrite_(map) {
  var lock = dp2Lock_(10000);   // v1.2.9: never two writers on the cache sheet
  if (!lock) return false;
  try { return dp2BarsCacheWriteLocked_(map); } finally { dp2Unlock_(lock); }
}
function dp2BarsCacheWriteLocked_(map) {
  var sh = dp2Sheet_(DP2X.SHEET_BARS, true);
  var rows = [];
  var syms = Object.keys(map).sort();
  for (var i = 0; i < syms.length; i++) {
    var e = map[syms[i]]; if (!e || !e.bars || !e.bars.length) continue;
    var bars = e.bars.slice(-Math.min(DP2X.KEEP_BARS_MAX, Math.max(DP2X.KEEP_BARS, e.keep || 0)));
    for (var j = 0; j < bars.length; j++) rows.push([syms[i], e.key || '', bars[j].date, bars[j].o, bars[j].h, bars[j].l, bars[j].c, e.source || '', e.fetched || '']);
  }
  sh.clear();
  var hdr = ['Symbol', 'Key', 'Date', 'Open', 'High', 'Low', 'Close', 'Source', 'Fetched (Riyadh)'];
  if (sh.getMaxRows() < rows.length + 2) sh.insertRowsAfter(sh.getMaxRows(), rows.length + 2 - sh.getMaxRows());
  sh.getRange(1, 1, 1, hdr.length).setValues([hdr]).setFontWeight('bold').setBackground(DP2C.headerBg);
  if (rows.length) {
    sh.getRange(2, 3, rows.length, 1).setNumberFormat('@');
    sh.getRange(2, 9, rows.length, 1).setNumberFormat('@');
    sh.getRange(2, 1, rows.length, hdr.length).setValues(rows);
  }
  sh.setFrozenRows(1);
  try { sh.hideSheet(); } catch (e) {}
  return rows.length;
}
/* Loads daily bars for a list of TFB symbols: backend route (if configured) → Yahoo → Stooq (US) → cached rows (stale). */
function dp2LoadBars_(symbols, sinceDate) {
  var cache = null, out = {}, changed = false;
  var rg = dp2RangeFor_(sinceDate);   // v1.2.11: the history must reach the oldest entry the caller replays
  for (var i = 0; i < symbols.length; i++) {
    var sym = dp2Str_(symbols[i]).trim();
    if (!sym || out[sym]) continue;
    if (DP2X_MEMO_[sym] && (DP2X_MEMO_[sym].keep || DP2X.KEEP_BARS) >= rg.keep) { out[sym] = DP2X_MEMO_[sym]; continue; }
    var key = dp2YahooKey_(sym), res = null, err = [];
    try { res = dp2FetchBackend_(sym, rg.range); } catch (e0) { err.push(String(e0 && e0.message || e0)); }
    if (!res) { try { res = dp2FetchYahoo_(key, rg.range); } catch (e1) { err.push(String(e1 && e1.message || e1)); } }
    if (!res && dp2IsUs_(sym)) { try { res = dp2FetchStooq_(key, rg.keep); } catch (e2) { err.push(String(e2 && e2.message || e2)); } }
    if (res && res.bars && res.bars.length) {
      res.bars = dp2DedupeBars_(res.bars).slice(-rg.keep);
      res.key = key; res.fetched = dp2Today_(); res.stale = false; res.error = ''; res.range = rg.range; res.keep = rg.keep;
      changed = true;
    } else {
      if (cache === null) cache = dp2BarsCacheRead_();
      var c = cache[sym];
      res = (c && c.bars.length) ? { bars: c.bars, last: c.bars[c.bars.length - 1].c, ccy: '', marketState: '', source: (c.source || 'cache') + ' (cached)', key: c.key, fetched: c.fetched, stale: true, error: err.join(' · ') }
                                 : { bars: [], last: null, ccy: '', marketState: '', source: '', key: key, fetched: '', stale: true, error: err.join(' · ') || 'no data' };
    }
    DP2X_MEMO_[sym] = res;
    out[sym] = res;
    if (i < symbols.length - 1 && DP2X.FETCH_SLEEP_MS) Utilities.sleep(DP2X.FETCH_SLEEP_MS);
  }
  if (changed) {
    if (cache === null) cache = dp2BarsCacheRead_();
    for (var s in out) if (out.hasOwnProperty(s) && !out[s].stale) cache[s] = out[s];
    try { dp2BarsCacheWrite_(cache); } catch (e3) {}
  }
  return out;
}

/* ========================================================= dynamic exits (v1.1.0): the rule */
function dp2Atr_(bars, n) {
  var out = new Array(bars.length), tr = new Array(bars.length);
  for (var i = 0; i < bars.length; i++) {
    var b = bars[i];
    if (i === 0) tr[i] = b.h - b.l;
    else { var pc = bars[i - 1].c; tr[i] = Math.max(b.h - b.l, Math.abs(b.h - pc), Math.abs(b.l - pc)); }
    out[i] = null;
  }
  if (bars.length < n) return out;
  var s = 0; for (i = 0; i < n; i++) s += tr[i]; s = s / n; out[n - 1] = s;
  for (i = n; i < bars.length; i++) { s = (s * (n - 1) + tr[i]) / n; out[i] = s; }
  return out;
}
function dp2Sma_(bars, i, n) { if (i + 1 < n) return null; var s = 0; for (var k = i - n + 1; k <= i; k++) s += bars[k].c; return s / n; }
/* Replays the rule from the entry bar to the last bar. entryDate 'yyyy-MM-dd' (or '' → last bar). lastOverride = live price if known. */
function dp2ExitCompute_(bars, entry, entryDate, lastOverride) {
  if (!bars || bars.length < DP2X.ATR_N + 1 || !entry) return null;
  // v1.2.11 (review finding): the replay is only valid when the history reaches the entry — a series that starts
  // after the entry would rebuild the trade from the wrong bar (highs, episodes and the entry ATR all lost).
  if (entryDate && bars[0].date > entryDate) return null;
  var atr = dp2Atr_(bars, DP2X.ATR_N);
  var e = -1, i;
  if (entryDate) { for (i = 0; i < bars.length; i++) { if (bars[i].date >= entryDate) { e = i; break; } } }
  if (e < 0) e = bars.length - 1;
  var a0 = atr[e], atrApprox = false;
  if (a0 === null) { atrApprox = true; for (i = 0; i < atr.length; i++) { if (atr[i] !== null) { a0 = atr[i]; break; } } }
  if (a0 === null || !(a0 > 0)) return null;
  var s0 = entry - DP2X.ATR_MULT_STOP * a0, R = entry - s0;
  var stop = s0, hi = bars[e].c, a = a0, gainR = 0, trailMult = DP2X.ATR_MULT_STOP, beLocked = false, mom = false, sma = null;
  var firstHit = null, firstMom = null, hits = [], moms = [], prevHit = false, prevMom = false;
  for (i = e; i < bars.length; i++) {
    a = atr[i] !== null ? atr[i] : a0;
    // v1.2.7: every trigger EPISODE is recorded (a bar where the condition holds after a bar where it did not), so an
    // exit that re-arms after an acknowledged one can latch again; firstHit / firstMom stay the first episodes.
    var hitNow = (i > e && bars[i].l <= stop);
    if (hitNow && !prevHit) hits.push({ date: bars[i].date, stop: stop });
    prevHit = hitNow;
    if (bars[i].c > hi) hi = bars[i].c;
    gainR = R > 0 ? (hi - entry) / R : 0;
    trailMult = gainR >= DP2X.TIGHTEN_R ? DP2X.TRAIL_MULT_TIGHT : DP2X.ATR_MULT_STOP;
    stop = Math.max(stop, s0, hi - trailMult * a);
    beLocked = gainR >= DP2X.PROFIT_LOCK_R;
    if (beLocked) stop = Math.max(stop, entry + DP2X.BREAKEVEN_ATR * a);
    sma = dp2Sma_(bars, i, DP2X.MOMENTUM_SMA);
    mom = bars[i].c > entry && sma !== null && bars[i].c < sma && (hi - bars[i].c) >= DP2X.MOM_GIVEBACK_ATR * a && gainR >= DP2X.MOM_MIN_R;
    if (mom && !prevMom) moms.push({ date: bars[i].date, price: bars[i].c });
    prevMom = mom;
  }
  firstHit = hits.length ? hits[0] : null; firstMom = moms.length ? moms[0] : null;
  var hitNow = prevHit, momNow = prevMom;   // v1.2.15: is the last bar inside the latest episode?
  var lastBar = bars[bars.length - 1];
  var last = (lastOverride !== null && lastOverride !== undefined && lastOverride > 0) ? lastOverride : lastBar.c;
  return { entry: entry, entryBar: bars[e].date, atrEntry: a0, s0: s0, R: R, hi: hi, gainR: gainR, atr: a, sma: sma, stop: stop, trailMult: trailMult,
           beLocked: beLocked, momentum: mom, last: last, lastDate: lastBar.date, barsSinceEntry: bars.length - e, firstHit: firstHit, firstMom: firstMom,
           hits: hits, moms: moms, hitNow: hitNow, momNow: momNow, atrApprox: atrApprox, barsBeforeEntry: e, lastR: R > 0 ? (last - entry) / R : null };
}
/* Signal for a held lot. x = dp2ExitCompute_ result (or null), br = { tp, stop } working broker levels (null when none), fallbackLast.
 * alert = immediate e-mail (SELL NOW); advisory = at most one e-mail per cooldown (stop near / raise / missing broker stop). */
function dp2ExitSignal_(x, br, fallbackLast, ackDate) {
  var out = { code: 'HOLD', kind: 'navy', text: 'HOLD — trailing', alert: false, advisory: false, dist: null, last: x ? x.last : fallbackLast, latched: null, latchedAck: false };
  if (!x) { out.code = 'ENGINE_NA'; out.kind = 'grey'; out.text = 'HOLD — exit engine n/a'; return out; }
  var last = x.last;
  out.dist = last ? (last / x.stop - 1) * 100 : null;
  var tpDist = (br && br.tp && last) ? (br.tp / last - 1) * 100 : null;
  if (last !== null && last <= x.stop) { out.code = 'STOP_HIT'; out.kind = 'red'; out.text = 'SELL NOW — stop ' + dp2Fmt_(x.stop, 2) + ' hit'; out.alert = true; }
  else if (x.momentum) { out.code = 'SELL_NOW'; out.kind = 'red'; out.text = 'SELL NOW — protect profit'; out.alert = true; }
  else if (out.dist !== null && out.dist <= DP2X.NEAR_STOP_PCT) { out.code = 'STOP_NEAR'; out.kind = 'amber'; out.text = 'STOP NEAR — ' + dp2PctPlain_(out.dist, 1) + ' above ' + dp2Fmt_(x.stop, 2); out.advisory = true; }
  // v1.2.4 (review finding): a missing or low broker stop outranks the take-profit watch — an unprotected lot near its
  // target is still unprotected; the TP proximity is carried alongside (out.tpWatch, text suffix).
  // v1.2.11 (review finding): an active stop that covers only part of the lot is not full protection
  else if (br && br.stop && br.shares && br.stopQty !== undefined && br.stopQty !== null && br.stopQty > 0 && br.stopQty < br.shares - 1e-9) { out.code = 'STOP_PARTIAL'; out.kind = 'amber'; out.advisory = true; out.uncovered = br.shares - br.stopQty; out.text = 'STOP COVERS ' + dp2Fmt_(br.stopQty) + ' of ' + dp2Fmt_(br.shares) + ' sh at ' + dp2Fmt_(br.stop, 2) + ' — place a stop for ' + dp2Fmt_(out.uncovered) + ' sh at ' + dp2Fmt_(x.stop, 2) + (x.stop >= br.stop * (1 + DP2X.RAISE_MIN_PCT / 100) ? ' · raise the existing one to ' + dp2Fmt_(x.stop, 2) : ''); }
  else if (br && br.stop && x.stop >= br.stop * (1 + DP2X.RAISE_MIN_PCT / 100)) {
    out.code = 'RAISE_STOP'; out.kind = 'amber'; out.advisory = true;
    // v1.2.13: with several active stop levels the raise applies to the shares resting below the rule
    var lowQty = 0, lvls = 0; if (br.stops && br.stops.length > 1) { for (var si = 0; si < br.stops.length; si++) { lvls++; if (br.stops[si].price !== null && x.stop >= br.stops[si].price * (1 + DP2X.RAISE_MIN_PCT / 100)) lowQty += br.stops[si].qty || 0; } }
    out.text = 'RAISE STOP ' + dp2Fmt_(br.stop, 2) + ' → ' + dp2Fmt_(x.stop, 2) + (lvls > 1 ? ' (' + dp2Fmt_(lowQty) + ' sh resting below the rule across ' + lvls + ' stop levels)' : '');
  }
  else if (br && br.known === false) { out.code = 'HOLD_BROKER_UNKNOWN'; out.kind = 'navy'; out.text = 'HOLD — broker orders unknown · rule stop ' + dp2Fmt_(x.stop, 2); }
  else if (!br || !br.stop) { out.code = 'NO_BROKER_STOP'; out.kind = 'amber'; out.lotOfMany = !!(br && br.lotOfMany); out.lotShares = br ? br.shares : null; out.text = 'NO WORKING STOP — place ' + dp2Fmt_(x.stop, 2) + (br && br.lotOfMany && br.shares ? ' for this lot\'s ' + dp2Fmt_(br.shares) + ' sh' : ''); out.advisory = true; }
  else if (tpDist !== null && tpDist <= DP2X.TP_WATCH_PCT) { out.code = 'TP_WATCH'; out.kind = 'amber'; out.text = 'TP WATCH — broker TP ' + dp2Fmt_(br.tp, 2) + ' ' + dp2Pct_(tpDist, 1); }
  else if (x.beLocked) { out.code = 'HOLD_BE'; out.kind = 'green'; out.text = 'HOLD — breakeven locked'; }
  out.tpDist = tpDist;
  out.tpWatch = (tpDist !== null && tpDist <= DP2X.TP_WATCH_PCT);
  if (out.tpWatch && out.code !== 'TP_WATCH' && (out.code === 'RAISE_STOP' || out.code === 'NO_BROKER_STOP')) out.text += ' · TP ' + dp2Fmt_(br.tp, 2) + ' ' + dp2Pct_(tpDist, 1) + ' away';
  // v1.2.5 (review finding): a rule exit recorded by the replay (first stop touch / first profit-protection close since
  // entry) stays LATCHED while the lot is open — a recovery before the next build never silently reverts it to HOLD.
  // The owner can acknowledge a dated exit (menu: Acknowledge rule exit) to hold by decision; it is then shown, not alerted.
  var episodes = [], ei;
  var hitsArr = x.hits || (x.firstHit ? [x.firstHit] : []), momsArr = x.moms || (x.firstMom ? [x.firstMom] : []);
  for (ei = 0; ei < hitsArr.length; ei++) episodes.push({ date: hitsArr[ei].date, kind: 'stop', price: hitsArr[ei].stop });
  for (ei = 0; ei < momsArr.length; ei++) episodes.push({ date: momsArr[ei].date, kind: 'momentum', price: momsArr[ei].price });
  episodes.sort(function (p1, p2) { return p1.date < p2.date ? -1 : (p1.date > p2.date ? 1 : (p1.kind < p2.kind ? -1 : 1)); });
  if (episodes.length && out.code !== 'STOP_HIT' && out.code !== 'SELL_NOW') {
    // v1.2.7: the acknowledgement covers every episode up to its date; the earliest episode AFTER it latches again
    var ack = ackDate ? String(ackDate) : '';
    var trig = null, covered = null;
    for (ei = 0; ei < episodes.length; ei++) { if (ack && episodes[ei].date <= ack) covered = episodes[ei]; else if (!trig) trig = episodes[ei]; }
    var fmtEp = function (ep) { return ep.date + ' (' + (ep.kind === 'momentum' ? 'profit protection, close ' + dp2Fmt_(ep.price, 2) : 'stop ' + dp2Fmt_(ep.price, 2) + ' touched') + ')'; };
    if (trig) {
      out.latched = { date: trig.date, kind: trig.kind, price: trig.price, text: fmtEp(trig), episodes: episodes.length };
      out.code = 'EXIT_LATCHED'; out.kind = 'red'; out.alert = true; out.advisory = false;
      out.text = 'SELL — rule exit ' + fmtEp(trig) + ' not acted' + (covered ? ' (after the acknowledged ' + covered.date + ' exit)' : '') + ' · last ' + dp2Fmt_(last, 2);
    } else if (covered) {
      out.latched = { date: covered.date, kind: covered.kind, price: covered.price, text: fmtEp(covered), episodes: episodes.length };
      out.latchedAck = true; out.text += ' · rule exit ' + covered.date + ' acknowledged';
    }
  }
  return out;
}
/* v1.2.11: one line explaining why the exit engine is unavailable for a lot. */
function dp2EngineNaWhy_(xb, entryDate) {
  if (!xb || !xb.bars || !xb.bars.length) return 'no price history' + (xb && xb.error ? ' — ' + dp2Str_(xb.error).slice(0, 90) : '');
  if (entryDate && xb.bars[0].date > entryDate) return 'exit engine n/a — history starts ' + xb.bars[0].date + ', after the entry ' + entryDate + ' (fetched ' + (xb.range || DP2X.RANGE) + '); widen the range or check the entry date';
  if (xb.bars.length < DP2X.ATR_N + 1) return 'exit engine n/a — only ' + xb.bars.length + ' bars';
  return 'exit engine n/a';
}
function dp2RFmt_(r) { return (r === null || r === undefined || isNaN(r)) ? '—' : ((r >= 0 ? '+' : '−') + Math.abs(r).toFixed(1) + 'R'); }

/* ========================================================= dynamic exits (v1.1.0): state + alerts */
var DP2X_STATE_HDR_ = ['Symbol', 'Name', 'Qty', 'Entry', 'Entry date', 'Initial stop', 'R', 'Highest close', 'ATR14', 'SMA10', 'Last', 'Dyn stop', 'Signal', 'Signal text', 'Signal since', 'Last alert signal', 'Last alert at (ms)', 'Last raise alert (ms)', 'Bars as of', 'Source', 'Updated (Riyadh)', 'Latched exit', 'Lot', 'Last alert episode'];   // v1.2.7: col 22 = the latched rule-exit date · v1.2.12: col 23 = lot key (symbol@entry#n) · v1.2.15: col 24 = episode date of the last sell alert
function dp2ExitStateRead_() {
  var sh = dp2Sheet_(DP2X.SHEET_STATE, false);
  var out = {};
  if (!sh || sh.getLastRow() < 2) return out;
  var vals = sh.getRange(2, 1, sh.getLastRow() - 1, DP2X_STATE_HDR_.length).getValues();
  for (var i = 0; i < vals.length; i++) {
    var sym = dp2Str_(vals[i][0]).trim(); if (!sym) continue;
    var lotKey = dp2Str_(vals[i][22]).trim() || sym;   // v1.2.12: one state row per LOT; legacy rows (no lot column) key by symbol
    out[lotKey] = { symbol: sym, signal: dp2Str_(vals[i][12]), text: dp2Str_(vals[i][13]), since: dp2Str_(vals[i][14]), lastAlertSignal: dp2Str_(vals[i][15]), lastAlertAt: dp2Num_(vals[i][16]) || 0, lastRaiseAt: dp2Num_(vals[i][17]) || 0, latched: dp2DateStr_(vals[i][21]) || dp2Str_(vals[i][21]), lastAlertEpisode: dp2EpisodeStr_(vals[i][23]) };   // v1.2.16: raw text, so a '<date> live' key survives
  }
  return out;
}
function dp2ExitStateWrite_(rows) {
  var sh = dp2Sheet_(DP2X.SHEET_STATE, true);
  sh.clear();
  if (sh.getMaxRows() < rows.length + 2) sh.insertRowsAfter(sh.getMaxRows(), rows.length + 2 - sh.getMaxRows());
  if (sh.getMaxColumns() < DP2X_STATE_HDR_.length) sh.insertColumnsAfter(sh.getMaxColumns(), DP2X_STATE_HDR_.length - sh.getMaxColumns());
  sh.getRange(1, 1, 1, DP2X_STATE_HDR_.length).setValues([DP2X_STATE_HDR_]).setFontWeight('bold').setBackground(DP2C.headerBg);
  if (rows.length) {
    sh.getRange(2, 5, rows.length, 1).setNumberFormat('@');
    sh.getRange(2, 15, rows.length, 1).setNumberFormat('@');
    sh.getRange(2, 19, rows.length, 6).setNumberFormat('@');
    sh.getRange(2, 1, rows.length, DP2X_STATE_HDR_.length).setValues(rows);
  }
  sh.setFrozenRows(1);
  try { sh.hideSheet(); } catch (e) {}
}
function dp2ExitAlertsEnabled_() { var v = dp2Prop_(DP2X.PROP_ALERTS); return !v || /^(on|true|1|yes)$/i.test(v); }
function dp2ExitAlertTo_() {
  var v = dp2Prop_(DP2X.PROP_ALERT_TO);
  if (v && /@/.test(v)) return v;
  var u = '';
  try { u = Session.getEffectiveUser().getEmail(); } catch (e) {}
  if (!u) { try { u = Session.getActiveUser().getEmail(); } catch (e2) {} }
  return u || '';
}
/* items: [{ symbol, name, qty, entry, entryDate, x, sig, brokerStop, brokerTp, fx, mvSar, source, barsAsOf }] */
/* v1.2.9: a script lock serialises the read → send → write transition (a manual refresh may overlap the hourly trigger). */
function dp2Lock_(ms) { try { var lk = LockService.getScriptLock(); return lk.tryLock(ms) ? lk : null; } catch (e) { return null; } }
function dp2Unlock_(lk) { try { if (lk) lk.releaseLock(); } catch (e) {} }
function dp2ExitAlerts_(items, fx) {
  var lock = dp2Lock_(20000);
  if (!lock) return { enabled: dp2ExitAlertsEnabled_(), to: dp2ExitAlertTo_(), due: 0, sent: 0, signals: 0, error: 'another build holds the alert lock — alerts and state skipped this run (no duplicate e-mail)', skipped: true };
  try { return dp2ExitAlertsLocked_(items, fx); } finally { dp2Unlock_(lock); }
}
function dp2ExitAlertsLocked_(items, fx) {
  var prev = dp2ExitStateRead_();
  var now = Date.now(), nowStr = dp2Today_();
  var cooldown = DP2X.COOLDOWN_H * 3600000;
  var send = [], rows = [], states = {};
  for (var i = 0; i < items.length; i++) {
    var it = items[i], k0 = it.lotKey || it.symbol, p = prev[k0] || prev[it.symbol] || {};
    var since = (p.signal === it.sig.code && p.since) ? p.since : nowStr;
    var st = { signal: it.sig.code, since: since, lastAlertSignal: p.lastAlertSignal || '', lastAlertAt: p.lastAlertAt || 0, lastRaiseAt: p.lastRaiseAt || 0, lastAlertEpisode: p.lastAlertEpisode || '', sendKind: '' };
    // v1.2.15 (review finding): STOP_HIT / SELL_NOW / EXIT_LATCHED are one SELL family keyed to the exit EPISODE — a
    // presentation change between them never resets the cooldown; a genuinely new episode alerts at once.
    // v1.2.16 (review finding): the decision is dp2SellAlertDue_ — a live break keyed '<last bar> live' is the same
    // episode as the stop touch the next bar records (no second e-mail when the bars catch up).
    var episode = dp2ExitEpisode_(it);
    if (dp2SellAlertDue_(st, it, now, cooldown)) { st.sendKind = 'sell'; st.episode = episode; }
    else if (DP2X.ALERT_RAISE && it.sig.advisory && (!st.lastRaiseAt || now - st.lastRaiseAt >= cooldown)) st.sendKind = 'advisory';
    if (st.sendKind) send.push(it);
    states[k0] = st;
  }
  var result = { enabled: dp2ExitAlertsEnabled_(), to: dp2ExitAlertTo_(), due: send.length, sent: 0, signals: 0, error: '' };
  if (send.length && result.enabled) {
    try {
      dp2ExitSendMail_(send, fx, result.to);
      result.sent = 1; result.signals = send.length;
      for (i = 0; i < send.length; i++) {
        var s2 = states[send[i].lotKey || send[i].symbol];
        if (s2.sendKind === 'sell') { s2.lastAlertSignal = send[i].sig.code; s2.lastAlertAt = now; s2.lastAlertEpisode = s2.episode || s2.lastAlertEpisode || ''; } else { s2.lastRaiseAt = now; }
      }
    } catch (e) { result.error = String(e && e.message || e); }
  }
  for (i = 0; i < items.length; i++) {
    var it2 = items[i], x = it2.x, s3 = states[it2.lotKey || it2.symbol];
    rows.push([it2.symbol, it2.name || '', it2.qty, it2.entry, it2.entryDate || '', x ? x.s0 : '', x ? x.R : '', x ? x.hi : '', x ? x.atr : '', x ? (x.sma === null ? '' : x.sma) : '', it2.sig.last, x ? x.stop : '', it2.sig.code, it2.sig.text, s3.since, s3.lastAlertSignal, s3.lastAlertAt || '', s3.lastRaiseAt || '', it2.barsAsOf || '', it2.source || '', nowStr, (it2.sig.latched && !it2.sig.latchedAck) ? it2.sig.latched.date : '', it2.lotKey || it2.symbol, s3.lastAlertEpisode || '']);
  }
  try { dp2ExitStateWrite_(rows); } catch (e4) { result.error = (result.error ? result.error + ' · ' : '') + 'state: ' + String(e4 && e4.message || e4); }
  return result;
}
/* v1.2.16 (review finding): the stored episode key is read as RAW TEXT — dp2DateStr_ would cut '2026-10-20 live' to
 * '2026-10-20', and the next build's live key would then compare as a later episode and re-send on every run. A Date
 * (a legacy row, or a sheet that converted a date-like cell) is formatted back to yyyy-MM-dd. */
function dp2EpisodeStr_(v) {
  if (v === null || v === undefined || v === '') return '';
  if (v instanceof Date) return isNaN(v.getTime()) ? '' : Utilities.formatDate(v, DP2.TZ, 'yyyy-MM-dd');
  return dp2Str_(v).trim();
}
/* v1.2.16: is exit episode `ep` LATER than the episode `prev` the last sell alert named (keys from dp2ExitEpisode_)?
 * A dated key is the start of a replay episode; 'D live' is a stop break the live quote shows after the bar D that the
 * bars do not record yet — it is the FIRST stop episode the replay records after D, so when the bars catch up and the
 * key becomes that episode's date it is the same episode, not a later one. */
function dp2EpisodeIsLater_(ep, prev, x) {
  if (!ep || !prev || ep === prev) return false;
  var re = /^(\d{4}-\d{2}-\d{2}) live$/, mp = re.exec(prev), me = re.exec(ep);
  var pd = mp ? mp[1] : prev, ed = me ? me[1] : ep;
  if (!(ed > pd)) return false;
  if (mp && !me) {
    var hh = (x && x.hits) || [];
    for (var i = 0; i < hh.length; i++) { if (hh[i].date > pd) return hh[i].date !== ed; }
  }
  return true;
}
/* v1.2.16: the SELL alert decision for one lot — st = its state (last alert signal / time / episode), it = the item.
 * Sent when the signal alerts and: no earlier sell alert, the cooldown has passed, or the episode is a later one. */
function dp2SellAlertDue_(st, it, now, cooldown) {
  if (!it || !it.sig || !it.sig.alert) return false;
  var prevSell = /^(STOP_HIT|SELL_NOW|EXIT_LATCHED)$/.test(dp2Str_(st && st.lastAlertSignal));
  if (!prevSell || !st.lastAlertAt || now - st.lastAlertAt >= cooldown) return true;
  return dp2EpisodeIsLater_(dp2ExitEpisode_(it), st.lastAlertEpisode || '', it.x);
}
/* v1.2.15: the exit EPISODE a sell signal belongs to — the latched date, else the start of the current stop / momentum
 * episode from the replay, else the bar date. Two sell codes about the same episode are the same alert. */
function dp2ExitEpisode_(it) {
  var s = it.sig, x = it.x;
  if (s.code === 'EXIT_LATCHED' && s.latched && s.latched.date) return s.latched.date;
  if (!x) return '';
  if (s.code === 'SELL_NOW') { var mm = x.moms || []; return mm.length ? mm[mm.length - 1].date : (x.lastDate || ''); }
  if (s.code === 'STOP_HIT') {
    // the live quote may break the stop before the bar shows it: that is a NEW episode dated by the last bar + ' live'
    var hh = x.hits || [];
    return (x.hitNow && hh.length) ? hh[hh.length - 1].date : ((x.lastDate || '') + ' live');
  }
  return x.lastDate || '';
}
function dp2ExitLine_(it, fx) {
  var x = it.x, s = it.sig;
  // v1.2.10 (review finding): x is null when the lot has no usable price history (ENGINE_NA); every reference below
  // must survive that, or one rotation candidate without bars would abort the whole e-mail.
  var lastPx = x ? x.last : (s.last !== null && s.last !== undefined ? s.last : null);
  var pnl = (lastPx !== null && it.qty && it.entry) ? (lastPx - it.entry) * it.qty * (it.fx || fx) : null;
  var why = '';
  if (s.code === 'STOP_HIT') why = 'last ' + dp2Fmt_(x.last, 2) + ' is at or below the dynamic stop ' + dp2Fmt_(x.stop, 2) + ' (highest close ' + dp2Fmt_(x.hi, 2) + ', ' + x.trailMult + ' × ATR ' + dp2Fmt_(x.atr, 2) + ' under it' + (x.beLocked ? ', breakeven locked' : '') + ').';
  else if (s.code === 'SELL_NOW') why = 'in profit, close ' + dp2Fmt_(x.last, 2) + ' is below the 10-day average ' + dp2Fmt_(x.sma, 2) + ' after giving back ' + dp2Fmt_((x.hi - x.last) / x.atr, 1) + ' ATR from the high ' + dp2Fmt_(x.hi, 2) + ' (the move at the high was ' + dp2RFmt_(x.gainR) + ').';
  else if (s.code === 'STOP_NEAR') why = 'last ' + dp2Fmt_(x.last, 2) + ' is only ' + dp2PctPlain_(s.dist, 1) + ' above the dynamic stop ' + dp2Fmt_(x.stop, 2) + ' (highest close ' + dp2Fmt_(x.hi, 2) + ', ATR ' + dp2Fmt_(x.atr, 2) + ')' + (it.brokerStop ? '; the fixed broker stop ' + dp2Fmt_(it.brokerStop, 2) + ' is ' + dp2Pct_((it.brokerStop / x.last - 1) * 100, 1) + ' away.' : '; no working broker stop is listed.');
  else if (s.code === 'RAISE_STOP') why = 'the trailing rule puts the stop at ' + dp2Fmt_(x.stop, 2) + ' (highest close ' + dp2Fmt_(x.hi, 2) + ', ' + dp2RFmt_(x.gainR) + ' at the high); the working broker stop is ' + dp2Fmt_(it.brokerStop, 2) + '.';
  else if (s.code === 'NO_BROKER_STOP') why = 'no working stop order is listed in _IBKR_Orders; the rule\'s stop is ' + dp2Fmt_(x.stop, 2) + '.';
  else if (s.code === 'STOP_PARTIAL') why = 'the active stop covers only part of the lot (' + dp2Fmt_(s.uncovered) + ' sh unprotected); the rule\'s stop is ' + dp2Fmt_(x.stop, 2) + '.';
  else if (s.code === 'EXIT_LATCHED') why = 'the rule exited this lot on ' + s.latched.text + ' and the position is still open; the alert for that day was never acted on, and a later recovery does not reinstate the trade. Today: last ' + dp2Fmt_(x.last, 2) + ', trailing stop ' + dp2Fmt_(x.stop, 2) + '.';
  else why = s.text;
  if (s.staleBars) why += ' PRICE HISTORY IS STALE (fetch failed; cached bars) — the engine ran on the cockpit quote; confirm the live price before any order.';
  // v1.2.3 (review finding): the action is specific to the signal code — a stop instruction only for RAISE_STOP /
  // NO_BROKER_STOP; HOLD-type codes never say "raise" to a lower level; a rotation-only mail carries the switch action.
  var action = null;
  var trailTxt = ' — or a trailing stop order, trail amount ' + (x ? dp2Fmt_(x.trailMult * x.atr, 2) + ' (' + x.trailMult + ' × ATR)' : 'n/a') + '.';
  if (s.code === 'STOP_HIT' || s.code === 'SELL_NOW') action = 'Sell now (market, or a limit no lower than ' + dp2Fmt_(x.last - 0.25 * x.atr, 2) + ').';
  else if (s.code === 'EXIT_LATCHED') action = 'Sell now (market, or a limit no lower than ' + dp2Fmt_(x.last - 0.25 * x.atr, 2) + ') — or, to hold by decision, acknowledge the ' + s.latched.date + ' exit (📊 Decision v2 ▸ ✅ Acknowledge rule exit); the trailing stop ' + dp2Fmt_(x.stop, 2) + ' then governs and must be working at the broker.';
  else if (s.code === 'STOP_NEAR') action = 'Decide now: pre-empt at ' + dp2Fmt_(x.last, 2) + ', or let the dynamic stop ' + dp2Fmt_(x.stop, 2) + ' decide — and make sure a broker stop at least that high is working.';
  else if (s.code === 'RAISE_STOP') action = 'Raise the IBKR stop from ' + dp2Fmt_(it.brokerStop, 2) + ' to ' + dp2Fmt_(x.stop, 2) + trailTxt;
  else if (s.code === 'NO_BROKER_STOP') action = 'Place a stop at ' + dp2Fmt_(x.stop, 2) + (s.lotOfMany && s.lotShares ? ' for this lot\'s ' + dp2Fmt_(s.lotShares) + ' sh' : '') + ' (no working stop is listed' + (s.lotOfMany ? ' for this lot' : '') + ')' + trailTxt;
  else if (s.code === 'STOP_PARTIAL') action = 'Place a stop for the uncovered ' + dp2Fmt_(s.uncovered) + ' sh at ' + dp2Fmt_(x.stop, 2) + (it.brokerStop && x.stop >= it.brokerStop * (1 + DP2X.RAISE_MIN_PCT / 100) ? ', and raise the existing stop from ' + dp2Fmt_(it.brokerStop, 2) + ' to ' + dp2Fmt_(x.stop, 2) : '') + trailTxt;
  else if (s.code === 'TP_WATCH') action = 'No stop change. The broker take-profit ' + dp2Fmt_(it.brokerTp, 2) + ' is close: keep it to bank the gain, or cancel it to let the trail (' + dp2Fmt_(x.stop, 2) + ') run the winner.';
  else if (s.code === 'HOLD_BROKER_UNKNOWN') action = 'No stop change from this page — the broker orders are unknown here; confirm at the broker that a stop at or above ' + (x ? dp2Fmt_(x.stop, 2) : 'the rule\'s level') + ' is working.';
  else if (s.code === 'HOLD' || s.code === 'HOLD_BE') action = 'No stop change — the working broker stop ' + (it.brokerStop && x ? dp2Fmt_(it.brokerStop, 2) + ' is at or above the rule\'s ' + dp2Fmt_(x.stop, 2) : 'is where the rule puts it') + '; keep it. Never lower a stop.';
  else action = s.text + ' — no stop change.';
  // v1.2.10 (review finding): on STALE bars no liquidation instruction leaves this page — verification wording only.
  if (s.staleBars) action = 'VERIFY ONLY — the price history is stale (fetch failed; cached bars to ' + (x ? x.lastDate : 'n/a') + '), so this signal was computed on the cockpit quote ' + dp2Fmt_(lastPx, 2) + '. Check the live price and the working stop at the broker; no sale or stop change is instructed by this message. The next successful fetch re-evaluates the rule.';
  if (it.rotation && it.rotation.plan) {
    var rp = it.rotation.plan, rr = rp.rotation;
    var rtxt = ' SWITCH (research): ' + rr.action.toLowerCase() + ' ' + dp2Fmt_(rr.proceeds) + ' SAR to fund ' + rp.sym + ' — ' + (rp.basis === '3m' ? 'plan 3M' : 'engine 12M') + ' ' + dp2PctPlain_(rp.oppRoi, 1) + ' vs this holding ' + dp2PctPlain_(rr.roi, 1) + ' (edge +' + dp2Fmt_(rr.edge, 1) + ' pp after ' + (rp.cost !== undefined ? rp.cost : DP2A.ROT_COST_PCT) + '% cost).';
    why += rtxt;
    var switchAct = 'SWITCH (research): ' + rr.action.toLowerCase() + ' ' + it.symbol + ' ' + dp2Fmt_(rr.proceeds) + ' SAR to fund ' + rp.sym + ' when the feed is released — the engine will carry the same plan. Research only; no order is placed by this page.';
    if (s.staleBars) action += ' ' + switchAct;
    else if (s.code === 'STOP_HIT' || s.code === 'SELL_NOW' || s.code === 'EXIT_LATCHED' || s.code === 'STOP_NEAR' || s.code === 'RAISE_STOP' || s.code === 'NO_BROKER_STOP' || s.code === 'STOP_PARTIAL' || s.code === 'TP_WATCH') action += ' Then: ' + switchAct;   // v1.2.14: a latched exit's sell instruction leads, the switch follows
    else action = switchAct + ' Stop: ' + action;
  }
  return { head: it.symbol + ' — ' + (it.name || '') + ' · ' + dp2Fmt_(it.qty) + ' × ' + dp2Fmt_(it.entry, 2) + ' → last ' + dp2Fmt_(lastPx, 2) + (pnl !== null ? ' (' + dp2Signed_(pnl) + ' SAR unrealized)' : '') + (x ? '' : ' · no price history'), signal: s.text + (it.rotation ? ' · SWITCH candidate' : ''), why: why, action: action };
}
function dp2ExitSendMail_(items, fx, to) {
  if (!to) throw new Error('no recipient — set Script Property ' + DP2X.PROP_ALERT_TO);
  var sells = items.filter(function (it) { return it.sig.alert; });
  var subject = 'TFB exit alert — ' + (sells.length ? sells.map(function (it) { return it.symbol + ' ' + (it.sig.code === 'STOP_HIT' ? 'STOP HIT' : (it.sig.code === 'EXIT_LATCHED' ? 'RULE EXIT ' + it.sig.latched.date + ' not acted' : 'SELL NOW')); }).join(', ') : items.map(function (it) { return it.symbol + ' ' + (it.sig.staleBars ? 'stale bars — verify' : (it.rotation ? 'switch candidate' : (it.sig.code === 'STOP_NEAR' ? 'stop near' : (it.sig.code === 'NO_BROKER_STOP' ? 'no broker stop' : (it.sig.code === 'STOP_PARTIAL' ? 'stop covers part of the lot' : (it.sig.code === 'RAISE_STOP' ? 'raise stop' : 'advisory')))))); }).join(', ')) + ' · ' + dp2Today_();
  var text = [], html = ['<div style="font-family:Arial,sans-serif;font-size:13px;color:#14202b">', '<p><b>Decision Pages v2 ' + DP2_VERSION + '</b> · dynamic exit signals · ' + dp2Today_() + ' Riyadh</p>'];
  for (var i = 0; i < items.length; i++) {
    var L = dp2ExitLine_(items[i], fx);
    var red = items[i].sig.alert;
    text.push(L.head, '  SIGNAL: ' + L.signal, '  WHY: ' + L.why, '  DO: ' + L.action, '');
    html.push('<div style="border:1px solid #d5dde7;border-left:6px solid ' + (red ? '#b42318' : '#8a5a00') + ';padding:8px 12px;margin:8px 0">',
      '<div style="font-weight:bold">' + L.head + '</div>',
      '<div style="margin-top:4px"><span style="background:' + (red ? '#b42318' : '#fff4e5') + ';color:' + (red ? '#fff' : '#8a5a00') + ';padding:2px 8px;font-weight:bold">' + L.signal + '</span></div>',
      '<div style="margin-top:6px;color:#4a5a6b">Why: ' + L.why + '</div>',
      '<div style="margin-top:4px"><b>Do:</b> ' + L.action + '</div></div>');
  }
  var foot = 'Rule: initial stop entry − 2 × ATR14; breakeven lock after +1R; trail 2 × ATR under the highest close (1.5 × ATR after +2R); profit protection when a profitable name closes below its 10-day average after giving back ≥ 1 ATR. Prices: daily bars + last from the chart API (may lag the tape by minutes). This page placed no order — act at the broker. Pause: Today ▸ 🔔 Dynamic Exits — alerts on/off.';
  text.push(foot);
  html.push('<p style="font-size:11px;color:#898781">' + foot + '</p></div>');
  MailApp.sendEmail({ to: to, subject: subject, body: text.join('\n'), htmlBody: html.join('') });
}
function dp2ExitStatus() {
  var sh = dp2Sheet_(DP2X.SHEET_STATE, false);
  var lines = ['Dynamic exits · ' + DP2_VERSION, 'alerts ' + (dp2ExitAlertsEnabled_() ? 'ON' : 'OFF') + ' → ' + (dp2ExitAlertTo_() || '(no recipient)'), 'cooldown ' + DP2X.COOLDOWN_H + ' h · rule 2×ATR / BE after +1R / trail 2×ATR (1.5×ATR after +2R) / SMA10 profit protection', ''];
  if (!sh || sh.getLastRow() < 2) lines.push('No state yet — run Build Portfolio_Decision_v2 first.');
  else {
    var vals = sh.getRange(2, 1, sh.getLastRow() - 1, DP2X_STATE_HDR_.length).getValues();
    for (var i = 0; i < vals.length; i++) {
      if (!dp2Str_(vals[i][0])) continue;
      lines.push(dp2Str_(vals[i][0]) + ' · ' + dp2Str_(vals[i][13]) + ' · last ' + dp2Fmt_(dp2Num_(vals[i][10]), 2) + ' · dyn stop ' + dp2Fmt_(dp2Num_(vals[i][11]), 2) + ' · high ' + dp2Fmt_(dp2Num_(vals[i][7]), 2) + ' · ATR ' + dp2Fmt_(dp2Num_(vals[i][8]), 2) + ' · since ' + dp2Str_(vals[i][14]) + (dp2Str_(vals[i][15]) ? ' · last alert ' + dp2Str_(vals[i][15]) : '') + ' · bars ' + dp2Str_(vals[i][18]) + ' (' + dp2Str_(vals[i][19]) + ')');
    }
    lines.push('', 'Updated ' + dp2Str_(vals[0][20]));
  }
  var msg = lines.join('\n');
  try { SpreadsheetApp.getUi().alert('Decision Pages v2', msg, SpreadsheetApp.getUi().ButtonSet.OK); } catch (e) { Logger.log(msg); }
  return msg;
}
function dp2ExitTestAlert() {
  var to = dp2ExitAlertTo_();
  var msg;
  try {
    if (!to) throw new Error('no recipient — set Script Property ' + DP2X.PROP_ALERT_TO);
    MailApp.sendEmail({ to: to, subject: 'TFB exit alert — TEST · ' + dp2Today_(), body: 'This is a test of the Decision Pages v2 ' + DP2_VERSION + ' exit alerts. Alerts are ' + (dp2ExitAlertsEnabled_() ? 'ON' : 'OFF') + '. Real alerts go out from Build Portfolio_Decision_v2 / the hourly trigger when a holding shows SELL NOW, and at most once a day for advisories (stop near, raise the broker stop, no working stop).' });
    msg = 'Test alert sent to ' + to + ' (remaining daily quota ' + MailApp.getRemainingDailyQuota() + ').';
  } catch (e) { msg = 'Test alert FAILED: ' + String(e && e.message || e); }
  try { SpreadsheetApp.getUi().alert('Decision Pages v2', msg, SpreadsheetApp.getUi().ButtonSet.OK); } catch (e2) { Logger.log(msg); }
  return msg;
}
function dp2ExitAlertsToggle() {
  var on = dp2ExitAlertsEnabled_();
  try { PropertiesService.getScriptProperties().setProperty(DP2X.PROP_ALERTS, on ? 'off' : 'on'); } catch (e) {}
  var msg = 'Dynamic exit e-mail alerts are now ' + (on ? 'OFF' : 'ON') + ' (Script Property ' + DP2X.PROP_ALERTS + '). The Exit signal chips on Portfolio_Decision_v2 keep updating either way.';
  try { SpreadsheetApp.getUi().alert('Decision Pages v2', msg, SpreadsheetApp.getUi().ButtonSet.OK); } catch (e2) { Logger.log(msg); }
  return !on;
}
/* v1.2.5: acknowledged rule exits — Script Property DP2_EXIT_ACK = JSON { '<lot key>': 'yyyy-MM-dd' }; v1.2.13: keyed by LOT
 * (symbol@entry), e.g. { 'CWBC.US@2026-09-10': '2026-10-14' }; a bare-symbol key is honoured only for a symbol held in one lot. */
function dp2ExitAckMap_() { var v = dp2Prop_(DP2X.PROP_EXIT_ACK); if (!v) return {}; try { var o = JSON.parse(v); return (o && typeof o === 'object') ? o : {}; } catch (e) { return {}; } }
function dp2ExitAckSet_(symbol, date) {
  var m = dp2ExitAckMap_(); if (date) m[symbol] = date; else delete m[symbol];
  try { PropertiesService.getScriptProperties().setProperty(DP2X.PROP_EXIT_ACK, JSON.stringify(m)); } catch (e) {}
  return m;
}
function dp2ExitAckLatched() {
  var ui = null; try { ui = SpreadsheetApp.getUi(); } catch (e) {}
  var st = dp2ExitStateRead_(), latched = [], k;
  for (k in st) { if (st.hasOwnProperty(k) && st[k].signal === 'EXIT_LATCHED') { var dm = /rule exit (\d{4}-\d{2}-\d{2})/.exec(st[k].text || ''); latched.push({ key: k, symbol: st[k].symbol || k, date: st[k].latched || (dm ? dm[1] : '') }); } }   // v1.2.13: one entry per LOT
  var ack = dp2ExitAckMap_();
  var cur = Object.keys(ack).map(function (s2) { return s2 + ' (' + ack[s2] + ')'; }).join(', ') || 'none';
  if (!latched.length) {
    var msg0 = 'No latched rule exit on the last build. Acknowledged so far: ' + cur + '. Type a symbol to clear its acknowledgement, or Cancel.';
    if (!ui) { Logger.log(msg0); return null; }
    var r0 = ui.prompt('Decision Pages v2 — rule exits', msg0, ui.ButtonSet.OK_CANCEL);
    if (r0.getSelectedButton() !== ui.Button.OK) return null;
    var c0 = dp2Str_(r0.getResponseText()).trim().toUpperCase();
    if (c0) { dp2ExitAckSet_(c0, null); ui.alert('Decision Pages v2', 'Acknowledgement cleared for ' + c0 + '. Refresh Portfolio_Decision_v2.', ui.ButtonSet.OK); }
    return c0 || null;
  }
  var list = latched.map(function (x) { return x.key + ' (exit ' + (x.date || 'date n/a') + ')'; }).join(', ');
  var msg = 'Latched rule exits (one per lot): ' + list + '.\nAcknowledged so far: ' + cur + '.\n\nType ONE lot key exactly as listed (a bare symbol is accepted only when that symbol has a single latched lot) to acknowledge its exit and hold by decision (the signal becomes an advisory and the trailing stop governs). This is your decision, recorded in Script Property ' + DP2X.PROP_EXIT_ACK + '.';
  if (!ui) { Logger.log(msg); return null; }
  var r = ui.prompt('Decision Pages v2 — acknowledge rule exit', msg, ui.ButtonSet.OK_CANCEL);
  if (r.getSelectedButton() !== ui.Button.OK) return null;
  var typed = dp2Str_(r.getResponseText()).trim().toUpperCase(); if (!typed) return null;
  var hit = null, matches = [];
  for (var i = 0; i < latched.length; i++) { if (latched[i].key.toUpperCase() === typed) { hit = latched[i]; break; } if (latched[i].symbol.toUpperCase() === typed) matches.push(latched[i]); }
  if (!hit && matches.length === 1) hit = matches[0];
  if (!hit) { ui.alert('Decision Pages v2', matches.length > 1 ? typed + ' has ' + matches.length + ' latched lots — type the exact lot key (' + matches.map(function (m) { return m.key; }).join(', ') + ').' : typed + ' is not a latched exit on the last build.', ui.ButtonSet.OK); return null; }
  if (!hit.date) { ui.alert('Decision Pages v2', hit.key + ': the latched exit has no date in _DP2_Exit_State — refresh Portfolio_Decision_v2 first, then acknowledge.', ui.ButtonSet.OK); return null; }
  dp2ExitAckSet_(hit.key, hit.date);
  ui.alert('Decision Pages v2', hit.key + ': rule exit ' + (hit.date || '') + ' acknowledged — refresh Portfolio_Decision_v2 to see the advisory state.', ui.ButtonSet.OK);
  return hit.symbol;
}
function dp2FixtureBars_(f) { var out = []; for (var i = 0; i < f.t.length; i++) out.push({ date: f.t[i], o: f.o[i], h: f.h[i], l: f.l[i], c: f.c[i] }); return out; }

/* ========================================================= research allocation + rotation (v1.2.0) */
function dp2PctNum_(v) { var n = dp2Num_(v); if (n === null) return null; return Math.abs(n) < 1.5 ? n * 100 : n; }
/* PURE — mirror of opportunity_builder._conviction_weight: plan ROI % × risk multiplier × reliability factor, tilted. */
function dp2ConvWeight_(roiPct, risk, rel, tilt, reqRoi) {
  var roi = (roiPct !== null && roiPct !== undefined && isFinite(roiPct) && roiPct > 0) ? roiPct : (reqRoi > 0 ? reqRoi : DP2A.REQUIRED_ROI_PCT);   // v1.2.20: the panel's Required ROI %
  var rm = DP2A.RISK_MULT.hasOwnProperty(risk) ? DP2A.RISK_MULT[risk] : DP2A.RISK_DEFAULT;
  var relm = (rel !== null && rel !== undefined && isFinite(rel)) ? Math.min(1, Math.max(DP2A.REL_MIN, rel / 100)) : DP2A.REL_MIN;
  var base = roi * rm * relm;
  var t = (tilt === undefined || tilt === null) ? DP2A.TILT : tilt;
  if (base <= 0) return 0;
  return t === 0 ? 1 : Math.pow(base, t);
}
/* PURE — mirror of opportunity_builder._conviction_targets (water-filling with per-seat cap and minimum ticket). */
/* v1.2.18: venue floors = the engine's _VENUE_COSTS floor_sar (raise the operator floor when TFB_OPP_VENUE_FLOORS is on). */
var DP2_VENUE_FLOOR_SAR_ = { '': 5000, '.US': 5000, '.SR': 4000, '.T': 13200, '.KL': 45000, '.HK': 27200, '.L': 27700,
  '.PA': 24500, '.AS': 24500, '.BR': 24500, '.DE': 24500, '.MI': 24500, '.MC': 24500, '.LS': 24500, '.VI': 24500,
  '.SW': 24200, '.TO': 15500, '.AX': 14100, '.OL': 20100, '.SI': 16700, '.MX': 11000 };   // = _VENUE_COSTS floor_sar (opportunity_builder v1.16.0+)
function dp2VenueFloor_(sym) {
  var s = dp2Str_(sym).trim().toUpperCase(), suf = s.indexOf('.') >= 0 ? '.' + s.split('.').pop() : '';
  return DP2_VENUE_FLOOR_SAR_.hasOwnProperty(suf) ? DP2_VENUE_FLOOR_SAR_[suf] : null;
}
/* v1.2.20 (review findings — tilt parity and the sector-cap denominator — and the class they belong to): this page
 * re-implements the engine's sizing and funding, so EVERY production setting on that path must be mirrored, or no
 * switch is planned. One Script Property carries them all: DP2_ENGINE_ENV = the TFB_OPP_* / TFB_T10_* variables of the
 * Render service as KEY=VALUE lines (or one JSON object). A key that is absent is unset in production — the engine
 * default — so the paste is the whole configuration by construction; lines that are not TFB_OPP_/TFB_T10_ are ignored
 * (never paste secrets). Parsing follows the engine's readers (_env_str: blank = default; _env_float / _env_int /
 * _env_flag01 / the default-ON kill switches). A Top_10 panel row the route maps onto a criterion (Max Weight %,
 * Min Ticket SAR, Lot Size, Required ROI %) wins over the ENV, as make_criteria. Switches are planned only when
 * (1) the paste is present, (2) every sizing/funding setting it carries is modelled here or sits at its engine default,
 * (3) production's engine (Top_10 status "builder vX") is recent enough for the rules in use, (4) the funding layer is
 * on. Otherwise the plans are withheld and the page says which condition failed. */
var DP2_GICS_SECTOR_ = { 'Basic Materials': 'Materials', 'Healthcare': 'Health Care', 'Consumer Cyclical': 'Consumer Discretionary',
  'Consumer Defensive': 'Consumer Staples', 'Technology': 'Information Technology', 'Financial Services': 'Financials' };   // = core.sectors.YAHOO_TO_GICS_SECTOR
var DP2_DEFAULT_VENUE_LOTS_ = { T: 100, SI: 100, KL: 100 };                                   // = _DEFAULT_VENUE_LOTS
var DP2_DEFAULT_SYNTHETIC_ = ['phaseiisynthetic'];                                            // = _DEFAULT_SYNTHETIC_SOURCES, normalized (_norm_token)
var DP2_DEGRADED_WARN_ = ['rank_skipped_low_trust', 'momentum_only_fallback'];                // = _ROTATION_DEGRADED_TOKENS
var DP2_ENGINE_NEED_ = { base: '1.26.0', conviction: '1.27.3', plan3m: '1.27.1' };            // engine versions whose rules this page mirrors
// sizing / funding settings this page does NOT model: switches are withheld unless they sit at the engine default
var DP2_ENV_UNMODELLED_ = [
  { k: 'TFB_OPP_ROTATION_FLOOR_GUARD', def: 'on', isDef: function (v) { return !/^(0|false|off|no)$/.test(v); } },
  { k: 'TFB_OPP_HELD_TARGET_NO_NEW_MONEY', def: 'off', isDef: function (v) { return !/^(1|true|yes|on)$/.test(v); } },
  { k: 'TFB_OPP_FUNDING_SETTLED_ONLY', def: 'off', isDef: function (v) { return !/^(1|true|yes|on)$/.test(v); } },
  { k: 'TFB_T10_PRICE_XCHECK', def: 'off / observe', isDef: function (v) { return v !== 'enforce'; } }
];
var DP2_NUM_RE_ = /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/;
function dp2Int_(x) { return x < 0 ? Math.ceil(x) : Math.floor(x); }   // Python int() on a float
function dp2NormToken_(s) { return dp2Str_(s).toLowerCase().replace(/[^a-z0-9]/g, ''); }   // _norm_token
function dp2VerCmp_(a, b) { var x = dp2Str_(a).split('.'), y = dp2Str_(b).split('.'); for (var i = 0; i < 3; i++) { var d = (parseInt(x[i], 10) || 0) - (parseInt(y[i], 10) || 0); if (d) return d < 0 ? -1 : 1; } return 0; }
function dp2EngineEnv_() {
  var raw = dp2Str_(dp2Prop_(DP2X.PROP_ENGINE_ENV));
  var out = { present: false, map: {}, keys: [], ignored: 0 };
  if (!raw.trim()) return out;
  out.present = true;
  var put = function (k, v) {
    k = dp2Str_(k).trim().replace(/^export\s+/i, '').toUpperCase();
    if (!/^TFB_(OPP|T10)_[A-Z0-9_]+$/.test(k)) { out.ignored++; return; }
    v = dp2Str_(v).trim(); var q = /^(['"])([\s\S]*)\1$/.exec(v); if (q) v = q[2].trim();
    if (!out.map.hasOwnProperty(k)) out.keys.push(k);
    out.map[k] = v;
  };
  var obj = null;
  if (/^\s*\{/.test(raw)) { try { obj = JSON.parse(raw); } catch (eJ) { obj = null; } }
  if (obj && typeof obj === 'object') { for (var k in obj) if (obj.hasOwnProperty(k)) put(k, obj[k]); return out; }
  var lines = raw.split(/\r?\n/);
  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].trim(); if (!line || /^#/.test(line)) continue;
    var m = /^([A-Za-z_][A-Za-z0-9_ ]*?)\s*[=:]\s*([\s\S]*)$/.exec(line);
    if (m) put(m[1], m[2]); else out.ignored++;
  }
  return out;
}
/* The Top_10 control-panel rows the route maps onto a sizing criterion (_opp_criteria_from_body → make_criteria). */
function dp2PanelCriteria_(t) {
  var pnl = (t && t.panel) || {}, out = {};
  var alias = { maxweightpct: 'max_weight_pct', maxweight: 'max_weight_pct', lotsize: 'lot_size', requiredroi: 'required_roi_pct', requiredroipct: 'required_roi_pct' };
  for (var k in pnl) {
    if (!pnl.hasOwnProperty(k)) continue;
    var tok = dp2Str_(k).trim().replace(/^t10:\s*/i, ''), compact = tok.toLowerCase().replace(/[^a-z0-9]+/g, '');
    var key = alias[compact] || tok.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
    if (key !== 'max_weight_pct' && key !== 'min_ticket_sar' && key !== 'lot_size' && key !== 'required_roi_pct' && key !== 'pf_max_sector_pct') continue;
    var v = dp2Num_(pnl[k]); if (v !== null) out[key] = key === 'lot_size' ? dp2Int_(v) : v;
  }
  return out;
}
/* v1.2.22: the last completed cockpit run's status line as _Status records it (row 'Top_10_Investments', Message). */
var DP2_T10_STATUS_MEMO_ = null;
function dp2T10StatusMsg_() {
  if (DP2_T10_STATUS_MEMO_ !== null) return DP2_T10_STATUS_MEMO_;
  var msg = '';
  try { var sh = dp2Sheet_(DP2.SRC_STATUS, false); if (sh && sh.getLastRow() > 1) { var v = sh.getRange(1, 1, Math.min(sh.getLastRow(), 40), 4).getValues(); for (var i = 0; i < v.length; i++) { if (dp2Str_(v[i][0]).trim() === 'Top_10_Investments') { msg = dp2Str_(v[i][3]); break; } } } } catch (eS) { msg = ''; }
  DP2_T10_STATUS_MEMO_ = msg;
  return msg;
}
function dp2ProdConfig_(t) {
  var env = dp2EngineEnv_(), M = env.map;
  var raw = function (k) { return M.hasOwnProperty(k) ? dp2Str_(M[k]).trim() : ''; };
  var low = function (k) { return raw(k).toLowerCase(); };
  var flt = function (k, d) { var v = raw(k); return (v !== '' && DP2_NUM_RE_.test(v)) ? Number(v) : d; };                       // _env_float
  var nonneg = function (k, d) { var v = raw(k); if (v === '' || !DP2_NUM_RE_.test(v)) return d; var x = Number(v); return x >= 0 ? x : d; };   // _env_rotation_*
  var c = { known: env.present, envPresent: env.present, envKeys: env.keys.slice(), envIgnored: env.ignored, unmodelled: [], issues: [] };
  var pc = dp2PanelCriteria_(t);
  // sizing (_select_and_size, _size_one, _conviction_targets)
  c.sizingMode = low('TFB_OPP_SIZING_MODE') === 'conviction' ? 'conviction' : 'cap';
  var tr = raw('TFB_OPP_SIZING_TILT'), tn = tr === '' ? 1 : (DP2_NUM_RE_.test(tr) ? Number(tr) : NaN);
  c.tilt = (isFinite(tn) && tn >= 0 && tn <= 3) ? tn : 1;
  c.maxWeightPct = pc.max_weight_pct !== undefined ? pc.max_weight_pct : flt('TFB_OPP_MAX_WEIGHT_PCT', DP2A.MAX_WEIGHT_PCT);
  c.capSource = pc.max_weight_pct !== undefined ? 'Top_10 panel' : (raw('TFB_OPP_MAX_WEIGHT_PCT') !== '' ? 'TFB_OPP_MAX_WEIGHT_PCT' : 'engine default');
  c.opFloor = env.present ? Math.max(0, pc.min_ticket_sar !== undefined ? pc.min_ticket_sar : flt('TFB_OPP_MIN_TICKET_SAR', 0)) : DP2A.MIN_TICKET_SAR;
  c.venueOn = env.present && /^(1|true|yes|on)$/.test(low('TFB_OPP_VENUE_FLOORS'));
  var lsRaw = raw('TFB_OPP_LOT_SIZE'), ls = pc.lot_size !== undefined ? pc.lot_size : ((lsRaw !== '' && DP2_NUM_RE_.test(lsRaw)) ? dp2Int_(Number(lsRaw)) : 1);
  c.lotSize = Math.max(1, ls);
  c.venueLots = {};
  var vl = raw('TFB_T10_VENUE_LOTS');
  if (vl.toLowerCase() === 'default') { for (var dl in DP2_DEFAULT_VENUE_LOTS_) if (DP2_DEFAULT_VENUE_LOTS_.hasOwnProperty(dl)) c.venueLots[dl] = DP2_DEFAULT_VENUE_LOTS_[dl]; }
  else if (vl) { vl.replace(/;/g, ',').split(',').forEach(function (tok) { tok = tok.trim(); var ix = tok.indexOf(':'); if (ix < 0) return; var suf = tok.slice(0, ix).trim().toUpperCase().replace(/^\.+/, ''), val = tok.slice(ix + 1).trim(); if (!DP2_NUM_RE_.test(val)) return; var lot = dp2Int_(Number(val)); if (suf && lot > 1) c.venueLots[suf] = lot; }); }
  c.entryHigh = /^(1|true|yes|on)$/.test(low('TFB_OPP_SIZE_AT_ENTRY_HIGH'));
  var scb = low('TFB_OPP_SECTOR_CAP_BASIS') || 'budget';
  c.sectorBasis = (scb === 'budget' || scb === 'legacy' || scb === 'pd') ? scb : 'budget';
  c.pfMaxSectorPct = pc.pf_max_sector_pct !== undefined ? pc.pf_max_sector_pct : flt('TFB_OPP_PF_MAX_SECTOR_PCT', 30);
  var sn = low('TFB_OPP_SECTOR_NORMALIZE'); c.sectorNormalize = sn === '' ? true : /^(1|true|yes|on|enabled|enable)$/.test(sn);
  var rr = pc.required_roi_pct !== undefined ? pc.required_roi_pct : DP2A.REQUIRED_ROI_PCT; c.requiredRoiPct = rr > 0 ? rr : 1;
  // funding layer (build_opportunity_payload → _funding_plans → _rotation_pick)
  c.fundingPlan = !/^(0|false|off|no)$/.test(low('TFB_OPP_FUNDING_PLAN'));
  c.rotBasis = low('TFB_OPP_ROTATION_BASIS') === 'plan3m' ? 'plan3m' : '12m';
  var rot = dp2RotParamsDefault_();
  rot.edge12 = nonneg('TFB_OPP_ROTATION_EDGE_PP', DP2A.ROT_EDGE_PP_12M); rot.edge3 = nonneg('TFB_OPP_ROTATION_EDGE_PP_3M', DP2A.ROT_EDGE_PP_3M);
  rot.cost = nonneg('TFB_OPP_ROTATION_COST_PCT', DP2A.ROT_COST_PCT); rot.minHeld = nonneg('TFB_OPP_ROTATION_MIN_HELD_DAYS', DP2A.ROT_MIN_HELD_DAYS);
  rot.tp1Prox = nonneg('TFB_OPP_ROTATION_TP1_PROXIMITY_PCT', DP2A.ROT_TP1_PROX_PCT);
  var exr = raw('TFB_OPP_ROTATION_EXCLUDE');
  rot.exclude = (exr === '' ? DP2A.ROT_EXCLUDE.join(',') : exr).toUpperCase().replace(/;/g, ',').split(',').map(function (x) { return x.trim(); }).filter(function (x) { return !!x; });
  var syn = raw('TFB_T10_SYNTHETIC_SOURCES').replace(/;/g, ',').split(',').map(dp2NormToken_).filter(function (x) { return !!x; });
  rot.synthetic = syn.length ? syn : DP2_DEFAULT_SYNTHETIC_.slice();
  rot.remnantFloor = c.opFloor;   // _funding_plans(floor_sar=_min_floor): the operator floor
  var tnr = raw('TFB_OPP_CAPITAL_CALL_TOPN'), tnv = (tnr !== '' && DP2_NUM_RE_.test(tnr)) ? dp2Int_(Number(tnr)) : 3; c.capitalCallTopN = tnv > 0 ? tnv : 3;   // _env_capital_call_topn
  rot.source = env.present ? 'DP2_ENGINE_ENV' : 'engine defaults';
  c.rot = rot;
  // what the page does not model
  for (var u = 0; u < DP2_ENV_UNMODELLED_.length; u++) { var U = DP2_ENV_UNMODELLED_[u], uv = low(U.k); if (uv !== '' && !U.isDef(uv)) c.unmodelled.push(U.k + '=' + raw(U.k) + ' (engine default ' + U.def + ')'); }
  // the production engine's version (Top_10 status line) against the rules in use
  // v1.2.22: a cockpit run still in progress shows 'status: running…' without the builder version — fall back to the
  // last completed run's line on _Status (row Top_10_Investments, Message)
  var mv = /builder v(\d+\.\d+\.\d+)/.exec(dp2Str_(t && t.status)) || /builder v(\d+\.\d+\.\d+)/.exec(dp2T10StatusMsg_()); c.engineVersion = mv ? mv[1] : '';
  var need = DP2_ENGINE_NEED_.base, why = 'the funding rules this page mirrors';
  if (c.sizingMode === 'conviction' && dp2VerCmp_(DP2_ENGINE_NEED_.conviction, need) > 0) { need = DP2_ENGINE_NEED_.conviction; why = 'conviction sizing (capital exhaustion and cap infeasibility, PR #803)'; }
  if (c.rotBasis === 'plan3m' && dp2VerCmp_(DP2_ENGINE_NEED_.plan3m, need) > 0) { need = DP2_ENGINE_NEED_.plan3m; why = 'the plan3m rotation basis'; }
  c.engineNeed = need;
  if (!env.present) c.issues.push('Script Property DP2_ENGINE_ENV is not set — paste the TFB_OPP_* and TFB_T10_* variables of the Render service as KEY=VALUE lines (no secrets)');
  if (!c.fundingPlan) c.issues.push('TFB_OPP_FUNDING_PLAN is off in production — the engine plans no funding');
  if (c.unmodelled.length) c.issues.push('production settings this page does not model: ' + c.unmodelled.join(', '));
  if (!c.engineVersion) c.issues.push('the production engine version is not on the Top_10 status line');
  else if (dp2VerCmp_(c.engineVersion, need) < 0) c.issues.push('production runs opportunity_builder ' + c.engineVersion + ', older than ' + need + ' needed for ' + why);
  c.planReady = c.issues.length === 0;
  c.floorFor = function (sym) {
    if (!c.envPresent) return { sar: DP2A.MIN_TICKET_SAR, src: 'research' };
    var f = c.opFloor, src = 'operator';
    if (c.venueOn) { var vf = dp2VenueFloor_(sym); if (vf && vf > f) { f = vf; src = 'venue'; } }
    return { sar: f, src: src };
  };
  c.venueLot = function (sym) { var s = dp2Str_(sym).trim().toUpperCase(); if (s.indexOf('.') < 0) return 0; return c.venueLots[s.split('.').pop()] || 0; };
  c.sector = function (s) { var x = dp2Str_(s).trim(); x = (c.sectorNormalize && DP2_GICS_SECTOR_.hasOwnProperty(x)) ? DP2_GICS_SECTOR_[x] : x; return x || 'Unknown'; };   // candidate / holding sector, as the engine
  c.canonMarket = !/^(0|false|no|off)$/.test(low('TFB_OPP_CANON_MARKET'));
  c.marketKey = function (m) { var x = dp2Str_(m); if (!c.canonMarket) return x; var parts = x.split('/').map(function (q) { return q.trim().toUpperCase(); }).filter(function (q) { return !!q; }); if (!parts.length) return x.trim().toUpperCase(); parts.sort(); return parts.join('/'); };   // _market_cap_key
  var vlTxt = Object.keys(c.venueLots).length ? Object.keys(c.venueLots).map(function (k) { return k + ':' + c.venueLots[k]; }).join(',') : 'off';
  c.summary = env.present
    ? 'DP2_ENGINE_ENV (' + env.keys.length + ' TFB settings' + (env.ignored ? ', ' + env.ignored + ' other lines ignored' : '') + '): mode ' + c.sizingMode + (c.sizingMode === 'conviction' ? ' (tilt ' + c.tilt + ')' : '') +
      ' · cap ' + c.maxWeightPct + '% (' + c.capSource + ') · minimum ticket ' + dp2Fmt_(c.opFloor) + ' SAR' + (c.venueOn ? ' + venue floors' : '') + ' · lots ' + (c.lotSize > 1 ? c.lotSize + ' / ' : '') + vlTxt +
      (c.entryHigh ? ' · sized at the entry high' : '') + ' · sector cap ' + c.pfMaxSectorPct + '% on the ' + c.sectorBasis + ' basis · rotation ' + (c.rotBasis === 'plan3m' ? 'plan 3M' : 'engine 12M') + ' ' + rot.edge12 + ' / ' + rot.edge3 + ' pp, cost ' + rot.cost + '%' +
      ' · engine ' + (c.engineVersion || '?') + (c.planReady ? '' : ' — SWITCHES WITHHELD: ' + c.issues.join('; '))
    : 'production configuration NOT mirrored (Script Property DP2_ENGINE_ENV unset) — research sizing on engine defaults with a ' + dp2Fmt_(DP2A.MIN_TICKET_SAR) + ' SAR research floor; rotation plans withheld';
  return c;
}
function dp2TicketPolicy_(t) { return dp2ProdConfig_(t); }   // v1.2.18/19 name, kept for callers

function dp2WaterFill_(weights, budget, cap, floor) {
  budget = Math.max(0, budget || 0); cap = Math.max(0, cap || 0); floor = Math.max(0, floor || 0);
  var active = {}, any = false, sym;
  for (sym in weights) if (weights.hasOwnProperty(sym)) { any = true; var w = Number(weights[sym]); if (isFinite(w) && w > 0) active[sym] = w; }
  if (!Object.keys(active).length && any) { for (sym in weights) if (weights.hasOwnProperty(sym)) active[sym] = 1; }
  if (budget <= 0 || cap <= 0 || !Object.keys(active).length) return {};
  while (Object.keys(active).length) {
    var targets = {}, pool = {}, left = budget, k;
    for (k in active) if (active.hasOwnProperty(k)) pool[k] = active[k];
    for (var guard = 0; guard <= Object.keys(active).length; guard++) {
      var total = 0; for (k in pool) if (pool.hasOwnProperty(k)) total += pool[k];
      if (total <= 0 || left <= 0 || !Object.keys(pool).length) break;
      var capped = []; for (k in pool) if (pool.hasOwnProperty(k) && left * pool[k] / total >= cap - 1e-9) capped.push(k);
      if (capped.length) { for (var c = 0; c < capped.length; c++) { targets[capped[c]] = cap; delete pool[capped[c]]; } left = budget; for (k in targets) if (targets.hasOwnProperty(k)) left -= targets[k]; continue; }
      for (k in pool) if (pool.hasOwnProperty(k)) targets[k] = left * pool[k] / total;
      break;
    }
    var below = []; for (k in targets) if (targets.hasOwnProperty(k) && targets[k] < floor - 1e-9) below.push(k);
    if (!below.length) { var out = {}; for (k in targets) if (targets.hasOwnProperty(k) && targets[k] > 0) out[k] = Math.round(targets[k] * 100) / 100; return out; }
    below.sort(function (a, b) { return (active[a] - active[b]) || (a < b ? -1 : 1); });
    delete active[below[0]];
  }
  return {};
}
/* FX to SAR for a seat currency: USD = the account rate, SAR = 1, GBX = GBP/100, otherwise the live cross via the
 * chart API (CCYSAR=X, cached like any bar series) and, as a last resort, an approximate table flagged 'approx'. */
var DP2A_FX_APPROX_ = { EUR: 4.35, GBP: 5.05, JPY: 0.0248, HKD: 0.482, CHF: 4.72, CAD: 2.72, AUD: 2.50, SGD: 2.90, NOK: 0.37, SEK: 0.39, DKK: 0.58, MXN: 0.20, CNY: 0.52, INR: 0.045 };
function dp2FxToSar_(ccy, fx) {
  var c = dp2Str_(ccy).trim().toUpperCase();
  if (!c || c === 'SAR') return { rate: 1, source: 'peg' };
  if (c === 'USD') return { rate: fx, source: 'account' };
  var pence = (c === 'GBX' || c === 'GBP');
  var base = pence ? 'GBP' : c;
  var live = null;
  try { var b = dp2LoadBars_([base + 'SAR=X']); var e = b[base + 'SAR=X']; if (e && !e.stale && e.last) live = e.last; else if (e && e.bars && e.bars.length) live = e.bars[e.bars.length - 1].c; } catch (eF) { live = null; }
  if (live) return { rate: pence && c === 'GBX' ? live / 100 : live, source: 'chart API' };
  if (DP2A_FX_APPROX_.hasOwnProperty(base)) return { rate: c === 'GBX' ? DP2A_FX_APPROX_[base] / 100 : DP2A_FX_APPROX_[base], source: 'approx' };
  return { rate: null, source: 'none' };
}
/* Plan seats = FAST-TRACK seats in rank order within the sector / market caps (the engine's sizing cohort). */
/* v1.2.4: the cohort caps come from the Top_10 panel (T10: Max Selected / Max Per Sector / Max Per Market), DP2A only as fallback. */
function dp2CohortCaps_(t) {
  var pnl = (t && t.panel) || {};
  var n = function (k, d) { var v = dp2Num_(pnl[k]); return (v !== null && v > 0) ? Math.round(v) : d; };
  return { maxSelected: n('T10: Max Selected', DP2A.MAX_SELECTED), maxPerSector: n('T10: Max Per Sector', DP2A.MAX_PER_SECTOR), maxPerMarket: n('T10: Max Per Market', DP2A.MAX_PER_MARKET), fromPanel: dp2Num_(pnl['T10: Max Per Sector']) !== null };
}
function dp2PlanSeats_(t, fx, xbars, qmap, cfg) {
  var out = [], sectors = {}, markets = {};
  var caps = dp2CohortCaps_(t);
  cfg = cfg || dp2ProdConfig_(t);   // v1.2.20: _sizing_cohort — normalized sectors (the Unknown bucket exempt from the count cap) and canonical market keys
  for (var i = 0; i < t.seats.length; i++) {
    var s = t.seats[i];
    var stability = dp2Str_(s['Stability']);
    if (!/FAST-TRACK/.test(stability)) continue;
    if (out.length >= caps.maxSelected) break;
    var sym = dp2Str_(s['Symbol']).trim(), sec = cfg.sector(s['Sector']), mkt = dp2Str_(s['Market']).trim() || 'Unknown', mkey = cfg.marketKey(mkt);
    if (((sectors[sec] || 0) >= caps.maxPerSector && !(cfg.sectorNormalize && sec === 'Unknown')) || (markets[mkey] || 0) >= caps.maxPerMarket) continue;
    var q = (qmap && qmap[sym]) || {};
    var ccy = dp2Str_(s['Ccy']);
    var fxRow = dp2Num_(s['FX→SAR']);
    var fxUse = fxRow !== null ? fxRow : null, fxSource = fxRow !== null ? 'cockpit' : '';
    if (fxUse === null) { var fxr = dp2FxToSar_(ccy, fx); fxUse = fxr.rate; fxSource = fxr.source; }
    var xb = xbars && xbars[sym];
    var price = (xb && !xb.stale && xb.last) ? xb.last : dp2Num_(s['Price']);
    var atr = null;
    if (xb && xb.bars && xb.bars.length > DP2X.ATR_N) { var a = dp2Atr_(xb.bars, DP2X.ATR_N); atr = a[a.length - 1]; }
    var roi3 = dp2Num_(s['ROI % (TP1)']); if (roi3 === null) roi3 = dp2Num_(q['ROI % (TP1)']);
    var roi12 = dp2Num_(s['Engine ROI % (12M)']); if (roi12 === null) roi12 = dp2Num_(q['Engine ROI % (12M)']);
    // v1.2.4 (review finding): the conviction WEIGHT uses the engine's roi_pct = valuation (TP2) upside, (TP2 − price)/price,
    // read from the cockpit's TP2 SAR / Price SAR; TP1 sits at the midpoint by construction, so 2 × ROI % (TP1) is the fallback.
    var tp2Sar = dp2Num_(s['TP2 SAR']), pxSar = dp2Num_(s['Price SAR']);
    var roiVal = (tp2Sar !== null && pxSar !== null && pxSar > 0) ? (tp2Sar / pxSar - 1) * 100 : (roi3 !== null ? roi3 * 2 : null);
    var roiValSource = (tp2Sar !== null && pxSar !== null && pxSar > 0) ? 'tp2' : (roi3 !== null ? 'tp1x2' : 'none');
    var rel = dp2Num_(s['Rel']); if (rel === null) rel = dp2Num_(q['Rel']);
    var risk = dp2Str_(q['Risk']) || dp2Str_(s['Conf']) || '';
    sectors[sec] = (sectors[sec] || 0) + 1; markets[mkey] = (markets[mkey] || 0) + 1;
    out.push({ sym: sym, name: dp2Str_(s['Name']), sector: sec, market: mkt, ccy: ccy, fxUse: fxUse, fxSource: fxSource, price: price, priceSar: (price !== null && fxUse !== null) ? price * fxUse : null,
               roi3: roi3, roi12: roi12, roiVal: roiVal, roiValSource: roiValSource, rel: rel, dq: dp2Num_(s['DQ']), risk: risk, rank: dp2Num_(s['Rank']), stability: stability, atr: atr,
               stop0: (price !== null && atr !== null) ? price - DP2X.ATR_MULT_STOP * atr : null, ticketEngine: dp2Num_(s['Ticket SAR']) });
  }
  return out;
}
/* Allocation of the deployable cash across the plan seats (research). pvSar = holdings value; cashSar = cash; navSar = pv + cash. */
function dp2Allocate_(plans, cashSar, pvSar, secCtx, reserveSar, cfg, mode) {
  var cash = Math.max(0, cashSar || 0), pv = Math.max(0, pvSar || 0), nav = cash + pv;
  cfg = cfg || dp2ProdConfig_(null);   // v1.2.20: the production configuration (DP2_ENGINE_ENV), engine defaults + a research floor when unset
  mode = mode || 'conviction';          // the research view is the conviction rule; the funding plans use production's mode
  var opFloor = cfg.opFloor;
  // v1.2.9 / v1.2.20: the engine's post-action PORTFOLIO sector cap (criteria pf_max_sector_pct = TFB_OPP_PF_MAX_SECTOR_PCT)
  // on the configured basis (TFB_OPP_SECTOR_CAP_BASIS): budget = portfolio + cash before the reserve (budget_base),
  // pd = portfolio value, legacy = portfolio value + the ticket. Available when the portfolio has holdings.
  var secSar = (secCtx && secCtx.sectors) ? secCtx.sectors : null;
  var secMax = (secCtx && secCtx.maxPct !== undefined && secCtx.maxPct !== null) ? secCtx.maxPct : cfg.pfMaxSectorPct;
  var secAvail = !!(secSar && (!secCtx || secCtx.available !== false));
  var basis = cfg.sectorBasis || 'budget', budgetBase = pv + cash;
  var postTotal = function (add) { return basis === 'budget' ? budgetBase : (basis === 'pd' ? pv : pv + add); };
  var capBaseOk = basis === 'budget' ? budgetBase > 0 : pv > 0;
  var secPct = function (sec, add) { return ((secSar[sec] || 0) + add) / postTotal(add) * 100; };
  var secOk = function (sec, add) { if (!secAvail || !capBaseOk || !(add > 0)) return true; return secPct(sec, add) <= secMax + 1e-9; };                 // the funded-ticket check
  var secOkFloor = function (sec, floor) { if (!secAvail) return true; var tot = postTotal(floor); if (!(tot > 0)) return true; return ((secSar[sec] || 0) + floor) / tot * 100 <= secMax + 1e-9; };   // the replay guard before a need
  var secOkTop = function (sec, add) { if (!secAvail) return true; var tot = postTotal(add); if (!(tot > 0)) return true; return ((secSar[sec] || 0) + add) / tot * 100 <= secMax + 1e-9; };       // the top-up check
  // v1.2.15: the reserve is the ENGINE's (dp2CashReserve_); the PF 10 % figure is kept only when no reserve is given (legacy callers)
  var floorSar = (reserveSar !== undefined && reserveSar !== null) ? Math.max(0, reserveSar) : nav * DP2.CASH_FLOOR_PCT;
  var deployable = Math.max(0, cash - floorSar);
  var cap = Math.max(0, cfg.maxWeightPct / 100 * budgetBase);   // _size_one: max_weight_pct × budget_base (pre-reserve)
  // _size_one: shares floored to max(lot_size, venue lot), sized at price (× 1.01 with TFB_OPP_SIZE_AT_ENTRY_HIGH)
  var sizeOne = function (p, alloc) {
    var lot = Math.max(1, cfg.lotSize || 1, cfg.venueLot(p.sym)), px = p.priceSar * (cfg.entryHigh ? 1.01 : 1), sh = 0;
    alloc = Math.max(0, alloc);
    if (px > 0 && alloc >= px * lot) sh = Math.floor(alloc / (px * lot)) * lot;
    return { qty: sh, amt: sh * px };
  };
  var weights = {}, i, p;
  for (i = 0; i < plans.length; i++) { p = plans[i]; if (p.priceSar) weights[p.sym] = dp2ConvWeight_(p.roiVal, p.risk, p.rel, cfg.tilt, cfg.requiredRoiPct); }
  var targets = mode === 'conviction' ? dp2WaterFill_(weights, deployable, cap, opFloor) : {};   // _conviction_targets on the OPERATOR floor
  var rows = [], allocated = 0, gain3 = 0, riskSum = 0, riskKnown = 0, remaining = deployable, needs = [];
  var anyTarget = false; for (var tk in targets) if (targets.hasOwnProperty(tk)) anyTarget = true;
  for (i = 0; i < plans.length; i++) {
    p = plans[i];
    var sec = cfg.sector(p.sector);
    var target = targets.hasOwnProperty(p.sym) ? targets[p.sym] : 0;
    var r = { sym: p.sym, weight: weights[p.sym] || 0, target: target, qty: 0, alloc: 0, gain3: null, riskSar: null, state: '', note: '', sector: sec };
    if (!p.priceSar) { r.state = 'NO_FX'; r.note = 'no FX→SAR for ' + p.ccy; rows.push(r); continue; }
    var sized;
    if (mode === 'conviction') {
      if (target <= 0 && anyTarget) {
        r.state = 'RELEASED'; r.note = 'conviction sizing: weighted share below the minimum ticket ' + dp2Fmt_(opFloor) + ' SAR — budget released to higher-conviction seats (deferred, no funding need — as the engine)';
        rows.push(r); continue;
      }
      if (target <= 0 && remaining > 0 && remaining >= opFloor && cap < opFloor) {
        // v1.2.8 (engine v1.27.3): the cohort is empty because the per-seat cap is below the minimum ticket while cash is there
        r.state = 'CAP_INFEASIBLE'; r.note = 'per-seat cap ' + dp2Fmt_(cap) + ' SAR below the minimum ticket ' + dp2Fmt_(opFloor) + ' SAR — infeasible under the cap (raise Max Weight % or the cash); deferred, no funding need';
        rows.push(r); continue;
      }
      sized = target > 0 ? sizeOne(p, Math.min(cap, Math.min(remaining, target))) : { qty: 0, amt: 0 };   // an empty cohort = capital exhaustion (v1.27.2): a 0-SAR pick
    } else {
      sized = sizeOne(p, Math.min(cap, remaining));   // cap mode: the first seats take the cap, the rest the remainder
    }
    // v1.11.0 [F-1]: a venue board lot alone priced the name out (one share affordable, one lot not) — deferral, no need
    var vlot = cfg.venueLot(p.sym);
    if (vlot > 1 && sized.qty === 0) {
      var lotAlloc = Math.max(0, Math.min(cap, remaining));
      if (p.priceSar > 0 && lotAlloc >= p.priceSar) { r.state = 'LOT_DEFERRED'; r.note = 'one board lot (' + dp2Fmt_(vlot) + ' sh ≈ ' + dp2Fmt_(p.priceSar * vlot) + ' SAR) exceeds the sized allocation ' + dp2Fmt_(lotAlloc) + ' SAR — deferred, no funding need (as the engine)'; rows.push(r); continue; }
    }
    var ef = cfg.floorFor(p.sym);
    if (ef.sar > sized.amt && !secOkFloor(sec, ef.sar)) { r.state = 'SECTOR_CAP'; r.note = dp2SectorFloorNote_(sec, ef.sar, secSar, postTotal(ef.sar), secMax); rows.push(r); continue; }   // v1.2.19: the replay guard before a need
    if (ef.sar > 0 && sized.amt > 0 && sized.amt < ef.sar - 1e-9) {
      r.state = 'NEED'; r.need = ef.sar; r.short = Math.max(0, ef.sar - remaining);
      r.note = 'sized ticket ' + dp2Fmt_(sized.amt) + ' SAR below minimum ticket floor ' + dp2Fmt_(ef.sar) + ' SAR' + dp2FloorNote_(ef, cfg);
      needs.push({ sym: p.sym, need: ef.sar, short: r.short, roi12: p.roi12, roi3: p.roi3, plan: p, why: r.note });
      rows.push(r); continue;
    }
    if (sized.amt > 0 && !secOk(sec, sized.amt)) {
      r.state = 'SECTOR_CAP'; r.note = 'post-action sector weight ' + dp2PctPlain_(secPct(sec, sized.amt), 1) + ' would exceed the sector cap ' + secMax + '% (' + sec + ', ' + basis + ' basis) — deferred, as the engine; no funding need';
      rows.push(r); continue;
    }
    if (!(sized.amt > 0)) {   // a 0-SAR pick: capital exhausted — the need is the seat's effective floor, none when it is 0
      if (!(ef.sar > 0)) { r.state = 'UNFUNDED'; r.note = 'capital exhausted — no minimum ticket in force (TFB_OPP_MIN_TICKET_SAR 0), so the engine records no funding need'; rows.push(r); continue; }
      r.state = 'NEED'; r.need = ef.sar; r.short = Math.max(0, ef.sar - remaining);
      r.note = (remaining > 0 ? 'the sized amount ' + dp2Fmt_(Math.min(cap, remaining)) + ' SAR buys no ' + (vlot > 1 ? 'lot' : 'share') : 'capital exhausted — deployable ' + dp2Fmt_(deployable) + ' SAR below one minimum ticket') + dp2FloorNote_(ef, cfg);
      needs.push({ sym: p.sym, need: ef.sar, short: r.short, roi12: p.roi12, roi3: p.roi3, plan: p, why: r.note });
      rows.push(r); continue;
    }
    r.qty = sized.qty; r.alloc = sized.amt; r.state = 'FUNDED'; r.plan = p;
    if (secSar) secSar[sec] = (secSar[sec] || 0) + r.alloc;
    remaining -= r.alloc;
    rows.push(r);
  }
  // v1.2.7: the conviction TOP-UP pass (engine v1.27.0) — the remainder goes to the funded seats in conviction order
  // (weight desc, symbol), each within the per-seat cap and the sector cap, through the same sizing (lots, entry high).
  var topup = 0;
  if (mode === 'conviction' && remaining > 0) {
    var funded = rows.filter(function (r0) { return r0.state === 'FUNDED'; }).sort(function (a1, b1) { return (b1.weight - a1.weight) || (a1.sym < b1.sym ? -1 : 1); });
    for (i = 0; i < funded.length && remaining > 0; i++) {
      var fr = funded[i], room = cap - fr.alloc; if (room <= 0) continue;
      var add = sizeOne(fr.plan, Math.min(cap, Math.min(remaining, room))); if (add.qty <= 0 || add.amt <= 0) continue;
      if (!secOkTop(fr.sector, add.amt)) continue;
      fr.qty += add.qty; fr.alloc += add.amt; fr.topup = (fr.topup || 0) + add.amt; remaining -= add.amt; topup += add.amt;
      if (secSar) secSar[fr.sector] = (secSar[fr.sector] || 0) + add.amt;
    }
  }
  for (i = 0; i < rows.length; i++) {
    var rr = rows[i]; if (rr.state !== 'FUNDED') continue;
    rr.gain3 = rr.plan.roi3 !== null ? rr.alloc * rr.plan.roi3 / 100 : null;
    rr.riskSar = rr.plan.stop0 !== null ? rr.qty * (rr.plan.price - rr.plan.stop0) * rr.plan.fxUse : null;
    allocated += rr.alloc; if (rr.gain3 !== null) gain3 += rr.gain3; if (rr.riskSar !== null) { riskSum += rr.riskSar; riskKnown++; }
  }
  var byRow = {}; for (i = 0; i < rows.length; i++) byRow[rows[i].sym] = rows[i];
  return { cash: cash, pv: pv, nav: nav, floorSar: floorSar, deployable: deployable, cap: cap, weights: weights, targets: targets, rows: rows, byRow: byRow,
           allocated: allocated, cashAfter: cash - allocated, gain3: gain3, riskSar: riskKnown ? riskSum : null, riskKnown: riskKnown, needs: needs, plans: plans, topup: topup, remaining: remaining,
           ticketPolicy: cfg, cfg: cfg, opFloor: opFloor, mode: mode, sectorMax: secMax };
}
/* v1.2.19 (review finding): _select_and_size (replay) tests the MINIMUM TICKET against the post-action sector cap before it
 * records a funding need; an inadmissible ticket is a diversification deferral with no funding plan. */
function dp2SectorFloorNote_(sec, floorSar, secSar, tot, secMax) {
  return 'Diversification: the minimum ticket ' + dp2Fmt_(floorSar) + ' SAR would take ' + sec + ' to ' + dp2PctPlain_(((secSar[sec] || 0) + floorSar) / tot * 100, 1) + ' > the sector cap ' + secMax + '% — deferred, no funding need (as the engine)';
}
function dp2FloorNote_(ef, cfg) {
  if (ef.src === 'venue') return ' (venue floor; operator floor ' + dp2Fmt_(cfg.opFloor) + ' SAR)';
  if (ef.src === 'research') return ' (research floor — DP2_ENGINE_ENV not set)';
  return '';
}
/* Rotation candidates — mirror of _rotation_pick / _funding_plans. holdings: [{symbol, valueSar, roi12, roi3, holdingDays, tp1, price, isFI}].
 * basis '12m' uses engine 12M forecasts (production rule); '3m' uses the plan / expected 3M ROI (proposed TFB_OPP_ROTATION_BASIS=plan3m). */
/* v1.2.17: the rotation test uses PRODUCTION's parameters — v1.2.20: read from DP2_ENGINE_ENV by dp2ProdConfig_ (hurdles,
 * cost, minimum holding days, TP1 proximity, exclude list, synthetic-forecast tokens); dp2RotParamsDefault_ = engine defaults. */
function dp2RotParamsDefault_() {
  var p = { edge12: DP2A.ROT_EDGE_PP_12M, edge3: DP2A.ROT_EDGE_PP_3M, cost: DP2A.ROT_COST_PCT, minHeld: DP2A.ROT_MIN_HELD_DAYS, tp1Prox: DP2A.ROT_TP1_PROX_PCT, exclude: DP2A.ROT_EXCLUDE.slice(), synthetic: DP2_DEFAULT_SYNTHETIC_.slice(), source: 'engine defaults' };
  p.hurdle = function (basis) { return (basis === '3m' || basis === 'plan3m') ? p.edge3 : p.edge12; };
  return p;
}
function dp2RotParams_(cfg) { return (cfg || dp2ProdConfig_(null)).rot; }
function dp2RotationPick_(oppRoi, holdings, basis, minValue, P) {
  if (oppRoi === null || oppRoi === undefined) return null;
  P = P || dp2RotParams_();   // v1.2.17 / v1.2.20: production parameters (DP2_ENGINE_ENV), engine defaults otherwise
  var edgeMin = P.hurdle(basis);
  var best = null;
  for (var i = 0; i < holdings.length; i++) {
    var h = holdings[i];
    if (h.isFI || !h.valueSar || h.valueSar <= 0) continue;
    if (h.degraded) continue;   // v1.2.20: the floor guard drops a holding whose forecast is synthesized or degraded (_holding_roi_map)
    if (P.exclude.indexOf(dp2Str_(h.symbol).trim().toUpperCase()) >= 0) continue;
    if (h.holdingDays !== null && h.holdingDays !== undefined && h.holdingDays < P.minHeld) continue;
    if (h.tp1 && h.price && h.tp1 > 0) { var gap = (h.tp1 - h.price) / h.tp1 * 100; if (gap >= 0 && gap <= P.tp1Prox) continue; }
    if (minValue && h.valueSar < minValue - 0.5) continue;   // v1.2.8: eligibility depends on the shortfall only (engine R1); the remnant rule (R2) handles a small holding
    var r = basis === '3m' ? h.roi3 : h.roi12;
    if (r === null || r === undefined) continue;
    var edge = oppRoi - r - P.cost;
    if (edge < edgeMin) continue;
    if (best === null || r < best.roi) best = { symbol: h.symbol, valueSar: h.valueSar, roi: r, edge: Math.round(edge * 10) / 10, basis: basis };
  }
  return best;
}
/* Funding plans for the unfunded plan seats (rank order). need = a full seat (per-seat cap). Cash left after the
 * allocation covers needs first; then ONE rotation per run with swap semantics: the proceeds of the exited (or, above
 * the cap, trimmed) holding plus the cash left form the ticket; the rest are capital calls. */
function dp2FundingPlans_(needs, remaining, holdings, basis, P) {
  P = P || dp2RotParams_();   // v1.2.17 / v1.2.20
  var plans = [], avail = Math.max(0, remaining || 0), used = false, insNoted = false;   // v1.2.21: rotation_insufficient is noted once per run, as the engine
  for (var i = 0; i < needs.length; i++) {
    var n = needs[i], need = n.need || 0; if (need <= 0) continue;
    var opp = basis === '3m' ? n.roi3 : n.roi12;
    var plan = { sym: n.sym, need: need, short: 0, state: 'FUNDABLE_NOW', rotation: null, basis: basis, oppRoi: opp, why: n.why || '', hurdle: P.hurdle(basis), cost: P.cost };
    // v1.2.6 (review finding): FUNDABLE_NOW only when the cash left covers the WHOLE need (engine: short = need − avail;
    // avail −= need); a partial amount is a shortfall that goes to the rotation / capital-call path, never a smaller ticket.
    var short = Math.max(0, need - avail);
    var cashUsed = Math.min(avail, need);
    avail = Math.max(0, avail - need);
    if (short <= 0) { plan.ticket = need; plans.push(plan); continue; }
    plan.short = short;
    // v1.2.3 (review finding): the holding must cover the shortfall (the engine's floor guard passes the shortfall as
    // min_value_sar); a smaller eligible holding is reported as rotation-insufficient and the seat stays a capital call.
    var rot = used ? null : dp2RotationPick_(opp, holdings, basis, short, P);
    if (rot) {
      used = true;
      var proceeds = Math.min(rot.valueSar, short);   // v1.2.4: sell only the shortfall — the cash left funds the rest (engine semantics)
      rot.action = proceeds >= rot.valueSar - 0.5 ? 'EXIT' : 'TRIM';
      var remFloor = (P.remnantFloor !== undefined && P.remnantFloor !== null) ? P.remnantFloor : DP2A.MIN_TICKET_SAR;   // v1.2.18: the OPERATOR floor (engine floor_sar=_min_floor)
      if (rot.action === 'TRIM' && (rot.valueSar - proceeds) < remFloor - 0.5) { rot.action = 'EXIT'; proceeds = rot.valueSar; rot.remnantExit = true; }
      rot.proceeds = proceeds;
      plan.ticket = Math.min(need, proceeds + cashUsed);
      rot.surplus = Math.max(0, proceeds + cashUsed - plan.ticket);
      // v1.2.14 (review finding): the engine records the surplus but does not recycle it to later needs in the same run
      plan.rotation = rot; plan.short = Math.max(0, need - plan.ticket); plan.state = 'FUNDABLE_BY_ROTATION';
    } else {
      plan.state = 'CAPITAL_CALL';
      plan.ticket = 0;
      plan.cashHeld = cashUsed;   // the cash left that this seat would have used stays reserved for it (engine: avail −= need)
      plan.rotationTaken = used;
      var ins = null;
      if (!used) { for (var k = 0; k < holdings.length; k++) { var h = holdings[k]; if (h.isFI || !h.valueSar || h.degraded) continue; var rr = basis === '3m' ? h.roi3 : h.roi12; if (rr === null || rr === undefined || opp === null) continue; var e = opp - rr - P.cost; if (ins === null || e > ins.edge) ins = { symbol: h.symbol, edge: Math.round(e * 10) / 10, roi: rr }; } }
      if (ins) plan.bestHolding = ins;
      // an eligible holding that clears the hurdle but is worth less than the shortfall (engine: rotation_insufficient)
      var small = (used || insNoted) ? null : dp2RotationPick_(opp, holdings, basis, 0, P);
      if (small && small.valueSar < short - 0.5) { plan.rotationInsufficient = { symbol: small.symbol, covers: small.valueSar, short: short, edge: small.edge, roi: small.roi }; insNoted = true; }
    }
    plans.push(plan);
  }
  return plans;
}
/* v1.2.21 (review finding): the engine's capital call — the KPI capital_call_topn_sar and the capital_call alert take the
 * FIRST TFB_OPP_CAPITAL_CALL_TOPN plans of the rank-ordered funding plans (any state) and sum their shortfalls; the alert
 * names those with a shortfall. */
function dp2CapitalCall_(plans, topN) {
  var n = topN > 0 ? topN : 3, first = (plans || []).slice(0, n), sum = 0, syms = [];
  for (var i = 0; i < first.length; i++) { var sh = Math.round(Math.max(0, first[i].short || 0)); sum += sh; if (sh > 0) syms.push(first[i].sym); }
  return { sum: sum, syms: syms, topN: n };
}
/* v1.2.21 (review finding on 736ccd6): the Portfolio page's unfunded-plans / capital-call action, computed ONCE per page —
 * it used to hang off lot 0's rotation branch and was skipped whenever lot 0 was itself a rotation candidate. Production
 * basis only; the capital call is the engine's top-N (dp2CapitalCall_); an eligible holding that clears the hurdle but
 * cannot cover the shortfall is named (rotation insufficient); no action when the cash left covers every need. */
function dp2UnfundedAction_(alloc) {
  var needs = alloc ? (alloc.needsProd || alloc.needs || []) : [];
  if (!needs.length) return null;
  var needSyms = needs.map(function (n) { return n.sym; }).join(', ');
  if (alloc.fundingWithheld) return { p: 4, t: 'Unfunded plans ' + needSyms + ': ' + alloc.fundingWithheld + '.' };   // v1.2.18
  var plans = alloc.plansProd || [];
  var anyShort = plans.some(function (pl) { return (pl.short || 0) > 0.5; });
  if (!anyShort) return null;   // the cash left covers every need (FUNDABLE_NOW) — nothing is asked of the operator
  var ccA = dp2CapitalCall_(plans, alloc.cfg ? alloc.cfg.capitalCallTopN : 3);   // top-N, as the engine's KPI and alert
  var ccTxt = ccA.sum > 0 ? 'capital call (top-' + ccA.topN + ', as the engine): deposit ≥ ' + dp2Fmt_(ccA.sum) + ' SAR to take ' + ccA.syms.join(', ') : '';
  if (alloc.rotationProd) return ccTxt ? { p: 4, t: 'Unfunded plans beyond this run\'s single rotation: ' + ccTxt + ', or wait for cash.' } : null;
  var insP = null; plans.forEach(function (pl) { if (!insP && pl.rotationInsufficient) insP = pl.rotationInsufficient; });
  var hurdleTxt = dp2RotationBasisLabel_(alloc.basis) + ', ' + (alloc.rotParams ? alloc.rotParams.hurdle(alloc.basis) : (alloc.basis === 'plan3m' ? DP2A.ROT_EDGE_PP_3M : DP2A.ROT_EDGE_PP_12M)) + ' pp after cost';
  if (insP) return { p: 4, t: 'Unfunded plans ' + needSyms + ': ' + insP.symbol + ' clears the rotation hurdle (edge +' + dp2Fmt_(insP.edge, 1) + ' pp, ' + hurdleTxt + ') but is worth ' + dp2Fmt_(insP.covers) + ' SAR against a ' + dp2Fmt_(insP.short) + ' SAR shortfall — rotation insufficient, no sale advised' + (ccTxt ? ' → ' + ccTxt : '') + ', or wait for cash.' };
  return { p: 4, t: 'Unfunded plans ' + needSyms + ': no holding trails them by the rotation hurdle on the production basis (' + hurdleTxt + ')' + (ccTxt ? ' → ' + ccTxt : '') + ', or wait for cash.' };
}
function dp2PlanText_(pl) {
  var b = pl.basis === '3m' ? 'plan 3M' : 'engine 12M';
  var hurdle = pl.hurdle !== undefined ? pl.hurdle : (pl.basis === '3m' ? DP2A.ROT_EDGE_PP_3M : DP2A.ROT_EDGE_PP_12M);
  var costP = pl.cost !== undefined ? pl.cost : DP2A.ROT_COST_PCT;   // v1.2.17: the production parameters the plan was built with
  if (pl.state === 'FUNDABLE_NOW') return pl.sym + ' — FUNDABLE NOW from the cash left: ticket ' + dp2Fmt_(pl.ticket) + ' SAR (the minimum ticket, as the engine)' + (pl.why ? ' (' + pl.why + ')' : '');
  if (pl.state === 'FUNDABLE_BY_ROTATION') {
    var r = pl.rotation;
    return pl.sym + ' — FUNDABLE BY ROTATION: ' + r.action.toLowerCase() + ' ' + r.symbol + ' (' + dp2Fmt_(r.proceeds) + ' SAR' + (pl.cashHeld || (pl.ticket - r.proceeds) > 0.5 ? ' + ' + dp2Fmt_(pl.ticket - r.proceeds) + ' SAR cash left' : '') + ') → ticket ' + dp2Fmt_(pl.ticket) + ' SAR (minimum ticket) · ' + b + ': ' + pl.sym + ' ' + dp2PctPlain_(pl.oppRoi, 1) + ' vs ' + r.symbol + ' ' + dp2PctPlain_(r.roi, 1) + ' → edge +' + dp2Fmt_(r.edge, 1) + ' pp after ' + costP + '% cost (hurdle ' + hurdle + ')' +
      (pl.short > 0 ? '; a full seat would need ' + dp2Fmt_(pl.short) + ' SAR more' : '') + (r.remnantExit ? ' (a trim would leave a sub-floor remnant → full exit' + (r.surplus ? ', surplus ' + dp2Fmt_(r.surplus) + ' SAR to cash' : '') + ')' : '');
  }
  if (pl.state === 'CAPITAL_CALL') return pl.sym + ' — CAPITAL CALL: ' + (pl.rotationTaken ? 'no cash left and this run\'s single rotation is already assigned (one switch per run, as in the engine)' : (pl.rotationInsufficient ? pl.rotationInsufficient.symbol + ' clears the hurdle (edge +' + dp2Fmt_(pl.rotationInsufficient.edge, 1) + ' pp, ' + b + ') but is worth ' + dp2Fmt_(pl.rotationInsufficient.covers) + ' SAR against a ' + dp2Fmt_(pl.rotationInsufficient.short) + ' SAR shortfall — rotation insufficient (floor guard), no sale advised' : 'no cash and no holding trails it by ≥ ' + hurdle + ' pp (' + b + ') after cost' + (pl.bestHolding ? ' — closest: ' + pl.bestHolding.symbol + ' ' + dp2PctPlain_(pl.bestHolding.roi, 1) + ' (edge ' + (pl.bestHolding.edge >= 0 ? '+' : '') + dp2Fmt_(pl.bestHolding.edge, 1) + ' pp)' : ''))) + '; a seat needs ' + dp2Fmt_(pl.need) + ' SAR';
  return pl.sym + ' — ' + pl.state;
}
/* Holdings for the rotation rule, from the ledger lots + My_Portfolio forward ROI + the working broker TP. */
function dp2RotationHoldings_(eq, fi, ib, qmap, seatMap, cfg) {
  var out = [], i;
  var rotP = (cfg && cfg.rot) || dp2RotParamsDefault_();
  // v1.2.14 (review finding): one rotation holding per SYMBOL — the engine evaluates the My_Portfolio position; lots of
  // one symbol are aggregated (value summed, the oldest entry for the min-held-days rule, the lot count kept for the text)
  var bySym = {}, order = [];
  for (i = 0; i < eq.length; i++) {
    var e0 = eq[i], k0 = e0.symbol;
    if (!bySym[k0]) { bySym[k0] = { symbol: k0, name: e0.name, mp: e0.mp, fx: e0.fx, price: e0.price, mvSar: 0, holdingDays: null, lots: 0, buyDate: null }; order.push(k0); }
    var agg = bySym[k0]; agg.mvSar += (e0.mvSar || 0); agg.lots++;
    if (e0.holdingDays !== null && e0.holdingDays !== undefined && (agg.holdingDays === null || e0.holdingDays > agg.holdingDays)) agg.holdingDays = e0.holdingDays;
    if (!agg.price && e0.price) agg.price = e0.price; if (!agg.fx && e0.fx) agg.fx = e0.fx; if (!agg.mp && e0.mp) agg.mp = e0.mp;
  }
  var eqAgg = []; for (i = 0; i < order.length; i++) eqAgg.push(bySym[order[i]]);
  for (i = 0; i < eqAgg.length; i++) {
    var l = eqAgg[i], m = l.mp || {};
    var br = dp2BracketFor_(ib.orders, dp2YahooKey_(l.symbol));
    var tp1 = ib.ordersKnown ? br.tp : null;
    // v1.2.5 (review finding): the 3M figure is the PLAN-TP1 ROI, as _tp1_plan_roi on both sides of the engine's plan3m
    // basis — the cockpit's own TP1 ladder for the held name first, the working broker TP1 second; a holding with
    // neither has no 3M figure and never rotates on that basis. The My_Portfolio model forecast is display-only.
    var row = (qmap && qmap[l.symbol]) || (seatMap && seatMap[l.symbol]) || null;
    var roi3 = row ? dp2Num_(row['ROI % (TP1)']) : null, roi3Src = roi3 !== null ? 'cockpit TP1' : '';
    // v1.2.6 (review finding): the plan TP1 for the proximity guard comes from the cockpit row (TP1 SAR via the lot's
    // FX, else price × (1 + ROI % (TP1))) independently of broker-order freshness; the working broker TP1 is the fallback.
    var planTp1 = null;
    if (row) { var tp1Sar = dp2Num_(row['TP1 SAR']); var lfx1 = l.fx || null; if (tp1Sar !== null && lfx1) planTp1 = tp1Sar / lfx1; else if (roi3 !== null && l.price) planTp1 = l.price * (1 + roi3 / 100); }
    if (planTp1 === null) planTp1 = tp1;
    if (roi3 === null && tp1 && l.price) { roi3 = (tp1 / l.price - 1) * 100; roi3Src = 'broker TP1'; }
    // v1.2.20: _holding_forecast_degraded — a synthesized forecast source or a degraded-forecast warning never serves as a rotation baseline
    var fsrc = dp2NormToken_(m.forecastSource), wtxt = dp2Str_(m.warnings).toLowerCase();
    var degraded = (fsrc !== '' && rotP.synthetic.indexOf(fsrc) >= 0) || DP2_DEGRADED_WARN_.some(function (w) { return wtxt.indexOf(w) >= 0; });
    out.push({ symbol: l.symbol, valueSar: l.mvSar || 0, lots: l.lots || 1, roi12: dp2PctNum_(m.roi12m), roi3: roi3, roi3Src: roi3Src, roi3Model: dp2PctNum_(m.roi3m), holdingDays: l.holdingDays, tp1: planTp1, brokerTp1: tp1, price: l.price, isFI: false, degraded: degraded, forecastSource: dp2Str_(m.forecastSource) });
  }
  for (i = 0; i < fi.length; i++) out.push({ symbol: fi[i].symbol, valueSar: fi[i].mvSar || 0, roi12: null, roi3: null, holdingDays: fi[i].holdingDays, tp1: null, price: fi[i].price, isFI: true });
  return out;
}
/* v1.2.14 (review finding): the PRODUCTION rotation basis. Read from the Top_10 KPI row when the cockpit publishes
 * it ('Rotation Basis' = 'engine 12M' | 'plan 3M'), else TFB_OPP_ROTATION_BASIS from DP2_ENGINE_ENV (v1.2.20), else
 * '12m' — the engine's default when TFB_OPP_ROTATION_BASIS is unset. Set the property together with the Render ENV. */
/* v1.2.21: the production rotation basis. With DP2_ENGINE_ENV present the CURRENT configuration decides
 * (TFB_OPP_ROTATION_BASIS, unset = 12m) — the last sync's "Rotation Basis" KPI may predate an ENV change and is only
 * compared, as the cash floor (dp2CashReserve_); without the paste the KPI is used, else the engine default. */
function dp2RotationBasisInfo_(t, cfg) {
  cfg = cfg || dp2ProdConfig_(t);
  var k = (t && t.kpi) || {}, v = '';
  for (var kk in k) { if (k.hasOwnProperty(kk) && /^rotation basis/i.test(kk)) v = dp2Str_(k[kk]); }
  var kb = v ? ((/3m|plan3m|plan 3m/.test(v.trim().toLowerCase())) ? 'plan3m' : '12m') : '';
  if (cfg.envPresent) return { basis: cfg.rotBasis, src: 'env', kpi: kb, differs: !!kb && kb !== cfg.rotBasis };
  if (kb) return { basis: kb, src: 'kpi', kpi: kb, differs: false };
  return { basis: cfg.rotBasis, src: 'default', kpi: '', differs: false };
}
function dp2RotationBasis_(t, cfg) { return dp2RotationBasisInfo_(t, cfg).basis; }
function dp2RotationBasisSrcTxt_(bi) {
  if (!bi) return 'engine default';
  var kv = bi.basis === 'plan3m' ? 'TFB_OPP_ROTATION_BASIS=plan3m' : 'TFB_OPP_ROTATION_BASIS unset / 12m';
  if (bi.src === 'env') return kv + ', from DP2_ENGINE_ENV' + (bi.differs ? ' — the last sync\'s Rotation Basis KPI reports ' + dp2RotationBasisLabel_(bi.kpi) + '; the next sync will match' : '');
  if (bi.src === 'kpi') return 'the last sync\'s Rotation Basis KPI — DP2_ENGINE_ENV not set';
  return 'engine default — DP2_ENGINE_ENV not set';
}
function dp2RotationBasisLabel_(b) { return b === 'plan3m' ? 'plan 3M' : 'engine 12M'; }
/* v1.2.15 (review finding): the CASH RESERVE the engine takes from the SIZING budget — never a hard-coded 10 %.
 * Engine rule (_select_and_size / _cash_floor_pct_ctx): the reserve binds sizing only in ENFORCE mode —
 *   pct set   → floor = max(TFB_OPP_CASH_FLOOR_SAR, pct × NAV), NAV = holdings + cash; reserve = min(cash, floor)
 *   pct unset → an absolute TFB_OPP_CASH_FLOOR_SAR binds only with an explicit TFB_OPP_CASH_FLOOR_MODE=enforce
 *   observe / off → nothing is taken from sizing (an absolute reserve then only relabels funding).
 * Inputs, in order: the engine's own cash_floor alert on the Top_10 sheet (mode, pct, absolute floor — emitted on
 * every build that has a floor in observe/enforce) → TFB_OPP_CASH_FLOOR_SAR / _PCT / _MODE from DP2_ENGINE_ENV (v1.2.20), mirroring
 * the Render ENV → none. The rule is re-applied to the CURRENT cash and NAV. The PF page's 10 % floor stays a
 * LIMITS check on Portfolio_Decision_v2, not a sizing input. */
function dp2CashReserve_(t, cashSar, navSar, cfg) {
  var cash = Math.max(0, cashSar || 0), nav = Math.max(0, navSar || 0);
  var out = { sar: 0, floor: 0, mode: 'off', abs: null, pct: null, source: '', pfFloor: nav * DP2.CASH_FLOOR_PCT };
  var num = function (v) { var n = dp2Num_(dp2Str_(v).replace(/,/g, '')); return n; };
  var cf = (t && t.alerts && t.alerts.cash_floor) ? dp2Str_(t.alerts.cash_floor.text) : '';
  // the last sync's alert (mode, pct, absolute floor)
  var al = null;
  if (cf) {
    al = { mode: /ENFORCED/i.test(cf) ? 'enforce' : (/\(observe\)/i.test(cf) ? 'observe' : null), abs: null, pct: null };
    var mAbs = /^Cash floor\s+([\d,\.]+)\s*SAR\s*\(absolute reserve/i.exec(cf);
    var mPct = /^Cash floor\s+([\d\.]+)%\s+of NAV\s+([\d,\.]+)\s*SAR\s*=\s*([\d,\.]+)\s*SAR/i.exec(cf);
    if (mAbs) al.abs = num(mAbs[1]);
    else if (mPct) {
      al.pct = num(mPct[1]);
      if (/absolute reserve TFB_OPP_CASH_FLOOR_SAR is the stricter/i.test(cf)) al.abs = num(mPct[3]);
      else { var mLo = /absolute reserve TFB_OPP_CASH_FLOOR_SAR\s+([\d,\.]+)\s*SAR\s+is lower/i.exec(cf); if (mLo) al.abs = num(mLo[1]); }   // v1.2.17: engine v1.27.4 names a lower absolute reserve
    }
  }
  // v1.2.21 (review finding): with DP2_ENGINE_ENV present, the CURRENT production configuration decides — a last-sync
  // alert may predate an ENV change. Parsed as _cash_floor_sar / _env_cash_floor_pct / _env_cash_floor_mode /
  // _cash_floor_pct_ctx: a percent floor follows its mode (observe by default); an absolute floor alone binds only with
  // an explicit enforce; floor = max(absolute, pct × NAV). Without the paste, the alert is used as before.
  var env = dp2EngineEnv_(), abs = null, pct = null, mode = null, src = '';
  if (env.present) {
    var ev = function (k) { return env.map.hasOwnProperty(k) ? dp2Str_(env.map[k]).trim() : ''; };
    var ra = ev('TFB_OPP_CASH_FLOOR_SAR'), rp = ev('TFB_OPP_CASH_FLOOR_PCT').replace(/,/g, '').replace(/%$/, ''), rm = ev('TFB_OPP_CASH_FLOOR_MODE').toLowerCase();
    var av = ra === '' ? 0 : num(ra); abs = (av !== null && av > 0) ? av : null;
    var pv0 = (rp !== '' && DP2_NUM_RE_.test(rp)) ? Number(rp) : null; pct = (pv0 !== null && pv0 > 0 && pv0 < 100) ? pv0 : null;
    var em = rm === 'enforce' ? 'enforce' : 'observe';
    mode = pct !== null ? em : ((abs !== null && em === 'enforce') ? 'enforce' : null);
    src = 'DP2_ENGINE_ENV (current production configuration)';
    if (al) {
      var same = (al.mode || null) === mode && ((al.pct === null && pct === null) || al.pct === pct) && (mode === null || al.pct !== null || (al.abs || 0) === (abs || 0));
      if (!same) { out.alertDiffers = true; src += ' — the last sync\'s cash_floor alert describes a different floor; the next sync will match'; }
    }
  } else if (al) {
    abs = al.abs; pct = al.pct; mode = al.mode; src = 'engine cash_floor alert (last sync)';
  } else if (t && t.kpiDeployable !== null && t.kpiDeployable !== undefined && t.panelCash !== null && t.panelCash !== undefined) {
    // v1.2.22: no paste and no alert — the last build's own deployable KPI shows the reserve it took (deployable =
    // cash + proceeds − reserve). The build does not show whether that reserve binds sizing, so it is treated as
    // binding (conservative: research never spends cash production may keep).
    var obs = Math.max(0, (t.panelCash || 0) + Math.max(0, t.panelProceeds || 0) - t.kpiDeployable);
    if (obs > 0.5) { abs = Math.round(obs); mode = 'enforce'; out.observed = true; src = 'the last build\'s deployable KPI (panel cash ' + dp2Fmt_(t.panelCash) + (t.panelProceeds ? ' + proceeds ' + dp2Fmt_(t.panelProceeds) : '') + ' − deployable ' + dp2Fmt_(t.kpiDeployable) + '; its mode is not shown — treated as binding)'; }
  }
  out.absUnstated = !!(!env.present && cf && pct !== null && abs === null);
  out.abs = abs; out.pct = pct; out.mode = mode || 'off';
  if (mode === 'enforce') {
    out.floor = pct !== null ? Math.max(abs || 0, pct / 100 * nav) : (abs || 0);
    out.sar = Math.min(cash, out.floor);
    out.source = src + ': ' + (pct !== null ? pct + '% of NAV ' + dp2Fmt_(nav) + (abs ? ' or the absolute ' + dp2Fmt_(abs) + ', the stricter' : '') : 'absolute reserve ' + dp2Fmt_(abs || 0)) + ' ENFORCED → ' + dp2Fmt_(out.sar) + ' SAR kept out of sizing' + (out.absUnstated ? ' (the alert names no absolute reserve; an engine before 1.27.4 omits a lower one — paste DP2_ENGINE_ENV to use the configured value)' : '');
  } else if (mode === 'observe') {
    out.source = src + ': floor in OBSERVE mode — nothing taken from the sizing budget';
  } else {
    out.source = src ? src + ': no binding reserve' : 'no cash_floor alert on the Top_10 sheet and no DP2_ENGINE_ENV — the engine takes no reserve';
  }
  return out;
}
/* One shared research allocation per execution: plans → allocation → funding plans on both bases. */
var DP2A_MEMO_ = null;   // { key: '<cash>|<fx>', alloc } — v1.2.2: a different cash figure or fx recomputes instead of returning the memo
function dp2ResearchAllocation_(t, led, mp, ib, cashSar, fx, xbars) {
  var memoKey = String(Math.round((cashSar || 0) * 100) / 100) + '|' + String(Math.round((fx || 0) * 10000) / 10000);
  if (DP2A_MEMO_ && DP2A_MEMO_.key === memoKey) return DP2A_MEMO_.alloc;
  var eq = [], fi = [], i;
  for (i = 0; i < led.lots.length; i++) {
    var lot = led.lots[i], m = mp[lot.symbol] || {};
    var isFI = /fixed income|sukuk/i.test(m.assetClass || '') || /sukuk/i.test(lot.name || '');
    if (!lot.mp) lot.mp = m;
    (isFI ? fi : eq).push(lot);
  }
  var pv = 0; for (i = 0; i < led.lots.length; i++) pv += led.lots[i].mvSar || 0;
  var qmap = {}; for (i = 0; i < t.qualified.length; i++) qmap[dp2Str_(t.qualified[i]['Symbol']).trim()] = t.qualified[i];
  var seatMap = {}; for (i = 0; i < t.seats.length; i++) seatMap[dp2Str_(t.seats[i]['Symbol']).trim()] = t.seats[i];
  var cfg0 = dp2ProdConfig_(t);
  var plans = dp2PlanSeats_(t, fx, xbars, qmap, cfg0);
  // v1.2.20: ONE production configuration for the whole build (DP2_ENGINE_ENV + the Top_10 panel + the engine version)
  var cfg = cfg0;
  // the engine's portfolio sector context: every holding (sukuk included, as pf["holdings"]), sector names normalized
  // like the engine (TFB_OPP_SECTOR_NORMALIZE), cap = criteria pf_max_sector_pct (TFB_OPP_PF_MAX_SECTOR_PCT, default 30)
  var mkSec = function () { var o = { sectors: {}, maxPct: cfg.pfMaxSectorPct, available: led.lots.length > 0 }; for (var j = 0; j < led.lots.length; j++) { var lj = led.lots[j], mj = lj.mp || mp[lj.symbol] || {}; var sName = cfg.sector(mj.sector || 'Unknown'); o.sectors[sName] = (o.sectors[sName] || 0) + (lj.mvSar || 0); } return o; };
  var secCtx = mkSec();
  var reserve = dp2CashReserve_(t, cashSar, (cashSar || 0) + pv, cfg);   // v1.2.15: the engine's reserve, not a hard-coded 10 %
  // the research view is the conviction rule (the owner's target); the funding plans follow production's mode
  var alloc = dp2Allocate_(plans, cashSar, pv, secCtx, reserve.sar, cfg, 'conviction');
  alloc.secCtx = secCtx; alloc.reserve = reserve; alloc.cfg = cfg;
  var allocP = cfg.sizingMode === 'conviction' ? alloc : dp2Allocate_(plans, cashSar, pv, mkSec(), reserve.sar, cfg, 'cap');
  alloc.prod = allocP; alloc.needsProd = allocP.needs;
  var holdings = dp2RotationHoldings_(eq, fi, ib, qmap, seatMap, cfg);
  alloc.holdings = holdings;
  var rotParams = cfg.rot;
  alloc.rotParams = rotParams;
  if (cfg.planReady) {
    alloc.plans12 = dp2FundingPlans_(allocP.needs, allocP.deployable - allocP.allocated, holdings, '12m', rotParams);
    alloc.plans3 = dp2FundingPlans_(allocP.needs, allocP.deployable - allocP.allocated, holdings, '3m', rotParams);
  } else {
    // v1.2.18 / v1.2.20: no switch is planned unless production's sizing and funding configuration is mirrored
    alloc.plans12 = []; alloc.plans3 = [];
    alloc.fundingWithheld = 'rotation plans withheld — ' + cfg.issues.join('; ');
  }
  alloc.rotation12 = null; alloc.rotation3 = null;
  for (i = 0; i < alloc.plans12.length; i++) if (alloc.plans12[i].rotation) { alloc.rotation12 = alloc.plans12[i]; break; }
  for (i = 0; i < alloc.plans3.length; i++) if (alloc.plans3[i].rotation) { alloc.rotation3 = alloc.plans3[i]; break; }
  // v1.2.14: the production basis decides which plan list is executable; the other is shown as research only
  alloc.basisInfo = dp2RotationBasisInfo_(t, cfg);   // v1.2.21: the current configuration decides when pasted
  alloc.basis = alloc.basisInfo.basis;
  alloc.rotationProd = alloc.basis === 'plan3m' ? alloc.rotation3 : alloc.rotation12;
  alloc.rotationOther = alloc.basis === 'plan3m' ? alloc.rotation12 : alloc.rotation3;
  alloc.plansProd = alloc.basis === 'plan3m' ? alloc.plans3 : alloc.plans12;
  alloc.cashKey = memoKey;
  DP2A_MEMO_ = { key: memoKey, alloc: alloc };
  return alloc;
}

/* ========================================================= PORTFOLIO_DECISION_v2 */
function buildPortfolioDecisionV2() {
  var ss = dp2Ss_();
  var kv = dp2ReadStatusKV_();
  var feed = dp2ParseFeed_(kv);
  var pd = dp2ReadPortfolioDecision_();
  var led = dp2ReadLedger_();
  var mp = dp2ReadMyPortfolio_();
  var ib = dp2ReadIbkr_();
  var s1 = dp2ReadS1_();
  var switches = dp2ReadShadowSwitch_();
  var t10 = dp2ReadTop10_();

  var fx = dp2Num_(ib.account['fx_usd_sar']) || pd.fx || DP2.FX_DEFAULT;
  // v1.2.2: the same cash basis as Top_10_Investments_v2 (dp2CashBasis_), so NAV, floor, deployable and the shared allocation agree
  var cb = dp2CashBasis_(t10, ib, fx);
  var cashSar = cb.sar;
  var cashSource = cb.source;

  // lots: equities vs fixed income
  var eq = [], fi = [];
  for (var i = 0; i < led.lots.length; i++) {
    var lot = led.lots[i];
    var m = mp[lot.symbol] || {};
    var isFI = /fixed income|sukuk/i.test(m.assetClass || '') || /sukuk/i.test(lot.name || '');
    lot.mp = m;
    (isFI ? fi : eq).push(lot);
  }
  // price history for the dynamic exits (backend route → Yahoo → Stooq → cache); never fatal for the page
  var xbars = {};
  var oldestEntry = '';
  for (var oe = 0; oe < eq.length; oe++) { var od = dp2DateStr_(eq[oe].buyDate); if (od && (!oldestEntry || od < oldestEntry)) oldestEntry = od; }
  try { xbars = dp2LoadBars_(eq.map(function (l) { return l.symbol; }), oldestEntry); } catch (eX) { xbars = {}; }   // v1.2.11: history back to the oldest entry
  // research allocation + rotation (shared with Top_10_Investments_v2; seat bars are memoized for that page)
  var alloc = null;
  try {
    var planSyms = []; for (var pi = 0; pi < t10.seats.length; pi++) { if (/FAST-TRACK/.test(dp2Str_(t10.seats[pi]['Stability']))) planSyms.push(dp2Str_(t10.seats[pi]['Symbol']).trim()); }
    var seatBars = {}; try { seatBars = dp2LoadBars_(planSyms); } catch (eS) { seatBars = {}; }
    alloc = dp2ResearchAllocation_(t10, led, mp, ib, cashSar || 0, fx, seatBars);
  } catch (eA) { alloc = null; }
  var eqSar = 0, fiSar = 0, unrealEqSar = 0, activeTotal = 0;
  for (i = 0; i < eq.length; i++) { eqSar += eq[i].mvSar || 0; unrealEqSar += eq[i].unrealSar || 0; activeTotal += eq[i].totalReturnSar || 0; }
  for (i = 0; i < fi.length; i++) { fiSar += fi[i].mvSar || 0; activeTotal += fi[i].totalReturnSar || 0; }
  var navSar = (cashSar || 0) + eqSar + fiSar;
  var floorSar = navSar * DP2.CASH_FLOOR_PCT;
  var deploySar = (cashSar || 0) - floorSar;

  // TWR
  var twr = null, twrPeak = null, twrTrough = null, twrPeakDate = '', twrTroughDate = '', twrRows = [], twrMaxDD = null, twrMaxDDDate = '';
  if (ib.nav.length > 1) {
    var base = 1 + ib.nav[0].cps, runPeak = null;
    for (i = 0; i < ib.nav.length; i++) {
      var v = (1 + ib.nav[i].cps) / base - 1;
      twrRows.push([ib.nav[i].date, v]);
      if (twrPeak === null || v > twrPeak) { twrPeak = v; twrPeakDate = ib.nav[i].date; }
      if (twrTrough === null || v < twrTrough) { twrTrough = v; twrTroughDate = ib.nav[i].date; }
      // v1.2.4 (review finding): max drawdown = worst decline from a PRECEDING peak (running peak), not global trough / global peak
      if (runPeak === null || v > runPeak) runPeak = v;
      var dd = (1 + v) / (1 + runPeak) - 1;
      if (twrMaxDD === null || dd < twrMaxDD) { twrMaxDD = dd; twrMaxDDDate = ib.nav[i].date; }
    }
    twr = twrRows[twrRows.length - 1][1];
  }
  var twrInception = dp2Num_(ib.account['twr_inception_pct']);

  // evidence
  var evState = pd.evidence ? pd.evidence.state : 'UNKNOWN';
  var evFresh = /SENT|OK|FRESH/.test(evState) && !/NOT/.test(evState);
  var certified = evFresh && pd.readiness === 'VERIFIED' && feed.run && pd.receiptRun === feed.run;
  var stateChip = certified ? ['CERTIFIED', 'green'] : ['UNCERTIFIED', 'amber'];

  // trades
  var wins = [], losses = [], net = 0;
  for (i = 0; i < ib.trades.length; i++) { var p = ib.trades[i].pnl; if (p === null) continue; net += p; (p >= 0 ? wins : losses).push(ib.trades[i]); }
  var avgWin = wins.length ? wins.reduce(function (a, t) { return a + t.pnl; }, 0) / wins.length : null;
  var avgLoss = losses.length ? losses.reduce(function (a, t) { return a + t.pnl; }, 0) / losses.length : null;
  var winRate = ib.trades.length ? wins.length / ib.trades.length : null;
  var expectancy = (winRate !== null && avgWin !== null && avgLoss !== null) ? winRate * avgWin + (1 - winRate) * avgLoss : null;
  var stopOuts = 0; for (i = 0; i < losses.length; i++) if (/stop/i.test(losses[i].exit)) stopOuts++;

  // ---------------------------------------------------------------- render
  var sh = dp2Sheet_(DP2.SHEET_PD, true);
  dp2ResetSheet_(sh);
  dp2ChartsBegin_(DP2.SHEET_CHARTS_PD);
  var row = 1;
  var reason = 'source readiness ' + (pd.readiness || 'n/a') + (pd.receiptRun ? ' (run ' + pd.receiptRun + ')' : '') +
    ' · IBKR evidence ' + evState + (pd.evidence && pd.evidence.age ? ' — capture ' + pd.evidence.capture + ' is ' + pd.evidence.age + ' old (limit ' + pd.evidence.limit + ' m)' : '') +
    ' · feed ' + (feed.state || 'n/a') + (feed.blocker ? ' (' + feed.blocker + ')' : '') + (feed.reasons ? ' → ' + feed.reasons : '');
  dp2Header_(sh, row, 'PORTFOLIO DECISION — v2', stateChip[0], stateChip[1], 'built ' + dp2Today_() + ' · PD run ' + (pd.lastRun || 'n/a') + ' · FX ' + fx);
  row++;
  dp2Line_(sh, row, reason, { color: DP2C.ink, size: 9.5, bg: DP2C.surface, height: 30 }); row++;
  dp2Line_(sh, row, 'Sources: ledger ' + (led.status ? led.status.split('|')[0].replace('Last refresh', '').trim() : 'n/a') + ' · cash ' + cashSource + (ib.capture ? ' · IBKR cash and working orders from the read-only capture ' + dp2Str_(ib.account['as_of'] || 'n/a') + (ib.ageH !== null ? ' (' + dp2Fmt_(ib.ageH, 0) + ' h old' + (ib.stale ? ', STALE' : '') + ')' : '') + '; trades and TWR from the _IBKR_* tabs' : ' · IBKR orders/trades/TWR from the _IBKR_* tabs (' + (ib.tabs.account ? dp2Str_(ib.account['as_of'] || 'n/a') + (ib.ageH !== null ? ', ' + dp2Fmt_(ib.ageH, 0) + ' h old' : '') + (ib.stale ? ' — STALE manual snapshot' : '') : 'MISSING — no broker snapshot; cash from ' + cashSource) + ')' + (ib.captureNote ? ' — capture: ' + ib.captureNote : '')) + ' · My_Portfolio quotes', { size: 8.5, bg: DP2C.surface, height: 20 }); row++;
  sh.setFrozenRows(row - 1);
  row++;

  // tiles
  var tiles = [
    { label: 'NAV', value: dp2Fmt_(navSar) + ' SAR', sub: 'equities ' + dp2Fmt_(eqSar) + ' + sukuk ' + dp2Fmt_(fiSar) + ' + cash ' + dp2Fmt_(cashSar) },
    { label: 'Equities', value: dp2Fmt_(eqSar) + ' SAR', sub: dp2PctPlain_(navSar ? eqSar / navSar * 100 : null) + ' · ' + eq.length + ' lots · unrealized ' + dp2Signed_(unrealEqSar) + ' SAR' },
    { label: 'Sukuk', value: dp2Fmt_(fiSar) + ' SAR', sub: dp2PctPlain_(navSar ? fiSar / navSar * 100 : null) + ' · ' + (fi.length ? fi.map(function (l) { return l.symbol + (pd.excluded && pd.excluded.indexOf(l.symbol) >= 0 ? ' (outside execution scope)' : ''); }).join(', ') : 'none') },
    { label: 'Cash', value: dp2Fmt_(cashSar) + ' SAR', sub: dp2PctPlain_(navSar ? (cashSar || 0) / navSar * 100 : null) + ' · floor 10% = ' + dp2Fmt_(floorSar) },
    { label: 'Deployable', value: dp2Fmt_(alloc ? alloc.deployable : deploySar) + ' SAR', sub: (alloc && alloc.reserve ? 'engine reserve ' + dp2Fmt_(alloc.floorSar) + ' SAR (' + alloc.reserve.mode + ') · PF 10% policy floor ' + dp2Fmt_(floorSar) : 'after PF 10% floor ' + dp2Fmt_(floorSar)) + ' · Tranche 0 (S-1 ' + (s1.verdict || 'n/a') + ')' },
    { label: 'Unrealized', value: dp2Signed_(unrealEqSar) + ' SAR', sub: 'equity lots, ledger · active lots total ' + dp2Signed_(activeTotal) + ' SAR', color: unrealEqSar >= 0 ? DP2C.green : DP2C.red },
    { label: 'Return (TWR)', value: twr === null ? 'n/a' : dp2Pct_(twr * 100, 1), sub: twr === null ? 'add rows to _IBKR_NAV_History' : 'since ' + (twrRows[0][0]) + ' · inception ' + dp2Pct_(twrInception, 1) + ' · max DD ' + (twrMaxDD === null ? 'n/a' : dp2Pct_(twrMaxDD * 100, 1) + ' (to ' + (dp2DateStr_(twrMaxDDDate) || dp2Str_(twrMaxDDDate)) + ')'), color: (twr !== null && twr >= 0) ? DP2C.green : DP2C.red },
    { label: 'Feed / S-1', value: (feed.state || 'n/a').replace('NOT_ACTIONABLE', 'NOT ACTIONABLE'), sub: 'S-1 ' + (s1.verdict || 'n/a') + ' · calibration ' + (/as_of=([0-9-]+)/.exec(kv['TFB Calibration'] || '') || ['', 'n/a'])[1], color: /^ACTIONABLE/.test(feed.state) ? DP2C.green : DP2C.red }
  ];
  row = dp2Tiles_(sh, row, tiles); row++;

  // charts row
  var cTop = row;
  dp2Section_(sh, row, 1, 8, 'ACCOUNT RETURN — IBKR, time-weighted', twrRows.length ? ('Rebased to ' + twrRows[0][0] + '. Peak ' + dp2Pct_(twrPeak * 100, 1) + ' (' + twrPeakDate + '), trough ' + dp2Pct_(twrTrough * 100, 1) + ' (' + twrTroughDate + '), now ' + dp2Pct_(twr * 100, 1) + '. Deposits do not move this line.') : 'No rows in _IBKR_NAV_History yet.');
  dp2Section_(sh, row, 9, 16, 'ALLOCATION AND LIMITS', 'Composition of NAV; sector weights vs the 15% position / 30% sector caps (PF panel).');
  row += 2;
  var chartRow = row;
  for (i = 0; i < 12; i++) { dp2Range_(sh, row + i, 1, 16).setBackground(DP2C.surface); }
  row += 12;
  if (twrRows.length) {
    var rngTwr = dp2ChartData_('TWR', ['Date', 'TWR since funded'], twrRows);
    dp2Chart_(sh, 'AREA', rngTwr, chartRow, 1, { width: 680, height: 250, colors: [DP2C.blue], vAxis: { format: '0.0%', gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 } }, hAxis: { textStyle: { color: DP2C.muted, fontSize: 9 }, slantedText: true, slantedTextAngle: 30 }, chartArea: { left: 50, top: 12, width: '88%', height: '68%' }, lineWidth: 2 });
  }
  var allocRows = [['Equities', eqSar], ['Sukuk', fiSar], ['Cash', cashSar || 0]];
  var sectorRows = [];
  var capPos = dp2Num_(pd.panel['PF: Max Position %']) || 15, capSec = dp2Num_(pd.panel['PF: Max Sector %']) || 30;
  var secMap = {};
  for (i = 0; i < eq.length; i++) { var sec = (eq[i].mp && eq[i].mp.sector) || 'Other'; secMap[sec] = (secMap[sec] || 0) + (eq[i].mvSar || 0); }
  for (var sk in secMap) if (secMap.hasOwnProperty(sk)) sectorRows.push([sk, navSar ? secMap[sk] / navSar : 0, capSec / 100]);
  sectorRows.sort(function (a, b) { return b[1] - a[1]; });
  var compRows = [['Equities', navSar ? eqSar / navSar : 0], ['Sukuk', navSar ? fiSar / navSar : 0], ['Cash', navSar ? (cashSar || 0) / navSar : 0]].concat([['— cash floor', DP2.CASH_FLOOR_PCT]]);
  var rngAlloc = dp2ChartData_('Allocation', ['Bucket', 'Share of NAV'], compRows.concat(sectorRows.map(function (r) { return [r[0] + ' (sector)', r[1]]; })));
  dp2Chart_(sh, 'BAR', rngAlloc, chartRow, 9, { width: 680, height: 250, colors: [DP2C.blue], hAxis: { format: '0%', minValue: 0, maxValue: 0.6, gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 } }, vAxis: { textStyle: { color: DP2C.ink2, fontSize: 9 } }, chartArea: { left: 140, top: 12, width: '70%', height: '80%' }, bar: { groupWidth: '55%' } });
  row++;

  // holdings table — dynamic exits
  row = dp2Section_(sh, row, 1, 16, 'HOLDINGS — ledger lots, dynamic exits and IBKR order status', 'Dyn. stop = trailing rule: entry − 2 × ATR14 at entry; breakeven lock after +1R; 2 × ATR under the highest close (1.5 × ATR after +2R); never loosens. SELL NOW = last at/below the dynamic stop, or a profitable name closing below its 10-day average after giving back ≥ 1 ATR. Broker levels from _IBKR_Orders; the fixed broker stop is only a floor — RAISE STOP tells you when to move it.');
  var hdrs = ['Symbol', 'Name · sector', '3-month close', 'Qty', 'Avg cost', 'Last', 'Value SAR', '% NAV', 'Unreal.', 'Broker TP / stop', 'Dyn. stop (trail)', 'Exit engine', 'Order status (IBKR)', 'Exit signal'];
  var spans = [1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1];
  var trows = [], sigKinds = [], exitItems = [];
  var brackets = {};
  var actions = [];
  var limitsRows = [];
  var workingBrackets = 0, xFresh = 0, xTotal = 0, xSources = {}, xBarsAsOf = '';
  var exitAck = dp2ExitAckMap_();   // v1.2.5: acknowledged rule exits { symbol: 'yyyy-MM-dd' }
  // v1.2.12: the ledger may hold several lots per symbol — stop coverage is judged on the POSITION (sum of the lots'
  // shares) and the alert state is keyed per lot (symbol@entry#n)
  var sharesBySym = {}, lotSeq = {}, lotsBySym = {};
  for (var sb = 0; sb < eq.length; sb++) { sharesBySym[eq[sb].symbol] = (sharesBySym[eq[sb].symbol] || 0) + (eq[sb].shares || 0); lotsBySym[eq[sb].symbol] = (lotsBySym[eq[sb].symbol] || 0) + 1; }
  // v1.2.13 (review finding): the position's active stop quantity is ALLOCATED to the lots oldest-first, so the uncovered
  // shares are instructed once in total (never once per lot) and each lot knows its own covered quantity
  var lotCover = {}, coverLeft = {};
  (function () {
    var order = eq.slice().sort(function (a1, b1) { var da = dp2DateStr_(a1.buyDate), db = dp2DateStr_(b1.buyDate); return da < db ? -1 : (da > db ? 1 : 0); });
    for (var ci = 0; ci < order.length; ci++) {
      var lc = order[ci], bq = dp2BracketFor_(ib.orders, dp2YahooKey_(lc.symbol));
      if (!ib.ordersKnown) { lotCover[lc.symbol + '|' + ci] = null; continue; }
      if (coverLeft[lc.symbol] === undefined) coverLeft[lc.symbol] = bq.stopQty || 0;
      var cov = Math.min(lc.shares || 0, coverLeft[lc.symbol]); coverLeft[lc.symbol] -= cov;
      lc.__cover = cov;   // attached to the lot object (same object the main loop iterates)
    }
  })();
  for (i = 0; i < eq.length; i++) {
    var l = eq[i], m2 = l.mp || {};
    var key = dp2YahooKey_(l.symbol);
    var br = dp2BracketFor_(ib.orders, key);
    brackets[l.symbol] = br;
    var cost = l.shares ? (l.costBasis / l.shares) : l.buyPrice;
    var lfx = l.fx || fx;
    var xb = xbars[l.symbol] || null;
    var entryDate = dp2DateStr_(l.buyDate);
    // v1.2.9 (review finding): on STALE bars the live last is the cockpit quote (never the cached close), and no
    // actionable alert is issued — the signal is shown and flagged, mailed at most as an advisory.
    var lastOv = (xb && xb.last !== null && !xb.stale) ? xb.last : ((xb && xb.stale && l.price) ? l.price : null);
    var x = (xb && xb.bars && xb.bars.length) ? dp2ExitCompute_(xb.bars, cost, entryDate, lastOv) : null;
    xTotal++;
    if (xb && !xb.stale && x) xFresh++;
    if (xb && xb.source) xSources[xb.source] = true;
    if (x && x.lastDate > xBarsAsOf) xBarsAsOf = x.lastDate;
    var brokerWorking = ib.ordersKnown && br.active > 0;
    var brokerStop = ib.ordersKnown ? br.stop : null;   // active-status orders only (v1.2.2), from a fresh dated snapshot only (v1.2.5)
    var brokerTp = ib.ordersKnown ? br.tp : null;
    var posShares = sharesBySym[l.symbol] || l.shares, lotOfMany = (lotsBySym[l.symbol] || 1) > 1;
    var lotKeyBase = l.symbol + '@' + (entryDate || 'na'); lotSeq[lotKeyBase] = (lotSeq[lotKeyBase] || 0) + 1; var lotKey = lotKeyBase + (lotSeq[lotKeyBase] > 1 ? '#' + lotSeq[lotKeyBase] : '');
    // v1.2.13: acknowledgements are keyed by LOT; a symbol-level acknowledgement is honoured only when the symbol has one lot
    var ackDate = exitAck[lotKey] || (!lotOfMany ? (exitAck[l.symbol] || null) : null);
    var lotCov = ib.ordersKnown ? (l.__cover !== undefined ? l.__cover : Math.min(l.shares || 0, br.stopQty || 0)) : null;   // this lot's share of the position's active stop quantity
    var stopForLot = (ib.ordersKnown && brokerStop && lotCov !== null && lotCov > 0) ? brokerStop : null;
    var sig = dp2ExitSignal_(x, { tp: brokerTp, stop: stopForLot, known: ib.ordersKnown, stopQty: lotCov, shares: l.shares, stops: br.stops, lotOfMany: lotOfMany }, l.price, ackDate);
    if (xb && xb.stale && x) { sig.staleBars = true; if (sig.alert) { sig.alert = false; sig.advisory = true; sig.kind = 'amber'; sig.text += ' · STALE BARS (' + dp2Str_(xb.fetched) + ') — verify the quote before acting'; } }
    var last = (x && x.last) ? x.last : l.price;
    var tpPct = (br.tp && last) ? (br.tp / last - 1) * 100 : null;
    var stopEff = x ? x.stop : brokerStop;
    var stopPct = (stopEff && last) ? (stopEff / last - 1) * 100 : null;
    var spark = dp2SparkFormula_(l.symbol);
    var dynTxt = x ? (dp2Fmt_(x.stop, 2) + ' (' + dp2Pct_(stopPct, 1) + ')\n' + (brokerStop ? 'broker ' + dp2Fmt_(brokerStop, 2) + (x.stop >= brokerStop * (1 + DP2X.RAISE_MIN_PCT / 100) ? ' ↑ raise' : '') : (br.lastStop ? 'broker ' + dp2Fmt_(br.lastStop, 2) + ' not working' : 'no broker stop')))
                   : ('—\n' + (brokerStop ? 'broker ' + dp2Fmt_(brokerStop, 2) : (br.lastStop ? 'broker ' + dp2Fmt_(br.lastStop, 2) + ' not working' : 'no broker stop')));
    var engTxt = x ? ('ATR ' + dp2Fmt_(x.atr, 2) + ' · high ' + dp2Fmt_(x.hi, 2) + ' (' + dp2RFmt_(x.gainR) + ' at the high) · now ' + dp2RFmt_(x.lastR) + (x.sma !== null ? ' · SMA10 ' + dp2Fmt_(x.sma, 2) : '') + ' · trail ' + x.trailMult + '×ATR' + (x.beLocked ? ' · BE locked' : '') + ' · R ' + dp2Fmt_(x.R, 2) + '\n' + (xb.stale ? 'STALE bars (' + dp2Str_(xb.fetched) + ') — ' + dp2Str_(xb.error).slice(0, 60) : dp2Str_(xb.source) + ' · bars to ' + x.lastDate + (xb.marketState ? ' · ' + xb.marketState.toLowerCase() : '')))
                   : (dp2EngineNaWhy_(xb, entryDate) + (m2.rsi !== null && m2.rsi !== undefined ? ' · RSI ' + Math.round(m2.rsi) : ''));
    if (x && x.atrApprox) engTxt += ' · entry ATR approximated (' + x.barsBeforeEntry + ' bars before the entry)';
    var ostat;
    if (!ib.ordersKnown && br.n > 0) { ostat = 'UNVERIFIED — ' + ib.unknownWhy + ' · last listed: ' + br.statuses.join(', ') + ' → paste a fresh connector read'; }
    else if (br.state === 'working' && posShares && br.stopQty > 0 && br.stopQty < posShares - 1e-9) { ostat = 'working since ' + br.placed + ' — stop covers ' + dp2Fmt_(br.stopQty) + ' of the position\'s ' + dp2Fmt_(posShares) + ' sh (partial' + (lotOfMany ? '; this lot ' + dp2Fmt_(l.shares) + ' sh, covered ' + dp2Fmt_(lotCov || 0) : '') + ')'; }   // v1.2.11/12/13
    else if (br.state === 'working') { ostat = 'working since ' + br.placed; workingBrackets++; }
    else if (br.state === 'replaced') { ostat = 'REPLACED — no working successor listed → verify at the broker'; actions.push({ p: 1, t: l.symbol + ' bracket: IBKR lists the ' + dp2Fmt_(br.lastTp, 2) + ' / ' + dp2Fmt_(br.lastStop, 2) + ' pair as REPLACED with no working successor. If nothing is working, ' + dp2Fmt_(l.mvSar) + ' SAR is unprotected — verify before the next US open' + (x ? '; the rule\'s stop is ' + dp2Fmt_(x.stop, 2) : '') + '.' }); }
    else if (br.state === 'inactive') { ostat = 'no active sell order — ' + br.n + ' listed (' + br.statuses.join(', ') + ') → verify at the broker'; actions.push({ p: 1, t: l.symbol + ': every sell order in _IBKR_Orders is ' + br.statuses.join(' / ') + ' — nothing active protects ' + dp2Fmt_(l.mvSar) + ' SAR' + (x ? '; the rule\'s stop is ' + dp2Fmt_(x.stop, 2) : '') + '. Verify at the broker.' }); }
    else if (br.state === 'none') { ostat = ib.ordersKnown ? 'no orders in _IBKR_Orders' : 'broker orders unknown — ' + ib.unknownWhy + ' (🌱 Seed IBKR Tabs, or paste the connector read)'; }
    else { ostat = 'partial — ' + (brokerStop ? 'stop working, no take-profit' : 'take-profit working, NO STOP') + ' (' + br.active + ' active of ' + br.n + ')'; }
    // actions from the exit signal
    var item = { symbol: l.symbol, lotKey: lotKey, name: l.name, qty: l.shares, entry: cost, entryDate: entryDate, x: x, sig: sig, brokerStop: brokerStop, brokerTp: brokerTp, fx: lfx, mvSar: l.mvSar, source: xb ? xb.source : '', barsAsOf: x ? x.lastDate : '' };
    exitItems.push(item);
    if (sig.staleBars && (sig.code === 'STOP_HIT' || sig.code === 'SELL_NOW' || sig.code === 'EXIT_LATCHED')) {
      // v1.2.12 (review finding): on stale bars the PAGE carries the same verify-only wording as the mail — never a SELL
      actions.push({ p: 1, t: 'VERIFY ' + l.symbol + ' (' + dp2Fmt_(l.shares) + ' sh): the rule reads "' + sig.text.replace(/ · STALE BARS.*$/, '') + '" on CACHED bars (' + dp2Str_(xb && xb.fetched) + '; fetch failed: ' + dp2Str_(xb && xb.error).slice(0, 80) + ') and the cockpit quote ' + dp2Fmt_(last, 2) + '. Confirm the live price and the working stop at the broker — no sale is instructed while the history is stale; the next successful fetch re-evaluates.' });
    } else if (sig.code === 'EXIT_LATCHED') {
      actions.push({ p: 0, t: 'SELL ' + l.symbol + ' (' + dp2Fmt_(l.shares) + ' sh) — rule exit ' + sig.latched.text + ' was never acted on; the lot is outside the rule. Unrealized ' + dp2Signed_((last - cost) * l.shares * lfx) + ' SAR now. Sell, or acknowledge the exit (📊 Decision v2 ▸ ✅ Acknowledge rule exit) to hold by decision under the trailing stop ' + (x ? dp2Fmt_(x.stop, 2) : '—') + '. No order was placed by this page — act at the broker.' });
    } else if (sig.code === 'STOP_HIT' || sig.code === 'SELL_NOW') {
      actions.push({ p: 0, t: 'SELL NOW ' + l.symbol + ' (' + dp2Fmt_(l.shares) + ' sh): ' + dp2ExitLine_(item, fx).why + ' Unrealized ' + dp2Signed_((last - cost) * l.shares * lfx) + ' SAR now' + (brokerStop ? ' vs ' + dp2Signed_((brokerStop - cost) * l.shares * lfx) + ' SAR if the fixed broker stop ' + dp2Fmt_(brokerStop, 2) + ' is left to fill' : '') + '. No order was placed by this page — act at the broker.' });
    } else if (sig.code === 'STOP_NEAR') {
      actions.push({ p: 1, t: l.symbol + ' is ' + dp2PctPlain_(sig.dist, 1) + ' above its dynamic stop ' + dp2Fmt_(x.stop, 2) + ' — a print at or below it is a SELL NOW; decide now whether to pre-empt at ' + dp2Fmt_(last, 2) + (brokerStop ? ' (fixed broker stop ' + dp2Fmt_(brokerStop, 2) + ' is ' + dp2Pct_((brokerStop / last - 1) * 100, 1) + ' away)' : '') + '.' });
    } else if (sig.code === 'STOP_PARTIAL') {
      actions.push({ p: 2, t: l.symbol + (lotOfMany ? ' (lot ' + entryDate + ')' : '') + ': the active stop covers ' + dp2Fmt_(br.stopQty) + ' of the position\'s ' + dp2Fmt_(posShares) + ' sh — place a stop for the uncovered ' + dp2Fmt_(sig.uncovered) + ' sh at ' + dp2Fmt_(x.stop, 2) + (brokerStop && x.stop >= brokerStop * (1 + DP2X.RAISE_MIN_PCT / 100) ? ' and raise the existing stop from ' + dp2Fmt_(brokerStop, 2) + ' to ' + dp2Fmt_(x.stop, 2) : '') + '.' });
    } else if (sig.code === 'RAISE_STOP' || sig.code === 'NO_BROKER_STOP') {
      actions.push({ p: 2, t: l.symbol + ': ' + (sig.code === 'RAISE_STOP' ? 'raise the IBKR stop from ' + dp2Fmt_(brokerStop, 2) + ' to ' + dp2Fmt_(x.stop, 2) : 'place a stop at ' + dp2Fmt_(x.stop, 2) + (sig.lotOfMany && sig.lotShares ? ' for this lot\'s ' + dp2Fmt_(sig.lotShares) + ' sh' : '') + ' (no working stop listed' + (sig.lotOfMany ? ' for this lot' : '') + ')') + ' — or use a trailing stop order, trail amount ' + dp2Fmt_(x.trailMult * x.atr, 2) + ' (' + x.trailMult + ' × ATR); the move at the high is ' + dp2RFmt_(x.gainR) + (x.beLocked ? ', breakeven is locked' : '') + '.' });
    } else if (sig.code === 'TP_WATCH') {
      actions.push({ p: 2, t: l.symbol + ' is ' + dp2Pct_(tpPct, 1) + ' from the broker take-profit ' + dp2Fmt_(br.tp, 2) + '; a fill returns ≈ ' + dp2Fmt_(l.shares * br.tp * lfx) + ' SAR to cash. Under the trailing rule the TP is optional: cancel it to let the trail (' + dp2Fmt_(x.stop, 2) + ') run the winner, or keep it and bank the gain.' });
    }
    if (xb && xb.stale && x) actions.push({ p: 3, t: l.symbol + ' exit engine is running on cached bars (' + dp2Str_(xb.fetched) + '): ' + dp2Str_(xb.error || 'fetch failed').slice(0, 120) + '.' });
    // rotation (switch) advisory — research mirror of the engine's funding plans
    // v1.2.14 (review finding): only the PRODUCTION basis (dp2RotationBasis_) raises a SWITCH action / advisory; the
    // other basis is a research note on the row, never an instruction
    var rotP = (alloc && alloc.rotationProd && alloc.rotationProd.rotation.symbol === l.symbol) ? alloc.rotationProd : null;
    var rotO = (alloc && alloc.rotationOther && alloc.rotationOther.rotation.symbol === l.symbol) ? alloc.rotationOther : null;
    var bLbl = alloc ? dp2RotationBasisLabel_(alloc.basis) : 'engine 12M', oLbl = alloc && alloc.basis === 'plan3m' ? 'engine 12M' : 'plan 3M';
    if (rotP) {
      item.rotation = { plan: rotP, alt: rotO };
      if (!sig.alert) sig.advisory = true;
      engTxt += '\n↻ rotation candidate → ' + rotP.sym + ' (' + bLbl + ' edge +' + dp2Fmt_(rotP.rotation.edge, 1) + ' pp after cost' + (rotO ? '; ' + oLbl + ' agrees' : '; ' + oLbl + ' does not reach its hurdle') + ')';
      actions.push({ p: 2, t: 'SWITCH (research, ' + bLbl + ' = production basis): ' + rotP.rotation.action.toLowerCase() + ' ' + l.symbol + ' ' + dp2Fmt_(rotP.rotation.proceeds) + ' SAR to fund ' + rotP.sym + ' (' + dp2PctPlain_(rotP.oppRoi, 1) + ' vs ' + l.symbol + ' forward ' + dp2PctPlain_(rotP.rotation.roi, 1) + ' → edge +' + dp2Fmt_(rotP.rotation.edge, 1) + ' pp after ' + (rotP.cost !== undefined ? rotP.cost : DP2A.ROT_COST_PCT) + '% round-trip cost)' + (rotO ? '; the ' + oLbl + ' basis agrees (edge +' + dp2Fmt_(rotO.rotation.edge, 1) + ' pp)' : '; the ' + oLbl + ' basis does not reach its hurdle') + '. Research only — the engine issues the executable plan when the feed is released.' });
    } else if (rotO) {
      engTxt += '\n↻ ' + oLbl + ' basis would rotate → ' + rotO.sym + ' (edge +' + dp2Fmt_(rotO.rotation.edge, 1) + ' pp) — not the production basis (' + bLbl + '), no action';
    }
    var decision = sig.text;
    if (sig.code === 'HOLD' && !certified) decision = 'HOLD · trailing · uncertified';
    sigKinds.push(sig.kind);
    trows.push([l.symbol, dp2Str_(l.name) + ' · ' + dp2Str_(m2.sector || '') + ' · ' + (l.holdingDays !== null ? l.holdingDays + ' d' : '') + (m2.rsi !== null && m2.rsi !== undefined ? ' · RSI ' + Math.round(m2.rsi) : ''), spark, l.shares, cost, last, l.mvSar, navSar ? l.mvSar / navSar : null, l.unrealSar && l.costBasis ? (l.unrealSar / (l.costBasis * lfx)) : null, (!ib.ordersKnown && br.n > 0 ? (br.lastTp ? dp2Fmt_(br.lastTp, 2) : '—') + ' / ' + (br.lastStop ? dp2Fmt_(br.lastStop, 2) : '—') + '\n(UNVERIFIED — stale/undated snapshot)' : (br.tp ? dp2Fmt_(br.tp, 2) : '—') + ' / ' + (br.stop ? dp2Fmt_(br.stop, 2) : '—') + (br.active === 0 && (br.lastTp || br.lastStop) ? '\n(last ' + (br.lastTp ? dp2Fmt_(br.lastTp, 2) : '—') + ' / ' + (br.lastStop ? dp2Fmt_(br.lastStop, 2) : '—') + ' not working)' : (br.state === 'partial' ? (br.stop ? '\nno active TP' : '\nNO ACTIVE STOP' + (br.lastStop ? ' (last ' + dp2Fmt_(br.lastStop, 2) + ')' : '')) : (tpPct !== null ? '\nTP ' + dp2Pct_(tpPct, 1) : '')))), dynTxt, engTxt, ostat, decision]);
  }
  // v1.2.21 (review finding on 736ccd6): the unfunded-plans / capital-call action is computed once per page (dp2UnfundedAction_)
  var unfundedAct = dp2UnfundedAction_(alloc);
  if (unfundedAct) actions.push(unfundedAct);
  for (i = 0; i < fi.length; i++) {
    var f = fi[i];
    sigKinds.push('grey');
    trows.push([f.symbol, dp2Str_(f.name) + ' · fixed income · ' + (f.holdingDays !== null ? f.holdingDays + ' d' : ''), 'no broker series', f.shares, f.shares ? f.costBasis / f.shares : f.buyPrice, f.price, f.mvSar, navSar ? f.mvSar / navSar : null, f.returnPct, '—', '—', 'held for yield · coupons ' + dp2Fmt_(f.dividends) + ' SAR', 'n/a', 'EXCLUDED (PD_EXECUTION_EXCLUDE)']);
  }
  var tableTop = row;
  row = dp2Table_(sh, row, hdrs, trows, { spans: spans, align: ['l', 'l', 'c', 'r', 'r', 'r', 'r', 'r', 'r', 'r', 'r', 'l', 'l', 'l'], fmt: ['', '', '', '#,##0', '0.00', '0.00', '#,##0', '0.0%', '+0.0%;-0.0%', '', '', '', '', ''], wrap: [false, true, false, false, false, false, false, false, false, true, true, true, true, true], rowHeight: 52 });
  // colour: unrealized (col 10), dyn stop (col 12), engine (13-14), order status (15), exit-signal chip (16)
  for (i = 0; i < trows.length; i++) {
    var rr = tableTop + 1 + i;
    var uc = sh.getRange(rr, 10); var uv = trows[i][8];
    if (typeof uv === 'number') uc.setFontColor(uv >= 0 ? DP2C.green : DP2C.red).setFontWeight('bold');
    sh.getRange(rr, 10, 1, 2).setFontSize(8.5);
    var dcol = sh.getRange(rr, 12).setFontSize(8.5).setFontWeight('bold');
    if (sigKinds[i] === 'red') dcol.setFontColor(DP2C.red); else if (sigKinds[i] === 'amber') dcol.setFontColor(DP2C.amber); else if (sigKinds[i] === 'green') dcol.setFontColor(DP2C.green);
    sh.getRange(rr, 13).setFontSize(8).setFontColor(DP2C.ink2);
    var oc = sh.getRange(rr, 15).setFontSize(8.5); var otext = dp2Str_(trows[i][12]);
    oc.setFontColor(/REPLACED|partial/.test(otext) ? DP2C.amber : (/working/.test(otext) ? DP2C.green : DP2C.ink2)).setFontWeight(/REPLACED/.test(otext) ? 'bold' : 'normal');
    var chip = sh.getRange(rr, 16);
    dp2Chip_(chip, dp2Str_(trows[i][13]), sigKinds[i]);
    chip.setFontSize(8.5).setWrap(true);
  }
  // e-mail alerts + state (never fatal for the page)
  var alertRes = { enabled: dp2ExitAlertsEnabled_(), to: dp2ExitAlertTo_(), due: 0, sent: 0, signals: 0, error: exitItems.length ? '' : 'no equity lots' };
  if (exitItems.length) { try { alertRes = dp2ExitAlerts_(exitItems, fx); } catch (eA) { alertRes.error = String(eA && eA.message || eA); } }
  var srcList = Object.keys(xSources).join(', ') || 'none';
  dp2Line_(sh, row, 'Exit engine: ' + xFresh + ' of ' + xTotal + ' lots on fresh bars (' + srcList + (xBarsAsOf ? ', bars to ' + xBarsAsOf : '') + ') · e-mail alerts ' + (alertRes.enabled ? 'ON → ' + (alertRes.to || '(no recipient)') : 'OFF') + ' · ' + (alertRes.sent ? '1 e-mail sent this run (' + alertRes.signals + ' signal' + (alertRes.signals === 1 ? '' : 's') + ')' : (alertRes.due ? alertRes.due + ' signal(s) due, e-mail not sent' : 'nothing new to send')) + (alertRes.error ? ' · ' + alertRes.error : '') + ' · cooldown ' + DP2X.COOLDOWN_H + ' h · state in ' + DP2X.SHEET_STATE + ' · rule parameters in DP2X.', { size: 8.5, color: alertRes.error ? DP2C.amber : DP2C.ink2, bg: DP2C.surface, height: 24 });
  row += 2;

  // trade record + what it means
  row = dp2Section_(sh, row, 1, 8, 'TRADE RECORD — IBKR closed positions (_IBKR_Trades)', 'Realized P&L per closed name after commissions; blue = gain, red = loss.');
  dp2Section_(sh, row - 2, 9, 16, 'WHAT THE TRADE RECORD MEANS', 'Computed from _IBKR_Trades; the ledger total is shown for reconciliation.');
  var tradeTop = row;
  var tRows = ib.trades.slice().sort(function (a, b) { return (b.pnl || 0) - (a.pnl || 0); }).map(function (t) { return [t.symbol + ' · ' + t.exit, t.pnl >= 0 ? t.pnl : 0, t.pnl < 0 ? t.pnl : 0]; });
  var tradeHeight = Math.max(220, 40 + tRows.length * 18);
  var tradeRowsNeeded = Math.ceil(tradeHeight / DP2.ROW_HEIGHT) + 1;
  for (i = 0; i < tradeRowsNeeded; i++) dp2Range_(sh, row + i, 1, 16).setBackground(DP2C.surface);
  if (tRows.length) {
    var rngTr = dp2ChartData_('Trades', ['Closed name', 'Gain USD', 'Loss USD'], tRows);
    dp2Chart_(sh, 'BAR', rngTr, tradeTop, 1, { width: 680, height: tradeHeight, colors: [DP2C.blue, DP2C.seriesRed], isStacked: true, legend: { position: 'top', textStyle: { color: DP2C.ink2, fontSize: 9 } }, hAxis: { gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 } }, vAxis: { textStyle: { color: DP2C.ink2, fontSize: 9 } }, chartArea: { left: 160, top: 28, width: '68%', height: '80%' }, bar: { groupWidth: '60%' } });
  }
  var statTiles = [
    { label: 'Win rate', value: ib.trades.length ? wins.length + ' / ' + ib.trades.length : 'n/a', sub: winRate !== null ? dp2PctPlain_(winRate * 100, 0) + ' of closed positions' : '', color: (winRate !== null && winRate < 0.45) ? DP2C.red : DP2C.ink },
    { label: 'Payoff ratio', value: (avgWin !== null && avgLoss) ? Math.abs(avgWin / avgLoss).toFixed(2) : 'n/a', sub: (avgWin !== null && avgLoss !== null) ? 'avg win ' + dp2Signed_(avgWin) + ' vs avg loss ' + dp2Signed_(avgLoss) + ' USD' : '' },
    { label: 'Expectancy', value: expectancy !== null ? dp2Signed_(expectancy) + ' USD' : 'n/a', sub: 'per closed trade', color: (expectancy !== null && expectancy < 0) ? DP2C.red : DP2C.ink },
    { label: 'Stop-outs', value: losses.length ? stopOuts + ' / ' + losses.length : 'n/a', sub: 'losses closed by the stop' }
  ];
  // tiles into columns 9..16 (4 tiles × 2 cols)
  var tc = 9;
  for (i = 0; i < statTiles.length; i++) {
    var box = dp2Range_(sh, tradeTop, tc, tc + 1, 3); dp2Box_(box);
    dp2Range_(sh, tradeTop, tc, tc + 1).merge().setNumberFormat('@').setValue(statTiles[i].label.toUpperCase()).setFontSize(8).setFontColor(DP2C.ink2);
    dp2Range_(sh, tradeTop + 1, tc, tc + 1).merge().setNumberFormat('@').setValue(statTiles[i].value).setFontSize(14).setFontWeight('bold').setFontColor(statTiles[i].color || DP2C.ink);
    dp2Range_(sh, tradeTop + 2, tc, tc + 1).merge().setNumberFormat('@').setValue(statTiles[i].sub).setFontSize(8).setFontColor(DP2C.ink2).setWrap(true);
    tc += 2;
  }
  var meaning = 'Net ' + dp2Signed_(net) + ' USD (≈ ' + dp2Signed_(net * fx) + ' SAR) over ' + ib.trades.length + ' closed names. ' +
    (winRate !== null && winRate < 0.45 ? 'Stops sit a few percent below entry while the names move 1–1.5% a day, so the stop is reached far more often than the target: a payoff ratio near 2 cannot carry a ' + dp2PctPlain_(winRate * 100, 0) + ' win rate. ' : '') +
    'D-stop is live on this page as the dynamic exit rule (initial stop 2 × ATR-14, breakeven lock after +1R, trailing stop, SMA10 profit protection); the broker brackets still have to be moved by hand — follow the RAISE STOP actions or replace fixed stops with trailing stop orders.' +
    (led.totals.closed !== undefined && led.totals.closed !== null ? ' Ledger closed total (lifetime) ' + dp2Fmt_(led.totals.closed) + ' SAR vs broker realized here ' + dp2Fmt_(net * fx) + ' SAR — reconcile in Broker_Reconciliation.' : '');
  var mRng = dp2Range_(sh, tradeTop + 3, 9, 16, Math.max(3, tradeRowsNeeded - 3)).merge();
  mRng.setValue(meaning).setWrap(true).setVerticalAlignment('top').setFontSize(9.5).setBackground(DP2C.surface).setBorder(true, true, true, true, false, false, DP2C.border, SpreadsheetApp.BorderStyle.SOLID);
  row = tradeTop + tradeRowsNeeded + 1;

  // actions + limits
  if (!evFresh) actions.push({ p: 0, t: 'Portfolio_Decision carries no fresh IBKR evidence (' + evState + (pd.evidence && pd.evidence.age ? ', ' + pd.evidence.age : '') + '): it certifies only when it runs within 14 min of the morning capture — install the capture watcher once (Apps Script editor: run pdInstallCaptureWatcher; Script Property PD_IBKR_EVIDENCE = observe or enforce), or refresh Portfolio_Decision right after the 08:40 capture. The figures above come from the ledger and ' + (ib.capture ? 'the IBKR capture ' : 'the _IBKR_* tabs ') + '(' + dp2Str_(ib.account['as_of'] || 'n/a') + ').' });   // v1.2.22
  if (s1.verdict !== 'PASS') actions.push({ p: 4, t: 'No board-originated capital: S-1 gate ' + (s1.verdict || 'n/a') + (s1.criteria.length ? ' (' + s1.criteria.filter(function (c) { return c.status !== 'PASS'; }).map(function (c) { return c.n + ' ' + c.status; }).join(', ') + ')' : '') + '; deployable ' + dp2Fmt_(deploySar) + ' SAR stays Tranche 0.' });
  for (i = 0; i < switches.length && i < 1; i++) actions.push({ p: 5, t: 'Switch advisory (Shadow_Board, research only): sell ' + switches[i].sell + ' → buy ' + switches[i].buy + ' Δ ' + dp2Pct_(switches[i].delta, 1) + ' vs hurdle ' + dp2PctPlain_(switches[i].hurdle, 1) + '; not executable while the feed is ' + (feed.state || 'n/a') + '.' });
  // v1.2.12: two lots of one symbol can raise the identical position-level action — show it once
  (function () { var seen = {}, u = []; for (var ai = 0; ai < actions.length; ai++) { if (!seen[actions[ai].t]) { seen[actions[ai].t] = true; u.push(actions[ai]); } } actions = u; })();
  actions.sort(function (a, b) { return a.p - b.p; });
  row = dp2Section_(sh, row, 1, 9, 'ACTIONS TODAY — what is actually asked of the operator', 'Generated from the rules on this page; at most five.');
  dp2Section_(sh, row - 2, 10, 16, 'LIMITS AND GATES', 'Pass / fail against the panel caps and the chain.');
  var actTop = row;
  var maxA = Math.min(5, actions.length);
  for (i = 0; i < maxA; i++) { dp2Line_(sh, row + i, (i + 1) + '. ' + actions[i].t, { c1: 1, c2: 9, size: 9.5, color: DP2C.ink, bg: DP2C.surface, height: 44 }); }
  var maxEq = 0; for (var s2 in secMap) if (secMap.hasOwnProperty(s2)) maxEq = Math.max(maxEq, secMap[s2]);
  var maxPos = 0; for (i = 0; i < eq.length; i++) maxPos = Math.max(maxPos, eq[i].mvSar || 0);
  limitsRows = [
    ['Position cap ' + capPos + '% NAV', navSar ? dp2PctPlain_(maxPos / navSar * 100, 1) + ' max · ' + (maxPos / navSar * 100 <= capPos ? 'pass' : 'FAIL') : 'n/a', navSar && maxPos / navSar * 100 <= capPos ? DP2C.green : DP2C.red],
    ['Sector cap ' + capSec + '% NAV', navSar ? dp2PctPlain_(maxEq / navSar * 100, 1) + ' max · ' + (maxEq / navSar * 100 <= capSec ? 'pass' : 'FAIL') : 'n/a', navSar && maxEq / navSar * 100 <= capSec ? DP2C.green : DP2C.red],
    ['Cash floor 10% NAV', navSar ? dp2PctPlain_((cashSar || 0) / navSar * 100, 1) + ' · ' + ((cashSar || 0) >= floorSar ? 'pass' : 'FAIL') : 'n/a', (cashSar || 0) >= floorSar ? DP2C.green : DP2C.red],
    ['Cash basis (shared with Top_10_v2)', dp2CashBasisText_(cb), cb.sar === null ? DP2C.red : (cb.warn ? DP2C.amber : DP2C.green)],
    ['Brackets working on every equity lot', workingBrackets + ' of ' + eq.length + (workingBrackets < eq.length ? ' · verify' : ' · pass'), workingBrackets === eq.length ? DP2C.green : DP2C.amber],
    ['Dynamic exit engine on fresh bars', xFresh + ' of ' + xTotal + ' lots · ' + srcList, (xTotal > 0 && xFresh === xTotal) ? DP2C.green : DP2C.amber],
    ['Broker stop ≥ dynamic stop on every lot, full quantity', (function () { var n = 0; for (var k = 0; k < exitItems.length; k++) if (exitItems[k].sig.code === 'RAISE_STOP' || exitItems[k].sig.code === 'NO_BROKER_STOP' || exitItems[k].sig.code === 'STOP_PARTIAL') n++; return n ? n + ' lot(s) to raise / place / complete' : 'pass'; })(), (function () { for (var k = 0; k < exitItems.length; k++) if (exitItems[k].sig.code === 'RAISE_STOP' || exitItems[k].sig.code === 'NO_BROKER_STOP' || exitItems[k].sig.code === 'STOP_PARTIAL') return DP2C.amber; return DP2C.green; })()],
    ['SELL NOW signals open', (function () { var n = []; for (var k = 0; k < exitItems.length; k++) if (exitItems[k].sig.alert) n.push(exitItems[k].symbol); return n.length ? n.join(', ') : 'none'; })(), (function () { for (var k = 0; k < exitItems.length; k++) if (exitItems[k].sig.alert) return DP2C.red; return DP2C.green; })()],
    ['Rotation candidate (research, ' + (alloc ? dp2RotationBasisLabel_(alloc.basis) : 'engine 12M') + ' = production basis)', alloc && alloc.fundingWithheld ? 'withheld — ' + (alloc.cfg && alloc.cfg.issues.length ? alloc.cfg.issues[0] : 'production configuration not mirrored') : alloc && alloc.rotationProd ? alloc.rotationProd.rotation.action + ' ' + alloc.rotationProd.rotation.symbol + ' → ' + alloc.rotationProd.sym + ' (+' + dp2Fmt_(alloc.rotationProd.rotation.edge, 1) + ' pp)' + (alloc.rotationOther ? '' : ' · other basis: none') : (alloc && alloc.rotationOther ? 'none on the production basis · ' + (alloc.basis === 'plan3m' ? 'engine 12M' : 'plan 3M') + ' would rotate ' + alloc.rotationOther.rotation.symbol + ' → ' + alloc.rotationOther.sym + ' (research only)' : (alloc && (alloc.needsProd || alloc.needs).length ? 'none — capital call' : 'none needed')), alloc && alloc.rotationProd ? DP2C.amber : DP2C.ink],
    ['IBKR evidence freshness ≤ ' + DP2.EVIDENCE_LIMIT_MIN + ' min', evState + (pd.evidence && pd.evidence.age ? ' · ' + pd.evidence.age : ''), evFresh ? DP2C.green : DP2C.red],
    ['Source receipt = sync run', (pd.receiptRun || 'n/a') + (feed.run ? ' vs ' + feed.run : '') + ' · ' + (feed.run && pd.receiptRun === feed.run ? 'pass' : 'fail'), feed.run && pd.receiptRun === feed.run ? DP2C.green : DP2C.red],
    ['Decision feed', (feed.state || 'n/a') + (feed.blocker ? ' (' + feed.blocker + ')' : ''), /^ACTIONABLE|^OK/.test(feed.state) ? DP2C.green : DP2C.red],
    ['S-1 gate', (s1.verdict || 'n/a') + (s1.criteria.length ? ' · pending: ' + s1.criteria.filter(function (c) { return c.status !== 'PASS'; }).map(function (c) { return c.n; }).join(', ') : ''), s1.verdict === 'PASS' ? DP2C.green : DP2C.amber],
    ['Leverage (IBKR)', ib.account['leverage'] !== undefined ? dp2Str_(ib.account['leverage']) + ' · no margin used' : 'n/a', DP2C.green]
  ];
  var limEnd = dp2KV_(sh, actTop, 10, 16, limitsRows);
  row = Math.max(actTop + Math.max(maxA, 1), limEnd) + 1;

  // evidence + ledger
  row = dp2Section_(sh, row, 1, 8, 'EVIDENCE TRAIL', null);
  dp2Section_(sh, row - 1, 9, 16, 'LEDGER AND RECONCILIATION', null);
  var evRows = [
    ['IBKR tabs (_IBKR_Account/Orders/Trades/NAV)', ib.tabs.account ? dp2Str_(ib.account['as_of'] || 'n/a') + (ib.ageH !== null ? ' · ' + dp2Fmt_(ib.ageH, 0) + ' h old' : '') + (ib.stale ? ' · STALE' : '') : 'missing — not seeded (manual snapshot; refresh never seeds it)', ib.tabs.account && !ib.stale ? DP2C.ink : DP2C.amber],
    ['TFB Flex capture (PD_IBKR_EVIDENCE)', evState + (pd.evidence && pd.evidence.capture ? ' · ' + pd.evidence.capture + ' (' + pd.evidence.age + ')' : ''), evFresh ? DP2C.green : DP2C.red],
    ['Source receipt (daily_sync)', pd.receiptRun ? 'run ' + pd.receiptRun : 'n/a'],
    ['Latest sync run (TFB Decision Feed)', feed.run ? 'run ' + feed.run + ' · ' + feed.at : 'n/a'],
    ['Portfolio_Decision last run', pd.lastRun || 'n/a'],
    ['Ledger (_Portfolio_CostBasis)', led.status ? led.status.split('|')[0].replace('Last refresh', '').trim() + ' · ' + (led.totals.activeCount || '?') + ' active · ' + (led.totals.closedCount || '?') + ' closed' : 'n/a'],
    ['Price history (' + DP2X.SHEET_BARS + ')', srcList + (xBarsAsOf ? ' · bars to ' + xBarsAsOf : '') + ' · ' + xFresh + '/' + xTotal + ' fresh', (xTotal > 0 && xFresh === xTotal) ? DP2C.green : DP2C.amber],
    ['Exit alerts (' + DP2X.SHEET_STATE + ')', (alertRes.enabled ? 'ON → ' + (alertRes.to || '(no recipient)') : 'OFF') + (alertRes.sent ? ' · e-mail sent (' + alertRes.signals + ')' : '') + (alertRes.error ? ' · ' + alertRes.error.slice(0, 40) : ''), alertRes.error ? DP2C.amber : DP2C.ink],
    ['Calibration (TFB Calibration)', (kv['TFB Calibration'] || 'n/a').split('|').pop().trim()]
  ];
  var ledRows = [
    ['Realized, closed lots (ledger, lifetime)', led.totals.closed !== undefined ? dp2Fmt_(led.totals.closed) + ' SAR' : 'n/a', (led.totals.closed || 0) < 0 ? DP2C.red : DP2C.green],
    ['Realized, IBKR closed names (_IBKR_Trades)', ib.trades.length ? dp2Fmt_(net * fx) + ' SAR · reconcile' : 'n/a', net < 0 ? DP2C.red : DP2C.green],
    ['Active lots (unrealized + coupons, ledger)', led.totals.active !== undefined ? dp2Fmt_(led.totals.active) + ' SAR' : 'n/a', (led.totals.active || 0) >= 0 ? DP2C.green : DP2C.red],
    ['Lifetime result (ledger)', led.totals.lifetime !== undefined ? dp2Fmt_(led.totals.lifetime) + ' SAR' : 'n/a'],
    ['Switch candidates (Shadow_Board)', switches.length ? switches.map(function (s) { return s.sell + '→' + s.buy + ' ' + dp2Pct_(s.delta, 1); }).join(' · ') : 'none'],
    ['Trade Notes', (function () { var t = dp2Sheet_('_Trade_Notes', false); return t ? Math.max(0, t.getLastRow() - 1) + ' entries' : 'tab missing'; })(), DP2C.amber]
  ];
  var e1 = dp2KV_(sh, row, 1, 8, evRows);
  var e2 = dp2KV_(sh, row, 9, 16, ledRows);
  row = Math.max(e1, e2) + 1;
  dp2Line_(sh, row, 'Decision Pages v2 ' + DP2_VERSION + ' · built ' + dp2Today_() + ' · read-only view over Portfolio_Decision, _Portfolio_CostBasis, My_Portfolio, _Status, S1_Gate, Shadow_Board, the _IBKR_* tabs and the price-history cache (' + DP2X.SHEET_BARS + ') — nothing on this sheet feeds a gate or a ticket, and no broker order is placed, changed or cancelled.', { size: 8, color: DP2C.muted });
  return { rows: row, nav: navSar, cash: cashSar, certified: certified, exits: exitItems.map(function (it) { return { symbol: it.symbol, signal: it.sig.code, stop: it.x ? it.x.stop : null, last: it.sig.last }; }), alerts: alertRes };
}

/* ========================================================= TOP_10_INVESTMENTS_v2 */
function buildTop10V2() {
  var kv = dp2ReadStatusKV_();
  var feed = dp2ParseFeed_(kv);
  var t = dp2ReadTop10_();
  var gm = dp2ReadGmInvestability_();
  var hit = dp2ReadHitRates_();
  var s1 = dp2ReadS1_();
  var led = dp2ReadLedger_();
  var ib = dp2ReadIbkr_();
  var fx = dp2Num_(ib.account['fx_usd_sar']) || DP2.FX_DEFAULT;
  // v1.2.2: the same cash basis as Portfolio_Decision_v2 (dp2CashBasis_); the allocation memo is keyed on it
  var cb = dp2CashBasis_(t, ib, fx);
  var cashSar = cb.sar;
  var cashSource = cb.source;
  var navSar = 0;
  for (var i = 0; i < led.lots.length; i++) navSar += led.lots[i].mvSar || 0;
  navSar += cashSar || 0;
  var refSize = navSar * DP2.REF_SIZE_PCT;
  var deploy = (cashSar || 0) - navSar * DP2.CASH_FLOOR_PCT;

  // seats
  var seats = t.seats, fast = [], grace = [];
  for (i = 0; i < seats.length; i++) { var st = dp2Str_(seats[i]['Stability']); if (/FAST-TRACK/.test(st)) fast.push(seats[i]); else if (/GRACE/.test(st)) grace.push(seats[i]); }
  var executable = 0; for (i = 0; i < seats.length; i++) if (dp2Num_(seats[i]['Ticket SAR']) > 0) executable++;
  // price history for the seats (initial dynamic stop = entry − 2 × ATR14); never fatal for the page
  var seatSyms = []; for (i = 0; i < seats.length; i++) { var ssym = dp2Str_(seats[i]['Symbol']).trim(); if (ssym) seatSyms.push(ssym); }
  var xbars = {};
  try { xbars = dp2LoadBars_(seatSyms); } catch (eX) { xbars = {}; }
  // research allocation (conviction mirror) + rotation funding plans
  var mp = dp2ReadMyPortfolio_();
  var alloc = null;
  try { alloc = dp2ResearchAllocation_(t, led, mp, ib, cashSar || 0, fx, xbars); } catch (eA) { alloc = null; }
  var qualified = t.qualified;
  var qCount = t.qualifiedCount || qualified.length;
  var inv = gm.counts['INVESTABLE'] || null;
  var capSector = dp2Num_(t.panel['T10: Max Per Sector']) || 2;
  var state = /^ACTIONABLE/.test(feed.state) ? 'ACTIONABLE' : (/^NOT_ACTIONABLE/.test(feed.state) ? 'NOT ACTIONABLE' : (feed.state || t.output || 'n/a'));
  var chipKind = state === 'ACTIONABLE' ? 'green' : (state === 'NOT ACTIONABLE' ? 'red' : 'amber');

  var sh = dp2Sheet_(DP2.SHEET_T10, true);
  dp2ResetSheet_(sh);
  dp2ChartsBegin_(DP2.SHEET_CHARTS_T10);
  var row = 1;
  dp2Header_(sh, row, 'TOP 10 INVESTMENTS — DECISION v2', state, chipKind, 'run ' + (feed.run || 'n/a') + ' · epoch ' + (t.epoch || 'n/a') + ' · cockpit ' + (t.lastRun || 'n/a'));
  row++;
  dp2Line_(sh, row, 'Blocker: ' + (feed.blocker || 'none') + (feed.reasons ? ' → ' + feed.reasons : '') + (feed.reasons && /coverage_failed/.test(feed.reasons) ? ' (' + dp2CoverageNote_() + ')' : '') + (feed.reasons && /decision_failed/.test(feed.reasons) ? ' (decision pages ran before the sync landed: receipt run ≠ sync run)' : ''), { color: DP2C.ink, bg: DP2C.surface, height: 30 }); row++;
  dp2Line_(sh, row, 'Status: ' + t.status, { size: 8, bg: DP2C.surface, height: 20 }); row++;
  sh.setFrozenRows(row - 1);
  row++;

  var tiles = [
    { label: 'Scanned', value: dp2Fmt_(t.scanned), sub: '4 pages' + (t.duplicates !== null ? ' · ' + t.duplicates + ' duplicates removed' : '') },
    { label: 'Engine INVESTABLE', value: inv !== null ? dp2Fmt_(inv) : 'n/a', sub: gm.total ? dp2PctPlain_(inv / gm.total * 100, 1) + ' of Global_Markets ' + dp2Fmt_(gm.total) : 'Global_Markets column not found' },
    { label: 'Merit-qualified', value: dp2Fmt_(qCount), sub: (t.gapsTotal ? 'of ' + t.gapsTotal + ' audited · ' : '') + (function () { var f = 0; for (var i = 0; i < qualified.length; i++) if (dp2Str_(qualified[i]['Sector']) === 'Financials') f++; return f + ' Financials'; })() },
    { label: 'Held seats', value: dp2Fmt_(seats.length), sub: fast.length + ' fast-track · ' + grace.length + ' grace-held' },
    { label: 'Executable', value: dp2Fmt_(executable), sub: 'of ' + seats.length + ' · ' + (t.output || ''), color: executable > 0 ? DP2C.green : DP2C.red },
    { label: 'Entered cash', value: dp2Fmt_(cashSar) + ' SAR', sub: cashSource + ' · deployable after the engine\'s reserve ' + dp2Fmt_(alloc ? alloc.deployable : deploy) + (alloc && alloc.reserve ? ' (' + (alloc.floorSar > 0 ? 'reserve ' + dp2Fmt_(alloc.floorSar) : 'no reserve') + ')' : '') },
    { label: 'Allocated (research)', value: alloc ? dp2Fmt_(alloc.allocated) + ' SAR' : 'n/a', sub: alloc ? ('of deployable ' + dp2Fmt_(alloc.deployable) + ' · ' + alloc.rows.filter(function (r) { return r.state === 'FUNDED'; }).length + ' of ' + alloc.plans.length + ' plans funded · cap ' + dp2Fmt_(alloc.cap) + '/seat') : 'allocation unavailable', color: alloc && alloc.allocated > 0 ? DP2C.green : DP2C.ink },
    { label: 'Model evidence', value: (function () { for (var i = 0; i < hit.labels.length; i++) if (hit.labels[i].label === 'INVESTABLE' && hit.labels[i].rate !== null) return dp2PctPlain_(hit.labels[i].rate * 100, 1); return 'n/a'; })(), sub: 'INVESTABLE hit rate, matured records', color: DP2C.amber }
  ];
  row = dp2Tiles_(sh, row, tiles); row++;

  // charts: funnel + universe/gates
  row = dp2Section_(sh, row, 1, 8, 'DECISION FUNNEL — where the day stops', 'Each bar = share of the previous stage that survives (not the absolute count).');
  dp2Section_(sh, row - 2, 9, 16, 'UNIVERSE AND GATES', 'Global_Markets investability split and the audited-pool gates (DATA GAPS block).');
  var chartRow = row;
  for (i = 0; i < 12; i++) dp2Range_(sh, row + i, 1, 16).setBackground(DP2C.surface);
  row += 12;
  var stages = [['Scanned → engine INVESTABLE', (inv !== null && t.scanned) ? inv / t.scanned : null, inv],
                ['INVESTABLE → merit-qualified', (inv && qCount) ? qCount / inv : null, qCount],
                ['Qualified → held seats', (qCount && seats.length) ? seats.length / qCount : null, seats.length],
                ['Seats → fast-track plans', seats.length ? fast.length / seats.length : null, fast.length],
                ['Plans → executable', fast.length ? executable / fast.length : 0, executable]];
  var fRows = stages.filter(function (s) { return s[1] !== null; }).map(function (s) { return [s[0] + ' (' + dp2Fmt_(s[2]) + ')', s[1]]; });
  var rngF = dp2ChartData_('Funnel', ['Stage', 'Conversion'], fRows);
  dp2Chart_(sh, 'BAR', rngF, chartRow, 1, { width: 680, height: 250, colors: [DP2C.blue], hAxis: { format: '0%', minValue: 0, maxValue: 1, gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 } }, vAxis: { textStyle: { color: DP2C.ink2, fontSize: 9 } }, chartArea: { left: 220, top: 12, width: '62%', height: '80%' }, bar: { groupWidth: '55%' } });
  var uRows = [];
  var order = ['WATCHLIST', 'INVESTABLE', 'BLOCKED'];
  for (i = 0; i < order.length; i++) if (gm.counts[order[i]]) uRows.push(['GM ' + order[i] + ' (' + dp2Fmt_(gm.counts[order[i]]) + ')', gm.total ? gm.counts[order[i]] / gm.total : 0]);
  for (i = 0; i < t.gaps.length; i++) uRows.push(['Gate: ' + t.gaps[i].gate + ' failed (' + dp2Fmt_(t.gaps[i].failed) + ')', t.gapsTotal ? t.gaps[i].failed / t.gapsTotal : 0]);
  if (t.gapsPassed !== null && t.gapsTotal) uRows.push(['Passed all gates (' + t.gapsPassed + ')', t.gapsPassed / t.gapsTotal]);
  var rngU = dp2ChartData_('Universe', ['Bucket', 'Share'], uRows);
  dp2Chart_(sh, 'BAR', rngU, chartRow, 9, { width: 680, height: 250, colors: [DP2C.grey], hAxis: { format: '0%', minValue: 0, maxValue: 1, gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 } }, vAxis: { textStyle: { color: DP2C.ink2, fontSize: 9 } }, chartArea: { left: 220, top: 12, width: '62%', height: '80%' }, bar: { groupWidth: '55%' } });
  row++;

  // seats table — 3-month close, research allocation (conviction mirror), initial dynamic stop
  row = dp2Section_(sh, row, 1, 16, 'SEATS — ' + seats.length + ' · ' + fast.length + ' plans · ' + grace.length + ' grace-held', 'Alloc / Buy qty = research allocation of the deployable cash across the FAST-TRACK plans, mirroring the engine\'s conviction sizing (weight = plan ROI × risk × reliability, per-seat cap ' + (alloc && alloc.cfg ? alloc.cfg.maxWeightPct : DP2A.MAX_WEIGHT_PCT) + '% of portfolio + cash (pre-reserve, the engine\'s budget_base), minimum ticket ' + (alloc && alloc.cfg && alloc.cfg.envPresent ? dp2Fmt_(alloc.cfg.opFloor) + ' SAR' + (alloc.cfg.venueOn ? ' raised to the venue floor' : '') : dp2Fmt_(DP2A.MIN_TICKET_SAR) + ' SAR research floor (DP2_ENGINE_ENV unset)') + ', the engine\'s own cash reserve). GRACE seats are not sized. Dyn. stop = entry − 2 × ATR14; Risk SAR = loss if that stop is hit; Exp. gain 3M = allocation × plan ROI. The engine\'s own tickets (when the feed is released) are the executable ones.');
  var sHdr = ['#', 'Symbol · name', '3-month close', 'Seat status', 'Price', 'Alloc SAR', 'Buy qty', 'Dyn. stop (2×ATR)', 'Risk SAR', 'Exp. gain 3M', 'ROI 3M plan', 'ROI 12M eng.', 'Rel · DQ · score', 'Flags'];
  var sSp = [1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2];
  var sRows = [], sRisk = [], sState = [];
  var secSeats = {};
  var qmap = {};
  var seatFresh = 0, seatSources = {};
  for (i = 0; i < qualified.length; i++) qmap[dp2Str_(qualified[i]['Symbol']).trim()] = qualified[i];
  for (i = 0; i < seats.length; i++) {
    var s = seats[i];
    var sym = dp2Str_(s['Symbol']).trim(), sec = dp2Str_(s['Sector']);
    var qrow = qmap[sym] || {};
    secSeats[sec] = (secSeats[sec] || 0) + 1;
    var price = dp2Num_(s['Price']);
    var xb = xbars[sym] || null;
    var xatr = null, xlast = null;
    if (xb && xb.bars && xb.bars.length > DP2X.ATR_N) {
      var atrArr = dp2Atr_(xb.bars, DP2X.ATR_N); xatr = atrArr[atrArr.length - 1];
      if (!xb.stale) { seatFresh++; if (xb.last) xlast = xb.last; }
      if (xb.source) seatSources[xb.source] = true;
    }
    var entry = (xlast !== null && xlast > 0) ? xlast : price;
    var roi3 = dp2Num_(s['ROI % (TP1)']); if (roi3 === null) roi3 = dp2Num_(qrow['ROI % (TP1)']);
    var roi12 = dp2Num_(s['Engine ROI % (12M)']); if (roi12 === null) roi12 = dp2Num_(qrow['Engine ROI % (12M)']);
    var rrCol = dp2Num_(qrow['R/R (TP2)']);
    var riskQ = dp2Str_(qrow['Risk']);
    var ccy = dp2Str_(s['Ccy']);
    var fxRow = dp2Num_(s['FX→SAR']);
    var fxUse = fxRow !== null ? fxRow : (ccy === 'USD' ? fx : (ccy === 'SAR' ? 1 : null));
    var stability = dp2Str_(s['Stability']);
    var isPlan = /FAST-TRACK/.test(stability);
    var ar = (alloc && alloc.byRow[sym]) || null;
    var planObj = null; if (alloc) { for (var pj = 0; pj < alloc.plans.length; pj++) if (alloc.plans[pj].sym === sym) planObj = alloc.plans[pj]; }
    if (fxUse === null && planObj && planObj.fxUse !== null) fxUse = planObj.fxUse;
    var shares = ar && ar.state === 'FUNDED' ? ar.qty : null;
    var allocSar = ar && ar.state === 'FUNDED' ? ar.alloc : null;
    var stop0 = (entry !== null && xatr !== null) ? entry - DP2X.ATR_MULT_STOP * xatr : null;
    var riskSar = (stop0 !== null && shares !== null && fxUse !== null) ? shares * (entry - stop0) * fxUse : null;
    var gain3 = (allocSar !== null && roi3 !== null) ? allocSar * roi3 / 100 : null;
    var note = dp2Str_(s['Advisor Note']);
    var flags = [];
    if (ar && ar.state === 'NEED') flags.push('UNFUNDED — need ' + dp2Fmt_(ar.need) + ' SAR (' + ar.note + ') → see FUNDING PLANS');   // v1.2.18: the shortfall is the funding plan's (cash left after every seat), not the row's
    if (ar && ar.state === 'RELEASED') flags.push('released: ' + ar.note);
    if (ar && ar.state === 'CAP_INFEASIBLE') flags.push('cap-infeasible: ' + ar.note);
    if (ar && ar.state === 'SECTOR_CAP') flags.push('sector cap: ' + ar.note);
    if (ar && ar.state === 'UNFUNDED') flags.push('unfunded: ' + ar.note);
    if (ar && ar.state === 'NO_FX') flags.push(ar.note);
    if (!isPlan) flags.push('grace-held · not sized');
    var mearn = /earnings ≤\s?(\d+)d/.exec(note); if (mearn) flags.push('earnings ≤ ' + mearn[1] + ' d');
    if (/52W/.test(note)) flags.push(/low/.test(note) ? '52W near low' : '52W flag');
    if (riskQ && riskQ !== 'Low') flags.push('risk ' + riskQ);
    if (secSeats[sec] > capSector) flags.push(sec + ' ' + secSeats[sec] + ' > cap ' + capSector);
    if (rrCol !== null) flags.push('R/R ' + dp2Fmt_(rrCol, 2));
    if (ar && ar.weight) flags.push('weight ' + dp2PctPlain_(alloc.deployable ? ar.target / alloc.deployable * 100 : null, 0) + ' of deployable');
    if (planObj && planObj.fxSource === 'approx') flags.push('FX ' + ccy + '→SAR approximate (' + dp2Fmt_(planObj.fxUse, 4) + ')');
    if (planObj && planObj.fxSource === 'chart API') flags.push('FX ' + ccy + '→SAR live ' + dp2Fmt_(planObj.fxUse, 4));
    if (riskSar !== null && navSar && riskSar / navSar * 100 > DP2X.RISK_WARN_PCT_NAV) flags.push('risk ' + dp2PctPlain_(riskSar / navSar * 100, 2) + ' NAV > ' + DP2X.RISK_WARN_PCT_NAV + '%');
    if (xb && xb.stale && xatr !== null) flags.push('ATR from cached bars');
    if (xb && !xb.bars.length) flags.push('no price history');
    sRisk.push(riskSar); sState.push(ar ? ar.state : (isPlan ? '' : 'GRACE'));
    sRows.push([dp2Num_(s['Rank']) !== null ? dp2Num_(s['Rank']) : '—', sym + ' · ' + dp2Str_(s['Name']) + '\n' + sec + ' · ' + dp2Str_(s['Market']), dp2SparkFormula_(sym), stability, entry !== null ? dp2Fmt_(entry, 2) + ' ' + ccy : '—', allocSar !== null ? allocSar : (isPlan ? '—' : ''), shares !== null ? shares : (isPlan ? '—' : ''), stop0 !== null ? dp2Fmt_(stop0, 2) + ' (' + dp2Pct_((stop0 / entry - 1) * 100, 1) + ')\nATR ' + dp2Fmt_(xatr, 2) : 'n/a', riskSar !== null ? riskSar : '—', gain3 !== null ? gain3 : '—', roi3 !== null ? roi3 / 100 : null, roi12 !== null ? roi12 / 100 : null, dp2Fmt_(dp2Num_(s['Rel']), 1) + ' · ' + dp2Fmt_(dp2Num_(s['DQ']), 0) + ' · ' + dp2Fmt_(dp2Num_(s['Sm Score']), 1), flags.join(' · ')]);
  }
  var seatTop = row;
  row = dp2Table_(sh, row, sHdr, sRows, { spans: sSp, align: ['r', 'l', 'c', 'l', 'r', 'r', 'r', 'r', 'r', 'r', 'r', 'r', 'r', 'l'], fmt: ['0', '', '', '', '', '#,##0', '#,##0', '', '#,##0', '#,##0', '0.0%', '0.0%', '', ''], wrap: [false, true, false, true, false, false, false, true, false, false, false, false, false, true], rowHeight: 36 });
  for (i = 0; i < sRows.length; i++) {
    var srr = seatTop + 1 + i;
    var sc = sh.getRange(srr, 5); var stxt = dp2Str_(sRows[i][3]); sc.setFontColor(/FAST-TRACK/.test(stxt) ? DP2C.navyText : DP2C.amber).setFontWeight('bold').setFontSize(8.5);
    sh.getRange(srr, 2, 1, 2).setFontSize(8.5);
    sh.getRange(srr, 9).setFontSize(8.5).setFontColor(DP2C.ink2);
    var rk = sRisk[i]; if (rk !== null && navSar && rk / navSar * 100 > DP2X.RISK_WARN_PCT_NAV) sh.getRange(srr, 10).setFontColor(DP2C.red).setFontWeight('bold');
    var ac = sh.getRange(srr, 7, 1, 2).setFontWeight('bold');
    if (sState[i] === 'FUNDED') ac.setFontColor(DP2C.green); else if (sState[i] === 'NEED') ac.setFontColor(DP2C.amber);
  }
  dp2Line_(sh, row, 'Price history for the stops: ' + seatFresh + ' of ' + seats.length + ' seats on fresh bars (' + (Object.keys(seatSources).join(', ') || 'none') + ') · prices = live last when available, else the cockpit price · dynamic stops for HELD names run on Portfolio_Decision_v2 with e-mail alerts.', { size: 8.5, color: DP2C.ink2, bg: DP2C.surface, height: 22 });
  row += 2;

  // allocation + funding plans / rotation
  row = dp2Section_(sh, row, 1, 8, 'ALLOCATION — research mirror of the engine\'s conviction sizing', 'Same formula as opportunity_builder v1.27.0 (TFB_OPP_SIZING_MODE=conviction): weights water-filled against the deployable cash. The engine sizes only when the feed is released.');
  var rpH = (alloc && alloc.rotParams) || dp2RotParams_();   // v1.2.17: the production parameters in use
  dp2Section_(sh, row - 2, 9, 16, 'FUNDING PLANS & ROTATION — when cash does not cover the plans', 'One rotation per run: the held equity with the lowest forward ROI that trails the opportunity by ≥ ' + rpH.edge12 + ' pp (engine 12M) or ≥ ' + rpH.edge3 + ' pp (plan 3M) after ' + rpH.cost + '% cost, on the production basis (TFB_OPP_ROTATION_BASIS).');
  var aTop = row;
  if (alloc) {
    var evr = (alloc.riskSar !== null && alloc.riskSar > 0) ? alloc.gain3 / alloc.riskSar : null;
    var aRows = [
      ['Cash available (' + cashSource + ')', dp2Fmt_(alloc.cash) + ' SAR'],
      ['Cash check (other reading)', cb.other !== null ? cb.otherLabel + ': ' + dp2Fmt_(cb.other) + ' SAR (' + (cb.diffPct >= 0 ? '+' : '') + dp2Fmt_(cb.diffPct, 1) + '%)' + (cb.warn ? ' — RECONCILE before placing orders' : ' · consistent') : 'no second reading', cb.warn ? DP2C.amber : DP2C.ink2],
      ['Cash reserve taken before sizing (engine)', dp2Fmt_(alloc.floorSar) + ' SAR — ' + (alloc.reserve ? alloc.reserve.source : 'n/a') + ' · PF policy floor ' + Math.round(DP2.CASH_FLOOR_PCT * 100) + '% of NAV ' + dp2Fmt_(alloc.nav) + ' = ' + dp2Fmt_(alloc.reserve ? alloc.reserve.pfFloor : alloc.nav * DP2.CASH_FLOOR_PCT) + ' SAR is a LIMITS check, not a sizing input', alloc.floorSar > 0 ? DP2C.ink : DP2C.ink2],
      ['Deployable', dp2Fmt_(alloc.deployable) + ' SAR', DP2C.green],
      ['Per-seat cap ' + (alloc.cfg ? alloc.cfg.maxWeightPct : DP2A.MAX_WEIGHT_PCT) + '% × (portfolio ' + dp2Fmt_(alloc.pv) + ' + cash ' + dp2Fmt_(alloc.cash) + ', pre-reserve — the engine\'s budget_base)', dp2Fmt_(alloc.cap) + ' SAR · ' + (alloc.cfg ? alloc.cfg.capSource : ''), alloc.cfg && alloc.cfg.envPresent ? DP2C.ink : DP2C.amber],
      ['Plans in the sizing cohort', alloc.plans.length + ' · ' + alloc.plans.map(function (p) { return p.sym + ' (' + (p.roiVal !== null ? dp2PctPlain_(p.roiVal, 1) + ' val' + (p.roiValSource === 'tp1x2' ? '≈' : '') : 'ROI n/a → ' + DP2A.REQUIRED_ROI_PCT + '%') + ')'; }).join(', ') + ' · weight = valuation ROI × risk × reliability (engine roi_pct) · caps ' + (function () { var c = dp2CohortCaps_(t); return c.maxSelected + ' seats / ' + c.maxPerSector + ' per sector / ' + c.maxPerMarket + ' per market (' + (c.fromPanel ? 'Top_10 panel' : 'defaults') + ')'; })()],
      ['Allocated · cash after', dp2Fmt_(alloc.allocated) + ' SAR · ' + dp2Fmt_(alloc.cashAfter) + ' SAR left' + (alloc.topup ? ' · incl. lot-rounding top-up ' + dp2Fmt_(alloc.topup) + ' SAR (engine pass)' : ''), alloc.allocated > 0 ? DP2C.green : DP2C.amber],
      ['Expected gain, 3-month plan', dp2Signed_(alloc.gain3) + ' SAR', alloc.gain3 > 0 ? DP2C.green : DP2C.ink],
      ['Risk if every initial stop is hit', alloc.riskSar !== null ? dp2Signed_(-alloc.riskSar) + ' SAR' + (alloc.nav ? ' (' + dp2PctPlain_(alloc.riskSar / alloc.nav * 100, 2) + ' NAV)' : '') + (alloc.riskKnown < alloc.rows.filter(function (r) { return r.state === 'FUNDED'; }).length ? ' · ' + alloc.riskKnown + ' of the funded seats have ATR' : '') : 'n/a — no price history for the seats yet', alloc.riskSar !== null ? DP2C.red : DP2C.ink2],
      ['Reward per riyal at risk (EV / risk)', evr !== null ? dp2Fmt_(evr, 2) + 'x' : 'n/a', evr !== null && evr >= 1.5 ? DP2C.green : DP2C.amber],
      ['Production configuration (DP2_ENGINE_ENV)', alloc.cfg ? alloc.cfg.summary : 'n/a', alloc.cfg && alloc.cfg.planReady ? DP2C.ink : DP2C.amber],
      ['Unfunded plans — production rule' + (alloc.cfg && alloc.cfg.envPresent ? ' (' + alloc.cfg.sizingMode + ' mode)' : '') + ' — need = the seat\'s effective minimum ticket', (alloc.needsProd || alloc.needs).length ? (alloc.needsProd || alloc.needs).map(function (n) { return n.sym + ' ' + dp2Fmt_(n.need); }).join(' · ') + (alloc.fundingWithheld ? ' · research floor — ' + alloc.fundingWithheld : ' · a rotation funds that minimum ticket (raise TFB_OPP_MIN_TICKET_SAR for a larger switch)') : (alloc.rows.some(function (r) { return r.state === 'RELEASED'; }) ? 'none — ' + alloc.rows.filter(function (r) { return r.state === 'RELEASED'; }).map(function (r) { return r.sym; }).join(', ') + ' released by the floor rule (deferred, no need)' : 'none')]
    ];
    var a1 = dp2KV_(sh, aTop, 1, 8, aRows);
    var pLines = [];
    pLines.push('Production basis: ' + dp2RotationBasisLabel_(alloc.basis) + ' (' + dp2RotationBasisSrcTxt_(alloc.basisInfo) + ') — only its plans drive SWITCH actions; the other basis is research.' + (alloc.rotParams ? ' Rotation rule: hurdle ' + alloc.rotParams.edge12 + ' pp (12M) / ' + alloc.rotParams.edge3 + ' pp (3M) after ' + alloc.rotParams.cost + '% cost, held ≥ ' + alloc.rotParams.minHeld + ' d, not within ' + alloc.rotParams.tp1Prox + '% of TP1, excluded ' + (alloc.rotParams.exclude.join(', ') || 'none') + ' (' + alloc.rotParams.source + ').' : '') + (alloc.prod && alloc.prod !== alloc ? ' Needs come from production\'s ' + alloc.prod.mode + '-mode sizing; the ALLOCATION block shows the conviction rule.' : ''));
    if (alloc.fundingWithheld) pLines.push('ROTATION PLANS WITHHELD — ' + alloc.fundingWithheld + '.');
    for (i = 0; i < alloc.plans12.length; i++) pLines.push('• 12M basis' + (alloc.basis === '12m' ? ' (production)' : ' (research)') + ': ' + dp2PlanText_(alloc.plans12[i]));
    for (i = 0; i < alloc.plans3.length; i++) pLines.push('• 3M basis' + (alloc.basis === 'plan3m' ? ' (production)' : ' (research)') + ': ' + dp2PlanText_(alloc.plans3[i]));
    if (!alloc.fundingWithheld) { var ccP = dp2CapitalCall_(alloc.plansProd, alloc.cfg ? alloc.cfg.capitalCallTopN : 3); if (ccP.sum > 0) pLines.push('Capital call on the production basis (top-' + ccP.topN + ' plans, TFB_OPP_CAPITAL_CALL_TOPN — as the engine\'s KPI and alert): deposit ≥ ' + dp2Fmt_(ccP.sum) + ' SAR to take ' + ccP.syms.join(', ') + '.'); }
    if (!(alloc.needsProd || alloc.needs).length && !alloc.fundingWithheld) pLines.push('No funding need on the production rule — no rotation today' + (alloc.rows.some(function (r) { return r.state === 'RELEASED' || r.state === 'UNFUNDED'; }) ? ' (released / unfunded seats are deferrals, not needs — as the engine).' : '.'));
    var holdTxt = alloc.holdings.filter(function (h) { return !h.isFI; }).map(function (h) { return h.symbol + ' fwd 12M ' + dp2PctPlain_(h.roi12, 1) + ' · 3M plan ' + (h.roi3 !== null ? dp2PctPlain_(h.roi3, 1) + ' (' + h.roi3Src + ')' : 'n/a — no TP1 ladder, never rotates on 3M') + (h.roi3Model !== null ? ' · model 3M ' + dp2PctPlain_(h.roi3Model, 1) : '') + ' · ' + dp2Fmt_(h.valueSar) + ' SAR · ' + (h.holdingDays !== null ? h.holdingDays + ' d' : '—'); }).join('\n');
    pLines.push('Holdings (12M = My_Portfolio engine forecast · 3M = plan-TP1 ROI, as the engine; the model 3M is shown, not used):\n' + (holdTxt || 'none'));
    var pRng = dp2Range_(sh, aTop, 9, 16, Math.max(aRows.length, 6)).merge();
    pRng.setValue(pLines.join('\n')).setWrap(true).setVerticalAlignment('top').setFontSize(9).setBackground(DP2C.surface).setBorder(true, true, true, true, false, false, DP2C.border, SpreadsheetApp.BorderStyle.SOLID);
    for (i = 0; i < Math.max(aRows.length, 6); i++) sh.setRowHeight(aTop + i, 24);
    row = Math.max(a1, aTop + Math.max(aRows.length, 6)) + 1;
  } else {
    dp2Line_(sh, row, 'Allocation unavailable (Top_10 read or ledger missing).', { bg: DP2C.surface });
    row += 2;
  }

  // scatter + concentration
  row = dp2Section_(sh, row, 1, 8, 'THE ' + qCount + ' QUALIFIED — risk/reward vs stability score', 'Blue = Financials, orange = other sectors. Hover a point for the symbol.');
  dp2Section_(sh, row - 2, 9, 16, 'CONCENTRATION vs the per-sector cap (' + capSector + ')', 'Qualified set by sector, and held seats by sector; the cap applies at selection.');
  chartRow = row;
  for (i = 0; i < 14; i++) dp2Range_(sh, row + i, 1, 16).setBackground(DP2C.surface);
  row += 14;
  var scRows = [];
  var secQ = {};
  for (i = 0; i < qualified.length; i++) {
    var q = qualified[i];
    var qsec = dp2Str_(q['Sector']);
    secQ[qsec] = (secQ[qsec] || 0) + 1;
    var qrr = dp2Num_(q['R/R (TP2)']), qsc = dp2Num_(q['Score']);
    if (qrr === null || qsc === null) continue;
    scRows.push([qrr, qsec === 'Financials' ? qsc : null, qsec === 'Financials' ? null : qsc]);
  }
  if (scRows.length) {
    var rngS = dp2ChartData_('Scatter', ['R/R (TP2)', 'Financials', 'Other sectors'], scRows.map(function (r) { return [r[0], r[1] === null ? '' : r[1], r[2] === null ? '' : r[2]]; }));
    dp2Chart_(sh, 'SCATTER', rngS, chartRow, 1, { width: 680, height: 290, colors: [DP2C.blue, DP2C.orange], pointSize: 7, legend: { position: 'top', textStyle: { color: DP2C.ink2, fontSize: 9 } }, hAxis: { title: 'Risk / reward (TP2)', minValue: 1.8, gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 }, titleTextStyle: { color: DP2C.ink2, fontSize: 9, italic: false } }, vAxis: { title: 'Stability score', gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 }, titleTextStyle: { color: DP2C.ink2, fontSize: 9, italic: false } }, chartArea: { left: 50, top: 30, width: '88%', height: '70%' } });
  }
  var cRows = [];
  var secQKeys = Object.keys(secQ).sort(function (a, b) { return secQ[b] - secQ[a]; });
  for (i = 0; i < secQKeys.length; i++) cRows.push(['Qualified · ' + secQKeys[i], secQ[secQKeys[i]], 0]);
  var secSKeys = Object.keys(secSeats).sort(function (a, b) { return secSeats[b] - secSeats[a]; });
  for (i = 0; i < secSKeys.length; i++) cRows.push(['Seats · ' + secSKeys[i], 0, secSeats[secSKeys[i]]]);
  var rngC = dp2ChartData_('Concentration', ['Sector', 'Qualified (' + qCount + ')', 'Held seats (' + seats.length + ')'], cRows);
  dp2Chart_(sh, 'BAR', rngC, chartRow, 9, { width: 680, height: 290, colors: [DP2C.blue, DP2C.orange], legend: { position: 'top', textStyle: { color: DP2C.ink2, fontSize: 9 } }, hAxis: { minValue: 0, gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 } }, vAxis: { textStyle: { color: DP2C.ink2, fontSize: 9 } }, chartArea: { left: 190, top: 28, width: '66%', height: '72%' }, bar: { groupWidth: '60%' } });
  row++;

  // model evidence + alerts/next step
  row = dp2Section_(sh, row, 1, 8, 'MODEL EVIDENCE — does the label predict anything?', 'Hit rate of matured Performance_Log records by engine label; 50% is a coin flip.');
  dp2Section_(sh, row - 2, 9, 16, 'ALERTS, GUARDS IN OBSERVE, NEXT STEP', 'From _Status and the cockpit status line.');
  chartRow = row;
  for (i = 0; i < 9; i++) dp2Range_(sh, row + i, 1, 16).setBackground(DP2C.surface);
  var hRows = [];
  for (i = 0; i < hit.labels.length; i++) if (hit.labels[i].rate !== null) hRows.push([hit.labels[i].label + ' (n ' + dp2Fmt_(hit.labels[i].n) + ')', hit.labels[i].rate, 0.5]);
  if (hRows.length) {
    var rngH = dp2ChartData_('HitRate', ['Label', 'Hit rate', 'Coin flip'], hRows);
    dp2Chart_(sh, 'BAR', rngH, chartRow, 1, { width: 680, height: 185, colors: [DP2C.blue, '#c3c2b7'], legend: { position: 'top', textStyle: { color: DP2C.ink2, fontSize: 9 } }, hAxis: { format: '0%', minValue: 0, maxValue: 1, gridlines: { color: '#e1e0d9' }, textStyle: { color: DP2C.muted, fontSize: 9 } }, vAxis: { textStyle: { color: DP2C.ink2, fontSize: 9 } }, chartArea: { left: 170, top: 28, width: '68%', height: '66%' }, bar: { groupWidth: '60%' } });
  }
  var alerts = [];
  if (feed.state) alerts.push('feed ' + feed.state + (feed.blocker ? ' · ' + feed.blocker : ''));
  var ma = /\| earn ⚠(\d+)\/(\d+)/.exec(t.status); if (ma) alerts.push('earnings ≤ 12 d on ' + ma[1] + ' of ' + ma[2] + ' seats (advisory)');
  for (i = 0; i < t.gaps.length; i++) alerts.push('gate ' + t.gaps[i].gate + ': ' + dp2Fmt_(t.gaps[i].failed) + ' failed (' + dp2PctPlain_(t.gaps[i].share, 1) + ')');
  if (kv['TFB Feed Global_Markets']) alerts.push('GM feed ' + kv['TFB Feed Global_Markets'].split('|').slice(0, 2).join(' ·').trim());
  if (kv['TFB Grid Capacity']) alerts.push('grid ' + kv['TFB Grid Capacity'].split('|')[1].trim());
  var pendingS1 = s1.criteria.filter(function (c) { return c.status !== 'PASS'; }).map(function (c) { return c.n + ' (' + c.detail.split('·')[0].trim() + ')'; }).join('; ');
  var covBad = []; try { covBad = dp2NameCoverage_().filter(function (x) { return x.fail; }); } catch (eNc) { covBad = []; }
  var next = 'Path to the first executable board: (1) Name coverage ≥ 99% on every market page (D3' + (covBad.length ? ': ' + dp2CoverageNote_() : '') + '); (2) the decision pages refreshed after the sync lands so receipt = sync run (26_Decision_Chain, TFB_DECISION_CHAIN=on; DT10_FEED_COHORT_BIND=on); (3) Portfolio_Decision refreshed within 14 min of the morning IBKR capture (pdInstallCaptureWatcher). Then the stability rule (3 confirmed days) and the S-1 gate (' + (s1.verdict || 'n/a') + (pendingS1 ? ' — pending ' + pendingS1 : '') + ') decide whether capital moves' + (s1.criteria.some(function (c) { return String(c.n) === '4' && c.status !== 'PASS'; }) ? '; S-1 criterion 4 is refreshed only by an audited Performance Clock run (scheduled runs are record-only since 12 Oct)' : '') + '.';
  var alertRng = dp2Range_(sh, chartRow, 9, 16, 4).merge();
  alertRng.setValue(alerts.join('\n')).setWrap(true).setVerticalAlignment('top').setFontSize(9).setBackground(DP2C.surface).setBorder(true, true, true, true, false, false, DP2C.border, SpreadsheetApp.BorderStyle.SOLID);
  var nextRng = dp2Range_(sh, chartRow + 4, 9, 16, 5).merge();
  nextRng.setValue(next).setWrap(true).setVerticalAlignment('top').setFontSize(9.5).setBackground(DP2C.surface).setBorder(true, true, true, true, false, false, DP2C.border, SpreadsheetApp.BorderStyle.SOLID);
  row += 9; row++;

  // all qualified table
  row = dp2Section_(sh, row, 1, 16, 'ALL QUALIFIED — INVEST opportunity set (' + qCount + ')', 'FLIP(strict) = the 3-month plan ROI is below the requirement when measured strictly (F-1 observe).');
  var qHdr = ['#', 'Symbol · name', 'Sector', 'Score', 'ROI 3M plan', 'ROI 12M eng.', 'Ann. ROI', 'R/R', 'Rel', 'DQ', 'Risk', 'Stability', 'Why not selected / tags'];
  var qSp = [1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2];
  var qRows = [];
  for (i = 0; i < qualified.length; i++) {
    var qq = qualified[i];
    var why = dp2Str_(qq['Why Not Selected']);
    var tags = [];
    if (/FLIP\(strict\)/.test(why)) tags.push('FLIP(strict)');
    if (/52W/.test(why)) tags.push(/low/.test(why) ? '52W near low' : '52W flag');
    var stabM = /awaiting confirmation \((\d\/\d) days\)/.exec(why); var stab = stabM ? 'awaiting ' + stabM[1] : (/FAST-TRACK/.test(why) ? 'FAST-TRACK' : (/GRACE/.test(why) ? 'GRACE' : ''));
    var r3 = dp2Num_(qq['ROI % (TP1)']), r12 = dp2Num_(qq['Engine ROI % (12M)']), ann = dp2Num_(qq['Ann ROI %']);
    qRows.push([dp2Num_(qq['Rank']), dp2Str_(qq['Symbol']) + ' · ' + dp2Str_(qq['Name']), dp2Str_(qq['Sector']), dp2Num_(qq['Score']), r3 !== null ? r3 / 100 : null, r12 !== null ? r12 / 100 : null, ann !== null ? ann / 100 : null, dp2Num_(qq['R/R (TP2)']), dp2Num_(qq['Rel']), dp2Num_(qq['DQ']), dp2Str_(qq['Risk']), stab, tags.join(' · ') || why.replace(/stability: /, '').slice(0, 90)]);
  }
  row = dp2Table_(sh, row, qHdr, qRows, { spans: qSp, align: ['r', 'l', 'l', 'r', 'r', 'r', 'r', 'r', 'r', 'r', 'l', 'l', 'l'], fmt: ['0', '', '', '0.0', '0.0%', '0.0%', '0.0%', '0.00', '0.0', '0', '', '', ''], wrap: [false, true, false, false, false, false, false, false, false, false, false, false, true], rowHeight: 22 });
  row++;

  // control panel
  row = dp2Section_(sh, row, 1, 16, 'CONTROL PANEL — operator inputs as read from Top_10_Investments (edit them there; blanks re-seed from _Lists_Config)', null);
  var keys = Object.keys(t.panel);
  var pr = row, pc = 1;
  for (i = 0; i < keys.length; i++) {
    var kcell = dp2Range_(sh, pr, pc, pc + 2).merge(); kcell.setValue(keys[i].replace(/^T10: /, '')).setFontSize(9).setFontColor(DP2C.ink2).setBackground(DP2C.surface);
    var vcell = sh.getRange(pr, pc + 3); vcell.setValue(t.panel[keys[i]]).setFontSize(9).setFontColor('#0b5fff').setBackground(DP2C.surface).setHorizontalAlignment('right');
    pc += 4;
    if (pc > 13) { pc = 1; pr++; }
  }
  row = pr + (pc > 1 ? 1 : 0) + 1;
  dp2Line_(sh, row, 'Decision Pages v2 ' + DP2_VERSION + ' · built ' + dp2Today_() + ' · read-only view over Top_10_Investments, Global_Markets, Performance_Log, _Status, S1_Gate, the ledger and the price-history cache (' + DP2X.SHEET_BARS + ') — nothing on this sheet feeds a gate or a ticket.', { size: 8, color: DP2C.muted });
  return { rows: row, seats: seats.length, qualified: qCount, executable: executable, seatBarsFresh: seatFresh };
}

/* ========================================================= self-test */
function dp2SelfTest() {
  var checks = [];
  var ss = dp2Ss_();
  var need = [DP2.SRC_T10, DP2.SRC_PD, DP2.SRC_MP, DP2.SRC_LEDGER, DP2.SRC_STATUS, DP2.SRC_GM, DP2.SRC_PERF, DP2.SRC_S1];
  for (var i = 0; i < need.length; i++) checks.push({ name: 'source tab ' + need[i], ok: !!ss.getSheetByName(need[i]) });
  var t = dp2ReadTop10_(); checks.push({ name: 'Top_10 status parsed (scanned ' + t.scanned + ', seats ' + t.seats.length + ', qualified ' + t.qualified.length + ')', ok: t.ok && t.seats.length > 0 && t.qualified.length > 0 });
  var led = dp2ReadLedger_(); checks.push({ name: 'ledger active lots ' + led.lots.length, ok: led.ok && led.lots.length > 0 });
  var mp = dp2ReadMyPortfolio_(); checks.push({ name: 'My_Portfolio rows ' + Object.keys(mp).length, ok: Object.keys(mp).length > 0 });
  var gm = dp2ReadGmInvestability_(); checks.push({ name: 'Global_Markets Investability Status (' + gm.total + ' rows)', ok: gm.total > 0 });
  var hit = dp2ReadHitRates_(); checks.push({ name: 'Performance_Log matured hit rates', ok: hit.ok && hit.labels.length > 0 && hit.labels[3].n > 0 });
  var kv = dp2ReadStatusKV_(); checks.push({ name: '_Status Decision Feed key', ok: !!kv['TFB Decision Feed'] });
  var ib = dp2ReadIbkr_(); checks.push({ name: 'IBKR tabs (orders ' + ib.orders.length + ', trades ' + ib.trades.length + ', nav ' + ib.nav.length + (ib.ageH !== null ? ', snapshot ' + dp2Fmt_(ib.ageH, 0) + ' h old' + (ib.stale ? ' STALE' : '') : '') + ')' + (ib.ordersKnown ? '' : ' — _IBKR_Orders missing/empty: refresh renders "broker orders unknown" (🌱 Seed IBKR Tabs or paste the connector read)'), ok: ib.ordersKnown && !ib.stale });
  // dynamic exits: the rule replayed on the embedded YUM fixture (24 sh @ 144.61, 12 Aug 2026; fixed stop 136.01 filled 1 Oct)
  var yb = dp2FixtureBars_(DP2_FIXTURE_YUM_);
  var yx = dp2ExitCompute_(yb, 144.61, '2026-08-12', null);
  var yOk = !!(yx && yx.firstMom && yx.firstMom.date === '2026-08-27' && Math.abs(yx.firstMom.price - 150.75) < 0.01 && Math.abs(yx.s0 - 135.27) < 0.02 && yx.firstHit && yx.firstHit.date === '2026-09-08');
  checks.push({ name: 'exit rule on YUM fixture: S0 ' + (yx ? dp2Fmt_(yx.s0, 2) : '—') + ' · SELL NOW ' + (yx && yx.firstMom ? yx.firstMom.date + ' @ ' + dp2Fmt_(yx.firstMom.price, 2) + ' (' + dp2Signed_((yx.firstMom.price - 144.61) * 24 * 3.75) + ' SAR)' : '—') + ' · trail hit ' + (yx && yx.firstHit ? yx.firstHit.date + ' @ ' + dp2Fmt_(yx.firstHit.stop, 2) : '—') + ' vs fixed stop 136.01 (' + dp2Signed_((136.01 - 144.61) * 24 * 3.75) + ' SAR)', ok: yOk });
  var wf = dp2WaterFill_({ A: 2, B: 1, C: 1 }, 100, 45, 10);
  var wfOk = Math.abs(wf.A - 45) < 0.01 && Math.abs(wf.B - 27.5) < 0.01 && Math.abs(wf.C - 27.5) < 0.01;
  var wf2 = dp2WaterFill_({ A: 10, B: 1 }, 100, 60, 20);   // A capped at 60, the surplus re-spread → B 40 (above the floor)
  var wf3 = dp2WaterFill_({ A: 10, B: 1 }, 100, 95, 20);   // A 90.9, B 9.1 < floor → B released → A capped at 95
  wfOk = wfOk && Math.abs(wf2.A - 60) < 0.01 && Math.abs(wf2.B - 40) < 0.01 && Math.abs(wf3.A - 95) < 0.01 && wf3.B === undefined;
  var rot = dp2RotationPick_(34.6, [{ symbol: 'CWBC.US', valueSar: 9664, roi12: 15.3, roi3: 5.4, holdingDays: 37, tp1: 29.6, price: 25.75, isFI: false }, { symbol: 'DDI.US', valueSar: 11860, roi12: 30.7, roi3: 12.9, holdingDays: 39, tp1: 15.03, price: 13.8, isFI: false }, { symbol: '5023.SR', valueSar: 9850, roi12: null, roi3: null, holdingDays: 328, tp1: null, price: 98.5, isFI: true }], '12m', 5600, dp2RotParamsDefault_());   // v1.2.17: engine defaults, whatever the properties say
  var rotOk = !!(rot && rot.symbol === 'CWBC.US' && Math.abs(rot.edge - 18.2) < 0.05);
  var rotNone = dp2RotationPick_(20, [{ symbol: 'DDI.US', valueSar: 11860, roi12: 30.7, roi3: 12.9, holdingDays: 39, tp1: null, price: 13.8, isFI: false }], '12m', 0, dp2RotParamsDefault_());
  checks.push({ name: 'research allocation: water-fill ' + JSON.stringify(wf) + ' · rotation pick ' + (rot ? rot.symbol + ' +' + rot.edge + 'pp' : 'none') + ' · no pick when the holding is better', ok: wfOk && rotOk && rotNone === null });
  var cfgS = dp2ProdConfig_(t);   // v1.2.20: the production configuration this workbook mirrors (informational)
  checks.push({ name: 'production configuration: ' + cfgS.summary, ok: true });
  var gfOk = dp2GfKey_('AZN.L') === 'LON:AZN' && dp2GfKey_('1120.SR') === 'TADAWUL:1120' && dp2GfKey_('DDI.US') === 'DDI' && dp2YahooKey_('DDI.US') === 'DDI' && dp2YahooKey_('AZN.L') === 'AZN.L';
  checks.push({ name: 'symbol keys (GOOGLEFINANCE / chart API)', ok: gfOk });
  var probe = null;
  var probeSym = 'SPY';
  for (var li = 0; li < led.lots.length; li++) { if (/\.US$/i.test(led.lots[li].symbol)) { probeSym = led.lots[li].symbol; break; } }
  try { var pb = dp2LoadBars_([probeSym]); var pk = Object.keys(pb)[0]; probe = pb[pk]; checks.push({ name: 'price history fetch ' + pk + ': ' + (probe ? probe.bars.length + ' bars via ' + (probe.source || 'none') + (probe.stale ? ' (STALE: ' + dp2Str_(probe.error).slice(0, 60) + ')' : '') : 'none'), ok: !!(probe && probe.bars.length > DP2X.ATR_N && !probe.stale) }); }
  catch (eP) { checks.push({ name: 'price history fetch: ' + String(eP && eP.message || eP).slice(0, 80), ok: false }); }
  var to = dp2ExitAlertTo_(); var quota = -1; try { quota = MailApp.getRemainingDailyQuota(); } catch (eQ) {}
  checks.push({ name: 'exit alerts ' + (dp2ExitAlertsEnabled_() ? 'ON' : 'OFF') + ' → ' + (to || '(no recipient)') + ' · mail quota ' + (quota >= 0 ? quota : 'n/a (grant the send-mail permission)'), ok: !!to && quota !== 0 });
  var ok = checks.every(function (c) { return c.ok; });
  var lines = checks.map(function (c) { return (c.ok ? 'PASS' : 'FAIL') + ' — ' + c.name; });
  var msg = 'Decision Pages v2 ' + DP2_VERSION + ' self-test: ' + (ok ? 'PASS' : 'FAIL') + '\n\n' + lines.join('\n');
  try { SpreadsheetApp.getUi().alert('Decision Pages v2', msg, SpreadsheetApp.getUi().ButtonSet.OK); } catch (e) { Logger.log(msg); }
  return { ok: ok, checks: checks };
}
