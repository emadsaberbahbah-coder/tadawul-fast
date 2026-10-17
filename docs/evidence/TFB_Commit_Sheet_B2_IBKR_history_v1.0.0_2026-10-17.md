# TFB Commit Sheet — B2: IBKR history plumbing (new `26_IBKR_History.gs` v1.0.0 + hook in `10_My_Portfolio.gs` v1.5.17)

**Date:** 2026-10-17 · **Plan item:** Decision Pages v2 Execution Plan §2 B2 / §5 (Codex) · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `45f51fe` (B3)

## 1. Deliverable

| File | Before | After | Gate |
|---|---|---|---|
| `apps_script/26_IBKR_History.gs` | (new) | **v1.0.0** · 570 lines · sha256 `d2848a0dbf34…` | none needed: no request, render, gate or property is touched; the module only appends to two hidden tabs |
| `apps_script/10_My_Portfolio.gs` | v1.5.16 `bb186c2a2b92…` | **v1.5.17** · 4,853 lines · sha256 `8d9234a9b259…` | the hook is `typeof`-guarded: without the module the watcher is byte-identical to v1.5.16 |

The pasted order in the IDE is free (either file first); the hook does nothing until both are present.

## 2. What it does

- **On each new capture** the watcher sees (`pdIbkrCaptureWatch`, after the "already processed" and future-clock checks and before the 10-minute decision-freshness return), `ibkrHistoryRecord_(cap.json, cap.id)` appends:
  - `_IBKR_NAV_History` (hidden): one row per capture **date** — Date, NAV, Base Ccy, Deposits/Withdrawals (Cash Report, base currency, same day), Daily Return `r = NAV1 / (NAV0 + flows) − 1` (deposits add to the base so a deposit is not a return), TWR (chain) `(1+TWR0)(1+r) − 1`, Capture, Recorded At, Source. Idempotent on the date.
  - `_IBKR_Trades` (hidden): one row per **fill** — Trade ID, Date, Symbol, Side, Qty, Price, Commission, Realized P&L, Currency, Order Type, Capture, Gain, Loss (the last two feed the bar chart). Idempotent on the trade id.
- **Capture sections** (plan C3 adds them to the Flex query): `net_asset_value|nav`, `cash_report|cash`, `trades`; both snake_case and Flex camelCase field names are accepted (`tradeID/tradeDate/buySell/tradePrice/ibCommission/fifoPnlRealized`, `depositWithdrawals`, `reportDate` in `yyyyMMdd`). A capture without the new sections records nothing and says so (`nav skipped: no NAV section, trades +0`).
- **Statistics** (`ihStats_`, pure, 90-day window): fills grouped into closed trades by symbol + sell date (PFLT's 8 partial fills = one trade); win = realized P&L > 0; stop-out = any fill with order type `STP*`/`TRAIL*`. Reported: n, wins/losses, win rate %, avg win, avg loss, payoff, expectancy, stop-outs and share, net.
- **Charts for the A3 renderer:** `ibkrHistoryTwrChart_(sheet, row, col)` (line, `#2a78d6`, 2 px, TWR % by date) and `ibkrHistoryTradeChart_(sheet, row, col)` (stacked column, Gain `#2a78d6` / Loss `#e34948` per fill). Idempotent per anchor (a prior chart of this module with the same title is replaced); fail-open.
- **Entry points:** `ibkrHistoryRecord_`, `ibkrHistoryStatus_` (one line, also printed by `pdSelfTest`), `ibkrHistorySelfTest_` (editor-run).

## 3. Validation against the plan's data (`IBKR_Closed_Trades_90d_2026-10-17.csv`)

The harness parses the CSV itself (not the module's embedded copy) and runs the real `ihStats_` with the window ending 2026-10-17:

| Metric | CSV TOTAL row | module |
|---|---|---|
| closed trades | 15 (3 W / 12 L) | 15 (3 W / 12 L) |
| win rate | 20 % | 20 % |
| avg win / avg loss | +278.6 / −137.1 | +278.61 / −137.08 |
| payoff | 2.03 | 2.03 |
| expectancy | −53.9 USD/trade | −53.94 |
| stop-outs | 6 | 6 (40 %) |
| net | −809.08 USD | −809.08 |

The 30-day window picks the four stop-outs after 17 Sep (AER, CARE, YUM, SBAC), as expected.

## 4. Protocol evidence

- **Anchored edits (10_My_Portfolio):** 4, each `count == 1`; added lines: 3 (hook) + 2 (selftest) + header. Functions added 0; removed 0.
- **New file:** 26_IBKR_History.gs, 32 functions, ES5 only (guard H1 scans every line).
- **Harness:** `node tests/js/ibkr_history_v100.test.js` 8/8 (CSV statistics, partial-fill grouping, NAV/TWR chain and idempotence, snake/camel extractors, sheet I/O on a stub book with hidden tabs and headers, both charts, hook placement, ES5); `pf_sparkline_col` 6/6, `pf_switch_advisory_col` 4/4, `pf_uncertified_context` 7/7 (pins → 1.5.17), `pf_ibkr_evidence` 14/14 golden intact, `apps_script_syntax` 5 compiled. `ci.yml` JS step += `ibkr_history_v100`.
- **No ENV, Script Property or cell changed by this commit.**

## 5. What is NOT here (by design)

- The charts are **not** placed on Portfolio_Decision by this commit: `pdRenderPayload_` belongs to the A3 renderer (plan owner Claude). A3 calls the two builders with its anchor row/col.
- The Flex query itself (plan C3 — Trades, Cash Report, Net Asset Value, Open Orders, Open Positions sections) is Emad's.
- A Flex **menu** item (A1 already has `menuIbkrEvidenceStatus_`); `ibkrHistoryStatus_()` is ready to be wired there if wanted.

## 6. Read-back after paste (Emad)

1. Paste both files; run `ibkrHistorySelfTest_` → `core: ok (CSV 2026-10-17 stats, chain, extract, dedupe)` and the live line `NAV rows 0 · fills 0` until the first capture with the new sections.
2. After the Flex query carries the five sections (C3) and the watcher sees a capture: `pdSelfTest` prints `IBKR history v1.0.0: NAV rows 1 (last yyyy-mm-dd, TWR 0%) · fills N · …`; after five captures the TWR series has five points and `_IBKR_Trades` matches the CSV for the 90-day window (plan acceptance).
3. Rollback: delete `26_IBKR_History.gs` from the project (the hook goes inert) or re-paste 10_My_Portfolio v1.5.16.
