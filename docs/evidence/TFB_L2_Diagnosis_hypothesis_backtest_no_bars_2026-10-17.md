# TFB — L2 diagnosis: the hypothesis backtest has no bars (Hypothesis_Registry Universe Size 0)

Date: 2026-10-17 (Saturday) · Brief: Codex Brief 2026-10-17 v2 §2 item **L2** ("diagnosis first") · Evidence: `scripts/track_performance.py` @ `11ef134`, `core/providers/yahoo_chart_provider.py`, `.github/workflows/backtest_harness.yml`, and the **live job log of backtest_harness run 38161414138 (job 114534938849, 2026-10-11 17:51–17:52Z)** read through the GitHub Actions API.

## Symptom (as measured in the brief)
Hypothesis_Registry: 6 hypotheses, all `INSUFFICIENT_DATA`, 0 trigger events, Universe Size 0, History Days 0, Last Backtest 2026-10-11T17:52Z. The same picture was recorded on 2026-07-19 (v6.24.0 / v6.25.0 notes in the file).

## What the live log shows (run #21, 11 Oct)
| Observation | Meaning |
|---|---|
| The whole job ran 17:51:55 → 17:52:30 (35 s); the harness step itself 7 s | nothing deep was fetched |
| Every history call: `GET https://query1.finance.yahoo.com/v8/finance/chart/<SYM>?range=1mo&interval=1d` → 200, ~1.3 KB gzip | **range=1mo** on all 16 symbols: ~22 bars each |
| `[backtest] history fetched for 16/16 symbols` | the fetch did not fail; it was shallow |
| six lines `… INSUFFICIENT_DATA n=0 hit=0% [0-0] eff=+0bps t=+0.00` | every hypothesis saw zero usable symbols |
| `PUT …/Hypothesis_Registry!A6:X11` then `[backtest] ACCEPTED 0/6`, exit 0 | the zero rows were written and the run was **green** |

No EODHD call appears in the log (`EODHD_API_KEY` is set in the step; it was never consulted). No yfinance call appears either.

## Mechanism (code path, HEAD 11ef134)
1. `PerformanceTrackerApp.run_backtest_mode` → `_fetch_backtest_history(universe, days=730)` (`_BT_DEFAULT_DAYS`, L7713).
2. `_fetch_from(mod, sym)` (≈L10590) calls `fn(sym, days=days)`. `core.providers.yahoo_chart_provider.fetch_history(symbol, period="1mo", interval="1d")` (L3334) **has no `days` parameter** → `TypeError` → the fallback `fn(sym)` runs with the provider default **period="1mo"** (~22 bars). This is the `range=1mo` in the log.
3. The provider loop `break`s on the first non-empty list (≈L10615), so **EODHD is never consulted** even though it accepts `days` (L3643) and serves US at full depth.
4. The deep-history repair of v6.11.0 / v6.25.0 (`_yf_deep_history`, yfinance at `period=2y`) runs **only when `TFB_BACKTEST_KSA_YF` is on** (≈L10604); `backtest_harness.yml` never sets it. Kill-switch-without-mapping class, same as the P-189b/P-188 finding.
5. `run_backtest` (L8162): `len(bars) < min_window + h + 1` = 22 < 12 + 20 + 1 = **33** → every symbol `continue`s **before** `max_hist` is updated → `universe_size = 0`, `history_days = 0`, `n = 0` → `INSUFFICIENT_DATA`, notes `"0 trigger events (< min 30); no verdict"`. The run returns 0 and writes the registry — hence a fresh timestamp beside zeros.
6. `got == 0 → abort` (L10485) never fires, because `got` counts non-empty lists, not usable depth.

So: **the symbol list is fine (16/16), the provider import is fine, the token is fine; the window is 1 month because of a keyword mismatch, and the depth guard then empties the universe.** The failure has been silent since v6.10.1 routed Yahoo first (the v6.25.0 note of 2026-07-19 observed exactly "~22 bars … silently skipped" and fixed the yfinance suffix — behind the unmapped flag).

## Why the registry row cannot tell you this today
`history_days` is only updated for symbols that pass the length guard, so a 22-bar universe reads as **0**, indistinguishable from "no history at all". The Notes cell carries only the trigger-count text. Nothing is written to `_Run_Log`. The harness is green.

## Fix delivered (v6.46.0, same commit) — see the commit sheet
(1) `days → period` translation on the TypeError path (730 → "2y"), default on, kill-switch `TFB_BACKTEST_PERIOD_FROM_DAYS=0`; (2) Notes name the cause when Universe Size is 0; (3) a depth line in the job log, one `[BACKTEST-EMPTY]` WARNING row in `_Run_Log`, and exit 2 when every hypothesis has Universe 0.

## Not changed, noted
- `backtest_harness.yml` still runs on `ubuntu-latest` with `actions/checkout@v4` / `setup-python@v5` (Node 20 deprecation warnings in the log) — the pin that #762 applied to the other production workflows was not applied here.
- The brief's observation that the four trigger fields (`flow`, `candle_structure`, `trend_200`, `vol_adj_mom`) are not Signal_History columns is correct but orthogonal: the backtest reads **bars**, not Signal_History, and derives the `_PS_FIELDS` from them.
