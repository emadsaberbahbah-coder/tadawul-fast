# Commit sheet - full audit of 25 Nov 2026: five fixes

Owner, Wed 25 Nov 2026 09:36 Riyadh: "make full review for the attached sheet ... GitHub, Render, BigQuery ... all
workflows ... go build as we planned plus your findings". Each fix below answers one finding of that audit. Every
change keeps a rollback switch; none changes a gate, ticket, sizing, order or broker call.

| # | File | Version | Finding | Fix | Rollback |
|---|---|---|---|---|---|
| 1 | `apps_script/27_Decision_Pages_v2.gs` | 1.4.6 -> 1.4.7 [DIV-LOG] | `_DP2_Exit_State` read SUPN.US "SELL NOW - stop 33.76 hit" on cached bars while Exit Plan, Today Action Plan and Final_Decisions read HOLD: a cached chart carries no dividend events, so the 8.13 USD special of 3 Nov reached no source | the exit engine also reads the ledger's `_Dividend_Log` (positive Amount/Share per symbol and ex-date), net of the tax row (1.4.5 rule); no double count with a chart event | Script Property `DP2_DIV_LOG=off` |
| 2 | `scripts/await_cockpit_receipts.py` | 1.3.0 -> 1.4.0 | sync 45539650359 ended red on `decision_failed`: its render handshake stopped after 3 Sheets read time-outs at 844 s (bound 30 min); the Top_10 receipt landed 10 minutes later | `--wait-only` waits through read faults until its bound (backstop 30 in a row); the release mode keeps 3 | repository variable `TFB_SYNC_AWAIT_FAULTS=3` |
| 3 | `scripts/finalize_decision_publication.py` | 1.1.3 -> 1.2.0 | a red verdict logged only "decision_failed" | prints every failing decision finding as `CODE@surface` (no workbook value) | revert |
| 4 | `scripts/sheets_backoff.py` (new 1.0.0) + `scripts/run_calendar_sync.py` 1.3.0 -> 1.3.1 | 5 hourly page runs red on Sheets HTTP 429 in 36 h; the workbook open was never retried | the shared `_open_book` (15 jobs) uses a client that waits out a 429 on every request (65 s + 0-20 s, 3 times); a 5xx is never resent | `TFB_SHEETS_BACKOFF=off` in a job's env |
| 5 | `scripts/repair_corporate_actions.py` | 1.1.1 -> 1.2.0 [WITNESS-BASIS] | S-1 criterion 5 "CA UNREPAIRED" on one held GRT-UN.TO row (split between target date and maturity stamp) | the row's own `[OUTCOME-WITNESS v1]` receipt (quote_session, price) fixes the exit basis | repository variable `TFB_CA_WITNESS_BASIS=off` |

## Tests

- `tests/test_dp2_v1470_div_log.js` (+ `.py`): 5/5 - reversal to the pinned v1.4.6 file (9 hunks), golden negative
  (gate off = v1.4.6), the SUPN shape through the REAL cache fallback of `dp2LoadBars_` (every provider fails, the cache
  answers: v1.4.6 blank currency and STOP_HIT; v1.4.7 USD, entry 29.89, no SELL NOW; gate off blank), parser rules,
  `dp2CachedCcy_`, 8 mutants.
  Every older `tests/test_dp2_*.js` harness passes through the extended history chain (`tests/js/dp2_history.js`).
- `tests/test_await_cockpit_receipts.py` 26/26 (read-fault case moved to 1.4.0; rollback case kept).
- `tests/test_final_finding_codes.py` 3/3; `tests/test_final_decision_publication.py`, `tests/test_release_decision_feed.py` unchanged and green.
- `tests/test_sheets_quota_client.py` 16/16; `tests/test_sheets_quota.py` unchanged.
- `tests/test_repair_corporate_actions_witness.py` 12/12; `tests/test_repair_corporate_actions_seed.py` (tag version read from the script).
- Manifest pins moved in `scripts/verify_deployment.py`; `tests/test_verify_manifest_pins.py` green.

## Owner steps after merge

1. Apps Script: paste `apps_script/27_Decision_Pages_v2.gs` 1.4.7 (leave `DP2_DIV_LOG` unset).
2. Nothing else: the Python and workflow changes run from main.

## Follow-up (Codex review of the first push)

The cache fallback of `dp2LoadBars_` returned `ccy: ''`, and `dp2ExitDivsFor_` refuses every dividend source on a
blank currency, so the dividend log would not have reached the SUPN lot in the very case it was built for; the first
harness supplied `ccy: 'USD'` by hand and missed it. `dp2CachedCcy_` (pure, 1.4.7, same `DP2_DIV_LOG` gate) now names
the currency the market fixes for a cached history: a US listing USD (as the Stooq path already states), a `.SR`
listing SAR, any other market blank (no dividend applied, the 1.4.6 rule). The harness now runs the real fallback.
The lean-lane import of `requests` in `tests/test_sheets_quota_client.py` moved behind `pytest.importorskip`.
