# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.17.0 [SHADOW-RANK-LOG] - 2026-10-27

**Scope:** program register **A2, part 2** (project doc `TFB_Self_Correcting_Recommendation_Program_2026-10-27.md`). The cockpit records the backend's two orders of the INVEST pool - the live order (champion) and the pre-registered shadow order (challenger, opportunity_builder v1.28.0, PR #848) - plus the seats it renders, so the nightly scorecard can compare both orders on the same days. Script Property `DT10_SHADOW_RANK_LOG`, default off = v1.16.0 byte for byte.
**Files:** `apps_script/16_Decision_Top10.gs` (1.16.0 -> 1.17.0; 493,183 -> 505,334 chars; seven anchored hunks; zero removals), `tests/test_dt10_shadow_rank_log.js` (new, 71 checks), `tests/fixtures/rank_challenger_payload_v1280.json` (the REAL v1.28.0 shadow payload subset for the A2 harness pool), `tests/js/dt10_edits_v1170.json` + `tests/js/dt10_cash_utc_history.js` (v1.17.0 hunks reversible, v1.16.0 base SHA-256 pinned), version/census pins moved in eight harnesses, `.github/workflows/ci.yml` 1.1.41, this sheet.

## S1 - WHY
- The October review and the first nightly scorecard: the live order does not predict outcomes (engine ROI vs excess Spearman -0.38 October / -0.17 since 1 Sep; ROI-cap names 34-35 % hit vs 53-61 %).
- PR #848 (opportunity_builder v1.28.0, `TFB_OPP_RANK_MODE=shadow`) publishes a pre-registered challenger order in `meta.rank_challenger`. Nothing in the workbook recorded it; `_Selection_Log` only holds the seats, and only in the live order. Without a daily record of both orders the promotion rule (program doc section 4) cannot be evaluated.

## S2 - WHAT (only when `DT10_SHADOW_RANK_LOG=on`)
- `dt10ShadowLogMode_()` - `on` only for that exact word (trimmed, any case); junk, blank, absent or a service fault -> off.
- `dt10ShadowLogRows_(payload, stamp, day, cohort)` (PURE) - from `payload.meta.rank_challenger` and `payload.selected`: CHAMPION rows (live order, top 10, Rank = live rank), CHALLENGER rows (shadow order, top 10, Rank = shadow rank), BOARD rows (the rendered seats: board rank, seat class from `_stab_status`, live rank when the name is in the published lists, shadow rank from the seat, else from its INVEST audit row, else blank - e.g. a grace hold outside today's pool). 24 columns: `Logged At, Day, Cohort, Order, Rank, Symbol, Market, Sector, Seat, Live Rank, Shadow Rank, Engine ROI %, Rel, Conf, Momentum, Strength, Strength Ref, Cap, Floor OK, Mode, Rule, Builder, Pool, Overlap Top5`. Signature = UTC day + both symbol orders + board rank:symbol. No block (backend at legacy), an error block or empty lists -> no rows, with the reason. No ticket, price or amount is ever logged.
- `dt10AppendShadowRankLog_(ss, payload, cohort)` - off: returns `''` after one property read (no sheet call, no write). On: the first event of a UTC day, then only when an order changes (state `DT10_SHADOW_LOG_STATE_V1`), appended as one rectangle to the hidden tab `_Shadow_Rank_Log` (created on first use: bold header, frozen row, hidden; a narrower existing tab gains columns). Returns `ShadowLog: +N` / `unchanged` / `none (<reason>)` / `ERROR ...`; never throws.
- Wiring (`dt10RefreshDecisionTop10Run_`, the `refreshDecisionTop10` body): one call after the exit log with the cohort of the final source readiness; the token is appended to the final status line only when non-empty. `dt10SelfTest` gains `shadow rank log (DT10_SHADOW_RANK_LOG): <mode>` and `shadow rank log core: ok`.
- Section 8 kept: nothing is ranked, scored or gated in GAS; both orders are the backend's. `_Selection_Log` is untouched (schema and cadence).

## S3 - Gate
| Script Property | Value | Effect |
|---|---|---|
| `DT10_SHADOW_RANK_LOG` | absent / anything but `on` (DEFAULT) | v1.16.0 renders byte for byte (one extra property read, nothing written) |
| | `on` | `_Shadow_Rank_Log` events whenever the backend publishes `meta.rank_challenger` (i.e. after `TFB_OPP_RANK_MODE=shadow` on Render) |

## S4 - Tests
- `node tests/test_dt10_shadow_rank_log.js` -> **ALL PASS (71 checks)**: S1 mode reader; S2 rows core on the REAL builder payload fixture (6 + 6 + 6 rows, header width, champion/challenger orders and ranks, A.SR row cell by cell, below-floor and no-momentum cells, board seat class / live / shadow rank incl. the audit-row fallback and a grace seat, signature, no ticket/price/amount); S3 nothing to log; S4 kill identity (absent / blank / off / observe: `''`, zero sheet calls, exactly one read, zero writes); S5 orchestrator on (tab created hidden+frozen+bold, 17 rows at rows 2-18, state saved, same-day unchanged with zero calls, changed order appends, new UTC day appends, legacy payload writes nothing, narrow tab widened, unreadable state healed); S6 never throws; S7 the embedded self-test lines; S8 wiring, version lockstep, ES5/ASCII lint, inventory; S9 golden negative - the v1.16.0 base rebuilt by reversing the seven hunks has SHA-256 `e1c43659...` (= the live paste), lacks every added name, census 214 -> 217 functions, 112 -> 116 vars, zero removals.
- Every native harness in the CI lane passes on the delivered file (`decision_board_funding` 64, `decision_source_readiness` 73, `apps_script_syntax` 5 sources, `portfolio_reconciliation_inputs` 20, `dt10_calendar_completeness`, `test_dt10_confirm_basis` 8/8 x3, `test_dt10_pending_grace` 8/8 x3, `test_dt10_funding_evidence` 103 (+ F11 golden negative on the rebuilt v1.15.0 base: 104), `test_pd_evidence_publish` 59, `decision_input_truth` 20 + 3 dual-tree skips (self-test now requires `shadow rank log core: ok`), `dt10_feed_cohort_binding` 21/21, `gas_ledger_fx_failed_state` 11/11, `dt10_bulk_format` 9/9, `decision_chain` 28/28). Python tests that read the GAS file: 170 passed, 10 skipped.
- Delivered SHA-256 `eba86a647f05b7a96d9caad9db121b69fe552536bf29674ef1e99b7fe654f4af`.

## S5 - Rollback
Delete `DT10_SHADOW_RANK_LOG` (no re-paste), or re-paste v1.16.0 (`e1c43659...`). The `_Shadow_Rank_Log` tab can stay (nothing reads it except the scorecard) or be deleted by hand.

## S6 - Known limits
- The log records the research/single-pass payload the cockpit renders; the allocate replay never ranks (v1.28.0), so the BOARD shadow rank is the research rank.
- One event per UTC day plus changes: intraday re-orders are logged, so the scorecard must pick one event per day (first of the day is the pre-registered choice; A2 part 3).
- `_Shadow_Rank_Log` and `_Board_Scorecard` are not yet in the DQ audit's tab registry (`scripts/tfb_workbook_dq_audit.py` KNOWN_TABS) - hygiene item.

## S7 - Operator steps
1. Merge (after #848, or independently - with the backend at legacy the log writes nothing).
2. Paste `apps_script/16_Decision_Top10.gs` into the bound project (replace the whole file), save, reload, verify SHA-256 `eba86a64...` and run `dt10SelfTest` (expect `[DT10 v1.17.0] self-test`, `shadow rank log core: ok`, zero FAIL).
3. When Render runs `TFB_OPP_RANK_MODE=shadow` (#848 merged): set Script Property `DT10_SHADOW_RANK_LOG=on`.
4. Verify on the next board render: Top_10_Investments B2 ends `| ShadowLog: +N`; the hidden `_Shadow_Rank_Log` tab holds CHAMPION / CHALLENGER / BOARD rows for the day.

Not a licensed financial adviser; the owner decides and executes every trade.
