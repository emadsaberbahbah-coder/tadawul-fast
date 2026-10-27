# Commit Sheet - `apps_script/16_Decision_Top10.gs` v1.17.0 [SHADOW-RANK-LOG] - 2026-10-27

**Scope:** program register **A2, part 2** (project doc `TFB_Self_Correcting_Recommendation_Program_2026-10-27.md`). The cockpit records the backend's two orders of the INVEST pool - the live order (champion) and the pre-registered shadow order (challenger, opportunity_builder v1.28.0, PR #848) - plus the seats it renders, so the nightly scorecard can compare both orders on the same days. Script Property `DT10_SHADOW_RANK_LOG`, default off = v1.16.0 byte for byte.
**Files:** `apps_script/16_Decision_Top10.gs` (1.16.0 -> 1.17.0; 493,183 -> 507,569 chars; seven anchored hunks; zero removals), `tests/test_dt10_shadow_rank_log.js` (new, 85 checks), `tests/fixtures/rank_challenger_payload_v1280.json` (the REAL v1.28.0 shadow payload subset for the A2 harness pool), `tests/js/dt10_edits_v1170.json` + `tests/js/dt10_cash_utc_history.js` (v1.17.0 hunks reversible, v1.16.0 base SHA-256 pinned), version/census pins moved in eight harnesses, `.github/workflows/ci.yml` 1.1.41, this sheet.

## S1 - WHY
- The October review and the first nightly scorecard: the live order does not predict outcomes (engine ROI vs excess Spearman -0.38 October / -0.17 since 1 Sep; ROI-cap names 34-35 % hit vs 53-61 %).
- PR #848 (opportunity_builder v1.28.0, `TFB_OPP_RANK_MODE=shadow`) publishes a pre-registered challenger order in `meta.rank_challenger`. Nothing in the workbook recorded it; `_Selection_Log` only holds the seats, and only in the live order. Without a daily record of both orders the promotion rule (program doc section 4) cannot be evaluated.

## S2 - WHAT (only when `DT10_SHADOW_RANK_LOG` is `on` or `all`)
- `dt10ShadowLogMode_()` - `on` or `all` only for those exact words (trimmed, any case); junk, blank, absent or a service fault -> off.
- `dt10ShadowLogRows_(payload, stamp, day, cohort)` (PURE) - from `payload.meta.rank_challenger` and `payload.selected`: CHAMPION rows (live order, top 10, Rank = live rank), CHALLENGER rows (shadow order, top 10, Rank = shadow rank), BOARD rows (the rendered seats: board rank, seat class from `_stab_status`, live rank when the name is in the published lists, shadow rank from the seat, else from its INVEST audit row, else blank - e.g. a grace hold outside today's pool). 24 columns: `Logged At, Day, Cohort, Order, Rank, Symbol, Market, Sector, Seat, Live Rank, Shadow Rank, Engine ROI %, Rel, Conf, Momentum, Strength, Strength Ref, Cap, Floor OK, Mode, Rule, Builder, Pool, Overlap Top5`. Rule and Builder are written as `v1.0` / `v1.28.0` (text Sheets cannot turn into a number or a date). Signature = UTC day + both symbol orders + board rank:symbol. No block (backend at legacy), an error block or empty lists -> no rows, with the reason. No ticket, price or amount is ever logged.
- `dt10AppendShadowRankLog_(ss, payload, cohort)` - off: returns `''` after one property read (no sheet call, no write). `on`: the first event of each UTC day only (what the nightly scorecard reads, ~30 rows a day); `all`: also whenever an order changes (audit, ~180 rows a day). State `DT10_SHADOW_LOG_STATE_V1`. Appended as one rectangle to the hidden tab `_Shadow_Rank_Log` (created on first use: the two unused columns dropped, hidden, the view returned to Top_10_Investments; a narrower existing tab gains columns). Row 1 must be the header: written (bold, frozen) whenever row 1 is blank - new, emptied or cleared tab, or a first run cut short after the tab was created - and a tab whose row 1 carries another layout is never written (`ERROR header mismatch at column N`). Rows are added before a write that would pass the grid. Returns `ShadowLog: +N` / `logged today` / `unchanged` / `none (<reason>)` / `ERROR ...`; never throws.
- Wiring (`dt10RefreshDecisionTop10Run_`, the `refreshDecisionTop10` body): one call after the exit log with the cohort of the final source readiness; the token is appended to the final status line only when non-empty. `dt10SelfTest` gains `shadow rank log (DT10_SHADOW_RANK_LOG): <mode>` and `shadow rank log core: ok`.
- Section 8 kept: nothing is ranked, scored or gated in GAS; both orders are the backend's. `_Selection_Log` is untouched (schema and cadence).

## S3 - Gate
| Script Property | Value | Effect |
|---|---|---|
| `DT10_SHADOW_RANK_LOG` | absent / anything but `on` (DEFAULT) | v1.16.0 renders byte for byte (one extra property read, nothing written) |
| | `on` | the first `_Shadow_Rank_Log` event of each UTC day whenever the backend publishes `meta.rank_challenger` (i.e. after `TFB_OPP_RANK_MODE=shadow` on Render) |
| | `all` | as `on`, plus every intraday order change (audit trail; ~6x the rows) |

## S4 - Tests
- `node tests/test_dt10_shadow_rank_log.js` -> **ALL PASS (85 checks)** (71 before the review fixes): S1 mode reader incl. `all`; S2 rows core on the REAL builder payload fixture (6 + 6 + 6 rows, header width, champion/challenger orders and ranks, A.SR row cell by cell, below-floor and no-momentum cells, board seat class / live / shadow rank incl. the audit-row fallback and a grace seat, signature, no ticket/price/amount); S3 nothing to log; S4 kill identity (absent / blank / off / observe: `''`, zero sheet calls, exactly one read, zero writes); S5 orchestrator on / all (tab created hidden+frozen+bold with the unused columns dropped and the view returned to Top_10_Investments, 17 rows at rows 2-18, state saved; `on`: same day -> `logged today` with zero calls even when an order changed; `all`: a changed order appends, same orders -> `unchanged`; new UTC day appends; the header is written once; legacy payload writes nothing; narrow tab widened and its header completed; an EMPTY existing tab and a cleared header row get the header before the rows; a foreign layout is never written and names the first wrong column; rows added before a write past the grid; unreadable state healed); S6 never throws; S7 the embedded self-test lines; S8 wiring, version lockstep, ES5/ASCII lint, inventory; S9 golden negative - the v1.16.0 base rebuilt by reversing the seven hunks has SHA-256 `e1c43659...` (= the live paste), lacks every added name, census 214 -> 217 functions, 112 -> 116 vars, zero removals.
- Every native harness in the CI lane passes on the delivered file (`decision_board_funding` 64, `decision_source_readiness` 73, `apps_script_syntax` 5 sources, `portfolio_reconciliation_inputs` 20, `dt10_calendar_completeness`, `test_dt10_confirm_basis` 8/8 x3, `test_dt10_pending_grace` 8/8 x3, `test_dt10_funding_evidence` 103 (+ F11 golden negative on the rebuilt v1.15.0 base: 104), `test_pd_evidence_publish` 59, `decision_input_truth` 20 + 3 dual-tree skips (self-test now requires `shadow rank log core: ok`), `dt10_feed_cohort_binding` 21/21, `gas_ledger_fx_failed_state` 11/11, `dt10_bulk_format` 9/9, `decision_chain` 28/28). Python tests that read the GAS file: 170 passed, 10 skipped (re-run after the review fixes: same counts).
- Delivered SHA-256 `cefeedeb056f7388fb166e57c524d8675d38f3d6c1d40484d03ea0024bd2db35` (was `eba86a64...` before the review fixes).

## S5 - Rollback
Delete `DT10_SHADOW_RANK_LOG` (no re-paste), or re-paste v1.16.0 (`e1c43659...`). The `_Shadow_Rank_Log` tab can stay (nothing reads it except the scorecard) or be deleted by hand.

## S6 - Known limits
- The log records the research/single-pass payload the cockpit renders; the allocate replay never ranks (v1.28.0), so the BOARD shadow rank is the research rank.
- `on` logs one event per UTC day (the pre-registered "first event of the day" the scorecard reads, A2 part 3 / PR #851); `all` adds intraday re-orders for audit, and the scorecard still reads only the first event of each day.
- `_Shadow_Rank_Log` and `_Board_Scorecard` are not yet in the DQ audit's tab registry (`scripts/tfb_workbook_dq_audit.py` KNOWN_TABS) - hygiene item.

## S7 - Operator steps
1. Merge (after #848, or independently - with the backend at legacy the log writes nothing).
2. Paste `apps_script/16_Decision_Top10.gs` into the bound project (replace the whole file), save, reload, verify SHA-256 `cefeedeb...` and run `dt10SelfTest` (expect `[DT10 v1.17.0] self-test`, `shadow rank log core: ok`, zero FAIL).
3. When Render runs `TFB_OPP_RANK_MODE=shadow` (#848 merged): set Script Property `DT10_SHADOW_RANK_LOG=on`.
4. Verify on the next board render: Top_10_Investments B2 ends `| ShadowLog: +N`; the hidden `_Shadow_Rank_Log` tab holds CHAMPION / CHALLENGER / BOARD rows for the day.

## Review fixes (27 Oct, independent review before merge)
1. **Header on an existing empty tab (should-fix):** v1.17.0 wrote the header only when it created the tab. A first run cut short after `insertSheet` (e.g. "Service Spreadsheets timed out"), or a tab emptied by hand, then received rows from row 1 with no header; the scorer (`shadow_events`) needs `Logged At` in row 1 and would have read NO_DATA for the whole evaluation window - indistinguishable from "not armed". Now the header is written whenever row 1 is blank, and a foreign layout is never written.
2. **View (minor):** `insertSheet` activates the new tab; creation now returns the view to Top_10_Investments (as `dt10CandTabWrite_` does) and drops the two unused columns.
3. **Text-safe versions (minor):** `setValues` parses like typing, so Rule `1.0` became the number 1 (and `1.10` would read 1.1); Rule / Builder are now `v1.0` / `v1.28.0`. The scorer's columns were never affected.
4. **Growth (minor):** on-change logging appended ~180 rows a day (~1.7M cells a year at the workbook's 64 % cell use). `on` now logs the first event of each UTC day only (~30 rows a day) - exactly what the scorer reads; `all` keeps the audit trail for whoever wants it.
Also: rows are inserted before a write that would pass the grid (defensive; `_Selection_Log` relies on the same append pattern).

Not a licensed financial adviser; the owner decides and executes every trade.
