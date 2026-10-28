# Commit Sheet - `apps_script/26_Decision_Chain.gs` v1.0.3 (wait for `_Status` before re-rendering an unverified board) - 2026-10-28

**Scope:** weekly-review defect D5 (28 Oct): the chain re-rendered the same WITHHELD Top-10 three times within ten minutes. Repo file; the live Apps Script project takes the paste. Default off and observe unchanged. No gate, receipt, verdict or release path touched; `refreshPortfolioDecision` / `refreshDecisionTop10` untouched.

**Files:**
- `apps_script/26_Decision_Chain.gs`: 1.0.2 -> 1.0.3. WHY block prepended; earlier text kept verbatim. ES5, ASCII additions, anchored edits.
- `tests/js/decision_chain.test.js`: C18 updated (a repaired source is a changed `_Status`), C29-C33 added; 33 cases.
- `tests/test_decision_chain_js.py`: expects `RESULT 33/33 PASS`.
- This sheet.

## S1 - Evidence
`_Run_Log` since 27 Oct: 203 INFO / 64 WARNING / **10 ERROR**, every ERROR row `tfbDecisionChainTick` with `native source receipt does not verify the planned run`. On 28 Oct the 04:00, 04:05 and 04:09 ticks each rendered the same Top-10 (`source readiness WITHHELD: Commodities_FX acquired 430/453 below 95%`), about 2 min of Apps Script time each, until `TFB_DC_MAX_ATTEMPTS` (3) gave up on the cohort. Nothing changed between the three renders: the receipt is derived from `_Status` (page rows A:J, keyed values L:M), and only the sync writes `_Status` (no Apps Script producer does; `16_Decision_Top10` and `10_My_Portfolio` only read it).

## S2 - WHAT
- `tfbDcReceiptCore_` (PURE) marks that one failure reason with `wait: true`; the four other reasons (status unavailable, board not updated, run invalid, timestamp outside the attempt) are unchanged and keep the v1.0.2 next-tick retry.
- `tfbDcFingerprintCore_(rowsAJ, rowsLM)` (PURE): djb2 over the non-empty cells of `_Status` A:J and L:M, base 36 plus the text length; `''` when nothing is readable, and `''` never holds anything.
- `tfbDcStatusFingerprint_(ss)`: one bounded read per tick (`A1:J<lastRow>` up to 400 rows, `L1:M60`); `''` on any failure.
- `tfbDcReadInputs_` adds `statusFp`; `tfbDcStateOf_` parses `hold` (`{attemptKey: fingerprint}`).
- `tfbDcPlanCore_` (PURE): a candidate whose attempt key is held under the current fingerprint is skipped with the reason `... did not verify; waiting for _Status to change` (Top-10 cohort, release re-render) or falls through to the Top-10 step (a held Portfolio_Decision no longer blocks it).
- `tfbDecisionChainTick`: after an attempt whose receipt returned `wait`, the fingerprint read at plan time is stored under the attempt key; holds are pruned exactly like attempts (same cohort or release stamp) and cleared with a verified render. The ERROR row says `| waiting for _Status to change`; the details JSON carries `wait`.
- Budget: `TFB_DC_MAX_ATTEMPTS` still bounds the cohort; a hold only decides *when* the next attempt may run (after the sync or its recovery rewrote `_Status`), never adds attempts.
- Limit: a WITHHELD reason that clears without a `_Status` write (a scope-policy change) waits for the next sync stamp (at most ~8 h); the 4-hourly wrapper (`13_AutoRefresh`) still renders independently.

## S3 - Tests (`node tests/js/decision_chain.test.js`, 33/33; `tests/test_decision_chain_js.py` wraps it)
| Case | Proves |
|---|---|
| C18 (updated) | A WITHHELD Portfolio_Decision is not re-rendered while `_Status` is unchanged; the held step no longer blocks Top-10; a re-published My_Portfolio row (changed `_Status`) lets the same cohort complete. |
| C29 | A WITHHELD Top-10 render is held on the 5-, 9-, 30- and 120-minute ticks (one render, one ERROR row); a rewritten Commodities_FX row retries once; a verifying render after the next change completes the cohort and clears attempts and hold. |
| C30 | Three WITHHELD renders across three `_Status` changes still give up after `TFB_DC_MAX_ATTEMPTS`. |
| C31 | A thrown refresh and an unchanged board (early return) keep the next-tick retry and record no hold. |
| C32 | Fingerprint core: `''` on empty, deterministic, changes on any cell or Date; an unreadable fingerprint never holds; observe mode unchanged. |
| C33 | A hold is pruned with its cohort when the next run arrives. |

Golden negative on `18ab6e38` (v1.0.2): the harness fails at C18 (no `waiting for _Status to change`, and the second tick re-renders).

### Review fix (Codex, PR #866, head `dc97332f`)
| Finding | Fix | Golden negative |
|---|---|---|
| P2: the fingerprint included renderer-owned rows. Every WITHHELD render upserts the cockpit's own `Top_10_Investments` row in `_Status` (`dt10WritePageStatus_` -> `writePageStatus_`, endpoint `/sheet-rows/opportunity-candidates`), so the render itself moved the plan-time fingerprint and the next tick retried until the budget was spent - the loop this change exists to stop. The harness spies did not write `_Status`. | `tfbDcFingerprintCore_` keeps only the source-owned part: A:J rows whose Endpoint starts `backend:` (the sync's documented author stamp, `run_dashboard_sync` `_STATUS_STAMP_ENDPOINT`) and the L:M keys `TFB Decision Feed` and `TFB Feed *` (the sync's and the release's keys). The `_Status` header row, the cockpit's row, GAS rows and GAS keys (`Last Global Update`, `Token Loaded`, `TFB Sync Hold`) never move it. The harness Top-10 spy now upserts the cockpit's own row and a GAS key on every render; new C34 proves the hold survives four ticks and is released by the next `backend:` stamp. | The new harness fails on `dc97332f` at C32 (a renderer-owned row moves the fingerprint). |

Harness: 34/34; `tests/test_decision_chain_js.py` expects 34/34.

## S4 - Owner steps (after merge)
1. Apps Script editor -> `26_Decision_Chain.gs` -> replace the whole file with the merged repo copy -> Ctrl+S. No trigger change: the 5-minute trigger and `TFB_DECISION_CHAIN` stay as they are.
2. Read-back next morning: `_Run_Log` shows at most one `tfbDecisionChainTick` ERROR row per cohort while the board is WITHHELD, with `waiting for _Status to change`; `tfbDecisionChainStatus()` reads `next=none (Top_10 run N did not verify; waiting for _Status to change)` between syncs.

## S5 - Rollback
Paste v1.0.2 back (Project History), or `TFB_DECISION_CHAIN=off`. The `hold` key in `TFB_DECISION_CHAIN_STATE` is ignored by v1.0.2.

Not a licensed financial adviser; the owner decides and executes every trade.
