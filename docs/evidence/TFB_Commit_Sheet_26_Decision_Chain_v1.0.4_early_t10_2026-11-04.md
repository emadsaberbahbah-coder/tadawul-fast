# Commit Sheet - `apps_script/26_Decision_Chain.gs` v1.0.4 [EARLY-T10] - 2026-11-04

**Scope:** Brief 9 of `claude/TFB_Delegation_Briefs_Build_Requirements_2026-11-04.md` (the sync-render race).
- **The defect:** both overnight syncs of 4 Nov ended red at `Verify final decision publication` with `blockers: decision_failed`:
  - 03:40 Riyadh, run 41748516298: `Await render (handshake): timeout after 846 s, 15 polls; receipts for run 41748516298: Portfolio_Decision at 1 s, Top_10_Investments none.`
  - 06:22 Riyadh, run 41766747380: the same timeout, with both receipts missing. That run also lacked My_Portfolio; daily_sync 1.0.10 fixes that part.
- **The cause:**
  - `audit_decision_surface_freshness.py` 1.2.3 (`T10_SOURCE_CONSUMPTION_UNPROVEN`) needs Top_10 to carry `source readiness VERIFIED (run N)` **before** the final audit. `daily_sync.yml` 1.0.9 waits up to 15 minutes for it.
  - This chain rendered Top_10 for run N only once `TFB Decision Feed` carried run N, which happens **after** that audit. The wait could only succeed by accident.
  - Each red finalizer published `NOT_ACTIONABLE(final_publication)` until a later release.
- **The change:** a new Script Property `TFB_DECISION_CHAIN_EARLY_T10`, **default OFF**.
  - Gate off: one property read per tick and nothing else. Plans, state JSON and `_Run_Log` rows are those of v1.0.3.
  - No gate, receipt, verdict, audit, release, ticket, order or broker call changes.

**Files:**
- `apps_script/26_Decision_Chain.gs` 1.0.3 -> 1.0.4 (680 -> 765 lines). Every added line is ASCII.
  - Added: 2 PURE functions (`tfbDcEarlyOn_`, `tfbDcUpstreamReadyRun_`) and 3 vars.
  - Changed: `tfbDcStateOf_`, `tfbDcReadInputs_`, `tfbDcPlanCore_`, `tfbDecisionChainTick`, `tfbDecisionChainStatus`.
  - Removed: 0.
- `apps_script/zz_Property_Tool`: adds `tfbPropsEarlyT10On`, because Project Settings cannot add a 51st property.
- `tests/js/decision_chain.test.js`: C37-C42 added; 36 -> 42 checks.
- `tests/test_decision_chain_js.py`: expects `RESULT 42/42 PASS`.

## S1 - WHAT (gate on only)
1. **Inputs** (`tfbDcReadInputs_`). The tick reads `TFB Upstream Feed Diagnostic`, `TFB Feed Commodities_FX` and `TFB Feed Mutual_Funds`; Market_Leaders and Global_Markets are already read. `tfbDcUpstreamReadyRun_` returns run N only when both hold:
   - the diagnostic reads `UPSTREAM_READY | run=N ...` with one `run=` token;
   - all four page feeds read `OK | ... | run=N` for that same N.

   The second rule matters because one finished sync job can write UPSTREAM_READY while another job of the same run is still writing its pages (`_uv_compose`: "Different run IDs are allowed").
2. **Plan** (`tfbDcPlanCore_`). Before `run N final audit not finished`, one step `t10e` renders Top_10 for run N while all of these hold:
   - the proved run equals the Global_Markets run, and `state.t10e` is not N yet;
   - the step stays under the 3-attempt budget (`t10e:N`);
   - it is not held (`waiting for _Status to change`);
   - no cockpit is running and it is not a quiet window.

   The claim, manual-priority and sync-in-flight skips come first, as before.
3. **Render and verify:** the step uses the same `refreshDecisionTop10`, the same receipt check and the same hold. Success records `state.t10e = N`. The `_Run_Log` row reads `[DECISION-CHAIN v1.0.4] t10e run N -> rendered (run N upstream ready)`.
4. **Unchanged:**
   - the post-audit render (`state.t10`) and the release re-render (`t10rel`), so the board still shows the final verdict after the audit;
   - Portfolio_Decision first.
5. **Status:** `tfbDecisionChainStatus` adds `early_t10=on|off last_t10_early=<run>`.

## S2 - Tests (node v22)
- **`node tests/js/decision_chain.test.js`:** `RESULT 42/42 PASS`, run twice, stdout SHA-256 prefix `56b09d49d7a7bbbc` both times.
  - **C37 golden negative:** the gate absent, blank, `off`, `0`, `enable`, `no`, or a property fault:
    - same plan, no render, no row, state untouched, the same `_Status` read count;
    - a pd render writes exactly the v1.0.3 state JSON (no `t10e` key).
  - **C38 the 4 Nov shape:** pd, then `t10e` with a VERIFIED receipt for run N, then `final audit not finished`. Once the finalizer carries run N: one post-audit `t10`, then `already refreshed`. The next run renders early again.
  - **C39 no early render** when:
    - the diagnostic belongs to another run, is not ready, has two `run=` tokens or is missing;
    - a page feed is not OK, belongs to another run or is missing;
    - a sync is in flight, it is a quiet window, a cockpit is running, or a claim is held.
  - **C40 budget and hold:** a wrong-run receipt is retried 3 times, then gives up. A source WITHHELD waits for `_Status` to change.
  - **C41 observe:** one OBSERVE row, `would refresh t10e for run N (run N upstream ready)`, never refreshes.
  - **C42 pure helpers and the status line.**
- **Golden negative file:** v1.0.3 run as the delivered file fails C37 (`the gate on reads the diagnostic and the two other page feeds`).
- **Mutants:** 18/18 killed:
  - the gate always or never on;
  - feeds not checked, feed run not compared, feed state not checked, diagnostic head not checked, no feeds required;
  - the early step ignoring the quiet window, a running cockpit, the budget or the hold;
  - early recorded as `t10`, early every tick;
  - `t10e` not kept, or always written;
  - the observe view without `t10e`, the status line without `early_t10`, extra reads while off.
- **`pytest tests/test_decision_chain_js.py tests/test_shadow_board_t10_settle.py`:** 8 passed.
- **Delivered file:** SHA-256 `900c4a2c0cc1f11f7c091844eca9fa6ff3f7de206d0e3384f4d16fdaeccf5f09` (765 lines, 38,781 bytes).

## S3 - Operator steps (the repo file is a mirror; nothing changes until the paste)
1. Merge.
2. Paste the full `apps_script/26_Decision_Chain.gs` v1.0.4 into Apps Script and Save.
3. In `zz_Property_Tool`, add `tfbPropsEarlyT10On` (bottom of the repo file) and Run it. Expect `TFB_DECISION_CHAIN_EARLY_T10=on`.
4. Run `tfbDecisionChainStatus`. Expect `[DECISION-CHAIN v1.0.4] mode=on ... early_t10=on`.
5. **Watch the next sync:**
   - `_Run_Log` shows `t10e run N -> rendered (run N upstream ready)` before the finalizer;
   - the finalizer log reads `receipts for run N: Portfolio_Decision ..., Top_10_Investments ...` and passes;
   - Release After Sync releases.
6. **Rollback:** delete `TFB_DECISION_CHAIN_EARLY_T10` (or set it `off`), or Project History -> 1.0.3.

## S4 - Known limits
- The early render needs all four page feeds of the run. A run with a key subset falls back to the v1.0.3 post-audit render.
- One extra Top_10 render per run (early + post-audit + release), about 1-2 min of Apps Script time.
- The early board is rendered under the `TFB Decision Feed` that `daily_sync.yml` (1.0.3, P0-1) superseded when the sync's writer started. It shows no executable ticket until the release re-render. Any render between two runs (for example the 4-hourly wrapper) shows the same state today.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
