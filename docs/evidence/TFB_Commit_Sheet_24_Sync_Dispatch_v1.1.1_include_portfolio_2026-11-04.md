# Commit Sheet - `apps_script/24_Sync_Dispatch.gs` v1.1.1 - 2026-11-04

**Defect (read-only evidence, 4 Nov):**
- The owner installed the dispatch triggers on 4 Nov 01:05 Riyadh. The first dispatched `daily_sync` (run 41766747380) started on the minute at 06:22, but it did not include My_Portfolio.
- `daily_sync.yml` (since 12 Oct) adds `MY_PORTFOLIO` to a full_sync only for a schedule event or for a `workflow_dispatch` with `include_portfolio=true`. The dispatcher sent only `run_mode`.
- With no My_Portfolio publication for the run, `26_Decision_Chain` never refreshed Portfolio_Decision for it. Portfolio_Decision B2 at 10:11 still reads `source readiness VERIFIED (run 41748516298)`, the 03:40 scheduled sync.
- The finalizer published `decision_failed` at 07:42. `_Status` `TFB Decision Feed` = `NOT_ACTIONABLE(final_publication) | run 41766747380`, superseding the 05:08 EXECUTABLE release of the 03:40 sync.
- `Release After Sync` then timed out on the missing receipt (`'pf': null`, `receipts_timeout`, 2622 s). No release followed for the Tadawul session.

**Change:**
- The daily_sync dispatch payload is now `{ref, inputs: {run_mode: 'full_sync', include_portfolio: 'true'}}`, the same page set as a scheduled run.
- Version 1.1.0 -> 1.1.1, with a dated WHY block.
- `tfbSyncDispatchSelfTest` checks the new field.
- Unchanged: the S-1 lanes, the guards, the probe, the log rows and the status line.
- Not gated. The dispatcher is opt-in (its triggers are installed by the owner), and its purpose is to run the scheduled sync on time; a dispatched run with a smaller page set strands the decision feed on every run.

**Also:** `apps_script/zz_Property_Tool` gains `tfbPropsExitSanityOn`. The Settings page cannot add a 51st Script Property, so `DP2_EXIT_SANITY=on` (27_Decision_Pages_v2 1.2.33) is set from the editor.

**Tests (node v22):**
- `tests/test_gas_sync_dispatch_p170.js`: SUMMARY 44/44 PASS, with a new payload check.
- `tests/test_gas_s1_dispatch_p170b.js`: SUMMARY 53/53 PASS. Its S9 asserts the scheduled page set.
- Golden negative: the v1.1.0 file fails T1, T2 (payload), S1, S2, S9 and S9-status (42/44 and 49/53).
- `pytest tests/test_release_decision_feed.py`: 43 passed, 10 skipped.

**Operator steps:**
1. Merge.
2. Paste the full `24_Sync_Dispatch.gs` v1.1.1 and Save. The installed triggers keep working; no re-install is needed.
3. Optional: run `tfbSyncDispatchSelfTest`.

Until then, the 06:15 dispatch keeps stranding the morning feed. To stop it, run `tfbRemoveSyncDispatchTriggers`.

**Rollback:** Project History -> 1.1.0, or `tfbRemoveSyncDispatchTriggers`.

**Known limit:** the finalizer's 15-minute receipt wait still races the chain's post-audit Top_10 render (the chain renders Top_10 only after `TFB Decision Feed` carries the run). Scheduled syncs therefore still end red at the finalizer and are released later by Release After Sync. That is brief 9 of `claude/TFB_Delegation_Briefs_Build_Requirements_2026-11-04.md`.

Not a licensed financial adviser; no broker order was created, modified or cancelled.
