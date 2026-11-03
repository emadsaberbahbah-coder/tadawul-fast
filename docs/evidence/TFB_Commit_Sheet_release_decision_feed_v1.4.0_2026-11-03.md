# Commit sheet — `scripts/release_decision_feed.py` 1.4.0 + `release_decision_feed.yml` 1.4.0: a live release is held, not withdrawn, while a cockpit is still mid-render (3 Nov 2026)

**Gate:** `vars.TFB_RELEASE_RENDER_HOLD` (`1` / `true` / `yes` / `on`).
- Unset (default) or any other word: 1.3.0 behaviour, byte for byte apart from `script_version`.
- Armed: a re-verification that fails only because a cockpit is mid-render is deferred with zero writes.

**Owner request (3 Nov 10:43 Riyadh):** "make the required update". The 09:55 release of run 41566014627 had been withdrawn at 10:08:41.

## 1. Evidence
Sources, all read-only: `_Status` L:M, the release run summary, the Apps Script execution log and the cockpit B2 lines. Times are Riyadh.

| Time | Event |
|---|---|
| 09:55 | Run 41566014627 released; expires 13:26:59 |
| 10:02:15–10:07:35 | `tfbDecisionChainTick` renders Top_10. DT10 v1.20.0 logs "done status=ok in 273.5s"; the board is QUALIFIED_UNFUNDED ("funding evidence not applied: capture too old (1h22m)") |
| by about 10:06 | A second release run re-verifies the live release. Its first strict audit reads Top_10 mid-render: `render_settle` attempt 1, cockpits `['Top_10_Investments']`, then a 150 s wait |
| 10:08:05–10:08:30 | `pdIbkrCaptureWatch` renders Portfolio_Decision: POST at 10:08:18, "done status=ok in 15s" at 10:08:29, then "capture watcher refreshed (enforce, status ok, follow-up armed)" |
| 10:08:41 | The re-audit fails (audits `[0,2]`) and step 7 withdraws the release: `NOT_ACTIONABLE(final_publication) \| run=41566014627 \| 2026-11-03 10:08:41+03:00 \| decision_failed \| release=41593890530` |
| 10:10:20–10:13:16 | `pdIbkrEvidenceFollowUp` renders Top_10. The board finishes at 10:12:58 as WITHHELD because the feed was withdrawn |
| 11:08:41 | A later watcher render of Portfolio_Decision still carries "source readiness VERIFIED (run 41566014627)", so the finished board passes the consumption check |
| 11:13:54 | Top_10 still shows WITHHELD; `classify_prior` returns `release_window_consumed` for the rest of the day |

**Evidence limit:** the release artifact holds the re-audit's findings, and this session cannot download it (blob 403). The timeline puts the re-audit inside the Portfolio_Decision render.

**The defect:**
- One 150 s settle cannot outlast back-to-back renders.
- The audit read a transient state that the cockpit declares on its own B2 line, and the run treated it as a verdict.
- The withdrawal then consumed the run's release window until the next sync.
- The 21 Oct withdrawal (16_Decision_Top10.gs v1.13.11 WHY) had the same shape.

## 2. What changed
**`scripts/release_decision_feed.py` 1.3.0 → 1.4.0**
- New `render_hold_enabled()`.
- New PURE `render_hold(result, source, prior, now)`.
- New step 7a. When a re-verification of the LIVE release of SOURCE is not accepted, the run holds only if all of these are true:
  - The result has exactly the shape a render produces: exit 2, audits `[0, 2]`, blockers `['decision_failed']`.
  - Every `accepted()` reason is one a render explains. It is never `run_id_not_source`, `not_strict`, `coverage_summary_not_zero` or `cockpit_run_stamps_missing`.
  - `settle_candidates(result)` is not empty. This is the v1.3.0 rule, unchanged: the coverage audit passed and every decision finding is explained by a cockpit whose B2 says `status: running…`.
  - Every running cockpit's own render start is at most `RENDER_HOLD_MAX_AGE_MIN` = 10 min old and at most 15 min in the future. The 10 minutes are Apps Script's 6-minute hard kill (which leaves B2 at `running…`) plus 4 minutes for the strict audits' own read time.
  - The live value is still well-formed and unexpired.
- When the run holds:
  - It exits 1 with outcome `deferred` and reason `render_still_running`.
  - It writes nothing: the value, its expiry and its `release=` token are untouched.
  - The output carries `render_hold` = cockpits, render_started, age_min, max_age_min and the audit's reasons.
- What the hold never does:
  - It never promotes and never extends an expiry.
  - It never applies to a first release, a kill or a sweep, or to a failure a render does not explain.
- The dry run reports the same hold instead of `would_downgrade`.

**`.github/workflows/release_decision_feed.yml` 1.3.0 → 1.4.0**
- One release STEP env line: `TFB_RELEASE_RENDER_HOLD: ${{ vars.TFB_RELEASE_RENDER_HOLD || '0' }}`.
- One summary row (`render_hold`), printed only when the key exists.
- The job env is untouched, so parity with the sync's final job holds.

**`scripts/verify_deployment.py`:** the `release_decision_feed` pin moves 1.3.0 → 1.4.0. This is a pin-only change.

**Tests**
- New `tests/test_release_render_hold.py`, H1–H13.
- In `tests/test_release_render_settle.py`, `arm()` now clears the new gate, so R1–R14 stay hermetic.

**Not touched:** the finalizer, the after-sync path, the Apps Script, the workbook, and every acceptance rule, gate, probe and compare-and-set.

## 3. Verification
- `pytest tests/test_release_render_hold.py` → 13 passed; the standalone runner prints PASS 13/13.
  - H13 replays the 3 Nov cascade: Top_10 mid-render at the first audit, Portfolio_Decision mid-render at the re-audit.
  - With the gate off it withdraws, as on 3 Nov. With the gate on it holds, and the next slot keeps the release.
- Mutation check: 11 mutants of the hold, all killed.
  - The gate always on, and always off.
  - Bound 11; bound exclusive.
  - No expiry guard, no shape guard, no reason guard, no future bound, no run guard.
  - The hold writes; the hold keeps the audit's reasons.
- Release, workflow and verifier suites: 494 passed, 10 skipped. The skips are base copies that are not supplied.
- `python scripts/audit_repository_workflows.py --root .` → errors 0, warnings 0.

## 4. Operator steps (owner)
1. Merge the PR.
2. Repository → Settings → Secrets and variables → Actions → Variables → **New repository variable**: `TFB_RELEASE_RENDER_HOLD` = `1`.
3. If not yet done (PR #901): `TFB_RELEASE_AFTER_SYNC_PRIOR` = `release`.
4. Expected: a slot that catches a render ends `deferred`, with reason `render_still_running` and a `render_hold` row in the summary, and the feed is unchanged.
   - On a schedule the slot is green with a warning.
   - On a manual dispatch it is red (exit 1), like every deferral.

**Rollback:** delete the variable.

## 5. Known limits
- A hold skips that slot's verdict. The next re-verification can be hours later, because GitHub delivers these crons 2–7 h late. A board that finished FAILED in between is withdrawn at that slot, not sooner.
- A render killed by the 6-minute limit within 10 minutes of its start can be held once; the next re-verification withdraws it.
- Run 41566014627 stays withdrawn today (`release_window_consumed`). The fix applies from the next sync's release.
- A first release still caught mid-render after the settle ends exit 2 with no write. This is unchanged.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
