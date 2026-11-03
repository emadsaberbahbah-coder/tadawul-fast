# Commit sheet — `scripts/await_cockpit_receipts.py` 1.2.0 + `release_after_sync.yml` 1.1.0: the release's own prior rule for an older run's veto (3 Nov 2026)

**Gate:** `vars.TFB_RELEASE_AFTER_SYNC_PRIOR`.
- `strict` (default; unset or any other word) = 1.1.0 behaviour, byte for byte.
- `release` = weigh an older run's value with `release_decision_feed.classify_prior`.

**Owner request (3 Nov 01:08 Riyadh):** make the program stable, clear and consistent, with a clear way to invest.

## 1. Evidence (read-only: GitHub Actions, `_Status` L:M, run summaries)
| UTC | Event |
|---|---|
| 2 Nov 20:32 | Release re-verification withdraws run 41445971593: `NOT_ACTIONABLE(final_publication) \| run=41445971593 \| 2026-11-02 23:32:56+03:00 \| decision_failed \| release=41519935330` |
| 3 Nov 02:25 | Scheduled sync 41566014627 starts (the 20:17 UTC slot, delivered six hours late) |
| 3 Nov 04:29–04:35 | Its final job `Verify final decision publication` **passes**. With the sync-render handshake (`TFB_SYNC_AWAIT_RENDER`), the audit runs after the cockpits rendered, so `finalize_decision_publication.py` writes no veto: it writes only blockers |
| 3 Nov 04:35 | After-sync attempt 41578984787: both receipts verified (PF consumed 41566014627 at 05:27 Riyadh, T10 at 07:33). Outcome `stopped`, reason **`decision_feed_not_this_run`** (`decision_run` 41445971593) |
| 3 Nov 05:30 (08:30 Riyadh slot) | Not delivered by 06:10 UTC; GitHub ran the repository's crons 2–3 h late that morning |

**The defect:**
- Version 1.0.0 stopped on an older run's veto because "this run's final veto never landed". That held only while every final job failed before the cockpits rendered.
- With the handshake, a healthy final job writes nothing. So a clean sync is now the one case this path cannot release, while a failed sync still can.
- The release script itself accepts that prior value: `classify_prior(value, SOURCE)` returns
  - `('eligible', 'older_run_veto')` for an older run's veto;
  - `('eligible', 'superseded_older_run')` for its exact supersede grammar.

## 2. What changed
- **`scripts/await_cockpit_receipts.py` 1.1.0 → 1.2.0:**
  - `prior_rule()` reads `TFB_RELEASE_AFTER_SYNC_PRIOR`. Exactly `release` (trimmed, any case) selects the new rule; anything else is `strict`.
  - `evaluate(..., prior)`: with `release`, a decision feed naming a run older than SOURCE goes through `classify_prior`.
    - `eligible` → keep waiting for both receipts, exactly as for this run's own veto.
    - Anything else → stop `prior_<reason>`, for example `prior_executable_other_run` or `prior_superseded_by_run`.
  - Unchanged:
    - a newer run (`superseded_by_newer_cohort`);
    - an unreadable feed;
    - this run's own veto;
    - this run's `EXECUTABLE`;
    - provenance, receipts, the in-flight and release-run probes, the single dispatch, the deadline.
  - The evidence gains `prior_rule` and, per poll, `prior`.
- **`.github/workflows/release_after_sync.yml` 1.0.0 → 1.1.0:** one job env line, `TFB_RELEASE_AFTER_SYNC_PRIOR: ${{ vars.TFB_RELEASE_AFTER_SYNC_PRIOR || 'strict' }}`, plus header notes.
- **`scripts/verify_deployment.py`:** pin `await_cockpit_receipts` 1.1.0 → 1.2.0 (pin only).
- **Not touched:**
  - `release_decision_feed.py` / `.yml`: the release still re-runs every strict audit, gate, kill switch, probe and compare-and-set.
  - The finalizer, the sync, the Apps Script and the workbook.
  - This path still never writes the workbook and still decides only when to dispatch, never whether to release.

## 3. Verification
- `pytest tests/test_await_cockpit_receipts.py` → 26 passed, including new case `w26`:
  - `strict` words (unset, empty, `strict`, `releases`, `on`, `1`, `observe`) stop exactly as before on an older veto, a supersede, and the live 3 Nov shape.
  - `release` words (`release`, ` Release `, `RELEASE`) dispatch once for an older veto, the live 3 Nov value and the supersede (`prior` = `older_run_veto` / `superseded_older_run`), and stop for:
    - an older EXECUTABLE;
    - a supersede by a newer run;
    - a newer veto;
    - an owner hold;
    - a missing feed;
    - this run's EXECUTABLE.
  - Both receipts are still required: a cockpit on another run times out, and a newer cohort mid-wait still stops the attempt.
  - The workflow env contract (`w14`) carries the new line.
- `pytest tests/test_verify_manifest_pins.py` → 3 passed. `python scripts/audit_repository_workflows.py --root .` → errors 0, warnings 0.
- Workflow, release, verify, await and manifest subsets: 522 passed. The 15 collection errors are modules that need `fastapi`, which is not installed in this sandbox.

## 4. Operator step (owner)
1. Merge.
2. Repository → Settings → Secrets and variables → Actions → Variables → **New repository variable**: `TFB_RELEASE_AFTER_SYNC_PRIOR` = `release`.
3. Read the next after-sync attempt's summary. Expected: `prior` = `older_run_veto` (or `superseded_older_run`), then `dispatched`.

**Rollback:** delete the variable (`strict`).

## 5. Known limits
- The release's 480-minute window still runs from the sync's own stamp. A sync that starts at 05:25 Riyadh releases until about 13:25, before the 17:30 US open. The venue-aware release is a separate item.
- A cockpit that never renders the new run still ends the attempt in `receipts_timeout`. The scheduled slots then apply, as before.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
