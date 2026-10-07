# TFB Commit Sheet - Sync Outcome Audit: audit 'cancelled' sources (gated) - 2026-10-07

Files: `.github/workflows/sync_outcome_audit.yml`, `.github/workflows/ci.yml`,
`tests/test_sync_audit_cancelled_source.py` (new).
Gate: repository variable `TFB_SYNC_AUDIT_ACCEPT_CANCELLED` (default `0` = OFF).

## Why this item

Priority (b) in the daily brief: a red pipeline that hides whether the data
refreshed. Open PR #720 covers investment safety, refresh validity and outcome
evidence. It does not touch the Sync Outcome Audit's source gate. Open PR #721
covers unmerged documents and Trade Notes. Neither overlaps this change.

## Evidence

- Scheduled Daily Sync run 37612073997 (2026-10-07 11:08Z, sha 26fe496)
  concluded `cancelled`.
- Jobs: Pre-flight `success`; Data Sync core-pages `success` (11:16Z);
  mutual-funds `success` (11:38Z); global-markets `success`, including Track
  Performance (12:16Z); recover-missing-market-pages `success` (12:21:41Z).
- The only non-success job was `Run verdict` (job 112789049165). Its `if:`
  requires a failure or cancellation among its needs, and every need
  succeeded. It was marked `cancelled` with no runner at 12:21:46Z. In the
  same second, the pending `workflow_dispatch` run 37619602053 (created
  12:14:36Z) took the shared `tadawul-production-write-*` lease. Its
  Pre-flight job was created at 12:21:47Z.
- Sync Outcome Audit 37620447754 then failed in `Resolve source run` with
  `Source Daily Sync run concluded 'cancelled'.`. It never downloaded or read
  the uploaded page verdicts. The audit job only runs for `schedule` sources,
  so the follow-up dispatch run is not audited by this workflow either.
  Today's scheduled slot therefore has no page-refresh verdict at all.
- The step's own 2026-10-06 WHY block says `cancelled` "upload[s] no usable
  evidence". Run 37612073997 shows that is not always true.

## What changed

1. `sync_outcome_audit.yml`, step `Resolve source run`:
   - New step env `TFB_SYNC_AUDIT_ACCEPT_CANCELLED: ${{ vars.TFB_SYNC_AUDIT_ACCEPT_CANCELLED || '0' }}`.
   - New `cancelled)` arm in the conclusion `case`. With the variable at
     exactly `1`, it logs `concluded 'cancelled' - auditing uploaded evidence`
     and continues to the existing download and audit steps. Otherwise it
     prints the previous `::error::` line and exits 1.
   - A dated WHY block is prepended above the 2026-10-06 block. All prior
     WHY text is kept verbatim.
   - `skipped`, `timed_out` and any other conclusion are still refused,
     whatever the gate says.
   - The new harness is added to the `pull_request`/`push` path filters, to
     the py_compile step, and as a step in the regression job.
2. `ci.yml`: the harness is added to the blocking lean pytest list and to the
   header list.
3. The new harness `tests/test_sync_audit_cancelled_source.py` extracts the
   real step script from the workflow and runs it under bash. It runs as a
   script (`PASS 11/11`) and under pytest. It needs no network and no PyYAML.

No Python runtime code changed. No function or class was removed.
`scripts/audit_sync_outcome.py` is untouched (still 1.1.2).

## Why it cannot manufacture a pass

The gate only lets the audit look at the evidence. The verdict still comes
from `audit_sync_outcome.py`:

- No sync log at all exits 3 (`SYNC_ARTIFACT_READ_ERROR`).
- Any required page verdict that is missing, failed, or incomplete under the
  armed full-fetch gate (`TFB_AUDIT_REQUIRE_FULL_FETCH=1`, 95%) exits 2.

The harness covers both cases. A run cancelled part-way through, by a person
or by a starved runner, uploads fewer verdicts and stays red, now with the
specific missing pages named.

## Default OFF identity

With the variable unset or `0` (and for every non-`1` value tested: `""`,
`true`, `yes`, `on`, `01`, `" 1"`, `2`), every conclusion produces the same
exit code, stdout and `GITHUB_OUTPUT` as before. These are pinned as a frozen
contract in the harness and verified against the base copy.

One surface does change: GitHub echoes the step's script text and env block
at the top of the step log. The new env line and comment lines appear there.
Annotations, exit codes, artifacts and outputs are identical.

## Measured results (Python 3.11, this container, zero network)

- Harness on this branch: `PASS 11/11`. Under pytest together with
  `tests/test_sync_outcome_audit.py`: 45 passed, 2 subtests.
- Golden negative: `TFB_SOA_YAML=<git show HEAD:.github/workflows/sync_outcome_audit.yml>`
  gives `FAIL 9/11`. The two failing cases are exactly the gate-ON cases
  (`test_gate_on_audits_cancelled_run`, `test_gate_on_still_validates_run_id`).
  All 9 default-OFF contract cases pass on the base.
- ci.yml lean job reproduced (numpy + pytest only, 24 files):
  559 passed, 3 skipped, 13 subtests.
- `python -m compileall main.py core routes scripts tests`: OK.
  `git diff --check`: clean. Both workflow files parse as YAML.

## Known limits

- This makes the scheduled slot auditable. It does not stop GitHub from
  recording such runs as `cancelled`. Two other audits share that symptom:
  Full Refresh Coverage 37620447743 and Decision Surface Freshness
  37620447899. Both ran and failed on their own content checks (page floors
  and freshness). This change does not touch them, and their failures are
  not attributed to the cancellation.
- I have not proven the GitHub mechanism that cancelled the `if:`-false
  verdict job. The timing correlation with the lease hand-off is exact to the
  second. The workflow could be changed so the verdict job does not need to
  be scheduled at all, but that touches daily_sync.yml concurrency. Per the
  brief, it needs end-to-end reasoning first and is not done here.
- I measured one instance (2026-10-07). The Actions list API in this
  container returned stale ordering, so I did not measure how often it
  happens.

## Operator steps

1. Merge only after review. Nothing changes on merge, because the gate
   defaults to OFF.
2. To arm: Settings -> Secrets and variables -> Actions -> Variables ->
   New repository variable `TFB_SYNC_AUDIT_ACCEPT_CANCELLED` = `1`.
3. To re-audit today's slot after arming, dispatch Sync Outcome Audit with
   `run_id=37612073997`. Note that the manual path already bypasses the
   conclusion check today, so this works even before arming.
4. Rollback: delete the variable or set it to `0`.
