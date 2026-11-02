# TFB Commit Sheet - .github/workflows/track_performance.yml v1.0.3 [owner opt-in scheduled maturity audit]

Date: 2026-10-20 (Tuesday, Riyadh). Base: `origin/main` aa1f440 (#802).
Delivered: `.github/workflows/track_performance.yml` 1.0.2 -> 1.0.3, `tests/test_tracker_record_only_workflows.py` (+13 cases), this sheet.
Not edited: `scripts/track_performance.py`, `daily_sync.yml` (its tracker step stays record-only), every gate, threshold, cron and concurrency group.

## Why (20 Oct workbook export)

Since 1.0.1 (#759, 12 Oct) scheduled clock runs record only; `--audit` needs a manual dispatch with `run_audit=true`.
Two consequences:

1. **Maturity evidence is lost, not delayed.** A record matures only on a quote from its exact target session.
   `_outcome_target_session` says "never use a later session", and a later quote leaves the record ACTIVE with
   `quote_outside_target_session`. So a target session the clock does not audit within about a day is never measured.
   In the export, the last maturity date is 2026-10-10, and 3,208 ACTIVE records are past their target date
   (1W 742, 2W 724, 1M 740, 3M 1,002).
2. **S-1 cannot pass.** The criterion-4 row (`_S1_Calibration` rows 1-2) is written only under `--audit`
   (`_publish_s1_calibration`). It reads `2026-10-11 23:00:18 PASS` (mean |err| 3.14 pp vs 10 pp band, n=6,647), and
   the scorer marks it PENDING once it is older than 48 h. The S1_Gate shows `calibration row stale (193h > 48h)`.

#759 kept schedules record-only because the matured-row guards (`TRACK_DEDUP_MATURED`, `TRACK_RISK_BLANK`) could not
run: their repository Variables were not mapped. #759 mapped them. They still default off. The export still holds
the same 27 duplicate MATURED keys.

## What

- Repository Variable **`TRACK_SCHEDULED_AUDIT`** (mapped as `${{ vars.TRACK_SCHEDULED_AUDIT || '0' }}`). Default `0`
  keeps the 1.0.2 behaviour.
- `1` makes a **scheduled** clock run pass `--audit`, and calibration publication is not overridden, exactly as a
  manual audited run. This applies only while the matured-duplicate guard measures or enforces
  (`TRACK_DEDUP_MATURED` = `observe` or `enforce`). Otherwise the run stays record-only and prints
  `::warning::TRACK_SCHEDULED_AUDIT=1 ignored ...`.
- Manual dispatch is unchanged (`run_audit=true` is still the only manual opt-in).
- Only this lane audits on schedule (14:47 and 18:47 Riyadh slots, private concurrency group and overlap guard).
  `daily_sync.yml`'s tracker step ignores the variable.

## Tests

| Battery | Result |
| --- | --- |
| `tests/test_tracker_record_only_workflows.py` (existing 30 + new: schedule x opt-in x guard, manual false wins, push never, warning only when ignored, daily lane unaffected) | 43 passed |
| `test_workflow_flag_mappings`, `test_workflow_activation_observe`, `test_track_flags_mapped`, `test_activation_rollout_evidence_workflows` | 241 passed (with the above) |
| `test_flag_registry`, `test_runner_pin_ubuntu2404`, `test_repository_workflow_audit`, `test_verify_optional_rollout_flags`, `test_workflow_audit_repairs`, `test_final_publication_workflow` | 50 passed, 3 skipped |
| `scripts/audit_repository_workflows.py --fail-on-warnings` | errors 0, the 2 pre-existing `daily_sync.yml` warnings |
| ruff E9/F63/F7/F82/F401/F841, YAML load | clean |

## Arming (owner, GitHub repository Variables, after merge)

1. `TRACK_DEDUP_MATURED=observe`. It is read by both tracker lanes; it measures and changes nothing.
2. `TRACK_SCHEDULED_AUDIT=1`. The next scheduled clock run audits; read `[DEDUP-MATURED]` in its job log and the
   `[PERF-VERDICT]` line in `_Run_Log`.
3. After that read-back, optionally `TRACK_DEDUP_MATURED=enforce` (the 27 duplicate keys).

Read-back: `_S1_Calibration` row 2 `As Of` is today, and the S1_Gate criterion 4 is no longer `stale`.
Performance_Log gains matured rows with today's maturity dates.

Rollback: unset `TRACK_SCHEDULED_AUDIT`.

## Known limits

- Target sessions between 12 and 20 Oct that were never audited stay unmeasured. This change stops the loss; it
  does not recover them.
- `TRACK_RISK_BLANK` stays the owner's separate decision. The parity rule requires it in both lanes at once.
