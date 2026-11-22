# Commit sheet: the three daily-red workflows (22 Nov 2026)

## What was red, and why

| Workflow | Red since | The one cause |
| --- | --- | --- |
| `provider_target_coverage.yml` (schedule) | 28 Oct | `CH_BASELINE_STALE`: the reference of 25 Oct aged past 72 h. Every page judged OK or STRUCTURAL_ZERO against it on 21 Nov (Market_Leaders 43.92% against 35.29%, Global_Markets 53.45% against 52.85%). |
| `workflow_repair_tests.yml` (verify-live, every push to main) | 28 Oct | The same audit, same stale reference, exit 2. |
| `name_label_renewal.yml` (daily check) | 19 Oct | Six bindings are held at every renewal (a ticker gone from the Nasdaq directory, an issuer page that changed, sources that refuse the request), so they stay expired and failed the check even on days the other 122 were fresh. On 20 Nov 21:32 UTC the 122 expired too: nobody dispatched the weekly renewal. |

## Why the reference deadlocked (audit_provider_target_coverage 1.2.2)

1. The verification clock moved only when every page was at or above its reference, and the reference ratchets up to each new high. Three ordinary days a fraction under a high aged it past 72 h.
2. A stale reference is a CONTROL failure, and a CONTROL failure can never refresh the reference. Once stale, it stayed stale until someone dispatched a bootstrap.

## What changed

**scripts/audit_provider_target_coverage.py 1.3.0**

- A healthy run (no finding; every page OK, STRUCTURAL_ZERO or BOOTSTRAP) refreshes the clock even when a page sits inside tolerance below its reference. The reference value is never lowered, so drift still accumulates against the last-good share (test: 50 -> 47 refreshes the clock, the reference stays 50, and 39 then fails against 50).
- When `CH_BASELINE_STALE` is the only CONTROL or FAIL finding and every page of the reference was measured today at or above its reference share, judged OK or STRUCTURAL_ZERO, with at least `min_rows` rows, the finding becomes the INFO note `BASELINE_STALE_REVERIFIED` and the run is judged on its coverage. The clock restarts on that run.
- Still red, still for an operator: a page below its reference, any other CONTROL or FAIL, a page of the reference that was not measured, a page under `min_rows`, bootstrap, acceptance.
- Rollback of the re-verification: `TFB_PTC_STALE_REVERIFY=0`. Under `TFB_PTC_FREEZE=1` (the verify-live job) a re-verified run passes and writes nothing.

**scripts/renew_verified_name_labels.py 1.2.0 and data/verified_name_labels_held.json**

- `--check` reads a reviewed list beside the registry. A due or expired binding on it, whose `review_by_utc` is ahead, is reported as `HELD_ACKNOWLEDGED` and no longer fails the check. Every other due binding fails it as before. An acknowledgement lapses on its own date. A malformed list is exit 2.
- The list names the six bindings held on 6 and 13 Nov, each with the renewal's own reason, review by 21 Jan 2027. They stay expired and fill no Name. Merging this pull request is the acknowledgement.

**.github/workflows/name_label_renewal.yml 1.2.0**

- A second schedule renews three times a week (Sun, Tue, Thu 21:40 UTC) and opens a ready pull request; an older renewal pull request still open is closed as superseded.
- With the repository Variable `TFB_NAME_RENEWAL_AUTOMERGE = on`, the job squash-merges its own pull request after checking that it changes only the registry file. Unset, nothing merges without a person.

**.github/workflows/provider_target_coverage.yml 1.2.0, workflow_repair_tests.yml**: map `TFB_PTC_STALE_REVERIFY` (default 1).

## Evidence

- `python scripts/audit_provider_target_coverage.py --selftest`: 18 of 18.
- `tests/test_workflow_audit_repairs.py`: 5 new lifecycle tests. `tests/test_renew_verified_name_labels.py`: T13 to T17.
- On the shipped registry today: `--check` reports 122 open and 6 held acknowledged (exit 1 until the renewal of run 45013078813 is merged).
- `python scripts/audit_repository_workflows.py`: errors=0 warnings=0.

## What the owner does

1. Merge this pull request, and the renewal pull request opened by run 45013078813.
2. Optional, for a hands-free renewal: repository Variable `TFB_NAME_RENEWAL_AUTOMERGE = on`.

Expected after the merge: the next scheduled Provider Target Coverage run (06:20 UTC) passes with `BASELINE_STALE_REVERIFIED`; the name check passes once the renewal is merged.
